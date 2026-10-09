import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type Delivery, type TraceSpan } from '../src/bus.ts';
import {
  formatTraceparent,
  newSpanId,
  newTraceId,
  parseTraceparent,
  resolveTraceOptions,
} from '../src/trace.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** 32-hex trace id — the WR-18 (webhook-relay-ts) interop format. */
const TRACE_ID_RE = /^[0-9a-f]{32}$/;
const SPAN_ID_RE = /^[0-9a-f]{16}$/;

function spanByName(spans: TraceSpan[], name: string): TraceSpan[] {
  return spans.filter((s) => s.name === name);
}

function only(spans: TraceSpan[]): TraceSpan {
  assert.equal(spans.length, 1);
  return spans[0];
}

// --- trace.ts unit tests ---

test('newTraceId is 32-hex (WR-18 interop format), newSpanId is 16-hex', () => {
  assert.match(newTraceId(), TRACE_ID_RE);
  assert.match(newSpanId(), SPAN_ID_RE);
  assert.notEqual(newTraceId(), newTraceId());
  assert.notEqual(newSpanId(), newSpanId());
});

test('parseTraceparent accepts valid headers and rejects malformed ones', () => {
  const traceId = 'a'.repeat(32);
  const spanId = 'b'.repeat(16);
  assert.deepEqual(parseTraceparent(`00-${traceId}-${spanId}-01`), {
    traceId,
    parentSpanId: spanId,
  });
  // Malformed: bad shape, ff version, all-zero ids -> undefined (fresh trace).
  assert.equal(parseTraceparent('bogus'), undefined);
  assert.equal(parseTraceparent(`00-${traceId}-${spanId}`), undefined);
  assert.equal(parseTraceparent(`ff-${traceId}-${spanId}-01`), undefined);
  assert.equal(parseTraceparent(`00-${'0'.repeat(32)}-${spanId}-01`), undefined);
  assert.equal(parseTraceparent(`00-${traceId}-${'0'.repeat(16)}-01`), undefined);
  assert.equal(parseTraceparent(`00-${traceId.toUpperCase()}-${spanId}-01`), undefined);
});

test('formatTraceparent renders a parseable header', () => {
  const traceId = newTraceId();
  const spanId = newSpanId();
  const header = formatTraceparent(traceId, spanId);
  assert.deepEqual(parseTraceparent(header), { traceId, parentSpanId: spanId });
});

test('resolveTraceOptions defaults and RangeError validation', () => {
  assert.equal(resolveTraceOptions(undefined), undefined);
  assert.equal(resolveTraceOptions(null), undefined);
  assert.equal(resolveTraceOptions(false), undefined);
  assert.equal(resolveTraceOptions({ enabled: false }), undefined);
  // Strict: other fields are still validated when explicitly disabled.
  assert.throws(() => resolveTraceOptions({ enabled: false, sampleRate: 2 }), RangeError);
  assert.deepEqual(resolveTraceOptions(true), {
    sampleRate: 1,
    sampler: undefined,
    onTraceSpan: undefined,
    bufferSize: 1024,
  });
  const resolved = resolveTraceOptions({ sampleRate: 0.5, bufferSize: 8 });
  assert.equal(resolved?.sampleRate, 0.5);
  assert.equal(resolved?.bufferSize, 8);
  // Invalid configs throw RangeError at construction time.
  for (const bad of [
    { sampleRate: -0.1 },
    { sampleRate: 1.1 },
    { sampleRate: NaN },
    { sampleRate: Infinity },
    { bufferSize: 0 },
    { bufferSize: 1.5 },
    { bufferSize: -3 },
    { sampler: 'yes' },
    { onTraceSpan: 42 },
  ] as const) {
    assert.throws(() => resolveTraceOptions(bad as never), RangeError, JSON.stringify(bad));
  }
  assert.throws(() => resolveTraceOptions('yes' as never), RangeError);
  assert.throws(() => new EventBus({ trace: { sampleRate: 2 } }), RangeError);
  assert.throws(() => new EventBus({ trace: { bufferSize: 0 } }), RangeError);
  assert.throws(() => new EventBus({ trace: 'yes' as never }), RangeError);
});

// --- bus integration: disabled by default ---

test('tracing is disabled by default: no spans, empty ring buffer', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribe('orders.*', () => {});
  bus.subscribeReliable('orders.*', (d) => deliveries.push(d));
  bus.publish('orders.new', { id: 1 });
  await flush();
  deliveries[0].ack();
  await flush();
  assert.deepEqual(bus.getStats().traceSpans, []);
});

test('sampleRate 0 emits nothing even when tracing is enabled', async () => {
  const clock = controllableClock();
  const seen: TraceSpan[] = [];
  const bus = new EventBus({
    now: clock.now,
    trace: { sampleRate: 0, onTraceSpan: (s) => seen.push(s) },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', {});
  await flush();
  assert.equal(seen.length, 0);
  assert.deepEqual(bus.getStats().traceSpans, []);
});

test('rejected publishes never start a trace (decision is at admission)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, trace: true });
  bus.setTopicSchema('t', () => false);
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', { bad: true }), 0);
  await flush();
  assert.deepEqual(bus.getStats().traceSpans, []);
});

// --- bus integration: full pipeline spans ---

test('sampled publish emits publish/admission/fanout/enqueue spans with linked ids', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now, trace: true });
  const sub = bus.subscribe('orders.*', () => {});
  bus.publish('orders.new', { id: 7 });
  // Synchronous: publish/admission/fanout/enqueue are done; deliver waits
  // for the flush.
  const spans = bus.getStats().traceSpans;
  assert.deepEqual(spans.map((s) => s.name), [
    'bus.admission',
    'bus.enqueue',
    'bus.fanout',
    'bus.publish',
  ]);
  const [admission, enqueue, fanout, publish] = spans;
  // One trace id shared by every span, 32-hex (WR-18 interop format).
  for (const s of spans) {
    assert.match(s.traceId, TRACE_ID_RE);
    assert.equal(s.traceId, publish.traceId);
    assert.match(s.spanId, SPAN_ID_RE);
  }
  assert.equal(new Set(spans.map((s) => s.spanId)).size, spans.length);
  // Parent linkage: admission/fanout/enqueue -> publish root.
  assert.equal(admission.parentId, publish.spanId);
  assert.equal(fanout.parentId, publish.spanId);
  assert.equal(enqueue.parentId, publish.spanId);
  assert.equal(publish.parentId, undefined);
  // Span shape: exactly the documented keys.
  assert.deepEqual(Object.keys(publish).sort(), [
    'at',
    'attrs',
    'durationMs',
    'name',
    'spanId',
    'traceId',
  ]);
  assert.deepEqual(Object.keys(enqueue).sort(), [
    'at',
    'attrs',
    'durationMs',
    'name',
    'parentId',
    'spanId',
    'traceId',
  ]);
  // Attributes.
  assert.deepEqual(publish.attrs, { topic: 'orders.new', seq: 1 });
  assert.deepEqual(admission.attrs, { topic: 'orders.new', seq: 1 });
  assert.deepEqual(fanout.attrs, { topic: 'orders.new', seq: 1, matched: 1, accepted: 1 });
  assert.deepEqual(enqueue.attrs, {
    topic: 'orders.new',
    seq: 1,
    subscriberId: sub.id,
    pattern: 'orders.*',
  });
  // Bus-clock timestamps; durations are non-negative.
  for (const s of spans) {
    assert.equal(s.at, 1_000);
    assert.ok(s.durationMs >= 0);
  }
  sub.unsubscribe();
});

test('deliver span follows the flush, parented to the enqueue span', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now, trace: true });
  bus.subscribe('t', () => {});
  bus.publish('t', {});
  clock.nowMs = 1_500; // queue dwell before the flush
  await flush();
  const spans = bus.getStats().traceSpans;
  const deliver = only(spanByName(spans, 'bus.deliver'));
  const enqueue = only(spanByName(spans, 'bus.enqueue'));
  assert.equal(deliver.parentId, enqueue.spanId);
  assert.equal(deliver.at, 1_500);
  assert.equal(deliver.attrs.dwellMs, 500);
  assert.equal(deliver.attrs.subscriberId, enqueue.attrs.subscriberId);
  assert.deepEqual(spans.map((s) => s.name), [
    'bus.admission',
    'bus.enqueue',
    'bus.fanout',
    'bus.publish',
    'bus.deliver',
  ]);
});

test('one enqueue/deliver span per accepting subscriber', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, trace: true });
  const a = bus.subscribe('t', () => {});
  const b = bus.subscribe('t', () => {});
  bus.publish('t', {});
  await flush();
  const spans = bus.getStats().traceSpans;
  const enqueues = spanByName(spans, 'bus.enqueue');
  const delivers = spanByName(spans, 'bus.deliver');
  assert.equal(enqueues.length, 2);
  assert.equal(delivers.length, 2);
  assert.deepEqual(
    enqueues.map((s) => s.attrs.subscriberId).sort(),
    [a.id, b.id].sort(),
  );
  const publish = only(spanByName(spans, 'bus.publish'));
  for (const s of [...enqueues, ...delivers]) assert.equal(s.traceId, publish.traceId);
});

test('onTraceSpan receives the same spans as the ring buffer, in order', async () => {
  const clock = controllableClock();
  const seen: TraceSpan[] = [];
  const bus = new EventBus({ now: clock.now, trace: { onTraceSpan: (s) => seen.push(s) } });
  bus.subscribe('t', () => {});
  bus.publish('t', {});
  await flush();
  assert.deepEqual(seen, bus.getStats().traceSpans);
  assert.ok(seen.length > 0);
});

test('a throwing onTraceSpan never disturbs the publish/deliver path', async () => {
  const clock = controllableClock();
  const bus = new EventBus({
    now: clock.now,
    trace: {
      onTraceSpan: () => {
        throw new Error('broken observer');
      },
    },
  });
  let delivered = 0;
  bus.subscribe('t', () => {
    delivered += 1;
  });
  bus.publish('t', {});
  await flush();
  assert.equal(delivered, 1);
  // The spans still landed in the ring buffer.
  assert.ok(bus.getStats().traceSpans.length > 0);
});

// --- sampling: head-based rate + pluggable sampler ---

test('custom sampler overrides sampleRate (head-based decision)', async () => {
  const clock = controllableClock();
  const decisions: Array<{ traceId: string; topic: string }> = [];
  const bus = new EventBus({
    now: clock.now,
    trace: {
      sampleRate: 0, // would trace nothing on its own
      sampler: (d) => {
        decisions.push(d);
        return d.topic.startsWith('imp.');
      },
    },
  });
  bus.subscribe('**', () => {});
  bus.publish('imp.orders', {});
  bus.publish('noise.heartbeats', {});
  await flush();
  // The sampler ran once per admitted publish (head-based).
  assert.equal(decisions.length, 2);
  assert.match(decisions[0].traceId, TRACE_ID_RE);
  const traces = new Set(bus.getStats().traceSpans.map((s) => s.traceId));
  assert.equal(traces.size, 1);
  const publish = only(spanByName(bus.getStats().traceSpans, 'bus.publish'));
  assert.equal(publish.attrs.topic, 'imp.orders');
  assert.equal(publish.traceId, decisions[0].traceId);
});

test('a throwing sampler propagates to the publish call', () => {
  const clock = controllableClock();
  const bus = new EventBus({
    now: clock.now,
    trace: {
      sampler: () => {
        throw new Error('sampler blew up');
      },
    },
  });
  bus.subscribe('t', () => {});
  assert.throws(() => bus.publish('t', {}), /sampler blew up/);
});

// --- traceparent propagation ---

test('valid traceparent continues the upstream trace', async () => {
  const clock = controllableClock();
  const upstreamTraceId = 'c'.repeat(32);
  const upstreamSpanId = 'd'.repeat(16);
  const bus = new EventBus({ now: clock.now, trace: true });
  bus.subscribe('t', () => {});
  bus.publish('t', {}, { traceparent: `00-${upstreamTraceId}-${upstreamSpanId}-01` });
  await flush();
  const spans = bus.getStats().traceSpans;
  assert.ok(spans.length > 0);
  for (const s of spans) assert.equal(s.traceId, upstreamTraceId);
  const publish = only(spanByName(spans, 'bus.publish'));
  assert.equal(publish.parentId, upstreamSpanId);
});

test('malformed traceparent mints a fresh trace id', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, trace: true });
  bus.subscribe('t', () => {});
  bus.publish('t', {}, { traceparent: 'not-a-traceparent' });
  const publish = only(spanByName(bus.getStats().traceSpans, 'bus.publish'));
  assert.match(publish.traceId, TRACE_ID_RE);
  assert.equal(publish.parentId, undefined);
});

test('non-string traceparent throws RangeError', () => {
  const bus = new EventBus({ trace: true });
  assert.throws(() => bus.publish('t', {}, { traceparent: 42 as never }), RangeError);
  assert.throws(
    () => bus.publishBatch([{ topic: 't', payload: {}, traceparent: 42 as never }]),
    RangeError,
  );
  assert.throws(
    () =>
      bus.publishDelayed('t', {}, { delayMs: 10, traceparent: 42 as never }),
    RangeError,
  );
});

// --- ack spans ---

test('reliable ack emits a bus.ack span parented to the enqueue span', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now, trace: true });
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => deliveries.push(d));
  bus.publish('t', {});
  await flush();
  assert.equal(deliveries.length, 1);
  clock.nowMs = 1_800;
  deliveries[0].ack();
  const spans = bus.getStats().traceSpans;
  const ack = only(spanByName(spans, 'bus.ack'));
  const enqueue = only(spanByName(spans, 'bus.enqueue'));
  assert.equal(ack.parentId, enqueue.spanId);
  assert.equal(ack.at, 1_000); // opened at hand-off…
  assert.equal(ack.durationMs, 800); // …closed when ack() finished
  assert.equal(ack.attrs.subscriberId, sub.id);
  assert.equal(ack.attrs.redeliveries, 0);
  assert.equal(ack.traceId, only(spanByName(spans, 'bus.publish')).traceId);
});

test('nack abandons the ack span; the redelivery opens a fresh one', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now, trace: true });
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable('t', (d) => deliveries.push(d));
  bus.publish('t', {});
  await flush();
  assert.equal(deliveries.length, 1);
  deliveries[0].nack(); // abandon: no ack span for this delivery
  await flush(); // redelivery
  assert.equal(deliveries.length, 2);
  assert.equal(spanByName(bus.getStats().traceSpans, 'bus.ack').length, 0);
  clock.nowMs = 2_000;
  deliveries[1].ack();
  const ack = only(spanByName(bus.getStats().traceSpans, 'bus.ack'));
  assert.equal(ack.attrs.redeliveries, 1);
  // The redelivery is a new enqueue event in the same trace.
  const enqueues = spanByName(bus.getStats().traceSpans, 'bus.enqueue');
  assert.equal(enqueues.length, 2);
  assert.equal(enqueues[0].traceId, enqueues[1].traceId);
});

test('acking a stale handle after timeout emits no ack span', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now, trace: true });
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable('t', (d) => deliveries.push(d), { ackTimeoutMs: 30 });
  bus.publish('t', {});
  await flush();
  const stale = deliveries[0];
  // Let the ack timeout fire (real timer): the delivery requeues.
  await new Promise((resolve) => setTimeout(resolve, 60));
  await flush();
  assert.ok(deliveries.length >= 2);
  stale.ack(); // stale handle: must not complete an ack span
  assert.equal(spanByName(bus.getStats().traceSpans, 'bus.ack').length, 0);
  // The live redelivery's ack still spans.
  deliveries[deliveries.length - 1].ack();
  assert.equal(spanByName(bus.getStats().traceSpans, 'bus.ack').length, 1);
});

// --- ring buffer ---

test('traceSpans ring buffer evicts the oldest spans past bufferSize', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, trace: { bufferSize: 4 } });
  bus.subscribe('t', () => {});
  // One publish emits 4 spans pre-flush: admission, enqueue, fanout, publish.
  bus.publish('t', { n: 1 });
  assert.equal(bus.getStats().traceSpans.length, 4);
  bus.publish('t', { n: 2 });
  const spans = bus.getStats().traceSpans;
  assert.equal(spans.length, 4);
  // The first publish's spans were evicted; only the second's remain.
  for (const s of spans) assert.equal(s.attrs.seq, 2);
  assert.deepEqual(spans.map((s) => s.name), [
    'bus.admission',
    'bus.enqueue',
    'bus.fanout',
    'bus.publish',
  ]);
});

test('getStats().traceSpans is a snapshot: mutating it does not affect the bus', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, trace: true });
  bus.subscribe('t', () => {});
  bus.publish('t', {});
  await flush();
  const snapshot = bus.getStats().traceSpans;
  const before = snapshot.length;
  snapshot.length = 0;
  assert.equal(bus.getStats().traceSpans.length, before);
});

// --- batch + delayed publish paths ---

test('publishBatch threads traceparent per message', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, trace: true });
  bus.subscribe('t', () => {});
  const upstream = `00-${'e'.repeat(32)}-${'f'.repeat(16)}-01`;
  bus.publishBatch([
    { topic: 't', payload: { a: 1 }, traceparent: upstream },
    { topic: 't', payload: { b: 2 } },
  ]);
  const spans = bus.getStats().traceSpans;
  const publishes = spanByName(spans, 'bus.publish');
  assert.equal(publishes.length, 2);
  assert.equal(publishes[0].traceId, 'e'.repeat(32));
  assert.equal(publishes[0].parentId, 'f'.repeat(16));
  assert.match(publishes[1].traceId, TRACE_ID_RE);
  assert.notEqual(publishes[1].traceId, 'e'.repeat(32));
});

test('batched subscriber deliveries emit one deliver span per message', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, trace: true });
  bus.subscribe('t', () => {}, { batch: { maxSize: 10, maxWaitMs: 0 } });
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  // The partial batch lingers on a 0ms timer before it is handed off.
  await new Promise((resolve) => setTimeout(resolve, 10));
  await flush();
  const spans = bus.getStats().traceSpans;
  const delivers = spanByName(spans, 'bus.deliver');
  assert.equal(delivers.length, 2);
  // Each deliver span parents to its own message's enqueue span.
  for (const d of delivers) {
    const enqueue = spanByName(spans, 'bus.enqueue').find(
      (e) => e.spanId === d.parentId,
    );
    assert.ok(enqueue !== undefined);
    assert.equal(enqueue.attrs.seq, d.attrs.seq);
  }
});

test('publishDelayed carries traceparent to fan-out time', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now, trace: true });
  bus.subscribe('t', () => {});
  const upstream = `00-${'a'.repeat(32)}-${'b'.repeat(16)}-01`;
  bus.publishDelayed('t', {}, { delayMs: 100, traceparent: upstream });
  // Not due yet: no spans.
  assert.deepEqual(bus.getStats().traceSpans, []);
  clock.nowMs = 1_200;
  bus.publish('t', {}); // any publish sweeps due delayed messages on flush
  await flush();
  const spans = bus.getStats().traceSpans;
  const continued = spans.filter((s) => s.traceId === 'a'.repeat(32));
  assert.ok(continued.length > 0);
  assert.equal(only(spanByName(continued, 'bus.publish')).parentId, 'b'.repeat(16));
});
