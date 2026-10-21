/**
 * Tail-based delivery-pipeline trace sampling (EB-181).
 *
 * The complement to head-based sampling (EB-45): every admitted
 * publish's spans buffer until the delivery terminal state, then the
 * trace is decided — handler errors, ack timeouts, and dead-letters are
 * always kept; successful traces are kept with probability
 * `successSampleRate`.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { resolveTailSamplingOptions } from '../src/tail-sampling.ts';
import type { TraceSpan } from '../src/trace.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function spanNames(spans: TraceSpan[]): string[] {
  return spans.map((s) => s.name);
}

test('disabled by default: no tail spans, no overhead', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(received, [1]);
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
});

test('invalid configuration throws RangeError at construction', () => {
  const bad: Array<unknown> = [
    { successSampleRate: -0.1 },
    { successSampleRate: 1.1 },
    { successSampleRate: Number.NaN },
    { maxOpenTraces: 0 },
    { maxOpenTraces: 1.5 },
    { maxSpansPerTrace: 0 },
    { bufferSize: 0 },
    { onTraceSpan: 'nope' },
    'yes',
    42,
  ];
  for (const config of bad) {
    assert.throws(
      () => new EventBus({ tailSampling: config as never }),
      RangeError,
      `expected RangeError for ${JSON.stringify(config)}`,
    );
  }
  assert.equal(resolveTailSamplingOptions(undefined), undefined);
  assert.equal(resolveTailSamplingOptions(false), undefined);
  assert.equal(resolveTailSamplingOptions({ enabled: false })?.successSampleRate, undefined);
  const defaults = resolveTailSamplingOptions(true)!;
  assert.equal(defaults.successSampleRate, 1);
  assert.equal(defaults.maxOpenTraces, 4096);
});

test('successful trace kept at rate 1: full span narrative', async () => {
  const bus = new EventBus({ tailSampling: true });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'hello');
  await flush();
  assert.deepEqual(received, ['hello']);
  const spans = bus.getStats().tailSampledSpans;
  // Completion order: the enqueue span closes before the fanout span
  // (fanout closes after all subscribers are fanned out to).
  assert.deepEqual(spanNames(spans), [
    'bus.admission',
    'bus.enqueue',
    'bus.fanout',
    'bus.publish',
    'bus.deliver',
  ]);
  // One trace, shared trace id, completion order.
  const traceIds = new Set(spans.map((s) => s.traceId));
  assert.equal(traceIds.size, 1);
  const publishSpan = spans.find((s) => s.name === 'bus.publish')!;
  assert.equal(publishSpan.attrs.tailKeepReason, 'sampled');
  assert.equal(publishSpan.attrs.topic, 't');
  assert.equal(publishSpan.attrs.seq, 1);
  const deliverSpan = spans.find((s) => s.name === 'bus.deliver')!;
  assert.equal(deliverSpan.attrs.outcome, 'delivered');
});

test('successful trace dropped at rate 0', async () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 0 } });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(bus.getStats().tailSampledSpans, []);
});

test('handler error always kept, even at rate 0', async () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 0 } });
  // A health-probed subscriber swallows the throw (so the drain
  // survives) and reports the true failure outcome to the tail sampler.
  bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  bus.publish('t', 1);
  await flush();
  await flush();
  const spans = bus.getStats().tailSampledSpans;
  assert.ok(spans.length > 0, 'expected the error trace to be kept');
  const publishSpan = spans.find((s) => s.name === 'bus.publish')!;
  assert.equal(publishSpan.attrs.tailKeepReason, 'error');
  const deliverSpan = spans.find((s) => s.name === 'bus.deliver')!;
  assert.equal(deliverSpan.attrs.outcome, 'error');
});

test('ack timeout always kept, even at rate 0', async () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 0 } });
  let deliveries = 0;
  bus.subscribeReliable(
    't',
    (delivery) => {
      deliveries += 1;
      // Ack the redelivery so the trace can close.
      if (deliveries > 1) delivery.ack();
    },
    { ackTimeoutMs: 30 },
  );
  bus.publish('t', 1);
  await sleep(150);
  await flush();
  assert.ok(deliveries >= 2, 'expected a timeout redelivery');
  const spans = bus.getStats().tailSampledSpans;
  assert.ok(spans.length > 0, 'expected the timeout trace to be kept');
  const publishSpan = spans.find((s) => s.name === 'bus.publish')!;
  assert.equal(publishSpan.attrs.tailKeepReason, 'ack-timeout');
});

test('dead letter always kept, even at rate 0', async () => {
  const bus = new EventBus({ tailSampling: { successSampleRate: 0 } });
  const sub = bus.subscribeReliable(
    't',
    (delivery) => delivery.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', 1);
  await flush();
  await flush();
  const spans = bus.getStats().tailSampledSpans;
  assert.ok(spans.length > 0, 'expected the dead-letter trace to be kept');
  const publishSpan = spans.find((s) => s.name === 'bus.publish')!;
  assert.equal(publishSpan.attrs.tailKeepReason, 'dead-letter');
  // The dead-lettered message is queryable — the trace and the DLQ agree.
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('trace waits for every delivery leg: two subscribers, one trace', async () => {
  const bus = new EventBus({ tailSampling: true });
  bus.subscribe('t', () => {});
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  const spans = bus.getStats().tailSampledSpans;
  assert.equal(spans.filter((s) => s.name === 'bus.enqueue').length, 2);
  assert.equal(spans.filter((s) => s.name === 'bus.deliver').length, 2);
  assert.equal(spans.filter((s) => s.name === 'bus.publish').length, 1);
});

test('tail spans flow to tailSampling.onTraceSpan', async () => {
  const seen: TraceSpan[] = [];
  const bus = new EventBus({
    tailSampling: { onTraceSpan: (span) => seen.push(span) },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  assert.ok(seen.length > 0, 'expected spans on the tail callback');
  assert.ok(seen.every((s) => typeof s.traceId === 'string'));
  assert.equal(
    seen.find((s) => s.name === 'bus.publish')?.attrs.tailKeepReason,
    'sampled',
  );
});

test('tail spans reuse the head-based onTraceSpan channel when no tail callback is set', async () => {
  const seen: TraceSpan[] = [];
  const bus = new EventBus({
    trace: { sampleRate: 0, onTraceSpan: (span) => seen.push(span) },
    tailSampling: true,
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  await flush();
  // Head-based sampling is off (rate 0), so every span here is tail-kept.
  // The keep reason is stamped on the publish root only, by design.
  assert.ok(seen.length > 0, 'expected tail spans on the shared channel');
  assert.equal(
    seen.find((s) => s.name === 'bus.publish')?.attrs.tailKeepReason,
    'sampled',
  );
});

test('a throwing tail callback never disturbs the publish path', async () => {
  const bus = new EventBus({
    tailSampling: {
      onTraceSpan: () => {
        throw new Error('observer is broken');
      },
    },
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(received, [1]);
  assert.ok(bus.getStats().tailSampledSpans.length > 0);
});
