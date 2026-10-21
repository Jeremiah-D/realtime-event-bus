import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  assertValidLineage,
  checkLineageCycle,
  appendLineageHop,
  isValidLineageShape,
  resolveMaxLineageHops,
  DEFAULT_MAX_LINEAGE_HOPS,
  LineageDepthSampler,
  type AdmissionRejectionEvent,
  type BridgeEnvelope,
  type BridgeTransport,
  type BusMessage,
  type MessageLineage,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function loopbackTransport(
  deliver: (env: BridgeEnvelope) => void,
  seen: BridgeEnvelope[] = [],
): BridgeTransport {
  return {
    name: 'loopback',
    publish(envelope: BridgeEnvelope) {
      seen.push(envelope);
      deliver(envelope);
    },
  };
}

const sink: BridgeTransport = { name: 'sink', publish() {} };

// ---------------------------------------------------------------------------
// Unit: lineage.ts
// ---------------------------------------------------------------------------

test('assertValidLineage accepts well-formed input and returns a defensive copy', () => {
  const input = {
    originTopic: 'orders',
    parentMessageId: 'm-1',
    hops: [{ topic: 'orders', at: 100 }],
  };
  const out = assertValidLineage(input, 't');
  assert.deepEqual(out, input);
  // Defensive copy: mutating the result must not touch the caller's object.
  (out as MessageLineage).hops.push({ topic: 'x', at: 1 });
  assert.equal(input.hops.length, 1);
  assert.equal(assertValidLineage(undefined, 't'), undefined);
  // originTopic defaults to '' (the bus fills the publish topic at admission).
  assert.equal(assertValidLineage({ hops: [] }, 't')?.originTopic, '');
});

test('assertValidLineage throws fail-fast on malformed shapes', () => {
  assert.throws(() => assertValidLineage(42, 't'), TypeError);
  assert.throws(() => assertValidLineage(null, 't'), TypeError);
  // Empty originTopic is accepted: the bus defaults it to the publish topic.
  assert.equal(assertValidLineage({ originTopic: '', hops: [] }, 't')?.originTopic, '');
  assert.throws(() => assertValidLineage({ originTopic: 42 }, 't'), RangeError);
  assert.throws(() => assertValidLineage({ hops: 'nope' }, 't'), RangeError);
  assert.throws(() => assertValidLineage({ hops: [{ topic: '', at: 1 }] }, 't'), RangeError);
  assert.throws(() => assertValidLineage({ hops: [{ topic: 'a', at: NaN }] }, 't'), RangeError);
  assert.throws(() => assertValidLineage({ hops: [{ topic: 'a' }] }, 't'), RangeError);
  assert.throws(() => assertValidLineage({ parentMessageId: 7 }, 't'), RangeError);
  assert.throws(
    () => assertValidLineage({ hops: new Array(5000).fill({ topic: 'a', at: 1 }) }, 't'),
    RangeError,
  );
});

test('isValidLineageShape never throws', () => {
  assert.equal(isValidLineageShape({ hops: [] }), true);
  assert.equal(isValidLineageShape({ hops: [{ topic: 'a' }] }), false);
  assert.equal(isValidLineageShape('nope'), false);
});

test('checkLineageCycle: ok / cycle / depth, trailing run exempt', () => {
  const mk = (hops: Array<{ topic: string; at: number }>): MessageLineage => ({
    originTopic: 'a',
    hops,
  });
  assert.equal(checkLineageCycle(mk([]), 'a', 32), 'ok');
  assert.equal(checkLineageCycle(mk([{ topic: 'a', at: 1 }]), 'b', 32), 'ok');
  // Visited and left: a real loop.
  assert.equal(
    checkLineageCycle(mk([{ topic: 'a', at: 1 }, { topic: 'b', at: 2 }]), 'a', 32),
    'cycle',
  );
  // Trailing run of the target topic is exempt (transform stages /
  // same-topic re-admission), so bridge A->B on one topic name works.
  assert.equal(checkLineageCycle(mk([{ topic: 'a', at: 1 }]), 'a', 32), 'ok');
  assert.equal(
    checkLineageCycle(mk([{ topic: 'a', at: 1 }, { topic: 'a', at: 2 }]), 'a', 32),
    'ok',
  );
  // Depth cap: already at the cap rejects before the hop that would exceed it.
  assert.equal(
    checkLineageCycle(mk([{ topic: 'x', at: 1 }, { topic: 'y', at: 2 }]), 'z', 2),
    'depth',
  );
  assert.equal(checkLineageCycle(mk([{ topic: 'x', at: 1 }]), 'z', 2), 'ok');
});

test('appendLineageHop is immutable and defaults an empty originTopic', () => {
  const base: MessageLineage = { originTopic: '', hops: [] };
  const next = appendLineageHop(base, 'orders', 42);
  assert.deepEqual(next, { originTopic: 'orders', hops: [{ topic: 'orders', at: 42 }] });
  assert.deepEqual(base.hops, []);
  const again = appendLineageHop(next, 'ship', 43);
  assert.equal(again.hops.length, 2);
  assert.equal(next.hops.length, 1);
});

test('resolveMaxLineageHops defaults and validates', () => {
  assert.equal(resolveMaxLineageHops(undefined), DEFAULT_MAX_LINEAGE_HOPS);
  assert.equal(DEFAULT_MAX_LINEAGE_HOPS, 32);
  assert.equal(resolveMaxLineageHops(4), 4);
  assert.throws(() => resolveMaxLineageHops(0), RangeError);
  assert.throws(() => resolveMaxLineageHops(-1), RangeError);
  assert.throws(() => resolveMaxLineageHops(1.5), RangeError);
  assert.throws(() => resolveMaxLineageHops('8'), RangeError);
});

test('LineageDepthSampler: nearest-rank p50/p95, 0 when empty', () => {
  const s = new LineageDepthSampler();
  assert.equal(s.p50(), 0);
  assert.equal(s.p95(), 0);
  for (let i = 1; i <= 100; i++) s.add(i);
  assert.equal(s.p50(), 50);
  assert.equal(s.p95(), 95);
});

// ---------------------------------------------------------------------------
// Bus: admission
// ---------------------------------------------------------------------------

test('publish stamps the origin hop; originTopic and parentMessageId default', async () => {
  const clock = controllableClock(5_000);
  const bus = new EventBus({ now: clock.now });
  const seen: BusMessage[] = [];
  bus.subscribe('orders', (m) => seen.push(m));

  bus.publish('orders', { id: 1 }, { lineage: { originTopic: '', hops: [] }, messageId: 'm-9' });
  await flush();
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].lineage, {
    originTopic: 'orders',
    parentMessageId: 'm-9',
    hops: [{ topic: 'orders', at: 5_000 }],
  });

  // Explicit values survive; the origin hop names the admitting topic.
  clock.nowMs = 6_000;
  bus.publish(
    'orders',
    { id: 2 },
    { lineage: { originTopic: 'shop', parentMessageId: 'p-1', hops: [] } },
  );
  await flush();
  assert.deepEqual(seen[1].lineage, {
    originTopic: 'shop',
    parentMessageId: 'p-1',
    hops: [{ topic: 'orders', at: 6_000 }],
  });
});

test('invalid lineage throws fail-fast with zero side effects', () => {
  const bus = new EventBus();
  const before = bus.getStats().totalPublished;
  assert.throws(() => bus.publish('t', {}, { lineage: { hops: [{ topic: 't' }] } as never }), RangeError);
  assert.throws(() => bus.publish('t', {}, { lineage: 42 as never }), TypeError);
  assert.equal(bus.getStats().totalPublished, before);
  assert.equal(bus.getStats().lineageCycleRejections, 0);
});

test('cycle: publish to a visited-and-left topic is rejected with zero side effects', async () => {
  const rejections: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    onAdmissionRejected: (e) => rejections.push(e),
  });
  const seen: BusMessage[] = [];
  bus.subscribe('b', (m) => seen.push(m));

  // 'a' -> 'b': 'b' was visited and left ('a' came after), so returning
  // to 'a' is a loop.
  const looped = bus.publish('a', { v: 1 }, {
    lineage: { originTopic: 'a', hops: [{ topic: 'a', at: 1 }, { topic: 'b', at: 2 }] },
  });
  assert.equal(looped, 0);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  assert.equal(rejections.length, 1);
  assert.equal(rejections[0].reason, 'lineage-cycle');
  assert.equal(rejections[0].topic, 'a');
  // Per-topic counter moves too.
  assert.equal(
    bus.getStats().topics.find((t) => t.topic === 'a')?.lineageCycleRejections,
    1,
  );
  // No sequence number consumed: a later good publish continues the stream.
  const lastSeqBefore = bus.getStats().topics.find((t) => t.topic === 'a')?.lastSeq ?? 0;
  bus.publish('a', { v: 2 });
  await flush();
  assert.equal(bus.getStats().topics.find((t) => t.topic === 'a')?.lastSeq, lastSeqBefore + 1);
});

test('depth: a lineage already at the cap is rejected', () => {
  const rejections: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    maxLineageHops: 2,
    onAdmissionRejected: (e) => rejections.push(e),
  });
  bus.subscribe('z', () => {});
  const n = bus.publish('z', {}, {
    lineage: { originTopic: 'x', hops: [{ topic: 'x', at: 1 }, { topic: 'y', at: 2 }] },
  });
  assert.equal(n, 0);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  assert.equal(rejections[0].reason, 'lineage-depth');
  // One below the cap still admits; the rejection count is cumulative.
  assert.equal(
    bus.publish('z', {}, { lineage: { originTopic: 'x', hops: [{ topic: 'x', at: 1 }] } }),
    1,
  );
  assert.equal(bus.getStats().lineageCycleRejections, 1);
});

test('constructor rejects an invalid maxLineageHops', () => {
  assert.throws(() => new EventBus({ maxLineageHops: 0 }), RangeError);
  assert.throws(() => new EventBus({ maxLineageHops: -3 }), RangeError);
  assert.throws(() => new EventBus({ maxLineageHops: 2.5 }), RangeError);
});

test('transform stages append one hop each', async () => {
  const clock = controllableClock(100);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTransforms('t', [
    (p) => p,
    (p) => p,
  ]);
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  bus.publish('t', { a: 1 }, { lineage: { originTopic: 't', hops: [] } });
  await flush();
  assert.equal(seen.length, 1);
  assert.deepEqual(
    seen[0].lineage?.hops.map((h) => h.topic),
    ['t', 't', 't'],
  );
  // One bus-clock read per hop.
  assert.ok((seen[0].lineage?.hops[0]?.at as number) <= (seen[0].lineage?.hops[2]?.at as number));
});

// ---------------------------------------------------------------------------
// Bus: forwarding paths extend the chain
// ---------------------------------------------------------------------------

test('topic route forward extends the lineage', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('a', 'b');
  const seenB: BusMessage[] = [];
  bus.subscribe('b', (m) => seenB.push(m));
  bus.publish('a', { x: 1 }, { lineage: { originTopic: 'a', hops: [] } });
  await flush();
  assert.equal(seenB.length, 1);
  assert.deepEqual(
    seenB[0].lineage?.hops.map((h) => h.topic),
    ['a', 'b'],
  );
  assert.equal(seenB[0].lineage?.originTopic, 'a');
});

test('cross-bus forward extends the lineage', async () => {
  const busA = new EventBus({ busId: 'gw-a' });
  const busB = new EventBus({ busId: 'gw-b' });
  busA.forward(busB, 'a', { dstTopic: 'b' });
  const seenB: BusMessage[] = [];
  busB.subscribe('b', (m) => seenB.push(m));
  busA.publish('a', { x: 1 }, { lineage: { originTopic: 'a', hops: [] } });
  await flush();
  assert.equal(seenB.length, 1);
  assert.deepEqual(
    seenB[0].lineage?.hops.map((h) => h.topic),
    ['a', 'b'],
  );
});

test('mirror ingress extends the lineage on the same topic', async () => {
  const busA = new EventBus({ busId: 'm-a' });
  const busB = new EventBus({ busId: 'm-b' });
  busB.mirror(busA, [{ srcPattern: 'a' }]);
  const seenB: BusMessage[] = [];
  busB.subscribe('a', (m) => seenB.push(m));
  busA.publish('a', { x: 1 }, { lineage: { originTopic: 'a', hops: [] } });
  await flush();
  assert.equal(seenB.length, 1);
  // Same topic on another bus: not a cycle, the chain grows.
  assert.deepEqual(
    seenB[0].lineage?.hops.map((h) => h.topic),
    ['a', 'a'],
  );
});

test('a forwarding loop back to a visited topic is rejected', async () => {
  const busA = new EventBus({ busId: 'loop-a' });
  const busB = new EventBus({ busId: 'loop-b' });
  // A -> B across two buses; the return leg is simulated by handing the
  // received lineage back — no registration-time check can see this
  // application-level loop.
  busA.forward(busB, 'a', { dstTopic: 'b' });
  const seenB: BusMessage[] = [];
  busB.subscribe('b', (m) => seenB.push(m));
  busA.publish('a', { x: 1 }, { lineage: { originTopic: 'a', hops: [] } });
  await flush();
  assert.equal(seenB.length, 1);
  assert.deepEqual(
    (seenB[0].lineage as MessageLineage).hops.map((h) => h.topic),
    ['a', 'b'],
  );
  // The app echoes the message back to 'a' on busA with the received
  // chain: 'a' was visited and left -> cycle.
  const back = busA.publish('a', { x: 1 }, { lineage: seenB[0].lineage });
  assert.equal(back, 0);
  assert.equal(busA.getStats().lineageCycleRejections, 1);
});

test('bridge carries the lineage and validates it on receipt', async () => {
  const seenA: BridgeEnvelope[] = [];
  let busB!: EventBus;
  const busA = new EventBus({
    bridge: { transport: loopbackTransport((env) => busB.receiveFromBridge(env), seenA) },
  });
  busB = new EventBus({ bridge: { transport: sink } });
  const received: BusMessage[] = [];
  busB.subscribe('t', (m) => received.push(m));

  busA.publish('t', { id: 1 }, { lineage: { originTopic: 't', hops: [] } });
  assert.equal(seenA.length, 1);
  assert.deepEqual(
    (seenA[0].lineage as MessageLineage).hops.map((h) => h.topic),
    ['t'],
  );
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(
    (received[0].lineage as MessageLineage).hops.map((h) => h.topic),
    ['t', 't'],
  );

  // A malformed lineage rejects the envelope outright.
  const bad = busB.receiveFromBridge({
    topic: 't',
    payload: { id: 2 },
    lineage: { hops: [{ topic: 't' }] } as never,
  });
  assert.equal(bad.accepted, false);
  assert.equal(bad.reason, 'invalid-envelope');
});

// ---------------------------------------------------------------------------
// Bus: stats, batch/atomic/delayed
// ---------------------------------------------------------------------------

test('getStats exposes the hop-depth distribution and rejection count', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  // Depths: 1, 1, 3, 5 -> p50 = 1 (nearest-rank), p95 = 5.
  bus.publish('t', {}, { lineage: { originTopic: 't', hops: [] } });
  bus.publish('t', {}, { lineage: { originTopic: 't', hops: [] } });
  bus.setTopicTransforms('t', [(p) => p, (p) => p]);
  bus.publish('t', {}, { lineage: { originTopic: 't', hops: [] } });
  bus.setTopicTransforms('t', [(p) => p, (p) => p, (p) => p, (p) => p]);
  bus.publish('t', {}, { lineage: { originTopic: 't', hops: [] } });
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.lineageCycleRejections, 0);
  assert.equal(stats.lineageDepthP50, 1);
  assert.equal(stats.lineageDepthP95, 5);
});

test('publishBatch entries carry lineage', async () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  const n = bus.publishBatch([
    { topic: 't', payload: { i: 0 }, lineage: { originTopic: 't', hops: [] } },
    { topic: 't', payload: { i: 1 } },
  ]);
  assert.equal(n, 2);
  await flush();
  assert.equal(seen.length, 2);
  assert.deepEqual(seen[0].lineage?.hops.map((h) => h.topic), ['t']);
  assert.equal(seen[1].lineage, undefined);
  assert.throws(
    () => bus.publishBatch([{ topic: 't', payload: {}, lineage: { hops: [{}] } as never }]),
    RangeError,
  );
});

test('publishAtomic: entries carry lineage; a looping entry aborts the batch', async () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  const ok = bus.publishAtomic([
    { topic: 't', payload: { i: 0 }, lineage: { originTopic: 't', hops: [] } },
    { topic: 't', payload: { i: 1 } },
  ]);
  assert.deepEqual(ok, { published: 2 });
  await flush();
  assert.deepEqual(seen[0].lineage?.hops.map((h) => h.topic), ['t']);

  const bad = bus.publishAtomic([
    { topic: 't', payload: { i: 0 } },
    {
      topic: 't',
      payload: { i: 1 },
      lineage: { originTopic: 't', hops: [{ topic: 't', at: 1 }, { topic: 'u', at: 2 }] },
    },
  ]);
  assert.equal(bad.published, 0);
  assert.equal(bad.rejected?.reason, 'lineage-cycle');
  assert.equal(bad.rejected?.index, 1);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  await flush();
  assert.equal(seen.length, 2); // the aborted batch committed nothing
});

test('publishDelayed carries lineage to the due-time fan-out', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  const id = bus.publishDelayed('t', { d: 1 }, { delayMs: 500, lineage: { originTopic: 't', hops: [] } });
  assert.ok(typeof id === 'string');
  await flush();
  assert.equal(seen.length, 0);
  clock.nowMs = 2_000;
  bus.publish('t', 'poke'); // any publish triggers the flush, whose sweep fans out due entries
  await flush();
  await flush();
  assert.equal(seen.length, 2); // the poke plus the due delayed message
  const delayed = seen.find((m) => (m.payload as { d?: number }).d === 1) as BusMessage;
  assert.deepEqual(delayed.lineage, {
    originTopic: 't',
    hops: [{ topic: 't', at: 2_000 }],
  });
  assert.throws(
    () => bus.publishDelayed('t', {}, { delayMs: 10, lineage: { hops: 'x' } as never }),
    RangeError,
  );
});
