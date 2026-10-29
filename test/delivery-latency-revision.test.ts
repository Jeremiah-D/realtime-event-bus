import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { DeliveryLatencyTracker } from '../src/latency.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Reads the one delivery-latency stats entry for a pattern. */
function latencyEntry(bus: EventBus, pattern: string) {
  const entries = bus.getStats().deliveryLatency.filter((e) => e.pattern === pattern);
  assert.equal(entries.length, 1, `expected one deliveryLatency entry for ${pattern}`);
  return entries[0];
}

test('DeliveryLatencyTracker.resize evicts oldest-first and keeps the sum exact', () => {
  const tracker = new DeliveryLatencyTracker(4);
  for (const ms of [10, 20, 30, 40, 50, 60]) tracker.record(ms);
  let s = tracker.summary();
  assert.equal(s.samples, 4);
  assert.equal(s.meanMs, 45); // 30, 40, 50, 60

  tracker.resize(2);
  assert.equal(tracker.windowSizeLimit(), 2);
  s = tracker.summary();
  assert.equal(s.samples, 2);
  assert.equal(s.minMs, 50); // 30 and 40 were evicted, oldest first
  assert.equal(s.maxMs, 60);
  assert.equal(s.meanMs, 55);

  // Widening evicts nothing and the bound moves.
  tracker.resize(10);
  assert.equal(tracker.windowSizeLimit(), 10);
  s = tracker.summary();
  assert.equal(s.samples, 2);
  assert.equal(s.meanMs, 55);
  tracker.record(70);
  tracker.record(80);
  tracker.record(90);
  assert.equal(tracker.summary().samples, 5);

  // The bound is enforced on the next record, not by the resize itself.
  tracker.resize(3);
  s = tracker.summary();
  assert.equal(s.samples, 3);
  assert.equal(s.minMs, 70);

  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => tracker.resize(bad), RangeError);
  }
  assert.equal(tracker.windowSizeLimit(), 3, 'a failed resize leaves the bound untouched');
});

test('setDeliveryLatency shrinking the window evicts only the oldest samples', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: { windowSize: 4 } });
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    clock.nowMs += 10;
  }
  clock.nowMs = 1100; // dwells 100, 90, 80, 70
  await flush();
  let entry = latencyEntry(bus, 't');
  assert.equal(entry.samples, 4);
  assert.equal(entry.minMs, 70);
  assert.equal(entry.meanMs, 85);

  bus.setDeliveryLatency(sub.id, { windowSize: 2 });
  entry = latencyEntry(bus, 't');
  assert.equal(entry.samples, 2);
  assert.equal(entry.minMs, 70); // 100 and 90 were evicted, oldest first
  assert.equal(entry.maxMs, 80);
  assert.equal(entry.meanMs, 75);

  const audit = bus.getSubscriptionAudit({ action: 'delivery_latency_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    deliveryLatency: { windowSize: 2 },
    oldDeliveryLatency: { windowSize: 4 },
  });
  sub.unsubscribe();
});

test('setDeliveryLatency widening evicts nothing and the new bound governs the next samples', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: { windowSize: 2 } });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  clock.nowMs += 10;
  await flush();
  let entry = latencyEntry(bus, 't');
  assert.equal(entry.samples, 2);

  bus.setDeliveryLatency(sub.id, { windowSize: 5 });
  entry = latencyEntry(bus, 't');
  assert.equal(entry.samples, 2, 'widening keeps every sample');

  // The new bound applies from the next samples: three more fit now.
  bus.publish('t', 'c');
  bus.publish('t', 'd');
  bus.publish('t', 'e');
  clock.nowMs += 10;
  await flush();
  entry = latencyEntry(bus, 't');
  assert.equal(entry.samples, 5);

  // And the shrunk-again bound evicts oldest-first on the next summary.
  bus.setDeliveryLatency(sub.id, { windowSize: 3 });
  entry = latencyEntry(bus, 't');
  assert.equal(entry.samples, 3);
  sub.unsubscribe();
});

test('setDeliveryLatency preserves the enqueue clocks of in-flight messages', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: { windowSize: 4 } });
  bus.publish('t', 'a'); // enqueued at 1000
  clock.nowMs += 10;
  await flush(); // sample 10
  let entry = latencyEntry(bus, 't');
  assert.equal(entry.samples, 1);
  assert.equal(entry.meanMs, 10);

  bus.publish('t', 'b'); // enqueued at 1010, still in flight
  bus.setDeliveryLatency(sub.id, { windowSize: 2 });
  clock.nowMs = 1060; // 50ms after b's enqueue, 50ms after the revision too
  await flush();
  entry = latencyEntry(bus, 't');
  assert.equal(entry.samples, 2);
  assert.equal(entry.minMs, 10);
  assert.equal(entry.maxMs, 50, 'b is measured from its original 1010 enqueue, not the revision');
  assert.equal(entry.meanMs, 30);
  sub.unsubscribe();
});

test('setDeliveryLatency identical revision is a no-op with no audit and no eviction', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: { windowSize: 2 } });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c'); // window 2 keeps only the last two
  clock.nowMs += 10;
  await flush();
  assert.equal(latencyEntry(bus, 't').samples, 2);

  bus.setDeliveryLatency(sub.id, { windowSize: 2 });
  bus.setDeliveryLatency(sub.id, {});
  assert.equal(latencyEntry(bus, 't').samples, 2, 'restating the window evicted nothing');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'delivery_latency_revised' }).length,
    0,
    'identical revisions (including an empty object) audit nothing',
  );
  sub.unsubscribe();
});

test('setDeliveryLatency null clears sampling, and revision after a clear is refused', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: { windowSize: 4 } });
  bus.publish('t', 'a');
  await flush();
  assert.equal(latencyEntry(bus, 't').samples, 1);

  bus.setDeliveryLatency(sub.id, null);
  const audit = bus.getSubscriptionAudit({ action: 'delivery_latency_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    deliveryLatency: null,
    oldDeliveryLatency: { windowSize: 4 },
  });

  // No series for the cleared subscriber, and later deliveries sample nothing.
  assert.equal(bus.getStats().deliveryLatency.filter((e) => e.pattern === 't').length, 0);
  bus.publish('t', 'b');
  await flush();
  assert.equal(bus.getStats().deliveryLatency.filter((e) => e.pattern === 't').length, 0);

  assert.throws(() => bus.setDeliveryLatency(sub.id, { windowSize: 8 }), RangeError);
  assert.throws(() => bus.setDeliveryLatency(sub.id, null), RangeError);
  sub.unsubscribe();
});

test('setDeliveryLatency is fail-closed for unknown, unsampled, or illegal values', () => {
  const bus = new EventBus();
  const sampled = bus.subscribe('t', () => {}, { deliveryLatency: { windowSize: 4 } });
  const plain = bus.subscribe('u', () => {});

  const bad: Array<() => void> = [
    () => bus.setDeliveryLatency('', { windowSize: 2 }),
    () => bus.setDeliveryLatency('sub-nope', { windowSize: 2 }),
    () => bus.setDeliveryLatency(plain.id, { windowSize: 2 }),
    () => bus.setDeliveryLatency(sampled.id, undefined as never),
    () => bus.setDeliveryLatency(sampled.id, 'x' as never),
    () => bus.setDeliveryLatency(sampled.id, [] as never),
    () => bus.setDeliveryLatency(sampled.id, { windowSize: 0 }),
    () => bus.setDeliveryLatency(sampled.id, { windowSize: -1 }),
    () => bus.setDeliveryLatency(sampled.id, { windowSize: 2.5 }),
    () => bus.setDeliveryLatency(sampled.id, { windowSize: Number.NaN }),
  ];
  for (const call of bad) assert.throws(call, RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'delivery_latency_revised' }).length,
    0,
    'no failed revision is audited',
  );

  // The live window survived every failed revision.
  bus.setDeliveryLatency(sampled.id, { windowSize: 8 });
  const audit = bus.getSubscriptionAudit({ action: 'delivery_latency_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail.oldDeliveryLatency, { windowSize: 4 });
  sampled.unsubscribe();
  plain.unsubscribe();
});
