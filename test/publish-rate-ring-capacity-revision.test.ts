import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusStats } from '../src/bus.ts';
import { DEFAULT_RATE_RING_CAPACITY } from '../src/rates.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const ratesOf = (stats: BusStats, topic: string) =>
  stats.topics.find((t) => t.topic === topic)?.rates;

function publishN(bus: EventBus, topic: string, n: number): void {
  for (let i = 0; i < n; i += 1) bus.publish(topic, i);
}

test('defaults: getter returns the module default before any revision', () => {
  const bus = new EventBus();
  assert.equal(bus.getPublishRateRingCapacity(), DEFAULT_RATE_RING_CAPACITY);
  assert.equal(DEFAULT_RATE_RING_CAPACITY, 60_000);
});

test('tighten caps the 5m rate at capacity/300 msg/s', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setPublishRateRingCapacity(300);
  publishN(bus, 'capped.a', 600);
  // The ring keeps only the newest 300 of the 600 samples, so the 5m
  // rate is floored at 300/300 = 1 instead of the true 600/300 = 2.
  assert.deepEqual(ratesOf(bus.getStats(), 'capped.a'), { r1s: 300, r1m: 5, r5m: 1 });
});

test('widen restores exact 5m rates for publishes after the revision', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setPublishRateRingCapacity(300);
  publishN(bus, 'widen.a', 600);
  assert.equal(ratesOf(bus.getStats(), 'widen.a')?.r5m, 1);
  bus.setPublishRateRingCapacity(1200);
  publishN(bus, 'widen.a', 600);
  // The widened ring holds all 900 samples (300 kept + 600 new), so the
  // 5m rate is exact again: 900/300 = 3.
  assert.deepEqual(ratesOf(bus.getStats(), 'widen.a'), { r1s: 900, r1m: 15, r5m: 3 });
});

test('tighten drops the oldest samples while the 1m rate is unchanged', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  publishN(bus, 'old.a', 100);
  clock.nowMs += 240_000;
  publishN(bus, 'old.a', 100);
  // First batch is 4m old: inside the 5m window, outside the 1m one.
  assert.deepEqual(ratesOf(bus.getStats(), 'old.a'), {
    r1s: 100,
    r1m: 100 / 60,
    r5m: 200 / 300,
  });
  bus.setPublishRateRingCapacity(100);
  // The ring keeps the newest 100 — the second batch — so the 1m rate
  // is untouched while the 5m rate halves.
  assert.deepEqual(ratesOf(bus.getStats(), 'old.a'), {
    r1s: 100,
    r1m: 100 / 60,
    r5m: 100 / 300,
  });
});

test('widen keeps every sample the old ring held', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setPublishRateRingCapacity(50);
  publishN(bus, 'keep.a', 100);
  assert.equal(ratesOf(bus.getStats(), 'keep.a')?.r1s, 50);
  bus.setPublishRateRingCapacity(200);
  publishN(bus, 'keep.a', 100);
  // 50 survivors + 100 new = 150 — the widened ring kept everything it
  // had and did not resurrect the 50 the tight ring had already evicted.
  assert.deepEqual(ratesOf(bus.getStats(), 'keep.a'), { r1s: 150, r1m: 2.5, r5m: 0.5 });
});

test('illegal capacities throw RangeError and leave the live value untouched', () => {
  const bus = new EventBus();
  const before = bus.getPublishRateRingCapacity();
  for (const capacity of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 'x']) {
    assert.throws(
      () => bus.setPublishRateRingCapacity(capacity as unknown as number),
      RangeError,
    );
    assert.equal(bus.getPublishRateRingCapacity(), before);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'publish_rate_ring_capacity_revised' }).length,
    0,
  );
  assert.throws(() => bus.setPublishRateRingCapacity(7, ''), RangeError);
  assert.equal(bus.getPublishRateRingCapacity(), before);
});

test('re-affirming the current capacity is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setPublishRateRingCapacity(300);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'publish_rate_ring_capacity_revised' }).length,
    1,
  );
  bus.setPublishRateRingCapacity(300);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'publish_rate_ring_capacity_revised' }).length,
    1,
  );
  assert.equal(bus.getPublishRateRingCapacity(), 300);
});

test('audit pins capacity/oldCapacity exactly and records the actor', () => {
  const bus = new EventBus();
  bus.setPublishRateRingCapacity(300, 'ops');
  bus.setPublishRateRingCapacity(1200, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'publish_rate_ring_capacity_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { capacity: 300, oldCapacity: DEFAULT_RATE_RING_CAPACITY });
  assert.deepEqual(audit[1].detail, { capacity: 1200, oldCapacity: 300 });
  assert.equal(audit[0].actor, 'ops');
  assert.equal(audit[1].actor, 'ops');
});

test('getter follows the live capacity across revisions', () => {
  const bus = new EventBus();
  assert.equal(bus.getPublishRateRingCapacity(), 60_000);
  bus.setPublishRateRingCapacity(300);
  assert.equal(bus.getPublishRateRingCapacity(), 300);
  bus.setPublishRateRingCapacity(1200);
  assert.equal(bus.getPublishRateRingCapacity(), 1200);
  bus.setPublishRateRingCapacity(60);
  assert.equal(bus.getPublishRateRingCapacity(), 60);
});

test('a topic created after the revision builds its ring at the new capacity', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setPublishRateRingCapacity(300);
  publishN(bus, 'fresh.a', 600);
  assert.deepEqual(ratesOf(bus.getStats(), 'fresh.a'), { r1s: 300, r1m: 5, r5m: 1 });
  // And a second new topic gets the same live capacity, not the default.
  publishN(bus, 'fresh.b', 600);
  assert.deepEqual(ratesOf(bus.getStats(), 'fresh.b'), { r1s: 300, r1m: 5, r5m: 1 });
});

test('hotTopics ordering follows the revised capacity', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  publishN(bus, 'zeta', 600);
  publishN(bus, 'alpha', 500);
  const before = bus.getStats().hotTopics.map((h) => h.topic);
  assert.deepEqual(before, ['zeta', 'alpha']);
  assert.equal(bus.getStats().hotTopics[0].r1m, 10);
  assert.equal(bus.getStats().hotTopics[1].r1m, 500 / 60);
  // Tightening to 400 caps both topics at 400 samples in the minute, so
  // the tie breaks on topic name and the ranking flips.
  bus.setPublishRateRingCapacity(400);
  const after = bus.getStats().hotTopics;
  assert.deepEqual(
    after.map((h) => h.topic),
    ['alpha', 'zeta'],
  );
  assert.equal(after[0].r1m, 400 / 60);
  assert.equal(after[1].r1m, 400 / 60);
});

test('interleaved revisions and publishes lose no sample and count none twice', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  // Simulate the ring's specified semantics step by step: a revision
  // keeps min(kept, capacity) samples, and each publish adds one sample
  // but the ring never holds more than its live capacity.
  let kept = 0;
  for (let i = 0; i < 20; i += 1) {
    const capacity = i % 2 === 0 ? 50 : 500;
    bus.setPublishRateRingCapacity(capacity);
    kept = Math.min(kept, capacity);
    publishN(bus, 'race.a', 10);
    kept = Math.min(kept + 10, capacity);
    assert.equal(ratesOf(bus.getStats(), 'race.a')?.r1s, kept, `step ${i}`);
    assert.ok(ratesOf(bus.getStats(), 'race.a')!.r1s <= capacity, `step ${i}`);
  }
  bus.setPublishRateRingCapacity(500);
  assert.equal(ratesOf(bus.getStats(), 'race.a')?.r1s, kept);
  assert.equal(ratesOf(bus.getStats(), 'race.a')?.r1m, kept / 60);
  assert.equal(ratesOf(bus.getStats(), 'race.a')?.r5m, kept / 300);
});
