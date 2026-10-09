import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type AckSloMiss, type Delivery } from '../src/bus.ts';
import { AckLatencyTracker } from '../src/acklatency.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

// --- Tracker unit tests: deterministic via an injected clock. ---

test('tracker pins nearest-rank percentiles with an injected clock', () => {
  const clock = controllableClock(10_000);
  const tracker = new AckLatencyTracker({ now: clock.now });
  const keys = Array.from({ length: 10 }, () => ({}));
  keys.forEach((k, i) => {
    tracker.accepted(k);
    clock.nowMs += (i + 1) * 10; // latencies 10..100, but the clock only moves forward...
    tracker.sampleOnAck(k);
  });
  // accepted() stamps at call time, so samples are 10,20,...,100.
  const s = tracker.summary();
  assert.equal(s.samples, 10);
  assert.equal(s.p50Ms, 50); // nearest-rank: ceil(0.5*10)-1 = 4 -> 50
  assert.equal(s.p95Ms, 100);
  assert.equal(s.p99Ms, 100);
  assert.equal(s.minMs, 10);
  assert.equal(s.maxMs, 100);
  assert.equal(s.meanMs, 55);
  // No SLO configured: everything counts as within SLO.
  assert.equal(s.withinSlo, 10);
  assert.equal(s.sloAttainment, 1);
});

test('tracker restarts the accept clock without clearing the sampled flag', () => {
  const clock = controllableClock(1_000);
  const tracker = new AckLatencyTracker({ now: clock.now });
  const key = {};
  tracker.accepted(key); // t=1000
  clock.nowMs = 1_200;
  tracker.accepted(key); // redelivery restarts the clock
  clock.nowMs = 1_250;
  assert.equal(tracker.sampleOnAck(key), 50);
  // A second ack for the same message records nothing.
  clock.nowMs = 9_999;
  assert.equal(tracker.sampleOnAck(key), undefined);
  assert.equal(tracker.summary().samples, 1);
});

test('tracker ignores samples for never-accepted messages', () => {
  const tracker = new AckLatencyTracker();
  assert.equal(tracker.sampleOnAck({}), undefined);
  assert.equal(tracker.summary().samples, 0);
});

test('tracker clamps backward-clock samples at 0', () => {
  const clock = controllableClock(2_000);
  const tracker = new AckLatencyTracker({ now: clock.now });
  const key = {};
  tracker.accepted(key);
  clock.nowMs = 1_900; // clock moved backwards before the ack
  assert.equal(tracker.sampleOnAck(key), 0);
  assert.equal(tracker.summary().minMs, 0);
});

test('tracker evicts the oldest samples past the window and adjusts SLO counts', () => {
  const clock = controllableClock(0);
  const misses: number[] = [];
  const tracker = new AckLatencyTracker({
    windowSize: 4,
    sloMs: 100,
    now: clock.now,
    onSloMiss: (e) => misses.push(e.latencyMs),
  });
  // Samples: 10 (ok), 20 (ok), 200 (miss), 30 (ok), 300 (miss) -> window keeps 20,200,30,300.
  for (const ms of [10, 20, 200, 30, 300]) {
    const key = {};
    tracker.accepted(key);
    clock.nowMs += ms;
    tracker.sampleOnAck(key);
  }
  const s = tracker.summary();
  assert.equal(s.samples, 4);
  assert.equal(s.minMs, 20); // 10 was evicted
  assert.equal(s.maxMs, 300);
  assert.equal(s.withinSlo, 2); // 20 and 30; the evicted 10 was within SLO too
  assert.equal(s.sloAttainment, 0.5);
  assert.deepEqual(misses, [200, 300]);
  // Empty-window summary is all zeros.
  assert.deepEqual(new AckLatencyTracker().summary(), {
    samples: 0,
    p50Ms: 0,
    p95Ms: 0,
    p99Ms: 0,
    minMs: 0,
    maxMs: 0,
    meanMs: 0,
    withinSlo: 0,
    sloAttainment: 0,
  });
});

test('tracker rejects invalid window and SLO values', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => new AckLatencyTracker({ windowSize: bad }), RangeError);
  }
  // sloMs need not be an integer, but must be positive and finite.
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => new AckLatencyTracker({ sloMs: bad }), RangeError);
  }
});

// --- Option validation at subscribe time. ---

test('invalid ackLatency options throw from subscribe with no half-registration', () => {
  const bus = new EventBus();
  for (const bad of [0, -3, 2.5]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { ackLatency: { windowSize: bad } }),
      RangeError,
    );
  }
  for (const bad of [0, -100, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => bus.subscribeReliable('t', (d) => d.ack(), { ackLatency: true, ackSloMs: bad }),
      RangeError,
    );
  }
  // SLO options without the opt-in are dead config: rejected.
  assert.throws(
    () => bus.subscribe('t', () => {}, { ackSloMs: 100 }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { onAckSloMiss: () => {} }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribeReliable('t', (d) => d.ack(), {
        ackLatency: true,
        onAckSloMiss: 'nope' as unknown as (e: AckSloMiss) => void,
      }),
    TypeError,
  );
  assert.equal(bus.subscriberCount(), 0);
});

// --- Bus integration: sampling, redelivery, SLO, exposition. ---

test('reliable ack samples accepted→ack latency into getStats().ackLatency', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const seen: Array<Delivery<unknown>> = [];
  bus.subscribeReliable('orders', (d) => {
    seen.push(d as Delivery<unknown>);
  }, { ackLatency: true });
  bus.publish('orders', 'a'); // accepted at t=1000
  await flush(); // handed to the handler at t=1000
  assert.equal(seen[0].acceptedAtMs, 1_000); // Delivery carries the bus-clock stamp
  clock.nowMs = 1_250;
  seen[0].ack(); // sample = 250
  const entries = bus.getStats().ackLatency;
  assert.equal(entries.length, 1);
  const s = entries[0];
  assert.equal(s.subscriberId, 'sub-1');
  assert.equal(s.pattern, 'orders');
  assert.equal(s.samples, 1);
  assert.equal(s.p50Ms, 250);
  assert.equal(s.p95Ms, 250);
  assert.equal(s.p99Ms, 250);
  assert.equal(s.minMs, 250);
  assert.equal(s.maxMs, 250);
  assert.equal(s.meanMs, 250);
  // Default SLO is 30000ms: the sample is within budget.
  assert.equal(s.withinSlo, 1);
  assert.equal(s.sloAttainment, 1);
});

test('nack redelivery restarts the ack clock but yields one sample per message', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const seen: Array<Delivery<unknown>> = [];
  bus.subscribeReliable('t', (d) => {
    seen.push(d as Delivery<unknown>);
  }, { ackLatency: true });
  bus.publish('t', 'a'); // accepted at t=1000
  await flush(); // first delivery at t=1000
  clock.nowMs = 1_100;
  seen[0].nack(); // requeue restarts the clock at t=1100
  await flush(); // redelivery handed out at t=1100
  assert.equal(seen.length, 2);
  assert.equal(seen[1].acceptedAtMs, 1_100); // the redelivery re-stamps
  assert.equal(seen[1].redeliveries, 1);
  clock.nowMs = 1_150;
  seen[1].ack(); // sample = 1150 - 1100 = 50, not 150
  // Acking the stale first handle afterwards records nothing.
  seen[0].ack();
  // Double-acking the live handle records nothing either.
  seen[1].ack();
  const s = bus.getStats().ackLatency[0];
  assert.equal(s.samples, 1);
  assert.equal(s.p50Ms, 50);
});

test('over-budget ack fires onAckSloMiss with subscriber identity', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const misses: AckSloMiss[] = [];
  bus.subscribeReliable('t', (d) => d.ack(), {
    ackLatency: true,
    ackSloMs: 100,
    onAckSloMiss: (e) => misses.push(e),
  });
  bus.publish('t', 'a'); // accepted at t=1000
  clock.nowMs = 1_300;
  await flush(); // handler acks synchronously at t=1300 -> latency 300 > 100
  assert.equal(misses.length, 1);
  assert.equal(misses[0].latencyMs, 300);
  assert.equal(misses[0].sloMs, 100);
  assert.equal(misses[0].subscriberId, 'sub-1');
  assert.equal(misses[0].pattern, 't');
  assert.equal(misses[0].at, 1_300);
  const s = bus.getStats().ackLatency[0];
  assert.equal(s.samples, 1);
  assert.equal(s.withinSlo, 0);
  assert.equal(s.sloAttainment, 0);
  // A within-budget ack does not fire the callback.
  bus.publish('t', 'b');
  await flush(); // acked at t=1300, accepted at t=1300 -> latency 0
  assert.equal(misses.length, 1);
  const s2 = bus.getStats().ackLatency[0];
  assert.equal(s2.samples, 2);
  assert.equal(s2.withinSlo, 1);
  assert.equal(s2.sloAttainment, 0.5);
});

test('ackLatency and deliveryLatency combined: one sample per tracker, no double sampling', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const seen: Array<Delivery<unknown>> = [];
  bus.subscribeReliable('t', (d) => {
    seen.push(d as Delivery<unknown>);
  }, { ackLatency: true, deliveryLatency: true });
  bus.publish('t', 'a');
  clock.nowMs = 1_040;
  await flush(); // delivery-latency samples the 40ms queue dwell
  clock.nowMs = 1_100;
  seen[0].nack(); // redelivery: delivery-latency samples again by design...
  await flush(); // ...at t=1100 with a 0ms dwell
  clock.nowMs = 1_120;
  seen[1].ack(); // ack-latency samples once: 1120 - 1100 = 20
  const stats = bus.getStats();
  assert.equal(stats.deliveryLatency[0].samples, 2); // one per delivery (EB-31 semantics)
  assert.equal(stats.ackLatency[0].samples, 1); // one per message (EB-40 semantics)
  assert.equal(stats.ackLatency[0].p50Ms, 20);
});

test('renderPrometheus emits the ack-latency series only for opted-in subscribers', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.subscribeReliable('tracked', (d) => d.ack(), { ackLatency: true });
  bus.subscribeReliable('plain', (d) => d.ack());
  bus.publish('tracked', 1);
  bus.publish('plain', 2);
  clock.nowMs = 1_050;
  await flush(); // both ack synchronously at t=1050 -> 50ms sample on the tracked one
  const text = renderPrometheus(bus.getStats());
  const sub1 = 'subscriber="sub-1",pattern="tracked"';
  assert.ok(text.includes(`eventbus_ack_latency_ms{quantile="0.5",${sub1}} 50`));
  assert.ok(text.includes(`eventbus_ack_latency_ms{quantile="0.95",${sub1}} 50`));
  assert.ok(text.includes(`eventbus_ack_latency_ms{quantile="0.99",${sub1}} 50`));
  assert.ok(text.includes(`eventbus_ack_latency_samples{${sub1}} 1`));
  // The untracked reliable subscription adds no series.
  assert.ok(!text.includes('pattern="plain"'));

  // A bus with no opted-in subscriber renders no ack-latency sample series.
  const bus2 = new EventBus();
  bus2.subscribe('t', () => {});
  const text2 = renderPrometheus(bus2.getStats());
  assert.ok(!text2.includes('eventbus_ack_latency_ms{'));
  assert.ok(!text2.includes('eventbus_ack_latency_samples{'));
});

test('batched reliable deliveries sample ack latency per message', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const seen: Array<Delivery<unknown>> = [];
  bus.subscribeReliable(
    't',
    (deliveries) => {
      for (const d of deliveries) seen.push(d as Delivery<unknown>);
    },
    { ackLatency: true, batch: { maxSize: 2, maxWaitMs: 10_000 } },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(seen.length, 2);
  clock.nowMs = 1_030;
  seen[0].ack();
  clock.nowMs = 1_070;
  seen[1].ack();
  const s = bus.getStats().ackLatency[0];
  assert.equal(s.samples, 2);
  // Nearest-rank over [30, 70]: p50 -> rank 0 -> 30; p95/p99 -> rank 1 -> 70.
  assert.equal(s.p50Ms, 30);
  assert.equal(s.p95Ms, 70);
  assert.equal(s.meanMs, 50);
});
