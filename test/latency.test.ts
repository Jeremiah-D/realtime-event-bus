import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { DeliveryLatencyTracker } from '../src/latency.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('tracker records enqueue→delivery latency with nearest-rank percentiles', () => {
  const tracker = new DeliveryLatencyTracker();
  for (const ms of [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]) tracker.record(ms);
  const s = tracker.summary();
  assert.equal(s.samples, 10);
  // Nearest-rank: p50 -> rank ceil(5)-1 = 4 -> 50; p95/p99 -> rank 9 -> 100.
  assert.equal(s.p50Ms, 50);
  assert.equal(s.p95Ms, 100);
  assert.equal(s.p99Ms, 100);
  assert.equal(s.minMs, 10);
  assert.equal(s.maxMs, 100);
  assert.equal(s.meanMs, 55);
});

test('tracker evicts the oldest samples past the window and clamps negatives', () => {
  const tracker = new DeliveryLatencyTracker(4);
  for (const ms of [10, 20, 30, 40, 50, 60]) tracker.record(ms);
  const s = tracker.summary();
  assert.equal(s.samples, 4);
  assert.equal(s.minMs, 30); // 10 and 20 were evicted
  assert.equal(s.maxMs, 60);
  assert.equal(s.meanMs, 45);
  // Clock skew (delivery reading before the enqueue stamp) clamps at 0.
  tracker.record(-5);
  assert.equal(tracker.summary().minMs, 0);
  assert.equal(tracker.summary().samples, 4); // still bounded
});

test('tracker rejects a non-positive or non-integer window', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => new DeliveryLatencyTracker(bad), RangeError);
  }
});

test('invalid windowSize throws RangeError from subscribe', () => {
  const bus = new EventBus();
  for (const bad of [0, -3, 2.5]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { deliveryLatency: { windowSize: bad } }),
      RangeError,
    );
  }
  // The failed subscribes left no half-registered subscribers behind.
  assert.equal(bus.subscriberCount(), 0);
});

test('deliveryLatency samples enqueue→delivery dwell with an injected clock', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { deliveryLatency: true });
  bus.publish('t', 'hello'); // enqueued at t=1000
  clock.nowMs += 15;
  await flush(); // delivered at t=1015
  const stats = bus.getStats();
  assert.equal(stats.deliveryLatency.length, 1);
  const entry = stats.deliveryLatency[0];
  assert.match(entry.subscriberId, /^sub-\d+$/);
  assert.equal(entry.pattern, 't');
  assert.equal(entry.samples, 1);
  assert.equal(entry.p50Ms, 15);
  assert.equal(entry.p95Ms, 15);
  assert.equal(entry.p99Ms, 15);
  assert.equal(entry.minMs, 15);
  assert.equal(entry.maxMs, 15);
  assert.equal(entry.meanMs, 15);
});

test('latency is zero when publish and flush share the clock tick', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { deliveryLatency: true });
  bus.publish('t', 1);
  await flush();
  const entry = bus.getStats().deliveryLatency[0];
  assert.equal(entry.samples, 1);
  assert.equal(entry.p99Ms, 0);
});

test('percentiles summarize the rolling window with deterministic clocks', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { deliveryLatency: true });
  // Publish 10 messages, 10ms apart by the injected clock.
  for (let i = 0; i < 10; i += 1) {
    bus.publish('t', i);
    clock.nowMs += 10;
  }
  clock.nowMs = 2000; // deliver everything at t=2000
  await flush();
  // Latencies: 2000 - (1000 + i*10) for i=0..9 -> 1000, 990, ..., 910.
  const entry = bus.getStats().deliveryLatency[0];
  assert.equal(entry.samples, 10);
  assert.equal(entry.p50Ms, 950); // rank ceil(5)-1 = 4 -> sorted[4]
  assert.equal(entry.p95Ms, 1000);
  assert.equal(entry.p99Ms, 1000);
  assert.equal(entry.minMs, 910);
  assert.equal(entry.maxMs, 1000);
  assert.equal(entry.meanMs, 955);
});

test('untracked subscribers add no series and cost no sampling', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('tracked', () => {}, { deliveryLatency: true });
  bus.subscribe('untracked', () => {});
  clock.nowMs += 50;
  bus.publish('tracked', 1);
  bus.publish('untracked', 2);
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.deliveryLatency.length, 1);
  assert.equal(stats.deliveryLatency[0].pattern, 'tracked');
});

test('slowestSubscribers ranks tracked subscribers by p99', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('slow', () => {}, { deliveryLatency: true });
  bus.subscribe('fast', () => {}, { deliveryLatency: true });
  bus.publish('slow', 1); // enqueued at t=1000
  clock.nowMs += 50;
  bus.publish('fast', 2); // enqueued at t=1050
  clock.nowMs += 50; // flush at t=1100
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.slowestSubscribers.length, 2);
  assert.equal(stats.slowestSubscribers[0].pattern, 'slow');
  assert.equal(stats.slowestSubscribers[0].p99Ms, 100);
  assert.equal(stats.slowestSubscribers[1].pattern, 'fast');
  assert.equal(stats.slowestSubscribers[1].p99Ms, 50);
});

test('dropped and expired messages are never sampled', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { deliveryLatency: true, queueSize: 2 });
  // 5 publishes into a capacity-2 queue: drop-oldest sheds the first 3
  // before the first flush; only the last 2 are ever delivered.
  for (let i = 0; i < 5; i += 1) bus.publish('t', i);
  clock.nowMs += 25;
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.droppedMessages, 3);
  assert.equal(stats.deliveredMessages, 2);
  assert.equal(stats.deliveryLatency[0].samples, 2);
});

test('reliable redeliveries sample each delivery separately', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let deliveries = 0;
  bus.subscribeReliable(
    't',
    (delivery) => {
      deliveries += 1;
      if (deliveries === 1) delivery.nack();
      else delivery.ack();
    },
    { deliveryLatency: true },
  );
  bus.publish('t', 'x'); // enqueued at t=1000
  clock.nowMs += 40;
  // The nack is synchronous inside the handler, so the redelivery flush
  // runs in the same microtask drain at the same clock reading: the
  // redelivery re-stamps at t=1040 and is delivered at t=1040.
  await flush();
  assert.equal(deliveries, 2);
  const entry = bus.getStats().deliveryLatency[0];
  assert.equal(entry.samples, 2);
  // First delivery dwelled 40ms; the redelivery re-stamped on requeue, so
  // it samples its own dwell (0) instead of the stale 40ms stamp.
  assert.equal(entry.minMs, 0);
  assert.equal(entry.maxMs, 40);
  assert.equal(entry.meanMs, 20);
});

test('health-probed subscribers are sampled on the health delivery path', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { deliveryLatency: true, healthProbe: true });
  bus.publish('t', 1);
  clock.nowMs += 33;
  await flush();
  const entry = bus.getStats().deliveryLatency[0];
  assert.equal(entry.samples, 1);
  assert.equal(entry.p99Ms, 33);
});

test('exposition renders per-subscriber quantile gauges and the samples gauge', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('orders.*', () => {}, { deliveryLatency: true });
  bus.publish('orders.new', 1);
  clock.nowMs += 42;
  await flush();
  const text = renderPrometheus(bus.getStats());
  const labels = `subscriber="${sub.id}",pattern="orders.*"`;
  assert.ok(
    text.includes(`eventbus_delivery_latency_ms{quantile="0.5",${labels}} 42`),
    'p50 gauge missing',
  );
  assert.ok(
    text.includes(`eventbus_delivery_latency_ms{quantile="0.95",${labels}} 42`),
    'p95 gauge missing',
  );
  assert.ok(
    text.includes(`eventbus_delivery_latency_ms{quantile="0.99",${labels}} 42`),
    'p99 gauge missing',
  );
  assert.ok(
    text.includes(`eventbus_delivery_latency_samples{${labels}} 1`),
    'samples gauge missing',
  );
  // An untracked subscriber adds no latency series.
  const plain = new EventBus();
  plain.subscribe('t', () => {});
  const plainText = renderPrometheus(plain.getStats());
  assert.ok(!plainText.includes('eventbus_delivery_latency_ms{'));
});
