import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type ThrottleEvent } from '../src/bus.ts';
import { TokenBucket } from '../src/throttle.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so throttle engagement windows and drain-rate measurements are
 * deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('TokenBucket consumes tokens and refills lazily with the injected clock', () => {
  const clock = controllableClock();
  const bucket = new TokenBucket(2, 4, clock.now); // 2 burst, 4/sec
  assert.equal(bucket.take(), true);
  assert.equal(bucket.take(), true);
  assert.equal(bucket.take(), false); // empty: no token consumed
  clock.nowMs += 250; // 1 token refilled
  assert.equal(bucket.take(), true);
  assert.equal(bucket.take(), false);
  clock.nowMs += 10_000; // capped at capacity, not unbounded
  assert.equal(bucket.take(), true);
  assert.equal(bucket.take(), true);
  assert.equal(bucket.take(), false);
});

test('TokenBucket validates its constructor arguments', () => {
  assert.throws(() => new TokenBucket(0, 1), RangeError);
  assert.throws(() => new TokenBucket(1, 0), RangeError);
  assert.throws(() => new TokenBucket(1, -5), RangeError);
  assert.throws(() => new TokenBucket(1, Infinity), RangeError);
});

test('throttle engages on backpressure with the initial rate', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => events.push(e),
  });
  // Default initial rate: one high-water-mark worth per second = 10 * 0.8 = 8/s.
  for (let i = 0; i < 8; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].ratePerSec, 8);
  assert.equal(events[0].adapted, false);
  assert.equal(events[0].queueSize, 8);
  assert.equal(events[0].capacity, 10);
  assert.equal(bus.getStats().throttledSubscribers, 1);
});

test('throttled publishes are shed at the publish side and counted', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
  });
  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // hits HWM, engages
  assert.equal(bus.getStats().throttledSubscribers, 1);
  // Bucket holds 2 tokens: 2 pass, 3 are shed without touching the queue.
  const accepted: number[] = [];
  for (let i = 0; i < 5; i += 1) accepted.push(bus.publish('t', `extra-${i}`));
  assert.deepEqual(accepted, [1, 1, 0, 0, 0]);
  assert.equal(bus.throttledCount(sub.id), 3);
  assert.equal(bus.pendingCount(sub.id), 82); // 80 + 2 passed the bucket
  assert.equal(bus.droppedCount(sub.id), 0); // shed by throttle, not the queue
});

test('drain disengages the throttle and the next engagement adapts to the measured drain rate', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
    onThrottled: (e) => events.push(e),
  });
  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // engage at t=1000
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 2);
  assert.equal(events[0].adapted, false);

  clock.nowMs = 3_000; // 2s later the consumer drains everything
  await flush();
  assert.equal(received.length, 80);
  assert.equal(bus.getStats().throttledSubscribers, 0);

  // Full speed resumes immediately: no shedding while unthrottled.
  for (let i = 0; i < 10; i += 1) assert.equal(bus.publish('t', `free-${i}`), 1);
  await flush();
  assert.equal(received.length, 90);

  // Second excursion: 80 messages drained over 2000ms = 40/s observed,
  // so the throttle re-engages at the measured consumer speed.
  for (let i = 0; i < 80; i += 1) bus.publish('t', `round2-${i}`);
  assert.equal(events.length, 2);
  assert.equal(events[1].ratePerSec, 40);
  assert.equal(events[1].adapted, true);
  assert.equal(bus.getStats().throttledSubscribers, 1);
});

test('throttle engages and disengages without user backpressure/drained callbacks', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const received: unknown[] = [];
  // No onBackpressure / onDrained: the bus still needs the excursion signals
  // internally to drive the throttle.
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => events.push(e),
  });
  for (let i = 0; i < 8; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().throttledSubscribers, 1);
  await flush();
  assert.equal(bus.getStats().throttledSubscribers, 0);
});

test('invalid throttle options throw RangeError from subscribe', () => {
  const bus = new EventBus();
  const noop = () => {};
  assert.throws(() => bus.subscribe('t', noop, { throttle: { minRatePerSec: 0 } }), RangeError);
  assert.throws(
    () => bus.subscribe('t', noop, { throttle: { initialRatePerSec: -1 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', noop, { throttle: { maxRatePerSec: 0 } }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribe('t', noop, { throttle: { minRatePerSec: 10, maxRatePerSec: 5 } }),
    RangeError,
  );
  // ... while valid bounds are accepted.
  bus.subscribe('t', noop, {
    throttle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 5 },
  }).unsubscribe();
});

test('throttling is disabled by default: no events, no sheds, zero stats', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  let throttledFired = false;
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 10,
    onThrottled: () => {
      throttledFired = true;
    },
  });
  for (let i = 0; i < 10; i += 1) bus.publish('t', i); // crosses HWM
  await flush();
  assert.equal(throttledFired, false);
  assert.equal(bus.getStats().throttledSubscribers, 0);
  assert.equal(bus.throttledCount(sub.id), 0);
});

test('unsubscribing a throttled subscriber clears it from the stats', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { queueSize: 10, throttle: true });
  for (let i = 0; i < 8; i += 1) bus.publish('t', i);
  assert.equal(bus.getStats().throttledSubscribers, 1);
  sub.unsubscribe();
  assert.equal(bus.getStats().throttledSubscribers, 0);
});

test('throttled subscriber keeps working with reliable (at-least-once) delivery', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const acked: unknown[] = [];
  const sub = bus.subscribeReliable(
    't',
    (delivery) => {
      acked.push(delivery.msg.payload);
      delivery.ack();
    },
    { queueSize: 10, throttle: true },
  );
  for (let i = 0; i < 8; i += 1) bus.publish('t', i); // engage
  assert.equal(bus.getStats().throttledSubscribers, 1);
  await flush();
  assert.equal(acked.length, 8);
  assert.equal(bus.getStats().throttledSubscribers, 0);
  assert.equal(bus.throttledCount(sub.id), 0); // nothing shed: only 8 published
});
