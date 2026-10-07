import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type DegradedEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`.
 * A "slow" handler simulates overrunning its processing budget by moving
 * the clock forward itself, keeping the timeout tests deterministic.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('consecutive handler errors auto-pause delivery after the threshold', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  let calls = 0;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 3 }, onDegraded: (e) => events.push(e) },
  );

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();

  assert.equal(calls, 3);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].consecutiveFailures, 3);
  assert.equal(events[0].reason, 'error');
  assert.equal(events[0].pendingMessages, 0);
  assert.equal(bus.getStats().degradedSubscribers, 1);
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: true,
    consecutiveFailures: 3,
  });

  // Paused: new publishes stay queued (preserved, not dropped), none delivered.
  bus.publish('t', 'd');
  bus.publish('t', 'e');
  await flush();
  assert.equal(calls, 3);
  assert.equal(events.length, 1); // no repeat event while already degraded
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 0);
});

test('processing timeouts count toward the failure threshold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: DegradedEvent[] = [];
  bus.subscribe(
    't',
    () => {
      clock.nowMs += 100; // overruns the 50ms processing budget
    },
    {
      healthProbe: { maxConsecutiveFailures: 2, processingTimeoutMs: 50 },
      onDegraded: (e) => events.push(e),
    },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'timeout');
  assert.equal(events[0].consecutiveFailures, 2);
  assert.equal(bus.getStats().degradedSubscribers, 1);
});

test('a successful delivery resets the consecutive-failure counter', async () => {
  const bus = new EventBus();
  let failures = 2; // throw twice, then behave
  const sub = bus.subscribe(
    't',
    () => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('boom');
      }
    },
    { healthProbe: { maxConsecutiveFailures: 3 } },
  );
  for (let i = 0; i < 3; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 0);
  assert.equal(bus.subscriberHealth(sub.id).degraded, false);

  // Two fresh failures are still below the threshold of 3.
  failures = 2;
  for (let i = 0; i < 2; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 2);
  assert.equal(bus.subscriberHealth(sub.id).degraded, false);
});

test('a degradation mid-drain requeues the unattempted messages in order', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  let failures = 3;
  const sub = bus.subscribe(
    't',
    (msg) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('boom');
      }
      received.push(msg.payload);
    },
    { healthProbe: { maxConsecutiveFailures: 3 } },
  );
  for (let i = 0; i < 5; i += 1) bus.publish('t', i); // one flush drains all five
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);
  assert.deepEqual(received, []); // the 3 failures consumed their messages; 3 and 4 were requeued
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 0);

  failures = 0;
  assert.equal(bus.resume(sub.id), true);
  await flush();
  assert.deepEqual(received, [3, 4]); // redelivered in original order
  assert.equal(bus.getStats().degradedSubscribers, 0);
});

test('manual resume redelivers the preserved backlog in order', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  let failures = 2;
  const sub = bus.subscribe(
    't',
    (msg) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('boom');
      }
      received.push(msg.payload);
    },
    { healthProbe: { maxConsecutiveFailures: 2 } },
  );
  bus.publish('t', 'a'); // fails
  bus.publish('t', 'b'); // fails -> degraded
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);

  bus.publish('t', 'c');
  bus.publish('t', 'd');
  bus.publish('t', 'e');
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.pendingCount(sub.id), 3);

  assert.equal(bus.resume(sub.id), true);
  await flush();
  assert.deepEqual(received, ['c', 'd', 'e']); // FIFO preserved across the pause
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 0);
  assert.equal(bus.resume(sub.id), false); // already healthy: no-op
  assert.throws(() => bus.resume('nope'), /unknown subscriber/);
});

test('auto-resume after the cooldown delivers the preserved backlog', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  let failures = 1;
  const sub = bus.subscribe(
    't',
    (msg) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('boom');
      }
      received.push(msg.payload);
    },
    { healthProbe: { maxConsecutiveFailures: 1, autoResumeAfterMs: 30 } },
  );
  bus.publish('t', 'a'); // fails -> degraded
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);
  bus.publish('t', 'b'); // queued while degraded
  await sleep(80); // cooldown elapses -> auto-resume (the timer is unref'd)
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.deepEqual(received, ['b']);
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 0);
});

test('onDegraded fires once per degradation excursion', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  let failures = 1;
  const sub = bus.subscribe(
    't',
    () => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('boom');
      }
    },
    { healthProbe: { maxConsecutiveFailures: 1 }, onDegraded: (e) => events.push(e) },
  );
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1);

  // While degraded, nothing is delivered, so no new failures accumulate.
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  assert.equal(events.length, 1);

  // After resume, the next failure trips a fresh degradation.
  assert.equal(bus.resume(sub.id), true);
  await flush(); // delivers b and c successfully
  failures = 1;
  bus.publish('t', 'd');
  await flush();
  assert.equal(events.length, 2);
  assert.equal(events[1].consecutiveFailures, 1);
});

test('stats expose degraded subscribers; health reads per-subscriber state', async () => {
  const bus = new EventBus();
  const probed = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 1 } },
  );
  const plain = bus.subscribe('t', () => {});
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.deepEqual(bus.subscriberHealth(plain.id), {
    subscriberId: plain.id,
    enabled: false,
    degraded: false,
    consecutiveFailures: 0,
  });
  bus.publish('t', 'x');
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);
  assert.equal(bus.subscriberHealth(probed.id).degraded, true);
  assert.throws(() => bus.subscriberHealth('nope'), /unknown subscriber/);
  // Unsubscribing clears the degraded count.
  probed.unsubscribe();
  assert.equal(bus.getStats().degradedSubscribers, 0);
});

test('invalid health probe options throw RangeError from subscribe', () => {
  const bus = new EventBus();
  const noop = () => {};
  assert.throws(
    () => bus.subscribe('t', noop, { healthProbe: { maxConsecutiveFailures: 0 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', noop, { healthProbe: { maxConsecutiveFailures: 2.5 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', noop, { healthProbe: { processingTimeoutMs: 0 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', noop, { healthProbe: { processingTimeoutMs: -10 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', noop, { healthProbe: { processingTimeoutMs: Infinity } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', noop, { healthProbe: { autoResumeAfterMs: -1 } }),
    RangeError,
  );
  // A failed validation leaves no half-registered subscriber behind.
  const before = bus.subscriberCount();
  assert.throws(
    () => bus.subscribe('t', noop, { healthProbe: { maxConsecutiveFailures: 0 } }),
    RangeError,
  );
  assert.equal(bus.subscriberCount(), before);
  // ... while valid bounds are accepted.
  bus.subscribe('t', noop, { healthProbe: true }).unsubscribe();
  bus
    .subscribe('t', noop, {
      healthProbe: { maxConsecutiveFailures: 1, processingTimeoutMs: 10, autoResumeAfterMs: 100 },
    })
    .unsubscribe();
});

test('a failing probed subscriber does not abort delivery to other subscribers', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'hello');
  await flush();
  // Without the probe the throw would propagate out of the drain and skip
  // every subscriber after it; the probe contains the failure.
  assert.deepEqual(received, ['hello']);
});

test('health probing works alongside reliable (at-least-once) delivery', async () => {
  const bus = new EventBus();
  const acked: unknown[] = [];
  let failures = 1;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      acked.push(d.msg.payload);
      d.ack();
      if (failures > 0) {
        failures -= 1;
        throw new Error('boom after ack');
      }
    },
    { healthProbe: { maxConsecutiveFailures: 2 } },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(acked, ['a', 'b']);
  assert.equal(bus.unackedCount(sub.id), 0);
  // One failure, then a success: the counter reset, no pause.
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 0);
  assert.equal(bus.subscriberHealth(sub.id).degraded, false);
});

test('unsubscribing a degraded subscriber clears its auto-resume timer', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 1, autoResumeAfterMs: 30 } },
  );
  bus.publish('t', 'a');
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);
  sub.unsubscribe();
  await sleep(80); // the cleared timer must not fire a phantom resume
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.equal(bus.subscriberCount(), 0);
});
