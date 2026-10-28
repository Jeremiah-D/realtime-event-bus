import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type ThrottleEvent } from '../src/bus.ts';

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

test('setThrottle before the first engagement uses the revised rates', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
    onThrottled: (e) => events.push(e),
  });

  bus.setThrottle(sub.id, { initialRatePerSec: 7, minRatePerSec: 3 });
  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // hits the high-water mark
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 7, 'the revised initial rate engages, not the subscribe-time one');
  assert.equal(events[0].adapted, false);

  const audit = bus.getSubscriptionAudit({ action: 'throttle_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    throttle: { minRatePerSec: 3, maxRatePerSec: Infinity, initialRatePerSec: 7 },
    oldThrottle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 2 },
  });
  sub.unsubscribe();
});

test('setThrottle during an excursion keeps the live bucket and the drop count', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // engages at 2/s
  assert.equal(bus.getStats().throttledSubscribers, 1);
  // The engagement bucket holds 2 tokens: 2 pass, then it is empty.
  const first: number[] = [];
  for (let i = 0; i < 2; i += 1) first.push(bus.publish('t', `extra-${i}`));
  assert.deepEqual(first, [1, 1]);
  assert.equal(bus.throttledCount(sub.id), 0);

  bus.setThrottle(sub.id, { initialRatePerSec: 100, minRatePerSec: 1, maxRatePerSec: 1000 });
  // A retuned bucket would admit these; the live bucket does not.
  const shed: number[] = [];
  for (let i = 0; i < 3; i += 1) shed.push(bus.publish('t', `shed-${i}`));
  assert.deepEqual(shed, [0, 0, 0], 'the current excursion keeps its original bucket');
  assert.equal(bus.throttledCount(sub.id), 3, 'sheds during the excursion still count');
  assert.equal(bus.pendingCount(sub.id), 82, 'the queue is untouched by the revision');
  assert.equal(bus.getStats().throttledSubscribers, 1, 'still throttled until the excursion ends');

  const audit = bus.getSubscriptionAudit({ action: 'throttle_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    throttle: { minRatePerSec: 1, maxRatePerSec: 1000, initialRatePerSec: 100 },
    oldThrottle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 2 },
  });
  sub.unsubscribe();
});

test('setThrottle applies revised bounds from the next engagement', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
    onThrottled: (e) => events.push(e),
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // engage at t=1000
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 2);

  bus.setThrottle(sub.id, { maxRatePerSec: 10 });
  clock.nowMs = 3_000; // 2s later the consumer drains everything
  await flush();
  assert.equal(received.length, 80);
  assert.equal(bus.getStats().throttledSubscribers, 0);

  // 80 messages drained over 2000ms = 40/s observed, clamped to the
  // revised ceiling of 10/s when the excursion ended.
  for (let i = 0; i < 80; i += 1) bus.publish('t', `round2-${i}`);
  assert.equal(events.length, 2);
  assert.equal(events[1].ratePerSec, 10, 'the next engagement uses the revised ceiling');
  assert.equal(events[1].adapted, true);

  const audit = bus.getSubscriptionAudit({ action: 'throttle_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    throttle: { minRatePerSec: 1, maxRatePerSec: 10, initialRatePerSec: 2 },
    oldThrottle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 2 },
  });
  sub.unsubscribe();
});

test('setThrottle keeps omitted rates and clamps the stored initial at engagement', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1, maxRatePerSec: 5 },
    onThrottled: (e) => events.push(e),
  });

  bus.setThrottle(sub.id, { minRatePerSec: 3 });
  const audit = bus.getSubscriptionAudit({ action: 'throttle_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    throttle: { minRatePerSec: 3, maxRatePerSec: 5, initialRatePerSec: 2 },
    oldThrottle: { minRatePerSec: 1, maxRatePerSec: 5, initialRatePerSec: 2 },
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(
    events[0].ratePerSec,
    3,
    'the stored initial below the raised floor is clamped when it is used',
  );
  sub.unsubscribe();
});

test('setThrottle is fail-closed for unknown, unthrottled, or illegal values', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const throttled = bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
    onThrottled: (e) => events.push(e),
  });
  const plain = bus.subscribe('u', () => {});

  assert.throws(() => bus.setThrottle('nope', { minRatePerSec: 1 }), RangeError);
  assert.throws(() => bus.setThrottle(plain.id, { minRatePerSec: 1 }), RangeError);
  assert.throws(() => bus.setThrottle('', { minRatePerSec: 1 }), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, { minRatePerSec: 0 }), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, { minRatePerSec: -1 }), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, { minRatePerSec: Number.NaN }), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, { maxRatePerSec: 0 }), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, { maxRatePerSec: -5 }), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, { maxRatePerSec: Number.NaN }), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, { initialRatePerSec: 0 }), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, { initialRatePerSec: Infinity }), RangeError);
  assert.throws(
    () => bus.setThrottle(throttled.id, { minRatePerSec: 10, maxRatePerSec: 5 }),
    RangeError,
  );
  assert.throws(() => bus.setThrottle(throttled.id, undefined as never), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, 'x' as never), RangeError);
  assert.throws(() => bus.setThrottle(throttled.id, [] as never), RangeError);

  assert.equal(bus.getSubscriptionAudit({ action: 'throttle_revised' }).length, 0);
  // The failed revisions moved nothing: the original rates still engage.
  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 2);
  throttled.unsubscribe();
  plain.unsubscribe();
});

test('setThrottle null clears the throttling and refuses later revisions', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // engages
  assert.equal(bus.getStats().throttledSubscribers, 1);

  bus.setThrottle(sub.id, null);
  assert.equal(bus.getStats().throttledSubscribers, 0, 'the publish-side gate is gone');

  const accepted: number[] = [];
  for (let i = 0; i < 10; i += 1) accepted.push(bus.publish('t', `free-${i}`));
  assert.deepEqual(accepted, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1], 'full speed resumes immediately');
  assert.equal(bus.pendingCount(sub.id), 90);

  assert.throws(() => bus.setThrottle(sub.id, { minRatePerSec: 1 }), RangeError);

  const audit = bus.getSubscriptionAudit({ action: 'throttle_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    throttle: null,
    oldThrottle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 2 },
  });
  await flush();
  sub.unsubscribe();
});
