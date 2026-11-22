import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type ThrottleEvent } from '../src/bus.ts';

/**
 * Subscriber adaptive-throttle live enable (EB-538):
 * `setThrottle` on a subscription that never opted into `throttle`
 * installs a fresh throttle instead of refusing: `true` takes the
 * subscribe-time built-in rates, an object patch resolves its missing
 * fields through the same chain as a subscribe-time opt-in (explicit >
 * live bus default > built-ins), and `{}` resolves like `true`. The
 * enable also installs the queue's `onHighWaterMark` hook live when
 * the subscription has none — a subscribe-time `onBackpressure` or
 * `throttle` would already have installed it — and flips
 * `backpressureHookInstalled`, so `setBackpressureCallbacks` and
 * `setThrottledCallback` become available on the newly throttled
 * subscriber. A queue already at or above the high-water mark when
 * the enable lands engages immediately: its crossing fired while no
 * hook was installed. `null` on a never-throttled subscriber is a
 * silent no-op with no audit. The enable is live-only: a restart
 * restores the unthrottled state. Audited as `throttle_revised`
 * with `oldThrottle: null`.
 *
 * Determinism tricks (as in `throttle-revision.test.ts`): a
 * manually-advanced clock and a `queueSize: 100` subscription whose
 * high-water mark is 80, so publishing 80 messages engages.
 */

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

const revisionAudits = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'throttle_revised' });

test('EB-538: enabling a never-throttled subscriber engages at the next high-water crossing', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    onThrottled: (e) => events.push(e),
  });

  for (let i = 0; i < 10; i += 1) bus.publish('t', i); // below the mark
  assert.equal(bus.getStats().throttledSubscribers, 0);

  bus.setThrottle(sub.id, { initialRatePerSec: 2, minRatePerSec: 1 });
  assert.equal(bus.getStats().throttledSubscribers, 0, 'enabling alone does not engage');

  for (let i = 0; i < 70; i += 1) bus.publish('t', `fill-${i}`); // hits the mark
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 2);
  assert.equal(events[0].adapted, false);
  assert.equal(bus.getStats().throttledSubscribers, 1);

  // The engagement bucket holds 2 tokens: 2 pass, then it is empty.
  const first: number[] = [];
  for (let i = 0; i < 2; i += 1) first.push(bus.publish('t', `extra-${i}`));
  assert.deepEqual(first, [1, 1]);
  const shed: number[] = [];
  for (let i = 0; i < 3; i += 1) shed.push(bus.publish('t', `shed-${i}`));
  assert.deepEqual(shed, [0, 0, 0]);
  assert.equal(bus.throttledCount(sub.id), 3, 'sheds during the excursion still count');
  assert.equal(bus.pendingCount(sub.id), 82, 'the queue is untouched by the enable');
  sub.unsubscribe();
});

test('EB-538: true enables with the subscribe-time built-in rates, and {} resolves the same way', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const byTrue = bus.subscribe('t', () => {}, {
    queueSize: 100,
    onThrottled: (e) => events.push(e),
  });
  const byEmpty = bus.subscribe('u', () => {}, { queueSize: 100 });

  bus.setThrottle(byTrue.id, true);
  bus.setThrottle(byEmpty.id, {});
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, {
    throttle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 80 },
    oldThrottle: null,
  });
  assert.deepEqual(audit[1].detail, {
    throttle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 80 },
    oldThrottle: null,
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 80, 'true takes one high-water-mark worth per second');
  for (let i = 0; i < 80; i += 1) bus.publish('u', i);
  assert.equal(bus.getStats().throttledSubscribers, 2);
  byTrue.unsubscribe();
  byEmpty.unsubscribe();
});

test('EB-538: an object patch fills omitted fields from the bus default, then the built-ins', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 8 });
  const patched = bus.subscribe('t', () => {}, { queueSize: 100 });
  const bare = bus.subscribe('u', () => {}, { queueSize: 100 });

  bus.setThrottle(patched.id, { initialRatePerSec: 4 });
  bus.setThrottle(bare.id, {});
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, {
    throttle: { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 },
    oldThrottle: null,
  });
  assert.deepEqual(audit[1].detail, {
    // No initial anywhere: the capacity-derived 80 is clamped into the
    // bus-default bounds [2, 8], exactly as a subscribe-time opt-in
    // with the same default would resolve.
    throttle: { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 8 },
    oldThrottle: null,
  });
  patched.unsubscribe();
  bare.unsubscribe();
});

test('EB-538: a bus default never enables throttling by itself', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    onThrottled: (e) => events.push(e),
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(events.length, 0, 'the default fills opt-ins only — it never opts anyone in');
  assert.equal(bus.getStats().throttledSubscribers, 0);

  // The explicit enable still resolves through the default.
  bus.setThrottle(sub.id, {});
  assert.deepEqual(revisionAudits(bus)[0].detail, {
    throttle: { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 },
    oldThrottle: null,
  });
  sub.unsubscribe();
});

test('EB-538: null on a never-throttled subscriber is a silent no-op with no audit', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { queueSize: 100 });

  bus.setThrottle(sub.id, null);
  assert.equal(revisionAudits(bus).length, 0);

  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(bus.getStats().throttledSubscribers, 0, 'still unthrottled after the no-op clear');
  sub.unsubscribe();
});

test('EB-538: a null clear can be followed by a fresh enable', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
    onThrottled: (e) => events.push(e),
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // engages
  assert.equal(bus.getStats().throttledSubscribers, 1);

  bus.setThrottle(sub.id, null);
  assert.equal(bus.getStats().throttledSubscribers, 0);
  for (let i = 0; i < 10; i += 1) bus.publish('t', `free-${i}`);
  assert.equal(bus.pendingCount(sub.id), 90);

  clock.nowMs = 3_000; // drain everything so the next engagement is a fresh crossing
  await flush();
  assert.equal(received.length, 90);
  assert.equal(bus.pendingCount(sub.id), 0);

  bus.setThrottle(sub.id, true);
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    throttle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 80 },
    oldThrottle: null,
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', `round2-${i}`);
  assert.equal(events.length, 2);
  assert.equal(events[1].ratePerSec, 80, 'the fresh enable engages at the built-in initial rate');
  assert.equal(events[1].adapted, false, 'the cleared excursion left no observed rate behind');
  sub.unsubscribe();
});

test('EB-538: a partial revision after the enable still merges the live values', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    onThrottled: (e) => events.push(e),
  });

  bus.setThrottle(sub.id, { initialRatePerSec: 2, minRatePerSec: 1, maxRatePerSec: 5 });
  bus.setThrottle(sub.id, { maxRatePerSec: 10 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    throttle: { minRatePerSec: 1, maxRatePerSec: 10, initialRatePerSec: 2 },
    oldThrottle: { minRatePerSec: 1, maxRatePerSec: 5, initialRatePerSec: 2 },
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 2, 'the enabled initial rate survives the partial revision');
  sub.unsubscribe();
});

test('EB-538: enabling while the queue is already above the mark engages immediately', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    onThrottled: (e) => events.push(e),
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // crosses with no hook installed
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().throttledSubscribers, 0);

  bus.setThrottle(sub.id, { initialRatePerSec: 2, minRatePerSec: 1 });
  assert.equal(events.length, 1, 'the in-flight excursion engages at enable time');
  assert.equal(events[0].queueSize, 80);
  assert.equal(events[0].ratePerSec, 2);
  assert.equal(bus.getStats().throttledSubscribers, 1);

  // The fresh bucket holds 2 tokens: 2 pass, then sheds begin.
  const first: number[] = [];
  for (let i = 0; i < 2; i += 1) first.push(bus.publish('t', `extra-${i}`));
  assert.deepEqual(first, [1, 1]);
  assert.equal(bus.publish('t', 'shed'), 0);
  assert.equal(bus.throttledCount(sub.id), 1);

  // Draining below the mark releases, and the installed hook fires on
  // the next crossing — the enable is not a one-shot.
  clock.nowMs = 3_000;
  await flush();
  assert.equal(received.length, 82);
  assert.equal(bus.getStats().throttledSubscribers, 0);
  for (let i = 0; i < 80; i += 1) bus.publish('t', `round2-${i}`);
  assert.equal(events.length, 2);
  sub.unsubscribe();
});

test('EB-538: an enable releases on drain and adapts from the measured drain rate', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    onThrottled: (e) => events.push(e),
  });

  bus.setThrottle(sub.id, { initialRatePerSec: 2, minRatePerSec: 1, maxRatePerSec: 100 });
  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // engage at t=1000
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 2);

  clock.nowMs = 3_000; // 2s later the consumer drains everything
  await flush();
  assert.equal(received.length, 80);
  assert.equal(bus.getStats().throttledSubscribers, 0, 'the enabled throttle releases on drain');

  // 80 messages drained over 2000ms = 40/s observed, clamped to the
  // enabled bounds when the excursion ended.
  for (let i = 0; i < 80; i += 1) bus.publish('t', `round2-${i}`);
  assert.equal(events.length, 2);
  assert.equal(events[1].ratePerSec, 40);
  assert.equal(events[1].adapted, true);
  sub.unsubscribe();
});

test('EB-538: enable is fail-closed for unknown, empty, or illegal values', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { queueSize: 100 });

  assert.throws(() => bus.setThrottle('nope', true), RangeError);
  assert.throws(() => bus.setThrottle('', true), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, { minRatePerSec: 0 }), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, { minRatePerSec: -1 }), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, { minRatePerSec: Number.NaN }), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, { maxRatePerSec: 0 }), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, { maxRatePerSec: -5 }), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, { maxRatePerSec: Number.NaN }), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, { initialRatePerSec: 0 }), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, { initialRatePerSec: Infinity }), RangeError);
  assert.throws(
    () => bus.setThrottle(sub.id, { minRatePerSec: 10, maxRatePerSec: 5 }),
    RangeError,
  );
  assert.throws(() => bus.setThrottle(sub.id, undefined as never), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, 'x' as never), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, [] as never), RangeError);
  assert.throws(() => bus.setThrottle(sub.id, false as never), RangeError);

  assert.equal(revisionAudits(bus).length, 0);
  // The failed enables moved nothing: the subscriber stays unthrottled.
  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(bus.getStats().throttledSubscribers, 0);
  sub.unsubscribe();
});

test('EB-538: the enable audit records the resolved rates with oldThrottle null and actor system', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { queueSize: 100 });

  bus.setThrottle(sub.id, { minRatePerSec: 3 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    throttle: { minRatePerSec: 3, maxRatePerSec: Infinity, initialRatePerSec: 80 },
    oldThrottle: null,
  });
  sub.unsubscribe();
});

test("EB-538: a group member's throttle is enabled the same way", () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const member = bus.subscribeToGroup('g1', 't', () => {}, {
    queueSize: 100,
    onThrottled: (e) => events.push(e),
  });

  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(bus.pendingCount(member.id), 80);
  assert.equal(bus.getStats().throttledSubscribers, 0);

  bus.setThrottle(member.id, { initialRatePerSec: 2, minRatePerSec: 1 });
  assert.equal(bus.getStats().throttledSubscribers, 1, 'the member queue was already above the mark');
  assert.deepEqual(revisionAudits(bus)[0].detail, {
    throttle: { minRatePerSec: 1, maxRatePerSec: Infinity, initialRatePerSec: 2 },
    oldThrottle: null,
  });
  assert.equal(events.length, 1);
  member.unsubscribe();
});

test('EB-538: enabling unlocks setThrottledCallback on the subscriber', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, { queueSize: 100 });

  assert.throws(() => bus.setThrottledCallback(sub.id, (e) => events.push(e)), RangeError);

  bus.setThrottle(sub.id, { initialRatePerSec: 2, minRatePerSec: 1 });
  bus.setThrottledCallback(sub.id, (e) => events.push(e));
  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 2);
  sub.unsubscribe();
});

test('EB-538: enabling installs the backpressure hook, unlocking setBackpressureCallbacks', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: number[] = [];
  const sub = bus.subscribe('t', () => {}, { queueSize: 100 });

  assert.throws(
    () => bus.setBackpressureCallbacks(sub.id, { onBackpressure: (e) => seen.push(e.queueSize) }),
    RangeError,
  );

  bus.setThrottle(sub.id, { initialRatePerSec: 2, minRatePerSec: 1 });
  bus.setBackpressureCallbacks(sub.id, { onBackpressure: (e) => seen.push(e.queueSize) });
  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.deepEqual(seen, [80]);
  sub.unsubscribe();
});
