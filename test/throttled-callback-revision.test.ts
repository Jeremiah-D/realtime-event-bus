import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type ThrottleEvent } from '../src/bus.ts';

/**
 * Throttle-engagement callback revision (EB-440):
 * `setThrottledCallback` swaps a throttled subscriber's `onThrottled`
 * without re-subscribing — the only throttle revision surface
 * `setThrottle` left uncovered: it moves the three rates, this one
 * moves who hears about the throttle engaging. Only the callback
 * moves: the bucket, the enforced rate, the observed drain rate, and
 * the drop count are untouched, and a throwing callback is swallowed
 * by `engageThrottle`, so observation never breaks the throttle path.
 *
 * Determinism trick (as in `throttle.test.ts`): a `queueSize: 10`
 * subscriber with `throttle: true` engages synchronously on the 8th
 * publish (the default high-water mark is 0.8 of capacity), and a
 * flush drains the queue so the next 8 publishes engage again.
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

/** The `throttled_callback_revised` audit events on the bus. */
function callbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'throttled_callback_revised' });
}

/** Publishes `n` messages to engage the throttle (queueSize 10, HWM 8). */
function engage(bus: EventBus, n = 8): void {
  for (let i = 0; i < n; i++) bus.publish('t', i);
}

test('swapping the callback routes the next engagement to the new callback with exact event fields', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const first: ThrottleEvent[] = [];
  const second: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => first.push(e),
  });
  engage(bus);
  assert.equal(first.length, 1);
  assert.equal(first[0].subscriberId, sub.id);
  assert.equal(first[0].pattern, 't');
  assert.equal(first[0].ratePerSec, 8);
  assert.equal(first[0].queueSize, 8);
  assert.equal(first[0].capacity, 10);
  assert.equal(first[0].adapted, false);
  assert.equal(bus.getStats().throttledSubscribers, 1);

  // Swap in the second callback and drain so the throttle disengages:
  // the next engagement reaches it only, with exact event fields.
  bus.setThrottledCallback(sub.id, (e) => second.push(e));
  await flush();
  assert.equal(bus.getStats().throttledSubscribers, 0);
  engage(bus);
  assert.equal(first.length, 1, 'the old callback never fires again');
  assert.equal(second.length, 1);
  assert.equal(second[0].subscriberId, sub.id);
  assert.equal(second[0].pattern, 't');
  assert.equal(second[0].ratePerSec, 8);
  assert.equal(second[0].queueSize, 8);
  assert.equal(second[0].capacity, 10);
  assert.equal(second[0].adapted, false);
  assert.equal(bus.getStats().throttledSubscribers, 1);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: true,
    oldHasCallback: true,
  });
  sub.unsubscribe();
});

test('clearing with null keeps the throttle engaging with no callback', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => events.push(e),
  });
  engage(bus);
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().throttledSubscribers, 1);

  bus.setThrottledCallback(sub.id, null);
  await flush();
  assert.equal(bus.getStats().throttledSubscribers, 0);
  engage(bus);
  assert.equal(events.length, 1, 'no callback fires after the clear');
  assert.equal(bus.getStats().throttledSubscribers, 1, 'the throttle still engages');
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: false,
    oldHasCallback: true,
  });
  sub.unsubscribe();
});

test('unknown, unthrottled, empty and illegal values throw RangeError with the live callback untouched', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const cb = (e: ThrottleEvent) => events.push(e);
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: true,
    onThrottled: cb,
  });
  const plain = bus.subscribe('plain', () => {});
  const bad = (value: unknown) =>
    assert.throws(() => bus.setThrottledCallback(sub.id, value as never), RangeError);
  assert.throws(() => bus.setThrottledCallback('', cb), RangeError);
  assert.throws(() => bus.setThrottledCallback('no-such-sub', cb), RangeError);
  assert.throws(() => bus.setThrottledCallback(plain.id, cb), RangeError);
  bad('not-a-function');
  bad(42);
  bad(undefined);
  bad({});
  bad([]);
  assert.equal(callbackRevisionAudits(bus).length, 0, 'failed revisions leave no audit');
  assert.equal(bus.getStats().throttledSubscribers, 0);

  // The live callback is untouched: the next engagement still fires it.
  engage(bus);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 8);
  assert.equal(callbackRevisionAudits(bus).length, 0);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('re-affirming the live callback and clearing an unset callback are no-ops with no audit', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, { queueSize: 10, throttle: true });
  // Clearing a callback that was never set: no swap, no audit.
  bus.setThrottledCallback(sub.id, null);
  assert.equal(callbackRevisionAudits(bus).length, 0);

  // Installing, then re-affirming the same reference: one audit total.
  const cb = (e: ThrottleEvent) => events.push(e);
  bus.setThrottledCallback(sub.id, cb);
  bus.setThrottledCallback(sub.id, cb);
  assert.equal(callbackRevisionAudits(bus).length, 1);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: true,
    oldHasCallback: false,
  });

  engage(bus);
  assert.equal(events.length, 1, 'the installed callback still governs');
  sub.unsubscribe();
});

test('a throwing callback is swallowed and the throttle path is undisturbed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    // A roomy queue (100) so the bucket, not the queue capacity, is
    // what sheds the extras below.
    queueSize: 100,
    throttle: { initialRatePerSec: 8, minRatePerSec: 1 },
  });
  bus.setThrottledCallback(sub.id, () => {
    throw new Error('observer blew up');
  });
  // The engagement itself must not throw out of publish (HWM is 80).
  assert.doesNotThrow(() => {
    for (let i = 0; i < 80; i++) bus.publish('t', i);
  });
  assert.equal(bus.getStats().throttledSubscribers, 1);
  // The bucket still sheds past its burst exactly as before: 8 pass,
  // the rest are shed and counted.
  const accepted: number[] = [];
  for (let i = 0; i < 10; i++) accepted.push(bus.publish('t', `extra-${i}`));
  assert.deepEqual(accepted, [1, 1, 1, 1, 1, 1, 1, 1, 0, 0]);
  assert.equal(bus.throttledCount(sub.id), 2);
  await flush();
  assert.equal(received.length, 88, 'the queued messages still drain');
  assert.equal(bus.getStats().throttledSubscribers, 0);
  sub.unsubscribe();
});

test('the audit event records the callback presence exactly', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { queueSize: 10, throttle: true });
  const cb = () => {};
  bus.setThrottledCallback(sub.id, cb);
  const installed = callbackRevisionAudits(bus);
  assert.equal(installed.length, 1);
  assert.equal(installed[0].action, 'throttled_callback_revised');
  assert.equal(installed[0].subId, sub.id);
  assert.equal(installed[0].pattern, 't');
  assert.deepEqual(installed[0].detail, { hasCallback: true, oldHasCallback: false });

  bus.setThrottledCallback(sub.id, null);
  const cleared = callbackRevisionAudits(bus);
  assert.equal(cleared.length, 2);
  assert.deepEqual(cleared[1].detail, { hasCallback: false, oldHasCallback: true });
  sub.unsubscribe();
});

test('the drain-rate seed survives a callback swap', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const first: ThrottleEvent[] = [];
  const second: ThrottleEvent[] = [];
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
    onThrottled: (e) => first.push(e),
  });
  for (let i = 0; i < 80; i++) bus.publish('t', i); // engage at t=1000
  assert.equal(first.length, 1);
  assert.equal(first[0].ratePerSec, 2);
  assert.equal(first[0].adapted, false);

  clock.nowMs = 3_000; // 2s later the consumer drains everything
  await flush();
  assert.equal(received.length, 80);
  assert.equal(bus.getStats().throttledSubscribers, 0);

  // Swap the callback after the excursion: the measured drain rate
  // (80 messages over 2000ms = 40/s) still seeds the next engagement.
  // Re-find the throttled subscription id via its first event.
  const throttledSubId = first[0].subscriberId;
  bus.setThrottledCallback(throttledSubId, (e) => second.push(e));
  for (let i = 0; i < 80; i++) bus.publish('t', `round2-${i}`);
  assert.equal(first.length, 1, 'the old callback never fires again');
  assert.equal(second.length, 1);
  assert.equal(second[0].ratePerSec, 40);
  assert.equal(second[0].adapted, true);
  assert.equal(bus.getStats().throttledSubscribers, 1);
});

test('setThrottle rate revision is unaffected by a callback revision', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const first: ThrottleEvent[] = [];
  const second: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
    onThrottled: (e) => first.push(e),
  });
  bus.setThrottledCallback(sub.id, (e) => second.push(e));
  bus.setThrottle(sub.id, { initialRatePerSec: 5 });
  engage(bus);
  assert.equal(first.length, 0, 'the old callback never fires again');
  assert.equal(second.length, 1);
  assert.equal(second[0].ratePerSec, 5, 'the revised rate governs the engagement');
  assert.equal(second[0].adapted, false);
  const rateAudits = bus.getSubscriptionAudit({ action: 'throttle_revised' });
  assert.equal(rateAudits.length, 1);
  assert.equal(rateAudits[0].detail.throttle.initialRatePerSec, 5);
  assert.equal(rateAudits[0].detail.oldThrottle.initialRatePerSec, 2);
  assert.equal(callbackRevisionAudits(bus).length, 1);
  sub.unsubscribe();
});

test('swapping mid-excursion preserves the bucket, the enforced rate, and the drop count', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const first: ThrottleEvent[] = [];
  const second: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    // A roomy queue (100) so the bucket, not the queue capacity, is
    // what sheds the extras below.
    queueSize: 100,
    throttle: { initialRatePerSec: 8, minRatePerSec: 1 },
    onThrottled: (e) => first.push(e),
  });
  for (let i = 0; i < 80; i++) bus.publish('t', i); // engage at HWM 80
  assert.equal(first.length, 1);
  assert.equal(first[0].ratePerSec, 8);
  // Spend the bucket: 8 pass, 2 shed.
  const accepted: number[] = [];
  for (let i = 0; i < 10; i++) accepted.push(bus.publish('t', `extra-${i}`));
  assert.deepEqual(accepted, [1, 1, 1, 1, 1, 1, 1, 1, 0, 0]);
  assert.equal(bus.throttledCount(sub.id), 2);

  // Swap mid-excursion: no re-fire, and the excursion state survives.
  bus.setThrottledCallback(sub.id, (e) => second.push(e));
  assert.equal(first.length, 1);
  assert.equal(second.length, 0);
  assert.equal(bus.getStats().throttledSubscribers, 1);
  assert.equal(bus.publish('t', 'late-1'), 0);
  assert.equal(bus.publish('t', 'late-2'), 0);
  assert.equal(bus.throttledCount(sub.id), 4, 'the drop count keeps counting');
  await flush();
  assert.equal(received.length, 88, 'the queued messages still drain');
  assert.equal(bus.getStats().throttledSubscribers, 0);
  sub.unsubscribe();
});
