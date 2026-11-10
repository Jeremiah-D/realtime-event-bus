import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BackpressureEvent,
  type DrainedEvent,
  type ThrottleEvent,
} from '../src/bus.ts';

/**
 * Backpressure observation-callback revision (EB-455):
 * `setBackpressureCallbacks` swaps a subscriber's `onBackpressure`
 * and/or `onDrained` without re-subscribing — the only backpressure
 * revision surface `setBackpressureTier` left uncovered: it moves the
 * full-queue behavior, this one moves who hears about the queue
 * filling up and draining again. Only the callbacks move: the queue
 * wrappers read the live subscriber fields on every excursion /
 * drain, so a cleared callback leaves the `'block'` latch, the
 * throttle engagement, and the drain-clear running with nobody
 * receiving.
 *
 * Determinism trick (as in `throttle.test.ts` and
 * `backpressure-tier-revision.test.ts`): a `queueSize: 10` subscriber
 * crosses the default 0.8 high-water mark on the 8th publish, and a
 * flush drains the queue so the next 8 publishes excursion again.
 * Publishes queue synchronously; the handler only runs on the flush.
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

/** The `backpressure_callbacks_revised` audit events on the bus. */
function callbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'backpressure_callbacks_revised' });
}

/** Publishes `n` messages to cross the high-water mark (queueSize 10). */
function excursion(bus: EventBus, n = 8): void {
  for (let i = 0; i < n; i++) bus.publish('t', i);
}

test('swapping onBackpressure routes the next excursion to the new callback with exact event fields', async () => {
  const bus = new EventBus();
  const first: BackpressureEvent[] = [];
  const second: BackpressureEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onBackpressure: (e) => first.push(e),
  });
  excursion(bus);
  assert.equal(first.length, 1);
  assert.equal(first[0].subscriberId, sub.id);
  assert.equal(first[0].pattern, 't');
  assert.equal(first[0].queueSize, 8);
  assert.equal(first[0].capacity, 10);
  assert.equal(first[0].dropped, 0);

  // Swap in the second callback and drain so the excursion re-arms:
  // the next excursion reaches it only, with exact event fields.
  bus.setBackpressureCallbacks(sub.id, { onBackpressure: (e) => second.push(e) });
  await flush();
  excursion(bus);
  assert.equal(first.length, 1, 'the old callback never fires again');
  assert.equal(second.length, 1);
  assert.equal(second[0].subscriberId, sub.id);
  assert.equal(second[0].pattern, 't');
  assert.equal(second[0].queueSize, 8);
  assert.equal(second[0].capacity, 10);
  assert.equal(second[0].dropped, 0);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    onBackpressure: 'set',
    oldOnBackpressure: 'set',
    onDrained: 'unchanged',
    oldOnDrained: 'absent',
  });
  sub.unsubscribe();
});

test('swapping onDrained routes the next drain to the new callback with exact event fields', async () => {
  const bus = new EventBus();
  const first: DrainedEvent[] = [];
  const second: DrainedEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onBackpressure: () => {},
    onDrained: (e) => first.push(e),
  });
  excursion(bus);
  await flush();
  assert.equal(first.length, 1);
  assert.equal(first[0].subscriberId, sub.id);
  assert.equal(first[0].pattern, 't');
  assert.equal(first[0].queueSize, 0);
  assert.equal(first[0].capacity, 10);
  assert.equal(first[0].dropped, 0);
  assert.equal(first[0].highWaterMark, 8);

  bus.setBackpressureCallbacks(sub.id, { onDrained: (e) => second.push(e) });
  excursion(bus);
  await flush();
  assert.equal(first.length, 1, 'the old callback never fires again');
  assert.equal(second.length, 1);
  assert.equal(second[0].subscriberId, sub.id);
  assert.equal(second[0].pattern, 't');
  assert.equal(second[0].queueSize, 0);
  assert.equal(second[0].capacity, 10);
  assert.equal(second[0].dropped, 0);
  assert.equal(second[0].highWaterMark, 8);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    onBackpressure: 'unchanged',
    oldOnBackpressure: 'set',
    onDrained: 'set',
    oldOnDrained: 'set',
  });
  sub.unsubscribe();
});

test('clearing onBackpressure keeps the throttle engaging with no callback', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const pressure: BackpressureEvent[] = [];
  const throttled: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: true,
    onBackpressure: (e) => pressure.push(e),
    onThrottled: (e) => throttled.push(e),
  });
  excursion(bus);
  assert.equal(pressure.length, 1);
  assert.equal(throttled.length, 1);
  assert.equal(bus.getStats().throttledSubscribers, 1);

  bus.setBackpressureCallbacks(sub.id, { onBackpressure: null });
  await flush();
  assert.equal(bus.getStats().throttledSubscribers, 0);
  excursion(bus);
  assert.equal(pressure.length, 1, 'no callback fires after the clear');
  assert.equal(throttled.length, 2, 'the throttle still engages');
  assert.equal(bus.getStats().throttledSubscribers, 1);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    onBackpressure: 'cleared',
    oldOnBackpressure: 'set',
    onDrained: 'unchanged',
    oldOnDrained: 'absent',
  });
  sub.unsubscribe();
});

test("clearing onDrained leaves the 'block' latch and its drain-clear running with no callback", async () => {
  const bus = new EventBus();
  const drained: DrainedEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 2,
    backpressure: 'block',
    onDrained: (e) => drained.push(e),
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // First saturation under 'block': admit-with-shed, and the latch arms.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  // Further arrivals are refused while latched.
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.blockedCount(sub.id), 1);

  bus.setBackpressureCallbacks(sub.id, { onDrained: null });
  await flush();
  assert.equal(drained.length, 0, 'no callback fires after the clear');
  assert.equal(bus.getStats().blockedSubscribers, 0, 'the drain still clears the latch');
  // Admissions resume once the latch cleared.
  assert.equal(bus.publish('t', 'm5'), 1);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    onBackpressure: 'unchanged',
    oldOnBackpressure: 'absent',
    onDrained: 'cleared',
    oldOnDrained: 'set',
  });
  sub.unsubscribe();
});

test('onBackpressure without a hook is refused fail-closed and the live state is untouched', async () => {
  const bus = new EventBus();
  const drained: DrainedEvent[] = [];
  const replacement: BackpressureEvent[] = [];
  const replacementDrained: DrainedEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onDrained: (e) => drained.push(e),
  });
  // No subscribe-time `onBackpressure` and no `throttle`: the queue has
  // no `onHighWaterMark` wrapper to retarget — re-subscribe to opt in.
  assert.throws(
    () => bus.setBackpressureCallbacks(sub.id, { onBackpressure: (e) => replacement.push(e) }),
    RangeError,
  );
  // The gate vetoes the whole patch: the `onDrained` half must not move
  // when the `onBackpressure` half is refused.
  assert.throws(
    () =>
      bus.setBackpressureCallbacks(sub.id, {
        onBackpressure: (e) => replacement.push(e),
        onDrained: (e) => replacementDrained.push(e),
      }),
    RangeError,
  );
  assert.equal(callbackRevisionAudits(bus).length, 0, 'failed revisions must not audit');
  excursion(bus);
  await flush();
  assert.equal(replacement.length, 0, 'no backpressure observer was installed');
  assert.equal(replacementDrained.length, 0, 'the vetoed drain half never moved');
  assert.equal(drained.length, 1, 'the live onDrained still fires');
  sub.unsubscribe();
});

test('onDrained can be installed after the fact on a subscription with no callbacks', async () => {
  const bus = new EventBus();
  const drained: DrainedEvent[] = [];
  const sub = bus.subscribe('t', () => {}, { queueSize: 10 });
  bus.setBackpressureCallbacks(sub.id, { onDrained: (e) => drained.push(e) });
  excursion(bus);
  await flush();
  assert.equal(drained.length, 1);
  assert.equal(drained[0].subscriberId, sub.id);
  assert.equal(drained[0].pattern, 't');
  assert.equal(drained[0].queueSize, 0);
  assert.equal(drained[0].capacity, 10);
  assert.equal(drained[0].dropped, 0);
  assert.equal(drained[0].highWaterMark, 8);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    onBackpressure: 'unchanged',
    oldOnBackpressure: 'absent',
    onDrained: 'set',
    oldOnDrained: 'absent',
  });
  sub.unsubscribe();
});

test('unknown, empty, and illegal patches throw RangeError with the live callbacks untouched', async () => {
  const bus = new EventBus();
  const pressure: BackpressureEvent[] = [];
  const drained: DrainedEvent[] = [];
  const pressureCb = (e: BackpressureEvent) => pressure.push(e);
  const drainedCb = (e: DrainedEvent) => drained.push(e);
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onBackpressure: pressureCb,
    onDrained: drainedCb,
  });
  for (const bad of [
    () => bus.setBackpressureCallbacks('no-such-sub', { onBackpressure: pressureCb }),
    () => bus.setBackpressureCallbacks('', { onBackpressure: pressureCb }),
    () => bus.setBackpressureCallbacks(sub.id, {}),
    () => bus.setBackpressureCallbacks(sub.id, null as never),
    () => bus.setBackpressureCallbacks(sub.id, [] as never),
    () => bus.setBackpressureCallbacks(sub.id, 'x' as never),
    () => bus.setBackpressureCallbacks(sub.id, { onBackpressure: 'x' as never }),
    () => bus.setBackpressureCallbacks(sub.id, { onBackpressure: 42 as never }),
    () => bus.setBackpressureCallbacks(sub.id, { onDrained: {} as never }),
    () =>
      bus.setBackpressureCallbacks(sub.id, {
        onBackpressure: undefined,
        onDrained: undefined,
      }),
    // One illegal half vetoes the whole patch — the legal half stays put.
    () =>
      bus.setBackpressureCallbacks(sub.id, {
        onBackpressure: (e: BackpressureEvent) => pressure.push(e),
        onDrained: 42 as never,
      }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(callbackRevisionAudits(bus).length, 0, 'failed revisions must not audit');
  // The live callbacks are untouched: the next excursion and drain
  // still reach them.
  excursion(bus);
  assert.equal(pressure.length, 1);
  await flush();
  assert.equal(drained.length, 1);
  sub.unsubscribe();
});

test('re-affirming the live callbacks is a no-op with no audit', () => {
  const bus = new EventBus();
  const pressureCb = () => {};
  const drainedCb = () => {};
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onBackpressure: pressureCb,
    onDrained: drainedCb,
  });
  bus.setBackpressureCallbacks(sub.id, { onBackpressure: pressureCb });
  bus.setBackpressureCallbacks(sub.id, { onDrained: drainedCb });
  bus.setBackpressureCallbacks(sub.id, { onBackpressure: pressureCb, onDrained: drainedCb });
  assert.equal(callbackRevisionAudits(bus).length, 0);

  // Clearing a callback that was never set is likewise a no-op.
  const plain = bus.subscribe('t2', () => {}, {
    queueSize: 10,
    onBackpressure: pressureCb,
  });
  bus.setBackpressureCallbacks(plain.id, { onDrained: null });
  assert.equal(callbackRevisionAudits(bus).length, 0);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('callback revision leaves tier, queue size, and high-water-mark ratio revisions working', async () => {
  const bus = new EventBus();
  const pressure: BackpressureEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onBackpressure: () => {},
  });
  bus.setBackpressureCallbacks(sub.id, { onBackpressure: (e) => pressure.push(e) });
  // The other backpressure revisions still apply on top of the callback
  // revision: the tier, the capacity, and the ratio each move.
  bus.setBackpressureTier(sub.id, 'drop-oldest');
  bus.setQueueSize(sub.id, 2);
  bus.setHighWaterMarkRatio(sub.id, 0.5);
  // HWM is now 1 of 2: the first publish excursions into the new callback.
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(pressure.length, 1);
  assert.equal(pressure[0].queueSize, 1);
  assert.equal(pressure[0].capacity, 2);
  assert.equal(bus.publish('t', 'm2'), 1);
  // Full under the revised drop tier: the newcomer is admitted and the
  // oldest shed, per the revised tier — not per the callback revision.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  await flush();
  sub.unsubscribe();
});

test('throttle still engages and disengages per the old wrapper, and a throttle-only subscription can gain onBackpressure', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const pressure: BackpressureEvent[] = [];
  const throttled: ThrottleEvent[] = [];
  // No `onBackpressure` at subscribe time — the wrapper exists only
  // because `throttle` is present, and that is enough to opt in.
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => throttled.push(e),
  });
  bus.setBackpressureCallbacks(sub.id, { onBackpressure: (e) => pressure.push(e) });
  excursion(bus);
  assert.equal(pressure.length, 1);
  assert.equal(pressure[0].queueSize, 8);
  assert.equal(pressure[0].capacity, 10);
  assert.equal(throttled.length, 1, 'the old throttle wrapper still fires');
  assert.equal(bus.getStats().throttledSubscribers, 1);
  await flush();
  assert.equal(bus.getStats().throttledSubscribers, 0, 'the drain still disengages');
  excursion(bus);
  assert.equal(pressure.length, 2);
  assert.equal(throttled.length, 2);
  assert.equal(bus.getStats().throttledSubscribers, 1);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    onBackpressure: 'set',
    oldOnBackpressure: 'absent',
    onDrained: 'unchanged',
    oldOnDrained: 'absent',
  });
  sub.unsubscribe();
});
