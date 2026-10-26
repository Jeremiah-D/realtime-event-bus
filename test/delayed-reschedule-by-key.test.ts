import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('reschedules only the pending schedules for one topic and one key, in schedule order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const id1 = bus.publishDelayed('t', 'one', { delayMs: 1000, key: 'k' });
  const id2 = bus.publishDelayed('t', 'two', { delayMs: 2000, key: 'k' });
  const otherKey = bus.publishDelayed('t', 'other-key', { delayMs: 1000, key: 'j' });
  const unkeyed = bus.publishDelayed('t', 'unkeyed', { delayMs: 1000 });
  const otherTopic = bus.publishDelayed('u', 'other-topic', { delayMs: 1000, key: 'k' });
  const result = bus.rescheduleDelayedByKey('t', 'k', { delayMs: 9000 });
  assert.deepEqual(result, { rescheduled: 2, delayIds: [id1, id2] });
  // Both moved schedules share one target due time (1000 + 9000),
  // keep their ids, and the untouched schedules keep theirs.
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.topic, e.key, e.deliverAt]),
    [
      [id1, 't', 'k', 10000],
      [id2, 't', 'k', 10000],
      [otherKey, 't', 'j', 2000],
      [unkeyed, 't', undefined, 2000],
      [otherTopic, 'u', 'k', 2000],
    ],
  );
  assert.equal(bus.getStats().pendingDelayed, 5);
  // The moved schedules do not fire at their original due times.
  // Note 'other-topic' is also held back here: it shares key 'k'
  // with the moved schedules, and per-key ordering keeps it behind
  // them even though its own due time passed — the move preserved
  // the key sequence, exactly as a single reschedule would.
  clock.nowMs = 3000;
  bus.publish('u', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'other-key', 'unkeyed']);
  // And they do fire at the new due time, with payloads intact —
  // after which the held-back same-key schedule follows.
  clock.nowMs = 10000;
  bus.publish('u', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'other-key', 'unkeyed', 'poke2', 'one', 'two', 'other-topic']);
});

test('a wildcard pattern expands to every matching concrete topic, still filtered by key', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('**', () => {});
  const eu = bus.publishDelayed('orders.eu', 'eu', { delayMs: 1000, key: 'k' });
  const us = bus.publishDelayed('orders.us', 'us', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('orders.us', 'other-key', { delayMs: 1000, key: 'j' });
  bus.publishDelayed('billing.eu', 'billing', { delayMs: 1000, key: 'k' });
  const result = bus.rescheduleDelayedByKey('orders.*', 'k', { deliverAt: 9000 });
  assert.deepEqual(result, { rescheduled: 2, delayIds: [eu, us] });
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.topic, e.deliverAt]),
    [
      ['orders.eu', 9000],
      ['orders.us', 9000],
      ['orders.us', 2000],
      ['billing.eu', 2000],
    ],
  );
});

test('fanned-out and cancelled schedules are not counted; an empty move returns zero', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const gone = bus.publishDelayed('t', 'due-now', { delayMs: 0, key: 'k' });
  await flush();
  const cancelled = bus.publishDelayed('t', 'never', { delayMs: 5000, key: 'k' });
  assert.equal(bus.cancelDelayed(cancelled as string), true);
  const staying = bus.publishDelayed('t', 'stays', { delayMs: 5000, key: 'k' });
  assert.deepEqual(bus.rescheduleDelayedByKey('t', 'k', { delayMs: 9000 }), {
    rescheduled: 1,
    delayIds: [staying],
  });
  assert.equal(bus.listDelayed()[0].deliverAt, 10000);
  // No pending schedule for an unknown key: zero, never a throw.
  assert.deepEqual(bus.rescheduleDelayedByKey('t', 'nobody', { delayMs: 9000 }), {
    rescheduled: 0,
    delayIds: [],
  });
  assert.equal(gone !== staying, true); // the fanned-out id was never re-moved
});

test('a moved schedule is still cancellable by key, and a second identical move is a no-op', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000, key: 'k' });
  bus.rescheduleDelayedByKey('t', 'k', { deliverAt: 9000 });
  bus.rescheduleDelayedByKey('t', 'k', { deliverAt: 9000 });
  assert.equal(bus.listDelayed()[0].deliverAt, 9000);
  assert.equal(bus.listDelayed()[0].delayId, id); // same id, never duplicated
  assert.equal(bus.cancelDelayedByKey('t', 'k').cancelled, 1);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('rescheduleDelayedByKey validates pattern, key, and timing before touching anything', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000, key: 'k' });
  assert.throws(() => bus.rescheduleDelayedByKey(123 as unknown as string, 'k', { delayMs: 1000 }), TypeError);
  assert.throws(() => bus.rescheduleDelayedByKey('', 'k', { delayMs: 1000 }), RangeError);
  assert.throws(() => bus.rescheduleDelayedByKey('t', 123 as unknown as string, { delayMs: 1000 }), TypeError);
  assert.throws(() => bus.rescheduleDelayedByKey('t', '', { delayMs: 1000 }), RangeError);
  assert.throws(() => bus.rescheduleDelayedByKey('t', 'k', {} as never), RangeError);
  assert.throws(
    () => bus.rescheduleDelayedByKey('t', 'k', { delayMs: 1000, deliverAt: 9000 }),
    RangeError,
  );
  assert.throws(() => bus.rescheduleDelayedByKey('t', 'k', { delayMs: -5 }), RangeError);
  assert.throws(() => bus.rescheduleDelayedByKey('t', 'k', { deliverAt: Number.NaN }), RangeError);
  // Every failed attempt left the original schedule exactly as it was.
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.deliverAt]),
    [[id, 2000]],
  );
  // And no audit record was written by the failed or empty moves.
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delayed_rescheduled' }), []);
});

test('a successful bulk move audits one delayed_rescheduled record per moved schedule', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id1 = bus.publishDelayed('t', 'one', { delayMs: 1000, key: 'k' });
  const id2 = bus.publishDelayed('t', 'two', { delayMs: 2000, key: 'k' });
  bus.rescheduleDelayedByKey('t', 'k', { delayMs: 9000 });
  const events = bus.getSubscriptionAudit({ action: 'delayed_rescheduled' });
  assert.equal(events.length, 2);
  assert.deepEqual(events[0].detail, { delayId: id1, topic: 't', oldDeliverAt: 2000, deliverAt: 10000 });
  assert.deepEqual(events[1].detail, { delayId: id2, topic: 't', oldDeliverAt: 3000, deliverAt: 10000 });
});
