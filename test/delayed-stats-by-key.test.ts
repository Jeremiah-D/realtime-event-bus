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

test('getDelayedStatsByKey on a fresh bus is empty', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.deepEqual(bus.getDelayedStatsByKey('k'), { total: 0, byTopic: [] });
});

test('getDelayedStatsByKey groups one key by topic, ignoring other keys and unkeyed schedules', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('orders', 'first', { delayMs: 5000, key: 'k' }); // due 6000
  bus.publishDelayed('orders', 'second', { delayMs: 1000, key: 'k' }); // due 2000, earliest for orders
  bus.publishDelayed('alerts', 'third', { delayMs: 3000, key: 'k' }); // due 4000
  bus.publishDelayed('orders', 'other-key', { delayMs: 1000, key: 'j' });
  bus.publishDelayed('alerts', 'unkeyed', { delayMs: 1000 });
  const stats = bus.getDelayedStatsByKey('k');
  assert.equal(stats.total, 3);
  // First-appearance schedule order, not due-time order.
  assert.deepEqual(stats.byTopic, [
    { topic: 'orders', pending: 2, earliestDeliverAt: 2000 },
    { topic: 'alerts', pending: 1, earliestDeliverAt: 4000 },
  ]);
  assert.equal(
    stats.byTopic.reduce((sum, row) => sum + row.pending, 0),
    stats.total,
  );
  // The other key sees only its own schedules.
  assert.deepEqual(bus.getDelayedStatsByKey('j'), {
    total: 1,
    byTopic: [{ topic: 'orders', pending: 1, earliestDeliverAt: 2000 }],
  });
});

test('getDelayedStatsByKey agrees with listDelayed for the same key', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('t', 'b', { delayMs: 2000, key: 'k' });
  bus.publishDelayed('u', 'c', { delayMs: 3000, key: 'k' });
  bus.publishDelayed('t', 'd', { delayMs: 4000, key: 'j' });
  const stats = bus.getDelayedStatsByKey('k');
  assert.equal(stats.total, bus.listDelayed(undefined, { key: 'k' }).length);
});

test('getDelayedStatsByKey filters by exact topic and by wildcard pattern', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('orders.eu', 'a', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('orders.us', 'b', { delayMs: 2000, key: 'k' });
  bus.publishDelayed('alerts', 'c', { delayMs: 3000, key: 'k' });
  assert.deepEqual(bus.getDelayedStatsByKey('k', 'orders.eu'), {
    total: 1,
    byTopic: [{ topic: 'orders.eu', pending: 1, earliestDeliverAt: 2000 }],
  });
  assert.deepEqual(bus.getDelayedStatsByKey('k', 'orders.*'), {
    total: 2,
    byTopic: [
      { topic: 'orders.eu', pending: 1, earliestDeliverAt: 2000 },
      { topic: 'orders.us', pending: 1, earliestDeliverAt: 3000 },
    ],
  });
  assert.deepEqual(bus.getDelayedStatsByKey('k', 'nothing-here'), { total: 0, byTopic: [] });
});

test('getDelayedStatsByKey counts only still-pending schedules', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const gone = bus.publishDelayed('t', 'due-now', { delayMs: 0, key: 'k' });
  const cancelled = bus.publishDelayed('t', 'never', { delayMs: 5000, key: 'k' });
  const staying = bus.publishDelayed('t', 'later', { delayMs: 9000, key: 'k' });
  assert.equal(bus.cancelDelayed(cancelled as string), true);
  await flush(); // fans out 'due-now'
  assert.deepEqual(received, ['due-now']);
  assert.equal(gone !== staying, true); // ids are distinct handles
  assert.deepEqual(bus.getDelayedStatsByKey('k'), {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 10000 }],
  });
});

test('getDelayedStatsByKey excludes a due schedule past its deadline even before any sweep runs', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  // Due t=3000, past its deadline t=2000: the next sweep would drop it.
  bus.publishDelayed('t', 'expired', { delayMs: 2000, deadlineAt: 2000, key: 'k' });
  bus.publishDelayed('t', 'still-pending', { delayMs: 5000, key: 'k' }); // due t=6000
  clock.nowMs = 3_000; // due, but no sweep has run yet
  assert.deepEqual(bus.getDelayedStatsByKey('k'), {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 6000 }],
  });
});

test('getDelayedStatsByKey still counts a not-yet-due schedule past its deadline', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('t', 'still-pending', { delayMs: 5000, deadlineAt: 2000, key: 'k' }); // due t=6000
  clock.nowMs = 2_000; // past the deadline, but not yet due: no sweep would drop it yet
  assert.deepEqual(bus.getDelayedStatsByKey('k'), {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 6000 }],
  });
});

test('getDelayedStatsByKey is read-only: no stats, listing, or audit side effects', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('t', 'must-not-leak', { delayMs: 5000, key: 'k' });
  bus.publishDelayed('u', 'other', { delayMs: 1000, key: 'k' });
  const statsBefore = bus.getStats();
  const listedBefore = bus.listDelayed();
  const auditBefore = bus.getSubscriptionAudit();
  const stats = bus.getDelayedStatsByKey('k');
  assert.equal(stats.total, 2);
  assert.ok(!JSON.stringify(stats).includes('must-not-leak'), 'stats must not carry payload');
  assert.deepEqual(bus.getStats(), statsBefore);
  assert.deepEqual(bus.listDelayed(), listedBefore);
  assert.deepEqual(bus.getSubscriptionAudit(), auditBefore);
});

test('getDelayedStatsByKey rejects a non-string or empty key or pattern', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.throws(() => bus.getDelayedStatsByKey(42 as never), TypeError);
  assert.throws(() => bus.getDelayedStatsByKey(''), RangeError);
  assert.throws(() => bus.getDelayedStatsByKey('k', 42 as never), TypeError);
  assert.throws(() => bus.getDelayedStatsByKey('k', ''), RangeError);
  assert.deepEqual(bus.getDelayedStatsByKey('k'), { total: 0, byTopic: [] });
});
