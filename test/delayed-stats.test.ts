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

test('getDelayedStats on a fresh bus is empty', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.deepEqual(bus.getDelayedStats(), { total: 0, byTopic: [] });
});

test('getDelayedStats counts per topic with earliest due time and a global total', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('orders', 'first', { delayMs: 5000 }); // due 6000
  bus.publishDelayed('orders', 'second', { delayMs: 1000 }); // due 2000, earliest for orders
  bus.publishDelayed('alerts', 'third', { delayMs: 3000 }); // due 4000
  const stats = bus.getDelayedStats();
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
});

test('getDelayedStats filters by exact topic and by wildcard pattern', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('orders.eu', 'a', { delayMs: 1000 });
  bus.publishDelayed('orders.us', 'b', { delayMs: 2000 });
  bus.publishDelayed('alerts', 'c', { delayMs: 3000 });
  assert.deepEqual(bus.getDelayedStats('orders.eu'), {
    total: 1,
    byTopic: [{ topic: 'orders.eu', pending: 1, earliestDeliverAt: 2000 }],
  });
  assert.deepEqual(bus.getDelayedStats('orders.*'), {
    total: 2,
    byTopic: [
      { topic: 'orders.eu', pending: 1, earliestDeliverAt: 2000 },
      { topic: 'orders.us', pending: 1, earliestDeliverAt: 3000 },
    ],
  });
  assert.deepEqual(bus.getDelayedStats('nothing-here'), { total: 0, byTopic: [] });
});

test('getDelayedStats counts only still-pending schedules', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const gone = bus.publishDelayed('t', 'due-now', { delayMs: 0 });
  const cancelled = bus.publishDelayed('t', 'never', { delayMs: 5000 });
  const staying = bus.publishDelayed('t', 'later', { delayMs: 9000 });
  assert.equal(bus.cancelDelayed(cancelled as string), true);
  await flush(); // fans out 'due-now'
  assert.deepEqual(received, ['due-now']);
  assert.equal(gone !== staying, true); // ids are distinct handles
  const stats = bus.getDelayedStats();
  assert.deepEqual(stats, {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 10000 }],
  });
});

test('getDelayedStats excludes a due schedule past its deadline even before any sweep runs', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100); // deadline t=1100
  bus.publishDelayed('t', 'doomed', { delayMs: 1000 }); // due t=2000
  const survivor = bus.publishDelayed('t', 'ok', { delayMs: 5000 }); // due t=6000
  clock.nowMs = 2_000; // doomed is due and past its deadline, but no sweep has run
  assert.deepEqual(bus.getDelayedStats(), {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 6000 }],
  });
  assert.equal(survivor !== undefined, true);
  // The stats call itself never sweeps: the doomed entry is still pending.
  assert.equal(bus.getStats().pendingDelayed, 2);
});

test('getDelayedStats still counts an entry past its deadline but not yet due', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100); // deadline t=1100
  bus.publishDelayed('t', 'still-pending', { delayMs: 5000 }); // due t=6000
  clock.nowMs = 2_000; // past the deadline, but not yet due: no sweep would drop it yet
  assert.deepEqual(bus.getDelayedStats(), {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 6000 }],
  });
});

test('getDelayedStats is read-only: no stats, listing, or audit side effects', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('t', { secret: 'must-not-leak' }, { delayMs: 5000 });
  bus.publishDelayed('u', 'other', { delayMs: 1000 });
  const statsBefore = bus.getStats();
  const listedBefore = bus.listDelayed();
  const auditBefore = bus.getSubscriptionAudit();
  const stats = bus.getDelayedStats();
  assert.equal(stats.total, 2);
  assert.ok(!JSON.stringify(stats).includes('must-not-leak'), 'stats must not carry payload');
  assert.deepEqual(bus.getStats(), statsBefore);
  assert.deepEqual(bus.listDelayed(), listedBefore);
  assert.deepEqual(bus.getSubscriptionAudit(), auditBefore);
});

test('getDelayedStats rejects a non-string or empty pattern', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.throws(() => bus.getDelayedStats(42 as never), TypeError);
  assert.throws(() => bus.getDelayedStats(''), RangeError);
  assert.deepEqual(bus.getDelayedStats(), { total: 0, byTopic: [] });
});
