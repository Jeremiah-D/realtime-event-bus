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

test('extendDelayedTtlByKey extends ttlMs for every hit of one topic and key, in schedule order', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id1 = bus.publishDelayed('t', 'one', { delayMs: 1000, ttlMs: 1000, key: 'k' });
  const id2 = bus.publishDelayed('t', 'two', { delayMs: 2000, ttlMs: 2000, key: 'k' });
  const otherKey = bus.publishDelayed('t', 'other-key', { delayMs: 1000, ttlMs: 1000, key: 'j' });
  const otherTopic = bus.publishDelayed('u', 'other-topic', { delayMs: 1000, ttlMs: 1000, key: 'k' });
  const result = bus.extendDelayedTtlByKey('t', 'k', { ttlMs: 10000 });
  assert.deepEqual(result, { extended: 2, delayIds: [id1, id2] });
  // The new value is really stored on both hits: a smaller ttlMs now
  // fails the only-extend rule, where it would have passed before.
  assert.throws(() => bus.extendDelayedTtl(id1 as string, { ttlMs: 5000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtl(id2 as string, { ttlMs: 5000 }), RangeError);
  // Untouched schedules keep their old values.
  assert.deepEqual(bus.extendDelayedTtl(otherKey as string, { ttlMs: 1000 }), {
    delayId: otherKey,
    topic: 't',
    ttlMs: 1000,
  });
  assert.deepEqual(bus.extendDelayedTtl(otherTopic as string, { ttlMs: 1000 }), {
    delayId: otherTopic,
    topic: 'u',
    ttlMs: 1000,
  });
  // Due times are untouched by a TTL extension.
  assert.deepEqual(
    bus.listDelayed().map((e) => e.deliverAt),
    [2000, 3000, 2000, 2000],
  );
});

test('extendDelayedTtlByKey extends deadlines so late schedules deliver', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Due t=3000, past their original deadlines t=2000: without an
  // extension the sweep would drop them as expired, never delivered.
  bus.publishDelayed('t', 'late-one', { delayMs: 2000, deadlineAt: 2000, key: 'k' });
  bus.publishDelayed('t', 'late-two', { delayMs: 2000, deadlineAt: 2000, key: 'k' });
  const result = bus.extendDelayedTtlByKey('t', 'k', { deadlineAt: 5000 });
  assert.equal(result.extended, 2);
  clock.nowMs = 3_000;
  bus.publish('t', 'poke'); // sweeps the due entries
  await flush();
  assert.ok(received.includes('late-one'), 'extended schedules must deliver');
  assert.ok(received.includes('late-two'), 'extended schedules must deliver');
});

test('extendDelayedTtlByKey expands a wildcard pattern but not other topics', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const eu = bus.publishDelayed('orders.eu', 'a', { delayMs: 1000, deadlineAt: 2000, key: 'k' });
  const us = bus.publishDelayed('orders.us', 'b', { delayMs: 2000, deadlineAt: 3000, key: 'k' });
  const alerts = bus.publishDelayed('alerts', 'c', { delayMs: 1000, deadlineAt: 2000, key: 'k' });
  const result = bus.extendDelayedTtlByKey('orders.*', 'k', { deadlineAt: 9000 });
  assert.deepEqual(result, { extended: 2, delayIds: [eu, us] });
  // The out-of-pattern schedule is untouched: extending it to its
  // old deadline still succeeds, which it would not do if moved.
  assert.deepEqual(bus.extendDelayedTtl(alerts as string, { deadlineAt: 2000 }), {
    delayId: alerts,
    topic: 'alerts',
    deadlineAt: 2000,
  });
});

test('extendDelayedTtlByKey counts only still-pending schedules', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const gone = bus.publishDelayed('t', 'due-now', { delayMs: 0, deadlineAt: 9000, key: 'k' });
  const cancelled = bus.publishDelayed('t', 'never', { delayMs: 5000, deadlineAt: 9000, key: 'k' });
  const staying = bus.publishDelayed('t', 'later', { delayMs: 9000, deadlineAt: 9000, key: 'k' });
  assert.equal(bus.cancelDelayed(cancelled as string), true);
  await flush(); // fans out 'due-now'
  assert.deepEqual(received, ['due-now']);
  assert.equal(gone !== staying, true); // ids are distinct handles
  const result = bus.extendDelayedTtlByKey('t', 'k', { deadlineAt: 15000 });
  assert.deepEqual(result, { extended: 1, delayIds: [staying] });
});

test('extendDelayedTtlByKey fails closed when any hit carries no extendable ttlMs', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const good = bus.publishDelayed('t', 'x', { delayMs: 1000, ttlMs: 1000, key: 'k' });
  bus.publishDelayed('t', 'y', { delayMs: 1000, deadlineAt: 9000, key: 'k' }); // no ttlMs
  assert.throws(() => bus.extendDelayedTtlByKey('t', 'k', { ttlMs: 10000 }), RangeError);
  // Zero changes: the good schedule still carries its old ttlMs, so
  // extending it to 5000 succeeds — it would fail had the batch
  // applied 10000 to it first.
  assert.deepEqual(bus.extendDelayedTtl(good as string, { ttlMs: 5000 }), {
    delayId: good,
    topic: 't',
    ttlMs: 5000,
  });
  // And the failed batch audited nothing.
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' }).length, 1);
});

test('extendDelayedTtlByKey fails closed when the new deadline precedes any hit deadline', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const early = bus.publishDelayed('t', 'x', { delayMs: 1000, deadlineAt: 2000, key: 'k' });
  const late = bus.publishDelayed('t', 'y', { delayMs: 1000, deadlineAt: 5000, key: 'k' });
  assert.throws(() => bus.extendDelayedTtlByKey('t', 'k', { deadlineAt: 3000 }), RangeError);
  // Zero changes: extending each hit to its old deadline still
  // succeeds (equal is not a shrink) — neither was moved to 3000.
  assert.deepEqual(bus.extendDelayedTtl(early as string, { deadlineAt: 2000 }), {
    delayId: early,
    topic: 't',
    deadlineAt: 2000,
  });
  assert.deepEqual(bus.extendDelayedTtl(late as string, { deadlineAt: 5000 }), {
    delayId: late,
    topic: 't',
    deadlineAt: 5000,
  });
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' }).length, 2);
});

test('extendDelayedTtlByKey extends only the TTL deadline of an EDF schedule in the batch', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 1000); // schedule-time deadline t=2000
  // EDF-class (deadlineMs without deadlineAt): its EDF deadline never
  // expires it, but its TTL deadline would drop it at its due time.
  const id = bus.publishDelayed('t', 'edf-ttl-extended', { delayMs: 2000, deadlineMs: 2000, key: 'k' });
  const result = bus.extendDelayedTtlByKey('t', 'k', { deadlineAt: 5000 });
  assert.deepEqual(result, { extended: 1, delayIds: [id] });
  clock.nowMs = 3_000;
  bus.publish('t', 'poke');
  await flush();
  assert.ok(received.includes('edf-ttl-extended'), 'EDF schedule TTL extension must deliver');
});

test('extendDelayedTtlByKey fails closed on an EDF-only schedule with no TTL deadline', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const plain = bus.publishDelayed('t', 'x', { delayMs: 1000, deadlineAt: 9000, key: 'k' });
  bus.publishDelayed('t', 'z', { delayMs: 5000, deadlineMs: 2000, key: 'k' }); // EDF-only, no TTL deadline
  assert.throws(() => bus.extendDelayedTtlByKey('t', 'k', { deadlineAt: 12000 }), RangeError);
  // The plain schedule is untouched: its old deadline still accepts
  // an equal extension.
  assert.deepEqual(bus.extendDelayedTtl(plain as string, { deadlineAt: 9000 }), {
    delayId: plain,
    topic: 't',
    deadlineAt: 9000,
  });
});

test('extendDelayedTtlByKey on an empty match returns zero, never a throw', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.deepEqual(bus.extendDelayedTtlByKey('nothing-here', 'k', { deadlineAt: 9000 }), {
    extended: 0,
    delayIds: [],
  });
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' }).length, 0);
});

test('extendDelayedTtlByKey audits one event per extended schedule and never moves deliverAt', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id1 = bus.publishDelayed('t', 'a', { delayMs: 1000, deadlineAt: 2000, key: 'k' });
  const id2 = bus.publishDelayed('t', 'b', { delayMs: 2000, deadlineAt: 3000, key: 'k' });
  bus.extendDelayedTtlByKey('t', 'k', { deadlineAt: 9000 });
  const events = bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' });
  assert.equal(events.length, 2);
  assert.deepEqual(
    events.map((e) => [e.detail.delayId, e.detail.deadlineAt]),
    [
      [id1, 9000],
      [id2, 9000],
    ],
  );
  assert.deepEqual(
    bus.listDelayed().map((e) => e.deliverAt),
    [2000, 3000],
  );
});

test('extendDelayedTtlByKey rejects bad inputs before touching anything', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.throws(() => bus.extendDelayedTtlByKey(42 as never, 'k', { deadlineAt: 9000 }), TypeError);
  assert.throws(() => bus.extendDelayedTtlByKey('', 'k', { deadlineAt: 9000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtlByKey('t', 42 as never, { deadlineAt: 9000 }), TypeError);
  assert.throws(() => bus.extendDelayedTtlByKey('t', '', { deadlineAt: 9000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtlByKey('t', 'k', {} as never), RangeError);
  assert.throws(
    () => bus.extendDelayedTtlByKey('t', 'k', { ttlMs: 1000, deadlineAt: 9000 }),
    RangeError,
  );
  assert.throws(() => bus.extendDelayedTtlByKey('t', 'k', { ttlMs: -1 }), RangeError);
  assert.throws(() => bus.extendDelayedTtlByKey('t', 'k', { ttlMs: 1.5 }), RangeError);
  assert.throws(() => bus.extendDelayedTtlByKey('t', 'k', { deadlineAt: Number.NaN }), RangeError);
  assert.deepEqual(bus.getDelayedStatsByKey('k'), { total: 0, byTopic: [] });
});
