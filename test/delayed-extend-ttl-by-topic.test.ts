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

test('extendDelayedTtlByTopic extends ttlMs for every hit of one topic, in schedule order', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id1 = bus.publishDelayed('t', 'one', { delayMs: 1000, ttlMs: 1000 });
  const id2 = bus.publishDelayed('t', 'two', { delayMs: 2000, ttlMs: 2000 });
  const otherTopic = bus.publishDelayed('u', 'other-topic', { delayMs: 1000, ttlMs: 1000 });
  const result = bus.extendDelayedTtlByTopic('t', { ttlMs: 10000 });
  assert.deepEqual(result, { extended: 2, delayIds: [id1, id2] });
  // The new value is really stored on both hits: a smaller ttlMs now
  // fails the only-extend rule, where it would have passed before.
  assert.throws(() => bus.extendDelayedTtl(id1 as string, { ttlMs: 5000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtl(id2 as string, { ttlMs: 5000 }), RangeError);
  // The untouched topic keeps its old value.
  assert.deepEqual(bus.extendDelayedTtl(otherTopic as string, { ttlMs: 1000 }), {
    delayId: otherTopic,
    topic: 'u',
    ttlMs: 1000,
  });
  // Due times are untouched by a TTL extension.
  assert.deepEqual(
    bus.listDelayed().map((e) => e.deliverAt),
    [2000, 3000, 2000],
  );
});

test('extendDelayedTtlByTopic extends deadlines so late schedules deliver', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Due t=3000, past their original deadlines t=2000: without an
  // extension the sweep would drop them as expired, never delivered.
  bus.publishDelayed('t', 'late-one', { delayMs: 2000, deadlineAt: 2000 });
  bus.publishDelayed('t', 'late-two', { delayMs: 2000, deadlineAt: 2000 });
  const result = bus.extendDelayedTtlByTopic('t', { deadlineAt: 5000 });
  assert.equal(result.extended, 2);
  clock.nowMs = 3_000;
  bus.publish('t', 'poke'); // sweeps the due entries
  await flush();
  assert.ok(received.includes('late-one'), 'extended schedules must deliver');
  assert.ok(received.includes('late-two'), 'extended schedules must deliver');
});

test('extendDelayedTtlByTopic expands a wildcard pattern but not other topics', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const eu = bus.publishDelayed('orders.eu', 'a', { delayMs: 1000, deadlineAt: 2000 });
  const us = bus.publishDelayed('orders.us', 'b', { delayMs: 2000, deadlineAt: 3000 });
  const alerts = bus.publishDelayed('alerts', 'c', { delayMs: 1000, deadlineAt: 2000 });
  const result = bus.extendDelayedTtlByTopic('orders.*', { deadlineAt: 9000 });
  assert.deepEqual(result, { extended: 2, delayIds: [eu, us] });
  // The out-of-pattern schedule is untouched: extending it to its
  // old deadline still succeeds, which it would not do if moved.
  assert.deepEqual(bus.extendDelayedTtl(alerts as string, { deadlineAt: 2000 }), {
    delayId: alerts,
    topic: 'alerts',
    deadlineAt: 2000,
  });
});

test('extendDelayedTtlByTopic is fail-closed: one unextendable hit extends nothing', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const good = bus.publishDelayed('t', 'good', { delayMs: 1000, ttlMs: 1000 });
  // No per-message TTL: the ttlMs form cannot extend this one.
  bus.publishDelayed('t', 'bare', { delayMs: 1000 });
  assert.throws(() => bus.extendDelayedTtlByTopic('t', { ttlMs: 5000 }), RangeError);
  // Zero changes: the good schedule still carries its old TTL.
  assert.deepEqual(bus.extendDelayedTtl(good as string, { ttlMs: 1000 }), {
    delayId: good,
    topic: 't',
    ttlMs: 1000,
  });
});

test('extendDelayedTtlByTopic is fail-closed on a shortening deadline', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const first = bus.publishDelayed('t', 'first', { delayMs: 1000, deadlineAt: 9000 });
  bus.publishDelayed('t', 'second', { delayMs: 1000, deadlineAt: 2000 });
  assert.throws(() => bus.extendDelayedTtlByTopic('t', { deadlineAt: 5000 }), RangeError);
  // The first schedule is untouched: extending it to 9000 still works.
  assert.deepEqual(bus.extendDelayedTtl(first as string, { deadlineAt: 9000 }), {
    delayId: first,
    topic: 't',
    deadlineAt: 9000,
  });
});

test('extendDelayedTtlByTopic on an empty match returns zero, never throws', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.extendDelayedTtlByTopic('nobody', { ttlMs: 5000 }), {
    extended: 0,
    delayIds: [],
  });
});

test('extendDelayedTtlByTopic validates pattern and timing before touching anything', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id = bus.publishDelayed('t', 'one', { delayMs: 1000, ttlMs: 1000 });
  assert.throws(() => bus.extendDelayedTtlByTopic(123 as unknown as string, { ttlMs: 5000 }), TypeError);
  assert.throws(() => bus.extendDelayedTtlByTopic('', { ttlMs: 5000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtlByTopic('t', {}), RangeError);
  assert.throws(
    () => bus.extendDelayedTtlByTopic('t', { ttlMs: 5000, deadlineAt: 9000 }),
    RangeError,
  );
  // A failed call changed nothing.
  assert.deepEqual(bus.extendDelayedTtl(id as string, { ttlMs: 1000 }), {
    delayId: id,
    topic: 't',
    ttlMs: 1000,
  });
});

test('extendDelayedTtlByTopic audits each hit as delayed_ttl_extended', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id1 = bus.publishDelayed('t', 'one', { delayMs: 1000, ttlMs: 1000 });
  const id2 = bus.publishDelayed('t', 'two', { delayMs: 2000, ttlMs: 2000 });
  bus.extendDelayedTtlByTopic('t', { ttlMs: 5000 });
  const audit = bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' });
  assert.equal(audit.length, 2);
  assert.deepEqual(
    audit.map((entry) => entry.detail.delayId),
    [id1, id2],
  );
});
