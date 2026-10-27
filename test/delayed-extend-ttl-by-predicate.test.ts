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
  const clock = { nowMs: startMs, now: () => clock.nowMs } ;
  return clock;
}

type OrderPayload = { kind: string; priority: string };

const isLowPriority = (payload: unknown) =>
  (payload as OrderPayload).priority === 'low';

test('extendDelayedTtlByPredicate extends ttlMs only for predicate-matching schedules, in schedule order', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id1 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 1000, ttlMs: 1000, key: 'k1' });
  const id2 = bus.publishDelayed('t', { kind: 'order', priority: 'high' }, { delayMs: 2000, ttlMs: 2000, key: 'k2' });
  const id3 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 3000, ttlMs: 3000 });
  const otherTopic = bus.publishDelayed('u', { kind: 'order', priority: 'low' }, { delayMs: 1000, ttlMs: 1000 });
  const result = bus.extendDelayedTtlByPredicate('t', isLowPriority, { ttlMs: 10000 });
  assert.deepEqual(result, { extended: 2, delayIds: [id1, id3] });
  // Each hit is audited separately, under the single-extension name —
  // asserted here, before the verification extends below add their own
  // audit rows.
  const audits = bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' });
  assert.deepEqual(
    audits.map((e) => e.detail.delayId),
    [id1, id3],
  );
  // The new value is really stored on both hits: a smaller ttlMs now
  // fails the only-extend rule, where it would have passed before.
  assert.throws(() => bus.extendDelayedTtl(id1 as string, { ttlMs: 5000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtl(id3 as string, { ttlMs: 5000 }), RangeError);
  // The non-matching schedule and the other topic keep their old values.
  assert.deepEqual(bus.extendDelayedTtl(id2 as string, { ttlMs: 2000 }), {
    delayId: id2,
    topic: 't',
    ttlMs: 2000,
  });
  assert.deepEqual(bus.extendDelayedTtl(otherTopic as string, { ttlMs: 1000 }), {
    delayId: otherTopic,
    topic: 'u',
    ttlMs: 1000,
  });
  // Due times are untouched by a TTL extension.
  assert.deepEqual(
    bus.listDelayed().map((e) => e.deliverAt),
    [2000, 3000, 4000, 2000],
  );
});

test('extendDelayedTtlByPredicate passes the compaction key to the predicate', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const keyed = bus.publishDelayed('t', 'a', { delayMs: 1000, ttlMs: 1000, key: 'k1' });
  const unkeyed = bus.publishDelayed('t', 'b', { delayMs: 1000, ttlMs: 1000 });
  const seen: Array<unknown> = [];
  const result = bus.extendDelayedTtlByPredicate(
    't',
    (_payload, key) => {
      seen.push(key);
      return key === 'k1';
    },
    { ttlMs: 9000 },
  );
  assert.deepEqual(result, { extended: 1, delayIds: [keyed] });
  assert.deepEqual(seen, ['k1', undefined]);
  assert.throws(() => bus.extendDelayedTtl(unkeyed as string, { ttlMs: 500 }), RangeError);
  assert.deepEqual(bus.extendDelayedTtl(keyed as string, { ttlMs: 9000 }), {
    delayId: keyed,
    topic: 't',
    ttlMs: 9000,
  });
});

test('extendDelayedTtlByPredicate extends deadlines so late matching schedules deliver', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Due t=3000, past their original deadlines t=2000: without an
  // extension the sweep would drop them as expired, never delivered.
  bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 2000, deadlineAt: 2000 });
  bus.publishDelayed('t', { kind: 'order', priority: 'high' }, { delayMs: 2000, deadlineAt: 2000 });
  const result = bus.extendDelayedTtlByPredicate('t', isLowPriority, { deadlineAt: 5000 });
  assert.equal(result.extended, 1);
  clock.nowMs = 3_000;
  bus.publish('t', 'poke'); // sweeps the due entries
  await flush();
  assert.ok(received.includes('poke'));
  assert.deepEqual(
    received.filter((p) => typeof p === 'object'),
    [{ kind: 'order', priority: 'low' }],
    'only the extended schedule must deliver; the unextended one expires',
  );
});

test('extendDelayedTtlByPredicate expands a wildcard pattern but never evaluates other topics', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const evaluated: unknown[] = [];
  const eu = bus.publishDelayed('orders.eu', 'a', { delayMs: 1000, deadlineAt: 2000 });
  const us = bus.publishDelayed('orders.us', 'b', { delayMs: 2000, deadlineAt: 3000 });
  const alerts = bus.publishDelayed('alerts', 'c', { delayMs: 1000, deadlineAt: 2000 });
  const result = bus.extendDelayedTtlByPredicate(
    'orders.*',
    (payload) => {
      evaluated.push(payload);
      return true;
    },
    { deadlineAt: 9000 },
  );
  assert.deepEqual(result, { extended: 2, delayIds: [eu, us] });
  assert.deepEqual(evaluated, ['a', 'b']);
  // The out-of-pattern schedule is untouched: extending it to its
  // old deadline still succeeds, which it would not do if moved.
  assert.deepEqual(bus.extendDelayedTtl(alerts as string, { deadlineAt: 2000 }), {
    delayId: alerts,
    topic: 'alerts',
    deadlineAt: 2000,
  });
});

test('extendDelayedTtlByPredicate is fail-closed: a throwing predicate extends nothing', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const good = bus.publishDelayed('t', 'good', { delayMs: 1000, ttlMs: 1000 });
  const bad = bus.publishDelayed('t', 'bad', { delayMs: 1000, ttlMs: 1000 });
  assert.throws(
    () =>
      bus.extendDelayedTtlByPredicate(
        't',
        (payload) => {
          if (payload === 'bad') throw new Error('predicate boom');
          return true;
        },
        { ttlMs: 9000 },
      ),
    /predicate boom/,
  );
  // Zero changes: no audit record was written for either schedule —
  // asserted before the verification extends below add their own rows.
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' }), []);
  // And both schedules still carry their original TTL.
  assert.deepEqual(bus.extendDelayedTtl(good as string, { ttlMs: 1000 }), {
    delayId: good,
    topic: 't',
    ttlMs: 1000,
  });
  assert.deepEqual(bus.extendDelayedTtl(bad as string, { ttlMs: 1000 }), {
    delayId: bad,
    topic: 't',
    ttlMs: 1000,
  });
});

test('extendDelayedTtlByPredicate is fail-closed: one unextendable hit extends nothing', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const good = bus.publishDelayed('t', 'good', { delayMs: 1000, ttlMs: 1000 });
  const noTtl = bus.publishDelayed('t', 'no-ttl', { delayMs: 1000 });
  assert.throws(
    () => bus.extendDelayedTtlByPredicate('t', () => true, { ttlMs: 9000 }),
    RangeError,
  );
  // Zero changes — asserted before the verification extend below adds
  // its own audit row.
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' }), []);
  assert.deepEqual(bus.extendDelayedTtl(good as string, { ttlMs: 1000 }), {
    delayId: good,
    topic: 't',
    ttlMs: 1000,
  });
  // A deadline earlier than a hit's governing deadline fails the same way.
  const withDeadline = bus.publishDelayed('t', 'd', { delayMs: 1000, deadlineAt: 5000 });
  assert.throws(
    () => bus.extendDelayedTtlByPredicate('t', (p) => p === 'd', { deadlineAt: 4000 }),
    RangeError,
  );
  assert.deepEqual(bus.extendDelayedTtl(withDeadline as string, { deadlineAt: 5000 }), {
    delayId: withDeadline,
    topic: 't',
    deadlineAt: 5000,
  });
  void noTtl;
});

test('extendDelayedTtlByPredicate rejects bad patterns, predicates, and timing before evaluating', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('t', 'x', { delayMs: 1000, ttlMs: 1000 });
  let evaluated = 0;
  const counting = () => {
    evaluated += 1;
    return true;
  };
  assert.throws(() => bus.extendDelayedTtlByPredicate(42 as never, counting, { ttlMs: 9000 }), TypeError);
  assert.throws(() => bus.extendDelayedTtlByPredicate('', counting, { ttlMs: 9000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtlByPredicate('t', null as never, { ttlMs: 9000 }), TypeError);
  assert.throws(() => bus.extendDelayedTtlByPredicate('t', counting, {} as never), RangeError);
  assert.throws(() => bus.extendDelayedTtlByPredicate('t', counting, { ttlMs: 9000, deadlineAt: 9000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtlByPredicate('t', counting, { ttlMs: -1 }), RangeError);
  assert.equal(evaluated, 0, 'no validation failure may trigger a predicate call');
});

test('extendDelayedTtlByPredicate returns an empty result when nothing matches', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('t', 'x', { delayMs: 1000, ttlMs: 1000 });
  assert.deepEqual(bus.extendDelayedTtlByPredicate('t', () => false, { ttlMs: 9000 }), {
    extended: 0,
    delayIds: [],
  });
  assert.deepEqual(bus.extendDelayedTtlByPredicate('nope', () => true, { ttlMs: 9000 }), {
    extended: 0,
    delayIds: [],
  });
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' }), []);
});

test('extendDelayedTtlByPredicate extends a held schedule and leaves the hold in place', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000, ttlMs: 1000 });
  assert.equal(bus.holdDelayedTopic('t'), true);
  const result = bus.extendDelayedTtlByPredicate('t', () => true, { ttlMs: 10000 });
  assert.deepEqual(result, { extended: 1, delayIds: [held] });
  // Still held: past its due time it does not fan out.
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  // The extension landed despite the hold: the old TTL now fails the
  // only-extend rule.
  assert.throws(() => bus.extendDelayedTtl(held as string, { ttlMs: 5000 }), RangeError);
  // Resuming fans it out at last.
  assert.equal(bus.resumeDelayedTopic('t'), true);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'held']);
});
