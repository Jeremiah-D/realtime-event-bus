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

type OrderPayload = { kind: string; priority: string };

const isLowPriority = (payload: unknown) =>
  (payload as OrderPayload).priority === 'low';

test('reschedules only the pending schedules whose payload satisfies the predicate, in schedule order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const id1 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 1000, key: 'k1' });
  const id2 = bus.publishDelayed('t', { kind: 'order', priority: 'high' }, { delayMs: 2000, key: 'k2' });
  const id3 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 3000 });
  const otherTopic = bus.publishDelayed('u', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  const result = bus.rescheduleDelayedByPredicate('t', isLowPriority, { delayMs: 9000 });
  assert.deepEqual(result, { rescheduled: 2, delayIds: [id1, id3] });
  // Both moved schedules share one target due time (1000 + 9000) and
  // keep their ids, keys, and payloads; the rest keep their due times.
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.topic, e.key, e.deliverAt]),
    [
      [id1, 't', 'k1', 10000],
      [id2, 't', 'k2', 3000],
      [id3, 't', undefined, 10000],
      [otherTopic, 'u', undefined, 2000],
    ],
  );
  assert.equal(bus.getStats().pendingDelayed, 4);
  const audits = bus.getSubscriptionAudit({ action: 'delayed_rescheduled' });
  assert.equal(audits.length, 2, 'each moved schedule is audited separately');
  assert.deepEqual(
    audits.map((e) => e.detail.delayId),
    [id1, id3],
  );
  // The moved schedules do not fire at their original due times;
  // the untouched ones do.
  clock.nowMs = 3000;
  bus.publish('u', 'poke');
  await flush();
  assert.deepEqual(received, [
    'poke',
    { kind: 'order', priority: 'low' },
    { kind: 'order', priority: 'high' },
  ]);
  // And they do fire at the new due time, with payloads intact.
  clock.nowMs = 10000;
  bus.publish('u', 'poke2');
  await flush();
  assert.deepEqual(received, [
    'poke',
    { kind: 'order', priority: 'low' },
    { kind: 'order', priority: 'high' },
    'poke2',
    { kind: 'order', priority: 'low' },
    { kind: 'order', priority: 'low' },
  ]);
});

test('predicate receives the entry key, so it can select by key without knowing ids', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('**', () => {});
  const id1 = bus.publishDelayed('t', 'for-k', { delayMs: 1000, key: 'k' });
  const id2 = bus.publishDelayed('t', 'for-j', { delayMs: 1000, key: 'j' });
  const unkeyed = bus.publishDelayed('t', 'unkeyed', { delayMs: 1000 });
  const seenKeys: (string | undefined)[] = [];
  const result = bus.rescheduleDelayedByPredicate(
    't',
    (_payload, key) => {
      seenKeys.push(key);
      return key === 'k';
    },
    { deliverAt: 9000 },
  );
  assert.deepEqual(result, { rescheduled: 1, delayIds: [id1] });
  // The predicate saw every topic-matching entry, unkeyed included.
  assert.deepEqual(seenKeys, ['k', 'j', undefined]);
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.deliverAt]),
    [
      [id1, 9000],
      [id2, 2000],
      [unkeyed, 2000],
    ],
  );
  assert.equal(bus.getStats().pendingDelayed, 3);
});

test('a wildcard pattern expands to every matching concrete topic, and entries outside the pattern are never evaluated', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('**', () => {});
  const eu = bus.publishDelayed('orders.eu', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  const us = bus.publishDelayed('orders.us', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  bus.publishDelayed('billing.eu', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  let evaluated = 0;
  const result = bus.rescheduleDelayedByPredicate(
    'orders.*',
    (payload) => {
      evaluated += 1;
      return isLowPriority(payload);
    },
    { deliverAt: 9000 },
  );
  assert.deepEqual(result, { rescheduled: 2, delayIds: [eu, us] });
  // Only the two orders.* entries were evaluated, never the billing one.
  assert.equal(evaluated, 2);
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.topic, e.deliverAt]),
    [
      ['orders.eu', 9000],
      ['orders.us', 9000],
      ['billing.eu', 2000],
    ],
  );
});

test('a throwing predicate fails the whole batch closed: zero moves, pending set untouched', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id1 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  const id2 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 2000 });
  const id3 = bus.publishDelayed('t', { kind: 'order', priority: 'high' }, { delayMs: 3000 });
  assert.equal(bus.getStats().pendingDelayed, 3);
  // Throws on the second entry — after the first already matched — so
  // a move-as-you-go implementation would have half-moved the batch.
  let calls = 0;
  assert.throws(
    () =>
      bus.rescheduleDelayedByPredicate(
        't',
        (payload) => {
          calls += 1;
          if (calls === 2) throw new Error('predicate blew up');
          return isLowPriority(payload);
        },
        { delayMs: 9000 },
      ),
    /predicate blew up/,
  );
  assert.equal(bus.getStats().pendingDelayed, 3);
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.deliverAt]),
    [
      [id1, 2000],
      [id2, 3000],
      [id3, 4000],
    ],
  );
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delayed_rescheduled' }), []);
  // A retry with a fixed predicate works against the untouched remainder.
  assert.deepEqual(bus.rescheduleDelayedByPredicate('t', isLowPriority, { delayMs: 9000 }), {
    rescheduled: 2,
    delayIds: [id1, id2],
  });
});

test('rescheduleDelayedByPredicate validates pattern, predicate, and timing before evaluating anything', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000 });
  let evaluated = 0;
  const counting = () => {
    evaluated += 1;
    return true;
  };
  assert.throws(
    () => bus.rescheduleDelayedByPredicate(123 as unknown as string, counting, { delayMs: 1000 }),
    TypeError,
  );
  assert.throws(() => bus.rescheduleDelayedByPredicate('', counting, { delayMs: 1000 }), RangeError);
  assert.throws(
    () => bus.rescheduleDelayedByPredicate('t', 'nope' as never, { delayMs: 1000 }),
    TypeError,
  );
  assert.throws(
    () => bus.rescheduleDelayedByPredicate('t', counting, {} as never),
    RangeError,
  );
  assert.throws(
    () => bus.rescheduleDelayedByPredicate('t', counting, { delayMs: 1000, deliverAt: 9000 }),
    RangeError,
  );
  assert.throws(
    () => bus.rescheduleDelayedByPredicate('t', counting, { delayMs: -5 }),
    RangeError,
  );
  assert.throws(
    () => bus.rescheduleDelayedByPredicate('t', counting, { deliverAt: Number.NaN }),
    RangeError,
  );
  // No failed call evaluated the predicate or moved anything.
  assert.equal(evaluated, 0);
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.deliverAt]),
    [[id, 2000]],
  );
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delayed_rescheduled' }), []);
});

test('fanned-out and cancelled schedules are not counted; an empty move returns zero', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const gone = bus.publishDelayed('t', 'due-now', { delayMs: 0 });
  await flush();
  const cancelled = bus.publishDelayed('t', 'never', { delayMs: 5000 });
  assert.equal(bus.cancelDelayed(cancelled as string), true);
  const staying = bus.publishDelayed('t', 'stays', { delayMs: 5000 });
  assert.deepEqual(bus.rescheduleDelayedByPredicate('t', () => true, { delayMs: 9000 }), {
    rescheduled: 1,
    delayIds: [staying],
  });
  assert.equal(bus.listDelayed()[0].deliverAt, 10000);
  // No pending schedule for an unknown topic: zero, never a throw.
  assert.deepEqual(bus.rescheduleDelayedByPredicate('nobody', () => true, { delayMs: 9000 }), {
    rescheduled: 0,
    delayIds: [],
  });
  assert.equal(gone !== staying, true); // the fanned-out id was never re-moved
});

test('a moved schedule is still cancellable, and a second identical move is a no-op', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000 });
  bus.rescheduleDelayedByPredicate('t', () => true, { deliverAt: 9000 });
  bus.rescheduleDelayedByPredicate('t', () => true, { deliverAt: 9000 });
  assert.equal(bus.listDelayed()[0].deliverAt, 9000);
  assert.equal(bus.listDelayed()[0].delayId, id); // same id, never duplicated
  assert.equal(bus.cancelDelayed(id as string), true);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('a topic-held schedule can be rescheduled by predicate and stays held until resume', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000 });
  const free = bus.publishDelayed('t', 'free', { delayMs: 1000 });
  assert.equal(bus.holdDelayedTopic('t'), true);
  // The predicate selects only the held schedule: the other schedule on
  // the same held topic keeps its due time (both stay held, since the
  // hold is topic-wide — the free schedule's fan-out is gated too).
  const result = bus.rescheduleDelayedByPredicate('t', (payload) => payload === 'held', {
    delayMs: 9000,
  });
  assert.deepEqual(result, { rescheduled: 1, delayIds: [held] });
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.deliverAt]),
    [
      [held, 10000],
      [free, 2000],
    ],
  );
  // Past the new due time, still held: nothing fans out except the poke.
  clock.nowMs = 11000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().pendingDelayed, 2);
  // Resume fans both out on the next sweep, earliest due time first.
  assert.equal(bus.resumeDelayedTopic('t'), true);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'free', 'held']);
});

test('a key-held schedule can be rescheduled by predicate and stays held until its key resumes', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000, key: 'k' });
  const free = bus.publishDelayed('t', 'free', { delayMs: 1000, key: 'j' });
  assert.equal(bus.holdDelayedByKey('t', 'k'), true);
  const result = bus.rescheduleDelayedByPredicate('t', () => true, { delayMs: 9000 });
  assert.deepEqual(result, { rescheduled: 2, delayIds: [held, free] });
  // The free schedule moved too and fires at the new due time; the held one stays pending.
  clock.nowMs = 11000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'free']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  assert.equal(bus.resumeDelayedByKey('t', 'k'), true);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'free', 'poke2', 'held']);
});
