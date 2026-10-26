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

test('cancels only the pending schedules whose payload satisfies the predicate, in schedule order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const id1 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  const id2 = bus.publishDelayed('t', { kind: 'order', priority: 'high' }, { delayMs: 2000 });
  const id3 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 3000 });
  assert.equal(bus.getStats().pendingDelayed, 3);
  const result = bus.cancelDelayedByPredicate('t', isLowPriority);
  assert.deepEqual(result, { cancelled: 2, delayIds: [id1, id3] });
  assert.equal(bus.getStats().pendingDelayed, 1);
  // Cancelling a predicate-cancelled id directly afterwards is a no-op false.
  assert.equal(bus.cancelDelayed(id1 as string), false);
  clock.nowMs = 5_000;
  bus.publish('u', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', { kind: 'order', priority: 'high' }]);
});

test('predicate receives the entry key, so it can select by key without knowing ids', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const id1 = bus.publishDelayed('t', 'for-k', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('t', 'for-j', { delayMs: 1000, key: 'j' });
  const unkeyed = bus.publishDelayed('t', 'unkeyed', { delayMs: 1000 });
  const seenKeys: (string | undefined)[] = [];
  const result = bus.cancelDelayedByPredicate('t', (_payload, key) => {
    seenKeys.push(key);
    return key === 'k';
  });
  assert.deepEqual(result, { cancelled: 1, delayIds: [id1] });
  // The predicate saw every topic-matching entry, unkeyed included.
  assert.deepEqual(seenKeys, ['k', 'j', undefined]);
  assert.equal(bus.getStats().pendingDelayed, 2);
  clock.nowMs = 5_000;
  bus.publish('u', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'for-j', 'unkeyed']);
  void unkeyed;
});

test('a wildcard pattern expands to every matching concrete topic, and entries outside the pattern are never evaluated', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const eu = bus.publishDelayed('orders.eu', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  const us = bus.publishDelayed('orders.us', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  bus.publishDelayed('billing.eu', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  let evaluated = 0;
  const result = bus.cancelDelayedByPredicate('orders.*', (payload) => {
    evaluated += 1;
    return isLowPriority(payload);
  });
  assert.deepEqual(result, { cancelled: 2, delayIds: [eu, us] });
  // Only the two orders.* entries were evaluated, never the billing one.
  assert.equal(evaluated, 2);
  assert.equal(bus.getStats().pendingDelayed, 1);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', { kind: 'order', priority: 'low' }]);
});

test('a throwing predicate fails the whole batch closed: zero cancellations, pending set untouched', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id1 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  const id2 = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 2000 });
  bus.publishDelayed('t', { kind: 'order', priority: 'high' }, { delayMs: 3000 });
  assert.equal(bus.getStats().pendingDelayed, 3);
  // Throws on the second entry — after the first already matched — so a
  // cancel-as-you-go implementation would have half-cancelled the batch.
  let calls = 0;
  assert.throws(
    () =>
      bus.cancelDelayedByPredicate('t', (payload) => {
        calls += 1;
        if (calls === 2) throw new Error('predicate blew up');
        return isLowPriority(payload);
      }),
    /predicate blew up/,
  );
  assert.equal(bus.getStats().pendingDelayed, 3);
  assert.equal(bus.cancelDelayed(id1 as string), true); // still cancellable
  assert.equal(bus.cancelDelayed(id2 as string), true);
  // A retry with a fixed predicate works against the untouched remainder.
  assert.deepEqual(bus.cancelDelayedByPredicate('t', isLowPriority), {
    cancelled: 0,
    delayIds: [],
  });
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('a throwing predicate on a non-matching topic never fires: the batch is unaffected', () => {
  const bus = new EventBus();
  bus.publishDelayed('other', 'x', { delayMs: 1000 });
  assert.deepEqual(
    bus.cancelDelayedByPredicate('t', () => {
      throw new Error('must not be called');
    }),
    { cancelled: 0, delayIds: [] },
  );
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('already fanned out, expired, or cancelled schedules are not counted, and a repeat call is empty', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  // Fanned out: due immediately, flushed before the bulk cancel.
  const fanned = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 0 });
  await flush();
  // Cancelled individually first.
  const single = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 5000 });
  assert.equal(bus.cancelDelayed(single as string), true);
  const result = bus.cancelDelayedByPredicate('t', isLowPriority);
  assert.deepEqual(result, { cancelled: 0, delayIds: [] });
  assert.equal(bus.cancelDelayed(fanned as string), false);
  // A repeat of a successful bulk cancel counts each schedule at most once.
  const id = bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 5000 });
  assert.deepEqual(bus.cancelDelayedByPredicate('t', isLowPriority), {
    cancelled: 1,
    delayIds: [id],
  });
  assert.deepEqual(bus.cancelDelayedByPredicate('t', isLowPriority), {
    cancelled: 0,
    delayIds: [],
  });
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('empty result on a topic with no pending schedules — never throws', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publishDelayed('t', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  assert.deepEqual(bus.cancelDelayedByPredicate('nothing-here', isLowPriority), {
    cancelled: 0,
    delayIds: [],
  });
  assert.deepEqual(bus.cancelDelayedByPredicate('t', () => false), {
    cancelled: 0,
    delayIds: [],
  });
  // The unrelated schedule is untouched.
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('keyed schedules release their per-key expectations on predicate cancel, other keys unaffected', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // The delayed schedule draws keySeq 1 for 'k' at schedule time; the
  // live keyed publish draws keySeq 2 and buffers behind it.
  bus.publishDelayed('t', 'first', { delayMs: 5000, key: 'k' });
  bus.publish('t', 'second', { key: 'k' });
  // Another key's delayed schedule is independent of the 'k' cancel.
  bus.publishDelayed('t', 'other', { delayMs: 5000, key: 'j' });
  bus.publish('t', 'other-live', { key: 'j' });
  await flush();
  assert.deepEqual(received, []); // both live messages wait for their keySeq 1
  const result = bus.cancelDelayedByPredicate('t', (_payload, key) => key === 'k');
  assert.equal(result.cancelled, 1);
  await flush();
  // Only the 'k' successor was released; 'j' still waits.
  assert.deepEqual(received, ['second']);
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('subaudit records delayed_cancelled with cancelled, delayIds and topicPattern', () => {
  const bus = new EventBus();
  const id1 = bus.publishDelayed('orders.eu', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  const id2 = bus.publishDelayed('orders.us', { kind: 'order', priority: 'low' }, { delayMs: 1000 });
  const result = bus.cancelDelayedByPredicate('orders.*', isLowPriority);
  assert.equal(result.cancelled, 2);
  const events = bus.getSubscriptionAudit({ action: 'delayed_cancelled' });
  assert.equal(events.length, 1);
  assert.equal(events[0].pattern, 'orders.*');
  assert.equal(events[0].actor, 'system');
  assert.deepEqual(events[0].detail, {
    cancelled: 2,
    delayIds: [id1, id2],
    topicPattern: 'orders.*',
  });
  // The empty case is audited too: the operator action is on record even
  // when nothing was pending.
  bus.cancelDelayedByPredicate('nothing-here', isLowPriority);
  const all = bus.getSubscriptionAudit({ action: 'delayed_cancelled' });
  assert.equal(all.length, 2);
  assert.deepEqual(all[1].detail, {
    cancelled: 0,
    delayIds: [],
    topicPattern: 'nothing-here',
  });
});

test('cancelDelayedByPredicate validates its arguments', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.cancelDelayedByPredicate(123 as unknown as string, isLowPriority),
    TypeError,
  );
  assert.throws(() => bus.cancelDelayedByPredicate('', isLowPriority), RangeError);
  assert.throws(
    () => bus.cancelDelayedByPredicate('t', 123 as unknown as (payload: unknown) => unknown),
    TypeError,
  );
  assert.throws(
    () => bus.cancelDelayedByPredicate('t', 'yes' as unknown as (payload: unknown) => unknown),
    TypeError,
  );
  assert.throws(
    () => bus.cancelDelayedByPredicate('t', null as unknown as (payload: unknown) => unknown),
    TypeError,
  );
  assert.deepEqual(bus.cancelDelayedByPredicate('t', isLowPriority), {
    cancelled: 0,
    delayIds: [],
  });
});
