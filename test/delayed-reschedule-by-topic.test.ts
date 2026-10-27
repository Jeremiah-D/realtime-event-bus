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

test('reschedules every pending schedule for one topic — all keys and unkeyed alike — in schedule order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const id1 = bus.publishDelayed('t', 'one', { delayMs: 1000, key: 'k1' });
  const id2 = bus.publishDelayed('t', 'two', { delayMs: 2000, key: 'k2' });
  const unkeyed = bus.publishDelayed('t', 'unkeyed', { delayMs: 1000 });
  const otherTopic = bus.publishDelayed('u', 'other-topic', { delayMs: 1000, key: 'm' });
  const result = bus.rescheduleDelayedByTopic('t', { delayMs: 9000 });
  assert.deepEqual(result, { rescheduled: 3, delayIds: [id1, id2, unkeyed] });
  // All moved schedules share one target due time (1000 + 9000),
  // keep their ids, and the untouched topic keeps its due time.
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.topic, e.key, e.deliverAt]),
    [
      [id1, 't', 'k1', 10000],
      [id2, 't', 'k2', 10000],
      [unkeyed, 't', undefined, 10000],
      [otherTopic, 'u', 'm', 2000],
    ],
  );
  assert.equal(bus.getStats().pendingDelayed, 4);
  // The moved schedules do not fire at their original due times;
  // the untouched topic does.
  clock.nowMs = 3000;
  bus.publish('u', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'other-topic']);
  // And they do fire at the new due time, with payloads intact.
  clock.nowMs = 10000;
  bus.publish('u', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'other-topic', 'poke2', 'one', 'two', 'unkeyed']);
});

test('a wildcard pattern expands to every matching concrete topic and nothing else', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('**', () => {});
  const eu = bus.publishDelayed('orders.eu', 'eu', { delayMs: 1000, key: 'k' });
  const us = bus.publishDelayed('orders.us', 'us', { delayMs: 1000 });
  bus.publishDelayed('billing.eu', 'billing', { delayMs: 1000, key: 'k' });
  const result = bus.rescheduleDelayedByTopic('orders.*', { deliverAt: 9000 });
  assert.deepEqual(result, { rescheduled: 2, delayIds: [eu, us] });
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.topic, e.deliverAt]),
    [
      ['orders.eu', 9000],
      ['orders.us', 9000],
      ['billing.eu', 2000],
    ],
  );
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
  assert.deepEqual(bus.rescheduleDelayedByTopic('t', { delayMs: 9000 }), {
    rescheduled: 1,
    delayIds: [staying],
  });
  assert.equal(bus.listDelayed()[0].deliverAt, 10000);
  // No pending schedule for an unknown topic: zero, never a throw.
  assert.deepEqual(bus.rescheduleDelayedByTopic('nobody', { delayMs: 9000 }), {
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
  bus.rescheduleDelayedByTopic('t', { deliverAt: 9000 });
  bus.rescheduleDelayedByTopic('t', { deliverAt: 9000 });
  assert.equal(bus.listDelayed()[0].deliverAt, 9000);
  assert.equal(bus.listDelayed()[0].delayId, id); // same id, never duplicated
  assert.equal(bus.cancelDelayed(id as string), true);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('rescheduleDelayedByTopic validates pattern and timing before touching anything', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000 });
  assert.throws(() => bus.rescheduleDelayedByTopic(123 as unknown as string, { delayMs: 1000 }), TypeError);
  assert.throws(() => bus.rescheduleDelayedByTopic('', { delayMs: 1000 }), RangeError);
  assert.throws(() => bus.rescheduleDelayedByTopic('t', {} as never), RangeError);
  assert.throws(
    () => bus.rescheduleDelayedByTopic('t', { delayMs: 1000, deliverAt: 9000 }),
    RangeError,
  );
  assert.throws(() => bus.rescheduleDelayedByTopic('t', { delayMs: -5 }), RangeError);
  assert.throws(() => bus.rescheduleDelayedByTopic('t', { deliverAt: Number.NaN }), RangeError);
  // Every failed move left the pending set untouched.
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.deliverAt]),
    [[id, 2000]],
  );
});

test('a topic-held schedule can be rescheduled and stays held until resume', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000 });
  assert.equal(bus.holdDelayedTopic('t'), true);
  const result = bus.rescheduleDelayedByTopic('t', { delayMs: 9000 });
  assert.deepEqual(result, { rescheduled: 1, delayIds: [held] });
  assert.equal(bus.listDelayed()[0].deliverAt, 10000);
  // Past the new due time, still held: nothing fans out.
  clock.nowMs = 11000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  // Resume fans it out on the next sweep, at the new due time.
  assert.equal(bus.resumeDelayedTopic('t'), true);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'held']);
});

test('a key-held schedule can be rescheduled by topic and stays held until its key resumes', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000, key: 'k' });
  const free = bus.publishDelayed('t', 'free', { delayMs: 1000, key: 'j' });
  assert.equal(bus.holdDelayedByKey('t', 'k'), true);
  const result = bus.rescheduleDelayedByTopic('t', { delayMs: 9000 });
  assert.deepEqual(result, { rescheduled: 2, delayIds: [held, free] });
  // The free schedule moved too (topic scope, not key scope) and
  // fires at the new due time; the held one stays pending.
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

test('a schedule moved past its deadline expires at the new due time, exactly like one due there originally', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 5000); // deadline t=6000 for a schedule made at t=1000
  bus.publishDelayed('t', 'doomed', { delayMs: 1000 }); // originally due t=2000
  bus.publishDelayed('t', 'safe', { delayMs: 1000 });
  // Move both past the deadline: both expire, neither delivers.
  // (Two schedules share the topic TTL, so both are doomed — the
  // point is the move preserves the deadline verbatim.)
  const result = bus.rescheduleDelayedByTopic('t', { deliverAt: 9000 });
  assert.equal(result.rescheduled, 2);
  clock.nowMs = 9000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 2);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('a schedule moved to before its deadline still delivers at the new due time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 5000); // deadline t=6000 for a schedule made at t=1000
  bus.publishDelayed('t', 'moved', { delayMs: 1000 }); // originally due t=2000
  bus.rescheduleDelayedByTopic('t', { deliverAt: 5000 });
  clock.nowMs = 5000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['moved']);
  assert.equal(bus.getStats().expiredMessages, 0);
});
