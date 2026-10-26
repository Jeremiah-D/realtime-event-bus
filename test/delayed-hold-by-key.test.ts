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

test('a held key does not fan out at its due time but stays pending and counted', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('t', 'free-key', { delayMs: 1000, key: 'j' });
  bus.publishDelayed('u', 'free-topic', { delayMs: 1000, key: 'm' });
  assert.equal(bus.holdDelayedByKey('t', 'k'), true);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  // Only the held schedule is held back; a different key in the
  // same topic and a different key in another topic fan out.
  // (A same-key schedule in another topic is a different case —
  // per-key order couples it to the held schedule, see the
  // coupling test below — so it uses its own key here.)
  assert.deepEqual(received, ['poke', 'free-key', 'free-topic']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  assert.deepEqual(
    bus.listDelayed().map((row) => row.delayId),
    [held],
  );
  assert.deepEqual(bus.getDelayedStats(), {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 2000 }],
  });
  assert.deepEqual(bus.getDelayedStatsByKey('k'), {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 2000 }],
  });
});

test('resuming fans out overdue entries on the next sweep, in order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'first', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('t', 'second', { delayMs: 2000, key: 'k' });
  bus.holdDelayedByKey('t', 'k');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  // The resume itself fans nothing out; the next sweep does, and the
  // original schedule order is preserved.
  assert.equal(bus.resumeDelayedByKey('t', 'k'), true);
  assert.deepEqual(received, ['poke']);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'first', 'second']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('a wildcard hold covers every matching topic and nothing else', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('orders.eu', 'eu', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('orders.us', 'us', { delayMs: 1000, key: 'k' });
  // A different key, so per-key order cannot couple it to the held
  // schedules: it must fan out while they stay held.
  bus.publishDelayed('billing.eu', 'billing', { delayMs: 1000, key: 'j' });
  bus.holdDelayedByKey('orders.*', 'k');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'billing']);
  assert.equal(bus.getStats().pendingDelayed, 2);
  bus.resumeDelayedByKey('orders.*', 'k');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'billing', 'poke2', 'eu', 'us']);
});

test('a same-key schedule in another topic waits for the held schedule, then delivers in order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'held', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('u', 'later-same-key', { delayMs: 1000, key: 'k' });
  bus.holdDelayedByKey('t', 'k');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  // The key hold covers only topic 't', so the 'u' schedule is not
  // held — but per-key order couples it to the earlier held
  // schedule: it cannot overtake keySeq 1, so it waits too, and
  // the held schedule stays the only pending one.
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  bus.resumeDelayedByKey('t', 'k');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'held', 'later-same-key']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('a publish scheduled during a hold still schedules, then fans out on resume', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.holdDelayedByKey('t', 'k');
  // The hold governs sweep fan-out only: scheduling still succeeds
  // and the schedule is pending like any other.
  const id = bus.publishDelayed('t', 'late', { delayMs: 1000, key: 'k' });
  assert.notEqual(id, undefined);
  assert.equal(bus.getStats().pendingDelayed, 1);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  bus.resumeDelayedByKey('t', 'k');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'late']);
});

test('a held keyed schedule keeps its key sequence: successors still wait', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'first', { delayMs: 1000, key: 'k' });
  bus.publish('t', 'second', { key: 'k' });
  await flush();
  assert.deepEqual(received, []); // 'second' waits for keySeq 1
  bus.holdDelayedByKey('t', 'k');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  // The hold neither fanned the schedule out nor released the key:
  // the successor is still waiting, and nothing was delivered.
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().pendingDelayed, 1);
  bus.resumeDelayedByKey('t', 'k');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['first', 'second']);
});

test('an unkeyed schedule is never held by a key hold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'plain', { delayMs: 1000 });
  bus.holdDelayedByKey('t', 'k');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'plain']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('a held entry past its deadline at resume time is dropped, never delivered late', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'doomed', { delayMs: 1000, deadlineAt: 3000, key: 'k' });
  bus.holdDelayedByKey('t', 'k');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  // Still in the raw pending set while held: the hold discarded
  // nothing, even past the deadline.
  assert.equal(bus.getStats().pendingDelayed, 1);
  bus.resumeDelayedByKey('t', 'k');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('resuming the key hold does not release a topic hold, and vice versa', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'held', { delayMs: 1000, key: 'k' });
  bus.holdDelayedByKey('t', 'k');
  bus.holdDelayedTopic('t');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  // Releasing the key hold alone leaves the topic hold in place.
  assert.equal(bus.resumeDelayedByKey('t', 'k'), true);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2']);
  // Only releasing the topic hold fans the entry out.
  assert.equal(bus.resumeDelayedTopic('t'), true);
  bus.publish('x', 'poke3');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'poke3', 'held']);
});

test('repeat hold and resume are idempotent no-ops with a single audit each', async () => {
  const bus = new EventBus();
  assert.equal(bus.holdDelayedByKey('t', 'k'), true);
  assert.equal(bus.holdDelayedByKey('t', 'k'), false);
  assert.equal(bus.resumeDelayedByKey('t', 'k'), true);
  assert.equal(bus.resumeDelayedByKey('t', 'k'), false);
  const held = bus.getSubscriptionAudit({ action: 'delayed_key_held' });
  assert.equal(held.length, 1);
  assert.equal(held[0].pattern, 't');
  assert.equal(held[0].actor, 'system');
  assert.deepEqual(held[0].detail, { topicPattern: 't', key: 'k' });
  const resumed = bus.getSubscriptionAudit({ action: 'delayed_key_resumed' });
  assert.equal(resumed.length, 1);
  assert.equal(resumed[0].pattern, 't');
  assert.deepEqual(resumed[0].detail, { topicPattern: 't', key: 'k' });
});

test('holdDelayedByKey and resumeDelayedByKey validate their arguments', () => {
  const bus = new EventBus();
  assert.throws(() => bus.holdDelayedByKey(123 as unknown as string, 'k'), TypeError);
  assert.throws(() => bus.holdDelayedByKey('t', 123 as unknown as string), TypeError);
  assert.throws(() => bus.holdDelayedByKey('', 'k'), RangeError);
  assert.throws(() => bus.holdDelayedByKey('t', ''), RangeError);
  assert.throws(() => bus.resumeDelayedByKey(123 as unknown as string, 'k'), TypeError);
  assert.throws(() => bus.resumeDelayedByKey('t', 123 as unknown as string), TypeError);
  assert.throws(() => bus.resumeDelayedByKey('', 'k'), RangeError);
  assert.throws(() => bus.resumeDelayedByKey('t', ''), RangeError);
  // A failed validation holds nothing.
  assert.equal(bus.resumeDelayedByKey('t', 'k'), false);
});
