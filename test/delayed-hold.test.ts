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

test('a held topic does not fan out at its due time but stays pending', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000 });
  bus.publishDelayed('u', 'free', { delayMs: 1000 });
  assert.equal(bus.holdDelayedTopic('t'), true);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  // Only the unheld topic fanned out; the held schedule is still
  // pending, still listed, and still counted — not dropped, not shed.
  assert.deepEqual(received, ['poke', 'free']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  assert.deepEqual(
    bus.listDelayed().map((row) => row.delayId),
    [held],
  );
  assert.deepEqual(bus.getDelayedStats(), {
    total: 1,
    byTopic: [{ topic: 't', pending: 1, earliestDeliverAt: 2000 }],
  });
});

test('resuming fans out overdue entries on the next sweep, in order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'first', { delayMs: 1000 });
  bus.publishDelayed('t', 'second', { delayMs: 2000 });
  bus.holdDelayedTopic('t');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  // The resume itself fans nothing out; the next sweep does, and the
  // original schedule order is preserved. The live poke2 delivers
  // first (live publishes fan out synchronously); the held schedules
  // follow on the sweep flush, in schedule order.
  assert.equal(bus.resumeDelayedTopic('t'), true);
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
  bus.publishDelayed('orders.eu', 'eu', { delayMs: 1000 });
  bus.publishDelayed('orders.us', 'us', { delayMs: 1000 });
  bus.publishDelayed('billing.eu', 'billing', { delayMs: 1000 });
  bus.holdDelayedTopic('orders.*');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'billing']);
  assert.equal(bus.getStats().pendingDelayed, 2);
  bus.resumeDelayedTopic('orders.*');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'billing', 'poke2', 'eu', 'us']);
});

test('a publish scheduled during a hold still schedules, then fans out on resume', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.holdDelayedTopic('t');
  // The hold governs sweep fan-out only: scheduling still succeeds
  // and the schedule is pending like any other.
  const id = bus.publishDelayed('t', 'late', { delayMs: 1000 });
  assert.notEqual(id, undefined);
  assert.equal(bus.getStats().pendingDelayed, 1);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  bus.resumeDelayedTopic('t');
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
  bus.holdDelayedTopic('t');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  // The hold neither fanned the schedule out nor released the key:
  // the successor is still waiting, not delivered, not dropped.
  assert.deepEqual(received, []);
  bus.resumeDelayedTopic('t');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['first', 'second']);
});

test('a held entry past its deadline is not discarded while held, then expires on resume', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  // Due at 2000, absolute deadline 2500: past the deadline by the
  // time the hold lifts at 3000, so the unchanged sweep expiry rule
  // must drop it on resume — never delivered late.
  bus.publishDelayed('t', 'doomed', { delayMs: 1000, deadlineAt: 2500 });
  bus.holdDelayedTopic('t');
  clock.nowMs = 3_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  // Still in the raw pending set while held: the hold discarded
  // nothing, even past the deadline.
  assert.equal(bus.getStats().pendingDelayed, 1);
  bus.resumeDelayedTopic('t');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('repeat hold and resume are idempotent no-ops with a single audit each', async () => {
  const bus = new EventBus();
  assert.equal(bus.holdDelayedTopic('t'), true);
  assert.equal(bus.holdDelayedTopic('t'), false);
  assert.equal(bus.resumeDelayedTopic('t'), true);
  assert.equal(bus.resumeDelayedTopic('t'), false);
  const held = bus.getSubscriptionAudit({ action: 'delayed_topic_held' });
  assert.equal(held.length, 1);
  assert.equal(held[0].pattern, 't');
  assert.equal(held[0].actor, 'system');
  assert.deepEqual(held[0].detail, { topicPattern: 't' });
  const resumed = bus.getSubscriptionAudit({ action: 'delayed_topic_resumed' });
  assert.equal(resumed.length, 1);
  assert.equal(resumed[0].pattern, 't');
  assert.deepEqual(resumed[0].detail, { topicPattern: 't' });
});

test('holdDelayedTopic and resumeDelayedTopic validate their argument', () => {
  const bus = new EventBus();
  assert.throws(() => bus.holdDelayedTopic(123 as unknown as string), TypeError);
  assert.throws(() => bus.holdDelayedTopic(''), RangeError);
  assert.throws(() => bus.resumeDelayedTopic(123 as unknown as string), TypeError);
  assert.throws(() => bus.resumeDelayedTopic(''), RangeError);
  // A failed validation holds nothing.
  assert.equal(bus.resumeDelayedTopic('t'), false);
});
