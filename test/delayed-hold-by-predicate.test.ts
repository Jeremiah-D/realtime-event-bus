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

test('a predicate-held schedule does not fan out at its due time but stays pending and counted', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('t', 'free-key', { delayMs: 1000, key: 'j' });
  bus.publishDelayed('u', 'free-topic', { delayMs: 1000, key: 'm' });
  assert.deepEqual(bus.holdDelayedByPredicate('t', (payload) => payload === 'held'), {
    held: 1,
    delayIds: [held],
  });
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  // Only the predicate-matching schedule is held back; a different
  // payload in the same topic and a matching payload in another
  // topic fan out (the topic scope, not the predicate alone, bounds
  // the hold).
  assert.deepEqual(received, ['poke', 'free-key', 'free-topic']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  assert.deepEqual(
    bus.listDelayed().map((row) => row.delayId),
    [held],
  );
  const events = bus.getSubscriptionAudit({ action: 'delayed_predicate_held' });
  assert.equal(events.length, 1);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].topic, 't');
  assert.deepEqual(events[0].detail, { topicPattern: 't', delayId: held });
});

test('resuming fans out overdue entries on the next sweep, in order, and the resume itself fans nothing out', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'first', { delayMs: 1000 });
  bus.publishDelayed('t', 'second', { delayMs: 2000 });
  bus.publishDelayed('t', 'other', { delayMs: 1000 });
  bus.holdDelayedByPredicate('t', (payload) => payload === 'first' || payload === 'second');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'other']);
  const resumed = bus.resumeDelayedByPredicate('t', (payload) => payload === 'first' || payload === 'second');
  assert.equal(resumed.resumed, 2);
  assert.deepEqual(received, ['poke', 'other']);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'other', 'poke2', 'first', 'second']);
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
  assert.equal(bus.holdDelayedByPredicate('orders.*', () => true).held, 2);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'billing']);
  assert.equal(bus.getStats().pendingDelayed, 2);
  bus.resumeDelayedByPredicate('orders.*', () => true);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'billing', 'poke2', 'eu', 'us']);
});

test('a schedule published during a predicate hold is not held: the hold is a snapshot, not a rule', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const held = bus.publishDelayed('t', 'held', { delayMs: 1000 });
  bus.holdDelayedByPredicate('t', (payload) => payload === 'held');
  // A later schedule carrying the same payload would match the
  // predicate, but the hold already happened — it was never
  // evaluated, so it fans out on its own due time.
  bus.publishDelayed('t', 'held', { delayMs: 1000 });
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'held']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  assert.deepEqual(
    bus.listDelayed().map((row) => row.delayId),
    [held],
  );
});

test('resuming the predicate hold does not release a topic hold, and vice versa', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'held', { delayMs: 1000 });
  bus.holdDelayedByPredicate('t', (payload) => payload === 'held');
  bus.holdDelayedTopic('t');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  // Releasing the predicate hold alone leaves the topic hold in place.
  assert.equal(bus.resumeDelayedByPredicate('t', (payload) => payload === 'held').resumed, 1);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2']);
  // Only releasing the topic hold fans the entry out.
  assert.equal(bus.resumeDelayedTopic('t'), true);
  bus.publish('x', 'poke3');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'poke3', 'held']);
});

test('repeat hold is an idempotent no-op with a single audit, and a resume predicate that no longer matches releases nothing', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'held', { delayMs: 1000 });
  const isHeld = (payload: unknown) => payload === 'held';
  assert.equal(bus.holdDelayedByPredicate('t', isHeld).held, 1);
  assert.deepEqual(bus.holdDelayedByPredicate('t', isHeld), { held: 0, delayIds: [] });
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_predicate_held' }).length, 1);
  // A resume predicate that selects nothing releases nothing — the
  // entry stays held until a predicate that selects it runs.
  assert.deepEqual(bus.resumeDelayedByPredicate('t', () => false), { resumed: 0, delayIds: [] });
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_predicate_resumed' }).length, 0);
  assert.equal(bus.resumeDelayedByPredicate('t', isHeld).resumed, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_predicate_resumed' }).length, 1);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'held']);
});

test('a held entry past its deadline at resume time is dropped, never delivered late', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'doomed', { delayMs: 1000, deadlineAt: 3000 });
  bus.holdDelayedByPredicate('t', (payload) => payload === 'doomed');
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  // Still in the raw pending set while held: the hold discarded
  // nothing, even past the deadline.
  assert.equal(bus.getStats().pendingDelayed, 1);
  bus.resumeDelayedByPredicate('t', (payload) => payload === 'doomed');
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('an empty match returns zero with no audit, and the predicate can select by key', () => {
  const bus = new EventBus();
  bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'j' });
  assert.deepEqual(bus.holdDelayedByPredicate('t', (payload) => payload === 'zzz'), {
    held: 0,
    delayIds: [],
  });
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_predicate_held' }).length, 0);
  assert.equal(bus.holdDelayedByPredicate('t', (_payload, key) => key === 'k').held, 1);
  assert.equal(bus.getStats().pendingDelayed, 2);
});

test('a throwing predicate fails closed on hold and on resume, and arguments are validated', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'held', { delayMs: 1000 });
  bus.publishDelayed('t', 'free', { delayMs: 1000 });
  const throwing = (): unknown => {
    throw new Error('predicate boom');
  };
  assert.throws(() => bus.holdDelayedByPredicate('t', throwing), /predicate boom/);
  // Nothing was held: both schedules are still pending, no audit.
  assert.equal(bus.getStats().pendingDelayed, 2);
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_predicate_held' }).length, 0);
  bus.holdDelayedByPredicate('t', (payload) => payload === 'held');
  assert.throws(() => bus.resumeDelayedByPredicate('t', throwing), /predicate boom/);
  // Nothing was released: the held schedule is still held, no audit.
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_predicate_resumed' }).length, 0);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'free']);
  assert.equal(bus.getStats().pendingDelayed, 1);
  // Argument validation, for both methods, holds/releases nothing.
  assert.throws(() => bus.holdDelayedByPredicate(123 as unknown as string, () => true), TypeError);
  assert.throws(() => bus.holdDelayedByPredicate('', () => true), RangeError);
  assert.throws(() => bus.holdDelayedByPredicate('t', 123 as unknown as () => true), TypeError);
  assert.throws(() => bus.resumeDelayedByPredicate(123 as unknown as string, () => true), TypeError);
  assert.throws(() => bus.resumeDelayedByPredicate('', () => true), RangeError);
  assert.throws(() => bus.resumeDelayedByPredicate('t', 123 as unknown as () => true), TypeError);
  assert.equal(bus.getStats().pendingDelayed, 1);
});
