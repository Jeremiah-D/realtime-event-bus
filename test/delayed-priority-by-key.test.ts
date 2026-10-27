import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time. Due
 * sweeps run at the start of every flush, so advancing this clock past
 * a `deliverAt` and then publishing anything delivers deterministically.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/**
 * One wildcard subscriber observes every topic on a single queue, so
 * the received order is exactly the fan-out order. The poke publish
 * that triggers each sweep lands first (it is queued synchronously
 * during `publish`, before the sweep runs at flush start); tests drop
 * it by filtering on its topic.
 */
function observingBus(clock: { now: () => number }) {
  const bus = new EventBus({ now: clock.now });
  const received: string[] = [];
  bus.subscribe('*', (msg) => received.push(`${msg.topic}:${msg.payload as string}`));
  return { bus, received };
}

/** Advances the clock and triggers a sweep via a poke on an untracked topic. */
async function sweepAt(clock: { nowMs: number }, bus: EventBus, atMs: number) {
  clock.nowMs = atMs;
  bus.publish('__poke__', 'poke');
  await flush();
}

test('a key rule beats a topic rule in the same tick, and other keys fall back to the topic rule', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedTopicPriority('b', 10);
  bus.setDelayedPriorityByKey('b', 'k', 20);
  bus.publishDelayed('b', 'topic-only', { delayMs: 1000, key: 'j' });
  bus.publishDelayed('b', 'keyed', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('a', 'plain', { delayMs: 1000 });
  await sweepAt(clock, bus, 2_000);
  // Keyed 'b/k' (20) first, then 'b/j' by its topic rule (10), then 'a' (0).
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['b:keyed', 'b:topic-only', 'a:plain'],
  );
});

test('a key rule overrides a topic rule downward, not just upward', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedTopicPriority('b', 10);
  bus.setDelayedPriorityByKey('b', 'k', 1);
  bus.setDelayedTopicPriority('a', 5);
  bus.publishDelayed('b', 'keyed', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('a', 'mid', { delayMs: 1000 });
  await sweepAt(clock, bus, 2_000);
  // 'b/k' resolves to 1 (its key rule), not 10 (its topic rule).
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['a:mid', 'b:keyed'],
  );
});

test('pattern key rules match topics; an exact rule wins over a pattern, earliest pattern wins', async () => {
  // Fan-out order uses distinct keys per topic: same-key schedules
  // in different topics are coupled by per-key order (EB-252), so a
  // shared key would re-impose schedule order no matter the
  // priority — priority orders the fan-out attempts, keySeq order
  // still governs same-key delivery.
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedPriorityByKey('orders.*', 'k-us', 1);
  bus.setDelayedPriorityByKey('orders.eu', 'k-eu', 8);
  bus.setDelayedPriorityByKey('solo', 'k-solo', 5);
  bus.publishDelayed('orders.us', 'us', { delayMs: 1000, key: 'k-us' });
  bus.publishDelayed('orders.eu', 'eu', { delayMs: 1000, key: 'k-eu' });
  bus.publishDelayed('solo', 's', { delayMs: 1000, key: 'k-solo' });
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['orders.eu:eu', 'solo:s', 'orders.us:us'],
  );

  // One (pattern, key) rule resolving across topics is observed via
  // listDelayed, where no keySeq coupling applies: the pattern rule
  // covers both matching topics, the exact rule wins for its topic
  // even though it was registered after the pattern.
  const clock2 = controllableClock();
  const bus2 = new EventBus({ now: clock2.now });
  bus2.setDelayedPriorityByKey('orders.*', 'k', 1);
  bus2.setDelayedPriorityByKey('orders.eu', 'k', 8);
  bus2.publishDelayed('orders.us', 'us', { delayMs: 1000, key: 'k' });
  bus2.publishDelayed('orders.eu', 'eu', { delayMs: 1000, key: 'k' });
  bus2.publishDelayed('billing', 'b', { delayMs: 1000, key: 'k' });
  assert.deepEqual(
    bus2.listDelayed().map((row) => row.priority),
    [1, 8, 0],
  );

  // Earliest-registered matching pattern wins: 'ev.*' (1) was
  // registered before '*.hot' (9), so 'ev.hot' resolves to 1, below
  // 'mid' (5) — not to 9, which would have put it first. Distinct
  // keys again, so keySeq coupling cannot mask the ordering.
  const clock3 = controllableClock();
  const third = observingBus(clock3);
  third.bus.setDelayedPriorityByKey('ev.*', 'k-e', 1);
  third.bus.setDelayedPriorityByKey('*.hot', 'k-e', 9);
  third.bus.setDelayedPriorityByKey('mid', 'k-m', 5);
  third.bus.publishDelayed('ev.hot', 'e', { delayMs: 1000, key: 'k-e' });
  third.bus.publishDelayed('mid', 'm', { delayMs: 1000, key: 'k-m' });
  await sweepAt(clock3, third.bus, 2_000);
  assert.deepEqual(
    third.received.filter((entry) => !entry.startsWith('__poke__')),
    ['mid:m', 'ev.hot:e'],
  );
});

test('a key rule for another key never applies, and unkeyed schedules fall back to the topic rule', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDelayedTopicPriority('b', 5);
  bus.setDelayedPriorityByKey('b', 'k', 20);
  const keyed = bus.publishDelayed('b', 'keyed', { delayMs: 1000, key: 'k' });
  const otherKey = bus.publishDelayed('b', 'other', { delayMs: 1000, key: 'j' });
  const unkeyed = bus.publishDelayed('b', 'plain', { delayMs: 1000 });
  const rows = new Map(bus.listDelayed().map((row) => [row.delayId, row]));
  assert.equal(rows.get(keyed)?.priority, 20);
  assert.equal(rows.get(otherKey)?.priority, 5);
  assert.equal(rows.get(unkeyed)?.priority, 5);
});

test('listDelayed discloses the effective priority, and clearing a key rule restores the topic priority', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDelayedTopicPriority('b', 7);
  bus.setDelayedPriorityByKey('b', 'k', 20);
  const id = bus.publishDelayed('b', 'x', { delayMs: 1000, key: 'k' });
  assert.equal(bus.listDelayed()[0].priority, 20);
  assert.equal(bus.clearDelayedPriorityByKey('never-set', 'k'), false);
  assert.equal(bus.clearDelayedPriorityByKey('b', 'k'), true);
  assert.equal(bus.clearDelayedPriorityByKey('b', 'k'), false);
  // Same schedule, now resolved through the topic rule.
  assert.equal(bus.listDelayed()[0].priority, 7);
  assert.equal(bus.listDelayed()[0].delayId, id);
});

test('setDelayedPriorityByKey validates its arguments', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  assert.throws(() => bus.setDelayedPriorityByKey(42 as never, 'k', 1), TypeError);
  assert.throws(() => bus.setDelayedPriorityByKey('', 'k', 1), RangeError);
  assert.throws(() => bus.setDelayedPriorityByKey('b', 42 as never, 1), TypeError);
  assert.throws(() => bus.setDelayedPriorityByKey('b', '', 1), RangeError);
  assert.throws(() => bus.setDelayedPriorityByKey('b', 'k', NaN), RangeError);
  assert.throws(() => bus.setDelayedPriorityByKey('b', 'k', Infinity), RangeError);
  // A failed set leaves no rule behind.
  assert.equal(bus.listDelayed().length, 0);
});

test('a rule set after scheduling applies to that schedule, and a schedule made under a rule keeps it until cleared', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.publishDelayed('low', 'l', { delayMs: 1000 });
  bus.publishDelayed('b', 'keyed', { delayMs: 1000, key: 'k' });
  // Rule arrives after scheduling: it still governs the next sweep.
  bus.setDelayedPriorityByKey('b', 'k', 20);
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['b:keyed', 'low:l'],
  );

  // A schedule made while a rule stands, then cleared before its
  // sweep, fans out in schedule order again.
  const clock2 = controllableClock();
  const second = observingBus(clock2);
  second.bus.setDelayedPriorityByKey('b', 'k', 20);
  second.bus.publishDelayed('low', 'l', { delayMs: 1000 });
  second.bus.publishDelayed('b', 'keyed', { delayMs: 1000, key: 'k' });
  second.bus.clearDelayedPriorityByKey('b', 'k');
  await sweepAt(clock2, second.bus, 2_000);
  assert.deepEqual(
    second.received.filter((entry) => !entry.startsWith('__poke__')),
    ['low:l', 'b:keyed'],
  );
});

test('FIFO is preserved within one (topic, key) even when it has a priority', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedPriorityByKey('b', 'k', 20);
  bus.publishDelayed('b', 'first', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('b', 'second', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('b', 'third', { delayMs: 1000, key: 'k' });
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['b:first', 'b:second', 'b:third'],
  );
});

test('held schedules keep their effective priority in listDelayed and fan out by it after resume', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  bus.setDelayedTopicPriority('a', 10);
  bus.setDelayedPriorityByKey('b', 'k', 20);
  const held = bus.publishDelayed('b', 'held', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('a', 'free', { delayMs: 1000 });
  assert.equal(bus.holdDelayedByKey('b', 'k'), true);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'free']);
  // Held, but its priority is still disclosed — the hold only defers fan-out.
  const rows = bus.listDelayed();
  assert.equal(rows.length, 1);
  assert.equal(rows[0].delayId, held);
  assert.equal(rows[0].priority, 20);
  assert.equal(bus.resumeDelayedByKey('b', 'k'), true);
  bus.publish('x', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'free', 'poke2', 'held']);
});

test('a topic-held schedule likewise keeps its key priority while held', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDelayedPriorityByKey('b', 'k', 20);
  const held = bus.publishDelayed('b', 'held', { delayMs: 1000, key: 'k' });
  assert.equal(bus.holdDelayedTopic('b'), true);
  assert.equal(bus.listDelayed()[0].delayId, held);
  assert.equal(bus.listDelayed()[0].priority, 20);
  assert.equal(bus.resumeDelayedTopic('b'), true);
});
