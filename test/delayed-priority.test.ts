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

test('a higher-priority topic fans out first within the same tick', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedTopicPriority('high', 10);
  bus.publishDelayed('low', 'low-msg', { delayMs: 1000 });
  bus.publishDelayed('high', 'high-msg', { delayMs: 1000 });
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['high:high-msg', 'low:low-msg'],
  );
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('FIFO is preserved within one topic even when it has a priority', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedTopicPriority('high', 10);
  bus.publishDelayed('high', 'first', { delayMs: 1000 });
  bus.publishDelayed('high', 'second', { delayMs: 1000 });
  bus.publishDelayed('high', 'third', { delayMs: 1000 });
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['high:first', 'high:second', 'high:third'],
  );
});

test('a lower-priority message due in an earlier tick still goes first across ticks', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedTopicPriority('high', 10);
  bus.publishDelayed('low', 'early', { delayMs: 1000 }); // due t=2000
  bus.publishDelayed('high', 'late', { delayMs: 2000 }); // due t=3000
  await sweepAt(clock, bus, 2_000); // first tick: only the low message is due
  await sweepAt(clock, bus, 3_000); // second tick: the high message follows
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['low:early', 'high:late'],
  );
});

test('priority never delivers a message before its deliverAt', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedTopicPriority('high', 10);
  bus.publishDelayed('high', 'not-yet', { delayMs: 5000 });
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    [],
  );
  assert.equal(bus.getStats().pendingDelayed, 1);
  await sweepAt(clock, bus, 6_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['high:not-yet'],
  );
});

test('priority does not save a message whose TTL expired while delayed', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setTopicTtl('high', 100); // deadline t=1100 for a schedule made at t=1000
  bus.setDelayedTopicPriority('high', 10);
  bus.publishDelayed('high', 'doomed', { delayMs: 1000 }); // due t=2000, past its deadline
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    [],
  );
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('topics without a priority fall back to default ordering around prioritized ones', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedTopicPriority('high', 10);
  bus.publishDelayed('mid-a', 'a', { delayMs: 1000 });
  bus.publishDelayed('high', 'h', { delayMs: 1000 });
  bus.publishDelayed('mid-b', 'b', { delayMs: 1000 });
  await sweepAt(clock, bus, 2_000);
  // Unset topics share priority 0 and keep schedule order among
  // themselves; the prioritized topic jumps ahead of both.
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['high:h', 'mid-a:a', 'mid-b:b'],
  );
});

test('with no priorities configured, same-tick order is exactly the old heap order', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  // No setDelayedTopicPriority call at all: default-off.
  bus.publishDelayed('b-topic', 'b', { delayMs: 1000 });
  bus.publishDelayed('a-topic', 'a', { delayMs: 1000 });
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['b-topic:b', 'a-topic:a'],
  );
  // Due-time order across schedules is likewise untouched.
  bus.publishDelayed('t', 'later', { delayMs: 2000 });
  bus.publishDelayed('t', 'sooner', { delayMs: 1000 });
  await sweepAt(clock, bus, 5_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__') && entry.startsWith('t:')),
    ['t:sooner', 't:later'],
  );
});

test('pattern rules match topics; an exact rule wins over a pattern, earliest pattern wins', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  bus.setDelayedTopicPriority('orders.*', 5);
  bus.setDelayedTopicPriority('orders.vip', 10);
  bus.publishDelayed('orders.normal', 'n', { delayMs: 1000 });
  bus.publishDelayed('orders.vip', 'v', { delayMs: 1000 });
  bus.publishDelayed('plain', 'p', { delayMs: 1000 });
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['orders.vip:v', 'orders.normal:n', 'plain:p'],
  );

  // Earliest-registered matching pattern wins: 'ev.*' (1) was
  // registered before '*.hot' (9), so 'ev.hot' resolves to 1, below
  // 'solo' (5) — not to 9, which would have put it first.
  const clock2 = controllableClock();
  const second = observingBus(clock2);
  second.bus.setDelayedTopicPriority('ev.*', 1);
  second.bus.setDelayedTopicPriority('*.hot', 9);
  second.bus.setDelayedTopicPriority('solo', 5);
  second.bus.publishDelayed('ev.hot', 'e', { delayMs: 1000 });
  second.bus.publishDelayed('solo', 's', { delayMs: 1000 });
  await sweepAt(clock2, second.bus, 2_000);
  assert.deepEqual(
    second.received.filter((entry) => !entry.startsWith('__poke__')),
    ['solo:s', 'ev.hot:e'],
  );
});

test('setDelayedTopicPriority validates its arguments; clearing restores heap order', async () => {
  const clock = controllableClock();
  const { bus, received } = observingBus(clock);
  assert.throws(() => bus.setDelayedTopicPriority(42 as never, 1), TypeError);
  assert.throws(() => bus.setDelayedTopicPriority('', 1), RangeError);
  assert.throws(() => bus.setDelayedTopicPriority('x', NaN), RangeError);
  assert.throws(() => bus.setDelayedTopicPriority('x', Infinity), RangeError);
  assert.equal(bus.clearDelayedTopicPriority('never-set'), false);

  bus.setDelayedTopicPriority('high', 10);
  assert.equal(bus.clearDelayedTopicPriority('high'), true);
  assert.equal(bus.clearDelayedTopicPriority('high'), false);
  // After clearing, the same tick fans out in schedule order again.
  bus.publishDelayed('low', 'l', { delayMs: 1000 });
  bus.publishDelayed('high', 'h', { delayMs: 1000 });
  await sweepAt(clock, bus, 2_000);
  assert.deepEqual(
    received.filter((entry) => !entry.startsWith('__poke__')),
    ['low:l', 'high:h'],
  );
});
