import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so EDF tests are deterministic instead of racing wall time. Follows
 * the `controllableClock` pattern from `test/deadline.test.ts`: the
 * delayed-delivery sweep runs at the start of a flush, so advancing the
 * clock alone is not enough — a `publish` (the "poke") triggers the
 * flush whose sweep fans out due entries.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('deadline-carrying messages drain earliest-deadline-first', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Published out of deadline order, all queued before the flush runs.
  bus.publish('t', 'd3000', { deadlineMs: 3_000 });
  bus.publish('t', 'd1000', { deadlineMs: 1_000 });
  bus.publish('t', 'd2000', { deadlineMs: 2_000 });
  await flush();
  await flush();
  assert.deepEqual(received, ['d1000', 'd2000', 'd3000']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('messages without a deadline keep FIFO order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  await flush();
  assert.deepEqual(received, ['a', 'b', 'c']);
});

test('mixed streams: deadline messages drain first (EDF), the rest stay FIFO', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'plain1');
  bus.publish('t', 'd2000', { deadlineMs: 2_000 });
  bus.publish('t', 'plain2');
  bus.publish('t', 'd1000', { deadlineMs: 1_000 });
  bus.publish('t', 'plain3');
  await flush();
  await flush();
  // Deadline-carrying messages earliest-deadline-first, then the
  // deadline-free messages in their original FIFO order.
  assert.deepEqual(received, ['d1000', 'd2000', 'plain1', 'plain2', 'plain3']);
});

test('a message handed off at or past its EDF deadline is marked and counted', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: Array<{ payload: unknown; missed: boolean }> = [];
  bus.subscribe('t', (msg) =>
    seen.push({ payload: msg.payload, missed: msg.missedDeadline === true }),
  );
  bus.publish('t', 'late', { deadlineMs: 1_100 });
  clock.nowMs = 1_200; // past the deadline when the flush drains
  await flush();
  await flush();
  // Delivered, not dropped — with the missedDeadline mark.
  assert.deepEqual(seen, [{ payload: 'late', missed: true }]);
  const stats = bus.getStats();
  assert.equal(stats.missedDeadlineMessages, 1);
  assert.equal(stats.topics[0].missedDeadlineMessages, 1);
  // An EDF deadline never expires the message.
  assert.equal(stats.expiredMessages, 0);
});

test('a message delivered before its EDF deadline is unmarked', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: Array<{ payload: unknown; missed: boolean }> = [];
  bus.subscribe('t', (msg) =>
    seen.push({ payload: msg.payload, missed: msg.missedDeadline === true }),
  );
  bus.publish('t', 'ontime', { deadlineMs: 2_000 });
  clock.nowMs = 1_500;
  await flush();
  await flush();
  assert.deepEqual(seen, [{ payload: 'ontime', missed: false }]);
  assert.equal(bus.getStats().missedDeadlineMessages, 0);
});

test('deadlineAt keeps hard-deadline semantics: dropped as expired, never marked', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: Array<{ payload: unknown; missed: boolean }> = [];
  bus.subscribe('t', (msg) =>
    seen.push({ payload: msg.payload, missed: msg.missedDeadline === true }),
  );
  bus.publish('t', 'hard', { deadlineAt: 1_100 });
  clock.nowMs = 1_200;
  await flush();
  await flush();
  assert.deepEqual(seen, []);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.missedDeadlineMessages, 0);
});

test('deadlineAt wins over deadlineMs when both are given: hard drop', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'at-wins', { deadlineAt: 1_100, deadlineMs: 5_000 });
  clock.nowMs = 1_200;
  await flush();
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.equal(bus.getStats().missedDeadlineMessages, 0);
});

test('EDF is within-lane: an urgent message still beats an earlier-deadline normal message', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'normal-early', { deadlineMs: 1_100 });
  bus.publish('t', 'urgent-late', { priority: 'urgent' });
  bus.publish('t', 'normal-later', { deadlineMs: 1_200 });
  await flush();
  await flush();
  // Lanes win across lanes (urgent first); EDF orders within the normal
  // lane. Orthogonal, as documented.
  assert.deepEqual(received, ['urgent-late', 'normal-early', 'normal-later']);
});

test('a topic TTL still expires an EDF message; the TTL drop is not a miss', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100); // TTL deadline t=1100, earlier than the EDF deadline
  const seen: Array<{ payload: unknown; missed: boolean }> = [];
  bus.subscribe('t', (msg) =>
    seen.push({ payload: msg.payload, missed: msg.missedDeadline === true }),
  );
  bus.publish('t', 'ttl-wins', { deadlineMs: 5_000 });
  clock.nowMs = 1_200;
  await flush();
  await flush();
  assert.deepEqual(seen, []);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.missedDeadlineMessages, 0);
});

test('reliable subscribers get EDF order and the missed mark', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: Array<{ payload: unknown; missed: boolean }> = [];
  bus.subscribeReliable(
    't',
    (delivery) => {
      seen.push({ payload: delivery.msg.payload, missed: delivery.msg.missedDeadline === true });
      delivery.ack();
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('t', 'r3000', { deadlineMs: 3_000 });
  bus.publish('t', 'r1000', { deadlineMs: 1_000 });
  clock.nowMs = 1_500; // past r1000's deadline, before r3000's
  await flush();
  await flush();
  assert.deepEqual(seen, [
    { payload: 'r1000', missed: true },
    { payload: 'r3000', missed: false },
  ]);
  assert.equal(bus.getStats().missedDeadlineMessages, 1);
});

test('batched subscribers collect in EDF order and mark late hand-offs', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: Array<{ payload: unknown; missed: boolean }> = [];
  bus.subscribe(
    't',
    (msgs) => {
      for (const msg of msgs) {
        seen.push({ payload: msg.payload, missed: msg.missedDeadline === true });
      }
    },
    { batch: { maxSize: 10, maxWaitMs: 1_000 } },
  );
  bus.publish('t', 'b3000', { deadlineMs: 3_000 });
  bus.publish('t', 'b1000', { deadlineMs: 1_000 });
  clock.nowMs = 1_500;
  await flush();
  await flush();
  await new Promise<void>((resolve) => setTimeout(resolve, 1_100));
  assert.deepEqual(seen, [
    { payload: 'b1000', missed: true },
    { payload: 'b3000', missed: false },
  ]);
  assert.equal(bus.getStats().missedDeadlineMessages, 1);
});

test('a delayed publish with deadlineMs fans out late instead of expiring at sweep', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: Array<{ payload: unknown; missed: boolean }> = [];
  bus.subscribe('t', (msg) =>
    seen.push({ payload: msg.payload, missed: msg.missedDeadline === true }),
  );
  // Due at t=1100, EDF deadline t=1200: the wait eats the deadline, but
  // the schedule is not dropped — it fans out late and is marked.
  bus.publishDelayed('t', 'delayed-late', { delayMs: 100, deadlineMs: 1_200 });
  clock.nowMs = 1_300;
  bus.publish('t', 'poke');
  await flush();
  await flush();
  // The delayed message carries the earlier EDF deadline, so it drains
  // ahead of the deadline-free poke — EDF orders across the whole queue.
  assert.deepEqual(seen, [
    { payload: 'delayed-late', missed: true },
    { payload: 'poke', missed: false },
  ]);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 0);
  assert.equal(stats.missedDeadlineMessages, 1);
});

test('a delayed publish with deadlineAt is still dropped as expired at sweep', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'delayed-stale', { delayMs: 500, deadlineAt: 1_200 });
  clock.nowMs = 1_600;
  bus.publish('t', 'poke');
  await flush();
  await flush();
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.equal(bus.getStats().missedDeadlineMessages, 0);
});

test('the missed-deadline counter renders in the Prometheus exposition', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.publish('t', 'late', { deadlineMs: 1_100 });
  clock.nowMs = 1_200;
  await flush();
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.match(text, /eventbus_missed_deadline_messages_total 1/);
});
