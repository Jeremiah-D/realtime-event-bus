import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so deadline tests are deterministic instead of racing wall time. Note
 * the delayed-delivery sweep runs at the start of a flush: advancing the
 * clock alone is not enough — a `publish` (the "poke") triggers the
 * flush whose sweep fans out due entries.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('a message before its deadline is delivered normally', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'fresh', { deadlineAt: 2_000 });
  clock.nowMs = 1_500;
  await flush();
  await flush();
  assert.deepEqual(received, ['fresh']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('a message past its deadline is dropped as expired and counted', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'first', { deadlineAt: 5_000 }); // seq 1, delivered: baseline
  await flush();
  await flush();
  bus.publish('t', 'stale', { deadlineAt: 1_100 }); // seq 2, expires
  clock.nowMs = 1_200; // past the deadline when the flush drains
  await flush();
  await flush();
  assert.deepEqual(received, ['first']);
  bus.publish('t', 'next', { deadlineAt: 5_000 }); // seq 3, delivered
  await flush();
  await flush();
  assert.deepEqual(received, ['first', 'next']);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics[0].expiredMessages, 1);
  assert.equal(stats.topics[0].publishedMessages, 3);
  // The expired seq 2 surfaces as a gap on the next delivery (seq 3).
  assert.equal(stats.sequenceGaps, 1);
  assert.equal(stats.topics[0].sequenceGaps, 1);
});

test('deadlineMs is an alias for deadlineAt', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'via-ms', { deadlineMs: 2_000 });
  clock.nowMs = 1_500;
  await flush();
  await flush();
  assert.deepEqual(received, ['via-ms']);
});

test('deadlineAt wins over deadlineMs when both are given', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // deadlineAt=1100 (past at drain) wins over deadlineMs=5000 (future).
  bus.publish('t', 'at-wins', { deadlineAt: 1_100, deadlineMs: 5_000 });
  clock.nowMs = 1_200;
  await flush();
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('the earlier of deadline and topic TTL wins', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100); // TTL deadline t=1100
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Deadline later than the TTL: the TTL still applies (min).
  bus.publish('t', 'ttl-wins', { deadlineAt: 5_000 });
  clock.nowMs = 1_200;
  await flush();
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);

  const bus2 = new EventBus({ now: clock.now });
  clock.nowMs = 1_000;
  bus2.setTopicTtl('t', 5_000); // TTL deadline t=6000
  const received2: unknown[] = [];
  bus2.subscribe('t', (msg) => received2.push(msg.payload));
  // Deadline earlier than the TTL: the deadline applies (min).
  bus2.publish('t', 'deadline-wins', { deadlineAt: 1_100 });
  clock.nowMs = 1_200;
  await flush();
  await flush();
  assert.deepEqual(received2, []);
  assert.equal(bus2.getStats().expiredMessages, 1);
});

test('invalid deadlines throw RangeError before anything is mutated', () => {
  const bus = new EventBus();
  for (const bad of [NaN, Infinity, -Infinity, 'x', {}, []] as unknown[]) {
    assert.throws(() => bus.publish('t', 'z', { deadlineAt: bad as number }), RangeError);
    assert.throws(() => bus.publish('t', 'z', { deadlineMs: bad as number }), RangeError);
  }
  // A failed publish mutates nothing: no sequence number consumed.
  assert.equal(bus.getStats().totalPublished, 0);
});

test('a past deadline is legal: admitted, then dropped as expired', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const accepted = bus.publish('t', 'already-dead', { deadlineAt: 500 });
  assert.equal(accepted, 1); // admitted, not rejected
  await flush();
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a redelivered message keeps its deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: unknown[] = [];
  const deliveries: Array<{ nack(): void }> = [];
  // Reliable subscription: the delivery handle is saved; nacking it
  // after the deadline requeues the identical message, which is then
  // dropped as expired instead of redelivered — the deadline rides the
  // message object through the requeue.
  bus.subscribeReliable(
    't',
    (delivery) => {
      seen.push(delivery.msg.payload);
      deliveries.push(delivery);
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('t', 'redeliver-me', { deadlineAt: 1_100 });
  clock.nowMs = 1_050;
  await flush();
  await flush();
  assert.deepEqual(seen, ['redeliver-me']);
  clock.nowMs = 1_200; // past the deadline
  deliveries[0].nack();
  await flush();
  await flush();
  // No second delivery: the requeue expired instead of redelivering.
  assert.deepEqual(seen, ['redeliver-me']);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a delayed publish honors its deadline at sweep time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Due at t=1100, deadline t=2000: delivered.
  bus.publishDelayed('t', 'delayed-ok', { delayMs: 100, deadlineAt: 2_000 });
  clock.nowMs = 1_100;
  bus.publish('t', 'poke');
  await flush();
  await flush();
  assert.deepEqual(received, ['poke', 'delayed-ok']);

  const bus2 = new EventBus({ now: clock.now });
  clock.nowMs = 1_000;
  const received2: unknown[] = [];
  bus2.subscribe('t', (msg) => received2.push(msg.payload));
  // Due at t=1500, deadline t=1200: the deadline passes first — dropped
  // as expired at sweep time, never delivered.
  bus2.publishDelayed('t', 'delayed-stale', { delayMs: 500, deadlineAt: 1_200 });
  clock.nowMs = 1_600;
  bus2.publish('t', 'poke');
  await flush();
  await flush();
  assert.deepEqual(received2, ['poke']);
  assert.equal(bus2.getStats().expiredMessages, 1);
});

test('a delayed publish validates its deadline fail-fast', () => {
  const bus = new EventBus();
  assert.throws(() => bus.publishDelayed('t', 'z', { delayMs: 10, deadlineAt: NaN }), RangeError);
  assert.throws(
    () => bus.publishDelayed('t', 'z', { delayMs: 10, deadlineMs: Infinity }),
    RangeError,
  );
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('the deadline rides the durable log and replay enforces it', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-deadline-'));
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('t', 'replay-me', { deadlineAt: 1_100 });
  await flush();
  // A late joiner resumes from the log after the deadline passed: the
  // recorded deadline is enforced, the message is never resurrected.
  clock.nowMs = 1_200;
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('the deadline rides the in-memory lookback buffer', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicLookback('t', { lookbackCount: 10 });
  bus.publish('t', 'lookback-fresh', { deadlineAt: 2_000 });
  bus.publish('t', 'lookback-stale', { deadlineAt: 1_100 });
  clock.nowMs = 1_200;
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(received, ['lookback-fresh']);
  assert.equal(bus.getStats().expiredMessages, 1);
});
