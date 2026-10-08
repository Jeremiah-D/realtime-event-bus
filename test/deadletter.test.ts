import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BusMessage,
  type DeadLetterEntry,
  type DeadLetterEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('nack beyond maxRedeliveries moves the message to the DLQ', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack(); // always fail
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 2 } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  // Initial delivery + 2 redeliveries, then DLQ — no 4th delivery.
  assert.deepEqual(seen, [0, 1, 2]);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  const [entry] = entries;
  assert.equal(entry.seq, 1);
  assert.equal(entry.topic, 't');
  assert.deepEqual(entry.payload, 'poison');
  assert.equal(entry.redeliveries, 2);
  assert.ok(entry.deadLetteredAt > 0);
  assert.equal(entry.expiresAt, undefined);
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  assert.equal(bus.unackedCount(sub.id), 0);
});

test('ack timeout beyond maxRedeliveries moves the message to the DLQ', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      // Never settle: the ack timer requeues it.
    },
    { ackTimeoutMs: 30, deadLetter: { maxRedeliveries: 1 } },
  );
  bus.publish('t', 'slow');
  await flush();
  assert.deepEqual(seen, [0]);
  await sleep(80); // first timeout -> redelivery
  await flush();
  assert.deepEqual(seen, [0, 1]);
  await sleep(80); // second timeout -> budget exhausted -> DLQ
  await flush();
  assert.deepEqual(seen, [0, 1]); // no third delivery
  assert.equal(bus.getStats().deadLetteredMessages, 1);
});

test('a repeatedly throwing handler lands the message in the DLQ', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable(
    't',
    () => {
      attempts += 1;
      throw new Error('boom');
    },
    { ackTimeoutMs: 10_000, deadLetter: { maxRedeliveries: 2 } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  // The throw is converted to an immediate redelivery: no waiting for
  // ack timeouts, no process crash — 3 attempts then DLQ.
  assert.equal(attempts, 3);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].payload, 'poison');
  assert.equal(entries[0].redeliveries, 2);
});

test('maxRedeliveries 0 dead-letters on the first redelivery attempt', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', 'x');
  await flush();
  await flush();
  assert.deepEqual(seen, [0]); // one delivery, then straight to the DLQ
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('without deadLetter the subscription retries forever (unchanged behavior)', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (seen.length >= 4) d.ack();
      else d.nack();
    },
    { ackTimeoutMs: 1000 },
  );
  bus.publish('t', 'x');
  await flush();
  await flush();
  await flush();
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3]);
  assert.equal(bus.getStats().deadLetteredMessages, 0);
});

test('DLQ entries keep arrival order and DLQ-local seqs', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', 'first');
  bus.publish('t', 'second');
  await flush();
  await flush();
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries.map((e) => e.payload), ['first', 'second']);
  assert.deepEqual(entries.map((e) => e.seq), [1, 2]);
});

test('getDeadLetterMessages throws on an unknown subscriber id', () => {
  const bus = new EventBus();
  assert.throws(() => bus.getDeadLetterMessages('nope'), /unknown subscriber/);
  assert.throws(() => bus.replayDeadLetter('nope', 1), /unknown subscriber/);
});

test('getDeadLetterMessages returns envelope snapshots', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', { mutable: true });
  await flush();
  await flush();
  const entries = bus.getDeadLetterMessages(sub.id);
  const payloadRef = entries[0].payload;
  // The array and the entry envelopes are snapshots: popping the array
  // or reassigning an envelope field does not affect the bus.
  (entries[0] as { seq: number }).seq = 999;
  entries.pop();
  const again = bus.getDeadLetterMessages(sub.id);
  assert.equal(again.length, 1);
  assert.equal(again[0].seq, 1);
  assert.deepEqual(again[0].payload, { mutable: true });
  // The payload itself is the original object, shared by reference —
  // documented on DeadLetterEntry.payload.
  assert.equal(again[0].payload, payloadRef);
});

test('replayDeadLetter requeues the message with its original seq and no false gap', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  let failures = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      if (failures < 1) {
        failures += 1;
        d.nack();
        return;
      }
      received.push(d.msg.payload);
      d.ack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', 'a'); // msg seq 1
  bus.publish('t', 'b'); // msg seq 2, delivered fine... (nack only the first)
  await flush();
  await flush();
  // 'a' was nacked once -> DLQ (maxRedeliveries 0); 'b' acked.
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].payload, 'a');
  assert.equal(bus.replayDeadLetter(sub.id, entries[0].seq), true);
  await flush();
  await flush();
  assert.deepEqual(received, ['b', 'a']);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0);
  // The replayed message kept its original per-topic seq (1 <= last 2):
  // redelivery, not a gap.
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('replayDeadLetter returns false for unknown seqs and DLQ-less subscribers', async () => {
  const bus = new EventBus();
  const noDlq = bus.subscribeReliable('t', (d) => d.ack(), { ackTimeoutMs: 1000 });
  assert.equal(bus.replayDeadLetter(noDlq.id, 1), false);
  const sub = bus.subscribeReliable('t2', (d) => d.ack(), {
    ackTimeoutMs: 1000,
    deadLetter: true,
  });
  assert.equal(bus.replayDeadLetter(sub.id, 999), false);
});

test('replay gives the message a fresh redelivery budget', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(), // always fail
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1 } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  // 1 initial + 1 redelivery, then DLQ.
  let entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(bus.getStats().deadLetteredMessages, 1);
  // Replay: fresh budget — it survives another full round before DLQ.
  assert.equal(bus.replayDeadLetter(sub.id, entries[0].seq), true);
  await flush();
  await flush();
  entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].seq, 2, 're-DLQed as a new entry');
  assert.equal(bus.getStats().deadLetteredMessages, 2);
});

test('DLQ is bounded: the oldest entry is evicted when full', async () => {
  const bus = new EventBus();
  const events: DeadLetterEvent[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        maxEntries: 2,
        onDeadLetter: (e) => events.push(e),
      },
    },
  );
  bus.publish('t', 'one');
  bus.publish('t', 'two');
  bus.publish('t', 'three');
  await flush();
  await flush();
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries.map((e) => e.payload), ['two', 'three']);
  assert.deepEqual(events.map((e) => e.evictedOldest), [false, false, true]);
  assert.equal(events[2].entry.payload, 'three');
  assert.equal(events[2].subscriberId, sub.id);
  assert.equal(events[2].pattern, 't');
});

test('a replayed message keeps its original TTL deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      received.push(d.msg.payload);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.setTopicTtl('t', 100); // published at t=1000, deadline t=1100
  bus.publish('t', 'doomed');
  await flush();
  await flush();
  let entries: DeadLetterEntry[] = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].expiresAt, 1100);
  clock.nowMs = 1_200; // past the deadline
  assert.equal(bus.replayDeadLetter(sub.id, entries[0].seq), true);
  await flush();
  // The replay preserved the deadline: dropped as expired, not resurrected.
  assert.deepEqual(received, ['doomed']); // only the pre-DLQ delivery
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0);
});

test('deadLetter option validation', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { maxRedeliveries: -1 },
      }),
    /maxRedeliveries must be a non-negative integer/,
  );
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { maxRedeliveries: 1.5 },
      }),
    /maxRedeliveries must be a non-negative integer/,
  );
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { maxEntries: 0 },
      }),
    /maxEntries must be a positive integer/,
  );
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        // @ts-expect-error - testing runtime validation
        deadLetter: { onDeadLetter: 'nope' },
      }),
    /onDeadLetter must be a function/,
  );
  // Validation happens before subscribing: the bad subscription was not added.
  assert.equal(bus.subscriberCount(), 0);
});

test('deadLetter: true uses the defaults', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: true },
  );
  bus.publish('t', 'x');
  for (let i = 0; i < 8; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    await flush();
  }
  // Default maxRedeliveries is 5: 1 initial + 5 redeliveries, then DLQ.
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 5]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});
