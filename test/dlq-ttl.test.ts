import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
/** Sleeps for wall-clock ms (auto-replay backoff uses real timers). */
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/**
 * Dead-letters one message on a reliable subscription: every handler run
 * nacks, so the message exhausts `maxRedeliveries` and lands in the DLQ.
 */
async function deadLetterOne(
  bus: EventBus,
  subId: string,
  topic: string,
  payload: unknown,
): Promise<void> {
  bus.publish(topic, payload);
  await flush();
  await flush();
  await flush();
}

test('invalid maxAgeMs throws from subscribeReliable', () => {
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () =>
        new EventBus().subscribeReliable('t', () => {}, {
          deadLetter: { maxAgeMs: bad },
        }),
      RangeError,
      `maxAgeMs=${bad}`,
    );
  }
  // A string is not a finite number either.
  assert.throws(
    () =>
      new EventBus().subscribeReliable('t', () => {}, {
        deadLetter: { maxAgeMs: '100' as never },
      }),
    RangeError,
  );
  // A valid value is accepted.
  const sub = new EventBus().subscribeReliable('t', () => {}, {
    deadLetter: { maxAgeMs: 60_000 },
  });
  assert.ok(sub.id.length > 0);
});

test('expired entries are lazily dropped at query time and counted', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    { deadLetter: { maxRedeliveries: 0, maxAgeMs: 1_000 } },
  );
  await deadLetterOne(bus, sub.id, 't', 'old');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
  // Within the TTL the entry is still there.
  clock.nowMs += 999;
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
  // Past the TTL the query lazily drops it and counts it once.
  clock.nowMs += 1;
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
  // Re-querying does not double-count.
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
});

test('without maxAgeMs the TTL is off: entries survive clock movement', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    { deadLetter: { maxRedeliveries: 0 } },
  );
  await deadLetterOne(bus, sub.id, 't', 'kept');
  clock.nowMs += 10_000_000;
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
});

test('replay of an expired entry returns false and never resurrects it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let deliveries = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries += 1;
      d.nack();
    },
    { deadLetter: { maxRedeliveries: 0, maxAgeMs: 500 } },
  );
  await deadLetterOne(bus, sub.id, 't', 'stale');
  const [entry] = bus.getDeadLetterMessages(sub.id);
  assert.equal(entry.seq, 1);
  clock.nowMs += 501;
  const deliveriesBefore = deliveries;
  assert.equal(bus.replayDeadLetter(sub.id, entry.seq), false);
  await flush();
  // No resurrection: the handler saw no new delivery, the DLQ is empty,
  // and the expiry was counted.
  assert.equal(deliveries, deliveriesBefore);
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
});

test('a live entry still replays when the TTL is configured', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let deliveries = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries += 1;
      if (deliveries === 1) d.nack();
      else d.ack();
    },
    { deadLetter: { maxRedeliveries: 0, maxAgeMs: 60_000 } },
  );
  await deadLetterOne(bus, sub.id, 't', 'fresh');
  const [entry] = bus.getDeadLetterMessages(sub.id);
  assert.equal(bus.replayDeadLetter(sub.id, entry.seq), true);
  await flush();
  assert.equal(deliveries, 2);
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
});

test('expire-first eviction: expired entries are shed before live ones', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    { deadLetter: { maxRedeliveries: 0, maxEntries: 2, maxAgeMs: 1_000 } },
  );
  // Entry 1 dead-letters at t=1000, entry 2 at t=1500.
  await deadLetterOne(bus, sub.id, 't', 'first');
  clock.nowMs += 500;
  await deadLetterOne(bus, sub.id, 't', 'second');
  // t=2100: entry 1 is expired (1000+1000 <= 2100), entry 2 is live.
  clock.nowMs += 600;
  await deadLetterOne(bus, sub.id, 't', 'third');
  const entries = bus.getDeadLetterMessages(sub.id);
  // The expired 'first' was shed; 'second' and 'third' remain — the
  // bounded eviction never had to shed a live entry (evictedOldest=false).
  assert.deepEqual(
    entries.map((e) => e.payload),
    ['second', 'third'],
  );
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
});

test('expired entries do not consume maxEntries slots', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const evicted: boolean[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    {
      deadLetter: {
        maxRedeliveries: 0,
        maxEntries: 1,
        maxAgeMs: 1_000,
        onDeadLetter: (e) => evicted.push(e.evictedOldest),
      },
    },
  );
  await deadLetterOne(bus, sub.id, 't', 'one');
  clock.nowMs += 1_001;
  await deadLetterOne(bus, sub.id, 't', 'two');
  // 'one' expired and was swept before the bounded check ran, so the
  // slot was free — no live entry was evicted.
  assert.deepEqual(evicted, [false, false]);
  assert.deepEqual(
    bus.getDeadLetterMessages(sub.id).map((e) => e.payload),
    ['two'],
  );
});

test('an expired entry with a pending auto-replay is never resurrected', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let deliveries = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries += 1;
      d.nack();
    },
    {
      deadLetter: {
        maxRedeliveries: 0,
        maxAgeMs: 1_000,
        autoReplay: { initialDelayMs: 30, maxDelayMs: 30, maxAutoReplayAttempts: 1 },
      },
    },
  );
  await deadLetterOne(bus, sub.id, 't', 'doomed');
  const deliveriesAfterDeadLetter = deliveries;
  // Age the entry past its TTL, then let the real-timer auto-replay fire:
  // the expired entry must be dropped, not requeued.
  clock.nowMs += 1_001;
  await sleep(120);
  await flush();
  assert.equal(deliveries, deliveriesAfterDeadLetter);
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
  assert.equal(bus.getStats().autoReplayed, 0);
  void sub;
});
