import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { validateRetryLadder, RetryLadder } from '../src/retry-ladder.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// Module unit tests: validation and the scheduler core.
// ---------------------------------------------------------------------------

test('validateRetryLadder: absent or empty means disabled', () => {
  assert.equal(validateRetryLadder(undefined, 't'), undefined);
  assert.equal(validateRetryLadder([], 't'), undefined);
});

test('validateRetryLadder: returns a copy of valid rung delays', () => {
  const input = [1000, 5000, 30000];
  const rungs = validateRetryLadder(input, 't');
  assert.deepEqual(rungs, [1000, 5000, 30000]);
  assert.notEqual(rungs, input);
  // Zero delays are legal — an immediate extra chance.
  assert.deepEqual(validateRetryLadder([0], 't'), [0]);
});

test('validateRetryLadder: illegal values throw RangeError', () => {
  // Non-array values.
  for (const bad of [1000, '1000', 'nope', null, true]) {
    assert.throws(() => validateRetryLadder(bad, 't'), RangeError);
  }
  // Illegal rung delays inside an array.
  for (const bad of [[-1], [Number.NaN], [Number.POSITIVE_INFINITY], [1000, -5], ['1000'], [null]]) {
    assert.throws(() => validateRetryLadder(bad, 't'), RangeError);
  }
});

test('RetryLadder: arm schedules the rung delay on the injected timers', () => {
  const scheduled: Array<{ delay: number; cancelled: boolean }> = [];
  let nowMs = 42;
  const ladder = new RetryLadder([100, 250], {
    now: () => nowMs,
    timers: {
      setTimeout: (cb: () => void, delayMs: number) => {
        const rec = { cb, delay: delayMs, cancelled: false };
        scheduled.push(rec);
        return rec;
      },
      clearTimeout: (h: unknown) => {
        (h as { cancelled: boolean }).cancelled = true;
      },
    },
  });
  assert.equal(ladder.size, 2);
  assert.equal(ladder.delayFor(0), 100);
  assert.equal(ladder.delayFor(1), 250);
  let fired = 0;
  const handle = ladder.arm(1, () => {
    fired += 1;
  });
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0].delay, 250);
  assert.equal(handle.armedAtMs, 42);
  nowMs = 43;
  scheduled[0].cb();
  assert.equal(fired, 1);
  handle.cancel(); // cancelling after the fire is a no-op
  assert.equal(scheduled[0].cancelled, false);
});

test('RetryLadder: cancel prevents the rung from firing', () => {
  const scheduled: Array<{ cb: () => void; cancelled: boolean }> = [];
  const ladder = new RetryLadder([100], {
    timers: {
      setTimeout: (cb: () => void) => {
        const rec = { cb, cancelled: false };
        scheduled.push(rec);
        return rec;
      },
      clearTimeout: (h: unknown) => {
        (h as { cancelled: boolean }).cancelled = true;
      },
    },
  });
  let fired = 0;
  const handle = ladder.arm(0, () => {
    fired += 1;
  });
  handle.cancel();
  handle.cancel(); // idempotent
  assert.equal(scheduled[0].cancelled, true);
  // A real timer never invokes the callback after clearTimeout — the fake
  // models that by honouring the cancelled flag on manual fire.
  const rec = scheduled[0];
  if (!rec.cancelled) rec.cb();
  assert.equal(fired, 0);
});

// ---------------------------------------------------------------------------
// Bus integration: the pre-DLQ retry ladder.
// ---------------------------------------------------------------------------

test('ladder walks each rung with its delay before dead-lettering', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const at: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      at.push(Date.now());
      d.nack(); // always fail
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [30, 60] } },
  );
  bus.publish('t', 'flaky');
  await sleep(400);
  // Initial delivery + one delivery per rung, then the DLQ — the instant
  // redelivery budget is not reset by the ladder.
  assert.deepEqual(seen, [0, 1, 2]);
  // Rung delays are honored (loose lower bounds — CI-safe).
  assert.ok(at[1] - at[0] >= 20, `rung 0 fired too early: ${at[1] - at[0]}ms`);
  assert.ok(at[2] - at[1] >= 45, `rung 1 fired too early: ${at[2] - at[1]}ms`);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].redeliveries, 2);
  assert.equal(entries[0].lastError, 'nack');
  const stats = bus.getStats();
  assert.equal(stats.ladderPending, 0);
  assert.equal(stats.ladderAttempts, 2);
  assert.equal(stats.deadLetteredMessages, 1);
});

test('acking a rung delivery leaves the ladder — no DLQ', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries === 0) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [30] } },
  );
  bus.publish('t', 'recovers');
  await sleep(200);
  assert.deepEqual(seen, [0, 1]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0);
  const stats = bus.getStats();
  assert.equal(stats.ladderPending, 0);
  assert.equal(stats.ladderAttempts, 1);
  assert.equal(stats.deadLetteredMessages, 0);
});

test('a message that expires while waiting is dropped at the rung hand-off, never resurrected', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [80] } },
  );
  // TTL (40ms) expires while the single rung (80ms) is still armed.
  bus.publish('t', 'short-lived', { ttlMs: 40 });
  await sleep(300);
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.ladderPending, 0);
  assert.equal(stats.ladderAttempts, 0);
  assert.equal(stats.deadLetteredMessages, 0);
});

test('unsubscribe clears armed ladder rungs', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [5000] } },
  );
  bus.publish('t', 'x');
  await flush();
  await flush();
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getStats().ladderPending, 1);
  sub.unsubscribe();
  assert.equal(bus.getStats().ladderPending, 0);
  await sleep(120);
  // The cleared rung never fired: no second delivery. (The subscriber is
  // gone, so its DLQ is gone with it — getDeadLetterMessages would throw
  // "unknown subscriber", the same as any post-unsubscribe query.)
  assert.deepEqual(seen, [0]);
});

test('without retryLadder the budget exhaustion dead-letters immediately (zero behavior change)', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1 } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  await flush();
  assert.deepEqual(seen, [0, 1]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  const stats = bus.getStats();
  assert.equal(stats.ladderPending, 0);
  assert.equal(stats.ladderAttempts, 0);
});

test('invalid retryLadder throws RangeError and leaves no half-registered subscription', () => {
  const bus = new EventBus();
  for (const bad of [[-1], [Number.NaN], ['1000'], 'nope']) {
    assert.throws(
      () =>
        bus.subscribeReliable('t', () => {}, {
          deadLetter: { retryLadder: bad as unknown as number[] },
        }),
      RangeError,
    );
  }
  assert.equal(bus.subscriberCount(), 0);
});

test('a degraded subscriber pauses the ladder until it resumes', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      // Overrun the health probe's processing budget, then nack: the
      // throw-free failure still counts as a health failure.
      const end = Date.now() + 40;
      while (Date.now() < end) {
        // busy-wait: deterministic without async races
      }
      d.nack();
    },
    {
      ackTimeoutMs: 1000,
      healthProbe: { maxConsecutiveFailures: 1, processingTimeoutMs: 15 },
      deadLetter: { maxRedeliveries: 0, retryLadder: [40] },
    },
  );
  bus.publish('t', 'slow');
  await sleep(250);
  // The first delivery failed and degraded the subscriber; the armed rung
  // kept re-arming instead of delivering while degraded.
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getStats().ladderPending, 1);
  bus.resume(sub.id);
  await sleep(250);
  // The ladder resumed: the rung delivered, failed again, and — the
  // single rung spent — the message dead-lettered.
  assert.deepEqual(seen, [0, 1]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().ladderPending, 0);
});

test('batch subscriptions walk the ladder per message, whole batch per rung', async () => {
  const bus = new EventBus();
  const batches: number[][] = [];
  const sub = bus.subscribeReliable(
    't',
    (deliveries) => {
      batches.push(deliveries.map((d) => d.redeliveries));
      for (const d of deliveries) d.nack();
    },
    {
      batch: true,
      ackTimeoutMs: 1000,
      deadLetter: { maxRedeliveries: 0, retryLadder: [30] },
    },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await sleep(300);
  // Both messages rode the single rung together, then dead-lettered.
  assert.deepEqual(batches, [
    [0, 0],
    [1, 1],
  ]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 2);
  assert.equal(bus.getStats().ladderAttempts, 2);
});
