import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { validateRedeliveryBackoff, RedeliveryBackoff } from '../src/redelivery-backoff.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// Module unit tests: validation and the delay computation.
// ---------------------------------------------------------------------------

test('validateRedeliveryBackoff: absent means disabled', () => {
  assert.equal(validateRedeliveryBackoff(undefined, 't'), undefined);
});

test('validateRedeliveryBackoff: normalizes a valid config', () => {
  const config = validateRedeliveryBackoff({ baseMs: 100, capMs: 500 }, 't');
  assert.deepEqual(config, { baseMs: 100, capMs: 500, jitter: false });
  // capMs absent means uncapped; jitter absent means off.
  assert.deepEqual(validateRedeliveryBackoff({ baseMs: 100 }, 't'), {
    baseMs: 100,
    capMs: Number.POSITIVE_INFINITY,
    jitter: false,
  });
  assert.deepEqual(validateRedeliveryBackoff({ baseMs: 100, jitter: true }, 't'), {
    baseMs: 100,
    capMs: Number.POSITIVE_INFINITY,
    jitter: true,
  });
  // The returned config is a fresh object, not the input.
  const input = { baseMs: 100 };
  assert.notEqual(validateRedeliveryBackoff(input, 't'), input);
});

test('validateRedeliveryBackoff: illegal values throw RangeError', () => {
  for (const bad of [100, '100', null, true, [100]]) {
    assert.throws(() => validateRedeliveryBackoff(bad, 't'), RangeError);
  }
  for (const bad of [{}, { baseMs: 0 }, { baseMs: -5 }, { baseMs: Number.NaN }, { baseMs: Number.POSITIVE_INFINITY }, { baseMs: '100' }]) {
    assert.throws(() => validateRedeliveryBackoff(bad, 't'), RangeError);
  }
  for (const bad of [{ baseMs: 100, capMs: 50 }, { baseMs: 100, capMs: -1 }, { baseMs: 100, capMs: Number.NaN }, { baseMs: 100, capMs: Number.POSITIVE_INFINITY }]) {
    assert.throws(() => validateRedeliveryBackoff(bad, 't'), RangeError);
  }
  for (const bad of [{ baseMs: 100, jitter: 1 }, { baseMs: 100, jitter: 'yes' }]) {
    assert.throws(() => validateRedeliveryBackoff(bad, 't'), RangeError);
  }
});

test('RedeliveryBackoff: delayFor grows exponentially and honors the cap', () => {
  const backoff = new RedeliveryBackoff({ baseMs: 100, capMs: 250, jitter: false });
  assert.equal(backoff.delayFor(1), 100);
  assert.equal(backoff.delayFor(2), 200);
  assert.equal(backoff.delayFor(3), 250);
  assert.equal(backoff.delayFor(10), 250);
  // Attempt numbers below 1 behave like attempt 1.
  assert.equal(backoff.delayFor(0), 100);
  const uncapped = new RedeliveryBackoff({ baseMs: 100, capMs: Number.POSITIVE_INFINITY, jitter: false });
  assert.equal(uncapped.delayFor(4), 800);
});

test('RedeliveryBackoff: jitter draws uniformly from [0, computed delay)', () => {
  const backoff = new RedeliveryBackoff(
    { baseMs: 100, capMs: 1000, jitter: true },
    { random: () => 0.5 },
  );
  assert.equal(backoff.delayFor(1), 50);
  assert.equal(backoff.delayFor(3), 200);
});

test('RedeliveryBackoff: schedule arms the delay on injected timers and reports it', () => {
  const scheduled: Array<{ cb: () => void; delay: number; cancelled: boolean }> = [];
  let nowMs = 42;
  const backoff = new RedeliveryBackoff(
    { baseMs: 100, capMs: 1000, jitter: false },
    {
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
    },
  );
  let fired = 0;
  const handle = backoff.schedule(2, () => {
    fired += 1;
  });
  assert.equal(scheduled.length, 1);
  assert.equal(scheduled[0].delay, 200);
  assert.equal(handle.delayMs, 200);
  assert.equal(handle.armedAtMs, 42);
  nowMs = 43;
  scheduled[0].cb();
  assert.equal(fired, 1);
  handle.cancel(); // cancelling after the fire is a no-op
  assert.equal(scheduled[0].cancelled, false);
});

test('RedeliveryBackoff: cancel prevents the delay from firing', () => {
  const scheduled: Array<{ cb: () => void; cancelled: boolean }> = [];
  const backoff = new RedeliveryBackoff(
    { baseMs: 100, capMs: 1000, jitter: false },
    {
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
    },
  );
  let fired = 0;
  const handle = backoff.schedule(1, () => {
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
// Bus integration: per-subscriber redelivery backoff.
// ---------------------------------------------------------------------------

test('subscribeReliable rejects an illegal redeliveryBackoff before registering', async () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        redeliveryBackoff: { baseMs: 0 },
      }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        redeliveryBackoff: { baseMs: 100, capMs: 50 },
      }),
    RangeError,
  );
  // No half-registered subscription survives the rejection: a publish
  // after the throws reaches nobody.
  bus.publish('t', 'x');
  await flush();
  assert.equal(bus.getStats().deliveredMessages, 0);
});

test('nack redeliveries wait out the exponential backoff', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const at: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      at.push(Date.now());
      if (d.redeliveries < 2) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000, redeliveryBackoff: { baseMs: 40 } },
  );
  bus.publish('t', 'flaky');
  await sleep(25);
  // The first redelivery is delayed, not immediate.
  assert.deepEqual(seen, [0]);
  await sleep(150);
  assert.deepEqual(seen, [0, 1, 2]);
  // Delays are honored (loose lower bounds — CI-safe): attempt 1 waits
  // baseMs, attempt 2 waits baseMs * 2.
  assert.ok(at[1] - at[0] >= 25, `attempt 1 fired too early: ${at[1] - at[0]}ms`);
  assert.ok(at[2] - at[1] >= 55, `attempt 2 fired too early: ${at[2] - at[1]}ms`);
});

test('ack-timeout redeliveries wait out the backoff too', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const at: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      at.push(Date.now());
      if (d.redeliveries >= 1) d.ack();
      // First delivery never settles: the ack timeout drives the redelivery.
    },
    { ackTimeoutMs: 30, redeliveryBackoff: { baseMs: 60 } },
  );
  bus.publish('t', 'stalled');
  await sleep(60);
  // The ack timeout (30ms) fired, but the backoff (60ms) has not elapsed.
  assert.deepEqual(seen, [0]);
  await sleep(120);
  assert.deepEqual(seen, [0, 1]);
  // Redelivery lands after timeout + backoff (loose lower bound).
  assert.ok(at[1] - at[0] >= 60, `ack-timeout redelivery fired too early: ${at[1] - at[0]}ms`);
});

test('without the option, nack redelivers immediately as before', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries < 3) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000 },
  );
  bus.publish('t', 'x');
  await sleep(150);
  assert.deepEqual(seen, [0, 1, 2, 3]);
});

test('unsubscribe cancels an armed backoff delay', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 5000, redeliveryBackoff: { baseMs: 5000 } },
  );
  bus.publish('t', 'x');
  await flush();
  await flush();
  assert.deepEqual(seen, [0]);
  sub.unsubscribe();
  await sleep(120);
  assert.deepEqual(seen, [0]);
});

test('the backoff never delays a dead-lettering: budget exhaustion dead-letters at once', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack(); // always fail
    },
    {
      ackTimeoutMs: 5000,
      deadLetter: { maxRedeliveries: 1 },
      redeliveryBackoff: { baseMs: 400 },
    },
  );
  bus.publish('t', 'poison');
  // Wait for the single in-budget redelivery (400ms backoff), then poll
  // for the DLQ entry: it must appear promptly after the second nack,
  // not after another 400ms backoff.
  for (let i = 0; i < 60 && seen.length < 2; i++) await sleep(25);
  assert.deepEqual(seen, [0, 1]);
  let dlq: unknown[] = [];
  for (let i = 0; i < 10; i++) {
    dlq = bus.getDeadLetterMessages(sub.id);
    if (dlq.length === 1) break;
    await sleep(10);
  }
  assert.equal(dlq.length, 1);
});

test('a message that expires while waiting is dropped at the hand-off, never resurrected', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 5000, redeliveryBackoff: { baseMs: 80 } },
  );
  // TTL (40ms) expires while the backoff delay (80ms) is still armed.
  bus.publish('t', 'short-lived', { ttlMs: 40 });
  await sleep(300);
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a graceful drain stays open across a pending backoff and completes on ack', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries === 0) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000, redeliveryBackoff: { baseMs: 60 } },
  );
  bus.publish('t', 'x');
  await flush();
  await flush();
  assert.deepEqual(seen, [0]);
  sub.unsubscribe({ drain: true });
  // The drain is open: the backoff delay is outstanding work, not a
  // settled backlog.
  assert.equal(bus.getStats().drainingSubscribers, 1);
  await sleep(30);
  assert.equal(bus.getStats().drainingSubscribers, 1);
  // The delayed redelivery arrives during the drain, is acked, and only
  // then does the drain complete.
  await sleep(200);
  assert.deepEqual(seen, [0, 1]);
  assert.equal(bus.getStats().drainingSubscribers, 0);
});
