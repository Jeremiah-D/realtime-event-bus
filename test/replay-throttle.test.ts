import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type ReplayProgressEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-replay-throttle-'));

function busWithLog(clock?: { now: () => number }) {
  return new EventBus({ durableLogDir: freshDir(), ...(clock ? { now: clock.now } : {}) });
}

test('disabled by default: a resume replay completes synchronously', async () => {
  const bus = busWithLog();
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  const rows = bus.getStats().replayProgress;
  assert.equal(rows.length, 1);
  assert.deepEqual(
    { total: rows[0].total, replayed: rows[0].replayed, skipped: rows[0].skipped, done: rows[0].done, failed: rows[0].failed },
    { total: 3, replayed: 3, skipped: 0, done: true, failed: false },
  );
  assert.equal(bus.getStats().replayThrottled, 0);
  await flush();
  assert.deepEqual(received, [0, 1, 2]);
  bus.destroy?.();
});

test('invalid replayThrottle options throw RangeError and register nothing', () => {
  const bad: unknown[] = [
    { messagesPerSec: 0 },
    { messagesPerSec: -5 },
    { messagesPerSec: NaN },
    { messagesPerSec: Infinity },
    { messagesPerSec: 10, burst: 0 },
    { messagesPerSec: 10, burst: -2 },
    { messagesPerSec: 10, burst: NaN },
    'fast',
    42,
  ];
  for (const replayThrottle of bad) {
    const bus = busWithLog();
    assert.throws(
      () =>
        bus.subscribe('t', () => {}, {
          resumeFromSeq: 0,
          replayThrottle: replayThrottle as never,
        }),
      RangeError,
      `expected RangeError for ${JSON.stringify(replayThrottle)}`,
    );
    assert.equal(bus.getStats().replayProgress.length, 0);
    bus.destroy?.();
  }
  // Any truthy opt-in without a resume cursor throws instead of silently
  // never engaging.
  for (const replayThrottle of [true, { messagesPerSec: 10 }]) {
    const bus = busWithLog();
    assert.throws(
      () => bus.subscribe('t', () => {}, { replayThrottle: replayThrottle as never }),
      RangeError,
    );
    bus.destroy?.();
  }
  // `false` is the same as disabled.
  const bus = busWithLog();
  for (let i = 0; i < 2; i++) bus.publish('t', i);
  bus.subscribe('t', () => {}, { resumeFromSeq: 0, replayThrottle: false });
  assert.equal(bus.getStats().replayProgress[0].done, true);
  bus.destroy?.();
});

test('throttle pauses the pass on budget exhaustion; flush resumes it after the clock advances', async () => {
  const clock = controllableClock();
  const bus = busWithLog(clock);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  const received: unknown[] = [];
  const events: ReplayProgressEvent[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromSeq: 0,
    replayThrottle: { messagesPerSec: 10, burst: 2 },
    onReplayProgress: (e) => events.push(e),
    progressInterval: 1,
  });
  // First chunk ran synchronously inside subscribe: burst of 2 admitted,
  // the pass is paused — not done, not failed, nothing dropped.
  const rows = bus.getStats().replayProgress;
  assert.equal(rows.length, 1);
  assert.deepEqual(
    { total: rows[0].total, replayed: rows[0].replayed, skipped: rows[0].skipped, done: rows[0].done, failed: rows[0].failed },
    { total: 5, replayed: 2, skipped: 0, done: false, failed: false },
  );
  assert.equal(bus.getStats().replayThrottled, 1);
  assert.equal(bus.getStats().sequenceGaps, 0);

  await flush();
  assert.deepEqual(received, [0, 1]); // the admitted chunk drains normally

  // Advance the injected clock past 2 refill windows, then anything that
  // flushes resumes the pass deterministically — no timer wait needed.
  // The bucket refills to its burst cap of 2, so the last record waits
  // for one more window: pacing is exact, never over-admitting.
  clock.nowMs += 200;
  bus.publish('zz', {}); // unmatched topic: only triggers a flush
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3]);
  const mid = bus.getStats().replayProgress[0];
  assert.equal(mid.replayed, 4);
  assert.equal(mid.done, false);
  assert.equal(bus.getStats().replayThrottled, 1);

  clock.nowMs += 100;
  bus.publish('zz', {});
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]); // order preserved, nothing lost
  const done = bus.getStats().replayProgress[0];
  assert.deepEqual(
    { total: done.total, replayed: done.replayed, skipped: done.skipped, done: done.done, failed: done.failed },
    { total: 5, replayed: 5, skipped: 0, done: true, failed: false },
  );
  assert.equal(bus.getStats().replayThrottled, 0);
  assert.equal(bus.getStats().sequenceGaps, 0);
  // The EB-71 observer kept its throttled reporting across the pause and
  // fired exactly one completion event.
  const completions = events.filter((e) => e.done);
  assert.equal(completions.length, 1);
  assert.equal(completions[0].replayed, 5);
  assert.equal(completions[0].failed, false);
  bus.destroy?.();
});

test('skipped records never burn throttle budget', async () => {
  const clock = controllableClock();
  const bus = busWithLog(clock);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromSeq: 0,
    replayThrottle: { messagesPerSec: 10, burst: 3 },
    filter: (payload) => (payload as number) % 2 === 0,
  });
  // 3 admitted records fit the burst of 3; the 2 filtered records cost
  // nothing — the whole pass completes synchronously, never pausing.
  const row = bus.getStats().replayProgress[0];
  assert.deepEqual(
    { total: row.total, replayed: row.replayed, skipped: row.skipped, done: row.done },
    { total: 5, replayed: 3, skipped: 2, done: true },
  );
  assert.equal(bus.getStats().replayThrottled, 0);
  await flush();
  assert.deepEqual(received, [0, 2, 4]);
  bus.destroy?.();
});

test('resumeFromTime replays are throttled the same way', async () => {
  const clock = controllableClock(1_000);
  const bus = busWithLog(clock);
  for (let i = 0; i < 4; i++) {
    bus.publish('t', i);
    clock.nowMs += 10;
  }
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromTime: 999,
    replayThrottle: { messagesPerSec: 10, burst: 2 },
  });
  const row = bus.getStats().replayProgress[0];
  assert.equal(row.total, 4);
  assert.equal(row.replayed, 2);
  assert.equal(row.done, false);
  assert.equal(bus.getStats().replayThrottled, 1);
  clock.nowMs += 500;
  bus.publish('zz', {});
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3]);
  assert.equal(bus.getStats().replayProgress[0].done, true);
  assert.equal(bus.getStats().replayThrottled, 0);
  bus.destroy?.();
});

test('the armed timer resumes the pass on an idle bus (real clock)', async () => {
  const bus = busWithLog(); // real Date.now clock
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromSeq: 0,
    replayThrottle: { messagesPerSec: 50, burst: 1 },
  });
  assert.equal(bus.getStats().replayProgress[0].replayed, 1);
  assert.equal(bus.getStats().replayProgress[0].done, false);
  // No further publishes: the unref'd resume timer paces the rest.
  await sleep(400);
  await flush();
  assert.deepEqual(received, [0, 1, 2]);
  assert.equal(bus.getStats().replayProgress[0].done, true);
  assert.equal(bus.getStats().replayThrottled, 0);
  bus.destroy?.();
});

test('unsubscribing mid-replay cancels the armed resume', async () => {
  const clock = controllableClock();
  const bus = busWithLog(clock);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromSeq: 0,
    replayThrottle: { messagesPerSec: 10, burst: 2 },
  });
  assert.equal(bus.getStats().replayThrottled, 1);
  sub.unsubscribe();
  assert.equal(bus.getStats().replayThrottled, 0);
  // Advancing the clock and waiting out the (cancelled) timer must not
  // resurrect the pass or throw.
  clock.nowMs += 10_000;
  await sleep(150);
  await flush();
  assert.deepEqual(received, []);
  bus.destroy?.();
});

test('replayThrottle: true uses the documented defaults', async () => {
  const bus = busWithLog();
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromSeq: 0,
    replayThrottle: true,
  });
  const row = bus.getStats().replayProgress[0];
  assert.equal(row.total, 3);
  assert.equal(row.replayed, 3);
  assert.equal(row.done, true);
  await flush();
  assert.deepEqual(received, [0, 1, 2]);
  bus.destroy?.();
});
