import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** A manually-advanced clock handed to the bus / log. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-retention-'));

test('EB-85: DurableTopicLog.open rejects invalid maxAgeMs', () => {
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '1000' as unknown as number]) {
    assert.throws(
      () => DurableTopicLog.open({ dir: freshDir(), maxAgeMs: bad }),
      RangeError,
      `maxAgeMs=${String(bad)}`,
    );
  }
  // Non-function now is rejected too.
  assert.throws(
    () => DurableTopicLog.open({ dir: freshDir(), now: 42 as unknown as () => number }),
    RangeError,
  );
});

test('EB-85: setMaxAgeMs validates and returns the previous bound', () => {
  const log = DurableTopicLog.open({ dir: freshDir(), maxAgeMs: 1000 });
  assert.equal(log.setMaxAgeMs(2000), 1000);
  assert.equal(log.setMaxAgeMs(undefined), 2000);
  assert.equal(log.setMaxAgeMs(undefined), undefined);
  for (const bad of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => log.setMaxAgeMs(bad), RangeError, `maxAgeMs=${String(bad)}`);
  }
});

test('EB-85: compact evicts records older than now-maxAgeMs and counts them', () => {
  const clock = controllableClock(1_000);
  // Small count budget so one burst triggers the amortized compact (2x).
  const log = DurableTopicLog.open({
    dir: freshDir(),
    maxEntriesPerTopic: 10,
    maxAgeMs: 1_000,
    now: clock.now,
  });
  // 11 records at t=1000 (old), then the clock moves past the window.
  for (let i = 1; i <= 11; i++) {
    log.append({ seq: i, topic: 't', at: 1_000, payload: `old-${i}` });
  }
  clock.nowMs = 5_000;
  for (let i = 12; i <= 21; i++) {
    log.append({ seq: i, topic: 't', at: 5_000, payload: `new-${i}` });
  }
  // 21 appends crossed the 2x trigger (20): compact ran, the 11 stale
  // records were evicted by age, the 10 fresh ones survive both bounds.
  assert.equal(log.stats().retentionEvicted, 11);
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    Array.from({ length: 10 }, (_, k) => `new-${k + 12}`),
  );
  assert.equal(log.stats().entries, 10);
});

test('EB-85: seq-0 delayed schedule records never age out', () => {
  const clock = controllableClock(1_000);
  const log = DurableTopicLog.open({
    dir: freshDir(),
    maxEntriesPerTopic: 10,
    maxAgeMs: 1_000,
    now: clock.now,
  });
  // A pending delayed-delivery schedule from long ago: a timer intent,
  // not a message — retention must not drop it.
  log.append({ seq: 0, topic: 't', at: 100, delayId: 'delayed-1', deliverAt: 50_000, payload: 'p' });
  for (let i = 1; i <= 11; i++) {
    log.append({ seq: i, topic: 't', at: 100, payload: `old-${i}` });
  }
  clock.nowMs = 5_000;
  for (let i = 12; i <= 20; i++) {
    log.append({ seq: i, topic: 't', at: 5_000, payload: `new-${i}` });
  }
  // 21 appends cross the 2x trigger (20): compact evicted the 11 stale
  // messages by age, kept the ancient seq-0 schedule record plus the 9
  // fresh messages (10 retained <= the count bound, so the count trim is
  // a no-op and the timer intent survives intact).
  assert.equal(log.stats().retentionEvicted, 11);
  assert.equal(log.stats().entries, 10);
  const records = log.readSince('t', 0);
  assert.deepEqual(
    records.map((r) => r.payload),
    Array.from({ length: 9 }, (_, k) => `new-${k + 12}`),
  );
});

test('EB-85: without maxAgeMs, old records survive compaction (backward compatible)', () => {
  const clock = controllableClock(1_000);
  const log = DurableTopicLog.open({
    dir: freshDir(),
    maxEntriesPerTopic: 10,
    now: clock.now,
  });
  for (let i = 1; i <= 21; i++) {
    log.append({ seq: i, topic: 't', at: 1_000, payload: i });
  }
  clock.nowMs = 999_999;
  log.append({ seq: 22, topic: 't', at: 999_999, payload: 22 });
  // Count-only compaction: the newest 10 survive each compact, none
  // counted as retention-evicted.
  assert.equal(log.stats().retentionEvicted, 0);
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.seq),
    [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
  );
});

test('EB-85: records exactly on the cutoff are kept', () => {
  const clock = controllableClock(2_000);
  const log = DurableTopicLog.open({
    dir: freshDir(),
    maxEntriesPerTopic: 10,
    maxAgeMs: 1_000,
    now: clock.now,
  });
  // at=1000 is exactly now(2000)-maxAgeMs(1000): kept (only strictly
  // older records go).
  log.append({ seq: 1, topic: 't', at: 1_000, payload: 'edge' });
  for (let i = 2; i <= 21; i++) {
    log.append({ seq: i, topic: 't', at: 2_000, payload: `n-${i}` });
  }
  assert.equal(log.stats().retentionEvicted, 0);
  assert.equal(log.readSince('t', 0).length, 10);
  assert.equal(log.readSince('t', 0)[0].payload, 'n-12');
});

test('EB-85: bus-level retention end to end with injected clock', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 1_000,
    now: clock.now,
  });
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload));
  for (let i = 0; i < 6; i++) bus.publish('t', `old-${i}`);
  clock.nowMs = 10_000;
  // 11 appends total cross the 2x trigger (10): the 6 stale records are
  // evicted by age at compact time.
  for (let i = 0; i < 5; i++) bus.publish('t', `new-${i}`);
  await new Promise<void>((resolve) => setImmediate(resolve));
  const stats = bus.getStats();
  assert.equal(stats.durableLog?.retentionEvicted, 6);
  // A fresh subscriber resuming from the log never sees the evicted past.
  const replayed: unknown[] = [];
  bus.subscribe(
    't',
    (msg) => replayed.push(msg.payload),
    { resumeFromSeq: 0 },
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(replayed, ['new-0', 'new-1', 'new-2', 'new-3', 'new-4']);
  assert.deepEqual(seen.slice(0, 6), ['old-0', 'old-1', 'old-2', 'old-3', 'old-4', 'old-5']);
});

test('EB-85: setDurableLogMaxAgeMs changes policy at runtime and audits it', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.setDurableLogMaxAgeMs(60_000, 'ops');
  bus.setDurableLogMaxAgeMs(undefined, 'ops');
  assert.throws(() => bus.setDurableLogMaxAgeMs(0), RangeError);
  assert.throws(() => bus.setDurableLogMaxAgeMs(-1), RangeError);
  const events = bus.getSubscriptionAudit({ action: 'retention_updated' });
  assert.equal(events.length, 2);
  assert.deepEqual(events[0].detail, { from: null, to: 60_000 });
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[1].detail, { from: 60_000, to: null });
});

test('EB-85: setDurableLogMaxAgeMs without a durable log throws', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setDurableLogMaxAgeMs(1000), RangeError);
});

test('EB-85: durableLogMaxAgeMs without durableLogDir throws at construction', () => {
  assert.throws(
    () => new EventBus({ durableLogMaxAgeMs: 1000 }),
    RangeError,
  );
});
