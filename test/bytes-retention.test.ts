import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-bytes-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Fixed-shape record: one line's on-disk bytes are stable across seqs. */
const payloadOf = (i: number): string => `p-${i}-` + 'x'.repeat(100);

/** Max on-disk line bytes for the probe workload, used to size budgets. */
function maxLineBytes(dir: string, topic: string): number {
  const lines = readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0);
  assert.ok(lines.length > 0);
  return Math.max(...lines.map((l) => Buffer.byteLength(`${l}\n`, 'utf8')));
}

function probeMaxLine(n = 12): number {
  const dir = freshDir();
  const probe = DurableTopicLog.open({ dir });
  for (let i = 1; i <= n; i++) {
    probe.append({ seq: i, topic: 't', at: 1000, payload: payloadOf(i) });
  }
  return maxLineBytes(dir, 't');
}

describe('durable log byte retention (EB-119)', () => {
  it('rejects invalid maxBytesPerTopic', () => {
    for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '1000' as unknown as number]) {
      assert.throws(
        () => DurableTopicLog.open({ dir: freshDir(), maxBytesPerTopic: bad }),
        RangeError,
        `maxBytesPerTopic=${String(bad)}`,
      );
    }
  });

  it('byte overage triggers compaction and evicts the oldest records', () => {
    const lineMax = probeMaxLine();
    const budget = 5 * lineMax;
    const dir = freshDir();
    // Count budget far away: only the byte bound can fire.
    const log = DurableTopicLog.open({ dir, maxEntriesPerTopic: 1000, maxBytesPerTopic: budget });
    let appended = 0;
    let compacted = false;
    for (let i = 1; i <= 30; i++) {
      log.append({ seq: i, topic: 't', at: 1000, payload: payloadOf(i) });
      appended = i;
      // The byte-triggered compact runs synchronously inside append: the
      // moment it fires, live bytes are back within budget and the oldest
      // records are gone.
      if (log.stats().bytes <= budget && log.entryCount('t') < i) {
        compacted = true;
        break;
      }
    }
    assert.ok(compacted, 'expected a byte-triggered compact within 30 appends');
    assert.ok(log.stats().bytes <= budget, `bytes=${log.stats().bytes} budget=${budget}`);
    const seqs = log.readSince('t', 0).map((r) => r.seq);
    // Newest-first fill: the survivors are the newest records that fit.
    assert.equal(seqs[seqs.length - 1], appended);
    assert.ok(seqs[0] > 1, 'the oldest records were evicted');
    for (let k = 1; k < seqs.length; k++) assert.equal(seqs[k], seqs[k - 1] + 1);
  });

  it('amortized semantics: live bytes never exceed twice the budget between compacts', () => {
    const lineMax = probeMaxLine();
    const budget = 5 * lineMax;
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, maxEntriesPerTopic: 1000, maxBytesPerTopic: budget });
    for (let i = 1; i <= 60; i++) {
      log.append({ seq: i, topic: 't', at: 1000, payload: payloadOf(i) });
      assert.ok(
        log.stats().bytes <= 2 * budget,
        `append ${i}: bytes=${log.stats().bytes} budget=${budget}`,
      );
    }
    // Steady state keeps the newest history: the tail is always the newest seqs.
    const seqs = log.readSince('t', 0).map((r) => r.seq);
    assert.equal(seqs[seqs.length - 1], 60);
    assert.ok(seqs[0] > 40, 'only recent history survives sustained overage');
  });

  it('byte accounting is the actual on-disk line bytes', () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, maxEntriesPerTopic: 1000 });
    for (let i = 1; i <= 7; i++) {
      log.append({ seq: i, topic: 't', at: 1000, payload: payloadOf(i) });
    }
    const fileBytes = readFileSync(join(dir, `${encodeURIComponent('t')}.log`)).length;
    assert.equal(log.stats().bytes, fileBytes);
    // Recovery seeds the accounting from the file bytes too.
    const reopened = DurableTopicLog.open({ dir, maxEntriesPerTopic: 1000 });
    assert.equal(reopened.stats().bytes, fileBytes);
  });

  it('keyed compaction exempts the latest value per key from byte eviction', () => {
    const dir = freshDir();
    const budget = 500;
    const log = DurableTopicLog.open({
      dir,
      maxEntriesPerTopic: 1000,
      keyCompaction: true,
      maxBytesPerTopic: budget,
    });
    // Three hot keys, five rounds each: every round supersedes the last.
    // Each line is ~450 bytes, so three live keyed records (~1350 bytes)
    // exceed the 500-byte budget on their own.
    let keySeq = 0;
    for (let round = 1; round <= 5; round++) {
      for (const k of ['a', 'b', 'c']) {
        keySeq += 1;
        log.append({
          seq: keySeq,
          topic: 't',
          at: 1000,
          key: k,
          keySeq,
          payload: `${k}-round-${round}-` + 'x'.repeat(380),
        });
      }
    }
    // The latest value per key survives even though the three together
    // exceed the budget: the key space itself bounds retention,
    // Kafka-style — the budget bounds evictable history, not pinned state.
    const records = log.readSince('t', 0);
    assert.deepEqual(
      records.map((r) => r.key).sort(),
      ['a', 'b', 'c'],
    );
    assert.deepEqual(
      records.map((r) => r.seq).sort((x, y) => x - y),
      [13, 14, 15],
    );
    for (const r of records) {
      assert.match(r.payload as string, new RegExp(`^${r.key}-round-5-`));
    }
    assert.ok(log.stats().bytes > budget, 'exempt state may push live bytes over budget');
  });

  it('seq-0 schedule records are never evicted by the byte bound', () => {
    const dir = freshDir();
    const budget = 400;
    const log = DurableTopicLog.open({ dir, maxEntriesPerTopic: 1000, maxBytesPerTopic: budget });
    // A pending delayed-delivery schedule: timer state, not a message.
    log.append({ seq: 0, topic: 't', at: 100, delayId: 'delayed-1', deliverAt: 50_000, payload: 'p' });
    for (let i = 1; i <= 20; i++) {
      log.append({ seq: i, topic: 't', at: 1000, payload: payloadOf(i) });
    }
    // The byte-triggered compacts evicted old messages, but the seq-0
    // schedule record is still there — dropping it would lose the timer
    // on restart (the same rationale as maxAgeMs, EB-85).
    const seqZero = log.readSince('t', -1).filter((r) => r.seq === 0);
    assert.equal(seqZero.length, 1);
    assert.equal(seqZero[0].delayId, 'delayed-1');
  });

  it('over-budget live data archives instead of evicting when rotation is on', async () => {
    const dir = freshDir();
    const budget = 2000;
    const log = DurableTopicLog.open({
      dir,
      maxEntriesPerTopic: 10000,
      maxBytesPerTopic: budget,
      segmentRotation: { maxEntriesPerSegment: 1000 },
    });
    const big = (i: number): string => `p-${i}-` + 'y'.repeat(200);
    for (let i = 1; i <= 30; i++) {
      log.append({ seq: i, topic: 't', at: 1000 + i, payload: big(i) });
      // With rotation the byte budget is enforced at every append
      // boundary: crossing it archives the live segment (resetting live
      // bytes) instead of evicting.
      assert.ok(log.stats().bytes <= budget, `append ${i}: live bytes bounded`);
    }
    const prefix = `${encodeURIComponent('t')}.seg-`;
    await waitFor(
      () =>
        readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.tmp')).length === 0 &&
        readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz')).length >= 2,
    );
    // Nothing was lost: archived segments plus the live tail account for
    // every record, and all 30 replay by time (EB-42 resumeFromTime) via
    // the archive index.
    const stats = log.stats();
    assert.equal(stats.archived + stats.entries, 30);
    const bySeq = log.readSince('t', 0, { includeArchived: true }).map((r) => r.seq);
    assert.deepEqual(bySeq, Array.from({ length: 30 }, (_, k) => k + 1));
    const byTime = log.readSinceTime('t', 0, { includeArchived: true }).map((r) => r.seq);
    assert.deepEqual(byTime, Array.from({ length: 30 }, (_, k) => k + 1));
  });

  it('without maxBytesPerTopic there is zero behavior change', () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, maxEntriesPerTopic: 10 });
    assert.equal(log.stats().bytesLimit, undefined);
    for (let i = 1; i <= 25; i++) {
      log.append({ seq: i, topic: 't', at: 1000, payload: payloadOf(i) });
    }
    // Count-only compaction, exactly as before: newest 10 survive each
    // compact; the last compact ran at append 21, so 12..25 remain.
    assert.deepEqual(
      log.readSince('t', 0).map((r) => r.seq),
      Array.from({ length: 14 }, (_, k) => k + 12),
    );
    // Byte accounting is still maintained (it is pure observation).
    const fileBytes = readFileSync(join(dir, `${encodeURIComponent('t')}.log`)).length;
    assert.equal(log.stats().bytes, fileBytes);
  });

  it('the bus exposes bytes/bytesLimit and validates the option', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, durableLogMaxBytesPerTopic: 5000 });
    bus.publish('t', 'hello');
    await flush();
    const dl = bus.getStats().durableLog;
    assert.ok(dl !== undefined);
    assert.equal(dl.bytesLimit, 5000);
    assert.equal(dl.bytes, readFileSync(join(dir, `${encodeURIComponent('t')}.log`)).length);
    // Requires a durable log directory, like durableLogMaxAgeMs.
    assert.throws(() => new EventBus({ durableLogMaxBytesPerTopic: 100 }), RangeError);
    // Invalid values throw at construction.
    assert.throws(
      () => new EventBus({ durableLogDir: freshDir(), durableLogMaxBytesPerTopic: 0 }),
      RangeError,
    );
    assert.throws(
      () => new EventBus({ durableLogDir: freshDir(), durableLogMaxBytesPerTopic: -5 }),
      RangeError,
    );
  });

  it('byte accounting uses compressed wire bytes, not raw payload bytes', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, durableLogMaxBytesPerTopic: 1_000_000 });
    bus.setTopicCompression('t', { thresholdBytes: 100 });
    // Highly compressible: ~54KB raw per message.
    const raw = 'a'.repeat(54000);
    for (let i = 0; i < 5; i++) bus.publish('t', raw);
    await flush();
    const fileBytes = readFileSync(join(dir, `${encodeURIComponent('t')}.log`)).length;
    const dl = bus.getStats().durableLog;
    assert.ok(dl !== undefined);
    // The log stores the wire payload (the deflated envelope) — the byte
    // budget counts what is actually on disk, not the raw 270KB.
    assert.equal(dl.bytes, fileBytes);
    assert.ok(fileBytes < 54000, `fileBytes=${fileBytes}: compressed wire bytes counted`);
    assert.equal(dl.bytesLimit, 1_000_000);
  });
});
