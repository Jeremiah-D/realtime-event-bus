/**
 * Manual durable-log compaction trigger (EB-573).
 *
 * `triggerDurableLogCompaction(topic?, actor?)` rewrites the live file
 * of one topic — or of every topic with live records when no topic is
 * given — at once, so an operator can reclaim dead weight (expired
 * records, superseded keys) inside a maintenance window instead of
 * waiting for the next append to cross twice the bound. It runs the
 * root log's compaction directly: the same pipeline (key TTL, age
 * retention, byte bound, keyed compaction / count slice, atomic
 * rewrite, index rebuilds) reclaims the space synchronously, and only
 * the live file is rewritten — archived segments are untouched, and
 * seq-0 delayed-delivery schedule records never age out. Only the root
 * log compacts; namespace child logs keep their own live files. The
 * trigger is audited as `durable_log_compaction_triggered`
 * (`{ topics }` — only the compacted topics, never record counts or
 * byte totals). A topic already inside every bound is silently skipped
 * (not listed, not rewritten), as are an empty or unknown topic, and a
 * second trigger right after the first reports an empty list. A bus
 * with no durable log refuses fail-closed. The keyCompaction switch is
 * not a gate: without it the same count/age/byte bounds still rewrite
 * the file.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-compaction-trigger-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

const segmentFiles = (dir: string, ext: '.jsonl.gz' | '.jsonl.tmp') =>
  readdirSync(dir).filter((f) => f.endsWith(ext));

/** Waits until the topic's closed segment is fully archived (gz + index, tmp gone). */
async function waitForArchived(dir: string, topic: string): Promise<void> {
  const prefix = `${encodeURIComponent(topic)}.seg-`;
  await waitFor(
    () =>
      segmentFiles(dir, '.jsonl.tmp').filter((f) => f.startsWith(prefix)).length === 0 &&
      segmentFiles(dir, '.jsonl.gz').filter((f) => f.startsWith(prefix)).length > 0,
  );
}

/** The archive index entries for one topic, in index order. */
const archiveIndex = (dir: string, topic: string) =>
  readFileSync(join(dir, '__archive_index.jsonl'), 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line))
    .filter((entry) => entry.topic === topic);

/** The live-file lines for one topic. */
const linesOf = (dir: string, topic: string) =>
  readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0);

/** The live-file bytes for one topic. */
const bytesOf = (dir: string, topic: string) =>
  readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8').length;

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_compaction_triggered' });

/** The bus's durable log (private field, reached the way the other durable-log revision tests reach it). */
const logOf = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

test('tightening the bound then triggering reclaims at once: the file shrinks and readSince keeps exactly the newest records', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  assert.equal(linesOf(dir, 't').length, 10);
  const beforeBytes = bytesOf(dir, 't');
  // Tightening alone compacts nothing (EB-322): the file and the read
  // view are untouched until the trigger runs.
  bus.setDurableLogMaxEntriesPerTopic(4);
  assert.equal(linesOf(dir, 't').length, 10);
  assert.equal(bytesOf(dir, 't'), beforeBytes);
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => r.seq),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
  assert.deepEqual(bus.triggerDurableLogCompaction('t'), { compacted: ['t'] });
  assert.ok(bytesOf(dir, 't') < beforeBytes);
  assert.equal(linesOf(dir, 't').length, 4);
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => [r.seq, r.payload]),
    [
      [7, 6],
      [8, 7],
      [9, 8],
      [10, 9],
    ],
  );
});

test('keyed superseded dead weight is reclaimed and the key index is rebuilt: the next write still follows the latest seq', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogKeyCompaction: true });
  bus.publish('t', 'v1', { key: 'K' });
  bus.publish('t', 'v2', { key: 'K' });
  bus.publish('t', 'v3', { key: 'K' });
  assert.equal(linesOf(dir, 't').length, 3);
  // The read view already shows only the latest value; the file still
  // holds the superseded dead weight until the trigger rewrites it.
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => r.payload),
    ['v3'],
  );
  assert.deepEqual(bus.triggerDurableLogCompaction('t'), { compacted: ['t'] });
  assert.equal(linesOf(dir, 't').length, 1);
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => [r.seq, r.payload]),
    [[3, 'v3']],
  );
  // The seq baseline survived the rewrite: the next write for the same
  // key takes seq 4, not a renumbered 2.
  bus.publish('t', 'v4', { key: 'K' });
  assert.equal(linesOf(dir, 't').length, 2);
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => [r.seq, r.payload]),
    [[4, 'v4']],
  );
});

test('an in-bounds topic is skipped and its file is not rewritten', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('t', 0);
  bus.publish('t', 1);
  bus.publish('u', 0);
  const tBytes = bytesOf(dir, 't');
  const uBytes = bytesOf(dir, 'u');
  assert.deepEqual(bus.triggerDurableLogCompaction('t'), { compacted: [] });
  assert.deepEqual(bus.triggerDurableLogCompaction(), { compacted: [] });
  assert.equal(bytesOf(dir, 't'), tBytes);
  assert.equal(bytesOf(dir, 'u'), uBytes);
});

test('an empty topic and an unknown topic are skipped silently and the live file is untouched', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('t', 0);
  bus.publish('t', 1);
  const tBytes = bytesOf(dir, 't');
  assert.deepEqual(bus.triggerDurableLogCompaction(''), { compacted: [] });
  assert.deepEqual(bus.triggerDurableLogCompaction('nope'), { compacted: [] });
  assert.equal(bytesOf(dir, 't'), tBytes);
  // No live file was ever minted for the skipped topics.
  assert.equal(
    readdirSync(dir).filter((f) => f.startsWith('nope.') || f === '.log').length,
    0,
  );
  // The skips were harmless: the real topic still compacts afterwards.
  bus.setDurableLogMaxEntriesPerTopic(1);
  assert.deepEqual(bus.triggerDurableLogCompaction('t'), { compacted: ['t'] });
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => r.seq),
    [2],
  );
});

test('bus without a durable log refuses; a bad actor refuses; a non-string topic refuses; no audit', () => {
  const bus = new EventBus({});
  assert.throws(
    () => bus.triggerDurableLogCompaction(),
    /requires EventBusOptions.durableLogDir/,
  );
  assert.throws(
    () => bus.triggerDurableLogCompaction('', 'ops'),
    /requires EventBusOptions.durableLogDir/,
  );
  assert.throws(() => bus.triggerDurableLogCompaction('t', ''), RangeError);
  assert.throws(
    () => bus.triggerDurableLogCompaction(42 as never),
    /must be a string/,
  );
  assert.equal(audit(bus).length, 0);
});

test('archived segment bytes are untouched by the trigger and the archived history still replays', async () => {
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 100 },
  });
  bus.publish('t', 0);
  bus.publish('t', 1);
  bus.publish('t', 2);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  await waitForArchived(dir, 't');
  const gz = segmentFiles(dir, '.jsonl.gz').find((f) => f.startsWith('t.seg-'));
  assert.ok(gz !== undefined);
  const gzBytes = readFileSync(join(dir, gz));
  assert.equal(archiveIndex(dir, 't').length, 1);
  assert.equal(archiveIndex(dir, 't')[0].seqEnd, 3);
  // Seven more records on the live file, then a tightened bound and a
  // compaction trigger: only the live file may change.
  for (let i = 3; i < 10; i++) bus.publish('t', i);
  bus.setDurableLogMaxEntriesPerTopic(3);
  assert.deepEqual(bus.triggerDurableLogCompaction('t'), { compacted: ['t'] });
  assert.deepEqual(readFileSync(join(dir, gz)), gzBytes);
  assert.equal(archiveIndex(dir, 't').length, 1);
  assert.equal(archiveIndex(dir, 't')[0].seqEnd, 3);
  // The archived segment (seqs 1-3) replays intact alongside the
  // compacted live remainder (seqs 8-10); seqs 4-7 were evicted by the
  // tightened bound, which is the compaction doing its job.
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0, { includeArchived: true })
      .map((r) => r.seq),
    [1, 2, 3, 8, 9, 10],
  );
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => r.seq),
    [8, 9, 10],
  );
});

test('the trigger is audited exactly: actor and compacted topics, never record counts or byte totals', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  bus.setDurableLogMaxEntriesPerTopic(4);
  assert.deepEqual(bus.triggerDurableLogCompaction('t', 'ops-oncall'), {
    compacted: ['t'],
  });
  assert.equal(audit(bus).length, 1);
  assert.equal(audit(bus)[0].actor, 'ops-oncall');
  assert.deepEqual(audit(bus)[0].detail, { topics: ['t'] });
  // A second trigger right after the first is audited with an empty list.
  assert.deepEqual(bus.triggerDurableLogCompaction('t', 'ops-oncall'), {
    compacted: [],
  });
  assert.equal(audit(bus).length, 2);
  assert.deepEqual(audit(bus)[1].detail, { topics: [] });
});

test('an immediate second trigger reports an empty list and rewrites nothing', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  bus.setDurableLogMaxEntriesPerTopic(4);
  assert.deepEqual(bus.triggerDurableLogCompaction('t'), { compacted: ['t'] });
  const afterBytes = bytesOf(dir, 't');
  assert.deepEqual(bus.triggerDurableLogCompaction('t'), { compacted: [] });
  assert.deepEqual(bus.triggerDurableLogCompaction(), { compacted: [] });
  assert.equal(bytesOf(dir, 't'), afterBytes);
});
