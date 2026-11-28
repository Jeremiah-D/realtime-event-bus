/**
 * Manual durable-log segment-rotation trigger (EB-572).
 *
 * `triggerDurableLogSegmentRotation(topic?, actor?)` seals the live
 * segment of one topic — or of every topic with live records when no
 * topic is given — at once, so an operator can close segments inside a
 * maintenance window instead of waiting for the next append to cross
 * an entry or age budget. It runs the root log's rotation directly:
 * the same O(1) rename seals the segment and the same background
 * archive pass gzips and indexes it, so the trigger itself never
 * blocks publishing, and a bus with offload configured uploads the
 * sealed segment on that same background pass. Only the root log
 * rotates; namespace child logs keep their own live files. The trigger
 * is audited as `durable_log_segment_rotation_triggered`
 * (`{ topics }` — only the rotated topics, never segment names or
 * seqs). An empty or unknown topic is silently skipped (it seals
 * nothing and writes no empty archive segment), and a second trigger
 * right after the first reports an empty list. A bus with no durable
 * log, or with no rotation configured, refuses fail-closed — enable
 * rotation live with `setDurableLogSegmentRotation` (EB-566) first.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { type RemoteSegmentS3Client } from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-rotation-trigger-'));

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

/** In-memory `RemoteSegmentS3Client` over a shared object store, with per-client call records. */
class SharedStoreS3 implements RemoteSegmentS3Client {
  readonly puts: string[] = [];
  private readonly store: Map<string, Uint8Array>;

  constructor(store: Map<string, Uint8Array>) {
    this.store = store;
  }

  putObject(key: string, body: Uint8Array): void {
    this.puts.push(key);
    this.store.set(key, Uint8Array.from(body));
  }

  getObject(key: string): Uint8Array | undefined {
    const body = this.store.get(key);
    return body === undefined ? undefined : Uint8Array.from(body);
  }

  headObject(key: string): boolean {
    return this.store.has(key);
  }

  listObjects(prefix: string): string[] {
    return [...this.store.keys()].filter((k) => k.startsWith(prefix)).sort();
  }

  deleteObject(key: string): void {
    this.store.delete(key);
  }
}

/** A bus with segment rotation on its durable log (budget far above the test volumes). */
function rotationBus(dir?: string): EventBus {
  return new EventBus({
    durableLogDir: dir ?? freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 100 },
  });
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_segment_rotation_triggered' });

/** The bus's durable log (private field, reached the way the other durable-log revision tests reach it). */
const logOf = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

test('trigger seals one topic live segment; the archive index records the real seq range, and later appends continue the numbering', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('t', 0);
  bus.publish('t', 1);
  bus.publish('t', 2);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  await waitForArchived(dir, 't');
  const entries = archiveIndex(dir, 't');
  assert.equal(entries.length, 1);
  assert.equal(entries[0].topic, 't');
  assert.equal(entries[0].seqStart, 1);
  assert.equal(entries[0].seqEnd, 3);
  assert.equal(entries[0].entries, 3);
  // The live segment restarts empty: the next appends take seqs 4-5,
  // and the merged read returns the whole history in order.
  bus.publish('t', 3);
  bus.publish('t', 4);
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0, { includeArchived: true })
      .map((r) => r.seq),
    [1, 2, 3, 4, 5],
  );
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => r.seq),
    [4, 5],
  );
});

test('a trigger with no topic seals every topic with live records', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('a', 0);
  bus.publish('a', 1);
  bus.publish('b', 0);
  bus.publish('b', 1);
  bus.publish('b', 2);
  const result = bus.triggerDurableLogSegmentRotation();
  assert.deepEqual([...result.rotated].sort(), ['a', 'b']);
  await waitForArchived(dir, 'a');
  await waitForArchived(dir, 'b');
  assert.equal(archiveIndex(dir, 'a')[0].seqEnd, 2);
  assert.equal(archiveIndex(dir, 'b')[0].seqEnd, 3);
});

test('an empty topic and an unknown topic are skipped silently and write no empty segment', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('t', 0);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('empty'), { rotated: [] });
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('nope'), { rotated: [] });
  assert.deepEqual(bus.triggerDurableLogSegmentRotation(), { rotated: ['t'] });
  await waitForArchived(dir, 't');
  // No pending or archived segment was ever minted for the skipped topics.
  assert.equal(
    segmentFiles(dir, '.jsonl.tmp').filter(
      (f) => f.startsWith('empty.') || f.startsWith('nope.'),
    ).length,
    0,
  );
  assert.equal(
    segmentFiles(dir, '.jsonl.gz').filter(
      (f) => f.startsWith('empty.') || f.startsWith('nope.'),
    ).length,
    0,
  );
  assert.equal(archiveIndex(dir, 'empty').length, 0);
  assert.equal(archiveIndex(dir, 'nope').length, 0);
});

test('bus without a durable log refuses; a bad actor refuses; an empty or non-string topic refuses; no audit', () => {
  const bus = new EventBus({});
  assert.throws(
    () => bus.triggerDurableLogSegmentRotation(),
    /requires EventBusOptions.durableLogDir/,
  );
  assert.throws(() => bus.triggerDurableLogSegmentRotation('t', ''), RangeError);
  assert.throws(() => bus.triggerDurableLogSegmentRotation('', 'ops'), /non-empty string/);
  assert.throws(
    () => bus.triggerDurableLogSegmentRotation(42 as never),
    /non-empty string/,
  );
  assert.equal(audit(bus).length, 0);
});

test('bus without rotation refuses, and enabling rotation live unlocks the trigger', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('t', 0);
  bus.publish('t', 1);
  assert.throws(
    () => bus.triggerDurableLogSegmentRotation('t'),
    /durableLogSegmentRotation/,
  );
  assert.equal(audit(bus).length, 0);
  // EB-566: the stored history seeds the first segment's identity, so
  // the trigger seals the real range instead of stamping seq 0.
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 100 }, 'ops');
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  await waitForArchived(dir, 't');
  assert.equal(archiveIndex(dir, 't')[0].seqStart, 1);
  assert.equal(archiveIndex(dir, 't')[0].seqEnd, 2);
});

test('a trigger with offload configured uploads the sealed segment on the background pass', async () => {
  const store = new Map<string, Uint8Array>();
  const s3 = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 100 },
    durableLogSegmentOffload: { s3, retainLocalSegments: 0 },
  });
  bus.publish('t', 0);
  bus.publish('t', 1);
  bus.publish('t', 2);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  const key = 'durable-log-offload/t.seg-1-3.jsonl.gz';
  const manifestKey = 'durable-log-offload/manifest.json';
  await waitFor(() => store.has(key) && store.has(manifestKey));
  assert.ok(s3.puts.includes(key));
  assert.ok(s3.puts.includes(manifestKey));
});

test('the trigger is audited exactly: actor and rotated topics, never segment names or seqs', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('t', 0);
  bus.publish('t', 1);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t', 'ops-oncall'), {
    rotated: ['t'],
  });
  assert.equal(audit(bus).length, 1);
  assert.equal(audit(bus)[0].actor, 'ops-oncall');
  assert.deepEqual(audit(bus)[0].detail, { topics: ['t'] });
  // A segment name or a seq range must never reach the audit trail.
  assert.equal(JSON.stringify(audit(bus)).includes('seg-1-2'), false);
  assert.equal(JSON.stringify(audit(bus)).includes('seqStart'), false);
  // A second trigger right after the first is audited with an empty list.
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t', 'ops-oncall'), {
    rotated: [],
  });
  assert.equal(audit(bus).length, 2);
  assert.deepEqual(audit(bus)[1].detail, { topics: [] });
});

test('an immediate second trigger reports an empty list and mints no second segment', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('t', 0);
  bus.publish('t', 1);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: [] });
  assert.deepEqual(bus.triggerDurableLogSegmentRotation(), { rotated: [] });
  await waitForArchived(dir, 't');
  assert.equal(archiveIndex(dir, 't').length, 1);
  assert.equal(
    segmentFiles(dir, '.jsonl.gz').filter((f) => f.startsWith('t.seg-')).length,
    1,
  );
});
