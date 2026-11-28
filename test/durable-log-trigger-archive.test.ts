/**
 * Manual durable-log archive trigger (EB-574).
 *
 * `triggerDurableLogArchive(topic?, actor?)` gzips and indexes every
 * pending segment (`*.jsonl.tmp`) — or only one topic's — at once, so an
 * operator can confirm a sealed segment is archived (gzip + sidecar +
 * index + chain manifest) inside a maintenance window instead of
 * waiting for the next background archive tick. It runs the root log's
 * archive loop directly: the same `archiveOne` per segment, the same
 * crash-safe ordering — and it never offloads, so the remote push stays
 * with `triggerDurableLogOffload` (EB-570). This closes the middle step
 * of the rotate → archive → offload chain: after
 * `triggerDurableLogSegmentRotation` (EB-572) seals a segment, this
 * trigger archives it, and the EB-570 trigger then uploads it — the
 * whole chain runs synchronously with no timer wait, and each step is
 * audited once. Only the root log archives; namespace child logs keep
 * their own pending segments and their own background pass. The trigger
 * is audited as `durable_log_archive_triggered` (`{ archived }` — only
 * the count, never segment names or seqs). An empty or unknown topic is
 * silently skipped, and a second trigger right after the first reports
 * an empty list. A bus with no durable log, or with no rotation
 * configured, refuses fail-closed — enable rotation live with
 * `setDurableLogSegmentRotation` (EB-566) first.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { type RemoteSegmentS3Client } from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-archive-trigger-'));

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
  bus.getSubscriptionAudit({ action: 'durable_log_archive_triggered' });

/** The bus's durable log (private field, reached the way the other durable-log revision tests reach it). */
const logOf = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

test('a rotation followed by the trigger archives synchronously: tmp gone, gz + sidecar + index land, and archived lists the segment path', () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('t', 0);
  bus.publish('t', 1);
  bus.publish('t', 2);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  // The segment is pending — no timer has had a chance to run.
  assert.equal(segmentFiles(dir, '.jsonl.tmp').length, 1);
  assert.equal(segmentFiles(dir, '.jsonl.gz').length, 0);
  const result = bus.triggerDurableLogArchive('t');
  assert.deepEqual(result, { archived: ['t.seg-1-3.jsonl.gz'] });
  // Synchronous: the pending file is gone and the archive is complete.
  assert.equal(segmentFiles(dir, '.jsonl.tmp').length, 0);
  assert.equal(segmentFiles(dir, '.jsonl.gz').length, 1);
  assert.ok(existsSync(join(dir, 't.seg-1-3.jsonl.gz')));
  assert.ok(existsSync(join(dir, 't.seg-1-3.jsonl.gz.sha256')));
  const entries = archiveIndex(dir, 't');
  assert.equal(entries.length, 1);
  assert.equal(entries[0].seqStart, 1);
  assert.equal(entries[0].seqEnd, 3);
  assert.equal(entries[0].entries, 3);
  // The archived records still replay, merged ahead of the live ones.
  bus.publish('t', 3);
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0, { includeArchived: true })
      .map((r) => r.seq),
    [1, 2, 3, 4],
  );
});

test('rotate, archive, offload runs the whole chain with no timer wait', () => {
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
  assert.deepEqual(bus.triggerDurableLogArchive('t'), {
    archived: ['t.seg-1-3.jsonl.gz'],
  });
  // The offload trigger uploads at once — no `await setTimeout(10)`
  // the way the EB-570 test needed when the archive pass was
  // background-only.
  const offloaded = bus.triggerDurableLogOffload();
  assert.equal(offloaded.uploaded, 1);
  const key = 'durable-log-offload/t.seg-1-3.jsonl.gz';
  const manifestKey = 'durable-log-offload/manifest.json';
  assert.ok(store.has(key));
  assert.ok(store.has(manifestKey));
  assert.ok(s3.puts.includes(key));
  assert.ok(s3.puts.includes(manifestKey));
});

test('no pending segments and an immediate second trigger both report an empty list', () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  assert.deepEqual(bus.triggerDurableLogArchive(), { archived: [] });
  bus.publish('t', 0);
  bus.publish('t', 1);
  assert.deepEqual(bus.triggerDurableLogArchive(), { archived: [] });
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  assert.deepEqual(bus.triggerDurableLogArchive(), {
    archived: ['t.seg-1-2.jsonl.gz'],
  });
  assert.deepEqual(bus.triggerDurableLogArchive('t'), { archived: [] });
  assert.deepEqual(bus.triggerDurableLogArchive(), { archived: [] });
  assert.equal(archiveIndex(dir, 't').length, 1);
  assert.equal(segmentFiles(dir, '.jsonl.gz').length, 1);
});

test('a topic filter archives only that topic; the other topic stays pending until its own trigger', () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('a', 0);
  bus.publish('a', 1);
  bus.publish('b', 0);
  bus.publish('b', 1);
  bus.publish('b', 2);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation(), { rotated: ['a', 'b'] });
  assert.deepEqual(bus.triggerDurableLogArchive('a'), {
    archived: ['a.seg-1-2.jsonl.gz'],
  });
  // Only `a` moved: `b` is still pending, nothing of `b` archived.
  assert.equal(segmentFiles(dir, '.jsonl.tmp').length, 1);
  assert.ok(segmentFiles(dir, '.jsonl.tmp')[0].startsWith('b.seg-'));
  assert.equal(archiveIndex(dir, 'b').length, 0);
  assert.deepEqual(bus.triggerDurableLogArchive('b'), {
    archived: ['b.seg-1-3.jsonl.gz'],
  });
  assert.equal(segmentFiles(dir, '.jsonl.tmp').length, 0);
  assert.equal(archiveIndex(dir, 'b')[0].seqEnd, 3);
});

test('an empty topic and an unknown topic are skipped silently and leave the pending segment untouched', () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('t', 0);
  bus.publish('t', 1);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  assert.deepEqual(bus.triggerDurableLogArchive(''), { archived: [] });
  assert.deepEqual(bus.triggerDurableLogArchive('nope'), { archived: [] });
  // Nothing was archived for the skipped topics, and the real pending
  // segment is still waiting.
  assert.equal(segmentFiles(dir, '.jsonl.tmp').length, 1);
  assert.equal(segmentFiles(dir, '.jsonl.gz').length, 0);
  assert.deepEqual(bus.triggerDurableLogArchive(), {
    archived: ['t.seg-1-2.jsonl.gz'],
  });
});

test('bus without a durable log refuses; a bad actor and a non-string topic refuse; no audit', () => {
  const bus = new EventBus({});
  assert.throws(
    () => bus.triggerDurableLogArchive(),
    /requires EventBusOptions.durableLogDir/,
  );
  assert.throws(() => bus.triggerDurableLogArchive('t', ''), RangeError);
  assert.throws(() => bus.triggerDurableLogArchive(42 as never), /must be a string/);
  assert.equal(audit(bus).length, 0);
});

test('bus without rotation refuses, and enabling rotation live unlocks the trigger', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('t', 0);
  bus.publish('t', 1);
  assert.throws(
    () => bus.triggerDurableLogArchive('t'),
    /durableLogSegmentRotation/,
  );
  assert.equal(audit(bus).length, 0);
  // EB-566: the stored history seeds the first segment's identity, so
  // the trigger seals and archives the real range instead of seq 0.
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 100 }, 'ops');
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  assert.deepEqual(bus.triggerDurableLogArchive('t'), {
    archived: ['t.seg-1-2.jsonl.gz'],
  });
  assert.equal(archiveIndex(dir, 't')[0].seqStart, 1);
  assert.equal(archiveIndex(dir, 't')[0].seqEnd, 2);
});

test('a namespace child log keeps its own pending segment: the root trigger never touches it, and its own background pass archives it', async () => {
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 2 },
  });
  bus.publish('t', 0);
  bus.publish('t', 1);
  const ns = bus.createNamespace('t1');
  ns.publish('orders', { id: 1 });
  ns.publish('orders', { id: 2 });
  // Both logs auto-rotated on the second append; neither background
  // pass has had a chance to run — everything below is synchronous.
  const childDir = join(dir, 'namespaces', encodeURIComponent('t1'));
  assert.equal(segmentFiles(dir, '.jsonl.tmp').length, 1);
  assert.equal(segmentFiles(childDir, '.jsonl.tmp').length, 1);
  assert.deepEqual(bus.triggerDurableLogArchive(), {
    archived: ['t.seg-1-2.jsonl.gz'],
  });
  // The root segment is archived; the child's pending segment is
  // untouched — no gz minted for it, no index entry for its topic.
  assert.equal(segmentFiles(dir, '.jsonl.tmp').length, 0);
  assert.equal(segmentFiles(childDir, '.jsonl.tmp').length, 1);
  assert.equal(segmentFiles(childDir, '.jsonl.gz').length, 0);
  // The child's own background pass still archives it on its own tick.
  await waitFor(() => segmentFiles(childDir, '.jsonl.gz').length === 1);
  assert.equal(segmentFiles(childDir, '.jsonl.tmp').length, 0);
});

test('the trigger is audited exactly: actor and archived count, never segment names or seqs', () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  bus.publish('t', 0);
  bus.publish('t', 1);
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t'), { rotated: ['t'] });
  assert.deepEqual(bus.triggerDurableLogArchive('t', 'ops-oncall'), {
    archived: ['t.seg-1-2.jsonl.gz'],
  });
  assert.equal(audit(bus).length, 1);
  assert.equal(audit(bus)[0].actor, 'ops-oncall');
  assert.deepEqual(audit(bus)[0].detail, { archived: 1 });
  // A segment name or a seq range must never reach the audit trail.
  assert.equal(JSON.stringify(audit(bus)).includes('seg-1-2'), false);
  assert.equal(JSON.stringify(audit(bus)).includes('seqStart'), false);
  // A second trigger right after the first is audited with a zero count.
  assert.deepEqual(bus.triggerDurableLogArchive('t', 'ops-oncall'), {
    archived: [],
  });
  assert.equal(audit(bus).length, 2);
  assert.deepEqual(audit(bus)[1].detail, { archived: 0 });
});
