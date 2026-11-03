/**
 * Bus-level durable-log S3 readback-cache bounds revision (EB-361).
 *
 * `setDurableLogReadbackCache(patch, actor?)` retunes the local LRU
 * cache's segment-count and downloaded-byte bounds live — without
 * restarting the bus. A partial revision: only the fields provided
 * move, the rest keep their live values. Only the two bounds move:
 * the S3 client, key prefix, namespace, `retainLocalSegments`, and
 * segment rotation are untouched. Tightening prunes the cache to the
 * new bounds at once (least-recently-used first, until both bounds
 * hold); loosening governs from the next insert. A bound of 0
 * disables the cache entirely (the prune empties it, later readbacks
 * download from the remote every time); a single segment larger than
 * the byte bound is never cached. The revision applies to the root
 * log and skips namespace child logs, which do not offload. A bus
 * without `durableLogDir`, or constructed without
 * `durableLogSegmentOffload`, refuses fail-closed (offload cannot be
 * enabled after the fact); illegal values fail closed with the live
 * bounds untouched; an identical revision, or a patch with neither
 * field, is a no-op with no audit. Audited as
 * `durable_log_readback_cache_revised` (old and new bounds); the live
 * bounds are visible in `getStats().durableLog`.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DurableTopicLog } from '../src/durablelog.ts';
import { EventBus } from '../src/bus.ts';
import type { RemoteSegmentS3Client } from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-readback-cache-rev-'));

const DEFAULT_SEGMENTS = 16;
const DEFAULT_BYTES = 64 * 1024 * 1024;

/** In-memory `RemoteSegmentS3Client` with per-key download counting. */
class CountingMemoryS3 implements RemoteSegmentS3Client {
  readonly objects = new Map<string, Uint8Array>();
  private readonly getObjectCalls = new Map<string, number>();

  putObject(key: string, body: Uint8Array): void {
    this.objects.set(key, Uint8Array.from(body));
  }

  getObject(key: string): Uint8Array | undefined {
    this.getObjectCalls.set(key, (this.getObjectCalls.get(key) ?? 0) + 1);
    const body = this.objects.get(key);
    return body === undefined ? undefined : Uint8Array.from(body);
  }

  headObject(key: string): boolean {
    return this.objects.has(key);
  }

  listObjects(prefix: string): string[] {
    return [...this.objects.keys()].filter((k) => k.startsWith(prefix)).sort();
  }

  deleteObject(key: string): void {
    this.objects.delete(key);
  }

  /** Downloads served for `key` so far. */
  gets(key: string): number {
    return this.getObjectCalls.get(key) ?? 0;
  }
}

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

/**
 * Appends 5 records (`seq` from `fromSeq`, payloads `payloadBase + seq`),
 * waits for the archive pass, then runs `offloadNow()` synchronously and
 * waits for `trimmedSegId`'s local `.gz` to be gone (when given).
 */
async function archiveAndOffload(
  dir: string,
  log: DurableTopicLog,
  fromSeq: number,
  payloadBase: number,
  trimmedSegId?: string,
): Promise<void> {
  for (let i = 0; i < 5; i += 1) {
    const seq = fromSeq + i;
    log.append({ seq, topic: 't', at: 1000 + seq, payload: payloadBase + seq });
  }
  const prefix = `${encodeURIComponent('t')}.seg-`;
  await waitFor(
    () => segmentFiles(dir, '.jsonl.tmp').filter((f) => f.startsWith(prefix)).length === 0,
  );
  log.offloadNow();
  if (trimmedSegId !== undefined) {
    await waitFor(
      () => !segmentFiles(dir, '.jsonl.gz').some((f) => f.includes(`.seg-${trimmedSegId}.`)),
    );
  }
}

/** Opens a log with two offloaded, locally-trimmed segments (1-5, 6-10). */
async function twoSegmentLog(
  mem: CountingMemoryS3,
  extra: { readbackCacheMaxSegments?: number; readbackCacheMaxBytes?: number } = {},
): Promise<{ dir: string; log: DurableTopicLog }> {
  const dir = freshDir();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0, ...extra },
  });
  await archiveAndOffload(dir, log, 1, 0, '1-5');
  await archiveAndOffload(dir, log, 6, 0, '6-10');
  return { dir, log };
}

/** The two remote keys for a two-segment log. */
const key1 = 'durable-log-offload/t.seg-1-5.jsonl.gz';
const key2 = 'durable-log-offload/t.seg-6-10.jsonl.gz';

/** Pulls both segments into the readback cache (a full archived replay). */
function pullBoth(log: DurableTopicLog): void {
  log.readSince('t', 0, { includeArchived: true });
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_readback_cache_revised' });

/** The bus's live readback-cache bounds — read off the root log itself. */
const liveLimits = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog.currentReadbackCacheLimits();

/** A bus with S3 offload enabled on its durable log. */
function offloadBus(mem: CountingMemoryS3, dir?: string): EventBus {
  return new EventBus({
    durableLogDir: dir ?? freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    durableLogSegmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
}

test('retuneReadbackCache refuses a log without offload; currentReadbackCacheLimits is undefined there', () => {
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
  });
  assert.equal(log.currentReadbackCacheLimits(), undefined);
  assert.throws(
    () => log.retuneReadbackCache({ readbackCacheMaxSegments: 1 }),
    /requires segmentOffload to be enabled/,
  );
  assert.equal(log.currentReadbackCacheLimits(), undefined);
});

test('retuneReadbackCache validates merged bounds and leaves live bounds untouched on illegal values', () => {
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
  assert.deepEqual(log.currentReadbackCacheLimits(), {
    readbackCacheMaxSegments: DEFAULT_SEGMENTS,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  for (const bad of [
    { readbackCacheMaxSegments: -1 },
    { readbackCacheMaxSegments: 1.5 },
    { readbackCacheMaxSegments: Number.NaN },
    { readbackCacheMaxSegments: '4' },
    { readbackCacheMaxBytes: -1 },
    { readbackCacheMaxBytes: 2.5 },
    { readbackCacheMaxBytes: Number.NaN },
    { readbackCacheMaxBytes: '1024' },
  ]) {
    assert.throws(() => log.retuneReadbackCache(bad as never), RangeError);
  }
  // The failed calls changed nothing.
  assert.deepEqual(log.currentReadbackCacheLimits(), {
    readbackCacheMaxSegments: DEFAULT_SEGMENTS,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
});

test('retuneReadbackCache is a partial revision and returns the previous bounds', () => {
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
  assert.deepEqual(log.retuneReadbackCache({ readbackCacheMaxSegments: 4 }), {
    readbackCacheMaxSegments: DEFAULT_SEGMENTS,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  assert.deepEqual(log.currentReadbackCacheLimits(), {
    readbackCacheMaxSegments: 4,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  // A patch with neither field is a no-op returning the current bounds.
  assert.deepEqual(log.retuneReadbackCache({}), {
    readbackCacheMaxSegments: 4,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  assert.deepEqual(log.retuneReadbackCache({ readbackCacheMaxBytes: 1024 }), {
    readbackCacheMaxSegments: 4,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  assert.deepEqual(log.currentReadbackCacheLimits(), {
    readbackCacheMaxSegments: 4,
    readbackCacheMaxBytes: 1024,
  });
});

test('tightening the segment bound prunes the cache at once — the evicted segment re-downloads', async () => {
  const mem = new CountingMemoryS3();
  const { log } = await twoSegmentLog(mem);
  pullBoth(log);
  assert.equal(mem.gets(key1), 1);
  assert.equal(mem.gets(key2), 1);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
  // Tighten to one segment: the least-recently-used (1-5) is evicted now,
  // not on the next insert.
  log.retuneReadbackCache({ readbackCacheMaxSegments: 1 });
  // The surviving tail (6-10) still hits the cache.
  const tail = log.readSince('t', 5, { includeArchived: true });
  assert.deepEqual(
    tail.map((r) => r.seq),
    [6, 7, 8, 9, 10],
  );
  assert.equal(mem.gets(key1), 1);
  assert.equal(mem.gets(key2), 1);
  assert.equal(log.stats().s3ReadbackCacheHits, 1);
  // The evicted head re-downloads on a full replay.
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 2);
  assert.equal(mem.gets(key2), 2);
});

test('tightening the byte bound prunes the cache at once', async () => {
  const mem = new CountingMemoryS3();
  const { log } = await twoSegmentLog(mem);
  pullBoth(log);
  assert.equal(mem.gets(key1), 1);
  assert.equal(mem.gets(key2), 1);
  // The segment bound alone would allow both (16); only the byte bound
  // can evict here — 1 byte holds no segment at all.
  log.retuneReadbackCache({ readbackCacheMaxBytes: 1 });
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 2);
  assert.equal(mem.gets(key2), 2);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
});

test('loosening the segment bound governs from the next insert', async () => {
  const mem = new CountingMemoryS3();
  const { log } = await twoSegmentLog(mem, { readbackCacheMaxSegments: 1 });
  // Under the tight bound only the newest segment survives the pull.
  pullBoth(log);
  assert.equal(mem.gets(key1), 1);
  assert.equal(mem.gets(key2), 1);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
  log.retuneReadbackCache({ readbackCacheMaxSegments: 2 });
  // The next insert (the re-downloaded head) now fits alongside the tail.
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 2);
  assert.equal(mem.gets(key2), 1);
  assert.equal(log.stats().s3ReadbackCacheHits, 1);
  // Both segments are cached now: a further replay downloads nothing.
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 2);
  assert.equal(mem.gets(key2), 1);
  assert.equal(log.stats().s3ReadbackCacheHits, 3);
});

test('a bound of 0 disables the cache and empties it at once', async () => {
  const mem = new CountingMemoryS3();
  const { log } = await twoSegmentLog(mem);
  pullBoth(log);
  assert.equal(mem.gets(key1), 1);
  assert.equal(mem.gets(key2), 1);
  log.retuneReadbackCache({ readbackCacheMaxSegments: 0 });
  // The prune emptied the cache: the next replay downloads both, and
  // the 0 bound refuses every later insert too.
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 2);
  assert.equal(mem.gets(key2), 2);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 3);
  assert.equal(mem.gets(key2), 3);
  // The byte bound disables the cache the same way.
  log.retuneReadbackCache({ readbackCacheMaxSegments: DEFAULT_SEGMENTS, readbackCacheMaxBytes: 0 });
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 4);
  assert.equal(mem.gets(key2), 4);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
  // Restoring the bounds re-enables caching from the next pull.
  log.retuneReadbackCache({ readbackCacheMaxBytes: DEFAULT_BYTES });
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 5);
  assert.equal(mem.gets(key2), 5);
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 5);
  assert.equal(mem.gets(key2), 5);
  assert.equal(log.stats().s3ReadbackCacheHits, 2);
});

test('a single segment larger than the byte bound is never cached', async () => {
  const mem = new CountingMemoryS3();
  const { log } = await twoSegmentLog(mem);
  pullBoth(log);
  assert.equal(mem.gets(key1), 1);
  assert.equal(mem.gets(key2), 1);
  // Shrink the byte budget below any single segment: the prune empties
  // the cache and no later pull is retained.
  log.retuneReadbackCache({ readbackCacheMaxBytes: 1 });
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 2);
  assert.equal(mem.gets(key2), 2);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 3);
  assert.equal(mem.gets(key2), 3);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
});

test('setDurableLogReadbackCache refuses a bus without durableLogDir or without offload', () => {
  const mem = new CountingMemoryS3();
  // No durable log at all.
  const noLog = new EventBus({});
  assert.throws(
    () => noLog.setDurableLogReadbackCache({ readbackCacheMaxSegments: 1 }),
    /requires EventBusOptions\.durableLogDir to be set/,
  );
  assert.deepEqual(audit(noLog), []);
  noLog.close();
  // A durable log, but offload never enabled.
  const noOffload = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
  });
  assert.throws(
    () => noOffload.setDurableLogReadbackCache({ readbackCacheMaxSegments: 1 }),
    /requires EventBusOptions\.durableLogSegmentOffload to be set/,
  );
  assert.deepEqual(audit(noOffload), []);
  noOffload.close();
  // Construction-time gates: offload needs a dir and rotation.
  assert.throws(
    () =>
      new EventBus({
        durableLogSegmentOffload: { s3: mem, retainLocalSegments: 0 },
      }),
    /requires EventBusOptions\.durableLogDir to be set/,
  );
  assert.throws(
    () =>
      new EventBus({
        durableLogDir: freshDir(),
        durableLogSegmentOffload: { s3: mem, retainLocalSegments: 0 },
      }),
    /requires segmentRotation/,
  );
});

test('setDurableLogReadbackCache: same-value and empty revisions are no-ops with no audit', () => {
  const mem = new CountingMemoryS3();
  const bus = offloadBus(mem);
  assert.deepEqual(liveLimits(bus), {
    readbackCacheMaxSegments: DEFAULT_SEGMENTS,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  bus.setDurableLogReadbackCache({ readbackCacheMaxSegments: 16 });
  bus.setDurableLogReadbackCache({});
  bus.setDurableLogReadbackCache({ readbackCacheMaxSegments: 16, readbackCacheMaxBytes: DEFAULT_BYTES });
  assert.deepEqual(audit(bus), []);
  assert.deepEqual(liveLimits(bus), {
    readbackCacheMaxSegments: DEFAULT_SEGMENTS,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  bus.close();
});

test('setDurableLogReadbackCache: audit detail is exact and getStats exposes the live bounds', () => {
  const mem = new CountingMemoryS3();
  const bus = offloadBus(mem);
  const statsBefore = bus.getStats().durableLog as { readbackCacheMaxSegments?: number; readbackCacheMaxBytes?: number };
  assert.equal(statsBefore.readbackCacheMaxSegments, DEFAULT_SEGMENTS);
  assert.equal(statsBefore.readbackCacheMaxBytes, DEFAULT_BYTES);
  bus.setDurableLogReadbackCache({ readbackCacheMaxSegments: 4 }, 'ops-lead');
  const events = audit(bus);
  assert.equal(events.length, 1);
  assert.equal(events[0].actor, 'ops-lead');
  assert.deepEqual(events[0].detail, {
    readbackCacheMaxSegments: 4,
    oldReadbackCacheMaxSegments: DEFAULT_SEGMENTS,
    readbackCacheMaxBytes: DEFAULT_BYTES,
    oldReadbackCacheMaxBytes: DEFAULT_BYTES,
  });
  // A partial revision moves only the provided field.
  assert.deepEqual(liveLimits(bus), {
    readbackCacheMaxSegments: 4,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  const statsAfter = bus.getStats().durableLog as { readbackCacheMaxSegments?: number; readbackCacheMaxBytes?: number };
  assert.equal(statsAfter.readbackCacheMaxSegments, 4);
  assert.equal(statsAfter.readbackCacheMaxBytes, DEFAULT_BYTES);
  bus.close();
});

test('setDurableLogReadbackCache: an illegal bound leaves live bounds and audit untouched', () => {
  const mem = new CountingMemoryS3();
  const bus = offloadBus(mem);
  for (const bad of [
    { readbackCacheMaxSegments: -1 },
    { readbackCacheMaxSegments: 1.5 },
    { readbackCacheMaxBytes: -1 },
    { readbackCacheMaxBytes: 2.5 },
    { readbackCacheMaxSegments: '4' },
  ]) {
    assert.throws(() => bus.setDurableLogReadbackCache(bad as never), RangeError);
  }
  assert.deepEqual(liveLimits(bus), {
    readbackCacheMaxSegments: DEFAULT_SEGMENTS,
    readbackCacheMaxBytes: DEFAULT_BYTES,
  });
  assert.deepEqual(audit(bus), []);
  const stats = bus.getStats().durableLog as { readbackCacheMaxSegments?: number; readbackCacheMaxBytes?: number };
  assert.equal(stats.readbackCacheMaxSegments, DEFAULT_SEGMENTS);
  assert.equal(stats.readbackCacheMaxBytes, DEFAULT_BYTES);
  bus.close();
});

test('setDurableLogReadbackCache skips namespace child logs, which do not offload', () => {
  const mem = new CountingMemoryS3();
  const bus = offloadBus(mem);
  bus.createNamespace('t1');
  const child = (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> }).namespaceLogs.get(
    't1',
  )!;
  assert.equal(child.currentReadbackCacheLimits(), undefined);
  bus.setDurableLogReadbackCache({ readbackCacheMaxSegments: 2, readbackCacheMaxBytes: 4096 });
  assert.deepEqual(liveLimits(bus), { readbackCacheMaxSegments: 2, readbackCacheMaxBytes: 4096 });
  assert.equal(child.currentReadbackCacheLimits(), undefined);
  assert.equal(audit(bus).length, 1);
  // A namespace registered after the revision likewise opens without offload.
  bus.createNamespace('t2');
  const later = (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> }).namespaceLogs.get(
    't2',
  )!;
  assert.equal(later.currentReadbackCacheLimits(), undefined);
  bus.close();
});

test('getStats().durableLog omits the cache bounds when offload is disabled', () => {
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
  });
  const stats = bus.getStats().durableLog as {
    readbackCacheMaxSegments?: number;
    readbackCacheMaxBytes?: number;
  };
  assert.equal(stats.readbackCacheMaxSegments, undefined);
  assert.equal(stats.readbackCacheMaxBytes, undefined);
  bus.close();
});
