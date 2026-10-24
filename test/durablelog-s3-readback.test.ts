/**
 * EB-218 — durable-log S3 offload readback: local LRU cache + readback
 * metrics.
 *
 * The remote pull itself (EB-163) is covered in
 * `test/durablelog-s3-offload.test.ts`. These tests pin what EB-218
 * adds on top: repeated replays of a locally-trimmed segment are
 * served from a local LRU cache instead of re-downloading, readback
 * volume is counted (`s3Readbacks` / `s3ReadbackBytes` /
 * `s3ReadbackCacheHits`), a failed pull is never cached, `purgeTopic`
 * evicts the topic from the cache, and the counters render as
 * Prometheus series.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DurableTopicLog } from '../src/durablelog.ts';
import { EventBus, type BusStats } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import type { RemoteSegmentS3Client } from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-readback-'));

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

test('readback: a pulled segment is cached — the second replay hits memory, not S3', async () => {
  const dir = freshDir();
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 1 },
  });
  await archiveAndOffload(dir, log, 1, 0);
  await archiveAndOffload(dir, log, 6, 0, '1-5');
  const key = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  // Segment 1-5 is trimmed locally; 6-10 stays. The replay must pull 1-5.
  assert.ok(!segmentFiles(dir, '.jsonl.gz').some((f) => f.includes('.seg-1-5.')));
  const first = log.readSince('t', 0, { includeArchived: true });
  assert.deepEqual(
    first.map((r) => r.payload),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
  assert.equal(mem.gets(key), 1);
  assert.equal(log.stats().s3Readbacks, 1);
  assert.ok(log.stats().s3ReadbackBytes > 0);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
  // The second replay is served from the local cache: no new download,
  // identical records.
  const second = log.readSince('t', 0, { includeArchived: true });
  assert.deepEqual(
    second.map((r) => r.payload),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
  assert.equal(mem.gets(key), 1);
  assert.equal(log.stats().s3Readbacks, 1);
  assert.equal(log.stats().s3ReadbackCacheHits, 1);
});

test('readback: LRU eviction by segment count re-downloads the evicted segment', async () => {
  const dir = freshDir();
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0, readbackCacheMaxSegments: 1 },
  });
  await archiveAndOffload(dir, log, 1, 0, '1-5');
  await archiveAndOffload(dir, log, 6, 0, '6-10');
  const key1 = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  const key2 = 'durable-log-offload/t.seg-6-10.jsonl.gz';
  // Full replay pulls both; segment 1-5 is evicted when 6-10 is cached.
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 1);
  assert.equal(mem.gets(key2), 1);
  assert.equal(log.stats().s3Readbacks, 2);
  // A cursor past segment 1-5 only needs the cached tail: no download.
  const tail = log.readSince('t', 5, { includeArchived: true });
  assert.deepEqual(
    tail.map((r) => r.seq),
    [6, 7, 8, 9, 10],
  );
  assert.equal(mem.gets(key1), 1);
  assert.equal(mem.gets(key2), 1);
  assert.equal(log.stats().s3ReadbackCacheHits, 1);
  // Segment 1-5 was evicted — a full replay downloads it again.
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key1), 2);
  assert.equal(mem.gets(key2), 2);
  assert.equal(log.stats().s3Readbacks, 4);
  assert.equal(log.stats().s3ReadbackCacheHits, 1);
});

test('readback: a zero byte budget disables the cache — every replay downloads', async () => {
  const dir = freshDir();
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0, readbackCacheMaxBytes: 0 },
  });
  await archiveAndOffload(dir, log, 1, 0, '1-5');
  const key = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  log.readSince('t', 0, { includeArchived: true });
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(mem.gets(key), 2);
  assert.equal(log.stats().s3Readbacks, 2);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
});

test('readback: cache bounds are validated', () => {
  const mem = new CountingMemoryS3();
  const base = { segmentRotation: { maxEntriesPerSegment: 5 } };
  assert.throws(
    () =>
      DurableTopicLog.open({
        dir: freshDir(),
        ...base,
        segmentOffload: { s3: mem, readbackCacheMaxSegments: -1 },
      }),
    /readbackCacheMaxSegments must be a non-negative integer/,
  );
  assert.throws(
    () =>
      DurableTopicLog.open({
        dir: freshDir(),
        ...base,
        segmentOffload: { s3: mem, readbackCacheMaxSegments: 1.5 },
      }),
    /readbackCacheMaxSegments must be a non-negative integer/,
  );
  assert.throws(
    () =>
      DurableTopicLog.open({
        dir: freshDir(),
        ...base,
        segmentOffload: { s3: mem, readbackCacheMaxBytes: -1 },
      }),
    /readbackCacheMaxBytes must be a non-negative integer/,
  );
  assert.throws(
    () =>
      DurableTopicLog.open({
        dir: freshDir(),
        ...base,
        segmentOffload: { s3: mem, readbackCacheMaxBytes: 2.5 },
      }),
    /readbackCacheMaxBytes must be a non-negative integer/,
  );
});

test('readback: a failed pull is never cached — the next attempt retries the remote', async () => {
  const dir = freshDir();
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
  await archiveAndOffload(dir, log, 1, 0, '1-5');
  const key = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  const good = mem.objects.get(key);
  assert.ok(good !== undefined);
  mem.objects.set(key, Buffer.from('tampered-by-an-attacker'));
  assert.throws(
    () => log.readSince('t', 0, { includeArchived: true }),
    /hash mismatch.*refusing to truncate replay/,
  );
  // The failed pull counted nothing and cached nothing.
  assert.equal(log.stats().s3Readbacks, 0);
  assert.equal(log.stats().s3ReadbackCacheHits, 0);
  // Once the remote is healthy again the pull succeeds and is cached.
  mem.objects.set(key, good);
  const records = log.readSince('t', 0, { includeArchived: true });
  assert.deepEqual(
    records.map((r) => r.payload),
    [1, 2, 3, 4, 5],
  );
  assert.equal(log.stats().s3Readbacks, 1);
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(log.stats().s3ReadbackCacheHits, 1);
});

test('readback: purgeTopic evicts the purged topic from the readback cache', async () => {
  const dir = freshDir();
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
  await archiveAndOffload(dir, log, 1, 0, '1-5');
  const key = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  log.readSince('t', 0, { includeArchived: true }); // pulls and caches
  assert.equal(mem.gets(key), 1);
  log.readSince('t', 0, { includeArchived: true }); // cache hit
  assert.equal(mem.gets(key), 1);
  assert.equal(log.stats().s3ReadbackCacheHits, 1);
  log.purgeTopic('t');
  // The topic is re-provisioned with new payloads; the remote still
  // holds the pre-purge bytes (purgeTopic never deletes remote objects)
  // and the manifest still lists them, so the replay pulls the remote
  // bytes again instead of serving the purged cache entry.
  await archiveAndOffload(dir, log, 1, 100, '1-5');
  const records = log.readSince('t', 0, { includeArchived: true });
  assert.deepEqual(
    records.map((r) => r.payload),
    [1, 2, 3, 4, 5],
  );
  assert.equal(mem.gets(key), 2);
});

test('readback: getStats and Prometheus exposition carry the readback counters', async () => {
  const dir = freshDir();
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
  await archiveAndOffload(dir, log, 1, 0, '1-5');
  log.readSince('t', 0, { includeArchived: true });
  const stats = log.stats();
  assert.equal(stats.s3Readbacks, 1);
  const bytes = stats.s3ReadbackBytes;
  assert.ok(bytes > 0);
  assert.equal(stats.s3ReadbackCacheHits, 0);
  const bus = new EventBus();
  const text = renderPrometheus({
    ...bus.getStats(),
    durableLog: { dir, ...stats },
  } as BusStats);
  const line = (name: string): string | undefined =>
    text.split('\n').find((l) => l.startsWith(`${name} `));
  assert.equal(line('durablelog_s3_readbacks_total'), 'durablelog_s3_readbacks_total 1');
  assert.equal(
    line('durablelog_s3_readback_bytes_total'),
    `durablelog_s3_readback_bytes_total ${bytes}`,
  );
  assert.equal(
    line('durablelog_s3_readback_cache_hits_total'),
    'durablelog_s3_readback_cache_hits_total 0',
  );
  // A bus that never read back renders no readback series at all.
  assert.equal(renderPrometheus(bus.getStats()).includes('durablelog_s3_readback'), false);
});
