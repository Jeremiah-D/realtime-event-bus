/**
 * Bus-level durable-log S3 offload local-retain segment count revision (EB-362).
 *
 * `setDurableLogRetainLocalSegments(retain, actor?)` retunes how many newest
 * archived segments per topic stay local after upload — without restarting
 * the bus. Tightening trims local segments to the new bound at once
 * (least-recently-used first, only segments the remote manifest confirms
 * as uploaded); loosening governs from the next offload pass. A count of 0
 * keeps nothing local; `Infinity` restores full retention (the construction
 * default). The revision applies to the root log and skips namespace child
 * logs, which do not offload. A bus without `durableLogDir`, or constructed
 * without `durableLogSegmentOffload`, refuses fail-closed (offload cannot be
 * enabled after the fact); illegal values fail closed with the live count
 * untouched; an identical revision is a no-op with no audit. Audited as
 * `durable_log_retain_local_segments_revised` (old and new counts, `Infinity`
 * recorded as `null`); the live count is visible in
 * `getStats().durableLog.retainLocalSegments`.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DurableTopicLog } from '../src/durablelog.ts';
import { EventBus } from '../src/bus.ts';
import type { RemoteSegmentS3Client } from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-retain-local-seg-rev-'));

/** In-memory `RemoteSegmentS3Client`. */
class CountingMemoryS3 implements RemoteSegmentS3Client {
  readonly objects = new Map<string, Uint8Array>();

  putObject(key: string, body: Uint8Array): void {
    this.objects.set(key, Uint8Array.from(body));
  }

  getObject(key: string): Uint8Array | undefined {
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
}

/**
 * A dry-run client whose `getObject` answers with a canned manifest
 * confirming both segments as remote — so a trim, if one ran, would
 * delete their local copies. `isDryRun` must prevent that.
 */
class ManifestDryRunS3 implements RemoteSegmentS3Client {
  readonly isDryRun = true;
  readonly keys: string[] = [];

  putObject(key: string, _body: Uint8Array): void {
    this.keys.push(key);
  }

  getObject(_key: string): Uint8Array | undefined {
    const sha = '0'.repeat(64);
    const manifest = {
      v: 1,
      segments: [
        {
          v: 1,
          topic: 't',
          segmentId: '1-5',
          key: 'durable-log-offload/t.seg-1-5.jsonl.gz',
          seqStart: 1,
          seqEnd: 5,
          sha256: sha,
          entries: 5,
          bytes: 1,
        },
        {
          v: 1,
          topic: 't',
          segmentId: '6-10',
          key: 'durable-log-offload/t.seg-6-10.jsonl.gz',
          seqStart: 6,
          seqEnd: 10,
          sha256: sha,
          entries: 5,
          bytes: 1,
        },
      ],
    };
    return Buffer.from(JSON.stringify(manifest), 'utf8');
  }

  headObject(_key: string): boolean {
    return false;
  }

  listObjects(_prefix: string): string[] {
    return [];
  }

  deleteObject(_key: string): void {}
}

/** Delegates to a `CountingMemoryS3` but throws from `getObject` while flagged. */
class FailingManifestS3 implements RemoteSegmentS3Client {
  failReads = false;
  private readonly inner: CountingMemoryS3;

  constructor(inner: CountingMemoryS3) {
    this.inner = inner;
  }

  putObject(key: string, body: Uint8Array): void {
    this.inner.putObject(key, body);
  }

  getObject(key: string): Uint8Array | undefined {
    if (this.failReads) throw new Error('manifest transport down');
    return this.inner.getObject(key);
  }

  headObject(key: string): boolean {
    return this.inner.headObject(key);
  }

  listObjects(prefix: string): string[] {
    return this.inner.listObjects(prefix);
  }

  deleteObject(key: string): void {
    this.inner.deleteObject(key);
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

const hasSegment = (dir: string, segId: string) =>
  segmentFiles(dir, '.jsonl.gz').some((f) => f.includes(`.seg-${segId}.`));

/** Appends 5 records and waits for the archive pass to seal the segment. */
async function archiveSegment(dir: string, log: DurableTopicLog, fromSeq: number): Promise<void> {
  for (let i = 0; i < 5; i += 1) {
    const seq = fromSeq + i;
    log.append({ seq, topic: 't', at: 1000 + seq, payload: seq });
  }
  const prefix = `${encodeURIComponent('t')}.seg-`;
  await waitFor(
    () => segmentFiles(dir, '.jsonl.tmp').filter((f) => f.startsWith(prefix)).length === 0,
  );
}

/** Archives one segment and runs an offload pass over it. */
async function archiveAndOffload(dir: string, log: DurableTopicLog, fromSeq: number): Promise<void> {
  await archiveSegment(dir, log, fromSeq);
  log.offloadNow();
}

/** Opens a log with two offloaded segments (1-5, 6-10) at the given retain count. */
async function twoSegmentLog(
  s3: RemoteSegmentS3Client,
  retainLocalSegments: number,
): Promise<{ dir: string; log: DurableTopicLog }> {
  const dir = freshDir();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3, retainLocalSegments },
  });
  await archiveAndOffload(dir, log, 1);
  await archiveAndOffload(dir, log, 6);
  return { dir, log };
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_retain_local_segments_revised' });

/** The bus's live retain count — read off the root log itself. */
const liveRetain = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog.currentRetainLocalSegments();

/** A bus with S3 offload enabled on its durable log. */
function offloadBus(
  mem: CountingMemoryS3,
  extra: { retainLocalSegments?: number } = {},
): EventBus {
  return new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    durableLogSegmentOffload: { s3: mem, ...extra },
  });
}

test('retuneRetainLocalSegments refuses a log without offload; currentRetainLocalSegments is undefined there', () => {
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
  });
  assert.equal(log.currentRetainLocalSegments(), undefined);
  assert.throws(
    () => log.retuneRetainLocalSegments(1),
    /requires segmentOffload to be enabled/,
  );
  assert.equal(log.currentRetainLocalSegments(), undefined);
});

test('retuneRetainLocalSegments validates and leaves the live count untouched on illegal values', () => {
  const mem = new CountingMemoryS3();
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 2 },
  });
  assert.equal(log.currentRetainLocalSegments(), 2);
  for (const bad of [-1, 1.5, Number.NaN, '4', -Infinity, null, undefined, {}]) {
    assert.throws(() => log.retuneRetainLocalSegments(bad as never), RangeError);
  }
  assert.equal(log.currentRetainLocalSegments(), 2);
});

test('tightening trims immediately to the new bound and keeps segments the manifest does not confirm', async () => {
  const mem = new CountingMemoryS3();
  const { dir, log } = await twoSegmentLog(mem, Infinity);
  assert.equal(hasSegment(dir, '1-5'), true);
  assert.equal(hasSegment(dir, '6-10'), true);
  // Simulate a segment the manifest never recorded: drop 6-10 from the
  // remote manifest (its bytes are remote, but unconfirmed).
  const manifestKey = 'durable-log-offload/manifest.json';
  const parsed = JSON.parse(
    Buffer.from(mem.objects.get(manifestKey)!).toString('utf8'),
  ) as { v: number; segments: Array<{ segmentId: string }> };
  parsed.segments = parsed.segments.filter((s) => s.segmentId !== '6-10');
  mem.objects.set(manifestKey, Buffer.from(JSON.stringify(parsed), 'utf8'));
  // Tighten to one segment: the oldest confirmed segment (1-5) is
  // deleted at once; the unconfirmed 6-10 stays local.
  assert.equal(log.retuneRetainLocalSegments(1), Infinity);
  assert.equal(log.currentRetainLocalSegments(), 1);
  assert.equal(hasSegment(dir, '1-5'), false);
  assert.equal(hasSegment(dir, '6-10'), true);
});

test('loosening keeps old local segments until the next offload pass', async () => {
  const mem = new CountingMemoryS3();
  const { dir, log } = await twoSegmentLog(mem, 1);
  // Retain 1 trimmed 1-5 at the second pass; 6-10 stays local.
  assert.equal(hasSegment(dir, '1-5'), false);
  assert.equal(hasSegment(dir, '6-10'), true);
  log.retuneRetainLocalSegments(2);
  assert.equal(log.currentRetainLocalSegments(), 2);
  assert.equal(hasSegment(dir, '6-10'), true);
  // The next offload pass governs with the loosened bound: the third
  // segment archives and 6-10 is still local afterwards.
  await archiveAndOffload(dir, log, 11);
  assert.equal(hasSegment(dir, '6-10'), true);
  assert.equal(hasSegment(dir, '11-15'), true);
});

test('retain 0 deletes every manifest-confirmed local segment at once', async () => {
  const mem = new CountingMemoryS3();
  const { dir, log } = await twoSegmentLog(mem, Infinity);
  assert.equal(hasSegment(dir, '1-5'), true);
  assert.equal(hasSegment(dir, '6-10'), true);
  assert.equal(log.retuneRetainLocalSegments(0), Infinity);
  assert.equal(hasSegment(dir, '1-5'), false);
  assert.equal(hasSegment(dir, '6-10'), false);
  // Reads still work — trimmed segments come back from the remote.
  assert.deepEqual(
    log.readSince('t', 0, { includeArchived: true }).map((r) => r.seq),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
});

test('Infinity restores full retention from the next offload pass', async () => {
  const mem = new CountingMemoryS3();
  const { dir, log } = await twoSegmentLog(mem, 0);
  assert.equal(hasSegment(dir, '1-5'), false);
  assert.equal(hasSegment(dir, '6-10'), false);
  log.retuneRetainLocalSegments(Infinity);
  assert.equal(log.currentRetainLocalSegments(), Infinity);
  await archiveAndOffload(dir, log, 11);
  assert.equal(hasSegment(dir, '11-15'), true);
});

test('a dry-run S3 client never trims, even when the manifest confirms the segments', async () => {
  const s3 = new ManifestDryRunS3();
  const { dir, log } = await twoSegmentLog(s3, Infinity);
  assert.equal(hasSegment(dir, '1-5'), true);
  assert.equal(hasSegment(dir, '6-10'), true);
  assert.equal(log.retuneRetainLocalSegments(0), Infinity);
  assert.equal(log.currentRetainLocalSegments(), 0);
  // The value moved, but no local segment was deleted.
  assert.equal(hasSegment(dir, '1-5'), true);
  assert.equal(hasSegment(dir, '6-10'), true);
  assert.equal(log.stats().offloadFailed, 0);
});

test('an unreadable manifest defers the trim to the next offload pass and counts offloadFailed', async () => {
  const mem = new CountingMemoryS3();
  const s3 = new FailingManifestS3(mem);
  const { dir, log } = await twoSegmentLog(s3, Infinity);
  assert.equal(hasSegment(dir, '1-5'), true);
  assert.equal(hasSegment(dir, '6-10'), true);
  const failedBefore = log.stats().offloadFailed;
  s3.failReads = true;
  assert.equal(log.retuneRetainLocalSegments(1), Infinity);
  // The value moved, but the trim deferred: both segments stay local.
  assert.equal(log.currentRetainLocalSegments(), 1);
  assert.equal(hasSegment(dir, '1-5'), true);
  assert.equal(hasSegment(dir, '6-10'), true);
  assert.equal(log.stats().offloadFailed, failedBefore + 1);
  // The next pass reads the manifest again and trims to the new bound.
  s3.failReads = false;
  log.offloadNow();
  assert.equal(hasSegment(dir, '1-5'), false);
  assert.equal(hasSegment(dir, '6-10'), true);
});

test('setDurableLogRetainLocalSegments refuses a bus without durableLogDir or without offload', () => {
  const mem = new CountingMemoryS3();
  // No durable log at all.
  const noLog = new EventBus({});
  assert.throws(
    () => noLog.setDurableLogRetainLocalSegments(1),
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
    () => noOffload.setDurableLogRetainLocalSegments(1),
    /requires EventBusOptions\.durableLogSegmentOffload to be set/,
  );
  assert.deepEqual(audit(noOffload), []);
  noOffload.close();
});

test('setDurableLogRetainLocalSegments: same-value revision is a no-op with no audit', () => {
  const mem = new CountingMemoryS3();
  const bus = offloadBus(mem, { retainLocalSegments: 2 });
  assert.equal(liveRetain(bus), 2);
  bus.setDurableLogRetainLocalSegments(2);
  assert.deepEqual(audit(bus), []);
  assert.equal(liveRetain(bus), 2);
  bus.close();
});

test('setDurableLogRetainLocalSegments: illegal values leave the live count and audit untouched', () => {
  const mem = new CountingMemoryS3();
  const bus = offloadBus(mem, { retainLocalSegments: 2 });
  for (const bad of [-1, 1.5, Number.NaN, '4', -Infinity]) {
    assert.throws(() => bus.setDurableLogRetainLocalSegments(bad as never), RangeError);
  }
  assert.equal(liveRetain(bus), 2);
  assert.deepEqual(audit(bus), []);
  assert.equal(bus.getStats().durableLog.retainLocalSegments, 2);
  bus.close();
});

test('setDurableLogRetainLocalSegments: audit detail is exact with null for Infinity, and getStats exposes the live count', () => {
  const mem = new CountingMemoryS3();
  const bus = offloadBus(mem);
  assert.equal(bus.getStats().durableLog.retainLocalSegments, Infinity);
  bus.setDurableLogRetainLocalSegments(2, 'ops-lead');
  const events = audit(bus);
  assert.equal(events.length, 1);
  assert.equal(events[0].actor, 'ops-lead');
  assert.deepEqual(events[0].detail, {
    retainLocalSegments: 2,
    oldRetainLocalSegments: null,
  });
  assert.equal(liveRetain(bus), 2);
  assert.equal(bus.getStats().durableLog.retainLocalSegments, 2);
  // Revising back to Infinity records null for the new value.
  bus.setDurableLogRetainLocalSegments(Infinity);
  assert.deepEqual(audit(bus)[1].detail, {
    retainLocalSegments: null,
    oldRetainLocalSegments: 2,
  });
  assert.equal(bus.getStats().durableLog.retainLocalSegments, Infinity);
  bus.close();
});

test('setDurableLogRetainLocalSegments skips namespace child logs, which do not offload', () => {
  const mem = new CountingMemoryS3();
  const bus = offloadBus(mem);
  bus.createNamespace('t1');
  const child = (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> }).namespaceLogs.get(
    't1',
  )!;
  assert.equal(child.currentRetainLocalSegments(), undefined);
  bus.setDurableLogRetainLocalSegments(2);
  assert.equal(liveRetain(bus), 2);
  assert.equal(child.currentRetainLocalSegments(), undefined);
  assert.equal(audit(bus).length, 1);
  // A namespace registered after the revision likewise opens without offload.
  bus.createNamespace('t2');
  const later = (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> }).namespaceLogs.get(
    't2',
  )!;
  assert.equal(later.currentRetainLocalSegments(), undefined);
  bus.close();
});

test('getStats().durableLog omits retainLocalSegments when offload is disabled', () => {
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
  });
  const stats = bus.getStats().durableLog as { retainLocalSegments?: number };
  assert.equal(stats.retainLocalSegments, undefined);
  bus.close();
});
