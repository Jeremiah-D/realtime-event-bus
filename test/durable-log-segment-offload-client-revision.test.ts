/**
 * Bus-level durable-log S3 offload client replacement (EB-399).
 *
 * `setDurableLogSegmentOffloadClient(s3, actor?)` swaps the transport the
 * log uploads sealed segments with, reads the remote manifest through,
 * and pulls trimmed segments back down through — without restarting the
 * bus. Only the client moves: the key prefix, namespace,
 * `retainLocalSegments`, readback-cache bounds, and segment rotation are
 * untouched, and segments already uploaded are not re-pushed. The new
 * client governs from the next offload call. Both clients must serve the
 * same bucket and key space (the operator's precondition — modelled here
 * by two clients over one shared object store). A dry-run client
 * rehearses only: the next pass records intended keys and never uploads,
 * writes a manifest, or trims local segments. A bus without
 * `durableLogDir`, or constructed without `durableLogSegmentOffload`,
 * refuses fail-closed; an unusable client leaves the live one untouched;
 * installing the same client object is a no-op with no audit. Audited as
 * `durable_log_segment_offload_client_revised`
 * (`{ isDryRun, oldIsDryRun }`, `null` when a client carries no brand) —
 * never an endpoint or a credential — and the live brand is visible via
 * `getDurableLogSegmentOffloadClientIsDryRun()` (the client itself is
 * never returned).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { DryRunS3Client, type RemoteSegmentS3Client } from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-offload-client-rev-'));

/** In-memory `RemoteSegmentS3Client` over a shared object store, with per-client call records. */
class SharedStoreS3 implements RemoteSegmentS3Client {
  readonly puts: string[] = [];
  readonly gets: string[] = [];
  private readonly store: Map<string, Uint8Array>;

  constructor(store: Map<string, Uint8Array>) {
    this.store = store;
  }

  putObject(key: string, body: Uint8Array): void {
    this.puts.push(key);
    this.store.set(key, Uint8Array.from(body));
  }

  getObject(key: string): Uint8Array | undefined {
    this.gets.push(key);
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

/** Appends 5 records, then waits for the archive pass to seal the segment. */
async function archive(dir: string, log: DurableTopicLog, fromSeq: number): Promise<void> {
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
  await archive(dir, log, fromSeq);
  log.offloadNow();
}

/** The bus's durable log (private field, reached the way the other durable-log revision tests reach it). */
const logOf = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

/** A bus with S3 offload enabled on its durable log. */
function offloadBus(store: Map<string, Uint8Array>, s3: RemoteSegmentS3Client, dir?: string): EventBus {
  return new EventBus({
    durableLogDir: dir ?? freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    durableLogSegmentOffload: { s3, retainLocalSegments: 0 },
  });
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_segment_offload_client_revised' });

/** The two remote segment keys and the manifest key for a two-segment log. */
const key1 = 'durable-log-offload/t.seg-1-5.jsonl.gz';
const key2 = 'durable-log-offload/t.seg-6-10.jsonl.gz';
const manifestKey = 'durable-log-offload/manifest.json';

test('retuneOffloadClient refuses a log without offload; currentOffloadClientIsDryRun is undefined there', () => {
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
  });
  assert.equal(log.currentOffloadClientIsDryRun(), undefined);
  const store = new Map<string, Uint8Array>();
  assert.throws(
    () => log.retuneOffloadClient(new SharedStoreS3(store)),
    /requires segmentOffload to be enabled/,
  );
  assert.equal(log.currentOffloadClientIsDryRun(), undefined);
});

test('retuneOffloadClient validates the client shape, returns the old brand, and leaves the live client untouched', () => {
  const store = new Map<string, Uint8Array>();
  const live = new SharedStoreS3(store);
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: live, retainLocalSegments: 0 },
  });
  assert.equal(log.currentOffloadClientIsDryRun(), undefined);
  // Every malformed shape refuses — a missing method, a non-object, a string.
  // (A method-missing fixture must be a plain object: `delete` on a class
  // instance cannot remove a prototype method.)
  const missingDelete: Record<string, unknown> = {
    putObject: () => {},
    getObject: () => undefined,
    headObject: () => false,
    listObjects: () => [],
  };
  for (const bad of [{}, missingDelete, null, 's3', 42]) {
    assert.throws(() => log.retuneOffloadClient(bad as never), RangeError);
  }
  // The failed calls changed nothing: the live client is still the real one.
  assert.equal(log.currentOffloadClientIsDryRun(), undefined);

  const dry = new DryRunS3Client();
  assert.equal(log.retuneOffloadClient(dry), undefined);
  assert.equal(log.currentOffloadClientIsDryRun(), true);
  for (const bad of [{}, null]) {
    assert.throws(() => log.retuneOffloadClient(bad as never), RangeError);
  }
  assert.equal(log.currentOffloadClientIsDryRun(), true);
  // Same object: a no-op returning the current brand.
  assert.equal(log.retuneOffloadClient(dry), true);
  // Swapping back returns the old brand.
  assert.equal(log.retuneOffloadClient(live), true);
  assert.equal(log.currentOffloadClientIsDryRun(), undefined);
});

test('the next offloadNow upload walks the new client; the old client sees nothing new', async () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const b = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, a, dir);
  const log = logOf(bus);
  await archiveAndOffload(dir, log, 1);
  assert.ok(a.puts.includes(key1));
  assert.ok(a.puts.includes(manifestKey));
  assert.deepEqual(b.puts, []);

  bus.setDurableLogSegmentOffloadClient(b, 'ops-oncall');
  await archiveAndOffload(dir, log, 6);
  // The new segment uploads through B; A sees nothing new — and the old
  // segment is not re-pushed (the manifest already knows it).
  assert.ok(b.puts.includes(key2));
  assert.ok(!b.puts.includes(key1));
  assert.ok(!a.puts.includes(key2));
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), undefined);
});

test('remote readback walks the new client', async () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const b = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, a, dir);
  const log = logOf(bus);
  await archiveAndOffload(dir, log, 1);
  await archiveAndOffload(dir, log, 6);
  // Both segments are trimmed locally (retain 0) — the replay must pull.
  assert.deepEqual(segmentFiles(dir, '.jsonl.gz'), []);

  bus.setDurableLogSegmentOffloadClient(b);
  const records = log.readSince('t', 0, { includeArchived: true });
  assert.deepEqual(
    records.map((r) => r.payload),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
  assert.ok(b.gets.includes(key1));
  assert.ok(b.gets.includes(key2));
  assert.ok(!a.gets.includes(key1));
  assert.ok(!a.gets.includes(key2));
});

test('bus without a durableLogDir refuses; the getter is undefined', () => {
  const store = new Map<string, Uint8Array>();
  const bus = new EventBus({});
  assert.throws(
    () => bus.setDurableLogSegmentOffloadClient(new SharedStoreS3(store)),
    /requires EventBusOptions.durableLogDir/,
  );
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), undefined);
  assert.equal(audit(bus).length, 0);
});

test('bus without offload refuses; the getter is undefined', () => {
  const store = new Map<string, Uint8Array>();
  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(
    () => bus.setDurableLogSegmentOffloadClient(new SharedStoreS3(store)),
    /offload cannot be enabled after the fact/,
  );
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), undefined);
  assert.equal(audit(bus).length, 0);
});

test('a malformed client refuses and the live client does not move', async () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, a, dir);
  const log = logOf(bus);
  // (A method-missing fixture must be a plain object: `delete` on a class
  // instance cannot remove a prototype method.)
  const missingGet: Record<string, unknown> = {
    putObject: () => {},
    headObject: () => false,
    listObjects: () => [],
    deleteObject: () => {},
  };
  for (const bad of [{}, missingGet, null, undefined]) {
    assert.throws(() => bus.setDurableLogSegmentOffloadClient(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  // The live client is untouched: the next upload still walks A.
  await archiveAndOffload(dir, log, 1);
  assert.ok(a.puts.includes(key1));
});

test('installing the same client is a no-op with no audit', () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const bus = offloadBus(store, a);
  bus.setDurableLogSegmentOffloadClient(a);
  bus.setDurableLogSegmentOffloadClient(a, 'ops-oncall');
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), undefined);
});

test('dry-run swap stops uploads and trimming; swapping back resumes', async () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, a, dir);
  const log = logOf(bus);
  await archiveAndOffload(dir, log, 1);
  assert.ok(a.puts.includes(key1));
  assert.deepEqual(segmentFiles(dir, '.jsonl.gz'), []);

  const dry = new DryRunS3Client();
  bus.setDurableLogSegmentOffloadClient(dry);
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), true);
  await archive(dir, log, 6);
  log.offloadNow();
  // The rehearsal records the intended upload but performs nothing: A
  // sees no new segment, and the local copy is not trimmed.
  assert.ok(!a.puts.includes(key2));
  assert.ok(dry.operations.some((op) => op.op === 'put' && op.key === key2));
  assert.ok(segmentFiles(dir, '.jsonl.gz').some((f) => f.includes('.seg-6-10.')));

  bus.setDurableLogSegmentOffloadClient(a);
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), undefined);
  log.offloadNow();
  assert.ok(a.puts.includes(key2));
  assert.deepEqual(segmentFiles(dir, '.jsonl.gz'), []);
});

test('revision audits the exact dry-run brands and the getter follows', () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const bus = offloadBus(store, a);
  const dry = new DryRunS3Client();
  bus.setDurableLogSegmentOffloadClient(dry, 'ops-oncall');
  assert.equal(audit(bus).length, 1);
  assert.equal(audit(bus)[0].actor, 'ops-oncall');
  assert.deepEqual(audit(bus)[0].detail, { isDryRun: true, oldIsDryRun: null });
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), true);
  // No endpoint or credential material anywhere in the audit trail.
  assert.equal(JSON.stringify(audit(bus)).includes('durable-log-offload'), false);

  bus.setDurableLogSegmentOffloadClient(a);
  assert.equal(audit(bus).length, 2);
  assert.deepEqual(audit(bus)[1].detail, { isDryRun: null, oldIsDryRun: true });
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), undefined);
});

test('revision leaves the manifest, key space, retain count and cache bounds untouched', async () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const b = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, a, dir);
  const log = logOf(bus);
  const before = bus.getStats().durableLog;
  assert.equal(before.retainLocalSegments, 0);
  assert.equal(before.readbackCacheMaxSegments, 16);
  assert.equal(before.readbackCacheMaxBytes, 64 * 1024 * 1024);

  bus.setDurableLogSegmentOffloadClient(b);
  const after = bus.getStats().durableLog;
  assert.equal(after.retainLocalSegments, 0);
  assert.equal(after.readbackCacheMaxSegments, 16);
  assert.equal(after.readbackCacheMaxBytes, 64 * 1024 * 1024);

  // The key space is intact under the new client: both segments upload
  // under the default prefix, and a full archived replay still reads.
  await archiveAndOffload(dir, log, 1);
  await archiveAndOffload(dir, log, 6);
  assert.ok(b.puts.includes(key1));
  assert.ok(b.puts.includes(key2));
  assert.deepEqual(
    log.readSince('t', 0, { includeArchived: true }).map((r) => r.payload),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
});
