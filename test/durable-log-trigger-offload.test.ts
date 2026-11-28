/**
 * Manual durable-log S3 offload trigger (EB-570).
 *
 * `triggerDurableLogOffload(actor?)` runs the root log's `offloadNow()`
 * synchronously and returns what the pass did — `{ uploaded, failed,
 * keys }` — so an operator can push sealed segments to S3 inside a
 * maintenance window instead of waiting for the next rotation or
 * archive sweep, and a client swapped in by
 * `setDurableLogSegmentOffloadClient` (EB-399) is walked at once
 * instead of from the next automatic pass. Only the root log offloads.
 * The pass is audited as `durable_log_offload_triggered`
 * (`{ uploaded, failed }` — the keys are never recorded, since a key
 * names a remote object). A bus with no durable log refuses
 * fail-closed; a bus constructed without `durableLogSegmentOffload`
 * returns the zero result (offload cannot be enabled after the fact,
 * but a trigger there is a harmless no-op, not an error). Segments the
 * remote manifest already knows are not re-pushed; a dry-run client
 * only rehearses; per-segment failures are counted in `failed` and
 * retried on the next trigger — the trigger never throws for a remote
 * failure.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { DryRunS3Client, type RemoteSegmentS3Client } from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-offload-trigger-'));

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

/** `RemoteSegmentS3Client` whose uploads always fail (remote outage), recording attempts. */
class FailingS3 implements RemoteSegmentS3Client {
  readonly attempts: string[] = [];

  putObject(key: string, _body: Uint8Array): void {
    this.attempts.push(key);
    throw new Error('remote unavailable');
  }

  getObject(_key: string): Uint8Array | undefined {
    return undefined;
  }

  headObject(_key: string): boolean {
    return false;
  }

  listObjects(_prefix: string): string[] {
    return [];
  }

  deleteObject(_key: string): void {}
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
  bus.getSubscriptionAudit({ action: 'durable_log_offload_triggered' });

/** The remote segment keys and the manifest key for a two-segment log. */
const key1 = 'durable-log-offload/t.seg-1-5.jsonl.gz';
const key2 = 'durable-log-offload/t.seg-6-10.jsonl.gz';
const manifestKey = 'durable-log-offload/manifest.json';

test('trigger uploads a pending sealed segment and writes the manifest; the audit records counts, never keys', async () => {
  const store = new Map<string, Uint8Array>();
  const failing = new FailingS3();
  const good = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, failing, dir);
  const log = logOf(bus);
  await archive(dir, log, 1);
  // The automatic pass after archiving tried the failing client and left
  // the segment pending: nothing reached the store.
  assert.ok(failing.attempts.includes(key1));
  assert.equal(store.has(key1), false);
  assert.equal(store.has(manifestKey), false);

  bus.setDurableLogSegmentOffloadClient(good, 'ops-oncall');
  const result = bus.triggerDurableLogOffload('ops-oncall');
  assert.deepEqual(result, { uploaded: 1, failed: 0, keys: [key1] });
  assert.ok(store.has(key1));
  assert.ok(store.has(manifestKey));
  assert.ok(good.puts.includes(key1));
  assert.ok(good.puts.includes(manifestKey));

  assert.equal(audit(bus).length, 1);
  assert.equal(audit(bus)[0].actor, 'ops-oncall');
  assert.deepEqual(audit(bus)[0].detail, { uploaded: 1, failed: 0 });
  // A key names a remote object: it must never reach the audit trail.
  assert.equal(JSON.stringify(audit(bus)).includes(key1), false);
});

test('after a client swap, the trigger walks the new client and never re-pushes the old segment', async () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const b = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, a, dir);
  const log = logOf(bus);
  await archive(dir, log, 1);
  assert.ok(a.puts.includes(key1));

  const dry = new DryRunS3Client();
  bus.setDurableLogSegmentOffloadClient(dry);
  await archive(dir, log, 6);
  // The automatic pass rehearsed the second segment only: A saw nothing
  // new and the local copy was not trimmed.
  assert.ok(!a.puts.includes(key2));
  assert.ok(dry.operations.some((op) => op.op === 'put' && op.key === key2));
  assert.ok(segmentFiles(dir, '.jsonl.gz').some((f) => f.includes('.seg-6-10.')));

  bus.setDurableLogSegmentOffloadClient(b);
  assert.ok(!b.puts.includes(key2));
  const result = bus.triggerDurableLogOffload();
  assert.deepEqual(result, { uploaded: 1, failed: 0, keys: [key2] });
  // The trigger walked B — and only for the pending segment.
  assert.ok(b.puts.includes(key2));
  assert.ok(!b.puts.includes(key1));
  assert.ok(!a.puts.includes(key2));
  assert.deepEqual(audit(bus)[0].detail, { uploaded: 1, failed: 0 });
});

test('dry-run trigger rehearses only: keys are returned, nothing uploads, no manifest, no trim', async () => {
  const dir = freshDir();
  const dry = new DryRunS3Client();
  const bus = offloadBus(new Map<string, Uint8Array>(), dry, dir);
  const log = logOf(bus);
  await archive(dir, log, 1);
  // The automatic pass already rehearsed once; the trigger re-runs it.
  const result = bus.triggerDurableLogOffload('ops-oncall');
  assert.deepEqual(result, { uploaded: 0, failed: 0, keys: [key1] });
  assert.equal(
    dry.operations.filter((op) => op.op === 'put' && op.key === key1).length,
    2,
  );
  assert.equal(
    dry.operations.some((op) => op.op === 'put' && op.key === manifestKey),
    false,
  );
  assert.ok(segmentFiles(dir, '.jsonl.gz').some((f) => f.includes('.seg-1-5.')));
  assert.deepEqual(audit(bus)[0].detail, { uploaded: 0, failed: 0 });
});

test('bus without a durable log refuses; a bad actor refuses; no audit', () => {
  const bus = new EventBus({});
  assert.throws(
    () => bus.triggerDurableLogOffload(),
    /requires EventBusOptions.durableLogDir/,
  );
  assert.throws(() => bus.triggerDurableLogOffload(''), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('bus with a log but no offload returns the zero result and still audits', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.deepEqual(bus.triggerDurableLogOffload(), { uploaded: 0, failed: 0, keys: [] });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { uploaded: 0, failed: 0 });
});

test('remote failure is counted, not fatal, and the next trigger retries', async () => {
  const store = new Map<string, Uint8Array>();
  const failing = new FailingS3();
  const good = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, failing, dir);
  const log = logOf(bus);
  await archive(dir, log, 1);

  const first = bus.triggerDurableLogOffload();
  assert.deepEqual(first, { uploaded: 0, failed: 1, keys: [] });
  assert.equal(store.has(key1), false);
  assert.deepEqual(audit(bus)[0].detail, { uploaded: 0, failed: 1 });

  bus.setDurableLogSegmentOffloadClient(good);
  const second = bus.triggerDurableLogOffload();
  assert.deepEqual(second, { uploaded: 1, failed: 0, keys: [key1] });
  assert.ok(store.has(key1));
  assert.ok(store.has(manifestKey));
  assert.deepEqual(audit(bus)[1].detail, { uploaded: 1, failed: 0 });
});

test('already-uploaded segments are not re-pushed by the trigger', async () => {
  const store = new Map<string, Uint8Array>();
  const a = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = offloadBus(store, a, dir);
  const log = logOf(bus);
  await archive(dir, log, 1);
  assert.deepEqual(a.puts, [key1, manifestKey]);

  const result = bus.triggerDurableLogOffload();
  assert.deepEqual(result, { uploaded: 0, failed: 0, keys: [] });
  assert.deepEqual(a.puts, [key1, manifestKey]);
  assert.deepEqual(audit(bus)[0].detail, { uploaded: 0, failed: 0 });
});
