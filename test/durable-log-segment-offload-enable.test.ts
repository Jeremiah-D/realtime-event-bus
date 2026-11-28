/**
 * Durable-log S3 segment offload live enable (EB-571).
 *
 * `setDurableLogSegmentOffload(opts, actor?)` installs the S3 upload
 * channel on a bus with a durable log but no offload, so a running bus
 * gains the cold-storage export without restarting. Segment rotation
 * must already be configured (at construction or live via
 * `setDurableLogSegmentRotation`, EB-566), since only sealed segments
 * are offloaded. The install validates the options first (a usable S3
 * client, a non-empty key prefix, a valid retain count, valid
 * readback-cache bounds), so an illegal option refuses with the bus
 * still unconfigured and a later valid call still enables it;
 * installing arms the background pass at once, so segments sealed
 * before the install upload on the next pass, and it opens the three
 * offload gates at once (`setDurableLogReadbackCache` EB-361,
 * `setDurableLogRetainLocalSegments` EB-362,
 * `setDurableLogSegmentOffloadClient` EB-399). Only the root log
 * offloads. A bus with no durable log, a bus that already has offload
 * (install-only, never replaced), and a closed bus all refuse with
 * `RangeError`; `null` is a silent no-op with no audit. Audited as
 * `durable_log_segment_offload_enabled` (the resolved key prefix,
 * namespace, retain count, readback-cache bounds, and dry-run brand —
 * `Infinity` recorded as `null`, the S3 client itself never recorded).
 * The enable is live-only: a restart restores the unconfigured state.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { DryRunS3Client, type RemoteSegmentS3Client } from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-offload-enable-'));

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

/** A bus with segment rotation but no offload. */
function rotationBus(dir?: string): EventBus {
  return new EventBus({
    durableLogDir: dir ?? freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
  });
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_segment_offload_enabled' });

/** The remote segment key and the manifest key for a one-segment log. */
const key1 = 'bus-offload/t.seg-1-5.jsonl.gz';
const manifestKey = 'bus-offload/manifest.json';

test('install enables offload and opens the three gates; audit records the resolved config, never the client', () => {
  const store = new Map<string, Uint8Array>();
  const s3 = new SharedStoreS3(store);
  const bus = rotationBus();
  bus.setDurableLogSegmentOffload(
    { s3, keyPrefix: 'bus-offload', retainLocalSegments: 2, readbackCacheMaxSegments: 4, readbackCacheMaxBytes: 1024 },
    'ops-oncall',
  );

  // The resolved config is visible without ever exposing the client.
  assert.deepEqual(logOf(bus).currentSegmentOffload(), {
    keyPrefix: 'bus-offload',
    namespace: '',
    retainLocalSegments: 2,
    readbackCacheMaxSegments: 4,
    readbackCacheMaxBytes: 1024,
    isDryRun: undefined,
  });
  assert.equal(bus.getStats().durableLog.retainLocalSegments, 2);
  assert.equal(bus.getStats().durableLog.readbackCacheMaxSegments, 4);
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), undefined);

  assert.equal(audit(bus).length, 1);
  assert.equal(audit(bus)[0].actor, 'ops-oncall');
  assert.deepEqual(audit(bus)[0].detail, {
    keyPrefix: 'bus-offload',
    namespace: '',
    retainLocalSegments: 2,
    readbackCacheMaxSegments: 4,
    readbackCacheMaxBytes: 1024,
    isDryRun: null,
    oldKeyPrefix: null,
    oldNamespace: null,
    oldRetainLocalSegments: null,
    oldReadbackCacheMaxSegments: null,
    oldReadbackCacheMaxBytes: null,
    oldIsDryRun: null,
  });
  assert.equal('s3' in (audit(bus)[0].detail as Record<string, unknown>), false);

  // All three offload gates open at once from the install onward.
  bus.setDurableLogReadbackCache({ readbackCacheMaxSegments: 8 }, 'ops-oncall');
  assert.equal(bus.getStats().durableLog.readbackCacheMaxSegments, 8);
  bus.setDurableLogRetainLocalSegments(1, 'ops-oncall');
  assert.equal(bus.getStats().durableLog.retainLocalSegments, 1);
  const other = new SharedStoreS3(store);
  bus.setDurableLogSegmentOffloadClient(other, 'ops-oncall');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'durable_log_segment_offload_client_revised' }).length,
    1,
  );
  // Exactly one enable audit: the revisions audit under their own actions.
  assert.equal(audit(bus).length, 1);
});

test('illegal options refuse and the bus stays unconfigured; a later valid call still installs', () => {
  const store = new Map<string, Uint8Array>();
  const good = new SharedStoreS3(store);
  const bus = rotationBus();
  // (A method-missing fixture must be a plain object: `delete` on a class
  // instance cannot remove a prototype method.)
  const missingGet: Record<string, unknown> = {
    putObject: () => {},
    headObject: () => false,
    listObjects: () => [],
    deleteObject: () => {},
  };
  assert.throws(
    () => bus.setDurableLogSegmentOffload({ s3: missingGet as unknown as RemoteSegmentS3Client }),
    /segmentOffload\.s3 must be a RemoteSegmentS3Client/,
  );
  assert.throws(
    () => bus.setDurableLogSegmentOffload({ s3: good, keyPrefix: '' }),
    /keyPrefix must be a non-empty string/,
  );
  assert.throws(
    () => bus.setDurableLogSegmentOffload({ s3: good, retainLocalSegments: -1 }),
    /retainLocalSegments must be a non-negative integer/,
  );
  assert.throws(
    () => bus.setDurableLogSegmentOffload({ s3: good, readbackCacheMaxSegments: -1 }),
    /readbackCacheMaxSegments must be a non-negative integer/,
  );
  assert.equal(audit(bus).length, 0);
  assert.equal(logOf(bus).currentSegmentOffload(), undefined);
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), undefined);
  assert.throws(
    () => bus.setDurableLogReadbackCache({ readbackCacheMaxSegments: 1 }),
    /use setDurableLogSegmentOffload to enable offload live/,
  );

  bus.setDurableLogSegmentOffload({ s3: good });
  assert.equal(audit(bus).length, 1);
  // Defaults applied: the default key prefix, full retention.
  assert.equal(logOf(bus).currentSegmentOffload()?.keyPrefix, 'durable-log-offload');
  assert.equal(audit(bus)[0].detail.retainLocalSegments, null);
});

test('already-installed bus refuses reinstall; a bus constructed with offload refuses too', () => {
  const store = new Map<string, Uint8Array>();
  const s3 = new SharedStoreS3(store);
  const bus = rotationBus();
  bus.setDurableLogSegmentOffload({ s3 });
  assert.throws(
    () => bus.setDurableLogSegmentOffload({ s3: new SharedStoreS3(store) }),
    /already enabled/,
  );
  assert.equal(audit(bus).length, 1);

  const constructed = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    durableLogSegmentOffload: { s3 },
  });
  assert.throws(
    () => constructed.setDurableLogSegmentOffload({ s3: new SharedStoreS3(store) }),
    /already enabled/,
  );
  assert.equal(audit(constructed).length, 0);
});

test('bus without a durable log refuses; bad actor refuses; null is a silent no-op', () => {
  const store = new Map<string, Uint8Array>();
  const s3 = new SharedStoreS3(store);
  const bus = new EventBus({});
  assert.throws(
    () => bus.setDurableLogSegmentOffload({ s3 }),
    /requires EventBusOptions.durableLogDir/,
  );
  assert.throws(() => bus.setDurableLogSegmentOffload({ s3 }, ''), RangeError);
  assert.equal(audit(bus).length, 0);

  const logged = rotationBus();
  logged.setDurableLogSegmentOffload(null);
  assert.equal(audit(logged).length, 0);
  assert.equal(logOf(logged).currentSegmentOffload(), undefined);
  assert.throws(
    () => logged.setDurableLogReadbackCache({ readbackCacheMaxSegments: 1 }),
    /use setDurableLogSegmentOffload to enable offload live/,
  );
});

test('bus without rotation refuses; enabling rotation first (EB-566) then installing offload works', () => {
  const store = new Map<string, Uint8Array>();
  const s3 = new SharedStoreS3(store);
  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(
    () => bus.setDurableLogSegmentOffload({ s3 }),
    /segmentOffload requires segmentRotation/,
  );
  assert.equal(audit(bus).length, 0);
  assert.equal(logOf(bus).currentSegmentOffload(), undefined);

  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 });
  bus.setDurableLogSegmentOffload({ s3, keyPrefix: 'bus-offload' }, 'ops-oncall');
  assert.equal(audit(bus).length, 1);
  assert.equal(logOf(bus).currentSegmentOffload()?.keyPrefix, 'bus-offload');
});

test('install walks the installed client on trigger: a sealed segment uploads and the manifest lands', async () => {
  const store = new Map<string, Uint8Array>();
  const failing = new FailingS3();
  const good = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = rotationBus(dir);
  const log = logOf(bus);
  await archive(dir, log, 1);

  bus.setDurableLogSegmentOffload({ s3: failing, keyPrefix: 'bus-offload' });
  // The install armed a background pass, which walked the failing client
  // and left the segment pending: nothing reached the store.
  await waitFor(() => failing.attempts.includes(key1));
  assert.equal(store.has(key1), false);
  assert.equal(store.has(manifestKey), false);

  const failed = bus.triggerDurableLogOffload('ops-oncall');
  assert.deepEqual(failed, { uploaded: 0, failed: 1, keys: [] });

  bus.setDurableLogSegmentOffloadClient(good, 'ops-oncall');
  const result = bus.triggerDurableLogOffload('ops-oncall');
  assert.deepEqual(result, { uploaded: 1, failed: 0, keys: [key1] });
  assert.ok(store.has(key1));
  assert.ok(store.has(manifestKey));
  assert.ok(good.puts.includes(key1));
  assert.ok(good.puts.includes(manifestKey));
  assert.equal(bus.getStats().durableLog.offloadUploaded, 1);
});

test('install arms the background pass: a segment sealed before the install uploads without a trigger', async () => {
  const store = new Map<string, Uint8Array>();
  const s3 = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = rotationBus(dir);
  const log = logOf(bus);
  await archive(dir, log, 1);
  assert.equal(store.has(key1), false);

  bus.setDurableLogSegmentOffload({ s3, keyPrefix: 'bus-offload' });
  await waitFor(() => store.has(key1));
  assert.ok(store.has(manifestKey));
  assert.equal(bus.getStats().durableLog.offloadUploaded, 1);
});

test('dry-run install rehearses only: keys are returned, nothing uploads, no manifest, no trim', async () => {
  const dir = freshDir();
  const dry = new DryRunS3Client();
  const bus = rotationBus(dir);
  const log = logOf(bus);
  await archive(dir, log, 1);

  bus.setDurableLogSegmentOffload({ s3: dry, keyPrefix: 'bus-offload' }, 'ops-oncall');
  assert.equal(audit(bus)[0].detail.isDryRun, true);
  assert.equal(bus.getDurableLogSegmentOffloadClientIsDryRun(), true);

  const result = bus.triggerDurableLogOffload('ops-oncall');
  assert.deepEqual(result, { uploaded: 0, failed: 0, keys: [key1] });
  assert.ok(dry.operations.some((op) => op.op === 'put' && op.key === key1));
  assert.equal(
    dry.operations.some((op) => op.op === 'put' && op.key === manifestKey),
    false,
  );
  assert.ok(segmentFiles(dir, '.jsonl.gz').some((f) => f.includes('.seg-1-5.')));
});

test('restart restores the unconfigured state', async () => {
  const store = new Map<string, Uint8Array>();
  const s3 = new SharedStoreS3(store);
  const dir = freshDir();
  const bus = rotationBus(dir);
  const log = logOf(bus);
  await archive(dir, log, 1);
  bus.setDurableLogSegmentOffload({ s3, keyPrefix: 'bus-offload' });
  assert.notEqual(logOf(bus).currentSegmentOffload(), undefined);

  const reopened = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
  });
  assert.equal(logOf(reopened).currentSegmentOffload(), undefined);
  assert.throws(
    () => reopened.setDurableLogReadbackCache({ readbackCacheMaxSegments: 1 }),
    /use setDurableLogSegmentOffload to enable offload live/,
  );
  assert.equal(audit(reopened).length, 0);
});

test('closed bus refuses', () => {
  const store = new Map<string, Uint8Array>();
  const s3 = new SharedStoreS3(store);
  const bus = rotationBus();
  bus.close();
  assert.throws(() => bus.setDurableLogSegmentOffload({ s3 }), /bus is closed/);
  assert.equal(audit(bus).length, 0);
});
