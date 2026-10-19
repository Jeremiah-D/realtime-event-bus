import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DurableTopicLog } from '../src/durablelog.ts';
import {
  DryRunS3Client,
  signS3RequestSigV4,
  type RemoteSegmentS3Client,
} from '../src/storage-s3.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-offload-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** In-memory `RemoteSegmentS3Client` — zero network. */
class MemoryS3 implements RemoteSegmentS3Client {
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

const segmentFiles = (dir: string, ext: '.jsonl.gz' | '.jsonl.tmp' | '.sha256') =>
  readdirSync(dir).filter((f) => f.endsWith(ext));

/** Archives one 5-record segment for topic 't' and waits for the background pass. */
async function archiveOneSegment(
  dir: string,
  log: DurableTopicLog,
  fromSeq: number,
  settled?: () => boolean,
): Promise<void> {
  for (let i = 0; i < 5; i += 1) {
    const seq = fromSeq + i;
    log.append({ seq, topic: 't', at: 1000 + seq, payload: seq });
  }
  const prefix = `${encodeURIComponent('t')}.seg-`;
  // With retainLocalSegments: 0 the pass uploads and trims the local
  // copy atomically — the terminal state may have no local .gz at all,
  // so a caller-supplied `settled` (e.g. "remote key present") also ends
  // the wait.
  await waitFor(
    () =>
      segmentFiles(dir, '.jsonl.tmp').filter((f) => f.startsWith(prefix)).length === 0 &&
      (segmentFiles(dir, '.jsonl.gz').filter((f) => f.startsWith(prefix)).length > 0 ||
        (settled?.() ?? false)),
  );
}

const waitForRemote = (mem: MemoryS3, key: string) =>
  waitFor(() => mem.objects.has(key));

function readManifest(mem: MemoryS3, key = 'durable-log-offload/manifest.json'): {
  v: number;
  segments: Array<{ topic: string; segmentId: string; key: string; sha256: string }>;
} {
  const raw = mem.objects.get(key);
  assert.ok(raw !== undefined, `manifest missing: ${key}`);
  return JSON.parse(Buffer.from(raw).toString('utf8'));
}

test('offload: SigV4 signer matches an independently computed signature', () => {
  // Input set cross-checked with a from-spec Python implementation:
  // expected signature 65f031d9… computed outside this codebase.
  const headers = signS3RequestSigV4({
    credentials: {
      accessKeyId: 'AKIDEXAMPLE',
      secretAccessKey: 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY',
    },
    region: 'us-east-1',
    service: 'iam',
    method: 'GET',
    url: 'https://iam.amazonaws.com/?Action=ListUsers&Version=2010-05-08',
    headers: { host: 'iam.amazonaws.com' },
    amzDate: '20150830T123600Z',
  });
  assert.equal(
    headers['Authorization'],
    'AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/iam/aws4_request, ' +
      'SignedHeaders=host;x-amz-content-sha256;x-amz-date, ' +
      'Signature=65f031d93b4631aedf16a8f7f830cdc8ce2bc5276c307b5a2cc2143d4b68e323',
  );
  // The payload hash is the well-known empty-string SHA-256.
  assert.equal(
    headers['x-amz-content-sha256'],
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
  );
  assert.equal(headers['x-amz-date'], '20150830T123600Z');
});

test('offload: sealed segments upload under deterministic keys; local copy kept by default', async () => {
  const dir = freshDir();
  const mem = new MemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem },
  });
  await archiveOneSegment(dir, log, 1);
  const key = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  await waitForRemote(mem, key);
  // Byte-identical replica of the local archive.
  assert.deepEqual(
    Buffer.from(mem.objects.get(key) as Uint8Array),
    readFileSync(join(dir, `${encodeURIComponent('t')}.seg-1-5.jsonl.gz`)),
  );
  // Default retainLocalSegments (Infinity): current semantics — the
  // local copy stays.
  assert.equal(segmentFiles(dir, '.jsonl.gz').length, 1);
  const manifest = readManifest(mem);
  assert.equal(manifest.segments.length, 1);
  assert.equal(manifest.segments[0].topic, 't');
  assert.equal(manifest.segments[0].segmentId, '1-5');
  assert.equal(manifest.segments[0].key, key);
  assert.match(manifest.segments[0].sha256, /^[0-9a-f]{64}$/);
  assert.equal(log.stats().offloadUploaded, 1);
  assert.equal(log.stats().offloadFailed, 0);
});

test('offload: key layout honors keyPrefix and namespace', async () => {
  const dir = freshDir();
  const mem = new MemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, keyPrefix: 'prod-logs', namespace: 'eu-1' },
  });
  await archiveOneSegment(dir, log, 1);
  const key = 'prod-logs/eu-1/t.seg-1-5.jsonl.gz';
  await waitForRemote(mem, key);
  const manifest = readManifest(mem, 'prod-logs/eu-1/manifest.json');
  assert.equal(manifest.segments[0].key, key);
});

test('offload: retainLocalSegments trims older local copies after confirmed upload', async () => {
  const dir = freshDir();
  const mem = new MemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 1 },
  });
  await archiveOneSegment(dir, log, 1);
  await archiveOneSegment(dir, log, 6);
  await archiveOneSegment(dir, log, 11);
  await waitForRemote(mem, 'durable-log-offload/t.seg-11-15.jsonl.gz');
  // All three segments landed remotely…
  assert.ok(mem.objects.has('durable-log-offload/t.seg-1-5.jsonl.gz'));
  assert.ok(mem.objects.has('durable-log-offload/t.seg-6-10.jsonl.gz'));
  assert.equal(readManifest(mem).segments.length, 3);
  // …but only the newest local copy survived, sidecars with their segments.
  await waitFor(
    () =>
      segmentFiles(dir, '.jsonl.gz').length === 1 && segmentFiles(dir, '.sha256').length === 1,
  );
  assert.deepEqual(segmentFiles(dir, '.jsonl.gz'), [
    `${encodeURIComponent('t')}.seg-11-15.jsonl.gz`,
  ]);
});

test('offload: readSince pulls a trimmed segment from remote and verifies the manifest hash', async () => {
  const dir = freshDir();
  const mem = new MemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 1 },
  });
  await archiveOneSegment(dir, log, 1);
  await archiveOneSegment(dir, log, 6);
  await waitForRemote(mem, 'durable-log-offload/t.seg-6-10.jsonl.gz');
  await waitFor(() => segmentFiles(dir, '.jsonl.gz').length === 1);
  // Segment 1-5 is gone locally — the replay below must pull it down.
  assert.ok(!segmentFiles(dir, '.jsonl.gz').some((f) => f.includes('.seg-1-5.')));
  const records = log.readSince('t', 0, { includeArchived: true });
  assert.deepEqual(
    records.map((r) => r.seq),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
  assert.deepEqual(
    records.map((r) => r.payload),
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  );
});

test('offload: hash mismatch on a pulled segment throws explicitly', async () => {
  const dir = freshDir();
  const mem = new MemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
  const key = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  await archiveOneSegment(dir, log, 1, () => mem.objects.has(key));
  await waitFor(() => segmentFiles(dir, '.jsonl.gz').length === 0);
  // Tamper with the remote bytes after the manifest recorded the hash.
  mem.objects.set(key, Buffer.from('tampered-by-an-attacker'));
  assert.throws(
    () => log.readSince('t', 0, { includeArchived: true }),
    /hash mismatch.*refusing to truncate replay/,
  );
});

test('offload: segment missing locally and remotely throws instead of truncating', async () => {
  const dir = freshDir();
  const mem = new MemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
  const key = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  await archiveOneSegment(dir, log, 1, () => mem.objects.has(key));
  await waitFor(() => segmentFiles(dir, '.jsonl.gz').length === 0);
  // The remote object is gone too — the replay must fail loudly,
  // never return the records after the gap as if history were whole.
  mem.objects.delete(key);
  assert.throws(
    () => log.readSince('t', 0, { includeArchived: true }),
    /missing remotely.*refusing to truncate replay/,
  );
});

test('offload: segment absent from the remote manifest throws explicitly', async () => {
  const dir = freshDir();
  const mem = new MemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
  await archiveOneSegment(dir, log, 1, () =>
    mem.objects.has('durable-log-offload/t.seg-1-5.jsonl.gz'),
  );
  await waitFor(() => segmentFiles(dir, '.jsonl.gz').length === 0);
  // Manifest wiped after upload: the segment is no longer attested.
  mem.objects.delete('durable-log-offload/manifest.json');
  assert.throws(
    () => log.readSince('t', 0, { includeArchived: true }),
    /not listed in the remote manifest.*refusing to truncate replay/,
  );
});

test('offload: dry run records intended keys with zero side effects', async () => {
  const dir = freshDir();
  const dry = new DryRunS3Client();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    // retainLocalSegments: 1 would trim on a real run — the rehearsal
    // must not.
    segmentOffload: { s3: dry, retainLocalSegments: 1 },
  });
  await archiveOneSegment(dir, log, 1);
  const key = 'durable-log-offload/t.seg-1-5.jsonl.gz';
  await waitFor(() => dry.operations.some((op) => op.op === 'put' && op.key === key));
  // The rehearsal announced the upload…
  assert.ok(dry.operations.some((op) => op.op === 'put' && op.key === key));
  assert.ok(
    dry.operations.some((op) => op.op === 'get' && op.key === 'durable-log-offload/manifest.json'),
  );
  // …but changed nothing: the local archive (and its sidecar) survive,
  // no upload is counted.
  assert.equal(segmentFiles(dir, '.jsonl.gz').length, 1);
  assert.equal(segmentFiles(dir, '.sha256').length, 1);
  assert.equal(log.stats().offloadUploaded, 0);
  assert.equal(log.stats().offloadFailed, 0);
});

test('offload: offloadNow is idempotent — no duplicate manifest entries', async () => {
  const dir = freshDir();
  const mem = new MemoryS3();
  const log = DurableTopicLog.open({
    dir,
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem },
  });
  await archiveOneSegment(dir, log, 1);
  await waitForRemote(mem, 'durable-log-offload/t.seg-1-5.jsonl.gz');
  const again = log.offloadNow();
  assert.equal(again.uploaded, 0);
  assert.equal(again.failed, 0);
  assert.deepEqual(again.keys, []);
  assert.equal(readManifest(mem).segments.length, 1);
  assert.equal(log.stats().offloadUploaded, 1);
});

test('offload: option validation fails closed', () => {
  const mem = new MemoryS3();
  // Offload without rotation: there are no sealed segments to offload.
  assert.throws(
    () => DurableTopicLog.open({ dir: freshDir(), segmentOffload: { s3: mem } }),
    /requires segmentRotation/,
  );
  // Not a usable S3 client.
  assert.throws(
    () =>
      DurableTopicLog.open({
        dir: freshDir(),
        segmentRotation: { maxEntriesPerSegment: 5 },
        segmentOffload: { s3: {} as RemoteSegmentS3Client },
      }),
    /RemoteSegmentS3Client/,
  );
  // Bad retention bound.
  for (const retainLocalSegments of [-1, 1.5, Number.NaN]) {
    assert.throws(
      () =>
        DurableTopicLog.open({
          dir: freshDir(),
          segmentRotation: { maxEntriesPerSegment: 5 },
          segmentOffload: { s3: mem, retainLocalSegments },
        }),
      /retainLocalSegments/,
    );
  }
  // Zero is allowed: keep nothing locally, remote is the archive.
  DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
    segmentOffload: { s3: mem, retainLocalSegments: 0 },
  });
});
