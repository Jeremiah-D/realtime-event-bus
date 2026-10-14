/**
 * Pluggable durable-log storage backends (EB-96).
 *
 * Covers: the `DurableStorageBackend` contract on the filesystem
 * implementation (atomic write, append, read, list, delete, rename,
 * scoping, key validation), `DurableTopicLog` running end to end on a
 * non-filesystem (in-memory) backend — live log, offset journal,
 * publish-dedup journal, recovery via backend enumeration — archive
 * rotation with per-segment checksums above an in-memory backend
 * (encryption/checksum semantics unchanged by the backend), the bus
 * option `durableLogStorageBackend`, and the S3-compatible example
 * adapter against a stub transport.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Buffer } from 'node:buffer';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import {
  FilesystemStorageBackend,
  PrefixedStorageBackend,
  assertValidStorageKey,
  type DurableStorageBackend,
} from '../src/storage.ts';
import { createS3CompatibleBackend } from '../src/storage-s3.ts';

/** In-memory `DurableStorageBackend` — the non-filesystem proof. */
class MemoryStorageBackend implements DurableStorageBackend {
  readonly objects = new Map<string, Uint8Array>();

  private bytes(data: Uint8Array | string): Uint8Array {
    return typeof data === 'string' ? Buffer.from(data, 'utf8') : Buffer.from(data);
  }

  write(key: string, data: Uint8Array | string, opts?: { exclusive?: boolean }): void {
    assertValidStorageKey(key, 'MemoryStorageBackend');
    if (opts?.exclusive === true && this.objects.has(key)) {
      throw new Error(`EEXIST: ${key}`);
    }
    this.objects.set(key, this.bytes(data));
  }

  append(key: string, data: Uint8Array | string): void {
    assertValidStorageKey(key, 'MemoryStorageBackend');
    const prev = this.objects.get(key) ?? new Uint8Array(0);
    const extra = this.bytes(data);
    const merged = new Uint8Array(prev.length + extra.length);
    merged.set(prev, 0);
    merged.set(extra, prev.length);
    this.objects.set(key, merged);
  }

  read(key: string): Uint8Array | undefined {
    return this.objects.get(key);
  }

  list(prefix = ''): string[] {
    return [...this.objects.keys()].filter((k) => k.startsWith(prefix));
  }

  delete(key: string): void {
    this.objects.delete(key);
  }

  rename(oldKey: string, newKey: string): void {
    const value = this.objects.get(oldKey);
    if (value === undefined) throw new Error(`ENOENT: ${oldKey}`);
    this.objects.delete(oldKey);
    this.objects.set(newKey, value);
  }

  scoped(prefix: string): DurableStorageBackend {
    return new PrefixedStorageBackend(this, prefix);
  }
}

const textOf = (backend: DurableStorageBackend, key: string): string | undefined => {
  const bytes = backend.read(key);
  return bytes === undefined ? undefined : Buffer.from(bytes).toString('utf8');
};

test('FilesystemStorageBackend: write/read/append round-trip', () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-storage-'));
  const backend = new FilesystemStorageBackend(dir);
  backend.write('a.log', 'hello');
  assert.equal(textOf(backend, 'a.log'), 'hello');
  backend.append('a.log', ' world');
  assert.equal(textOf(backend, 'a.log'), 'hello world');
  backend.write('b.bin', new Uint8Array([0, 1, 2, 255]));
  assert.deepEqual([...(backend.read('b.bin') as Uint8Array)], [0, 1, 2, 255]);
  assert.equal(backend.read('missing'), undefined);
});

test('FilesystemStorageBackend: exclusive write fails when the key exists', () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-storage-'));
  const backend = new FilesystemStorageBackend(dir);
  backend.write('x', 'first', { exclusive: true });
  assert.throws(() => backend.write('x', 'second', { exclusive: true }));
  assert.equal(textOf(backend, 'x'), 'first');
  // Non-exclusive write replaces atomically.
  backend.write('x', 'second');
  assert.equal(textOf(backend, 'x'), 'second');
});

test('FilesystemStorageBackend: delete is idempotent, rename moves', () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-storage-'));
  const backend = new FilesystemStorageBackend(dir);
  backend.delete('nope'); // no throw
  backend.write('a', '1');
  backend.rename('a', 'b');
  assert.equal(backend.read('a'), undefined);
  assert.equal(textOf(backend, 'b'), '1');
  backend.delete('b');
  assert.equal(backend.read('b'), undefined);
});

test('FilesystemStorageBackend: list filters by prefix, scoped views nest', () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-storage-'));
  const backend = new FilesystemStorageBackend(dir);
  backend.write('t.log', 'x');
  backend.write('__group_offsets.jsonl', 'y');
  const child = backend.scoped('namespaces/t1');
  child.write('t1%2Forders.log', 'z');
  assert.deepEqual(backend.list('__').sort(), ['__group_offsets.jsonl']);
  assert.deepEqual(backend.list('namespaces/'), ['namespaces/t1/t1%2Forders.log']);
  assert.deepEqual(child.list(), ['t1%2Forders.log']);
  assert.equal(textOf(child, 't1%2Forders.log'), 'z');
});

test('storage keys reject empty, absolute, and parent-traversal keys', () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-storage-'));
  const backend = new FilesystemStorageBackend(dir);
  for (const bad of ['', '/abs', '../up', 'a/../b', 'a//b']) {
    assert.throws(() => backend.write(bad, 'x'), RangeError, bad || '(empty)');
  }
  assert.throws(() => backend.scoped(''), RangeError);
});

test('DurableTopicLog runs on a custom backend: live log + recovery via list()', () => {
  const backend = new MemoryStorageBackend();
  const log = DurableTopicLog.open({ dir: 'memory://eb96', backend });
  assert.ok(log.append({ seq: 1, topic: 't', at: 1000, payload: { n: 1 } }));
  assert.ok(log.append({ seq: 2, topic: 't', at: 1001, payload: { n: 2 } }));
  // The backend holds the former file basenames as keys.
  assert.ok(backend.objects.has('t.log'));
  assert.deepEqual(log.topics(), ['t']);

  // Reopen over the same objects: recovery enumerates the backend.
  const reopened = DurableTopicLog.open({ dir: 'memory://eb96', backend });
  assert.deepEqual(reopened.topics(), ['t']);
  const records = reopened.readSince('t', 0);
  assert.deepEqual(records.map((r) => r.seq), [1, 2]);
  assert.deepEqual(records[0].payload, { n: 1 });
});

test('DurableTopicLog on a custom backend: offset + publish-dedup journals round-trip', () => {
  const backend = new MemoryStorageBackend();
  const log = DurableTopicLog.open({ dir: 'memory://eb96', backend });
  assert.ok(log.appendOffset('g1', 't', 7, 1000));
  assert.ok(log.appendPublishDedup({ topic: 't', messageId: 'm1', at: 1000 }));
  assert.ok(backend.objects.has('__group_offsets.jsonl'));
  assert.ok(backend.objects.has('__publish_dedup.jsonl'));

  const reopened = DurableTopicLog.open({ dir: 'memory://eb96', backend });
  assert.equal(reopened.recoveredCommittedOffsets().get('g1\0t'), 7);
  const claims = reopened.recoveredPublishDedup();
  assert.equal(claims.length, 1);
  assert.equal(claims[0].topic, 't');
  assert.equal(claims[0].messageId, 'm1');
});

test('DurableTopicLog.open validates the backend option', () => {
  assert.throws(
    () => DurableTopicLog.open({ dir: 'x', backend: { write: 1 } as unknown as DurableStorageBackend }),
    RangeError,
  );
});

test('archive rotation + checksums work above a custom backend; tampering isolates the segment', async () => {
  const backend = new MemoryStorageBackend();
  const log = DurableTopicLog.open({
    dir: 'memory://eb96',
    backend,
    segmentRotation: { maxEntriesPerSegment: 3 },
  });
  for (let i = 1; i <= 3; i += 1) {
    log.append({ seq: i, topic: 't', at: 1000 + i, payload: i });
  }
  // Rotation closed the live segment synchronously; the gzip/index work
  // runs on the background pass.
  await new Promise((resolve) => setTimeout(resolve, 50));
  const gzKeys = backend.list().filter((k) => k.endsWith('.jsonl.gz'));
  assert.equal(gzKeys.length, 1);
  const gzKey = gzKeys[0];
  assert.ok(backend.objects.has(`${gzKey}.sha256`), 'checksum sidecar written through the backend');
  assert.ok(textOf(backend, '__archive_index.jsonl')?.includes('t'));

  // Archived reads work through the backend.
  const archived = log.readSince('t', 0, { includeArchived: true });
  assert.deepEqual(archived.map((r) => r.seq), [1, 2, 3]);

  // Tamper with the archived bytes: recovery isolates the segment
  // (checksum mismatch), never fatal — the EB-94 semantic, unchanged
  // by the backend.
  const gz = backend.objects.get(gzKey) as Uint8Array;
  const tampered = Buffer.from(gz);
  tampered[10] ^= 0xff;
  backend.objects.set(gzKey, tampered);
  const reopened = DurableTopicLog.open({
    dir: 'memory://eb96',
    backend,
    segmentRotation: { maxEntriesPerSegment: 3 },
  });
  assert.equal(reopened.stats().corruptSegments, 1);
  assert.deepEqual(reopened.readSince('t', 0, { includeArchived: true }), []);
});

test('EventBus: durableLogStorageBackend plugs the whole bus into a custom backend', async () => {
  const backend = new MemoryStorageBackend();
  const bus = new EventBus({ durableLogDir: 'memory://eb96-bus', durableLogStorageBackend: backend });
  bus.publish('t', { v: 1 });
  await new Promise((resolve) => setImmediate(resolve));
  assert.ok(backend.objects.has('t.log'));

  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(received, [{ v: 1 }]);
});

test('EventBus: durableLogStorageBackend requires durableLogDir', () => {
  const backend = new MemoryStorageBackend();
  assert.throws(
    () => new EventBus({ durableLogStorageBackend: backend }),
    /durableLogStorageBackend requires EventBusOptions\.durableLogDir/,
  );
});

test('EventBus: namespace child logs share the root backend via scoped()', async () => {
  const backend = new MemoryStorageBackend();
  const bus = new EventBus({ durableLogDir: 'memory://eb96-ns', durableLogStorageBackend: backend });
  const t1 = bus.createNamespace('t1');
  t1.publish('orders', { id: 1 });
  await new Promise((resolve) => setImmediate(resolve));
  // The child log's keys live under the reserved prefix on the root backend.
  assert.deepEqual(backend.list('namespaces/'), ['namespaces/t1/t1%2Forders.log']);
  // The root log's own enumeration ignores the child prefix.
  const root = DurableTopicLog.open({ dir: 'memory://eb96-ns', backend });
  assert.deepEqual(root.topics(), []);
});

/** Minimal stub S3 transport: a Map standing in for the object store. */
function stubS3Transport() {
  const objects = new Map<string, Uint8Array>();
  const calls: Array<{ method: string; url: string }> = [];
  const xmlEscape = (s: string) =>
    s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  return {
    objects,
    calls,
    transport: {
      request(method: string, url: string, _headers: Record<string, string>, body?: Uint8Array) {
        calls.push({ method, url });
        const u = new URL(url);
        if (u.searchParams.get('list-type') === '2') {
          const prefix = u.searchParams.get('prefix') ?? '';
          const keys = [...objects.keys()]
            .filter((k) => k.startsWith(prefix))
            .map((k) => `      <Key>${xmlEscape(k)}</Key>`)
            .join('\n');
          const xml =
            `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated>\n${keys}\n</ListBucketResult>`;
          return { status: 200, headers: {}, body: Buffer.from(xml, 'utf8') };
        }
        // Object path: /{bucket}/{key...}
        const key = u.pathname.split('/').slice(2).map(decodeURIComponent).join('/');
        if (method === 'PUT') {
          if (_headers['x-amz-copy-source'] !== undefined) {
            const src = decodeURIComponent(_headers['x-amz-copy-source'].split('/').slice(2).join('/'));
            const value = objects.get(src);
            if (value === undefined) return { status: 404, headers: {}, body: new Uint8Array(0) };
            objects.set(key, value);
            return { status: 200, headers: {}, body: new Uint8Array(0) };
          }
          objects.set(key, body ?? new Uint8Array(0));
          return { status: 200, headers: {}, body: new Uint8Array(0) };
        }
        if (method === 'GET' || method === 'HEAD') {
          const value = objects.get(key);
          if (value === undefined) return { status: 404, headers: {}, body: new Uint8Array(0) };
          return { status: 200, headers: {}, body: method === 'HEAD' ? new Uint8Array(0) : value };
        }
        if (method === 'DELETE') {
          objects.delete(key);
          return { status: 204, headers: {}, body: new Uint8Array(0) };
        }
        throw new Error(`stub: unexpected ${method}`);
      },
    },
  };
}

const s3Options = (stub: ReturnType<typeof stubS3Transport>, prefix = 'logs/') => ({
  endpoint: 'https://s3.example.test',
  bucket: 'bus-logs',
  prefix,
  signRequest: (_m: string, _u: string, headers: Record<string, string>) => headers,
  transport: stub.transport,
});

test('S3 example backend: write/read/list/delete/rename through the stub transport', () => {
  const stub = stubS3Transport();
  const backend = createS3CompatibleBackend(s3Options(stub));
  assert.equal(backend.read('t.log'), undefined);
  backend.write('t.log', 'hello');
  assert.equal(Buffer.from(backend.read('t.log') as Uint8Array).toString(), 'hello');
  backend.append('t.log', ' world');
  assert.equal(Buffer.from(backend.read('t.log') as Uint8Array).toString(), 'hello world');
  assert.deepEqual(backend.list(), ['t.log']);
  assert.deepEqual(backend.list('__'), []);
  backend.rename('t.log', 't2.log');
  assert.equal(backend.read('t.log'), undefined);
  assert.ok(backend.read('t2.log') !== undefined);
  backend.delete('t2.log');
  backend.delete('t2.log'); // idempotent
  assert.deepEqual(backend.list(), []);
  // Object URLs carry bucket + prefix + encoded key.
  assert.ok(stub.calls[0].url.startsWith('https://s3.example.test/bus-logs/logs/'));
});

test('S3 example backend: exclusive write throws when the object exists', () => {
  const stub = stubS3Transport();
  const backend = createS3CompatibleBackend(s3Options(stub));
  backend.write('seg.gz', 'a', { exclusive: true });
  assert.throws(() => backend.write('seg.gz', 'b', { exclusive: true }));
});

test('S3 example backend: option validation', () => {
  const stub = stubS3Transport();
  assert.throws(() => createS3CompatibleBackend({ ...s3Options(stub), endpoint: '' }), RangeError);
  assert.throws(() => createS3CompatibleBackend({ ...s3Options(stub), bucket: '' }), RangeError);
  assert.throws(
    () =>
      createS3CompatibleBackend({
        ...s3Options(stub),
        signRequest: undefined as unknown as (m: string, u: string, h: Record<string, string>) => Record<string, string>,
      }),
    RangeError,
  );
});

test('DurableTopicLog runs end to end on the S3 example backend (stub transport)', () => {
  const stub = stubS3Transport();
  const backend = createS3CompatibleBackend(s3Options(stub));
  const log = DurableTopicLog.open({ dir: 's3://bus-logs/logs', backend });
  log.append({ seq: 1, topic: 't', at: 1000, payload: 'v1' });
  assert.ok(log.appendOffset('g', 't', 1, 1000));
  const reopened = DurableTopicLog.open({ dir: 's3://bus-logs/logs', backend });
  assert.deepEqual(reopened.readSince('t', 0).map((r) => r.seq), [1]);
  assert.equal(reopened.recoveredCommittedOffsets().get('g\0t'), 1);
});
