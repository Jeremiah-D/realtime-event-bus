import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { gunzipSync } from 'node:zlib';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import {
  AesGcmEncryptor,
  NoopEncryptor,
  isEncryptedPayload,
  randomAes256Key,
} from '../src/encrypt.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-logenc-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

const PAYLOAD_SECRET = 'payment-card-pan-4111-1111-1111-1111';

test('encrypt module: AES-GCM round-trip, fresh IVs, tamper fails loudly', () => {
  const key = randomAes256Key();
  const enc = new AesGcmEncryptor(key);
  const a = enc.encrypt(Buffer.from('hello', 'utf8'));
  const b = enc.encrypt(Buffer.from('hello', 'utf8'));
  assert.equal(a.alg, 'aes-256-gcm');
  // Fresh random IV per seal: identical plaintexts seal to different bytes.
  assert.notEqual(a.iv, b.iv);
  assert.notEqual(a.data, b.data);
  assert.deepEqual(enc.decrypt(a), Buffer.from('hello', 'utf8'));
  assert.ok(isEncryptedPayload(a));
  assert.ok(!isEncryptedPayload({ v: 1 }));
  // Tampered ciphertext fails loudly, never returns garbage.
  const tampered = { ...a, data: a.data.slice(0, -4) + 'AAAA' };
  assert.throws(() => enc.decrypt(tampered), /decrypt failed/);
  // Wrong key fails loudly.
  const other = new AesGcmEncryptor(randomAes256Key());
  assert.throws(() => other.decrypt(a), /decrypt failed/);
  // Bad key shape is a RangeError at construction.
  assert.throws(() => new AesGcmEncryptor(Buffer.alloc(16)), RangeError);
  // NoopEncryptor is an envelope-shaped passthrough (tests only).
  const noop = new NoopEncryptor();
  const sealed = noop.encrypt(Buffer.from('x', 'utf8'));
  assert.equal(sealed.alg, 'none');
  assert.deepEqual(noop.decrypt(sealed), Buffer.from('x', 'utf8'));
  assert.throws(() => noop.decrypt(a), /unsupported envelope/);
});

test('DurableTopicLog: sealed lines at rest, transparent replay', () => {
  const dir = freshDir();
  const key = randomAes256Key();
  const log = DurableTopicLog.open({ dir, encryptor: new AesGcmEncryptor(key) });
  log.append({ seq: 1, topic: 'pay.events', at: 1000, payload: { pan: PAYLOAD_SECRET } });
  log.append({ seq: 2, topic: 'pay.events', at: 1001, payload: { pan: PAYLOAD_SECRET } });

  const raw = readFileSync(join(dir, `${encodeURIComponent('pay.events')}.log`), 'utf8');
  const lines = raw.split('\n').filter((l) => l.length > 0);
  assert.equal(lines.length, 2);
  for (const line of lines) {
    const parsed = JSON.parse(line) as { v: number; enc: unknown };
    assert.equal(parsed.v, 2);
    assert.ok(isEncryptedPayload(parsed.enc));
    assert.equal((parsed.enc as { alg: string }).alg, 'aes-256-gcm');
  }
  // The plaintext payload never touches the disk bytes.
  assert.ok(!raw.includes(PAYLOAD_SECRET));
  assert.equal(log.stats().encrypted, true);

  // Transparent replay with the same key.
  const reopened = DurableTopicLog.open({ dir, encryptor: new AesGcmEncryptor(key) });
  assert.deepEqual(
    reopened.readSince('pay.events', 0).map((r) => r.payload),
    [{ pan: PAYLOAD_SECRET }, { pan: PAYLOAD_SECRET }],
  );
  assert.equal(reopened.lastSeq('pay.events'), 2);
});

test('DurableTopicLog: plaintext files from before encryption stay readable (mixed dir)', () => {
  const dir = freshDir();
  const plain = DurableTopicLog.open({ dir });
  plain.append({ seq: 1, topic: 't', at: 1000, payload: 'legacy' });

  const key = randomAes256Key();
  const sealed = DurableTopicLog.open({ dir, encryptor: new AesGcmEncryptor(key) });
  sealed.append({ seq: 2, topic: 't', at: 1001, payload: 'new' });

  const raw = readFileSync(join(dir, `${encodeURIComponent('t')}.log`), 'utf8');
  const lines = raw.split('\n').filter((l) => l.length > 0);
  assert.equal((JSON.parse(lines[0]) as { v: number }).v, 1); // legacy plaintext line
  assert.equal((JSON.parse(lines[1]) as { v: number }).v, 2); // new sealed line

  const reread = DurableTopicLog.open({ dir, encryptor: new AesGcmEncryptor(key) });
  assert.deepEqual(
    reread.readSince('t', 0).map((r) => r.payload),
    ['legacy', 'new'],
  );
});

test('DurableTopicLog: opening a sealed dir without an encryptor fails loudly', () => {
  const dir = freshDir();
  const key = randomAes256Key();
  const log = DurableTopicLog.open({ dir, encryptor: new AesGcmEncryptor(key) });
  log.append({ seq: 1, topic: 't', at: 1000, payload: 'secret' });

  // Fail-closed: never silently treat a sealed journal as empty/corrupt.
  assert.throws(() => DurableTopicLog.open({ dir }), /no encryptor is configured/);
});

test('DurableTopicLog: wrong key skips lines as corrupt, never garbage', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, encryptor: new AesGcmEncryptor(randomAes256Key()) });
  log.append({ seq: 1, topic: 't', at: 1000, payload: 'secret' });

  const reopened = DurableTopicLog.open({
    dir,
    encryptor: new AesGcmEncryptor(randomAes256Key()),
  });
  assert.equal(reopened.stats().corruptLines, 1);
  assert.deepEqual(reopened.readSince('t', 0), []);
  // Re-reads re-count (same as plaintext corrupt-line semantics).
  assert.equal(reopened.stats().corruptLines, 2);
});

test('DurableTopicLog: tampered sealed line is skipped and counted', () => {
  const dir = freshDir();
  const key = randomAes256Key();
  const log = DurableTopicLog.open({ dir, encryptor: new AesGcmEncryptor(key) });
  log.append({ seq: 1, topic: 't', at: 1000, payload: 'one' });
  log.append({ seq: 2, topic: 't', at: 1001, payload: 'two' });

  const file = join(dir, `${encodeURIComponent('t')}.log`);
  const lines = readFileSync(file, 'utf8').split('\n').filter((l) => l.length > 0);
  const tampered = JSON.parse(lines[0]) as { v: number; enc: { data: string } };
  tampered.enc.data = tampered.enc.data.slice(0, -4) + 'AAAA';
  lines[0] = JSON.stringify(tampered);
  writeFileSync(file, `${lines.join('\n')}\n`, 'utf8');

  const reopened = DurableTopicLog.open({ dir, encryptor: new AesGcmEncryptor(key) });
  assert.equal(reopened.stats().corruptLines, 1);
  assert.deepEqual(
    reopened.readSince('t', 0).map((r) => r.payload),
    ['two'],
  );
});

test('DurableTopicLog: invalid encryptor option is a RangeError', () => {
  assert.throws(
    () => DurableTopicLog.open({ dir: freshDir(), encryptor: {} as never }),
    RangeError,
  );
});

test('EventBus: publish-dedup journal is sealed and suppresses duplicates across restarts', async () => {
  const dir = freshDir();
  const key = randomAes256Key();
  const bus = new EventBus({ durableLogDir: dir, durableLogEncryptor: new AesGcmEncryptor(key) });
  const first = bus.publishIdempotent('pay.events', { pan: PAYLOAD_SECRET }, { messageId: 'm-1' });
  assert.equal(first.duplicate, false);

  const raw = readFileSync(join(dir, '__publish_dedup.jsonl'), 'utf8');
  assert.ok(!raw.includes(PAYLOAD_SECRET));
  assert.ok(!raw.includes('m-1'));
  for (const line of raw.split('\n').filter((l) => l.length > 0)) {
    const parsed = JSON.parse(line) as { v: number; enc: unknown };
    assert.equal(parsed.v, 2);
    assert.ok(isEncryptedPayload(parsed.enc));
  }

  // Restart with the same key: the within-window claim is still suppressed.
  const bus2 = new EventBus({ durableLogDir: dir, durableLogEncryptor: new AesGcmEncryptor(key) });
  const dup = bus2.publishIdempotent('pay.events', { pan: PAYLOAD_SECRET }, { messageId: 'm-1' });
  assert.equal(dup.duplicate, true);
  await flush();
});

test('EventBus: sealed replay across restarts (resumeFromSeq)', async () => {
  const dir = freshDir();
  const key = randomAes256Key();
  const bus = new EventBus({ durableLogDir: dir, durableLogEncryptor: new AesGcmEncryptor(key) });
  bus.publish('pay.events', { pan: PAYLOAD_SECRET });
  bus.publish('pay.events', { pan: PAYLOAD_SECRET });
  await flush();

  const bus2 = new EventBus({ durableLogDir: dir, durableLogEncryptor: new AesGcmEncryptor(key) });
  const seen: unknown[] = [];
  bus2.subscribe('pay.events', (msg) => {
    seen.push(msg.payload);
  }, { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(seen, [{ pan: PAYLOAD_SECRET }, { pan: PAYLOAD_SECRET }]);
  assert.equal(bus2.getStats().durableLog?.encrypted, true);
  await flush();
});

test('EventBus: sealed segment archives decrypt transparently on includeArchived reads', async () => {
  const dir = freshDir();
  const key = randomAes256Key();
  const log = DurableTopicLog.open({
    dir,
    encryptor: new AesGcmEncryptor(key),
    segmentRotation: { maxEntriesPerSegment: 2 },
  });
  log.append({ seq: 1, topic: 't', at: 1000, payload: { pan: PAYLOAD_SECRET } });
  log.append({ seq: 2, topic: 't', at: 1001, payload: { pan: PAYLOAD_SECRET } });
  // Wait for the background archive pass to gzip the closed segment.
  const prefix = `${encodeURIComponent('t')}.seg-`;
  await waitFor(
    () =>
      readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz')).length > 0,
  );
  const gzName = readdirSync(dir).filter(
    (f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz'),
  )[0];
  const archived = gunzipSync(readFileSync(join(dir, gzName))).toString('utf8');
  assert.ok(!archived.includes(PAYLOAD_SECRET));
  for (const line of archived.split('\n').filter((l) => l.length > 0)) {
    const parsed = JSON.parse(line) as { v: number; enc: unknown };
    assert.equal(parsed.v, 2);
    assert.ok(isEncryptedPayload(parsed.enc));
  }
  // Archived reads decrypt transparently.
  assert.deepEqual(
    log.readSince('t', 0, { includeArchived: true }).map((r) => r.payload),
    [{ pan: PAYLOAD_SECRET }, { pan: PAYLOAD_SECRET }],
  );
});

test('EventBus: durableLogEncryptor without durableLogDir is a RangeError', () => {
  assert.throws(
    () =>
      new EventBus({
        durableLogEncryptor: new AesGcmEncryptor(randomAes256Key()),
      }),
    /durableLogEncryptor requires EventBusOptions.durableLogDir/,
  );
});

test('EventBus: default (no encryptor) logs stay plaintext and stats report unencrypted', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  log.append({ seq: 1, topic: 't', at: 1000, payload: 'plain' });
  const raw = readFileSync(join(dir, `${encodeURIComponent('t')}.log`), 'utf8');
  assert.equal((JSON.parse(raw.trim()) as { v: number }).v, 1);
  assert.equal(log.stats().encrypted, false);

  const bus = new EventBus({ durableLogDir: dir });
  assert.equal(bus.getStats().durableLog?.encrypted, false);
});
