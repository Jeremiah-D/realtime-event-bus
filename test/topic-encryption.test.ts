import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import {
  RotatingDekEncryptor,
  isEncryptedPayload,
  randomAes256Key,
} from '../src/encrypt.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-topicenc-'));

const PAYLOAD_SECRET = 'sk-live-9f2c41';

test('ring: rotate keeps old key decrypting inside the grace window', () => {
  const ring = new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], {
    graceMs: 1000,
    nowMs: 1000,
  });
  const sealed1 = ring.encrypt(Buffer.from('v1'));
  assert.equal(sealed1.keyId, 'k1');
  const k2 = ring.rotate(undefined, 2000);
  assert.equal(k2, 'k2');
  assert.equal(ring.liveKeyId, 'k2');
  const sealed2 = ring.encrypt(Buffer.from('v2'));
  assert.equal(sealed2.keyId, 'k2');
  // Old key still opens inside grace.
  assert.deepEqual(ring.decrypt(sealed1, 2500), Buffer.from('v1'));
  // Past grace it fails loudly.
  assert.throws(() => ring.decrypt(sealed1, 3001), /grace window elapsed/);
  // New key always works.
  assert.deepEqual(ring.decrypt(sealed2, 3001), Buffer.from('v2'));
  assert.deepEqual(ring.keyIds(), ['k2', 'k1']);
  assert.equal(ring.pruneRetiredKeys(3001), 1);
  assert.deepEqual(ring.keyIds(), ['k2']);
  assert.throws(() => ring.decrypt(sealed1, 3001), /unknown key id/);
});

test('ring: unknown keyId and keyless envelopes fail loudly, never guess', () => {
  const ring = new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }]);
  const sealed = ring.encrypt(Buffer.from('x'));
  assert.throws(
    () => ring.decrypt({ ...sealed, keyId: 'k9' } as never),
    /unknown key id "k9"/,
  );
  const { keyId: _dropped, ...keyless } = sealed;
  assert.throws(() => ring.decrypt(keyless as never), /no keyId/);
  // Tampered ciphertext fails loudly.
  const tampered = { ...sealed, data: sealed.data.slice(0, -4) + 'AAAA' };
  assert.throws(() => ring.decrypt(tampered), /decrypt failed/);
});

test('ring: validation is fail-fast', () => {
  assert.throws(() => new RotatingDekEncryptor([]), RangeError);
  assert.throws(
    () => new RotatingDekEncryptor([{ keyId: 'k1', key: Buffer.alloc(16) }]),
    RangeError,
  );
  assert.throws(
    () =>
      new RotatingDekEncryptor([
        { keyId: 'k1', key: randomAes256Key() },
        { keyId: 'k1', key: randomAes256Key() },
      ]),
    /duplicate keyId/,
  );
  assert.throws(
    () => new RotatingDekEncryptor([{ keyId: 'bad id!', key: randomAes256Key() }]),
    RangeError,
  );
  assert.throws(() => new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], { graceMs: -1 }), RangeError);
  const ring = new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }]);
  assert.throws(() => ring.rotate('k1'), /already held/);
  assert.throws(() => ring.rotate('bad id!'), RangeError);
  assert.throws(() => ring.rekey([]), RangeError);
});

test('ring: rekey follows the signing rotation-stamping convention', () => {
  const ring = new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], {
    graceMs: 1000,
    nowMs: 1000,
  });
  const sealed1 = ring.encrypt(Buffer.from('v1'));
  // Re-setting with a new live key retires k1 as of now; the old stamp
  // is kept when k1 is re-listed (never extended).
  ring.rekey(
    [
      { keyId: 'k2', key: randomAes256Key() },
      { keyId: 'k1', key: randomAes256Key() }, // key material ignored: k1 already held
    ],
    2000,
  );
  assert.equal(ring.liveKeyId, 'k2');
  assert.deepEqual(ring.decrypt(sealed1, 2500), Buffer.from('v1'));
  assert.throws(() => ring.decrypt(sealed1, 3001), /grace window elapsed/);
  // The re-listed k1 kept its original retiredAtMs (2000), not a fresh one.
  ring.rekey([{ keyId: 'k2', key: randomAes256Key() }], 2900);
  assert.throws(() => ring.decrypt(sealed1, 3001), /grace window elapsed/);
  // A dropped-and-relisted key promotes back to live.
  ring.rekey([{ keyId: 'k1', key: randomAes256Key() }], 4000);
  assert.equal(ring.liveKeyId, 'k1');
  const sealed3 = ring.encrypt(Buffer.from('v3'));
  assert.deepEqual(ring.decrypt(sealed3, 4000), Buffer.from('v3'));
});

test('bus: sealed publish delivers plaintext transparently; log holds ciphertext', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  try {
    bus.setTopicEncryptionKey('pay.*', { keys: [{ keyId: 'k1', key: randomAes256Key() }] });
    const seen: unknown[] = [];
    bus.subscribe('pay.events', (msg) => seen.push(msg.payload));
    bus.publish('pay.events', { pan: PAYLOAD_SECRET, amount: 100 });
    bus.publish('other.topic', { pan: PAYLOAD_SECRET });
    await flush();
    await flush();
    assert.deepEqual(seen, [{ pan: PAYLOAD_SECRET, amount: 100 }]);
    // The durable log line for the sealed topic carries the envelope —
    // ciphertext at rest, never the plaintext.
    const sealedPath = join(dir, `${encodeURIComponent('pay.events')}.log`);
    const raw = readFileSync(sealedPath, 'utf8');
    assert.ok(!raw.includes(PAYLOAD_SECRET), 'plaintext must not touch disk');
    const lines = raw.split('\n').filter((l) => l.length > 0);
    const sealedLine = lines
      .map((l) => JSON.parse(l) as { payload?: unknown })
      .find((p) => isEncryptedPayload(p.payload));
    assert.ok(sealedLine, 'expected a sealed envelope on disk');
    assert.equal((sealedLine!.payload as { keyId: string }).keyId, 'k1');
    // Stats: per-topic + global.
    const stats = bus.getStats();
    assert.equal(stats.encryptedMessages, 1);
    const row = stats.topics.find((t) => t.topic === 'pay.events');
    assert.equal(row?.encryptedMessages, 1);
    assert.equal(row?.decryptFailures, 0);
  } finally {
    await bus.close();
  }
});

test('bus: rotateTopicKey re-keys live traffic; old messages still open in grace', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => 1_000_000 });
  try {
    bus.setTopicEncryptionKey('pay.*', {
      keys: [{ keyId: 'k1', key: randomAes256Key() }],
      graceMs: 60_000,
    });
    const seen: unknown[] = [];
    bus.subscribe('pay.events', (msg) => seen.push(msg.payload));
    bus.publish('pay.events', { v: 1 });
    await flush();
    const newKeyId = bus.rotateTopicKey('pay.*');
    assert.equal(newKeyId, 'k2');
    bus.publish('pay.events', { v: 2 });
    await flush();
    await flush();
    assert.deepEqual(seen, [{ v: 1 }, { v: 2 }]);
    // The audit trail records the rotation with both key ids.
    const events = bus.getSubscriptionAudit({ action: 'topic_key_rotated' });
    assert.equal(events.length, 1);
    assert.equal(events[0].topic, 'pay.*');
    assert.deepEqual(events[0].detail, {
      oldKeyId: 'k1',
      newKeyId: 'k2',
      rotatedAtMs: 1_000_000,
    });
    // New messages seal with the new key id on disk.
    const raw = readFileSync(join(dir, `${encodeURIComponent('pay.events')}.log`), 'utf8');
    const keyIds = raw
      .split('\n')
      .filter((l) => l.length > 0)
      .map((l) => (JSON.parse(l) as { payload?: { keyId?: string } }).payload?.keyId)
      .filter(Boolean);
    assert.deepEqual(keyIds, ['k1', 'k2']);
    // Stats expose the live key id (never the key material).
    const row = bus.getStats().topics.find((t) => t.topic === 'pay.events');
    assert.deepEqual(row?.encryption, { keyIds: ['k2', 'k1'], liveKeyId: 'k2', graceMs: 60_000 });
  } finally {
    await bus.close();
  }
});

test('bus: rotateTopicKey on an unregistered pattern throws RangeError', async () => {
  const bus = new EventBus();
  try {
    assert.throws(() => bus.rotateTopicKey('nope.*'), RangeError);
    bus.setTopicEncryptionKey('pay.*', { keys: [{ keyId: 'k1', key: randomAes256Key() }] });
    // Rotation targets the registered rule, not a concrete topic it matches.
    assert.throws(() => bus.rotateTopicKey('pay.events'), RangeError);
    assert.equal(bus.clearTopicEncryption('pay.*'), true);
    assert.equal(bus.clearTopicEncryption('pay.*'), false);
  } finally {
    await bus.close();
  }
});

test('bus: setTopicEncryptionKey validation is fail-fast', async () => {
  const bus = new EventBus();
  try {
    assert.throws(() => bus.setTopicEncryptionKey('', { keys: [{ keyId: 'k1', key: randomAes256Key() }] }), RangeError);
    assert.throws(() => bus.setTopicEncryptionKey('t', { keys: [] }), RangeError);
    assert.throws(
      () => bus.setTopicEncryptionKey('t', { keys: [{ keyId: 'k1', key: Buffer.alloc(8) }] }),
      RangeError,
    );
    assert.throws(
      () =>
        bus.setTopicEncryptionKey('t', {
          keys: [
            { keyId: 'k1', key: randomAes256Key() },
            { keyId: 'k1', key: randomAes256Key() },
          ],
        }),
      /duplicate/,
    );
    assert.throws(
      () => bus.setTopicEncryptionKey('t', { keys: [{ keyId: 'k1', key: randomAes256Key() }], graceMs: -1 }),
      RangeError,
    );
  } finally {
    await bus.close();
  }
});

test('bus: exact-topic rule wins over pattern; earliest pattern wins', async () => {
  const bus = new EventBus();
  try {
    bus.setTopicEncryptionKey('pay.*', { keys: [{ keyId: 'pattern', key: randomAes256Key() }] });
    bus.setTopicEncryptionKey('pay.events', { keys: [{ keyId: 'exact', key: randomAes256Key() }] });
    const seen: unknown[] = [];
    bus.subscribe('pay.events', (msg) => seen.push(msg.payload));
    bus.publish('pay.events', { v: 1 });
    await flush();
    await flush();
    assert.deepEqual(seen, [{ v: 1 }]);
    const row = bus.getStats().topics.find((t) => t.topic === 'pay.events');
    assert.equal(row?.encryption?.liveKeyId, 'exact');
  } finally {
    await bus.close();
  }
});

test('bus: decrypt failure drops fail-closed — counted, audited, never delivered', async () => {
  const bus = new EventBus();
  try {
    bus.setTopicEncryptionKey('pay.*', { keys: [{ keyId: 'k1', key: randomAes256Key() }] });
    const seen: unknown[] = [];
    bus.subscribe('pay.events', (msg) => seen.push(msg.payload));
    bus.publish('pay.events', { v: 1 });
    // The sealed message is already queued; the key vanishes before the
    // drain runs — delivery must drop it fail-closed, never as ciphertext.
    bus.clearTopicEncryption('pay.*');
    await flush();
    await flush();
    assert.deepEqual(seen, [], 'undecryptable message must not reach the handler');
    const stats = bus.getStats();
    assert.equal(stats.decryptFailures, 1);
    assert.equal(stats.topics.find((t) => t.topic === 'pay.events')?.decryptFailures, 1);
    const audit = bus.getSubscriptionAudit({ action: 'decrypt_failed' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].topic, 'pay.events');
    assert.equal(audit[0].detail?.keyId, 'k1');
    assert.equal(audit[0].detail?.seq, 1);
  } finally {
    await bus.close();
  }
});

test('bus: replay of a sealed log decrypts transparently after resubscribe', async () => {
  const dir = freshDir();
  const key = randomAes256Key();
  {
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicEncryptionKey('pay.*', { keys: [{ keyId: 'k1', key }] });
    bus.publish('pay.events', { v: 1 });
    bus.publish('pay.events', { v: 2 });
    await bus.close();
  }
  // A new bus with the same DEK replays the sealed log transparently.
  const bus2 = new EventBus({ durableLogDir: dir });
  try {
    bus2.setTopicEncryptionKey('pay.*', { keys: [{ keyId: 'k1', key }] });
    const seen: unknown[] = [];
    bus2.subscribe('pay.events', (msg) => seen.push(msg.payload), { resumeFromSeq: 0 });
    await flush();
    await flush();
    assert.deepEqual(seen, [{ v: 1 }, { v: 2 }]);
    assert.equal(bus2.getStats().decryptFailures, 0);
  } finally {
    await bus2.close();
  }
  // A bus WITHOUT the key drops the replayed messages fail-closed.
  const bus3 = new EventBus({ durableLogDir: dir });
  try {
    const seen: unknown[] = [];
    bus3.subscribe('pay.events', (msg) => seen.push(msg.payload), { resumeFromSeq: 0 });
    await flush();
    await flush();
    assert.deepEqual(seen, [], 'keyless bus must not deliver sealed messages');
    assert.equal(bus3.getStats().decryptFailures, 2);
    const audit = bus3.getSubscriptionAudit({ action: 'decrypt_failed' });
    assert.equal(audit.length, 2);
    assert.equal(audit[0].detail?.keyId, 'k1');
  } finally {
    await bus3.close();
  }
});

test('bus: encryption composes with compression (seal-then-encrypt order)', async () => {
  const bus = new EventBus();
  try {
    bus.setTopicCompression('big.*', { thresholdBytes: 16 });
    bus.setTopicEncryptionKey('big.*', { keys: [{ keyId: 'k1', key: randomAes256Key() }] });
    const seen: unknown[] = [];
    bus.subscribe('big.events', (msg) => seen.push(msg.payload));
    const big = { text: 'x'.repeat(500) };
    bus.publish('big.events', big);
    await flush();
    await flush();
    assert.deepEqual(seen, [big]);
    const stats = bus.getStats();
    assert.equal(stats.encryptedMessages, 1);
    const row = stats.topics.find((t) => t.topic === 'big.events');
    assert.equal(row?.encryptedMessages, 1);
    assert.ok((row?.compressedMessages ?? 0) >= 1, 'compression still ran underneath');
  } finally {
    await bus.close();
  }
});

test('bus: encryption composes with per-topic signing (encrypt-then-sign)', async () => {
  const bus = new EventBus();
  try {
    bus.setTopicEncryptionKey('sec.*', { keys: [{ keyId: 'k1', key: randomAes256Key() }] });
    bus.setTopicSigningKey('sec.*', { keys: [{ kid: 's1', secret: 'top-secret' }] });
    const seen: unknown[] = [];
    bus.subscribe('sec.events', (msg) => seen.push(msg.payload), { verifySignature: true });
    bus.publish('sec.events', { v: 42 });
    await flush();
    await flush();
    assert.deepEqual(seen, [{ v: 42 }]);
    assert.equal(bus.getStats().topics.find((t) => t.topic === 'sec.events')?.signatureFailures ?? 0, 0);
  } finally {
    await bus.close();
  }
});

test('bus: binary payloads seal to raw bytes and back', async () => {
  const bus = new EventBus();
  try {
    bus.setTopicEncryptionKey('bin.*', { keys: [{ keyId: 'k1', key: randomAes256Key() }] });
    const seen: unknown[] = [];
    bus.subscribe('bin.events', (msg) => seen.push(msg.payload));
    const bytes = new Uint8Array([1, 2, 3, 250, 0, 255]);
    bus.publish('bin.events', bytes, { binary: true } as never);
    await flush();
    await flush();
    assert.equal(seen.length, 1);
    assert.ok(seen[0] instanceof Uint8Array);
    assert.deepEqual(Array.from(seen[0] as Uint8Array), [1, 2, 3, 250, 0, 255]);
  } finally {
    await bus.close();
  }
});
