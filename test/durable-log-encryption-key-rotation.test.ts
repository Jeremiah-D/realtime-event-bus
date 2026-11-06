/**
 * Bus-level durable-log at-rest encryption DEK rotation (EB-398).
 *
 * `rotateDurableLogEncryptionKey(newKeyId?, actor?)` mints a fresh random
 * DEK in the `RotatingDekEncryptor` ring the bus was constructed with and
 * makes it live — without restarting the bus. New appends seal with the
 * new live key from the next append onward; old sealed lines keep their
 * envelope key id and are never re-sealed; namespace child logs share the
 * same ring reference so they follow automatically. The retired key keeps
 * decrypting inside the ring's live grace window — with the default grace
 * of 0 an old line fails loudly at recovery once the wall clock has moved
 * past the rotation, is counted in `corruptLines`, and is never returned
 * as garbage. A bus without a `durableLogEncryptor`, or whose encryptor
 * is not a rotating ring, refuses fail-closed. Every rotation mints a
 * fresh DEK, so there is no same-value no-op and every call is audited
 * as `durable_log_encryption_key_rotated` (`{ keyId, oldKeyId }` — key ids
 * are labels, never key material).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import {
  AesGcmEncryptor,
  NoopEncryptor,
  RotatingDekEncryptor,
  randomAes256Key,
} from '../src/encrypt.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-log-key-rot-'));

/** A ring with a single live key `k1` and the given grace window. */
const makeRing = (graceMs = 0) =>
  new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], { graceMs });

/** A bus whose durable log seals with `ring`. */
const makeBus = (dir: string, ring: RotatingDekEncryptor) =>
  new EventBus({ durableLogDir: dir, durableLogEncryptor: ring });

/** The bus's durable log (private field, reached the way the other durable-log revision tests reach it). */
const logOf = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

/** The envelope key id of every sealed line in a topic's live log file. */
const envelopeKeyIds = (dir: string, topic: string): (string | undefined)[] => {
  const raw = readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8');
  return raw
    .split('\n')
    .filter((l) => l.length > 0)
    .map((line) => {
      const parsed = JSON.parse(line) as { v: number; enc: { keyId?: string } };
      assert.equal(parsed.v, 2);
      return parsed.enc.keyId;
    });
};

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_encryption_key_rotated' });

test('rotation seals new appends with the new key id and both generations stay readable inside grace', () => {
  const dir = freshDir();
  const ring = makeRing(60_000);
  const bus = makeBus(dir, ring);
  const log = logOf(bus);
  log.append({ seq: 1, topic: 'pay.events', at: 1000, payload: { n: 1 } });

  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k1');
  const result = bus.rotateDurableLogEncryptionKey('k2', 'ops-oncall');
  assert.deepEqual(result, { keyId: 'k2', oldKeyId: 'k1' });
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k2');
  assert.equal(ring.liveKeyId, 'k2');

  log.append({ seq: 2, topic: 'pay.events', at: 1001, payload: { n: 2 } });
  // The old line keeps its envelope key id; the new line carries the new one.
  assert.deepEqual(envelopeKeyIds(dir, 'pay.events'), ['k1', 'k2']);
  // Inside the grace window the old line still opens — no history rewritten.
  assert.deepEqual(
    log.readSince('pay.events', 0).map((r) => r.payload),
    [{ n: 1 }, { n: 2 }],
  );
});

test('grace 0: old lines fail loudly at recovery after rotation and count corruptLines, never garbage', async () => {
  const dir = freshDir();
  const ring = makeRing(); // graceMs 0 — the default.
  const bus = makeBus(dir, ring);
  const log = logOf(bus);
  log.append({ seq: 1, topic: 'pay.events', at: 1000, payload: { n: 1 } });
  bus.rotateDurableLogEncryptionKey('k2');
  // Let the wall clock move past the retirement stamp: with grace 0 the
  // old line must fail on its next decrypt, not open on a same-ms tie.
  await sleep(5);
  log.append({ seq: 2, topic: 'pay.events', at: 1001, payload: { n: 2 } });

  const reopened = DurableTopicLog.open({ dir, encryptor: ring });
  assert.equal(reopened.stats().corruptLines, 1);
  assert.deepEqual(
    reopened.readSince('pay.events', 0).map((r) => r.payload),
    [{ n: 2 }],
  );
  assert.equal(reopened.lastSeq('pay.events'), 2);
});

test('unconfigured bus refuses rotation and the getter is undefined', () => {
  const bus = new EventBus({});
  assert.throws(
    () => bus.rotateDurableLogEncryptionKey('k2'),
    /durableLogEncryptor is not enabled on this bus/,
  );
  assert.equal(bus.getDurableLogEncryptionKeyId(), undefined);
  assert.equal(audit(bus).length, 0);
});

test('plain AesGcmEncryptor refuses rotation; the getter is undefined and sealing still works', () => {
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogEncryptor: new AesGcmEncryptor(randomAes256Key()),
  });
  assert.throws(
    () => bus.rotateDurableLogEncryptionKey('k2'),
    /not a RotatingDekEncryptor/,
  );
  assert.equal(bus.getDurableLogEncryptionKeyId(), undefined);
  assert.equal(audit(bus).length, 0);
  // The refusal changed nothing: the log still seals and replays.
  const log = logOf(bus);
  log.append({ seq: 1, topic: 't', at: 1000, payload: { n: 1 } });
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    [{ n: 1 }],
  );
});

test('NoopEncryptor refuses rotation; the getter is undefined', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogEncryptor: new NoopEncryptor() });
  assert.throws(
    () => bus.rotateDurableLogEncryptionKey('k2'),
    /not a RotatingDekEncryptor/,
  );
  assert.equal(bus.getDurableLogEncryptionKeyId(), undefined);
  assert.equal(audit(bus).length, 0);
});

test('duplicate key id refuses and the live key does not move', () => {
  const dir = freshDir();
  const ring = makeRing(60_000);
  const bus = makeBus(dir, ring);
  assert.throws(() => bus.rotateDurableLogEncryptionKey('k1'), RangeError);
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k1');
  assert.equal(ring.liveKeyId, 'k1');
  assert.equal(audit(bus).length, 0);
  // The failed call changed nothing: the next append still seals with k1.
  logOf(bus).append({ seq: 1, topic: 't', at: 1000, payload: { n: 1 } });
  assert.deepEqual(envelopeKeyIds(dir, 't'), ['k1']);
});

test('illegal key id refuses and the live key does not move', () => {
  const dir = freshDir();
  const ring = makeRing(60_000);
  const bus = makeBus(dir, ring);
  for (const bad of ['', 'bad id!', 'x'.repeat(65), 7]) {
    assert.throws(() => bus.rotateDurableLogEncryptionKey(bad as never), RangeError);
  }
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k1');
  assert.equal(ring.liveKeyId, 'k1');
  assert.equal(audit(bus).length, 0);
});

test('omitted key id takes the smallest free k<N>', () => {
  const dir = freshDir();
  const ring = makeRing(60_000);
  const bus = makeBus(dir, ring);
  assert.equal(bus.rotateDurableLogEncryptionKey().keyId, 'k2');
  assert.equal(bus.rotateDurableLogEncryptionKey().keyId, 'k3');
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k3');

  // A ring that already holds k2 (retired at construction) skips it.
  const dir2 = freshDir();
  const ring2 = new RotatingDekEncryptor(
    [
      { keyId: 'k1', key: randomAes256Key() },
      { keyId: 'k2', key: randomAes256Key() },
    ],
    { graceMs: 60_000 },
  );
  const bus2 = makeBus(dir2, ring2);
  assert.equal(bus2.rotateDurableLogEncryptionKey().keyId, 'k3');
});

test('rotation audits the exact key ids and the getter follows the live key', () => {
  const dir = freshDir();
  const ring = makeRing(60_000);
  const bus = makeBus(dir, ring);
  const first = bus.rotateDurableLogEncryptionKey('k9', 'ops-oncall');
  assert.deepEqual(first, { keyId: 'k9', oldKeyId: 'k1' });
  assert.equal(audit(bus).length, 1);
  assert.equal(audit(bus)[0].actor, 'ops-oncall');
  assert.deepEqual(audit(bus)[0].detail, { keyId: 'k9', oldKeyId: 'k1' });
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k9');

  const second = bus.rotateDurableLogEncryptionKey('k10');
  assert.deepEqual(second, { keyId: 'k10', oldKeyId: 'k9' });
  assert.equal(audit(bus).length, 2);
  assert.deepEqual(audit(bus)[1].detail, { keyId: 'k10', oldKeyId: 'k9' });
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k10');
});

test('namespace child logs seal with the new key after rotation', async () => {
  const dir = freshDir();
  const ring = makeRing(60_000);
  const bus = makeBus(dir, ring);
  bus.createNamespace('tenant-a', { allowPublish: true, allowSubscribe: true });
  bus.publish('t', { n: 1 }, { namespace: 'tenant-a' });
  await flush();
  bus.rotateDurableLogEncryptionKey('k2');
  bus.publish('t', { n: 2 }, { namespace: 'tenant-a' });
  await flush();

  const childDir = join(dir, 'namespaces', 'tenant-a');
  // The child log stores namespaced topics under `tenant-a/t`.
  assert.deepEqual(envelopeKeyIds(childDir, 'tenant-a/t'), ['k1', 'k2']);
  // The child log reopens with the same ring and both generations read.
  const reopened = DurableTopicLog.open({ dir: childDir, encryptor: ring });
  assert.deepEqual(
    reopened.readSince('tenant-a/t', 0).map((r) => r.payload),
    [{ n: 1 }, { n: 2 }],
  );
});

test('rotation does not disturb live subscriptions or replay position', async () => {
  const dir = freshDir();
  const ring = makeRing(60_000);
  const bus = makeBus(dir, ring);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', { n: 1 });
  await flush();
  assert.deepEqual(received, [{ n: 1 }]);

  bus.rotateDurableLogEncryptionKey('k2');
  bus.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(received, [{ n: 1 }, { n: 2 }]);
  // Replay from the log still returns both records, in order.
  assert.deepEqual(
    logOf(bus)
      .readSince('t', 0)
      .map((r) => r.payload),
    [{ n: 1 }, { n: 2 }],
  );
  assert.deepEqual(envelopeKeyIds(dir, 't'), ['k1', 'k2']);
});
