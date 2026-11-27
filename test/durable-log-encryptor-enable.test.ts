/**
 * Bus-level durable-log at-rest encryptor live enable (EB-568).
 *
 * `setDurableLogEncryptor(encryptor, actor?)` on a bus with a durable log
 * but no encryptor installs the rotating ring instead of refusing: new
 * appends seal from the next append onward while lines written before
 * the install in plaintext stay plaintext and still read, the install
 * fans out to namespace child logs, and the rotate gate opens. Only a
 * `RotatingDekEncryptor` can be installed live; the enable is live-only
 * (a restart restores the unconfigured state).
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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-log-encryptor-enable-'));

/** A ring with a single live key `k1` and the given grace window. */
const makeRing = (graceMs = 0) =>
  new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], { graceMs });

/** The bus's live root log. */
const logOf = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

/** The child log object of one namespace. */
const childLog = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!;

/** The raw on-disk lines of one topic's live log file. */
const rawLines = (dir: string, topic: string): string[] => {
  const raw = readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8');
  return raw.split('\n').filter((l) => l.length > 0);
};

/** The raw on-disk lines of one namespace child's topic file. */
const rawChildLines = (dir: string, prefix: string, topic: string): string[] => {
  const file = join(
    dir,
    'namespaces',
    encodeURIComponent(prefix),
    `${encodeURIComponent(`${prefix}/${topic}`)}.log`,
  );
  const raw = readFileSync(file, 'utf8');
  return raw.split('\n').filter((l) => l.length > 0);
};

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_encryptor_enabled' });

test('install opens the rotate gate, seals new appends, and audits previous null', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  assert.equal(bus.getDurableLogEncryptionKeyId(), undefined);
  assert.equal(bus.getStats().durableLog?.encrypted, false);
  bus.setDurableLogEncryptor(makeRing(), 'ops-oncall');
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k1');
  assert.equal(bus.getStats().durableLog?.encrypted, true);
  bus.publish('pay.events', { n: 1 });
  const lines = rawLines(dir, 'pay.events');
  assert.equal(lines.length, 1);
  const sealed = JSON.parse(lines[0]) as { v: number; enc: { keyId?: string } };
  assert.equal(sealed.v, 2);
  assert.equal(sealed.enc.keyId, 'k1');
  // The rotate gate opens with the install.
  const result = bus.rotateDurableLogEncryptionKey('k2', 'ops-oncall');
  assert.deepEqual(result, { keyId: 'k2', oldKeyId: 'k1' });
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k2');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { liveKeyId: 'k1', oldLiveKeyId: null });
  bus.close();
});

test('a non-ring encryptor refuses and the bus stays unconfigured until a ring installs', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  assert.throws(
    () =>
      bus.setDurableLogEncryptor(
        new AesGcmEncryptor(randomAes256Key()) as unknown as RotatingDekEncryptor,
      ),
    /must be a RotatingDekEncryptor/,
  );
  assert.throws(
    () =>
      bus.setDurableLogEncryptor(
        new NoopEncryptor() as unknown as RotatingDekEncryptor,
      ),
    /must be a RotatingDekEncryptor/,
  );
  assert.throws(
    () =>
      bus.setDurableLogEncryptor(
        'not-an-encryptor' as unknown as RotatingDekEncryptor,
      ),
    /must be a PayloadEncryptor/,
  );
  assert.equal(bus.getDurableLogEncryptionKeyId(), undefined);
  assert.equal(bus.getStats().durableLog?.encrypted, false);
  assert.equal(audit(bus).length, 0);
  // A failed call changes nothing: a later valid call still enables.
  bus.setDurableLogEncryptor(makeRing(), 'ops');
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k1');
  assert.equal(audit(bus).length, 1);
  bus.close();
});

test('a bus with no durable log refuses fail-closed', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.setDurableLogEncryptor(makeRing()),
    /requires EventBusOptions\.durableLogDir to be set/,
  );
  assert.equal(audit(bus).length, 0);
  bus.close();
});

test('an already-configured bus refuses reinstall', () => {
  // Constructed with a ring.
  const dir = freshDir();
  const constructed = new EventBus({
    durableLogDir: dir,
    durableLogEncryptor: makeRing(),
  });
  assert.throws(
    () => constructed.setDurableLogEncryptor(makeRing()),
    /already enabled/,
  );
  assert.equal(constructed.getDurableLogEncryptionKeyId(), 'k1');
  constructed.close();
  // Installed live.
  const installed = new EventBus({ durableLogDir: freshDir() });
  installed.setDurableLogEncryptor(makeRing());
  assert.throws(
    () => installed.setDurableLogEncryptor(makeRing()),
    /already enabled/,
  );
  assert.equal(installed.getDurableLogEncryptionKeyId(), 'k1');
  assert.equal(audit(installed).length, 1);
  installed.close();
});

test('old plaintext lines stay readable after install and a fresh bus with the ring reads both generations', () => {
  const dir = freshDir();
  const ring = makeRing();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('pay.events', { n: 1 });
  // The pre-install line is plaintext on disk.
  const before = rawLines(dir, 'pay.events');
  assert.equal(before.length, 1);
  assert.equal(JSON.parse(before[0]).v, 1);
  bus.setDurableLogEncryptor(ring);
  bus.publish('pay.events', { n: 2 });
  const lines = rawLines(dir, 'pay.events');
  assert.equal(lines.length, 2);
  assert.equal(JSON.parse(lines[0]).v, 1);
  const sealed = JSON.parse(lines[1]) as { v: number; enc: { keyId?: string } };
  assert.equal(sealed.v, 2);
  assert.equal(sealed.enc.keyId, 'k1');
  // The live log reads both generations.
  assert.deepEqual(
    logOf(bus).readSince('pay.events', 0).map((r) => r.payload),
    [{ n: 1 }, { n: 2 }],
  );
  // So does a fresh log opening the same directory with the same ring.
  const reopened = DurableTopicLog.open({ dir, encryptor: ring });
  assert.deepEqual(
    reopened.readSince('pay.events', 0).map((r) => r.payload),
    [{ n: 1 }, { n: 2 }],
  );
  bus.close();
});

test('a restart restores the unconfigured state', () => {
  const dir = freshDir();
  const first = new EventBus({ durableLogDir: dir });
  first.setDurableLogEncryptor(makeRing());
  assert.equal(first.getDurableLogEncryptionKeyId(), 'k1');
  first.close();
  // A new bus over the same directory starts unconfigured: the rotate
  // gate refuses until the encryptor is installed again.
  const bus = new EventBus({ durableLogDir: dir });
  assert.equal(bus.getDurableLogEncryptionKeyId(), undefined);
  assert.throws(() => bus.rotateDurableLogEncryptionKey(), RangeError);
  assert.equal(audit(bus).length, 0);
  // …and installing again works.
  bus.setDurableLogEncryptor(makeRing());
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k1');
  bus.close();
});

test('null is a silent no-op and a closed bus refuses', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogEncryptor(null);
  assert.equal(bus.getDurableLogEncryptionKeyId(), undefined);
  assert.equal(audit(bus).length, 0);
  // …and a later valid call still enables.
  bus.setDurableLogEncryptor(makeRing());
  assert.equal(audit(bus).length, 1);
  // `null` on an already-enabled bus is a no-op too, not a reinstall.
  bus.setDurableLogEncryptor(null);
  assert.equal(audit(bus).length, 1);
  assert.equal(bus.getDurableLogEncryptionKeyId(), 'k1');
  bus.close();

  const closed = new EventBus({ durableLogDir: freshDir() });
  closed.close();
  assert.throws(() => closed.setDurableLogEncryptor(makeRing()), /bus is closed/);
  assert.equal(audit(closed).length, 0);
});

test('install fans out to namespace child logs and a later namespace seals from the start', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 0);
  bus.setDurableLogEncryptor(makeRing());
  ns.publish('orders', 1);
  // The child's pre-install line stays plaintext, the post-install line
  // is sealed — and both read back through the child log.
  const lines = rawChildLines(dir, 't1', 'orders');
  assert.equal(lines.length, 2);
  assert.equal(JSON.parse(lines[0]).v, 1);
  assert.equal(JSON.parse(lines[1]).v, 2);
  assert.deepEqual(
    childLog(bus, 't1').readSince('t1/orders', 0).map((r) => r.payload),
    [0, 1],
  );
  assert.equal(childLog(bus, 't1').stats().encrypted, true);
  // A namespace registered after the install opens with the live ring.
  const later = bus.createNamespace('t2');
  later.publish('orders', 'a');
  const laterLines = rawChildLines(dir, 't2', 'orders');
  assert.equal(laterLines.length, 1);
  assert.equal(JSON.parse(laterLines[0]).v, 2);
  assert.equal(childLog(bus, 't2').stats().encrypted, true);
  bus.close();
});
