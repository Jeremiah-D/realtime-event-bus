/**
 * Bus-level publish audit journal directory live migration (EB-422).
 *
 * `setPublishAuditDir` moves the directory the journal is built under
 * without restarting the bus. A fresh `PublishAuditLog` is built under
 * the new directory with the live journal's current `maxEntries` /
 * `maxBytes` and the bus clock, and the bus reference is swapped to it
 * — the new journal governs from the next record onward, its
 * constructor `recover()` back-fills entries already in the new
 * directory, and the old directory's files are left exactly as they
 * are (never deleted). Migrating to the current directory is a no-op
 * with no audit. Audited as `publish_audit_dir_revised`
 * (`{ oldDir, dir }`, `oldDir` the construction-time `durableLogDir`
 * on the first migration).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { PUBLISH_AUDIT_LIVE_NAME } from '../src/publishaudit.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 5_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const tmpDir = (prefix: string): string => mkdtempSync(join(tmpdir(), prefix));

/** A bus with the publish audit journal enabled under `dir`. */
function auditedBus(dir: string, extra: Record<string, unknown> = {}): EventBus {
  return new EventBus({
    durableLogDir: dir,
    publishAudit: true,
    now: controllableClock().now,
    ...extra,
  });
}

/** Publish `n` admitted messages on `topic`, then flush. */
async function publishN(bus: EventBus, topic: string, n: number): Promise<void> {
  for (let i = 0; i < n; i += 1) {
    bus.publish(topic, { n: i });
  }
  await flush();
}

/** Raw lines currently in the live journal file; [] when absent. */
function liveLines(dir: string): string[] {
  const file = join(dir, PUBLISH_AUDIT_LIVE_NAME);
  if (!existsSync(file)) return [];
  return readFileSync(file, 'utf8')
    .split('\n')
    .filter((line) => line.trim() !== '');
}

/** Archived (rotated) files currently in the directory. */
function archivedFiles(dir: string): string[] {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter(
    (f) => f.startsWith('__publish_audit.') && f !== PUBLISH_AUDIT_LIVE_NAME,
  );
}

const dirRevised = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'publish_audit_dir_revised' });

test('setPublishAuditDir fails closed when publishAudit is not enabled', () => {
  const plain = new EventBus();
  assert.throws(() => plain.setPublishAuditDir(tmpDir('eb422-')), RangeError);
  assert.equal(dirRevised(plain).length, 0);
  // A durable log directory alone is not enough: the gate is publishAudit.
  const dir = tmpDir('eb422-');
  const logged = new EventBus({ durableLogDir: dir });
  assert.throws(() => logged.setPublishAuditDir(tmpDir('eb422-')), RangeError);
  assert.equal(dirRevised(logged).length, 0);
  plain.close();
  logged.close();
});

test('setPublishAuditDir rejects an illegal actor and leaves the live journal untouched', async () => {
  const dir = tmpDir('eb422-');
  const bus = auditedBus(dir);
  await publishN(bus, 'orders.created', 1);
  const before = bus.getPublishAudit();
  for (const badActor of ['', 42, null]) {
    assert.throws(() => bus.setPublishAuditDir(tmpDir('eb422-'), badActor as never), RangeError);
  }
  assert.deepEqual(bus.getPublishAudit(), before);
  assert.equal(liveLines(dir).length, 1);
  assert.equal(dirRevised(bus).length, 0);
  bus.close();
});

test('setPublishAuditDir rejects a closed bus', async () => {
  const dir = tmpDir('eb422-');
  const bus = auditedBus(dir);
  await publishN(bus, 'orders.created', 1);
  bus.close();
  assert.throws(() => bus.setPublishAuditDir(tmpDir('eb422-')), RangeError);
});

test('setPublishAuditDir rejects an empty or non-string dir and leaves the live journal untouched', async () => {
  const dir = tmpDir('eb422-');
  const bus = auditedBus(dir);
  await publishN(bus, 'orders.created', 2);
  const beforeEntries = bus.getPublishAudit();
  const beforeLines = liveLines(dir);
  for (const badDir of ['', 42, null, undefined]) {
    assert.throws(() => bus.setPublishAuditDir(badDir as never), RangeError);
  }
  assert.deepEqual(bus.getPublishAudit(), beforeEntries);
  assert.deepEqual(liveLines(dir), beforeLines);
  assert.equal(dirRevised(bus).length, 0);
  bus.close();
});

test('migrating to the current directory is a no-op with no audit', async () => {
  const dir = tmpDir('eb422-');
  const bus = auditedBus(dir);
  await publishN(bus, 'orders.created', 1);
  bus.setPublishAuditDir(dir);
  assert.equal(dirRevised(bus).length, 0);
  assert.equal(bus.getPublishAudit().length, 1);
  assert.equal(liveLines(dir).length, 1);
  bus.close();
});

test('migration to an empty directory: new records land there, old entries leave the read view', async () => {
  const oldDir = tmpDir('eb422-old-');
  const newDir = tmpDir('eb422-new-');
  const bus = auditedBus(oldDir);
  await publishN(bus, 'orders.created', 2);
  assert.equal(bus.getPublishAudit().length, 2);
  const oldLinesBefore = liveLines(oldDir);

  bus.setPublishAuditDir(newDir, 'ops');
  // The read view is the new journal's (empty) recovered view now.
  assert.deepEqual(bus.getPublishAudit(), []);
  assert.deepEqual(dirRevised(bus)[0].detail, { oldDir, dir: newDir });
  assert.equal(dirRevised(bus)[0].actor, 'ops');

  await publishN(bus, 'orders.paid', 2);
  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 2);
  assert.ok(entries.every((e) => e.topic === 'orders.paid'));
  assert.equal(liveLines(newDir).length, 2);
  assert.deepEqual(bus.verifyPublishAuditChain(), { ok: true, checked: 2 });
  // The old directory's files are untouched (never deleted, never rewritten).
  assert.deepEqual(liveLines(oldDir), oldLinesBefore);
  bus.close();
});

test('migration to a directory with journal history recovers those entries and the chain verifies', async () => {
  const newDir = tmpDir('eb422-new-');
  // Seed the new directory with two journaled admissions first.
  const seeder = auditedBus(newDir);
  await publishN(seeder, 'orders.created', 2);
  seeder.close();
  assert.equal(liveLines(newDir).length, 2);

  const oldDir = tmpDir('eb422-old-');
  const bus = auditedBus(oldDir);
  await publishN(bus, 'orders.paid', 1);
  assert.equal(bus.getPublishAudit().length, 1);

  bus.setPublishAuditDir(newDir);
  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 2);
  assert.ok(entries.every((e) => e.topic === 'orders.created'));
  // The chain continues from the recovered history.
  assert.deepEqual(bus.verifyPublishAuditChain(), { ok: true, checked: 2 });
  // A record after the migration links onto the recovered head.
  await publishN(bus, 'orders.refunded', 1);
  assert.equal(bus.getPublishAudit().length, 3);
  assert.deepEqual(bus.verifyPublishAuditChain(), { ok: true, checked: 3 });
  assert.equal(liveLines(newDir).length, 3);
  bus.close();
});

test('migration carries the live maxEntries and maxBytes forward', async () => {
  const oldDir = tmpDir('eb422-old-');
  const newDir = tmpDir('eb422-new-');
  // A tiny in-memory bound (2) and a tiny byte budget (10 bytes: any
  // second line in the same live file exceeds it, forcing a rotation).
  const bus = auditedBus(oldDir, {
    publishAudit: { maxEntries: 2, maxBytes: 10 },
  });
  await publishN(bus, 'orders.created', 2);
  bus.setPublishAuditDir(newDir);
  await publishN(bus, 'orders.paid', 3);
  // The carried maxEntries still bounds the read view.
  assert.equal(bus.getPublishAudit().length, 2);
  // The carried maxBytes still rotates the live file in the new dir:
  // the 2nd and 3rd post-migration records each rotate the live file
  // (every line far exceeds the 10-byte budget), leaving two archives
  // and one live line.
  assert.equal(archivedFiles(newDir).length, 2);
  assert.equal(liveLines(newDir).length, 1);
  bus.close();
});

test('two consecutive migrations both take effect with chained audit', async () => {
  const dirA = tmpDir('eb422-a-');
  const dirB = tmpDir('eb422-b-');
  const dirC = tmpDir('eb422-c-');
  const bus = auditedBus(dirA);
  await publishN(bus, 'orders.created', 1);
  bus.setPublishAuditDir(dirB);
  await publishN(bus, 'orders.paid', 1);
  assert.equal(liveLines(dirB).length, 1);
  bus.setPublishAuditDir(dirC);
  await publishN(bus, 'orders.refunded', 1);
  assert.equal(liveLines(dirC).length, 1);
  assert.equal(bus.getPublishAudit().length, 1);
  assert.equal(bus.getPublishAudit()[0].topic, 'orders.refunded');
  assert.deepEqual(
    dirRevised(bus).map((e) => e.detail),
    [
      { oldDir: dirA, dir: dirB },
      { oldDir: dirB, dir: dirC },
    ],
  );
  bus.close();
});

test('setPublishAudit still retunes the migrated journal', async () => {
  const oldDir = tmpDir('eb422-old-');
  const newDir = tmpDir('eb422-new-');
  const bus = auditedBus(oldDir, { publishAudit: { maxEntries: 2 } });
  bus.setPublishAuditDir(newDir);
  bus.setPublishAudit({ maxEntries: 5 });
  await publishN(bus, 'orders.created', 4);
  // The retune reached the new journal, not an orphaned old one.
  assert.equal(bus.getPublishAudit().length, 4);
  assert.equal(liveLines(newDir).length, 4);
  assert.equal(liveLines(oldDir).length, 0);
  bus.close();
});

test('old directory files are byte-identical after migration and later publishes', async () => {
  const oldDir = tmpDir('eb422-old-');
  const newDir = tmpDir('eb422-new-');
  const bus = auditedBus(oldDir);
  await publishN(bus, 'orders.created', 2);
  const before = readFileSync(join(oldDir, PUBLISH_AUDIT_LIVE_NAME), 'utf8');
  bus.setPublishAuditDir(newDir);
  await publishN(bus, 'orders.paid', 3);
  assert.equal(readFileSync(join(oldDir, PUBLISH_AUDIT_LIVE_NAME), 'utf8'), before);
  // No new publish-audit file appeared in the old directory (the
  // durable topic logs also live here, so filter to the journal).
  assert.deepEqual(
    readdirSync(oldDir).filter((f) => f.startsWith('__publish_audit')),
    [PUBLISH_AUDIT_LIVE_NAME],
  );
  bus.close();
});
