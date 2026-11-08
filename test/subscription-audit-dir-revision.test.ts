/**
 * Bus-level subscription audit disk-sink directory live migration (EB-423).
 *
 * `setSubscriptionAuditDir` moves the directory future appends land
 * under without restarting the bus — deliberately unlike the journal
 * migrations (EB-421/EB-422), it never re-recovers and never replaces
 * the read view: the in-memory index *is* the live log, and
 * re-running `recover()` mid-life would fork the EB-198 chain. The
 * index and the chain head stay exactly as they are, entries recorded
 * before the move stay queryable, files already in the new directory
 * are *not* back-filled into the live view (visible only after a
 * restart), and the old directory simply stops receiving new appends.
 * A bus built without a directory gains disk landing on the first
 * migration. Migrating to the current directory is a no-op with no
 * audit. Audited as `subscription_audit_dir_revised`
 * (`{ oldDir, dir }`, `oldDir` `null` when the bus was built without
 * a `subscriptionAuditDir`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 5_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const tmpDir = (prefix: string): string => mkdtempSync(join(tmpdir(), prefix));

/** The daily audit file name for the fixed test clock (1970-01-01 UTC). */
const DAY_FILE = 'audit-1970-01-01.jsonl';

/** A bus with disk landing under `dir`. */
function landedBus(dir: string, extra: Record<string, unknown> = {}): EventBus {
  return new EventBus({ subscriptionAuditDir: dir, now: controllableClock().now, ...extra });
}

/** A bus with no disk landing. */
function memoryBus(extra: Record<string, unknown> = {}): EventBus {
  return new EventBus({ now: controllableClock().now, ...extra });
}

/** Subscribe `n` distinct topics (each audits one `subscribe` event). */
function subscribeN(bus: EventBus, n: number, prefix = 't'): void {
  for (let i = 0; i < n; i += 1) {
    bus.subscribe(`${prefix}.${i}`, () => {});
  }
}

/** Raw lines currently in the daily audit file; [] when absent. */
function auditLines(dir: string): string[] {
  const file = join(dir, DAY_FILE);
  if (!existsSync(file)) return [];
  return readFileSync(file, 'utf8')
    .split('\n')
    .filter((line) => line.trim() !== '');
}

/** Parsed actions currently in the daily audit file. */
function auditFileActions(dir: string): string[] {
  return auditLines(dir).map((line) => (JSON.parse(line) as { action: string }).action);
}

const dirRevised = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'subscription_audit_dir_revised' });

test('first set on a bus built without a dir enables disk landing with oldDir null', () => {
  const newDir = tmpDir('eb423-new-');
  const bus = memoryBus();
  subscribeN(bus, 1);
  // Nothing on disk yet: the log is memory-only.
  assert.equal(auditLines(newDir).length, 0);

  bus.setSubscriptionAuditDir(newDir, 'ops');
  assert.deepEqual(dirRevised(bus)[0].detail, { oldDir: null, dir: newDir });
  assert.equal(dirRevised(bus)[0].actor, 'ops');
  // The migration event itself is the first append under the new dir.
  assert.deepEqual(auditFileActions(newDir), ['subscription_audit_dir_revised']);

  subscribeN(bus, 1, 'u');
  assert.deepEqual(auditFileActions(newDir), ['subscription_audit_dir_revised', 'subscribe']);
  assert.equal(bus.getSubscriptionAudit().length, 3);
  bus.close();
});

test('migration keeps pre-migration entries queryable, chain verifies, old dir stops receiving', () => {
  const oldDir = tmpDir('eb423-old-');
  const newDir = tmpDir('eb423-new-');
  const bus = landedBus(oldDir);
  subscribeN(bus, 2);
  assert.equal(bus.getSubscriptionAudit().length, 2);
  const oldFileBefore = auditLines(oldDir);

  bus.setSubscriptionAuditDir(newDir);
  subscribeN(bus, 1, 'u');
  // Memory index untouched: all four events queryable, chain intact.
  assert.equal(bus.getSubscriptionAudit().length, 4);
  assert.deepEqual(bus.verifySubscriptionAuditChain(), { ok: true, checked: 4 });
  // New appends land only in the new directory.
  assert.deepEqual(auditFileActions(newDir), ['subscription_audit_dir_revised', 'subscribe']);
  // The old directory's file is byte-identical: it stopped receiving.
  assert.deepEqual(auditLines(oldDir), oldFileBefore);
  bus.close();
});

test('files already in the new directory do not enter the live view (restart only)', () => {
  const newDir = tmpDir('eb423-new-');
  // Seed the new directory with one journaled subscribe first.
  const seeder = landedBus(newDir);
  seeder.subscribe('seed.topic', () => {});
  seeder.close();
  // The seeder's close audits its own unsubscribe as well.
  assert.deepEqual(auditFileActions(newDir), ['subscribe', 'unsubscribe']);

  const oldDir = tmpDir('eb423-old-');
  const bus = landedBus(oldDir);
  subscribeN(bus, 1);
  bus.setSubscriptionAuditDir(newDir);
  // The seeded event is on disk in the new directory but NOT in the
  // live view — the migration deliberately does not re-recover.
  assert.equal(
    bus.getSubscriptionAudit().filter((e) => e.pattern === 'seed.topic').length,
    0,
  );
  assert.deepEqual(bus.verifySubscriptionAuditChain(), {
    ok: true,
    checked: bus.getSubscriptionAudit().length,
  });

  // A restart recovers it: a fresh bus under the new directory sees
  // the seeded event (and the migration's own appends).
  const restarted = landedBus(newDir);
  assert.equal(
    restarted
      .getSubscriptionAudit()
      .filter((e) => e.action === 'subscribe' && e.pattern === 'seed.topic').length,
    1,
  );
  restarted.close();
  bus.close();
});

test('migrating to the current directory is a no-op with no audit', () => {
  const dir = tmpDir('eb423-');
  const bus = landedBus(dir);
  subscribeN(bus, 1);
  bus.setSubscriptionAuditDir(dir);
  assert.equal(dirRevised(bus).length, 0);
  assert.equal(bus.getSubscriptionAudit().length, 1);
  assert.equal(auditLines(dir).length, 1);
  bus.close();
});

test('setSubscriptionAuditDir rejects an empty or non-string dir and leaves the live log untouched', () => {
  const oldDir = tmpDir('eb423-old-');
  const bus = landedBus(oldDir);
  subscribeN(bus, 2);
  const beforeEntries = bus.getSubscriptionAudit();
  const beforeLines = auditLines(oldDir);
  for (const badDir of ['', 42, null, undefined]) {
    assert.throws(() => bus.setSubscriptionAuditDir(badDir as never), RangeError);
  }
  assert.deepEqual(bus.getSubscriptionAudit(), beforeEntries);
  assert.deepEqual(auditLines(oldDir), beforeLines);
  assert.equal(dirRevised(bus).length, 0);
  // A valid migration afterwards still works on the untouched log.
  const newDir = tmpDir('eb423-new-');
  bus.setSubscriptionAuditDir(newDir);
  assert.deepEqual(dirRevised(bus)[0].detail, { oldDir, dir: newDir });
  bus.close();
});

test('setSubscriptionAuditDir rejects a closed bus', () => {
  const dir = tmpDir('eb423-');
  const bus = landedBus(dir);
  subscribeN(bus, 1);
  bus.close();
  assert.throws(() => bus.setSubscriptionAuditDir(tmpDir('eb423-')), RangeError);
});

test('setSubscriptionAuditDir rejects an illegal actor and leaves the live log untouched', () => {
  const oldDir = tmpDir('eb423-old-');
  const bus = landedBus(oldDir);
  subscribeN(bus, 1);
  const beforeLines = auditLines(oldDir);
  for (const badActor of ['', 42, null]) {
    assert.throws(() => bus.setSubscriptionAuditDir(tmpDir('eb423-'), badActor as never), RangeError);
  }
  assert.deepEqual(auditLines(oldDir), beforeLines);
  assert.equal(dirRevised(bus).length, 0);
  assert.equal(bus.getSubscriptionAudit().length, 1);
  bus.close();
});

test('two consecutive migrations both take effect with chained audit', () => {
  const dirA = tmpDir('eb423-a-');
  const dirB = tmpDir('eb423-b-');
  const dirC = tmpDir('eb423-c-');
  const bus = landedBus(dirA);
  subscribeN(bus, 1);
  bus.setSubscriptionAuditDir(dirB);
  subscribeN(bus, 1, 'u');
  assert.deepEqual(auditFileActions(dirB), ['subscription_audit_dir_revised', 'subscribe']);
  bus.setSubscriptionAuditDir(dirC);
  subscribeN(bus, 1, 'v');
  assert.deepEqual(auditFileActions(dirC), ['subscription_audit_dir_revised', 'subscribe']);
  assert.deepEqual(
    dirRevised(bus).map((e) => e.detail),
    [
      { oldDir: dirA, dir: dirB },
      { oldDir: dirB, dir: dirC },
    ],
  );
  bus.close();
});

test('migration leaves the retention bound untouched', () => {
  const oldDir = tmpDir('eb423-old-');
  const newDir = tmpDir('eb423-new-');
  const bus = landedBus(oldDir, { subscriptionAuditMaxEntries: 3 });
  subscribeN(bus, 3);
  bus.setSubscriptionAuditDir(newDir);
  subscribeN(bus, 1, 'u');
  // The bound still caps the index at 3 — the migration event itself
  // evicted the oldest subscribe, and the newest subscribe evicted
  // the next one.
  assert.equal(bus.getSubscriptionAudit().length, 3);
  assert.equal(dirRevised(bus).length, 1);
  assert.equal(
    bus.getSubscriptionAudit().filter((e) => e.action === 'subscribe').length,
    2,
  );
  bus.close();
});

test('migration to a not-yet-existing nested directory creates it and appends land there', () => {
  const oldDir = tmpDir('eb423-old-');
  const nested = join(tmpDir('eb423-new-'), 'nested', 'deep');
  const bus = landedBus(oldDir);
  assert.equal(existsSync(nested), false);
  bus.setSubscriptionAuditDir(nested);
  subscribeN(bus, 1);
  assert.deepEqual(auditFileActions(nested), [
    'subscription_audit_dir_revised',
    'subscribe',
  ]);
  bus.close();
});
