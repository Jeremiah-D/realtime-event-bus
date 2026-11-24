/**
 * Bus-level publish audit live enable (EB-554):
 * `setPublishAudit` on a bus that never enabled `publishAudit` installs
 * the journal live — no restart, no lost subscriptions — with the call
 * resolved through the construction-time rule. `{}` takes the full
 * defaults (`maxEntries` 10_000, `maxBytes` 64 MiB, the
 * construction-time `publishAudit: true` shape); a partial call fills
 * only the fields it leaves out; the journal is built under the bus's
 * own `durableLogDir` with the bus clock. The enable is audited like
 * any revision, with both old values `null`. `null` is a silent no-op;
 * a bus without `durableLogDir` cannot be enabled; a closed bus
 * refuses the enable. The enable is live-only: a restart restores the
 * unconfigured state, and a re-enable recovers the old directory's
 * journal files into the read view.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import {
  PUBLISH_AUDIT_DEFAULT_MAX_BYTES,
  PUBLISH_AUDIT_DEFAULT_MAX_ENTRIES,
  PUBLISH_AUDIT_LIVE_NAME,
} from '../src/publishaudit.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const tempDir = (prefix: string): string => mkdtempSync(join(tmpdir(), prefix));

const liveLines = (dir: string): string[] =>
  readFileSync(join(dir, PUBLISH_AUDIT_LIVE_NAME), 'utf8')
    .split('\n')
    .filter((l) => l.trim() !== '');

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'publish_audit_revised' });

/** A bus with a durableLogDir but no publishAudit, subscribed so publishes fan out. */
function unconfiguredBus(dir: string): EventBus {
  const bus = new EventBus({ durableLogDir: dir });
  bus.subscribe('t', () => {});
  return bus;
}

function publishN(bus: EventBus, n: number, startSeq = 1): void {
  for (let i = 0; i < n; i += 1) {
    bus.publish('t', { n: startSeq + i });
  }
}

test('enabling with {} takes the full defaults and journals the next publish', (t) => {
  const dir = tempDir('pubaudit-enable-defaults-');
  const bus = unconfiguredBus(dir);
  t.after(() => bus.close());

  bus.setPublishAudit({});
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    maxEntries: PUBLISH_AUDIT_DEFAULT_MAX_ENTRIES,
    oldMaxEntries: null,
    maxBytes: PUBLISH_AUDIT_DEFAULT_MAX_BYTES,
    oldMaxBytes: null,
  });
  assert.equal(PUBLISH_AUDIT_DEFAULT_MAX_ENTRIES, 10_000);
  assert.equal(PUBLISH_AUDIT_DEFAULT_MAX_BYTES, 64 * 1024 * 1024);

  publishN(bus, 3);
  assert.equal(bus.getPublishAudit().length, 3);
  assert.equal(liveLines(dir).length, 3);
  assert.deepEqual(bus.verifyPublishAuditChain(), { ok: true, checked: 3 });
});

test('enabling with explicit values is exact, and the bound governs from the next record', (t) => {
  const dir = tempDir('pubaudit-enable-explicit-');
  const bus = unconfiguredBus(dir);
  t.after(() => bus.close());

  bus.setPublishAudit({ maxEntries: 3, maxBytes: 100_000 });
  assert.deepEqual(audit(bus)[0].detail, {
    maxEntries: 3,
    oldMaxEntries: null,
    maxBytes: 100_000,
    oldMaxBytes: null,
  });
  publishN(bus, 5);
  assert.deepEqual(
    bus.getPublishAudit().map((e) => e.seq),
    [3, 4, 5],
  );
});

test('a partial { maxEntries } enable takes the maxBytes default', (t) => {
  const dir = tempDir('pubaudit-enable-partial-entries-');
  const bus = unconfiguredBus(dir);
  t.after(() => bus.close());

  bus.setPublishAudit({ maxEntries: 7 });
  assert.deepEqual(audit(bus)[0].detail, {
    maxEntries: 7,
    oldMaxEntries: null,
    maxBytes: PUBLISH_AUDIT_DEFAULT_MAX_BYTES,
    oldMaxBytes: null,
  });
});

test('a partial { maxBytes } enable takes the maxEntries default', (t) => {
  const dir = tempDir('pubaudit-enable-partial-bytes-');
  const bus = unconfiguredBus(dir);
  t.after(() => bus.close());

  bus.setPublishAudit({ maxBytes: 100_000 });
  assert.deepEqual(audit(bus)[0].detail, {
    maxEntries: PUBLISH_AUDIT_DEFAULT_MAX_ENTRIES,
    oldMaxEntries: null,
    maxBytes: 100_000,
    oldMaxBytes: null,
  });
});

test('null on a never-enabled bus is a silent no-op with no audit', (t) => {
  const dir = tempDir('pubaudit-enable-null-');
  const bus = unconfiguredBus(dir);
  t.after(() => bus.close());

  bus.setPublishAudit(null as never);
  assert.equal(audit(bus).length, 0);
  // Still unconfigured: a publish before any enable is never recorded.
  publishN(bus, 1);
  assert.equal(bus.getPublishAudit().length, 0);
  // Still enableable.
  bus.setPublishAudit({});
  publishN(bus, 1, 2);
  assert.equal(bus.getPublishAudit().length, 1);
});

test('illegal values are rejected and leave the bus unconfigured', (t) => {
  const dir = tempDir('pubaudit-enable-bad-');
  const bus = unconfiguredBus(dir);
  t.after(() => bus.close());

  const bad: Array<unknown> = [
    'nope',
    42,
    [],
    { maxEntries: 0 },
    { maxEntries: -1 },
    { maxEntries: 1.5 },
    { maxEntries: '3' },
    { maxBytes: 0 },
    { maxBytes: Number.NaN },
    { maxBytes: Infinity },
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setPublishAudit(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    // A failed enable leaves the bus unconfigured.
    publishN(bus, 1);
    assert.equal(bus.getPublishAudit().length, 0);
  }
  assert.equal(audit(bus).length, 0);
  // A failed enable leaves the bus unconfigured: a later valid call
  // still enables it.
  bus.setPublishAudit({ maxEntries: 3 });
  assert.equal(audit(bus).length, 1);
});

test('a bus built without durableLogDir cannot be enabled', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  assert.throws(() => bus.setPublishAudit({}), /requires durableLogDir/);
  assert.throws(() => bus.setPublishAudit({ maxEntries: 100 }), /requires durableLogDir/);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getPublishAudit().length, 0);
  // There is no directory to journal under, and `durableLogDir` is
  // fixed at construction — a later valid call still refuses.
  assert.throws(() => bus.setPublishAudit({ maxEntries: 3, maxBytes: 1024 }), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('publishes before the enable are not back-filled; the next publish is journaled, rejections included', async (t) => {
  const dir = tempDir('pubaudit-enable-noretro-');
  const bus = new EventBus({
    durableLogDir: dir,
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
  });
  bus.subscribe('**', () => {});
  t.after(() => bus.close());

  bus.publish('t', { n: 1 });
  bus.publish('admin.cmd', { x: 1 }, { publisherId: 'intruder' });
  await flush();
  assert.equal(bus.getPublishAudit().length, 0);

  bus.setPublishAudit({});
  // The pre-enable publishes were never recorded.
  assert.equal(bus.getPublishAudit().length, 0);
  bus.publish('t', { n: 2 });
  bus.publish('admin.cmd', { x: 2 }, { publisherId: 'intruder' });
  await flush();
  const entries = bus.getPublishAudit();
  assert.equal(entries.length, 2);
  assert.equal(entries[0].outcome, 'admitted');
  assert.equal(entries[0].topic, 't');
  assert.equal(entries[1].outcome, 'rejected');
  assert.equal(entries[1].topic, 'admin.cmd');
  assert.equal(entries[1].reason, 'acl');
  assert.deepEqual(bus.verifyPublishAuditChain(), { ok: true, checked: 2 });
});

test('a revision after the enable merges the live values; a same-value or empty revision is a no-op', (t) => {
  const dir = tempDir('pubaudit-enable-revise-');
  const bus = unconfiguredBus(dir);
  t.after(() => bus.close());

  bus.setPublishAudit({ maxEntries: 3, maxBytes: 100_000 });
  bus.setPublishAudit({ maxBytes: 200_000 });
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[1].detail, {
    maxEntries: 3,
    oldMaxEntries: 3,
    maxBytes: 200_000,
    oldMaxBytes: 100_000,
  });
  bus.setPublishAudit({ maxEntries: 3 });
  bus.setPublishAudit({ maxBytes: 200_000 });
  bus.setPublishAudit({});
  bus.setPublishAudit(null as never);
  assert.equal(audit(bus).length, 2);
});

test('the directory can still migrate after the enable', (t) => {
  const dir1 = tempDir('pubaudit-enable-migrate-a-');
  const dir2 = tempDir('pubaudit-enable-migrate-b-');
  const bus = unconfiguredBus(dir1);
  t.after(() => bus.close());

  bus.setPublishAudit({ maxEntries: 10, maxBytes: 100_000 });
  publishN(bus, 1);
  assert.equal(liveLines(dir1).length, 1);
  bus.setPublishAuditDir(dir2);
  publishN(bus, 1, 2);
  // The old directory's entries leave the live read view; the new
  // directory holds only the post-migration record.
  assert.equal(bus.getPublishAudit().length, 1);
  assert.equal(liveLines(dir2).length, 1);
  assert.equal(liveLines(dir1).length, 1);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'publish_audit_dir_revised' }).length,
    1,
  );
});

test('a restart restores the unconfigured state; a re-enable recovers the old files', async (t) => {
  const dir = tempDir('pubaudit-enable-restart-');
  const first = unconfiguredBus(dir);
  first.setPublishAudit({});
  publishN(first, 2);
  assert.equal(first.getPublishAudit().length, 2);
  await first.close();

  // A fresh bus built from the same construction options holds no
  // journal: the enable was live-only.
  const fresh = new EventBus({ durableLogDir: dir });
  fresh.subscribe('t', () => {});
  t.after(() => fresh.close());
  assert.equal(fresh.getPublishAudit().length, 0);
  assert.deepEqual(fresh.verifyPublishAuditChain(), { ok: true, checked: 0 });
  assert.equal(audit(fresh).length, 0);

  // Re-enabling on the same directory recovers the old journal files
  // into the read view, and the chain verifies over them.
  fresh.setPublishAudit({});
  assert.deepEqual(
    fresh.getPublishAudit().map((e) => e.seq),
    [1, 2],
  );
  assert.deepEqual(fresh.verifyPublishAuditChain(), { ok: true, checked: 2 });
  assert.equal(audit(fresh).length, 1);
  // The next publish chains onto the recovered history.
  publishN(fresh, 1, 3);
  assert.equal(fresh.getPublishAudit().length, 3);
  assert.deepEqual(fresh.verifyPublishAuditChain(), { ok: true, checked: 3 });
});

test('a closed bus refuses the enable', async () => {
  const dir = tempDir('pubaudit-enable-closed-');
  const bus = unconfiguredBus(dir);
  await bus.close();

  assert.throws(() => bus.setPublishAudit({}), /bus is closed/);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getPublishAudit().length, 0);
});
