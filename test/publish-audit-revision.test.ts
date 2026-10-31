/**
 * Bus-level publish audit revision (EB-309).
 *
 * `setPublishAudit` retunes the publish audit journal live — the
 * in-memory read-view bound and the live-file byte budget — without
 * restarting the bus. Only a bus that enabled `publishAudit` can be
 * revised; illegal values fail closed with the live tuning and the
 * audit log untouched; an identical (or empty) revision is a no-op
 * with no audit. In-flight entries are retained: a tightened bound
 * evicts at the next record, never immediately.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { PublishAuditLog, PUBLISH_AUDIT_LIVE_NAME } from '../src/publishaudit.ts';

const tempDir = (prefix: string): string => mkdtempSync(join(tmpdir(), prefix));

/** Archived journal files (`__publish_audit.<day>.<n>.jsonl`), never the live file. */
const archivedFiles = (dir: string): string[] =>
  readdirSync(dir).filter((f) => /^__publish_audit\.\d{4}-\d{2}-\d{2}\.\d+\.jsonl$/.test(f));

const liveLines = (dir: string): string[] =>
  readFileSync(join(dir, PUBLISH_AUDIT_LIVE_NAME), 'utf8')
    .split('\n')
    .filter((l) => l.trim() !== '');

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'publish_audit_revised' });

/** A bus with the journal on, subscribed so publishes fan out. */
function auditedBus(dir: string, opts: { maxEntries: number; maxBytes: number }): EventBus {
  const bus = new EventBus({
    durableLogDir: dir,
    publishAudit: { maxEntries: opts.maxEntries, maxBytes: opts.maxBytes },
  });
  bus.subscribe('t', () => {});
  return bus;
}

function publishN(bus: EventBus, n: number, startSeq = 1): void {
  for (let i = 0; i < n; i += 1) {
    bus.publish('t', { n: startSeq + i });
  }
}

test('setPublishAudit fails closed when publishAudit is not enabled', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setPublishAudit({ maxEntries: 100 }), RangeError);
  assert.throws(() => bus.setPublishAudit({ maxBytes: 1024 }), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('setPublishAudit rejects illegal values and leaves the live tuning untouched', (t) => {
  const dir = tempDir('pubaudit-rev-bad-');
  const bus = auditedBus(dir, { maxEntries: 3, maxBytes: 100_000 });
  t.after(() => bus.close());

  const bad: Array<unknown> = [
    { maxEntries: 0 },
    { maxEntries: -1 },
    { maxEntries: 1.5 },
    { maxEntries: Number.NaN },
    { maxEntries: Infinity },
    { maxEntries: '3' },
    { maxBytes: 0 },
    { maxBytes: -1 },
    { maxBytes: Number.NaN },
    { maxBytes: Infinity },
    { maxBytes: '1024' },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setPublishAudit(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: the 3-entry bound still holds.
  publishN(bus, 5);
  assert.equal(bus.getPublishAudit().length, 3);
  assert.equal(audit(bus).length, 0);
});

test('setPublishAudit with identical values or an empty revision is a no-op with no audit', (t) => {
  const dir = tempDir('pubaudit-rev-noop-');
  const bus = auditedBus(dir, { maxEntries: 3, maxBytes: 100_000 });
  t.after(() => bus.close());

  bus.setPublishAudit({ maxEntries: 3 });
  bus.setPublishAudit({ maxBytes: 100_000 });
  bus.setPublishAudit({});
  bus.setPublishAudit(null as never);
  assert.equal(audit(bus).length, 0);
});

test('partial revision moves only the provided field', (t) => {
  const dir = tempDir('pubaudit-rev-partial-');
  const bus = auditedBus(dir, { maxEntries: 3, maxBytes: 100_000 });
  t.after(() => bus.close());

  bus.setPublishAudit({ maxEntries: 7 });
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    maxEntries: 7,
    oldMaxEntries: 3,
    maxBytes: 100_000,
    oldMaxBytes: 100_000,
  });
});

test('growing maxEntries retains the in-flight entries', (t) => {
  const dir = tempDir('pubaudit-rev-grow-');
  const bus = auditedBus(dir, { maxEntries: 3, maxBytes: 100_000 });
  t.after(() => bus.close());

  publishN(bus, 5);
  assert.deepEqual(
    bus.getPublishAudit().map((e) => e.seq),
    [3, 4, 5],
  );
  bus.setPublishAudit({ maxEntries: 10 });
  publishN(bus, 5, 6);
  // The three retained entries survive the growth; the five new ones join.
  assert.deepEqual(
    bus.getPublishAudit().map((e) => e.seq),
    [3, 4, 5, 6, 7, 8, 9, 10],
  );
});

test('shrinking maxEntries evicts at the next record, not immediately', (t) => {
  const dir = tempDir('pubaudit-rev-shrink-');
  const bus = auditedBus(dir, { maxEntries: 3, maxBytes: 100_000 });
  t.after(() => bus.close());

  publishN(bus, 5);
  assert.equal(bus.getPublishAudit().length, 3);
  bus.setPublishAudit({ maxEntries: 2 });
  // In-flight entries are retained: the view still holds three until
  // the next record evicts down to the live bound.
  assert.equal(bus.getPublishAudit().length, 3);
  publishN(bus, 1, 6);
  assert.deepEqual(
    bus.getPublishAudit().map((e) => e.seq),
    [5, 6],
  );
});

test('growing maxBytes defers the next rotation', (t) => {
  const dir = tempDir('pubaudit-rev-rotate-grow-');
  // One journal line is ~230 bytes (two 64-hex chain digests alone),
  // so a 100-byte budget rotates on the second record.
  const bus = auditedBus(dir, { maxEntries: 100, maxBytes: 100 });
  t.after(() => bus.close());

  publishN(bus, 2);
  assert.equal(archivedFiles(dir).length, 1);
  assert.equal(liveLines(dir).length, 1);
  bus.setPublishAudit({ maxBytes: 64 * 1024 * 1024 });
  publishN(bus, 3, 3);
  // No further rotation under the loosened budget; the live file grows.
  assert.equal(archivedFiles(dir).length, 1);
  assert.equal(liveLines(dir).length, 4);
});

test('shrinking maxBytes rotates at the next record', (t) => {
  const dir = tempDir('pubaudit-rev-rotate-shrink-');
  const bus = auditedBus(dir, { maxEntries: 100, maxBytes: 64 * 1024 * 1024 });
  t.after(() => bus.close());

  publishN(bus, 2);
  assert.equal(archivedFiles(dir).length, 0);
  assert.equal(liveLines(dir).length, 2);
  bus.setPublishAudit({ maxBytes: 100 });
  publishN(bus, 1, 3);
  // The live file (~460 bytes) plus one more line exceeds the new
  // 100-byte budget, so the next record rotates it.
  assert.equal(archivedFiles(dir).length, 1);
  assert.equal(liveLines(dir).length, 1);
});

test('PublishAuditLog retune/current: construction semantics, failed retune untouched', (t) => {
  const dir = tempDir('pubaudit-log-retune-');
  const log = new PublishAuditLog(dir, { maxEntries: 5, maxBytes: 1500.5 });
  assert.deepEqual(log.current(), { maxEntries: 5, maxBytes: 1500.5 });
  // A fractional byte budget is valid — the constructor accepts any
  // positive finite number of bytes, and retune reuses that rule.
  log.retune({ maxEntries: 2, maxBytes: 2000.5 });
  assert.deepEqual(log.current(), { maxEntries: 2, maxBytes: 2000.5 });
  for (const next of [
    { maxEntries: 0, maxBytes: 2000.5 },
    { maxEntries: 2.5, maxBytes: 2000.5 },
    { maxEntries: 2, maxBytes: 0 },
    { maxEntries: 2, maxBytes: Number.NaN },
    { maxEntries: 2, maxBytes: Infinity },
  ]) {
    assert.throws(() => log.retune(next), RangeError);
  }
  assert.deepEqual(log.current(), { maxEntries: 2, maxBytes: 2000.5 });
  void t;
});
