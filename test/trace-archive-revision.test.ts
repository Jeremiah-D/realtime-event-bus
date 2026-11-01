/**
 * Bus-level trace span archive revision (EB-315).
 *
 * `setTraceArchive` retunes the `trace.archive` journal bounds live —
 * without restarting the bus. Only a bus that enabled `trace.archive`
 * can be revised; illegal values fail closed with the live tuning and
 * the audit log untouched; an identical (or empty) revision is a no-op
 * with no audit. Shrinking `maxEntries` keeps the journaled spans
 * until the next record evicts down to the live bound; a revised
 * `maxBytes` is honored by the next record's rotation check. The
 * journal directory and the clock are untouched.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import {
  TraceSpanJournal,
  TRACE_ARCHIVE_LIVE_NAME,
  TRACE_ARCHIVE_DEFAULT_MAX_ENTRIES,
  TRACE_ARCHIVE_DEFAULT_MAX_BYTES,
} from '../src/tracearchive.ts';
import type { TraceSpan } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function tmpLogDir(): string {
  return mkdtempSync(join(tmpdir(), 'eb315-'));
}

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const TRACE_ID = 'a'.repeat(32);

function sampleSpan(spanId: string, at = 1000): TraceSpan {
  return {
    traceId: TRACE_ID,
    spanId,
    name: 'bus.publish',
    at,
    durationMs: 2,
    attrs: { topic: 't', seq: 1 },
  };
}

function archivedFiles(dir: string): string[] {
  // Only rotated trace-span journals count — the durable topic log
  // (`t.log`, EB-17) shares the directory and is not an archive file.
  return readdirSync(dir).filter((f) => /^__trace_spans\.\d{4}-\d{2}-\d{2}\.\d+\.jsonl$/.test(f));
}

function archivedBus(opts: {
  maxEntries?: number;
  maxBytes?: number;
}): { clock: { nowMs: number; now: () => number }; dir: string; bus: EventBus } {
  const clock = controllableClock();
  const dir = tmpLogDir();
  const bus = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: { maxEntries: opts.maxEntries, maxBytes: opts.maxBytes } },
  });
  return { clock, dir, bus };
}

// --- TraceSpanJournal unit behavior ---

test('TraceSpanJournal current reports the live tuning, defaults included', () => {
  const dir = tmpLogDir();
  const journal = new TraceSpanJournal(dir, {});
  assert.deepEqual(journal.current(), {
    maxEntries: TRACE_ARCHIVE_DEFAULT_MAX_ENTRIES,
    maxBytes: TRACE_ARCHIVE_DEFAULT_MAX_BYTES,
  });
  const tuned = new TraceSpanJournal(tmpLogDir(), { maxEntries: 7, maxBytes: 1234 });
  assert.deepEqual(tuned.current(), { maxEntries: 7, maxBytes: 1234 });
});

test('TraceSpanJournal retune validates and leaves the live tuning untouched on failure', () => {
  const journal = new TraceSpanJournal(tmpLogDir(), { maxEntries: 10, maxBytes: 1000 });
  assert.throws(() => journal.retune({ maxEntries: 0, maxBytes: 1000 }), RangeError);
  assert.throws(() => journal.retune({ maxEntries: -1, maxBytes: 1000 }), RangeError);
  assert.throws(() => journal.retune({ maxEntries: 2.5, maxBytes: 1000 }), RangeError);
  assert.throws(() => journal.retune({ maxEntries: 10, maxBytes: 0 }), RangeError);
  assert.throws(() => journal.retune({ maxEntries: 10, maxBytes: Number.NaN }), RangeError);
  assert.throws(() => journal.retune({ maxEntries: 10, maxBytes: Infinity }), RangeError);
  assert.deepEqual(journal.current(), { maxEntries: 10, maxBytes: 1000 });
  journal.retune({ maxEntries: 5, maxBytes: 2000 });
  assert.deepEqual(journal.current(), { maxEntries: 5, maxBytes: 2000 });
});

test('journal shrink retains entries until the next record evicts down to the live bound', () => {
  const dir = tmpLogDir();
  const journal = new TraceSpanJournal(dir, { maxEntries: 10 });
  for (let i = 0; i < 4; i++) {
    journal.record(sampleSpan(String(i).repeat(16), 1000 + i));
  }
  assert.equal(journal.size(), 4);
  journal.retune({ maxEntries: 2, maxBytes: TRACE_ARCHIVE_DEFAULT_MAX_BYTES });
  // Unlike a rebuild, the shrink itself evicts nothing: all four spans
  // are still in the read view.
  assert.equal(journal.size(), 4);
  journal.record(sampleSpan('4'.repeat(16), 1004));
  assert.equal(journal.size(), 2);
  assert.deepEqual(
    journal.query({}).map((e) => e.spanId),
    ['3'.repeat(16), '4'.repeat(16)],
  );
});

test('journal tightened maxBytes rotates at the next record', () => {
  const dir = tmpLogDir();
  const journal = new TraceSpanJournal(dir, { maxBytes: 100_000 });
  journal.record(sampleSpan('0'.repeat(16)));
  journal.record(sampleSpan('1'.repeat(16), 1001));
  assert.equal(archivedFiles(dir).length, 0);
  journal.retune({ maxEntries: TRACE_ARCHIVE_DEFAULT_MAX_ENTRIES, maxBytes: 120 });
  journal.record(sampleSpan('2'.repeat(16), 1002));
  const archived = archivedFiles(dir);
  assert.ok(archived.length >= 1, `expected rotation, got: ${readdirSync(dir)}`);
  assert.match(archived[0], /^__trace_spans\.\d{4}-\d{2}-\d{2}\.\d+\.jsonl$/);
  // The new line still lands in the live file: nothing is lost.
  const recovered = new TraceSpanJournal(dir, {});
  assert.equal(recovered.size(), 3);
});

test('journal loosened maxBytes defers the next rotation', () => {
  const dir = tmpLogDir();
  const journal = new TraceSpanJournal(dir, { maxBytes: 120 });
  for (let i = 0; i < 4; i++) {
    journal.record(sampleSpan(String(i).repeat(16), 1000 + i));
  }
  const rotated = archivedFiles(dir).length;
  assert.ok(rotated >= 1, 'expected rotations under the tight budget');
  journal.retune({ maxEntries: TRACE_ARCHIVE_DEFAULT_MAX_ENTRIES, maxBytes: 64 * 1024 * 1024 });
  journal.record(sampleSpan('4'.repeat(16), 1004));
  assert.equal(archivedFiles(dir).length, rotated);
});

// --- bus-level setTraceArchive ---

test('setTraceArchive fails closed when trace.archive is not enabled', () => {
  const plain = new EventBus();
  assert.throws(() => plain.setTraceArchive({ maxEntries: 5 }), RangeError);
  assert.equal(plain.getSubscriptionAudit({ action: 'trace_archive_revised' }).length, 0);

  const traced = new EventBus({ trace: true });
  assert.throws(() => traced.setTraceArchive({ maxBytes: 1024 }), RangeError);

  // Trace without an archive journal is still not revisable.
  const dir = tmpLogDir();
  const noArchive = new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1 },
  });
  assert.throws(() => noArchive.setTraceArchive({ maxEntries: 5 }), RangeError);
  assert.equal(noArchive.getSubscriptionAudit({ action: 'trace_archive_revised' }).length, 0);
});

test('setTraceArchive rejects illegal values and leaves the live tuning untouched', async () => {
  const { dir, bus } = archivedBus({ maxEntries: 3, maxBytes: 100_000 });
  bus.subscribe('t', () => {});
  const bad: Array<unknown> = [
    { maxEntries: 0 },
    { maxEntries: -1 },
    { maxEntries: 2.5 },
    { maxEntries: '3' },
    { maxBytes: 0 },
    { maxBytes: -1 },
    { maxBytes: Number.NaN },
    { maxBytes: Infinity },
    { maxBytes: '1024' },
    // One good field does not save a bad sibling: the whole call fails.
    { maxEntries: 5, maxBytes: 0 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setTraceArchive(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: one publish still journals 5
  // spans and the view still holds the old bound of 3.
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 3);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_revised' }).length, 0);
  // A valid revision afterwards still works.
  bus.setTraceArchive({ maxEntries: 100 });
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_revised' }).length, 1);
  void dir;
});

test('setTraceArchive with identical values or an empty revision is a no-op with no audit', () => {
  const { bus } = archivedBus({ maxEntries: 100, maxBytes: 100_000 });
  bus.setTraceArchive({ maxEntries: 100 });
  bus.setTraceArchive({ maxBytes: 100_000 });
  bus.setTraceArchive({ maxEntries: 100, maxBytes: 100_000 });
  bus.setTraceArchive({});
  bus.setTraceArchive(null as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_revised' }).length, 0);
});

test('partial revision moves only the provided field', () => {
  const { bus } = archivedBus({ maxEntries: 100, maxBytes: 100_000 });
  bus.setTraceArchive({ maxBytes: 200_000 });
  let audit = bus.getSubscriptionAudit({ action: 'trace_archive_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    maxEntries: 100,
    oldMaxEntries: 100,
    maxBytes: 200_000,
    oldMaxBytes: 100_000,
  });
  bus.setTraceArchive({ maxEntries: 5 });
  audit = bus.getSubscriptionAudit({ action: 'trace_archive_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    maxEntries: 5,
    oldMaxEntries: 100,
    maxBytes: 200_000,
    oldMaxBytes: 200_000,
  });
});

test('shrinking maxEntries retains entries until the next record evicts', async () => {
  const { bus } = archivedBus({ maxEntries: 100 });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 10);
  bus.setTraceArchive({ maxEntries: 3 });
  // The shrink itself evicts nothing: all ten spans are still visible.
  assert.equal(bus.getTraceArchive().length, 10);
  bus.publish('t', { n: 3 });
  await flush();
  const after = bus.getTraceArchive();
  assert.equal(after.length, 3);
  // The survivors are the newest spans, all from the third publish.
  assert.ok(after.every((s) => (s.attrs.seq as number) === 3));
});

test('growing maxEntries keeps every journaled span', async () => {
  const { bus } = archivedBus({ maxEntries: 3 });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 3);
  bus.setTraceArchive({ maxEntries: 100 });
  bus.publish('t', { n: 2 });
  await flush();
  const after = bus.getTraceArchive();
  assert.equal(after.length, 8);
  assert.ok(after.some((s) => (s.attrs.seq as number) === 1));
  assert.ok(after.some((s) => (s.attrs.seq as number) === 2));
});

test('tightened maxBytes rotates the live file at the next record', async () => {
  const { dir, bus } = archivedBus({ maxBytes: 64 * 1024 * 1024 });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(archivedFiles(dir).length, 0);
  assert.ok(readdirSync(dir).includes(TRACE_ARCHIVE_LIVE_NAME));
  bus.setTraceArchive({ maxBytes: 200 });
  bus.publish('t', { n: 2 });
  await flush();
  const archived = archivedFiles(dir);
  assert.ok(archived.length >= 1, `expected rotation, got: ${readdirSync(dir)}`);
  // The journal keeps working after the rotation: both publishes are
  // still in the read view.
  assert.equal(bus.getTraceArchive().length, 10);
});

test('loosened maxBytes defers the next rotation', async () => {
  const { dir, bus } = archivedBus({ maxBytes: 200 });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.ok(archivedFiles(dir).length >= 1, 'expected rotation under the tight budget');
  const rotated = archivedFiles(dir).length;
  bus.setTraceArchive({ maxBytes: 64 * 1024 * 1024 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.equal(archivedFiles(dir).length, rotated);
});
