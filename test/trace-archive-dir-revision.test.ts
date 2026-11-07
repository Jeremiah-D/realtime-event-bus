/**
 * Bus-level trace span archive journal directory live migration (EB-421).
 *
 * `setTraceArchiveDir` moves the directory the journal is built under
 * without restarting the bus — the archive volume filled up, or the
 * bus was built without a `durableLogDir` and archiving should start
 * somewhere now. When a journal is attached, a fresh `TraceSpanJournal`
 * is built under the new directory with the live journal's current
 * `maxEntries` / `maxBytes` and the bus clock, and the shared span
 * closure is re-composed so new spans land in the new directory; the
 * new journal's constructor `recover()` back-fills spans already in
 * the new directory, and the old directory's files are left exactly as
 * they are (never deleted). When no journal is attached only the
 * captured directory moves, so the next attach builds under the new
 * directory. Migrating to the current directory is a no-op with no
 * audit. Audited as `trace_archive_dir_revised` (`{ oldDir, dir }`,
 * `oldDir` `null` when the bus was built without a `durableLogDir`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { TRACE_ARCHIVE_LIVE_NAME } from '../src/tracearchive.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function tmpLogDir(): string {
  return mkdtempSync(join(tmpdir(), 'eb421-'));
}

/** Lines currently in the live journal file; 0 when the file is absent. */
function liveLines(dir: string): number {
  const file = join(dir, TRACE_ARCHIVE_LIVE_NAME);
  if (!existsSync(file)) return 0;
  return readFileSync(file, 'utf8')
    .split('\n')
    .filter((line) => line.length > 0).length;
}

/** Archived (rotated) files currently in the directory. */
function archivedFiles(dir: string): string[] {
  if (!existsSync(dir)) return [];
  return readdirSync(dir).filter(
    (f) => f.startsWith('__trace_spans.') && f !== TRACE_ARCHIVE_LIVE_NAME,
  );
}

/** A bus with tracing on and a log directory, but no archive journal. */
function unarchivedBus(dir: string): EventBus {
  return new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1 },
  });
}

/** A bus with the archive journal attached under `dir`. */
function archivedBus(dir: string): EventBus {
  return new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true },
  });
}

test('setTraceArchiveDir fails closed when trace is not enabled', () => {
  const plain = new EventBus();
  assert.throws(() => plain.setTraceArchiveDir(tmpLogDir()), RangeError);
  assert.equal(plain.getSubscriptionAudit({ action: 'trace_archive_dir_revised' }).length, 0);
  // A durable log directory alone is not enough: the gate is trace.
  const dir = tmpLogDir();
  const logged = new EventBus({ durableLogDir: dir });
  assert.throws(() => logged.setTraceArchiveDir(tmpLogDir()), RangeError);
  assert.equal(logged.getSubscriptionAudit({ action: 'trace_archive_dir_revised' }).length, 0);
});

test('setTraceArchiveDir rejects an illegal actor', () => {
  const bus = unarchivedBus(tmpLogDir());
  assert.throws(() => bus.setTraceArchiveDir(tmpLogDir(), ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' }).length, 0);
});

test('a closed bus refuses the migration', async () => {
  const bus = unarchivedBus(tmpLogDir());
  await bus.close();
  assert.throws(() => bus.setTraceArchiveDir(tmpLogDir()), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' }).length, 0);
});

test('an empty or non-string dir refuses and leaves the live state untouched', async () => {
  const dirA = tmpLogDir();
  const dirB = tmpLogDir();
  const bus = archivedBus(dirA);
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 5);
  assert.equal(liveLines(dirA), 5);

  const bad: Array<unknown> = ['', 42, null, undefined, {}, []];
  for (const dir of bad) {
    assert.throws(
      () => bus.setTraceArchiveDir(dir as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(dir)}`,
    );
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' }).length, 0);
  // The journal is exactly where it was: the next spans still land in
  // the old directory, and a later valid migration still reports it
  // as the old directory.
  bus.publish('t', { n: 2 });
  await flush();
  assert.equal(liveLines(dirA), 10);
  assert.equal(liveLines(dirB), 0);
  bus.setTraceArchiveDir(dirB);
  const audit = bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { oldDir: dirA, dir: dirB });
});

test('migrating to the current directory is a no-op with no audit', async () => {
  const dirA = tmpLogDir();
  const bus = archivedBus(dirA);
  bus.subscribe('t', () => {});
  bus.setTraceArchiveDir(dirA);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' }).length, 0);
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 5);
  assert.equal(liveLines(dirA), 5);
  // Same for an unattached bus: the captured directory is already this one.
  const plain = unarchivedBus(dirA);
  plain.setTraceArchiveDir(dirA);
  assert.equal(plain.getSubscriptionAudit({ action: 'trace_archive_dir_revised' }).length, 0);
});

test('attached migration moves new spans to the new directory; old files untouched', async () => {
  const dirA = tmpLogDir();
  const dirB = tmpLogDir();
  const bus = archivedBus(dirA);
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 5);
  assert.equal(liveLines(dirA), 5);

  // A span pre-placed in the new directory's live file is back-filled
  // by the new journal's constructor `recover()`.
  const prePlaced = JSON.stringify({
    v: 1,
    traceId: 'a'.repeat(32),
    spanId: '0123456789abcdef',
    name: 'bus.publish',
    at: Date.now(),
    durationMs: 2,
    attrs: { topic: 't', seq: 0 },
  });
  writeFileSync(join(dirB, TRACE_ARCHIVE_LIVE_NAME), `${prePlaced}\n`);

  bus.setTraceArchiveDir(dirB, 'ops');
  // The read view is now the new journal's recovered view: the
  // pre-placed span only — the old directory's spans stay on disk
  // there, never re-sent, never re-read.
  const recovered = bus.getTraceArchive();
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].spanId, '0123456789abcdef');
  bus.publish('t', { n: 2 });
  await flush();
  const after = bus.getTraceArchive();
  assert.equal(after.length, 6);
  assert.equal(
    after.filter((s) => (s.attrs.seq as number) === 2).length,
    5,
  );
  assert.equal(liveLines(dirB), 6);
  // The old directory's files are untouched (EB-61: never deleted,
  // never rewritten).
  assert.equal(liveLines(dirA), 5);
  assert.deepEqual(archivedFiles(dirA), []);
  const audit = bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { oldDir: dirA, dir: dirB });
  assert.equal(audit[0].actor, 'ops');
});

test('migration carries the live maxEntries and maxBytes forward', async () => {
  const dirA = tmpLogDir();
  const dirB = tmpLogDir();
  const bus = new EventBus({
    durableLogDir: dirA,
    trace: { sampleRate: 1, archive: { maxEntries: 5, maxBytes: 300 } },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  // The tiny byte budget already rotated in the old directory.
  assert.ok(archivedFiles(dirA).length >= 1, `files: ${readdirSync(dirA)}`);
  bus.setTraceArchiveDir(dirB);
  bus.publish('t', { n: 2 });
  await flush();
  bus.publish('t', { n: 3 });
  await flush();
  // The carried maxEntries still caps the view at the newest 5 spans.
  const view = bus.getTraceArchive();
  assert.equal(view.length, 5);
  assert.ok(view.every((s) => (s.attrs.seq as number) === 3));
  // The carried maxBytes still rotates in the new directory — a reset
  // to the 64 MiB default would have left no archive behind.
  assert.ok(archivedFiles(dirB).length >= 1, `files: ${readdirSync(dirB)}`);
});

test('the tail shared channel still flows into the new directory after migration', async () => {
  const dirA = tmpLogDir();
  const dirB = tmpLogDir();
  // Head sampling off: every archived span is a tail-decided span,
  // riding the shared channel with no callback of its own.
  const bus = new EventBus({
    durableLogDir: dirA,
    trace: { sampleRate: 0, archive: true },
    tailSampling: { successSampleRate: 1 },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.ok(bus.getTraceArchive().length > 0);
  assert.ok(bus.getStats().tailSampledSpans.length > 0);
  const linesBefore = liveLines(dirA);
  assert.ok(linesBefore > 0);

  bus.setTraceArchiveDir(dirB);
  bus.publish('t', { n: 2 });
  await flush();
  // The tail sampler was re-pointed at the fresh closure: the new
  // trace's spans land in the new directory, not the old one.
  assert.ok(liveLines(dirB) > 0);
  assert.equal(liveLines(dirA), linesBefore);
  assert.ok(bus.getTraceArchive().length > 0);
});

test('unattached migration moves the next attach to the new directory', async () => {
  const dirA = tmpLogDir();
  const dirB = tmpLogDir();
  const bus = unarchivedBus(dirA);
  bus.subscribe('t', () => {});
  bus.setTraceArchiveDir(dirB);
  const audit = bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { oldDir: dirA, dir: dirB });
  // The next attach builds under the new directory, not the captured one.
  bus.setTraceArchiveEnabled(true);
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 5);
  assert.equal(liveLines(dirB), 5);
  assert.equal(liveLines(dirA), 0);
});

test('a bus built without a durableLogDir can attach after migration', async () => {
  const dirB = tmpLogDir();
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  // Attaching refuses until the bus gains a directory.
  assert.throws(() => bus.setTraceArchiveEnabled(true), RangeError);
  bus.setTraceArchiveDir(dirB);
  const audit = bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { oldDir: null, dir: dirB });
  bus.setTraceArchiveEnabled(true);
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getStats().traceArchiveEnabled, true);
  assert.equal(bus.getTraceArchive().length, 5);
  assert.equal(liveLines(dirB), 5);
});

test('the migration audits exact old and new directories with the actor', () => {
  const dirA = tmpLogDir();
  const dirB = tmpLogDir();
  const bus = archivedBus(dirA);
  bus.setTraceArchiveDir(dirB, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { oldDir: dirA, dir: dirB });
  assert.equal(audit[0].actor, 'ops');
});

test('two consecutive migrations both take effect', async () => {
  const dirA = tmpLogDir();
  const dirB = tmpLogDir();
  const dirC = tmpLogDir();
  const bus = archivedBus(dirA);
  bus.subscribe('t', () => {});
  bus.setTraceArchiveDir(dirB);
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(liveLines(dirB), 5);
  assert.equal(liveLines(dirA), 0);
  bus.setTraceArchiveDir(dirC);
  bus.publish('t', { n: 2 });
  await flush();
  assert.equal(liveLines(dirC), 5);
  assert.equal(liveLines(dirB), 5);
  assert.equal(liveLines(dirA), 0);
  const audit = bus.getSubscriptionAudit({ action: 'trace_archive_dir_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { oldDir: dirA, dir: dirB });
  assert.deepEqual(audit[1].detail, { oldDir: dirB, dir: dirC });
});
