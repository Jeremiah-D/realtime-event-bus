/**
 * Bus-level trace span archive journal live attach/detach (EB-419).
 *
 * `setTraceArchiveEnabled` switches `trace.archive` on or off at
 * runtime — without restarting the bus. Only a bus that enabled
 * `trace` can be revised; attaching builds a fresh
 * `TraceSpanJournal` under the bus's construction-time
 * `durableLogDir` (never revisable, no `dir` parameter), with the
 * bus clock, and its constructor `recover()` back-fills from disk,
 * so spans archived before the attach are queryable at once;
 * detaching drops only the journal reference and leaves the disk
 * files exactly as they are, so re-attaching recovers them. The
 * attach re-composes the single shared closure and installs it in
 * both emitters that share the channel, so the new journal never
 * misses a span and a detached one stops receiving them. A
 * same-state attach or detach is a no-op with no audit. Audited as
 * `trace_archive_enabled_revised` (`{ enabled, oldEnabled }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { TRACE_ARCHIVE_LIVE_NAME } from '../src/tracearchive.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function tmpLogDir(): string {
  return mkdtempSync(join(tmpdir(), 'eb419-'));
}

/** Lines currently in the live journal file; 0 when the file is absent. */
function liveLines(dir: string): number {
  const file = join(dir, TRACE_ARCHIVE_LIVE_NAME);
  if (!existsSync(file)) return 0;
  return readFileSync(file, 'utf8')
    .split('\n')
    .filter((line) => line.length > 0).length;
}

/** A bus with tracing on and a log directory, but no archive journal. */
function unarchivedBus(dir: string): EventBus {
  return new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1 },
  });
}

test('setTraceArchiveEnabled fails closed when trace is not enabled', () => {
  const plain = new EventBus();
  assert.throws(() => plain.setTraceArchiveEnabled(true), RangeError);
  assert.throws(() => plain.setTraceArchiveEnabled(false), RangeError);
  assert.equal(plain.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 0);
  // A durable log directory alone is not enough: the gate is trace.
  const dir = tmpLogDir();
  const logged = new EventBus({ durableLogDir: dir });
  assert.throws(() => logged.setTraceArchiveEnabled(true), RangeError);
  assert.equal(logged.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 0);
});

test('setTraceArchiveEnabled rejects an illegal actor', () => {
  const bus = unarchivedBus(tmpLogDir());
  assert.throws(() => bus.setTraceArchiveEnabled(true, undefined, ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 0);
});

test('a closed bus refuses the switch', async () => {
  const bus = unarchivedBus(tmpLogDir());
  await bus.close();
  assert.throws(() => bus.setTraceArchiveEnabled(true), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 0);
});

test('enabled must be a boolean', () => {
  const bus = unarchivedBus(tmpLogDir());
  const bad: Array<unknown> = ['yes', 1, 0, {}, [], null, undefined];
  for (const enabled of bad) {
    assert.throws(() => bus.setTraceArchiveEnabled(enabled as never), RangeError);
  }
  assert.equal(bus.getStats().traceArchiveEnabled, false);
  assert.deepEqual(bus.getTraceArchive(), []);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 0);
});

test('attaching without a durableLogDir refuses; detaching a detached bus is a no-op', () => {
  const bus = new EventBus({ trace: { sampleRate: 1 } });
  assert.throws(
    () => bus.setTraceArchiveEnabled(true),
    /trace\.archive requires EventBusOptions\.durableLogDir to be set/,
  );
  assert.equal(bus.getStats().traceArchiveEnabled, false);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 0);
  // Detaching needs no directory: a bus with no journal is already
  // detached, so this is a same-state no-op, not an error.
  bus.setTraceArchiveEnabled(false);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 0);
});

test('illegal opts refuse and leave the live state untouched', async () => {
  const dir = tmpLogDir();
  const bus = unarchivedBus(dir);
  bus.subscribe('t', () => {});
  const bad: Array<unknown> = [
    { maxEntries: 0 },
    { maxEntries: -1 },
    { maxEntries: 2.5 },
    { maxEntries: '5' },
    { maxBytes: 0 },
    { maxBytes: -1 },
    { maxBytes: Number.NaN },
    { maxBytes: Infinity },
    { maxBytes: '1024' },
    // One good field does not save a bad sibling.
    { maxEntries: 5, maxBytes: 0 },
    'nope',
    42,
    [],
  ];
  for (const opts of bad) {
    assert.throws(
      () => bus.setTraceArchiveEnabled(true, opts as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(opts)}`,
    );
  }
  assert.equal(bus.getStats().traceArchiveEnabled, false);
  assert.deepEqual(bus.getTraceArchive(), []);
  assert.equal(liveLines(dir), 0);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 0);
  // A valid attach afterwards still works.
  bus.setTraceArchiveEnabled(true, { maxEntries: 100 });
  assert.equal(bus.getStats().traceArchiveEnabled, true);
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 5);
});

test('a repeat attach or detach is a no-op with no audit, and opts are ignored', () => {
  const bus = unarchivedBus(tmpLogDir());
  bus.setTraceArchiveEnabled(true, { maxEntries: 100 });
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 1);
  // A repeat attach moves nothing — not even the bounds in `opts`.
  bus.setTraceArchiveEnabled(true, { maxEntries: 5 });
  bus.setTraceArchiveEnabled(true);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 1);
  assert.equal(bus.getStats().traceArchiveEnabled, true);
  // Illegal opts are still refused even on a repeat attach: validation
  // runs before the same-state no-op check.
  assert.throws(() => bus.setTraceArchiveEnabled(true, { maxEntries: 0 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 1);
  bus.setTraceArchiveEnabled(false);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 2);
  bus.setTraceArchiveEnabled(false);
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' }).length, 2);
  assert.equal(bus.getStats().traceArchiveEnabled, false);
});

test('attaching journals the next span, under the attached bounds', async () => {
  const dir = tmpLogDir();
  const bus = unarchivedBus(dir);
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  // No journal yet: nothing was archived.
  assert.deepEqual(bus.getTraceArchive(), []);
  assert.equal(liveLines(dir), 0);
  bus.setTraceArchiveEnabled(true, { maxEntries: 5, maxBytes: 100_000 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 5);
  assert.ok(bus.getTraceArchive().every((s) => (s.attrs.seq as number) === 2));
  assert.equal(liveLines(dir), 5);
  bus.publish('t', { n: 3 });
  await flush();
  // The in-memory view is capped at the attached maxEntries of 5 —
  // the newest spans only — while the live file keeps every record.
  assert.equal(bus.getTraceArchive().length, 5);
  assert.ok(bus.getTraceArchive().every((s) => (s.attrs.seq as number) === 3));
  assert.equal(liveLines(dir), 10);
});

test('attaching back-fills a span pre-placed in the live file', () => {
  const dir = tmpLogDir();
  const line = JSON.stringify({
    v: 1,
    traceId: 'a'.repeat(32),
    spanId: '0123456789abcdef',
    name: 'bus.publish',
    at: 1000,
    durationMs: 2,
    attrs: { topic: 't', seq: 1 },
  });
  writeFileSync(join(dir, TRACE_ARCHIVE_LIVE_NAME), `${line}\n`);
  const bus = unarchivedBus(dir);
  assert.deepEqual(bus.getTraceArchive(), []);
  bus.setTraceArchiveEnabled(true);
  const archived = bus.getTraceArchive();
  assert.equal(archived.length, 1);
  assert.equal(archived[0].traceId, 'a'.repeat(32));
  assert.equal(archived[0].spanId, '0123456789abcdef');
  assert.equal(archived[0].name, 'bus.publish');
  assert.equal(archived[0].attrs.seq, 1);
});

test('attaching back-fills spans written by an earlier journal on the same directory', async () => {
  const dir = tmpLogDir();
  const first = new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true },
  });
  first.subscribe('t', () => {});
  first.publish('t', { n: 1 });
  await flush();
  assert.equal(first.getTraceArchive().length, 5);
  assert.equal(liveLines(dir), 5);
  // A second bus on the same directory, built without an archive,
  // sees nothing until it attaches — then the earlier spans are back.
  const second = unarchivedBus(dir);
  assert.deepEqual(second.getTraceArchive(), []);
  second.setTraceArchiveEnabled(true);
  const archived = second.getTraceArchive();
  assert.equal(archived.length, 5);
  assert.ok(archived.every((s) => (s.attrs.seq as number) === 1));
});

test('detaching stops journaling and re-attaching recovers everything', async () => {
  const dir = tmpLogDir();
  const bus = unarchivedBus(dir);
  bus.subscribe('t', () => {});
  bus.setTraceArchiveEnabled(true);
  bus.publish('t', { n: 1 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 5);
  assert.equal(liveLines(dir), 5);
  bus.setTraceArchiveEnabled(false);
  assert.equal(bus.getStats().traceArchiveEnabled, false);
  bus.publish('t', { n: 2 });
  await flush();
  // The read view went away with the reference, and the live file
  // stopped growing — the seq-2 spans were never journaled.
  assert.deepEqual(bus.getTraceArchive(), []);
  assert.equal(liveLines(dir), 5);
  bus.setTraceArchiveEnabled(true);
  assert.equal(bus.getStats().traceArchiveEnabled, true);
  assert.equal(bus.getTraceArchive().length, 5);
  assert.ok(bus.getTraceArchive().every((s) => (s.attrs.seq as number) === 1));
  bus.publish('t', { n: 3 });
  await flush();
  assert.equal(bus.getTraceArchive().length, 10);
  assert.equal(bus.getTraceArchive().filter((s) => (s.attrs.seq as number) === 2).length, 0);
  assert.equal(liveLines(dir), 10);
});

test('attaching and detaching leave sampleRate, bufferSize, sampler, callback and the ring untouched', async () => {
  const headSeen: Array<{ name: string; seq: number }> = [];
  const allowAll = () => true;
  const bus = new EventBus({
    durableLogDir: tmpLogDir(),
    trace: {
      sampleRate: 0,
      bufferSize: 6,
      sampler: allowAll,
      onTraceSpan: (span) => headSeen.push({ name: span.name, seq: span.attrs.seq as number }),
    },
  });
  bus.publish('orders', 1);
  await flush();
  const ringBefore = bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]);
  assert.equal(ringBefore.length, 3);
  const headBefore = headSeen.length;
  bus.setTraceArchiveEnabled(true);
  assert.deepEqual(
    bus.getStats().traceSpans.map((s) => [s.name, s.attrs.seq as number]),
    ringBefore,
  );
  bus.publish('orders', 2);
  await flush();
  // Still traced (the sampler survived the attach despite rate 0),
  // and the ring is still capped at the live bufferSize of 6.
  assert.equal(bus.getStats().traceSpans.length, 6);
  assert.equal(bus.getStats().traceHasCustomSampler, true);
  assert.ok(headSeen.length > headBefore);
  bus.setTraceArchiveEnabled(false);
  assert.equal(bus.getStats().traceSpans.length, 6);
  bus.publish('orders', 3);
  await flush();
  assert.equal(bus.getStats().traceSpans.length, 6);
  assert.ok(bus.getStats().traceSpans.every((s) => [2, 3].includes(s.attrs.seq as number)));
  assert.equal(bus.getStats().traceHasCustomSampler, true);
  assert.ok(headSeen.length > headBefore);
});

test('setTraceArchive works only while the journal is attached', () => {
  const bus = unarchivedBus(tmpLogDir());
  assert.throws(() => bus.setTraceArchive({ maxEntries: 5 }), RangeError);
  bus.setTraceArchiveEnabled(true);
  bus.setTraceArchive({ maxEntries: 100 });
  assert.equal(bus.getSubscriptionAudit({ action: 'trace_archive_revised' }).length, 1);
  bus.setTraceArchiveEnabled(false);
  assert.throws(() => bus.setTraceArchive({ maxEntries: 5 }), RangeError);
});

test('the switch audits exact switch flags with the actor', () => {
  const bus = unarchivedBus(tmpLogDir());
  bus.setTraceArchiveEnabled(true, undefined, 'ops');
  bus.setTraceArchiveEnabled(false, undefined, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'trace_archive_enabled_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { enabled: true, oldEnabled: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { enabled: false, oldEnabled: true });
  assert.equal(audit[1].actor, 'ops');
});

test('getStats().traceArchiveEnabled follows the live value', () => {
  const plain = new EventBus();
  assert.equal(plain.getStats().traceArchiveEnabled, false);
  const traced = new EventBus({ trace: { sampleRate: 1 } });
  assert.equal(traced.getStats().traceArchiveEnabled, false);
  const dir = tmpLogDir();
  const bus = unarchivedBus(dir);
  assert.equal(bus.getStats().traceArchiveEnabled, false);
  bus.setTraceArchiveEnabled(true);
  assert.equal(bus.getStats().traceArchiveEnabled, true);
  bus.setTraceArchiveEnabled(false);
  assert.equal(bus.getStats().traceArchiveEnabled, false);
  // A bus with an archive at construction reports true.
  const archived = new EventBus({
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true },
  });
  assert.equal(archived.getStats().traceArchiveEnabled, true);
});
