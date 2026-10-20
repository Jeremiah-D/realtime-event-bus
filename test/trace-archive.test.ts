import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync, appendFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type TraceSpan } from '../src/bus.ts';
import {
  TraceSpanJournal,
  parseTraceSpanLine,
  assertValidTraceArchiveQuery,
  TRACE_ARCHIVE_LIVE_NAME,
} from '../src/tracearchive.ts';
import { resolveTraceOptions } from '../src/trace.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function tmpLogDir(): string {
  return mkdtempSync(join(tmpdir(), 'eb167-'));
}

const TRACE_ID = 'a'.repeat(32);
const SPAN_ID = 'b'.repeat(16);

function sampleSpan(overrides: Partial<TraceSpan> = {}): TraceSpan {
  return {
    traceId: TRACE_ID,
    spanId: SPAN_ID,
    name: 'bus.publish',
    at: 1000,
    durationMs: 2,
    attrs: { topic: 't', seq: 1 },
    ...overrides,
  };
}

// --- resolveTraceOptions: archive normalization ---

test('resolveTraceOptions normalizes the trace.archive option', () => {
  // The `true` fast path keeps its exact resolved shape (no archive key).
  assert.deepEqual(resolveTraceOptions(true), {
    sampleRate: 1,
    sampler: undefined,
    onTraceSpan: undefined,
    bufferSize: 1024,
  });
  assert.deepEqual(resolveTraceOptions({ archive: true })?.archive, {});
  assert.deepEqual(resolveTraceOptions({ archive: { maxEntries: 5 } })?.archive, {
    maxEntries: 5,
  });
  // Absent / false archive leaves the key unset.
  assert.equal('archive' in (resolveTraceOptions({}) ?? {}), false);
  assert.equal('archive' in (resolveTraceOptions({ archive: false }) ?? {}), false);
  // Disabled tracing wins over archive.
  assert.equal(resolveTraceOptions({ enabled: false, archive: true }), undefined);
  // Malformed archive values fail fast.
  assert.throws(() => resolveTraceOptions({ archive: 'yes' }), RangeError);
  assert.throws(() => resolveTraceOptions({ archive: [] }), RangeError);
  assert.throws(() => resolveTraceOptions({ archive: 1 }), RangeError);
  // Other fields are still validated when archive is present.
  assert.throws(() => resolveTraceOptions({ archive: true, sampleRate: 2 }), RangeError);
});

// --- parseTraceSpanLine ---

test('parseTraceSpanLine accepts valid lines and rejects corrupt ones', () => {
  const good = JSON.stringify({ v: 1, ...sampleSpan(), parentId: 'c'.repeat(16) });
  const parsed = parseTraceSpanLine(good);
  assert.equal(parsed?.traceId, TRACE_ID);
  assert.equal(parsed?.parentId, 'c'.repeat(16));
  assert.deepEqual(parsed?.attrs, { topic: 't', seq: 1 });

  const bad = [
    'not json{',
    JSON.stringify({ v: 2, ...sampleSpan() }),
    JSON.stringify({ ...sampleSpan(), v: 1, traceId: 'xyz' }),
    JSON.stringify({ ...sampleSpan(), v: 1, spanId: 'short' }),
    JSON.stringify({ ...sampleSpan(), v: 1, name: '' }),
    JSON.stringify({ ...sampleSpan(), v: 1, at: 'now' }),
    JSON.stringify({ ...sampleSpan(), v: 1, durationMs: -1 }),
    JSON.stringify({ ...sampleSpan(), v: 1, attrs: { nested: { deep: 1 } } }),
    JSON.stringify({ ...sampleSpan(), v: 1, attrs: [1, 2] }),
    JSON.stringify({ ...sampleSpan(), v: 1, parentId: 42 }),
  ];
  for (const line of bad) {
    assert.equal(parseTraceSpanLine(line), undefined, `line should be corrupt: ${line}`);
  }
});

test('assertValidTraceArchiveQuery rejects illegal queries', () => {
  assert.throws(() => assertValidTraceArchiveQuery({ traceId: 'nope' }), RangeError);
  assert.throws(() => assertValidTraceArchiveQuery({ since: -1 }), RangeError);
  assert.throws(() => assertValidTraceArchiveQuery({ until: Number.NaN }), RangeError);
  assert.throws(() => assertValidTraceArchiveQuery({ limit: 0 }), RangeError);
  assert.doesNotThrow(() =>
    assertValidTraceArchiveQuery({ traceId: TRACE_ID, since: 0, until: 999, limit: 10 }),
  );
});

// --- TraceSpanJournal unit behavior ---

test('TraceSpanJournal records, queries, and counts corrupt lines on recovery', () => {
  const dir = tmpLogDir();
  const journal = new TraceSpanJournal(dir, {});
  journal.record(sampleSpan());
  journal.record(sampleSpan({ spanId: 'd'.repeat(16), name: 'bus.deliver', at: 1500 }));
  assert.equal(journal.size(), 2);
  assert.equal(journal.query({ traceId: TRACE_ID }).length, 2);
  assert.equal(journal.query({ traceId: 'f'.repeat(32) }).length, 0);
  assert.equal(journal.query({ since: 1200 }).length, 1);
  assert.equal(journal.query({ until: 1200 }).length, 1);
  assert.equal(journal.query({ limit: 1 })[0].name, 'bus.deliver');
  assert.equal(journal.writeErrors(), 0);
  assert.equal(journal.corruptLineCount(), 0);

  // Corrupt line on disk: recovery skips it and counts it.
  appendFileSync(join(dir, TRACE_ARCHIVE_LIVE_NAME), 'garbage line\n', 'utf8');
  const recovered = new TraceSpanJournal(dir, {});
  assert.equal(recovered.size(), 2);
  assert.equal(recovered.corruptLineCount(), 1);
  assert.equal(recovered.query({ traceId: TRACE_ID }).length, 2);
});

test('TraceSpanJournal rotates the live file past its byte budget', () => {
  const dir = tmpLogDir();
  const journal = new TraceSpanJournal(dir, { maxBytes: 120 });
  for (let i = 0; i < 4; i++) {
    journal.record(sampleSpan({ spanId: String(i).repeat(16), at: 1000 + i }));
  }
  const files = readdirSync(dir);
  const archived = files.filter((f) => f !== TRACE_ARCHIVE_LIVE_NAME);
  assert.ok(archived.length >= 1, `expected an archived file, got: ${files}`);
  assert.match(archived[0], /^__trace_spans\.\d{4}-\d{2}-\d{2}\.\d+\.jsonl$/);
  // Recovery reads archived files first, then the live file.
  const recovered = new TraceSpanJournal(dir, {});
  assert.equal(recovered.size(), 4);
  assert.equal(recovered.corruptLineCount(), 0);
});

test('TraceSpanJournal record never throws on disk failure', () => {
  const journal = new TraceSpanJournal('/tmp/eb167-ok', {});
  // Point at an unusable directory by sabotage: record into a path that is a file.
  const filePath = join(tmpdir(), `eb167-file-${Date.now()}`);
  appendFileSync(filePath, 'x', 'utf8');
  const broken = new TraceSpanJournal(filePath, {});
  broken.record(sampleSpan());
  assert.ok(broken.writeErrors() >= 1);
  // In-memory entry still lands.
  assert.equal(broken.size(), 1);
});

// --- bus integration ---

test('trace.archive journals every sampled span; getTraceArchive queries by traceId', async () => {
  const clock = controllableClock();
  const dir = tmpLogDir();
  const seen: TraceSpan[] = [];
  const bus = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    trace: {
      sampleRate: 1,
      archive: true,
      onTraceSpan: (span) => seen.push(span),
    },
  });
  bus.subscribe('orders.*', () => {});
  bus.publish('orders.new', { id: 1 });
  bus.publish('orders.cancel', { id: 2 });
  await flush();

  const archived = bus.getTraceArchive();
  assert.ok(archived.length > 0);
  // Journal sees exactly what the callback saw.
  assert.equal(archived.length, seen.length);
  const names = new Set(archived.map((s) => s.name));
  for (const expected of ['bus.publish', 'bus.admission', 'bus.fanout', 'bus.enqueue', 'bus.deliver']) {
    assert.ok(names.has(expected), `missing span ${expected}`);
  }
  // Query by trace id isolates one publish's spans.
  const oneTraceId = archived[0].traceId;
  const only = bus.getTraceArchive({ traceId: oneTraceId });
  assert.ok(only.length > 0);
  assert.ok(only.every((s) => s.traceId === oneTraceId));
  assert.ok(only.length < archived.length);
  // Live file exists on disk.
  const files = readdirSync(dir);
  assert.ok(files.includes(TRACE_ARCHIVE_LIVE_NAME), `files: ${files}`);
  // Stats counters are clean.
  assert.equal(bus.getStats().traceArchiveWriteErrors, 0);
  assert.equal(bus.getStats().traceArchiveCorruptLines, 0);
});

test('trace.archive requires durableLogDir and is disabled by default', () => {
  assert.throws(() => new EventBus({ trace: { archive: true } }), RangeError);
  assert.throws(
    () => new EventBus({ trace: { archive: { maxEntries: 0 } }, durableLogDir: tmpLogDir() }),
    RangeError,
  );
  const plain = new EventBus({ trace: true });
  assert.deepEqual(plain.getTraceArchive(), []);
  assert.equal(plain.getStats().traceArchiveWriteErrors, 0);
  assert.equal(plain.getStats().traceArchiveCorruptLines, 0);
});

test('trace archive survives restart: recovery back-fills the read view', async () => {
  const clock = controllableClock();
  const dir = tmpLogDir();
  const bus = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  await flush();
  const before = bus.getTraceArchive();
  assert.ok(before.length > 0);

  const restarted = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true },
  });
  const after = restarted.getTraceArchive();
  assert.equal(after.length, before.length);
  assert.deepEqual(
    after.map((s) => s.spanId).sort(),
    before.map((s) => s.spanId).sort(),
  );
});

test('trace archive rotation recovers archived files (dashed day names)', async () => {
  const clock = controllableClock(1_700_000_000_000);
  const dir = tmpLogDir();
  const bus = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: { maxBytes: 200 } },
  });
  bus.subscribe('t', () => {});
  for (let i = 0; i < 6; i++) bus.publish('t', { n: i });
  await flush();
  const archivedFiles = readdirSync(dir).filter((f) => f !== TRACE_ARCHIVE_LIVE_NAME);
  assert.ok(archivedFiles.length >= 1, 'expected rotation to archive a file');

  const restarted = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true },
  });
  // Archived spans are recovered (unlike EB-138's publish audit, whose
  // recovery regex never matches its own dashed file names).
  assert.equal(restarted.getTraceArchive().length, bus.getTraceArchive().length);
  assert.equal(restarted.getStats().traceArchiveCorruptLines, 0);
});

test('getTraceArchive validates queries and supports time windows', async () => {
  const clock = controllableClock();
  const dir = tmpLogDir();
  const bus = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    trace: { sampleRate: 1, archive: true },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', { n: 1 });
  clock.nowMs += 5000;
  bus.publish('t', { n: 2 });
  await flush();

  assert.throws(() => bus.getTraceArchive({ traceId: 'bogus' }), RangeError);
  assert.throws(() => bus.getTraceArchive({ limit: 0 }), RangeError);
  const first = bus.getTraceArchive({ until: clock.nowMs - 2500 });
  const second = bus.getTraceArchive({ since: clock.nowMs - 2500 });
  assert.ok(first.length > 0 && second.length > 0);
  assert.ok(first.length + second.length >= bus.getTraceArchive().length);
  const limited = bus.getTraceArchive({ limit: 2 });
  assert.equal(limited.length, 2);
});
