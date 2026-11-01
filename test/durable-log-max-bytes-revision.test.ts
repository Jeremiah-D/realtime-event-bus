/**
 * Bus-level durable-log per-topic byte-budget revision (EB-323).
 *
 * `setDurableLogMaxBytesPerTopic(maxBytesPerTopic, actor?)` retunes how
 * many live bytes each topic retains before the log compacts away the
 * oldest (or, with segment rotation on, archives the live segment) —
 * without restarting the bus. Only the bound moves: the amortized 2x
 * compact trigger, the rotation trigger, and the newest-first
 * byte-budget fill all keep reading the bound live, and the records
 * already persisted are untouched — tightening takes effect from the
 * next append, loosening simply allows more capacity, and passing
 * `undefined` clears the byte bound and returns to count-only
 * retention. The revision fans out to the root log and every namespace
 * child log, and a namespace registered later opens with the live bus
 * bound. A bus without `durableLogDir` refuses fail-closed; illegal
 * values fail closed with the live bound untouched; an identical
 * revision is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-max-bytes-'));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_max_bytes_revised' });

/** Reopens a log directory and reads back one topic's persisted records. */
const readBack = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0);

/** Reopens a log directory and reads back one topic including archives. */
const readBackArchived = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0, { includeArchived: true });

/**
 * The bus's live per-topic byte budget — read off the root log itself.
 * (A reopened log reports the construction-time bound: the revised bound
 * is runtime-only, like every sibling revision.)
 */
const liveBound = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog.currentMaxBytesPerTopic();

/** The live per-topic byte budget of one namespace's child log. */
const liveChildBound = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!
    .currentMaxBytesPerTopic();

const childDir = (dir: string, prefix: string) =>
  join(dir, 'namespaces', encodeURIComponent(prefix));

/** Fixed-shape record: one line's on-disk bytes are stable across seqs. */
const payloadOf = (i: number): string => `p-${i}-` + 'x'.repeat(100);

/** Max on-disk line bytes for the probe workload, used to size budgets. */
function maxLineBytes(dir: string, topic: string): number {
  const lines = readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0);
  assert.ok(lines.length > 0);
  return Math.max(...lines.map((l) => Buffer.byteLength(`${l}\n`, 'utf8')));
}

/**
 * Max on-disk line bytes for `payloadOf` records as the bus itself
 * writes them (real timestamps, real topic name) — probed through a
 * throwaway bus so budgets sized from it fit exactly three newest
 * records and no fourth. A raw `DurableTopicLog` probe would
 * under-measure: its fixed `at: 1000` and short topic name write
 * shorter lines than the bus does.
 */
function probeBusLineMax(): number {
  const dir = freshDir();
  const probe = new EventBus({ durableLogDir: dir });
  for (let i = 1; i <= 12; i++) probe.publish('hot', payloadOf(i));
  const max = maxLineBytes(dir, 'hot');
  probe.close();
  return max;
}

/** Same probe for a namespace child log (`t1/orders`). */
function probeChildLineMax(): number {
  const dir = freshDir();
  const probe = new EventBus({ durableLogDir: dir });
  const ns = probe.createNamespace('t1');
  for (let i = 1; i <= 12; i++) ns.publish('orders', payloadOf(i));
  const max = maxLineBytes(childDir(dir, 't1'), 't1/orders');
  probe.close();
  return max;
}

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

test('constructor still rejects illegal durableLogMaxBytesPerTopic', () => {
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '1000']) {
    assert.throws(
      () => new EventBus({ durableLogDir: freshDir(), durableLogMaxBytesPerTopic: bad as never }),
      RangeError,
    );
  }
  // The option is silently ignored without a dir at construction time…
  assert.throws(() => new EventBus({ durableLogMaxBytesPerTopic: 100 }), RangeError);
});

test('DurableTopicLog.setMaxBytesPerTopic validates, returns the old bound, and leaves it untouched on illegal values', () => {
  const log = DurableTopicLog.open({ dir: freshDir(), maxBytesPerTopic: 5000 });
  assert.equal(log.currentMaxBytesPerTopic(), 5000);
  assert.equal(log.setMaxBytesPerTopic(3000), 5000);
  assert.equal(log.currentMaxBytesPerTopic(), 3000);
  // Clearing restores count-only retention.
  assert.equal(log.setMaxBytesPerTopic(undefined), 3000);
  assert.equal(log.currentMaxBytesPerTopic(), undefined);
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '3000']) {
    assert.throws(() => log.setMaxBytesPerTopic(bad as never), RangeError);
  }
  // The failed calls changed nothing.
  assert.equal(log.currentMaxBytesPerTopic(), undefined);
});

test('setDurableLogMaxBytesPerTopic requires durableLogDir and a valid actor', () => {
  const noLog = new EventBus();
  assert.throws(() => noLog.setDurableLogMaxBytesPerTopic(5000), RangeError);
  assert.throws(
    () => noLog.setDurableLogMaxBytesPerTopic(5000, ''),
    /actor must be a non-empty string/,
  );
  noLog.close();

  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => bus.setDurableLogMaxBytesPerTopic(5000, '' as never), RangeError);
  assert.throws(() => bus.setDurableLogMaxBytesPerTopic(5000, 42 as never), RangeError);
  // The failed calls changed nothing: no audit, no bound.
  assert.equal(audit(bus).length, 0);
  assert.equal(liveBound(bus), undefined);
  bus.close();
});

test('revision rejects illegal values and leaves the live bound and audit untouched', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogMaxBytesPerTopic: 3000 });
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '1000']) {
    assert.throws(() => bus.setDurableLogMaxBytesPerTopic(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  assert.equal(liveBound(bus), 3000);
  // A valid revision afterwards still works on the untouched bound.
  bus.setDurableLogMaxBytesPerTopic(7000, 'ops');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { from: 3000, to: 7000 });
  assert.equal(audit(bus)[0].actor, 'ops');
  assert.equal(liveBound(bus), 7000);
  bus.close();
});

test('an identical revision is a no-op with no audit', () => {
  const bus = new EventBus({ durableLogDir: freshDir(), durableLogMaxBytesPerTopic: 5000 });
  bus.setDurableLogMaxBytesPerTopic(5000);
  assert.equal(audit(bus).length, 0);

  const defaulted = new EventBus({ durableLogDir: freshDir() });
  // No byte bound by default: clearing an already-unset bound is a no-op.
  defaulted.setDurableLogMaxBytesPerTopic(undefined);
  assert.equal(audit(defaulted).length, 0);
  bus.close();
  defaulted.close();
});

test('revision audits from/to with null when unset and moves the root log', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogMaxBytesPerTopic(5000);
  bus.setDurableLogMaxBytesPerTopic(undefined);
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, { from: null, to: 5000 });
  assert.deepEqual(entries[1].detail, { from: 5000, to: null });
  assert.equal(liveBound(bus), undefined);
  assert.equal(bus.getStats().durableLog.bytesLimit, undefined);
  bus.close();
});

test('revision fans out to namespace child logs', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 'one');
  bus.setDurableLogMaxBytesPerTopic(7000);
  assert.equal(liveBound(bus), 7000);
  assert.equal(liveChildBound(bus, 't1'), 7000);
  assert.deepEqual(audit(bus)[0].detail, { from: null, to: 7000 });
  assert.equal(bus.getStats().durableLog.bytesLimit, 7000);
  bus.close();
});

test('a namespace registered after the revision opens with the live bound', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogMaxBytesPerTopic(4000);
  bus.createNamespace('t2');
  assert.equal(liveChildBound(bus, 't2'), 4000);
  bus.close();
});

test('tightening takes effect from the next append-triggered compact; in-flight records untouched', () => {
  const lineMax = probeBusLineMax();
  const budget = 3 * lineMax;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 10; i += 1) bus.publish('hot', payloadOf(i));
  assert.equal(readBack(dir, 'hot').length, 10);
  bus.setDurableLogMaxBytesPerTopic(budget);
  // No immediate compact: the ten persisted records are still there.
  assert.equal(readBack(dir, 'hot').length, 10);
  assert.equal(bus.getStats().durableLog.bytesLimit, budget);
  // The next append trips the 2x trigger (11 lines > 2 * 3 lines) and
  // compacts down to the newest records whose bytes fit the budget.
  bus.publish('hot', payloadOf(10));
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [payloadOf(8), payloadOf(9), payloadOf(10)],
  );
  bus.close();
});

test('loosening allows more capacity from the next append', () => {
  const lineMax = probeBusLineMax();
  const budget = 3 * lineMax;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogMaxBytesPerTopic: budget });
  for (let i = 0; i <= 10; i += 1) bus.publish('hot', payloadOf(i));
  // Two compactions under the tight bound leave only the newest 3.
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [payloadOf(8), payloadOf(9), payloadOf(10)],
  );
  bus.setDurableLogMaxBytesPerTopic(1_000_000);
  for (let i = 11; i <= 15; i += 1) bus.publish('hot', payloadOf(i));
  // No compact under the loosened bound: all eight records survive.
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [8, 9, 10, 11, 12, 13, 14, 15].map((i) => payloadOf(i)),
  );
  bus.close();
});

test('clearing with undefined returns to count-only retention', () => {
  const lineMax = probeBusLineMax();
  const budget = 3 * lineMax;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogMaxBytesPerTopic: budget });
  for (let i = 0; i <= 10; i += 1) bus.publish('hot', payloadOf(i));
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [payloadOf(8), payloadOf(9), payloadOf(10)],
  );
  bus.setDurableLogMaxBytesPerTopic(undefined);
  assert.deepEqual(audit(bus)[0].detail, { from: budget, to: null });
  assert.equal(bus.getStats().durableLog.bytesLimit, undefined);
  // With no byte bound, eleven more appends compact nothing (the count
  // bound is far away): every record survives.
  for (let i = 11; i <= 21; i += 1) bus.publish('hot', payloadOf(i));
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21].map((i) => payloadOf(i)),
  );
  bus.close();
});

test('a namespace child log compacts under the revised bound', () => {
  const lineMax = probeChildLineMax();
  const budget = 3 * lineMax;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const ns = bus.createNamespace('t1');
  for (let i = 0; i < 10; i += 1) ns.publish('orders', payloadOf(i));
  assert.equal(readBack(childDir(dir, 't1'), 't1/orders').length, 10);
  bus.setDurableLogMaxBytesPerTopic(budget);
  assert.equal(readBack(childDir(dir, 't1'), 't1/orders').length, 10);
  ns.publish('orders', payloadOf(10));
  assert.deepEqual(
    readBack(childDir(dir, 't1'), 't1/orders').map((r) => r.payload),
    [payloadOf(8), payloadOf(9), payloadOf(10)],
  );
  bus.close();
});

test('with rotation on, crossing the revised bound archives instead of evicting', async () => {
  const lineMax = probeBusLineMax();
  const budget = 3 * lineMax;
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 1000 },
  });
  for (let i = 0; i < 10; i += 1) bus.publish('hot', payloadOf(i));
  assert.equal(readBack(dir, 'hot').length, 10);
  bus.setDurableLogMaxBytesPerTopic(budget);
  // The next append crosses the revised bound: the live segment is
  // archived (nothing evicted) and a fresh live segment starts.
  bus.publish('hot', payloadOf(10));
  const prefix = `${encodeURIComponent('hot')}.seg-`;
  await waitFor(
    () =>
      readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.tmp')).length === 0 &&
      readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz')).length >= 1,
  );
  assert.equal(readBack(dir, 'hot').length, 0);
  // Nothing was lost: the archived segment replays all eleven records.
  assert.deepEqual(
    readBackArchived(dir, 'hot').map((r) => r.payload),
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map((i) => payloadOf(i)),
  );
  bus.close();
});
