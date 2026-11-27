/**
 * Bus-level durable-log segment-rotation live enable (EB-566).
 *
 * `setDurableLogSegmentRotation(patch, actor?)` on a bus constructed
 * without rotation installs the triggers instead of refusing: each
 * topic's live-segment identity is seeded from the records already on
 * disk (the live entry count, and the first well-formed record's seq /
 * timestamp), so the pre-existing live file becomes the new segment's
 * first segment — the next append is judged against the real stored
 * history, not counted from 0. Enabling fans out to the root log and
 * every namespace child log, audits `durable_log_segment_rotation_revised`
 * with `from` all `null`, and is live-only: a restart restores the
 * unconfigured state. A closed bus enables exactly like the EB-355
 * revision does (no closed gate); an illegal patch refuses with the bus
 * unconfigured; a patch with neither field is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-seg-rot-enable-'));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_segment_rotation_revised' });

/** Reopens a log directory and reads back one topic's live records. */
const readBack = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0);

/** Reopens a log directory and reads back one topic's records including archives. */
const readAll = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0, { includeArchived: true });

/** The bus's live rotation triggers — read off the root log itself. */
const liveRotation = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog.currentSegmentRotation();

/** The live rotation triggers of one namespace's child log. */
const liveChildRotation = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!
    .currentSegmentRotation();

/** The child log object of one namespace. */
const childLog = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!;

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

const segmentFiles = (dir: string, ext: '.jsonl.gz' | '.jsonl.tmp') =>
  readdirSync(dir).filter((f) => f.endsWith(ext));

/** Waits until the topic's closed segment is fully archived (gz + index, tmp gone). */
async function waitForArchived(dir: string, topic: string): Promise<void> {
  const prefix = `${encodeURIComponent(topic)}.seg-`;
  await waitFor(
    () =>
      segmentFiles(dir, '.jsonl.tmp').filter((f) => f.startsWith(prefix)).length === 0 &&
      segmentFiles(dir, '.jsonl.gz').filter((f) => f.startsWith(prefix)).length > 0,
  );
}

/** The archive index entries for one topic, in index order. */
const archiveIndex = (dir: string, topic: string) =>
  readFileSync(join(dir, '__archive_index.jsonl'), 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line))
    .filter((entry) => entry.topic === topic);

test('enabling on an empty bus rotates at the new budget', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 2 }, 'ops');
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 2 });
  bus.publish('hot', 0);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0],
  );
  bus.publish('hot', 1);
  assert.deepEqual(readBack(dir, 'hot'), []);
  bus.publish('hot', 2);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [2],
  );
  bus.close();
});

test('stored records seed the segment count and the archived segment keeps the real first seq', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('hot', i);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0, 1, 2],
  );
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 });
  // No immediate rotation: the three stored records are still live.
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0, 1, 2],
  );
  // Two more appends reach the seeded count of 5 — the segment closes.
  // Had the count started at 0, five more appends would be needed.
  bus.publish('hot', 3);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0, 1, 2, 3],
  );
  bus.publish('hot', 4);
  assert.deepEqual(readBack(dir, 'hot'), []);
  await waitForArchived(dir, 'hot');
  const entries = archiveIndex(dir, 'hot');
  assert.equal(entries.length, 1);
  assert.equal(entries[0].segmentId, '1-5');
  assert.equal(entries[0].seqStart, 1);
  assert.equal(entries[0].seqEnd, 5);
  assert.equal(entries[0].entries, 5);
  // The full history still replays, live + archived in seq order.
  assert.deepEqual(
    readAll(dir, 'hot').map((r) => r.payload),
    [0, 1, 2, 3, 4],
  );
  bus.close();
});

test('only maxSegmentAgeMs: a stored old segment rotates on the next append', () => {
  const dir = freshDir();
  const clock = { nowMs: 1000, now: () => clock.nowMs };
  const bus = new EventBus({ durableLogDir: dir, now: clock.now });
  bus.publish('hot', 'a'); // segment starts at t=1000.
  clock.nowMs = 6000;
  bus.setDurableLogSegmentRotation({ maxSegmentAgeMs: 2000 });
  // No immediate rotation: the stored record is still live.
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    ['a'],
  );
  // At t=6000 the stored segment is 5000ms old — past the 2000ms age,
  // judged on the real first record's timestamp, so this append closes it.
  bus.publish('hot', 'b');
  assert.deepEqual(readBack(dir, 'hot'), []);
  bus.publish('hot', 'c');
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    ['c'],
  );
  bus.close();
});

test('a partial enable leaves the other trigger null in the live value and the audit', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 }, 'ops');
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 5 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    from: { maxEntriesPerSegment: null, maxSegmentAgeMs: null },
    to: { maxEntriesPerSegment: 5, maxSegmentAgeMs: null },
  });
  bus.close();

  const ageOnly = new EventBus({ durableLogDir: freshDir() });
  ageOnly.setDurableLogSegmentRotation({ maxSegmentAgeMs: 2000 });
  assert.deepEqual(liveRotation(ageOnly), { maxSegmentAgeMs: 2000 });
  assert.deepEqual(audit(ageOnly)[0].detail, {
    from: { maxEntriesPerSegment: null, maxSegmentAgeMs: null },
    to: { maxEntriesPerSegment: null, maxSegmentAgeMs: 2000 },
  });
  ageOnly.close();
});

test('illegal values refuse and stay unconfigured, then a valid enable works', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  for (const bad of [
    { maxEntriesPerSegment: 0 },
    { maxEntriesPerSegment: -1 },
    { maxEntriesPerSegment: 1.5 },
    { maxEntriesPerSegment: Number.NaN },
    { maxEntriesPerSegment: '3' },
    { maxSegmentAgeMs: 0 },
    { maxSegmentAgeMs: -1 },
    { maxSegmentAgeMs: Number.NaN },
    { maxSegmentAgeMs: Infinity },
    { maxEntriesPerSegment: 0, maxSegmentAgeMs: 0 },
    { maxEntriesPerSegment: undefined },
  ]) {
    assert.throws(() => bus.setDurableLogSegmentRotation(bad as never), RangeError);
  }
  assert.equal(liveRotation(bus), undefined);
  assert.equal(audit(bus).length, 0);
  // The failed calls changed nothing: a valid enable still works.
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 }, 'ops');
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 5 });
  assert.equal(audit(bus).length, 1);
  bus.close();
});

test('an empty patch is a no-op with no audit and stays unconfigured', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.setDurableLogSegmentRotation({});
  assert.equal(liveRotation(bus), undefined);
  assert.equal(audit(bus).length, 0);
  // A valid enable still works afterwards.
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 });
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 5 });
  bus.close();

  const log = DurableTopicLog.open({ dir: freshDir() });
  assert.equal(log.installSegmentRotation({}), undefined);
  assert.equal(log.currentSegmentRotation(), undefined);
});

test('enabling then revising follows the existing revision path', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 10 }, 'ops');
  for (let i = 0; i < 3; i += 1) bus.publish('hot', i);
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 3 });
  // The tightened budget rotates on the next append (4 >= 3).
  bus.publish('hot', 3);
  assert.deepEqual(readBack(dir, 'hot'), []);
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, {
    from: { maxEntriesPerSegment: null, maxSegmentAgeMs: null },
    to: { maxEntriesPerSegment: 10, maxSegmentAgeMs: null },
  });
  assert.deepEqual(entries[1].detail, {
    from: { maxEntriesPerSegment: 10, maxSegmentAgeMs: null },
    to: { maxEntriesPerSegment: 3, maxSegmentAgeMs: null },
  });
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 3 });
  bus.close();
});

test('enabling fans out to namespace child logs and seeds their stored segments', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const ns = bus.createNamespace('t1');
  for (let i = 0; i < 3; i += 1) ns.publish('orders', i);
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 });
  assert.deepEqual(liveChildRotation(bus, 't1'), { maxEntriesPerSegment: 5 });
  // The child's stored 3 count toward the budget: two more close it.
  // (The child log keys topics as `<namespace>/<topic>`.)
  ns.publish('orders', 3);
  assert.deepEqual(
    childLog(bus, 't1').readSince('t1/orders', 0).map((r) => r.payload),
    [0, 1, 2, 3],
  );
  ns.publish('orders', 4);
  assert.deepEqual(childLog(bus, 't1').readSince('t1/orders', 0), []);
  ns.publish('orders', 5);
  assert.deepEqual(
    childLog(bus, 't1').readSince('t1/orders', 0).map((r) => r.payload),
    [5],
  );
  bus.close();
});

test('a namespace registered after enabling opens with the live triggers', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 2 });
  const ns = bus.createNamespace('t2');
  assert.deepEqual(liveChildRotation(bus, 't2'), { maxEntriesPerSegment: 2 });
  ns.publish('orders', 'a');
  ns.publish('orders', 'b');
  // The second append closes the segment (2 >= 2).
  assert.deepEqual(childLog(bus, 't2').readSince('t2/orders', 0), []);
  bus.close();
});

test('pre-enable records are not rewritten', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('hot', i);
  const before = readBack(dir, 'hot').map((r) => ({
    seq: r.seq,
    at: r.at,
    payload: r.payload,
  }));
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 });
  const after = readBack(dir, 'hot').map((r) => ({
    seq: r.seq,
    at: r.at,
    payload: r.payload,
  }));
  assert.deepEqual(after, before);
  bus.close();
});

test('restart restores the unconfigured state', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('hot', i);
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 });
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 5 });
  bus.close();

  const reopened = new EventBus({ durableLogDir: dir });
  assert.equal(liveRotation(reopened), undefined);
  assert.equal(audit(reopened).length, 0);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0, 1, 2],
  );
  reopened.close();
});

test('the enable audit is exact: from is all null, actor recorded', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.setDurableLogSegmentRotation(
    { maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 },
    'ops',
  );
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    from: { maxEntriesPerSegment: null, maxSegmentAgeMs: null },
    to: { maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 },
  });
  assert.equal(entries[0].actor, 'ops');
  bus.close();
});

test('enabling on a closed bus inherits the revision behaviour (no closed gate)', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.publish('hot', 'a');
  bus.close();
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 }, 'ops');
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 5 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    from: { maxEntriesPerSegment: null, maxSegmentAgeMs: null },
    to: { maxEntriesPerSegment: 5, maxSegmentAgeMs: null },
  });
});

test('the seed scan skips leading corrupt lines without recounting them', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  log.append({ seq: 1, topic: 't', at: 1000, payload: 'a' });
  log.append({ seq: 2, topic: 't', at: 1001, payload: 'b' });
  assert.equal(log.stats().corruptLines, 0);
  const liveFile = join(dir, 't.log');
  writeFileSync(liveFile, `not-json\n${readFileSync(liveFile, 'utf8')}`);
  log.installSegmentRotation({ maxEntriesPerSegment: 3 });
  // The corrupt head line was skipped silently: no recount.
  assert.equal(log.stats().corruptLines, 0);
  assert.deepEqual(log.currentSegmentRotation(), { maxEntriesPerSegment: 3 });
  // The seed counted the two stored records, so the next append closes it.
  log.append({ seq: 3, topic: 't', at: 1002, payload: 'c' });
  assert.deepEqual(log.readSince('t', 0), []);
  await waitForArchived(dir, 't');
  const entries = archiveIndex(dir, 't');
  assert.equal(entries.length, 1);
  assert.equal(entries[0].seqStart, 1);
  assert.equal(entries[0].seqEnd, 3);
});

test('a live file with no well-formed record leaves the segment start empty', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  log.append({ seq: 1, topic: 't', at: 1000, payload: 'a' });
  writeFileSync(join(dir, 't.log'), 'garbage-only\n');
  log.installSegmentRotation({ maxEntriesPerSegment: 2 });
  assert.deepEqual(log.currentSegmentRotation(), { maxEntriesPerSegment: 2 });
  // The seeded entry count (1) still counts, but no start was seeded —
  // the next append seeds the start from its own record and closes the
  // segment (2 >= 2), with the segment starting at the real first seq.
  log.append({ seq: 2, topic: 't', at: 1001, payload: 'b' });
  assert.deepEqual(log.readSince('t', 0), []);
  await waitForArchived(dir, 't');
  const entries = archiveIndex(dir, 't');
  assert.equal(entries.length, 1);
  assert.equal(entries[0].seqStart, 2);
  assert.equal(entries[0].seqEnd, 2);
});
