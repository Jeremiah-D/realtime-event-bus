/**
 * Bus-level durable-log segment-rotation revision (EB-355).
 *
 * `setDurableLogSegmentRotation(patch, actor?)` retunes when a topic's
 * live segment closes and archives — without restarting the bus. A
 * partial revision: only the fields provided move, the rest keep their
 * live values (passing a field as `undefined` clears that trigger, as
 * long as at least one trigger remains). Only the triggers move: the
 * in-flight live segment keeps its original start time and counted
 * entries, and the new triggers govern from the next append — a
 * tightened entry budget rotates on that next append, the age budget
 * is judged on record timestamps (the bus clock). The revision fans
 * out to the root log and every namespace child log, and a namespace
 * registered later opens with the live bus triggers. A bus without
 * `durableLogDir`, or constructed without rotation, refuses
 * fail-closed (rotation cannot be enabled after the fact); illegal
 * values fail closed with the live triggers untouched; an identical
 * revision, or a patch with neither field, is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog, type SegmentRotationOptions } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-seg-rot-'));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_segment_rotation_revised' });

/** Reopens a log directory and reads back one topic's live records. */
const readBack = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0);

/** The bus's live rotation triggers — read off the root log itself. */
const liveRotation = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog.currentSegmentRotation();

/** The live rotation triggers of one namespace's child log. */
const liveChildRotation = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!
    .currentSegmentRotation();

test('constructor still rejects illegal segment rotation triggers', () => {
  for (const bad of [
    {},
    { maxEntriesPerSegment: 0 },
    { maxEntriesPerSegment: -1 },
    { maxEntriesPerSegment: 1.5 },
    { maxSegmentAgeMs: 0 },
    { maxSegmentAgeMs: -1 },
    { maxSegmentAgeMs: Number.NaN },
    { maxSegmentAgeMs: Infinity },
  ]) {
    assert.throws(
      () =>
        new EventBus({
          durableLogDir: freshDir(),
          durableLogSegmentRotation: bad as never,
        }),
      RangeError,
    );
  }
});

test('DurableTopicLog.setSegmentRotation validates, returns the old triggers, and leaves them untouched on illegal values', () => {
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 },
  });
  assert.deepEqual(log.currentSegmentRotation(), {
    maxEntriesPerSegment: 5,
    maxSegmentAgeMs: 1000,
  });
  assert.deepEqual(log.setSegmentRotation({ maxEntriesPerSegment: 3 }), {
    maxEntriesPerSegment: 5,
    maxSegmentAgeMs: 1000,
  });
  assert.deepEqual(log.currentSegmentRotation(), {
    maxEntriesPerSegment: 3,
    maxSegmentAgeMs: 1000,
  });
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
  ]) {
    assert.throws(() => log.setSegmentRotation(bad as never), RangeError);
  }
  // The failed calls changed nothing.
  assert.deepEqual(log.currentSegmentRotation(), {
    maxEntriesPerSegment: 3,
    maxSegmentAgeMs: 1000,
  });
});

test('DurableTopicLog.setSegmentRotation is a partial revision: the other trigger keeps its live value', () => {
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
  });
  log.setSegmentRotation({ maxSegmentAgeMs: 2000 });
  assert.deepEqual(log.currentSegmentRotation(), {
    maxEntriesPerSegment: 5,
    maxSegmentAgeMs: 2000,
  });
  // Clearing a trigger explicitly keeps the other one.
  log.setSegmentRotation({ maxEntriesPerSegment: undefined });
  assert.deepEqual(log.currentSegmentRotation(), { maxSegmentAgeMs: 2000 });
  // Clearing the last trigger is refused: at least one must remain.
  assert.throws(() => log.setSegmentRotation({ maxSegmentAgeMs: undefined }), RangeError);
  assert.deepEqual(log.currentSegmentRotation(), { maxSegmentAgeMs: 2000 });
});

test('DurableTopicLog.setSegmentRotation with neither field is a no-op', () => {
  const log = DurableTopicLog.open({
    dir: freshDir(),
    segmentRotation: { maxEntriesPerSegment: 5 },
  });
  assert.deepEqual(log.setSegmentRotation({}), { maxEntriesPerSegment: 5 });
  assert.deepEqual(log.currentSegmentRotation(), { maxEntriesPerSegment: 5 });
  const unrotated = DurableTopicLog.open({ dir: freshDir() });
  assert.equal(unrotated.currentSegmentRotation(), undefined);
  assert.equal(unrotated.setSegmentRotation({}), undefined);
});

test('a log constructed without rotation refuses to enable it after the fact', () => {
  const log = DurableTopicLog.open({ dir: freshDir() });
  assert.throws(() => log.setSegmentRotation({ maxEntriesPerSegment: 5 }), RangeError);
  assert.equal(log.currentSegmentRotation(), undefined);
});

test('setDurableLogSegmentRotation requires durableLogDir, rotation configured, and a valid actor', () => {
  const noLog = new EventBus();
  assert.throws(() => noLog.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 }), RangeError);
  assert.throws(
    () => noLog.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 }, ''),
    /actor must be a non-empty string/,
  );
  noLog.close();

  const noRotation = new EventBus({ durableLogDir: freshDir() });
  assert.throws(
    () => noRotation.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 }),
    /cannot be enabled after the fact/,
  );
  assert.equal(audit(noRotation).length, 0);
  assert.equal(liveRotation(noRotation), undefined);
  noRotation.close();

  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
  });
  assert.throws(
    () => bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 3 }, '' as never),
    RangeError,
  );
  assert.throws(
    () => bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 3 }, 42 as never),
    RangeError,
  );
  assert.equal(audit(bus).length, 0);
  bus.close();
});

test('revision rejects illegal values and leaves the live triggers and audit untouched', () => {
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 },
  });
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
  ]) {
    assert.throws(() => bus.setDurableLogSegmentRotation(bad as never), RangeError);
  }
  // Clearing the last trigger is refused: at least one must remain.
  const single = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
  });
  assert.throws(
    () => single.setDurableLogSegmentRotation({ maxEntriesPerSegment: undefined }),
    RangeError,
  );
  assert.deepEqual(liveRotation(single), { maxEntriesPerSegment: 5 });
  assert.equal(audit(single).length, 0);
  single.close();
  // The empty patch above is the no-op case, not a throw: re-check it
  // separately — it must not throw and must not audit.
  bus.setDurableLogSegmentRotation({});
  assert.equal(audit(bus).length, 0);
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 });
  // A valid revision afterwards still works on the untouched triggers.
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 7 }, 'ops');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    from: { maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 },
    to: { maxEntriesPerSegment: 7, maxSegmentAgeMs: 1000 },
  });
  assert.equal(audit(bus)[0].actor, 'ops');
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 7, maxSegmentAgeMs: 1000 });
  bus.close();
});

test('an identical revision and an empty patch are no-ops with no audit', () => {
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 },
  });
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 });
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 });
  bus.setDurableLogSegmentRotation({});
  assert.equal(audit(bus).length, 0);
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 5, maxSegmentAgeMs: 1000 });
  bus.close();
});

test('revision audits from/to with null for unset triggers and moves the root log', () => {
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 10 },
  });
  bus.setDurableLogSegmentRotation({ maxSegmentAgeMs: 5000 });
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 4 });
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, {
    from: { maxEntriesPerSegment: 10, maxSegmentAgeMs: null },
    to: { maxEntriesPerSegment: 10, maxSegmentAgeMs: 5000 },
  });
  assert.deepEqual(entries[1].detail, {
    from: { maxEntriesPerSegment: 10, maxSegmentAgeMs: 5000 },
    to: { maxEntriesPerSegment: 4, maxSegmentAgeMs: 5000 },
  });
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 4, maxSegmentAgeMs: 5000 });
  bus.close();
});

test('revision fans out to namespace child logs', () => {
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 10 },
  });
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 'one');
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 7, maxSegmentAgeMs: 9000 });
  assert.deepEqual(liveRotation(bus), { maxEntriesPerSegment: 7, maxSegmentAgeMs: 9000 });
  assert.deepEqual(liveChildRotation(bus, 't1'), {
    maxEntriesPerSegment: 7,
    maxSegmentAgeMs: 9000,
  });
  bus.close();
});

test('a namespace registered after the revision opens with the live triggers', () => {
  const bus = new EventBus({
    durableLogDir: freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 10 },
  });
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 4 });
  bus.createNamespace('t2');
  assert.deepEqual(liveChildRotation(bus, 't2'), { maxEntriesPerSegment: 4 });
  bus.close();
});

test('tightening maxEntriesPerSegment rotates on the next append; in-flight records untouched until then', () => {
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 10 },
  });
  for (let i = 0; i < 3; i += 1) bus.publish('hot', i);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0, 1, 2],
  );
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 3 });
  // No immediate rotation: the three persisted records are still live.
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0, 1, 2],
  );
  // The next append trips the tightened budget (4 >= 3) and closes the
  // segment — the live file is empty again until the following append.
  bus.publish('hot', 3);
  assert.deepEqual(readBack(dir, 'hot').map((r) => r.payload), []);
  bus.publish('hot', 4);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [4],
  );
  bus.close();
});

test('maxSegmentAgeMs is judged on record timestamps: a revised age rotates the aged segment', () => {
  const dir = freshDir();
  const clock = { nowMs: 1000, now: () => clock.nowMs };
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxSegmentAgeMs: 5000 },
    now: clock.now,
  });
  bus.publish('hot', 'a'); // segment starts at t=1000.
  bus.setDurableLogSegmentRotation({ maxSegmentAgeMs: 2000 });
  // At t=2500 the segment is only 1500ms old — no rotation yet.
  clock.nowMs = 2500;
  bus.publish('hot', 'b');
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    ['a', 'b'],
  );
  // At t=4000 the segment is 3000ms old — past the revised 2000ms age.
  clock.nowMs = 4000;
  bus.publish('hot', 'c');
  assert.deepEqual(readBack(dir, 'hot').map((r) => r.payload), []);
  bus.publish('hot', 'd');
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    ['d'],
  );
  bus.close();
});

test('loosening maxEntriesPerSegment lets the live segment grow past the old budget', () => {
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 3 },
  });
  for (let i = 0; i < 3; i += 1) bus.publish('hot', i);
  // The 3rd append closed the segment (3 >= 3); the live file is empty.
  assert.deepEqual(readBack(dir, 'hot').map((r) => r.payload), []);
  bus.publish('hot', 3);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [3],
  );
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 100 });
  for (let i = 4; i <= 8; i += 1) bus.publish('hot', i);
  // No rotation under the loosened budget: all five records stay live.
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [3, 4, 5, 6, 7, 8],
  );
  bus.close();
});
