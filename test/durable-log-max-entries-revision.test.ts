/**
 * Bus-level durable-log per-topic entry-count limit revision (EB-322).
 *
 * `setDurableLogMaxEntriesPerTopic(maxEntriesPerTopic, actor?)` retunes
 * how many entries each topic retains before the log compacts away the
 * oldest — without restarting the bus. Only the bound moves: the
 * amortized 2x compact trigger, the keyed-compaction rule, and the
 * offset/dedup journal guards all keep reading the bound live, and the
 * records already persisted are untouched — tightening takes effect
 * from the next append-triggered compact, loosening simply allows more
 * capacity. The revision fans out to the root log and every namespace
 * child log, and a namespace registered later opens with the live bus
 * bound. A bus without `durableLogDir` refuses fail-closed; illegal
 * values fail closed with the live bound untouched; an identical
 * revision is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-max-entries-'));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_max_entries_revised' });

/** Reopens a log directory and reads back one topic's persisted records. */
const readBack = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0);

/**
 * The bus's live per-topic entry budget — read off the root log itself.
 * (A reopened log reports the construction-time bound: the revised bound
 * is runtime-only, like every sibling revision.)
 */
const liveBound = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog.currentMaxEntriesPerTopic();

/** The live per-topic entry budget of one namespace's child log. */
const liveChildBound = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!
    .currentMaxEntriesPerTopic();

const childDir = (dir: string, prefix: string) =>
  join(dir, 'namespaces', encodeURIComponent(prefix));

test('constructor still rejects illegal durableLogMaxEntriesPerTopic', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, '100', Infinity]) {
    assert.throws(
      () => new EventBus({ durableLogDir: freshDir(), durableLogMaxEntriesPerTopic: bad as never }),
      RangeError,
    );
  }
});

test('DurableTopicLog.setMaxEntriesPerTopic validates, returns the old bound, and leaves it untouched on illegal values', () => {
  const log = DurableTopicLog.open({ dir: freshDir(), maxEntriesPerTopic: 5 });
  assert.equal(log.currentMaxEntriesPerTopic(), 5);
  assert.equal(log.setMaxEntriesPerTopic(3), 5);
  assert.equal(log.currentMaxEntriesPerTopic(), 3);
  for (const bad of [0, -1, 1.5, Number.NaN, '3', Infinity]) {
    assert.throws(() => log.setMaxEntriesPerTopic(bad as never), RangeError);
  }
  // The failed calls changed nothing.
  assert.equal(log.currentMaxEntriesPerTopic(), 3);
});

test('setDurableLogMaxEntriesPerTopic requires durableLogDir and a valid actor', () => {
  const noLog = new EventBus();
  assert.throws(() => noLog.setDurableLogMaxEntriesPerTopic(5), RangeError);
  assert.throws(
    () => noLog.setDurableLogMaxEntriesPerTopic(5, ''),
    /actor must be a non-empty string/,
  );
  noLog.close();

  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => bus.setDurableLogMaxEntriesPerTopic(5, '' as never), RangeError);
  assert.throws(() => bus.setDurableLogMaxEntriesPerTopic(5, 42 as never), RangeError);
  // The failed calls changed nothing: no audit, default bound intact.
  assert.equal(audit(bus).length, 0);
  bus.close();
});

test('revision rejects illegal values and leaves the live bound and audit untouched', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogMaxEntriesPerTopic: 3 });
  for (const bad of [0, -1, 1.5, Number.NaN, '100', Infinity]) {
    assert.throws(() => bus.setDurableLogMaxEntriesPerTopic(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  assert.equal(liveBound(bus), 3);
  // A valid revision afterwards still works on the untouched bound.
  bus.setDurableLogMaxEntriesPerTopic(7, 'ops');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { maxEntriesPerTopic: 7, oldMaxEntriesPerTopic: 3 });
  assert.equal(audit(bus)[0].actor, 'ops');
  assert.equal(liveBound(bus), 7);
  bus.close();
});

test('an identical revision is a no-op with no audit', () => {
  const bus = new EventBus({ durableLogDir: freshDir(), durableLogMaxEntriesPerTopic: 5 });
  bus.setDurableLogMaxEntriesPerTopic(5);
  assert.equal(audit(bus).length, 0);

  const defaulted = new EventBus({ durableLogDir: freshDir() });
  // The default bound is 10000: re-affirming it is a no-op.
  defaulted.setDurableLogMaxEntriesPerTopic(10000);
  assert.equal(audit(defaulted).length, 0);
  bus.close();
  defaulted.close();
});

test('revision audits the old and new bound and moves the root log', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogMaxEntriesPerTopic(500);
  bus.setDurableLogMaxEntriesPerTopic(2000);
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, { maxEntriesPerTopic: 500, oldMaxEntriesPerTopic: 10000 });
  assert.deepEqual(entries[1].detail, { maxEntriesPerTopic: 2000, oldMaxEntriesPerTopic: 500 });
  assert.equal(liveBound(bus), 2000);
  bus.close();
});

test('revision fans out to namespace child logs', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 'one');
  bus.setDurableLogMaxEntriesPerTopic(7);
  assert.equal(liveBound(bus), 7);
  assert.equal(liveChildBound(bus, 't1'), 7);
  assert.deepEqual(audit(bus)[0].detail, { maxEntriesPerTopic: 7, oldMaxEntriesPerTopic: 10000 });
  bus.close();
});

test('a namespace registered after the revision opens with the live bound', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogMaxEntriesPerTopic(4);
  bus.createNamespace('t2');
  assert.equal(liveChildBound(bus, 't2'), 4);
  bus.close();
});

test('tightening takes effect from the next append-triggered compact; in-flight records untouched', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 10; i += 1) bus.publish('hot', i);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  );
  bus.setDurableLogMaxEntriesPerTopic(3);
  // No immediate compact: the ten persisted records are still there.
  assert.equal(readBack(dir, 'hot').length, 10);
  // The next append trips the 2x trigger (11 > 2*3) and compacts down
  // to the newest 3.
  bus.publish('hot', 10);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [8, 9, 10],
  );
  bus.close();
});

test('loosening allows more capacity from the next append', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogMaxEntriesPerTopic: 3 });
  for (let i = 1; i <= 11; i += 1) bus.publish('hot', i);
  // Two compactions under the tight bound leave only the newest 3.
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [9, 10, 11],
  );
  bus.setDurableLogMaxEntriesPerTopic(100);
  for (let i = 12; i <= 16; i += 1) bus.publish('hot', i);
  // No compact under the loosened bound: all eight records survive.
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [9, 10, 11, 12, 13, 14, 15, 16],
  );
  bus.close();
});

test('a namespace child log compacts under the revised bound', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const ns = bus.createNamespace('t1');
  for (let i = 0; i < 10; i += 1) ns.publish('orders', i);
  assert.equal(readBack(childDir(dir, 't1'), 't1/orders').length, 10);
  bus.setDurableLogMaxEntriesPerTopic(3);
  assert.equal(readBack(childDir(dir, 't1'), 't1/orders').length, 10);
  ns.publish('orders', 10);
  assert.deepEqual(
    readBack(childDir(dir, 't1'), 't1/orders').map((r) => r.payload),
    [8, 9, 10],
  );
  bus.close();
});
