/**
 * Per-topic durable-log max-age live revision (EB-388).
 *
 * `setTopicDurableLogMaxAgeMs(topic, maxAgeMs, actor?)` installs one
 * concrete topic's own time-based retention bound — records older than
 * `now - maxAgeMs` (bus clock) are evicted at the topic's next compact,
 * dual-bounded with the count limit like the global bound. `undefined`
 * clears the override and the topic falls back to the live bus-global
 * value. An overridden topic no longer follows later
 * `setDurableLogMaxAgeMs` moves (the override pins it); uncovered
 * topics keep following the live global value. Tightening never forces
 * an immediate compact; loosening governs from the next compact. Seq-0
 * schedule records never age out, under an override exactly as under
 * the global bound. The revision fans out to the root log and every
 * namespace child log, and a namespace registered later opens with the
 * live bus mapping. A bus without `durableLogDir` refuses fail-closed;
 * illegal values fail closed with live state untouched; an identical
 * revision is a no-op with no audit. The revision is live-only — a
 * restart clears every per-topic override.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** A manually-advanced clock handed to the bus / log. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-topic-max-age-'));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'topic_durable_log_max_age_revised' });

/** Reopens a log directory and reads back one topic's persisted records. */
const readBack = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0);

/** The live per-topic override on the root log itself. */
const liveRootOverride = (bus: EventBus, topic: string) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog.topicMaxAge(topic);

/** The live per-topic override on one namespace's child log. */
const liveChildOverride = (bus: EventBus, prefix: string, topic: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!
    .topicMaxAge(topic);

const childDir = (dir: string, prefix: string) =>
  join(dir, 'namespaces', encodeURIComponent(prefix));

test('DurableTopicLog.setTopicMaxAgeMs validates, returns the previous override, and leaves it untouched on illegal values', () => {
  const log = DurableTopicLog.open({ dir: freshDir(), maxAgeMs: 1000 });
  assert.equal(log.topicMaxAge('t'), undefined);
  assert.equal(log.setTopicMaxAgeMs('t', 100), undefined);
  assert.equal(log.topicMaxAge('t'), 100);
  assert.equal(log.setTopicMaxAgeMs('t', 200), 100);
  assert.equal(log.setTopicMaxAgeMs('t', undefined), 200);
  assert.equal(log.topicMaxAge('t'), undefined);
  for (const bad of [0, -5, Number.NaN, Number.POSITIVE_INFINITY, '100' as never]) {
    assert.throws(() => log.setTopicMaxAgeMs('t', bad), RangeError);
  }
  // The failed calls changed nothing.
  assert.equal(log.topicMaxAge('t'), undefined);
});

test('setTopicDurableLogMaxAgeMs requires durableLogDir and a valid actor', () => {
  const noLog = new EventBus();
  assert.throws(() => noLog.setTopicDurableLogMaxAgeMs('t', 1000), RangeError);
  assert.throws(
    () => noLog.setTopicDurableLogMaxAgeMs('t', 1000, ''),
    /actor must be a non-empty string/,
  );
  // The getters stay inert without a log: no override installed.
  assert.equal(noLog.getTopicDurableLogMaxAgeMs('t'), undefined);
  assert.deepEqual(noLog.listTopicDurableLogMaxAgeMs(), []);
  noLog.close();

  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => bus.setTopicDurableLogMaxAgeMs('t', 1000, '' as never), RangeError);
  assert.throws(() => bus.setTopicDurableLogMaxAgeMs('t', 1000, 42 as never), RangeError);
  assert.equal(audit(bus).length, 0);
  bus.close();
});

test('revision rejects illegal topics and values and leaves live state and audit untouched', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogMaxAgeMs: 10_000 });
  for (const badTopic of ['', 42, null]) {
    assert.throws(
      () => bus.setTopicDurableLogMaxAgeMs(badTopic as never, 1000),
      RangeError,
    );
  }
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '1000' as never]) {
    assert.throws(() => bus.setTopicDurableLogMaxAgeMs('audit', bad), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getTopicDurableLogMaxAgeMs('audit'), undefined);
  assert.deepEqual(bus.listTopicDurableLogMaxAgeMs(), []);
  assert.equal(liveRootOverride(bus, 'audit'), undefined);
  // A valid revision afterwards still works on the untouched state.
  bus.setTopicDurableLogMaxAgeMs('audit', 1000, 'ops');
  assert.equal(audit(bus).length, 1);
  assert.equal(bus.getTopicDurableLogMaxAgeMs('audit'), 1000);
  assert.equal(audit(bus)[0].actor, 'ops');
  bus.close();
});

test('tightening affects only that topic; the uncovered topic keeps the global bound', () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 10_000,
    now: clock.now,
  });
  for (let i = 0; i < 8; i += 1) {
    bus.publish('audit', `old-${i}`);
    bus.publish('telemetry', `old-${i}`);
  }
  bus.setTopicDurableLogMaxAgeMs('audit', 1000);
  clock.nowMs = 5_000;
  for (let i = 0; i < 3; i += 1) {
    bus.publish('audit', `new-${i}`);
    bus.publish('telemetry', `new-${i}`);
  }
  // The tightened topic lost its 8 old records at its compact (age
  // 4000 > 1000); the uncovered topic kept them under the global bound
  // (age 4000 < 10000) and only count-trimmed to the newest 5.
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 8);
  assert.deepEqual(
    readBack(dir, 'audit').map((r) => r.payload),
    ['new-0', 'new-1', 'new-2'],
  );
  assert.deepEqual(
    readBack(dir, 'telemetry').map((r) => r.payload),
    ['old-6', 'old-7', 'new-0', 'new-1', 'new-2'],
  );
  bus.close();
});

test('loosening affects only that topic; the uncovered topic keeps the tight global bound', () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 100,
    now: clock.now,
  });
  for (let i = 0; i < 8; i += 1) {
    bus.publish('audit', `old-${i}`);
    bus.publish('telemetry', `old-${i}`);
  }
  // The audit topic is loosened far past the tight global bound.
  bus.setTopicDurableLogMaxAgeMs('audit', 1000);
  clock.nowMs = 1_500;
  for (let i = 0; i < 3; i += 1) {
    bus.publish('audit', `new-${i}`);
    bus.publish('telemetry', `new-${i}`);
  }
  // Age 500: inside the loosened bound, outside the tight global one.
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 8);
  assert.deepEqual(
    readBack(dir, 'audit').map((r) => r.payload),
    ['old-6', 'old-7', 'new-0', 'new-1', 'new-2'],
  );
  assert.deepEqual(
    readBack(dir, 'telemetry').map((r) => r.payload),
    ['new-0', 'new-1', 'new-2'],
  );
  bus.close();
});

test('loosening takes effect from the next compact', () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 1000,
    now: clock.now,
  });
  for (let i = 0; i < 8; i += 1) bus.publish('audit', `old-${i}`);
  bus.setTopicDurableLogMaxAgeMs('audit', 100);
  clock.nowMs = 1_500;
  for (let i = 0; i < 3; i += 1) bus.publish('audit', `new-${i}`);
  // The tight bound evicted the 8 old records at the first compact.
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 8);
  assert.deepEqual(
    readBack(dir, 'audit').map((r) => r.payload),
    ['new-0', 'new-1', 'new-2'],
  );
  // Loosen far past the records' age, then age them past the old bound.
  bus.setTopicDurableLogMaxAgeMs('audit', 100_000);
  for (let i = 0; i < 8; i += 1) bus.publish('audit', `mid-${i}`);
  clock.nowMs = 5_000;
  for (let i = 0; i < 6; i += 1) bus.publish('audit', `late-${i}`);
  // Two more compactions ran (the count trim proves it: only 5
  // records survive) and neither evicted by age — the mid records aged
  // 3500, past the old bound of 100, inside the loosened 100_000.
  assert.deepEqual(
    readBack(dir, 'audit').map((r) => r.payload),
    ['late-1', 'late-2', 'late-3', 'late-4', 'late-5'],
  );
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 8);
  bus.close();
});

test('undefined clears the override and the topic falls back to the live global value', () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 10_000,
    now: clock.now,
  });
  bus.setTopicDurableLogMaxAgeMs('audit', 1000);
  // Move the global bound after installing the override, then clear:
  // the topic must follow the *live* global value, not the one in
  // force when the override was installed.
  bus.setDurableLogMaxAgeMs(100);
  bus.setTopicDurableLogMaxAgeMs('audit', undefined, 'ops');
  assert.equal(bus.getTopicDurableLogMaxAgeMs('audit'), undefined);
  assert.deepEqual(bus.listTopicDurableLogMaxAgeMs(), []);
  assert.equal(liveRootOverride(bus, 'audit'), undefined);
  assert.deepEqual(audit(bus)[1].detail, {
    topic: 'audit',
    maxAgeMs: null,
    oldMaxAgeMs: 1000,
  });
  for (let i = 0; i < 8; i += 1) bus.publish('audit', `old-${i}`);
  clock.nowMs = 1_500;
  for (let i = 0; i < 3; i += 1) bus.publish('audit', `new-${i}`);
  // Age 500 evicts under the live global bound of 100.
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 8);
  assert.deepEqual(
    readBack(dir, 'audit').map((r) => r.payload),
    ['new-0', 'new-1', 'new-2'],
  );
  bus.close();
});

test('a covered topic ignores later global revisions while an uncovered topic follows them', () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 10_000,
    now: clock.now,
  });
  bus.setTopicDurableLogMaxAgeMs('audit', 1000);
  bus.setDurableLogMaxAgeMs(100);
  // The override pins the topic: the global move left it at 1000.
  assert.equal(bus.getTopicDurableLogMaxAgeMs('audit'), 1000);
  assert.equal(liveRootOverride(bus, 'audit'), 1000);
  for (let i = 0; i < 8; i += 1) {
    bus.publish('audit', `old-${i}`);
    bus.publish('telemetry', `old-${i}`);
  }
  clock.nowMs = 1_500;
  for (let i = 0; i < 3; i += 1) {
    bus.publish('audit', `new-${i}`);
    bus.publish('telemetry', `new-${i}`);
  }
  // Age 500: inside the pinned 1000, outside the moved global 100.
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 8);
  assert.deepEqual(
    readBack(dir, 'audit').map((r) => r.payload),
    ['old-6', 'old-7', 'new-0', 'new-1', 'new-2'],
  );
  assert.deepEqual(
    readBack(dir, 'telemetry').map((r) => r.payload),
    ['new-0', 'new-1', 'new-2'],
  );
  bus.close();
});

test('tightening does not force an immediate compact', () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 10_000,
    now: clock.now,
  });
  for (let i = 0; i < 8; i += 1) bus.publish('audit', `old-${i}`);
  clock.nowMs = 5_000;
  bus.setTopicDurableLogMaxAgeMs('audit', 1000);
  // No immediate compact: the 8 stale records are still on disk.
  assert.equal(readBack(dir, 'audit').length, 8);
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 0);
  // The next append-triggered compact evicts them under the new bound.
  for (let i = 0; i < 3; i += 1) bus.publish('audit', `new-${i}`);
  assert.deepEqual(
    readBack(dir, 'audit').map((r) => r.payload),
    ['new-0', 'new-1', 'new-2'],
  );
  assert.equal(bus.getStats().durableLog?.retentionEvicted, 8);
  bus.close();
});

test('a revision identical to the current override is a no-op with no audit', () => {
  const bus = new EventBus({ durableLogDir: freshDir(), durableLogMaxAgeMs: 10_000 });
  // Installing the live global value still counts: it pins the topic
  // against later global moves.
  bus.setTopicDurableLogMaxAgeMs('audit', 10_000);
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    topic: 'audit',
    maxAgeMs: 10_000,
    oldMaxAgeMs: 10_000,
  });
  // Re-affirming the installed override is a no-op.
  bus.setTopicDurableLogMaxAgeMs('audit', 10_000);
  assert.equal(audit(bus).length, 1);
  bus.setTopicDurableLogMaxAgeMs('audit', undefined);
  assert.equal(audit(bus).length, 2);
  // Clearing a topic that holds no override is a no-op too.
  bus.setTopicDurableLogMaxAgeMs('audit', undefined);
  assert.equal(audit(bus).length, 2);
  bus.close();
});

test('revision audits previous and current, with the first previous equal to the live global value', () => {
  const bus = new EventBus({ durableLogDir: freshDir(), durableLogMaxAgeMs: 10_000 });
  bus.setTopicDurableLogMaxAgeMs('audit', 1000, 'ops');
  bus.setTopicDurableLogMaxAgeMs('audit', 2000, 'ops');
  bus.setTopicDurableLogMaxAgeMs('audit', undefined, 'ops');
  const entries = audit(bus);
  assert.equal(entries.length, 3);
  assert.deepEqual(entries[0].detail, { topic: 'audit', maxAgeMs: 1000, oldMaxAgeMs: 10_000 });
  assert.equal(entries[0].actor, 'ops');
  assert.deepEqual(entries[1].detail, { topic: 'audit', maxAgeMs: 2000, oldMaxAgeMs: 1000 });
  assert.deepEqual(entries[2].detail, { topic: 'audit', maxAgeMs: null, oldMaxAgeMs: 2000 });

  // Without a global bound the first previous is null (unset).
  const unbound = new EventBus({ durableLogDir: freshDir() });
  unbound.setTopicDurableLogMaxAgeMs('audit', 500);
  assert.deepEqual(audit(unbound)[0].detail, {
    topic: 'audit',
    maxAgeMs: 500,
    oldMaxAgeMs: null,
  });
  bus.close();
  unbound.close();
});

test('seq-0 schedule records are never age-evicted under a per-topic override', () => {
  const clock = controllableClock(1_000);
  // Global 1000, override 100: old records at 4600 are inside the
  // global window but outside the override — only the override may
  // evict them, and the seq-0 record survives either way.
  const log = DurableTopicLog.open({
    dir: freshDir(),
    maxEntriesPerTopic: 10,
    maxAgeMs: 1000,
    now: clock.now,
  });
  log.setTopicMaxAgeMs('t', 100);
  log.append({ seq: 0, topic: 't', at: 100, delayId: 'delayed-1', deliverAt: 50_000, payload: 'p' });
  for (let i = 1; i <= 11; i++) {
    log.append({ seq: i, topic: 't', at: 4_600, payload: `old-${i}` });
  }
  clock.nowMs = 5_000;
  for (let i = 12; i <= 20; i++) {
    log.append({ seq: i, topic: 't', at: 5_000, payload: `new-${i}` });
  }
  // 21 appends cross the 2x trigger (20): the compact evicted the 11
  // stale messages under the override and kept the ancient seq-0
  // schedule record plus the 9 fresh messages — 11 evicted + 10 kept
  // accounts for all 21 appends, so the seq-0 record survived (had it
  // been evicted, retentionEvicted would read 12 and entries 9).
  assert.equal(log.stats().retentionEvicted, 11);
  assert.equal(log.stats().entries, 10);
  // readSince replays message records (seq >= 1) only.
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    Array.from({ length: 9 }, (_, k) => `new-${k + 12}`),
  );
});

test('revision fans out to namespace child logs; a late namespace reads the live override', () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 10_000,
    now: clock.now,
  });
  const ns1 = bus.createNamespace('t1');
  bus.setTopicDurableLogMaxAgeMs('t1/orders', 1000);
  assert.equal(liveChildOverride(bus, 't1', 't1/orders'), 1000);
  assert.equal(bus.getTopicDurableLogMaxAgeMs('t1/orders'), 1000);
  for (let i = 0; i < 8; i += 1) ns1.publish('orders', `old-${i}`);
  clock.nowMs = 5_000;
  for (let i = 0; i < 3; i += 1) ns1.publish('orders', `new-${i}`);
  assert.deepEqual(
    readBack(childDir(dir, 't1'), 't1/orders').map((r) => r.payload),
    ['new-0', 'new-1', 'new-2'],
  );

  // A namespace registered after the override opens with the live map.
  bus.setTopicDurableLogMaxAgeMs('t2/orders', 1000);
  const ns2 = bus.createNamespace('t2');
  assert.equal(liveChildOverride(bus, 't2', 't2/orders'), 1000);
  for (let i = 0; i < 8; i += 1) ns2.publish('orders', `old2-${i}`);
  clock.nowMs = 9_000;
  for (let i = 0; i < 3; i += 1) ns2.publish('orders', `new2-${i}`);
  assert.deepEqual(
    readBack(childDir(dir, 't2'), 't2/orders').map((r) => r.payload),
    ['new2-0', 'new2-1', 'new2-2'],
  );
  bus.close();
});

test('getter and list mirror live state', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.setTopicDurableLogMaxAgeMs('a', 1000);
  bus.setTopicDurableLogMaxAgeMs('b', 2000);
  assert.equal(bus.getTopicDurableLogMaxAgeMs('a'), 1000);
  assert.equal(bus.getTopicDurableLogMaxAgeMs('b'), 2000);
  assert.equal(bus.getTopicDurableLogMaxAgeMs('c'), undefined);
  assert.deepEqual(bus.listTopicDurableLogMaxAgeMs(), [
    { topic: 'a', maxAgeMs: 1000 },
    { topic: 'b', maxAgeMs: 2000 },
  ]);
  // The returned rows are fresh objects: mutating them moves nothing.
  const rows = bus.listTopicDurableLogMaxAgeMs();
  rows[0].maxAgeMs = 1;
  assert.equal(bus.getTopicDurableLogMaxAgeMs('a'), 1000);
  bus.setTopicDurableLogMaxAgeMs('a', undefined);
  assert.deepEqual(bus.listTopicDurableLogMaxAgeMs(), [{ topic: 'b', maxAgeMs: 2000 }]);
  bus.setTopicDurableLogMaxAgeMs('b', undefined);
  assert.deepEqual(bus.listTopicDurableLogMaxAgeMs(), []);
  bus.close();
});

test('restart clears per-topic overrides', () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const first = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 10_000,
    now: clock.now,
  });
  first.setTopicDurableLogMaxAgeMs('audit', 1000);
  for (let i = 0; i < 8; i += 1) first.publish('audit', `old-${i}`);
  first.close();

  clock.nowMs = 5_000;
  const second = new EventBus({
    durableLogDir: dir,
    durableLogMaxEntriesPerTopic: 5,
    durableLogMaxAgeMs: 10_000,
    now: clock.now,
  });
  // The override did not survive the restart.
  assert.equal(second.getTopicDurableLogMaxAgeMs('audit'), undefined);
  assert.deepEqual(second.listTopicDurableLogMaxAgeMs(), []);
  assert.equal(liveRootOverride(second, 'audit'), undefined);
  for (let i = 0; i < 3; i += 1) second.publish('audit', `new-${i}`);
  // The compact now runs under the live global bound (age 4000 <
  // 10000): nothing age-evicts, only the count trim applies.
  assert.equal(second.getStats().durableLog?.retentionEvicted, 0);
  assert.deepEqual(
    readBack(dir, 'audit').map((r) => r.payload),
    ['old-6', 'old-7', 'new-0', 'new-1', 'new-2'],
  );
  second.close();
});
