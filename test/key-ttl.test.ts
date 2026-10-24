import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus/log. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-keyttl-'));

const linesOf = (dir: string, topic: string) =>
  readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0)
    .map((l) => JSON.parse(l) as Record<string, unknown>);

// ---------------------------------------------------------------------------
// Log level: per-key TTL eviction, tombstones, readSince filtering.
// ---------------------------------------------------------------------------

test('setKeyTtl validates and reports the previous value', () => {
  const log = DurableTopicLog.open({ dir: freshDir(), keyCompaction: true });
  assert.equal(log.setKeyTtl('t', 100), undefined);
  assert.equal(log.setKeyTtl('t', 200), 100);
  assert.equal(log.keyTtl('t'), 200);
  assert.equal(log.setKeyTtl('t', undefined), 200);
  assert.equal(log.keyTtl('t'), undefined);
  for (const bad of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => log.setKeyTtl('t', bad as number), RangeError);
  }
});

test('compact evicts cold keys, writes tombstones, and counts them', () => {
  const dir = freshDir();
  const clock = controllableClock(1000);
  const log = DurableTopicLog.open({
    dir,
    keyCompaction: true,
    maxEntriesPerTopic: 2,
    now: clock.now,
  });
  log.append({ seq: 1, topic: 't', at: 1000, key: 'A', keySeq: 1, payload: 'a1' });
  log.append({ seq: 2, topic: 't', at: 1100, key: 'A', keySeq: 2, payload: 'a2' });
  log.append({ seq: 3, topic: 't', at: 1000, key: 'B', keySeq: 1, payload: 'b1' });
  log.setKeyTtl('t', 500);
  clock.nowMs = 2000;
  // Fillers push the live count past 2 * 2, triggering a compact:
  // cutoff is 2000 - 500 = 1500, so both keys (latest at 1100 / 1000)
  // are cold and go.
  log.append({ seq: 4, topic: 't', at: 2000, payload: 'f1' });
  log.append({ seq: 5, topic: 't', at: 2000, payload: 'f2' });
  assert.equal(log.stats().keyTtlEvicted, 2);
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    ['f1', 'f2'],
  );
  const tombstones = linesOf(dir, 't').filter((l) => l['keyTtlEvicted'] === true);
  assert.equal(tombstones.length, 2);
  assert.deepEqual(
    tombstones.map((l) => [l['key'], l['keySeq'], l['seq']]).sort(),
    [
      ['A', 2, 0],
      ['B', 1, 0],
    ],
  );
  // The seq baseline survives: in-memory and across a reopen.
  assert.equal(log.recoveredKeySeqs().get('A'), 2);
  const reopened = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 2 });
  assert.equal(reopened.recoveredKeySeqs().get('A'), 2);
  assert.equal(reopened.recoveredKeySeqs().get('B'), 1);
  assert.equal(reopened.stats().keyTtlEvicted, 0); // counters are runtime-only
});

test('readSince skips expired keys before any compact rewrites the file', () => {
  const clock = controllableClock(1000);
  const log = DurableTopicLog.open({
    dir: freshDir(),
    keyCompaction: true,
    maxEntriesPerTopic: 1000,
    now: clock.now,
  });
  log.append({ seq: 1, topic: 't', at: 1000, key: 'A', keySeq: 1, payload: 'a1' });
  log.append({ seq: 2, topic: 't', at: 2000, key: 'B', keySeq: 1, payload: 'b1' });
  log.setKeyTtl('t', 500);
  clock.nowMs = 2000;
  // Cutoff is 1500: A (at 1000) is expired and skipped in replay even
  // though its record is still on disk; B (at 2000) replays.
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    ['b1'],
  );
  // A record exactly on the cutoff is kept.
  log.append({ seq: 3, topic: 't', at: 1500, key: 'C', keySeq: 1, payload: 'c1' });
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    ['b1', 'c1'],
  );
});

test('key TTL is inert without keyed compaction', () => {
  const clock = controllableClock(1000);
  const log = DurableTopicLog.open({
    dir: freshDir(),
    keyCompaction: false,
    maxEntriesPerTopic: 1000,
    now: clock.now,
  });
  log.append({ seq: 1, topic: 't', at: 1000, key: 'A', keySeq: 1, payload: 'a1' });
  log.setKeyTtl('t', 500);
  clock.nowMs = 2000;
  // Without latest-per-key semantics there is no "cold key" to skip:
  // replay still returns the expired value, and no eviction is counted.
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    ['a1'],
  );
  assert.equal(log.stats().keyTtlEvicted, 0);
  // Even a compact that runs for other reasons (count trigger) evicts
  // nothing for TTL: the counter stays 0.
  log.append({ seq: 2, topic: 't', at: 2000, payload: 'f2' });
  log.append({ seq: 3, topic: 't', at: 2000, payload: 'f3' });
  assert.equal(log.stats().keyTtlEvicted, 0);
});

test('pending schedule records for an evicted key survive compaction', () => {
  const dir = freshDir();
  const clock = controllableClock(1000);
  const log = DurableTopicLog.open({
    dir,
    keyCompaction: true,
    maxEntriesPerTopic: 2,
    now: clock.now,
  });
  // A delayed schedule already consumed keySeq 5 for key A — a timer
  // intent, not A's value.
  log.append({
    seq: 0,
    topic: 't',
    at: 1000,
    delayId: 'd1',
    deliverAt: 5000,
    key: 'A',
    keySeq: 5,
    payload: 'later',
  });
  log.append({ seq: 1, topic: 't', at: 1000, key: 'A', keySeq: 1, payload: 'a1' });
  log.setKeyTtl('t', 500);
  clock.nowMs = 2000;
  log.append({ seq: 2, topic: 't', at: 2000, payload: 'f1' });
  log.append({ seq: 3, topic: 't', at: 2000, payload: 'f2' });
  log.append({ seq: 4, topic: 't', at: 2000, payload: 'f3' });
  // The compact evicted A's value but kept the pending schedule.
  const lines = linesOf(dir, 't');
  assert.ok(lines.some((l) => l['delayId'] === 'd1'));
  assert.ok(!lines.some((l) => l['payload'] === 'a1'));
  assert.equal(log.stats().keyTtlEvicted, 1);
});

test('re-eviction replaces the stale tombstone instead of stacking them', () => {
  const dir = freshDir();
  const clock = controllableClock(1000);
  const log = DurableTopicLog.open({
    dir,
    keyCompaction: true,
    maxEntriesPerTopic: 2,
    now: clock.now,
  });
  log.append({ seq: 1, topic: 't', at: 1000, key: 'A', keySeq: 1, payload: 'a1' });
  log.append({ seq: 2, topic: 't', at: 1000, key: 'A', keySeq: 2, payload: 'a2' });
  log.setKeyTtl('t', 500);
  clock.nowMs = 2000;
  // 5 entries pass the 2 * maxEntries trigger, so this compact runs.
  log.append({ seq: 3, topic: 't', at: 2000, payload: 'f1' });
  log.append({ seq: 4, topic: 't', at: 2000, payload: 'f2' });
  log.append({ seq: 5, topic: 't', at: 2000, payload: 'f3' });
  assert.equal(log.stats().keyTtlEvicted, 1);
  // Republish the key, then let it go cold again.
  log.append({ seq: 6, topic: 't', at: 2000, key: 'A', keySeq: 3, payload: 'a3' });
  clock.nowMs = 3000;
  log.append({ seq: 7, topic: 't', at: 3000, payload: 'g1' });
  log.append({ seq: 8, topic: 't', at: 3000, payload: 'g2' });
  log.append({ seq: 9, topic: 't', at: 3000, payload: 'g3' });
  assert.equal(log.stats().keyTtlEvicted, 2);
  const tombstones = linesOf(dir, 't').filter((l) => l['keyTtlEvicted'] === true);
  assert.equal(tombstones.length, 1);
  assert.equal(tombstones[0]['key'], 'A');
  assert.equal(tombstones[0]['keySeq'], 3);
  assert.equal(log.recoveredKeySeqs().get('A'), 3);
});

test('corrupt key-TTL lines are rejected, never resurrected', () => {
  const dir = freshDir();
  const file = join(dir, `${encodeURIComponent('t')}.log`);
  const lines = [
    JSON.stringify({ v: 1, seq: 1, topic: 't', at: 100, payload: 'ok' }),
    // keyTtlEvicted on a real message: corruption.
    JSON.stringify({ v: 1, seq: 5, topic: 't', at: 100, payload: 'x', key: 'K', keySeq: 1, keyTtlEvicted: true }),
    // Bare seq-0 record belonging to no family: corruption.
    JSON.stringify({ v: 1, seq: 0, topic: 't', at: 100, payload: null }),
  ];
  writeFileSync(file, `${lines.join('\n')}\n`);
  const log = DurableTopicLog.open({ dir, keyCompaction: true });
  assert.equal(log.stats().corruptLines, 2);
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    ['ok'],
  );
});

// ---------------------------------------------------------------------------
// Bus level: setTopicKeyTtl pattern rules, replay, keySeq continuation.
// ---------------------------------------------------------------------------

test('setTopicKeyTtl validates preconditions and arguments', () => {
  const noLog = new EventBus();
  assert.throws(() => noLog.setTopicKeyTtl('t', 100), /durableLogDir/);
  const noCompaction = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => noCompaction.setTopicKeyTtl('t', 100), /durableLogKeyCompaction/);
  const bus = new EventBus({ durableLogDir: freshDir(), durableLogKeyCompaction: true });
  assert.throws(() => bus.setTopicKeyTtl('', 100), RangeError);
  assert.throws(() => bus.setTopicKeyTtl('t', 0), RangeError);
  assert.throws(() => bus.setTopicKeyTtl('t', -1), RangeError);
  assert.throws(() => bus.setTopicKeyTtl('t', Number.NaN), RangeError);
  assert.equal(bus.clearTopicKeyTtl('missing'), false);
});

test('pattern resolution: exact wins over patterns, earliest pattern wins, clear falls back', async () => {
  const dir = freshDir();
  const clock = controllableClock(1000);
  const bus = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    durableLogKeyCompaction: true,
    durableLogMaxEntriesPerTopic: 2,
  });
  bus.setTopicKeyTtl('state.*', 100);
  bus.setTopicKeyTtl('state.hot', 100000);
  bus.publish('state.hot', 'h1', { key: 'H' });
  bus.publish('state.cold', 'c1', { key: 'C' });
  await flush();
  // Trigger a compact on state.hot while its exact rule still shields
  // it: 5 entries pass the 2 * maxEntries trigger, and the compact
  // keeps H (TTL 100000) while capping keyless fillers at 2.
  bus.publish('state.hot', 'hf1');
  bus.publish('state.hot', 'hf2');
  bus.publish('state.hot', 'hf3');
  bus.publish('state.hot', 'hf4');
  await flush();
  assert.equal(bus.getStats().durableLog?.keyTtlEvicted, 0);
  assert.ok(linesOf(dir, 'state.hot').some((l) => l['payload'] === 'h1'));
  // Cold topic: advance past its pattern TTL, then compact it away.
  clock.nowMs = 2000;
  bus.publish('state.cold', 'cf1');
  bus.publish('state.cold', 'cf2');
  bus.publish('state.cold', 'cf3');
  bus.publish('state.cold', 'cf4');
  await flush();
  assert.equal(bus.getStats().durableLog?.keyTtlEvicted, 1);
  assert.ok(!linesOf(dir, 'state.cold').some((l) => l['payload'] === 'c1'));
  // Clearing the exact rule drops state.hot back to the pattern TTL —
  // its key is already cold, so the next compact evicts it.
  assert.equal(bus.clearTopicKeyTtl('state.hot'), true);
  assert.equal(bus.clearTopicKeyTtl('state.hot'), false);
  bus.publish('state.hot', 'hf5');
  bus.publish('state.hot', 'hf6');
  await flush();
  assert.equal(bus.getStats().durableLog?.keyTtlEvicted, 2);
});

test('resumeFromSeq never resurrects an evicted key; keySeq never renumbers', async () => {
  const dir = freshDir();
  const clock = controllableClock(1000);
  const bus = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    durableLogKeyCompaction: true,
    durableLogMaxEntriesPerTopic: 2,
  });
  bus.setTopicKeyTtl('state', 100);
  bus.publish('state', 'a1', { key: 'A' });
  bus.publish('state', 'a2', { key: 'A' });
  await flush();
  clock.nowMs = 2000;
  bus.publish('state', 'f1');
  bus.publish('state', 'f2');
  bus.publish('state', 'f3');
  await flush();
  assert.equal(bus.getStats().durableLog?.keyTtlEvicted, 1);
  // A resubscribing consumer replays the fillers, never the evicted values.
  // (The compact that evicted A also capped keyless fillers at
  // maxEntriesPerTopic = 2, so f1 is gone too — that is count
  // compaction, not key TTL.)
  const replayed: unknown[] = [];
  bus.subscribe('state', (m) => replayed.push(m.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(replayed, ['f2', 'f3']);
  // Republishing the evicted key continues its keySeq (1, 2 -> 3).
  bus.publish('state', 'a3', { key: 'A' });
  await flush();
  const keySeqsOf = () =>
    linesOf(dir, 'state')
      .filter((l) => l['key'] === 'A' && typeof l['seq'] === 'number' && (l['seq'] as number) >= 1)
      .map((l) => l['keySeq']);
  assert.deepEqual(keySeqsOf(), [3]);
  // Restart: the tombstone reseeds the baseline, so the next publish is
  // keySeq 4, never a renumbered 1.
  const bus2 = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    durableLogKeyCompaction: true,
    durableLogMaxEntriesPerTopic: 2,
  });
  bus2.setTopicKeyTtl('state', 100);
  bus2.publish('state', 'a4', { key: 'A' });
  await flush();
  // a4 supersedes a3 under keyed compaction, so only the latest A
  // record remains — and its keySeq is 4, continuing the baseline
  // (tombstone 2 -> a3 3 -> a4 4), never a renumbered 1.
  assert.deepEqual(keySeqsOf(), [4]);
});
