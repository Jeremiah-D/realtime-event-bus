import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, appendFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-offsets-'));

test('group offsets: commitOffset is journaled and reseeded after restart', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.subscribeToGroup('workers', 'jobs.*', () => {});
  bus.publish('jobs.email', {});
  bus.publish('jobs.sms', {});
  bus.commitOffset('workers', 'jobs.email', 1);
  bus.commitOffset('workers', 'jobs.sms', 2);
  assert.ok(existsSync(join(dir, '__group_offsets.jsonl')));

  // A new bus over the same directory recovers the checkpoints — a
  // rejoining consumer resumes from where its predecessor committed.
  const restarted = new EventBus({ durableLogDir: dir });
  assert.deepEqual(restarted.getCommittedOffsets('workers'), {
    'jobs.email': 1,
    'jobs.sms': 2,
  });
});

test('group offsets: highest seq wins and corrupt journal lines are skipped', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.commitOffset('g', 't', 3);
  bus.commitOffset('g', 't', 1); // older commit must not clobber the max
  appendFileSync(join(dir, '__group_offsets.jsonl'), 'not json\n{"v":2,"group":"g","topic":"t","seq":99,"at":1}\n');
  const restarted = new EventBus({ durableLogDir: dir });
  assert.deepEqual(restarted.getCommittedOffsets('g'), { t: 3 });
  assert.equal(restarted.getStats().durableLog.corruptLines, 2);
});

test('group offsets: journal compacts to the latest commit per (group, topic)', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogMaxEntriesPerTopic: 8 });
  for (let i = 0; i < 20; i += 1) {
    bus.commitOffset('g', `t.${i}`, i + 1);
    bus.commitOffset('g', `t.${i}`, i + 101);
  }
  // 40 appends with a 2x8 budget: the journal rewrites down to one line
  // per (group, topic) instead of growing without bound.
  const lines = readFileSync(join(dir, '__group_offsets.jsonl'), 'utf8').trim().split('\n');
  assert.equal(lines.length, 20);
  assert.equal(bus.getStats().durableLog.offsetEntries, 20);
  const restarted = new EventBus({ durableLogDir: dir });
  const offsets = restarted.getCommittedOffsets('g');
  assert.equal(Object.keys(offsets).length, 20);
  assert.equal(offsets['t.19'], 120);
});

test('group offsets: without a durable log, commitOffset stays in-memory only', () => {
  const bus = new EventBus();
  bus.commitOffset('g', 't', 5);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 5 });
  // A restart with no journal has nothing to reseed from.
  const restarted = new EventBus();
  assert.deepEqual(restarted.getCommittedOffsets('g'), {});
});

test('rebalance linger: leave event carries the handoff window; replay during the linger skips in-flight seqs', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  const events: GroupRebalanceEvent[] = [];
  const leaver = bus.subscribeToGroup('g', 'jobs.*', () => {}, { handoffLingerMs: 30_000 });
  bus.subscribeToGroup('g', 'jobs.*', () => {}, { onRebalance: (e) => events.push(e) });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);
  leaver.unsubscribe();

  const leave = events.find((e) => e.trigger === 'leave');
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, now + 30_000);
  assert.deepEqual(leave.lingering, [{ topic: 'jobs.email', fromSeq: 3, toSeq: 5 }]);

  // A member that rejoins during the linger and resumes from the committed
  // offset must NOT re-receive the leaver's in-flight seqs 4-5.
  const replayed: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => replayed.push(m.seq), { resumeFromSeq: 3 });
  await flush();
  assert.deepEqual(replayed, []);

  // Live traffic is unaffected by the linger: new publishes fan out normally.
  const liveB: number[] = [];
  const liveC: number[] = [];
  bus.subscribeToGroup('g2', 'jobs.*', (m) => liveB.push(m.seq));
  bus.subscribeToGroup('g2', 'jobs.*', (m) => liveC.push(m.seq));
  bus.publish('jobs.email', {});
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual([...liveB, ...liveC].sort((a, b) => a - b), [6, 7]);
});

test('rebalance linger: after the window expires the backlog becomes replayable', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  const leaver = bus.subscribeToGroup('g', 'jobs.*', () => {}, { handoffLingerMs: 30_000 });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);
  leaver.unsubscribe();

  // The leaver is presumed dead past the linger: at-least-once resumes.
  now += 31_000;
  const replayed: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => replayed.push(m.seq), { resumeFromSeq: 3 });
  await flush();
  assert.deepEqual(replayed, [4, 5]);
});

test('rebalance linger: no window when the leaver committed everything', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const events: GroupRebalanceEvent[] = [];
  const leaver = bus.subscribeToGroup('g', 't', () => {}, {
    handoffLingerMs: 1000,
    onRebalance: (e) => events.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {}, { onRebalance: (e) => events.push(e) });
  bus.publish('t', {});
  await flush();
  bus.commitOffset('g', 't', 1);
  leaver.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave');
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, undefined);
  assert.equal(leave.lingering, undefined);
});

test('rebalance linger: inert without a durable log (nothing to replay)', async () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const leaver = bus.subscribeToGroup('g', 't', () => {}, {
    handoffLingerMs: 1000,
    onRebalance: (e) => events.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {}, { onRebalance: (e) => events.push(e) });
  bus.publish('t', {});
  await flush();
  leaver.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave');
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, undefined);
  assert.equal(leave.lingering, undefined);
});

test('rebalance linger: handoffLingerMs validation throws before registering', () => {
  const bus = new EventBus();
  for (const bad of [-1, NaN, Infinity]) {
    assert.throws(
      () => bus.subscribeToGroup('g', 't', () => {}, { handoffLingerMs: bad }),
      RangeError,
    );
  }
  // The failed subscribes left nothing registered behind.
  assert.deepEqual(bus.getStats().consumerGroups, []);
});
