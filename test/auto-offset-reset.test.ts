import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-autooffsetreset-'));

// EB-207: consumer-group auto.offset.reset semantics.

test('EB-207: earliest (default) replays the durable-log backlog on first join', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', {});
  const got: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => got.push(m.seq));
  await flush();
  assert.deepEqual(got, [1, 2, 3]);
});

test('EB-207: earliest with no durable log starts at the live stream', async () => {
  const bus = new EventBus();
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', {});
  const got: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => got.push(m.seq));
  bus.publish('jobs.email', {});
  await flush();
  // No durable log: no history to replay — the member starts live.
  assert.deepEqual(got, [4]);
});

test('EB-207: latest only sees messages published after the join', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', {});
  const got: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => got.push(m.seq), { autoOffsetReset: 'latest' });
  bus.publish('jobs.email', {});
  await flush();
  // Starts at max assigned seq + 1: pre-join backlog is skipped.
  assert.deepEqual(got, [4]);
});

test('EB-207: latest with no durable log is a plain live start', async () => {
  const bus = new EventBus();
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', {});
  const got: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => got.push(m.seq), { autoOffsetReset: 'latest' });
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual(got, [4]);
});

test('EB-207: resetOnlyOnce — a committed offset suppresses the reset on rejoin', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  const first: number[] = [];
  const m1 = bus.subscribeToGroup('g', 'jobs.*', (m) => first.push(m.seq));
  await flush();
  assert.deepEqual(first, [1, 2, 3, 4, 5], 'first join with earliest replays the backlog');
  bus.commitOffset('g', 'jobs.email', 3);
  m1.unsubscribe();
  await flush();

  // A rejoin with a committed offset must NOT re-apply autoOffsetReset:
  // the reset fires only while nothing is committed.
  const second: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => second.push(m.seq));
  await flush();
  assert.deepEqual(second, [], 'no reset redelivery on rejoin');
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual(second, [6], 'only post-rejoin messages are delivered live');
  assert.deepEqual(bus.getCommittedOffsets('g'), { 'jobs.email': 3 });
});

test('EB-207: resetOnlyOnce — a rejoin with resumeFromSeq uses the committed-offset recovery path', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  const m1 = bus.subscribeToGroup('g', 'jobs.*', () => {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);
  m1.unsubscribe();
  await flush();

  // The operator seeds resumeFromSeq from getCommittedOffsets: the normal
  // recovery path — no autoOffsetReset re-application.
  const replayed: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => replayed.push(m.seq), { resumeFromSeq: 3 });
  await flush();
  assert.deepEqual(replayed, [4, 5]);
});

test('EB-207: a later member first-joining with nothing committed also gets the reset', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('t', {});
  bus.publish('t', {});
  const a: number[] = [];
  const b: number[] = [];
  bus.subscribeToGroup('g', 't', (m) => a.push(m.seq));
  await flush();
  assert.deepEqual(a, [1, 2]);
  // No commit happened anywhere: B's first join applies the policy too.
  bus.subscribeToGroup('g', 't', (m) => b.push(m.seq));
  await flush();
  assert.deepEqual(b, [1, 2]);
});

test('EB-207: explicit resumeFromSeq wins over autoOffsetReset', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('t', {});
  const got: number[] = [];
  bus.subscribeToGroup('g', 't', (m) => got.push(m.seq), {
    autoOffsetReset: 'earliest',
    resumeFromSeq: 2,
  });
  await flush();
  assert.deepEqual(got, [3]);
});

test('EB-207: invalid autoOffsetReset throws RangeError and registers nothing', () => {
  const bus = new EventBus();
  for (const bad of ['sometimes', 'EARLIEST', 42, null] as unknown[]) {
    assert.throws(
      () => bus.subscribeToGroup('g', 't', () => {}, { autoOffsetReset: bad as 'earliest' }),
      RangeError,
      `autoOffsetReset=${String(bad)}`,
    );
  }
  // No half-joined member: no group row, no roster, no subscriber.
  assert.deepEqual(bus.getStats().consumerGroups, []);
  assert.deepEqual(bus.getGroupMembers('g', 't'), []);
  assert.deepEqual(bus.getStats().subscribersByPattern, {});
});

test('EB-207: partitioned groups decide per partition — committed recovers, uncommitted latest-skips', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicPartitions('pp', 4);
  const m1 = bus.subscribeToGroup('g', 'pp', () => {}, { partitions: 2 });
  // Publish-side partitions fold into the group's space (mod 2): 0 -> 0, 1 -> 1.
  bus.publish('pp', 'p0-a', { partition: 0 }); // seq 1
  bus.publish('pp', 'p0-b', { partition: 0 }); // seq 2
  bus.publish('pp', 'p1-a', { partition: 1 }); // seq 3
  bus.publish('pp', 'p1-b', { partition: 1 }); // seq 4
  await sleep(50);
  // Commit only partition 0's watermark.
  bus.commitOffset('g', 'pp', 2, { partition: 0 });
  bus.publish('pp', 'p0-c', { partition: 0 }); // seq 5
  bus.publish('pp', 'p0-d', { partition: 0 }); // seq 6
  await sleep(50);
  m1.unsubscribe();
  await flush();

  const got: string[] = [];
  bus.subscribeToGroup('g', 'pp', (m) => got.push(m.payload as string), {
    partitions: 2,
    autoOffsetReset: 'latest',
  });
  await sleep(100);
  // Partition 0 has a committed offset: normal recovery replays (2, 6] —
  // the two new partition-0 messages. Partition 1 has none: 'latest'
  // skips its backlog (no redelivery of p1-a/p1-b).
  assert.deepEqual(got, ['p0-c', 'p0-d']);

  // Live traffic still flows to both partitions.
  bus.publish('pp', 'p1-c', { partition: 1 });
  await sleep(50);
  assert.deepEqual(got, ['p0-c', 'p0-d', 'p1-c']);
});

test('EB-207: partitioned earliest replays the uncommitted partition fully on rejoin', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicPartitions('pp', 4);
  const m1 = bus.subscribeToGroup('g', 'pp', () => {}, { partitions: 2 });
  bus.publish('pp', 'p0-a', { partition: 0 }); // seq 1
  bus.publish('pp', 'p0-b', { partition: 0 }); // seq 2
  bus.publish('pp', 'p1-a', { partition: 1 }); // seq 3
  bus.publish('pp', 'p1-b', { partition: 1 }); // seq 4
  await sleep(50);
  bus.commitOffset('g', 'pp', 2, { partition: 0 });
  bus.publish('pp', 'p0-c', { partition: 0 }); // seq 5
  bus.publish('pp', 'p0-d', { partition: 0 }); // seq 6
  await sleep(50);
  m1.unsubscribe();
  await flush();

  const got: string[] = [];
  bus.subscribeToGroup('g', 'pp', (m) => got.push(m.payload as string), {
    partitions: 2,
    autoOffsetReset: 'earliest',
  });
  await sleep(100);
  // Partition 0: committed at 2, so the recovery path replays (2, 6] —
  // not a full reset. Partition 1: nothing committed, so 'earliest'
  // replays its whole backlog from the log's start. Each partition
  // replays in its own pass (migration order), so compare as a set.
  assert.deepEqual([...got].sort(), ['p0-c', 'p0-d', 'p1-a', 'p1-b']);
});
