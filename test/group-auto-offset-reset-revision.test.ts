import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-groupautooffsetreset-'));

// EB-400: consumer-group auto.offset.reset group-level default live revision.

test('EB-400: group default latest — a member with no option starts at the live stream', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', {});
  bus.setGroupAutoOffsetReset('g', 'jobs.*', 'latest');
  const got: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => got.push(m.seq));
  bus.publish('jobs.email', {});
  await flush();
  // The backlog is skipped: only post-join traffic arrives.
  assert.deepEqual(got, [4]);
});

test('EB-400: group default earliest — a member with no option replays from the log start', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', {});
  bus.setGroupAutoOffsetReset('g', 'jobs.*', 'earliest');
  const got: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => got.push(m.seq));
  await flush();
  assert.deepEqual(got, [1, 2, 3]);
});

test('EB-400: a member-explicit option wins over the group default in both directions', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('t', {});
  bus.setGroupAutoOffsetReset('g', 't', 'latest');
  // Explicit 'earliest' beats a 'latest' default: full replay.
  const a: number[] = [];
  bus.subscribeToGroup('g', 't', (m) => a.push(m.seq), { autoOffsetReset: 'earliest' });
  await flush();
  assert.deepEqual(a, [1, 2, 3]);

  bus.setGroupAutoOffsetReset('h', 't', 'earliest');
  // Explicit 'latest' beats an 'earliest' default: live start only.
  const b: number[] = [];
  bus.subscribeToGroup('h', 't', (m) => b.push(m.seq), { autoOffsetReset: 'latest' });
  bus.publish('t', {});
  await flush();
  assert.deepEqual(b, [4]);
});

test('EB-400: resetOnlyOnce — a committed offset suppresses the group default on rejoin', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  const first: number[] = [];
  const m1 = bus.subscribeToGroup('g', 'jobs.*', (m) => first.push(m.seq));
  await flush();
  assert.deepEqual(first, [1, 2, 3, 4, 5]);
  bus.commitOffset('g', 'jobs.email', 3);
  m1.unsubscribe();
  await flush();

  bus.setGroupAutoOffsetReset('g', 'jobs.*', 'earliest');
  const second: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => second.push(m.seq));
  await flush();
  assert.deepEqual(second, [], 'the default never fires once anything is committed');
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual(second, [6]);
});

test('EB-400: clear restores the member default, and clearing a never-set default is a false no-op', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('t', {});
  bus.setGroupAutoOffsetReset('g', 't', 'latest');
  assert.equal(bus.clearGroupAutoOffsetReset('g', 't'), true);
  assert.equal(bus.getGroupAutoOffsetReset('g', 't'), undefined);
  const got: number[] = [];
  bus.subscribeToGroup('g', 't', (m) => got.push(m.seq));
  await flush();
  assert.deepEqual(got, [1, 2, 3], 'back to the member default earliest');

  // Clearing again: nothing stored, no audit.
  assert.equal(bus.clearGroupAutoOffsetReset('g', 't'), false);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_auto_offset_reset_cleared' }).length,
    1,
  );
});

test('EB-400: an illegal policy throws RangeError and leaves the live default untouched', () => {
  const bus = new EventBus();
  bus.setGroupAutoOffsetReset('g', 't', 'latest');
  for (const bad of ['sometimes', 'EARLIEST', 42, null] as unknown[]) {
    assert.throws(
      () => bus.setGroupAutoOffsetReset('g', 't', bad as 'earliest'),
      RangeError,
      `policy=${String(bad)}`,
    );
  }
  assert.equal(bus.getGroupAutoOffsetReset('g', 't'), 'latest');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_auto_offset_reset_revised' }).length,
    1,
  );
});

test('EB-400: an empty groupId or pattern throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setGroupAutoOffsetReset('', 't', 'latest'), RangeError);
  assert.throws(() => bus.setGroupAutoOffsetReset('g', '', 'latest'), RangeError);
  assert.throws(() => bus.setGroupAutoOffsetReset('g', 't', 'latest', ''), RangeError);
  assert.throws(() => bus.getGroupAutoOffsetReset('', 't'), RangeError);
  assert.throws(() => bus.getGroupAutoOffsetReset('g', ''), RangeError);
  assert.throws(() => bus.clearGroupAutoOffsetReset('', 't'), RangeError);
  assert.throws(() => bus.clearGroupAutoOffsetReset('g', ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_auto_offset_reset_revised' }).length, 0);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_auto_offset_reset_cleared' }).length, 0);
});

test('EB-400: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setGroupAutoOffsetReset('g', 't', 'latest');
  bus.setGroupAutoOffsetReset('g', 't', 'latest');
  const audit = bus.getSubscriptionAudit({ action: 'group_auto_offset_reset_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous_policy: null, policy: 'latest' });
});

test('EB-400: audit detail carries previous/policy exactly, with actor attribution', () => {
  const bus = new EventBus();
  bus.setGroupAutoOffsetReset('g', 't.*', 'latest', 'ops');
  bus.setGroupAutoOffsetReset('g', 't.*', 'earliest', 'ops');
  const revised = bus.getSubscriptionAudit({ action: 'group_auto_offset_reset_revised' });
  assert.equal(revised.length, 2);
  assert.equal(revised[0].groupId, 'g');
  assert.equal(revised[0].pattern, 't.*');
  assert.equal(revised[0].actor, 'ops');
  assert.deepEqual(revised[0].detail, { previous_policy: null, policy: 'latest' });
  assert.deepEqual(revised[1].detail, { previous_policy: 'latest', policy: 'earliest' });

  assert.equal(bus.clearGroupAutoOffsetReset('g', 't.*', 'ops'), true);
  const cleared = bus.getSubscriptionAudit({ action: 'group_auto_offset_reset_cleared' });
  assert.equal(cleared.length, 1);
  assert.equal(cleared[0].actor, 'ops');
  assert.deepEqual(cleared[0].detail, { previous_policy: 'earliest' });
});

test('EB-400: the getter follows the live value and getStats reports the default', () => {
  const bus = new EventBus();
  assert.equal(bus.getGroupAutoOffsetReset('g', 't'), undefined);
  bus.subscribeToGroup('g', 't', () => {});
  assert.deepEqual(bus.getStats().consumerGroups, [
    { groupId: 'g', pattern: 't', members: 1 },
  ]);
  bus.setGroupAutoOffsetReset('g', 't', 'latest');
  assert.equal(bus.getGroupAutoOffsetReset('g', 't'), 'latest');
  assert.deepEqual(bus.getStats().consumerGroups, [
    { groupId: 'g', pattern: 't', members: 1, autoOffsetReset: 'latest' },
  ]);
  bus.clearGroupAutoOffsetReset('g', 't');
  assert.equal(bus.getGroupAutoOffsetReset('g', 't'), undefined);
  assert.deepEqual(bus.getStats().consumerGroups, [
    { groupId: 'g', pattern: 't', members: 1 },
  ]);
});

test('EB-400: partitioned groups — an uncommitted partition follows the group default', async () => {
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

  // Group default 'latest': partition 0 recovers (2, 6] via its committed
  // offset; partition 1 has none, so the default skips its backlog.
  bus.setGroupAutoOffsetReset('g', 'pp', 'latest');
  const got: string[] = [];
  bus.subscribeToGroup('g', 'pp', (m) => got.push(m.payload as string), { partitions: 2 });
  await sleep(100);
  assert.deepEqual(got, ['p0-c', 'p0-d']);
});

test('EB-400: an explicit resumeFromSeq wins over the group default', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('t', {});
  bus.setGroupAutoOffsetReset('g', 't', 'latest');
  const got: number[] = [];
  bus.subscribeToGroup('g', 't', (m) => got.push(m.seq), { resumeFromSeq: 2 });
  await flush();
  assert.deepEqual(got, [3]);
});
