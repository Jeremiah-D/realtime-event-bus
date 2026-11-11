import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-groupdefaultautooffsetreset-'));

// EB-465: consumer-group auto.offset.reset bus-level terminal default live revision.

test('EB-465: the factory bus default is earliest', () => {
  const bus = new EventBus();
  assert.equal(bus.getGroupDefaultAutoOffsetReset(), 'earliest');
});

test('EB-465: bus default latest — a new group member with no option starts at the live stream', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', {});
  bus.setGroupDefaultAutoOffsetReset('latest');
  const got: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => got.push(m.seq));
  bus.publish('jobs.email', {});
  await flush();
  // The backlog is skipped: only post-join traffic arrives.
  assert.deepEqual(got, [4]);
});

test('EB-465: a group rule beats the bus default in both directions', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('t', {});
  bus.setGroupDefaultAutoOffsetReset('latest');
  bus.setGroupAutoOffsetReset('g', 't', 'earliest');
  // Group 'earliest' beats a 'latest' bus default: full replay.
  const a: number[] = [];
  bus.subscribeToGroup('g', 't', (m) => a.push(m.seq));
  await flush();
  assert.deepEqual(a, [1, 2, 3]);

  bus.setGroupDefaultAutoOffsetReset('earliest');
  bus.setGroupAutoOffsetReset('h', 't', 'latest');
  // Group 'latest' beats an 'earliest' bus default: live start only.
  const b: number[] = [];
  bus.subscribeToGroup('h', 't', (m) => b.push(m.seq));
  bus.publish('t', {});
  await flush();
  assert.deepEqual(b, [4]);
});

test('EB-465: a member-explicit option beats the bus default in both directions', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('t', {});
  bus.setGroupDefaultAutoOffsetReset('latest');
  // Explicit 'earliest' beats a 'latest' bus default: full replay.
  const a: number[] = [];
  bus.subscribeToGroup('g', 't', (m) => a.push(m.seq), { autoOffsetReset: 'earliest' });
  await flush();
  assert.deepEqual(a, [1, 2, 3]);

  bus.setGroupDefaultAutoOffsetReset('earliest');
  // Explicit 'latest' beats an 'earliest' bus default: live start only.
  const b: number[] = [];
  bus.subscribeToGroup('h', 't', (m) => b.push(m.seq), { autoOffsetReset: 'latest' });
  bus.publish('t', {});
  await flush();
  assert.deepEqual(b, [4]);
});

test('EB-465: an explicit resumeFromSeq wins over the bus default', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  bus.setGroupDefaultAutoOffsetReset('latest');
  const replayed: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => replayed.push(m.seq), { resumeFromSeq: 2 });
  await flush();
  assert.deepEqual(replayed, [3, 4, 5], 'resumeFromSeq replays from the cursor, never the default');
});

test('EB-465: resetOnlyOnce — a committed offset suppresses the bus default on rejoin', async () => {
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

  bus.setGroupDefaultAutoOffsetReset('latest');
  const second: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => second.push(m.seq));
  await flush();
  assert.deepEqual(second, [], 'the default never fires once anything is committed');
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual(second, [6]);
});

test('EB-465: an illegal policy or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoOffsetReset('latest');
  for (const bad of ['sometimes', 'EARLIEST', 42, null] as unknown[]) {
    assert.throws(
      () => bus.setGroupDefaultAutoOffsetReset(bad as 'earliest'),
      RangeError,
      `policy=${String(bad)}`,
    );
    assert.equal(bus.getGroupDefaultAutoOffsetReset(), 'latest');
  }
  assert.throws(() => bus.setGroupDefaultAutoOffsetReset('earliest', ''), RangeError);
  assert.equal(bus.getGroupDefaultAutoOffsetReset(), 'latest');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_auto_offset_reset_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-465: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoOffsetReset('earliest');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_auto_offset_reset_revised' }).length,
    0,
  );
  bus.setGroupDefaultAutoOffsetReset('latest');
  bus.setGroupDefaultAutoOffsetReset('latest');
  const audit = bus.getSubscriptionAudit({ action: 'group_default_auto_offset_reset_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous_policy: 'earliest', policy: 'latest' });
});

test('EB-465: setGroupDefaultAutoOffsetReset audits each revision with its old and new policy and actor', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoOffsetReset('latest', 'ops');
  bus.setGroupDefaultAutoOffsetReset('earliest', 'ops');
  const events = bus.getSubscriptionAudit({ action: 'group_default_auto_offset_reset_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous_policy: 'earliest', policy: 'latest' });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous_policy: 'latest', policy: 'earliest' });
});

test('EB-465: the getter follows the live default', () => {
  const bus = new EventBus();
  assert.equal(bus.getGroupDefaultAutoOffsetReset(), 'earliest');
  bus.setGroupDefaultAutoOffsetReset('latest');
  assert.equal(bus.getGroupDefaultAutoOffsetReset(), 'latest');
  bus.setGroupDefaultAutoOffsetReset('earliest');
  assert.equal(bus.getGroupDefaultAutoOffsetReset(), 'earliest');
});

test('EB-465: revising the bus default touches no live member or committed offset and fires no rebalance', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', {});
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
  const got: number[] = [];
  const m1 = bus.subscribeToGroup('g', 'jobs.*', (m) => got.push(m.seq), { onRebalance });
  await flush();
  assert.deepEqual(got, [1, 2, 3]);
  bus.commitOffset('g', 'jobs.email', 3);
  events.length = 0;
  bus.setGroupDefaultAutoOffsetReset('latest');
  assert.equal(events.length, 0, 'the revision itself must not rebalance');
  assert.deepEqual(bus.getGroupMembers('g', 'jobs.*'), [m1.id]);
  assert.deepEqual(bus.getCommittedOffsets('g'), { 'jobs.email': 3 });
  // The live member keeps receiving live traffic at its original position.
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual(got, [1, 2, 3, 4]);
  m1.unsubscribe();
});
