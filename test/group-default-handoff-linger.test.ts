import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-group-default-linger-'));

const noop = (): void => {};

// EB-471: bus-level terminal group handoff-linger default live revision.

test('EB-471: the factory bus default is unset (no window)', () => {
  const bus = new EventBus();
  assert.equal(bus.getGroupDefaultHandoffLinger(), undefined);
});

test('EB-471: a member that omits handoffLingerMs takes the bus-default window on leave', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  bus.setGroupDefaultHandoffLinger(30_000);
  const events: GroupRebalanceEvent[] = [];
  const member = bus.subscribeToGroup('g', 'jobs.*', noop);
  bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);

  member.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave' && e.memberId === member.id);
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, now + 30_000);
  assert.deepEqual(leave.lingering, [{ topic: 'jobs.email', fromSeq: 3, toSeq: 5 }]);
});

test('EB-471: an explicit window beats the bus default', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  bus.setGroupDefaultHandoffLinger(30_000);
  const events: GroupRebalanceEvent[] = [];
  const member = bus.subscribeToGroup('g', 'jobs.*', noop, { handoffLingerMs: 5_000 });
  bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);

  member.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave' && e.memberId === member.id);
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, now + 5_000);
  assert.deepEqual(leave.lingering, [{ topic: 'jobs.email', fromSeq: 3, toSeq: 5 }]);
});

test('EB-471: an explicit 0 does not take the bus default — no window on leave', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  bus.setGroupDefaultHandoffLinger(30_000);
  const events: GroupRebalanceEvent[] = [];
  const member = bus.subscribeToGroup('g', 'jobs.*', noop, { handoffLingerMs: 0 });
  bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);

  member.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave' && e.memberId === member.id);
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, undefined);
  assert.equal(leave.lingering, undefined);
});

test('EB-471: clearing with null returns new members to no window', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  bus.setGroupDefaultHandoffLinger(30_000);
  bus.setGroupDefaultHandoffLinger(null);
  assert.equal(bus.getGroupDefaultHandoffLinger(), undefined);
  const events: GroupRebalanceEvent[] = [];
  const member = bus.subscribeToGroup('g', 'jobs.*', noop);
  bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);

  member.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave' && e.memberId === member.id);
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, undefined);
  assert.equal(leave.lingering, undefined);
});

test('EB-471: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setGroupDefaultHandoffLinger(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_handoff_linger_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setGroupDefaultHandoffLinger(30_000);
  bus.setGroupDefaultHandoffLinger(30_000);
  let audit = bus.getSubscriptionAudit({ action: 'group_default_handoff_linger_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, handoffLingerMs: 30_000 });
  bus.setGroupDefaultHandoffLinger(null);
  bus.setGroupDefaultHandoffLinger(null);
  audit = bus.getSubscriptionAudit({ action: 'group_default_handoff_linger_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, { previous: 30_000, handoffLingerMs: null });
});

test('EB-471: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setGroupDefaultHandoffLinger(30_000);
  for (const bad of [-1, Number.NaN, Infinity, -0.5, '30000'] as unknown[]) {
    assert.throws(
      () => bus.setGroupDefaultHandoffLinger(bad as number),
      RangeError,
      `windowMs=${String(bad)}`,
    );
    assert.equal(bus.getGroupDefaultHandoffLinger(), 30_000);
  }
  assert.throws(() => bus.setGroupDefaultHandoffLinger(60_000, ''), RangeError);
  assert.equal(bus.getGroupDefaultHandoffLinger(), 30_000);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_handoff_linger_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-471: revising the bus default never moves an already-joined member window', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  bus.setGroupDefaultHandoffLinger(30_000);
  const events: GroupRebalanceEvent[] = [];
  const member = bus.subscribeToGroup('g', 'jobs.*', noop);
  bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);

  // The default moves after the member joined: its window stays 30s.
  bus.setGroupDefaultHandoffLinger(60_000);
  assert.equal(bus.getGroupDefaultHandoffLinger(), 60_000);
  member.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave' && e.memberId === member.id);
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, now + 30_000);
  assert.deepEqual(leave.lingering, [{ topic: 'jobs.email', fromSeq: 3, toSeq: 5 }]);
});

test('EB-471: an already-armed window keeps its stamped until; a member leaving after a revision gets the new default window', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  bus.setGroupDefaultHandoffLinger(30_000);
  const events: GroupRebalanceEvent[] = [];
  const a = bus.subscribeToGroup('g', 'jobs.*', noop);
  const b = bus.subscribeToGroup('g', 'jobs.*', noop);
  bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);

  // A leaves first: its window is armed at the original 30s value.
  a.unsubscribe();
  const leaveA = events.find((e) => e.trigger === 'leave' && e.memberId === a.id);
  assert.ok(leaveA !== undefined);
  assert.equal(leaveA.lingerUntil, now + 30_000);

  // Revising the default must not rewrite A's armed window, and B —
  // still joined — keeps its join-time 30s value too.
  bus.setGroupDefaultHandoffLinger(1_000);
  b.unsubscribe();
  const leaveB = events.find((e) => e.trigger === 'leave' && e.memberId === b.id);
  assert.ok(leaveB !== undefined);
  assert.equal(leaveB.lingerUntil, now + 30_000);

  // But a member joining after the revision takes the new default.
  const c = bus.subscribeToGroup('g', 'jobs.*', noop);
  c.unsubscribe();
  const leaveC = events.find((e) => e.trigger === 'leave' && e.memberId === c.id);
  assert.ok(leaveC !== undefined);
  assert.equal(leaveC.lingerUntil, now + 1_000);

  // Past C's 1s window but inside A's original 30s one, the in-flight
  // seqs are still covered — A's armed window was not shortened.
  now += 2_000;
  const replayed: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => replayed.push(m.seq), { resumeFromSeq: 3 });
  await flush();
  assert.deepEqual(replayed, [], "A's armed window still suppresses the replay");
});

test('EB-471: setGroupHandoffLinger still revises/clears one member and leaves the bus default alone', async () => {
  let now = 1_000_000;
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, now: () => now });
  bus.setGroupDefaultHandoffLinger(30_000);
  const events: GroupRebalanceEvent[] = [];
  const member = bus.subscribeToGroup('g', 'jobs.*', noop);
  bus.subscribeToGroup('g', 'jobs.*', noop, { onRebalance: (e) => events.push(e) });
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', {});
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);

  // Retune the member to 60s: the bus default stays 30s.
  bus.setGroupHandoffLinger(member.id, 60_000);
  assert.equal(bus.getGroupDefaultHandoffLinger(), 30_000);
  member.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave' && e.memberId === member.id);
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, now + 60_000);

  // Clear another default-derived member to 0: no window on leave,
  // and the bus default is still 30s for the next joiner.
  const closer = bus.subscribeToGroup('g', 'jobs.*', noop);
  bus.setGroupHandoffLinger(closer.id, 0);
  closer.unsubscribe();
  const leaveClosed = events.find((e) => e.trigger === 'leave' && e.memberId === closer.id);
  assert.ok(leaveClosed !== undefined);
  assert.equal(leaveClosed.lingerUntil, undefined);
  assert.equal(bus.getGroupDefaultHandoffLinger(), 30_000);
  const next = bus.subscribeToGroup('g', 'jobs.*', noop);
  next.unsubscribe();
  const leaveNext = events.find((e) => e.trigger === 'leave' && e.memberId === next.id);
  assert.ok(leaveNext !== undefined);
  assert.equal(leaveNext.lingerUntil, now + 30_000);
});

test('EB-471: without a durable log the default is lazily inert', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultHandoffLinger(30_000);
  const events: GroupRebalanceEvent[] = [];
  const member = bus.subscribeToGroup('g', 't', noop);
  bus.subscribeToGroup('g', 't', noop, { onRebalance: (e) => events.push(e) });
  bus.publish('t', {});
  await flush();

  member.unsubscribe();
  const leave = events.find((e) => e.trigger === 'leave' && e.memberId === member.id);
  assert.ok(leave !== undefined);
  assert.equal(leave.lingerUntil, undefined);
  assert.equal(leave.lingering, undefined);
});

test('EB-471: each revision audits its old and new window and actor; the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setGroupDefaultHandoffLinger(30_000, 'ops');
  assert.equal(bus.getGroupDefaultHandoffLinger(), 30_000);
  bus.setGroupDefaultHandoffLinger(60_000, 'ops');
  assert.equal(bus.getGroupDefaultHandoffLinger(), 60_000);
  bus.setGroupDefaultHandoffLinger(null, 'ops');
  assert.equal(bus.getGroupDefaultHandoffLinger(), undefined);
  const events = bus.getSubscriptionAudit({ action: 'group_default_handoff_linger_revised' });
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, handoffLingerMs: 30_000 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 30_000, handoffLingerMs: 60_000 });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 60_000, handoffLingerMs: null });
});
