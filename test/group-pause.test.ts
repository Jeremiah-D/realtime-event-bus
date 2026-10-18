import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type GroupRebalanceEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('pauseGroup/resumeGroup reject empty and unknown group ids', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't.*', () => {});
  for (const bad of ['', 'nope']) {
    assert.throws(() => bus.pauseGroup(bad), RangeError, `pauseGroup(${JSON.stringify(bad)})`);
    assert.throws(() => bus.resumeGroup(bad), RangeError, `resumeGroup(${JSON.stringify(bad)})`);
  }
  // A live group id passes validation even when not paused.
  assert.equal(bus.resumeGroup('g'), false);
});

test('pause holds delivery; resume continues round-robin from the pause point', async () => {
  const bus = new EventBus();
  const seenA: number[] = [];
  const seenB: number[] = [];
  const subA = bus.subscribeToGroup('workers', 'jobs.*', (m) => seenA.push(m.seq));
  const subB = bus.subscribeToGroup('workers', 'jobs.*', (m) => seenB.push(m.seq));

  assert.equal(bus.pauseGroup('workers'), true);
  assert.equal(bus.pauseGroup('workers'), false); // idempotent
  assert.deepEqual(bus.getStats().pausedGroups, ['workers']);

  for (let i = 0; i < 4; i += 1) bus.publish('jobs.email', { i });
  await flush();
  // Nothing delivered while paused — but the backlog is retained per
  // member, under the normal backpressure policy.
  assert.deepEqual(seenA, []);
  assert.deepEqual(seenB, []);
  assert.equal(bus.pendingCount(subA.id) + bus.pendingCount(subB.id), 4);

  assert.equal(bus.resumeGroup('workers'), true);
  assert.equal(bus.resumeGroup('workers'), false); // idempotent
  assert.deepEqual(bus.getStats().pausedGroups, []);
  await flush();
  // Round-robin continued across the pause: A takes seq 1,3; B takes 2,4.
  assert.deepEqual(seenA, [1, 3]);
  assert.deepEqual(seenB, [2, 4]);
});

test('backpressure still applies while a group is paused', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeToGroup('g', 't', (m) => seen.push(m.seq), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  bus.pauseGroup('g');
  for (let i = 0; i < 5; i += 1) bus.publish('t', { i });
  await flush();
  assert.deepEqual(seen, []);
  // The pause itself drops nothing; the member's own drop-oldest policy
  // shed the overflow while the backlog accumulated.
  assert.equal(bus.pendingCount(sub.id), 2);
  bus.resumeGroup('g');
  await flush();
  assert.deepEqual(seen, [4, 5]);
  assert.equal(bus.getStats().droppedMessages, 3);
});

test('join/leave during a pause still rebalance but deliver nothing', async () => {
  const bus = new EventBus();
  const rebalances: GroupRebalanceEvent[] = [];
  const seenA: number[] = [];
  const seenB: number[] = [];
  const subA = bus.subscribeToGroup('g', 't', (m) => seenA.push(m.seq), {
    onRebalance: (e) => rebalances.push(e),
  });
  bus.pauseGroup('g');
  const joinsBefore = rebalances.filter((e) => e.trigger === 'join').length;
  const subB = bus.subscribeToGroup('g', 't', (m) => seenB.push(m.seq), {
    onRebalance: (e) => rebalances.push(e),
  });
  // The join still fired a rebalance…
  assert.ok(rebalances.filter((e) => e.trigger === 'join').length > joinsBefore);
  // Round-robin: A takes the first message, B the second.
  bus.publish('t', {});
  bus.publish('t', {});
  await flush();
  // …but nothing was delivered while paused.
  assert.deepEqual(seenA, []);
  assert.deepEqual(seenB, []);
  // Leave also rebalances; the survivor stays paused.
  subA.unsubscribe();
  await flush();
  assert.ok(rebalances.some((e) => e.trigger === 'leave'));
  assert.deepEqual(seenB, []);
  bus.resumeGroup('g');
  await flush();
  // B keeps its own assigned backlog (seq 2); A's copy left with A.
  assert.deepEqual(seenB, [2]);
  assert.equal(subB.id.length > 0, true);
});

test('pause is sticky across full member churn', async () => {
  const bus = new EventBus();
  const seenOld: number[] = [];
  const seenNew: number[] = [];
  const old = bus.subscribeToGroup('g', 't', (m) => seenOld.push(m.seq));
  bus.pauseGroup('g');
  old.unsubscribe();
  await flush();
  // Every member left, but the pause was never resumed: the group
  // re-created under the same groupId stays paused…
  const fresh = bus.subscribeToGroup('g', 't', (m) => seenNew.push(m.seq));
  bus.publish('t', {});
  await flush();
  assert.deepEqual(seenNew, []);
  assert.deepEqual(bus.getStats().pausedGroups, ['g']);
  bus.resumeGroup('g');
  await flush();
  assert.deepEqual(seenNew, [1]);
  assert.equal(fresh.id.length > 0, true);
});

test('pause/resume works for partitioned groups', async () => {
  const bus = new EventBus();
  const seenA: number[] = [];
  const seenB: number[] = [];
  bus.subscribeToGroup('g', 't', (m) => seenA.push(m.seq), { partitions: 2 });
  bus.subscribeToGroup('g', 't', (m) => seenB.push(m.seq), { partitions: 2 });
  bus.pauseGroup('g');
  for (let i = 0; i < 6; i += 1) bus.publish('t', { i });
  await flush();
  assert.deepEqual(seenA, []);
  assert.deepEqual(seenB, []);
  bus.resumeGroup('g');
  await flush();
  // Every message delivered exactly once across the two members.
  const all = [...seenA, ...seenB].sort((a, b) => a - b);
  assert.deepEqual(all, [1, 2, 3, 4, 5, 6]);
});

test('pausing one group leaves other groups and plain subscribers alone', async () => {
  const bus = new EventBus();
  const seenG1: number[] = [];
  const seenG2: number[] = [];
  const seenPlain: number[] = [];
  bus.subscribeToGroup('g1', 't', (m) => seenG1.push(m.seq));
  bus.subscribeToGroup('g2', 't', (m) => seenG2.push(m.seq));
  bus.subscribe('t', (m) => seenPlain.push(m.seq));
  bus.pauseGroup('g1');
  bus.publish('t', {});
  await flush();
  assert.deepEqual(seenG1, []);
  assert.deepEqual(seenG2, [1]);
  assert.deepEqual(seenPlain, [1]);
  bus.resumeGroup('g1');
  await flush();
  assert.deepEqual(seenG1, [1]);
});

test('the same groupId on two patterns pauses both competing sets', async () => {
  const bus = new EventBus();
  const seenA: number[] = [];
  const seenB: number[] = [];
  bus.subscribeToGroup('g', 'a.*', (m) => seenA.push(m.seq));
  bus.subscribeToGroup('g', 'b.*', (m) => seenB.push(m.seq));
  bus.pauseGroup('g');
  bus.publish('a.x', {});
  bus.publish('b.y', {});
  await flush();
  assert.deepEqual(seenA, []);
  assert.deepEqual(seenB, []);
  bus.resumeGroup('g');
  await flush();
  assert.deepEqual(seenA, [1]);
  assert.deepEqual(seenB, [1]);
});

test('pause/resume are audited as group_paused/group_resumed', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {});
  bus.pauseGroup('g');
  bus.pauseGroup('g'); // idempotent: no second audit entry
  bus.resumeGroup('g');
  const paused = bus.getSubscriptionAudit({ action: 'group_paused' });
  const resumed = bus.getSubscriptionAudit({ action: 'group_resumed' });
  assert.equal(paused.length, 1);
  assert.equal(paused[0].groupId, 'g');
  assert.equal(resumed.length, 1);
  assert.equal(resumed[0].groupId, 'g');
});
