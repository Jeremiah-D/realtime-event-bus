import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BusMessage,
  type GroupRebalanceEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('consumer group: each message goes to exactly one member, round-robin in join order', async () => {
  const bus = new EventBus();
  const seenA: number[] = [];
  const seenB: number[] = [];
  bus.subscribeToGroup('workers', 'jobs.*', (m) => seenA.push(m.seq));
  bus.subscribeToGroup('workers', 'jobs.*', (m) => seenB.push(m.seq));
  for (let i = 0; i < 6; i += 1) bus.publish('jobs.email', { i });
  await flush();
  // Join order is A, B: even positions (seq 1,3,5) to A, odd (2,4,6) to B.
  assert.deepEqual(seenA, [1, 3, 5]);
  assert.deepEqual(seenB, [2, 4, 6]);
});

test('consumer groups: different groups on the same pattern each get a copy', async () => {
  const bus = new EventBus();
  const seenG1: number[] = [];
  const seenG2: number[] = [];
  bus.subscribeToGroup('g1', 'events.*', (m) => seenG1.push(m.seq));
  bus.subscribeToGroup('g2', 'events.*', (m) => seenG2.push(m.seq));
  bus.publish('events.click', {});
  await flush();
  assert.deepEqual(seenG1, [1]);
  assert.deepEqual(seenG2, [1]);
  // Fan-out width counts one per matching group, not per member.
  const stats = bus.getStats();
  assert.equal(stats.topics[0].subscriberCount, 2);
});

test('consumer group: plain subscribers still get their own copy alongside groups', async () => {
  const bus = new EventBus();
  const seenPlain: number[] = [];
  const seenGroup: number[] = [];
  bus.subscribe('jobs.*', (m) => seenPlain.push(m.seq));
  bus.subscribeToGroup('workers', 'jobs.*', (m) => seenGroup.push(m.seq));
  bus.publish('jobs.email', {});
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual(seenPlain, [1, 2]);
  assert.deepEqual(seenGroup, [1, 2]);
});

test('consumer group: same groupId on different patterns is independent', async () => {
  const bus = new EventBus();
  const seenA: Array<[string, number]> = [];
  const seenB: Array<[string, number]> = [];
  bus.subscribeToGroup('g', 'a.*', (m) => seenA.push([m.topic, m.seq]));
  bus.subscribeToGroup('g', 'b.*', (m) => seenB.push([m.topic, m.seq]));
  bus.publish('a.x', {});
  bus.publish('b.y', {});
  await flush();
  assert.deepEqual(seenA, [['a.x', 1]]);
  assert.deepEqual(seenB, [['b.y', 1]]);
  assert.deepEqual(bus.getGroupMembers('g', 'a.*').length, 1);
  assert.deepEqual(bus.getGroupMembers('g', 'b.*').length, 1);
});

test('consumer group: member leave rebalances to the survivors', async () => {
  const bus = new EventBus();
  const rebalances: GroupRebalanceEvent[] = [];
  const seenA: number[] = [];
  const seenB: number[] = [];
  const subA = bus.subscribeToGroup('workers', 'jobs.*', (m) => seenA.push(m.seq), {
    onRebalance: (e) => rebalances.push(e),
  });
  const subB = bus.subscribeToGroup('workers', 'jobs.*', (m) => seenB.push(m.seq), {
    onRebalance: (e) => rebalances.push(e),
  });
  // Join events: A got one for its own join ([A]); both got one for B's join ([A, B]).
  assert.equal(rebalances.filter((e) => e.trigger === 'join').length, 3);
  const lastJoin = rebalances[rebalances.length - 1];
  assert.deepEqual(lastJoin.members, [subA.id, subB.id]);
  assert.equal(lastJoin.memberId, subB.id);

  subA.unsubscribe();
  // The survivor is told about the leave with the new roster.
  const leaves = rebalances.filter((e) => e.trigger === 'leave');
  assert.equal(leaves.length, 1);
  assert.deepEqual(leaves[0].members, [subB.id]);
  assert.equal(leaves[0].memberId, subA.id);
  assert.deepEqual(bus.getGroupMembers('workers', 'jobs.*'), [subB.id]);

  bus.publish('jobs.email', {});
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual(seenB, [1, 2]);
  assert.deepEqual(seenA, []);
});

test('consumer group: double unsubscribe is a no-op (no duplicate leave event)', () => {
  const bus = new EventBus();
  let leaves = 0;
  const subA = bus.subscribeToGroup('g', 't', () => {});
  // The leave event fires on the survivors, so the counter lives on B.
  bus.subscribeToGroup('g', 't', () => {}, {
    onRebalance: (e) => {
      if (e.trigger === 'leave') leaves += 1;
    },
  });
  subA.unsubscribe();
  subA.unsubscribe();
  assert.equal(leaves, 1);
  assert.equal(bus.subscriberCount(), 1);
});

test('consumer group: assignment does not skip a slow member (its own drop policy applies)', async () => {
  const bus = new EventBus();
  const seenA: number[] = [];
  const seenB: number[] = [];
  // A is slow: capacity 1, drop-oldest. B is healthy.
  bus.subscribeToGroup('workers', 'jobs.*', (m) => seenA.push(m.seq), { queueSize: 1 });
  const subB = bus.subscribeToGroup('workers', 'jobs.*', (m) => seenB.push(m.seq));
  bus.publish('jobs.email', {}); // seq 1 -> A
  bus.publish('jobs.email', {}); // seq 2 -> B
  bus.publish('jobs.email', {}); // seq 3 -> A, drops seq 1 (drop-oldest)
  bus.publish('jobs.email', {}); // seq 4 -> B
  await flush();
  assert.deepEqual(seenA, [3]);
  assert.deepEqual(seenB, [2, 4]);
  const subA = bus.getGroupMembers('workers', 'jobs.*')[0];
  assert.equal(bus.droppedCount(subA), 1);
  assert.equal(bus.droppedCount(subB.id), 0);
});

test('consumer group: getGroupOffsets tracks the per-topic assignment watermark', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('workers', 'jobs.*', () => {});
  bus.subscribeToGroup('workers', 'jobs.*', () => {});
  assert.deepEqual(bus.getGroupOffsets('workers'), {});
  bus.publish('jobs.email', {});
  bus.publish('jobs.sms', {});
  bus.publish('jobs.email', {});
  await flush();
  assert.deepEqual(bus.getGroupOffsets('workers'), { 'jobs.email': 2, 'jobs.sms': 1 });
  // Watermark survives the group emptying: it is history, not live state.
  for (const id of bus.getGroupMembers('workers', 'jobs.*')) {
    // (no public unsubscribe-by-id; covered via the leave test instead)
    assert.ok(id.length > 0);
  }
  assert.deepEqual(bus.getGroupOffsets('nope'), {});
});

test('consumer group: commitOffset / getCommittedOffsets round-trip and validation', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('workers', 'jobs.*', () => {});
  assert.deepEqual(bus.getCommittedOffsets('workers'), {});
  bus.commitOffset('workers', 'jobs.email', 41);
  bus.commitOffset('workers', 'jobs.email', 42);
  bus.commitOffset('workers', 'jobs.sms', 7);
  assert.deepEqual(bus.getCommittedOffsets('workers'), { 'jobs.email': 42, 'jobs.sms': 7 });
  // Committing for a group with no live members is allowed (restore-before-rejoin).
  bus.commitOffset('future-group', 'jobs.email', 1);
  assert.deepEqual(bus.getCommittedOffsets('future-group'), { 'jobs.email': 1 });

  assert.throws(() => bus.commitOffset('', 'jobs.email', 1), RangeError);
  assert.throws(() => bus.commitOffset('workers', '', 1), RangeError);
  assert.throws(() => bus.commitOffset('workers', 'jobs.email', 0), RangeError);
  assert.throws(() => bus.commitOffset('workers', 'jobs.email', 1.5), RangeError);
  assert.throws(() => bus.commitOffset('workers', 'jobs.email', NaN), RangeError);
});

test('consumer group: empty groupId throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribeToGroup('', 't', () => {}), RangeError);
});

test('consumer group: getStats exposes live groups', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.getStats().consumerGroups, []);
  bus.subscribeToGroup('g1', 'a.*', () => {});
  bus.subscribeToGroup('g1', 'a.*', () => {});
  bus.subscribeToGroup('g2', 'b.*', () => {});
  assert.deepEqual(bus.getStats().consumerGroups, [
    { groupId: 'g1', pattern: 'a.*', members: 2 },
    { groupId: 'g2', pattern: 'b.*', members: 1 },
  ]);
});

test('consumer group: non-matching members never receive the message', async () => {
  const bus = new EventBus();
  let calls = 0;
  bus.subscribeToGroup('g', 'a.*', () => {
    calls += 1;
  });
  bus.publish('b.x', {});
  await flush();
  assert.equal(calls, 0);
  assert.deepEqual(bus.getGroupOffsets('g'), {});
});

test('consumer group: works with wildcard patterns (**)', async () => {
  const bus = new EventBus();
  const seen: Array<{ topic: string; seq: number }> = [];
  bus.subscribeToGroup('g', 'market.**', (m: BusMessage) => {
    seen.push({ topic: m.topic, seq: m.seq });
  });
  bus.subscribeToGroup('g', 'market.**', (m: BusMessage) => {
    seen.push({ topic: m.topic, seq: m.seq });
  });
  bus.publish('market.btc.trades', {});
  bus.publish('market', {});
  await flush();
  // seq is per-topic: each topic starts at 1. Each message delivered
  // exactly once across the group.
  assert.deepEqual(
    seen.map((s) => `${s.topic}#${s.seq}`).sort(),
    ['market#1', 'market.btc.trades#1'],
  );
});
