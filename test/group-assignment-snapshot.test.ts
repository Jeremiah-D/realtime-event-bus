import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';
import type { PartitionAssignor } from '../src/assignor.ts';

/** A custom assignor that pins every partition to the first roster member. */
const pinFirst: PartitionAssignor = {
  name: 'pin-first',
  assign(partitions: number, members: string[]): Map<number, string> {
    const out = new Map<number, string>();
    for (let p = 0; p < partitions; p++) out.set(p, members[0]);
    return out;
  },
};

test('getGroupAssignment snapshots a partitioned group without disturbing it', () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky', onRebalance });
  const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky', onRebalance });
  const snapshot = bus.getGroupAssignment('g', 't.*');
  assert.equal(snapshot.groupId, 'g');
  assert.equal(snapshot.pattern, 't.*');
  assert.equal(snapshot.strategy, 'sticky');
  assert.deepEqual(snapshot.members, [m1.id, m2.id]);
  assert.deepEqual(
    snapshot.assignments.map((a) => a.partition),
    [0, 1, 2, 3, 4, 5],
  );
  assert.ok(snapshot.assignments.every((a) => a.memberId === m1.id || a.memberId === m2.id));
  // Sticky balance: 3 partitions per member, matching the live placement.
  const loads = new Map<string, number>();
  for (const a of snapshot.assignments) loads.set(a.memberId, (loads.get(a.memberId) ?? 0) + 1);
  assert.deepEqual([...loads.values()].sort(), [3, 3]);
  const live = bus.getPartitionAssignment('g', 't.*');
  assert.deepEqual(
    Object.fromEntries(snapshot.assignments.map((a) => [a.partition, a.memberId])),
    live,
  );
  // Read-only: a second snapshot is identical and no rebalance fired.
  const eventsBefore = events.length;
  assert.deepEqual(bus.getGroupAssignment('g', 't.*'), snapshot);
  assert.equal(events.length, eventsBefore, 'snapshotting must not fire onRebalance');
  // A later join is still a real rebalance — snapshotting never froze it.
  const m3 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky' });
  assert.deepEqual(bus.getGroupAssignment('g', 't.*').members, [m1.id, m2.id, m3.id]);
  assert.equal(bus.getGroupAssignment('g', 't.*').assignments.length, 6);
  m1.unsubscribe();
  m2.unsubscribe();
  m3.unsubscribe();
});

test('getGroupAssignment resolves a single-set group without a pattern and reports a custom strategy', () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignor: pinFirst });
  const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignor: pinFirst });
  const snapshot = bus.getGroupAssignment('g');
  assert.equal(snapshot.pattern, 't.*');
  assert.equal(snapshot.strategy, 'pin-first');
  assert.deepEqual(snapshot.members, [m1.id, m2.id]);
  assert.ok(snapshot.assignments.every((a) => a.memberId === m1.id), 'pin-first owns every partition');
  // The returned snapshot is detached: mutating it never affects the group.
  snapshot.members.push('intruder');
  snapshot.assignments.push({ partition: 99, memberId: 'intruder' });
  const reread = bus.getGroupAssignment('g', 't.*');
  assert.deepEqual(reread.members, [m1.id, m2.id]);
  assert.equal(reread.assignments.length, 4);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('getGroupAssignment reports an empty assignment for a classic round-robin group', () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  const m2 = bus.subscribeToGroup('g', 't', () => {});
  const snapshot = bus.getGroupAssignment('g', 't');
  assert.equal(snapshot.strategy, 'rendezvous');
  assert.deepEqual(snapshot.members, [m1.id, m2.id]);
  assert.deepEqual(snapshot.assignments, []);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('getGroupAssignment fails closed on unknown, empty, and ambiguous groups', () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't.a', () => {});
  const m2 = bus.subscribeToGroup('g', 't.b', () => {});
  assert.throws(() => bus.getGroupAssignment('no-such-group'), RangeError);
  assert.throws(() => bus.getGroupAssignment('g', 't.c'), RangeError);
  assert.throws(() => bus.getGroupAssignment(''), RangeError);
  assert.throws(() => bus.getGroupAssignment('g', ''), RangeError);
  // Same groupId on two patterns: the caller must name the set.
  assert.throws(() => bus.getGroupAssignment('g'), RangeError);
  assert.equal(bus.getGroupAssignment('g', 't.a').members.length, 1);
  assert.equal(bus.getGroupAssignment('g', 't.b').members.length, 1);
  // A drained group is unknown again: the last member leaving deletes the set.
  m1.unsubscribe();
  assert.throws(() => bus.getGroupAssignment('g', 't.a'), RangeError);
  assert.equal(bus.getGroupAssignment('g').pattern, 't.b');
  m2.unsubscribe();
  assert.throws(() => bus.getGroupAssignment('g'), RangeError);
});
