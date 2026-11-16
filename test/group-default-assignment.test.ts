import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type PartitionAssignor } from '../src/bus.ts';

const noop = (): void => {};

/** Counts partitions per owner from the live assignment table. */
function ownerCounts(bus: EventBus, groupId: string, pattern: string): number[] {
  const counts = new Map<string, number>();
  for (const owner of Object.values(bus.getPartitionAssignment(groupId, pattern))) {
    counts.set(owner, (counts.get(owner) ?? 0) + 1);
  }
  return [...counts.values()].sort((a, b) => a - b);
}

/** Partitions owned by one member, as a set. */
function ownedPartitions(bus: EventBus, groupId: string, pattern: string, memberId: string): Set<number> {
  const out = new Set<number>();
  for (const [partition, owner] of Object.entries(bus.getPartitionAssignment(groupId, pattern))) {
    if (owner === memberId) out.add(Number(partition));
  }
  return out;
}

/** Assignor that pins every partition to the first roster member. */
const allToFirst: PartitionAssignor = {
  name: 'all-first',
  assign(partitions: number, members: string[]): Map<number, string> {
    const out = new Map<number, string>();
    for (let p = 0; p < partitions; p++) out.set(p, members[0]);
    return out;
  },
};

// EB-523: bus-level live default for consumer-group partition assignment.

test('EB-523: the factory bus default is rendezvous', () => {
  const bus = new EventBus();
  assert.equal(bus.getGroupDefaultAssignment(), 'rendezvous');
});

test('EB-523: a new group whose first member omits assignment takes the sticky bus default (balanced, minimal migration)', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('sticky');
  const m1 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 6 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
  assert.deepEqual(ownerCounts(bus, 'g', 't.*'), [6]);
  const before = ownedPartitions(bus, 'g', 't.*', m1.id);
  assert.equal(before.size, 6);

  const m2 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 6 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
  // Sticky balances floor/ceil: 6 partitions over 2 members is 3/3.
  assert.deepEqual(ownerCounts(bus, 'g', 't.*'), [3, 3]);
  // Minimal migration: the first member kept exactly 3 of its 6
  // partitions — only the 3 the balance required actually moved.
  const kept = ownedPartitions(bus, 'g', 't.*', m1.id);
  assert.equal(kept.size, 3);
  for (const p of kept) assert.ok(before.has(p));
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-523: a member-explicit assignment beats the bus default', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('sticky');
  const m1 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4, assignment: 'rendezvous' });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
  const m2 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-523: an explicit custom assignor beats the bus default', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('sticky');
  const m1 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4, assignor: allToFirst });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'all-first');
  assert.deepEqual(ownerCounts(bus, 'g', 't.*'), [4]);
  m1.unsubscribe();
});

test("EB-523: set('rendezvous') restores the factory default for new groups", () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('sticky');
  bus.setGroupDefaultAssignment('rendezvous');
  assert.equal(bus.getGroupDefaultAssignment(), 'rendezvous');
  const m1 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
  m1.unsubscribe();
});

test('EB-523: a same-value set is a no-op with no audit (including re-setting rendezvous)', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('rendezvous');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_assignment_revised' }).length,
    0,
    're-affirming the factory default audits nothing',
  );
  bus.setGroupDefaultAssignment('sticky');
  bus.setGroupDefaultAssignment('sticky');
  const audit = bus.getSubscriptionAudit({ action: 'group_default_assignment_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: 'rendezvous', assignment: 'sticky' });
});

test('EB-523: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('sticky');
  for (const bad of ['random', '', null, 0, 123, {}, true, ['sticky']] as unknown[]) {
    assert.throws(
      () => bus.setGroupDefaultAssignment(bad as 'rendezvous' | 'sticky'),
      RangeError,
      `assignment=${String(bad)}`,
    );
    assert.equal(bus.getGroupDefaultAssignment(), 'sticky');
  }
  assert.throws(() => bus.setGroupDefaultAssignment('rendezvous', ''), RangeError);
  assert.equal(bus.getGroupDefaultAssignment(), 'sticky');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_assignment_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-523: revising the bus default never moves an already-baked group assignor', () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
  bus.setGroupDefaultAssignment('sticky');
  assert.equal(bus.getGroupDefaultAssignment(), 'sticky');
  // A member joining after the revision still inherits the group's
  // baked rendezvous assignor.
  const m2 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-523: a member explicitly naming a different strategy than the group (default-derived) still throws RangeError', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('sticky');
  const m1 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
  assert.throws(
    () => bus.subscribeToGroup('g', 't.*', noop, { partitions: 4, assignment: 'rendezvous' }),
    RangeError,
  );
  // The group is untouched, and a member that omits the strategy
  // still inherits the group's sticky assignor.
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
  const m2 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-523: setGroupAssignor(groupId, null) restores the live bus default', () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
  // With the default revised to sticky, null restores sticky.
  bus.setGroupDefaultAssignment('sticky');
  bus.setGroupAssignor('g', null, 'ops');
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
  // With the default back at the factory value, null restores rendezvous.
  bus.setGroupDefaultAssignment('rendezvous');
  bus.setGroupAssignor('g', null, 'ops');
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
  const audit = bus.getSubscriptionAudit({ action: 'group_assignor_revised' });
  assert.equal(audit.length, 2);
  assert.equal(audit[0].actor, 'ops');
  m1.unsubscribe();
});

test('EB-523: a drained-and-rebuilt group takes the default live at rebuild time', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('sticky');
  const m1 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
  m1.unsubscribe();

  // The group drained fully; the default moved meanwhile.
  bus.setGroupDefaultAssignment('rendezvous');
  const m2 = bus.subscribeToGroup('g', 't.*', noop, { partitions: 4 });
  assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
  m2.unsubscribe();
});

test('EB-523: each revision audits its old and new value and actor; the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAssignment('sticky', 'ops');
  assert.equal(bus.getGroupDefaultAssignment(), 'sticky');
  bus.setGroupDefaultAssignment('rendezvous', 'ops');
  assert.equal(bus.getGroupDefaultAssignment(), 'rendezvous');
  const events = bus.getSubscriptionAudit({ action: 'group_default_assignment_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: 'rendezvous', assignment: 'sticky' });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 'sticky', assignment: 'rendezvous' });
});
