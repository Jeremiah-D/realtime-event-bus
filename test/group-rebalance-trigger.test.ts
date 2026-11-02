import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type GroupRebalanceEvent, type PartitionAssignor } from '../src/bus.ts';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Polls until `cond()` holds or the deadline passes; throws on timeout. */
async function waitFor(cond: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (cond()) return;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for: ${label}`);
    await sleep(10);
  }
}

/** Deterministic round-robin assignor: partition p goes to members[p % n]. */
const roundRobin: PartitionAssignor = {
  name: 'round-robin-test',
  assign(partitions: number, members: string[]): Map<number, string> {
    const out = new Map<number, string>();
    for (let p = 0; p < partitions; p++) out.set(p, members[p % members.length] as string);
    return out;
  },
};

describe('manual consumer-group rebalance trigger (EB-351)', () => {
  it('rejects invalid input fail-fast and leaves the live group and audit untouched', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2 });
    assert.throws(() => bus.triggerGroupRebalance(''), RangeError);
    assert.throws(() => bus.triggerGroupRebalance('g', ''), RangeError);
    assert.throws(() => bus.triggerGroupRebalance('g', 't.*', ''), RangeError);
    assert.throws(() => bus.triggerGroupRebalance('nope', 't.*'), RangeError);
    assert.throws(() => bus.triggerGroupRebalance('g', 'other.*'), RangeError);
    assert.throws(() => bus.triggerGroupRebalance('nope'), RangeError);
    // Nothing changed: the group still runs, with no trigger audit.
    assert.equal(bus.getGroupAssignment('g', 't.*').members.length, 1);
    assert.equal(bus.getSubscriptionAudit({ action: 'group_rebalance_triggered' }).length, 0);
    m1.unsubscribe();
  });

  it('rejects an ambiguous groupId when the pattern is omitted', () => {
    const bus = new EventBus();
    const a = bus.subscribeToGroup('g', 't.a', () => {});
    const b = bus.subscribeToGroup('g', 't.b', () => {});
    assert.throws(() => bus.triggerGroupRebalance('g'), RangeError);
    // With a pattern the same groupId resolves fine.
    const result = bus.triggerGroupRebalance('g', 't.a');
    assert.equal(result.triggered, true);
    assert.equal(result.pattern, 't.a');
    a.unsubscribe();
    b.unsubscribe();
  });

  it('resolves exactly one group when the pattern is omitted', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('solo', 't.*', () => {}, {
      onRebalance: (e) => events.push(e),
    });
    events.length = 0;
    const result = bus.triggerGroupRebalance('solo');
    assert.deepEqual(result, {
      groupId: 'solo',
      pattern: 't.*',
      members: 1,
      triggered: true,
    });
    assert.equal(events.length, 1);
    assert.equal(events[0]?.trigger, 'operator');
    assert.equal(events[0]?.memberId, '');
    assert.deepEqual(events[0]?.members, [m1.id]);
    m1.unsubscribe();
  });

  it('fires an operator rebalance on a round-robin group and audits the trigger', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      onRebalance: (e) => events.push(e),
    });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, {
      onRebalance: (e) => events.push(e),
    });
    events.length = 0;
    const result = bus.triggerGroupRebalance('g', 't.*', 'ops');
    assert.deepEqual(result, { groupId: 'g', pattern: 't.*', members: 2, triggered: true });
    // One operator rebalance per member, fired synchronously.
    assert.equal(events.length, 2);
    for (const e of events) {
      assert.equal(e.trigger, 'operator');
      assert.equal(e.memberId, '');
      assert.deepEqual(e.members, [m1.id, m2.id]);
      assert.equal(e.partitionRebalance, undefined);
      assert.equal(e.suppressedRebalances, undefined);
    }
    const triggerAudit = bus.getSubscriptionAudit({ action: 'group_rebalance_triggered' });
    assert.equal(triggerAudit.length, 1);
    assert.equal(triggerAudit[0]?.groupId, 'g');
    assert.equal(triggerAudit[0]?.pattern, 't.*');
    assert.equal(triggerAudit[0]?.actor, 'ops');
    assert.deepEqual(triggerAudit[0]?.detail, { trigger: 'operator', members: 2 });
    const rebalanceAudit = bus
      .getSubscriptionAudit({ action: 'rebalance' })
      .filter((e) => e.detail?.trigger === 'operator');
    assert.equal(rebalanceAudit.length, 1);
    assert.equal(rebalanceAudit[0]?.detail?.trigger, 'operator');
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('partitioned group in steady state: assignment reported, no migration, listeners silent', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const revoked: Array<{ member: string; partitions: number[] }> = [];
    const assigned: Array<{ member: string; partitions: number[] }> = [];
    const listener = {
      onPartitionsRevoked: (member: string, partitions: number[]) =>
        revoked.push({ member, partitions: [...partitions] }),
      onPartitionsAssigned: (member: string, partitions: number[]) =>
        assigned.push({ member, partitions: [...partitions] }),
    };
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 4,
      onRebalance: (e) => events.push(e),
      rebalanceListener: listener,
    });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 4,
      onRebalance: (e) => events.push(e),
      rebalanceListener: listener,
    });
    // Joins already notified the listeners; the trigger must not repeat them.
    events.length = 0;
    revoked.length = 0;
    assigned.length = 0;
    const live = bus.getPartitionAssignment('g', 't.*');
    const result = bus.triggerGroupRebalance('g', 't.*');
    assert.equal(result.triggered, true);
    assert.equal(result.members, 2);
    assert.equal(result.partitionRebalance?.strategy, 'rendezvous');
    assert.deepEqual(result.partitionRebalance?.assignment, live);
    assert.deepEqual(result.partitionRebalance?.migrated, []);
    assert.equal(events.length, 2);
    for (const e of events) {
      assert.equal(e.trigger, 'operator');
      assert.equal(e.memberId, '');
      assert.deepEqual(e.partitionRebalance?.assignment, live);
      assert.deepEqual(e.partitionRebalance?.migrated, []);
      assert.equal(e.partitionRebalance?.strategy, 'rendezvous');
    }
    // Steady state: no partition changed hands, so no listener fires.
    assert.deepEqual(revoked, []);
    assert.deepEqual(assigned, []);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('debounced group: pending migration surfaces through revoke/assign immediately', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const revoked: Array<{ member: string; partitions: number[] }> = [];
    const assigned: Array<{ member: string; partitions: number[] }> = [];
    const listener = {
      onPartitionsRevoked: (member: string, partitions: number[]) =>
        revoked.push({ member, partitions: [...partitions] }),
      onPartitionsAssigned: (member: string, partitions: number[]) =>
        assigned.push({ member, partitions: [...partitions] }),
    };
    const opts = {
      partitions: 6,
      rebalanceDebounceMs: 100,
      assignor: roundRobin,
      onRebalance: (e: GroupRebalanceEvent) => events.push(e),
      rebalanceListener: listener,
    };
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, opts);
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, opts);
    // Both joins are absorbed into the debounce window; the merged event
    // fires at window end with the 2-way assignment.
    await waitFor(() => events.length === 2, 5000, 'debounced join flush');
    assert.equal(events[0]?.trigger, 'join');
    assert.equal(events[0]?.suppressedRebalances, 1);
    events.length = 0;
    revoked.length = 0;
    assigned.length = 0;
    // A third member joins: absorbed again, and the live assignment
    // moves eagerly to the 3-way split before any event fires.
    const m3 = bus.subscribeToGroup('g', 't.*', () => {}, opts);
    assert.equal(events.length, 0);
    // The trigger fires synchronously — no waiting for the window — and
    // reports the net migration since the last emission.
    const result = bus.triggerGroupRebalance('g', 't.*', 'ops');
    assert.equal(result.triggered, true);
    assert.equal(events.length, 3);
    for (const e of events) {
      assert.equal(e.trigger, 'operator');
      assert.equal(e.memberId, '');
      assert.equal(e.suppressedRebalances, undefined);
    }
    const expectedAssignment: Record<number, string> = {
      0: m1.id,
      1: m2.id,
      2: m3.id,
      3: m1.id,
      4: m2.id,
      5: m3.id,
    };
    assert.deepEqual(result.partitionRebalance?.assignment, expectedAssignment);
    // 2-way [0,2,4]/[1,3,5] -> 3-way: p2 m1->m3, p3 m2->m1, p4 m1->m2, p5 m2->m3.
    // Nothing was published, so every migration's watermarks are empty.
    assert.deepEqual(result.partitionRebalance?.migrated, [
      { partition: 2, from: m1.id, to: m3.id, watermarks: {} },
      { partition: 3, from: m2.id, to: m1.id, watermarks: {} },
      { partition: 4, from: m1.id, to: m2.id, watermarks: {} },
      { partition: 5, from: m2.id, to: m3.id, watermarks: {} },
    ]);
    // Listeners see revoke-before-assign in the expand-path shape.
    assert.deepEqual(revoked, [
      { member: m1.id, partitions: [2, 4] },
      { member: m2.id, partitions: [3, 5] },
    ]);
    assert.deepEqual(assigned, [
      { member: m1.id, partitions: [3] },
      { member: m2.id, partitions: [4] },
      { member: m3.id, partitions: [2, 5] },
    ]);
    const triggerAudit = bus.getSubscriptionAudit({ action: 'group_rebalance_triggered' });
    assert.equal(triggerAudit.length, 1);
    assert.equal(triggerAudit[0]?.actor, 'ops');
    m1.unsubscribe();
    m2.unsubscribe();
    m3.unsubscribe();
  });

  it('drained partitioned group: audited no-op, nothing fires', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 2,
      onRebalance: (e) => events.push(e),
    });
    m1.unsubscribe();
    events.length = 0;
    const result = bus.triggerGroupRebalance('g', 't.*', 'ops');
    assert.deepEqual(result, { groupId: 'g', pattern: 't.*', members: 0, triggered: false });
    assert.equal(events.length, 0);
    const triggerAudit = bus.getSubscriptionAudit({ action: 'group_rebalance_triggered' });
    assert.equal(triggerAudit.length, 1);
    assert.equal(triggerAudit[0]?.actor, 'ops');
    assert.deepEqual(triggerAudit[0]?.detail, { trigger: 'operator', members: 0 });
    // No rebalance was recorded for the operator trigger.
    const operatorRebalances = bus
      .getSubscriptionAudit({ action: 'rebalance' })
      .filter((e) => e.detail?.trigger === 'operator');
    assert.equal(operatorRebalances.length, 0);
  });

  it('drained round-robin group reads as unknown', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {});
    m1.unsubscribe();
    assert.throws(() => bus.triggerGroupRebalance('g', 't.*'), RangeError);
    assert.throws(() => bus.triggerGroupRebalance('g'), RangeError);
  });
});
