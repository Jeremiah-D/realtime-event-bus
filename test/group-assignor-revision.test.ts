import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type GroupRebalanceEvent, type PartitionAssignor } from '../src/bus.ts';
import { createWeightedPartitionAssignor, stickyPartitionAssignor } from '../src/assignor.ts';

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

function makeBusWithLog(): { bus: EventBus; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), 'eb-assignor-'));
  return { bus: new EventBus({ durableLogDir: dir }), dir };
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

/** Assignor that pins every partition to the last roster member. */
const allToSecond: PartitionAssignor = {
  name: 'all-second',
  assign(partitions: number, members: string[]): Map<number, string> {
    const out = new Map<number, string>();
    // Last member, not members[1]: the roster shrinks as members leave,
    // and an assignor that names an absent member fails the bus's
    // fail-fast validation mid-leave.
    for (let p = 0; p < partitions; p++) out.set(p, members[members.length - 1]);
    return out;
  },
};

describe('consumer-group partition assignor live switch (EB-346)', () => {
  it('rejects invalid input fail-fast and leaves the live assignor untouched', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    assert.throws(() => bus.setGroupAssignor('', 'sticky'), RangeError);
    assert.throws(() => bus.setGroupAssignor('g', 'sticky', ''), RangeError);
    assert.throws(() => bus.setGroupAssignor('g', 'zone-aware' as never), RangeError);
    assert.throws(() => bus.setGroupAssignor('g', 'weighted' as never), RangeError);
    assert.throws(() => bus.setGroupAssignor('g', 'lag-aware' as never), RangeError);
    assert.throws(() => bus.setGroupAssignor('g', '' as never), RangeError);
    assert.throws(
      () => bus.setGroupAssignor('g', { name: '', assign: () => new Map() } as never),
      RangeError,
    );
    assert.throws(
      () => bus.setGroupAssignor('g', { name: 'x' } as never),
      RangeError,
    );
    // Unknown group and round-robin (non-partitioned) group.
    assert.throws(() => bus.setGroupAssignor('nope', 'sticky'), RangeError);
    const r = bus.subscribeToGroup('rr', 't.*', () => {});
    assert.throws(() => bus.setGroupAssignor('rr', 'sticky'), RangeError);
    // Nothing changed: the group still runs rendezvous, with no audit.
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
    assert.equal(bus.getSubscriptionAudit({ action: 'group_assignor_revised' }).length, 0);
    m1.unsubscribe();
    m2.unsubscribe();
    r.unsubscribe();
  });

  it('rejects an ambiguous groupId with several partitioned competing sets', () => {
    const bus = new EventBus();
    const a = bus.subscribeToGroup('g', 't.a', () => {}, { partitions: 2 });
    const b = bus.subscribeToGroup('g', 't.b', () => {}, { partitions: 2 });
    assert.throws(() => bus.setGroupAssignor('g', 'sticky'), RangeError);
    a.unsubscribe();
    b.unsubscribe();
  });

  it('switches rendezvous to sticky: balanced placement, assignor rebalance, audit', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 4,
      onRebalance: (e) => events.push(e),
    });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 4,
      onRebalance: (e) => events.push(e),
    });
    events.length = 0;
    bus.setGroupAssignor('g', 'sticky', 'ops');
    const after = bus.getPartitionAssignment('g', 't.*');
    // Sticky balances: each of the two members owns exactly two partitions.
    const counts = new Map<string, number>();
    for (const owner of Object.values(after)) counts.set(owner, (counts.get(owner) ?? 0) + 1);
    assert.deepEqual([...counts.values()].sort(), [2, 2]);
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
    // One operator rebalance per member, with the assignor trigger.
    assert.equal(events.length, 2);
    for (const e of events) {
      assert.equal(e.trigger, 'assignor');
      assert.equal(e.memberId, '');
      assert.equal(e.partitionRebalance?.strategy, 'sticky');
      assert.deepEqual(e.partitionRebalance?.assignment, after);
    }
    const audit = bus.getSubscriptionAudit({ action: 'group_assignor_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].groupId, 'g');
    assert.equal(audit[0].pattern, 't.*');
    assert.equal(audit[0].actor, 'ops');
    assert.deepEqual(audit[0].detail, { assignor: 'sticky', oldAssignor: 'rendezvous' });
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('restores the default rendezvous with null, and accepts the sticky builtin object', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    bus.setGroupAssignor('g', stickyPartitionAssignor);
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'sticky');
    bus.setGroupAssignor('g', null);
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
    const audit = bus.getSubscriptionAudit({ action: 'group_assignor_revised' });
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[1].detail, { assignor: 'rendezvous', oldAssignor: 'sticky' });
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('switching to the running assignor is a no-op with no audit and no rebalance', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 4,
      onRebalance: (e) => events.push(e),
    });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    events.length = 0;
    bus.setGroupAssignor('g', 'rendezvous');
    bus.setGroupAssignor('g', null);
    assert.equal(events.length, 0);
    assert.equal(bus.getSubscriptionAudit({ action: 'group_assignor_revised' }).length, 0);
    // Same name by object: a custom assignor sharing the running name is the same strategy.
    bus.setGroupAssignor('g', allToFirst);
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'all-first');
    events.length = 0;
    bus.setGroupAssignor('g', { name: 'all-first', assign: () => new Map() });
    assert.equal(events.length, 0);
    assert.equal(bus.getSubscriptionAudit({ action: 'group_assignor_revised' }).length, 1);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('switches to a custom assignor and the fixed assignor binds later joins', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 3, assignor: allToFirst });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 3 });
    assert.deepEqual(Object.values(bus.getPartitionAssignment('g', 't.*')), [m1.id, m1.id, m1.id]);
    bus.setGroupAssignor('g', allToSecond);
    assert.deepEqual(Object.values(bus.getPartitionAssignment('g', 't.*')), [m2.id, m2.id, m2.id]);
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'all-second');
    // A later joiner that names a different assignor is refused.
    assert.throws(
      () => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 3, assignment: 'sticky' }),
      RangeError,
    );
    assert.throws(
      () => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 3, assignor: allToFirst }),
      RangeError,
    );
    // A later joiner that names the running assignor (or none) joins freely.
    const m3 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 3, assignor: allToSecond });
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'all-second');
    m1.unsubscribe();
    m2.unsubscribe();
    m3.unsubscribe();
  });

  it('trial-validates a custom assignor: throwing or invalid, the old assignor stays', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const throwing: PartitionAssignor = {
      name: 'throws',
      assign(): Map<number, string> {
        throw new Error('boom');
      },
    };
    assert.throws(() => bus.setGroupAssignor('g', throwing), /boom/);
    const empty: PartitionAssignor = {
      name: 'empty',
      assign: () => new Map(),
    };
    assert.throws(() => bus.setGroupAssignor('g', empty), RangeError);
    const wrongOwner: PartitionAssignor = {
      name: 'wrong-owner',
      assign(partitions: number): Map<number, string> {
        const out = new Map<number, string>();
        for (let p = 0; p < partitions; p++) out.set(p, 'ghost');
        return out;
      },
    };
    assert.throws(() => bus.setGroupAssignor('g', wrongOwner), RangeError);
    // The live assignor, the assignment, and the audit log are untouched.
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
    assert.equal(bus.getSubscriptionAudit({ action: 'group_assignor_revised' }).length, 0);
    assert.equal(bus.getSubscriptionAudit({ action: 'rebalance' }).length, 2);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('accepts a factory-built weighted assignor object', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    bus.setGroupAssignor('g', createWeightedPartitionAssignor());
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'weighted');
    const audit = bus.getSubscriptionAudit({ action: 'group_assignor_revised' });
    assert.deepEqual(audit[0].detail, { assignor: 'weighted', oldAssignor: 'rendezvous' });
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('a fully drained group records the switch by audit alone; the next generation re-fixes', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 4,
      onRebalance: (e) => events.push(e),
    });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    m1.unsubscribe();
    m2.unsubscribe();
    events.length = 0;
    bus.setGroupAssignor('g', 'sticky');
    // No members to notify: no rebalance event, but the switch is audited.
    assert.equal(events.length, 0);
    const audit = bus.getSubscriptionAudit({ action: 'group_assignor_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, { assignor: 'sticky', oldAssignor: 'rendezvous' });
    // The next generation's first member re-fixes the assignor (the
    // EB-106 first-member-fixes rule): joining with nothing specified
    // resets it to rendezvous, and the drained switch does not bind
    // the new generation — it only recorded the operator's intent.
    const n1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const n2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    assert.equal(bus.getGroupAssignment('g', 't.*').strategy, 'rendezvous');
    assert.throws(
      () => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' }),
      RangeError,
    );
    n1.unsubscribe();
    n2.unsubscribe();
  });

  it('migrated partitions replay their uncommitted backlog to the new owner', async () => {
    const { bus } = makeBusWithLog();
    const seenA: number[] = [];
    const seenB: number[] = [];
    const m1 = bus.subscribeToGroup(
      'g',
      't',
      (msg) => {
        seenA.push((msg.payload as { i: number }).i);
      },
      { partitions: 2, assignor: allToFirst },
    );
    const m2 = bus.subscribeToGroup(
      'g',
      't',
      (msg) => {
        seenB.push((msg.payload as { i: number }).i);
      },
      { partitions: 2 },
    );
    bus.publish('t', { i: 1 });
    bus.publish('t', { i: 2 });
    bus.publish('t', { i: 3 });
    await waitFor(() => seenA.length === 3, 5000, 'owner A to receive all three live');
    // Commit only the first message: seq 2 and 3 stay uncommitted.
    bus.commitOffset('g', 't', 1);
    bus.setGroupAssignor('g', allToSecond);
    await waitFor(() => seenB.length === 2, 5000, 'new owner B to replay the uncommitted backlog');
    // The two uncommitted messages arrive (one per migrated partition,
    // so the order across partitions is not pinned — only the set is).
    assert.deepEqual([...seenB].sort(), [2, 3]);
    // The committed message is not replayed, and A keeps its live deliveries.
    assert.deepEqual(seenA, [1, 2, 3]);
    m1.unsubscribe();
    m2.unsubscribe();
  });
});
