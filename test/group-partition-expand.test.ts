import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage, type GroupRebalanceEvent } from '../src/bus.ts';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeBusWithLog(): { bus: EventBus; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), 'eb-expand-'));
  return { bus: new EventBus({ durableLogDir: dir }), dir };
}

describe('consumer-group partition expansion (EB-210)', () => {
  it('rejects invalid input fail-fast', () => {
    const bus = new EventBus();
    const m = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    assert.throws(() => bus.setGroupPartitionCount('', 8), RangeError);
    assert.throws(() => bus.setGroupPartitionCount('g', 0), RangeError);
    assert.throws(() => bus.setGroupPartitionCount('g', -2), RangeError);
    assert.throws(() => bus.setGroupPartitionCount('g', 2.5), RangeError);
    // Shrink and no-op are rejected: growth only.
    assert.throws(() => bus.setGroupPartitionCount('g', 2), RangeError);
    assert.throws(() => bus.setGroupPartitionCount('g', 4), RangeError);
    // Unknown group and round-robin (non-partitioned) group.
    assert.throws(() => bus.setGroupPartitionCount('nope', 8), RangeError);
    const r = bus.subscribeToGroup('rr', 't.*', () => {});
    assert.throws(() => bus.setGroupPartitionCount('rr', 8), RangeError);
    // Nothing changed: the count is still 4.
    assert.equal(Object.keys(bus.getPartitionAssignment('g', 't.*')).length, 4);
    m.unsubscribe();
    r.unsubscribe();
  });

  it('rejects an ambiguous groupId with several partitioned competing sets', () => {
    const bus = new EventBus();
    const a = bus.subscribeToGroup('g', 't.a', () => {}, { partitions: 2 });
    const b = bus.subscribeToGroup('g', 't.b', () => {}, { partitions: 2 });
    assert.throws(() => bus.setGroupPartitionCount('g', 4), RangeError);
    a.unsubscribe();
    b.unsubscribe();
  });

  it('expands a rendezvous group: old partitions keep owners, only new partitions migrate', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const before = bus.getPartitionAssignment('g', 't.*');
    const result = bus.setGroupPartitionCount('g', 7);
    assert.equal(result.partitionsBefore, 4);
    assert.equal(result.partitionsAfter, 7);
    assert.equal(result.groupId, 'g');
    assert.equal(result.pattern, 't.*');
    const after = bus.getPartitionAssignment('g', 't.*');
    assert.equal(Object.keys(after).length, 7);
    // Old partitions never move under rendezvous.
    for (let p = 0; p < 4; p++) assert.equal(after[p], before[p], `partition ${p} kept its owner`);
    // The migration list is exactly the new partitions, from null.
    assert.deepEqual(
      result.migrated.map((m) => m.partition),
      [4, 5, 6],
    );
    for (const m of result.migrated) {
      assert.equal(m.from, null);
      assert.ok([m1.id, m2.id].includes(m.to));
      assert.deepEqual(m.watermarks, {});
    }
    assert.deepEqual(result.assignment, after);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('expands a sticky group keeping balance and old owners', () => {
    const bus = new EventBus();
    const members = [
      bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky' }),
      bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky' }),
      bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky' }),
    ];
    const before = bus.getPartitionAssignment('g', 't.*');
    const result = bus.setGroupPartitionCount('g', 9);
    const after = bus.getPartitionAssignment('g', 't.*');
    // Sticky: 6 -> 9 over 3 members keeps every old owner, 3/3/3 balanced.
    for (let p = 0; p < 6; p++) assert.equal(after[p], before[p], `partition ${p} kept its owner`);
    const loads = new Map<string, number>();
    for (const owner of Object.values(after)) loads.set(owner, (loads.get(owner) ?? 0) + 1);
    assert.deepEqual([...loads.values()].sort(), [3, 3, 3]);
    assert.equal(result.migrated.length, 3);
    for (const m of result.migrated) assert.equal(m.from, null);
    for (const m of members) m.unsubscribe();
  });

  it('fires an operator rebalance with trigger expand and memberId empty', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 2,
      onRebalance: (e) => events.push(e),
    });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 2,
      onRebalance: (e) => events.push(e),
    });
    events.length = 0;
    bus.setGroupPartitionCount('g', 4);
    const expand = events.filter((e) => e.trigger === 'expand');
    // Both members observe the operator rebalance.
    assert.equal(expand.length, 2);
    for (const e of expand) {
      assert.equal(e.memberId, '');
      assert.equal(e.groupId, 'g');
      assert.ok(e.partitionRebalance !== undefined);
      assert.equal(e.partitionRebalance.strategy, 'rendezvous');
      assert.deepEqual(
        e.partitionRebalance.migrated.map((m) => m.partition),
        [2, 3],
      );
      assert.equal(Object.keys(e.partitionRebalance.assignment).length, 4);
    }
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('fires rebalance-listener assigned callbacks for the new partitions', () => {
    const bus = new EventBus();
    const assigned: Array<{ member: string; partitions: number[] }> = [];
    const revoked: Array<{ member: string; partitions: number[] }> = [];
    const listener = {
      onPartitionsAssigned: (member: string, partitions: number[]) => assigned.push({ member, partitions }),
      onPartitionsRevoked: (member: string, partitions: number[]) => revoked.push({ member, partitions }),
    };
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 2,
      rebalanceListener: listener,
    });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 2,
      rebalanceListener: listener,
    });
    assigned.length = 0;
    revoked.length = 0;
    const result = bus.setGroupPartitionCount('g', 5);
    // New partitions are announced to their new owners (one batched
    // callback per member); nothing is revoked.
    assert.equal(revoked.length, 0);
    const announcedByMember = new Map<string, number[]>();
    for (const a of assigned) announcedByMember.set(a.member, a.partitions);
    const announcedAll = [...announcedByMember.values()].flat().sort((x, y) => x - y);
    assert.deepEqual(announcedAll, [2, 3, 4]);
    // Every new partition's owner was told exactly its new partitions.
    for (const [member, partitions] of announcedByMember) {
      for (const p of partitions) {
        const mig = result.migrated.find((m) => m.partition === p);
        assert.ok(mig !== undefined && mig.to === member && mig.from === null);
      }
    }
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('routes new traffic under the new count with no loss or duplication', async () => {
    const bus = new EventBus();
    const got: BusMessage[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', (m) => got.push(m), { partitions: 3 });
    const m2 = bus.subscribeToGroup('g', 't.*', (m) => got.push(m), { partitions: 3 });
    void m1;
    void m2;
    for (let i = 0; i < 30; i++) bus.publish('t.1', `pre-${i}`, { key: `k${i}` });
    await sleep(50);
    const preCount = got.length;
    assert.equal(preCount, 30);
    bus.setGroupPartitionCount('g', 6);
    const after = bus.getPartitionAssignment('g', 't.*');
    assert.equal(Object.keys(after).length, 6);
    for (let i = 0; i < 30; i++) bus.publish('t.1', `post-${i}`, { key: `k${i}` });
    await sleep(50);
    // Exactly 60 deliveries: no loss, no duplication across the expansion.
    assert.equal(got.length, 60);
    const payloads = got.map((m) => m.payload as string).sort();
    const expected = [...Array.from({ length: 30 }, (_, i) => `pre-${i}`), ...Array.from({ length: 30 }, (_, i) => `post-${i}`)].sort();
    assert.deepEqual(payloads, expected);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('preserves per-partition committed offsets across expansion', async () => {
    const { bus } = makeBusWithLog();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2 });
    for (let i = 0; i < 10; i++) bus.publish('t.1', i, { key: `k${i}` });
    await sleep(50);
    const assignment = bus.getPartitionAssignment('g', 't.*');
    const committedBefore = bus.getPartitionCommittedOffsets('g', 't.*');
    // Commit per-partition checkpoints for the old partitions.
    for (const p of [0, 1]) {
      const wm = bus.getPartitionWatermarks('g', 't.*')[p]?.['t.1'] ?? 0;
      if (wm > 0) bus.commitOffset('g', 't.1', wm, { partition: p });
    }
    const committed = bus.getPartitionCommittedOffsets('g', 't.*');
    bus.setGroupPartitionCount('g', 4);
    const committedAfter = bus.getPartitionCommittedOffsets('g', 't.*');
    // Old partitions keep their checkpoints; new partitions have none.
    for (const p of ['0', '1']) {
      assert.deepEqual(committedAfter[Number(p)]?.['t.1'] ?? 0, committed[Number(p)]?.['t.1'] ?? 0);
    }
    assert.equal(committedAfter[2], undefined);
    assert.equal(committedAfter[3], undefined);
    void assignment;
    void committedBefore;
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('new partitions start from the latest offset: no backlog replay', async () => {
    const { bus } = makeBusWithLog();
    const seen: string[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', (m) => seen.push(m.payload as string), { partitions: 2 });
    const m2 = bus.subscribeToGroup('g', 't.*', (m) => seen.push(m.payload as string), { partitions: 2 });
    for (let i = 0; i < 20; i++) bus.publish('t.1', `old-${i}`, { key: `k${i}` });
    await sleep(50);
    seen.length = 0;
    bus.setGroupPartitionCount('g', 4);
    await sleep(50);
    // No replay: the new partitions do not redeliver history.
    assert.equal(seen.length, 0);
    for (let i = 0; i < 20; i++) bus.publish('t.1', `new-${i}`, { key: `k${i}` });
    await sleep(50);
    assert.equal(seen.length, 20);
    assert.ok(seen.every((p) => p.startsWith('new-')));
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('a custom assignor moving an old partition gets an exact old-count handoff', async () => {
    const { bus } = makeBusWithLog();
    const movingAssignor = {
      name: 'test-moving',
      assign(partitions: number, members: string[], previous?: ReadonlyMap<number, string>): Map<number, string> {
        const out = new Map<number, string>();
        // Old partitions: rotate ownership by one slot; new partitions
        // go to the first member.
        for (let p = 0; p < partitions; p++) {
          const prevOwner = previous?.get(p);
          const prevIdx = prevOwner === undefined ? -1 : members.indexOf(prevOwner);
          out.set(p, members[(prevIdx + 1 + members.length) % members.length] ?? members[0]);
        }
        return out;
      },
    };
    const gotA: string[] = [];
    const gotB: string[] = [];
    const mA = bus.subscribeToGroup('g', 't.*', (m) => gotA.push(m.payload as string), {
      partitions: 2,
      assignor: movingAssignor,
    });
    const mB = bus.subscribeToGroup('g', 't.*', (m) => gotB.push(m.payload as string), {
      partitions: 2,
      assignor: movingAssignor,
    });
    // Keyed traffic so records hash deterministically under the old count.
    for (let i = 0; i < 12; i++) bus.publish('t.1', `m${i}`, { key: `kk${i}` });
    await sleep(50);
    assert.equal(gotA.length + gotB.length, 12);
    const before = bus.getPartitionAssignment('g', 't.*');
    const result = bus.setGroupPartitionCount('g', 3);
    // Old partitions moved (custom assignor rotates); new partition is from-null.
    const moved = result.migrated.filter((m) => m.from !== null);
    assert.equal(moved.length, 2);
    for (const m of moved) {
      assert.notEqual(m.from, m.to);
      assert.equal(m.from, before[m.partition]);
    }
    assert.equal(result.migrated.filter((m) => m.from === null).length, 1);
    mA.unsubscribe();
    mB.unsubscribe();
  });

  it('an invalid custom assignor aborts the expansion before any state changes', () => {
    const bus = new EventBus();
    // Valid for the initial 2 partitions, invalid for any larger count:
    // the expansion trial must fail fast with nothing mutated.
    const badAssignor = {
      name: 'test-bad',
      assign(partitions: number, members: string[]): Map<number, string> {
        const out = new Map<number, string>();
        for (let p = 0; p < partitions; p++) {
          if (p >= 2 || members.length === 0) return new Map();
          out.set(p, members[p % members.length]);
        }
        return out;
      },
    };
    const m = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2, assignor: badAssignor });
    assert.throws(() => bus.setGroupPartitionCount('g', 4), RangeError);
    // Untouched: still 2 partitions, stats agree.
    assert.equal(Object.keys(bus.getPartitionAssignment('g', 't.*')).length, 2);
    assert.equal(bus.getStats().consumerGroups[0].partitions, 2);
    m.unsubscribe();
  });

  it('expanding a drained group updates the retained count without a rebalance', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 2,
      onRebalance: (e) => events.push(e),
    });
    m.unsubscribe();
    events.length = 0;
    const result = bus.setGroupPartitionCount('g', 5);
    assert.equal(result.partitionsBefore, 2);
    assert.equal(result.partitionsAfter, 5);
    assert.deepEqual(result.migrated, []);
    assert.equal(events.filter((e) => e.trigger === 'expand').length, 0);
    // A rejoining generation resumes with the new count.
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 5 });
    assert.equal(Object.keys(bus.getPartitionAssignment('g', 't.*')).length, 5);
    m2.unsubscribe();
  });

  it('shows the new count in getStats().consumerGroups', () => {
    const bus = new EventBus();
    const m = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 3 });
    bus.setGroupPartitionCount('g', 6);
    const row = bus.getStats().consumerGroups.find((g) => g.groupId === 'g');
    assert.equal(row?.partitions, 6);
    m.unsubscribe();
  });
});
