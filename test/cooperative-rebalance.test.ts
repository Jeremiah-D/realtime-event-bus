import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';
import { stickyPartitionAssignment } from '../src/sticky.ts';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Independent key->partition mapping (same contract as the bus). */
function keyPartition(key: string, n: number): number {
  const digest = createHash('sha256').update(`k\0${key}`, 'utf8').digest();
  return Number(digest.readBigUInt64BE(0) % BigInt(n));
}

/** First key (of `key-${i}` candidates) landing on partition `p`. */
function keyForPartition(p: number, n: number): string {
  for (let i = 0; ; i++) {
    const k = `key-${i}`;
    if (keyPartition(k, n) === p) return k;
  }
}

function countsOf(assignment: Record<number, string>): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const owner of Object.values(assignment)) counts[owner] = (counts[owner] ?? 0) + 1;
  return counts;
}

describe('stickyPartitionAssignment (unit)', () => {
  it('balances 6 partitions over 3 members 2/2/2 (rendezvous skews 3/2/1)', () => {
    const a = stickyPartitionAssignment({ partitions: 6, members: ['a', 'b', 'c'] });
    assert.deepEqual(countsOf(Object.fromEntries(a)), { a: 2, b: 2, c: 2 });
  });

  it('is deterministic and pinned for a small case', () => {
    // Sorted roster ['sub-1','sub-2'], targets 2/2, no previous: pool
    // fills the first member to target before moving on.
    const a = stickyPartitionAssignment({ partitions: 4, members: ['sub-2', 'sub-1'] });
    assert.deepEqual(Object.fromEntries(a), { 0: 'sub-1', 1: 'sub-1', 2: 'sub-2', 3: 'sub-2' });
  });

  it('keeps every partition on its surviving previous owner when balanced', () => {
    const prev = new Map([
      [0, 'a'],
      [1, 'a'],
      [2, 'b'],
      [3, 'b'],
    ]);
    const a = stickyPartitionAssignment({ partitions: 4, members: ['a', 'b'], previous: prev });
    assert.deepEqual(Object.fromEntries(a), Object.fromEntries(prev));
  });

  it('is maximally sticky under the balance target', () => {
    // Deterministic pseudo-random churn: for every roster change the
    // retained count must equal sum(min(target_m, stickyOwned_m)) — the
    // most any balanced assignment can keep.
    let seed = 0x12345678;
    const rand = () => {
      seed = (seed * 1664525 + 1013904223) >>> 0;
      return seed / 0x100000000;
    };
    let prev: Map<number, string> | undefined;
    for (let round = 0; round < 50; round++) {
      const partitions = 4 + Math.floor(rand() * 12);
      const n = 1 + Math.floor(rand() * 5);
      const members = Array.from({ length: n }, (_, i) => `m${i}`);
      // Randomly drop some members to force orphan redistribution.
      const roster = prev === undefined ? members : members.filter(() => rand() > 0.25);
      if (roster.length === 0) continue;
      const next = stickyPartitionAssignment({ partitions, members: roster, previous: prev });
      // Balance invariant.
      const counts = Object.values(countsOf(Object.fromEntries(next)));
      assert.ok(Math.max(...counts) - Math.min(...counts) <= 1, 'balanced within 1');
      // Maximal stickiness.
      if (prev !== undefined) {
        const sorted = [...roster].sort();
        const base = Math.floor(partitions / roster.length);
        const extra = partitions % roster.length;
        const stickyOwned = new Map<string, number>();
        for (const [p, o] of prev) {
          // Only partitions that exist in the new generation (p < P) can
          // possibly be retained — shrunk partition counts orphan the rest.
          if (p < partitions && roster.includes(o)) {
            stickyOwned.set(o, (stickyOwned.get(o) ?? 0) + 1);
          }
        }
        let expected = 0;
        sorted.forEach((m, i) => {
          expected += Math.min(base + (i < extra ? 1 : 0), stickyOwned.get(m) ?? 0);
        });
        let retained = 0;
        for (const [p, o] of next) if (prev.get(p) === o) retained++;
        assert.equal(retained, expected, `round ${round}: maximally sticky`);
      }
      prev = next;
    }
  });

  it('handles fewer partitions than members', () => {
    const a = stickyPartitionAssignment({ partitions: 2, members: ['a', 'b', 'c'] });
    assert.deepEqual(countsOf(Object.fromEntries(a)), { a: 1, b: 1 });
  });

  it('returns an empty assignment for an empty roster', () => {
    assert.equal(stickyPartitionAssignment({ partitions: 4, members: [] }).size, 0);
  });

  it('rejects a non-positive partition count', () => {
    assert.throws(() => stickyPartitionAssignment({ partitions: 0, members: ['a'] }), RangeError);
  });
});

describe('cooperative incremental rebalance (EB-54)', () => {
  it('join migrates exactly the minimal diff to restore balance', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky', onRebalance });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky', onRebalance });
    // 6 partitions / 2 members: 3/3, first member fills 0..2.
    assert.deepEqual(bus.getPartitionAssignment('g', 't.*'), {
      0: m1.id, 1: m1.id, 2: m1.id, 3: m2.id, 4: m2.id, 5: m2.id,
    });
    events.length = 0;
    const m3 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky', onRebalance });
    // 6/3 -> 2/2/2 with exactly 2 migrations, one shed by each incumbent.
    const a = bus.getPartitionAssignment('g', 't.*');
    assert.deepEqual(countsOf(a), { [m1.id]: 2, [m2.id]: 2, [m3.id]: 2 });
    const joinEvent = events.find((e) => e.trigger === 'join' && e.memberId === m3.id);
    assert.ok(joinEvent?.partitionRebalance !== undefined);
    assert.equal(joinEvent.partitionRebalance.strategy, 'sticky');
    const migrated = joinEvent.partitionRebalance.migrated;
    assert.equal(migrated.length, 2);
    const froms = new Set(migrated.map((m) => m.from));
    assert.deepEqual([...froms].sort(), [m1.id, m2.id].sort());
    assert.ok(migrated.every((m) => m.to === m3.id));
    // Stickiness: survivors kept their lowest partitions.
    assert.deepEqual(a, { 0: m1.id, 1: m1.id, 2: m3.id, 3: m2.id, 4: m2.id, 5: m3.id });
    m1.unsubscribe();
    m2.unsubscribe();
    m3.unsubscribe();
  });

  it('leave redistributes only the departed member partitions', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky', onRebalance });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky', onRebalance });
    const m3 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 6, assignment: 'sticky', onRebalance });
    events.length = 0;
    m1.unsubscribe();
    const a = bus.getPartitionAssignment('g', 't.*');
    assert.deepEqual(countsOf(a), { [m2.id]: 3, [m3.id]: 3 });
    const leaveEvent = events.find((e) => e.trigger === 'leave' && e.memberId === m1.id);
    assert.equal(leaveEvent?.partitionRebalance?.strategy, 'sticky');
    const migrated = leaveEvent?.partitionRebalance?.migrated ?? [];
    // Only m1's two partitions moved, one to each survivor.
    assert.equal(migrated.length, 2);
    assert.ok(migrated.every((m) => m.from === m1.id));
    const tos = new Set(migrated.map((m) => m.to));
    assert.deepEqual([...tos].sort(), [m2.id, m3.id].sort());
    // Survivors kept every partition they had: {3,4} and {2,5}.
    assert.equal(a[2], m3.id);
    assert.equal(a[3], m2.id);
    assert.equal(a[4], m2.id);
    assert.equal(a[5], m3.id);
    m2.unsubscribe();
    m3.unsubscribe();
  });

  it('stable partitions keep consuming through a rebalance (no stop-the-world)', async () => {
    const bus = new EventBus();
    const got1: string[] = [];
    const got2: string[] = [];
    const got3: string[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', (m) => got1.push(m.payload as string), {
      partitions: 6,
      assignment: 'sticky',
    });
    const m2 = bus.subscribeToGroup('g', 't.*', (m) => got2.push(m.payload as string), {
      partitions: 6,
      assignment: 'sticky',
    });
    // Partition 0 stays on m1 across the join; partition 2 migrates to m3.
    const stableKey = keyForPartition(0, 6);
    const movingKey = keyForPartition(2, 6);
    bus.publish('t.1', 'before-stable', { key: stableKey });
    bus.publish('t.1', 'before-moving', { key: movingKey });
    await sleep(50);
    assert.deepEqual(got1, ['before-stable', 'before-moving']);
    assert.deepEqual(got2, []);
    const m3 = bus.subscribeToGroup('g', 't.*', (m) => got3.push(m.payload as string), {
      partitions: 6,
      assignment: 'sticky',
    });
    // No pause, no fence: publish immediately around the rebalance.
    bus.publish('t.1', 'after-stable', { key: stableKey });
    bus.publish('t.1', 'after-moving', { key: movingKey });
    await sleep(50);
    // Stable partition kept flowing to its owner without interruption.
    assert.deepEqual(got1, ['before-stable', 'before-moving', 'after-stable']);
    // The migrated partition now flows to its new owner.
    assert.deepEqual(got3, ['after-moving']);
    assert.deepEqual(got2, []);
    // No message lost or duplicated across the group.
    const all = [...got1, ...got2, ...got3].sort();
    assert.deepEqual(all, ['after-moving', 'after-stable', 'before-moving', 'before-stable']);
    m1.unsubscribe();
    m2.unsubscribe();
    m3.unsubscribe();
  });

  it('rejects a strategy mismatch and rolls back the join', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
    assert.throws(
      () => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'rendezvous' }),
      /strategy mismatch/,
    );
    // Rolled back: the group still has exactly one member.
    assert.equal(bus.getGroupMembers('g', 't.*').length, 1);
    // Omitting the strategy inherits the group's: fine.
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    assert.equal(bus.getGroupMembers('g', 't.*').length, 2);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('rejects an invalid assignment value before registering', () => {
    const bus = new EventBus();
    assert.throws(
      () =>
        bus.subscribeToGroup('g', 't.*', () => {}, {
          partitions: 4,
          assignment: 'consistent-hashing' as 'sticky',
        }),
      /assignment must be 'rendezvous' or 'sticky'/,
    );
    assert.equal(bus.getGroupMembers('g', 't.*').length, 0);
  });

  it('defaults to rendezvous and reports it on the rebalance event', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 4,
      onRebalance: (e) => events.push(e),
    });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const joinEvent = events.find((e) => e.trigger === 'join' && e.memberId === m2.id);
    assert.equal(joinEvent?.partitionRebalance?.strategy, 'rendezvous');
    // Rendezvous behavior unchanged: deterministic from the roster alone.
    const a1 = bus.getPartitionAssignment('g', 't.*');
    const bus2 = new EventBus();
    bus2.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    bus2.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    assert.deepEqual(bus2.getPartitionAssignment('g', 't.*'), a1);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('a fully drained group starts fresh: no pinning to departed members', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
    m1.unsubscribe();
    m2.unsubscribe();
    // New generation (first rejoiner fixes the mode, mirroring the
    // `partitions` option): previous owners are gone, so the assignment
    // references only live members.
    const n1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
    const n2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
    const a = bus.getPartitionAssignment('g', 't.*');
    assert.deepEqual(countsOf(a), { [n1.id]: 2, [n2.id]: 2 });
    for (const owner of Object.values(a)) {
      assert.ok(owner === n1.id || owner === n2.id);
    }
    n1.unsubscribe();
    n2.unsubscribe();
  });
});
