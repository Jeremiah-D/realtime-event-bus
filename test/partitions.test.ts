import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage, type GroupRebalanceEvent } from '../src/bus.ts';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Independent reimplementation of the bus's key->partition mapping. */
function keyPartition(key: string, n: number): number {
  const digest = createHash('sha256').update(`k\0${key}`, 'utf8').digest();
  return Number(digest.readBigUInt64BE(0) % BigInt(n));
}

function makeBusWithLog(): { bus: EventBus; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), 'eb-part-'));
  return { bus: new EventBus({ durableLogDir: dir }), dir };
}

describe('partitioned consumer groups (EB-38)', () => {
  it('assigns partitions deterministically via rendezvous hashing', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const a1 = bus.getPartitionAssignment('g', 't.*');
    assert.equal(Object.keys(a1).length, 4);
    // Deterministic: a fresh bus with the same join order agrees.
    const bus2 = new EventBus();
    bus2.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    bus2.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    assert.deepEqual(bus2.getPartitionAssignment('g', 't.*'), a1);
    // Every partition is owned by a current member.
    const members = new Set([m1.id, m2.id]);
    for (const owner of Object.values(a1)) assert.ok(members.has(owner));
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('delivers each keyed message exclusively to its partition owner', async () => {
    const bus = new EventBus();
    const gotA: BusMessage[] = [];
    const gotB: BusMessage[] = [];
    const mA = bus.subscribeToGroup('g', 't.*', (m) => gotA.push(m), { partitions: 4 });
    const mB = bus.subscribeToGroup('g', 't.*', (m) => gotB.push(m), { partitions: 4 });
    const assignment = bus.getPartitionAssignment('g', 't.*');
    const keys = ['alpha', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf', 'hotel'];
    for (const k of keys) bus.publish('t.1', k, { key: k });
    await sleep(50);
    assert.equal(gotA.length + gotB.length, keys.length);
    // No duplicates: each message went to exactly one member.
    const all = [...gotA.map((m) => m.payload), ...gotB.map((m) => m.payload)].sort();
    assert.deepEqual(all, [...keys].sort());
    // And it was the partition owner: key -> partition -> owner.
    for (const m of gotA) {
      const p = keyPartition(m.payload as string, 4);
      assert.equal(assignment[p], mA.id, `key ${m.payload} should go to partition ${p} owner`);
    }
    for (const m of gotB) {
      const p = keyPartition(m.payload as string, 4);
      assert.equal(assignment[p], mB.id, `key ${m.payload} should go to partition ${p} owner`);
    }
    mA.unsubscribe();
    mB.unsubscribe();
  });

  it('spreads keyless messages across members deterministically', async () => {
    const bus = new EventBus();
    const gotA: BusMessage[] = [];
    const gotB: BusMessage[] = [];
    const mA = bus.subscribeToGroup('g', 't.*', (m) => gotA.push(m), { partitions: 8 });
    const mB = bus.subscribeToGroup('g', 't.*', (m) => gotB.push(m), { partitions: 8 });
    for (let i = 0; i < 40; i++) bus.publish('t.1', i);
    await sleep(50);
    assert.equal(gotA.length + gotB.length, 40);
    assert.ok(gotA.length > 0 && gotB.length > 0, 'both members receive some partitions');
    // Deterministic: same seqs map to the same owners on replay of the mapping.
    const assignment = bus.getPartitionAssignment('g', 't.*');
    for (const m of gotA) {
      const p = Number(
        createHash('sha256').update(`t\0t.1\0${m.seq}`, 'utf8').digest().readBigUInt64BE(0) % 8n,
      );
      assert.equal(assignment[p], mA.id);
    }
    mA.unsubscribe();
    mB.unsubscribe();
    void mB;
  });

  it('migrates only affected partitions on membership change (rendezvous)', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 8, onRebalance });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 8, onRebalance });
    const before = bus.getPartitionAssignment('g', 't.*');
    events.length = 0;
    const m3 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 8, onRebalance });
    const after = bus.getPartitionAssignment('g', 't.*');
    const joinEvent = events[events.length - 1];
    assert.equal(joinEvent.trigger, 'join');
    assert.ok(joinEvent.partitionRebalance !== undefined);
    const migrated = joinEvent.partitionRebalance!.migrated;
    // Only partitions whose winner changed migrate — never the whole set.
    assert.ok(migrated.length > 0 && migrated.length < 8);
    for (const m of migrated) {
      assert.equal(m.to, m3.id, 'newcomer only takes partitions it wins');
      assert.equal(after[m.partition], m3.id);
      // Unaffected partitions keep their owner.
      void before;
    }
    for (const [p, owner] of Object.entries(before)) {
      if (!migrated.some((m) => m.partition === Number(p))) {
        assert.equal(after[Number(p)], owner, `partition ${p} should not have migrated`);
      }
    }
    // Leave: the departed member's partitions redistribute, others stay.
    events.length = 0;
    m3.unsubscribe();
    const leaveEvent = events[events.length - 1];
    assert.equal(leaveEvent.trigger, 'leave');
    const lm = leaveEvent.partitionRebalance!.migrated;
    assert.ok(lm.every((m) => m.from === m3.id));
    assert.ok(lm.length > 0 && lm.length < 8);
    const afterLeave = bus.getPartitionAssignment('g', 't.*');
    for (const [p, owner] of Object.entries(after)) {
      if (!lm.some((m) => m.partition === Number(p))) {
        assert.equal(afterLeave[Number(p)], owner);
      }
    }
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('replays a migrated partition backlog to the new owner on leave', async () => {
    const { bus } = makeBusWithLog();
    const got1: BusMessage[] = [];
    const got2: BusMessage[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', (m) => got1.push(m), { partitions: 4 });
    for (let i = 0; i < 12; i++) bus.publish('t.1', `k${i % 4}-m${i}`, { key: `k${i % 4}` });
    await sleep(50);
    assert.equal(got1.length, 12);
    // m1 leaves without committing: its partitions' backlog must move to m2.
    m1.unsubscribe();
    const m2 = bus.subscribeToGroup('g', 't.*', (m) => got2.push(m), { partitions: 4 });
    await sleep(100);
    assert.equal(got2.length, 12, 'new owner replays the full uncommitted backlog');
    const watermarks = bus.getPartitionWatermarks('g', 't.*');
    // One watermark entry per partition that actually received traffic
    // (two of the four keys may share a partition).
    const touched = new Set([0, 1, 2, 3].map((i) => keyPartition(`k${i}`, 4)));
    assert.equal(Object.keys(watermarks).length, touched.size);
    m2.unsubscribe();
  });

  it('replays from per-partition commits, not from zero', async () => {
    const { bus } = makeBusWithLog();
    const got2: BusMessage[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const keys = ['k0', 'k1', 'k2', 'k3'];
    for (let round = 0; round < 3; round++) {
      for (const k of keys) bus.publish('t.1', `${k}-r${round}`, { key: k });
    }
    await sleep(50);
    // Commit k0's partition up to its 2nd message (seqs are per-topic).
    const p0 = keyPartition('k0', 4);
    // k0's messages are seqs 1, 5, 9 on t.1 (interleaved with k1..k3).
    bus.commitOffset('g', 't.1', 5, { partition: p0 });
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*')[p0], { 't.1': 5 });
    m1.unsubscribe();
    const m2 = bus.subscribeToGroup('g', 't.*', (m) => got2.push(m), { partitions: 4 });
    await sleep(100);
    // k0's first two messages are committed: 12 - 2 = 10 replayed.
    assert.equal(got2.length, 10);
    assert.ok(!got2.some((m) => m.payload === 'k0-r0' || m.payload === 'k0-r1'));
    assert.ok(got2.some((m) => m.payload === 'k0-r2'));
    m2.unsubscribe();
  });

  it('partition-aware resumeFromSeq only replays owned partitions', async () => {
    const { bus } = makeBusWithLog();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    for (let i = 0; i < 8; i++) bus.publish('t.1', `msg${i}`, { key: `k${i % 4}` });
    await sleep(50);
    m1.unsubscribe();
    const got3: BusMessage[] = [];
    // New member resumes from seq 4: only its own partitions' seqs > 4 —
    // never the other member's partitions. The explicit resume point also
    // suppresses the automatic migration replay (no double delivery).
    const m3 = bus.subscribeToGroup('g', 't.*', (m) => got3.push(m), { partitions: 4, resumeFromSeq: 4 });
    await sleep(100);
    const assignment = bus.getPartitionAssignment('g', 't.*');
    const owned = new Set<number>();
    for (const [p, owner] of Object.entries(assignment)) {
      if (owner === m3.id) owned.add(Number(p));
    }
    const expected: string[] = [];
    for (let i = 0; i < 8; i++) {
      const seq = i + 1;
      if (seq > 4 && owned.has(keyPartition(`k${i % 4}`, 4))) expected.push(`msg${i}`);
    }
    assert.deepEqual(
      got3.map((m) => m.payload).sort(),
      expected.sort(),
      'replays exactly the owned partitions past the resume point',
    );
    m2.unsubscribe();
    m3.unsubscribe();
  });

  it('validates partition options and rolls back on mismatch', () => {
    const bus = new EventBus();
    assert.throws(() => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 0 }), RangeError);
    assert.throws(() => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: -2 }), RangeError);
    assert.throws(() => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 1.5 }), RangeError);
    assert.equal(bus.subscriberCount(), 0);
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    // Different count for the same group: fail fast, subscriber rolled back.
    assert.throws(() => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 8 }), RangeError);
    assert.equal(bus.subscriberCount(), 1);
    // Specifying partitions for a round-robin group: fail fast too.
    const r1 = bus.subscribeToGroup('r', 't.*', () => {});
    assert.throws(() => bus.subscribeToGroup('r', 't.*', () => {}, { partitions: 4 }), RangeError);
    assert.equal(bus.subscriberCount(), 2);
    // Joining a partitioned group without specifying is fine.
    const m2 = bus.subscribeToGroup('g', 't.*', () => {});
    assert.equal(bus.subscriberCount(), 3);
    m1.unsubscribe();
    m2.unsubscribe();
    r1.unsubscribe();
  });

  it('validates per-partition commits', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    bus.subscribeToGroup('r', 't.*', () => {});
    assert.throws(() => bus.commitOffset('g', 't.1', 1, { partition: -1 }), RangeError);
    assert.throws(() => bus.commitOffset('g', 't.1', 1, { partition: 1.5 }), RangeError);
    assert.throws(() => bus.commitOffset('g', 't.1', 1, { partition: 4 }), RangeError);
    assert.throws(() => bus.commitOffset('r', 't.1', 1, { partition: 0 }), RangeError);
    assert.throws(() => bus.commitOffset('nope', 't.1', 1, { partition: 0 }), RangeError);
    // Group-level commits still work alongside.
    bus.commitOffset('g', 't.1', 3);
    assert.deepEqual(bus.getCommittedOffsets('g'), { 't.1': 3 });
    bus.commitOffset('g', 't.1', 5, { partition: 2 });
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), { 2: { 't.1': 5 } });
  });

  it('persists per-partition commits across restarts', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'eb-part-restart-'));
    {
      const bus = new EventBus({ durableLogDir: dir });
      bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
      bus.publish('t.1', 'x', { key: 'k0' });
      bus.commitOffset('g', 't.1', 1, { partition: keyPartition('k0', 4) });
      bus.commitOffset('g', 't.1', 1);
      await sleep(20);
    }
    const bus2 = new EventBus({ durableLogDir: dir });
    assert.deepEqual(bus2.getCommittedOffsets('g'), { 't.1': 1 });
    const pc = bus2.getPartitionCommittedOffsets('g', 't.*');
    assert.deepEqual(pc[keyPartition('k0', 4)], { 't.1': 1 });
  });

  it('reports partitions in getStats().consumerGroups', () => {
    const bus = new EventBus();
    bus.subscribeToGroup('g1', 'a.*', () => {}, { partitions: 4 });
    bus.subscribeToGroup('g2', 'b.*', () => {});
    assert.deepEqual(bus.getStats().consumerGroups, [
      { groupId: 'g1', pattern: 'a.*', members: 1, partitions: 4 },
      { groupId: 'g2', pattern: 'b.*', members: 1 },
    ]);
  });

  it('keeps round-robin groups untouched', async () => {
    const bus = new EventBus();
    const got: BusMessage[][] = [[], []];
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', (m) => got[0].push(m), { onRebalance: (e) => events.push(e) });
    const m2 = bus.subscribeToGroup('g', 't.*', (m) => got[1].push(m), { onRebalance: (e) => events.push(e) });
    for (let i = 0; i < 6; i++) bus.publish('t.1', i);
    await sleep(50);
    assert.equal(got[0].length + got[1].length, 6);
    assert.ok(events.every((e) => e.partitionRebalance === undefined));
    assert.deepEqual(bus.getPartitionAssignment('g', 't.*'), {});
    m1.unsubscribe();
    m2.unsubscribe();
  });
});
