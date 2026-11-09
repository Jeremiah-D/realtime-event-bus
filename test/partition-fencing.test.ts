import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';
import {
  FIRST_PARTITION_GENERATION,
  nextPartitionGeneration,
} from '../src/partition-fencing.ts';

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

/** Live fencing generation of one competing set. */
function generationOf(bus: EventBus, groupId = 'g', pattern = 't.*'): number {
  return bus.getGroupAssignment(groupId, pattern).generation;
}

describe('nextPartitionGeneration (unit)', () => {
  it('starts at 1 and increments by one', () => {
    assert.equal(FIRST_PARTITION_GENERATION, 1);
    assert.equal(nextPartitionGeneration(0), 1);
    assert.equal(nextPartitionGeneration(1), 2);
    assert.equal(nextPartitionGeneration(41), 42);
  });

  it('wraps at MAX_SAFE_INTEGER back to 1, never to 0 or backwards', () => {
    assert.equal(nextPartitionGeneration(Number.MAX_SAFE_INTEGER), 1);
    assert.equal(nextPartitionGeneration(1), 2);
  });
});

describe('partition fencing generations (EB-441)', () => {
  it('first assignment is generation 1 and each rebalance bumps by exactly one', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    // onRebalance fires once per member per rebalance, so the last
    // event after each step is the rebalance's own generation.
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, onRebalance });
    assert.equal(generationOf(bus), 1);
    assert.equal(events.at(-1)?.partitionRebalance?.generation, 1);
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, onRebalance });
    assert.equal(generationOf(bus), 2);
    assert.equal(events.at(-1)?.partitionRebalance?.generation, 2);
    // A leave is a revocation: it bumps too.
    m2.unsubscribe();
    assert.equal(generationOf(bus), 3);
    assert.equal(events.at(-1)?.partitionRebalance?.generation, 3);
    assert.equal(events.at(-1)?.trigger, 'leave');
    m1.unsubscribe();
  });

  it('rebalance events and snapshots agree on the live generation', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, onRebalance });
    const lastGen1 = events.at(-1)?.partitionRebalance?.generation;
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, onRebalance });
    const lastGen2 = events.at(-1)?.partitionRebalance?.generation;
    bus.setGroupPartitionCount('g', 6);
    const lastGen3 = events.at(-1)?.partitionRebalance?.generation;
    assert.deepEqual([lastGen1, lastGen2, lastGen3], [1, 2, 3]);
    assert.equal(generationOf(bus), 3);
    assert.equal(bus.getGroupAssignment('g', 't.*').generation, lastGen3);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('reports generation 0 for a classic round-robin group', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't', () => {});
    const m2 = bus.subscribeToGroup('g', 't', () => {});
    assert.equal(bus.getGroupAssignment('g', 't').generation, 0);
    assert.deepEqual(bus.getGroupAssignment('g', 't').assignments, []);
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('rejects a stale per-partition commit, leaves the offset untouched, and audits it', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const gen1 = generationOf(bus);
    assert.equal(gen1, 1);
    // A live-generation commit moves the offset.
    bus.commitOffset('g', 't.a', 7, { partition: 0, generation: gen1 });
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), { 0: { 't.a': 7 } });
    // The rebalance moves the generation on; the old one is now stale.
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    assert.equal(generationOf(bus), 2);
    assert.throws(
      () => bus.commitOffset('g', 't.a', 9, { partition: 0, generation: gen1 }),
      /stale partition generation 1/,
    );
    // The offset did not move, and the rejection is on the audit record.
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), { 0: { 't.a': 7 } });
    const audit = bus.getSubscriptionAudit({ action: 'stale_partition_generation' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0]?.groupId, 'g');
    assert.deepEqual(audit[0]?.detail, {
      group: 'g',
      partition: 0,
      presented_generation: 1,
      live_generation: 2,
    });
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('accepts a commit presenting the live generation after a rebalance', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    const live = generationOf(bus);
    bus.commitOffset('g', 't.a', 5, { partition: 2, generation: live });
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), { 2: { 't.a': 5 } });
    m1.unsubscribe();
    m2.unsubscribe();
  });

  it('rejects commits for unknown groups/partitions and malformed generations', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4 });
    // Unknown group: no partitioned set to fence against.
    assert.throws(() => bus.commitOffset('nope', 't.a', 1, { partition: 0, generation: 1 }), RangeError);
    // Partition outside the group's count.
    assert.throws(() => bus.commitOffset('g', 't.a', 1, { partition: 4, generation: 1 }), RangeError);
    // Generation 0 is not a generation: malformed input, not a stale one.
    assert.throws(() => bus.commitOffset('g', 't.a', 1, { partition: 0, generation: 0 }), RangeError);
    assert.equal(bus.getSubscriptionAudit({ action: 'stale_partition_generation' }).length, 0);
    m1.unsubscribe();
  });

  it('drops an old-generation in-flight delivery at the drain gate', async () => {
    const bus = new EventBus();
    let aCalls = 0;
    // Sticky assignment balances 4 partitions 2/2, so m1 is guaranteed
    // to own partitions (rendezvous could skew them all onto m2).
    const m1 = bus.subscribeToGroup('g', 't.*', () => { aCalls += 1; }, { partitions: 4, assignment: 'sticky' });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
    // Find a partition owned by m1 and publish a keyed message onto it.
    const assignment = bus.getGroupAssignment('g', 't.*').assignments;
    const owned = assignment.find((a) => a.memberId === m1.id);
    assert.ok(owned !== undefined, 'm1 must own at least one partition');
    const partition = owned.partition;
    bus.publish('t.a', 'stale', { key: keyForPartition(partition, 4) });
    // The message is queued in m1's queue; the drain has not run yet
    // (queueMicrotask). Rebalance synchronously: the generation moves
    // on while the delivery is still in flight.
    const genBefore = generationOf(bus);
    m2.unsubscribe();
    assert.equal(generationOf(bus), genBefore + 1);
    await sleep(25);
    // The old owner's handler never saw the revoked delivery...
    assert.equal(aCalls, 0);
    // ...and the drop is audited with the presented/live generations.
    const audit = bus.getSubscriptionAudit({ action: 'stale_partition_generation' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0]?.detail, {
      group: 'g',
      partition,
      presented_generation: genBefore,
      live_generation: genBefore + 1,
    });
    // The partition watermark still advanced (the message was assigned),
    // but nothing was consumed under the revoked generation.
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), {});
    m1.unsubscribe();
  });

  it('delivers normally to the new owner under the live generation', async () => {
    const bus = new EventBus();
    let aCalls = 0;
    let bCalls = 0;
    const m1 = bus.subscribeToGroup('g', 't.*', () => { aCalls += 1; }, { partitions: 4 });
    const m2 = bus.subscribeToGroup('g', 't.*', () => { bCalls += 1; }, { partitions: 4 });
    // m1 leaves: every partition moves to m2 under the new generation.
    m1.unsubscribe();
    const live = generationOf(bus);
    assert.equal(live, 3);
    const owned = bus.getGroupAssignment('g', 't.*').assignments[0];
    assert.equal(owned?.memberId, m2.id);
    bus.publish('t.a', 'fresh', { key: keyForPartition(owned.partition, 4) });
    await sleep(25);
    assert.equal(aCalls, 0);
    assert.equal(bCalls, 1);
    // And the new owner can commit under the live generation.
    bus.commitOffset('g', 't.a', 1, { partition: owned.partition, generation: live });
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), {
      [owned.partition]: { 't.a': 1 },
    });
    m2.unsubscribe();
  });

  it('a rejoining old owner gets the new generation; its old one stays stale', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, onRebalance });
    const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, onRebalance });
    const gen2 = generationOf(bus);
    assert.equal(gen2, 2);
    bus.commitOffset('g', 't.a', 3, { partition: 1, generation: gen2 });
    // m1 leaves (generation 3) and rejoins (generation 4).
    m1.unsubscribe();
    assert.equal(generationOf(bus), 3);
    const m1b = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, onRebalance });
    assert.equal(generationOf(bus), 4);
    assert.equal(events.at(-1)?.partitionRebalance?.generation, 4);
    // The generation m1 was served under is stale now, even for the
    // member that just rejoined under the new one.
    assert.throws(
      () => bus.commitOffset('g', 't.a', 4, { partition: 1, generation: gen2 }),
      /stale partition generation 2/,
    );
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), { 1: { 't.a': 3 } });
    // Under the live generation the same member commits fine.
    bus.commitOffset('g', 't.a', 4, { partition: 1, generation: 4 });
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), { 1: { 't.a': 4 } });
    m1b.unsubscribe();
    m2.unsubscribe();
  });
});
