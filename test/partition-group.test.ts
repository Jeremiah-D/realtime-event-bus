import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Independent reimplementation of the bus's key->partition mapping. */
function keyPartition(key: string, n: number): number {
  const digest = createHash('sha256').update(`k\0${key}`, 'utf8').digest();
  return Number(digest.readBigUInt64BE(0) % BigInt(n));
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-partgroup-'));

describe('publish-side partition alignment for partitioned groups (EB-93)', () => {
  it('an explicit publish partition wins over the consume-side key hash', async () => {
    const bus = new EventBus();
    bus.setTopicPartitions('pt', 4);
    const gotA: BusMessage[] = [];
    const gotB: BusMessage[] = [];
    const mA = bus.subscribeToGroup('g', 'pt', (m) => gotA.push(m), { partitions: 4 });
    const mB = bus.subscribeToGroup('g', 'pt', (m) => gotB.push(m), { partitions: 4 });
    const assignment = bus.getPartitionAssignment('g', 'pt');
    // Pick a key whose consume-side hash deliberately disagrees with the
    // explicit publish partition: alignment must follow the producer.
    let key = 'k0';
    for (let i = 0; i < 64 && keyPartition(`k${i}`, 4) === 2; i += 1) key = `k${i + 1}`;
    assert.notEqual(keyPartition(key, 4), 2, 'test setup: hash must disagree with partition 2');
    bus.publish('pt', 'explicit', { key, partition: 2 });
    await sleep(50);
    assert.equal(gotA.length + gotB.length, 1);
    const owner = assignment[2];
    const ownerGot = owner === mA.id ? gotA : gotB;
    const otherGot = owner === mA.id ? gotB : gotA;
    assert.equal(ownerGot.length, 1, 'partition 2 owner receives the message');
    assert.equal(otherGot.length, 0);
    assert.equal(ownerGot[0].partition, 2, 'the envelope keeps the publish-side partition');
    mA.unsubscribe();
    mB.unsubscribe();
    void mB;
  });

  it('folds the publish partition into the group space with modulo when counts differ', async () => {
    const bus = new EventBus();
    bus.setTopicPartitions('pt8', 8);
    const got: BusMessage[][] = [[], []];
    const m0 = bus.subscribeToGroup('g', 'pt8', (m) => got[0].push(m), { partitions: 4 });
    const m1 = bus.subscribeToGroup('g', 'pt8', (m) => got[1].push(m), { partitions: 4 });
    const assignment = bus.getPartitionAssignment('g', 'pt8');
    // Topic partition 7 has no group partition 7: 7 % 4 = 3.
    bus.publish('pt8', 'p7', { partition: 7 });
    bus.publish('pt8', 'p5', { partition: 5 }); // 5 % 4 = 1
    await sleep(50);
    const all = [...got[0], ...got[1]];
    assert.equal(all.length, 2);
    for (const m of all) {
      const expectedGroupPartition = (m.partition as number) % 4;
      const expectedOwner = assignment[expectedGroupPartition];
      const actualOwner = got[0].includes(m) ? m0.id : m1.id;
      assert.equal(actualOwner, expectedOwner, `partition ${m.partition} -> group ${expectedGroupPartition}`);
    }
    m0.unsubscribe();
    m1.unsubscribe();
  });

  it('a partitionKey publish aligns to its resolved partition', async () => {
    const bus = new EventBus();
    bus.setTopicPartitions('pk', 8);
    const got: BusMessage[][] = [[], []];
    const m0 = bus.subscribeToGroup('g', 'pk', (m) => got[0].push(m), { partitions: 4 });
    const m1 = bus.subscribeToGroup('g', 'pk', (m) => got[1].push(m), { partitions: 4 });
    const assignment = bus.getPartitionAssignment('g', 'pk');
    // partitionKey resolves publish-side to hash64('my-key') % 8.
    const resolved = Number(createHash('sha256').update('my-key', 'utf8').digest().readBigUInt64BE(0) % 8n);
    bus.publish('pk', 'via-key', { partitionKey: 'my-key' });
    await sleep(50);
    const all = [...got[0], ...got[1]];
    assert.equal(all.length, 1);
    assert.equal(all[0].partition, resolved, 'publish resolved the partitionKey to a topic partition');
    const expectedOwner = assignment[resolved % 4];
    const actualOwner = got[0].includes(all[0]) ? m0.id : m1.id;
    assert.equal(actualOwner, expectedOwner, 'group consumes on the aligned partition');
    m0.unsubscribe();
    m1.unsubscribe();
  });

  it('unpartitioned traffic keeps the deterministic hash spread', async () => {
    const bus = new EventBus();
    const gotA: BusMessage[] = [];
    const gotB: BusMessage[] = [];
    // Sticky assignment splits 4 partitions 2/2 deterministically, so a
    // key set touching every partition provably spreads across members.
    const mA = bus.subscribeToGroup('g', 'plain', (m) => gotA.push(m), { partitions: 4, assignment: 'sticky' });
    const mB = bus.subscribeToGroup('g', 'plain', (m) => gotB.push(m), { partitions: 4, assignment: 'sticky' });
    const assignment = bus.getPartitionAssignment('g', 'plain');
    const keys = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'];
    for (const k of keys) bus.publish('plain', k, { key: k });
    await sleep(50);
    assert.equal(gotA.length + gotB.length, keys.length);
    assert.ok(gotA.length > 0 && gotB.length > 0, 'single-partition traffic still spreads');
    for (const m of gotA) {
      assert.equal(assignment[keyPartition(m.payload as string, 4)], mA.id);
    }
    for (const m of gotB) {
      assert.equal(assignment[keyPartition(m.payload as string, 4)], mB.id);
    }
    mA.unsubscribe();
    mB.unsubscribe();
  });

  it('per-partition watermarks are recorded on the aligned partition', async () => {
    const bus = new EventBus();
    bus.setTopicPartitions('wm', 4);
    const m1 = bus.subscribeToGroup('g', 'wm', () => {}, { partitions: 4 });
    bus.publish('wm', 'x', { partition: 3 });
    bus.publish('wm', 'y', { partition: 3 });
    bus.publish('wm', 'z', { partition: 1 });
    await sleep(50);
    const watermarks = bus.getPartitionWatermarks('g', 'wm');
    // seqs on the 4-partition topic: partition 3 drew seqs 1,2; partition 1 drew seq 1.
    assert.deepEqual(watermarks[3], { wm: 2 });
    assert.deepEqual(watermarks[1], { wm: 1 });
    assert.equal(watermarks[0], undefined);
    assert.equal(watermarks[2], undefined);
    m1.unsubscribe();
  });

  it('a migrated partition replays its backlog by aligned partition, not by hash', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicPartitions('mig', 4);
    const got1: BusMessage[] = [];
    const m1 = bus.subscribeToGroup('g', 'mig', (m) => got1.push(m), { partitions: 2 });
    // Group partitions 0..1; topic partitions 2,3 fold onto group partition 0,1.
    bus.publish('mig', 'on-p2', { partition: 2 });
    bus.publish('mig', 'on-p3', { partition: 3 });
    await sleep(50);
    assert.equal(got1.length, 2, 'single member owns every group partition');
    // Leave without committing: the new owner must replay the uncommitted
    // backlog — filtered by the aligned partition, not the key hash.
    m1.unsubscribe();
    const got2: BusMessage[] = [];
    const m2 = bus.subscribeToGroup('g', 'mig', (m) => got2.push(m), { partitions: 2 });
    await sleep(100);
    assert.equal(got2.length, 2, 'migration replays the aligned backlog');
    assert.deepEqual(
      got2.map((m) => m.payload).sort(),
      ['on-p2', 'on-p3'],
    );
    m2.unsubscribe();
  });
});
