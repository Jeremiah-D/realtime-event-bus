import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import {
  EventBus,
  type BusMessage,
  type GroupRebalanceEvent,
  type TopicPartitionScale,
} from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-scale-'));

/** sha256(key) mod n — the publish-side partition routing (EB-90). */
function hashPartition(key: string, n: number): number {
  return Number(createHash('sha256').update(key, 'utf8').digest().readBigUInt64BE(0) % BigInt(n));
}

describe('topic partition scaling (EB-216)', () => {
  it('validates grow-only: equal/smaller/non-integer counts throw RangeError', () => {
    const bus = new EventBus();
    const result: TopicPartitionScale = bus.scalePartitions('t', 4);
    assert.equal(result.topic, 't');
    assert.equal(result.partitionsBefore, 1);
    assert.equal(result.partitionsAfter, 4);
    assert.deepEqual(result.groups, []);
    // Equal and smaller are rejected — growth only.
    assert.throws(() => bus.scalePartitions('t', 4), RangeError);
    assert.throws(() => bus.scalePartitions('t', 2), RangeError);
    // Non-integer, zero, and negative counts are rejected.
    for (const bad of [0, -1, 2.5, NaN, Infinity]) {
      assert.throws(() => bus.scalePartitions('t', bad), RangeError, `n=${bad}`);
    }
    assert.throws(() => bus.scalePartitions('', 4), RangeError);
    // A rejected scale touches nothing: the rule is still 4.
    assert.deepEqual(bus.getStats().topicPartitions, [{ topicPattern: 't', partitions: 4 }]);
  });

  it('new publishes route by the new count', async () => {
    const bus = new EventBus();
    const seen: BusMessage[] = [];
    bus.subscribe('orders', (m) => seen.push(m));
    bus.scalePartitions('orders', 8);
    const key = 'order-42';
    bus.publish('orders', 'via-key', { partitionKey: key });
    bus.publish('orders', 'unkeyed');
    await flush();
    assert.equal(seen.length, 2);
    const keyed = seen.find((m) => m.payload === 'via-key') as BusMessage;
    assert.equal(keyed.partition, hashPartition(key, 8), 'partitionKey hashes mod the NEW count');
    const unkeyed = seen.find((m) => m.payload === 'unkeyed') as BusMessage;
    assert.equal(unkeyed.partition, 0, 'no key still lands on partition 0');
    // The new count admits partitions the old count rejected.
    bus.publish('orders', 'high', { partition: 7 });
    assert.throws(() => bus.publish('orders', 'x', { partition: 8 }), RangeError);
    await flush();
    assert.ok(seen.some((m) => m.payload === 'high' && m.partition === 7));
  });

  it('sticky group on the exact topic: old partitions keep owners, only new ones distribute', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const members = [
      bus.subscribeToGroup('g', 'orders', () => {}, {
        partitions: 4,
        assignment: 'sticky',
        onRebalance: (e) => events.push(e),
      }),
      bus.subscribeToGroup('g', 'orders', () => {}, {
        partitions: 4,
        assignment: 'sticky',
        onRebalance: (e) => events.push(e),
      }),
    ];
    const before = bus.getPartitionAssignment('g', 'orders');
    assert.equal(Object.keys(before).length, 4);
    events.length = 0;
    const result = bus.scalePartitions('orders', 6);
    // One group expansion, in the setGroupPartitionCount shape.
    assert.equal(result.groups.length, 1);
    const expansion = result.groups[0];
    assert.equal(expansion.groupId, 'g');
    assert.equal(expansion.pattern, 'orders');
    assert.equal(expansion.partitionsBefore, 4);
    assert.equal(expansion.partitionsAfter, 6);
    const after = bus.getPartitionAssignment('g', 'orders');
    assert.equal(Object.keys(after).length, 6);
    // Minimal migration: every old partition keeps its owner.
    for (let p = 0; p < 4; p++) assert.equal(after[p], before[p], `partition ${p} kept its owner`);
    // Only the new partitions migrate, from null.
    assert.deepEqual(
      expansion.migrated.map((m) => m.partition),
      [4, 5],
    );
    for (const m of expansion.migrated) assert.equal(m.from, null);
    // Sticky balance: 6 partitions over 2 members is 3/3.
    const loads = new Map<string, number>();
    for (const owner of Object.values(after)) loads.set(owner, (loads.get(owner) ?? 0) + 1);
    assert.deepEqual([...loads.values()].sort(), [3, 3]);
    // The operator rebalance kept its existing shape.
    const expand = events.filter((e) => e.trigger === 'expand');
    assert.equal(expand.length, 2, 'both members observe the operator rebalance');
    for (const e of expand) {
      assert.equal(e.memberId, '');
      assert.equal(e.partitionRebalance?.strategy, 'sticky');
      assert.deepEqual(
        e.partitionRebalance?.migrated.map((m) => m.partition),
        [4, 5],
      );
    }
    for (const m of members) m.unsubscribe();
  });

  it('leaves wildcard-pattern groups and already-large groups alone', () => {
    const bus = new EventBus();
    const wildcardEvents: GroupRebalanceEvent[] = [];
    const bigEvents: GroupRebalanceEvent[] = [];
    const w = bus.subscribeToGroup('w', 'orders.*', () => {}, {
      partitions: 2,
      onRebalance: (e) => wildcardEvents.push(e),
    });
    const b = bus.subscribeToGroup('big', 'orders', () => {}, {
      partitions: 8,
      assignment: 'sticky',
      onRebalance: (e) => bigEvents.push(e),
    });
    const beforeW = bus.getPartitionAssignment('w', 'orders.*');
    const beforeBig = bus.getPartitionAssignment('big', 'orders');
    wildcardEvents.length = 0;
    bigEvents.length = 0;
    const result = bus.scalePartitions('orders', 4);
    // Neither group expanded: wildcard patterns don't follow one
    // topic's scale, and 'big' is already past the new count.
    assert.deepEqual(result.groups, []);
    assert.deepEqual(bus.getPartitionAssignment('w', 'orders.*'), beforeW);
    assert.deepEqual(bus.getPartitionAssignment('big', 'orders'), beforeBig);
    assert.equal(
      wildcardEvents.filter((e) => e.trigger === 'expand').length,
      0,
    );
    assert.equal(
      bigEvents.filter((e) => e.trigger === 'expand').length,
      0,
    );
    w.unsubscribe();
    b.unsubscribe();
  });

  it('does not migrate stored data: seqs, durable records, and committed offsets stay put', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir });
    const seen: BusMessage[] = [];
    bus.subscribe('orders', (m) => seen.push(m));
    const g = bus.subscribeToGroup('g', 'orders', () => {}, { partitions: 2 });
    bus.publish('orders', 'a', { partition: 0 });
    bus.publish('orders', 'b', { partition: 0 });
    await flush();
    assert.deepEqual(
      seen.map((m) => m.seq),
      [1, 2],
      'partition 0 numbers its own seqs from 1',
    );
    bus.commitOffset('g', 'orders', 2, { partition: 0 });
    const filesBefore = readdirSync(dir)
      .sort()
      .map((f) => `${f}:${statSync(join(dir, f)).size}`);
    const result = bus.scalePartitions('orders', 4);
    assert.equal(result.groups.length, 1, 'the partitioned group expanded with the topic');
    // The durable log was not rewritten: no file added, removed, or resized.
    const filesAfter = readdirSync(dir)
      .sort()
      .map((f) => `${f}:${statSync(join(dir, f)).size}`);
    assert.deepEqual(filesAfter, filesBefore, 'stored records were not rewritten');
    // Partition 0's numbering continues — its history was not migrated.
    bus.publish('orders', 'c', { partition: 0 });
    await flush();
    const c = seen.find((m) => m.payload === 'c') as BusMessage;
    assert.equal(c.partition, 0);
    assert.equal(c.seq, 3, 'partition 0 seq continues after the scale');
    // A brand-new partition starts its own numbering at 1.
    bus.publish('orders', 'fresh', { partition: 3 });
    await flush();
    const fresh = seen.find((m) => m.payload === 'fresh') as BusMessage;
    assert.equal(fresh.seq, 1, 'new partitions start empty');
    // The group's committed checkpoint survived the expansion.
    assert.deepEqual(bus.getPartitionCommittedOffsets('g', 'orders')['0'], { orders: 2 });
    g.unsubscribe();
  });

  it('audits the scale as topic_partition_scaled with topic, counts, and groups', () => {
    const bus = new EventBus();
    const g = bus.subscribeToGroup('g', 'orders', () => {}, { partitions: 2 });
    bus.scalePartitions('orders', 4);
    const events = bus.getSubscriptionAudit({ action: 'topic_partition_scaled' });
    assert.equal(events.length, 1);
    const e = events[0];
    assert.equal(e.action, 'topic_partition_scaled');
    assert.equal(e.actor, 'system');
    assert.equal(e.pattern, 'orders');
    assert.equal(e.topic, 'orders');
    assert.equal(typeof e.at, 'number');
    assert.deepEqual(e.detail, {
      partitionsBefore: 1,
      partitionsAfter: 4,
      groups: ['g'],
    });
    // The publish-side rule change is audited through the normal path too.
    const ruleEvents = bus.getSubscriptionAudit({ action: 'partition_rule_set' });
    assert.ok(ruleEvents.some((r) => r.pattern === 'orders' && (r.detail as { partitions: number }).partitions === 4));
    g.unsubscribe();
  });

  it('drained-group expansions are audited as queryable partition_expansion events', () => {
    const bus = new EventBus();
    const m = bus.subscribeToGroup('dg', 'drainme', () => {}, { partitions: 2 });
    m.unsubscribe();
    // Fully drained: only the retained count changes, audited on its own.
    const result = bus.setGroupPartitionCount('dg', 4);
    assert.equal(result.partitionsAfter, 4);
    const events = bus.getSubscriptionAudit({ action: 'partition_expansion' });
    assert.equal(events.length, 1);
    assert.equal(events[0].groupId, 'dg');
    assert.equal(events[0].pattern, 'drainme');
    assert.deepEqual(events[0].detail, { partitionsBefore: 2, partitionsAfter: 4, members: 0 });
  });

  it('reports partition counts on getStats and Prometheus', () => {
    const bus = new EventBus();
    assert.deepEqual(bus.getStats().topicPartitions, []);
    // A bus with no partition rules renders no partition series.
    assert.ok(!renderPrometheus(bus.getStats()).includes('eventbus_topic_partitions'));
    bus.setTopicPartitions('orders.*', 2);
    bus.scalePartitions('orders', 4);
    assert.deepEqual(bus.getStats().topicPartitions, [
      { topicPattern: 'orders.*', partitions: 2 },
      { topicPattern: 'orders', partitions: 4 },
    ]);
    const exposition = renderPrometheus(bus.getStats());
    assert.ok(exposition.includes('eventbus_topic_partitions{topic_pattern="orders.*"} 2'));
    assert.ok(exposition.includes('eventbus_topic_partitions{topic_pattern="orders"} 4'));
  });
});
