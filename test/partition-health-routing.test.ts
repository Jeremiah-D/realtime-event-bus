import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** sha256(key) mod n — the publish-side partition routing (EB-90). */
function hashPartition(key: string, n: number): number {
  return Number(createHash('sha256').update(key, 'utf8').digest().readBigUInt64BE(0) % BigInt(n));
}

/** A partitionKey whose EB-90 hash lands on `target` (of `n` partitions). */
function keyForPartition(target: number, n: number): string {
  for (let i = 0; ; i++) {
    const k = `ph-key-${i}`;
    if (hashPartition(k, n) === target) return k;
  }
}

/** Tight health tuning so a few reports flip health deterministically. */
const tight = {
  partitionHealth: { windowSize: 4, minSamples: 2, unhealthyThreshold: 0.5, cooldownSuccesses: 2 },
};

describe('partition health-aware routing (EB-217)', () => {
  it('validates options fail-fast in the constructor', () => {
    for (const bad of [{ windowSize: 0 }, { windowSize: -1 }, { windowSize: 1.5 }]) {
      assert.throws(() => new EventBus({ partitionHealth: bad }), RangeError, JSON.stringify(bad));
    }
    for (const bad of [{ unhealthyThreshold: 0 }, { unhealthyThreshold: 1.5 }, { unhealthyThreshold: NaN }]) {
      assert.throws(() => new EventBus({ partitionHealth: bad }), RangeError, JSON.stringify(bad));
    }
    assert.throws(() => new EventBus({ partitionHealth: { minSamples: 0 } }), RangeError);
    // minSamples beyond the window can never condemn — fail fast.
    assert.throws(() => new EventBus({ partitionHealth: { windowSize: 4, minSamples: 5 } }), RangeError);
    assert.throws(() => new EventBus({ partitionHealth: { cooldownSuccesses: 0 } }), RangeError);
    assert.throws(() => new EventBus({ partitionHealth: 'yes' as never }), TypeError);
    // Empty options object arms with defaults.
    const bus = new EventBus({ partitionHealth: {} });
    assert.deepEqual(bus.getStats().partitionHealth, []);
  });

  it('absent option disables everything: no state, no rerouting, no new series', async () => {
    const bus = new EventBus();
    bus.setTopicPartitions('t', 3);
    assert.equal(bus.getStats().partitionHealth, undefined);
    assert.equal(bus.getStats().partitionSkippedUnhealthy, undefined);
    assert.throws(() => bus.reportPartitionFailure('t', 0), RangeError);
    assert.throws(() => bus.reportPartitionTimeout('t', 0), RangeError);
    assert.throws(() => bus.reportPartitionSuccess('t', 0), RangeError);
    const k0 = keyForPartition(0, 3);
    bus.publish('t', 'x', { partitionKey: k0 });
    await flush();
    const rows = bus.getStats().topics.find((t) => t.topic === 't')?.partitions;
    assert.deepEqual(rows?.map((r) => r.publishedMessages), [1, 0, 0]);
    const exposition = renderPrometheus(bus.getStats());
    assert.ok(!exposition.includes('eventbus_partition_skipped_unhealthy_total'));
  });

  it('marks a partition unhealthy at the threshold and exposes it in stats', () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    // One bad report is below minSamples (2): still healthy.
    bus.reportPartitionFailure('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, true);
    // Second failure: 2/2 = 1.0 >= 0.5 → unhealthy.
    bus.reportPartitionFailure('t', 0);
    const row = bus.getStats().partitionHealth?.[0];
    assert.deepEqual(row, {
      topic: 't',
      partition: 0,
      healthy: false,
      failureRate: 1,
      failures: 2,
      timeouts: 0,
      samples: 2,
    });
    // Other partitions are unaffected.
    assert.equal(bus.getStats().partitionHealth?.length, 1);
  });

  it('timeouts count toward the bad-outcome rate', () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 2);
    bus.reportPartitionTimeout('t', 1);
    bus.reportPartitionFailure('t', 1);
    const row = bus.getStats().partitionHealth?.[0];
    assert.equal(row?.healthy, false);
    assert.equal(row?.failureRate, 1);
    assert.equal(row?.failures, 1);
    assert.equal(row?.timeouts, 1);
  });

  it('skips the unhealthy partition on key-hashed publish, preserving key affinity', async () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    const k0 = keyForPartition(0, 3);
    const k1 = keyForPartition(1, 3);
    // Sanity: healthy partitions route by hash (key affinity).
    bus.publish('t', 'a', { partitionKey: k0 });
    bus.publish('t', 'b', { partitionKey: k1 });
    await flush();
    // Mark partition 0 unhealthy (2 failures >= minSamples).
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    // Same key k0 now skips 0 → nearest healthy partition 1.
    bus.publish('t', 'c', { partitionKey: k0 });
    await flush();
    const rows = bus.getStats().topics.find((t) => t.topic === 't')?.partitions;
    assert.deepEqual(rows?.map((r) => r.publishedMessages), [1, 2, 0]);
    // The skip is counted against the *skipped* partition.
    assert.deepEqual(bus.getStats().partitionSkippedUnhealthy, [{ topic: 't', partition: 0, skipped: 1 }]);
    const exposition = renderPrometheus(bus.getStats());
    assert.ok(exposition.includes('eventbus_partition_skipped_unhealthy_total{topic="t",partition="0"} 1'));
  });

  it('explicit partition always wins, even when unhealthy', async () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    bus.reportPartitionFailure('t', 2);
    bus.reportPartitionFailure('t', 2);
    bus.publish('t', 'x', { partition: 2 });
    await flush();
    const rows = bus.getStats().topics.find((t) => t.topic === 't')?.partitions;
    assert.deepEqual(rows?.map((r) => r.publishedMessages), [0, 0, 1]);
    // No skip counted: the caller pinned the partition deliberately.
    assert.equal(bus.getStats().partitionSkippedUnhealthy, undefined);
  });

  it('default partition 0 is health-routed too', async () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    bus.publish('t', 'x'); // no key → default 0, unhealthy → 1
    await flush();
    const rows = bus.getStats().topics.find((t) => t.topic === 't')?.partitions;
    assert.deepEqual(rows?.map((r) => r.publishedMessages), [0, 1, 0]);
    assert.deepEqual(bus.getStats().partitionSkippedUnhealthy, [{ topic: 't', partition: 0, skipped: 1 }]);
  });

  it('cool-down: consecutive successes cut the partition back into routing', async () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    const k0 = keyForPartition(0, 3);
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    // One success is not enough (cool-down is 2).
    bus.reportPartitionSuccess('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
    bus.publish('t', 'a', { partitionKey: k0 });
    await flush();
    // Second consecutive success: cool-down complete, cut back in.
    bus.reportPartitionSuccess('t', 0);
    const row = bus.getStats().partitionHealth?.[0];
    assert.equal(row?.healthy, true);
    assert.equal(row?.samples, 0); // window cleared on recovery
    bus.publish('t', 'b', { partitionKey: k0 });
    await flush();
    const rows = bus.getStats().topics.find((t) => t.topic === 't')?.partitions;
    // 'a' skipped 0→1, 'b' went to 0: key affinity restored.
    assert.deepEqual(rows?.map((r) => r.publishedMessages), [1, 1, 0]);
  });

  it('a bad outcome during the cool-down restarts it', () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionSuccess('t', 0); // cool-down: 1 of 2
    bus.reportPartitionFailure('t', 0); // restarts the cool-down
    bus.reportPartitionSuccess('t', 0); // 1 of 2 again — still unhealthy
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
    bus.reportPartitionSuccess('t', 0); // 2 of 2 — recovered
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, true);
  });

  it('all partitions unhealthy: best-effort to the original candidate, no skip counted', async () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    for (const p of [0, 1, 2]) {
      bus.reportPartitionFailure('t', p);
      bus.reportPartitionFailure('t', p);
    }
    const k0 = keyForPartition(0, 3);
    bus.publish('t', 'x', { partitionKey: k0 });
    await flush();
    const rows = bus.getStats().topics.find((t) => t.topic === 't')?.partitions;
    assert.deepEqual(rows?.map((r) => r.publishedMessages), [1, 0, 0]);
    assert.equal(bus.getStats().partitionSkippedUnhealthy, undefined);
  });

  it('report APIs validate topic and partition range', () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    assert.throws(() => bus.reportPartitionFailure('', 0), RangeError);
    assert.throws(() => bus.reportPartitionFailure('t', -1), RangeError);
    assert.throws(() => bus.reportPartitionFailure('t', 3), RangeError);
    assert.throws(() => bus.reportPartitionFailure('t', 1.5), RangeError);
    // No rule → 1 partition: only partition 0 is legal.
    assert.throws(() => bus.reportPartitionFailure('plain', 1), RangeError);
    bus.reportPartitionSuccess('plain', 0); // fine
  });

  it('reports resolve through live alias links like the publish path', () => {
    const bus = new EventBus(tight);
    bus.setTopicAlias('old', 'new');
    bus.setTopicPartitions('new', 3);
    bus.reportPartitionFailure('old', 0);
    bus.reportPartitionFailure('old', 0);
    const row = bus.getStats().partitionHealth?.[0];
    assert.equal(row?.topic, 'new');
    assert.equal(row?.healthy, false);
  });
});
