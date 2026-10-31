/**
 * Bus-level partition-health revision (EB-298).
 *
 * `setPartitionHealth` retunes the tracker live — the sliding window,
 * the unhealthy threshold, the minimum samples, and the recovery
 * cool-down — without restarting the bus. Only a bus that enabled
 * `partitionHealth` can be revised; illegal values fail closed with
 * the live tuning and the audit log untouched; an identical (or empty)
 * revision is a no-op with no audit. In-flight outcome windows are
 * untouched: a tightened window truncates at the next report, a
 * tightened threshold condemns at the next classify, and a revised
 * cool-down arms the next condemnation (an in-flight cool-down
 * counter is not rewritten).
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Tight health tuning so a few reports flip health deterministically. */
const tight = {
  partitionHealth: { windowSize: 4, minSamples: 2, unhealthyThreshold: 0.5, cooldownSuccesses: 2 },
};

describe('partition health revision (EB-298)', () => {
  it('setPartitionHealth fails closed when partitionHealth is not enabled', () => {
    const bus = new EventBus();
    bus.setTopicPartitions('t', 3);
    assert.throws(() => bus.setPartitionHealth({ unhealthyThreshold: 0.5 }), RangeError);
    assert.throws(() => bus.setPartitionHealth({ windowSize: 10 }), RangeError);
    assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);
  });

  it('setPartitionHealth rejects illegal values and leaves the live tuning untouched', () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    const bad: Array<unknown> = [
      { windowSize: 0 },
      { windowSize: -1 },
      { windowSize: 1.5 },
      { unhealthyThreshold: 0 },
      { unhealthyThreshold: 1.5 },
      { unhealthyThreshold: Number.NaN },
      { unhealthyThreshold: Infinity },
      { minSamples: 0 },
      { minSamples: 1.5 },
      // minSamples beyond the merged window can never condemn.
      { minSamples: 5 },
      // Tightening the window below the live minSamples is the same failure.
      { windowSize: 1 },
      { cooldownSuccesses: 0 },
      { cooldownSuccesses: -1 },
      'nope',
      42,
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setPartitionHealth(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    // The failed calls changed nothing: the live tuning still condemns
    // on two failures (minSamples 2, threshold 0.5).
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
    assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);
  });

  it('setPartitionHealth with identical values or an empty revision is a no-op with no audit', () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    bus.setPartitionHealth({ unhealthyThreshold: 0.5 });
    bus.setPartitionHealth({ windowSize: 4 });
    bus.setPartitionHealth({});
    bus.setPartitionHealth(null as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);
    // The untouched tuning still governs.
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
  });

  it('threshold revision condemns at the next classify, audit carries old and new', () => {
    const bus = new EventBus({
      partitionHealth: { windowSize: 10, minSamples: 2, unhealthyThreshold: 0.9, cooldownSuccesses: 2 },
    });
    bus.setTopicPartitions('t', 3);
    // One failure + one success: rate 0.5 < 0.9, healthy.
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionSuccess('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, true);
    bus.setPartitionHealth({ unhealthyThreshold: 0.5 });
    // The revision alone condemns nothing — the next classify does.
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, true);
    bus.reportPartitionFailure('t', 0);
    const row = bus.getStats().partitionHealth?.[0];
    assert.deepEqual(row, {
      topic: 't',
      partition: 0,
      healthy: false,
      failureRate: 2 / 3,
      failures: 2,
      timeouts: 0,
      samples: 3,
    });
    const audit = bus.getSubscriptionAudit({ action: 'partition_health_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 10,
      oldWindowSize: 10,
      unhealthyThreshold: 0.5,
      oldUnhealthyThreshold: 0.9,
      minSamples: 2,
      oldMinSamples: 2,
      cooldownSuccesses: 2,
      oldCooldownSuccesses: 2,
    });
  });

  it('partial revision moves only the provided field', () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    bus.setPartitionHealth({ cooldownSuccesses: 7 });
    const audit = bus.getSubscriptionAudit({ action: 'partition_health_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 4,
      oldWindowSize: 4,
      unhealthyThreshold: 0.5,
      oldUnhealthyThreshold: 0.5,
      minSamples: 2,
      oldMinSamples: 2,
      cooldownSuccesses: 7,
      oldCooldownSuccesses: 2,
    });
  });

  it('minSamples revision gates condemnation at the next classify', () => {
    const bus = new EventBus({
      partitionHealth: { windowSize: 10, minSamples: 4, unhealthyThreshold: 0.5, cooldownSuccesses: 2 },
    });
    bus.setTopicPartitions('t', 3);
    // Two failures are below the live minSamples (4): still healthy.
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, true);
    bus.setPartitionHealth({ minSamples: 2 });
    // The next classify sees samples (3) >= the revised minSamples (2)
    // and the 2/3 rate at the threshold.
    bus.reportPartitionSuccess('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
  });

  it('windowSize tightening truncates the oldest outcome at the next record, records table untouched until then', () => {
    const bus = new EventBus({
      partitionHealth: { windowSize: 10, minSamples: 2, unhealthyThreshold: 0.5, cooldownSuccesses: 2 },
    });
    bus.setTopicPartitions('t', 3);
    bus.reportPartitionSuccess('t', 0);
    bus.reportPartitionSuccess('t', 0);
    bus.reportPartitionSuccess('t', 0);
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    // 2/5 = 0.4 < 0.5 at minSamples 2: healthy, five samples recorded.
    const before = bus.getStats().partitionHealth?.[0];
    assert.deepEqual(before, {
      topic: 't',
      partition: 0,
      healthy: true,
      failureRate: 0.4,
      failures: 2,
      timeouts: 0,
      samples: 5,
    });
    bus.setPartitionHealth({ windowSize: 3 });
    // The retune alone touches no record: the window still holds five.
    assert.deepEqual(bus.getStats().partitionHealth?.[0], before);
    // The next record truncates to the newest three outcomes
    // [failure, failure, success]: 2/3 >= 0.5 → unhealthy.
    bus.reportPartitionSuccess('t', 0);
    assert.deepEqual(bus.getStats().partitionHealth?.[0], {
      topic: 't',
      partition: 0,
      healthy: false,
      failureRate: 2 / 3,
      failures: 2,
      timeouts: 0,
      samples: 3,
    });
  });

  it('cooldownSuccesses revision arms the next condemnation, in-flight cool-down untouched', () => {
    const bus = new EventBus(tight);
    bus.setTopicPartitions('t', 3);
    // Condemn, then one success into the cool-down of 2.
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
    bus.reportPartitionSuccess('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
    // Revise the cool-down to 5 mid-flight: the in-flight counter is
    // not rewritten, so one more success still recovers.
    bus.setPartitionHealth({ cooldownSuccesses: 5 });
    bus.reportPartitionSuccess('t', 0);
    const recovered = bus.getStats().partitionHealth?.[0];
    assert.equal(recovered?.healthy, true);
    // Recovery clears the window (the row stays, with zero samples).
    assert.equal(recovered?.samples, 0);
    // Re-condemn: the revised cool-down of 5 now arms the new
    // condemnation — two successes are not enough.
    bus.reportPartitionFailure('t', 0);
    bus.reportPartitionFailure('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
    bus.reportPartitionSuccess('t', 0);
    bus.reportPartitionSuccess('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
    bus.reportPartitionSuccess('t', 0);
    bus.reportPartitionSuccess('t', 0);
    bus.reportPartitionSuccess('t', 0);
    assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, true);
    assert.equal(bus.getStats().partitionHealth?.[0]?.samples, 0);
  });
});
