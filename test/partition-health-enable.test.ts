/**
 * Partition-health live enable (EB-547):
 * `setPartitionHealth` on a bus that never enabled `partitionHealth`
 * installs the tracker live — no restart, no lost subscriptions —
 * with the revision resolved through the construction-time rule
 * (`resolvePartitionHealthOptions`: `true` takes every default, a
 * partial object falls back per field, and the `minSamples <=
 * windowSize` gate still applies). The fresh tracker starts with an
 * empty window — outcomes reported before the enable were never
 * recorded, so nothing is decided retroactively — and it is
 * live-only: a restart restores the unconfigured state. The enable is
 * audited like any revision, with all four old values `null`.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Tight health tuning so a few reports flip health deterministically. */
const tight = { windowSize: 4, minSamples: 2, unhealthyThreshold: 0.5, cooldownSuccesses: 2 };

const DEFAULTS_DETAIL = {
  windowSize: 20,
  oldWindowSize: null,
  unhealthyThreshold: 0.5,
  oldUnhealthyThreshold: null,
  minSamples: 10,
  oldMinSamples: null,
  cooldownSuccesses: 5,
  oldCooldownSuccesses: null,
};

test('the first enable with {} takes the four construction defaults', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.setPartitionHealth({});
  const audit = bus.getSubscriptionAudit({ action: 'partition_health_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, DEFAULTS_DETAIL);
  // The tracker is live: the snapshot key exists (empty window).
  assert.deepEqual(bus.getStats().partitionHealth, []);
});

test('setPartitionHealth(true) enables with the defaults', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.setPartitionHealth(true);
  const audit = bus.getSubscriptionAudit({ action: 'partition_health_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, DEFAULTS_DETAIL);
});

test('a partial enable falls back to the defaults per field', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.setPartitionHealth({ unhealthyThreshold: 0.25 });
  const audit = bus.getSubscriptionAudit({ action: 'partition_health_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    windowSize: 20,
    oldWindowSize: null,
    unhealthyThreshold: 0.25,
    oldUnhealthyThreshold: null,
    minSamples: 10,
    oldMinSamples: null,
    cooldownSuccesses: 5,
    oldCooldownSuccesses: null,
  });
  // A post-enable call revises from the live enabled values — the
  // enable is not one-shot, and the old values are now real.
  bus.setPartitionHealth({ cooldownSuccesses: 7 });
  const revised = bus.getSubscriptionAudit({ action: 'partition_health_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    windowSize: 20,
    oldWindowSize: 20,
    unhealthyThreshold: 0.25,
    oldUnhealthyThreshold: 0.25,
    minSamples: 10,
    oldMinSamples: 10,
    cooldownSuccesses: 7,
    oldCooldownSuccesses: 5,
  });
});

test('null on a never-enabled bus is a silent no-op with no audit', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.setPartitionHealth(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);
  // Still unconfigured: reporting still refuses, and the stats key
  // stays absent.
  assert.throws(() => bus.reportPartitionFailure('t', 0), RangeError);
  assert.equal(bus.getStats().partitionHealth, undefined);
  // A valid enable afterwards still works.
  bus.setPartitionHealth({});
  assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 1);
});

test('an illegal value is rejected and the bus stays unconfigured', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  const bad: Array<unknown> = [
    { windowSize: 0 },
    { windowSize: -1 },
    { windowSize: 1.5 },
    // A window tighter than the default minSamples (10) trips the
    // construction-time minSamples <= windowSize gate.
    { windowSize: 4 },
    { unhealthyThreshold: 0 },
    { unhealthyThreshold: 1.5 },
    { unhealthyThreshold: Number.NaN },
    { unhealthyThreshold: Infinity },
    { minSamples: 0 },
    { minSamples: 1.5 },
    // minSamples beyond the default window (20) can never condemn.
    { minSamples: 25 },
    { cooldownSuccesses: 0 },
    { cooldownSuccesses: -1 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setPartitionHealth(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);
  // Still unconfigured: reporting still refuses.
  assert.throws(() => bus.reportPartitionFailure('t', 0), RangeError);
  assert.equal(bus.getStats().partitionHealth, undefined);
  // A valid enable afterwards still works.
  bus.setPartitionHealth({ windowSize: 4, minSamples: 2 });
  assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 1);
});

test('after enabling, the next reports are judged at the live tuning', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.setPartitionHealth(tight);
  // The fresh tracker starts with an empty window.
  assert.deepEqual(bus.getStats().partitionHealth, []);
  // One failure is below minSamples (2): still healthy.
  bus.reportPartitionFailure('t', 0);
  assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, true);
  // The second failure fills the window at the threshold: condemned.
  bus.reportPartitionFailure('t', 0);
  assert.deepEqual(bus.getStats().partitionHealth?.[0], {
    topic: 't',
    partition: 0,
    healthy: false,
    failureRate: 1,
    failures: 2,
    timeouts: 0,
    samples: 2,
  });
});

test('outcomes reported before the enable are never tracked back', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  // Before the enable there is no tracker to record into — reporting
  // refuses outright, so no outcome can predate the enable.
  assert.throws(() => bus.reportPartitionFailure('t', 0), RangeError);
  assert.throws(() => bus.reportPartitionTimeout('t', 0), RangeError);
  assert.throws(() => bus.reportPartitionSuccess('t', 0), RangeError);
  bus.setPartitionHealth(tight);
  // The new tracker starts empty: one failure cannot condemn.
  bus.reportPartitionFailure('t', 0);
  assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, true);
  // And the window accumulates only from the enable onward.
  bus.reportPartitionFailure('t', 0);
  assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
});

test('there is no live clear: null after enabling leaves the tracker live', () => {
  // Verified by grep: no clearPartitionHealth exists anywhere — the
  // constructor is the only place that assigned the tracker, and
  // `null` is a no-op on both sides of the enable. The nearest
  // faithful equivalent of "clear then re-enable" is: a failed enable
  // leaves the bus unconfigured and a later valid call still enables
  // (pinned above), while `null` after enabling changes nothing.
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.setPartitionHealth(tight);
  bus.setPartitionHealth(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 1);
  bus.reportPartitionFailure('t', 0);
  bus.reportPartitionFailure('t', 0);
  // The tracker is still live: the partition is condemned at the
  // live tuning.
  assert.equal(bus.getStats().partitionHealth?.[0]?.healthy, false);
  // And a later valid revision still moves the live tuning.
  bus.setPartitionHealth({ cooldownSuccesses: 3 });
  assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 2);
});

test('a same-value revision after enabling is a no-op with no second audit', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.setPartitionHealth({ windowSize: 4, minSamples: 2, unhealthyThreshold: 0.5 });
  bus.setPartitionHealth({ unhealthyThreshold: 0.5 });
  bus.setPartitionHealth({ windowSize: 4 });
  bus.setPartitionHealth({ minSamples: 2 });
  bus.setPartitionHealth({ cooldownSuccesses: 5 });
  bus.setPartitionHealth({ windowSize: 4, minSamples: 2, unhealthyThreshold: 0.5, cooldownSuccesses: 5 });
  bus.setPartitionHealth({});
  bus.setPartitionHealth(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 1);
});

test('true on a bus that already has a tracker is refused', () => {
  // EB-540 / EB-536 family convention: `true` is an enable-only
  // token — a configured bus retunes through an object.
  const constructed = new EventBus({ partitionHealth: tight });
  constructed.setTopicPartitions('t', 3);
  assert.throws(() => constructed.setPartitionHealth(true), RangeError);
  assert.equal(constructed.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);

  const enabled = new EventBus();
  enabled.setTopicPartitions('t', 3);
  enabled.setPartitionHealth(tight);
  assert.throws(() => enabled.setPartitionHealth(true), RangeError);
  assert.equal(enabled.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 1);
  // The live tuning is untouched by the refused call: two failures
  // still condemn at the tight tuning.
  enabled.reportPartitionFailure('t', 0);
  enabled.reportPartitionFailure('t', 0);
  assert.equal(enabled.getStats().partitionHealth?.[0]?.healthy, false);
});

test('the enable is live-only: a fresh bus is unconfigured again', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.setPartitionHealth(tight);
  assert.equal(bus.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 1);
  // A restart restores the unconfigured state: a fresh bus built from
  // the same construction options holds no tracker.
  const fresh = new EventBus();
  fresh.setTopicPartitions('t', 3);
  assert.equal(fresh.getStats().partitionHealth, undefined);
  assert.throws(() => fresh.reportPartitionFailure('t', 0), RangeError);
  assert.equal(fresh.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);
  // The fresh bus can still be enabled the same way.
  fresh.setPartitionHealth(true);
  assert.equal(fresh.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 1);
});

test('a closed bus refuses the enable and the revision with RangeError', async () => {
  const unconfigured = new EventBus();
  await unconfigured.close();
  assert.throws(
    () => unconfigured.setPartitionHealth({ windowSize: 10 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(unconfigured.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);

  const configured = new EventBus({ partitionHealth: tight });
  await configured.close();
  assert.throws(
    () => configured.setPartitionHealth({ windowSize: 10 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(configured.getSubscriptionAudit({ action: 'partition_health_revised' }).length, 0);
});
