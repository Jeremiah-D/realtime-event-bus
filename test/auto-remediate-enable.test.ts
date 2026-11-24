/**
 * Bus-level doctor auto-remediation live enable (EB-553):
 * `setAutoRemediate` on a bus that never enabled `autoRemediate`
 * installs the schedule live — no restart, no lost subscriptions —
 * with the call resolved through the construction-time rule
 * (`assertValidRemediateSchedule`). The enable must carry
 * `intervalMs` (there is no default interval); the other fields take
 * their defaults, a partial `doctor` fills only the thresholds it
 * leaves out, and the enable is audited like any revision, with every
 * old value `null`. `null` is a silent no-op; a closed bus refuses the
 * enable. The enable is live-only: a restart restores the
 * unconfigured state.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Degrades one subscriber: a throwing handler plus a health probe that
 * trips after a single failure, so the subscriber is degraded after
 * one publish + flush — the stuck state the remediator resumes.
 */
async function degradedSubscriber(bus: EventBus) {
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 1 } },
  );
  bus.publish('t', 'x');
  await flush();
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  return sub;
}

describe('doctor auto-remediation live enable (EB-553)', () => {
  it('enables on a never-configured bus with the exact defaults and null old values in the audit', async () => {
    const bus = new EventBus();
    assert.equal(bus.getStats().autoRemediate, undefined);
    bus.setAutoRemediate({ intervalMs: 50 });
    assert.deepEqual(bus.getStats().autoRemediate, {
      intervalMs: 50,
      maxActions: 10,
      dryRun: false,
      doctor: { slowSubscriberP99Ms: 1000, lagWatermarkDefaultMs: 30000 },
    });
    const revised = bus.getSubscriptionAudit({ action: 'auto_remediate_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, {
      intervalMs: 50,
      oldIntervalMs: null,
      maxActions: 10,
      oldMaxActions: null,
      dryRun: false,
      oldDryRun: null,
      doctor: { slowSubscriberP99Ms: 1000, lagWatermarkDefaultMs: 30000 },
      oldDoctor: null,
    });
    await bus.close();
  });

  it('the enabled timer fires a scheduled run in dryRun mode: it plans only, nothing is applied', async () => {
    const bus = new EventBus();
    bus.setAutoRemediate({ intervalMs: 20, dryRun: true });
    const sub = await degradedSubscriber(bus);
    // Runs fire on the short schedule, but only preview: the
    // subscriber stays degraded and nothing is resumed.
    await sleep(300);
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);
    const runs = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
    assert.ok(runs.length >= 1, 'the enabled schedule fired at least once');
    const plannedRun = runs.find((r) => r.detail.planned >= 1);
    assert.ok(plannedRun !== undefined, 'at least one run planned the resume');
    assert.equal(plannedRun.detail.dryRun, true);
    assert.equal(plannedRun.detail.applied, 0);
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_resume' }).length, 0);
    await bus.close();
  });

  it('the enabled timer applies a real run: a degraded subscriber is resumed', async () => {
    const bus = new EventBus();
    bus.setAutoRemediate({ intervalMs: 20 });
    const sub = await degradedSubscriber(bus);
    await sleep(300);
    assert.equal(bus.subscriberHealth(sub.id).degraded, false);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_run' }).length >= 1);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_resume' }).length >= 1);
    await bus.close();
  });

  it('an enable without intervalMs cannot enable: the bus stays unconfigured and can still be enabled', async () => {
    const bus = new EventBus();
    const noInterval: Array<unknown> = [
      {},
      { maxActions: 1 },
      { dryRun: true },
      { doctor: { slowSubscriberP99Ms: 500 } },
    ];
    for (const revision of noInterval) {
      assert.throws(
        () => bus.setAutoRemediate(revision as never),
        /intervalMs is required/,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
      assert.equal(bus.getStats().autoRemediate, undefined);
    }
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
    // Still unconfigured — and still enableable.
    bus.setAutoRemediate({ intervalMs: 50 });
    assert.notEqual(bus.getStats().autoRemediate, undefined);
    await bus.close();
  });

  it('null on a never-enabled bus is a silent no-op with no audit', async () => {
    const bus = new EventBus();
    bus.setAutoRemediate(null as never);
    assert.equal(bus.getStats().autoRemediate, undefined);
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
    // Still unconfigured — and still enableable.
    bus.setAutoRemediate({ intervalMs: 50 });
    assert.notEqual(bus.getStats().autoRemediate, undefined);
    await bus.close();
  });

  it('illegal values are rejected and leave the bus unconfigured', async () => {
    const bus = new EventBus();
    const bad: Array<unknown> = [
      'nope',
      42,
      ['x'],
      { intervalMs: 0 },
      { intervalMs: -5 },
      { intervalMs: Number.NaN },
      { intervalMs: Number.POSITIVE_INFINITY },
      { intervalMs: 'fast' },
      { intervalMs: 50, maxActions: -1 },
      { intervalMs: 50, maxActions: 1.5 },
      { intervalMs: 50, maxActions: 'many' },
      { intervalMs: 50, dryRun: 'yes' },
      { intervalMs: 50, dryRun: 1 },
      { intervalMs: 50, doctor: { slowSubscriberP99Ms: 0 } },
      { intervalMs: 50, doctor: { slowSubscriberP99Ms: Number.NaN } },
      { intervalMs: 50, doctor: { lagWatermarkDefaultMs: 0 } },
      { intervalMs: 50, doctor: { lagWatermarkDefaultMs: 'slow' } },
      { intervalMs: 50, doctor: 'yes' },
      { intervalMs: 50, doctor: [] },
      { intervalMs: 50, doctor: null },
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setAutoRemediate(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
      assert.equal(bus.getStats().autoRemediate, undefined);
    }
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
    // A failed enable leaves the bus unconfigured: a later valid call
    // still enables it.
    bus.setAutoRemediate({ intervalMs: 50 });
    assert.notEqual(bus.getStats().autoRemediate, undefined);
    await bus.close();
  });

  it('a partial doctor on enable fills only the missing thresholds', async () => {
    const bus = new EventBus();
    bus.setAutoRemediate({ intervalMs: 50, doctor: { slowSubscriberP99Ms: 2500 } });
    assert.deepEqual(bus.getStats().autoRemediate?.doctor, {
      slowSubscriberP99Ms: 2500,
      lagWatermarkDefaultMs: 30000,
    });
    await bus.close();

    const second = new EventBus();
    second.setAutoRemediate({
      intervalMs: 50,
      doctor: { slowSubscriberP99Ms: 2500, lagWatermarkDefaultMs: 5000 },
    });
    assert.deepEqual(second.getStats().autoRemediate?.doctor, {
      slowSubscriberP99Ms: 2500,
      lagWatermarkDefaultMs: 5000,
    });
    await second.close();
  });

  it('a revision after the enable re-arms the timer at the new interval', async () => {
    const bus = new EventBus();
    bus.setAutoRemediate({ intervalMs: 60_000 });
    const sub = await degradedSubscriber(bus);
    // Nothing fires on the 60s schedule while we wait.
    await sleep(50);
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_run' }).length, 0);

    bus.setAutoRemediate({ intervalMs: 20 });
    // No run fired as a side effect of the revision itself.
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_run' }).length, 0);
    assert.deepEqual(bus.getStats().autoRemediate, {
      intervalMs: 20,
      maxActions: 10,
      dryRun: false,
      doctor: { slowSubscriberP99Ms: 1000, lagWatermarkDefaultMs: 30000 },
    });
    // Two audits: the enable (old values null) and the revision.
    const revised = bus.getSubscriptionAudit({ action: 'auto_remediate_revised' });
    assert.equal(revised.length, 2);
    assert.equal(revised[0].detail.oldIntervalMs, null);
    assert.deepEqual(revised[1].detail, {
      intervalMs: 20,
      oldIntervalMs: 60_000,
      maxActions: 10,
      oldMaxActions: 10,
      dryRun: false,
      oldDryRun: false,
      doctor: { slowSubscriberP99Ms: 1000, lagWatermarkDefaultMs: 30000 },
      oldDoctor: { slowSubscriberP99Ms: 1000, lagWatermarkDefaultMs: 30000 },
    });
    // The re-armed timer fires at the new interval and resumes it.
    await sleep(300);
    assert.equal(bus.subscriberHealth(sub.id).degraded, false);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_run' }).length >= 1);
    await bus.close();
  });

  it('a maxActions-only revision after the enable leaves the pending run countdown untouched', async () => {
    const bus = new EventBus();
    bus.setAutoRemediate({ intervalMs: 300 });
    const sub = await degradedSubscriber(bus);
    // 200ms into the 300ms countdown, revise maxActions only.
    await sleep(200);
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);
    bus.setAutoRemediate({ maxActions: 5 });
    // 200ms later (400ms total) the run has fired — the countdown was
    // not reset (a re-armed timer would not fire until 500ms).
    await sleep(200);
    assert.equal(bus.subscriberHealth(sub.id).degraded, false);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_run' }).length >= 1);
    await bus.close();
  });

  it('a same-value revision after the enable is a no-op with no audit', async () => {
    const bus = new EventBus();
    bus.setAutoRemediate({
      intervalMs: 5000,
      maxActions: 3,
      dryRun: true,
      doctor: { slowSubscriberP99Ms: 2000, lagWatermarkDefaultMs: 4000 },
    });
    bus.setAutoRemediate({ intervalMs: 5000 });
    bus.setAutoRemediate({ maxActions: 3 });
    bus.setAutoRemediate({ dryRun: true });
    bus.setAutoRemediate({ doctor: { slowSubscriberP99Ms: 2000 } });
    bus.setAutoRemediate({});
    bus.setAutoRemediate(null as never);
    // No re-arm, no audit beyond the enable: the live schedule is
    // byte-identical.
    assert.deepEqual(bus.getStats().autoRemediate, {
      intervalMs: 5000,
      maxActions: 3,
      dryRun: true,
      doctor: { slowSubscriberP99Ms: 2000, lagWatermarkDefaultMs: 4000 },
    });
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 1);
    await bus.close();
  });

  it('a restart restores the unconfigured state', async () => {
    const bus = new EventBus();
    bus.setAutoRemediate({ intervalMs: 50 });
    assert.notEqual(bus.getStats().autoRemediate, undefined);
    await bus.close();
    // A fresh bus built from the same construction options holds no
    // schedule: the enable was live-only.
    const fresh = new EventBus();
    assert.equal(fresh.getStats().autoRemediate, undefined);
    assert.equal(fresh.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
    // The fresh bus can still be enabled the same way.
    fresh.setAutoRemediate({ intervalMs: 50 });
    assert.notEqual(fresh.getStats().autoRemediate, undefined);
    await fresh.close();
  });

  it('a closed bus refuses the enable', async () => {
    const bus = new EventBus();
    await bus.close();
    assert.throws(() => bus.setAutoRemediate({ intervalMs: 100 }), /bus is closed/);
    assert.equal(bus.getStats().autoRemediate, undefined);
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
  });
});
