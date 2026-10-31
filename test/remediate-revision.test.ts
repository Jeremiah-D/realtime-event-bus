/**
 * Bus-level doctor auto-remediation schedule revision (EB-306).
 *
 * `setAutoRemediate` retunes the periodic `autoRemediate()` schedule
 * live — without restarting the bus. Only a bus that already enabled
 * the `autoRemediate` opt-in can be revised; illegal values fail
 * closed with the live schedule and the audit log untouched; an
 * identical (or empty) revision is a no-op with no audit. A moved
 * interval clears and re-arms the timer at the new interval with no
 * extra run fired; a maxActions/dryRun-only revision leaves the
 * pending run's countdown untouched and takes effect on the next run.
 * The live schedule is observable via `getStats().autoRemediate`.
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

describe('auto-remediation schedule revision (EB-306)', () => {
  it('setAutoRemediate fails closed when autoRemediate is not enabled', async () => {
    const bus = new EventBus();
    assert.equal(bus.getStats().autoRemediate, undefined);
    assert.throws(() => bus.setAutoRemediate({ intervalMs: 100 }), RangeError);
    assert.throws(() => bus.setAutoRemediate({ maxActions: 1 }), RangeError);
    assert.throws(() => bus.setAutoRemediate({ dryRun: true }), RangeError);
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
    await bus.close();
  });

  it('setAutoRemediate fails closed on a closed bus', async () => {
    const bus = new EventBus({ autoRemediate: { intervalMs: 60_000 } });
    await bus.close();
    assert.throws(() => bus.setAutoRemediate({ intervalMs: 100 }), RangeError);
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
  });

  it('setAutoRemediate rejects illegal values and leaves the live schedule untouched', async () => {
    const bus = new EventBus({ autoRemediate: { intervalMs: 100, maxActions: 1 } });
    assert.deepEqual(bus.getStats().autoRemediate, {
      intervalMs: 100,
      maxActions: 1,
      dryRun: false,
    });
    const bad: Array<unknown> = [
      { intervalMs: 0 },
      { intervalMs: -5 },
      { intervalMs: Number.NaN },
      { intervalMs: Number.POSITIVE_INFINITY },
      { intervalMs: 'fast' },
      { maxActions: -1 },
      { maxActions: 1.5 },
      { maxActions: 'many' },
      { dryRun: 'yes' },
      { dryRun: 1 },
      'nope',
      42,
      ['x'],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setAutoRemediate(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    // The failed calls changed nothing: the live schedule is intact
    // and no revision was audited.
    assert.deepEqual(bus.getStats().autoRemediate, {
      intervalMs: 100,
      maxActions: 1,
      dryRun: false,
    });
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
    // The untouched schedule still fires with the live cap of 1: two
    // degraded subscribers, one run, exactly one resumed. Both are
    // degraded before the first 100ms tick, so the first run plans 2
    // and applies 1 — the audit pins the live cap, not the clock
    // (later runs resume the second, so only the audit is asserted).
    const first = await degradedSubscriber(bus);
    const second = await degradedSubscriber(bus);
    await sleep(400);
    assert.equal(bus.subscriberHealth(first.id).degraded, false);
    assert.equal(bus.subscriberHealth(second.id).degraded, false);
    const runs = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
    const cappedRun = runs.find((r) => r.detail.planned === 2);
    assert.ok(cappedRun !== undefined);
    assert.equal(cappedRun.detail.maxActions, 1);
    assert.equal(cappedRun.detail.applied, 1);
    assert.equal(cappedRun.detail.capped, true);
    await bus.close();
  });

  it('setAutoRemediate with identical values or an empty revision is a no-op with no audit', async () => {
    const bus = new EventBus({
      autoRemediate: { intervalMs: 5_000, maxActions: 3, dryRun: true },
    });
    bus.setAutoRemediate({ intervalMs: 5_000 });
    bus.setAutoRemediate({ maxActions: 3 });
    bus.setAutoRemediate({ dryRun: true });
    bus.setAutoRemediate({ intervalMs: 5_000, maxActions: 3, dryRun: true });
    bus.setAutoRemediate({});
    bus.setAutoRemediate(null as never);
    // No re-arm, no audit: the live schedule is byte-identical.
    assert.deepEqual(bus.getStats().autoRemediate, {
      intervalMs: 5_000,
      maxActions: 3,
      dryRun: true,
    });
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
    await bus.close();
  });

  it('setAutoRemediate partially revises only the fields provided', async () => {
    const bus = new EventBus({ autoRemediate: { intervalMs: 30_000, maxActions: 1 } });
    bus.setAutoRemediate({ maxActions: 5 });
    assert.deepEqual(bus.getStats().autoRemediate, {
      intervalMs: 30_000,
      maxActions: 5,
      dryRun: false,
    });
    const revised = bus.getSubscriptionAudit({ action: 'auto_remediate_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, {
      intervalMs: 30_000,
      oldIntervalMs: 30_000,
      maxActions: 5,
      oldMaxActions: 1,
      dryRun: false,
      oldDryRun: false,
    });
    await bus.close();
  });

  it('setAutoRemediate re-arms the timer at the new interval', async () => {
    const bus = new EventBus({ autoRemediate: { intervalMs: 60_000 } });
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
    });
    const revised = bus.getSubscriptionAudit({ action: 'auto_remediate_revised' });
    assert.equal(revised.length, 1);
    assert.deepEqual(revised[0].detail, {
      intervalMs: 20,
      oldIntervalMs: 60_000,
      maxActions: 10,
      oldMaxActions: 10,
      dryRun: false,
      oldDryRun: false,
    });
    // The re-armed timer fires at the new interval and resumes it.
    await sleep(300);
    assert.equal(bus.subscriberHealth(sub.id).degraded, false);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_run' }).length >= 1);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_resume' }).length >= 1);
    await bus.close();
  });

  it('a dryRun revision takes effect on the next scheduled run', async () => {
    const bus = new EventBus({ autoRemediate: { intervalMs: 20 } });
    const sub = await degradedSubscriber(bus);
    bus.setAutoRemediate({ dryRun: true });
    assert.deepEqual(bus.getStats().autoRemediate, {
      intervalMs: 20,
      maxActions: 10,
      dryRun: true,
    });
    // Runs fire, but only preview: the subscriber stays degraded.
    await sleep(300);
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);
    const runs = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
    assert.ok(runs.length >= 1);
    assert.equal(runs[0].detail.dryRun, true);
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_resume' }).length, 0);
    // Switching dryRun back off resumes it on the next run.
    bus.setAutoRemediate({ dryRun: false });
    await sleep(300);
    assert.equal(bus.subscriberHealth(sub.id).degraded, false);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_resume' }).length >= 1);
    await bus.close();
  });

  it('a maxActions revision takes effect on the next scheduled run', async () => {
    const bus = new EventBus({ autoRemediate: { intervalMs: 60_000, maxActions: 1 } });
    // Both degraded before any run fires (60s schedule), so the first
    // run after arming plans both under the live cap of 1.
    await degradedSubscriber(bus);
    await degradedSubscriber(bus);
    bus.setAutoRemediate({ intervalMs: 20 });
    await sleep(300);
    const runs = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
    const cappedRun = runs.find((r) => r.detail.planned === 2);
    assert.ok(cappedRun !== undefined);
    assert.equal(cappedRun.detail.maxActions, 1);
    assert.equal(cappedRun.detail.applied, 1);
    assert.equal(cappedRun.detail.capped, true);
    // Raising the cap: the next run that plans anything carries the
    // new cap and is no longer capped.
    const third = await degradedSubscriber(bus);
    bus.setAutoRemediate({ maxActions: 5 });
    await sleep(300);
    assert.equal(bus.subscriberHealth(third.id).degraded, false);
    const laterRuns = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
    const uncappedRun = laterRuns.find(
      (r) => r.detail.maxActions === 5 && r.detail.planned >= 1,
    );
    assert.ok(uncappedRun !== undefined);
    assert.equal(uncappedRun.detail.capped, false);
    await bus.close();
  });

  it('a maxActions-only revision leaves the pending run countdown untouched', async () => {
    const bus = new EventBus({ autoRemediate: { intervalMs: 300 } });
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
});
