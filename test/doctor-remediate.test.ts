import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';
import { planRemediation } from '../src/remediate.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
function freshDir(): string {
  return mkdtempSync(join(tmpdir(), 'eb-remediate-'));
}

describe('doctor auto-remediation (EB-211)', () => {
  it('does nothing on a healthy bus but audits the run', () => {
    const bus = new EventBus();
    const report = bus.autoRemediate();
    assert.equal(report.healthBefore, 'healthy');
    assert.deepEqual(report.planned, []);
    assert.deepEqual(report.results, []);
    assert.equal(report.capped, false);
    assert.equal(report.dryRun, false);
    assert.equal(report.maxActions, 10);
    const runs = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
    assert.equal(runs.length, 1);
    assert.equal(runs[0].detail?.dryRun, false);
    assert.equal(runs[0].detail?.applied, 0);
    assert.equal(runs[0].detail?.healthBefore, 'healthy');
  });

  it('resumes a stuck degraded subscriber', async () => {
    const bus = new EventBus();
    let calls = 0;
    const sub = bus.subscribe(
      't',
      () => {
        calls += 1;
        throw new Error('boom');
      },
      { healthProbe: { maxConsecutiveFailures: 2 } },
    );
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    await flush();
    assert.equal(calls, 2);
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);

    const report = bus.autoRemediate();
    assert.equal(report.planned.length, 1);
    assert.equal(report.planned[0].kind, 'resume-subscriber');
    assert.equal(report.planned[0].target, sub.id);
    assert.equal(report.results[0].applied, true);
    assert.deepEqual(bus.subscriberHealth(sub.id), {
      subscriberId: sub.id,
      enabled: true,
      degraded: false,
      consecutiveFailures: 0,
    });
    const audits = bus.getSubscriptionAudit({ action: 'auto_remediate_resume' });
    assert.equal(audits.length, 1);
    assert.equal(audits[0].subId, sub.id);
  });

  it('skips degraded subscribers with an armed auto-resume cooldown', async () => {
    const bus = new EventBus();
    const sub = bus.subscribe(
      't',
      () => {
        throw new Error('boom');
      },
      { healthProbe: { maxConsecutiveFailures: 2, autoResumeAfterMs: 60_000 } },
    );
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    await flush();
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);
    // The probe's own cooldown will recover it — the remediator stays out.
    const report = bus.autoRemediate();
    assert.deepEqual(report.planned, []);
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  });

  it('prunes expired linger windows', async () => {
    let now = 1_000_000;
    const bus = new EventBus({ durableLogDir: freshDir(), now: () => now });
    const leaver = bus.subscribeToGroup('g', 'jobs.*', () => {}, { handoffLingerMs: 30_000 });
    bus.subscribeToGroup('g', 'jobs.*', () => {});
    for (let i = 0; i < 5; i++) bus.publish('jobs.email', {});
    await flush();
    bus.commitOffset('g', 'jobs.email', 3);
    leaver.unsubscribe();

    // Window is live (until = now + 30s): nothing to prune.
    let report = bus.autoRemediate();
    assert.ok(!report.planned.some((a) => a.kind === 'prune-linger-windows'));

    // Expire it.
    now += 60_000;
    report = bus.autoRemediate();
    const prune = report.planned.find((a) => a.kind === 'prune-linger-windows');
    assert.ok(prune !== undefined);
    assert.equal(prune.target, 'g');
    assert.equal(report.results.find((r) => r.kind === 'prune-linger-windows')?.applied, true);
    const audits = bus.getSubscriptionAudit({ action: 'auto_remediate_linger_pruned' });
    assert.equal(audits.length, 1);
    assert.equal(audits[0].groupId, 'g');
    assert.equal(audits[0].detail?.pruned, 1);
  });

  it('forces a rebalance for a skewed partitioned group', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m = bus.subscribeToGroup('g', 't.*', () => {}, {
      partitions: 4,
      onRebalance: (e) => events.push(e),
    });
    events.length = 0;
    const report = bus.autoRemediate();
    const reb = report.planned.find((a) => a.kind === 'rebalance-group');
    assert.ok(reb !== undefined);
    assert.equal(reb.target, 'g');
    assert.equal(reb.pattern, 't.*');
    assert.equal(report.results.find((r) => r.kind === 'rebalance-group')?.applied, true);
    const remediate = events.filter((e) => e.trigger === 'remediate');
    assert.equal(remediate.length, 1);
    assert.equal(remediate[0].memberId, '');
    assert.ok(remediate[0].partitionRebalance !== undefined);
    assert.equal(Object.keys(remediate[0].partitionRebalance.assignment).length, 4);
    const audits = bus.getSubscriptionAudit({ action: 'auto_remediate_rebalance' });
    assert.equal(audits.length, 1);
    assert.equal(audits[0].groupId, 'g');
    m.unsubscribe();
  });

  it('dry-run previews without applying', async () => {
    const bus = new EventBus();
    const sub = bus.subscribe(
      't',
      () => {
        throw new Error('boom');
      },
      { healthProbe: { maxConsecutiveFailures: 2 } },
    );
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    await flush();
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);

    const report = bus.autoRemediate({ dryRun: true });
    assert.equal(report.dryRun, true);
    assert.equal(report.planned.length, 1);
    assert.equal(report.results[0].applied, false);
    assert.equal(report.results[0].skippedReason, 'dry-run');
    // Untouched.
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);
    const runs = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
    assert.equal(runs.length, 1);
    assert.equal(runs[0].detail?.dryRun, true);
    assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_resume' }).length, 0);
  });

  it('caps applied actions at maxActions', async () => {
    const bus = new EventBus();
    const subs = [];
    for (let i = 0; i < 3; i++) {
      const sub = bus.subscribe(
        `t${i}`,
        () => {
          throw new Error('boom');
        },
        { healthProbe: { maxConsecutiveFailures: 1 } },
      );
      subs.push(sub);
      bus.publish(`t${i}`, 'x');
    }
    await flush();
    for (const s of subs) assert.equal(bus.subscriberHealth(s.id).degraded, true);

    const report = bus.autoRemediate({ maxActions: 2 });
    assert.equal(report.planned.length, 3);
    assert.equal(report.capped, true);
    assert.equal(report.results.filter((r) => r.applied).length, 2);
    const capped = report.results.filter((r) => !r.applied);
    assert.equal(capped.length, 1);
    assert.equal(capped[0].skippedReason, 'capped by maxActions=2');
    // Two resumed, one still degraded.
    assert.equal(subs.filter((s) => bus.subscriberHealth(s.id).degraded).length, 1);
    const runs = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
    assert.equal(runs[0].detail?.capped, true);
    assert.equal(runs[0].detail?.applied, 2);
  });

  it('a zero maxActions plans but applies nothing', async () => {
    const bus = new EventBus();
    const sub = bus.subscribe(
      't',
      () => {
        throw new Error('boom');
      },
      { healthProbe: { maxConsecutiveFailures: 1 } },
    );
    bus.publish('t', 'x');
    await flush();
    const report = bus.autoRemediate({ maxActions: 0 });
    assert.equal(report.planned.length, 1);
    assert.equal(report.capped, true);
    assert.equal(report.results[0].applied, false);
    assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  });

  it('rejects an invalid maxActions', () => {
    const bus = new EventBus();
    assert.throws(() => bus.autoRemediate({ maxActions: -1 }), RangeError);
    assert.throws(() => bus.autoRemediate({ maxActions: 1.5 }), RangeError);
  });

  it('rejects an invalid autoRemediate schedule in the constructor', () => {
    assert.throws(() => new EventBus({ autoRemediate: { intervalMs: 0 } }), RangeError);
    assert.throws(() => new EventBus({ autoRemediate: { intervalMs: -100 } }), RangeError);
    assert.throws(() => new EventBus({ autoRemediate: { intervalMs: NaN } }), RangeError);
    assert.throws(
      () => new EventBus({ autoRemediate: { intervalMs: 1000, maxActions: -1 } }),
      RangeError,
    );
  });

  it('runs remediation on the configured schedule', async () => {
    const bus = new EventBus({ autoRemediate: { intervalMs: 20 } });
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
    // A scheduled run resumes it without any explicit call.
    await sleep(120);
    assert.equal(bus.subscriberHealth(sub.id).degraded, false);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_run' }).length >= 1);
    assert.ok(bus.getSubscriptionAudit({ action: 'auto_remediate_resume' }).length >= 1);
    await bus.close();
  });

  it('planRemediation is pure: orders by blast radius, skips armed subscribers', () => {
    const planned = planRemediation({
      report: { status: 'degraded', at: 0, checks: [] },
      degradedSubscribers: [
        { subscriberId: 'armed', pattern: 't', consecutiveFailures: 5, autoResumeArmed: true },
        { subscriberId: 'stuck', pattern: 't', consecutiveFailures: 5, autoResumeArmed: false },
      ],
      groupsWithExpiredLinger: [{ groupId: 'g1', expiredWindows: 2 }],
      skewedGroups: [{ groupId: 'g2', pattern: 'p', partitions: 4, members: 1 }],
    });
    assert.deepEqual(
      planned.map((a) => a.kind),
      ['resume-subscriber', 'prune-linger-windows', 'rebalance-group'],
    );
    assert.equal(planned[0].target, 'stuck');
    assert.equal(planned[2].pattern, 'p');
    assert.ok(planned[0].reason.length > 0);
  });
});
