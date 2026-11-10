import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { DeliveryOutcome, FailureRateRegimeChange } from '../src/failure-regime.ts';

/**
 * Failure-rate regime detection parameter revision (EB-446):
 * `reviseFailureRegimeParams` live-retunes `delta` / `lambda` /
 * `warmupSamples` / `windowSamples` without restarting the bus.
 * The monitor fans the retune out to every already-built detector
 * (and stores it for topics built later) — retuning only the
 * monitor's stored options would leave already-built detectors on
 * the old parameters (the WR-372 tracker-capture trap). Only the
 * parameters move: the callback identity (EB-445), the baselines,
 * the settling counters, the sums, and any already-raised change
 * points are untouched, and a revision inside the settling window
 * does not reset the settling count.
 */

// Deterministic clock: tests set `now` explicitly.
let now = 5_000_000;
const clock = { nowMs: now, now: () => clock.nowMs };

/**
 * Deterministic outcome stream: exactly `badCount` failures evenly
 * spread over `total` observations (a failure every `total/badCount`
 * samples, first failure at the end of the first interval). Successes
 * fill the rest. (Same shape as `failure-regime.test.ts`.)
 */
function spread(total: number, badCount: number, bad: DeliveryOutcome = 'failure'): DeliveryOutcome[] {
  const out: DeliveryOutcome[] = Array.from({ length: total }, () => 'success');
  if (badCount <= 0) return out;
  const step = total / badCount;
  for (let i = 0; i < badCount; i++) {
    out[Math.min(total - 1, Math.floor((i + 1) * step) - 1)] = bad;
  }
  return out;
}

function report(bus: EventBus, topic: string, outcomes: DeliveryOutcome[]): void {
  for (const o of outcomes) bus.reportDeliveryOutcome(topic, o);
}

interface CollectingBus {
  bus: EventBus;
  seen: FailureRateRegimeChange[];
  /** Feeds outcomes, returning every alarm with its 1-based index within the fed chunks. */
  feed(topic: string, chunks: DeliveryOutcome[][]): { at: number; event: FailureRateRegimeChange }[];
  report(topic: string, outcomes: DeliveryOutcome[]): void;
}

function collectingBus(
  failureRegime: Record<string, unknown> = {},
): CollectingBus {
  now = 5_000_000;
  clock.nowMs = now;
  const seen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({
    now: clock.now,
    failureRegime: {
      ...failureRegime,
      onFailureRateRegimeChange: (e) => seen.push(e),
    },
  });
  let fed = 0;
  return {
    bus,
    seen,
    feed(topic, chunks) {
      const alarms: { at: number; event: FailureRateRegimeChange }[] = [];
      for (const chunk of chunks) {
        for (const o of chunk) {
          fed += 1;
          const at = fed;
          const mark = seen.length;
          bus.reportDeliveryOutcome(topic, o);
          if (seen.length > mark) {
            alarms.push({ at, event: seen[seen.length - 1] });
          }
        }
      }
      return alarms;
    },
    report(topic, outcomes) {
      report(bus, topic, outcomes);
    },
  };
}

/** The `failure_rate_regime_params_revised` audit events on the bus. */
function paramsRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'failure_rate_regime_params_revised' });
}

const DEFAULTS = { delta: 0.05, lambda: 5, warmupSamples: 50, windowSamples: 50 };

test('a tighter delta makes a 1% -> 8% climb alarm earlier, with exact before/after', () => {
  const control = collectingBus();
  const revised = collectingBus();
  // Both warm up on a 1% stream: baseline 0.01.
  control.report('orders', spread(100, 1));
  revised.report('orders', spread(100, 1));

  const snapshot = revised.bus.reviseFailureRegimeParams({ delta: 0.02 });
  assert.deepEqual(snapshot, { ...DEFAULTS, delta: 0.02 });

  const creep = [spread(100, 3), spread(100, 5), spread(300, 24)];
  const controlAlarms = control.feed('orders', creep);
  const revisedAlarms = revised.feed('orders', creep);
  assert.equal(controlAlarms.length, 1, 'the default delta alarms once on the creep');
  // Exact alarm points, calibrated against the detector: the default
  // delta needs 225 creep outcomes, the tightened one only 160.
  assert.equal(controlAlarms[0].at, 225);
  assert.equal(revisedAlarms[0].at, 160);
  assert.ok(revisedAlarms[0].at < controlAlarms[0].at, 'tighter delta alarms earlier');
  // The tightened bus alarms a second time later in the creep: its
  // settling window straddled the 5% -> 8% step, so it settled onto
  // a ~4% baseline while the stream runs steady at 8% — a further
  // shift inside the settling window is still detected after
  // settling, against the confirmed baseline (the module doc's
  // stated settling semantics), not a revision artifact. The
  // control's settling window sat wholly in the 8% stream, so it
  // settles onto 8% and stays quiet.
  assert.equal(revisedAlarms.length, 2, 'the straddled settling window re-alarms once');
  for (const alarms of [controlAlarms, revisedAlarms]) {
    assert.equal(alarms[0].event.topic, 'orders');
    assert.equal(alarms[0].event.direction, 'increase');
    assert.equal(alarms[0].event.detectedAtMs, 5_000_000);
  }
  assert.deepEqual(controlAlarms[0].event.before, { failureRate: 0.04, samples: 50 });
  assert.deepEqual(controlAlarms[0].event.after, { failureRate: 0.08, samples: 50 });
  assert.deepEqual(revisedAlarms[0].event.before, { failureRate: 0.04, samples: 50 });
  assert.deepEqual(revisedAlarms[0].event.after, { failureRate: 0.06, samples: 50 });
});

test('a higher lambda suppresses a small burst but still catches a sustained shift', () => {
  const { bus, seen } = collectingBus();
  report(bus, 'orders', spread(100, 1));
  const snapshot = bus.reviseFailureRegimeParams({ lambda: 10 });
  assert.deepEqual(snapshot, { ...DEFAULTS, lambda: 10 });

  // Six consecutive failures: under the default lambda (5) this
  // alarms on the 6th failure; under lambda 10 it is absorbed.
  report(bus, 'orders', Array.from({ length: 6 }, () => 'failure' as DeliveryOutcome));
  assert.equal(seen.length, 0, 'a 6-failure burst must not alarm under lambda 10');

  // Five more failures (11 in a row) cross even the raised threshold.
  report(bus, 'orders', Array.from({ length: 5 }, () => 'failure' as DeliveryOutcome));
  assert.equal(seen.length, 1, 'a sustained 11-failure run still alarms');
  assert.equal(seen[0].direction, 'increase');
  assert.deepEqual(seen[0].before, { failureRate: 0, samples: 50 });
  assert.deepEqual(seen[0].after, { failureRate: 1, samples: 11 });
});

test('a not-yet-built topic uses the new parameters from its first alarm', () => {
  const revised = collectingBus();
  const control = collectingBus();
  revised.bus.reviseFailureRegimeParams({ delta: 0.02 });
  // 'fresh' has never been reported on either bus: its detector is
  // built after the revision on the revised bus.
  const stream = [spread(100, 1), spread(100, 3), spread(100, 5)];
  const revisedAlarms = revised.feed('fresh', stream);
  const controlAlarms = control.feed('fresh', stream);
  assert.equal(revisedAlarms.length, 1, 'the later-built detector alarms under the new delta');
  assert.equal(revisedAlarms[0].at, 260);
  assert.deepEqual(revisedAlarms[0].event.before, { failureRate: 0.04, samples: 50 });
  assert.deepEqual(revisedAlarms[0].event.after, { failureRate: 0.06, samples: 50 });
  assert.equal(controlAlarms.length, 0, 'the default delta needs 25 more outcomes to alarm');
});

test('raising warmupSamples mid-warmup extends baseline estimation', () => {
  // A bus built with warmupSamples 200 from construction is the
  // control: the revised bus must behave identically to it.
  const control = collectingBus({ warmupSamples: 200 });
  const revised = collectingBus();
  for (const c of [control, revised]) {
    c.report('orders', Array.from({ length: 49 }, () => 'success' as DeliveryOutcome));
  }
  const snapshot = revised.bus.reviseFailureRegimeParams({ warmupSamples: 200 });
  assert.deepEqual(snapshot, { ...DEFAULTS, warmupSamples: 200 });

  // 6 failures + 145 successes complete the 200-sample warmup.
  // Under the old warmup (50) the baseline would have finalized at
  // 0 after 50 samples and the 6-failure burst would have alarmed;
  // under the extended warmup these are just baseline samples.
  for (const c of [control, revised]) {
    c.report('orders', Array.from({ length: 6 }, () => 'failure' as DeliveryOutcome));
    c.report('orders', Array.from({ length: 145 }, () => 'success' as DeliveryOutcome));
    assert.equal(c.seen.length, 0, 'no alarm while the extended warmup runs');
  }
  // Both baselines are now 6/200 = 0.03: a jump to all-failures
  // alarms on the 6th failure on both buses, identically.
  const controlAlarms = control.feed('orders', [Array.from({ length: 30 }, () => 'failure' as DeliveryOutcome)]);
  const revisedAlarms = revised.feed('orders', [Array.from({ length: 30 }, () => 'failure' as DeliveryOutcome)]);
  assert.equal(controlAlarms.length, 1);
  assert.equal(revisedAlarms.length, 1);
  assert.equal(controlAlarms[0].at, 6);
  assert.equal(revisedAlarms[0].at, 6);
  assert.deepEqual(revisedAlarms[0].event, controlAlarms[0].event);
});

test('lowering warmupSamples to the sampled count finalizes the baseline immediately', () => {
  const { bus, seen } = collectingBus();
  // 30 samples in hand (1 failure): still warming up under the
  // default 50, so the mean is not yet fixed.
  report(bus, 'orders', [...Array.from({ length: 29 }, () => 'success' as DeliveryOutcome), 'failure' as DeliveryOutcome]);
  assert.equal(seen.length, 0);
  const snapshot = bus.reviseFailureRegimeParams({ warmupSamples: 20 });
  assert.deepEqual(snapshot, { ...DEFAULTS, warmupSamples: 20 });

  // The baseline is now fixed at 1/30 at once: 6 failures alarm
  // immediately. Without the finalize these would be warmup samples
  // (n = 30..36 < 50) and detection would stall for 14 more samples.
  report(bus, 'orders', Array.from({ length: 6 }, () => 'failure' as DeliveryOutcome));
  assert.equal(seen.length, 1, 'detection must be live right after the finalize');
  assert.equal(seen[0].direction, 'increase');
  assert.equal(seen[0].before.samples, 30);
  assert.ok(
    Math.abs(seen[0].before.failureRate - 1 / 30) < 1e-12,
    `before rate ${seen[0].before.failureRate} should be the 1-in-30 sampled mean`,
  );
  assert.deepEqual(seen[0].after, { failureRate: 1, samples: 6 });
});

test('an already-warmed detector changing warmupSamples keeps its live baseline', () => {
  const control = collectingBus();
  const revised = collectingBus();
  for (const c of [control, revised]) {
    c.report('orders', spread(100, 1));
  }
  const snapshot = revised.bus.reviseFailureRegimeParams({ warmupSamples: 200 });
  assert.deepEqual(snapshot, { ...DEFAULTS, warmupSamples: 200 });

  // The creep must alarm at exactly the same outcome on both buses,
  // with identical snapshots: the warmed detector's baseline (0.01)
  // is untouched by the warmupSamples change.
  const creep = [spread(100, 3), spread(100, 5), spread(300, 24)];
  const controlAlarms = control.feed('orders', creep);
  const revisedAlarms = revised.feed('orders', creep);
  assert.equal(controlAlarms.length, 1);
  assert.equal(revisedAlarms.length, 1);
  assert.equal(revisedAlarms[0].at, controlAlarms[0].at);
  assert.equal(revisedAlarms[0].at, 225);
  assert.deepEqual(revisedAlarms[0].event, controlAlarms[0].event);
});

test('a larger windowSamples lengthens settling and enlarges the snapshot sample count', () => {
  const { bus, seen } = collectingBus();
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(30, 15));
  assert.equal(seen.length, 1, 'first alarm puts the detector into settling');

  // 10 more settling samples go by (~30 of 50 spent: the alarm chunk
  // itself left 20 settling samples behind).
  report(bus, 'orders', spread(10, 5));
  const snapshot = bus.reviseFailureRegimeParams({ windowSamples: 100 });
  assert.deepEqual(snapshot, { ...DEFAULTS, windowSamples: 100 });

  // 30 more settling samples (~60 total): settling would have
  // completed at 50 under the old window. An 11-failure probe now
  // must still be swallowed — settling needs 100 samples — where
  // under the old window it would alarm (11 x (1 - 0.5 - 0.025)
  // clears lambda 5). The probe's failures join the settling
  // window, so the tail below rebalances the settled mean to 0.51.
  report(bus, 'orders', spread(30, 15));
  report(bus, 'orders', Array.from({ length: 11 }, () => 'failure' as DeliveryOutcome));
  assert.equal(seen.length, 1, 'still settling at ~71 samples under the widened window');

  // The remaining 29 settling samples complete the 100-sample
  // window at a 51% mean; a steady 50% stream then stays quiet
  // (the mean is within delta/2 of it), and a jump alarms with a
  // 100-sample before-window.
  report(bus, 'orders', spread(29, 10));
  report(bus, 'orders', spread(30, 15));
  assert.equal(seen.length, 1, 'the steady settled regime does not re-alarm');
  report(bus, 'orders', Array.from({ length: 30 }, () => 'failure' as DeliveryOutcome));
  assert.equal(seen.length, 2, 'the jump alarms once settling completed');
  assert.equal(seen[1].direction, 'increase');
  assert.equal(seen[1].before.samples, 100, 'the widened window keeps 100 before-samples');
  assert.ok(
    Math.abs(seen[1].before.failureRate - 0.51) < 1e-12,
    `before rate ${seen[1].before.failureRate} should be the settled 51% regime`,
  );
  assert.deepEqual(seen[1].after, { failureRate: 1, samples: 11 });
});

test('an unconfigured bus throws a must-restart Error', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.reviseFailureRegimeParams({ delta: 0.02 }),
    (e: unknown) =>
      e instanceof Error &&
      !(e instanceof RangeError) &&
      /failureRegime is not configured — enabling it requires a restart/.test((e as Error).message),
  );
  assert.equal(paramsRevisionAudits(bus).length, 0, 'a refused revision must not audit');
  // A configured bus (even with no callback) accepts the revision
  // and reports the resolved defaults.
  const enabled = new EventBus({ failureRegime: {} });
  assert.deepEqual(enabled.reviseFailureRegimeParams({ lambda: 10 }), {
    ...DEFAULTS,
    lambda: 10,
  });
  assert.equal(paramsRevisionAudits(enabled).length, 1);
});

test('an empty patch, illegal values, and unknown fields throw RangeError and leave live state untouched', () => {
  const { bus, seen } = collectingBus();
  report(bus, 'orders', spread(100, 1));
  const bad: unknown[] = [
    {},
    { delta: 0 },
    { delta: 1 },
    { delta: -0.1 },
    { delta: Number.NaN },
    { delta: 'fast' },
    { lambda: 0 },
    { lambda: -1 },
    { lambda: Number.POSITIVE_INFINITY },
    { warmupSamples: 0 },
    { warmupSamples: 1.5 },
    { warmupSamples: -3 },
    { windowSamples: 1 },
    { windowSamples: 2.5 },
    { windowSamples: 0 },
    { bogus: 1 },
    { delta: 0.02, bogus: 1 },
    null,
    [],
    'nope',
    42,
  ];
  for (const patch of bad) {
    assert.throws(
      () => bus.reviseFailureRegimeParams(patch as never),
      RangeError,
      `patch ${JSON.stringify(patch)} must throw RangeError`,
    );
  }
  assert.equal(paramsRevisionAudits(bus).length, 0, 'failed revisions must not audit');

  // The live parameters are untouched: the creep still alarms at the
  // default-delta point, with default-delta snapshots.
  const creep = [spread(100, 3), spread(100, 5), spread(300, 24)];
  let at = 0;
  let alarm: FailureRateRegimeChange | undefined;
  outer: for (const chunk of creep) {
    for (const o of chunk) {
      at += 1;
      const mark = seen.length;
      bus.reportDeliveryOutcome('orders', o);
      if (seen.length > mark) {
        alarm = seen[seen.length - 1];
        break outer;
      }
    }
  }
  assert.equal(at, 225, 'the live delta is still the default 0.05');
  assert.deepEqual(alarm?.before, { failureRate: 0.04, samples: 50 });
  assert.deepEqual(alarm?.after, { failureRate: 0.08, samples: 50 });
  // And a later valid revision still merges over the live defaults.
  assert.deepEqual(bus.reviseFailureRegimeParams({ delta: 0.02 }), {
    ...DEFAULTS,
    delta: 0.02,
  });
  assert.equal(paramsRevisionAudits(bus).length, 1);
});

test('a same-value patch is a no-op and is not audited', () => {
  const bus = new EventBus({
    failureRegime: { delta: 0.02, lambda: 10, warmupSamples: 20, windowSamples: 30 },
  });
  const live = { delta: 0.02, lambda: 10, warmupSamples: 20, windowSamples: 30 };
  assert.deepEqual(bus.reviseFailureRegimeParams({ ...live }), live);
  assert.deepEqual(bus.reviseFailureRegimeParams({ delta: 0.02 }), live);
  assert.deepEqual(bus.reviseFailureRegimeParams({ windowSamples: 30 }), live);
  assert.equal(paramsRevisionAudits(bus).length, 0, 'same-value revisions write no audit event');
  // A real change audits exactly once.
  assert.deepEqual(bus.reviseFailureRegimeParams({ delta: 0.03 }), { ...live, delta: 0.03 });
  assert.equal(paramsRevisionAudits(bus).length, 1);
});

test('the audit event carries the exact four parameters before and after', () => {
  const bus = new EventBus({ failureRegime: {} });
  const snapshot = bus.reviseFailureRegimeParams({ delta: 0.02, lambda: 10 }, 'system');
  assert.deepEqual(snapshot, { ...DEFAULTS, delta: 0.02, lambda: 10 });
  const audit = paramsRevisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].action, 'failure_rate_regime_params_revised');
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, {
    params: { delta: 0.02, lambda: 10, warmupSamples: 50, windowSamples: 50 },
    oldParams: { ...DEFAULTS },
  });
  // The four fields are exactly the four parameters — no extras.
  assert.deepEqual(Object.keys(audit[0].detail.params).sort(), [
    'delta',
    'lambda',
    'warmupSamples',
    'windowSamples',
  ]);
  assert.deepEqual(Object.keys(audit[0].detail.oldParams).sort(), [
    'delta',
    'lambda',
    'warmupSamples',
    'windowSamples',
  ]);
  // A second partial revision snapshots the new live values as old.
  bus.reviseFailureRegimeParams({ windowSamples: 100 });
  assert.deepEqual(paramsRevisionAudits(bus)[1].detail, {
    params: { delta: 0.02, lambda: 10, warmupSamples: 50, windowSamples: 100 },
    oldParams: { delta: 0.02, lambda: 10, warmupSamples: 50, windowSamples: 50 },
  });
});

test('revising parameters does not disturb the EB-445 callback identity', () => {
  const { bus, seen } = collectingBus();
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(30, 15));
  assert.equal(seen.length, 1, 'the original callback hears the first alarm');

  // Retune (and separately prove the EB-445 entry still redirects):
  // the next alarm must still reach the original callback.
  bus.reviseFailureRegimeParams({ delta: 0.02 });
  report(bus, 'orders', spread(50, 25));
  report(bus, 'orders', spread(30, 30));
  assert.equal(seen.length, 2, 'the next alarm still reaches the original callback');
  assert.equal(seen[1].topic, 'orders');
  assert.equal(seen[1].direction, 'increase');
  assert.equal(paramsRevisionAudits(bus).length, 1, 'the params revision audited once');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'failure_rate_regime_change_callback_revised' }).length,
    0,
    'the params revision must not touch the callback audit',
  );
});

test('revising parameters inside the settling window does not reset settling or the baseline', () => {
  const { bus, seen } = collectingBus();
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(30, 15));
  assert.equal(seen.length, 1, 'first alarm puts the detector into settling');

  // 30 of the 50 settling samples go by.
  report(bus, 'orders', spread(30, 15));
  // Retune mid-settling, then feed the remaining 20 settling samples:
  // settling completes on the 50th sample only if the count survived.
  bus.reviseFailureRegimeParams({ delta: 0.02 });
  report(bus, 'orders', spread(20, 10));
  // A jump now must alarm. If the revision had reset the settling
  // count (or the baseline), these 30 samples would be swallowed by
  // the restarted settling window and no alarm would fire.
  report(bus, 'orders', Array.from({ length: 30 }, () => 'failure' as DeliveryOutcome));
  assert.equal(seen.length, 2, 'the revision must not reset settling');
  assert.equal(seen[1].direction, 'increase');
  assert.ok(
    seen[1].before.failureRate >= 0.4 && seen[1].before.failureRate <= 0.6,
    `before rate ${seen[1].before.failureRate} should still be the settled 50% regime`,
  );
  assert.ok(seen[1].after.failureRate >= 0.9, `after rate ${seen[1].after.failureRate} should be the jump`);
});

test('a closed bus refuses the revision', async () => {
  const bus = new EventBus({ failureRegime: {} });
  await bus.close();
  assert.throws(() => bus.reviseFailureRegimeParams({ delta: 0.02 }), RangeError);
  assert.equal(paramsRevisionAudits(bus).length, 0, 'a refused revision must not audit');
});
