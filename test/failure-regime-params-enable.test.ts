import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { DeliveryOutcome, FailureRateRegimeChange } from '../src/failure-regime.ts';

/**
 * Failure-rate regime detection parameter enable (EB-543):
 * `reviseFailureRegimeParams` on a bus that never opted into
 * `failureRegime` builds the monitor live — no restart, no lost
 * subscriptions — with the patch resolved over the construction
 * defaults (`delta` 0.05 / `lambda` 5 / `warmupSamples` 50 /
 * `windowSamples` 50; explicit values win, fields left out fall
 * back to the defaults) through the same
 * `ensureFailureRegimeMonitor` wiring construction uses. Enabling
 * installs no callback (`onFailureRateRegimeChange` stays
 * undefined; detection runs audit-only until the EB-542 entry
 * point installs one). Detection starts from the first outcome
 * after the enable — outcomes reported before it were refused, so
 * nothing is traced back — and the enable is live-only: a restart
 * restores the unconfigured state. The enable is audited like any
 * revision, with `oldParams: null`.
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

function collectingBus(): CollectingBus {
  now = 5_000_000;
  clock.nowMs = now;
  const seen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({ now: clock.now });
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

test('the first enable on an unconfigured bus takes all four defaults', () => {
  const bus = new EventBus();
  // The patch must provide at least one field; an explicit default
  // still enables with the full default set.
  const snapshot = bus.reviseFailureRegimeParams({ delta: 0.05 });
  assert.deepEqual(snapshot, { ...DEFAULTS });
  assert.deepEqual(paramsRevisionAudits(bus)[0].detail, {
    params: { ...DEFAULTS },
    oldParams: null,
  });
  // The bus is now configured: reporting works.
  assert.doesNotThrow(() => bus.reportDeliveryOutcome('orders', 'success'));
});

test('a partial patch falls back to the defaults per field', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.reviseFailureRegimeParams({ lambda: 10, windowSamples: 100 }), {
    ...DEFAULTS,
    lambda: 10,
    windowSamples: 100,
  });
  assert.deepEqual(paramsRevisionAudits(bus)[0].detail.params, {
    ...DEFAULTS,
    lambda: 10,
    windowSamples: 100,
  });
  assert.equal(paramsRevisionAudits(bus)[0].detail.oldParams, null);

  // Each field on its own falls back the same way.
  const warmupOnly = new EventBus();
  assert.deepEqual(warmupOnly.reviseFailureRegimeParams({ warmupSamples: 20 }), {
    ...DEFAULTS,
    warmupSamples: 20,
  });
  const full = new EventBus();
  assert.deepEqual(
    full.reviseFailureRegimeParams({ delta: 0.02, lambda: 10, warmupSamples: 20, windowSamples: 30 }),
    { delta: 0.02, lambda: 10, warmupSamples: 20, windowSamples: 30 },
  );
});

test('an empty, unknown, or illegal patch is rejected and the bus stays unconfigured', () => {
  const bus = new EventBus();
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
  assert.equal(paramsRevisionAudits(bus).length, 0, 'failed enables must not audit');
  // Still unconfigured: reporting still refuses — no monitor was
  // half-built by any failed enable.
  assert.throws(
    () => bus.reportDeliveryOutcome('orders', 'success'),
    /requires EventBusOptions.failureRegime/,
  );
  // A later valid call still enables it, with the defaults resolved.
  assert.deepEqual(bus.reviseFailureRegimeParams({ delta: 0.02 }), {
    ...DEFAULTS,
    delta: 0.02,
  });
  assert.equal(paramsRevisionAudits(bus).length, 1);
  assert.equal(paramsRevisionAudits(bus)[0].detail.oldParams, null);
  assert.doesNotThrow(() => bus.reportDeliveryOutcome('orders', 'success'));
});

test('after enabling, the next observe() judges with the new params', () => {
  const control = collectingBus();
  const enabled = collectingBus();
  // The control enables with the defaults; the enabled bus with a
  // tighter delta. Both warm up on a 1% stream: baseline 0.01.
  control.bus.reviseFailureRegimeParams({ delta: 0.05 });
  enabled.bus.reviseFailureRegimeParams({ delta: 0.02 });
  // The callback is installed after the enable, via the EB-542 entry
  // point, so the alarms below are heard.
  control.bus.setFailureRateRegimeChangeCallback((e) => control.seen.push(e));
  enabled.bus.setFailureRateRegimeChangeCallback((e) => enabled.seen.push(e));
  control.report('orders', spread(100, 1));
  enabled.report('orders', spread(100, 1));

  const creep = [spread(100, 3), spread(100, 5), spread(300, 24)];
  const controlAlarms = control.feed('orders', creep);
  const enabledAlarms = enabled.feed('orders', creep);
  assert.equal(controlAlarms.length, 1, 'the default delta alarms once on the creep');
  // Exact alarm points, calibrated against the detector: the default
  // delta needs 225 creep outcomes, the tightened one only 160 —
  // the same points EB-446 calibrated for a construction-time bus,
  // so the live-built monitor judges exactly like a built one.
  assert.equal(controlAlarms[0].at, 225);
  assert.equal(enabledAlarms[0].at, 160);
  assert.ok(enabledAlarms[0].at < controlAlarms[0].at, 'tighter delta alarms earlier');
  assert.deepEqual(enabledAlarms[0].event.before, { failureRate: 0.04, samples: 50 });
  assert.deepEqual(enabledAlarms[0].event.after, { failureRate: 0.06, samples: 50 });
});

test('the callback is still installable afterwards via the EB-542 entry point', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const seen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({ now: clock.now });
  bus.reviseFailureRegimeParams({ delta: 0.02 });
  assert.equal(paramsRevisionAudits(bus).length, 1);

  // Install the callback on the monitor the enable built: the
  // callback audit records a fresh enable, not a swap.
  bus.setFailureRateRegimeChangeCallback((e) => seen.push(e));
  const callbackAudits = bus.getSubscriptionAudit({
    action: 'failure_rate_regime_change_callback_revised',
  });
  assert.equal(callbackAudits.length, 1);
  assert.deepEqual(callbackAudits[0].detail, { hasCallback: true, oldHasCallback: false });

  // The creep alarms twice on the tightened delta: the second alarm
  // is the straddled settling window re-alarming (the same second
  // alarm EB-446 calibrates for a construction-time bus at delta
  // 0.02), not an enable artifact.
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(100, 3));
  report(bus, 'orders', spread(100, 5));
  report(bus, 'orders', spread(300, 24));
  assert.equal(seen.length, 2);
  assert.equal(seen[0].topic, 'orders');
  assert.equal(seen[0].direction, 'increase');
  assert.equal(seen[0].detectedAtMs, 5_000_000);
});

test('a post-enable revision merges over the live enabled values', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.reviseFailureRegimeParams({ delta: 0.02, warmupSamples: 20 }), {
    ...DEFAULTS,
    delta: 0.02,
    warmupSamples: 20,
  });
  // Only the provided field moves; the enabled values are the merge base.
  assert.deepEqual(bus.reviseFailureRegimeParams({ lambda: 10 }), {
    delta: 0.02,
    lambda: 10,
    warmupSamples: 20,
    windowSamples: 50,
  });
  const audits = paramsRevisionAudits(bus);
  assert.equal(audits.length, 2);
  assert.deepEqual(audits[0].detail, {
    params: { ...DEFAULTS, delta: 0.02, warmupSamples: 20 },
    oldParams: null,
  });
  assert.deepEqual(audits[1].detail, {
    params: { delta: 0.02, lambda: 10, warmupSamples: 20, windowSamples: 50 },
    oldParams: { ...DEFAULTS, delta: 0.02, warmupSamples: 20 },
  });
});

test('the enable is live-only: a fresh bus is unconfigured again', () => {
  const bus = new EventBus();
  bus.reviseFailureRegimeParams({ delta: 0.02 });
  assert.doesNotThrow(() => bus.reportDeliveryOutcome('orders', 'success'));

  // A restart restores the unconfigured state: a fresh bus holds no
  // monitor and refuses reporting until enabled again.
  const restarted = new EventBus();
  assert.throws(
    () => restarted.reportDeliveryOutcome('orders', 'success'),
    /requires EventBusOptions.failureRegime/,
  );
  assert.equal(paramsRevisionAudits(restarted).length, 0);
});

test('the enable audits the exact four parameters, oldParams null, and the actor', () => {
  const bus = new EventBus();
  const snapshot = bus.reviseFailureRegimeParams({ delta: 0.02, lambda: 10 }, 'ops');
  assert.deepEqual(snapshot, { ...DEFAULTS, delta: 0.02, lambda: 10 });
  const audit = paramsRevisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].action, 'failure_rate_regime_params_revised');
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail, {
    params: { delta: 0.02, lambda: 10, warmupSamples: 50, windowSamples: 50 },
    oldParams: null,
  });
  // The four fields are exactly the four parameters — no extras —
  // and oldParams is null, not an empty object.
  assert.deepEqual(Object.keys(audit[0].detail.params).sort(), [
    'delta',
    'lambda',
    'warmupSamples',
    'windowSamples',
  ]);
  assert.equal(audit[0].detail.oldParams, null);
});

test('a closed bus refuses the enable with RangeError and stays unconfigured', async () => {
  const bus = new EventBus();
  await bus.close();
  assert.throws(
    () => bus.reviseFailureRegimeParams({ delta: 0.02 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(paramsRevisionAudits(bus).length, 0, 'a refused enable must not audit');
  assert.throws(
    () => bus.reportDeliveryOutcome('orders', 'success'),
    /requires EventBusOptions.failureRegime/,
    'no monitor was built by the refused enable',
  );
});

test('a same-value revision after enabling is a no-op with no second audit', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.reviseFailureRegimeParams({ delta: 0.02 }), {
    ...DEFAULTS,
    delta: 0.02,
  });
  assert.equal(paramsRevisionAudits(bus).length, 1);
  // Re-affirming the enabled parameters — partial or full — is a no-op.
  assert.deepEqual(bus.reviseFailureRegimeParams({ delta: 0.02 }), {
    ...DEFAULTS,
    delta: 0.02,
  });
  assert.deepEqual(
    bus.reviseFailureRegimeParams({ delta: 0.02, lambda: 5, warmupSamples: 50, windowSamples: 50 }),
    { ...DEFAULTS, delta: 0.02 },
  );
  assert.equal(paramsRevisionAudits(bus).length, 1, 'a same-value revision writes no second event');
});

test('enabling installs no callback: detection runs audit-only until one is installed', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const seen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({ now: clock.now });
  bus.reviseFailureRegimeParams({ delta: 0.02 });
  // The creep alarms internally with nobody listening — the detector
  // settles onto the new 8% regime during the audit-only window.
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(100, 3));
  report(bus, 'orders', spread(100, 5));
  report(bus, 'orders', spread(300, 24));
  assert.equal(seen.length, 0, 'no callback fires while audit-only');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'failure_rate_regime_change_callback_revised' }).length,
    0,
    'the params enable must not touch the callback audit',
  );

  // Install the callback: the steady 8% stream must NOT alarm — the
  // baseline re-estimated to 8% while audit-only. If detection had
  // frozen at the enable, the baseline would still be 1% and this
  // would alarm.
  bus.setFailureRateRegimeChangeCallback((e) => seen.push(e));
  report(bus, 'orders', spread(300, 24));
  assert.equal(seen.length, 0, 'the steady new regime does not re-alarm');
  // A fresh jump alarms on the installed callback.
  report(bus, 'orders', spread(30, 15));
  assert.equal(seen.length, 1);
  assert.equal(seen[0].direction, 'increase');
});
