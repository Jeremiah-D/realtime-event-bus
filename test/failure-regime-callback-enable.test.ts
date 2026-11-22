import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { DeliveryOutcome, FailureRateRegimeChange } from '../src/failure-regime.ts';

/**
 * Failure-rate regime-change alert callback live enable (EB-542):
 * `setFailureRateRegimeChangeCallback` on a bus that never opted
 * into `failureRegime` builds the monitor live — no restart, no lost
 * subscriptions — with the construction defaults (`delta` 0.05 /
 * `lambda` 5 / `warmupSamples` 50 / `windowSamples` 50) on the bus
 * clock, through the same `ensureFailureRegimeMonitor` wiring
 * construction uses. A function arms alerting; `null` enables
 * detection audit-only. Detection starts from the first outcome
 * after the enable — outcomes reported before it were refused, so
 * nothing is traced back — and the enable is live-only: a restart
 * restores the unconfigured state.
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

/** The `failure_rate_regime_change_callback_revised` audit events on the bus. */
function callbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'failure_rate_regime_change_callback_revised' });
}

/** The 1% -> 8% creep that alarms exactly once on a default monitor. */
function creep(bus: EventBus, topic: string): void {
  report(bus, topic, spread(100, 1));
  report(bus, topic, spread(100, 3));
  report(bus, topic, spread(100, 5));
  report(bus, topic, spread(300, 24));
}

test('an unconfigured bus enables on the first call and the new callback hears the next creep alarm', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const seen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({ now: clock.now });
  bus.setFailureRateRegimeChangeCallback((e) => seen.push(e));
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: true,
    oldHasCallback: false,
  });

  creep(bus, 'orders');
  assert.equal(seen.length, 1, 'the enabled monitor alarms on the 1% -> 8% creep');
  assert.equal(seen[0].topic, 'orders');
  assert.equal(seen[0].direction, 'increase');
  assert.equal(seen[0].detectedAtMs, 5_000_000);
});

test('enabling with null runs detection audit-only and a later reinstall hears only the next shift', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const seen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({ now: clock.now });
  bus.setFailureRateRegimeChangeCallback(null);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: false,
    oldHasCallback: false,
  });

  // The creep alarms internally with nobody listening — the detector
  // settles onto the new 8% regime during the audit-only window.
  creep(bus, 'orders');
  assert.equal(seen.length, 0, 'no callback fires while audit-only');

  // Re-install: the steady 8% stream must NOT alarm — the baseline
  // re-estimated to 8% while audit-only. If detection had frozen at
  // the enable, the baseline would still be 1% and this would alarm.
  bus.setFailureRateRegimeChangeCallback((e) => seen.push(e));
  report(bus, 'orders', spread(300, 24));
  assert.equal(seen.length, 0, 'the steady new regime does not re-alarm');
  // A fresh jump alarms on the re-installed callback.
  report(bus, 'orders', spread(30, 15));
  assert.equal(seen.length, 1);
  assert.equal(seen[0].direction, 'increase');
});

test('an illegal callback is refused and the bus stays unconfigured', () => {
  const bus = new EventBus();
  for (const bad of [
    () => bus.setFailureRateRegimeChangeCallback(42 as never),
    () => bus.setFailureRateRegimeChangeCallback('nope' as never),
    () => bus.setFailureRateRegimeChangeCallback({} as never),
    () => bus.setFailureRateRegimeChangeCallback(undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(callbackRevisionAudits(bus).length, 0, 'failed enables must not audit');
  // Still unconfigured: reporting still refuses, and a later valid
  // enable still works.
  assert.throws(
    () => bus.reportDeliveryOutcome('orders', 'success'),
    /requires EventBusOptions.failureRegime/,
  );
  assert.doesNotThrow(() => bus.setFailureRateRegimeChangeCallback(() => {}));
  assert.equal(callbackRevisionAudits(bus).length, 1);
  assert.doesNotThrow(() => bus.reportDeliveryOutcome('orders', 'success'));
});

test('outcomes reported before the enable are not traced back', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const seen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({ now: clock.now });
  // Before the enable there is no monitor at all: reporting refuses.
  assert.throws(
    () => bus.reportDeliveryOutcome('orders', 'failure'),
    /requires EventBusOptions.failureRegime/,
  );
  bus.setFailureRateRegimeChangeCallback((e) => seen.push(e));
  // Detection starts from the first outcome after the enable: a 1%
  // baseline, then a jump, alarms exactly once with a ~1% before-rate.
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(30, 15));
  assert.equal(seen.length, 1);
  assert.equal(seen[0].direction, 'increase');
  assert.ok(
    seen[0].before.failureRate <= 0.1,
    `before rate ${seen[0].before.failureRate} should still be the post-enable 1% baseline`,
  );
  assert.ok(seen[0].after.failureRate >= 0.4, `after rate ${seen[0].after.failureRate} should be the 50% jump`);
});

test('re-installing the same callback after enabling is a no-op with no second audit', () => {
  const handler = (): void => {};
  const bus = new EventBus();
  // Enabling audit-only, then clearing what was never set, is a no-op.
  bus.setFailureRateRegimeChangeCallback(null);
  assert.equal(callbackRevisionAudits(bus).length, 1);
  bus.setFailureRateRegimeChangeCallback(null);
  assert.equal(
    callbackRevisionAudits(bus).length,
    1,
    'clearing an audit-only enabled bus must not audit',
  );

  bus.setFailureRateRegimeChangeCallback(handler);
  let audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: false });

  // Re-affirming the same function reference is a no-op.
  bus.setFailureRateRegimeChangeCallback(handler);
  audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 2, 'a same-value revision writes no second event');
});

test('clearing after enabling returns to audit-only, and a reinstall hears the next shift', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const oldSeen: FailureRateRegimeChange[] = [];
  const newSeen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({ now: clock.now });
  bus.setFailureRateRegimeChangeCallback((e) => oldSeen.push(e));
  creep(bus, 'orders');
  assert.equal(oldSeen.length, 1, 'the creep alarms once on the enabling callback');

  // Clear: detection keeps running through the cleared window — the
  // detector settles onto the new 8% regime with nobody listening.
  bus.setFailureRateRegimeChangeCallback(null);
  assert.deepEqual(callbackRevisionAudits(bus).map((a) => a.detail), [
    { hasCallback: true, oldHasCallback: false },
    { hasCallback: false, oldHasCallback: true },
  ]);
  report(bus, 'orders', spread(300, 24));
  assert.equal(oldSeen.length, 1, 'no callback fires while cleared');

  // Re-install: the steady 8% stream must NOT alarm.
  bus.setFailureRateRegimeChangeCallback((e) => newSeen.push(e));
  report(bus, 'orders', spread(300, 24));
  assert.equal(newSeen.length, 0, 'the steady new regime does not re-alarm');
  // A fresh jump alarms on the re-installed callback.
  report(bus, 'orders', spread(30, 15));
  assert.equal(newSeen.length, 1);
  assert.equal(newSeen[0].direction, 'increase');
});

test('the enable is live-only: a fresh bus is unconfigured again', () => {
  const bus = new EventBus();
  bus.setFailureRateRegimeChangeCallback(() => {});
  assert.doesNotThrow(() => bus.reportDeliveryOutcome('orders', 'success'));

  // A restart restores the unconfigured state: a fresh bus holds no
  // monitor and refuses reporting until enabled again.
  const restarted = new EventBus();
  assert.throws(
    () => restarted.reportDeliveryOutcome('orders', 'success'),
    /requires EventBusOptions.failureRegime/,
  );
  assert.equal(callbackRevisionAudits(restarted).length, 0);
});

test('a closed bus refuses the enable with RangeError and stays unconfigured', async () => {
  const bus = new EventBus();
  await bus.close();
  assert.throws(
    () => bus.setFailureRateRegimeChangeCallback(() => {}),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(callbackRevisionAudits(bus).length, 0, 'a refused enable must not audit');
  assert.throws(
    () => bus.reportDeliveryOutcome('orders', 'success'),
    /requires EventBusOptions.failureRegime/,
    'no monitor was built by the refused enable',
  );
});

test('the enable audits exact presence flags and the actor', () => {
  const first = (): void => {};
  const bus = new EventBus();
  bus.setFailureRateRegimeChangeCallback(first, 'ops');
  const audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(audit[0].actor, 'ops');

  const auditOnly = new EventBus();
  auditOnly.setFailureRateRegimeChangeCallback(null);
  const auditOnlyAudit = callbackRevisionAudits(auditOnly);
  assert.equal(auditOnlyAudit.length, 1);
  assert.deepEqual(auditOnlyAudit[0].detail, { hasCallback: false, oldHasCallback: false });
  assert.equal(auditOnlyAudit[0].actor, 'system');
  // The functions themselves are never recorded — only their presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
});

test('after enabling, reviseFailureRegimeParams works on the same monitor', () => {
  const bus = new EventBus();
  bus.setFailureRateRegimeChangeCallback(() => {});
  // Before EB-542 this threw `Error` on an unconfigured bus; the
  // monitor built by the enable is the one EB-543's ensure will reuse.
  const params = bus.reviseFailureRegimeParams({ delta: 0.2 });
  assert.deepEqual(params, { delta: 0.2, lambda: 5, warmupSamples: 50, windowSamples: 50 });
  const paramsAudits = bus.getSubscriptionAudit({ action: 'failure_rate_regime_params_revised' });
  assert.equal(paramsAudits.length, 1);
  assert.deepEqual(paramsAudits[0].detail.params, {
    delta: 0.2,
    lambda: 5,
    warmupSamples: 50,
    windowSamples: 50,
  });
  assert.deepEqual(paramsAudits[0].detail.oldParams, {
    delta: 0.05,
    lambda: 5,
    warmupSamples: 50,
    windowSamples: 50,
  });
  assert.doesNotThrow(() => bus.reportDeliveryOutcome('orders', 'success'));
});
