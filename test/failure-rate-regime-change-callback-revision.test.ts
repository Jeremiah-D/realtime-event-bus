import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { DeliveryOutcome, FailureRateRegimeChange } from '../src/failure-regime.ts';

/**
 * Failure-rate regime-change alert callback revision (EB-445):
 * `setFailureRateRegimeChangeCallback` live-redirects the
 * `onFailureRateRegimeChange` alert without restarting the bus.
 * The callback is captured per-detector at construction, so the
 * monitor must fan the swap out to already-built detectors —
 * swapping only the monitor's stored options would leave them on
 * the old callback (the WR-372 tracker-capture trap). Only the
 * callback moves: detection parameters, baselines, warmup, and
 * settling state are untouched, and a throwing callback is
 * swallowed at the detector's firing site, so observation never
 * breaks the outcome stream.
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

test('an already-built topic detector alarms on the new callback after the swap', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const oldSeen: FailureRateRegimeChange[] = [];
  const newSeen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({
    now: clock.now,
    failureRegime: { onFailureRateRegimeChange: (e) => oldSeen.push(e) },
  });
  // Build the 'orders' detector and alarm it once on the old callback.
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(30, 15));
  assert.equal(oldSeen.length, 1);
  assert.equal(oldSeen[0].topic, 'orders');
  assert.equal(oldSeen[0].direction, 'increase');

  // Swap: the detector was built before the swap, so only a fan-out
  // reaches it — swapping the monitor's stored options alone would
  // leave it on the old callback.
  bus.setFailureRateRegimeChangeCallback((e) => newSeen.push(e));
  // Settle onto the new 50% regime, then jump to all-failures.
  report(bus, 'orders', spread(50, 25));
  report(bus, 'orders', spread(30, 30));
  assert.equal(oldSeen.length, 1, 'the old callback never fires again');
  assert.equal(newSeen.length, 1, 'the already-built detector alarms on the new callback');
  assert.equal(newSeen[0].topic, 'orders');
  assert.equal(newSeen[0].direction, 'increase');
  assert.ok(
    newSeen[0].before.failureRate >= 0.4 && newSeen[0].before.failureRate <= 0.6,
    `before rate ${newSeen[0].before.failureRate} should still be the 50% regime`,
  );
  assert.ok(newSeen[0].after.failureRate >= 0.9, `after rate ${newSeen[0].after.failureRate} should be the jump`);
  assert.ok(newSeen[0].before.samples >= 1 && newSeen[0].after.samples >= 1);
  assert.equal(newSeen[0].detectedAtMs, 5_000_000);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: true,
    oldHasCallback: true,
  });
});

test('a not-yet-built topic detector alarms on the new callback from its first alarm', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const oldSeen: FailureRateRegimeChange[] = [];
  const newSeen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({
    now: clock.now,
    failureRegime: { onFailureRateRegimeChange: (e) => oldSeen.push(e) },
  });
  // Swap before 'fresh' is ever reported: its detector is built later.
  bus.setFailureRateRegimeChangeCallback((e) => newSeen.push(e));
  report(bus, 'fresh', spread(100, 1));
  report(bus, 'fresh', spread(30, 15));
  assert.equal(oldSeen.length, 0, 'the old callback never fires');
  assert.equal(newSeen.length, 1, 'the later-built detector uses the new callback from its first alarm');
  assert.equal(newSeen[0].topic, 'fresh');
  assert.equal(newSeen[0].direction, 'increase');
});

test('clearing with null keeps detection running with no callback, and a reinstall hears the next shift', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const oldSeen: FailureRateRegimeChange[] = [];
  const newSeen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({
    now: clock.now,
    failureRegime: { onFailureRateRegimeChange: (e) => oldSeen.push(e) },
  });
  // Alarm once on the old callback with a slow 1% -> 8% creep.
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(100, 3));
  report(bus, 'orders', spread(100, 5));
  report(bus, 'orders', spread(300, 24));
  assert.equal(oldSeen.length, 1, 'the creep alarms once on the old callback');

  // Clear: detection keeps running through the cleared window — the
  // detector settles onto the new 8% regime with nobody listening.
  bus.setFailureRateRegimeChangeCallback(null);
  report(bus, 'orders', spread(300, 24));
  assert.equal(oldSeen.length, 1, 'no callback fires while cleared');

  // Re-install: the steady 8% stream must NOT alarm — the baseline
  // re-estimated to 8% during the cleared window. If detection had
  // frozen at the clear, the baseline would still be 1% and this
  // stream would alarm.
  bus.setFailureRateRegimeChangeCallback((e) => newSeen.push(e));
  report(bus, 'orders', spread(300, 24));
  assert.equal(newSeen.length, 0, 'the steady new regime does not re-alarm');
  // A fresh jump alarms on the re-installed callback.
  report(bus, 'orders', spread(30, 15));
  assert.equal(newSeen.length, 1);
  assert.equal(newSeen[0].direction, 'increase');
  assert.deepEqual(callbackRevisionAudits(bus).map((a) => a.detail), [
    { hasCallback: false, oldHasCallback: true },
    { hasCallback: true, oldHasCallback: false },
  ]);
});

test('an unconfigured bus throws a must-restart Error', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.setFailureRateRegimeChangeCallback(() => {}),
    (e: unknown) =>
      e instanceof Error &&
      !(e instanceof RangeError) &&
      /failureRegime is not configured — enabling it requires a restart/.test((e as Error).message),
  );
  assert.equal(callbackRevisionAudits(bus).length, 0, 'a refused revision must not audit');
  // A configured bus (even with no callback) accepts the revision.
  const enabled = new EventBus({ failureRegime: {} });
  assert.doesNotThrow(() => enabled.setFailureRateRegimeChangeCallback(() => {}));
  assert.equal(callbackRevisionAudits(enabled).length, 1);
});

test('a non-function callback throws RangeError and the live callback is untouched', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const seen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({
    now: clock.now,
    failureRegime: { onFailureRateRegimeChange: (e) => seen.push(e) },
  });
  for (const bad of [
    () => bus.setFailureRateRegimeChangeCallback(42 as never),
    () => bus.setFailureRateRegimeChangeCallback('nope' as never),
    () => bus.setFailureRateRegimeChangeCallback({} as never),
    () => bus.setFailureRateRegimeChangeCallback(undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(callbackRevisionAudits(bus).length, 0, 'failed revisions must not audit');
  // The live callback is untouched: the next alarm still reaches it.
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(30, 15));
  assert.equal(seen.length, 1, 'the original callback still fires after refused revisions');
});

test('re-installing the same callback reference is a no-op with no audit', () => {
  const handler = (): void => {};
  const bus = new EventBus({ failureRegime: {} });
  // Clearing a callback that was never set is a no-op, not a clear.
  bus.setFailureRateRegimeChangeCallback(null);
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'clearing an absent callback must not audit',
  );

  bus.setFailureRateRegimeChangeCallback(handler);
  let audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });

  // Re-affirming the same function reference is a no-op.
  bus.setFailureRateRegimeChangeCallback(handler);
  audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 1, 'a same-value revision writes no second event');
});

test('a throwing callback is swallowed and the reporting path is unaffected', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const bus = new EventBus({
    now: clock.now,
    failureRegime: {
      onFailureRateRegimeChange: () => {
        throw new Error('boom');
      },
    },
  });
  // A throwing callback installed at construction must not break the
  // reporting call that delivers the alarming outcome.
  assert.doesNotThrow(() => {
    report(bus, 'orders', spread(100, 1));
    report(bus, 'orders', spread(30, 15));
  });
  assert.doesNotThrow(() => {
    for (let i = 0; i < 200; i++) bus.reportDeliveryOutcome('orders', 'success');
  });

  // The same for a throwing callback installed by revision.
  const good: FailureRateRegimeChange[] = [];
  bus.setFailureRateRegimeChangeCallback(() => {
    throw new Error('boom');
  });
  assert.doesNotThrow(() => {
    report(bus, 'orders', spread(50, 25));
    report(bus, 'orders', spread(30, 30));
  });
  // And the path still works after the throw: swap in a good callback
  // and the next shift alarms on it.
  bus.setFailureRateRegimeChangeCallback((e) => good.push(e));
  report(bus, 'orders', spread(50, 25));
  report(bus, 'orders', spread(30, 30));
  assert.equal(good.length, 1, 'the reporting path survives a throwing callback');
  assert.equal(good[0].topic, 'orders');
});

test('the swap audits exact presence flags', () => {
  const first = (): void => {};
  const second = (): void => {};
  const bus = new EventBus({ failureRegime: {} });
  bus.setFailureRateRegimeChangeCallback(first);
  bus.setFailureRateRegimeChangeCallback(second);
  bus.setFailureRateRegimeChangeCallback(null);
  const audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(audit[2].detail, { hasCallback: false, oldHasCallback: true });
  for (const entry of audit) {
    assert.equal(entry.actor, 'system');
  }
  // The functions themselves are never recorded — only their presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
});

test('swapping mid-settling does not reset the settling count', () => {
  now = 5_000_000;
  clock.nowMs = now;
  const oldSeen: FailureRateRegimeChange[] = [];
  const newSeen: FailureRateRegimeChange[] = [];
  const bus = new EventBus({
    now: clock.now,
    failureRegime: { onFailureRateRegimeChange: (e) => oldSeen.push(e) },
  });
  // Alarm once: the detector enters its settling window (50 samples).
  report(bus, 'orders', spread(100, 1));
  report(bus, 'orders', spread(30, 15));
  assert.equal(oldSeen.length, 1);

  // 30 of the 50 settling samples go by on the old callback's watch.
  report(bus, 'orders', spread(30, 15));
  // Swap mid-settling, then feed the remaining 20 settling samples:
  // settling completes on the 50th sample only if the count survived.
  bus.setFailureRateRegimeChangeCallback((e) => newSeen.push(e));
  report(bus, 'orders', spread(20, 10));
  // A jump now must alarm on the new callback. If the swap had reset
  // the settling count, these 30 samples would be swallowed by the
  // restarted settling window and no alarm would fire.
  report(bus, 'orders', spread(30, 30));
  assert.equal(oldSeen.length, 1, 'the old callback never fires again');
  assert.equal(newSeen.length, 1, 'the swap must not reset settling');
  assert.equal(newSeen[0].direction, 'increase');
  assert.ok(
    newSeen[0].before.failureRate >= 0.4 && newSeen[0].before.failureRate <= 0.6,
    `before rate ${newSeen[0].before.failureRate} should still be the 50% regime`,
  );
  assert.ok(newSeen[0].after.failureRate >= 0.9, `after rate ${newSeen[0].after.failureRate} should be the jump`);
});

test('a closed bus refuses the revision', async () => {
  const bus = new EventBus({ failureRegime: {} });
  await bus.close();
  assert.throws(
    () => bus.setFailureRateRegimeChangeCallback(() => {}),
    RangeError,
  );
  assert.equal(callbackRevisionAudits(bus).length, 0, 'a refused revision must not audit');
});
