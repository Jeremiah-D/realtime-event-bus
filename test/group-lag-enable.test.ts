/**
 * Consumer-group lag monitor live enable (EB-565):
 * `setGroupLag` on a bus that never opted into `groupLag` installs the
 * whole monitor live — the default and per-group thresholds, the
 * `onGroupLag` callback, and the SLO — resolved through the
 * construction-time rule (`resolveGroupLagOptions`). No restart, no lost
 * subscriptions, and the enable is itself audited as
 * `group_lag_revised` with the old value `null`. A bus that already
 * enabled `groupLag` refuses a second call, and a failed enable leaves
 * the bus unconfigured, so a later valid call still enables it. The
 * enable re-evaluates every group at once, so a bus that installs a
 * callback pages already-lagging rows immediately — the latch starts
 * empty, and lag accrued before the enable is never traced back.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { GroupLagEvent, LagSloBreachEvent } from '../src/grouplag.ts';

function lagBus(): EventBus {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {});
  return bus;
}

test('the first enable with {} takes every construction default and stays silent', () => {
  const bus = lagBus();
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6, no monitor yet
  bus.setGroupLag({});
  const audit = bus.getSubscriptionAudit({ action: 'group_lag_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, {
    defaultThreshold: 100,
    oldDefaultThreshold: null,
    hasCallback: false,
    hasSlo: false,
  });
  // Silent: no callback installed, and the lag rows keep their accounting.
  assert.equal(bus.getStats().groupLagHasCallback, false);
  assert.equal(bus.getStats().groupLagThresholdFor('g'), 100);
  assert.equal(bus.getStats().groupLag[0].lag, 6);
  assert.equal(bus.getStats().lagSloBreaches, 0);
});

test('a threshold plus callback pages on the next evaluation, latch from empty', () => {
  const events: GroupLagEvent[] = [];
  const bus = lagBus();
  for (let i = 0; i < 3; i++) bus.publish('t', i); // lag 3 < 5
  bus.setGroupLag({ thresholdMessages: 5, onGroupLag: (e) => events.push(e) });
  // The immediate re-evaluation finds lag 3 below the new threshold.
  assert.equal(events.length, 0);
  bus.publish('t', 'x'); // lag 4: still below
  assert.equal(events.length, 0);
  bus.publish('t', 'y'); // lag 5: at the threshold, not above
  assert.equal(events.length, 0);
  bus.publish('t', 'z'); // lag 6 > 5: fires once
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 6);
  // Still over threshold: the latch holds, no re-fire.
  bus.publish('t', 'w');
  assert.equal(events.length, 1);
});

test('a partial enable falls back to the defaults per field', () => {
  const bus = lagBus();
  bus.setGroupLag({ thresholdMessages: 5 });
  const audit = bus.getSubscriptionAudit({ action: 'group_lag_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    defaultThreshold: 5,
    oldDefaultThreshold: null,
    hasCallback: false,
    hasSlo: false,
  });
  assert.equal(bus.getStats().groupLagThresholdFor('g'), 5);
  assert.equal(bus.getStats().groupLagThresholdFor('other'), 5);
  // A callback installed afterwards (EB-417) pages under the live default.
  const events: GroupLagEvent[] = [];
  bus.setGroupLagCallback((e) => events.push(e));
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6 > 5
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 6);
});

test('per-group thresholds override and the default covers the rest', () => {
  const events: GroupLagEvent[] = [];
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {});
  bus.subscribeToGroup('h', 'u', () => {});
  for (let i = 0; i < 4; i++) bus.publish('t', i); // g lag 4
  for (let i = 0; i < 5; i++) bus.publish('u', i); // h lag 5
  bus.setGroupLag({
    thresholdMessages: 10,
    thresholds: { g: 3 },
    onGroupLag: (e) => events.push(e),
  });
  // The immediate re-evaluation pages g (4 > 3) but not h (5 < 10).
  assert.equal(events.length, 1);
  assert.equal(events[0].groupId, 'g');
  assert.equal(events[0].lag, 4);
  assert.equal(bus.getStats().groupLagThresholdFor('g'), 3);
  assert.equal(bus.getStats().groupLagThresholdFor('h'), 10);
  for (let i = 0; i < 6; i++) bus.publish('u', i); // h lag 11 > 10
  assert.equal(events.length, 2);
  assert.equal(events[1].groupId, 'h');
  assert.equal(events[1].lag, 11);
});

test('an SLO installed with the monitor judges from the enable, never before it', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = lagBus();
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6, no monitor yet
  bus.setGroupLag({
    thresholdMessages: 10,
    slo: { maxLagMessages: 5, breachSamples: 3, onLagSloBreach: (e) => events.push(e) },
  });
  // The immediate re-evaluation is the first SLO sample (c1): no fire yet.
  assert.equal(events.length, 0);
  bus.publish('t', 'x'); // lag 7: c2
  assert.equal(events.length, 0);
  bus.publish('t', 'y'); // lag 8: c3 — fires, with the lag at the firing sample
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 8);
  assert.equal(events[0].consecutiveSamples, 3);
  assert.equal(events[0].breachSamples, 3);
  assert.equal(bus.getStats().lagSloBreaches, 1);
});

test('an SLO without a callback stays silent until the EB-415 entry installs one', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = lagBus();
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6
  bus.setGroupLag({ slo: { maxLagMessages: 5, breachSamples: 1 } });
  assert.equal(bus.getStats().lagSloHasBreachCallback, false);
  // Silent: no evaluation ran, so nothing fired and no sample was taken.
  assert.equal(events.length, 0);
  bus.setLagSloBreachCallback((e) => events.push(e));
  assert.equal(bus.getStats().lagSloHasBreachCallback, true);
  bus.publish('t', 'x'); // lag 7 > 5: the first sample after the callback fires
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 7);
  assert.equal(events[0].violated, 'maxLagMessages');
});

test('an illegal value is rejected and the bus stays unconfigured', () => {
  const bus = lagBus();
  const bad: Array<unknown> = [
    { thresholdMessages: -1 },
    { thresholdMessages: Number.NaN },
    { thresholdMessages: Number.POSITIVE_INFINITY },
    { thresholdMessages: '5' },
    { thresholds: { '': 5 } },
    { thresholds: { g: -1 } },
    { thresholds: { g: Number.NaN } },
    { thresholds: null },
    { onGroupLag: 'not-a-function' },
    { onGroupLag: 42 },
    { slo: {} }, // no bound at all
    { slo: { breachSamples: 2 } }, // debounce alone is not a bound
    { slo: { maxLagMessages: -1 } },
    { slo: { maxLagMessages: 5, breachSamples: 0 } },
    { slo: { maxLagMessages: 5, onLagSloBreach: 'nope' } },
    'nope',
    42,
    [],
  ];
  for (const opts of bad) {
    assert.throws(
      () => bus.setGroupLag(opts as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(opts)}`,
    );
  }
  // Still unconfigured: the fine-grained gates still refuse, no audit.
  assert.throws(() => bus.setGroupLagCallback(() => {}), RangeError);
  assert.throws(() => bus.setLagSlo({ maxLagMessages: 5 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 0);
  // A valid enable afterwards still works.
  bus.setGroupLag({});
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 1);
});

test('null on a never-enabled bus is a silent no-op with no audit', () => {
  const bus = lagBus();
  bus.setGroupLag(null);
  bus.setGroupLag(undefined as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 0);
  // Still unconfigured: the callback gate still refuses.
  assert.throws(() => bus.setGroupLagCallback(() => {}), RangeError);
  assert.equal(bus.getStats().groupLagHasCallback, false);
  // A valid enable afterwards still works.
  bus.setGroupLag({ thresholdMessages: 5 });
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 1);
});

test('a second call on an enabled bus is refused', () => {
  const bus = lagBus();
  bus.setGroupLag({});
  assert.throws(
    () => bus.setGroupLag({ thresholdMessages: 5 }),
    (e: unknown) => e instanceof RangeError && /already enabled/.test((e as Error).message),
  );
  assert.throws(
    () => bus.setGroupLag({}),
    (e: unknown) => e instanceof RangeError && /already enabled/.test((e as Error).message),
  );
  // `null` stays a no-op even on an enabled bus (the null branch sits
  // before the already-enabled gate), and the audit still holds one entry.
  bus.setGroupLag(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 1);
  // A bus built with groupLag configured refuses the same way.
  const configured = new EventBus({ groupLag: { thresholdMessages: 10 } });
  assert.throws(
    () => configured.setGroupLag({}),
    (e: unknown) => e instanceof RangeError && /already enabled/.test((e as Error).message),
  );
  assert.equal(configured.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 0);
});

test('the enable opens every fine-grained revision gate', () => {
  const bus = lagBus();
  // Before the enable every gate refuses.
  assert.throws(() => bus.setGroupLagCallback(() => {}), RangeError);
  assert.throws(() => bus.setLagSlo({ maxLagMessages: 5 }), RangeError);
  assert.throws(() => bus.setGroupLagDefaultThreshold(7), RangeError);
  assert.throws(() => bus.setLagSloBreachCallback(() => {}), RangeError);
  bus.setGroupLag({ thresholdMessages: 5, onGroupLag: () => {} });
  // After the enable each gate opens: callback, SLO, default threshold.
  bus.setGroupLagCallback(() => {});
  bus.setLagSlo({ maxLagMessages: 3, breachSamples: 1 });
  bus.setGroupLagDefaultThreshold(7);
  assert.equal(bus.getStats().groupLagThresholdFor('g'), 7);
  // The breach callback needs the SLO first — now installed, it opens too.
  const events: LagSloBreachEvent[] = [];
  bus.setLagSloBreachCallback((e) => events.push(e));
  assert.equal(bus.getStats().lagSloHasBreachCallback, true);
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 4 > 3 fires on the 4th publish
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 4);
});

test('the enable re-evaluates at once: already-lagging rows page without a new publish', () => {
  const events: GroupLagEvent[] = [];
  const bus = lagBus();
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6, no monitor yet
  assert.equal(events.length, 0);
  bus.setGroupLag({ thresholdMessages: 5, onGroupLag: (e) => events.push(e) });
  // No publish happened after the enable — the immediate re-evaluation
  // paged the already-lagging row.
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 6);
  assert.equal(events[0].groupId, 'g');
});

test('the enable is live-only: a fresh bus is unconfigured again', () => {
  const bus = lagBus();
  bus.setGroupLag({ thresholdMessages: 5, onGroupLag: () => {} });
  assert.equal(bus.getStats().groupLagHasCallback, true);
  // A restart restores the unconfigured state: a fresh bus built from
  // the same construction options holds no monitor.
  const fresh = lagBus();
  assert.equal(fresh.getStats().groupLagHasCallback, false);
  assert.throws(() => fresh.setGroupLagCallback(() => {}), RangeError);
  assert.equal(fresh.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 0);
  // The fresh bus can still be enabled the same way.
  fresh.setGroupLag({ thresholdMessages: 5 });
  assert.equal(fresh.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 1);
});

test('the enable audits the exact tuning with a null old value and the actor', () => {
  const bus = lagBus();
  bus.setGroupLag({
    thresholdMessages: 5,
    thresholds: { g: 2 },
    onGroupLag: () => {},
    slo: { maxLagMessages: 3, onLagSloBreach: () => {} },
  });
  const audit = bus.getSubscriptionAudit({ action: 'group_lag_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, {
    defaultThreshold: 5,
    oldDefaultThreshold: null,
    hasCallback: true,
    hasSlo: true,
  });
  // Presence-only: per-group threshold values and the functions
  // themselves never enter the audit.
  assert.equal('thresholds' in (audit[0].detail as object), false);
  assert.equal('onGroupLag' in (audit[0].detail as object), false);
  assert.equal('slo' in (audit[0].detail as object), false);
});

test('a closed bus refuses the enable with RangeError', async () => {
  const unconfigured = lagBus();
  await unconfigured.close();
  assert.throws(
    () => unconfigured.setGroupLag({}),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.throws(
    () => unconfigured.setGroupLag(null),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(unconfigured.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 0);

  const configured = lagBus();
  configured.setGroupLag({ thresholdMessages: 5 });
  await configured.close();
  assert.throws(
    () => configured.setGroupLag({ thresholdMessages: 10 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(configured.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 1);
});

describe('the enable gate', () => {
  test('a bus primed by the setGroupLagThreshold side door already counts as configured', () => {
    // The EB-48 lazy initialization (grouplag.test.ts: the runtime
    // threshold works without a constructor option) creates a resolved
    // tuning with no callback — `groupLagCurrent()` is defined, so
    // the monitor counts as enabled and `setGroupLag` refuses instead
    // of overwriting the side-door tuning. The side door itself is
    // unchanged: the threshold it set still governs.
    const bus = lagBus();
    bus.setGroupLagThreshold('g', 5); // no callback configured: must not throw
    assert.equal(bus.getStats().groupLagThresholdFor('g'), 5);
    assert.throws(
      () => bus.setGroupLag({ thresholdMessages: 10 }),
      (e: unknown) => e instanceof RangeError && /already enabled/.test((e as Error).message),
    );
    assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_revised' }).length, 0);
    assert.equal(bus.getStats().groupLagThresholdFor('g'), 5);
  });
});
