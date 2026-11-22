/**
 * Consumer-lag SLO live enable (EB-544):
 * `setLagSlo` on a bus whose `groupLag` is configured but never opted
 * into `groupLag.slo` installs the SLO live — no restart, no lost
 * subscriptions — with the revision resolved through the
 * construction-time rule (`resolveLagSloOptions`: at least one bound,
 * `breachSamples` defaulting to 3). Enabling installs no callback
 * (the SLO stays silent until `setLagSloBreachCallback`, EB-415,
 * installs one), and it is live-only: a restart restores the
 * unconfigured state. A bus with no `groupLag` at all still refuses.
 * The enable is audited like any revision, with every old value
 * omitted. Detection starts from the first evaluation after the
 * enable (and after a callback is installed) — lag accrued before it
 * never counts toward the consecutive-breach debounce.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { LagSloBreachEvent } from '../src/grouplag.ts';

function lagOnlyBus() {
  return new EventBus({ groupLag: { thresholdMessages: 10 } });
}

test('the first enable on a groupLag-only bus takes the default breachSamples 3', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = lagOnlyBus();
  bus.setLagSlo({ maxLagMessages: 5 });
  const audit = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, { maxLagMessages: 5, breachSamples: 3 });
  // Behaviourally the default debounce is 3: the breach needs three
  // consecutive samples after the callback is installed.
  bus.setLagSloBreachCallback((e) => events.push(e));
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6 > 5: c1
  assert.equal(events.length, 0);
  bus.publish('t', 'x'); // lag 7: c2
  assert.equal(events.length, 0);
  bus.publish('t', 'y'); // lag 8: c3 — fires
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 8);
  assert.equal(events[0].breachSamples, 3);
  assert.equal(events[0].consecutiveSamples, 3);
});

test('a partial enable falls back to the defaults per field', () => {
  const bus = lagOnlyBus();
  bus.setLagSlo({ maxLagMs: 500 });
  const audit = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { maxLagMs: 500, breachSamples: 3 });
  // The unconfigured bound stays unconfigured: adding it later omits
  // its old value from the audit, like any first-time bound.
  bus.setLagSlo({ maxLagMessages: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    maxLagMessages: 5,
    maxLagMs: 500,
    oldMaxLagMs: 500,
    breachSamples: 3,
    oldBreachSamples: 3,
  });
});

test('no bound or an illegal value is rejected and the bus stays unconfigured', () => {
  const bus = lagOnlyBus();
  const bad: Array<unknown> = [
    {}, // no bound at all
    { breachSamples: 2 }, // debounce alone is not a bound
    { maxLagMessages: -1 },
    { maxLagMessages: Number.NaN },
    { maxLagMessages: Infinity },
    { maxLagMessages: '5' },
    { maxLagMs: -1 },
    { breachSamples: 0 },
    { breachSamples: 1.5 },
    { maxLagMessages: 10, breachSamples: 0 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setLagSlo(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // An absent revision asks nothing to enable — a silent no-op.
  bus.setLagSlo(null as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
  // Still unconfigured: the callback gate still refuses.
  assert.throws(() => bus.setLagSloBreachCallback(() => {}), RangeError);
  assert.equal(bus.getStats().lagSloHasBreachCallback, false);
  // A valid enable afterwards still works.
  bus.setLagSlo({ maxLagMessages: 5 });
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 1);
});

test('after enabling, the next lag evaluation judges with the new bound', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = lagOnlyBus();
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6, no SLO yet
  assert.equal(events.length, 0);
  bus.setLagSlo({ maxLagMessages: 5, breachSamples: 1 });
  bus.setLagSloBreachCallback((e) => events.push(e));
  bus.publish('t', 'x'); // lag 7 > 5: the next sample breaches
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 7);
  assert.equal(events[0].violated, 'maxLagMessages');
  assert.equal(bus.getStats().lagSloBreaches, 1);
});

test('consecutive counts start at the enable — pre-enable lag is not traced back', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = lagOnlyBus();
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6, already over 5
  bus.setLagSlo({ maxLagMessages: 5, breachSamples: 3 });
  bus.setLagSloBreachCallback((e) => events.push(e));
  bus.publish('t', 'x'); // lag 7: c1 — had the pre-enable samples
  assert.equal(events.length, 0); // counted, this would already have fired
  bus.publish('t', 'y'); // lag 8: c2
  assert.equal(events.length, 0);
  bus.publish('t', 'z'); // lag 9: c3 — fires
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 9);
  assert.equal(events[0].consecutiveSamples, 3);
});

test('the callback is installable afterwards via the EB-415 entry point', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = lagOnlyBus();
  // Before the enable the callback gate still refuses.
  assert.throws(() => bus.setLagSloBreachCallback((e) => events.push(e)), RangeError);
  bus.setLagSlo({ maxLagMessages: 5, breachSamples: 1 });
  assert.equal(bus.getStats().lagSloHasBreachCallback, false);
  bus.setLagSloBreachCallback((e) => events.push(e));
  assert.equal(bus.getStats().lagSloHasBreachCallback, true);
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 6; i++) bus.publish('t', i); // fires at lag 6
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 6);
  assert.equal(bus.getStats().lagSloBreaches, 1);
});

test('a post-enable revision merges over the live enabled values', () => {
  const bus = lagOnlyBus();
  bus.setLagSlo({ maxLagMessages: 5 });
  bus.setLagSlo({ maxLagMs: 500 });
  const audit = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    maxLagMessages: 5,
    oldMaxLagMessages: 5,
    maxLagMs: 500,
    breachSamples: 3,
    oldBreachSamples: 3,
  });
  // And the merged tuning is the live one: a breach now needs either bound.
  const events: LagSloBreachEvent[] = [];
  bus.setLagSloBreachCallback((e) => events.push(e));
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 8; i++) bus.publish('t', i); // lag 6..8 > 5: c1..c3
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 8);
});

test('a same-value revision after enabling is a no-op with no second audit', () => {
  const bus = lagOnlyBus();
  bus.setLagSlo({ maxLagMessages: 5, maxLagMs: 1000, breachSamples: 3 });
  bus.setLagSlo({ maxLagMessages: 5 });
  bus.setLagSlo({ maxLagMs: 1000 });
  bus.setLagSlo({ breachSamples: 3 });
  bus.setLagSlo({ maxLagMessages: 5, maxLagMs: 1000, breachSamples: 3 });
  bus.setLagSlo({});
  bus.setLagSlo(null as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 1);
});

test('the enable is live-only: a fresh bus is unconfigured again', () => {
  const bus = lagOnlyBus();
  bus.setLagSlo({ maxLagMessages: 5 });
  bus.setLagSloBreachCallback(() => {});
  assert.equal(bus.getStats().lagSloHasBreachCallback, true);
  // A restart restores the unconfigured state: a fresh bus built from
  // the same construction options holds no SLO.
  const fresh = lagOnlyBus();
  assert.equal(fresh.getStats().lagSloHasBreachCallback, false);
  assert.throws(() => fresh.setLagSloBreachCallback(() => {}), RangeError);
  assert.equal(fresh.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
  // The fresh bus can still be enabled the same way.
  fresh.setLagSlo({ maxLagMessages: 5 });
  assert.equal(fresh.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 1);
});

test('the enable audits the exact bounds with no old values and the actor', () => {
  const bus = lagOnlyBus();
  bus.setLagSlo({ maxLagMs: 500, breachSamples: 2 });
  const audit = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, { maxLagMs: 500, breachSamples: 2 });
  // No maxLagMessages key, no old* key at all on the first enable.
  assert.equal('maxLagMessages' in (audit[0].detail as object), false);
  assert.equal('oldBreachSamples' in (audit[0].detail as object), false);
});

test('a closed bus refuses the enable and the revision with RangeError', async () => {
  const unconfigured = lagOnlyBus();
  await unconfigured.close();
  assert.throws(
    () => unconfigured.setLagSlo({ maxLagMessages: 5 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(unconfigured.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);

  const configured = new EventBus({
    groupLag: { slo: { maxLagMessages: 5 } },
  });
  await configured.close();
  assert.throws(
    () => configured.setLagSlo({ maxLagMessages: 10 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(configured.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
});

describe('the enable gate', () => {
  test('a bus with no groupLag at all still refuses', () => {
    const plain = new EventBus();
    assert.throws(() => plain.setLagSlo({ maxLagMessages: 5 }), RangeError);
    assert.throws(() => plain.setLagSlo({ maxLagMs: 100, breachSamples: 2 }), RangeError);
    assert.equal(plain.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
    // And it cannot be smuggled in through the callback gate either.
    assert.throws(() => plain.setLagSloBreachCallback(() => {}), RangeError);
  });

  test('a groupLag configured only with onGroupLag can gain an SLO', () => {
    const bus = new EventBus({ groupLag: { onGroupLag: () => {} } });
    bus.setLagSlo({ maxLagMessages: 5, breachSamples: 1 });
    assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 1);
    const events: LagSloBreachEvent[] = [];
    bus.setLagSloBreachCallback((e) => events.push(e));
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 6; i++) bus.publish('t', i);
    assert.equal(events.length, 1);
  });
});
