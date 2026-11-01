/**
 * Bus-level consumer-lag SLO revision (EB-314).
 *
 * `setLagSlo` retunes the `groupLag.slo` bounds live — without
 * restarting the bus. Only a bus that enabled `groupLag.slo` can be
 * revised; illegal values fail closed with the live tuning and the
 * audit log untouched; an identical (or empty) revision is a no-op
 * with no audit. In-flight episode state is preserved: a retune never
 * resets a row's consecutive-breach count, latched breach, or episode
 * start — a new bound governs from the next sample and a new
 * `breachSamples` from the next consecutive count.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { LagSloBreachEvent, LagSloOptions } from '../src/grouplag.ts';

function sloBus(slo: LagSloOptions, opts?: { now?: () => number }) {
  return new EventBus({ ...(opts?.now === undefined ? {} : { now: opts.now }), groupLag: { slo } });
}

test('setLagSlo fails closed when groupLag.slo is not enabled', () => {
  const plain = new EventBus();
  assert.throws(() => plain.setLagSlo({ maxLagMessages: 5 }), RangeError);
  assert.equal(plain.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);

  // groupLag alerting without an SLO is not an SLO: still refused.
  const lagOnly = new EventBus({ groupLag: { thresholdMessages: 10 } });
  assert.throws(() => lagOnly.setLagSlo({ maxLagMessages: 5 }), RangeError);
  const hookOnly = new EventBus({ groupLag: { onGroupLag: () => {} } });
  assert.throws(() => hookOnly.setLagSlo({ breachSamples: 2 }), RangeError);
  assert.equal(hookOnly.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
});

test('setLagSlo works when the SLO is configured without a callback', () => {
  // Valid but silent (EB-112): no callback, so nothing ever fires —
  // but the SLO is configured, so it is revisable.
  const bus = sloBus({ maxLagMessages: 5 });
  bus.setLagSlo({ maxLagMessages: 10 });
  const audit = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    maxLagMessages: 10,
    oldMaxLagMessages: 5,
    breachSamples: 3,
    oldBreachSamples: 3,
  });
});

test('setLagSlo rejects illegal values and leaves the live tuning untouched', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 1,
    onLagSloBreach: (e) => events.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  const bad: Array<unknown> = [
    { maxLagMessages: -1 },
    { maxLagMessages: Number.NaN },
    { maxLagMessages: Infinity },
    { maxLagMessages: '5' },
    { maxLagMs: -1 },
    { maxLagMs: Number.NaN },
    { maxLagMs: Infinity },
    { breachSamples: 0 },
    { breachSamples: 1.5 },
    { breachSamples: '3' },
    // One good field does not save a bad sibling: the whole call fails.
    { maxLagMessages: 10, breachSamples: 0 },
    { maxLagMs: 500, maxLagMessages: -2 },
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
  // The failed calls changed nothing: the bound is still 5 with a
  // debounce of 1, so the sixth publish still breaches, and the audit
  // log is untouched.
  for (let i = 0; i < 6; i++) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 6);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
  // A valid revision afterwards still works.
  bus.setLagSlo({ maxLagMessages: 100 });
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 1);
});

test('setLagSlo with identical values or an empty revision is a no-op with no audit', () => {
  const bus = sloBus({ maxLagMessages: 5, maxLagMs: 1000, breachSamples: 3 });
  bus.setLagSlo({ maxLagMessages: 5 });
  bus.setLagSlo({ maxLagMs: 1000 });
  bus.setLagSlo({ breachSamples: 3 });
  bus.setLagSlo({ maxLagMessages: 5, maxLagMs: 1000, breachSamples: 3 });
  bus.setLagSlo({});
  bus.setLagSlo(null as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
});

test('partial revision moves only the provided field', () => {
  const bus = sloBus({ maxLagMessages: 5, maxLagMs: 1000, breachSamples: 3 });
  bus.setLagSlo({ breachSamples: 2 });
  let audit = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    maxLagMessages: 5,
    oldMaxLagMessages: 5,
    maxLagMs: 1000,
    oldMaxLagMs: 1000,
    breachSamples: 2,
    oldBreachSamples: 3,
  });
  bus.setLagSlo({ maxLagMessages: 10 });
  audit = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    maxLagMessages: 10,
    oldMaxLagMessages: 5,
    maxLagMs: 1000,
    oldMaxLagMs: 1000,
    breachSamples: 2,
    oldBreachSamples: 2,
  });
});

test('adding a bound omits its old value from the audit', () => {
  const bus = sloBus({ maxLagMessages: 5 });
  bus.setLagSlo({ maxLagMs: 500 });
  const audit = bus.getSubscriptionAudit({ action: 'lag_slo_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    maxLagMessages: 5,
    oldMaxLagMessages: 5,
    maxLagMs: 500,
    breachSamples: 3,
    oldBreachSamples: 3,
  });
});

test('tightened message bound fires at the next sample', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 10,
    breachSamples: 1,
    onLagSloBreach: (e) => events.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 1..6: under the old bound
  assert.equal(events.length, 0);
  bus.setLagSlo({ maxLagMessages: 5 });
  bus.publish('t', 'x'); // lag 7 > 5: the next sample breaches
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 7);
  assert.equal(events[0].violated, 'maxLagMessages');
  assert.equal(bus.getStats().lagSloBreaches, 1);
});

test('loosened message bound defers firing', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 1,
    onLagSloBreach: (e) => events.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 6; i++) bus.publish('t', i); // fires at lag 6
  assert.equal(events.length, 1);
  bus.commitOffset('g', 't', 6); // lag 0: recovery rearms the latch
  bus.setLagSlo({ maxLagMessages: 100 });
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 1..6: under the new bound
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().lagSloBreaches, 1);
});

test('breachSamples retune mid-episode preserves the consecutive count', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 3,
    onLagSloBreach: (e) => events.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 7; i++) bus.publish('t', i); // breach at lag 6 (c1), 7 (c2)
  assert.equal(events.length, 0);
  bus.setLagSlo({ breachSamples: 5 });
  bus.publish('t', 'x'); // lag 8: c3 — not a reset to c1
  assert.equal(events.length, 0);
  bus.publish('t', 'y'); // lag 9: c4
  assert.equal(events.length, 0);
  bus.publish('t', 'z'); // lag 10: c5 — fires against the live debounce
  assert.equal(events.length, 1);
  assert.equal(events[0].consecutiveSamples, 5);
  assert.equal(events[0].breachSamples, 5);
  assert.equal(events[0].lag, 10);
});

describe('lag SLO revision (maxLagMs)', () => {
  function clockBus(slo: LagSloOptions) {
    const clock = { nowMs: 1_000, now: () => clock.nowMs };
    return { clock, bus: sloBus(slo, { now: clock.now }) };
  }

  test('tightened duration bound fires at the next sample without resetting the episode', () => {
    const events: LagSloBreachEvent[] = [];
    const { clock, bus } = clockBus({
      maxLagMs: 500,
      breachSamples: 1,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 3; i++) bus.publish('t', i); // t=1000: episode starts
    clock.nowMs = 1_400;
    bus.publish('t', 'x'); // 400ms < 500ms: no breach
    assert.equal(events.length, 0);
    bus.setLagSlo({ maxLagMs: 300 });
    // The episode start is preserved (400ms already elapsed): the next
    // sample at the same clock breaches against the live bound. Had the
    // retune reset the episode, 0ms < 300ms would not fire.
    bus.publish('t', 'y');
    assert.equal(events.length, 1);
    assert.equal(events[0].violated, 'maxLagMs');
    assert.equal(events[0].lag, 5);
  });

  test('loosened duration bound defers firing', () => {
    const events: LagSloBreachEvent[] = [];
    const { clock, bus } = clockBus({
      maxLagMs: 500,
      breachSamples: 1,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 3; i++) bus.publish('t', i); // episode starts at t=1000
    bus.setLagSlo({ maxLagMs: 5_000 });
    clock.nowMs = 1_600;
    bus.publish('t', 'x'); // 600ms < 5000ms: no breach under the new bound
    assert.equal(events.length, 0);
  });
});

test('the onLagSloBreach callback survives a revision', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 1,
    onLagSloBreach: (e) => events.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 6; i++) bus.publish('t', i); // fires at lag 6
  assert.equal(events.length, 1);
  bus.commitOffset('g', 't', 6);
  bus.setLagSlo({ maxLagMessages: 100 });
  for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 1..6: under the new bound
  assert.equal(events.length, 1);
  bus.setLagSlo({ maxLagMessages: 5 });
  bus.publish('t', 'x'); // lag 7 > 5: the same callback fires again
  assert.equal(events.length, 2);
  assert.equal(events[1].lag, 7);
  assert.equal(bus.getStats().lagSloBreaches, 2);
});
