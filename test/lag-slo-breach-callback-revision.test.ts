/**
 * Bus-level consumer-lag SLO breach callback live replacement (EB-415).
 *
 * `setLagSloBreachCallback` swaps the `onLagSloBreach` paging hook
 * without restarting the bus. Only a bus that enabled `groupLag.slo`
 * can be revised — the gate is the SLO being configured, not a
 * callback already existing, so a bounds-only SLO can gain one. The
 * new callback governs from the next lag evaluation; episode state is
 * fully preserved — a latched episode never re-fires under the new
 * callback, a mid-debounce swap hands the remaining samples over,
 * and `null` freezes the episode state rather than resetting it. The
 * callback function itself is never written to the audit log: only
 * whether one is installed is recorded (`{ hasCallback, oldHasCallback
 * }`). Re-installing the live callback is a no-op with no audit.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { RecordingAlertSink } from '../src/ops-alert.ts';
import type { LagSloBreachEvent, LagSloOptions } from '../src/grouplag.ts';

function sloBus(slo: LagSloOptions, opts?: { now?: () => number }) {
  return new EventBus({ ...(opts?.now === undefined ? {} : { now: opts.now }), groupLag: { slo } });
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** One lag sample per publish: group member assigned, never committed. */
function publishLag(bus: EventBus, count: number): void {
  for (let i = 0; i < count; i++) bus.publish('t', i);
}

test('setLagSloBreachCallback fails closed when groupLag.slo is not enabled', () => {
  const cb = () => {};
  const plain = new EventBus();
  assert.throws(() => plain.setLagSloBreachCallback(cb), RangeError);
  assert.equal(plain.getSubscriptionAudit({ action: 'lag_slo_breach_callback_revised' }).length, 0);

  // groupLag alerting without an SLO is not an SLO: still refused.
  const lagOnly = new EventBus({ groupLag: { thresholdMessages: 10 } });
  assert.throws(() => lagOnly.setLagSloBreachCallback(cb), RangeError);
  const hookOnly = new EventBus({ groupLag: { onGroupLag: () => {} } });
  assert.throws(() => hookOnly.setLagSloBreachCallback(cb), RangeError);
  assert.equal(hookOnly.getSubscriptionAudit({ action: 'lag_slo_breach_callback_revised' }).length, 0);
});

test('setLagSloBreachCallback rejects an illegal actor', () => {
  const bus = sloBus({ maxLagMessages: 5 });
  assert.throws(() => bus.setLagSloBreachCallback(() => {}, ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_breach_callback_revised' }).length, 0);
});

test('a closed bus refuses the swap', async () => {
  const bus = sloBus({ maxLagMessages: 5 });
  await bus.close();
  assert.throws(() => bus.setLagSloBreachCallback(() => {}), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_breach_callback_revised' }).length, 0);
});

test('setLagSloBreachCallback rejects non-functions and leaves the live callback untouched', () => {
  const events: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 1,
    onLagSloBreach: (e) => events.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const callback of bad) {
    assert.throws(
      () => bus.setLagSloBreachCallback(callback as never),
      RangeError,
      `expected RangeError for ${String(callback)}`,
    );
  }
  // The failed calls changed nothing: the original callback still
  // pages the sixth publish, and the audit log is untouched.
  publishLag(bus, 6);
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 6);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_breach_callback_revised' }).length, 0);
});

test('a bounds-only SLO can gain a callback', () => {
  // Valid but silent (EB-112): no callback, so nothing ever fires —
  // but the SLO is configured, so a callback can be installed.
  const events: LagSloBreachEvent[] = [];
  const bus = sloBus({ maxLagMessages: 5, breachSamples: 1 });
  assert.equal(bus.getStats().lagSloHasBreachCallback, false);
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // would breach, but silent: no callback yet
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().lagSloBreaches, 0);
  bus.setLagSloBreachCallback((e) => events.push(e));
  assert.equal(bus.getStats().lagSloHasBreachCallback, true);
  bus.publish('t', 'x'); // lag 7: the next sample fires the new callback
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 7);
  assert.equal(events[0].violated, 'maxLagMessages');
  assert.equal(bus.getStats().lagSloBreaches, 1);
});

test('re-installing the live callback is a no-op with no audit', () => {
  const cb = () => {};
  const bus = sloBus({ maxLagMessages: 5, onLagSloBreach: cb });
  bus.setLagSloBreachCallback(cb);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_breach_callback_revised' }).length, 0);
});

test('clearing a bus with no callback is a no-op with no audit', () => {
  const bus = sloBus({ maxLagMessages: 5 });
  bus.setLagSloBreachCallback(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_breach_callback_revised' }).length, 0);
  assert.equal(bus.getStats().lagSloHasBreachCallback, false);
});

test('replacing the callback retires the old one, with the exact breach event', () => {
  const oldEvents: LagSloBreachEvent[] = [];
  const newEvents: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 1,
    onLagSloBreach: (e) => oldEvents.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // fires the old callback
  assert.equal(oldEvents.length, 1);
  bus.setLagSloBreachCallback((e) => newEvents.push(e));
  bus.commitOffset('g', 't', 6); // recovery rearms the latch
  publishLag(bus, 6); // lag 6: fires the new callback
  assert.equal(oldEvents.length, 1);
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].groupId, 'g');
  assert.equal(newEvents[0].topic, 't');
  assert.equal(newEvents[0].lag, 6);
  assert.equal(newEvents[0].violated, 'maxLagMessages');
  assert.equal(newEvents[0].consecutiveSamples, 1);
  assert.equal(newEvents[0].breachSamples, 1);
  assert.equal(bus.getStats().lagSloBreaches, 2);
});

test('clearing silences paging and freezes the counter; reinstall resumes counting', () => {
  const oldEvents: LagSloBreachEvent[] = [];
  const newEvents: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 1,
    onLagSloBreach: (e) => oldEvents.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // fires the old callback
  assert.equal(oldEvents.length, 1);
  assert.equal(bus.getStats().lagSloBreaches, 1);
  bus.setLagSloBreachCallback(null);
  bus.commitOffset('g', 't', 6); // recovery while silent
  publishLag(bus, 6); // would breach — but silent, and the counter is frozen
  assert.equal(oldEvents.length, 1);
  assert.equal(newEvents.length, 0);
  assert.equal(bus.getStats().lagSloBreaches, 1);
  // Reinstall: counting resumes — the next fresh breach pages the new
  // callback and moves the counter.
  bus.setLagSloBreachCallback((e) => newEvents.push(e));
  bus.commitOffset('g', 't', 12);
  publishLag(bus, 6);
  assert.equal(newEvents.length, 1);
  assert.equal(bus.getStats().lagSloBreaches, 2);
});

test('a latched episode never re-fires under the new callback', () => {
  const oldEvents: LagSloBreachEvent[] = [];
  const newEvents: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 1,
    onLagSloBreach: (e) => oldEvents.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // fires, episode latched
  assert.equal(oldEvents.length, 1);
  bus.setLagSloBreachCallback((e) => newEvents.push(e));
  publishLag(bus, 3); // still breaching: no re-fire under either callback
  assert.equal(oldEvents.length, 1);
  assert.equal(newEvents.length, 0);
  bus.commitOffset('g', 't', 9); // recovery rearms the latch
  publishLag(bus, 6); // fresh breach: the new callback fires
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].lag, 6);
});

test('a mid-debounce swap hands the remaining samples to the new callback', () => {
  const oldEvents: LagSloBreachEvent[] = [];
  const newEvents: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    breachSamples: 3,
    onLagSloBreach: (e) => oldEvents.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 7); // breach at lag 6 (c1), 7 (c2) — below the debounce
  assert.equal(oldEvents.length, 0);
  bus.setLagSloBreachCallback((e) => newEvents.push(e));
  bus.publish('t', 'x'); // lag 8: c3 — the new callback fires, not a reset to c1
  assert.equal(oldEvents.length, 0);
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].consecutiveSamples, 3);
  assert.equal(newEvents[0].breachSamples, 3);
  assert.equal(newEvents[0].lag, 8);
});

test('the ops-alert outlet follows the new callback: exactly one note per episode', async () => {
  const sink = new RecordingAlertSink();
  const oldEvents: LagSloBreachEvent[] = [];
  const newEvents: LagSloBreachEvent[] = [];
  let now = 1_000;
  const bus = new EventBus({
    busId: 'ops-lag-415',
    now: () => now,
    opsAlert: { sink, secret: 's', dedupWindowMs: 1_000 },
    groupLag: {
      slo: {
        maxLagMessages: 2,
        breachSamples: 1,
        onLagSloBreach: (e) => oldEvents.push(e),
      },
    },
  });
  bus.subscribeToGroup('g1', 't', () => {});
  publishLag(bus, 3); // lag 3 > 2: breach fires the old hook + one note
  // The note's delivery is detached (`OpsAlerter.deliver` awaits the
  // sink): the hook fires synchronously, the `sent` count lands on a
  // microtask — yield once before reading the sink stats.
  await flush();
  assert.equal(oldEvents.length, 1);
  assert.equal(sink.deliveries.length, 1);
  assert.equal(sink.deliveries[0].alert.kind, 'lag-slo-breach');
  assert.equal(bus.getStats().opsAlert.sent, 1);
  assert.equal(bus.getStats().opsAlert.suppressed, 0);
  // Swap: the replacement is wrapped exactly once — the next episode
  // notes once, not twice (a double wrap would show as a suppression).
  bus.setLagSloBreachCallback((e) => newEvents.push(e));
  bus.commitOffset('g1', 't', 3);
  now += 2_000; // past the dedup window, so the next note is not collapsed
  publishLag(bus, 3);
  await flush();
  assert.equal(oldEvents.length, 1);
  assert.equal(newEvents.length, 1);
  assert.equal(sink.deliveries.length, 2);
  assert.equal(bus.getStats().opsAlert.sent, 2);
  assert.equal(bus.getStats().opsAlert.suppressed, 0);
  // Clearing silences the outlet too: no hook, no note.
  bus.setLagSloBreachCallback(null);
  bus.commitOffset('g1', 't', 6);
  now += 2_000;
  publishLag(bus, 3);
  await flush();
  assert.equal(newEvents.length, 1);
  assert.equal(sink.deliveries.length, 2);
  assert.equal(bus.getStats().opsAlert.sent, 2);
});

describe('lag SLO callback revision (maxLagMs freeze)', () => {
  test('clearing freezes the episode: reinstall may trip maxLagMs at once', () => {
    const clock = { nowMs: 1_000, now: () => clock.nowMs };
    const oldEvents: LagSloBreachEvent[] = [];
    const newEvents: LagSloBreachEvent[] = [];
    const bus = sloBus(
      {
        maxLagMs: 500,
        breachSamples: 1,
        onLagSloBreach: (e) => oldEvents.push(e),
      },
      { now: clock.now },
    );
    bus.subscribeToGroup('g', 't', () => {});
    publishLag(bus, 3); // episode starts at t=1000
    clock.nowMs = 1_400;
    bus.publish('t', 'x'); // 400ms < 500ms: no breach yet
    assert.equal(oldEvents.length, 0);
    // Clear mid-episode: the episode state freezes — no evaluation
    // runs while the callback is gone, so the start stays at t=1000.
    bus.setLagSloBreachCallback(null);
    clock.nowMs = 2_000; // 1000ms elapsed since the frozen episode start
    bus.publish('t', 'y'); // frozen: no callback, no fire, counter frozen
    assert.equal(oldEvents.length, 0);
    assert.equal(bus.getStats().lagSloBreaches, 0);
    // Reinstall: the frozen episode start (t=1000) is still the
    // baseline, so the very next sample trips the 500ms bound at once.
    bus.setLagSloBreachCallback((e) => newEvents.push(e));
    bus.publish('t', 'z');
    assert.equal(newEvents.length, 1);
    assert.equal(newEvents[0].violated, 'maxLagMs');
    assert.equal(bus.getStats().lagSloBreaches, 1);
  });
});

test('the swap audits exact presence flags with the actor', () => {
  const bus = sloBus({ maxLagMessages: 5 });
  bus.setLagSloBreachCallback(() => {}, 'ops');
  bus.setLagSloBreachCallback(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'lag_slo_breach_callback_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { hasCallback: false, oldHasCallback: true });
  assert.equal(audit[1].actor, 'ops');
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('setLagSloBreachCallback'));
});

test('getStats().lagSloHasBreachCallback follows the live value', () => {
  const bus = sloBus({ maxLagMessages: 5 });
  assert.equal(bus.getStats().lagSloHasBreachCallback, false);
  bus.setLagSloBreachCallback(() => {});
  assert.equal(bus.getStats().lagSloHasBreachCallback, true);
  bus.setLagSloBreachCallback(null);
  assert.equal(bus.getStats().lagSloHasBreachCallback, false);
  // A bus without an SLO reports false, not undefined.
  const plain = new EventBus();
  assert.equal(plain.getStats().lagSloHasBreachCallback, false);
});

test('swapping the callback leaves the three numeric bounds and the breach count untouched', () => {
  const oldEvents: LagSloBreachEvent[] = [];
  const bus = sloBus({
    maxLagMessages: 5,
    maxLagMs: 1000,
    breachSamples: 2,
    onLagSloBreach: (e) => oldEvents.push(e),
  });
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 7); // fires at lag 6 (c1), 7 (c2)
  assert.equal(oldEvents.length, 1);
  assert.equal(bus.getStats().lagSloBreaches, 1);
  bus.setLagSloBreachCallback(() => {});
  // The numeric bounds are exactly what they were: restating them via
  // setLagSlo is a no-op (no audit), and the breach count is unchanged.
  bus.setLagSlo({ maxLagMessages: 5, maxLagMs: 1000, breachSamples: 2 });
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
  assert.equal(bus.getStats().lagSloBreaches, 1);
  assert.equal(bus.getStats().lagSloHasBreachCallback, true);
});
