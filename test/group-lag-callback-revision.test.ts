/**
 * Bus-level consumer-group lag threshold-alert callback live replacement (EB-417).
 *
 * `setGroupLagCallback` swaps the `onGroupLag` paging hook without
 * restarting the bus. Only a bus that enabled `groupLag` can be revised
 * — the gate is `groupLag` being configured, not a callback already
 * existing, so a silent `groupLag` can gain one. The new callback
 * governs from the next lag evaluation; the alarm latch is fully
 * preserved — a latched row never re-fires under the new callback, a
 * silent bus that gains a callback fires on the next evaluation for
 * every row already over threshold, and `null` freezes the latch
 * rather than resetting it. Clearing also makes
 * `setGroupLagDefaultThreshold` refuse until a callback is reinstalled,
 * while the SLO keeps paging and `lagSloBreaches` keeps climbing. The
 * callback function itself is never written to the audit log: only
 * whether one is installed is recorded (`{ hasCallback, oldHasCallback
 * }`). Re-installing the live callback is a no-op with no audit.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { GroupLagEvent, GroupLagMonitorOptions } from '../src/grouplag.ts';

function lagBus(options: GroupLagMonitorOptions) {
  return new EventBus({ groupLag: options });
}

/** One lag sample per publish: group member assigned, never committed. */
function publishLag(bus: EventBus, count: number): void {
  for (let i = 0; i < count; i++) bus.publish('t', i);
}

test('setGroupLagCallback fails closed when groupLag is not enabled', () => {
  const cb = () => {};
  const plain = new EventBus();
  assert.throws(() => plain.setGroupLagCallback(cb), RangeError);
  assert.equal(plain.getSubscriptionAudit({ action: 'group_lag_callback_revised' }).length, 0);
});

test('setGroupLagCallback rejects an illegal actor', () => {
  const bus = lagBus({ thresholdMessages: 5 });
  assert.throws(() => bus.setGroupLagCallback(() => {}, ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_callback_revised' }).length, 0);
});

test('a closed bus refuses the swap', async () => {
  const bus = lagBus({ thresholdMessages: 5 });
  await bus.close();
  assert.throws(() => bus.setGroupLagCallback(() => {}), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_callback_revised' }).length, 0);
});

test('setGroupLagCallback rejects non-functions and leaves the live callback untouched', () => {
  const events: GroupLagEvent[] = [];
  const bus = lagBus({ thresholdMessages: 5, onGroupLag: (e) => events.push(e) });
  bus.subscribeToGroup('g', 't', () => {});
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const callback of bad) {
    assert.throws(
      () => bus.setGroupLagCallback(callback as never),
      RangeError,
      `expected RangeError for ${String(callback)}`,
    );
  }
  // The failed calls changed nothing: the original callback still
  // pages the sixth publish, and the audit log is untouched.
  publishLag(bus, 6);
  assert.equal(events.length, 1);
  assert.equal(events[0].lag, 6);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_callback_revised' }).length, 0);
});

test('re-installing the live callback is a no-op with no audit', () => {
  const cb = () => {};
  const bus = lagBus({ thresholdMessages: 5, onGroupLag: cb });
  bus.setGroupLagCallback(cb);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_callback_revised' }).length, 0);
});

test('clearing a bus with no callback is a no-op with no audit', () => {
  const bus = lagBus({ thresholdMessages: 5 });
  bus.setGroupLagCallback(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_lag_callback_revised' }).length, 0);
  assert.equal(bus.getStats().groupLagHasCallback, false);
});

test('a silent groupLag can gain a callback, which pages already-lagging rows at once', () => {
  // Valid but silent (EB-48): no callback, so nothing ever fires —
  // but `groupLag` is configured, so a callback can be installed.
  const events: GroupLagEvent[] = [];
  const bus = lagBus({ thresholdMessages: 5 });
  assert.equal(bus.getStats().groupLagHasCallback, false);
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // lag 6 > 5, but silent: no callback yet
  assert.equal(events.length, 0);
  bus.setGroupLagCallback((e) => events.push(e));
  assert.equal(bus.getStats().groupLagHasCallback, true);
  bus.publish('t', 'x'); // lag 7: the next sample fires the new callback
  assert.equal(events.length, 1);
  assert.equal(events[0].groupId, 'g');
  assert.equal(events[0].topic, 't');
  assert.equal(events[0].lag, 7);
});

test('replacing the callback retires the old one, with the exact lag event', () => {
  const oldEvents: GroupLagEvent[] = [];
  const newEvents: GroupLagEvent[] = [];
  const bus = lagBus({ thresholdMessages: 5, onGroupLag: (e) => oldEvents.push(e) });
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // fires the old callback
  assert.equal(oldEvents.length, 1);
  bus.setGroupLagCallback((e) => newEvents.push(e));
  bus.commitOffset('g', 't', 6); // recovery rearms the latch
  publishLag(bus, 6); // lag 6: fires the new callback
  assert.equal(oldEvents.length, 1);
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].groupId, 'g');
  assert.equal(newEvents[0].topic, 't');
  assert.equal(newEvents[0].lag, 6);
});

test('clearing silences threshold paging while the SLO keeps paging', () => {
  const oldEvents: GroupLagEvent[] = [];
  const sloEvents: Array<{ lag: number }> = [];
  const bus = lagBus({
    thresholdMessages: 5,
    onGroupLag: (e) => oldEvents.push(e),
    slo: {
      maxLagMessages: 5,
      breachSamples: 1,
      onLagSloBreach: (e) => sloEvents.push(e),
    },
  });
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // fires the threshold callback and the SLO breach
  assert.equal(oldEvents.length, 1);
  assert.equal(sloEvents.length, 1);
  assert.equal(bus.getStats().lagSloBreaches, 1);
  bus.setGroupLagCallback(null);
  assert.equal(bus.getStats().groupLagHasCallback, false);
  bus.commitOffset('g', 't', 6); // recovery while silent
  publishLag(bus, 6); // would page — but silent; the SLO still fires
  assert.equal(oldEvents.length, 1);
  assert.equal(sloEvents.length, 2);
  assert.equal(bus.getStats().lagSloBreaches, 2);
  // The lag rows themselves are still computed on demand, unaffected
  // by the frozen threshold latch.
  assert.equal(bus.getStats().groupLag.length, 1);
  assert.equal(bus.getStats().groupLag[0].lag, 6);
});

test('a latched row never re-fires under the new callback', () => {
  const oldEvents: GroupLagEvent[] = [];
  const newEvents: GroupLagEvent[] = [];
  const bus = lagBus({ thresholdMessages: 5, onGroupLag: (e) => oldEvents.push(e) });
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // fires, row latched
  assert.equal(oldEvents.length, 1);
  bus.setGroupLagCallback((e) => newEvents.push(e));
  publishLag(bus, 3); // still over threshold: no re-fire under either callback
  assert.equal(oldEvents.length, 1);
  assert.equal(newEvents.length, 0);
  bus.commitOffset('g', 't', 9); // recovery rearms the latch
  publishLag(bus, 6); // fresh excursion: the new callback fires
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].lag, 6);
});

test('clearing refuses setGroupLagDefaultThreshold until a callback returns', () => {
  const cb = (e: GroupLagEvent) => void e;
  const bus = lagBus({ thresholdMessages: 5, onGroupLag: cb });
  bus.setGroupLagCallback(null);
  assert.throws(() => bus.setGroupLagDefaultThreshold(10), RangeError);
  assert.equal(bus.getStats().groupLagThresholdFor('g'), 5);
  bus.setGroupLagCallback(cb);
  bus.setGroupLagDefaultThreshold(10);
  assert.equal(bus.getStats().groupLagThresholdFor('g'), 10);
});

test('the swap audits exact presence flags with the actor', () => {
  const bus = lagBus({ thresholdMessages: 5 });
  bus.setGroupLagCallback(() => {}, 'ops');
  bus.setGroupLagCallback(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'group_lag_callback_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { hasCallback: false, oldHasCallback: true });
  assert.equal(audit[1].actor, 'ops');
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('setGroupLagCallback'));
});

test('getStats().groupLagHasCallback follows the live value', () => {
  const bus = lagBus({ thresholdMessages: 5 });
  assert.equal(bus.getStats().groupLagHasCallback, false);
  bus.setGroupLagCallback(() => {});
  assert.equal(bus.getStats().groupLagHasCallback, true);
  bus.setGroupLagCallback(null);
  assert.equal(bus.getStats().groupLagHasCallback, false);
  // A bus without groupLag reports false, not undefined.
  const plain = new EventBus();
  assert.equal(plain.getStats().groupLagHasCallback, false);
});

test('swapping the callback leaves the thresholds, SLO bounds, and breach count untouched', () => {
  const oldEvents: GroupLagEvent[] = [];
  const bus = lagBus({
    thresholdMessages: 5,
    onGroupLag: (e) => oldEvents.push(e),
    slo: { maxLagMessages: 5, breachSamples: 1, onLagSloBreach: () => {} },
  });
  bus.setGroupLagThreshold('g', 3);
  bus.subscribeToGroup('g', 't', () => {});
  publishLag(bus, 6); // fires the old callback (lag 6 > 3)
  assert.equal(oldEvents.length, 1);
  assert.equal(bus.getStats().lagSloBreaches, 1);
  bus.setGroupLagCallback(() => {});
  // The default and per-group thresholds are exactly what they were,
  // the SLO bounds restate as a no-op (no audit), and the breach
  // count is unchanged.
  assert.equal(bus.getStats().groupLagThresholdFor('g'), 3);
  assert.equal(bus.getStats().groupLagThresholdFor('other'), 5);
  bus.setLagSlo({ maxLagMessages: 5, breachSamples: 1 });
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_slo_revised' }).length, 0);
  assert.equal(bus.getStats().lagSloBreaches, 1);
  assert.equal(bus.getStats().groupLagHasCallback, true);
});
