/**
 * Consumer-group lag alert default threshold revision (EB-363).
 *
 * `bus.setGroupLagDefaultThreshold(thresholdMessages)` retunes the
 * default `onGroupLag` fires above — the threshold every group without
 * a per-group override falls back to — without restarting the bus.
 * Only the default moves: per-group overrides still win, and
 * `clearGroupLagThreshold` falls back to the revised default. The
 * revision re-evaluates every group at once, so a lowered default
 * alerts already-lagging groups immediately and a raised default clears
 * already-triggered latches. A bus with no `groupLag`, or with
 * `groupLag` but no `onGroupLag`, refuses fail-closed; illegal values
 * fail closed with the live default untouched; an identical revision is
 * a no-op. There is no audit event for this family (along EB-48);
 * observe the live default via
 * `getStats().groupLagThresholdFor(groupId)`.
 */
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import {
  DEFAULT_GROUP_LAG_THRESHOLD,
  GroupLagMonitor,
  resolveGroupLagOptions,
  type GroupLagEvent,
} from '../src/grouplag.ts';

/** A bus with lag alerting on, default threshold 100. */
function alertingBus(events: GroupLagEvent[]): EventBus {
  return new EventBus({
    groupLag: { thresholdMessages: 100, onGroupLag: (e) => events.push(e) },
  });
}

describe('GroupLagMonitor default-threshold revision (EB-363)', () => {
  test('defaultThresholdValue is undefined without groupLag or without onGroupLag', () => {
    const none = new GroupLagMonitor(undefined);
    assert.equal(none.defaultThresholdValue(), undefined);
    const silent = new GroupLagMonitor(
      resolveGroupLagOptions({ thresholdMessages: 100 }, 'test'),
    );
    assert.equal(silent.defaultThresholdValue(), undefined);
    const alerting = new GroupLagMonitor(
      resolveGroupLagOptions({ thresholdMessages: 100, onGroupLag: () => {} }, 'test'),
    );
    assert.equal(alerting.defaultThresholdValue(), 100);
  });

  test('setDefaultThreshold moves only the default; per-group overrides still win', () => {
    const monitor = new GroupLagMonitor(
      resolveGroupLagOptions({ thresholdMessages: 100, onGroupLag: () => {} }, 'test'),
    );
    monitor.setThreshold('g', 7);
    monitor.setDefaultThreshold(5);
    assert.equal(monitor.defaultThresholdValue(), 5);
    assert.equal(monitor.thresholdFor('g'), 7);
    assert.equal(monitor.thresholdFor('other'), 5);
    monitor.clearThreshold('g');
    assert.equal(monitor.thresholdFor('g'), 5);
  });

  test('setDefaultThreshold refuses a monitor without alerting', () => {
    const none = new GroupLagMonitor(undefined);
    assert.throws(() => none.setDefaultThreshold(5), RangeError);
    const silent = new GroupLagMonitor(
      resolveGroupLagOptions({ thresholdMessages: 100 }, 'test'),
    );
    assert.throws(() => silent.setDefaultThreshold(5), RangeError);
    assert.equal(silent.defaultThresholdValue(), undefined);
  });
});

describe('bus.setGroupLagDefaultThreshold (EB-363)', () => {
  test('refuses a bus without groupLag or without onGroupLag', () => {
    const plain = new EventBus();
    assert.throws(() => plain.setGroupLagDefaultThreshold(5), /requires EventBusOptions\.groupLag with onGroupLag/);
    assert.equal(plain.getStats().groupLagThresholdFor('g'), DEFAULT_GROUP_LAG_THRESHOLD);
    plain.close();
    const silent = new EventBus({ groupLag: { thresholdMessages: 100 } });
    assert.throws(() => silent.setGroupLagDefaultThreshold(5), /requires EventBusOptions\.groupLag with onGroupLag/);
    assert.equal(silent.getStats().groupLagThresholdFor('g'), 100);
    silent.close();
  });

  test('illegal values are rejected and the live default is untouched', () => {
    const events: GroupLagEvent[] = [];
    const bus = alertingBus(events);
    for (const bad of [-1, Number.NaN, Infinity, -Infinity, '5', null, undefined]) {
      assert.throws(() => bus.setGroupLagDefaultThreshold(bad as never), RangeError);
    }
    assert.equal(bus.getStats().groupLagThresholdFor('g'), 100);
    assert.equal(events.length, 0);
    bus.close();
  });

  test('lowering the default alerts an already-lagging group at once', () => {
    const events: GroupLagEvent[] = [];
    const bus = alertingBus(events);
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 10; i++) bus.publish('t', i);
    assert.equal(events.length, 0);
    bus.setGroupLagDefaultThreshold(5); // lag 10 already above: alerts now
    assert.equal(events.length, 1);
    assert.equal(events[0].lag, 10);
    assert.equal(events[0].groupId, 'g');
    assert.equal(bus.getStats().groupLagThresholdFor('g'), 5);
    bus.close();
  });

  test('raising the default clears an already-triggered latch', () => {
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({
      groupLag: { thresholdMessages: 2, onGroupLag: (e) => events.push(e) },
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 3; i++) bus.publish('t', i);
    assert.equal(events.length, 1); // lag 3 > 2: latched
    bus.setGroupLagDefaultThreshold(100); // lag 3 back below: latch clears silently
    assert.equal(events.length, 1);
    assert.equal(bus.getStats().groupLagThresholdFor('g'), 100);
    bus.setGroupLagDefaultThreshold(2); // lag 3 above again: refires
    assert.equal(events.length, 2);
    assert.equal(events[1].lag, 3);
    bus.close();
  });

  test('per-group overrides still win over the revised default', () => {
    const events: GroupLagEvent[] = [];
    const bus = new EventBus({
      groupLag: {
        thresholdMessages: 100,
        thresholds: { fast: 1 },
        onGroupLag: (e) => events.push(e),
      },
    });
    bus.subscribeToGroup('fast', 't', () => {});
    bus.subscribeToGroup('slow', 't', () => {});
    bus.setGroupLagDefaultThreshold(50);
    assert.equal(bus.getStats().groupLagThresholdFor('fast'), 1);
    assert.equal(bus.getStats().groupLagThresholdFor('slow'), 50);
    assert.equal(bus.getStats().groupLagThresholdFor('unknown'), 50);
    bus.publish('t', 1);
    bus.publish('t', 2);
    assert.deepEqual(events.map((e) => e.groupId), ['fast']);
    bus.close();
  });

  test('clearThreshold falls back to the revised default', () => {
    const events: GroupLagEvent[] = [];
    const bus = alertingBus(events);
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 10; i++) bus.publish('t', i);
    bus.setGroupLagThreshold('g', 5); // lag 10 above the override: alerts
    assert.equal(events.length, 1);
    bus.setGroupLagDefaultThreshold(50); // re-evaluates: lag 10 below 50, latch clears
    assert.equal(events.length, 1);
    assert.equal(bus.getStats().groupLagThresholdFor('g'), 5);
    bus.clearGroupLagThreshold('g'); // back to the revised default 50
    assert.equal(bus.getStats().groupLagThresholdFor('g'), 50);
    assert.equal(events.length, 1); // lag 10 below 50: no alert
    bus.close();
  });

  test('an identical revision is a no-op: no alert, no state change', () => {
    const events: GroupLagEvent[] = [];
    const bus = alertingBus(events);
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 10; i++) bus.publish('t', i);
    bus.setGroupLagDefaultThreshold(100); // same as the current default
    assert.equal(events.length, 0);
    assert.equal(bus.getStats().groupLagThresholdFor('g'), 100);
    // The bus still alerts normally afterwards.
    bus.setGroupLagDefaultThreshold(5);
    assert.equal(events.length, 1);
    bus.close();
  });

  test('no audit event is recorded for a default-threshold revision', () => {
    const events: GroupLagEvent[] = [];
    const bus = alertingBus(events);
    bus.subscribeToGroup('g', 't', () => {});
    bus.setGroupLagDefaultThreshold(5);
    bus.setGroupLagDefaultThreshold(50);
    bus.setGroupLagDefaultThreshold(100);
    const lagEvents = bus.getSubscriptionAudit().filter((e) => e.action.includes('lag'));
    assert.deepEqual(lagEvents, []);
    bus.close();
  });
});
