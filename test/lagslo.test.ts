import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import type { LagSloBreachEvent, LagSloOptions } from '../src/grouplag.ts';

/**
 * Consumer-lag SLO alerting (EB-112): `EventBusOptions.groupLag.slo`.
 *
 * One lag evaluation is one SLO sample: group subscribe/rebalance, every
 * publish that advances a group's assignment watermark
 * (`recordGroupOffset`), `commitOffset`, and threshold changes.
 * `commitOffset` requires a positive integer seq.
 */
function sloBus(slo: LagSloOptions, opts?: { now?: () => number }) {
  return new EventBus({ ...(opts?.now === undefined ? {} : { now: opts.now }), groupLag: { slo } });
}

describe('lag SLO alerting (maxLagMessages)', () => {
  test('fires after breachSamples consecutive breach samples', () => {
    const events: LagSloBreachEvent[] = [];
    const bus = sloBus({
      maxLagMessages: 5,
      breachSamples: 2,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 1..6: breach at 6 (1st sample)
    assert.equal(events.length, 0);
    bus.publish('t', 'x'); // lag 7: 2nd consecutive breach sample -> fire
    assert.equal(events.length, 1);
    const [e] = events;
    assert.equal(e.groupId, 'g');
    assert.equal(e.topic, 't');
    assert.equal(e.assignedSeq, 7);
    assert.equal(e.committedSeq, 0);
    assert.equal(e.lag, 7);
    assert.equal(e.violated, 'maxLagMessages');
    assert.equal(e.consecutiveSamples, 2);
    assert.equal(e.breachSamples, 2);
    assert.equal(bus.getStats().lagSloBreaches, 1);
  });

  test('no fire while under the message bound', () => {
    const events: LagSloBreachEvent[] = [];
    const bus = sloBus({
      maxLagMessages: 5,
      breachSamples: 1,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 5; i++) bus.publish('t', i); // lag 1..5, never > 5
    assert.equal(events.length, 0);
    assert.equal(bus.getStats().lagSloBreaches, 0);
  });

  test('edge-triggered latch: no refire while the breach persists, rearm on recovery', () => {
    const events: LagSloBreachEvent[] = [];
    const bus = sloBus({
      maxLagMessages: 5,
      breachSamples: 1,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 10; i++) bus.publish('t', i); // fires at lag 6; latched for 7..10
    assert.equal(events.length, 1);
    bus.commitOffset('g', 't', 10); // lag 0: recovery rearms the latch
    assert.equal(events.length, 1);
    for (let i = 0; i < 6; i++) bus.publish('t', i); // assignedSeq 16, lag 1..6: fires at 6
    assert.equal(events.length, 2);
    assert.equal(events[1].lag, 6);
    assert.equal(bus.getStats().lagSloBreaches, 2);
  });

  test('a single clean sample resets the consecutive-breach debounce', () => {
    const events: LagSloBreachEvent[] = [];
    const bus = sloBus({
      maxLagMessages: 5,
      breachSamples: 3,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 7; i++) bus.publish('t', i); // lag 1..7: breach at 6 (c1), 7 (c2)
    bus.commitOffset('g', 't', 6); // lag 1: clean sample, debounce resets
    assert.equal(events.length, 0);
    // assignedSeq 7, committed 6: next publishes drive lag 2..9.
    for (let i = 0; i < 8; i++) bus.publish('t', i); // breach at lag 6 (c1), 7 (c2), 8 (c3)
    assert.equal(events.length, 1);
    assert.equal(events[0].consecutiveSamples, 3);
    assert.equal(events[0].lag, 8);
  });
});

describe('lag SLO alerting (maxLagMs)', () => {
  function clockBus(slo: LagSloOptions) {
    const clock = { nowMs: 1_000, now: () => clock.nowMs };
    return { clock, bus: sloBus(slo, { now: clock.now }) };
  }

  test('fires when the above-zero-lag episode outlasts maxLagMs', () => {
    const events: LagSloBreachEvent[] = [];
    const { clock, bus } = clockBus({
      maxLagMs: 500,
      breachSamples: 1,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 3; i++) bus.publish('t', i); // t=1000: episode starts, 0ms elapsed
    assert.equal(events.length, 0);
    clock.nowMs = 1_400;
    bus.publish('t', 'x'); // 400ms < 500ms: no breach
    assert.equal(events.length, 0);
    clock.nowMs = 1_600;
    bus.publish('t', 'y'); // 600ms >= 500ms: breach
    assert.equal(events.length, 1);
    assert.equal(events[0].violated, 'maxLagMs');
    assert.equal(events[0].lag, 5);
    assert.equal(bus.getStats().lagSloBreaches, 1);
  });

  test('the episode clock resets when lag clears', () => {
    const events: LagSloBreachEvent[] = [];
    const { clock, bus } = clockBus({
      maxLagMs: 500,
      breachSamples: 1,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 3; i++) bus.publish('t', i); // episode starts at t=1000
    clock.nowMs = 1_400;
    bus.commitOffset('g', 't', 3); // lag 0: episode ends, latch rearms
    clock.nowMs = 1_900;
    bus.publish('t', 'x'); // lag 1: new episode starts at t=1900, 0ms elapsed
    assert.equal(events.length, 0);
    clock.nowMs = 2_500;
    bus.publish('t', 'y'); // 600ms into the new episode
    assert.equal(events.length, 1);
  });

  test("violated is 'both' when the message and duration bounds trip together", () => {
    const events: LagSloBreachEvent[] = [];
    const bus = sloBus({
      maxLagMessages: 1,
      maxLagMs: 0,
      breachSamples: 2,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    bus.publish('t', 'x'); // lag 1: duration bound only (c1)
    assert.equal(events.length, 0);
    bus.publish('t', 'y'); // lag 2: both bounds (c2) -> fire
    assert.equal(events.length, 1);
    assert.equal(events[0].violated, 'both');
    assert.equal(events[0].lag, 2);
  });
});

describe('lag SLO configuration', () => {
  test('slo without a callback is valid but never fires', () => {
    const bus = sloBus({ maxLagMessages: 5 });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 6; i++) bus.publish('t', i); // lag 6 > 5: would breach
    assert.equal(bus.getStats().lagSloBreaches, 0);
  });

  test('slo works without onGroupLag configured', () => {
    const events: LagSloBreachEvent[] = [];
    const bus = sloBus({
      maxLagMessages: 0,
      breachSamples: 1,
      onLagSloBreach: (e) => events.push(e),
    });
    bus.subscribeToGroup('g', 't', () => {});
    bus.publish('t', 'x');
    bus.publish('t', 'y'); // lag 2 > 0
    assert.equal(events.length, 1);
  });

  test('invalid slo options throw RangeError at construction', () => {
    assert.throws(() => sloBus({}), RangeError); // needs a bound
    assert.throws(() => sloBus({ maxLagMessages: -1 }), RangeError);
    assert.throws(() => sloBus({ maxLagMessages: Number.NaN }), RangeError);
    assert.throws(() => sloBus({ maxLagMs: -1 }), RangeError);
    assert.throws(() => sloBus({ maxLagMs: Number.POSITIVE_INFINITY }), RangeError);
    assert.throws(() => sloBus({ maxLagMessages: 5, breachSamples: 0 }), RangeError);
    assert.throws(() => sloBus({ maxLagMessages: 5, breachSamples: 1.5 }), RangeError);
    assert.throws(
      () => sloBus({ maxLagMessages: 5, onLagSloBreach: 'nope' as never }),
      RangeError,
    );
    assert.throws(() => sloBus(null as never), RangeError);
  });

  test('a throwing onLagSloBreach is isolated from the publish path', () => {
    const bus = sloBus({
      maxLagMessages: 0,
      breachSamples: 1,
      onLagSloBreach: () => {
        throw new Error('boom');
      },
    });
    bus.subscribeToGroup('g', 't', () => {});
    bus.publish('t', 'x'); // must not throw
    assert.equal(bus.getStats().lagSloBreaches, 1);
  });

  test('lagSloBreaches is 0 when slo is not configured', () => {
    const bus = new EventBus();
    assert.equal(bus.getStats().lagSloBreaches, 0);
  });
});

describe('lag SLO in prometheus exposition', () => {
  test('breach episodes render as eventbus_consumer_lag_slo_breach_total', () => {
    const bus = sloBus({
      maxLagMessages: 1,
      breachSamples: 1,
      onLagSloBreach: () => {},
    });
    bus.subscribeToGroup('g', 't', () => {});
    for (let i = 0; i < 3; i++) bus.publish('t', i); // fires at lag 2; latched at 3
    bus.commitOffset('g', 't', 3); // lag 0: recovery
    bus.publish('t', 'z'); // lag 1
    bus.publish('t', 'w'); // lag 2: fires again
    const text = renderPrometheus(bus.getStats());
    assert.ok(
      text.includes('eventbus_consumer_lag_slo_breach_total 2'),
      'expected two breach episodes in exposition',
    );
  });

  test('zero breaches render as 0', () => {
    const bus = new EventBus();
    const text = renderPrometheus(bus.getStats());
    assert.ok(text.includes('eventbus_consumer_lag_slo_breach_total 0'));
  });
});
