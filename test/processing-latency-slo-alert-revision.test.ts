/**
 * Bus-level processing-latency SLO alert switch live revision (EB-331).
 *
 * `setProcessingLatencySloAlert(enabled)` arms or disarms SLO-miss
 * phase attribution without restarting the bus. Only subscriptions
 * created after the revision read the live switch: a `latencySlo`
 * subscriber created while the switch is on builds a `sloPhase` and
 * stamps phases, one created while it is off builds none.
 * Subscriptions already built are untouched — an armed subscriber
 * keeps its `sloPhase` (and keeps attributing misses) after a
 * disable, and an unarmed subscriber gains none after an enable.
 * A non-boolean value throws `RangeError` with the live switch
 * untouched; an identical revision is a no-op with no audit.
 * Every success is audited as `processing_latency_slo_alert_revised`
 * (`{ enabled, oldEnabled }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { LatencySloMissEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'processing_latency_slo_alert_revised' });

/**
 * Subscribes a `latencySlo` tracker that records every miss, publishes
 * one message whose handler takes 50ms (past the 10ms threshold), and
 * returns the miss events. A miss carries `phaseAttribution` exactly
 * when the subscriber was built with a `sloPhase`.
 */
async function missEvents(
  bus: EventBus,
  clock: { nowMs: number },
  topic = 't',
): Promise<LatencySloMissEvent[]> {
  const events: LatencySloMissEvent[] = [];
  bus.subscribe(topic, () => {
    clock.nowMs += 50; // 50ms of handler work — past the 10ms threshold
  }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 10, onLatencySloMiss: (e) => events.push(e) },
  });
  bus.publish(topic, 'a');
  await flush();
  return events;
}

test('a non-boolean revision throws RangeError and leaves the live switch untouched', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  // Off by default: a bad revision cannot arm it.
  for (const bad of [1, 0, 'true', null, undefined, {}, []]) {
    assert.throws(() => bus.setProcessingLatencySloAlert(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  // Still off: a new subscriber builds no sloPhase.
  const offEvents = await missEvents(bus, clock, 't-off');
  assert.equal(offEvents.length, 1);
  assert.ok(!('phaseAttribution' in offEvents[0]));

  // On: a bad revision cannot disarm it.
  bus.setProcessingLatencySloAlert(true);
  assert.equal(audit(bus).length, 1);
  for (const bad of [1, 'false', null, undefined, {}]) {
    assert.throws(() => bus.setProcessingLatencySloAlert(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 1);
  // Still on: a new subscriber builds a sloPhase.
  const onEvents = await missEvents(bus, clock, 't-on');
  assert.equal(onEvents.length, 1);
  assert.ok(onEvents[0].phaseAttribution !== undefined);
  assert.equal(onEvents[0].phaseAttribution.handlerMs.p99Ms, 50);
});

test('an identical revision is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setProcessingLatencySloAlert(false);
  assert.equal(audit(bus).length, 0);

  bus.setProcessingLatencySloAlert(true);
  assert.equal(audit(bus).length, 1);
  bus.setProcessingLatencySloAlert(true);
  assert.equal(audit(bus).length, 1);

  bus.setProcessingLatencySloAlert(false);
  assert.equal(audit(bus).length, 2);
  bus.setProcessingLatencySloAlert(false);
  assert.equal(audit(bus).length, 2);
});

test('enable arms only subscriptions created after the revision', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setProcessingLatencySloAlert(true);
  const events = await missEvents(bus, clock);
  assert.equal(events.length, 1);
  const attribution = events[0].phaseAttribution;
  assert.ok(attribution !== undefined);
  assert.equal(attribution.handlerMs.p99Ms, 50);
  assert.equal(attribution.queueWaitMs.p99Ms, 0);
  // Stats mirror the attributed miss.
  assert.equal(bus.getStats().sloMisses, 1);
  assert.equal(bus.getStats().sloMissesByPhase.length, 1);
});

test('disable disarms only subscriptions created after the revision', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, processingLatencySloAlert: true });
  bus.setProcessingLatencySloAlert(false);
  const events = await missEvents(bus, clock);
  assert.equal(events.length, 1);
  assert.ok(!('phaseAttribution' in events[0]));
  // The miss itself still fires (EB-53 tracking is untouched), but the
  // attribution counters stay empty: `sloMisses` counts only attributed
  // misses (EB-133).
  assert.equal(bus.getStats().sloMisses, 0);
  assert.deepEqual(bus.getStats().sloMissesByPhase, []);
});

test('a pre-existing armed subscriber keeps its sloPhase after a disable', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, processingLatencySloAlert: true });
  const events: LatencySloMissEvent[] = [];
  bus.subscribe('t', () => {
    clock.nowMs += 50;
  }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 10, onLatencySloMiss: (e) => events.push(e) },
  });
  bus.setProcessingLatencySloAlert(false);
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1);
  // Still armed: the miss carries phase attribution.
  assert.ok(events[0].phaseAttribution !== undefined);
  assert.equal(events[0].phaseAttribution.handlerMs.p99Ms, 50);
  assert.equal(bus.getStats().sloMisses, 1);
  assert.equal(bus.getStats().sloMissesByPhase.length, 1);
});

test('a pre-existing unarmed subscriber gains no sloPhase after an enable', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  bus.subscribe('t', () => {
    clock.nowMs += 50;
  }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 10, onLatencySloMiss: (e) => events.push(e) },
  });
  bus.setProcessingLatencySloAlert(true);
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1);
  // Still unarmed: no phase attribution is stamped.
  assert.ok(!('phaseAttribution' in events[0]));
  // The miss itself still fires (EB-53 tracking is untouched), but the
  // attribution counters stay empty: `sloMisses` counts only attributed
  // misses (EB-133).
  assert.equal(bus.getStats().sloMisses, 0);
  assert.deepEqual(bus.getStats().sloMissesByPhase, []);
});

test('the audit event carries enabled and oldEnabled', () => {
  const bus = new EventBus();
  bus.setProcessingLatencySloAlert(true);
  bus.setProcessingLatencySloAlert(false);
  bus.setProcessingLatencySloAlert(true);
  const entries = audit(bus);
  assert.equal(entries.length, 3);
  assert.deepEqual(entries[0].detail, { enabled: true, oldEnabled: false });
  assert.deepEqual(entries[1].detail, { enabled: false, oldEnabled: true });
  assert.deepEqual(entries[2].detail, { enabled: true, oldEnabled: false });
  assert.equal(entries[0].actor, 'system');
});

test('constructor arming still uses `=== true`', async () => {
  const clock = controllableClock();
  // Armed at construction: new subscribers stamp phases with no revision.
  const armed = new EventBus({ now: clock.now, processingLatencySloAlert: true });
  const armedEvents = await missEvents(armed, clock, 't-armed');
  assert.equal(armedEvents.length, 1);
  assert.ok(armedEvents[0].phaseAttribution !== undefined);

  // A truthy non-`true` value does not arm the switch at construction.
  const truthy = new EventBus({ now: clock.now, processingLatencySloAlert: 1 as never });
  const truthyEvents = await missEvents(truthy, clock, 't-truthy');
  assert.equal(truthyEvents.length, 1);
  assert.ok(!('phaseAttribution' in truthyEvents[0]));
  // ...but the live revision can still arm it afterwards.
  truthy.setProcessingLatencySloAlert(true);
  const revisedEvents = await missEvents(truthy, clock, 't-revised');
  assert.equal(revisedEvents.length, 1);
  assert.ok(revisedEvents[0].phaseAttribution !== undefined);
});
