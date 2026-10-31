/**
 * Bus-level admission-latency observation revision (EB-304).
 *
 * `setAdmissionLatency` retunes the publish-side admission latency
 * sampling live — the per-topic total-span window, the per-gate
 * window, and the slow-admission threshold — without restarting the
 * bus. Only a bus that already enabled `publishLatency` can be
 * revised; illegal values fail closed with the live tuning and the
 * audit log untouched; an identical (or empty) revision is a no-op
 * with no audit. The samples are untouched by a revision: a tightened
 * window truncates at the next recorded sample, and a revised slow
 * threshold counts from the next sample onward — samples already
 * counted keep the verdict they were recorded under.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { AdmissionLatencyTracker } from '../src/publishlatency.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/**
 * Arms a schema validator that consumes `advanceMs` of bus-clock time
 * per publish, so each publish's total admission span is exactly
 * `advanceMs` (and the schema gate sample matches it). `advances` may
 * be a per-publish sequence; once exhausted it repeats the last value.
 */
function armClockBurningValidator(
  bus: EventBus,
  clock: { nowMs: number },
  advances: number[],
): void {
  let i = 0;
  bus.setTopicSchema('t', () => {
    clock.nowMs += advances[Math.min(i, advances.length - 1)];
    i += 1;
    return true;
  });
}

function totalFor(bus: EventBus, topic = 't') {
  return bus.getStats().publishLatency.find((s) => s.topic === topic)!;
}

function gateFor(bus: EventBus, gate: string, topic = 't') {
  return bus
    .getStats()
    .publishLatencyGates.find((g) => g.topic === topic && g.gate === gate)!;
}

describe('admission-latency revision (EB-304)', () => {
  it('setAdmissionLatency fails closed when publishLatency is not enabled', () => {
    const bus = new EventBus();
    assert.throws(() => bus.setAdmissionLatency({ windowSize: 5 }), RangeError);
    assert.throws(() => bus.setAdmissionLatency({ gateWindowSize: 5 }), RangeError);
    assert.throws(() => bus.setAdmissionLatency({ slowAdmissionThresholdMs: 10 }), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'admission_latency_revised' }).length,
      0,
    );
  });

  it('setAdmissionLatency rejects illegal values and leaves the live tuning untouched', () => {
    const clock = controllableClock();
    const bus = new EventBus({
      now: clock.now,
      publishLatency: { windowSize: 4, gateWindowSize: 2, slowAdmissionThresholdMs: 50 },
    });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10]);
    const bad: Array<unknown> = [
      { windowSize: 0 },
      { windowSize: -1 },
      { windowSize: 1.5 },
      { gateWindowSize: 0 },
      { gateWindowSize: -1 },
      { gateWindowSize: 2.5 },
      { slowAdmissionThresholdMs: -1 },
      { slowAdmissionThresholdMs: Number.NaN },
      { slowAdmissionThresholdMs: Number.POSITIVE_INFINITY },
      'nope',
      42,
      ['x'],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setAdmissionLatency(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    // The failed calls changed nothing: six 10ms spans still keep the
    // last four under the live total window and the last two under the
    // live gate window, and none counts as slow under the live 50ms
    // threshold.
    for (let i = 0; i < 6; i++) bus.publish('t', i);
    assert.equal(totalFor(bus).samples, 4);
    assert.equal(totalFor(bus).p50Ms, 10);
    assert.equal(totalFor(bus).slowCount, 0);
    assert.equal(gateFor(bus, 'schema').samples, 2);
    assert.equal(gateFor(bus, 'schema').p50Ms, 10);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'admission_latency_revised' }).length,
      0,
    );
  });

  it('setAdmissionLatency with identical values or an empty revision is a no-op with no audit', () => {
    const clock = controllableClock();
    const bus = new EventBus({
      now: clock.now,
      publishLatency: { windowSize: 4, gateWindowSize: 2, slowAdmissionThresholdMs: 50 },
    });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10]);
    bus.setAdmissionLatency({ windowSize: 4 });
    bus.setAdmissionLatency({ gateWindowSize: 2 });
    bus.setAdmissionLatency({ slowAdmissionThresholdMs: 50 });
    bus.setAdmissionLatency({ windowSize: 4, gateWindowSize: 2, slowAdmissionThresholdMs: 50 });
    bus.setAdmissionLatency({});
    bus.setAdmissionLatency(null as never);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'admission_latency_revised' }).length,
      0,
    );
    // The untouched tuning still governs.
    bus.publish('t', 1);
    assert.equal(totalFor(bus).samples, 1);
    assert.equal(totalFor(bus).slowCount, 0);
  });

  it('windowSize revision keeps the samples and truncates at the next record', () => {
    const clock = controllableClock();
    const bus = new EventBus({
      now: clock.now,
      publishLatency: { windowSize: 4, gateWindowSize: 2, slowAdmissionThresholdMs: 50 },
    });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10]);
    for (let i = 0; i < 6; i++) bus.publish('t', i);
    assert.equal(totalFor(bus).samples, 4);
    assert.equal(gateFor(bus, 'schema').samples, 2);
    bus.setAdmissionLatency({ windowSize: 2 });
    // The revision itself moved nothing: still four total samples.
    assert.equal(totalFor(bus).samples, 4);
    assert.equal(gateFor(bus, 'schema').samples, 2);
    bus.publish('t', 6);
    // The next record truncated the totals to the two newest; the
    // gate window was never revised and still holds two.
    assert.equal(totalFor(bus).samples, 2);
    assert.equal(totalFor(bus).p50Ms, 10);
    assert.equal(gateFor(bus, 'schema').samples, 2);
    const audit = bus.getSubscriptionAudit({ action: 'admission_latency_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 2,
      oldWindowSize: 4,
      gateWindowSize: 2,
      oldGateWindowSize: 2,
      slowAdmissionThresholdMs: 50,
      oldSlowAdmissionThresholdMs: 50,
    });
  });

  it('gateWindowSize revision truncates the per-gate window at the next record', () => {
    const clock = controllableClock();
    const bus = new EventBus({
      now: clock.now,
      publishLatency: { windowSize: 10, gateWindowSize: 2, slowAdmissionThresholdMs: 50 },
    });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10]);
    for (let i = 0; i < 6; i++) bus.publish('t', i);
    assert.equal(gateFor(bus, 'schema').samples, 2);
    assert.equal(totalFor(bus).samples, 6);
    bus.setAdmissionLatency({ gateWindowSize: 4 });
    // The revision itself moved nothing.
    assert.equal(gateFor(bus, 'schema').samples, 2);
    assert.equal(totalFor(bus).samples, 6);
    bus.publish('t', 6);
    bus.publish('t', 7);
    // The gate window grew to the four newest; the total window was
    // never revised and still holds all eight.
    assert.equal(gateFor(bus, 'schema').samples, 4);
    assert.equal(totalFor(bus).samples, 8);
    const audit = bus.getSubscriptionAudit({ action: 'admission_latency_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 10,
      oldWindowSize: 10,
      gateWindowSize: 4,
      oldGateWindowSize: 2,
      slowAdmissionThresholdMs: 50,
      oldSlowAdmissionThresholdMs: 50,
    });
  });

  it('slowAdmissionThresholdMs revision counts from the next record onward', () => {
    const clock = controllableClock();
    const bus = new EventBus({
      now: clock.now,
      publishLatency: { windowSize: 4, slowAdmissionThresholdMs: 50 },
    });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10]);
    for (let i = 0; i < 4; i++) bus.publish('t', i);
    assert.equal(totalFor(bus).samples, 4);
    assert.equal(totalFor(bus).slowCount, 0); // 10 < 50: none slow
    bus.setAdmissionLatency({ slowAdmissionThresholdMs: 10 });
    bus.publish('t', 4);
    // Only the sample recorded under the new threshold counts: the
    // four older samples keep the verdict they were recorded under.
    assert.equal(totalFor(bus).samples, 4);
    assert.equal(totalFor(bus).slowCount, 1);
    const audit = bus.getSubscriptionAudit({ action: 'admission_latency_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 4,
      oldWindowSize: 4,
      gateWindowSize: 1024,
      oldGateWindowSize: 1024,
      slowAdmissionThresholdMs: 10,
      oldSlowAdmissionThresholdMs: 50,
    });
  });

  it('threshold change plus window tightening keeps the slow count exact on eviction', () => {
    const clock = controllableClock();
    const bus = new EventBus({
      now: clock.now,
      publishLatency: { windowSize: 4, slowAdmissionThresholdMs: 50 },
    });
    bus.subscribe('t', () => {});
    // Spans 60, 60, 5, 5: the two 60s count as slow under threshold 50.
    armClockBurningValidator(bus, clock, [60, 60, 5, 5]);
    for (let i = 0; i < 4; i++) bus.publish('t', i);
    assert.equal(totalFor(bus).samples, 4);
    assert.equal(totalFor(bus).slowCount, 2);
    // Raise the threshold past every sample and tighten the window:
    // the next record truncates to the new bound in one step, evicting
    // both slow 60s at once — and the slow counter must fall back to 0,
    // never negative, never stale.
    bus.setAdmissionLatency({ slowAdmissionThresholdMs: 100, windowSize: 2 });
    bus.publish('t', 4); // span 5, evicts 60, 60, 5 in one step
    assert.equal(totalFor(bus).samples, 2);
    assert.equal(totalFor(bus).slowCount, 0);
    bus.publish('t', 5); // span 5, evicts a 5
    assert.equal(totalFor(bus).samples, 2);
    assert.equal(totalFor(bus).slowCount, 0);
  });

  it('tracker retune keeps samples and applies the new bounds from the next record', () => {
    const tracker = new AdmissionLatencyTracker({
      windowSize: 4,
      gateWindowSize: 2,
      slowAdmissionThresholdMs: 50,
    });
    for (const ms of [60, 60, 5, 5]) tracker.record('t', ms);
    tracker.recordGate('t', 'schema', 60);
    tracker.recordGate('t', 'schema', 60);
    tracker.recordGate('t', 'schema', 60);
    assert.equal(tracker.summary('t').samples, 4);
    assert.equal(tracker.summary('t').slowCount, 2);
    assert.equal(tracker.gateSummary('t').schema.samples, 2);
    assert.deepEqual(tracker.current(), {
      windowSize: 4,
      gateWindowSize: 2,
      slowAdmissionThresholdMs: 50,
    });
    tracker.retune({ windowSize: 2, gateWindowSize: 3, slowAdmissionThresholdMs: 100 });
    // Retune moved nothing yet.
    assert.equal(tracker.summary('t').samples, 4);
    assert.equal(tracker.summary('t').slowCount, 2);
    assert.equal(tracker.gateSummary('t').schema.samples, 2);
    assert.deepEqual(tracker.current(), {
      windowSize: 2,
      gateWindowSize: 3,
      slowAdmissionThresholdMs: 100,
    });
    tracker.record('t', 5);
    // One record truncates the totals to the new bound in one step,
    // evicting both slow 60s — the slow counter falls to 0 exactly.
    assert.equal(tracker.summary('t').samples, 2);
    assert.equal(tracker.summary('t').slowCount, 0);
    tracker.record('t', 5);
    assert.equal(tracker.summary('t').samples, 2);
    assert.equal(tracker.summary('t').slowCount, 0);
    // A gate window created after the retune is built at the new bound.
    tracker.recordGate('t', 'acl', 5);
    tracker.recordGate('t', 'acl', 5);
    tracker.recordGate('t', 'acl', 5);
    tracker.recordGate('t', 'acl', 5);
    assert.equal(tracker.gateSummary('t').acl.samples, 3);
    // A new topic's total window is built at the new bound too.
    tracker.record('u', 5);
    tracker.record('u', 5);
    tracker.record('u', 5);
    assert.equal(tracker.summary('u').samples, 2);
  });

  it('a partial revision keeps the fields it leaves out', () => {
    const clock = controllableClock();
    const bus = new EventBus({
      now: clock.now,
      publishLatency: { windowSize: 4, gateWindowSize: 2, slowAdmissionThresholdMs: 50 },
    });
    bus.subscribe('t', () => {});
    armClockBurningValidator(bus, clock, [10]);
    // Only the slow threshold moves; both windows keep their bounds.
    bus.setAdmissionLatency({ slowAdmissionThresholdMs: 10 });
    for (let i = 0; i < 6; i++) bus.publish('t', i);
    assert.equal(totalFor(bus).samples, 4);
    assert.equal(gateFor(bus, 'schema').samples, 2);
    assert.equal(totalFor(bus).slowCount, 4); // every 10ms span is slow now
    const audit = bus.getSubscriptionAudit({ action: 'admission_latency_revised' });
    assert.deepEqual(audit[0].detail, {
      windowSize: 4,
      oldWindowSize: 4,
      gateWindowSize: 2,
      oldGateWindowSize: 2,
      slowAdmissionThresholdMs: 10,
      oldSlowAdmissionThresholdMs: 50,
    });
  });
});
