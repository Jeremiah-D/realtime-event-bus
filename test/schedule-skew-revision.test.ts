/**
 * Bus-level schedule-skew observation revision (EB-303).
 *
 * `setScheduleSkew` retunes the delayed-delivery schedule-skew
 * observation live — the rolling window bound, the alert threshold,
 * and the alert callback — without restarting the bus. Only a bus
 * that already enabled `scheduleSkew` can be revised; illegal values
 * fail closed with the live tuning and the audit log untouched; an
 * identical (or empty) revision is a no-op with no audit. The samples
 * are untouched by a revision: a tightened window truncates at the
 * next recorded sample, and a revised threshold / callback governs
 * from the next sweep's alert check.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { ScheduleSkewTracker, type ScheduleSkewEvent } from '../src/scheduleskew.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so skew tests are deterministic instead of racing wall time. Due
 * sweeps run at the start of every flush, so advancing this clock past a
 * `deliverAt` and then publishing anything fans out deterministically.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/**
 * Fans out one delayed schedule with a controlled skew: schedules at
 * the live clock, advances past its due time by `skewMs`, then pokes
 * the bus so the sweep fans it out.
 */
async function fanOutWithSkew(
  bus: EventBus,
  clock: { nowMs: number },
  delayMs: number,
  skewMs: number,
): Promise<void> {
  bus.publishDelayed('t', `skew-${skewMs}`, { delayMs });
  clock.nowMs += delayMs + skewMs;
  bus.publish('t', 'poke');
  await flush();
}

describe('schedule-skew revision (EB-303)', () => {
  it('setScheduleSkew fails closed when scheduleSkew is not enabled', () => {
    const bus = new EventBus();
    assert.throws(() => bus.setScheduleSkew({ windowSize: 5 }), RangeError);
    assert.throws(() => bus.setScheduleSkew({ alertThresholdMs: 10 }), RangeError);
    assert.throws(() => bus.setScheduleSkew({ onScheduleSkew: () => {} }), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );
  });

  it('setScheduleSkew rejects illegal values and leaves the live tuning untouched', async () => {
    const clock = controllableClock();
    const events: ScheduleSkewEvent[] = [];
    const bus = new EventBus({
      now: clock.now,
      scheduleSkew: {
        windowSize: 4,
        alertThresholdMs: 10_000,
        onScheduleSkew: (event) => events.push(event),
      },
    });
    bus.subscribe('t', () => {});
    const bad: Array<unknown> = [
      { windowSize: 0 },
      { windowSize: -1 },
      { windowSize: 1.5 },
      { alertThresholdMs: -1 },
      { alertThresholdMs: Number.NaN },
      { alertThresholdMs: Number.POSITIVE_INFINITY },
      { onScheduleSkew: 'fast' },
      { onScheduleSkew: 42 },
      'nope',
      42,
      ['x'],
    ];
    for (const revision of bad) {
      assert.throws(
        () => bus.setScheduleSkew(revision as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(revision)}`,
      );
    }
    // A callback with no threshold in force can never fire — rejected
    // even when the bus itself armed no threshold.
    const noThreshold = new EventBus({ now: clock.now, scheduleSkew: { windowSize: 4 } });
    assert.throws(
      () => noThreshold.setScheduleSkew({ onScheduleSkew: () => {} }),
      RangeError,
    );
    assert.equal(
      noThreshold.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );
    // The failed calls changed nothing: six fan-outs still keep the
    // last four under the live window, and no alert fired under the
    // live 10s threshold.
    for (let i = 1; i <= 6; i++) {
      bus.publishDelayed('t', `s${i}`, { delayMs: i * 1000 });
    }
    clock.nowMs = 7_050; // skews 5050, 4050, 3050, 2050, 1050, 50
    bus.publish('t', 'poke');
    await flush();
    assert.deepEqual(bus.getStats().delayedScheduleSkew, {
      samples: 4,
      p50Ms: 1050,
      p99Ms: 3050,
      minMs: 50,
      maxMs: 3050,
      meanMs: 1550,
    });
    assert.equal(events.length, 0);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );
  });

  it('setScheduleSkew with identical values or an empty revision is a no-op with no audit', async () => {
    const clock = controllableClock();
    let fired = 0;
    const cb = () => {
      fired += 1;
    };
    const bus = new EventBus({
      now: clock.now,
      scheduleSkew: { windowSize: 4, alertThresholdMs: 1000, onScheduleSkew: cb },
    });
    bus.subscribe('t', () => {});
    bus.setScheduleSkew({ windowSize: 4 });
    bus.setScheduleSkew({ alertThresholdMs: 1000 });
    bus.setScheduleSkew({ onScheduleSkew: cb });
    bus.setScheduleSkew({ windowSize: 4, alertThresholdMs: 1000, onScheduleSkew: cb });
    bus.setScheduleSkew({});
    bus.setScheduleSkew(null as never);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );
    // The untouched tuning still governs: a skew-50 fan-out stays
    // silent under the live 1000ms threshold.
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(fired, 0);
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 1);
  });

  it('retune keeps the samples; a tightened window truncates at the next record', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now, scheduleSkew: { windowSize: 4 } });
    bus.subscribe('t', () => {});
    for (let i = 1; i <= 6; i++) {
      bus.publishDelayed('t', `s${i}`, { delayMs: i * 1000 });
    }
    clock.nowMs = 7_050; // skews 5050, 4050, 3050, 2050, 1050, 50
    bus.publish('t', 'poke');
    await flush();
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 4);
    bus.setScheduleSkew({ windowSize: 2 });
    // The revision itself moved nothing: still four samples.
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 4);
    assert.equal(bus.getStats().delayedScheduleSkew.minMs, 50);
    assert.equal(bus.getStats().delayedScheduleSkew.maxMs, 3050);
    await fanOutWithSkew(bus, clock, 1000, 70);
    // The next record truncated to the two newest: 50 and 70.
    assert.deepEqual(bus.getStats().delayedScheduleSkew, {
      samples: 2,
      p50Ms: 50,
      p99Ms: 70,
      minMs: 50,
      maxMs: 70,
      meanMs: 60,
    });
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 2,
      oldWindowSize: 4,
      alertThresholdMs: undefined,
      oldAlertThresholdMs: undefined,
      hasOnScheduleSkew: false,
      oldHasOnScheduleSkew: false,
    });
  });

  it('tracker retune keeps samples and truncates at the next record', () => {
    const tracker = new ScheduleSkewTracker(4);
    for (const v of [10, 20, 30, 40, 50, 60]) tracker.record(v);
    assert.equal(tracker.summary().samples, 4);
    assert.deepEqual(tracker.current(), { windowSize: 4 });
    tracker.retune(2);
    // Retune moved nothing yet: still four samples.
    assert.equal(tracker.summary().samples, 4);
    assert.deepEqual(tracker.current(), { windowSize: 2 });
    tracker.record(70);
    const s = tracker.summary();
    assert.equal(s.samples, 2);
    assert.equal(s.minMs, 60);
    assert.equal(s.maxMs, 70);
  });

  it('alertThresholdMs revision governs from the next sweep', async () => {
    const clock = controllableClock();
    const events: ScheduleSkewEvent[] = [];
    const bus = new EventBus({
      now: clock.now,
      scheduleSkew: {
        alertThresholdMs: 1000,
        onScheduleSkew: (event) => events.push(event),
      },
    });
    bus.subscribe('t', () => {});
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(events.length, 0); // 50 <= 1000: silent
    bus.setScheduleSkew({ alertThresholdMs: 10 });
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(events.length, 1);
    assert.equal(events[0].skewMs, 50);
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 1024,
      oldWindowSize: 1024,
      alertThresholdMs: 10,
      oldAlertThresholdMs: 1000,
      hasOnScheduleSkew: true,
      oldHasOnScheduleSkew: true,
    });
    // Raising the threshold again silences the next fan-out.
    bus.setScheduleSkew({ alertThresholdMs: 1000 });
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(events.length, 1);
  });

  it('onScheduleSkew revision replaces the callback from the next alert', async () => {
    const clock = controllableClock();
    const first: ScheduleSkewEvent[] = [];
    const second: ScheduleSkewEvent[] = [];
    const bus = new EventBus({
      now: clock.now,
      scheduleSkew: {
        alertThresholdMs: 10,
        onScheduleSkew: (event) => first.push(event),
      },
    });
    bus.subscribe('t', () => {});
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(first.length, 1);
    assert.equal(second.length, 0);
    bus.setScheduleSkew({ onScheduleSkew: (event) => second.push(event) });
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(first.length, 1); // the old callback never fires again
    assert.equal(second.length, 1);
    assert.equal(second[0].skewMs, 50);
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 1024,
      oldWindowSize: 1024,
      alertThresholdMs: 10,
      oldAlertThresholdMs: 10,
      hasOnScheduleSkew: true,
      oldHasOnScheduleSkew: true,
    });
  });

  it('a callback can be armed later on a bus that only set a threshold', async () => {
    const clock = controllableClock();
    const events: ScheduleSkewEvent[] = [];
    const bus = new EventBus({
      now: clock.now,
      scheduleSkew: { alertThresholdMs: 10 },
    });
    bus.subscribe('t', () => {});
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(events.length, 0); // no callback armed yet
    bus.setScheduleSkew({ onScheduleSkew: (event) => events.push(event) });
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(events.length, 1);
    assert.equal(events[0].skewMs, 50);
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].detail.hasOnScheduleSkew, true);
    assert.equal(audit[0].detail.oldHasOnScheduleSkew, false);
  });

  it('a partial revision keeps the fields it leaves out', async () => {
    const clock = controllableClock();
    const events: ScheduleSkewEvent[] = [];
    const bus = new EventBus({
      now: clock.now,
      scheduleSkew: {
        windowSize: 4,
        alertThresholdMs: 10,
        onScheduleSkew: (event) => events.push(event),
      },
    });
    bus.subscribe('t', () => {});
    // Only the window moves; the threshold and callback stay live.
    bus.setScheduleSkew({ windowSize: 2 });
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(events.length, 1);
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.deepEqual(audit[0].detail, {
      windowSize: 2,
      oldWindowSize: 4,
      alertThresholdMs: 10,
      oldAlertThresholdMs: 10,
      hasOnScheduleSkew: true,
      oldHasOnScheduleSkew: true,
    });
  });
});
