/**
 * Schedule-skew live enable (EB-548):
 * `setScheduleSkew` on a bus that never enabled `scheduleSkew`
 * installs the tracker live — no restart, no lost subscriptions —
 * with the revision resolved through the construction-time rule
 * (`resolveScheduleSkewOptions`: `{}` takes every default, a
 * partial object falls back per field, and a callback still requires
 * a threshold in force). The fresh tracker starts with an empty
 * window — skews fanned out before the enable were never recorded,
 * so nothing is sampled retroactively — and it is live-only: a
 * restart restores the unconfigured state. The enable is audited
 * like any revision, with `oldWindowSize` / `oldAlertThresholdMs`
 * `null` and `oldHasOnScheduleSkew` `false`.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { ScheduleSkewEvent } from '../src/scheduleskew.ts';

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

describe('schedule-skew live enable (EB-548)', () => {
  it('the first enable with {} takes the construction defaults', () => {
    const bus = new EventBus();
    bus.setScheduleSkew({});
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].actor, 'system');
    assert.deepEqual(audit[0].detail, {
      windowSize: 1024,
      oldWindowSize: null,
      alertThresholdMs: undefined,
      oldAlertThresholdMs: null,
      hasOnScheduleSkew: false,
      oldHasOnScheduleSkew: false,
    });
    // The tracker is live with an empty window: zeroed stats, ready
    // to sample from the next sweep onward.
    assert.deepEqual(bus.getStats().delayedScheduleSkew, {
      samples: 0,
      p50Ms: 0,
      p99Ms: 0,
      minMs: 0,
      maxMs: 0,
      meanMs: 0,
    });
  });

  it('a partial enable falls back to the defaults per field', () => {
    const bus = new EventBus();
    // Only the threshold moves; the window takes the default 1024
    // and no callback is armed.
    bus.setScheduleSkew({ alertThresholdMs: 10 });
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 1024,
      oldWindowSize: null,
      alertThresholdMs: 10,
      oldAlertThresholdMs: null,
      hasOnScheduleSkew: false,
      oldHasOnScheduleSkew: false,
    });
  });

  it('a partial enable with only a window takes no threshold and no callback', () => {
    const bus = new EventBus();
    bus.setScheduleSkew({ windowSize: 4 });
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      windowSize: 4,
      oldWindowSize: null,
      alertThresholdMs: undefined,
      oldAlertThresholdMs: null,
      hasOnScheduleSkew: false,
      oldHasOnScheduleSkew: false,
    });
  });

  it('the enable audit records the exact detail with old values null/false', () => {
    const bus = new EventBus();
    const events: ScheduleSkewEvent[] = [];
    bus.setScheduleSkew({
      windowSize: 4,
      alertThresholdMs: 10,
      onScheduleSkew: (event) => events.push(event),
    });
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].actor, 'system');
    assert.deepEqual(audit[0].detail, {
      windowSize: 4,
      oldWindowSize: null,
      alertThresholdMs: 10,
      oldAlertThresholdMs: null,
      hasOnScheduleSkew: true,
      oldHasOnScheduleSkew: false,
    });
  });

  it('null on a never-enabled bus is a silent no-op with no audit', () => {
    const bus = new EventBus();
    bus.setScheduleSkew(null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );
    // Still unconfigured: the next valid call enables, and its audit
    // still records the old side as null — the null call installed
    // nothing.
    bus.setScheduleSkew({ windowSize: 4 });
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].detail.oldWindowSize, null);
  });

  it('an illegal value is rejected and the bus stays unconfigured', () => {
    const bus = new EventBus();
    const bad: Array<unknown> = [
      { windowSize: 0 },
      { windowSize: -1 },
      { windowSize: 1.5 },
      { alertThresholdMs: -1 },
      { alertThresholdMs: Number.NaN },
      { alertThresholdMs: Number.POSITIVE_INFINITY },
      // A callback with no threshold in force can never fire.
      { onScheduleSkew: () => {} },
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
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );
    // Still unconfigured: a fan-out leaves the window empty, and a
    // later valid call still enables with the old side null.
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 0);
    bus.setScheduleSkew({ windowSize: 4 });
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].detail.oldWindowSize, null);
  });

  it('after enabling, the next sweep samples with p50/p99 over the new window', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    bus.setScheduleSkew({ windowSize: 2 });
    // The fresh tracker starts empty.
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 0);
    await fanOutWithSkew(bus, clock, 1000, 100);
    await fanOutWithSkew(bus, clock, 1000, 200);
    await fanOutWithSkew(bus, clock, 1000, 300);
    // The window keeps the two newest samples: 200 and 300.
    assert.deepEqual(bus.getStats().delayedScheduleSkew, {
      samples: 2,
      p50Ms: 200,
      p99Ms: 300,
      minMs: 200,
      maxMs: 300,
      meanMs: 250,
    });
  });

  it('a threshold breach after enabling triggers the new callback', async () => {
    const clock = controllableClock();
    const events: ScheduleSkewEvent[] = [];
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    bus.setScheduleSkew({
      alertThresholdMs: 10,
      onScheduleSkew: (event) => events.push(event),
    });
    await fanOutWithSkew(bus, clock, 1000, 5);
    assert.equal(events.length, 0); // 5 <= 10: silent
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(events.length, 1);
    assert.deepEqual(events[0], {
      scheduledFor: 3005,
      fannedOutAtMs: 3055,
      skewMs: 50,
      topic: 't',
      delayId: 'delayed-2',
    });
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 2);
  });

  it('fan-outs before the enable are never sampled back', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    // This fan-out happens while no tracker exists: unsampled.
    await fanOutWithSkew(bus, clock, 1000, 100);
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 0);
    bus.setScheduleSkew({ windowSize: 4 });
    // The new tracker starts empty — the pre-enable fan-out is not
    // in the window.
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 0);
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.deepEqual(bus.getStats().delayedScheduleSkew, {
      samples: 1,
      p50Ms: 50,
      p99Ms: 50,
      minMs: 50,
      maxMs: 50,
      meanMs: 50,
    });
  });

  it('a post-enable revision merges the live values', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    bus.setScheduleSkew({ windowSize: 4, alertThresholdMs: 1000 });
    // Only the window moves; the threshold stays live, and the old
    // side is now real.
    bus.setScheduleSkew({ windowSize: 2 });
    const audit = bus.getSubscriptionAudit({ action: 'schedule_skew_revised' });
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[1].detail, {
      windowSize: 2,
      oldWindowSize: 4,
      alertThresholdMs: 1000,
      oldAlertThresholdMs: 1000,
      hasOnScheduleSkew: false,
      oldHasOnScheduleSkew: false,
    });
    // The tracker is still live at the new window.
    await fanOutWithSkew(bus, clock, 1000, 50);
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 1);
  });

  it('a same-value revision after enabling is a no-op with no second audit', () => {
    const bus = new EventBus();
    bus.setScheduleSkew({ windowSize: 4, alertThresholdMs: 10 });
    bus.setScheduleSkew({ windowSize: 4 });
    bus.setScheduleSkew({ alertThresholdMs: 10 });
    bus.setScheduleSkew({ windowSize: 4, alertThresholdMs: 10 });
    bus.setScheduleSkew({});
    bus.setScheduleSkew(null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      1,
    );
  });

  it('there is no live clear: null after enabling leaves the tracker live', async () => {
    const clock = controllableClock();
    const events: ScheduleSkewEvent[] = [];
    const bus = new EventBus({ now: clock.now });
    bus.subscribe('t', () => {});
    bus.setScheduleSkew({
      windowSize: 4,
      alertThresholdMs: 10,
      onScheduleSkew: (event) => events.push(event),
    });
    bus.setScheduleSkew(null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      1,
    );
    await fanOutWithSkew(bus, clock, 1000, 50);
    // The tracker is still live: the fan-out sampled and alerted.
    assert.equal(bus.getStats().delayedScheduleSkew.samples, 1);
    assert.equal(events.length, 1);
    // And a later valid revision still moves the live tuning.
    bus.setScheduleSkew({ windowSize: 2 });
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      2,
    );
  });

  it('the enable is live-only: a fresh bus is unconfigured again', () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    bus.setScheduleSkew({ windowSize: 4 });
    assert.equal(
      bus.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      1,
    );
    // A restart restores the unconfigured state: a fresh bus built
    // from the same construction options holds no tracker.
    const fresh = new EventBus({ now: clock.now });
    assert.equal(fresh.getStats().delayedScheduleSkew.samples, 0);
    assert.equal(
      fresh.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );
    // The fresh bus can still be enabled the same way.
    fresh.setScheduleSkew({});
    assert.equal(
      fresh.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      1,
    );
  });

  it('a closed bus refuses the enable and the revision with RangeError', async () => {
    const unconfigured = new EventBus();
    await unconfigured.close();
    assert.throws(
      () => unconfigured.setScheduleSkew({ windowSize: 10 }),
      (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
    );
    assert.equal(
      unconfigured.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );

    const configured = new EventBus({ scheduleSkew: { windowSize: 4 } });
    await configured.close();
    assert.throws(
      () => configured.setScheduleSkew({ windowSize: 10 }),
      (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
    );
    assert.equal(
      configured.getSubscriptionAudit({ action: 'schedule_skew_revised' }).length,
      0,
    );
  });
});
