import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { LagForecastTracker } from '../src/forecast.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Reads the one lag-forecast stats row for a pattern. */
function forecastRow(bus: EventBus, pattern: string) {
  const rows = bus.getStats().lagForecast.filter((r) => r.pattern === pattern);
  assert.equal(rows.length, 1, `expected one lagForecast row for ${pattern}`);
  return rows[0];
}

/** Publishes `count` messages without flushing, advancing the clock between them. */
function growBacklog(
  bus: EventBus,
  topic: string,
  clock: { nowMs: number },
  count: number,
  stepMs = 100,
): void {
  for (let i = 0; i < count; i += 1) {
    bus.publish(topic, i); // queue grows; no flush, so nothing drains
    clock.nowMs += stepMs;
  }
}

test('LagForecastTracker.resize evicts oldest-first, keeps the array, and retunes minSamples', () => {
  const tracker = new LagForecastTracker(4, 2);
  tracker.record(1000, 0);
  tracker.record(1100, 0);
  tracker.record(1200, 0);
  tracker.record(1300, 300);
  assert.equal(tracker.sampleCount, 4);
  const samplesRef = (tracker as unknown as { samples: unknown[] }).samples;

  tracker.resize(2);
  assert.equal(tracker.windowSizeLimit(), 2);
  assert.equal(tracker.sampleCount, 2);
  assert.equal(
    (tracker as unknown as { samples: unknown[] }).samples,
    samplesRef,
    'resize splices in place, it never replaces the sample array',
  );
  // Surviving samples are (1200,0),(1300,300): slope 3, crosses 600 at
  // t=1400, 100ms out. Evicting the newest instead would leave a flat
  // trend and no forecast at all.
  const p = tracker.forecast(1300, 600)!;
  assert.equal(p.forecastMsAtThreshold, 100);
  assert.equal(p.slopeMsPerMs, 3);

  // Widening evicts nothing and restores nothing.
  tracker.resize(6);
  assert.equal(tracker.windowSizeLimit(), 6);
  assert.equal(tracker.sampleCount, 2);
  assert.equal(tracker.forecast(1300, 600)!.forecastMsAtThreshold, 100);

  // Raising minSamples alone changes no sample, only the fit bar.
  tracker.resize(6, 4);
  assert.equal(tracker.minSamplesLimit(), 4);
  assert.equal(tracker.sampleCount, 2);
  assert.equal(tracker.forecast(1300, 600), undefined);
  tracker.record(1400, 600);
  tracker.record(1500, 900);
  assert.equal(tracker.forecast(1500, 1800)!.forecastMsAtThreshold, 300);

  for (const bad of [
    () => tracker.resize(0),
    () => tracker.resize(-1),
    () => tracker.resize(2.5),
    () => tracker.resize(Number.NaN),
    () => tracker.resize(6, 1),
    () => tracker.resize(2, 3), // minSamples above the window
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(tracker.windowSizeLimit(), 6, 'a failed resize leaves the bounds untouched');
  assert.equal(tracker.minSamplesLimit(), 4);
});

test('setLagForecast shrinking the window evicts only the oldest samples; widening restores nothing', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: { windowSize: 4, minSamples: 2, thresholdMs: 1000 },
  });
  growBacklog(bus, 't', clock, 4);
  assert.equal(forecastRow(bus, 't').samples, 4);

  bus.setLagForecast(sub.id, { windowSize: 2 });
  assert.equal(forecastRow(bus, 't').samples, 2);
  const audit = bus.getSubscriptionAudit({ action: 'lag_forecast_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    lagForecast: { windowSize: 2, minSamples: 2, thresholdMs: 1000, forecastHorizonMs: 1000 },
    oldLagForecast: { windowSize: 4, minSamples: 2, thresholdMs: 1000, forecastHorizonMs: 1000 },
  });

  bus.setLagForecast(sub.id, { windowSize: 6 });
  assert.equal(forecastRow(bus, 't').samples, 2, 'widening keeps the shrunk window as-is');
  growBacklog(bus, 't', clock, 2);
  assert.equal(forecastRow(bus, 't').samples, 4, 'the new bound governs the next samples');
  await flush();
  sub.unsubscribe();
});

test('setLagForecast partial revision of thresholdMs takes effect on the next accepted enqueue', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: Array<{ thresholdMs: number; forecastMsAtThreshold: number }> = [];
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: {
      windowSize: 64,
      minSamples: 2,
      thresholdMs: 100000,
      forecastHorizonMs: 1000,
      onLagForecast: (e) =>
        events.push({ thresholdMs: e.thresholdMs, forecastMsAtThreshold: e.forecastMsAtThreshold }),
    },
  });
  growBacklog(bus, 't', clock, 4);
  assert.equal(events.length, 0, 'a crossing ~100s out is outside the 1000ms horizon');

  // Tighten only the threshold; the horizon stays 1000ms.
  bus.setLagForecast(sub.id, { thresholdMs: 1000 });
  assert.equal(events.length, 0, 'a revision alone evaluates nothing');
  bus.publish('t', 4);
  clock.nowMs += 100;
  assert.equal(events.length, 1, 'the next accepted enqueue evaluates the new threshold');
  assert.equal(events[0].thresholdMs, 1000);
  assert.ok(events[0].forecastMsAtThreshold <= 1000);
  sub.unsubscribe();
});

test('setLagForecast partial revision of forecastHorizonMs re-arms the firing window', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let fired = 0;
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: {
      windowSize: 64,
      minSamples: 2,
      thresholdMs: 1000,
      forecastHorizonMs: 100,
      onLagForecast: () => {
        fired += 1;
      },
    },
  });
  growBacklog(bus, 't', clock, 4);
  assert.equal(fired, 0, 'a ~700ms-out crossing is outside the 100ms horizon');

  bus.setLagForecast(sub.id, { forecastHorizonMs: 1000 });
  bus.publish('t', 4);
  clock.nowMs += 100;
  assert.equal(fired, 1, 'the widened horizon admits the same crossing on the next enqueue');
  sub.unsubscribe();
});

test('setLagForecast partial revision of minSamples moves the fit bar both ways', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let fired = 0;
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: {
      windowSize: 64,
      minSamples: 4,
      thresholdMs: 1000,
      onLagForecast: () => {
        fired += 1;
      },
    },
  });
  growBacklog(bus, 't', clock, 2);
  assert.equal(fired, 0, '2 samples are below the 4-sample bar');

  bus.setLagForecast(sub.id, { minSamples: 2 });
  bus.publish('t', 2);
  clock.nowMs += 100;
  assert.equal(fired, 1, 'lowering the bar lets the same trend forecast on the next enqueue');

  // Raising the bar hides the forecast again without touching the samples.
  const plain = bus.subscribe('u', () => {}, {
    lagForecast: { windowSize: 64, minSamples: 2, thresholdMs: 1000 },
  });
  growBacklog(bus, 'u', clock, 2);
  assert.notEqual(forecastRow(bus, 'u').forecastMsAtThreshold, undefined);
  bus.setLagForecast(plain.id, { minSamples: 4 });
  assert.equal(forecastRow(bus, 'u').samples, 2, 'raising the bar evicts nothing');
  assert.equal(forecastRow(bus, 'u').forecastMsAtThreshold, undefined);
  assert.equal(forecastRow(bus, 'u').confidence, 0);
  await flush();
  sub.unsubscribe();
  plain.unsubscribe();
});

test('setLagForecast partial revision of onLagForecast swaps the callback', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let firedA = 0;
  let firedB = 0;
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: {
      windowSize: 64,
      minSamples: 2,
      thresholdMs: 1000,
      onLagForecast: () => {
        firedA += 1;
      },
    },
  });
  growBacklog(bus, 't', clock, 2);
  assert.equal(firedA, 1);
  await flush(); // drains: latch re-arms, sample window resets

  bus.setLagForecast(sub.id, {
    onLagForecast: () => {
      firedB += 1;
    },
  });
  growBacklog(bus, 't', clock, 2);
  assert.equal(firedA, 1, 'the old callback never fires again');
  assert.equal(firedB, 1, 'the new callback fires for the next excursion');
  const audit = bus.getSubscriptionAudit({ action: 'lag_forecast_revised' });
  assert.equal(audit.length, 1);
  assert.equal(
    (audit[0].detail as { lagForecast: { onLagForecast?: string } }).lagForecast.onLagForecast,
    'set',
    'the callback is recorded by presence only',
  );
  await flush();
  sub.unsubscribe();
});

test('setLagForecast preserves the enqueue stamps and the alerted latch', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let fired = 0;
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: {
      windowSize: 64,
      minSamples: 2,
      thresholdMs: 1000,
      onLagForecast: () => {
        fired += 1;
      },
    },
  });
  growBacklog(bus, 't', clock, 2);
  assert.equal(fired, 1);
  assert.equal(forecastRow(bus, 't').alerted, true);

  // Widening the window while alerted must not re-fire: the latch holds.
  bus.setLagForecast(sub.id, { windowSize: 128 });
  growBacklog(bus, 't', clock, 2);
  assert.equal(fired, 1, 'the revision neither re-fires nor clears the armed latch');
  assert.equal(forecastRow(bus, 't').alerted, true);

  // Drain clears the latch self-consistently, and the next excursion fires again.
  await flush();
  assert.equal(forecastRow(bus, 't').alerted, false);
  growBacklog(bus, 't', clock, 2);
  assert.equal(fired, 2);

  // The enqueue stamps survive a revision: the head message still
  // measures from its original enqueue, not from the revision.
  const stamped = bus.subscribe('u', () => {}, { lagForecast: { windowSize: 64 } });

  bus.publish('u', 'a'); // enqueued at the current clock
  const enqueuedAt = clock.nowMs;
  bus.setLagForecast(stamped.id, { windowSize: 32 });
  clock.nowMs = enqueuedAt + 100;
  bus.publish('u', 'b');
  assert.equal(
    forecastRow(bus, 'u').watermarkMs,
    100,
    'the head dwell counts from the original enqueue',
  );
  await flush();
  sub.unsubscribe();
  stamped.unsubscribe();
});

test('setLagForecast identical revision is a no-op with no audit and no eviction', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const onLagForecast = () => {};
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: { windowSize: 4, minSamples: 2, thresholdMs: 1000, onLagForecast },
  });
  growBacklog(bus, 't', clock, 4);
  assert.equal(forecastRow(bus, 't').samples, 4);

  bus.setLagForecast(sub.id, { windowSize: 4 });
  bus.setLagForecast(sub.id, {});
  bus.setLagForecast(sub.id, {
    windowSize: 4,
    minSamples: 2,
    thresholdMs: 1000,
    forecastHorizonMs: 1000,
    onLagForecast,
  });
  assert.equal(forecastRow(bus, 't').samples, 4, 'restating the window evicted nothing');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'lag_forecast_revised' }).length,
    0,
    'identical revisions (including an empty object) audit nothing',
  );
  await flush();
  sub.unsubscribe();
});

test('setLagForecast null clears forecasting, and revision after a clear is refused', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: {
      windowSize: 4,
      minSamples: 2,
      thresholdMs: 1000,
      forecastHorizonMs: 500,
      onLagForecast: () => {},
    },
  });
  bus.publish('t', 'a');
  assert.equal(forecastRow(bus, 't').samples, 1);

  bus.setLagForecast(sub.id, null);
  const audit = bus.getSubscriptionAudit({ action: 'lag_forecast_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    lagForecast: null,
    oldLagForecast: {
      windowSize: 4,
      minSamples: 2,
      thresholdMs: 1000,
      forecastHorizonMs: 500,
      onLagForecast: 'set',
    },
  });

  // No row for the cleared subscriber, and later deliveries forecast nothing.
  assert.equal(bus.getStats().lagForecast.filter((r) => r.pattern === 't').length, 0);
  bus.publish('t', 'b');
  assert.equal(bus.getStats().lagForecast.filter((r) => r.pattern === 't').length, 0);

  assert.throws(() => bus.setLagForecast(sub.id, { windowSize: 8 }), RangeError);
  assert.throws(() => bus.setLagForecast(sub.id, null), RangeError);
  await flush();
  sub.unsubscribe();
});

test('setLagForecast is fail-closed for unknown, unforecast, or illegal values', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sampled = bus.subscribe('t', () => {}, {
    lagForecast: {
      windowSize: 4,
      minSamples: 2,
      thresholdMs: 1000,
      forecastHorizonMs: 500,
      onLagForecast: () => {},
    },
  });
  const plain = bus.subscribe('u', () => {});
  const thresholdless = bus.subscribe('v', () => {}, { lagForecast: true });
  growBacklog(bus, 't', clock, 2);

  const bad: Array<() => void> = [
    () => bus.setLagForecast('', { windowSize: 2 }),
    () => bus.setLagForecast('sub-nope', { windowSize: 2 }),
    () => bus.setLagForecast(plain.id, { windowSize: 2 }),
    () => bus.setLagForecast(sampled.id, undefined as never),
    () => bus.setLagForecast(sampled.id, 'x' as never),
    () => bus.setLagForecast(sampled.id, [] as never),
    () => bus.setLagForecast(sampled.id, { windowSize: 0 }),
    () => bus.setLagForecast(sampled.id, { windowSize: -1 }),
    () => bus.setLagForecast(sampled.id, { windowSize: 2.5 }),
    () => bus.setLagForecast(sampled.id, { windowSize: Number.NaN }),
    () => bus.setLagForecast(sampled.id, { windowSize: 1 }), // below the live minSamples 2
    () => bus.setLagForecast(sampled.id, { minSamples: 1 }),
    () => bus.setLagForecast(sampled.id, { minSamples: 5 }), // above the live window 4
    () => bus.setLagForecast(sampled.id, { thresholdMs: 0 }),
    () => bus.setLagForecast(sampled.id, { thresholdMs: -1 }),
    () => bus.setLagForecast(sampled.id, { thresholdMs: Number.NaN }),
    () => bus.setLagForecast(sampled.id, { thresholdMs: Number.POSITIVE_INFINITY }),
    () => bus.setLagForecast(sampled.id, { forecastHorizonMs: 0 }),
    () => bus.setLagForecast(sampled.id, { forecastHorizonMs: Number.NaN }),
    () => bus.setLagForecast(sampled.id, { onLagForecast: 'x' as never }),
    // A callback with no threshold to arm it — including revising
    // onLagForecast alone on a threshold-less subscription.
    () => bus.setLagForecast(thresholdless.id, { onLagForecast: () => {} }),
  ];
  for (const call of bad) assert.throws(call, RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'lag_forecast_revised' }).length,
    0,
    'no failed revision is audited',
  );

  // The live parameters and samples survived every failed revision.
  assert.equal(forecastRow(bus, 't').samples, 2);
  bus.setLagForecast(sampled.id, { windowSize: 8 });
  const audit = bus.getSubscriptionAudit({ action: 'lag_forecast_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail.oldLagForecast, {
    windowSize: 4,
    minSamples: 2,
    thresholdMs: 1000,
    forecastHorizonMs: 500,
    onLagForecast: 'set',
  });
  await flush();
  sampled.unsubscribe();
  plain.unsubscribe();
  thresholdless.unsubscribe();
});
