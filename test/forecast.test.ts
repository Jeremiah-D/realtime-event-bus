import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import {
  LagForecastTracker,
  resolveLagForecastOptions,
} from '../src/forecast.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('tracker extrapolates a linear watermark trend to the threshold', () => {
  const tracker = new LagForecastTracker(64, 4);
  tracker.record(1000, 0);
  tracker.record(1100, 100);
  tracker.record(1200, 200);
  assert.equal(tracker.forecast(1200, 1000), undefined); // only 3 samples < minSamples 4
  tracker.record(1300, 300);
  const p = tracker.forecast(1300, 1000)!;
  // Slope 1.0 ms/ms, intercept -1000: crosses 1000 at t=2000 -> 700ms out.
  assert.equal(p.forecastMsAtThreshold, 700);
  assert.equal(p.slopeMsPerMs, 1);
  assert.ok(Math.abs(p.confidence - 1) < 1e-9); // perfect linear fit
});

test('tracker yields no forecast for flat, shrinking, or past trends', () => {
  const flat = new LagForecastTracker(64, 4);
  for (const t of [1000, 1100, 1200, 1300]) flat.record(t, 50);
  assert.equal(flat.forecast(1300, 1000), undefined); // slope 0

  const shrinking = new LagForecastTracker(64, 4);
  shrinking.record(1000, 300);
  shrinking.record(1100, 200);
  shrinking.record(1200, 100);
  shrinking.record(1300, 50);
  assert.equal(shrinking.forecast(1300, 1000), undefined); // slope < 0

  const past = new LagForecastTracker(64, 4);
  past.record(1000, 900);
  past.record(1100, 950);
  past.record(1200, 1000);
  past.record(1300, 1050);
  // Already at/above the threshold: the lag monitor owns that signal.
  assert.equal(past.forecast(1300, 1000), undefined);
});

test('tracker evicts oldest samples past the window', () => {
  const tracker = new LagForecastTracker(4, 4);
  for (let i = 0; i < 6; i++) tracker.record(1000 + i * 100, i * 100);
  assert.equal(tracker.sampleCount, 4);
  // Window holds (1200,200)..(1500,500): slope 1, crosses 1000 at t=2000.
  const p = tracker.forecast(1500, 1000)!;
  assert.equal(p.forecastMsAtThreshold, 500);
});

test('tracker validates window and minSamples', () => {
  assert.throws(() => new LagForecastTracker(0), RangeError);
  assert.throws(() => new LagForecastTracker(64, 1), RangeError);
  assert.throws(() => new LagForecastTracker(4, 8), RangeError);
});

test('resolveLagForecastOptions validates', () => {
  assert.equal(resolveLagForecastOptions(undefined), undefined);
  assert.equal(resolveLagForecastOptions(false), undefined);
  const def = resolveLagForecastOptions(true)!;
  assert.equal(def.thresholdMs, undefined);
  assert.equal(def.alerted, false);
  assert.throws(() => resolveLagForecastOptions('yes' as never), TypeError);
  assert.throws(() => resolveLagForecastOptions({ windowSize: 0 }), RangeError);
  assert.throws(() => resolveLagForecastOptions({ minSamples: 1 }), RangeError);
  assert.throws(() => resolveLagForecastOptions({ thresholdMs: -1 }), RangeError);
  assert.throws(
    () => resolveLagForecastOptions({ onLagForecast: () => {}, forecastHorizonMs: 0 }),
    RangeError,
  );
  // A callback with no threshold can never fire: fail fast.
  assert.throws(
    () => resolveLagForecastOptions({ onLagForecast: () => {} }),
    RangeError,
  );
});

test('subscribe validates lagForecast', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { lagForecast: { windowSize: 0 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { lagForecast: { onLagForecast: () => {} } }),
    RangeError,
  );
});

test('disabled by default: no forecast state', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 'x');
  assert.deepEqual(bus.getStats().lagForecast, []);
});

test('onLagForecast fires early while the watermark is still below the threshold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: Array<{ forecastMsAtThreshold: number; confidence: number; watermarkMs: number }> =
    [];
  const sub = bus.subscribe('t', () => {}, {
    // Never drain between publishes: the head's dwell grows 100ms per publish.
    lagForecast: {
      thresholdMs: 1000,
      onLagForecast: (e) =>
        events.push({
          forecastMsAtThreshold: e.forecastMsAtThreshold,
          confidence: e.confidence,
          watermarkMs: e.watermarkMs,
        }),
    },
  });
  assert.equal(typeof sub.id, 'string');

  for (let i = 0; i < 4; i++) {
    bus.publish('t', i); // queue grows; no flush, so nothing drains
    clock.nowMs += 100;
  }
  // 4 samples: (1000,0),(1100,100),(1200,200),(1300,300) — slope 1,
  // crosses 1000 at t=2000, 700ms out, within the 1000ms horizon.
  assert.equal(events.length, 1);
  assert.equal(events[0].forecastMsAtThreshold, 700);
  assert.ok(events[0].confidence > 0.99);
  assert.equal(events[0].watermarkMs, 300); // still well below the 1000 threshold

  // Fires once per excursion: more growth does not re-fire.
  bus.publish('t', 4);
  clock.nowMs += 100;
  bus.publish('t', 5);
  assert.equal(events.length, 1);

  // The trend is observable via getStats.
  const row = bus.getStats().lagForecast.find((r) => r.pattern === 't')!;
  assert.equal(row.samples, 6);
  assert.equal(row.watermarkMs, 500); // head enqueued at t=1000, now t=1500
  assert.ok(row.slopeMsPerMs !== undefined && row.slopeMsPerMs > 0);
  assert.ok(row.forecastMsAtThreshold !== undefined && row.forecastMsAtThreshold <= 1000);
  assert.equal(row.alerted, true);

  // Drain: the watermark collapses, the prediction clears, the latch re-arms.
  await flush();
  assert.equal(bus.getStats().lagForecast.find((r) => r.pattern === 't')!.alerted, false);

  // ...and a new growth excursion fires again.
  for (let i = 0; i < 4; i++) {
    bus.publish('t', i);
    clock.nowMs += 100;
  }
  assert.equal(events.length, 2);
  await flush();
});

test('no forecast when the consumer keeps up', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let fired = 0;
  bus.subscribe('t', () => {}, {
    lagForecast: {
      thresholdMs: 1000,
      onLagForecast: () => {
        fired += 1;
      },
    },
  });
  for (let i = 0; i < 6; i++) {
    bus.publish('t', i);
    clock.nowMs += 100;
    await flush(); // drains every time: watermark never grows
  }
  assert.equal(fired, 0);
  const row = bus.getStats().lagForecast.find((r) => r.pattern === 't')!;
  // Every flush drained the queue, resetting the sample window: no
  // backlog episode ever accumulated a trend.
  assert.equal(row.samples, 0);
  assert.equal(row.forecastMsAtThreshold, undefined);
  assert.equal(row.alerted, false);
});

test('forecast samples without a threshold but never alerts', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { lagForecast: true });
  for (let i = 0; i < 5; i++) {
    bus.publish('t', i);
    clock.nowMs += 100;
  }
  const row = bus.getStats().lagForecast.find((r) => r.pattern === 't')!;
  assert.equal(row.samples, 5);
  assert.equal(row.alerted, false);
});

test('forecast is independent of the lag monitor', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  // lagForecast WITHOUT lagMonitor: own stamp map, own watermark.
  const forecasts: number[] = [];
  bus.subscribe('t', () => {}, {
    lagForecast: {
      thresholdMs: 500,
      onLagForecast: (e) => forecasts.push(e.forecastMsAtThreshold),
    },
  });
  for (let i = 0; i < 4; i++) {
    bus.publish('t', i);
    clock.nowMs += 100;
  }
  assert.equal(forecasts.length, 1);
  assert.deepEqual(bus.getStats().lag, []); // no lag monitor was armed
});
