import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type LagForecastEvent,
  type LagForecastOptions,
} from '../src/bus.ts';

/**
 * Bus-level default lag forecast (EB-495):
 * `setDefaultLagForecast` installs the `windowSize` / `minSamples` /
 * `thresholdMs` / `forecastHorizonMs` / `onLagForecast` new opt-in
 * `lagForecast` subscribers fall back to when they omit a field — the
 * priority chain is subscriber-explicit > bus default > built-in
 * (windowSize 64, minSamples 4, horizon defaults to `thresholdMs`) —
 * and the default never enables forecasting: a subscriber that does
 * not opt into `lagForecast` is untouched by it.
 *
 * Window / minimum-sample assertions read `getStats().lagForecast`
 * (following `forecast.test.ts`): an injectable clock makes queue
 * dwells deterministic, the `samples` count exposes the window bound
 * — publish more messages than the window and the count caps at it —
 * and `slopeMsPerMs` appears only once the minimum-sample bar is met
 * (a threshold must be set for a prediction — and hence a slope — to
 * exist at all).
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 1_000) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

function forecastEntry(bus: EventBus, pattern: string) {
  const entry = bus.getStats().lagForecast.find((e) => e.pattern === pattern);
  assert.ok(entry !== undefined, `expected a getStats().lagForecast entry for ${pattern}`);
  return entry;
}

/** Publishes `n` messages without draining, advancing the clock between them. */
function publishGrowing(
  bus: EventBus,
  clock: ReturnType<typeof makeClock>,
  pattern: string,
  n: number,
) {
  for (let i = 0; i < n; i++) {
    bus.publish(pattern, i);
    clock.advance(100);
  }
}

test('EB-495: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultLagForecast(), undefined);
  bus.setDefaultLagForecast(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lag_forecast_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-495: lagForecast: true takes the whole bus default window and minimum-sample bar', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLagForecast({ windowSize: 5, minSamples: 3, thresholdMs: 1000 });
  const sub = bus.subscribe('t', () => {}, { lagForecast: true });
  publishGrowing(bus, clock, 't', 2);
  assert.equal(
    forecastEntry(bus, 't').slopeMsPerMs,
    undefined,
    '2 samples are below the default minSamples of 3: no fit yet',
  );
  publishGrowing(bus, clock, 't', 1);
  const fitted = forecastEntry(bus, 't');
  assert.ok(
    fitted.slopeMsPerMs !== undefined && fitted.slopeMsPerMs > 0,
    'the 3rd sample meets the default minSamples: the fit appears',
  );
  publishGrowing(bus, clock, 't', 4);
  assert.equal(
    forecastEntry(bus, 't').samples,
    5,
    'the window comes from the bus default, not the built-in 64',
  );
  sub.unsubscribe();
  await flush();
});

test('EB-495: {} opts in and takes the whole default (window, threshold, callback)', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagForecastEvent[] = [];
  bus.setDefaultLagForecast({
    windowSize: 5,
    minSamples: 3,
    thresholdMs: 1000,
    onLagForecast: (e) => events.push(e),
  });
  const sub = bus.subscribe('t', () => {}, { lagForecast: {} });
  publishGrowing(bus, clock, 't', 4);
  // 3 samples (1000,0),(1100,100),(1200,200) — slope 1, crosses 1000
  // at t=2000, 800ms out, within the 1000ms horizon: fires from the
  // default while the watermark (200) is still well below the threshold.
  assert.equal(events.length, 1);
  assert.equal(events[0].forecastMsAtThreshold, 800);
  assert.equal(events[0].thresholdMs, 1000);
  assert.equal(events[0].watermarkMs, 200);
  assert.ok(events[0].confidence > 0.99);
  // Fires once per excursion: more growth does not re-fire.
  publishGrowing(bus, clock, 't', 2);
  assert.equal(events.length, 1);
  assert.equal(forecastEntry(bus, 't').samples, 5);
  assert.equal(forecastEntry(bus, 't').alerted, true);
  // Drain: the watermark collapses, the prediction clears, the latch re-arms.
  await flush();
  assert.equal(forecastEntry(bus, 't').alerted, false);
  sub.unsubscribe();
  await flush();
});

test('EB-495: an explicit field wins, field by field, over the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagForecastEvent[] = [];
  bus.setDefaultLagForecast({
    windowSize: 5,
    minSamples: 3,
    thresholdMs: 1000,
    onLagForecast: (e) => events.push(e),
  });
  // Explicit windowSize 7 beats the default 5; the default threshold
  // and callback still apply (no explicit ones given).
  const sub = bus.subscribe('t', () => {}, { lagForecast: { windowSize: 7 } });
  publishGrowing(bus, clock, 't', 9);
  assert.equal(forecastEntry(bus, 't').samples, 7, 'the explicit window wins');
  assert.equal(events.length, 1, 'the default threshold and callback still apply');
  assert.equal(events[0].thresholdMs, 1000);
  sub.unsubscribe();
  await flush();
});

test('EB-495: with no default, true still takes the built-ins 64/4', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagForecastEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    lagForecast: {
      thresholdMs: 1000,
      onLagForecast: (e) => events.push(e),
    },
  });
  publishGrowing(bus, clock, 't', 3);
  assert.equal(events.length, 0, '3 samples are below the built-in minSamples of 4');
  publishGrowing(bus, clock, 't', 1);
  // 4 samples: (1000,0)..(1300,300) — slope 1, crosses 1000 at
  // t=2000, 700ms out, within the 1000ms horizon.
  assert.equal(events.length, 1);
  assert.equal(events[0].forecastMsAtThreshold, 700);
  assert.equal(events[0].watermarkMs, 300);
  // The built-in window is 64: 70 undrained publishes cap at 64.
  publishGrowing(bus, clock, 't', 66);
  assert.equal(forecastEntry(bus, 't').samples, 64);
  assert.equal(events.length, 1, 'the excursion latch fires once');
  sub.unsubscribe();
  await flush();
});

test('EB-495: clearing to null restores the built-ins for new subscribers', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLagForecast({ windowSize: 5, minSamples: 3, thresholdMs: 1000 });
  bus.setDefaultLagForecast(null);
  assert.equal(bus.getDefaultLagForecast(), undefined);
  const audit = bus.getSubscriptionAudit({ action: 'default_lag_forecast_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { windowSize: 5, minSamples: 3, thresholdMs: 1000 },
    lagForecast: null,
  });
  const sub = bus.subscribe('t', () => {}, { lagForecast: true });
  publishGrowing(bus, clock, 't', 7);
  assert.equal(
    forecastEntry(bus, 't').samples,
    7,
    'no default caps the window: the built-in 64 applies again',
  );
  sub.unsubscribe();
  await flush();
});

test('EB-495: omitted/false lagForecast is untouched by the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLagForecast({ windowSize: 5, minSamples: 3, thresholdMs: 1000 });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {}, { lagForecast: false });
  bus.publish('a', 'x');
  bus.publish('b', 'x');
  assert.deepEqual(
    bus.getStats().lagForecast,
    [],
    'the default never opts a subscriber into forecasting',
  );
  await flush();
});

test('EB-495: audit shape, defensive copies, and same-value no-op', () => {
  const bus = new EventBus();
  bus.setDefaultLagForecast({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lag_forecast_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultLagForecast(), undefined);
  const onLagForecast = () => {};
  const opts: LagForecastOptions = {
    windowSize: 5,
    minSamples: 3,
    thresholdMs: 1000,
    onLagForecast,
  };
  bus.setDefaultLagForecast(opts);
  opts.windowSize = 999;
  opts.thresholdMs = 999;
  assert.deepEqual(
    bus.getDefaultLagForecast(),
    { windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast },
    'the setter stores a defensive copy',
  );
  bus.setDefaultLagForecast({ windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast });
  bus.setDefaultLagForecast({ windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast });
  const audit = bus.getSubscriptionAudit({ action: 'default_lag_forecast_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    lagForecast: { windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast: 'set' },
  });
  assert.equal(audit[0].actor, 'system');
  const held = bus.getDefaultLagForecast();
  assert.ok(held !== undefined);
  held.windowSize = 999;
  assert.deepEqual(
    bus.getDefaultLagForecast(),
    { windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast },
    'the getter returns a copy',
  );
  bus.setDefaultLagForecast({ windowSize: 8, minSamples: 4 }, 'ops');
  assert.deepEqual(bus.getDefaultLagForecast(), { windowSize: 8, minSamples: 4 });
  const revised = bus.getSubscriptionAudit({ action: 'default_lag_forecast_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast: 'set' },
    lagForecast: { windowSize: 8, minSamples: 4 },
  });
  assert.equal(revised[1].actor, 'ops');
});

test('EB-495: invalid values throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  const onLagForecast = () => {};
  bus.setDefaultLagForecast({ windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast });
  const bad = [
    0,
    -1,
    '5',
    [],
    true,
    { windowSize: 0 },
    { windowSize: -1 },
    { windowSize: 1.5 },
    { windowSize: Number.NaN },
    { windowSize: '5' },
    { windowSize: 3 },
    { windowSize: 3, minSamples: 4 },
    { minSamples: 1 },
    { minSamples: 2.5 },
    { minSamples: 6, windowSize: 5 },
    { thresholdMs: 0 },
    { thresholdMs: -1 },
    { thresholdMs: Number.NaN },
    { thresholdMs: Number.POSITIVE_INFINITY },
    { forecastHorizonMs: 0 },
    { forecastHorizonMs: -5 },
    { forecastHorizonMs: Number.POSITIVE_INFINITY },
    { onLagForecast: () => {} },
    { thresholdMs: 100, onLagForecast: 'not-a-function' },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultLagForecast(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultLagForecast(), {
      windowSize: 5,
      minSamples: 3,
      thresholdMs: 1000,
      onLagForecast,
    });
  }
  assert.throws(() => bus.setDefaultLagForecast({ windowSize: 7 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultLagForecast(), {
    windowSize: 5,
    minSamples: 3,
    thresholdMs: 1000,
    onLagForecast,
  });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lag_forecast_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-495: revising the bus default never moves an already-subscribed window, threshold, or samples', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLagForecast({ windowSize: 5, minSamples: 3, thresholdMs: 10000 });
  const sub = bus.subscribe('t', () => {}, { lagForecast: true });
  publishGrowing(bus, clock, 't', 7);
  assert.equal(forecastEntry(bus, 't').samples, 5);
  bus.setDefaultLagForecast({ windowSize: 2, minSamples: 2 });
  publishGrowing(bus, clock, 't', 5);
  assert.equal(
    forecastEntry(bus, 't').samples,
    5,
    'the early subscriber keeps the window it was baked with',
  );
  assert.ok(
    forecastEntry(bus, 't').forecastMsAtThreshold !== undefined,
    'the early subscriber keeps its baked threshold of 10000 (the watermark is still below it)',
  );
  // A subscriber joining after the revision takes the new default.
  const late = bus.subscribe('u', () => {}, { lagForecast: true });
  publishGrowing(bus, clock, 'u', 5);
  assert.equal(forecastEntry(bus, 'u').samples, 2, 'the new default window is 2');
  sub.unsubscribe();
  late.unsubscribe();
  await flush();
});

test('EB-495: setLagForecast(null) clears a subscriber without the default re-applying', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const onLagForecast = () => {};
  bus.setDefaultLagForecast({ windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast });
  const sub = bus.subscribe('t', () => {}, { lagForecast: true });
  publishGrowing(bus, clock, 't', 5);
  assert.equal(forecastEntry(bus, 't').samples, 5);
  bus.setLagForecast(sub.id, null);
  assert.deepEqual(
    bus.getStats().lagForecast,
    [],
    'the cleared subscriber leaves getStats().lagForecast',
  );
  assert.throws(() => bus.setLagForecast(sub.id, { windowSize: 2 }), RangeError);
  assert.deepEqual(
    bus.getDefaultLagForecast(),
    { windowSize: 5, minSamples: 3, thresholdMs: 1000, onLagForecast },
    'clearing a subscriber never moves the bus default',
  );
  // A new subscriber still takes the untouched bus default.
  const fresh = bus.subscribe('u', () => {}, { lagForecast: true });
  publishGrowing(bus, clock, 'u', 5);
  assert.equal(forecastEntry(bus, 'u').samples, 5);
  sub.unsubscribe();
  fresh.unsubscribe();
  await flush();
});
