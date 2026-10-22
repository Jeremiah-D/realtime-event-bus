import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import {
  computeScalingSignal,
  resolveAutoscaleOptions,
  SCALING_SIGNAL_VALUES,
  type ResolvedAutoscaleThresholds,
  type ScalingSignalInput,
} from '../src/autoscale.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const DEFAULTS: ResolvedAutoscaleThresholds = resolveAutoscaleOptions();

test('resolveAutoscaleOptions applies defaults and rejects bad thresholds', () => {
  assert.deepEqual(resolveAutoscaleOptions(), {
    lagThresholdMs: 1000,
    rateThreshold: 1,
    idleLagMs: 10,
    idleRate: 0.1,
  });
  assert.deepEqual(resolveAutoscaleOptions({ lagThresholdMs: 500 }), {
    lagThresholdMs: 500,
    rateThreshold: 1,
    idleLagMs: 10,
    idleRate: 0.1,
  });
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => resolveAutoscaleOptions({ lagThresholdMs: bad }), RangeError);
  }
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => resolveAutoscaleOptions({ rateThreshold: bad }), RangeError);
    assert.throws(() => resolveAutoscaleOptions({ idleLagMs: bad }), RangeError);
    assert.throws(() => resolveAutoscaleOptions({ idleRate: bad }), RangeError);
  }
  // The idle band must sit strictly below the scale-up threshold.
  assert.throws(() => resolveAutoscaleOptions({ idleLagMs: 1000 }), RangeError);
  assert.throws(() => resolveAutoscaleOptions({ idleLagMs: 2000 }), RangeError);
  assert.throws(() => resolveAutoscaleOptions(42 as never), TypeError);
});

test('constructor validates autoscale thresholds fail-fast', () => {
  assert.throws(() => new EventBus({ autoscale: { lagThresholdMs: 0 } }), RangeError);
  assert.throws(() => new EventBus({ autoscale: { idleLagMs: 5000 } }), RangeError);
  assert.throws(() => new EventBus({ autoscale: { rateThreshold: -2 } }), RangeError);
  // A valid config wires through: a lag-monitored subscriber with an empty
  // queue reports a real watermark of 0 (hold), not missing data.
  const bus = new EventBus({ autoscale: { lagThresholdMs: 250 } });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  assert.equal(bus.getScalingSignal('t', sub.id).reason, 'lag-normal');
});

test('computeScalingSignal decision matrix', () => {
  const at = 9_999;
  const sig = (input: ScalingSignalInput) => computeScalingSignal(input, DEFAULTS, at);

  // Sustained overload: lag p99 above threshold with the rate holding.
  const up = sig({ lagP99Ms: 5000, watermarkMs: 5000, rate1m: 2 });
  assert.equal(up.signal, 'scale_up');
  assert.equal(up.reason, 'lag-above-threshold');
  assert.ok(up.reasonDetail.length > 0);
  assert.equal(up.lagP99Ms, 5000);
  assert.equal(up.rate1m, 2);
  assert.equal(up.at, at);

  // Lag is high but the load already fell off: hold, let it drain.
  const draining = sig({ lagP99Ms: 5000, watermarkMs: 100, rate1m: 0.05 });
  assert.equal(draining.signal, 'hold');
  assert.equal(draining.reason, 'lag-high-rate-dropping');

  // Lag above threshold but no rate history: cannot confirm sustained load.
  const noRate = sig({ lagP99Ms: 5000, watermarkMs: 5000 });
  assert.equal(noRate.signal, 'hold');
  assert.equal(noRate.reason, 'no-rate-data');

  // Lag within its bands: nothing to do.
  const normal = sig({ lagP99Ms: 200, watermarkMs: 50, rate1m: 2 });
  assert.equal(normal.signal, 'hold');
  assert.equal(normal.reason, 'lag-normal');

  // Drained and idle: excess capacity.
  const down = sig({ lagP99Ms: 5, watermarkMs: 0, rate1m: 0.01 });
  assert.equal(down.signal, 'scale_down');
  assert.equal(down.reason, 'drained-idle');

  // Drained distribution but the queue head is still old: not idle yet.
  const headGrowing = sig({ lagP99Ms: 5, watermarkMs: 4000, rate1m: 0.01 });
  assert.equal(headGrowing.signal, 'hold');
  assert.equal(headGrowing.reason, 'lag-normal');

  // No instrumentation at all: hold, and say so.
  const noData = sig({});
  assert.equal(noData.signal, 'hold');
  assert.equal(noData.reason, 'no-data');
  assert.ok(noData.reasonDetail.includes('neither lagMonitor nor deliveryLatency'));

  // A bare watermark (no distribution samples yet) still evaluates.
  const watermarkOnly = sig({ watermarkMs: 5000, rate1m: 3 });
  assert.equal(watermarkOnly.signal, 'scale_up');
  assert.equal(watermarkOnly.reason, 'lag-above-threshold');
  assert.equal(watermarkOnly.lagP99Ms, undefined);
});

test('bus: sustained lag on a hot topic signals scale_up', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('hot', (msg) => received.push(msg.payload), {
    lagMonitor: true,
    queueSize: 200,
  });

  // 120 publishes at t=1000 sit in the queue; draining them at t=61000
  // records 60s dwells (p99 = 60000 > 1000ms threshold).
  for (let i = 0; i < 120; i++) bus.publish('hot', i);
  clock.nowMs = 61_000;
  await flush();
  assert.equal(received.length, 120);

  const byBoth = bus.getScalingSignal('hot', sub.id);
  assert.equal(byBoth.signal, 'scale_up');
  assert.equal(byBoth.reason, 'lag-above-threshold');
  assert.equal(byBoth.lagP99Ms, 60_000);
  assert.equal(byBoth.rate1m, 2); // 120 publishes inside the trailing 60s
  assert.equal(byBoth.at, 61_000);

  // Topic-only scope aggregates the matching subscriber the same way.
  const byTopic = bus.getScalingSignal('hot');
  assert.equal(byTopic.signal, 'scale_up');
  assert.equal(byTopic.rate1m, 2);

  // Subscriber-only scope reads the hottest matching topic's rate.
  const bySub = bus.getScalingSignal(undefined, sub.id);
  assert.equal(bySub.signal, 'scale_up');

  // Bus-wide scope uses the worst subscriber and the hottest topic.
  const busWide = bus.getScalingSignal();
  assert.equal(busWide.signal, 'scale_up');
});

test('bus: drained and idle subscriber signals scale_down', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('cool', () => {}, { lagMonitor: true, deliveryLatency: true });

  bus.publish('cool', 'x');
  clock.nowMs += 5; // 5ms dwell: inside the 10ms idle band
  await flush();

  const signal = bus.getScalingSignal('cool', sub.id);
  assert.equal(signal.signal, 'scale_down');
  assert.equal(signal.reason, 'drained-idle');
  assert.ok(signal.rate1m !== undefined && signal.rate1m < 0.1);
});

test('bus: high lag on a cooling topic holds instead of scaling up', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('spike', () => {}, { lagMonitor: true, queueSize: 200 });

  // 10 publishes 90s ago: the rate window has fully decayed, but a
  // 60s-old drain still sits in the dwell window's p99.
  for (let i = 0; i < 10; i++) bus.publish('spike', i);
  clock.nowMs += 60_000;
  await flush(); // dwell 60000 -> p99 60000
  clock.nowMs += 90_000; // rate decays to 0, samples stay in the window

  const signal = bus.getScalingSignal('spike', sub.id);
  assert.equal(signal.signal, 'hold');
  assert.equal(signal.reason, 'lag-high-rate-dropping');
  assert.equal(signal.rate1m, 0);
});

test('bus: delivery-latency sampling feeds the signal without a lag monitor', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('warm', () => {}, { deliveryLatency: true, queueSize: 200 });

  for (let i = 0; i < 120; i++) bus.publish('warm', i);
  clock.nowMs = 61_000;
  await flush();

  const signal = bus.getScalingSignal('warm', sub.id);
  assert.equal(signal.signal, 'scale_up');
  assert.equal(signal.reason, 'lag-above-threshold');
  // No lag monitor means no live watermark — the distribution alone decides.
  assert.equal(signal.lagP99Ms, 60_000);
});

test('bus: missing instrumentation and unknown subscribers', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const plain = bus.subscribe('plain', () => {});
  bus.publish('plain', 1);
  await flush();

  const signal = bus.getScalingSignal('plain', plain.id);
  assert.equal(signal.signal, 'hold');
  assert.equal(signal.reason, 'no-data');
  assert.equal(signal.at, clock.nowMs);

  // A topic that never published has no rate history either.
  assert.equal(bus.getScalingSignal('never-published').reason, 'no-data');

  assert.throws(() => bus.getScalingSignal('plain', 'no-such-subscriber'), /unknown subscriber/);
});

test('bus: topic scope takes the worst matching subscriber', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  // The cold subscriber only accepts the 'quick' probe, so its dwell
  // window stays tiny while the hot subscriber absorbs the backlog.
  bus.subscribe('shared', () => {}, {
    lagMonitor: true,
    filter: (payload) => payload === 'quick',
  });
  const hotSub = bus.subscribe('shared', () => {}, { lagMonitor: true, queueSize: 200 });

  bus.publish('shared', 'quick');
  await flush(); // cold subscriber: tiny dwell
  for (let i = 0; i < 120; i++) bus.publish('shared', i);
  clock.nowMs = 61_000;
  await flush(); // hot subscriber: 60s dwell

  const signal = bus.getScalingSignal('shared');
  assert.equal(signal.signal, 'scale_up');
  assert.equal(signal.lagP99Ms, 60_000);
  assert.equal(hotSub.id.length > 0, true);
});

test('setAutoscaleThresholds adjusts the signal at runtime', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true, queueSize: 200 });

  for (let i = 0; i < 120; i++) bus.publish('t', i);
  clock.nowMs = 61_000;
  await flush();
  assert.equal(bus.getScalingSignal('t', sub.id).signal, 'scale_up');

  // Raising the lag threshold past the observed p99 flips the signal to
  // hold without touching the other thresholds.
  bus.setAutoscaleThresholds({ lagThresholdMs: 120_000 });
  const held = bus.getScalingSignal('t', sub.id);
  assert.equal(held.signal, 'hold');
  assert.equal(held.reason, 'lag-normal');

  // Invalid runtime values throw and leave the current thresholds alone.
  assert.throws(() => bus.setAutoscaleThresholds({ lagThresholdMs: -5 }), RangeError);
  assert.throws(() => bus.setAutoscaleThresholds({ idleLagMs: 500_000 }), RangeError);
  assert.equal(bus.getScalingSignal('t', sub.id).signal, 'hold');

  // Lowering the threshold again re-arms scale_up.
  bus.setAutoscaleThresholds({ lagThresholdMs: 1000 });
  assert.equal(bus.getScalingSignal('t', sub.id).signal, 'scale_up');
});

test('renderPrometheus exposes the autoscale signal gauge', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { lagMonitor: true });
  bus.publish('t', 1);
  await flush();

  const stats = bus.getStats();
  // One instantly-delivered message at a tiny rate: drained and idle.
  const sub = bus.getScalingSignal('t');
  assert.equal(sub.signal, 'scale_down');
  assert.equal(sub.reason, 'drained-idle');
  const text = renderPrometheus(stats, [
    { signal: 'scale_up', subscriberId: 's-1', topic: 't' },
    { signal: 'hold', topic: 't' },
    { signal: 'scale_down', subscriberId: 's-2' },
  ]);
  assert.ok(text.includes('# TYPE eventbus_autoscale_signal gauge'));
  assert.ok(text.includes('eventbus_autoscale_signal{subscriber="s-1",topic="t"} 1'));
  assert.ok(text.includes('eventbus_autoscale_signal{topic="t"} 0'));
  assert.ok(text.includes('eventbus_autoscale_signal{subscriber="s-2"} -1'));

  // No rows, no series — a bus that never asks renders what it always did.
  const plain = renderPrometheus(stats);
  assert.ok(!plain.includes('eventbus_autoscale_signal'));
});

test('SCALING_SIGNAL_VALUES is a deterministic three-way encoding', () => {
  assert.deepEqual(SCALING_SIGNAL_VALUES, { scale_up: 1, hold: 0, scale_down: -1 });
});
