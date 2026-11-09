import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import {
  FailureRateRegimeDetector,
  FailureRateRegimeMonitor,
  type DeliveryOutcome,
  type FailureRateRegimeChange,
} from '../src/failure-regime.ts';

// Deterministic clock: tests set `now` explicitly.
let now = 5_000_000;
const nowMs = () => now;

/**
 * Deterministic outcome stream: exactly `badCount` failures evenly
 * spread over `total` observations (a failure every `total/badCount`
 * samples, first failure at the end of the first interval). Successes
 * fill the rest. Timeouts are expressed by the caller mapping the
 * bad slots to 'timeout' instead of 'failure'.
 */
function spread(total: number, badCount: number, bad: DeliveryOutcome = 'failure'): DeliveryOutcome[] {
  const out: DeliveryOutcome[] = Array.from({ length: total }, () => 'success');
  if (badCount <= 0) return out;
  const step = total / badCount;
  for (let i = 0; i < badCount; i++) {
    out[Math.min(total - 1, Math.floor((i + 1) * step) - 1)] = bad;
  }
  return out;
}

function feed(
  d: FailureRateRegimeDetector,
  outcomes: DeliveryOutcome[],
): FailureRateRegimeChange[] {
  const events: FailureRateRegimeChange[] = [];
  for (const o of outcomes) {
    const e = d.observe(o);
    if (e !== undefined) events.push(e);
  }
  return events;
}

/** Max windowed failure rate (window 50) over a stream — the fixed 10% line. */
function maxWindowedRate(outcomes: DeliveryOutcome[], window = 50): number {
  let max = 0;
  for (let i = 0; i < outcomes.length; i++) {
    const slice = outcomes.slice(Math.max(0, i - window + 1), i + 1);
    const bad = slice.filter((o) => o !== 'success').length;
    max = Math.max(max, bad / slice.length);
  }
  return max;
}

describe('FailureRateRegimeDetector (EB-442)', () => {
  it('catches a slow 1% -> 8% creep below a fixed 10% line', () => {
    now = 5_000_000;
    const d = new FailureRateRegimeDetector('orders', { nowMs });
    const stream = [
      ...spread(100, 1), // 1% baseline
      ...spread(100, 3), // 3% — still below the detectable shift
      ...spread(100, 5), // 5% — the creep gathers pace
      ...spread(300, 24), // 8% — the new regime
    ];
    const events = feed(d, stream);
    assert.equal(events.length, 1);
    const e = events[0];
    assert.equal(e.topic, 'orders');
    assert.equal(e.direction, 'increase');
    assert.equal(e.detectedAtMs, 5_000_000);
    assert.ok(e.before.failureRate < e.after.failureRate, 'the regime did rise');
    assert.ok(e.before.failureRate <= 0.08, `before rate ${e.before.failureRate} should still be the old regime`);
    assert.ok(
      e.after.failureRate >= 0.05 && e.after.failureRate <= 0.12,
      `after rate ${e.after.failureRate} should be the new regime`,
    );
    assert.ok(e.before.samples >= 1 && e.after.samples >= 1);
    // The fixed 10% line never fires on this stream — the creep stays under it.
    assert.ok(maxWindowedRate(stream) < 0.1, `max windowed rate ${maxWindowedRate(stream)}`);
  });

  it('alarms immediately on a sudden jump', () => {
    now = 5_000_000;
    const d = new FailureRateRegimeDetector('orders', { nowMs });
    assert.equal(feed(d, spread(100, 1)).length, 0);
    const events = feed(d, spread(30, 15));
    assert.equal(events.length, 1);
    assert.equal(events[0].direction, 'increase');
    assert.ok(events[0].before.failureRate <= 0.05);
    assert.ok(events[0].after.failureRate >= 0.4);
  });

  it('alarms on an improving regime too', () => {
    now = 5_000_000;
    const d = new FailureRateRegimeDetector('orders', { nowMs });
    assert.equal(feed(d, spread(100, 20)).length, 0);
    const events = feed(d, spread(100, 2));
    assert.equal(events.length, 1);
    assert.equal(events[0].direction, 'decrease');
    assert.ok(events[0].before.failureRate >= 0.15);
    assert.ok(events[0].after.failureRate <= 0.05);
  });

  it('a single warmup outlier does not poison the baseline or alarm', () => {
    now = 5_000_000;
    const d = new FailureRateRegimeDetector('orders', { nowMs });
    // Warmup (first 50 observations) carries exactly one failure:
    // the mean baseline moves by at most 1/50, which the threshold
    // absorbs.
    const warmup = spread(50, 1);
    assert.equal(feed(d, warmup).length, 0);
    // Steady 1% afterwards: no alarm — the baseline stayed near 0.
    assert.equal(feed(d, spread(200, 2)).length, 0);
  });

  it('re-baselines after an alarm and does not repeat-alarm on the new regime', () => {
    now = 5_000_000;
    const d = new FailureRateRegimeDetector('orders', { nowMs });
    assert.equal(feed(d, spread(100, 1)).length, 0);
    assert.equal(feed(d, spread(30, 15)).length, 1);
    // The new 50% regime is the normal now: 300 more samples, no second alarm.
    assert.equal(feed(d, spread(300, 150)).length, 0);
  });

  it('counts timeouts as failures', () => {
    now = 5_000_000;
    const d = new FailureRateRegimeDetector('orders', { nowMs });
    assert.equal(feed(d, spread(100, 1)).length, 0);
    const events = feed(d, spread(300, 24, 'timeout'));
    assert.equal(events.length, 1);
    assert.equal(events[0].direction, 'increase');
    assert.ok(events[0].after.failureRate >= 0.05);
  });

  it('fires the callback with the same event and honors the injected clock', () => {
    const seen: FailureRateRegimeChange[] = [];
    now = 5_000_000;
    const d = new FailureRateRegimeDetector('orders', {
      nowMs,
      onFailureRateRegimeChange: (e) => seen.push(e),
    });
    assert.equal(feed(d, spread(100, 1)).length, 0);
    const events = feed(d, spread(30, 15));
    assert.equal(events.length, 1);
    assert.equal(seen.length, 1);
    assert.equal(seen[0], events[0]);
    assert.equal(events[0].detectedAtMs, 5_000_000);
    // A second shift later is stamped with the advanced clock.
    now = 6_000_000;
    const second = feed(d, spread(100, 5));
    assert.equal(second.length, 1);
    assert.equal(second[0].direction, 'decrease');
    assert.equal(second[0].detectedAtMs, 6_000_000);
  });

  it('validates its options and outcomes fail-fast', () => {
    assert.throws(() => new FailureRateRegimeDetector('', {}), RangeError);
    assert.throws(() => new FailureRateRegimeDetector('t', { delta: 0 }), RangeError);
    assert.throws(() => new FailureRateRegimeDetector('t', { delta: 1 }), RangeError);
    assert.throws(() => new FailureRateRegimeDetector('t', { delta: Number.NaN }), RangeError);
    assert.throws(() => new FailureRateRegimeDetector('t', { lambda: 0 }), RangeError);
    assert.throws(() => new FailureRateRegimeDetector('t', { warmupSamples: 0 }), RangeError);
    assert.throws(() => new FailureRateRegimeDetector('t', { warmupSamples: 1.5 }), RangeError);
    assert.throws(() => new FailureRateRegimeDetector('t', { windowSamples: 1 }), RangeError);
    assert.throws(
      () => new FailureRateRegimeDetector('t', { onFailureRateRegimeChange: 42 as never }),
      RangeError,
    );
    const d = new FailureRateRegimeDetector('t', { nowMs });
    assert.throws(() => d.observe('bogus' as never), RangeError);
  });
});

describe('FailureRateRegimeMonitor (EB-442)', () => {
  it('keeps topics isolated and never alarms an idle topic', () => {
    now = 5_000_000;
    const seen: FailureRateRegimeChange[] = [];
    const m = new FailureRateRegimeMonitor({
      nowMs,
      onFailureRateRegimeChange: (e) => seen.push(e),
    });
    // Topic A creeps; topic B idles at 1% and topic C never reports at all.
    const feedTopic = (topic: string, outcomes: DeliveryOutcome[]) => {
      for (const o of outcomes) m.observe(topic, o);
    };
    feedTopic('a', spread(100, 1));
    feedTopic('b', spread(100, 1));
    feedTopic('a', spread(100, 3));
    feedTopic('a', spread(100, 5));
    feedTopic('a', spread(300, 24));
    feedTopic('b', spread(300, 3));
    assert.equal(seen.length, 1);
    assert.equal(seen[0].topic, 'a');
    assert.deepEqual(m.topics().sort(), ['a', 'b']);
  });

  it('coexists with a fixed 10% threshold: the threshold stays silent while the detector fires', () => {
    now = 5_000_000;
    const m = new FailureRateRegimeMonitor({ nowMs });
    const stream = [
      ...spread(100, 1),
      ...spread(100, 3),
      ...spread(100, 5),
      ...spread(300, 24),
    ];
    let fixedFired = false;
    let detected: FailureRateRegimeChange | undefined;
    const window: number[] = [];
    for (const o of stream) {
      const e = m.observe('orders', o);
      if (e !== undefined) detected = e;
      window.push(o === 'success' ? 0 : 1);
      if (window.length > 50) window.shift();
      if (window.length === 50 && window.reduce((a, b) => a + b, 0) / 50 >= 0.1) {
        fixedFired = true;
      }
    }
    assert.ok(detected !== undefined, 'the regime detector should fire');
    assert.equal(detected.direction, 'increase');
    assert.equal(fixedFired, false, 'the fixed 10% line must stay silent on this creep');
  });
});

describe('EventBus.reportDeliveryOutcome (EB-442)', () => {
  it('alarms through the bus and stamps on bus time', () => {
    const clock = { nowMs: 5_000_000, now: () => clock.nowMs };
    const seen: FailureRateRegimeChange[] = [];
    const bus = new EventBus({
      now: clock.now,
      failureRegime: { onFailureRateRegimeChange: (e) => seen.push(e) },
    });
    const report = (topic: string, outcomes: DeliveryOutcome[]) => {
      for (const o of outcomes) bus.reportDeliveryOutcome(topic, o);
    };
    report('orders', spread(100, 1));
    report('orders', spread(30, 15));
    assert.equal(seen.length, 1);
    assert.equal(seen[0].topic, 'orders');
    assert.equal(seen[0].direction, 'increase');
    assert.equal(seen[0].detectedAtMs, 5_000_000);
    assert.ok(seen[0].after.failureRate >= 0.4);
  });

  it('throws when the feature is not enabled, and validates topic and outcome', () => {
    const bus = new EventBus();
    assert.throws(() => bus.reportDeliveryOutcome('orders', 'failure'), RangeError);
    const enabled = new EventBus({ failureRegime: {} });
    assert.throws(() => enabled.reportDeliveryOutcome('', 'failure'), RangeError);
    assert.throws(() => enabled.reportDeliveryOutcome('orders', 'bogus' as never), RangeError);
    // Successes alone never alarm and never throw.
    for (let i = 0; i < 200; i++) enabled.reportDeliveryOutcome('orders', 'success');
  });

  it('rejects a malformed failureRegime option fail-fast at construction', () => {
    assert.throws(() => new EventBus({ failureRegime: { delta: 0 } }), RangeError);
    assert.throws(() => new EventBus({ failureRegime: { lambda: -1 } }), RangeError);
    assert.throws(() => new EventBus({ failureRegime: { warmupSamples: 0 } }), RangeError);
  });

  it('forces the detector clock to the bus clock, ignoring a caller-passed nowMs', () => {
    const clock = { nowMs: 7_000_000, now: () => clock.nowMs };
    const seen: FailureRateRegimeChange[] = [];
    const bus = new EventBus({
      now: clock.now,
      failureRegime: {
        nowMs: () => 1, // ignored: the bus forces its own clock
        onFailureRateRegimeChange: (e) => seen.push(e),
      },
    });
    for (const o of spread(100, 1)) bus.reportDeliveryOutcome('orders', o);
    for (const o of spread(30, 15)) bus.reportDeliveryOutcome('orders', o);
    assert.equal(seen.length, 1);
    assert.equal(seen[0].detectedAtMs, 7_000_000);
  });
});
