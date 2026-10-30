import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { LatencySloMissEvent } from '../src/bus.ts';

/**
 * Processing-latency SLO threshold revision (EB-290):
 * `setLatencySloThreshold` retunes a monitored subscriber's p99
 * threshold without re-subscribing — only `latencySlo.thresholdMs`
 * moves, the tracker window and its samples, the `onLatencySloMiss`
 * callback, and the `alerted` latch are untouched, and the new
 * threshold governs from the next sample evaluation on both paths
 * (plain invocation sampling and reliable `ack()`-completion
 * sampling), which read the live value.
 *
 * Determinism trick (as in `latency-slo.test.ts`): the handler
 * advances a controllable clock, so every sample is an exact,
 * deterministic processing time.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('setLatencySloThreshold rejects unknown, empty, unmonitored, and invalid revisions fail-closed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: {
      windowSize: 10,
      p99ThresholdMs: 100,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  const plain = bus.subscribe('t2', () => {});
  for (const bad of [
    () => bus.setLatencySloThreshold('no-such-sub', 50),
    () => bus.setLatencySloThreshold('', 50),
    () => bus.setLatencySloThreshold(plain.id, 50),
    () => bus.setLatencySloThreshold(sub.id, 0),
    () => bus.setLatencySloThreshold(sub.id, -5),
    () => bus.setLatencySloThreshold(sub.id, Number.NaN),
    () => bus.setLatencySloThreshold(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setLatencySloThreshold(sub.id, '50' as never),
    () => bus.setLatencySloThreshold(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'latency_slo_threshold_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live threshold, the samples, and the latch are untouched: the
  // old threshold still governs.
  const entry = bus.getStats().subscriberLatencyP99[0];
  assert.equal(entry.thresholdMs, 100);
  assert.equal(entry.samples, 0);
  assert.equal(entry.breaching, false);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('lowering the threshold below the live p99 alerts on the next sample', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: {
      windowSize: 10,
      p99ThresholdMs: 100,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 0); // p99 = 25, under the threshold
  bus.setLatencySloThreshold(sub.id, 15);
  // The sampled window is not recomputed by the revision: the same
  // samples, the same p99 — only the threshold moves.
  const before = bus.getStats().subscriberLatencyP99[0];
  assert.equal(before.samples, 10);
  assert.equal(before.p99Ms, 25);
  assert.equal(before.thresholdMs, 15);
  assert.equal(before.breaching, false);
  // The very next sample evaluates against the live threshold.
  bus.publish('t', 'next');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].p99Ms, 25);
  assert.equal(events[0].thresholdMs, 15);
  assert.equal(events[0].samples, 10);
  assert.equal(bus.getStats().subscriberLatencyP99[0].breaching, true);
  const audit = bus.getSubscriptionAudit({ action: 'latency_slo_threshold_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { p99ThresholdMs: 15, oldP99ThresholdMs: 100 });
  sub.unsubscribe();
});

test('a revision identical to the current threshold is a no-op with no audit', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 100 },
  });
  bus.setLatencySloThreshold(sub.id, 100);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'latency_slo_threshold_revised' }).length,
    0,
    'a same-threshold revision must not audit',
  );
  assert.equal(bus.getStats().subscriberLatencyP99[0].thresholdMs, 100);
  sub.unsubscribe();
});

test('raising the threshold never clears an already-triggered latch; the latch re-arms only when the p99 falls below the new threshold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  let duration = 25;
  const sub = bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: {
      windowSize: 10,
      p99ThresholdMs: 15,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdMs, 15);
  // Raise the threshold while breaching: the latch stays set — a
  // further slow sample does not re-fire, and no audit-side clearing
  // happens either.
  bus.setLatencySloThreshold(sub.id, 20);
  bus.publish('t', 'still-slow');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().subscriberLatencyP99[0].breaching, true);
  // Drain the slow samples out of the window: the p99 falls to 5,
  // below the new threshold, and the latch re-arms without firing.
  duration = 5;
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().subscriberLatencyP99[0].breaching, false);
  // Crossing again fires a second time, against the new threshold.
  duration = 25;
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 2);
  assert.equal(events[1].p99Ms, 25);
  assert.equal(events[1].thresholdMs, 20);
  const audit = bus.getSubscriptionAudit({ action: 'latency_slo_threshold_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { p99ThresholdMs: 20, oldP99ThresholdMs: 15 });
  sub.unsubscribe();
});

test('reliable subscribers evaluate the revised threshold on ack completion', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  const sub = bus.subscribeReliable('t', (delivery) => {
    clock.nowMs += 30; // consumer think time before acking
    delivery.ack();
  }, {
    latencySlo: {
      windowSize: 8,
      p99ThresholdMs: 100,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  for (let i = 0; i < 8; i++) bus.publish('t', i);
  await flush();
  const entry = bus.getStats().subscriberLatencyP99[0];
  assert.equal(entry.samples, 8);
  assert.equal(entry.p99Ms, 30);
  assert.equal(events.length, 0);
  // Lower the threshold below the live p99: the next ack-completion
  // sample fires, exactly like the plain invocation path.
  bus.setLatencySloThreshold(sub.id, 20);
  bus.publish('t', 'next');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].p99Ms, 30);
  assert.equal(events[0].thresholdMs, 20);
  assert.equal(bus.getStats().subscriberLatencyP99[0].samples, 8);
  sub.unsubscribe();
});
