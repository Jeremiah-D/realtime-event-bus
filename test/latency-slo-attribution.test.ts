import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { LatencySloMissEvent } from '../src/bus.ts';
import { RecordingAlertSink } from '../src/ops-alert.ts';

/** Yields until the bus's scheduled flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('attribution is off by default: miss events carry no phaseAttribution', async () => {
  const clock = controllableClock();
  // No `processingLatencySloAlert` — behaves exactly like EB-53 alone.
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  bus.subscribe('t', () => {
    clock.nowMs += 50;
  }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 10, onLatencySloMiss: (e) => events.push(e) },
  });
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].p99Ms, 50);
  assert.ok(!('phaseAttribution' in events[0]));
  // No attribution state is exposed either.
  assert.equal(bus.getStats().sloMisses, 0);
  assert.deepEqual(bus.getStats().sloMissesByPhase, []);
});

test('attribution on: queue wait and handler time are attributed on a miss', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, processingLatencySloAlert: true });
  const events: LatencySloMissEvent[] = [];
  bus.subscribe('t', () => {
    clock.nowMs += 50; // 50ms of handler work
  }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 10, onLatencySloMiss: (e) => events.push(e) },
  });
  bus.publish('t', 'a'); // enqueued at t=1000
  clock.nowMs += 200; // t=1200 — 200ms of queue wait before the flush
  await flush(); // handed off at t=1200, handler runs to t=1250
  assert.equal(events.length, 1);
  const attribution = events[0].phaseAttribution;
  assert.ok(attribution !== undefined);
  // One sample each: p50 == p99 == the single sample.
  assert.equal(attribution.queueWaitMs.p99Ms, 200);
  assert.equal(attribution.handlerMs.p99Ms, 50);
  // The handler phase agrees with the miss event's own p99.
  assert.equal(attribution.handlerMs.p99Ms, events[0].p99Ms);
  // No backpressure, no redelivery on this path.
  assert.equal(attribution.backpressureWaitMs.p99Ms, 0);
  assert.equal(attribution.redeliveryWaitMs.p99Ms, 0);
  // Stats mirror the miss.
  assert.equal(bus.getStats().sloMisses, 1);
  const rows = bus.getStats().sloMissesByPhase;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].sloMisses, 1);
  assert.equal(rows[0].phaseAttribution.queueWaitMs.p99Ms, 200);
});

test('backpressure wait is attributed when the drain is fully gated', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, processingLatencySloAlert: true });
  const events: LatencySloMissEvent[] = [];
  let releaseFirst: (() => void) | undefined;
  bus.subscribe('t', () => {
    clock.nowMs += 5; // 5ms of handler work per invocation
    if (releaseFirst === undefined) {
      // The first delivery holds its in-flight slot until the test
      // releases it — the second message waits behind a fully gated
      // drain (maxInflight budget 0).
      return new Promise<void>((resolve) => {
        releaseFirst = resolve;
      });
    }
    return undefined;
  }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 1, onLatencySloMiss: (e) => events.push(e) },
    maxInflight: 1,
  });
  bus.publish('t', 'm1'); // t=1000
  await flush(); // m1 handed off at t=1000, slot held (inFlight=1)
  bus.publish('t', 'm2'); // t=1000
  await flush(); // gated round: inflight budget 0, queue non-empty → blocked-since stamped
  clock.nowMs += 500; // t=1500
  releaseFirst!(); // slot frees → releaseInflightSlot schedules a flush
  await flush();
  await flush(); // m2 handed off at t=1500
  // m2 waited 500ms behind the gated drain.
  const rows = bus.getStats().sloMissesByPhase;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].phaseAttribution.backpressureWaitMs.p99Ms, 500);
  assert.equal(rows[0].phaseAttribution.queueWaitMs.p99Ms, 500);
  // m1 never waited behind backpressure: its samples are 0.
  assert.equal(rows[0].phaseAttribution.backpressureWaitMs.p50Ms, 0);
});

test('redelivery wait is attributed on a reliable nack redelivery', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, processingLatencySloAlert: true });
  const events: LatencySloMissEvent[] = [];
  let attempts = 0;
  bus.subscribeReliable('t', (delivery) => {
    attempts += 1;
    clock.nowMs += 40; // 40ms of work per attempt
    if (attempts === 1) {
      delivery.nack(); // onRedeliver stamps the redelivery schedule
    } else {
      delivery.ack();
    }
  }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 10, onLatencySloMiss: (e) => events.push(e) },
  });
  bus.publish('t', 'x'); // enqueued at t=1000; schedules flush A
  // Interpose between flush A (first hand-off + nack) and flush D (the
  // redelivery): microtasks run FIFO, so this runs after A's delivery
  // but before D's hand-off.
  queueMicrotask(() => {
    clock.nowMs += 100; // t=1140 — 100ms of redelivery wait
  });
  await flush();
  // A: hand-off at t=1000, handler to t=1040, nack stamps 1040.
  // D: redelivery hand-off at t=1140, handler to t=1180, ack.
  assert.equal(attempts, 2);
  // The redelivery sampled its wait: re-hand-off (1140) − schedule (1040).
  // The first attempt sampled nothing — it was never redelivered, so
  // there is exactly one redelivery-wait sample.
  const rows = bus.getStats().sloMissesByPhase;
  assert.equal(rows.length, 1);
  assert.equal(rows[0].phaseAttribution.redeliveryWaitMs.p99Ms, 100);
  assert.equal(rows[0].phaseAttribution.redeliveryWaitMs.p50Ms, 100);
});

test('a processing-latency-slo-breach alert reaches the ops outlet', async () => {
  const clock = controllableClock();
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    now: clock.now,
    processingLatencySloAlert: true,
    opsAlert: { sink, secret: 's3cr3t', dedupWindowMs: 60_000 },
  });
  bus.subscribe('t', () => {
    clock.nowMs += 50;
  }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 10 },
  });
  bus.publish('t', 'a');
  clock.nowMs += 200;
  await flush();
  // The alert is delivered asynchronously through the sink.
  await flush();
  assert.equal(sink.deliveries.length, 1);
  const delivery = sink.deliveries[0];
  assert.equal(delivery.alert.kind, 'processing-latency-slo-breach');
  const detail = delivery.alert.detail as {
    p99Ms: number;
    phaseAttribution: { queueWaitMs: { p99Ms: number }; handlerMs: { p99Ms: number } };
  };
  assert.equal(detail.p99Ms, 50);
  assert.equal(detail.phaseAttribution.queueWaitMs.p99Ms, 200);
  assert.equal(detail.phaseAttribution.handlerMs.p99Ms, 50);
});

test('stats stay empty when attribution is armed but no SLO is tracked', async () => {
  const bus = new EventBus({ processingLatencySloAlert: true });
  bus.subscribe('t', () => {});
  bus.publish('t', 'hello');
  await flush();
  assert.equal(bus.getStats().sloMisses, 0);
  assert.deepEqual(bus.getStats().sloMissesByPhase, []);
});
