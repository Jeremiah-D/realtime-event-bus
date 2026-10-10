import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { LatencySloMissEvent } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('invalid latencySlo config throws RangeError/TypeError from subscribe', () => {
  const bus = new EventBus();
  // p99ThresholdMs is required and must be a positive finite number.
  for (const bad of [undefined, 0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { latencySlo: { p99ThresholdMs: bad as number } }),
      RangeError,
    );
  }
  // windowSize must be a positive integer.
  for (const bad of [0, -3, 2.5]) {
    assert.throws(
      () =>
        bus.subscribe('t', () => {}, { latencySlo: { p99ThresholdMs: 10, windowSize: bad } }),
      RangeError,
    );
  }
  // The callback must be a function; the option itself must be an object.
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        latencySlo: { p99ThresholdMs: 10, onLatencySloMiss: 'nope' as never },
      }),
    TypeError,
  );
  assert.throws(() => bus.subscribe('t', () => {}, { latencySlo: true as never }), TypeError);
  // The failed subscribes left no half-registered subscribers behind.
  assert.equal(bus.subscriberCount(), 0);
});

test('latencySlo is opt-in: untracked subscribers expose nothing', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 'hello');
  await flush();
  assert.deepEqual(bus.getStats().subscriberLatencyP99, []);
  const text = renderPrometheus(bus.getStats());
  assert.ok(!text.includes('eventbus_subscriber_processing_latency_p99{'));
});

test('samples handler processing time with an injected clock (deterministic p99)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => { clock.nowMs += 12; }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 1000 },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.subscriberLatencyP99.length, 1);
  const entry = stats.subscriberLatencyP99[0];
  assert.match(entry.subscriberId, /^sub-\d+$/);
  assert.equal(entry.pattern, 't');
  assert.equal(entry.samples, 10);
  assert.equal(entry.p99Ms, 12);
  assert.equal(entry.thresholdMs, 1000);
  assert.equal(entry.breaching, false);
});

test('nearest-rank p99 pins to the top sample of the window', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const durations = [...Array(98).fill(5), 80, 80];
  let i = 0;
  bus.subscribe('t', () => { clock.nowMs += durations[i++]; }, {
    latencySlo: { windowSize: 100, p99ThresholdMs: 1000 },
  });
  for (let n = 0; n < 100; n++) bus.publish('t', n);
  await flush();
  const entry = bus.getStats().subscriberLatencyP99[0];
  assert.equal(entry.samples, 100);
  // Nearest-rank: rank ceil(0.99 * 100) - 1 = 98 -> the top two 80ms samples.
  assert.equal(entry.p99Ms, 80);
});

test('breach fires onLatencySloMiss exactly once per excursion', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  let duration = 5;
  bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: {
      windowSize: 10,
      p99ThresholdMs: 15,
      onLatencySloMiss: (e) => { events.push(e); },
    },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 0); // p99 = 5, under the threshold
  duration = 25;
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  // The first slow sample pushed the windowed p99 to 25; the latch holds
  // for the rest of the excursion.
  assert.equal(events.length, 1);
  const event = events[0];
  assert.match(event.subscriberId, /^sub-\d+$/);
  assert.equal(event.pattern, 't');
  assert.equal(event.p99Ms, 25);
  assert.equal(event.thresholdMs, 15);
  assert.equal(event.samples, 10);
  assert.equal(typeof event.at, 'number');
  assert.equal(bus.getStats().subscriberLatencyP99[0].breaching, true);
});

test('latch re-arms when the p99 drops back to or below the threshold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let calls = 0;
  let duration = 25;
  bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: {
      windowSize: 10,
      p99ThresholdMs: 15,
      onLatencySloMiss: () => { calls += 1; },
    },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(calls, 1);
  // Drain the slow samples out of the window: the p99 falls back to 5 and
  // the latch re-arms without firing again.
  duration = 5;
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(calls, 1);
  assert.equal(bus.getStats().subscriberLatencyP99[0].breaching, false);
  // Crossing again fires a second time.
  duration = 25;
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(calls, 2);
});

test('a throwing onLatencySloMiss never disturbs the delivery flow', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let delivered = 0;
  let attempts = 0;
  bus.subscribe('t', () => { clock.nowMs += 5; delivered += 1; }, {
    latencySlo: {
      p99ThresholdMs: 1,
      onLatencySloMiss: () => {
        attempts += 1;
        throw new Error('alert pipeline is down');
      },
    },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush(); // would surface a propagated throw as an uncaught exception
  assert.equal(delivered, 5);
  assert.equal(attempts, 1); // latch held despite the throw
  assert.equal(bus.getStats().subscriberLatencyP99[0].breaching, true);
});

test('window evicts the oldest samples past the bound', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const durations = [100, 10, 10, 10, 10];
  let i = 0;
  let calls = 0;
  bus.subscribe('t', () => { clock.nowMs += durations[i++]; }, {
    latencySlo: {
      windowSize: 4,
      p99ThresholdMs: 50,
      onLatencySloMiss: () => { calls += 1; },
    },
  });
  for (let n = 0; n < 5; n++) bus.publish('t', n);
  await flush();
  const entry = bus.getStats().subscriberLatencyP99[0];
  assert.equal(entry.samples, 4);
  // The 100ms sample was evicted; the window holds four 10ms samples.
  assert.equal(entry.p99Ms, 10);
  assert.equal(entry.breaching, false);
  // The excursion fired once while the 100ms sample was still in the window.
  assert.equal(calls, 1);
});

test('SLO alert is orthogonal to healthProbe: slow handler alerts without degrading', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let delivered = 0;
  let sloCalls = 0;
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; delivered += 1; }, {
    healthProbe: { maxConsecutiveFailures: 2, processingTimeoutMs: 5000 },
    latencySlo: { p99ThresholdMs: 15, onLatencySloMiss: () => { sloCalls += 1; } },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  // 25ms trips the SLO alert, but stays inside the health probe's
  // processing budget and never throws: no health failure is counted.
  assert.equal(sloCalls, 1);
  assert.equal(delivered, 3);
  const health = bus.subscriberHealth(sub.id);
  assert.equal(health.enabled, true);
  assert.equal(health.degraded, false);
  assert.equal(health.consecutiveFailures, 0);
});

test('advisory only: a breaching subscriber keeps receiving messages', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let delivered = 0;
  bus.subscribe('t', () => { clock.nowMs += 25; delivered += 1; }, {
    latencySlo: { p99ThresholdMs: 10 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(delivered, 5);
  const entry = bus.getStats().subscriberLatencyP99[0];
  assert.equal(entry.breaching, true);
  assert.equal(entry.p99Ms, 25);
});

test('reliable subscribers sample delivery→ack completion, exactly once per message', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribeReliable('t', (delivery) => {
    clock.nowMs += 30; // consumer think time before acking
    delivery.ack();
  }, {
    latencySlo: { windowSize: 8, p99ThresholdMs: 100 },
  });
  bus.publish('t', 'x');
  await flush();
  const entry = bus.getStats().subscriberLatencyP99[0];
  // One sample per ack completion — the synchronous invocation timing is
  // skipped for reliable subscribers, so there is no double counting.
  assert.equal(entry.samples, 1);
  assert.equal(entry.p99Ms, 30);
  assert.equal(entry.breaching, false);
});

test('reliable: deliveries that are never acked are never sampled', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribeReliable('t', () => {
    clock.nowMs += 30; // handled but never acked
  }, {
    latencySlo: { p99ThresholdMs: 100 },
    ackTimeoutMs: 60_000, // no redelivery inside the test
  });
  bus.publish('t', 'x');
  await flush();
  const entry = bus.getStats().subscriberLatencyP99[0];
  assert.equal(entry.samples, 0);
  assert.equal(entry.p99Ms, 0);
});

test('batch subscribers contribute one sample per batch invocation', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let invocations = 0;
  const sub = bus.subscribe('t', (msgs) => {
    invocations += 1;
    assert.ok(Array.isArray(msgs));
    clock.nowMs += 40;
  }, {
    batch: { maxSize: 3, maxWaitMs: 10_000 },
    latencySlo: { p99ThresholdMs: 1000 },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.equal(invocations, 1);
  const entry = bus.getStats().subscriberLatencyP99[0];
  assert.equal(entry.samples, 1);
  assert.equal(entry.p99Ms, 40);
  sub.unsubscribe();
});

test('metrics render the p99 gauge only for SLO-tracked subscribers', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('a', () => { clock.nowMs += 12; }, {
    latencySlo: { p99ThresholdMs: 1000 },
  });
  bus.subscribe('b', () => { clock.nowMs += 12; });
  bus.publish('a', 1);
  bus.publish('b', 2);
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(
    text.includes('eventbus_subscriber_processing_latency_p99{subscriber="sub-1",pattern="a"} 12'),
  );
  assert.ok(!text.includes('pattern="b"}'));
});

test('processing-latency and ack-latency sample independently on one reliable subscriber', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribeReliable('t', (delivery) => {
    clock.nowMs += 30;
    delivery.ack();
  }, {
    ackLatency: true,
    latencySlo: { p99ThresholdMs: 1000 },
  });
  bus.publish('t', 'x'); // accepted at t=1000
  clock.nowMs += 100; // queue dwell before the flush hands it off
  await flush(); // delivered at t=1100, acked at t=1130
  const stats = bus.getStats();
  // ackLatency measures accepted→ack (130); the SLO window measures
  // delivery→ack only (30): enabling one never double-samples the other.
  assert.equal(stats.ackLatency[0].p99Ms, 130);
  const entry = stats.subscriberLatencyP99[0];
  assert.equal(entry.samples, 1);
  assert.equal(entry.p99Ms, 30);
});
