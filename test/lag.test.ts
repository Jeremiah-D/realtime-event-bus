import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { LagTracker } from '../src/lag.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('tracker records dwell with nearest-rank p50/p99', () => {
  const tracker = new LagTracker();
  for (const ms of [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]) tracker.record(ms);
  const s = tracker.summary();
  assert.equal(s.samples, 10);
  // Nearest-rank: p50 -> rank ceil(5)-1 = 4 -> 50; p99 -> rank 9 -> 100.
  assert.equal(s.p50Ms, 50);
  assert.equal(s.p99Ms, 100);
  assert.equal(s.minMs, 10);
  assert.equal(s.maxMs, 100);
  assert.equal(s.meanMs, 55);
});

test('tracker evicts the oldest samples past the window and clamps negatives', () => {
  const tracker = new LagTracker(4);
  for (const ms of [10, 20, 30, 40, 50, 60]) tracker.record(ms);
  const s = tracker.summary();
  assert.equal(s.samples, 4);
  assert.equal(s.minMs, 30); // 10 and 20 were evicted
  assert.equal(s.maxMs, 60);
  assert.equal(s.meanMs, 45);
  // Clock skew (drain reading before the enqueue stamp) clamps at 0.
  tracker.record(-5);
  assert.equal(tracker.summary().minMs, 0);
  assert.equal(tracker.summary().samples, 4); // still bounded
});

test('tracker rejects a non-positive or non-integer window', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => new LagTracker(bad), RangeError);
  }
});

test('invalid lagMonitor options throw from subscribe', () => {
  const bus = new EventBus();
  for (const bad of [0, -3, 2.5]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { lagMonitor: { windowSize: bad } }),
      RangeError,
    );
  }
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { lagMonitor: { thresholdMs: bad } }),
      RangeError,
    );
  }
  // onLag with no threshold can never fire: fail fast.
  assert.throws(
    () => bus.subscribe('t', () => {}, { lagMonitor: { onLag: () => {} } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { lagMonitor: { thresholdMs: 10, onLag: 'x' as never } }),
    TypeError,
  );
  assert.throws(() => bus.subscribe('t', () => {}, { lagMonitor: 42 as never }), TypeError);
  // The failed subscribes left no half-registered subscribers behind.
  assert.equal(bus.subscriberCount(), 0);
});

test('watermark measures the oldest queued message dwell', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { lagMonitor: true });
  bus.publish('t', 'a'); // enqueued at t=1000
  clock.nowMs += 30;
  bus.publish('t', 'b'); // enqueued at t=1030
  clock.nowMs += 12; // now t=1042, flush has not run yet
  const entry = bus.getStats().lag[0];
  assert.equal(entry.pattern, 't');
  assert.equal(entry.watermarkMs, 42); // head 'a' waited 42ms
  assert.equal(entry.samples, 0); // nothing drained yet
  await flush();
  const after = bus.getStats().lag[0];
  assert.equal(after.watermarkMs, 0); // queue empty
  assert.equal(after.samples, 2);
});

test('unmonitored subscribers add no lag entries', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  const stats = bus.getStats();
  assert.equal(stats.lag.length, 0);
  assert.equal(stats.laggingSubscribers.length, 0);
});

test('onLag fires once per excursion and re-arms after recovery', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: Array<{ lagMs: number; thresholdMs: number; queueSize: number }> = [];
  const sub = bus.subscribe('t', () => {}, {
    lagMonitor: {
      thresholdMs: 100,
      onLag: (e) => events.push({ lagMs: e.lagMs, thresholdMs: e.thresholdMs, queueSize: e.queueSize }),
    },
  });
  bus.publish('t', 'a'); // head stamped at t=1000
  clock.nowMs += 150; // head now waits 150ms > 100ms threshold
  bus.publish('t', 'b'); // enqueue path evaluates the watermark -> fires
  assert.equal(events.length, 1);
  assert.equal(events[0].lagMs, 150);
  assert.equal(events[0].thresholdMs, 100);
  assert.equal(events[0].queueSize, 2);
  // Still past the threshold: no second fire (latched).
  clock.nowMs += 50;
  bus.publish('t', 'c');
  assert.equal(events.length, 1);
  // Drain everything: the watermark drops to 0 and the latch re-arms.
  await flush();
  assert.equal(bus.getStats().lag[0].watermarkMs, 0);
  bus.publish('t', 'd');
  clock.nowMs += 200;
  bus.publish('t', 'e');
  assert.equal(events.length, 2);
  assert.equal(events[1].lagMs, 200);
  assert.match(sub.id, /^sub-\d+$/);
  await flush();
});

test('onLag also fires on the drain path when the backlog survives', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let fires = 0;
  // Shaped subscriber: the drain only delivers 1 msg/sec, so the backlog
  // survives the first flush and the drain-path check observes it.
  bus.subscribe('t', () => {}, {
    lagMonitor: { thresholdMs: 50, onLag: () => { fires += 1; } },
    deliveryShaping: { messagesPerSec: 1, burst: 1 },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  clock.nowMs += 60;
  await flush(); // delivers 'a' (dwell 60), 'b' stays queued
  assert.equal(fires, 1);
  const entry = bus.getStats().lag[0];
  assert.equal(entry.samples, 1);
  assert.equal(entry.p99Ms, 60);
});

test('p50/p99 summarize the drain dwell window', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { lagMonitor: true });
  for (let i = 0; i < 10; i += 1) {
    bus.publish('t', i);
    clock.nowMs += 10;
  }
  clock.nowMs = 2000; // deliver everything at t=2000
  await flush();
  // Dwells: 2000 - (1000 + i*10) for i=0..9 -> 1000, 990, ..., 910.
  const entry = bus.getStats().lag[0];
  assert.equal(entry.samples, 10);
  assert.equal(entry.p50Ms, 950);
  assert.equal(entry.p99Ms, 1000);
  assert.equal(entry.minMs, 910);
  assert.equal(entry.maxMs, 1000);
});

test('laggingSubscribers ranks monitored subscribers by p99', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('slow', () => {}, { lagMonitor: true });
  bus.subscribe('fast', () => {}, { lagMonitor: true });
  bus.publish('slow', 1); // enqueued at t=1000
  clock.nowMs += 50;
  bus.publish('fast', 2); // enqueued at t=1050
  clock.nowMs += 50; // flush at t=1100
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.laggingSubscribers.length, 2);
  assert.equal(stats.laggingSubscribers[0].pattern, 'slow');
  assert.equal(stats.laggingSubscribers[0].p99Ms, 100);
  assert.equal(stats.laggingSubscribers[0].watermarkMs, 0);
  assert.equal(stats.laggingSubscribers[1].pattern, 'fast');
  assert.equal(stats.laggingSubscribers[1].p99Ms, 50);
});

test('dropped messages are never sampled and the watermark tracks survivors', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { lagMonitor: true, queueSize: 2 });
  for (let i = 0; i < 5; i += 1) {
    bus.publish('t', i);
    clock.nowMs += 10; // i=0@1000, i=1@1010, ..., i=4@1040
  }
  // Capacity 2, drop-oldest: messages 0..2 were shed before the flush;
  // survivors are 3 (@1030) and 4 (@1040).
  clock.nowMs += 10; // flush at t=1060
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.droppedMessages, 3);
  const entry = stats.lag[0];
  assert.equal(entry.samples, 2);
  // Drained dwells: 1060-1030=30, 1060-1040=20.
  assert.equal(entry.p50Ms, 20); // nearest-rank over [20, 30]
  assert.equal(entry.p99Ms, 30);
});

test('exposition renders lag quantile, samples, and watermark gauges', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('orders.*', () => {}, { lagMonitor: true });
  bus.publish('orders.new', 1);
  clock.nowMs += 42;
  await flush();
  const text = renderPrometheus(bus.getStats());
  const labels = `subscriber="${sub.id}",pattern="orders.*"`;
  assert.ok(text.includes(`eventbus_lag_ms{quantile="0.5",${labels}} 42`), 'p50 gauge missing');
  assert.ok(text.includes(`eventbus_lag_ms{quantile="0.99",${labels}} 42`), 'p99 gauge missing');
  assert.ok(text.includes(`eventbus_lag_samples{${labels}} 1`), 'samples gauge missing');
  assert.ok(
    text.includes(`eventbus_lag_watermark_ms{${labels}} 0`),
    'watermark gauge missing',
  );
  // An unmonitored subscriber adds no lag series.
  const plain = new EventBus();
  plain.subscribe('t', () => {});
  const plainText = renderPrometheus(plain.getStats());
  assert.ok(!plainText.includes('eventbus_lag_ms{'));
});

test('watermark is visible before the flush when the queue is backlogged', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { lagMonitor: { thresholdMs: 1000 } });
  bus.publish('t', 'x');
  clock.nowMs += 2500; // no flush yet: the message sits 2500ms
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_lag_watermark_ms{subscriber="sub-1",pattern="t"} 2500'));
  await flush();
});
