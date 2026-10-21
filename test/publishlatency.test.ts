import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import {
  AdmissionLatencyTracker,
  resolveAdmissionLatencyOptions,
} from '../src/publishlatency.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('tracker records total spans with nearest-rank percentiles and slow counts', () => {
  const tracker = new AdmissionLatencyTracker({ slowAdmissionThresholdMs: 50 });
  for (const ms of [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]) tracker.record('t', ms);
  const s = tracker.summary('t');
  assert.equal(s.samples, 10);
  // Nearest-rank: p50 -> rank ceil(5)-1 = 4 -> 50; p95/p99 -> rank 9 -> 100.
  assert.equal(s.p50Ms, 50);
  assert.equal(s.p95Ms, 100);
  assert.equal(s.p99Ms, 100);
  assert.equal(s.minMs, 10);
  assert.equal(s.maxMs, 100);
  assert.equal(s.meanMs, 55);
  // Samples at or above the 50ms threshold: 50..100 -> 6.
  assert.equal(s.slowCount, 6);
});

test('tracker evicts oldest samples past the window and clamps negatives', () => {
  const tracker = new AdmissionLatencyTracker({ windowSize: 4 });
  for (const ms of [10, 20, 30, 40, 50, 60]) tracker.record('t', ms);
  const s = tracker.summary('t');
  assert.equal(s.samples, 4);
  assert.equal(s.minMs, 30); // 10 and 20 were evicted
  assert.equal(s.maxMs, 60);
  assert.equal(s.meanMs, 45);
  // Clock skew clamps at 0.
  tracker.record('t', -5);
  assert.equal(tracker.summary('t').minMs, 0);
});

test('tracker ranks slow-admission topics by p99', () => {
  const tracker = new AdmissionLatencyTracker();
  tracker.record('fast', 5);
  tracker.record('fast', 6);
  tracker.record('slow', 200);
  tracker.record('slow', 210);
  tracker.record('mid', 50);
  const top = tracker.topSlowTopics(2);
  assert.equal(top.length, 2);
  assert.equal(top[0].topic, 'slow');
  assert.equal(top[0].p99Ms, 210);
  assert.equal(top[0].samples, 2);
  assert.equal(top[1].topic, 'mid');
  // Unknown topics summarize to zeros.
  assert.equal(tracker.summary('nope').samples, 0);
  assert.deepEqual(tracker.topics().sort(), ['fast', 'mid', 'slow']);
});

test('gateSummary reports zeroed rows for unsampled gates', () => {
  const tracker = new AdmissionLatencyTracker();
  tracker.recordGate('t', 'schema', 12);
  const gates = tracker.gateSummary('t');
  assert.equal(gates.schema.samples, 1);
  assert.equal(gates.schema.p50Ms, 12);
  assert.equal(gates.acl.samples, 0);
  assert.equal(gates.acl.p99Ms, 0);
});

test('resolveAdmissionLatencyOptions validates', () => {
  assert.equal(resolveAdmissionLatencyOptions(undefined), undefined);
  assert.equal(resolveAdmissionLatencyOptions(false), undefined);
  assert.deepEqual(resolveAdmissionLatencyOptions(true), {});
  assert.throws(() => resolveAdmissionLatencyOptions('yes' as never), TypeError);
  assert.throws(() => resolveAdmissionLatencyOptions({ windowSize: 0 }), RangeError);
  assert.throws(() => resolveAdmissionLatencyOptions({ gateWindowSize: -1 }), RangeError);
  assert.throws(
    () => resolveAdmissionLatencyOptions({ slowAdmissionThresholdMs: -5 }),
    RangeError,
  );
  // The constructor surfaces the same validation.
  assert.throws(() => new EventBus({ publishLatency: { windowSize: 0 } }), RangeError);
});

test('disabled by default: no sampling, empty stats', () => {
  const bus = new EventBus();
  bus.publish('t', 'x');
  const stats = bus.getStats();
  assert.deepEqual(stats.publishLatency, []);
  assert.deepEqual(stats.publishLatencyGates, []);
  assert.deepEqual(stats.slowAdmissionTopics, []);
});

test('publish samples the schema gate and the total span', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, publishLatency: { slowAdmissionThresholdMs: 10 } });
  // A schema validator that consumes 25ms of bus-clock time: the schema
  // gate sample is deterministic.
  bus.setTopicSchema('orders', () => {
    clock.nowMs += 25;
    return true;
  });
  bus.setTopicRateLimit('orders', 1000);
  bus.subscribe('orders', () => {});
  bus.publish('orders', { id: 1 });

  const stats = bus.getStats();
  assert.equal(stats.publishLatency.length, 1);
  const total = stats.publishLatency[0];
  assert.equal(total.topic, 'orders');
  assert.equal(total.samples, 1);
  assert.equal(total.p50Ms, 25);
  assert.equal(total.p95Ms, 25);
  assert.equal(total.p99Ms, 25);
  assert.equal(total.slowCount, 1); // 25 >= 10ms threshold

  const gates = stats.publishLatencyGates.filter((g) => g.topic === 'orders');
  const byGate = new Map(gates.map((g) => [g.gate, g]));
  assert.equal(byGate.get('schema')!.p50Ms, 25);
  // The other gates ran (zero bus-clock time each).
  assert.equal(byGate.get('acl')!.samples, 1);
  assert.equal(byGate.get('acl')!.p50Ms, 0);
  assert.equal(byGate.get('publisher-rate-limit')!.samples, 1);
  assert.equal(byGate.get('size')!.samples, 1);
  assert.equal(byGate.get('topic-rate-limit')!.samples, 1);

  // Slow-admission hotspot ranking surfaces the topic.
  assert.equal(stats.slowAdmissionTopics.length, 1);
  assert.equal(stats.slowAdmissionTopics[0].topic, 'orders');
  assert.equal(stats.slowAdmissionTopics[0].p99Ms, 25);

  // Prometheus exposition renders the series.
  const exposition = renderPrometheus(stats);
  assert.match(
    exposition,
    /eventbus_publish_admission_latency_ms\{quantile="0.99",topic="orders"\} 25/,
  );
  assert.match(
    exposition,
    /eventbus_publish_admission_gate_latency_ms\{quantile="0.5",topic="orders",gate="schema"\} 25/,
  );
  assert.match(exposition, /eventbus_publish_admission_slow_total\{topic="orders"\} 1/);
});

test('rejected publishes sample up to the refusing gate', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, publishLatency: true });
  bus.setTopicSchema('orders', () => {
    clock.nowMs += 40;
    return false; // reject
  });
  assert.equal(bus.publish('orders', { id: 'bad' }), 0);

  const stats = bus.getStats();
  const total = stats.publishLatency.find((s) => s.topic === 'orders')!;
  assert.equal(total.samples, 1);
  assert.equal(total.p50Ms, 40); // span stopped at the schema rejection
  const gates = new Map(
    stats.publishLatencyGates.filter((g) => g.topic === 'orders').map((g) => [g.gate, g]),
  );
  assert.equal(gates.get('schema')!.samples, 1);
  // Gates after schema never ran for the rejected publish.
  assert.ok(!gates.has('size'));
  assert.ok(!gates.has('topic-rate-limit'));
});

test('publishBatch samples each entry', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, publishLatency: true });
  bus.setTopicSchema('t', () => {
    clock.nowMs += 10;
    return true;
  });
  bus.subscribe('t', () => {});
  const accepted = bus.publishBatch([
    { topic: 't', payload: 1 },
    { topic: 't', payload: 2 },
    { topic: 't', payload: 3 },
  ]);
  assert.equal(accepted, 3);
  const stats = bus.getStats();
  const total = stats.publishLatency.find((s) => s.topic === 't')!;
  assert.equal(total.samples, 3);
  assert.equal(total.p50Ms, 10);
});

test('publishIdempotent samples the dedup gate', () => {
  const bus = new EventBus({ publishLatency: true });
  bus.subscribe('t', () => {});
  const first = bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  assert.equal(first.duplicate, false);
  const second = bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  assert.equal(second.duplicate, true); // suppressed duplicate

  const stats = bus.getStats();
  const gates = new Map(
    stats.publishLatencyGates.filter((g) => g.topic === 't').map((g) => [g.gate, g]),
  );
  // Both the first attempt and the suppressed duplicate probed the table.
  assert.equal(gates.get('idempotency-dedup')!.samples, 2);
  // The admitted first attempt ran the full gate section: one total span.
  const total = stats.publishLatency.find((s) => s.topic === 't')!;
  assert.equal(total.samples, 1);
});

test('publishAtomic samples the shadow admission gates', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, publishLatency: true });
  bus.setTopicSchema('t', () => {
    clock.nowMs += 15;
    return true;
  });
  bus.subscribe('t', () => {});
  const result = bus.publishAtomic([
    { topic: 't', payload: 1 },
    { topic: 't', payload: 2 },
  ]);
  assert.equal(result.published, 2);

  const stats = bus.getStats();
  const total = stats.publishLatency.find((s) => s.topic === 't')!;
  // One total-span sample per shadow-admitted entry.
  assert.equal(total.samples, 2);
  assert.equal(total.p50Ms, 15);
  const gates = new Map(
    stats.publishLatencyGates.filter((g) => g.topic === 't').map((g) => [g.gate, g]),
  );
  assert.equal(gates.get('schema')!.samples, 2);
  assert.equal(gates.get('acl')!.samples, 2);
});

test('publishDelayed samples the schedule-time fail-fast gates', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, publishLatency: true });
  bus.setTopicSchema('t', () => {
    clock.nowMs += 20;
    return true;
  });
  bus.setTopicRateLimit('t', 1000);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'late', { delayMs: 1000 });
  assert.match(id as string, /^delayed-\d+$/);

  // Schedule time ran the fail-fast schema gate once.
  let gates = new Map(
    bus
      .getStats()
      .publishLatencyGates.filter((g) => g.topic === 't')
      .map((g) => [g.gate, g]),
  );
  assert.equal(gates.get('schema')!.samples, 1);
  assert.equal(gates.get('schema')!.p50Ms, 20);

  // Due-time fan-out (pre-admitted): the topic-rate-limit gate re-runs.
  clock.nowMs = 2_000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'late']);
  gates = new Map(
    bus
      .getStats()
      .publishLatencyGates.filter((g) => g.topic === 't')
      .map((g) => [g.gate, g]),
  );
  assert.equal(gates.get('topic-rate-limit')!.samples, 2); // poke + late
});

test('slowAdmissionTopics ranks topics by total-span p99', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, publishLatency: true });
  bus.setTopicSchema('heavy', () => {
    clock.nowMs += 100;
    return true;
  });
  bus.setTopicSchema('light', () => {
    clock.nowMs += 5;
    return true;
  });
  bus.subscribe('heavy', () => {});
  bus.subscribe('light', () => {});
  bus.publish('light', 1);
  bus.publish('heavy', 1);
  bus.publish('light', 2);

  const stats = bus.getStats();
  assert.equal(stats.slowAdmissionTopics[0].topic, 'heavy');
  assert.equal(stats.slowAdmissionTopics[0].p99Ms, 100);
  assert.equal(stats.slowAdmissionTopics[1].topic, 'light');
});
