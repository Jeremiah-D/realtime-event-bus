import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  CircuitBreakerOpenError,
  type AdmissionRejectionEvent,
} from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so half-open probe intervals are deterministic instead of racing wall
 * time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/**
 * Subscribes a throwing handler with a health probe and drives it to the
 * degraded (auto-paused) state. Returns the subscription id.
 */
async function degradeSubscriber(bus: EventBus, topic: string): Promise<string> {
  const sub = bus.subscribe(
    topic,
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 3 } },
  );
  bus.publish(topic, 'a');
  bus.publish(topic, 'b');
  bus.publish(topic, 'c');
  await flush();
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  return sub.id;
}

function assertCircuitOpenError(fn: () => void, topic: string): void {
  assert.throws(fn, (err: unknown) => {
    assert.ok(err instanceof CircuitBreakerOpenError);
    assert.equal(err.name, 'CircuitBreakerOpenError');
    assert.equal(err.code, 'CIRCUIT_BREAKER_OPEN');
    assert.equal(err.topic, topic);
    return true;
  });
}

test('disabled by default: a degraded topic publishes exactly as before', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 't');
  // No rule armed: degraded delivery pauses, the queue still accepts, and
  // publish never throws — the pre-EB-116 behavior, byte for byte.
  assert.equal(bus.publish('t', 'd'), 1);
  assert.equal(bus.circuitBreakerState('t'), undefined);
  const stats = bus.getStats();
  assert.equal(stats.circuitBrokenMessages, 0);
  assert.equal(stats.circuitBreakerTrips, 0);
  assert.deepEqual(stats.circuitBreakers, []);
  const topics = stats.topics.filter((row) => row.topic === 't');
  assert.equal(topics.length, 1);
  assert.equal(topics[0].circuitBrokenMessages, 0);
});

test('setPublishCircuitBreaker validates its arguments', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setPublishCircuitBreaker('', {}), RangeError);
  assert.throws(() => bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 0 }), RangeError);
  assert.throws(() => bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 1.5 }), RangeError);
  assert.throws(() => bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: -2 }), RangeError);
  assert.throws(() => bus.setPublishCircuitBreaker('t', { halfOpenProbeIntervalMs: 0 }), RangeError);
  assert.throws(() => bus.setPublishCircuitBreaker('t', { halfOpenProbeIntervalMs: -1 }), RangeError);
  assert.throws(() => bus.setPublishCircuitBreaker('t', { halfOpenProbeIntervalMs: NaN }), RangeError);
  assert.throws(
    () => bus.setPublishCircuitBreaker('t', { halfOpenProbeIntervalMs: Infinity }),
    RangeError,
  );
  // Clearing is boolean-idempotent and re-arming resets the state.
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 2 });
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  assert.equal(bus.clearPublishCircuitBreaker('t'), true);
  assert.equal(bus.clearPublishCircuitBreaker('t'), false);
  assert.equal(bus.circuitBreakerState('t'), undefined);
});

test('trips open after N consecutive publishes with every subscriber degraded', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 't');
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 2 });
  // Degraded delivery is paused but the queue still accepts: the breaker
  // watches subscriber *state*, not the publish return value.
  assert.equal(bus.publish('t', 'd'), 1);
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  bus.publish('t', 'e');
  assert.equal(bus.circuitBreakerState('t'), 'open');
  assert.equal(bus.getStats().circuitBreakerTrips, 1);
  // Fast-fail: the message is refused outright, never fanned out.
  assertCircuitOpenError(() => bus.publish('t', 'f'), 't');
  assert.equal(bus.getStats().circuitBrokenMessages, 1);
});

test('fast-fail refusal counts stats, the breaker row, and the hook', async () => {
  const events: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({ onAdmissionRejected: (e) => events.push(e) });
  await degradeSubscriber(bus, 't');
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 1 });
  bus.publish('t', 'd'); // trips open immediately
  assert.equal(bus.circuitBreakerState('t'), 'open');
  assertCircuitOpenError(() => bus.publish('t', 'nope'), 't');
  assertCircuitOpenError(() => bus.publish('t', 'nope'), 't');
  // Hook and stats stay exactly reconcilable.
  assert.equal(events.length, 2);
  for (const event of events) {
    assert.equal(event.topic, 't');
    assert.equal(event.reason, 'circuit-breaker');
  }
  const stats = bus.getStats();
  assert.equal(stats.circuitBrokenMessages, 2);
  assert.deepEqual(stats.circuitBreakers, [{ topic: 't', state: 'open', rejectedMessages: 2 }]);
  const row = stats.topics.find((entry) => entry.topic === 't');
  assert.ok(row !== undefined);
  assert.equal(row.circuitBrokenMessages, 2);
  // Refusals never touch the publish/seq counters: no message was made.
  assert.equal(stats.totalPublished, 4); // a, b, c, d — the two 'nope' publishes were refused pre-admission
});

test('throttle-shedding subscribers trip the breaker too', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, { queueSize: 10, throttle: true });
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 2 });
  for (let i = 0; i < 8; i += 1) bus.publish('t', i); // hits the high-water mark: throttle engages
  assert.equal(bus.getStats().throttledSubscribers, 1);
  bus.publish('t', 'a'); // every subscriber mid shed-excursion: streak 2 → trips
  assert.equal(bus.circuitBreakerState('t'), 'open');
  assertCircuitOpenError(() => bus.publish('t', 'b'), 't');
});

test('one healthy subscriber prevents (and resets) the trip streak', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload)); // healthy
  await degradeSubscriber(bus, 't'); // second subscriber, degraded
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 2 });
  for (let i = 0; i < 5; i += 1) bus.publish('t', i);
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  assert.equal(bus.getStats().circuitBreakerTrips, 0);
  await flush();
  assert.equal(received.length, 8); // 3 drive-to-degraded + 5
});

test('a topic with no subscribers never trips', () => {
  const bus = new EventBus();
  bus.setPublishCircuitBreaker('ghost', { consecutiveShedPublishes: 1 });
  for (let i = 0; i < 3; i += 1) assert.equal(bus.publish('ghost', i), 0);
  assert.equal(bus.circuitBreakerState('ghost'), 'closed');
  assert.equal(bus.getStats().circuitBreakerTrips, 0);
});

test('a healthy publish resets the consecutive streak', async () => {
  const bus = new EventBus();
  const subId = await degradeSubscriber(bus, 't');
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 3 });
  bus.publish('t', 'd'); // streak 1
  bus.publish('t', 'e'); // streak 2 — not yet open
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  // The subscriber recovers: the next publish finds a healthy consumer
  // and the streak restarts from zero.
  assert.equal(bus.resume(subId), true);
  bus.publish('t', 'f'); // streak reset to 0
  bus.publish('t', 'g'); // streak 1
  bus.publish('t', 'h'); // streak 2 — still closed, proving the reset
  assert.equal(bus.circuitBreakerState('t'), 'closed');
});

test('half-open probe succeeds: recovers and closes the breaker', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const subId = await degradeSubscriber(bus, 't');
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 1, halfOpenProbeIntervalMs: 1000 });
  bus.publish('t', 'd'); // trips open
  assert.equal(bus.circuitBreakerState('t'), 'open');
  // The operator fixed the consumer: resume clears the degraded pause.
  assert.equal(bus.resume(subId), true);
  clock.nowMs += 999;
  assertCircuitOpenError(() => bus.publish('t', 'too-early'), 't');
  clock.nowMs += 1; // probe interval elapsed: this publish is the probe
  assert.equal(bus.publish('t', 'probe'), 1);
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  // Normal service resumes: no more fast-fails, the streak restarts clean.
  assert.equal(bus.publish('t', 'after'), 1);
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  assert.equal(bus.getStats().circuitBreakerTrips, 1);
});

test('half-open probe fails: re-opens the breaker', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  await degradeSubscriber(bus, 't'); // stays degraded: recovery never happens
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 1, halfOpenProbeIntervalMs: 1000 });
  bus.publish('t', 'd'); // trips open
  assert.equal(bus.circuitBreakerState('t'), 'open');
  clock.nowMs += 1000; // probe interval elapsed: this publish is the probe
  assert.equal(bus.publish('t', 'probe'), 1); // admitted as the probe (queue accepts)
  assert.equal(bus.circuitBreakerState('t'), 'open'); // still all-degraded: re-opened
  assert.equal(bus.getStats().circuitBreakerTrips, 1); // one trip, not two
  // The probe interval restarts at the re-open: immediate publishes fail fast.
  assertCircuitOpenError(() => bus.publish('t', 'x'), 't');
  assert.equal(bus.getStats().circuitBrokenMessages, 1);
});

test('clearing the rule disarms the gate', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 't');
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 1 });
  bus.publish('t', 'd'); // trips open
  assert.equal(bus.clearPublishCircuitBreaker('t'), true);
  assert.equal(bus.publish('t', 'e'), 1); // gate gone: normal degraded behavior
  assert.equal(bus.circuitBreakerState('t'), undefined);
});

test('Prometheus exposition carries the breaker series', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 't');
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 1 });
  bus.publish('t', 'd'); // trips open
  assertCircuitOpenError(() => bus.publish('t', 'nope'), 't');
  const exposition = renderPrometheus(bus.getStats());
  assert.match(exposition, /^eventbus_circuit_broken_messages_total 1$/m);
  assert.match(exposition, /^eventbus_circuit_breaker_trips_total 1$/m);
  assert.match(exposition, /^eventbus_circuit_breakers_open 1$/m);
  assert.match(exposition, /^eventbus_topic_circuit_broken_messages_total\{topic="t"\} 1$/m);
});
