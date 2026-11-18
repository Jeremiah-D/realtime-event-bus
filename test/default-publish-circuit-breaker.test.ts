/**
 * Bus-level default publish circuit breaker thresholds live revision
 * (EB-533).
 *
 * `setDefaultPublishCircuitBreaker` decides the trip thresholds a topic
 * without an explicit `setPublishCircuitBreaker` rule falls back to.
 * The default never arms a topic by itself — a record materializes on
 * demand, only when a publish finds every subscriber of the topic
 * degraded/shedding, so a healthy topic never grows a record. The
 * priority chain is explicit rule > bus default > no protection.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  CircuitBreakerOpenError,
} from '../src/bus.ts';

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

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_publish_circuit_breaker_revised' });

test('default threshold trips an unregistered degraded topic, then half-open probe closes it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const subId = await degradeSubscriber(bus, 't');
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  // No explicit rule, but the default covers the topic: the first
  // all-degraded publish materializes a closed `fromDefault` record.
  bus.publish('t', 'd');
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  assert.deepEqual(bus.publishCircuitBreaker('t'), {
    topic: 't',
    consecutiveShedPublishes: 2,
    halfOpenProbeIntervalMs: 1000,
    state: 'closed',
    consecutiveBlocked: 1,
    rejectedMessages: 0,
  });
  // Second all-degraded publish: streak 2 reaches the default threshold.
  bus.publish('t', 'e');
  assert.equal(bus.circuitBreakerState('t'), 'open');
  // Third publish fast-fails and is counted.
  assertCircuitOpenError(() => bus.publish('t', 'f'), 't');
  assert.equal(bus.getStats().circuitBrokenMessages, 1);
  assert.deepEqual(bus.getStats().circuitBreakers, [
    { topic: 't', state: 'open', rejectedMessages: 1 },
  ]);
  // The operator fixed the consumer: resume clears the degraded pause.
  assert.equal(bus.resume(subId), true);
  clock.nowMs += 999;
  assertCircuitOpenError(() => bus.publish('t', 'too-early'), 't');
  clock.nowMs += 1; // probe interval elapsed: this publish is the probe
  assert.equal(bus.publish('t', 'probe'), 1);
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  assert.equal(bus.getStats().circuitBreakerTrips, 1);
});

test('a healthy topic never materializes a record under a default', () => {
  const bus = new EventBus();
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  bus.subscribe('t', () => {});
  for (let i = 0; i < 5; i += 1) bus.publish('t', i);
  assert.equal(bus.circuitBreakerState('t'), undefined);
  assert.equal(bus.publishCircuitBreaker('t'), undefined);
  assert.deepEqual(bus.getStats().circuitBreakers, []);
  assert.equal(bus.getStats().circuitBreakerTrips, 0);
});

test('an explicit rule beats the default on its own topic only', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 'a');
  await degradeSubscriber(bus, 'b');
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  bus.setPublishCircuitBreaker('a', { consecutiveShedPublishes: 5 });
  bus.publish('a', 'd');
  bus.publish('a', 'e');
  assert.equal(bus.circuitBreakerState('a'), 'closed');
  bus.publish('b', 'd');
  bus.publish('b', 'e');
  assert.equal(bus.circuitBreakerState('b'), 'open');
  // The explicit topic keeps its own thresholds; the default-covered
  // topic took the default ones.
  assert.equal(bus.publishCircuitBreaker('a')?.consecutiveShedPublishes, 5);
  assert.equal(bus.publishCircuitBreaker('b')?.consecutiveShedPublishes, 2);
});

test('re-arming a materialized topic rebuilds it explicit and shields it from default revisions', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 't');
  // NOTE: `{ consecutiveShedPublishes: 5 }` alone resolves to the factory
  // thresholds and is a no-op on a factory bus — the 500ms interval here
  // makes this a real install.
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 5, halfOpenProbeIntervalMs: 500 });
  bus.publish('t', 'd'); // materializes: streak 1, closed
  assert.equal(bus.publishCircuitBreaker('t')?.consecutiveBlocked, 1);
  // Explicit re-arm: full rebuild — state closed, streak zeroed.
  bus.setPublishCircuitBreaker('t', { consecutiveShedPublishes: 5 });
  assert.deepEqual(bus.publishCircuitBreaker('t'), {
    topic: 't',
    consecutiveShedPublishes: 5,
    halfOpenProbeIntervalMs: 1000,
    state: 'closed',
    consecutiveBlocked: 0,
    rejectedMessages: 0,
  });
  // A later default revision never retunes the now-explicit record.
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  assert.equal(bus.publishCircuitBreaker('t')?.consecutiveShedPublishes, 5);
  assert.equal(bus.getDefaultPublishCircuitBreaker()?.consecutiveShedPublishes, 2);
});

test('clearing a rule under a default falls back to default coverage on the next shed publish', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 't');
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  bus.publish('t', 'd'); // materializes
  assert.equal(bus.publishCircuitBreaker('t')?.consecutiveBlocked, 1);
  assert.equal(bus.clearPublishCircuitBreaker('t'), true);
  assert.equal(bus.circuitBreakerState('t'), undefined);
  assert.equal(bus.publishCircuitBreaker('t'), undefined);
  // No per-topic opt-out while a default is installed: the next
  // all-degraded publish materializes a fresh record.
  bus.publish('t', 'e');
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  assert.equal(bus.publishCircuitBreaker('t')?.consecutiveBlocked, 1);
});

test('a default revision live-retunes a materialized record trip threshold', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 't');
  // NOTE: `{ consecutiveShedPublishes: 5 }` alone resolves to the factory
  // thresholds and is a no-op on a factory bus — the 500ms interval here
  // makes this a real install.
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 5, halfOpenProbeIntervalMs: 500 });
  bus.publish('t', 'd'); // streak 1 of 5 — still closed
  assert.equal(bus.circuitBreakerState('t'), 'closed');
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  assert.equal(bus.publishCircuitBreaker('t')?.consecutiveShedPublishes, 2);
  assert.equal(bus.publishCircuitBreaker('t')?.consecutiveBlocked, 1);
  // The next all-degraded publish trips at the retuned threshold.
  bus.publish('t', 'e');
  assert.equal(bus.circuitBreakerState('t'), 'open');
});

test('a default revision shortens the probe interval of an open materialized breaker', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const subId = await degradeSubscriber(bus, 't');
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 1, halfOpenProbeIntervalMs: 1000 });
  bus.publish('t', 'd'); // trips at once
  assert.equal(bus.circuitBreakerState('t'), 'open');
  assertCircuitOpenError(() => bus.publish('t', 'refused'), 't');
  assert.equal(bus.publishCircuitBreaker('t')?.rejectedMessages, 1);
  // Retune: only the interval moves — the open state, the open time,
  // and the refusal count are preserved verbatim.
  bus.setDefaultPublishCircuitBreaker({ halfOpenProbeIntervalMs: 100 });
  const snapshot = bus.publishCircuitBreaker('t');
  assert.equal(snapshot?.halfOpenProbeIntervalMs, 100);
  assert.equal(snapshot?.state, 'open');
  assert.equal(snapshot?.rejectedMessages, 1);
  assert.equal(bus.resume(subId), true);
  clock.nowMs += 99;
  assertCircuitOpenError(() => bus.publish('t', 'too-early'), 't');
  clock.nowMs += 1; // probe admitted 900ms earlier than before
  assert.equal(bus.publish('t', 'probe'), 1);
  assert.equal(bus.circuitBreakerState('t'), 'closed');
});

test('clearing the default retires materialized records but leaves explicit ones', async () => {
  const bus = new EventBus();
  await degradeSubscriber(bus, 'a');
  await degradeSubscriber(bus, 'b');
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  bus.setPublishCircuitBreaker('b', { consecutiveShedPublishes: 5 });
  bus.publish('a', 'd');
  bus.publish('a', 'e'); // 'a' trips open on the default
  assert.equal(bus.circuitBreakerState('a'), 'open');
  bus.publish('b', 'd');
  assert.equal(bus.circuitBreakerState('b'), 'closed');
  bus.setDefaultPublishCircuitBreaker(null);
  // The default-covered record is retired — its stats row disappears
  // and the topic is unprotected again; the explicit record survives.
  assert.equal(bus.circuitBreakerState('a'), undefined);
  assert.equal(bus.publishCircuitBreaker('a'), undefined);
  assert.equal(bus.circuitBreakerState('b'), 'closed');
  assert.deepEqual(bus.getStats().circuitBreakers, [
    { topic: 'b', state: 'closed', rejectedMessages: 0 },
  ]);
  // The retired topic no longer fast-fails, even fully degraded.
  assert.equal(bus.publish('a', 'after'), 1);
  assert.equal(bus.circuitBreakerState('a'), undefined);
});

test('revisePublishCircuitBreaker still refuses a topic with no record and no default', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.revisePublishCircuitBreaker('ghost', { consecutiveShedPublishes: 2 }),
    (err: unknown) => {
      assert.ok(err instanceof RangeError);
      return true;
    },
  );
});

test('illegal default values throw and leave the live default untouched', () => {
  const bus = new EventBus();
  const illegal = [
    { consecutiveShedPublishes: 0 },
    { consecutiveShedPublishes: 1.5 },
    { halfOpenProbeIntervalMs: 0 },
    { halfOpenProbeIntervalMs: Number.NaN },
    { halfOpenProbeIntervalMs: Number.POSITIVE_INFINITY },
  ];
  for (const opts of illegal) {
    assert.throws(() => bus.setDefaultPublishCircuitBreaker(opts), RangeError);
    assert.equal(bus.getDefaultPublishCircuitBreaker(), undefined);
    assert.equal(defaultAudits(bus).length, 0);
  }
  for (const opts of ['x', 5, [] as never]) {
    assert.throws(() => bus.setDefaultPublishCircuitBreaker(opts as never), TypeError);
    assert.equal(bus.getDefaultPublishCircuitBreaker(), undefined);
    assert.equal(defaultAudits(bus).length, 0);
  }
  // After a valid install, illegal attempts still leave it untouched.
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  assert.throws(
    () => bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 0 }),
    RangeError,
  );
  assert.deepEqual(bus.getDefaultPublishCircuitBreaker(), {
    consecutiveShedPublishes: 2,
    halfOpenProbeIntervalMs: 1000,
  });
  assert.equal(defaultAudits(bus).length, 1);
});

test('same-value revisions and clearing an unset default audit nothing', () => {
  const bus = new EventBus();
  // `{}` on a factory bus is the factory thresholds already — no-op.
  bus.setDefaultPublishCircuitBreaker({});
  assert.equal(defaultAudits(bus).length, 0);
  assert.equal(bus.getDefaultPublishCircuitBreaker(), undefined);
  // Clearing an already-unset default is a no-op too.
  bus.setDefaultPublishCircuitBreaker(null);
  assert.equal(defaultAudits(bus).length, 0);
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  assert.equal(defaultAudits(bus).length, 1);
  // Re-affirming the same resolved thresholds audits nothing.
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2, halfOpenProbeIntervalMs: 1000 });
  assert.equal(defaultAudits(bus).length, 1);
  // Clearing an installed default audits once; clearing again does not.
  bus.setDefaultPublishCircuitBreaker(null);
  assert.equal(defaultAudits(bus).length, 2);
  bus.setDefaultPublishCircuitBreaker(null);
  assert.equal(defaultAudits(bus).length, 2);
});

test('audit carries the exact resolved previous and current thresholds, getter follows live', () => {
  const bus = new EventBus();
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  assert.deepEqual(defaultAudits(bus).map((a) => a.detail), [
    {
      previous: null,
      circuitBreaker: { consecutiveShedPublishes: 2, halfOpenProbeIntervalMs: 1000 },
    },
  ]);
  assert.equal(defaultAudits(bus)[0].actor, 'system');
  assert.deepEqual(bus.getDefaultPublishCircuitBreaker(), {
    consecutiveShedPublishes: 2,
    halfOpenProbeIntervalMs: 1000,
  });
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 3 }, 'ops');
  assert.deepEqual(defaultAudits(bus)[1].detail, {
    previous: { consecutiveShedPublishes: 2, halfOpenProbeIntervalMs: 1000 },
    circuitBreaker: { consecutiveShedPublishes: 3, halfOpenProbeIntervalMs: 1000 },
  });
  assert.equal(defaultAudits(bus)[1].actor, 'ops');
  bus.setDefaultPublishCircuitBreaker(null);
  assert.deepEqual(defaultAudits(bus)[2].detail, {
    previous: { consecutiveShedPublishes: 3, halfOpenProbeIntervalMs: 1000 },
    circuitBreaker: null,
  });
  assert.equal(bus.getDefaultPublishCircuitBreaker(), undefined);
  // The getter returns a copy: mutating it never moves the live default.
  bus.setDefaultPublishCircuitBreaker({ consecutiveShedPublishes: 2 });
  const copy = bus.getDefaultPublishCircuitBreaker() as { consecutiveShedPublishes: number };
  copy.consecutiveShedPublishes = 99;
  assert.equal(bus.getDefaultPublishCircuitBreaker()?.consecutiveShedPublishes, 2);
});
