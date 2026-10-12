import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import {
  SlowStartGate,
  createSlowStartGate,
  resolveSlowStartOptions,
} from '../src/slowstart.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

function makeClock(startMs = 1_000_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function approxEqual(actual: number, expected: number, epsilon = 1e-9): void {
  assert.ok(
    Math.abs(actual - expected) <= epsilon,
    `expected ${actual} to be within ${epsilon} of ${expected}`,
  );
}

test('linear ramp interpolates the rate toward a finite target', () => {
  const clock = makeClock();
  const gate = new SlowStartGate(
    { initialPerSec: 10, rampMs: 1000, strategy: 'linear' },
    110,
    clock.now,
  );
  approxEqual(gate.rateAt(clock.nowMs), 10);
  approxEqual(gate.rateAt(clock.nowMs + 500), 60);
  approxEqual(gate.rateAt(clock.nowMs + 1000), 110);
  approxEqual(gate.rateAt(clock.nowMs + 5000), 110);
  assert.equal(gate.isComplete(clock.nowMs + 999), false);
  assert.equal(gate.isComplete(clock.nowMs + 1000), true);
});

test('exponential ramp grows geometrically toward a finite target', () => {
  const clock = makeClock();
  const gate = new SlowStartGate(
    { initialPerSec: 10, rampMs: 1000, strategy: 'exponential' },
    80,
    clock.now,
  );
  approxEqual(gate.rateAt(clock.nowMs), 10);
  approxEqual(gate.rateAt(clock.nowMs + 500), 10 * Math.sqrt(8));
  approxEqual(gate.rateAt(clock.nowMs + 1000), 80);
});

test('without a finite target the ramp keeps growing until the window elapses', () => {
  const clock = makeClock();
  const linear = new SlowStartGate(
    { initialPerSec: 10, rampMs: 1000, strategy: 'linear' },
    Number.POSITIVE_INFINITY,
    clock.now,
  );
  approxEqual(linear.rateAt(clock.nowMs + 500), 15);
  assert.equal(linear.isComplete(clock.nowMs + 1000), true);
  const exponential = new SlowStartGate(
    { initialPerSec: 10, rampMs: 1000, strategy: 'exponential' },
    Number.POSITIVE_INFINITY,
    clock.now,
  );
  approxEqual(exponential.rateAt(clock.nowMs + 500), 10 * Math.SQRT2);
});

test('resolveSlowStartOptions rejects invalid values', () => {
  assert.throws(() => resolveSlowStartOptions({ initialPerSec: 0 }), RangeError);
  assert.throws(() => resolveSlowStartOptions({ initialPerSec: -3 }), RangeError);
  assert.throws(() => resolveSlowStartOptions({ initialPerSec: Number.NaN }), RangeError);
  assert.throws(() => resolveSlowStartOptions({ rampMs: 0 }), RangeError);
  assert.throws(() => resolveSlowStartOptions({ rampMs: -1 }), RangeError);
  assert.throws(
    () => resolveSlowStartOptions({ strategy: 'cubic' as 'linear' }),
    RangeError,
  );
  assert.throws(() => resolveSlowStartOptions('yes' as unknown as boolean), RangeError);
  assert.equal(resolveSlowStartOptions(undefined), undefined);
  assert.equal(resolveSlowStartOptions(false), undefined);
  assert.deepEqual(resolveSlowStartOptions(true), {
    initialPerSec: 10,
    rampMs: 1000,
    strategy: 'linear',
  });
});

test('a degenerate ramp (initial at/above target) is a no-op, not a decreasing ramp', () => {
  const clock = makeClock();
  assert.equal(
    createSlowStartGate({ initialPerSec: 200, rampMs: 1000 }, 100, clock.now),
    undefined,
  );
  assert.equal(
    createSlowStartGate({ initialPerSec: 100, rampMs: 1000 }, 100, clock.now),
    undefined,
  );
  assert.ok(
    createSlowStartGate({ initialPerSec: 10, rampMs: 1000 }, 100, clock.now) instanceof SlowStartGate,
  );
});

test('early deliveries are paced, then the gate opens fully after the ramp', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: { initialPerSec: 10, rampMs: 1000, strategy: 'linear' },
  });
  assert.equal(bus.getStats().rampingSubscribers, 1);

  for (let i = 0; i < 30; i += 1) bus.publish('t', i);
  await flush();
  // One second of initial budget goes at once; the rest waits.
  assert.equal(received.length, 10);

  // Halfway through the ramp the instantaneous rate is 15/s: 500ms of
  // ramp accrues ~7.5 tokens.
  clock.nowMs += 500;
  bus.publish('t', 'tick');
  await flush();
  assert.equal(received.length, 17);

  // Past the ramp window the gate is dropped: everything drains.
  clock.nowMs += 600;
  bus.publish('t', 'tick');
  await flush();
  assert.equal(received.length, 32);
  assert.equal(bus.getStats().rampingSubscribers, 0);
  sub.unsubscribe();
});

test('slow-start composes with deliveryShaping by taking the minimum', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 500,
    deliveryShaping: { messagesPerSec: 100 },
    slowStart: { initialPerSec: 10, rampMs: 1000, strategy: 'linear' },
  });
  for (let i = 0; i < 200; i += 1) bus.publish('t', i);
  await flush();
  // Shaping alone would allow 100; slow-start binds at 10.
  assert.equal(received.length, 10);
  sub.unsubscribe();
});

test('degenerate slow-start with shaping leaves shaping in force', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 500,
    deliveryShaping: { messagesPerSec: 100 },
    slowStart: { initialPerSec: 500, rampMs: 1000 },
  });
  assert.equal(bus.getStats().rampingSubscribers, 0);
  for (let i = 0; i < 200; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(received.length, 100);
  sub.unsubscribe();
});

test('invalid slowStart options throw RangeError at subscribe time', () => {
  const bus = new EventBus();
  const noop = () => {};
  assert.throws(() => bus.subscribe('t', noop, { slowStart: { initialPerSec: 0 } }), RangeError);
  assert.throws(() => bus.subscribe('t', noop, { slowStart: { rampMs: -5 } }), RangeError);
  assert.throws(
    () => bus.subscribe('t', noop, { slowStart: { strategy: 'x' as 'linear' } }),
    RangeError,
  );
  // A throw leaves no half-registered subscriber behind.
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('shadow subscriptions reject slowStart', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { shadow: true, slowStart: true }),
    RangeError,
  );
});

test('batched subscribers are paced through the collection budget', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const batches: unknown[][] = [];
  const sub = bus.subscribe(
    't',
    (msgs) => batches.push([...(msgs as unknown[])]),
    {
      queueSize: 100,
      batch: { maxSize: 5, maxWaitMs: 10_000 },
      slowStart: { initialPerSec: 5, rampMs: 60_000 },
    },
  );
  for (let i = 0; i < 20; i += 1) bus.publish('t', i);
  await flush();
  // Collection is bounded by the 5-token initial budget: one full batch.
  assert.equal(batches.length, 1);
  assert.equal(batches[0].length, 5);

  // A second of ramp accrues ~5 more tokens: one more batch.
  clock.nowMs += 1000;
  bus.publish('t', 'tick');
  await flush();
  assert.equal(batches.length, 2);
  assert.equal(batches[1].length, 5);
  sub.unsubscribe();
});

test('the re-flush timer drains the backlog as the ramp accrues budget', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: { initialPerSec: 5, rampMs: 60_000 },
  });
  for (let i = 0; i < 20; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(received.length, 5);
  // No new publishes: the armed re-flush timer paces the backlog at ~5/s.
  await new Promise((resolve) => setTimeout(resolve, 450));
  assert.ok(
    received.length >= 6,
    `expected the timer to pace at least one more delivery, got ${received.length}`,
  );
  sub.unsubscribe();
});
