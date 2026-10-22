import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type SlowHandlerEvent } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
/** Yields one macrotask turn (the slow-handler continuation runs on setImmediate). */
const macrotask = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('slow handler trips: rest of drain moves to a macrotask, order preserved', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: SlowHandlerEvent[] = [];
  const delivered: string[] = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      // Simulate a slow handler by advancing the bus clock itself —
      // the same convention the health-probe timeout tests use.
      clock.nowMs += 150;
    },
    { slowHandler: { thresholdMs: 100, onSlowHandler: (e) => events.push(e) } },
  );
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  await flush();
  // The first message was delivered synchronously in the flush; the slow
  // trip moved the rest to a macrotask continuation.
  assert.deepEqual(delivered, ['m1']);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0], {
    subscriberId: sub.id,
    pattern: 't',
    elapsedMs: 150,
    thresholdMs: 100,
  });
  assert.deepEqual(bus.getStats().slowHandlers, { count: 1, lastMs: 150 });
  assert.equal(bus.pendingCount(sub.id), 2);

  // Each continuation delivers one more slow message, then trips again.
  await macrotask();
  assert.deepEqual(delivered, ['m1', 'm2']);
  assert.equal(bus.getStats().slowHandlers.count, 2);
  await macrotask();
  assert.deepEqual(delivered, ['m1', 'm2', 'm3']);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 3, lastMs: 150 });
  assert.equal(bus.pendingCount(sub.id), 0);
});

test('fast handler never trips the offload', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: SlowHandlerEvent[] = [];
  const delivered: string[] = [];
  bus.subscribe('t', (msg) => delivered.push(String(msg.payload)), {
    slowHandler: { thresholdMs: 100, onSlowHandler: (e) => events.push(e) },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await macrotask();
  assert.deepEqual(delivered, ['a', 'b']);
  assert.equal(events.length, 0);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 0, lastMs: 0 });
});

test('offload is disabled by default: slow handler does not trip or record', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const delivered: string[] = [];
  bus.subscribe('t', (msg) => {
    delivered.push(String(msg.payload));
    clock.nowMs += 500;
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  // No offload: the whole drain ran synchronously in one flush.
  assert.deepEqual(delivered, ['a', 'b']);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 0, lastMs: 0 });
});

test('slowHandler: true uses the default 100ms threshold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const delivered: string[] = [];
  bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 150;
    },
    { slowHandler: true },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(delivered, ['a']);
  assert.equal(bus.getStats().slowHandlers.count, 1);
  await macrotask();
  assert.deepEqual(delivered, ['a', 'b']);
});

test('invalid slowHandler options throw RangeError', () => {
  const bus = new EventBus();
  const bad = [
    { thresholdMs: 0 },
    { thresholdMs: -5 },
    { thresholdMs: NaN },
    { thresholdMs: Infinity },
    { thresholdMs: 'fast' },
    { thresholdMs: 100, onSlowHandler: 'nope' },
    { thresholdMs: 100, onSlowHandler: 42 },
  ];
  for (const opt of bad) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { slowHandler: opt as never }),
      RangeError,
      `slowHandler=${JSON.stringify(opt)}`,
    );
  }
  // No half-registered subscriber is left behind by a throw.
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('composes with healthProbe: processingTimeout still timed after offload', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const delivered: string[] = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 150;
    },
    {
      healthProbe: { maxConsecutiveFailures: 5, processingTimeoutMs: 50 },
      slowHandler: { thresholdMs: 100 },
    },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  // The handler was slow on both axes: the health probe counted a
  // processing-timeout failure, and the offload moved the rest async.
  assert.deepEqual(delivered, ['a']);
  assert.deepEqual(bus.subscriberHealth(sub.id)?.consecutiveFailures, 1);
  assert.equal(bus.getStats().slowHandlers.count, 1);
  await macrotask();
  assert.deepEqual(delivered, ['a', 'b']);
  assert.deepEqual(bus.subscriberHealth(sub.id)?.consecutiveFailures, 2);
  assert.equal(bus.getStats().slowHandlers.count, 2);
});

test('unsubscribe before the continuation: no further delivery', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const delivered: string[] = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 150;
    },
    { slowHandler: { thresholdMs: 100 } },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(delivered, ['a']);
  sub.unsubscribe();
  await macrotask();
  await macrotask();
  assert.deepEqual(delivered, ['a']);
});

test('throwing onSlowHandler never breaks the drain', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const delivered: string[] = [];
  bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 150;
    },
    {
      slowHandler: {
        thresholdMs: 100,
        onSlowHandler: () => {
          throw new Error('observer boom');
        },
      },
    },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await macrotask();
  assert.deepEqual(delivered, ['a', 'b']);
  assert.equal(bus.getStats().slowHandlers.count, 2);
});

test('metrics exposition carries the slow-handler gauge', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe(
    't',
    () => {
      clock.nowMs += 150;
    },
    { slowHandler: { thresholdMs: 100 } },
  );
  let text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_slow_handler_last_ms 0'));
  bus.publish('t', 'a');
  await flush();
  text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_slow_handler_last_ms 150'), `missing gauge in:\n${text}`);
});

test('slowHandler cannot combine with shadow subscriptions', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        shadow: true,
        slowHandler: { thresholdMs: 100 },
      } as never),
    RangeError,
  );
});
