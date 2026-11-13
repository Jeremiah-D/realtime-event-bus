import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type SlowHandlerEvent,
  type SlowHandlerOptions,
} from '../src/bus.ts';

/**
 * Bus-level default slow-handler offload (EB-497):
 * `setDefaultSlowHandler` installs the `thresholdMs` / `onSlowHandler`
 * new opt-in `slowHandler` subscribers fall back to when they omit a
 * field — the priority chain is subscriber-explicit > bus default >
 * built-in (thresholdMs 100, no callback) — and the default never
 * enables the offload: a subscriber that does not opt into
 * `slowHandler` is untouched by it.
 *
 * A "slow" handler simulates overrunning its budget by moving the bus
 * clock forward itself (the same convention the health-probe timeout
 * and slow-handler tests use), keeping the trip tests deterministic.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
/** Yields one macrotask turn (the slow-handler continuation runs on setImmediate). */
const macrotask = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('EB-497: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultSlowHandler(), undefined);
  bus.setDefaultSlowHandler(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_slow_handler_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-497: slowHandler: true takes the whole bus default (threshold + callback)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: SlowHandlerEvent[] = [];
  const delivered: string[] = [];
  bus.setDefaultSlowHandler({
    thresholdMs: 50,
    onSlowHandler: (e) => events.push(e),
  });
  const sub = bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 100;
    },
    { slowHandler: true },
  );
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  await flush();
  // The first message was delivered synchronously in the flush; the
  // slow trip at the default 50 ms moved the rest to a macrotask.
  assert.deepEqual(delivered, ['m1']);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0], {
    subscriberId: sub.id,
    pattern: 't',
    elapsedMs: 100,
    thresholdMs: 50,
  });
  assert.deepEqual(bus.getStats().slowHandlers, { count: 1, lastMs: 100 });
  await macrotask();
  assert.deepEqual(delivered, ['m1', 'm2']);
});

test('EB-497: an explicit thresholdMs wins over the bus default', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: SlowHandlerEvent[] = [];
  const delivered: string[] = [];
  bus.setDefaultSlowHandler({
    thresholdMs: 50,
    onSlowHandler: (e) => events.push(e),
  });
  bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 100;
    },
    { slowHandler: { thresholdMs: 200 } },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await macrotask();
  // 100 ms is over the default 50 but under the explicit 200: no trip,
  // and the default callback never fires.
  assert.deepEqual(delivered, ['a', 'b']);
  assert.equal(events.length, 0);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 0, lastMs: 0 });
});

test('EB-497: an explicit onSlowHandler wins over the default callback', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const defaultEvents: SlowHandlerEvent[] = [];
  const ownEvents: SlowHandlerEvent[] = [];
  bus.setDefaultSlowHandler({
    thresholdMs: 50,
    onSlowHandler: (e) => defaultEvents.push(e),
  });
  bus.subscribe(
    't',
    () => {
      clock.nowMs += 100;
    },
    { slowHandler: { onSlowHandler: (e) => ownEvents.push(e) } },
  );
  bus.publish('t', 'a');
  await flush();
  // The default threshold (50) still applies — only the callback is
  // overridden.
  assert.equal(ownEvents.length, 1);
  assert.equal(ownEvents[0].thresholdMs, 50);
  assert.equal(defaultEvents.length, 0);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 1, lastMs: 100 });
});

test('EB-497: slowHandler: {} opts in and takes the whole bus default too', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: SlowHandlerEvent[] = [];
  bus.setDefaultSlowHandler({
    thresholdMs: 50,
    onSlowHandler: (e) => events.push(e),
  });
  bus.subscribe(
    't',
    () => {
      clock.nowMs += 100;
    },
    { slowHandler: {} },
  );
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdMs, 50);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 1, lastMs: 100 });
});

test('EB-497: with no default, true still takes the built-ins 100 / no callback', async () => {
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
  assert.deepEqual(bus.getStats().slowHandlers, { count: 1, lastMs: 150 });
  await macrotask();
  assert.deepEqual(delivered, ['a', 'b']);

  // Just under the built-in threshold: no trip at all.
  const fastClock = controllableClock();
  const fastBus = new EventBus({ now: fastClock.now });
  const fastDelivered: string[] = [];
  fastBus.subscribe(
    'u',
    (msg) => {
      fastDelivered.push(String(msg.payload));
      fastClock.nowMs += 50;
    },
    { slowHandler: true },
  );
  fastBus.publish('u', 'a');
  fastBus.publish('u', 'b');
  await flush();
  await macrotask();
  assert.deepEqual(fastDelivered, ['a', 'b']);
  assert.deepEqual(fastBus.getStats().slowHandlers, { count: 0, lastMs: 0 });
});

test('EB-497: clearing to null restores the built-ins for new subscribers', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const defaultEvents: SlowHandlerEvent[] = [];
  bus.setDefaultSlowHandler({
    thresholdMs: 50,
    onSlowHandler: (e) => defaultEvents.push(e),
  });
  bus.setDefaultSlowHandler(null);
  assert.equal(bus.getDefaultSlowHandler(), undefined);
  const audit = bus.getSubscriptionAudit({ action: 'default_slow_handler_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { thresholdMs: 50, onSlowHandler: 'set' },
    slowHandler: null,
  });
  const delivered: string[] = [];
  bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 100;
    },
    { slowHandler: true },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await macrotask();
  // 100 ms does not exceed the built-in 100: no trip, and the cleared
  // default callback never fires.
  assert.deepEqual(delivered, ['a', 'b']);
  assert.equal(defaultEvents.length, 0);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 0, lastMs: 0 });
});

test('EB-497: omitted/false slowHandler is untouched by the bus default', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowHandler({ thresholdMs: 10 });
  const receivedA: unknown[] = [];
  const receivedB: unknown[] = [];
  bus.subscribe('a', (msg) => {
    receivedA.push(msg.payload);
    clock.nowMs += 500;
  });
  bus.subscribe(
    'b',
    (msg) => {
      receivedB.push(msg.payload);
      clock.nowMs += 500;
    },
    { slowHandler: false },
  );
  bus.publish('a', 'x');
  bus.publish('b', 'x');
  await flush();
  // No offload: the whole drain ran synchronously in one flush.
  assert.deepEqual(receivedA, ['x']);
  assert.deepEqual(receivedB, ['x']);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 0, lastMs: 0 });
});

test('EB-497: audit shape, defensive copies, and same-value no-op', () => {
  const bus = new EventBus();
  bus.setDefaultSlowHandler({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_slow_handler_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultSlowHandler(), undefined);
  const cb = () => {};
  const opts: SlowHandlerOptions = { thresholdMs: 50, onSlowHandler: cb };
  bus.setDefaultSlowHandler(opts);
  opts.thresholdMs = 999;
  assert.deepEqual(
    bus.getDefaultSlowHandler(),
    { thresholdMs: 50, onSlowHandler: cb },
    'the setter stores a defensive copy',
  );
  bus.setDefaultSlowHandler({ thresholdMs: 50, onSlowHandler: cb });
  bus.setDefaultSlowHandler({ thresholdMs: 50, onSlowHandler: cb });
  const audit = bus.getSubscriptionAudit({ action: 'default_slow_handler_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    slowHandler: { thresholdMs: 50, onSlowHandler: 'set' },
  });
  assert.equal(audit[0].actor, 'system');
  const held = bus.getDefaultSlowHandler();
  assert.ok(held !== undefined);
  held.thresholdMs = 999;
  assert.deepEqual(
    bus.getDefaultSlowHandler(),
    { thresholdMs: 50, onSlowHandler: cb },
    'the getter returns a copy',
  );
  bus.setDefaultSlowHandler({ thresholdMs: 80 }, 'ops');
  assert.deepEqual(bus.getDefaultSlowHandler(), { thresholdMs: 80 });
  const revised = bus.getSubscriptionAudit({ action: 'default_slow_handler_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { thresholdMs: 50, onSlowHandler: 'set' },
    slowHandler: { thresholdMs: 80 },
  });
  assert.equal(revised[1].actor, 'ops');
});

test('EB-497: invalid values throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  const cb = () => {};
  bus.setDefaultSlowHandler({ thresholdMs: 50, onSlowHandler: cb });
  const bad = [
    0,
    -1,
    '50',
    [],
    true,
    { thresholdMs: 0 },
    { thresholdMs: -1 },
    { thresholdMs: Number.NaN },
    { thresholdMs: Number.POSITIVE_INFINITY },
    { thresholdMs: '50' },
    { onSlowHandler: 'x' },
    { onSlowHandler: 42 },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultSlowHandler(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultSlowHandler(), { thresholdMs: 50, onSlowHandler: cb });
  }
  assert.throws(() => bus.setDefaultSlowHandler({ thresholdMs: 70 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultSlowHandler(), { thresholdMs: 50, onSlowHandler: cb });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_slow_handler_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-497: revising the bus default never moves an already-subscribed threshold or armed continuation', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: SlowHandlerEvent[] = [];
  const delivered: string[] = [];
  bus.setDefaultSlowHandler({ thresholdMs: 50, onSlowHandler: (e) => events.push(e) });
  const sub = bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 100;
    },
    { slowHandler: true },
  );
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  await flush();
  // The first trip armed a macrotask continuation for m2/m3.
  assert.deepEqual(delivered, ['m1']);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 1, lastMs: 100 });
  assert.equal(bus.pendingCount(sub.id), 2);
  // Revise the default far above the baked threshold: the early
  // subscriber keeps tripping at 50 and its armed continuation still
  // drains on the baked parameters.
  bus.setDefaultSlowHandler({ thresholdMs: 500 });
  await macrotask();
  assert.deepEqual(delivered, ['m1', 'm2']);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 2, lastMs: 100 });
  await macrotask();
  assert.deepEqual(delivered, ['m1', 'm2', 'm3']);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 3, lastMs: 100 });
  assert.equal(events.length, 3);
  assert.ok(events.every((e) => e.thresholdMs === 50));
  // A subscriber joining after the revision takes the new default.
  const lateDelivered: string[] = [];
  bus.subscribe(
    'u',
    (msg) => {
      lateDelivered.push(String(msg.payload));
      clock.nowMs += 100;
    },
    { slowHandler: true },
  );
  bus.publish('u', 'a');
  bus.publish('u', 'b');
  await flush();
  await macrotask();
  assert.deepEqual(lateDelivered, ['a', 'b']);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 3, lastMs: 100 });
});

test('EB-497: setSlowHandler(null) clears the offload and the default never re-applies', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: SlowHandlerEvent[] = [];
  bus.setDefaultSlowHandler({ thresholdMs: 50, onSlowHandler: (e) => events.push(e) });
  const sub = bus.subscribe(
    't',
    () => {
      clock.nowMs += 100;
    },
    { slowHandler: true },
  );
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 1, lastMs: 100 });
  bus.setSlowHandler(sub.id, null);
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  await macrotask();
  // Cleared: no new trip, and the bus default does not re-apply.
  assert.equal(events.length, 1);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 1, lastMs: 100 });
  // A revision after a clear is refused until the subscriber opts in again.
  assert.throws(() => bus.setSlowHandler(sub.id, { thresholdMs: 10 }), RangeError);
  // The bus default itself survived the per-subscriber clear: a new
  // subscriber joining now still takes it (its trip fires the same
  // default callback).
  bus.subscribe(
    'u',
    () => {
      clock.nowMs += 100;
    },
    { slowHandler: true },
  );
  bus.publish('u', 'a');
  await flush();
  assert.equal(events.length, 2);
  assert.deepEqual(bus.getStats().slowHandlers, { count: 2, lastMs: 100 });
});
