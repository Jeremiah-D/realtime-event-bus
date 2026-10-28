import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type SlowHandlerEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
/** Yields one macrotask turn (the slow-handler continuation runs on setImmediate). */
const macrotask = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('setSlowHandler tightens the threshold from the next handler call', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: SlowHandlerEvent[] = [];
  const delivered: string[] = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 150; // slow against 100, fast against 200
    },
    { slowHandler: { thresholdMs: 200, onSlowHandler: (e) => events.push(e) } },
  );
  bus.publish('t', 'a');
  await flush();
  assert.deepEqual(delivered, ['a']);
  assert.equal(events.length, 0, '150ms does not trip the 200ms threshold');
  assert.deepEqual(bus.getStats().slowHandlers, { count: 0, lastMs: 0 });

  bus.setSlowHandler(sub.id, { thresholdMs: 100 });
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(delivered, ['a', 'b']);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0], {
    subscriberId: sub.id,
    pattern: 't',
    elapsedMs: 150,
    thresholdMs: 100,
  });

  const audit = bus.getSubscriptionAudit({ action: 'slow_handler_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    slowHandler: { thresholdMs: 100, onSlowHandler: 'set' },
    oldSlowHandler: { thresholdMs: 200, onSlowHandler: 'set' },
  });
  sub.unsubscribe();
});

test('setSlowHandler loosening stops trips, and a revised callback replaces the old one', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const oldEvents: SlowHandlerEvent[] = [];
  const newEvents: SlowHandlerEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      clock.nowMs += 150;
    },
    { slowHandler: { thresholdMs: 100, onSlowHandler: (e) => oldEvents.push(e) } },
  );
  bus.publish('t', 'a');
  await flush();
  await macrotask();
  assert.equal(oldEvents.length, 1);

  bus.setSlowHandler(sub.id, {
    thresholdMs: 200,
    onSlowHandler: (e) => newEvents.push(e),
  });
  bus.publish('t', 'b');
  await flush();
  await macrotask();
  assert.equal(oldEvents.length, 1, 'the replaced callback never fires again');
  assert.equal(newEvents.length, 0, '150ms does not trip the loosened 200ms threshold');
  assert.equal(bus.getStats().slowHandlers.count, 1, 'no new observation was recorded');

  // Tighten again, leaving the callback out: the partial revision
  // keeps the new callback it was given above.
  bus.setSlowHandler(sub.id, { thresholdMs: 100 });
  bus.publish('t', 'c');
  await flush();
  assert.equal(newEvents.length, 1, 'the kept callback fires under the retightened threshold');
  assert.equal(newEvents[0].thresholdMs, 100);
  sub.unsubscribe();
});

test('setSlowHandler while a continuation is armed neither drops nor duplicates it', async () => {
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
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  await flush();
  assert.deepEqual(delivered, ['m1'], 'the first slow trip armed the continuation');
  assert.equal(bus.pendingCount(sub.id), 2);

  // Revise while the continuation is armed: the pending drain must
  // still run exactly once, and the new threshold governs its calls.
  bus.setSlowHandler(sub.id, { thresholdMs: 200 });
  await macrotask();
  assert.deepEqual(
    delivered,
    ['m1', 'm2', 'm3'],
    'the armed continuation drained the rest with no loss or duplication',
  );
  assert.equal(bus.getStats().slowHandlers.count, 1, 'no new trip under the loosened threshold');
  sub.unsubscribe();
});

test('setSlowHandler partial revision keeps omitted fields, and an identical revision is a no-op', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    slowHandler: { thresholdMs: 100, onSlowHandler: () => {} },
  });
  bus.setSlowHandler(sub.id, { thresholdMs: 250 });
  let audit = bus.getSubscriptionAudit({ action: 'slow_handler_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    slowHandler: { thresholdMs: 250, onSlowHandler: 'set' },
    oldSlowHandler: { thresholdMs: 100, onSlowHandler: 'set' },
  });

  bus.setSlowHandler(sub.id, { thresholdMs: 250 });
  bus.setSlowHandler(sub.id, {});
  audit = bus.getSubscriptionAudit({ action: 'slow_handler_revised' });
  assert.equal(audit.length, 1, 'identical revisions (including an empty object) audit nothing');
  sub.unsubscribe();
});

test('setSlowHandler null clears the offload, and revision after a clear is refused', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const delivered: string[] = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      delivered.push(String(msg.payload));
      clock.nowMs += 500;
    },
    { slowHandler: { thresholdMs: 100 } },
  );
  bus.setSlowHandler(sub.id, null);
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await macrotask();
  assert.deepEqual(delivered, ['a', 'b'], 'untimed delivery runs the whole drain at once');
  assert.deepEqual(bus.getStats().slowHandlers, { count: 0, lastMs: 0 });

  const audit = bus.getSubscriptionAudit({ action: 'slow_handler_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    slowHandler: null,
    oldSlowHandler: { thresholdMs: 100 },
  });
  assert.throws(() => bus.setSlowHandler(sub.id, { thresholdMs: 50 }), RangeError);
  sub.unsubscribe();
});

test('setSlowHandler is fail-closed for unknown, offload-less, or illegal values', () => {
  const bus = new EventBus();
  const watched = bus.subscribe('t', () => {}, { slowHandler: { thresholdMs: 100 } });
  const plain = bus.subscribe('u', () => {});

  const bad: Array<() => void> = [
    () => bus.setSlowHandler('', { thresholdMs: 50 }),
    () => bus.setSlowHandler('sub-nope', { thresholdMs: 50 }),
    () => bus.setSlowHandler(plain.id, { thresholdMs: 50 }),
    () => bus.setSlowHandler(watched.id, undefined as never),
    () => bus.setSlowHandler(watched.id, 'x' as never),
    () => bus.setSlowHandler(watched.id, [] as never),
    () => bus.setSlowHandler(watched.id, { thresholdMs: 0 }),
    () => bus.setSlowHandler(watched.id, { thresholdMs: -5 }),
    () => bus.setSlowHandler(watched.id, { thresholdMs: Number.NaN }),
    () => bus.setSlowHandler(watched.id, { thresholdMs: Number.POSITIVE_INFINITY }),
    () => bus.setSlowHandler(watched.id, { onSlowHandler: 'x' as never }),
  ];
  for (const call of bad) assert.throws(call, RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'slow_handler_revised' }).length,
    0,
    'no failed revision is audited',
  );

  // The live parameters survived every failed revision.
  bus.setSlowHandler(watched.id, { thresholdMs: 200 });
  assert.equal(bus.getSubscriptionAudit({ action: 'slow_handler_revised' }).length, 1);
  watched.unsubscribe();
  plain.unsubscribe();
});
