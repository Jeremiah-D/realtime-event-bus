import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('rescheduleDelayed moves a pending schedule to a later due time and it fans out there', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'payload', { delayMs: 1000 });
  bus.rescheduleDelayed(id as string, { delayMs: 5000 });
  assert.equal(bus.listDelayed()[0].deliverAt, 6000); // 1000 + 5000, bus clock
  assert.equal(bus.listDelayed()[0].delayId, id); // same id, not a new schedule
  clock.nowMs = 2000; // past the ORIGINAL due time, before the new one
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']); // the rescheduled message must NOT fire early
  clock.nowMs = 6000;
  bus.publish('t', 'poke2');
  await flush();
  // 'poke2' was queued synchronously during publish(); the sweep fans
  // 'payload' out after it (the delayed.test.ts 'poke'-first convention).
  assert.deepEqual(received, ['poke', 'poke2', 'payload']);
});

test('rescheduleDelayed to an earlier due time fans out at the new time, not the old one', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'early-bird', { delayMs: 9000 });
  bus.rescheduleDelayed(id as string, { delayMs: 1000 });
  assert.equal(bus.listDelayed()[0].deliverAt, 2000);
  clock.nowMs = 2000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'early-bird']);
});

test('rescheduleDelayed to an already-due time fans out on the next flush', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'now-please', { delayMs: 9000 });
  bus.rescheduleDelayed(id as string, { delayMs: 0 });
  await flush();
  assert.deepEqual(received, ['now-please']);
  assert.deepEqual(bus.listDelayed(), []);
});

test('rescheduleDelayed keeps topic, key, and payload, and leaves other schedules alone', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const moved = bus.publishDelayed('t', { order: 7 }, { delayMs: 1000, key: 'k1' });
  const staying = bus.publishDelayed('t', 'untouched', { delayMs: 2000 });
  bus.rescheduleDelayed(moved as string, { deliverAt: 9000 });
  const listed = bus.listDelayed();
  assert.deepEqual(
    listed.map((e) => e.delayId),
    [moved, staying],
  );
  assert.equal(listed[0].topic, 't');
  assert.equal(listed[0].key, 'k1');
  assert.equal(listed[0].deliverAt, 9000);
  assert.equal(listed[1].deliverAt, 3000); // the other schedule is untouched
  // The moved schedule is still cancellable by key after the move.
  assert.equal(bus.cancelDelayedByKey('t', 'k1').cancelled, 1);
  clock.nowMs = 9000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'untouched']);
});

test('rescheduleDelayed with an absolute deliverAt works, and a second reschedule moves it again', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000 });
  bus.rescheduleDelayed(id as string, { deliverAt: 8000 });
  assert.equal(bus.listDelayed()[0].deliverAt, 8000);
  clock.nowMs = 3000;
  bus.rescheduleDelayed(id as string, { delayMs: 1000 });
  assert.equal(bus.listDelayed()[0].deliverAt, 4000); // relative to the CURRENT clock
  assert.equal(bus.getStats().pendingDelayed, 1); // still exactly one schedule, never duplicated
});

test('rescheduleDelayed throws RangeError for unknown, fanned-out, and cancelled ids', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  assert.throws(() => bus.rescheduleDelayed('delayed-999', { delayMs: 1000 }), RangeError);
  const gone = bus.publishDelayed('t', 'due-now', { delayMs: 0 });
  await flush();
  assert.throws(() => bus.rescheduleDelayed(gone as string, { delayMs: 1000 }), RangeError);
  const cancelled = bus.publishDelayed('t', 'never', { delayMs: 5000 });
  assert.equal(bus.cancelDelayed(cancelled as string), true);
  assert.throws(() => bus.rescheduleDelayed(cancelled as string, { delayMs: 1000 }), RangeError);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('rescheduleDelayed validates timing before touching anything (fail-closed)', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000 });
  assert.throws(() => bus.rescheduleDelayed(id as string, {} as never), RangeError);
  assert.throws(
    () => bus.rescheduleDelayed(id as string, { delayMs: 1000, deliverAt: 9000 }),
    RangeError,
  );
  assert.throws(() => bus.rescheduleDelayed(id as string, { delayMs: -5 }), RangeError);
  assert.throws(() => bus.rescheduleDelayed(id as string, { deliverAt: Number.NaN }), RangeError);
  assert.throws(() => bus.rescheduleDelayed(123 as unknown as string, { delayMs: 1000 }), TypeError);
  // Every failed attempt left the original schedule exactly as it was.
  assert.deepEqual(
    bus.listDelayed().map((e) => [e.delayId, e.deliverAt]),
    [[id, 2000]],
  );
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delayed_rescheduled' }), []);
});

test('subaudit records delayed_rescheduled with delayId, topic, and both due times', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000 });
  bus.rescheduleDelayed(id as string, { delayMs: 5000 });
  const events = bus.getSubscriptionAudit({ action: 'delayed_rescheduled' });
  assert.equal(events.length, 1);
  assert.equal(events[0].actor, 'system');
  assert.equal(events[0].pattern, 't');
  assert.deepEqual(events[0].detail, {
    delayId: id,
    topic: 't',
    oldDeliverAt: 2000,
    deliverAt: 6000,
  });
});

test('a rescheduled schedule past its TTL deadline is dropped as expired at the new due time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // deadline t=1100 for schedules made at t=1000
  const id = bus.publishDelayed('t', 'doomed', { delayMs: 50 }); // originally due t=1050, inside TTL
  bus.rescheduleDelayed(id as string, { delayMs: 5000 }); // new due t=6000, past the deadline
  clock.nowMs = 6000;
  bus.publish('t', 'poke');
  await flush();
  // Rescheduling moved the due time, not the message's life: it expires, never delivers.
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().expiredMessages, 1);
});
