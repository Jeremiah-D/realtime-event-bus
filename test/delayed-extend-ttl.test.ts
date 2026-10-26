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

test('extendDelayedTtl extends a per-message deadline so a late schedule delivers', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Due t=3000, past its original deadline t=2000: without an extension
  // the sweep would drop it as expired, never delivered.
  const id = bus.publishDelayed('t', 'late-but-extended', {
    delayMs: 2000,
    deadlineAt: 2000,
  }) as string;
  const result = bus.extendDelayedTtl(id, { deadlineAt: 5000 });
  assert.deepEqual(result, { delayId: id, topic: 't', deadlineAt: 5000 });
  clock.nowMs = 3_000;
  bus.publish('t', 'poke'); // sweeps the due entry
  await flush();
  assert.ok(received.includes('late-but-extended'), 'extended schedule must deliver');
});

test('extendDelayedTtl lifts a schedule-time TTL deadline so a late schedule delivers', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 1000); // schedule-time deadline t=2000 for schedules made at t=1000
  const id = bus.publishDelayed('t', 'ttl-extended', { delayMs: 2000 }) as string; // due t=3000
  bus.extendDelayedTtl(id, { deadlineAt: 5000 });
  clock.nowMs = 3_000;
  bus.publish('t', 'poke'); // sweeps the due entry
  await flush();
  assert.ok(received.includes('ttl-extended'), 'TTL-extended schedule must deliver');
});

test('extendDelayedTtl extends a per-message ttlMs, monotonically', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id = bus.publishDelayed('t', 'x', { delayMs: 5000, ttlMs: 1000 }) as string;
  const result = bus.extendDelayedTtl(id, { ttlMs: 10000 });
  assert.deepEqual(result, { delayId: id, topic: 't', ttlMs: 10000 });
  // The new value is really stored: a smaller ttlMs now fails the
  // only-extend rule, where it would have passed against the old 1000.
  assert.throws(() => bus.extendDelayedTtl(id, { ttlMs: 5000 }), RangeError);
  // Extending to the same value is allowed (not a shrink).
  assert.deepEqual(bus.extendDelayedTtl(id, { ttlMs: 10000 }), {
    delayId: id,
    topic: 't',
    ttlMs: 10000,
  });
  // The due time is untouched by a TTL extension.
  assert.equal(bus.listDelayed()[0].deliverAt, 6000);
});

test('extendDelayedTtl never moves deliverAt, never re-enters the heap, never fans out by itself', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'waiting', {
    delayMs: 5000,
    deadlineAt: 2000,
  }) as string;
  clock.nowMs = 6_000; // due, but no sweep has run yet
  bus.extendDelayedTtl(id, { deadlineAt: 9000 });
  await flush();
  assert.deepEqual(received, [], 'extension alone must not fan anything out');
  assert.equal(bus.listDelayed()[0].deliverAt, 6000);
  assert.equal(bus.getDelayedStats().total, 1);
  bus.publish('t', 'poke');
  await flush();
  assert.ok(received.includes('waiting'), 'extended schedule delivers on the next sweep');
});

test('extendDelayedTtl rejects a shrink and leaves the original deadline intact', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'unshrunk', {
    delayMs: 3000,
    deadlineAt: 5000,
  }) as string; // due t=4000, deadline t=5000
  assert.throws(() => bus.extendDelayedTtl(id, { deadlineAt: 4000 }), RangeError);
  // If the failed shrink had mutated the deadline to 4000, the sweep at
  // t=4000 would drop the entry (now >= deadline); with the original
  // deadline intact it delivers.
  clock.nowMs = 4_000;
  bus.publish('t', 'poke');
  await flush();
  assert.ok(received.includes('unshrunk'), 'failed shrink must leave the original deadline');
});

test('extendDelayedTtl rejects a ttlMs shrink and leaves the original ttlMs intact', () => {
  const bus = new EventBus({ now: controllableClock().now });
  const id = bus.publishDelayed('t', 'x', { delayMs: 5000, ttlMs: 1000 }) as string;
  assert.throws(() => bus.extendDelayedTtl(id, { ttlMs: 500 }), RangeError);
  // The original ttlMs is intact: extending to 1000 (equal) still passes.
  assert.deepEqual(bus.extendDelayedTtl(id, { ttlMs: 1000 }), {
    delayId: id,
    topic: 't',
    ttlMs: 1000,
  });
});

test('extendDelayedTtl fails closed on unknown, fanned-out, and cancelled ids', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  assert.throws(() => bus.extendDelayedTtl('delayed-999', { deadlineAt: 9000 }), RangeError);
  const gone = bus.publishDelayed('t', 'due-now', { delayMs: 0, deadlineAt: 9000 }) as string;
  await flush(); // fans out 'due-now'
  assert.throws(() => bus.extendDelayedTtl(gone, { deadlineAt: 99000 }), RangeError);
  const cancelled = bus.publishDelayed('t', 'never', { delayMs: 5000, deadlineAt: 9000 }) as string;
  assert.equal(bus.cancelDelayed(cancelled), true);
  assert.throws(() => bus.extendDelayedTtl(cancelled, { deadlineAt: 99000 }), RangeError);
});

test('extendDelayedTtl rejects illegal timing and schedules with nothing to extend', () => {
  const bus = new EventBus({ now: controllableClock().now });
  const plain = bus.publishDelayed('t', 'x', { delayMs: 5000 }) as string;
  // No per-message ttlMs, no deadline, no topic TTL: nothing to extend.
  assert.throws(() => bus.extendDelayedTtl(plain, { ttlMs: 1000 }), RangeError);
  assert.throws(() => bus.extendDelayedTtl(plain, { deadlineAt: 9000 }), RangeError);
  // Exactly one of ttlMs/deadlineAt.
  assert.throws(() => bus.extendDelayedTtl(plain, {}), RangeError);
  assert.throws(() => bus.extendDelayedTtl(plain, { ttlMs: 1000, deadlineAt: 9000 }), RangeError);
  // Illegal values, before any mutation.
  const ttl = bus.publishDelayed('t', 'y', { delayMs: 5000, ttlMs: 1000 }) as string;
  assert.throws(() => bus.extendDelayedTtl(ttl, { ttlMs: -1 }), RangeError);
  assert.throws(() => bus.extendDelayedTtl(ttl, { ttlMs: 1.5 }), RangeError);
  assert.throws(() => bus.extendDelayedTtl(ttl, { deadlineAt: Number.NaN }), RangeError);
  assert.throws(() => bus.extendDelayedTtl(42 as never, { ttlMs: 1000 }), TypeError);
  // An EDF-only schedule (deadlineMs without deadlineAt) has no expiry
  // deadline to extend, so its deadlineAt form fails closed.
  const edf = bus.publishDelayed('t', 'z', { delayMs: 5000, deadlineMs: 2000 }) as string;
  assert.throws(() => bus.extendDelayedTtl(edf, { deadlineAt: 9000 }), RangeError);
});

test('extendDelayedTtl extends only the TTL deadline of an EDF schedule', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 1000); // schedule-time deadline t=2000
  // EDF-class (deadlineMs without deadlineAt): its EDF deadline never
  // expires it, but its TTL deadline would drop it at its due time.
  const id = bus.publishDelayed('t', 'edf-ttl-extended', { delayMs: 2000, deadlineMs: 2000 }) as string;
  bus.extendDelayedTtl(id, { deadlineAt: 5000 });
  clock.nowMs = 3_000;
  bus.publish('t', 'poke');
  await flush();
  assert.ok(received.includes('edf-ttl-extended'), 'EDF schedule TTL extension must deliver');
});

test('extendDelayedTtl audits success and records nothing on failure', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' }), []);
  const id = bus.publishDelayed('t', 'x', { delayMs: 5000, deadlineAt: 6000 }) as string;
  bus.extendDelayedTtl(id, { deadlineAt: 9000 });
  const events = bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' });
  assert.equal(events.length, 1);
  assert.equal(events[0].detail.delayId, id);
  assert.equal(events[0].detail.topic, 't');
  assert.equal(events[0].detail.deadlineAt, 9000);
  assert.equal(events[0].detail.oldDeadlineAt, 6000);
  assert.throws(() => bus.extendDelayedTtl(id, { deadlineAt: 8000 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_ttl_extended' }).length, 1);
});
