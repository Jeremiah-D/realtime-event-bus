import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import type { Delivery } from '../src/ack.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('extendAckDeadline by extensionMs keeps a slow delivery outstanding past its original timeout', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.seq === 2) d.ack(); // settle the eventual redelivery so the test ends cleanly
    },
    { ackTimeoutMs: 40 },
  );
  bus.publish('t', 'slow');
  await flush();
  assert.equal(deliveries.length, 1);
  assert.equal(bus.unackedCount(sub.id), 1);
  await sleep(10);
  assert.equal(bus.extendAckDeadline(sub.id, 1, { extensionMs: 300 }), true);
  await sleep(100); // past the original 40 ms timeout, inside the extension
  await flush();
  assert.equal(deliveries.length, 1, 'extended delivery must not be requeued yet');
  assert.equal(bus.unackedCount(sub.id), 1);
  assert.equal(deliveries[0].redeliveries, 0, 'extending must not bump the redelivery count');
  assert.equal(deliveries[0].seq, 1, 'extending must not change the sequence');
  await sleep(300); // past the extended deadline: the timeout requeue finally fires
  await flush();
  assert.equal(deliveries.length, 2, 'the extended timer still requeues once it expires');
  assert.equal(deliveries[1].redeliveries, 1);
});

test('extendAckDeadline by newDeadlineMs uses the bus clock for the new deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.seq === 2) d.ack();
    },
    { ackTimeoutMs: 40 },
  );
  bus.publish('t', 'slow');
  await flush();
  assert.equal(bus.extendAckDeadline(sub.id, 1, { newDeadlineMs: 1500 }), true);
  const events = bus.getSubscriptionAudit({ action: 'ack_deadline_extended' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, { seq: 1, extensionMs: 500, deadlineMs: 1500 });
  await sleep(100); // past the original 40 ms timeout, inside the 500 ms extension
  await flush();
  assert.equal(deliveries.length, 1, 'deadline-extended delivery must not be requeued yet');
  assert.equal(bus.unackedCount(sub.id), 1);
});

test('extendAckDeadline rejects an already-acked delivery fail-closed', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => deliveries.push(d), { ackTimeoutMs: 1000 });
  bus.publish('t', 'x');
  await flush();
  deliveries[0].ack();
  assert.equal(bus.unackedCount(sub.id), 0);
  assert.throws(() => bus.extendAckDeadline(sub.id, 1, { extensionMs: 500 }), RangeError);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }), []);
});

test('extendAckDeadline rejects a timed-out delivery but accepts its live redelivery', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => deliveries.push(d), { ackTimeoutMs: 60 });
  bus.publish('t', 'x');
  await flush();
  await sleep(80); // the original delivery times out and requeues
  await flush();
  assert.deepEqual(
    deliveries.map((d) => d.seq),
    [1, 2],
  );
  assert.throws(() => bus.extendAckDeadline(sub.id, 1, { extensionMs: 500 }), RangeError);
  assert.equal(bus.extendAckDeadline(sub.id, 2, { extensionMs: 500 }), true);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }).length, 1);
});

test('extendAckDeadline rejects unknown seqs, unknown subscribers, and non-reliable subscribers', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  bus.publish('t', 'x');
  await flush();
  assert.throws(() => bus.extendAckDeadline(sub.id, 99, { extensionMs: 500 }), RangeError);
  assert.throws(() => bus.extendAckDeadline('no-such-sub', 1, { extensionMs: 500 }), RangeError);
  const plain = bus.subscribe('plain', () => {});
  assert.throws(() => bus.extendAckDeadline(plain.id, 1, { extensionMs: 500 }), RangeError);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }), []);
});

test('extendAckDeadline rejects malformed options before touching any timer', async () => {
  const bus = new EventBus();
  const clock = controllableClock();
  const clockBus = new EventBus({ now: clock.now });
  const sub = bus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  const clockSub = clockBus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  bus.publish('t', 'x');
  clockBus.publish('t', 'x');
  await flush();
  assert.throws(() => bus.extendAckDeadline(sub.id, 1, {} as never), RangeError);
  assert.throws(
    () => bus.extendAckDeadline(sub.id, 1, { extensionMs: 500, newDeadlineMs: 2000 }),
    RangeError,
  );
  assert.throws(() => bus.extendAckDeadline(sub.id, 1, { extensionMs: 0 }), RangeError);
  assert.throws(() => bus.extendAckDeadline(sub.id, 1, { extensionMs: -5 }), RangeError);
  assert.throws(() => bus.extendAckDeadline(sub.id, 1, { extensionMs: Number.NaN }), RangeError);
  assert.throws(() => bus.extendAckDeadline(clockSub.id, 1, { newDeadlineMs: 1000 }), RangeError);
  assert.throws(() => bus.extendAckDeadline(clockSub.id, 1, { newDeadlineMs: 999 }), RangeError);
  assert.throws(() => bus.extendAckDeadline(sub.id, 0, { extensionMs: 500 }), RangeError);
  // Nothing above extended anything: both deliveries are still outstanding on their original timers.
  assert.equal(bus.unackedCount(sub.id), 1);
  assert.equal(clockBus.unackedCount(clockSub.id), 1);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }), []);
});
