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

test('extendAckDeadlines by extensionMs keeps several slow deliveries outstanding past their original timeout', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.seq > 2) d.ack(); // settle the eventual redeliveries so the test ends cleanly
    },
    { ackTimeoutMs: 40 },
  );
  bus.publish('t', 'slow-one');
  bus.publish('t', 'slow-two');
  await flush();
  assert.equal(deliveries.length, 2);
  assert.equal(bus.unackedCount(sub.id), 2);
  await sleep(10);
  assert.deepEqual(bus.extendAckDeadlines(sub.id, [
    { seq: 1, extensionMs: 300 },
    { seq: 2, extensionMs: 300 },
  ]), { extended: 2, seqs: [1, 2] });
  await sleep(100); // past the original 40 ms timeout, inside the extension
  await flush();
  assert.equal(deliveries.length, 2, 'extended deliveries must not be requeued yet');
  assert.equal(bus.unackedCount(sub.id), 2);
  assert.deepEqual(
    deliveries.map((d) => d.redeliveries),
    [0, 0],
    'extending must not bump the redelivery counts',
  );
  assert.deepEqual(
    deliveries.map((d) => d.seq),
    [1, 2],
    'extending must not change the sequences',
  );
  const events = bus.getSubscriptionAudit({ action: 'ack_deadline_extended' });
  assert.equal(events.length, 2, 'each extended entry is audited separately');
  assert.deepEqual(
    events.map((e) => e.detail.seq),
    [1, 2],
  );
  await sleep(300); // past the extended deadline: both timeout requeues finally fire
  await flush();
  assert.equal(deliveries.length, 4, 'the extended timers still requeue once they expire');
  assert.deepEqual(
    deliveries.slice(2).map((d) => d.redeliveries),
    [1, 1],
  );
});

test('extendAckDeadlines mixes extensionMs and newDeadlineMs, priced once against the bus clock', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.seq > 2) d.ack();
    },
    { ackTimeoutMs: 40 },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(bus.extendAckDeadlines(sub.id, [
    { seq: 1, extensionMs: 500 },
    { seq: 2, newDeadlineMs: 2000 },
  ]), { extended: 2, seqs: [1, 2] });
  const events = bus.getSubscriptionAudit({ action: 'ack_deadline_extended' });
  assert.equal(events.length, 2);
  assert.deepEqual(events[0].detail, { seq: 1, extensionMs: 500, deadlineMs: 1500 });
  assert.deepEqual(events[1].detail, { seq: 2, extensionMs: 1000, deadlineMs: 2000 });
  await sleep(100); // past the original 40 ms timeout, inside both extensions
  await flush();
  assert.equal(deliveries.length, 2, 'neither mixed-form delivery may be requeued yet');
  assert.equal(bus.unackedCount(sub.id), 2);
});

test('extendAckDeadlines leaves the subscription ackTimeoutMs unchanged for later deliveries', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.seq > 2) d.ack();
    },
    { ackTimeoutMs: 40 },
  );
  bus.publish('t', 'slow');
  await flush();
  assert.equal(bus.extendAckDeadlines(sub.id, [{ seq: 1, extensionMs: 1000 }]).extended, 1);
  bus.publish('t', 'fresh');
  await flush();
  await sleep(100); // past the fresh delivery's original 40 ms timeout, inside the extension
  await flush();
  assert.deepEqual(
    deliveries.map((d) => d.seq),
    [1, 2, 3],
    'the fresh delivery requeues on its original timeout while the extended one stays outstanding',
  );
  assert.equal(bus.unackedCount(sub.id), 1, 'only the extended delivery is still outstanding: the fresh redelivery was acked by the handler');
  deliveries[0].ack();
});

test('extendAckDeadlines rejects a batch with one already-acked entry fail-closed', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.seq > 2) d.ack();
    },
    { ackTimeoutMs: 60 },
  );
  bus.publish('t', 'x');
  bus.publish('t', 'y');
  await flush();
  deliveries[0].ack();
  assert.throws(
    () =>
      bus.extendAckDeadlines(sub.id, [
        { seq: 1, extensionMs: 500 },
        { seq: 2, extensionMs: 500 },
      ]),
    RangeError,
  );
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }), []);
  // The good entry was not extended either: it still times out on its original deadline.
  await sleep(80);
  await flush();
  assert.deepEqual(
    deliveries.map((d) => d.seq),
    [1, 2, 3],
    'the untouched delivery requeues at its original timeout, not the extended one',
  );
});

test('extendAckDeadlines rejects a timed-out delivery but accepts its live redelivery in a fresh batch', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.seq > 2) d.ack();
    },
    { ackTimeoutMs: 60 },
  );
  bus.publish('t', 'x');
  await flush();
  await sleep(80); // the original delivery times out and requeues
  await flush();
  assert.deepEqual(
    deliveries.map((d) => d.seq),
    [1, 2],
  );
  assert.throws(
    () => bus.extendAckDeadlines(sub.id, [{ seq: 1, extensionMs: 500 }]),
    RangeError,
  );
  assert.equal(bus.extendAckDeadlines(sub.id, [{ seq: 2, extensionMs: 500 }]).extended, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }).length, 1);
  deliveries[1].ack();
});

test('extendAckDeadlines rejects unknown seqs, unknown and non-reliable subscribers, and duplicates', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  bus.publish('t', 'x');
  await flush();
  assert.throws(
    () => bus.extendAckDeadlines(sub.id, [{ seq: 99, extensionMs: 500 }]),
    RangeError,
  );
  assert.throws(
    () => bus.extendAckDeadlines('no-such-sub', [{ seq: 1, extensionMs: 500 }]),
    RangeError,
  );
  const plain = bus.subscribe('plain', () => {});
  assert.throws(
    () => bus.extendAckDeadlines(plain.id, [{ seq: 1, extensionMs: 500 }]),
    RangeError,
  );
  assert.throws(
    () =>
      bus.extendAckDeadlines(sub.id, [
        { seq: 1, extensionMs: 500 },
        { seq: 1, extensionMs: 500 },
      ]),
    RangeError,
  );
  assert.throws(() => bus.extendAckDeadlines('', [{ seq: 1, extensionMs: 500 }]), RangeError);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }), []);
  assert.equal(bus.unackedCount(sub.id), 1, 'no rejected batch touched the outstanding delivery');
});

test('extendAckDeadlines rejects malformed entries before touching any timer', async () => {
  const bus = new EventBus();
  const clock = controllableClock();
  const clockBus = new EventBus({ now: clock.now });
  const sub = bus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  const clockSub = clockBus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  bus.publish('t', 'x');
  bus.publish('t', 'y');
  clockBus.publish('t', 'x');
  await flush();
  assert.throws(() => bus.extendAckDeadlines(sub.id, 'nope' as never), TypeError);
  assert.throws(() => bus.extendAckDeadlines(sub.id, [null as never]), TypeError);
  assert.throws(
    () => bus.extendAckDeadlines(sub.id, [{ seq: 1 } as never]),
    RangeError,
  );
  assert.throws(
    () => bus.extendAckDeadlines(sub.id, [{ seq: 1, extensionMs: 500, newDeadlineMs: 2000 }]),
    RangeError,
  );
  assert.throws(
    () => bus.extendAckDeadlines(sub.id, [{ seq: 1, extensionMs: 0 }]),
    RangeError,
  );
  assert.throws(
    () => bus.extendAckDeadlines(sub.id, [{ seq: 1, extensionMs: -5 }]),
    RangeError,
  );
  assert.throws(
    () => bus.extendAckDeadlines(sub.id, [{ seq: 1, extensionMs: Number.NaN }]),
    RangeError,
  );
  assert.throws(
    () => bus.extendAckDeadlines(clockSub.id, [{ seq: 1, newDeadlineMs: 1000 }]),
    RangeError,
  );
  assert.throws(
    () => bus.extendAckDeadlines(clockSub.id, [{ seq: 1, newDeadlineMs: 999 }]),
    RangeError,
  );
  assert.throws(
    () => bus.extendAckDeadlines(sub.id, [{ seq: 0, extensionMs: 500 }]),
    RangeError,
  );
  // A malformed entry after a good one still rejects the whole batch.
  assert.throws(
    () =>
      bus.extendAckDeadlines(sub.id, [
        { seq: 1, extensionMs: 500 },
        { seq: 2, extensionMs: 0 },
      ]),
    RangeError,
  );
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }), []);
  assert.deepEqual(clockBus.getSubscriptionAudit({ action: 'ack_deadline_extended' }), []);
  assert.equal(bus.unackedCount(sub.id), 2);
  assert.equal(clockBus.unackedCount(clockSub.id), 1);
});

test('extendAckDeadlines with an empty batch is a no-op', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  assert.deepEqual(bus.extendAckDeadlines(sub.id, []), { extended: 0, seqs: [] });
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_deadline_extended' }), []);
});
