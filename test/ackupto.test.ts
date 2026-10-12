import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type Delivery } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('ackUpTo acks the whole outstanding range at once', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  for (let i = 0; i < 5; i++) bus.publish('t', { i });
  await flush();
  assert.equal(deliveries.length, 5);
  assert.equal(bus.unackedCount(sub.id), 5);

  assert.equal(bus.ackUpTo(sub.id, 3), 3);
  assert.equal(bus.unackedCount(sub.id), 2);
  // The remaining deliveries keep their individual handles: acking one
  // more by hand works, and the range below it is empty.
  deliveries[3].ack();
  assert.equal(bus.ackUpTo(sub.id, 5), 1);
  assert.equal(bus.unackedCount(sub.id), 0);
  assert.equal(bus.getStats().ackedRanges, 2);
});

test('ackUpTo skips non-outstanding seqs silently', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await flush();
  // Seq 2 acked individually first: the range has a hole.
  deliveries[1].ack();
  assert.equal(bus.ackUpTo(sub.id, 3), 2, 'seqs 1 and 3 acked, seq 2 skipped');
  assert.equal(bus.unackedCount(sub.id), 0);
  // Repeating the same range acks nothing new — no error.
  assert.equal(bus.ackUpTo(sub.id, 3), 0);
});

test('ackUpTo on an unknown subscriber returns 0', () => {
  const bus = new EventBus();
  assert.equal(bus.ackUpTo('nope', 5), 0);
  assert.equal(bus.getStats().ackedRanges, 0, 'unknown-subscriber calls are not counted');
});

test('ackUpTo rejects bad seqs and non-reliable subscribers', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  const plain = bus.subscribe('t', () => {});
  bus.publish('t', 'x');
  await flush();
  assert.equal(deliveries.length, 1);

  assert.throws(() => bus.ackUpTo(sub.id, 0), RangeError);
  assert.throws(() => bus.ackUpTo(sub.id, -2), RangeError);
  assert.throws(() => bus.ackUpTo(sub.id, 1.5), RangeError);
  assert.throws(() => bus.ackUpTo(sub.id, 2), RangeError, 'seq 2 was never tracked');
  assert.throws(() => bus.ackUpTo(plain.id, 1), RangeError, 'not a reliable subscriber');
  assert.equal(bus.unackedCount(sub.id), 1, 'failed calls change nothing');
});

test('a nacked redelivery gets a fresh seq outside the old range', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  bus.publish('t', 'x');
  await flush();
  assert.equal(deliveries[0].seq, 1);
  deliveries[0].nack();
  await flush();
  await flush();
  assert.equal(deliveries.length, 2);
  assert.equal(deliveries[1].seq, 2, 'redelivery carries a new delivery seq');
  assert.equal(deliveries[1].redeliveries, 1);
  // The old range is empty now: the live delivery sits at seq 2.
  assert.equal(bus.ackUpTo(sub.id, 1), 0);
  assert.equal(bus.ackUpTo(sub.id, 2), 1);
  assert.equal(bus.unackedCount(sub.id), 0);
});

test('ackUpTo skips messages already moved to the dead-letter queue (EB-28)', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), {
    ackTimeoutMs: 60_000,
    deadLetter: { maxRedeliveries: 1 },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await flush();
  // Exhaust the first message's budget: two nacks move it to the DLQ.
  deliveries[0].nack();
  await flush();
  await flush();
  deliveries[3].nack(); // the redelivery of message 0 (seq 4)
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // Seqs 1 and 4 are gone (nacked / dead-lettered); 2 and 3 ack.
  assert.equal(bus.ackUpTo(sub.id, 4), 2);
  assert.equal(bus.unackedCount(sub.id), 0);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'DLQ entry untouched');
});

test('ackUpTo contributes a single ack-latency sample at the last ack moment (EB-40)', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), {
    ackTimeoutMs: 60_000,
    ackLatency: true,
  });
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await flush();
  clock.nowMs = 1_100;
  assert.equal(bus.ackUpTo(sub.id, 3), 3);
  const stats = bus.getStats().ackLatency.find((s) => s.subscriberId === sub.id);
  assert.ok(stats !== undefined);
  assert.equal(stats.samples, 1, 'one sample for the whole range');
  assert.equal(stats.p50Ms, 100, 'last message: accepted at 1000, acked at 1100');
});

test('ackUpTo releases maxInflight slots through the uniform settle path (EB-67)', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), {
    ackTimeoutMs: 60_000,
    maxInflight: 2,
  });
  for (let i = 0; i < 5; i++) bus.publish('t', { i });
  await flush();
  assert.equal(deliveries.length, 2, 'in-flight cap holds back the rest');
  assert.equal(bus.ackUpTo(sub.id, 2), 2);
  await flush();
  assert.equal(deliveries.length, 4, 'settled slots pull the next messages in');
  assert.equal(bus.unackedCount(sub.id), 2);
});
