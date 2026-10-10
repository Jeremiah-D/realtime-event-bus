import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { AckTracker, type Delivery } from '../src/ack.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('ack confirms delivery: no redelivery after the timeout', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    'market.btc',
    (d) => {
      deliveries.push(d);
      d.ack();
    },
    { ackTimeoutMs: 30 },
  );
  bus.publish('market.btc', { price: 1 });
  await flush();
  assert.equal(deliveries.length, 1);
  assert.equal(deliveries[0].seq, 1);
  assert.equal(deliveries[0].redeliveries, 0);
  assert.equal(deliveries[0].msg.topic, 'market.btc');
  assert.deepEqual(deliveries[0].msg.payload, { price: 1 });
  assert.equal(bus.unackedCount(sub.id), 0);
  await sleep(60);
  await flush();
  assert.equal(deliveries.length, 1); // acked: nothing requeued
});

test('nack triggers immediate redelivery with bumped redeliveries', async () => {
  const bus = new EventBus();
  const seen: Array<{ seq: number; redeliveries: number }> = [];
  let nacked = false;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push({ seq: d.seq, redeliveries: d.redeliveries });
      if (!nacked) {
        nacked = true;
        d.nack();
      } else {
        d.ack();
      }
    },
    { ackTimeoutMs: 1000 },
  );
  bus.publish('t', 'x');
  await flush();
  await flush();
  assert.deepEqual(seen, [
    { seq: 1, redeliveries: 0 },
    { seq: 2, redeliveries: 1 },
  ]);
  assert.equal(bus.unackedCount(sub.id), 0);
});

test('unacked delivery is requeued automatically after ackTimeoutMs', async () => {
  const bus = new EventBus();
  const seen: Array<{ seq: number; redeliveries: number }> = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push({ seq: d.seq, redeliveries: d.redeliveries });
      if (d.seq === 2) d.ack(); // settle the redelivery so the test ends cleanly
    },
    { ackTimeoutMs: 30 },
  );
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(seen, [{ seq: 1, redeliveries: 0 }]);
  await sleep(80); // past the ack timeout: requeue + flush redelivers
  await flush();
  assert.deepEqual(seen, [
    { seq: 1, redeliveries: 0 },
    { seq: 2, redeliveries: 1 },
  ]);
});

test('double ack and late ack after timeout are no-ops', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  let first: Delivery<BusMessage> | undefined;
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.seq);
      if (d.seq === 1) {
        first = d;
        d.ack();
        d.ack(); // second ack: no-op
      } else {
        d.ack();
      }
    },
    { ackTimeoutMs: 30 },
  );
  bus.publish('t', 1);
  await flush();
  await sleep(60);
  await flush();
  assert.deepEqual(seen, [1]);
  first!.ack(); // late ack long after settle: no-op, no throw
  await flush();
  assert.deepEqual(seen, [1]);
});

test('unsubscribe cancels pending ack timers: no phantom redeliveries', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.msg.payload); // never ack
    },
    { ackTimeoutMs: 30 },
  );
  bus.publish('t', 1);
  await flush();
  assert.equal(seen.length, 1);
  sub.unsubscribe();
  await sleep(60);
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(bus.subscriberCount(), 0);
});

test('redeliveries counter increments across repeated nacks', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries < 2) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 1000 },
  );
  bus.publish('t', 1);
  await flush();
  await flush();
  await flush();
  assert.deepEqual(seen, [0, 1, 2]);
});

test('redelivered message keeps its original TTL deadline', async () => {
  const bus = new EventBus();
  bus.setTopicTtl('t', 40);
  const seen: unknown[] = [];
  bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.msg.payload); // never ack: timeout requeues it
    },
    { ackTimeoutMs: 100 },
  );
  bus.publish('t', 'v');
  await flush();
  assert.equal(seen.length, 1);
  await sleep(160); // past the TTL and past the ack timeout
  await flush();
  assert.equal(seen.length, 1); // requeued copy expired at drain: not redelivered
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('invalid ackTimeoutMs throws RangeError', () => {
  const bus = new EventBus();
  for (const bad of [0, -5, NaN, Infinity]) {
    assert.throws(() => bus.subscribeReliable('t', () => {}, { ackTimeoutMs: bad }), RangeError);
  }
  assert.throws(() => new AckTracker({ ackTimeoutMs: 0, onRedeliver: () => {} }), RangeError);
});

test('unackedCount tracks outstanding deliveries; unknown subscriber throws', async () => {
  const bus = new EventBus();
  const pending: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => pending.push(d), { ackTimeoutMs: 60000 });
  assert.equal(bus.unackedCount(sub.id), 0);
  bus.publish('t', 1);
  await flush();
  assert.equal(bus.unackedCount(sub.id), 1);
  assert.equal(bus.getStats().unackedDeliveries, 1);
  pending[0].ack();
  assert.equal(bus.unackedCount(sub.id), 0);
  assert.equal(bus.getStats().unackedDeliveries, 0);
  const plain = bus.subscribe('t', () => {});
  assert.equal(bus.unackedCount(plain.id), 0);
  assert.throws(() => bus.unackedCount('nope'), /unknown subscriber/);
});

test('AckTracker.clear cancels timers without requeueing', () => {
  let redelivered = 0;
  const tracker = new AckTracker<string>({ ackTimeoutMs: 20, onRedeliver: () => redelivered++ });
  const d = tracker.track('m', 0);
  assert.equal(tracker.unackedCount, 1);
  assert.equal(tracker.clear(), 1);
  assert.equal(tracker.unackedCount, 0);
  d.ack(); // settled by clear: no-op
  d.nack();
  assert.equal(redelivered, 0);
  assert.equal(tracker.clear(), 0);
});

test('AckTracker reports the redelivery reason to onRedeliver', async () => {
  const reasons: string[] = [];
  const tracker = new AckTracker<string>({
    ackTimeoutMs: 10,
    onRedeliver: (_msg, reason) => reasons.push(reason),
  });
  tracker.track('explicit', 0).nack();
  tracker.track('silent', 0); // never settled: the timer fires
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.deepEqual(reasons, ['nack', 'ack-timeout']);
});
