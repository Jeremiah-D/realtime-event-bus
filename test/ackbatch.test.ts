import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type Delivery } from '../src/bus.ts';
import { AckTracker } from '../src/ack.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('ackBatch settles an out-of-order subset in one call', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  for (let i = 0; i < 5; i++) bus.publish('t', { i });
  await flush();
  assert.equal(deliveries.length, 5);
  assert.equal(bus.unackedCount(sub.id), 5);

  // Arbitrary subset, out of order — the complement of ackUpTo's range.
  assert.equal(bus.ackBatch(sub.id, [deliveries[4], deliveries[1], deliveries[3]]), 3);
  assert.equal(bus.unackedCount(sub.id), 2);
  // The survivors keep their individual handles.
  assert.equal(bus.ackBatch(sub.id, [deliveries[0], deliveries[2]]), 2);
  assert.equal(bus.unackedCount(sub.id), 0);
  assert.equal(bus.getStats().ackBatches, 2);
});

test('ackBatch treats duplicates and stale handles as no-ops', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();

  deliveries[0].ack(); // stale before the batch runs
  assert.equal(bus.ackBatch(sub.id, [deliveries[0], deliveries[1], deliveries[1]]), 1);
  assert.equal(bus.unackedCount(sub.id), 0);
});

test('ackBatch on an empty array is a no-op', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  bus.publish('t', 'x');
  await flush();
  assert.equal(bus.ackBatch(sub.id, []), 0);
  assert.equal(bus.unackedCount(sub.id), 1);
  assert.equal(bus.getStats().ackBatches, 1, 'the call is still counted');
});

test('ackBatch settles valid handles then throws on invalid ones', async () => {
  const bus = new EventBus();
  const d1: Array<Delivery<BusMessage>> = [];
  const d2: Array<Delivery<BusMessage>> = [];
  const sub1 = bus.subscribeReliable('t', (d) => void d1.push(d), { ackTimeoutMs: 60_000 });
  const sub2 = bus.subscribeReliable('t', (d) => void d2.push(d), { ackTimeoutMs: 60_000 });
  bus.publish('t', 'x');
  bus.publish('t', 'y');
  await flush();

  // d2[0] belongs to another subscriber; null is garbage. Neither may
  // pollute the valid entries in the same batch.
  assert.throws(
    () => bus.ackBatch(sub1.id, [d1[0], d2[0], null as unknown as Delivery<BusMessage>, d1[1]]),
    /ackBatch: 2 invalid delivery handle\(s\) at index\(es\) 1, 2/,
  );
  assert.equal(bus.unackedCount(sub1.id), 0, 'valid handles still settled');
  assert.equal(bus.unackedCount(sub2.id), 2, 'foreign subscriber untouched');
  void sub2;
});

test('ackBatch validates the subscriber like ackUpTo', async () => {
  const bus = new EventBus();
  assert.equal(bus.ackBatch('nope', []), 0);
  assert.equal(bus.getStats().ackBatches, 0, 'unknown-subscriber calls are not counted');
  const plain = bus.subscribe('t', () => {});
  assert.throws(() => bus.ackBatch(plain.id, []), /not a reliable/);
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  assert.throws(
    () => bus.ackBatch(sub.id, 'nope' as unknown as Array<Delivery<BusMessage>>),
    /must be an array/,
  );
});

test('nackBatch requeues each message like an individual nack', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await flush();
  assert.equal(deliveries.length, 3);

  // Out-of-order nack: each message requeues to the tail in batch
  // order (EB-11 FIFO), keeping its per-message redelivery budget.
  assert.equal(bus.nackBatch(sub.id, [deliveries[2], deliveries[0]]), 2);
  assert.equal(bus.unackedCount(sub.id), 1);
  await flush();
  assert.equal(deliveries.length, 5);
  assert.equal(deliveries[3].redeliveries, 1);
  assert.equal(deliveries[4].redeliveries, 1);
  assert.deepEqual(deliveries[3].msg.payload, { i: 2 });
  assert.deepEqual(deliveries[4].msg.payload, { i: 0 });
  assert.equal(bus.getStats().nackBatches, 1);
});

test('nackBatch throws on invalid handles after requeueing the valid ones', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  bus.publish('t', 'x');
  bus.publish('t', 'y');
  await flush();

  assert.throws(
    () => bus.nackBatch(sub.id, [deliveries[0], undefined as unknown as Delivery<BusMessage>]),
    /nackBatch: 1 invalid delivery handle\(s\) at index\(es\) 1/,
  );
  // The valid nack still requeued: one redelivery arrives on flush.
  await flush();
  assert.equal(deliveries.length, 3);
  assert.equal(deliveries[2].redeliveries, 1);
});

test('ackBatch works when no instrumentation wraps the handles', async () => {
  // Without ackLatency/trace/SLO the consumer holds the raw tracker
  // handles directly — batch settles must recognize those too.
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable('t', (d) => void deliveries.push(d), { ackTimeoutMs: 60_000 });
  bus.publish('t', 'x');
  bus.publish('t', 'y');
  await flush();
  assert.equal(bus.ackBatch(sub.id, [deliveries[1], deliveries[0]]), 2);
  assert.equal(bus.unackedCount(sub.id), 0);
});

test('tracker-level ackBatch settles valid handles then throws on foreign ones', () => {
  const redelivered: string[] = [];
  const tracker = new AckTracker<string>({
    ackTimeoutMs: 60_000,
    onRedeliver: (msg) => void redelivered.push(msg),
  });
  const other = new AckTracker<string>({ ackTimeoutMs: 60_000, onRedeliver: () => {} });
  const h1 = tracker.track('a', 0);
  const h2 = tracker.track('b', 0);
  const foreign = other.track('x', 0);

  assert.throws(
    () => tracker.ackBatch([h1, foreign, h2]),
    /ackBatch: 1 invalid delivery handle\(s\) at index\(es\) 1/,
  );
  assert.equal(tracker.unackedCount, 0, 'valid handles settled despite the throw');
  assert.deepEqual(redelivered, [], 'acks never requeue');
  void foreign;
});

test('tracker-level nackBatch keeps per-message nack semantics', () => {
  const redelivered: Array<{ msg: string; reason: string }> = [];
  const tracker = new AckTracker<string>({
    ackTimeoutMs: 60_000,
    onRedeliver: (msg, reason) => void redelivered.push({ msg, reason }),
  });
  const h1 = tracker.track('a', 0);
  const h2 = tracker.track('b', 0);

  const settled = tracker.nackBatch([h2, h1]);
  assert.deepEqual(
    settled.map((s) => s.seq),
    [2, 1],
    'batch order, not sequence order',
  );
  assert.deepEqual(redelivered, [
    { msg: 'b', reason: 'nack' },
    { msg: 'a', reason: 'nack' },
  ]);
  assert.equal(tracker.unackedCount, 0);
});

test('tracker-level batch of only stale handles is a silent no-op', () => {
  const tracker = new AckTracker<string>({
    ackTimeoutMs: 60_000,
    onRedeliver: () => {},
  });
  const h = tracker.track('a', 0);
  h.ack();
  assert.deepEqual(tracker.ackBatch([h]), []);
  assert.deepEqual(tracker.ackBatch([]), []);
});
