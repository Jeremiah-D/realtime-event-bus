import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { type Delivery } from '../src/ack.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const nOf = (msg: BusMessage): number => (msg.payload as { n: number }).n;

test('maxInflight caps concurrent async handler invocations (plain)', async () => {
  const bus = new EventBus();
  let inFlight = 0;
  let maxObserved = 0;
  const seen: number[] = [];
  const releases: Array<() => void> = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      inFlight += 1;
      maxObserved = Math.max(maxObserved, inFlight);
      seen.push(nOf(msg));
      return new Promise<void>((resolve) => {
        releases.push(() => {
          inFlight -= 1;
          resolve();
        });
      });
    },
    { maxInflight: 2 },
  );
  for (let n = 1; n <= 5; n += 1) bus.publish('t', { n });
  await flush();
  // Only two handler invocations opened; the rest wait queued, in order.
  assert.deepEqual(seen, [1, 2]);
  assert.equal(maxObserved, 2);
  assert.equal(inFlight, 2);
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 2, inFlight: 2 },
  ]);
  // Settling one invocation pulls exactly one more message into the slot.
  releases[0]();
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  assert.equal(maxObserved, 2);
  assert.equal(inFlight, 2);
  // Settling the rest drains the backlog fully, in FIFO order.
  for (const release of releases.splice(0)) release();
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  for (const release of releases.splice(0)) release();
  await flush();
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);
  sub.unsubscribe();
});

test('plain: a rejected handler promise releases its slot', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  let calls = 0;
  bus.subscribe(
    't',
    (msg) => {
      calls += 1;
      seen.push(nOf(msg));
      return calls === 1 ? Promise.reject(new Error('boom')) : Promise.resolve();
    },
    { maxInflight: 1 },
  );
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  await flush();
  // The rejection freed the slot — no leak, no stall, FIFO preserved.
  assert.deepEqual(seen, [1, 2]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);
});

test('reliable: slots release on ack and queued messages are pulled after settle', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      seen.push(nOf(d.msg));
    },
    { maxInflight: 2, ackTimeoutMs: 5000 },
  );
  for (let n = 1; n <= 5; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(seen, [1, 2]);
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 2, inFlight: 2 },
  ]);
  // Ack out of order: the second delivery settles first — the next queued
  // message is still pulled in FIFO order.
  deliveries[1].ack();
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  deliveries[0].ack();
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4]);
  for (const d of deliveries) d.ack();
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  deliveries[deliveries.length - 1].ack();
  await flush();
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);
  sub.unsubscribe();
});

test('reliable: nack releases the slot and the message is redelivered', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  let calls = 0;
  bus.subscribeReliable(
    't',
    (d) => {
      calls += 1;
      seen.push(nOf(d.msg));
      if (calls === 1) d.nack();
      else d.ack();
    },
    { maxInflight: 1, ackTimeoutMs: 5000 },
  );
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  await flush();
  await flush();
  // n=1 was nacked (slot released, so m2 flowed without waiting for a
  // timeout); the nacked message is requeued to the tail per the existing
  // at-least-once redelivery semantics and delivered again afterwards.
  assert.deepEqual(seen, [1, 2, 1]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);
});

test('reliable: ack timeout releases the slot', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(nOf(d.msg));
      // Never settles: the ack timeout must free the slot.
    },
    { maxInflight: 1, ackTimeoutMs: 50 },
  );
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(seen, [1]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 1);
  // Long enough for m1's timeout but not m2's: no redelivery cascade.
  await sleep(70);
  await flush();
  // The timeout settled m1 (slot released) and requeued it behind m2.
  assert.deepEqual(seen, [1, 2]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 1);
  sub.unsubscribe();
});

test('composes orthogonally with deliveryShaping', async () => {
  // The cap is the tighter bound: shaping allows 10 per round, the cap 1.
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
    },
    { maxInflight: 1, deliveryShaping: { messagesPerSec: 1000, burst: 10 }, ackTimeoutMs: 5000 },
  );
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  assert.equal(deliveries.length, 1);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 1);
  deliveries[0].ack();
  await flush();
  assert.equal(deliveries.length, 2);

  // Shaping is the tighter bound: burst 1, cap 5.
  const bus2 = new EventBus();
  const seen: number[] = [];
  bus2.subscribe(
    's',
    (msg) => {
      seen.push(nOf(msg));
    },
    { maxInflight: 5, deliveryShaping: { messagesPerSec: 10, burst: 1 } },
  );
  for (let n = 1; n <= 3; n += 1) bus2.publish('s', { n });
  await flush();
  assert.deepEqual(seen, [1]);
  // The shaping re-flush timer still drains the backlog past the cap.
  await sleep(150);
  await flush();
  assert.deepEqual(seen, [1, 2]);
});

test('inFlightUsage reports cap and live usage per opted-in subscriber', () => {
  const bus = new EventBus();
  const a = bus.subscribe('a', () => {}, { maxInflight: 3 });
  bus.subscribe('b', () => {}); // no cap: absent from inFlightUsage
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: a.id, pattern: 'a', maxInflight: 3, inFlight: 0 },
  ]);
  a.unsubscribe();
});

test('maxInflight rejects illegal values and incoherent combinations', () => {
  const bus = new EventBus();
  const noop = (): void => {};
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => bus.subscribe('t', noop, { maxInflight: bad }), RangeError);
  }
  // subscribeReliable funnels through the same validation.
  assert.throws(
    () => bus.subscribeReliable('t', () => {}, { maxInflight: 0, ackTimeoutMs: 1000 }),
    RangeError,
  );
  assert.throws(() => bus.subscribe('t', noop, { maxInflight: 2, batch: true }), RangeError);
  assert.throws(() => bus.subscribe('t', noop, { maxInflight: 2, shadow: true }), RangeError);
  // A rejected option leaves no half-registered subscriber behind.
  assert.deepEqual(bus.getStats().inFlightUsage, []);
});

test('default off: no cap without the option', async () => {
  const bus = new EventBus();
  let inFlight = 0;
  let maxObserved = 0;
  const seen: number[] = [];
  const resolvers: Array<() => void> = [];
  bus.subscribe('t', (msg) => {
    inFlight += 1;
    maxObserved = Math.max(maxObserved, inFlight);
    seen.push(nOf(msg));
    return new Promise<void>((resolve) => {
      resolvers.push(() => {
        inFlight -= 1;
        resolve();
      });
    });
  });
  for (let n = 1; n <= 5; n += 1) bus.publish('t', { n });
  await flush();
  // Without the option every queued message is pulled in one round —
  // concurrency is unbounded, exactly as before.
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  assert.equal(maxObserved, 5);
  assert.deepEqual(bus.getStats().inFlightUsage, []);
  for (const r of resolvers.splice(0)) r();
  await flush();
  assert.equal(inFlight, 0);
});
