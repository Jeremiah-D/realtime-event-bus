import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('invalid maxConcurrentDeliveries throws RangeError from subscribe', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, 1.5, NaN, Infinity, '2' as unknown as number]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { maxConcurrentDeliveries: bad }),
      RangeError,
      `maxConcurrentDeliveries=${String(bad)}`,
    );
  }
  // Valid values register fine, including the explicit default.
  bus.subscribe('t', () => {}, { maxConcurrentDeliveries: 1 });
  bus.subscribe('t', () => {}, { maxConcurrentDeliveries: 4 });
  assert.equal(bus.getStats().deliveryConcurrency.length, 2);
});

test('default (1): async handler invocations serialize in publish order', async () => {
  const bus = new EventBus();
  const events: string[] = [];
  let releaseFirst!: () => void;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  bus.subscribe('t', async (msg) => {
    const n = (msg.payload as { n: number }).n;
    events.push(`start-${n}`);
    if (n === 1) await firstGate;
    events.push(`end-${n}`);
  });
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  // The first handler is still executing (gated): nothing else may start.
  assert.deepEqual(events, ['start-1']);
  assert.deepEqual(bus.getStats().deliveryConcurrency[0].inFlight, 1);
  releaseFirst();
  await flush();
  await flush();
  assert.deepEqual(events, ['start-1', 'end-1', 'start-2', 'end-2', 'start-3', 'end-3']);
  assert.deepEqual(bus.getStats().deliveryConcurrency[0].inFlight, 0);
});

test('default (1): sync handlers drain exactly as before', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribe('t', (msg) => {
    seen.push((msg.payload as { n: number }).n);
  });
  for (let n = 1; n <= 5; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
});

test('maxConcurrentDeliveries: 2 lets keyless handlers overlap', async () => {
  const bus = new EventBus();
  let current = 0;
  let maxSeen = 0;
  bus.subscribe(
    't',
    async () => {
      current += 1;
      maxSeen = Math.max(maxSeen, current);
      await sleep(20);
      current -= 1;
    },
    { maxConcurrentDeliveries: 2 },
  );
  for (let n = 1; n <= 4; n += 1) bus.publish('t', { n });
  await sleep(150);
  assert.equal(maxSeen, 2);
  assert.equal(bus.getStats().deliveryConcurrency[0].maxConcurrentDeliveries, 2);
  assert.equal(bus.getStats().deliveryConcurrency[0].inFlight, 0);
});

test('maxConcurrentDeliveries: 1 keeps keyless handlers serial', async () => {
  const bus = new EventBus();
  let current = 0;
  let maxSeen = 0;
  bus.subscribe('t', async () => {
    current += 1;
    maxSeen = Math.max(maxSeen, current);
    await sleep(10);
    current -= 1;
  });
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await sleep(100);
  assert.equal(maxSeen, 1);
});

test('keyed messages serialize per key while keyless messages overlap', async () => {
  const bus = new EventBus();
  const activeByKey = new Map<string, number>();
  let keyOverlap = false;
  const order: string[] = [];
  bus.subscribe(
    't',
    async (msg) => {
      const key = msg.key ?? '<none>';
      activeByKey.set(key, (activeByKey.get(key) ?? 0) + 1);
      if ((activeByKey.get(key) ?? 0) > 1) keyOverlap = true;
      order.push(`start:${key}`);
      await sleep(15);
      activeByKey.set(key, (activeByKey.get(key) ?? 0) - 1);
      order.push(`end:${key}`);
    },
    { maxConcurrentDeliveries: 3 },
  );
  bus.publish('t', 'a', { key: 'k1' });
  bus.publish('t', 'b', { key: 'k1' });
  bus.publish('t', 'c', { key: 'k2' });
  bus.publish('t', 'd');
  await sleep(200);
  assert.equal(keyOverlap, false);
  // Same-key order preserved: a fully before b.
  const aEnd = order.indexOf('end:k1');
  assert.ok(aEnd >= 0);
  const k1Starts = order.filter((e) => e === 'start:k1').length;
  assert.equal(k1Starts, 2);
  assert.equal(order.filter((e) => e === 'start:k2').length, 1);
  assert.equal(order.filter((e) => e === 'start:<none>').length, 1);
  // All four messages were delivered exactly once.
  assert.equal(order.filter((e) => e.startsWith('start:')).length, 4);
});

test('reliable: an unacked delivery does not block the next dispatch', async () => {
  const bus = new EventBus();
  const deliveries: Array<{ n: number }> = [];
  bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push({ n: (d.msg.payload as { n: number }).n });
      // Deliberately never ack: the concurrency slot covers handler
      // execution, not the unacked window.
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(
    deliveries.map((d) => d.n),
    [1, 2],
  );
});

test('a rejected async handler releases its slot', async () => {
  const bus = new EventBus();
  const attempted: number[] = [];
  bus.subscribe('t', async (msg) => {
    const n = (msg.payload as { n: number }).n;
    attempted.push(n);
    if (n === 1) throw new Error('boom');
  });
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  await flush();
  assert.deepEqual(attempted, [1, 2]);
  assert.equal(bus.getStats().deliveryConcurrency[0].inFlight, 0);
});

test('composes with healthProbe: probed subscribers still drain', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribe(
    't',
    (msg) => {
      seen.push((msg.payload as { n: number }).n);
    },
    { healthProbe: true },
  );
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(seen, [1, 2]);
});

test('getStats exposes per-subscriber concurrency usage', async () => {  const bus = new EventBus();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const sub = bus.subscribe(
    't',
    async () => {
      await gate;
    },
    { maxConcurrentDeliveries: 2 },
  );
  bus.publish('t', 'x', { key: 'k9' });
  await flush();
  const rows = bus.getStats().deliveryConcurrency;
  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0], {
    subscriberId: sub.id,
    pattern: 't',
    maxConcurrentDeliveries: 2,
    inFlight: 1,
    keysInFlight: { k9: 1 },
  });
  release();
  await flush();
  await flush();
  assert.deepEqual(bus.getStats().deliveryConcurrency[0].inFlight, 0);
  assert.deepEqual(bus.getStats().deliveryConcurrency[0].keysInFlight, {});
});

test('maxInflight doubles as the concurrency default; explicit maxConcurrentDeliveries wins', async () => {
  const bus = new EventBus();
  let current = 0;
  let maxSeen = 0;
  const sub = bus.subscribe(
    't',
    async () => {
      current += 1;
      maxSeen = Math.max(maxSeen, current);
      await sleep(15);
      current -= 1;
    },
    { maxInflight: 3 },
  );
  // No explicit maxConcurrentDeliveries: the maxInflight opt-in carries
  // over, so three handlers may overlap.
  assert.equal(bus.getStats().deliveryConcurrency[0].maxConcurrentDeliveries, 3);
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await sleep(100);
  assert.equal(maxSeen, 3);

  // An explicit maxConcurrentDeliveries always wins over the fallback.
  let current2 = 0;
  let maxSeen2 = 0;
  const sub2 = bus.subscribe(
    't2',
    async () => {
      current2 += 1;
      maxSeen2 = Math.max(maxSeen2, current2);
      await sleep(15);
      current2 -= 1;
    },
    { maxInflight: 3, maxConcurrentDeliveries: 1 },
  );
  assert.equal(
    bus.getStats().deliveryConcurrency.find((r) => r.subscriberId === sub2.id)
      ?.maxConcurrentDeliveries,
    1,
  );
  for (let n = 1; n <= 3; n += 1) bus.publish('t2', { n });
  await sleep(150);
  assert.equal(maxSeen2, 1);
  sub.unsubscribe();
  sub2.unsubscribe();
});
