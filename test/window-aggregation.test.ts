import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type WindowAggregateResult } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('tumbling window aggregates a burst into one message: sum', async () => {
  const bus = new EventBus();
  const received: WindowAggregateResult[] = [];
  bus.subscribe('prices', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 40, aggregate: 'sum', field: 'price' },
  });
  for (let i = 1; i <= 5; i++) bus.publish('prices', { price: i * 10 });
  await flush();
  // Raw messages never reach the handler: the burst is still buffering.
  assert.equal(received.length, 0);
  await sleep(120);
  await flush();
  assert.equal(received.length, 1);
  const agg = received[0];
  assert.equal(agg.count, 5);
  assert.equal(agg.value, 150);
  assert.equal(agg.windowEnd - agg.windowStart, 40);
  assert.equal(bus.getStats().aggregatedWindows, 1);
});

test('every aggregate kind folds correctly', async () => {
  const cases: Array<{
    aggregate: 'count' | 'sum' | 'avg' | 'min' | 'max' | 'last' | 'concat';
    field?: string;
    payloads: unknown[];
    expected: unknown;
  }> = [
    { aggregate: 'count', payloads: ['a', 'b', 'c'], expected: 3 },
    { aggregate: 'sum', field: 'v', payloads: [{ v: 1 }, { v: 2 }, { v: 3 }], expected: 6 },
    { aggregate: 'avg', field: 'v', payloads: [{ v: 2 }, { v: 4 }], expected: 3 },
    { aggregate: 'min', field: 'v', payloads: [{ v: 5 }, { v: 1 }, { v: 9 }], expected: 1 },
    { aggregate: 'max', field: 'v', payloads: [{ v: 5 }, { v: 1 }, { v: 9 }], expected: 9 },
    { aggregate: 'last', field: 'v', payloads: [{ v: 'x' }, { v: 'y' }], expected: 'y' },
    { aggregate: 'concat', payloads: ['a', 'b', 'c'], expected: 'abc' },
    { aggregate: 'concat', field: 'v', payloads: [{ v: 1 }, { v: 2 }], expected: '12' },
  ];
  for (const c of cases) {
    const bus = new EventBus();
    const received: WindowAggregateResult[] = [];
    bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
      window: { windowMs: 20, aggregate: c.aggregate, ...(c.field ? { field: c.field } : {}) },
    });
    for (const p of c.payloads) bus.publish('t', p);
    await sleep(80);
    await flush();
    assert.equal(received.length, 1, `${c.aggregate}: one aggregate`);
    assert.equal(received[0].count, c.payloads.length, `${c.aggregate}: count`);
    assert.deepEqual(received[0].value, c.expected, `${c.aggregate}: value`);
  }
});

test('numeric aggregates ignore non-numeric messages; empty numeric window delivers nothing', async () => {
  const bus = new EventBus();
  const received: WindowAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 30, aggregate: 'sum', field: 'v' },
  });
  bus.publish('t', { v: 10 });
  bus.publish('t', { v: 'not-a-number' });
  bus.publish('t', { v: NaN });
  bus.publish('t', {});
  bus.publish('t', { v: 20 });
  await sleep(100);
  await flush();
  assert.equal(received.length, 1);
  // Only the two finite numbers contributed.
  assert.equal(received[0].count, 2);
  assert.equal(received[0].value, 30);

  // A window whose messages carry no finite number at all never delivers.
  const bus2 = new EventBus();
  const received2: WindowAggregateResult[] = [];
  bus2.subscribe('t', (m) => received2.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 30, aggregate: 'avg', field: 'v' },
  });
  bus2.publish('t', { v: 'nan' });
  await sleep(100);
  await flush();
  assert.equal(received2.length, 0);
  assert.equal(bus2.getStats().aggregatedWindows, 0);
});

test('windows tumble: consecutive windows produce contiguous aggregates', async () => {
  const bus = new EventBus();
  const received: WindowAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 40, aggregate: 'count' },
  });
  bus.publish('t', 1);
  bus.publish('t', 2);
  await sleep(100);
  bus.publish('t', 3);
  await sleep(100);
  await flush();
  assert.equal(received.length, 2);
  assert.equal(received[0].count, 2);
  assert.equal(received[1].count, 1);
  // Non-overlapping, ordered windows.
  assert.ok(received[1].windowStart >= received[0].windowEnd);
  assert.equal(bus.getStats().aggregatedWindows, 2);
});

test('window boundaries follow the injected bus clock', async () => {
  let nowMs = 1_700_000_000_000;
  const bus = new EventBus({ now: () => nowMs });
  const received: WindowAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 1000, aggregate: 'sum', field: 'v' },
  });
  bus.publish('t', { v: 1 });
  // The bus clock jumps past the window end: the next publish closes the
  // old window first (tumbling on the injected clock, not the timer).
  nowMs += 1500;
  bus.publish('t', { v: 2 });
  await sleep(60);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].windowStart, 1_700_000_000_000);
  assert.equal(received[0].windowEnd, 1_700_000_001_000);
  assert.equal(received[0].count, 1);
  assert.equal(received[0].value, 1);
});

test('invalid window options throw RangeError at subscribe time', () => {
  const bus = new EventBus();
  const noop = () => {};
  const bad: unknown[] = [
    { windowMs: 0, aggregate: 'count' },
    { windowMs: -5, aggregate: 'count' },
    { windowMs: NaN, aggregate: 'count' },
    { windowMs: Infinity, aggregate: 'count' },
    { windowMs: 10, aggregate: 'median' },
    { windowMs: 10 },
    { windowMs: 10, aggregate: 'sum', field: '' },
    { windowMs: 10, aggregate: 'sum', field: 'a..b' },
    { windowMs: 10, aggregate: 'sum', field: '__proto__.x' },
    { windowMs: 10, aggregate: 'sum', field: 'a.constructor' },
  ];
  for (const window of bad) {
    assert.throws(
      () => bus.subscribe('t', noop, { window: window as never }),
      RangeError,
      JSON.stringify(window),
    );
  }
  assert.equal(bus.getStats().totalSubscribers, 0, 'no half-registered subscriber');
  assert.throws(() => bus.subscribe('t', noop, { window: true as never }), TypeError);
});

test('the content filter applies to raw messages before aggregation', async () => {
  const bus = new EventBus();
  const received: WindowAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    filter: (payload) => (payload as { v: number }).v > 10,
    window: { windowMs: 30, aggregate: 'sum', field: 'v' },
  });
  bus.publish('t', { v: 5 }); // filtered out: never enters the window
  bus.publish('t', { v: 20 });
  bus.publish('t', { v: 30 });
  await sleep(100);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 2);
  assert.equal(received[0].value, 50);
});

test('the aggregate travels the normal delivery path: backpressure queue', async () => {
  const bus = new EventBus();
  const received: WindowAggregateResult[] = [];
  // Tiny queue: the aggregate still flows through it (and would drop per
  // the drop policy under real pressure).
  bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    queueSize: 1,
    window: { windowMs: 30, aggregate: 'count' },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await sleep(100);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 10);
  // Raw messages never churned the queue's drop accounting.
  assert.equal(bus.getStats().droppedMessages, 0);
});

test('window composes with reliable delivery: one Delivery per window', async () => {
  const bus = new EventBus();
  const received: Array<{ payload: WindowAggregateResult; ack: () => void }> = [];
  bus.subscribeReliable(
    't',
    (d) => {
      received.push({ payload: d.msg.payload as WindowAggregateResult, ack: () => d.ack() });
      d.ack();
    },
    { window: { windowMs: 30, aggregate: 'sum', field: 'v' }, ackTimeoutMs: 5000 },
  );
  bus.publish('t', { v: 1 });
  bus.publish('t', { v: 2 });
  bus.publish('t', { v: 3 });
  await sleep(100);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].payload.count, 3);
  assert.equal(received[0].payload.value, 6);
  await flush();
  assert.equal(bus.getStats().unackedDeliveries, 0);
});

test('unsubscribing mid-window clears the timer: no phantom aggregate', async () => {
  const bus = new EventBus();
  const received: WindowAggregateResult[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 40, aggregate: 'count' },
  });
  bus.publish('t', 1);
  bus.publish('t', 2);
  sub.unsubscribe();
  await sleep(120);
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().aggregatedWindows, 0);
});

test('an already-expired message never enters the window', async () => {
  const bus = new EventBus();
  bus.setTopicTtl('t', 10);
  const received: WindowAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 60, aggregate: 'count' },
  });
  // Delayed past the topic TTL: by the time fan-out sees it, it is
  // already expired — it must be counted as expired, never aggregated.
  bus.publishDelayed('t', 'stale', { delayMs: 30 });
  bus.publish('t', 'fresh');
  await sleep(250);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 1, 'only the live message aggregated');
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('shadow cannot be combined with window', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        shadow: true,
        window: { windowMs: 10, aggregate: 'count' },
      }),
    RangeError,
  );
});

test('no sequence gaps are counted across aggregated windows', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  bus.subscribe('t', (m) => seqs.push(m.seq), {
    window: { windowMs: 30, aggregate: 'count' },
  });
  for (let i = 0; i < 6; i++) bus.publish('t', i);
  await sleep(100);
  await flush();
  // One aggregate carrying the window's last raw seq: no phantom gaps.
  assert.equal(seqs.length, 1);
  assert.equal(seqs[0], 6);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('hot topic QPS collapses to one delivery per window', async () => {
  const bus = new EventBus();
  let calls = 0;
  bus.subscribe('ticks', () => calls++, {
    window: { windowMs: 50, aggregate: 'avg', field: 'price' },
  });
  for (let i = 0; i < 200; i++) bus.publish('ticks', { price: i });
  await sleep(150);
  await flush();
  assert.equal(calls, 1, '200 publishes -> 1 handler call');
  assert.equal(bus.getStats().aggregatedWindows, 1);
});
