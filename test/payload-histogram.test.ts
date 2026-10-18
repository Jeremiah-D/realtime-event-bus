import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** The exact byte size the bus samples: JSON UTF-8 bytes, unserializable payloads count as 0. */
const sizeOf = (payload: unknown): number => {
  try {
    const s = JSON.stringify(payload);
    return typeof s === 'string' ? Buffer.byteLength(s, 'utf8') : 0;
  } catch {
    return 0;
  }
};

/** A string payload whose sampled size is exactly `n` bytes (`"..."` adds 2). */
const sized = (n: number): string => 'x'.repeat(n - 2);

test('payload histogram is opt-in: no rule means no stats key and no metrics', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', { a: 1 });
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.topics.find((t) => t.topic === 't')?.payloadHistogram, undefined);
  assert.equal(stats.payloadHistogram, undefined);
  const text = renderPrometheus(stats);
  assert.ok(!text.includes('eventbus_payload_bytes_bucket'));
  assert.ok(!text.includes('eventbus_payload_bytes_count'));
  assert.ok(!text.includes('eventbus_payload_bytes_sum'));
});

test('payload histogram samples admitted publishes into cumulative buckets', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicPayloadHistogram('t', { buckets: [10, 20, 30] });
  bus.publish('t', sized(10));
  bus.publish('t', sized(20));
  bus.publish('t', sized(30));
  bus.publish('t', sized(25)); // lands in the (20, 30] bucket
  await flush();
  const h = bus.getStats().topics.find((t) => t.topic === 't')?.payloadHistogram;
  assert.ok(h !== undefined);
  assert.deepEqual(
    h.buckets.map((b) => [b.le, b.count]),
    [
      [10, 1],
      [20, 2],
      [30, 4],
    ],
  );
  assert.equal(h.count, 4);
  assert.equal(h.sum, 10 + 20 + 30 + 25);
  assert.equal(h.mean, (10 + 20 + 30 + 25) / 4);
  // nearest-rank: p50 rank = ceil(0.5*4) = 2 -> le 20; p99 rank = ceil(0.99*4) = 4 -> le 30.
  assert.equal(h.p50, 20);
  assert.equal(h.p99, 30);
});

test('payload histogram p50/p99 are bucket-boundary nearest-rank estimates', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicPayloadHistogram('t', { buckets: [10, 20, 30] });
  bus.publish('t', sized(5));
  bus.publish('t', sized(15));
  bus.publish('t', sized(25));
  await flush();
  const h = bus.getStats().topics.find((t) => t.topic === 't')?.payloadHistogram;
  assert.ok(h !== undefined);
  assert.equal(h.count, 3);
  assert.equal(h.mean, 15);
  // p50 rank = ceil(1.5) = 2 -> first bucket with cumulative >= 2 is le 20.
  assert.equal(h.p50, 20);
  // p99 rank = ceil(2.97) = 3 -> le 30.
  assert.equal(h.p99, 30);
});

test('payload histogram: rule set but nothing sampled yet is zero-filled', () => {
  const bus = new EventBus();
  bus.setTopicPayloadHistogram('t', { buckets: [10, 20] });
  const h = bus.getStats().topics.find((t) => t.topic === 't')?.payloadHistogram;
  assert.ok(h !== undefined);
  assert.deepEqual(
    h.buckets.map((b) => [b.le, b.count]),
    [
      [10, 0],
      [20, 0],
    ],
  );
  assert.equal(h.count, 0);
  assert.equal(h.sum, 0);
  assert.equal(h.mean, 0);
  assert.equal(h.p50, undefined);
  assert.equal(h.p99, undefined);
});

test('payload histogram: exact rule wins over pattern; first pattern wins', async () => {
  const bus = new EventBus();
  bus.subscribe('orders.eu', () => {});
  bus.setTopicPayloadHistogram('orders.*', { buckets: [5] });
  bus.setTopicPayloadHistogram('orders.eu', { buckets: [100] });
  bus.publish('orders.eu', sized(50));
  await flush();
  const h = bus.getStats().topics.find((t) => t.topic === 'orders.eu')?.payloadHistogram;
  assert.ok(h !== undefined);
  // Exact rule won: the 50-byte payload is <= 100, not > 5.
  assert.deepEqual(
    h.buckets.map((b) => [b.le, b.count]),
    [[100, 1]],
  );

  const bus2 = new EventBus();
  bus2.subscribe('a.b', () => {});
  bus2.setTopicPayloadHistogram('a.*', { buckets: [7] });
  bus2.setTopicPayloadHistogram('*.b', { buckets: [9] });
  bus2.publish('a.b', sized(8));
  await flush();
  const h2 = bus2.getStats().topics.find((t) => t.topic === 'a.b')?.payloadHistogram;
  assert.ok(h2 !== undefined);
  // First-registered pattern won: 8 bytes > 7, so the single bucket counts 0.
  assert.deepEqual(
    h2.buckets.map((b) => [b.le, b.count]),
    [[7, 0]],
  );
  assert.equal(h2.count, 1);
});

test('payload histogram: clear stops sampling and drops the per-topic key', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicPayloadHistogram('t', { buckets: [10] });
  bus.publish('t', sized(5));
  await flush();
  assert.equal(bus.clearTopicPayloadHistogram('t'), true);
  assert.equal(bus.clearTopicPayloadHistogram('t'), false);
  bus.publish('t', sized(5));
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.topics.find((t) => t.topic === 't')?.payloadHistogram, undefined);
  // The bus-wide aggregate keeps its cumulative history.
  assert.equal(stats.payloadHistogram?.count, 1);
});

test('payload histogram: schema rejections are never sampled', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchema('t', () => false);
  bus.setTopicPayloadHistogram('t', { buckets: [10, 100] });
  bus.publish('t', sized(50));
  await flush();
  const stats = bus.getStats();
  const row = stats.topics.find((t) => t.topic === 't');
  assert.equal(row?.rejectedMessages, 1);
  // Rule still matches, so the key is present — but zero-filled: the
  // rejected publish never became a sample.
  const h = row?.payloadHistogram;
  assert.ok(h !== undefined);
  assert.equal(h.count, 0);
  assert.equal(h.sum, 0);
  assert.equal(stats.payloadHistogram, undefined);
});

test('payload histogram: unserializable payloads sample as 0 bytes', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicPayloadHistogram('t', { buckets: [10] });
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  bus.publish('t', circular);
  await flush();
  const h = bus.getStats().topics.find((t) => t.topic === 't')?.payloadHistogram;
  assert.ok(h !== undefined);
  assert.equal(h.count, 1);
  assert.equal(h.sum, 0);
  assert.deepEqual(
    h.buckets.map((b) => [b.le, b.count]),
    [[10, 1]],
  );
});

test('payload histogram: re-setting buckets resets the per-topic counts', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicPayloadHistogram('t', { buckets: [10] });
  bus.publish('t', sized(5));
  await flush();
  bus.setTopicPayloadHistogram('t', { buckets: [10, 100] });
  const h = bus.getStats().topics.find((t) => t.topic === 't')?.payloadHistogram;
  assert.ok(h !== undefined);
  // New boundaries: the old sample is gone, buckets are zero-filled.
  assert.deepEqual(
    h.buckets.map((b) => [b.le, b.count]),
    [
      [10, 0],
      [100, 0],
    ],
  );
  assert.equal(h.count, 0);
  bus.publish('t', sized(50));
  await flush();
  const h2 = bus.getStats().topics.find((t) => t.topic === 't')?.payloadHistogram;
  assert.equal(h2?.count, 1);
  assert.deepEqual(
    h2?.buckets.map((b) => [b.le, b.count]),
    [
      [10, 0],
      [100, 1],
    ],
  );
});

test('payload histogram: bus-wide aggregate accumulates across topics', async () => {
  const bus = new EventBus();
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  bus.setTopicPayloadHistogram('a', { buckets: [10, 20] });
  bus.setTopicPayloadHistogram('b', { buckets: [10, 20] });
  bus.publish('a', sized(5));
  bus.publish('a', sized(15));
  bus.publish('b', sized(25));
  await flush();
  const g = bus.getStats().payloadHistogram;
  assert.ok(g !== undefined);
  assert.equal(g.count, 3);
  assert.equal(g.sum, 5 + 15 + 25);
  assert.deepEqual(
    g.buckets.map((b) => [b.le, b.count]),
    [
      [10, 1],
      [20, 2],
    ],
  );
  assert.equal(g.mean, 15);
  assert.equal(g.p50, 20);
  assert.equal(g.p99, 20);
});

test('payload histogram: invalid bucket configs throw RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicPayloadHistogram('', { buckets: [10] }), RangeError);
  assert.throws(() => bus.setTopicPayloadHistogram('t', { buckets: [] }), RangeError);
  assert.throws(() => bus.setTopicPayloadHistogram('t', { buckets: [10, 10] }), RangeError);
  assert.throws(() => bus.setTopicPayloadHistogram('t', { buckets: [20, 10] }), RangeError);
  assert.throws(() => bus.setTopicPayloadHistogram('t', { buckets: [0] }), RangeError);
  assert.throws(() => bus.setTopicPayloadHistogram('t', { buckets: [-5] }), RangeError);
  assert.throws(() => bus.setTopicPayloadHistogram('t', { buckets: [Number.NaN] }), RangeError);
  assert.throws(() => bus.setTopicPayloadHistogram('t', { buckets: [Number.POSITIVE_INFINITY] }), RangeError);
  assert.throws(
    () => bus.setTopicPayloadHistogram('t', { buckets: 'nope' as unknown as number[] }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicPayloadHistogram('t', { buckets: [10, 'x' as unknown as number] }),
    RangeError,
  );
  // A failed validation leaves no rule behind.
  assert.equal(bus.getStats().topics.find((t) => t.topic === 't')?.payloadHistogram, undefined);
});

test('payload histogram: rule stores a defensive copy of the buckets array', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const buckets = [10, 20];
  bus.setTopicPayloadHistogram('t', { buckets });
  buckets.push(30); // must not rewrite the installed rule
  bus.publish('t', sized(25));
  await flush();
  const h = bus.getStats().topics.find((t) => t.topic === 't')?.payloadHistogram;
  assert.deepEqual(
    h?.buckets.map((b) => b.le),
    [10, 20],
  );
  assert.equal(h?.count, 1);
});

test('renderPrometheus emits the payload histogram series', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.subscribe('plain', () => {});
  bus.setTopicPayloadHistogram('t', { buckets: [10, 20] });
  bus.publish('t', sized(5));
  bus.publish('t', sized(15));
  bus.publish('t', sized(25));
  bus.publish('plain', { a: 1 });
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('# TYPE eventbus_payload_bytes_bucket histogram'));
  assert.ok(text.includes('eventbus_payload_bytes_bucket{topic="t",le="10"} 1'));
  assert.ok(text.includes('eventbus_payload_bytes_bucket{topic="t",le="20"} 2'));
  assert.ok(text.includes('eventbus_payload_bytes_bucket{topic="t",le="+Inf"} 3'));
  assert.ok(text.includes('eventbus_payload_bytes_count{topic="t"} 3'));
  assert.ok(text.includes('eventbus_payload_bytes_sum{topic="t"} 45'));
  // The unconfigured topic carries no histogram series.
  assert.ok(!text.includes('topic="plain",le='));
  assert.ok(!text.includes('eventbus_payload_bytes_count{topic="plain"}'));
  assert.ok(text.endsWith('\n'));
});

test('renderPrometheus escapes topic names in histogram labels', () => {
  const bus = new EventBus();
  bus.setTopicPayloadHistogram('we"ird', { buckets: [10] });
  bus.publish('we"ird', sized(5));
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_payload_bytes_bucket{topic="we\\"ird",le="10"} 1'));
});

test('sizeOf sanity: the test helper matches the bus estimator', () => {
  assert.equal(sizeOf({ a: 1 }), 7);
  assert.equal(sizeOf(sized(10)), 10);
});
