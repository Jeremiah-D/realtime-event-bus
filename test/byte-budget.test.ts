import test from 'node:test';
import assert from 'node:assert/strict';
import { BoundedQueue } from '../src/backpressure.ts';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const strBytes = (s: string): number => Buffer.byteLength(s, 'utf8');

test('drop-oldest evicts the oldest entries until the incoming item fits', () => {
  const q = new BoundedQueue<string>({ capacity: 100, maxBytes: 10, byteSize: strBytes });
  assert.equal(q.push('aaaa'), 'accepted'); // 4
  assert.equal(q.push('bbbb'), 'accepted'); // 8
  assert.equal(q.push('cccc'), 'accepted'); // 12 > 10 -> evict 'aaaa' -> 8
  assert.equal(q.size, 2);
  assert.equal(q.queueBytes, 8);
  assert.equal(q.droppedCount, 1);
  assert.deepEqual(q.drain(), ['bbbb', 'cccc']);
  assert.equal(q.queueBytes, 0);
});

test('drop-newest discards the incoming item when it would overflow', () => {
  const q = new BoundedQueue<string>({
    capacity: 100,
    policy: 'drop-newest',
    maxBytes: 10,
    byteSize: strBytes,
  });
  assert.equal(q.push('aaaa'), 'accepted');
  assert.equal(q.push('bbbb'), 'accepted');
  assert.equal(q.push('cccc'), 'dropped');
  assert.equal(q.droppedCount, 1);
  assert.equal(q.queueBytes, 8);
  assert.deepEqual(q.drain(), ['aaaa', 'bbbb']);
});

test('an item larger than the whole budget is never admitted', () => {
  for (const policy of ['drop-oldest', 'drop-newest'] as const) {
    const q = new BoundedQueue<string>({
      capacity: 100,
      policy,
      maxBytes: 10,
      byteSize: strBytes,
    });
    assert.equal(q.push('0123456789abcdef'), 'dropped');
    assert.equal(q.droppedCount, 1);
    assert.equal(q.queueBytes, 0);
    assert.equal(q.size, 0);
  }
});

test('byte-budget eviction is priority-aware under drop-oldest', () => {
  const q = new BoundedQueue<string>({ capacity: 100, maxBytes: 10, byteSize: strBytes });
  assert.equal(q.push('aaaaa', 0), 'accepted'); // 5, low priority
  assert.equal(q.push('bbbbb', 9), 'accepted'); // 5, high priority
  assert.equal(q.push('c', 0), 'accepted'); // 11 > 10 -> evict 'aaaaa' (oldest lowest priority)
  assert.equal(q.droppedCount, 1);
  assert.deepEqual([...q.droppedByPriority.entries()], [[0, 1]]);
  assert.deepEqual(q.drain(), ['bbbbb', 'c']);
  // An incoming item strictly lower priority than everything queued is
  // dropped instead of evicting higher-priority backlog.
  const q2 = new BoundedQueue<string>({ capacity: 100, maxBytes: 10, byteSize: strBytes });
  assert.equal(q2.push('aaaaa', 5), 'accepted');
  assert.equal(q2.push('bbbbbb', 0), 'dropped'); // 11 > 10, incoming is lowest priority
  assert.equal(q2.droppedCount, 1);
  assert.deepEqual(q2.drain(), ['aaaaa']);
});

test('count and byte dimensions compose: whichever fills first sheds', () => {
  const q = new BoundedQueue<string>({
    capacity: 2,
    maxBytes: 100,
    byteSize: strBytes,
  });
  assert.equal(q.push('a'), 'accepted');
  assert.equal(q.push('b'), 'accepted');
  assert.equal(q.push('c'), 'accepted'); // count-full, not byte-full
  assert.equal(q.droppedCount, 1);
  assert.deepEqual(q.drain(), ['b', 'c']);
});

test('drainLiveUpTo keeps the byte accounting exact', () => {
  const q = new BoundedQueue<string>({ capacity: 100, maxBytes: 100, byteSize: strBytes });
  q.push('aaaa');
  q.push('bbbb', 0, 10); // expires at t=10
  q.push('cccc');
  assert.equal(q.queueBytes, 12);
  const { live, expired } = q.drainLiveUpTo(20, 1);
  assert.deepEqual(live, ['aaaa']);
  assert.deepEqual(expired, ['bbbb']);
  assert.equal(q.queueBytes, 4); // only 'cccc' remains
  assert.equal(q.expiredCount, 1);
});

test('maxBytes and byteSize are validated', () => {
  for (const maxBytes of [0, -1, NaN, Infinity]) {
    assert.throws(
      () => new BoundedQueue<string>({ capacity: 10, maxBytes, byteSize: strBytes }),
      RangeError,
    );
  }
  assert.throws(
    () => new BoundedQueue<string>({ capacity: 10, maxBytes: 10, byteSize: 42 as never }),
    TypeError,
  );
  const q = new BoundedQueue<string>({ capacity: 10, maxBytes: 10, byteSize: () => NaN });
  assert.throws(() => q.push('x'), RangeError);
  const q2 = new BoundedQueue<string>({ capacity: 10, maxBytes: 10, byteSize: () => -1 });
  assert.throws(() => q2.push('x'), RangeError);
});

test('default byte estimator uses JSON UTF-8 bytes', () => {
  const q = new BoundedQueue<unknown>({ capacity: 100, maxBytes: 12 });
  assert.equal(q.push({ a: 1 }), 'accepted'); // {"a":1} = 7
  assert.equal(q.push('bb'), 'accepted'); // "bb" = 4, total 11
  assert.equal(q.push('c'), 'accepted'); // "c" = 3, 14 > 12 -> evict {a:1} -> 7
  assert.equal(q.droppedCount, 1);
  assert.deepEqual(q.drain(), ['bb', 'c']);
});

test('bus: queueMaxBytes sheds per drop policy and surfaces as dropped + gap', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  // '"xxxxxxxxxx"' is 12 JSON bytes; budget 20 holds one at a time.
  const sub = bus.subscribe('t', (msg) => received.push(msg.seq), {
    queueSize: 100,
    queueMaxBytes: 20,
  });
  bus.publish('t', 'baseline');
  await flush();
  assert.deepEqual(received, [1]);
  bus.publish('t', 'xxxxxxxxxx'); // 12
  bus.publish('t', 'yyyyyyyyyy'); // 24 > 20 -> evict seq 2
  bus.publish('t', 'zzzzzzzzzz'); // 24 > 20 -> evict seq 3
  // Before the flush: one message buffered, two shed by the byte budget.
  assert.equal(bus.getStats().queueBytes, 12);
  assert.equal(bus.droppedCount(sub.id), 2);
  await flush();
  assert.deepEqual(received, [1, 4]);
  const stats = bus.getStats();
  assert.equal(stats.droppedMessages, 2);
  assert.equal(stats.sequenceGaps, 2); // seqs 2 and 3 never delivered
  assert.equal(stats.queueBytes, 0); // drained
});

test('bus: queueMaxBytes validation fails fast at subscribe time', () => {
  const bus = new EventBus();
  for (const queueMaxBytes of [0, -100, NaN, Infinity]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { queueMaxBytes }),
      RangeError,
    );
  }
  assert.equal(bus.getStats().totalSubscribers, 0); // nothing half-registered
});

test('bus: queueBytes is 0 without a byte budget', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 'x'.repeat(1000));
  assert.equal(bus.getStats().queueBytes, 0); // not measured when disabled
  await flush();
  assert.equal(bus.getStats().queueBytes, 0);
});

test('bus: byte-budget eviction is visible in the Prometheus exposition', async () => {
  const { renderPrometheus } = await import('../src/metrics.ts');
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { queueSize: 100, queueMaxBytes: 20 });
  bus.publish('t', 'xxxxxxxxxx');
  bus.publish('t', 'yyyyyyyyyy'); // evicts the first
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_queue_bytes 12'), `missing gauge in:\n${text}`);
  assert.ok(
    text.includes('eventbus_dropped_messages_total 1'),
    `missing drop counter in:\n${text}`,
  );
  await flush();
});
