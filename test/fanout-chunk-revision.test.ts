/**
 * Bus-level fan-out chunk size revision (EB-320).
 *
 * `setFanOutChunkSize` retunes how many subscriber deliveries one
 * synchronous `publish` performs before yielding to chained microtask
 * chunks — without restarting the bus. The fan-out itself is never
 * revised: matching, filtering, signing, backpressure, and
 * per-subscriber FIFO are unchanged, and the new size governs from
 * the next publish — a chain already in flight keeps the size it
 * started with. Illegal values fail closed with the live size and the
 * audit log untouched; an identical (or empty) revision is a no-op
 * with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const audit = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'fan_out_chunk_size_revised' });

/**
 * Per-subscriber queue depths in subscription order, read synchronously —
 * the observable for "this chunk ran inside `publish`, not on a later
 * microtask". Test-only introspection (mirrors `getStats().shadows[].queueSize`).
 */
const queueSizes = (bus: EventBus): number[] =>
  [...(bus as unknown as { subscribers: Map<string, { queue: { size: number } }> }).subscribers.values()].map(
    (s) => s.queue.size,
  );

function busWithSubscribers(count: number, chunkSize?: number) {
  const bus = new EventBus(chunkSize === undefined ? {} : { fanOutChunkSize: chunkSize });
  const seen: number[] = [];
  for (let i = 0; i < count; i++) {
    const idx = i;
    bus.subscribe('t', () => {
      seen.push(idx);
    });
  }
  return { bus, seen };
}

test('constructor still rejects illegal fanOutChunkSize, Infinity stays legal', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, '3']) {
    assert.throws(() => new EventBus({ fanOutChunkSize: bad as never }), RangeError);
  }
  assert.doesNotThrow(() => new EventBus());
  assert.doesNotThrow(() => new EventBus({ fanOutChunkSize: Infinity }));
  assert.doesNotThrow(() => new EventBus({ fanOutChunkSize: 1 }));
});

test('setFanOutChunkSize rejects illegal values and leaves the live size untouched', async () => {
  const { bus, seen } = busWithSubscribers(5, 2);
  bus.publish('t', 'x');
  assert.deepEqual(queueSizes(bus), [1, 1, 0, 0, 0]);
  const bad: Array<unknown> = [
    { fanOutChunkSize: 0 },
    { fanOutChunkSize: -1 },
    { fanOutChunkSize: 1.5 },
    { fanOutChunkSize: Number.NaN },
    { fanOutChunkSize: '2' },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setFanOutChunkSize(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: same chunking, same queues, no audit.
  assert.equal(audit(bus).length, 0);
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().fanOutYields, 2);
  // A second publish still chunks at the untouched size 2.
  bus.publish('t', 'y');
  assert.deepEqual(queueSizes(bus), [1, 1, 0, 0, 0]);
  // A valid revision afterwards still works on the untouched size.
  bus.setFanOutChunkSize({ fanOutChunkSize: 3 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { fanOutChunkSize: 3, oldFanOutChunkSize: 2 });
  bus.close();
});

test('setFanOutChunkSize with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus({ fanOutChunkSize: 2 });
  bus.setFanOutChunkSize({ fanOutChunkSize: 2 });
  bus.setFanOutChunkSize({});
  bus.setFanOutChunkSize(null as never);
  // `Infinity` re-affirms the fully-synchronous default on a default bus.
  const plain = new EventBus();
  plain.setFanOutChunkSize({ fanOutChunkSize: Infinity });
  plain.setFanOutChunkSize({});
  assert.equal(audit(bus).length, 0);
  assert.equal(audit(plain).length, 0);
  bus.close();
  plain.close();
});

test('revision audits the old and new size, null for fully-synchronous', () => {
  const bus = new EventBus();
  bus.setFanOutChunkSize({ fanOutChunkSize: 2 });
  bus.setFanOutChunkSize({ fanOutChunkSize: Infinity });
  bus.setFanOutChunkSize({ fanOutChunkSize: 5 });
  const entries = audit(bus);
  assert.equal(entries.length, 3);
  assert.deepEqual(entries[0].detail, { fanOutChunkSize: 2, oldFanOutChunkSize: null });
  assert.deepEqual(entries[1].detail, { fanOutChunkSize: null, oldFanOutChunkSize: 2 });
  assert.deepEqual(entries[2].detail, { fanOutChunkSize: 5, oldFanOutChunkSize: null });
  bus.close();
});

test('enabling chunking mid-life chunks the next publish, order preserved', async () => {
  const { bus, seen } = busWithSubscribers(5);
  // Fully synchronous before the revision: all queues filled at once.
  bus.publish('t', 'a');
  assert.deepEqual(queueSizes(bus), [1, 1, 1, 1, 1]);
  assert.equal(bus.getStats().fanOutYields, 0);
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4]);
  // Enable chunking: the next publish runs its first chunk synchronously.
  bus.setFanOutChunkSize({ fanOutChunkSize: 2 });
  bus.publish('t', 'b');
  assert.deepEqual(queueSizes(bus), [1, 1, 0, 0, 0]);
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().fanOutYields, 2);
  bus.close();
});

test('Infinity revision restores fully-synchronous fan-out mid-life', async () => {
  const { bus, seen } = busWithSubscribers(5, 2);
  bus.publish('t', 'a');
  assert.deepEqual(queueSizes(bus), [1, 1, 0, 0, 0]);
  await flush();
  bus.setFanOutChunkSize({ fanOutChunkSize: Infinity });
  bus.publish('t', 'b');
  // Fully synchronous again: every queue filled inside publish, no new yield.
  assert.deepEqual(queueSizes(bus), [1, 1, 1, 1, 1]);
  assert.equal(bus.getStats().fanOutYields, 2);
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
  bus.close();
});

test('an in-flight chain keeps the size it started with', async () => {
  const { bus, seen } = busWithSubscribers(5, 2);
  bus.publish('t', 'x');
  // First chunk (2 deliveries) ran inside publish; the rest are pending.
  assert.deepEqual(queueSizes(bus), [1, 1, 0, 0, 0]);
  // Revise to 1 before the chained steps drain: the in-flight chain
  // must finish at size 2 (2 yields for 5 tasks), not size 1 (4 yields).
  bus.setFanOutChunkSize({ fanOutChunkSize: 1 });
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().fanOutYields, 2);
  // The new size governs the next publish: 5 tasks at size 1 yield 4.
  bus.publish('t', 'y');
  assert.deepEqual(queueSizes(bus), [1, 0, 0, 0, 0]);
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().fanOutYields, 6);
  bus.close();
});

test('shrinking the chunk size re-chunks the next publish, widening defers yields', async () => {
  const { bus, seen } = busWithSubscribers(5, 4);
  bus.publish('t', 'a');
  assert.deepEqual(queueSizes(bus), [1, 1, 1, 1, 0]);
  await flush();
  bus.setFanOutChunkSize({ fanOutChunkSize: 2 });
  bus.publish('t', 'b');
  assert.deepEqual(queueSizes(bus), [1, 1, 0, 0, 0]);
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
  bus.setFanOutChunkSize({ fanOutChunkSize: 10 });
  bus.publish('t', 'c');
  // At or under the chunk size the whole fan-out runs synchronously.
  assert.deepEqual(queueSizes(bus), [1, 1, 1, 1, 1]);
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
  bus.close();
});
