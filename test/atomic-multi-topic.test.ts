import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/**
 * EB-237: multi-topic atomic publish. The atomic core (all-or-nothing
 * shadow admission + single-turn commit) predates this item (EB-23 /
 * EB-190, covered in test/atomic.test.ts); this file covers the
 * EB-237 contract from the caller's side — cross-topic all-or-nothing
 * with per-topic seqs and durable-log records — plus the genuinely new
 * piece: one batch-level traceparent shared by the whole batch.
 */

test('EB-237: a cross-topic batch commits atomically with per-topic seqs and durable-log records', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-atomic-multi-'));
  const bus = new EventBus({ durableLogDir: dir });
  const a: unknown[] = [];
  const b: unknown[] = [];
  bus.subscribe('a', (msg) => a.push(msg.payload));
  bus.subscribe('b', (msg) => b.push(msg.payload));
  const res = bus.publishAtomic([
    { topic: 'a', payload: 1 },
    { topic: 'b', payload: 2 },
    { topic: 'a', payload: 3 },
  ]);
  assert.deepEqual(res, { published: 3 });
  const stats = bus.getStats();
  // Each topic stamps its own independent sequence.
  assert.equal(stats.topics.find((t) => t.topic === 'a')!.lastSeq, 2);
  assert.equal(stats.topics.find((t) => t.topic === 'b')!.lastSeq, 1);
  // One durable-log record per committed entry, across topics.
  assert.equal(stats.durableLog?.entries, 3);
  return flush().then(() => {
    assert.deepEqual(a, [1, 3]);
    assert.deepEqual(b, [2]);
  });
});

test('EB-237: one failing topic rejects the whole cross-topic batch with zero side effects', async () => {
  const bus = new EventBus();
  const a: unknown[] = [];
  const b: unknown[] = [];
  bus.subscribe('a', (msg) => a.push(msg.payload));
  bus.subscribe('b', (msg) => b.push(msg.payload));
  bus.setTopicSchema('b', (p) => typeof p === 'number');
  const res = bus.publishAtomic([
    { topic: 'a', payload: 1 },
    { topic: 'b', payload: 'bad' },
  ]);
  assert.deepEqual(res, { published: 0, rejected: { index: 1, topic: 'b', reason: 'schema' } });
  await flush();
  assert.deepEqual(a, []);
  assert.deepEqual(b, []);
  const stats = bus.getStats();
  // A topic with zero admissions has no stats row: both read as 0.
  assert.equal(stats.topics.find((t) => t.topic === 'a')?.lastSeq ?? 0, 0);
  assert.equal(stats.topics.find((t) => t.topic === 'b')?.lastSeq ?? 0, 0);
});

test('EB-237: a batch traceparent runs one trace id through the whole batch', () => {
  const bus = new EventBus({ trace: true });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  const batchTraceId = 'c'.repeat(32);
  const batchSpanId = 'd'.repeat(16);
  const ownTraceId = 'e'.repeat(32);
  const ownSpanId = 'f'.repeat(16);
  const res = bus.publishAtomic(
    [
      { topic: 'a', payload: 1 },
      // An entry with its own traceparent keeps it.
      { topic: 'b', payload: 2, traceparent: `00-${ownTraceId}-${ownSpanId}-01` },
      { topic: 'a', payload: 3 },
    ],
    { traceparent: `00-${batchTraceId}-${batchSpanId}-01` },
  );
  assert.deepEqual(res, { published: 3 });
  const publishes = bus.getStats().traceSpans.filter((s) => s.name === 'bus.publish');
  assert.equal(publishes.length, 3);
  // Entries without their own traceparent inherit the batch trace.
  const batchSpans = publishes.filter((s) => s.traceId === batchTraceId);
  assert.equal(batchSpans.length, 2);
  for (const s of batchSpans) assert.equal(s.parentId, batchSpanId);
  // The entry with its own traceparent is untouched by the batch value.
  const ownSpans = publishes.filter((s) => s.traceId === ownTraceId);
  assert.equal(ownSpans.length, 1);
  assert.equal(ownSpans[0].parentId, ownSpanId);
});

test('EB-237: without a batch traceparent every entry traces on its own, as before', () => {
  const bus = new EventBus({ trace: true });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  bus.publishAtomic([
    { topic: 'a', payload: 1 },
    { topic: 'b', payload: 2 },
  ]);
  const publishes = bus.getStats().traceSpans.filter((s) => s.name === 'bus.publish');
  assert.equal(publishes.length, 2);
  assert.notEqual(publishes[0].traceId, publishes[1].traceId);
});

test('EB-237: a non-string batch traceparent throws RangeError before anything is admitted', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  bus.subscribe('a', (msg) => seen.push(msg.payload));
  assert.throws(
    () => bus.publishAtomic([{ topic: 'a', payload: 1 }], { traceparent: 42 as never }),
    RangeError,
  );
  await flush();
  assert.deepEqual(seen, []);
  assert.equal(bus.getStats().topics.find((t) => t.topic === 'a')?.lastSeq ?? 0, 0);
});
