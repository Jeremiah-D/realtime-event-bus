import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('publishReceipt: accepted entry carries topic and stamped seq', async () => {
  const bus = new EventBus();
  const seen: Array<{ topic: string; seq: number }> = [];
  bus.subscribe('orders.*', (msg) => seen.push({ topic: msg.topic, seq: msg.seq }));
  const r1 = bus.publishReceipt('orders.created', { id: 1 });
  const r2 = bus.publishReceipt('orders.created', { id: 2 });
  assert.deepEqual(r1, { accepted: [{ topic: 'orders.created', seq: 1 }], rejected: [] });
  assert.deepEqual(r2, { accepted: [{ topic: 'orders.created', seq: 2 }], rejected: [] });
  assert.equal(bus.getStats().receiptsIssued, 2);
  await flush();
  assert.deepEqual(seen, [
    { topic: 'orders.created', seq: 1 },
    { topic: 'orders.created', seq: 2 },
  ]);
});

test('publishReceipt: rejected entry carries the admission gate reason', () => {
  const bus = new EventBus();
  bus.setTopicSchema('strict', () => false);
  const receipt = bus.publishReceipt('strict', { id: 1 });
  assert.deepEqual(receipt, {
    accepted: [],
    rejected: [{ topic: 'strict', reason: 'schema' }],
  });
  // The rejection is reconcilable with the admission-rejection counters.
  assert.equal(bus.getStats().topics.find((t) => t.topic === 'strict')?.rejectedMessages, 1);
});

test('publishReceipt: legacy publish/publishBatch/publishAtomic shapes are untouched', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  assert.equal(typeof bus.publish('t', 1), 'number');
  assert.equal(typeof bus.publishBatch([{ topic: 't', payload: 1 }]), 'number');
  assert.deepEqual(bus.publishAtomic([{ topic: 't', payload: 1 }]), { published: 1 });
  // Receipt calls do not change the legacy return values.
  assert.equal(bus.getStats().receiptsIssued, 0);
  await flush();
});

test('publishReceiptBatch: per-entry accepted/rejected with seqs in order', async () => {
  const bus = new EventBus();
  bus.subscribe('a.*', () => {});
  bus.setTopicSchema('bad', () => false);
  const receipt = bus.publishReceiptBatch([
    { topic: 'a.1', payload: 'x' },
    { topic: 'bad', payload: 'y' },
    { topic: 'a.2', payload: 'z' },
  ]);
  assert.deepEqual(receipt, {
    accepted: [
      { topic: 'a.1', seq: 1 },
      { topic: 'a.2', seq: 1 },
    ],
    rejected: [{ topic: 'bad', reason: 'schema' }],
  });
  assert.equal(bus.getStats().receiptsIssued, 1);
  await flush();
});

test('publishReceiptBatch: explicit idempotency-key hit receipts as duplicate', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const first = bus.publishReceiptBatch([{ topic: 't', payload: { n: 1 }, idempotencyKey: 'k1' }]);
  assert.equal(first.accepted.length, 1);
  assert.equal(first.rejected.length, 0);
  const retry = bus.publishReceiptBatch([{ topic: 't', payload: { n: 1 }, idempotencyKey: 'k1' }]);
  assert.deepEqual(retry, {
    accepted: [],
    rejected: [{ topic: 't', reason: 'duplicate' }],
  });
  await flush();
});

test('publishReceiptAtomic: success lists every entry with its seq', async () => {
  const bus = new EventBus();
  const seen: string[] = [];
  bus.subscribe('a.*', (msg) => seen.push(msg.topic));
  bus.subscribe('b.*', (msg) => seen.push(msg.topic));
  const receipt = bus.publishReceiptAtomic([
    { topic: 'a.1', payload: 1 },
    { topic: 'b.1', payload: 2 },
  ]);
  assert.deepEqual(receipt, {
    accepted: [
      { topic: 'a.1', seq: 1 },
      { topic: 'b.1', seq: 1 },
    ],
    rejected: [],
  });
  await flush();
  assert.deepEqual(seen, ['a.1', 'b.1']);
});

test('publishReceiptAtomic: rejected batch lists a reason per entry', async () => {
  const bus = new EventBus();
  const seen: string[] = [];
  bus.subscribe('a.*', (msg) => seen.push(msg.topic));
  bus.subscribe('b.*', (msg) => seen.push(msg.topic));
  bus.setTopicSchema('bad', () => false);
  const receipt = bus.publishReceiptAtomic([
    { topic: 'a.1', payload: 1 },
    { topic: 'bad', payload: 2 },
    { topic: 'b.1', payload: 3 },
  ]);
  // Nothing committed: every entry is rejected. The failing entry carries
  // its own gate reason; the rest carry the batch-level cause.
  assert.deepEqual(receipt, {
    accepted: [],
    rejected: [
      { topic: 'a.1', reason: 'schema' },
      { topic: 'bad', reason: 'schema' },
      { topic: 'b.1', reason: 'schema' },
    ],
  });
  await flush();
  assert.deepEqual(seen, []);
  // Classic publishAtomic is unaffected and still reports the first failure.
  const classic = bus.publishAtomic([
    { topic: 'a.1', payload: 1 },
    { topic: 'bad', payload: 2 },
  ]);
  assert.equal(classic.published, 0);
  assert.equal(classic.rejected?.reason, 'schema');
  assert.equal(classic.rejected?.index, 1);
});

test('publishReceiptAtomic: whole-batch idempotency conflict maps to duplicate', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const receipt = bus.publishReceiptAtomic([
    { topic: 't', payload: { n: 1 }, idempotencyKey: 'dup' },
    { topic: 't', payload: { n: 2 }, idempotencyKey: 'dup' },
  ]);
  assert.deepEqual(receipt.accepted, []);
  assert.equal(receipt.rejected.length, 2);
  for (const entry of receipt.rejected) {
    assert.equal(entry.topic, 't');
    assert.equal(entry.reason, 'duplicate');
  }
});

test('publishReceipt*: empty input issues an empty receipt', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.publishReceiptBatch([]), { accepted: [], rejected: [] });
  assert.deepEqual(bus.publishReceiptAtomic([]), { accepted: [], rejected: [] });
  assert.equal(bus.getStats().receiptsIssued, 2);
});

test('publishReceipt: closed bus throws', () => {
  const bus = new EventBus();
  bus.close();
  assert.throws(() => bus.publishReceipt('t', 1));
  assert.throws(() => bus.publishReceiptBatch([{ topic: 't', payload: 1 }]));
  assert.throws(() => bus.publishReceiptAtomic([{ topic: 't', payload: 1 }]));
});
