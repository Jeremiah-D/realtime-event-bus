import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Accepts payloads shaped like { id: number }; rejects everything else. */
const orderValidator = (payload: unknown): boolean =>
  typeof payload === 'object' && payload !== null && typeof (payload as { id?: unknown }).id === 'number';

test('no rule means no validation', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('t', 'anything goes'), 1);
  await flush();
  assert.deepEqual(received, ['anything goes']);
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('rejected publish is never fanned out and counted per topic and globally', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  bus.setTopicSchema('orders', orderValidator);
  assert.equal(bus.publish('orders', { id: 1 }), 1);
  assert.equal(bus.publish('orders', { id: 'x' }), 0);
  assert.equal(bus.publish('orders', null), 0);
  await flush();
  assert.deepEqual(received, [{ id: 1 }]);
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 'orders')!;
  assert.equal(topic.publishedMessages, 1);
  assert.equal(topic.rejectedMessages, 2);
  assert.equal(stats.rejectedMessages, 2);
  assert.equal(stats.totalPublished, 1);
});

test('rejection consumes no sequence number', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  bus.subscribe('orders', (msg) => seqs.push(msg.seq));
  bus.setTopicSchema('orders', orderValidator);
  bus.publish('orders', 'bad');
  bus.publish('orders', 'also bad');
  bus.publish('orders', { id: 1 });
  await flush();
  assert.deepEqual(seqs, [1]);
  assert.equal(bus.getStats().topics[0].lastSeq, 1);
});

test('exact-topic rule wins over patterns', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('orders.eu', (msg) => received.push(msg.payload));
  bus.setTopicSchema('orders.*', () => false);
  bus.setTopicSchema('orders.eu', () => true);
  assert.equal(bus.publish('orders.eu', 'ok'), 1);
  await flush();
  assert.deepEqual(received, ['ok']);
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('earliest-registered matching pattern wins; exact beats pattern regardless of order', () => {
  const bus = new EventBus();
  bus.subscribe('orders.*', () => {});
  bus.subscribe('orders.eu', () => {});
  bus.setTopicSchema('orders.*', () => true); // earliest matching pattern: admits
  bus.setTopicSchema('orders.**', () => false); // later pattern: would reject, but loses
  bus.setTopicSchema('orders.eu', () => false); // exact rule: wins over both patterns
  assert.equal(bus.publish('orders.us', 'ok'), 1); // admitted by the earliest pattern
  assert.equal(bus.publish('orders.eu', 'no'), 0); // rejected by the exact rule
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 1);
});

test('re-setting a rule replaces it; clearing removes it', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicSchema('t', () => false);
  assert.equal(bus.publish('t', 1), 0);
  bus.setTopicSchema('t', () => true);
  assert.equal(bus.publish('t', 2), 1);
  assert.equal(bus.clearTopicSchema('t'), true);
  assert.equal(bus.clearTopicSchema('t'), false);
  assert.equal(bus.publish('t', 3), 1);
  await flush();
  assert.deepEqual(received, [2, 3]);
});

test('invalid configuration throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicSchema('', () => true), RangeError);
  assert.throws(() => bus.setTopicSchema('t', undefined as never), RangeError);
  assert.throws(() => bus.setTopicSchema('t', 'yes' as never), RangeError);
});

test('a throwing validator propagates and mutates nothing', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicSchema('t', () => {
    throw new Error('validator bug');
  });
  assert.throws(() => bus.publish('t', 1), /validator bug/);
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 0);
  assert.equal(stats.totalPublished, 0);
  // The failed validation left no half-published state behind.
  bus.setTopicSchema('t', () => true);
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(received, [2]);
});

test('publishBatch validates each message independently', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  bus.setTopicSchema('orders', orderValidator);
  const accepted = bus.publishBatch([
    { topic: 'orders', payload: { id: 1 } },
    { topic: 'orders', payload: 'bad' },
    { topic: 'orders', payload: { id: 2 } },
  ]);
  assert.equal(accepted, 2);
  await flush();
  assert.deepEqual(received, [{ id: 1 }, { id: 2 }]);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('rejected payloads are never written to the durable log', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-schema-'));
  const bus = new EventBus({ durableLogDir: dir });
  const received: unknown[] = [];
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  bus.setTopicSchema('orders', orderValidator);
  bus.publish('orders', 'bad');
  bus.publish('orders', { id: 1 });
  await flush();
  assert.equal(bus.getStats().durableLog?.entries, 1);
  // A restarted bus replays only the admitted message.
  const bus2 = new EventBus({ durableLogDir: dir });
  const replayed: unknown[] = [];
  bus2.subscribe('orders', (msg) => replayed.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(replayed, [{ id: 1 }]);
});

test('schema rules do not inflate the subscriber pattern cache', () => {
  const bus = new EventBus();
  bus.subscribe('orders.*', () => {});
  bus.setTopicSchema('orders.*', () => true);
  bus.setTopicSchema('payments.**', () => true);
  bus.publish('orders.eu', 1);
  bus.publish('payments.x', 1);
  // One cached pattern: the subscriber's. Schema matchers live in their own cache.
  assert.equal(bus.getStats().patternCacheSize, 1);
});

test('rejected publishes do not burn the topic rate-limit budget', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  bus.setTopicSchema('t', (payload) => payload === 'ok');
  assert.equal(bus.publish('t', 'bad'), 0); // rejected: budget untouched
  assert.equal(bus.publish('t', 'ok'), 1); // burst still available
  assert.equal(bus.publish('t', 'ok'), 0); // now the bucket is empty: shed
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.rejectedMessages, 1);
  assert.equal(topic.rateLimitedMessages, 1);
});
