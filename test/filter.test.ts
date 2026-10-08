import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const collect = (bus: EventBus, pattern: string, opts?: Parameters<EventBus['subscribe']>[2]) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

test('filter: rejects non-matching messages, accepts matching ones', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 'market.**', {
    filter: (payload) => (payload as { symbol: string }).symbol === 'BTC',
  });
  bus.publish('market.btc', { symbol: 'BTC', price: 1 });
  bus.publish('market.eth', { symbol: 'ETH', price: 2 });
  bus.publish('market.btc', { symbol: 'BTC', price: 3 });
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    [
      { symbol: 'BTC', price: 1 },
      { symbol: 'BTC', price: 3 },
    ],
  );
  const stats = bus.getStats();
  assert.equal(stats.filteredMessages, 1);
  assert.equal(stats.topics.find((t) => t.topic === 'market.eth')?.filteredMessages, 1);
  assert.equal(stats.topics.find((t) => t.topic === 'market.btc')?.filteredMessages, 0);
});

test('filter: rejected messages consume no backpressure budget', async () => {
  const bus = new EventBus();
  // Tiny queue with drop-oldest: if filtered messages ever entered it,
  // accepted messages would be churned out and droppedCount would move.
  const { received, sub } = collect(bus, 't', {
    queueSize: 2,
    filter: (payload) => (payload as { keep: boolean }).keep,
  });
  for (let i = 0; i < 10; i++) bus.publish('t', { keep: false, i });
  bus.publish('t', { keep: true, i: 'a' });
  bus.publish('t', { keep: true, i: 'b' });
  await flush();
  assert.equal(bus.droppedCount(sub.id), 0);
  assert.deepEqual(
    received.map((m) => (m.payload as { i: unknown }).i),
    ['a', 'b'],
  );
  assert.equal(bus.getStats().filteredMessages, 10);
});

test('filter: skipped messages never count as sequence gaps', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', {
    filter: (payload) => (payload as { n: number }).n % 2 === 0,
  });
  for (let n = 1; n <= 6; n++) bus.publish('t', { n });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n),
    [2, 4, 6],
  );
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('filter: a genuinely dropped message still counts as a gap', async () => {
  const bus = new EventBus();
  // queueSize 1 + drop-oldest, filter accepts odd n:
  //   s1 {n:1} queued; s2 {n:2} filtered (baseline 2);
  //   s3 {n:3} queued, drops s1; s4 {n:5} queued, drops s3.
  // Delivery of s4 sees last=2, seq=4 -> the dropped s3 counts as one
  // gap; the filtered s2 does not.
  const { received } = collect(bus, 't', {
    queueSize: 1,
    filter: (payload) => (payload as { n: number }).n % 2 === 1,
  });
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  bus.publish('t', { n: 3 });
  bus.publish('t', { n: 5 });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n),
    [5],
  );
  assert.equal(bus.getStats().sequenceGaps, 1);
});

test('filter: non-function filter throws TypeError at subscribe time', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { filter: 'nope' as unknown as () => boolean }),
    TypeError,
  );
  // The failed subscribe left nothing registered.
  assert.equal(bus.subscriberCount(), 0);
});

test('filter: throwing filter propagates to the publish call', () => {
  const bus = new EventBus();
  collect(bus, 't', {
    filter: () => {
      throw new Error('boom');
    },
  });
  assert.throws(() => bus.publish('t', {}), /boom/);
});

test('filter: applies to the assigned member of a consumer group', async () => {
  const bus = new EventBus();
  const gotA: BusMessage[] = [];
  const gotB: BusMessage[] = [];
  // One member filters everything out; the other accepts. Round-robin
  // assigns m1 -> A (filtered, dropped), m2 -> B (delivered).
  bus.subscribeToGroup('g', 't', (m) => gotA.push(m), { filter: () => false });
  bus.subscribeToGroup('g', 't', (m) => gotB.push(m));
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.equal(gotA.length, 0);
  assert.deepEqual(gotB.map((m) => (m.payload as { n: number }).n), [2]);
  assert.equal(bus.getStats().filteredMessages, 1);
});

test('filter: sees the raw payload when compression is enabled', async () => {
  const bus = new EventBus();
  bus.setTopicCompression('big', { thresholdBytes: 10 });
  const { received } = collect(bus, 'big', {
    filter: (payload) => (payload as { keep: boolean }).keep === true,
  });
  bus.publish('big', { keep: true, pad: 'x'.repeat(100) });
  bus.publish('big', { keep: false, pad: 'y'.repeat(100) });
  await flush();
  assert.equal(received.length, 1);
  assert.equal((received[0].payload as { keep: boolean }).keep, true);
  assert.equal(bus.getStats().filteredMessages, 1);
});

test('filter: applies to durable-log replay without queue churn or gaps', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-filter-'));
  const bus = new EventBus({ durableLogDir: dir });
  for (let n = 1; n <= 4; n++) bus.publish('t', { n });
  await flush();
  const bus2 = new EventBus({ durableLogDir: dir });
  const { received } = collect(bus2, 't', {
    resumeFromSeq: 0,
    filter: (payload) => (payload as { n: number }).n % 2 === 0,
  });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n),
    [2, 4],
  );
  assert.equal(bus2.getStats().filteredMessages, 2);
  assert.equal(bus2.getStats().sequenceGaps, 0);
  // Live publishes after the replay keep working with the filter.
  bus2.publish('t', { n: 5 });
  bus2.publish('t', { n: 6 });
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n),
    [2, 4, 6],
  );
  assert.equal(bus2.getStats().sequenceGaps, 0);
});

test('filter: replay of compressed records filters on the raw payload', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-filter-cmp-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('t', { thresholdBytes: 10 });
  bus.publish('t', { keep: true, pad: 'x'.repeat(100) });
  bus.publish('t', { keep: false, pad: 'y'.repeat(100) });
  await flush();
  const bus2 = new EventBus({ durableLogDir: dir });
  const { received } = collect(bus2, 't', {
    resumeFromSeq: 0,
    filter: (payload) => (payload as { keep: boolean }).keep === true,
  });
  await flush();
  assert.equal(received.length, 1);
  // The accepted replayed message still inflates transparently.
  assert.equal((received[0].payload as { keep: boolean }).keep, true);
  assert.equal(bus2.getStats().filteredMessages, 1);
});

test('filter: absent by default, and publish return counts only accepted', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't');
  const accepted = bus.publish('t', { n: 1 });
  await flush();
  assert.equal(accepted, 1);
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().filteredMessages, 0);
});
