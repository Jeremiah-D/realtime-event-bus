import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('messages carry a per-topic sequence number starting at 1', async () => {
  const bus = new EventBus();
  const seen: Array<{ topic: string; seq: number }> = [];
  bus.subscribe('market.*', (msg: BusMessage) => seen.push({ topic: msg.topic, seq: msg.seq }));
  bus.publish('market.btc', 'a');
  bus.publish('market.btc', 'b');
  bus.publish('market.eth', 'c');
  bus.publish('market.btc', 'd');
  await flush();
  // Counters are per concrete topic: btc runs 1..3 while eth has its own 1.
  assert.deepEqual(seen, [
    { topic: 'market.btc', seq: 1 },
    { topic: 'market.btc', seq: 2 },
    { topic: 'market.eth', seq: 1 },
    { topic: 'market.btc', seq: 3 },
  ]);
});

test('publishBatch assigns consecutive sequence numbers in batch order', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  bus.publishBatch([
    { topic: 't', payload: 'a' },
    { topic: 't', payload: 'b' },
    { topic: 't', payload: 'c' },
  ]);
  await flush();
  assert.deepEqual(seqs, [1, 2, 3]);
});

test('getStats exposes lastSeq per topic', async () => {
  const bus = new EventBus();
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  bus.publish('a', 1);
  bus.publish('a', 2);
  bus.publish('b', 1);
  await flush();
  const stats = bus.getStats();
  const byTopic = Object.fromEntries(stats.topics.map((t) => [t.topic, t]));
  assert.equal(byTopic['a'].lastSeq, 2);
  assert.equal(byTopic['b'].lastSeq, 1);
  assert.equal(byTopic['a'].sequenceGaps, 0);
  assert.equal(stats.sequenceGaps, 0);
});

test('messages dropped by backpressure are counted as sequence gaps', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq), { queueSize: 2 });
  bus.publish('t', 'first');
  await flush(); // baseline: seq 1 delivered, so no gap can be counted yet
  // Three rapid publishes into a capacity-2 queue: the oldest (seq 2) is
  // evicted, leaving seqs 3 and 4 for the next flush.
  bus.publish('t', 'p2');
  bus.publish('t', 'p3');
  bus.publish('t', 'p4');
  await flush();
  assert.deepEqual(received, [1, 3, 4]);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].sequenceGaps, 1); // exactly seq 2 went missing
  assert.equal(stats.sequenceGaps, 1);
});

test('sequence gaps aggregate across subscribers of a topic', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { queueSize: 2 });
  bus.subscribe('t', () => {}, { queueSize: 2 });
  bus.publish('t', 'baseline');
  await flush();
  bus.publish('t', 'p2');
  bus.publish('t', 'p3');
  bus.publish('t', 'p4');
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.topics[0].sequenceGaps, 2); // one lost message per subscriber
  assert.equal(stats.sequenceGaps, 2);
});

test('gap tracking is per topic for wildcard subscribers', async () => {
  const bus = new EventBus();
  const received: Array<[string, number]> = [];
  bus.subscribe('market.*', (msg) => received.push([msg.topic, msg.seq]), { queueSize: 2 });
  bus.publish('market.btc', 'b1');
  bus.publish('market.eth', 'e1');
  await flush(); // baselines: btc -> 1, eth -> 1
  // Overflow the shared queue with btc traffic only; the oldest (btc seq 2)
  // is evicted while the eth baseline is untouched.
  bus.publish('market.btc', 'b2');
  bus.publish('market.btc', 'b3');
  bus.publish('market.btc', 'b4');
  await flush();
  assert.deepEqual(received, [
    ['market.btc', 1],
    ['market.eth', 1],
    ['market.btc', 3],
    ['market.btc', 4],
  ]);
  const stats = bus.getStats();
  const byTopic = Object.fromEntries(stats.topics.map((t) => [t.topic, t]));
  assert.equal(byTopic['market.btc'].sequenceGaps, 1);
  assert.equal(byTopic['market.eth'].sequenceGaps, 0);
});

test('redelivered messages are not counted as sequence gaps', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribeReliable('t', (delivery) => {
    seen.push(delivery.msg.seq);
    if (delivery.redeliveries === 0 && delivery.msg.seq === 1) {
      delivery.nack(); // requeued at the tail; arrives again after seq 2
      return;
    }
    delivery.ack();
  });
  bus.publish('t', 'a'); // seq 1
  bus.publish('t', 'b'); // seq 2
  await flush();
  await flush(); // let the requeued delivery land
  assert.deepEqual(seen, [1, 2, 1]);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].sequenceGaps, 0);
  assert.equal(stats.sequenceGaps, 0);
});

test('TTL-expired messages surface as sequence gaps on the next delivery', async () => {
  const clock = { nowMs: 1_000, now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq));
  bus.setTopicTtl('t', 100); // deadline = publishTime + 100
  bus.publish('t', 'a'); // seq 1, deadline t=1100
  await flush(); // delivered at t=1000: baseline seq 1
  bus.publish('t', 'b'); // seq 2, deadline t=1100
  clock.nowMs = 1_200;
  await flush(); // seq 2 expires before the drain
  bus.publish('t', 'c'); // seq 3, deadline t=1300
  await flush(); // delivered: last was 1, seq 3 skips one number
  assert.deepEqual(received, [1, 3]);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].expiredMessages, 1);
  assert.equal(stats.topics[0].sequenceGaps, 1);
});

test('a subscriber that joins late does not count earlier messages as gaps', async () => {
  const bus = new EventBus();
  bus.publish('t', 'unseen-1'); // seq 1, no subscribers: never queued
  bus.publish('t', 'unseen-2'); // seq 2, no subscribers: never queued
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq));
  bus.publish('t', 'seen-3');
  bus.publish('t', 'seen-4');
  await flush();
  assert.deepEqual(received, [3, 4]);
  const stats = bus.getStats();
  // The first delivery establishes the baseline; pre-subscription history
  // is not the subscriber's loss to report.
  assert.equal(stats.topics[0].sequenceGaps, 0);
  assert.equal(stats.topics[0].lastSeq, 4);
});
