import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so expiry tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('a message older than its TTL is dropped before delivery and counted as expired', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('market.*', (msg) => received.push(msg.payload));
  bus.setTopicTtl('market.*', 100); // published at t=1000, deadline t=1100
  bus.publish('market.btc', 'stale');
  clock.nowMs = 1_200; // past the deadline when the flush drains
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics[0].expiredMessages, 1);
  assert.equal(stats.topics[0].publishedMessages, 1); // publishing still counts
});

test('a message within its TTL is delivered normally', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('market.*', (msg) => received.push(msg.payload));
  bus.setTopicTtl('market.*', 100);
  bus.publish('market.btc', 'fresh');
  clock.nowMs = 1_050; // still before the t=1100 deadline
  await flush();
  assert.deepEqual(received, ['fresh']);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 0);
  assert.equal(stats.topics[0].expiredMessages, 0);
});

test('expiry deadline is inclusive: a message exactly at its deadline is expired, not delivered', async () => {
  // Semantics: a message is expired when now >= publishTime + ttlMs.
  // Hitting the deadline exactly counts as expired — "older than its TTL"
  // includes "exactly its TTL".
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // deadline t=1100
  bus.publish('t', 'boundary');
  clock.nowMs = 1_100; // exactly the deadline
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a topic without a TTL rule is never expired, however late the flush', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('plain', (msg) => received.push(msg.payload));
  // no setTopicTtl call for 'plain'
  bus.publish('plain', 'immortal');
  clock.nowMs = 9_999_999; // far in the future: still delivered
  await flush();
  assert.deepEqual(received, ['immortal']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('a topic rule does not leak onto unrelated topics', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: string[] = [];
  bus.subscribe('*', (msg) => received.push(msg.topic));
  bus.setTopicTtl('market.*', 100);
  bus.publish('market.btc', 'stale');
  bus.publish('news.btc', 'unaffected');
  clock.nowMs = 5_000;
  await flush();
  assert.deepEqual(received, ['news.btc']);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  const news = stats.topics.find((t) => t.topic === 'news.btc');
  assert.equal(news?.expiredMessages, 0);
});

test('an exact-topic rule wins over a matching pattern rule', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('market.btc', (msg) => received.push(msg.payload));
  bus.setTopicTtl('market.*', 100); // would expire at t=1100
  bus.setTopicTtl('market.btc', 10_000); // exact rule wins: expires at t=11000
  bus.publish('market.btc', 'protected');
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, ['protected']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('TTL works with publishBatch and expires each message independently', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100);
  bus.publishBatch([
    { topic: 't', payload: 'first' }, // published at t=1000
  ]);
  clock.nowMs = 1_150; // after the deadline of 'first'
  bus.publishBatch([{ topic: 't', payload: 'second' }]); // deadline t=1250
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, ['second']);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics[0].expiredMessages, 1);
  assert.equal(stats.topics[0].publishedMessages, 2);
});

test('expired copies count once per subscriber queue (fan-out aware)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.subscribe('t', () => {});
  bus.setTopicTtl('t', 100);
  bus.publish('t', 'stale');
  clock.nowMs = 5_000;
  await flush();
  // one published message, but two queue copies were discarded
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 2);
  assert.equal(stats.topics[0].expiredMessages, 2);
  assert.equal(stats.topics[0].publishedMessages, 1);
});

test('clearTopicTtl restores no-expiry behavior for the topic', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100);
  assert.equal(bus.clearTopicTtl('t'), true);
  assert.equal(bus.clearTopicTtl('t'), false); // nothing left to clear
  bus.publish('t', 'late');
  clock.nowMs = 5_000;
  await flush();
  assert.deepEqual(received, ['late']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('re-setting a TTL rule replaces it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100);
  bus.setTopicTtl('t', 10_000); // replaced: deadline t=11000
  bus.publish('t', 'replaced');
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, ['replaced']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('setTopicTtl validates its arguments', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicTtl('t', -1), RangeError);
  assert.throws(() => bus.setTopicTtl('t', NaN), RangeError);
  assert.throws(() => bus.setTopicTtl('t', Infinity), RangeError);
  assert.throws(() => bus.setTopicTtl('', 100), RangeError);
});

test('TTL configuration does not inflate patternCacheSize', () => {
  const bus = new EventBus();
  bus.subscribe('market.*', () => {});
  bus.setTopicTtl('market.btc', 100); // exact-topic rule, no pattern compiled
  bus.setTopicTtl('news.**', 100); // pattern rule, but kept in a separate cache
  assert.equal(bus.getStats().patternCacheSize, 1);
});

test('TTL does not change backpressure drop behavior', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  bus.setTopicTtl('t', 60_000); // generous TTL: nothing expires
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3'); // drop-oldest still sheds 'm1' when full
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  clock.nowMs = 1_001; // well within the TTL
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('expired counters start at zero on a fresh bus', () => {
  const bus = new EventBus();
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 0);
  bus.publish('t', 1); // no TTL configured
  assert.equal(bus.getStats().topics[0].expiredMessages, 0);
});
