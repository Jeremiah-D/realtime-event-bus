import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so age-bound tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('a late subscriber immediately receives recent messages without a durable log', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('market.*', { lookbackCount: 10 });
  bus.publish('market.btc', 'a');
  bus.publish('market.btc', 'b');
  bus.publish('market.eth', 'c');
  const received: unknown[] = [];
  bus.subscribe('market.*', (msg) => received.push([msg.topic, msg.payload]));
  await flush();
  await flush();
  assert.deepEqual(received, [
    ['market.btc', 'a'],
    ['market.btc', 'b'],
    ['market.eth', 'c'],
  ]);
});

test('lookbackCount bounds the replay to the most recent messages', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('t', { lookbackCount: 2 });
  for (let i = 1; i <= 5; i++) bus.publish('t', i);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(received, [4, 5]);
  // The subscriber's sequence baseline advanced over the replayed
  // messages: live publishes afterwards produce no false gaps.
  bus.publish('t', 6);
  await flush();
  assert.deepEqual(received, [4, 5, 6]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('lookbackMs drops messages older than the age bound', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicLookback('t', { lookbackMs: 100 });
  bus.publish('t', 'old'); // at t=1000
  clock.nowMs = 1_150;
  bus.publish('t', 'new'); // at t=1150
  clock.nowMs = 1_200;
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(received, ['new']);
});

test('a topic without a matching rule retains nothing', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('other.*', { lookbackCount: 10 });
  bus.publish('t', 'x');
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(received, []);
  assert.deepEqual(bus.getStats().lookbackBuffers, { rules: 1, topics: 0, messages: 0 });
});

test('setTopicLookback rejects illegal options with RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicLookback('t', {} as never), RangeError);
  assert.throws(() => bus.setTopicLookback('t', { lookbackCount: 0 }), RangeError);
  assert.throws(() => bus.setTopicLookback('t', { lookbackCount: -1 }), RangeError);
  assert.throws(() => bus.setTopicLookback('t', { lookbackCount: 1.5 }), RangeError);
  assert.throws(() => bus.setTopicLookback('t', { lookbackMs: 0 }), RangeError);
  assert.throws(() => bus.setTopicLookback('t', { lookbackMs: -5 }), RangeError);
  assert.throws(() => bus.setTopicLookback('t', { lookbackMs: NaN }), RangeError);
  assert.throws(() => bus.setTopicLookback('t', { lookbackMs: Infinity }), RangeError);
  assert.throws(() => bus.setTopicLookback('', { lookbackCount: 1 }), RangeError);
  // A failed registration retains nothing.
  assert.deepEqual(bus.getStats().lookbackBuffers, { rules: 0, topics: 0, messages: 0 });
});

test('an exact-topic rule wins over a pattern rule', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('t.*', { lookbackCount: 10 });
  bus.setTopicLookback('t.a', { lookbackCount: 1 });
  bus.publish('t.a', 1);
  bus.publish('t.a', 2);
  bus.publish('t.b', 1);
  bus.publish('t.b', 2);
  const a: unknown[] = [];
  const b: unknown[] = [];
  bus.subscribe('t.a', (msg) => a.push(msg.payload));
  bus.subscribe('t.b', (msg) => b.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(a, [2]); // exact rule: last 1
  assert.deepEqual(b, [1, 2]); // pattern rule: last 10
});

test('clearTopicLookback drops the buffers of unmatched topics', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('t.*', { lookbackCount: 10 });
  bus.publish('t.a', 1);
  assert.equal(bus.getStats().lookbackBuffers.messages, 1);
  assert.equal(bus.clearTopicLookback('t.*'), true);
  assert.equal(bus.clearTopicLookback('t.*'), false);
  assert.deepEqual(bus.getStats().lookbackBuffers, { rules: 0, topics: 0, messages: 0 });
  const received: unknown[] = [];
  bus.subscribe('t.*', (msg) => received.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(received, []);
});

test('re-setting a rule re-trims existing buffers to the new bound', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('t', { lookbackCount: 10 });
  for (let i = 1; i <= 5; i++) bus.publish('t', i);
  bus.setTopicLookback('t', { lookbackCount: 2 });
  assert.equal(bus.getStats().lookbackBuffers.messages, 2);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(received, [4, 5]);
});

test('a TTL-expired buffered message is never resurrected for the late joiner', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicLookback('t', { lookbackCount: 10 });
  bus.setTopicTtl('t', 100); // deadline t=1100
  bus.publish('t', 'fresh');
  clock.nowMs = 1_200; // past the deadline when the late subscriber joins
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics[0].expiredMessages, 1);
});

test('the subscriber content filter applies to lookback replay', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('t', { lookbackCount: 10 });
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.publish('t', 3);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    filter: (payload) => (payload as number) % 2 === 1,
  });
  await flush();
  await flush();
  assert.deepEqual(received, [1, 3]);
  // Filtered-out replayed messages advance the baseline: no false gaps.
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('a durable resume cursor wins over lookback: history is never delivered twice', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-lookback-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicLookback('t', { lookbackCount: 10 });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  const received: unknown[] = [];
  // resumeFromSeq=0 replays the durable log; the in-memory lookback must
  // stay out so each message arrives exactly once.
  bus.subscribe('t', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  await flush();
  assert.deepEqual(received, ['a', 'b']);
});

test('compressed messages replay inflated to the late joiner', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('t', { lookbackCount: 10 });
  bus.setTopicCompression('t', { thresholdBytes: 10 });
  const payload = { tick: 'x'.repeat(200) };
  bus.publish('t', payload);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  await flush();
  await flush();
  assert.deepEqual(received, [payload]);
});

test('getStats exposes the lookback buffer state', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.getStats().lookbackBuffers, { rules: 0, topics: 0, messages: 0 });
  bus.setTopicLookback('a.*', { lookbackCount: 5 });
  bus.setTopicLookback('b', { lookbackMs: 1000 });
  bus.publish('a.1', 1);
  bus.publish('a.2', 2);
  bus.publish('b', 3);
  bus.publish('c', 4); // no rule: not retained
  assert.deepEqual(bus.getStats().lookbackBuffers, { rules: 2, topics: 3, messages: 3 });
});

test('a wildcard subscriber replays every matching topic in publish-time order', async () => {
  const bus = new EventBus();
  bus.setTopicLookback('**', { lookbackCount: 10 });
  bus.publish('a', 1);
  bus.publish('b', 2);
  bus.publish('a', 3);
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push([msg.topic, msg.payload]));
  await flush();
  await flush();
  assert.deepEqual(received, [
    ['a', 1],
    ['b', 2],
    ['a', 3],
  ]);
});
