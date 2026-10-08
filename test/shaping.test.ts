import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Injectable clock so token-bucket refills are deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const flush = () => new Promise((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('disabled by default: every queued message delivers in one flush', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().shapedSubscribers, 0);
});

test('over-budget messages wait in the queue instead of being dropped', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 2 },
  });
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', i), 1);
  await flush();
  // Burst of 2 delivered; the other 3 wait — nothing dropped.
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.pendingCount(sub.id), 3);
  assert.equal(bus.droppedCount(sub.id), 0);
  assert.equal(bus.getStats().shapedSubscribers, 1);
  sub.unsubscribe();
});

test('queued backlog drains in FIFO order as the bucket refills, with no gaps', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 2 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  clock.advance(100); // 10/s -> 1 token
  bus.publish('t', 5); // also triggers a flush
  await flush();
  assert.deepEqual(received, [0, 1, 2]);
  for (let n = 6; n <= 8; n++) {
    clock.advance(1000); // refill to the burst cap of 2
    bus.publish('t', n); // also triggers a flush
    await flush();
  }
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5, 6, 7, 8]);
  // Pacing never drops, so the gap detector stays silent.
  assert.equal(bus.getStats().sequenceGaps, 0);
  assert.equal(bus.getStats().shapedSubscribers, 0);
  sub.unsubscribe();
});

test('invalid shaping options throw RangeError and register nothing', () => {
  const bus = new EventBus();
  const bad = [
    { messagesPerSec: 0 },
    { messagesPerSec: -5 },
    { messagesPerSec: Infinity },
    { messagesPerSec: NaN },
    { messagesPerSec: 10, burst: 0 },
    { messagesPerSec: 10, burst: -2 },
    { messagesPerSec: 10, burst: Infinity },
  ];
  for (const deliveryShaping of bad) {
    assert.throws(() => bus.subscribe('t', () => {}, { deliveryShaping }), RangeError);
  }
  assert.equal(bus.subscriberCount(), 0);
});

test('deliveryShaping: true uses the documented defaults', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { deliveryShaping: true });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  // Default burst (100) covers all three.
  assert.deepEqual(received, [0, 1, 2]);
});

test('shaping does not extend TTL: messages that expire while waiting are dropped', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 50);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 1 },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0]);
  clock.advance(100); // the two waiting messages outlived their TTL
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(received, [0, 3]);
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.expiredMessages, 2);
});

test('shaping protects downstream rate, not memory: a full queue still drops per policy', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 3,
    deliveryShaping: { messagesPerSec: 10, burst: 1 },
  });
  for (let i = 0; i < 6; i++) bus.publish('t', i);
  await flush();
  // drop-oldest shed 0, 1, 2 at publish time; the paced drain then delivered 3.
  assert.deepEqual(received, [3]);
  assert.equal(bus.droppedCount(sub.id), 3);
  sub.unsubscribe();
});

test('re-flush timer drains the backlog with no new publishes (real clock)', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 20, burst: 2 },
  });
  for (let i = 0; i < 6; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.getStats().shapedSubscribers, 1);
  // No more publishes: the timer keeps flushing as tokens refill.
  await sleep(500); // ~10 tokens at 20/s; needs 4
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5]);
  assert.equal(bus.getStats().shapedSubscribers, 0);
});

test('shaping composes with reliable subscriptions', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribeReliable(
    't',
    (delivery) => {
      received.push(delivery.msg.payload);
      delivery.ack();
    },
    { deliveryShaping: { messagesPerSec: 10, burst: 1 } },
  );
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  // Burst of 1: only the first delivery went out; the rest pace behind it.
  assert.deepEqual(received, [0]);
  for (let n = 0; n < 2; n++) {
    clock.advance(1000); // one token per round (bucket capacity is 1)
    bus.publish('t', `pad-${n}`); // also triggers a flush
    await flush();
  }
  assert.deepEqual(received, [0, 1, 2]);
  sub.unsubscribe();
});

test('health-probe degradation mid-drain preserves FIFO order with a shaped backlog', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const attempts: unknown[] = [];
  let fail = true;
  const sub = bus.subscribe(
    't',
    (msg) => {
      attempts.push(msg.payload);
      if (fail) throw new Error('boom');
    },
    {
      healthProbe: { maxConsecutiveFailures: 2 },
      deliveryShaping: { messagesPerSec: 100, burst: 3 },
    },
  );
  for (let i = 0; i < 8; i++) bus.publish('t', i);
  await flush();
  // Burst of 3 dequeued; the first two throws trip the probe mid-drain and
  // the unattempted message is requeued ahead of the still-queued backlog.
  assert.deepEqual(attempts, [0, 1]);
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  fail = false;
  assert.equal(bus.resume(sub.id), true);
  await flush();
  // One token of budget left after the two takes: message 2 delivers now.
  assert.deepEqual(attempts, [0, 1, 2]);
  clock.advance(100); // refill to the burst cap
  bus.publish('t', 8);
  await flush();
  assert.deepEqual(attempts, [0, 1, 2, 3, 4, 5]);
  clock.advance(100);
  bus.publish('t', 9);
  await flush();
  assert.deepEqual(attempts, [0, 1, 2, 3, 4, 5, 6, 7, 8]);
  clock.advance(100);
  bus.publish('t', 10);
  await flush();
  assert.deepEqual(attempts, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  sub.unsubscribe();
});

test('publish return value is unaffected by shaping', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { deliveryShaping: { messagesPerSec: 1, burst: 1 } });
  // Shaping paces delivery; fan-out to the queue still accepts.
  assert.equal(bus.publish('t', 1), 1);
  assert.equal(bus.publish('t', 2), 1);
});

test('unsubscribing a shaping subscriber is clean', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { deliveryShaping: { messagesPerSec: 1, burst: 1 } });
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  assert.equal(bus.getStats().shapedSubscribers, 1);
  sub.unsubscribe();
  assert.equal(bus.getStats().shapedSubscribers, 0);
});
