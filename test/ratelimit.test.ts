import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Injectable clock so refill timing is deterministic. */
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

/** Bus with a no-op subscriber on every given topic (publish returns 1 per publish when unthrottled). */
function busOn(clock: { now: () => number }, ...topics: string[]): EventBus {
  const bus = new EventBus({ now: clock.now });
  for (const topic of topics) bus.subscribe(topic, () => {});
  return bus;
}

test('no rule means no rate limiting', () => {
  const bus = busOn(makeClock(), 't');
  for (let i = 0; i < 100; i++) assert.equal(bus.publish('t', i), 1);
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 100);
  assert.equal(stats.rateLimitedMessages, 0);
});

test('burst messages pass, excess is shed at the publish side', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRateLimit('hot', 10, { burst: 5 });
  const received: unknown[] = [];
  bus.subscribe('hot', (msg) => received.push(msg.payload));
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('hot', i), 1);
  // Sixth publish finds the bucket empty: shed, never fanned out.
  assert.equal(bus.publish('hot', 5), 0);
  assert.equal(bus.publish('hot', 6), 0);
  await flush();
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 'hot')!;
  assert.equal(topic.publishedMessages, 7);
  assert.equal(topic.rateLimitedMessages, 2);
  assert.equal(stats.rateLimitedMessages, 2);
  // The shed never reached a queue.
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
});

test('bucket refills at the configured rate', () => {
  const clock = makeClock();
  const bus = busOn(clock, 'hot');
  bus.setTopicRateLimit('hot', 10, { burst: 2 });
  assert.equal(bus.publish('hot', 'a'), 1);
  assert.equal(bus.publish('hot', 'b'), 1);
  assert.equal(bus.publish('hot', 'c'), 0); // empty
  clock.advance(100); // 10/s -> 1 token per 100ms
  assert.equal(bus.publish('hot', 'd'), 1);
  assert.equal(bus.publish('hot', 'e'), 0); // only one token refilled
  assert.equal(bus.getStats().rateLimitedMessages, 2);
});

test('default burst equals one second of budget', () => {
  const bus = busOn(makeClock(), 'hot');
  bus.setTopicRateLimit('hot', 4); // burst defaults to 4
  for (let i = 0; i < 4; i++) assert.equal(bus.publish('hot', i), 1);
  assert.equal(bus.publish('hot', 'over'), 0);
});

test('clearTopicRateLimit removes the limit', () => {
  const bus = busOn(makeClock(), 'hot');
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  assert.equal(bus.publish('hot', 'a'), 1);
  assert.equal(bus.publish('hot', 'b'), 0);
  assert.equal(bus.clearTopicRateLimit('hot'), true);
  assert.equal(bus.clearTopicRateLimit('hot'), false);
  assert.equal(bus.publish('hot', 'c'), 1);
  assert.equal(bus.publish('hot', 'd'), 1);
});

test('exact-topic rule wins over patterns; earliest pattern wins', () => {
  const clock = makeClock();
  const bus = busOn(clock, 'm.btc', 'm.eth');
  bus.setTopicRateLimit('m.**', 1, { burst: 1 });
  bus.setTopicRateLimit('m.btc', 100, { burst: 100 });
  assert.equal(bus.publish('m.btc', 1), 1);
  assert.equal(bus.publish('m.btc', 2), 1); // exact rule: generous
  assert.equal(bus.publish('m.eth', 1), 1);
  assert.equal(bus.publish('m.eth', 2), 0); // pattern rule: strict

  const clock2 = makeClock();
  const bus2 = busOn(clock2, 'm.btc');
  bus2.setTopicRateLimit('m.*', 1, { burst: 1 });
  bus2.setTopicRateLimit('m.**', 100, { burst: 100 });
  assert.equal(bus2.publish('m.btc', 1), 1);
  assert.equal(bus2.publish('m.btc', 2), 0); // earliest pattern wins
});

test('re-setting a rule resets the budget', () => {
  const bus = busOn(makeClock(), 'hot');
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  assert.equal(bus.publish('hot', 'a'), 1);
  assert.equal(bus.publish('hot', 'b'), 0);
  bus.setTopicRateLimit('hot', 1, { burst: 1 }); // fresh budget
  assert.equal(bus.publish('hot', 'c'), 1);
});

test('shed messages surface as sequence gaps downstream', async () => {
  const clock = makeClock();
  const bus = busOn(clock, 'hot');
  bus.setTopicRateLimit('hot', 2, { burst: 2 });
  bus.publish('hot', 'a'); // seq 1
  bus.publish('hot', 'b'); // seq 2
  bus.publish('hot', 'c'); // seq 3, shed
  bus.publish('hot', 'd'); // seq 4, shed
  clock.advance(1000); // refill 2
  bus.publish('hot', 'e'); // seq 5
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.sequenceGaps, 2); // seqs 3 and 4 skipped
  assert.equal(stats.rateLimitedMessages, 2);
});

test('publishBatch respects the rate limit', () => {
  const bus = busOn(makeClock(), 'hot');
  bus.setTopicRateLimit('hot', 10, { burst: 2 });
  const accepted = bus.publishBatch([
    { topic: 'hot', payload: 1 },
    { topic: 'hot', payload: 2 },
    { topic: 'hot', payload: 3 },
  ]);
  assert.equal(accepted, 2);
  assert.equal(bus.getStats().rateLimitedMessages, 1);
});

test('rate limit does not affect other topics', () => {
  const bus = busOn(makeClock(), 'hot', 'cold');
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  assert.equal(bus.publish('hot', 'a'), 1);
  assert.equal(bus.publish('hot', 'b'), 0);
  assert.equal(bus.publish('cold', 'x'), 1);
  assert.equal(bus.publish('cold', 'y'), 1);
  const stats = bus.getStats();
  assert.equal(stats.topics.find((t) => t.topic === 'cold')!.rateLimitedMessages, 0);
});

test('invalid rate-limit configuration throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicRateLimit('', 10), RangeError);
  assert.throws(() => bus.setTopicRateLimit('t', 0), RangeError);
  assert.throws(() => bus.setTopicRateLimit('t', -5), RangeError);
  assert.throws(() => bus.setTopicRateLimit('t', Number.POSITIVE_INFINITY), RangeError);
  assert.throws(() => bus.setTopicRateLimit('t', 10, { burst: 0 }), RangeError);
  assert.throws(() => bus.setTopicRateLimit('t', 10, { burst: Number.NaN }), RangeError);
});

test('rate-limited publish does not touch the durable log', async () => {
  const { mkdtemp, rm } = await import('node:fs/promises');
  const { tmpdir } = await import('node:os');
  const { join } = await import('node:path');
  const dir = await mkdtemp(join(tmpdir(), 'ratelimit-log-'));
  try {
    const clock = makeClock();
    const bus = new EventBus({ now: clock.now, durableLogDir: dir });
    bus.setTopicRateLimit('hot', 1, { burst: 1 });
    bus.subscribe('hot', () => {});
    bus.publish('hot', 'a'); // seq 1, logged
    bus.publish('hot', 'b'); // seq 2, shed: not logged
    const stats = bus.getStats();
    assert.equal(stats.durableLog!.entries, 1);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
