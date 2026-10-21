/**
 * Bus-level global publish rate limit (EB-180).
 *
 * One token bucket shared by every publish on the bus — the backstop
 * under the per-topic (EB-19) and per-publisher (EB-79) limits.
 * Over-budget publishes are shed in the admission phase: never fanned
 * out, never logged, never queued, no sequence number consumed
 * (subscribers see no gap), no per-topic rate-limit token burned.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import {
  GlobalPublishRateLimiter,
  resolveGlobalPublishRateLimit,
} from '../src/global-rate-limit.ts';

/** Injectable clock so bucket refills are deterministic. */
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

test('disabled by default: unlimited publishes, zero counter', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 10; i++) assert.equal(bus.publish('t', i), 1);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  assert.equal(bus.getStats().globalRateLimitedMessages, 0);
});

test('over-budget publishes shed: publish returns 0, no gap, counted', async () => {
  const clock = makeClock();
  const rejections: Array<{ topic: string; reason: string; payloadBytes: number }> = [];
  const bus = new EventBus({
    now: clock.now,
    globalPublishRateLimit: { messagesPerSecond: 2, burst: 2 },
    onAdmissionRejected: (e) =>
      rejections.push({ topic: e.topic, reason: e.reason, payloadBytes: e.payloadBytes }),
  });
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));

  assert.equal(bus.publish('t', 'a'), 1);
  assert.equal(bus.publish('t', 'b'), 1);
  // Bucket exhausted: the third publish is shed at the publish side.
  assert.equal(bus.publish('t', 'c'), 0);
  await flush();

  assert.deepEqual(seqs, [1, 2]);
  const stats = bus.getStats();
  assert.equal(stats.globalRateLimitedMessages, 1);
  // No sequence number consumed by the shed: subscribers see no gap.
  assert.equal(stats.sequenceGaps, 0);
  assert.equal(rejections.length, 1);
  assert.equal(rejections[0].topic, 't');
  assert.equal(rejections[0].reason, 'global-rate-limit');
  assert.ok(rejections[0].payloadBytes > 0);
});

test('the bucket refills: shed publishes resume after the rate interval', async () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    globalPublishRateLimit: { messagesPerSecond: 2, burst: 2 },
  });
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 1);
  bus.publish('t', 2);
  assert.equal(bus.publish('t', 3), 0);
  clock.advance(1000); // 2 msg/s refills both tokens
  assert.equal(bus.publish('t', 4), 1);
  assert.equal(bus.publish('t', 5), 1);
  assert.equal(bus.publish('t', 6), 0);
  await flush();
  assert.deepEqual(received, [1, 2, 4, 5]);
  assert.equal(bus.getStats().globalRateLimitedMessages, 2);
});

test('global runs before the per-topic rate limit: no topic token burned', async () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    globalPublishRateLimit: { messagesPerSecond: 1, burst: 1 },
  });
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));

  assert.equal(bus.publish('t', 'a'), 1); // burns the single global and topic token
  // Both buckets are empty now; the global gate fires first (pre-seq),
  // so this shed consumes no sequence number — unlike a 'rate-limit'
  // shed, which would consume one.
  assert.equal(bus.publish('t', 'b'), 0);
  const stats = bus.getStats();
  assert.equal(stats.globalRateLimitedMessages, 1);
  assert.equal(stats.rateLimitedMessages, 0);
  assert.equal(stats.sequenceGaps, 0);
  clock.advance(1000); // both buckets refill
  assert.equal(bus.publish('t', 'c'), 1);
  await flush();
  // Contiguous seqs prove the global shed burned neither a seq nor the
  // per-topic budget (which was spent only by the admitted publishes).
  assert.deepEqual(seqs, [1, 2]);
});

test('global shed does not burn per-topic budget: later topic publishes still pass', async () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    globalPublishRateLimit: { messagesPerSecond: 1, burst: 1 },
  });
  bus.setTopicRateLimit('t', 1000);
  bus.subscribe('t', () => {});
  // The global shed fires before the per-topic gate: the topic bucket
  // keeps its tokens, so topic publishes still pass after the refill.
  assert.equal(bus.publish('t', 'a'), 1);
  assert.equal(bus.publish('t', 'b'), 0);
  assert.equal(bus.getStats().globalRateLimitedMessages, 1);
  assert.equal(bus.getStats().rateLimitedMessages, 0);
  clock.advance(1000);
  assert.equal(bus.publish('t', 'c'), 1);
  await flush();
});

test('publishAtomic: the whole batch is rejected when it exceeds the global budget', async () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    globalPublishRateLimit: { messagesPerSecond: 2, burst: 2 },
  });
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));

  const ok = bus.publishAtomic([
    { topic: 't', payload: 1 },
    { topic: 't', payload: 2 },
  ]);
  assert.equal(ok.published, 2);
  // The batch consumed both global tokens; a 3-entry batch no longer
  // fits — all-or-nothing rejection, zero side effects.
  const rejected = bus.publishAtomic([
    { topic: 't', payload: 3 },
    { topic: 't', payload: 4 },
    { topic: 't', payload: 5 },
  ]);
  assert.equal(rejected.published, 0);
  assert.equal(rejected.rejected?.reason, 'global-rate-limit');
  await flush();
  assert.deepEqual(received, [1, 2]);
  assert.equal(bus.getStats().globalRateLimitedMessages, 0);
  assert.equal(bus.getStats().sequenceGaps, 0);
  // The committed batch consumed its tokens: the next publish sheds.
  assert.equal(bus.publish('t', 6), 0);
  assert.equal(bus.getStats().globalRateLimitedMessages, 1);
});

test('previewPublish reports the global gate without consuming budget', () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    globalPublishRateLimit: { messagesPerSecond: 1, burst: 1 },
  });
  bus.subscribe('t', () => {});
  const first = bus.previewPublish('t', 'a');
  assert.equal(first.admitted, true);
  assert.equal(bus.publish('t', 'a'), 1);
  const preview = bus.previewPublish('t', 'b');
  assert.equal(preview.admitted, false);
  assert.equal(preview.reason, 'global-rate-limit');
  assert.equal(preview.failedGate, 'global-rate-limit');
  // The preview burned nothing: after the refill the publish passes.
  clock.advance(1000);
  assert.equal(bus.previewPublish('t', 'c').admitted, true);
  assert.equal(bus.publish('t', 'c'), 1);
});

test('invalid configuration throws RangeError at construction', () => {
  const bad: Array<unknown> = [
    { messagesPerSecond: 0 },
    { messagesPerSecond: -5 },
    { messagesPerSecond: Number.NaN },
    { messagesPerSecond: Number.POSITIVE_INFINITY },
    { messagesPerSecond: 10, burst: 0 },
    { messagesPerSecond: 10, burst: -1 },
    { messagesPerSecond: 10, burst: Number.NaN },
    'fast',
    42,
    [],
  ];
  for (const config of bad) {
    assert.throws(
      () => new EventBus({ globalPublishRateLimit: config as never }),
      RangeError,
      `expected RangeError for ${JSON.stringify(config)}`,
    );
  }
  // The resolver mirrors the constructor validation.
  assert.throws(() => resolveGlobalPublishRateLimit({ messagesPerSecond: 0 }), RangeError);
  assert.equal(resolveGlobalPublishRateLimit(undefined), undefined);
  assert.deepEqual(resolveGlobalPublishRateLimit({ messagesPerSecond: 10 }), {
    messagesPerSecond: 10,
    burst: 10,
  });
});

test('GlobalPublishRateLimiter: take/availableTokens against an injected clock', () => {
  const clock = makeClock();
  const limiter = new GlobalPublishRateLimiter({ messagesPerSecond: 1, burst: 1 }, clock.now);
  assert.equal(limiter.tryTake(), true);
  assert.equal(limiter.tryTake(), false);
  assert.ok(limiter.availableTokens < 1);
  clock.advance(1000);
  assert.ok(limiter.availableTokens >= 1);
  assert.equal(limiter.tryTake(), true);
});

test('metrics exposition renders the global shed counter', async () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    globalPublishRateLimit: { messagesPerSecond: 1, burst: 1 },
  });
  bus.subscribe('t', () => {});
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  const exposition = renderPrometheus(bus.getStats());
  assert.ok(
    exposition.includes('eventbus_global_rate_limited_messages_total 1'),
    'expected the global shed series',
  );
  await flush();
});
