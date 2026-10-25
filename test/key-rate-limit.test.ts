import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type BusMessage,
} from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Injectable clock so the token-bucket refill is deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-key-rate-limit-'));

test('per-key independence: one hot key sheds while another key flows', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  bus.subscribe('orders', (msg) => {
    received.push(msg);
  });
  bus.setKeyRateLimit('orders', { messagesPerSec: 1, burst: 1 });

  assert.equal(bus.publish('orders', 'hot-1', { key: 'hot' }), 1);
  assert.equal(bus.publish('orders', 'hot-2', { key: 'hot' }), 0); // shed
  assert.equal(bus.publish('orders', 'cold-1', { key: 'cold' }), 1); // own bucket
  assert.equal(bus.publish('orders', 'hot-3', { key: 'hot' }), 0); // still shed
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    ['hot-1', 'cold-1'],
  );
  // Refill at 1/s: the hot key flows again, the cold key is untouched.
  clock.advance(1000);
  assert.equal(bus.publish('orders', 'hot-4', { key: 'hot' }), 1);
  assert.equal(bus.publish('orders', 'hot-5', { key: 'hot' }), 0);
  const stats = bus.getStats();
  assert.equal(stats.keyRateLimited, 3);
  assert.equal(stats.topics.find((t) => t.topic === 'orders')?.keyRateLimited, 3);
});

test('the same key on different topics has independent buckets', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('orders.a', () => {});
  bus.subscribe('orders.b', () => {});
  bus.setKeyRateLimit('orders.*', { messagesPerSec: 1, burst: 1 });

  assert.equal(bus.publish('orders.a', 'a1', { key: 'u1' }), 1);
  assert.equal(bus.publish('orders.a', 'a2', { key: 'u1' }), 0);
  assert.equal(bus.publish('orders.b', 'b1', { key: 'u1' }), 1); // other topic
  assert.equal(bus.getStats().keyRateLimited, 1);
});

test('unkeyed messages are unaffected by a key rule', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  bus.setKeyRateLimit('t', { messagesPerSec: 1, burst: 1 });

  for (let i = 0; i < 10; i++) assert.equal(bus.publish('t', i), 1);
  // A keyed message on a topic with no rule is unaffected too.
  bus.subscribe('other', () => {});
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('other', i, { key: 'k' }), 1);
  assert.equal(bus.getStats().keyRateLimited, 0);
});

test('a key shed consumes no seq and writes nothing to the durable log', async () => {
  const clock = makeClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg);
  });
  bus.setKeyRateLimit('t', { messagesPerSec: 1, burst: 1 });

  assert.equal(bus.publish('t', 'first', { key: 'k' }), 1);
  assert.equal(bus.publish('t', 'shed', { key: 'k' }), 0);
  clock.advance(1000); // refill one token
  assert.equal(bus.publish('t', 'second', { key: 'k' }), 1);
  await flush();
  assert.deepEqual(
    received.map((m) => m.seq),
    [1, 2],
  );
  assert.deepEqual(
    received.map((m) => m.payload),
    ['first', 'second'],
  );
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.lastSeq, 2);
  assert.equal(topic.publishedMessages, 2);
  assert.equal(bus.getStats().sequenceGaps, 0);
  const log = DurableTopicLog.open({ dir });
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.payload),
    ['first', 'second'],
  );
  assert.equal(bus.getStats().durableLog?.entries, 2);
});

test('key sheds count in keyRateLimited and fire onAdmissionRejected with reason key-rate-limit', () => {
  const clock = makeClock(9_000);
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.setKeyRateLimit('orders', { messagesPerSec: 1, burst: 1 });
  bus.publish('orders', 'a', { key: 'k' });
  bus.publish('orders', 'b', { key: 'k' });
  bus.publish('orders', 'c', { key: 'k' });
  assert.equal(sink.events.length, 2);
  assert.deepEqual(
    sink.events.map((e) => e.reason),
    ['key-rate-limit', 'key-rate-limit'],
  );
  for (const event of sink.events) {
    assert.equal(event.topic, 'orders');
    assert.equal(event.at, 9_000);
  }
  // Hook events reconcile exactly against getStats().
  assert.equal(sink.events.length, bus.getStats().keyRateLimited);
  assert.equal(
    sink.events.length,
    bus.getStats().topics.find((t) => t.topic === 'orders')?.keyRateLimited,
  );
});

test('setKeyRateLimit rejects invalid config with RangeError and leaves no rule behind', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setKeyRateLimit('', { messagesPerSec: 1 }), RangeError);
  for (const messagesPerSec of [0, -1, NaN, Infinity, -Infinity]) {
    assert.throws(
      () => bus.setKeyRateLimit('t', { messagesPerSec }),
      RangeError,
      `messagesPerSec=${messagesPerSec}`,
    );
  }
  for (const burst of [0, -1, NaN, Infinity]) {
    assert.throws(
      () => bus.setKeyRateLimit('t', { messagesPerSec: 1, burst }),
      RangeError,
      `burst=${burst}`,
    );
  }
  assert.throws(
    // @ts-expect-error deliberately invalid opts shape
    () => bus.setKeyRateLimit('t', null),
    RangeError,
  );
  // A failed call leaves no rule behind.
  assert.equal(bus.clearKeyRateLimit('t'), false);
});

test('burst defaults to one second of budget; re-setting resets the budget', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  bus.setKeyRateLimit('t', { messagesPerSec: 2 }); // burst defaults to 2
  assert.equal(bus.publish('t', 'a', { key: 'k' }), 1);
  assert.equal(bus.publish('t', 'b', { key: 'k' }), 1);
  assert.equal(bus.publish('t', 'c', { key: 'k' }), 0);
  bus.setKeyRateLimit('t', { messagesPerSec: 2 }); // fresh budget
  assert.equal(bus.publish('t', 'd', { key: 'k' }), 1);
  assert.equal(bus.clearKeyRateLimit('t'), true);
  assert.equal(bus.clearKeyRateLimit('t'), false);
  assert.equal(bus.publish('t', 'e', { key: 'k' }), 1);
});

test('combination with the per-topic limit: each shed counts exactly once', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  bus.subscribe('hot', (msg) => {
    received.push(msg);
  });
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  bus.setKeyRateLimit('hot', { messagesPerSec: 1, burst: 1 });

  // First keyed publish consumes one topic token and one key token.
  assert.equal(bus.publish('hot', 'a', { key: 'k' }), 1);
  // Both budgets are now empty, but the key gate runs first in
  // admission — so this shed counts ONLY as a key shed, never as a
  // topic shed, and burns no topic token it did not already spend.
  assert.equal(bus.publish('hot', 'b', { key: 'k' }), 0);
  let stats = bus.getStats();
  assert.equal(stats.keyRateLimited, 1);
  assert.equal(stats.rateLimitedMessages, 0);
  // Unkeyed publishes skip the key gate and reach the topic gate: the
  // topic shed consumes a sequence number (a visible gap), unlike the
  // key shed above, and counts ONLY as a topic shed.
  assert.equal(bus.publish('hot', 'c'), 0);
  stats = bus.getStats();
  assert.equal(stats.keyRateLimited, 1);
  assert.equal(stats.rateLimitedMessages, 1);
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    ['a'],
  );
  assert.equal(stats.topics.find((t) => t.topic === 'hot')?.lastSeq, 2);
});

test('exact-topic rule wins over patterns; earliest pattern wins', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('m.btc', () => {});
  bus.subscribe('m.eth', () => {});
  bus.setKeyRateLimit('m.**', { messagesPerSec: 1, burst: 1 });
  bus.setKeyRateLimit('m.btc', { messagesPerSec: 100, burst: 100 });
  assert.equal(bus.publish('m.btc', 1, { key: 'k' }), 1);
  assert.equal(bus.publish('m.btc', 2, { key: 'k' }), 1); // exact rule: generous
  assert.equal(bus.publish('m.eth', 1, { key: 'k' }), 1);
  assert.equal(bus.publish('m.eth', 2, { key: 'k' }), 0); // pattern rule: strict
});

test('publishBatch sheds over-budget keyed entries one by one', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  bus.setKeyRateLimit('t', { messagesPerSec: 1, burst: 1 });
  assert.equal(
    bus.publishBatch([
      { topic: 't', payload: 'a', key: 'k' },
      { topic: 't', payload: 'b', key: 'k' },
      { topic: 't', payload: 'c', key: 'other' },
    ]),
    2,
  );
  assert.equal(bus.getStats().keyRateLimited, 1);
});

test('publishAtomic and previewPublish see the key gate in shadow admission', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  bus.setKeyRateLimit('t', { messagesPerSec: 1, burst: 1 });
  // The shadow verdict reports the key gate without consuming budget.
  assert.deepEqual(bus.previewPublish('t', 'x', { key: 'k' }), { admitted: true });
  assert.deepEqual(bus.previewPublish('t', 'x', { key: 'k' }), { admitted: true });
  // An atomic batch over the key budget aborts whole-batch, counted once.
  bus.publish('t', 'first', { key: 'k' });
  assert.deepEqual(bus.previewPublish('t', 'x', { key: 'k' }), {
    admitted: false,
    reason: 'key-rate-limit',
    failedGate: 'key-rate-limit',
  });
  assert.deepEqual(
    bus.publishAtomic([
      { topic: 't', payload: 'a', key: 'other' },
      { topic: 't', payload: 'b', key: 'k' },
    ]),
    { published: 0, rejected: { index: 1, topic: 't', reason: 'key-rate-limit' } },
  );
  assert.equal(bus.getStats().keyRateLimited, 1);
  // A committed atomic batch consumes exactly one key token per entry.
  assert.deepEqual(bus.publishAtomic([{ topic: 't', payload: 'a', key: 'other' }]), {
    published: 1,
  });
  assert.equal(bus.publish('t', 'late', { key: 'other' }), 0);
  assert.equal(bus.getStats().keyRateLimited, 2);
});

test('publishDelayed fails fast at schedule time on an over-budget key', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg);
  });
  bus.setKeyRateLimit('t', { messagesPerSec: 1, burst: 1 });
  assert.notEqual(bus.publishDelayed('t', 'first', { delayMs: 0, key: 'k' }), undefined);
  assert.equal(bus.publishDelayed('t', 'shed', { delayMs: 0, key: 'k' }), undefined);
  assert.equal(bus.getStats().keyRateLimited, 1);
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    ['first'],
  );
});
