import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type AdmissionRejectReason,
  type BusMessage,
} from '../src/bus.ts';

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

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('setPublisherRateLimit rejects invalid publisher identities', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setPublisherRateLimit('', { messagesPerSec: 1 }), RangeError);
  assert.throws(() => bus.setPublisherRateLimit('has space', { messagesPerSec: 1 }), RangeError);
  assert.throws(() => bus.setPublisherRateLimit('a'.repeat(65), { messagesPerSec: 1 }), RangeError);
  // The 64-character boundary is valid.
  bus.setPublisherRateLimit('a'.repeat(64), { messagesPerSec: 1 });
  assert.equal(bus.clearPublisherRateLimit('a'.repeat(64)), true);
});

test('setPublisherRateLimit rejects invalid rate values', () => {
  const bus = new EventBus();
  for (const messagesPerSec of [0, -1, NaN, Infinity, -Infinity]) {
    assert.throws(
      () => bus.setPublisherRateLimit('svc', { messagesPerSec }),
      RangeError,
      `messagesPerSec=${messagesPerSec}`,
    );
  }
  for (const burst of [0, -1, NaN, Infinity]) {
    assert.throws(
      () => bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst }),
      RangeError,
      `burst=${burst}`,
    );
  }
  // A failed call leaves no rule behind.
  assert.equal(bus.clearPublisherRateLimit('svc'), false);
});

test('burst defaults to one second of budget', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 2 });
  bus.subscribe('t', () => {});

  assert.equal(bus.publish('t', 'a', { publisherId: 'svc' }), 1);
  assert.equal(bus.publish('t', 'b', { publisherId: 'svc' }), 1);
  assert.equal(bus.publish('t', 'c', { publisherId: 'svc' }), 0);
  assert.equal(bus.getStats().publisherRateLimited, 1);
});

test('over-budget publish is shed in admission: no seq consumed, no gap', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg);
  });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  assert.equal(bus.publish('t', 'first', { publisherId: 'svc' }), 1);
  assert.equal(bus.publish('t', 'second', { publisherId: 'svc' }), 0); // shed
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].seq, 1);
  const stats = bus.getStats();
  // The shed never became a message: lastSeq untouched, no gap visible.
  assert.equal(stats.topics.find((t) => t.topic === 't')?.lastSeq, 1);
  assert.equal(stats.topics.find((t) => t.topic === 't')?.publishedMessages, 1);
  assert.equal(stats.sequenceGaps, 0);
  assert.equal(stats.publisherRateLimited, 1);
  assert.equal(stats.topics.find((t) => t.topic === 't')?.publisherRateLimited, 1);
  // The topic rate-limit counter is untouched: this shed never reached it.
  assert.equal(stats.rateLimitedMessages, 0);
  // Only admitted publishes move the per-publisher counter (EB-65).
  assert.deepEqual(stats.publishers, [{ publisher: 'svc', publishedMessages: 1 }]);
});

test('admission hook fires with reason publisher-rate-limit and reconciles', () => {
  const clock = makeClock(9_000);
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  bus.publish('orders', 'a', { publisherId: 'svc' });
  bus.publish('orders', 'b', { publisherId: 'svc' });
  bus.publish('orders', 'c', { publisherId: 'svc' });
  assert.equal(sink.events.length, 2);
  const reasons: AdmissionRejectReason[] = sink.events.map((e) => e.reason);
  assert.deepEqual(reasons, ['publisher-rate-limit', 'publisher-rate-limit']);
  for (const event of sink.events) {
    assert.equal(event.topic, 'orders');
    assert.equal(event.publisher, 'svc');
    assert.equal(event.at, 9_000);
  }
  // Hook events reconcile exactly against getStats().
  assert.equal(sink.events.length, bus.getStats().publisherRateLimited);
});

test('anonymous publishes and unconfigured publishers are unlimited', () => {
  const bus = new EventBus();
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', 'a'), 1);
  assert.equal(bus.publish('t', 'b'), 1);
  assert.equal(bus.publish('t', 'c'), 1);
  assert.equal(bus.publish('t', 'd', { publisherId: 'other' }), 1);
  assert.equal(bus.publish('t', 'e', { publisherId: 'other' }), 1);
  assert.equal(bus.getStats().publisherRateLimited, 0);
});

test('the bucket is keyed by publisher identity, across topics', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  bus.subscribe('t1', () => {});
  bus.subscribe('t2', () => {});
  assert.equal(bus.publish('t1', 'a', { publisherId: 'svc' }), 1);
  // Same publisher on another topic shares the budget: shed.
  assert.equal(bus.publish('t2', 'b', { publisherId: 'svc' }), 0);
  assert.equal(bus.getStats().publisherRateLimited, 1);
  assert.equal(bus.getStats().topics.find((t) => t.topic === 't2')?.publisherRateLimited, 1);
});

test('the injected clock refills the bucket deterministically', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', 'a', { publisherId: 'svc' }), 1);
  assert.equal(bus.publish('t', 'b', { publisherId: 'svc' }), 0);
  clock.advance(999);
  assert.equal(bus.publish('t', 'c', { publisherId: 'svc' }), 0);
  clock.advance(1);
  assert.equal(bus.publish('t', 'd', { publisherId: 'svc' }), 1);
  assert.equal(bus.getStats().publisherRateLimited, 2);
});

test('re-setting a rule replaces it and resets only that publisher budget', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  bus.setPublisherRateLimit('other', { messagesPerSec: 1, burst: 1 });
  bus.subscribe('t', () => {});

  assert.equal(bus.publish('t', 'a', { publisherId: 'svc' }), 1);
  assert.equal(bus.publish('t', 'b', { publisherId: 'other' }), 1);
  assert.equal(bus.publish('t', 'c', { publisherId: 'svc' }), 0);
  // Re-setting resets svc's budget; other's half-spent bucket is untouched.
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  assert.equal(bus.publish('t', 'd', { publisherId: 'svc' }), 1);
  assert.equal(bus.publish('t', 'e', { publisherId: 'other' }), 0);
  assert.equal(bus.getStats().publisherRateLimited, 2);
});

test('clearPublisherRateLimit removes the rule; unknown id is a no-op', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  assert.equal(bus.clearPublisherRateLimit('nobody'), false);

  assert.equal(bus.clearPublisherRateLimit(''), false); // invalid: never matched
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  assert.equal(bus.publish('t', 'a', { publisherId: 'svc' }), 1);
  assert.equal(bus.publish('t', 'b', { publisherId: 'svc' }), 0);
  assert.equal(bus.clearPublisherRateLimit('svc'), true);
  assert.equal(bus.clearPublisherRateLimit('svc'), false);
  assert.equal(bus.publish('t', 'c', { publisherId: 'svc' }), 1);
  assert.equal(bus.getStats().publisherRateLimited, 1);
});

test('publishBatch sheds over-budget entries one by one', async () => {
  const bus = new EventBus({ now: makeClock().now });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg);
  });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 2, burst: 2 });
  const accepted = bus.publishBatch([
    { topic: 't', payload: 'a', publisherId: 'svc' },
    { topic: 't', payload: 'b', publisherId: 'svc' },
    { topic: 't', payload: 'c', publisherId: 'svc' },
    { topic: 't', payload: 'd' }, // anonymous: unaffected
  ]);
  assert.equal(accepted, 3);
  await flush();
  assert.deepEqual(received.map((m) => m.seq), [1, 2, 3]);
  assert.equal(bus.getStats().publisherRateLimited, 1);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('publishAtomic rejects the whole batch on an over-budget entry', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  bus.subscribe('t1', () => {});
  bus.subscribe('t2', () => {});
  const result = bus.publishAtomic([
    { topic: 't1', payload: 'a', publisherId: 'svc' },
    { topic: 't2', payload: 'b', publisherId: 'svc' },
  ]);
  assert.deepEqual(result, {
    published: 0,
    rejected: { index: 1, topic: 't2', reason: 'publisher-rate-limit' },
  });
  // Shadow admission created no state: no seq, no budget, no log.
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 0);
  assert.equal(stats.publisherRateLimited, 1);
  assert.equal(stats.topics.find((t) => t.topic === 't2')?.lastSeq, 0);
  // The live budget is untouched: a lone publish still passes.
  assert.equal(bus.publish('t1', 'ok', { publisherId: 'svc' }), 1);
});

test('publishAtomic shadow budget spans entries of one publisher', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 2, burst: 2 });
  // Two entries fit the burst; the third would overdraft the publisher.
  const ok = bus.publishAtomic([
    { topic: 't', payload: 'a', publisherId: 'svc' },
    { topic: 't', payload: 'b', publisherId: 'svc' },
  ]);
  assert.deepEqual(ok, { published: 2 });
  const rejected = bus.publishAtomic([
    { topic: 't', payload: 'c', publisherId: 'svc' },
    { topic: 't', payload: 'd', publisherId: 'svc' },
    { topic: 't', payload: 'e', publisherId: 'svc' },
  ]);
  assert.equal(rejected.published, 0);
  assert.equal(rejected.rejected?.reason, 'publisher-rate-limit');
});

test('publishDelayed fails fast at schedule time and spends the token there', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg);
  });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  // delayMs: 0 fans out on the next flush — the admitted schedule already
  // spent the token, so the due-time fan-out burns no budget again.
  const id = bus.publishDelayed('t', 'a', { delayMs: 0, publisherId: 'svc' });
  assert.ok(typeof id === 'string');
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 60_000, publisherId: 'svc' }), undefined);
  assert.equal(bus.getStats().publisherRateLimited, 1);
  assert.equal(bus.getStats().pendingDelayed, 0);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].payload, 'a');
});

test('publishIdempotent: duplicates burn no budget, a shed claims no dedup slot', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 2, burst: 2 });
  bus.subscribe('t', () => {});

  assert.deepEqual(bus.publishIdempotent('t', 'a', { messageId: 'm1', publisherId: 'svc' }), {
    duplicate: false,
    accepted: 1,
  });
  // Retries of the duplicate burn no publisher budget...
  for (let i = 0; i < 5; i++) {
    assert.deepEqual(bus.publishIdempotent('t', 'a', { messageId: 'm1', publisherId: 'svc' }), {
      duplicate: true,
      accepted: 0,
    });
  }
  // ...so the second real message still has budget left.
  assert.deepEqual(bus.publishIdempotent('t', 'b', { messageId: 'm2', publisherId: 'svc' }), {
    duplicate: false,
    accepted: 1,
  });
  // Budget now empty: a new message is shed, not admitted.
  assert.deepEqual(bus.publishIdempotent('t', 'c', { messageId: 'm3', publisherId: 'svc' }), {
    duplicate: false,
    accepted: 0,
  });
  assert.equal(bus.getStats().publisherRateLimited, 1);
  // The shed claimed no dedup slot: after a refill the retry is fresh.
  clock.advance(60_000);
  assert.deepEqual(bus.publishIdempotent('t', 'c', { messageId: 'm3', publisherId: 'svc' }), {
    duplicate: false,
    accepted: 1,
  });
});

test('publisher shed runs before the topic rate-limit budget', () => {
  const sink = makeSink();
  const bus = new EventBus({ now: makeClock().now, onAdmissionRejected: sink.hook });
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  bus.subscribe('t', () => {});
  bus.subscribe('other', () => {});

  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  // Spend the publisher budget on another topic; the topic budget for
  // 't' stays full.
  assert.equal(bus.publish('other', 'x', { publisherId: 'svc' }), 1);
  // The publisher gate sheds first — the topic bucket is never touched.
  assert.equal(bus.publish('t', 'y', { publisherId: 'svc' }), 0);
  assert.deepEqual(sink.events.map((e) => e.reason), ['publisher-rate-limit']);
  assert.equal(bus.getStats().rateLimitedMessages, 0);
  // Proof the topic budget survived: an anonymous publish still passes.
  assert.equal(bus.publish('t', 'z'), 1);
  assert.equal(bus.getStats().publisherRateLimited, 1);
});
