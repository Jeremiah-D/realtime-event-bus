import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type AdmissionRejectReason,
} from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Collects hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

/** UTF-8 JSON byte size of a payload, as the admission gate measures it. */
const bytesOf = (payload: unknown): number => Buffer.byteLength(JSON.stringify(payload), 'utf8');

test('no rule means no size check', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const big = { data: 'x'.repeat(10_000) };
  assert.equal(bus.publish('t', big), 1);
  await flush();
  assert.deepEqual(received, [big]);
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('oversized publish is rejected with zero side effects', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    seqs.push(msg.seq);
    received.push(msg.payload);
  });
  // Cap: 10 bytes. {"a":"x"} is 9 bytes, {"a":"xy"} is 10 bytes.
  bus.setTopicMaxMessageBytes('t', 10);
  const small = { a: 'x' };
  const boundary = { a: 'xy' };
  const big = { a: 'xyz' };
  assert.equal(bytesOf(small), 9);
  assert.equal(bytesOf(boundary), 10);
  assert.equal(bytesOf(big), 11);
  assert.equal(bus.publish('t', small), 1);
  assert.equal(bus.publish('t', boundary), 1);
  assert.equal(bus.publish('t', big), 0);
  assert.equal(bus.publish('t', small), 1);
  await flush();
  // The rejection consumed no sequence number: the next publish kept
  // marching (1, 2, 3 — no gap).
  assert.deepEqual(seqs, [1, 2, 3]);
  assert.deepEqual(received, [small, boundary, small]);
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 't')!;
  assert.equal(topic.publishedMessages, 3);
  assert.equal(topic.rejectedMessages, 1);
  assert.equal(topic.lastSeq, 3);
  assert.equal(topic.sequenceGaps, 0);
  assert.equal(stats.rejectedMessages, 1);
  assert.equal(stats.totalPublished, 3);
});

test('rejection burns no rate-limit budget', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  bus.setTopicMaxMessageBytes('t', 10);
  const small = { a: 'x' };
  const big = { a: 'xyz' };
  // Oversized first: the size gate must not take the single token.
  assert.equal(bus.publish('t', big), 0);
  assert.equal(bus.getStats().topics[0].rejectedMessages, 1);
  assert.equal(bus.getStats().topics[0].rateLimitedMessages, 0);
  // The token is still there for a valid publish.
  assert.equal(bus.publish('t', small), 1);
  // Now the bucket is empty and the payload is small: the shed is a
  // rate-limit event, not a size event.
  assert.equal(bus.publish('t', small), 0);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].rejectedMessages, 1);
  assert.equal(stats.topics[0].rateLimitedMessages, 1);
});

test('oversized publish never touches the durable log', () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb64-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicMaxMessageBytes('t', 10);
  bus.publish('t', { a: 'x' });
  assert.equal(bus.getStats().durableLog?.entries, 1);
  bus.publish('t', { a: 'xyz' }); // rejected
  assert.equal(bus.getStats().durableLog?.entries, 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('hook event reason message-too-big reconciles with stats', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  bus.setTopicMaxMessageBytes('t', 10);
  const big = { a: 'xyz' };
  assert.equal(bus.publish('t', big), 0);
  assert.equal(bus.publish('t', big), 0);
  assert.equal(sink.events.length, 2);
  for (const event of sink.events) {
    assert.equal(event.topic, 't');
    assert.equal(event.reason, 'message-too-big');
    assert.equal(event.payloadBytes, bytesOf(big));
  }
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 2);
  assert.equal(stats.topics[0].rejectedMessages, 2);
  // The reason is a distinct member of the reason space, sharing only the
  // counter with the other pre-admission rejections.
  const reasons: AdmissionRejectReason[] = sink.events.map((e) => e.reason);
  assert.deepEqual(reasons, ['message-too-big', 'message-too-big']);
});

test('exact-topic rule wins over patterns; earliest pattern wins', () => {
  const bus = new EventBus();
  bus.subscribe('**', () => {});
  bus.setTopicMaxMessageBytes('orders.*', 10);
  bus.setTopicMaxMessageBytes('orders.eu', 1000);
  bus.setTopicMaxMessageBytes('a.**', 5);
  bus.setTopicMaxMessageBytes('a.b.**', 1000);
  const twenty = { data: 'x'.repeat(9) }; // 20 bytes
  // Exact beats pattern: 20 bytes fits 1000, not 10.
  assert.equal(bus.publish('orders.eu', twenty), 1);
  // Pattern applies: 20 bytes exceeds 10.
  assert.equal(bus.publish('orders.us', twenty), 0);
  // Earliest-registered matching pattern wins: 'a.*' (5) was registered
  // before 'a.b.*' (1000); a 20-byte payload matches both.
  assert.equal(bus.publish('a.b.c', twenty), 0);
  assert.equal(bus.getStats().rejectedMessages, 2);
});

test('clearTopicMaxMessageBytes removes the rule', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  assert.equal(bus.clearTopicMaxMessageBytes('t'), false);
  bus.setTopicMaxMessageBytes('t', 10);
  assert.equal(bus.publish('t', { a: 'xyz' }), 0);
  assert.equal(bus.clearTopicMaxMessageBytes('t'), true);
  assert.equal(bus.publish('t', { a: 'xyz' }), 1);
  assert.equal(bus.clearTopicMaxMessageBytes('t'), false);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('illegal values throw RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicMaxMessageBytes('', 10), RangeError);
  assert.throws(() => bus.setTopicMaxMessageBytes('t', 0), RangeError);
  assert.throws(() => bus.setTopicMaxMessageBytes('t', -5), RangeError);
  assert.throws(() => bus.setTopicMaxMessageBytes('t', 1.5), RangeError);
  assert.throws(() => bus.setTopicMaxMessageBytes('t', NaN), RangeError);
  assert.throws(() => bus.setTopicMaxMessageBytes('t', Infinity), RangeError);
  assert.throws(
    () => bus.setTopicMaxMessageBytes('t', '10' as unknown as number),
    RangeError,
  );
});

test('gate order: ACL, then schema, then size, then rate-limit', () => {
  const sink = makeSink();
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'secret', publish: 'deny' }] },
    onAdmissionRejected: sink.hook,
  });
  bus.subscribe('**', () => {});
  const rejectAll = (payload: unknown): boolean => false;
  const acceptAll = (payload: unknown): boolean => true;
  bus.setTopicSchema('orders', rejectAll);
  bus.setTopicSchema('plain', acceptAll);
  bus.setTopicMaxMessageBytes('orders', 10);
  bus.setTopicMaxMessageBytes('plain', 10);
  bus.setTopicRateLimit('plain', 1, { burst: 1 });
  // ACL fires before everything: small or not, a denied publish is 'acl'.
  assert.equal(bus.publish('secret', { a: 'x' }), 0);
  // Schema fires before the size cap: a schema-failing payload reports
  // 'schema' even when it is also oversized.
  assert.equal(bus.publish('orders', { a: 'x'.repeat(100) }), 0);
  // Size cap fires before rate limiting: oversized reports 'message-too-big'.
  assert.equal(bus.publish('plain', { a: 'x'.repeat(100) }), 0);
  assert.deepEqual(
    sink.events.map((e) => e.reason),
    ['acl', 'schema', 'message-too-big'],
  );
  assert.equal(bus.getStats().rejectedMessages, 3);
  assert.equal(bus.getStats().rateLimitedMessages, 0);
  // Rate limiting is untouched by the size rejection: the bucket still has
  // its token for a valid small publish.
  assert.equal(bus.publish('plain', { a: 'x' }), 1);
});

test('oversized first attempt claims no idempotency slot', () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicMaxMessageBytes('t', 10);
  const big = { a: 'xyz' };
  const small = { a: 'x' };
  const first = bus.publishIdempotent('t', big, { messageId: 'm1' });
  assert.deepEqual(first, { duplicate: false, accepted: 0 });
  // The retry is a fresh publish, never suppressed — the rejected attempt
  // claimed no dedup slot.
  const retry = bus.publishIdempotent('t', small, { messageId: 'm1' });
  assert.deepEqual(retry, { duplicate: false, accepted: 1 });
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('publishBatch: oversized entries are rejected individually, batch is not atomic', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  bus.setTopicMaxMessageBytes('t', 10);
  const small = { a: 'x' };
  const big = { a: 'xyz' };
  const accepted = bus.publishBatch([
    { topic: 't', payload: small },
    { topic: 't', payload: big },
    { topic: 't', payload: small },
  ]);
  assert.equal(accepted, 2);
  await flush();
  // No gap where the rejected entry would have been.
  assert.deepEqual(seqs, [1, 2]);
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 1);
  assert.equal(stats.topics[0].rejectedMessages, 1);
});

test('publishAtomic: one oversized entry aborts the batch with zero side effects', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicMaxMessageBytes('t', 10);
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  const small = { a: 'x' };
  const big = { a: 'xyz' };
  const result = bus.publishAtomic([
    { topic: 't', payload: small },
    { topic: 't', payload: big },
  ]);
  assert.deepEqual(result, {
    published: 0,
    rejected: { index: 1, topic: 't', reason: 'message-too-big' },
  });
  // Counted once against the failing entry's admission-gate counters, like
  // any other atomic batch rejection — and nothing else moved.
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].reason, 'message-too-big');
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 1);
  assert.equal(stats.totalPublished, 0);
  assert.equal(stats.rateLimitedMessages, 0);
  assert.equal(stats.topics.length, 1);
  assert.equal(stats.topics[0].lastSeq, 0);
  // The atomic rejection touched no rate-limit token: a later small publish
  // still has its budget.
  assert.equal(bus.publish('t', small), 1);
});

test('publishDelayed: oversized payload fails fast at schedule time', () => {
  const bus = new EventBus();
  bus.setTopicMaxMessageBytes('t', 10);
  const big = { a: 'xyz' };
  const id = bus.publishDelayed('t', big, { delayMs: 1000 });
  assert.equal(id, undefined);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('unserializable payloads measure 0 bytes and never trip the cap', () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicMaxMessageBytes('t', 1);
  // JSON.stringify(undefined) is undefined -> measured as 0 bytes.
  assert.equal(bus.publish('t', undefined), 1);
  // A circular structure is not serializable -> measured as 0 bytes, so
  // the size gate passes it (the durable log's own rules still apply).
  const circular: { self?: unknown } = {};
  circular.self = circular;
  assert.equal(bus.publish('t', circular), 1);
  assert.equal(bus.getStats().rejectedMessages, 0);
});
