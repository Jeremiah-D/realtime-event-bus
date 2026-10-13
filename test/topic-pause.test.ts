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
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so pause/TTL tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-pause-'));

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

const bytesOf = (payload: unknown): number => Buffer.byteLength(JSON.stringify(payload), 'utf8');

test('shed mode: publish returns 0, counts pausedMessages, hook reason topic-paused', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  bus.subscribe('t', () => {});
  bus.pauseTopic('t');
  assert.equal(bus.publish('t', { a: 1 }), 0);
  assert.equal(bus.publish('t', { a: 2 }), 0);
  assert.equal(sink.events.length, 2);
  for (const event of sink.events) {
    assert.equal(event.topic, 't');
    assert.equal(event.reason, 'topic-paused');
  }
  assert.equal(sink.events[0].payloadBytes, bytesOf({ a: 1 }));
  const reasons: AdmissionRejectReason[] = sink.events.map((e) => e.reason);
  assert.deepEqual(reasons, ['topic-paused', 'topic-paused']);
  const stats = bus.getStats();
  assert.equal(stats.pausedMessages, 2);
  assert.equal(stats.topics[0].pausedMessages, 2);
  // A dedicated counter — not the pre-admission rejection counter.
  assert.equal(stats.rejectedMessages, 0);
  assert.equal(stats.topics[0].rejectedMessages, 0);
});

test('shed mode: the shed consumes a sequence number, so subscribers see a gap', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  assert.equal(bus.publish('t', 'a'), 1); // seq 1
  bus.pauseTopic('t');
  assert.equal(bus.publish('t', 'b'), 0); // shed: consumes seq 2
  assert.equal(bus.resumeTopic('t'), true);
  assert.equal(bus.publish('t', 'c'), 1); // seq 3
  await flush();
  assert.deepEqual(seqs, [1, 3]);
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 't')!;
  assert.equal(topic.publishedMessages, 3); // the shed counts as published, like a rate-limit shed
  assert.equal(topic.lastSeq, 3);
  assert.equal(topic.sequenceGaps, 1);
  assert.equal(stats.sequenceGaps, 1);
  assert.equal(topic.pausedMessages, 1);
});

test('pause is the first gate: schema never runs and publisher budget is untouched', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  bus.subscribe('t', () => {});
  let schemaCalls = 0;
  bus.setTopicSchema('t', () => {
    schemaCalls += 1;
    return true;
  });
  bus.setPublisherRateLimit('p1', { messagesPerSec: 1, burst: 1 });
  bus.pauseTopic('t');
  // A payload that would fail schema, from a budgeted publisher: the
  // pause gate fires first — no schema verdict, no publisher token.
  assert.equal(bus.publish('t', { bad: true }, { publisherId: 'p1' }), 0);
  assert.equal(bus.publish('t', { bad: true }, { publisherId: 'p1' }), 0);
  assert.equal(schemaCalls, 0);
  assert.deepEqual(
    sink.events.map((e) => e.reason),
    ['topic-paused', 'topic-paused'],
  );
  assert.equal(bus.getStats().rejectedMessages, 0);
  // The publisher budget survived the pause untouched.
  assert.equal(bus.resumeTopic('t'), true);
  assert.equal(bus.publish('t', { ok: true }, { publisherId: 'p1' }), 1);
  assert.equal(bus.publish('t', { ok: true }, { publisherId: 'p1' }), 0);
  assert.equal(sink.events[2].reason, 'publisher-rate-limit');
});

test('queue mode requires a durable log', () => {
  const bus = new EventBus();
  assert.throws(() => bus.pauseTopic('t', { mode: 'queue' }), RangeError);
  // Shed mode works without one.
  bus.pauseTopic('t');
  assert.equal(bus.publish('t', 'x'), 0);
  assert.equal(bus.resumeTopic('t'), true);
});

test('queue mode: publishes buffer with no seq or budget burned, resume fans out in order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  const received: unknown[] = [];
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
    seqs.push(msg.seq);
  });
  bus.pauseTopic('t', { mode: 'queue' });
  assert.equal(bus.publish('t', 'm1'), 0);
  assert.equal(bus.publish('t', 'm2'), 0);
  await flush();
  assert.deepEqual(received, []); // held, not delivered
  // No sequence numbers burned while buffering — and, like a delayed
  // schedule, nothing was admitted, so the topic has no stats entry yet.
  const stats = bus.getStats();
  assert.equal(stats.topics.length, 0);
  assert.equal(stats.totalPublished, 0);
  assert.equal(stats.pausedMessages, 0); // buffered, not shed
  assert.deepEqual(stats.pausedTopics, [{ topicPattern: 't', mode: 'queue', bufferedMessages: 2 }]);
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  // Original order preserved; sequence numbers drawn at resume, in order.
  assert.deepEqual(received, ['m1', 'm2']);
  assert.deepEqual(seqs, [1, 2]);
  assert.deepEqual(bus.getStats().pausedTopics, []);
});

test('queue mode: a message that expires while buffered is counted expired at resume, never delivered', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // deadline t=1100 for the buffered message
  bus.pauseTopic('t', { mode: 'queue' });
  assert.equal(bus.publish('t', 'doomed'), 0);
  clock.nowMs = 1_200; // past the deadline while still buffered
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  assert.deepEqual(received, []); // never resurrected
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics.length, 0); // never admitted: no per-topic entry
  assert.equal(stats.totalPublished, 0);
});

test('queue mode: resume preserves the pause-time TTL deadline instead of extending it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // buffered at t=1000 -> deadline t=1100
  bus.pauseTopic('t', { mode: 'queue' });
  assert.equal(bus.publish('t', 'm1'), 0);
  clock.nowMs = 1_050;
  assert.equal(bus.resumeTopic('t'), true); // fans out; a fresh deadline would be t=1150
  clock.nowMs = 1_120; // past the original deadline, before a recomputed one
  await flush();
  // The original deadline held: expired at drain, never delivered.
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('queue mode: resume before the deadline delivers normally', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100);
  bus.pauseTopic('t', { mode: 'queue' });
  bus.publish('t', 'fresh');
  clock.nowMs = 1_050; // before the t=1100 deadline
  bus.resumeTopic('t');
  await flush();
  assert.deepEqual(received, ['fresh']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('exact-topic rule wins over patterns; earliest-registered pattern wins', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.subscribe('**', () => {});
  const cacheBefore = bus.getStats().patternCacheSize;
  bus.pauseTopic('orders.*'); // shed
  bus.pauseTopic('orders.eu', { mode: 'queue' }); // exact beats pattern
  bus.pauseTopic('a.**', { mode: 'queue' }); // earliest pattern...
  bus.pauseTopic('a.b.**'); // ...beats the later one
  // 'orders.eu' matches the exact queue rule, not the shed pattern.
  assert.equal(bus.publish('orders.eu', 'x'), 0);
  // 'orders.us' matches only the shed pattern.
  assert.equal(bus.publish('orders.us', 'y'), 0);
  // 'a.b.c' matches both patterns: the earliest-registered ('a.**',
  // queue) wins over the later shed rule.
  assert.equal(bus.publish('a.b.c', 'z'), 0);
  const stats = bus.getStats();
  assert.equal(stats.pausedMessages, 1); // only the orders.us shed
  const rules = new Map(stats.pausedTopics.map((r) => [r.topicPattern, r]));
  assert.equal(rules.get('orders.eu')?.bufferedMessages, 1);
  assert.equal(rules.get('a.**')?.bufferedMessages, 1);
  assert.equal(rules.get('a.b.**')?.bufferedMessages, 0);
  // Pause configuration uses its own matcher cache — the subscriber
  // pattern cache did not grow.
  assert.equal(stats.patternCacheSize, cacheBefore);
});

test('publishBatch: paused entries shed individually, the batch is not atomic', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.subscribe('u', (msg) => received.push(msg.payload));
  bus.pauseTopic('t');
  const accepted = bus.publishBatch([
    { topic: 't', payload: 'shed' },
    { topic: 'u', payload: 'kept' },
  ]);
  assert.equal(accepted, 1);
  await flush();
  assert.deepEqual(received, ['kept']);
  assert.equal(bus.getStats().pausedMessages, 1);
});

test('publishAtomic: a paused-shed entry rejects the whole batch with zero side effects', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  bus.subscribe('t', () => {});
  bus.pauseTopic('t');
  const result = bus.publishAtomic([
    { topic: 't', payload: 'a' },
    { topic: 't', payload: 'b' },
  ]);
  assert.deepEqual(result, {
    published: 0,
    rejected: { index: 0, topic: 't', reason: 'topic-paused' },
  });
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].reason, 'topic-paused');
  const stats = bus.getStats();
  assert.equal(stats.pausedMessages, 1);
  assert.equal(stats.totalPublished, 0);
  assert.equal(stats.topics[0].lastSeq, 0); // atomic rejections consume no seq
});

test('publishAtomic: a queue-paused entry also rejects the batch (cannot commit a queued message)', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.subscribe('t', () => {});
  bus.pauseTopic('t', { mode: 'queue' });
  const result = bus.publishAtomic([{ topic: 't', payload: 'a' }]);
  assert.deepEqual(result, {
    published: 0,
    rejected: { index: 0, topic: 't', reason: 'topic-paused' },
  });
  // Nothing was buffered: the batch failed instead of partially queueing.
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 0);
  assert.equal(bus.getStats().pausedMessages, 1);
});

test('publishDelayed: shed mode fails fast at schedule time', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  bus.pauseTopic('t');
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000 });
  assert.equal(id, undefined);
  assert.equal(bus.getStats().pendingDelayed, 0);
  assert.equal(bus.getStats().pausedMessages, 1);
  assert.equal(sink.events[0].reason, 'topic-paused');
});

test('publishDelayed: queue mode accepts the schedule, defers the due-time fan-out into the pause buffer', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.pauseTopic('t', { mode: 'queue' });
  const id = bus.publishDelayed('t', 'deferred', { delayMs: 1000 });
  assert.match(id as string, /^delayed-\d+$/);
  assert.equal(bus.getStats().pendingDelayed, 1);
  clock.nowMs = 2_000;
  bus.publish('other', 'poke'); // the flush sweep finds the due entry paused
  await flush();
  // Not fanned out while paused: it moved into the pause buffer instead.
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().pendingDelayed, 0);
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 1);
  bus.resumeTopic('t');
  await flush();
  assert.deepEqual(received, ['deferred']);
});

test('publishDelayed: a schedule accepted before the pause is shed at due time in shed mode', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
    seqs.push(msg.seq);
  });
  bus.publishDelayed('t', 'x', { delayMs: 1000 }); // accepted while unpaused
  bus.pauseTopic('t'); // shed
  clock.nowMs = 2_000;
  bus.publish('other', 'poke');
  await flush();
  assert.deepEqual(received, []); // shed at due time, never delivered
  const stats = bus.getStats();
  assert.equal(stats.pendingDelayed, 0);
  assert.equal(stats.pausedMessages, 1);
  // The due-time shed consumed a sequence number, like a live shed.
  assert.equal(stats.topics[0].lastSeq, 1);
});

test('publishIdempotent: the pause check runs before the dedup claim', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.pauseTopic('t');
  // Paused: no dedup slot claimed.
  assert.deepEqual(bus.publishIdempotent('t', 'p', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 0,
  });
  assert.equal(bus.getStats().pausedMessages, 1);
  assert.equal(bus.resumeTopic('t'), true);
  // The retry is a fresh publish — the paused attempt claimed nothing.
  assert.deepEqual(bus.publishIdempotent('t', 'p', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  await flush();
  assert.deepEqual(received, ['p']);
});

test('publishIdempotent: a retry buffered twice under queue mode still delivers once', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.pauseTopic('t', { mode: 'queue' });
  assert.deepEqual(bus.publishIdempotent('t', 'p1', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 0,
  });
  assert.deepEqual(bus.publishIdempotent('t', 'p2', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 0,
  });
  assert.equal(bus.resumeTopic('t'), true);
  await flush();
  // The first buffered entry claimed the slot at resume; the retry was
  // suppressed as a duplicate.
  assert.deepEqual(received, ['p1']);
  assert.equal(bus.getStats().duplicateMessages, 1);
});

test('subaudit records topic_paused and topic_resumed with the mode', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.pauseTopic('orders.*', { mode: 'queue' });
  bus.publish('orders.eu', 'x');
  bus.resumeTopic('orders.*');
  const paused = bus.getSubscriptionAudit({ action: 'topic_paused' });
  assert.equal(paused.length, 1);
  assert.equal(paused[0].pattern, 'orders.*');
  assert.equal(paused[0].actor, 'system');
  assert.deepEqual(paused[0].detail, { mode: 'queue' });
  const resumed = bus.getSubscriptionAudit({ action: 'topic_resumed' });
  assert.equal(resumed.length, 1);
  assert.equal(resumed[0].pattern, 'orders.*');
  assert.deepEqual(resumed[0].detail, { mode: 'queue', drainedMessages: 1 });
  // Unknown actions still validate: the new actions are in the vocabulary.
  assert.doesNotThrow(() => bus.getSubscriptionAudit({ action: ['topic_paused', 'topic_resumed'] }));
});

test('prometheus exposes pause counters and the paused-topics gauge', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.pauseTopic('t');
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_paused_messages_total 2'), text);
  assert.ok(text.includes('eventbus_paused_topics 1'), text);
  assert.ok(text.includes('eventbus_topic_paused_messages_total{topic="t"} 2'), text);
  bus.resumeTopic('t');
  const after = renderPrometheus(bus.getStats());
  assert.ok(after.includes('eventbus_paused_topics 0'), after);
  assert.ok(after.includes('eventbus_paused_messages_total 2'), after);
});

test('resumeTopic on an unknown pattern is a no-op false; pauseTopic validates its arguments', () => {
  const bus = new EventBus();
  assert.equal(bus.resumeTopic('never-paused'), false);
  assert.throws(() => bus.pauseTopic(123 as unknown as string), TypeError);
  assert.throws(() => bus.pauseTopic(''), RangeError);
  assert.throws(() => bus.pauseTopic('t', { mode: 'bogus' as unknown as 'shed' }), RangeError);
  assert.throws(() => bus.pauseTopic('t', { mode: 42 as unknown as 'shed' }), TypeError);
  assert.equal(bus.getStats().pausedTopics.length, 0);
});

test('re-setting a queue rule keeps its buffer; the buffer drains on resume', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.pauseTopic('t', { mode: 'queue' });
  bus.publish('t', 'm1');
  // Re-setting the same key replaces the mode but keeps the data: the
  // buffered message is not silently dropped.
  bus.pauseTopic('t', { mode: 'queue' });
  assert.equal(bus.getStats().pausedTopics[0].bufferedMessages, 1);
  bus.resumeTopic('t');
  await flush();
  assert.deepEqual(received, ['m1']);
});

test('restart rebuilds the pause buffer: buffered messages fan out on the new bus, tombstoned afterwards', async () => {
  const dir = freshDir();
  const clock1 = controllableClock();
  const bus1 = new EventBus({ now: clock1.now, durableLogDir: dir });
  bus1.pauseTopic('t', { mode: 'queue' });
  bus1.publish('t', 'm1');
  bus1.publish('t', 'm2');
  assert.equal(bus1.getStats().pausedTopics[0].bufferedMessages, 2);
  // Restart: pause rules are runtime state and do not survive, so the
  // recovered buffer fans out immediately through full admission (the
  // delayed-delivery "already due" precedent).
  const clock2 = controllableClock();
  const bus2 = new EventBus({ now: clock2.now, durableLogDir: dir });
  const topic = bus2.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.publishedMessages, 2);
  assert.equal(topic.lastSeq, 2);
  assert.deepEqual(bus2.getStats().pausedTopics, []);
  // A further restart does not re-deliver: each drained entry was
  // tombstoned.
  const bus3 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  assert.equal(bus3.getStats().topics.find((t) => t.topic === 't')?.publishedMessages, 2);
});
