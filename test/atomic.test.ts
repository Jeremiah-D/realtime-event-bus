import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Injectable clock so rate-limit budgets stay deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

/** Accepts payloads shaped like { id: number }; rejects everything else. */
const orderValidator = (payload: unknown): boolean =>
  typeof payload === 'object' && payload !== null && typeof (payload as { id?: unknown }).id === 'number';

test('commits the whole batch across topics: subscribers see all messages', async () => {
  const bus = new EventBus();
  const a: unknown[] = [];
  const b: unknown[] = [];
  bus.subscribe('a', (msg) => a.push(msg.payload));
  bus.subscribe('b', (msg) => b.push(msg.payload));
  const res = bus.publishAtomic([
    { topic: 'a', payload: 1 },
    { topic: 'b', payload: 2 },
    { topic: 'a', payload: 3 },
  ]);
  assert.deepEqual(res, { published: 3 });
  await flush();
  assert.deepEqual(a, [1, 3]);
  assert.deepEqual(b, [2]);
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 3);
  assert.equal(stats.topics.find((t) => t.topic === 'a')!.lastSeq, 2);
  assert.equal(stats.topics.find((t) => t.topic === 'b')!.lastSeq, 1);
});

test('a schema rejection aborts the batch with zero side effects', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-atomic-'));
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: unknown[] = [];
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  bus.subscribe('news', (msg) => received.push(msg.payload));
  bus.setTopicSchema('orders', orderValidator);
  bus.setTopicRateLimit('orders', 100, { burst: 100 });
  const res = bus.publishAtomic([
    { topic: 'orders', payload: { id: 1 } },
    { topic: 'news', payload: 'x' },
    { topic: 'orders', payload: 'bad' }, // rejected
  ]);
  assert.deepEqual(res, { published: 0, rejected: { index: 2, topic: 'orders', reason: 'schema' } });
  await flush();
  // No subscriber saw any of the batch — not even the entries before the failure.
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 0);
  // The batch rejection is counted once against the failing entry's
  // admission-gate counters (EB-39 unified admission-rejection channel),
  // while everything delivery-side stays untouched.
  assert.equal(stats.rejectedMessages, 1);
  assert.equal(stats.rateLimitedMessages, 0);
  assert.equal(stats.topics.length, 1); // 'orders' stats entry from the rejection
  assert.equal(stats.durableLog?.entries, 0); // nothing written to the log
  // Sequence numbers were not consumed: the next publish starts at 1.
  const seqs: number[] = [];
  bus.subscribe('orders.seq', () => {});
  bus.subscribe('orders', (msg) => seqs.push(msg.seq));
  assert.equal(bus.publish('orders', { id: 2 }), 2); // both 'orders' subscribers accept
  await flush();
  assert.deepEqual(seqs, [1]);
});

test('a rate-limit shed aborts the batch and the budget is fully restored', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('hot', () => {});
  bus.setTopicRateLimit('hot', 10, { burst: 2 });
  const res = bus.publishAtomic([
    { topic: 'hot', payload: 1 },
    { topic: 'hot', payload: 2 },
    { topic: 'hot', payload: 3 }, // overdrafts the batch's own share of the budget
  ]);
  assert.deepEqual(res, { published: 0, rejected: { index: 2, topic: 'hot', reason: 'rate-limit' } });
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 0);
  // The batch rejection is counted once against the failing entry's
  // rate-limit counter (EB-39 unified admission-rejection channel).
  assert.equal(stats.rateLimitedMessages, 1);
  assert.equal(stats.topics.length, 1);
  // The shadow budget was never charged against the real bucket: the full
  // burst is still available for later publishes.
  assert.equal(bus.publish('hot', 'a'), 1);
  assert.equal(bus.publish('hot', 'b'), 1);
  assert.equal(bus.publish('hot', 'c'), 0); // now the bucket is genuinely empty
  assert.equal(bus.getStats().rateLimitedMessages, 2);
});

test('repeated same-topic entries share one shadow budget: an exact-fit batch commits', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const seqs: number[] = [];
  bus.subscribe('hot', (msg) => seqs.push(msg.seq));
  bus.setTopicRateLimit('hot', 10, { burst: 2 });
  assert.deepEqual(
    bus.publishAtomic([
      { topic: 'hot', payload: 1 },
      { topic: 'hot', payload: 2 },
    ]),
    { published: 2 },
  );
  await flush();
  // No gaps: a shed message would have consumed a sequence number.
  assert.deepEqual(seqs, [1, 2]);
});

test('the first failing entry is reported when several would fail', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.setTopicSchema('t', orderValidator);
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  const res = bus.publishAtomic([
    { topic: 't', payload: { id: 1 } }, // ok
    { topic: 't', payload: 'bad' }, // schema fails first
    { topic: 't', payload: { id: 3 } }, // would also shed: never reached
  ]);
  assert.deepEqual(res, { published: 0, rejected: { index: 1, topic: 't', reason: 'schema' } });
  assert.equal(bus.getStats().totalPublished, 0);
});

test('empty batch is a no-op', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  assert.deepEqual(bus.publishAtomic([]), { published: 0 });
  assert.equal(bus.getStats().totalPublished, 0);
  assert.equal(bus.getStats().topics.length, 0);
});

test('a throwing validator propagates and commits nothing', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('a', (msg) => received.push(msg.payload));
  bus.subscribe('b', (msg) => received.push(msg.payload));
  bus.setTopicSchema('b', () => {
    throw new Error('validator bug');
  });
  // The throw happens during admission — entry 0 was validated fine but
  // nothing was committed yet, so the batch leaves no trace.
  assert.throws(
    () =>
      bus.publishAtomic([
        { topic: 'a', payload: 1 },
        { topic: 'b', payload: 2 },
      ]),
    /validator bug/,
  );
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 0);
  assert.equal(stats.rejectedMessages, 0);
  assert.equal(stats.topics.length, 0);
});

test('TTL is drain-time, not an admission gate: a batch is never rejected for TTL', async () => {
  const clock = makeClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // deadlines at t=1100
  const res = bus.publishAtomic([
    { topic: 't', payload: 'first' },
    { topic: 't', payload: 'second' },
  ]);
  assert.deepEqual(res, { published: 2 }); // admitted despite the TTL rule
  clock.advance(200); // past both deadlines when the flush drains
  await flush();
  assert.deepEqual(received, []); // both expired at drain, independently
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 2);
  assert.equal(stats.topics[0].publishedMessages, 2); // publishing still counted
});

test('pattern-vs-exact rule resolution matches publish', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('m.btc', () => {});
  bus.subscribe('m.eth', () => {});
  bus.setTopicRateLimit('m.**', 1, { burst: 1 }); // strict pattern
  bus.setTopicRateLimit('m.btc', 100, { burst: 100 }); // generous exact rule wins
  // Exact rule: both entries fit the generous budget.
  assert.deepEqual(
    bus.publishAtomic([
      { topic: 'm.btc', payload: 1 },
      { topic: 'm.btc', payload: 2 },
    ]),
    { published: 2 },
  );
  // Pattern rule: the second entry overdrafts the strict budget.
  assert.deepEqual(
    bus.publishAtomic([
      { topic: 'm.eth', payload: 1 },
      { topic: 'm.eth', payload: 2 },
    ]),
    { published: 0, rejected: { index: 1, topic: 'm.eth', reason: 'rate-limit' } },
  );
});

test('subscribers receive messages in batch order with independent per-topic seqs', async () => {
  const bus = new EventBus();
  const seen: Array<{ topic: string; payload: unknown; seq: number }> = [];
  bus.subscribe('**', (msg) => seen.push({ topic: msg.topic, payload: msg.payload, seq: msg.seq }));
  bus.publishAtomic([
    { topic: 'a', payload: 'a1' },
    { topic: 'b', payload: 'b1' },
    { topic: 'a', payload: 'a2' },
  ]);
  await flush();
  assert.deepEqual(
    seen.map((s) => [s.topic, s.payload]),
    [
      ['a', 'a1'],
      ['b', 'b1'],
      ['a', 'a2'],
    ],
  );
  assert.deepEqual(
    seen.filter((s) => s.topic === 'a').map((s) => s.seq),
    [1, 2],
  );
  assert.deepEqual(
    seen.filter((s) => s.topic === 'b').map((s) => s.seq),
    [1],
  );
});

test('a committed batch is written to the durable log', () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-atomic-log-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  assert.deepEqual(
    bus.publishAtomic([
      { topic: 'a', payload: 1 },
      { topic: 'b', payload: 2 },
    ]),
    { published: 2 },
  );
  assert.equal(bus.getStats().durableLog?.entries, 2);
});

test('all messages land in subscriber queues before any delivery happens', () => {
  const bus = new EventBus();
  const subA = bus.subscribe('a', () => {});
  const subB = bus.subscribe('b', () => {});
  bus.publishAtomic([
    { topic: 'a', payload: 1 },
    { topic: 'b', payload: 2 },
  ]);
  // Fan-out ran synchronously in one turn; the flush (delivery) has not
  // run yet, so both queues already hold the full batch.
  assert.equal(bus.pendingCount(subA.id), 1);
  assert.equal(bus.pendingCount(subB.id), 1);
});

test('downstream drop policies still apply per committed message', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 1 });
  assert.deepEqual(
    bus.publishAtomic([
      { topic: 't', payload: 'm1' },
      { topic: 't', payload: 'm2' },
    ]),
    { published: 2 },
  );
  await flush();
  // Atomicity covers admission, not delivery: the queue's drop-oldest
  // policy shed the first message exactly as it would for two publishes.
  assert.deepEqual(received, ['m2']);
});
