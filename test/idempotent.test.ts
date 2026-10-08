import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Injectable clock so the dedup window is deterministic. */
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
const flush = () => new Promise((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-idem-'));

test('first publish admitted, immediate retry suppressed as duplicate', async () => {
  const bus = new EventBus({ now: makeClock().now });
  const received: BusMessage[] = [];
  bus.subscribe('pay.orders', (msg) => received.push(msg));
  const first = bus.publishIdempotent('pay.orders', { id: 1 }, { messageId: 'm1' });
  assert.deepEqual(first, { duplicate: false, accepted: 1 });
  const retry = bus.publishIdempotent('pay.orders', { id: 1 }, { messageId: 'm1' });
  assert.deepEqual(retry, { duplicate: true, accepted: 0 });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].seq, 1);
});

test('duplicate consumes no sequence number', async () => {
  const bus = new EventBus({ now: makeClock().now });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg));
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  bus.publishIdempotent('t', 'a', { messageId: 'm1' }); // suppressed
  bus.publish('t', 'b'); // takes the very next seq
  await flush();
  assert.deepEqual(received.map((m) => m.seq), [1, 2]);
  assert.deepEqual(received.map((m) => m.payload), ['a', 'b']);
});

test('duplicate is never written to the durable log', () => {
  const bus = new EventBus({ now: makeClock().now, durableLogDir: freshDir() });
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  const durable = bus.getStats().durableLog;
  assert.ok(durable != null);
  assert.equal(durable.entries, 1);
});

test('duplicate burns no rate-limit budget', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  bus.setTopicRateLimit('t', 10, { burst: 2 });
  const a = bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  assert.deepEqual(a, { duplicate: false, accepted: 1 });
  const dup = bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  assert.deepEqual(dup, { duplicate: true, accepted: 0 });
  // Both burst tokens are still effectively unspent by the duplicate: a
  // fresh messageId is admitted on the second token.
  const b = bus.publishIdempotent('t', 'b', { messageId: 'm2' });
  assert.deepEqual(b, { duplicate: false, accepted: 1 });
  assert.equal(bus.getStats().rateLimitedMessages, 0);
});

test('window expiry re-admits and restarts the window', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 1000 });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  assert.deepEqual(bus.publishIdempotent('t', 'a', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  clock.advance(1000); // exactly the window: expired
  assert.deepEqual(bus.publishIdempotent('t', 'a', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  // New window started at the re-admitted publish: immediate retry is a
  // duplicate again.
  assert.deepEqual(bus.publishIdempotent('t', 'a', { messageId: 'm1' }), {
    duplicate: true,
    accepted: 0,
  });
  await flush();
  assert.deepEqual(received, ['a', 'a']);
});

test('within the window the retry is a duplicate, at the boundary it expires', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 1000 });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  clock.advance(999);
  assert.equal(bus.publishIdempotent('t', 'a', { messageId: 'm1' }).duplicate, true);
  clock.advance(1);
  assert.equal(bus.publishIdempotent('t', 'a', { messageId: 'm1' }).duplicate, false);
});

test('same messageId on different topics is independent', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  assert.deepEqual(bus.publishIdempotent('a', 'x', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishIdempotent('b', 'x', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
});

test('different messageIds on the same topic are independent', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  for (const id of ['m1', 'm2', 'm3']) {
    assert.deepEqual(bus.publishIdempotent('t', id, { messageId: id }), {
      duplicate: false,
      accepted: 1,
    });
  }
  assert.deepEqual(bus.publishIdempotent('t', 'm1', { messageId: 'm1' }), {
    duplicate: true,
    accepted: 0,
  });
});

test('absent or empty messageId behaves exactly like publish', async () => {
  const bus = new EventBus({ now: makeClock().now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  assert.deepEqual(bus.publishIdempotent('t', 'a'), { duplicate: false, accepted: 1 });
  assert.deepEqual(bus.publishIdempotent('t', 'a', {}), { duplicate: false, accepted: 1 });
  assert.deepEqual(bus.publishIdempotent('t', 'a', { messageId: '' }), {
    duplicate: false,
    accepted: 1,
  });
  await flush();
  assert.deepEqual(received, ['a', 'a', 'a']);
});

test('maxEntries evicts the oldest entry', () => {
  const bus = new EventBus({
    now: makeClock().now,
    idempotencyMaxEntries: 2,
    idempotencyWindowMs: 60_000,
  });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  bus.publishIdempotent('t', 'b', { messageId: 'm2' });
  bus.publishIdempotent('t', 'c', { messageId: 'm3' }); // evicts m1
  assert.equal(bus.publishIdempotent('t', 'a', { messageId: 'm1' }).duplicate, false);
  // Re-admitting m1 evicted the next-oldest, m2.
  assert.equal(bus.publishIdempotent('t', 'c', { messageId: 'm3' }).duplicate, true);
  assert.equal(bus.publishIdempotent('t', 'b', { messageId: 'm2' }).duplicate, false);
});

test('schema-rejected first attempt does not suppress the retry', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  bus.setTopicSchema('t', (payload) => payload !== 'bad');
  const rejected = bus.publishIdempotent('t', 'bad', { messageId: 'm1' });
  assert.deepEqual(rejected, { duplicate: false, accepted: 0 });
  // The rejected attempt claimed no dedup slot: the fixed retry publishes.
  const retry = bus.publishIdempotent('t', 'good', { messageId: 'm1' });
  assert.deepEqual(retry, { duplicate: false, accepted: 1 });
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('rate-limit-shed first attempt does not suppress the retry', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  bus.publish('t', 'filler'); // burns the only token
  const shed = bus.publishIdempotent('t', 'x', { messageId: 'm1' });
  assert.deepEqual(shed, { duplicate: false, accepted: 0 });
  clock.advance(1000); // bucket refills
  const retry = bus.publishIdempotent('t', 'x', { messageId: 'm1' });
  assert.deepEqual(retry, { duplicate: false, accepted: 1 });
});

test('duplicateMessages stats are exposed per topic and globally', () => {
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 't')!;
  assert.equal(topic.duplicateMessages, 2);
  assert.equal(stats.duplicateMessages, 2);
  // Duplicates changed nothing else.
  assert.equal(topic.publishedMessages, 1);
  assert.equal(topic.lastSeq, 1);
  assert.equal(stats.totalPublished, 1);
  assert.equal(stats.rateLimitedMessages, 0);
  assert.equal(stats.rejectedMessages, 0);
});

test('constructor validates the idempotency options', () => {
  for (const windowMs of [0, -1, NaN, Infinity]) {
    assert.throws(
      () => new EventBus({ idempotencyWindowMs: windowMs }),
      RangeError,
      `windowMs=${windowMs}`,
    );
  }
  for (const maxEntries of [0, -5, 1.5, NaN, Infinity]) {
    assert.throws(
      () => new EventBus({ idempotencyMaxEntries: maxEntries }),
      RangeError,
      `maxEntries=${maxEntries}`,
    );
  }
  // Defaults are accepted and sane.
  const bus = new EventBus({ now: makeClock().now });
  bus.subscribe('t', () => {});
  assert.deepEqual(bus.publishIdempotent('t', 'a', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishIdempotent('t', 'a', { messageId: 'm1' }), {
    duplicate: true,
    accepted: 0,
  });
});
