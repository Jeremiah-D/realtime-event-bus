import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import type { Delivery } from '../src/ack.ts';

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
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-dedupwin-'));

test('subscriber dedup: same messageId delivered once, identity visible on the envelope', async () => {
  const bus = new EventBus();
  const deduped: BusMessage[] = [];
  const plain: BusMessage[] = [];
  bus.subscribe('pay.orders', (msg) => deduped.push(msg), { deduplicateMessages: true });
  bus.subscribe('pay.orders', (msg) => plain.push(msg));
  // Plain publish carries no publish-side suppression: the same identity
  // fans out twice, and only the dedup-enabled subscriber collapses it.
  bus.publish('pay.orders', { id: 1 }, { messageId: 'm1' });
  bus.publish('pay.orders', { id: 1 }, { messageId: 'm1' });
  await flush();
  assert.equal(deduped.length, 1);
  assert.equal(deduped[0].messageId, 'm1');
  assert.equal(plain.length, 2);
  assert.equal(plain[0].messageId, 'm1');
});

test('suppressed duplicates count dedupDropped and never surface as sequence gaps', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: true });
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'b', { messageId: 'm2' });
  await flush();
  assert.deepEqual(received.map((m) => m.seq), [1, 4]);
  assert.equal(bus.getStats().dedupDropped, 2);
  assert.equal(bus.getStats().topics.find((t) => t.topic === 't')?.dedupDropped, 2);
  // The duplicates were already-delivered messages, not lost ones.
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('messages without a messageId are unaffected by the dedup window', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: true });
  bus.publish('t', 'a');
  bus.publish('t', 'a');
  bus.publish('t', 'a', { messageId: '' }); // empty identity: treated as absent
  await flush();
  assert.equal(received.length, 3);
  assert.equal(bus.getStats().dedupDropped, 0);
});

test('dedup window is per subscriber: one subscriber’s sighting does not suppress another’s', async () => {
  const bus = new EventBus();
  const a: BusMessage[] = [];
  const b: BusMessage[] = [];
  bus.subscribe('t', (msg) => a.push(msg), { deduplicateMessages: true });
  bus.subscribe('t', (msg) => b.push(msg), { deduplicateMessages: true });
  bus.publish('t', 'x', { messageId: 'm1' });
  bus.publish('t', 'x', { messageId: 'm1' });
  await flush();
  assert.equal(a.length, 1);
  assert.equal(b.length, 1);
  assert.equal(bus.getStats().dedupDropped, 2);
});

test('window expiry: an aged-out messageId is unknown and delivered again', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: { windowMs: 1000 } });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  clock.advance(999);
  bus.publish('t', 'a', { messageId: 'm1' }); // still inside the window
  await flush();
  clock.advance(2); // 1001ms after the first delivery: the window lapsed
  bus.publish('t', 'a', { messageId: 'm1' }); // unknown again: delivered
  await flush();
  assert.equal(received.length, 2);
  assert.equal(bus.getStats().dedupDropped, 1);
});

test('maxEntries bounds the window: the oldest identity is evicted first', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: { maxEntries: 2 } });
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'b', { messageId: 'm2' });
  bus.publish('t', 'c', { messageId: 'm3' }); // evicts m1
  bus.publish('t', 'c', { messageId: 'm3' }); // retained: suppressed
  bus.publish('t', 'a', { messageId: 'm1' }); // evicted: delivered again
  await flush();
  assert.deepEqual(received.map((m) => m.payload), ['a', 'b', 'c', 'a']);
  assert.equal(bus.getStats().dedupDropped, 1);
});

test('ack-timeout redelivery is suppressed within the window', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      // Never ack: the delivery times out and would be requeued.
    },
    { ackTimeoutMs: 30, deduplicateMessages: true },
  );
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(deliveries.length, 1);
  await sleep(60);
  await flush();
  // The redelivery was suppressed: exactly-once within the window wins
  // over at-least-once.
  assert.equal(deliveries.length, 1);
  assert.equal(bus.getStats().dedupDropped, 1);
});

test('nack redelivery is suppressed within the window', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  let nacked = false;
  bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (!nacked) {
        nacked = true;
        d.nack();
      } else {
        d.ack();
      }
    },
    { deduplicateMessages: true },
  );
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  await flush();
  assert.equal(deliveries.length, 1);
  assert.equal(bus.getStats().dedupDropped, 1);
});

test('durable window: a restarted bus does not double-deliver on resume', async () => {
  const dir = freshDir();
  const clock = makeClock(1_000_000);
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  const seen1: BusMessage[] = [];
  bus1.subscribe('pay', (msg) => seen1.push(msg), {
    deduplicateMessages: { consumerId: 'worker-1' },
  });
  bus1.publish('pay', { id: 1 }, { messageId: 'm1' });
  await flush();
  assert.equal(seen1.length, 1);
  assert.ok(existsSync(join(dir, '__dedup.jsonl')), 'dedup journal written');

  // Crash and restart: same directory, same consumer identity.
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const seen2: BusMessage[] = [];
  bus2.subscribe('pay', (msg) => seen2.push(msg), {
    deduplicateMessages: { consumerId: 'worker-1' },
    resumeFromSeq: 0,
  });
  await flush();
  // m1 replays from the log but the rehydrated window suppresses it.
  assert.equal(seen2.length, 0);
  assert.equal(bus2.getStats().dedupDropped, 1);

  // A new message after the restart still flows.
  bus2.publish('pay', { id: 2 }, { messageId: 'm2' });
  await flush();
  assert.equal(seen2.length, 1);
  assert.equal(seen2[0].messageId, 'm2');
});

test('durable window: aged-out identities are not rehydrated', async () => {
  const dir = freshDir();
  const clock = makeClock(1_000_000);
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus1.subscribe('pay', () => {}, { deduplicateMessages: { consumerId: 'w', windowMs: 1000 } });
  bus1.publish('pay', { id: 1 }, { messageId: 'm1' });
  await flush();

  clock.advance(5000); // the window lapsed before the restart
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const seen2: BusMessage[] = [];
  bus2.subscribe('pay', (msg) => seen2.push(msg), {
    deduplicateMessages: { consumerId: 'w', windowMs: 1000 },
    resumeFromSeq: 0,
  });
  await flush();
  // Unknown again: the replayed message is delivered.
  assert.equal(seen2.length, 1);
  assert.equal(bus2.getStats().dedupDropped, 0);
});

test('durable window: a different consumerId starts with an empty window', async () => {
  const dir = freshDir();
  const bus1 = new EventBus({ durableLogDir: dir });
  bus1.subscribe('pay', () => {}, { deduplicateMessages: { consumerId: 'worker-1' } });
  bus1.publish('pay', { id: 1 }, { messageId: 'm1' });
  await flush();

  const bus2 = new EventBus({ durableLogDir: dir });
  const seen2: BusMessage[] = [];
  bus2.subscribe('pay', (msg) => seen2.push(msg), {
    deduplicateMessages: { consumerId: 'worker-2' },
    resumeFromSeq: 0,
  });
  await flush();
  // worker-2 never saw m1: the replay delivers it.
  assert.equal(seen2.length, 1);
  assert.equal(bus2.getStats().dedupDropped, 0);
});

test('publishIdempotent stamps the envelope identity for downstream dedup', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: true });
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  // The publish-side window already suppresses the retry; the envelope
  // identity is what the subscriber would dedup on for replays.
  bus.publishIdempotent('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].messageId, 'm1');
});

test('delayed publish carries the identity to fan-out time', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: true });
  bus.publishDelayed('t', 'a', { delayMs: 100, messageId: 'm1' });
  clock.advance(100);
  bus.publish('t', 'poke'); // any publish triggers the flush whose sweep fans out due entries
  await flush();
  assert.equal(received.length, 2);
  assert.equal(received[1].messageId, 'm1');
});

test('dedup counter is exposed in the Prometheus exposition', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { deduplicateMessages: true });
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_dedup_dropped_messages_total 1'));
});

test('invalid deduplicateMessages options throw RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribe('t', () => {}, { deduplicateMessages: { windowMs: 0 } }), RangeError);
  assert.throws(() => bus.subscribe('t', () => {}, { deduplicateMessages: { windowMs: -5 } }), RangeError);
  assert.throws(
    () => bus.subscribe('t', () => {}, { deduplicateMessages: { maxEntries: 0 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { deduplicateMessages: { maxEntries: 1.5 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { deduplicateMessages: { consumerId: '' } }),
    RangeError,
  );
});

test('consumerId without a durable log throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { deduplicateMessages: { consumerId: 'w1' } }),
    /durableLogDir/,
  );
  // Memory-only dedup without an identity is fine without a log.
  bus.subscribe('t', () => {}, { deduplicateMessages: true });
});
