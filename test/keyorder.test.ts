import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Reads every keyed log record's keySeq from a durable-log directory. */
function keySeqsOnDisk(dir: string): number[] {
  const seqs: number[] = [];
  for (const f of readdirSync(dir).filter((f) => f.endsWith('.log'))) {
    for (const line of readFileSync(join(dir, f), 'utf8').split('\n')) {
      if (line.length === 0) continue;
      const rec = JSON.parse(line);
      // Schedule records (seq 0) carry their keySeq too — a delayed keyed
      // schedule consumed its number at schedule time.
      if (rec.key !== undefined) seqs.push(rec.keySeq);
    }
  }
  return seqs.sort((a, b) => a - b);
}

test('a delayed keyed message is delivered in publish order, not fan-out order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.publishDelayed('a', 'delayed', { delayMs: 100, key: 'k' }); // keySeq 1, due later
  bus.publish('b', 'live', { key: 'k' }); // keySeq 2: held until keySeq 1 fans out
  await flush();
  assert.deepEqual(received, []); // 'live' waits pre-queue; nothing delivered yet
  assert.equal(bus.getStats().keyedReorderedMessages, 1);
  clock.nowMs += 100;
  bus.publish('c', 'trigger'); // drives the flush; the sweep fans out the delayed message
  await flush();
  // 'trigger' was queued before the sweep ran; the keyed pair keeps publish order.
  assert.deepEqual(received, ['trigger', 'delayed', 'live']);
});

test('different keys never block each other', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.publishDelayed('a', 'A-delayed', { delayMs: 100, key: 'k1' });
  bus.publish('b', 'B-live', { key: 'k2' }); // independent key: delivered immediately
  await flush();
  assert.deepEqual(received, ['B-live']);
  clock.nowMs += 100;
  bus.publish('c', 'trigger');
  await flush();
  assert.deepEqual(received, ['B-live', 'trigger', 'A-delayed']);
});

test('delayed keyed schedules keep schedule order across topics', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.publishDelayed('a', 'first', { delayMs: 200, key: 'k' }); // keySeq 1
  bus.publishDelayed('b', 'second', { delayMs: 100, key: 'k' }); // keySeq 2, due first
  clock.nowMs += 100;
  bus.publish('c', 'trigger');
  await flush();
  // 'second' is due but held: its predecessor (keySeq 1) has not fanned out yet.
  assert.deepEqual(received, ['trigger']);
  assert.equal(bus.getStats().keyedReorderedMessages, 1);
  clock.nowMs += 100;
  bus.publish('c', 'trigger2');
  await flush();
  assert.deepEqual(received, ['trigger', 'trigger2', 'first', 'second']);
});

test('a keyed message on a non-matching topic never stalls a matching subscriber', async () => {
  const bus = new EventBus();
  const a: unknown[] = [];
  const b: unknown[] = [];
  bus.subscribe('a.*', (msg) => a.push(msg.payload));
  bus.subscribe('b.*', (msg) => b.push(msg.payload));
  bus.publish('a.x', 'A1', { key: 'k' }); // keySeq 1
  bus.publish('b.y', 'B2', { key: 'k' }); // keySeq 2: a.* never sees it
  bus.publish('a.z', 'A3', { key: 'k' }); // keySeq 3: must not wait for keySeq 2
  await flush();
  assert.deepEqual(a, ['A1', 'A3']);
  assert.deepEqual(b, ['B2']);
});

test('cancelling a delayed keyed schedule releases buffered successors', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('a', 'delayed', { delayMs: 10_000, key: 'k' });
  assert.ok(id);
  bus.publish('b', 'live', { key: 'k' }); // buffered behind keySeq 1
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.cancelDelayed(id), true); // releases keySeq 2 and schedules a flush
  await flush();
  assert.deepEqual(received, ['live']);
});

test('a delayed keyed message that expires before its due time releases its successors', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.setTopicTtl('a', 50);
  bus.publishDelayed('a', 'delayed', { delayMs: 1000, key: 'k' }); // keySeq 1, expires at t=1050
  bus.publish('b', 'live', { key: 'k' }); // keySeq 2, buffered
  await flush();
  assert.deepEqual(received, []);
  clock.nowMs += 1000; // t=2000: the delayed entry is due and already expired
  bus.publish('c', 'trigger'); // sweep drops it as expired and releases keySeq 2
  await flush();
  assert.deepEqual(received, ['trigger', 'live']);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a delayed keyed message shed by the rate limiter at fan-out releases its successors', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.setTopicRateLimit('a', 1, { burst: 1 });
  bus.publishDelayed('a', 'delayed', { delayMs: 100, key: 'k' }); // keySeq 1
  bus.publish('a', 'live', { key: 'k' }); // keySeq 2: buffered; burns the single token
  await flush();
  assert.deepEqual(received, []);
  clock.nowMs += 100; // t=1100: delayed is due, the bucket holds 0.1 tokens
  bus.publish('c', 'trigger');
  await flush();
  // keySeq 1 was shed at fan-out (no token); keySeq 2 was released past it.
  assert.deepEqual(received, ['trigger', 'live']);
  assert.equal(bus.getStats().rateLimitedMessages, 1);
});

test('a filtered keyed message advances the key baseline', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload), { filter: (p) => p !== 'skip' });
  bus.publish('a', 'skip', { key: 'k' }); // keySeq 1, filtered
  bus.publish('b', 'keep', { key: 'k' }); // keySeq 2: must not wait for keySeq 1
  await flush();
  assert.deepEqual(received, ['keep']);
});

test('a throttle-shed keyed message advances the key baseline instead of hanging it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 10,
    throttle: { initialRatePerSec: 1, minRatePerSec: 1, maxRatePerSec: 1 },
  });
  // Fill to the high-water mark (8 of 10) to engage adaptive throttling.
  for (let i = 0; i < 8; i += 1) bus.publish('t', `m${i}`, { key: 'k' });
  clock.nowMs += 5000; // the throttle bucket refills to its 1-token burst cap
  bus.publish('t', 'A', { key: 'k' }); // takes the token: admitted
  bus.publish('t', 'B', { key: 'k' }); // shed: baseline advances past it
  bus.publish('t', 'C', { key: 'k' }); // shed
  clock.nowMs += 2000; // one token refills
  bus.publish('t', 'D', { key: 'k' }); // admitted: not stuck behind B/C
  await flush();
  assert.deepEqual(received, ['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'A', 'D']);
  assert.equal(bus.getStats().throttledMessages, 2);
});

test('a subscriber that joins after keyed publishes baselines at its first keyed message', async () => {
  const bus = new EventBus();
  bus.publish('a', 'old', { key: 'k' }); // no subscribers
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.publish('b', 'new', { key: 'k' });
  await flush();
  assert.deepEqual(received, ['new']);
});

test('unkeyed messages are never held by the key reorder buffer', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.publishDelayed('a', 'delayed', { delayMs: 100, key: 'k' });
  bus.publish('b', 'plain'); // unkeyed: bypasses the ordering gate entirely
  await flush();
  assert.deepEqual(received, ['plain']);
});

test('keyed ordering composes with batch delivery', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const batches: unknown[][] = [];
  bus.subscribe(
    '*',
    (msgs) => batches.push((msgs as unknown as Array<{ payload: unknown }>).map((m) => m.payload)),
    { batch: { maxSize: 3, maxWaitMs: 60_000 } },
  );
  bus.publishDelayed('a', 'delayed', { delayMs: 100, key: 'k' });
  bus.publish('b', 'live', { key: 'k' });
  clock.nowMs += 100;
  bus.publish('c', 'trigger');
  await flush();
  // A full batch delivers immediately; the keyed pair arrives in publish order.
  assert.equal(batches.length, 1);
  assert.deepEqual(batches[0], ['trigger', 'delayed', 'live']);
});

test('keyed ordering composes with reliable at-least-once delivery', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribeReliable('*', (delivery) => {
    received.push(delivery.msg.payload);
    delivery.ack();
  });
  bus.publishDelayed('a', 'delayed', { delayMs: 100, key: 'k' });
  bus.publish('b', 'live', { key: 'k' });
  clock.nowMs += 100;
  bus.publish('c', 'trigger');
  await flush();
  assert.deepEqual(received, ['trigger', 'delayed', 'live']);
});

test('keyed ordering composes with health probing and delivery shaping', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload), {
    healthProbe: { maxConsecutiveFailures: 5 },
    deliveryShaping: { messagesPerSec: 1000, burst: 1000 },
  });
  bus.publishDelayed('a', 'delayed', { delayMs: 100, key: 'k' });
  bus.publish('b', 'live', { key: 'k' });
  clock.nowMs += 100;
  bus.publish('c', 'trigger');
  await flush();
  assert.deepEqual(received, ['trigger', 'delayed', 'live']);
});

test('consumer-group members each observe an ordered subsequence of a key', async () => {
  const bus = new EventBus();
  const m1: unknown[] = [];
  const m2: unknown[] = [];
  bus.subscribeToGroup('g', '*', (msg) => m1.push(msg.payload));
  bus.subscribeToGroup('g', '*', (msg) => m2.push(msg.payload));
  // Round-robin assignment: keySeq 1->m1, 2->m2, 3->m1, 4->m2.
  bus.publish('t', 'one', { key: 'k' });
  bus.publish('t', 'two', { key: 'k' });
  bus.publish('t', 'three', { key: 'k' });
  bus.publish('t', 'four', { key: 'k' });
  await flush();
  assert.deepEqual(m1, ['one', 'three']);
  assert.deepEqual(m2, ['two', 'four']);
});

test('durable log persists keySeq; restart reseeds cursors without renumbering', async () => {
  const clock = controllableClock();
  const dir = mkdtempSync(join(tmpdir(), 'keyorder-'));
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus1.publish('a', 'x', { key: 'k' }); // keySeq 1
  const id = bus1.publishDelayed('b', 'y', { delayMs: 100_000, key: 'k' }); // keySeq 2
  assert.ok(id);
  await flush();
  // Both the delivery record and the schedule record carry their keySeq.
  assert.deepEqual(keySeqsOnDisk(dir), [1, 2]);

  // Restart: cursors are reseeded from the log — the next keyed publish is
  // keySeq 3, not 1 (renumbering would collide with the replayed keySeqs).
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: unknown[] = [];
  bus2.subscribe('*', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  bus2.publish('c', 'z', { key: 'k' }); // keySeq 3: buffered behind the still-pending keySeq 2
  await flush();
  assert.deepEqual(received, ['x']); // 'x' replayed (keySeq 1); 'z' waits for 'y'
  // The recovered delayed schedule kept its keySeq 2: advancing past its
  // due time delivers 'y' before 'z', in publish order.
  clock.nowMs += 100_000;
  bus2.publish('c', 'trigger');
  await flush();
  assert.deepEqual(received, ['x', 'trigger', 'y', 'z']);
});

test('publishAtomic assigns keySeqs in entry order; a rejected batch consumes none', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'keyorder-atomic-'));
  const bus = new EventBus({ durableLogDir: dir });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  bus.setTopicSchema('s', (p) => p !== 'bad');
  const rejected = bus.publishAtomic([
    { topic: 'a', payload: 'A', key: 'k' },
    { topic: 's', payload: 'bad', key: 'k' },
  ]);
  assert.deepEqual(rejected, { published: 0, rejected: { index: 1, topic: 's', reason: 'schema' } });
  // The rejected batch consumed no keySeqs: the next keyed publish is keySeq 1.
  bus.publish('a', 'first', { key: 'k' });
  await flush();
  assert.deepEqual(received, ['first']);
  assert.deepEqual(keySeqsOnDisk(dir), [1]);
  // A committed batch takes keySeqs 2 and 3 in entry order, across topics.
  const ok = bus.publishAtomic([
    { topic: 'a', payload: 'A2', key: 'k' },
    { topic: 'b', payload: 'B2', key: 'k' },
  ]);
  assert.deepEqual(ok, { published: 2 });
  await flush();
  assert.deepEqual(received, ['first', 'A2', 'B2']);
  assert.deepEqual(keySeqsOnDisk(dir), [1, 2, 3]);
});

test('publishIdempotent assigns keySeqs only to admitted publishes', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'keyorder-idem-'));
  const bus = new EventBus({ durableLogDir: dir });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload));
  const r1 = bus.publishIdempotent('t', 'x', { key: 'k', messageId: 'm1' });
  assert.equal(r1.duplicate, false);
  const r2 = bus.publishIdempotent('t', 'x', { key: 'k', messageId: 'm1' });
  assert.equal(r2.duplicate, true); // suppressed: consumes no keySeq
  const r3 = bus.publishIdempotent('t', 'y', { key: 'k', messageId: 'm2' });
  assert.equal(r3.duplicate, false);
  await flush();
  assert.deepEqual(received, ['x', 'y']);
  assert.deepEqual(keySeqsOnDisk(dir), [1, 2]);
});

test('exposition renders the keyed reorder counter', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('*', () => {});
  bus.publishDelayed('a', 'delayed', { delayMs: 100, key: 'k' });
  bus.publish('b', 'live', { key: 'k' });
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_keyed_reordered_messages_total 1'));
});
