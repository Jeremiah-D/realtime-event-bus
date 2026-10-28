import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const nOf = (msg: BusMessage): number => (msg.payload as { n: number }).n;

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

test('setFilter revises admission for subsequent publishes and audits the change', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(nOf(msg)), {
    filter: (payload) => (payload as { n: number }).n >= 10,
  });
  bus.publish('t', { n: 3 });
  bus.publish('t', { n: 12 });
  await flush();
  assert.deepEqual(received, [12], 'the original predicate admits only n >= 10');

  bus.setFilter(sub.id, (payload) => (payload as { n: number }).n < 10);
  bus.publish('t', { n: 3 });
  bus.publish('t', { n: 12 });
  await flush();
  assert.deepEqual(received, [12, 3], 'the revised predicate judges the next fan-out');

  const events = bus.getSubscriptionAudit({ action: 'filter_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, { filter: 'set', oldFilter: 'set' });
  sub.unsubscribe();
});

test('setFilter leaves already-queued messages under their original admission', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: number[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(nOf(msg)), {
    filter: (payload) => (payload as { n: number }).n % 2 === 0,
    deliveryShaping: { messagesPerSec: 10, burst: 1 },
  });
  bus.publish('t', { n: 2 });
  bus.publish('t', { n: 4 });
  await flush();
  assert.deepEqual(received, [2], 'the burst delivers one message');
  assert.equal(bus.pendingCount(sub.id), 1, 'the second message waits shaped');

  // The new predicate rejects the queued message — but the queue
  // keeps the admission it received under the old predicate.
  bus.setFilter(sub.id, (payload) => (payload as { n: number }).n === 2);
  bus.publish('t', { n: 6 });
  await flush();
  assert.equal(bus.pendingCount(sub.id), 1, 'a message the new predicate rejects never queues');

  clock.advance(100); // one token at 10/s
  bus.publish('t', { n: 2 }); // passes the new predicate, queues behind the held message
  await flush();
  assert.deepEqual(received, [2, 4], 'the queued message still delivers under its original admission');
  assert.equal(bus.pendingCount(sub.id), 1, 'the newly queued message waits for the next token');
  sub.unsubscribe();
});

test('setFilter fails closed for unknown, unfiltered, undefined, and non-function values', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  const filtered = bus.subscribe('t', (msg) => received.push(nOf(msg)), {
    filter: (payload) => (payload as { n: number }).n % 2 === 0,
  });
  const plain = bus.subscribe('t', () => {});
  assert.throws(() => bus.setFilter('no-such-sub', () => true), RangeError);
  assert.throws(() => bus.setFilter(plain.id, () => true), RangeError);
  assert.throws(() => bus.setFilter(filtered.id, undefined as never), RangeError);
  assert.throws(() => bus.setFilter(filtered.id, 'even' as never), RangeError);
  assert.throws(() => bus.setFilter(filtered.id, 42 as never), RangeError);
  assert.throws(() => bus.setFilter('', () => true), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'filter_revised' }).length, 0);

  // Every failure left the original predicate untouched.
  bus.publish('t', { n: 3 });
  bus.publish('t', { n: 4 });
  await flush();
  assert.deepEqual(received, [4]);
  filtered.unsubscribe();
  plain.unsubscribe();
});

test('setFilter with null clears to unfiltered delivery and a later revision is refused', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(nOf(msg)), {
    filter: (payload) => (payload as { n: number }).n % 2 === 0,
  });
  bus.publish('t', { n: 3 });
  bus.publish('t', { n: 4 });
  await flush();
  assert.deepEqual(received, [4]);

  bus.setFilter(sub.id, null);
  bus.publish('t', { n: 3 });
  await flush();
  assert.deepEqual(received, [4, 3], 'clearing returns the subscriber to unfiltered delivery');
  assert.throws(() => bus.setFilter(sub.id, () => true), RangeError);

  const events = bus.getSubscriptionAudit({ action: 'filter_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, { filter: null, oldFilter: 'set' });
  sub.unsubscribe();
});

test('setFilter during a throttled replay pause judges the remaining records', async () => {
  const clock = makeClock(1000);
  const dir = mkdtempSync(join(tmpdir(), 'eb-filter-revision-'));
  const bus = new EventBus({ durableLogDir: dir, now: clock.now });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromSeq: 0,
    replayThrottle: { messagesPerSec: 10, burst: 2 },
    filter: (payload) => (payload as number) % 2 === 0,
  });
  // First chunk, synchronously inside subscribe: 0 admitted, 1 skipped,
  // 2 admitted — then the bucket check tops the loop and pauses the
  // pass before record 3 is judged, so 3 and 4 are both still to come.
  const paused = bus.getStats().replayProgress[0];
  assert.deepEqual(
    { replayed: paused.replayed, skipped: paused.skipped, done: paused.done },
    { replayed: 2, skipped: 1, done: false },
  );
  await flush();
  assert.deepEqual(received, [0, 2]);

  // Revise mid-pause: the remaining records must be judged by the
  // new predicate, not the one the pass started with. Under the old
  // (even) predicate the tail would deliver [.., 4]; under the new
  // (odd) predicate it delivers 3 and skips 4.
  bus.setFilter(sub.id, (payload) => (payload as number) % 2 === 1);
  clock.advance(200); // refill to the burst cap of 2
  bus.publish('zz', {}); // unmatched topic: only triggers a flush
  await flush();
  assert.deepEqual(received, [0, 2, 3], 'records 3/4 are judged by the revised predicate');
  const done = bus.getStats().replayProgress[0];
  assert.deepEqual(
    { replayed: done.replayed, skipped: done.skipped, done: done.done },
    { replayed: 3, skipped: 2, done: true },
  );
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});
