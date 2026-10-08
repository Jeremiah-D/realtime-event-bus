import test from 'node:test';
import assert from 'node:assert/strict';
import { BoundedQueue } from '../src/backpressure.ts';

test('drop-oldest sheds the oldest item when full', () => {
  const q = new BoundedQueue<string>({ capacity: 2, policy: 'drop-oldest' });
  assert.equal(q.push('a'), 'accepted');
  assert.equal(q.push('b'), 'accepted');
  assert.equal(q.push('c'), 'accepted'); // 'a' is shed to make room
  assert.deepEqual(q.drain(), ['b', 'c']);
  assert.equal(q.droppedCount, 1);
});

test('drop-newest sheds the incoming item when full', () => {
  const q = new BoundedQueue<string>({ capacity: 2, policy: 'drop-newest' });
  q.push('a');
  q.push('b');
  assert.equal(q.push('c'), 'dropped');
  assert.deepEqual(q.drain(), ['a', 'b']);
  assert.equal(q.droppedCount, 1);
});

test('high-water-mark callback fires once until the queue recedes', () => {
  const seen: number[] = [];
  const q = new BoundedQueue<number>({
    capacity: 10,
    onHighWaterMark: (size) => seen.push(size),
  });
  for (let i = 0; i < 7; i++) q.push(i);
  assert.deepEqual(seen, []); // below 80%
  q.push(7); // size hits 8 = 80% of capacity
  assert.deepEqual(seen, [8]);
  q.push(8);
  assert.deepEqual(seen, [8]); // fires only once while above the mark
  q.drain(); // recede -> re-arms the callback
  for (let i = 0; i < 8; i++) q.push(i);
  assert.deepEqual(seen, [8, 8]);
});

test('drain empties the queue and preserves FIFO order', () => {
  const q = new BoundedQueue<number>({ capacity: 3 });
  q.push(1);
  q.push(2);
  assert.equal(q.size, 2);
  assert.deepEqual(q.drain(), [1, 2]);
  assert.equal(q.size, 0);
  assert.deepEqual(q.drain(), []);
});

test('non-positive capacity throws', () => {
  assert.throws(() => new BoundedQueue<never>({ capacity: 0 }), RangeError);
});

test('priority: high-priority message survives drop-oldest shedding', () => {
  const q = new BoundedQueue<string>({ capacity: 2, policy: 'drop-oldest' });
  q.push('a', 0);
  q.push('b', 0);
  assert.equal(q.push('c', 10), 'accepted'); // oldest low-priority 'a' is shed
  assert.deepEqual(q.drain(), ['b', 'c']);
  assert.equal(q.droppedCount, 1);
});

test('priority: lowest-priority entry is shed first, oldest among ties', () => {
  const q = new BoundedQueue<string>({ capacity: 3, policy: 'drop-oldest' });
  q.push('a', 0);
  q.push('b', 9);
  q.push('c', 0);
  assert.equal(q.push('d', 5), 'accepted'); // sheds 'a' (oldest of the priority-0 pair)
  assert.deepEqual(q.drain(), ['b', 'c', 'd']);
});

test('priority: incoming strictly-lowest-priority item is dropped', () => {
  const q = new BoundedQueue<string>({ capacity: 2, policy: 'drop-oldest' });
  q.push('a', 5);
  q.push('b', 5);
  assert.equal(q.push('c', 0), 'dropped'); // would evict a higher-priority message
  assert.deepEqual(q.drain(), ['a', 'b']);
  assert.equal(q.droppedCount, 1);
});

test('priority: equal priorities keep the classic oldest-first behavior', () => {
  const q = new BoundedQueue<string>({ capacity: 2, policy: 'drop-oldest' });
  q.push('a', 3);
  q.push('b', 3);
  assert.equal(q.push('c', 3), 'accepted'); // ties: incoming is newest, so 'a' is shed
  assert.deepEqual(q.drain(), ['b', 'c']);
});

test('priority: drop-newest ignores priority and drops the incoming item', () => {
  const q = new BoundedQueue<string>({ capacity: 2, policy: 'drop-newest' });
  q.push('a', 0);
  q.push('b', 0);
  assert.equal(q.push('c', 99), 'dropped');
  assert.deepEqual(q.drain(), ['a', 'b']);
});

test('priority: droppedByPriority tracks per-priority drop counts', () => {
  const q = new BoundedQueue<string>({ capacity: 1, policy: 'drop-oldest' });
  q.push('a', 1);
  q.push('b', 2); // sheds 'a' (priority 1)
  q.push('c', 0); // dropped: incoming is the lowest priority
  assert.equal(q.droppedCount, 2);
  assert.deepEqual(Object.fromEntries(q.droppedByPriority), { 1: 1, 0: 1 });
  assert.deepEqual(q.drain(), ['b']);
});

test('priority: non-finite priority throws', () => {
  const q = new BoundedQueue<never>({ capacity: 2 });
  assert.throws(() => q.push('x' as never, NaN), RangeError);
  assert.throws(() => q.push('x' as never, Infinity), RangeError);
});

test('setHighWaterMarkRatio adjusts the mark at runtime', () => {
  const seen: number[] = [];
  const q = new BoundedQueue<number>({ capacity: 10, onHighWaterMark: (s) => seen.push(s) });
  assert.equal(q.highWaterMark, 8);
  assert.equal(q.highWaterMarkRatio, 0.8);
  for (let i = 0; i < 5; i++) q.push(i);
  assert.deepEqual(seen, []); // below the default mark
  q.setHighWaterMarkRatio(0.5);
  assert.equal(q.highWaterMark, 5);
  assert.equal(q.highWaterMarkRatio, 0.5);
  q.push(5); // size 6 >= 5: excursion under the new mark
  assert.deepEqual(seen, [6]);
});

test('onDrained fires once when the queue recedes below the mark', () => {
  const events: string[] = [];
  const q = new BoundedQueue<number>({
    capacity: 10,
    onHighWaterMark: () => events.push('high'),
    onDrained: (size) => events.push(`drained:${size}`),
  });
  for (let i = 0; i < 8; i++) q.push(i);
  assert.deepEqual(events, ['high']);
  q.drain();
  assert.deepEqual(events, ['high', 'drained:0']);
  // Re-arms: a new excursion fires both callbacks again.
  for (let i = 0; i < 8; i++) q.push(i);
  q.drain();
  assert.deepEqual(events, ['high', 'drained:0', 'high', 'drained:0']);
});

test('onDrained does not fire without a preceding high-water excursion', () => {
  let drained = 0;
  const q = new BoundedQueue<number>({ capacity: 10, onDrained: () => drained++ });
  for (let i = 0; i < 3; i++) q.push(i);
  q.drain();
  assert.equal(drained, 0);
});

test('lowering the mark below the current size mid-excursion fires onDrained immediately', () => {
  const events: string[] = [];
  const q = new BoundedQueue<number>({
    capacity: 10,
    onHighWaterMark: () => events.push('high'),
    onDrained: (size) => events.push(`drained:${size}`),
  });
  for (let i = 0; i < 8; i++) q.push(i); // excursion at mark 8
  q.setHighWaterMarkRatio(0.95); // mark 9.5 > size 8: excursion ends now
  assert.deepEqual(events, ['high', 'drained:8']);
  assert.equal(q.highWaterMark, 9.5);
});

test('lowering the mark but staying above the current size keeps the excursion', () => {
  let drained = 0;
  const q = new BoundedQueue<number>({ capacity: 10, onDrained: () => drained++ });
  for (let i = 0; i < 8; i++) q.push(i);
  q.setHighWaterMarkRatio(0.5); // mark 5 <= size 8: still in excursion
  assert.equal(drained, 0);
  q.drain();
  assert.equal(drained, 1);
});

test('raising the mark above the current size mid-excursion ends it immediately', () => {
  const events: string[] = [];
  const q = new BoundedQueue<number>({
    capacity: 10,
    onHighWaterMark: () => events.push('high'),
    onDrained: (size) => events.push(`drained:${size}`),
  });
  for (let i = 0; i < 8; i++) q.push(i); // excursion at mark 8
  q.setHighWaterMarkRatio(0.9); // mark 9 > size 8: below the new mark, excursion ends
  assert.deepEqual(events, ['high', 'drained:8']);
  // The latch re-armed: filling past the new mark fires again.
  q.push(9); // size 9 >= 9
  assert.deepEqual(events, ['high', 'drained:8', 'high']);
});

test('raising the mark but staying at or below the current size keeps the excursion', () => {
  let drained = 0;
  const q = new BoundedQueue<number>({ capacity: 10, onDrained: () => drained++ });
  for (let i = 0; i < 8; i++) q.push(i);
  q.setHighWaterMarkRatio(0.8); // mark 8 <= size 8: still in excursion
  assert.equal(drained, 0);
  q.drain();
  assert.equal(drained, 1);
});

test('setHighWaterMarkRatio rejects out-of-range values', () => {
  const q = new BoundedQueue<number>({ capacity: 10 });
  for (const bad of [0, -0.5, 1.5, NaN, Infinity]) {
    assert.throws(() => q.setHighWaterMarkRatio(bad), RangeError);
  }
});

test('constructor accepts an initial highWaterMarkRatio', () => {
  const seen: number[] = [];
  const q = new BoundedQueue<number>({
    capacity: 10,
    highWaterMarkRatio: 0.5,
    onHighWaterMark: (s) => seen.push(s),
  });
  assert.equal(q.highWaterMarkRatio, 0.5);
  for (let i = 0; i < 5; i++) q.push(i);
  assert.deepEqual(seen, [5]);
  assert.throws(() => new BoundedQueue<number>({ capacity: 10, highWaterMarkRatio: 0 }), RangeError);
  assert.throws(
    () => new BoundedQueue<number>({ capacity: 10, highWaterMarkRatio: 1.1 }),
    RangeError,
  );
});

test('drainLiveUpTo returns at most maxLive items and keeps the rest queued', () => {
  const q = new BoundedQueue<string>({ capacity: 10 });
  for (const item of ['a', 'b', 'c', 'd']) q.push(item);
  const first = q.drainLiveUpTo(0, 2);
  assert.deepEqual(first, { live: ['a', 'b'], expired: [] });
  assert.equal(q.size, 2);
  const second = q.drainLiveUpTo(0, 10);
  assert.deepEqual(second, { live: ['c', 'd'], expired: [] });
  assert.equal(q.size, 0);
});

test('drainLiveUpTo still enforces expiry within the window', () => {
  const q = new BoundedQueue<string>({ capacity: 10 });
  q.push('old', 0, 100); // expires at t=100
  q.push('fresh', 0, 1000);
  q.push('kept');
  const { live, expired } = q.drainLiveUpTo(100, 1);
  assert.deepEqual(live, ['fresh']);
  assert.deepEqual(expired, ['old']);
  assert.equal(q.expiredCount, 1);
  // 'kept' was past the live window: still queued, evaluated on a later drain.
  assert.equal(q.size, 1);
  assert.deepEqual(q.drainLiveUpTo(0, 10).live, ['kept']);
});

test('drainLiveUpTo with maxLive 0 only collects expired entries', () => {
  const q = new BoundedQueue<string>({ capacity: 10 });
  q.push('old', 0, 100);
  q.push('fresh');
  const { live, expired } = q.drainLiveUpTo(100, 0);
  assert.deepEqual(live, []);
  assert.deepEqual(expired, ['old']);
  assert.equal(q.size, 1);
});

test('drainLive drains everything, like drainLiveUpTo with no bound', () => {
  const q = new BoundedQueue<string>({ capacity: 10 });
  q.push('a', 0, 50);
  q.push('b');
  const { live, expired } = q.drainLive(100);
  assert.deepEqual(live, ['b']);
  assert.deepEqual(expired, ['a']);
  assert.equal(q.size, 0);
});
