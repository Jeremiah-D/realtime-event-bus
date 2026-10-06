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
