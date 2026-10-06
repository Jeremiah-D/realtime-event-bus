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
