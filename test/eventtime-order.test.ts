import test from 'node:test';
import assert from 'node:assert/strict';
import { EventTimeOrderedBuffer } from '../src/eventtime-order.ts';

interface Msg {
  id: string;
  eventTime?: number;
}

const ids = (msgs: Msg[]) => msgs.map((m) => m.id);

test('EventTimeOrderedBuffer: messages without eventTime pass straight through', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000 });
  const out = buf.push({ id: 'untimed' }, 0);
  assert.deepEqual(ids(out.ready), ['untimed']);
  assert.equal(out.late, false);
  assert.equal(buf.watermark, undefined);
  assert.equal(buf.pendingCount, 0);
});

test('EventTimeOrderedBuffer: out-of-order arrivals within grace release in event-time order', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000 });
  // 5000 arrives first: watermark 4000, held (5000 > 4000).
  assert.deepEqual(ids(buf.push({ id: 't5000', eventTime: 5000 }, 0).ready), []);
  assert.equal(buf.pendingCount, 1);
  // 4500 arrives within grace: on time (>= 4000), held too.
  assert.deepEqual(ids(buf.push({ id: 't4500', eventTime: 4500 }, 10).ready), []);
  assert.equal(buf.pendingCount, 2);
  // 6000 advances the watermark to 5000: both held messages release, earliest event time first.
  const out = buf.push({ id: 't6000', eventTime: 6000 }, 20);
  assert.deepEqual(ids(out.ready), ['t4500', 't5000']);
  assert.equal(out.late, false);
  assert.equal(buf.pendingCount, 1); // t6000 itself is held: 6000 > 5000.
  assert.equal(buf.watermark, 5000);
});

test('EventTimeOrderedBuffer: equal event times keep arrival order', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 0 });
  // With zero lateness the watermark equals the peak, so each arrival
  // releases immediately; use a lateness that holds them instead.
  const held = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000 });
  held.push({ id: 'first', eventTime: 5000 }, 0);
  held.push({ id: 'second', eventTime: 5000 }, 10);
  const out = held.push({ id: 'peak', eventTime: 6000 }, 20);
  assert.deepEqual(ids(out.ready), ['first', 'second']);
  assert.equal(buf.pendingCount, 0);
});

test('EventTimeOrderedBuffer: late beyond grace is reported and delivered immediately, never buffered', () => {
  const late: { eventTime: number; watermark: number }[] = [];
  const buf = new EventTimeOrderedBuffer<Msg>({
    allowedLatenessMs: 1000,
    onLate: (event) => {
      late.push({ eventTime: event.eventTime, watermark: event.watermark });
    },
  });
  buf.push({ id: 't5000', eventTime: 5000 }, 0); // watermark 4000, held.
  const out = buf.push({ id: 't3999', eventTime: 3999 }, 10);
  assert.equal(out.late, true);
  assert.deepEqual(ids(out.ready), ['t3999']);
  assert.deepEqual(late, [{ eventTime: 3999, watermark: 4000 }]);
  // The late message did not join the buffer and did not move the watermark.
  assert.equal(buf.pendingCount, 1);
  assert.equal(buf.watermark, 4000);
});

test('EventTimeOrderedBuffer: watermark boundary is on time, not late', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000 });
  buf.push({ id: 't5000', eventTime: 5000 }, 0); // watermark 4000.
  const out = buf.push({ id: 't4000', eventTime: 4000 }, 10);
  assert.equal(out.late, false);
  // Exactly at the watermark is on time — and releasable: nothing
  // within grace can still arrive earlier than the watermark itself.
  assert.deepEqual(ids(out.ready), ['t4000']);
  assert.equal(buf.pendingCount, 1); // only t5000 remains held.
});

test('EventTimeOrderedBuffer: maxHold releases a stalled message without a new peak', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 500 });
  buf.push({ id: 't5000', eventTime: 5000 }, 100); // held, watermark 4000.
  assert.deepEqual(ids(buf.drainDue(599)), []);
  assert.equal(buf.pendingCount, 1);
  // At exactly maxHoldMs the hold expires.
  assert.deepEqual(ids(buf.drainDue(600)), ['t5000']);
  assert.equal(buf.pendingCount, 0);
});

test('EventTimeOrderedBuffer: maxHold also releases on the next push', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 100 });
  buf.push({ id: 't5000', eventTime: 5000 }, 0);
  // A new on-time arrival past the stalled message's hold expiry
  // flushes the stalled one; the just-arrived message itself is
  // neither watermark-releasable (4500 > 4000) nor hold-expired.
  const out = buf.push({ id: 't4500', eventTime: 4500 }, 200);
  assert.deepEqual(ids(out.ready), ['t5000']);
  assert.equal(buf.pendingCount, 1);
  assert.deepEqual(ids(buf.drainDue(300)), ['t4500']);
});

test('EventTimeOrderedBuffer: flushAll releases everything in event-time order', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000 });
  buf.push({ id: 't5000', eventTime: 5000 }, 0);
  buf.push({ id: 't4500', eventTime: 4500 }, 10);
  assert.deepEqual(ids(buf.flushAll()), ['t4500', 't5000']);
  assert.equal(buf.pendingCount, 0);
  assert.deepEqual(ids(buf.flushAll()), []);
});

test('EventTimeOrderedBuffer: invalid options and event times throw before anything is buffered', () => {
  assert.throws(() => new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: -1 }), RangeError);
  assert.throws(() => new EventTimeOrderedBuffer<Msg>({ maxHoldMs: 0 }), RangeError);
  assert.throws(() => new EventTimeOrderedBuffer<Msg>({ onLate: 'nope' as never }), TypeError);
  const buf = new EventTimeOrderedBuffer<Msg>({});
  assert.throws(() => buf.push({ id: 'bad', eventTime: -5 }, 0), RangeError);
  assert.throws(() => buf.push({ id: 'bad', eventTime: Number.NaN }, 0), RangeError);
  assert.equal(buf.pendingCount, 0);
  assert.equal(buf.watermark, undefined);
});
