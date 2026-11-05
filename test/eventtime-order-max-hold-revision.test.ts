/**
 * Event-time max-hold live revision (EB-393).
 *
 * `EventTimeOrderedBuffer.setMaxHoldMs(maxHoldMs)` revises how long a
 * buffered message may wait before the escape hatch releases it —
 * without rebuilding the buffer or touching anything already held.
 * Only the hold moves: already-buffered messages keep their original
 * arrival stamps, so tightening releases held messages on the next
 * `push` / `drainDue` and loosening extends their life. The live
 * hold is observable via the `maxHoldMs` getter. Validation reuses
 * the construction-time rule (a positive finite number of
 * milliseconds): an illegal value throws `RangeError` with the live
 * hold untouched, and revising to the current hold is a no-op. The
 * revision itself releases nothing — a held message past the new
 * hold still waits for the next `push` or `drainDue` — and
 * `allowedLatenessMs` / the watermark are untouched. This is the
 * per-topic event-time buffer's hold, not the subscriber
 * delivery-window `maxHoldMs` (EB-230).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventTimeOrderedBuffer } from '../src/eventtime-order.ts';

interface Msg {
  id: string;
  eventTime?: number;
}

const ids = (msgs: Msg[]) => msgs.map((m) => m.id);

test('tightening releases a held message on the next drain even when the watermark has not advanced', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 60_000 });
  buf.push({ id: 't5000', eventTime: 5000 }, 0); // held, watermark 4000.
  assert.equal(buf.pendingCount, 1);
  buf.setMaxHoldMs(100);
  assert.equal(buf.maxHoldMs, 100);
  // The watermark never moved — still 4000 — so only the new hold releases it.
  assert.equal(buf.watermark, 4000);
  assert.deepEqual(ids(buf.drainDue(99)), []);
  assert.deepEqual(ids(buf.drainDue(100)), ['t5000']);
  assert.equal(buf.pendingCount, 0);
});

test('tightening also releases a held message on the next push', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 60_000 });
  buf.push({ id: 't5000', eventTime: 5000 }, 0); // held, watermark 4000.
  buf.setMaxHoldMs(100);
  // A new on-time arrival past the stalled message's new hold expiry
  // flushes the stalled one; the just-arrived message itself is
  // neither watermark-releasable (4500 > 4000) nor hold-expired.
  const out = buf.push({ id: 't4500', eventTime: 4500 }, 200);
  assert.deepEqual(ids(out.ready), ['t5000']);
  assert.equal(buf.pendingCount, 1);
  assert.deepEqual(ids(buf.drainDue(300)), ['t4500']);
});

test('loosening keeps a held message waiting past the old hold', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 500 });
  buf.push({ id: 't5000', eventTime: 5000 }, 100); // held, watermark 4000.
  buf.setMaxHoldMs(5000);
  assert.equal(buf.maxHoldMs, 5000);
  // Past the old 500 ms hold (expiry 600) but inside the new one (expiry 5100).
  assert.deepEqual(ids(buf.drainDue(600)), []);
  assert.deepEqual(ids(buf.drainDue(5099)), []);
  assert.equal(buf.pendingCount, 1);
  assert.deepEqual(ids(buf.drainDue(5100)), ['t5000']);
  assert.equal(buf.pendingCount, 0);
});

test('illegal values throw RangeError and leave the live hold untouched', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 1000 });
  buf.push({ id: 't5000', eventTime: 5000 }, 0); // held.
  for (const bad of [0, -1, Number.NaN, Infinity, -Infinity, '1000', undefined]) {
    assert.throws(() => buf.setMaxHoldMs(bad as never), RangeError);
  }
  assert.equal(buf.maxHoldMs, 1000);
  // The untouched hold still governs: held at 999, released at 1000.
  assert.deepEqual(ids(buf.drainDue(999)), []);
  assert.deepEqual(ids(buf.drainDue(1000)), ['t5000']);
});

test('revising to the current hold is a silent no-op', () => {
  const buf = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 1000 });
  buf.push({ id: 't5000', eventTime: 5000 }, 0); // held.
  buf.setMaxHoldMs(1000);
  assert.equal(buf.maxHoldMs, 1000);
  assert.equal(buf.pendingCount, 1);
  // The hold boundary is unchanged: still 1000 ms from arrival.
  assert.deepEqual(ids(buf.drainDue(999)), []);
  assert.deepEqual(ids(buf.drainDue(1000)), ['t5000']);
});

test('getter follows the live hold, defaulting to 60000', () => {
  const defaulted = new EventTimeOrderedBuffer<Msg>({});
  assert.equal(defaulted.maxHoldMs, 60_000);
  const buf = new EventTimeOrderedBuffer<Msg>({ maxHoldMs: 500 });
  assert.equal(buf.maxHoldMs, 500);
  buf.setMaxHoldMs(2000);
  assert.equal(buf.maxHoldMs, 2000);
  buf.setMaxHoldMs(500);
  assert.equal(buf.maxHoldMs, 500);
});

test('buffered messages keep their original arrival stamps across a revision', () => {
  // Tightening: the release boundary is arrival + the NEW hold, not
  // revision time + the new hold.
  const tight = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 60_000 });
  tight.push({ id: 't5000', eventTime: 5000 }, 100); // arrived at 100.
  tight.setMaxHoldMs(500); // revised at 200, but the stamp stays 100.
  assert.deepEqual(ids(tight.drainDue(599)), []);
  assert.deepEqual(ids(tight.drainDue(600)), ['t5000']); // 100 + 500, not 200 + 500.
  // A message pushed after the revision is measured from its own arrival.
  const after = new EventTimeOrderedBuffer<Msg>({ allowedLatenessMs: 1000, maxHoldMs: 60_000 });
  after.setMaxHoldMs(500);
  after.push({ id: 't5000', eventTime: 5000 }, 1000);
  assert.deepEqual(ids(after.drainDue(1499)), []);
  assert.deepEqual(ids(after.drainDue(1500)), ['t5000']); // 1000 + 500.
});

test('the revision itself releases nothing and leaves watermark and lateness untouched', () => {
  const late: { eventTime: number; watermark: number }[] = [];
  const buf = new EventTimeOrderedBuffer<Msg>({
    allowedLatenessMs: 1000,
    maxHoldMs: 60_000,
    onLate: (event) => {
      late.push({ eventTime: event.eventTime, watermark: event.watermark });
    },
  });
  buf.push({ id: 't5000', eventTime: 5000 }, 0); // held, watermark 4000.
  buf.setMaxHoldMs(100);
  // Past the new hold, but no push / drainDue has happened yet.
  assert.equal(buf.pendingCount, 1);
  assert.equal(buf.watermark, 4000);
  // Lateness is still judged against the unchanged watermark and grace.
  // The late message returns immediately (no drain on the late path),
  // so the held message still waits for the next drain.
  const out = buf.push({ id: 't3999', eventTime: 3999 }, 200);
  assert.equal(out.late, true);
  assert.deepEqual(ids(out.ready), ['t3999']);
  assert.deepEqual(late, [{ eventTime: 3999, watermark: 4000 }]);
  assert.equal(buf.pendingCount, 1);
  assert.deepEqual(ids(buf.drainDue(200)), ['t5000']);
  assert.equal(buf.pendingCount, 0);
});
