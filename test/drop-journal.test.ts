import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so expiry tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

describe('subscriber-queue drop journal (EB-225)', () => {
  test('drop-oldest shed victims are journaled by identity, never payload', () => {
    const bus = new EventBus();
    // Pause holds the backlog so the third publish sheds the oldest
    // instead of draining first.
    const sub = bus.subscribe('t', () => {}, { queueSize: 2 });
    bus.pauseSubscriber(sub.id);
    bus.publish('t', { secret: 'must-not-appear' });
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // sheds seq 1
    const rows = bus.getDropJournal();
    assert.equal(rows.length, 1);
    assert.deepEqual(Object.keys(rows[0]).sort(), ['at', 'reason', 'seq', 'subscriberId', 'topic']);
    assert.equal(rows[0].topic, 't');
    assert.equal(rows[0].subscriberId, sub.id);
    assert.equal(rows[0].seq, 1);
    assert.equal(rows[0].reason, 'evicted');
    assert.equal(typeof rows[0].at, 'number');
    assert.equal(bus.getStats().dropJournal.total, 1);
    assert.equal(bus.getStats().dropJournal.byReason.evicted, 1);
  });

  test('drop-newest refusals are journaled as refused', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {}, { queueSize: 2, dropPolicy: 'drop-newest' });
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // refused: seq 3 never enters the queue
    const rows = bus.getDropJournal();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].seq, 3);
    assert.equal(rows[0].reason, 'refused');
  });

  test('block tier: first saturation sheds, later arrivals are refused', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {}, { queueSize: 2, backpressure: 'block' });
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // fills: sheds seq 1, then arms the block
    bus.publish('t', 'd'); // refused while blocked: seq 4
    const rows = bus.getDropJournal();
    assert.deepEqual(
      rows.map((r) => [r.seq, r.reason]),
      [
        [1, 'evicted'],
        [4, 'refused'],
      ],
    );
  });

  test('drain-time TTL expiry is journaled as ttl-expired', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const received: unknown[] = [];
    bus.subscribe('t', (m) => received.push(m.payload));
    bus.publish('t', 'stale', { ttlMs: 100 }); // deadline t=1100
    bus.publish('t', 'fresh', { ttlMs: 10_000 });
    clock.nowMs = 1_200;
    await flush();
    assert.deepEqual(received, ['fresh']);
    const rows = bus.getDropJournal();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].seq, 1);
    assert.equal(rows[0].reason, 'ttl-expired');
    assert.equal(rows[0].at, 1_200);
  });

  test('drain-time hard-deadline expiry is journaled as deadline-expired', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const received: unknown[] = [];
    bus.subscribe('t', (m) => received.push(m.payload));
    bus.publish('t', 'doomed', { deadlineAt: 1_100 });
    bus.publish('t', 'fine', { deadlineAt: 10_000 });
    clock.nowMs = 1_200;
    await flush();
    assert.deepEqual(received, ['fine']);
    const rows = bus.getDropJournal();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].seq, 1);
    assert.equal(rows[0].reason, 'deadline-expired');
  });

  test('an EDF scheduling deadline alone never expires a message', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const received: unknown[] = [];
    bus.subscribe('t', (m) => received.push(m.payload));
    bus.publish('t', 'late-but-live', { deadlineMs: 100 });
    clock.nowMs = 1_200;
    await flush();
    assert.deepEqual(received, ['late-but-live']);
    assert.deepEqual(bus.getDropJournal(), []);
  });

  test('query filters by subscriber, topic, reason, time range, and limit', () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const a = bus.subscribe('alpha', () => {}, { queueSize: 1 });
    const b = bus.subscribe('beta', () => {}, { queueSize: 1, dropPolicy: 'drop-newest' });
    bus.pauseSubscriber(a.id);
    bus.pauseSubscriber(b.id);
    bus.publish('alpha', 'a1');
    bus.publish('alpha', 'a2'); // evicts seq 1 at t=1000
    bus.publish('beta', 'b1');
    clock.nowMs = 2_000;
    bus.publish('beta', 'b2'); // refuses seq 2 at t=2000
    assert.equal(bus.getDropJournal({ subscriberId: a.id }).length, 1);
    assert.equal(bus.getDropJournal({ subscriberId: b.id }).length, 1);
    assert.equal(bus.getDropJournal({ topic: 'alpha' })[0].reason, 'evicted');
    assert.equal(bus.getDropJournal({ topic: 'beta' })[0].reason, 'refused');
    assert.equal(bus.getDropJournal({ reason: 'evicted' }).length, 1);
    assert.equal(bus.getDropJournal({ reason: 'refused' }).length, 1);
    assert.equal(bus.getDropJournal({ since: 1_500 }).length, 1);
    assert.equal(bus.getDropJournal({ until: 1_500 }).length, 1);
    assert.equal(bus.getDropJournal({ since: 1_500, until: 1_500 }).length, 0);
    assert.equal(bus.getDropJournal({ limit: 1 }).length, 1);
    assert.equal(bus.getDropJournal({ limit: 1 })[0].topic, 'alpha'); // oldest first
    assert.equal(bus.getDropJournal({ topic: 'nope' }).length, 0);
  });

  test('the ring is bounded: oldest rows evict but lifetime totals survive', () => {
    const bus = new EventBus({ dropJournalMaxEntries: 2 });
    // Explicit drop-oldest tier: the default 'block' tier would refuse
    // (not shed) everything after the first saturation.
    const sub = bus.subscribe('t', () => {}, { queueSize: 1, backpressure: 'drop-oldest' });
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b'); // evicts seq 1
    bus.publish('t', 'c'); // evicts seq 2
    bus.publish('t', 'd'); // evicts seq 3
    const rows = bus.getDropJournal();
    assert.deepEqual(
      rows.map((r) => r.seq),
      [2, 3],
    );
    const stats = bus.getStats().dropJournal;
    assert.equal(stats.total, 3);
    assert.equal(stats.size, 2);
    assert.equal(stats.capacity, 2);
    assert.equal(stats.byReason.evicted, 3);
  });

  test('reliable-subscriber queue sheds are counted but NOT journaled', () => {
    const bus = new EventBus();
    const sub = bus.subscribeReliable('t', () => {}, { queueSize: 2 });
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // sheds seq 1 from the reliable queue
    // The shed really happened (aggregate counter moved) but leaves no
    // journal row: the reliable loss path is the DLQ/ack machinery.
    assert.ok(bus.getStats().droppedMessages >= 1);
    assert.deepEqual(bus.getDropJournal(), []);
    assert.equal(bus.getStats().dropJournal.total, 0);
  });

  test('invalid bounds and query values throw RangeError', () => {
    assert.throws(() => new EventBus({ dropJournalMaxEntries: 0 }), RangeError);
    assert.throws(() => new EventBus({ dropJournalMaxEntries: 1.5 }), RangeError);
    const bus = new EventBus();
    assert.throws(() => bus.getDropJournal({ limit: 0 }), RangeError);
    assert.throws(() => bus.getDropJournal({ since: Number.NaN }), RangeError);
    assert.throws(
      () => bus.getDropJournal({ reason: 'nope' as 'evicted' }),
      RangeError,
    );
  });
});
