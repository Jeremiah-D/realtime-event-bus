import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type DroppedEvent } from '../src/bus.ts';

/**
 * Per-subscription queue-drop observation callback (EB-456):
 * `onDropped` fires at drop time with the drop journal row's identity
 * fields plus the live pattern — never the payload — and
 * `setDroppedCallback` swaps or clears it live without re-subscribing.
 * The hook sits at `noteSubscriberDrop`, the one convergence point every
 * drop reason flows through (enqueue eviction, drain expiry, admission
 * refusal), and follows the same scope as the journal (EB-225): reliable
 * subscriptions and shadow mirrors never fire it.
 *
 * Determinism trick (as in `drop-journal.test.ts`): `pauseSubscriber`
 * holds the backlog so publishes shed synchronously instead of draining
 * first; a controllable clock makes drain-time expiry deterministic.
 */

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

/** The `dropped_callback_revised` audit events on the bus. */
function droppedCallbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'dropped_callback_revised' });
}

describe('per-subscription dropped-message callback (EB-456)', () => {
  test('swapping onDropped routes the next drop to the new callback with exact event fields', () => {
    const bus = new EventBus();
    const first: DroppedEvent[] = [];
    const second: DroppedEvent[] = [];
    const sub = bus.subscribe('t', () => {}, {
      queueSize: 2,
      onDropped: (e) => first.push(e),
    });
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // sheds seq 1
    assert.equal(first.length, 1);
    assert.equal(first[0].subscriberId, sub.id);
    assert.equal(first[0].pattern, 't');
    assert.equal(first[0].topic, 't');
    assert.equal(first[0].seq, 1);
    assert.equal(first[0].reason, 'evicted');
    assert.equal(typeof first[0].at, 'number');
    assert.deepEqual(Object.keys(first[0]).sort(), [
      'at',
      'pattern',
      'reason',
      'seq',
      'subscriberId',
      'topic',
    ]);

    // Swap in the second callback: the next drop reaches it only — the
    // subscriber is blocked after the first saturation, so the next
    // arrival is refused instead of evicting.
    bus.setDroppedCallback(sub.id, (e) => second.push(e));
    bus.publish('t', 'd'); // refused: seq 4 never enters the queue
    assert.equal(first.length, 1, 'the old callback never fires again');
    assert.equal(second.length, 1);
    assert.equal(second[0].subscriberId, sub.id);
    assert.equal(second[0].pattern, 't');
    assert.equal(second[0].topic, 't');
    assert.equal(second[0].seq, 4);
    assert.equal(second[0].reason, 'refused');
    assert.deepEqual(droppedCallbackRevisionAudits(bus)[0].detail, {
      hasCallback: true,
      oldHasCallback: true,
    });
  });

  test('clearing onDropped keeps counting and journaling with nobody receiving', () => {
    const bus = new EventBus();
    const seen: DroppedEvent[] = [];
    const sub = bus.subscribe('t', () => {}, {
      queueSize: 2,
      backpressure: 'drop-oldest',
      onDropped: (e) => seen.push(e),
    });
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // evicts seq 1
    assert.equal(seen.length, 1);
    assert.equal(bus.droppedCount(sub.id), 1);
    assert.equal(bus.getDropJournal().length, 1);

    bus.setDroppedCallback(sub.id, null);
    bus.publish('t', 'd'); // evicts seq 2 — still counted and journaled
    assert.equal(seen.length, 1, 'the cleared callback never fires again');
    assert.equal(bus.droppedCount(sub.id), 2);
    assert.equal(bus.getDropJournal().length, 2);
    assert.equal(bus.getDropJournal()[1].seq, 2);
    assert.equal(bus.getDropJournal()[1].reason, 'evicted');
    assert.deepEqual(droppedCallbackRevisionAudits(bus)[0].detail, {
      hasCallback: false,
      oldHasCallback: true,
    });
  });

  test('a subscriber without onDropped refuses revision (fail-closed gate)', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {}, { queueSize: 2 });
    assert.throws(
      () => bus.setDroppedCallback(sub.id, () => {}),
      RangeError,
    );
    // The refusal leaves the live state untouched: the drop still counts
    // and journals, and no revision audit exists.
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // sheds seq 1
    assert.equal(bus.droppedCount(sub.id), 1);
    assert.equal(bus.getDropJournal().length, 1);
    assert.deepEqual(droppedCallbackRevisionAudits(bus), []);
  });

  test('an unknown subscriber id throws RangeError', () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {}, { queueSize: 2, onDropped: () => {} });
    assert.throws(() => bus.setDroppedCallback('no-such-sub', () => {}), RangeError);
    assert.throws(() => bus.setDroppedCallback('', () => {}), RangeError);
    assert.deepEqual(droppedCallbackRevisionAudits(bus), []);
  });

  test('an illegal callback value throws RangeError and leaves the live callback', () => {
    const bus = new EventBus();
    const seen: DroppedEvent[] = [];
    const sub = bus.subscribe('t', () => {}, {
      queueSize: 2,
      backpressure: 'drop-oldest',
      onDropped: (e) => seen.push(e),
    });
    bus.pauseSubscriber(sub.id);
    assert.throws(
      () => bus.setDroppedCallback(sub.id, 'nope' as unknown as (e: DroppedEvent) => void),
      RangeError,
    );
    assert.throws(
      () =>
        bus.setDroppedCallback(
          sub.id,
          undefined as unknown as (e: DroppedEvent) => void,
        ),
      RangeError,
    );
    assert.throws(
      () => bus.setDroppedCallback(sub.id, 42 as unknown as (e: DroppedEvent) => void),
      RangeError,
    );
    // The live callback survived every refused revision.
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // evicts seq 1
    assert.equal(seen.length, 1);
    assert.equal(seen[0].seq, 1);
    assert.deepEqual(droppedCallbackRevisionAudits(bus), []);
  });

  test('re-affirming the live callback is a no-op with no audit', () => {
    const bus = new EventBus();
    const cb = () => {};
    const sub = bus.subscribe('t', () => {}, { queueSize: 2, onDropped: cb });
    bus.setDroppedCallback(sub.id, cb);
    assert.deepEqual(droppedCallbackRevisionAudits(bus), []);
    // Clearing is a real revision (one audit); clearing again is a no-op.
    bus.setDroppedCallback(sub.id, null);
    assert.equal(droppedCallbackRevisionAudits(bus).length, 1);
    bus.setDroppedCallback(sub.id, null);
    assert.equal(droppedCallbackRevisionAudits(bus).length, 1);
  });

  test('each of the four drop reasons fires once with event fields matching the journal row', async () => {
    // 'evicted': a full drop-oldest queue sheds the oldest entry.
    {
      const bus = new EventBus();
      const seen: DroppedEvent[] = [];
      const sub = bus.subscribe('t', () => {}, {
        queueSize: 2,
        onDropped: (e) => seen.push(e),
      });
      bus.pauseSubscriber(sub.id);
      bus.publish('t', 'a');
      bus.publish('t', 'b');
      bus.publish('t', 'c'); // sheds seq 1
      assert.equal(seen.length, 1);
      const [row] = bus.getDropJournal();
      assert.equal(seen[0].reason, 'evicted');
      assertEventMatchesRow(seen[0], row, sub.id);
    }
    // 'refused': a full drop-newest queue refuses the newcomer.
    {
      const bus = new EventBus();
      const seen: DroppedEvent[] = [];
      const sub = bus.subscribe('t', () => {}, {
        queueSize: 2,
        dropPolicy: 'drop-newest',
        onDropped: (e) => seen.push(e),
      });
      bus.pauseSubscriber(sub.id);
      bus.publish('t', 'a');
      bus.publish('t', 'b');
      bus.publish('t', 'c'); // refused: seq 3 never enters the queue
      assert.equal(seen.length, 1);
      const [row] = bus.getDropJournal();
      assert.equal(seen[0].reason, 'refused');
      assertEventMatchesRow(seen[0], row, sub.id);
    }
    // 'ttl-expired': the drain expires a message past its TTL.
    {
      const clock = controllableClock();
      const bus = new EventBus({ now: clock.now });
      const seen: DroppedEvent[] = [];
      const received: unknown[] = [];
      const sub = bus.subscribe(
        't',
        (m) => received.push(m.payload),
        { onDropped: (e) => seen.push(e) },
      );
      bus.publish('t', 'stale', { ttlMs: 100 }); // deadline t=1100
      bus.publish('t', 'fresh', { ttlMs: 10_000 });
      clock.nowMs = 1_200;
      await flush();
      assert.deepEqual(received, ['fresh']);
      assert.equal(seen.length, 1);
      const [row] = bus.getDropJournal();
      assert.equal(seen[0].reason, 'ttl-expired');
      assertEventMatchesRow(seen[0], row, sub.id);
    }
    // 'deadline-expired': the drain expires a message past its hard deadline.
    {
      const clock = controllableClock();
      const bus = new EventBus({ now: clock.now });
      const seen: DroppedEvent[] = [];
      const received: unknown[] = [];
      const sub = bus.subscribe(
        't',
        (m) => received.push(m.payload),
        { onDropped: (e) => seen.push(e) },
      );
      bus.publish('t', 'doomed', { deadlineAt: 1_100 });
      bus.publish('t', 'fine', { deadlineAt: 10_000 });
      clock.nowMs = 1_200;
      await flush();
      assert.deepEqual(received, ['fine']);
      assert.equal(seen.length, 1);
      const [row] = bus.getDropJournal();
      assert.equal(seen[0].reason, 'deadline-expired');
      assertEventMatchesRow(seen[0], row, sub.id);
    }

    /** Every event field matches its journal row, plus the live pattern. */
    function assertEventMatchesRow(
      event: DroppedEvent,
      row: { topic: string; subscriberId: string; seq: number; reason: string; at: number },
      subId: string,
    ): void {
      assert.equal(event.topic, row.topic);
      assert.equal(event.subscriberId, row.subscriberId);
      assert.equal(event.subscriberId, subId);
      assert.equal(event.seq, row.seq);
      assert.equal(event.reason, row.reason);
      assert.equal(event.at, row.at);
      assert.equal(event.pattern, 't');
    }
  });

  test('a reliable subscription never triggers onDropped', () => {
    const bus = new EventBus();
    const seen: DroppedEvent[] = [];
    const sub = bus.subscribeReliable('t', () => {}, {
      queueSize: 2,
      onDropped: (e) => seen.push(e),
    });
    bus.pauseSubscriber(sub.id);
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // sheds seq 1 from the reliable queue
    // The shed really happened (aggregate counter moved) but leaves no
    // journal row and fires no callback: the reliable loss path is the
    // DLQ/ack machinery.
    assert.ok(bus.getStats().droppedMessages >= 1);
    assert.deepEqual(bus.getDropJournal(), []);
    assert.deepEqual(seen, []);
    // Even a revised callback stays silent on the reliable path.
    const revised: DroppedEvent[] = [];
    bus.setDroppedCallback(sub.id, (e) => revised.push(e));
    bus.publish('t', 'd');
    assert.deepEqual(seen, []);
    assert.deepEqual(revised, []);
  });

  test('a shadow mirror never triggers onDropped', async () => {
    const bus = new EventBus();
    const seen: DroppedEvent[] = [];
    const prod: unknown[] = [];
    bus.subscribe('t', (msg) => {
      prod.push(msg.payload);
    });
    // Shadow queue holds 2 with drop-newest: the third copy sheds on the
    // shadow side only.
    bus.subscribe('t', () => {}, {
      shadow: { queueSize: 2, dropPolicy: 'drop-newest' },
      onDropped: (e) => seen.push(e),
    });
    bus.publish('t', 1);
    bus.publish('t', 2);
    bus.publish('t', 3);
    await flush();
    assert.deepEqual(prod, [1, 2, 3]);
    const stats = bus.getStats();
    assert.equal(stats.shadowDroppedMessages, 1);
    assert.equal(stats.droppedMessages, 0);
    assert.deepEqual(bus.getDropJournal(), []);
    assert.deepEqual(seen, []);
  });

  test('a throwing onDropped never breaks the drop path', () => {
    const bus = new EventBus();
    const sub = bus.subscribe('t', () => {}, {
      queueSize: 2,
      backpressure: 'drop-oldest',
      onDropped: () => {
        throw new Error('drop observer exploded');
      },
    });
    bus.pauseSubscriber(sub.id);
    // The publish calls themselves must not throw, and every drop is
    // still counted and journaled exactly once.
    bus.publish('t', 'a');
    bus.publish('t', 'b');
    bus.publish('t', 'c'); // evicts seq 1
    assert.equal(bus.droppedCount(sub.id), 1);
    assert.equal(bus.getDropJournal().length, 1);
    bus.publish('t', 'd'); // evicts seq 2
    assert.equal(bus.droppedCount(sub.id), 2);
    assert.equal(bus.getDropJournal().length, 2);
    assert.deepEqual(
      bus.getDropJournal().map((r) => r.seq),
      [1, 2],
    );
  });
});
