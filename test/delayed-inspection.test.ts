import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('listDelayed returns a payload-free snapshot in schedule order with due timing', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id1 = bus.publishDelayed('t', { secret: 'must-not-leak' }, { delayMs: 5000 });
  const id2 = bus.publishDelayed('t', 'second', { delayMs: 1000, key: 'k1' });
  const listed = bus.listDelayed();
  assert.equal(listed.length, 2);
  // Schedule order, not due-time order: the later-due entry was scheduled first.
  assert.equal(listed[0].delayId, id1);
  assert.equal(listed[1].delayId, id2);
  assert.equal(listed[0].topic, 't');
  assert.equal(listed[0].deliverAt, 6000); // 1000 + 5000, bus clock
  assert.equal(listed[0].remainingMs, 5000);
  assert.equal(listed[1].deliverAt, 2000);
  assert.equal(listed[1].remainingMs, 1000);
  assert.equal(listed[1].key, 'k1');
  // No payload anywhere: the snapshot carries identity and timing only.
  for (const entry of listed) {
    assert.ok(!('payload' in entry), 'snapshot must not carry payload');
    assert.ok(!JSON.stringify(entry).includes('must-not-leak'));
  }
  assert.deepEqual(Object.keys(listed[0]).sort(), ['delayId', 'deliverAt', 'key', 'remainingMs', 'topic']);
  assert.equal(listed[0].key, undefined); // unkeyed schedule: key present, undefined
});

test('listDelayed on a fresh bus is empty, and remainingMs tracks the bus clock', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  assert.deepEqual(bus.listDelayed(), []);
  bus.publishDelayed('t', 'x', { delayMs: 1000 });
  assert.equal(bus.listDelayed()[0].remainingMs, 1000);
  clock.nowMs = 1_500;
  assert.equal(bus.listDelayed()[0].remainingMs, 500); // same entry, clock moved
});

test('fanned-out schedules leave the listing; cancelled schedules never appear', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const gone = bus.publishDelayed('t', 'due-now', { delayMs: 0 });
  const cancelled = bus.publishDelayed('t', 'never', { delayMs: 5000 });
  const staying = bus.publishDelayed('t', 'later', { delayMs: 9000 });
  assert.equal(bus.cancelDelayed(cancelled as string), true);
  await flush(); // fans out 'due-now'
  assert.deepEqual(received, ['due-now']);
  const listed = bus.listDelayed();
  assert.deepEqual(
    listed.map((e) => e.delayId),
    [staying],
  );
  assert.ok(!listed.some((e) => e.delayId === gone));
  assert.ok(!listed.some((e) => e.delayId === cancelled));
  // A second cancellation is a no-op and changes nothing in the listing.
  assert.equal(bus.cancelDelayed(cancelled as string), false);
  assert.deepEqual(
    bus.listDelayed().map((e) => e.delayId),
    [staying],
  );
});

test('an expired schedule is not listed once a sweep drops it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // deadline t=1100 for schedules made at t=1000
  bus.publishDelayed('t', 'doomed', { delayMs: 1000 }); // due t=2000, past its deadline
  const survivor = bus.publishDelayed('t', 'ok', { delayMs: 5000 }); // due t=6000, deadline t=6100
  clock.nowMs = 2_000;
  bus.publish('t', 'poke'); // sweep drops 'doomed' as expired
  await flush();
  assert.deepEqual(received, ['poke']);
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.deepEqual(
    bus.listDelayed().map((e) => e.delayId),
    [survivor],
  );
});

test('a due schedule past its deadline is not listed even before any sweep runs', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100); // deadline t=1100
  bus.publishDelayed('t', 'doomed', { delayMs: 1000 }); // due t=2000
  clock.nowMs = 2_000; // due and past deadline, but no publish/flush has swept yet
  assert.deepEqual(bus.listDelayed(), []);
  // Listing never sweeps: the entry is still pending (still cancellable,
  // still counted) until a sweep actually runs — the listing only reports
  // what would still deliver.
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('an entry past its deadline but not yet due is still listed (expiry is sweep-time)', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100); // deadline t=1100
  const id = bus.publishDelayed('t', 'still-pending', { delayMs: 5000 }); // due t=6000
  clock.nowMs = 2_000; // past the deadline, but not yet due: no sweep would drop it yet
  assert.deepEqual(
    bus.listDelayed().map((e) => e.delayId),
    [id],
  );
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('topicPattern filters: exact topic, single-segment wildcard, multi-segment wildcard', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('orders.eu', 'a', { delayMs: 1000 });
  bus.publishDelayed('orders.us', 'b', { delayMs: 1000 });
  bus.publishDelayed('orders', 'c', { delayMs: 1000 });
  bus.publishDelayed('billing.eu', 'd', { delayMs: 1000 });
  assert.deepEqual(
    bus.listDelayed('orders.eu').map((e) => e.topic),
    ['orders.eu'],
  );
  assert.deepEqual(
    bus.listDelayed('orders.*').map((e) => e.topic),
    ['orders.eu', 'orders.us'],
  );
  assert.deepEqual(
    bus.listDelayed('orders.**').map((e) => e.topic),
    ['orders.eu', 'orders.us', 'orders'],
  );
  assert.deepEqual(
    bus.listDelayed('*').map((e) => e.topic),
    ['orders.eu', 'orders.us', 'orders', 'billing.eu'],
  );
  assert.deepEqual(bus.listDelayed('nope'), []);
});

test('key filter narrows within the topic scope; unkeyed schedules never match a key', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const k1 = bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k1' });
  bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k2' });
  bus.publishDelayed('t', 'c', { delayMs: 1000 }); // unkeyed
  bus.publishDelayed('other', 'd', { delayMs: 1000, key: 'k1' }); // same key, other topic
  assert.deepEqual(
    bus.listDelayed('t', { key: 'k1' }).map((e) => e.delayId),
    [k1],
  );
  assert.deepEqual(bus.listDelayed('t', { key: 'missing' }), []);
  assert.equal(bus.listDelayed('t').length, 3); // no key filter: keyed and unkeyed alike
  assert.equal(bus.listDelayed(undefined, { key: 'k1' }).length, 2); // key filter without a topic scope
});

test('invalid pattern/key throws with the cancel-family convention and leaves the pending set untouched', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const id = bus.publishDelayed('t', 'x', { delayMs: 1000 });
  assert.throws(() => bus.listDelayed(123 as unknown as string), TypeError);
  assert.throws(() => bus.listDelayed(''), RangeError);
  assert.throws(() => bus.listDelayed('t', { key: 42 as unknown as string }), TypeError);
  assert.throws(() => bus.listDelayed('t', { key: '' }), RangeError);
  // Every throw above was fail-closed: the pending set is exactly as before.
  assert.deepEqual(
    bus.listDelayed().map((e) => e.delayId),
    [id],
  );
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('listing is read-only: heap, seq, and stats are unchanged and fan-out still delivers in order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'live'); // seq 1
  const first = bus.publishDelayed('t', 'first', { delayMs: 1000, key: 'k1' });
  const second = bus.publishDelayed('t', 'second', { delayMs: 2000 });
  const statsBefore = bus.getStats();
  const seqBefore = statsBefore.topics.find((s) => s.topic === 't')?.lastSeq;
  bus.listDelayed();
  bus.listDelayed('t', { key: 'k1' });
  bus.listDelayed('nomatch');
  const statsAfter = bus.getStats();
  assert.deepEqual(statsAfter, statsBefore); // no counters moved, pendingDelayed included
  assert.equal(statsAfter.topics.find((s) => s.topic === 't')?.lastSeq, seqBefore);
  // The returned snapshots are fresh copies: mutating one cannot corrupt the bus.
  const snapshot = bus.listDelayed();
  (snapshot[0] as { deliverAt: number }).deliverAt = -999;
  (snapshot[0] as { topic: string }).topic = 'forged';
  assert.equal(bus.listDelayed()[0].deliverAt, 2000);
  assert.equal(bus.listDelayed()[0].topic, 't');
  // Fan-out still delivers both schedules, in due order, with original payloads.
  clock.nowMs = 3_000;
  bus.publish('t', 'poke'); // seq 2
  await flush();
  assert.deepEqual(received, ['live', 'poke', 'first', 'second']);
  assert.deepEqual(
    bus.listDelayed().map((e) => e.delayId),
    [],
  );
  assert.ok(first !== undefined && second !== undefined);
});
