/**
 * Bus-level drop journal retention bound revision (EB-318).
 *
 * `setDropJournal` retunes how many per-message drop rows the
 * in-memory ring retains — without restarting the bus. The drop
 * determination itself is never revised: drops are still decided by
 * each queue path, and the journal only records them. The rows already
 * recorded are retained, never cleared or rewritten: shrinking does
 * not evict immediately — the next record evicts the oldest rows down
 * to the live bound — while a widened bound simply retains more.
 * Lifetime totals (`total` / `byReason`) are unaffected by the
 * revision. Illegal values fail closed with the live bound and the
 * audit log untouched; an identical (or empty) revision is a no-op
 * with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const audit = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'drop_journal_revised' });

/**
 * Forces `count` drop-oldest sheds on a paused queue of size 2 and
 * returns the bus: publishes `count + 2` messages, so the first
 * `count` are evicted and journaled. The `drop-oldest` backpressure
 * tier is explicit: the default `block` tier sheds once, then refuses
 * further arrivals while blocked.
 */
function busWithEvictedDrops(capacity: number, count: number) {
  const bus = new EventBus({ dropJournalMaxEntries: capacity });
  const sub = bus.subscribe('t', () => {}, { queueSize: 2, backpressure: 'drop-oldest' });
  bus.pauseSubscriber(sub.id);
  for (let i = 1; i <= count + 2; i += 1) {
    bus.publish('t', { n: i });
  }
  return bus;
}

test('constructor still rejects illegal dropJournalMaxEntries', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, '1000']) {
    assert.throws(() => new EventBus({ dropJournalMaxEntries: bad as never }), RangeError);
  }
});

test('setDropJournal rejects illegal values and leaves the live bound untouched', () => {
  const bus = busWithEvictedDrops(3, 3);
  assert.equal(bus.getStats().dropJournal.capacity, 3);
  assert.equal(bus.getDropJournal().length, 3);
  const bad: Array<unknown> = [
    { maxEntries: 0 },
    { maxEntries: -1 },
    { maxEntries: 1.5 },
    { maxEntries: Number.NaN },
    { maxEntries: '1000' },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setDropJournal(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: same bound, same rows, no audit.
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getStats().dropJournal.capacity, 3);
  assert.deepEqual(
    bus.getDropJournal().map((r) => r.seq),
    [1, 2, 3],
  );
  // A valid revision afterwards still works on the untouched bound.
  bus.setDropJournal({ maxEntries: 2 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { maxEntries: 2, oldMaxEntries: 3 });
  bus.close();
});

test('setDropJournal with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus({ dropJournalMaxEntries: 2 });
  bus.setDropJournal({ maxEntries: 2 });
  bus.setDropJournal({});
  bus.setDropJournal(null as never);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getStats().dropJournal.capacity, 2);
  bus.close();
});

test('revision audits the old and new bound and stats read the live bound', () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().dropJournal.capacity, 1000);
  bus.setDropJournal({ maxEntries: 500 });
  assert.equal(bus.getStats().dropJournal.capacity, 500);
  bus.setDropJournal({ maxEntries: 2000 });
  assert.equal(bus.getStats().dropJournal.capacity, 2000);
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, { maxEntries: 500, oldMaxEntries: 1000 });
  assert.deepEqual(entries[1].detail, { maxEntries: 2000, oldMaxEntries: 500 });
  bus.close();
});

test('shrinking evicts the oldest rows at the next record, totals untouched', () => {
  const bus = busWithEvictedDrops(3, 3);
  assert.deepEqual(
    bus.getDropJournal().map((r) => [r.seq, r.reason]),
    [
      [1, 'evicted'],
      [2, 'evicted'],
      [3, 'evicted'],
    ],
  );
  assert.deepEqual(bus.getStats().dropJournal, {
    total: 3,
    size: 3,
    capacity: 3,
    byReason: { evicted: 3, refused: 0, 'ttl-expired': 0, 'deadline-expired': 0 },
  });
  bus.setDropJournal({ maxEntries: 1 });
  // Shrinking evicts nothing immediately: the in-flight rows stay, and
  // the lifetime totals are unaffected — only the bound moved.
  assert.deepEqual(
    bus.getDropJournal().map((r) => r.seq),
    [1, 2, 3],
  );
  assert.equal(bus.getStats().dropJournal.total, 3);
  assert.equal(bus.getStats().dropJournal.size, 3);
  assert.equal(bus.getStats().dropJournal.capacity, 1);
  // The next drop evicts the oldest rows down to the live bound in one
  // step: only the freshest row survives.
  bus.publish('t', { n: 6 });
  assert.deepEqual(
    bus.getDropJournal().map((r) => [r.seq, r.reason]),
    [[4, 'evicted']],
  );
  assert.equal(bus.getStats().dropJournal.total, 4);
  assert.equal(bus.getStats().dropJournal.size, 1);
  assert.equal(bus.getStats().dropJournal.byReason.evicted, 4);
  bus.close();
});

test('widening the bound retains more rows without evicting', () => {
  const bus = busWithEvictedDrops(1, 1);
  assert.deepEqual(
    bus.getDropJournal().map((r) => r.seq),
    [1],
  );
  bus.setDropJournal({ maxEntries: 3 });
  bus.publish('t', { n: 4 });
  bus.publish('t', { n: 5 });
  assert.deepEqual(
    bus.getDropJournal().map((r) => [r.seq, r.reason]),
    [
      [1, 'evicted'],
      [2, 'evicted'],
      [3, 'evicted'],
    ],
  );
  assert.equal(bus.getStats().dropJournal.total, 3);
  assert.equal(bus.getStats().dropJournal.size, 3);
  assert.equal(bus.getStats().dropJournal.capacity, 3);
  bus.close();
});

test('drop determination is untouched: refusals are still recorded after a revision', () => {
  const bus = new EventBus({ dropJournalMaxEntries: 2 });
  const sub = bus.subscribe('u', () => {}, { queueSize: 2, dropPolicy: 'drop-newest' });
  bus.pauseSubscriber(sub.id);
  bus.publish('u', 'a');
  bus.publish('u', 'b');
  bus.publish('u', 'c'); // refused: seq 3 never enters the queue
  assert.deepEqual(
    bus.getDropJournal().map((r) => [r.seq, r.reason]),
    [[3, 'refused']],
  );
  bus.setDropJournal({ maxEntries: 1 });
  // The refused row is retained by the revision, and the next refusal
  // is still recorded as a refusal — the drop policy did not move.
  bus.publish('u', 'd'); // refused: seq 4
  assert.deepEqual(
    bus.getDropJournal().map((r) => [r.seq, r.reason]),
    [[4, 'refused']],
  );
  assert.equal(bus.getStats().dropJournal.total, 2);
  assert.equal(bus.getStats().dropJournal.byReason.refused, 2);
  bus.close();
});
