import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type AdmissionRejectionEvent } from '../src/bus.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-delayed-key-capacity-'));

function topicRow(bus: EventBus, topic: string) {
  const row = bus.getStats().topics.find((t) => t.topic === topic);
  assert.ok(row !== undefined, `expected a stats row for topic ${topic}`);
  return row;
}

test('a per-key cap admits up to the budget, then refuses; other keys and topics are unaffected', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('hot', 'k', 1);
  assert.ok(bus.publishDelayed('hot', 'first', { delayMs: 1000, key: 'k' }) !== undefined);
  assert.equal(bus.publishDelayed('hot', 'second', { delayMs: 1000, key: 'k' }), undefined);
  assert.ok(bus.publishDelayed('hot', 'other-key', { delayMs: 1000, key: 'j' }) !== undefined);
  assert.ok(bus.publishDelayed('cold', 'other-topic', { delayMs: 1000, key: 'k' }) !== undefined);
  assert.equal(bus.getStats().pendingDelayed, 3);
});

test('the boundary is count == cap: one below admits, at cap refuses', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('t', 'k', 2);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }) !== undefined);
  assert.ok(bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k' }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000, key: 'k' }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 2);
});

test('0 refuses every new schedule for that (topic, key); clearing to undefined restores admission', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('t', 'k', 0);
  assert.equal(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 0);
  bus.setKeyDelayedCapacity('t', 'k', undefined);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }) !== undefined);
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('tightening never evicts pending schedules; admission resumes once drained below the cap', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('t', 'k', 2);
  const a = bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }) as string;
  const b = bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k' }) as string;
  bus.setKeyDelayedCapacity('t', 'k', 1);
  assert.equal(bus.getStats().pendingDelayed, 2, 'tightening must not evict');
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000, key: 'k' }), undefined);
  // Another key on the same topic is untouched by the tightening.
  assert.ok(bus.publishDelayed('t', 'other', { delayMs: 1000, key: 'j' }) !== undefined);
  assert.equal(bus.cancelDelayed(a), true);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000, key: 'k' }), undefined, 'still at cap');
  assert.equal(bus.cancelDelayed(b), true);
  assert.ok(bus.publishDelayed('t', 'c', { delayMs: 1000, key: 'k' }) !== undefined);
});

test('loosening takes effect for the very next schedule attempt', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('t', 'k', 1);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k' }), undefined);
  bus.setKeyDelayedCapacity('t', 'k', 3);
  assert.ok(bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k' }) !== undefined);
});

test('illegal topic, key and revision values throw RangeError and leave the live budgets untouched', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('t', 'k', 2);
  assert.throws(() => bus.setKeyDelayedCapacity('', 'k', 1), RangeError);
  assert.throws(() => bus.setKeyDelayedCapacity(123 as unknown as string, 'k', 1), RangeError);
  assert.throws(() => bus.setKeyDelayedCapacity('t', '', 1), RangeError);
  assert.throws(() => bus.setKeyDelayedCapacity('t', 123 as unknown as string, 1), RangeError);
  assert.throws(
    () => bus.setKeyDelayedCapacity('t', undefined as unknown as string, 1),
    RangeError,
  );
  assert.throws(() => bus.setKeyDelayedCapacity('t', 'k', -1), RangeError);
  assert.throws(() => bus.setKeyDelayedCapacity('t', 'k', 1.5), RangeError);
  assert.throws(() => bus.setKeyDelayedCapacity('t', 'k', Number.NaN), RangeError);
  assert.throws(() => bus.setKeyDelayedCapacity('t', 'k', 'x' as unknown as number), RangeError);
  assert.equal(bus.getKeyDelayedCapacity('t', 'k'), 2, 'failed revisions leave the live budget untouched');
  assert.equal(bus.getKeyDelayedCapacity('t', 'other'), undefined);
  assert.equal(bus.getKeyDelayedCapacity('other', 'k'), undefined);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_delayed_capacity_revised' }).length,
    1,
    'failed revisions add no audit; only the initial set is recorded',
  );
});

test('re-affirming the current budget is a no-op with no audit', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('t', 'k', 2);
  bus.setKeyDelayedCapacity('t', 'k', 2);
  assert.equal(bus.getSubscriptionAudit({ action: 'key_delayed_capacity_revised' }).length, 1);
  const open = new EventBus({ now: controllableClock().now });
  open.setKeyDelayedCapacity('t', 'k', undefined);
  assert.equal(open.getSubscriptionAudit({ action: 'key_delayed_capacity_revised' }).length, 0);
});

test('audit pins topic, key, previous and current, with null standing for unlimited', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('t', 'k', 2, 'ops');
  bus.setKeyDelayedCapacity('t', 'k', 5, 'ops');
  bus.setKeyDelayedCapacity('t', 'k', undefined, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'key_delayed_capacity_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { topic: 't', key: 'k', maxEntries: 2, oldMaxEntries: null });
  assert.deepEqual(audit[1].detail, { topic: 't', key: 'k', maxEntries: 5, oldMaxEntries: 2 });
  assert.deepEqual(audit[2].detail, { topic: 't', key: 'k', maxEntries: null, oldMaxEntries: 5 });
  assert.equal(audit[0].actor, 'ops');
});

test('a refused schedule counts per-topic and globally and fires onAdmissionRejected with key-delayed-capacity', () => {
  const events: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    now: controllableClock().now,
    onAdmissionRejected: (e) => events.push(e),
  });
  bus.setKeyDelayedCapacity('t', 'k', 1);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k' }), undefined);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000, key: 'k' }), undefined);
  assert.equal(events.length, 2);
  assert.equal(events[0].reason, 'key-delayed-capacity');
  assert.equal(events[0].topic, 't');
  assert.ok(events[0].payloadBytes > 0);
  const row = topicRow(bus, 't');
  assert.equal(row.keyDelayedCapacityRejected, 2);
  assert.equal(row.topicDelayedCapacityRejected, 0, 'the per-topic counter must not move');
  assert.equal(row.delayedCapacityRejected, 0, 'the bus-wide counter must not move');
  assert.equal(row.rejectedMessages, 2);
  const stats = bus.getStats();
  assert.equal(stats.keyDelayedCapacityRejected, 2);
  assert.equal(stats.topicDelayedCapacityRejected, 0);
  assert.equal(stats.delayedCapacityRejected, 0);
  assert.equal(stats.rejectedMessages, 2);
});

test('attribution order is bus-wide, then per-topic, then per-key', () => {
  // All three budgets full: the bus-wide gate owns the refusal.
  {
    const events: AdmissionRejectionEvent[] = [];
    const bus = new EventBus({
      now: controllableClock().now,
      maxDelayedEntries: 1,
      onAdmissionRejected: (e) => events.push(e),
    });
    bus.setTopicDelayedCapacity('t', 1);
    bus.setKeyDelayedCapacity('t', 'k', 1);
    assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }) !== undefined);
    assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k' }), undefined);
    assert.equal(events.length, 1);
    assert.equal(events[0].reason, 'delayed-capacity');
    assert.equal(topicRow(bus, 't').delayedCapacityRejected, 1);
    assert.equal(topicRow(bus, 't').topicDelayedCapacityRejected, 0);
    assert.equal(topicRow(bus, 't').keyDelayedCapacityRejected, 0);
  }
  // Per-topic full, per-key not full: the per-topic gate owns it.
  {
    const events: AdmissionRejectionEvent[] = [];
    const bus = new EventBus({
      now: controllableClock().now,
      maxDelayedEntries: 10,
      onAdmissionRejected: (e) => events.push(e),
    });
    bus.setTopicDelayedCapacity('t', 1);
    bus.setKeyDelayedCapacity('t', 'k', 5);
    assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }) !== undefined);
    assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k' }), undefined);
    assert.equal(events.length, 1);
    assert.equal(events[0].reason, 'topic-delayed-capacity');
    assert.equal(topicRow(bus, 't').topicDelayedCapacityRejected, 1);
    assert.equal(topicRow(bus, 't').keyDelayedCapacityRejected, 0);
  }
  // Only the per-key budget full: the per-key gate owns it.
  {
    const events: AdmissionRejectionEvent[] = [];
    const bus = new EventBus({
      now: controllableClock().now,
      maxDelayedEntries: 10,
      onAdmissionRejected: (e) => events.push(e),
    });
    bus.setTopicDelayedCapacity('t', 10);
    bus.setKeyDelayedCapacity('t', 'k', 1);
    assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k' }) !== undefined);
    assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k' }), undefined);
    assert.equal(events.length, 1);
    assert.equal(events[0].reason, 'key-delayed-capacity');
    assert.equal(topicRow(bus, 't').keyDelayedCapacityRejected, 1);
    assert.equal(topicRow(bus, 't').topicDelayedCapacityRejected, 0);
    assert.equal(topicRow(bus, 't').delayedCapacityRejected, 0);
  }
});

test('unkeyed schedules bypass the per-key gate but stay under the bus-wide and per-topic gates', () => {
  // A zero per-key budget refuses keyed schedules but not unkeyed ones.
  {
    const bus = new EventBus({ now: controllableClock().now });
    bus.setKeyDelayedCapacity('t', 'k', 0);
    assert.ok(bus.publishDelayed('t', 'unkeyed', { delayMs: 1000 }) !== undefined);
    assert.equal(bus.publishDelayed('t', 'keyed', { delayMs: 1000, key: 'k' }), undefined);
    assert.ok(bus.publishDelayed('t', 'other-key', { delayMs: 1000, key: 'j' }) !== undefined);
    assert.equal(bus.getStats().pendingDelayed, 2);
  }
  // The bus-wide gate still refuses unkeyed schedules.
  {
    const events: AdmissionRejectionEvent[] = [];
    const bus = new EventBus({
      now: controllableClock().now,
      maxDelayedEntries: 1,
      onAdmissionRejected: (e) => events.push(e),
    });
    bus.setKeyDelayedCapacity('t', 'k', 0);
    assert.ok(bus.publishDelayed('t', 'first', { delayMs: 1000 }) !== undefined);
    assert.equal(bus.publishDelayed('t', 'second', { delayMs: 1000 }), undefined);
    assert.equal(events[0].reason, 'delayed-capacity');
  }
  // The per-topic gate still refuses unkeyed schedules.
  {
    const events: AdmissionRejectionEvent[] = [];
    const bus = new EventBus({
      now: controllableClock().now,
      onAdmissionRejected: (e) => events.push(e),
    });
    bus.setTopicDelayedCapacity('t', 1);
    bus.setKeyDelayedCapacity('t', 'k', 0);
    assert.ok(bus.publishDelayed('t', 'first', { delayMs: 1000 }) !== undefined);
    assert.equal(bus.publishDelayed('t', 'second', { delayMs: 1000 }), undefined);
    assert.equal(events[0].reason, 'topic-delayed-capacity');
  }
});

test('a refused schedule burns no publisher token', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setPublisherRateLimit('p', { messagesPerSec: 1, burst: 2 });
  bus.setKeyDelayedCapacity('t', 'k', 1);
  // Burns the first of two publisher tokens.
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, key: 'k', publisherId: 'p' }) !== undefined);
  // Refused by the per-key capacity: must not burn the second token.
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000, key: 'k', publisherId: 'p' }), undefined);
  bus.setKeyDelayedCapacity('t', 'k', undefined);
  // Accepted: burns the second token. Had the refusal burned one, this
  // would have been publisher-rate-limited instead.
  assert.ok(bus.publishDelayed('t', 'c', { delayMs: 1000, key: 'k', publisherId: 'p' }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'd', { delayMs: 1000, key: 'k', publisherId: 'p' }), undefined);
  assert.equal(topicRow(bus, 't').publisherRateLimited, 1);
});

test('a batch with one per-key-refused entry rolls back entirely', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setKeyDelayedCapacity('t', 'k', 2);
  assert.throws(
    () =>
      bus.publishDelayedBatch([
        { topic: 't', payload: 'a', delayMs: 1000, key: 'k' },
        { topic: 't', payload: 'b', delayMs: 1000, key: 'k' },
        { topic: 't', payload: 'c', delayMs: 1000, key: 'k' },
      ]),
    /was not admitted; the batch was rolled back/,
  );
  assert.equal(bus.getStats().pendingDelayed, 0, 'no residue from the rolled-back batch');
  assert.equal(bus.listDelayed().length, 0);
  assert.equal(topicRow(bus, 't').keyDelayedCapacityRejected, 1);
});

test('restart recovery rebuilds over-cap schedules and still caps new ones', () => {
  const dir = freshDir();
  const bus1 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  bus1.publishDelayed('t', 'a', { delayMs: 5000, key: 'k' });
  bus1.publishDelayed('t', 'b', { delayMs: 5000, key: 'k' });
  bus1.publishDelayed('t', 'c', { delayMs: 5000, key: 'k' });
  // Simulate a restart: per-key budgets are live-only, so the new bus
  // starts unlimited and recovery must rebuild all three schedules.
  const bus2 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  assert.equal(bus2.getStats().pendingDelayed, 3);
  assert.equal(bus2.getKeyDelayedCapacity('t', 'k'), undefined, 'per-key budgets do not survive a restart');
  bus2.setKeyDelayedCapacity('t', 'k', 1);
  assert.equal(bus2.publishDelayed('t', 'd', { delayMs: 5000, key: 'k' }), undefined);
  // Unkeyed schedules bypass the per-key gate; another key is uncapped.
  assert.ok(bus2.publishDelayed('t', 'e', { delayMs: 5000 }) !== undefined);
  assert.ok(bus2.publishDelayed('t', 'f', { delayMs: 5000, key: 'j' }) !== undefined);
  assert.equal(bus2.getStats().pendingDelayed, 5);
});

test('getters and list track the live budgets', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.equal(bus.getKeyDelayedCapacity('t', 'k'), undefined);
  assert.deepEqual(bus.listKeyDelayedCapacities(), []);
  bus.setKeyDelayedCapacity('t', 'k', 4);
  bus.setKeyDelayedCapacity('t', 'j', 2);
  bus.setKeyDelayedCapacity('u', 'k', 1);
  assert.equal(bus.getKeyDelayedCapacity('t', 'k'), 4);
  assert.deepEqual(bus.listKeyDelayedCapacities(), [
    { topic: 't', key: 'k', maxEntries: 4 },
    { topic: 't', key: 'j', maxEntries: 2 },
    { topic: 'u', key: 'k', maxEntries: 1 },
  ]);
  bus.setKeyDelayedCapacity('t', 'k', undefined);
  assert.equal(bus.getKeyDelayedCapacity('t', 'k'), undefined);
  assert.deepEqual(bus.listKeyDelayedCapacities(), [
    { topic: 't', key: 'j', maxEntries: 2 },
    { topic: 'u', key: 'k', maxEntries: 1 },
  ]);
  // Clearing the last key of a topic removes the topic row entirely.
  bus.setKeyDelayedCapacity('t', 'j', undefined);
  assert.deepEqual(bus.listKeyDelayedCapacities(), [{ topic: 'u', key: 'k', maxEntries: 1 }]);
  // Mutating a returned row must not move a live budget.
  const rows = bus.listKeyDelayedCapacities();
  rows[0].maxEntries = 99;
  assert.equal(bus.getKeyDelayedCapacity('u', 'k'), 1);
});
