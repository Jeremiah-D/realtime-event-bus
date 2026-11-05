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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-delayed-topic-capacity-'));

function topicRow(bus: EventBus, topic: string) {
  const row = bus.getStats().topics.find((t) => t.topic === topic);
  assert.ok(row !== undefined, `expected a stats row for topic ${topic}`);
  return row;
}

test('a per-topic cap admits up to the budget, then refuses; other topics are unaffected', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('hot', 1);
  assert.ok(bus.publishDelayed('hot', 'first', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('hot', 'second', { delayMs: 1000 }), undefined);
  assert.ok(bus.publishDelayed('cold', 'other', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.getStats().pendingDelayed, 2);
});

test('the boundary is count == cap: one below admits, at cap refuses', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('t', 2);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.ok(bus.publishDelayed('t', 'b', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000 }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 2);
});

test('0 refuses every new schedule for that topic; clearing to undefined restores admission', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('t', 0);
  assert.equal(bus.publishDelayed('t', 'a', { delayMs: 1000 }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 0);
  bus.setTopicDelayedCapacity('t', undefined);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('tightening never evicts pending schedules; admission resumes once drained below the cap', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('t', 2);
  const a = bus.publishDelayed('t', 'a', { delayMs: 1000 }) as string;
  const b = bus.publishDelayed('t', 'b', { delayMs: 1000 }) as string;
  bus.setTopicDelayedCapacity('t', 1);
  assert.equal(bus.getStats().pendingDelayed, 2, 'tightening must not evict');
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000 }), undefined);
  assert.equal(bus.cancelDelayed(a), true);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000 }), undefined, 'still at cap');
  assert.equal(bus.cancelDelayed(b), true);
  assert.ok(bus.publishDelayed('t', 'c', { delayMs: 1000 }) !== undefined);
});

test('loosening takes effect for the very next schedule attempt', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('t', 1);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000 }), undefined);
  bus.setTopicDelayedCapacity('t', 3);
  assert.ok(bus.publishDelayed('t', 'b', { delayMs: 1000 }) !== undefined);
});

test('illegal topic and revision values throw RangeError and leave the live budgets untouched', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('t', 2);
  assert.throws(() => bus.setTopicDelayedCapacity('', 1), RangeError);
  assert.throws(() => bus.setTopicDelayedCapacity(123 as unknown as string, 1), RangeError);
  assert.throws(() => bus.setTopicDelayedCapacity('t', -1), RangeError);
  assert.throws(() => bus.setTopicDelayedCapacity('t', 1.5), RangeError);
  assert.throws(() => bus.setTopicDelayedCapacity('t', Number.NaN), RangeError);
  assert.throws(() => bus.setTopicDelayedCapacity('t', 'x' as unknown as number), RangeError);
  assert.equal(bus.getTopicDelayedCapacity('t'), 2, 'failed revisions leave the live budget untouched');
  assert.equal(bus.getTopicDelayedCapacity('other'), undefined);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'topic_delayed_capacity_revised' }).length,
    1,
    'failed revisions add no audit; only the initial set is recorded',
  );
});

test('re-affirming the current budget is a no-op with no audit', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('t', 2);
  bus.setTopicDelayedCapacity('t', 2);
  assert.equal(bus.getSubscriptionAudit({ action: 'topic_delayed_capacity_revised' }).length, 1);
  const open = new EventBus({ now: controllableClock().now });
  open.setTopicDelayedCapacity('t', undefined);
  assert.equal(open.getSubscriptionAudit({ action: 'topic_delayed_capacity_revised' }).length, 0);
});

test('audit pins topic, previous and current, with null standing for unlimited', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('t', 2, 'ops');
  bus.setTopicDelayedCapacity('t', 5, 'ops');
  bus.setTopicDelayedCapacity('t', undefined, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'topic_delayed_capacity_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { topic: 't', maxEntries: 2, oldMaxEntries: null });
  assert.deepEqual(audit[1].detail, { topic: 't', maxEntries: 5, oldMaxEntries: 2 });
  assert.deepEqual(audit[2].detail, { topic: 't', maxEntries: null, oldMaxEntries: 5 });
  assert.equal(audit[0].actor, 'ops');
});

test('a refused schedule counts per-topic and globally and fires onAdmissionRejected with topic-delayed-capacity', () => {
  const events: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    now: controllableClock().now,
    onAdmissionRejected: (e) => events.push(e),
  });
  bus.setTopicDelayedCapacity('t', 1);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000 }), undefined);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000 }), undefined);
  assert.equal(events.length, 2);
  assert.equal(events[0].reason, 'topic-delayed-capacity');
  assert.equal(events[0].topic, 't');
  assert.ok(events[0].payloadBytes > 0);
  const row = topicRow(bus, 't');
  assert.equal(row.topicDelayedCapacityRejected, 2);
  assert.equal(row.delayedCapacityRejected, 0, 'the bus-wide counter must not move');
  assert.equal(row.rejectedMessages, 2);
  const stats = bus.getStats();
  assert.equal(stats.topicDelayedCapacityRejected, 2);
  assert.equal(stats.delayedCapacityRejected, 0);
  assert.equal(stats.rejectedMessages, 2);
});

test('when both budgets are full the refusal is attributed to the bus-wide gate', () => {
  const events: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    now: controllableClock().now,
    maxDelayedEntries: 1,
    onAdmissionRejected: (e) => events.push(e),
  });
  bus.setTopicDelayedCapacity('t', 1);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000 }), undefined);
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'delayed-capacity');
  assert.equal(topicRow(bus, 't').delayedCapacityRejected, 1);
  assert.equal(topicRow(bus, 't').topicDelayedCapacityRejected, 0);
});

test('when only the per-topic budget is full the refusal is attributed to the per-topic gate', () => {
  const events: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    now: controllableClock().now,
    maxDelayedEntries: 10,
    onAdmissionRejected: (e) => events.push(e),
  });
  bus.setTopicDelayedCapacity('t', 1);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000 }), undefined);
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'topic-delayed-capacity');
  assert.equal(topicRow(bus, 't').topicDelayedCapacityRejected, 1);
  assert.equal(topicRow(bus, 't').delayedCapacityRejected, 0);
});

test('a refused schedule burns no publisher token', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setPublisherRateLimit('p', { messagesPerSec: 1, burst: 2 });
  bus.setTopicDelayedCapacity('t', 1);
  // Burns the first of two publisher tokens.
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, publisherId: 'p' }) !== undefined);
  // Refused by the per-topic capacity: must not burn the second token.
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000, publisherId: 'p' }), undefined);
  bus.setTopicDelayedCapacity('t', undefined);
  // Accepted: burns the second token. Had the refusal burned one, this
  // would have been publisher-rate-limited instead.
  assert.ok(bus.publishDelayed('t', 'c', { delayMs: 1000, publisherId: 'p' }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'd', { delayMs: 1000, publisherId: 'p' }), undefined);
  assert.equal(topicRow(bus, 't').publisherRateLimited, 1);
});

test('a batch with one per-topic-refused entry rolls back entirely', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setTopicDelayedCapacity('t', 2);
  assert.throws(
    () =>
      bus.publishDelayedBatch([
        { topic: 't', payload: 'a', delayMs: 1000 },
        { topic: 't', payload: 'b', delayMs: 1000 },
        { topic: 't', payload: 'c', delayMs: 1000 },
      ]),
    /was not admitted; the batch was rolled back/,
  );
  assert.equal(bus.getStats().pendingDelayed, 0, 'no residue from the rolled-back batch');
  assert.equal(bus.listDelayed().length, 0);
  assert.equal(topicRow(bus, 't').topicDelayedCapacityRejected, 1);
});

test('restart recovery rebuilds over-cap schedules and still caps new ones', () => {
  const dir = freshDir();
  const bus1 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  bus1.publishDelayed('t', 'a', { delayMs: 5000 });
  bus1.publishDelayed('t', 'b', { delayMs: 5000 });
  bus1.publishDelayed('t', 'c', { delayMs: 5000 });
  // Simulate a restart: per-topic budgets are live-only, so the new bus
  // starts unlimited and recovery must rebuild all three schedules.
  const bus2 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  assert.equal(bus2.getStats().pendingDelayed, 3);
  assert.equal(bus2.getTopicDelayedCapacity('t'), undefined, 'per-topic budgets do not survive a restart');
  bus2.setTopicDelayedCapacity('t', 1);
  assert.equal(bus2.publishDelayed('t', 'd', { delayMs: 5000 }), undefined);
  assert.equal(bus2.getStats().pendingDelayed, 3);
});

test('getters and list track the live budgets', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.equal(bus.getTopicDelayedCapacity('t'), undefined);
  assert.deepEqual(bus.listTopicDelayedCapacities(), []);
  bus.setTopicDelayedCapacity('t', 4);
  bus.setTopicDelayedCapacity('u', 2);
  assert.equal(bus.getTopicDelayedCapacity('t'), 4);
  assert.deepEqual(bus.listTopicDelayedCapacities(), [
    { topic: 't', maxEntries: 4 },
    { topic: 'u', maxEntries: 2 },
  ]);
  bus.setTopicDelayedCapacity('t', undefined);
  assert.equal(bus.getTopicDelayedCapacity('t'), undefined);
  assert.deepEqual(bus.listTopicDelayedCapacities(), [{ topic: 'u', maxEntries: 2 }]);
  // Mutating a returned row must not move a live budget.
  const rows = bus.listTopicDelayedCapacities();
  rows[0].maxEntries = 99;
  assert.equal(bus.getTopicDelayedCapacity('u'), 2);
});
