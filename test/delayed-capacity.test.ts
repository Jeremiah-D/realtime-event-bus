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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-delayed-capacity-'));

function topicRow(bus: EventBus, topic: string) {
  const row = bus.getStats().topics.find((t) => t.topic === topic);
  assert.ok(row !== undefined, `expected a stats row for topic ${topic}`);
  return row;
}

test('a construction-time cap admits up to the budget, then refuses', () => {
  const bus = new EventBus({ now: controllableClock().now, maxDelayedEntries: 1 });
  assert.ok(bus.publishDelayed('t', 'first', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'second', { delayMs: 1000 }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 1);
  assert.equal(bus.listDelayed().length, 1);
});

test('the boundary is size == cap: one below admits, at cap refuses', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setDelayedCapacity(2);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.ok(bus.publishDelayed('t', 'b', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000 }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 2);
});

test('0 refuses every new schedule; clearing to undefined restores admission', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setDelayedCapacity(0);
  assert.equal(bus.publishDelayed('t', 'a', { delayMs: 1000 }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 0);
  bus.setDelayedCapacity(undefined);
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('tightening never evicts pending schedules; admission resumes once drained below the cap', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setDelayedCapacity(2);
  const a = bus.publishDelayed('t', 'a', { delayMs: 1000 }) as string;
  const b = bus.publishDelayed('t', 'b', { delayMs: 1000 }) as string;
  bus.setDelayedCapacity(1);
  assert.equal(bus.getStats().pendingDelayed, 2, 'tightening must not evict');
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000 }), undefined);
  assert.equal(bus.cancelDelayed(a), true);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000 }), undefined, 'still at cap');
  assert.equal(bus.cancelDelayed(b), true);
  assert.ok(bus.publishDelayed('t', 'c', { delayMs: 1000 }) !== undefined);
});

test('loosening takes effect for the very next schedule attempt', () => {
  const bus = new EventBus({ now: controllableClock().now, maxDelayedEntries: 1 });
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000 }), undefined);
  bus.setDelayedCapacity(3);
  assert.ok(bus.publishDelayed('t', 'b', { delayMs: 1000 }) !== undefined);
});

test('illegal construction and revision values throw RangeError and leave the live budget untouched', () => {
  assert.throws(() => new EventBus({ maxDelayedEntries: -1 }), RangeError);
  assert.throws(() => new EventBus({ maxDelayedEntries: 1.5 }), RangeError);
  assert.throws(() => new EventBus({ maxDelayedEntries: Number.NaN }), RangeError);
  const bus = new EventBus({ now: controllableClock().now, maxDelayedEntries: 2 });
  assert.throws(() => bus.setDelayedCapacity(-1), RangeError);
  assert.throws(() => bus.setDelayedCapacity(1.5), RangeError);
  assert.throws(() => bus.setDelayedCapacity(Number.NaN), RangeError);
  assert.throws(() => bus.setDelayedCapacity('x' as unknown as number), RangeError);
  assert.equal(bus.getDelayedCapacity(), 2, 'failed revisions leave the live budget untouched');
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_capacity_revised' }).length, 0);
});

test('re-affirming the current budget is a no-op with no audit', () => {
  const bus = new EventBus({ now: controllableClock().now, maxDelayedEntries: 2 });
  bus.setDelayedCapacity(2);
  assert.equal(bus.getSubscriptionAudit({ action: 'delayed_capacity_revised' }).length, 0);
  const open = new EventBus({ now: controllableClock().now });
  open.setDelayedCapacity(undefined);
  assert.equal(open.getSubscriptionAudit({ action: 'delayed_capacity_revised' }).length, 0);
});

test('audit pins previous and current, with null standing for unlimited', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setDelayedCapacity(2, 'ops');
  bus.setDelayedCapacity(5, 'ops');
  bus.setDelayedCapacity(undefined, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'delayed_capacity_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { maxEntries: 2, oldMaxEntries: null });
  assert.deepEqual(audit[1].detail, { maxEntries: 5, oldMaxEntries: 2 });
  assert.deepEqual(audit[2].detail, { maxEntries: null, oldMaxEntries: 5 });
  assert.equal(audit[0].actor, 'ops');
});

test('a refused schedule counts per-topic and globally and fires onAdmissionRejected with delayed-capacity', () => {
  const events: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    now: controllableClock().now,
    maxDelayedEntries: 1,
    onAdmissionRejected: (e) => events.push(e),
  });
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000 }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000 }), undefined);
  assert.equal(bus.publishDelayed('t', 'c', { delayMs: 1000 }), undefined);
  assert.equal(events.length, 2);
  assert.equal(events[0].reason, 'delayed-capacity');
  assert.equal(events[0].topic, 't');
  assert.ok(events[0].payloadBytes > 0);
  const row = topicRow(bus, 't');
  assert.equal(row.delayedCapacityRejected, 2);
  assert.equal(row.rejectedMessages, 2);
  const stats = bus.getStats();
  assert.equal(stats.delayedCapacityRejected, 2);
  assert.equal(stats.rejectedMessages, 2);
});

test('a refused schedule burns no publisher token', () => {
  const bus = new EventBus({ now: controllableClock().now });
  bus.setPublisherRateLimit('p', { messagesPerSec: 1, burst: 2 });
  bus.setDelayedCapacity(1);
  // Burns the first of two publisher tokens.
  assert.ok(bus.publishDelayed('t', 'a', { delayMs: 1000, publisherId: 'p' }) !== undefined);
  // Refused by capacity: must not burn the second token.
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000, publisherId: 'p' }), undefined);
  bus.setDelayedCapacity(undefined);
  // Accepted: burns the second token. Had the refusal burned one, this
  // would have been publisher-rate-limited instead.
  assert.ok(bus.publishDelayed('t', 'c', { delayMs: 1000, publisherId: 'p' }) !== undefined);
  assert.equal(bus.publishDelayed('t', 'd', { delayMs: 1000, publisherId: 'p' }), undefined);
  assert.equal(topicRow(bus, 't').publisherRateLimited, 1);
});

test('a batch with one capacity-refused entry rolls back entirely', () => {
  const bus = new EventBus({ now: controllableClock().now, maxDelayedEntries: 2 });
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
  assert.equal(topicRow(bus, 't').delayedCapacityRejected, 1);
});

test('restart recovery rebuilds over-cap schedules and still caps new ones', () => {
  const dir = freshDir();
  const bus1 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  bus1.publishDelayed('t', 'a', { delayMs: 5000 });
  bus1.publishDelayed('t', 'b', { delayMs: 5000 });
  bus1.publishDelayed('t', 'c', { delayMs: 5000 });
  // Simulate a restart with a cap below the recovered count: recovery
  // must not consult the budget.
  const bus2 = new EventBus({ now: controllableClock().now, durableLogDir: dir, maxDelayedEntries: 1 });
  assert.equal(bus2.getStats().pendingDelayed, 3);
  assert.equal(bus2.publishDelayed('t', 'd', { delayMs: 5000 }), undefined);
  assert.equal(bus2.getStats().pendingDelayed, 3);
});

test('jitter, TTL, and deadline schedules all occupy capacity', () => {
  const bus = new EventBus({ now: controllableClock().now, maxDelayedEntries: 1 });
  assert.ok(
    bus.publishDelayed('t', 'a', { delayMs: 1000, jitterMs: 10, ttlMs: 60_000, deadlineMs: 60_000 }) !==
      undefined,
  );
  assert.equal(bus.publishDelayed('t', 'b', { delayMs: 1000 }), undefined);
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('getDelayedCapacity tracks the live budget through construction and revision', () => {
  const bus = new EventBus({ now: controllableClock().now });
  assert.equal(bus.getDelayedCapacity(), undefined);
  bus.setDelayedCapacity(4);
  assert.equal(bus.getDelayedCapacity(), 4);
  bus.setDelayedCapacity(0);
  assert.equal(bus.getDelayedCapacity(), 0);
  bus.setDelayedCapacity(undefined);
  assert.equal(bus.getDelayedCapacity(), undefined);
});
