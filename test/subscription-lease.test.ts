/**
 * Subscription lease live arm/retune/clear (EB-348).
 *
 * `setSubscriptionLease(subId, ms | null, actor = 'system')` arms a lease
 * on a subscription that never opted in, re-arms an existing lease from
 * the revision moment (the `renewLease` from-now semantics), or clears
 * it — without re-subscribing. Unknown ids return `false` (the
 * `renewLease` convention: nothing to update is not an error); illegal
 * values throw `RangeError` with the live lease untouched; every applied
 * change is audited as `subscription_lease_revised` with
 * `{ previous_lease_ms, lease_ms }`.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type LeaseExpiredEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

function leaseRevisions(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'subscription_lease_revised' });
}

test('arming a lease on a lease-less subscription expires it on time', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload));
  assert.equal(bus.setSubscriptionLease(sub.id, 40), true);
  bus.publish('t', 'before');
  await flush();
  assert.equal(received.length, 1);
  await sleep(120);
  // Gone, exactly like a subscribe-time lease expiry.
  assert.equal(bus.getStats().totalSubscribers, 0);
  bus.publish('t', 'after');
  await flush();
  assert.equal(received.length, 1);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].leaseMs, 40);
  assert.equal(bus.getStats().expiredLeases, 1);
});

test('retuning an existing lease re-counts from the revision moment', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { leaseMs: 60 });
  await sleep(30);
  assert.equal(bus.setSubscriptionLease(sub.id, 90), true);
  // Past the original 60ms deadline, but inside the retuned window.
  await sleep(60);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  bus.publish('t', 'still-here');
  await flush();
  // Past the retuned deadline (30 + 90 = 120ms): expired.
  await sleep(80);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().expiredLeases, 1);
});

test('clearing with null stops expiry, and the lease can be re-armed after', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { leaseMs: 40 });
  assert.equal(bus.setSubscriptionLease(sub.id, null), true);
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().expiredLeases, 0);
  // Re-arming after a clear works like the first arm.
  assert.equal(bus.setSubscriptionLease(sub.id, 40), true);
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].leaseMs, 40);
});

test('clearing a lease-less subscription is a no-op with no audit', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop);
  assert.equal(leaseRevisions(bus).length, 0);
  assert.equal(bus.setSubscriptionLease(sub.id, null), true);
  assert.equal(leaseRevisions(bus).length, 0);
  await sleep(60);
  assert.equal(bus.getStats().totalSubscribers, 1);
  sub.unsubscribe();
});

test('illegal values throw RangeError and leave the live lease untouched', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { leaseMs: 10_000 });
  for (const bad of [0, -5, 2.5, NaN, Infinity, '100' as never]) {
    assert.throws(() => bus.setSubscriptionLease(sub.id, bad as never), RangeError, `ms=${bad}`);
  }
  // An illegal actor throws too, before anything is touched.
  assert.throws(() => bus.setSubscriptionLease(sub.id, 100, ''), RangeError);
  // Validation runs before the lookup: an unknown id still throws.
  assert.throws(() => bus.setSubscriptionLease('ghost', 0), RangeError);
  // The failed revisions left the original lease untouched, and
  // nothing was audited.
  assert.equal(bus.renewLease(sub.id, 10_000), true);
  assert.equal(leaseRevisions(bus).length, 0);
  sub.unsubscribe();
});

test('unknown subscription ids return false and audit nothing', () => {
  const bus = new EventBus();
  assert.equal(bus.setSubscriptionLease('sub-999', 100), false);
  assert.equal(bus.setSubscriptionLease('sub-999', null), false);
  assert.equal(leaseRevisions(bus).length, 0);
});

test('a group member can be armed: expiry leaves the group', async () => {
  const expired: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => expired.push(e) });
  const m1 = bus.subscribeToGroup('g', 't.*', noop);
  const m2 = bus.subscribeToGroup('g', 't.*', noop);
  assert.equal(bus.setSubscriptionLease(m1.id, 40), true);
  await sleep(120);
  // The member is gone through the outermost teardown: the group
  // leave is audited, the other member is untouched.
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(expired.length, 1);
  assert.equal(expired[0].subscriberId, m1.id);
  assert.equal(bus.getStats().expiredLeases, 1);
  const leaves = bus.getSubscriptionAudit({ action: 'group_leave' });
  assert.equal(leaves.length, 1);
  assert.equal(leaves[0].subId, m1.id);
  assert.equal(leaves[0].groupId, 'g');
  m2.unsubscribe();
});

test('a group member lease can be cleared before it fires', async () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't.*', noop);
  assert.equal(bus.setSubscriptionLease(m1.id, 40), true);
  assert.equal(bus.setSubscriptionLease(m1.id, null), true);
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  m1.unsubscribe();
});

test('remainingMs reflects the new lease immediately', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop);
  assert.equal(bus.setSubscriptionLease(sub.id, 5000), true);
  const armed = bus.exportSubscription(sub.id);
  assert.ok(armed !== null);
  assert.ok(armed.leaseRemainingMs > 4000 && armed.leaseRemainingMs <= 5000);
  assert.equal(bus.cancelMigration(armed.handoffToken), true);
  // Retuning moves the remaining time with it.
  assert.equal(bus.setSubscriptionLease(sub.id, 2000), true);
  const retuned = bus.exportSubscription(sub.id);
  assert.ok(retuned !== null);
  assert.ok(retuned.leaseRemainingMs > 1000 && retuned.leaseRemainingMs <= 2000);
  assert.equal(bus.cancelMigration(retuned.handoffToken), true);
  assert.equal(bus.getStats().totalSubscribers, 1);
  sub.unsubscribe();
});

test('revisions are audited with previous and new lease durations', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop);
  assert.equal(bus.setSubscriptionLease(sub.id, 100, 'ops'), true);
  assert.equal(bus.setSubscriptionLease(sub.id, 200), true);
  assert.equal(bus.setSubscriptionLease(sub.id, null), true);
  // The no-op clear and the unknown id add nothing.
  assert.equal(bus.setSubscriptionLease(sub.id, null), true);
  assert.equal(bus.setSubscriptionLease('ghost', 100), false);
  const entries = leaseRevisions(bus);
  assert.equal(entries.length, 3);
  assert.equal(entries[0].subId, sub.id);
  assert.equal(entries[0].pattern, 't');
  assert.equal(entries[0].actor, 'ops');
  assert.deepEqual(entries[0].detail, { previous_lease_ms: null, lease_ms: 100 });
  assert.equal(entries[1].actor, 'system');
  assert.deepEqual(entries[1].detail, { previous_lease_ms: 100, lease_ms: 200 });
  assert.deepEqual(entries[2].detail, { previous_lease_ms: 200, lease_ms: null });
  sub.unsubscribe();
});
