import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type LeaseExpiredEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

// EB-478: bus-level terminal subscription-lease default live revision.

test('EB-478: the factory bus default is unset (new subscribers get no lease)', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultLeaseMs(), undefined);
  const sub = bus.subscribe('t', noop);
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  sub.unsubscribe();
});

test('EB-478: a subscriber that omits leaseMs takes the bus default and expires on schedule', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  bus.setDefaultLeaseMs(50);
  assert.equal(bus.getDefaultLeaseMs(), 50);
  const sub = bus.subscribe('t', noop);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].leaseMs, 50);
  assert.equal(bus.getStats().expiredLeases, 1);
});

test('EB-478: an explicit leaseMs beats the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultLeaseMs(50);
  // Explicit 5000 beats a bus default of 50: still alive after the default lapses.
  const sub = bus.subscribe('t', noop, { leaseMs: 5000 });
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  sub.unsubscribe();

  // Explicit 50 beats a bus default of 5000: expires on the explicit lease.
  const events: LeaseExpiredEvent[] = [];
  const loud = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  loud.setDefaultLeaseMs(5000);
  const sub2 = loud.subscribe('t', noop, { leaseMs: 50 });
  await sleep(150);
  assert.equal(loud.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].leaseMs, 50);
  assert.equal(loud.getStats().expiredLeases, 1);
  void sub2;
});

test('EB-478: clearing with null returns new subscribers to no lease', async () => {
  const bus = new EventBus();
  bus.setDefaultLeaseMs(50);
  bus.setDefaultLeaseMs(null);
  assert.equal(bus.getDefaultLeaseMs(), undefined);
  const sub = bus.subscribe('t', noop);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  sub.unsubscribe();
});

test('EB-478: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultLeaseMs(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lease_ms_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultLeaseMs(100);
  assert.equal(bus.getDefaultLeaseMs(), 100);
  bus.setDefaultLeaseMs(100);
  assert.equal(bus.getDefaultLeaseMs(), 100);
  const audit = bus.getSubscriptionAudit({ action: 'default_lease_ms_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, leaseMs: 100 });
  bus.setDefaultLeaseMs(null);
  assert.equal(bus.getDefaultLeaseMs(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_lease_ms_revised' });
  assert.equal(cleared.length, 2);
  assert.deepEqual(cleared[1].detail, { previous: 100, leaseMs: null });
});

test('EB-478: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultLeaseMs(50);
  for (const bad of [0, -1, 1.5, Number.NaN, Infinity, '50'] as unknown[]) {
    assert.throws(
      () => bus.setDefaultLeaseMs(bad as number),
      RangeError,
      `ms=${String(bad)}`,
    );
    assert.equal(bus.getDefaultLeaseMs(), 50);
  }
  assert.throws(() => bus.setDefaultLeaseMs(60, ''), RangeError);
  assert.equal(bus.getDefaultLeaseMs(), 50);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lease_ms_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-478: revising the bus default never moves an already-subscribed lease', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  bus.setDefaultLeaseMs(60);
  const sub = bus.subscribe('t', noop);
  bus.setDefaultLeaseMs(10000);
  await sleep(150);
  // The subscriber keeps the 60ms lease it was baked with.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].leaseMs, 60);
  assert.equal(bus.getStats().expiredLeases, 1);
  // A subscriber joining after the revision takes the new default.
  const late = bus.subscribe('u', noop);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1, 'the late joiner holds the 10000ms lease');
  assert.equal(bus.getStats().expiredLeases, 1);
  late.unsubscribe();
  void sub;
});

test('EB-478: setSubscriptionLease(null) clears to the current bus default, and to no lease when no default is set', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { leaseMs: 60 });
  bus.setDefaultLeaseMs(200);
  bus.setSubscriptionLease(sub.id, null);
  const cleared = bus.getSubscriptionAudit({ action: 'subscription_lease_revised' });
  assert.equal(cleared.length, 1);
  assert.deepEqual(cleared[0].detail, { previous_lease_ms: 60, lease_ms: 200 });
  // Still alive past the old 60ms lease: the lease is 200ms now, not cleared.
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  // ...and it lapses on the default lease.
  await sleep(200);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].leaseMs, 200);
  assert.equal(bus.getStats().expiredLeases, 1);

  // No bus default: the clear target is no lease (the old behavior).
  const plain = new EventBus();
  const plainSub = plain.subscribe('t', noop, { leaseMs: 60 });
  assert.equal(plain.setSubscriptionLease(plainSub.id, null), true);
  const plainEvents = plain.getSubscriptionAudit({ action: 'subscription_lease_revised' });
  assert.deepEqual(plainEvents[0].detail, { previous_lease_ms: 60, lease_ms: null });
  await sleep(150);
  assert.equal(plain.getStats().totalSubscribers, 1);
  assert.equal(plain.getStats().expiredLeases, 0);
  plainSub.unsubscribe();
});

test('EB-478: setSubscriptionLease(null) on a lease-less subscriber arms the bus default when one is installed', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop);
  bus.setDefaultLeaseMs(50);
  assert.equal(bus.setSubscriptionLease(sub.id, null), true);
  const revised = bus.getSubscriptionAudit({ action: 'subscription_lease_revised' });
  assert.equal(revised.length, 1);
  assert.deepEqual(revised[0].detail, { previous_lease_ms: null, lease_ms: 50 });
  // Clearing to the very lease already armed is a no-op: no second
  // audit, and the pending timer keeps its original fire time.
  await sleep(30);
  assert.equal(bus.setSubscriptionLease(sub.id, null), true);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscription_lease_revised' }).length,
    1,
    're-clearing to the armed default audits nothing',
  );
  await sleep(40);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].leaseMs, 50);
  assert.equal(bus.getStats().expiredLeases, 1);
});

test('EB-478: with no default a new subscriber has no lease, and setSubscriptionLease can still arm one afterwards', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop);
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  assert.equal(bus.setSubscriptionLease(sub.id, 50), true);
  const armed = bus.getSubscriptionAudit({ action: 'subscription_lease_revised' });
  assert.equal(armed.length, 1);
  assert.deepEqual(armed[0].detail, { previous_lease_ms: null, lease_ms: 50 });
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].leaseMs, 50);
  assert.equal(bus.getStats().expiredLeases, 1);
});

test('EB-478: a snapshot import with an explicit leaseMs beats the bus default', async () => {
  const events: LeaseExpiredEvent[] = [];
  const source = new EventBus();
  const sub = source.subscribe('t', noop, { leaseMs: 600 });
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert(snap.leaseRemainingMs > 0);
  assert(snap.leaseRemainingMs <= 600);

  const target = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  target.setDefaultLeaseMs(5000);
  const res = target.importSubscription(snap, noop);
  assert.equal(res.handoffToken, snap.handoffToken);
  await flush();
  // Still alive past 150ms — and the import's ~600ms remaining lease,
  // not the 5000ms default, governs: it lapses well before 5000ms.
  await sleep(150);
  assert.equal(target.getStats().totalSubscribers, 1);
  assert.equal(target.getStats().expiredLeases, 0);
  await sleep(600);
  assert.equal(target.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert(events[0].leaseMs <= 600);
  assert(events[0].leaseMs > 0);
  assert.equal(target.getStats().expiredLeases, 1);
  sub.unsubscribe();
});
