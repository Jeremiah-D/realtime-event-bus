import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type LeaseExpiredEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

test('a lease expires and auto-unsubscribes: event fires, counter moves', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload), { leaseMs: 40 });
  bus.publish('t', 'before');
  await flush();
  assert.equal(received.length, 1);
  await sleep(120);
  // Gone, exactly like a manual unsubscribe.
  assert.equal(bus.getStats().totalSubscribers, 0);
  bus.publish('t', 'after');
  await flush();
  assert.equal(received.length, 1);
  // Expiry fired exactly once, after the unsubscribe.
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].leaseMs, 40);
  assert.equal(bus.getStats().expiredLeases, 1);
});

test('invalid leaseMs values throw RangeError at subscribe time', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, -100, 1.5, NaN, Infinity, -Infinity]) {
    assert.throws(() => bus.subscribe('t', noop, { leaseMs: bad }), RangeError, `leaseMs=${bad}`);
  }
  // Nothing half-registered by the failed subscribes.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().expiredLeases, 0);
});

test('renewLease extends the lease from the renewal moment', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { leaseMs: 60 });
  await sleep(30);
  assert.equal(bus.renewLease(sub.id, 90), true);
  // Past the original 60ms deadline, but inside the renewed window.
  await sleep(60);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().expiredLeases, 0);
  bus.publish('t', 'still-here');
  await flush();
  // Past the renewed deadline (30 + 90 = 120ms): expired.
  await sleep(80);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().expiredLeases, 1);
});

test('renewLease returns false for unknown ids and lease-less subscriptions', async () => {
  const bus = new EventBus();
  assert.equal(bus.renewLease('sub-999', 100), false);
  const plain = bus.subscribe('t', noop);
  assert.equal(bus.renewLease(plain.id, 100), false);
  // A lease-less subscription cannot gain a lease through renew.
  await sleep(50);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.renewLease('sub-999', 100), false);
  plain.unsubscribe();
});

test('renewLease rejects invalid durations with RangeError', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { leaseMs: 10_000 });
  for (const bad of [0, -5, 2.5, NaN]) {
    assert.throws(() => bus.renewLease(sub.id, bad), RangeError, `ms=${bad}`);
  }
  // The failed renews left the original lease untouched.
  assert.equal(bus.renewLease(sub.id, 10_000), true);
  sub.unsubscribe();
});

test('unsubscribe clears the lease: no phantom expiry afterwards', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { leaseMs: 40 });
  sub.unsubscribe();
  // The lease is gone with the subscriber: renew finds nothing, and no
  // expiry fires or counts once the original deadline passes.
  assert.equal(bus.renewLease(sub.id, 1000), false);
  await sleep(120);
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().expiredLeases, 0);
});

test('no leaseMs means no lease: fully backward compatible', async () => {
  const events: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop);
  await sleep(80);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().expiredLeases, 0);
  sub.unsubscribe();
});

test('a throwing onLeaseExpired is isolated: unsubscribe still completes', async () => {
  const bus = new EventBus({
    onLeaseExpired: () => {
      throw new Error('observer boom');
    },
  });
  const sub = bus.subscribe('t', noop, { leaseMs: 40 });
  await sleep(120);
  // The broken observer did not disturb the unsubscribe path or the count.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().expiredLeases, 1);
  void sub;
});

test('queued but undelivered messages die with the queue at expiry', async () => {
  const bus = new EventBus();
  const received: unknown[][] = [];
  // A lingering partial batch holds published messages undelivered; when
  // the lease expires, the batch is discarded with the subscriber — the
  // subscriber's own teardown semantics, nothing lease-specific.
  const sub = bus.subscribe('t', (batch) => received.push(batch), {
    leaseMs: 40,
    batch: { maxSize: 10, maxWaitMs: 5000 },
  });
  bus.publish('t', 'queued');
  await flush();
  assert.equal(received.length, 0); // still lingering, undelivered
  await sleep(120);
  await flush();
  assert.equal(received.length, 0); // discarded at expiry, never delivered
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().expiredLeases, 1);
  void sub;
});

test('group member leases expire through the group leave path', async () => {
  const leaves: string[] = [];
  const bus = new EventBus();
  const member = bus.subscribeToGroup('g', 't', noop, { leaseMs: 40 });
  // A second member observes the leave: leave events fire on the members
  // that remain.
  const watcher = bus.subscribeToGroup('g', 't', noop, {
    onRebalance: (e) => {
      if (e.trigger === 'leave') leaves.push(e.memberId);
    },
  });
  assert.equal(bus.getStats().totalSubscribers, 2);
  await sleep(120);
  // The leased member left the competing set through the normal group
  // leave path (roster updated, linger considered, survivors notified).
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.deepEqual(leaves, [member.id]);
  assert.equal(bus.getStats().expiredLeases, 1);
  // The group still works for the survivor.
  const got: unknown[] = [];
  watcher.unsubscribe();
  const m3 = bus.subscribeToGroup('g', 't', (m) => got.push(m.payload));
  bus.publish('t', 'x');
  await flush();
  assert.equal(got.length, 1);
  m3.unsubscribe();
});

test('reliable subscription leases expire and tear down the ack tracker', async () => {
  const bus = new EventBus();
  const deliveries: unknown[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      d.ack();
    },
    { leaseMs: 40, ackTimeoutMs: 30 },
  );
  bus.publish('t', 'one');
  await flush();
  assert.equal(deliveries.length, 1);
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().unackedDeliveries, 0);
  assert.equal(bus.getStats().expiredLeases, 1);
  // No phantom redelivery after the lease took the subscriber away.
  await sleep(80);
  assert.equal(deliveries.length, 1);
  void sub;
});

test('expiredLeases counts every expiry across subscriptions', async () => {
  const bus = new EventBus();
  const a = bus.subscribe('t', noop, { leaseMs: 30 });
  const b = bus.subscribe('t', noop, { leaseMs: 30 });
  const c = bus.subscribe('t', noop); // no lease: never expires
  await sleep(150);
  assert.equal(bus.getStats().expiredLeases, 2);
  assert.equal(bus.getStats().totalSubscribers, 1);
  c.unsubscribe();
  void a;
  void b;
});
