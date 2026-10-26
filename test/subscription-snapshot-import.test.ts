import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BusMessage,
  type Delivery,
  type SubscriptionSnapshot,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('importSubscriptionSnapshot restores the definition but never replays history', async () => {
  const source = new EventBus();
  const sub = source.subscribe('orders.**', () => {}, { queueSize: 10 });
  source.publish('orders', { id: 1 });
  source.publish('orders', { id: 2 });
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.pendingMessages.length, 2);

  const target = new EventBus();
  const received: unknown[] = [];
  const res = target.importSubscriptionSnapshot(snap, (m: BusMessage) => received.push(m.payload));
  assert.deepEqual(res, { restored: 1, skipped: 0 });
  assert.equal(target.getStats().totalSubscribers, 1);

  await flush();
  // The snapshot's backlog stays dead: no historical message is replayed.
  assert.deepEqual(received, []);

  // But the restored definition is live: new traffic flows.
  target.publish('orders', { id: 3 });
  await flush();
  assert.deepEqual(received, [{ id: 3 }]);
});

test('importSubscriptionSnapshot is idempotent per handoff token', async () => {
  const source = new EventBus();
  const sub = source.subscribe('orders.**', () => {});
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);

  const target = new EventBus();
  const received: unknown[] = [];
  const handler = (m: BusMessage) => received.push(m.payload);
  assert.deepEqual(target.importSubscriptionSnapshot(snap, handler), { restored: 1, skipped: 0 });
  assert.deepEqual(target.importSubscriptionSnapshot(snap, handler), { restored: 0, skipped: 1 });
  assert.equal(target.getStats().totalSubscribers, 1);

  target.publish('orders', { id: 9 });
  await flush();
  // Exactly one subscription delivers: the second import built nothing.
  assert.deepEqual(received, [{ id: 9 }]);
});

test('importSubscriptionSnapshot restores a batch and skips only the already-restored', async () => {
  const source = new EventBus();
  const a = source.subscribe('a.**', () => {});
  const b = source.subscribe('b.**', () => {});
  const snapA = source.exportSubscription(a.id);
  const snapB = source.exportSubscription(b.id);
  assert(snapA !== null && snapB !== null);

  const target = new EventBus();
  const noop = () => {};
  assert.deepEqual(target.importSubscriptionSnapshot([snapA, snapB], noop), {
    restored: 2,
    skipped: 0,
  });
  assert.equal(target.getStats().totalSubscribers, 2);

  const c = source.subscribe('c.**', () => {});
  const snapC = source.exportSubscription(c.id);
  assert(snapC !== null);
  // Re-presenting A skips it; the new C restores.
  assert.deepEqual(target.importSubscriptionSnapshot([snapA, snapC], noop), {
    restored: 1,
    skipped: 1,
  });
  assert.equal(target.getStats().totalSubscribers, 3);
});

test('importSubscriptionSnapshot fails a bad batch closed with zero writes', async () => {
  const source = new EventBus();
  const sub = source.subscribe('orders.**', () => {});
  const good = source.exportSubscription(sub.id);
  assert(good !== null);

  const target = new EventBus();
  const noop = () => {};
  // Unknown top-level field: not a format this bus produced.
  const foreign = { ...good, operatorNote: 'stale schema' };
  assert.throws(
    () => target.importSubscriptionSnapshot([good, foreign], noop),
    /importSubscriptionSnapshot: invalid snapshot \(unknown field operatorNote\)/,
  );
  assert.equal(target.getStats().totalSubscribers, 0);

  // Unsupported version fails the same way, still with zero writes.
  const oldVersion = { ...good, version: 0 };
  assert.throws(
    () => target.importSubscriptionSnapshot(oldVersion as unknown as SubscriptionSnapshot, noop),
    /importSubscriptionSnapshot: invalid snapshot \(unsupported version\)/,
  );
  assert.equal(target.getStats().totalSubscribers, 0);

  // Malformed stats and a function-valued option are rejected too.
  const badStats = { ...good, stats: { ...good.stats, queueDepth: -1 } };
  assert.throws(
    () => target.importSubscriptionSnapshot(badStats, noop),
    /importSubscriptionSnapshot: invalid snapshot \(bad stats\.queueDepth\)/,
  );
  const fnOption = { ...good, options: { ...good.options, filter: () => true } };
  assert.throws(
    () => target.importSubscriptionSnapshot(fnOption, noop),
    /importSubscriptionSnapshot: invalid snapshot \(function-valued option\)/,
  );
  assert.equal(target.getStats().totalSubscribers, 0);
});

test('importSubscriptionSnapshot rejects a multi-pattern snapshot claiming reliable', async () => {
  const source = new EventBus();
  const sub = source.subscribe(['a.**', 'b.**'], () => {});
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert(snap.patterns !== undefined);

  const target = new EventBus();
  const claimed = { ...snap, reliable: true };
  assert.throws(
    () => target.importSubscriptionSnapshot(claimed, () => {}),
    /multi-pattern snapshots are never reliable/,
  );
  assert.equal(target.getStats().totalSubscribers, 0);
});

test('importSubscriptionSnapshot restores a reliable subscription without its backlog', async () => {
  const source = new EventBus();
  const sub = source.subscribeReliable('orders.**', () => {});
  source.publish('orders', { id: 1 });
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.reliable, true);

  const target = new EventBus();
  const deliveries: Delivery[] = [];
  const res = target.importSubscriptionSnapshot(snap, (d: Delivery) => deliveries.push(d));
  assert.deepEqual(res, { restored: 1, skipped: 0 });

  await flush();
  assert.deepEqual(deliveries, []);

  target.publish('orders', { id: 2 });
  await flush();
  assert.equal(deliveries.length, 1);
  assert.deepEqual(deliveries[0].msg.payload, { id: 2 });
  deliveries[0].ack();
});
