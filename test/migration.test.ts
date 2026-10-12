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
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

const payloads = (msgs: BusMessage[]): unknown[] => msgs.map((m) => m.payload);

test('export/import round-trips a plain subscription: backlog moves, source pauses', async () => {
  const source = new EventBus();
  const sourceReceived: unknown[] = [];
  const sub = source.subscribe('orders.**', (m) => sourceReceived.push(m.payload), {
    queueSize: 10,
  });
  source.publish('orders', { id: 1 });
  source.publish('orders', { id: 2 });
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.kind, 'realtime-event-bus/subscription-snapshot');
  assert.equal(snap.version, 1);
  assert.equal(snap.pattern, 'orders.**');
  assert.equal(snap.reliable, false);
  assert.equal(snap.pendingMessages.length, 2);
  assert.deepEqual(payloads(snap.pendingMessages), [{ id: 1 }, { id: 2 }]);
  assert.deepEqual(
    snap.pendingMessages.map((m) => m.seq),
    [1, 2],
  );
  assert.match(snap.handoffToken, /^[0-9a-f]{32}$/);
  assert.equal(snap.stats.queueDepth, 2);
  assert.equal(snap.options.queueSize, 10);
  // Delivery is paused on the source: new publishes queue up, nothing reaches the handler.
  source.publish('orders', { id: 3 });
  await flush();
  assert.deepEqual(sourceReceived, []);
  assert.equal(source.getStats().migratingSubscribers, 1);
  assert.equal(source.getStats().totalSubscribers, 1);

  const target = new EventBus();
  const targetReceived: BusMessage[] = [];
  const res = target.importSubscription(snap, (m) => targetReceived.push(m));
  assert.equal(res.handoffToken, snap.handoffToken);
  await flush();
  // The backlog arrives in order, same envelopes (per-topic seqs preserved).
  assert.deepEqual(payloads(targetReceived), [{ id: 1 }, { id: 2 }]);
  assert.deepEqual(
    targetReceived.map((m) => m.seq),
    [1, 2],
  );
  // New publishes flow to the target now.
  target.publish('orders', { id: 4 });
  await flush();
  assert.deepEqual(
    targetReceived.map((m) => (m.payload as { id: number }).id),
    [1, 2, 4],
  );

  // The target has taken over: the source unsubscribes through the normal path.
  const done = source.completeMigration(snap.handoffToken);
  assert(done !== false);
  assert.equal(done.subId, sub.id);
  assert.equal(done.handoffToken, snap.handoffToken);
  // { id: 3 } arrived after the export: reported, not hidden.
  assert.equal(done.stragglers, 1);
  assert.equal(source.getStats().totalSubscribers, 0);
  assert.equal(source.getStats().migratingSubscribers, 0);
  // Completing twice is a no-op.
  assert.equal(source.completeMigration(snap.handoffToken), false);
});

test('reliable migration: acked messages never redeliver, unacked redeliver exactly once', async () => {
  const source = new EventBus();
  const deliveries: Delivery<BusMessage>[] = [];
  const sub = source.subscribeReliable('t', (d) => {
    deliveries.push(d);
  });
  for (let i = 1; i <= 4; i++) source.publish('t', { n: i });
  await flush();
  assert.equal(deliveries.length, 4);
  // Ack the first two; 3 and 4 stay in flight (unacked).
  deliveries[0].ack();
  deliveries[1].ack();

  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.reliable, true);
  assert.equal(snap.ackCommittedSeq, 4);
  assert.equal(snap.stats.unackedCount, 2);
  assert.deepEqual(
    snap.pendingMessages.map((m) => (m.payload as { n: number }).n),
    [3, 4],
  );

  const target = new EventBus();
  const targetDeliveries: Delivery<BusMessage>[] = [];
  target.importSubscription(snap, (d) => {
    targetDeliveries.push(d);
    d.ack();
  });
  await flush();
  // Exactly the unacked two, in order, with the delivery numbering
  // continuing where the source left off.
  assert.deepEqual(
    targetDeliveries.map((d) => (d.msg.payload as { n: number }).n),
    [3, 4],
  );
  assert.deepEqual(
    targetDeliveries.map((d) => d.seq),
    [5, 6],
  );
  // The source-acked messages stay acked: never redelivered.
  await flush();
  assert.equal(targetDeliveries.length, 2);

  const done = source.completeMigration(snap.handoffToken);
  assert(done !== false);
  assert.equal(source.getStats().totalSubscribers, 0);
});

test('reliable migration carries never-delivered queued messages with fresh seqs', async () => {
  const source = new EventBus();
  const sub = source.subscribeReliable('t', noop);
  source.publish('t', 'a');
  source.publish('t', 'b');
  // Export before any flush: nothing was ever delivered on the source.
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.pendingMessages.length, 2);
  assert.equal(snap.ackCommittedSeq, 0);

  const target = new EventBus();
  const seqs: number[] = [];
  target.importSubscription(snap, (d) => {
    seqs.push(d.seq);
    d.ack();
  });
  await flush();
  // No committed position on the source: numbering restarts at 1.
  assert.deepEqual(seqs, [1, 2]);
  assert.equal(source.completeMigration(snap.handoffToken) !== false, true);
});

test('export returns null for unknown ids, group members, and double exports', () => {
  const bus = new EventBus();
  assert.equal(bus.exportSubscription('sub-nope'), null);
  const member = bus.subscribeToGroup('g1', 't', noop);
  // Group membership cannot migrate: the roster is bus-local.
  assert.equal(bus.exportSubscription(member.id), null);
  const sub = bus.subscribe('t', noop);
  const snap = bus.exportSubscription(sub.id);
  assert(snap !== null);
  // A second export while one is in flight would fork the backlog.
  assert.equal(bus.exportSubscription(sub.id), null);
  member.unsubscribe();
});

test('complete/cancel return false for unknown tokens', () => {
  const bus = new EventBus();
  const bad = '0'.repeat(32);
  assert.equal(bus.completeMigration(bad), false);
  assert.equal(bus.cancelMigration(bad), false);
});

test('cancelMigration resumes delivery on the source without loss', async () => {
  const source = new EventBus();
  const received: unknown[] = [];
  const sub = source.subscribe('t', (m) => received.push(m.payload));
  source.publish('t', 'a');
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  source.publish('t', 'b');
  await flush();
  assert.deepEqual(received, []);
  assert.equal(source.cancelMigration(snap.handoffToken), true);
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  assert.equal(source.getStats().migratingSubscribers, 0);
  // The token is forgotten: neither finish path works twice.
  assert.equal(source.cancelMigration(snap.handoffToken), false);
  assert.equal(source.completeMigration(snap.handoffToken), false);
  sub.unsubscribe();
});

test('cancelMigration restores in-flight reliable deliveries (at-least-once)', async () => {
  const source = new EventBus();
  const deliveries: Delivery<BusMessage>[] = [];
  const sub = source.subscribeReliable('t', (d) => {
    deliveries.push(d);
  });
  source.publish('t', 'x');
  source.publish('t', 'y');
  await flush();
  assert.equal(deliveries.length, 2);

  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.pendingMessages.length, 2);
  assert.equal(source.cancelMigration(snap.handoffToken), true);
  await flush();
  // The taken deliveries come back as redeliveries — at-least-once, in order.
  assert.equal(deliveries.length, 4);
  assert.deepEqual(
    deliveries.slice(2).map((d) => d.msg.payload),
    ['x', 'y'],
  );
  for (const d of deliveries) d.ack();
  sub.unsubscribe();
});

test('double import with the same token is rejected: no double subscription', () => {
  const source = new EventBus();
  const sub = source.subscribe('t', noop);
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  const target = new EventBus();
  target.importSubscription(snap, noop);
  assert.throws(() => target.importSubscription(snap, noop), RangeError);
  assert.equal(target.getStats().totalSubscribers, 1);
  source.completeMigration(snap.handoffToken);
});

test('import validates the snapshot and the handler', () => {
  const source = new EventBus();
  const sub = source.subscribe('t', noop);
  const good = source.exportSubscription(sub.id);
  assert(good !== null);
  const target = new EventBus();
  assert.throws(() => target.importSubscription(null as unknown as SubscriptionSnapshot, noop), RangeError);
  assert.throws(() => target.importSubscription({} as unknown as SubscriptionSnapshot, noop), RangeError);
  assert.throws(
    () => target.importSubscription({ ...good, kind: 'nope' } as unknown as SubscriptionSnapshot, noop),
    RangeError,
  );
  assert.throws(
    () =>
      target.importSubscription(
        { ...good, handoffToken: 'xyz' } as unknown as SubscriptionSnapshot,
        noop,
      ),
    RangeError,
  );
  assert.throws(
    () => target.importSubscription(good, 'not-a-function' as never),
    RangeError,
  );
  // Nothing half-registered by the failed imports.
  assert.equal(target.getStats().totalSubscribers, 0);
  source.completeMigration(good.handoffToken);
});

test('import fails fast like a fresh subscribe when the target lacks the namespace', async () => {
  const source = new EventBus();
  const ns = source.createNamespace('t1');
  const sub = ns.subscribe('orders.*', noop);
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.namespace, 't1');
  // The stored pattern is the registration-time (pre-translation) form.
  assert.equal(snap.pattern, 'orders.*');

  const target = new EventBus();
  // Fail-fast: unknown namespace throws, the token stays unconsumed.
  assert.throws(() => target.importSubscription(snap, noop), RangeError);
  assert.equal(target.getStats().totalSubscribers, 0);

  const tns = target.createNamespace('t1');
  const got: unknown[] = [];
  target.importSubscription(snap, (m) => got.push(m.payload));
  tns.publish('orders.new', { id: 7 });
  await flush();
  assert.deepEqual(got, [{ id: 7 }]);
  source.completeMigration(snap.handoffToken);
});

test('composite patterns migrate with their include/exclude rules', async () => {
  const source = new EventBus();
  const sub = source.subscribe('**', noop, {
    patterns: { patterns: ['market.btc.*', 'market.eth.*'], mode: 'any' },
  });
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.pattern, '**');
  assert.deepEqual(snap.options.patterns, {
    patterns: ['market.btc.*', 'market.eth.*'],
    mode: 'any',
  });

  const target = new EventBus();
  const got: string[] = [];
  target.importSubscription(snap, (m) => got.push(m.topic));
  target.publish('market.btc.trades', 1);
  target.publish('market.sol.trades', 2);
  await flush();
  assert.deepEqual(got, ['market.btc.trades']);
  source.completeMigration(snap.handoffToken);
});

test('non-serializable options are reported, not silently dropped', () => {
  const source = new EventBus();
  const sub = source.subscribe('t', noop, {
    queueSize: 50,
    filter: () => true,
    onBackpressure: () => {},
  });
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.deepEqual([...snap.unsupportedOptions].sort(), ['filter', 'onBackpressure']);
  assert.equal(snap.options.queueSize, 50);
  assert(!('filter' in snap.options));
  assert(!('onBackpressure' in snap.options));

  const target = new EventBus();
  target.importSubscription(snap, noop);
  assert.equal(target.getStats().totalSubscribers, 1);
  source.completeMigration(snap.handoffToken);
});

test('the lease migrates with its remaining time, not the original duration', async () => {
  const source = new EventBus();
  const sub = source.subscribe('t', noop, { leaseMs: 600 });
  await sleep(350);
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  // Remaining time: strictly below the original full duration.
  assert(snap.leaseRemainingMs < 600);
  assert(snap.leaseRemainingMs > 0);

  const target = new EventBus();
  target.importSubscription(snap, noop);
  assert.equal(target.getStats().totalSubscribers, 1);
  // The remaining ~250ms elapse well before the original 600ms would:
  // if the target had armed the full duration, the subscription would
  // still be alive here.
  await sleep(500);
  assert.equal(target.getStats().totalSubscribers, 0);
  assert.equal(target.getStats().expiredLeases, 1);
  source.completeMigration(snap.handoffToken);
});

test('stats stay consistent: migrating subscriptions still count as live', async () => {
  const source = new EventBus();
  const a = source.subscribe('a', noop);
  const b = source.subscribe('b', noop);
  assert.equal(source.getStats().migratingSubscribers, 0);
  const snap = source.exportSubscription(a.id);
  assert(snap !== null);
  const stats = source.getStats();
  assert.equal(stats.totalSubscribers, 2);
  assert.equal(stats.migratingSubscribers, 1);
  source.completeMigration(snap.handoffToken);
  b.unsubscribe();
});
