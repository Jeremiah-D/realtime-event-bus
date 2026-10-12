import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  AclDeniedError,
  type BackpressureEvent,
  type GroupRebalanceEvent,
  type LeaseExpiredEvent,
} from '../src/bus.ts';
import { NamespaceDeniedError } from '../src/namespace.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('updateSubscriptionPattern swaps the live match surface', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('a.*', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('a.1', 'one'), 1);
  await flush();
  assert.deepEqual(received, ['one']);

  assert.equal(bus.updateSubscriptionPattern(sub.id, 'b.*'), true);
  assert.equal(bus.publish('a.2', 'old'), 0);
  assert.equal(bus.publish('b.2', 'new'), 1);
  await flush();
  assert.deepEqual(received, ['one', 'new']);
});

test('updateSubscriptionPattern returns false for an unknown subscription id', () => {
  const bus = new EventBus();
  assert.equal(bus.updateSubscriptionPattern('sub-nope', 'a.*'), false);
});

test('updateSubscriptionPattern rejects an empty pattern and keeps the old one', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('a.*', (msg) => received.push(msg.payload));
  assert.throws(
    () => bus.updateSubscriptionPattern(sub.id, ''),
    (err: unknown) => err instanceof RangeError,
  );
  // Unknown ids return false before validation — no throw, no mutation.
  assert.equal(bus.updateSubscriptionPattern('sub-nope', ''), false);
  assert.equal(bus.publish('a.1', 'still-here'), 1);
  await flush();
  assert.deepEqual(received, ['still-here']);
});

test('updateSubscriptionPattern to the same pattern is a no-op', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('a.*', (msg) => received.push(msg.payload));
  assert.equal(bus.updateSubscriptionPattern(sub.id, 'a.*'), true);
  assert.equal(bus.publish('a.1', 'x'), 1);
  await flush();
  assert.deepEqual(received, ['x']);
  assert.equal(bus.getStats().patternCacheSize, 1);
});

test('pattern cache is shared and evicted like unsubscribe', () => {
  const bus = new EventBus();
  const noop = () => {};
  const s1 = bus.subscribe('p.*', noop);
  const s2 = bus.subscribe('p.*', noop);
  assert.equal(bus.getStats().patternCacheSize, 1);
  assert.equal(bus.updateSubscriptionPattern(s1.id, 'q.*'), true);
  // Both patterns still live: p.* has one subscriber left, q.* is compiled.
  assert.equal(bus.getStats().patternCacheSize, 2);
  assert.equal(bus.updateSubscriptionPattern(s2.id, 'q.*'), true);
  // p.* lost its last subscriber: evicted from the cache.
  assert.equal(bus.getStats().patternCacheSize, 1);
});

test('queued messages and per-topic sequence state survive the swap', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('a.*', (msg) => received.push(msg.payload), { queueSize: 10 });
  bus.publish('a.1', 'm1');
  bus.publish('a.1', 'm2');
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  // Sequence baselines are per topic and must not reset: a fresh
  // subscriber on the new pattern would count these as gaps.
  assert.equal(bus.updateSubscriptionPattern(sub.id, 'b.*'), true);
  bus.publish('b.2', 'm3');
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3']);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('lease is preserved and its expiry event names the new pattern', async () => {
  const expired: LeaseExpiredEvent[] = [];
  const bus = new EventBus({ onLeaseExpired: (e) => expired.push(e) });
  const sub = bus.subscribe('a.*', () => {}, { leaseMs: 40 });
  assert.equal(bus.updateSubscriptionPattern(sub.id, 'b.*'), true);
  await new Promise((resolve) => setTimeout(resolve, 150));
  assert.equal(expired.length, 1);
  assert.equal(expired[0].pattern, 'b.*');
  // The lease fired through the original unsubscribe path: the
  // subscription is gone.
  assert.equal(bus.publish('b.1', 'x'), 0);
});

test('backpressure events name the live pattern after the swap', async () => {
  const bus = new EventBus();
  const seen: BackpressureEvent[] = [];
  const sub = bus.subscribe('a.*', () => {}, {
    queueSize: 2,
    onBackpressure: (e) => seen.push(e),
  });
  assert.equal(bus.updateSubscriptionPattern(sub.id, 'b.*'), true);
  // Two synchronous publishes cross the 80% high-water mark before the
  // scheduled flush drains the queue.
  bus.publish('b.1', 1);
  bus.publish('b.2', 2);
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(seen[0].pattern, 'b.*');
  assert.equal(seen[0].subscriberId, sub.id);
});

test('group members move competing sets and both groups get a rebalance', async () => {
  const bus = new EventBus();
  const r1: unknown[] = [];
  const r2: unknown[] = [];
  const e1: GroupRebalanceEvent[] = [];
  const e2: GroupRebalanceEvent[] = [];
  const m1 = bus.subscribeToGroup('g', 'm.*', (msg) => r1.push(msg.payload), {
    onRebalance: (e) => e1.push(e),
  });
  const m2 = bus.subscribeToGroup('g', 'm.*', (msg) => r2.push(msg.payload), {
    onRebalance: (e) => e2.push(e),
  });
  e1.length = 0;
  e2.length = 0;
  bus.publish('m.1', 'one');
  bus.publish('m.2', 'two');
  await flush();
  assert.equal(r1.length + r2.length, 2);

  assert.equal(bus.updateSubscriptionPattern(m2.id, 'n.*'), true);
  // Old set: the remaining member sees a leave with itself as the roster.
  const leave = e1.filter((e) => e.trigger === 'leave');
  assert.equal(leave.length, 1);
  assert.equal(leave[0].memberId, m2.id);
  assert.deepEqual(leave[0].members, [m1.id]);
  // New set: the mover sees its own join as the first member.
  const join = e2.filter((e) => e.trigger === 'join');
  assert.equal(join.length, 1);
  assert.equal(join[0].memberId, m2.id);
  assert.equal(join[0].groupId, 'g');
  assert.deepEqual(join[0].members, [m2.id]);

  r1.length = 0;
  r2.length = 0;
  assert.equal(bus.publish('m.3', 'old-group'), 1);
  assert.equal(bus.publish('n.3', 'new-group'), 1);
  await flush();
  assert.deepEqual(r1, ['old-group']);
  assert.deepEqual(r2, ['new-group']);
});

test('composite subscriptions keep their include/exclude rules across the swap', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('old.*', (msg) => received.push(msg.payload), {
    patterns: { patterns: ['new.**'], mode: 'any', exclude: [] },
  });
  assert.equal(bus.publish('new.1', 'blocked-by-base'), 0);
  await flush();
  assert.deepEqual(received, []);

  assert.equal(bus.updateSubscriptionPattern(sub.id, 'new.*'), true);
  assert.equal(bus.publish('new.1', 'through'), 1);
  await flush();
  assert.deepEqual(received, ['through']);
});

test('ACL denial on the new pattern leaves the old pattern in force', async () => {
  const denied: string[] = [];
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'secret.**', subscribe: 'deny' }] },
    onAuthzDenied: (e) => denied.push(e.pattern ?? ''),
  });
  const received: unknown[] = [];
  const sub = bus.subscribe('public.*', (msg) => received.push(msg.payload));
  assert.throws(
    () => bus.updateSubscriptionPattern(sub.id, 'secret.**'),
    (err: unknown) => err instanceof AclDeniedError,
  );
  assert.deepEqual(denied, ['secret.**']);
  assert.equal(bus.publish('public.1', 'still-live'), 1);
  await flush();
  assert.deepEqual(received, ['still-live']);
});

test('namespaced subscriptions stay inside their namespace', async () => {
  const bus = new EventBus();
  const ns = bus.createNamespace('t1');
  const received: unknown[] = [];
  const sub = ns.subscribe('a.*', (msg) => received.push(msg.payload));
  assert.equal(ns.publish('a.1', 'one'), 1);
  await flush();
  assert.deepEqual(received, ['one']);

  assert.equal(bus.updateSubscriptionPattern(sub.id, 'b.*'), true);
  assert.equal(ns.publish('a.2', 'old'), 0);
  assert.equal(ns.publish('b.2', 'new'), 1);
  await flush();
  assert.deepEqual(received, ['one', 'new']);

  // A relative pattern containing '/' is an escape attempt, like at
  // subscribe time — rejected, old pattern untouched.
  assert.throws(
    () => bus.updateSubscriptionPattern(sub.id, '../x'),
    (err: unknown) => err instanceof NamespaceDeniedError,
  );
  assert.equal(ns.publish('b.3', 'still-here'), 1);
  await flush();
  assert.deepEqual(received, ['one', 'new', 'still-here']);
});
