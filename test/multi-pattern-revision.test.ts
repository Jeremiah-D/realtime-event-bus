import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  AclDeniedError,
  type BusMessage,
} from '../src/bus.ts';
import { NamespaceDeniedError } from '../src/namespace.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const topicsOf = (received: BusMessage[]): string[] => received.map((m) => m.topic);

const revisionAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'multi_patterns_revised' });

test('updateSubscriptionMultiPatterns revises the set live, audits, and drops old patterns', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe(['orders.*', 'payments.**'], (m) => received.push(m));
  bus.publish('orders.created', { id: 1 });
  await flush();
  assert.deepEqual(topicsOf(received), ['orders.created']);

  assert.equal(bus.updateSubscriptionMultiPatterns(sub.id, ['payments.**', 'inventory.*']), true);
  bus.publish('orders.created', { id: 2 }); // dropped pattern — no match
  bus.publish('payments.eu.settled', { id: 3 }); // retained pattern
  bus.publish('inventory.low', { id: 4 }); // added pattern
  await flush();
  assert.deepEqual(topicsOf(received), ['orders.created', 'payments.eu.settled', 'inventory.low']);
  assert.deepEqual(bus.getStats().subscribersByPattern, {
    'payments.**': 1,
    'inventory.*': 1,
  });
  const events = revisionAudits(bus).filter((e) => e.subId === sub.id);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    patterns: ['payments.**', 'inventory.*'],
    oldPatterns: ['orders.*', 'payments.**'],
  });
  sub.unsubscribe();
  bus.destroy?.();
});

test('updateSubscriptionMultiPatterns returns false for unknown ids and refuses single-pattern subscriptions', () => {
  const bus = new EventBus();
  assert.equal(bus.updateSubscriptionMultiPatterns('sub-nope', ['a.*']), false);
  // Unknown ids return false before validation — no throw, no mutation.
  assert.equal(bus.updateSubscriptionMultiPatterns('sub-nope', []), false);
  const single = bus.subscribe('a.*', () => {});
  assert.throws(
    () => bus.updateSubscriptionMultiPatterns(single.id, ['b.*', 'c.*']),
    RangeError,
  );
  assert.deepEqual(bus.getStats().subscribersByPattern, { 'a.*': 1 });
  assert.equal(revisionAudits(bus).length, 0);
  single.unsubscribe();
  bus.destroy?.();
});

test('updateSubscriptionMultiPatterns rejects bad sets fail-closed', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe(['a.*', 'b.*'], (m) => received.push(m));
  const badSets = [
    [],
    'a.*',
    ['a.*', 42],
    ['a.*', ''],
    ['a.*', 'a.*'],
    [null],
  ] as unknown as string[][];
  for (const bad of badSets) {
    assert.throws(() => bus.updateSubscriptionMultiPatterns(sub.id, bad), RangeError);
  }
  assert.equal(revisionAudits(bus).length, 0);
  assert.deepEqual(bus.getStats().subscribersByPattern, { 'a.*': 1, 'b.*': 1 });
  bus.publish('a.1', { n: 1 });
  bus.publish('b.1', { n: 2 });
  await flush();
  assert.deepEqual(topicsOf(received), ['a.1', 'b.1']);
  sub.unsubscribe();
  bus.destroy?.();
});

test('updateSubscriptionMultiPatterns set-equal reorder is a no-op with no audit', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe(['a.*', 'b.*'], (m) => received.push(m));
  assert.equal(bus.updateSubscriptionMultiPatterns(sub.id, ['b.*', 'a.*']), true);
  assert.equal(revisionAudits(bus).length, 0);
  // The stored order is left exactly as it was: a later revision audits
  // against the original order, not the reordered call.
  assert.equal(bus.updateSubscriptionMultiPatterns(sub.id, ['c.*', 'a.*']), true);
  const events = revisionAudits(bus).filter((e) => e.subId === sub.id);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    patterns: ['c.*', 'a.*'],
    oldPatterns: ['a.*', 'b.*'],
  });
  bus.publish('c.1', { n: 1 });
  await flush();
  assert.deepEqual(topicsOf(received), ['c.1']);
  sub.unsubscribe();
  bus.destroy?.();
});

test('updateSubscriptionMultiPatterns keeps shared-pattern counts and other subscribers live', async () => {
  const bus = new EventBus();
  const first: BusMessage[] = [];
  const second: BusMessage[] = [];
  const sub1 = bus.subscribe(['a.*', 'shared.*'], (m) => first.push(m));
  const sub2 = bus.subscribe(['shared.*'], (m) => second.push(m));
  assert.deepEqual(bus.getStats().subscribersByPattern, { 'a.*': 1, 'shared.*': 2 });

  assert.equal(bus.updateSubscriptionMultiPatterns(sub1.id, ['b.*', 'shared.*']), true);
  assert.deepEqual(bus.getStats().subscribersByPattern, { 'shared.*': 2, 'b.*': 1 });

  // Dropping 'shared.*' from sub1 leaves sub2's count and delivery intact.
  assert.equal(bus.updateSubscriptionMultiPatterns(sub1.id, ['b.*']), true);
  assert.deepEqual(bus.getStats().subscribersByPattern, { 'shared.*': 1, 'b.*': 1 });
  bus.publish('shared.x', { n: 1 });
  bus.publish('b.x', { n: 2 });
  await flush();
  assert.deepEqual(topicsOf(first), ['b.x']);
  assert.deepEqual(topicsOf(second), ['shared.x']);
  sub1.unsubscribe();
  sub2.unsubscribe();
  bus.destroy?.();
});

test('updateSubscriptionMultiPatterns leaves already-queued messages untouched', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe(['a.*', 'b.*'], (m) => received.push(m));
  bus.publish('a.1', { n: 1 });
  // Revise before the drain runs: the queued message survives the
  // revision (the queue itself never moves), but later 'a.*' traffic
  // no longer matches.
  assert.equal(bus.updateSubscriptionMultiPatterns(sub.id, ['c.*']), true);
  bus.publish('a.2', { n: 2 });
  bus.publish('c.2', { n: 3 });
  await flush();
  assert.deepEqual(topicsOf(received), ['a.1', 'c.2']);
  sub.unsubscribe();
  bus.destroy?.();
});

test('updateSubscriptionMultiPatterns ACL denial leaves the old set in force', async () => {
  const denied: string[] = [];
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'secret.**', subscribe: 'deny' }] },
    onAuthzDenied: (e) => denied.push(e.pattern ?? ''),
  });
  const received: BusMessage[] = [];
  const sub = bus.subscribe(['public.*', 'other.*'], (m) => received.push(m));
  assert.throws(
    () => bus.updateSubscriptionMultiPatterns(sub.id, ['public.*', 'secret.**']),
    (err: unknown) => {
      assert.ok(err instanceof AclDeniedError);
      assert.equal((err as AclDeniedError).pattern, 'secret.**');
      return true;
    },
  );
  assert.deepEqual(denied, ['secret.**']);
  assert.equal(revisionAudits(bus).length, 0);
  assert.deepEqual(bus.getStats().subscribersByPattern, { 'public.*': 1, 'other.*': 1 });
  bus.publish('public.1', { n: 1 });
  await flush();
  assert.deepEqual(topicsOf(received), ['public.1']);
  sub.unsubscribe();
  bus.destroy?.();
});

test('updateSubscriptionMultiPatterns stays inside its namespace', async () => {
  const bus = new EventBus();
  const ns = bus.createNamespace('t1');
  const received: unknown[] = [];
  const sub = bus.subscribe(['a.*', 'b.*'], (msg) => received.push(msg.payload), {
    namespace: 't1',
  });
  assert.equal(ns.publish('a.1', 'one'), 1);
  await flush();
  assert.deepEqual(received, ['one']);

  assert.equal(bus.updateSubscriptionMultiPatterns(sub.id, ['b.*', 'c.*']), true);
  assert.equal(ns.publish('a.2', 'old'), 0);
  assert.equal(ns.publish('c.2', 'new'), 1);
  await flush();
  assert.deepEqual(received, ['one', 'new']);
  assert.deepEqual(bus.getStats().subscribersByPattern, {
    't1/b.*': 1,
    't1/c.*': 1,
  });
  const events = revisionAudits(bus).filter((e) => e.subId === sub.id);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    patterns: ['t1/b.*', 't1/c.*'],
    oldPatterns: ['t1/a.*', 't1/b.*'],
  });

  // A relative pattern containing '/' is an escape attempt, like at
  // subscribe time — rejected, old set untouched.
  assert.throws(
    () => bus.updateSubscriptionMultiPatterns(sub.id, ['c.*', '../x']),
    (err: unknown) => err instanceof NamespaceDeniedError,
  );
  assert.equal(revisionAudits(bus).length, 1);
  assert.equal(ns.publish('c.3', 'still-here'), 1);
  await flush();
  assert.deepEqual(received, ['one', 'new', 'still-here']);
  sub.unsubscribe();
  bus.destroy?.();
});
