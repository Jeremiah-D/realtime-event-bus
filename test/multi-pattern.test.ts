import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  AclDeniedError,
  type BusMessage,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const topicsOf = (received: BusMessage[]): string[] => received.map((m) => m.topic);

test('multi-pattern: OR semantics across independent patterns, one subscriber', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe(['orders.*', 'payments.**'], (m) => received.push(m));

  bus.publish('orders.created', { id: 1 });
  bus.publish('payments.eu.settled', { id: 2 });
  bus.publish('inventory.low', { id: 3 });
  bus.publish('orders', { id: 4 }); // 'orders.*' needs one segment — no match
  await flush();

  assert.deepEqual(topicsOf(received), ['orders.created', 'payments.eu.settled']);
  const stats = bus.getStats();
  assert.equal(stats.totalSubscribers, 1);
  assert.equal(stats.multiPatternSubscriptions, 1);
  assert.equal(stats.compositeSubscriptions, 0);
  // Per-pattern stats stay visible in getStats.
  assert.deepEqual(stats.subscribersByPattern, { 'orders.*': 1, 'payments.**': 1 });
  sub.unsubscribe();
  bus.destroy?.();
});

test('multi-pattern: a topic matching several patterns is delivered exactly once', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe(['a.*', 'a.b', '**.b'], (m) => received.push(m));
  bus.publish('a.b', { n: 1 });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].topic, 'a.b');
  bus.destroy?.();
});

test('multi-pattern: patterns share one FIFO queue — cross-pattern order preserved', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe(['p1.*', 'p2.*'], (m) => received.push(m));
  bus.publish('p2.z', { n: 1 });
  bus.publish('p1.a', { n: 2 });
  bus.publish('p2.y', { n: 3 });
  await flush();
  assert.deepEqual(topicsOf(received), ['p2.z', 'p1.a', 'p2.y']);
  bus.destroy?.();
});

test('multi-pattern: dedup window is shared across the aggregate', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe(['d.*', 'e.*'], (m) => received.push(m), {
    deduplicateMessages: { windowMs: 60_000 },
  });
  bus.publish('d.x', { n: 1 }, { messageId: 'm-1' });
  bus.publish('d.x', { n: 1 }, { messageId: 'm-1' });
  bus.publish('e.y', { n: 2 }, { messageId: 'm-1' }); // different topic: not a dup
  await flush();
  assert.deepEqual(topicsOf(received), ['d.x', 'e.y']);
  bus.destroy?.();
});

test('multi-pattern: validation fails fast — empty array, non-string, empty pattern', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribe([], () => {}), RangeError);
  assert.throws(
    () => bus.subscribe(['a.*', 42 as unknown as string], () => {}),
    TypeError,
  );
  assert.throws(() => bus.subscribe(['a.*', ''], () => {}), RangeError);
  assert.throws(() => bus.subscribe([''], () => {}), RangeError);
  // Nothing registered by the failed calls.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.deepEqual(bus.getStats().subscribersByPattern, {});
  bus.destroy?.();
});

test('multi-pattern: duplicates are deduped to one entry per pattern', () => {
  const bus = new EventBus();
  const sub = bus.subscribe(['a.*', 'a.*', 'b.*', 'a.*'], () => {});
  assert.deepEqual(bus.getStats().subscribersByPattern, { 'a.*': 1, 'b.*': 1 });
  sub.unsubscribe();
  assert.deepEqual(bus.getStats().subscribersByPattern, {});
  bus.destroy?.();
});

test('multi-pattern: one unsubscribe tears down every pattern', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe(['u1.*', 'u2.*'], (m) => received.push(m));
  bus.publish('u1.a', {});
  await flush();
  assert.equal(received.length, 1);

  sub.unsubscribe();
  const stats = bus.getStats();
  assert.equal(stats.totalSubscribers, 0);
  assert.equal(stats.multiPatternSubscriptions, 0);
  assert.deepEqual(stats.subscribersByPattern, {});
  assert.equal(stats.patternCacheSize, 0);

  // No delivery after teardown, on either pattern.
  bus.publish('u1.a', {});
  bus.publish('u2.b', {});
  await flush();
  assert.equal(received.length, 1);
  bus.destroy?.();
});

test('multi-pattern: ACL is evaluated per pattern — any deny rejects the whole call', () => {
  const authzEvents: Array<{ action: string; pattern: string }> = [];
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', subscribe: 'deny' }] },
    onAuthzDenied: (e) => authzEvents.push({ action: e.action, pattern: e.pattern }),
  });
  const before = bus.getStats().totalSubscribers;

  assert.throws(
    () => bus.subscribe(['market.*', 'admin.cmd'], () => {}),
    (err: unknown) => {
      assert.ok(err instanceof AclDeniedError);
      assert.equal((err as AclDeniedError).pattern, 'admin.cmd');
      return true;
    },
  );
  // Nothing registered — not even the allowed pattern.
  assert.equal(bus.getStats().totalSubscribers, before);
  assert.deepEqual(bus.getStats().subscribersByPattern, {});
  assert.equal(authzEvents.length, 1);
  assert.deepEqual(authzEvents[0], { action: 'subscribe', pattern: 'admin.cmd' });

  // All-allowed patterns still subscribe fine.
  bus.subscribe(['market.*', 'orders.*'], () => {});
  assert.equal(bus.getStats().totalSubscribers, before + 1);
  bus.destroy?.();
});

test('multi-pattern: group subscribe rejects arrays (EB-16 group semantics)', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeToGroup('g1', ['a.*', 'b.*'] as unknown as string, () => {}),
    TypeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
  // Single-pattern group subscribe still works.
  const sub = bus.subscribeToGroup('g1', 'a.*', () => {});
  assert.equal(bus.getStats().totalSubscribers, 1);
  sub.unsubscribe();
  bus.destroy?.();
});

test('multi-pattern: traceId, key, messageId and TTL deadline pass through unchanged', async () => {
  const bus = new EventBus({ trace: true });
  const received: BusMessage[] = [];
  bus.subscribe(['t1.*', 't2.*'], (m) => received.push(m));
  const upstreamTraceId = 'e'.repeat(32);
  bus.publish(
    't2.x',
    { v: 1 },
    {
      traceparent: `00-${upstreamTraceId}-${'f'.repeat(16)}-01`,
      key: 'k-9',
      messageId: 'mid-7',
      ttlMs: 60_000,
    },
  );
  await flush();
  // The TTL'd message is delivered (not wrongly expired): the deadline
  // rides the publish path untouched by the subscribe form.
  assert.equal(received.length, 1);
  const msg = received[0];
  assert.equal(msg.key, 'k-9');
  assert.equal(msg.messageId, 'mid-7');
  // The upstream trace continues end-to-end through the multi-pattern fan-out.
  const publishSpan = bus.getStats().traceSpans.find((s) => s.name === 'bus.publish');
  assert.ok(publishSpan);
  assert.equal(publishSpan.traceId, upstreamTraceId);
  bus.destroy?.();
});

test('multi-pattern: updateSubscriptionPattern is rejected for aggregates', () => {
  const bus = new EventBus();
  const sub = bus.subscribe(['w1.*', 'w2.*'], () => {});
  assert.throws(() => bus.updateSubscriptionPattern(sub.id, 'w3.*'), RangeError);
  // The aggregate is untouched.
  assert.deepEqual(bus.getStats().subscribersByPattern, { 'w1.*': 1, 'w2.*': 1 });
  sub.unsubscribe();
  bus.destroy?.();
});

test('multi-pattern: coexists with composite (EB-70) — distinct semantics', async () => {
  const bus = new EventBus();
  const multiReceived: BusMessage[] = [];
  const compositeReceived: BusMessage[] = [];
  // Multi-pattern: pure OR — no exclude applies.
  bus.subscribe(['c1.*', 'c2.*'], (m) => multiReceived.push(m));
  // Composite: include/exclude layering (EB-70).
  bus.subscribe('**', (m) => compositeReceived.push(m), {
    patterns: { patterns: ['c1.*', 'c2.*'], exclude: ['c2.secret'] },
  });
  bus.publish('c2.secret', { n: 1 });
  bus.publish('c1.ok', { n: 2 });
  await flush();
  // The multi-pattern aggregate delivers both — it has no exclude.
  assert.deepEqual(topicsOf(multiReceived), ['c2.secret', 'c1.ok']);
  // The composite drops the excluded topic.
  assert.deepEqual(topicsOf(compositeReceived), ['c1.ok']);
  assert.equal(bus.getStats().multiPatternSubscriptions, 1);
  assert.equal(bus.getStats().compositeSubscriptions, 1);
  bus.destroy?.();
});

test('multi-pattern: migration export carries patterns; import rebuilds the aggregate', async () => {
  const source = new EventBus();
  const sourceReceived: BusMessage[] = [];
  const sub = source.subscribe(['mig1.*', 'mig2.**'], (m) => sourceReceived.push(m.payload), {
    queueSize: 10,
  });
  source.publish('mig1.a', { id: 1 });
  source.publish('mig2.deep.x', { id: 2 });
  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);
  assert.equal(snap.pattern, 'mig1.*');
  assert.deepEqual(snap.patterns, ['mig1.*', 'mig2.**']);
  assert.equal(snap.pendingMessages.length, 2);

  const target = new EventBus();
  const targetReceived: BusMessage[] = [];
  const res = target.importSubscription(snap, (m) => targetReceived.push(m));
  assert.equal(res.handoffToken, snap.handoffToken);
  await flush();
  assert.deepEqual(
    targetReceived.map((m) => m.payload),
    [{ id: 1 }, { id: 2 }],
  );
  const targetStats = target.getStats();
  assert.equal(targetStats.multiPatternSubscriptions, 1);
  assert.deepEqual(targetStats.subscribersByPattern, { 'mig1.*': 1, 'mig2.**': 1 });

  // The rebuilt aggregate matches on both patterns.
  target.publish('mig2.other', { id: 3 });
  await flush();
  assert.equal(targetReceived.length, 3);
  source.destroy?.();
  target.destroy?.();
});

test('multi-pattern: getStats lifecycle — 0 → 1 → 0 across subscribe/unsubscribe', () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().multiPatternSubscriptions, 0);
  const sub = bus.subscribe(['s1.*', 's2.*'], () => {});
  assert.equal(bus.getStats().multiPatternSubscriptions, 1);
  sub.unsubscribe();
  assert.equal(bus.getStats().multiPatternSubscriptions, 0);
  bus.destroy?.();
});

test('multi-pattern: batched overload works with the array form', async () => {
  const bus = new EventBus();
  const batches: BusMessage[][] = [];
  bus.subscribe(
    ['b1.*', 'b2.*'],
    (msgs) => batches.push(msgs),
    { batch: { maxSize: 2 } },
  );
  bus.publish('b1.a', { n: 1 });
  bus.publish('b2.b', { n: 2 });
  await flush();
  assert.equal(batches.length, 1);
  assert.deepEqual(topicsOf(batches[0]), ['b1.a', 'b2.b']);
  bus.destroy?.();
});
