import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, SubscriptionLimitError } from '../src/bus.ts';

const noop = () => {};

test('setTopicMaxSubscribers: cap enforced fail-fast with SubscriptionLimitError', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('orders', 2);
  const a = bus.subscribe('orders', noop);
  const b = bus.subscribe('orders', noop);
  assert.throws(() => bus.subscribe('orders', noop), SubscriptionLimitError);
  try {
    bus.subscribe('orders', noop);
    assert.fail('expected SubscriptionLimitError');
  } catch (err) {
    assert.ok(err instanceof SubscriptionLimitError);
    assert.ok(err instanceof Error);
    assert.equal(err.name, 'SubscriptionLimitError');
    assert.equal(err.pattern, 'orders');
    assert.equal(err.limit, 2);
    assert.equal(err.current, 2);
    assert.match(err.message, /subscriber limit reached/);
  }
  a.unsubscribe();
  b.unsubscribe();
});

test('setTopicMaxSubscribers: invalid configs throw RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicMaxSubscribers('', 2), RangeError);
  assert.throws(() => bus.setTopicMaxSubscribers('t', 0), RangeError);
  assert.throws(() => bus.setTopicMaxSubscribers('t', -1), RangeError);
  assert.throws(() => bus.setTopicMaxSubscribers('t', 1.5), RangeError);
  assert.throws(() => bus.setTopicMaxSubscribers('t', Number.NaN), RangeError);
});

test('setTopicMaxSubscribers: unsubscribe frees a slot', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('t', 1);
  const a = bus.subscribe('t', noop);
  assert.throws(() => bus.subscribe('t', noop), SubscriptionLimitError);
  a.unsubscribe();
  const b = bus.subscribe('t', noop); // slot freed
  b.unsubscribe();
});

test('setTopicMaxSubscribers: lowering the cap never kicks existing subscribers', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('t', 2);
  const a = bus.subscribe('t', noop);
  const b = bus.subscribe('t', noop);
  bus.setTopicMaxSubscribers('t', 1); // below the live count: no kick
  // Existing subscriptions keep working.
  const seen: unknown[] = [];
  const c = bus.subscribe('other', (msg) => seen.push(msg.payload));
  a.unsubscribe();
  // ...but new registrations are rejected.
  assert.throws(() => bus.subscribe('t', noop), SubscriptionLimitError);
  b.unsubscribe();
  c.unsubscribe();
});

test('clearTopicMaxSubscribers: lifts the cap', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('t', 1);
  bus.subscribe('t', noop);
  assert.throws(() => bus.subscribe('t', noop), SubscriptionLimitError);
  assert.equal(bus.clearTopicMaxSubscribers('t'), true);
  const extra = bus.subscribe('t', noop); // cap lifted
  assert.equal(bus.clearTopicMaxSubscribers('t'), false); // already gone
  assert.equal(bus.clearTopicMaxSubscribers('never-set'), false);
  extra.unsubscribe();
});

test('setTopicMaxSubscribers: pattern rules cap matching subscriptions, exact rule wins', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('market.**', 1);
  const a = bus.subscribe('market.btc.trades', noop);
  // A different concrete topic resolves to the same pattern rule.
  assert.throws(() => bus.subscribe('market.eth.trades', noop), (err: unknown) => {
    assert.ok(err instanceof SubscriptionLimitError);
    assert.equal(err.pattern, 'market.eth.trades');
    assert.equal(err.limit, 1);
    return true;
  });
  // An exact-topic rule for btc wins over the pattern rule — and rule
  // resolution is live (like TTL/rate-limit rules): the existing btc
  // subscriber now counts against the exact rule, freeing the pattern
  // rule's slot.
  bus.setTopicMaxSubscribers('market.btc.trades', 2);
  const b = bus.subscribe('market.btc.trades', noop); // 2/2 on the exact rule
  assert.throws(() => bus.subscribe('market.btc.trades', noop), (err: unknown) => {
    assert.ok(err instanceof SubscriptionLimitError);
    assert.equal(err.limit, 2);
    return true;
  });
  const e1 = bus.subscribe('market.eth.trades', noop); // pattern rule freed by the migration
  assert.throws(() => bus.subscribe('market.eth.trades', noop), SubscriptionLimitError);
  a.unsubscribe();
  b.unsubscribe();
  e1.unsubscribe();
});

test('setTopicMaxSubscribers: first-registered pattern wins between patterns', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('a.**', 1);
  bus.setTopicMaxSubscribers('a.b.**', 10);
  bus.subscribe('a.b.c', noop); // resolves to 'a.**' (earliest-registered)
  assert.throws(() => bus.subscribe('a.b.d', noop), (err: unknown) => {
    assert.ok(err instanceof SubscriptionLimitError);
    assert.equal(err.limit, 1); // the 'a.**' cap, not 10
    return true;
  });
});

test('setTopicMaxSubscribers: group members and shadow mirrors count', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('jobs', 2);
  const m1 = bus.subscribeToGroup('g1', 'jobs', noop);
  const m2 = bus.subscribeToGroup('g1', 'jobs', noop);
  // The third group member hits the same cap.
  assert.throws(() => bus.subscribeToGroup('g1', 'jobs', noop), SubscriptionLimitError);
  m1.unsubscribe();
  m2.unsubscribe();

  const bus2 = new EventBus();
  bus2.setTopicMaxSubscribers('t', 1);
  const s = bus2.subscribe('t', noop, { shadow: true });
  assert.throws(() => bus2.subscribe('t', noop), SubscriptionLimitError);
  s.unsubscribe();
});

test('setTopicMaxSubscribers: subscribeReliable is gated too', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('t', 1);
  const r = bus.subscribeReliable('t', noop);
  assert.throws(() => bus.subscribeReliable('t', noop), SubscriptionLimitError);
  r.unsubscribe();
});

test('getStats().topicSubscriberLimits exposes rule, cap and live count', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.getStats().topicSubscriberLimits, []);
  bus.setTopicMaxSubscribers('a', 2);
  bus.setTopicMaxSubscribers('b.**', 3);
  const s1 = bus.subscribe('a', noop);
  const s2 = bus.subscribe('a', noop);
  const s3 = bus.subscribe('b.c', noop);
  assert.deepEqual(bus.getStats().topicSubscriberLimits, [
    { rule: 'a', maxSubscribers: 2, currentSubscribers: 2 },
    { rule: 'b.**', maxSubscribers: 3, currentSubscribers: 1 },
  ]);
  s1.unsubscribe();
  assert.deepEqual(
    bus.getStats().topicSubscriberLimits[0],
    { rule: 'a', maxSubscribers: 2, currentSubscribers: 1 },
  );
  bus.clearTopicMaxSubscribers('a');
  assert.deepEqual(
    bus.getStats().topicSubscriberLimits.map((r) => r.rule),
    ['b.**'],
  );
  s2.unsubscribe();
  s3.unsubscribe();
});

test('setTopicMaxSubscribers: topics without a rule are unaffected', () => {
  const bus = new EventBus();
  bus.setTopicMaxSubscribers('capped', 1);
  const subs = Array.from({ length: 10 }, () => bus.subscribe('free', noop));
  for (const s of subs) s.unsubscribe();
});
