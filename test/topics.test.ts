import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type UnregisteredTopicEvent } from '../src/bus.ts';
import { TopicRegistry, assertValidTopicRegistration } from '../src/topics.ts';
import { renderPrometheus } from '../src/metrics.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('registerTopic creates and updates registrations', () => {
  let nowMs = 1000;
  const bus = new EventBus({ now: () => nowMs });
  assert.equal(
    bus.registerTopic({
      topic: 'orders.created',
      owner: 'payments-svc',
      description: 'new orders',
      schemaVersion: 3,
      tags: ['payments'],
    }),
    true,
  );
  nowMs += 10;
  // Re-registration updates in place and reports false.
  assert.equal(
    bus.registerTopic({ topic: 'orders.created', owner: 'checkout-svc', schemaVersion: 4 }),
    false,
  );
  const info = bus.getTopicInfo('orders.created');
  assert.ok(info !== undefined);
  assert.equal(info.registered, true);
  assert.equal(info.owner, 'checkout-svc');
  assert.equal(info.description, undefined);
  assert.equal(info.schemaVersion, 4);
  assert.equal(info.registeredAt, 1000);
  assert.equal(info.updatedAt, 1010);
  assert.deepEqual(info.tags, undefined);
});

test('registerTopic validates its input', () => {
  const bus = new EventBus();
  assert.throws(() => bus.registerTopic({ topic: '' }), RangeError);
  assert.throws(() => bus.registerTopic({ topic: 't', owner: '' }), RangeError);
  assert.throws(() => bus.registerTopic({ topic: 't', schemaVersion: -1 }), RangeError);
  assert.throws(() => bus.registerTopic({ topic: 't', schemaVersion: NaN }), RangeError);
  assert.throws(() => bus.registerTopic({ topic: 't', tags: ['ok', ''] }), RangeError);
  assert.throws(() => bus.registerTopic({ topic: 't', tags: 'x' as never }), RangeError);
  assert.equal(bus.topicCatalog().length, 0);
});

test('registerTopic validates the actor', () => {
  const bus = new EventBus();
  assert.throws(() => bus.registerTopic({ topic: 't' }, ''), RangeError);
  assert.throws(() => bus.deregisterTopic('t', ''), RangeError);
  assert.equal(bus.getTopicInfo('t'), undefined);
});

test('deregisterTopic keeps a tombstone with last-known metadata', () => {
  let nowMs = 2000;
  const bus = new EventBus({ now: () => nowMs });
  bus.registerTopic({ topic: 'legacy.events', owner: 'old-svc', description: 'retired' });
  nowMs += 5;
  assert.equal(bus.deregisterTopic('legacy.events'), true);
  assert.equal(bus.deregisterTopic('legacy.events'), false);
  assert.equal(bus.deregisterTopic('never.existed'), false);
  const info = bus.getTopicInfo('legacy.events');
  assert.ok(info !== undefined);
  assert.equal(info.registered, false);
  assert.equal(info.owner, 'old-svc');
  assert.equal(info.deregisteredAt, 2005);
  // Live catalog hides the tombstone; the full catalog keeps it.
  assert.deepEqual(bus.topicCatalog().map((t) => t.topic), []);
  const full = bus.topicCatalog({ includeDeregistered: true });
  assert.deepEqual(full.map((t) => t.topic), ['legacy.events']);
  assert.equal(full[0].registered, false);
});

test('deregisterTopic rejects an empty topic', () => {
  const bus = new EventBus();
  assert.throws(() => bus.deregisterTopic(''), RangeError);
});

test('a deregistered topic re-registers fresh keeping its original registeredAt', () => {
  let nowMs = 3000;
  const bus = new EventBus({ now: () => nowMs });
  bus.registerTopic({ topic: 't', owner: 'a' });
  nowMs += 1;
  bus.deregisterTopic('t');
  nowMs += 1;
  assert.equal(bus.registerTopic({ topic: 't', owner: 'b' }), true);
  const info = bus.getTopicInfo('t');
  assert.ok(info !== undefined);
  assert.equal(info.registered, true);
  assert.equal(info.registeredAt, 3000);
  assert.equal(info.owner, 'b');
  assert.equal(info.deregisteredAt, 3001);
});

test('publishes to unregistered topics are counted, never blocked', async () => {
  const bus = new EventBus();
  bus.registerTopic({ topic: 'known.topic' });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('known.topic', 'a'), 1);
  assert.equal(bus.publish('mystery.topic', 'b'), 1);
  assert.equal(bus.publish('mystery.topic', 'c'), 1);
  await flush();
  assert.deepEqual(received, ['a', 'b', 'c']);
  assert.equal(bus.getStats().unregisteredTopicPublishes, 2);
  assert.equal(bus.getStats().totalPublished, 3);
});

test('onUnregisteredTopic fires per occurrence and is error-isolated', async () => {
  const events: UnregisteredTopicEvent[] = [];
  let calls = 0;
  const bus = new EventBus({
    onUnregisteredTopic: (e) => {
      calls += 1;
      events.push(e);
      if (calls === 1) throw new Error('broken observer');
    },
  });
  assert.equal(bus.publish('ghost.topic', 'x'), 0); // no subscribers; still counted
  assert.equal(bus.publish('ghost.topic', 'y'), 0);
  await flush();
  assert.equal(calls, 2);
  assert.equal(events[0].topic, 'ghost.topic');
  assert.equal(events[0].unregisteredPublishes, 1);
  assert.equal(events[1].unregisteredPublishes, 2);
  assert.ok(typeof events[0].at === 'number');
  assert.equal(bus.getStats().unregisteredTopicPublishes, 2);
});

test('no hook means counting only', () => {
  const bus = new EventBus();
  bus.publish('nowhere', 1);
  assert.equal(bus.getStats().unregisteredTopicPublishes, 1);
});

test('registry changes are recorded in the subscription audit log', () => {
  const bus = new EventBus();
  bus.registerTopic({ topic: 'audited.topic', owner: 'svc' }, 'deployer');
  bus.deregisterTopic('audited.topic', 'deployer');
  const events = bus.getSubscriptionAudit();
  assert.deepEqual(
    events.map((e) => e.action),
    ['topic_register', 'topic_deregister'],
  );
  assert.equal(events[0].actor, 'deployer');
  assert.equal(events[0].topic, 'audited.topic');
  assert.deepEqual(events[0].detail, {
    owner: 'svc',
    description: undefined,
    schemaVersion: undefined,
    tags: undefined,
    updated: false,
  });
  assert.equal(events[1].topic, 'audited.topic');
  // A no-op deregistration audits nothing.
  bus.deregisterTopic('audited.topic', 'deployer');
  assert.equal(bus.getSubscriptionAudit().length, 2);
});

test('the new counter renders in the Prometheus exposition', () => {
  const bus = new EventBus();
  bus.publish('unregistered', 1);
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_unregistered_topic_publishes_total 1'));
});

test('TopicRegistry is a standalone unit with defensive copies', () => {
  const registry = new TopicRegistry();
  const tags = ['a'];
  assert.equal(
    registry.register({ topic: 't', tags }, 10),
    true,
  );
  tags.push('mutated');
  assert.deepEqual(registry.get('t')?.tags, ['a']);
  const info = registry.get('t');
  assert.ok(info !== undefined);
  assert.equal(info.registeredAt, 10);
  assert.equal(registry.size(), 1);
  assert.equal(registry.has('t'), true);
  assert.equal(registry.deregister('t', 20), true);
  assert.equal(registry.has('t'), false);
  assert.equal(registry.size(), 0);
  assert.deepEqual(
    registry.catalog(true).map((t) => t.topic),
    ['t'],
  );
  assert.throws(() => assertValidTopicRegistration(null as never), RangeError);
});
