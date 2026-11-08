/**
 * Unregistered-topic publish hook live redirection (EB-426).
 *
 * `setUnregisteredTopicCallback` swaps the `onUnregisteredTopic` observer
 * every publish to an unregistered topic is handed to without restarting
 * the bus. There is no enable gate — any open bus can install one — and
 * `null` clears back to no observer. The new hook governs from the next
 * unregistered publish onward, the counter
 * (`getStats().unregisteredTopicPublishes`) moves regardless of whether a
 * hook is installed, and a throwing hook is swallowed by
 * `checkTopicRegistered`. The callback function itself is never written to
 * the audit log: only whether one is installed is recorded
 * (`{ hasCallback, oldHasCallback }`). Re-installing the live callback is
 * a no-op with no audit. The topic registry itself is untouched.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type UnregisteredTopicEvent } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('installing a hook routes later unregistered publishes to it', async () => {
  const seen: UnregisteredTopicEvent[] = [];
  const bus = new EventBus();
  bus.registerTopic({ topic: 'known.topic' });
  // No hook at construction: the publish is counted, not observed.
  assert.equal(bus.publish('mystery.topic', 1), 0);
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().unregisteredTopicPublishes, 1);
  bus.setUnregisteredTopicCallback((e) => seen.push(e));
  assert.equal(bus.publish('mystery.topic', 2), 0);
  // A registered topic never fires the hook.
  assert.equal(bus.publish('known.topic', 3), 0);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 'mystery.topic');
  assert.equal(seen[0].unregisteredPublishes, 2);
  assert.ok(typeof seen[0].at === 'number');
  assert.equal(bus.getStats().unregisteredTopicPublishes, 2);
  bus.destroy?.();
});

test('clearing with null stops delivery and the counter keeps moving', async () => {
  const seen: UnregisteredTopicEvent[] = [];
  const bus = new EventBus({ onUnregisteredTopic: (e: UnregisteredTopicEvent) => seen.push(e) });
  assert.equal(bus.publish('ghost.topic', 1), 0);
  assert.equal(seen.length, 1);
  bus.setUnregisteredTopicCallback(null);
  assert.equal(bus.publish('ghost.topic', 2), 0);
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().unregisteredTopicPublishes, 2);
  // Clearing a bus with no callback is a no-op with no audit.
  const before = bus.getSubscriptionAudit({ action: 'unregistered_topic_callback_revised' }).length;
  bus.setUnregisteredTopicCallback(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'unregistered_topic_callback_revised' }).length,
    before,
  );
  bus.destroy?.();
});

test('non-functions are rejected and the live hook stays put', async () => {
  const seen: UnregisteredTopicEvent[] = [];
  const bus = new EventBus({ onUnregisteredTopic: (e: UnregisteredTopicEvent) => seen.push(e) });
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const handler of bad) {
    assert.throws(
      () => bus.setUnregisteredTopicCallback(handler as never),
      RangeError,
      `expected RangeError for ${String(handler)}`,
    );
  }
  // The failed calls changed nothing: the live hook still observes.
  assert.equal(bus.publish('ghost.topic', 1), 0);
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'unregistered_topic_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('re-installing the live callback is a no-op with no audit', () => {
  const handler = (_e: UnregisteredTopicEvent): void => {};
  const bus = new EventBus({ onUnregisteredTopic: handler });
  bus.setUnregisteredTopicCallback(handler);
  assert.equal(bus.getSubscriptionAudit({ action: 'unregistered_topic_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('a closed bus refuses the swap', async () => {
  const bus = new EventBus();
  await bus.close();
  const handler = (_e: UnregisteredTopicEvent): void => {};
  assert.throws(() => bus.setUnregisteredTopicCallback(handler), RangeError);
  assert.throws(() => bus.setUnregisteredTopicCallback(null), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'unregistered_topic_callback_revised' }).length, 0);
});

test('a throwing hook is swallowed and the publish path is unaffected', async () => {
  const bus = new EventBus();
  bus.setUnregisteredTopicCallback(() => {
    throw new Error('broken observer');
  });
  // The publish still returns 0 (not a throw) and the counter moves.
  assert.equal(bus.publish('ghost.topic', 1), 0);
  assert.equal(bus.getStats().unregisteredTopicPublishes, 1);
  // An admitted publish still flows to subscribers.
  const received: unknown[] = [];
  bus.subscribe('known.topic', (msg) => received.push(msg.payload));
  bus.registerTopic({ topic: 'known.topic' });
  assert.equal(bus.publish('known.topic', 2), 1);
  await flush();
  assert.deepEqual(received, [2]);
  bus.destroy?.();
});

test('the swap audits exact presence flags with the actor', () => {
  const first = (_e: UnregisteredTopicEvent): void => {};
  const second = (_e: UnregisteredTopicEvent): void => {};
  const bus = new EventBus();
  bus.setUnregisteredTopicCallback(first, 'ops');
  bus.setUnregisteredTopicCallback(second, 'ops');
  bus.setUnregisteredTopicCallback(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'unregistered_topic_callback_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(audit[2].detail, { hasCallback: false, oldHasCallback: true });
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
  bus.destroy?.();
});

test('setUnregisteredTopicCallback rejects an illegal actor', () => {
  const bus = new EventBus();
  const handler = (_e: UnregisteredTopicEvent): void => {};
  assert.throws(() => bus.setUnregisteredTopicCallback(handler, ''), RangeError);
  assert.throws(() => bus.setUnregisteredTopicCallback(handler, 42 as never), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'unregistered_topic_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('the topic registry is untouched by a hook swap', async () => {
  const seen: UnregisteredTopicEvent[] = [];
  const bus = new EventBus();
  bus.setUnregisteredTopicCallback((e) => seen.push(e));
  bus.registerTopic({ topic: 'known.topic', owner: 'svc' });
  assert.equal(bus.getTopicInfo('known.topic')?.registered, true);
  assert.equal(bus.publish('known.topic', 1), 0);
  assert.equal(seen.length, 0);
  // Deregistering makes the topic unregistered again — the live hook sees it.
  bus.deregisterTopic('known.topic');
  assert.equal(bus.publish('known.topic', 2), 0);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 'known.topic');
  bus.setUnregisteredTopicCallback(null);
  bus.destroy?.();
});

test('two consecutive redirects both take effect', () => {
  const firstSeen: UnregisteredTopicEvent[] = [];
  const secondSeen: UnregisteredTopicEvent[] = [];
  const bus = new EventBus();
  bus.setUnregisteredTopicCallback((e) => firstSeen.push(e));
  assert.equal(bus.publish('ghost.topic', 1), 0);
  assert.equal(firstSeen.length, 1);
  bus.setUnregisteredTopicCallback((e) => secondSeen.push(e));
  assert.equal(bus.publish('ghost.topic', 2), 0);
  assert.equal(firstSeen.length, 1);
  assert.equal(secondSeen.length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'unregistered_topic_callback_revised' }).length, 2);
  bus.destroy?.();
});
