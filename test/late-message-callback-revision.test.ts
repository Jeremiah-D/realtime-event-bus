/**
 * Late-message hook live redirection (EB-427).
 *
 * `setLateMessageCallback` swaps the `onLate` observer every late
 * event-time publish is handed to without restarting the bus. There is no
 * enable gate — any open bus can install one — and `null` clears back to
 * no observer. The new hook governs from the next lateness judgment
 * onward, the counters (`TopicStats.lateMessages`,
 * `BusStats.lateMessages`) move regardless of whether a hook is
 * installed, and a throwing hook is swallowed by `observeEventTime`. The
 * callback function itself is never written to the audit log: only
 * whether one is installed is recorded (`{ hasCallback, oldHasCallback }`).
 * Re-installing the live callback is a no-op with no audit. The
 * watermark mechanism, the allowed lateness, and the late judgment itself
 * are untouched.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type LateMessageEvent } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Advances the topic watermark to 5000 with an on-time publish. */
function prime(bus: EventBus, topic = 'orders'): void {
  bus.publish(topic, 'first', { eventTime: 5000 });
}

test('installing a hook routes later late messages to it with exact fields', async () => {
  const seen: LateMessageEvent[] = [];
  const bus = new EventBus();
  prime(bus);
  // No hook at construction: the late message is counted, not observed.
  bus.publish('orders', 'late', { eventTime: 1000 });
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().lateMessages, 1);
  bus.setLateMessageCallback((e) => seen.push(e));
  bus.publish('orders', 'late2', { eventTime: 2000 });
  // An on-time publish never fires the hook.
  bus.publish('orders', 'on-time', { eventTime: 6000 });
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0], {
    topic: 'orders',
    seq: 3,
    eventTime: 2000,
    watermark: 5000,
    allowedLatenessMs: 0,
  });
  assert.equal(bus.getStats().lateMessages, 2);
  bus.destroy?.();
});

test('clearing with null stops delivery and the counters keep moving', async () => {
  const seen: LateMessageEvent[] = [];
  const bus = new EventBus({ onLate: (e: LateMessageEvent) => seen.push(e) });
  prime(bus);
  bus.publish('orders', 'late', { eventTime: 1000 });
  assert.equal(seen.length, 1);
  bus.setLateMessageCallback(null);
  bus.publish('orders', 'late2', { eventTime: 2000 });
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().lateMessages, 2);
  // Clearing a bus with no callback is a no-op with no audit.
  const before = bus.getSubscriptionAudit({ action: 'late_message_callback_revised' }).length;
  bus.setLateMessageCallback(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'late_message_callback_revised' }).length, before);
  bus.destroy?.();
});

test('non-functions are rejected and the live hook stays put', async () => {
  const seen: LateMessageEvent[] = [];
  const bus = new EventBus({ onLate: (e: LateMessageEvent) => seen.push(e) });
  prime(bus);
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const handler of bad) {
    assert.throws(
      () => bus.setLateMessageCallback(handler as never),
      RangeError,
      `expected RangeError for ${String(handler)}`,
    );
  }
  // The failed calls changed nothing: the live hook still observes.
  bus.publish('orders', 'late', { eventTime: 1000 });
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'late_message_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('re-installing the live callback is a no-op with no audit', () => {
  const handler = (_e: LateMessageEvent): void => {};
  const bus = new EventBus({ onLate: handler });
  bus.setLateMessageCallback(handler);
  assert.equal(bus.getSubscriptionAudit({ action: 'late_message_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('a closed bus refuses the swap', async () => {
  const bus = new EventBus();
  await bus.close();
  const handler = (_e: LateMessageEvent): void => {};
  assert.throws(() => bus.setLateMessageCallback(handler), RangeError);
  assert.throws(() => bus.setLateMessageCallback(null), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'late_message_callback_revised' }).length, 0);
});

test('a throwing hook is swallowed and the publish path is unaffected', async () => {
  const bus = new EventBus();
  bus.setLateMessageCallback(() => {
    throw new Error('broken observer');
  });
  const received: unknown[] = [];
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  prime(bus);
  // The late message is still delivered and counted.
  bus.publish('orders', 'late', { eventTime: 1000 });
  await flush();
  assert.deepEqual(received, ['first', 'late']);
  assert.equal(bus.getStats().lateMessages, 1);
  bus.destroy?.();
});

test('the swap audits exact presence flags with the actor', () => {
  const first = (_e: LateMessageEvent): void => {};
  const second = (_e: LateMessageEvent): void => {};
  const bus = new EventBus();
  bus.setLateMessageCallback(first, 'ops');
  bus.setLateMessageCallback(second, 'ops');
  bus.setLateMessageCallback(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'late_message_callback_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(audit[2].detail, { hasCallback: false, oldHasCallback: true });
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
  bus.destroy?.();
});

test('setLateMessageCallback rejects an illegal actor', () => {
  const bus = new EventBus();
  const handler = (_e: LateMessageEvent): void => {};
  assert.throws(() => bus.setLateMessageCallback(handler, ''), RangeError);
  assert.throws(() => bus.setLateMessageCallback(handler, 42 as never), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'late_message_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('a message already judged late is never re-delivered to the new hook', async () => {
  const seen: LateMessageEvent[] = [];
  const bus = new EventBus();
  prime(bus);
  bus.publish('orders', 'late', { eventTime: 1000 });
  assert.equal(bus.getStats().lateMessages, 1);
  // The new hook only sees judgments made after it was installed.
  bus.setLateMessageCallback((e) => seen.push(e));
  await flush();
  assert.equal(seen.length, 0);
  bus.publish('orders', 'late2', { eventTime: 2000 });
  assert.equal(seen.length, 1);
  assert.equal(seen[0].eventTime, 2000);
  bus.destroy?.();
});

test('two consecutive redirects both take effect', () => {
  const firstSeen: LateMessageEvent[] = [];
  const secondSeen: LateMessageEvent[] = [];
  const bus = new EventBus();
  prime(bus);
  bus.setLateMessageCallback((e) => firstSeen.push(e));
  bus.publish('orders', 'late', { eventTime: 1000 });
  assert.equal(firstSeen.length, 1);
  bus.setLateMessageCallback((e) => secondSeen.push(e));
  bus.publish('orders', 'late2', { eventTime: 2000 });
  assert.equal(firstSeen.length, 1);
  assert.equal(secondSeen.length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'late_message_callback_revised' }).length, 2);
  bus.destroy?.();
});
