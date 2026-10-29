import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A payload whose JSON UTF-8 encoding is exactly `size` bytes. */
const sizedPayload = (size: number) => ({ data: 'x'.repeat(Math.max(0, size - 11)) });

const collect = (bus: EventBus, pattern: string, opts?: Parameters<EventBus['subscribe']>[2]) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

test('setSubscriberMaxPayloadBytes rejects unknown, empty, unset-cap, and invalid revisions fail-closed', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { subscriberMaxPayloadBytes: 64 });
  const plain = bus.subscribe('t2', () => {});
  bus.publish('t', sizedPayload(32));
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  for (const bad of [
    () => bus.setSubscriberMaxPayloadBytes('no-such-sub', 32),
    () => bus.setSubscriberMaxPayloadBytes('', 32),
    () => bus.setSubscriberMaxPayloadBytes(plain.id, 32),
    () => bus.setSubscriberMaxPayloadBytes(sub.id, 0),
    () => bus.setSubscriberMaxPayloadBytes(sub.id, -1),
    () => bus.setSubscriberMaxPayloadBytes(sub.id, 1.5),
    () => bus.setSubscriberMaxPayloadBytes(sub.id, Number.NaN),
    () => bus.setSubscriberMaxPayloadBytes(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setSubscriberMaxPayloadBytes(sub.id, undefined as never),
    () => bus.setSubscriberMaxPayloadBytes(sub.id, '32' as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_max_payload_bytes_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live cap is untouched: a 128-byte payload is still skipped.
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 2);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('setSubscriberMaxPayloadBytes shrink governs the next delivery, already-delivered messages untouched', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { subscriberMaxPayloadBytes: 128 });
  bus.publish('t', sizedPayload(64));
  await flush();
  assert.equal(received.length, 1);
  bus.setSubscriberMaxPayloadBytes(sub.id, 48);
  const events = bus.getSubscriptionAudit({ action: 'subscriber_max_payload_bytes_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.deepEqual(events[0].detail, { subscriberMaxPayloadBytes: 48, oldSubscriberMaxPayloadBytes: 128 });
  // The already-delivered 64-byte message stays delivered; the next
  // 64-byte message is skipped under the new cap, the 32-byte one passes.
  bus.publish('t', sizedPayload(64));
  bus.publish('t', sizedPayload(32));
  await flush();
  assert.equal(received.length, 2);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('setSubscriberMaxPayloadBytes grow admits immediately and leaves the skip history counted', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { subscriberMaxPayloadBytes: 48 });
  bus.publish('t', sizedPayload(64));
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  bus.setSubscriberMaxPayloadBytes(sub.id, 128);
  const events = bus.getSubscriptionAudit({ action: 'subscriber_max_payload_bytes_revised' });
  assert.deepEqual(events[0].detail, { subscriberMaxPayloadBytes: 128, oldSubscriberMaxPayloadBytes: 48 });
  // The previously skipped message is not resurrected, but the next
  // 64-byte message is delivered at once; the old skip stays counted.
  bus.publish('t', sizedPayload(64));
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('setSubscriberMaxPayloadBytes null clears to no cap, and a revision after the clear throws', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { subscriberMaxPayloadBytes: 64 });
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 0);
  bus.setSubscriberMaxPayloadBytes(sub.id, null);
  const events = bus.getSubscriptionAudit({ action: 'subscriber_max_payload_bytes_revised' });
  assert.deepEqual(events[0].detail, { subscriberMaxPayloadBytes: null, oldSubscriberMaxPayloadBytes: 64 });
  // No cap left (and no topic cap set): large payloads deliver.
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 1);
  // The cleared cap cannot be revised back without re-subscribing.
  assert.throws(() => bus.setSubscriberMaxPayloadBytes(sub.id, 64), RangeError);
  assert.throws(() => bus.setSubscriberMaxPayloadBytes(sub.id, null), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_max_payload_bytes_revised' }).length,
    1,
    'refused post-clear revisions must not audit',
  );
  sub.unsubscribe();
});

test('setSubscriberMaxPayloadBytes same-value is a no-op with no audit', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', { subscriberMaxPayloadBytes: 64 });
  bus.setSubscriberMaxPayloadBytes(sub.id, 64);
  bus.setSubscriberMaxPayloadBytes(sub.id, 64);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_max_payload_bytes_revised' }).length,
    0,
    'restating the current cap must not audit',
  );
  // The live cap still enforces: a 128-byte payload is skipped.
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  sub.unsubscribe();
});

test('setSubscriberMaxPayloadBytes clear falls back to the topic cap on the next delivery', async () => {
  const bus = new EventBus();
  // Topic cap 1024 admits everything in this test; the subscriber's
  // explicit 64-byte cap is what skips the 128-byte payload.
  bus.setTopicMaxPayloadBytes('t', 1024);
  const { received, sub } = collect(bus, 't', { subscriberMaxPayloadBytes: 64 });
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 0);
  // Clearing removes the explicit cap: the topic cap (1024) now
  // governs, so the 128-byte payload delivers.
  bus.setSubscriberMaxPayloadBytes(sub.id, null);
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 1);
  sub.unsubscribe();
});
