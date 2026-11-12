/**
 * Bus-level default subscriber payload cap live revision (EB-477).
 *
 * `setDefaultSubscriberMaxPayloadBytes` decides the per-subscriber
 * payload cap for future subscribers that omit their own
 * `subscriberMaxPayloadBytes` — `64` to bound every new subscriber's
 * single-message payload fleet-wide with one call, `null` to clear
 * the default back to topic-cap inheritance. The priority chain is
 * subscriber-explicit > bus default > topic cap > no cap; the stored
 * field is the derived value, so `subscriberPayloadCapFor` is
 * untouched and still reports source `'subscriber'` for a
 * default-derived cap. A subscriber keeps the cap it was baked with,
 * and `setSubscriberMaxPayloadBytes(subId, null)` now clears to the
 * bus default in effect at revision time (topic-cap inheritance
 * when unset). A revision identical to the current default is a
 * no-op with no audit. Audited as
 * `default_subscriber_max_payload_bytes_revised`
 * (`{ previous, subscriberMaxPayloadBytes }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_subscriber_max_payload_bytes_revised' });

const collect = (bus: EventBus, pattern: string, opts?: Parameters<EventBus['subscribe']>[2]) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

/** A payload whose JSON UTF-8 encoding is exactly `size` bytes. */
const sizedPayload = (size: number) => ({ data: 'x'.repeat(Math.max(0, size - 11)) });

test('EB-477: a later subscriber that omits subscriberMaxPayloadBytes takes the bus default and oversize payloads are skipped and counted', async () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), 64);
  const { received } = collect(bus, 't');
  bus.publish('t', sizedPayload(32));
  bus.publish('t', sizedPayload(128));
  bus.publish('t', sizedPayload(48));
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { data: string }).data.length),
    [sizedPayload(32).data.length, sizedPayload(48).data.length],
  );
  const stats = bus.getStats();
  assert.equal(stats.subscriberOversizeDropped, 1);
  assert.equal(stats.topics.find((t) => t.topic === 't')?.subscriberOversizeDropped, 1);
  // The skip is deliberate narrowing: never a sequence gap, never a drop.
  assert.equal(stats.sequenceGaps, 0);
  assert.equal(stats.droppedMessages, 0);
});

test('EB-477: an explicit subscriberMaxPayloadBytes beats the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  const { received } = collect(bus, 't', { subscriberMaxPayloadBytes: 256 });
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 1, 'the explicit 256 admits what the 64 default would skip');
  assert.equal(bus.getStats().subscriberOversizeDropped, 0);

  // An explicit 64 beats a 256 default.
  const loud = new EventBus();
  loud.setDefaultSubscriberMaxPayloadBytes(256);
  const loudCollected = collect(loud, 't', { subscriberMaxPayloadBytes: 64 });
  loud.publish('t', sizedPayload(128));
  await flush();
  assert.equal(loudCollected.received.length, 0);
  assert.equal(loud.getStats().subscriberOversizeDropped, 1);
});

test('EB-477: the bus default beats a wider topic cap', async () => {
  const bus = new EventBus();
  bus.setTopicMaxPayloadBytes('t', 4096);
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  const { received } = collect(bus, 't');
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 0, 'the 64 default governs even though the topic cap allows 4096');
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
});

test('EB-477: the factory default is unset and clearing with null returns new subscribers to topic-cap inheritance', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), undefined);
  bus.setTopicMaxPayloadBytes('t', 64);
  // No default: the topic cap governs — a 128-byte payload is refused
  // at admission, a 48-byte one delivers.
  const first = collect(bus, 't');
  bus.publish('t', sizedPayload(48));
  assert.equal(bus.publish('t', sizedPayload(128)), 0, 'over the topic cap: refused at admission');
  await flush();
  assert.equal(first.received.length, 1);
  assert.equal(
    bus.getStats().subscriberOversizeDropped,
    0,
    'the topic cap refuses at admission, before the subscriber gate',
  );
  first.sub.unsubscribe();

  // While a default is installed the topic cap stops governing: 48 is
  // admitted, then skipped for the subscriber under the 32 default.
  bus.setDefaultSubscriberMaxPayloadBytes(32);
  const during = collect(bus, 't');
  bus.publish('t', sizedPayload(48));
  await flush();
  assert.equal(during.received.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  during.sub.unsubscribe();

  bus.setDefaultSubscriberMaxPayloadBytes(null);
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), undefined);
  // After the clear the topic cap governs again.
  const cleared = collect(bus, 't');
  bus.publish('t', sizedPayload(48));
  assert.equal(bus.publish('t', sizedPayload(128)), 0);
  await flush();
  assert.equal(cleared.received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1, 'no new skip after the clear');
  cleared.sub.unsubscribe();
});

test('EB-477: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(null);
  assert.equal(
    defaultAudits(bus).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), 64);
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), 64);
  const audit = defaultAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, subscriberMaxPayloadBytes: 64 });
  bus.setDefaultSubscriberMaxPayloadBytes(null);
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), undefined);
  bus.setDefaultSubscriberMaxPayloadBytes(null);
  assert.equal(defaultAudits(bus).length, 2, 're-clearing audits nothing');
});

test('EB-477: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  for (const bad of [
    0,
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    'x',
    {},
    [],
    undefined,
  ] as unknown[]) {
    assert.throws(
      () => bus.setDefaultSubscriberMaxPayloadBytes(bad as never),
      RangeError,
      `bytes=${String(bad)}`,
    );
    assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), 64);
  }
  assert.throws(() => bus.setDefaultSubscriberMaxPayloadBytes(128, ''), RangeError);
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), 64);
  assert.equal(
    defaultAudits(bus).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-477: revising the bus default never moves an already-subscribed cap', async () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  const { received, sub } = collect(bus, 't');
  bus.publish('t', sizedPayload(128)); // skipped under the baked 64
  bus.setDefaultSubscriberMaxPayloadBytes(256);
  bus.publish('t', sizedPayload(128)); // still skipped — the subscriber keeps 64
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 2);
  // A subscriber joining after the revision takes the new default.
  const late = collect(bus, 'u');
  bus.publish('u', sizedPayload(128));
  await flush();
  assert.equal(late.received.length, 1, 'the late subscriber admits 128 under the 256 default');
  late.sub.unsubscribe();
  sub.unsubscribe();
});

test('EB-477: setSubscriberMaxPayloadBytes(null) clears to the bus default in effect at revision time, and to topic-cap inheritance when unset', async () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(128);
  const { received, sub } = collect(bus, 't', { subscriberMaxPayloadBytes: 32 });
  bus.publish('t', sizedPayload(64)); // skipped under the explicit 32
  bus.setSubscriberMaxPayloadBytes(sub.id, null); // clears to the 128 default
  bus.publish('t', sizedPayload(64)); // delivered under 128
  bus.publish('t', sizedPayload(256)); // skipped under 128
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 2);
  // The audit names the resolved cap, not null.
  const revisions = bus.getSubscriptionAudit({ action: 'subscriber_max_payload_bytes_revised' });
  assert.equal(revisions.length, 1);
  assert.deepEqual(revisions[0].detail, {
    subscriberMaxPayloadBytes: 128,
    oldSubscriberMaxPayloadBytes: 32,
  });
  sub.unsubscribe();

  // Without a default the clear is the old one: back to topic-cap inheritance.
  const plain = new EventBus();
  plain.setTopicMaxPayloadBytes('t', 1024);
  const p = collect(plain, 't', { subscriberMaxPayloadBytes: 32 });
  plain.publish('t', sizedPayload(64)); // skipped under the explicit 32
  plain.setSubscriberMaxPayloadBytes(p.sub.id, null); // clears to the 1024 topic cap
  plain.publish('t', sizedPayload(64)); // delivered under the topic cap
  await flush();
  assert.equal(p.received.length, 1);
  assert.equal(plain.getStats().subscriberOversizeDropped, 1);
  // A payload over the topic cap is refused at admission — the topic cap governs.
  assert.equal(plain.publish('t', sizedPayload(2048)), 0);
  await flush();
  assert.equal(p.received.length, 1);
  assert.equal(plain.getStats().subscriberOversizeDropped, 1);
  const plainRevisions = plain.getSubscriptionAudit({
    action: 'subscriber_max_payload_bytes_revised',
  });
  assert.equal(plainRevisions.length, 1);
  assert.deepEqual(plainRevisions[0].detail, {
    subscriberMaxPayloadBytes: null,
    oldSubscriberMaxPayloadBytes: 32,
  });
  p.sub.unsubscribe();
});

test('EB-477: a subscriber with neither an explicit cap nor a bus default still refuses revision', () => {
  const bus = new EventBus();
  const { sub } = collect(bus, 't');
  assert.throws(
    () => bus.setSubscriberMaxPayloadBytes(sub.id, 128),
    RangeError,
    'no explicit cap and no default: fail-closed',
  );
  assert.equal(bus.getStats().subscriberOversizeDropped, 0);
  sub.unsubscribe();
});

test('EB-477: a default-derived subscriber can be revised one at a time and the bus default stays in place', async () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  const { received, sub } = collect(bus, 't'); // derived 64
  bus.setSubscriberMaxPayloadBytes(sub.id, 256);
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), 64);
  bus.publish('t', sizedPayload(128)); // delivered under the revised 256
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 0);
  // A subscriber joining after the per-subscriber revision still takes the bus default.
  const late = collect(bus, 'u');
  bus.publish('u', sizedPayload(128)); // skipped under 64
  await flush();
  assert.equal(late.received.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  late.sub.unsubscribe();
  sub.unsubscribe();
});

test('EB-477: a counted skip is not recounted after the cap is revised', async () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(64);
  const { received, sub } = collect(bus, 't');
  bus.publish('t', sizedPayload(128)); // skipped, counted once
  bus.setSubscriberMaxPayloadBytes(sub.id, 256);
  assert.equal(
    bus.getStats().subscriberOversizeDropped,
    1,
    'the counted skip stays counted',
  );
  bus.publish('t', sizedPayload(128)); // admitted under the wider cap
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  sub.unsubscribe();
});

test('EB-477: setDefaultSubscriberMaxPayloadBytes audits each revision with its old and new cap and actor', () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberMaxPayloadBytes(64, 'ops');
  bus.setDefaultSubscriberMaxPayloadBytes(128, 'ops');
  bus.setDefaultSubscriberMaxPayloadBytes(null, 'ops');
  const events = defaultAudits(bus);
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, subscriberMaxPayloadBytes: 64 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 64, subscriberMaxPayloadBytes: 128 });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 128, subscriberMaxPayloadBytes: null });
  assert.equal(bus.getDefaultSubscriberMaxPayloadBytes(), undefined);
});
