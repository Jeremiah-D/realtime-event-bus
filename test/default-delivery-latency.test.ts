import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default delivery-latency sampling window (EB-491):
 * `setDefaultDeliveryLatency` installs the `windowSize` new opt-in
 * delivery-latency subscribers fall back to when they omit the field
 * — the priority chain is subscriber-explicit > bus default > 1024 —
 * and the default never enables sampling: a subscriber that does not
 * opt into `deliveryLatency` is untouched by it.
 *
 * Window assertions follow `latency.test.ts`: an injectable clock
 * makes the enqueue→delivery dwell deterministic, and the `samples`
 * count in `getStats().deliveryLatency` exposes the window bound —
 * publish more messages than the window and the count caps at it.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 1_000) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

function samplesFor(bus: EventBus, pattern: string): number {
  const entry = bus.getStats().deliveryLatency.find((e) => e.pattern === pattern);
  assert.ok(entry !== undefined, `expected a deliveryLatency entry for ${pattern}`);
  return entry.samples;
}

/** Publishes `count` messages on `topic`, then delivers them all with a 15ms dwell. */
async function publishAndDeliver(bus: EventBus, clock: { advance: (ms: number) => void }, topic: string, count: number) {
  for (let i = 0; i < count; i++) bus.publish(topic, i);
  clock.advance(15);
  await flush();
}

test('EB-491: the factory bus default is unset (getter undefined, clearing audits nothing)', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  assert.equal(bus.getDefaultDeliveryLatency(), undefined);
  bus.setDefaultDeliveryLatency(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_latency_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  // No default installed: a bare opt-in still takes the documented 1024.
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: {} });
  await publishAndDeliver(bus, clock, 't', 5);
  assert.equal(samplesFor(bus, 't'), 5);
  sub.unsubscribe();
});

test('EB-491: true takes the whole bus default window', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryLatency({ windowSize: 3 });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: true });
  await publishAndDeliver(bus, clock, 't', 5);
  assert.equal(samplesFor(bus, 't'), 3, 'the window caps at the default 3');
  sub.unsubscribe();
});

test('EB-491: a partial object falls back to the default and an explicit windowSize wins', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryLatency({ windowSize: 3 });
  const bare = bus.subscribe('a', () => {}, { deliveryLatency: {} });
  const explicit = bus.subscribe('b', () => {}, { deliveryLatency: { windowSize: 2 } });
  await publishAndDeliver(bus, clock, 'a', 5);
  await publishAndDeliver(bus, clock, 'b', 5);
  assert.equal(samplesFor(bus, 'a'), 3, '{} takes the default window');
  assert.equal(samplesFor(bus, 'b'), 2, 'the explicit windowSize beats the default');
  bare.unsubscribe();
  explicit.unsubscribe();
});

test('EB-491: true with no default means the documented 1024', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: true });
  // The default per-subscriber queue holds 100, so feed the window in
  // waves — each wave delivers fully and samples, until the 1024
  // window caps the count.
  for (let wave = 0; wave < 12; wave++) {
    await publishAndDeliver(bus, clock, 't', 90);
    assert.equal(bus.droppedCount(sub.id), 0);
  }
  assert.equal(samplesFor(bus, 't'), 1024);
  sub.unsubscribe();
});

test('EB-491: clearing with null returns new opt-in subscribers to 1024', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryLatency({ windowSize: 3 });
  bus.setDefaultDeliveryLatency(null);
  assert.equal(bus.getDefaultDeliveryLatency(), undefined);
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: true });
  await publishAndDeliver(bus, clock, 't', 5);
  assert.equal(samplesFor(bus, 't'), 5);
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_latency_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { windowSize: 3 },
    deliveryLatency: null,
  });
  sub.unsubscribe();
});

test('EB-491: revising the bus default never moves an already-subscribed window or its samples', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryLatency({ windowSize: 3 });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: true });
  await publishAndDeliver(bus, clock, 't', 5);
  assert.equal(samplesFor(bus, 't'), 3);
  bus.setDefaultDeliveryLatency({ windowSize: 2 });
  await publishAndDeliver(bus, clock, 't', 5);
  assert.equal(
    samplesFor(bus, 't'),
    3,
    'the early subscriber keeps the window it was baked with',
  );
  // A subscriber joining after the revision takes the new default.
  const late = bus.subscribe('u', () => {}, { deliveryLatency: true });
  await publishAndDeliver(bus, clock, 'u', 5);
  assert.equal(samplesFor(bus, 'u'), 2, 'the new default window is 2');
  sub.unsubscribe();
  late.unsubscribe();
});

test('EB-491: the default never enables sampling', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryLatency({ windowSize: 3 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  await publishAndDeliver(bus, clock, 't', 5);
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.deepEqual(bus.getStats().deliveryLatency, []);
  sub.unsubscribe();
});

test('EB-491: setDeliveryLatency still revises one sampler at a time without touching the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryLatency({ windowSize: 3 });
  const sub = bus.subscribe('t', () => {}, { deliveryLatency: true });
  await publishAndDeliver(bus, clock, 't', 5);
  assert.equal(samplesFor(bus, 't'), 3);
  bus.setDeliveryLatency(sub.id, { windowSize: 5 });
  await publishAndDeliver(bus, clock, 't', 5);
  assert.equal(samplesFor(bus, 't'), 5, 'the widened window keeps the 3 old samples');
  assert.deepEqual(bus.getDefaultDeliveryLatency(), { windowSize: 3 });
  bus.setDeliveryLatency(sub.id, null);
  assert.deepEqual(bus.getStats().deliveryLatency, []);
  // A new subscriber still takes the untouched bus default.
  const fresh = bus.subscribe('u', () => {}, { deliveryLatency: true });
  await publishAndDeliver(bus, clock, 'u', 5);
  assert.equal(samplesFor(bus, 'u'), 3);
  sub.unsubscribe();
  fresh.unsubscribe();
});

test('EB-491: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and illegal values throw RangeError with the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryLatency(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_latency_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultDeliveryLatency({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_latency_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultDeliveryLatency(), undefined);
  const opts = { windowSize: 3 };
  bus.setDefaultDeliveryLatency(opts);
  opts.windowSize = 999;
  assert.deepEqual(
    bus.getDefaultDeliveryLatency(),
    { windowSize: 3 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultDeliveryLatency({ windowSize: 3 });
  bus.setDefaultDeliveryLatency({ windowSize: 3 });
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_latency_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    deliveryLatency: { windowSize: 3 },
  });
  const held = bus.getDefaultDeliveryLatency();
  assert.ok(held !== undefined);
  held.windowSize = 999;
  assert.deepEqual(
    bus.getDefaultDeliveryLatency(),
    { windowSize: 3 },
    'the getter returns a copy',
  );
  bus.setDefaultDeliveryLatency({ windowSize: 5 }, 'ops');
  assert.deepEqual(bus.getDefaultDeliveryLatency(), { windowSize: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'default_delivery_latency_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { windowSize: 3 },
    deliveryLatency: { windowSize: 5 },
  });
  assert.equal(revised[1].actor, 'ops');
  const bad = [
    0,
    -1,
    '3',
    [],
    true,
    { windowSize: 0 },
    { windowSize: -1 },
    { windowSize: 1.5 },
    { windowSize: Number.NaN },
    { windowSize: '3' },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultDeliveryLatency(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultDeliveryLatency(), { windowSize: 5 });
  }
  assert.throws(() => bus.setDefaultDeliveryLatency({ windowSize: 7 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultDeliveryLatency(), { windowSize: 5 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_latency_revised' }).length,
    2,
    'failed revisions must not audit',
  );
});
