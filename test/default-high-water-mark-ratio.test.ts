/**
 * Bus-level default high-water-mark ratio live revision (EB-476).
 *
 * `setDefaultHighWaterMarkRatio` decides the fraction of capacity at
 * which future subscribers that omit their own `highWaterMarkRatio`
 * trip their slow-consumer throttle and fire `onBackpressure` /
 * `onDrained` — `0.5` to make a fleet's backpressure line twice as
 * sensitive with one call, `null` to clear the default back to the
 * documented 0.8. The priority chain is subscriber-explicit > bus
 * default > 0.8, and the merged ratio feeds both the throttle
 * threshold and the LaneQueue mark at subscribe time, so the two
 * never fork. A subscriber keeps the ratio it was baked with, and
 * `setHighWaterMarkRatio` still revises such a subscriber one at a
 * time without touching the bus default. A revision identical to the
 * current default is a no-op with no audit. Audited as
 * `default_high_water_mark_ratio_revised`
 * (`{ previous, highWaterMarkRatio }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BackpressureEvent,
  type DrainedEvent,
  type ThrottleEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_high_water_mark_ratio_revised' });

/** Reads a subscriber's live queue ratio and absolute mark (test-only introspection). */
const queueMark = (bus: EventBus, subId: string) => {
  const sub = (
    bus as unknown as {
      subscribers: Map<string, { queue: { highWaterMarkRatio: number; highWaterMark: number } }>;
    }
  ).subscribers.get(subId);
  assert.ok(sub, 'subscriber exists');
  return { ratio: sub.queue.highWaterMarkRatio, mark: sub.queue.highWaterMark };
};

test('EB-476: a later subscriber that omits highWaterMarkRatio takes the bus default, queue mark follows capacity x default', () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(0.5);
  assert.equal(bus.getDefaultHighWaterMarkRatio(), 0.5);
  const pressured: BackpressureEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onBackpressure: (e) => pressured.push(e),
  });
  assert.deepEqual(queueMark(bus, sub.id), { ratio: 0.5, mark: 5 });
  // The mark scales with capacity, not just the ratio.
  const wide = bus.subscribe('w', () => {}, { queueSize: 20 });
  assert.deepEqual(queueMark(bus, wide.id), { ratio: 0.5, mark: 10 });
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  assert.equal(pressured.length, 0, 'below capacity x default, no excursion');
  bus.publish('t', 4);
  assert.equal(pressured.length, 1);
  assert.equal(pressured[0].queueSize, 5);
  wide.unsubscribe();
  sub.unsubscribe();
});

test('EB-476: an explicit highWaterMarkRatio beats the bus default in both directions', () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(0.5);
  const pressured: BackpressureEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    highWaterMarkRatio: 0.9,
    onBackpressure: (e) => pressured.push(e),
  });
  assert.deepEqual(queueMark(bus, sub.id), { ratio: 0.9, mark: 9 });
  for (let i = 0; i < 8; i++) bus.publish('t', i);
  assert.equal(pressured.length, 0, 'the explicit 0.9 holds past the 0.5 default mark');
  bus.publish('t', 8);
  assert.equal(pressured.length, 1);
  assert.equal(pressured[0].queueSize, 9);
  sub.unsubscribe();

  // An explicit 0.5 beats a 0.9 default.
  const loud = new EventBus();
  loud.setDefaultHighWaterMarkRatio(0.9);
  const loudPressured: BackpressureEvent[] = [];
  const loudSub = loud.subscribe('t', () => {}, {
    queueSize: 10,
    highWaterMarkRatio: 0.5,
    onBackpressure: (e) => loudPressured.push(e),
  });
  assert.deepEqual(queueMark(loud, loudSub.id), { ratio: 0.5, mark: 5 });
  for (let i = 0; i < 5; i++) loud.publish('t', i);
  assert.equal(loudPressured.length, 1);
  assert.equal(loudPressured[0].queueSize, 5);
  loudSub.unsubscribe();
});

test('EB-476: the factory default is unset and clearing with null returns new subscribers to 0.8', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultHighWaterMarkRatio(), undefined);
  const sub = bus.subscribe('t', () => {}, { queueSize: 10 });
  assert.deepEqual(queueMark(bus, sub.id), { ratio: 0.8, mark: 8 });
  sub.unsubscribe();

  bus.setDefaultHighWaterMarkRatio(0.5);
  bus.setDefaultHighWaterMarkRatio(null);
  assert.equal(bus.getDefaultHighWaterMarkRatio(), undefined);
  const cleared = bus.subscribe('u', () => {}, { queueSize: 10 });
  assert.deepEqual(queueMark(bus, cleared.id), { ratio: 0.8, mark: 8 });
  cleared.unsubscribe();
});

test('EB-476: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(null);
  assert.equal(
    defaultAudits(bus).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultHighWaterMarkRatio(0.5);
  assert.equal(bus.getDefaultHighWaterMarkRatio(), 0.5);
  bus.setDefaultHighWaterMarkRatio(0.5);
  assert.equal(bus.getDefaultHighWaterMarkRatio(), 0.5);
  const audit = defaultAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, highWaterMarkRatio: 0.5 });
  bus.setDefaultHighWaterMarkRatio(null);
  assert.equal(bus.getDefaultHighWaterMarkRatio(), undefined);
  bus.setDefaultHighWaterMarkRatio(null);
  assert.equal(defaultAudits(bus).length, 2, 're-clearing audits nothing');
});

test('EB-476: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(0.5);
  for (const bad of [
    0,
    -0.5,
    1.1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    'x',
    {},
    [],
    undefined,
  ] as unknown[]) {
    assert.throws(
      () => bus.setDefaultHighWaterMarkRatio(bad as never),
      RangeError,
      `ratio=${String(bad)}`,
    );
    assert.equal(bus.getDefaultHighWaterMarkRatio(), 0.5);
  }
  assert.throws(() => bus.setDefaultHighWaterMarkRatio(0.9, ''), RangeError);
  assert.equal(bus.getDefaultHighWaterMarkRatio(), 0.5);
  assert.equal(
    defaultAudits(bus).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-476: revising the bus default never moves an already-subscribed ratio or its in-flight excursion', async () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(0.5);
  const pressured: BackpressureEvent[] = [];
  const drained: DrainedEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    onBackpressure: (e) => pressured.push(e),
    onDrained: (e) => drained.push(e),
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i); // excursion at mark 5
  assert.equal(pressured.length, 1);
  bus.setDefaultHighWaterMarkRatio(0.9);
  // The subscriber keeps the 0.5 it was baked with.
  assert.deepEqual(queueMark(bus, sub.id), { ratio: 0.5, mark: 5 });
  for (let i = 5; i < 8; i++) bus.publish('t', i);
  assert.equal(pressured.length, 1, 'still one excursion — no re-trip at the new mark');
  await flush();
  assert.equal(drained.length, 1);
  assert.equal(drained[0].highWaterMark, 5, 'release still reads the subscribe-time mark');
  // A subscriber joining after the revision takes the new default.
  const latePressured: BackpressureEvent[] = [];
  const late = bus.subscribe('u', () => {}, {
    queueSize: 10,
    onBackpressure: (e) => latePressured.push(e),
  });
  assert.deepEqual(queueMark(bus, late.id), { ratio: 0.9, mark: 9 });
  for (let i = 0; i < 8; i++) bus.publish('u', i);
  assert.equal(latePressured.length, 0);
  bus.publish('u', 8);
  assert.equal(latePressured.length, 1);
  assert.equal(latePressured[0].queueSize, 9);
  late.unsubscribe();
  sub.unsubscribe();
});

test('EB-476: the throttle threshold follows the bus default ratio for a later subscriber', () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(0.5);
  const throttled: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => throttled.push(e),
  });
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  assert.equal(throttled.length, 0);
  bus.publish('t', 4); // 5th message crosses capacity x 0.5
  assert.equal(throttled.length, 1);
  assert.equal(throttled[0].queueSize, 5);
  sub.unsubscribe();

  // Without a default the mark is 0.8: five messages do not throttle.
  const plain = new EventBus();
  const plainThrottled: ThrottleEvent[] = [];
  const plainSub = plain.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => plainThrottled.push(e),
  });
  for (let i = 0; i < 5; i++) plain.publish('t', i);
  assert.equal(plainThrottled.length, 0);
  for (let i = 5; i < 8; i++) plain.publish('t', i);
  assert.equal(plainThrottled.length, 1);
  plainSub.unsubscribe();
});

test('EB-476: setHighWaterMarkRatio still revises one subscriber at a time and leaves the bus default in place', () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(0.5);
  const sub = bus.subscribe('t', () => {}, { queueSize: 10 });
  bus.setHighWaterMarkRatio(sub.id, 0.9);
  assert.deepEqual(queueMark(bus, sub.id), { ratio: 0.9, mark: 9 });
  assert.equal(bus.getDefaultHighWaterMarkRatio(), 0.5);
  // The per-subscriber revision carries no audit of its own.
  assert.equal(defaultAudits(bus).length, 1);
  // A subscriber joining after the per-subscriber revision still takes the bus default.
  const late = bus.subscribe('u', () => {}, { queueSize: 10 });
  assert.deepEqual(queueMark(bus, late.id), { ratio: 0.5, mark: 5 });
  late.unsubscribe();
  sub.unsubscribe();
});

test('EB-476: a subscriber already throttling above the mark is untouched by a revision — release still uses the subscribe-time threshold', async () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(0.5);
  const throttled: ThrottleEvent[] = [];
  const drained: DrainedEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => throttled.push(e),
    onDrained: (e) => drained.push(e),
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i); // throttle engages at mark 5
  assert.equal(throttled.length, 1);
  assert.equal(bus.getStats().throttledSubscribers, 1);
  bus.setDefaultHighWaterMarkRatio(0.9);
  assert.deepEqual(queueMark(bus, sub.id), { ratio: 0.5, mark: 5 });
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(drained.length, 1);
  assert.equal(
    drained[0].highWaterMark,
    5,
    'the release fires against the subscribe-time mark, not the revised 0.9',
  );
  assert.equal(bus.getStats().throttledSubscribers, 0);
  sub.unsubscribe();
});

test('EB-476: setDefaultHighWaterMarkRatio audits each revision with its old and new ratio and actor', () => {
  const bus = new EventBus();
  bus.setDefaultHighWaterMarkRatio(0.5, 'ops');
  bus.setDefaultHighWaterMarkRatio(0.9, 'ops');
  bus.setDefaultHighWaterMarkRatio(null, 'ops');
  const events = defaultAudits(bus);
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, highWaterMarkRatio: 0.5 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 0.5, highWaterMarkRatio: 0.9 });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 0.9, highWaterMarkRatio: null });
  assert.equal(bus.getDefaultHighWaterMarkRatio(), undefined);
});
