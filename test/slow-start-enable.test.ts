import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const revisionAudits = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'slow_start_revised' });

test('EB-541: true on a never-configured subscriber enables with the documented defaults', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));

  bus.setSlowStart(sub.id, true);
  assert.equal(bus.getStats().rampingSubscribers, 1, 'the fresh gate ramps from the enable moment');
  for (let i = 0; i < 15; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    'the default gate admits its ten-token initial budget',
  );
  assert.equal(bus.pendingCount(sub.id), 5);

  const events = revisionAudits(bus);
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 10, rampMs: 1000, strategy: 'linear' },
    oldSlowStart: null,
  });
  sub.unsubscribe();
});

test('EB-541: a partial enable resolves omitted fields against the live bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  const other = bus.subscribe('t', () => {});

  // Explicit wins, the bus default fills the rest, the documented
  // default covers nothing here.
  bus.setSlowStart(sub.id, { rampMs: 2000 });
  const events = revisionAudits(bus);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 2, rampMs: 2000, strategy: 'linear' },
    oldSlowStart: null,
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1], 'the enabled gate admits the default two-token budget');
  assert.equal(bus.pendingCount(sub.id), 3);

  // `true` with a bus default installed takes the default wholesale.
  bus.setSlowStart(other.id, true);
  assert.deepEqual(revisionAudits(bus)[1].detail, {
    slowStart: { initialPerSec: 2, rampMs: 5000, strategy: 'linear' },
    oldSlowStart: null,
  });
  sub.unsubscribe();
  other.unsubscribe();
});

test('EB-541: null on a never-configured subscriber is a silent no-op', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));

  bus.setSlowStart(sub.id, null);
  assert.equal(revisionAudits(bus).length, 0, 'clearing what was never set audits nothing');
  assert.equal(bus.getStats().rampingSubscribers, 0);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4], 'the subscriber stays at full speed');
  assert.equal(bus.pendingCount(sub.id), 0);
  sub.unsubscribe();
});

test('EB-541: illegal enables throw RangeError and leave the subscriber unconfigured', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));

  assert.throws(() => bus.setSlowStart('sub-999', true), RangeError);
  assert.throws(() => bus.setSlowStart('', true), RangeError);
  const bad: unknown[] = [
    undefined,
    false,
    'yes',
    42,
    { initialPerSec: 0 },
    { initialPerSec: -1 },
    { initialPerSec: Number.NaN },
    { initialPerSec: Number.POSITIVE_INFINITY },
    { rampMs: 0 },
    { rampMs: -5 },
    { rampMs: Number.POSITIVE_INFINITY },
    { strategy: 'cubic' },
  ];
  for (const opts of bad) {
    assert.throws(
      () => bus.setSlowStart(sub.id, opts as never),
      RangeError,
      `expected RangeError for ${String(opts)}`,
    );
  }

  assert.equal(revisionAudits(bus).length, 0, 'failed enables audit nothing');
  assert.equal(bus.getStats().rampingSubscribers, 0, 'the subscriber stays unconfigured');
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2], 'the subscriber stays at full speed after failed enables');
  // And a legal call still enables afterwards.
  bus.setSlowStart(sub.id, { initialPerSec: 1, rampMs: 10000 });
  assert.equal(bus.getStats().rampingSubscribers, 1);
  sub.unsubscribe();
});

test('EB-541: the enable paces the next drain from the enable moment', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  // Enable before the scheduled flush runs: the backlog is not
  // reordered or re-judged, it simply leaves under the new gate.
  bus.setSlowStart(sub.id, { initialPerSec: 2, rampMs: 10000 });
  await flush();
  assert.deepEqual(received, [0, 1], 'the fresh gate admits its two-token budget');
  assert.equal(bus.pendingCount(sub.id), 3);
  sub.unsubscribe();
});

test('EB-541: the enable target is the live shaping cap, and a no-headroom enable is a no-op', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 5, burst: 50 },
  });

  // 5 is at the live shaping cap: no ramp to run, so the subscriber
  // stays unconfigured — exactly like subscribe-time inertness.
  bus.setSlowStart(sub.id, { initialPerSec: 5, rampMs: 100 });
  assert.equal(revisionAudits(bus).length, 0, 'a no-headroom enable audits nothing');
  assert.equal(bus.getStats().rampingSubscribers, 0);
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2], 'the subscriber stays at full speed after the no-op');
  assert.equal(bus.pendingCount(sub.id), 0);

  // Below the cap a real gate is built, and it ramps toward the cap.
  bus.setSlowStart(sub.id, { initialPerSec: 4, rampMs: 10000 });
  assert.equal(bus.getStats().rampingSubscribers, 1, 'the below-cap enable ramps');
  for (let i = 3; i < 9; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5, 6], 'the fresh gate admits its four-token budget');
  assert.equal(bus.pendingCount(sub.id), 2);
  const events = revisionAudits(bus);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 4, rampMs: 10000, strategy: 'linear' },
    oldSlowStart: null,
  });
  sub.unsubscribe();
});

test('EB-541: a clear can be followed by a fresh enable', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    slowStart: { initialPerSec: 1, rampMs: 10000 },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0]);
  assert.equal(bus.pendingCount(sub.id), 2);

  bus.setSlowStart(sub.id, null);
  await flush();
  assert.deepEqual(received, [0, 1, 2], 'clearing drains the backlog at full speed');

  // The EB-359 "a clear is terminal" rule no longer applies to this
  // method: the same setter re-enables with a fresh gate.
  bus.setSlowStart(sub.id, { initialPerSec: 3, rampMs: 10000, strategy: 'exponential' });
  assert.equal(bus.getStats().rampingSubscribers, 1, 'the re-enable re-ramps');
  for (let i = 3; i < 8; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5], 'the fresh gate admits its three-token budget');
  assert.equal(bus.pendingCount(sub.id), 2);
  const events = revisionAudits(bus);
  assert.equal(events.length, 2);
  assert.deepEqual(events[1].detail, {
    slowStart: { initialPerSec: 3, rampMs: 10000, strategy: 'exponential' },
    oldSlowStart: null,
  });
  sub.unsubscribe();
});

test('EB-541: a dropped completed gate re-enables with oldSlowStart null', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    slowStart: { initialPerSec: 1, rampMs: 100 },
  });
  bus.publish('t', 0);
  await flush();
  assert.deepEqual(received, [0]);

  // Advance past the ramp window, then drain once more: the completed
  // gate is dropped by the drain, leaving the subscriber unconfigured.
  clock.advance(200);
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.getStats().rampingSubscribers, 0, 'the completed gate was dropped');

  bus.setSlowStart(sub.id, { initialPerSec: 2, rampMs: 10000 });
  assert.equal(bus.getStats().rampingSubscribers, 1, 'the re-enable re-ramps from the enable moment');
  for (let i = 2; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3], 'the fresh gate admits its two-token budget');
  assert.equal(bus.pendingCount(sub.id), 1);
  const events = revisionAudits(bus);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 2, rampMs: 10000, strategy: 'linear' },
    oldSlowStart: null,
  });
  sub.unsubscribe();
});

test('EB-541: a migration round-trip restores the subscriber unconfigured', async () => {
  const source = new EventBus();
  const sub = source.subscribe('t', () => {});
  source.setSlowStart(sub.id, { initialPerSec: 1, rampMs: 10000 });
  assert.equal(source.getStats().rampingSubscribers, 1, 'the enable lands on the source');

  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);

  // The restart path: a fresh bus imports the snapshot, which carries
  // only the subscribe-time options — the live enable does not travel.
  const target = new EventBus();
  const targetReceived: unknown[] = [];
  const res = target.importSubscriptionSnapshot(snap, (m: BusMessage) => {
    targetReceived.push(m.payload);
  });
  assert.deepEqual(res, { restored: 1, skipped: 0 });
  assert.equal(revisionAudits(target).length, 0, 'no enable audit on the restarted bus');
  assert.equal(target.getStats().rampingSubscribers, 0, 'the restored subscriber is unconfigured');

  for (let i = 0; i < 3; i++) target.publish('t', i);
  await flush();
  assert.deepEqual(targetReceived, [0, 1, 2], 'the restored subscriber delivers at full speed');
  sub.unsubscribe();
});

test('EB-541: a post-enable revision replaces the gate and re-ramps with the target re-read live', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));

  bus.setSlowStart(sub.id, { initialPerSec: 1, rampMs: 10000 });
  // A revision after an enable follows EB-332: it is a full
  // replacement — a field left out takes the documented default, not
  // the enabled value — and the fresh gate re-ramps from the revision
  // moment with the target re-read live.
  bus.setSlowStart(sub.id, { initialPerSec: 3, rampMs: 2000 });
  const events = revisionAudits(bus);
  assert.equal(events.length, 2);
  assert.deepEqual(events[1].detail, {
    slowStart: { initialPerSec: 3, rampMs: 2000, strategy: 'linear' },
    oldSlowStart: { initialPerSec: 1, rampMs: 10000, strategy: 'linear' },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2], 'the replacement gate admits its three-token budget');
  assert.equal(bus.pendingCount(sub.id), 2);

  // The partial-revision rule is unchanged: omitted fields take the
  // documented defaults, they are not merged from the live gate.
  bus.setSlowStart(sub.id, { rampMs: 500 });
  assert.deepEqual(revisionAudits(bus)[2].detail, {
    slowStart: { initialPerSec: 10, rampMs: 500, strategy: 'linear' },
    oldSlowStart: { initialPerSec: 3, rampMs: 2000, strategy: 'linear' },
  });
  sub.unsubscribe();
});
