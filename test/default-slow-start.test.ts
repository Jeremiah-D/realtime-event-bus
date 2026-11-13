import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default slow-start ramp (EB-486):
 * `setDefaultSlowStart` installs the `initialPerSec` / `rampMs` /
 * `strategy` new opt-in slow-start subscribers fall back to when they
 * omit a field — the priority chain is subscriber-explicit > bus
 * default > 10 / 1000 / `'linear'` — and the default never enables
 * slow-start: a subscriber that does not opt into `slowStart` is
 * untouched by it.
 *
 * Ramp assertions follow `slowstart.test.ts`: an injectable clock
 * makes token refills deterministic, and a publish triggers the next
 * flush round.
 */

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

test('EB-486: the factory bus default is unset (slowStart: true still means 10/1000/linear)', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  assert.equal(bus.getDefaultSlowStart(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: true,
  });
  assert.equal(bus.getStats().rampingSubscribers, 1);
  for (let i = 0; i < 15; i++) bus.publish('t', i);
  await flush();
  // One second of initial budget (10) goes at once; the rest waits.
  assert.equal(received.length, 10);
  assert.equal(bus.pendingCount(sub.id), 5);
  clock.advance(500); // halfway through the 1000ms ramp: rate 15/s
  bus.publish('t', 'tick');
  await flush();
  assert.equal(received.length, 16, 'the 500ms refill drains the rest');
  clock.advance(600); // past the ramp window: the gate is complete
  assert.equal(bus.getStats().rampingSubscribers, 0);
  sub.unsubscribe();
});

test('EB-486: a new opt-in subscriber takes the bus default ramp, then climbs and drains', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: true,
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  // The default initial budget is 2, not 10.
  assert.equal(received.length, 2);
  assert.equal(bus.pendingCount(sub.id), 8);
  clock.advance(2500); // halfway through the 5000ms ramp: rate 3/s
  bus.publish('t', 'tick');
  await flush();
  assert.equal(received.length, 5, 'the refill runs at the default ramp');
  clock.advance(2500); // past the ramp window: the gate is dropped
  bus.publish('t', 'tick');
  await flush();
  assert.equal(received.length, 12);
  assert.equal(bus.getStats().rampingSubscribers, 0);
  sub.unsubscribe();
});

test('EB-486: an explicit initialPerSec beats the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: { initialPerSec: 10 },
  });
  for (let i = 0; i < 15; i++) bus.publish('t', i);
  await flush();
  // The explicit 10 wins over the default 2.
  assert.equal(received.length, 10);
  assert.equal(bus.pendingCount(sub.id), 5);
  sub.unsubscribe();
});

test('EB-486: an explicit rampMs beats the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: { rampMs: 1000 },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(received.length, 2, 'the initial rate still comes from the default');
  clock.advance(1000); // the explicit 1000ms ramp is over
  bus.publish('t', 'tick');
  await flush();
  assert.equal(received.length, 11, 'the gate is complete after the explicit ramp');
  assert.equal(bus.getStats().rampingSubscribers, 0);
  sub.unsubscribe();
});

test('EB-486: an explicit strategy beats the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ strategy: 'exponential', initialPerSec: 10, rampMs: 1000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 500,
    deliveryShaping: { messagesPerSec: 100 },
    slowStart: { strategy: 'linear' },
  });
  for (let i = 0; i < 200; i++) bus.publish('t', i);
  await flush();
  assert.equal(received.length, 10, 'the initial budget comes from the default');
  clock.advance(500); // halfway through the ramp, toward the 100/s cap
  bus.publish('t', 'tick');
  await flush();
  // Linear: 10 + (100 - 10) * 0.5 = 55/s -> 27 tokens. Exponential
  // would give 10 * sqrt(10) = 31.6/s -> 15 tokens.
  assert.equal(received.length, 37, 'the explicit linear curve wins over the default exponential');
  sub.unsubscribe();
});

test('EB-486: a default rampMs alone leaves initialPerSec at 10 and strategy linear', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ rampMs: 5000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: true,
  });
  for (let i = 0; i < 30; i++) bus.publish('t', i);
  await flush();
  assert.equal(received.length, 10, 'the initial rate is still the documented 10');
  clock.advance(500); // 10% into the 5000ms ramp: rate 11/s -> ~5 tokens
  bus.publish('t', 'tick');
  await flush();
  assert.equal(received.length, 15, 'the refill runs on the default 5000ms ramp');
  clock.advance(4500); // past the ramp window: the gate is dropped
  bus.publish('t', 'tick');
  await flush();
  assert.equal(received.length, 32);
  assert.equal(bus.getStats().rampingSubscribers, 0);
  sub.unsubscribe();
});

test('EB-486: clearing with null returns new opt-in subscribers to 10/1000/linear', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  bus.setDefaultSlowStart(null);
  assert.equal(bus.getDefaultSlowStart(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: true,
  });
  for (let i = 0; i < 15; i++) bus.publish('t', i);
  await flush();
  assert.equal(received.length, 10);
  assert.equal(bus.pendingCount(sub.id), 5);
  sub.unsubscribe();
});

test('EB-486: revising the bus default never moves an already-subscribed ramp or its armed re-flush timer', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: true,
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.pendingCount(sub.id), 3);
  bus.setDefaultSlowStart({ initialPerSec: 1, rampMs: 5000 });
  clock.advance(500); // old rate 2/s -> ~1 token; the new 1/s would give ~0.5
  bus.publish('t', 'tick');
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2],
    'the early subscriber keeps the ramp it was baked with',
  );
  assert.equal(bus.pendingCount(sub.id), 3);

  // A subscriber joining after the revision takes the new default.
  const lateReceived: unknown[] = [];
  const late = bus.subscribe('u', (msg) => lateReceived.push(msg.payload), {
    queueSize: 100,
    slowStart: true,
  });
  for (let i = 0; i < 3; i++) bus.publish('u', i);
  await flush();
  assert.deepEqual(lateReceived, [0], 'the new default initial budget is 1');
  sub.unsubscribe();
  late.unsubscribe();
});

test('EB-486: the default never enables slow-start', async () => {
  const bus = new EventBus();
  bus.setDefaultSlowStart({ initialPerSec: 1, rampMs: 5000 });
  const received: unknown[] = [];
  // No `slowStart` opt-in: still one handler call per message.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().rampingSubscribers, 0);
  sub.unsubscribe();
});

test('EB-486: setSlowStart partial revision still takes the documented defaults, null clears without the default re-applying, and a later revision is refused', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    slowStart: true,
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  // Partial revision: only the ramp moves; the initial rate takes the
  // documented 10, not the bus default 2.
  bus.setSlowStart(sub.id, { rampMs: 1000 });
  const events = bus.getSubscriptionAudit({ action: 'slow_start_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    slowStart: { initialPerSec: 10, rampMs: 1000, strategy: 'linear' },
    oldSlowStart: { initialPerSec: 2, rampMs: 5000, strategy: 'linear' },
  });
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4], 'the fresh 10-token gate drains the backlog');
  bus.setSlowStart(sub.id, null);
  assert.equal(bus.getStats().rampingSubscribers, 0, 'the ramp is gone');
  for (let i = 5; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    'the cleared subscriber delivers at full speed even though a default is installed',
  );
  assert.throws(() => bus.setSlowStart(sub.id, { initialPerSec: 5 }), RangeError);
  bus.publish('t', 10);
  bus.publish('t', 11);
  await flush();
  assert.equal(received.length, 12, 'still full speed after the refused revision');
  sub.unsubscribe();
});

test('EB-486: the headless-room rule still applies to the default initial rate', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSlowStart({ initialPerSec: 10, rampMs: 5000 });
  const received: unknown[] = [];
  // The default initial rate is already at the shaping cap: no gate.
  const flat = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    deliveryShaping: { messagesPerSec: 10 },
    slowStart: true,
  });
  assert.equal(bus.getStats().rampingSubscribers, 0, 'initialPerSec already at the default target: no ramp');
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  flat.unsubscribe();
  const rampingReceived: unknown[] = [];
  const ramping = bus.subscribe('u', (msg) => rampingReceived.push(msg.payload), {
    queueSize: 100,
    deliveryShaping: { messagesPerSec: 100 },
    slowStart: true,
  });
  assert.equal(bus.getStats().rampingSubscribers, 1, 'a slower start still ramps toward the higher cap');
  ramping.unsubscribe();
});

test('EB-486: revising to the current default is a no-op with no audit, and the getter follows the live value and returns a copy', () => {
  const bus = new EventBus();
  bus.setDefaultSlowStart(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_slow_start_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultSlowStart({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_slow_start_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultSlowStart(), undefined);
  const opts = { initialPerSec: 2, rampMs: 5000 };
  bus.setDefaultSlowStart(opts);
  opts.initialPerSec = 999;
  assert.deepEqual(
    bus.getDefaultSlowStart(),
    { initialPerSec: 2, rampMs: 5000 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000, strategy: undefined });
  assert.equal(bus.getDefaultSlowStart()?.initialPerSec, 2);
  const audit = bus.getSubscriptionAudit({ action: 'default_slow_start_revised' });
  assert.equal(audit.length, 1, 're-affirming the default (undefined counts as missing) audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    slowStart: { initialPerSec: 2, rampMs: 5000 },
  });
  const held = bus.getDefaultSlowStart();
  assert.ok(held !== undefined);
  held.initialPerSec = 999;
  assert.deepEqual(bus.getDefaultSlowStart(), { initialPerSec: 2, rampMs: 5000 }, 'the getter returns a copy');
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000, strategy: 'exponential' }, 'ops');
  assert.deepEqual(bus.getDefaultSlowStart(), {
    initialPerSec: 2,
    rampMs: 5000,
    strategy: 'exponential',
  });
  const revised = bus.getSubscriptionAudit({ action: 'default_slow_start_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { initialPerSec: 2, rampMs: 5000 },
    slowStart: { initialPerSec: 2, rampMs: 5000, strategy: 'exponential' },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultSlowStart(null);
  assert.equal(bus.getDefaultSlowStart(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_slow_start_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { initialPerSec: 2, rampMs: 5000, strategy: 'exponential' },
    slowStart: null,
  });
  bus.setDefaultSlowStart(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_slow_start_revised' }).length,
    3,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-486: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultSlowStart({ initialPerSec: 2, rampMs: 5000 });
  const bad = [
    0,
    -1,
    '2',
    [],
    true,
    { initialPerSec: 0 },
    { initialPerSec: -5 },
    { initialPerSec: Number.NaN },
    { initialPerSec: Number.POSITIVE_INFINITY },
    { initialPerSec: '2' },
    { rampMs: 0 },
    { rampMs: -1 },
    { rampMs: Number.NaN },
    { rampMs: Number.POSITIVE_INFINITY },
    { rampMs: '5000' },
    { strategy: 'cubic' },
    { strategy: 5 },
    undefined,
  ] as unknown[];
  for (const opts of bad) {
    assert.throws(
      () => bus.setDefaultSlowStart(opts as never),
      RangeError,
      `opts=${String(opts)}`,
    );
    assert.deepEqual(bus.getDefaultSlowStart(), { initialPerSec: 2, rampMs: 5000 });
  }
  assert.throws(() => bus.setDefaultSlowStart({ initialPerSec: 3 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultSlowStart(), { initialPerSec: 2, rampMs: 5000 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_slow_start_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});
