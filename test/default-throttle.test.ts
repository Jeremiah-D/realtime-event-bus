import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type ThrottleEvent } from '../src/bus.ts';

/**
 * Bus-level default backpressure-throttle rate bounds (EB-488):
 * `setDefaultThrottle` installs the `minRatePerSec` /
 * `maxRatePerSec` / `initialRatePerSec` new opt-in throttled
 * subscribers fall back to when they omit a field — the priority
 * chain is subscriber-explicit > bus default > documented built-ins
 * (1 / `Infinity` / one high-water-mark worth per second) — and the
 * default never enables throttling: a subscriber that does not opt
 * into `throttle` is untouched by it.
 *
 * Rate assertions follow `throttle.test.ts`: an injectable clock
 * makes engagement deterministic, and the `onThrottled` engagement
 * event carries the effective initial rate (`ratePerSec`, clamped
 * into `[minRatePerSec, maxRatePerSec]`), so it pins all three
 * bounds at once.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Subscribes a throttled subscriber and publishes until it engages. */
function engage(
  bus: EventBus,
  publishes: number,
  opts: Parameters<EventBus['subscribe']>[2] = {},
): { events: ThrottleEvent[]; sub: ReturnType<EventBus['subscribe']> } {
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 10,
    throttle: true,
    onThrottled: (e) => events.push(e),
    ...opts,
  });
  for (let i = 0; i < publishes; i += 1) bus.publish('t', i);
  return { events, sub };
}

test('EB-488: the factory bus default is unset (throttle: true still uses the built-ins)', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  assert.equal(bus.getDefaultThrottle(), undefined);
  const { events, sub } = engage(bus, 8);
  // Built-in initial rate: one high-water-mark worth per second = 10 * 0.8 = 8/s.
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 8);
  assert.equal(events[0].adapted, false);
  assert.equal(events[0].queueSize, 8);
  assert.equal(events[0].capacity, 10);
  assert.equal(bus.getStats().throttledSubscribers, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'default_throttle_revised' }).length, 0);
  sub.unsubscribe();
});

test('EB-488: throttle: true takes all three bus-default fields', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 3 });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 20,
    throttle: true,
    onThrottled: (e) => events.push(e),
  });
  // High-water mark at 20 * 0.8 = 16: the 16th publish engages.
  for (let i = 0; i < 16; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 3);
  // Bucket holds ceil(3) = 3 tokens: 3 pass, 2 are shed without touching the queue.
  const accepted: number[] = [];
  for (let i = 0; i < 5; i += 1) accepted.push(bus.publish('t', `extra-${i}`));
  assert.deepEqual(accepted, [1, 1, 1, 0, 0]);
  assert.equal(bus.throttledCount(sub.id), 2);
  assert.equal(bus.pendingCount(sub.id), 19); // 16 + 3 passed the bucket
  assert.equal(bus.droppedCount(sub.id), 0); // shed by throttle, not the queue
  sub.unsubscribe();
});

test('EB-488: a partial object falls back field by field and an explicit field always wins', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 6, initialRatePerSec: 4 });
  // Explicit max clamps the default initial down: min(4, 3) = 3.
  const cappedByMax = engage(bus, 8, { throttle: { maxRatePerSec: 3 } });
  assert.equal(cappedByMax.events.length, 1);
  assert.equal(cappedByMax.events[0].ratePerSec, 3);
  cappedByMax.sub.unsubscribe();
  // Explicit min clamps the default initial up: max(4, 5) = 5.
  const flooredByMin = engage(bus, 8, { throttle: { minRatePerSec: 5 } });
  assert.equal(flooredByMin.events.length, 1);
  assert.equal(flooredByMin.events[0].ratePerSec, 5);
  flooredByMin.sub.unsubscribe();
  // Explicit initial beats the default initial outright.
  const explicitInitial = engage(bus, 8, { throttle: { initialRatePerSec: 5 } });
  assert.equal(explicitInitial.events.length, 1);
  assert.equal(explicitInitial.events[0].ratePerSec, 5);
  explicitInitial.sub.unsubscribe();
});

test('EB-488: the bus-default initial rate is absolute and replaces the capacity-derived fallback', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultThrottle({ initialRatePerSec: 7 });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: true,
    onThrottled: (e) => events.push(e),
  });
  // Without a default the initial rate would be 100 * 0.8 = 80/s.
  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 7);
  assert.equal(events[0].queueSize, 80);
  assert.equal(events[0].capacity, 100);
  sub.unsubscribe();
});

test('EB-488: null clears the default back to the built-ins', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 3 });
  bus.setDefaultThrottle(null);
  assert.equal(bus.getDefaultThrottle(), undefined);
  const { events, sub } = engage(bus, 8);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 8, 'one high-water-mark worth per second again');
  sub.unsubscribe();
});

test('EB-488: the default never enables throttling', async () => {
  const bus = new EventBus();
  bus.setDefaultThrottle({ minRatePerSec: 1, maxRatePerSec: 2, initialRatePerSec: 1 });
  const received: unknown[] = [];
  // No `throttle` opt-in: still one handler call per message.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 20; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, Array.from({ length: 20 }, (_, i) => i));
  assert.equal(bus.getStats().throttledSubscribers, 0);
  assert.equal(bus.throttledCount(sub.id), 0);
  sub.unsubscribe();
});

test('EB-488: a later revision never moves an already-subscribed throttle', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const { events, sub } = engage(bus, 0);
  // Baked at subscribe time with the built-in initial rate of 8/s.
  bus.setDefaultThrottle({ minRatePerSec: 1, maxRatePerSec: 3, initialRatePerSec: 2 });
  for (let i = 0; i < 8; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 8, 'the baked rate survives the revision');
  sub.unsubscribe();
});

test('EB-488: setThrottle keeps its own semantics — partial revisions take the live value, null clears without the default re-applying', () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultThrottle({ minRatePerSec: 1, maxRatePerSec: 10, initialRatePerSec: 3 });
  const events: ThrottleEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: { initialRatePerSec: 5 },
    onThrottled: (e) => events.push(e),
  });
  // A partial revision keeps the live initial rate (5), not the default (3).
  bus.setThrottle(sub.id, { minRatePerSec: 2 });
  for (let i = 0; i < 80; i += 1) bus.publish('t', i);
  assert.equal(events.length, 1);
  assert.equal(events[0].ratePerSec, 5);
  bus.setThrottle(sub.id, null);
  for (let i = 0; i < 20; i += 1) bus.publish('t', i);
  assert.equal(
    bus.getStats().throttledSubscribers,
    0,
    'the cleared subscriber publishes at full speed even though a default is installed',
  );
  assert.equal(bus.pendingCount(sub.id), 100);
  assert.throws(
    () => bus.setThrottle(sub.id, { minRatePerSec: 2 }),
    RangeError,
  );
  sub.unsubscribe();
});

test('EB-488: revising to the current default is a no-op with no audit, and the getter follows the live value and returns a copy', () => {
  const bus = new EventBus();
  bus.setDefaultThrottle(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_throttle_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultThrottle({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_throttle_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultThrottle(), undefined);
  const opts = { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 };
  bus.setDefaultThrottle(opts);
  opts.initialRatePerSec = 999;
  assert.deepEqual(
    bus.getDefaultThrottle(),
    { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 });
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 });
  assert.equal(bus.getDefaultThrottle()?.initialRatePerSec, 4);
  const audit = bus.getSubscriptionAudit({ action: 'default_throttle_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    throttle: { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 },
  });
  const held = bus.getDefaultThrottle();
  assert.ok(held !== undefined);
  held.initialRatePerSec = 999;
  assert.deepEqual(
    bus.getDefaultThrottle(),
    { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 },
    'the getter returns a copy',
  );
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 5 }, 'ops');
  assert.deepEqual(bus.getDefaultThrottle(), { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'default_throttle_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 },
    throttle: { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 5 },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultThrottle(null);
  assert.equal(bus.getDefaultThrottle(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_throttle_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 5 },
    throttle: null,
  });
  bus.setDefaultThrottle(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_throttle_revised' }).length,
    3,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-488: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultThrottle({ minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 });
  const bad = [
    0,
    -1,
    '2',
    [],
    true,
    { minRatePerSec: 0 },
    { minRatePerSec: -1 },
    { minRatePerSec: Number.NaN },
    { minRatePerSec: Number.POSITIVE_INFINITY },
    { minRatePerSec: '2' },
    { maxRatePerSec: 0 },
    { maxRatePerSec: -1 },
    { maxRatePerSec: Number.NaN },
    { maxRatePerSec: '8' },
    { initialRatePerSec: 0 },
    { initialRatePerSec: -2 },
    { initialRatePerSec: Number.NaN },
    { initialRatePerSec: Number.POSITIVE_INFINITY },
    { initialRatePerSec: '4' },
    { minRatePerSec: 5, maxRatePerSec: 2 },
    undefined,
  ] as unknown[];
  for (const opts of bad) {
    assert.throws(
      () => bus.setDefaultThrottle(opts as never),
      RangeError,
      `opts=${String(opts)}`,
    );
    assert.deepEqual(bus.getDefaultThrottle(), { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 });
  }
  assert.throws(() => bus.setDefaultThrottle({ minRatePerSec: 3 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultThrottle(), { minRatePerSec: 2, maxRatePerSec: 8, initialRatePerSec: 4 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_throttle_revised' }).length,
    1,
    'failed revisions must not audit',
  );
  // `Infinity` is legal for the ceiling only.
  bus.setDefaultThrottle({ maxRatePerSec: Number.POSITIVE_INFINITY });
  assert.deepEqual(bus.getDefaultThrottle(), { maxRatePerSec: Number.POSITIVE_INFINITY });
});
