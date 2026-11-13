import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default sliding-window rate limit (EB-487):
 * `setDefaultRateLimit` installs the `maxMessages` / `perWindowMs`
 * new opt-in rate-limited subscribers fall back to when they omit a
 * field — the priority chain is subscriber-explicit > bus default >
 * required (no documented fallback) — and the default never enables
 * rate limiting: a subscriber that does not opt into `rateLimit` is
 * untouched by it.
 *
 * Window assertions follow `ratewindow.test.ts`: an injectable clock
 * makes window slides deterministic, and a publish triggers the next
 * flush round.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

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

function waitingFor(bus: EventBus, subscriberId: string): number {
  const row = bus.getStats().rateLimitedWaiting.find((r) => r.subscriberId === subscriberId);
  assert.ok(row !== undefined, 'expected a rateLimitedWaiting row');
  return row.waiting;
}

test('EB-487: the factory bus default is unset (a partial rateLimit still throws)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultRateLimit(), undefined);
  assert.throws(
    () => bus.subscribe('t', () => {}, { rateLimit: { maxMessages: 2 } as never }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { rateLimit: {} as never }),
    RangeError,
  );
  assert.equal(bus.getSubscriptionAudit({ action: 'default_rate_limit_revised' }).length, 0);
});

test('EB-487: a new opt-in subscriber passes rateLimit: {} and takes both default fields', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: {},
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  // Hard cap of 2 per rolling second: the other 3 wait — never dropped.
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.pendingCount(sub.id), 3);
  assert.equal(bus.droppedCount(sub.id), 0);
  assert.equal(waitingFor(bus, sub.id), 3);
  assert.equal(bus.getStats().sequenceGaps, 0);
  clock.advance(1001); // the two deliveries at t=0 slide out of the window
  bus.publish('t', 'kick'); // also triggers a flush
  await flush();
  // The refilled window admits 2 more; the rest still wait.
  assert.deepEqual(received, [0, 1, 2, 3]);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(waitingFor(bus, sub.id), 2);
  clock.advance(1001); // the deliveries at t=1001 slide out
  bus.publish('t', 'kick2');
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 'kick']);
  assert.equal(bus.pendingCount(sub.id), 1);
  clock.advance(1001); // the deliveries at t=2002 slide out
  bus.publish('t', 'kick3');
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 'kick', 'kick2', 'kick3']);
  assert.equal(bus.pendingCount(sub.id), 0);
  assert.equal(waitingFor(bus, sub.id), 0);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('EB-487: an explicit maxMessages beats the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 3 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  // The explicit cap is 3, not the default 2; the window is the default 1000ms.
  assert.deepEqual(received, [0, 1, 2]);
  assert.equal(waitingFor(bus, sub.id), 2);
  clock.advance(1001);
  bus.publish('t', 'kick');
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 'kick']);
  sub.unsubscribe();
});

test('EB-487: an explicit perWindowMs beats the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { perWindowMs: 200 },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  // The cap is the default 2; the window is the explicit 200ms.
  assert.deepEqual(received, [0, 1]);
  assert.equal(waitingFor(bus, sub.id), 1);
  clock.advance(201); // the two deliveries at t=0 slide out of the 200ms window
  bus.publish('t', 'kick');
  await flush();
  assert.deepEqual(received, [0, 1, 2, 'kick']);
  sub.unsubscribe();
});

test('EB-487: only a perWindowMs default installed — { maxMessages: 5 } takes it, {} still throws, no rateLimit stays unlimited', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultRateLimit({ perWindowMs: 1000 });
  assert.deepEqual(bus.getDefaultRateLimit(), { perWindowMs: 1000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 5 },
  });
  for (let i = 0; i < 6; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(waitingFor(bus, sub.id), 1);
  clock.advance(1001);
  bus.publish('t', 'kick');
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5, 'kick']);
  sub.unsubscribe();
  // No default covers maxMessages: a bare opt-in still throws.
  assert.throws(() => bus.subscribe('u', () => {}, { rateLimit: {} }), RangeError);
  // And a subscriber that never opts in is untouched by the default.
  const freeReceived: unknown[] = [];
  const free = bus.subscribe('v', (msg) => freeReceived.push(msg.payload));
  for (let i = 0; i < 5; i++) bus.publish('v', i);
  await flush();
  assert.deepEqual(freeReceived, [0, 1, 2, 3, 4]);
  assert.deepEqual(bus.getStats().rateLimitedWaiting, []);
  free.unsubscribe();
});

test('EB-487: clearing with null returns new opt-in subscribers to both fields required', () => {
  const bus = new EventBus();
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  bus.setDefaultRateLimit(null);
  assert.equal(bus.getDefaultRateLimit(), undefined);
  assert.throws(() => bus.subscribe('t', () => {}, { rateLimit: {} }), RangeError);
  assert.throws(
    () => bus.subscribe('t', () => {}, { rateLimit: { maxMessages: 2 } as never }),
    RangeError,
  );
});

test('EB-487: revising the bus default never moves an already-subscribed window or its armed re-flush timer', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: {},
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.pendingCount(sub.id), 3);
  bus.setDefaultRateLimit({ maxMessages: 1, perWindowMs: 1000 });
  clock.advance(1001); // the old window slides out
  bus.publish('t', 'tick');
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2, 3],
    'the early subscriber keeps the window bounds it was baked with',
  );
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(waitingFor(bus, sub.id), 2);

  // A subscriber joining after the revision takes the new default.
  const lateReceived: unknown[] = [];
  const late = bus.subscribe('u', (msg) => lateReceived.push(msg.payload), {
    rateLimit: {},
  });
  for (let i = 0; i < 3; i++) bus.publish('u', i);
  await flush();
  assert.deepEqual(lateReceived, [0], 'the new default cap is 1');
  sub.unsubscribe();
  late.unsubscribe();

  // The armed re-flush timer also survives a revision: with a real
  // clock the backlog drains on its own, at the baked cap, with no
  // new publish to kick it.
  const realBus = new EventBus();
  realBus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 300 });
  const timerReceived: unknown[] = [];
  const timerSub = realBus.subscribe('t', (msg) => timerReceived.push(msg.payload), {
    rateLimit: {},
  });
  for (let i = 0; i < 4; i++) realBus.publish('t', i);
  await flush();
  assert.deepEqual(timerReceived, [0, 1]);
  assert.equal(waitingFor(realBus, timerSub.id), 2);
  realBus.setDefaultRateLimit({ maxMessages: 1, perWindowMs: 300 });
  await sleep(450); // past the 300ms window, before a second window could drain one more
  assert.deepEqual(
    timerReceived,
    [0, 1, 2, 3],
    'the armed timer still drains at the baked cap of 2, not the revised 1',
  );
  assert.equal(waitingFor(realBus, timerSub.id), 0);
  timerSub.unsubscribe();
});

test('EB-487: the default never enables rate limiting', async () => {
  const bus = new EventBus();
  bus.setDefaultRateLimit({ maxMessages: 1, perWindowMs: 1000 });
  const received: unknown[] = [];
  // No `rateLimit` opt-in: still one handler call per message.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.deepEqual(bus.getStats().rateLimitedWaiting, []);
  sub.unsubscribe();
});

test('EB-487: setRateLimit still needs a complete object, null clears without the default re-applying, and a later revision is refused', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: {},
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  // A partial revision is still refused: no default fills the gap.
  assert.throws(
    () => bus.setRateLimit(sub.id, { maxMessages: 5 } as never),
    RangeError,
  );
  bus.setRateLimit(sub.id, { maxMessages: 5, perWindowMs: 1000 });
  const events = bus.getSubscriptionAudit({ action: 'rate_limit_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    rateLimit: { maxMessages: 5, perWindowMs: 1000 },
    oldRateLimit: { maxMessages: 2, perWindowMs: 1000 },
  });
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4], 'the fresh 5-wide window drains the backlog');
  clock.advance(1001); // the old window slides out
  for (let i = 5; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  bus.setRateLimit(sub.id, null);
  for (let i = 10; i < 15; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],
    'the cleared subscriber delivers at full speed even though a default is installed',
  );
  assert.throws(
    () => bus.setRateLimit(sub.id, { maxMessages: 2, perWindowMs: 1000 }),
    RangeError,
  );
  bus.publish('t', 15);
  bus.publish('t', 16);
  await flush();
  assert.equal(received.length, 17, 'still full speed after the refused revision');
  sub.unsubscribe();
});

test('EB-487: revising to the current default is a no-op with no audit, and the getter follows the live value and returns a copy', () => {
  const bus = new EventBus();
  bus.setDefaultRateLimit(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_rate_limit_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultRateLimit({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_rate_limit_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultRateLimit(), undefined);
  const opts = { maxMessages: 2, perWindowMs: 1000 };
  bus.setDefaultRateLimit(opts);
  opts.maxMessages = 999;
  assert.deepEqual(
    bus.getDefaultRateLimit(),
    { maxMessages: 2, perWindowMs: 1000 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  assert.equal(bus.getDefaultRateLimit()?.maxMessages, 2);
  const audit = bus.getSubscriptionAudit({ action: 'default_rate_limit_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    rateLimit: { maxMessages: 2, perWindowMs: 1000 },
  });
  const held = bus.getDefaultRateLimit();
  assert.ok(held !== undefined);
  held.maxMessages = 999;
  assert.deepEqual(
    bus.getDefaultRateLimit(),
    { maxMessages: 2, perWindowMs: 1000 },
    'the getter returns a copy',
  );
  bus.setDefaultRateLimit({ maxMessages: 3, perWindowMs: 1000 }, 'ops');
  assert.deepEqual(bus.getDefaultRateLimit(), { maxMessages: 3, perWindowMs: 1000 });
  const revised = bus.getSubscriptionAudit({ action: 'default_rate_limit_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { maxMessages: 2, perWindowMs: 1000 },
    rateLimit: { maxMessages: 3, perWindowMs: 1000 },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultRateLimit(null);
  assert.equal(bus.getDefaultRateLimit(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_rate_limit_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { maxMessages: 3, perWindowMs: 1000 },
    rateLimit: null,
  });
  bus.setDefaultRateLimit(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_rate_limit_revised' }).length,
    3,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-487: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultRateLimit({ maxMessages: 2, perWindowMs: 1000 });
  const bad = [
    0,
    -1,
    '2',
    [],
    true,
    { maxMessages: 0 },
    { maxMessages: -1 },
    { maxMessages: 1.5 },
    { maxMessages: Number.NaN },
    { maxMessages: '2' },
    { perWindowMs: 0 },
    { perWindowMs: -1 },
    { perWindowMs: Number.NaN },
    { perWindowMs: Number.POSITIVE_INFINITY },
    { perWindowMs: '1000' },
    undefined,
  ] as unknown[];
  for (const opts of bad) {
    assert.throws(
      () => bus.setDefaultRateLimit(opts as never),
      RangeError,
      `opts=${String(opts)}`,
    );
    assert.deepEqual(bus.getDefaultRateLimit(), { maxMessages: 2, perWindowMs: 1000 });
  }
  assert.throws(() => bus.setDefaultRateLimit({ maxMessages: 3 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultRateLimit(), { maxMessages: 2, perWindowMs: 1000 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_rate_limit_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});
