import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type SessionAggregateResult, type SessionWindowOptions } from '../src/bus.ts';

/**
 * Bus-level default session-window aggregation (EB-507):
 * `setDefaultSessionWindow` installs the `gapMs` / `aggregate` /
 * `field` / `maxSessionsPerKey` new opt-in `sessionWindow`
 * subscribers fall back to when they omit a field — the priority
 * chain is subscriber-explicit > bus default > required (a missing
 * `gapMs` / `aggregate` still throws) / 1024 — and the default never
 * enables aggregation: a subscriber that does not opt into
 * `sessionWindow` still receives one message at a time.
 *
 * Session assertions follow `session-window.test.ts`: the bus clock
 * is injected and sessions close on the next arrival past the gap,
 * so the boundaries (`sessionStart` / `sessionEnd` / `count` /
 * `value`) assert exactly with no wall-clock waiting.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A bus with a manually advanced clock: no wall-clock timer can fire a gap close. */
function clockedBus(startMs = 1_000_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return { bus: new EventBus({ now: clock.now }), clock };
}

test('EB-507: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultSessionWindow(), undefined);
  bus.setDefaultSessionWindow(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_session_window_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-507: `sessionWindow: {}` opts in and takes the whole default — one aggregate, exact boundaries', async () => {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'count' });
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), { sessionWindow: {} });
  for (let i = 1; i <= 5; i++) bus.publish('t', i, { key: 'btc' });
  await flush();
  // Raw messages never reach the handler: the burst is still in the open session.
  assert.equal(received.length, 0);
  // The activity gap expires: the next message closes the old session first.
  clock.nowMs += 5000;
  bus.publish('t', 0, { key: 'btc' });
  await flush();
  assert.equal(received.length, 1);
  const agg = received[0];
  assert.equal(agg.count, 5);
  assert.equal(agg.value, 5);
  assert.equal(agg.sessionStart, 1_000_000);
  assert.equal(agg.sessionEnd, 1_000_000);
  assert.equal(bus.getStats().aggregatedSessionWindows, 1);
});

test('EB-507: an explicit gapMs / aggregate / field / maxSessionsPerKey each wins over the bus default', async () => {
  // Explicit gapMs 50 wins over the default 5000: the session closes
  // on the next arrival past 50ms, not 5000ms.
  {
    const { bus, clock } = clockedBus();
    const received: SessionAggregateResult[] = [];
    bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'sum', field: 'price' });
    bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
      sessionWindow: { gapMs: 50, aggregate: 'sum', field: 'price' },
    });
    for (let i = 1; i <= 3; i++) bus.publish('t', { price: i * 10 }, { key: 'k' });
    clock.nowMs += 60;
    bus.publish('t', { price: 1 }, { key: 'k' });
    await flush();
    assert.equal(received.length, 1);
    assert.equal(received[0].count, 3);
    assert.equal(received[0].value, 60);
  }
  // Explicit aggregate 'count' wins over the default 'sum'.
  {
    const { bus, clock } = clockedBus();
    const received: SessionAggregateResult[] = [];
    bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'sum', field: 'price' });
    bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
      sessionWindow: { gapMs: 5000, aggregate: 'count', field: 'price' },
    });
    for (let i = 1; i <= 3; i++) bus.publish('t', { price: i * 10 }, { key: 'k' });
    clock.nowMs += 5000;
    bus.publish('t', { price: 1 }, { key: 'k' });
    await flush();
    assert.equal(received.length, 1);
    assert.equal(received[0].value, 3, 'count folds every message, not the sum');
  }
  // Explicit field 'v' wins over the default 'price'.
  {
    const { bus, clock } = clockedBus();
    const received: SessionAggregateResult[] = [];
    bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'sum', field: 'price' });
    bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
      sessionWindow: { gapMs: 5000, aggregate: 'sum', field: 'v' },
    });
    for (let i = 1; i <= 3; i++) bus.publish('t', { v: i }, { key: 'k' });
    clock.nowMs += 5000;
    bus.publish('t', { v: 99 }, { key: 'k' });
    await flush();
    assert.equal(received.length, 1);
    assert.equal(received[0].value, 6, 'the explicit field v was folded, not price');
  }
  // Explicit maxSessionsPerKey 1 wins over the default 2: the second
  // key evicts the first session, and eviction still delivers it.
  {
    const { bus } = clockedBus();
    const received: SessionAggregateResult[] = [];
    bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'count', maxSessionsPerKey: 2 });
    bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
      sessionWindow: { gapMs: 5000, aggregate: 'count', maxSessionsPerKey: 1 },
    });
    bus.publish('t', 'a', { key: 'k1' });
    bus.publish('t', 'b', { key: 'k2' });
    await flush();
    assert.equal(received.length, 1, 'the k1 session was evicted and delivered');
    assert.equal(received[0].count, 1);
  }
});

test('EB-507: with only a default gapMs, an explicit aggregate takes it — but a missing aggregate still throws', async () => {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  const plain: unknown[] = [];
  bus.setDefaultSessionWindow({ gapMs: 50 });
  // Explicit aggregate + default gapMs: the session closes on the next
  // arrival past 50ms.
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { aggregate: 'sum' },
  });
  bus.publish('t', 10, { key: 'k' });
  bus.publish('t', 20, { key: 'k' });
  clock.nowMs += 50;
  bus.publish('t', 0, { key: 'k' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 2);
  assert.equal(received[0].value, 30);
  // `{}` with only a default gapMs: aggregate is missing from both
  // sides, so subscribe still throws — the default fills in omitted
  // fields, it never satisfies the required ones on its own.
  assert.throws(() => bus.subscribe('u', () => {}, { sessionWindow: {} }), RangeError);
  // No sessionWindow at all: no aggregation, raw messages as before.
  bus.subscribe('v', (m) => plain.push(m.payload));
  bus.publish('v', 'raw');
  await flush();
  assert.deepEqual(plain, ['raw']);
  assert.equal(bus.getStats().aggregatedSessionWindows, 1, 'only the t session aggregated');
});

test('EB-507: the default maxSessionsPerKey covers the built-in 1024 — eviction still delivers the aggregate', async () => {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'count', maxSessionsPerKey: 2 });
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), { sessionWindow: {} });
  bus.publish('t', 'a', { key: 'k1' });
  bus.publish('t', 'b', { key: 'k2' });
  // The third key exceeds the default bound of 2: the least-recently
  // active session (k1) is evicted, and eviction still delivers it.
  bus.publish('t', 'c', { key: 'k3' });
  await flush();
  assert.equal(received.length, 1, 'the k1 session was evicted and delivered');
  assert.equal(received[0].count, 1);
  // k2 and k3 are still open: only k2's gap has expired.
  clock.nowMs += 5000;
  bus.publish('t', 'd', { key: 'k2' });
  await flush();
  assert.equal(received.length, 2);
  assert.equal(received[1].count, 1);
  assert.equal(bus.getStats().aggregatedSessionWindows, 2);
});

test('EB-507: clearing with null returns new opt-in subscribers to the required fields and the built-in 1024', async () => {
  const { bus } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.setDefaultSessionWindow({ gapMs: 50, aggregate: 'count', maxSessionsPerKey: 2 });
  bus.setDefaultSessionWindow(null);
  assert.equal(bus.getDefaultSessionWindow(), undefined);
  assert.throws(() => bus.subscribe('t', () => {}, { sessionWindow: {} }), RangeError);
  const audit = bus.getSubscriptionAudit({ action: 'default_session_window_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { gapMs: 50, aggregate: 'count', maxSessionsPerKey: 2 },
    sessionWindow: null,
  });
  // The bound is back to the built-in 1024: three keys stay open.
  bus.subscribe('u', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 50, aggregate: 'count' },
  });
  bus.publish('u', 'a', { key: 'k1' });
  bus.publish('u', 'b', { key: 'k2' });
  bus.publish('u', 'c', { key: 'k3' });
  await flush();
  assert.equal(received.length, 0, 'no eviction under the built-in 1024');
});

test('EB-507: the default never enables aggregation for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultSessionWindow({ gapMs: 200, aggregate: 'sum', field: 'price' });
  const plain: unknown[] = [];
  bus.subscribe('t', (m) => plain.push(m.payload));
  bus.publish('t', { price: 1 });
  bus.publish('t', { price: 2 });
  await flush();
  assert.deepEqual(plain, [{ price: 1 }, { price: 2 }]);
  assert.equal(bus.getStats().aggregatedSessionWindows, 0);
});

test('EB-507: revising the bus default never moves an already-subscribed session table or its sweep timer', async () => {
  const { bus, clock } = clockedBus();
  const early: SessionAggregateResult[] = [];
  const late: SessionAggregateResult[] = [];
  bus.subscribe('t', (m) => early.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 200, aggregate: 'sum', field: 'price' },
  });
  bus.publish('t', { price: 10 }, { key: 'k' });
  bus.publish('t', { price: 20 }, { key: 'k' });
  bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'count' });
  // The early session keeps the 200ms gap and the sum fold it was
  // baked with: it closes on the next arrival past the old gap.
  clock.nowMs += 250;
  bus.publish('t', { price: 1 }, { key: 'k' });
  await flush();
  assert.equal(early.length, 1);
  assert.equal(early[0].count, 2);
  assert.equal(early[0].value, 30);
  assert.equal(early[0].sessionStart, 1_000_000);
  assert.equal(early[0].sessionEnd, 1_000_000);
  assert.equal(bus.getStats().aggregatedSessionWindows, 1);
  // A subscriber joining after the revision takes the new default.
  bus.subscribe('u', (m) => late.push(m.payload as SessionAggregateResult), { sessionWindow: {} });
  bus.publish('u', { price: 1 }, { key: 'k' });
  clock.nowMs += 250;
  bus.publish('u', { price: 2 }, { key: 'k' });
  await flush();
  assert.equal(late.length, 0, 'the 5000ms default gap is still open');
});

test('EB-507: a default-baked subscriber still takes partial setSessionWindow revisions, and the default never re-applies', async () => {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.setDefaultSessionWindow({ gapMs: 200, aggregate: 'sum', field: 'price' });
  const sub = bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: {},
  });
  // The per-subscriber revision still works on a default-baked
  // subscriber: only the gap moves.
  bus.setSessionWindow(sub.id, { gapMs: 1000 });
  assert.throws(() => bus.setSessionWindow(sub.id, { aggregate: 'count' }), RangeError);
  assert.throws(() => bus.setSessionWindow(sub.id, { field: 'v' }), RangeError);
  // Clearing the default does not re-apply anything to the subscriber.
  bus.setDefaultSessionWindow(null);
  assert.equal(bus.getDefaultSessionWindow(), undefined);
  bus.publish('t', { price: 7 }, { key: 'k' });
  // 400ms in, a 200ms gap would already have closed the session on
  // the next arrival; the revised 1000ms gap keeps it open, so the
  // arrival joins instead of closing.
  clock.nowMs += 400;
  bus.publish('t', { price: 0 }, { key: 'k' });
  await flush();
  assert.equal(received.length, 0);
  // 1000ms past the last activity the session finally closes.
  clock.nowMs += 1000;
  bus.publish('t', { price: 0 }, { key: 'k' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 2);
  assert.equal(received[0].value, 7, 'the baked sum fold survived the default clearing');
  assert.equal(received[0].sessionStart, 1_000_000);
  assert.equal(received[0].sessionEnd, 1_000_400);
});

test('EB-507: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'sum', field: 'price', maxSessionsPerKey: 10 });
  const bad = [
    0,
    -1,
    1.5,
    'x',
    [],
    true,
    { gapMs: 0 },
    { gapMs: -1 },
    { gapMs: Number.NaN },
    { gapMs: Number.POSITIVE_INFINITY },
    { gapMs: '40' },
    { aggregate: 'total' },
    { aggregate: '' },
    { aggregate: 'COUNT' },
    { field: '__proto__' },
    { field: 'a.constructor' },
    { field: 'a..b' },
    { field: '' },
    { field: 5 },
    { maxSessionsPerKey: 0 },
    { maxSessionsPerKey: -1 },
    { maxSessionsPerKey: 1.5 },
    { maxSessionsPerKey: '3' },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultSessionWindow(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultSessionWindow(), {
      gapMs: 5000,
      aggregate: 'sum',
      field: 'price',
      maxSessionsPerKey: 10,
    });
  }
  assert.throws(() => bus.setDefaultSessionWindow({ gapMs: 50 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultSessionWindow(), {
    gapMs: 5000,
    aggregate: 'sum',
    field: 'price',
    maxSessionsPerKey: 10,
  });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_session_window_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-507: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultSessionWindow(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_session_window_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultSessionWindow({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_session_window_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultSessionWindow(), undefined);
  const opts: SessionWindowOptions = {
    gapMs: 5000,
    aggregate: 'sum',
    field: 'price',
    maxSessionsPerKey: 10,
  };
  bus.setDefaultSessionWindow(opts);
  opts.gapMs = 999;
  assert.deepEqual(
    bus.getDefaultSessionWindow(),
    { gapMs: 5000, aggregate: 'sum', field: 'price', maxSessionsPerKey: 10 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'sum', field: 'price', maxSessionsPerKey: 10 });
  const audit = bus.getSubscriptionAudit({ action: 'default_session_window_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    sessionWindow: { gapMs: 5000, aggregate: 'sum', field: 'price', maxSessionsPerKey: 10 },
  });
  const held = bus.getDefaultSessionWindow();
  assert.ok(held !== undefined);
  held.gapMs = 999;
  assert.deepEqual(
    bus.getDefaultSessionWindow(),
    { gapMs: 5000, aggregate: 'sum', field: 'price', maxSessionsPerKey: 10 },
    'the getter returns a copy',
  );
  // A revision moves the live value and audits both sides.
  bus.setDefaultSessionWindow({ gapMs: 1000, aggregate: 'count' }, 'ops');
  assert.deepEqual(bus.getDefaultSessionWindow(), { gapMs: 1000, aggregate: 'count' });
  const revised = bus.getSubscriptionAudit({ action: 'default_session_window_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { gapMs: 5000, aggregate: 'sum', field: 'price', maxSessionsPerKey: 10 },
    sessionWindow: { gapMs: 1000, aggregate: 'count' },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultSessionWindow(null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'default_session_window_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { gapMs: 1000, aggregate: 'count' },
    sessionWindow: null,
  });
});

test('EB-507: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultSessionWindow({ gapMs: 5000, aggregate: 'sum', field: 'price' });
  const snapshot = bus.exportBusConfig();
  assert.ok(!('defaultSessionWindow' in snapshot), 'no defaultSessionWindow key');
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultSessionWindow'),
    'the field name appears nowhere in the exported snapshot',
  );
  assert.deepEqual(bus.getDefaultSessionWindow(), { gapMs: 5000, aggregate: 'sum', field: 'price' });
});
