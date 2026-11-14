import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type WindowAggregateOptions, type WindowAggregateResult } from '../src/bus.ts';

/**
 * Bus-level default tumbling-window aggregation (EB-506):
 * `setDefaultWindow` installs the `windowMs` / `aggregate` / `field`
 * new opt-in `window` subscribers fall back to when they omit a field
 * — the priority chain is subscriber-explicit > bus default >
 * required (a missing `windowMs` / `aggregate` still throws) — and
 * the default never enables aggregation: a subscriber that does not
 * opt into `window` still receives one message at a time.
 *
 * Window assertions follow `window-aggregation.test.ts`: the close
 * timer is real time, so the windows are 200ms with 400ms sleeps —
 * wide margins, because under parallel-suite load a microtask flush
 * can land after a short window already closed (the same flake the
 * 40ms shape shows in `window-aggregation.test.ts`); the bus clock is
 * injected so the window boundaries (`windowEnd - windowStart`)
 * assert exactly.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

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

test('EB-506: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultWindow(), undefined);
  bus.setDefaultWindow(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_window_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-506: `window: {}` opts in and takes the whole default — one aggregate, exact boundaries', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: WindowAggregateResult[] = [];
  bus.setDefaultWindow({ windowMs: 200, aggregate: 'sum', field: 'price' });
  bus.subscribe('prices', (m) => received.push(m.payload as WindowAggregateResult), { window: {} });
  for (let i = 1; i <= 5; i++) bus.publish('prices', { price: i * 10 });
  await flush();
  // Raw messages never reach the handler: the burst is still buffering.
  assert.equal(received.length, 0);
  await sleep(400);
  await flush();
  assert.equal(received.length, 1);
  const agg = received[0];
  assert.equal(agg.count, 5);
  assert.equal(agg.value, 150);
  assert.equal(agg.windowEnd - agg.windowStart, 200);
  assert.equal(bus.getStats().aggregatedWindows, 1);
});

test('EB-506: an explicit windowMs / aggregate / field each wins over the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const byLength: WindowAggregateResult[] = [];
  const byFold: WindowAggregateResult[] = [];
  const byField: WindowAggregateResult[] = [];
  bus.setDefaultWindow({ windowMs: 5000, aggregate: 'sum', field: 'price' });
  // Explicit windowMs 200 wins over the default 5000: the window
  // closes long before the default length.
  bus.subscribe('a', (m) => byLength.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 200, aggregate: 'sum', field: 'price' },
  });
  // Explicit aggregate 'count' wins over the default 'sum'.
  bus.subscribe('b', (m) => byFold.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 200, aggregate: 'count', field: 'price' },
  });
  // Explicit field 'v' wins over the default 'price'.
  bus.subscribe('c', (m) => byField.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 200, aggregate: 'sum', field: 'v' },
  });
  for (let i = 1; i <= 3; i++) {
    bus.publish('a', { price: i * 10 });
    bus.publish('b', { price: i * 10 });
    bus.publish('c', { v: i });
  }
  await sleep(400);
  await flush();
  assert.equal(byLength.length, 1, 'explicit windowMs 200 closed the window');
  assert.equal(byLength[0].count, 3);
  assert.equal(byLength[0].value, 60);
  assert.equal(byFold.length, 1);
  assert.equal(byFold[0].value, 3, 'count folds every message, not the sum');
  assert.equal(byField.length, 1);
  assert.equal(byField[0].value, 6, 'the explicit field v was folded, not price');
});

test('EB-506: with only a default windowMs, an explicit aggregate takes it — but a missing aggregate still throws', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: WindowAggregateResult[] = [];
  const plain: unknown[] = [];
  bus.setDefaultWindow({ windowMs: 200 });
  // Explicit aggregate + default windowMs: the window closes at 200ms.
  bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { aggregate: 'count' },
  });
  for (let i = 1; i <= 3; i++) bus.publish('t', i);
  await sleep(400);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 3);
  assert.equal(received[0].value, 3);
  // `{}` with only a default windowMs: aggregate is missing from both
  // sides, so subscribe still throws — the default fills in omitted
  // fields, it never satisfies the required ones on its own.
  assert.throws(() => bus.subscribe('u', () => {}, { window: {} }), RangeError);
  // No window at all: no aggregation, raw messages as before.
  bus.subscribe('v', (m) => plain.push(m.payload));
  bus.publish('v', 'raw');
  await flush();
  assert.deepEqual(plain, ['raw']);
  assert.equal(bus.getStats().aggregatedWindows, 1, 'only the t window aggregated');
});

test('EB-506: clearing with null returns new opt-in subscribers to the required fields', async () => {
  const bus = new EventBus();
  bus.setDefaultWindow({ windowMs: 200, aggregate: 'sum', field: 'price' });
  bus.setDefaultWindow(null);
  assert.equal(bus.getDefaultWindow(), undefined);
  assert.throws(() => bus.subscribe('t', () => {}, { window: {} }), RangeError);
  const audit = bus.getSubscriptionAudit({ action: 'default_window_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { windowMs: 200, aggregate: 'sum', field: 'price' },
    window: null,
  });
});

test('EB-506: the default never enables aggregation for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultWindow({ windowMs: 200, aggregate: 'sum', field: 'price' });
  const plain: unknown[] = [];
  bus.subscribe('t', (m) => plain.push(m.payload));
  bus.publish('t', { price: 1 });
  bus.publish('t', { price: 2 });
  await flush();
  assert.deepEqual(plain, [{ price: 1 }, { price: 2 }]);
  assert.equal(bus.getStats().aggregatedWindows, 0);
});

test('EB-506: revising the bus default never moves an already-subscribed window or its armed timer', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const early: WindowAggregateResult[] = [];
  const late: WindowAggregateResult[] = [];
  bus.subscribe('t', (m) => early.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 200, aggregate: 'sum', field: 'price' },
  });
  bus.publish('t', { price: 10 });
  bus.publish('t', { price: 20 });
  bus.setDefaultWindow({ windowMs: 5000, aggregate: 'count' });
  // The early window keeps the 200ms length and the sum fold it was
  // baked with: it closes on the old timer, not the new default.
  await sleep(400);
  await flush();
  assert.equal(early.length, 1);
  assert.equal(early[0].count, 2);
  assert.equal(early[0].value, 30);
  assert.equal(early[0].windowEnd - early[0].windowStart, 200);
  // A subscriber joining after the revision takes the new default.
  bus.subscribe('u', (m) => late.push(m.payload as WindowAggregateResult), { window: {} });
  bus.publish('u', { price: 1 });
  await sleep(400);
  await flush();
  assert.equal(late.length, 0, 'the 5000ms default window is still open');
  assert.equal(bus.getStats().aggregatedWindows, 1, 'only the early window closed');
});

test('EB-506: a default-baked subscriber still takes partial setWindow revisions, and the default never re-applies', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: WindowAggregateResult[] = [];
  bus.setDefaultWindow({ windowMs: 200, aggregate: 'sum', field: 'price' });
  const sub = bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: {},
  });
  // The per-subscriber revision still works on a default-baked
  // subscriber: only the length moves.
  bus.setWindow(sub.id, { windowMs: 1000 });
  assert.throws(() => bus.setWindow(sub.id, { aggregate: 'count' }), RangeError);
  assert.throws(() => bus.setWindow(sub.id, { field: 'v' }), RangeError);
  // Clearing the default does not re-apply anything to the subscriber.
  bus.setDefaultWindow(null);
  assert.equal(bus.getDefaultWindow(), undefined);
  bus.publish('t', { price: 7 });
  // 400ms in, a 200ms window would already have closed; the revised
  // 1000ms window is still open.
  await sleep(400);
  await flush();
  assert.equal(received.length, 0);
  await sleep(800);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 1);
  assert.equal(received[0].value, 7, 'the baked sum fold survived the default clearing');
  assert.equal(received[0].windowEnd - received[0].windowStart, 1000);
});

test('EB-506: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultWindow({ windowMs: 200, aggregate: 'sum', field: 'price' });
  const bad = [
    0,
    -1,
    1.5,
    'x',
    [],
    true,
    { windowMs: 0 },
    { windowMs: -1 },
    { windowMs: Number.NaN },
    { windowMs: Number.POSITIVE_INFINITY },
    { windowMs: '40' },
    { aggregate: 'total' },
    { aggregate: '' },
    { aggregate: 'COUNT' },
    { field: '__proto__' },
    { field: 'a.constructor' },
    { field: 'a..b' },
    { field: '' },
    { field: 5 },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultWindow(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultWindow(), { windowMs: 200, aggregate: 'sum', field: 'price' });
  }
  assert.throws(() => bus.setDefaultWindow({ windowMs: 50 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultWindow(), { windowMs: 200, aggregate: 'sum', field: 'price' });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_window_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-506: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultWindow(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_window_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultWindow({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_window_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultWindow(), undefined);
  const opts: WindowAggregateOptions = { windowMs: 200, aggregate: 'sum', field: 'price' };
  bus.setDefaultWindow(opts);
  opts.windowMs = 999;
  assert.deepEqual(
    bus.getDefaultWindow(),
    { windowMs: 200, aggregate: 'sum', field: 'price' },
    'the setter stores a defensive copy',
  );
  bus.setDefaultWindow({ windowMs: 200, aggregate: 'sum', field: 'price' });
  const audit = bus.getSubscriptionAudit({ action: 'default_window_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    window: { windowMs: 200, aggregate: 'sum', field: 'price' },
  });
  const held = bus.getDefaultWindow();
  assert.ok(held !== undefined);
  held.windowMs = 999;
  assert.deepEqual(
    bus.getDefaultWindow(),
    { windowMs: 200, aggregate: 'sum', field: 'price' },
    'the getter returns a copy',
  );
  // A revision moves the live value and audits both sides.
  bus.setDefaultWindow({ windowMs: 1000, aggregate: 'count' }, 'ops');
  assert.deepEqual(bus.getDefaultWindow(), { windowMs: 1000, aggregate: 'count' });
  const revised = bus.getSubscriptionAudit({ action: 'default_window_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { windowMs: 200, aggregate: 'sum', field: 'price' },
    window: { windowMs: 1000, aggregate: 'count' },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultWindow(null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'default_window_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { windowMs: 1000, aggregate: 'count' },
    window: null,
  });
});

test('EB-506: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultWindow({ windowMs: 200, aggregate: 'sum', field: 'price' });
  const snapshot = bus.exportBusConfig();
  assert.ok(!('defaultWindow' in snapshot), 'no defaultWindow key');
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultWindow'),
    'the field name appears nowhere in the exported snapshot',
  );
  assert.deepEqual(bus.getDefaultWindow(), { windowMs: 200, aggregate: 'sum', field: 'price' });
});
