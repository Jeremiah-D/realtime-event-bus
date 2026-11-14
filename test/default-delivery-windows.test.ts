import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type DeliveryWindowOptions } from '../src/bus.ts';

/**
 * Bus-level default delivery windows (EB-509):
 * `setDefaultDeliveryWindows` installs the `windows` / `maxHoldMs`
 * new opt-in `deliveryWindows` subscribers fall back to when they
 * omit a field — the priority chain is subscriber-explicit > bus
 * default > required (a missing `windows` still throws) — and the
 * default never enables windowing: a subscriber that does not opt
 * into `deliveryWindows` still receives every message at once.
 *
 * Hold/flush assertions follow `delivery-window-bus.test.ts`: the
 * bus clock is injected, windows are 09:00–18:00 UTC, and a message
 * published while the window is closed holds until the window opens
 * (or `flushDeliveryWindows()` is called), so the boundaries assert
 * exactly with no wall-clock waiting.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

// 2026-10-25 is a Sunday; windows default to every weekday.
const at = (h: number, m = 0, s = 0) => Date.UTC(2026, 9, 25, h, m, s);

/** A bus with a manually advanced clock: no wall-clock timer can fire a flush. */
function clockedBus(startMs = at(8)) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return { bus: new EventBus({ now: clock.now }), clock };
}

const businessHours: DeliveryWindowOptions = {
  windows: [{ start: '09:00', end: '18:00' }],
  maxHoldMs: 60000,
};

test('EB-509: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDeliveryWindows(), undefined);
  bus.setDefaultDeliveryWindows(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_windows_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-509: `deliveryWindows: {}` opts in and takes the whole default — closed-window publishes hold, open-window flushes', async () => {
  const { bus, clock } = clockedBus();
  const received: unknown[] = [];
  bus.setDefaultDeliveryWindows(businessHours);
  bus.subscribe('t', (m) => received.push(m.payload), { deliveryWindows: {} });
  assert.equal(bus.publish('t', 1), 1);
  assert.equal(bus.publish('t', 2), 1);
  await flush();
  assert.deepEqual(received, [], '08:00 is outside the 09:00–18:00 default window');
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 2);
  assert.equal(bus.getStats().deliveryWindow.flushedMessages, 0);

  clock.nowMs = at(9);
  assert.equal(bus.flushDeliveryWindows(), 2);
  await flush();
  assert.deepEqual(received, [1, 2]);
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 0);
  assert.equal(bus.getStats().deliveryWindow.flushedMessages, 2);
});

test('EB-509: an explicit windows list wins over the default windows', async () => {
  const { bus, clock } = clockedBus(at(8, 30));
  const received: unknown[] = [];
  const held: unknown[] = [];
  bus.setDefaultDeliveryWindows(businessHours);
  // Explicit 08:00–09:00 is open at 08:30 while the default is closed.
  bus.subscribe('t', (m) => received.push(m.payload), {
    deliveryWindows: { windows: [{ start: '08:00', end: '09:00' }] },
  });
  // Control subscriber: `{}` takes the default and holds at 08:30.
  bus.subscribe('u', (m) => held.push(m.payload), { deliveryWindows: {} });
  bus.publish('t', 1);
  bus.publish('u', 2);
  await flush();
  assert.deepEqual(received, [1], 'the explicit window was open');
  assert.deepEqual(held, [], 'the default window was still closed');
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 1);
  clock.nowMs = at(9);
  assert.equal(bus.flushDeliveryWindows(), 1);
  await flush();
  assert.deepEqual(held, [2]);
});

test('EB-509: an explicit maxHoldMs wins over the default maxHoldMs, including an explicit 0', async () => {
  // Explicit 120000 wins over the default 60000: 61s after the first
  // hold the second arrival is still admitted.
  {
    const { bus, clock } = clockedBus();
    const received: unknown[] = [];
    bus.setDefaultDeliveryWindows(businessHours);
    bus.subscribe('t', (m) => received.push(m.payload), {
      deliveryWindows: { windows: [{ start: '09:00', end: '18:00' }], maxHoldMs: 120000 },
    });
    assert.equal(bus.publish('t', 1), 1);
    clock.nowMs = at(8, 1, 1);
    assert.equal(bus.publish('t', 2), 1, '61000ms is under the explicit 120000, over the default 60000');
    await flush();
    assert.deepEqual(received, []);
    assert.equal(bus.getStats().deliveryWindow.heldMessages, 2);
  }
  // Explicit 0 wins over the default 60000: any wait at all refuses
  // the next arrival.
  {
    const { bus, clock } = clockedBus();
    bus.setDefaultDeliveryWindows(businessHours);
    bus.subscribe('t', () => {}, {
      deliveryWindows: { windows: [{ start: '09:00', end: '18:00' }], maxHoldMs: 0 },
    });
    assert.equal(bus.publish('t', 1), 1);
    clock.nowMs = at(8, 2);
    assert.equal(bus.publish('t', 2), 0, '120000ms is over the explicit 0');
    await flush();
    assert.equal(bus.getStats().deliveryWindow.rejectedMessages, 1);
    assert.equal(bus.getStats().deliveryWindow.heldMessages, 1);
  }
});

test('EB-509: an explicit `windows: []` wins over the default windows (all-day delivery)', async () => {
  const { bus } = clockedBus();
  const received: unknown[] = [];
  bus.setDefaultDeliveryWindows(businessHours);
  bus.subscribe('t', (m) => received.push(m.payload), { deliveryWindows: { windows: [] } });
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(received, [1, 2], 'an empty list means all-day delivery, even with a default installed');
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 0);
});

test('EB-509: with only default windows, an explicit maxHoldMs combines with them', async () => {
  const { bus, clock } = clockedBus();
  bus.setDefaultDeliveryWindows({ windows: [{ start: '09:00', end: '18:00' }] });
  bus.subscribe('t', () => {}, { deliveryWindows: { maxHoldMs: 60000 } });
  assert.equal(bus.publish('t', 1), 1);
  await flush();
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 1, 'the windows came from the default');
  clock.nowMs = at(8, 1, 1);
  assert.equal(bus.publish('t', 2), 0, '61000ms is over the explicit 60000');
  assert.equal(bus.getStats().deliveryWindow.rejectedMessages, 1);
});

test('EB-509: clearing with null returns new opt-in subscribers to windows being required', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryWindows(businessHours);
  bus.setDefaultDeliveryWindows(null);
  assert.equal(bus.getDefaultDeliveryWindows(), undefined);
  assert.throws(() => bus.subscribe('t', () => {}, { deliveryWindows: {} }), RangeError);
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_windows_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: businessHours,
    deliveryWindows: null,
  });
});

test('EB-509: the default never enables windowing for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryWindows(businessHours);
  const plain: unknown[] = [];
  bus.subscribe('t', (m) => plain.push(m.payload));
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(plain, [1, 2]);
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 0);
});

test('EB-509: revising the bus default never moves an already-subscribed gate or its held messages’ heldAtMs', async () => {
  const { bus, clock } = clockedBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload), {
    deliveryWindows: { windows: [{ start: '09:00', end: '18:00' }], maxHoldMs: 60000 },
  });
  assert.equal(bus.publish('t', 1), 1);
  await flush();
  assert.deepEqual(received, []);
  // 30s in, revise the default to a narrower window with no hold slack.
  clock.nowMs = at(8, 0, 30);
  bus.setDefaultDeliveryWindows({ windows: [{ start: '10:00', end: '11:00' }], maxHoldMs: 0 });
  // 61s after the original arrival (31s after the revision): the
  // second arrival is refused, so the gate still runs the baked
  // 60000ms bound against the original 08:00 heldAtMs — neither the
  // default's 0 nor a reset clock moved it.
  clock.nowMs = at(8, 1, 1);
  assert.equal(bus.publish('t', 2), 0);
  assert.equal(bus.getStats().deliveryWindow.rejectedMessages, 1);
  // 09:00 opens the baked window, not the revised 10:00–11:00 one.
  clock.nowMs = at(9);
  assert.equal(bus.flushDeliveryWindows(), 1);
  await flush();
  assert.deepEqual(received, [1]);
});

test('EB-509: a default-baked subscriber still takes whole-object setDeliveryWindow revisions, and clearing never re-applies the default', async () => {
  const { bus, clock } = clockedBus();
  const received: unknown[] = [];
  bus.setDefaultDeliveryWindows(businessHours);
  const sub = bus.subscribe('t', (m) => received.push(m.payload), { deliveryWindows: {} });
  assert.equal(bus.publish('t', 1), 1);
  await flush();
  assert.deepEqual(received, [], '08:00 is outside the default window');
  // The per-subscriber revision is a whole-object replace that does
  // not consult the default: the new 08:00–09:00 window is already
  // open, so the held message flushes at once.
  bus.setDeliveryWindow(sub.id, { windows: [{ start: '08:00', end: '09:00' }] });
  await flush();
  assert.deepEqual(received, [1]);
  // `{}` at the per-subscriber level still throws: it never takes
  // the bus default either.
  assert.throws(() => bus.setDeliveryWindow(sub.id, {} as never), RangeError);
  // Clearing the subscriber's windows releases it to all-day
  // delivery — the bus default does not re-apply.
  bus.setDeliveryWindow(sub.id, null);
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(received, [1, 2]);
  // Clearing the bus default does not move the subscriber either.
  bus.setDefaultDeliveryWindows(null);
  clock.nowMs = at(8, 30);
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(received, [1, 2, 3]);
});

test('EB-509: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryWindows(businessHours);
  const bad = [
    0,
    -1,
    1.5,
    'x',
    [],
    true,
    undefined,
    {},
    { windows: 'x' },
    { windows: [{ start: '25:00', end: '18:00' }] },
    { windows: [{ start: '9:00', end: '18:00' }] },
    { windows: [{ start: '09:00', end: '18:60' }] },
    { windows: [{}] },
    { windows: [{ start: '09:00' }] },
    { windows: [{ start: '09:00', end: '18:00', days: [7] }] },
    { windows: [{ start: '09:00', end: '18:00', days: [-1] }] },
    { windows: [{ start: '09:00', end: '18:00', days: [] }] },
    { windows: [{ start: '09:00', end: '18:00', days: 'x' }] },
    { windows: [{ start: '09:00', end: '18:00', timeZone: 'Nowhere/Here' }] },
    { windows: [{ start: '09:00', end: '18:00', timeZone: '' }] },
    { windows: [{ start: '09:00', end: '18:00' }], maxHoldMs: -1 },
    { windows: [{ start: '09:00', end: '18:00' }], maxHoldMs: Number.NaN },
    { windows: [{ start: '09:00', end: '18:00' }], maxHoldMs: Number.POSITIVE_INFINITY },
    { windows: [{ start: '09:00', end: '18:00' }], maxHoldMs: '60000' },
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultDeliveryWindows(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultDeliveryWindows(), businessHours);
  }
  assert.throws(() => bus.setDefaultDeliveryWindows(businessHours, ''), RangeError);
  assert.deepEqual(bus.getDefaultDeliveryWindows(), businessHours);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_windows_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-509: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryWindows(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_windows_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  const opts: DeliveryWindowOptions = {
    windows: [{ start: '09:00', end: '18:00', days: [1, 2, 3, 4, 5], timeZone: 'UTC' }],
    maxHoldMs: 60000,
  };
  bus.setDefaultDeliveryWindows(opts, 'ops');
  // Mutating the caller's object must not move the live default.
  opts.windows.push({ start: '20:00', end: '21:00' });
  opts.maxHoldMs = 1;
  assert.deepEqual(bus.getDefaultDeliveryWindows(), {
    windows: [{ start: '09:00', end: '18:00', days: [1, 2, 3, 4, 5], timeZone: 'UTC' }],
    maxHoldMs: 60000,
  });
  // Re-affirming the same value — even with the days in a different
  // order — audits nothing.
  bus.setDefaultDeliveryWindows({
    windows: [{ start: '09:00', end: '18:00', days: [5, 4, 3, 2, 1], timeZone: 'UTC' }],
    maxHoldMs: 60000,
  });
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_windows_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    deliveryWindows: {
      windows: [{ start: '09:00', end: '18:00', days: [1, 2, 3, 4, 5], timeZone: 'UTC' }],
      maxHoldMs: 60000,
    },
  });
  assert.equal(audit[0].actor, 'ops');
  // Mutating the getter's windows array (or a window inside it) must
  // not move the live default.
  const held = bus.getDefaultDeliveryWindows();
  assert.ok(held !== undefined);
  held.windows.push({ start: '20:00', end: '21:00' });
  held.windows[0].start = '00:00';
  assert.deepEqual(bus.getDefaultDeliveryWindows(), {
    windows: [{ start: '09:00', end: '18:00', days: [1, 2, 3, 4, 5], timeZone: 'UTC' }],
    maxHoldMs: 60000,
  });
  // A revision moves the live value and audits both sides.
  bus.setDefaultDeliveryWindows({ windows: [{ start: '10:00', end: '12:00' }] }, 'ops');
  assert.deepEqual(bus.getDefaultDeliveryWindows(), { windows: [{ start: '10:00', end: '12:00' }] });
  const revised = bus.getSubscriptionAudit({ action: 'default_delivery_windows_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: {
      windows: [{ start: '09:00', end: '18:00', days: [1, 2, 3, 4, 5], timeZone: 'UTC' }],
      maxHoldMs: 60000,
    },
    deliveryWindows: { windows: [{ start: '10:00', end: '12:00' }] },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultDeliveryWindows(null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'default_delivery_windows_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { windows: [{ start: '10:00', end: '12:00' }] },
    deliveryWindows: null,
  });
});

test('EB-509: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryWindows(businessHours);
  const snapshot = bus.exportBusConfig();
  assert.ok(!('defaultDeliveryWindows' in snapshot), 'no defaultDeliveryWindows key');
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultDeliveryWindows'),
    'the field name appears nowhere in the exported snapshot',
  );
  assert.deepEqual(bus.getDefaultDeliveryWindows(), businessHours);
});
