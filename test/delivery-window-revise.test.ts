import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

// 2026-10-25 is a Sunday; 2026-10-26 is a Monday.
const sun = (h: number, m = 0) => Date.UTC(2026, 9, 25, h, m);
const mon = (h: number, m = 0) => Date.UTC(2026, 9, 26, h, m);
const weekdayWindow = {
  windows: [{ start: '09:00', end: '17:00', days: [1, 2, 3, 4, 5] }],
};

test('setDeliveryWindow revises to an open window and flushes the hold immediately', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const plain: unknown[] = [];
  const sub = bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: weekdayWindow,
  });
  bus.subscribe('market.**', (msg) => plain.push(msg.payload));
  bus.publish('market.btc', 1);
  bus.publish('market.eth', 2);
  await flush();
  assert.deepEqual(received, []);
  assert.deepEqual(plain, [1, 2]);

  // Sunday all-day: start == end means the whole listed day.
  bus.setDeliveryWindow(sub.id, { windows: [{ start: '00:00', end: '00:00', days: [0] }] });
  await flush();
  assert.deepEqual(received, [1, 2], 'held messages must move to the new gate and flush at once');
  bus.publish('market.sol', 3);
  await flush();
  assert.deepEqual(received, [1, 2, 3], 'the new window governs later publishes');

  const events = bus.getSubscriptionAudit({ action: 'delivery_window_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, {
    deliveryWindows: {
      windows: [{ startMin: 0, endMin: 0, days: [0], timeZone: 'UTC' }],
    },
    oldDeliveryWindows: {
      windows: [{ startMin: 540, endMin: 1020, days: [1, 2, 3, 4, 5], timeZone: 'UTC' }],
    },
  });
});

test('setDeliveryWindow to a still-closed window keeps the hold in the new gate', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: weekdayWindow,
  });
  bus.publish('market.btc', 1);
  await flush();
  assert.deepEqual(received, []);

  bus.setDeliveryWindow(sub.id, { windows: [{ start: '09:00', end: '17:00', days: [1] }] });
  await flush();
  assert.deepEqual(received, [], 'the new window is still closed on Sunday');
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 1);

  clock.nowMs = mon(9);
  assert.equal(bus.flushDeliveryWindows(), 1);
  await flush();
  assert.deepEqual(received, [1]);
});

test('setDeliveryWindow with null clears the windows and releases the hold at once', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: weekdayWindow,
  });
  bus.publish('market.btc', 1);
  await flush();
  assert.deepEqual(received, []);

  bus.setDeliveryWindow(sub.id, null);
  await flush();
  assert.deepEqual(received, [1], 'clearing must release the hold, not strand it');
  bus.publish('market.eth', 2);
  await flush();
  assert.deepEqual(received, [1, 2], 'cleared means all-day delivery');

  const events = bus.getSubscriptionAudit({ action: 'delivery_window_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].detail.deliveryWindows, null);
  assert.throws(() => bus.setDeliveryWindow(sub.id, weekdayWindow), RangeError);
});

test('setDeliveryWindow leaves already-delivered messages alone when closing the window', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: { windows: [{ start: '00:00', end: '00:00', days: [0] }] },
  });
  bus.publish('market.btc', 'early');
  await flush();
  assert.deepEqual(received, ['early']);

  bus.setDeliveryWindow(sub.id, weekdayWindow);
  await flush();
  assert.deepEqual(received, ['early'], 'a revision never recalls a delivered message');
  bus.publish('market.eth', 'late');
  await flush();
  assert.deepEqual(received, ['early'], 'later publishes wait behind the new closed window');
});

test('setDeliveryWindow carries maxHoldMs into the audit detail', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { deliveryWindows: weekdayWindow });
  bus.setDeliveryWindow(sub.id, {
    windows: [{ start: '09:00', end: '17:00', days: [1] }],
    maxHoldMs: 5000,
  });
  const events = bus.getSubscriptionAudit({ action: 'delivery_window_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail.deliveryWindows, {
    windows: [{ startMin: 540, endMin: 1020, days: [1], timeZone: 'UTC' }],
    maxHoldMs: 5000,
  });
});

test('setDeliveryWindow rejects unknown, windowless, and illegal revisions fail-closed', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: weekdayWindow,
  });
  const plain = bus.subscribe('plain', () => {});
  bus.publish('market.btc', 1);
  await flush();
  assert.deepEqual(received, []);

  assert.throws(() => bus.setDeliveryWindow('', weekdayWindow), RangeError);
  assert.throws(() => bus.setDeliveryWindow('no-such-sub', weekdayWindow), RangeError);
  assert.throws(() => bus.setDeliveryWindow(plain.id, weekdayWindow), RangeError);
  assert.throws(() => bus.setDeliveryWindow(plain.id, null), RangeError);
  assert.throws(
    () => bus.setDeliveryWindow(sub.id, { windows: [{ start: '25:00', end: '17:00' }] }),
    /HH:MM/,
  );
  assert.throws(
    () => bus.setDeliveryWindow(sub.id, { windows: [{ start: '09:00', end: '17:00', timeZone: 'Mars/Olympus' }] }),
    /IANA/,
  );
  assert.throws(
    () => bus.setDeliveryWindow(sub.id, undefined as unknown as null),
    RangeError,
  );
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'delivery_window_revised' }), []);

  // The failed revisions left the original gate intact: the held
  // message still flushes when the original window opens.
  clock.nowMs = mon(9);
  assert.equal(bus.flushDeliveryWindows(), 1);
  await flush();
  assert.deepEqual(received, [1]);
});
