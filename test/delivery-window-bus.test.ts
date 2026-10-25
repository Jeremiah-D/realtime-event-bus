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

test('deliveryWindow bus: closed-window publishes hold and flush in order on open', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: weekdayWindow,
  });
  assert.equal(bus.publish('market.btc', 1), 1);
  assert.equal(bus.publish('market.eth', 2), 1);
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 2);
  assert.equal(bus.getStats().deliveryWindow.flushedMessages, 0);

  clock.nowMs = mon(9);
  assert.equal(bus.flushDeliveryWindows(), 2);
  await flush();
  assert.deepEqual(received, [1, 2]);
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 0);
  assert.equal(bus.getStats().deliveryWindow.flushedMessages, 2);
});

test('deliveryWindow bus: the next arrival after opening flushes the hold first', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: weekdayWindow,
  });
  bus.publish('market.btc', 1);
  await flush();
  assert.deepEqual(received, []);
  clock.nowMs = mon(9);
  bus.publish('market.eth', 2);
  await flush();
  assert.deepEqual(received, [1, 2]);
});

test('deliveryWindow bus: in-window publishes deliver immediately', async () => {
  const clock = { nowMs: mon(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: weekdayWindow,
  });
  bus.publish('market.btc', 1);
  await flush();
  assert.deepEqual(received, [1]);
  assert.equal(bus.getStats().deliveryWindow.heldMessages, 0);
});

test('deliveryWindow bus: max hold refuses new arrivals but keeps the old hold', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('market.**', (msg) => received.push(msg.payload), {
    deliveryWindows: { ...weekdayWindow, maxHoldMs: 60_000 },
  });
  assert.equal(bus.publish('market.btc', 1), 1);
  clock.nowMs = sun(10, 2);
  assert.equal(bus.publish('market.eth', 2), 0);
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().deliveryWindow.rejectedMessages, 1);
  assert.equal(bus.getStats().deliveryWindow.overlongMessages, 1);
  clock.nowMs = mon(9);
  assert.equal(bus.flushDeliveryWindows(), 1);
  await flush();
  assert.deepEqual(received, [1]);
});

test('deliveryWindow bus: malformed windows throw at subscribe time', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('a', () => {}, { deliveryWindows: { windows: [{ start: '25:00', end: '17:00' }] } }),
    /HH:MM/,
  );
  assert.throws(
    () => bus.subscribe('a', () => {}, { deliveryWindows: { windows: [{ start: '09:00', end: '17:00', timeZone: 'Nowhere/Here' }] } }),
    /IANA/,
  );
});

test('deliveryWindow bus: reliable deliveries ack after the window flush', async () => {
  const clock = { nowMs: sun(10), now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribeReliable(
    'market.**',
    (delivery) => {
      received.push(delivery.msg.payload);
      delivery.ack();
    },
    { deliveryWindows: weekdayWindow },
  );
  bus.publish('market.btc', 7);
  await flush();
  assert.deepEqual(received, []);
  clock.nowMs = mon(9);
  assert.equal(bus.flushDeliveryWindows(), 1);
  await flush();
  assert.deepEqual(received, [7]);
});
