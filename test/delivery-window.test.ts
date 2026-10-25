import test from 'node:test';
import assert from 'node:assert/strict';
import {
  DeliveryWindowGate,
  isInDeliveryWindow,
  isInDeliveryWindows,
  nextDeliveryWindowOpenMs,
  resolveDeliveryWindow,
  resolveDeliveryWindowOptions,
} from '../src/delivery-window.ts';

// 2026-10-25 is a Sunday; 2026-10-26 is a Monday.
const sun = (h: number, m = 0) => Date.UTC(2026, 9, 25, h, m);
const mon = (h: number, m = 0) => Date.UTC(2026, 9, 26, h, m);
const fri = (h: number, m = 0) => Date.UTC(2026, 9, 23, h, m);
const sat = (h: number, m = 0) => Date.UTC(2026, 9, 24, h, m);

test('deliveryWindow: weekday window is inclusive at start, exclusive at end', () => {
  const w = resolveDeliveryWindow({ start: '09:00', end: '17:00', days: [1, 2, 3, 4, 5] });
  assert.equal(isInDeliveryWindow(w, mon(9)), true);
  assert.equal(isInDeliveryWindow(w, mon(16, 59)), true);
  assert.equal(isInDeliveryWindow(w, mon(17)), false);
  assert.equal(isInDeliveryWindow(w, mon(8, 59)), false);
  assert.equal(isInDeliveryWindow(w, sun(10)), false);
});

test('deliveryWindow: overnight window covers the next morning under the starting day', () => {
  const w = resolveDeliveryWindow({ start: '22:00', end: '02:00', days: [5] });
  assert.equal(isInDeliveryWindow(w, fri(23)), true);
  assert.equal(isInDeliveryWindow(w, sat(1)), true);
  assert.equal(isInDeliveryWindow(w, sat(23)), false);
  assert.equal(isInDeliveryWindow(w, sun(1)), false);
  assert.equal(isInDeliveryWindow(w, fri(2)), false);
});

test('deliveryWindow: time zone shifts the window, and start==end means all day', () => {
  const w = resolveDeliveryWindow({ start: '09:00', end: '17:00', timeZone: 'America/New_York' });
  // 2026-10-26 is in EDT (UTC-4): 13:00 UTC is 09:00 local.
  assert.equal(isInDeliveryWindow(w, mon(13)), true);
  assert.equal(isInDeliveryWindow(w, mon(12)), false);
  const allDay = resolveDeliveryWindow({ start: '00:00', end: '00:00', days: [0] });
  assert.equal(isInDeliveryWindow(allDay, sun(3)), true);
  assert.equal(isInDeliveryWindow(allDay, mon(3)), false);
  assert.equal(isInDeliveryWindows([], mon(3)), true);
});

test('deliveryWindow: malformed windows fail fast', () => {
  assert.throws(() => resolveDeliveryWindow({ start: '9:00', end: '17:00' }), /HH:MM/);
  assert.throws(() => resolveDeliveryWindow({ start: '24:00', end: '17:00' }), /HH:MM/);
  assert.throws(() => resolveDeliveryWindow({ start: '09:00', end: '17:00', timeZone: 'Mars/Olympus' }), /IANA/);
  assert.throws(() => resolveDeliveryWindow({ start: '09:00', end: '17:00', days: [7] }), /0-6/);
  assert.throws(() => resolveDeliveryWindow({ start: '09:00', end: '17:00', days: [] }), /non-empty/);
  assert.throws(() => resolveDeliveryWindowOptions({ windows: [{ start: '09:00', end: '17:00' }], maxHoldMs: -1 }), /maxHoldMs/);
});

test('deliveryWindow: next open is the next window start', () => {
  const opts = resolveDeliveryWindowOptions({ windows: [{ start: '09:00', end: '17:00', days: [1] }] })!;
  assert.equal(nextDeliveryWindowOpenMs(opts.windows, mon(18)), Date.UTC(2026, 10, 2, 9, 0));
  assert.equal(nextDeliveryWindowOpenMs(opts.windows, mon(10)), mon(10));
  assert.equal(nextDeliveryWindowOpenMs([], mon(18)), undefined);
});

test('deliveryWindow gate: holds out-of-window, flushes in order when open', () => {
  const opts = resolveDeliveryWindowOptions({ windows: [{ start: '09:00', end: '17:00', days: [1] }] })!;
  const gate = new DeliveryWindowGate(opts);
  assert.equal(gate.isOpen(sun(10)), false);
  assert.equal(gate.hold({ msg: 'a', expiresAt: undefined, rawPayload: 'a', heldAtMs: sun(10) }, sun(10)), 'held');
  assert.equal(gate.hold({ msg: 'b', expiresAt: undefined, rawPayload: 'b', heldAtMs: sun(11) }, sun(11)), 'held');
  assert.equal(gate.flush(sun(12)).length, 0);
  assert.equal(gate.heldMessages, 2);
  const flushed = gate.flush(mon(9));
  assert.deepEqual(flushed.map((h) => h.msg), ['a', 'b']);
  assert.equal(gate.heldMessages, 0);
  assert.equal(gate.flushedCount, 2);
});

test('deliveryWindow gate: max hold refuses new arrivals once the oldest is overlong', () => {
  const opts = resolveDeliveryWindowOptions({
    windows: [{ start: '09:00', end: '17:00', days: [1] }],
    maxHoldMs: 60_000,
  })!;
  const gate = new DeliveryWindowGate(opts);
  assert.equal(gate.hold({ msg: 'a', expiresAt: undefined, rawPayload: 'a', heldAtMs: sun(10) }, sun(10)), 'held');
  assert.equal(gate.overlongCount(sun(10, 2)), 1);
  assert.equal(gate.hold({ msg: 'b', expiresAt: undefined, rawPayload: 'b', heldAtMs: sun(10, 2) }, sun(10, 2)), 'rejected');
  assert.equal(gate.rejectedCount, 1);
  assert.equal(gate.heldMessages, 1);
  // The stale hold still flushes when the window opens — rejection only
  // stopped new arrivals, it never discarded what was already held.
  assert.deepEqual(gate.flush(mon(9)).map((h) => h.msg), ['a']);
});
