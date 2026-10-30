import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type WindowAggregateResult } from '../src/bus.ts';

/**
 * Tumbling-window length revision (EB-293): `setWindow` retunes a
 * windowed subscriber's `windowMs` without re-subscribing — only
 * `window.windowMs` moves, the aggregate fold, the extracted field,
 * and the open window (its `windowStart` / `windowEnd` / accumulators
 * / armed timer) are untouched, and the new length governs from the
 * next window opening (which reads the live value). An open window
 * still closes on its old `windowEnd`; shortening the length never
 * cuts it early and lengthening it never extends it.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('setWindow rejects unknown, empty, non-windowed, and invalid revisions fail-closed', async () => {
  let nowMs = 1_700_000_000_000;
  const bus = new EventBus({ now: () => nowMs });
  const received: WindowAggregateResult[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 40, aggregate: 'sum', field: 'price' },
  });
  const plain = bus.subscribe('t2', () => {});
  for (const bad of [
    () => bus.setWindow('no-such-sub', { windowMs: 100 }),
    () => bus.setWindow('', { windowMs: 100 }),
    () => bus.setWindow(plain.id, { windowMs: 100 }),
    () => bus.setWindow(sub.id, { windowMs: 0 }),
    () => bus.setWindow(sub.id, { windowMs: -1 }),
    () => bus.setWindow(sub.id, { windowMs: Number.NaN }),
    () => bus.setWindow(sub.id, { windowMs: Number.POSITIVE_INFINITY }),
    () => bus.setWindow(sub.id, { windowMs: '100' as never }),
    () => bus.setWindow(sub.id, { windowMs: undefined as never, aggregate: 'count' }),
    () => bus.setWindow(sub.id, null as never),
    () => bus.setWindow(sub.id, 'x' as never),
    // A differing fold or field would reinterpret the messages already
    // folded into the open window (EB-280 precedent).
    () => bus.setWindow(sub.id, { windowMs: 100, aggregate: 'count' }),
    () => bus.setWindow(sub.id, { windowMs: 100, field: 'other' }),
    () => bus.setWindow(sub.id, { windowMs: 100, field: '__proto__' }),
    () => bus.setWindow(sub.id, { windowMs: 100, field: '' }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'window_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live length is untouched: the first window still spans the
  // original 40ms and still folds the `price` field.
  bus.publish('t', { price: 10 });
  nowMs += 50;
  bus.publish('t', { price: 20 });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 1);
  assert.equal(received[0].value, 10);
  assert.equal(received[0].windowEnd - received[0].windowStart, 40);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('the new length governs from the next window; the open window keeps its old end', async () => {
  let nowMs = 1_700_000_000_000;
  const bus = new EventBus({ now: () => nowMs });
  const received: WindowAggregateResult[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 40, aggregate: 'sum', field: 'price' },
  });
  bus.publish('t', { price: 10 });
  bus.setWindow(sub.id, { windowMs: 200 });
  const audit = bus.getSubscriptionAudit({ action: 'window_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, 't');
  assert.deepEqual(audit[0].detail, { windowMs: 200, oldWindowMs: 40 });
  // Past the old window's end (t0 + 40) but inside the new length:
  // the first window closes on its old end and the next window opens
  // with the new length.
  nowMs += 60;
  bus.publish('t', { price: 20 });
  nowMs += 100;
  bus.publish('t', { price: 30 });
  // Still inside the new window [t0 + 60, t0 + 260): no second close.
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 1);
  assert.equal(received[0].value, 10);
  assert.equal(received[0].windowEnd - received[0].windowStart, 40);
  // Past the new window's end: it closes spanning the new length,
  // still folding the same `price` field.
  nowMs += 150;
  bus.publish('t', { price: 40 });
  await flush();
  assert.equal(received.length, 2);
  assert.equal(received[1].count, 2);
  assert.equal(received[1].value, 50);
  assert.equal(received[1].windowEnd - received[1].windowStart, 200);
  assert.equal(bus.getStats().aggregatedWindows, 2);
  sub.unsubscribe();
});

test('shortening the length never cuts the open window early', async () => {
  let nowMs = 1_700_000_000_000;
  const bus = new EventBus({ now: () => nowMs });
  const received: WindowAggregateResult[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 200, aggregate: 'sum', field: 'price' },
  });
  bus.publish('t', { price: 10 });
  bus.setWindow(sub.id, { windowMs: 40 });
  // Past the new length (40ms) but inside the old window's end
  // (t0 + 200): the open window must not close early.
  nowMs += 100;
  bus.publish('t', { price: 20 });
  await flush();
  assert.equal(received.length, 0, 'the open window still closes on its old windowEnd');
  // Past the old end: it closes spanning the old length, both folded
  // messages intact.
  nowMs += 150;
  bus.publish('t', { price: 30 });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 2);
  assert.equal(received[0].value, 30);
  assert.equal(received[0].windowEnd - received[0].windowStart, 200);
  sub.unsubscribe();
});

test('re-affirming the current length is a no-op with no audit; restating the fold and field is accepted', async () => {
  let nowMs = 1_700_000_000_000;
  const bus = new EventBus({ now: () => nowMs });
  const received: WindowAggregateResult[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as WindowAggregateResult), {
    window: { windowMs: 40, aggregate: 'sum', field: 'price' },
  });
  bus.setWindow(sub.id, { windowMs: 40 });
  bus.setWindow(sub.id, {});
  bus.setWindow(sub.id, { windowMs: 40, aggregate: 'sum', field: 'price' });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'window_revised' }).length,
    0,
    'a same-length revision must not audit',
  );
  // Restating the fold and field alongside a real change does not
  // block the revision.
  bus.setWindow(sub.id, { windowMs: 200, aggregate: 'sum', field: 'price' });
  const audit = bus.getSubscriptionAudit({ action: 'window_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { windowMs: 200, oldWindowMs: 40 });
  // The new length governs from the next window.
  bus.publish('t', { price: 10 });
  nowMs += 250;
  bus.publish('t', { price: 20 });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 1);
  assert.equal(received[0].value, 10);
  assert.equal(received[0].windowEnd - received[0].windowStart, 200);
  sub.unsubscribe();
});
