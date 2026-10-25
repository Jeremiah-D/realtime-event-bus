import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-cancelkey-'));

test('cancels only the pending schedules for one topic and one key, in schedule order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const id1 = bus.publishDelayed('t', 'one', { delayMs: 1000, key: 'k' });
  const id2 = bus.publishDelayed('t', 'two', { delayMs: 2000, key: 'k' });
  bus.publishDelayed('t', 'other-key', { delayMs: 1000, key: 'j' });
  bus.publishDelayed('t', 'unkeyed', { delayMs: 1000 });
  bus.publishDelayed('u', 'other-topic', { delayMs: 1000, key: 'k' });
  assert.equal(bus.getStats().pendingDelayed, 5);
  const result = bus.cancelDelayedByKey('t', 'k');
  assert.deepEqual(result, { cancelled: 2, delayIds: [id1, id2] });
  assert.equal(bus.getStats().pendingDelayed, 3);
  // Cancelling one id directly afterwards is a no-op false.
  assert.equal(bus.cancelDelayed(id1 as string), false);
  clock.nowMs = 5_000;
  bus.publish('u', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'other-key', 'unkeyed', 'other-topic']);
});

test('a wildcard pattern expands to every matching concrete topic, still filtered by key', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  const eu = bus.publishDelayed('orders.eu', 'eu', { delayMs: 1000, key: 'k' });
  const us = bus.publishDelayed('orders.us', 'us', { delayMs: 1000, key: 'k' });
  bus.publishDelayed('orders.us', 'other-key', { delayMs: 1000, key: 'j' });
  bus.publishDelayed('billing.eu', 'billing', { delayMs: 1000, key: 'k' });
  const result = bus.cancelDelayedByKey('orders.*', 'k');
  assert.deepEqual(result, { cancelled: 2, delayIds: [eu, us] });
  assert.equal(bus.getStats().pendingDelayed, 2);
  clock.nowMs = 5_000;
  bus.publish('x', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'other-key', 'billing']);
});

test('already fanned out, expired, or cancelled schedules are not counted, and a repeat call is empty', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  // Fanned out: due immediately, flushed before the bulk cancel.
  const fanned = bus.publishDelayed('t', 'gone', { delayMs: 0, key: 'k' });
  await flush();
  // Cancelled individually first.
  const single = bus.publishDelayed('t', 'single', { delayMs: 5000, key: 'k' });
  assert.equal(bus.cancelDelayed(single as string), true);
  const result = bus.cancelDelayedByKey('t', 'k');
  assert.deepEqual(result, { cancelled: 0, delayIds: [] });
  assert.equal(bus.cancelDelayed(fanned as string), false);
  // A repeat of a successful bulk cancel counts each schedule at most once.
  const id = bus.publishDelayed('t', 'again', { delayMs: 5000, key: 'k' });
  assert.deepEqual(bus.cancelDelayedByKey('t', 'k'), { cancelled: 1, delayIds: [id] });
  assert.deepEqual(bus.cancelDelayedByKey('t', 'k'), { cancelled: 0, delayIds: [] });
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('empty result on a topic/key with no pending schedules — never throws', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publishDelayed('t', 'x', { delayMs: 1000, key: 'k' });
  assert.deepEqual(bus.cancelDelayedByKey('nothing-here', 'k'), { cancelled: 0, delayIds: [] });
  assert.deepEqual(bus.cancelDelayedByKey('t', 'no-such-key'), { cancelled: 0, delayIds: [] });
  assert.deepEqual(bus.cancelDelayedByKey('t.*', 'k'), { cancelled: 0, delayIds: [] });
  // The unrelated schedule is untouched.
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('tombstones survive restart: cancelled schedules are not resurrected', () => {
  const dir = freshDir();
  const bus1 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  bus1.publishDelayed('t', 'x', { delayMs: 5000, key: 'k' });
  bus1.publishDelayed('t', 'y', { delayMs: 5000, key: 'k' });
  const result = bus1.cancelDelayedByKey('t', 'k');
  assert.equal(result.cancelled, 2);
  assert.equal(result.delayIds.length, 2);
  // A restarted bus over the same directory rebuilds nothing: every
  // cancellation was tombstoned.
  const bus2 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  assert.equal(bus2.getStats().pendingDelayed, 0);
});

test('keyed schedules release their per-key expectations on bulk cancel, other keys unaffected', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // The delayed schedule draws keySeq 1 for 'k' at schedule time; the
  // live keyed publish draws keySeq 2 and buffers behind it.
  bus.publishDelayed('t', 'first', { delayMs: 5000, key: 'k' });
  bus.publish('t', 'second', { key: 'k' });
  // Another key's delayed schedule is independent of the 'k' cancel.
  bus.publishDelayed('t', 'other', { delayMs: 5000, key: 'j' });
  bus.publish('t', 'other-live', { key: 'j' });
  await flush();
  assert.deepEqual(received, []); // both live messages wait for their keySeq 1
  const result = bus.cancelDelayedByKey('t', 'k');
  assert.equal(result.cancelled, 1);
  await flush();
  // Only the 'k' successor was released; 'j' still waits.
  assert.deepEqual(received, ['second']);
  assert.equal(bus.getStats().pendingDelayed, 1);
});

test('subaudit records delayed_cancelled with cancelled, delayIds, topicPattern and key', () => {
  const bus = new EventBus();
  const id1 = bus.publishDelayed('orders.eu', 'a', { delayMs: 1000, key: 'k' });
  const id2 = bus.publishDelayed('orders.us', 'b', { delayMs: 1000, key: 'k' });
  const result = bus.cancelDelayedByKey('orders.*', 'k');
  assert.equal(result.cancelled, 2);
  const events = bus.getSubscriptionAudit({ action: 'delayed_cancelled' });
  assert.equal(events.length, 1);
  assert.equal(events[0].pattern, 'orders.*');
  assert.equal(events[0].actor, 'system');
  assert.deepEqual(events[0].detail, {
    cancelled: 2,
    delayIds: [id1, id2],
    topicPattern: 'orders.*',
    key: 'k',
  });
  // The empty case is audited too: the operator action is on record even
  // when nothing was pending.
  bus.cancelDelayedByKey('nothing-here', 'k');
  const all = bus.getSubscriptionAudit({ action: 'delayed_cancelled' });
  assert.equal(all.length, 2);
  assert.deepEqual(all[1].detail, { cancelled: 0, delayIds: [], topicPattern: 'nothing-here', key: 'k' });
});

test('cancelDelayedByKey validates its arguments', () => {
  const bus = new EventBus();
  assert.throws(() => bus.cancelDelayedByKey(123 as unknown as string, 'k'), TypeError);
  assert.throws(() => bus.cancelDelayedByKey('', 'k'), RangeError);
  assert.throws(() => bus.cancelDelayedByKey('t', 123 as unknown as string), TypeError);
  assert.throws(() => bus.cancelDelayedByKey('t', ''), RangeError);
  assert.deepEqual(bus.cancelDelayedByKey('t', 'k'), { cancelled: 0, delayIds: [] });
});
