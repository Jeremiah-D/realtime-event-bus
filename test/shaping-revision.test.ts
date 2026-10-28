import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

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

test('setDeliveryShaping raises the rate from the next refill without refilling the bucket', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 1 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0], 'the original burst delivers one message');
  assert.equal(bus.pendingCount(sub.id), 4);

  bus.setDeliveryShaping(sub.id, { messagesPerSec: 100, burst: 10 });
  // A raised burst does not refill the bucket: the stored budget is
  // still the 0 tokens left after the first delivery, so nothing new
  // can go out until the new rate refills it.
  bus.publish('t', 5);
  await flush();
  assert.deepEqual(received, [0], 'retuning keeps the stored budget, it does not top it up');
  assert.equal(bus.pendingCount(sub.id), 5);

  clock.advance(100); // at 100/s, one window banks 10 tokens (the new burst cap)
  bus.publish('t', 6);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5, 6], 'the new rate drains the backlog in order');
  assert.equal(bus.pendingCount(sub.id), 0);

  const events = bus.getSubscriptionAudit({ action: 'delivery_shaping_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, {
    deliveryShaping: { messagesPerSec: 100, burst: 10 },
    oldDeliveryShaping: { messagesPerSec: 10, burst: 1 },
  });
  sub.unsubscribe();
});

test('setDeliveryShaping caps stored tokens at a lowered burst and keeps a partial rate', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 5 },
  });
  bus.publish('t', 0);
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(received, [0, 1], 'the original burst covers both messages');
  // 3 tokens banked. Lowering the burst to 1 caps the stored budget
  // at 1 instead of leaving 3 spendable tokens behind.
  bus.setDeliveryShaping(sub.id, { burst: 1 });
  bus.publish('t', 2);
  bus.publish('t', 3);
  bus.publish('t', 4);
  await flush();
  assert.deepEqual(received, [0, 1, 2], 'only the capped token is spendable after the revision');
  assert.equal(bus.pendingCount(sub.id), 2);

  // The partial revision kept the old rate: one window at 10/s banks
  // exactly one more token, not a burst's worth at a new rate.
  clock.advance(100);
  bus.publish('t', 5);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3], 'the kept rate refills one token per window');
  assert.equal(bus.pendingCount(sub.id), 2);

  const events = bus.getSubscriptionAudit({ action: 'delivery_shaping_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    deliveryShaping: { messagesPerSec: 10, burst: 1 },
    oldDeliveryShaping: { messagesPerSec: 10, burst: 5 },
  });
  sub.unsubscribe();
});

test('setDeliveryShaping fails closed for unknown, unshaped, undefined, and illegal values', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const shaped = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 2 },
  });
  const plain = bus.subscribe('t', () => {});
  assert.throws(() => bus.setDeliveryShaping('no-such-sub', { messagesPerSec: 20 }), RangeError);
  assert.throws(() => bus.setDeliveryShaping(plain.id, { messagesPerSec: 20 }), RangeError);
  assert.throws(() => bus.setDeliveryShaping('', { messagesPerSec: 20 }), RangeError);
  assert.throws(() => bus.setDeliveryShaping(shaped.id, undefined as never), RangeError);
  assert.throws(() => bus.setDeliveryShaping(shaped.id, 'fast' as never), RangeError);
  assert.throws(() => bus.setDeliveryShaping(shaped.id, true as never), RangeError);
  assert.throws(() => bus.setDeliveryShaping(shaped.id, [] as never), RangeError);
  for (const bad of [
    { messagesPerSec: 0 },
    { messagesPerSec: -5 },
    { messagesPerSec: Infinity },
    { messagesPerSec: NaN },
    { messagesPerSec: 10, burst: 0 },
    { messagesPerSec: 10, burst: -2 },
    { messagesPerSec: 10, burst: Infinity },
    { messagesPerSec: 10, burst: NaN },
  ]) {
    assert.throws(() => bus.setDeliveryShaping(shaped.id, bad), RangeError);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'delivery_shaping_revised' }).length, 0);

  // Every failure left the original rate untouched.
  bus.publish('t', 0);
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(received, [0, 1], 'the original burst of 2 still paces delivery');
  assert.equal(bus.pendingCount(shaped.id), 1);
  shaped.unsubscribe();
  plain.unsubscribe();
});

test('setDeliveryShaping with null clears shaping and a later revision is refused', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 1 },
  });
  bus.publish('t', 0);
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(received, [0]);
  assert.equal(bus.pendingCount(sub.id), 1);

  bus.setDeliveryShaping(sub.id, null);
  await flush();
  assert.deepEqual(received, [0, 1], 'clearing drains the shaped backlog unshaped');
  assert.equal(bus.pendingCount(sub.id), 0);
  bus.publish('t', 2);
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3], 'later messages deliver immediately once shaping is gone');

  assert.throws(() => bus.setDeliveryShaping(sub.id, { messagesPerSec: 20 }), RangeError);
  const events = bus.getSubscriptionAudit({ action: 'delivery_shaping_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    deliveryShaping: null,
    oldDeliveryShaping: { messagesPerSec: 10, burst: 1 },
  });
  sub.unsubscribe();
});
