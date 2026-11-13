import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default delivery-shaping rate (EB-484):
 * `setDefaultDeliveryShaping` installs the `messagesPerSec` / `burst`
 * new opt-in shaping subscribers fall back to when they omit a field —
 * the priority chain is subscriber-explicit > bus default > 100 / one
 * second's budget — and the default never enables shaping: a
 * subscriber that does not opt into `deliveryShaping` is untouched
 * by it.
 *
 * Rate assertions follow `shaping.test.ts`: an injectable clock makes
 * token-bucket refills deterministic, and a publish triggers the next
 * flush round.
 */

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

test('EB-484: the factory bus default is unset (deliveryShaping: true still means 100/s with a 100 burst)', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  assert.equal(bus.getDefaultDeliveryShaping(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: true,
    queueSize: 200,
  });
  for (let i = 0; i < 100; i++) bus.publish('t', i);
  await flush();
  // The default burst (100) covers all one hundred.
  assert.deepEqual(received, Array.from({ length: 100 }, (_, i) => i));
  assert.equal(bus.pendingCount(sub.id), 0);
  assert.equal(
    bus.getStats().shapedSubscribers,
    0,
    'nothing is held back once the backlog has drained',
  );
  sub.unsubscribe();
});

test('EB-484: a new opt-in subscriber takes the bus default rate, with a one-second burst', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 50 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { deliveryShaping: true });
  for (let i = 0; i < 60; i++) bus.publish('t', i);
  await flush();
  // Burst of 50 delivered; the other 10 wait — nothing dropped.
  assert.equal(received.length, 50);
  assert.equal(bus.pendingCount(sub.id), 10);
  clock.advance(20); // 50/s -> 1 token
  bus.publish('t', 60); // also triggers a flush
  await flush();
  assert.equal(received.length, 51, 'the refill runs at the default 50/s');
  assert.equal(received[50], 50);
  sub.unsubscribe();
});

test('EB-484: an explicit messagesPerSec beats the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 2, burst: 2 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 100, burst: 2 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  clock.advance(100); // 100/s -> 10 tokens, capped at the burst of 2
  bus.publish('t', 5); // also triggers a flush
  await flush();
  // At the default 2/s, 100ms would refill 0.2 tokens — nothing new.
  assert.deepEqual(received, [0, 1, 2, 3]);
  sub.unsubscribe();
});

test('EB-484: an explicit burst beats the bus default, and the default rate still paces refills', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 10, burst: 2 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { burst: 5 },
  });
  for (let i = 0; i < 8; i++) bus.publish('t', i);
  await flush();
  // The explicit burst of 5 wins over the default 2.
  assert.equal(received.length, 5);
  assert.equal(bus.pendingCount(sub.id), 3);
  clock.advance(100); // 10/s (the default rate) -> 1 token
  bus.publish('t', 8); // also triggers a flush
  await flush();
  assert.equal(received.length, 6);
  sub.unsubscribe();
});

test('EB-484: a default burst alone leaves messagesPerSec at 100', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ burst: 3 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { deliveryShaping: true });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(received.length, 3, 'the burst comes from the default');
  clock.advance(10); // 100/s -> 1 token
  bus.publish('t', 5); // also triggers a flush
  await flush();
  assert.equal(received.length, 4, 'the refill still runs at the undocumented 100/s');
  sub.unsubscribe();
});

test('EB-484: clearing with null returns new opt-in subscribers to 100 / one second', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 2, burst: 2 });
  bus.setDefaultDeliveryShaping(null);
  assert.equal(bus.getDefaultDeliveryShaping(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: true,
    queueSize: 200,
  });
  for (let i = 0; i < 101; i++) bus.publish('t', i);
  await flush();
  assert.equal(received.length, 100);
  assert.equal(bus.pendingCount(sub.id), 1);
  sub.unsubscribe();
});

test('EB-484: revising the bus default never moves an already-subscribed rate, burst, or bucket budget', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 10, burst: 2 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { deliveryShaping: true });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.pendingCount(sub.id), 3);
  bus.setDefaultDeliveryShaping({ messagesPerSec: 1, burst: 1 });
  clock.advance(100); // old rate 10/s -> 1 token; the new 1/s would give 0.1
  bus.publish('t', 5); // also triggers a flush
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2],
    'the early subscriber keeps the rate and burst it was baked with',
  );
  assert.equal(bus.pendingCount(sub.id), 3);

  // A subscriber joining after the revision takes the new default.
  const lateReceived: unknown[] = [];
  const late = bus.subscribe('u', (msg) => lateReceived.push(msg.payload), {
    deliveryShaping: true,
  });
  for (let i = 0; i < 3; i++) bus.publish('u', i);
  await flush();
  assert.deepEqual(lateReceived, [0], 'the new default burst is 1');
  sub.unsubscribe();
  late.unsubscribe();
});

test('EB-484: the default never enables shaping', async () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryShaping({ messagesPerSec: 1, burst: 1 });
  const received: unknown[] = [];
  // No `deliveryShaping` opt-in: still one handler call per message.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().shapedSubscribers, 0);
  sub.unsubscribe();
});

test('EB-484: setDeliveryShaping partial revision still takes the live value and never recomputes the burst', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 10, burst: 2 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { deliveryShaping: true });
  // Partial revision: only the rate moves; the burst keeps its live 2
  // instead of being recomputed from the new rate (which would be 20).
  bus.setDeliveryShaping(sub.id, { messagesPerSec: 20 });
  const events = bus.getSubscriptionAudit({ action: 'delivery_shaping_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    deliveryShaping: { messagesPerSec: 20, burst: 2 },
    oldDeliveryShaping: { messagesPerSec: 10, burst: 2 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1], 'the burst is still 2, not 20');
  sub.unsubscribe();
});

test('EB-484: setDeliveryShaping null clears without the default re-applying, and a later revision is refused', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 2, burst: 2 });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { deliveryShaping: true });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  bus.setDeliveryShaping(sub.id, null);
  assert.equal(bus.getStats().shapedSubscribers, 0, 'the shaping is gone');
  for (let i = 5; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    received,
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    'the cleared subscriber delivers unshaped even though a default is installed',
  );
  assert.throws(() => bus.setDeliveryShaping(sub.id, { messagesPerSec: 5 }), RangeError);
  bus.publish('t', 10);
  bus.publish('t', 11);
  await flush();
  assert.equal(received.length, 12, 'still unshaped after the refused revision');
  sub.unsubscribe();
});

test('EB-484: revising to the current default is a no-op with no audit, and the getter follows the live value and returns a copy', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryShaping(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_shaping_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultDeliveryShaping({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_shaping_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  const opts = { messagesPerSec: 50 };
  bus.setDefaultDeliveryShaping(opts);
  opts.messagesPerSec = 999;
  assert.deepEqual(
    bus.getDefaultDeliveryShaping(),
    { messagesPerSec: 50 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultDeliveryShaping({ messagesPerSec: 50 });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 50, burst: undefined });
  assert.equal(bus.getDefaultDeliveryShaping()?.messagesPerSec, 50);
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_shaping_revised' });
  assert.equal(audit.length, 1, 're-affirming the default (undefined counts as missing) audits nothing');
  assert.deepEqual(audit[0].detail, { previous: null, deliveryShaping: { messagesPerSec: 50 } });
  const held = bus.getDefaultDeliveryShaping();
  assert.ok(held !== undefined);
  held.messagesPerSec = 999;
  assert.deepEqual(bus.getDefaultDeliveryShaping(), { messagesPerSec: 50 }, 'the getter returns a copy');
  bus.setDefaultDeliveryShaping({ messagesPerSec: 50, burst: 60 }, 'ops');
  assert.deepEqual(bus.getDefaultDeliveryShaping(), { messagesPerSec: 50, burst: 60 });
  const revised = bus.getSubscriptionAudit({ action: 'default_delivery_shaping_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { messagesPerSec: 50 },
    deliveryShaping: { messagesPerSec: 50, burst: 60 },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultDeliveryShaping(null);
  assert.equal(bus.getDefaultDeliveryShaping(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_delivery_shaping_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { messagesPerSec: 50, burst: 60 },
    deliveryShaping: null,
  });
  bus.setDefaultDeliveryShaping(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_shaping_revised' }).length,
    3,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-484: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryShaping({ messagesPerSec: 50 });
  const bad = [
    0,
    -1,
    '50',
    [],
    true,
    { messagesPerSec: 0 },
    { messagesPerSec: -5 },
    { messagesPerSec: Number.NaN },
    { messagesPerSec: Number.POSITIVE_INFINITY },
    { messagesPerSec: '50' },
    { burst: 0 },
    { burst: -2 },
    { burst: Number.NaN },
    { burst: Number.POSITIVE_INFINITY },
    undefined,
  ] as unknown[];
  for (const opts of bad) {
    assert.throws(
      () => bus.setDefaultDeliveryShaping(opts as never),
      RangeError,
      `opts=${String(opts)}`,
    );
    assert.deepEqual(bus.getDefaultDeliveryShaping(), { messagesPerSec: 50 });
  }
  assert.throws(() => bus.setDefaultDeliveryShaping({ messagesPerSec: 60 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultDeliveryShaping(), { messagesPerSec: 50 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_shaping_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-484: the slow-start ramp targets the default shaping rate', () => {
  const clock = makeClock();
  // Without a default the ramp targets 100, so a 10/s start still ramps.
  const plain = new EventBus({ now: clock.now });
  const plainSub = plain.subscribe('t', () => {}, {
    deliveryShaping: true,
    slowStart: { initialPerSec: 10, rampMs: 10000 },
  });
  assert.equal(plain.getStats().rampingSubscribers, 1);
  plainSub.unsubscribe();

  // With a default of 10/s the ramp target is 10/s, so a 10/s start is
  // degenerate — no gate at all — while a 1/s start still ramps.
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeliveryShaping({ messagesPerSec: 10 });
  const flat = bus.subscribe('t', () => {}, {
    deliveryShaping: true,
    slowStart: { initialPerSec: 10, rampMs: 10000 },
  });
  assert.equal(bus.getStats().rampingSubscribers, 0, 'initialPerSec already at the default target: no ramp');
  flat.unsubscribe();
  const ramping = bus.subscribe('u', () => {}, {
    deliveryShaping: true,
    slowStart: { initialPerSec: 1, rampMs: 10000 },
  });
  assert.equal(bus.getStats().rampingSubscribers, 1, 'a slower start still ramps toward the default rate');
  ramping.unsubscribe();
});
