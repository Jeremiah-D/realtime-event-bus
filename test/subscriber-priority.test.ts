import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import {
  orderDrainVisits,
  resolveDeliveryPriority,
  DEFAULT_DELIVERY_PRIORITY_WEIGHT,
} from '../src/subscriber-priority.ts';

/**
 * Subscriber drain priority (EB-166): `SubscribeOptions.deliveryPriority`
 * reorders the flush loop's drain visits by deficit round-robin weight —
 * high-priority subscribers are served first under contention, every
 * subscriber is still visited every round (no starvation), and drain
 * budgets (shaping / throttling / rate limits / in-flight caps) are
 * untouched.
 */

const flush = () => new Promise((resolve) => setImmediate(resolve));

function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

test('resolveDeliveryPriority: named priorities and integer weights resolve, garbage throws', () => {
  assert.equal(resolveDeliveryPriority(undefined, 'subscribe'), undefined);
  assert.deepEqual(resolveDeliveryPriority('low', 'subscribe'), { weight: 1, label: 'low' });
  assert.deepEqual(resolveDeliveryPriority('normal', 'subscribe'), { weight: 2, label: 'normal' });
  assert.deepEqual(resolveDeliveryPriority('high', 'subscribe'), { weight: 4, label: 'high' });
  assert.deepEqual(resolveDeliveryPriority(7, 'subscribe'), { weight: 7, label: 'custom' });
  assert.equal(DEFAULT_DELIVERY_PRIORITY_WEIGHT, 2);
  for (const bad of ['urgent', 'HIGH', '', 0, -1, 1.5, NaN, null, {}, [], true]) {
    assert.throws(
      () => resolveDeliveryPriority(bad, 'subscribe'),
      RangeError,
      `expected RangeError for ${JSON.stringify(bad)}`,
    );
  }
});

test('orderDrainVisits: weight-priority order, insertion-stable within a class', () => {
  const deficits = new Map<string, number>();
  const candidates = [
    { id: 'low', weight: 1, insertionIndex: 0 },
    { id: 'high', weight: 4, insertionIndex: 1 },
    { id: 'normal', weight: 2, insertionIndex: 2 },
    { id: 'high2', weight: 4, insertionIndex: 3 },
    { id: 'custom9', weight: 9, insertionIndex: 4 },
  ];
  const ordered = orderDrainVisits(candidates, deficits);
  assert.deepEqual(
    ordered.map((c) => c.id),
    ['custom9', 'high', 'high2', 'normal', 'low'],
  );
  // Input array is not mutated.
  assert.deepEqual(
    candidates.map((c) => c.id),
    ['low', 'high', 'normal', 'high2', 'custom9'],
  );
  // Stable across rounds: deficits carry over but the order does not flap.
  const again = orderDrainVisits(candidates, deficits);
  assert.deepEqual(
    again.map((c) => c.id),
    ['custom9', 'high', 'high2', 'normal', 'low'],
  );
  // Empty input, empty output — no deficit entries created.
  assert.deepEqual(orderDrainVisits([], new Map()), []);
});

test('orderDrainVisits: no deficit drift across rounds; late joiners order correctly', () => {
  const deficits = new Map<string, number>();
  const candidates = [
    { id: 'a', weight: 4, insertionIndex: 0 },
    { id: 'b', weight: 1, insertionIndex: 1 },
  ];
  for (let round = 0; round < 100; round++) {
    const ordered = orderDrainVisits(candidates, deficits);
    assert.deepEqual(ordered.map((c) => c.id), ['a', 'b']);
  }
  // Steady state: every visited candidate's deficit returns to its
  // pre-round value, so nothing drifts even over many rounds.
  assert.deepEqual(Object.fromEntries(deficits), { a: 0, b: 0 });
  // A subscriber that joins later (no deficit history) slots into
  // weight order immediately.
  const joined = orderDrainVisits(
    [...candidates, { id: 'c', weight: 10, insertionIndex: 2 }],
    deficits,
  );
  assert.deepEqual(joined.map((c) => c.id), ['c', 'a', 'b']);
});

test('default: no priorities — subscription order, normal dispatches counted', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  bus.subscribe('t', () => order.push('first'));
  bus.subscribe('t', () => order.push('second'));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['first', 'second']);
  const stats = bus.getStats();
  assert.deepEqual(stats.subscriberPriority.dispatches, {
    low: 0,
    normal: 2,
    high: 0,
    custom: 0,
  });
  // No drain priorities in play: no priority series in the exposition.
  const exposition = renderPrometheus(stats);
  assert.ok(!exposition.includes('eventbus_subscriber_priority_dispatch_total'));
});

test('high-priority subscriber is served before low/normal under contention', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  bus.subscribe('t', () => order.push('low'), { deliveryPriority: 'low' });
  bus.subscribe('t', () => order.push('normal'));
  bus.subscribe('t', () => order.push('high'), { deliveryPriority: 'high' });
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['high', 'normal', 'low']);
  const stats = bus.getStats();
  assert.deepEqual(stats.subscriberPriority.dispatches, {
    low: 1,
    normal: 1,
    high: 1,
    custom: 0,
  });
});

test('custom integer weights order by weight descending', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  bus.subscribe('t', () => order.push('w1'), { deliveryPriority: 1 });
  bus.subscribe('t', () => order.push('w10'), { deliveryPriority: 10 });
  bus.subscribe('t', () => order.push('w3'), { deliveryPriority: 3 });
  bus.subscribe('t', () => order.push('normal'));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['w10', 'w3', 'normal', 'w1']);
  const stats = bus.getStats();
  assert.deepEqual(stats.subscriberPriority.dispatches, {
    low: 0,
    normal: 1,
    high: 0,
    custom: 3,
  });
});

test('same weight class keeps subscription order (stable)', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  bus.subscribe('t', () => order.push('h1'), { deliveryPriority: 'high' });
  bus.subscribe('t', () => order.push('h2'), { deliveryPriority: 'high' });
  bus.subscribe('t', () => order.push('l1'), { deliveryPriority: 'low' });
  bus.publish('t', 'x');
  await flush();
  bus.publish('t', 'y');
  await flush();
  assert.deepEqual(order, ['h1', 'h2', 'l1', 'h1', 'h2', 'l1']);
});

test('low-priority subscribers are never starved', async () => {
  const bus = new EventBus();
  const lowReceived: unknown[] = [];
  const highReceived: unknown[] = [];
  bus.subscribe('t', (msg) => lowReceived.push(msg.payload), { deliveryPriority: 'low' });
  bus.subscribe('t', (msg) => highReceived.push(msg.payload), { deliveryPriority: 'high' });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(lowReceived, [0, 1, 2, 3, 4]);
  assert.deepEqual(highReceived, [0, 1, 2, 3, 4]);
});

test('subscribe validates deliveryPriority with RangeError, leaving no subscriber behind', () => {
  const bus = new EventBus();
  for (const bad of ['urgent', 0, -3, 2.5, null, {}, 'HIGH']) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { deliveryPriority: bad as never }),
      RangeError,
    );
  }
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('unsubscribing the last non-default subscriber restores plain subscription order', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  const lowSub = bus.subscribe('t', () => order.push('low'), { deliveryPriority: 'low' });
  bus.subscribe('t', () => order.push('high'), { deliveryPriority: 'high' });
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['high', 'low']);
  lowSub.unsubscribe();
  order.length = 0;
  bus.publish('t', 'y');
  await flush();
  // Only the high subscriber remains; a lone subscriber trivially keeps
  // its own order — the DRR path must not disturb it.
  assert.deepEqual(order, ['high']);
  const stats = bus.getStats();
  assert.equal(stats.subscriberPriority.dispatches.high, 2);
  assert.equal(stats.subscriberPriority.dispatches.low, 1);
});

test('priority composes with deliveryShaping: order changes, budget does not', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const order: string[] = [];
  const highReceived: unknown[] = [];
  const lowReceived: unknown[] = [];
  bus.subscribe(
    't',
    (msg) => {
      order.push('high');
      highReceived.push(msg.payload);
    },
    { deliveryPriority: 'high', deliveryShaping: { messagesPerSec: 2, burst: 2 } },
  );
  bus.subscribe(
    't',
    (msg) => {
      order.push('low');
      lowReceived.push(msg.payload);
    },
    { deliveryPriority: 'low' },
  );
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  // High is visited first, but its shaping budget (2) is untouched by priority.
  assert.equal(order[0], 'high');
  assert.deepEqual(highReceived, [0, 1]);
  assert.deepEqual(lowReceived, [0, 1, 2, 3, 4]);
});

test('metrics exposition renders per-class dispatch series, activity-gated', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { deliveryPriority: 'high' });
  bus.subscribe('t', () => {}, { deliveryPriority: 'low' });
  bus.publish('t', 'x');
  await flush();
  const exposition = renderPrometheus(bus.getStats());
  assert.ok(
    exposition.includes('# HELP eventbus_subscriber_priority_dispatch_total'),
    'missing HELP block',
  );
  assert.ok(
    exposition.includes('eventbus_subscriber_priority_dispatch_total{class="high"} 1'),
  );
  assert.ok(
    exposition.includes('eventbus_subscriber_priority_dispatch_total{class="low"} 1'),
  );
  assert.ok(
    exposition.includes('eventbus_subscriber_priority_dispatch_total{class="normal"} 0'),
  );
  assert.ok(
    exposition.includes('eventbus_subscriber_priority_dispatch_total{class="custom"} 0'),
  );
});

test('dispatch counts accumulate across flush rounds', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { deliveryPriority: 'high' });
  for (let round = 0; round < 3; round++) {
    bus.publish('t', round);
    await flush();
  }
  const stats = bus.getStats();
  assert.equal(stats.subscriberPriority.dispatches.high, 3);
  assert.equal(stats.subscriberPriority.dispatches.normal, 0);
});

test('priority applies to subscribeReliable wrappers too', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  bus.subscribeReliable(
    't',
    (delivery) => {
      order.push('low');
      delivery.ack();
    },
    { deliveryPriority: 'low' },
  );
  bus.subscribeReliable(
    't',
    (delivery) => {
      order.push('high');
      delivery.ack();
    },
    { deliveryPriority: 'high' },
  );
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['high', 'low']);
});
