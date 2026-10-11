import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('shadow subscriber receives a full copy of production traffic', async () => {
  const bus = new EventBus();
  const prod: unknown[] = [];
  const shadow: unknown[] = [];
  bus.subscribe('orders.*', (msg) => {
    prod.push(msg.payload);
  });
  bus.subscribe('orders.*', (msg) => {
    shadow.push(msg.payload);
  }, { shadow: true });
  bus.publish('orders.created', { id: 1 });
  bus.publish('orders.paid', { id: 2 });
  bus.publish('other.topic', { id: 3 });
  await flush();
  assert.deepEqual(prod, [{ id: 1 }, { id: 2 }]);
  assert.deepEqual(shadow, [{ id: 1 }, { id: 2 }]);
  const stats = bus.getStats();
  assert.equal(stats.shadowedMessages, 2);
  assert.equal(stats.shadowDroppedMessages, 0);
  // Production counters are untouched by the mirror.
  assert.equal(stats.deliveredMessages, 2);
  assert.equal(stats.droppedMessages, 0);
  assert.equal(stats.shadows.length, 1);
  const [row] = stats.shadows;
  assert.equal(row.shadowed, 2);
  assert.equal(row.dropped, 0);
  assert.equal(row.queueSize, 0);
  assert.equal(row.pattern, 'orders.*');
});

test('shadow deliveries are not counted in publish fan-out width', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { shadow: true });
  // One shadow subscriber matches: the production fan-out width stays 0.
  // `publish` returns the accepted count — the shadow's copy never counts.
  const accepted = bus.publish('t', 'x');
  assert.equal(accepted, 0);
  await flush();
  assert.equal(bus.getStats().shadowedMessages, 1);
  assert.equal(bus.getStats().deliveredMessages, 0);
  const topic = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topic?.subscriberCount, 0);
});

test('shadow queue sheds independently without touching production drops', async () => {
  const bus = new EventBus();
  const prod: unknown[] = [];
  bus.subscribe('t', (msg) => {
    prod.push(msg.payload);
  });
  // Shadow queue holds 2 with drop-newest: the third copy sheds on the
  // shadow side only.
  bus.subscribe('t', () => {}, { shadow: { queueSize: 2, dropPolicy: 'drop-newest' } });
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(prod, [1, 2, 3]);
  const stats = bus.getStats();
  assert.equal(stats.shadowedMessages, 2);
  assert.equal(stats.shadowDroppedMessages, 1);
  assert.equal(stats.droppedMessages, 0);
  assert.equal(stats.deliveredMessages, 3);
  const [row] = stats.shadows;
  assert.equal(row.shadowed, 2);
  assert.equal(row.dropped, 1);
});

test('a throwing shadow handler is isolated from production', async () => {
  const bus = new EventBus();
  const prod: unknown[] = [];
  bus.subscribe('t', (msg) => {
    prod.push(msg.payload);
  });
  bus.subscribe(
    't',
    () => {
      throw new Error('shadow validator exploded');
    },
    { shadow: true },
  );
  // The publish call itself must not throw, and the flush must survive
  // the shadow's throw: production still gets everything.
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(prod, ['a', 'b']);
  const stats = bus.getStats();
  // The messages were handed to the shadow handler, so they count as
  // shadowed — exactly like production counts a throwing handler's
  // message as delivered.
  assert.equal(stats.shadowedMessages, 2);
  assert.equal(stats.deliveredMessages, 2);
});

test('shadow subscriber never disturbs consumer-group assignment or committed offsets', async () => {
  const bus = new EventBus();
  const memberA: unknown[] = [];
  const memberB: unknown[] = [];
  const shadow: unknown[] = [];
  bus.subscribeToGroup('g', 't', (msg) => {
    memberA.push(msg.payload);
  });
  bus.subscribeToGroup('g', 't', (msg) => {
    memberB.push(msg.payload);
  });
  bus.subscribe('t', (msg) => {
    shadow.push(msg.payload);
  }, { shadow: true });
  for (let i = 1; i <= 4; i += 1) bus.publish('t', i);
  await flush();
  // The group's 4 messages are split across the two members (2 each) —
  // the shadow stole none of the group's copies.
  assert.equal(memberA.length + memberB.length, 4);
  assert.deepEqual(shadow, [1, 2, 3, 4]);
  // The group's assignment watermark advanced over all 4; committing is
  // unaffected by the shadow's presence.
  assert.equal(bus.getGroupOffsets('g')['t'], 4);
  bus.commitOffset('g', 't', 4);
  assert.equal(bus.getCommittedOffsets('g')['t'], 4);
  assert.equal(bus.getStats().shadowedMessages, 4);
});

test('shadow rejects incompatible options with RangeError', () => {
  const bus = new EventBus();
  const noop = () => {};
  // Reliable and group subscriptions reject shadow outright.
  assert.throws(
    () => bus.subscribeReliable('t', (d) => d.ack(), { shadow: true }),
    RangeError,
  );
  assert.throws(() => bus.subscribeToGroup('g', 't', noop, { shadow: true }), RangeError);
  // Within subscribe(): delivery-control, ordering, replay and
  // exactly-once features fail fast instead of silently doing nothing.
  const bad: Array<[string, Record<string, unknown>]> = [
    ['throttle', { throttle: true }],
    ['deliveryShaping', { deliveryShaping: true }],
    ['rateLimit', { rateLimit: { maxMessages: 1, perWindowMs: 1000 } }],
    ['healthProbe', { healthProbe: true }],
    ['batch', { batch: true }],
    ['causal', { causal: true }],
    ['deduplicateMessages', { deduplicateMessages: true }],
    ['keyHotspot', { keyHotspot: true }],
    ['ackLatency', { ackLatency: true }],
  ];
  for (const [name, opts] of bad) {
    assert.throws(() => bus.subscribe('t', noop, { shadow: true, ...opts }), RangeError, name);
  }
  // Invalid shadow tuning fails fast too.
  assert.throws(() => bus.subscribe('t', noop, { shadow: { queueSize: 0 } }), RangeError);
  assert.throws(() => bus.subscribe('t', noop, { shadow: { queueSize: 1.5 } }), RangeError);
  assert.throws(
    () => bus.subscribe('t', noop, { shadow: { dropPolicy: 'drop-random' as never } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', noop, { shadow: 42 as never }),
    TypeError,
  );
  // Nothing half-registered: every throw above left no subscriber behind.
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('shadow filter narrows the mirror without touching production filter stats', async () => {
  const bus = new EventBus();
  const shadow: unknown[] = [];
  bus.subscribe('market.**', (msg) => {
    shadow.push(msg.payload);
  }, {
    shadow: true,
    filter: (payload) => (payload as { symbol: string }).symbol === 'BTC',
  });
  bus.publish('market.btc', { symbol: 'BTC' });
  bus.publish('market.eth', { symbol: 'ETH' });
  await flush();
  assert.deepEqual(shadow, [{ symbol: 'BTC' }]);
  const stats = bus.getStats();
  assert.equal(stats.shadowedMessages, 1);
  // The shadow's filter rejection is deliberately skipped — never queued,
  // never counted in the production filteredMessages.
  assert.equal(stats.filteredMessages, 0);
  assert.equal(stats.shadowDroppedMessages, 0);
});

test('shadow composes with deliveryLatency and lagMonitor observability', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    shadow: true,
    deliveryLatency: true,
    lagMonitor: true,
  });
  bus.publish('t', 'x');
  await flush();
  const stats = bus.getStats();
  const latencyRow = stats.deliveryLatency.find((s) => s.subscriberId === sub.id);
  assert.ok(latencyRow !== undefined);
  assert.equal(latencyRow.samples, 1);
  const lagRow = stats.lag.find((s) => s.subscriberId === sub.id);
  assert.ok(lagRow !== undefined);
  assert.equal(lagRow.samples, 1);
  assert.equal(lagRow.watermarkMs, 0);
});

test('shadowed and shadow-dropped counters render in Prometheus exposition', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {}, { shadow: { queueSize: 1, dropPolicy: 'drop-newest' } });
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.match(text, /eventbus_shadowed_messages_total 1/);
  assert.match(text, /eventbus_shadow_dropped_messages_total 1/);
  assert.match(text, /eventbus_delivered_messages_total 0/);
  assert.match(text, /eventbus_dropped_messages_total 0/);
});

test('shadow drops do not surface as production sequence gaps', async () => {
  const bus = new EventBus();
  const prod: number[] = [];
  bus.subscribe('t', (msg: BusMessage) => {
    prod.push(msg.seq);
  });
  // Shadow queue of 1 with drop-oldest: heavy shed on the shadow side.
  bus.subscribe('t', () => {}, { shadow: { queueSize: 1 } });
  for (let i = 1; i <= 10; i += 1) bus.publish('t', i);
  await flush();
  assert.deepEqual(prod, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  const stats = bus.getStats();
  assert.equal(stats.sequenceGaps, 0);
  assert.ok(stats.shadowDroppedMessages > 0);
  assert.equal(stats.shadowedMessages + stats.shadowDroppedMessages, 10);
});

test('unsubscribing a shadow subscriber stops the mirror', async () => {
  const bus = new EventBus();
  const shadow: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => {
    shadow.push(msg.payload);
  }, { shadow: true });
  bus.publish('t', 1);
  await flush();
  sub.unsubscribe();
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(shadow, [1]);
  assert.equal(bus.getStats().shadows.length, 0);
  assert.equal(bus.getStats().shadowedMessages, 1);
});
