import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type BridgeEnvelope,
  type BridgeTransport,
  type BusMessage,
} from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/**
 * In-memory transport that records every mirrored envelope and hands it to
 * `deliver` synchronously — the two-process bridge without the processes.
 */
function loopbackTransport(
  deliver: (env: BridgeEnvelope) => void,
  seen: BridgeEnvelope[] = [],
): BridgeTransport {
  return {
    name: 'loopback',
    publish(envelope: BridgeEnvelope) {
      seen.push(envelope);
      deliver(envelope);
    },
  };
}

const sink: BridgeTransport = { name: 'sink', publish() {} };

test('bridge mirrors admitted publishes to the peer; inbound never re-mirrors', async () => {
  const seenA: BridgeEnvelope[] = [];
  let busB!: EventBus;
  const busA = new EventBus({
    bridge: { transport: loopbackTransport((env) => busB.receiveFromBridge(env), seenA) },
  });
  busB = new EventBus({ bridge: { transport: sink } });
  const received: BusMessage[] = [];
  busB.subscribe('orders.*', (msg) => {
    received.push(msg);
  });

  busA.publish('orders.new', { id: 1 });

  assert.equal(busA.getStats().bridge.outbound, 1);
  assert.equal(seenA.length, 1);
  assert.equal(seenA[0].topic, 'orders.new');
  assert.deepEqual(seenA[0].payload, { id: 1 });

  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].topic, 'orders.new');
  assert.deepEqual(received[0].payload, { id: 1 });
  assert.equal(busB.getStats().bridge.inbound, 1);
  // Loop-free: the peer's inbound never goes back out on the bridge.
  assert.equal(busB.getStats().bridge.outbound, 0);
  assert.equal(busA.getStats().bridge.inbound, 0);
});

test('bridge envelope carries key, keySeq, messageId and traceId end to end', async () => {
  const seen: BridgeEnvelope[] = [];
  let busB!: EventBus;
  const busA = new EventBus({
    trace: true,
    bridge: { transport: loopbackTransport((env) => busB.receiveFromBridge(env), seen) },
  });
  busB = new EventBus({ trace: true, bridge: { transport: sink } });
  const received: BusMessage[] = [];
  busB.subscribe('pay.*', (msg) => {
    received.push(msg);
  });

  busA.publish('pay.done', { amount: 5 }, { key: 'order-1', messageId: 'm-1' });
  await flush();

  assert.equal(seen.length, 1);
  const env = seen[0];
  assert.equal(env.key, 'order-1');
  assert.equal(env.keySeq, 1);
  assert.equal(env.messageId, 'm-1');
  assert.match(env.traceId ?? '', /^[0-9a-f]{32}$/);

  assert.equal(received.length, 1);
  assert.equal(received[0].messageId, 'm-1');
  // The trace continues on the peer under the source's trace id.
  const spans = busB.getStats().traceSpans.filter((s) => s.traceId === env.traceId);
  assert.ok(spans.length > 0, 'expected the continued trace on the peer');
});

test('per-key publish order survives out-of-order transport delivery', async () => {
  const seen: BridgeEnvelope[] = [];
  const busA = new EventBus({
    bridge: {
      transport: {
        name: 'reorder',
        publish(env: BridgeEnvelope) {
          seen.push(env);
        },
      },
    },
  });
  const busB = new EventBus({ bridge: { transport: sink } });
  const order: number[] = [];
  busB.subscribe('k.*', (msg) => {
    order.push((msg.payload as { n: number }).n);
  });

  busA.publish('k.t', { n: 1 }, { key: 'k' });
  busA.publish('k.t', { n: 2 }, { key: 'k' });
  busA.publish('k.t', { n: 3 }, { key: 'k' });
  assert.equal(seen[0].keySeq, 1);
  assert.equal(seen[1].keySeq, 2);
  assert.equal(seen[2].keySeq, 3);

  // m1 establishes the subscriber's per-key baseline; the transport then
  // delivers m3 before m2 — the reorder buffer must hold m3 until m2
  // arrives, so the subscriber still sees publish order.
  busB.receiveFromBridge(seen[0]);
  busB.receiveFromBridge(seen[2]);
  busB.receiveFromBridge(seen[1]);
  await flush();
  assert.deepEqual(order, [1, 2, 3]);
});

test('inbound keySeq advances the local key cursor', async () => {
  const seen: BridgeEnvelope[] = [];
  const bus = new EventBus({
    bridge: { transport: loopbackTransport(() => {}, seen) },
  });
  bus.subscribe('k.*', () => {});
  bus.receiveFromBridge({ topic: 'k.t', payload: { n: 1 }, key: 'k', keySeq: 5 });
  await flush();
  // The next LOCAL keyed publish draws past the remote high-water mark
  // instead of reusing a number the bridge already handed out.
  bus.publish('k.t', { n: 2 }, { key: 'k' });
  assert.equal(seen[seen.length - 1].keySeq, 6);
});

test('inbound envelopes go through admission: schema rejection', async () => {
  const rejections: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    bridge: { transport: sink },
    onAdmissionRejected: (e) => {
      rejections.push(e);
    },
  });
  bus.setTopicSchema('pay.*', (p) => typeof p === 'object' && p !== null);
  const received: BusMessage[] = [];
  bus.subscribe('pay.*', (msg) => {
    received.push(msg);
  });

  const res = bus.receiveFromBridge({ topic: 'pay.new', payload: 42 });
  assert.equal(res.accepted, true); // queued; the rejection happens at drain
  await flush();

  assert.equal(received.length, 0);
  assert.deepEqual(
    rejections.map((e) => e.reason),
    ['schema'],
  );
  assert.equal(bus.getStats().bridge.inbound, 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('inbound envelopes go through admission: ACL deny', async () => {
  const bus = new EventBus({
    bridge: { transport: sink },
    acl: { rules: [{ pattern: 'secret.**', publish: 'deny' }] },
  });
  const received: BusMessage[] = [];
  bus.subscribe('secret.**', (msg) => {
    received.push(msg);
  });

  bus.receiveFromBridge({ topic: 'secret.x', payload: 1 });
  await flush();

  assert.equal(received.length, 0);
  assert.equal(bus.getStats().authzDenied, 1);
  assert.equal(bus.getStats().bridge.inbound, 0);
});

test('source TTL deadline rides the envelope and expires at drain', async () => {
  const clock = controllableClock(1_000);
  let busB!: EventBus;
  const busA = new EventBus({
    now: clock.now,
    bridge: { transport: loopbackTransport((env) => busB.receiveFromBridge(env)) },
  });
  busB = new EventBus({ now: clock.now, bridge: { transport: sink } });
  busA.setTopicTtl('tmp.*', 50);
  const received: BusMessage[] = [];
  busB.subscribe('tmp.*', (msg) => {
    received.push(msg);
  });

  busA.publish('tmp.x', { v: 1 });
  // The envelope carries expiresAt = 1000 + 50; the clock moves past it
  // before the peer drains, so the message expires instead of delivering.
  clock.nowMs = 2_000;
  await flush();

  assert.equal(received.length, 0);
  assert.equal(busB.getStats().expiredMessages, 1);
});

test('full ingress buffer sheds the newest envelope and counts it', async () => {
  const bus = new EventBus({
    bridge: { transport: sink, maxInboundQueue: 2 },
  });
  const received: BusMessage[] = [];
  bus.subscribe('s.*', (msg) => {
    received.push(msg);
  });

  const r1 = bus.receiveFromBridge({ topic: 's.1', payload: 1 });
  const r2 = bus.receiveFromBridge({ topic: 's.2', payload: 2 });
  const r3 = bus.receiveFromBridge({ topic: 's.3', payload: 3 });
  const r4 = bus.receiveFromBridge({ topic: 's.4', payload: 4 });
  assert.deepEqual(
    [r1.accepted, r2.accepted, r3.accepted, r4.accepted],
    [true, true, false, false],
  );
  assert.equal(r3.reason, 'shed');
  assert.equal(r4.reason, 'shed');
  assert.equal(bus.getStats().bridge.dropped, 2);

  await flush();
  // Drop-newest: the two oldest envelopes survived.
  assert.deepEqual(
    received.map((m) => m.topic),
    ['s.1', 's.2'],
  );
  assert.equal(bus.getStats().bridge.inbound, 2);
});

test('malformed envelopes are reported, never thrown', async () => {
  const bus = new EventBus({ bridge: { transport: sink } });
  const received: BusMessage[] = [];
  bus.subscribe('**', (msg) => {
    received.push(msg);
  });
  const bad: unknown[] = [
    null,
    undefined,
    42,
    'x',
    {},
    { topic: '' },
    { topic: 'a'.repeat(1025) },
    { topic: 'ok', traceId: 'not-hex' },
    { topic: 'ok', traceId: '0'.repeat(32) },
    { topic: 'ok', key: 'k' }, // key without keySeq
    { topic: 'ok', keySeq: 3 }, // keySeq without key
    { topic: 'ok', key: 'k', keySeq: 0 },
    { topic: 'ok', messageId: 7 },
    { topic: 'ok', expiresAt: Number.NaN },
  ];
  for (const envelope of bad) {
    const res = bus.receiveFromBridge(envelope as BridgeEnvelope);
    assert.equal(res.accepted, false, `envelope accepted: ${JSON.stringify(envelope)}`);
    assert.equal(res.reason, 'invalid-envelope');
  }
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().bridge.inbound, 0);
});

test('receiveFromBridge without a bridge reports not-configured', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.receiveFromBridge({ topic: 'a', payload: 1 }), {
    accepted: false,
    reason: 'not-configured',
  });
});

test('bridge config validation fails fast in the constructor', () => {
  assert.throws(
    () => new EventBus({ bridge: { transport: undefined as never } }),
    RangeError,
  );
  assert.throws(() => new EventBus({ bridge: { transport: { name: 'x' } as never } }), RangeError);
  assert.throws(
    () =>
      new EventBus({
        bridge: { transport: sink, maxInboundQueue: 0 },
      }),
    RangeError,
  );
  assert.throws(
    () =>
      new EventBus({
        bridge: { transport: sink, maxInboundQueue: 1.5 },
      }),
    RangeError,
  );
});

test('a throwing transport never breaks publishing', async () => {
  const bus = new EventBus({
    bridge: {
      transport: {
        name: 'bad',
        publish() {
          throw new Error('broker down');
        },
      },
    },
  });
  const received: BusMessage[] = [];
  bus.subscribe('t.*', (msg) => {
    received.push(msg);
  });
  assert.equal(bus.publish('t.x', { v: 1 }), 1);
  assert.equal(bus.getStats().bridge.outbound, 1);
  await flush();
  assert.equal(received.length, 1);
});

test('a rejecting transport promise is contained', async () => {
  const bus = new EventBus({
    bridge: {
      transport: {
        name: 'bad-async',
        publish() {
          return Promise.reject(new Error('broker down'));
        },
      },
    },
  });
  const received: BusMessage[] = [];
  bus.subscribe('t.*', (msg) => {
    received.push(msg);
  });
  assert.equal(bus.publish('t.x', { v: 1 }), 1);
  await flush();
  assert.equal(received.length, 1);
});

test('metrics exposition carries the bridge counters', async () => {
  let busB!: EventBus;
  const busA = new EventBus({
    bridge: { transport: loopbackTransport((env) => busB.receiveFromBridge(env)) },
  });
  busB = new EventBus({ bridge: { transport: sink } });
  busB.subscribe('m.*', () => {});
  busA.publish('m.x', 1);
  busB.receiveFromBridge({ topic: 'm.y', payload: 2 });
  busB.receiveFromBridge({ topic: 'm.y', payload: 3 });
  await flush();
  const text = renderPrometheus(busB.getStats());
  assert.ok(text.includes('eventbus_bridge_outbound_total 0'));
  assert.ok(text.includes('eventbus_bridge_inbound_total 3'));
});
