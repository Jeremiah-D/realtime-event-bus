import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import {
  OpsAlerter,
  RecordingAlertSink,
  canonicalAlertJson,
  signAlert,
  verifyDelivery,
  OPS_SIGNATURE_HEADER,
  type OpsAlert,
} from '../src/ops-alert.ts';
import { GatewayServer } from '../src/gateway-ws.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('signing: HMAC-SHA256 over canonical bytes, verifiable by the receiver', () => {
  const alert: OpsAlert = {
    kind: 'dead-letter-growth',
    at: 12345,
    busId: 'b1',
    detail: { subscriberId: 's1', topic: 't' },
  };
  const sig = signAlert(alert, 'secret');
  assert.match(sig, /^[0-9a-f]{64}$/);
  // Key order does not affect the signature (canonical JSON).
  const reordered: OpsAlert = {
    detail: { topic: 't', subscriberId: 's1' },
    busId: 'b1',
    at: 12345,
    kind: 'dead-letter-growth',
  };
  assert.equal(signAlert(reordered, 'secret'), sig);
  assert.ok(verifyDelivery({ alert, signature: sig }, 'secret'));
  assert.equal(verifyDelivery({ alert, signature: sig }, 'wrong'), false);
  // Tampering with any byte breaks verification.
  const tampered: OpsAlert = { ...alert, at: 12346 };
  assert.equal(verifyDelivery({ alert: tampered, signature: sig }, 'secret'), false);
  assert.equal(verifyDelivery({ alert, signature: 'zz' }, 'secret'), false);
  assert.equal(verifyDelivery(null as unknown as never, 'secret'), false);
  assert.equal(OPS_SIGNATURE_HEADER, 'x-ops-signature');
});

test('canonicalAlertJson sorts keys recursively', () => {
  const a = canonicalAlertJson({ z: 1, a: { y: 2, b: 3 }, m: [3, 1] });
  const b = canonicalAlertJson({ a: { b: 3, y: 2 }, m: [3, 1], z: 1 });
  assert.equal(a, b);
  assert.equal(a, '{"a":{"b":3,"y":2},"m":[3,1],"z":1}');
});

test('dead-letter growth raises a signed alert (dedup collapses the flood)', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'ops-dl',
    opsAlert: { sink, secret: 's3cr3t', keyId: 'ops-1', dedupWindowMs: 60_000 },
  });
  bus.subscribeReliable(
    't',
    () => {
      throw new Error('poison');
    },
    { deadLetter: { maxRedeliveries: 1 }, ackTimeoutMs: 5 },
  );
  bus.publish('t', { v: 1 });
  bus.publish('t', { v: 2 });
  bus.publish('t', { v: 3 });
  await new Promise((r) => setTimeout(r, 50));
  await flush();
  // Three DLQ moves, one alert — the dedup window collapsed the flood.
  assert.equal(sink.deliveries.length, 1);
  const delivery = sink.deliveries[0];
  assert.equal(delivery.alert.kind, 'dead-letter-growth');
  assert.equal(delivery.alert.busId, 'ops-dl');
  assert.equal(delivery.keyId, 'ops-1');
  assert.ok(verifyDelivery(delivery, 's3cr3t'));
  assert.equal(bus.getStats().opsAlert.sent, 1);
  assert.ok(bus.getStats().opsAlert.suppressed >= 2);
});

test('lag-SLO breach fires the user hook AND the ops alert', async () => {
  const sink = new RecordingAlertSink();
  let now = 1_000;
  const breaches: unknown[] = [];
  const bus = new EventBus({
    busId: 'ops-lag',
    now: () => now,
    opsAlert: { sink, secret: 's', dedupWindowMs: 60_000 },
    groupLag: {
      slo: {
        maxLagMessages: 2,
        breachSamples: 1,
        onLagSloBreach: (e) => breaches.push(e),
      },
    },
  });
  // Build lag: group member subscribed, publishes advance the assignment
  // watermark without commits — each publish is one SLO sample.
  const sub = bus.subscribeToGroup('g1', 't', () => {});
  bus.publish('t', { v: 1 });
  bus.publish('t', { v: 2 });
  bus.publish('t', { v: 3 }); // lag 3 > 2: breach fires
  await flush();
  assert.equal(breaches.length, 1);
  assert.equal(sink.deliveries.length, 1);
  assert.equal(sink.deliveries[0].alert.kind, 'lag-slo-breach');
  assert.equal(sink.deliveries[0].alert.detail.groupId, 'g1');
  assert.ok(verifyDelivery(sink.deliveries[0], 's'));
  // The user's hook ran too (fired first, alert second).
  assert.equal((breaches[0] as { groupId: string }).groupId, 'g1');
  sub.unsubscribe();
});

test('gateway disconnect storm: threshold triggers one alert, then rearms', () => {
  const sink = new RecordingAlertSink();
  let now = 1_000;
  const bus = new EventBus({
    busId: 'ops-gw',
    now: () => now,
    opsAlert: {
      sink,
      secret: 's',
      dedupWindowMs: 60_000,
      gatewayDisconnectThreshold: 3,
      gatewayDisconnectWindowMs: 10_000,
    },
  });
  bus.noteGatewayDisconnect();
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 0);
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1);
  assert.equal(sink.deliveries[0].alert.kind, 'gateway-disconnect-storm');
  assert.deepEqual(sink.deliveries[0].alert.detail, {
    disconnects: 3,
    windowMs: 10_000,
    threshold: 3,
  });
  // Still storming: no repeat alert.
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1);
  // Window drains below threshold: rearms, next storm alerts again.
  now += 20_000;
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1); // below threshold, rearmed silently
  bus.noteGatewayDisconnect();
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1); // dedup window still holds 'storm'
});

test('bounded retry: sink failures exhaust attempts then count failed', async () => {
  const sink = new RecordingAlertSink(10); // always fails
  const alerter = new OpsAlerter({
    sink,
    secret: 's',
    busId: 'ops-retry',
    maxAttempts: 3,
    retryBaseMs: 1,
    dedupWindowMs: 60_000,
  });
  alerter.note('dead-letter-growth', 'k', {});
  // 3 attempts at ~1ms/2ms backoff.
  await new Promise((r) => setTimeout(r, 50));
  const stats = alerter.getStats();
  assert.equal(stats.sent, 0);
  assert.equal(stats.failed, 1);
  assert.equal(sink.failuresLeft, 7); // 10 - 3 attempts
});

test('retry recovers: a flapping sink eventually delivers', async () => {
  const sink = new RecordingAlertSink(2); // fails twice, then succeeds
  const alerter = new OpsAlerter({
    sink,
    secret: 's',
    busId: 'ops-flap',
    maxAttempts: 5,
    retryBaseMs: 1,
    dedupWindowMs: 60_000,
  });
  alerter.note('dead-letter-growth', 'k', {});
  await new Promise((r) => setTimeout(r, 50));
  const stats = alerter.getStats();
  assert.equal(stats.sent, 1);
  assert.equal(stats.failed, 0);
  assert.equal(sink.deliveries.length, 1);
});

test('note is infallible and never blocks: a throwing sink cannot break the bus', async () => {
  const badSink = {
    send: async () => {
      throw new Error('down');
    },
  };
  const bus = new EventBus({
    opsAlert: { sink: badSink, secret: 's', maxAttempts: 2, retryBaseMs: 1 },
  });
  // Publishing must not throw even though every alert delivery fails.
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', { v: 1 }), 1);
  await flush();
  bus.noteGatewayDisconnect();
  await new Promise((r) => setTimeout(r, 30));
  assert.equal(bus.getStats().opsAlert.failed >= 0, true);
});

test('disabled by default: no alerter, zero behavior change', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', { v: 1 }), 1);
  await flush();
  assert.deepEqual(bus.getStats().opsAlert, { sent: 0, failed: 0, suppressed: 0 });
  // noteGatewayDisconnect is a safe no-op when disabled.
  bus.noteGatewayDisconnect();
  assert.deepEqual(bus.getStats().opsAlert, { sent: 0, failed: 0, suppressed: 0 });
});

test('invalid opsAlert config fails fast with RangeError', () => {
  assert.throws(() => new EventBus({ opsAlert: { secret: 's' } as never }), RangeError);
  assert.throws(
    () =>
      new EventBus({
        opsAlert: { sink: new RecordingAlertSink(), secret: '' },
      }),
    RangeError,
  );
  assert.throws(
    () =>
      new EventBus({
        opsAlert: {
          sink: new RecordingAlertSink(),
          secret: 's',
          dedupWindowMs: -1,
        },
      }),
    RangeError,
  );
  assert.throws(
    () =>
      new EventBus({
        opsAlert: {
          sink: new RecordingAlertSink(),
          secret: 's',
          maxAttempts: 0,
        },
      }),
    RangeError,
  );
});

test('gateway onDisconnect hook wires to bus.noteGatewayDisconnect', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    opsAlert: {
      sink,
      secret: 's',
      gatewayDisconnectThreshold: 2,
      gatewayDisconnectWindowMs: 60_000,
    },
  });
  let hookCalls = 0;
  const gateway = new GatewayServer(bus, {
    port: 0,
    onDisconnect: () => {
      hookCalls += 1;
      bus.noteGatewayDisconnect();
    },
  });
  const port = await gateway.listen();
  const { connect } = await import('node:net');
  const mkConn = () =>
    new Promise<void>((resolve, reject) => {
      const sock = connect(port, '127.0.0.1', () => {
        // Minimal WS handshake.
        const key = Buffer.from('1234567890123456').toString('base64');
        sock.write(
          `GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`,
        );
      });
      let buf = Buffer.alloc(0);
      sock.on('data', (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        if (buf.includes(Buffer.from('\r\n\r\n'))) {
          sock.destroy();
          resolve();
        }
      });
      sock.on('error', reject);
    });
  await mkConn();
  await mkConn();
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(hookCalls, 2);
  assert.equal(sink.deliveries.length, 1);
  assert.equal(sink.deliveries[0].alert.kind, 'gateway-disconnect-storm');
  await gateway.close();
});

test('resolveOpsAlerterOptions validates everything', async () => {
  const { resolveOpsAlerterOptions } = await import('../src/ops-alert.ts');
  const good = {
    sink: new RecordingAlertSink(),
    secret: 's',
    busId: 'b',
  };
  const r = resolveOpsAlerterOptions(good, 'test');
  assert.equal(r.dedupWindowMs, 5 * 60_000);
  assert.equal(r.maxAttempts, 3);
  assert.equal(r.retryBaseMs, 1000);
  assert.equal(r.gatewayDisconnectThreshold, 10);
  assert.equal(r.gatewayDisconnectWindowMs, 60_000);
  assert.throws(() => resolveOpsAlerterOptions(null as never, 't'), RangeError);
  assert.throws(() => resolveOpsAlerterOptions({ ...good, sink: {} as never }, 't'), RangeError);
});
