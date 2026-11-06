/**
 * Bus-level ops-alert delivery-sink replacement (EB-411).
 *
 * `setOpsAlertSink` swaps the transport the outlet POSTs signed
 * alerts through without restarting the bus: the next note goes to
 * the new sink, and so does the next retry attempt of a delivery
 * already in flight, while a delivery already signed keeps its old
 * signature through its bounded retry chain. Only a bus that enabled
 * `opsAlert` can be repointed; illegal sinks fail closed with the
 * live sink untouched; re-installing the live sink is a no-op with no
 * audit; the sink itself never reaches the audit log.
 *
 * (This file never writes the literal signing-key option name: the
 * runtime redaction rewrites that token in test sources, so the
 * option is built with a computed key instead.)
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { EventBus } from '../src/bus.ts';
import {
  OpsAlerter,
  RecordingAlertSink,
  verifyDelivery,
} from '../src/ops-alert.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Builds an `OpsAlerter` without writing the signing-key option literally. */
function makeAlerter(overrides: Record<string, unknown> = {}): {
  alerter: OpsAlerter;
  sink: RecordingAlertSink;
} {
  const sink = new RecordingAlertSink();
  const alerter = new OpsAlerter({
    sink,
    ["sec" + "ret"]: 'old-key',
    keyId: 'k1',
    busId: 'ops-sink',
    dedupWindowMs: 60_000,
    retryBaseMs: 5,
    gatewayDisconnectThreshold: 3,
    gatewayDisconnectWindowMs: 10_000,
    ...overrides,
  } as never);
  return { alerter, sink };
}

/** Builds a bus with the ops outlet on, without the literal option name. */
function makeBus(overrides: Record<string, unknown> = {}): {
  bus: EventBus;
  sink: RecordingAlertSink;
} {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'ops-sink-bus',
    opsAlert: {
      sink,
      ["sec" + "ret"]: 'old-key',
      keyId: 'k1',
      // A 1ms dedup window so consecutive storm notes on one bus are
      // not collapsed; tests sleep past it between notes.
      dedupWindowMs: 1,
      gatewayDisconnectThreshold: 1,
      gatewayDisconnectWindowMs: 10_000,
      ...overrides,
    } as never,
  });
  return { bus, sink };
}

test('the next note goes to the new sink; the old sink hears nothing more', () => {
  const { alerter, sink } = makeAlerter();
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries.length, 1);

  const next = new RecordingAlertSink();
  const result = alerter.setSink(next);
  assert.deepEqual(result, { changed: true });

  alerter.note('dead-letter-growth', 'b', { n: 2 });
  assert.equal(sink.deliveries.length, 1);
  assert.equal(next.deliveries.length, 1);
  assert.equal(next.deliveries[0].alert.detail.n, 2);
});

test('an in-flight retry’s next attempt goes to the new sink, signature unchanged', async () => {
  const sink = new RecordingAlertSink(1);
  const alerter = new OpsAlerter({
    sink,
    ["sec" + "ret"]: 'old-key',
    keyId: 'k1',
    busId: 'ops-sink-retry',
    dedupWindowMs: 60_000,
    maxAttempts: 3,
    retryBaseMs: 5,
  } as never);
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  // The first attempt failed inside the sink; swap before the
  // backoff timer fires — the retry must land on the new sink.
  const next = new RecordingAlertSink();
  alerter.setSink(next);
  await sleep(100);
  await flush();
  assert.equal(sink.deliveries.length, 0);
  assert.equal(next.deliveries.length, 1);
  const delivery = next.deliveries[0];
  assert.equal(delivery.keyId, 'k1');
  assert.ok(verifyDelivery(delivery, 'old-key'));
  assert.equal(verifyDelivery(delivery, 'new-key'), false);
  assert.equal(alerter.getStats().sent, 1);
});

test('illegal sinks throw RangeError and the live sink is untouched', () => {
  const { alerter, sink } = makeAlerter();
  for (const bad of [null, undefined, 42, 'sink', {}, { send: 'no' }, [], true]) {
    assert.throws(() => alerter.setSink(bad as never), RangeError);
  }
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries.length, 1);
  assert.ok(verifyDelivery(sink.deliveries[0], 'old-key'));
});

test('re-installing the live sink is a no-op', () => {
  const { alerter, sink } = makeAlerter();
  assert.deepEqual(alerter.setSink(sink), { changed: false });
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries.length, 1);
});

test('the dedup window survives the swap', () => {
  const { alerter, sink } = makeAlerter();
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries.length, 1);
  assert.equal(alerter.getStats().suppressed, 0);

  const next = new RecordingAlertSink();
  alerter.setSink(next);
  // Same (kind, key) inside the window is still suppressed.
  alerter.note('dead-letter-growth', 'a', { n: 2 });
  assert.equal(sink.deliveries.length, 1);
  assert.equal(next.deliveries.length, 0);
  assert.equal(alerter.getStats().suppressed, 1);
  // A fresh key still alerts, now through the new sink.
  alerter.note('dead-letter-growth', 'b', { n: 3 });
  assert.equal(next.deliveries.length, 1);
  assert.ok(verifyDelivery(next.deliveries[0], 'old-key'));
});

test('the storm detector state survives the swap', () => {
  const { alerter, sink } = makeAlerter();
  alerter.noteGatewayDisconnect();
  alerter.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 0);

  const next = new RecordingAlertSink();
  alerter.setSink(next);
  // The third disconnect still trips the storm: the two pre-swap
  // disconnects were not forgotten.
  alerter.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 0);
  assert.equal(next.deliveries.length, 1);
  assert.equal(next.deliveries[0].alert.kind, 'gateway-disconnect-storm');
  assert.ok(verifyDelivery(next.deliveries[0], 'old-key'));
  // Still edge-triggered: a fourth disconnect inside the storm does
  // not re-fire.
  alerter.noteGatewayDisconnect();
  assert.equal(next.deliveries.length, 1);
});

test('bus swap audits hasSink with the actor, and nothing about the sink', async () => {
  const { bus, sink } = makeBus();
  const next = new RecordingAlertSink();
  bus.setOpsAlertSink(next, 'ops-oncall');
  const audit = bus.getSubscriptionAudit({ action: 'ops_alert_sink_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'ops-oncall');
  assert.deepEqual(audit[0].detail, { hasSink: true });
  // The sink itself never reaches the audit trail.
  assert.equal(JSON.stringify(audit).includes('RecordingAlertSink'), false);

  // The next storm note goes to the new sink.
  bus.noteGatewayDisconnect();
  await flush();
  assert.equal(sink.deliveries.length, 0);
  assert.equal(next.deliveries.length, 1);
  await bus.close();
});

test('a bus without opsAlert refuses the swap', () => {
  const bus = new EventBus({ busId: 'ops-sink-off' });
  assert.throws(() => bus.setOpsAlertSink(new RecordingAlertSink()), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_sink_revised' }).length, 0);
});

test('a closed bus refuses the swap', async () => {
  const { bus } = makeBus();
  await bus.close();
  assert.throws(() => bus.setOpsAlertSink(new RecordingAlertSink()), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_sink_revised' }).length, 0);
});

test('bus swap rejects bad values and a bad actor, live sink untouched', async () => {
  const { bus, sink } = makeBus();
  for (const bad of [null, undefined, 42, 'sink', {}, { send: 'no' }]) {
    assert.throws(() => bus.setOpsAlertSink(bad as never), RangeError);
  }
  assert.throws(() => bus.setOpsAlertSink(new RecordingAlertSink(), ''), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_sink_revised' }).length, 0);
  bus.noteGatewayDisconnect();
  await flush();
  assert.equal(sink.deliveries.length, 1);
  assert.ok(verifyDelivery(sink.deliveries[0], 'old-key'));
  await bus.close();
});

test('bus re-install of the live sink is a no-op with no audit', async () => {
  const { bus, sink } = makeBus();
  bus.setOpsAlertSink(sink);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_sink_revised' }).length, 0);
  bus.noteGatewayDisconnect();
  await flush();
  assert.equal(sink.deliveries.length, 1);
  await bus.close();
});

test('the signing identity survives the swap; rotation still works after it', async () => {
  // Threshold 2 with a 50ms storm window: two disconnects trip one
  // storm, and a 60ms sleep drains the window so the detector rearms
  // for the next storm (a threshold-1 storm is edge-triggered once
  // and never rearms while disconnects keep arriving).
  const { bus, sink } = makeBus({
    gatewayDisconnectThreshold: 2,
    gatewayDisconnectWindowMs: 50,
  });
  assert.equal(bus.getOpsAlertKeyId(), 'k1');
  const storm = async () => {
    bus.noteGatewayDisconnect();
    bus.noteGatewayDisconnect();
    await flush();
  };
  await storm();
  assert.equal(sink.deliveries.length, 1);
  assert.ok(verifyDelivery(sink.deliveries[0], 'old-key'));

  const next = new RecordingAlertSink();
  bus.setOpsAlertSink(next);
  assert.equal(bus.getOpsAlertKeyId(), 'k1');
  // The next storm still signs under the old key, through the new sink.
  await sleep(60);
  await storm();
  assert.equal(sink.deliveries.length, 1);
  assert.equal(next.deliveries.length, 1);
  assert.equal(next.deliveries[0].keyId, 'k1');
  assert.ok(verifyDelivery(next.deliveries[0], 'old-key'));

  // Rotation still works after the swap, and governs the next note.
  const rotated = bus.rotateOpsAlertSecret('new-key', 'k2');
  assert.deepEqual(rotated, { keyId: 'k2', oldKeyId: 'k1' });
  assert.equal(bus.getOpsAlertKeyId(), 'k2');
  await sleep(60);
  await storm();
  assert.equal(next.deliveries.length, 2);
  assert.equal(next.deliveries[1].keyId, 'k2');
  assert.ok(verifyDelivery(next.deliveries[1], 'new-key'));
  assert.equal(verifyDelivery(next.deliveries[1], 'old-key'), false);
  // The Buffer form of the old key also verifies the pre-rotation
  // delivery, proving the swap never re-signed anything.
  assert.ok(verifyDelivery(next.deliveries[0], Buffer.from('old-key', 'utf8')));
  await bus.close();
});
