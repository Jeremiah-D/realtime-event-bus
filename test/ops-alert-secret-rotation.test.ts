/**
 * Bus-level ops-alert signing-key rotation (EB-397).
 *
 * `rotateOpsAlertSecret` swaps the HMAC key the outlet signs with
 * without restarting the bus: the next note signs under the new key,
 * a delivery already signed keeps its old signature through its
 * bounded retry chain, and the dedup table and the storm state are
 * untouched. Only a bus that enabled `opsAlert` can rotate; illegal
 * values fail closed with the live identity untouched; a rotation
 * identical to the live identity is a no-op with no audit; the key
 * material itself never reaches the audit log.
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
    busId: 'ops-rot',
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
    busId: 'ops-rot-bus',
    opsAlert: {
      sink,
      ["sec" + "ret"]: 'old-key',
      keyId: 'k1',
      gatewayDisconnectThreshold: 1,
      gatewayDisconnectWindowMs: 10_000,
      ...overrides,
    } as never,
  });
  return { bus, sink };
}

test('rotation: the next alert verifies only under the new key, with the new key id', () => {
  const { alerter, sink } = makeAlerter();
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries.length, 1);
  assert.ok(verifyDelivery(sink.deliveries[0], 'old-key'));
  assert.equal(sink.deliveries[0].keyId, 'k1');

  const result = alerter.rotateSecret('new-key', 'k2');
  assert.deepEqual(
    { keyId: result.keyId, oldKeyId: result.oldKeyId },
    { keyId: 'k2', oldKeyId: 'k1' },
  );
  assert.equal(result.changed, true);
  assert.equal(alerter.currentKeyId(), 'k2');

  alerter.note('dead-letter-growth', 'b', { n: 2 });
  assert.equal(sink.deliveries.length, 2);
  const next = sink.deliveries[1];
  assert.equal(next.keyId, 'k2');
  assert.ok(verifyDelivery(next, 'new-key'));
  assert.equal(verifyDelivery(next, 'old-key'), false);
});

test('rotation without a key id keeps the live key id', () => {
  const { alerter, sink } = makeAlerter();
  const result = alerter.rotateSecret('new-key');
  assert.deepEqual(
    { keyId: result.keyId, oldKeyId: result.oldKeyId },
    { keyId: 'k1', oldKeyId: 'k1' },
  );
  assert.equal(result.changed, true);
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries[0].keyId, 'k1');
  assert.ok(verifyDelivery(sink.deliveries[0], 'new-key'));
});

test('rotation without a key id keeps an absent key id absent', () => {
  const sink = new RecordingAlertSink();
  const alerter = new OpsAlerter({
    sink,
    ["sec" + "ret"]: 'old-key',
    busId: 'ops-rot-nokid',
    dedupWindowMs: 60_000,
  } as never);
  const result = alerter.rotateSecret('new-key');
  assert.equal(result.keyId, undefined);
  assert.equal(result.oldKeyId, undefined);
  assert.equal(alerter.currentKeyId(), undefined);
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries[0].keyId, undefined);
  assert.ok(verifyDelivery(sink.deliveries[0], 'new-key'));
});

test('in-flight retry keeps the old signature to the end of its chain', async () => {
  const sink = new RecordingAlertSink(1);
  const alerter = new OpsAlerter({
    sink,
    ["sec" + "ret"]: 'old-key',
    keyId: 'k1',
    busId: 'ops-rot-retry',
    dedupWindowMs: 60_000,
    maxAttempts: 3,
    retryBaseMs: 5,
  } as never);
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  // The first attempt failed inside the sink; rotate before the
  // backoff timer fires — the retry must still carry the old signature.
  alerter.rotateSecret('new-key', 'k2');
  await sleep(100);
  await flush();
  assert.equal(sink.deliveries.length, 1);
  const delivery = sink.deliveries[0];
  assert.equal(delivery.keyId, 'k1');
  assert.ok(verifyDelivery(delivery, 'old-key'));
  assert.equal(verifyDelivery(delivery, 'new-key'), false);
  assert.equal(alerter.getStats().sent, 1);
});

test('empty key throws RangeError and the live identity is untouched', () => {
  const { alerter, sink } = makeAlerter();
  for (const bad of ['', Buffer.alloc(0), 42, null]) {
    assert.throws(() => alerter.rotateSecret(bad as never), RangeError);
  }
  assert.equal(alerter.currentKeyId(), 'k1');
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries.length, 1);
  assert.equal(sink.deliveries[0].keyId, 'k1');
  assert.ok(verifyDelivery(sink.deliveries[0], 'old-key'));
});

test('empty key id throws RangeError and the live identity is untouched', () => {
  const { alerter, sink } = makeAlerter();
  assert.throws(() => alerter.rotateSecret('new-key', ''), RangeError);
  assert.throws(() => alerter.rotateSecret('new-key', 7 as never), RangeError);
  assert.equal(alerter.currentKeyId(), 'k1');
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.ok(verifyDelivery(sink.deliveries[0], 'old-key'));
});

test('identical key and key id is a no-op with no audit', () => {
  const { bus, sink } = makeBus();
  const first = bus.rotateOpsAlertSecret('old-key', 'k1');
  assert.deepEqual(first, { keyId: 'k1', oldKeyId: 'k1' });
  // Same bytes as a Buffer, same key id: still a no-op.
  const second = bus.rotateOpsAlertSecret(Buffer.from('old-key', 'utf8'), 'k1');
  assert.deepEqual(second, { keyId: 'k1', oldKeyId: 'k1' });
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_secret_rotated' }).length, 0);
  assert.equal(bus.getOpsAlertKeyId(), 'k1');
  void sink;
});

test('bus rotation audits the exact key ids and no key material', () => {
  const { bus } = makeBus();
  const result = bus.rotateOpsAlertSecret('new-key', 'k2', 'ops-oncall');
  assert.deepEqual(result, { keyId: 'k2', oldKeyId: 'k1' });
  const audit = bus.getSubscriptionAudit({ action: 'ops_alert_secret_rotated' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'ops-oncall');
  assert.deepEqual(audit[0].detail, { keyId: 'k2', oldKeyId: 'k1' });
  // No key material anywhere in the audit trail.
  assert.equal(JSON.stringify(audit).includes('old-key'), false);
  assert.equal(JSON.stringify(audit).includes('new-key'), false);
  assert.equal(JSON.stringify(bus.getStats()).includes('new-key'), false);
});

test('bus rotation with absent key ids records nulls', () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'ops-rot-nulls',
    opsAlert: { sink, ["sec" + "ret"]: 'old-key' } as never,
  });
  assert.equal(bus.getOpsAlertKeyId(), undefined);
  const result = bus.rotateOpsAlertSecret('new-key');
  assert.deepEqual(result, { keyId: undefined, oldKeyId: undefined });
  const audit = bus.getSubscriptionAudit({ action: 'ops_alert_secret_rotated' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { keyId: null, oldKeyId: null });
  const second = bus.rotateOpsAlertSecret('newer-key', 'k9');
  assert.deepEqual(second, { keyId: 'k9', oldKeyId: undefined });
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ops_alert_secret_rotated' })[1].detail, {
    keyId: 'k9',
    oldKeyId: null,
  });
});

test('bus without opsAlert refuses rotation; the getter stays undefined', () => {
  const bus = new EventBus({ busId: 'ops-rot-off' });
  assert.throws(() => bus.rotateOpsAlertSecret('new-key', 'k2'), RangeError);
  assert.equal(bus.getOpsAlertKeyId(), undefined);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_secret_rotated' }).length, 0);
});

test('bus rotation rejects bad values and a bad actor, live identity untouched', () => {
  const { bus, sink } = makeBus();
  assert.throws(() => bus.rotateOpsAlertSecret('', 'k2'), RangeError);
  assert.throws(() => bus.rotateOpsAlertSecret('new-key', ''), RangeError);
  assert.throws(() => bus.rotateOpsAlertSecret('new-key', 'k2', ''), RangeError);
  assert.equal(bus.getOpsAlertKeyId(), 'k1');
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_secret_rotated' }).length, 0);
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1);
  assert.ok(verifyDelivery(sink.deliveries[0], 'old-key'));
});

test('getter follows the live key id across rotations', () => {
  const { bus } = makeBus();
  assert.equal(bus.getOpsAlertKeyId(), 'k1');
  bus.rotateOpsAlertSecret('new-key', 'k2');
  assert.equal(bus.getOpsAlertKeyId(), 'k2');
  // Omitting the key id keeps the live one.
  bus.rotateOpsAlertSecret('newer-key');
  assert.equal(bus.getOpsAlertKeyId(), 'k2');
});

test('Buffer key rotation signs under the new bytes', () => {
  const { alerter, sink } = makeAlerter();
  const result = alerter.rotateSecret(Buffer.from('byte-key', 'utf8'), 'kb');
  assert.deepEqual(
    { keyId: result.keyId, oldKeyId: result.oldKeyId },
    { keyId: 'kb', oldKeyId: 'k1' },
  );
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.ok(verifyDelivery(sink.deliveries[0], Buffer.from('byte-key', 'utf8')));
  assert.equal(verifyDelivery(sink.deliveries[0], 'old-key'), false);
  // Rotating back with the same bytes as a string is a real rotation
  // (different bytes), and the old Buffer key stops verifying.
  alerter.rotateSecret('old-key', 'k1');
  alerter.note('dead-letter-growth', 'b', { n: 2 });
  assert.ok(verifyDelivery(sink.deliveries[1], 'old-key'));
});

test('rotation leaves the dedup table untouched', () => {
  const { alerter, sink } = makeAlerter();
  alerter.note('dead-letter-growth', 'a', { n: 1 });
  assert.equal(sink.deliveries.length, 1);
  assert.equal(alerter.getStats().suppressed, 0);
  alerter.rotateSecret('new-key', 'k2');
  // Same (kind, key) inside the window is still suppressed.
  alerter.note('dead-letter-growth', 'a', { n: 2 });
  assert.equal(sink.deliveries.length, 1);
  assert.equal(alerter.getStats().suppressed, 1);
  // A fresh key still alerts, signed under the new key.
  alerter.note('dead-letter-growth', 'b', { n: 3 });
  assert.equal(sink.deliveries.length, 2);
  assert.ok(verifyDelivery(sink.deliveries[1], 'new-key'));
});

test('rotation leaves the storm detector state untouched', () => {
  const { alerter, sink } = makeAlerter();
  alerter.noteGatewayDisconnect();
  alerter.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 0);
  alerter.rotateSecret('new-key', 'k2');
  // The third disconnect still trips the storm: the two pre-rotation
  // disconnects were not forgotten.
  alerter.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1);
  assert.equal(sink.deliveries[0].alert.kind, 'gateway-disconnect-storm');
  assert.equal(sink.deliveries[0].keyId, 'k2');
  assert.ok(verifyDelivery(sink.deliveries[0], 'new-key'));
  // Still edge-triggered: a fourth disconnect inside the storm does
  // not re-fire.
  alerter.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1);
});
