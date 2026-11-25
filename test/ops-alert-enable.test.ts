/**
 * Bus-level ops-alert live enable (EB-558).
 *
 * `setOpsAlert` installs the outlet on a bus that never enabled
 * `opsAlert` — the `sink` and the signing key are required, the five
 * numeric bounds fall back to the construction defaults, and the
 * enable is audited as `ops_alert_revised` with every `old*` value
 * `null`. A failed enable leaves the bus unconfigured, so a later
 * valid call still enables it; `null` on a never-enabled bus is a
 * silent no-op; a closed bus refuses; and the enable is live-only —
 * a restart restores the unconfigured state.
 *
 * (This file never writes the literal signing-key option name: the
 * runtime redaction rewrites that token in test sources, so the
 * option is built with a computed key instead.)
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { EventBus } from '../src/bus.ts';
import { RecordingAlertSink, verifyDelivery } from '../src/ops-alert.ts';

/** Builds an enable patch without writing the signing-key option literally. */
function enableOptions(
  sink: RecordingAlertSink,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    sink,
    ["sec" + "ret"]: 'enable-key',
    ...overrides,
  };
}

test('setOpsAlert with sink and signing key enables a never-enabled bus with construction defaults', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-defaults' });
  try {
    bus.setOpsAlert(enableOptions(sink, { keyId: 'ops-1' }) as never);
    const audit = bus.getSubscriptionAudit({ action: 'ops_alert_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      dedupWindowMs: 5 * 60_000,
      oldDedupWindowMs: null,
      maxAttempts: 3,
      oldMaxAttempts: null,
      retryBaseMs: 1000,
      oldRetryBaseMs: null,
      gatewayDisconnectThreshold: 10,
      oldGatewayDisconnectThreshold: null,
      gatewayDisconnectWindowMs: 60_000,
      oldGatewayDisconnectWindowMs: null,
    });
    assert.equal(bus.getOpsAlertKeyId(), 'ops-1');
    assert.deepEqual(bus.getStats().opsAlert, { sent: 0, failed: 0, suppressed: 0 });
  } finally {
    await bus.close();
  }
});

test('the first note after enable goes to the new sink, signed and verifiable', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-note' });
  try {
    bus.setOpsAlert(
      enableOptions(sink, { keyId: 'ops-1', gatewayDisconnectThreshold: 2 }) as never,
    );
    bus.noteGatewayDisconnect();
    assert.equal(sink.deliveries.length, 0);
    bus.noteGatewayDisconnect();
    assert.equal(sink.deliveries.length, 1);
    const delivery = sink.deliveries[0];
    assert.equal(delivery.alert.kind, 'gateway-disconnect-storm');
    assert.equal(delivery.alert.busId, 'ops-enable-note');
    assert.equal(delivery.keyId, 'ops-1');
    assert.ok(verifyDelivery(delivery, 'enable-key'));
    assert.equal(verifyDelivery(delivery, 'wrong-key'), false);
  } finally {
    await bus.close();
  }
});

test('enable without a sink refuses and the bus stays unconfigured', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-nosink' });
  try {
    assert.throws(() => bus.setOpsAlert({ ["sec" + "ret"]: 'k' } as never), RangeError);
    assert.throws(() => bus.setOpsAlert({ dedupWindowMs: 1000 }), RangeError);
    assert.throws(() => bus.setOpsAlert({}), RangeError);
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
    assert.equal(bus.getOpsAlertKeyId(), undefined);
    // A later valid call still enables it.
    bus.setOpsAlert(enableOptions(sink) as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 1);
  } finally {
    await bus.close();
  }
});

test('enable without a signing key refuses and the bus stays unconfigured', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-nokey' });
  try {
    assert.throws(() => bus.setOpsAlert({ sink } as never), RangeError);
    assert.throws(
      () => bus.setOpsAlert({ sink, ["sec" + "ret"]: '' } as never),
      RangeError,
    );
    assert.throws(
      () => bus.setOpsAlert({ sink, ["sec" + "ret"]: Buffer.alloc(0) } as never),
      RangeError,
    );
    assert.throws(
      () =>
        bus.setOpsAlert({ sink, ["sec" + "ret"]: 'k', keyId: '' } as never),
      RangeError,
    );
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
    // A later valid call still enables it.
    bus.setOpsAlert(enableOptions(sink) as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 1);
  } finally {
    await bus.close();
  }
});

test('illegal numeric values refuse the enable and leave the bus unconfigured', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-badnum' });
  try {
    const bad: Array<Record<string, unknown>> = [
      { dedupWindowMs: 0 },
      { dedupWindowMs: -1 },
      { dedupWindowMs: Number.NaN },
      { dedupWindowMs: Infinity },
      { maxAttempts: 0 },
      { maxAttempts: 1.5 },
      { retryBaseMs: 0 },
      { retryBaseMs: Number.NaN },
      { gatewayDisconnectThreshold: 0 },
      { gatewayDisconnectThreshold: 2.5 },
      { gatewayDisconnectWindowMs: 0 },
      { gatewayDisconnectWindowMs: -100 },
    ];
    for (const patch of bad) {
      assert.throws(
        () => bus.setOpsAlert({ ...enableOptions(sink), ...patch } as never),
        RangeError,
        `expected RangeError for ${JSON.stringify(patch)}`,
      );
    }
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
    assert.equal(bus.getOpsAlertKeyId(), undefined);
    // A later valid call still enables it.
    bus.setOpsAlert(enableOptions(sink) as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 1);
  } finally {
    await bus.close();
  }
});

test('a non-object or array enable refuses', async () => {
  const bus = new EventBus({ busId: 'ops-enable-nonobj' });
  try {
    for (const bad of ['nope', 42, [], true]) {
      assert.throws(() => bus.setOpsAlert(bad as never), RangeError);
    }
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
  } finally {
    await bus.close();
  }
});

test('null on a never-enabled bus is a silent no-op with no audit', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-null' });
  try {
    bus.setOpsAlert(null as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
    assert.equal(bus.getOpsAlertKeyId(), undefined);
    // The bus is still unconfigured — a later valid call enables it.
    bus.setOpsAlert(enableOptions(sink) as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 1);
  } finally {
    await bus.close();
  }
});

test('a closed bus refuses the enable and the revision', async () => {
  const sink = new RecordingAlertSink();
  const plain = new EventBus({ busId: 'ops-enable-closed' });
  await plain.close();
  assert.throws(
    () => plain.setOpsAlert(enableOptions(sink) as never),
    /bus is closed/,
  );
  assert.equal(plain.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);

  const enabled = new EventBus({ busId: 'ops-enable-closed-2' });
  enabled.setOpsAlert(enableOptions(sink) as never);
  await enabled.close();
  assert.throws(() => enabled.setOpsAlert({ maxAttempts: 5 }), /bus is closed/);
  assert.equal(enabled.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 1);
});

test('an identical or empty revision after enable is a no-op with no audit', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-noop' });
  try {
    bus.setOpsAlert(
      enableOptions(sink, { maxAttempts: 3, dedupWindowMs: 5 * 60_000 }) as never,
    );
    bus.setOpsAlert({ maxAttempts: 3 });
    bus.setOpsAlert({ dedupWindowMs: 5 * 60_000 });
    bus.setOpsAlert({});
    bus.setOpsAlert(null as never);
    assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 1);
  } finally {
    await bus.close();
  }
});

test('a revision after enable merges the live values', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-merge' });
  try {
    bus.setOpsAlert(enableOptions(sink, { maxAttempts: 3 }) as never);
    bus.setOpsAlert({ maxAttempts: 5, retryBaseMs: 250 });
    const audit = bus.getSubscriptionAudit({ action: 'ops_alert_revised' });
    assert.equal(audit.length, 2);
    assert.deepEqual(audit[1].detail, {
      dedupWindowMs: 5 * 60_000,
      oldDedupWindowMs: 5 * 60_000,
      maxAttempts: 5,
      oldMaxAttempts: 3,
      retryBaseMs: 250,
      oldRetryBaseMs: 1000,
      gatewayDisconnectThreshold: 10,
      oldGatewayDisconnectThreshold: 10,
      gatewayDisconnectWindowMs: 60_000,
      oldGatewayDisconnectWindowMs: 60_000,
    });
  } finally {
    await bus.close();
  }
});

test('a revision after enable cannot move the sink or the signing key', async () => {
  const sink = new RecordingAlertSink();
  const other = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-fixed' });
  try {
    bus.setOpsAlert(
      enableOptions(sink, { gatewayDisconnectThreshold: 1 }) as never,
    );
    // The sink and the key ride along in the patch but are not
    // revisable here — only the numeric bound moves.
    bus.setOpsAlert({
      sink: other,
      ["sec" + "ret"]: 'another-key',
      maxAttempts: 5,
    } as never);
    bus.noteGatewayDisconnect();
    assert.equal(sink.deliveries.length, 1);
    assert.equal(other.deliveries.length, 0);
    assert.ok(verifyDelivery(sink.deliveries[0], 'enable-key'));
    assert.equal(verifyDelivery(sink.deliveries[0], 'another-key'), false);
  } finally {
    await bus.close();
  }
});

test('rotateOpsAlertSecret and setOpsAlertSink work on a live-enabled alerter', async () => {
  let nowMs = 1_000;
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-live', now: () => nowMs });
  try {
    bus.setOpsAlert(
      enableOptions(sink, {
        keyId: 'k1',
        dedupWindowMs: 1,
        gatewayDisconnectThreshold: 2,
        gatewayDisconnectWindowMs: 10_000,
      }) as never,
    );
    // Rotate: the next storm signs under the new key.
    const rotated = bus.rotateOpsAlertSecret('new-key', 'k2');
    assert.deepEqual(rotated, { keyId: 'k2', oldKeyId: 'k1' });
    assert.equal(bus.getOpsAlertKeyId(), 'k2');
    bus.noteGatewayDisconnect();
    bus.noteGatewayDisconnect();
    assert.equal(sink.deliveries.length, 1);
    assert.ok(verifyDelivery(sink.deliveries[0], 'new-key'));
    assert.equal(verifyDelivery(sink.deliveries[0], 'enable-key'), false);
    // Swap the sink: the next storm goes to the new sink only.
    const next = new RecordingAlertSink();
    bus.setOpsAlertSink(next);
    nowMs += 11_000; // drain the storm window so the detector rearms
    bus.noteGatewayDisconnect();
    bus.noteGatewayDisconnect();
    assert.equal(sink.deliveries.length, 1);
    assert.equal(next.deliveries.length, 1);
    assert.ok(verifyDelivery(next.deliveries[0], 'new-key'));
  } finally {
    await bus.close();
  }
});

test('a restart restores the unconfigured state', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-restart' });
  bus.setOpsAlert(enableOptions(sink) as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 1);
  await bus.close();
  const restarted = new EventBus({ busId: 'ops-enable-restart' });
  try {
    assert.equal(restarted.getOpsAlertKeyId(), undefined);
    assert.equal(restarted.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
    assert.throws(() => restarted.setOpsAlert({ dedupWindowMs: 1000 }), RangeError);
  } finally {
    await restarted.close();
  }
});

test('enable accepts a Buffer signing key and explicit numerics', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({ busId: 'ops-enable-buffer' });
  try {
    const key = Buffer.from('buf-key');
    bus.setOpsAlert(
      enableOptions(sink, {
        ["sec" + "ret"]: key,
        keyId: 'ops-b',
        dedupWindowMs: 120_000,
        maxAttempts: 2,
        retryBaseMs: 500,
        gatewayDisconnectThreshold: 1,
        gatewayDisconnectWindowMs: 5_000,
      }) as never,
    );
    const audit = bus.getSubscriptionAudit({ action: 'ops_alert_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      dedupWindowMs: 120_000,
      oldDedupWindowMs: null,
      maxAttempts: 2,
      oldMaxAttempts: null,
      retryBaseMs: 500,
      oldRetryBaseMs: null,
      gatewayDisconnectThreshold: 1,
      oldGatewayDisconnectThreshold: null,
      gatewayDisconnectWindowMs: 5_000,
      oldGatewayDisconnectWindowMs: null,
    });
    assert.equal(bus.getOpsAlertKeyId(), 'ops-b');
    bus.noteGatewayDisconnect();
    assert.equal(sink.deliveries.length, 1);
    assert.ok(verifyDelivery(sink.deliveries[0], key));
  } finally {
    await bus.close();
  }
});
