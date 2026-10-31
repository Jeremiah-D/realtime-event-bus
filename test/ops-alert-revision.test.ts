/**
 * Bus-level ops-alert revision (EB-307).
 *
 * `setOpsAlert` retunes the outlet live — the dedup window, the retry
 * budget and base, and the gateway-disconnect storm threshold and
 * window — without restarting the bus. Only a bus that enabled
 * `opsAlert` can be revised; illegal values fail closed with the live
 * tuning and the audit log untouched; an identical (or empty) revision
 * is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { OpsAlerter, RecordingAlertSink, verifyDelivery } from '../src/ops-alert.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setOpsAlert fails closed when opsAlert is not enabled', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setOpsAlert({ dedupWindowMs: 1000 }), RangeError);
  assert.throws(() => bus.setOpsAlert({ gatewayDisconnectThreshold: 2 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
});

test('setOpsAlert rejects illegal values and leaves the live tuning untouched', () => {
  let now = 1_000;
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'ops-rev-bad',
    now: () => now,
    opsAlert: {
      sink,
      secret: 's',
      gatewayDisconnectThreshold: 3,
      gatewayDisconnectWindowMs: 10_000,
    },
  });
  const bad: Array<unknown> = [
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
    'nope',
    42,
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setOpsAlert(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: the storm threshold is still 3.
  bus.noteGatewayDisconnect();
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 0);
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
  void now;
});

test('setOpsAlert with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus({
    opsAlert: { sink: new RecordingAlertSink(), secret: 's', maxAttempts: 3 },
  });
  bus.setOpsAlert({ maxAttempts: 3 });
  bus.setOpsAlert({ dedupWindowMs: 5 * 60_000 });
  bus.setOpsAlert({});
  bus.setOpsAlert(null as never);
  assert.equal(bus.getSubscriptionAudit({ action: 'ops_alert_revised' }).length, 0);
});

test('partial revision moves only the provided field', () => {
  const bus = new EventBus({
    opsAlert: { sink: new RecordingAlertSink(), secret: 's', maxAttempts: 3 },
  });
  bus.setOpsAlert({ retryBaseMs: 250 });
  const audit = bus.getSubscriptionAudit({ action: 'ops_alert_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    dedupWindowMs: 5 * 60_000,
    oldDedupWindowMs: 5 * 60_000,
    maxAttempts: 3,
    oldMaxAttempts: 3,
    retryBaseMs: 250,
    oldRetryBaseMs: 1000,
    gatewayDisconnectThreshold: 10,
    oldGatewayDisconnectThreshold: 10,
    gatewayDisconnectWindowMs: 60_000,
    oldGatewayDisconnectWindowMs: 60_000,
  });
});

test('threshold revision trips at the live threshold, identity untouched', () => {
  let now = 1_000;
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'ops-rev-threshold',
    now: () => now,
    opsAlert: {
      sink,
      secret: 's',
      keyId: 'ops-1',
      gatewayDisconnectThreshold: 3,
      gatewayDisconnectWindowMs: 10_000,
    },
  });
  bus.noteGatewayDisconnect();
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 0);
  bus.setOpsAlert({ gatewayDisconnectThreshold: 2 });
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1);
  assert.equal(sink.deliveries[0].alert.kind, 'gateway-disconnect-storm');
  assert.deepEqual(sink.deliveries[0].alert.detail, {
    disconnects: 3,
    windowMs: 10_000,
    threshold: 2,
  });
  // The revision moved no identity: the delivery still verifies under
  // the original secret and key id.
  assert.equal(sink.deliveries[0].keyId, 'ops-1');
  assert.ok(verifyDelivery(sink.deliveries[0], 's'));
  const audit = bus.getSubscriptionAudit({ action: 'ops_alert_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    dedupWindowMs: 5 * 60_000,
    oldDedupWindowMs: 5 * 60_000,
    maxAttempts: 3,
    oldMaxAttempts: 3,
    retryBaseMs: 1000,
    oldRetryBaseMs: 1000,
    gatewayDisconnectThreshold: 2,
    oldGatewayDisconnectThreshold: 3,
    gatewayDisconnectWindowMs: 10_000,
    oldGatewayDisconnectWindowMs: 10_000,
  });
  void now;
});

test('window shrink drops stale disconnects on the next note', () => {
  let now = 1_000;
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'ops-rev-window',
    now: () => now,
    opsAlert: {
      sink,
      secret: 's',
      gatewayDisconnectThreshold: 2,
      gatewayDisconnectWindowMs: 60_000,
    },
  });
  bus.noteGatewayDisconnect();
  now = 51_000;
  bus.setOpsAlert({ gatewayDisconnectWindowMs: 10_000 });
  bus.noteGatewayDisconnect();
  // The first disconnect (t=1000) is past the new 10s horizon, so the
  // live window holds only the second one — below the threshold of 2.
  assert.equal(sink.deliveries.length, 0);
  bus.noteGatewayDisconnect();
  assert.equal(sink.deliveries.length, 1);
});

test('dedup window shrink un-suppresses after the new horizon (dead-letter growth)', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'ops-rev-dedup',
    opsAlert: { sink, secret: 's', dedupWindowMs: 60_000 },
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
  await sleep(50);
  await flush();
  assert.equal(sink.deliveries.length, 1);
  assert.ok(bus.getStats().opsAlert.suppressed >= 2);
  // Shrink the window below the time since the last alert: the next
  // flood notes again instead of suppressing.
  bus.setOpsAlert({ dedupWindowMs: 1 });
  bus.publish('t', { v: 4 });
  bus.publish('t', { v: 5 });
  bus.publish('t', { v: 6 });
  await sleep(50);
  await flush();
  assert.equal(sink.deliveries.length, 2);
  assert.equal(sink.deliveries[1].alert.kind, 'dead-letter-growth');
});

test('alerter retune: dedup window governs the next note', async () => {
  let now = 1_000;
  const sink = new RecordingAlertSink();
  const alerter = new OpsAlerter({
    sink,
    secret: 's',
    busId: 'ops-rev-unit',
    now: () => now,
    dedupWindowMs: 60_000,
  });
  alerter.note('lag-slo-breach', 'k', {});
  await flush();
  assert.equal(sink.deliveries.length, 1);
  alerter.note('lag-slo-breach', 'k', {});
  await flush();
  assert.equal(sink.deliveries.length, 1);
  assert.equal(alerter.getStats().suppressed, 1);
  alerter.retune({ ...alerter.current(), dedupWindowMs: 1 });
  now = 1_002;
  alerter.note('lag-slo-breach', 'k', {});
  await flush();
  assert.equal(sink.deliveries.length, 2);
  assert.deepEqual(alerter.getStats(), { sent: 2, failed: 0, suppressed: 1 });
});

test('alerter retune: maxAttempts governs the next delivery budget', async () => {
  const sink = new RecordingAlertSink(2); // fails twice, then succeeds
  const alerter = new OpsAlerter({
    sink,
    secret: 's',
    busId: 'ops-rev-attempts',
    maxAttempts: 3,
    retryBaseMs: 1,
    dedupWindowMs: 60_000,
  });
  alerter.note('dead-letter-growth', 'k', {});
  await sleep(50);
  assert.deepEqual(alerter.getStats(), { sent: 1, failed: 0, suppressed: 0 });
  alerter.retune({ ...alerter.current(), maxAttempts: 1 });
  sink.failuresLeft = 1;
  alerter.note('dead-letter-growth', 'k2', {});
  await sleep(50);
  // One attempt only: the single failure exhausts the new budget.
  assert.deepEqual(alerter.getStats(), { sent: 1, failed: 1, suppressed: 0 });
});

test('alerter retune: retryBaseMs governs the next retry delay', async () => {
  const sink = new RecordingAlertSink(1); // fails once, then succeeds
  const alerter = new OpsAlerter({
    sink,
    secret: 's',
    busId: 'ops-rev-backoff',
    maxAttempts: 5,
    retryBaseMs: 1,
    dedupWindowMs: 60_000,
  });
  alerter.note('dead-letter-growth', 'k', {});
  await sleep(30);
  assert.equal(alerter.getStats().sent, 1);
  sink.failuresLeft = 1;
  alerter.retune({ ...alerter.current(), retryBaseMs: 300 });
  alerter.note('dead-letter-growth', 'k2', {});
  await sleep(50);
  // The retry is still pending: 50ms is far short of the new 300ms base.
  assert.equal(alerter.getStats().sent, 1);
  await sleep(400);
  assert.equal(alerter.getStats().sent, 2);
});

test('alerter retune rejects an illegal merged value and moves nothing', () => {
  const alerter = new OpsAlerter({
    sink: new RecordingAlertSink(),
    secret: 's',
    busId: 'ops-rev-retune-bad',
    maxAttempts: 3,
  });
  const before = alerter.current();
  assert.throws(() => alerter.retune({ ...before, maxAttempts: 0 }), RangeError);
  assert.throws(() => alerter.retune({ ...before, dedupWindowMs: Number.NaN }), RangeError);
  assert.deepEqual(alerter.current(), before);
});
