/**
 * Bus-level default drain timeout (EB-405).
 *
 * `setDefaultDrainTimeout` revises the timeout a graceful drain falls
 * back to when the caller omits `drainTimeoutMs` — for both
 * `unsubscribe({ drain: true })` (EB-128) and `close()` (EB-154) — live,
 * without restarting the bus. The factory default is 30000; an explicit
 * per-call value always wins over the bus default at either call site;
 * a drain already armed keeps the timer it was armed with; the default
 * must be a positive finite number (`0` stays a per-call-only value
 * for `close()`); a revision identical to the live default is a no-op
 * with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * A reliable subscription with one delivery that never settles: the
 * only way its drain can end is the drain timeout. Returns the bus,
 * the subscription, and a delivery counter.
 */
function unackedBus(): {
  bus: EventBus;
  sub: ReturnType<EventBus['subscribeReliable']>;
  deliveries: () => number;
} {
  const bus = new EventBus();
  let deliveries = 0;
  const sub = bus.subscribeReliable(
    'drain.default',
    () => {
      deliveries += 1;
      // Never ack: the drain can only end via the timeout.
    },
    { ackTimeoutMs: 60_000 },
  );
  return { bus, sub, deliveries: () => deliveries };
}

async function armUnacked(t: {
  bus: EventBus;
  sub: ReturnType<EventBus['subscribeReliable']>;
  deliveries: () => number;
}): Promise<void> {
  t.bus.publish('drain.default', { n: 1 });
  await flush();
  assert.equal(t.deliveries(), 1);
  assert.equal(t.bus.unackedCount(t.sub.id), 1);
}

test('the factory default is 30000 and a new unsubscribe-drain arms at it', async () => {
  const t = unackedBus();
  assert.equal(t.bus.getDefaultDrainTimeout(), 30_000);
  await armUnacked(t);
  t.sub.unsubscribe({ drain: true });
  assert.equal(t.bus.getStats().drainingSubscribers, 1);
  // Still inside the 30000ms factory window: the drain must not force.
  await sleep(150);
  await flush();
  assert.equal(
    t.bus.getStats().drainingSubscribers,
    1,
    'a drain armed at the factory default must not force early',
  );
  // Forced teardown cleans up: a second unsubscribe abandons the drain.
  t.sub.unsubscribe();
  assert.equal(t.bus.getStats().drainingSubscribers, 0);
});

test('revising the default arms new unsubscribe-drains at the new value', async () => {
  const t = unackedBus();
  t.bus.setDefaultDrainTimeout(60);
  assert.equal(t.bus.getDefaultDrainTimeout(), 60);
  await armUnacked(t);
  t.sub.unsubscribe({ drain: true });
  assert.equal(t.bus.getStats().drainingSubscribers, 1);
  await sleep(150);
  await flush();
  assert.equal(
    t.bus.getStats().drainingSubscribers,
    0,
    'a drain started after the revision must force at the new default',
  );
  t.sub.unsubscribe();
});

test('revising the default arms a new close() watchdog at the new value', async () => {
  const t = unackedBus();
  t.bus.setDefaultDrainTimeout(60);
  await armUnacked(t);
  const started = Date.now();
  const result = await t.bus.close();
  assert.equal(result.drained, false);
  assert.equal(result.unackedDeliveries, 1);
  assert.ok(
    Date.now() - started < 2000,
    'a close started after the revision must force at the new default',
  );
});

test('the factory default keeps close() waiting', async () => {
  const t = unackedBus();
  assert.equal(t.bus.getDefaultDrainTimeout(), 30_000);
  await armUnacked(t);
  // Race the close against a short sleep: at the 30s factory default
  // the close must still be pending. The watchdog is unref'd, so the
  // never-awaited close promise cannot hold the test process open.
  const outcome = await Promise.race([
    t.bus.close().then(() => 'closed' as const),
    sleep(200).then(() => 'still-open' as const),
  ]);
  assert.equal(outcome, 'still-open', 'a close armed at the factory default must not force early');
});

test('an explicit drainTimeoutMs wins over the bus default for unsubscribe, both directions', async () => {
  {
    // A long per-call value beats a tightened bus default.
    const t = unackedBus();
    t.bus.setDefaultDrainTimeout(60);
    await armUnacked(t);
    t.sub.unsubscribe({ drain: true, drainTimeoutMs: 5000 });
    await sleep(150);
    await flush();
    assert.equal(
      t.bus.getStats().drainingSubscribers,
      1,
      'an explicit drainTimeoutMs must beat a tightened bus default',
    );
    t.sub.unsubscribe();
  }
  {
    // A short per-call value beats a widened bus default.
    const t = unackedBus();
    t.bus.setDefaultDrainTimeout(30_000);
    await armUnacked(t);
    t.sub.unsubscribe({ drain: true, drainTimeoutMs: 60 });
    await sleep(150);
    await flush();
    assert.equal(
      t.bus.getStats().drainingSubscribers,
      0,
      'an explicit drainTimeoutMs must beat a widened bus default',
    );
    t.sub.unsubscribe();
  }
});

test('an explicit drainTimeoutMs wins over the bus default for close(), both directions', async () => {
  {
    // A long per-call value beats a tightened bus default.
    const t = unackedBus();
    t.bus.setDefaultDrainTimeout(60);
    await armUnacked(t);
    const outcome = await Promise.race([
      t.bus.close({ drainTimeoutMs: 5000 }).then(() => 'closed' as const),
      sleep(200).then(() => 'still-open' as const),
    ]);
    assert.equal(outcome, 'still-open', 'an explicit close drainTimeoutMs must beat a tightened bus default');
  }
  {
    // A short per-call value beats the factory default.
    const t = unackedBus();
    await armUnacked(t);
    const started = Date.now();
    const result = await t.bus.close({ drainTimeoutMs: 60 });
    assert.equal(result.drained, false);
    assert.ok(Date.now() - started < 2000, 'an explicit close drainTimeoutMs must beat the factory default');
  }
});

test('a default revision never moves an already-armed unsubscribe drain', async () => {
  {
    // Drain armed at 60ms, then the default widens to 30s: the armed
    // timer still forces at ~60ms.
    const t = unackedBus();
    t.bus.setDefaultDrainTimeout(60);
    await armUnacked(t);
    t.sub.unsubscribe({ drain: true });
    t.bus.setDefaultDrainTimeout(30_000);
    await sleep(150);
    await flush();
    assert.equal(
      t.bus.getStats().drainingSubscribers,
      0,
      'widening the default must not stretch an armed drain timer',
    );
    t.sub.unsubscribe();
  }
  {
    // Drain armed at the 30s factory default, then the default
    // tightens to 60ms: the armed timer still waits past 150ms.
    const t = unackedBus();
    await armUnacked(t);
    t.sub.unsubscribe({ drain: true });
    t.bus.setDefaultDrainTimeout(60);
    await sleep(150);
    await flush();
    assert.equal(
      t.bus.getStats().drainingSubscribers,
      1,
      'tightening the default must not shorten an armed drain timer',
    );
    t.sub.unsubscribe();
  }
});

test('a default revision never moves an already-armed close watchdog', async () => {
  {
    // Close armed at a 60ms default, then the default widens: the
    // watchdog still forces at ~60ms.
    const t = unackedBus();
    t.bus.setDefaultDrainTimeout(60);
    await armUnacked(t);
    const closing = t.bus.close();
    t.bus.setDefaultDrainTimeout(30_000);
    const started = Date.now();
    const result = await closing;
    assert.equal(result.drained, false);
    assert.ok(Date.now() - started < 2000, 'widening the default must not stretch an armed close watchdog');
  }
  {
    // Close armed at the 30s factory default, then the default
    // tightens to 60ms: the watchdog still waits past 200ms.
    const t = unackedBus();
    await armUnacked(t);
    const closing = t.bus.close();
    t.bus.setDefaultDrainTimeout(60);
    const outcome = await Promise.race([
      closing.then(() => 'closed' as const),
      sleep(200).then(() => 'still-open' as const),
    ]);
    assert.equal(outcome, 'still-open', 'tightening the default must not shorten an armed close watchdog');
  }
});

test('setDefaultDrainTimeout rejects illegal values fail-closed', () => {
  const bus = new EventBus();
  bus.setDefaultDrainTimeout(60_000);
  for (const timeoutMs of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => bus.setDefaultDrainTimeout(timeoutMs), RangeError, `timeoutMs=${timeoutMs}`);
  }
  assert.throws(() => bus.setDefaultDrainTimeout('30000' as never), RangeError);
  assert.throws(() => bus.setDefaultDrainTimeout(100, ''), RangeError, 'an illegal actor throws');
  // The live default and the audit log are untouched: only the one
  // valid setup revision is recorded.
  assert.equal(bus.getDefaultDrainTimeout(), 60_000);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_drain_timeout_revised' }).length,
    1,
  );
});

test('revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultDrainTimeout(30_000);
  assert.equal(bus.getSubscriptionAudit({ action: 'default_drain_timeout_revised' }).length, 0);
  bus.setDefaultDrainTimeout(60_000);
  bus.setDefaultDrainTimeout(60_000);
  const audit = bus.getSubscriptionAudit({ action: 'default_drain_timeout_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous_timeout_ms: 30_000, timeout_ms: 60_000 });
});

test('setDefaultDrainTimeout audits each revision with its old and new value', () => {
  const bus = new EventBus();
  bus.setDefaultDrainTimeout(60_000, 'ops');
  bus.setDefaultDrainTimeout(5000, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'default_drain_timeout_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous_timeout_ms: 30_000, timeout_ms: 60_000 });
  assert.deepEqual(events[1].detail, { previous_timeout_ms: 60_000, timeout_ms: 5000 });
});

test('the getter follows the live value and setting 30000 restores the factory default', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDrainTimeout(), 30_000);
  bus.setDefaultDrainTimeout(60);
  assert.equal(bus.getDefaultDrainTimeout(), 60);
  bus.setDefaultDrainTimeout(30_000);
  assert.equal(bus.getDefaultDrainTimeout(), 30_000);
  // Restored: a new drain arms at 30000 again.
  let deliveries = 0;
  const sub = bus.subscribeReliable(
    'drain.default',
    () => {
      deliveries += 1;
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('drain.default', { n: 1 });
  await flush();
  assert.equal(deliveries, 1);
  sub.unsubscribe({ drain: true });
  await sleep(150);
  await flush();
  assert.equal(
    bus.getStats().drainingSubscribers,
    1,
    'a drain started after restoring the factory default must not force early',
  );
  sub.unsubscribe();
});

test('close per-call 0 stays legal after a default revision', async () => {
  const bus = new EventBus();
  bus.setDefaultDrainTimeout(60);
  let deliveries = 0;
  bus.subscribeReliable(
    'drain.default',
    () => {
      deliveries += 1;
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('drain.default', { n: 1 });
  await flush();
  assert.equal(deliveries, 1);
  // 0 is illegal as a bus default but stays legal as a close()
  // per-call value: an immediate forced teardown.
  const result = await bus.close({ drainTimeoutMs: 0 });
  assert.equal(result.drained, false);
  assert.equal(result.unackedDeliveries, 1);
});

test('unsubscribe drainTimeoutMs still goes through the per-call validation', () => {
  const bus = new EventBus();
  bus.setDefaultDrainTimeout(60);
  const sub = bus.subscribe('drain.validate', () => {});
  assert.throws(() => sub.unsubscribe({ drain: true, drainTimeoutMs: 0 }), RangeError);
  assert.throws(() => sub.unsubscribe({ drain: true, drainTimeoutMs: -1 }), RangeError);
  // The failed calls changed nothing: the subscription still works and
  // is not draining, and the live default is untouched.
  assert.equal(bus.getDefaultDrainTimeout(), 60);
  assert.equal(bus.getStats().drainingSubscribers, 0);
  sub.unsubscribe();
});
