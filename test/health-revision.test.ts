import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type DegradedEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`.
 * A "slow" handler simulates overrunning its processing budget by moving
 * the clock forward itself, keeping the timeout tests deterministic.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('setHealthProbe tightens the threshold for the next failure without touching the counter', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 3 }, onDegraded: (e) => events.push(e) },
  );

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 0, 'two failures stay under the original threshold of 3');
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: false,
    consecutiveFailures: 2,
  });

  bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 2 });
  // The revision itself counts nothing: the counter and the degraded
  // flag are untouched, only the threshold moves.
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: false,
    consecutiveFailures: 2,
  });

  bus.publish('t', 'c');
  await flush();
  assert.equal(events.length, 1, 'the next failure trips the tightened threshold');
  assert.equal(events[0].consecutiveFailures, 3);
  assert.equal(events[0].reason, 'error');

  const audit = bus.getSubscriptionAudit({ action: 'health_probe_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    healthProbe: { maxConsecutiveFailures: 2 },
    oldHealthProbe: { maxConsecutiveFailures: 3 },
  });
  sub.unsubscribe();
});

test('setHealthProbe applies a new processing timeout from the next handler call', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: DegradedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      clock.nowMs += 100; // slow, but no budget is configured yet
    },
    { healthProbe: { maxConsecutiveFailures: 1 }, onDegraded: (e) => events.push(e) },
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 0, 'without a budget a slow handler counts nothing');

  bus.setHealthProbe(sub.id, { processingTimeoutMs: 50 });
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 1, 'the next slow call overruns the new budget');
  assert.equal(events[0].reason, 'timeout');

  const audit = bus.getSubscriptionAudit({ action: 'health_probe_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    healthProbe: { maxConsecutiveFailures: 1, processingTimeoutMs: 50 },
    oldHealthProbe: { maxConsecutiveFailures: 1 },
  });
  sub.unsubscribe();
});

test('setHealthProbe applies a new cooldown from the next degradation, not the armed timer', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  let calls = 0;
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
      if (calls <= 2) throw new Error('boom');
    },
    {
      healthProbe: { maxConsecutiveFailures: 1, autoResumeAfterMs: 10_000 },
      onDegraded: (e) => events.push(e),
    },
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1, 'first failure degrades with the long cooldown armed');
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);

  // Revising while degraded is allowed: the counter and the flag stay,
  // and the already-armed 10s timer keeps its cooldown.
  bus.setHealthProbe(sub.id, { autoResumeAfterMs: 30 });
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 1);

  assert.equal(bus.resume(sub.id), true, 'manual resume cancels the armed timer');
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 2, 'the second failure degrades again');
  await sleep(80);
  assert.equal(
    bus.subscriberHealth(sub.id).degraded,
    false,
    'the second degradation auto-resumes on the revised 30ms cooldown',
  );
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 0);

  const audit = bus.getSubscriptionAudit({ action: 'health_probe_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    healthProbe: { maxConsecutiveFailures: 1, autoResumeAfterMs: 30 },
    oldHealthProbe: { maxConsecutiveFailures: 1, autoResumeAfterMs: 10_000 },
  });
  sub.unsubscribe();
});

test('setHealthProbe keeps omitted fields and still applies the revised threshold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: DegradedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      clock.nowMs += 100; // overruns the 50ms budget that must survive the revision
    },
    {
      healthProbe: { maxConsecutiveFailures: 5, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
      onDegraded: (e) => events.push(e),
    },
  );

  bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 1 });
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1, 'one timeout failure trips the revised threshold');
  assert.equal(events[0].reason, 'timeout', 'the kept 50ms budget still times the handler');

  const audit = bus.getSubscriptionAudit({ action: 'health_probe_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    healthProbe: { maxConsecutiveFailures: 1, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
    oldHealthProbe: { maxConsecutiveFailures: 5, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
  });
  sub.unsubscribe();
});

test('setHealthProbe is fail-closed for unknown, unprobed, or illegal values', async () => {
  const bus = new EventBus();
  const probed = bus.subscribe('t', () => {}, { healthProbe: { maxConsecutiveFailures: 3 } });
  const plain = bus.subscribe('u', () => {});

  assert.throws(() => bus.setHealthProbe('nope', { maxConsecutiveFailures: 1 }), RangeError);
  assert.throws(() => bus.setHealthProbe(plain.id, { maxConsecutiveFailures: 1 }), RangeError);
  assert.throws(() => bus.setHealthProbe('', { maxConsecutiveFailures: 1 }), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, { maxConsecutiveFailures: 0 }), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, { maxConsecutiveFailures: 1.5 }), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, { maxConsecutiveFailures: Number.NaN }), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, { processingTimeoutMs: 0 }), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, { processingTimeoutMs: -5 }), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, { processingTimeoutMs: Number.NaN }), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, { autoResumeAfterMs: Infinity }), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, undefined as never), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, 'x' as never), RangeError);
  assert.throws(() => bus.setHealthProbe(probed.id, [] as never), RangeError);

  assert.deepEqual(bus.subscriberHealth(probed.id), {
    subscriberId: probed.id,
    enabled: true,
    degraded: false,
    consecutiveFailures: 0,
  });
  assert.equal(bus.getSubscriptionAudit({ action: 'health_probe_revised' }).length, 0);

  // A failed revision leaves the probe usable: a valid revision still works.
  bus.setHealthProbe(probed.id, { maxConsecutiveFailures: 2 });
  assert.equal(bus.getSubscriptionAudit({ action: 'health_probe_revised' }).length, 1);
  probed.unsubscribe();
  plain.unsubscribe();
});

test('setHealthProbe null clears the probe, releases a degraded pause, and refuses later revisions', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  const received: unknown[] = [];
  let calls = 0;
  const sub = bus.subscribe(
    't',
    (msg) => {
      calls += 1;
      if (calls === 1) throw new Error('boom');
      received.push(msg.payload);
    },
    { healthProbe: { maxConsecutiveFailures: 1 }, onDegraded: (e) => events.push(e) },
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1, 'first failure degrades');
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);

  bus.setHealthProbe(sub.id, null);
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: false,
    degraded: false,
    consecutiveFailures: 0,
  });
  assert.equal(bus.getStats().degradedSubscribers, 0);

  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(received, ['b'], 'delivery flows again once the probe gate is gone');
  assert.equal(bus.getStats().degradedSubscribers, 0);

  assert.throws(() => bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 1 }), RangeError);

  const audit = bus.getSubscriptionAudit({ action: 'health_probe_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    healthProbe: null,
    oldHealthProbe: { maxConsecutiveFailures: 1 },
  });
  sub.unsubscribe();
});
