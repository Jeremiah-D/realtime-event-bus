import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type DegradedEvent,
  type HealthProbeOptions,
} from '../src/bus.ts';

/**
 * Bus-level default health probe (EB-496):
 * `setDefaultHealthProbe` installs the `maxConsecutiveFailures` /
 * `processingTimeoutMs` / `autoResumeAfterMs` new opt-in `healthProbe`
 * subscribers fall back to when they omit a field — the priority
 * chain is subscriber-explicit > bus default > built-in
 * (maxConsecutiveFailures 5, no timeout budget, manual-only resume)
 * — and the default never enables probing: a subscriber that does
 * not opt into `healthProbe` is untouched by it.
 *
 * Failure assertions read `subscriberHealth(subId)` (following
 * `health.test.ts`): the counter exposes the baked threshold, and
 * `getStats().degradedSubscribers` exposes the auto-pause. A
 * controllable clock makes processing-timeout tests deterministic —
 * a "slow" handler moves the clock forward itself.
 */

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

test('EB-496: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultHealthProbe(), undefined);
  bus.setDefaultHealthProbe(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_health_probe_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-496: healthProbe: true takes the whole bus default threshold (auto-pause + onDegraded once)', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  let calls = 0;
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 2 });
  const sub = bus.subscribe(
    't',
    () => {
      calls += 1;
      throw new Error('boom');
    },
    { healthProbe: true, onDegraded: (e) => events.push(e) },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(calls, 2);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].consecutiveFailures, 2);
  assert.equal(events[0].reason, 'error');
  assert.equal(bus.getStats().degradedSubscribers, 1);
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: true,
    consecutiveFailures: 2,
  });
  // Paused: new publishes stay queued, none delivered, no repeat event.
  bus.publish('t', 'c');
  await flush();
  assert.equal(calls, 2);
  assert.equal(events.length, 1);
  assert.equal(bus.pendingCount(sub.id), 1);
});

test('EB-496: an explicit maxConsecutiveFailures wins over the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 2 });
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: { maxConsecutiveFailures: 3 } },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(
    bus.subscriberHealth(sub.id).consecutiveFailures,
    2,
    'two failures are below the explicit threshold of 3',
  );
  assert.equal(bus.subscriberHealth(sub.id).degraded, false);
  bus.publish('t', 'c');
  await flush();
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 3);
  assert.equal(bus.getStats().degradedSubscribers, 1);
});

test('EB-496: the default processingTimeoutMs makes slow handlers count as failures', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: DegradedEvent[] = [];
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 2, processingTimeoutMs: 50 });
  bus.subscribe(
    't',
    () => {
      clock.nowMs += 100; // overruns the 50ms processing budget
    },
    { healthProbe: true, onDegraded: (e) => events.push(e) },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'timeout');
  assert.equal(events[0].consecutiveFailures, 2);
  assert.equal(bus.getStats().degradedSubscribers, 1);
});

test('EB-496: the default autoResumeAfterMs auto-resumes after the cooldown', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  let failures = 1;
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 1, autoResumeAfterMs: 30 });
  const sub = bus.subscribe(
    't',
    (msg) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('boom');
      }
      received.push(msg.payload);
    },
    { healthProbe: true },
  );
  bus.publish('t', 'a'); // fails -> degraded
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);
  bus.publish('t', 'b'); // queued while degraded
  await sleep(80); // cooldown elapses -> auto-resume (the timer is unref'd)
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.deepEqual(received, ['b']);
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 0);
});

test('EB-496: with no default, true still takes the built-ins 5 / no timeout / manual-only', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  for (let i = 0; i < 4; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(
    bus.subscriberHealth(sub.id).consecutiveFailures,
    4,
    '4 failures are below the built-in threshold of 5',
  );
  assert.equal(bus.subscriberHealth(sub.id).degraded, false);
  bus.publish('t', 'x');
  await flush();
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 5);
  assert.equal(bus.getStats().degradedSubscribers, 1);
  // Manual-only resume: no cooldown fires on its own.
  await sleep(60);
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);

  // No timeout budget: a slow handler is not counted at all.
  const clock = controllableClock();
  const slowBus = new EventBus({ now: clock.now });
  const slow = slowBus.subscribe(
    'u',
    () => {
      clock.nowMs += 100;
    },
    { healthProbe: true },
  );
  slowBus.publish('u', 'a');
  slowBus.publish('u', 'b');
  await flush();
  assert.equal(slowBus.subscriberHealth(slow.id).consecutiveFailures, 0);
  assert.equal(slowBus.subscriberHealth(slow.id).degraded, false);
});

test('EB-496: clearing to null restores the built-ins for new subscribers', async () => {
  const bus = new EventBus();
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 2 });
  bus.setDefaultHealthProbe(null);
  assert.equal(bus.getDefaultHealthProbe(), undefined);
  const audit = bus.getSubscriptionAudit({ action: 'default_health_probe_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { maxConsecutiveFailures: 2 },
    healthProbe: null,
  });
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(
    bus.subscriberHealth(sub.id).consecutiveFailures,
    2,
    'no default caps the threshold: the built-in 5 applies again',
  );
  assert.equal(bus.subscriberHealth(sub.id).degraded, false);
  for (let i = 0; i < 3; i += 1) bus.publish('t', i);
  await flush();
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 5);
  assert.equal(bus.getStats().degradedSubscribers, 1);
});

test('EB-496: omitted/false healthProbe is untouched by the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 1 });
  const receivedA: unknown[] = [];
  const receivedB: unknown[] = [];
  const plain = bus.subscribe('a', (msg) => {
    receivedA.push(msg.payload);
  });
  const optedOut = bus.subscribe(
    'b',
    (msg) => {
      receivedB.push(msg.payload);
    },
    { healthProbe: false },
  );
  bus.publish('a', 'x');
  bus.publish('b', 'x');
  await flush();
  assert.deepEqual(receivedA, ['x']);
  assert.deepEqual(receivedB, ['x']);
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.deepEqual(bus.subscriberHealth(plain.id), {
    subscriberId: plain.id,
    enabled: false,
    degraded: false,
    consecutiveFailures: 0,
  });
  assert.deepEqual(bus.subscriberHealth(optedOut.id), {
    subscriberId: optedOut.id,
    enabled: false,
    degraded: false,
    consecutiveFailures: 0,
  });
});

test('EB-496: audit shape, defensive copies, and same-value no-op', () => {
  const bus = new EventBus();
  bus.setDefaultHealthProbe({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_health_probe_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultHealthProbe(), undefined);
  const opts: HealthProbeOptions = {
    maxConsecutiveFailures: 2,
    processingTimeoutMs: 50,
    autoResumeAfterMs: 1000,
  };
  bus.setDefaultHealthProbe(opts);
  opts.maxConsecutiveFailures = 999;
  opts.processingTimeoutMs = 999;
  assert.deepEqual(
    bus.getDefaultHealthProbe(),
    { maxConsecutiveFailures: 2, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultHealthProbe({
    maxConsecutiveFailures: 2,
    processingTimeoutMs: 50,
    autoResumeAfterMs: 1000,
  });
  bus.setDefaultHealthProbe({
    maxConsecutiveFailures: 2,
    processingTimeoutMs: 50,
    autoResumeAfterMs: 1000,
  });
  const audit = bus.getSubscriptionAudit({ action: 'default_health_probe_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    healthProbe: { maxConsecutiveFailures: 2, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
  });
  assert.equal(audit[0].actor, 'system');
  const held = bus.getDefaultHealthProbe();
  assert.ok(held !== undefined);
  held.maxConsecutiveFailures = 999;
  assert.deepEqual(
    bus.getDefaultHealthProbe(),
    { maxConsecutiveFailures: 2, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
    'the getter returns a copy',
  );
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 8 }, 'ops');
  assert.deepEqual(bus.getDefaultHealthProbe(), { maxConsecutiveFailures: 8 });
  const revised = bus.getSubscriptionAudit({ action: 'default_health_probe_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { maxConsecutiveFailures: 2, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
    healthProbe: { maxConsecutiveFailures: 8 },
  });
  assert.equal(revised[1].actor, 'ops');
});

test('EB-496: invalid values throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 2, processingTimeoutMs: 50 });
  const bad = [
    0,
    -1,
    '2',
    [],
    true,
    { maxConsecutiveFailures: 0 },
    { maxConsecutiveFailures: -1 },
    { maxConsecutiveFailures: 2.5 },
    { maxConsecutiveFailures: Number.NaN },
    { maxConsecutiveFailures: '2' },
    { processingTimeoutMs: 0 },
    { processingTimeoutMs: -1 },
    { processingTimeoutMs: Number.NaN },
    { processingTimeoutMs: Number.POSITIVE_INFINITY },
    { autoResumeAfterMs: 0 },
    { autoResumeAfterMs: -5 },
    { autoResumeAfterMs: Number.NaN },
    { autoResumeAfterMs: Number.POSITIVE_INFINITY },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultHealthProbe(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultHealthProbe(), {
      maxConsecutiveFailures: 2,
      processingTimeoutMs: 50,
    });
  }
  assert.throws(() => bus.setDefaultHealthProbe({ maxConsecutiveFailures: 7 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultHealthProbe(), {
    maxConsecutiveFailures: 2,
    processingTimeoutMs: 50,
  });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_health_probe_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-496: revising the bus default never moves an already-subscribed threshold or counter', async () => {
  const bus = new EventBus();
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 2 });
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  bus.publish('t', 'a');
  await flush();
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 1);
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 5 });
  bus.publish('t', 'b');
  await flush();
  assert.equal(
    bus.subscriberHealth(sub.id).consecutiveFailures,
    2,
    'the early subscriber keeps the threshold it was baked with',
  );
  assert.equal(bus.getStats().degradedSubscribers, 1);
  // A subscriber joining after the revision takes the new default.
  const late = bus.subscribe(
    'u',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  for (let i = 0; i < 4; i += 1) bus.publish('u', i);
  await flush();
  assert.equal(bus.subscriberHealth(late.id).consecutiveFailures, 4);
  assert.equal(
    bus.subscriberHealth(late.id).degraded,
    false,
    'the new default threshold is 5',
  );
});

test('EB-496: setHealthProbe(null) clears a subscriber without the default re-applying', async () => {
  const bus = new EventBus();
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 1 });
  let calls = 0;
  let failures = 1; // throw once, then behave
  const received: unknown[] = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      calls += 1;
      if (failures > 0) {
        failures -= 1;
        throw new Error('boom');
      }
      received.push(msg.payload);
    },
    { healthProbe: true },
  );
  bus.publish('t', 'a'); // fails -> degraded
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);
  bus.publish('t', 'b'); // queued while degraded
  await flush();
  assert.equal(calls, 1);
  assert.equal(bus.pendingCount(sub.id), 1);
  bus.setHealthProbe(sub.id, null);
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: false,
    degraded: false,
    consecutiveFailures: 0,
  });
  assert.equal(calls, 2, 'clearing releases the degraded gate: the backlog drains');
  assert.deepEqual(received, ['b']);
  // Still unprobed afterwards: further deliveries succeed and never degrade.
  bus.publish('t', 'c');
  await flush();
  assert.equal(calls, 3);
  assert.deepEqual(received, ['b', 'c']);
  assert.equal(bus.getStats().degradedSubscribers, 0);
  assert.throws(() => bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 2 }), RangeError);
  assert.deepEqual(
    bus.getDefaultHealthProbe(),
    { maxConsecutiveFailures: 1 },
    'clearing a subscriber never moves the bus default',
  );
  // A new subscriber still takes the untouched bus default.
  const fresh = bus.subscribe(
    'u',
    () => {
      throw new Error('boom');
    },
    { healthProbe: true },
  );
  bus.publish('u', 'x');
  await flush();
  assert.equal(bus.getStats().degradedSubscribers, 1);
});
