import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type DegradedEvent } from '../src/bus.ts';

/**
 * Subscriber health-probe live enable (EB-540):
 * `setHealthProbe` on a subscription that never opted into
 * `healthProbe` installs a fresh probe instead of refusing: `true`
 * takes the documented default (`maxConsecutiveFailures` 5, no
 * timeout budget, manual-only resume), an object patch resolves its
 * missing fields through the same chain as a subscribe-time opt-in
 * (explicit > live bus default from `setDefaultHealthProbe` (EB-496)
 * > documented default), and `{}` resolves like `true`. The enable
 * installs a fresh `HealthProbeState` (`consecutiveFailures` 0,
 * `degraded` false, no auto-resume timer) and only governs failures
 * after it — failures recorded before the enable are not counted
 * retroactively. `null` on a never-probed subscriber is a silent
 * no-op with no audit, and a `null` clear can be followed by a fresh
 * enable. The enable is live-only: a migration round-trip
 * (export/import, the restart path) restores the subscriber
 * unconfigured, because the snapshot carries only the subscribe-time
 * options. Audited as `health_probe_revised` with
 * `oldHealthProbe: null`.
 *
 * Determinism tricks (as in `health-revision.test.ts`): a
 * manually-advanced clock and a throwing handler, so degradation
 * trips on a known failure count instead of racing wall time.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`.
 * A "slow" handler simulates overrunning its processing budget by moving
 * the clock forward itself, keeping the timeout tests deterministic.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const revisionAudits = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'health_probe_revised' });

test('EB-540: first enable with true takes the documented default 5', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});

  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: false,
    degraded: false,
    consecutiveFailures: 0,
  });

  bus.setHealthProbe(sub.id, true);
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: false,
    consecutiveFailures: 0,
  });

  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    healthProbe: { maxConsecutiveFailures: 5 },
    oldHealthProbe: null,
  });
  sub.unsubscribe();
});

test('EB-540: a partial object resolves omitted fields through the bus default, and {} resolves like true', () => {
  const bus = new EventBus();
  bus.setDefaultHealthProbe({ maxConsecutiveFailures: 2, processingTimeoutMs: 50 });
  const patched = bus.subscribe('t', () => {});
  const whole = bus.subscribe('u', () => {});

  bus.setHealthProbe(patched.id, { autoResumeAfterMs: 1000 });
  bus.setHealthProbe(whole.id, {});

  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, {
    healthProbe: { maxConsecutiveFailures: 2, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
    oldHealthProbe: null,
  });
  assert.deepEqual(audit[1].detail, {
    healthProbe: { maxConsecutiveFailures: 2, processingTimeoutMs: 50 },
    oldHealthProbe: null,
  });
  patched.unsubscribe();
  whole.unsubscribe();
});

test('EB-540: null on an unconfigured subscriber is a silent no-op', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => {
    received.push(msg.payload);
  });

  bus.setHealthProbe(sub.id, null);
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: false,
    degraded: false,
    consecutiveFailures: 0,
  });
  assert.equal(revisionAudits(bus).length, 0, 'no audit for clearing what was never set');

  bus.publish('t', 'a');
  await flush();
  assert.deepEqual(received, ['a'], 'delivery is untouched by the no-op');
  sub.unsubscribe();
});

test('EB-540: invalid values are refused with the live subscriber untouched', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});

  assert.throws(() => bus.setHealthProbe(sub.id, undefined as never), RangeError);
  assert.throws(() => bus.setHealthProbe(sub.id, 'x' as never), RangeError);
  assert.throws(() => bus.setHealthProbe(sub.id, [] as never), RangeError);
  assert.throws(() => bus.setHealthProbe(sub.id, false as never), RangeError);
  assert.throws(() => bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 0 }), RangeError);
  assert.throws(() => bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 1.5 }), RangeError);
  assert.throws(() => bus.setHealthProbe(sub.id, { processingTimeoutMs: 0 }), RangeError);
  assert.throws(() => bus.setHealthProbe(sub.id, { autoResumeAfterMs: Infinity }), RangeError);

  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: false,
    degraded: false,
    consecutiveFailures: 0,
  });
  assert.equal(revisionAudits(bus).length, 0, 'a failed enable leaves no audit');

  // A failed enable leaves the subscriber usable: a valid enable still works.
  bus.setHealthProbe(sub.id, true);
  assert.equal(bus.subscriberHealth(sub.id).enabled, true);
  assert.equal(revisionAudits(bus).length, 1);
  sub.unsubscribe();
});

test('EB-540: consecutive failures after the enable reach the threshold and degrade', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    { onDegraded: (e) => events.push(e) },
  );

  bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 2 });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 1, 'the second failure trips the enabled threshold');
  assert.equal(events[0].consecutiveFailures, 2);
  assert.equal(events[0].reason, 'error');
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  assert.equal(bus.getStats().degradedSubscribers, 1);
  sub.unsubscribe();
});

test('EB-540: failures before the enable are not counted retroactively', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  // Handler fault isolation (EB-197) contains the pre-enable throws:
  // without a probe — or the isolation — a throwing plain handler
  // escapes the drain as an uncaught exception. The isolation is
  // orthogonal to the probe (its own counter, its own budget of 100,
  // never quarantines here) and only keeps the test process clean.
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    {
      handlerFaultIsolation: { maxConsecutiveErrors: 100 },
      onDegraded: (e) => events.push(e),
    },
  );

  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 0, 'two failures with no probe degrade nothing');

  bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 2 });
  bus.publish('t', 'c');
  await flush();
  assert.equal(events.length, 0, 'the first failure after the enable is only failure 1');
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 1);

  bus.publish('t', 'd');
  await flush();
  assert.equal(events.length, 1, 'the second failure after the enable trips the threshold');
  assert.equal(events[0].consecutiveFailures, 2);
  sub.unsubscribe();
});

test('EB-540: clear then re-enable installs a fresh probe that governs again', async () => {
  const bus = new EventBus();
  const events: DegradedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      throw new Error('boom');
    },
    {
      healthProbe: { maxConsecutiveFailures: 1 },
      // Contains the throw while the probe is cleared (see the
      // pre-enable test above): without it the unprobed throw escapes
      // the drain as an uncaught exception.
      handlerFaultIsolation: { maxConsecutiveErrors: 100 },
      onDegraded: (e) => events.push(e),
    },
  );

  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 1, 'first failure degrades on the subscribe-time probe');
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);

  bus.setHealthProbe(sub.id, null);
  assert.equal(bus.subscriberHealth(sub.id).enabled, false);

  // The clear released the pause, so the next failure is delivered
  // (and counted by nothing) while the subscriber is unprobed.
  bus.publish('t', 'b');
  await flush();
  assert.equal(events.length, 1, 'no probe, no second degradation');
  assert.equal(bus.getStats().degradedSubscribers, 0);

  // A clear is no longer terminal (EB-540): the same call re-enables
  // with a fresh counter, and the fresh probe governs from there.
  bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 2 });
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: false,
    consecutiveFailures: 0,
  });

  bus.publish('t', 'c');
  await flush();
  assert.equal(events.length, 1, 'one failure stays under the fresh threshold of 2');
  bus.publish('t', 'd');
  await flush();
  assert.equal(events.length, 2, 'the second failure trips the re-enabled probe');
  assert.equal(events[1].consecutiveFailures, 2);

  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, {
    healthProbe: null,
    oldHealthProbe: { maxConsecutiveFailures: 1 },
  });
  assert.deepEqual(audit[1].detail, {
    healthProbe: { maxConsecutiveFailures: 2 },
    oldHealthProbe: null,
  });
  sub.unsubscribe();
});

test('EB-540: the enable audit carries oldHealthProbe null for a full object', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});

  bus.setHealthProbe(sub.id, {
    maxConsecutiveFailures: 3,
    processingTimeoutMs: 50,
    autoResumeAfterMs: 1000,
  });

  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    healthProbe: { maxConsecutiveFailures: 3, processingTimeoutMs: 50, autoResumeAfterMs: 1000 },
    oldHealthProbe: null,
  });
  sub.unsubscribe();
});

test('EB-540: a migration round-trip restores the subscriber unconfigured', async () => {
  const source = new EventBus();
  const sub = source.subscribe('t', () => {
    throw new Error('boom');
  }, {
    // Migrates with the snapshot (it is a serializable subscribe
    // option), so the restored subscriber on the target contains the
    // throws while it is unprobed — without it they would escape the
    // drain as uncaught exceptions.
    handlerFaultIsolation: { maxConsecutiveErrors: 100 },
  });
  source.setHealthProbe(sub.id, { maxConsecutiveFailures: 1 });
  assert.equal(source.subscriberHealth(sub.id).enabled, true, 'the enable lands on the source');

  const snap = source.exportSubscription(sub.id);
  assert(snap !== null);

  // The restart path: a fresh bus imports the snapshot, which carries
  // only the subscribe-time options — the live enable does not travel.
  // (The source's `onDegraded` is a function and does not migrate
  // either, so degradation is read off the target's stats, not events.)
  const target = new EventBus();
  const res = target.importSubscriptionSnapshot(snap, (m: BusMessage) => {
    throw new Error('boom');
  });
  assert.deepEqual(res, { restored: 1, skipped: 0 });
  assert.equal(revisionAudits(target).length, 0, 'no enable audit on the restarted bus');

  // One failure would have degraded the threshold-1 probe had it
  // survived the round-trip; the restored subscriber stays unprobed.
  target.publish('t', 'a');
  await flush();
  assert.equal(target.getStats().degradedSubscribers, 0);

  // The restored subscriber can be enabled again from scratch.
  target.publish('t', 'b');
  await flush();
  assert.equal(target.getStats().degradedSubscribers, 0, 'still unprobed after a second failure');
  sub.unsubscribe();
});

test('EB-540: a post-enable revision merges the live values and keeps the counter', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: DegradedEvent[] = [];
  const sub = bus.subscribe(
    't',
    () => {
      clock.nowMs += 100; // overruns the 50ms budget the enable installs
    },
    { onDegraded: (e) => events.push(e) },
  );

  bus.setHealthProbe(sub.id, { maxConsecutiveFailures: 2, processingTimeoutMs: 50 });
  // The revision adds only the cooldown: the threshold and the budget
  // it leaves out keep their enabled values.
  bus.setHealthProbe(sub.id, { autoResumeAfterMs: 30 });
  assert.deepEqual(bus.subscriberHealth(sub.id), {
    subscriberId: sub.id,
    enabled: true,
    degraded: false,
    consecutiveFailures: 0,
  });

  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    healthProbe: { maxConsecutiveFailures: 2, processingTimeoutMs: 50, autoResumeAfterMs: 30 },
    oldHealthProbe: { maxConsecutiveFailures: 2, processingTimeoutMs: 50 },
  });

  // The merged values govern: one slow call is one timeout failure
  // under the kept threshold of 2, not a degradation.
  bus.publish('t', 'a');
  await flush();
  assert.equal(events.length, 0);
  assert.equal(bus.subscriberHealth(sub.id).consecutiveFailures, 1);
  sub.unsubscribe();
});
