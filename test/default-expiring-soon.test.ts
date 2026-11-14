import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { ExpiringSoonEvent } from '../src/expiring.ts';

/**
 * Bus-level default TTL expiring-soon warning threshold (EB-500):
 * `setDefaultExpiringSoon` installs the `expiringSoonMs` new opt-in
 * `expiringSoon` subscribers fall back to when they omit the threshold
 * — the priority chain is subscriber-explicit > bus default >
 * threshold required — and the default never enables warning: a
 * subscriber that does not opt into `expiringSoon` is untouched by
 * it. The default carries no callback: `onExpiringSoon` stays the
 * subscriber's own.
 *
 * Determinism trick, following `expiring.test.ts`: `bus.publish`
 * enqueues synchronously and only schedules the drain as a microtask,
 * so several publishes in a row build a real backlog synchronously —
 * the warning is evaluated at each enqueue, exactly like the
 * production path.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('EB-500: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultExpiringSoon(), undefined);
  bus.setDefaultExpiringSoon(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_expiring_soon_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-500: { onExpiringSoon } without a bus default threshold still throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { expiringSoon: { onExpiringSoon: () => {} } }),
    RangeError,
  );
  assert.equal(bus.subscriberCount(), 0, 'the failed subscribe leaves nobody behind');
});

test('EB-500: a subscriber that passes only a callback takes the default threshold and warns', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  const events: ExpiringSoonEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    expiringSoon: { onExpiringSoon: (e) => events.push(e) },
  });
  bus.publish('t', 'm1'); // deadline 1200, remaining 200 > 100: no warning
  assert.equal(events.length, 0);
  clock.nowMs = 1150; // head remaining 50 <= 100
  bus.publish('t', 'm2'); // enqueue evaluation sees the head inside the window
  assert.equal(events.length, 1);
  const [event] = events;
  assert.equal(event.subscriberId, sub.id);
  assert.equal(event.pattern, 't');
  assert.equal(event.remainingMs, 50);
  assert.equal(event.expiringSoonMs, 100, 'the threshold comes from the bus default');
  assert.equal(event.queueDepth, 2);
  assert.equal(event.at, 1150);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
});

test('EB-500: an explicit threshold always wins over the bus default', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  const events: ExpiringSoonEvent[] = [];
  bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 1000, onExpiringSoon: (e) => events.push(e) },
  });
  bus.publish('t', 'm1'); // remaining 200 <= 1000: warns at once
  assert.equal(events.length, 1);
  assert.equal(events[0].expiringSoonMs, 1000, 'the explicit threshold wins');
  // A subscriber with a tighter explicit threshold does not fire
  // where the default would have: remaining 200 is above 50.
  const tight: ExpiringSoonEvent[] = [];
  bus.subscribe('u', () => {}, {
    expiringSoon: { expiringSoonMs: 50, onExpiringSoon: (e) => tight.push(e) },
  });
  bus.setTopicTtl('u', 200);
  bus.publish('u', 'm1');
  assert.equal(tight.length, 0, 'the explicit 50ms threshold governs, not the default 100ms');
});

test('EB-500: clearing with null returns new opt-in subscribers to the threshold-required status quo', () => {
  const bus = new EventBus();
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  bus.setDefaultExpiringSoon(null);
  assert.equal(bus.getDefaultExpiringSoon(), undefined);
  assert.throws(
    () => bus.subscribe('t', () => {}, { expiringSoon: { onExpiringSoon: () => {} } }),
    RangeError,
  );
  const audit = bus.getSubscriptionAudit({ action: 'default_expiring_soon_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { expiringSoonMs: 100 },
    expiringSoon: null,
  });
});

test('EB-500: the default never enables warning for a subscriber that does not opt in', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  bus.subscribe('t', () => {});
  bus.publish('t', 'm1');
  clock.nowMs = 1150;
  bus.publish('t', 'm2');
  assert.equal(bus.getStats().expiringSoonAlerts, 0);
  await flush();
  assert.equal(bus.getStats().expiringSoonAlerts, 0);
});

test('EB-500: revising the bus default never moves an already-subscribed threshold or latch', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  bus.setTopicTtl('u', 200);
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  const early: ExpiringSoonEvent[] = [];
  bus.subscribe('t', () => {}, {
    expiringSoon: { onExpiringSoon: (e) => early.push(e) },
  });
  bus.publish('t', 'm1'); // remaining 200 > 100: no warning yet
  assert.equal(early.length, 0);
  bus.setDefaultExpiringSoon({ expiringSoonMs: 10 });
  clock.nowMs = 1150; // head remaining 50: inside the baked 100, outside the new 10
  bus.publish('t', 'm2');
  assert.equal(early.length, 1, 'the early subscriber keeps the threshold it was baked with');
  assert.equal(early[0].expiringSoonMs, 100);
  // A subscriber joining after the revision takes the new default.
  const late: ExpiringSoonEvent[] = [];
  bus.subscribe('u', () => {}, {
    expiringSoon: { onExpiringSoon: (e) => late.push(e) },
  });
  bus.publish('u', 'm1'); // deadline 1350, remaining 200 > 10: no warning
  assert.equal(late.length, 0);
  clock.nowMs = 1345; // head remaining 5 <= 10
  bus.publish('u', 'm2');
  assert.equal(late.length, 1, 'the new default threshold is 10');
  assert.equal(late[0].expiringSoonMs, 10);
});

test('EB-500: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  const bad = [
    -1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    '100',
    [],
    true,
    { expiringSoonMs: -5 },
    { expiringSoonMs: Number.NaN },
    { expiringSoonMs: Number.POSITIVE_INFINITY },
    { expiringSoonMs: '100' },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultExpiringSoon(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultExpiringSoon(), { expiringSoonMs: 100 });
  }
  assert.throws(() => bus.setDefaultExpiringSoon({ expiringSoonMs: 50 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultExpiringSoon(), { expiringSoonMs: 100 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_expiring_soon_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-500: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultExpiringSoon(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_expiring_soon_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultExpiringSoon({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_expiring_soon_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultExpiringSoon(), undefined);
  const opts = { expiringSoonMs: 100 };
  bus.setDefaultExpiringSoon(opts);
  opts.expiringSoonMs = 999;
  assert.deepEqual(
    bus.getDefaultExpiringSoon(),
    { expiringSoonMs: 100 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  const audit = bus.getSubscriptionAudit({ action: 'default_expiring_soon_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    expiringSoon: { expiringSoonMs: 100 },
  });
  const held = bus.getDefaultExpiringSoon();
  assert.ok(held !== undefined);
  held.expiringSoonMs = 999;
  assert.deepEqual(
    bus.getDefaultExpiringSoon(),
    { expiringSoonMs: 100 },
    'the getter returns a copy',
  );
  bus.setDefaultExpiringSoon({ expiringSoonMs: 50 }, 'ops');
  assert.deepEqual(bus.getDefaultExpiringSoon(), { expiringSoonMs: 50 });
  const revised = bus.getSubscriptionAudit({ action: 'default_expiring_soon_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { expiringSoonMs: 100 },
    expiringSoon: { expiringSoonMs: 50 },
  });
  assert.equal(revised[1].actor, 'ops');
});

test('EB-500: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultExpiringSoon({ expiringSoonMs: 100 });
  const snapshot = bus.exportBusConfig();
  assert.ok(!('expiringSoon' in snapshot), 'no top-level expiringSoon key');
  assert.ok(
    !JSON.stringify(snapshot).includes('expiringSoon'),
    'the default appears nowhere in the exported snapshot',
  );
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultExpiringSoon'),
    'the field name appears nowhere in the exported snapshot',
  );
});
