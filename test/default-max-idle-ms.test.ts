import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type IdleExpiredEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

// EB-472: bus-level terminal max-idle default live revision.

test('EB-472: the factory bus default is unset (new subscribers get no idle expiry)', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxIdleMs(), undefined);
  const sub = bus.subscribe('t', noop);
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().idleExpired, 0);
  sub.unsubscribe();
});

test('EB-472: a subscriber that omits maxIdleMs takes the bus default and expires on schedule', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultMaxIdleMs(50);
  assert.equal(bus.getDefaultMaxIdleMs(), 50);
  const sub = bus.subscribe('t', noop);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.equal(events[0].maxIdleMs, 50);
  assert.equal(events[0].idleActivity, 'any');
  assert.equal(bus.getStats().idleExpired, 1);
});

test('EB-472: an explicit maxIdleMs beats the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxIdleMs(50);
  // Explicit 5000 beats a bus default of 50: still alive after the default lapses.
  const sub = bus.subscribe('t', noop, { maxIdleMs: 5000 });
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().idleExpired, 0);
  sub.unsubscribe();

  // Explicit 50 beats a bus default of 5000: expires on the explicit window.
  const events: IdleExpiredEvent[] = [];
  const loud = new EventBus({ onIdleExpired: (e) => events.push(e) });
  loud.setDefaultMaxIdleMs(5000);
  const sub2 = loud.subscribe('t', noop, { maxIdleMs: 50 });
  await sleep(150);
  assert.equal(loud.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].maxIdleMs, 50);
  assert.equal(loud.getStats().idleExpired, 1);
  void sub2;
});

test('EB-472: clearing with null returns new subscribers to no idle expiry', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxIdleMs(50);
  bus.setDefaultMaxIdleMs(null);
  assert.equal(bus.getDefaultMaxIdleMs(), undefined);
  const sub = bus.subscribe('t', noop);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().idleExpired, 0);
  sub.unsubscribe();
});

test('EB-472: idleActivity without maxIdleMs opts in exactly when a default is installed', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultMaxIdleMs(80);
  const sub = bus.subscribe('t', noop, { idleActivity: 'deliver' });
  await sleep(160);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].maxIdleMs, 80);
  assert.equal(events[0].idleActivity, 'deliver');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('EB-472: idleActivity without maxIdleMs still throws when no default is installed', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribe('t', noop, { idleActivity: 'ack' }), RangeError);
  assert.throws(() => bus.subscribe('t', noop, { idleActivity: 'deliver' }), RangeError);
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('EB-472: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultMaxIdleMs(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_max_idle_ms_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultMaxIdleMs(100);
  assert.equal(bus.getDefaultMaxIdleMs(), 100);
  bus.setDefaultMaxIdleMs(100);
  assert.equal(bus.getDefaultMaxIdleMs(), 100);
  const audit = bus.getSubscriptionAudit({ action: 'default_max_idle_ms_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, maxIdleMs: 100 });
  bus.setDefaultMaxIdleMs(null);
  assert.equal(bus.getDefaultMaxIdleMs(), undefined);
});

test('EB-472: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultMaxIdleMs(50);
  for (const bad of [0, -1, 1.5, Number.NaN, Infinity, '50'] as unknown[]) {
    assert.throws(
      () => bus.setDefaultMaxIdleMs(bad as number),
      RangeError,
      `ms=${String(bad)}`,
    );
    assert.equal(bus.getDefaultMaxIdleMs(), 50);
  }
  assert.throws(() => bus.setDefaultMaxIdleMs(60, ''), RangeError);
  assert.equal(bus.getDefaultMaxIdleMs(), 50);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_max_idle_ms_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-472: revising the bus default never moves an already-subscribed window', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultMaxIdleMs(60);
  const sub = bus.subscribe('t', noop);
  bus.setDefaultMaxIdleMs(10000);
  await sleep(150);
  // The subscriber keeps the 60ms window it was baked with.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].maxIdleMs, 60);
  assert.equal(bus.getStats().idleExpired, 1);
  // A subscriber joining after the revision takes the new default.
  const late = bus.subscribe('u', noop);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1, 'the late joiner holds the 10000ms window');
  assert.equal(bus.getStats().idleExpired, 1);
  late.unsubscribe();
  void sub;
});

test('EB-472: setIdleWindow(null) clears to the current bus default, and to no expiry when no default is set', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  bus.setDefaultMaxIdleMs(200);
  bus.setIdleWindow(sub.id, null);
  const cleared = bus.getSubscriptionAudit({ action: 'idle_window_revised' });
  assert.equal(cleared.length, 1);
  assert.deepEqual(cleared[0].detail, { maxIdleMs: 200, oldMaxIdleMs: 60 });
  // Still alive past the old 60ms window: the window is 200ms now, not cleared.
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().idleExpired, 0);
  // ...and it lapses on the default window.
  await sleep(200);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].maxIdleMs, 200);
  assert.equal(bus.getStats().idleExpired, 1);

  // No bus default: the clear target is no idle expiry (the old behavior).
  const plain = new EventBus();
  const plainSub = plain.subscribe('t', noop, { maxIdleMs: 60 });
  plain.setIdleWindow(plainSub.id, null);
  const plainEvents = plain.getSubscriptionAudit({ action: 'idle_window_revised' });
  assert.deepEqual(plainEvents[0].detail, { maxIdleMs: null, oldMaxIdleMs: 60 });
  await sleep(150);
  assert.equal(plain.getStats().totalSubscribers, 1);
  assert.equal(plain.getStats().idleExpired, 0);
  // The EB-267 clear rule still applies with no default installed.
  assert.throws(() => plain.setIdleWindow(plainSub.id, 100), RangeError);
  plainSub.unsubscribe();
});

test("EB-472: with a bus default installed, the subscribed idleActivity mode still gates activity", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultMaxIdleMs(100);
  const sub = bus.subscribeReliable(
    't',
    (d) => d.ack(),
    { idleActivity: 'ack', ackTimeoutMs: 5000 },
  );
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush(); // the ack lands in the same round
    await sleep(40);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  await sleep(220);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'ack');
  assert.equal(events[0].maxIdleMs, 100);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('EB-472: setDefaultMaxIdleMs audits each revision with its old and new window and actor', () => {
  const bus = new EventBus();
  bus.setDefaultMaxIdleMs(50, 'ops');
  bus.setDefaultMaxIdleMs(120, 'ops');
  bus.setDefaultMaxIdleMs(null, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'default_max_idle_ms_revised' });
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, maxIdleMs: 50 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 50, maxIdleMs: 120 });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 120, maxIdleMs: null });
  assert.equal(bus.getDefaultMaxIdleMs(), undefined);
});
