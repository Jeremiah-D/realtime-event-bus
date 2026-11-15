import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type IdleExpiredEvent } from '../src/bus.ts';

/**
 * Bus-level default idle activity mode (EB-516):
 * `setDefaultIdleActivity` installs the activity mode a new windowed
 * subscriber without an explicit `idleActivity` falls back to — the
 * precedence chain is subscriber-explicit > bus default > `'any'` —
 * and the default never opts a subscriber into idle reclamation: a
 * subscriber with no effective window does not take the default.
 *
 * Mode assertions follow `idle-activity-revision.test.ts`: the
 * `onIdleExpired` event reports the mode baked at subscribe time,
 * and re-arming behaviour (deliveries vs acks) proves which mode a
 * live subscriber is running under.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

test('EB-516: the factory default is unset (an omitted mode behaves as any)', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  assert.equal(bus.getDefaultIdleActivity(), undefined);
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'any');
  assert.equal(bus.getStats().idleExpired, 1);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_idle_activity_revised' }).length,
    0,
  );
  void sub;
});

test("EB-516: set 'ack' then a new subscriber counts activity by ack (acks re-arm)", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultIdleActivity('ack');
  const sub = bus.subscribeReliable(
    't',
    (d) => d.ack(),
    { maxIdleMs: 100, ackTimeoutMs: 5000 },
  );
  // Acks re-arm the 100ms window: four publishes 40ms apart keep the
  // subscriber alive past 160ms.
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush(); // the ack lands in the same round
    await sleep(40);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  // Go quiet: the window finally lapses after the last ack.
  await sleep(220);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'ack');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("EB-516: under an 'ack' default, deliveries without acks do not re-arm", async () => {
  const bus = new EventBus();
  bus.setDefaultIdleActivity('ack');
  const sub = bus.subscribeReliable(
    't',
    () => {
      /* never acks */
    },
    { maxIdleMs: 80, ackTimeoutMs: 5000 },
  );
  bus.publish('t', 'one');
  await flush();
  bus.publish('t', 'two');
  await flush();
  // Deliveries happened, but none settled via ack() — the window armed
  // at subscribe time still lapses on schedule.
  await sleep(160);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("EB-516: an explicit 'deliver' beats an 'ack' default", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultIdleActivity('ack');
  const sub = bus.subscribe('t', noop, { maxIdleMs: 150, idleActivity: 'deliver' });
  // Deliveries re-arm the 150ms window: four publishes 60ms apart
  // keep the subscriber alive past 240ms.
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(60);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  // Go quiet: the window finally lapses 150ms after the last delivery.
  await sleep(250);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'deliver');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("EB-516: an explicit 'any' beats an 'ack' default", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultIdleActivity('ack');
  const sub = bus.subscribe('t', noop, { maxIdleMs: 150, idleActivity: 'any' });
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(60);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  await sleep(250);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'any');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('EB-516: a subscriber getting its window via the EB-472 default also takes the mode default', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultMaxIdleMs(100);
  bus.setDefaultIdleActivity('ack');
  // No maxIdleMs and no idleActivity of its own: both come from the
  // bus defaults.
  const sub = bus.subscribeReliable(
    't',
    (d) => d.ack(),
    { ackTimeoutMs: 5000 },
  );
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush();
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

test('EB-516: a subscriber with no effective window does not take the default and does not throw', async () => {
  const bus = new EventBus();
  bus.setDefaultIdleActivity('ack');
  // No maxIdleMs of its own and no EB-472 default installed: the
  // mode default must not opt this subscriber into reclamation.
  const sub = bus.subscribe('t', noop);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1, 'no idle expiry without a window');
  assert.equal(bus.getStats().idleExpired, 0);
  // The record keeps idleActivity undefined: there is no window to
  // revise, so setIdleActivity refuses it.
  assert.throws(() => bus.setIdleActivity(sub.id, 'deliver'), RangeError);
  sub.unsubscribe();
});

test('EB-516: an explicit idleActivity without maxIdleMs still throws and the live default is unchanged', () => {
  const bus = new EventBus();
  bus.setDefaultIdleActivity('ack');
  assert.throws(
    () => bus.subscribe('t', noop, { idleActivity: 'ack' }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', noop, { idleActivity: 'deliver' }),
    RangeError,
  );
  // The failed subscribes registered nothing and the live default
  // is untouched.
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getDefaultIdleActivity(), 'ack');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_idle_activity_revised' }).length,
    1,
    'the failed subscribes audited nothing',
  );
});

test("EB-516: set(null) clears the default back to 'any'", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultIdleActivity('ack');
  bus.setDefaultIdleActivity(null);
  assert.equal(bus.getDefaultIdleActivity(), undefined);
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'any');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('EB-516: illegal values and illegal actors throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultIdleActivity('ack');
  for (const bad of ['both', 'ACK', '', 42, undefined, {}]) {
    assert.throws(
      () => bus.setDefaultIdleActivity(bad as never),
      RangeError,
      `mode=${String(bad)}`,
    );
  }
  assert.throws(() => bus.setDefaultIdleActivity('deliver', ''), RangeError);
  assert.throws(() => bus.setDefaultIdleActivity('deliver', 42 as never), RangeError);
  assert.equal(bus.getDefaultIdleActivity(), 'ack');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_idle_activity_revised' }).length,
    1,
    'the failed calls audited nothing',
  );
});

test('EB-516: a same-value revision is a no-op with no audit', () => {
  const bus = new EventBus();
  // Setting 'any' on a factory bus and clearing an already-unset
  // default both audit nothing.
  bus.setDefaultIdleActivity('any');
  bus.setDefaultIdleActivity(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_idle_activity_revised' }).length,
    0,
  );
  assert.equal(bus.getDefaultIdleActivity(), undefined);
  bus.setDefaultIdleActivity('ack');
  bus.setDefaultIdleActivity('ack');
  const audit = bus.getSubscriptionAudit({ action: 'default_idle_activity_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, { previous: null, idleActivity: 'ack' });
});

test("EB-516: a revision never moves an already-subscribed subscriber's mode", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 150 });
  // The subscriber baked 'any' at subscribe time; installing an
  // 'ack' default afterwards must not change what counts for it.
  bus.setDefaultIdleActivity('ack');
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(60);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  await sleep(250);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'any', 'the expiry reports the baked mode');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('EB-516: per-subscriber setIdleActivity still revises one subscriber and a later default revision does not move it', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  bus.setDefaultIdleActivity('ack');
  const sub = bus.subscribe('t', noop, { maxIdleMs: 200 });
  // Per-subscriber revision to 'deliver': deliveries now re-arm.
  bus.setIdleActivity(sub.id, 'deliver');
  // A later default revision must not move the mode setIdleActivity
  // installed.
  bus.setDefaultIdleActivity('any');
  for (let i = 0; i < 3; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(80);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  await sleep(300);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'deliver');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('EB-516: audit records previous/idleActivity exactly and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultIdleActivity('ack', 'ops');
  bus.setDefaultIdleActivity('deliver');
  bus.setDefaultIdleActivity(null);
  const audit = bus.getSubscriptionAudit({ action: 'default_idle_activity_revised' });
  assert.equal(audit.length, 3);
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail, { previous: null, idleActivity: 'ack' });
  assert.deepEqual(audit[1].detail, { previous: 'ack', idleActivity: 'deliver' });
  assert.deepEqual(audit[2].detail, { previous: 'deliver', idleActivity: null });
  assert.equal(bus.getDefaultIdleActivity(), undefined);
  // Setting 'any' on the cleared bus is a no-op too: unset already
  // means the effective 'any', so it audits nothing.
  bus.setDefaultIdleActivity('any');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_idle_activity_revised' }).length,
    3,
  );
  // Clearing a bus already at the effective 'any' is a no-op: the
  // third entry above already cleared it, so a second clear audits
  // nothing.
  bus.setDefaultIdleActivity(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_idle_activity_revised' }).length,
    3,
  );
});
