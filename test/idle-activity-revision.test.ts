import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type IdleExpiredEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

test('unknown subscriber and illegal subId throw RangeError', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  assert.throws(() => bus.setIdleActivity('no-such-sub', 'ack'), RangeError);
  for (const badId of ['', 42, null, undefined]) {
    assert.throws(() => bus.setIdleActivity(badId as never, 'ack'), RangeError, `subId=${String(badId)}`);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_activity_revised' }).length, 0);
  sub.unsubscribe();
});

test('never-opted-in and cleared-window subscribers refuse revision', async () => {
  const bus = new EventBus();
  const plain = bus.subscribe('t', noop);
  assert.throws(() => bus.setIdleActivity(plain.id, 'ack'), RangeError);
  assert.throws(() => bus.setIdleActivity(plain.id, 'deliver'), RangeError);
  // The plain subscriber is untouched and never expires.
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_activity_revised' }).length, 0);
  plain.unsubscribe();

  const idle = bus.subscribe('t', noop, { maxIdleMs: 60 });
  bus.setIdleWindow(idle.id, null);
  // The EB-267 clear rule: a cleared window cannot be revised until
  // the subscriber opts in again at subscribe time.
  assert.throws(() => bus.setIdleActivity(idle.id, 'ack'), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_activity_revised' }).length, 0);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1, 'the cleared subscriber keeps running');
  idle.unsubscribe();
});

test('illegal modes and illegal actors throw RangeError and leave the live mode untouched', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60, idleActivity: 'deliver' });
  for (const bad of ['both', 'DELIVER', '', 42, null, undefined, {}]) {
    assert.throws(() => bus.setIdleActivity(sub.id, bad as never), RangeError, `idleActivity=${String(bad)}`);
  }
  assert.throws(() => bus.setIdleActivity(sub.id, 'ack', ''), RangeError);
  assert.throws(() => bus.setIdleActivity(sub.id, 'ack', 42 as never), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_activity_revised' }).length, 0);
  // The live mode is untouched: deliveries still re-arm, and the
  // expiry still reports the original mode.
  bus.publish('t', 'still-counted');
  await flush();
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'deliver');
  assert.equal(events[0].maxIdleMs, 60);
  assert.equal(bus.getStats().idleExpired, 1);
});

test('an identical revision is a no-op: no audit and the timer is not re-armed', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { maxIdleMs: 200, idleActivity: 'deliver' });
  await sleep(100);
  bus.setIdleActivity(sub.id, 'deliver');
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_activity_revised' }).length, 0);
  // The pending timer kept its original fire time (200ms from
  // subscribe): had the no-op re-armed it, the subscriber would
  // still be alive 150ms later.
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0, 'the original timer fired on schedule');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("revising 'deliver' to 'ack' stops deliveries from re-arming the window", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
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
  // Switch to 'ack': a plain subscription never acks, so no further
  // event qualifies — the window lapses ~150ms after the last delivery
  // even though publishes keep arriving.
  bus.setIdleActivity(sub.id, 'ack');
  for (let i = 0; i < 5; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(60);
  }
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'ack', 'the expiry reports the revised mode');
  assert.equal(events[0].maxIdleMs, 150);
  assert.equal(bus.getStats().idleExpired, 1);

  const audit = bus.getSubscriptionAudit({ action: 'idle_activity_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { idleActivity: 'ack', oldIdleActivity: 'deliver' });
  void sub;
});

test("revising 'ack' to 'deliver' starts deliveries re-arming the window", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  // A plain subscription never acks, so under 'ack' nothing re-arms:
  // without the revision it would expire 200ms after subscribe.
  const sub = bus.subscribe('t', noop, { maxIdleMs: 200, idleActivity: 'ack' });
  await sleep(100);
  bus.setIdleActivity(sub.id, 'deliver');
  // Deliveries now re-arm the 200ms window — three publishes 80ms
  // apart keep it alive well past the original 200ms bound.
  for (let i = 0; i < 3; i += 1) {
    bus.publish('t', i);
    await flush();
    await sleep(80);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  // Go quiet: the window finally lapses 200ms after the last delivery.
  await sleep(300);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].idleActivity, 'deliver');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('the IdleManager entry moves too: an expiry after the revision reports the new mode', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60, idleActivity: 'deliver' });
  bus.setIdleActivity(sub.id, 'ack');
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].maxIdleMs, 60);
  assert.equal(events[0].idleActivity, 'ack');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('audit payload shape and actor passthrough', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { maxIdleMs: 5000, idleActivity: 'deliver' });
  bus.setIdleActivity(sub.id, 'ack', 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'idle_activity_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail, { idleActivity: 'ack', oldIdleActivity: 'deliver' });

  // The default actor is 'system'.
  const other = bus.subscribe('t', noop, { maxIdleMs: 5000, idleActivity: 'deliver' });
  bus.setIdleActivity(other.id, 'any');
  const all = bus.getSubscriptionAudit({ action: 'idle_activity_revised' });
  assert.equal(all.length, 2);
  assert.equal(all[1].actor, 'system');
  assert.deepEqual(all[1].detail, { idleActivity: 'any', oldIdleActivity: 'deliver' });
  sub.unsubscribe();
  other.unsubscribe();
});

test('a revision does not touch the armed window: the remaining time is unchanged', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { maxIdleMs: 200, idleActivity: 'deliver' });
  await sleep(100);
  bus.setIdleActivity(sub.id, 'ack');
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_activity_revised' }).length, 1);
  // The pending timer kept its original fire time (200ms from
  // subscribe): had the revision re-armed it, the subscriber would
  // still be alive 150ms later.
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0, 'the original timer fired on schedule');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});
