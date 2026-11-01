import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type IdleExpiredEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

test('setIdleWindow widens the window: the old timer is cleared and expiry reports the new value', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  await sleep(30);
  bus.setIdleWindow(sub.id, 400);
  // The 60ms timer armed at subscribe time would have fired by now;
  // the revision re-armed a fresh 400ms window from the revision moment.
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1, 'the old timer was cleared');
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().idleExpired, 0);
  await sleep(400);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].subscriberId, sub.id);
  assert.equal(events[0].maxIdleMs, 400, 'the expiry event reports the revised window');
  assert.equal(events[0].idleActivity, 'any');
  assert.equal(bus.getStats().idleExpired, 1);

  const audit = bus.getSubscriptionAudit({ action: 'idle_window_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, { maxIdleMs: 400, oldMaxIdleMs: 60 });
});

test('setIdleWindow tightens the window: expiry comes early and reports the new value', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 5000 });
  bus.setIdleWindow(sub.id, 60);
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].maxIdleMs, 60);
  assert.equal(bus.getStats().idleExpired, 1);

  const audit = bus.getSubscriptionAudit({ action: 'idle_window_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { maxIdleMs: 60, oldMaxIdleMs: 5000 });
  void sub;
});

test('activity after a revision re-arms with the new window', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  bus.setIdleWindow(sub.id, 200);
  // Keep the subscriber busy: every delivery re-arms a fresh 200ms
  // window — under the old 60ms window it would have expired twice over.
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
  assert.equal(events[0].maxIdleMs, 200);
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test('setIdleWindow with null clears the window and a later revision is refused', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  bus.setIdleWindow(sub.id, null);
  // The subscriber keeps running with no idle expiry at all.
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().idleExpired, 0);
  // Deliveries still land — only the reclamation is gone.
  bus.publish('t', 'still-here');
  await flush();
  assert.equal(bus.getStats().totalSubscribers, 1);

  const audit = bus.getSubscriptionAudit({ action: 'idle_window_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { maxIdleMs: null, oldMaxIdleMs: 60 });

  // Re-revision requires opting in again at subscribe time.
  assert.throws(() => bus.setIdleWindow(sub.id, 100), RangeError);
  assert.throws(() => bus.setIdleWindow(sub.id, null), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_window_revised' }).length, 1);
  sub.unsubscribe();
});

test('unknown subscriber and never-opted-in subscriber throw RangeError', () => {
  const bus = new EventBus();
  const plain = bus.subscribe('t', noop);
  assert.throws(() => bus.setIdleWindow('no-such-sub', 100), RangeError);
  assert.throws(() => bus.setIdleWindow(plain.id, 100), RangeError);
  assert.throws(() => bus.setIdleWindow(plain.id, null), RangeError);
  // The plain subscriber is untouched and never expires.
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_window_revised' }).length, 0);
  plain.unsubscribe();
});

test('illegal subId and illegal windows throw RangeError and leave the live window untouched', async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribe('t', noop, { maxIdleMs: 60 });
  for (const badId of ['', 42, null, undefined]) {
    assert.throws(() => bus.setIdleWindow(badId as never, 100), RangeError, `subId=${String(badId)}`);
  }
  for (const bad of [0, -1, -100, 1.5, NaN, Infinity, -Infinity, 'x', undefined]) {
    assert.throws(() => bus.setIdleWindow(sub.id, bad as never), RangeError, `maxIdleMs=${String(bad)}`);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_window_revised' }).length, 0);
  // The live window is untouched: it still expires on the original
  // 60ms schedule and reports the original value.
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].maxIdleMs, 60);
  assert.equal(bus.getStats().idleExpired, 1);
});

test('an identical revision is a no-op: no audit and the timer is not re-armed', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', noop, { maxIdleMs: 200 });
  await sleep(100);
  bus.setIdleWindow(sub.id, 200);
  assert.equal(bus.getSubscriptionAudit({ action: 'idle_window_revised' }).length, 0);
  // The pending timer kept its original fire time (200ms from
  // subscribe): had the no-op re-armed it, the subscriber would
  // still be alive 150ms later.
  await sleep(150);
  assert.equal(bus.getStats().totalSubscribers, 0, 'the original timer fired on schedule');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});

test("idleActivity mode is preserved: 'ack' still gates re-arming after a revision", async () => {
  const events: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => events.push(e) });
  const sub = bus.subscribeReliable(
    't',
    (d) => d.ack(),
    { maxIdleMs: 60, idleActivity: 'ack', ackTimeoutMs: 5000 },
  );
  bus.setIdleWindow(sub.id, 250);
  // Acks re-arm the revised 250ms window, exactly as before.
  for (let i = 0; i < 3; i += 1) {
    bus.publish('t', i);
    await flush(); // the ack lands in the same round
    await sleep(80);
  }
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(events.length, 0);
  await sleep(350);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(events.length, 1);
  assert.equal(events[0].maxIdleMs, 250);
  assert.equal(events[0].idleActivity, 'ack');
  assert.equal(bus.getStats().idleExpired, 1);
  void sub;
});
