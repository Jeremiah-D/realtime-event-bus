/**
 * Idle-expiry hook live redirection (EB-430).
 *
 * `setIdleExpiredCallback` swaps the `onIdleExpired` observer every
 * idle-expired subscription is handed to without restarting the bus.
 * There is no enable gate — any open bus can install one — and `null`
 * clears back to no observer. The new hook governs from the next idle
 * expiry onward, the expiry counter moves regardless of whether a hook
 * is installed, and a throwing hook is swallowed by
 * `handleIdleExpiry`. The callback function itself is never written to
 * the audit log: only whether one is installed is recorded
 * (`{ hasCallback, oldHasCallback }`). Re-installing the live callback
 * is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type IdleExpiredEvent } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'idle_expired_callback_revised' });

test('installing a hook routes later expiries to it', async () => {
  const seen: IdleExpiredEvent[] = [];
  const bus = new EventBus();
  // No hook at construction: the first expiry is counted, not observed.
  const first = bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  bus.setIdleExpiredCallback((e) => seen.push(e));
  const second = bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].subscriberId, second.id);
  assert.equal(seen[0].pattern, 't');
  assert.equal(seen[0].maxIdleMs, 40);
  assert.equal(seen[0].idleActivity, 'any');
  assert.equal(bus.getStats().idleExpired, 2);
  assert.notEqual(first.id, second.id);
  bus.destroy?.();
});

test('clearing with null stops delivery and the counter keeps moving', async () => {
  const seen: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => seen.push(e) });
  bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(seen.length, 1);
  bus.setIdleExpiredCallback(null);
  bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().idleExpired, 2);
  // Clearing a bus with no callback is a no-op with no audit.
  const before = audit(bus).length;
  bus.setIdleExpiredCallback(null);
  assert.equal(audit(bus).length, before);
  bus.destroy?.();
});

test('non-functions are rejected and the live hook stays put', async () => {
  const seen: IdleExpiredEvent[] = [];
  const bus = new EventBus({ onIdleExpired: (e) => seen.push(e) });
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const handler of bad) {
    assert.throws(
      () => bus.setIdleExpiredCallback(handler as never),
      RangeError,
      `expected RangeError for ${String(handler)}`,
    );
  }
  // The failed calls changed nothing: the live hook still observes.
  bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(seen.length, 1);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('re-installing the live callback is a no-op with no audit', () => {
  const handler = (_e: IdleExpiredEvent): void => {};
  const bus = new EventBus({ onIdleExpired: handler });
  bus.setIdleExpiredCallback(handler);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('a closed bus refuses the swap', async () => {
  const bus = new EventBus();
  await bus.close();
  const handler = (_e: IdleExpiredEvent): void => {};
  assert.throws(() => bus.setIdleExpiredCallback(handler), RangeError);
  assert.throws(() => bus.setIdleExpiredCallback(null), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('at fire time the subscription is already gone and the counter already moved', async () => {
  let expiredAtFire = -1;
  let subscribersAtFire = -1;
  const bus = new EventBus();
  bus.setIdleExpiredCallback(() => {
    expiredAtFire = bus.getStats().idleExpired;
    subscribersAtFire = bus.getStats().totalSubscribers;
  });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload), { maxIdleMs: 40 });
  bus.publish('t', 'before');
  await flush();
  assert.equal(received.length, 1);
  await sleep(120);
  // The hook saw the expiry after the unsubscribe ran and the counter
  // moved — the subscription was already gone when it fired.
  assert.equal(expiredAtFire, 1);
  assert.equal(subscribersAtFire, 0);
  bus.publish('t', 'after');
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().idleExpired, 1);
  bus.destroy?.();
});

test('a throwing hook is swallowed and the unsubscribe path is unaffected', async () => {
  const bus = new EventBus();
  bus.setIdleExpiredCallback(() => {
    throw new Error('broken observer');
  });
  bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(bus.getStats().totalSubscribers, 0);
  assert.equal(bus.getStats().idleExpired, 1);
  // An admitted publish still flows to a fresh subscriber.
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  assert.equal(bus.publish('t', 2), 1);
  await flush();
  assert.deepEqual(received, [2]);
  bus.destroy?.();
});

test('the swap audits exact presence flags with the actor', () => {
  const first = (_e: IdleExpiredEvent): void => {};
  const second = (_e: IdleExpiredEvent): void => {};
  const bus = new EventBus();
  bus.setIdleExpiredCallback(first, 'ops');
  bus.setIdleExpiredCallback(second, 'ops');
  bus.setIdleExpiredCallback(null, 'ops');
  const entries = audit(bus);
  assert.equal(entries.length, 3);
  assert.deepEqual(entries[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(entries[0].actor, 'ops');
  assert.deepEqual(entries[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(entries[2].detail, { hasCallback: false, oldHasCallback: true });
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(entries).includes('first'));
  bus.destroy?.();
});

test('setIdleExpiredCallback rejects an illegal actor', () => {
  const bus = new EventBus();
  const handler = (_e: IdleExpiredEvent): void => {};
  assert.throws(() => bus.setIdleExpiredCallback(handler, ''), RangeError);
  assert.throws(() => bus.setIdleExpiredCallback(handler, 42 as never), RangeError);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('a restart restores the construction-time callback', async () => {
  const constructionSeen: IdleExpiredEvent[] = [];
  const liveSeen: IdleExpiredEvent[] = [];
  const construction = (e: IdleExpiredEvent) => constructionSeen.push(e);
  const live = (e: IdleExpiredEvent) => liveSeen.push(e);
  // First bus: the live hook replaces the construction-time one.
  const bus = new EventBus({ onIdleExpired: construction });
  bus.setIdleExpiredCallback(live);
  bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(constructionSeen.length, 0);
  assert.equal(liveSeen.length, 1);
  bus.destroy?.();
  // A fresh bus with the same construction options (a restart) fires
  // the construction-time hook again.
  const restarted = new EventBus({ onIdleExpired: construction });
  restarted.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(constructionSeen.length, 1);
  assert.equal(liveSeen.length, 1);
  restarted.destroy?.();
});

test('an unexpired idle window is unaffected by the revision', async () => {
  const seen: IdleExpiredEvent[] = [];
  const bus = new EventBus();
  bus.setIdleExpiredCallback((e) => seen.push(e));
  bus.subscribe('t', noop, { maxIdleMs: 10_000 });
  await sleep(80);
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().totalSubscribers, 1);
  assert.equal(bus.getStats().idleExpired, 0);
  bus.destroy?.();
});

test('two consecutive redirects both take effect', async () => {
  const firstSeen: IdleExpiredEvent[] = [];
  const secondSeen: IdleExpiredEvent[] = [];
  const bus = new EventBus();
  bus.setIdleExpiredCallback((e) => firstSeen.push(e));
  bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(firstSeen.length, 1);
  assert.equal(secondSeen.length, 0);
  bus.setIdleExpiredCallback((e) => secondSeen.push(e));
  bus.subscribe('t', noop, { maxIdleMs: 40 });
  await sleep(120);
  assert.equal(firstSeen.length, 1);
  assert.equal(secondSeen.length, 1);
  assert.equal(audit(bus).length, 2);
  bus.destroy?.();
});
