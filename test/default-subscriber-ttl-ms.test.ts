import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level terminal per-subscriber message TTL cap default (EB-481):
 * `setDefaultSubscriberTtlMs` installs the `subscriberTtlMs` new
 * subscribers fall back to when they omit their own (EB-480) — the
 * priority chain is subscriber-explicit > bus default > no cap — and
 * `setSubscriberTtl(subId, null)` now clears to the bus default in
 * effect at revision time.
 *
 * Determinism trick (as in `subscriber-ttl.test.ts`): a manually-advanced
 * clock is handed to the bus via `EventBusOptions.now`, and `bus.publish`
 * enqueues synchronously while the drain only runs on the microtask
 * flush — so several publishes in a row build a real backlog
 * synchronously and the clock can move before the drain judges it.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('EB-481: the factory bus default is unset (omitting subscriberTtlMs means no cap)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultSubscriberTtlMs(), undefined);
  const sub = bus.subscribe('t', () => {});
  assert.equal(bus.getSubscriberTtl(sub.id), undefined);
});

test('EB-481: a new subscriber takes the bus default, and a cap shorter than the topic TTL expires it early, counted as expired', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000); // topic deadline t=11000
  bus.setDefaultSubscriberTtlMs(100); // cap deadline t=1100
  const capped: unknown[] = [];
  const plain: unknown[] = [];
  const cappedSub = bus.subscribe('t', (msg) => capped.push(msg.payload));
  bus.subscribe('t', (msg) => plain.push(msg.payload), { subscriberTtlMs: 10_000 });
  assert.equal(bus.getSubscriberTtl(cappedSub.id), 100);
  bus.publish('t', 'stale-for-default-capped');
  clock.nowMs = 1_200; // past the default cap, inside the topic TTL
  await flush();
  assert.deepEqual(capped, [], 'the default-capped subscriber never sees the message');
  assert.deepEqual(plain, ['stale-for-default-capped'], 'the explicitly-capped subscriber still receives it');
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('EB-481: an explicit subscriberTtlMs beats the bus default in both directions', async () => {
  // Explicit 10_000 beats a bus default of 100: the message survives.
  const looseClock = controllableClock();
  const loose = new EventBus({ now: looseClock.now });
  loose.setTopicTtl('t', 10_000);
  loose.setDefaultSubscriberTtlMs(100);
  const looseSeen: unknown[] = [];
  loose.subscribe('t', (msg) => looseSeen.push(msg.payload), { subscriberTtlMs: 10_000 });
  loose.publish('t', 'explicit-wins');
  looseClock.nowMs = 1_200;
  await flush();
  assert.deepEqual(looseSeen, ['explicit-wins']);

  // Explicit 100 beats a bus default of 10_000: the message expires.
  const strictClock = controllableClock();
  const strict = new EventBus({ now: strictClock.now });
  strict.setTopicTtl('t', 10_000);
  strict.setDefaultSubscriberTtlMs(10_000);
  const strictSeen: unknown[] = [];
  strict.subscribe('t', (msg) => strictSeen.push(msg.payload), { subscriberTtlMs: 100 });
  strict.publish('t', 'explicit-wins-too');
  strictClock.nowMs = 1_200;
  await flush();
  assert.deepEqual(strictSeen, []);
  assert.equal(strict.getStats().expiredMessages, 1);
});

test('EB-481: clearing with null returns new subscribers to no cap', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000);
  bus.setDefaultSubscriberTtlMs(100);
  bus.setDefaultSubscriberTtlMs(null);
  assert.equal(bus.getDefaultSubscriberTtlMs(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  assert.equal(bus.getSubscriberTtl(sub.id), undefined);
  bus.publish('t', 'uncapped-again');
  clock.nowMs = 1_200; // past the old default cap, inside the topic TTL
  await flush();
  assert.deepEqual(received, ['uncapped-again']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('EB-481: a cap longer than the message deadline never extends it, even when it comes from the bus default', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultSubscriberTtlMs(10_000);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'short-lived', { ttlMs: 100 }); // deadline t=1100, default cap would allow t=11000
  clock.nowMs = 1_200; // past the message deadline, inside the default cap
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('EB-481: revising the bus default never moves an already-subscribed cap or an already-queued message deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000); // topic deadline t=11000
  bus.setDefaultSubscriberTtlMs(100);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  assert.equal(bus.getSubscriberTtl(sub.id), 100);
  bus.publish('t', 'admitted-under-100'); // capped deadline t=1100
  bus.setDefaultSubscriberTtlMs(10_000); // new default must not move the baked cap
  assert.equal(bus.getSubscriberTtl(sub.id), 100, 'the revision never moves an already-subscribed cap');
  bus.publish('t', 'admitted-under-100-too'); // still capped at t=1100
  clock.nowMs = 1_200; // past the baked cap, inside the topic TTL
  await flush();
  assert.deepEqual(received, [], 'both messages keep the deadline they were admitted with');
  assert.equal(bus.getStats().expiredMessages, 2);

  // A subscriber joining after the revision takes the new default.
  const lateSeen: unknown[] = [];
  const late = bus.subscribe('u', (msg) => lateSeen.push(msg.payload));
  assert.equal(bus.getSubscriberTtl(late.id), 10_000);
});

test('EB-481: live revisions govern only messages enqueued after them', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000); // topic deadline t=11000
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'before-default'); // no cap: deadline t=11000
  bus.setDefaultSubscriberTtlMs(100); // a cap for future subscribers only
  bus.publish('t', 'after-default-same-subscriber'); // the live subscriber keeps its baked no-cap
  clock.nowMs = 1_200; // past 100ms, inside the topic TTL
  await flush();
  assert.deepEqual(
    received,
    ['before-default', 'after-default-same-subscriber'],
    'the bus default never moves a live subscriber — only a new subscription takes it',
  );
  assert.equal(bus.getStats().expiredMessages, 0);

  // A subscriber joining after the revision takes the new default.
  const lateSeen: unknown[] = [];
  bus.subscribe('u', (msg) => lateSeen.push(msg.payload));
  bus.publish('u', 'late-joiner');
  clock.nowMs = 1_400; // past the late joiner's t=1300 cap deadline (enqueued at 1200)
  await flush();
  assert.deepEqual(lateSeen, [], 'the late joiner is capped at the live default');
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('EB-481: setSubscriberTtl(null) clears to the bus default in effect at revision time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000); // topic deadline t=11000
  bus.setDefaultSubscriberTtlMs(100);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { subscriberTtlMs: 10_000 });
  assert.equal(bus.getSubscriberTtl(sub.id), 10_000);
  bus.setSubscriberTtl(sub.id, null);
  assert.equal(bus.getSubscriberTtl(sub.id), 100, 'null clears to the live bus default, not to no cap');
  bus.publish('t', 'cleared-to-default'); // capped deadline t=1100
  clock.nowMs = 1_200; // past the default cap, inside the topic TTL
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);

  // With no default installed, null clears back to no cap — the old behavior.
  bus.setDefaultSubscriberTtlMs(null);
  bus.setSubscriberTtl(sub.id, null);
  assert.equal(bus.getSubscriberTtl(sub.id), undefined);
  bus.publish('t', 'cleared-to-no-cap');
  clock.nowMs = 1_300; // inside the topic TTL
  await flush();
  assert.deepEqual(received, ['cleared-to-no-cap']);
});

test('EB-481: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberTtlMs(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_subscriber_ttl_ms_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultSubscriberTtlMs(100);
  assert.equal(bus.getDefaultSubscriberTtlMs(), 100);
  bus.setDefaultSubscriberTtlMs(100);
  assert.equal(bus.getDefaultSubscriberTtlMs(), 100);
  const audit = bus.getSubscriptionAudit({ action: 'default_subscriber_ttl_ms_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, subscriberTtlMs: 100 });
  bus.setDefaultSubscriberTtlMs(200, 'ops');
  assert.equal(bus.getDefaultSubscriberTtlMs(), 200);
  const revised = bus.getSubscriptionAudit({ action: 'default_subscriber_ttl_ms_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, { previous: 100, subscriberTtlMs: 200 });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultSubscriberTtlMs(null);
  assert.equal(bus.getDefaultSubscriberTtlMs(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_subscriber_ttl_ms_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, { previous: 200, subscriberTtlMs: null });
});

test('EB-481: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultSubscriberTtlMs(100);
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, '100', {}] as unknown[]) {
    assert.throws(
      () => bus.setDefaultSubscriberTtlMs(bad as number),
      RangeError,
      `ms=${String(bad)}`,
    );
    assert.equal(bus.getDefaultSubscriberTtlMs(), 100);
  }
  assert.throws(() => bus.setDefaultSubscriberTtlMs(200, ''), RangeError);
  assert.equal(bus.getDefaultSubscriberTtlMs(), 100);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_subscriber_ttl_ms_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});
