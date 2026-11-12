import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, BusClosedError } from '../src/bus.ts';
import type { ExpiringSoonEvent } from '../src/expiring.ts';

/**
 * Per-subscriber message TTL cap (EB-480): `SubscribeOptions.subscriberTtlMs`
 * caps how long a message may sit in one subscriber's queue — at enqueue
 * time its effective deadline becomes `min(its existing deadline,
 * enqueueTime + subscriberTtlMs)` — and `setSubscriberTtl` revises the
 * cap live. Expiry still runs the existing TTL path (dropped before
 * delivery, counted as `expired`), and `expiringSoon` judges the capped
 * deadline.
 *
 * Determinism trick (as in `ttl.test.ts`): a manually-advanced clock is
 * handed to the bus via `EventBusOptions.now`, and `bus.publish`
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

test('subscribe-time subscriberTtlMs must be a positive integer; the getter follows it', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { subscriberTtlMs: 100 });
  assert.equal(bus.getSubscriberTtl(sub.id), 100);
  assert.equal(bus.getSubscriberTtl('no-such-sub'), undefined);
  const plain = bus.subscribe('t2', () => {});
  assert.equal(bus.getSubscriberTtl(plain.id), undefined);
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, '100', null, {}]) {
    assert.throws(
      () => bus.subscribe('t3', () => {}, { subscriberTtlMs: bad as never }),
      RangeError,
      `subscriberTtlMs=${String(bad)} should throw`,
    );
  }
});

test('setSubscriberTtl rejects unknown, empty, and invalid revisions fail-closed', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { subscriberTtlMs: 100 });
  for (const bad of [
    () => bus.setSubscriberTtl('no-such-sub', 100),
    () => bus.setSubscriberTtl('', 100),
    () => bus.setSubscriberTtl(sub.id, 0),
    () => bus.setSubscriberTtl(sub.id, -1),
    () => bus.setSubscriberTtl(sub.id, 1.5),
    () => bus.setSubscriberTtl(sub.id, Number.NaN),
    () => bus.setSubscriberTtl(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setSubscriberTtl(sub.id, '100' as never),
    () => bus.setSubscriberTtl(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(bus.getSubscriberTtl(sub.id), 100, 'a failed revision must not move the live cap');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_ttl_revised' }).length,
    0,
    'failed revisions must not audit',
  );
});

test('a cap shorter than the topic TTL expires the message early, counted as expired', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000); // topic deadline t=11000
  const capped: unknown[] = [];
  const plain: unknown[] = [];
  bus.subscribe('t', (msg) => capped.push(msg.payload), { subscriberTtlMs: 100 }); // cap deadline t=1100
  bus.subscribe('t', (msg) => plain.push(msg.payload));
  bus.publish('t', 'stale-for-capped');
  clock.nowMs = 1_200; // past the cap, inside the topic TTL
  await flush();
  assert.deepEqual(capped, [], 'the capped subscriber never sees the message');
  assert.deepEqual(plain, ['stale-for-capped'], 'the uncapped subscriber still receives it');
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('a cap longer than the message deadline never extends it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), { subscriberTtlMs: 10_000 });
  bus.publish('t', 'short-lived', { ttlMs: 100 }); // deadline t=1100, cap would allow t=11000
  clock.nowMs = 1_200; // past the message deadline, inside the cap
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('null clears back to the topic TTL; already-queued messages keep their deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000); // topic deadline t=11000
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { subscriberTtlMs: 100 });
  bus.publish('t', 'admitted-under-cap'); // capped deadline t=1100
  bus.setSubscriberTtl(sub.id, null); // clear before the drain
  bus.publish('t', 'admitted-after-clear'); // topic deadline t=11000
  clock.nowMs = 1_200; // past the old cap, inside the topic TTL
  await flush();
  assert.deepEqual(
    received,
    ['admitted-after-clear'],
    'the message admitted under the cap still expires; the one admitted after the clear does not',
  );
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.equal(bus.getSubscriberTtl(sub.id), undefined);
});

test('a live revision governs only messages enqueued after it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000); // topic deadline t=11000
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'before-revision'); // no cap: deadline t=11000
  bus.setSubscriberTtl(sub.id, 100); // cap deadline for new arrivals: t=1100
  bus.publish('t', 'after-revision'); // capped deadline t=1100
  clock.nowMs = 1_200; // past the cap, inside the topic TTL
  await flush();
  assert.deepEqual(received, ['before-revision']);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('revising to the current cap is a no-op with no audit, including clearing an unset cap', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  bus.setSubscriberTtl(sub.id, null);
  assert.equal(bus.getSubscriptionAudit({ action: 'subscriber_ttl_revised' }).length, 0);
  bus.setSubscriberTtl(sub.id, 30_000, 'ops');
  bus.setSubscriberTtl(sub.id, 30_000, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'subscriber_ttl_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'ops');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, { ttl_ms: 30_000, previous_ttl_ms: null });
  bus.setSubscriberTtl(sub.id, null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'subscriber_ttl_revised' });
  assert.equal(cleared.length, 2);
  assert.deepEqual(cleared[1].detail, { ttl_ms: null, previous_ttl_ms: 30_000 });
  // Clearing an already-unset cap is a no-op too.
  bus.setSubscriberTtl(sub.id, null);
  assert.equal(bus.getSubscriptionAudit({ action: 'subscriber_ttl_revised' }).length, 2);
});

test('expiringSoon judges the capped deadline, not the topic deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 10_000); // topic deadline t=11000
  const events: ExpiringSoonEvent[] = [];
  bus.subscribe('t', () => {}, {
    subscriberTtlMs: 100, // capped deadline t=1100
    expiringSoon: { expiringSoonMs: 50, onExpiringSoon: (e) => events.push(e) },
  });
  bus.publish('t', 'm1'); // remaining 100 at enqueue: outside the 50ms window
  assert.equal(events.length, 0);
  clock.nowMs = 1_060; // head remaining 40: inside the window
  bus.publish('t', 'm2'); // the enqueue evaluation sees the capped head
  assert.equal(events.length, 1);
  assert.equal(events[0].remainingMs, 40);
  await flush();
});

test('a closed bus refuses the revision with BusClosedError', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { subscriberTtlMs: 100 });
  const closed = bus.close();
  assert.throws(() => bus.setSubscriberTtl(sub.id, 200), BusClosedError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_ttl_revised' }).length,
    0,
    'the refused revision must not audit',
  );
  await closed;
});
