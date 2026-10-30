import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { ExpiringSoonEvent } from '../src/expiring.ts';

/**
 * Expiring-soon threshold revision (EB-288): `setExpiringSoonThreshold`
 * retunes a monitored subscriber's warning window without
 * re-subscribing — only the threshold moves, the `onExpiringSoon`
 * callback and the `alerted` latch are untouched, and the new
 * threshold governs from the next enqueue/drain evaluation.
 *
 * Determinism trick (as in `expiring.test.ts`): `bus.publish`
 * enqueues synchronously and only schedules the drain as a
 * microtask, so several publishes in a row build a real backlog
 * synchronously — the warning is evaluated at each enqueue,
 * exactly like the production path.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('setExpiringSoonThreshold rejects unknown, empty, unmonitored, and invalid revisions fail-closed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const events: ExpiringSoonEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) },
  });
  const plain = bus.subscribe('t2', () => {});
  for (const bad of [
    () => bus.setExpiringSoonThreshold('no-such-sub', 100),
    () => bus.setExpiringSoonThreshold('', 100),
    () => bus.setExpiringSoonThreshold(plain.id, 100),
    () => bus.setExpiringSoonThreshold(sub.id, -1),
    () => bus.setExpiringSoonThreshold(sub.id, Number.NaN),
    () => bus.setExpiringSoonThreshold(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setExpiringSoonThreshold(sub.id, '100' as never),
    () => bus.setExpiringSoonThreshold(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'expiring_soon_threshold_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live threshold is untouched: a head inside the old window
  // still warns at the old threshold.
  bus.publish('t', 'm1'); // deadline 1200
  clock.nowMs = 1150; // head remaining 50 <= 100
  bus.publish('t', 'm2');
  assert.equal(events.length, 1);
  assert.equal(events[0].expiringSoonMs, 100);
  assert.equal(events[0].remainingMs, 50);
  await flush();
  sub.unsubscribe();
  plain.unsubscribe();
});

test('widening the threshold warns on the next enqueue evaluation', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 1000);
  const events: ExpiringSoonEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) },
  });
  bus.publish('t', 'm1'); // deadline 2000
  clock.nowMs = 1700; // head remaining 300 > 100
  bus.publish('t', 'm2'); // enqueue evaluation: still outside the window
  assert.equal(events.length, 0);
  bus.setExpiringSoonThreshold(sub.id, 500);
  const audit = bus.getSubscriptionAudit({ action: 'expiring_soon_threshold_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, 't');
  assert.deepEqual(audit[0].detail, { expiringSoonMs: 500, oldExpiringSoonMs: 100 });
  // The next enqueue is judged by the new threshold: the same head
  // (remaining 300) now sits inside the widened window.
  bus.publish('t', 'm3');
  assert.equal(events.length, 1);
  assert.equal(events[0].remainingMs, 300);
  assert.equal(events[0].expiringSoonMs, 500);
  assert.equal(events[0].queueDepth, 3);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  await flush();
  sub.unsubscribe();
});

test('tightening the threshold stops warning at the next evaluation, widening back re-arms it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 1000);
  const events: ExpiringSoonEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 500, onExpiringSoon: (e) => events.push(e) },
  });
  bus.publish('t', 'm1'); // deadline 2000
  clock.nowMs = 1700; // head remaining 300
  bus.setExpiringSoonThreshold(sub.id, 100);
  assert.deepEqual(
    bus.getSubscriptionAudit({ action: 'expiring_soon_threshold_revised' })[0].detail,
    { expiringSoonMs: 100, oldExpiringSoonMs: 500 },
  );
  bus.publish('t', 'm2'); // 300 > 100: no warning under the tightened threshold
  assert.equal(events.length, 0);
  bus.setExpiringSoonThreshold(sub.id, 400);
  bus.publish('t', 'm3'); // 300 <= 400: the same head warns at once
  assert.equal(events.length, 1);
  assert.equal(events[0].remainingMs, 300);
  assert.equal(events[0].expiringSoonMs, 400);
  await flush();
  sub.unsubscribe();
});

test('a widened threshold does not re-fire an already-warned backlog until the queue drains', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const events: ExpiringSoonEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) },
  });
  bus.publish('t', 'm1'); // deadline 1200
  clock.nowMs = 1150; // head remaining 50
  bus.publish('t', 'm2'); // fires; the latch is now set for this backlog
  assert.equal(events.length, 1);
  bus.setExpiringSoonThreshold(sub.id, 500);
  bus.publish('t', 'm3'); // latch set: no second warning for the same backlog
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  await flush(); // queue drains -> latch re-arms
  // A fresh backlog is judged by the new threshold: the new head is
  // already inside the widened window and warns at once.
  bus.publish('t', 'm4'); // deadline 1350, remaining 200 <= 500
  assert.equal(events.length, 2);
  assert.equal(events[1].remainingMs, 200);
  assert.equal(events[1].expiringSoonMs, 500);
  assert.equal(bus.getStats().expiringSoonAlerts, 2);
  await flush();
  sub.unsubscribe();
});

test('re-affirming the current threshold is a no-op with no audit', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const events: ExpiringSoonEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) },
  });
  bus.setExpiringSoonThreshold(sub.id, 100);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'expiring_soon_threshold_revised' }).length,
    0,
    'a same-threshold revision must not audit',
  );
  // The threshold is unchanged: the old window still warns.
  bus.publish('t', 'm1'); // deadline 1200
  clock.nowMs = 1150; // head remaining 50 <= 100
  bus.publish('t', 'm2');
  assert.equal(events.length, 1);
  assert.equal(events[0].expiringSoonMs, 100);
  await flush();
  sub.unsubscribe();
});

test('a zero threshold only warns on an already-expired head', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const events: ExpiringSoonEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) },
  });
  bus.setExpiringSoonThreshold(sub.id, 0);
  bus.publish('t', 'm1'); // deadline 1200
  clock.nowMs = 1150; // head remaining 50 > 0: no warning at a zero threshold
  bus.publish('t', 'm2');
  assert.equal(events.length, 0);
  clock.nowMs = 1250; // head past its deadline: remaining -50 <= 0
  bus.publish('t', 'm3');
  assert.equal(events.length, 1);
  assert.equal(events[0].remainingMs, -50);
  assert.equal(events[0].expiringSoonMs, 0);
  await flush();
  sub.unsubscribe();
});

test('the drain-path evaluation reads the revised threshold (partial drain)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 1000);
  const events: ExpiringSoonEvent[] = [];
  // A controllable handler: each delivery parks on a promise the test
  // resolves, so `maxInflight: 1` stops the drain after one message
  // and the post-drain evaluation runs against the head left queued.
  const gates: Array<() => void> = [];
  const sub = bus.subscribe(
    't',
    () => new Promise<void>((resolve) => gates.push(resolve)),
    {
      maxInflight: 1,
      expiringSoon: { expiringSoonMs: 100, onExpiringSoon: (e) => events.push(e) },
    },
  );
  bus.publish('t', 'm1'); // deadline 2000
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  await flush(); // delivers m1, stops; post-drain head m2 remaining 1000 > 100
  assert.equal(events.length, 0);
  clock.nowMs = 1700; // head m2 remaining 300
  bus.setExpiringSoonThreshold(sub.id, 500);
  // Settling m1 frees the slot and schedules a flush: the drain
  // delivers m2, stops again, and the post-drain evaluation judges
  // the new head m3 by the revised threshold — with no new publish.
  gates[0]();
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].remainingMs, 300);
  assert.equal(events[0].expiringSoonMs, 500);
  assert.equal(events[0].queueDepth, 1);
  gates[1](); // let the rest drain so the test bus is left quiescent
  await flush();
  sub.unsubscribe();
});
