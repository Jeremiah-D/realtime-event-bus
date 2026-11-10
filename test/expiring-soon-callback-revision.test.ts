import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { ExpiringSoonEvent } from '../src/expiring.ts';

/**
 * Expiring-soon callback revision (EB-454): `setExpiringSoonCallback`
 * swaps a monitored subscriber's `onExpiringSoon` without
 * re-subscribing — the only callback revision surface
 * `setExpiringSoonThreshold` (EB-288) left uncovered: it moves the
 * threshold, this one moves who hears about the warning. Only the
 * callback moves: the threshold, the `alerted` latch, and the alert
 * count are untouched, and a throwing callback is swallowed by
 * `checkExpiringSoon`, so observation never breaks the publish/drain
 * path.
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

/** The `expiring_soon_callback_revised` audit events on the bus. */
function callbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'expiring_soon_callback_revised' });
}

/**
 * Builds a bus with one expiring-soon-monitored subscriber on topic
 * `t` (TTL 200ms, warning window 100ms) and publishes the first
 * message at the starting clock, so the head's deadline is 1200.
 */
function busWithMonitoredSub(onExpiringSoon: (e: ExpiringSoonEvent) => void) {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const sub = bus.subscribe('t', () => {}, {
    expiringSoon: { expiringSoonMs: 100, onExpiringSoon },
  });
  bus.publish('t', 'm1'); // deadline 1200
  return { clock, bus, sub };
}

/** Moves the clock so the head sits inside the warning window (remaining 50). */
function enterWarningWindow(clock: { nowMs: number }) {
  clock.nowMs = 1150; // head remaining 50 <= 100
}

test('replacement routes the next warning only to the new callback, with exact event fields', async () => {
  const first: ExpiringSoonEvent[] = [];
  const second: ExpiringSoonEvent[] = [];
  const { clock, bus, sub } = busWithMonitoredSub((e) => first.push(e));
  bus.setExpiringSoonCallback(sub.id, (e) => second.push(e));
  const audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, 't');
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: true });
  // The next enqueue is judged by the new callback: the head (remaining
  // 50) is inside the window and warns at once.
  enterWarningWindow(clock);
  bus.publish('t', 'm2');
  assert.equal(first.length, 0, 'the replaced callback must hear nothing');
  assert.equal(second.length, 1);
  assert.equal(second[0].subscriberId, sub.id);
  assert.equal(second[0].pattern, 't');
  assert.equal(second[0].remainingMs, 50);
  assert.equal(second[0].expiringSoonMs, 100);
  assert.equal(second[0].queueDepth, 2);
  assert.equal(second[0].at, 1150);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  await flush();
  sub.unsubscribe();
});

test('null clears the callback: the warning goes silent but the count and latch still run', async () => {
  const events: ExpiringSoonEvent[] = [];
  const { clock, bus, sub } = busWithMonitoredSub((e) => events.push(e));
  bus.setExpiringSoonCallback(sub.id, null);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: false,
    oldHasCallback: true,
  });
  enterWarningWindow(clock);
  bus.publish('t', 'm2'); // fires silently: nobody receives it
  assert.equal(events.length, 0);
  // The threshold judgment and the count still ran, and the latch is
  // set for this backlog — a further enqueue does not warn again.
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  bus.publish('t', 'm3');
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  await flush(); // queue drains -> latch re-arms
  // A fresh backlog warns silently again under the cleared callback.
  bus.publish('t', 'm4'); // deadline 1350, remaining 200 > 100: no warning yet
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  clock.nowMs = 1300; // head remaining 50 <= 100
  bus.publish('t', 'm5');
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().expiringSoonAlerts, 2);
  await flush();
  sub.unsubscribe();
});

test('unknown, empty, unmonitored, non-function, and closed-bus revisions throw RangeError fail-closed', async () => {
  const events: ExpiringSoonEvent[] = [];
  const { clock, bus, sub } = busWithMonitoredSub((e) => events.push(e));
  const plain = bus.subscribe('t2', () => {});
  const replacement = (_e: ExpiringSoonEvent) => {};
  for (const bad of [
    () => bus.setExpiringSoonCallback('no-such-sub', replacement),
    () => bus.setExpiringSoonCallback('', replacement),
    () => bus.setExpiringSoonCallback(plain.id, replacement),
    () => bus.setExpiringSoonCallback(sub.id, 'not-a-function' as never),
    () => bus.setExpiringSoonCallback(sub.id, 42 as never),
    () => bus.setExpiringSoonCallback(sub.id, undefined as never),
    () => bus.setExpiringSoonCallback(sub.id, {} as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'failed revisions must not audit',
  );
  // The live callback is untouched: a head inside the window still
  // warns through the original callback.
  enterWarningWindow(clock);
  bus.publish('t', 'm2');
  assert.equal(events.length, 1);
  assert.equal(events[0].remainingMs, 50);
  await flush();
  sub.unsubscribe();
  plain.unsubscribe();

  // A closed bus refuses the revision (EB-154).
  const closed = busWithMonitoredSub((e) => events.push(e));
  closed.bus.close();
  assert.throws(
    () => closed.bus.setExpiringSoonCallback(closed.sub.id, replacement),
    /bus is closed/,
  );
  assert.equal(
    callbackRevisionAudits(closed.bus).length,
    0,
    'a closed-bus refusal must not audit',
  );
});

test('same-reference reset and null-on-cleared are no-ops with no audit', async () => {
  const events: ExpiringSoonEvent[] = [];
  const original = (e: ExpiringSoonEvent) => events.push(e);
  const { clock, bus, sub } = busWithMonitoredSub(original);
  bus.setExpiringSoonCallback(sub.id, original);
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'a same-reference revision must not audit',
  );
  // Clearing twice: the second clear is a no-op too.
  bus.setExpiringSoonCallback(sub.id, null);
  assert.equal(callbackRevisionAudits(bus).length, 1);
  bus.setExpiringSoonCallback(sub.id, null);
  assert.equal(
    callbackRevisionAudits(bus).length,
    1,
    'null on an already-cleared callback must not audit',
  );
  // The cleared state is live: warnings stay silent.
  enterWarningWindow(clock);
  bus.publish('t', 'm2');
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  await flush();
  sub.unsubscribe();
});

test('an already-warned in-flight backlog is not re-sent to the new callback', async () => {
  const first: ExpiringSoonEvent[] = [];
  const second: ExpiringSoonEvent[] = [];
  const { clock, bus, sub } = busWithMonitoredSub((e) => first.push(e));
  enterWarningWindow(clock);
  bus.publish('t', 'm2'); // fires through the original callback; latch set
  assert.equal(first.length, 1);
  bus.setExpiringSoonCallback(sub.id, (e) => second.push(e));
  bus.publish('t', 'm3'); // latch set: no second warning for the same backlog
  assert.equal(first.length, 1);
  assert.equal(second.length, 0);
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  await flush();
  sub.unsubscribe();
});

test('after the queue drains and re-arms, the new callback receives the next warning', async () => {
  const first: ExpiringSoonEvent[] = [];
  const second: ExpiringSoonEvent[] = [];
  const { clock, bus, sub } = busWithMonitoredSub((e) => first.push(e));
  enterWarningWindow(clock);
  bus.publish('t', 'm2'); // warns through the original callback
  assert.equal(first.length, 1);
  bus.setExpiringSoonCallback(sub.id, (e) => second.push(e));
  await flush(); // queue drains -> latch re-arms
  // A fresh backlog is judged by the new callback: the new head is
  // already inside the window and warns at once.
  bus.publish('t', 'm3'); // deadline 1350, remaining 200 > 100: no warning yet
  assert.equal(second.length, 0);
  clock.nowMs = 1300; // head remaining 50 <= 100
  bus.publish('t', 'm4');
  assert.equal(first.length, 1, 'the replaced callback must hear nothing more');
  assert.equal(second.length, 1);
  assert.equal(second[0].remainingMs, 50);
  assert.equal(second[0].expiringSoonMs, 100);
  assert.equal(bus.getStats().expiringSoonAlerts, 2);
  await flush();
  sub.unsubscribe();
});

test('a throwing new callback is swallowed and publish/drain are unaffected', async () => {
  const delivered: unknown[] = [];
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 200);
  const sub = bus.subscribe('t', (m) => delivered.push(m.payload), {
    expiringSoon: {
      expiringSoonMs: 100,
      onExpiringSoon: () => {},
    },
  });
  bus.setExpiringSoonCallback(sub.id, () => {
    throw new Error('alerting is down');
  });
  bus.publish('t', 'm1'); // deadline 1200
  clock.nowMs = 1150; // head remaining 50
  bus.publish('t', 'm2'); // the warning fires and throws — swallowed
  assert.equal(bus.getStats().expiringSoonAlerts, 1);
  // Publishing past the warning still works, and the drain still
  // delivers everything.
  bus.publish('t', 'm3');
  await flush();
  assert.deepEqual(delivered, ['m1', 'm2', 'm3']);
  sub.unsubscribe();
});

test('threshold revision is unaffected by callback revision, and vice versa', async () => {
  const events: ExpiringSoonEvent[] = [];
  const { clock, bus, sub } = busWithMonitoredSub((e) => events.push(e));
  const replacement = (e: ExpiringSoonEvent) => events.push(e);
  bus.setExpiringSoonCallback(sub.id, replacement);
  // The threshold still revises through its own entry (EB-288) and
  // the warning is judged by the new threshold and heard by the new
  // callback.
  bus.setExpiringSoonThreshold(sub.id, 500);
  assert.deepEqual(
    bus.getSubscriptionAudit({ action: 'expiring_soon_threshold_revised' })[0].detail,
    { expiringSoonMs: 500, oldExpiringSoonMs: 100 },
  );
  bus.publish('t', 'm2'); // head remaining 200 <= 500 under the new threshold
  assert.equal(events.length, 1);
  assert.equal(events[0].remainingMs, 200);
  assert.equal(events[0].expiringSoonMs, 500);
  // And a callback revision after a threshold revision leaves the
  // threshold alone.
  bus.setExpiringSoonCallback(sub.id, null);
  bus.setExpiringSoonCallback(sub.id, replacement);
  assert.deepEqual(
    bus.getSubscriptionAudit({ action: 'expiring_soon_threshold_revised' })[0].detail,
    { expiringSoonMs: 500, oldExpiringSoonMs: 100 },
  );
  await flush();
  sub.unsubscribe();
});

test('the audit detail carries only callback presence, never function values', async () => {
  const { bus, sub } = busWithMonitoredSub(() => {});
  const replacement = (_e: ExpiringSoonEvent) => {};
  bus.setExpiringSoonCallback(sub.id, replacement);
  bus.setExpiringSoonCallback(sub.id, null);
  bus.setExpiringSoonCallback(sub.id, replacement);
  const audits = callbackRevisionAudits(bus);
  assert.equal(audits.length, 3);
  assert.deepEqual(audits[0].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(audits[1].detail, { hasCallback: false, oldHasCallback: true });
  assert.deepEqual(audits[2].detail, { hasCallback: true, oldHasCallback: false });
  for (const audit of audits) {
    for (const value of Object.values(audit.detail as Record<string, unknown>)) {
      assert.equal(typeof value, 'boolean', 'audit detail must hold only booleans');
    }
  }
  sub.unsubscribe();
});
