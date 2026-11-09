import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { LatencySloMissEvent } from '../src/bus.ts';

/**
 * Processing-latency SLO miss-callback revision (EB-437):
 * `setLatencySloMissCallback` swaps a monitored subscriber's
 * `onLatencySloMiss` without re-subscribing — the only `latencySlo`
 * revision surface EB-290 (threshold) and EB-368 (window size) left
 * uncovered. Only the callback moves: the threshold, the tracker
 * window and its samples, and the `alerted` latch are untouched.
 * `null` clears back to latch-and-ops-alert-only reporting — a p99
 * over the threshold still sets the latch and still counts the miss,
 * just with no callback fired — and clearing during an over-threshold
 * excursion does not re-arm the latch.
 *
 * Determinism trick (as in `latency-slo-threshold.test.ts`): the handler
 * advances a controllable clock, so every sample is an exact,
 * deterministic processing time.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Reads the one processing-latency SLO stats entry for a pattern. */
function sloEntry(bus: EventBus, pattern: string) {
  const entries = bus.getStats().subscriberLatencyP99.filter((e) => e.pattern === pattern);
  assert.equal(entries.length, 1, `expected one subscriberLatencyP99 entry for ${pattern}`);
  return entries[0];
}

/** The `latency_slo_miss_callback_revised` audit events on the bus. */
function callbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'latency_slo_miss_callback_revised' });
}

test('swapping the callback routes the next over-threshold miss to the new callback only', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const first: LatencySloMissEvent[] = [];
  const second: LatencySloMissEvent[] = [];
  let duration = 25;
  const sub = bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: {
      windowSize: 5,
      p99ThresholdMs: 15,
      onLatencySloMiss: (e) => first.push(e),
    },
  });
  // Five slow samples: p99 = 25, breaching — the first callback fires.
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(first.length, 1);
  assert.equal(first[0].p99Ms, 25);
  assert.equal(first[0].thresholdMs, 15);
  assert.equal(sloEntry(bus, 't').breaching, true);

  // Five fast samples re-arm the latch without firing.
  duration = 5;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(first.length, 1);
  assert.equal(sloEntry(bus, 't').breaching, false);

  // Swap in the second callback: the next excursion reaches it only.
  bus.setLatencySloMissCallback(sub.id, (e) => second.push(e));
  duration = 25;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(first.length, 1, 'the old callback never fires again');
  assert.equal(second.length, 1);
  assert.equal(second[0].p99Ms, 25);
  assert.equal(second[0].samples, 5);
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: true,
    oldHasCallback: true,
  });
  sub.unsubscribe();
});

test('clearing with null stops delivery but the latch and the miss count still walk', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, processingLatencySloAlert: true });
  const events: LatencySloMissEvent[] = [];
  let duration = 25;
  const sub = bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: {
      windowSize: 5,
      p99ThresholdMs: 15,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().sloMisses, 1);

  // Clear the callback, re-arm the latch with fast samples, then
  // breach again: no callback fires, but the latch sets and the
  // miss is still counted.
  bus.setLatencySloMissCallback(sub.id, null);
  duration = 5;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(sloEntry(bus, 't').breaching, false);
  duration = 25;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1, 'no callback fires after clearing');
  assert.equal(sloEntry(bus, 't').breaching, true, 'the latch still walks');
  assert.equal(bus.getStats().sloMisses, 2, 'the miss is still counted');
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: false,
    oldHasCallback: true,
  });
  sub.unsubscribe();
});

test('clearing during an over-threshold excursion does not re-arm the latch', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  let duration = 25;
  const sub = bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: {
      windowSize: 5,
      p99ThresholdMs: 15,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(sloEntry(bus, 't').breaching, true);

  // Clear while still breaching: more slow samples keep the latch
  // set without firing anything.
  bus.setLatencySloMissCallback(sub.id, null);
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(sloEntry(bus, 't').breaching, true);

  // Re-installing while still breaching does not re-fire either —
  // the p99 must fall back below the threshold first.
  const reinstalled: LatencySloMissEvent[] = [];
  bus.setLatencySloMissCallback(sub.id, (e) => reinstalled.push(e));
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.equal(reinstalled.length, 0);
  assert.equal(sloEntry(bus, 't').breaching, true);

  // Fast samples re-arm the latch; the next excursion fires the
  // re-installed callback.
  duration = 5;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(sloEntry(bus, 't').breaching, false);
  duration = 25;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(reinstalled.length, 1);
  assert.equal(reinstalled[0].p99Ms, 25);
  sub.unsubscribe();
});

test('threshold and window revisions are unaffected by a callback revision', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: {
      windowSize: 10,
      p99ThresholdMs: 100,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 0); // p99 = 25, under the threshold

  // The callback swap moves nothing else.
  const replacement: LatencySloMissEvent[] = [];
  bus.setLatencySloMissCallback(sub.id, (e) => replacement.push(e));
  let entry = sloEntry(bus, 't');
  assert.equal(entry.thresholdMs, 100, 'the threshold survives the callback revision');
  assert.equal(entry.windowSize, 10, 'the window survives the callback revision');
  assert.equal(entry.samples, 10, 'the samples survive the callback revision');
  assert.equal(events.length, 0);
  assert.equal(replacement.length, 0, 'a swap alone never evaluates the SLO');

  // Lowering the threshold afterwards still fires the new callback
  // on the next sample, and the window revision still governs.
  bus.setLatencySloThreshold(sub.id, 20);
  bus.setLatencySloWindowSize(sub.id, 4);
  entry = sloEntry(bus, 't');
  assert.equal(entry.thresholdMs, 20);
  assert.equal(entry.windowSize, 4);
  assert.equal(entry.samples, 4);
  bus.publish('t', 'next');
  await flush();
  assert.equal(events.length, 0, 'the old callback never fires again');
  assert.equal(replacement.length, 1);
  assert.equal(replacement[0].p99Ms, 25);
  assert.equal(replacement[0].thresholdMs, 20);
  assert.equal(replacement[0].samples, 4, 'the window stays bounded at 4');
  sub.unsubscribe();
});

test('setLatencySloMissCallback rejects unknown, empty, unmonitored, and invalid revisions fail-closed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: {
      windowSize: 5,
      p99ThresholdMs: 15,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  const plain = bus.subscribe('t2', () => {});
  const replacement = (_e: LatencySloMissEvent): void => {};
  for (const bad of [
    () => bus.setLatencySloMissCallback('no-such-sub', replacement),
    () => bus.setLatencySloMissCallback('', replacement),
    () => bus.setLatencySloMissCallback(123 as never, replacement),
    () => bus.setLatencySloMissCallback(plain.id, replacement),
    () => bus.setLatencySloMissCallback(sub.id, 'nope' as never),
    () => bus.setLatencySloMissCallback(sub.id, 42 as never),
    () => bus.setLatencySloMissCallback(sub.id, {} as never),
    () => bus.setLatencySloMissCallback(sub.id, [] as never),
    () => bus.setLatencySloMissCallback(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'failed revisions must not audit',
  );
  // The live callback, the threshold, the window, and the latch are
  // untouched: the original callback still fires on the next breach.
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].p99Ms, 25);
  assert.equal(sloEntry(bus, 't').thresholdMs, 15);
  assert.equal(sloEntry(bus, 't').windowSize, 5);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('a revision identical to the live callback is a no-op with no audit', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const handler = (_e: LatencySloMissEvent): void => {};
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: {
      windowSize: 5,
      p99ThresholdMs: 100,
      onLatencySloMiss: handler,
    },
  });
  // Re-installing the same reference audits nothing.
  bus.setLatencySloMissCallback(sub.id, handler);
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'a same-reference revision must not audit',
  );
  // Clearing a callback that was never set is likewise a no-op.
  const bare = bus.subscribe('t2', () => { clock.nowMs += 25; }, {
    latencySlo: { windowSize: 5, p99ThresholdMs: 100 },
  });
  bus.setLatencySloMissCallback(bare.id, null);
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'clearing an absent callback must not audit',
  );
  // And the live callback still governs after the no-ops.
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(sloEntry(bus, 't').samples, 5);
  sub.unsubscribe();
  bare.unsubscribe();
});

test('the swap audits exact presence flags with the subscriber', () => {
  const bus = new EventBus();
  const first = (_e: LatencySloMissEvent): void => {};
  const second = (_e: LatencySloMissEvent): void => {};
  const sub = bus.subscribe('t', () => {}, {
    latencySlo: { windowSize: 5, p99ThresholdMs: 100 },
  });
  bus.setLatencySloMissCallback(sub.id, first);
  bus.setLatencySloMissCallback(sub.id, second);
  bus.setLatencySloMissCallback(sub.id, null);
  const audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(audit[2].detail, { hasCallback: false, oldHasCallback: true });
  for (const entry of audit) {
    assert.equal(entry.subId, sub.id);
    assert.equal(entry.pattern, 't');
    assert.equal(entry.actor, 'system');
  }
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
  sub.unsubscribe();
});

test('reliable subscribers evaluate the revised callback on ack completion', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const first: LatencySloMissEvent[] = [];
  const second: LatencySloMissEvent[] = [];
  const sub = bus.subscribeReliable('t', (delivery) => {
    clock.nowMs += 30; // consumer think time before acking
    delivery.ack();
  }, {
    latencySlo: {
      windowSize: 5,
      p99ThresholdMs: 15,
      onLatencySloMiss: (e) => first.push(e),
    },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(first.length, 1);
  assert.equal(first[0].p99Ms, 30);
  assert.equal(sloEntry(bus, 't').samples, 5);

  // Swap the callback: while the latch is set, further slow acks
  // cannot re-fire, so the old callback stays silent and the new
  // one stays quiet too. Raising the threshold above the live p99
  // re-arms the latch on the next ack evaluation (the p99 drops
  // back to or below the live threshold), and lowering it again
  // breaches on the next ack — reaching the new callback only.
  bus.setLatencySloMissCallback(sub.id, (e) => second.push(e));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(first.length, 1, 'the old callback never fires again');
  assert.equal(second.length, 0, 'no re-fire while the latch is set');
  // Raising the threshold never clears the latch either; the next
  // ack evaluation re-arms it only when the p99 drops below — with
  // every ack at 30ms and a raised threshold of 100, the next ack
  // evaluation sees p99 30 <= 100 and re-arms.
  bus.setLatencySloThreshold(sub.id, 100);
  bus.publish('t', 'rearm');
  await flush();
  assert.equal(sloEntry(bus, 't').breaching, false);
  // Lower the threshold back below the live p99: the next ack
  // evaluation breaches and the new callback fires.
  bus.setLatencySloThreshold(sub.id, 15);
  bus.publish('t', 'breach');
  await flush();
  assert.equal(first.length, 1);
  assert.equal(second.length, 1);
  assert.equal(second[0].p99Ms, 30);
  assert.equal(second[0].thresholdMs, 15);
  sub.unsubscribe();
});
