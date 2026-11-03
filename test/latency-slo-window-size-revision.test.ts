import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { LatencySloMissEvent } from '../src/bus.ts';

/**
 * Processing-latency SLO sample-window revision (EB-368):
 * `setLatencySloWindowSize` retunes a monitored subscriber's rolling
 * window without re-subscribing — only the window moves, the threshold,
 * the `onLatencySloMiss` callback, and the `alerted` latch are
 * untouched, and when phase attribution is armed (EB-133) its three
 * phase trackers resize to the same bound so the attribution keeps
 * covering exactly the history the miss was evaluated against.
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

test('setLatencySloWindowSize rejects unknown, empty, unmonitored, and invalid revisions fail-closed', async () => {
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
  const plain = bus.subscribe('t2', () => {});
  for (const bad of [
    () => bus.setLatencySloWindowSize('no-such-sub', 5),
    () => bus.setLatencySloWindowSize('', 5),
    () => bus.setLatencySloWindowSize(123 as never, 5),
    () => bus.setLatencySloWindowSize(plain.id, 5),
    () => bus.setLatencySloWindowSize(sub.id, 0),
    () => bus.setLatencySloWindowSize(sub.id, -1),
    () => bus.setLatencySloWindowSize(sub.id, 1.5),
    () => bus.setLatencySloWindowSize(sub.id, Number.NaN),
    () => bus.setLatencySloWindowSize(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setLatencySloWindowSize(sub.id, '5' as never),
    () => bus.setLatencySloWindowSize(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'latency_slo_window_size_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live window, the samples, the threshold, and the latch are
  // untouched: the old window still governs.
  const entry = sloEntry(bus, 't');
  assert.equal(entry.windowSize, 10);
  assert.equal(entry.samples, 0);
  assert.equal(entry.thresholdMs, 100);
  assert.equal(entry.breaching, false);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('shrinking the window evicts the oldest samples and the stats reflect the new window at once', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let duration = 100;
  const sub = bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 1000 },
  });
  // Five slow samples, then five fast ones: [100 x5, 10 x5]. The
  // first batch must flush before the duration changes — publish only
  // enqueues, and the handler runs at flush time.
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  duration = 10;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  let entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 10);
  assert.equal(entry.p99Ms, 100);
  assert.equal(entry.windowSize, 10);

  bus.setLatencySloWindowSize(sub.id, 5);
  // The five oldest (slow) samples are gone at once — no new sample
  // needed for the stats to reflect the new window.
  entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 5);
  assert.equal(entry.p99Ms, 10);
  assert.equal(entry.windowSize, 5);
  assert.equal(entry.thresholdMs, 1000, 'the threshold is untouched by a window revision');
  const audit = bus.getSubscriptionAudit({ action: 'latency_slo_window_size_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { windowSize: 5, oldWindowSize: 10 });
  sub.unsubscribe();
});

test('widening the window keeps every sample and the new bound governs the next samples', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: { windowSize: 3, p99ThresholdMs: 1000 },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  let entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 3);
  assert.equal(entry.p99Ms, 25);

  bus.setLatencySloWindowSize(sub.id, 10);
  entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 3, 'widening keeps every sample');
  assert.equal(entry.windowSize, 10);

  // The new bound applies from the next samples: seven more fit now.
  for (let i = 0; i < 7; i++) bus.publish('t', i);
  await flush();
  entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 10);
  assert.equal(entry.p99Ms, 25);

  // And shrinking again evicts oldest-first on the spot.
  bus.setLatencySloWindowSize(sub.id, 4);
  entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 4);
  assert.equal(entry.windowSize, 4);
  sub.unsubscribe();
});

test('a revision identical to the current window is a no-op with no audit', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 100 },
  });
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  await flush();
  bus.setLatencySloWindowSize(sub.id, 10);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'latency_slo_window_size_revised' }).length,
    0,
    'a same-window revision must not audit',
  );
  const entry = sloEntry(bus, 't');
  assert.equal(entry.windowSize, 10);
  assert.equal(entry.samples, 4);
  sub.unsubscribe();
});

test('the stats row carries the live window size, defaulting to 1024', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: { p99ThresholdMs: 100 },
  });
  bus.publish('t', 'one');
  await flush();
  let entry = sloEntry(bus, 't');
  assert.equal(entry.windowSize, 1024);
  assert.equal(entry.samples, 1);
  bus.setLatencySloWindowSize(sub.id, 512);
  entry = sloEntry(bus, 't');
  assert.equal(entry.windowSize, 512);
  assert.equal(entry.samples, 1, 'a pure bound change keeps the samples');
  sub.unsubscribe();
});

test('the threshold and the miss callback are unaffected by a window revision', async () => {
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

  bus.setLatencySloWindowSize(sub.id, 4);
  let entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 4);
  assert.equal(entry.p99Ms, 25);
  assert.equal(entry.thresholdMs, 100, 'the threshold survives the window revision');
  assert.equal(events.length, 0, 'a resize alone never evaluates the SLO');

  // Lowering the threshold afterwards still fires the original
  // callback on the next sample, against the new window.
  bus.setLatencySloThreshold(sub.id, 20);
  bus.publish('t', 'next');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].p99Ms, 25);
  assert.equal(events[0].thresholdMs, 20);
  assert.equal(events[0].samples, 4, 'the window stays bounded at 4 after the new sample');
  entry = sloEntry(bus, 't');
  assert.equal(entry.windowSize, 4);
  sub.unsubscribe();
});

test('shrinking the slow tail out of the window does not clear the latch; it re-arms only on the next sample evaluation', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LatencySloMissEvent[] = [];
  let duration = 25;
  const sub = bus.subscribe('t', () => { clock.nowMs += duration; }, {
    latencySlo: {
      windowSize: 10,
      p99ThresholdMs: 15,
      onLatencySloMiss: (e) => events.push(e),
    },
  });
  // Five slow samples, then five fast ones: p99 = 25, breaching. The
  // first batch must flush before the duration changes — publish only
  // enqueues, and the handler runs at flush time.
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  duration = 5;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(bus.getStats().subscriberLatencyP99[0].breaching, true);

  // Shrink past the slow tail: the p99 drops to 5 at once, but the
  // latch stays set — a resize never evaluates the SLO.
  bus.setLatencySloWindowSize(sub.id, 5);
  let entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 5);
  assert.equal(entry.p99Ms, 5);
  assert.equal(entry.breaching, true, 'the latch survives the window revision');
  assert.equal(events.length, 1);

  // The next sample evaluation re-arms the latch without firing.
  bus.publish('t', 'fast');
  await flush();
  assert.equal(events.length, 1);
  assert.equal(sloEntry(bus, 't').breaching, false);

  // And crossing again fires a second time, against the new window.
  duration = 25;
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 2);
  assert.equal(events[1].p99Ms, 25);
  assert.equal(events[1].samples, 5);
  sub.unsubscribe();
});

test('phase trackers resize with the SLO window when attribution is armed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, processingLatencySloAlert: true });
  const sub = bus.subscribe('t', () => {}, {
    latencySlo: { windowSize: 4, p99ThresholdMs: 1000 },
  });
  // Stagger the publishes so each message waits a different time in
  // the queue: queue-wait samples 100, 90, 80, 70.
  bus.publish('t', 'a');
  clock.nowMs += 10;
  bus.publish('t', 'b');
  clock.nowMs += 10;
  bus.publish('t', 'c');
  clock.nowMs += 10;
  bus.publish('t', 'd');
  clock.nowMs = 1100;
  await flush();
  const phaseEntry = () => {
    const rows = bus.getStats().sloMissesByPhase.filter((e) => e.pattern === 't');
    assert.equal(rows.length, 1, 'expected one sloMissesByPhase entry for t');
    return rows[0];
  };
  assert.equal(phaseEntry().phaseAttribution.queueWaitMs.p99Ms, 100);
  assert.equal(sloEntry(bus, 't').samples, 4);

  // Shrink the SLO window: the phase windows shrink with it, so the
  // attribution covers the same history as the SLO evaluation.
  bus.setLatencySloWindowSize(sub.id, 2);
  assert.equal(phaseEntry().phaseAttribution.queueWaitMs.p99Ms, 80);
  assert.equal(sloEntry(bus, 't').samples, 2);
  assert.equal(sloEntry(bus, 't').windowSize, 2);
  sub.unsubscribe();
});

test('phase trackers stay untouched when attribution is not armed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => { clock.nowMs += 25; }, {
    latencySlo: { windowSize: 10, p99ThresholdMs: 1000 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  bus.setLatencySloWindowSize(sub.id, 3);
  assert.equal(sloEntry(bus, 't').samples, 3);
  assert.equal(
    bus.getStats().sloMissesByPhase.length,
    0,
    'no attribution rows without the bus-level switch',
  );
  sub.unsubscribe();
});

test('reliable subscribers evaluate the revised window on ack completion', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribeReliable('t', (delivery) => {
    clock.nowMs += 30; // consumer think time before acking
    delivery.ack();
  }, {
    latencySlo: { windowSize: 8, p99ThresholdMs: 1000 },
  });
  for (let i = 0; i < 8; i++) bus.publish('t', i);
  await flush();
  let entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 8);
  assert.equal(entry.p99Ms, 30);

  // Shrink: the oldest ack-completion samples are evicted at once.
  bus.setLatencySloWindowSize(sub.id, 3);
  entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 3);
  assert.equal(entry.p99Ms, 30);
  assert.equal(entry.windowSize, 3);

  // The next ack-completion sample keeps the window bounded.
  bus.publish('t', 'next');
  await flush();
  entry = sloEntry(bus, 't');
  assert.equal(entry.samples, 3);
  assert.equal(entry.p99Ms, 30);
  sub.unsubscribe();
});
