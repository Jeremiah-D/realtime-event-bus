import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type LagEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('setLagMonitor revises the threshold from the next watermark evaluation, keeping samples', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    lagMonitor: { windowSize: 1024, thresholdMs: 100, onLag: (e) => events.push(e) },
  });
  bus.publish('t', 'a'); // enqueued at 1000
  clock.nowMs += 50;
  bus.publish('t', 'b'); // head watermark 50 < 100: no fire
  assert.equal(events.length, 0);

  bus.setLagMonitor(sub.id, { thresholdMs: 40 });
  clock.nowMs += 40; // head watermark now 90
  bus.publish('t', 'c'); // enqueue-path evaluation trips the new threshold
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdMs, 40);
  assert.equal(events[0].lagMs, 90);

  await flush();
  const entry = bus.getStats().lag[0];
  assert.equal(entry.samples, 3, 'the dwell history survives the revision');

  const audit = bus.getSubscriptionAudit({ action: 'lag_monitor_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    lagMonitor: { windowSize: 1024, thresholdMs: 40, onLag: 'set' },
    oldLagMonitor: { windowSize: 1024, thresholdMs: 100, onLag: 'set' },
  });
  sub.unsubscribe();
});

test('setLagMonitor shrinking the window evicts only the oldest samples', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: { windowSize: 4 } });
  for (let i = 0; i < 4; i += 1) {
    bus.publish('t', i);
    clock.nowMs += 10;
  }
  clock.nowMs = 1100; // dwells 100, 90, 80, 70
  await flush();
  let entry = bus.getStats().lag[0];
  assert.equal(entry.samples, 4);
  assert.equal(entry.minMs, 70);

  bus.setLagMonitor(sub.id, { windowSize: 2 });
  entry = bus.getStats().lag[0];
  assert.equal(entry.samples, 2);
  assert.equal(entry.minMs, 70, 'the two oldest samples (100, 90) were evicted');
  assert.equal(entry.maxMs, 80);
  assert.equal(entry.meanMs, 75);

  // Growing the window keeps the surviving samples.
  bus.setLagMonitor(sub.id, { windowSize: 10 });
  entry = bus.getStats().lag[0];
  assert.equal(entry.samples, 2);
  sub.unsubscribe();
});

test('setLagMonitor leaves an armed excursion latch untouched', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagEvent[] = [];
  const sub = bus.subscribe('t', () => {}, {
    lagMonitor: { thresholdMs: 50, onLag: (e) => events.push(e) },
  });
  bus.publish('t', 'a');
  clock.nowMs += 60;
  bus.publish('t', 'b'); // watermark 60 >= 50: fires once
  assert.equal(events.length, 1);

  // Tighten while latched: still past the threshold, but the latch
  // must not re-fire — the revision is not a new excursion.
  bus.setLagMonitor(sub.id, { thresholdMs: 30 });
  bus.publish('t', 'c');
  assert.equal(events.length, 1, 'an armed latch stays armed across the revision');

  // Drain everything: the watermark drops to 0 and the latch re-arms.
  await flush();
  assert.equal(bus.getStats().lag[0].watermarkMs, 0);
  bus.publish('t', 'd');
  clock.nowMs += 40;
  bus.publish('t', 'e'); // watermark 40 >= new threshold 30: fires again
  assert.equal(events.length, 2);
  assert.equal(events[1].thresholdMs, 30);
  sub.unsubscribe();
});

test('setLagMonitor partial revision keeps omitted fields, and an identical revision is a no-op', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    lagMonitor: { windowSize: 64, thresholdMs: 100 },
  });
  bus.setLagMonitor(sub.id, { thresholdMs: 200 });
  let audit = bus.getSubscriptionAudit({ action: 'lag_monitor_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    lagMonitor: { windowSize: 64, thresholdMs: 200 },
    oldLagMonitor: { windowSize: 64, thresholdMs: 100 },
  });

  bus.setLagMonitor(sub.id, { windowSize: 64, thresholdMs: 200 });
  bus.setLagMonitor(sub.id, {});
  audit = bus.getSubscriptionAudit({ action: 'lag_monitor_revised' });
  assert.equal(audit.length, 1, 'identical revisions (including an empty object) audit nothing');
  sub.unsubscribe();
});

test('setLagMonitor null clears monitoring, and revision after a clear is refused', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    lagMonitor: { windowSize: 64, thresholdMs: 100, onLag: () => {} },
  });
  bus.publish('t', 'a');
  assert.equal(bus.getStats().lag.length, 1);

  bus.setLagMonitor(sub.id, null);
  assert.equal(bus.getStats().lag.length, 0, 'the subscriber leaves the lag stats');
  const audit = bus.getSubscriptionAudit({ action: 'lag_monitor_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    lagMonitor: null,
    oldLagMonitor: { windowSize: 64, thresholdMs: 100, onLag: 'set' },
  });
  assert.throws(() => bus.setLagMonitor(sub.id, { thresholdMs: 50 }), RangeError);
  sub.unsubscribe();
});

test('setLagMonitor is fail-closed for unknown, unmonitored, or illegal values', () => {
  const bus = new EventBus();
  const monitored = bus.subscribe('t', () => {}, { lagMonitor: { thresholdMs: 100 } });
  const plain = bus.subscribe('u', () => {});
  const noThreshold = bus.subscribe('v', () => {}, { lagMonitor: true });

  const bad: Array<() => void> = [
    () => bus.setLagMonitor('', { thresholdMs: 50 }),
    () => bus.setLagMonitor('sub-nope', { thresholdMs: 50 }),
    () => bus.setLagMonitor(plain.id, { thresholdMs: 50 }),
    () => bus.setLagMonitor(monitored.id, undefined as never),
    () => bus.setLagMonitor(monitored.id, 'x' as never),
    () => bus.setLagMonitor(monitored.id, [] as never),
    () => bus.setLagMonitor(monitored.id, { windowSize: 0 }),
    () => bus.setLagMonitor(monitored.id, { windowSize: 1.5 }),
    () => bus.setLagMonitor(monitored.id, { thresholdMs: -1 }),
    () => bus.setLagMonitor(monitored.id, { thresholdMs: Number.NaN }),
    () => bus.setLagMonitor(monitored.id, { onLag: 'x' as never }),
    // A threshold-less subscription cannot gain a callback alone:
    // the callback would have no threshold to arm it.
    () => bus.setLagMonitor(noThreshold.id, { onLag: () => {} }),
  ];
  for (const call of bad) assert.throws(call, RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'lag_monitor_revised' }).length,
    0,
    'no failed revision is audited',
  );

  // The live parameters survived every failed revision.
  bus.setLagMonitor(monitored.id, { thresholdMs: 200 });
  assert.equal(bus.getSubscriptionAudit({ action: 'lag_monitor_revised' }).length, 1);
  monitored.unsubscribe();
  plain.unsubscribe();
  noThreshold.unsubscribe();
});
