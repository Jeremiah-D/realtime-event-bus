import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { ScheduleSkewTracker, type ScheduleSkewEvent } from '../src/scheduleskew.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so skew tests are deterministic instead of racing wall time. Due
 * sweeps run at the start of every flush, so advancing this clock past a
 * `deliverAt` and then publishing anything fans out deterministically —
 * and the skew is just how far past the due time the clock reads.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-schedule-skew-'));

test('sweep samples skew = now - deliverAt for a schedule that fans out', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, scheduleSkew: {} });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'late', { delayMs: 1000 }); // deliverAt = t=2000
  clock.nowMs = 2_050;
  bus.publish('t', 'poke'); // flush sweep fans out the due entry with skew 50
  await flush();
  assert.deepEqual(received, ['poke', 'late']);
  const summary = bus.getStats().delayedScheduleSkew;
  assert.deepEqual(summary, {
    samples: 1,
    p50Ms: 50,
    p99Ms: 50,
    minMs: 50,
    maxMs: 50,
    meanMs: 50,
  });
});

test('p50/p99 use nearest-rank over the sample window', async () => {
  const clock = controllableClock(0);
  const bus = new EventBus({ now: clock.now, scheduleSkew: {} });
  bus.subscribe('t', () => {});
  bus.publishDelayed('t', 'a', { delayMs: 1000 }); // due t=1000
  bus.publishDelayed('t', 'b', { delayMs: 2000 }); // due t=2000
  bus.publishDelayed('t', 'c', { delayMs: 3000 }); // due t=3000
  clock.nowMs = 3_500; // skews 2500, 1500, 500
  bus.publish('t', 'poke');
  await flush();
  const s = bus.getStats().delayedScheduleSkew;
  assert.equal(s.samples, 3);
  // Sorted [500, 1500, 2500]: p50 rank ceil(0.5*3)-1 = 1 -> 1500;
  // p99 rank ceil(0.99*3)-1 = 2 -> 2500.
  assert.equal(s.p50Ms, 1500);
  assert.equal(s.p99Ms, 2500);
  assert.equal(s.minMs, 500);
  assert.equal(s.maxMs, 2500);
  assert.equal(s.meanMs, 1500);
});

test('the rolling window evicts the oldest samples past windowSize', () => {
  const tracker = new ScheduleSkewTracker(4);
  for (const v of [10, 20, 30, 40, 50, 60]) tracker.record(v);
  const s = tracker.summary();
  assert.equal(s.samples, 4);
  assert.equal(s.minMs, 30); // 10 and 20 were evicted
  assert.equal(s.maxMs, 60);
  assert.equal(s.meanMs, 45);
  // Sorted [30, 40, 50, 60]: p50 rank ceil(0.5*4)-1 = 1 -> 40.
  assert.equal(s.p50Ms, 40);
});

test('a negative sample (clock moved backwards) clamps to 0', () => {
  const tracker = new ScheduleSkewTracker();
  tracker.record(-5);
  const s = tracker.summary();
  assert.equal(s.samples, 1);
  assert.equal(s.minMs, 0);
  assert.equal(s.maxMs, 0);
  assert.equal(s.meanMs, 0);
  assert.equal(s.p50Ms, 0);
});

test('skew beyond alertThresholdMs fires onScheduleSkew; a throwing callback never breaks fan-out', async () => {
  const clock = controllableClock();
  const events: ScheduleSkewEvent[] = [];
  const bus = new EventBus({
    now: clock.now,
    scheduleSkew: {
      alertThresholdMs: 10,
      onScheduleSkew: (event) => {
        events.push(event);
        if (event.delayId === 'delayed-2') throw new Error('alert outlet exploded');
      },
    },
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'a', { delayMs: 1000 }); // due t=2000, id delayed-1
  bus.publishDelayed('t', 'b', { delayMs: 2000 }); // due t=3000, id delayed-2
  clock.nowMs = 3_050; // skews 1050 and 50 — both exceed the 10ms threshold
  bus.publish('t', 'poke');
  await flush();
  // The second callback threw, but both schedules still fanned out.
  assert.deepEqual(received, ['poke', 'a', 'b']);
  assert.equal(events.length, 2);
  // `scheduledFor` is the schedule's due time (deliverAt) — no
  // redundant timestamp field is stamped on DelayedEntry.
  assert.deepEqual(events[0], {
    scheduledFor: 2000,
    fannedOutAtMs: 3050,
    skewMs: 1050,
    topic: 't',
    delayId: 'delayed-1',
  });
  assert.deepEqual(events[1], {
    scheduledFor: 3000,
    fannedOutAtMs: 3050,
    skewMs: 50,
    topic: 't',
    delayId: 'delayed-2',
  });
});

test('onScheduleSkew stays silent when skew is at or below the threshold', async () => {
  const clock = controllableClock();
  let fired = 0;
  const bus = new EventBus({
    now: clock.now,
    scheduleSkew: { alertThresholdMs: 50, onScheduleSkew: () => { fired += 1; } },
  });
  bus.subscribe('t', () => {});
  bus.publishDelayed('t', 'x', { delayMs: 1000 }); // due t=2000
  clock.nowMs = 2_050; // skew exactly 50: not *beyond* the threshold
  bus.publish('t', 'poke');
  await flush();
  assert.equal(fired, 0);
  // Still sampled — just not alerted.
  assert.equal(bus.getStats().delayedScheduleSkew.samples, 1);
  assert.equal(bus.getStats().delayedScheduleSkew.p50Ms, 50);
});

test('scheduleSkew is opt-in: without the option nothing is sampled or rendered', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.publishDelayed('t', 'x', { delayMs: 1000 }); // due t=2000
  clock.nowMs = 2_050;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(bus.getStats().delayedScheduleSkew, {
    samples: 0,
    p50Ms: 0,
    p99Ms: 0,
    minMs: 0,
    maxMs: 0,
    meanMs: 0,
  });
  const exposition = renderPrometheus(bus.getStats());
  assert.ok(!exposition.includes('eventbus_delayed_schedule_skew_ms{'), 'no skew series when disabled');
});

test('a schedule rebuilt from the durable log samples its skew on the recovery fan-out', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir, scheduleSkew: {} });
  bus1.publishDelayed('t', 'survivor', { delayMs: 1000 }); // due t=2000, persisted
  // Simulate a restart: a new bus over the same directory, clock still t=1000.
  // The recovery sweep runs at t=1000, so nothing is due yet — then the
  // clock moves past the due time and the rebuilt schedule fans out and
  // is sampled like a live one.
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir, scheduleSkew: {} });
  assert.equal(bus2.getStats().pendingDelayed, 1);
  const received: unknown[] = [];
  bus2.subscribe('t', (msg) => received.push(msg.payload));
  clock.nowMs = 2_050;
  bus2.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'survivor']);
  const summary = bus2.getStats().delayedScheduleSkew;
  assert.equal(summary.samples, 1);
  assert.equal(summary.p50Ms, 50);
});

test('invalid scheduleSkew options throw before the bus arms anything', () => {
  const clock = controllableClock();
  assert.throws(() => new EventBus({ now: clock.now, scheduleSkew: { windowSize: 0 } }), RangeError);
  assert.throws(() => new EventBus({ now: clock.now, scheduleSkew: { windowSize: -1 } }), RangeError);
  assert.throws(() => new EventBus({ now: clock.now, scheduleSkew: { windowSize: 1.5 } }), RangeError);
  assert.throws(() => new EventBus({ now: clock.now, scheduleSkew: { alertThresholdMs: -1 } }), RangeError);
  assert.throws(() => new EventBus({ now: clock.now, scheduleSkew: { alertThresholdMs: Number.NaN } }), RangeError);
  assert.throws(() => new EventBus({ now: clock.now, scheduleSkew: { alertThresholdMs: Number.POSITIVE_INFINITY } }), RangeError);
  assert.throws(
    () => new EventBus({ now: clock.now, scheduleSkew: { onScheduleSkew: 'not-a-function' as never } }),
    TypeError,
  );
  // A callback with no threshold can never fire: a configuration bug,
  // rejected like the lag-monitor equivalent.
  assert.throws(
    () => new EventBus({ now: clock.now, scheduleSkew: { onScheduleSkew: () => {} } }),
    RangeError,
  );
});

test('renderPrometheus exposes the skew distribution once sampled', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, scheduleSkew: {} });
  bus.subscribe('t', () => {});
  bus.publishDelayed('t', 'x', { delayMs: 1000 }); // due t=2000
  clock.nowMs = 2_050;
  bus.publish('t', 'poke');
  await flush();
  const exposition = renderPrometheus(bus.getStats());
  assert.ok(exposition.includes('eventbus_delayed_schedule_skew_ms{quantile="0.5"} 50'));
  assert.ok(exposition.includes('eventbus_delayed_schedule_skew_ms{quantile="0.99"} 50'));
  assert.ok(exposition.includes('# HELP eventbus_delayed_schedule_skew_ms'));
  assert.ok(exposition.includes('# TYPE eventbus_delayed_schedule_skew_ms gauge'));
});

test('a delayed message dropped as expired is never sampled', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, scheduleSkew: {} });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // deadline t=1100 for schedules made at t=1000
  bus.publishDelayed('t', 'doomed', { delayMs: 1000 }); // due t=2000, past its deadline
  clock.nowMs = 2_050;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']); // 'doomed' expired, never fanned out
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.equal(bus.getStats().delayedScheduleSkew.samples, 0);
});
