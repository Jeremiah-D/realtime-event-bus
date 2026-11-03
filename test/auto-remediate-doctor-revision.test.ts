import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setAutoRemediate doctor revision fails closed when autoRemediate is not enabled', async () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().autoRemediate, undefined);
  assert.throws(
    () => bus.setAutoRemediate({ doctor: { slowSubscriberP99Ms: 500 } }),
    RangeError,
  );
  assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
  await bus.close();
});

test('setAutoRemediate rejects illegal doctor thresholds and leaves the live schedule untouched', async () => {
  const bus = new EventBus({
    autoRemediate: { intervalMs: 5000, doctor: { slowSubscriberP99Ms: 2000 } },
  });
  const before = bus.getStats().autoRemediate;
  assert.deepEqual(before, {
    intervalMs: 5000,
    maxActions: 10,
    dryRun: false,
    doctor: { slowSubscriberP99Ms: 2000, lagWatermarkDefaultMs: 30000 },
  });
  const bad: Array<unknown> = [
    { doctor: { slowSubscriberP99Ms: 0 } },
    { doctor: { slowSubscriberP99Ms: -5 } },
    { doctor: { slowSubscriberP99Ms: Number.NaN } },
    { doctor: { slowSubscriberP99Ms: Number.POSITIVE_INFINITY } },
    { doctor: { slowSubscriberP99Ms: 'fast' } },
    { doctor: { lagWatermarkDefaultMs: 0 } },
    { doctor: { lagWatermarkDefaultMs: -1 } },
    { doctor: { lagWatermarkDefaultMs: 'slow' } },
    { doctor: 'yes' },
    { doctor: [] },
    { doctor: null },
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setAutoRemediate(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  assert.deepEqual(bus.getStats().autoRemediate, before);
  assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
  await bus.close();
});

test('setAutoRemediate doctor revision is partial: only the named thresholds move', async () => {
  const bus = new EventBus({ autoRemediate: { intervalMs: 5000 } });
  bus.setAutoRemediate({ doctor: { slowSubscriberP99Ms: 2500 } });
  assert.deepEqual(bus.getStats().autoRemediate?.doctor, {
    slowSubscriberP99Ms: 2500,
    lagWatermarkDefaultMs: 30000,
  });
  bus.setAutoRemediate({ doctor: { lagWatermarkDefaultMs: 5000 } });
  assert.deepEqual(bus.getStats().autoRemediate?.doctor, {
    slowSubscriberP99Ms: 2500,
    lagWatermarkDefaultMs: 5000,
  });
  // A schedule-only revision keeps the doctor tuning.
  bus.setAutoRemediate({ maxActions: 3 });
  assert.deepEqual(bus.getStats().autoRemediate?.doctor, {
    slowSubscriberP99Ms: 2500,
    lagWatermarkDefaultMs: 5000,
  });
  await bus.close();
});

test('setAutoRemediate with the same doctor thresholds or an empty doctor is a no-op with no audit', async () => {
  const bus = new EventBus({
    autoRemediate: {
      intervalMs: 5000,
      doctor: { slowSubscriberP99Ms: 2000, lagWatermarkDefaultMs: 4000 },
    },
  });
  bus.setAutoRemediate({ doctor: { slowSubscriberP99Ms: 2000 } });
  bus.setAutoRemediate({ doctor: { lagWatermarkDefaultMs: 4000 } });
  bus.setAutoRemediate({ doctor: {} });
  bus.setAutoRemediate({ doctor: { slowSubscriberP99Ms: 2000, lagWatermarkDefaultMs: 4000 } });
  assert.deepEqual(bus.getStats().autoRemediate?.doctor, {
    slowSubscriberP99Ms: 2000,
    lagWatermarkDefaultMs: 4000,
  });
  assert.equal(bus.getSubscriptionAudit({ action: 'auto_remediate_revised' }).length, 0);
  await bus.close();
});

test('setAutoRemediate doctor revision audits the parsed doctor pair', async () => {
  const bus = new EventBus({
    autoRemediate: { intervalMs: 5000, doctor: { slowSubscriberP99Ms: 2000 } },
  });
  bus.setAutoRemediate({ doctor: { lagWatermarkDefaultMs: 5000 } });
  const events = bus.getSubscriptionAudit({ action: 'auto_remediate_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    intervalMs: 5000,
    oldIntervalMs: 5000,
    maxActions: 10,
    oldMaxActions: 10,
    dryRun: false,
    oldDryRun: false,
    doctor: { slowSubscriberP99Ms: 2000, lagWatermarkDefaultMs: 5000 },
    oldDoctor: { slowSubscriberP99Ms: 2000, lagWatermarkDefaultMs: 30000 },
  });
  await bus.close();
});

test('getStats().autoRemediate exposes the parsed doctor thresholds', async () => {
  const bus = new EventBus({
    autoRemediate: { intervalMs: 5000, doctor: { slowSubscriberP99Ms: 2500 } },
  });
  assert.deepEqual(bus.getStats().autoRemediate, {
    intervalMs: 5000,
    maxActions: 10,
    dryRun: false,
    doctor: { slowSubscriberP99Ms: 2500, lagWatermarkDefaultMs: 30000 },
  });
  bus.setAutoRemediate({ doctor: { lagWatermarkDefaultMs: 5000 } });
  assert.deepEqual(bus.getStats().autoRemediate, {
    intervalMs: 5000,
    maxActions: 10,
    dryRun: false,
    doctor: { slowSubscriberP99Ms: 2500, lagWatermarkDefaultMs: 5000 },
  });
  await bus.close();
});

test('a doctor revision takes effect on the next scheduled run: the lag diagnosis flips', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, autoRemediate: { intervalMs: 50 } });
  // A lag-monitored subscriber with no threshold of its own, paused so
  // its one queued message ages in place and the watermark is the
  // manually-advanced clock: deterministic without real-time dwell.
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  bus.pauseSubscriber(sub.id);
  bus.publish('t', 'x');
  clock.advance(5000);
  assert.equal(bus.getStats().lag[0].watermarkMs, 5000);
  await sleep(300);
  const runsBefore = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
  assert.ok(runsBefore.length >= 1, 'the 50ms schedule fired at least once');
  assert.equal(
    runsBefore[runsBefore.length - 1].detail.healthBefore,
    'healthy',
    'a 5000ms watermark passes the 30000ms default threshold',
  );

  bus.setAutoRemediate({ doctor: { lagWatermarkDefaultMs: 1000 } });
  await sleep(300);
  const runsAfter = bus.getSubscriptionAudit({ action: 'auto_remediate_run' });
  assert.ok(runsAfter.length > runsBefore.length, 'the schedule kept firing after the revision');
  assert.equal(
    runsAfter[runsAfter.length - 1].detail.healthBefore,
    'degraded',
    'the same 5000ms watermark warns under the revised 1000ms threshold',
  );
  // Unsubscribe before close: a paused subscriber never drains, so
  // close() would wait out the full drain timeout on its queued message.
  sub.unsubscribe();
  await bus.close();
});

test('a stored doctor tuning does not leak into manual autoRemediate or exportBusHealth', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, autoRemediate: { intervalMs: 60_000 } });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  bus.pauseSubscriber(sub.id);
  bus.publish('t', 'x');
  clock.advance(5000);
  bus.setAutoRemediate({ doctor: { lagWatermarkDefaultMs: 1000 } });
  assert.deepEqual(bus.getStats().autoRemediate?.doctor, {
    slowSubscriberP99Ms: 1000,
    lagWatermarkDefaultMs: 1000,
  });
  // Manual calls diagnose with their own per-call doctor (or the
  // defaults when none is given) — never the stored tuning.
  assert.equal(bus.autoRemediate().healthBefore, 'healthy');
  assert.equal(bus.exportBusHealth().status, 'healthy');
  assert.equal(
    bus.autoRemediate({ doctor: { lagWatermarkDefaultMs: 1000 } }).healthBefore,
    'degraded',
  );
  assert.equal(bus.exportBusHealth({ lagWatermarkDefaultMs: 1000 }).status, 'degraded');
  // Unsubscribe before close: a paused subscriber never drains, so
  // close() would wait out the full drain timeout on its queued message.
  sub.unsubscribe();
  await bus.close();
});
