import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type LagEvent, type LagMonitorOptions } from '../src/bus.ts';

/**
 * Bus-level default lag watermark monitor (EB-494):
 * `setDefaultLagMonitor` installs the `windowSize` / `thresholdMs` /
 * `onLag` new opt-in `lagMonitor` subscribers fall back to when they
 * omit a field — the priority chain is subscriber-explicit > bus
 * default > built-in (windowSize 1024, no threshold, no alert) — and
 * the default never enables monitoring: a subscriber that does not opt
 * into `lagMonitor` is untouched by it.
 *
 * Window assertions read `getStats().lag` (`samples`, following
 * `lag-monitor-revision.test.ts`): an injectable clock makes queue
 * dwells deterministic, and the `samples` count exposes the window
 * bound — publish more messages than the window and the count caps
 * at it.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 1_000) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

function lagEntry(bus: EventBus, pattern: string) {
  const entry = bus.getStats().lag.find((e) => e.pattern === pattern);
  assert.ok(entry !== undefined, `expected a getStats().lag entry for ${pattern}`);
  return entry;
}

/** Publishes `n` messages, advancing the clock between them, then drains. */
async function publishAndDrain(bus: EventBus, clock: ReturnType<typeof makeClock>, pattern: string, n: number) {
  for (let i = 0; i < n; i++) {
    bus.publish(pattern, i);
    clock.advance(10);
  }
  await flush();
}

test('EB-494: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultLagMonitor(), undefined);
  bus.setDefaultLagMonitor(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lag_monitor_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-494: lagMonitor: true takes the whole bus default window', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLagMonitor({ windowSize: 3 });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  await publishAndDrain(bus, clock, 't', 5);
  assert.equal(lagEntry(bus, 't').samples, 3, 'the window comes from the bus default');
  sub.unsubscribe();
});

test('EB-494: {} opts in and takes the whole default (window, threshold, callback)', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagEvent[] = [];
  bus.setDefaultLagMonitor({
    windowSize: 3,
    thresholdMs: 50,
    onLag: (e) => events.push(e),
  });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: {} });
  bus.publish('t', 'a'); // enqueued at 1000
  clock.advance(60);
  bus.publish('t', 'b'); // head watermark 60 >= 50: fires once, from the default
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdMs, 50);
  assert.equal(events[0].lagMs, 60);
  await flush();
  assert.equal(lagEntry(bus, 't').samples, 2);
  assert.equal(events.length, 1, 'the excursion latch fires once');
  sub.unsubscribe();
});

test('EB-494: an explicit field wins, field by field, over the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagEvent[] = [];
  bus.setDefaultLagMonitor({
    windowSize: 3,
    thresholdMs: 50,
    onLag: (e) => events.push(e),
  });
  // Explicit windowSize 5 beats the default 3; the default threshold
  // and callback still apply (no explicit ones given).
  const sub = bus.subscribe('t', () => {}, { lagMonitor: { windowSize: 5 } });
  await publishAndDrain(bus, clock, 't', 7);
  assert.equal(lagEntry(bus, 't').samples, 5, 'the explicit window wins');
  sub.unsubscribe();

  // Explicit threshold 1000 beats the default 50: a 60ms watermark
  // stays silent even though the default callback is installed.
  // (The 't' subscriber above already fired once on its own 50ms
  // default threshold; only the count staying put matters here.)
  const firedBefore = events.length;
  const quiet = bus.subscribe('u', () => {}, { lagMonitor: { thresholdMs: 1000 } });
  bus.publish('u', 'a');
  clock.advance(60);
  bus.publish('u', 'b');
  assert.equal(events.length, firedBefore, 'the explicit threshold wins over the default');
  await flush();
  quiet.unsubscribe();
});

test('EB-494: a default threshold + onLag alerts on a watermark excursion', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagEvent[] = [];
  bus.setDefaultLagMonitor({ thresholdMs: 50, onLag: (e) => events.push(e) });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  bus.publish('t', 'a');
  clock.advance(60);
  bus.publish('t', 'b'); // watermark 60 >= 50: fires once
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdMs, 50);
  assert.equal(events[0].lagMs, 60);
  await flush();
  assert.equal(events.length, 1, 'the latch fires once per excursion');
  sub.unsubscribe();
});

test('EB-494: without a bus default, true still takes the built-in 1024', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  await publishAndDrain(bus, clock, 't', 5);
  assert.equal(lagEntry(bus, 't').samples, 5, 'no default means the built-in 1024 window');
  sub.unsubscribe();
});

test('EB-494: clearing with null returns new opt-in subscribers to the built-ins', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLagMonitor({ windowSize: 3, thresholdMs: 50 });
  bus.setDefaultLagMonitor(null);
  assert.equal(bus.getDefaultLagMonitor(), undefined);
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  await publishAndDrain(bus, clock, 't', 5);
  assert.equal(lagEntry(bus, 't').samples, 5, 'the cleared default means the built-in 1024');
  const audit = bus.getSubscriptionAudit({ action: 'default_lag_monitor_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { windowSize: 3, thresholdMs: 50 },
    lagMonitor: null,
  });
  sub.unsubscribe();
});

test('EB-494: the default never opts a subscriber in (omitted / false stay unmonitored)', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLagMonitor({ windowSize: 3, thresholdMs: 50, onLag: () => {} });
  const received: unknown[] = [];
  const plain = bus.subscribe('t', (msg) => received.push(msg.payload));
  const off = bus.subscribe('u', () => {}, { lagMonitor: false });
  await publishAndDrain(bus, clock, 't', 3);
  await publishAndDrain(bus, clock, 'u', 3);
  assert.deepEqual(received, [0, 1, 2]);
  assert.deepEqual(bus.getStats().lag, [], 'no subscriber opted in, so nothing is monitored');
  plain.unsubscribe();
  off.unsubscribe();
});

test('EB-494: re-affirming the default is a no-op, the getter and setter copy defensively, and illegal values throw RangeError with the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultLagMonitor(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lag_monitor_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultLagMonitor({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lag_monitor_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultLagMonitor(), undefined);
  const onLag = () => {};
  const opts: LagMonitorOptions = { windowSize: 3, thresholdMs: 50, onLag };
  bus.setDefaultLagMonitor(opts);
  opts.windowSize = 999;
  opts.thresholdMs = 999;
  assert.deepEqual(
    bus.getDefaultLagMonitor(),
    { windowSize: 3, thresholdMs: 50, onLag },
    'the setter stores a defensive copy',
  );
  bus.setDefaultLagMonitor({ windowSize: 3, thresholdMs: 50, onLag });
  bus.setDefaultLagMonitor({ windowSize: 3, thresholdMs: 50, onLag });
  const audit = bus.getSubscriptionAudit({ action: 'default_lag_monitor_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    lagMonitor: { windowSize: 3, thresholdMs: 50, onLag: 'set' },
  });
  assert.equal(audit[0].actor, 'system');
  const held = bus.getDefaultLagMonitor();
  assert.ok(held !== undefined);
  held.windowSize = 999;
  assert.deepEqual(
    bus.getDefaultLagMonitor(),
    { windowSize: 3, thresholdMs: 50, onLag },
    'the getter returns a copy',
  );
  bus.setDefaultLagMonitor({ windowSize: 5 }, 'ops');
  assert.deepEqual(bus.getDefaultLagMonitor(), { windowSize: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'default_lag_monitor_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { windowSize: 3, thresholdMs: 50, onLag: 'set' },
    lagMonitor: { windowSize: 5 },
  });
  assert.equal(revised[1].actor, 'ops');
  const bad = [
    0,
    -1,
    '3',
    [],
    true,
    { windowSize: 0 },
    { windowSize: -1 },
    { windowSize: 1.5 },
    { windowSize: Number.NaN },
    { windowSize: '3' },
    { thresholdMs: -1 },
    { thresholdMs: Number.NaN },
    { thresholdMs: Number.POSITIVE_INFINITY },
    { onLag: () => {} },
    { thresholdMs: 50, onLag: 'not-a-function' },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultLagMonitor(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultLagMonitor(), { windowSize: 5 });
  }
  assert.throws(() => bus.setDefaultLagMonitor({ windowSize: 7 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultLagMonitor(), { windowSize: 5 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_lag_monitor_revised' }).length,
    2,
    'failed revisions must not audit',
  );
});

test('EB-494: revising the bus default never moves an already-subscribed window, threshold, or samples', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const events: LagEvent[] = [];
  bus.setDefaultLagMonitor({ windowSize: 3, thresholdMs: 50, onLag: (e) => events.push(e) });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  await publishAndDrain(bus, clock, 't', 5);
  assert.equal(lagEntry(bus, 't').samples, 3);
  bus.setDefaultLagMonitor({ windowSize: 2, thresholdMs: 1000 });
  await publishAndDrain(bus, clock, 't', 5);
  assert.equal(
    lagEntry(bus, 't').samples,
    3,
    'the early subscriber keeps the window it was baked with',
  );
  // The old threshold still governs the early subscriber: a 60ms
  // watermark still fires even though the live default is now 1000.
  bus.publish('t', 'a');
  clock.advance(60);
  bus.publish('t', 'b');
  assert.equal(events.length, 1, 'the early subscriber keeps its baked threshold');
  await flush();
  // A subscriber joining after the revision takes the new default.
  const late = bus.subscribe('u', () => {}, { lagMonitor: true });
  await publishAndDrain(bus, clock, 'u', 5);
  assert.equal(lagEntry(bus, 'u').samples, 2, 'the new default window is 2');
  sub.unsubscribe();
  late.unsubscribe();
});

test('EB-494: setLagMonitor(null) clears a subscriber without the default re-applying', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const onLag = () => {};
  bus.setDefaultLagMonitor({ windowSize: 3, thresholdMs: 50, onLag });
  const sub = bus.subscribe('t', () => {}, { lagMonitor: true });
  await publishAndDrain(bus, clock, 't', 5);
  assert.equal(lagEntry(bus, 't').samples, 3);
  bus.setLagMonitor(sub.id, null);
  assert.deepEqual(bus.getStats().lag, [], 'the cleared subscriber leaves getStats().lag');
  assert.throws(() => bus.setLagMonitor(sub.id, { windowSize: 2 }), RangeError);
  assert.deepEqual(
    bus.getDefaultLagMonitor(),
    { windowSize: 3, thresholdMs: 50, onLag },
    'clearing a subscriber never moves the bus default',
  );
  // A new subscriber still takes the untouched bus default.
  const fresh = bus.subscribe('u', () => {}, { lagMonitor: true });
  await publishAndDrain(bus, clock, 'u', 5);
  assert.equal(lagEntry(bus, 'u').samples, 3);
  sub.unsubscribe();
  fresh.unsubscribe();
});
