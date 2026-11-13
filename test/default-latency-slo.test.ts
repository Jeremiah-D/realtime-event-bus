import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type LatencySloMissEvent } from '../src/bus.ts';

/**
 * Bus-level default processing-latency SLO (EB-493):
 * `setDefaultLatencySlo` installs the `windowSize` / `p99ThresholdMs`
 * new opt-in `latencySlo` subscribers fall back to when they omit a
 * field — the priority chain is subscriber-explicit > bus default >
 * documented behavior (window 1024, threshold required) — and the
 * default never enables SLO observation: a subscriber that does not
 * opt into `latencySlo` is untouched by it. The default carries no
 * callback: `onLatencySloMiss` stays the subscriber's own.
 *
 * Threshold/window assertions read `getStats().subscriberLatencyP99`
 * (`thresholdMs`, `windowSize`, `samples`, `breaching`), following
 * `latency-slo.test.ts`: an injectable clock makes handler processing
 * time deterministic, and the `samples` count exposes the window
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

function sloEntry(bus: EventBus, pattern: string) {
  const entry = bus.getStats().subscriberLatencyP99.find((e) => e.pattern === pattern);
  assert.ok(entry !== undefined, `expected a subscriberLatencyP99 entry for ${pattern}`);
  return entry;
}

test('EB-493: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultLatencySlo(), undefined);
  bus.setDefaultLatencySlo(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_latency_slo_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-493: {} without a bus default threshold still throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribe('t', () => {}, { latencySlo: {} }), RangeError);
  assert.equal(bus.subscriberCount(), 0, 'the failed subscribe leaves nobody behind');
  // A window-only default still leaves the threshold uncovered.
  bus.setDefaultLatencySlo({ windowSize: 3 });
  assert.throws(() => bus.subscribe('t', () => {}, { latencySlo: {} }), RangeError);
  assert.equal(bus.subscriberCount(), 0);
});

test('EB-493: {} with a bus default threshold opts in and takes the default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 50 });
  bus.subscribe('t', () => { clock.advance(12); }, { latencySlo: {} });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  const entry = sloEntry(bus, 't');
  assert.equal(entry.thresholdMs, 50, 'the threshold comes from the bus default');
  assert.equal(entry.windowSize, 3, 'the window comes from the bus default');
  assert.equal(entry.samples, 3, 'the window caps at the default 3');
  assert.equal(entry.p99Ms, 12);
  assert.equal(entry.breaching, false);
});

test('EB-493: a partial object falls back field by field and an explicit value wins', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 50 });
  bus.subscribe('a', () => { clock.advance(12); }, { latencySlo: { p99ThresholdMs: 20 } });
  bus.subscribe('b', () => { clock.advance(12); }, { latencySlo: { windowSize: 2 } });
  bus.subscribe('c', () => { clock.advance(12); }, { latencySlo: {} });
  for (let i = 0; i < 5; i++) {
    bus.publish('a', i);
    bus.publish('b', i);
    bus.publish('c', i);
  }
  await flush();
  const a = sloEntry(bus, 'a');
  assert.equal(a.thresholdMs, 20, 'the explicit threshold wins');
  assert.equal(a.windowSize, 3, 'the window falls back to the default');
  assert.equal(a.samples, 3);
  const b = sloEntry(bus, 'b');
  assert.equal(b.thresholdMs, 50, 'the threshold falls back to the default');
  assert.equal(b.windowSize, 2, 'the explicit window wins');
  assert.equal(b.samples, 2);
  const c = sloEntry(bus, 'c');
  assert.equal(c.thresholdMs, 50);
  assert.equal(c.windowSize, 3);
  assert.equal(c.samples, 3);
});

test('EB-493: the bus default window covers 1024', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 1000 });
  bus.subscribe('t', () => { clock.advance(12); }, { latencySlo: {} });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  const entry = sloEntry(bus, 't');
  assert.equal(entry.windowSize, 3);
  assert.equal(entry.samples, 3, 'the window caps at the default 3, not 1024');
  // Without a default the same opt-in takes the documented 1024.
  const plain = new EventBus({ now: clock.now });
  plain.subscribe('t', () => { clock.advance(12); }, { latencySlo: { p99ThresholdMs: 1000 } });
  for (let i = 0; i < 5; i++) plain.publish('t', i);
  await flush();
  assert.equal(sloEntry(plain, 't').windowSize, 1024);
  assert.equal(sloEntry(plain, 't').samples, 5);
});

test('EB-493: the miss callback is always the subscriber\'s own — the bus default carries none', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLatencySlo({ windowSize: 10, p99ThresholdMs: 15 });
  const events: LatencySloMissEvent[] = [];
  let duration = 5;
  bus.subscribe('t', () => { clock.advance(duration); }, {
    latencySlo: { onLatencySloMiss: (e) => { events.push(e); } },
  });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 0, 'p99 = 5, under the default threshold');
  duration = 25;
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.equal(events.length, 1, 'the subscriber\'s own callback fires on the breach');
  assert.equal(events[0].thresholdMs, 15, 'the event carries the default threshold');
  assert.equal(events[0].p99Ms, 25);
  assert.equal(events[0].pattern, 't');
  // A subscriber that takes the default with no callback still
  // latches the breach in stats — the miss is counted, just with no
  // callback fired, exactly as a no-callback explicit subscription.
  const bare = new EventBus({ now: clock.now });
  bare.setDefaultLatencySlo({ windowSize: 10, p99ThresholdMs: 15 });
  bare.subscribe('u', () => { clock.advance(25); }, { latencySlo: {} });
  for (let i = 0; i < 10; i++) bare.publish('u', i);
  await flush();
  assert.equal(sloEntry(bare, 'u').breaching, true);
});

test('EB-493: clearing with null returns new opt-in subscribers to the threshold-required status quo', () => {
  const bus = new EventBus();
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 50 });
  bus.setDefaultLatencySlo(null);
  assert.equal(bus.getDefaultLatencySlo(), undefined);
  assert.throws(() => bus.subscribe('t', () => {}, { latencySlo: {} }), RangeError);
  const audit = bus.getSubscriptionAudit({ action: 'default_latency_slo_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { windowSize: 3, p99ThresholdMs: 50 },
    latencySlo: null,
  });
});

test('EB-493: revising the bus default never moves an already-subscribed threshold, window, or samples', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 50 });
  bus.subscribe('t', () => { clock.advance(12); }, { latencySlo: {} });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(sloEntry(bus, 't').samples, 3);
  assert.equal(sloEntry(bus, 't').thresholdMs, 50);
  bus.setDefaultLatencySlo({ windowSize: 2, p99ThresholdMs: 20 });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  const entry = sloEntry(bus, 't');
  assert.equal(entry.windowSize, 3, 'the early subscriber keeps the window it was baked with');
  assert.equal(entry.thresholdMs, 50, 'the early subscriber keeps the threshold it was baked with');
  assert.equal(entry.samples, 3);
  // A subscriber joining after the revision takes the new default.
  bus.subscribe('u', () => { clock.advance(12); }, { latencySlo: {} });
  for (let i = 0; i < 5; i++) bus.publish('u', i);
  await flush();
  assert.equal(sloEntry(bus, 'u').windowSize, 2, 'the new default window is 2');
  assert.equal(sloEntry(bus, 'u').thresholdMs, 20, 'the new default threshold is 20');
});

test('EB-493: the default never enables SLO observation for a subscriber that does not opt in', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 50 });
  bus.subscribe('t', () => { clock.advance(25); });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(bus.getStats().subscriberLatencyP99, []);
});

test('EB-493: setLatencySloThreshold/setLatencySloWindowSize still revise one subscriber at a time without touching the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 50 });
  const sub = bus.subscribe('t', () => { clock.advance(12); }, { latencySlo: {} });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(sloEntry(bus, 't').thresholdMs, 50);
  assert.equal(sloEntry(bus, 't').windowSize, 3);
  bus.setLatencySloThreshold(sub.id, 20);
  bus.setLatencySloWindowSize(sub.id, 5);
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.equal(sloEntry(bus, 't').thresholdMs, 20);
  assert.equal(sloEntry(bus, 't').windowSize, 5);
  assert.deepEqual(
    bus.getDefaultLatencySlo(),
    { windowSize: 3, p99ThresholdMs: 50 },
    'the per-subscriber revisions leave the bus default alone',
  );
  // A new subscriber still takes the untouched bus default.
  bus.subscribe('u', () => { clock.advance(12); }, { latencySlo: {} });
  for (let i = 0; i < 5; i++) bus.publish('u', i);
  await flush();
  assert.equal(sloEntry(bus, 'u').thresholdMs, 50);
  assert.equal(sloEntry(bus, 'u').windowSize, 3);
});

test('EB-493: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and illegal values throw RangeError with the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultLatencySlo(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_latency_slo_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultLatencySlo({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_latency_slo_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultLatencySlo(), undefined);
  const opts = { windowSize: 3, p99ThresholdMs: 50 };
  bus.setDefaultLatencySlo(opts);
  opts.windowSize = 999;
  opts.p99ThresholdMs = 999;
  assert.deepEqual(
    bus.getDefaultLatencySlo(),
    { windowSize: 3, p99ThresholdMs: 50 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 50 });
  bus.setDefaultLatencySlo({ windowSize: 3, p99ThresholdMs: 50 });
  const audit = bus.getSubscriptionAudit({ action: 'default_latency_slo_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    latencySlo: { windowSize: 3, p99ThresholdMs: 50 },
  });
  const held = bus.getDefaultLatencySlo();
  assert.ok(held !== undefined);
  held.windowSize = 999;
  held.p99ThresholdMs = 999;
  assert.deepEqual(
    bus.getDefaultLatencySlo(),
    { windowSize: 3, p99ThresholdMs: 50 },
    'the getter returns a copy',
  );
  bus.setDefaultLatencySlo({ windowSize: 5 }, 'ops');
  assert.deepEqual(bus.getDefaultLatencySlo(), { windowSize: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'default_latency_slo_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { windowSize: 3, p99ThresholdMs: 50 },
    latencySlo: { windowSize: 5 },
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
    { p99ThresholdMs: 0 },
    { p99ThresholdMs: -5 },
    { p99ThresholdMs: Number.NaN },
    { p99ThresholdMs: Number.POSITIVE_INFINITY },
    { p99ThresholdMs: '50' },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultLatencySlo(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultLatencySlo(), { windowSize: 5 });
  }
  assert.throws(() => bus.setDefaultLatencySlo({ windowSize: 7 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultLatencySlo(), { windowSize: 5 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_latency_slo_revised' }).length,
    2,
    'failed revisions must not audit',
  );
});
