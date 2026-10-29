import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type AckSloMiss, type Delivery } from '../src/bus.ts';
import { AckLatencyTracker } from '../src/acklatency.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Reads the one ack-latency stats row for pattern 't'. */
function ackRow(bus: EventBus) {
  const rows = bus.getStats().ackLatency.filter((r) => r.pattern === 't');
  assert.equal(rows.length, 1, 'expected one ackLatency row for t');
  return rows[0];
}

/** A reliable subscription that collects deliveries instead of acking them. */
function trackedBus(opts: {
  windowSize?: number;
  ackSloMs?: number;
  onAckSloMiss?: (event: AckSloMiss) => void;
}) {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d as Delivery<unknown>);
    },
    {
      ackLatency: opts.windowSize === undefined ? true : { windowSize: opts.windowSize },
      ...(opts.ackSloMs !== undefined ? { ackSloMs: opts.ackSloMs } : {}),
      ...(opts.onAckSloMiss !== undefined ? { onAckSloMiss: opts.onAckSloMiss } : {}),
    },
  );
  return { clock, bus, seen, sub };
}

/** Publishes one message, delivers it, then acks it `latencyMs` later. */
async function sampleOne(
  ctx: ReturnType<typeof trackedBus>,
  latencyMs: number,
): Promise<void> {
  ctx.bus.publish('t', ctx.seen.length);
  await flush();
  const delivery = ctx.seen[ctx.seen.length - 1];
  ctx.clock.nowMs += latencyMs;
  delivery.ack();
}

/** Samples each latency in order on one tracked subscription. */
async function sampleAll(
  ctx: ReturnType<typeof trackedBus>,
  latencies: number[],
): Promise<void> {
  for (const ms of latencies) await sampleOne(ctx, ms);
}

// --- Tracker unit tests: resize / retune semantics. ---

test('AckLatencyTracker.resize evicts oldest-first, syncs sum and SLO counts, and never restores', () => {
  const clock = controllableClock(0);
  const tracker = new AckLatencyTracker({
    windowSize: 4,
    sloMs: 100,
    now: clock.now,
  });
  // Samples: 10 (ok), 20 (ok), 200 (miss), 30 (ok).
  for (const ms of [10, 20, 200, 30]) {
    const key = {};
    tracker.accepted(key);
    clock.nowMs += ms;
    tracker.sampleOnAck(key);
  }
  const samplesRef = (tracker as unknown as { samples: unknown[] }).samples;
  assert.deepEqual(tracker.summary(), {
    samples: 4,
    p50Ms: 20,
    p95Ms: 200,
    p99Ms: 200,
    minMs: 10,
    maxMs: 200,
    meanMs: 65,
    withinSlo: 3,
    sloAttainment: 0.75,
  });

  tracker.resize(2);
  assert.equal(tracker.windowSizeLimit(), 2);
  assert.equal(
    (tracker as unknown as { samples: unknown[] }).samples,
    samplesRef,
    'resize splices in place, it never replaces the sample array',
  );
  // Surviving samples are 200, 30: the two oldest (10, 20) are gone.
  assert.deepEqual(tracker.summary(), {
    samples: 2,
    p50Ms: 30,
    p95Ms: 200,
    p99Ms: 200,
    minMs: 30,
    maxMs: 200,
    meanMs: 115,
    withinSlo: 1,
    sloAttainment: 0.5,
  });

  // Widening evicts nothing and restores nothing.
  tracker.resize(6);
  assert.equal(tracker.windowSizeLimit(), 6);
  assert.equal(tracker.summary().samples, 2);
  assert.equal(tracker.summary().meanMs, 115);

  for (const bad of [
    () => tracker.resize(0),
    () => tracker.resize(-1),
    () => tracker.resize(2.5),
    () => tracker.resize(Number.NaN),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(tracker.windowSizeLimit(), 6, 'a failed resize leaves the bound untouched');
  assert.equal(tracker.summary().samples, 2);
});

test('AckLatencyTracker.retune re-scores retained samples and replaces the callback', () => {
  const clock = controllableClock(0);
  const oldMisses: number[] = [];
  const newMisses: number[] = [];
  const tracker = new AckLatencyTracker({
    windowSize: 4,
    sloMs: 100,
    now: clock.now,
    onSloMiss: (e) => oldMisses.push(e.latencyMs),
  });
  for (const ms of [10, 200]) {
    const key = {};
    tracker.accepted(key);
    clock.nowMs += ms;
    tracker.sampleOnAck(key);
  }
  assert.equal(tracker.summary().withinSlo, 1);

  // Loosening the SLO re-scores without touching any sample.
  tracker.retune(300);
  assert.equal(tracker.sloMsLimit(), 300);
  assert.equal(tracker.summary().samples, 2);
  assert.equal(tracker.summary().withinSlo, 2);
  assert.equal(tracker.summary().sloAttainment, 1);

  // Tightening re-scores the other way.
  tracker.retune(50);
  assert.equal(tracker.summary().withinSlo, 1);
  assert.equal(tracker.summary().sloAttainment, 0.5);

  // Replacing the callback: only the new one fires, under the new SLO.
  tracker.retune(50, (e) => newMisses.push(e.latencyMs));
  const key = {};
  tracker.accepted(key);
  clock.nowMs += 150;
  tracker.sampleOnAck(key);
  assert.deepEqual(oldMisses, [200]);
  assert.deepEqual(newMisses, [150]);

  for (const bad of [
    () => tracker.retune(0),
    () => tracker.retune(-1),
    () => tracker.retune(Number.NaN),
    () => tracker.retune(Number.POSITIVE_INFINITY),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(tracker.sloMsLimit(), 50, 'a failed retune leaves the SLO untouched');
});

// --- Bus integration: setAckLatency. ---

test('setAckLatency rejects unknown, unsampled, and undefined targets with config untouched', async () => {
  const ctx = trackedBus({ windowSize: 4, ackSloMs: 100 });
  await sampleAll(ctx, [10, 20]);
  assert.equal(ackRow(ctx.bus).samples, 2);

  const plain = ctx.bus.subscribe('plain', () => {});
  for (const bad of [
    () => ctx.bus.setAckLatency('no-such-sub', { windowSize: 2 }),
    () => ctx.bus.setAckLatency(plain.id, { windowSize: 2 }),
    () => ctx.bus.setAckLatency(ctx.sub.id, undefined as unknown as { windowSize: number }),
    () => ctx.bus.setAckLatency('', { windowSize: 2 }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(ackRow(ctx.bus).samples, 2, 'a rejected revision leaves samples untouched');
  assert.equal(ackRow(ctx.bus).meanMs, 15);
  assert.equal(
    ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' }).length,
    0,
    'a rejected revision leaves the audit log untouched',
  );
  plain.unsubscribe();
  ctx.sub.unsubscribe();
});

test('setAckLatency rejects illegal merged values and non-objects with config untouched', async () => {
  const misses: AckSloMiss[] = [];
  const ctx = trackedBus({
    windowSize: 4,
    ackSloMs: 100,
    onAckSloMiss: (e) => misses.push(e),
  });
  await sampleAll(ctx, [10, 20]);
  for (const bad of [
    () => ctx.bus.setAckLatency(ctx.sub.id, { windowSize: 0 }),
    () => ctx.bus.setAckLatency(ctx.sub.id, { windowSize: 2.5 }),
    () => ctx.bus.setAckLatency(ctx.sub.id, { ackSloMs: 0 }),
    () => ctx.bus.setAckLatency(ctx.sub.id, { ackSloMs: -5 }),
    () => ctx.bus.setAckLatency(ctx.sub.id, { ackSloMs: Number.NaN }),
    () => ctx.bus.setAckLatency(ctx.sub.id, { ackSloMs: Number.POSITIVE_INFINITY }),
    () =>
      ctx.bus.setAckLatency(ctx.sub.id, {
        onAckSloMiss: 'nope' as unknown as (e: AckSloMiss) => void,
      }),
    () => ctx.bus.setAckLatency(ctx.sub.id, 'nope' as unknown as { windowSize: number }),
    () => ctx.bus.setAckLatency(ctx.sub.id, [] as unknown as { windowSize: number }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(ackRow(ctx.bus).samples, 2);
  assert.equal(ackRow(ctx.bus).withinSlo, 2);
  assert.deepEqual(misses, []);
  assert.equal(
    ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' }).length,
    0,
  );
  ctx.sub.unsubscribe();
});

test('setAckLatency partial SLO revision re-scores retained samples and audits old vs new', async () => {
  const misses: AckSloMiss[] = [];
  const ctx = trackedBus({
    windowSize: 4,
    ackSloMs: 100,
    onAckSloMiss: (e) => misses.push(e),
  });
  await sampleAll(ctx, [10, 20, 200, 30]);
  assert.equal(ackRow(ctx.bus).withinSlo, 3);
  assert.deepEqual(
    misses.map((e) => e.latencyMs),
    [200],
  );

  // Partial revision: only the SLO moves; the window and callback stay.
  ctx.bus.setAckLatency(ctx.sub.id, { ackSloMs: 300 });
  assert.equal(ackRow(ctx.bus).samples, 4, 'a SLO-only revision keeps every sample');
  assert.equal(ackRow(ctx.bus).withinSlo, 4, 'retained samples are re-scored under the new SLO');
  assert.equal(ackRow(ctx.bus).sloAttainment, 1);
  const audit = ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    ackLatency: { windowSize: 4, ackSloMs: 300, onAckSloMiss: 'set' },
    oldAckLatency: { windowSize: 4, ackSloMs: 100, onAckSloMiss: 'set' },
  });
  ctx.sub.unsubscribe();
});

test('setAckLatency shrinking the window evicts only the oldest samples; widening restores nothing', async () => {
  const ctx = trackedBus({ windowSize: 4, ackSloMs: 100 });
  await sampleAll(ctx, [10, 20, 200, 30]);
  assert.equal(ackRow(ctx.bus).meanMs, 65);

  ctx.bus.setAckLatency(ctx.sub.id, { windowSize: 2 });
  assert.equal(ackRow(ctx.bus).samples, 2);
  assert.equal(ackRow(ctx.bus).minMs, 30);
  assert.equal(ackRow(ctx.bus).maxMs, 200);
  assert.equal(ackRow(ctx.bus).meanMs, 115);
  assert.equal(ackRow(ctx.bus).withinSlo, 1);
  const audit = ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    ackLatency: { windowSize: 2, ackSloMs: 100 },
    oldAckLatency: { windowSize: 4, ackSloMs: 100 },
  });

  ctx.bus.setAckLatency(ctx.sub.id, { windowSize: 6 });
  assert.equal(ackRow(ctx.bus).samples, 2, 'widening keeps the shrunk window as-is');
  await sampleOne(ctx, 40);
  assert.equal(ackRow(ctx.bus).samples, 3, 'the new bound governs the next samples');
  assert.equal(ackRow(ctx.bus).maxMs, 200);
  ctx.sub.unsubscribe();
});

test('setAckLatency keeps the tracker instance: in-flight clocks and samples survive', async () => {
  const oldMisses: AckSloMiss[] = [];
  const newMisses: AckSloMiss[] = [];
  const ctx = trackedBus({
    windowSize: 4,
    ackSloMs: 1000,
    onAckSloMiss: (e) => oldMisses.push(e),
  });
  // One sample before the revision, plus one message accepted but not yet acked.
  await sampleOne(ctx, 10);
  ctx.bus.publish('t', 'in-flight');
  await flush();
  const inFlight = ctx.seen[ctx.seen.length - 1];

  // Revise window, SLO, and callback while the message is in flight.
  ctx.bus.setAckLatency(ctx.sub.id, {
    windowSize: 2,
    ackSloMs: 100,
    onAckSloMiss: (e) => newMisses.push(e),
  });
  assert.equal(ackRow(ctx.bus).samples, 1, 'the pre-revision sample survives');

  // The in-flight ack is measured from its original accept, not the revision...
  ctx.clock.nowMs += 250;
  inFlight.ack();
  assert.equal(ackRow(ctx.bus).samples, 2);
  assert.equal(ackRow(ctx.bus).maxMs, 250);
  // ...and scored and reported under the new SLO and callback, with context.
  assert.deepEqual(oldMisses, []);
  assert.equal(newMisses.length, 1);
  assert.equal(newMisses[0].subscriberId, ctx.sub.id);
  assert.equal(newMisses[0].pattern, 't');
  assert.equal(newMisses[0].latencyMs, 250);
  assert.equal(newMisses[0].sloMs, 100);
  ctx.sub.unsubscribe();
});

test('setAckLatency new parameters take effect from the next sample', async () => {
  const misses: AckSloMiss[] = [];
  const ctx = trackedBus({ windowSize: 4, ackSloMs: 1000 });
  await sampleOne(ctx, 10);

  ctx.bus.setAckLatency(ctx.sub.id, {
    ackSloMs: 100,
    onAckSloMiss: (e) => misses.push(e),
  });
  await sampleOne(ctx, 150);
  assert.equal(misses.length, 1, 'the next over-budget sample fires under the new SLO');
  assert.equal(misses[0].sloMs, 100);
  assert.equal(misses[0].latencyMs, 150);
  await sampleOne(ctx, 50);
  assert.equal(misses.length, 1, 'an in-budget sample under the new SLO fires nothing');
  ctx.sub.unsubscribe();
});

test('setAckLatency callback replacement keeps subscriberId/pattern context', async () => {
  const oldMisses: AckSloMiss[] = [];
  const newMisses: AckSloMiss[] = [];
  const ctx = trackedBus({
    windowSize: 4,
    ackSloMs: 100,
    onAckSloMiss: (e) => oldMisses.push(e),
  });
  const replacement = (e: AckSloMiss) => newMisses.push(e);
  ctx.bus.setAckLatency(ctx.sub.id, { onAckSloMiss: replacement });
  await sampleOne(ctx, 500);
  assert.deepEqual(oldMisses, [], 'the replaced callback never fires again');
  assert.equal(newMisses.length, 1);
  assert.equal(newMisses[0].subscriberId, ctx.sub.id);
  assert.equal(newMisses[0].pattern, 't');
  assert.equal(newMisses[0].latencyMs, 500);
  assert.equal(newMisses[0].sloMs, 100);
  const audit = ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    ackLatency: { windowSize: 4, ackSloMs: 100, onAckSloMiss: 'set' },
    oldAckLatency: { windowSize: 4, ackSloMs: 100, onAckSloMiss: 'set' },
  });
  ctx.sub.unsubscribe();
});

test('setAckLatency identical parameters are a no-op with no audit and no eviction', async () => {
  const misses: AckSloMiss[] = [];
  const cb = (e: AckSloMiss) => misses.push(e);
  const ctx = trackedBus({ windowSize: 4, ackSloMs: 100, onAckSloMiss: cb });
  await sampleAll(ctx, [10, 20, 200, 30]);
  assert.equal(ackRow(ctx.bus).samples, 4);

  // Restating the current parameters — including the same callback — audits nothing...
  ctx.bus.setAckLatency(ctx.sub.id, { windowSize: 4, ackSloMs: 100, onAckSloMiss: cb });
  // ...and neither does an empty revision.
  ctx.bus.setAckLatency(ctx.sub.id, {});
  assert.equal(
    ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' }).length,
    0,
  );
  assert.equal(ackRow(ctx.bus).samples, 4, 'a restated window evicts nothing');
  assert.equal(ackRow(ctx.bus).meanMs, 65);
  ctx.sub.unsubscribe();
});

test('setAckLatency null clears sampling and a revision after a clear is refused', async () => {
  const ctx = trackedBus({ windowSize: 4, ackSloMs: 100 });
  await sampleAll(ctx, [10, 20]);
  assert.equal(ackRow(ctx.bus).samples, 2);

  ctx.bus.setAckLatency(ctx.sub.id, null);
  assert.equal(ctx.bus.getStats().ackLatency.length, 0, 'the subscriber leaves getStats().ackLatency');
  const audit = ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    ackLatency: null,
    oldAckLatency: { windowSize: 4, ackSloMs: 100 },
  });

  assert.throws(() => ctx.bus.setAckLatency(ctx.sub.id, { windowSize: 2 }), RangeError);
  assert.equal(
    ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' }).length,
    1,
    'the refused revision audits nothing',
  );
  // Later deliveries stay unsampled after the clear.
  await sampleOne(ctx, 30);
  assert.equal(ctx.bus.getStats().ackLatency.length, 0);
  ctx.sub.unsubscribe();
});
