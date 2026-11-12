import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type AckSloMiss, type Delivery } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Reads the one ack-latency stats row for the given pattern. */
function ackRow(bus: EventBus, pattern = 't') {
  const rows = bus.getStats().ackLatency.filter((r) => r.pattern === pattern);
  assert.equal(rows.length, 1, `expected one ackLatency row for ${pattern}`);
  return rows[0];
}

interface Tracked {
  clock: ReturnType<typeof controllableClock>;
  bus: EventBus;
}

/** A bus with a controllable clock. */
function makeBus(): Tracked {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  return { clock, bus };
}

/** Adds an ackLatency-tracked reliable subscriber and collects its deliveries. */
function subscribeTracked(
  ctx: Tracked,
  opts: {
    pattern?: string;
    ackSloMs?: number;
    onAckSloMiss?: (event: AckSloMiss) => void;
  } = {},
) {
  const seen: Array<Delivery<unknown>> = [];
  const sub = ctx.bus.subscribeReliable(
    opts.pattern ?? 't',
    (d) => {
      seen.push(d as Delivery<unknown>);
    },
    {
      ackLatency: true,
      ...(opts.ackSloMs !== undefined ? { ackSloMs: opts.ackSloMs } : {}),
      ...(opts.onAckSloMiss !== undefined ? { onAckSloMiss: opts.onAckSloMiss } : {}),
    },
  );
  return { sub, seen };
}

/** Publishes one message, delivers it, then acks it `latencyMs` later. */
async function sampleOne(
  ctx: Tracked,
  seen: Array<Delivery<unknown>>,
  pattern: string,
  latencyMs: number,
): Promise<void> {
  ctx.bus.publish(pattern, seen.length);
  await flush();
  const delivery = seen[seen.length - 1];
  ctx.clock.nowMs += latencyMs;
  delivery.ack();
}

/** Samples each latency in order on one tracked subscription. */
async function sampleAll(
  ctx: Tracked,
  seen: Array<Delivery<unknown>>,
  pattern: string,
  latencies: number[],
): Promise<void> {
  for (const ms of latencies) await sampleOne(ctx, seen, pattern, ms);
}

// EB-479: bus-level terminal ack-latency SLO default live revision.

test('EB-479: the factory bus default is unset (omitting ackSloMs scores against 30000ms)', async () => {
  const ctx = makeBus();
  assert.equal(ctx.bus.getDefaultAckSloMs(), undefined);
  const { sub, seen } = subscribeTracked(ctx);
  // 40000ms is over the historical 30000ms terminal fallback only.
  await sampleAll(ctx, seen, 't', [10, 40000]);
  assert.equal(ackRow(ctx.bus).samples, 2);
  assert.equal(ackRow(ctx.bus).withinSlo, 1);
  assert.equal(ackRow(ctx.bus).sloAttainment, 0.5);
  sub.unsubscribe();
});

test('EB-479: a new subscriber takes the bus default and withinSlo/sloAttainment score against it', async () => {
  const ctx = makeBus();
  ctx.bus.setDefaultAckSloMs(100);
  assert.equal(ctx.bus.getDefaultAckSloMs(), 100);
  const { sub, seen } = subscribeTracked(ctx);
  await sampleAll(ctx, seen, 't', [10, 20, 200, 30]);
  assert.equal(ackRow(ctx.bus).samples, 4);
  assert.equal(ackRow(ctx.bus).withinSlo, 3);
  assert.equal(ackRow(ctx.bus).sloAttainment, 0.75);
  const audit = ctx.bus.getSubscriptionAudit({ action: 'default_ack_slo_ms_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, ackSloMs: 100 });
  sub.unsubscribe();
});

test('EB-479: an explicit ackSloMs beats the bus default in both directions', async () => {
  // Explicit 5000 beats a bus default of 100: a 200ms ack is within.
  const tight = makeBus();
  tight.bus.setDefaultAckSloMs(100);
  const loose = subscribeTracked(tight, { ackSloMs: 5000 });
  await sampleOne(tight, loose.seen, 't', 200);
  assert.equal(ackRow(tight.bus).withinSlo, 1);
  assert.equal(ackRow(tight.bus).sloAttainment, 1);
  loose.sub.unsubscribe();

  // Explicit 100 beats a bus default of 5000: a 200ms ack is a miss.
  const wide = makeBus();
  wide.bus.setDefaultAckSloMs(5000);
  const strict = subscribeTracked(wide, { ackSloMs: 100 });
  await sampleOne(wide, strict.seen, 't', 200);
  assert.equal(ackRow(wide.bus).withinSlo, 0);
  assert.equal(ackRow(wide.bus).sloAttainment, 0);
  strict.sub.unsubscribe();
});

test('EB-479: clearing with null returns new subscribers to 30000ms', async () => {
  const ctx = makeBus();
  ctx.bus.setDefaultAckSloMs(100);
  ctx.bus.setDefaultAckSloMs(null);
  assert.equal(ctx.bus.getDefaultAckSloMs(), undefined);
  const { sub, seen } = subscribeTracked(ctx);
  await sampleAll(ctx, seen, 't', [10, 40000]);
  assert.equal(ackRow(ctx.bus).withinSlo, 1);
  assert.equal(ackRow(ctx.bus).sloAttainment, 0.5);
  sub.unsubscribe();
});

test('EB-479: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultAckSloMs(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_ack_slo_ms_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultAckSloMs(100);
  assert.equal(bus.getDefaultAckSloMs(), 100);
  bus.setDefaultAckSloMs(100);
  assert.equal(bus.getDefaultAckSloMs(), 100);
  const audit = bus.getSubscriptionAudit({ action: 'default_ack_slo_ms_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, ackSloMs: 100 });
  // A fractional SLO is legal: the subscribe-time gate is positive
  // finite, not positive integer.
  bus.setDefaultAckSloMs(150.5, 'ops');
  assert.equal(bus.getDefaultAckSloMs(), 150.5);
  const revised = bus.getSubscriptionAudit({ action: 'default_ack_slo_ms_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, { previous: 100, ackSloMs: 150.5 });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultAckSloMs(null);
  assert.equal(bus.getDefaultAckSloMs(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_ack_slo_ms_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, { previous: 150.5, ackSloMs: null });
});

test('EB-479: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultAckSloMs(100);
  for (const bad of [0, -1, Number.NaN, Infinity, '100', {}] as unknown[]) {
    assert.throws(
      () => bus.setDefaultAckSloMs(bad as number),
      RangeError,
      `ms=${String(bad)}`,
    );
    assert.equal(bus.getDefaultAckSloMs(), 100);
  }
  assert.throws(() => bus.setDefaultAckSloMs(200, ''), RangeError);
  assert.equal(bus.getDefaultAckSloMs(), 100);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_ack_slo_ms_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-479: revising the bus default never moves an already-subscribed SLO or its samples', async () => {
  const ctx = makeBus();
  ctx.bus.setDefaultAckSloMs(100);
  const first = subscribeTracked(ctx);
  await sampleAll(ctx, first.seen, 't', [10, 20]);
  assert.equal(ackRow(ctx.bus).withinSlo, 2);

  ctx.bus.setDefaultAckSloMs(10000);
  assert.equal(ackRow(ctx.bus).samples, 2, 'the revision itself samples nothing');
  assert.equal(ackRow(ctx.bus).withinSlo, 2, 'retained samples are not re-scored');
  // A 200ms ack still misses under the baked-in 100ms SLO.
  await sampleOne(ctx, first.seen, 't', 200);
  assert.equal(ackRow(ctx.bus).samples, 3);
  assert.equal(ackRow(ctx.bus).withinSlo, 2);
  first.sub.unsubscribe();

  // A subscriber joining after the revision takes the new default.
  const late = subscribeTracked(ctx, { pattern: 'u' });
  await sampleOne(ctx, late.seen, 'u', 200);
  assert.equal(ackRow(ctx.bus, 'u').withinSlo, 1);
  assert.equal(ackRow(ctx.bus, 'u').sloAttainment, 1);
  late.sub.unsubscribe();
});

test('EB-479: the bus default never enables sampling', async () => {
  const ctx = makeBus();
  ctx.bus.setDefaultAckSloMs(100);
  const plain = ctx.bus.subscribe('t', () => {});
  assert.equal(ctx.bus.getStats().ackLatency.length, 0, 'a plain subscriber gets no tracker');
  const reliable = ctx.bus.subscribeReliable('u', () => {});
  assert.equal(ctx.bus.getStats().ackLatency.length, 0, 'a reliable subscriber without ackLatency gets no tracker');
  // The gate still reads only the subscriber's explicit value: passing
  // an explicit ackSloMs (or onAckSloMiss) without ackLatency throws,
  // even with a bus default installed.
  assert.throws(
    () => ctx.bus.subscribeReliable('v', () => {}, { ackSloMs: 50 }),
    RangeError,
  );
  assert.throws(
    () => ctx.bus.subscribeReliable('w', () => {}, { onAckSloMiss: () => {} }),
    RangeError,
  );
  assert.equal(ctx.bus.getStats().ackLatency.length, 0);
  assert.equal(
    ctx.bus.getSubscriptionAudit({ action: 'ack_latency_revised' }).length,
    0,
  );
  plain.unsubscribe();
  reliable.unsubscribe();
});

test('EB-479: setAckLatency partial revision leaves the bus default intact for new subscribers', async () => {
  const ctx = makeBus();
  ctx.bus.setDefaultAckSloMs(100);
  const first = subscribeTracked(ctx);
  await sampleAll(ctx, first.seen, 't', [10, 200]);
  assert.equal(ackRow(ctx.bus).withinSlo, 1);

  // Retune only the live sampler: its retained samples re-score under 500ms.
  ctx.bus.setAckLatency(first.sub.id, { ackSloMs: 500 });
  assert.equal(ackRow(ctx.bus).samples, 2, 'a SLO-only revision keeps every sample');
  assert.equal(ackRow(ctx.bus).withinSlo, 2);
  assert.equal(ctx.bus.getDefaultAckSloMs(), 100, 'the per-subscriber revision never touches the bus default');

  // A subscriber joining after the revision still takes the bus default.
  const late = subscribeTracked(ctx, { pattern: 'u' });
  await sampleOne(ctx, late.seen, 'u', 200);
  assert.equal(ackRow(ctx.bus, 'u').withinSlo, 0, 'the late joiner scores against the 100ms default, not the 500ms retune');
  first.sub.unsubscribe();
  late.sub.unsubscribe();
});

test('EB-479: onAckSloMiss fires against the bus default SLO', async () => {
  const misses: AckSloMiss[] = [];
  const ctx = makeBus();
  ctx.bus.setDefaultAckSloMs(100);
  const tracked = subscribeTracked(ctx, {
    onAckSloMiss: (e) => misses.push(e),
  });
  await sampleOne(ctx, tracked.seen, 't', 150);
  assert.equal(misses.length, 1, 'an over-default ack fires under the default SLO');
  assert.equal(misses[0].subscriberId, tracked.sub.id);
  assert.equal(misses[0].pattern, 't');
  assert.equal(misses[0].latencyMs, 150);
  assert.equal(misses[0].sloMs, 100);
  await sampleOne(ctx, tracked.seen, 't', 50);
  assert.equal(misses.length, 1, 'an in-default ack fires nothing');
  assert.equal(ackRow(ctx.bus).withinSlo, 1);
  tracked.sub.unsubscribe();
});
