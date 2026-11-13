import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type Delivery } from '../src/bus.ts';

/**
 * Bus-level default ack-latency sampling window (EB-492):
 * `setDefaultAckLatency` installs the `windowSize` new opt-in
 * ack-latency subscribers fall back to when they omit the field — the
 * priority chain is subscriber-explicit > bus default > 1024 — and the
 * default never enables sampling: a subscriber that does not opt into
 * `ackLatency` is untouched by it. The `ackSloMs` linkage is untouched:
 * the SLO still resolves subscriber-explicit > `defaultAckSloMs`
 * (EB-479) > 30000ms.
 *
 * Window assertions follow `acklatency.test.ts`: an injectable clock
 * makes the accepted→ack round trip deterministic, and the `samples`
 * count in `getStats().ackLatency` exposes the window bound —
 * publish and ack more messages than the window and the count caps
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

function samplesFor(bus: EventBus, pattern: string): number {
  const entry = bus.getStats().ackLatency.find((e) => e.pattern === pattern);
  assert.ok(entry !== undefined, `expected an ackLatency entry for ${pattern}`);
  return entry.samples;
}

/**
 * Publishes `count` messages on `topic`, lets the flush hand them to
 * the collector, then acks each one after `dwellMs` — one sample per
 * message at that dwell.
 */
async function publishAck(
  bus: EventBus,
  clock: { advance: (ms: number) => void },
  sub: { id: string },
  seen: Array<Delivery<unknown>>,
  topic: string,
  count: number,
  dwellMs = 15,
) {
  const before = seen.length;
  for (let i = 0; i < count; i++) bus.publish(topic, i);
  clock.advance(dwellMs);
  await flush();
  assert.equal(seen.length, before + count, 'every publish was handed out');
  for (let j = before; j < seen.length; j++) seen[j].ack();
  assert.equal(bus.droppedCount(sub.id), 0);
}

test('EB-492: the factory bus default is unset (getter undefined, clearing audits nothing)', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  assert.equal(bus.getDefaultAckLatency(), undefined);
  bus.setDefaultAckLatency(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_ack_latency_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  // No default installed: a bare opt-in still takes the documented 1024.
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable('t', (d) => seen.push(d as Delivery<unknown>), {
    ackLatency: {},
  });
  await publishAck(bus, clock, sub, seen, 't', 5);
  assert.equal(samplesFor(bus, 't'), 5);
  sub.unsubscribe();
});

test('EB-492: true takes the whole bus default window', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultAckLatency({ windowSize: 3 });
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable('t', (d) => seen.push(d as Delivery<unknown>), {
    ackLatency: true,
  });
  await publishAck(bus, clock, sub, seen, 't', 5);
  assert.equal(samplesFor(bus, 't'), 3, 'the window caps at the default 3');
  sub.unsubscribe();
});

test('EB-492: a partial object falls back to the default and an explicit windowSize wins', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultAckLatency({ windowSize: 3 });
  const seenA: Array<Delivery<unknown>> = [];
  const seenB: Array<Delivery<unknown>> = [];
  const bare = bus.subscribeReliable('a', (d) => seenA.push(d as Delivery<unknown>), {
    ackLatency: {},
  });
  const explicit = bus.subscribeReliable('b', (d) => seenB.push(d as Delivery<unknown>), {
    ackLatency: { windowSize: 2 },
  });
  await publishAck(bus, clock, bare, seenA, 'a', 5);
  await publishAck(bus, clock, explicit, seenB, 'b', 5);
  assert.equal(samplesFor(bus, 'a'), 3, '{} takes the default window');
  assert.equal(samplesFor(bus, 'b'), 2, 'the explicit windowSize beats the default');
  bare.unsubscribe();
  explicit.unsubscribe();
});

test('EB-492: true with no default means the documented 1024', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable('t', (d) => seen.push(d as Delivery<unknown>), {
    ackLatency: true,
  });
  // The default per-subscriber queue holds 100, so feed the window in
  // waves — each wave delivers and acks fully, until the 1024 window
  // caps the count.
  for (let wave = 0; wave < 12; wave++) {
    await publishAck(bus, clock, sub, seen, 't', 90);
  }
  assert.equal(samplesFor(bus, 't'), 1024);
  sub.unsubscribe();
});

test('EB-492: clearing with null returns new opt-in subscribers to 1024', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultAckLatency({ windowSize: 3 });
  bus.setDefaultAckLatency(null);
  assert.equal(bus.getDefaultAckLatency(), undefined);
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable('t', (d) => seen.push(d as Delivery<unknown>), {
    ackLatency: true,
  });
  await publishAck(bus, clock, sub, seen, 't', 5);
  assert.equal(samplesFor(bus, 't'), 5);
  const audit = bus.getSubscriptionAudit({ action: 'default_ack_latency_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { windowSize: 3 },
    ackLatency: null,
  });
  sub.unsubscribe();
});

test('EB-492: revising the bus default never moves an already-subscribed window or its samples', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultAckLatency({ windowSize: 3 });
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable('t', (d) => seen.push(d as Delivery<unknown>), {
    ackLatency: true,
  });
  await publishAck(bus, clock, sub, seen, 't', 5);
  assert.equal(samplesFor(bus, 't'), 3);
  bus.setDefaultAckLatency({ windowSize: 2 });
  await publishAck(bus, clock, sub, seen, 't', 5);
  assert.equal(
    samplesFor(bus, 't'),
    3,
    'the early subscriber keeps the window it was baked with',
  );
  // A subscriber joining after the revision takes the new default.
  const lateSeen: Array<Delivery<unknown>> = [];
  const late = bus.subscribeReliable('u', (d) => lateSeen.push(d as Delivery<unknown>), {
    ackLatency: true,
  });
  await publishAck(bus, clock, late, lateSeen, 'u', 5);
  assert.equal(samplesFor(bus, 'u'), 2, 'the new default window is 2');
  sub.unsubscribe();
  late.unsubscribe();
});

test('EB-492: the default never enables sampling', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultAckLatency({ windowSize: 3 });
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable('t', (d) => seen.push(d as Delivery<unknown>));
  await publishAck(bus, clock, sub, seen, 't', 5);
  assert.equal(seen.length, 5);
  assert.deepEqual(bus.getStats().ackLatency, []);
  sub.unsubscribe();
});

test('EB-492: setAckLatency still revises one sampler at a time without touching the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultAckLatency({ windowSize: 3 });
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable('t', (d) => seen.push(d as Delivery<unknown>), {
    ackLatency: true,
  });
  await publishAck(bus, clock, sub, seen, 't', 5);
  assert.equal(samplesFor(bus, 't'), 3);
  bus.setAckLatency(sub.id, { windowSize: 5 });
  await publishAck(bus, clock, sub, seen, 't', 5);
  assert.equal(samplesFor(bus, 't'), 5, 'the widened window keeps the 3 old samples');
  assert.deepEqual(bus.getDefaultAckLatency(), { windowSize: 3 });
  bus.setAckLatency(sub.id, null);
  assert.deepEqual(bus.getStats().ackLatency, []);
  // A new subscriber still takes the untouched bus default.
  const freshSeen: Array<Delivery<unknown>> = [];
  const fresh = bus.subscribeReliable('u', (d) => freshSeen.push(d as Delivery<unknown>), {
    ackLatency: true,
  });
  await publishAck(bus, clock, fresh, freshSeen, 'u', 5);
  assert.equal(samplesFor(bus, 'u'), 3);
  sub.unsubscribe();
  fresh.unsubscribe();
});

test('EB-492: the ackSloMs linkage is untouched — the window default and the SLO default coexist', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultAckLatency({ windowSize: 3 });
  bus.setDefaultAckSloMs(100);
  const seen: Array<Delivery<unknown>> = [];
  const sub = bus.subscribeReliable('t', (d) => seen.push(d as Delivery<unknown>), {
    ackLatency: true,
  });
  // A 15ms round trip is within the 100ms SLO default.
  await publishAck(bus, clock, sub, seen, 't', 1);
  let entry = bus.getStats().ackLatency.find((e) => e.pattern === 't');
  assert.ok(entry !== undefined);
  assert.equal(entry.samples, 1);
  assert.equal(entry.withinSlo, 1);
  // A 200ms round trip exceeds the 100ms SLO default but not the
  // 30000ms built-in — it must count as a miss, proving the SLO
  // default (EB-479) still governs while the window default caps
  // the samples at 3.
  await publishAck(bus, clock, sub, seen, 't', 1, 200);
  entry = bus.getStats().ackLatency.find((e) => e.pattern === 't');
  assert.ok(entry !== undefined);
  assert.equal(entry.samples, 2);
  assert.equal(entry.withinSlo, 1, 'the 200ms sample missed the 100ms SLO default');
  assert.equal(entry.sloAttainment, 0.5);
  sub.unsubscribe();
});

test('EB-492: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and illegal values throw RangeError with the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultAckLatency(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_ack_latency_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultAckLatency({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_ack_latency_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultAckLatency(), undefined);
  const opts = { windowSize: 3 };
  bus.setDefaultAckLatency(opts);
  opts.windowSize = 999;
  assert.deepEqual(
    bus.getDefaultAckLatency(),
    { windowSize: 3 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultAckLatency({ windowSize: 3 });
  bus.setDefaultAckLatency({ windowSize: 3 });
  const audit = bus.getSubscriptionAudit({ action: 'default_ack_latency_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    ackLatency: { windowSize: 3 },
  });
  const held = bus.getDefaultAckLatency();
  assert.ok(held !== undefined);
  held.windowSize = 999;
  assert.deepEqual(
    bus.getDefaultAckLatency(),
    { windowSize: 3 },
    'the getter returns a copy',
  );
  bus.setDefaultAckLatency({ windowSize: 5 }, 'ops');
  assert.deepEqual(bus.getDefaultAckLatency(), { windowSize: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'default_ack_latency_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { windowSize: 3 },
    ackLatency: { windowSize: 5 },
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
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultAckLatency(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultAckLatency(), { windowSize: 5 });
  }
  assert.throws(() => bus.setDefaultAckLatency({ windowSize: 7 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultAckLatency(), { windowSize: 5 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_ack_latency_revised' }).length,
    2,
    'failed revisions must not audit',
  );
});
