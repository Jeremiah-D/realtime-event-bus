import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { HotKeyMitigatedEvent } from '../src/keyhotspot.ts';

/**
 * Key-hotspot auto-mitigation recovery-depth revision (EB-294):
 * `setKeyHotspotRecoverDepth` retunes a monitored subscriber's
 * `mitigate.recoverDepth` without re-subscribing — only that depth
 * moves, the `thresholdDepth`, the `onKeyHotspot` /
 * `onHotKeyMitigated` callbacks, and the `mitigatedKeys` latch are
 * untouched, and the new depth governs from the next keyed-delivery
 * evaluation (which reads the live value).
 *
 * Determinism tricks (as in `keyhotspot.test.ts`): a delayed
 * predecessor (`publishDelayed`, due later) holds the key's keySeq 1,
 * so the live publishes that follow pile onto the reorder buffer
 * synchronously — each one is a keyed-delivery evaluation, exactly
 * like the production path. A staggered pile-up (two delayed
 * predecessors with different due times) drains the buffer only
 * part-way: the first predecessor's cascade stops at the second
 * predecessor's keySeq, so the release decision at an intermediate
 * depth is observable.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Builds a live reorder buffer of `depth` for one key: one delayed predecessor plus `depth` held live messages. */
function pileUp(bus: EventBus, key: string, depth: number): void {
  bus.publishDelayed('a', 'delayed', { delayMs: 10_000, key });
  for (let i = 0; i < depth; i++) {
    bus.publish('b', `live${i}`, { key });
  }
}

/**
 * Builds a staggered pile-up: a predecessor due in 10s, two held live
 * messages, a second predecessor due in 30s, two more held live
 * messages — depth 4 in total, draining to depth 2 when only the
 * first predecessor has fanned out.
 */
function pileUpStaggered(bus: EventBus, key: string): void {
  bus.publishDelayed('a', 'pre1', { delayMs: 10_000, key });
  bus.publish('b', 'live0', { key });
  bus.publish('c', 'live1', { key });
  bus.publishDelayed('a', 'pre2', { delayMs: 30_000, key });
  bus.publish('b', 'live2', { key });
  bus.publish('c', 'live3', { key });
}

/** Fans out the delayed predecessor(s) due within `ms` so the held buffer drains. */
async function drainBy(clock: { nowMs: number }, bus: EventBus, ms: number): Promise<void> {
  clock.nowMs += ms;
  bus.publish('x', 'trigger');
  await flush();
}

/** Fans out the delayed predecessor so the held buffer drains in order. */
async function drain(clock: { nowMs: number }, bus: EventBus): Promise<void> {
  await drainBy(clock, bus, 10_000);
}

test('setKeyHotspotRecoverDepth rejects unknown, empty, unmonitored, detection-only, and invalid revisions fail-closed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: { onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  const plain = bus.subscribe('t2', () => {});
  const detectionOnly = bus.subscribe('t3', () => {}, { keyHotspot: { thresholdDepth: 2 } });
  for (const bad of [
    () => bus.setKeyHotspotRecoverDepth('no-such-sub', 1),
    () => bus.setKeyHotspotRecoverDepth('', 1),
    () => bus.setKeyHotspotRecoverDepth(plain.id, 1),
    () => bus.setKeyHotspotRecoverDepth(detectionOnly.id, 1),
    () => bus.setKeyHotspotRecoverDepth(sub.id, -1),
    () => bus.setKeyHotspotRecoverDepth(sub.id, 2.5),
    () => bus.setKeyHotspotRecoverDepth(sub.id, Number.NaN),
    () => bus.setKeyHotspotRecoverDepth(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setKeyHotspotRecoverDepth(sub.id, 3),
    () => bus.setKeyHotspotRecoverDepth(sub.id, '1' as never),
    () => bus.setKeyHotspotRecoverDepth(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_recover_depth_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live depth is untouched: the next engagement still carries
  // the subscribe-time recoverDepth.
  pileUp(bus, 'k', 2);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 2);
  assert.equal(mitigations[0].recoverDepth, 2);
  await drain(clock, bus);
  sub.unsubscribe();
  plain.unsubscribe();
  detectionOnly.unsubscribe();
});

test('re-affirming the current depth is a no-op with no audit', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: { recoverDepth: 1, onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  bus.setKeyHotspotRecoverDepth(sub.id, 1);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_recover_depth_revised' }).length,
    0,
    'a same-depth revision must not audit',
  );
  // The depth is unchanged: the next engagement still carries it.
  pileUp(bus, 'k', 2);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].recoverDepth, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('a revised depth is audited and governs the next engagement; the threshold is untouched', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 4,
      autoMitigate: { onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  bus.setKeyHotspotRecoverDepth(sub.id, 1);
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_recover_depth_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, '*');
  assert.deepEqual(audit[0].detail, { recoverDepth: 1, oldRecoverDepth: 4 });
  // The next engagement carries the new depth and the old threshold.
  pileUp(bus, 'k', 4);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 4);
  assert.equal(mitigations[0].recoverDepth, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('a depth above the live threshold is refused, and the revision repairs an EB-289 inverted hysteresis band', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 5,
      autoMitigate: { onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  // Lowering the alert threshold leaves recoverDepth at its
  // subscribe-time snapshot (5) — now above the live threshold (2).
  bus.setKeyHotspotThreshold(sub.id, 2);
  assert.throws(() => bus.setKeyHotspotRecoverDepth(sub.id, 3), RangeError);
  assert.throws(() => bus.setKeyHotspotRecoverDepth(sub.id, 5), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_recover_depth_revised' }).length,
    0,
    'refused depths must not audit',
  );
  // Revising down to at most the live threshold repairs the band.
  bus.setKeyHotspotRecoverDepth(sub.id, 2);
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_recover_depth_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { recoverDepth: 2, oldRecoverDepth: 5 });
  pileUp(bus, 'k', 2);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 2);
  assert.equal(mitigations[0].recoverDepth, 2);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('lowering the depth keeps an engaged mitigation engaged until the buffer drains below the new depth', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: {
      thresholdDepth: 4,
      autoMitigate: { onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  pileUpStaggered(bus, 'k'); // depth 4 -> mitigation engages, recoverDepth mirrors the threshold
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].recoverDepth, 4);
  bus.setKeyHotspotRecoverDepth(sub.id, 2);
  // Only the first predecessor fans out: the buffer drains to depth 2
  // ({live2, live3}), which is below the old depth (4) but not below
  // the new one (2) — the mitigation stays engaged.
  await drainBy(clock, bus, 10_000);
  assert.deepEqual(received, ['trigger', 'pre1', 'live0', 'live1']);
  const hot = bus.getStats().hotKeys.find((h) => h.key === 'k');
  assert.ok(hot, 'the partially drained stream must still be reported');
  assert.equal(hot.bufferedDepth, 2);
  assert.equal(hot.mitigated, true);
  // A new out-of-order arrival is still shed while mitigated.
  bus.publish('d', 'probe', { key: 'k' });
  assert.equal(bus.getStats().hotKeyShedMessages, 1);
  assert.equal(mitigations.length, 1, 'no re-engagement while still mitigated');
  // The second predecessor fans out: the buffer drains to 0, below the
  // new depth — the mitigation releases, and the shed probe never
  // delivers.
  await drainBy(clock, bus, 20_000);
  assert.deepEqual(received, ['trigger', 'pre1', 'live0', 'live1', 'trigger', 'pre2', 'live2', 'live3']);
  assert.equal(bus.getStats().hotKeys.length, 0);
  sub.unsubscribe();
});

test('raising the depth releases an engaged mitigation at the next evaluation below the new depth', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: {
      thresholdDepth: 4,
      autoMitigate: { recoverDepth: 2, onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  pileUpStaggered(bus, 'k'); // depth 4 -> mitigation engages, recoverDepth 2
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].recoverDepth, 2);
  bus.setKeyHotspotRecoverDepth(sub.id, 3);
  // Only the first predecessor fans out: the buffer drains to depth 2,
  // below the new depth (3) — the mitigation releases at once, even
  // though the old depth (2) would have kept it engaged.
  await drainBy(clock, bus, 10_000);
  assert.deepEqual(received, ['trigger', 'pre1', 'live0', 'live1']);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.mitigated, false);
  // A new out-of-order arrival now buffers instead of being shed.
  bus.publish('d', 'probe', { key: 'k' });
  assert.equal(bus.getStats().hotKeyShedMessages, 0);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.bufferedDepth, 3);
  await drainBy(clock, bus, 20_000);
  assert.deepEqual(received, [
    'trigger', 'pre1', 'live0', 'live1',
    'trigger', 'pre2', 'live2', 'live3', 'probe',
  ]);
  sub.unsubscribe();
});

test('recoverDepth 0 means no automatic release: a drained stream stays mitigated', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: { onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  pileUp(bus, 'k', 2); // depth 2 -> mitigation engages, recoverDepth 2
  assert.equal(mitigations.length, 1);
  // Revising to 0 while mitigated does not release at once — the
  // release decision is taken at the next evaluation, which reads the
  // live depth, and the buffer can never drain strictly below zero.
  bus.setKeyHotspotRecoverDepth(sub.id, 0);
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_recover_depth_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { recoverDepth: 0, oldRecoverDepth: 2 });
  await drain(clock, bus);
  assert.deepEqual(received, ['trigger', 'delayed', 'live0', 'live1']);
  // The stream is still mitigated: the next pile-up's live messages
  // are shed, never delivered, and no second engagement fires.
  pileUp(bus, 'k', 2);
  assert.equal(mitigations.length, 1, 'no re-engagement while still mitigated');
  assert.equal(bus.getStats().hotKeyShedMessages, 2);
  await drain(clock, bus);
  assert.deepEqual(received, ['trigger', 'delayed', 'live0', 'live1', 'trigger', 'delayed']);
  sub.unsubscribe();
});
