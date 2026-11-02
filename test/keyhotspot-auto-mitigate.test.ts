import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { HotKeyMitigatedEvent, KeyHotspotEvent } from '../src/keyhotspot.ts';

/**
 * Key-hotspot auto-mitigation switch revision (EB-338):
 * `setKeyHotspotAutoMitigate` enables, retunes, or disables a
 * monitored subscriber's per-key auto-mitigation without
 * re-subscribing — only that switch moves, the `thresholdDepth` and
 * the `onKeyHotspot` callback are untouched, enabling never touches
 * the alert latch, and disabling also clears the `mitigatedKeys`
 * latch so engaged streams release at once.
 *
 * Determinism tricks (as in `keyhotspot.test.ts`): a delayed
 * predecessor (`publishDelayed`, due later) holds the key's keySeq 1,
 * so the live publishes that follow pile onto the reorder buffer
 * synchronously — each one is a keyed-delivery evaluation, exactly
 * like the production path.
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

/** Fans out the delayed predecessor so the held buffer drains in order. */
async function drain(clock: { nowMs: number }, bus: EventBus): Promise<void> {
  clock.nowMs += 10_000;
  bus.publish('x', 'trigger');
  await flush();
}

test('setKeyHotspotAutoMitigate rejects unknown, empty, unmonitored, and invalid revisions fail-closed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: { recoverDepth: 1, onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  const plain = bus.subscribe('t2', () => {});
  const detectionOnly = bus.subscribe('t3', () => {}, { keyHotspot: { thresholdDepth: 2 } });
  for (const bad of [
    () => bus.setKeyHotspotAutoMitigate('no-such-sub', true),
    () => bus.setKeyHotspotAutoMitigate('', true),
    () => bus.setKeyHotspotAutoMitigate(plain.id, true),
    () => bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: -1 }),
    () => bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: 2.5 }),
    () => bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: Number.NaN }),
    () => bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: Number.POSITIVE_INFINITY }),
    () => bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: 3 }),
    () => bus.setKeyHotspotAutoMitigate(sub.id, { onHotKeyMitigated: 'x' as never }),
    () => bus.setKeyHotspotAutoMitigate(sub.id, 'yes' as never),
    () => bus.setKeyHotspotAutoMitigate(sub.id, 42 as never),
    () => bus.setKeyHotspotAutoMitigate(sub.id, undefined as never),
    // A detection-only subscription refuses a depth above its live
    // threshold, even though it could opt in at a legal depth.
    () => bus.setKeyHotspotAutoMitigate(detectionOnly.id, { recoverDepth: 3 }),
  ]) {
    assert.throws(bad, (err: unknown) => err instanceof RangeError || err instanceof TypeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live switch is untouched: the next engagement still carries
  // the subscribe-time recoverDepth.
  pileUp(bus, 'k', 2);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 2);
  assert.equal(mitigations[0].recoverDepth, 1);
  await drain(clock, bus);
  sub.unsubscribe();
  plain.unsubscribe();
  detectionOnly.unsubscribe();
});

test('closing a detection-only subscription is a no-op with no audit, and detection keeps working', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const alerts: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      onKeyHotspot: (e) => alerts.push(e),
    },
  });
  bus.setKeyHotspotAutoMitigate(sub.id, false);
  bus.setKeyHotspotAutoMitigate(sub.id, null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' }).length,
    0,
    'closing an already-disabled switch must not audit',
  );
  // Detection still alerts on the monitored subscription, and no
  // mitigation engages anywhere.
  pileUp(bus, 'k', 2);
  assert.equal(alerts.length, 1);
  assert.equal(bus.getStats().hotKeyShedMessages, 0);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.mitigated, false);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('enabling with true defaults recoverDepth to the live threshold and is audited', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 5 },
  });
  // Lowering the alert threshold first: `true` must take the LIVE
  // threshold (3), not the subscribe-time one (5).
  bus.setKeyHotspotThreshold(sub.id, 3);
  bus.setKeyHotspotAutoMitigate(sub.id, true);
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, '*');
  assert.deepEqual(audit[0].detail, {
    enabled: true,
    recoverDepth: 3,
    oldEnabled: false,
    oldRecoverDepth: null,
  });
  // The switch is live: the pile-up engages mitigation (the next
  // out-of-order arrival is shed) even though no callback was armed.
  pileUp(bus, 'k', 3);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.mitigated, true);
  bus.publish('b', 'probe', { key: 'k' });
  assert.equal(bus.getStats().hotKeyShedMessages, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('enabling with an object that omits recoverDepth also defaults it to the live threshold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 4 },
  });
  const onHotKeyMitigated = (e: HotKeyMitigatedEvent) => mitigations.push(e);
  bus.setKeyHotspotAutoMitigate(sub.id, { onHotKeyMitigated });
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    enabled: true,
    recoverDepth: 4,
    oldEnabled: false,
    oldRecoverDepth: null,
  });
  pileUp(bus, 'k', 4);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 4);
  assert.equal(mitigations[0].recoverDepth, 4);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('enabling with an object tunes recoverDepth and the callback; the engagement carries the new values', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 4 },
  });
  const onHotKeyMitigated = (e: HotKeyMitigatedEvent) => mitigations.push(e);
  bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: 1, onHotKeyMitigated });
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    enabled: true,
    recoverDepth: 1,
    oldEnabled: false,
    oldRecoverDepth: null,
  });
  pileUp(bus, 'k', 4);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 4);
  assert.equal(mitigations[0].recoverDepth, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('homomorphic revisions are no-ops with no audit; only a real change audits', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: true,
    },
  });
  // Already enabled with the defaults: `true`, `{}`, and the same
  // explicit depth are all homomorphic.
  bus.setKeyHotspotAutoMitigate(sub.id, true);
  bus.setKeyHotspotAutoMitigate(sub.id, {});
  bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: 2 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' }).length,
    0,
    'homomorphic enables must not audit',
  );
  // A real change audits once; restating it does not audit again.
  bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: 1 });
  bus.setKeyHotspotAutoMitigate(sub.id, { recoverDepth: 1 });
  let audit = bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    enabled: true,
    recoverDepth: 1,
    oldEnabled: true,
    oldRecoverDepth: 2,
  });
  // Disabling audits once; restating the disable does not.
  bus.setKeyHotspotAutoMitigate(sub.id, false);
  bus.setKeyHotspotAutoMitigate(sub.id, null);
  bus.setKeyHotspotAutoMitigate(sub.id, false);
  audit = bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    enabled: false,
    recoverDepth: null,
    oldEnabled: true,
    oldRecoverDepth: 1,
  });
  sub.unsubscribe();
});

test('disabling an engaged mitigation clears the latch and releases immediately; stats never go stale', async () => {
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
  pileUp(bus, 'k', 2); // depth 2 -> mitigation engages
  assert.equal(mitigations.length, 1);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.mitigated, true);
  bus.setKeyHotspotAutoMitigate(sub.id, false);
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_auto_mitigate_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    enabled: false,
    recoverDepth: null,
    oldEnabled: true,
    oldRecoverDepth: 2,
  });
  // The latch is cleared at once: the stream reports unmitigated
  // while its buffer is untouched.
  const hot = bus.getStats().hotKeys.find((h) => h.key === 'k');
  assert.ok(hot, 'the stream must still be reported');
  assert.equal(hot.bufferedDepth, 2);
  assert.equal(hot.mitigated, false);
  // The next out-of-order arrival buffers instead of being shed.
  bus.publish('d', 'probe', { key: 'k' });
  assert.equal(bus.getStats().hotKeyShedMessages, 0);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.bufferedDepth, 3);
  await drain(clock, bus);
  assert.deepEqual(received, ['trigger', 'delayed', 'live0', 'live1', 'probe']);
  sub.unsubscribe();
});

test('disabling keeps the threshold and the alert callback: re-piling still alerts but never mitigates', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const alerts: KeyHotspotEvent[] = [];
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: {
      thresholdDepth: 2,
      onKeyHotspot: (e) => alerts.push(e),
      autoMitigate: { onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  pileUp(bus, 'k', 2);
  assert.equal(alerts.length, 1);
  assert.equal(mitigations.length, 1);
  bus.setKeyHotspotAutoMitigate(sub.id, null);
  await drain(clock, bus);
  assert.deepEqual(received, ['trigger', 'delayed', 'live0', 'live1']);
  // Re-piling past the same threshold alerts again (threshold and
  // callback untouched) but engages no mitigation and sheds nothing.
  pileUp(bus, 'k', 2);
  assert.equal(alerts.length, 2);
  assert.equal(alerts[1].thresholdDepth, 2);
  assert.equal(mitigations.length, 1);
  assert.equal(bus.getStats().hotKeyShedMessages, 0);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.mitigated, false);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('enabling never touches the alert latch: an already-alerted key mitigates on the next evaluation', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const alerts: KeyHotspotEvent[] = [];
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      onKeyHotspot: (e) => alerts.push(e),
    },
  });
  pileUp(bus, 'k', 2); // detection-only: alerts once, no mitigation
  assert.equal(alerts.length, 1);
  assert.equal(mitigations.length, 0);
  // Enable with a callback (no explicit recoverDepth, so it defaults
  // to the live threshold) — the alert latch must stay set.
  bus.setKeyHotspotAutoMitigate(sub.id, { onHotKeyMitigated: (e) => mitigations.push(e) });
  // The next keyed-delivery evaluation (depth already at threshold)
  // engages mitigation without re-firing the alert.
  bus.publish('b', 'live2', { key: 'k' });
  assert.equal(alerts.length, 1, 'enabling must not re-fire the alert');
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].recoverDepth, 2);
  // And the stream now sheds: the following out-of-order arrival
  // never buffers.
  bus.publish('b', 'probe', { key: 'k' });
  assert.equal(bus.getStats().hotKeyShedMessages, 1);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.bufferedDepth, 3);
  await drain(clock, bus);
  sub.unsubscribe();
});
