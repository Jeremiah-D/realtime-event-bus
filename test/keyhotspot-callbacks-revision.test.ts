import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { HotKeyMitigatedEvent, KeyHotspotEvent } from '../src/keyhotspot.ts';

/**
 * Key-hotspot alert callback revision (EB-433): `setKeyHotspotCallbacks`
 * swaps a monitored subscriber's `onKeyHotspot` and/or
 * `onHotKeyMitigated` in place — only the callbacks move, the
 * `thresholdDepth`, the `recoverDepth`, and the `alertedKeys` /
 * `mitigatedKeys` latches are untouched, and the new callbacks govern
 * from the next keyed-delivery evaluation. Changing the alert recipient
 * no longer means re-subscribing (and losing the queue, the lease, and
 * the offset state).
 *
 * Determinism trick (as in `keyhotspot.test.ts`): a delayed
 * predecessor (`publishDelayed`, due later) holds the key's
 * keySeq 1, so the live publishes that follow pile onto the
 * reorder buffer synchronously — each one is a keyed-delivery
 * evaluation, exactly like the production path.
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

test('revising onKeyHotspot routes the next excursion to the new callback', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const oldAlerts: KeyHotspotEvent[] = [];
  const newAlerts: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 2, onKeyHotspot: (e) => oldAlerts.push(e) },
  });
  pileUp(bus, 'k', 2); // depth 2 -> old callback fires
  assert.equal(oldAlerts.length, 1);
  await drain(clock, bus);

  bus.setKeyHotspotCallbacks(sub.id, { onKeyHotspot: (e) => newAlerts.push(e) });
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, '*');
  assert.deepEqual(audit[0].detail, {
    onKeyHotspot: 'set',
    oldOnKeyHotspot: 'set',
    onHotKeyMitigated: 'unchanged',
    oldOnHotKeyMitigated: 'absent',
  });

  pileUp(bus, 'k', 2); // next excursion: the new callback fires
  assert.equal(newAlerts.length, 1);
  assert.equal(newAlerts[0].thresholdDepth, 2);
  assert.equal(newAlerts[0].bufferedDepth, 2);
  assert.equal(oldAlerts.length, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('revising onHotKeyMitigated routes the next engagement to the new callback', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const oldMitigations: HotKeyMitigatedEvent[] = [];
  const newMitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: { recoverDepth: 1, onHotKeyMitigated: (e) => oldMitigations.push(e) },
    },
  });
  pileUp(bus, 'k', 2); // depth 2 -> mitigation engages, old callback fires
  assert.equal(oldMitigations.length, 1);
  assert.equal(oldMitigations[0].recoverDepth, 1);
  await drain(clock, bus);

  bus.setKeyHotspotCallbacks(sub.id, {
    onHotKeyMitigated: (e) => newMitigations.push(e),
  });
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onKeyHotspot: 'unchanged',
    oldOnKeyHotspot: 'absent',
    onHotKeyMitigated: 'set',
    oldOnHotKeyMitigated: 'set',
  });

  pileUp(bus, 'k', 2); // next engagement: the new callback fires
  assert.equal(newMitigations.length, 1);
  assert.equal(newMitigations[0].thresholdDepth, 2);
  assert.equal(newMitigations[0].recoverDepth, 1);
  assert.equal(oldMitigations.length, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('clearing onKeyHotspot with null silences alerts while stats keep reporting', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const alerts: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 2, onKeyHotspot: (e) => alerts.push(e) },
  });
  pileUp(bus, 'k', 2);
  assert.equal(alerts.length, 1);
  await drain(clock, bus);

  bus.setKeyHotspotCallbacks(sub.id, { onKeyHotspot: null });
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onKeyHotspot: 'cleared',
    oldOnKeyHotspot: 'set',
    onHotKeyMitigated: 'unchanged',
    oldOnHotKeyMitigated: 'absent',
  });

  pileUp(bus, 'k', 2); // same excursion shape, no alert anymore
  assert.equal(alerts.length, 1);
  // Stats-only alerting: the stream is still reported with its live
  // depth and threshold.
  const hot = bus.getStats().hotKeys.find((h) => h.key === 'k');
  assert.ok(hot, 'the stream must still be reported');
  assert.equal(hot.bufferedDepth, 2);
  assert.equal(hot.thresholdDepth, 2);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('clearing onHotKeyMitigated keeps mitigation engaging, silently', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: { recoverDepth: 1, onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  pileUp(bus, 'k', 2);
  assert.equal(mitigations.length, 1);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.mitigated, true);
  await drain(clock, bus);

  bus.setKeyHotspotCallbacks(sub.id, { onHotKeyMitigated: null });
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onKeyHotspot: 'unchanged',
    oldOnKeyHotspot: 'absent',
    onHotKeyMitigated: 'cleared',
    oldOnHotKeyMitigated: 'set',
  });

  pileUp(bus, 'k', 2); // mitigation engages again, but silently
  assert.equal(mitigations.length, 1);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.mitigated, true);
  // The engaged stream still sheds: the next out-of-order arrival
  // never buffers.
  bus.publish('b', 'probe', { key: 'k' });
  assert.equal(bus.getStats().hotKeyShedMessages, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('setKeyHotspotCallbacks rejects unknown, empty, unmonitored, detection-only, and invalid revisions fail-closed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const alerts: KeyHotspotEvent[] = [];
  const mitigations: HotKeyMitigatedEvent[] = [];
  const detectionAlerts: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      onKeyHotspot: (e) => alerts.push(e),
      autoMitigate: { recoverDepth: 1, onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  const plain = bus.subscribe('t2', () => {});
  const detectionOnly = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 2, onKeyHotspot: (e) => detectionAlerts.push(e) },
  });
  const newAlert = () => {};
  const newMitigation = () => {};
  for (const bad of [
    () => bus.setKeyHotspotCallbacks('no-such-sub', { onKeyHotspot: newAlert }),
    () => bus.setKeyHotspotCallbacks('', { onKeyHotspot: newAlert }),
    () => bus.setKeyHotspotCallbacks(plain.id, { onKeyHotspot: newAlert }),
    // A detection-only subscription has no mitigation callback to swap.
    () => bus.setKeyHotspotCallbacks(detectionOnly.id, { onHotKeyMitigated: newMitigation }),
    // A mixed patch is refused whole: the alert half must not move
    // either when the mitigation half cannot be revised.
    () =>
      bus.setKeyHotspotCallbacks(detectionOnly.id, {
        onKeyHotspot: newAlert,
        onHotKeyMitigated: newMitigation,
      }),
    () => bus.setKeyHotspotCallbacks(sub.id, {}),
    // @ts-expect-error: intentional bad input
    () => bus.setKeyHotspotCallbacks(sub.id, null),
    // @ts-expect-error: intentional bad input
    () => bus.setKeyHotspotCallbacks(sub.id, 'nope'),
    () => bus.setKeyHotspotCallbacks(sub.id, { onKeyHotspot: 'x' as never }),
    () => bus.setKeyHotspotCallbacks(sub.id, { onKeyHotspot: 42 as never }),
    () => bus.setKeyHotspotCallbacks(sub.id, { onHotKeyMitigated: 'x' as never }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_callbacks_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live callbacks are untouched: the old ones still fire.
  pileUp(bus, 'k', 2);
  assert.equal(alerts.length, 1);
  assert.equal(mitigations.length, 1);
  assert.equal(detectionAlerts.length, 1);
  await drain(clock, bus);
  sub.unsubscribe();
  plain.unsubscribe();
  detectionOnly.unsubscribe();
});

test('a same-value revision is a no-op with no audit', () => {
  const bus = new EventBus();
  const onKeyHotspot = () => {};
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      onKeyHotspot,
      autoMitigate: { recoverDepth: 1 },
    },
  });
  // Same alert callback reference: nothing moves.
  bus.setKeyHotspotCallbacks(sub.id, { onKeyHotspot });
  // Clearing a mitigation callback that was never set: nothing moves.
  bus.setKeyHotspotCallbacks(sub.id, { onHotKeyMitigated: null });
  // Clearing an alert callback that was never set: nothing moves.
  const statsOnly = bus.subscribe('t2', () => {}, { keyHotspot: { thresholdDepth: 2 } });
  bus.setKeyHotspotCallbacks(statsOnly.id, { onKeyHotspot: null });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_callbacks_revised' }).length,
    0,
    'same-value revisions must not audit',
  );
  sub.unsubscribe();
  statsOnly.unsubscribe();
});

test('revision leaves the alert latch, the threshold, and the mitigation config untouched', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const oldAlerts: KeyHotspotEvent[] = [];
  const newAlerts: KeyHotspotEvent[] = [];
  const oldMitigations: HotKeyMitigatedEvent[] = [];
  const newMitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      onKeyHotspot: (e) => oldAlerts.push(e),
      autoMitigate: { recoverDepth: 1, onHotKeyMitigated: (e) => oldMitigations.push(e) },
    },
  });
  pileUp(bus, 'k', 2); // alert fires, latch set; mitigation engages
  assert.equal(oldAlerts.length, 1);
  assert.equal(oldMitigations.length, 1);

  bus.setKeyHotspotCallbacks(sub.id, {
    onKeyHotspot: (e) => newAlerts.push(e),
    onHotKeyMitigated: (e) => newMitigations.push(e),
  });
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onKeyHotspot: 'set',
    oldOnKeyHotspot: 'set',
    onHotKeyMitigated: 'set',
    oldOnHotKeyMitigated: 'set',
  });

  // The latch is held: the next evaluation does not re-fire either
  // callback, old or new.
  bus.publish('b', 'live2', { key: 'k' });
  assert.equal(oldAlerts.length, 1);
  assert.equal(newAlerts.length, 0);
  assert.equal(oldMitigations.length, 1);
  assert.equal(newMitigations.length, 0);
  await drain(clock, bus); // latch re-arms
  // The next excursion is judged by the live threshold and recovery
  // depth, and fires the NEW callbacks.
  pileUp(bus, 'k', 2);
  assert.equal(newAlerts.length, 1);
  assert.equal(newAlerts[0].thresholdDepth, 2);
  assert.equal(newMitigations.length, 1);
  assert.equal(newMitigations[0].recoverDepth, 1);
  assert.equal(oldAlerts.length, 1);
  assert.equal(oldMitigations.length, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('revising both callbacks at once audits both presences and both fire', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const alerts: KeyHotspotEvent[] = [];
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 2, autoMitigate: { recoverDepth: 1 } },
  });
  bus.setKeyHotspotCallbacks(sub.id, {
    onKeyHotspot: (e) => alerts.push(e),
    onHotKeyMitigated: (e) => mitigations.push(e),
  });
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onKeyHotspot: 'set',
    oldOnKeyHotspot: 'absent',
    onHotKeyMitigated: 'set',
    oldOnHotKeyMitigated: 'absent',
  });
  pileUp(bus, 'k', 2);
  assert.equal(alerts.length, 1);
  assert.equal(mitigations.length, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});
