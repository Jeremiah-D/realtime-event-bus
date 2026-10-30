import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { KeyHotspotEvent, HotKeyMitigatedEvent } from '../src/keyhotspot.ts';

/**
 * Key-hotspot threshold revision (EB-289): `setKeyHotspotThreshold`
 * retunes a monitored subscriber's alert threshold without
 * re-subscribing — only `keyHotspot.thresholdDepth` moves, the
 * `onKeyHotspot` callback and the `autoMitigate` configuration
 * (including the subscribe-time `recoverDepth` snapshot) are
 * untouched, and the new threshold governs from the next
 * keyed-delivery evaluation.
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

test('setKeyHotspotThreshold rejects unknown, empty, unmonitored, and invalid revisions fail-closed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 2, onKeyHotspot: (e) => events.push(e) },
  });
  const plain = bus.subscribe('t2', () => {});
  for (const bad of [
    () => bus.setKeyHotspotThreshold('no-such-sub', 2),
    () => bus.setKeyHotspotThreshold('', 2),
    () => bus.setKeyHotspotThreshold(plain.id, 2),
    () => bus.setKeyHotspotThreshold(sub.id, 0),
    () => bus.setKeyHotspotThreshold(sub.id, -1),
    () => bus.setKeyHotspotThreshold(sub.id, 2.5),
    () => bus.setKeyHotspotThreshold(sub.id, Number.NaN),
    () => bus.setKeyHotspotThreshold(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setKeyHotspotThreshold(sub.id, '2' as never),
    () => bus.setKeyHotspotThreshold(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_threshold_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live threshold is untouched: the old threshold still alerts.
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdDepth, 2);
  assert.equal(events[0].bufferedDepth, 2);
  await drain(clock, bus);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('lowering the threshold below the live buffer depth alerts on the next evaluation', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 5, onKeyHotspot: (e) => events.push(e) },
  });
  pileUp(bus, 'k', 3); // depth 3 < 5: no alert yet
  assert.equal(events.length, 0);
  bus.setKeyHotspotThreshold(sub.id, 3);
  const audit = bus.getSubscriptionAudit({ action: 'key_hotspot_threshold_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, '*');
  assert.deepEqual(audit[0].detail, { thresholdDepth: 3, oldThresholdDepth: 5 });
  // The revision itself does not evaluate; the next keyed delivery
  // does — the live depth (now 4) already exceeds the new threshold.
  bus.publish('b', 'live3', { key: 'k' });
  assert.equal(events.length, 1);
  assert.equal(events[0].bufferedDepth, 4);
  assert.equal(events[0].thresholdDepth, 3);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('raising the threshold does not clear an already-triggered latch', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: { thresholdDepth: 2, onKeyHotspot: (e) => events.push(e) },
  });
  pileUp(bus, 'k', 2); // depth 2 -> alert fires, latch set
  assert.equal(events.length, 1);
  bus.setKeyHotspotThreshold(sub.id, 5);
  bus.publish('b', 'live2', { key: 'k' });
  bus.publish('c', 'live3', { key: 'k' }); // depth 4: latch held, no second alert
  assert.equal(events.length, 1);
  await drain(clock, bus); // buffer drains below the threshold -> latch re-arms
  assert.deepEqual(received, ['trigger', 'delayed', 'live0', 'live1', 'live2', 'live3']);
  // The second excursion is judged by the new threshold.
  pileUp(bus, 'k', 4); // depth 4 < 5: still silent
  assert.equal(events.length, 1);
  bus.publish('b', 'live4', { key: 'k' }); // depth 5 -> alerts at the new threshold
  assert.equal(events.length, 2);
  assert.equal(events[1].thresholdDepth, 5);
  assert.equal(events[1].bufferedDepth, 5);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('lowering the threshold does not re-alert while the latch is held', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 2, onKeyHotspot: (e) => events.push(e) },
  });
  pileUp(bus, 'k', 2); // depth 2 -> alert fires, latch set
  assert.equal(events.length, 1);
  bus.setKeyHotspotThreshold(sub.id, 1);
  bus.publish('b', 'live2', { key: 'k' }); // depth 3 >= 1, but the latch is held
  assert.equal(events.length, 1);
  await drain(clock, bus); // buffer drains below the threshold -> latch re-arms
  // The next excursion alerts at the new threshold at once.
  pileUp(bus, 'k', 1); // depth 1 >= 1
  assert.equal(events.length, 2);
  assert.equal(events[1].thresholdDepth, 1);
  assert.equal(events[1].bufferedDepth, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('widening the threshold delays the alert until the new threshold is reached', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 2, onKeyHotspot: (e) => events.push(e) },
  });
  pileUp(bus, 'k', 1); // depth 1 < 2: silent
  assert.equal(events.length, 0);
  bus.setKeyHotspotThreshold(sub.id, 4);
  bus.publish('b', 'live1', { key: 'k' }); // depth 2: would have alerted at the old threshold
  assert.equal(events.length, 0);
  bus.publish('c', 'live2', { key: 'k' }); // depth 3: still below the new threshold
  assert.equal(events.length, 0);
  bus.publish('d', 'live3', { key: 'k' }); // depth 4 -> alerts at the new threshold
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdDepth, 4);
  assert.equal(events[0].bufferedDepth, 4);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('re-affirming the current threshold is a no-op with no audit', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 2, onKeyHotspot: (e) => events.push(e) },
  });
  bus.setKeyHotspotThreshold(sub.id, 2);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'key_hotspot_threshold_revised' }).length,
    0,
    'a same-threshold revision must not audit',
  );
  // The threshold is unchanged: the old threshold still alerts.
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdDepth, 2);
  await drain(clock, bus);
  sub.unsubscribe();
});

test('the subscribe-time recoverDepth snapshot is not recomputed from the revised threshold', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const sub = bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: { onHotKeyMitigated: (e) => mitigations.push(e) },
    },
  });
  pileUp(bus, 'k', 2); // depth 2 -> mitigation engages, recoverDepth mirrors the old threshold
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 2);
  assert.equal(mitigations[0].recoverDepth, 2);
  await drain(clock, bus); // buffer drains below recoverDepth -> mitigation releases
  assert.equal(bus.getStats().hotKeys.length, 0);
  // Lower the alert threshold: the recovery depth stays snapshotted.
  bus.setKeyHotspotThreshold(sub.id, 1);
  pileUp(bus, 'k', 1); // depth 1 -> mitigation engages at the new threshold
  assert.equal(mitigations.length, 2);
  assert.equal(mitigations[1].thresholdDepth, 1);
  assert.equal(mitigations[1].recoverDepth, 2, 'recoverDepth must stay at its subscribe-time snapshot');
  await drain(clock, bus);
  sub.unsubscribe();
});

test('getStats().hotKeys reports the revised threshold live', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('*', () => {}, { keyHotspot: { thresholdDepth: 2 } });
  pileUp(bus, 'k', 1);
  assert.equal(bus.getStats().hotKeys[0].thresholdDepth, 2);
  bus.setKeyHotspotThreshold(sub.id, 7);
  assert.equal(bus.getStats().hotKeys[0].thresholdDepth, 7);
  assert.equal(bus.getStats().hotKeys[0].bufferedDepth, 1);
  await drain(clock, bus);
  sub.unsubscribe();
});
