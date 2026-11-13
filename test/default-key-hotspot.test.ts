import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type {
  HotKeyMitigatedEvent,
  KeyHotspotEvent,
  KeyHotspotOptions,
} from '../src/keyhotspot.ts';

/**
 * Bus-level default key-hotspot monitoring (EB-498):
 * `setDefaultKeyHotspot` installs the `thresholdDepth` /
 * `onKeyHotspot` / `autoMitigate` new opt-in `keyHotspot`
 * subscribers fall back to when they omit a field — the priority
 * chain is subscriber-explicit > bus default > built-in
 * (thresholdDepth 100, no callback, detection only) — and the
 * default never enables monitoring: a subscriber that does not opt
 * into `keyHotspot` is untouched by it.
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

test('EB-498: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultKeyHotspot(), undefined);
  bus.setDefaultKeyHotspot(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-498: keyHotspot: true takes the whole bus default (threshold + callback)', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: (e) => events.push(e),
  });
  const sub = bus.subscribe('*', () => {}, { keyHotspot: true });
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 1);
  assert.deepEqual(events[0], {
    subscriberId: sub.id,
    pattern: '*',
    key: 'k',
    bufferedDepth: 2,
    thresholdDepth: 2,
    at: clock.nowMs,
  });
  const hot = bus.getStats().hotKeys.find((h) => h.key === 'k');
  assert.ok(hot !== undefined);
  assert.equal(hot.subscriberId, sub.id);
  assert.equal(hot.thresholdDepth, 2);
  assert.equal(hot.bufferedDepth, 2);
  assert.equal(hot.mitigated, false);
  await drain(clock, bus);
});

test('EB-498: an explicit thresholdDepth wins over the bus default', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: (e) => events.push(e),
  });
  bus.subscribe('*', () => {}, { keyHotspot: { thresholdDepth: 5 } });
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 0, 'depth 2 is under the explicit 5, not the default 2');
  bus.publish('b', 'live2', { key: 'k' });
  bus.publish('b', 'live3', { key: 'k' });
  bus.publish('b', 'live4', { key: 'k' });
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdDepth, 5);
  assert.equal(events[0].bufferedDepth, 5);
});

test('EB-498: an explicit onKeyHotspot wins over the default callback', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const defaultEvents: KeyHotspotEvent[] = [];
  const ownEvents: KeyHotspotEvent[] = [];
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: (e) => defaultEvents.push(e),
  });
  bus.subscribe('*', () => {}, { keyHotspot: { onKeyHotspot: (e) => ownEvents.push(e) } });
  pileUp(bus, 'k', 2);
  // The default threshold (2) still applies — only the callback is
  // overridden.
  assert.equal(ownEvents.length, 1);
  assert.equal(ownEvents[0].thresholdDepth, 2);
  assert.equal(defaultEvents.length, 0);
});

test('EB-498: keyHotspot: {} opts in and takes the whole bus default too', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: (e) => events.push(e),
  });
  bus.subscribe('*', () => {}, { keyHotspot: {} });
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdDepth, 2);
});

test('EB-498: a default autoMitigate: true makes a new subscriber hot key auto-mitigate', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  const received: unknown[] = [];
  bus.setDefaultKeyHotspot({ thresholdDepth: 2, autoMitigate: true });
  // The subscriber arms only the mitigation callback; the enablement
  // and the recoverDepth (the merged threshold, 2) come from the
  // default's `autoMitigate: true`.
  bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: { autoMitigate: { onHotKeyMitigated: (e) => mitigations.push(e) } },
  });
  pileUp(bus, 'k', 2);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 2);
  assert.equal(mitigations[0].recoverDepth, 2);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.mitigated, true);
  bus.publish('b', 'probe', { key: 'k' });
  assert.equal(bus.getStats().hotKeyShedMessages, 1);
  await drain(clock, bus);
  assert.deepEqual(received, ['trigger', 'delayed', 'live0', 'live1']);
});

test('EB-498: a subscriber autoMitigate object merges field by field with the default object', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const mitigations: HotKeyMitigatedEvent[] = [];
  bus.setDefaultKeyHotspot({
    thresholdDepth: 3,
    autoMitigate: { recoverDepth: 2, onHotKeyMitigated: (e) => mitigations.push(e) },
  });
  // Explicit recoverDepth 1 wins; the default threshold (3) and the
  // default mitigation callback still apply.
  bus.subscribe('*', () => {}, { keyHotspot: { autoMitigate: { recoverDepth: 1 } } });
  pileUp(bus, 'k', 3);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].thresholdDepth, 3);
  assert.equal(mitigations[0].recoverDepth, 1);
});

test('EB-498: a default recoverDepth above a subscriber explicit threshold makes subscribe throw', () => {
  const bus = new EventBus();
  bus.setDefaultKeyHotspot({
    thresholdDepth: 10,
    autoMitigate: { recoverDepth: 8 },
  });
  assert.throws(
    () => bus.subscribe('*', () => {}, { keyHotspot: { thresholdDepth: 5 } }),
    RangeError,
    'merged recoverDepth 8 exceeds the explicit threshold 5',
  );
  // Taking the whole default is fine: 8 <= 10.
  const sub = bus.subscribe('*', () => {}, { keyHotspot: true });
  assert.ok(sub.id.length > 0);
  // An explicit recoverDepth below the tightened threshold is fine too.
  bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 5, autoMitigate: { recoverDepth: 3 } },
  });
});

test('EB-498: with no default, true still takes the built-ins 100 / detection only', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  bus.subscribe('*', () => {}, { keyHotspot: true });
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 0);
  const hot = bus.getStats().hotKeys.find((h) => h.key === 'k');
  assert.ok(hot !== undefined);
  assert.equal(hot.thresholdDepth, 100);
  assert.equal(hot.mitigated, false);
});

test('EB-498: clearing to null restores the built-ins for new subscribers', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const defaultEvents: KeyHotspotEvent[] = [];
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: (e) => defaultEvents.push(e),
  });
  bus.setDefaultKeyHotspot(null);
  assert.equal(bus.getDefaultKeyHotspot(), undefined);
  const audit = bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { thresholdDepth: 2, onKeyHotspot: 'set' },
    keyHotspot: null,
  });
  bus.subscribe('*', () => {}, { keyHotspot: true });
  pileUp(bus, 'k', 2);
  assert.equal(defaultEvents.length, 0);
  assert.equal(bus.getStats().hotKeys.find((h) => h.key === 'k')?.thresholdDepth, 100);
});

test('EB-498: omitted/false keyHotspot is untouched by the bus default', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: (e) => events.push(e),
  });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {}, { keyHotspot: false });
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 0);
  assert.deepEqual(bus.getStats().hotKeys, []);
});

test('EB-498: audit shape, defensive copies, and same-value no-op', () => {
  const bus = new EventBus();
  bus.setDefaultKeyHotspot({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultKeyHotspot(), undefined);
  const cb = () => {};
  const mcb = () => {};
  const opts: KeyHotspotOptions = {
    thresholdDepth: 2,
    onKeyHotspot: cb,
    autoMitigate: { recoverDepth: 1, onHotKeyMitigated: mcb },
  };
  bus.setDefaultKeyHotspot(opts);
  opts.thresholdDepth = 999;
  (opts.autoMitigate as { recoverDepth: number }).recoverDepth = 999;
  assert.deepEqual(
    bus.getDefaultKeyHotspot(),
    { thresholdDepth: 2, onKeyHotspot: cb, autoMitigate: { recoverDepth: 1, onHotKeyMitigated: mcb } },
    'the setter stores a defensive copy, including the nested autoMitigate object',
  );
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: cb,
    autoMitigate: { recoverDepth: 1, onHotKeyMitigated: mcb },
  });
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: cb,
    autoMitigate: { recoverDepth: 1, onHotKeyMitigated: mcb },
  });
  const audit = bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    keyHotspot: {
      thresholdDepth: 2,
      onKeyHotspot: 'set',
      autoMitigate: { recoverDepth: 1, onHotKeyMitigated: 'set' },
    },
  });
  assert.equal(audit[0].actor, 'system');
  const held = bus.getDefaultKeyHotspot();
  assert.ok(held !== undefined);
  held.thresholdDepth = 999;
  (held.autoMitigate as { recoverDepth: number }).recoverDepth = 999;
  assert.deepEqual(
    bus.getDefaultKeyHotspot(),
    { thresholdDepth: 2, onKeyHotspot: cb, autoMitigate: { recoverDepth: 1, onHotKeyMitigated: mcb } },
    'the getter returns a copy, including the nested autoMitigate object',
  );
  bus.setDefaultKeyHotspot({ thresholdDepth: 80 }, 'ops');
  assert.deepEqual(bus.getDefaultKeyHotspot(), { thresholdDepth: 80 });
  const revised = bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: {
      thresholdDepth: 2,
      onKeyHotspot: 'set',
      autoMitigate: { recoverDepth: 1, onHotKeyMitigated: 'set' },
    },
    keyHotspot: { thresholdDepth: 80 },
  });
  assert.equal(revised[1].actor, 'ops');
  // `autoMitigate: true` and `autoMitigate: {}` are homomorphic.
  bus.setDefaultKeyHotspot({ autoMitigate: true });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' }).length,
    3,
    'replacing the threshold-only default with an autoMitigate default audits once',
  );
  assert.deepEqual(
    bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' })[2].detail,
    {
      previous: { thresholdDepth: 80 },
      keyHotspot: { autoMitigate: { recoverDepth: null } },
    },
  );
  bus.setDefaultKeyHotspot({ autoMitigate: {} });
  bus.setDefaultKeyHotspot({ autoMitigate: true });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' }).length,
    3,
    'homomorphic autoMitigate restatements audit nothing',
  );
});

test('EB-498: invalid values throw and leave the live default untouched', () => {
  const bus = new EventBus();
  const cb = () => {};
  const mcb = () => {};
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: cb,
    autoMitigate: { recoverDepth: 1, onHotKeyMitigated: mcb },
  });
  const live = { thresholdDepth: 2, onKeyHotspot: cb, autoMitigate: { recoverDepth: 1, onHotKeyMitigated: mcb } };
  const rangeBad = [
    { thresholdDepth: 0 },
    { thresholdDepth: -1 },
    { thresholdDepth: 2.5 },
    { thresholdDepth: Number.NaN },
    { thresholdDepth: Number.POSITIVE_INFINITY },
    { thresholdDepth: '2' },
    { autoMitigate: { recoverDepth: -1 } },
    { autoMitigate: { recoverDepth: 2.5 } },
    { autoMitigate: { recoverDepth: Number.NaN } },
    { autoMitigate: { recoverDepth: Number.POSITIVE_INFINITY } },
    // Above the built-in threshold 100 when no threshold is given.
    { autoMitigate: { recoverDepth: 150 } },
    // Above the default's own threshold 2.
    { thresholdDepth: 2, autoMitigate: { recoverDepth: 3 } },
  ] as unknown[];
  for (const value of rangeBad) {
    assert.throws(
      () => bus.setDefaultKeyHotspot(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultKeyHotspot(), live);
  }
  // Non-object values keep the resolver's own error kind (TypeError),
  // as do non-function callbacks.
  const typeBad = [
    0,
    -1,
    '2',
    [],
    true,
    undefined,
    { onKeyHotspot: 'x' },
    { onKeyHotspot: 42 },
    { autoMitigate: { onHotKeyMitigated: 'x' } },
    { autoMitigate: 'yes' },
    { autoMitigate: 42 },
  ] as unknown[];
  for (const value of typeBad) {
    assert.throws(
      () => bus.setDefaultKeyHotspot(value as never),
      TypeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultKeyHotspot(), live);
  }
  assert.throws(() => bus.setDefaultKeyHotspot({ thresholdDepth: 70 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultKeyHotspot(), live);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_key_hotspot_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-498: revising the bus default never moves an already-subscribed threshold, recovery depth, or latch', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const mitigations: HotKeyMitigatedEvent[] = [];
  const received: unknown[] = [];
  bus.setDefaultKeyHotspot({
    thresholdDepth: 2,
    onKeyHotspot: (e) => events.push(e),
    autoMitigate: { recoverDepth: 1, onHotKeyMitigated: (e) => mitigations.push(e) },
  });
  bus.subscribe('*', (msg) => received.push(msg.payload), { keyHotspot: true });
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 1);
  assert.equal(mitigations.length, 1);
  assert.equal(mitigations[0].recoverDepth, 1);
  await drain(clock, bus);
  assert.deepEqual(received, ['trigger', 'delayed', 'live0', 'live1']);
  // Revise the default far above the baked values: the early
  // subscriber keeps alerting at 2 and mitigating with recoverDepth 1.
  bus.setDefaultKeyHotspot({ thresholdDepth: 50 });
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 2);
  assert.equal(events[1].thresholdDepth, 2);
  assert.equal(mitigations.length, 2);
  assert.equal(mitigations[1].thresholdDepth, 2);
  assert.equal(mitigations[1].recoverDepth, 1);
  await drain(clock, bus);
  // A subscriber joining after the revision takes the new default.
  // Same '*' pattern as the early subscriber, so it sees the same
  // pile-up; the two hotKeys entries are told apart by subscriberId.
  const late = bus.subscribe('*', () => {}, { keyHotspot: true });
  pileUp(bus, 'k2', 2);
  // The early subscriber still alerts at its baked threshold 2 on the
  // new key; the late subscriber sits at the new default 50 with no
  // callback, so no fourth event can come from it.
  assert.equal(events.length, 3);
  assert.equal(events[2].thresholdDepth, 2);
  const lateHot = bus.getStats().hotKeys.find((h) => h.key === 'k2' && h.subscriberId === late.id);
  assert.ok(lateHot !== undefined);
  assert.equal(lateHot.thresholdDepth, 50);
  assert.equal(lateHot.mitigated, false);
});

test('EB-498: a per-subscriber threshold revision works on a default-baked subscriber and never re-applies the default', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  bus.setDefaultKeyHotspot({ thresholdDepth: 2, onKeyHotspot: (e) => events.push(e) });
  const sub = bus.subscribe('*', () => {}, { keyHotspot: true });
  bus.setKeyHotspotThreshold(sub.id, 5);
  pileUp(bus, 'k', 2);
  assert.equal(events.length, 0, 'the per-subscriber revision to 5 governs, not the default 2');
  bus.publish('b', 'live2', { key: 'k' });
  bus.publish('b', 'live3', { key: 'k' });
  bus.publish('b', 'live4', { key: 'k' });
  assert.equal(events.length, 1);
  assert.equal(events[0].thresholdDepth, 5);
});
