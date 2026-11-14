import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BarrierSubscribeOptions } from '../src/bus.ts';

/**
 * Bus-level default barrier-delivery hold bound (EB-504):
 * `setDefaultBarrier` installs the `maxHeldMessages` new opt-in
 * `barrier` subscribers fall back to when they omit the bound — the
 * priority chain is subscriber-explicit > bus default > built-in
 * 1000 — and the default never enables barrier delivery: a subscriber
 * that does not opt into `barrier` never sees barrier control
 * messages and is untouched by it.
 *
 * Barrier assertions follow `barrier.test.ts`: a barrier publish arms
 * the gate and every message arriving after it is held pre-queue
 * until the barrier is handed to the handler, then released at once;
 * overflowing the hold bound fails open (everything held is released
 * at once, the barrier is disarmed, the cut is abandoned), counted in
 * `getStats().barrier.overflowedHolds`.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('EB-504: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultBarrier(), undefined);
  bus.setDefaultBarrier(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_barrier_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-504: `true` opt-in takes the default bound — fail-open at the default, counted', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.setDefaultBarrier({ maxHeldMessages: 3 });
  bus.subscribe('t', (m) => received.push(m.payload), { barrier: true });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  assert.equal(bus.getStats().barrier.heldMessages, 3);
  assert.equal(bus.getStats().barrier.overflowedHolds, 0);
  // The fourth post-cut arrival overflows the default bound of 3:
  // the gate fails open — everything held is released at once, the
  // barrier is disarmed, and the cut is abandoned.
  bus.publish('t', 'm4');
  assert.equal(bus.getStats().barrier.heldMessages, 0);
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  assert.equal(bus.getStats().barrier.releasedMessages, 3);
  await flush();
  assert.deepEqual(received, ['cut', 'm1', 'm2', 'm3', 'm4']);
});

test('EB-504: a default bound above the built-in 1000 holds past 1000 without failing open', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.setDefaultBarrier({ maxHeldMessages: 1002 });
  // A roomy queue: the fail-open release re-injects ~1000 messages at
  // once, and the default 100-deep queue would backpressure-drop
  // most of them before the handler sees them.
  bus.subscribe('t', (m) => received.push(m.payload), { barrier: true, queueSize: 2000 });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  for (let i = 1; i <= 1001; i += 1) {
    bus.publish('t', `m${i}`);
  }
  // 1001 held — past the built-in 1000, yet no fail-open: the
  // default bound governs, not the built-in.
  assert.equal(bus.getStats().barrier.heldMessages, 1001);
  assert.equal(bus.getStats().barrier.overflowedHolds, 0);
  bus.publish('t', 'm1002');
  assert.equal(bus.getStats().barrier.heldMessages, 1002);
  assert.equal(bus.getStats().barrier.overflowedHolds, 0);
  // The 1003rd post-cut arrival overflows the default bound of 1002.
  bus.publish('t', 'm1003');
  assert.equal(bus.getStats().barrier.heldMessages, 0);
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  assert.equal(bus.getStats().barrier.releasedMessages, 1002);
  await flush();
  assert.equal(received.length, 1004);
  assert.equal(received[0], 'cut');
  assert.equal(received[1], 'm1');
  assert.equal(received[1003], 'm1003');
});

test('EB-504: an explicit maxHeldMessages always wins over the bus default', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.setDefaultBarrier({ maxHeldMessages: 2 });
  bus.subscribe('t', (m) => received.push(m.payload), { barrier: { maxHeldMessages: 5 } });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  for (let i = 1; i <= 5; i += 1) {
    bus.publish('t', `m${i}`);
  }
  // Under the default bound of 2, m3 would already have failed the
  // gate open; the explicit 5 holds all five.
  assert.equal(bus.getStats().barrier.heldMessages, 5);
  assert.equal(bus.getStats().barrier.overflowedHolds, 0);
  bus.publish('t', 'm6');
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  assert.equal(bus.getStats().barrier.releasedMessages, 5);
  await flush();
  assert.deepEqual(received, ['cut', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6']);
});

test('EB-504: `{}` opts in and takes the whole default', async () => {
  const bus = new EventBus();
  bus.setDefaultBarrier({ maxHeldMessages: 2 });
  bus.subscribe('t', () => {}, { barrier: {} });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  bus.publish('t', 'm3');
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
});

test('EB-504: with no default, `true` still takes the built-in 1000', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  // A roomy queue: the fail-open release re-injects ~1000 messages
  // at once, and the default 100-deep queue would backpressure-drop
  // most of them before the handler sees them.
  bus.subscribe('t', (m) => received.push(m.payload), { barrier: true, queueSize: 2000 });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  for (let i = 1; i <= 1001; i += 1) {
    bus.publish('t', `m${i}`);
  }
  // The 1001st post-cut arrival overflows the built-in 1000.
  assert.equal(bus.getStats().barrier.heldMessages, 0);
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  assert.equal(bus.getStats().barrier.releasedMessages, 1000);
  await flush();
  assert.equal(received.length, 1002);
  assert.equal(received[0], 'cut');
  assert.equal(received[1], 'm1');
  assert.equal(received[1001], 'm1001');
});

test('EB-504: clearing with null returns new opt-in subscribers to the built-in 1000', async () => {
  const bus = new EventBus();
  bus.setDefaultBarrier({ maxHeldMessages: 2 });
  bus.setDefaultBarrier(null);
  assert.equal(bus.getDefaultBarrier(), undefined);
  bus.subscribe('t', () => {}, { barrier: true, queueSize: 2000 });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  for (let i = 1; i <= 1001; i += 1) {
    bus.publish('t', `m${i}`);
  }
  assert.equal(bus.getStats().barrier.heldMessages, 0);
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  const audit = bus.getSubscriptionAudit({ action: 'default_barrier_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { maxHeldMessages: 2 },
    barrier: null,
  });
});

test('EB-504: the default never enables barrier delivery for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultBarrier({ maxHeldMessages: 2 });
  const plain: unknown[] = [];
  const declined: unknown[] = [];
  bus.subscribe('t', (m) => plain.push(m.payload));
  bus.subscribe('t', (m) => declined.push(m.payload), { barrier: false });
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  await flush();
  // Neither subscriber sees the barrier control message, and nothing
  // is held: no opt-in, no barrier gate.
  assert.deepEqual(plain, ['m1', 'm2']);
  assert.deepEqual(declined, ['m1', 'm2']);
  assert.equal(bus.getStats().barrier.heldMessages, 0);
  assert.equal(bus.getStats().barrier.overflowedHolds, 0);
});

test('EB-504: revising the bus default never moves an already-subscribed bound, held buffer, or epoch', async () => {
  const bus = new EventBus();
  bus.setDefaultBarrier({ maxHeldMessages: 2 });
  const early: unknown[] = [];
  const earlySub = bus.subscribe('t', (m) => early.push(m.payload), { barrier: true });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  bus.setDefaultBarrier({ maxHeldMessages: 100 });
  // The early subscriber keeps the bound it was baked with: the
  // next arrival fails open under 2, not 100.
  bus.publish('t', 'm3');
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  assert.equal(bus.getStats().barrier.releasedMessages, 2);
  await flush();
  assert.deepEqual(early, ['cut', 'm1', 'm2', 'm3']);
  // A subscriber joining after the revision takes the new default.
  bus.subscribe('u', () => {}, { barrier: true });
  bus.publish('u', 'cut', { barrier: { epoch: 1 } });
  for (let i = 1; i <= 3; i += 1) {
    bus.publish('u', `d${i}`);
  }
  assert.equal(bus.getStats().barrier.heldMessages, 3);
  assert.equal(bus.getStats().barrier.overflowedHolds, 1, 'no new fail-open under the new bound');
  // The per-subscriber revision still works on a default-baked
  // subscriber without touching the bus default.
  bus.setBarrier(earlySub.id, 5);
  assert.deepEqual(bus.getDefaultBarrier(), { maxHeldMessages: 100 });
  earlySub.unsubscribe();
});

test('EB-504: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultBarrier({ maxHeldMessages: 2 });
  const bad = [
    0,
    -1,
    1.5,
    'x',
    [],
    true,
    { maxHeldMessages: 0 },
    { maxHeldMessages: -1 },
    { maxHeldMessages: 2.5 },
    { maxHeldMessages: '2' },
    { maxHeldMessages: Number.NaN },
    { maxHeldMessages: Number.POSITIVE_INFINITY },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultBarrier(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultBarrier(), { maxHeldMessages: 2 });
  }
  assert.throws(() => bus.setDefaultBarrier({ maxHeldMessages: 3 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultBarrier(), { maxHeldMessages: 2 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_barrier_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-504: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultBarrier(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_barrier_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultBarrier({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_barrier_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultBarrier(), undefined);
  const opts: BarrierSubscribeOptions = { maxHeldMessages: 2 };
  bus.setDefaultBarrier(opts);
  opts.maxHeldMessages = 999;
  assert.deepEqual(bus.getDefaultBarrier(), { maxHeldMessages: 2 }, 'the setter stores a defensive copy');
  bus.setDefaultBarrier({ maxHeldMessages: 2 });
  const audit = bus.getSubscriptionAudit({ action: 'default_barrier_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    barrier: { maxHeldMessages: 2 },
  });
  const held = bus.getDefaultBarrier();
  assert.ok(held !== undefined);
  held.maxHeldMessages = 999;
  assert.deepEqual(
    bus.getDefaultBarrier(),
    { maxHeldMessages: 2 },
    'the getter returns a copy',
  );
  // A revision moves the live value and audits both sides.
  bus.setDefaultBarrier({ maxHeldMessages: 5 }, 'ops');
  assert.deepEqual(bus.getDefaultBarrier(), { maxHeldMessages: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'default_barrier_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { maxHeldMessages: 2 },
    barrier: { maxHeldMessages: 5 },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultBarrier(null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'default_barrier_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { maxHeldMessages: 5 },
    barrier: null,
  });
});

test('EB-504: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultBarrier({ maxHeldMessages: 2 });
  const snapshot = bus.exportBusConfig();
  assert.ok(!('defaultBarrier' in snapshot), 'no defaultBarrier key');
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultBarrier'),
    'the field name appears nowhere in the exported snapshot',
  );
  assert.deepEqual(bus.getDefaultBarrier(), { maxHeldMessages: 2 });
});
