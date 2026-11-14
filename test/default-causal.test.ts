import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type CausalSubscribeOptions } from '../src/bus.ts';

/**
 * Bus-level default causal-delivery buffer bound (EB-503):
 * `setDefaultCausal` installs the `maxBufferPerSource` new opt-in
 * `causal` subscribers fall back to when they omit the bound — the
 * priority chain is subscriber-explicit > bus default > built-in
 * 1000 — and the default never enables causal delivery: a subscriber
 * that does not opt into `causal` is untouched by it.
 *
 * Causal assertions follow `causal-revision.test.ts`: early arrivals
 * buffer per source until their dependency arrives, and overflowing
 * the bound drops the oldest buffered message (drop-oldest) and
 * advances the expectation past it, counted in
 * `getStats().causalBuffer.droppedMessages`.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('EB-503: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultCausal(), undefined);
  bus.setDefaultCausal(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_causal_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-503: `true` opt-in takes the default bound — drop-oldest at the default, counted', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.setDefaultCausal({ maxBufferPerSource: 3 });
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), { causal: true });
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  bus.publish('t', 'c3', { causal: { source: 's', clock: 3 } });
  assert.equal(bus.causalBufferDepth(sub.id), 3);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 0);
  // The fourth early arrival overflows the default bound of 3: the
  // oldest buffered message (c1) is dropped, the expectation advances
  // past it, and the rest release in clock order.
  bus.publish('t', 'c4', { causal: { source: 's', clock: 4 } });
  assert.equal(bus.causalBufferDepth(sub.id), 0);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  await flush();
  assert.deepEqual(received, ['c2', 'c3', 'c4']);
  sub.unsubscribe();
});

test('EB-503: a default bound above the built-in 1000 holds past 1000 without dropping', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.setDefaultCausal({ maxBufferPerSource: 1002 });
  // A roomy queue: the release cascade re-injects ~1000 messages at
  // once, and the default 100-deep queue would backpressure-drop
  // most of them before the handler sees them.
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: true,
    queueSize: 2000,
  });
  for (let clock = 1; clock <= 1001; clock += 1) {
    bus.publish('t', `c${clock}`, { causal: { source: 's', clock } });
  }
  // 1001 buffered — past the built-in 1000, yet nothing dropped:
  // the default bound governs, not the built-in.
  assert.equal(bus.causalBufferDepth(sub.id), 1001);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 0);
  bus.publish('t', 'c1002', { causal: { source: 's', clock: 1002 } });
  assert.equal(bus.causalBufferDepth(sub.id), 1002);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 0);
  // The 1003rd early arrival overflows the default bound of 1002:
  // the oldest (c1) is shed and the expectation advances past it,
  // releasing the rest in clock order.
  bus.publish('t', 'c1003', { causal: { source: 's', clock: 1003 } });
  assert.equal(bus.causalBufferDepth(sub.id), 0);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  await flush();
  // The whole released stream arrives in clock order, minus the shed c1.
  assert.equal(received.length, 1002);
  assert.equal(received[0], 'c2', 'c1 was shed by the default bound');
  assert.equal(received[1001], 'c1003');
  sub.unsubscribe();
});

test('EB-503: an explicit maxBufferPerSource always wins over the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultCausal({ maxBufferPerSource: 2 });
  const received: string[] = [];
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: { maxBufferPerSource: 5 },
  });
  for (let clock = 1; clock <= 5; clock += 1) {
    bus.publish('t', `c${clock}`, { causal: { source: 's', clock } });
  }
  // Under the default bound of 2, clock 3 would already have shed
  // clock 1; the explicit 5 holds all five.
  assert.equal(bus.causalBufferDepth(sub.id), 5);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 0);
  bus.publish('t', 'c6', { causal: { source: 's', clock: 6 } });
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  await flush();
  assert.deepEqual(received, ['c2', 'c3', 'c4', 'c5', 'c6']);
  sub.unsubscribe();
});

test('EB-503: `{}` opts in and takes the whole default', async () => {
  const bus = new EventBus();
  bus.setDefaultCausal({ maxBufferPerSource: 2 });
  const sub = bus.subscribe('*', () => {}, { causal: {} });
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  assert.equal(bus.causalBufferDepth(sub.id), 2);
  bus.publish('t', 'c3', { causal: { source: 's', clock: 3 } });
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  sub.unsubscribe();
});

test('EB-503: with no default, `true` still takes the built-in 1000', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  // A roomy queue: the release cascade re-injects ~1000 messages at
  // once, and the default 100-deep queue would backpressure-drop
  // most of them before the handler sees them.
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: true,
    queueSize: 2000,
  });
  for (let clock = 1; clock <= 1001; clock += 1) {
    bus.publish('t', `c${clock}`, { causal: { source: 's', clock } });
  }
  // The 1001st early arrival overflows the built-in 1000: the oldest
  // (c1) is shed and the expectation advances past it, releasing the
  // rest in clock order — the buffer drains to 0.
  assert.equal(bus.causalBufferDepth(sub.id), 0);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  await flush();
  assert.equal(received.length, 1000);
  assert.equal(received[0], 'c2');
  sub.unsubscribe();
});

test('EB-503: clearing with null returns new opt-in subscribers to the built-in 1000', async () => {
  const bus = new EventBus();
  bus.setDefaultCausal({ maxBufferPerSource: 2 });
  bus.setDefaultCausal(null);
  assert.equal(bus.getDefaultCausal(), undefined);
  const sub = bus.subscribe('*', () => {}, { causal: true });
  for (let clock = 1; clock <= 1001; clock += 1) {
    bus.publish('t', `c${clock}`, { causal: { source: 's', clock } });
  }
  assert.equal(bus.causalBufferDepth(sub.id), 0);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  const audit = bus.getSubscriptionAudit({ action: 'default_causal_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { maxBufferPerSource: 2 },
    causal: null,
  });
  sub.unsubscribe();
});

test('EB-503: the default never enables causal delivery for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultCausal({ maxBufferPerSource: 2 });
  const received: string[] = [];
  const plain = bus.subscribe('t', (m) => received.push(m.payload as string));
  const declined = bus.subscribe('t', (m) => received.push(m.payload as string), {
    causal: false,
  });
  // Out-of-order clocks arrive in arrival order for both: no gate.
  bus.publish('t', 'c0', { causal: { source: 's', clock: 0 } });
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  await flush();
  assert.deepEqual(received, ['c0', 'c2', 'c1', 'c0', 'c2', 'c1']);
  assert.equal(bus.causalBufferDepth(plain.id), 0);
  assert.equal(bus.causalBufferDepth(declined.id), 0);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 0);
  plain.unsubscribe();
  declined.unsubscribe();
});

test('EB-503: revising the bus default never moves an already-subscribed bound, buffer, or expectation', async () => {
  const bus = new EventBus();
  bus.setDefaultCausal({ maxBufferPerSource: 2 });
  const early: string[] = [];
  const earlySub = bus.subscribe('t', (m) => early.push(m.payload as string), { causal: true });
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  assert.equal(bus.causalBufferDepth(earlySub.id), 2);
  bus.setDefaultCausal({ maxBufferPerSource: 100 });
  // The early subscriber keeps the bound it was baked with: the
  // next arrival sheds under 2, not 100.
  bus.publish('t', 'c3', { causal: { source: 's', clock: 3 } });
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  await flush();
  assert.deepEqual(early, ['c2', 'c3']);
  // A subscriber joining after the revision takes the new default.
  const lateSub = bus.subscribe('u', () => {}, { causal: true });
  for (let clock = 1; clock <= 3; clock += 1) {
    bus.publish('u', `d${clock}`, { causal: { source: 's2', clock } });
  }
  assert.equal(bus.causalBufferDepth(lateSub.id), 3);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1, 'no new drop under the new bound');
  // The per-subscriber revision still works on a default-baked
  // subscriber without touching the bus default.
  bus.setCausal(earlySub.id, 5);
  assert.deepEqual(bus.getDefaultCausal(), { maxBufferPerSource: 100 });
  earlySub.unsubscribe();
  lateSub.unsubscribe();
});

test('EB-503: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultCausal({ maxBufferPerSource: 2 });
  const bad = [
    0,
    -1,
    1.5,
    'x',
    [],
    true,
    { maxBufferPerSource: 0 },
    { maxBufferPerSource: -1 },
    { maxBufferPerSource: 2.5 },
    { maxBufferPerSource: '2' },
    { maxBufferPerSource: Number.NaN },
    { maxBufferPerSource: Number.POSITIVE_INFINITY },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultCausal(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultCausal(), { maxBufferPerSource: 2 });
  }
  assert.throws(() => bus.setDefaultCausal({ maxBufferPerSource: 3 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultCausal(), { maxBufferPerSource: 2 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_causal_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-503: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultCausal(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_causal_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultCausal({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_causal_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultCausal(), undefined);
  const opts: CausalSubscribeOptions = { maxBufferPerSource: 2 };
  bus.setDefaultCausal(opts);
  opts.maxBufferPerSource = 999;
  assert.deepEqual(bus.getDefaultCausal(), { maxBufferPerSource: 2 }, 'the setter stores a defensive copy');
  bus.setDefaultCausal({ maxBufferPerSource: 2 });
  const audit = bus.getSubscriptionAudit({ action: 'default_causal_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    causal: { maxBufferPerSource: 2 },
  });
  const held = bus.getDefaultCausal();
  assert.ok(held !== undefined);
  held.maxBufferPerSource = 999;
  assert.deepEqual(
    bus.getDefaultCausal(),
    { maxBufferPerSource: 2 },
    'the getter returns a copy',
  );
  // A revision moves the live value and audits both sides.
  bus.setDefaultCausal({ maxBufferPerSource: 5 }, 'ops');
  assert.deepEqual(bus.getDefaultCausal(), { maxBufferPerSource: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'default_causal_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { maxBufferPerSource: 2 },
    causal: { maxBufferPerSource: 5 },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultCausal(null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'default_causal_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { maxBufferPerSource: 5 },
    causal: null,
  });
});

test('EB-503: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultCausal({ maxBufferPerSource: 2 });
  const snapshot = bus.exportBusConfig();
  assert.ok(!('defaultCausal' in snapshot), 'no defaultCausal key');
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultCausal'),
    'the field name appears nowhere in the exported snapshot',
  );
  assert.deepEqual(bus.getDefaultCausal(), { maxBufferPerSource: 2 });
});
