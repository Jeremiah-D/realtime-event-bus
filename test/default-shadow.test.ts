import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type ShadowSubscribeOptions } from '../src/bus.ts';

/**
 * Bus-level default shadow-queue tuning (EB-505):
 * `setDefaultShadow` installs the `queueSize` / `dropPolicy` new
 * opt-in `shadow` subscribers fall back to when they omit a field —
 * the priority chain is subscriber-explicit shadow field > bus
 * default shadow field > subscription's own `queueSize` /
 * `dropPolicy` > bus default `queueSize` / `dropPolicy` (EB-470 /
 * EB-475) > 100 / `'drop-oldest'` — and the default never enables
 * shadowing: a subscriber that does not opt into `shadow` never
 * mirrors anything.
 *
 * Shadow assertions follow `shadow.test.ts`: the shadow queue is the
 * subscription's own queue, so a saturated shadow sheds only its own
 * copy per its own drop policy, counted in the shadow's independent
 * counters (`getStats().shadows[]` / `shadowDroppedMessages`) and
 * never in the production delivery/drop counters.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('EB-505: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultShadow(), undefined);
  bus.setDefaultShadow(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_shadow_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-505: `true` opt-in takes the default queue and policy — newest shed, production untouched', async () => {
  const bus = new EventBus();
  const prod: unknown[] = [];
  const shadow: unknown[] = [];
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-newest' });
  bus.subscribe('t', (m) => prod.push(m.payload));
  bus.subscribe('t', (m) => shadow.push(m.payload), { shadow: true });
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.publish('t', 3);
  await flush();
  assert.deepEqual(prod, [1, 2, 3]);
  assert.deepEqual(shadow, [1, 2]);
  const stats = bus.getStats();
  assert.equal(stats.shadowedMessages, 2);
  assert.equal(stats.shadowDroppedMessages, 1);
  assert.equal(stats.deliveredMessages, 3);
  assert.equal(stats.droppedMessages, 0);
  const [row] = stats.shadows;
  assert.equal(row.shadowed, 2);
  assert.equal(row.dropped, 1);
  assert.equal(row.queueSize, 0);
});

test('EB-505: `{}` opts in and takes the whole default', async () => {
  const bus = new EventBus();
  const shadow: unknown[] = [];
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-oldest' });
  bus.subscribe('t', (m) => shadow.push(m.payload), { shadow: {} });
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.publish('t', 3);
  await flush();
  // Capacity 2 with drop-oldest: the first copy is shed, the last two mirror.
  assert.deepEqual(shadow, [2, 3]);
  const stats = bus.getStats();
  assert.equal(stats.shadowedMessages, 2);
  assert.equal(stats.shadowDroppedMessages, 1);
  assert.equal(stats.droppedMessages, 0);
});

test('EB-505: an explicit shadow field always wins over the bus default, the other field still falls back', async () => {
  const bus = new EventBus();
  const shadow: unknown[] = [];
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-newest' });
  // Explicit queueSize 5 wins; dropPolicy falls back to the default 'drop-newest'.
  bus.subscribe('t', (m) => shadow.push(m.payload), { shadow: { queueSize: 5 } });
  for (let i = 1; i <= 6; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(shadow, [1, 2, 3, 4, 5]);
  const stats = bus.getStats();
  assert.equal(stats.shadowedMessages, 5);
  assert.equal(stats.shadowDroppedMessages, 1);
  assert.equal(stats.droppedMessages, 0);
});

test('EB-505: with only a default queueSize, dropPolicy falls back to the subscription\'s own value', async () => {
  const bus = new EventBus();
  const newest: unknown[] = [];
  const oldest: unknown[] = [];
  bus.setDefaultShadow({ queueSize: 2 });
  // The subscription's own dropPolicy wins when the default has none.
  bus.subscribe('t', (m) => newest.push(m.payload), { shadow: true, dropPolicy: 'drop-newest' });
  // No subscription dropPolicy either: the built-in 'drop-oldest'.
  bus.subscribe('u', (m) => oldest.push(m.payload), { shadow: true });
  for (let i = 1; i <= 3; i++) {
    bus.publish('t', i);
    bus.publish('u', i);
  }
  await flush();
  assert.deepEqual(newest, [1, 2]);
  assert.deepEqual(oldest, [2, 3]);
  const stats = bus.getStats();
  assert.equal(stats.shadowedMessages, 4);
  assert.equal(stats.shadowDroppedMessages, 2);
  assert.equal(stats.droppedMessages, 0);
});

test('EB-505: clearing with null returns new opt-in subscribers to the subscription\'s own values', async () => {
  const bus = new EventBus();
  const shadow: unknown[] = [];
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-newest' });
  bus.setDefaultShadow(null);
  assert.equal(bus.getDefaultShadow(), undefined);
  bus.subscribe('t', (m) => shadow.push(m.payload), { shadow: true });
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.publish('t', 3);
  await flush();
  // No default left: the shadow queue is the subscription's own 100-deep queue.
  assert.deepEqual(shadow, [1, 2, 3]);
  const audit = bus.getSubscriptionAudit({ action: 'default_shadow_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { queueSize: 2, dropPolicy: 'drop-newest' },
    shadow: null,
  });
});

test('EB-505: the default never enables shadowing for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-newest' });
  const plain: unknown[] = [];
  const declined: unknown[] = [];
  bus.subscribe('t', (m) => plain.push(m.payload));
  bus.subscribe('t', (m) => declined.push(m.payload), { shadow: false });
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(plain, [1, 2]);
  assert.deepEqual(declined, [1, 2]);
  const stats = bus.getStats();
  assert.equal(stats.shadows.length, 0, 'no shadow row without an opt-in');
  assert.equal(stats.shadowedMessages, 0);
  assert.equal(stats.shadowDroppedMessages, 0);
});

test('EB-505: revising the bus default never moves an already-subscribed shadow queue or its counters', async () => {
  const bus = new EventBus();
  const early: unknown[] = [];
  const late: unknown[] = [];
  bus.subscribe('t', (m) => early.push(m.payload), { shadow: true });
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-newest' });
  // The early subscriber keeps the 100-deep queue it was baked with:
  // two more arrivals still mirror instead of shedding.
  bus.publish('t', 3);
  bus.publish('t', 4);
  await flush();
  assert.deepEqual(early, [1, 2, 3, 4]);
  const stats = bus.getStats();
  assert.equal(stats.shadowedMessages, 4);
  assert.equal(stats.shadowDroppedMessages, 0);
  // A subscriber joining after the revision takes the new default.
  bus.subscribe('u', (m) => late.push(m.payload), { shadow: true });
  bus.publish('u', 1);
  bus.publish('u', 2);
  bus.publish('u', 3);
  await flush();
  assert.deepEqual(late, [1, 2]);
  assert.equal(bus.getStats().shadowDroppedMessages, 1);
});

test('EB-505: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-newest' });
  const bad = [
    0,
    -1,
    1.5,
    'x',
    [],
    true,
    { queueSize: 0 },
    { queueSize: -1 },
    { queueSize: 2.5 },
    { queueSize: '2' },
    { queueSize: Number.NaN },
    { queueSize: Number.POSITIVE_INFINITY },
    { dropPolicy: 'drop-all' },
    { dropPolicy: '' },
    { dropPolicy: 'block' },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultShadow(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultShadow(), { queueSize: 2, dropPolicy: 'drop-newest' });
  }
  assert.throws(() => bus.setDefaultShadow({ queueSize: 3 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultShadow(), { queueSize: 2, dropPolicy: 'drop-newest' });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_shadow_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-505: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultShadow(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_shadow_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultShadow({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_shadow_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultShadow(), undefined);
  const opts: ShadowSubscribeOptions = { queueSize: 2, dropPolicy: 'drop-newest' };
  bus.setDefaultShadow(opts);
  opts.queueSize = 999;
  assert.deepEqual(
    bus.getDefaultShadow(),
    { queueSize: 2, dropPolicy: 'drop-newest' },
    'the setter stores a defensive copy',
  );
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-newest' });
  const audit = bus.getSubscriptionAudit({ action: 'default_shadow_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    shadow: { queueSize: 2, dropPolicy: 'drop-newest' },
  });
  const held = bus.getDefaultShadow();
  assert.ok(held !== undefined);
  held.queueSize = 999;
  assert.deepEqual(
    bus.getDefaultShadow(),
    { queueSize: 2, dropPolicy: 'drop-newest' },
    'the getter returns a copy',
  );
  // A revision moves the live value and audits both sides.
  bus.setDefaultShadow({ queueSize: 5 }, 'ops');
  assert.deepEqual(bus.getDefaultShadow(), { queueSize: 5 });
  const revised = bus.getSubscriptionAudit({ action: 'default_shadow_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { queueSize: 2, dropPolicy: 'drop-newest' },
    shadow: { queueSize: 5 },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultShadow(null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'default_shadow_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { queueSize: 5 },
    shadow: null,
  });
});

test('EB-505: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultShadow({ queueSize: 2, dropPolicy: 'drop-newest' });
  const snapshot = bus.exportBusConfig();
  assert.ok(!('defaultShadow' in snapshot), 'no defaultShadow key');
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultShadow'),
    'the field name appears nowhere in the exported snapshot',
  );
  assert.deepEqual(bus.getDefaultShadow(), { queueSize: 2, dropPolicy: 'drop-newest' });
});
