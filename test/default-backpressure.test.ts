/**
 * Bus-level default backpressure tier live revision (EB-511).
 *
 * `setDefaultBackpressure` decides what a future subscriber does when
 * its bounded queue is full and it omits its own `backpressure`
 * (EB-83) — `'drop-oldest'` to shed the oldest entry fleet-wide with
 * one call, `'drop-newest'` to refuse newcomers, `'block'` to
 * admit-with-shed on first saturation and then refuse while saturated,
 * `null` to clear back to the historical `'block'`. The priority chain
 * is subscriber-explicit > bus default > `'block'`; an explicit tier
 * always wins, and the merged tier still goes through the
 * subscribe-time dropPolicy-conflict check. A `'block'` default leaves
 * the `dropPolicy` chain untouched: explicit `dropPolicy` > bus default
 * (EB-475) > `'drop-oldest'`. A subscriber keeps the tier it was baked
 * with, and `setBackpressureTier` (EB-285) still revises one subscriber
 * at a time without touching the bus default. A revision identical to
 * the current default is a no-op with no audit. Audited as
 * `default_backpressure_revised` (`{ previous, backpressure }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_backpressure_revised' });

test("EB-511: set default 'drop-oldest' — a later subscriber that omits backpressure sheds the oldest", async () => {
  const bus = new EventBus();
  bus.setDefaultBackpressure('drop-oldest');
  assert.equal(bus.getDefaultBackpressure(), 'drop-oldest');
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // The default drop-oldest tier: the newcomer is admitted, the oldest shed.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  sub.unsubscribe();
});

test("EB-511: an explicit tier always beats the bus default, including an explicit 'block' back to the 'block' branch", async () => {
  const bus = new EventBus();
  bus.setDefaultBackpressure('drop-oldest');
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'block',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // Explicit 'block' beats the drop-oldest default: first saturation
  // admits-with-shed, then further arrivals are refused while saturated.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  // One shed (m1, on the first saturation) plus one refused newcomer.
  assert.equal(bus.droppedCount(sub.id), 2);
  assert.equal(bus.blockedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  sub.unsubscribe();

  // An explicit drop-newest tier beats a drop-oldest default: the
  // newcomer is refused outright, the backlog intact.
  const loud = new EventBus();
  loud.setDefaultBackpressure('drop-oldest');
  const loudReceived: unknown[] = [];
  const loudSub = loud.subscribe('t', (msg) => loudReceived.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(loud.publish('t', 'm1'), 1);
  assert.equal(loud.publish('t', 'm2'), 1);
  assert.equal(loud.publish('t', 'm3'), 0);
  assert.equal(loud.pendingCount(loudSub.id), 2);
  assert.equal(loud.droppedCount(loudSub.id), 1);
  await flush();
  assert.deepEqual(loudReceived, ['m1', 'm2']);
  loudSub.unsubscribe();
});

test('EB-511: an explicit dropPolicy contradicting the default drop tier throws and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultBackpressure('drop-oldest');
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        queueSize: 2,
        dropPolicy: 'drop-newest',
      }),
    RangeError,
  );
  assert.equal(bus.getDefaultBackpressure(), 'drop-oldest');
  assert.equal(
    defaultAudits(bus).length,
    1,
    'the failed subscribe must not audit',
  );
  // The failed subscribe left no half-registered subscriber behind:
  // a later valid subscription on the same topic works normally.
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  sub.unsubscribe();
});

test("EB-511: a 'block' default leaves the dropPolicy chain walking the bus default (EB-475)", async () => {
  const bus = new EventBus();
  bus.setDefaultBackpressure('block');
  bus.setDefaultDropPolicy('drop-newest');
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // First saturation under the 'block' tier sheds per the bus-default
  // drop-newest: the newcomer is refused, the backlog intact.
  assert.equal(bus.publish('t', 'm3'), 0);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  sub.unsubscribe();

  // Without a drop-policy default the same 'block' default falls back
  // to the historical 'drop-oldest' on first saturation.
  const old = new EventBus();
  old.setDefaultBackpressure('block');
  const oldReceived: unknown[] = [];
  const oldSub = old.subscribe('t', (msg) => oldReceived.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(old.publish('t', 'm1'), 1);
  assert.equal(old.publish('t', 'm2'), 1);
  assert.equal(old.publish('t', 'm3'), 1);
  assert.equal(old.publish('t', 'm4'), 0);
  await flush();
  assert.deepEqual(oldReceived, ['m2', 'm3']);
  oldSub.unsubscribe();
});

test("EB-511: the factory default is unset and clearing with null returns new subscribers to 'block'", async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultBackpressure(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // No default: the historical 'block' — first saturation admits with
  // a shed, then arrivals are refused while saturated.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.publish('t', 'm4'), 0);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  sub.unsubscribe();

  bus.setDefaultBackpressure('drop-oldest');
  bus.setDefaultBackpressure(null);
  assert.equal(bus.getDefaultBackpressure(), undefined);
  const cleared: unknown[] = [];
  const clearedSub = bus.subscribe('u', (msg) => cleared.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('u', 'm1'), 1);
  assert.equal(bus.publish('u', 'm2'), 1);
  assert.equal(bus.publish('u', 'm3'), 1);
  assert.equal(bus.publish('u', 'm4'), 0);
  await flush();
  assert.deepEqual(cleared, ['m2', 'm3']);
  clearedSub.unsubscribe();
});

test('EB-511: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultBackpressure(null);
  assert.equal(
    defaultAudits(bus).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultBackpressure('drop-oldest');
  assert.equal(bus.getDefaultBackpressure(), 'drop-oldest');
  bus.setDefaultBackpressure('drop-oldest');
  assert.equal(bus.getDefaultBackpressure(), 'drop-oldest');
  const audit = defaultAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, backpressure: 'drop-oldest' });
  bus.setDefaultBackpressure(null);
  assert.equal(bus.getDefaultBackpressure(), undefined);
  bus.setDefaultBackpressure(null);
  assert.equal(defaultAudits(bus).length, 2, 're-clearing audits nothing');
});

test('EB-511: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultBackpressure('drop-oldest');
  for (const bad of [
    'queue',
    'BLOCK',
    'drop',
    '',
    undefined,
    42,
    {},
    ['drop-oldest'],
  ] as unknown[]) {
    assert.throws(
      () => bus.setDefaultBackpressure(bad as never),
      RangeError,
      `tier=${String(bad)}`,
    );
    assert.equal(bus.getDefaultBackpressure(), 'drop-oldest');
  }
  assert.throws(() => bus.setDefaultBackpressure('drop-newest', ''), RangeError);
  assert.equal(bus.getDefaultBackpressure(), 'drop-oldest');
  assert.equal(
    defaultAudits(bus).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-511: revising the bus default never moves an already-subscribed tier', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  bus.setDefaultBackpressure('drop-oldest');
  // The subscriber keeps the 'block' it was baked with: first
  // saturation admits with a shed, then arrivals are refused.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  // One shed (m1) plus one refused newcomer.
  assert.equal(bus.droppedCount(sub.id), 2);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  // A subscriber joining after the revision takes the new default.
  const lateReceived: unknown[] = [];
  const late = bus.subscribe('u', (msg) => lateReceived.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('u', 'm1'), 1);
  assert.equal(bus.publish('u', 'm2'), 1);
  assert.equal(bus.publish('u', 'm3'), 1);
  assert.equal(bus.pendingCount(late.id), 2);
  assert.equal(bus.droppedCount(late.id), 1);
  await flush();
  assert.deepEqual(lateReceived, ['m2', 'm3']);
  late.unsubscribe();
  sub.unsubscribe();
});

test('EB-511: setBackpressureTier still revises one subscriber at a time and leaves the bus default in place', async () => {
  const bus = new EventBus();
  bus.setDefaultBackpressure('drop-oldest');
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // The per-subscriber revision retunes the tier: 'block' arms the
  // refusal on the next saturation, while the shed still follows the
  // standing 'drop-oldest' policy (EB-285 — a tier revision never
  // moves the drop policy).
  bus.setBackpressureTier(sub.id, 'block');
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 2);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  // The bus default is untouched by the per-subscriber revision.
  assert.equal(bus.getDefaultBackpressure(), 'drop-oldest');
  const tierEvents = bus.getSubscriptionAudit({ action: 'backpressure_tier_revised' });
  assert.equal(tierEvents.length, 1);
  assert.deepEqual(tierEvents[0].detail, {
    backpressure: 'block',
    oldBackpressure: 'drop-oldest',
  });
  sub.unsubscribe();
});

test('EB-511: setDefaultBackpressure audits each revision with its old and new tier and actor, and the getter follows the live value', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultBackpressure(), undefined);
  bus.setDefaultBackpressure('drop-newest', 'ops');
  assert.equal(bus.getDefaultBackpressure(), 'drop-newest');
  bus.setDefaultBackpressure('block', 'ops');
  assert.equal(bus.getDefaultBackpressure(), 'block');
  bus.setDefaultBackpressure(null, 'ops');
  assert.equal(bus.getDefaultBackpressure(), undefined);
  const events = defaultAudits(bus);
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, backpressure: 'drop-newest' });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 'drop-newest', backpressure: 'block' });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 'block', backpressure: null });
});
