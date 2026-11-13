import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default delivery priority (EB-490):
 * `setDefaultDeliveryPriority` installs the drain priority new
 * subscribers without an explicit `deliveryPriority` fall back to —
 * the priority chain is subscriber-explicit > bus default >
 * `'normal'` — and a revision never moves an already-subscribed
 * subscriber's weight. This default is the one family member with a
 * documented factory value: a bus that never installed a default
 * already drains new plain subscribers at `'normal'` / weight 2.
 *
 * Class assertions follow `subscriber-priority.test.ts`: several
 * subscribers on one topic, one publish, one flush, and the handler
 * call order plus `getStats().subscriberPriority.dispatches` read
 * the resolved weights off the drain.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test("EB-490: the factory default is 'normal' (a plain subscriber drains as normal)", async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDeliveryPriority(), undefined);
  const order: string[] = [];
  bus.subscribe('t', () => order.push('first'));
  bus.subscribe('t', () => order.push('second'));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['first', 'second']);
  assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
    low: 0,
    normal: 2,
    high: 0,
    custom: 0,
  });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_priority_revised' }).length,
    0,
  );
});

test("EB-490: set('high') makes new plain subscribers drain as high", async () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority('high');
  assert.equal(bus.getDefaultDeliveryPriority(), 'high');
  const order: string[] = [];
  bus.subscribe('t', () => order.push('low'), { deliveryPriority: 'low' });
  bus.subscribe('t', () => order.push('plain'));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['plain', 'low'], 'the plain subscriber took the high default');
  assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
    low: 1,
    normal: 0,
    high: 1,
    custom: 0,
  });
});

test('EB-490: set(3) makes new plain subscribers drain at custom weight 3', async () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority(3);
  assert.equal(bus.getDefaultDeliveryPriority(), 3);
  const order: string[] = [];
  bus.subscribe('t', () => order.push('low'), { deliveryPriority: 'low' });
  bus.subscribe('t', () => order.push('plain'));
  bus.subscribe('t', () => order.push('high'), { deliveryPriority: 'high' });
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['high', 'plain', 'low'], 'weight 4 > 3 > 1');
  assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
    low: 1,
    normal: 0,
    high: 1,
    custom: 1,
  });
});

test("EB-490: an explicit 'low' always beats a bus default of 'high'", async () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority('high');
  const order: string[] = [];
  bus.subscribe('t', () => order.push('explicit-low'), { deliveryPriority: 'low' });
  bus.subscribe('t', () => order.push('explicit-high'), { deliveryPriority: 'high' });
  bus.subscribe('t', () => order.push('plain'));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(
    order,
    ['explicit-high', 'plain', 'explicit-low'],
    'explicit low stays low even under a high default; the plain subscriber takes the default',
  );
  assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
    low: 1,
    normal: 0,
    high: 2,
    custom: 0,
  });
});

test("EB-490: clearing with null returns new plain subscribers to 'normal'", async () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority('high');
  bus.setDefaultDeliveryPriority(null);
  assert.equal(bus.getDefaultDeliveryPriority(), undefined);
  const order: string[] = [];
  bus.subscribe('t', () => order.push('first'));
  bus.subscribe('t', () => order.push('second'));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['first', 'second']);
  assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
    low: 0,
    normal: 2,
    high: 0,
    custom: 0,
  });
});

test('EB-490: revising the bus default never moves an already-subscribed subscriber', async () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority('high');
  const order: string[] = [];
  const early = bus.subscribe('t', () => order.push('early'));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['early']);
  assert.equal(bus.getStats().subscriberPriority.dispatches.high, 1);
  bus.setDefaultDeliveryPriority('low');
  bus.publish('t', 'y');
  await flush();
  assert.deepEqual(order, ['early', 'early'], 'the early subscriber still drains high');
  assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
    low: 0,
    normal: 0,
    high: 2,
    custom: 0,
  });
  // A subscriber joining after the revision takes the new default.
  bus.subscribe('t', () => order.push('late'));
  bus.publish('t', 'z');
  await flush();
  assert.deepEqual(order, ['early', 'early', 'early', 'late']);
  assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
    low: 1,
    normal: 0,
    high: 3,
    custom: 0,
  });
  early.unsubscribe();
});

test('EB-490: setDeliveryPriority still revises one subscriber, and null restores normal without the bus default re-applying', async () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority('high');
  const order: string[] = [];
  const sub = bus.subscribe('t', () => order.push('sub'));
  bus.setDeliveryPriority(sub.id, 'low');
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
    low: 1,
    normal: 0,
    high: 0,
    custom: 0,
  });
  bus.setDeliveryPriority(sub.id, null);
  bus.publish('t', 'y');
  await flush();
  assert.deepEqual(
    bus.getStats().subscriberPriority.dispatches,
    { low: 1, normal: 1, high: 0, custom: 0 },
    'the null restore is the documented normal, not the installed high default',
  );
  assert.throws(() => bus.setDeliveryPriority(sub.id, 'urgent' as never), RangeError);
  assert.throws(() => bus.setDeliveryPriority(sub.id, undefined as never), RangeError);
  sub.unsubscribe();
});

test('EB-490: revising to the current default is a no-op with no audit, and a label-only change at the same weight is audited', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority(null);
  bus.setDefaultDeliveryPriority('normal');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_priority_revised' }).length,
    0,
    "clearing an already-unset default and setting 'normal' on a factory bus both audit nothing",
  );
  assert.equal(bus.getDefaultDeliveryPriority(), undefined);
  bus.setDefaultDeliveryPriority('high');
  bus.setDefaultDeliveryPriority('high');
  bus.setDefaultDeliveryPriority('high');
  const audit = bus.getSubscriptionAudit({ action: 'default_delivery_priority_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, { previous: null, deliveryPriority: 'high' });
  // A custom weight of 4 orders like 'high' but is a different
  // revision: the observability class moves.
  bus.setDefaultDeliveryPriority(4);
  assert.equal(bus.getDefaultDeliveryPriority(), 4);
  const relabelled = bus.getSubscriptionAudit({ action: 'default_delivery_priority_revised' });
  assert.equal(relabelled.length, 2);
  assert.deepEqual(relabelled[1].detail, { previous: 'high', deliveryPriority: 4 });
  bus.setDefaultDeliveryPriority('low', 'ops');
  assert.equal(bus.getDefaultDeliveryPriority(), 'low');
  const revised = bus.getSubscriptionAudit({ action: 'default_delivery_priority_revised' });
  assert.equal(revised.length, 3);
  assert.deepEqual(revised[2].detail, { previous: 4, deliveryPriority: 'low' });
  assert.equal(revised[2].actor, 'ops');
  bus.setDefaultDeliveryPriority(null);
  assert.equal(bus.getDefaultDeliveryPriority(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_delivery_priority_revised' });
  assert.equal(cleared.length, 4);
  assert.deepEqual(cleared[3].detail, { previous: 'low', deliveryPriority: null });
  bus.setDefaultDeliveryPriority(null);
  bus.setDefaultDeliveryPriority('normal');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_priority_revised' }).length,
    4,
    'clearing an already-unset default and setting normal on a cleared bus both audit nothing',
  );
});

test('EB-490: an explicit null deliveryPriority still throws at subscribe, even with a bus default installed', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority('high');
  assert.throws(
    () => bus.subscribe('t', () => {}, { deliveryPriority: null as never }),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0);
  // And an explicitly undefined priority still takes the default.
  const order: string[] = [];
  bus.subscribe('t', () => order.push('plain'), { deliveryPriority: undefined });
  bus.publish('t', 'x');
  return flush().then(() => {
    assert.deepEqual(order, ['plain']);
    assert.deepEqual(bus.getStats().subscriberPriority.dispatches, {
      low: 0,
      normal: 0,
      high: 1,
      custom: 0,
    });
  });
});

test('EB-490: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultDeliveryPriority('high');
  const bad = ['urgent', 'HIGH', '', 0, -1, 1.5, Number.NaN, {}, [], true, undefined] as unknown[];
  for (const priority of bad) {
    assert.throws(
      () => bus.setDefaultDeliveryPriority(priority as never),
      RangeError,
      `priority=${String(priority)}`,
    );
    assert.equal(bus.getDefaultDeliveryPriority(), 'high');
  }
  assert.throws(() => bus.setDefaultDeliveryPriority('low', ''), RangeError);
  assert.equal(bus.getDefaultDeliveryPriority(), 'high');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_delivery_priority_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});
