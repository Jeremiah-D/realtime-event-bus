import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('setDeliveryPriority revises drain order from the next flush', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  const first = bus.subscribe('t', () => order.push('first'));
  const second = bus.subscribe('t', () => order.push('second'));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['first', 'second']);

  bus.setDeliveryPriority(first.id, 'low');
  bus.publish('t', 'y');
  await flush();
  assert.deepEqual(order, ['first', 'second', 'second', 'first'], 'the revised low weight serves second');

  bus.setDeliveryPriority(first.id, 10);
  bus.publish('t', 'z');
  await flush();
  assert.deepEqual(order, ['first', 'second', 'second', 'first', 'first', 'second'], 'a custom weight serves first again');

  const audit = bus.getSubscriptionAudit({ action: 'delivery_priority_revised' });
  assert.equal(audit.length, 2);
  assert.equal(audit[0].subId, first.id);
  assert.deepEqual(audit[0].detail, {
    deliveryPriority: { weight: 1, label: 'low' },
    oldDeliveryPriority: { weight: 2, label: 'normal' },
  });
  assert.deepEqual(audit[1].detail, {
    deliveryPriority: { weight: 10, label: 'custom' },
    oldDeliveryPriority: { weight: 1, label: 'low' },
  });
  first.unsubscribe();
  second.unsubscribe();
});

test('setDeliveryPriority maintains the non-default count across the default boundary', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  const first = bus.subscribe('t', () => order.push('first'));
  const second = bus.subscribe('t', () => order.push('second'));

  // Both subscribers leave the default side: the DRR path must engage.
  bus.setDeliveryPriority(first.id, 'high');
  bus.setDeliveryPriority(second.id, 'low');
  bus.publish('t', 'a');
  await flush();
  assert.deepEqual(order, ['first', 'second']);

  // Same-side revision (non-default to non-default): the count must not move.
  bus.setDeliveryPriority(second.id, 'high');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(order, ['first', 'second', 'first', 'second']);

  // First subscriber returns to the default side, but the second is
  // still non-default: the count must stay above zero, or the fast
  // path would silently ignore the second subscriber's weight.
  bus.setDeliveryPriority(first.id, null);
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(
    order,
    ['first', 'second', 'first', 'second', 'second', 'first'],
    'the remaining high weight still outranks the restored default',
  );

  // The default side gains a non-default weight again: the DRR path
  // must engage for it (a missed increment would serve in plain
  // subscription order instead).
  bus.setDeliveryPriority(first.id, 'low');
  bus.publish('t', 'd');
  await flush();
  assert.deepEqual(
    order,
    ['first', 'second', 'first', 'second', 'second', 'first', 'second', 'first'],
    'the newly low subscriber serves after the high one',
  );
  first.unsubscribe();
  second.unsubscribe();
});

test('setDeliveryPriority null restores the default, and a custom weight of 2 stays on the default side', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  const first = bus.subscribe('t', () => order.push('first'));
  const second = bus.subscribe('t', () => order.push('second'));

  bus.setDeliveryPriority(first.id, 'high');
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['first', 'second']);

  bus.setDeliveryPriority(first.id, null);
  bus.publish('t', 'y');
  await flush();
  assert.deepEqual(order, ['first', 'second', 'first', 'second'], 'both back to normal: subscription order');

  // Same weight as the default, different label: not a no-op (the
  // label moves), but the count must not move — weight 2 is the
  // default side, whatever the label says.
  bus.setDeliveryPriority(first.id, 2);
  bus.publish('t', 'z');
  await flush();
  assert.deepEqual(order, ['first', 'second', 'first', 'second', 'first', 'second']);

  const audit = bus.getSubscriptionAudit({ action: 'delivery_priority_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[1].detail, {
    deliveryPriority: { weight: 2, label: 'normal' },
    oldDeliveryPriority: { weight: 4, label: 'high' },
  });
  assert.deepEqual(audit[2].detail, {
    deliveryPriority: { weight: 2, label: 'custom' },
    oldDeliveryPriority: { weight: 2, label: 'normal' },
  });
  first.unsubscribe();
  second.unsubscribe();
});

test('setDeliveryPriority with the same weight and label is a no-op with no audit', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  const plain = bus.subscribe('t', () => order.push('plain'));
  const high = bus.subscribe('t', () => order.push('high'), { deliveryPriority: 'high' });

  bus.setDeliveryPriority(plain.id, 'normal');
  bus.setDeliveryPriority(high.id, 'high');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'delivery_priority_revised' }).length,
    0,
    're-stating the current weight and label audits nothing',
  );

  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['high', 'plain']);
  plain.unsubscribe();
  high.unsubscribe();
});

test('setDeliveryPriority is fail-closed and leaves live weights and audit untouched', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  const first = bus.subscribe('t', () => order.push('first'));
  const second = bus.subscribe('t', () => order.push('second'), { deliveryPriority: 'high' });

  assert.throws(() => bus.setDeliveryPriority('no-such-sub', 'low'), RangeError);
  assert.throws(() => bus.setDeliveryPriority(first.id, undefined as never), RangeError, 'undefined is not a restore');
  for (const bad of ['urgent', 'HIGH', '', 0, -1, 1.5, NaN, {}, [], true]) {
    assert.throws(() => bus.setDeliveryPriority(first.id, bad as never), RangeError);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'delivery_priority_revised' }).length, 0, 'no failed revision is audited');

  // The original weights are untouched by every failed attempt.
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(order, ['second', 'first']);

  // And a legal revision still works afterwards.
  bus.setDeliveryPriority(first.id, 'low');
  assert.equal(bus.getSubscriptionAudit({ action: 'delivery_priority_revised' }).length, 1);
  first.unsubscribe();
  second.unsubscribe();
});
