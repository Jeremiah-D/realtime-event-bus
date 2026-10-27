import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import type { Delivery } from '../src/ack.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setAckTimeout revises the timeout for new deliveries but leaves an armed timer alone', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.redeliveries >= 1) d.ack(); // settle redeliveries so the test ends cleanly
    },
    { ackTimeoutMs: 40 },
  );
  bus.publish('t', 'first');
  await flush();
  assert.equal(deliveries.length, 1);
  bus.setAckTimeout(sub.id, 400);
  await sleep(100); // past the original 40 ms timer, inside the revised 400 ms one
  await flush();
  assert.equal(deliveries.length, 2, 'the in-flight delivery must keep its original deadline');
  assert.equal(deliveries[1].redeliveries, 1);
  assert.equal(deliveries[1].seq, 2, 'revising must not change the sequence');
  bus.publish('t', 'second');
  await flush();
  assert.equal(deliveries.length, 3);
  await sleep(100); // inside the revised 400 ms timeout for the new delivery
  await flush();
  assert.equal(deliveries.length, 3, 'a delivery tracked after the revision must use the new timeout');
  await sleep(400); // past the revised timeout: the new delivery finally requeues
  await flush();
  assert.equal(deliveries.length, 4);
  assert.equal(deliveries[3].redeliveries, 1);
  const events = bus.getSubscriptionAudit({ action: 'ack_timeout_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, { ackTimeoutMs: 400, oldAckTimeoutMs: 40 });
});

test('setAckTimeout rejects unknown subscribers, non-reliable subscribers, and illegal values fail-closed', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.redeliveries >= 1) d.ack();
    },
    { ackTimeoutMs: 60 },
  );
  const plain = bus.subscribe('plain', () => {});
  assert.throws(() => bus.setAckTimeout('', 100), RangeError);
  assert.throws(() => bus.setAckTimeout('no-such-sub', 100), RangeError);
  assert.throws(() => bus.setAckTimeout(plain.id, 100), RangeError);
  assert.throws(() => bus.setAckTimeout(sub.id, 0), RangeError);
  assert.throws(() => bus.setAckTimeout(sub.id, -5), RangeError);
  assert.throws(() => bus.setAckTimeout(sub.id, Number.NaN), RangeError);
  assert.throws(() => bus.setAckTimeout(sub.id, Number.POSITIVE_INFINITY), RangeError);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'ack_timeout_revised' }), []);
  // The rejected revisions left the original timeout intact: a fresh
  // delivery still requeues on the original 60 ms timer.
  bus.publish('t', 'x');
  await flush();
  await sleep(120);
  await flush();
  assert.equal(deliveries.length, 2, 'failed revisions must not move the timeout');
});

test('setAckTimeout audits each revision with its old and new value', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  bus.setAckTimeout(sub.id, 2000);
  bus.setAckTimeout(sub.id, 500);
  const events = bus.getSubscriptionAudit({ action: 'ack_timeout_revised' });
  assert.equal(events.length, 2);
  assert.deepEqual(events[0].detail, { ackTimeoutMs: 2000, oldAckTimeoutMs: 1000 });
  assert.deepEqual(events[1].detail, { ackTimeoutMs: 500, oldAckTimeoutMs: 2000 });
  assert.ok(events.every((e) => e.subId === sub.id));
});
