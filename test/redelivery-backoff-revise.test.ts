import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setRedeliveryBackoff leaves an armed delay alone but spaces later redeliveries by the new policy', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries === 0) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000, redeliveryBackoff: { baseMs: 50 } },
  );
  bus.publish('t', 'first');
  await flush();
  assert.deepEqual(seen, [0]);
  // The nack above armed a 50 ms delay; revising to 400 ms must not
  // move that armed timer, only the policy for redeliveries after it.
  bus.setRedeliveryBackoff(sub.id, { baseMs: 400, capMs: 400 });
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the armed 50 ms delay must fire on its original schedule');
  // A fresh message nacked now arms under the new 400 ms policy.
  bus.publish('t', 'second');
  await flush();
  assert.deepEqual(seen, [0, 1, 0]);
  await sleep(150);
  assert.deepEqual(seen, [0, 1, 0], 'the new 400 ms policy must delay the fresh redelivery');
  await sleep(350);
  assert.deepEqual(seen, [0, 1, 0, 1]);
  const events = bus.getSubscriptionAudit({ action: 'redelivery_backoff_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, {
    redeliveryBackoff: { baseMs: 400, capMs: 400, jitter: false },
    oldRedeliveryBackoff: { baseMs: 50, capMs: Number.POSITIVE_INFINITY, jitter: false },
  });
});

test('setRedeliveryBackoff can add a policy to a subscriber without one, and null clears it back to immediate', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries === 0) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000 },
  );
  // No policy: the nack requeues immediately.
  bus.publish('t', 'plain');
  await flush();
  await sleep(50);
  assert.deepEqual(seen, [0, 1]);
  // Opt in after the fact: the next nack waits out the new policy.
  bus.setRedeliveryBackoff(sub.id, { baseMs: 60 });
  bus.publish('t', 'delayed');
  await flush();
  assert.deepEqual(seen, [0, 1, 0]);
  await sleep(30);
  assert.deepEqual(seen, [0, 1, 0]);
  await sleep(80);
  assert.deepEqual(seen, [0, 1, 0, 1]);
  // Clear: the next nack requeues immediately again.
  bus.setRedeliveryBackoff(sub.id, null);
  bus.publish('t', 'cleared');
  await flush();
  await sleep(50);
  assert.deepEqual(seen, [0, 1, 0, 1, 0, 1]);
  const events = bus.getSubscriptionAudit({ action: 'redelivery_backoff_revised' });
  assert.equal(events.length, 2);
  assert.deepEqual(events[0].detail, {
    redeliveryBackoff: { baseMs: 60, capMs: Number.POSITIVE_INFINITY, jitter: false },
    oldRedeliveryBackoff: null,
  });
  assert.deepEqual(events[1].detail, {
    redeliveryBackoff: null,
    oldRedeliveryBackoff: { baseMs: 60, capMs: Number.POSITIVE_INFINITY, jitter: false },
  });
});

test('setRedeliveryBackoff rejects unknown, non-reliable, and illegal policies fail-closed', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries === 0) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 5000, redeliveryBackoff: { baseMs: 100 } },
  );
  const plain = bus.subscribe('plain', () => {});
  assert.throws(() => bus.setRedeliveryBackoff('', { baseMs: 50 }), RangeError);
  assert.throws(() => bus.setRedeliveryBackoff('no-such-sub', { baseMs: 50 }), RangeError);
  assert.throws(() => bus.setRedeliveryBackoff(plain.id, { baseMs: 50 }), RangeError);
  assert.throws(() => bus.setRedeliveryBackoff(sub.id, { baseMs: 0 }), RangeError);
  assert.throws(() => bus.setRedeliveryBackoff(sub.id, { baseMs: 100, capMs: 50 }), RangeError);
  assert.throws(
    () => bus.setRedeliveryBackoff(sub.id, { baseMs: 100, jitter: 'yes' } as never),
    RangeError,
  );
  assert.throws(() => bus.setRedeliveryBackoff(sub.id, 100 as never), RangeError);
  assert.throws(() => bus.setRedeliveryBackoff(sub.id, undefined as never), RangeError);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'redelivery_backoff_revised' }), []);
  // The rejected revisions left the original 100 ms policy intact.
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(seen, [0]);
  await sleep(50);
  assert.deepEqual(seen, [0]);
  await sleep(120);
  assert.deepEqual(seen, [0, 1], 'failed revisions must not move the policy');
});
