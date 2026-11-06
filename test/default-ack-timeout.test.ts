import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import type { Delivery } from '../src/ack.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Settles a promise without throwing when it rejects. */
const settled = <T>(p: Promise<T>): Promise<T | Error> =>
  p.then(
    (v) => v,
    (e: unknown) => (e instanceof Error ? e : new Error(String(e))),
  );

test('the factory default is 5000 and a new reliable subscription arms at it', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultAckTimeout(), 5000);
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable('t', (d) => {
    deliveries.push(d);
  });
  bus.publish('t', 'first');
  await flush();
  assert.equal(deliveries.length, 1);
  // Still inside the 5000ms factory deadline: no redelivery yet. (A
  // default of 300ms or less would already have requeued by now.)
  await sleep(300);
  await flush();
  assert.equal(deliveries.length, 1, 'a delivery armed at the factory default must not requeue early');
});

test('revising the default arms new subscriptions at the new value', async () => {
  const bus = new EventBus();
  bus.setDefaultAckTimeout(60);
  assert.equal(bus.getDefaultAckTimeout(), 60);
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.redeliveries >= 1) d.ack(); // settle redeliveries so the test ends cleanly
    },
  );
  bus.publish('t', 'first');
  await flush();
  assert.equal(deliveries.length, 1);
  await sleep(150); // past the revised 60ms deadline
  await flush();
  assert.equal(deliveries.length, 2, 'a subscription created after the revision must use the new default');
  assert.equal(deliveries[1].redeliveries, 1);

  // And a widened default arms new subscriptions long: no requeue
  // inside 300ms once the default is 30s.
  bus.setDefaultAckTimeout(30_000);
  const late: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable('u', (d) => {
    late.push(d);
  });
  bus.publish('u', 'second');
  await flush();
  assert.equal(late.length, 1);
  await sleep(300);
  await flush();
  assert.equal(late.length, 1, 'a subscription created after widening must not requeue early');
});

test('an explicit ackTimeoutMs wins over the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultAckTimeout(30_000);
  // Explicit 40 beats a 30s default: the delivery requeues.
  const fast: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable(
    't',
    (d) => {
      fast.push(d);
      if (d.redeliveries >= 1) d.ack();
    },
    { ackTimeoutMs: 40 },
  );
  bus.publish('t', 'first');
  await flush();
  await sleep(150);
  await flush();
  assert.equal(fast.length, 2, 'an explicit timeout must beat a widened bus default');

  bus.setDefaultAckTimeout(60);
  // Explicit 5000 beats a 60ms default: no requeue inside 300ms.
  const slow: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable(
    'u',
    (d) => {
      slow.push(d);
    },
    { ackTimeoutMs: 5000 },
  );
  bus.publish('u', 'second');
  await flush();
  await sleep(300);
  await flush();
  assert.equal(slow.length, 1, 'an explicit timeout must beat a tightened bus default');
});

test('a default revision never moves an already-armed timer', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.redeliveries >= 1) d.ack();
    },
    { ackTimeoutMs: 40 },
  );
  bus.publish('t', 'first');
  await flush();
  assert.equal(deliveries.length, 1);
  // Widen the bus default far past the armed 40ms timer: the in-flight
  // delivery must still requeue on its original deadline.
  bus.setDefaultAckTimeout(30_000);
  await sleep(100);
  await flush();
  assert.equal(deliveries.length, 2, 'the in-flight delivery must keep its original deadline');
  assert.equal(deliveries[1].redeliveries, 1);
});

test('an existing reliable subscription keeps its baked tracker value after a default revision', async () => {
  const bus = new EventBus();
  const oldDeliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable('t', (d) => {
    oldDeliveries.push(d);
  });
  bus.publish('t', 'before');
  await flush();
  assert.equal(oldDeliveries.length, 1);

  // Tighten the bus default to 60ms: the existing subscriber was
  // created under the 5000ms factory default and keeps it.
  bus.setDefaultAckTimeout(60);
  bus.publish('t', 'after');
  await flush();
  assert.equal(oldDeliveries.length, 2);
  await sleep(300);
  await flush();
  assert.equal(
    oldDeliveries.length,
    2,
    'later deliveries on an existing subscription must still arm at its baked value',
  );

  // A subscriber created after the revision takes the new default.
  const newDeliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable(
    'u',
    (d) => {
      newDeliveries.push(d);
      if (d.redeliveries >= 1) d.ack();
    },
  );
  bus.publish('u', 'new');
  await flush();
  await sleep(150);
  await flush();
  assert.equal(newDeliveries.length, 2, 'a subscription created after the revision must use it');
  assert.equal(newDeliveries[1].redeliveries, 1);
});

test('setDefaultAckTimeout rejects illegal values fail-closed', () => {
  const bus = new EventBus();
  bus.setDefaultAckTimeout(30_000);
  for (const timeoutMs of [0, -5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(() => bus.setDefaultAckTimeout(timeoutMs), RangeError, `timeoutMs=${timeoutMs}`);
  }
  assert.throws(() => bus.setDefaultAckTimeout('5000' as never), RangeError);
  assert.throws(() => bus.setDefaultAckTimeout(100, ''), RangeError, 'an illegal actor throws');
  // The live default and the audit log are untouched: only the one
  // valid setup revision is recorded.
  assert.equal(bus.getDefaultAckTimeout(), 30_000);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_ack_timeout_revised' }).length,
    1,
  );
});

test('revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultAckTimeout(5000);
  assert.equal(bus.getSubscriptionAudit({ action: 'default_ack_timeout_revised' }).length, 0);
  bus.setDefaultAckTimeout(30_000);
  bus.setDefaultAckTimeout(30_000);
  const audit = bus.getSubscriptionAudit({ action: 'default_ack_timeout_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous_timeout_ms: 5000, timeout_ms: 30_000 });
});

test('setDefaultAckTimeout audits each revision with its old and new value', () => {
  const bus = new EventBus();
  bus.setDefaultAckTimeout(30_000, 'ops');
  bus.setDefaultAckTimeout(5000, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'default_ack_timeout_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous_timeout_ms: 5000, timeout_ms: 30_000 });
  assert.deepEqual(events[1].detail, { previous_timeout_ms: 30_000, timeout_ms: 5000 });
});

test('the getter follows the live value and setting 5000 restores the factory default', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultAckTimeout(), 5000);
  bus.setDefaultAckTimeout(30_000);
  assert.equal(bus.getDefaultAckTimeout(), 30_000);
  bus.setDefaultAckTimeout(5000);
  assert.equal(bus.getDefaultAckTimeout(), 5000);
  // Restored: a new subscription arms at 5000 again.
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable('t', (d) => {
    deliveries.push(d);
  });
  bus.publish('t', 'first');
  await flush();
  await sleep(300);
  await flush();
  assert.equal(deliveries.length, 1);
});

test('setAckTimeout still revises one subscriber after a default revision', async () => {
  const bus = new EventBus();
  bus.setDefaultAckTimeout(60);
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      if (d.redeliveries >= 1) d.ack();
    },
  );
  // The per-subscriber revision (EB-259) still governs this subscriber's
  // future deliveries, independent of the bus default.
  bus.setAckTimeout(sub.id, 5000);
  bus.publish('t', 'first');
  await flush();
  assert.equal(deliveries.length, 1);
  await sleep(300);
  await flush();
  assert.equal(deliveries.length, 1, 'the per-subscriber revision must beat the bus default');
});

test('the RPC reliable router inherits the bus default when the caller passes true', async () => {
  const bus = new EventBus();
  try {
    bus.setDefaultAckTimeout(60);
    let deliveries = 0;
    bus.subscribe('rpc.rel', (msg) => {
      deliveries += 1;
      bus.reply(msg, { n: deliveries });
    });
    const result = await settled(
      bus.requestResponse('rpc.rel', {}, { timeoutMs: 2000, reliable: true }),
    );
    assert.deepEqual(result, { n: 1 });
    // The router was created under the revised default and acked the
    // response: no redelivery storm followed.
    await sleep(300);
    assert.equal(deliveries, 1);
    assert.equal(bus.getStats().rpc.discardedResponses, 0);
  } finally {
    await bus.close();
  }
});

test('the RPC reliable router takes an explicit ackTimeoutMs when the caller passes one', async () => {
  const bus = new EventBus();
  try {
    bus.setDefaultAckTimeout(60);
    let deliveries = 0;
    bus.subscribe('rpc.rel', (msg) => {
      deliveries += 1;
      bus.reply(msg, { n: deliveries });
    });
    const result = await settled(
      bus.requestResponse(
        'rpc.rel',
        {},
        { timeoutMs: 2000, reliable: { ackTimeoutMs: 5000 } },
      ),
    );
    assert.deepEqual(result, { n: 1 });
    await sleep(300);
    assert.equal(deliveries, 1);
    assert.equal(bus.getStats().rpc.discardedResponses, 0);
  } finally {
    await bus.close();
  }
});
