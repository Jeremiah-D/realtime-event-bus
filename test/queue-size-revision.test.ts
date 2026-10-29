import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('setQueueSize rejects unknown, empty, and invalid revisions fail-closed', () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  for (const bad of [
    () => bus.setQueueSize('no-such-sub', 5),
    () => bus.setQueueSize('', 5),
    () => bus.setQueueSize(sub.id, 0),
    () => bus.setQueueSize(sub.id, -1),
    () => bus.setQueueSize(sub.id, 1.5),
    () => bus.setQueueSize(sub.id, Number.NaN),
    () => bus.setQueueSize(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setQueueSize(sub.id, undefined as never),
    () => bus.setQueueSize(sub.id, '3' as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'queue_size_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live capacity is untouched: still capped at 2 under drop-newest.
  assert.equal(bus.publish('t', 'm3'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  sub.unsubscribe();
});

test('setQueueSize shrink keeps queued messages and refuses new admissions until below the cap', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 5,
    backpressure: 'drop-newest',
  });
  for (let i = 1; i <= 4; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  bus.setQueueSize(sub.id, 3);
  // The shrink itself evicts nothing, even though the backlog is over the new cap.
  assert.equal(bus.pendingCount(sub.id), 4);
  const events = bus.getSubscriptionAudit({ action: 'queue_size_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.deepEqual(events[0].detail, { queueSize: 3, oldQueueSize: 5 });
  // At/above the new cap: refused, backlog untouched.
  assert.equal(bus.publish('t', 'm5'), 0);
  assert.equal(bus.pendingCount(sub.id), 4);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4']);
  // Below the cap again: admissions resume.
  assert.equal(bus.publish('t', 'm6'), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4', 'm6']);
  sub.unsubscribe();
});

test("setQueueSize shrink arms the default 'block' tier at once, without shedding", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 5 });
  for (let i = 1; i <= 4; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  bus.setQueueSize(sub.id, 3);
  assert.equal(bus.pendingCount(sub.id), 4);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  // Refused immediately — the next arrival does not shed a queued message first.
  assert.equal(bus.publish('t', 'm5'), 0);
  assert.equal(bus.pendingCount(sub.id), 4);
  assert.equal(bus.blockedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4']);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.publish('t', 'm6'), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4', 'm6']);
  sub.unsubscribe();
});

test('setQueueSize grow admits immediately with no drain required', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.publish('t', 'm3'), 0);
  bus.setQueueSize(sub.id, 5);
  for (let i = 3; i <= 5; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.pendingCount(sub.id), 5);
  const events = bus.getSubscriptionAudit({ action: 'queue_size_revised' });
  assert.deepEqual(events[0].detail, { queueSize: 5, oldQueueSize: 2 });
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4', 'm5']);
  sub.unsubscribe();
});

test('setQueueSize grow lifts a blocked excursion when the depth sits below the new mark', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3'); // sheds m1, saturates -> blocked
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.publish('t', 'm4'), 0);
  bus.setQueueSize(sub.id, 10);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.publish('t', 'm4'), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3', 'm4']);
  sub.unsubscribe();
});

test('setQueueSize null resets to the default 100', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.publish('t', 'm3'), 0);
  bus.setQueueSize(sub.id, null);
  const events = bus.getSubscriptionAudit({ action: 'queue_size_revised' });
  assert.deepEqual(events[0].detail, { queueSize: 100, oldQueueSize: 2 });
  for (let i = 3; i <= 12; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.pendingCount(sub.id), 12);
  await flush();
  assert.equal(received.length, 12);
  sub.unsubscribe();
});

test('setQueueSize same-value and null-when-already-default are no-ops with no audit', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { queueSize: 4, backpressure: 'drop-newest' });
  bus.setQueueSize(sub.id, 4);
  bus.setQueueSize(sub.id, 4);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'queue_size_revised' }).length,
    0,
    'restating the current capacity must not audit',
  );
  const plain = bus.subscribe('t2', () => {});
  bus.setQueueSize(plain.id, null);
  bus.setQueueSize(plain.id, 100);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'queue_size_revised' }).length,
    0,
    'null / 100 on a default-capacity subscriber must not audit',
  );
  sub.unsubscribe();
  plain.unsubscribe();
});

test('setQueueSize leaves dropPolicy, ratio, and byte budget untouched', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
    highWaterMarkRatio: 0.5,
    queueMaxBytes: 100000,
  });
  bus.setQueueSize(sub.id, 4);
  // Still drop-newest at the new cap: the 5th message is refused, not shed.
  for (let i = 1; i <= 4; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.publish('t', 'm5'), 0);
  assert.equal(bus.pendingCount(sub.id), 4);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4']);
  sub.unsubscribe();
});
