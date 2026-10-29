import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

// A 10-character string payload serializes to 12 JSON bytes:
// `"xxxxxxxxxx"`. All budgets below are counted in those units.
const ten = (ch: string) => ch.repeat(10);

test('setQueueMaxBytes rejects unknown, empty, unset-budget, and invalid revisions fail-closed', () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 24,
    backpressure: 'drop-newest',
  });
  const plain = bus.subscribe('t2', () => {});
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  for (const bad of [
    () => bus.setQueueMaxBytes('no-such-sub', 12),
    () => bus.setQueueMaxBytes('', 12),
    () => bus.setQueueMaxBytes(plain.id, 12),
    () => bus.setQueueMaxBytes(sub.id, 0),
    () => bus.setQueueMaxBytes(sub.id, -1),
    () => bus.setQueueMaxBytes(sub.id, Number.NaN),
    () => bus.setQueueMaxBytes(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setQueueMaxBytes(sub.id, undefined as never),
    () => bus.setQueueMaxBytes(sub.id, '12' as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'queue_max_bytes_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live budget is untouched: still capped at 24 under drop-newest.
  assert.equal(bus.publish('t', ten('c')), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('setQueueMaxBytes shrink keeps queued bytes and refuses new admissions under drop-newest', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 36,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  bus.setQueueMaxBytes(sub.id, 12);
  // The shrink itself evicts nothing, even though the backlog is over the new budget.
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.getStats().queueBytes, 24);
  const events = bus.getSubscriptionAudit({ action: 'queue_max_bytes_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.deepEqual(events[0].detail, { queueMaxBytes: 12, oldQueueMaxBytes: 36 });
  // Over the new budget: refused, backlog untouched.
  assert.equal(bus.publish('t', ten('c')), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.getStats().queueBytes, 24);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, [ten('a'), ten('b')]);
  // Below the budget again: admissions resume.
  assert.equal(bus.publish('t', ten('d')), 1);
  await flush();
  assert.deepEqual(received, [ten('a'), ten('b'), ten('d')]);
  sub.unsubscribe();
});

test('setQueueMaxBytes shrink sheds the oldest backlog per drop-oldest', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 36,
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  bus.setQueueMaxBytes(sub.id, 24);
  assert.equal(bus.pendingCount(sub.id), 2);
  // 24 buffered + 12 incoming > 24: the oldest is shed to make room.
  assert.equal(bus.publish('t', ten('c')), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.getStats().queueBytes, 24);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, [ten('b'), ten('c')]);
  sub.unsubscribe();
});

test('setQueueMaxBytes grow admits immediately with no drain required', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 12,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 0);
  bus.setQueueMaxBytes(sub.id, 24);
  assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  const events = bus.getSubscriptionAudit({ action: 'queue_max_bytes_revised' });
  assert.deepEqual(events[0].detail, { queueMaxBytes: 24, oldQueueMaxBytes: 12 });
  await flush();
  assert.deepEqual(received, [ten('a'), ten('b')]);
  sub.unsubscribe();
});

test('setQueueMaxBytes null clears to no budget, and a revision after the clear throws', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 12,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 0);
  bus.setQueueMaxBytes(sub.id, null);
  const events = bus.getSubscriptionAudit({ action: 'queue_max_bytes_revised' });
  assert.deepEqual(events[0].detail, { queueMaxBytes: null, oldQueueMaxBytes: 12 });
  // No byte dimension left: large admissions are constrained only by the count cap.
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.pendingCount(sub.id), 6);
  assert.equal(bus.getStats().queueBytes, 0);
  await flush();
  assert.equal(received.length, 6);
  // The cleared budget cannot be revised back without re-subscribing.
  assert.throws(() => bus.setQueueMaxBytes(sub.id, 12), RangeError);
  assert.throws(() => bus.setQueueMaxBytes(sub.id, null), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'queue_max_bytes_revised' }).length,
    1,
    'refused post-clear revisions must not audit',
  );
  sub.unsubscribe();
});

test('setQueueMaxBytes same-value is a no-op with no audit', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    queueMaxBytes: 24,
    backpressure: 'drop-newest',
  });
  bus.setQueueMaxBytes(sub.id, 24);
  bus.setQueueMaxBytes(sub.id, 24);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'queue_max_bytes_revised' }).length,
    0,
    'restating the current budget must not audit',
  );
  // The live budget still enforces: a third 12-byte message is refused.
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.publish('t', ten('c')), 0);
  sub.unsubscribe();
});

test('setQueueMaxBytes leaves queueSize, dropPolicy, and ratio untouched', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    queueMaxBytes: 100000,
    backpressure: 'drop-newest',
    highWaterMarkRatio: 0.5,
  });
  bus.setQueueMaxBytes(sub.id, 24);
  // Still drop-newest at the count cap of 2: the 3rd message is refused, not shed.
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.publish('t', ten('c')), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  await flush();
  assert.deepEqual(received, [ten('a'), ten('b')]);
  sub.unsubscribe();
});

test('setQueueMaxBytes shrink under the default block tier sheds per policy without arming block', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 36,
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  bus.setQueueMaxBytes(sub.id, 24);
  // Byte-budget sheds never arm the block tier: no refusal latch, and the
  // newcomer is admitted by shedding the oldest, exactly as at subscribe time.
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.publish('t', ten('c')), 1);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, [ten('b'), ten('c')]);
  sub.unsubscribe();
});
