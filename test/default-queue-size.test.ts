import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

// EB-470: bus-level terminal queue-size default live revision.

test('EB-470: the factory bus default is unset (new subscriptions fall back to 100)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultQueueSize(), undefined);
});

test('EB-470: a subscriber that omits queueSize takes the bus default, live capacity included', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  const seen: Array<{ queueSize: number; capacity: number }> = [];
  const sub = bus.subscribe('t', () => {}, {
    backpressure: 'drop-newest',
    onBackpressure: (info) => seen.push({ queueSize: info.queueSize, capacity: info.capacity }),
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // The third arrival is refused: the subscriber was baked with capacity 2.
  assert.equal(bus.publish('t', 'm3'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  // The excursion fired at the default-derived capacity, not at 100.
  assert.equal(seen.length, 1);
  assert.equal(seen[0].capacity, 2);
  assert.equal(seen[0].queueSize, 2);
  sub.unsubscribe();
});

test('EB-470: an explicit queueSize beats the bus default in both directions', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  const received: unknown[] = [];
  // Explicit 5 beats a bus default of 2: all five admitted.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 5,
    backpressure: 'drop-newest',
  });
  for (let i = 1; i <= 5; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.publish('t', 'm6'), 0);
  assert.equal(bus.pendingCount(sub.id), 5);
  sub.unsubscribe();

  // Explicit 1 beats a bus default of 5: only one admitted.
  bus.setDefaultQueueSize(5);
  const sub2 = bus.subscribe('u', () => {}, { queueSize: 1, backpressure: 'drop-newest' });
  assert.equal(bus.publish('u', 'm1'), 1);
  assert.equal(bus.publish('u', 'm2'), 0);
  assert.equal(bus.pendingCount(sub2.id), 1);
  sub2.unsubscribe();
});

test('EB-470: clearing with null returns new subscribers to 100', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  bus.setDefaultQueueSize(null);
  assert.equal(bus.getDefaultQueueSize(), undefined);
  const sub = bus.subscribe('t', () => {}, { backpressure: 'drop-newest' });
  for (let i = 1; i <= 10; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.pendingCount(sub.id), 10);
  sub.unsubscribe();
});

test('EB-470: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_queue_size_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultQueueSize(4);
  bus.setDefaultQueueSize(4);
  const audit = bus.getSubscriptionAudit({ action: 'default_queue_size_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, queueSize: 4 });
});

test('EB-470: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  for (const bad of [0, -1, 1.5, Number.NaN, Infinity, '3'] as unknown[]) {
    assert.throws(
      () => bus.setDefaultQueueSize(bad as number),
      RangeError,
      `size=${String(bad)}`,
    );
    assert.equal(bus.getDefaultQueueSize(), 2);
  }
  assert.throws(() => bus.setDefaultQueueSize(3, ''), RangeError);
  assert.equal(bus.getDefaultQueueSize(), 2);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_queue_size_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-470: revising the bus default never moves an already-subscribed capacity', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  const sub = bus.subscribe('t', () => {}, { backpressure: 'drop-newest' });
  bus.setDefaultQueueSize(5);
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.publish('t', 'm3'), 0, 'the subscriber keeps the capacity it was baked with');
  assert.equal(bus.pendingCount(sub.id), 2);
  sub.unsubscribe();
});

test('EB-470: setQueueSize(null) clears back to the bus default in effect at revision time', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(7);
  const sub = bus.subscribe('t', () => {}, { queueSize: 2, backpressure: 'drop-newest' });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.publish('t', 'm3'), 0);
  bus.setQueueSize(sub.id, null);
  const events = bus.getSubscriptionAudit({ action: 'queue_size_revised' });
  assert.deepEqual(events[0].detail, { queueSize: 7, oldQueueSize: 2 });
  for (let i = 3; i <= 7; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.publish('t', 'm8'), 0);
  assert.equal(bus.pendingCount(sub.id), 7);
  sub.unsubscribe();

  // With no bus default set, null clears back to 100 as before.
  const plain = bus.subscribe('u', () => {}, { queueSize: 2, backpressure: 'drop-newest' });
  bus.setDefaultQueueSize(null);
  bus.setQueueSize(plain.id, null);
  const cleared = bus.getSubscriptionAudit({ action: 'queue_size_revised' });
  assert.deepEqual(cleared[cleared.length - 1].detail, { queueSize: 100, oldQueueSize: 2 });
  plain.unsubscribe();
});

test('EB-470: a shadow that omits queueSize takes the bus default, an explicit shadow queueSize still wins', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  bus.subscribe('t', () => {});
  bus.subscribe('t', () => {}, { shadow: { dropPolicy: 'drop-newest' } });
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.publish('t', 3);
  await flush();
  // The mirror was baked with the bus default of 2: two copies delivered, one shed.
  assert.equal(bus.getStats().shadowedMessages, 2);
  assert.equal(bus.getStats().shadowDroppedMessages, 1);

  // An explicit shadow queueSize beats the bus default.
  bus.subscribe('u', () => {});
  bus.subscribe('u', () => {}, { shadow: { queueSize: 5, dropPolicy: 'drop-newest' } });
  bus.publish('u', 1);
  bus.publish('u', 2);
  bus.publish('u', 3);
  await flush();
  assert.equal(bus.getStats().shadowedMessages, 2 + 3);
  assert.equal(bus.getStats().shadowDroppedMessages, 1);
});

test('EB-470: a smaller bus default moves the high-water mark of new subscribers with it', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3'); // sheds m1, saturates -> blocked, exactly as with an explicit queueSize of 2
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.publish('t', 'm4'), 0);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  sub.unsubscribe();
});

test('EB-470: setDefaultQueueSize audits each revision with its old and new capacity and actor', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2, 'ops');
  assert.equal(bus.getDefaultQueueSize(), 2);
  bus.setDefaultQueueSize(5, 'ops');
  assert.equal(bus.getDefaultQueueSize(), 5);
  bus.setDefaultQueueSize(null, 'ops');
  assert.equal(bus.getDefaultQueueSize(), undefined);
  const events = bus.getSubscriptionAudit({ action: 'default_queue_size_revised' });
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, queueSize: 2 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 2, queueSize: 5 });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 5, queueSize: null });
});
