import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

// EB-470: bus-level terminal queue-size default live revision.

test('EB-470: the factory bus default is unset (new subscribers fall back to 100)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultQueueSize(), undefined);
});

test('EB-470: a subscriber that omits queueSize takes the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(3);
  assert.equal(bus.getDefaultQueueSize(), 3);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    backpressure: 'drop-newest',
  });
  for (let i = 1; i <= 3; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  // At the bus-default cap: the 4th message is refused, backlog untouched.
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.pendingCount(sub.id), 3);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3']);
  sub.unsubscribe();
});

test('EB-470: an explicit queueSize beats the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(1);
  const received: unknown[] = [];
  // Explicit 3 beats a bus default of 1: all three admitted.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 3,
    backpressure: 'drop-newest',
  });
  for (let i = 1; i <= 3; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.pendingCount(sub.id), 3);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3']);
  sub.unsubscribe();

  // Explicit 1 beats a bus default of 3: only one admitted.
  bus.setDefaultQueueSize(3);
  const second: unknown[] = [];
  const sub2 = bus.subscribe('u', (msg) => second.push(msg.payload), {
    queueSize: 1,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('u', 'm1'), 1);
  assert.equal(bus.publish('u', 'm2'), 0);
  assert.equal(bus.publish('u', 'm3'), 0);
  assert.equal(bus.pendingCount(sub2.id), 1);
  await flush();
  assert.deepEqual(second, ['m1']);
  sub2.unsubscribe();
});

test('EB-470: clearing with null returns new subscribers to the documented 100', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  bus.setDefaultQueueSize(null);
  assert.equal(bus.getDefaultQueueSize(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    backpressure: 'drop-newest',
  });
  // Five admitted: the cap is 100 again, not the cleared 2.
  for (let i = 1; i <= 5; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.pendingCount(sub.id), 5);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4', 'm5']);
  sub.unsubscribe();
});

test('EB-470: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_queue_size_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultQueueSize(4);
  assert.equal(bus.getDefaultQueueSize(), 4);
  bus.setDefaultQueueSize(4);
  assert.equal(bus.getDefaultQueueSize(), 4);
  const audit = bus.getSubscriptionAudit({ action: 'default_queue_size_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, queueSize: 4 });
  bus.setDefaultQueueSize(null);
  assert.equal(bus.getDefaultQueueSize(), undefined);
});

test('EB-470: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  for (const bad of [0, -1, 1.5, Number.NaN, Infinity, '3'] as unknown[]) {
    assert.throws(
      () => bus.setDefaultQueueSize(bad as number),
      RangeError,
      `n=${String(bad)}`,
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

test('EB-470: revising the bus default never moves an already-subscribed capacity', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    backpressure: 'drop-newest',
  });
  bus.setDefaultQueueSize(5);
  for (let i = 1; i <= 4; i++) {
    assert.equal(bus.publish('t', `m${i}`), i <= 2 ? 1 : 0);
  }
  assert.equal(bus.pendingCount(sub.id), 2, 'the subscriber keeps the capacity it was baked with');
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  sub.unsubscribe();
});

test('EB-470: setQueueSize(null) clears to the current bus default, and to 100 when no default is set', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  bus.setDefaultQueueSize(5);
  bus.setQueueSize(sub.id, null);
  const events = bus.getSubscriptionAudit({ action: 'queue_size_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, { queueSize: 5, oldQueueSize: 2 });
  for (let i = 1; i <= 5; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.publish('t', 'm6'), 0);
  assert.equal(bus.pendingCount(sub.id), 5);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4', 'm5']);
  sub.unsubscribe();

  // No bus default: the clear target is the documented 100.
  const plain = new EventBus();
  const plainSub = plain.subscribe('t', () => {}, {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  plain.setQueueSize(plainSub.id, null);
  const plainEvents = plain.getSubscriptionAudit({ action: 'queue_size_revised' });
  assert.deepEqual(plainEvents[0].detail, { queueSize: 100, oldQueueSize: 2 });
  plainSub.unsubscribe();
});

test('EB-470: a shadow queue that omits queueSize takes the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(3);
  const prod: unknown[] = [];
  const shadow: unknown[] = [];
  bus.subscribe(
    't',
    (msg) => {
      prod.push(msg.payload);
    },
    { queueSize: 100 },
  );
  const shadowSub = bus.subscribe(
    't',
    (msg) => {
      shadow.push(msg.payload);
    },
    { shadow: { dropPolicy: 'drop-newest' } },
  );
  for (let n = 1; n <= 4; n++) bus.publish('t', n);
  assert.equal(
    bus.getStats().shadowDroppedMessages,
    1,
    'the 4th mirror copy is refused at the bus-default cap of 3',
  );
  await flush();
  assert.deepEqual(prod, [1, 2, 3, 4]);
  assert.deepEqual(shadow, [1, 2, 3]);
  shadowSub.unsubscribe();
});

test('EB-470: an explicit shadow queueSize still wins over the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(3);
  bus.subscribe('t', () => {}, { queueSize: 100 });
  const shadowSub = bus.subscribe('t', () => {}, {
    shadow: { queueSize: 2, dropPolicy: 'drop-newest' },
  });
  for (let n = 1; n <= 4; n++) bus.publish('t', n);
  assert.equal(
    bus.getStats().shadowDroppedMessages,
    2,
    'explicit shadow queueSize 2 beats the bus default of 3',
  );
  await flush();
  shadowSub.unsubscribe();
});

test('EB-470: the high-water mark follows the bus-default capacity', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(4);
  const pressure: Array<{ queueSize: number; capacity: number }> = [];
  const sub = bus.subscribe('t', () => {}, {
    onBackpressure: (event) => {
      pressure.push({ queueSize: event.queueSize, capacity: event.capacity });
    },
  });
  for (let i = 1; i <= 4; i++) bus.publish('t', `m${i}`);
  assert.equal(pressure.length, 1, 'the excursion fires when the depth reaches 4 * 0.8');
  assert.deepEqual(pressure[0], { queueSize: 4, capacity: 4 });
  await flush();
  sub.unsubscribe();

  // Without a bus default the mark sits at 80: four messages cross nothing.
  const plain = new EventBus();
  const plainPressure: unknown[] = [];
  const plainSub = plain.subscribe('t', () => {}, {
    onBackpressure: () => plainPressure.push(1),
  });
  for (let i = 1; i <= 4; i++) plain.publish('t', `m${i}`);
  assert.equal(plainPressure.length, 0);
  await flush();
  plainSub.unsubscribe();
});

test('EB-470: setDefaultQueueSize audits each revision with its old and new size and actor', () => {
  const bus = new EventBus();
  bus.setDefaultQueueSize(2, 'ops');
  bus.setDefaultQueueSize(5, 'ops');
  bus.setDefaultQueueSize(null, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'default_queue_size_revised' });
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, queueSize: 2 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 2, queueSize: 5 });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 5, queueSize: null });
});
