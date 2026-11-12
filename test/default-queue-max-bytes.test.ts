/**
 * Bus-level default queue byte budget live revision (EB-474).
 *
 * `setDefaultQueueMaxBytes` decides the byte budget for future
 * subscribers that omit their own `queueMaxBytes` (EB-83) — `1024`
 * to bound a fleet's per-subscriber buffered bytes with one call,
 * `null` to clear the default back to no byte dimension. The
 * priority chain is subscriber-explicit > bus default > no byte
 * dimension; a subscriber keeps the budget it was baked with, and
 * `setQueueMaxBytes(subId, null)` clears to the bus default in
 * effect at revision time (no byte dimension when unset). A shadow
 * queue that omits `queueMaxBytes` takes the same derived budget.
 * A revision identical to the current default is a no-op with no
 * audit. Audited as `default_queue_max_bytes_revised`
 * (`{ previous, queueMaxBytes }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

// A 10-character string payload serializes to 12 JSON bytes:
// `"xxxxxxxxxx"`. All budgets below are counted in those units.
const ten = (ch: string) => ch.repeat(10);

test('EB-474: the factory bus default is unset (new subscribers get no byte dimension)', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultQueueMaxBytes(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    backpressure: 'drop-newest',
  });
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.pendingCount(sub.id), 5);
  assert.equal(bus.getStats().queueBytes, 0);
  await flush();
  assert.equal(received.length, 5);
  sub.unsubscribe();
});

test('EB-474: a subscriber that omits queueMaxBytes takes the bus default and sheds over budget per drop-newest', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(24);
  assert.equal(bus.getDefaultQueueMaxBytes(), 24);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.publish('t', ten('c')), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.getStats().queueBytes, 24);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, [ten('a'), ten('b')]);
  sub.unsubscribe();
});

test('EB-474: an explicit queueMaxBytes beats the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(24);
  // Explicit 100000 beats a bus default of 24: all three fit.
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 100000,
    backpressure: 'drop-newest',
  });
  for (let i = 0; i < 3; i++) assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.pendingCount(sub.id), 3);
  await flush();
  assert.equal(received.length, 3);
  sub.unsubscribe();

  // Explicit 12 beats a bus default of 100000: the second is refused.
  const loud = new EventBus();
  loud.setDefaultQueueMaxBytes(100000);
  const loudSub = loud.subscribe('t', () => {}, {
    queueSize: 100,
    queueMaxBytes: 12,
    backpressure: 'drop-newest',
  });
  assert.equal(loud.publish('t', ten('a')), 1);
  assert.equal(loud.publish('t', ten('b')), 0);
  assert.equal(loud.pendingCount(loudSub.id), 1);
  assert.equal(loud.droppedCount(loudSub.id), 1);
  loudSub.unsubscribe();
});

test('EB-474: clearing with null returns new subscribers to no byte dimension', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(24);
  bus.setDefaultQueueMaxBytes(null);
  assert.equal(bus.getDefaultQueueMaxBytes(), undefined);
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    backpressure: 'drop-newest',
  });
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.pendingCount(sub.id), 5);
  assert.equal(bus.getStats().queueBytes, 0);
  sub.unsubscribe();
});

test('EB-474: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_queue_max_bytes_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultQueueMaxBytes(24);
  assert.equal(bus.getDefaultQueueMaxBytes(), 24);
  bus.setDefaultQueueMaxBytes(24);
  assert.equal(bus.getDefaultQueueMaxBytes(), 24);
  const audit = bus.getSubscriptionAudit({ action: 'default_queue_max_bytes_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, queueMaxBytes: 24 });
  bus.setDefaultQueueMaxBytes(null);
  assert.equal(bus.getDefaultQueueMaxBytes(), undefined);
});

test('EB-474: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(24);
  for (const bad of [0, -1, Number.NaN, Infinity, '24'] as unknown[]) {
    assert.throws(
      () => bus.setDefaultQueueMaxBytes(bad as number),
      RangeError,
      `bytes=${String(bad)}`,
    );
    assert.equal(bus.getDefaultQueueMaxBytes(), 24);
  }
  assert.throws(() => bus.setDefaultQueueMaxBytes(36, ''), RangeError);
  assert.equal(bus.getDefaultQueueMaxBytes(), 24);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_queue_max_bytes_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-474: revising the bus default never moves an already-subscribed budget', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(36);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  bus.setDefaultQueueMaxBytes(12);
  // The subscriber keeps the 36-byte budget it was baked with.
  assert.equal(bus.publish('t', ten('c')), 1);
  assert.equal(bus.publish('t', ten('d')), 0);
  assert.equal(bus.pendingCount(sub.id), 3);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, [ten('a'), ten('b'), ten('c')]);
  // A subscriber joining after the revision takes the new default.
  const late = bus.subscribe('u', () => {}, {
    queueSize: 100,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('u', ten('a')), 1);
  assert.equal(bus.publish('u', ten('b')), 0);
  assert.equal(bus.pendingCount(late.id), 1);
  late.unsubscribe();
  sub.unsubscribe();
});

test('EB-474: setQueueMaxBytes(null) clears to the current bus default, and to no byte dimension when no default is set', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 36,
    backpressure: 'drop-newest',
  });
  bus.setDefaultQueueMaxBytes(24);
  bus.setQueueMaxBytes(sub.id, null);
  const cleared = bus.getSubscriptionAudit({ action: 'queue_max_bytes_revised' });
  assert.equal(cleared.length, 1);
  assert.deepEqual(cleared[0].detail, { queueMaxBytes: 24, oldQueueMaxBytes: 36 });
  // Still capped at the default window of 24: the third is refused.
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  assert.equal(bus.publish('t', ten('c')), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  // ...and the re-armed budget stays revisable.
  bus.setQueueMaxBytes(sub.id, 48);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'queue_max_bytes_revised' }).length,
    2,
    'a null-clear to a default leaves the budget revisable',
  );
  await flush();
  assert.deepEqual(received, [ten('a'), ten('b')]);
  sub.unsubscribe();

  // No bus default: the clear target is no byte dimension (the old behavior).
  const plain = new EventBus();
  const plainReceived: unknown[] = [];
  const plainSub = plain.subscribe('t', (msg) => plainReceived.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 12,
    backpressure: 'drop-newest',
  });
  assert.equal(plain.publish('t', ten('a')), 1);
  assert.equal(plain.publish('t', ten('b')), 0);
  plain.setQueueMaxBytes(plainSub.id, null);
  const plainEvents = plain.getSubscriptionAudit({ action: 'queue_max_bytes_revised' });
  assert.deepEqual(plainEvents[0].detail, { queueMaxBytes: null, oldQueueMaxBytes: 12 });
  for (let i = 0; i < 5; i++) assert.equal(plain.publish('t', ten('b')), 1);
  assert.equal(plain.pendingCount(plainSub.id), 6);
  assert.equal(plain.getStats().queueBytes, 0);
  await flush();
  assert.equal(plainReceived.length, 6);
  // The EB-279/EB-280 clear rule still applies with no default installed.
  assert.throws(() => plain.setQueueMaxBytes(plainSub.id, 12), RangeError);
  assert.throws(() => plain.setQueueMaxBytes(plainSub.id, null), RangeError);
  assert.equal(
    plain.getSubscriptionAudit({ action: 'queue_max_bytes_revised' }).length,
    1,
    'refused post-clear revisions must not audit',
  );
  plainSub.unsubscribe();
});

test('EB-474: a shadow queue that omits queueMaxBytes takes the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(24);
  const prod: unknown[] = [];
  const shadow: unknown[] = [];
  bus.subscribe('t', (msg) => prod.push(msg.payload), {
    queueSize: 100,
    queueMaxBytes: 100000,
  });
  const shadowSub = bus.subscribe(
    't',
    (msg) => {
      shadow.push(msg.payload);
    },
    { shadow: { dropPolicy: 'drop-newest' } },
  );
  bus.publish('t', ten('a'));
  bus.publish('t', ten('b'));
  bus.publish('t', ten('c'));
  assert.equal(
    bus.getStats().shadowDroppedMessages,
    1,
    'the 3rd mirror copy is refused at the bus-default budget of 24',
  );
  await flush();
  assert.deepEqual(prod, [ten('a'), ten('b'), ten('c')]);
  assert.deepEqual(shadow, [ten('a'), ten('b')]);
  shadowSub.unsubscribe();
});

test('EB-474: an explicit shadow queueMaxBytes still wins over the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(24);
  bus.subscribe('t', () => {}, { queueSize: 100, queueMaxBytes: 100000 });
  const shadowSub = bus.subscribe('t', () => {}, {
    queueMaxBytes: 12,
    shadow: { dropPolicy: 'drop-newest' },
  });
  bus.publish('t', ten('a'));
  bus.publish('t', ten('b'));
  assert.equal(
    bus.getStats().shadowDroppedMessages,
    1,
    'explicit shadow queueMaxBytes 12 beats the bus default of 24',
  );
  await flush();
  shadowSub.unsubscribe();
});

test('EB-474: shrinking the bus default evicts nothing from a baked backlog, and new admissions follow the new budget', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(36);
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 100,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  bus.setDefaultQueueMaxBytes(12);
  // The shrink never moves the already-subscribed budget: the backlog
  // stays, and the subscriber even grows to its baked 36.
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.publish('t', ten('c')), 1);
  assert.equal(bus.pendingCount(sub.id), 3);
  assert.equal(bus.getStats().queueBytes, 36);
  // ...while a subscriber joining after the revision takes the new default.
  const late = bus.subscribe('u', () => {}, {
    queueSize: 100,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('u', ten('a')), 1);
  assert.equal(bus.publish('u', ten('b')), 0);
  assert.equal(bus.pendingCount(late.id), 1);
  late.unsubscribe();
  sub.unsubscribe();
});

test('EB-474: byte-budget sheds under the bus default never arm the default block tier', async () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(24);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 100,
  });
  assert.equal(bus.publish('t', ten('a')), 1);
  assert.equal(bus.publish('t', ten('b')), 1);
  // Byte-budget sheds never arm the block tier: no refusal latch, and
  // the newcomer is admitted by shedding the oldest, exactly as at
  // subscribe time.
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.publish('t', ten('c')), 1);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, [ten('b'), ten('c')]);
  sub.unsubscribe();
});

test('EB-474: setDefaultQueueMaxBytes audits each revision with its old and new budget and actor', () => {
  const bus = new EventBus();
  bus.setDefaultQueueMaxBytes(24, 'ops');
  bus.setDefaultQueueMaxBytes(48, 'ops');
  bus.setDefaultQueueMaxBytes(null, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'default_queue_max_bytes_revised' });
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, queueMaxBytes: 24 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 24, queueMaxBytes: 48 });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 48, queueMaxBytes: null });
  assert.equal(bus.getDefaultQueueMaxBytes(), undefined);
});
