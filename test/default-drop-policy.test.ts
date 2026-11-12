/**
 * Bus-level default drop policy live revision (EB-475).
 *
 * `setDefaultDropPolicy` decides the queue shed policy for future
 * subscribers that omit their own `dropPolicy` under the 'block'
 * backpressure tier (EB-83) — `'drop-newest'` to shed fresh arrivals
 * fleet-wide with one call, `null` to clear the default back to the
 * historical `'drop-oldest'`. The priority chain is
 * subscriber-explicit > bus default > 'drop-oldest' for the 'block'
 * branch; an explicit drop tier is the policy itself and always beats
 * the bus default. A subscriber keeps the policy it was baked with,
 * and `setDropPolicy` (EB-375) still revises such a subscriber one
 * at a time without touching the bus default. A shadow queue that
 * omits `dropPolicy` takes the same derived policy. A revision
 * identical to the current default is a no-op with no audit. Audited
 * as `default_drop_policy_revised` (`{ previous, dropPolicy }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_drop_policy_revised' });

test("EB-475: set drop-newest — a later 'block' subscriber that omits dropPolicy takes the default and sheds the newcomer", async () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy('drop-newest');
  assert.equal(bus.getDefaultDropPolicy(), 'drop-newest');
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // First saturation under the default drop-newest: the newcomer is
  // refused, the backlog intact — and the 'block' latch arms.
  assert.equal(bus.publish('t', 'm3'), 0);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  sub.unsubscribe();
});

test('EB-475: an explicit dropPolicy beats the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy('drop-newest');
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    dropPolicy: 'drop-oldest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // Explicit drop-oldest beats the drop-newest default: the newcomer
  // is admitted, the oldest shed.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  sub.unsubscribe();

  // Explicit drop-newest beats a drop-oldest default: the newcomer is refused.
  const loud = new EventBus();
  loud.setDefaultDropPolicy('drop-oldest');
  const loudReceived: unknown[] = [];
  const loudSub = loud.subscribe('t', (msg) => loudReceived.push(msg.payload), {
    queueSize: 2,
    dropPolicy: 'drop-newest',
  });
  assert.equal(loud.publish('t', 'm1'), 1);
  assert.equal(loud.publish('t', 'm2'), 1);
  assert.equal(loud.publish('t', 'm3'), 0);
  assert.equal(loud.pendingCount(loudSub.id), 2);
  assert.equal(loud.droppedCount(loudSub.id), 1);
  await flush();
  assert.deepEqual(loudReceived, ['m1', 'm2']);
  loudSub.unsubscribe();
});

test('EB-475: an explicit drop tier beats the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy('drop-newest');
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // The explicit drop-oldest tier is the policy itself: the newcomer
  // is admitted despite the drop-newest default.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  sub.unsubscribe();

  // The explicit drop-newest tier beats a drop-oldest default.
  const loud = new EventBus();
  loud.setDefaultDropPolicy('drop-oldest');
  const loudReceived: unknown[] = [];
  const loudSub = loud.subscribe('t', (msg) => loudReceived.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(loud.publish('t', 'm1'), 1);
  assert.equal(loud.publish('t', 'm2'), 1);
  assert.equal(loud.publish('t', 'm3'), 0);
  assert.equal(loud.pendingCount(loudSub.id), 2);
  assert.equal(loud.droppedCount(loudSub.id), 1);
  await flush();
  assert.deepEqual(loudReceived, ['m1', 'm2']);
  loudSub.unsubscribe();
});

test("EB-475: the factory default is unset and clearing with null returns new subscribers to 'drop-oldest'", async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDropPolicy(), undefined);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // No default: the historical 'drop-oldest' — the newcomer is admitted.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  sub.unsubscribe();

  bus.setDefaultDropPolicy('drop-newest');
  bus.setDefaultDropPolicy(null);
  assert.equal(bus.getDefaultDropPolicy(), undefined);
  const cleared: unknown[] = [];
  const clearedSub = bus.subscribe('u', (msg) => cleared.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('u', 'm1'), 1);
  assert.equal(bus.publish('u', 'm2'), 1);
  assert.equal(bus.publish('u', 'm3'), 1);
  await flush();
  assert.deepEqual(cleared, ['m2', 'm3']);
  clearedSub.unsubscribe();
});

test('EB-475: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy(null);
  assert.equal(
    defaultAudits(bus).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultDropPolicy('drop-newest');
  assert.equal(bus.getDefaultDropPolicy(), 'drop-newest');
  bus.setDefaultDropPolicy('drop-newest');
  assert.equal(bus.getDefaultDropPolicy(), 'drop-newest');
  const audit = defaultAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, dropPolicy: 'drop-newest' });
  bus.setDefaultDropPolicy(null);
  assert.equal(bus.getDefaultDropPolicy(), undefined);
  bus.setDefaultDropPolicy(null);
  assert.equal(defaultAudits(bus).length, 2, 're-clearing audits nothing');
});

test('EB-475: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy('drop-newest');
  for (const bad of [
    'block',
    'BLOCK',
    'drop',
    '',
    undefined,
    42,
    {},
    ['drop-newest'],
  ] as unknown[]) {
    assert.throws(
      () => bus.setDefaultDropPolicy(bad as never),
      RangeError,
      `policy=${String(bad)}`,
    );
    assert.equal(bus.getDefaultDropPolicy(), 'drop-newest');
  }
  assert.throws(() => bus.setDefaultDropPolicy('drop-oldest', ''), RangeError);
  assert.equal(bus.getDefaultDropPolicy(), 'drop-newest');
  assert.equal(
    defaultAudits(bus).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-475: revising the bus default never moves an already-subscribed policy', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  bus.setDefaultDropPolicy('drop-newest');
  // The subscriber keeps the 'drop-oldest' it was baked with: the
  // newcomer is admitted, the oldest shed.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  // A subscriber joining after the revision takes the new default.
  const lateReceived: unknown[] = [];
  const late = bus.subscribe('u', (msg) => lateReceived.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('u', 'm1'), 1);
  assert.equal(bus.publish('u', 'm2'), 1);
  assert.equal(bus.publish('u', 'm3'), 0);
  assert.equal(bus.pendingCount(late.id), 2);
  assert.equal(bus.droppedCount(late.id), 1);
  await flush();
  assert.deepEqual(lateReceived, ['m1', 'm2']);
  late.unsubscribe();
  sub.unsubscribe();
});

test('EB-475: setDropPolicy still revises one subscriber at a time and leaves the bus default in place', async () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy('drop-newest');
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  bus.setDropPolicy(sub.id, 'drop-oldest');
  // The per-subscriber revision governs from the next push: the
  // newcomer is admitted, the oldest shed.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  // The bus default is untouched by the per-subscriber revision.
  assert.equal(bus.getDefaultDropPolicy(), 'drop-newest');
  const policyEvents = bus.getSubscriptionAudit({ action: 'drop_policy_revised' });
  assert.equal(policyEvents.length, 1);
  assert.deepEqual(policyEvents[0].detail, {
    dropPolicy: 'drop-oldest',
    oldDropPolicy: 'drop-newest',
  });
  sub.unsubscribe();
});

test('EB-475: a shadow queue that omits dropPolicy takes the bus default, and an explicit shadow value still wins', async () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy('drop-newest');
  bus.subscribe('t', () => {}, { queueSize: 100 });
  const shadow: unknown[] = [];
  const shadowSub = bus.subscribe(
    't',
    (msg) => {
      shadow.push(msg.payload);
    },
    { shadow: { queueSize: 2 } },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  assert.equal(
    bus.getStats().shadowDroppedMessages,
    1,
    'the 3rd mirror copy is refused under the bus-default drop-newest',
  );
  await flush();
  assert.deepEqual(shadow, ['a', 'b']);
  shadowSub.unsubscribe();

  // An explicit shadow dropPolicy still wins over the bus default.
  const loud = new EventBus();
  loud.setDefaultDropPolicy('drop-newest');
  loud.subscribe('t', () => {}, { queueSize: 100 });
  const loudShadow: unknown[] = [];
  const loudShadowSub = loud.subscribe(
    't',
    (msg) => {
      loudShadow.push(msg.payload);
    },
    { shadow: { queueSize: 2, dropPolicy: 'drop-oldest' } },
  );
  loud.publish('t', 'a');
  loud.publish('t', 'b');
  loud.publish('t', 'c');
  assert.equal(loud.getStats().shadowDroppedMessages, 1);
  await flush();
  assert.deepEqual(loudShadow, ['b', 'c']);
  loudShadowSub.unsubscribe();
});

test('EB-475: byte-budget sheds under the bus default pick their victim by the default policy', async () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy('drop-newest');
  const received: unknown[] = [];
  // String payloads measure as their JSON length: '"aa"' is 4 bytes, so
  // two messages exactly fill the 8-byte budget with count headroom.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 10,
    queueMaxBytes: 8,
  });
  assert.equal(bus.publish('t', 'aa'), 1);
  assert.equal(bus.publish('t', 'bb'), 1);
  // Over budget under the default drop-newest: the newcomer is refused.
  assert.equal(bus.publish('t', 'cc'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['aa', 'bb']);
  sub.unsubscribe();

  // Under a drop-oldest default the same over-budget push sheds the oldest.
  const old = new EventBus();
  old.setDefaultDropPolicy('drop-oldest');
  const oldReceived: unknown[] = [];
  const oldSub = old.subscribe('t', (msg) => oldReceived.push(msg.payload), {
    queueSize: 10,
    queueMaxBytes: 8,
  });
  assert.equal(old.publish('t', 'aa'), 1);
  assert.equal(old.publish('t', 'bb'), 1);
  assert.equal(old.publish('t', 'cc'), 1);
  assert.equal(old.pendingCount(oldSub.id), 2);
  assert.equal(old.droppedCount(oldSub.id), 1);
  await flush();
  assert.deepEqual(oldReceived, ['bb', 'cc']);
  oldSub.unsubscribe();
});

test('EB-475: setDefaultDropPolicy audits each revision with its old and new policy and actor', () => {
  const bus = new EventBus();
  bus.setDefaultDropPolicy('drop-newest', 'ops');
  bus.setDefaultDropPolicy('drop-oldest', 'ops');
  bus.setDefaultDropPolicy(null, 'ops');
  const events = defaultAudits(bus);
  assert.equal(events.length, 3);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, dropPolicy: 'drop-newest' });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 'drop-newest', dropPolicy: 'drop-oldest' });
  assert.equal(events[2].actor, 'ops');
  assert.deepEqual(events[2].detail, { previous: 'drop-oldest', dropPolicy: null });
  assert.equal(bus.getDefaultDropPolicy(), undefined);
});
