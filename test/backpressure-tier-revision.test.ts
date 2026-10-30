import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const tierAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'backpressure_tier_revised' });

test('setBackpressureTier rejects unknown, empty, and invalid tiers fail-closed', () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  for (const bad of [
    () => bus.setBackpressureTier('no-such-sub', 'drop-oldest'),
    () => bus.setBackpressureTier('', 'drop-oldest'),
    () => bus.setBackpressureTier(sub.id, 'drop' as never),
    () => bus.setBackpressureTier(sub.id, 'BLOCK' as never),
    () => bus.setBackpressureTier(sub.id, '' as never),
    () => bus.setBackpressureTier(sub.id, undefined as never),
    () => bus.setBackpressureTier(sub.id, null as never),
    () => bus.setBackpressureTier(sub.id, 42 as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(tierAudits(bus).length, 0, 'failed revisions must not audit');
  // The live tier is untouched: still drop-newest, so the newcomer is refused.
  assert.equal(bus.publish('t', 'm3'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  sub.unsubscribe();
});

test('setBackpressureTier revises the tier, audits, and same-value is a no-op', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  // A default subscription has no explicit tier at subscribe time, yet
  // it still carries a resolved one — there is no opt-in gate.
  const plain = bus.subscribe('t2', () => {});
  bus.setBackpressureTier(plain.id, 'drop-oldest');
  const plainEvents = tierAudits(bus).filter((e) => e.subId === plain.id);
  assert.equal(plainEvents.length, 1);
  assert.deepEqual(plainEvents[0].detail, {
    backpressure: 'drop-oldest',
    oldBackpressure: 'block',
  });

  bus.setBackpressureTier(sub.id, 'drop-newest');
  const events = tierAudits(bus).filter((e) => e.subId === sub.id);
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.deepEqual(events[0].detail, {
    backpressure: 'drop-newest',
    oldBackpressure: 'drop-oldest',
  });
  // Same value: a no-op with no audit event.
  bus.setBackpressureTier(sub.id, 'drop-newest');
  assert.equal(tierAudits(bus).filter((e) => e.subId === sub.id).length, 1);
  await flush();
  assert.deepEqual(received, []);
  sub.unsubscribe();
  plain.unsubscribe();
});

test("setBackpressureTier to 'block' arms the refusal on the next saturation, not at revision time", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  bus.setBackpressureTier(sub.id, 'block');
  // The revision itself does not arm the latch, even though the queue
  // is already full.
  assert.equal(bus.getStats().blockedSubscribers, 0);
  // First saturation under 'block': admit-with-shed, exactly like the
  // historical default — m1 is shed and the latch arms.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  // Further arrivals are refused while latched, and counted as blocked.
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.blockedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  // Drained below the high-water mark: admissions resume.
  assert.equal(bus.publish('t', 'm5'), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3', 'm5']);
  sub.unsubscribe();
});

test("setBackpressureTier from 'block' to a drop tier sheds instead of blocking", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  bus.setBackpressureTier(sub.id, 'drop-oldest');
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // Full under the drop tier: the newcomer is admitted, the oldest shed,
  // and the latch never arms.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.blockedCount(sub.id), 0);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  sub.unsubscribe();
});

test('setBackpressureTier away from block preserves the blocked latch until the drain clears it', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.publish('t', 'm3'), 1); // sheds m1, arms the latch
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.blockedCount(sub.id), 0);
  // The revision neither wakes blocked publishers nor discards state.
  bus.setBackpressureTier(sub.id, 'drop-oldest');
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.blockedCount(sub.id), 0);
  // Still latched: the next arrival is refused through the blocked gate.
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.blockedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  // The drain below the high-water mark clears the latch even though
  // the live tier is no longer 'block' — and the drop tier now governs.
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.publish('t', 'm5'), 1);
  assert.equal(bus.publish('t', 'm6'), 1);
  assert.equal(bus.publish('t', 'm7'), 1); // sheds m5, never re-blocks
  assert.equal(bus.getStats().blockedSubscribers, 0);
  await flush();
  assert.deepEqual(received, ['m2', 'm3', 'm6', 'm7']);
  sub.unsubscribe();
});

test("a tier revised to 'block' after subscribe unblocks on drain, and a blocked latch survives a revision away", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  // No throttle, no user onDrained: before EB-285 this subscription had
  // no drain hook at all, so a latch armed after a revision to 'block'
  // would have wedged forever.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.publish('t', 'm3'), 0); // drop-newest refuses
  bus.setBackpressureTier(sub.id, 'block');
  assert.equal(bus.getStats().blockedSubscribers, 0);
  // Full under 'block': the queue's drop-newest shed policy refuses the
  // newcomer, and the saturation arms the latch.
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.blockedCount(sub.id), 0);
  // Revising away while latched neither wakes publishers nor discards
  // the count: the latch and its count survive the revision itself.
  bus.setBackpressureTier(sub.id, 'drop-newest');
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.blockedCount(sub.id), 0);
  assert.equal(bus.publish('t', 'm5'), 0); // refused through the blocked gate
  assert.equal(bus.blockedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  // The drain clears the latch; the drop tier governs from here.
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.publish('t', 'm6'), 1);
  assert.equal(bus.publish('t', 'm7'), 1);
  assert.equal(bus.publish('t', 'm8'), 0); // drop-newest refuses again
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm6', 'm7']);
  sub.unsubscribe();
});

test('setBackpressureTier leaves queue size, drop policy, high-water ratio, and queued messages untouched', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  let backpressureEvents = 0;
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 10,
    backpressure: 'drop-oldest',
    highWaterMarkRatio: 0.5,
    onBackpressure: () => {
      backpressureEvents += 1;
    },
  });
  for (let i = 1; i <= 5; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(backpressureEvents, 1, 'high-water mark crossed at 5 of 10');
  // Already-queued messages are untouched by the revision itself.
  bus.setBackpressureTier(sub.id, 'drop-newest');
  assert.equal(bus.pendingCount(sub.id), 5);
  await flush();
  assert.deepEqual(received, ['m1', 'm2', 'm3', 'm4', 'm5']);
  // Capacity unchanged: still admits up to 10, and the high-water mark
  // still fires at 5 — the ratio was not reset to the 0.8 default.
  for (let i = 1; i <= 5; i++) assert.equal(bus.publish('t', `n${i}`), 1);
  assert.equal(backpressureEvents, 2);
  for (let i = 6; i <= 10; i++) assert.equal(bus.publish('t', `n${i}`), 1);
  assert.equal(bus.pendingCount(sub.id), 10);
  // Shed policy unchanged: the standing drop-oldest policy still sheds
  // the oldest queued message, even though the live tier is now
  // drop-newest — the tier decides block-vs-drop, the dropPolicy
  // decides which message is dropped.
  assert.equal(bus.publish('t', 'n11'), 1);
  assert.equal(bus.pendingCount(sub.id), 10);
  await flush();
  assert.deepEqual(received, [
    'm1', 'm2', 'm3', 'm4', 'm5',
    'n2', 'n3', 'n4', 'n5', 'n6', 'n7', 'n8', 'n9', 'n10', 'n11',
  ]);
  sub.unsubscribe();
});
