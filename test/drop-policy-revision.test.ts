import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, BusClosedError } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const policyAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'drop_policy_revised' });

test('setDropPolicy rejects unknown, empty, and illegal policies fail-closed', () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  for (const bad of [
    () => bus.setDropPolicy('no-such-sub', 'drop-newest'),
    () => bus.setDropPolicy('', 'drop-newest'),
    // 'block' is a backpressure tier, not a shed policy.
    () => bus.setDropPolicy(sub.id, 'block' as never),
    () => bus.setDropPolicy(sub.id, 'BLOCK' as never),
    () => bus.setDropPolicy(sub.id, 'drop' as never),
    () => bus.setDropPolicy(sub.id, '' as never),
    () => bus.setDropPolicy(sub.id, undefined as never),
    () => bus.setDropPolicy(sub.id, null as never),
    () => bus.setDropPolicy(sub.id, 42 as never),
    () => bus.setDropPolicy(sub.id, {} as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(policyAudits(bus).length, 0, 'failed revisions must not audit');
  // The live policy is untouched: still drop-oldest, so the newcomer
  // sheds the oldest entry instead of being refused.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  sub.unsubscribe();
});

test('setDropPolicy drop-oldest -> drop-newest: the next full-queue push drops the newcomer', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  bus.setDropPolicy(sub.id, 'drop-newest');
  // Full queue, new policy: the newcomer is refused, the backlog intact.
  assert.equal(bus.publish('t', 'm3'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  sub.unsubscribe();
});

test('setDropPolicy drop-newest -> drop-oldest restores oldest-eviction', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  bus.setDropPolicy(sub.id, 'drop-oldest');
  // Full queue, restored policy: the newcomer is admitted, the oldest shed.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  sub.unsubscribe();
});

test('setDropPolicy steers the byte-budget shed victim', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  // String payloads measure as their JSON length: '"aa"' is 4 bytes, so
  // two messages exactly fill the 8-byte budget with count headroom.
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 10,
    queueMaxBytes: 8,
    backpressure: 'drop-oldest',
  });
  assert.equal(bus.publish('t', 'aa'), 1);
  assert.equal(bus.publish('t', 'bb'), 1);
  bus.setDropPolicy(sub.id, 'drop-newest');
  // Over budget under drop-newest: the newcomer is refused, nothing shed.
  assert.equal(bus.publish('t', 'cc'), 0);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  bus.setDropPolicy(sub.id, 'drop-oldest');
  // Over budget under drop-oldest: the oldest entry is shed for it.
  assert.equal(bus.publish('t', 'cc'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 2);
  await flush();
  assert.deepEqual(received, ['bb', 'cc']);
  sub.unsubscribe();
});

test("setDropPolicy under 'block': first saturation admits-with-shed per the new policy, and the revision itself does not arm the latch", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  bus.setDropPolicy(sub.id, 'drop-newest');
  // The revision alone arms nothing — the latch is still clear.
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.blockedCount(sub.id), 0);
  // First saturation under the new policy: the newcomer is refused
  // (drop-newest) instead of shedding, and the latch arms on the
  // count-full admission. The arming push itself is not a
  // while-blocked refusal, so blockedCount stays 0.
  assert.equal(bus.publish('t', 'm3'), 0);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.blockedCount(sub.id), 0);
  assert.equal(bus.droppedCount(sub.id), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  // Still blocked: the next arrival is refused without touching the queue.
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.blockedCount(sub.id), 1);
  assert.equal(bus.droppedCount(sub.id), 2);
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  sub.unsubscribe();
});

test("setDropPolicy while 'block'-latched neither arms nor clears the latch", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  // First saturation under the standing drop-oldest: sheds 'm1', arms.
  // The arming push itself is not a while-blocked refusal.
  assert.equal(bus.publish('t', 'm3'), 1);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.blockedCount(sub.id), 0);
  assert.equal(bus.droppedCount(sub.id), 1);
  bus.setDropPolicy(sub.id, 'drop-newest');
  // Still latched after the revision: the next arrival is refused.
  assert.equal(bus.publish('t', 'm4'), 0);
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.blockedCount(sub.id), 1);
  assert.equal(bus.droppedCount(sub.id), 2);
  assert.equal(bus.pendingCount(sub.id), 2);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  sub.unsubscribe();
});

test('setDropPolicy to the current policy is a no-op with no audit', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  // The standing policy is already drop-oldest: no audit, no state move.
  bus.setDropPolicy(sub.id, 'drop-oldest');
  assert.equal(policyAudits(bus).length, 0);
  bus.setDropPolicy(sub.id, 'drop-newest');
  assert.equal(policyAudits(bus).length, 1);
  bus.setDropPolicy(sub.id, 'drop-newest');
  assert.equal(policyAudits(bus).length, 1, 're-affirming the live policy audits nothing');
  sub.unsubscribe();
});

test('setDropPolicy audits the exact detail with actor system', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  bus.setDropPolicy(sub.id, 'drop-newest');
  const events = policyAudits(bus).filter((e) => e.subId === sub.id);
  assert.equal(events.length, 1);
  assert.equal(events[0].actor, 'system');
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.deepEqual(events[0].detail, {
    dropPolicy: 'drop-newest',
    oldDropPolicy: 'drop-oldest',
  });
  sub.unsubscribe();
});

test('setDropPolicy revises a shadow subscription queue and the mirror sheds per the new policy', async () => {
  const bus = new EventBus();
  const prod: unknown[] = [];
  const shadow: unknown[] = [];
  bus.subscribe('t', (msg) => {
    prod.push(msg.payload);
  });
  const shadowSub = bus.subscribe(
    't',
    (msg) => {
      shadow.push(msg.payload);
    },
    { shadow: { queueSize: 2, dropPolicy: 'drop-oldest' } },
  );
  bus.publish('t', 1);
  bus.publish('t', 2);
  // Shadow queue is full at [1, 2]; revise its policy in place.
  bus.setDropPolicy(shadowSub.id, 'drop-newest');
  // Mirror copies are refused under the new policy — production is untouched.
  bus.publish('t', 3);
  bus.publish('t', 4);
  assert.equal(bus.getStats().shadowedMessages, 0);
  assert.equal(bus.getStats().shadowDroppedMessages, 2);
  // Back to drop-oldest: the next mirror copy sheds the oldest shadow entry.
  bus.setDropPolicy(shadowSub.id, 'drop-oldest');
  bus.publish('t', 5);
  assert.equal(bus.getStats().shadowDroppedMessages, 3);
  await flush();
  assert.deepEqual(prod, [1, 2, 3, 4, 5]);
  assert.deepEqual(shadow, [2, 5]);
  assert.equal(bus.getStats().shadowedMessages, 2);
  const [row] = bus.getStats().shadows;
  assert.equal(row.shadowed, 2);
  assert.equal(row.dropped, 3);
  shadowSub.unsubscribe();
});

test('setDropPolicy leaves already-queued messages untouched', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.pendingCount(sub.id), 2);
  bus.setDropPolicy(sub.id, 'drop-newest');
  assert.equal(bus.pendingCount(sub.id), 2, 'no queued message is re-shed');
  assert.equal(bus.droppedCount(sub.id), 0);
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  sub.unsubscribe();
});

test('setDropPolicy after close is rejected with BusClosedError', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  await bus.close();
  assert.throws(
    () => bus.setDropPolicy(sub.id, 'drop-newest'),
    (err: unknown) => {
      assert.ok(err instanceof BusClosedError, `expected BusClosedError, got ${String(err)}`);
      assert.equal((err as Error).name, 'BusClosedError');
      assert.equal((err as BusClosedError).code, 'BUS_CLOSED');
      assert.equal((err as BusClosedError).action, 'subscribe');
      return true;
    },
  );
  assert.equal(policyAudits(bus).length, 0);
});
