import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/**
 * Barrier hold-bound revision (EB-292): `setBarrier` retunes a
 * barrier-opted-in subscriber's `maxHeldMessages` without
 * re-subscribing — only `barrier.maxHeldMessages` moves, the epoch
 * tracking (`lastEpoch`, `armedEpoch`) and the already-held messages
 * are untouched, and the new bound governs from the next hold decision
 * (which reads the live value). Lowering the bound below the live held
 * count fails open on the next arrival (everything held is released at
 * once and the barrier is disarmed, exactly like the subscribe-time
 * bound); raising it admits more held messages at once.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('setBarrier rejects unknown, empty, non-barrier, and invalid revisions fail-closed', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as string), {
    barrier: { maxHeldMessages: 2 },
  });
  const plain = bus.subscribe('t2', () => {});
  for (const bad of [
    () => bus.setBarrier('no-such-sub', 2),
    () => bus.setBarrier('', 2),
    () => bus.setBarrier(plain.id, 2),
    () => bus.setBarrier(sub.id, 0),
    () => bus.setBarrier(sub.id, -1),
    () => bus.setBarrier(sub.id, 2.5),
    () => bus.setBarrier(sub.id, Number.NaN),
    () => bus.setBarrier(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setBarrier(sub.id, '2' as never),
    () => bus.setBarrier(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'barrier_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live bound is untouched: two messages hold behind the armed
  // barrier, and the third arrival overflows under the old bound of 2.
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  bus.publish('t', 'm4');
  assert.equal(bus.getStats().barrier.heldMessages, 0);
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  await flush();
  assert.deepEqual(received, ['m1', 'cut', 'm2', 'm3', 'm4']);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('lowering the bound fails open on the next arrival without touching held messages first', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as string), {
    barrier: { maxHeldMessages: 3 },
  });
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  bus.publish('t', 'm4');
  assert.equal(bus.getStats().barrier.heldMessages, 3);
  bus.setBarrier(sub.id, 1);
  const audit = bus.getSubscriptionAudit({ action: 'barrier_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, 't');
  assert.deepEqual(audit[0].detail, { maxHeldMessages: 1, oldMaxHeldMessages: 3 });
  // The revision itself does not clean up: the three held messages
  // stay held until the next arrival overflows the new bound.
  assert.equal(bus.getStats().barrier.heldMessages, 3);
  assert.equal(bus.getStats().barrier.overflowedHolds, 0);
  // The next arrival overflows the lowered bound: the gate fails open
  // — everything held is released at once, the barrier is disarmed,
  // and the current message flows through the normal pipeline.
  bus.publish('t', 'm5');
  assert.equal(bus.getStats().barrier.heldMessages, 0);
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  await flush();
  assert.deepEqual(received, ['m1', 'cut', 'm2', 'm3', 'm4', 'm5']);
  sub.unsubscribe();
});

test('raising the bound admits more held messages at once', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as string), {
    barrier: { maxHeldMessages: 1 },
  });
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  assert.equal(bus.getStats().barrier.heldMessages, 1);
  bus.setBarrier(sub.id, 3);
  // Under the old bound the next arrival would have failed open;
  // under the new bound m3 and m4 hold alongside m2.
  bus.publish('t', 'm3');
  bus.publish('t', 'm4');
  assert.equal(bus.getStats().barrier.heldMessages, 3);
  assert.equal(bus.getStats().barrier.overflowedHolds, 0);
  // The barrier drains to the handler, releasing everything held.
  await flush();
  assert.deepEqual(received, ['m1', 'cut', 'm2', 'm3', 'm4']);
  sub.unsubscribe();
});

test('re-affirming the current bound is a no-op with no audit', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as string), {
    barrier: { maxHeldMessages: 2 },
  });
  bus.setBarrier(sub.id, 2);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'barrier_revised' }).length,
    0,
    'a same-bound revision must not audit',
  );
  // The bound is unchanged: two messages hold, the third overflows.
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  bus.publish('t', 'm4');
  assert.equal(bus.getStats().barrier.overflowedHolds, 1);
  await flush();
  assert.deepEqual(received, ['m1', 'cut', 'm2', 'm3', 'm4']);
  sub.unsubscribe();
});

test('the revision changes only the bound: epochs and held messages survive', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m), {
    barrier: { maxHeldMessages: 2 },
  });
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  bus.setBarrier(sub.id, 5);
  // The held messages are untouched and still drain behind the same
  // barrier, in order, once it reaches the handler.
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  await flush();
  assert.deepEqual(
    received.map((r) => r.payload),
    ['m1', 'cut', 'm2', 'm3'],
  );
  assert.deepEqual(received[1].barrier, { epoch: 1 });
  // The epoch tracking is untouched too: a replayed epoch 1 is still
  // out-of-order against the recorded lastEpoch of 1.
  bus.publish('t', 'cut-again', { barrier: { epoch: 1 } });
  assert.equal(bus.getStats().barrier.droppedBarriers, 1);
  const dropped = bus.getSubscriptionAudit({ action: 'barrier_dropped' });
  assert.equal(dropped.length, 1);
  assert.deepEqual(dropped[0].detail, { epoch: 1, lastEpoch: 1 });
  // And the next epoch still arms a fresh hold under the new bound.
  bus.publish('t', 'cut2', { barrier: { epoch: 2 } });
  bus.publish('t', 'm4');
  assert.equal(bus.getStats().barrier.heldMessages, 1);
  await flush();
  assert.deepEqual(
    received.map((r) => r.payload),
    ['m1', 'cut', 'm2', 'm3', 'cut2', 'm4'],
  );
  sub.unsubscribe();
});
