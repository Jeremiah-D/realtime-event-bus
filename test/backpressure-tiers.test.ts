import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test("default 'block' tier preserves single-saturation behavior (backward compatible)", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 1);
  // First saturation shed exactly like the historical default — and armed the block.
  assert.equal(bus.getStats().blockedSubscribers, 1);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']); // 'm1' was shed
  assert.equal(bus.getStats().blockedSubscribers, 0); // drain receded below the high-water mark
  sub.unsubscribe();
});

test("'block' refuses sustained saturation instead of churning the backlog", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3'); // sheds 'm1', saturates -> blocked
  assert.equal(bus.publish('t', 'm4'), 0); // refused: backlog preserved
  assert.equal(bus.publish('t', 'm5'), 0); // refused: still blocked
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.blockedCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 3); // 1 shed + 2 blocked refusals
  assert.equal(bus.getStats().blockedMessages, 2);
  assert.equal(bus.getStats().droppedMessages, 3);
  await flush();
  assert.deepEqual(received, ['m2', 'm3']); // backlog intact, nothing churned
  sub.unsubscribe();
});

test("'block' clears below the high-water mark and accepts again", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 10 });
  for (let i = 1; i <= 11; i++) bus.publish('t', `m${i}`); // 11th sheds m1 -> blocked
  assert.equal(bus.publish('t', 'm12'), 0); // refused while blocked
  assert.equal(bus.getStats().blockedSubscribers, 1);
  await flush(); // full drain recedes below the 80% mark -> unblocked
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.publish('t', 'm13'), 1); // accepted again
  await flush();
  assert.deepEqual(
    received,
    ['m2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8', 'm9', 'm10', 'm11', 'm13'],
  );
  sub.unsubscribe();
});

test("explicit 'drop-oldest' never blocks", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  for (let i = 1; i <= 5; i++) assert.equal(bus.publish('t', `m${i}`), 1);
  assert.equal(bus.droppedCount(sub.id), 3);
  assert.equal(bus.blockedCount(sub.id), 0);
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.getStats().blockedMessages, 0);
  await flush();
  assert.deepEqual(received, ['m4', 'm5']);
  sub.unsubscribe();
});

test("explicit 'drop-newest' refuses the newcomer immediately", async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-newest',
  });
  assert.equal(bus.publish('t', 'm1'), 1);
  assert.equal(bus.publish('t', 'm2'), 1);
  assert.equal(bus.publish('t', 'm3'), 0); // refused: backlog untouched
  assert.equal(bus.publish('t', 'm4'), 0); // refused again: drop tiers never block
  assert.equal(bus.blockedCount(sub.id), 0);
  assert.equal(bus.droppedCount(sub.id), 2);
  assert.equal(bus.getStats().blockedMessages, 0);
  await flush();
  assert.deepEqual(received, ['m1', 'm2']);
  sub.unsubscribe();
});

test('invalid backpressure values throw RangeError and register nothing', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribe('t', () => {}, { backpressure: 'sometimes' }), RangeError);
  assert.throws(() => bus.subscribe('t', () => {}, { backpressure: '' }), RangeError);
  assert.throws(
    () => bus.subscribe('t', () => {}, { backpressure: null as unknown as 'block' }),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, 0); // nothing half-registered
});

test('an explicit dropPolicy contradicting an explicit drop tier throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        queueSize: 2,
        backpressure: 'drop-oldest',
        dropPolicy: 'drop-newest',
      }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        queueSize: 2,
        backpressure: 'drop-newest',
        dropPolicy: 'drop-oldest',
      }),
    RangeError,
  );
  // 'block' + an explicit dropPolicy is coherent: the tier sheds per the queue policy.
  const sub = bus.subscribe('t', () => {}, { queueSize: 2, dropPolicy: 'drop-newest' });
  assert.equal(bus.getStats().totalSubscribers, 1);
  sub.unsubscribe();
});

test('backpressure drops are audited as backpressure_dropped', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { queueSize: 2 });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3'); // sheds m1
  bus.publish('t', 'm4'); // refused while blocked
  const drops = bus.getSubscriptionAudit({ action: 'backpressure_dropped' });
  assert.equal(drops.length, 2);
  assert.equal(drops[0].subId, sub.id);
  assert.equal(drops[0].topic, 't');
  assert.equal(drops[0].pattern, 't');
  assert.deepEqual(drops[0].detail, { policy: 'block', dropped: 1, queueSize: 2 });
  assert.deepEqual(drops[1].detail, { policy: 'block', dropped: 1, queueSize: 2 });
  // Lifecycle forensics stay intact alongside drop forensics.
  assert.equal(bus.getSubscriptionAudit({ action: 'subscribe' }).length, 1);
  sub.unsubscribe();
});

test("'block' composes with maxInflight (EB-67): the cap still governs the preserved backlog", async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const releases: Array<() => void> = [];
  const sub = bus.subscribe(
    't',
    (msg) => {
      seen.push(msg.payload as number);
      return new Promise<void>((resolve) => {
        releases.push(resolve);
      });
    },
    { queueSize: 10, maxInflight: 5 },
  );
  for (let n = 1; n <= 12; n++) bus.publish('t', n);
  // 10 fill the queue, the 11th sheds n=1 and blocks, the 12th is refused.
  assert.equal(bus.getStats().blockedSubscribers, 1);
  assert.equal(bus.blockedCount(sub.id), 1);
  await flush();
  // In-flight cap still enforced on the preserved backlog: 5 at a time, in order.
  assert.deepEqual(seen, [2, 3, 4, 5, 6]);
  for (const release of releases.splice(0)) release();
  await flush();
  assert.deepEqual(seen, [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
  // Draining below the high-water mark unblocked the subscriber.
  assert.equal(bus.getStats().blockedSubscribers, 0);
  assert.equal(bus.publish('t', 13), 1);
  sub.unsubscribe();
});
