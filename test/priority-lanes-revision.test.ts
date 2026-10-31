/**
 * Bus-level priority-lane revision (EB-310).
 *
 * `setPriorityLanes` retunes the weighted round-robin drain weights and
 * the bulk starvation threshold live — without restarting the bus.
 * Lanes are always on, so there is no never-enabled bus to refuse;
 * illegal values fail closed with the live tuning and the audit log
 * untouched; an identical (or empty) revision is a no-op with no audit.
 * The revision fans out to every in-flight subscriber queue and governs
 * from the next drain round; a subscriber created after the revision
 * builds its queue from the new tuning; a bulk starvation counter
 * already counting keeps counting against the live threshold.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { LaneQueue } from '../src/lanes.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Injectable clock so shaping behavior is deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'priority_lanes_revised' });

test('setPriorityLanes rejects illegal values and leaves the live tuning untouched', async (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));

  const bad: Array<unknown> = [
    { weights: { urgent: 0 } },
    { weights: { urgent: -1 } },
    { weights: { urgent: 1.5 } },
    { weights: { urgent: Number.NaN } },
    { weights: { urgent: Infinity } },
    { weights: { urgent: '8' } },
    { weights: { normal: 0 } },
    { weights: { bulk: 0 } },
    { weights: 'nope' },
    { weights: [] },
    { weights: null },
    { bulkStarvationRounds: 0 },
    { bulkStarvationRounds: -1 },
    { bulkStarvationRounds: 1.5 },
    { bulkStarvationRounds: Number.NaN },
    { bulkStarvationRounds: Infinity },
    { bulkStarvationRounds: '10' },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setPriorityLanes(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  assert.equal(audit(bus).length, 0);
  // The failed calls changed nothing: the default 8:2:1 rotation still holds.
  for (let i = 0; i < 4; i++) bus.publish('t', `u${i}`, { priority: 'urgent' });
  for (let i = 0; i < 2; i++) bus.publish('t', `n${i}`);
  for (let i = 0; i < 2; i++) bus.publish('t', `b${i}`, { priority: 'bulk' });
  await flush();
  assert.deepEqual(received, ['u0', 'u1', 'u2', 'u3', 'n0', 'n1', 'b0', 'b1']);
});

test('setPriorityLanes with identical values or an empty revision is a no-op with no audit', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  bus.setPriorityLanes({ weights: { urgent: 8, normal: 2, bulk: 1 } });
  bus.setPriorityLanes({ bulkStarvationRounds: 10 });
  bus.setPriorityLanes({ weights: { bulk: 1 } });
  bus.setPriorityLanes({});
  bus.setPriorityLanes(null as never);
  assert.equal(audit(bus).length, 0);
});

test('partial revision moves only the provided field, and audits old and new', (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());

  bus.setPriorityLanes({ weights: { bulk: 5 } });
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    weights: { urgent: 8, normal: 2, bulk: 5 },
    oldWeights: { urgent: 8, normal: 2, bulk: 1 },
    bulkStarvationRounds: 10,
    oldBulkStarvationRounds: 10,
  });

  bus.setPriorityLanes({ bulkStarvationRounds: 4 });
  const after = audit(bus);
  assert.equal(after.length, 2);
  assert.deepEqual(after[1].detail, {
    weights: { urgent: 8, normal: 2, bulk: 5 },
    oldWeights: { urgent: 8, normal: 2, bulk: 5 },
    bulkStarvationRounds: 4,
    oldBulkStarvationRounds: 10,
  });
});

test('revision retunes the in-flight queue: already-queued entries drain under the new weights', async (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));

  for (let i = 0; i < 4; i++) bus.publish('t', `u${i}`, { priority: 'urgent' });
  for (let i = 0; i < 2; i++) bus.publish('t', `n${i}`);
  for (let i = 0; i < 2; i++) bus.publish('t', `b${i}`, { priority: 'bulk' });
  // The drain is deferred to a macrotask, so this revision lands before
  // the queued batch drains — and the batch drains 1:1:1, not 8:2:1.
  bus.setPriorityLanes({ weights: { urgent: 1, normal: 1, bulk: 1 } });
  await flush();
  assert.deepEqual(received, ['u0', 'n0', 'b0', 'u1', 'n1', 'b1', 'u2', 'u3']);
  assert.equal(audit(bus).length, 1);
});

test('a later batch drains under the revised weights, and a new subscriber builds from them', async (t) => {
  const bus = new EventBus();
  t.after(() => bus.close());
  const first: unknown[] = [];
  bus.subscribe('t', (msg) => first.push(msg.payload));
  for (let i = 0; i < 2; i++) bus.publish('t', `u${i}`, { priority: 'urgent' });
  bus.publish('t', 'n0');
  bus.publish('t', 'b0', { priority: 'bulk' });
  await flush();
  assert.deepEqual(first, ['u0', 'u1', 'n0', 'b0']);

  bus.setPriorityLanes({ weights: { urgent: 1, normal: 1, bulk: 1 } });
  const second: unknown[] = [];
  bus.subscribe('t', (msg) => second.push(msg.payload));
  for (let i = 2; i < 4; i++) bus.publish('t', `u${i}`, { priority: 'urgent' });
  bus.publish('t', 'n1');
  bus.publish('t', 'b1', { priority: 'bulk' });
  await flush();
  // The in-flight queue was fanned out, and the new subscriber's queue
  // was built from the live bus values — both drain 1:1:1.
  assert.deepEqual(first, ['u0', 'u1', 'n0', 'b0', 'u2', 'n1', 'b1', 'u3']);
  assert.deepEqual(second, ['u2', 'n1', 'b1', 'u3']);
});

test('tightening bulkStarvationRounds force-interleaves on the very next round', async () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    priorityLanes: {
      weights: { urgent: 100, normal: 100, bulk: 1 },
      bulkStarvationRounds: 10,
    },
  });
  // No close(): the bus still holds budget-starved backlog, and
  // close() would drain it against the 30s default drain timeout on a
  // clock that never advances again — the lanes.test.ts starvation
  // test likewise ends without closing.
  const received: unknown[] = [];
  // Burst of 1, refilling 1 token per injected second: every flush round
  // drains exactly one message, deterministically.
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 1, burst: 1 },
  });
  bus.publish('t', 'u1', { priority: 'urgent' });
  bus.publish('t', 'u2', { priority: 'urgent' });
  bus.publish('t', 'u3', { priority: 'urgent' });
  bus.publish('t', 'b1', { priority: 'bulk' });
  await flush(); // round 1: u1 — bulk starved x1
  assert.deepEqual(received, ['u1']);
  // Tighten the threshold below the counter already counting: the next
  // round force-interleaves the bulk message instead of draining u2.
  bus.setPriorityLanes({ bulkStarvationRounds: 2 });
  clock.advance(1000);
  bus.publish('t', 'u4', { priority: 'urgent' });
  await flush(); // round 2: b1 forced ahead
  assert.deepEqual(received, ['u1', 'b1']);
});

test('loosening bulkStarvationRounds defers the force-interleave', async () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    priorityLanes: {
      weights: { urgent: 100, normal: 100, bulk: 1 },
      bulkStarvationRounds: 2,
    },
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 1, burst: 1 },
  });
  bus.publish('t', 'u1', { priority: 'urgent' });
  bus.publish('t', 'u2', { priority: 'urgent' });
  bus.publish('t', 'u3', { priority: 'urgent' });
  bus.publish('t', 'b1', { priority: 'bulk' });
  await flush(); // round 1: u1 — bulk starved x1
  assert.deepEqual(received, ['u1']);
  // Loosen the threshold: the counter keeps counting, but the force
  // that the old threshold of 2 would have triggered next round is
  // deferred — u2 and u3 drain first.
  bus.setPriorityLanes({ bulkStarvationRounds: 10 });
  clock.advance(1000);
  bus.publish('t', 'u4', { priority: 'urgent' });
  await flush(); // round 2: u2, not b1
  assert.deepEqual(received, ['u1', 'u2']);
  clock.advance(1000);
  bus.publish('t', 'u5', { priority: 'urgent' });
  await flush(); // round 3: u3, still no force
  assert.deepEqual(received, ['u1', 'u2', 'u3']);
});

test('LaneQueue retune/current: construction semantics, failed retune untouched', () => {
  const queue = new LaneQueue<string>({ capacity: 10 });
  assert.deepEqual(queue.current(), {
    weights: { urgent: 8, normal: 2, bulk: 1 },
    bulkStarvationRounds: 10,
  });
  queue.retune({ weights: { urgent: 1, normal: 1, bulk: 1 }, bulkStarvationRounds: 3 });
  assert.deepEqual(queue.current(), {
    weights: { urgent: 1, normal: 1, bulk: 1 },
    bulkStarvationRounds: 3,
  });
  // current() returns a copy: mutating it must not move the live tuning.
  const snapshot = queue.current();
  snapshot.weights.urgent = 99;
  snapshot.bulkStarvationRounds = 99;
  assert.deepEqual(queue.current(), {
    weights: { urgent: 1, normal: 1, bulk: 1 },
    bulkStarvationRounds: 3,
  });
  for (const next of [
    { weights: { urgent: 0, normal: 1, bulk: 1 }, bulkStarvationRounds: 3 },
    { weights: { urgent: 1, normal: 1, bulk: 1 }, bulkStarvationRounds: 0 },
    { weights: { urgent: 1.5, normal: 1, bulk: 1 }, bulkStarvationRounds: 3 },
    { weights: { urgent: 1, normal: 1, bulk: 1 }, bulkStarvationRounds: Number.NaN },
  ]) {
    assert.throws(() => queue.retune(next), RangeError);
  }
  assert.deepEqual(queue.current(), {
    weights: { urgent: 1, normal: 1, bulk: 1 },
    bulkStarvationRounds: 3,
  });
});
