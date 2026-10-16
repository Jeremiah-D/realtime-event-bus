import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { LaneQueue } from '../src/lanes.ts';

/**
 * Per-topic priority lanes (EB-122): urgent / normal / bulk lanes per
 * subscriber queue, weighted round-robin drain (default 8:2:1), bulk
 * starvation protection, per-lane metrics, FIFO within a lane.
 */

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Injectable clock so TTL / shaping behavior is deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-lanes-'));

test('default: publishes without priority ride the normal lane in publish order', async () => {
  const bus = new EventBus();
  const received: Array<{ payload: unknown; lane?: string }> = [];
  bus.subscribe('t', (msg) => received.push({ payload: msg.payload, lane: msg.lane }));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    received.map((r) => r.payload),
    [0, 1, 2, 3, 4],
  );
  // The envelope shape is unchanged for priority-unaware traffic.
  assert.ok(received.every((r) => r.lane === undefined));
  const stats = bus.getStats();
  assert.deepEqual(stats.lanes.delivered, { urgent: 0, normal: 5, bulk: 0 });
  assert.deepEqual(stats.lanes.queued, { urgent: 0, normal: 0, bulk: 0 });
  assert.deepEqual(stats.lanes.dropped, { urgent: 0, normal: 0, bulk: 0 });
  const topic = stats.topics.find((t) => t.topic === 't');
  assert.deepEqual(topic?.lanes.delivered, { urgent: 0, normal: 5, bulk: 0 });
});

test('drain follows the configured weighted round-robin rotation', async () => {
  const bus = new EventBus({
    priorityLanes: { weights: { urgent: 2, normal: 1, bulk: 1 } },
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Publish bulk first: rotation must still serve urgent first.
  bus.publish('t', 'b1', { priority: 'bulk' });
  bus.publish('t', 'b2', { priority: 'bulk' });
  bus.publish('t', 'n1');
  bus.publish('t', 'n2');
  for (const u of ['u1', 'u2', 'u3', 'u4']) bus.publish('t', u, { priority: 'urgent' });
  await flush();
  // One rotation pass = 2 urgent, 1 normal, 1 bulk; the pass repeats
  // while the (unbounded) budget remains.
  assert.deepEqual(received, ['u1', 'u2', 'n1', 'b1', 'u3', 'u4', 'n2', 'b2']);
});

test('default weights drain 8 urgent : 2 normal : 1 bulk per pass', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 16; i++) bus.publish('t', `u${i}`, { priority: 'urgent' });
  for (let i = 0; i < 4; i++) bus.publish('t', `n${i}`);
  for (let i = 0; i < 2; i++) bus.publish('t', `b${i}`, { priority: 'bulk' });
  await flush();
  const pass = (u0: number, n0: number, b: number) => [
    `u${u0}`,
    `u${u0 + 1}`,
    `u${u0 + 2}`,
    `u${u0 + 3}`,
    `u${u0 + 4}`,
    `u${u0 + 5}`,
    `u${u0 + 6}`,
    `u${u0 + 7}`,
    `n${n0}`,
    `n${n0 + 1}`,
    `b${b}`,
  ];
  assert.deepEqual(received, [...pass(0, 0, 0), ...pass(8, 2, 1)]);
});

test('FIFO is preserved within each lane', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'u1', { priority: 'urgent' });
  bus.publish('t', 'n1');
  bus.publish('t', 'b1', { priority: 'bulk' });
  bus.publish('t', 'u2', { priority: 'urgent' });
  bus.publish('t', 'n2');
  bus.publish('t', 'b2', { priority: 'bulk' });
  await flush();
  // Each lane keeps its own publish order; lanes interleave by weight.
  assert.deepEqual(received, ['u1', 'u2', 'n1', 'n2', 'b1', 'b2']);
});

test('sequence numbers stay publish-order and monotonic across lanes', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  bus.publish('t', 'b1', { priority: 'bulk' }); // seq 1
  bus.publish('t', 'u1', { priority: 'urgent' }); // seq 2
  bus.publish('t', 'n1'); // seq 3
  await flush();
  // Delivery order is priority-based, but seq is stamped at admission in
  // publish order — lanes never renumber.
  assert.deepEqual(seqs, [2, 3, 1]);
  assert.equal(bus.getStats().topics[0].lastSeq, 3);
});

test('bulk starvation guard force-interleaves after N starved rounds', async () => {
  const clock = makeClock();
  const bus = new EventBus({
    now: clock.now,
    priorityLanes: {
      weights: { urgent: 100, normal: 100, bulk: 1 },
      bulkStarvationRounds: 3,
    },
  });
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
  clock.advance(1000);
  bus.publish('t', 'u4', { priority: 'urgent' });
  await flush(); // round 2: u2 — bulk starved x2
  assert.deepEqual(received, ['u1', 'u2']);
  clock.advance(1000);
  bus.publish('t', 'u5', { priority: 'urgent' });
  await flush(); // round 3: would-be 3rd starved round → b1 forced ahead
  assert.deepEqual(received, ['u1', 'u2', 'b1']);
  // The urgent backlog is untouched by the guard and drains afterwards.
  clock.advance(1000);
  bus.publish('t', 'u6', { priority: 'urgent' });
  await flush(); // round 4: u3
  assert.deepEqual(received, ['u1', 'u2', 'b1', 'u3']);
  const stats = bus.getStats();
  assert.deepEqual(stats.lanes.delivered, { urgent: 3, normal: 0, bulk: 1 });
});

test('per-lane backlog metrics while messages wait', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 1, burst: 1 },
  });
  bus.publish('t', 'u1', { priority: 'urgent' });
  bus.publish('t', 'n1');
  bus.publish('t', 'b1', { priority: 'bulk' });
  await flush();
  assert.deepEqual(received, ['u1']);
  const stats = bus.getStats();
  assert.deepEqual(stats.lanes.queued, { urgent: 0, normal: 1, bulk: 1 });
  assert.deepEqual(stats.lanes.delivered, { urgent: 1, normal: 0, bulk: 0 });
  const topic = stats.topics.find((t) => t.topic === 't');
  assert.deepEqual(topic?.lanes.queued, { urgent: 0, normal: 1, bulk: 1 });
  assert.deepEqual(topic?.lanes.delivered, { urgent: 1, normal: 0, bulk: 0 });
});

test('unsubscribe releases the lane backlog gauges', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    deliveryShaping: { messagesPerSec: 1, burst: 1 },
  });
  bus.publish('t', 'u1', { priority: 'urgent' });
  bus.publish('t', 'n1');
  bus.publish('t', 'b1', { priority: 'bulk' });
  await flush();
  assert.deepEqual(bus.getStats().lanes.queued, { urgent: 0, normal: 1, bulk: 1 });
  sub.unsubscribe();
  assert.deepEqual(bus.getStats().lanes.queued, { urgent: 0, normal: 0, bulk: 0 });
});

test('drop-oldest sheds bulk before urgent under pressure', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  bus.publish('t', 'b1', { priority: 'bulk' });
  bus.publish('t', 'b2', { priority: 'bulk' });
  bus.publish('t', 'u1', { priority: 'urgent' }); // sheds oldest bulk (b1)
  await flush();
  assert.deepEqual(received, ['u1', 'b2']);
  const stats = bus.getStats();
  assert.deepEqual(stats.lanes.dropped, { urgent: 0, normal: 0, bulk: 1 });
  assert.deepEqual(stats.lanes.delivered, { urgent: 1, normal: 0, bulk: 1 });
  assert.deepEqual(stats.lanes.queued, { urgent: 0, normal: 0, bulk: 0 });
});

test('a bulk newcomer is refused when the queue holds only urgent', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 1,
    backpressure: 'drop-oldest',
  });
  bus.publish('t', 'u1', { priority: 'urgent' });
  // The incoming bulk lane is strictly lower precedence than everything
  // queued: refused instead of churning the urgent backlog.
  assert.equal(bus.publish('t', 'b1', { priority: 'bulk' }), 0);
  await flush();
  assert.deepEqual(received, ['u1']);
  assert.deepEqual(bus.getStats().lanes.dropped, { urgent: 0, normal: 0, bulk: 1 });
  assert.equal(bus.droppedCount(sub.id), 1);
});

test('drop-newest refuses overflow regardless of lane', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    queueSize: 1,
    backpressure: 'drop-newest',
  });
  bus.publish('t', 'n1');
  assert.equal(bus.publish('t', 'u1', { priority: 'urgent' }), 0);
  await flush();
  assert.deepEqual(received, ['n1']);
  assert.deepEqual(bus.getStats().lanes.dropped, { urgent: 1, normal: 0, bulk: 0 });
});

test('TTL expiry leaves the lane backlog', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 1, burst: 1 },
  });
  bus.setTopicTtl('t', 100);
  bus.publish('t', 'u1', { priority: 'urgent' });
  bus.publish('t', 'b1', { priority: 'bulk' });
  clock.advance(200);
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 2);
  assert.deepEqual(stats.lanes.queued, { urgent: 0, normal: 0, bulk: 0 });
});

test('invalid priority throws RangeError before anything is mutated', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  assert.throws(
    () => bus.publish('t', 1, { priority: 'nope' as never }),
    RangeError,
  );
  assert.throws(
    () => bus.publishDelayed('t', 1, { delayMs: 10, priority: 'nope' as never }),
    RangeError,
  );
  assert.throws(
    () => bus.publishIdempotent('t', 1, { messageId: 'm1', priority: 'nope' as never }),
    RangeError,
  );
  assert.throws(
    () => bus.publishBatch([{ topic: 't', payload: 1, lane: 'nope' as never }]),
    RangeError,
  );
  assert.equal(bus.getStats().totalPublished, 0);
});

test('invalid lane tuning throws at construction', () => {
  assert.throws(() => new EventBus({ priorityLanes: { weights: { urgent: 0 } } }), RangeError);
  assert.throws(() => new EventBus({ priorityLanes: { weights: { bulk: -1 } } }), RangeError);
  assert.throws(() => new EventBus({ priorityLanes: { bulkStarvationRounds: 0 } }), RangeError);
});

test('delayed publishes keep their lane at fan-out', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'n1');
  bus.publishDelayed('t', 'u1', { delayMs: 100, priority: 'urgent' });
  await flush();
  assert.deepEqual(received, ['n1']);
  clock.advance(100);
  bus.publish('t', 'n2'); // triggers the sweep; u1 fans out into the urgent lane
  await flush();
  assert.deepEqual(received, ['n1', 'u1', 'n2']);
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 1, normal: 2, bulk: 0 });
});

test('batch and atomic entries carry their lane', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishBatch([
    { topic: 't', payload: 'b1', lane: 'bulk' },
    { topic: 't', payload: 'n1' },
    { topic: 't', payload: 'u1', lane: 'urgent' },
  ]);
  const atomic = bus.publishAtomic([
    { topic: 't', payload: 'b2', lane: 'bulk' },
    { topic: 't', payload: 'u2', lane: 'urgent' },
  ]);
  assert.equal(atomic.published, 2);
  await flush();
  // Urgent first (both batches), then normal, then bulk.
  assert.deepEqual(received, ['u1', 'u2', 'n1', 'b1', 'b2']);
});

test('consumer-group assignment keeps the lane', async () => {
  const bus = new EventBus();
  const a: unknown[] = [];
  const b: unknown[] = [];
  bus.subscribeToGroup('g', 't', (msg) => a.push(msg.payload));
  bus.subscribeToGroup('g', 't', (msg) => b.push(msg.payload));
  bus.publish('t', 'u1', { priority: 'urgent' });
  await flush();
  // Exactly one member gets the group's single copy…
  assert.equal(a.length + b.length, 1);
  // …and the lane delivery is counted.
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 1, normal: 0, bulk: 0 });
});

test('lane persists on the durable log and replay restores it', async () => {
  const dir = freshDir();
  try {
    const bus1 = new EventBus({ durableLogDir: dir });
    bus1.subscribe('t', () => {});
    bus1.publish('t', 'u1', { priority: 'urgent' });
    bus1.publish('t', 'n1');
    await flush();
    const raw = readFileSync(join(dir, `${encodeURIComponent('t')}.log`), 'utf8');
    assert.ok(raw.includes('"lane":"urgent"'), 'urgent lane is written to the log line');
    assert.ok(!raw.includes('"lane":"normal"'), 'normal lane stays absent (byte-identical lines)');
    const bus2 = new EventBus({ durableLogDir: dir });
    const received: Array<{ payload: unknown; lane?: string }> = [];
    bus2.subscribe('t', (msg) => received.push({ payload: msg.payload, lane: msg.lane }), {
      resumeFromSeq: 0,
    });
    await flush();
    assert.deepEqual(received, [
      { payload: 'u1', lane: 'urgent' },
      { payload: 'n1', lane: undefined },
    ]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('LaneQueue: a single occupied lane drains exactly like a FIFO', () => {
  const q = new LaneQueue<string>({ capacity: 10 });
  q.push('a', 'normal');
  q.push('b', 'normal');
  q.push('c', 'normal');
  const { live, expired } = q.drainLiveUpTo(0, 10);
  assert.deepEqual(live, ['a', 'b', 'c']);
  assert.deepEqual(expired, []);
  assert.equal(q.size, 0);
});

test('LaneQueue: weighted rotation across lanes with a bounded budget', () => {
  const q = new LaneQueue<string>({
    capacity: 10,
    weights: { urgent: 2, normal: 1, bulk: 1 },
    bulkStarvationRounds: 100,
  });
  q.push('u1', 'urgent');
  q.push('u2', 'urgent');
  q.push('u3', 'urgent');
  q.push('n1', 'normal');
  q.push('b1', 'bulk');
  // Budget of 3: one pass takes 2 urgent + 1 normal; bulk waits.
  const first = q.drainLiveUpTo(0, 3);
  assert.deepEqual(first.live, ['u1', 'u2', 'n1']);
  // Budget of 3 again: urgent takes 1 (only u3 left), normal empty, bulk takes 1.
  const second = q.drainLiveUpTo(0, 3);
  assert.deepEqual(second.live, ['u3', 'b1']);
  assert.equal(q.size, 0);
});

test('LaneQueue: bulk starvation guard force-interleaves', () => {
  const q = new LaneQueue<string>({
    capacity: 10,
    weights: { urgent: 100, normal: 100, bulk: 1 },
    bulkStarvationRounds: 2,
  });
  q.push('u0', 'urgent');
  q.push('b1', 'bulk');
  // Round 1: the urgent flood consumes the whole budget — bulk starved x1.
  let r = q.drainLiveUpTo(0, 1);
  assert.deepEqual(r.live, ['u0']);
  q.push('u1', 'urgent');
  // Round 2: this would be the 2nd consecutive undrained round, so the
  // guard force-interleaves one bulk message ahead of the rotation.
  r = q.drainLiveUpTo(0, 1);
  assert.deepEqual(r.live, ['b1']);
  r = q.drainLiveUpTo(0, 10);
  assert.deepEqual(r.live, ['u1']);
  assert.equal(q.size, 0);
});

test('LaneQueue: drop-oldest sheds the lowest-precedence lane first', () => {
  const q = new LaneQueue<string>({ capacity: 2, policy: 'drop-oldest' });
  assert.equal(q.push('b1', 'bulk'), 'accepted');
  assert.equal(q.push('n1', 'normal'), 'accepted');
  // Full: an urgent newcomer sheds the oldest bulk entry.
  assert.equal(q.push('u1', 'urgent'), 'accepted');
  assert.equal(q.droppedByLane('bulk'), 1);
  assert.equal(q.droppedByLane('normal'), 0);
  assert.equal(q.droppedByLane('urgent'), 0);
  const { live } = q.drainLiveUpTo(0, 10);
  assert.deepEqual(live, ['u1', 'n1']);
});

test('LaneQueue: a lower-precedence newcomer is refused, never churning', () => {
  const q = new LaneQueue<string>({ capacity: 1, policy: 'drop-oldest' });
  assert.equal(q.push('u1', 'urgent'), 'accepted');
  assert.equal(q.push('b1', 'bulk'), 'dropped');
  assert.equal(q.droppedByLane('bulk'), 1);
  assert.equal(q.laneSize('urgent'), 1);
});

test('LaneQueue: TTL expiry is swept per lane', () => {
  const q = new LaneQueue<string>({ capacity: 10 });
  q.push('u1', 'urgent', 100);
  q.push('n1', 'normal', 100);
  q.push('b1', 'bulk', 100);
  const { live, expired } = q.drainLiveUpTo(200, 10);
  assert.deepEqual(live, []);
  assert.deepEqual(expired, ['u1', 'n1', 'b1']);
  assert.equal(q.expiredByLane('urgent'), 1);
  assert.equal(q.expiredByLane('normal'), 1);
  assert.equal(q.expiredByLane('bulk'), 1);
});

test('LaneQueue: high-water mark tracks the aggregate size', () => {
  const events: string[] = [];
  const q = new LaneQueue<string>({
    capacity: 10,
    highWaterMarkRatio: 0.5,
    onHighWaterMark: (size) => events.push(`hwm:${size}`),
    onDrained: (size) => events.push(`drained:${size}`),
  });
  q.push('u1', 'urgent');
  q.push('u2', 'urgent');
  q.push('n1', 'normal');
  q.push('n2', 'normal');
  q.push('b1', 'bulk'); // aggregate hits 5 = the mark
  assert.deepEqual(events, ['hwm:5']);
  q.drainLiveUpTo(0, 10);
  assert.deepEqual(events, ['hwm:5', 'drained:0']);
});
