import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { BoundedQueue } from '../src/backpressure.ts';
import { LaneQueue } from '../src/lanes.ts';
import {
  deficitRoundRobinSelect,
  resolveTopicFairnessOptions,
} from '../src/fair.ts';
import { renderPrometheus } from '../src/metrics.ts';

/**
 * Topic-level fair scheduling (EB-135): deficit round-robin across the
 * topics queued for one subscriber, quantum = 1 message.
 *
 * All tests are clock-independent pure counting tests: message order and
 * per-topic counts are asserted exactly, with an injectable clock only
 * where a delivery-shaping budget has to refill between rounds (no real
 * timers are ever awaited).
 */

function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

interface Tagged {
  topic: string;
  id: number;
}

function tagged(topic: string, count: number, from = 0): Tagged[] {
  const items: Tagged[] = [];
  for (let i = 0; i < count; i += 1) items.push({ topic, id: from + i });
  return items;
}

const groupOf = (item: Tagged): string => item.topic;

// ---------------------------------------------------------------------------
// Pure DRR selector
// ---------------------------------------------------------------------------

test('deficitRoundRobinSelect: weighted interleave at single-message granularity', () => {
  const deficits = new Map<string, number>();
  const items = [...tagged('hot', 2), ...tagged('cold', 6)];
  const selected = deficitRoundRobinSelect(items, groupOf, (g) => (g === 'hot' ? 1 : 3), deficits, 8);
  assert.deepEqual(
    selected.map((m) => `${m.topic}:${m.id}`),
    ['hot:0', 'cold:0', 'cold:1', 'cold:2', 'hot:1', 'cold:3', 'cold:4', 'cold:5'],
  );
});

test('deficitRoundRobinSelect: rotation follows first-appearance order', () => {
  const deficits = new Map<string, number>();
  // cold appears first: with equal weights it is served first.
  const items = [...tagged('cold', 2), ...tagged('hot', 2)];
  const selected = deficitRoundRobinSelect(items, groupOf, () => 1, deficits, 4);
  assert.deepEqual(
    selected.map((m) => `${m.topic}:${m.id}`),
    ['cold:0', 'hot:0', 'cold:1', 'hot:1'],
  );
});

test('deficitRoundRobinSelect: per-group FIFO order is preserved', () => {
  const deficits = new Map<string, number>();
  const items = [...tagged('b', 3), ...tagged('a', 3)];
  const selected = deficitRoundRobinSelect(items, groupOf, () => 1, deficits, 6);
  assert.deepEqual(
    selected.map((m) => m.id),
    [0, 0, 1, 1, 2, 2],
  );
});

test('deficitRoundRobinSelect: a drained group loses its deficit', () => {
  const deficits = new Map<string, number>();
  const items = tagged('solo', 2);
  deficitRoundRobinSelect(items, groupOf, () => 5, deficits, 10);
  assert.equal(deficits.has('solo'), false);
});

test('deficitRoundRobinSelect: unknown groups fall back to weight 1', () => {
  const deficits = new Map<string, number>();
  const items = [...tagged('known', 2), ...tagged('mystery', 2)];
  const selected = deficitRoundRobinSelect(items, groupOf, (g) => (g === 'known' ? 2 : 1), deficits, 4);
  assert.deepEqual(
    selected.map((m) => `${m.topic}:${m.id}`),
    ['known:0', 'known:1', 'mystery:0', 'mystery:1'],
  );
});

// ---------------------------------------------------------------------------
// BoundedQueue.drainLiveUpToFair
// ---------------------------------------------------------------------------

function makeQueue(): BoundedQueue<Tagged> {
  return new BoundedQueue<Tagged>({ capacity: 100 });
}

function pushAll(queue: BoundedQueue<Tagged>, items: Tagged[]): void {
  for (const item of items) assert.equal(queue.push(item), 'accepted');
}

test('BoundedQueue.drainLiveUpToFair: 1:3 interleave, per-topic FIFO', () => {
  const queue = makeQueue();
  const deficits = new Map<string, number>();
  pushAll(queue, [...tagged('hot', 6), ...tagged('cold', 6)]);
  const { live, expired } = queue.drainLiveUpToFair(
    0,
    12,
    groupOf,
    (g) => (g === 'hot' ? 1 : 3),
    deficits,
  );
  assert.equal(expired.length, 0);
  assert.deepEqual(
    live.map((m) => `${m.topic}:${m.id}`),
    [
      'hot:0', 'cold:0', 'cold:1', 'cold:2',
      'hot:1', 'cold:3', 'cold:4', 'cold:5',
      'hot:2', 'hot:3', 'hot:4', 'hot:5',
    ],
  );
  assert.equal(queue.size, 0);
});

test('BoundedQueue.drainLiveUpToFair: deficit carries across rounds', () => {
  const queue = makeQueue();
  const deficits = new Map<string, number>();
  const weightOf = (g: string): number => (g === 'hot' ? 1 : 3);
  pushAll(queue, [...tagged('hot', 4), ...tagged('cold', 8)]);
  // Budget 2 per round: cold banks deficit it cannot spend yet.
  const round1 = queue.drainLiveUpToFair(0, 2, groupOf, weightOf, deficits);
  assert.deepEqual(
    round1.live.map((m) => `${m.topic}:${m.id}`),
    ['hot:0', 'cold:0'],
  );
  // hot spent its 1, cold banked 3-1=2.
  assert.deepEqual(Object.fromEntries(deficits), { hot: 0, cold: 2 });
  const round2 = queue.drainLiveUpToFair(0, 2, groupOf, weightOf, deficits);
  assert.deepEqual(
    round2.live.map((m) => `${m.topic}:${m.id}`),
    ['hot:1', 'cold:1'],
  );
  // cold: 2+3-1=4 carried.
  assert.equal(deficits.get('cold'), 4);
  const round3 = queue.drainLiveUpToFair(0, 2, groupOf, weightOf, deficits);
  assert.deepEqual(
    round3.live.map((m) => `${m.topic}:${m.id}`),
    ['hot:2', 'cold:2'],
  );
});

test('BoundedQueue.drainLiveUpToFair: a newly joined topic starts at deficit 0', () => {
  const queue = makeQueue();
  const deficits = new Map<string, number>();
  const weightOf = (): number => 1;
  pushAll(queue, tagged('a', 4));
  const round1 = queue.drainLiveUpToFair(0, 2, groupOf, weightOf, deficits);
  assert.deepEqual(round1.live.map((m) => m.topic), ['a', 'a']);
  pushAll(queue, tagged('b', 4));
  // 'b' joins at deficit 0 — it does not jump ahead of 'a'.
  const round2 = queue.drainLiveUpToFair(0, 2, groupOf, weightOf, deficits);
  assert.deepEqual(
    round2.live.map((m) => `${m.topic}:${m.id}`),
    ['a:2', 'b:0'],
  );
});

test('BoundedQueue.drainLiveUpToFair: unselected entries stay queued in FIFO order', () => {
  const queue = makeQueue();
  const deficits = new Map<string, number>();
  pushAll(queue, [...tagged('hot', 3), ...tagged('cold', 3)]);
  const { live } = queue.drainLiveUpToFair(0, 2, groupOf, (g) => (g === 'hot' ? 1 : 3), deficits);
  assert.deepEqual(
    live.map((m) => `${m.topic}:${m.id}`),
    ['hot:0', 'cold:0'],
  );
  // The queue is not reordered: the next drain resumes in FIFO order per
  // topic, and the untouched entries kept their relative positions.
  assert.deepEqual(
    queue.snapshot().map((m) => `${m.topic}:${m.id}`),
    ['hot:1', 'hot:2', 'cold:1', 'cold:2'],
  );
  const round2 = queue.drainLiveUpToFair(0, 10, groupOf, (g) => (g === 'hot' ? 1 : 3), deficits);
  assert.deepEqual(
    round2.live.map((m) => `${m.topic}:${m.id}`),
    ['hot:1', 'cold:1', 'cold:2', 'hot:2'],
  );
});

test('BoundedQueue.drainLiveUpToFair: expiry is swept and counted like the plain drain', () => {
  const queue = makeQueue();
  const deficits = new Map<string, number>();
  queue.push({ topic: 'hot', id: 0 }, 0, 100);
  queue.push({ topic: 'hot', id: 1 }, 0, 50);
  queue.push({ topic: 'cold', id: 0 }, 0, 100);
  queue.push({ topic: 'cold', id: 1 });
  const { live, expired } = queue.drainLiveUpToFair(
    60,
    10,
    groupOf,
    () => 1,
    deficits,
  );
  assert.equal(expired.length, 1);
  assert.equal(expired[0].id, 1);
  assert.equal(expired[0].topic, 'hot');
  assert.equal(queue.expiredCount, 1);
  assert.deepEqual(
    live.map((m) => `${m.topic}:${m.id}`),
    ['hot:0', 'cold:0', 'cold:1'],
  );
});

test('BoundedQueue.drainLiveUpToFair: budget-exempt entries flow freely, ahead of scheduled ones', () => {
  const queue = makeQueue();
  const deficits = new Map<string, number>();
  pushAll(queue, [...tagged('hot', 2), ...tagged('cold', 2)]);
  const isCounting = (item: Tagged): boolean => item.topic === 'hot';
  // Zero budget: only the exempt (cold) entries flow.
  const { live, expired } = queue.drainLiveUpToFair(0, 0, groupOf, () => 1, deficits, isCounting);
  assert.equal(expired.length, 0);
  assert.deepEqual(
    live.map((m) => `${m.topic}:${m.id}`),
    ['cold:0', 'cold:1'],
  );
  // Exempt entries consumed neither budget nor deficit — and with zero
  // budget the DRR never ran, so the map is untouched.
  assert.deepEqual(Object.fromEntries(deficits), {});
  assert.deepEqual(
    queue.snapshot().map((m) => `${m.topic}:${m.id}`),
    ['hot:0', 'hot:1'],
  );
  // With budget, exempt entries still flow first, then the scheduled ones.
  pushAll(queue, tagged('cold', 1));
  const round2 = queue.drainLiveUpToFair(0, 1, groupOf, () => 1, deficits, isCounting);
  assert.deepEqual(
    round2.live.map((m) => `${m.topic}:${m.id}`),
    ['cold:0', 'hot:0'],
  );
});

test('BoundedQueue.drainLiveUpToFair: onlyGroup restricts the drain to one group', () => {
  const queue = makeQueue();
  const deficits = new Map<string, number>([['hot', 2]]);
  pushAll(queue, [...tagged('hot', 2), ...tagged('cold', 2)]);
  const { live } = queue.drainLiveUpToFair(0, 10, groupOf, () => 1, deficits, undefined, 'cold');
  assert.deepEqual(
    live.map((m) => `${m.topic}:${m.id}`),
    ['cold:0', 'cold:1'],
  );
  // The other group's entries stay queued untouched.
  assert.deepEqual(
    queue.snapshot().map((m) => `${m.topic}:${m.id}`),
    ['hot:0', 'hot:1'],
  );
  // The untouched group's deficit carryover survives the round.
  assert.deepEqual(Object.fromEntries(deficits), { hot: 2 });
});

// ---------------------------------------------------------------------------
// LaneQueue.drainLiveUpToFair
// ---------------------------------------------------------------------------

test('LaneQueue.drainLiveUpToFair: single occupied lane delegates verbatim', () => {
  const queue = new LaneQueue<Tagged>({ capacity: 100 });
  const deficits = new Map<string, number>();
  for (const m of [...tagged('hot', 3), ...tagged('cold', 3)]) queue.push(m, 'normal');
  const { live } = queue.drainLiveUpToFair(0, 6, groupOf, (g) => (g === 'hot' ? 1 : 3), deficits);
  assert.deepEqual(
    live.map((m) => `${m.topic}:${m.id}`),
    ['hot:0', 'cold:0', 'cold:1', 'cold:2', 'hot:1', 'hot:2'],
  );
});

test('LaneQueue.drainLiveUpToFair: lanes keep precedence, topics share fairly within a lane', () => {
  const queue = new LaneQueue<Tagged>({ capacity: 100 });
  const deficits = new Map<string, number>();
  for (const m of tagged('x', 2)) queue.push(m, 'urgent');
  for (const m of tagged('y', 2)) queue.push(m, 'urgent');
  for (const m of tagged('x', 2)) queue.push(m, 'bulk');
  for (const m of tagged('y', 2)) queue.push(m, 'bulk');
  const { live } = queue.drainLiveUpToFair(0, 8, groupOf, () => 1, deficits);
  // Urgent first (lane precedence), round-robin within the lane share.
  // Each lane share is an independent DRR round (rotation restarts per
  // call), so the bulk lane — one message per pass at weight 1 — serves
  // its head group first every pass: counts stay fair (2:2), order is
  // head-biased within the tiny share.
  assert.deepEqual(
    live.map((m) => `${m.topic}:${m.id}`),
    ['x:0', 'y:0', 'x:1', 'y:1', 'x:0', 'x:1', 'y:0', 'y:1'],
  );
});

test('LaneQueue.peekFairWinner: resolves the head group without removing anything', () => {
  const queue = new LaneQueue<Tagged>({ capacity: 100 });
  for (const m of [...tagged('cold', 2), ...tagged('hot', 2)]) queue.push(m, 'normal');
  const winner = queue.peekFairWinner(0, groupOf);
  assert.equal(winner, 'cold');
  assert.equal(queue.size, 4);
});

test('LaneQueue.peekFairWinner: falls back to the first exempt entry with no counting entries', () => {
  const queue = new LaneQueue<Tagged>({ capacity: 100 });
  for (const m of tagged('solo', 2)) queue.push(m, 'normal');
  const isCounting = (): boolean => false;
  assert.equal(queue.peekFairWinner(0, groupOf, isCounting), 'solo');
  const empty = new LaneQueue<Tagged>({ capacity: 100 });
  assert.equal(empty.peekFairWinner(0, groupOf), undefined);
});

// ---------------------------------------------------------------------------
// subscribe() validation
// ---------------------------------------------------------------------------

test('subscribe: illegal weights throw RangeError and register nothing', () => {
  const bus = new EventBus();
  const before = bus.getStats().totalSubscribers;
  const bad: Array<unknown> = [
    { weights: { hot: 0 } },
    { weights: { hot: -1 } },
    { weights: { hot: 1.5 } },
    { weights: { hot: NaN } },
    { weights: { hot: Infinity } },
    { weights: { '': 1 } },
    { weights: { hot: '2' } },
    { weights: 'nope' },
    { weights: [{ hot: 1 }] },
    42,
    'yes',
  ];
  for (const value of bad) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { topicFairness: value as never }),
      RangeError,
      `expected RangeError for ${JSON.stringify(value)}`,
    );
  }
  assert.equal(bus.getStats().totalSubscribers, before);
});

test('subscribe: topicFairness cannot be combined with shadow', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { topicFairness: true, shadow: true }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        topicFairness: { weights: { t: 2 } },
        shadow: { queueSize: 10 },
      }),
    RangeError,
  );
});

test('resolveTopicFairnessOptions: true enables with default weights, false/undefined disables', () => {
  assert.equal(resolveTopicFairnessOptions(undefined, 'subscribe'), undefined);
  assert.equal(resolveTopicFairnessOptions(false, 'subscribe'), undefined);
  const enabled = resolveTopicFairnessOptions(true, 'subscribe');
  assert.ok(enabled !== undefined);
  assert.equal(enabled.weightOf('anything'), 1);
  const weighted = resolveTopicFairnessOptions({ weights: { hot: 2 } }, 'subscribe');
  assert.ok(weighted !== undefined);
  assert.equal(weighted.weightOf('hot'), 2);
  assert.equal(weighted.weightOf('cold'), 1);
});

// ---------------------------------------------------------------------------
// Bus integration: plain drain
// ---------------------------------------------------------------------------

test('fair drain: 1:3 interleave at single-message granularity, per-topic FIFO', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 1, cold: 3 } },
  });
  for (let i = 0; i < 6; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(received, [
    'hot:0', 'cold:0', 'cold:1', 'cold:2',
    'hot:1', 'cold:3', 'cold:4', 'cold:5',
    'hot:2', 'hot:3', 'hot:4', 'hot:5',
  ]);
  // No gaps: per-topic order is preserved, so the gap detector stays silent.
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('fair drain: disabled by default — plain FIFO order is preserved', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`));
  for (let i = 0; i < 3; i++) bus.publish('hot', i);
  for (let i = 0; i < 3; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(received, ['hot:0', 'hot:1', 'hot:2', 'cold:0', 'cold:1', 'cold:2']);
  assert.deepEqual(bus.getStats().topicFairness, { scheduledByTopic: {}, deficits: {} });
});

test('fair drain: topicFairness true enables fair scheduling with default weights', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: true,
  });
  for (let i = 0; i < 2; i++) bus.publish('a', i);
  for (let i = 0; i < 2; i++) bus.publish('b', i);
  await flush();
  await flush();
  assert.deepEqual(received, ['a:0', 'b:0', 'a:1', 'b:1']);
});

test('fair drain: unconfigured topics drain at weight 1', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 2 } },
  });
  for (let i = 0; i < 4; i++) bus.publish('hot', i);
  for (let i = 0; i < 2; i++) bus.publish('cold', i);
  await flush();
  await flush();
  assert.deepEqual(received, ['hot:0', 'hot:1', 'cold:0', 'hot:2', 'hot:3', 'cold:1']);
});

test('fair drain: composes with deliveryShaping — budget per round, 1:3 across rounds', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: string[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(`${msg.topic}:${String(msg.payload)}`), {
    topicFairness: { weights: { hot: 1, cold: 3 } },
    deliveryShaping: { messagesPerSec: 4, burst: 4 },
  });
  for (let i = 0; i < 12; i++) bus.publish('hot', `h${i}`);
  for (let i = 0; i < 12; i++) bus.publish('cold', `c${i}`);
  await flush();
  // Round 1: exactly one hot and three colds.
  assert.deepEqual(received, ['hot:h0', 'cold:c0', 'cold:c1', 'cold:c2']);
  // Both topics spent exactly their weight: no deficit carried.
  assert.deepEqual(bus.getStats().topicFairness.deficits, { hot: 0, cold: 0 });
  // Drive the next rounds: refill the shaping bucket, then publish to
  // trigger the flush — the extra cold messages join the tail and do not
  // disturb the 1:3 head-of-line order.
  const expectedRounds = [
    ['hot:h1', 'cold:c3', 'cold:c4', 'cold:c5'],
    ['hot:h2', 'cold:c6', 'cold:c7', 'cold:c8'],
    ['hot:h3', 'cold:c9', 'cold:c10', 'cold:c11'],
  ];
  for (let round = 0; round < expectedRounds.length; round += 1) {
    clock.advance(1000);
    bus.publish('cold', `pad${round}`);
    await flush();
    await flush();
    assert.deepEqual(received.slice(-4), expectedRounds[round], `round ${round + 2}`);
  }
  assert.equal(bus.getStats().topicFairness.scheduledByTopic.hot, 4);
  assert.equal(bus.getStats().topicFairness.scheduledByTopic.cold, 12);
  sub.unsubscribe();
});

// ---------------------------------------------------------------------------
// Bus integration: batch + fairness
// ---------------------------------------------------------------------------

test('fair batch: collected batches never mix topics', async () => {
  const bus = new EventBus();
  const batches: string[][] = [];
  bus.subscribe(
    '*',
    (msgs) => batches.push(msgs.map((m) => `${m.topic}:${String(m.payload)}`)),
    {
      batch: { maxSize: 3, maxWaitMs: 10_000 },
      topicFairness: { weights: { hot: 1, cold: 3 } },
    },
  );
  for (let i = 0; i < 3; i++) bus.publish('hot', i);
  for (let i = 0; i < 3; i++) bus.publish('cold', i);
  await flush();
  await flush();
  await flush();
  // The round-robin winner (hot) fills the first batch alone; cold fills
  // the second — every batch holds a single topic.
  assert.deepEqual(batches, [
    ['hot:0', 'hot:1', 'hot:2'],
    ['cold:0', 'cold:1', 'cold:2'],
  ]);
  assert.deepEqual(bus.getStats().topicFairness.scheduledByTopic, { hot: 3, cold: 3 });
});

test('fair batch: weights hold at message granularity across batches', async () => {
  const bus = new EventBus();
  const batches: string[][] = [];
  bus.subscribe(
    '*',
    (msgs) => batches.push(msgs.map((m) => `${m.topic}:${String(m.payload)}`)),
    {
      batch: { maxSize: 2, maxWaitMs: 10_000 },
      topicFairness: { weights: { hot: 1, cold: 3 } },
    },
  );
  for (let i = 0; i < 2; i++) bus.publish('hot', i);
  for (let i = 0; i < 6; i++) bus.publish('cold', i);
  await flush();
  await flush();
  await flush();
  await flush();
  for (const batch of batches) {
    const topics = new Set(batch.map((m) => m.split(':')[0]));
    assert.equal(topics.size, 1, `mixed batch: ${batch.join(',')}`);
  }
  // 1 hot per 3 cold across the batch sequence.
  assert.deepEqual(
    batches.flat(),
    ['hot:0', 'hot:1', 'cold:0', 'cold:1', 'cold:2', 'cold:3', 'cold:4', 'cold:5'],
  );
});

// ---------------------------------------------------------------------------
// Stats + Prometheus
// ---------------------------------------------------------------------------

test('getStats().topicFairness: bus-level aggregates across subscribers', async () => {
  const bus = new EventBus();
  bus.subscribe('hot', () => {}, { topicFairness: { weights: { hot: 2 } } });
  bus.subscribe('*', () => {}, { topicFairness: true });
  bus.subscribe('cold', () => {});
  for (let i = 0; i < 4; i++) bus.publish('hot', i);
  for (let i = 0; i < 4; i++) bus.publish('cold', i);
  await flush();
  await flush();
  const fair = bus.getStats().topicFairness;
  // Two fair subscribers each scheduled every message they received:
  // the 'hot'-pattern subscriber saw 4 hot, the '*'-subscriber saw 4+4.
  assert.equal(fair.scheduledByTopic.hot, 8);
  assert.equal(fair.scheduledByTopic.cold, 4);
  assert.deepEqual(fair.deficits, {});
});

test('renderPrometheus: emits eventbus_fair_scheduled_messages_total per topic', async () => {
  const bus = new EventBus();
  bus.subscribe('*', () => {}, { topicFairness: { weights: { hot: 1, cold: 3 } } });
  for (let i = 0; i < 2; i++) bus.publish('hot', i);
  for (let i = 0; i < 2; i++) bus.publish('cold', i);
  await flush();
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('# HELP eventbus_fair_scheduled_messages_total'));
  assert.ok(text.includes('# TYPE eventbus_fair_scheduled_messages_total counter'));
  assert.ok(text.includes('eventbus_fair_scheduled_messages_total{topic="hot"} 2'));
  assert.ok(text.includes('eventbus_fair_scheduled_messages_total{topic="cold"} 2'));
  // Label escaping: a topic with quotes/backslashes renders escaped.
  const bus2 = new EventBus();
  bus2.subscribe('*', () => {}, { topicFairness: true });
  bus2.publish('we"ird\\topic', 0);
  await flush();
  await flush();
  const text2 = renderPrometheus(bus2.getStats());
  assert.ok(text2.includes('eventbus_fair_scheduled_messages_total{topic="we\\"ird\\\\topic"} 1'));
});

test('renderPrometheus: no fair series for topics without scheduled messages', async () => {
  const bus = new EventBus();
  bus.subscribe('*', () => {});
  bus.publish('t', 1);
  await flush();
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('# HELP eventbus_fair_scheduled_messages_total'));
  assert.equal(
    text.split('\n').filter((line) => line.startsWith('eventbus_fair_scheduled_messages_total{'))
      .length,
    0,
  );
});

test('renderPrometheus: tolerates a stats object that predates topicFairness', async () => {
  const bus = new EventBus();
  bus.publish('t', 1);
  await flush();
  await flush();
  const stats = bus.getStats() as unknown as Record<string, unknown>;
  delete stats.topicFairness;
  const text = renderPrometheus(stats as unknown as Parameters<typeof renderPrometheus>[0]);
  assert.ok(text.includes('# HELP eventbus_fair_scheduled_messages_total'));
  assert.equal(
    text.split('\n').filter((line) => line.startsWith('eventbus_fair_scheduled_messages_total{'))
      .length,
    0,
  );
});
