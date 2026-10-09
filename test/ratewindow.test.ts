import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { SlidingWindowLimiter } from '../src/ratewindow.ts';

/** Injectable clock so window slides are deterministic. */
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
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function waitingFor(bus: EventBus, subscriberId: string): number {
  const row = bus.getStats().rateLimitedWaiting.find((r) => r.subscriberId === subscriberId);
  assert.ok(row !== undefined, 'expected a rateLimitedWaiting row');
  return row.waiting;
}

test('disabled by default: no rows, everything delivers in one flush', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.deepEqual(bus.getStats().rateLimitedWaiting, []);
});

test('over-budget messages wait in the queue: no drops, no gaps', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 3, perWindowMs: 1000 },
  });
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', i), 1);
  await flush();
  // Hard cap of 3 per rolling second: the other 2 wait — never dropped.
  assert.deepEqual(received, [0, 1, 2]);
  assert.equal(bus.pendingCount(sub.id), 2);
  assert.equal(bus.droppedCount(sub.id), 0);
  assert.equal(waitingFor(bus, sub.id), 2);
  // Nothing was shed, so the gap detector stays silent.
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('the window slides: held messages resume in FIFO order', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 3, perWindowMs: 1000 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1, 2]);
  assert.equal(waitingFor(bus, sub.id), 2);
  clock.advance(1001); // the three deliveries at t=0 slide out of the window
  bus.publish('t', 'kick'); // also triggers a flush
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 'kick']);
  assert.equal(bus.pendingCount(sub.id), 0);
  assert.equal(waitingFor(bus, sub.id), 0);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('window edge: a delivery exactly perWindowMs old still counts', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 1, perWindowMs: 1000 },
  });
  bus.publish('t', 'a');
  await flush();
  assert.deepEqual(received, ['a']);
  clock.advance(1000); // exactly one window: 'a' still counts (nowMs - t <= perWindowMs)
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(received, ['a']);
  assert.equal(waitingFor(bus, sub.id), 1);
  clock.advance(1); // one millisecond older: 'a' slides out
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  assert.equal(waitingFor(bus, sub.id), 1); // 'c' now waits behind 'b'
  sub.unsubscribe();
});

test('no gradual refill: the full window stays blocked until it slides', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 2, perWindowMs: 1000 },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  // A token bucket would have refilled ~2 by now; the exact window has not.
  clock.advance(999);
  bus.publish('t', 'd');
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  assert.equal(waitingFor(bus, sub.id), 2);
  clock.advance(1); // t=1000: still counts; t=1001: slides out
  clock.advance(1);
  bus.publish('t', 'e');
  await flush();
  assert.deepEqual(received, ['a', 'b', 'c', 'd']);
  assert.equal(bus.pendingCount(sub.id), 1); // 'e' waits behind the fresh window
  sub.unsubscribe();
});

test('invalid rateLimit options throw RangeError and register nothing', () => {
  const bus = new EventBus();
  const bad = [
    { maxMessages: 0, perWindowMs: 1000 },
    { maxMessages: -2, perWindowMs: 1000 },
    { maxMessages: 1.5, perWindowMs: 1000 },
    { maxMessages: Number.NaN, perWindowMs: 1000 },
    { maxMessages: Number.POSITIVE_INFINITY, perWindowMs: 1000 },
    { perWindowMs: 1000 }, // missing maxMessages
    { maxMessages: 2, perWindowMs: 0 },
    { maxMessages: 2, perWindowMs: -5 },
    { maxMessages: 2, perWindowMs: Number.NaN },
    { maxMessages: 2, perWindowMs: Number.POSITIVE_INFINITY },
    { maxMessages: 2 }, // missing perWindowMs
    {},
  ];
  for (const rateLimit of bad) {
    // Deliberately malformed: cast past the type system to test runtime validation.
    assert.throws(() => bus.subscribe('t', () => {}, { rateLimit: rateLimit as never }), RangeError);
  }
  assert.throws(() => bus.subscribe('t', () => {}, { rateLimit: true as never }), RangeError);
  assert.equal(bus.subscriberCount(), 0);
});

test('composes with deliveryShaping: shaping paces, the window caps', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    deliveryShaping: { messagesPerSec: 10, burst: 2 },
    rateLimit: { maxMessages: 3, perWindowMs: 1000 },
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  // Shaping binds first (burst of 2); the window still has room for one more.
  assert.deepEqual(received, [0, 1]);
  assert.equal(bus.getStats().shapedSubscribers, 1);
  assert.equal(waitingFor(bus, sub.id), 0);
  clock.advance(100); // one shaping token refills; the window still has budget
  bus.publish('t', 5);
  await flush();
  assert.deepEqual(received, [0, 1, 2]);
  // The window is now full (3 deliveries in the trailing second): even
  // with shaping budget available, message 3 waits on the window.
  assert.equal(waitingFor(bus, sub.id), 3);
  clock.advance(100); // t=200: shaping refills, but the window still binds
  bus.publish('t', 6);
  await flush();
  assert.deepEqual(received, [0, 1, 2]);
  assert.equal(waitingFor(bus, sub.id), 4);
  // Drain the rest: each step slides the window past the oldest delivery
  // and publishes to an empty topic — a flush trigger that adds nothing to
  // this subscriber's queue.
  for (let i = 0; i < 12 && bus.pendingCount(sub.id) > 0; i++) {
    clock.advance(1000);
    bus.publish('other', i);
    await flush();
  }
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5, 6]);
  assert.equal(bus.pendingCount(sub.id), 0);
  assert.equal(waitingFor(bus, sub.id), 0);
  assert.equal(bus.getStats().shapedSubscribers, 0);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('rateLimit does not extend TTL: messages that expire while waiting are dropped', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 1500);
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 1, perWindowMs: 1000 },
  });
  bus.publish('t', 'a'); // deadline 1500
  bus.publish('t', 'b'); // deadline 1500
  await flush();
  assert.deepEqual(received, ['a']);
  assert.equal(waitingFor(bus, sub.id), 1);
  clock.advance(1600); // t=1600: 'b' outlived its TTL while waiting; 'a' slides out of the window
  bus.publish('t', 'c'); // deadline 3100
  bus.publish('t', 'd'); // deadline 3100
  await flush();
  // 'b' expired without consuming window budget: the freed window delivered
  // 'c' in the same flush, while 'd' waits behind it.
  assert.deepEqual(received, ['a', 'c']);
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.expiredMessages, 1);
  assert.equal(waitingFor(bus, sub.id), 1);
  clock.advance(1001); // t=2601: 'c' slides out of the window
  bus.publish('other', 'kick'); // flush trigger only; adds nothing to 't'
  await flush();
  assert.deepEqual(received, ['a', 'c', 'd']);
  assert.equal(waitingFor(bus, sub.id), 0);
  sub.unsubscribe();
});

test('filter runs before the window: filtered messages consume no budget', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    filter: (payload) => (payload as number) % 2 === 0,
    rateLimit: { maxMessages: 2, perWindowMs: 1000 },
  });
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  await flush();
  // 1 and 3 never reached the queue, so the window still had room for
  // both survivors.
  assert.deepEqual(received, [0, 2]);
  assert.equal(bus.getStats().filteredMessages, 2);
  bus.publish('t', 4);
  await flush();
  // The window is full from the two real deliveries: 4 waits.
  assert.deepEqual(received, [0, 2]);
  assert.equal(waitingFor(bus, sub.id), 1);
  sub.unsubscribe();
});

test('health-probe degradation keeps the window: nothing is consumed while paused', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const attempts: unknown[] = [];
  let fail = true;
  const sub = bus.subscribe(
    't',
    (msg) => {
      attempts.push(msg.payload);
      if (fail) throw new Error('boom');
    },
    {
      healthProbe: { maxConsecutiveFailures: 1 },
      rateLimit: { maxMessages: 1, perWindowMs: 1000 },
    },
  );
  bus.publish('t', 'a');
  await flush();
  assert.deepEqual(attempts, ['a']);
  assert.equal(bus.subscriberHealth(sub.id).degraded, true);
  bus.publish('t', 'b');
  await flush();
  // Degraded: 'b' stays queued and the window is untouched while paused.
  assert.deepEqual(attempts, ['a']);
  assert.equal(waitingFor(bus, sub.id), 0);
  fail = false;
  assert.equal(bus.resume(sub.id), true);
  await flush();
  // The window still holds 'a' (t=0): 'b' keeps waiting after resume.
  assert.deepEqual(attempts, ['a']);
  assert.equal(waitingFor(bus, sub.id), 1);
  clock.advance(1001);
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(attempts, ['a', 'b']);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('re-flush timer drains the backlog with no new publishes (real clock)', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 2, perWindowMs: 200 },
  });
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [0, 1]);
  assert.equal(waitingFor(bus, sub.id), 2);
  // No more publishes: the timer fires when the window slides and keeps
  // draining on its own.
  await sleep(500);
  assert.deepEqual(received, [0, 1, 2, 3]);
  assert.equal(waitingFor(bus, sub.id), 0);
  sub.unsubscribe();
});

test('unsubscribing a rate-limited subscriber is clean', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), {
    rateLimit: { maxMessages: 1, perWindowMs: 60_000 },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(received, ['a']);
  assert.equal(waitingFor(bus, sub.id), 1);
  sub.unsubscribe();
  assert.deepEqual(
    bus.getStats().rateLimitedWaiting.filter((r) => r.subscriberId === sub.id),
    [],
  );
  // The armed re-flush timer was cleared: advancing the clock and flushing
  // delivers nothing to the gone subscriber.
  clock.advance(60_001);
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(received, ['a']);
});

test('rateLimit composes with reliable subscriptions', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  const sub = bus.subscribeReliable(
    't',
    (delivery) => {
      received.push(delivery.msg.payload);
      delivery.ack();
    },
    { rateLimit: { maxMessages: 1, perWindowMs: 1000 } },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(received, ['a']);
  assert.equal(waitingFor(bus, sub.id), 1);
  clock.advance(1001);
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  assert.equal(waitingFor(bus, sub.id), 1); // 'c' waits behind 'b'
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('rateLimit composes with batch delivery: the window bounds collection', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[][] = [];
  const sub = bus.subscribe(
    't',
    (batch) => received.push((batch as unknown as { payload: unknown }[]).map((m) => m.payload)),
    {
      batch: { maxSize: 2, maxWaitMs: 50 },
      rateLimit: { maxMessages: 2, perWindowMs: 1000 },
    },
  );
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  await flush();
  // The window allowed exactly one full batch; the rest wait.
  assert.deepEqual(received, [[0, 1]]);
  assert.equal(waitingFor(bus, sub.id), 2);
  clock.advance(1000); // edge: the t=0 deliveries still count
  bus.publish('t', 4);
  await flush();
  assert.deepEqual(received, [[0, 1]]);
  clock.advance(1); // the window slides
  bus.publish('t', 5);
  await flush();
  assert.deepEqual(received, [
    [0, 1],
    [2, 3],
  ]);
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('SlidingWindowLimiter: budget and msUntilBudget', () => {
  const limiter = new SlidingWindowLimiter(2, 1000);
  assert.equal(limiter.budget(0), 2);
  assert.equal(limiter.msUntilBudget(0), 0);
  limiter.record(0);
  limiter.record(100);
  assert.equal(limiter.budget(100), 0);
  // The window frees a slot exactly when the oldest delivery slides out.
  assert.equal(limiter.msUntilBudget(100), 900);
  assert.equal(limiter.budget(1000), 0); // exactly perWindowMs old: still counts
  assert.equal(limiter.budget(1001), 1);
  assert.equal(limiter.msUntilBudget(1001), 0);
  assert.throws(() => new SlidingWindowLimiter(0, 1000), RangeError);
  assert.throws(() => new SlidingWindowLimiter(2, 0), RangeError);
});
