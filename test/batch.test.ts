import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { Delivery } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Injectable clock so linger-adjacent timing stays deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

test('disabled by default: one handler call per message', async () => {
  const bus = new EventBus();
  const calls: unknown[][] = [];
  bus.subscribe('t', (msgs) => calls.push(msgs));
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  // Plain handler receives single messages, not arrays.
  assert.equal(calls.length, 3);
  assert.deepEqual(calls.map((c) => (c as { payload: unknown }).payload), [0, 1, 2]);
});

test('full batch delivers immediately as one array call', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 3, maxWaitMs: 10_000 },
  });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  // A full batch never waits for the linger timer.
  assert.deepEqual(batches, [[0, 1, 2]]);
});

test('batching continues across flushes until the backlog drains', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  const sub = bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 3, maxWaitMs: 5 },
  });
  for (let i = 0; i < 7; i++) bus.publish('t', i);
  await flush();
  await sleep(30);
  await flush();
  assert.deepEqual(batches, [
    [0, 1, 2],
    [3, 4, 5],
    [6],
  ]);
  sub.unsubscribe();
});

test('partial batch lingers up to maxWaitMs, then delivers', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 5, maxWaitMs: 40 },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  // Still lingering — nothing delivered yet.
  assert.deepEqual(batches, []);
  await sleep(80);
  assert.deepEqual(batches, [['a', 'b']]);
});

test('maxWaitMs: 0 delivers whatever is queued on every flush', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 100, maxWaitMs: 0 },
  });
  for (let i = 0; i < 7; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  await flush();
  assert.deepEqual(batches, [[0, 1, 2, 3, 4, 5, 6]]);
});

test('linger collects messages that arrive while waiting', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 5, maxWaitMs: 60 },
  });
  bus.publish('t', 'a');
  await flush();
  await sleep(20);
  bus.publish('t', 'b');
  await flush();
  await sleep(70);
  // Both arrivals landed in the same lingered batch.
  assert.deepEqual(batches, [['a', 'b']]);
});

test('invalid batch options throw RangeError/TypeError before registering', () => {
  const bus = new EventBus();
  const before = bus.subscriberCount();
  for (const bad of [
    { maxSize: 0 },
    { maxSize: -1 },
    { maxSize: 1.5 },
    { maxSize: NaN },
    { maxWaitMs: -1 },
    { maxWaitMs: NaN },
    { maxWaitMs: Infinity },
  ]) {
    assert.throws(() => bus.subscribe('t', () => {}, { batch: bad }), RangeError);
  }
  assert.throws(() => bus.subscribe('t', () => {}, { batch: 'yes' as never }), TypeError);
  assert.equal(bus.subscriberCount(), before);
});

test('batch: true uses the documented defaults', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), { batch: true });
  for (let i = 0; i < 100; i++) bus.publish('t', i);
  await flush();
  // Default maxSize 100: one full batch, delivered without lingering.
  assert.equal(batches.length, 1);
  assert.equal(batches[0].length, 100);
});

test('unsubscribe drops the pending partial batch and clears the timer', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  const sub = bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 5, maxWaitMs: 30 },
  });
  bus.publish('t', 'a');
  await flush();
  sub.unsubscribe();
  await sleep(60);
  await flush();
  assert.deepEqual(batches, []);
});

test('TTL: a message that expires while the batch lingers is dropped, not resurrected', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.setTopicTtl('t', 30);
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 5, maxWaitMs: 80 },
  });
  bus.publish('t', 'doomed');
  await flush();
  // Still lingering — nothing delivered yet.
  assert.deepEqual(batches, []);
  // The linger timer fires at ~80ms, well past the 30ms TTL: the message
  // is dropped as expired at hand-off, never delivered.
  await sleep(150);
  assert.deepEqual(batches, []);
  assert.equal(bus.getStats().topics[0].expiredMessages, 1);
});

test('sequence gap detection counts per message inside batches', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.seq)), {
    batch: { maxSize: 10, maxWaitMs: 0 },
    queueSize: 4,
  });
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  await sleep(10);
  // Baseline established at seq 2; now flood past the queue capacity so
  // seqs 3 and 4 are shed by backpressure before any delivery.
  for (let i = 3; i <= 8; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  await flush();
  assert.deepEqual(batches, [
    [1, 2],
    [5, 6, 7, 8],
  ]);
  // The two shed messages surface as sequence gaps on the batched path.
  assert.equal(bus.getStats().topics[0].sequenceGaps, 2);
});

test('delivery latency samples every message in the batch', async () => {
  const clock = makeClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, {
    batch: { maxSize: 3, maxWaitMs: 0 },
    deliveryLatency: true,
  });
  bus.publish('t', 'a');
  clock.advance(5);
  bus.publish('t', 'b');
  clock.advance(5);
  bus.publish('t', 'c');
  clock.advance(20);
  await flush();
  await sleep(10);
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.deliveryLatency.length, 1);
  assert.equal(stats.deliveryLatency[0].samples, 3);
});

test('reliable batch: handler receives Delivery envelopes, ack confirms the batch', async () => {
  const bus = new EventBus();
  const seen: Array<Array<Delivery<{ payload: unknown }>>> = [];
  bus.subscribeReliable(
    't',
    (deliveries) => {
      seen.push(deliveries);
      for (const d of deliveries) d.ack();
    },
    { batch: { maxSize: 3, maxWaitMs: 0 } },
  );
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  await flush();
  assert.equal(seen.length, 1);
  assert.deepEqual(
    seen[0].map((d) => d.msg.payload),
    [0, 1, 2],
  );
  // Every delivery settled: nothing outstanding, no redelivery timers left.
  assert.equal(bus.getStats().unackedDeliveries, 0);
  await sleep(30);
  assert.equal(seen.length, 1);
});

test('reliable batch: nacking the whole batch requeues it in FIFO order', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  let nacked = false;
  bus.subscribeReliable(
    't',
    (deliveries) => {
      batches.push(deliveries.map((d) => d.msg.payload));
      if (!nacked) {
        nacked = true;
        for (const d of deliveries) d.nack();
      } else {
        for (const d of deliveries) d.ack();
      }
    },
    { batch: { maxSize: 3, maxWaitMs: 0 }, ackTimeoutMs: 10_000 },
  );
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  await flush();
  await sleep(10);
  await flush();
  // First delivery nacked as a unit, redelivered as a unit in order.
  assert.deepEqual(batches, [
    [0, 1, 2],
    [0, 1, 2],
  ]);
});

test('reliable batch with DLQ: a throwing batch handler dead-letters every message', async () => {
  const bus = new EventBus();
  let calls = 0;
  const sub = bus.subscribeReliable(
    't',
    () => {
      calls += 1;
      throw new Error('poison');
    },
    {
      batch: { maxSize: 2, maxWaitMs: 0 },
      ackTimeoutMs: 10_000,
      deadLetter: { maxRedeliveries: 1, maxEntries: 10 },
    },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  // Each failed batch nacks both messages; after maxRedeliveries they land
  // in the DLQ instead of being requeued forever.
  for (let i = 0; i < 10 && bus.getDeadLetterMessages(sub.id).length < 2; i++) {
    // eslint-disable-next-line no-await-in-loop
    await flush();
    // eslint-disable-next-line no-await-in-loop
    await sleep(5);
  }
  const dlq = bus.getDeadLetterMessages(sub.id);
  assert.equal(dlq.length, 2);
  assert.deepEqual(dlq.map((e) => e.payload), ['a', 'b']);
  assert.ok(calls >= 2);
  sub.unsubscribe();
});

test('batch composes with delivery shaping: the batch never exceeds the budget', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const batches: unknown[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 10, maxWaitMs: 0 },
    deliveryShaping: { messagesPerSec: 4, burst: 4 },
  });
  for (let i = 0; i < 8; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  // Burst of 4 delivered as one batch of 4; the other 4 wait for budget.
  assert.deepEqual(batches, [[0, 1, 2, 3]]);
  clock.advance(1000); // refill to the burst cap
  bus.publish('t', 8); // also triggers a flush
  await flush();
  await sleep(10);
  await flush();
  // The refilled budget of 4 delivered one more batch of 4; message 8
  // waits for the next refill — the batch never outruns the budget.
  assert.deepEqual(batches, [
    [0, 1, 2, 3],
    [4, 5, 6, 7],
  ]);
});

test('batch composes with the health probe: one batch call is one failure', async () => {
  const bus = new EventBus();
  const degraded: string[] = [];
  const sub = bus.subscribe('t', () => {
    throw new Error('boom');
  }, {
    batch: { maxSize: 5, maxWaitMs: 0 },
    healthProbe: { maxConsecutiveFailures: 2 },
    onDegraded: (e) => degraded.push(e.subscriberId),
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  // One failed batch call: a single failure, not five.
  assert.deepEqual(degraded, []);
  for (let i = 5; i < 10; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  await flush();
  // Two failed batch calls trip the probe.
  assert.deepEqual(degraded, [sub.id]);
  assert.equal(bus.getStats().degradedSubscribers, 1);
  assert.ok(bus.resume(sub.id));
  assert.equal(bus.getStats().degradedSubscribers, 0);
  sub.unsubscribe();
});

test('batch composes with the content filter: filtered messages never join a batch', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 10, maxWaitMs: 0 },
    filter: (payload) => (payload as number) % 2 === 0,
  });
  for (let i = 0; i < 6; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  await flush();
  assert.deepEqual(batches, [[0, 2, 4]]);
});
