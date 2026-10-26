import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('publishDelayedBatch returns one delay id per entry in order, and each fans out at its own due time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const ids = bus.publishDelayedBatch([
    { topic: 't', payload: 'first', delayMs: 5000 },
    { topic: 't', payload: 'second', delayMs: 1000 },
    { topic: 't', payload: 'third', deliverAt: 9000 },
  ]);
  assert.equal(ids.length, 3);
  assert.equal(new Set(ids).size, 3); // distinct ids, in entry order
  assert.deepEqual(
    bus.listDelayed().map((e) => e.delayId),
    ids,
  );
  assert.deepEqual(
    bus.listDelayed().map((e) => e.deliverAt),
    [6000, 2000, 9000],
  );
  // Live 'poke' messages queue synchronously during publish(); the sweep
  // fans each due entry out after them (the delayed.test.ts convention).
  clock.nowMs = 2000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'second']);
  clock.nowMs = 6000;
  bus.publish('t', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'second', 'poke2', 'first']);
  clock.nowMs = 9000;
  bus.publish('t', 'poke3');
  await flush();
  assert.deepEqual(received, ['poke', 'second', 'poke2', 'first', 'poke3', 'third']);
});

test('publishDelayedBatch carries keys, and a zero-delay entry fans out on the next flush', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const ids = bus.publishDelayedBatch([
    { topic: 't', payload: 'now', delayMs: 0 },
    { topic: 't', payload: 'keyed', delayMs: 5000, key: 'k1' },
  ]);
  await flush();
  assert.deepEqual(received, ['now']);
  assert.equal(bus.listDelayed()[0].key, 'k1');
  assert.equal(bus.listDelayed()[0].delayId, ids[1]);
  assert.equal(bus.cancelDelayedByKey('t', 'k1').cancelled, 1);
});

test('publishDelayedBatch of an empty batch returns an empty array and schedules nothing', () => {
  const bus = new EventBus();
  assert.deepEqual(bus.publishDelayedBatch([]), []);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('publishDelayedBatch validates every entry before scheduling anything (all-or-nothing)', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const badBatches: Array<unknown> = [
    'not-an-array',
    [{ topic: 't', payload: 'x' }], // neither timing
    [{ topic: 't', payload: 'x', delayMs: 1000, deliverAt: 9000 }], // both timings
    [{ topic: 't', payload: 'x', delayMs: -5 }], // negative delay
    [{ topic: 't', payload: 'x', deliverAt: Number.NaN }], // non-finite deliverAt
    [{ topic: '', payload: 'x', delayMs: 1000 }], // empty topic
    [{ topic: 42, payload: 'x', delayMs: 1000 }], // non-string topic
    [{ topic: 't', payload: 'x', delayMs: 1000, key: '' }], // empty key
    [null], // non-object entry
    // A valid entry followed by an invalid one: the valid one must NOT be scheduled.
    [
      { topic: 't', payload: 'good', delayMs: 1000 },
      { topic: 't', payload: 'bad', delayMs: -1 },
    ],
  ];
  for (const batch of badBatches) {
    assert.throws(() => bus.publishDelayedBatch(batch as never));
    assert.equal(bus.getStats().pendingDelayed, 0, `batch must leave zero schedules: ${JSON.stringify(batch)}`);
    assert.deepEqual(bus.listDelayed(), []);
  }
});

test('publishDelayedBatch rolls back already-scheduled entries when a later entry is refused by admission', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  // A schema validator that rejects exactly one payload: validation
  // passes for every entry (the payloads are well-formed), but
  // admission refuses the second entry — so the batch must unwind.
  bus.setTopicSchema('t', (payload) => payload !== 'rejected');
  assert.throws(() =>
    bus.publishDelayedBatch([
      { topic: 't', payload: 'accepted', delayMs: 1000 },
      { topic: 't', payload: 'rejected', delayMs: 2000 },
    ]),
  );
  assert.equal(bus.getStats().pendingDelayed, 0);
  assert.deepEqual(bus.listDelayed(), []);
  // The rolled-back entry never fans out later either.
  clock.nowMs = 9000;
  bus.publish('t', 'poke');
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('publishDelayedBatch entries for different topics schedule independently', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const receivedA: unknown[] = [];
  const receivedB: unknown[] = [];
  bus.subscribe('a', (msg) => receivedA.push(msg.payload));
  bus.subscribe('b', (msg) => receivedB.push(msg.payload));
  bus.publishDelayedBatch([
    { topic: 'a', payload: 'for-a', delayMs: 1000 },
    { topic: 'b', payload: 'for-b', delayMs: 2000 },
  ]);
  clock.nowMs = 2000;
  bus.publish('a', 'poke');
  await flush();
  assert.deepEqual(receivedA, ['poke', 'for-a']);
  assert.deepEqual(receivedB, []);
  clock.nowMs = 3000;
  bus.publish('b', 'poke');
  await flush();
  assert.deepEqual(receivedB, ['poke', 'for-b']);
});

test('publishDelayedBatch matches publishDelayed for a single entry, including immediate fan-out', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const [id] = bus.publishDelayedBatch([{ topic: 't', payload: 'solo', delayMs: 1000 }]);
  assert.equal(typeof id, 'string');
  assert.ok(id.startsWith('delayed-'));
  assert.equal(bus.cancelDelayed(id), true);
  assert.deepEqual(bus.listDelayed(), []);
  bus.publishDelayedBatch([{ topic: 't', payload: 'immediate', delayMs: 0 }]);
  await flush();
  assert.deepEqual(received, ['immediate']);
});
