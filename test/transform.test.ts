import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('transform chain applies in registration order; subscriber sees the transformed payload', async () => {
  const bus = new EventBus();
  const order: string[] = [];
  bus.setTopicTransforms('orders.**', [
    (p) => {
      order.push('first');
      return { ...(p as Record<string, unknown>), step1: true };
    },
    (p, topic) => {
      order.push('second');
      assert.equal(topic, 'orders.created');
      return { ...(p as Record<string, unknown>), step2: true };
    },
  ]);
  const received: BusMessage[] = [];
  bus.subscribe('orders.**', (m) => received.push(m));
  bus.publish('orders.created', { id: 1 });
  await flush();
  assert.deepEqual(order, ['first', 'second']);
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { id: 1, step1: true, step2: true });
  const stats = bus.getStats();
  assert.equal(stats.topics[0].transformCalls, 2);
  assert.equal(stats.topics[0].transformErrors, 0);
  assert.equal(stats.transformCalls, 2);
  assert.equal(stats.transformErrors, 0);
});

test('transform runs before schema validation: the schema sees the transformed payload', async () => {
  const bus = new EventBus();
  // The transform completes a missing required field; the schema must
  // see the completed payload and admit it.
  bus.setTopicTransforms('t', [(p) => ({ ...(p as Record<string, unknown>), kind: 'filled' })]);
  bus.setTopicSchema('t', (p) => (p as any).kind === 'filled');
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  assert.equal(bus.publish('t', {}), 1);
  await flush();
  assert.deepEqual(received[0].payload, { kind: 'filled' });
  const stats = bus.getStats();
  assert.equal(stats.topics[0].transformCalls, 1);
  assert.equal(stats.topics[0].rejectedMessages, 0);
});

test('a throwing transform propagates to the publish caller with zero side effects', () => {
  const bus = new EventBus();
  const boom = new Error('transform boom');
  bus.setTopicTransforms('t', [
    (p) => ({ ...(p as Record<string, unknown>), touched: true }),
    () => {
      throw boom;
    },
  ]);
  assert.throws(() => bus.publish('t', { a: 1 }), (err) => err === boom);
  // Zero side effects: no sequence number consumed (no gap for the next
  // publish), nothing counted as published, nothing rejected.
  const stats = bus.getStats();
  assert.equal(stats.topics[0].transformCalls, 2);
  assert.equal(stats.topics[0].transformErrors, 1);
  assert.equal(stats.transformCalls, 2);
  assert.equal(stats.transformErrors, 1);
  assert.equal(stats.topics[0].lastSeq, 0);
  assert.equal(stats.topics[0].publishedMessages, 0);
  assert.equal(stats.topics[0].rejectedMessages, 0);
  // The next publish draws seq 1 — the failed one left no gap.
  bus.clearTopicTransforms('t');
  const received2: BusMessage[] = [];
  bus.subscribe('t', (m) => received2.push(m));
  assert.equal(bus.publish('t', { a: 2 }), 1);
  assert.equal(bus.getStats().topics[0].lastSeq, 1);
  assert.equal(bus.getStats().topics[0].sequenceGaps, 0);
});

test('transform rule matching: exact wins over pattern, earliest pattern wins', async () => {
  const bus = new EventBus();
  bus.setTopicTransforms('a.**', [(p) => ({ ...(p as any), via: 'pattern-a' })]);
  bus.setTopicTransforms('a.b', [(p) => ({ ...(p as any), via: 'exact' })]);
  // Two distinct patterns both matching 'b.c.d': the earliest-registered
  // one wins.
  bus.setTopicTransforms('b.**', [(p) => ({ ...(p as any), via: 'pattern-b1' })]);
  bus.setTopicTransforms('b.c.**', [(p) => ({ ...(p as any), via: 'pattern-b2' })]);
  // Re-setting the same pattern replaces its chain (the setTopicSchema
  // convention).
  bus.setTopicTransforms('c.**', [(p) => ({ ...(p as any), via: 'old' })]);
  bus.setTopicTransforms('c.**', [(p) => ({ ...(p as any), via: 'new' })]);
  const received: BusMessage[] = [];
  bus.subscribe('**', (m) => received.push(m));
  bus.publish('a.b', {});
  bus.publish('b.c.d', {});
  bus.publish('c.e', {});
  await flush();
  const byTopic = new Map(received.map((m) => [m.topic, m.payload]));
  assert.deepEqual((byTopic.get('a.b') as any).via, 'exact');
  assert.deepEqual((byTopic.get('b.c.d') as any).via, 'pattern-b1');
  assert.deepEqual((byTopic.get('c.e') as any).via, 'new');
});

test('setTopicTransforms validates its arguments fail-fast', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicTransforms('', [(p) => p]), RangeError);
  assert.throws(() => bus.setTopicTransforms('t', []), RangeError);
  assert.throws(() => bus.setTopicTransforms('t', 'nope' as any), RangeError);
  assert.throws(() => bus.setTopicTransforms('t', [(p) => p, 42 as any]), RangeError);
  // A failed registration leaves no rule behind.
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', { x: 1 }), 1);
  assert.equal(bus.getStats().topics[0].transformCalls, 0);
});

test('clearTopicTransforms removes the chain; unknown pattern returns false', async () => {
  const bus = new EventBus();
  bus.setTopicTransforms('t', [(p) => ({ ...(p as any), marked: true })]);
  assert.equal(bus.clearTopicTransforms('t'), true);
  assert.equal(bus.clearTopicTransforms('t'), false);
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  bus.publish('t', {});
  await flush();
  assert.deepEqual(received[0].payload, {});
  assert.equal(bus.getStats().topics[0].transformCalls, 0);
});

test('publishAtomic runs the transform chain in shadow admission, once per entry', async () => {
  const bus = new EventBus();
  let calls = 0;
  bus.setTopicTransforms('t', [
    (p) => {
      calls += 1;
      return { ...(p as Record<string, unknown>), n: calls };
    },
  ]);
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  const result = bus.publishAtomic([
    { topic: 't', payload: { a: 1 } },
    { topic: 't', payload: { a: 2 } },
  ]);
  assert.deepEqual(result, { published: 2 });
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    [
      { a: 1, n: 1 },
      { a: 2, n: 2 },
    ],
  );
  // Exactly once per entry — the pre-admitted commit phase does not
  // re-run the chain.
  assert.equal(calls, 2);
  assert.equal(bus.getStats().topics[0].transformCalls, 2);
});

test('publishAtomic aborts on a throwing transform with zero side effects', () => {
  const bus = new EventBus();
  bus.setTopicTransforms('t', [() => { throw new Error('atomic transform boom'); }]);
  assert.throws(() => bus.publishAtomic([{ topic: 't', payload: {} }]), /atomic transform boom/);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].transformErrors, 1);
  assert.equal(stats.topics[0].publishedMessages, 0);
  assert.equal(stats.topics[0].lastSeq, 0);
});

test('publishDelayed transforms at schedule time; the due-time fan-out does not re-run', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  let calls = 0;
  bus.setTopicTransforms('t', [
    (p) => {
      calls += 1;
      return { ...(p as Record<string, unknown>), delayed: true };
    },
  ]);
  // The schedule-time schema gate sees the transformed payload.
  bus.setTopicSchema('t', (p) => (p as any).delayed === true);
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  const id = bus.publishDelayed('t', {}, { delayMs: 1000 });
  assert.match(id as string, /^delayed-\d+$/);
  assert.equal(calls, 1);
  clock.nowMs = 2_000;
  bus.publish('t', { poke: true }); // triggers the flush whose sweep fans out the due entry
  await flush();
  assert.equal(calls, 2); // the poke publish ran the chain; the due entry did not re-run it
  const payloads = received.map((m) => m.payload);
  assert.deepEqual(payloads, [{ poke: true, delayed: true }, { delayed: true }]);
});

test('publishDelayed fails fast when the transform throws: nothing is scheduled', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTransforms('t', [() => { throw new Error('delayed transform boom'); }]);
  assert.throws(() => bus.publishDelayed('t', {}, { delayMs: 1000 }), /delayed transform boom/);
  assert.equal(bus.getStats().pendingDelayed, 0);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].transformErrors, 1);
  assert.equal(stats.topics[0].transformCalls, 1);
});

test('transform does not run for topics without a matching chain', () => {
  const bus = new EventBus();
  bus.setTopicTransforms('other.**', [(p) => ({ ...(p as any), x: 1 })]);
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', { y: 2 }), 1);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].transformCalls, 0);
  assert.equal(stats.transformCalls, 0);
});
