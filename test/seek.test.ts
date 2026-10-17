import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import type { Delivery } from '../src/ack.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Deterministic bus clock: the log's `at` stamps ride on it. */
function makeClock(start = 1_000_000) {
  let now = start;
  return {
    now: () => now,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

function makeBus(clock: { now: () => number }, withLog: boolean) {
  const dir = withLog ? mkdtempSync(join(tmpdir(), 'seek-test-')) : undefined;
  return new EventBus({ now: clock.now, ...(dir === undefined ? {} : { durableLogDir: dir }) });
}

test('backward seek replays from the durable log and audits the seek', async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const seen: number[] = [];
  const sub = bus.subscribe('orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  for (let i = 0; i < 5; i++) {
    clock.advance(10);
    bus.publish('orders', { i });
  }
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);

  const result = bus.seek(sub.id, { seq: 2 }, { actor: 'ops' });
  assert.equal(result.subscriberId, sub.id);
  assert.equal(result.invalidatedInflight, 0);
  assert.equal(result.topics.length, 1);
  const t = result.topics[0];
  assert.equal(t.topic, 'orders');
  assert.equal(t.from, 5);
  assert.equal(t.requested, 2);
  assert.equal(t.position, 2);
  assert.equal(t.replayed, 3);
  assert.equal(t.droppedQueued, 0);
  assert.ok(t.clampedByCommitted === undefined);
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 3, 4, 5]);
  assert.equal(bus.getStats().sequenceGaps, 0);

  const audit = bus.getSubscriptionAudit({ action: 'subscriber_seek' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].actor, 'ops');
  assert.equal(audit[0].detail?.mode, 'seq');
  assert.deepEqual(audit[0].detail?.invalidatedInflight, 0);
});

test('forward seek drops the stale backlog and the new position holds', async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const seen: number[] = [];
  const sub = bus.subscribe('orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  for (let i = 0; i < 5; i++) bus.publish('orders', { i });
  // No flush: 1..5 sit in the queue, undelivered.
  const result = bus.seek(sub.id, { seq: 3 });
  await flush();
  assert.deepEqual(seen, [4, 5]);
  assert.equal(result.topics[0].droppedQueued, 5);
  assert.equal(result.topics[0].replayed, 2);
  bus.publish('orders', { i: 5 });
  await flush();
  assert.deepEqual(seen, [4, 5, 6]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test("'latest' skips to the head; only newer messages arrive", async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const seen: number[] = [];
  const sub = bus.subscribe('orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  for (let i = 0; i < 5; i++) bus.publish('orders', { i });
  await flush();
  for (let i = 5; i < 8; i++) bus.publish('orders', { i });
  const result = bus.seek(sub.id, 'latest');
  assert.equal(result.topics[0].position, 8);
  assert.equal(result.topics[0].droppedQueued, 3);
  bus.publish('orders', { i: 8 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 9]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test("'earliest' replays the whole retained history", async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const seen: number[] = [];
  const sub = bus.subscribe('orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  for (let i = 0; i < 3; i++) bus.publish('orders', { i });
  await flush();
  const result = bus.seek(sub.id, 'earliest');
  assert.equal(result.topics[0].requested, 0);
  assert.equal(result.topics[0].position, 0);
  assert.equal(result.topics[0].replayed, 3);
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 1, 2, 3]);
});

test('timestamp seek replays messages published after the timestamp', async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const seen: number[] = [];
  const sub = bus.subscribe('orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  const t0 = clock.now();
  bus.publish('orders', { i: 0 }); // at t0
  clock.advance(100);
  bus.publish('orders', { i: 1 }); // at t0+100
  clock.advance(100);
  bus.publish('orders', { i: 2 }); // at t0+200
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);

  const result = bus.seek(sub.id, { timestamp: t0 + 150 });
  assert.equal(result.topics[0].requested, 2);
  assert.equal(result.topics[0].position, 2);
  assert.equal(result.topics[0].replayed, 1);
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 3]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('reliable in-flight deliveries are invalidated by the new position', async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      // Never ack: the deliveries stay in-flight until the seek.
    },
    { ackTimeoutMs: 60_000 },
  );
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await flush();
  assert.equal(deliveries.length, 3);
  assert.equal(bus.unackedCount(sub.id), 3);

  const result = bus.seek(sub.id, { seq: 1 });
  assert.equal(result.invalidatedInflight, 3);
  assert.equal(bus.unackedCount(sub.id), 0);

  // Stale handles settle to silent no-ops: no throw, no redelivery, no gap.
  for (const d of deliveries) d.ack();
  await flush();
  // The replay re-tracks messages 2 and 3 as fresh deliveries.
  assert.equal(deliveries.length, 5);
  assert.deepEqual(
    deliveries.map((d) => d.msg.seq),
    [1, 2, 3, 2, 3],
  );
  assert.deepEqual(
    deliveries.slice(3).map((d) => d.redeliveries),
    [0, 0],
  );
  assert.equal(bus.unackedCount(sub.id), 2);
  assert.equal(bus.getStats().sequenceGaps, 0);
  for (const d of deliveries) d.ack();
  assert.equal(bus.unackedCount(sub.id), 0);
});

test('seek never regresses a committed offset (EB-127 highest-seq-wins)', async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const seen: number[] = [];
  const member = bus.subscribeToGroup('g', 't', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  for (let i = 0; i < 5; i++) bus.publish('t', { i });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  bus.commitOffset('g', 't', 4);
  assert.equal(bus.getCommittedOffsets('g')['t'], 4);

  const result = bus.seek(member.id, { seq: 1 });
  const t = result.topics[0];
  assert.equal(t.requested, 1);
  assert.equal(t.position, 4);
  assert.equal(t.clampedByCommitted, 4);
  // Only message 5 sits past the clamped position.
  assert.equal(t.replayed, 1);
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 5]);
  // The committed checkpoint itself never moves on a seek.
  assert.equal(bus.getCommittedOffsets('g')['t'], 4);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('without a durable log the seek is forward-only and takes effect live', async () => {
  const clock = makeClock();
  const bus = makeBus(clock, false);
  const seen: number[] = [];
  const sub = bus.subscribe('orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  for (let i = 0; i < 3; i++) bus.publish('orders', { i });
  await flush();

  const result = bus.seek(sub.id, 'latest');
  assert.equal(result.topics[0].position, 3);
  assert.equal(result.topics[0].replayed, 0);
  bus.publish('orders', { i: 3 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4]);
  assert.equal(bus.getStats().sequenceGaps, 0);

  // Backward without history to replay from is illegal.
  assert.throws(() => bus.seek(sub.id, { seq: 1 }), RangeError);
  // Wall-clock positioning without history is unresolvable.
  assert.throws(() => bus.seek(sub.id, { timestamp: clock.now() }), RangeError);
});

test('seek target validation fails fast with RangeError', () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const sub = bus.subscribe('orders', () => {});
  assert.throws(() => bus.seek('nope', { seq: 1 }), Error);
  assert.throws(() => bus.seek(sub.id, { seq: -1 }), RangeError);
  assert.throws(() => bus.seek(sub.id, { seq: 1.5 }), RangeError);
  assert.throws(() => bus.seek(sub.id, { seq: Number.NaN }), RangeError);
  assert.throws(() => bus.seek(sub.id, { timestamp: clock.now() + 1 }), RangeError);
  assert.throws(() => bus.seek(sub.id, { timestamp: -5 }), RangeError);
  assert.throws(() => bus.seek(sub.id, { seq: 1, timestamp: 2 } as never), RangeError);
  assert.throws(() => bus.seek(sub.id, {} as never), RangeError);
  assert.throws(() => bus.seek(sub.id, 'middle' as never), RangeError);
  assert.throws(() => bus.seek(sub.id, { seq: 1, bogus: true } as never), RangeError);
  assert.throws(() => bus.seek(sub.id, { seq: 1, topic: 'other' }), RangeError);
  assert.throws(() => bus.seek(sub.id, { seq: 1 }, { actor: '' }), RangeError);
  // A rejected seek mutates nothing: no audit entry, position untouched.
  assert.equal(bus.getSubscriptionAudit({ action: 'subscriber_seek' }).length, 0);
});

test("wildcard 'latest' fans out to every topic feeding the subscription", async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const seen: Array<[string, number]> = [];
  const sub = bus.subscribe('orders.*', (msg: BusMessage) => {
    seen.push([msg.topic, msg.seq]);
  });
  bus.publish('orders.a', { i: 0 });
  bus.publish('orders.a', { i: 1 });
  bus.publish('orders.b', { i: 0 });
  bus.publish('orders.b', { i: 1 });
  await flush();
  assert.equal(seen.length, 4);

  const result = bus.seek(sub.id, 'latest');
  assert.equal(result.topics.length, 2);
  for (const t of result.topics) assert.equal(t.position, 2);
  // { seq } without a topic is ambiguous on a multi-topic pattern.
  assert.throws(() => bus.seek(sub.id, { seq: 1 }), RangeError);

  bus.publish('orders.a', { i: 2 });
  await flush();
  assert.deepEqual(seen[seen.length - 1], ['orders.a', 3]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('explicit topic seeks one topic of a wildcard subscription', async () => {
  const clock = makeClock();
  const bus = makeBus(clock, true);
  const seen: Array<[string, number]> = [];
  const sub = bus.subscribe('orders.*', (msg: BusMessage) => {
    seen.push([msg.topic, msg.seq]);
  });
  bus.publish('orders.a', { i: 0 });
  bus.publish('orders.a', { i: 1 });
  bus.publish('orders.b', { i: 0 });
  await flush();

  const result = bus.seek(sub.id, { seq: 0, topic: 'orders.a' });
  assert.equal(result.topics.length, 1);
  assert.equal(result.topics[0].topic, 'orders.a');
  assert.equal(result.topics[0].replayed, 2);
  await flush();
  assert.deepEqual(
    seen.filter(([t]) => t === 'orders.a').map(([, s]) => s),
    [1, 2, 1, 2],
  );
  // orders.b untouched.
  assert.deepEqual(
    seen.filter(([t]) => t === 'orders.b').map(([, s]) => s),
    [1],
  );
});
