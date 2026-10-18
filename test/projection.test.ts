import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-projection-'));

const collect = (
  bus: EventBus,
  pattern: string,
  opts?: Parameters<EventBus['subscribe']>[2],
) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

test('projection: handler receives the transformed payload', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', {
    project: (payload) => ({ ...(payload as object), masked: true }),
  });
  bus.publish('t', { user: 'alice', ssn: '123' });
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { user: 'alice', ssn: '123', masked: true });
  // Envelope identity is untouched: same topic and seq.
  assert.equal(received[0].topic, 't');
  assert.equal(received[0].seq, 1);
});

test('projection: evaluated after the filter, only for accepted messages', async () => {
  const bus = new EventBus();
  let projections = 0;
  const { received } = collect(bus, 't', {
    filter: (payload) => (payload as { keep: boolean }).keep,
    project: (payload) => {
      projections += 1;
      return (payload as { v: number }).v * 10;
    },
  });
  bus.publish('t', { keep: false, v: 1 });
  bus.publish('t', { keep: true, v: 2 });
  bus.publish('t', { keep: false, v: 3 });
  bus.publish('t', { keep: true, v: 4 });
  await flush();
  // Filter ran first: only the two accepted messages were projected.
  assert.equal(projections, 2);
  assert.deepEqual(
    received.map((m) => m.payload),
    [20, 40],
  );
  const stats = bus.getStats();
  assert.equal(stats.projectedMessages, 2);
  assert.equal(stats.projectErrors, 0);
});

test('projection: a subscriber without project on the same topic is unaffected', async () => {
  const bus = new EventBus();
  const projected = collect(bus, 't', { project: () => 'projected' });
  const plain = collect(bus, 't');
  bus.publish('t', 'original');
  await flush();
  assert.deepEqual(
    projected.received.map((m) => m.payload),
    ['projected'],
  );
  assert.deepEqual(
    plain.received.map((m) => m.payload),
    ['original'],
  );
});

test('projection: throwing projection trips the health probe without interrupting the drain', async () => {
  const bus = new EventBus();
  let calls = 0;
  const { received, sub } = collect(bus, 't', {
    healthProbe: { maxConsecutiveFailures: 1 },
    project: (payload) => {
      calls += 1;
      if ((payload as { boom: boolean }).boom) throw new Error('projection blew up');
      return payload;
    },
  });
  bus.publish('t', { boom: false, n: 1 });
  bus.publish('t', { boom: true, n: 2 });
  bus.publish('t', { boom: false, n: 3 });
  await flush();
  // First message delivered fine; the second message's projection threw
  // and degraded the subscriber — the drain did not wedge, it requeued
  // the undelivered tail.
  assert.equal(calls, 2);
  assert.deepEqual(
    received.map((m) => (m.payload as { n: number }).n),
    [1],
  );
  const stats = bus.getStats();
  assert.equal(stats.projectedMessages, 2);
  assert.equal(stats.projectErrors, 1);
  assert.equal(stats.degradedSubscribers, 1);
  const row = stats.projections.find((p) => p.subscriberId === sub.id);
  assert.ok(row !== undefined);
  assert.equal(row.projectedMessages, 2);
  assert.equal(row.projectErrors, 1);
  // Degraded means paused: nothing more is delivered while degraded.
  bus.publish('t', { boom: false, n: 4 });
  await flush();
  assert.equal(received.length, 1);
});

test('projection: non-function project throws TypeError at subscribe time', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { project: 'nope' as unknown as () => unknown }),
    TypeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { project: 42 as unknown as () => unknown }),
    TypeError,
  );
  // A rejected subscribe registers nothing.
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('projection: durable log stores the original payload', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const { received } = collect(bus, 't', {
    project: (payload) => ({ ...(payload as object), projected: true }),
  });
  bus.publish('t', { v: 1 });
  await flush();
  assert.deepEqual(received[0].payload, { v: 1, projected: true });
  // The log holds the pre-projection bytes: pull reads the original.
  const pulled = await bus.pull('t', { maxMessages: 10 });
  assert.equal(pulled.length, 1);
  assert.deepEqual(pulled[0].payload, { v: 1 });
});

test('projection: batched subscribers project each message independently', async () => {
  const bus = new EventBus();
  const received: BusMessage[][] = [];
  bus.subscribe(
    't',
    ((msgs: BusMessage[]) => received.push(msgs)) as unknown as (msg: BusMessage) => void,
    {
      batch: { maxSize: 3 },
      project: (payload) => (payload as number) * 2,
    },
  );
  bus.publish('t', 1);
  bus.publish('t', 2);
  bus.publish('t', 3);
  await flush();
  // maxSize reached: the batch delivered immediately.
  assert.equal(received.length, 1);
  assert.deepEqual(
    received[0].map((m) => m.payload),
    [2, 4, 6],
  );
  // Per-message seqs survive the projection.
  assert.deepEqual(
    received[0].map((m) => m.seq),
    [1, 2, 3],
  );
  assert.equal(bus.getStats().projectedMessages, 3);
});

test('projection: reliable subscriber sees the projected view, tracking stays original', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  const sub = bus.subscribeReliable(
    't',
    (delivery) => {
      seen.push(delivery.msg.payload);
      delivery.ack();
    },
    { project: (payload) => ({ ...(payload as object), projected: true }) },
  );
  bus.publish('t', { v: 1 });
  await flush();
  assert.deepEqual(seen, [[{ v: 1, projected: true }][0]]);
  // ack() resolved against the original envelope: nothing unacked.
  assert.equal(bus.getStats().unackedDeliveries, 0);
  assert.equal(bus.getStats().projectedMessages, 1);
  sub.unsubscribe();
});

test('projection: reliable redelivery re-projects; poison lands in the DLQ as the original', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable(
    't',
    (delivery) => {
      attempts += 1;
      // The handler always sees the projected view, on every redelivery.
      assert.deepEqual(delivery.msg.payload, { v: 1, projected: true });
      delivery.nack();
    },
    {
      project: (payload) => ({ ...(payload as object), projected: true }),
      deadLetter: { maxRedeliveries: 1 },
    },
  );
  bus.publish('t', { v: 1 });
  await flush();
  // First delivery + one redelivery, then the poison budget is exhausted.
  await flush();
  assert.equal(attempts, 2);
  const dlq = bus.getDeadLetterMessages(sub.id);
  assert.equal(dlq.length, 1);
  // The DLQ entry holds the original payload, not the projected view.
  assert.deepEqual(dlq[0].payload, { v: 1 });
  assert.equal(bus.getStats().projectedMessages, 2);
  sub.unsubscribe();
});

test('projection: throwing projection on a reliable subscriber with DLQ nacks like a handler throw', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable(
    't',
    (delivery) => {
      attempts += 1;
      delivery.ack();
    },
    {
      project: () => {
        throw new Error('cannot project');
      },
      deadLetter: { maxRedeliveries: 0 },
    },
  );
  bus.publish('t', { v: 1 });
  await flush();
  await flush();
  // Projection threw on the first delivery attempt; with maxRedeliveries
  // 0 the message went straight to the DLQ — the handler never ran.
  assert.equal(attempts, 0);
  const dlq = bus.getDeadLetterMessages(sub.id);
  assert.equal(dlq.length, 1);
  assert.deepEqual(dlq[0].payload, { v: 1 });
  const stats = bus.getStats();
  assert.equal(stats.projectedMessages, 1);
  assert.equal(stats.projectErrors, 1);
  sub.unsubscribe();
});

test('projection: stats rows only list projecting subscribers', async () => {
  const bus = new EventBus();
  const a = collect(bus, 't', { project: (p) => p });
  collect(bus, 't');
  bus.publish('t', 1);
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.projectedMessages, 1);
  assert.equal(stats.projectErrors, 0);
  assert.equal(stats.projections.length, 1);
  assert.equal(stats.projections[0].subscriberId, a.sub.id);
  assert.equal(stats.projections[0].pattern, 't');
  assert.equal(stats.projections[0].projectedMessages, 1);
  assert.equal(stats.projections[0].projectErrors, 0);
});
