import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const nOf = (payload: unknown): number => (payload as { n: number }).n;

test('drain delivers the queued backlog, then removes the subscription', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribe('drain.basic', (msg) => seen.push(nOf(msg.payload)));
  bus.publish('drain.basic', { n: 1 });
  bus.publish('drain.basic', { n: 2 });
  bus.publish('drain.basic', { n: 3 });
  sub.unsubscribe({ drain: true });
  assert.equal(bus.getStats().drainingSubscribers, 1);
  // New publishes during the drain are skipped — never queued, never delivered.
  bus.publish('drain.basic', { n: 4 });
  bus.publish('drain.basic', { n: 5 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  assert.equal(bus.getStats().drainingSubscribers, 0);
  // The subscription is gone: later publishes reach nobody.
  bus.publish('drain.basic', { n: 6 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  // A second unsubscribe after teardown is a no-op.
  sub.unsubscribe();
});

test('drain skips do not count as drops and do not pollute stats', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribe('drain.stats', (msg) => seen.push(nOf(msg.payload)));
  bus.publish('drain.stats', { n: 1 });
  sub.unsubscribe({ drain: true });
  const before = bus.getStats();
  bus.publish('drain.stats', { n: 2 });
  bus.publish('drain.stats', { n: 3 });
  await flush();
  const after = bus.getStats();
  assert.deepEqual(seen, [1]);
  assert.equal(after.droppedMessages, before.droppedMessages);
  assert.equal(after.filteredMessages, before.filteredMessages);
  assert.equal(after.deliveredMessages, before.deliveredMessages + 1);
  assert.equal(after.drainingSubscribers, 0);
});

test('drain timeout forces teardown when the backlog never settles', async () => {
  const bus = new EventBus();
  let deliveries = 0;
  const sub = bus.subscribeReliable(
    'drain.timeout',
    () => {
      deliveries += 1;
      // Never ack: the drain can only end via the timeout.
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('drain.timeout', { n: 1 });
  await flush();
  assert.equal(deliveries, 1);
  assert.equal(bus.unackedCount(sub.id), 1);
  sub.unsubscribe({ drain: true, drainTimeoutMs: 50 });
  assert.equal(bus.getStats().drainingSubscribers, 1);
  await sleep(150);
  await flush();
  assert.equal(bus.getStats().drainingSubscribers, 0);
  // The teardown cancelled the ack timer: no redelivery ever arrives.
  assert.equal(deliveries, 1);
  sub.unsubscribe();
});

test('reliable drain: a nacked message redelivers, then the drain completes on ack', async () => {
  const bus = new EventBus();
  const seen: string[] = [];
  const sub = bus.subscribeReliable(
    'drain.redeliver',
    (d) => {
      seen.push(`${nOf(d.msg.payload)}:${d.redeliveries}`);
      if (d.redeliveries === 0) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.publish('drain.redeliver', { n: 1 });
  sub.unsubscribe({ drain: true });
  assert.equal(bus.getStats().drainingSubscribers, 1);
  await flush();
  // Nack → normal redelivery (requeued through the drain gate) → ack →
  // the drain completes only after the redelivery settles.
  assert.deepEqual(seen, ['1:0', '1:1']);
  assert.equal(bus.getStats().drainingSubscribers, 0);
});

test('reliable drain: an exhausted redelivery budget dead-letters, then the drain completes', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  let deadLettered = 0;
  const sub = bus.subscribeReliable(
    'drain.dlq',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    {
      ackTimeoutMs: 60_000,
      deadLetter: { maxRedeliveries: 1, onDeadLetter: () => { deadLettered += 1; } },
    },
  );
  bus.publish('drain.dlq', { n: 1 });
  sub.unsubscribe({ drain: true });
  await flush();
  assert.deepEqual(seen, [0, 1]);
  assert.equal(deadLettered, 1);
  assert.equal(bus.getStats().drainingSubscribers, 0);
});

test('drainingSubscribers counts live drains; an empty backlog drains immediately', async () => {
  const bus = new EventBus();
  const a = bus.subscribe('drain.count.a', () => {});
  const b = bus.subscribe('drain.count.b', () => {});
  assert.equal(bus.getStats().drainingSubscribers, 0);
  bus.publish('drain.count.a', { n: 1 });
  a.unsubscribe({ drain: true });
  assert.equal(bus.getStats().drainingSubscribers, 1);
  // Nothing outstanding: the drain completes synchronously inside the call.
  b.unsubscribe({ drain: true });
  assert.equal(bus.getStats().drainingSubscribers, 1);
  await flush();
  assert.equal(bus.getStats().drainingSubscribers, 0);
});

test('unsubscribe validates drain options before changing any state', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribe('drain.validate', (msg) => seen.push(nOf(msg.payload)));
  assert.throws(() => sub.unsubscribe({ drain: 'yes' as unknown as boolean }), RangeError);
  assert.throws(() => sub.unsubscribe({ drain: 1 as unknown as boolean }), RangeError);
  for (const bad of [0, -1, NaN, Infinity, -Infinity, '100' as unknown as number]) {
    assert.throws(() => sub.unsubscribe({ drainTimeoutMs: bad }), RangeError);
  }
  // The failed calls changed nothing: the subscription still works and is
  // not draining.
  bus.publish('drain.validate', { n: 1 });
  await flush();
  assert.deepEqual(seen, [1]);
  assert.equal(bus.getStats().drainingSubscribers, 0);
  sub.unsubscribe();
});

test('immediate unsubscribe is unchanged (regression)', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribe('drain.immediate', (msg) => seen.push(nOf(msg.payload)));
  bus.publish('drain.immediate', { n: 1 });
  sub.unsubscribe();
  assert.equal(bus.getStats().drainingSubscribers, 0);
  await flush();
  assert.deepEqual(seen, []);
  // Second call is a no-op.
  sub.unsubscribe();
  // drain: false is immediate too.
  const sub2 = bus.subscribe('drain.immediate2', (msg) => seen.push(nOf(msg.payload)));
  bus.publish('drain.immediate2', { n: 2 });
  sub2.unsubscribe({ drain: false });
  await flush();
  assert.deepEqual(seen, []);
});

test('a second unsubscribe during the drain forces immediate teardown', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribe('drain.force', (msg) => seen.push(nOf(msg.payload)));
  bus.publish('drain.force', { n: 1 });
  bus.publish('drain.force', { n: 2 });
  sub.unsubscribe({ drain: true });
  assert.equal(bus.getStats().drainingSubscribers, 1);
  sub.unsubscribe();
  assert.equal(bus.getStats().drainingSubscribers, 0);
  await flush();
  // The backlog is discarded with the queue — not delivered.
  assert.deepEqual(seen, []);
});

test('drain passes through the reliable and consumer-group wrappers', async () => {
  {
    const bus = new EventBus();
    const seen: number[] = [];
    const sub = bus.subscribeReliable('drain.wrap.rel', (d) => {
      seen.push(nOf(d.msg.payload));
      d.ack();
    });
    bus.publish('drain.wrap.rel', { n: 1 });
    sub.unsubscribe({ drain: true });
    await flush();
    assert.deepEqual(seen, [1]);
    assert.equal(bus.getStats().drainingSubscribers, 0);
  }
  {
    const bus = new EventBus();
    const seen: number[] = [];
    const sub = bus.subscribeToGroup('g1', 'drain.wrap.grp', (msg) => seen.push(nOf(msg.payload)));
    bus.publish('drain.wrap.grp', { n: 1 });
    bus.publish('drain.wrap.grp', { n: 2 });
    sub.unsubscribe({ drain: true });
    await flush();
    assert.deepEqual(seen, [1, 2]);
    assert.equal(bus.getStats().drainingSubscribers, 0);
  }
});
