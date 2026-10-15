import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, AclDeniedError } from '../src/bus.ts';

/**
 * Pull-mode consumption (EB-111): `bus.pull(topic, {maxMessages, waitMs?,
 * fromOffset?})` — opt-in long-polling pull, the complement to push
 * `subscribe`. Caller-owned cursor (at-least-once), exact topics only,
 * orthogonal to reliable delivery / ACK.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-pull-'));

test('pull: basic fetch returns messages in ascending seq order', async () => {
  const bus = new EventBus();
  bus.publish('orders', { id: 1 });
  bus.publish('orders', { id: 2 });
  bus.publish('orders', { id: 3 });
  const msgs = await bus.pull('orders', { maxMessages: 10 });
  assert.deepEqual(
    msgs.map((m) => m.seq),
    [1, 2, 3],
  );
  assert.deepEqual(
    msgs.map((m) => m.payload),
    [{ id: 1 }, { id: 2 }, { id: 3 }],
  );
  assert.ok(msgs.every((m) => m.topic === 'orders'));
  assert.ok(msgs.every((m) => typeof m.at === 'number'));
});

test('pull: maxMessages caps the batch', async () => {
  const bus = new EventBus();
  for (let i = 1; i <= 5; i++) bus.publish('t', i);
  const msgs = await bus.pull('t', { maxMessages: 2 });
  assert.deepEqual(
    msgs.map((m) => m.seq),
    [1, 2],
  );
});

test('pull: fromOffset pages forward through the stream', async () => {
  const bus = new EventBus();
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  const page1 = await bus.pull('t', { maxMessages: 2, fromOffset: 0 });
  assert.deepEqual(
    page1.map((m) => m.seq),
    [1, 2],
  );
  const page2 = await bus.pull('t', { maxMessages: 2, fromOffset: 2 });
  assert.deepEqual(
    page2.map((m) => m.seq),
    [3],
  );
  const page3 = await bus.pull('t', { maxMessages: 2, fromOffset: 3 });
  assert.deepEqual(page3, []);
});

test('pull: not advancing the offset re-delivers (at-least-once)', async () => {
  const bus = new EventBus();
  bus.publish('t', 'x');
  bus.publish('t', 'y');
  const first = await bus.pull('t', { maxMessages: 10, fromOffset: 0 });
  const second = await bus.pull('t', { maxMessages: 10, fromOffset: 0 });
  assert.deepEqual(
    second.map((m) => m.seq),
    first.map((m) => m.seq),
  );
  assert.deepEqual(
    second.map((m) => m.payload),
    ['x', 'y'],
  );
});

test('pull: unknown topic resolves [] without throwing', async () => {
  const bus = new EventBus();
  bus.publish('elsewhere', 1);
  assert.deepEqual(await bus.pull('never.published', { maxMessages: 5 }), []);
});

test('pull: pattern topics are rejected with RangeError', async () => {
  const bus = new EventBus();
  await assert.rejects(bus.pull('orders.*', { maxMessages: 5 }), RangeError);
  await assert.rejects(bus.pull('**', { maxMessages: 5 }), RangeError);
});

test('pull: option validation throws RangeError', async () => {
  const bus = new EventBus();
  await assert.rejects(bus.pull('t', { maxMessages: 0 }), RangeError);
  await assert.rejects(bus.pull('t', { maxMessages: 1.5 }), RangeError);
  await assert.rejects(bus.pull('t', { maxMessages: 5, waitMs: -1 }), RangeError);
  await assert.rejects(
    bus.pull('t', { maxMessages: 5, waitMs: Number.NaN }),
    RangeError,
  );
  await assert.rejects(bus.pull('t', { maxMessages: 5, fromOffset: -1 }), RangeError);
  await assert.rejects(bus.pull('t', { maxMessages: 5, fromOffset: 1.5 }), RangeError);
  await assert.rejects(bus.pull('', { maxMessages: 5 }), RangeError);
});

test('pull: waitMs expiry resolves [] and counts a long-poll timeout', async () => {
  const bus = new EventBus();
  const started = Date.now();
  const msgs = await bus.pull('quiet', { maxMessages: 5, waitMs: 100 });
  const elapsed = Date.now() - started;
  assert.deepEqual(msgs, []);
  assert.ok(elapsed >= 50, `long poll returned too early after ${elapsed}ms`);
  assert.equal(bus.getStats().pullStats.longPollTimeouts, 1);
});

test('pull: a publish during the wait wakes the long poll', async () => {
  const bus = new EventBus();
  const pending = bus.pull('t', { maxMessages: 5, waitMs: 5000 });
  setTimeout(() => bus.publish('t', 'late-arrival'), 20);
  const msgs = await pending;
  assert.equal(msgs.length, 1);
  assert.equal(msgs[0].payload, 'late-arrival');
  assert.equal(msgs[0].seq, 1);
  // The waiter resolved on wake, not on timeout.
  assert.equal(bus.getStats().pullStats.longPollTimeouts, 0);
});

test('pull: concurrent pulls on one topic keep independent offsets', async () => {
  const bus = new EventBus();
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  const [fromZero, fromTwo] = await Promise.all([
    bus.pull('t', { maxMessages: 10, fromOffset: 0 }),
    bus.pull('t', { maxMessages: 10, fromOffset: 2 }),
  ]);
  assert.deepEqual(
    fromZero.map((m) => m.seq),
    [1, 2, 3],
  );
  assert.deepEqual(
    fromTwo.map((m) => m.seq),
    [3],
  );
});

test('pull: in-memory ring is bounded by pullBufferSize', async () => {
  const bus = new EventBus({ pullBufferSize: 2 });
  for (let i = 1; i <= 5; i++) bus.publish('t', i);
  const msgs = await bus.pull('t', { maxMessages: 10, fromOffset: 0 });
  // Only the two most recent admitted messages survive the ring.
  assert.deepEqual(
    msgs.map((m) => m.seq),
    [4, 5],
  );
});

test('pull: messages are orthogonal to ACK — no unacked growth, no tracker entry', async () => {
  const bus = new EventBus();
  const deliveries: unknown[] = [];
  bus.subscribeReliable('t', (delivery) => {
    deliveries.push(delivery);
  });
  bus.publish('t', 'one');
  await flush();
  assert.equal(deliveries.length, 1);
  assert.equal(bus.getStats().unackedDeliveries, 1);
  const pulled = await bus.pull('t', { maxMessages: 5 });
  assert.equal(pulled.length, 1);
  assert.equal(pulled[0].payload, 'one');
  // Pull created no delivery, entered no ACK tracker, started no timeout.
  assert.equal(bus.getStats().unackedDeliveries, 1);
  await flush();
  assert.equal(bus.getStats().unackedDeliveries, 1);
});

test('pull: durable log survives a bus restart at the same offset', async () => {
  const dir = freshDir();
  const bus1 = new EventBus({ durableLogDir: dir });
  bus1.publish('orders', 'a');
  bus1.publish('orders', 'b');
  bus1.publish('orders', 'c');
  // A fresh bus over the same directory keeps pulling from the log.
  const bus2 = new EventBus({ durableLogDir: dir });
  const replayed = await bus2.pull('orders', { maxMessages: 10, fromOffset: 0 });
  assert.deepEqual(
    replayed.map((m) => m.seq),
    [1, 2, 3],
  );
  assert.deepEqual(
    replayed.map((m) => m.payload),
    ['a', 'b', 'c'],
  );
  assert.deepEqual(await bus2.pull('orders', { maxMessages: 10, fromOffset: 3 }), []);
  // The restarted bus continues the sequence; pull keeps advancing.
  bus2.publish('orders', 'd');
  const continued = await bus2.pull('orders', { maxMessages: 10, fromOffset: 3 });
  assert.deepEqual(
    continued.map((m) => m.seq),
    [4],
  );
  assert.equal(continued[0].payload, 'd');
});

test('pull: durable log reads inflate compressed payloads', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('t', { thresholdBytes: 1 });
  const payload = { big: 'x'.repeat(200) };
  bus.publish('t', payload);
  const msgs = await bus.pull('t', { maxMessages: 5 });
  assert.equal(msgs.length, 1);
  // What pull returns is the application payload, not the wire envelope.
  assert.deepEqual(msgs[0].payload, payload);
});

test('pull: TTL-expired messages are invisible to pull', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('t', 100);
  bus.publish('t', 'fresh');
  clock.nowMs += 50;
  bus.publish('t', 'also-fresh');
  clock.nowMs += 60; // first message's deadline (t=1100) has passed
  const msgs = await bus.pull('t', { maxMessages: 10, fromOffset: 0 });
  assert.deepEqual(
    msgs.map((m) => m.payload),
    ['also-fresh'],
  );
});

test('pull: stats count pulls, pulled messages and long-poll timeouts', async () => {
  const bus = new EventBus();
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  assert.deepEqual(bus.getStats().pullStats, {
    pulls: 0,
    pulledMessages: 0,
    longPollTimeouts: 0,
  });
  await bus.pull('t', { maxMessages: 1 });
  await bus.pull('t', { maxMessages: 10, fromOffset: 1 });
  await bus.pull('quiet', { maxMessages: 5, waitMs: 30 });
  assert.deepEqual(bus.getStats().pullStats, {
    pulls: 3,
    pulledMessages: 2,
    longPollTimeouts: 1,
  });
});

test('pull: unauthorized topic throws AclDeniedError like subscribe', async () => {
  const bus = new EventBus({
    acl: {
      defaultPolicy: 'deny',
      rules: [{ pattern: 'open.**', publish: 'allow', subscribe: 'allow' }],
    },
  });
  await assert.rejects(bus.pull('closed.topic', { maxMessages: 5 }), AclDeniedError);
  const ok = await bus.pull('open.topic', { maxMessages: 5 });
  assert.deepEqual(ok, []);
});

test('pull: messageId and publisherId ride along when present', async () => {
  const bus = new EventBus();
  bus.publish('t', 'p', { messageId: 'm-1', publisherId: 'svc-a' });
  const [msg] = await bus.pull('t', { maxMessages: 5 });
  assert.equal(msg.messageId, 'm-1');
  assert.equal(msg.publisherId, 'svc-a');
});

test('pull: namespaced topic resolves like publish/subscribe', async () => {
  const bus = new EventBus();
  bus.createNamespace('t1', { allowPublish: true, allowSubscribe: true });
  bus.publish('orders', 'n1', { namespace: 't1' });
  const msgs = await bus.pull('orders', { maxMessages: 5, namespace: 't1' });
  assert.equal(msgs.length, 1);
  assert.equal(msgs[0].topic, 't1/orders');
  assert.equal(msgs[0].payload, 'n1');
  // The global name does not see the namespaced message.
  assert.deepEqual(await bus.pull('orders', { maxMessages: 5 }), []);
});
