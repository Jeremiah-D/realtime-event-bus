import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Publishes once and drains until the subscriber's DLQ holds `count` entries. */
async function poisonByThrowing(
  bus: EventBus,
  subId: string,
  topic: string,
  payload: unknown,
  count: number,
): Promise<void> {
  bus.publish(topic, payload);
  for (let i = 0; i < count * 3; i++) {
    await flush();
    if (bus.getDeadLetterMessages(subId).length >= count) return;
  }
  throw new Error('timed out waiting for the DLQ to fill');
}

test('repeated handler throws land in the DLQ with lastError carrying the thrown message', async () => {
  const bus = new EventBus();
  let throws = 0;
  const sub = bus.subscribeReliable(
    't',
    () => {
      throws += 1;
      throw new Error(`downstream exploded #${throws}`);
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 2 } },
  );
  await poisonByThrowing(bus, sub.id, 't', { order: 1 }, 1);

  assert.equal(throws, 3); // initial + 2 redeliveries, then DLQ — no 4th attempt
  const [entry] = bus.getDeadLetterMessages(sub.id);
  assert.equal(entry.seq, 1);
  assert.equal(entry.topic, 't');
  assert.deepEqual(entry.payload, { order: 1 });
  assert.equal(entry.redeliveries, 2);
  assert.equal(entry.lastError, 'downstream exploded #3');
  assert.ok(typeof entry.deadLetteredAt === 'number');
});

test('explicit nack() loops record lastError as nack', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', (d) => d.nack(), {
    ackTimeoutMs: 1000,
    deadLetter: { maxRedeliveries: 1 },
  });
  await poisonByThrowing(bus, sub.id, 't', 'nack-poison', 1);
  const [entry] = bus.getDeadLetterMessages(sub.id);
  assert.equal(entry.lastError, 'nack');
});

test('silent ack timeouts record lastError as ack-timeout', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {}, {
    ackTimeoutMs: 5, // never settles: every delivery times out
    deadLetter: { maxRedeliveries: 1 },
  });
  bus.publish('t', 'timeout-poison');
  await sleep(50);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].lastError, 'ack-timeout');
});

test('lastError describes the final failure, not a stale throw from an earlier round', async () => {
  const bus = new EventBus();
  let calls = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      calls += 1;
      if (calls === 1) throw new Error('first-boom');
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1 } },
  );
  await poisonByThrowing(bus, sub.id, 't', 'mixed', 1);
  const [entry] = bus.getDeadLetterMessages(sub.id);
  // The throw was consumed by the first redelivery; the final failure was
  // an explicit nack, and that is what the record must say.
  assert.equal(entry.lastError, 'nack');
});

test('a non-Error throw is stringified into lastError', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {
      // eslint-disable-next-line no-throw-literal
      throw 'plain string failure';
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  await poisonByThrowing(bus, sub.id, 't', 'x', 1);
  const [entry] = bus.getDeadLetterMessages(sub.id);
  assert.equal(entry.lastError, 'plain string failure');
});

test('batch reliable handlers record the thrown error per message', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {
      throw new Error('batch boom');
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 }, batch: { maxSize: 2, maxWaitMs: 10_000 } },
  );
  bus.publish('t', 'b1');
  bus.publish('t', 'b2');
  for (let i = 0; i < 10; i++) {
    await flush();
    if (bus.getDeadLetterMessages(sub.id).length >= 2) break;
  }
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 2);
  for (const e of entries) assert.equal(e.lastError, 'batch boom');
});

test('DLQ entries carry the end-to-end traceId', async () => {
  const spans: unknown[] = [];
  const bus = new EventBus({ trace: { sampleRate: 1, onTraceSpan: (s) => spans.push(s) } });
  const sub = bus.subscribeReliable('t', () => {
    throw new Error('traced poison');
  }, { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } });
  const upstreamTraceId = '4bf92f3577b34da6a3ce929d0e0e4736';
  bus.publish('t', 'traced', { traceparent: `00-${upstreamTraceId}-00f067aa0ba902b7-01` });
  for (let i = 0; i < 10; i++) {
    await flush();
    if (bus.getDeadLetterMessages(sub.id).length >= 1) break;
  }
  const [entry] = bus.getDeadLetterMessages(sub.id);
  assert.equal(entry.payload, 'traced');
  assert.equal(entry.traceId, upstreamTraceId);
});

test('DLQ entries omit traceId when tracing is disabled', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {
    throw new Error('untraced poison');
  }, { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } });
  await poisonByThrowing(bus, sub.id, 't', 'plain', 1);
  const [entry] = bus.getDeadLetterMessages(sub.id);
  assert.equal(entry.traceId, undefined);
});

test('getDeadLetterMessages supports a limit for newest-first triage', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', (d) => d.nack(), {
    ackTimeoutMs: 1000,
    deadLetter: { maxRedeliveries: 0 },
  });
  for (const p of ['a', 'b', 'c']) {
    bus.publish('t', p);
    await flush();
    await flush();
  }
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 3);
  const limited = bus.getDeadLetterMessages(sub.id, { limit: 2 });
  assert.equal(limited.length, 2);
  // Most recently dead-lettered first: seq 2 and 3.
  assert.deepEqual(limited.map((e) => e.payload), ['b', 'c']);
  assert.deepEqual(limited.map((e) => e.seq), [2, 3]);
});

test('getDeadLetterMessages rejects a non-positive-integer limit', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', (d) => d.ack(), {
    deadLetter: true,
  });
  for (const bad of [0, -1, 1.5, Number.NaN]) {
    assert.throws(() => bus.getDeadLetterMessages(sub.id, { limit: bad }), RangeError);
  }
});

test('unsubscribe clears the subscriber DLQ', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', (d) => d.nack(), {
    ackTimeoutMs: 1000,
    deadLetter: { maxRedeliveries: 0 },
  });
  await poisonByThrowing(bus, sub.id, 't', 'doomed', 1);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  sub.unsubscribe();
  assert.throws(() => bus.getDeadLetterMessages(sub.id), /unknown subscriber/);
});

test('replay resets the failure record along with the redelivery budget', async () => {
  const bus = new EventBus();
  let calls = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      calls += 1;
      if (calls === 1) throw new Error('original sin');
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  await poisonByThrowing(bus, sub.id, 't', 'replay-me', 1);
  const [first] = bus.getDeadLetterMessages(sub.id);
  assert.equal(first.lastError, 'original sin');
  assert.equal(bus.replayDeadLetter(sub.id, first.seq), true);
  // The replayed message fails by explicit nack this time (calls >= 2).
  for (let i = 0; i < 20; i++) {
    await flush();
    if (bus.getDeadLetterMessages(sub.id).length >= 1) break;
  }
  const second = bus.getDeadLetterMessages(sub.id).find((e) => e.payload === 'replay-me');
  assert.ok(second, 'replayed message should be back in the DLQ');
  // The replayed message failed by nack, not by the pre-replay throw:
  // the old error must not leak into the new record.
  assert.equal(second.lastError, 'nack');
});

test('deadLetteredMessages counts every poison message', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', (d) => d.nack(), {
    ackTimeoutMs: 1000,
    deadLetter: { maxRedeliveries: 0 },
  });
  bus.publish('t', 'one');
  bus.publish('t', 'two');
  await poisonByThrowing(bus, sub.id, 't', 'three', 3);
  assert.equal(bus.getStats().deadLetteredMessages, 3);
});
