import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setRetryLadder leaves an armed rung alone but spaces the next rung by the new ladder', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack(); // always fail
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [30, 60] } },
  );
  bus.publish('t', 'flaky');
  await flush();
  assert.deepEqual(seen, [0]);
  // The nack above armed rung 0 at 30 ms; revising must not move it.
  bus.setRetryLadder(sub.id, [400, 400]);
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the armed rung fires on its original schedule');
  // Rung 0 failed: the next rung arms from the new ladder, not the old 60 ms.
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the new 400 ms rung has not fired yet');
  await sleep(300);
  assert.deepEqual(seen, [0, 1, 2], 'the new ladder spaces the second rung');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the spent new ladder dead-letters');

  const audit = bus.getSubscriptionAudit({ action: 'retry_ladder_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    retryLadder: [400, 400],
    oldRetryLadder: [30, 60],
  });
});

test('setRetryLadder null clears: an armed rung still fires once, then immediate dead-letter', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [30, 60] } },
  );
  bus.publish('t', 'doomed');
  await flush();
  bus.setRetryLadder(sub.id, null);
  await sleep(200);
  assert.deepEqual(seen, [0, 1], 'the armed rung fires once, but no second rung follows');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the rung failure dead-letters immediately');

  const audit = bus.getSubscriptionAudit({ action: 'retry_ladder_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { retryLadder: null, oldRetryLadder: [30, 60] });

  assert.throws(() => bus.setRetryLadder(sub.id, [10]), RangeError, 'a revision after a clear is refused');
  assert.throws(() => bus.setRetryLadder(sub.id, null), RangeError, 'a second clear is refused too');
});

test('setRetryLadder empty array clears like null', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [30, 60] } },
  );
  bus.publish('t', 'doomed');
  await flush();
  bus.setRetryLadder(sub.id, []);
  await sleep(200);
  assert.deepEqual(seen, [0, 1]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  const audit = bus.getSubscriptionAudit({ action: 'retry_ladder_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { retryLadder: null, oldRetryLadder: [30, 60] });
});

test('setRetryLadder is fail-closed and defensively copies the new delays', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [30, 60] } },
  );
  const plain = bus.subscribe('plain', () => {});
  const noDlq = bus.subscribeReliable('nodlq', () => {}, { ackTimeoutMs: 1000 });
  const noLadder = bus.subscribeReliable('noladder', () => {}, {
    ackTimeoutMs: 1000,
    deadLetter: { maxRedeliveries: 0 },
  });

  assert.throws(() => bus.setRetryLadder('', [10]), RangeError);
  assert.throws(() => bus.setRetryLadder('no-such-sub', [10]), RangeError);
  assert.throws(() => bus.setRetryLadder(plain.id, [10]), RangeError, 'a plain subscriber has no ladder');
  assert.throws(() => bus.setRetryLadder(noDlq.id, [10]), RangeError, 'no dead-letter queue, no ladder');
  assert.throws(() => bus.setRetryLadder(noLadder.id, [10]), RangeError, 'a DLQ without a ladder cannot be revised');
  assert.throws(() => bus.setRetryLadder(sub.id, undefined as never), RangeError, 'undefined is not a clear');
  assert.throws(() => bus.setRetryLadder(sub.id, 1000 as never), RangeError);
  assert.throws(() => bus.setRetryLadder(sub.id, '1000' as never), RangeError);
  for (const bad of [[-1], [Number.NaN], [Number.POSITIVE_INFINITY], [1000, -5], ['1000']]) {
    assert.throws(() => bus.setRetryLadder(sub.id, bad as never), RangeError);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'retry_ladder_revised' }).length, 0, 'no failed revision is audited');

  // The original ladder is untouched by every failed attempt, and the
  // caller's array cannot move a rung once a revision succeeds.
  const delays = [30, 60];
  bus.setRetryLadder(sub.id, delays);
  delays[0] = 5000;
  delays.push(9999);
  bus.publish('t', 'flaky');
  await flush();
  await sleep(250);
  assert.deepEqual(seen, [0, 1, 2], 'the ladder still walks its copied delays, not the mutated input');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  plain.unsubscribe();
  noDlq.unsubscribe();
  noLadder.unsubscribe();
});
