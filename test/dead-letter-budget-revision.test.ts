import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setDeadLetterBudget rejects unknown, non-reliable, and DLQ-less subscribers', () => {
  const bus = new EventBus();
  const plain = bus.subscribe('plain', () => {});
  const noDlq = bus.subscribeReliable('nodlq', () => {}, { ackTimeoutMs: 1000 });
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1 } },
  );

  assert.throws(() => bus.setDeadLetterBudget('', { maxRedeliveries: 2 }), RangeError);
  assert.throws(() => bus.setDeadLetterBudget('no-such-sub', { maxRedeliveries: 2 }), RangeError);
  assert.throws(() => bus.setDeadLetterBudget(plain.id, { maxRedeliveries: 2 }), RangeError);
  assert.throws(() => bus.setDeadLetterBudget(noDlq.id, { maxRedeliveries: 2 }), RangeError);
  assert.throws(() => bus.setDeadLetterBudget(sub.id, null as never), RangeError);
  assert.throws(() => bus.setDeadLetterBudget(sub.id, 3 as never), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_budget_revised' }).length,
    0,
    'no failed revision is audited',
  );
});

test('setDeadLetterBudget illegal values are fail-closed', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1, maxEntries: 2 } },
  );

  for (const bad of [{ maxRedeliveries: -1 }, { maxRedeliveries: 1.5 }, { maxRedeliveries: Number.NaN }]) {
    assert.throws(() => bus.setDeadLetterBudget(sub.id, bad), RangeError);
  }
  for (const bad of [{ maxEntries: 0 }, { maxEntries: -3 }, { maxEntries: 1.5 }]) {
    assert.throws(() => bus.setDeadLetterBudget(sub.id, bad), RangeError);
  }
  for (const bad of [{ maxAgeMs: 0 }, { maxAgeMs: -5 }, { maxAgeMs: Number.NaN }, { maxAgeMs: Number.POSITIVE_INFINITY }]) {
    assert.throws(() => bus.setDeadLetterBudget(sub.id, bad), RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_budget_revised' }).length,
    0,
    'no failed revision is audited',
  );

  // The original budget is untouched: budget 1 still dead-letters on
  // the second failure.
  bus.publish('t', 'doomed');
  await sleep(100);
  assert.deepEqual(seen, [0, 1]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('setDeadLetterBudget raising the budget rescues an in-flight message', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries === 0) {
        // Still in the immediate redelivery loop: raising the budget
        // from 1 to 5 must rescue this message.
        bus.setDeadLetterBudget(sub.id, { maxRedeliveries: 5 });
      }
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 1 } },
  );
  bus.publish('t', 'flaky');
  await sleep(200);
  // Without the rescue the message would have dead-lettered after
  // delivery 1; with it, it survives to the new budget of 5.
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 5]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'spending the new budget dead-letters');
});

test('setDeadLetterBudget lowering the budget dead-letters on the next failure', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      if (d.redeliveries === 1) {
        // The next failure must be judged by the new budget of 0.
        bus.setDeadLetterBudget(sub.id, { maxRedeliveries: 0 });
      }
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 5 } },
  );
  bus.publish('t', 'doomed');
  await sleep(200);
  assert.deepEqual(seen, [0, 1]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('setDeadLetterBudget leaves messages already on the retry ladder alone', async () => {
  const bus = new EventBus();
  const raised: number[] = [];
  const raisedSub = bus.subscribeReliable(
    'raised',
    (d) => {
      raised.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, retryLadder: [30, 60] } },
  );
  const lowered: number[] = [];
  const loweredSub = bus.subscribeReliable(
    'lowered',
    (d) => {
      lowered.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 2, retryLadder: [30, 60] } },
  );

  bus.publish('raised', 'a');
  bus.publish('lowered', 'b');
  await flush();
  assert.deepEqual(raised, [0], 'first failure arms rung 0');
  assert.deepEqual(lowered, [0, 1, 2], 'budget 2 exhausts on the third failure and arms rung 0');
  // Raising must not rescue the laddered message; lowering must not
  // pull down the already-armed rung.
  bus.setDeadLetterBudget(raisedSub.id, { maxRedeliveries: 5 });
  bus.setDeadLetterBudget(loweredSub.id, { maxRedeliveries: 0 });
  await sleep(250);
  assert.deepEqual(raised, [0, 1, 2], 'both rungs fire on their old schedule, then dead-letter');
  assert.deepEqual(lowered, [0, 1, 2, 3, 4], 'both rungs fire on their old schedule, then dead-letter');
  assert.equal(bus.getDeadLetterMessages(raisedSub.id).length, 1);
  assert.equal(bus.getDeadLetterMessages(loweredSub.id).length, 1);
});

test('setDeadLetterBudget maxEntries tightening evicts the oldest on the next enqueue, loosening holds more', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, maxEntries: 3 } },
  );
  const deadLetter = async (payload: string) => {
    bus.publish('t', payload);
    await sleep(50);
  };
  await deadLetter('a');
  await deadLetter('b');
  await deadLetter('c');
  assert.deepEqual(
    bus.getDeadLetterMessages(sub.id).map((e) => e.payload),
    ['a', 'b', 'c'],
  );

  bus.setDeadLetterBudget(sub.id, { maxEntries: 2 });
  assert.deepEqual(
    bus.getDeadLetterMessages(sub.id).map((e) => e.payload),
    ['a', 'b', 'c'],
    'tightening does not immediately trim',
  );
  await deadLetter('d');
  assert.deepEqual(
    bus.getDeadLetterMessages(sub.id).map((e) => e.payload),
    ['b', 'c', 'd'],
    'the next enqueue evicts the oldest under the new bound (the old bound of 3 would have kept a)',
  );

  bus.setDeadLetterBudget(sub.id, { maxEntries: 5 });
  await deadLetter('e');
  await deadLetter('f');
  assert.deepEqual(
    bus.getDeadLetterMessages(sub.id).map((e) => e.payload),
    ['b', 'c', 'd', 'e', 'f'],
    'loosening holds more',
  );
});

test('setDeadLetterBudget newly set maxAgeMs expires old entries on the next lazy sweep', async () => {
  const clock = { nowMs: 1000, now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', 'old');
  await sleep(50);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);

  clock.nowMs = 5000;
  bus.setDeadLetterBudget(sub.id, { maxAgeMs: 1000 });
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), [], 'deadLetteredAt 1000 + 1000 <= now 5000');
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
});

test('setDeadLetterBudget maxAgeMs null clears the TTL', async () => {
  const clock = { nowMs: 1000, now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, maxAgeMs: 1000 } },
  );
  bus.publish('t', 'kept');
  await sleep(50);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);

  clock.nowMs = 1500;
  bus.setDeadLetterBudget(sub.id, { maxAgeMs: null });
  clock.nowMs = 5000;
  assert.equal(
    bus.getDeadLetterMessages(sub.id).length,
    1,
    'clearing stops expiry: the entry no longer ages out',
  );
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
  const audit = bus.getSubscriptionAudit({ action: 'dead_letter_budget_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    maxRedeliveries: 0,
    oldMaxRedeliveries: 0,
    maxEntries: 1000,
    oldMaxEntries: 1000,
    maxAgeMs: null,
    oldMaxAgeMs: 1000,
  });
});

test('setDeadLetterBudget same values are a no-op with no audit', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 5, maxEntries: 1000 } },
  );
  bus.setDeadLetterBudget(sub.id, { maxRedeliveries: 5, maxEntries: 1000 });
  bus.setDeadLetterBudget(sub.id, {});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_budget_revised' }).length,
    0,
  );
  // A real revision afterwards still audits exactly once.
  bus.setDeadLetterBudget(sub.id, { maxRedeliveries: 3 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_budget_revised' }).length,
    1,
  );
});

test('setDeadLetterBudget audit is exact and a partial revision leaves other fields', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 5, maxEntries: 1000 } },
  );
  bus.setDeadLetterBudget(sub.id, { maxRedeliveries: 2 });
  let audit = bus.getSubscriptionAudit({ action: 'dead_letter_budget_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    maxRedeliveries: 2,
    oldMaxRedeliveries: 5,
    maxEntries: 1000,
    oldMaxEntries: 1000,
    maxAgeMs: null,
    oldMaxAgeMs: null,
  });

  bus.setDeadLetterBudget(sub.id, { maxAgeMs: 5000 });
  audit = bus.getSubscriptionAudit({ action: 'dead_letter_budget_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    maxRedeliveries: 2,
    oldMaxRedeliveries: 2,
    maxEntries: 1000,
    oldMaxEntries: 1000,
    maxAgeMs: 5000,
    oldMaxAgeMs: null,
  });

  bus.setDeadLetterBudget(sub.id, { maxAgeMs: null });
  audit = bus.getSubscriptionAudit({ action: 'dead_letter_budget_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[2].detail, {
    maxRedeliveries: 2,
    oldMaxRedeliveries: 2,
    maxEntries: 1000,
    oldMaxEntries: 1000,
    maxAgeMs: null,
    oldMaxAgeMs: 5000,
  });
});

test('setDeadLetterBudget live maxEntries shows in the health DLQ row', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, maxEntries: 2 } },
  );
  bus.publish('t', 'poison');
  await sleep(50);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);

  const before = bus.exportBusHealth().checks.find((c) => c.name === 'dead-letter-backlog');
  assert.ok(before !== undefined);
  assert.match(before.detail, /1\/2 entries/);

  bus.setDeadLetterBudget(sub.id, { maxEntries: 5 });
  const after = bus.exportBusHealth().checks.find((c) => c.name === 'dead-letter-backlog');
  assert.ok(after !== undefined);
  assert.match(after.detail, /1\/5 entries/);
});
