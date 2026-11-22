import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setRetryLadder enables a ladder on a DLQ subscriber that never had one', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack(); // always fail
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );

  const delays = [30, 300];
  bus.setRetryLadder(sub.id, delays);
  // The caller's array is defensively copied: mutating it afterwards
  // must not move a rung.
  delays[0] = 5000;
  delays.push(9999);

  bus.publish('t', 'flaky');
  await flush();
  assert.deepEqual(seen, [0]);
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'rung 0 fires on its copied 30 ms delay');
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the 300 ms second rung has not fired yet');
  await sleep(250);
  assert.deepEqual(seen, [0, 1, 2], 'rung 1 fires on its copied 300 ms delay');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the spent ladder dead-letters');

  const audit = bus.getSubscriptionAudit({ action: 'retry_ladder_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    retryLadder: [30, 300],
    oldRetryLadder: null,
  });
});

test('setRetryLadder enable does not resurrect a message that already dead-lettered', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', 'doomed');
  await flush();
  await sleep(50);
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'dead-lettered while ladderless');

  bus.setRetryLadder(sub.id, [30, 60]);
  await sleep(200);
  assert.deepEqual(seen, [0], 'no rung is armed for the already-dead-lettered message');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);

  // The enable itself is audited, and the next message walks the ladder.
  const audit = bus.getSubscriptionAudit({ action: 'retry_ladder_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { retryLadder: [30, 60], oldRetryLadder: null });
  bus.publish('t', 'flaky');
  await flush();
  await sleep(250);
  // `redeliveries` counts per message, so the second message starts at 0 again.
  assert.deepEqual(seen, [0, 0, 1, 2], 'the next message walks the fresh ladder');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 2);
});

test('setRetryLadder null on a never-laddered subscriber is a silent no-op', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.setRetryLadder(sub.id, null);
  assert.equal(bus.getSubscriptionAudit({ action: 'retry_ladder_revised' }).length, 0, 'no audit for a no-op');

  // The subscriber is still ladderless: the next budget exhaustion
  // dead-letters immediately — and a later enable still works.
  bus.publish('t', 'doomed');
  await flush();
  await sleep(50);
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  bus.setRetryLadder(sub.id, [30]);
  assert.equal(bus.getSubscriptionAudit({ action: 'retry_ladder_revised' }).length, 1);
});

test('setRetryLadder empty array on a never-laddered subscriber is a silent no-op', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.setRetryLadder(sub.id, []);
  assert.equal(bus.getSubscriptionAudit({ action: 'retry_ladder_revised' }).length, 0, 'no audit for a no-op');

  bus.publish('t', 'doomed');
  await flush();
  await sleep(50);
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'still ladderless');
  bus.setRetryLadder(sub.id, [30]);
  assert.equal(bus.getSubscriptionAudit({ action: 'retry_ladder_revised' }).length, 1);
});

test('setRetryLadder null clears and a later enable installs a fresh ladder', async () => {
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
  bus.setRetryLadder(sub.id, null);
  bus.setRetryLadder(sub.id, [30, 60]);

  const audit = bus.getSubscriptionAudit({ action: 'retry_ladder_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { retryLadder: null, oldRetryLadder: [30, 60] });
  assert.deepEqual(audit[1].detail, { retryLadder: [30, 60], oldRetryLadder: null });

  bus.publish('t', 'flaky');
  await flush();
  await sleep(250);
  assert.deepEqual(seen, [0, 1, 2], 'the re-enabled ladder walks its rungs');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
});

test('setRetryLadder revision after an enable leaves an armed rung on its old delay', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.setRetryLadder(sub.id, [30, 60]);
  bus.publish('t', 'flaky');
  await flush();
  assert.deepEqual(seen, [0]);
  // Rung 0 is armed at 30 ms; revising must not move it.
  bus.setRetryLadder(sub.id, [400, 400]);
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the armed rung fires on its original schedule');
  await sleep(150);
  assert.deepEqual(seen, [0, 1], 'the new 400 ms rung has not fired yet');
  await sleep(300);
  assert.deepEqual(seen, [0, 1, 2], 'the new ladder spaces the second rung');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);

  const audit = bus.getSubscriptionAudit({ action: 'retry_ladder_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, { retryLadder: [30, 60], oldRetryLadder: null });
  assert.deepEqual(audit[1].detail, { retryLadder: [400, 400], oldRetryLadder: [30, 60] });
});

test('setRetryLadder enable on a reliable subscriber without a dead-letter queue throws', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {}, { ackTimeoutMs: 1000 });
  assert.throws(() => bus.setRetryLadder(sub.id, [10]), RangeError, 'no dead-letter queue, no ladder to enable');
  assert.equal(bus.getSubscriptionAudit({ action: 'retry_ladder_revised' }).length, 0, 'no failed enable is audited');
});

test('setRetryLadder enable is fail-closed and leaves the subscriber ladderless', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack();
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  const plain = bus.subscribe('plain', () => {});

  assert.throws(() => bus.setRetryLadder('', [10]), RangeError);
  assert.throws(() => bus.setRetryLadder('no-such-sub', [10]), RangeError);
  assert.throws(() => bus.setRetryLadder(plain.id, [10]), RangeError, 'a plain subscriber has no ladder to enable');
  assert.throws(() => bus.setRetryLadder(sub.id, undefined as never), RangeError, 'undefined is not an enable');
  assert.throws(() => bus.setRetryLadder(sub.id, 1000 as never), RangeError);
  assert.throws(() => bus.setRetryLadder(sub.id, '1000' as never), RangeError);
  for (const bad of [[-1], [Number.NaN], [Number.POSITIVE_INFINITY], [1000, -5], ['1000']]) {
    assert.throws(() => bus.setRetryLadder(sub.id, bad as never), RangeError);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'retry_ladder_revised' }).length, 0, 'no failed enable is audited');

  // Every failed attempt left the subscriber ladderless: the next
  // budget exhaustion still dead-letters immediately.
  bus.publish('t', 'doomed');
  await flush();
  await sleep(50);
  assert.deepEqual(seen, [0]);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  plain.unsubscribe();
});

test('setRetryLadder enable audit records the ladder with oldRetryLadder null and actor system', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {}, {
    ackTimeoutMs: 1000,
    deadLetter: { maxRedeliveries: 0 },
  });
  bus.setRetryLadder(sub.id, [30, 60]);
  const audit = bus.getSubscriptionAudit({ action: 'retry_ladder_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, { retryLadder: [30, 60], oldRetryLadder: null });
});

test('setRetryLadder enable does not apply to a group member (plain subscribers have no ladder)', () => {
  const bus = new EventBus();
  // Consumer-group members are plain subscribers by construction
  // (`subscribeToGroup` wraps `subscribe`, never `subscribeReliable`),
  // so there is no reliable group member to enable — the enable is
  // refused exactly like any other plain subscriber.
  const member = bus.subscribeToGroup('g1', 't', () => {});
  assert.throws(() => bus.setRetryLadder(member.id, [10]), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'retry_ladder_revised' }).length, 0);
  member.unsubscribe();
});

test('setRetryLadder enable arms a message already in its instant redelivery loop', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.redeliveries);
      d.nack(); // always fail
    },
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 2 } },
  );
  bus.publish('t', 'flaky');
  // The enable lands synchronously after the publish, before any
  // delivery: the instant budget (2 redeliveries) is spent first,
  // then the fresh ladder arms instead of an immediate dead-letter.
  bus.setRetryLadder(sub.id, [30, 60]);
  await sleep(300);
  assert.deepEqual(seen, [0, 1, 2, 3, 4], 'instant redeliveries, then rung 0, then rung 1');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1, 'the spent ladder dead-letters');
});
