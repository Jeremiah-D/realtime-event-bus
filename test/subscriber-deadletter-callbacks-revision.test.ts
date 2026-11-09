import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type DeadLetterEvent,
  type PoisonedMessageEvent,
} from '../src/bus.ts';

/**
 * Dead-letter callback revision (EB-434):
 * `reviseSubscriberDeadLetterCallbacks` swaps a reliable subscriber's
 * `onDeadLetter` and/or `onPoisoned` in place — only the callbacks
 * move, the budget, the retry ladder, the auto-replay config, the
 * DLQ entries, and the diagnostic topic are untouched, and the new
 * callbacks govern from the next dead-lettering / poisoning.
 * Changing the alert recipient no longer means re-subscribing (and
 * losing the ACK state, the redelivery budget, and the DLQ contents).
 *
 * Determinism trick (as in `deadletter.test.ts`): a handler that
 * always nacks with `maxRedeliveries: 0` dead-letters on the first
 * failure, and `autoReplay: { maxAutoReplayAttempts: 0 }` poisons
 * immediately — no timers, no waiting.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Publishes once and drains until the subscriber's DLQ holds `count` entries. */
async function deadLetter(
  bus: EventBus,
  subId: string,
  topic: string,
  payload: unknown,
  count: number,
): Promise<void> {
  bus.publish(topic, payload);
  for (let i = 0; i < count * 3 + 3; i++) {
    await flush();
    if (bus.getDeadLetterMessages(subId).length >= count) return;
  }
  throw new Error('timed out waiting for the DLQ to fill');
}

test('revising onDeadLetter routes the next dead-lettering to the new callback', async () => {
  const bus = new EventBus();
  const oldEvents: DeadLetterEvent[] = [];
  const newEvents: DeadLetterEvent[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    {
      ackTimeoutMs: 1000,
      deadLetter: { maxRedeliveries: 0, onDeadLetter: (e) => oldEvents.push(e) },
    },
  );
  await deadLetter(bus, sub.id, 't', 'first', 1);
  assert.equal(oldEvents.length, 1);
  assert.equal(oldEvents[0].entry.payload, 'first');

  bus.reviseSubscriberDeadLetterCallbacks(sub.id, {
    onDeadLetter: (e) => newEvents.push(e),
  });
  const audit = bus.getSubscriptionAudit({ action: 'subscriber_dead_letter_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, 't');
  assert.deepEqual(audit[0].detail, {
    onDeadLetter: 'set',
    oldOnDeadLetter: 'set',
    onPoisoned: 'unchanged',
    oldOnPoisoned: 'absent',
  });

  await deadLetter(bus, sub.id, 't', 'second', 2);
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].entry.payload, 'second');
  assert.equal(newEvents[0].entry.seq, 2);
  assert.equal(oldEvents.length, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 2);
});

test('revising onPoisoned routes the next poisoning to the new callback', async () => {
  const bus = new EventBus();
  const oldEvents: PoisonedMessageEvent[] = [];
  const newEvents: PoisonedMessageEvent[] = [];
  const sub = bus.subscribeReliable(
    't',
    () => {
      throw new Error('fail');
    },
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        autoReplay: { maxAutoReplayAttempts: 0 },
        onPoisoned: (e) => oldEvents.push(e),
      },
    },
  );
  await deadLetter(bus, sub.id, 't', 'first', 1);
  assert.equal(oldEvents.length, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].poisoned, true);

  bus.reviseSubscriberDeadLetterCallbacks(sub.id, {
    onPoisoned: (e) => newEvents.push(e),
  });
  const audit = bus.getSubscriptionAudit({ action: 'subscriber_dead_letter_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onDeadLetter: 'unchanged',
    oldOnDeadLetter: 'absent',
    onPoisoned: 'set',
    oldOnPoisoned: 'set',
  });

  await deadLetter(bus, sub.id, 't', 'second', 2);
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].entry.poisoned, true);
  assert.equal(oldEvents.length, 1);
});

test('clearing onDeadLetter with null keeps the DLQ move and diagnostics, silencing only the operator', async () => {
  const bus = new EventBus();
  const operatorEvents: DeadLetterEvent[] = [];
  const diagnostics: unknown[] = [];
  bus.subscribe('dlq.diag', (m) => diagnostics.push(m.payload));
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        diagnosticTopic: 'dlq.diag',
        onDeadLetter: (e) => operatorEvents.push(e),
      },
    },
  );
  await deadLetter(bus, sub.id, 't', 'first', 1);
  await flush();
  assert.equal(operatorEvents.length, 1);
  assert.equal(diagnostics.length, 1);

  bus.reviseSubscriberDeadLetterCallbacks(sub.id, { onDeadLetter: null });
  const audit = bus.getSubscriptionAudit({ action: 'subscriber_dead_letter_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onDeadLetter: 'cleared',
    oldOnDeadLetter: 'set',
    onPoisoned: 'unchanged',
    oldOnPoisoned: 'absent',
  });

  await deadLetter(bus, sub.id, 't', 'second', 2);
  await flush();
  // The DLQ still moves and the diagnostic topic still fans out —
  // only the operator callback is silent now.
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 2);
  assert.equal(diagnostics.length, 2);
  assert.equal(operatorEvents.length, 1);
});

test('a partial revision keeps the unprovided callback live', async () => {
  const bus = new EventBus();
  const oldDead: DeadLetterEvent[] = [];
  const newDead: DeadLetterEvent[] = [];
  const oldPoisoned: PoisonedMessageEvent[] = [];
  const sub = bus.subscribeReliable(
    't',
    () => {
      throw new Error('fail');
    },
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        autoReplay: { maxAutoReplayAttempts: 0 },
        onDeadLetter: (e) => oldDead.push(e),
        onPoisoned: (e) => oldPoisoned.push(e),
      },
    },
  );
  await deadLetter(bus, sub.id, 't', 'first', 1);
  assert.equal(oldDead.length, 1);
  assert.equal(oldPoisoned.length, 1);

  // Only onDeadLetter moves; onPoisoned keeps its live value.
  bus.reviseSubscriberDeadLetterCallbacks(sub.id, {
    onDeadLetter: (e) => newDead.push(e),
  });
  const audit = bus.getSubscriptionAudit({ action: 'subscriber_dead_letter_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onDeadLetter: 'set',
    oldOnDeadLetter: 'set',
    onPoisoned: 'unchanged',
    oldOnPoisoned: 'set',
  });

  await deadLetter(bus, sub.id, 't', 'second', 2);
  assert.equal(newDead.length, 1);
  assert.equal(oldDead.length, 1);
  assert.equal(oldPoisoned.length, 2);
});

test('reviseSubscriberDeadLetterCallbacks rejects unknown, empty, non-reliable, DLQ-less, and invalid revisions fail-closed', async () => {
  const bus = new EventBus();
  const deadEvents: DeadLetterEvent[] = [];
  const poisonedEvents: PoisonedMessageEvent[] = [];
  const sub = bus.subscribeReliable(
    't',
    () => {
      throw new Error('fail');
    },
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        autoReplay: { maxAutoReplayAttempts: 0 },
        onDeadLetter: (e) => deadEvents.push(e),
        onPoisoned: (e) => poisonedEvents.push(e),
      },
    },
  );
  const plain = bus.subscribe('t2', () => {});
  const noDlq = bus.subscribeReliable('t3', (d) => d.nack(), { ackTimeoutMs: 1000 });
  const newDead = () => {};
  const newPoisoned = () => {};
  for (const bad of [
    () => bus.reviseSubscriberDeadLetterCallbacks('no-such-sub', { onDeadLetter: newDead }),
    () => bus.reviseSubscriberDeadLetterCallbacks('', { onDeadLetter: newDead }),
    () => bus.reviseSubscriberDeadLetterCallbacks(plain.id, { onDeadLetter: newDead }),
    () => bus.reviseSubscriberDeadLetterCallbacks(noDlq.id, { onDeadLetter: newDead }),
    () => bus.reviseSubscriberDeadLetterCallbacks(sub.id, {}),
    // @ts-expect-error: intentional bad input
    () => bus.reviseSubscriberDeadLetterCallbacks(sub.id, null),
    // @ts-expect-error: intentional bad input
    () => bus.reviseSubscriberDeadLetterCallbacks(sub.id, 'nope'),
    () => bus.reviseSubscriberDeadLetterCallbacks(sub.id, { onDeadLetter: 'x' as never }),
    () => bus.reviseSubscriberDeadLetterCallbacks(sub.id, { onDeadLetter: 42 as never }),
    () => bus.reviseSubscriberDeadLetterCallbacks(sub.id, { onPoisoned: 'x' as never }),
    () => bus.reviseSubscriberDeadLetterCallbacks(sub.id, { onPoisoned: {} as never }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_dead_letter_callbacks_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live callbacks are untouched: the old ones still fire.
  await deadLetter(bus, sub.id, 't', 'x', 1);
  assert.equal(deadEvents.length, 1);
  assert.equal(poisonedEvents.length, 1);
});

test('a same-value revision is a no-op with no audit', () => {
  const bus = new EventBus();
  const onDeadLetter = () => {};
  const onPoisoned = () => {};
  const sub = bus.subscribeReliable('t', (d) => d.nack(), {
    ackTimeoutMs: 1000,
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { maxAutoReplayAttempts: 0 },
      onDeadLetter,
    },
  });
  // Same dead-letter callback reference: nothing moves.
  bus.reviseSubscriberDeadLetterCallbacks(sub.id, { onDeadLetter });
  // Clearing a poison callback that was never set: nothing moves.
  bus.reviseSubscriberDeadLetterCallbacks(sub.id, { onPoisoned: null });
  // Clearing a dead-letter callback that was never set: nothing moves.
  const poisonOnly = bus.subscribeReliable('t2', (d) => d.nack(), {
    ackTimeoutMs: 1000,
    deadLetter: { maxRedeliveries: 0, onPoisoned },
  });
  bus.reviseSubscriberDeadLetterCallbacks(poisonOnly.id, { onDeadLetter: null });
  bus.reviseSubscriberDeadLetterCallbacks(poisonOnly.id, { onPoisoned });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'subscriber_dead_letter_callbacks_revised' }).length,
    0,
    'same-value revisions must not audit',
  );
});

test('entries already in the DLQ are untouched by the revision', async () => {
  const bus = new EventBus();
  const oldEvents: DeadLetterEvent[] = [];
  const newEvents: DeadLetterEvent[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    {
      ackTimeoutMs: 1000,
      deadLetter: { maxRedeliveries: 0, onDeadLetter: (e) => oldEvents.push(e) },
    },
  );
  await deadLetter(bus, sub.id, 't', 'first', 1);
  const before = bus.getDeadLetterMessages(sub.id);
  assert.equal(before.length, 1);
  assert.equal(before[0].seq, 1);
  assert.equal(before[0].payload, 'first');

  bus.reviseSubscriberDeadLetterCallbacks(sub.id, {
    onDeadLetter: (e) => newEvents.push(e),
  });
  // The already-dead-lettered entry keeps its content and seq.
  const after = bus.getDeadLetterMessages(sub.id);
  assert.deepEqual(
    after.map((e) => ({ seq: e.seq, payload: e.payload, redeliveries: e.redeliveries })),
    [{ seq: 1, payload: 'first', redeliveries: 0 }],
  );

  await deadLetter(bus, sub.id, 't', 'second', 2);
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].entry.seq, 2);
  assert.equal(oldEvents.length, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].payload, 'first');
});

test('revising one subscriber leaves another subscriber callbacks untouched', async () => {
  const bus = new EventBus();
  const firstOld: DeadLetterEvent[] = [];
  const firstNew: DeadLetterEvent[] = [];
  const secondOld: DeadLetterEvent[] = [];
  const sub1 = bus.subscribeReliable(
    't1',
    (d) => d.nack(),
    {
      ackTimeoutMs: 1000,
      deadLetter: { maxRedeliveries: 0, onDeadLetter: (e) => firstOld.push(e) },
    },
  );
  const sub2 = bus.subscribeReliable(
    't2',
    (d) => d.nack(),
    {
      ackTimeoutMs: 1000,
      deadLetter: { maxRedeliveries: 0, onDeadLetter: (e) => secondOld.push(e) },
    },
  );
  await deadLetter(bus, sub1.id, 't1', 'a', 1);
  await deadLetter(bus, sub2.id, 't2', 'b', 1);
  assert.equal(firstOld.length, 1);
  assert.equal(secondOld.length, 1);

  bus.reviseSubscriberDeadLetterCallbacks(sub1.id, {
    onDeadLetter: (e) => firstNew.push(e),
  });

  await deadLetter(bus, sub1.id, 't1', 'a2', 2);
  await deadLetter(bus, sub2.id, 't2', 'b2', 2);
  assert.equal(firstNew.length, 1);
  assert.equal(firstOld.length, 1);
  assert.equal(secondOld.length, 2);
});

test('revising both callbacks at once from absent audits both and both fire', async () => {
  const bus = new EventBus();
  const deadEvents: DeadLetterEvent[] = [];
  const poisonedEvents: PoisonedMessageEvent[] = [];
  const sub = bus.subscribeReliable(
    't',
    () => {
      throw new Error('fail');
    },
    {
      ackTimeoutMs: 1000,
      deadLetter: { maxRedeliveries: 0, autoReplay: { maxAutoReplayAttempts: 0 } },
    },
  );
  bus.reviseSubscriberDeadLetterCallbacks(sub.id, {
    onDeadLetter: (e) => deadEvents.push(e),
    onPoisoned: (e) => poisonedEvents.push(e),
  });
  const audit = bus.getSubscriptionAudit({ action: 'subscriber_dead_letter_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onDeadLetter: 'set',
    oldOnDeadLetter: 'absent',
    onPoisoned: 'set',
    oldOnPoisoned: 'absent',
  });
  await deadLetter(bus, sub.id, 't', 'x', 1);
  assert.equal(deadEvents.length, 1);
  assert.equal(poisonedEvents.length, 1);
});
