import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { DeadLetterEntry, PoisonedMessageEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Sleeps for wall-clock ms (auto-replay backoff uses real timers). */
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('invalid autoReplay config throws from subscribeReliable', () => {
  const bus = new EventBus();
  // autoReplay must be true or an object.
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { autoReplay: 'yes' as never },
      }),
    TypeError,
  );
  // initialDelayMs must be a non-negative finite number.
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () =>
        bus.subscribeReliable('t', () => {}, {
          deadLetter: { autoReplay: { initialDelayMs: bad } },
        }),
      RangeError,
    );
  }
  // maxDelayMs must be positive finite.
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { autoReplay: { maxDelayMs: 0 } },
      }),
    RangeError,
  );
  // backoffMultiplier must be >= 1.
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { autoReplay: { backoffMultiplier: 0.5 } },
      }),
    RangeError,
  );
  // maxAutoReplayAttempts must be a non-negative integer.
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { autoReplay: { maxAutoReplayAttempts: -1 } },
      }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { autoReplay: { maxAutoReplayAttempts: 1.5 } },
      }),
    RangeError,
  );
  // onPoisoned must be a function.
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { autoReplay: true, onPoisoned: 'nope' as never },
      }),
    TypeError,
  );
  // Failed subscribes left nothing behind.
  assert.equal(bus.subscriberCount(), 0);
});

test('auto-replay re-enqueues a dead-lettered message after the backoff', async () => {
  const bus = new EventBus();
  const deliveries: string[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', (delivery) => {
    attempts += 1;
    deliveries.push(delivery.msg.payload as string);
    if (attempts === 1) {
      // First delivery fails → maxRedeliveries: 0 dead-letters immediately.
      throw new Error('poison on first try');
    }
    delivery.ack();
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 10, maxAutoReplayAttempts: 3 },
    },
  });
  bus.publish('t', 'hello');
  await flush();
  // Dead-lettered after the first failure.
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  const entry = bus.getDeadLetterMessages(sub.id)[0];
  assert.equal(entry.autoReplayAttempts, 0);
  assert.equal(entry.poisoned, false);
  // Wait for the auto-replay timer (10ms backoff).
  await sleep(50);
  await flush();
  // The message was replayed and acked on the second attempt.
  assert.equal(attempts, 2);
  assert.deepEqual(deliveries, ['hello', 'hello']);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0);
  assert.equal(bus.getStats().autoReplayed, 1);
  assert.equal(bus.getStats().deadLetteredMessages, 1);
});

test('auto-replay preserves the messageId and bypasses dedup', async () => {
  const bus = new EventBus();
  const seenIds = new Set<string>();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', (delivery) => {
    attempts += 1;
    // The replayed message carries the original messageId.
    seenIds.add(delivery.msg.messageId!);
    if (attempts === 1) throw new Error('fail once');
    delivery.ack();
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 10, maxAutoReplayAttempts: 3 },
    },
    deduplicateMessages: true,
  });
  // Publish with an explicit messageId.
  bus.publish('t', 'x', { messageId: 'msg-123' });
  await flush();
  await sleep(50);
  await flush();
  assert.equal(attempts, 2);
  // Both deliveries saw the same messageId — and the redelivery was
  // not suppressed as a duplicate (the EB-134 onRedeliver fix).
  assert.deepEqual([...seenIds], ['msg-123']);
  assert.equal(bus.getStats().autoReplayed, 1);
});

test('poison: attempts exhausted → onPoisoned fires, no further replays', async () => {
  const bus = new EventBus();
  const poisonedEvents: PoisonedMessageEvent[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', (delivery) => {
    attempts += 1;
    // Always fails — every delivery dead-letters.
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 10, maxAutoReplayAttempts: 1 },
      onPoisoned: (e) => poisonedEvents.push(e),
    },
  });
  bus.publish('t', 'doomed');
  await flush();
  // First dead-lettering: attempts=0 < 1, so an auto-replay is scheduled.
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  await sleep(50);
  await flush();
  // The replay was delivered (attempt 2) and failed → dead-lettered
  // again with attempts=1 >= 1 → poisoned.
  assert.equal(attempts, 2);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(entries[0].autoReplayAttempts, 1);
  assert.equal(poisonedEvents.length, 1);
  assert.equal(poisonedEvents[0].autoReplayAttempts, 1);
  assert.equal(poisonedEvents[0].entry.poisoned, true);
  assert.equal(bus.getStats().autoReplayed, 1);
  assert.equal(bus.getStats().autoReplayExhausted, 1);
  assert.equal(bus.getStats().poisoned, 1);
  // No further auto-replays: wait past any backoff and confirm.
  await sleep(50);
  await flush();
  assert.equal(attempts, 2);
  assert.equal(bus.getStats().autoReplayed, 1);
});

test('maxAutoReplayAttempts: 0 poisons immediately without replaying', async () => {
  const bus = new EventBus();
  const poisonedEvents: PoisonedMessageEvent[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error('fail');
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { maxAutoReplayAttempts: 0 },
      onPoisoned: (e) => poisonedEvents.push(e),
    },
  });
  bus.publish('t', 'x');
  await flush();
  assert.equal(attempts, 1);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(poisonedEvents.length, 1);
  assert.equal(bus.getStats().autoReplayed, 0);
  assert.equal(bus.getStats().poisoned, 1);
});

test('unsubscribe cancels a pending auto-replay', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error('fail');
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 30, maxAutoReplayAttempts: 3 },
    },
  });
  bus.publish('t', 'x');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // Unsubscribe before the 30ms backoff fires.
  sub.unsubscribe();
  await sleep(80);
  await flush();
  // No replay happened — the timer was cleared with the subscriber.
  assert.equal(attempts, 1);
  assert.equal(bus.getStats().autoReplayed, 0);
});

test('manual replayDeadLetter cancels the pending auto-replay', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', (delivery) => {
    attempts += 1;
    if (attempts === 1) throw new Error('fail once');
    delivery.ack();
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 30, maxAutoReplayAttempts: 3 },
    },
  });
  bus.publish('t', 'x');
  await flush();
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  // Manual replay before the auto-replay timer fires.
  assert.equal(bus.replayDeadLetter(sub.id, entries[0].seq), true);
  await flush();
  assert.equal(attempts, 2);
  // Wait past the auto-replay backoff — no duplicate replay.
  await sleep(80);
  await flush();
  assert.equal(attempts, 2);
  assert.equal(bus.getStats().autoReplayed, 0);
});

test('auto-replay is off by default: DLQ entries sit without timers', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error('fail');
  }, {
    deadLetter: { maxRedeliveries: 0 },
  });
  bus.publish('t', 'x');
  await flush();
  await sleep(30);
  await flush();
  assert.equal(attempts, 1);
  const entries: DeadLetterEntry[] = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].autoReplayAttempts, 0);
  assert.equal(entries[0].poisoned, false);
  assert.equal(bus.getStats().autoReplayed, 0);
  assert.equal(bus.getStats().poisoned, 0);
});
