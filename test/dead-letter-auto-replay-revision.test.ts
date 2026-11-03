import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { PoisonedMessageEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Sleeps for wall-clock ms (auto-replay backoff uses real timers). */
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setDeadLetterAutoReplay rejects unknown, non-reliable, DLQ-less subscribers and non-object patches', () => {
  const bus = new EventBus();
  const plain = bus.subscribe('plain', () => {});
  const noDlq = bus.subscribeReliable('nodlq', () => {}, { ackTimeoutMs: 1000 });
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, autoReplay: true } },
  );

  assert.throws(() => bus.setDeadLetterAutoReplay('', { initialDelayMs: 5 }), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay('no-such-sub', { initialDelayMs: 5 }), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(plain.id, { initialDelayMs: 5 }), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(noDlq.id, { initialDelayMs: 5 }), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, undefined as never), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, 3 as never), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, 'x' as never), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    0,
    'no failed revision is audited',
  );
});

test('setDeadLetterAutoReplay refuses a subscriber whose deadLetter never enabled autoReplay', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  const disabled = bus.subscribeReliable(
    'f',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, autoReplay: false } },
  );

  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 5 }), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, null), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(disabled.id, { initialDelayMs: 5 }), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(disabled.id, null), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    0,
    'no failed revision is audited',
  );
});

test('setDeadLetterAutoReplay illegal values are fail-closed', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        autoReplay: { initialDelayMs: 100, maxDelayMs: 5000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
      },
    },
  );

  for (const bad of [{ initialDelayMs: -1 }, { initialDelayMs: Number.NaN }, { initialDelayMs: Number.POSITIVE_INFINITY }]) {
    assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, bad), RangeError);
  }
  for (const bad of [{ maxDelayMs: 0 }, { maxDelayMs: -5 }, { maxDelayMs: Number.NaN }, { maxDelayMs: Number.POSITIVE_INFINITY }]) {
    assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, bad), RangeError);
  }
  for (const bad of [{ backoffMultiplier: 0.5 }, { backoffMultiplier: Number.NaN }, { backoffMultiplier: Number.POSITIVE_INFINITY }]) {
    assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, bad), RangeError);
  }
  for (const bad of [{ maxAutoReplayAttempts: -1 }, { maxAutoReplayAttempts: 1.5 }, { maxAutoReplayAttempts: Number.NaN }]) {
    assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, bad), RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    0,
    'no failed revision is audited',
  );

  // The original parameters are untouched: re-stating them is a no-op,
  // and the first real revision audits against the original values.
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 100, maxDelayMs: 5000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    0,
    'unchanged values are still a no-op after the failed revisions',
  );
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 200 });
  const audit = bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail.oldAutoReplay, {
    initialDelayMs: 100,
    maxDelayMs: 5000,
    backoffMultiplier: 2,
    maxAutoReplayAttempts: 3,
  });
});

test('setDeadLetterAutoReplay partial revision leaves the other fields and audits the merge', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        autoReplay: { initialDelayMs: 100, maxDelayMs: 5000, backoffMultiplier: 3, maxAutoReplayAttempts: 2 },
      },
    },
  );

  bus.setDeadLetterAutoReplay(sub.id, { maxDelayMs: 9000 });
  let audit = bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    autoReplay: { initialDelayMs: 100, maxDelayMs: 9000, backoffMultiplier: 3, maxAutoReplayAttempts: 2 },
    oldAutoReplay: { initialDelayMs: 100, maxDelayMs: 5000, backoffMultiplier: 3, maxAutoReplayAttempts: 2 },
  });

  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 50, maxAutoReplayAttempts: 5 });
  audit = bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    autoReplay: { initialDelayMs: 50, maxDelayMs: 9000, backoffMultiplier: 3, maxAutoReplayAttempts: 5 },
    oldAutoReplay: { initialDelayMs: 100, maxDelayMs: 9000, backoffMultiplier: 3, maxAutoReplayAttempts: 2 },
  });
});

test('setDeadLetterAutoReplay raising maxAutoReplayAttempts rescues a message at the poison edge', async () => {
  const bus = new EventBus();
  const poisonedEvents: PoisonedMessageEvent[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    // Always fails — every delivery dead-letters.
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 30, maxAutoReplayAttempts: 1 },
      onPoisoned: (e) => poisonedEvents.push(e),
    },
  });
  bus.publish('t', 'doomed');
  await flush();
  // First dead-lettering: attempts=0 < 1, so a replay is armed at 30ms.
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // Under the old max of 1 the next dead-lettering would poison; raising
  // to 3 before the timer fires must rescue it.
  bus.setDeadLetterAutoReplay(sub.id, { maxAutoReplayAttempts: 3 });
  await sleep(150);
  // Replay 1 (attempt 2) and replay 2 (attempt 3) both failed and
  // re-dead-lettered without poisoning: attempts 1 and 2 are below 3.
  assert.equal(attempts, 3);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, false);
  assert.equal(entries[0].autoReplayAttempts, 2);
  assert.equal(poisonedEvents.length, 0);
  await sleep(150);
  // Replay 3 (attempt 4) failed and re-dead-lettered with attempts=3,
  // which finally reaches the new max.
  assert.equal(attempts, 4);
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].poisoned, true);
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].autoReplayAttempts, 3);
  assert.equal(poisonedEvents.length, 1);
  assert.equal(bus.getStats().autoReplayed, 3);
  assert.equal(bus.getStats().poisoned, 1);
});

test('setDeadLetterAutoReplay lowering maxAutoReplayAttempts poisons on the next dead-lettering', async () => {
  const bus = new EventBus();
  const poisonedEvents: PoisonedMessageEvent[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 30, maxAutoReplayAttempts: 3 },
      onPoisoned: (e) => poisonedEvents.push(e),
    },
  });
  bus.publish('t', 'doomed');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // Tighten the budget to 1 before the armed replay fires.
  bus.setDeadLetterAutoReplay(sub.id, { maxAutoReplayAttempts: 1 });
  await sleep(100);
  // The replay (attempt 2) failed and re-dead-lettered with attempts=1,
  // which reaches the new max — poisoned instead of re-armed.
  assert.equal(attempts, 2);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(entries[0].autoReplayAttempts, 1);
  assert.equal(poisonedEvents.length, 1);
  assert.equal(poisonedEvents[0].autoReplayAttempts, 1);
  assert.equal(bus.getStats().autoReplayed, 1);
  assert.equal(bus.getStats().poisoned, 1);
  // No further auto-replays.
  await sleep(80);
  await flush();
  assert.equal(attempts, 2);
});

test('setDeadLetterAutoReplay lowering maxAutoReplayAttempts to 0 poisons on the next dead-lettering', async () => {
  const bus = new EventBus();
  const poisonedEvents: PoisonedMessageEvent[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 30, maxAutoReplayAttempts: 3 },
      onPoisoned: (e) => poisonedEvents.push(e),
    },
  });
  bus.publish('t', 'doomed');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // A max of 0 means any re-dead-lettering poisons at once.
  bus.setDeadLetterAutoReplay(sub.id, { maxAutoReplayAttempts: 0 });
  await sleep(100);
  // The already-armed replay still fired once (attempt 2), failed, and
  // re-dead-lettered with attempts=1 >= 0 — poisoned.
  assert.equal(attempts, 2);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(entries[0].autoReplayAttempts, 1);
  assert.equal(poisonedEvents.length, 1);
  assert.equal(bus.getStats().poisoned, 1);
});

test('setDeadLetterAutoReplay delay fields reshape the next arming delay', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', (delivery) => {
    attempts += 1;
    // Fail twice, then ack: two dead-letterings, two replays.
    if (attempts <= 2) throw new Error(`fail #${attempts}`);
    delivery.ack();
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 200, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
    },
  });
  bus.publish('t', 'flaky');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // The first replay is already armed at the old 200ms. Retune the
  // initial delay now: the second arming must use 10 * 2^1 = 20ms,
  // not the old 200 * 2 = 400ms.
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 10 });
  await sleep(280);
  // Replay 1 fired at ~200ms and failed; replay 2 fired ~20ms later
  // and acked. Under the old delay replay 2 would not arrive until
  // ~600ms.
  assert.equal(attempts, 3);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0);
  assert.equal(bus.getStats().autoReplayed, 2);
});

test('setDeadLetterAutoReplay maxDelayMs caps the exponential backoff', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', (delivery) => {
    attempts += 1;
    if (attempts <= 2) throw new Error(`fail #${attempts}`);
    delivery.ack();
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 50, maxDelayMs: 60, backoffMultiplier: 10, maxAutoReplayAttempts: 3 },
    },
  });
  bus.publish('t', 'flaky');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  await sleep(200);
  // Replay 1 fired at ~50ms and failed; the second arming is
  // min(60, 50 * 10) = 60ms, so replay 2 fired at ~110ms and acked.
  // Without the cap it would arm at 500ms and arrive at ~550ms.
  assert.equal(attempts, 3);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0);
  assert.equal(bus.getStats().autoReplayed, 2);
});

test('setDeadLetterAutoReplay null clears auto-replay: new dead-letters neither arm nor poison', async () => {
  const bus = new EventBus();
  const poisonedEvents: PoisonedMessageEvent[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: { initialDelayMs: 10, maxAutoReplayAttempts: 3 },
      onPoisoned: (e) => poisonedEvents.push(e),
    },
  });
  bus.setDeadLetterAutoReplay(sub.id, null);
  bus.publish('t', 'x');
  await flush();
  assert.equal(attempts, 1);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].autoReplayAttempts, 0);
  assert.equal(entries[0].poisoned, false, 'cleared auto-replay never poisons');
  await sleep(80);
  await flush();
  assert.equal(attempts, 1, 'no auto-replay was armed');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(poisonedEvents.length, 0);
  assert.equal(bus.getStats().autoReplayed, 0);
  assert.equal(bus.getStats().poisoned, 0);
});

test('setDeadLetterAutoReplay null does not cancel an already-armed replay timer', async () => {
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
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // Clear after the 30ms replay was armed: the timer still fires once.
  bus.setDeadLetterAutoReplay(sub.id, null);
  await sleep(100);
  await flush();
  assert.equal(attempts, 2);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0);
  assert.equal(bus.getStats().autoReplayed, 1);
});

test('setDeadLetterAutoReplay after a null clear is refused until subscribe-time opt-in', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, autoReplay: true } },
  );
  bus.setDeadLetterAutoReplay(sub.id, null);
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 5 }), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, null), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    1,
    'only the clear itself was audited',
  );

  // Clearing also leaves the DLQ functional: a message still
  // dead-letters, just without auto-replay.
  bus.publish('t', 'x');
  await sleep(50);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].poisoned, false);
});

test('setDeadLetterAutoReplay same values are a no-op with no audit', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    {
      ackTimeoutMs: 1000,
      deadLetter: {
        maxRedeliveries: 0,
        autoReplay: { initialDelayMs: 100, maxDelayMs: 5000, backoffMultiplier: 3, maxAutoReplayAttempts: 2 },
      },
    },
  );
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 100, maxDelayMs: 5000, backoffMultiplier: 3, maxAutoReplayAttempts: 2 });
  bus.setDeadLetterAutoReplay(sub.id, {});
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 100 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    0,
  );
  // A real revision afterwards still audits exactly once.
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 101 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    1,
  );
});

test('setDeadLetterAutoReplay audit is exact and a null clear records null', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, autoReplay: true } },
  );
  // `true` enables the defaults: 1000 / 60000 / 2 / 3.
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 250 });
  let audit = bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    autoReplay: { initialDelayMs: 250, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
    oldAutoReplay: { initialDelayMs: 1000, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
  });

  bus.setDeadLetterAutoReplay(sub.id, null);
  audit = bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    autoReplay: null,
    oldAutoReplay: { initialDelayMs: 250, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
  });
});
