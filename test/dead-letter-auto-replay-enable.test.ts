/**
 * Per-subscriber dead-letter auto-replay enable (EB-535).
 *
 * `setDeadLetterAutoReplay` on a subscriber whose `deadLetter` never
 * enabled `autoReplay` — or whose auto-replay was cleared — enables it
 * instead of refusing: `true` resolves to the subscribe-time factory
 * defaults (`{ 1000, 60000, 2, 3 }`), an object resolves its missing
 * fields against the same defaults, and `null` on a never-enabled
 * subscriber is a silent no-op. An enable only arms dead-letterings
 * after it — entries already in the DLQ are never retro-armed, never
 * resurrected, and still replay manually via `replayDeadLetter`.
 * Auto-replay still bypasses the approval gate (EB-233). Audited as
 * `dead_letter_auto_replay_revised` (`{ autoReplay, oldAutoReplay }`,
 * `null` on the old side meaning "auto-replay freshly enabled").
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { PoisonedMessageEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Sleeps for wall-clock ms (auto-replay backoff uses real timers). */
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const FULL_DEFAULTS = { initialDelayMs: 1000, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 };

const revisionAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' });

test('EB-535: true enables with the factory defaults — three replays at ~1000/~2000/~4000ms, then poisoned', async () => {
  const bus = new EventBus();
  const poisonedEvents: PoisonedMessageEvent[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      onPoisoned: (e) => poisonedEvents.push(e),
    },
  });

  bus.setDeadLetterAutoReplay(sub.id, true);
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { autoReplay: FULL_DEFAULTS, oldAutoReplay: null });

  bus.publish('t', 'doomed');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // First replay is armed at the factory ~1000ms.
  await sleep(1400);
  await flush();
  assert.equal(attempts, 2, 'first auto-replay fired at ~1000ms');
  assert.equal(bus.getStats().autoReplayed, 1);
  // Replays 2 and 3 arm at ~2000ms and ~4000ms after the previous
  // dead-lettering; the third failure exhausts the budget of 3.
  await sleep(7000);
  await flush();
  assert.equal(attempts, 4, 'three auto-replays, then poisoned');
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(entries[0].autoReplayAttempts, 3);
  assert.equal(poisonedEvents.length, 1);
  assert.equal(poisonedEvents[0].autoReplayAttempts, 3);
  assert.equal(bus.getStats().autoReplayed, 3);
  assert.equal(bus.getStats().poisoned, 1);
  // No further auto-replays.
  await sleep(300);
  await flush();
  assert.equal(attempts, 4);
});

test('EB-535: an object patch enables with one fast replay, then poison', async () => {
  const bus = new EventBus();
  const poisonedEvents: PoisonedMessageEvent[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      onPoisoned: (e) => poisonedEvents.push(e),
    },
  });

  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 50, maxAutoReplayAttempts: 1 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    autoReplay: { initialDelayMs: 50, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 1 },
    oldAutoReplay: null,
  });

  bus.publish('t', 'doomed');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  await sleep(200);
  await flush();
  assert.equal(attempts, 2, 'the single fast replay fired');
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(entries[0].autoReplayAttempts, 1);
  assert.equal(poisonedEvents.length, 1);
  assert.equal(bus.getStats().autoReplayed, 1);
  assert.equal(bus.getStats().poisoned, 1);
  await sleep(150);
  await flush();
  assert.equal(attempts, 2, 'no further auto-replays after poisoning');
});

test('EB-535: a partial object enable resolves missing fields against the factory defaults', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: { maxRedeliveries: 0 },
  });

  // Only the budget is given: the delays still take the factory
  // defaults (1000 / 60000 / 2).
  bus.setDeadLetterAutoReplay(sub.id, { maxAutoReplayAttempts: 1 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    autoReplay: { initialDelayMs: 1000, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 1 },
    oldAutoReplay: null,
  });

  bus.publish('t', 'doomed');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  await sleep(400);
  await flush();
  assert.equal(attempts, 1, 'no replay before the factory ~1000ms delay');
  await sleep(1000);
  await flush();
  assert.equal(attempts, 2, 'the replay fired at the factory delay');
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].poisoned, true);
});

test('EB-535: an enable never retro-arms entries already in the DLQ, which still replay manually', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', (delivery) => {
    attempts += 1;
    if (attempts === 2) delivery.ack();
    else throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: { maxRedeliveries: 0 },
  });

  bus.publish('t', 'x');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  // Enable after the entry is already dead-lettered.
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 50, maxAutoReplayAttempts: 3 });
  await sleep(200);
  await flush();
  assert.equal(attempts, 1, 'the already-dead-lettered entry was not retro-armed');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].poisoned, false);

  // Manual replay still works on it: the entry is delivered again
  // and acked, clearing the DLQ entry.
  const seq = bus.getDeadLetterMessages(sub.id)[0].seq;
  assert.equal(bus.replayDeadLetter(sub.id, seq), true);
  await flush();
  assert.equal(attempts, 2, 'the manual replay delivered the entry');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 0, 'the manual replay acked and cleared the entry');
});

test('EB-535: null on a never-enabled subscriber is a silent no-op and leaves it without auto-replay', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: { maxRedeliveries: 0 },
  });

  bus.setDeadLetterAutoReplay(sub.id, null);
  assert.equal(revisionAudits(bus).length, 0, 'null on a never-enabled subscriber audits nothing');

  bus.publish('t', 'x');
  await flush();
  assert.equal(attempts, 1);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  await sleep(150);
  await flush();
  assert.equal(attempts, 1, 'no auto-replay was armed');
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].poisoned, false);
  assert.equal(bus.getStats().autoReplayed, 0);
});

test('EB-535: a null clear can be followed by a fresh enable with true', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', () => {
    attempts += 1;
    throw new Error(`fail #${attempts}`);
  }, {
    deadLetter: {
      maxRedeliveries: 0,
      autoReplay: true,
    },
  });

  bus.setDeadLetterAutoReplay(sub.id, null);
  bus.setDeadLetterAutoReplay(sub.id, true);
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, { autoReplay: FULL_DEFAULTS, oldAutoReplay: null });

  bus.publish('t', 'x');
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  await sleep(1400);
  await flush();
  assert.equal(attempts, 2, 'the re-enabled auto-replay fired at the factory delay');
});

test('EB-535: unknown, non-reliable and DLQ-less subscribers still throw on enable', () => {
  const bus = new EventBus();
  const plain = bus.subscribe('plain', () => {});
  const noDlq = bus.subscribeReliable('nodlq', () => {}, { ackTimeoutMs: 1000 });

  assert.throws(() => bus.setDeadLetterAutoReplay('', true), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay('no-such-sub', true), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay('no-such-sub', { initialDelayMs: 5 }), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(plain.id, true), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(noDlq.id, true), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(noDlq.id, { initialDelayMs: 5 }), RangeError);
  assert.equal(revisionAudits(bus).length, 0, 'no failed enable is audited');
});

test('EB-535: illegal enable values and non-object patches are fail-closed', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );

  for (const bad of [{ initialDelayMs: -1 }, { initialDelayMs: Number.NaN }, { initialDelayMs: Number.POSITIVE_INFINITY }]) {
    assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, bad), RangeError);
  }
  for (const bad of [{ maxDelayMs: 0 }, { maxDelayMs: -5 }, { maxDelayMs: Number.NaN }]) {
    assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, bad), RangeError);
  }
  for (const bad of [{ backoffMultiplier: 0.5 }, { backoffMultiplier: Number.NaN }]) {
    assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, bad), RangeError);
  }
  for (const bad of [{ maxAutoReplayAttempts: -1 }, { maxAutoReplayAttempts: 1.5 }, { maxAutoReplayAttempts: Number.NaN }]) {
    assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, bad), RangeError);
  }
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, undefined as never), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, 3 as never), RangeError);
  assert.throws(() => bus.setDeadLetterAutoReplay(sub.id, 'x' as never), RangeError);
  assert.equal(revisionAudits(bus).length, 0, 'no failed enable is audited');

  // The live config is untouched: a valid enable afterwards still
  // audits with oldAutoReplay null.
  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 30 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    autoReplay: { initialDelayMs: 30, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
    oldAutoReplay: null,
  });
});

test('EB-535: a partial revision after an enable still merges the live values', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );

  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 50, maxDelayMs: 5000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 });
  bus.setDeadLetterAutoReplay(sub.id, { maxDelayMs: 9000 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    autoReplay: { initialDelayMs: 50, maxDelayMs: 9000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
    oldAutoReplay: { initialDelayMs: 50, maxDelayMs: 5000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
  });
});

test('EB-535: true on an already-enabled subscriber resets to the factory defaults, then is a no-op', () => {
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

  bus.setDeadLetterAutoReplay(sub.id, true);
  let audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    autoReplay: FULL_DEFAULTS,
    oldAutoReplay: { initialDelayMs: 100, maxDelayMs: 5000, backoffMultiplier: 3, maxAutoReplayAttempts: 2 },
  });

  // Re-affirming the factory values — via `true` or the full-value
  // object — is a no-op with no audit.
  bus.setDeadLetterAutoReplay(sub.id, true);
  bus.setDeadLetterAutoReplay(sub.id, { ...FULL_DEFAULTS });
  audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
});

test('EB-535: an enabled auto-replay still bypasses the approval gate', async () => {
  const bus = new EventBus();
  let attempts = 0;
  const sub = bus.subscribeReliable('t', (d) => {
    attempts += 1;
    d.nack();
  }, {
    ackTimeoutMs: 10_000,
    deadLetter: { maxRedeliveries: 0, requireApproval: true },
  });

  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 30, maxAutoReplayAttempts: 1 });
  assert.equal(bus.getDeadLetterStats(sub.id).requireApproval, true);
  bus.publish('t', 'doomed');
  await flush();
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  await sleep(200);
  await flush();
  assert.equal(attempts, 2, 'the auto-replay fired without approval');
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].poisoned, true);
  assert.deepEqual(bus.getPendingDeadLetterReplays(sub.id), [], 'nothing parked');
  assert.equal(bus.getStats().autoReplayed, 1);
});

test('EB-535: the enable audit is exact — actor system, oldAutoReplay null', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );

  bus.setDeadLetterAutoReplay(sub.id, { initialDelayMs: 250, maxDelayMs: 9000 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    autoReplay: { initialDelayMs: 250, maxDelayMs: 9000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
    oldAutoReplay: null,
  });
});
