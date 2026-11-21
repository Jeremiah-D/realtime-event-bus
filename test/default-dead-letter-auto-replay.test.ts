/**
 * Bus-level default dead-letter auto-replay live revision (EB-532).
 *
 * `setDefaultDeadLetterAutoReplay` decides how a new reliable
 * subscriber that opts into a dead-letter queue and omits `autoReplay`
 * heals itself — `true` to switch fleet-wide self-healing on with one
 * call, an object to tune the fleet backoff, `false` to switch it off,
 * `null` to clear the default back to closed. The priority chain is
 * explicit `autoReplay` > bus default > closed; an explicit value
 * always wins, including an explicit `false` under a `true` default.
 * The default never enables a dead-letter queue — a subscriber that
 * does not pass `deadLetter` still has none — and a revision only
 * affects reliable subscribers created after it: a default-baked
 * subscriber keeps the config it was baked with, an already-armed
 * timer keeps the delay it was armed with, and
 * `setDeadLetterAutoReplay` (EB-367) still revises one subscriber at
 * a time. Auto-replay still bypasses the approval gate (EB-233).
 * Audited as `default_dead_letter_auto_replay_revised`
 * (`{ previous, autoReplay }`, both the resolved four-field object,
 * `false`, or `null`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { AutoReplayOptions, PoisonedMessageEvent } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Sleeps for wall-clock ms (auto-replay backoff uses real timers). */
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const FULL_DEFAULTS = { initialDelayMs: 1000, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 };

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_dead_letter_auto_replay_revised' });

/** Poison subscriber: nacks until `succeed` flips, then acks. */
function poisonBus(
  opts: {
    defaultAutoReplay?: boolean | AutoReplayOptions | null;
    autoReplay?: boolean | AutoReplayOptions;
    defaultRequireApproval?: boolean | null;
  } = {},
) {
  const bus = new EventBus();
  if (opts.defaultRequireApproval !== undefined) {
    bus.setDefaultDeadLetterRequireApproval(opts.defaultRequireApproval);
  }
  if (opts.defaultAutoReplay !== undefined) {
    bus.setDefaultDeadLetterAutoReplay(opts.defaultAutoReplay);
  }
  const poisoned: PoisonedMessageEvent[] = [];
  const seen: unknown[] = [];
  const state = { succeed: false };
  let attempts = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      attempts += 1;
      seen.push(d.msg.payload);
      if (state.succeed) d.ack();
      else d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: {
        maxRedeliveries: 0,
        autoReplay: opts.autoReplay,
        onPoisoned: (e) => poisoned.push(e),
      },
    },
  );
  return { bus, sub, seen, state, poisoned, get attempts() { return attempts; } };
}

async function deadLetterOne(setup: ReturnType<typeof poisonBus>, payload: unknown) {
  setup.bus.publish('t', payload);
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  return entries[0];
}

test('EB-532: a bus default of true auto-replays a new subscriber that omits autoReplay, then poisons after 3 attempts', async () => {
  const setup = poisonBus({ defaultAutoReplay: true });
  assert.deepEqual(setup.bus.getDefaultDeadLetterAutoReplay(), FULL_DEFAULTS);
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(entry.poisoned, false);
  // First replay is armed at the default ~1000ms.
  await sleep(1400);
  await flush();
  assert.equal(setup.attempts, 2, 'first auto-replay fired at ~1000ms');
  assert.equal(setup.bus.getStats().autoReplayed, 1);
  // Replays 2 and 3 arm at ~2000ms and ~4000ms after the previous
  // dead-lettering; the third failure exhausts the budget of 3.
  await sleep(7000);
  await flush();
  assert.equal(setup.attempts, 4, 'three auto-replays, then poisoned');
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(entries[0].autoReplayAttempts, 3);
  assert.equal(setup.poisoned.length, 1);
  assert.equal(setup.poisoned[0].autoReplayAttempts, 3);
  assert.equal(setup.bus.getStats().autoReplayed, 3);
  assert.equal(setup.bus.getStats().autoReplayExhausted, 1);
  assert.equal(setup.bus.getStats().poisoned, 1);
  // No further auto-replays.
  await sleep(300);
  await flush();
  assert.equal(setup.attempts, 4);
});

test('EB-532: a default object tunes the backoff — one fast replay, then poison', async () => {
  const setup = poisonBus({ defaultAutoReplay: { initialDelayMs: 50, maxAutoReplayAttempts: 1 } });
  assert.deepEqual(setup.bus.getDefaultDeadLetterAutoReplay(), {
    initialDelayMs: 50,
    maxDelayMs: 60000,
    backoffMultiplier: 2,
    maxAutoReplayAttempts: 1,
  });
  await deadLetterOne(setup, 'doomed');
  await sleep(300);
  await flush();
  assert.equal(setup.attempts, 2, 'one fast replay at ~50ms');
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(entries[0].autoReplayAttempts, 1);
  assert.equal(setup.poisoned.length, 1);
  assert.equal(setup.bus.getStats().autoReplayed, 1);
  assert.equal(setup.bus.getStats().autoReplayExhausted, 1);
  await sleep(200);
  await flush();
  assert.equal(setup.attempts, 2, 'no further auto-replays after poison');
});

test('EB-532: an explicit autoReplay: false beats a true bus default', async () => {
  const setup = poisonBus({ defaultAutoReplay: true, autoReplay: false });
  await deadLetterOne(setup, 'poison');
  await sleep(400);
  await flush();
  assert.equal(setup.attempts, 1, 'explicit false switches this subscriber off');
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, false, 'a switched-off entry never poisons');
  assert.equal(setup.poisoned.length, 0);
  assert.equal(setup.bus.getStats().autoReplayed, 0);
  assert.equal(setup.bus.getStats().poisoned, 0);
});

test('EB-532: an explicit autoReplay object beats the bus default', async () => {
  const setup = poisonBus({
    defaultAutoReplay: true,
    autoReplay: { initialDelayMs: 40, maxAutoReplayAttempts: 1 },
  });
  await deadLetterOne(setup, 'doomed');
  // Under the default the first replay would not arrive until ~1000ms;
  // the explicit object replays at ~40ms and poisons after 1 attempt.
  await sleep(300);
  await flush();
  assert.equal(setup.attempts, 2);
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, true);
  assert.equal(setup.poisoned.length, 1);
  assert.equal(setup.bus.getStats().autoReplayed, 1);
});

test('EB-532: an explicit null autoReplay at subscribe time still throws TypeError', () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterAutoReplay(true);
  assert.throws(
    () =>
      bus.subscribeReliable(
        't',
        (d) => d.nack(),
        {
          ackTimeoutMs: 10_000,
          deadLetter: { maxRedeliveries: 0, autoReplay: null as never },
        },
      ),
    TypeError,
    'null is never a legal subscribe-time autoReplay — only an absent value falls back',
  );
  assert.equal(bus.getDefaultDeadLetterAutoReplay() !== undefined, true, 'live default untouched');
});

test('EB-532: the default never enables a dead-letter queue', async () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterAutoReplay(true);
  let failures = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      failures += 1;
      if (failures <= 1) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 1000 },
  );
  bus.publish('t', 'hiccup');
  await flush();
  await flush();
  assert.equal(failures, 2, 'immediate redelivery only — no DLQ was created');
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(bus.getStats().autoReplayed, 0);
});

test('EB-532: clearing the default returns new subscribers to no auto-replay', async () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterAutoReplay(true);
  bus.setDefaultDeadLetterAutoReplay(null);
  assert.equal(bus.getDefaultDeadLetterAutoReplay(), undefined);
  assert.deepEqual(defaultAudits(bus).map((a) => a.detail), [
    { previous: null, autoReplay: FULL_DEFAULTS },
    { previous: FULL_DEFAULTS, autoReplay: null },
  ]);
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 10_000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('t', 'poison');
  await flush();
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  await sleep(300);
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id)[0].poisoned, false);
  assert.equal(bus.getStats().autoReplayed, 0);
});

test('EB-532: illegal values throw TypeError/RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterAutoReplay(true);
  const illegal: unknown[] = ['yes', 1, undefined, () => {}];
  for (const value of illegal) {
    assert.throws(
      () => bus.setDefaultDeadLetterAutoReplay(value as boolean),
      TypeError,
      `setDefaultDeadLetterAutoReplay(${String(value)}) must throw`,
    );
    assert.deepEqual(bus.getDefaultDeadLetterAutoReplay(), FULL_DEFAULTS, 'live default untouched after a failed call');
  }
  const badFields: unknown[] = [
    { initialDelayMs: -1 },
    { initialDelayMs: Number.NaN },
    { initialDelayMs: Number.POSITIVE_INFINITY },
    { maxDelayMs: 0 },
    { maxDelayMs: -5 },
    { maxDelayMs: Number.NaN },
    { backoffMultiplier: 0.5 },
    { backoffMultiplier: Number.NaN },
    { maxAutoReplayAttempts: -1 },
    { maxAutoReplayAttempts: 1.5 },
    { maxAutoReplayAttempts: Number.NaN },
  ];
  for (const value of badFields) {
    assert.throws(
      () => bus.setDefaultDeadLetterAutoReplay(value as AutoReplayOptions),
      RangeError,
      `setDefaultDeadLetterAutoReplay(${JSON.stringify(value)}) must throw`,
    );
    assert.deepEqual(bus.getDefaultDeadLetterAutoReplay(), FULL_DEFAULTS, 'live default untouched after a failed call');
  }
  assert.equal(defaultAudits(bus).length, 1, 'failed calls audit nothing');
  // An illegal actor also throws with the live default untouched.
  assert.throws(() => bus.setDefaultDeadLetterAutoReplay(false, ''), RangeError);
  assert.deepEqual(bus.getDefaultDeadLetterAutoReplay(), FULL_DEFAULTS);
});

test('EB-532: a same-value revision is a silent no-op with no audit', () => {
  const bus = new EventBus();
  // Setting false on a factory bus is a no-op: the unset default
  // already behaves as closed.
  bus.setDefaultDeadLetterAutoReplay(false);
  assert.equal(bus.getDefaultDeadLetterAutoReplay(), undefined);
  // Clearing an already-unset default is a no-op.
  bus.setDefaultDeadLetterAutoReplay(null);
  assert.equal(defaultAudits(bus).length, 0);
  bus.setDefaultDeadLetterAutoReplay(true);
  assert.equal(defaultAudits(bus).length, 1);
  // Re-setting the same value audits nothing.
  bus.setDefaultDeadLetterAutoReplay(true);
  assert.equal(defaultAudits(bus).length, 1);
  // The full-value object is the same default as `true`.
  bus.setDefaultDeadLetterAutoReplay({ ...FULL_DEFAULTS });
  assert.equal(defaultAudits(bus).length, 1);
  bus.setDefaultDeadLetterAutoReplay({});
  assert.equal(defaultAudits(bus).length, 1, 'an empty object resolves to the same defaults');
  assert.deepEqual(bus.getDefaultDeadLetterAutoReplay(), FULL_DEFAULTS);
  // Switching off audits; re-setting false does not.
  bus.setDefaultDeadLetterAutoReplay(false);
  assert.equal(defaultAudits(bus).length, 2);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: FULL_DEFAULTS, autoReplay: false });
  bus.setDefaultDeadLetterAutoReplay(false);
  assert.equal(defaultAudits(bus).length, 2);
  assert.equal(bus.getDefaultDeadLetterAutoReplay(), false);
  // Clearing an installed `false` default still moves it back to
  // unset and audits.
  bus.setDefaultDeadLetterAutoReplay(null);
  assert.equal(defaultAudits(bus).length, 3);
  assert.deepEqual(defaultAudits(bus)[2].detail, { previous: false, autoReplay: null });
  bus.setDefaultDeadLetterAutoReplay(null);
  assert.equal(defaultAudits(bus).length, 3);
});

test('EB-532: a revision does not move an already-subscribed config or an already-armed timer', async () => {
  // Subscribed before any default: still closed after the default
  // flips on.
  const before = poisonBus({});
  before.bus.setDefaultDeadLetterAutoReplay(true);
  await deadLetterOne(before, 'poison');
  await sleep(400);
  await flush();
  assert.equal(before.attempts, 1, 'a pre-default subscriber keeps its baked closed config');
  assert.equal(before.bus.getDeadLetterMessages(before.sub.id).length, 1);
  assert.equal(before.bus.getDeadLetterMessages(before.sub.id)[0].poisoned, false);

  // A default-baked subscriber arms a replay; flipping the default
  // off leaves the baked config and the armed timer in place.
  const bus = new EventBus();
  bus.setDefaultDeadLetterAutoReplay(true);
  const seen: string[] = [];
  let attempts = 0;
  const sub = bus.subscribeReliable(
    'a',
    (d) => {
      attempts += 1;
      seen.push(d.msg.payload as string);
      d.nack();
    },
    { ackTimeoutMs: 10_000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('a', 'armed');
  await flush();
  await flush();
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  bus.setDefaultDeadLetterAutoReplay(false);
  await sleep(1400);
  await flush();
  assert.equal(attempts, 2, 'the already-armed replay still fired once at its armed delay');
  assert.deepEqual(seen, ['armed', 'armed']);
  // The baked config survives too: the re-dead-lettered message
  // re-arms under the baked defaults instead of staying closed.
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].poisoned, false);
  assert.equal(entries[0].autoReplayAttempts, 1);
  // A subscriber created after the flip takes the new default.
  let laterAttempts = 0;
  const later = bus.subscribeReliable(
    'b',
    (d) => {
      laterAttempts += 1;
      d.nack();
    },
    { ackTimeoutMs: 10_000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.publish('b', 'later');
  await flush();
  await flush();
  assert.equal(bus.getDeadLetterMessages(later.id).length, 1);
  await sleep(400);
  await flush();
  assert.equal(laterAttempts, 1, 'a post-flip subscriber takes the closed default');
});

test('EB-532: a default-baked subscriber can still be revised per subscriber', async () => {
  // Per-subscriber null clear: no more replays — but a fresh enable
  // after the clear is allowed (EB-535), and it only arms
  // dead-letterings after it.
  const cleared = poisonBus({ defaultAutoReplay: { initialDelayMs: 30, maxAutoReplayAttempts: 3 } });
  cleared.bus.setDeadLetterAutoReplay(cleared.sub.id, null);
  assert.equal(
    cleared.bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    1,
    'only the clear itself was audited',
  );
  cleared.bus.publish('t', 'x');
  await flush();
  await flush();
  assert.equal(cleared.bus.getDeadLetterMessages(cleared.sub.id).length, 1);
  await sleep(200);
  await flush();
  assert.equal(cleared.attempts, 1, 'no auto-replay was armed after the per-subscriber clear');
  assert.equal(cleared.bus.getDeadLetterMessages(cleared.sub.id)[0].poisoned, false);
  // `null` on the cleared subscriber is a silent no-op, not a refusal.
  cleared.bus.setDeadLetterAutoReplay(cleared.sub.id, null);
  assert.equal(
    cleared.bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' }).length,
    1,
    'null on a cleared subscriber audits nothing',
  );
  // Re-enable after the clear: audits with oldAutoReplay null, and the
  // entry already in the DLQ is not retro-armed.
  cleared.bus.setDeadLetterAutoReplay(cleared.sub.id, { initialDelayMs: 30, maxAutoReplayAttempts: 1 });
  let audit = cleared.bus.getSubscriptionAudit({ action: 'dead_letter_auto_replay_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    autoReplay: { initialDelayMs: 30, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 1 },
    oldAutoReplay: null,
  });
  await sleep(200);
  await flush();
  assert.equal(cleared.attempts, 1, 'an enable never retro-arms entries already in the DLQ');
  // A message dead-lettered after the enable auto-replays and poisons
  // once its single attempt is spent. The re-dead-lettering re-inserts
  // the entry with a fresh seq, so the second entry stays last.
  cleared.bus.publish('t', 'second');
  await flush();
  await flush();
  assert.equal(cleared.bus.getDeadLetterMessages(cleared.sub.id).length, 2);
  await sleep(200);
  await flush();
  assert.equal(cleared.attempts, 3, 'the publish plus the re-enabled auto-replay');
  const afterReplay = cleared.bus.getDeadLetterMessages(cleared.sub.id);
  assert.equal(afterReplay.length, 2);
  assert.equal(afterReplay[0].poisoned, false, 'the pre-enable entry was never retro-armed');
  assert.equal(afterReplay[1].poisoned, true);
  assert.equal(afterReplay[1].autoReplayAttempts, 1);
  assert.equal(cleared.poisoned.length, 1);

  // Per-subscriber partial revision still works on a default-baked
  // subscriber: tightening the budget to 1 poisons after one replay.
  const revised = poisonBus({ defaultAutoReplay: { initialDelayMs: 30, maxAutoReplayAttempts: 3 } });
  revised.bus.setDeadLetterAutoReplay(revised.sub.id, { maxAutoReplayAttempts: 1 });
  revised.bus.publish('t', 'doomed');
  await flush();
  await flush();
  assert.equal(revised.bus.getDeadLetterMessages(revised.sub.id).length, 1);
  await sleep(200);
  await flush();
  assert.equal(revised.attempts, 2);
  assert.equal(revised.bus.getDeadLetterMessages(revised.sub.id)[0].poisoned, true);
  assert.equal(revised.poisoned.length, 1);
  // A later bus-default revision never re-applies to the
  // already-revised subscriber.
  revised.bus.setDefaultDeadLetterAutoReplay({ initialDelayMs: 5000, maxAutoReplayAttempts: 3 });
  revised.bus.publish('t', 'second');
  await flush();
  await flush();
  assert.equal(revised.bus.getDeadLetterMessages(revised.sub.id).length, 2);
  await sleep(300);
  await flush();
  assert.equal(revised.attempts, 4, 'the revised subscriber still replays at its own 30ms delay');
});

test('EB-532: a default-enabled auto-replay still bypasses the approval gate', async () => {
  const setup = poisonBus({
    defaultRequireApproval: true,
    defaultAutoReplay: { initialDelayMs: 50, maxAutoReplayAttempts: 1 },
  });
  // The gate is baked on, yet the system-initiated replay never parks.
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).requireApproval, true);
  await deadLetterOne(setup, 'doomed');
  await sleep(300);
  await flush();
  assert.equal(setup.attempts, 2, 'the auto-replay fired without approval');
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id)[0].poisoned, true);
  assert.equal(setup.poisoned.length, 1);
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), [], 'nothing parked');
  assert.equal(setup.bus.getStats().autoReplayed, 1);
});

test('EB-532: audit previous/autoReplay are exact and the getter follows the live value', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDeadLetterAutoReplay(), undefined);
  bus.setDefaultDeadLetterAutoReplay(true, 'ops');
  assert.deepEqual(bus.getDefaultDeadLetterAutoReplay(), FULL_DEFAULTS);
  bus.setDefaultDeadLetterAutoReplay({ initialDelayMs: 50 });
  assert.deepEqual(bus.getDefaultDeadLetterAutoReplay(), {
    initialDelayMs: 50,
    maxDelayMs: 60000,
    backoffMultiplier: 2,
    maxAutoReplayAttempts: 3,
  });
  // The getter returns a copy: mutating it never moves the live default.
  const copy = bus.getDefaultDeadLetterAutoReplay() as { initialDelayMs: number };
  copy.initialDelayMs = 999;
  assert.deepEqual(bus.getDefaultDeadLetterAutoReplay(), {
    initialDelayMs: 50,
    maxDelayMs: 60000,
    backoffMultiplier: 2,
    maxAutoReplayAttempts: 3,
  });
  bus.setDefaultDeadLetterAutoReplay(false);
  assert.equal(bus.getDefaultDeadLetterAutoReplay(), false);
  bus.setDefaultDeadLetterAutoReplay(null);
  assert.equal(bus.getDefaultDeadLetterAutoReplay(), undefined);
  const audits = defaultAudits(bus);
  assert.equal(audits.length, 4);
  assert.deepEqual(audits[0].detail, { previous: null, autoReplay: FULL_DEFAULTS });
  assert.equal(audits[0].actor, 'ops');
  assert.deepEqual(audits[1].detail, {
    previous: FULL_DEFAULTS,
    autoReplay: { initialDelayMs: 50, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
  });
  assert.deepEqual(audits[2].detail, {
    previous: { initialDelayMs: 50, maxDelayMs: 60000, backoffMultiplier: 2, maxAutoReplayAttempts: 3 },
    autoReplay: false,
  });
  assert.deepEqual(audits[3].detail, { previous: false, autoReplay: null });
});
