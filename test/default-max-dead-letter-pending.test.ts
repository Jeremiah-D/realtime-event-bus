/**
 * Bus-level default dead-letter replay-approval pending-queue capacity
 * live revision (EB-529).
 *
 * `setDefaultMaxDeadLetterPending` decides how many replay requests
 * may park in the approval queue of a new reliable subscriber that
 * opts into a dead-letter queue and omits `maxPending` — `1` to give
 * every new approval-gated subscriber a one-request queue fleet-wide
 * with one call, `null` to clear the default back to 100. The
 * priority chain is explicit `maxPending` > bus default > 100; an
 * explicit capacity always wins. The default never enables a
 * dead-letter queue — a subscriber that does not pass `deadLetter`
 * still has none — and it never enables the approval gate
 * (`requireApproval` still defaults to `false`), so the capacity is
 * only meaningful when a subscriber explicitly opens the gate. A
 * revision only affects reliable subscribers created after it,
 * tightening the default never evicts an existing subscriber's
 * parked requests (rejection still happens only on the next replay
 * request), and a revision identical to the current default
 * (compared on the effective capacity, including setting `100` on a
 * factory bus) is a no-op with no audit. Audited as
 * `default_max_dead_letter_pending_revised`
 * (`{ previous, maxPending }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_max_dead_letter_pending_revised' });

/** Poison subscriber: always nacks until `succeed` flips, then acks. */
function poisonBus(opts: { requireApproval?: boolean; maxPending?: number | null; defaultMaxPending?: number | null } = {}) {
  const bus = new EventBus();
  if (opts.defaultMaxPending !== undefined) {
    bus.setDefaultMaxDeadLetterPending(opts.defaultMaxPending);
  }
  const seen: unknown[] = [];
  const state = { succeed: false };
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      seen.push(d.msg.payload);
      if (state.succeed) d.ack();
      else d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: {
        maxRedeliveries: 0,
        requireApproval: opts.requireApproval,
        maxPending: opts.maxPending,
      },
    },
  );
  return { bus, sub, seen, state };
}

async function deadLetterOne(setup: ReturnType<typeof poisonBus>, payload: unknown) {
  setup.bus.publish('t', payload);
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  return entries[0];
}

test('EB-529: a bus default capacity of 1 parks the first replay request and refuses the second', async () => {
  const setup = poisonBus({ requireApproval: true, defaultMaxPending: 1 });
  assert.equal(setup.bus.getDefaultMaxDeadLetterPending(), 1);
  const first = await deadLetterOne(setup, 'first');
  setup.bus.publish('t', 'second');
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 2);
  const second = entries.find((e) => e.payload === 'second')!;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, first.seq), true);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, second.seq), false);
  // The refused entry stayed in the DLQ; the accepted one is pending.
  assert.deepEqual(
    setup.bus.getDeadLetterMessages(setup.sub.id).map((e) => e.seq),
    [second.seq],
  );
  assert.deepEqual(
    setup.bus.getPendingDeadLetterReplays(setup.sub.id).map((e) => e.seq),
    [first.seq],
  );
  const audit = setup.bus.getSubscriptionAudit({ action: 'dead_letter_replay_rejected' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].detail?.reason, 'approval_queue_full');
  assert.deepEqual(setup.bus.getDeadLetterStats(setup.sub.id), {
    entries: 1,
    pendingApprovals: 1,
    requireApproval: true,
    maxPending: 1,
    replayRateLimited: 0,
  });
});

test('EB-529: an explicit maxPending always beats the bus default', async () => {
  const setup = poisonBus({ requireApproval: true, defaultMaxPending: 1, maxPending: 3 });
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).maxPending, 3);
  // An explicit `null` keeps the subscribe-time gate's own behavior
  // (capacity 100) — it is not swallowed into the bus default.
  const bus2 = new EventBus();
  bus2.setDefaultMaxDeadLetterPending(1);
  const sub2 = bus2.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: { maxRedeliveries: 0, requireApproval: true, maxPending: null },
    },
  );
  assert.equal(bus2.getDeadLetterStats(sub2.id).maxPending, 100);
});

test('EB-529: with requireApproval off the default has no effect — replay goes straight through', async () => {
  const setup = poisonBus({ defaultMaxPending: 1 });
  const entry = await deadLetterOne(setup, 'poison');
  // The default still baked in (stats show it), but the gate is off.
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).maxPending, 1);
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['poison', 'poison']);
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).pendingApprovals, 0);
});

test('EB-529: the default never enables a dead-letter queue', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterPending(1);
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
});

test('EB-529: clearing the default returns new subscribers to a capacity of 100', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterPending(1);
  bus.setDefaultMaxDeadLetterPending(null);
  assert.equal(bus.getDefaultMaxDeadLetterPending(), undefined);
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: { maxRedeliveries: 0, requireApproval: true },
    },
  );
  assert.equal(bus.getDeadLetterStats(sub.id).maxPending, 100);
  assert.deepEqual(defaultAudits(bus).map((a) => a.detail), [
    { previous: null, maxPending: 1 },
    { previous: 1, maxPending: null },
  ]);
});

test('EB-529: illegal values throw RangeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterPending(7);
  const illegal: unknown[] = [0, -1, 1.5, Number.NaN, Infinity, -Infinity, '3', {}, [], true];
  for (const value of illegal) {
    assert.throws(
      () => bus.setDefaultMaxDeadLetterPending(value as number),
      RangeError,
      `setDefaultMaxDeadLetterPending(${String(value)}) must throw`,
    );
    assert.equal(bus.getDefaultMaxDeadLetterPending(), 7, 'live default untouched after a failed call');
  }
  assert.equal(defaultAudits(bus).length, 1, 'failed calls audit nothing');
  // An illegal actor also throws with the live default untouched.
  assert.throws(() => bus.setDefaultMaxDeadLetterPending(3, ''), RangeError);
  assert.equal(bus.getDefaultMaxDeadLetterPending(), 7);
});

test('EB-529: a same-value revision is a silent no-op with no audit', () => {
  const bus = new EventBus();
  // Setting 100 on a factory bus is a no-op: the unset default
  // already behaves as 100.
  bus.setDefaultMaxDeadLetterPending(100);
  assert.equal(bus.getDefaultMaxDeadLetterPending(), undefined);
  // Clearing an already-unset default is a no-op.
  bus.setDefaultMaxDeadLetterPending(null);
  assert.equal(defaultAudits(bus).length, 0);
  bus.setDefaultMaxDeadLetterPending(2);
  assert.equal(defaultAudits(bus).length, 1);
  // Re-setting the same capacity audits nothing.
  bus.setDefaultMaxDeadLetterPending(2);
  assert.equal(defaultAudits(bus).length, 1);
  assert.equal(bus.getDefaultMaxDeadLetterPending(), 2);
  // Clearing an installed default audits; clearing again does not.
  bus.setDefaultMaxDeadLetterPending(null);
  assert.equal(defaultAudits(bus).length, 2);
  bus.setDefaultMaxDeadLetterPending(null);
  assert.equal(defaultAudits(bus).length, 2);
});

test('EB-529: a revision does not move an already-subscribed subscriber capacity or its parked requests', async () => {
  const setup = poisonBus({ requireApproval: true, defaultMaxPending: 1 });
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);
  // Tighten the bus default: the existing subscriber keeps capacity 1
  // and its parked request.
  setup.bus.setDefaultMaxDeadLetterPending(5);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).maxPending, 1);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);
  // A subscriber created after the revision takes the new default.
  const later = setup.bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: { maxRedeliveries: 0, requireApproval: true },
    },
  );
  assert.equal(setup.bus.getDeadLetterStats(later.id).maxPending, 5);
});

test('EB-529: a default-baked subscriber can still be revised per subscriber, and tightening keeps parked requests', async () => {
  const setup = poisonBus({ requireApproval: true, defaultMaxPending: 2 });
  const first = await deadLetterOne(setup, 'first');
  setup.bus.publish('t', 'second');
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  const second = entries.find((e) => e.payload === 'second')!;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, first.seq), true);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, second.seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 2);
  // Tighten this subscriber to 1: the parked requests stay.
  setup.bus.setDeadLetterApproval(setup.sub.id, { maxPending: 1 });
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).maxPending, 1);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 2);
  // The next replay request is refused — the queue is over the new bound.
  setup.bus.publish('t', 'third');
  await flush();
  await flush();
  const third = setup.bus.getDeadLetterMessages(setup.sub.id).find((e) => e.payload === 'third')!;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, third.seq), false);
  assert.deepEqual(
    setup.bus.getDeadLetterMessages(setup.sub.id).map((e) => e.seq),
    [third.seq],
  );
  // A later bus-default revision never re-applies to the already-revised
  // subscriber.
  setup.bus.setDefaultMaxDeadLetterPending(5);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).maxPending, 1);
});

test('EB-529: audit previous/maxPending are exact and the getter follows the live value', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxDeadLetterPending(), undefined);
  bus.setDefaultMaxDeadLetterPending(2, 'ops');
  assert.equal(bus.getDefaultMaxDeadLetterPending(), 2);
  bus.setDefaultMaxDeadLetterPending(7);
  assert.equal(bus.getDefaultMaxDeadLetterPending(), 7);
  bus.setDefaultMaxDeadLetterPending(null);
  assert.equal(bus.getDefaultMaxDeadLetterPending(), undefined);
  const audits = defaultAudits(bus);
  assert.equal(audits.length, 3);
  assert.deepEqual(audits[0].detail, { previous: null, maxPending: 2 });
  assert.equal(audits[0].actor, 'ops');
  assert.deepEqual(audits[1].detail, { previous: 2, maxPending: 7 });
  assert.deepEqual(audits[2].detail, { previous: 7, maxPending: null });
});
