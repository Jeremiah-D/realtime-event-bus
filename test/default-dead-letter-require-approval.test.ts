/**
 * Bus-level default dead-letter replay approval gate live revision
 * (EB-531).
 *
 * `setDefaultDeadLetterRequireApproval` decides whether a new
 * reliable subscriber that opts into a dead-letter queue and omits
 * `requireApproval` parks its replay requests for approval — `true`
 * to open the gate fleet-wide with one call, `null` to clear the
 * default back to straight-through replays. The priority chain is
 * explicit `requireApproval` > bus default > `false`; an explicit
 * value always wins, including an explicit `false` under a `true`
 * default. The default never enables a dead-letter queue — a
 * subscriber that does not pass `deadLetter` still has none — and
 * a revision only affects reliable subscribers created after it:
 * flipping the default off never auto-releases an existing
 * subscriber's parked requests, and a default-baked subscriber can
 * still be flipped per subscriber with `setDeadLetterApproval`
 * (EB-377). It composes with the EB-529 default pending-queue
 * capacity: a default-gated subscriber that omits `maxPending`
 * still takes the EB-529 default capacity. A revision identical to
 * the current default (compared on the effective gate, including
 * setting `false` on a factory bus) is a no-op with no audit.
 * Audited as `default_dead_letter_require_approval_revised`
 * (`{ previous, requireApproval }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_dead_letter_require_approval_revised' });

/** Poison subscriber: always nacks until `succeed` flips, then acks. */
function poisonBus(
  opts: {
    requireApproval?: boolean | null;
    maxPending?: number;
    defaultRequireApproval?: boolean | null;
    defaultMaxPending?: number | null;
  } = {},
) {
  const bus = new EventBus();
  if (opts.defaultRequireApproval !== undefined) {
    bus.setDefaultDeadLetterRequireApproval(opts.defaultRequireApproval);
  }
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

test('EB-531: a bus default of true parks the replay of a new subscriber that omits requireApproval', async () => {
  const setup = poisonBus({ defaultRequireApproval: true });
  assert.equal(setup.bus.getDefaultDeadLetterRequireApproval(), true);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).requireApproval, true);
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  await flush();
  // Not delivered, no longer in the DLQ, parked in the approval queue.
  assert.deepEqual(setup.seen, ['poison']);
  assert.deepEqual(setup.bus.getDeadLetterMessages(setup.sub.id), []);
  const pending = setup.bus.getPendingDeadLetterReplays(setup.sub.id);
  assert.equal(pending.length, 1);
  assert.equal(pending[0].seq, entry.seq);
  // Approval requeues with a fresh budget.
  setup.state.succeed = true;
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['poison', 'poison']);
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);
});

test('EB-531: an explicit requireApproval always beats the bus default', async () => {
  // Explicit false under a true default still replays straight through.
  const setup = poisonBus({ defaultRequireApproval: true, requireApproval: false });
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).requireApproval, false);
  const entry = await deadLetterOne(setup, 'poison');
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['poison', 'poison']);
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);
  // An explicit `null` at subscribe time keeps the subscribe-time
  // gate's own behavior (`?? false`) — it is not swallowed into the
  // bus default.
  const bus2 = new EventBus();
  bus2.setDefaultDeadLetterRequireApproval(true);
  const sub2 = bus2.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: { maxRedeliveries: 0, requireApproval: null },
    },
  );
  assert.equal(bus2.getDeadLetterStats(sub2.id).requireApproval, false);
});

test('EB-531: an explicit true parks with no bus default installed', async () => {
  const setup = poisonBus({ requireApproval: true });
  assert.equal(setup.bus.getDefaultDeadLetterRequireApproval(), undefined);
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).requireApproval, true);
});

test('EB-531: the default never enables a dead-letter queue', async () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterRequireApproval(true);
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
  assert.equal(bus.getDeadLetterStats(sub.id).requireApproval, false);
});

test('EB-531: clearing the default returns new subscribers to straight-through replays', async () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterRequireApproval(true);
  bus.setDefaultDeadLetterRequireApproval(null);
  assert.equal(bus.getDefaultDeadLetterRequireApproval(), undefined);
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: { maxRedeliveries: 0 },
    },
  );
  assert.equal(bus.getDeadLetterStats(sub.id).requireApproval, false);
  assert.deepEqual(defaultAudits(bus).map((a) => a.detail), [
    { previous: null, requireApproval: true },
    { previous: true, requireApproval: null },
  ]);
});

test('EB-531: illegal values throw TypeError and leave the live default untouched', () => {
  const bus = new EventBus();
  bus.setDefaultDeadLetterRequireApproval(true);
  const illegal: unknown[] = ['yes', 'true', 1, 0, {}, [], undefined];
  for (const value of illegal) {
    assert.throws(
      () => bus.setDefaultDeadLetterRequireApproval(value as boolean),
      TypeError,
      `setDefaultDeadLetterRequireApproval(${String(value)}) must throw`,
    );
    assert.equal(bus.getDefaultDeadLetterRequireApproval(), true, 'live default untouched after a failed call');
  }
  assert.equal(defaultAudits(bus).length, 1, 'failed calls audit nothing');
  // An illegal actor also throws with the live default untouched.
  assert.throws(() => bus.setDefaultDeadLetterRequireApproval(false, ''), RangeError);
  assert.equal(bus.getDefaultDeadLetterRequireApproval(), true);
});

test('EB-531: a same-value revision is a silent no-op with no audit', () => {
  const bus = new EventBus();
  // Setting false on a factory bus is a no-op: the unset default
  // already behaves as false.
  bus.setDefaultDeadLetterRequireApproval(false);
  assert.equal(bus.getDefaultDeadLetterRequireApproval(), undefined);
  // Clearing an already-unset default is a no-op.
  bus.setDefaultDeadLetterRequireApproval(null);
  assert.equal(defaultAudits(bus).length, 0);
  bus.setDefaultDeadLetterRequireApproval(true);
  assert.equal(defaultAudits(bus).length, 1);
  // Re-setting the same gate audits nothing.
  bus.setDefaultDeadLetterRequireApproval(true);
  assert.equal(defaultAudits(bus).length, 1);
  assert.equal(bus.getDefaultDeadLetterRequireApproval(), true);
  // Clearing an installed default audits; clearing again does not.
  bus.setDefaultDeadLetterRequireApproval(null);
  assert.equal(defaultAudits(bus).length, 2);
  bus.setDefaultDeadLetterRequireApproval(null);
  assert.equal(defaultAudits(bus).length, 2);
  // Clearing a bus that holds an explicit false still moves it back
  // to unset and audits (the default was installed, even as false —
  // it got there via true -> false, since setting false on an
  // unset bus is itself a no-op).
  bus.setDefaultDeadLetterRequireApproval(true);
  assert.equal(defaultAudits(bus).length, 3);
  bus.setDefaultDeadLetterRequireApproval(false);
  assert.equal(defaultAudits(bus).length, 4);
  assert.deepEqual(defaultAudits(bus)[3].detail, { previous: true, requireApproval: false });
  bus.setDefaultDeadLetterRequireApproval(null);
  assert.equal(defaultAudits(bus).length, 5);
  assert.deepEqual(defaultAudits(bus)[4].detail, { previous: false, requireApproval: null });
});

test('EB-531: a revision does not move an already-subscribed gate or its parked requests', async () => {
  // Subscribed before any default: still replays straight through
  // after the default flips on.
  const setup = poisonBus({});
  setup.bus.setDefaultDeadLetterRequireApproval(true);
  const entry = await deadLetterOne(setup, 'poison');
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['poison', 'poison']);
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);

  // A default-baked subscriber parks its request; flipping the
  // default off leaves the gate and the parked request in place.
  const gated = poisonBus({ defaultRequireApproval: true });
  const parked = await deadLetterOne(gated, 'parked');
  assert.equal(gated.bus.replayDeadLetter(gated.sub.id, parked.seq), true);
  assert.equal(gated.bus.getPendingDeadLetterReplays(gated.sub.id).length, 1);
  gated.bus.setDefaultDeadLetterRequireApproval(false);
  assert.equal(gated.bus.getDeadLetterStats(gated.sub.id).requireApproval, true);
  assert.equal(gated.bus.getPendingDeadLetterReplays(gated.sub.id).length, 1);
  // The parked request still needs approval — it is not auto-released.
  gated.state.succeed = true;
  assert.equal(gated.bus.approveDeadLetterReplay(gated.sub.id, parked.seq), true);
  await flush();
  assert.deepEqual(gated.seen, ['parked', 'parked']);
  // A subscriber created after the flip takes the new default.
  const later = gated.bus.subscribeReliable(
    't',
    (d) => {
      d.nack();
    },
    {
      ackTimeoutMs: 10_000,
      deadLetter: { maxRedeliveries: 0 },
    },
  );
  assert.equal(gated.bus.getDeadLetterStats(later.id).requireApproval, false);
});

test('EB-531: a default-baked subscriber can still be flipped per subscriber, and parked requests still need approval', async () => {
  const setup = poisonBus({ defaultRequireApproval: true });
  const first = await deadLetterOne(setup, 'first');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, first.seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);
  // Flip this subscriber off: the parked request stays parked.
  setup.bus.setDeadLetterApproval(setup.sub.id, { requireApproval: false });
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).requireApproval, false);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);
  // A new replay request now goes straight through.
  setup.bus.publish('t', 'second');
  await flush();
  await flush();
  const second = setup.bus.getDeadLetterMessages(setup.sub.id).find((e) => e.payload === 'second')!;
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, second.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['first', 'second', 'second']);
  // The parked request still needs an explicit decision.
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, first.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['first', 'second', 'second', 'first']);
  // A later bus-default revision never re-applies to the
  // already-revised subscriber.
  setup.bus.setDefaultDeadLetterRequireApproval(true);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).requireApproval, false);
});

test('EB-531: the gate default stacks with the EB-529 capacity default', async () => {
  const setup = poisonBus({ defaultRequireApproval: true, defaultMaxPending: 1 });
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).requireApproval, true);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).maxPending, 1);
  const first = await deadLetterOne(setup, 'first');
  setup.bus.publish('t', 'second');
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 2);
  const second = entries.find((e) => e.payload === 'second')!;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, first.seq), true);
  // The second request is refused: the default capacity is 1.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, second.seq), false);
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
});

test('EB-531: audit previous/requireApproval are exact and the getter follows the live value', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDeadLetterRequireApproval(), undefined);
  bus.setDefaultDeadLetterRequireApproval(true, 'ops');
  assert.equal(bus.getDefaultDeadLetterRequireApproval(), true);
  bus.setDefaultDeadLetterRequireApproval(false);
  assert.equal(bus.getDefaultDeadLetterRequireApproval(), false);
  bus.setDefaultDeadLetterRequireApproval(null);
  assert.equal(bus.getDefaultDeadLetterRequireApproval(), undefined);
  const audits = defaultAudits(bus);
  assert.equal(audits.length, 3);
  assert.deepEqual(audits[0].detail, { previous: null, requireApproval: true });
  assert.equal(audits[0].actor, 'ops');
  assert.deepEqual(audits[1].detail, { previous: true, requireApproval: false });
  assert.deepEqual(audits[2].detail, { previous: false, requireApproval: null });
});
