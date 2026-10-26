import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Poison subscriber: always nacks until `succeed` flips, then acks. */
function poisonBus(opts: { requireApproval?: boolean; maxPending?: number; maxAgeMs?: number; now?: () => number } = {}) {
  const bus = new EventBus(opts.now === undefined ? undefined : { now: opts.now });
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
        maxAgeMs: opts.maxAgeMs,
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

test('EB-233: replay under requireApproval parks the entry instead of delivering it', async () => {
  const setup = poisonBus({ requireApproval: true });
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  await flush();
  // Not delivered, no longer in the DLQ, parked in the approval queue.
  assert.deepEqual(setup.seen, ['poison']);
  assert.deepEqual(setup.bus.getDeadLetterMessages(setup.sub.id), []);
  const pending = setup.bus.getPendingDeadLetterReplays(setup.sub.id);
  assert.equal(pending.length, 1);
  assert.equal(pending[0].seq, entry.seq);
  assert.deepEqual(pending[0].payload, 'poison');
  assert.deepEqual(setup.bus.getDeadLetterStats(setup.sub.id), {
    entries: 0,
    pendingApprovals: 1,
    requireApproval: true,
    maxPending: 100,
    replayRateLimited: 0,
  })
  const audit = setup.bus.getSubscriptionAudit({ action: 'dead_letter_replay_requested' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].detail?.seq, entry.seq);
  // A second request for an already-pending entry finds nothing in the DLQ.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), false);
});

test('EB-233: approve requeues with a fresh budget (per-message semantics unchanged)', async () => {
  const setup = poisonBus({ requireApproval: true });
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  setup.state.succeed = true;
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['poison', 'poison']);
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);
  assert.deepEqual(setup.bus.getDeadLetterMessages(setup.sub.id), []);
  const audit = setup.bus.getSubscriptionAudit({ action: 'dead_letter_replay_approved' });
  assert.equal(audit.length, 1);
  // Approving again is a no-op: nothing pending with that seq.
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), false);
});

test('EB-233: reject returns the entry to the DLQ, still dead-lettered, and it can be requested again', async () => {
  const setup = poisonBus({ requireApproval: true });
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  assert.equal(setup.bus.rejectDeadLetterReplay(setup.sub.id, entry.seq), true);
  await flush();
  // No delivery, entry back in the DLQ in arrival order.
  assert.deepEqual(setup.seen, ['poison']);
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].seq, entry.seq);
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);
  const audit = setup.bus.getSubscriptionAudit({ action: 'dead_letter_replay_rejected' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].detail?.reason, 'operator_rejected');
  // Rejecting again is a no-op; re-requesting works.
  assert.equal(setup.bus.rejectDeadLetterReplay(setup.sub.id, entry.seq), false);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).pendingApprovals, 1);
});

test('EB-233: a full approval queue refuses the new request and leaves the entry in the DLQ', async () => {
  const setup = poisonBus({ requireApproval: true, maxPending: 1 });
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
  })
});

test('EB-233: without requireApproval replay still delivers immediately (backward compatible)', async () => {
  const setup = poisonBus({});
  const entry = await deadLetterOne(setup, 'poison');
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['poison', 'poison']);
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), false);
  assert.equal(setup.bus.rejectDeadLetterReplay(setup.sub.id, entry.seq), false);
  assert.deepEqual(setup.bus.getDeadLetterStats(setup.sub.id), {
    entries: 0,
    pendingApprovals: 0,
    requireApproval: false,
    maxPending: 100,
    replayRateLimited: 0,
  })
});

test('EB-233: a pending approval ages out under maxAgeMs and can no longer be approved', async () => {
  const clock = controllableClock();
  const setup = poisonBus({ requireApproval: true, maxAgeMs: 100, now: clock.now });
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  clock.nowMs += 100; // exactly at the TTL boundary: expired (deadLetteredAt + maxAgeMs <= now)
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), false);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).pendingApprovals, 0);
  assert.equal(setup.bus.getStats().dlqExpiredMessages, 1);
});

test('EB-233: invalid approval options throw before subscribing, and unknown subscribers throw', async () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { requireApproval: 'yes' as unknown as boolean },
      }),
    TypeError,
  );
  assert.throws(
    () =>
      bus.subscribeReliable('t', () => {}, {
        deadLetter: { requireApproval: true, maxPending: 0 },
      }),
    RangeError,
  );
  assert.throws(() => bus.approveDeadLetterReplay('nope', 1), /unknown subscriber/);
  assert.throws(() => bus.rejectDeadLetterReplay('nope', 1), /unknown subscriber/);
  assert.throws(() => bus.getDeadLetterStats('nope'), /unknown subscriber/);
  assert.throws(() => bus.getPendingDeadLetterReplays('nope'), /unknown subscriber/);
  // No DLQ at all: stats are zeros, approve/reject are false, not throws.
  const plain = bus.subscribe('t', () => {});
  assert.deepEqual(bus.getDeadLetterStats(plain.id), {
    entries: 0,
    pendingApprovals: 0,
    requireApproval: false,
    maxPending: 100,
    replayRateLimited: 0,
  })
  assert.equal(bus.approveDeadLetterReplay(plain.id, 1), false);
  assert.equal(bus.rejectDeadLetterReplay(plain.id, 1), false);
});
