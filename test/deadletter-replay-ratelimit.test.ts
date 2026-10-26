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
function poisonBus(opts: { requireApproval?: boolean; now?: () => number } = {}) {
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
      },
    },
  );
  return { bus, sub, seen, state };
}

async function deadLetterMany(setup: ReturnType<typeof poisonBus>, payloads: unknown[]) {
  for (const p of payloads) setup.bus.publish('t', p);
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, payloads.length);
  return entries;
}

test('EB-238: an over-budget direct replay defers the entry in place and counts it', async () => {
  const clock = controllableClock();
  const setup = poisonBus({ now: clock.now });
  const entries = await deadLetterMany(setup, ['one', 'two']);
  setup.bus.setDeadLetterReplayRateLimit(setup.sub.id, { messagesPerSec: 1, burst: 1 });
  setup.state.succeed = true;
  // First replay takes the single burst token.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[0].seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['one', 'two', 'one']);
  // Second replay is over budget: deferred, not dropped, not jumping ahead.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[1].seq), false);
  const remaining = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(remaining.length, 1);
  assert.equal(remaining[0].seq, entries[1].seq);
  assert.deepEqual(setup.bus.getDeadLetterStats(setup.sub.id), {
    entries: 1,
    pendingApprovals: 0,
    requireApproval: false,
    maxPending: 100,
    replayRateLimited: 1,
  });
  // Retrying inside the same window defers again and counts again.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[1].seq), false);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).replayRateLimited, 2);
  // The next window refills the bucket: the deferred entry replays.
  clock.nowMs += 1000;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[1].seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['one', 'two', 'one', 'two']);
  assert.deepEqual(setup.bus.getDeadLetterMessages(setup.sub.id), []);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).replayRateLimited, 2);
});

test('EB-238: re-setting the limit replaces the bucket with a fresh burst', async () => {
  const clock = controllableClock();
  const setup = poisonBus({ now: clock.now });
  const entries = await deadLetterMany(setup, ['one']);
  setup.bus.setDeadLetterReplayRateLimit(setup.sub.id, { messagesPerSec: 1, burst: 1 });
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[0].seq), true);
  await flush();
  // Dead-letter it again: nack the replayed delivery by flipping back.
  setup.state.succeed = false;
  setup.bus.publish('t', 'one');
  await flush();
  await flush();
  const again = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(again.length, 1);
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, again[0].seq), false);
  // A fresh limit means a fresh burst: the deferred entry replays now.
  setup.bus.setDeadLetterReplayRateLimit(setup.sub.id, { messagesPerSec: 1, burst: 1 });
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, again[0].seq), true);
  await flush();
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id).length, 0);
});

test('EB-238: approved replays are still rate-limited; parking itself is not', async () => {
  const clock = controllableClock();
  const setup = poisonBus({ requireApproval: true, now: clock.now });
  const entries = await deadLetterMany(setup, ['one', 'two']);
  // Parking is not a requeue, so it is not gated.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[0].seq), true);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[1].seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 2);
  setup.bus.setDeadLetterReplayRateLimit(setup.sub.id, { messagesPerSec: 1, burst: 1 });
  setup.state.succeed = true;
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entries[0].seq), true);
  await flush();
  // Second approval is over budget: stays parked in request order.
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entries[1].seq), false);
  const parked = setup.bus.getPendingDeadLetterReplays(setup.sub.id);
  assert.equal(parked.length, 1);
  assert.equal(parked[0].seq, entries[1].seq);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).replayRateLimited, 1);
  // Next window: the parked approval requeues.
  clock.nowMs += 1000;
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entries[1].seq), true);
  await flush();
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);
  assert.deepEqual(setup.seen, ['one', 'two', 'one', 'two']);
});

test('EB-238: rejecting a parked replay is never rate-limited', async () => {
  const clock = controllableClock();
  const setup = poisonBus({ requireApproval: true, now: clock.now });
  const entries = await deadLetterMany(setup, ['one', 'two']);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[0].seq), true);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[1].seq), true);
  setup.bus.setDeadLetterReplayRateLimit(setup.sub.id, { messagesPerSec: 1, burst: 1 });
  setup.state.succeed = true;
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entries[0].seq), true);
  await flush();
  // The token is spent, but rejection is not a requeue: it succeeds and
  // returns the entry to the DLQ, counting nothing.
  assert.equal(setup.bus.rejectDeadLetterReplay(setup.sub.id, entries[1].seq), true);
  const back = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(back.length, 1);
  assert.equal(back[0].seq, entries[1].seq);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).replayRateLimited, 0);
});

test('EB-238: invalid limits, unknown subscribers, and DLQ-less subscribers throw', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable('t', () => {}, { deadLetter: { maxRedeliveries: 0 } });
  assert.throws(
    () => bus.setDeadLetterReplayRateLimit(sub.id, { messagesPerSec: 0, burst: 1 }),
    RangeError,
  );
  assert.throws(
    () => bus.setDeadLetterReplayRateLimit(sub.id, { messagesPerSec: -1, burst: 1 }),
    RangeError,
  );
  assert.throws(
    () => bus.setDeadLetterReplayRateLimit(sub.id, { messagesPerSec: Number.NaN, burst: 1 }),
    RangeError,
  );
  assert.throws(
    () => bus.setDeadLetterReplayRateLimit(sub.id, { messagesPerSec: 1, burst: 0 }),
    RangeError,
  );
  assert.throws(
    () => bus.setDeadLetterReplayRateLimit(sub.id, { messagesPerSec: 1, burst: Number.POSITIVE_INFINITY }),
    RangeError,
  );
  assert.throws(() => bus.setDeadLetterReplayRateLimit('nope', { messagesPerSec: 1, burst: 1 }), /unknown subscriber/);
  const plain = bus.subscribe('t', () => {});
  assert.throws(
    () => bus.setDeadLetterReplayRateLimit(plain.id, { messagesPerSec: 1, burst: 1 }),
    /no dead-letter queue/,
  );
  // A valid limit on a DLQ subscriber is accepted and starts unlimited-adjacent:
  // the burst is full, so the first replay is unaffected.
  bus.setDeadLetterReplayRateLimit(sub.id, { messagesPerSec: 1, burst: 1 });
  assert.equal(bus.getDeadLetterStats(sub.id).replayRateLimited, 0);
});
