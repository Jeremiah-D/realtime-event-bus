import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BusMessage,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function poisonBus(opts: {
  requireApproval?: boolean;
  maxPending?: number;
} = {}) {
  const bus = new EventBus();
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
        ...(opts.requireApproval === undefined ? {} : { requireApproval: opts.requireApproval }),
        ...(opts.maxPending === undefined ? {} : { maxPending: opts.maxPending }),
      },
    },
  );
  const repairMsgs: BusMessage[] = [];
  bus.subscribe('repair', (msg) => {
    repairMsgs.push(msg);
  });
  return { bus, sub, seen, state, repairMsgs };
}

async function deadLetterOne(setup: ReturnType<typeof poisonBus>, payload: unknown) {
  const before = setup.bus.getDeadLetterMessages(setup.sub.id).length;
  setup.bus.publish('t', payload);
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, before + 1, `expected one new DLQ entry for ${String(payload)}`);
  return entries[entries.length - 1];
}

test('setDeadLetterApproval flipping ON parks the next replay', async () => {
  const setup = poisonBus();
  const entry = await deadLetterOne(setup, 'poison');
  assert.equal(setup.seen.length, 1);

  setup.bus.setDeadLetterApproval(setup.sub.id, { requireApproval: true });
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  assert.deepEqual(setup.bus.getDeadLetterMessages(setup.sub.id), [], 'entry left the DLQ');
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1, 'request parked');
  assert.equal(setup.seen.length, 1, 'parked replay is not redelivered yet');

  // Approval requeues with a fresh budget.
  setup.state.succeed = true;
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['poison', 'poison']);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 0);
});

test('setDeadLetterApproval flipping OFF leaves parked requests parked, new requests go straight through', async () => {
  const setup = poisonBus({ requireApproval: true });
  const first = await deadLetterOne(setup, 'a');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, first.seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);

  setup.bus.setDeadLetterApproval(setup.sub.id, { requireApproval: false });

  // The parked request is NOT auto-released: it still needs approval.
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);
  assert.equal(setup.seen.length, 1);
  setup.state.succeed = true;
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, first.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['a', 'a']);

  // A new request now goes straight through.
  setup.state.succeed = false;
  const second = await deadLetterOne(setup, 'b');
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, second.seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 0, 'nothing parked');
  await flush();
  assert.deepEqual(setup.seen, ['a', 'a', 'b', 'b']);
});

test('setDeadLetterApproval flipping OFF leaves a parked request rejectable', async () => {
  const setup = poisonBus({ requireApproval: true });
  const entry = await deadLetterOne(setup, 'a');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);

  setup.bus.setDeadLetterApproval(setup.sub.id, { requireApproval: false });
  assert.equal(setup.bus.rejectDeadLetterReplay(setup.sub.id, entry.seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 0);
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].seq, entry.seq, 'rejection returns the entry to the DLQ');
  assert.equal(setup.seen.length, 1, 'rejected replay is never redelivered');
});

test('setDeadLetterApproval tightening maxPending keeps parked requests and gates only new ones', async () => {
  const setup = poisonBus({ requireApproval: true, maxPending: 2 });
  const a = await deadLetterOne(setup, 'a');
  const b = await deadLetterOne(setup, 'b');
  const c = await deadLetterOne(setup, 'c');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, a.seq), true);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, b.seq), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 2);

  setup.bus.setDeadLetterApproval(setup.sub.id, { maxPending: 1 });
  assert.equal(
    setup.bus.getPendingDeadLetterReplays(setup.sub.id).length,
    2,
    'tightening evicts nothing already parked',
  );
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, c.seq), false, 'new request hits the new bound');
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].seq, c.seq, 'refused entry stays in the DLQ');
  const rejected = setup.bus.getSubscriptionAudit({ action: 'dead_letter_replay_rejected' });
  assert.equal(rejected.length, 1);
  assert.equal(rejected[0].detail?.reason, 'approval_queue_full');
});

test('setDeadLetterApproval loosening maxPending parks more', async () => {
  const setup = poisonBus({ requireApproval: true, maxPending: 1 });
  const a = await deadLetterOne(setup, 'a');
  const b = await deadLetterOne(setup, 'b');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, a.seq), true);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, b.seq), false, 'queue full at 1');

  setup.bus.setDeadLetterApproval(setup.sub.id, { maxPending: 2 });
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, b.seq), true, 'loosening adds capacity');
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 2);
});

test('setDeadLetterApproval rejects unknown, non-reliable, and DLQ-less subscribers', () => {
  const bus = new EventBus();
  const plain = bus.subscribe('plain', () => {});
  const noDlq = bus.subscribeReliable('nodlq', () => {}, { ackTimeoutMs: 1000 });
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );

  assert.throws(() => bus.setDeadLetterApproval('', { requireApproval: true }), RangeError);
  assert.throws(() => bus.setDeadLetterApproval('no-such-sub', { requireApproval: true }), RangeError);
  assert.throws(() => bus.setDeadLetterApproval(plain.id, { requireApproval: true }), RangeError);
  assert.throws(() => bus.setDeadLetterApproval(noDlq.id, { requireApproval: true }), RangeError);
  assert.throws(() => bus.setDeadLetterApproval(sub.id, null as never), RangeError);
  assert.throws(() => bus.setDeadLetterApproval(sub.id, 3 as never), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_approval_revised' }).length,
    0,
    'no failed revision is audited',
  );
  assert.deepEqual(
    { requireApproval: bus.getDeadLetterStats(sub.id).requireApproval, maxPending: bus.getDeadLetterStats(sub.id).maxPending },
    { requireApproval: false, maxPending: 100 },
    'live gate untouched by failed revisions',
  );
});

test('setDeadLetterApproval illegal values are fail-closed', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(),
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, requireApproval: true, maxPending: 2 } },
  );

  assert.throws(() => bus.setDeadLetterApproval(sub.id, { requireApproval: 'yes' as never }), TypeError);
  assert.throws(() => bus.setDeadLetterApproval(sub.id, { requireApproval: 1 as never }), TypeError);
  for (const bad of [{ maxPending: 0 }, { maxPending: -3 }, { maxPending: 1.5 }, { maxPending: Number.NaN }]) {
    assert.throws(() => bus.setDeadLetterApproval(sub.id, bad), RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_approval_revised' }).length,
    0,
    'no failed revision is audited',
  );
  assert.deepEqual(
    { requireApproval: bus.getDeadLetterStats(sub.id).requireApproval, maxPending: bus.getDeadLetterStats(sub.id).maxPending },
    { requireApproval: true, maxPending: 2 },
    'live gate untouched by invalid input',
  );
});

test('setDeadLetterApproval same values and empty patch are no-ops with no audit', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, requireApproval: true, maxPending: 5 } },
  );
  bus.setDeadLetterApproval(sub.id, { requireApproval: true, maxPending: 5 });
  bus.setDeadLetterApproval(sub.id, {});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_approval_revised' }).length,
    0,
  );
  // A real revision afterwards still audits exactly once.
  bus.setDeadLetterApproval(sub.id, { maxPending: 6 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'dead_letter_approval_revised' }).length,
    1,
  );
});

test('setDeadLetterApproval audit is exact and records construction defaults as previous', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  bus.setDeadLetterApproval(sub.id, { requireApproval: true, maxPending: 3 });
  const audit = bus.getSubscriptionAudit({ action: 'dead_letter_approval_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    requireApproval: true,
    oldRequireApproval: false,
    maxPending: 3,
    oldMaxPending: 100,
  });
});

test('setDeadLetterApproval partial revision leaves the other field alone', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0, requireApproval: true, maxPending: 5 } },
  );
  bus.setDeadLetterApproval(sub.id, { maxPending: 7 });
  let audit = bus.getSubscriptionAudit({ action: 'dead_letter_approval_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    requireApproval: true,
    oldRequireApproval: true,
    maxPending: 7,
    oldMaxPending: 5,
  });
  bus.setDeadLetterApproval(sub.id, { requireApproval: false });
  audit = bus.getSubscriptionAudit({ action: 'dead_letter_approval_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    requireApproval: false,
    oldRequireApproval: true,
    maxPending: 7,
    oldMaxPending: 7,
  });
});

test('setDeadLetterApproval retarget replay flipped ON parks with its target (EB-240)', async () => {
  const setup = poisonBus();
  const entry = await deadLetterOne(setup, 'poison');

  setup.bus.setDeadLetterApproval(setup.sub.id, { requireApproval: true });
  assert.equal(
    setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair' }),
    true,
  );
  assert.deepEqual(setup.repairMsgs, [], 'parked retarget does not publish yet');
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);

  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), true);
  await flush();
  assert.equal(setup.repairMsgs.length, 1);
  assert.equal(setup.repairMsgs[0].payload, 'poison');
  assert.equal(setup.repairMsgs[0].topic, 'repair');
  assert.deepEqual(setup.seen, ['poison'], 'approval published to the repair topic, not the faulty subscriber');
});

test('setDeadLetterApproval getDeadLetterStats follows the live gate', () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    () => {},
    { ackTimeoutMs: 1000, deadLetter: { maxRedeliveries: 0 } },
  );
  assert.deepEqual(
    { requireApproval: bus.getDeadLetterStats(sub.id).requireApproval, maxPending: bus.getDeadLetterStats(sub.id).maxPending },
    { requireApproval: false, maxPending: 100 },
  );
  bus.setDeadLetterApproval(sub.id, { requireApproval: true, maxPending: 7 });
  assert.deepEqual(
    { requireApproval: bus.getDeadLetterStats(sub.id).requireApproval, maxPending: bus.getDeadLetterStats(sub.id).maxPending },
    { requireApproval: true, maxPending: 7 },
  );
  bus.setDeadLetterApproval(sub.id, { maxPending: 9 });
  assert.deepEqual(
    { requireApproval: bus.getDeadLetterStats(sub.id).requireApproval, maxPending: bus.getDeadLetterStats(sub.id).maxPending },
    { requireApproval: true, maxPending: 9 },
  );
});
