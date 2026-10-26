import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type BusMessage,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function poisonBus(opts: {
  requireApproval?: boolean;
  now?: () => number;
  trace?: boolean;
} = {}) {
  const bus = new EventBus({
    ...(opts.now === undefined ? {} : { now: opts.now }),
    ...(opts.trace === true ? { trace: { sampleRate: 1 } } : {}),
  });
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
  const repairMsgs: BusMessage[] = [];
  bus.subscribe('repair', (msg) => {
    repairMsgs.push(msg);
  });
  const tMsgs: BusMessage[] = [];
  bus.subscribe('t', (msg) => {
    tMsgs.push(msg);
  });
  return { bus, sub, seen, state, repairMsgs, tMsgs };
}

async function deadLetterOne(setup: ReturnType<typeof poisonBus>, payload: unknown, opts: { messageId?: string; key?: string; traceparent?: string } = {}) {
  setup.bus.publish('t', payload, {
    ...(opts.messageId === undefined ? {} : { messageId: opts.messageId }),
    ...(opts.key === undefined ? {} : { key: opts.key }),
    ...(opts.traceparent === undefined ? {} : { traceparent: opts.traceparent }),
  });
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 1);
  return entries[0];
}

test('EB-240: retarget republishes to the repair topic, not to the faulty subscriber', async () => {
  const setup = poisonBus();
  const entry = await deadLetterOne(setup, 'poison', { messageId: 'm-1', key: 'k-1' });
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair' }), true);
  await flush();
  // The faulty subscriber never sees a second delivery.
  assert.deepEqual(setup.seen, ['poison']);
  // The repair subscriber gets the payload as a fresh publish: the
  // repair topic's own seq (1), the original identity, and the origin
  // topic carried as lineage.
  assert.equal(setup.repairMsgs.length, 1);
  const [msg] = setup.repairMsgs;
  assert.equal(msg.payload, 'poison');
  assert.equal(msg.topic, 'repair');
  assert.equal(msg.seq, 1);
  assert.equal(msg.messageId, 'm-1');
  assert.equal(msg.key, 'k-1');
  assert.equal(msg.lineage?.originTopic, 't');
  assert.deepEqual(setup.bus.getDeadLetterMessages(setup.sub.id), []);
  // The retarget consumed the repair topic's seq, not the original's:
  // the next publish to 't' still carries seq 2.
  setup.bus.publish('t', 'next');
  await flush();
  assert.equal(setup.tMsgs[setup.tMsgs.length - 1]?.seq, 2);
});

test('EB-240: retarget preserves the end-to-end trace id', async () => {
  const setup = poisonBus({ trace: true });
  const upstreamTraceId = '4bf92f3577b34da6a3ce929d0e0e4736';
  const entry = await deadLetterOne(setup, 'traced', {
    traceparent: `00-${upstreamTraceId}-00f067aa0ba902b7-01`,
  });
  assert.equal(entry.traceId, upstreamTraceId);
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair' }), true);
  await flush();
  assert.equal(setup.repairMsgs.length, 1);
  const repairSpans = setup.bus.getStats().traceSpans.filter(
    (s) => s.name === 'bus.publish' && s.attrs.topic === 'repair',
  );
  assert.equal(repairSpans.length, 1);
  assert.equal(repairSpans[0]?.traceId, upstreamTraceId);
});

test('EB-240: without targetTopic the original requeue semantics are unchanged', async () => {
  const setup = poisonBus();
  const entry = await deadLetterOne(setup, 'again');
  setup.state.succeed = true;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.seen, ['again', 'again']);
  assert.deepEqual(setup.repairMsgs, []);
});

test('EB-240: an invalid targetTopic throws before anything is mutated', async () => {
  const setup = poisonBus();
  const entry = await deadLetterOne(setup, 'held');
  assert.throws(
    () => setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: '' }),
    RangeError,
  );
  assert.throws(
    () =>
      setup.bus.replayDeadLetter(setup.sub.id, entry.seq, {
        targetTopic: 42 as unknown as string,
      }),
    RangeError,
  );
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id).length, 1);
  // The entry is still retargetable after the refused calls.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair' }), true);
  await flush();
  assert.equal(setup.repairMsgs.length, 1);
});

test('EB-240: unknown seqs and DLQ-less subscribers still return false', async () => {
  const setup = poisonBus();
  const entry = await deadLetterOne(setup, 'x');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, 999, { targetTopic: 'repair' }), false);
  const plain = setup.bus.subscribeReliable('other', (d) => d.ack(), { ackTimeoutMs: 1000 });
  assert.equal(setup.bus.replayDeadLetter(plain.id, 1, { targetTopic: 'repair' }), false);
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id).length, 1);
  assert.equal(entry.seq, 1);
});

test('EB-240: a refused repair-topic admission restores the entry, never drops it', async () => {
  const setup = poisonBus();
  setup.bus.setTopicSchema('repair', () => false);
  const entry = await deadLetterOne(setup, 'blocked');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair' }), false);
  await flush();
  assert.deepEqual(setup.repairMsgs, []);
  const remaining = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(remaining.length, 1);
  assert.equal(remaining[0].seq, entry.seq);
  // A different, admissible repair topic still works for the same entry.
  const repair2Msgs: BusMessage[] = [];
  setup.bus.subscribe('repair2', (msg) => {
    repair2Msgs.push(msg);
  });
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair2' }), true);
  await flush();
  assert.equal(repair2Msgs.length, 1);
  assert.equal(repair2Msgs[0]?.payload, 'blocked');
});

test('EB-240: under approval a retarget parks with its target and only approval publishes it', async () => {
  const setup = poisonBus({ requireApproval: true });
  const entry = await deadLetterOne(setup, 'parked');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair' }), true);
  await flush();
  assert.deepEqual(setup.repairMsgs, []);
  assert.deepEqual(setup.seen, ['parked']);
  const parked = setup.bus.getPendingDeadLetterReplays(setup.sub.id);
  assert.equal(parked.length, 1);
  assert.equal(parked[0]?.seq, entry.seq);
  // The internal retarget field never leaks into the public snapshot.
  assert.equal('replayTargetTopic' in (parked[0] as object), false);
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), true);
  await flush();
  assert.equal(setup.repairMsgs.length, 1);
  assert.equal(setup.repairMsgs[0]?.payload, 'parked');
  assert.deepEqual(setup.bus.getPendingDeadLetterReplays(setup.sub.id), []);
  // The faulty subscriber never saw the retargeted delivery.
  assert.deepEqual(setup.seen, ['parked']);
});

test('EB-240: rejecting a parked retarget returns a clean entry to the DLQ', async () => {
  const setup = poisonBus({ requireApproval: true });
  const entry = await deadLetterOne(setup, 'rejected');
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair' }), true);
  assert.equal(setup.bus.rejectDeadLetterReplay(setup.sub.id, entry.seq), true);
  await flush();
  assert.deepEqual(setup.repairMsgs, []);
  const back = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(back.length, 1);
  assert.equal(back[0]?.seq, entry.seq);
  assert.equal('replayTargetTopic' in (back[0] as object), false);
  // And the same entry can be retargeted again afterwards.
  // Under approval a direct call parks again, not publishes.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entry.seq, { targetTopic: 'repair' }), true);
  assert.equal(setup.bus.getPendingDeadLetterReplays(setup.sub.id).length, 1);
  assert.equal(setup.bus.approveDeadLetterReplay(setup.sub.id, entry.seq), true);
  await flush();
  assert.equal(setup.repairMsgs.length, 1);
});

test('EB-240: retargets are still gated by the replay rate limit (EB-238)', async () => {
  const clock = controllableClock();
  const setup = poisonBus({ now: clock.now });
  setup.bus.publish('t', 'one');
  setup.bus.publish('t', 'two');
  await flush();
  await flush();
  const entries = setup.bus.getDeadLetterMessages(setup.sub.id);
  assert.equal(entries.length, 2);
  setup.bus.setDeadLetterReplayRateLimit(setup.sub.id, { messagesPerSec: 1, burst: 1 });
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[0].seq, { targetTopic: 'repair' }), true);
  await flush();
  assert.equal(setup.repairMsgs.length, 1);
  // Over budget: the second entry stays in the DLQ, counted, not dropped.
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[1].seq, { targetTopic: 'repair' }), false);
  assert.equal(setup.bus.getDeadLetterMessages(setup.sub.id).length, 1);
  assert.equal(setup.bus.getDeadLetterStats(setup.sub.id).replayRateLimited, 1);
  clock.nowMs += 1000;
  assert.equal(setup.bus.replayDeadLetter(setup.sub.id, entries[1].seq, { targetTopic: 'repair' }), true);
  await flush();
  assert.equal(setup.repairMsgs.length, 2);
  assert.deepEqual(setup.repairMsgs.map((m) => m.payload), ['one', 'two']);
  assert.deepEqual(setup.repairMsgs.map((m) => m.seq), [1, 2]);
});
