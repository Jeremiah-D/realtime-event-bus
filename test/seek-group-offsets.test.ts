import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import type { Delivery } from '../src/ack.ts';
import type { OffsetStore, OffsetStoreEntry } from '../src/offsetstore.ts';

/** Minimal store whose `entries()` actually yields its contents (the
 * default in-memory store's `entries()` returns `[]` by design). */
class RecordingStore implements OffsetStore {
  private readonly offsets = new Map<string, Map<string, number>>();
  get(groupId: string, topic: string): number | undefined {
    return this.offsets.get(groupId)?.get(topic);
  }
  set(groupId: string, topic: string, seq: number): void {
    let byTopic = this.offsets.get(groupId);
    if (byTopic == null) {
      byTopic = new Map();
      this.offsets.set(groupId, byTopic);
    }
    byTopic.set(topic, seq);
  }
  entries(): OffsetStoreEntry[] {
    const out: OffsetStoreEntry[] = [];
    for (const [groupId, byTopic] of this.offsets) {
      for (const [topic, seq] of byTopic) out.push({ groupId, topic, seq });
    }
    return out;
  }
}

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Deterministic bus clock: the log's `at` stamps ride on it. */
function makeClock(start = 1_000_000) {
  let now = start;
  return {
    now: () => now,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

function makeBus(clock: { now: () => number }, withLog: boolean, store?: RecordingStore) {
  const dir = withLog ? mkdtempSync(join(tmpdir(), 'seek-group-test-')) : undefined;
  return {
    bus: new EventBus({
      now: clock.now,
      ...(dir === undefined ? {} : { durableLogDir: dir }),
      ...(store === undefined ? {} : { offsetStore: store }),
    }),
    dir,
  };
}

/** Members join before anything is published, so the EB-207 join-time replay stays out of the way. */
async function publish(bus: EventBus, clock: { advance: (ms: number) => void }, topic: string, n: number) {
  for (let i = 0; i < n; i++) {
    clock.advance(10);
    bus.publish(topic, { i });
  }
  await flush();
}

test('rewind moves the checkpoint and replays from the durable log', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 'orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  await publish(bus, clock, 'orders', 5);
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  bus.commitOffset('g', 'orders', 5);

  const result = bus.seekGroupOffsets('g', 'orders', { orders: 2 }, { actor: 'ops' });
  assert.equal(result.groupId, 'g');
  assert.equal(result.pattern, 'orders');
  assert.equal(result.members, 1);
  assert.equal(result.topics.length, 1);
  assert.deepEqual(result.topics[0], { topic: 'orders', from: 5, requested: 2, position: 2, replayed: 3 });
  assert.deepEqual(bus.getCommittedOffsets('g'), { orders: 2 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 3, 4, 5]);

  const audit = bus.getSubscriptionAudit({ action: 'group_offsets_seeked' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].groupId, 'g');
  assert.equal(audit[0].pattern, 'orders');
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail?.targets, { orders: 2 });
  assert.deepEqual(audit[0].detail?.previous, { orders: 5 });
});

test('fast-forward only moves the checkpoint, nothing replays', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 'orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  await publish(bus, clock, 'orders', 5);
  bus.commitOffset('g', 'orders', 2);

  const result = bus.seekGroupOffsets('g', 'orders', { orders: 5 });
  assert.deepEqual(result.topics[0], { topic: 'orders', from: 2, requested: 5, position: 5, replayed: 0 });
  assert.deepEqual(bus.getCommittedOffsets('g'), { orders: 5 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  bus.publish('orders', { i: 5 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 6]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test("'earliest' replays from the log start and clears the checkpoint", async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 'orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  await publish(bus, clock, 'orders', 3);
  bus.commitOffset('g', 'orders', 3);

  const result = bus.seekGroupOffsets('g', 'orders', { orders: 'earliest' });
  assert.deepEqual(result.topics[0], { topic: 'orders', from: 3, requested: 0, position: 0, replayed: 3 });
  assert.deepEqual(bus.getCommittedOffsets('g'), {});
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 1, 2, 3]);
  const audit = bus.getSubscriptionAudit({ action: 'group_offsets_seeked' });
  assert.deepEqual(audit[0].detail?.previous, { orders: 3 });
  assert.deepEqual(audit[0].detail?.targets, { orders: 'earliest' });
});

test("'latest' fast-forwards to the live head with no replay", async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 'orders', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  await publish(bus, clock, 'orders', 5);
  bus.commitOffset('g', 'orders', 2);

  const result = bus.seekGroupOffsets('g', 'orders', { orders: 'latest' });
  assert.deepEqual(result.topics[0], { topic: 'orders', from: 2, requested: 5, position: 5, replayed: 0 });
  assert.deepEqual(bus.getCommittedOffsets('g'), { orders: 5 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  bus.publish('orders', { i: 5 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 6]);

  // A 'latest' never regresses a checkpoint an operator already committed
  // past the live head (a manual over-commit).
  bus.commitOffset('g', 'orders', 10);
  const clamped = bus.seekGroupOffsets('g', 'orders', { orders: 'latest' });
  assert.deepEqual(clamped.topics[0], { topic: 'orders', from: 10, requested: 10, position: 10, replayed: 0 });
  assert.deepEqual(bus.getCommittedOffsets('g'), { orders: 10 });
});

test('unknown group refuses, including a groupId known under another pattern', () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  bus.subscribeToGroup('g', 'a', () => {});
  assert.throws(() => bus.seekGroupOffsets('nope', 't', { t: 1 }), RangeError);
  assert.throws(() => bus.seekGroupOffsets('g', 'b', { b: 1 }), RangeError);
  assert.throws(() => bus.seekGroupOffsets('', 't', { t: 1 }), RangeError);
  assert.throws(() => bus.seekGroupOffsets('g', '', { t: 1 }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_offsets_seeked' }).length, 0);
});

test('illegal seq targets refuse with RangeError and mutate nothing', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 't', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  await publish(bus, clock, 't', 3);
  bus.commitOffset('g', 't', 3);
  for (const bad of [0, -1, 1.5, Number.NaN, 'x', null]) {
    assert.throws(() => bus.seekGroupOffsets('g', 't', { t: bad as never }), RangeError);
  }
  assert.throws(() => bus.seekGroupOffsets('g', 't', null as never), RangeError);
  assert.throws(() => bus.seekGroupOffsets('g', 't', [{ t: 1 }] as never), RangeError);
  assert.throws(() => bus.seekGroupOffsets('g', 't', { t: 1 }, { actor: '' }), RangeError);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });
  assert.equal(bus.getSubscriptionAudit({ action: 'group_offsets_seeked' }).length, 0);
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
});

test('without a durable log a rewind refuses but a fast-forward moves the checkpoint', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, false);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 't', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  await publish(bus, clock, 't', 3);
  bus.commitOffset('g', 't', 3);

  assert.throws(() => bus.seekGroupOffsets('g', 't', { t: 1 }), RangeError);
  assert.throws(() => bus.seekGroupOffsets('g', 't', { t: 'earliest' }), RangeError);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });
  assert.equal(bus.getSubscriptionAudit({ action: 'group_offsets_seeked' }).length, 0);

  const result = bus.seekGroupOffsets('g', 't', { t: 'latest' });
  assert.deepEqual(result.topics[0], { topic: 't', from: 3, requested: 3, position: 3, replayed: 0 });
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });
  bus.publish('t', { i: 3 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4]);

  // With nothing committed, 'earliest' is a no-op position, not a rewind.
  const fresh = new EventBus({ now: clock.now });
  fresh.subscribeToGroup('h', 'u', () => {});
  const noop = fresh.seekGroupOffsets('h', 'u', { u: 'earliest' });
  assert.deepEqual(noop.topics[0], { topic: 'u', from: 0, requested: 0, position: 0, replayed: 0 });
});

test('checkpoint, journal, and offset-store mirror follow the move', async () => {
  const clock = makeClock();
  const store = new RecordingStore();
  const { bus, dir } = makeBus(clock, true, store);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 't', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  await publish(bus, clock, 't', 5);
  bus.commitOffset('g', 't', 5);
  assert.equal(store.get('g', 't'), 5);

  // Rewind: the live checkpoint and the store mirror move to 2...
  const result = bus.seekGroupOffsets('g', 't', { t: 2 });
  assert.equal(result.topics[0].replayed, 3);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 2 });
  assert.equal(store.get('g', 't'), 2);
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 3, 4, 5]);

  // ...but the offset journal's recovery is highest-wins by design (see
  // `seekGroupOffsets`): a rewind appends its lower seq without erasing
  // the earlier high-water, so a restart before the next forward commit
  // reseeds the pre-seek checkpoint from the journal — while the store
  // mirror itself (last-wins) does follow the move.
  const restarted = new EventBus({ durableLogDir: dir });
  assert.deepEqual(restarted.getCommittedOffsets('g'), { t: 5 });
  const fromStore = new EventBus({ offsetStore: store });
  await fromStore.recoverFromOffsetStore();
  assert.deepEqual(fromStore.getCommittedOffsets('g'), { t: 2 });

  // A forward move does persist through the journal: the max advances.
  bus.subscribeToGroup('g2', 'u', () => {});
  await publish(bus, clock, 'u', 5);
  bus.commitOffset('g2', 'u', 2);
  bus.seekGroupOffsets('g2', 'u', { u: 5 });
  assert.equal(store.get('g2', 'u'), 5);
  const restarted2 = new EventBus({ durableLogDir: dir });
  assert.deepEqual(restarted2.getCommittedOffsets('g2'), { u: 5 });
});

test('lag is re-evaluated from the new checkpoint', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  bus.subscribeToGroup('g', 'orders', () => {});
  await publish(bus, clock, 'orders', 5);
  bus.commitOffset('g', 'orders', 5);
  const lagOf = () =>
    bus
      .getStats()
      .groupLag.find((row) => row.groupId === 'g' && row.topic === 'orders');
  assert.deepEqual(lagOf(), {
    groupId: 'g',
    pattern: 'orders',
    topic: 'orders',
    assignedSeq: 5,
    committedSeq: 5,
    lingerHeldToSeq: 0,
    lag: 0,
  });
  bus.seekGroupOffsets('g', 'orders', { orders: 2 });
  assert.deepEqual(lagOf(), {
    groupId: 'g',
    pattern: 'orders',
    topic: 'orders',
    assignedSeq: 5,
    committedSeq: 2,
    lingerHeldToSeq: 0,
    lag: 3,
  });
});

test('queued backlog and in-flight deliveries are untouched by the seek', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 't', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  // No flush: 1..5 sit queued, undelivered — unlike `seek`, the group
  // seek must not drop them as stale.
  for (let i = 0; i < 5; i++) {
    clock.advance(10);
    bus.publish('t', { i });
  }
  bus.commitOffset('g', 't', 5);
  const result = bus.seekGroupOffsets('g', 't', { t: 1 });
  assert.equal(result.topics[0].replayed, 4);
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 2, 3, 4, 5]);

  // Reliable in-flight deliveries on the same topic are untouched too:
  // the group seek replays only into the group's own members — it never
  // invalidates another subscription's outstanding deliveries (unlike
  // `seek`, which invalidates its own subscriber's in-flight).
  const deliveries: Array<Delivery<BusMessage>> = [];
  bus.subscribeReliable(
    't2',
    (d) => {
      deliveries.push(d);
      // Never ack: the deliveries stay in-flight across the seek.
    },
    { ackTimeoutMs: 60_000 },
  );
  const memberSeen: number[] = [];
  const member = bus.subscribeToGroup('g2', 't2', (msg: BusMessage) => {
    memberSeen.push(msg.seq);
  });
  void member;
  for (let i = 0; i < 3; i++) bus.publish('t2', { i });
  await flush();
  assert.equal(deliveries.length, 3);
  assert.deepEqual(memberSeen, [1, 2, 3]);
  bus.commitOffset('g2', 't2', 3);
  bus.seekGroupOffsets('g2', 't2', { t2: 1 });
  assert.equal(deliveries.length, 3);
  await flush();
  assert.equal(deliveries.length, 3);
  assert.deepEqual(memberSeen, [1, 2, 3, 2, 3]);
  for (const d of deliveries) d.ack();
});

test('rewind redelivers under at-least-once and never counts a gap', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seen: number[] = [];
  bus.subscribeToGroup('g', 't', (msg: BusMessage) => {
    seen.push(msg.seq);
  });
  await publish(bus, clock, 't', 5);
  bus.commitOffset('g', 't', 5);
  bus.seekGroupOffsets('g', 't', { t: 3 });
  await flush();
  // The replay deliberately duplicates 4 and 5: at-least-once, the
  // consumer dedupes by messageId when that matters.
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 4, 5]);
  bus.publish('t', { i: 5 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 4, 5, 6]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('every live member replays the full range', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seenA: number[] = [];
  const seenB: number[] = [];
  bus.subscribeToGroup('g', 't', (msg: BusMessage) => {
    seenA.push(msg.seq);
  });
  bus.subscribeToGroup('g', 't', (msg: BusMessage) => {
    seenB.push(msg.seq);
  });
  await publish(bus, clock, 't', 5);
  bus.commitOffset('g', 't', 5);
  const result = bus.seekGroupOffsets('g', 't', { t: 2 });
  assert.equal(result.members, 2);
  assert.equal(result.topics[0].replayed, 6);
  await flush();
  // Round-robin live delivery split 1/3/5 and 2/4; the seek replays
  // 3..5 into each member, so each member sees the range again.
  assert.deepEqual(seenA, [1, 3, 5, 3, 4, 5]);
  assert.deepEqual(seenB, [2, 4, 3, 4, 5]);
});

test('per-topic targets move independently', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const seenEmail: number[] = [];
  const seenSms: number[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (msg: BusMessage) => {
    if (msg.topic === 'jobs.email') seenEmail.push(msg.seq);
    else seenSms.push(msg.seq);
  });
  for (let i = 0; i < 3; i++) {
    clock.advance(10);
    bus.publish('jobs.email', { i });
  }
  for (let i = 0; i < 2; i++) {
    clock.advance(10);
    bus.publish('jobs.sms', { i });
  }
  await flush();
  bus.commitOffset('g', 'jobs.email', 3);
  bus.commitOffset('g', 'jobs.sms', 2);

  const result = bus.seekGroupOffsets('g', 'jobs.*', { 'jobs.email': 1, 'jobs.sms': 'latest' });
  assert.deepEqual(
    result.topics.map((t) => [t.topic, t.position, t.replayed]),
    [
      ['jobs.email', 1, 2],
      ['jobs.sms', 2, 0],
    ],
  );
  assert.deepEqual(bus.getCommittedOffsets('g'), { 'jobs.email': 1, 'jobs.sms': 2 });
  await flush();
  assert.deepEqual(seenEmail, [1, 2, 3, 2, 3]);
  assert.deepEqual(seenSms, [1, 2]);
  const audit = bus.getSubscriptionAudit({ action: 'group_offsets_seeked' });
  assert.deepEqual(audit[0].detail?.targets, { 'jobs.email': 1, 'jobs.sms': 'latest' });
  assert.deepEqual(audit[0].detail?.previous, { 'jobs.email': 3, 'jobs.sms': 2 });
});

test('a seek with no live members still moves the checkpoint', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  const member = bus.subscribeToGroup('g', 't', () => {});
  await publish(bus, clock, 't', 5);
  bus.commitOffset('g', 't', 5);
  member.unsubscribe();
  await flush();
  assert.deepEqual(bus.getGroupMembers('g', 't'), []);

  const result = bus.seekGroupOffsets('g', 't', { t: 2 });
  assert.equal(result.members, 0);
  assert.equal(result.topics[0].replayed, 0);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 2 });
  assert.equal(bus.getSubscriptionAudit({ action: 'group_offsets_seeked' }).length, 1);
});

test('empty targets is a no-op with no audit', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  bus.subscribeToGroup('g', 't', () => {});
  await publish(bus, clock, 't', 2);
  bus.commitOffset('g', 't', 2);
  const result = bus.seekGroupOffsets('g', 't', {});
  assert.deepEqual(result, { groupId: 'g', pattern: 't', topics: [], members: 1 });
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 2 });
  assert.equal(bus.getSubscriptionAudit({ action: 'group_offsets_seeked' }).length, 0);
});

test('partitioned group: each member replays only its own partitions', async () => {
  const clock = makeClock();
  const { bus } = makeBus(clock, true);
  bus.setTopicPartitions('pt', 2);
  const gotA: Array<[number, number]> = [];
  const gotB: Array<[number, number]> = [];
  const mA = bus.subscribeToGroup('g', 'pt', (m: BusMessage) => {
    gotA.push([m.partition ?? 0, m.seq]);
  }, { partitions: 2, assignment: 'sticky' });
  const mB = bus.subscribeToGroup('g', 'pt', (m: BusMessage) => {
    gotB.push([m.partition ?? 0, m.seq]);
  }, { partitions: 2, assignment: 'sticky' });
  // Sticky balances 2 partitions over 2 members, one each — but which
  // member owns which partition is the assignor's call, so resolve the
  // owners from the live assignment.
  const assignment = bus.getPartitionAssignment('g', 'pt');
  assert.equal(Object.keys(assignment).length, 2);
  const ownerOf = (p: number) => (assignment[p] === mA.id ? gotA : gotB);
  const owner0 = ownerOf(0);
  const owner1 = ownerOf(1);
  assert.notEqual(owner0, owner1);
  for (let i = 0; i < 4; i++) bus.publish('pt', { i }, { partition: 0 });
  for (let i = 0; i < 4; i++) bus.publish('pt', { i }, { partition: 1 });
  await flush();
  // Per-partition numbering: each partition runs its own seq 1..4.
  assert.deepEqual(owner0, [
    [0, 1],
    [0, 2],
    [0, 3],
    [0, 4],
  ]);
  assert.deepEqual(owner1, [
    [1, 1],
    [1, 2],
    [1, 3],
    [1, 4],
  ]);
  bus.commitOffset('g', 'pt', 4);

  const result = bus.seekGroupOffsets('g', 'pt', { pt: 2 });
  assert.equal(result.topics[0].replayed, 4);
  assert.deepEqual(bus.getCommittedOffsets('g'), { pt: 2 });
  await flush();
  // Each member replays only its own partition's seqs past position 2.
  assert.deepEqual(owner0, [
    [0, 1],
    [0, 2],
    [0, 3],
    [0, 4],
    [0, 3],
    [0, 4],
  ]);
  assert.deepEqual(owner1, [
    [1, 1],
    [1, 2],
    [1, 3],
    [1, 4],
    [1, 3],
    [1, 4],
  ]);
  bus.publish('pt', { i: 4 }, { partition: 0 });
  bus.publish('pt', { i: 4 }, { partition: 1 });
  await flush();
  assert.equal(bus.getStats().sequenceGaps, 0);
});
