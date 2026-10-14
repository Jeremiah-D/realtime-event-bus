import test from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs';
import {
  EventBus,
  type GroupRebalanceEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

test('instanceId validation: empty / NUL / sub-N collision / bad timeout throw RangeError', () => {
  assert.throws(
    () => new EventBus().subscribeToGroup('g', 't', () => {}, { instanceId: '' }),
    RangeError,
  );
  assert.throws(
    () => new EventBus().subscribeToGroup('g', 't', () => {}, { instanceId: 'a\0b' }),
    RangeError,
  );
  assert.throws(
    () => new EventBus().subscribeToGroup('g', 't', () => {}, { instanceId: 'sub-12' }),
    RangeError,
  );
  for (const sessionTimeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () =>
        new EventBus().subscribeToGroup('g', 't', () => {}, {
          instanceId: 'i1',
          sessionTimeoutMs,
        }),
      RangeError,
      `sessionTimeoutMs=${sessionTimeoutMs}`,
    );
  }
});

test('duplicate online instanceId throws and rolls back; the first member keeps working', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  const m1 = bus.subscribeToGroup('g', 't', (msg) => seen.push(msg.payload), {
    instanceId: 'i1',
  });
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}, { instanceId: 'i1' }),
    /already online/,
  );
  // The failed join rolled back: the roster still has exactly one member,
  // and the first member still receives messages.
  assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1']);
  bus.publish('t', 'hello');
  await flush();
  assert.deepEqual(seen, ['hello']);
  m1.unsubscribe();
});

// ---------------------------------------------------------------------------
// limbo: leave inside the window fires no rebalance
// ---------------------------------------------------------------------------

test('static leave inside the session window: no rebalance, slot held', async () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
  const m1 = bus.subscribeToGroup('g', 't', () => {}, {
    instanceId: 'i1',
    sessionTimeoutMs: 5000,
    onRebalance,
  });
  const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
  events.length = 0;

  m1.unsubscribe();
  await flush();

  assert.equal(events.length, 0, 'no rebalance should fire while the slot is held');
  // The roster still names the instanceId — the slot is held, not released.
  assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);
  m2.unsubscribe();
});

test('rejoin inside the window: no rebalance, partition assignment retained', async () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
  const seenOld: unknown[] = [];
  const seenNew: unknown[] = [];
  const m1 = bus.subscribeToGroup('g', 't', (msg) => seenOld.push(msg.payload), {
    instanceId: 'i1',
    sessionTimeoutMs: 5000,
    partitions: 4,
    onRebalance,
  });
  bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
  const before = bus.getPartitionAssignment('g', 't');
  assert.equal(Object.keys(before).length, 4);
  events.length = 0;

  m1.unsubscribe();
  await flush();
  assert.equal(events.length, 0, 'leave must not rebalance inside the window');
  // The assignment is frozen while the slot is held.
  assert.deepEqual(bus.getPartitionAssignment('g', 't'), before);

  const m1b = bus.subscribeToGroup('g', 't', (msg) => seenNew.push(msg.payload), {
    instanceId: 'i1',
    sessionTimeoutMs: 5000,
    onRebalance,
  });
  await flush();
  assert.equal(events.length, 0, 'rejoin must not rebalance inside the window');
  // The assignment still names the same roster keys — retained, not recomputed.
  assert.deepEqual(bus.getPartitionAssignment('g', 't'), before);

  // Functional proof: a message for a partition owned by i1 reaches the
  // rejoined subscription. The topic needs a matching publish-side
  // partition count for the explicit partition to be in range (EB-90).
  const owned = Object.entries(before)
    .filter(([, owner]) => owner === 'i1')
    .map(([p]) => Number(p));
  assert.ok(owned.length > 0, 'i1 should own at least one partition');
  bus.setTopicPartitions('t', 4);
  bus.publish('t', 'back', { partition: owned[0] });
  await flush();
  assert.deepEqual(seenNew, ['back']);
  assert.deepEqual(seenOld, []);
  m1b.unsubscribe();
});

test('messages published during limbo reach live members (no loss)', async () => {
  const bus = new EventBus();
  const seen1: unknown[] = [];
  const seen2: unknown[] = [];
  const m1 = bus.subscribeToGroup('g', 't', (msg) => seen1.push(msg.payload), {
    instanceId: 'i1',
    sessionTimeoutMs: 5000,
    partitions: 2,
  });
  const m2 = bus.subscribeToGroup('g', 't', (msg) => seen2.push(msg.payload), {});
  m1.unsubscribe();

  bus.publish('t', 'x');
  bus.publish('t', 'y');
  await flush();

  assert.deepEqual(seen1, []);
  assert.deepEqual([...seen2].sort(), ['x', 'y']);
  m2.unsubscribe();
});

// ---------------------------------------------------------------------------
// expiry: the slot is released and the group rebalances cooperatively
// ---------------------------------------------------------------------------

test('session expiry releases the slot and rebalances', async () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
  const m1 = bus.subscribeToGroup('g', 't', () => {}, {
    instanceId: 'i1',
    sessionTimeoutMs: 40,
    partitions: 2,
    onRebalance,
  });
  const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
  events.length = 0;

  m1.unsubscribe();
  await flush();
  assert.equal(events.length, 0, 'no rebalance before the window expires');

  await sleep(150);
  const leaves = events.filter((e) => e.trigger === 'leave');
  assert.equal(leaves.length, 1);
  assert.equal(leaves[0].memberId, 'i1');
  assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id]);
  // The partitions moved to the remaining member.
  const assignment = bus.getPartitionAssignment('g', 't');
  assert.ok(Object.values(assignment).every((owner) => owner === m2.id));

  // A rejoin after expiry is a fresh join: it rebalances like a newcomer.
  events.length = 0;
  const m3 = bus.subscribeToGroup('g', 't', () => {}, {
    instanceId: 'i1',
    sessionTimeoutMs: 5000,
    onRebalance,
  });
  assert.ok(events.some((e) => e.trigger === 'join' && e.memberId === 'i1'));
  assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id, 'i1']);
  m2.unsubscribe();
  m3.unsubscribe();
});

// ---------------------------------------------------------------------------
// sticky: a limbo member keeps its partitions when others join
// ---------------------------------------------------------------------------

test('sticky + static: limbo member keeps its partitions on a join rebalance', async () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't', () => {}, {
    instanceId: 'i1',
    sessionTimeoutMs: 5000,
    partitions: 4,
    assignment: 'sticky',
  });
  bus.subscribeToGroup('g', 't', () => {}, {});
  m1.unsubscribe();
  await flush();
  const frozen = bus.getPartitionAssignment('g', 't');
  const i1Partitions = Object.entries(frozen)
    .filter(([, owner]) => owner === 'i1')
    .map(([p]) => p);
  assert.ok(i1Partitions.length > 0, 'i1 should own partitions before the join');

  // A new member joins while i1 is in limbo: the rebalance runs (join),
  // but the sticky assignor keeps i1's partitions on i1 — the limbo
  // member is still in the roster, so stickiness retains them.
  bus.subscribeToGroup('g', 't', () => {}, {});
  await flush();
  const after = bus.getPartitionAssignment('g', 't');
  for (const p of i1Partitions) {
    assert.equal(after[Number(p)], 'i1', `partition ${p} should stay on i1`);
  }
});

// ---------------------------------------------------------------------------
// per-instance offsets: assignment watermarks + committed checkpoints
// ---------------------------------------------------------------------------

test('getGroupOffsets(groupId, instanceId): per-member assignment watermarks', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { instanceId: 'i1' });
  bus.subscribeToGroup('g', 't', () => {}, {});
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  // Round-robin over the roster ['i1', sub-N]: i1 takes seq 1, the dynamic
  // member takes seq 2.
  assert.deepEqual(bus.getGroupOffsets('g', 'i1'), { t: 1 });
  assert.deepEqual(bus.getGroupOffsets('g'), { t: 2 });
  assert.deepEqual(bus.getGroupOffsets('g', 'nope'), {});
});

test('commitOffset with instanceId: per-member checkpoints, journaled across restarts', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'eb-static-'));
  try {
    const bus = new EventBus({ durableLogDir: dir });
    bus.subscribeToGroup('g', 't', () => {}, { instanceId: 'i1', partitions: 2 });
    bus.commitOffset('g', 't', 5, { instanceId: 'i1' });
    bus.commitOffset('g', 't', 7, { partition: 1, instanceId: 'i1' });
    bus.commitOffset('g', 't', 3);
    assert.deepEqual(bus.getInstanceCommittedOffsets('g', 'i1'), { t: 5 });
    assert.deepEqual(bus.getInstancePartitionCommittedOffsets('g', 't', 'i1'), {
      1: { t: 7 },
    });
    assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });

    // A restarted bus recovers the per-instance checkpoints from the journal.
    const bus2 = new EventBus({ durableLogDir: dir });
    assert.deepEqual(bus2.getInstanceCommittedOffsets('g', 'i1'), { t: 5 });
    assert.deepEqual(bus2.getInstancePartitionCommittedOffsets('g', 't', 'i1'), {
      1: { t: 7 },
    });
    assert.deepEqual(bus2.getCommittedOffsets('g'), { t: 3 });

    assert.throws(
      () => bus.commitOffset('g', 't', 1, { instanceId: '' }),
      RangeError,
    );
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// round-robin rotation is undisturbed by limbo
// ---------------------------------------------------------------------------

test('round-robin rotation skips the limbo member and resumes on rejoin', async () => {
  const bus = new EventBus();
  const seen1: unknown[] = [];
  const seen2: unknown[] = [];
  const m1 = bus.subscribeToGroup('g', 't', (msg) => seen1.push(msg.payload), {
    instanceId: 'i1',
    sessionTimeoutMs: 5000,
  });
  bus.subscribeToGroup('g', 't', (msg) => seen2.push(msg.payload), {});
  bus.publish('t', 'a');
  await flush();
  assert.deepEqual(seen1, ['a']);

  m1.unsubscribe();
  bus.publish('t', 'b');
  await flush();
  // i1 is in limbo: the live member takes the copy.
  assert.deepEqual(seen2, ['b']);

  const seen3: unknown[] = [];
  bus.subscribeToGroup('g', 't', (msg) => seen3.push(msg.payload), {
    instanceId: 'i1',
    sessionTimeoutMs: 5000,
  });
  bus.publish('t', 'c');
  bus.publish('t', 'd');
  await flush();
  // All four messages were delivered exactly once across the three
  // subscription incarnations, and the rejoined member is back in the
  // rotation.
  assert.equal(seen1.length + seen2.length + seen3.length, 4);
  assert.ok(seen3.length >= 1, 'rejoined member should receive messages');
});
