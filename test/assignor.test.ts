import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';
import {
  assertValidPartitionAssignor,
  assertValidPartitionAssignment,
  rendezvousPartitionAssignor,
  resolveBuiltinPartitionAssignor,
  stickyPartitionAssignor,
  type PartitionAssignor,
} from '../src/assignor.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A custom assignor that pins every partition to the first roster member. */
const pinFirst: PartitionAssignor = {
  name: 'pin-first',
  assign(partitions: number, members: string[]): Map<number, string> {
    const out = new Map<number, string>();
    for (let p = 0; p < partitions; p++) out.set(p, members[0]);
    return out;
  },
};

// --- assertValidPartitionAssignor ---

test('assertValidPartitionAssignor accepts a well-formed assignor', () => {
  assert.doesNotThrow(() => assertValidPartitionAssignor(pinFirst, 'test'));
  assert.doesNotThrow(() =>
    assertValidPartitionAssignor({ name: 'x', assign: () => new Map() }, 'test'),
  );
});

test('assertValidPartitionAssignor rejects malformed assignors with RangeError', () => {
  assert.throws(() => assertValidPartitionAssignor(undefined, 'test'), RangeError);
  assert.throws(() => assertValidPartitionAssignor(null, 'test'), RangeError);
  assert.throws(() => assertValidPartitionAssignor('nope', 'test'), RangeError);
  assert.throws(() => assertValidPartitionAssignor({}, 'test'), RangeError);
  assert.throws(() => assertValidPartitionAssignor({ name: '' }, 'test'), RangeError);
  assert.throws(() => assertValidPartitionAssignor({ name: 'x' }, 'test'), RangeError);
  assert.throws(
    () => assertValidPartitionAssignor({ name: 'x', assign: 'not-a-function' }, 'test'),
    RangeError,
  );
});

// --- assertValidPartitionAssignment ---

test('assertValidPartitionAssignment accepts a complete assignment', () => {
  const m = new Map([
    [0, 'a'],
    [1, 'b'],
    [2, 'a'],
  ]);
  assert.doesNotThrow(() => assertValidPartitionAssignment(m, 3, ['a', 'b'], 'test'));
});

test('assertValidPartitionAssignment accepts empty when there is nothing to assign', () => {
  assert.doesNotThrow(() => assertValidPartitionAssignment(new Map(), 0, ['a'], 'test'));
  assert.doesNotThrow(() => assertValidPartitionAssignment(new Map(), 4, [], 'test'));
});

test('assertValidPartitionAssignment rejects invalid results with RangeError', () => {
  // Empty while there is work to do.
  assert.throws(() => assertValidPartitionAssignment(new Map(), 3, ['a'], 'test'), RangeError);
  // Not a Map.
  assert.throws(
    () => assertValidPartitionAssignment({ 0: 'a' }, 1, ['a'], 'test'),
    RangeError,
  );
  // Missing partition.
  assert.throws(
    () => assertValidPartitionAssignment(new Map([[0, 'a']]), 2, ['a'], 'test'),
    RangeError,
  );
  // Unknown owner.
  assert.throws(
    () =>
      assertValidPartitionAssignment(
        new Map([
          [0, 'a'],
          [1, 'ghost'],
        ]),
        2,
        ['a'],
        'test',
      ),
    RangeError,
  );
  // Out-of-range partition key.
  assert.throws(
    () =>
      assertValidPartitionAssignment(
        new Map([
          [0, 'a'],
          [7, 'a'],
        ]),
        2,
        ['a'],
        'test',
      ),
    RangeError,
  );
  // Non-empty when there are no members.
  assert.throws(
    () => assertValidPartitionAssignment(new Map([[0, 'a']]), 1, [], 'test'),
    RangeError,
  );
});

// --- built-in assignors ---

test('built-in assignors keep their historical names and semantics', () => {
  assert.equal(rendezvousPartitionAssignor.name, 'rendezvous');
  assert.equal(stickyPartitionAssignor.name, 'sticky');
  assert.equal(resolveBuiltinPartitionAssignor('rendezvous'), rendezvousPartitionAssignor);
  assert.equal(resolveBuiltinPartitionAssignor('sticky'), stickyPartitionAssignor);
  // Deterministic: same inputs always agree.
  const members = ['m1', 'm2', 'm3'];
  const a = rendezvousPartitionAssignor.assign(6, members);
  const b = rendezvousPartitionAssignor.assign(6, members);
  assert.deepEqual([...a.entries()], [...b.entries()]);
  assert.equal(a.size, 6);
  for (const [, owner] of a) assert.ok(members.includes(owner));
  // Sticky stays balanced.
  const s = stickyPartitionAssignor.assign(6, members);
  const loads = new Map<string, number>();
  for (const [, owner] of s) loads.set(owner, (loads.get(owner) ?? 0) + 1);
  assert.deepEqual([...loads.values()].sort(), [2, 2, 2]);
});

// --- bus integration ---

test('custom assignor drives the group assignment', async () => {
  const bus = new EventBus();
  const got: string[][] = [[], []];
  const m1 = bus.subscribeToGroup('g', 't.*', (m) => got[0].push(m.payload as string), {
    partitions: 4,
    assignor: pinFirst,
  });
  const m2 = bus.subscribeToGroup('g', 't.*', (m) => got[1].push(m.payload as string), {
    partitions: 4,
    assignor: pinFirst,
  });
  // Every partition is pinned to the first member, so only it delivers.
  assert.deepEqual(bus.getPartitionAssignment('g', 't.*'), {
    0: m1.id,
    1: m1.id,
    2: m1.id,
    3: m1.id,
  });
  for (let i = 0; i < 8; i++) bus.publish('t.1', `m${i}`);
  await flush();
  assert.equal(got[0].length, 8);
  assert.equal(got[1].length, 0);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('rebalance events report the custom assignor name', () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
    partitions: 2,
    assignor: pinFirst,
    onRebalance,
  });
  const m2 = bus.subscribeToGroup('g', 't.*', () => {}, {
    partitions: 2,
    assignor: pinFirst,
    onRebalance,
  });
  assert.ok(events.length > 0);
  for (const e of events) {
    assert.equal(e.partitionRebalance?.strategy, 'pin-first');
  }
  m1.unsubscribe();
  m2.unsubscribe();
});

test('default group still reports the rendezvous strategy', () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
    partitions: 2,
    onRebalance: (e) => events.push(e),
  });
  assert.equal(events[0].partitionRebalance?.strategy, 'rendezvous');
  m1.unsubscribe();
});

test('a later member naming a different assignor fails fast and rolls back', () => {
  const bus = new EventBus();
  const before = bus.getStats().totalSubscribers;
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2, assignor: pinFirst });
  const other: PartitionAssignor = {
    name: 'something-else',
    assign: (p, members) => pinFirst.assign(p, members),
  };
  assert.throws(
    () => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2, assignor: other }),
    RangeError,
  );
  // The failed join left no half-registered subscriber behind.
  assert.equal(bus.getStats().totalSubscribers, before + 1);
  // The group's assignor is unchanged.
  const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2, assignor: pinFirst });
  assert.deepEqual(bus.getPartitionAssignment('g', 't.*'), {
    0: m1.id,
    1: m1.id,
  });
  m1.unsubscribe();
  m2.unsubscribe();
});

test('contradictory assignment string + assignor name throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeToGroup('g', 't.*', () => {}, {
        partitions: 2,
        assignment: 'sticky',
        assignor: pinFirst, // name 'pin-first' !== 'sticky'
      }),
    RangeError,
  );
});

test('consistent assignment string + built-in assignor object works', () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
    partitions: 2,
    assignment: 'sticky',
    assignor: stickyPartitionAssignor,
    onRebalance: (e) => events.push(e),
  });
  assert.equal(events[0].partitionRebalance?.strategy, 'sticky');
  m1.unsubscribe();
});

test('an assignor returning an empty assignment fails the join with RangeError', () => {
  const bus = new EventBus();
  const before = bus.getStats().totalSubscribers;
  const empty: PartitionAssignor = { name: 'empty', assign: () => new Map() };
  assert.throws(
    () => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 3, assignor: empty }),
    RangeError,
  );
  assert.equal(bus.getStats().totalSubscribers, before);
});

test('an assignor naming an unknown owner fails the join with RangeError', () => {
  const bus = new EventBus();
  const bad: PartitionAssignor = {
    name: 'bad-owner',
    assign: (partitions) => {
      const m = new Map<number, string>();
      for (let p = 0; p < partitions; p++) m.set(p, 'ghost');
      return m;
    },
  };
  assert.throws(
    () => bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2, assignor: bad }),
    RangeError,
  );
});

test('a malformed assignor object throws RangeError at registration', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribeToGroup('g', 't.*', () => {}, {
        partitions: 2,
        assignor: { name: 'x' } as never,
      }),
    RangeError,
  );
});

test('assignment results are recorded in the subscription audit log', () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2, assignor: pinFirst });
  const entries = bus.getSubscriptionAudit({ action: 'rebalance' });
  assert.ok(entries.length > 0);
  const last = entries[entries.length - 1];
  const detail = last.detail as { assignor?: string; assignment?: Record<number, string> };
  assert.equal(detail.assignor, 'pin-first');
  assert.deepEqual(detail.assignment, { 0: m1.id, 1: m1.id });
  m1.unsubscribe();
});

test('non-partitioned rebalance audits carry no assignment', () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't.*', () => {});
  const entries = bus.getSubscriptionAudit({ action: 'rebalance' });
  assert.ok(entries.length > 0);
  const last = entries[entries.length - 1];
  const detail = last.detail as { assignor?: string; assignment?: unknown };
  assert.equal(detail.assignor, undefined);
  assert.equal(detail.assignment, undefined);
  m1.unsubscribe();
});

test('sticky assignor still minimizes migration across a leave', () => {
  const bus = new EventBus();
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
  const m2 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
  const m3 = bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 4, assignment: 'sticky' });
  const before = bus.getPartitionAssignment('g', 't.*');
  m3.unsubscribe();
  const after = bus.getPartitionAssignment('g', 't.*');
  // Sticky keeps every surviving member's partitions where they were.
  for (const [p, owner] of Object.entries(before)) {
    if (owner !== m3.id) assert.equal(after[Number(p)], owner);
  }
  m1.unsubscribe();
  m2.unsubscribe();
});
