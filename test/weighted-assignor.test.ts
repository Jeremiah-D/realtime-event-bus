import test from 'node:test';
import assert from 'node:assert/strict';
import {
  assertValidPartitionAssignment,
  createWeightedPartitionAssignor,
  resolveBuiltinPartitionAssignor,
  weightedAssignment,
} from '../src/assignor.ts';

/**
 * Independent recomputation of the documented weighted semantics,
 * written from the spec (not imported from the implementation):
 * sorted roster, largest-remainder targets over normalized weights
 * (missing weight = 1), keep phase then least-loaded pool phase.
 */
function expectedAssignment(
  partitions: number,
  members: string[],
  weights: ReadonlyMap<string, number>,
  previous?: ReadonlyMap<number, string>,
): Map<number, string> {
  const out = new Map<number, string>();
  if (partitions <= 0 || members.length === 0) return out;
  const roster = [...new Set(members)].sort();
  let total = 0;
  for (const m of roster) total += weights.get(m) ?? 1;
  const target = new Map<string, number>();
  const remainders: Array<{ member: string; remainder: number }> = [];
  let assigned = 0;
  for (const m of roster) {
    const exact = (partitions * (weights.get(m) ?? 1)) / total;
    const floor = Math.floor(exact);
    target.set(m, floor);
    assigned += floor;
    remainders.push({ member: m, remainder: exact - floor });
  }
  remainders.sort((a, b) => {
    if (a.remainder !== b.remainder) return b.remainder - a.remainder;
    return a.member < b.member ? -1 : a.member > b.member ? 1 : 0;
  });
  for (let i = 0; i < partitions - assigned; i++) {
    const entry = remainders[i] as { member: string; remainder: number };
    target.set(entry.member, (target.get(entry.member) as number) + 1);
  }
  const load = new Map(roster.map((m) => [m, 0] as [string, number]));
  const pooled: number[] = [];
  for (let p = 0; p < partitions; p++) {
    const owner = previous?.get(p);
    if (owner !== undefined && target.has(owner) && (load.get(owner) as number) < (target.get(owner) as number)) {
      out.set(p, owner);
      load.set(owner, (load.get(owner) as number) + 1);
    } else {
      pooled.push(p);
    }
  }
  for (const p of pooled) {
    let chosen: string | undefined;
    let chosenLoad = Infinity;
    for (const m of roster) {
      const memberLoad = load.get(m) as number;
      if (memberLoad < (target.get(m) as number) && memberLoad < chosenLoad) {
        chosen = m;
        chosenLoad = memberLoad;
      }
    }
    assert.ok(chosen !== undefined);
    out.set(p, chosen);
    load.set(chosen, (load.get(chosen) as number) + 1);
  }
  return out;
}

function counts(assignment: Map<number, string>): Map<string, number> {
  const out = new Map<string, number>();
  for (const owner of assignment.values()) out.set(owner, (out.get(owner) ?? 0) + 1);
  return out;
}

// --- factory validation ---

test('weighted factory rejects malformed options and bad weights with RangeError', () => {
  assert.throws(() => createWeightedPartitionAssignor(null as never), RangeError);
  assert.throws(() => createWeightedPartitionAssignor([] as never), RangeError);
  assert.throws(
    () => createWeightedPartitionAssignor({ memberWeights: [] as never }),
    RangeError,
  );
  assert.throws(
    () => createWeightedPartitionAssignor({ memberWeights: { a: 0 } }),
    RangeError,
  );
  assert.throws(
    () => createWeightedPartitionAssignor({ memberWeights: { a: -2 } }),
    RangeError,
  );
  assert.throws(
    () => createWeightedPartitionAssignor({ memberWeights: { a: Number.NaN } }),
    RangeError,
  );
  assert.throws(
    () => createWeightedPartitionAssignor({ memberWeights: { a: Infinity } }),
    RangeError,
  );
  assert.throws(
    () => createWeightedPartitionAssignor({ memberWeights: { a: 'x' as never } }),
    RangeError,
  );
  assert.throws(
    () => weightedAssignment(4, ['a', 'b'], new Map([['a', 0]])),
    RangeError,
  );
  assert.throws(
    () => weightedAssignment(4, ['a', 'b'], new Map([['a', -1]])),
    RangeError,
  );
});

// --- shares follow normalized weights ---

test('weighted shares match largest-remainder targets', () => {
  const cases: Array<[number, string[], Map<string, number>, Map<string, number>]> = [
    [12, ['a', 'b'], new Map([['a', 2], ['b', 1]]), new Map([['a', 8], ['b', 4]])],
    [10, ['a', 'b', 'c'], new Map([['a', 3], ['b', 1], ['c', 1]]), new Map([['a', 6], ['b', 2], ['c', 2]])],
    [7, ['a', 'b'], new Map([['a', 1], ['b', 1]]), new Map([['a', 4], ['b', 3]])],
    [5, ['a', 'b', 'c'], new Map([['a', 5], ['b', 3], ['c', 2]]), new Map([['a', 3], ['b', 1], ['c', 1]])],
  ];
  for (const [partitions, members, weights, expected] of cases) {
    const got = weightedAssignment(partitions, members, weights);
    assert.deepEqual(counts(got), expected, `partitions=${partitions} weights=${[...weights]}`);
    assert.deepEqual(got, expectedAssignment(partitions, members, weights));
    assertValidPartitionAssignment(got, partitions, members, 'weighted');
  }
});

test('weights normalize: rescaling every weight leaves the assignment unchanged', () => {
  const members = ['a', 'b', 'c'];
  const base = new Map([['a', 2], ['b', 1], ['c', 1]]);
  const scaled = new Map([['a', 20], ['b', 10], ['c', 10]]);
  const fractional = new Map([['a', 0.5], ['b', 0.25], ['c', 0.25]]);
  const first = weightedAssignment(16, members, base);
  assert.deepEqual(weightedAssignment(16, members, scaled), first);
  assert.deepEqual(weightedAssignment(16, members, fractional), first);
});

test('missing weight is one unit share; empty weights are the equal split', () => {
  // Only 'a' is configured: 'b' still gets one unit share, 3:1.
  const partial = weightedAssignment(12, ['a', 'b'], new Map([['a', 3]]));
  assert.deepEqual(counts(partial), new Map([['a', 9], ['b', 3]]));
  // Nothing configured: equal split, within one of each other.
  const empty = weightedAssignment(10, ['a', 'b', 'c'], new Map());
  assert.deepEqual(counts(empty), new Map([['a', 4], ['b', 3], ['c', 3]]));
  const factoryEmpty = createWeightedPartitionAssignor().assign(10, ['a', 'b', 'c']);
  assert.deepEqual(factoryEmpty, empty);
  // Weights for members that never join are ignored.
  const ghost = weightedAssignment(12, ['a', 'b'], new Map([['a', 2], ['b', 1], ['ghost', 100]]));
  assert.deepEqual(counts(ghost), new Map([['a', 8], ['b', 4]]));
});

test('equal weights reproduce the balanced split, not rendezvous hashing', () => {
  const got = weightedAssignment(12, ['a', 'b', 'c'], new Map());
  assert.deepEqual(counts(got), new Map([['a', 4], ['b', 4], ['c', 4]]));
  assertValidPartitionAssignment(got, 12, ['a', 'b', 'c'], 'weighted');
});

test('mixed weights match the independent recomputation', () => {
  const cases: Array<[number, string[], Map<string, number>]> = [
    [24, ['m1', 'm2', 'm3', 'm4'], new Map([['m1', 4], ['m2', 2], ['m3', 1], ['m4', 1]])],
    [13, ['x', 'y', 'z'], new Map([['x', 7], ['y', 2]])],
    [100, ['a', 'b'], new Map([['a', 3], ['b', 7]])],
  ];
  for (const [partitions, members, weights] of cases) {
    const got = weightedAssignment(partitions, members, weights);
    assert.deepEqual(got, expectedAssignment(partitions, members, weights));
    assertValidPartitionAssignment(got, partitions, members, 'weighted');
  }
});

// --- determinism / byte-stability ---

test('weighted assignment is deterministic and roster-order independent', () => {
  const weights = new Map([['a', 2], ['b', 1], ['c', 3]]);
  const first = weightedAssignment(18, ['a', 'b', 'c'], weights);
  assert.deepEqual(weightedAssignment(18, ['a', 'b', 'c'], weights), first);
  // Same weight set + member set, different roster order: same map.
  assert.deepEqual(weightedAssignment(18, ['c', 'a', 'b'], weights), first);
  assert.deepEqual(weightedAssignment(18, ['b', 'c', 'a'], weights), first);
  const assignor = createWeightedPartitionAssignor({ memberWeights: { a: 2, b: 1, c: 3 } });
  assert.deepEqual(assignor.assign(18, ['a', 'b', 'c']), first);
  assert.deepEqual(assignor.assign(18, ['c', 'b', 'a']), first);
});

test('empty inputs return an empty assignment', () => {
  const assignor = createWeightedPartitionAssignor({ memberWeights: { a: 2 } });
  assert.equal(assignor.assign(0, ['a']).size, 0);
  assert.equal(assignor.assign(4, []).size, 0);
  assert.equal(weightedAssignment(0, ['a'], new Map()).size, 0);
});

// --- minimal migration on weight-change rebalance ---

test('weight-change rebalance moves exactly the changed-share portion', () => {
  const cases: Array<[number, string[], Map<string, number>, Map<string, number>]> = [
    [12, ['a', 'b'], new Map([['a', 1], ['b', 1]]), new Map([['a', 3], ['b', 1]])],
    [12, ['a', 'b', 'c'], new Map([['a', 1], ['b', 1], ['c', 1]]), new Map([['a', 2], ['b', 1], ['c', 1]])],
    [20, ['a', 'b', 'c'], new Map([['a', 1], ['b', 2], ['c', 1]]), new Map([['a', 3], ['b', 1], ['c', 2]])],
  ];
  for (const [partitions, members, before, after] of cases) {
    const previous = weightedAssignment(partitions, members, before);
    const next = weightedAssignment(partitions, members, after, previous);
    assert.deepEqual(next, expectedAssignment(partitions, members, after, previous));
    assertValidPartitionAssignment(next, partitions, members, 'weighted');
    // Exactly the shed portion moves: sum of (previously owned - new
    // target) over members that shrank, no extra churn anywhere.
    const prevCounts = counts(previous);
    const nextCounts = counts(next);
    let shed = 0;
    for (const m of members) {
      shed += Math.max(0, (prevCounts.get(m) ?? 0) - (nextCounts.get(m) ?? 0));
    }
    let migrated = 0;
    for (let p = 0; p < partitions; p++) {
      if (previous.get(p) !== next.get(p)) migrated += 1;
    }
    assert.equal(migrated, shed, `weights ${[...before]} -> ${[...after]}`);
    // Every member keeps min(new target, previously owned): nothing
    // a member was entitled to keep was taken away.
    for (const m of members) {
      let kept = 0;
      for (let p = 0; p < partitions; p++) {
        if (previous.get(p) === m && next.get(p) === m) kept += 1;
      }
      assert.equal(kept, Math.min(nextCounts.get(m) as number, prevCounts.get(m) as number));
    }
  }
});

test('a member whose target is unchanged keeps every partition', () => {
  // 'c' keeps weight 2 and the weight total stays 6 while a/b
  // rebalance around it, so c's target stays 4 of 12 and all of
  // c's partitions stay put.
  const members = ['a', 'b', 'c'];
  const before = new Map([['a', 1], ['b', 3], ['c', 2]]);
  const after = new Map([['a', 2], ['b', 2], ['c', 2]]);
  const previous = weightedAssignment(12, members, before);
  const next = weightedAssignment(12, members, after, previous);
  assert.deepEqual(counts(next), new Map([['a', 4], ['b', 4], ['c', 4]]));
  for (let p = 0; p < 12; p++) {
    if (previous.get(p) === 'c') assert.equal(next.get(p), 'c', `partition ${p} moved away from unchanged member c`);
  }
});

test('rescaling weights across a rebalance moves nothing', () => {
  const members = ['a', 'b'];
  const previous = weightedAssignment(12, members, new Map([['a', 2], ['b', 1]]));
  const next = weightedAssignment(12, members, new Map([['a', 4], ['b', 2]]), previous);
  assert.deepEqual(next, previous);
});

// --- resolveBuiltin ---

test('resolveBuiltin resolves weighted with and without options', () => {
  const assignor = resolveBuiltinPartitionAssignor('weighted', { memberWeights: { a: 2, b: 1 } });
  assert.equal(assignor.name, 'weighted');
  assert.deepEqual(counts(assignor.assign(12, ['a', 'b'])), new Map([['a', 8], ['b', 4]]));
  const equal = resolveBuiltinPartitionAssignor('weighted');
  assert.equal(equal.name, 'weighted');
  assert.deepEqual(counts(equal.assign(12, ['a', 'b'])), new Map([['a', 6], ['b', 6]]));
  assert.throws(
    () => resolveBuiltinPartitionAssignor('weighted', { memberWeights: { a: 0 } }),
    RangeError,
  );
});

test('weighted assignment survives a member join/leave without stranding partitions', () => {
  const before = weightedAssignment(12, ['a', 'b'], new Map([['a', 2], ['b', 1]]));
  const joined = weightedAssignment(12, ['a', 'b', 'c'], new Map([['a', 2], ['b', 1]]), before);
  assert.deepEqual(counts(joined), new Map([['a', 6], ['b', 3], ['c', 3]]));
  assertValidPartitionAssignment(joined, 12, ['a', 'b', 'c'], 'weighted');
  const left = weightedAssignment(12, ['a'], new Map([['a', 2], ['b', 1]]), joined);
  assert.deepEqual(counts(left), new Map([['a', 12]]));
  assertValidPartitionAssignment(left, 12, ['a'], 'weighted');
});
