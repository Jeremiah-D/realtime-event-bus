import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  assertValidPartitionAssignment,
  createLagAwarePartitionAssignor,
  lagAwareAssignment,
  rendezvousPartitionAssignor,
  resolveBuiltinPartitionAssignor,
} from '../src/assignor.ts';

/**
 * Independent recomputation of the documented lag-aware semantics,
 * written from the spec (not imported from the implementation):
 * per-partition lowest-lag winner, ties broken by rendezvous score,
 * members without a lag entry ranking at the worst observed lag.
 */
const memberScore = (p: number, member: string): bigint =>
  createHash('sha256').update(`${p}\0${member}`, 'utf8').digest().readBigUInt64BE(0);

function expectedAssignment(
  partitions: number,
  members: string[],
  lag: ReadonlyMap<string, number>,
): Map<number, string> {
  let worst = -1;
  for (const m of members) {
    const v = lag.get(m);
    if (v !== undefined && v > worst) worst = v;
  }
  const out = new Map<number, string>();
  if (worst < 0) return rendezvousPartitionAssignor.assign(partitions, members);
  for (let p = 0; p < partitions; p++) {
    let best = members[0] as string;
    let bestLag = lag.get(best) ?? worst;
    let bestScore = memberScore(p, best);
    for (const m of members) {
      const l = lag.get(m) ?? worst;
      const s = memberScore(p, m);
      if (l < bestLag || (l === bestLag && s > bestScore)) {
        best = m;
        bestLag = l;
        bestScore = s;
      }
    }
    out.set(p, best);
  }
  return out;
}

// --- factory validation ---

test('lag-aware factory rejects malformed options with RangeError', () => {
  assert.throws(() => createLagAwarePartitionAssignor(undefined as never), RangeError);
  assert.throws(() => createLagAwarePartitionAssignor(null as never), RangeError);
  assert.throws(() => createLagAwarePartitionAssignor([] as never), RangeError);
  assert.throws(
    () => createLagAwarePartitionAssignor({ memberLag: [] as never }),
    RangeError,
  );
  assert.throws(
    () => createLagAwarePartitionAssignor({ memberLag: { a: -1 } }),
    RangeError,
  );
  assert.throws(
    () => createLagAwarePartitionAssignor({ memberLag: { a: Number.NaN } }),
    RangeError,
  );
  assert.throws(
    () => createLagAwarePartitionAssignor({ memberLag: { a: Infinity } }),
    RangeError,
  );
  assert.throws(
    () => createLagAwarePartitionAssignor({ memberLag: { a: 'x' as never } }),
    RangeError,
  );
  assert.throws(
    () => createLagAwarePartitionAssignor({ lagProvider: 'x' as never }),
    RangeError,
  );
  assert.throws(
    () => createLagAwarePartitionAssignor({ onFallback: 'x' as never }),
    RangeError,
  );
  assert.throws(
    () => resolveBuiltinPartitionAssignor('lag-aware', undefined as never),
    RangeError,
  );
});

// --- lowest lag wins; ties go to rendezvous ---

test('lowest-lag member wins every partition when lags strictly differ', () => {
  const cases: Array<[number, string[], Map<string, number>]> = [
    [8, ['a', 'b'], new Map([['a', 0], ['b', 100]])],
    [12, ['a', 'b', 'c'], new Map([['a', 50], ['b', 0], ['c', 30]])],
  ];
  for (const [partitions, members, lag] of cases) {
    const got = lagAwareAssignment(partitions, members, lag);
    assert.deepEqual(got, expectedAssignment(partitions, members, lag));
    const winner = [...new Set(got.values())];
    assert.equal(winner.length, 1);
    assertValidPartitionAssignment(got, partitions, members, 'lag-aware');
  }
});

test('equal lags are byte-identical to rendezvous', () => {
  const cases: Array<[number, string[]]> = [
    [1, ['a']],
    [6, ['a', 'b', 'c']],
    [12, ['m1', 'm2']],
    [20, ['x', 'y', 'z', 'w']],
  ];
  for (const [partitions, members] of cases) {
    const lag = new Map(members.map((m) => [m, 42] as [string, number]));
    assert.deepEqual(
      lagAwareAssignment(partitions, members, lag),
      rendezvousPartitionAssignor.assign(partitions, members),
      `partitions=${partitions} members=${members}`,
    );
  }
});

test('mixed lags match the independent recomputation, including ties', () => {
  const cases: Array<[number, string[], Map<string, number>]> = [
    [16, ['a', 'b', 'c'], new Map([['a', 10], ['b', 10], ['c', 30]])],
    [16, ['a', 'b', 'c'], new Map([['a', 0], ['b', 5], ['c', 5]])],
    [24, ['m1', 'm2', 'm3', 'm4'], new Map([['m1', 3], ['m2', 3], ['m3', 3], ['m4', 9]])],
  ];
  for (const [partitions, members, lag] of cases) {
    const got = lagAwareAssignment(partitions, members, lag);
    assert.deepEqual(got, expectedAssignment(partitions, members, lag));
    assertValidPartitionAssignment(got, partitions, members, 'lag-aware');
  }
});

test('members without lag data rank at the worst observed lag', () => {
  // All sampled members tie at 5, so the unsampled member ties with
  // them at the worst observed lag and the whole roster competes on
  // rendezvous — byte-identical to rendezvous over the full roster.
  const members = ['a', 'b', 'c'];
  const lag = new Map([['a', 5], ['c', 5]]);
  assert.deepEqual(
    lagAwareAssignment(16, members, lag),
    rendezvousPartitionAssignor.assign(16, members),
  );
  // A strictly lower sampled lag still beats the unsampled member.
  const lag2 = new Map([['a', 0], ['c', 9]]);
  const got = lagAwareAssignment(8, members, lag2);
  assert.deepEqual([...new Set(got.values())], ['a']);
});

test('no roster member with lag data falls back to rendezvous', () => {
  const members = ['a', 'b'];
  const lag = new Map([['ghost', 0]]);
  assert.deepEqual(
    lagAwareAssignment(8, members, lag),
    rendezvousPartitionAssignor.assign(8, members),
  );
});

test('empty inputs return an empty assignment without counting a fallback', () => {
  const assignor = createLagAwarePartitionAssignor({});
  assert.equal(assignor.assign(0, ['a']).size, 0);
  assert.equal(assignor.assign(4, []).size, 0);
  assert.equal(assignor.fallbackCount(), 0);
});

// --- factory fallback accounting ---

test('no lag data degrades to rendezvous and counts once per assignment', () => {
  const reasons: string[] = [];
  const assignor = createLagAwarePartitionAssignor({
    onFallback: (reason) => {
      reasons.push(reason);
    },
  });
  const got = assignor.assign(8, ['a', 'b']);
  assert.deepEqual(got, rendezvousPartitionAssignor.assign(8, ['a', 'b']));
  assert.equal(assignor.fallbackCount(), 1);
  assert.deepEqual(reasons, ['no-lag-data']);
  assignor.assign(4, ['a']);
  assert.equal(assignor.fallbackCount(), 2);
});

test('provider sample steers the assignment and refreshes per call', () => {
  let sample = new Map([['a', 100], ['b', 0]]);
  const assignor = createLagAwarePartitionAssignor({
    lagProvider: () => sample,
  });
  assert.deepEqual([...new Set(assignor.assign(8, ['a', 'b']).values())], ['b']);
  sample = new Map([['a', 0], ['b', 100]]);
  assert.deepEqual([...new Set(assignor.assign(8, ['a', 'b']).values())], ['a']);
  assert.equal(assignor.fallbackCount(), 0);
});

test('provider returning undefined falls back to the static map', () => {
  const assignor = createLagAwarePartitionAssignor({
    memberLag: { a: 0, b: 50 },
    lagProvider: () => undefined,
  });
  assert.deepEqual([...new Set(assignor.assign(8, ['a', 'b']).values())], ['a']);
  assert.equal(assignor.fallbackCount(), 0);
});

test('provider returning an empty map falls back to the static map', () => {
  const assignor = createLagAwarePartitionAssignor({
    memberLag: { a: 0, b: 50 },
    lagProvider: () => new Map(),
  });
  assert.deepEqual([...new Set(assignor.assign(8, ['a', 'b']).values())], ['a']);
  assert.equal(assignor.fallbackCount(), 0);
});

test('throwing provider never blocks the rebalance: rendezvous plus a counted sample-error', () => {
  const boom = new Error('metrics down');
  const seen: Array<{ reason: string; error?: unknown }> = [];
  const assignor = createLagAwarePartitionAssignor({
    memberLag: { a: 0, b: 50 },
    lagProvider: () => {
      throw boom;
    },
    onFallback: (reason, error) => {
      seen.push({ reason, error });
    },
  });
  const got = assignor.assign(8, ['a', 'b']);
  assert.deepEqual(got, rendezvousPartitionAssignor.assign(8, ['a', 'b']));
  assert.equal(assignor.fallbackCount(), 1);
  assert.equal(seen.length, 1);
  assert.equal(seen[0]?.reason, 'sample-error');
  assert.equal(seen[0]?.error, boom);
});

test('malformed provider sample degrades to rendezvous as a sample-error', () => {
  const assignor = createLagAwarePartitionAssignor({
    lagProvider: () => new Map([['a', -5]] as never),
  });
  const got = assignor.assign(8, ['a', 'b']);
  assert.deepEqual(got, rendezvousPartitionAssignor.assign(8, ['a', 'b']));
  assert.equal(assignor.fallbackCount(), 1);
});

test('sample covering no roster member counts as no-lag-data', () => {
  const assignor = createLagAwarePartitionAssignor({
    lagProvider: () => new Map([['ghost', 0]]),
  });
  const got = assignor.assign(8, ['a', 'b']);
  assert.deepEqual(got, rendezvousPartitionAssignor.assign(8, ['a', 'b']));
  assert.equal(assignor.fallbackCount(), 1);
});

test('throwing onFallback hook is swallowed and the assignment still lands', () => {
  const assignor = createLagAwarePartitionAssignor({
    onFallback: () => {
      throw new Error('hook down');
    },
  });
  const got = assignor.assign(4, ['a', 'b']);
  assert.deepEqual(got, rendezvousPartitionAssignor.assign(4, ['a', 'b']));
  assert.equal(assignor.fallbackCount(), 1);
});

test('resolveBuiltin resolves lag-aware with options and reports its name', () => {
  const assignor = resolveBuiltinPartitionAssignor('lag-aware', { memberLag: { a: 0, b: 50 } });
  assert.equal(assignor.name, 'lag-aware');
  assert.equal(assignor.fallbackCount(), 0);
  assert.deepEqual([...new Set(assignor.assign(4, ['a', 'b']).values())], ['a']);
});

test('lag-aware assignment is deterministic across calls', () => {
  const assignor = createLagAwarePartitionAssignor({
    memberLag: { a: 3, b: 3, c: 9 },
  });
  const first = assignor.assign(16, ['a', 'b', 'c']);
  const second = assignor.assign(16, ['a', 'b', 'c']);
  assert.deepEqual(first, second);
});
