import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';
import {
  assertValidPartitionAssignment,
  createZoneAwarePartitionAssignor,
  rendezvousPartitionAssignor,
  resolveBuiltinPartitionAssignor,
  zoneAwareAssignment,
  type ZoneAwareAssignorOptions,
} from '../src/assignor.ts';

/**
 * Independent recomputation of the documented zone-aware semantics,
 * written from the spec (not imported from the implementation):
 * per-partition zone ranking by sha256(partition + NUL + 'zone' +
 * NUL + zone), then the rendezvous winner inside the top-ranked
 * non-empty zone; unzoned members share one implicit pool.
 */
const UNZONED = '\0unzoned';
const zoneScore = (p: number, zone: string): bigint =>
  createHash('sha256').update(`${p}\0zone\0${zone}`, 'utf8').digest().readBigUInt64BE(0);
const memberScore = (p: number, member: string): bigint =>
  createHash('sha256').update(`${p}\0${member}`, 'utf8').digest().readBigUInt64BE(0);

function expectedAssignment(
  partitions: number,
  members: string[],
  options: ZoneAwareAssignorOptions,
): Map<number, string> {
  const declared = new Map(Object.entries(options.memberZones));
  const byZone = new Map<string, string[]>();
  for (const m of members) {
    const key = declared.get(m) ?? UNZONED;
    const list = byZone.get(key) ?? [];
    list.push(m);
    byZone.set(key, list);
  }
  const universe = new Set<string>(options.zones ?? []);
  for (const z of declared.values()) universe.add(z);
  if (byZone.has(UNZONED)) universe.add(UNZONED);
  const out = new Map<number, string>();
  for (let p = 0; p < partitions; p++) {
    const ranked = [...universe].sort((a, b) => {
      const sa = zoneScore(p, a);
      const sb = zoneScore(p, b);
      if (sa !== sb) return sa > sb ? -1 : 1;
      return a < b ? -1 : a > b ? 1 : 0;
    });
    for (const zone of ranked) {
      const candidates = byZone.get(zone);
      if (candidates === undefined || candidates.length === 0) continue;
      let best = candidates[0];
      for (const m of candidates) {
        if (memberScore(p, m) > memberScore(p, best)) best = m;
      }
      out.set(p, best);
      break;
    }
  }
  return out;
}

// --- factory validation ---

test('zone-aware factory rejects malformed options with RangeError', () => {
  assert.throws(() => createZoneAwarePartitionAssignor(undefined as never), RangeError);
  assert.throws(() => createZoneAwarePartitionAssignor(null as never), RangeError);
  assert.throws(
    () => createZoneAwarePartitionAssignor({ memberZones: [] as never }),
    RangeError,
  );
  assert.throws(
    () => createZoneAwarePartitionAssignor({ memberZones: { a: '' } }),
    RangeError,
  );
  assert.throws(
    () => createZoneAwarePartitionAssignor({ memberZones: { a: 'zone\0x' } }),
    RangeError,
  );
  assert.throws(
    () => createZoneAwarePartitionAssignor({ memberZones: {}, zones: 'zone-a' as never }),
    RangeError,
  );
  assert.throws(
    () => createZoneAwarePartitionAssignor({ memberZones: {}, zones: [''] }),
    RangeError,
  );
  assert.throws(
    () => createZoneAwarePartitionAssignor({ memberZones: {}, zones: ['zone\0a'] }),
    RangeError,
  );
});

// --- all-unzoned reduces to rendezvous ---

test('all-unzoned roster is byte-identical to rendezvous', () => {
  const cases: Array<[number, string[]]> = [
    [1, ['a']],
    [6, ['a', 'b', 'c']],
    [12, ['m1', 'm2']],
    [20, ['x', 'y', 'z', 'w']],
  ];
  for (const [partitions, members] of cases) {
    const zoneAware = zoneAwareAssignment(partitions, members, { memberZones: {} });
    const rendezvous = rendezvousPartitionAssignor.assign(partitions, members);
    assert.deepEqual(zoneAware, rendezvous, `partitions=${partitions} members=${members}`);
  }
});

// --- empty inputs ---

test('zone-aware returns an empty assignment when there is nothing to assign', () => {
  assert.equal(
    zoneAwareAssignment(0, ['a'], { memberZones: { a: 'zone-a' } }).size,
    0,
  );
  assert.equal(
    zoneAwareAssignment(4, [], { memberZones: {} }).size,
    0,
  );
});

// --- determinism ---

test('zone-aware assignment is deterministic across calls and factories', () => {
  const options: ZoneAwareAssignorOptions = {
    memberZones: { a: 'zone-a', b: 'zone-b', c: 'zone-a' },
    zones: ['zone-a', 'zone-b', 'zone-c'],
  };
  const first = zoneAwareAssignment(12, ['a', 'b', 'c'], options);
  const second = zoneAwareAssignment(12, ['a', 'b', 'c'], options);
  assert.deepEqual(first, second);
  const viaFactory = createZoneAwarePartitionAssignor(options).assign(12, ['a', 'b', 'c']);
  assert.deepEqual(viaFactory, first);
  const viaOtherFactory = createZoneAwarePartitionAssignor({
    memberZones: { a: 'zone-a', b: 'zone-b', c: 'zone-a' },
    zones: ['zone-a', 'zone-b', 'zone-c'],
  }).assign(12, ['a', 'b', 'c']);
  assert.deepEqual(viaOtherFactory, first);
});

// --- locality + fallback, checked against the independent recomputation ---

test('zoned roster matches the independent zone-ranking recomputation', () => {
  const options: ZoneAwareAssignorOptions = {
    memberZones: { a: 'zone-a', b: 'zone-b', c: 'zone-a', d: 'zone-c' },
    zones: ['zone-a', 'zone-b', 'zone-c'],
  };
  const members = ['a', 'b', 'c', 'd'];
  const actual = zoneAwareAssignment(24, members, options);
  assert.deepEqual(actual, expectedAssignment(24, members, options));
  assert.doesNotThrow(() => assertValidPartitionAssignment(actual, 24, members, 'zone-aware'));
});

test('a zone with no live member falls back instead of stranding partitions', () => {
  // zone-a is listed but has no live member: every partition must land
  // on a zone-b member, and the assignment must stay complete.
  const options: ZoneAwareAssignorOptions = {
    memberZones: { a: 'zone-a', b: 'zone-b' },
    zones: ['zone-a', 'zone-b'],
  };
  const members = ['b'];
  const actual = zoneAwareAssignment(8, members, options);
  assert.deepEqual(actual, expectedAssignment(8, members, options));
  for (const owner of actual.values()) assert.equal(owner, 'b');
  assert.doesNotThrow(() => assertValidPartitionAssignment(actual, 8, members, 'zone-aware'));
});

test('mixed roster: unzoned members still receive partitions', () => {
  const options: ZoneAwareAssignorOptions = {
    memberZones: { a: 'zone-a', b: 'zone-b' },
  };
  const members = ['a', 'b', 'u1', 'u2'];
  const actual = zoneAwareAssignment(64, members, options);
  assert.deepEqual(actual, expectedAssignment(64, members, options));
  assert.doesNotThrow(() => assertValidPartitionAssignment(actual, 64, members, 'zone-aware'));
  // Over 64 partitions the unzoned pool must win at least one ranking —
  // otherwise the pool would be starving behind zoned members.
  const owners = new Set(actual.values());
  assert.ok(owners.has('u1') || owners.has('u2'), `owners=${[...owners]}`);
});

test('a member id that shadows an Object prototype key is still unzoned', () => {
  const actual = zoneAwareAssignment(4, ['toString'], { memberZones: {} });
  const rendezvous = rendezvousPartitionAssignor.assign(4, ['toString']);
  assert.deepEqual(actual, rendezvous);
});

// --- resolver ---

test('resolver returns zone-aware with options and fails fast without them', () => {
  const options: ZoneAwareAssignorOptions = { memberZones: { a: 'zone-a' } };
  const resolved = resolveBuiltinPartitionAssignor('zone-aware', options);
  assert.equal(resolved.name, 'zone-aware');
  assert.deepEqual(resolved.assign(2, ['a']), zoneAwareAssignment(2, ['a'], options));
  assert.throws(
    () => (resolveBuiltinPartitionAssignor as (name: string) => unknown)('zone-aware'),
    RangeError,
  );
  assert.equal(resolveBuiltinPartitionAssignor('rendezvous').name, 'rendezvous');
  assert.equal(resolveBuiltinPartitionAssignor('sticky').name, 'sticky');
});

// --- bus integration ---

test('bus rebalance events report the zone-aware strategy name', () => {
  const bus = new EventBus();
  const events: GroupRebalanceEvent[] = [];
  const assignor = createZoneAwarePartitionAssignor({
    memberZones: { 'm-a': 'zone-a', 'm-b': 'zone-b' },
  });
  const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
    partitions: 4,
    assignor,
    instanceId: 'm-a',
    onRebalance: (e) => events.push(e),
  });
  const m2 = bus.subscribeToGroup('g', 't.*', () => {}, {
    partitions: 4,
    assignor,
    instanceId: 'm-b',
    onRebalance: (e) => events.push(e),
  });
  assert.ok(events.length > 0);
  for (const e of events) {
    assert.equal(e.partitionRebalance?.strategy, 'zone-aware');
  }
  const expected = zoneAwareAssignment(4, ['m-a', 'm-b'], {
    memberZones: { 'm-a': 'zone-a', 'm-b': 'zone-b' },
  });
  const actual = bus.getPartitionAssignment('g', 't.*');
  for (const [p, owner] of expected) {
    assert.equal(actual[p], owner);
  }
  m1.unsubscribe();
  m2.unsubscribe();
});
