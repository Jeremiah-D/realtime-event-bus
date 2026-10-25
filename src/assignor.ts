/**
 * Pluggable consumer-group partition assignors (EB-106).
 *
 * Kafka-style partitioned consumer groups (`GroupSubscribeOptions.partitions`,
 * EB-38) divide a competing set's logical partitions among its members.
 * Historically the assignment strategy was a closed string option —
 * `'rendezvous'` (default) or `'sticky'` (EB-54) — selected by the group's
 * first member. This module opens it up: a `PartitionAssignor` is any
 * object with a stable `name` and a pure `assign` function, so operators
 * can plug in rack-aware, weighted, or otherwise custom placement without
 * forking the bus.
 *
 * The two built-ins are adapted as `PartitionAssignor` implementations
 * with their exact historical semantics, so the existing
 * `'rendezvous'`/`'sticky'` string options keep behaving byte-for-byte as
 * before:
 *
 * - `rendezvousPartitionAssignor` (`'rendezvous'`): deterministic
 *   highest-random-weight hashing over the roster — minimal disruption
 *   on join/leave, no balance guarantee.
 * - `stickyPartitionAssignor` (`'sticky'`): deterministic sticky balanced
 *   assignment (`src/sticky.ts`) — every member owns `floor(P/N)` or
 *   `ceil(P/N)` partitions, maximally sticky across rebalances.
 *
 * A third built-in, the zone-aware assignor (EB-223), is constructed
 * with operator-supplied zone declarations —
 * `createZoneAwarePartitionAssignor({ memberZones })` — because zone
 * topology is operator data, not a bare string option: it cannot be
 * selected by the `assignment` string alone. It is otherwise a
 * first-class built-in with the same pure, deterministic contract, and
 * it is resolvable by name with its options via
 * `resolveBuiltinPartitionAssignor('zone-aware', options)`.
 * A fourth built-in, the lag-aware assignor (EB-231), is constructed
 * with per-member consumer lag —
 * `createLagAwarePartitionAssignor({ memberLag })` or with a
 * `lagProvider` sampled on every rebalance — because lag is live
 * operator data, not a bare string option: it cannot be selected by
 * the `assignment` string alone either. It is otherwise a first-class
 * built-in with the same contract, resolvable by name with its
 * options via `resolveBuiltinPartitionAssignor('lag-aware', options)`.
 * An assignor should be pure and deterministic: same
 * `{ partitions, members, previous }` must yield the same assignment, so
 * every computation agrees without coordination. `previous` carries the
 * assignment in force before the rebalance (absent for a fresh group);
 * sticky-style assignors use it to minimize migration.
 */

import { createHash } from 'node:crypto';
import { stickyPartitionAssignment } from './sticky.ts';

/**
 * A partition assignor: computes `partition -> owner member id` for the
 * logical partitions of one competing set.
 */
export interface PartitionAssignor {
  /**
   * Stable name, reported on `GroupRebalanceEvent.partitionRebalance`
   * and in the subscription audit log. The built-ins use
   * `'rendezvous'`/`'sticky'`; custom names must be non-empty strings.
   */
  readonly name: string;
  /**
   * Compute the assignment. `partitions` is the group's logical
   * partition count, `members` the current roster keys in join order
   * (instanceIds for static members, subscription ids otherwise),
   * `previous` the assignment in force before this rebalance.
   *
   * A throwing assignor propagates to the caller (fail-fast at member
   * join); an assignor that returns an invalid assignment — empty when
   * there is work to do, a missing partition, an unknown owner — is
   * rejected with `RangeError` by the bus.
   */
  assign(
    partitions: number,
    members: string[],
    previous?: ReadonlyMap<number, string>,
  ): Map<number, string>;
}

/**
 * Validates a user-supplied assignor object. Throws `RangeError` when it
 * is not an object with a non-empty string `name` and a function
 * `assign` — the fail-fast gate at `subscribeToGroup` registration.
 */
export function assertValidPartitionAssignor(
  raw: unknown,
  where: string,
): asserts raw is PartitionAssignor {
  if (typeof raw !== 'object' || raw === null) {
    throw new RangeError(`${where}: assignor must be an object with { name, assign }`);
  }
  const { name, assign } = raw as { name?: unknown; assign?: unknown };
  if (typeof name !== 'string' || name.length === 0) {
    throw new RangeError(`${where}: assignor.name must be a non-empty string`);
  }
  if (typeof assign !== 'function') {
    throw new RangeError(`${where}: assignor.assign must be a function`);
  }
}

/** Rendezvous (highest-random-weight) score of a member for a partition. */
function rendezvousScore(partition: number, memberId: string): bigint {
  const digest = createHash('sha256').update(`${partition}\0${memberId}`, 'utf8').digest();
  return digest.readBigUInt64BE(0);
}

/**
 * The historical `'rendezvous'` strategy as a `PartitionAssignor`:
 * highest-random-weight hashing over the roster. Deterministic from the
 * roster alone — a join/leave only changes the winners it actually
 * affects — but with no balance guarantee. Byte-for-byte the pre-EB-106
 * behavior.
 */
export const rendezvousPartitionAssignor: PartitionAssignor = {
  name: 'rendezvous',
  assign(partitions: number, members: string[]): Map<number, string> {
    const assignment = new Map<number, string>();
    for (let p = 0; p < partitions; p++) {
      let best: string | undefined;
      let bestScore = -1n;
      for (const m of members) {
        const score = rendezvousScore(p, m);
        // Strictly greater: roster order breaks the (astronomically
        // unlikely) tie deterministically.
        if (score > bestScore) {
          bestScore = score;
          best = m;
        }
      }
      if (best !== undefined) assignment.set(p, best);
    }
    return assignment;
  },
};

/**
 * The historical `'sticky'` strategy as a `PartitionAssignor`:
 * deterministic sticky balanced assignment — `floor(P/N)`/`ceil(P/N)`
 * per member, maximally sticky across rebalances (see `src/sticky.ts`).
 * Byte-for-byte the pre-EB-106 behavior.
 */
export const stickyPartitionAssignor: PartitionAssignor = {
  name: 'sticky',
  assign(
    partitions: number,
    members: string[],
    previous?: ReadonlyMap<number, string>,
  ): Map<number, string> {
    return stickyPartitionAssignment({ partitions, members, previous });
  },
};

/**
 * Zone declarations for the zone-aware assignor (EB-223).
 */
export interface ZoneAwareAssignorOptions {
  /**
   * Member id -> zone name. A member absent from the map declares no
   * zone and falls into the implicit unzoned pool (see
   * `zoneAwareAssignment`). Zone names must be non-empty and must not
   * contain NUL. Entries for members that never join are ignored, so
   * the map may cover the full deployment roster up front.
   */
  readonly memberZones: Readonly<Record<string, string>>;
  /**
   * Full deployment zone list. Defaults to the distinct zones in
   * `memberZones`. An explicitly listed zone with no live member is a
   * fallback candidate that is simply skipped — a zone whose members
   * all left (or never joined) never strands a partition. A live
   * member's declared zone is always ranked even when the list omits
   * it, so the list can only add zones, never remove a live member's.
   */
  readonly zones?: readonly string[];
}

/**
 * Sentinel zone for members that declare no zone. Zone names are
 * validated NUL-free, so a real zone can never collide with it.
 */
const UNZONED_ZONE = '\0unzoned';

/** Validates one zone name. Throws `RangeError` on a bad shape. */
function assertValidZoneName(zone: unknown, where: string): asserts zone is string {
  if (typeof zone !== 'string' || zone.length === 0 || zone.includes('\0')) {
    throw new RangeError(`${where}: zone names must be non-empty strings without NUL`);
  }
}

/**
 * Validates zone-aware options and returns the normalized form: the
 * declared map as a `Map` (own entries only, so a member id like
 * `'toString'` can never pick up an inherited property) and the
 * explicitly listed zones deduplicated in order.
 */
function normalizeZoneAwareOptions(
  options: ZoneAwareAssignorOptions,
): { declared: Map<string, string>; listedZones: string[] } {
  if (typeof options !== 'object' || options === null) {
    throw new RangeError('zone-aware assignor: options must be an object with { memberZones }');
  }
  const { memberZones, zones } = options;
  if (typeof memberZones !== 'object' || memberZones === null || Array.isArray(memberZones)) {
    throw new RangeError('zone-aware assignor: `memberZones` must be an object mapping member id -> zone');
  }
  const declared = new Map<string, string>();
  for (const [member, zone] of Object.entries(memberZones)) {
    assertValidZoneName(zone, `zone-aware assignor: zone of member '${member}'`);
    declared.set(member, zone);
  }
  const listedZones: string[] = [];
  if (zones !== undefined) {
    if (!Array.isArray(zones)) {
      throw new RangeError('zone-aware assignor: `zones` must be an array of zone names when set');
    }
    const seen = new Set<string>();
    for (const zone of zones) {
      assertValidZoneName(zone, 'zone-aware assignor: `zones` entry');
      if (!seen.has(zone)) {
        seen.add(zone);
        listedZones.push(zone);
      }
    }
  }
  return { declared, listedZones };
}

/** Zone-aware score of a zone for a partition (rendezvous over zones). */
function zoneScore(partition: number, zone: string): bigint {
  const digest = createHash('sha256').update(`${partition}\0zone\0${zone}`, 'utf8').digest();
  return digest.readBigUInt64BE(0);
}

/**
 * Zone-aware assignment (EB-223), as a pure function.
 *
 * Cross-zone traffic localization, without a coordinator: for each
 * partition, rank the zones by rendezvous score
 * (`sha256(partition + NUL + 'zone' + NUL + zone)`, ties broken by
 * zone name so the ranking is total and deterministic), then take the
 * highest-ranked zone that still has a live roster member and pick its
 * winner by the same rendezvous hashing used for members. A zone with
 * no live member is skipped — the cross-zone fallback — so a zone
 * emptying out (all members left, none joined yet) migrates its
 * partitions to the next-ranked zone instead of stranding them.
 *
 * Members with no zone declaration form one implicit unzoned pool that
 * ranks like any other zone. Consequences, both deliberate:
 *
 * - when every member is unzoned, the pool is the only zone and the
 *   result is byte-identical to the plain `'rendezvous'` assignment;
 * - in a mixed roster the unzoned pool competes for partitions on its
 *   hash ranking, so unzoned members still receive work instead of
 *   starving behind zoned ones.
 *
 * Like rendezvous, this assignor is stateless with respect to
 * `previous`: it is a pure function of the roster and the zone
 * topology, so every computation agrees without coordination. It is
 * orthogonal to both other built-ins — rendezvous has no zone
 * preference at all, and sticky's minimal-migration guarantee is a
 * different axis (a future sticky+zone combination would be a fourth
 * strategy, not a change to this one).
 */
export function zoneAwareAssignment(
  partitions: number,
  members: string[],
  options: ZoneAwareAssignorOptions,
): Map<number, string> {
  const assignment = new Map<number, string>();
  if (partitions <= 0 || members.length === 0) return assignment;
  const { declared, listedZones } = normalizeZoneAwareOptions(options);
  // Group the live roster by zone; undeclared members share the pool.
  const byZone = new Map<string, string[]>();
  for (const m of members) {
    const key = declared.get(m) ?? UNZONED_ZONE;
    const list = byZone.get(key);
    if (list === undefined) byZone.set(key, [m]);
    else list.push(m);
  }
  // Ranking universe: listed zones, every live member's zone, and the
  // unzoned pool when it has members. Zones with no live member are
  // ranked but skipped at pick time (the fallback).
  const universe = new Set<string>(listedZones);
  for (const zone of declared.values()) universe.add(zone);
  if (byZone.has(UNZONED_ZONE)) universe.add(UNZONED_ZONE);
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
      let best: string | undefined;
      let bestScore = -1n;
      for (const m of candidates) {
        const score = rendezvousScore(p, m);
        if (score > bestScore) {
          bestScore = score;
          best = m;
        }
      }
      if (best !== undefined) assignment.set(p, best);
      break;
    }
  }
  return assignment;
}

/**
 * The zone-aware built-in as a `PartitionAssignor` (EB-223), named
 * `'zone-aware'`. Options are validated eagerly, so a bad zone map
 * fails at construction — not at the first rebalance. Pass the result
 * as `GroupSubscribeOptions.assignor`; the group's first member fixes
 * it like any other assignor.
 */
export function createZoneAwarePartitionAssignor(
  options: ZoneAwareAssignorOptions,
): PartitionAssignor {
  normalizeZoneAwareOptions(options);
  return {
    name: 'zone-aware',
    assign(partitions: number, members: string[]): Map<number, string> {
      return zoneAwareAssignment(partitions, members, options);
    },
  };
}

/**
 * Why a member's lag fell back to rendezvous instead of steering the
 * assignment: `'no-lag-data'` when no usable lag sample exists at all
 * (no provider sample and no static map, or a sample that covers no
 * roster member), `'sample-error'` when the provider threw or returned
 * an unusable sample (not a map, or an entry that is not a finite
 * number `>= 0`). Either way the rebalance still happens — the
 * fallback is counted, never fatal.
 */
export type LagAwareFallbackReason = 'no-lag-data' | 'sample-error';

/**
 * Lag data for the lag-aware assignor (EB-231). Exactly one source is
 * needed; when both are given the provider is consulted first and the
 * static map is the fallback when the provider returns `undefined` or
 * an empty map (a throwing or malformed provider sample does not fall
 * back to the static map — it is a sample error, see below).
 */
export interface LagAwareAssignorOptions {
  /**
   * Static per-member lag snapshot (messages behind, `>= 0`):
   * member id -> current consumer lag. Entries for members that never
   * join are ignored, so the map may cover the full deployment
   * roster up front. The values are conceptually the EB-48 group-lag
   * rows attributed to members by the operator — the assignor itself
   * never reads the bus, it only consumes the numbers it is given.
   * Validated eagerly: a negative, non-finite, or non-numeric entry
   * throws `RangeError` at construction.
   */
  readonly memberLag?: Readonly<Record<string, number>>;
  /**
   * Live lag source, consulted on every `assign` call: returns the
   * current member id -> lag map, or `undefined` when no fresh sample
   * is available (the static map, when given, is then used). A
   * throwing provider, or a sample that is not a map of finite
   * numbers `>= 0`, is a sample error: the assignment degrades to
   * rendezvous for that rebalance and the fallback is counted — it
   * never blocks the rebalance and never throws out of `assign`.
   */
  readonly lagProvider?: () => ReadonlyMap<string, number> | undefined;
  /**
   * Best-effort fallback hook, called once per degraded assignment
   * with the reason (and the provider's error, for `'sample-error'`).
   * A throwing hook is swallowed: monitoring must never break the
   * rebalance path.
   */
  readonly onFallback?: (reason: LagAwareFallbackReason, error?: unknown) => void;
}

/**
 * The lag-aware built-in as a `PartitionAssignor`, with its fallback
 * accounting: `fallbackCount()` reports how many assignments so far
 * degraded to rendezvous (no lag data, or a failed lag sample).
 */
export interface LagAwarePartitionAssignor extends PartitionAssignor {
  readonly name: 'lag-aware';
  /** Assignments so far that degraded to rendezvous (see `LagAwareFallbackReason`). */
  fallbackCount(): number;
}

/**
 * Validates lag-aware options and returns the normalized form: the
 * static map as a `Map` (own entries only, so a member id like
 * `'toString'` can never pick up an inherited property) plus the
 * provider and fallback hook untouched. Throws `RangeError` on a bad
 * shape or a bad static lag value — eagerly, so a bad map fails at
 * construction, not at the first rebalance.
 */
function normalizeLagAwareOptions(
  options: LagAwareAssignorOptions,
): {
  staticLag: Map<string, number>;
  lagProvider?: () => ReadonlyMap<string, number> | undefined;
  onFallback?: (reason: LagAwareFallbackReason, error?: unknown) => void;
} {
  if (typeof options !== 'object' || options === null || Array.isArray(options)) {
    throw new RangeError('lag-aware assignor: options must be an object');
  }
  const { memberLag, lagProvider, onFallback } = options;
  const staticLag = new Map<string, number>();
  if (memberLag !== undefined) {
    if (typeof memberLag !== 'object' || memberLag === null || Array.isArray(memberLag)) {
      throw new RangeError('lag-aware assignor: `memberLag` must be an object mapping member id -> lag');
    }
    for (const [member, value] of Object.entries(memberLag)) {
      if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
        throw new RangeError(
          `lag-aware assignor: lag of member '${member}' must be a finite number >= 0`,
        );
      }
      staticLag.set(member, value);
    }
  }
  if (lagProvider !== undefined && typeof lagProvider !== 'function') {
    throw new RangeError('lag-aware assignor: `lagProvider` must be a function when set');
  }
  if (onFallback !== undefined && typeof onFallback !== 'function') {
    throw new RangeError('lag-aware assignor: `onFallback` must be a function when set');
  }
  return { staticLag, lagProvider, onFallback };
}

/**
 * Validates one provider sample. Returns the sample as a `Map` when
 * every entry is a string member id mapped to a finite number `>= 0`,
 * and `undefined` when the sample is unusable — not a map, or any bad
 * entry — so the caller can degrade to rendezvous instead of trusting
 * a half-valid sample.
 */
function validateLagSample(
  sample: ReadonlyMap<string, number>,
): Map<string, number> | undefined {
  if (!(sample instanceof Map)) return undefined;
  const out = new Map<string, number>();
  for (const [member, value] of sample) {
    if (typeof member !== 'string' || typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
      return undefined;
    }
    out.set(member, value);
  }
  return out;
}

/**
 * Lag-aware assignment (EB-231), as a pure function.
 *
 * Per partition, the member with the lowest current consumer lag
 * wins — a member falling behind stops receiving new partitions
 * until it catches up, which is the point: new work goes to the
 * members that are keeping up. Lag ties are broken by the same
 * rendezvous hashing used for members, so the tie-break is total
 * and deterministic, and when every member's lag is equal the result
 * is byte-identical to the plain `'rendezvous'` assignment.
 *
 * Members with no lag entry rank at the worst observed lag: they
 * still compete (via the rendezvous tie-break against the worst-lag
 * members) instead of starving behind sampled members or
 * monopolizing work as an assumed zero. When no roster member has a
 * lag entry at all there is nothing to steer by, so the assignment
 * is the plain rendezvous assignment — the same fallback the
 * factory counts as `'no-lag-data'`.
 *
 * Like rendezvous and zone-aware, this assignor is stateless with
 * respect to `previous`: it is a pure function of the roster and
 * the lag snapshot, so every computation agrees without
 * coordination. It is orthogonal to both other built-ins and to
 * the EB-112 lag SLO: the SLO is alerting on lag, this is
 * placement steered by lag, and sticky's minimal-migration
 * guarantee is a different axis (a future sticky+lag combination
 * would be a fifth strategy, not a change to this one).
 */
export function lagAwareAssignment(
  partitions: number,
  members: string[],
  lag: ReadonlyMap<string, number>,
): Map<number, string> {
  const assignment = new Map<number, string>();
  if (partitions <= 0 || members.length === 0) return assignment;
  let worst = -1;
  for (const m of members) {
    const value = lag.get(m);
    if (value !== undefined && value > worst) worst = value;
  }
  if (worst < 0) {
    // No roster member has lag data: nothing to steer by.
    return rendezvousPartitionAssignor.assign(partitions, members);
  }
  for (let p = 0; p < partitions; p++) {
    let best: string | undefined;
    let bestLag = Infinity;
    let bestScore = -1n;
    for (const m of members) {
      const memberLag = lag.get(m) ?? worst;
      const score = rendezvousScore(p, m);
      // Lowest lag wins; ties go to the highest rendezvous score,
      // roster order breaks the (astronomically unlikely) remainder.
      if (memberLag < bestLag || (memberLag === bestLag && score > bestScore)) {
        best = m;
        bestLag = memberLag;
        bestScore = score;
      }
    }
    if (best !== undefined) assignment.set(p, best);
  }
  return assignment;
}

/**
 * The lag-aware built-in as a `PartitionAssignor` (EB-231), named
 * `'lag-aware'`. Options are validated eagerly, so a bad static map
 * fails at construction — not at the first rebalance. A provider
 * sample failure never fails the rebalance: the assignment for that
 * rebalance degrades to rendezvous, `fallbackCount()` increments,
 * and `onFallback` fires (best-effort). Pass the result as
 * `GroupSubscribeOptions.assignor`; the group's first member fixes
 * it like any other assignor.
 */
export function createLagAwarePartitionAssignor(
  options: LagAwareAssignorOptions,
): LagAwarePartitionAssignor {
  const { staticLag, lagProvider, onFallback } = normalizeLagAwareOptions(options);
  let fallbacks = 0;
  const degrade = (
    reason: LagAwareFallbackReason,
    partitions: number,
    members: string[],
    error?: unknown,
  ): Map<number, string> => {
    fallbacks += 1;
    if (onFallback !== undefined) {
      try {
        onFallback(reason, error);
      } catch {
        // Monitoring is best-effort: a throwing hook must not
        // propagate into the rebalance path.
      }
    }
    return rendezvousPartitionAssignor.assign(partitions, members);
  };
  return {
    name: 'lag-aware',
    fallbackCount: () => fallbacks,
    assign(partitions: number, members: string[]): Map<number, string> {
      if (partitions <= 0 || members.length === 0) return new Map<number, string>();
      let sample: Map<string, number> | undefined;
      if (lagProvider !== undefined) {
        try {
          const provided = lagProvider();
          if (provided !== undefined) {
            sample = validateLagSample(provided);
            if (sample === undefined) {
              return degrade('sample-error', partitions, members);
            }
          }
        } catch (err) {
          return degrade('sample-error', partitions, members, err);
        }
      }
      if (sample === undefined || sample.size === 0) {
        sample = staticLag.size > 0 ? staticLag : undefined;
      }
      if (sample === undefined) {
        return degrade('no-lag-data', partitions, members);
      }
      if (!members.some((m) => sample.has(m))) {
        return degrade('no-lag-data', partitions, members);
      }
      return lagAwareAssignment(partitions, members, sample);
    },
  };
}

/** Resolves one of the built-in strategy names to its assignor. */
export function resolveBuiltinPartitionAssignor(
  name: 'rendezvous' | 'sticky',
): PartitionAssignor;
export function resolveBuiltinPartitionAssignor(
  name: 'zone-aware',
  options: ZoneAwareAssignorOptions,
): PartitionAssignor;
export function resolveBuiltinPartitionAssignor(
  name: 'lag-aware',
  options: LagAwareAssignorOptions,
): LagAwarePartitionAssignor;
export function resolveBuiltinPartitionAssignor(
  name: 'rendezvous' | 'sticky' | 'zone-aware' | 'lag-aware',
  options?: ZoneAwareAssignorOptions | LagAwareAssignorOptions,
): PartitionAssignor {
  if (name === 'sticky') return stickyPartitionAssignor;
  if (name === 'zone-aware') {
    if (options === undefined) {
      throw new RangeError(
        `resolveBuiltinPartitionAssignor: 'zone-aware' requires zone declarations — pass { memberZones } as the second argument (or use createZoneAwarePartitionAssignor)`,
      );
    }
    return createZoneAwarePartitionAssignor(options as ZoneAwareAssignorOptions);
  }
  if (name === 'lag-aware') {
    if (options === undefined) {
      throw new RangeError(
        `resolveBuiltinPartitionAssignor: 'lag-aware' requires lag data — pass { memberLag } or { lagProvider } as the second argument (or use createLagAwarePartitionAssignor)`,
      );
    }
    return createLagAwarePartitionAssignor(options as LagAwareAssignorOptions);
  }
  return rendezvousPartitionAssignor;
}

/**
 * Validates an assignor's result. Throws `RangeError` when the result is
 * not a `Map`, is empty while there are partitions and members to
 * assign, misses a partition in `[0, partitions)`, carries a key outside
 * that range, or names an owner outside the roster. The fail-fast gate
 * the bus applies to every (re)computation, so a broken custom assignor
 * can never strand a group with a partial assignment.
 */
export function assertValidPartitionAssignment(
  assignment: unknown,
  partitions: number,
  members: string[],
  assignorName: string,
): asserts assignment is Map<number, string> {
  const where = `partition assignor '${assignorName}'`;
  if (!(assignment instanceof Map)) {
    throw new RangeError(`${where} must return a Map<partition, memberId>`);
  }
  if (partitions === 0 || members.length === 0) {
    if (assignment.size !== 0) {
      throw new RangeError(
        `${where} must return an empty assignment when there are no partitions or no members`,
      );
    }
    return;
  }
  if (assignment.size === 0) {
    throw new RangeError(
      `${where} returned an empty assignment for ${partitions} partitions and ${members.length} members`,
    );
  }
  const roster = new Set(members);
  for (const key of assignment.keys()) {
    if (!Number.isInteger(key) || key < 0 || key >= partitions) {
      throw new RangeError(`${where} assigned out-of-range partition ${String(key)}`);
    }
  }
  for (let p = 0; p < partitions; p++) {
    if (!assignment.has(p)) {
      throw new RangeError(`${where} did not assign partition ${p}`);
    }
    const owner = assignment.get(p);
    if (typeof owner !== 'string' || !roster.has(owner)) {
      throw new RangeError(`${where} assigned partition ${p} to unknown member ${String(owner)}`);
    }
  }
}
