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

/** Resolves one of the two built-in strategy names to its assignor. */
export function resolveBuiltinPartitionAssignor(
  name: 'rendezvous' | 'sticky',
): PartitionAssignor {
  return name === 'sticky' ? stickyPartitionAssignor : rendezvousPartitionAssignor;
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
