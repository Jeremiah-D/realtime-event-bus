/**
 * Sticky balanced partition assignment for partitioned consumer groups
 * (EB-54) — the cooperative alternative to rendezvous hashing.
 *
 * Rendezvous (highest-random-weight) assignment is minimal-disruption but
 * has no balance guarantee: 6 partitions over 3 members can land 3/2/1,
 * overloading one consumer while another idles. The sticky assignor
 * guarantees balance — every member owns `floor(P/N)` or `ceil(P/N)`
 * partitions — while keeping the maximum possible number of partitions on
 * their previous owners subject to that balance ("maximally sticky").
 *
 * Cooperative protocol property: a membership change migrates exactly the
 * minimal diff the balance requires. Partitions that keep their owner are
 * never touched — their consumer keeps consuming through the rebalance
 * with no pause, no replay, no fence; only revoked partitions hand over
 * to their new owner via the normal migration replay.
 *
 * The function is pure and deterministic: same
 * `{ partitions, members, previous }` always yields the same assignment,
 * so every node (and every test) agrees without coordination.
 * Tie-breaks are by ascending partition number and lexicographic member
 * id — no randomness, no hash input beyond the ids themselves.
 */

export interface StickyAssignmentInput {
  /** Total logical partitions; must be a positive integer. */
  partitions: number;
  /** Current member ids (roster). Empty yields an empty assignment. */
  members: string[];
  /**
   * Previous partition -> member assignment, e.g. the assignment in force
   * before the latest join/leave. Partitions whose previous owner is no
   * longer a member are treated as orphaned and redistributed for
   * balance. Omit for a fresh group.
   */
  previous?: ReadonlyMap<number, string> | Record<number, string> | undefined;
}

/**
 * Computes the sticky balanced assignment: partition -> owner member id.
 *
 * Algorithm:
 * 1. Sort the roster lexicographically (canonical order, deterministic).
 * 2. Target loads: `base = floor(P/N)`, the first `P % N` sorted members
 *    get `base + 1`. Every member's final load equals its target, so
 *    `max - min <= 1` by construction.
 * 3. Keep phase (ascending partition order): a partition stays with its
 *    previous owner when that owner is still a member and still below its
 *    target. Each member retains `min(target, stickyOwned)` of its
 *    previous partitions — the maximum any balanced assignment can
 *    retain, since a member must shed exactly `stickyOwned - target`
 *    partitions when it owned more than its target.
 * 4. Pool phase (ascending partition order): every unkept partition goes
 *    to the least-loaded member below its target, ties broken by sorted
 *    member order.
 *
 * Throws `RangeError` on a non-positive-integer partition count.
 */
export function stickyPartitionAssignment(input: StickyAssignmentInput): Map<number, string> {
  const { partitions } = input;
  if (!Number.isInteger(partitions) || partitions < 1) {
    throw new RangeError('partitions must be a positive integer');
  }
  const members = [...new Set(input.members)].sort();
  const assignment = new Map<number, string>();
  if (members.length === 0) return assignment;

  const previous = toPartitionMap(input.previous);
  const base = Math.floor(partitions / members.length);
  const extra = partitions % members.length;
  const target = new Map<string, number>();
  members.forEach((m, i) => target.set(m, base + (i < extra ? 1 : 0)));

  const load = new Map<string, number>();
  for (const m of members) load.set(m, 0);

  // Keep phase: maximally sticky under the balance targets.
  const pooled: number[] = [];
  for (let p = 0; p < partitions; p++) {
    const owner = previous.get(p);
    if (owner !== undefined && target.has(owner) && (load.get(owner) as number) < (target.get(owner) as number)) {
      assignment.set(p, owner);
      load.set(owner, (load.get(owner) as number) + 1);
    } else {
      pooled.push(p);
    }
  }
  // Pool phase: orphaned and balance-shed partitions fill targets.
  for (const p of pooled) {
    let chosen: string | undefined;
    for (const m of members) {
      if ((load.get(m) as number) < (target.get(m) as number)) {
        chosen = m;
        break;
      }
    }
    // Unreachable: sum(targets) == partitions == kept + pooled.
    if (chosen === undefined) {
      throw new Error('stickyPartitionAssignment: no member below target (unreachable)');
    }
    assignment.set(p, chosen);
    load.set(chosen, (load.get(chosen) as number) + 1);
  }
  return assignment;
}

function toPartitionMap(
  previous: ReadonlyMap<number, string> | Record<number, string> | undefined,
): Map<number, string> {
  if (previous == null) return new Map();
  if (previous instanceof Map) return previous;
  const out = new Map<number, string>();
  for (const [k, v] of Object.entries(previous)) {
    const p = Number(k);
    if (Number.isInteger(p) && p >= 0) out.set(p, v);
  }
  return out;
}
