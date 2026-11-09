/**
 * Fencing generations for partitioned consumer groups (EB-441).
 *
 * A partitioned competing set can have two owners for one partition
 * across a rebalance: the member that just lost the partition still
 * holds its in-flight deliveries and its last checkpoint, while the
 * new owner is already consuming. Without a fence both write offsets
 * for the same partition and the old owner's late work lands on top of
 * the new owner's — the split-brain double-delivery Kafka's fencing
 * token exists to prevent.
 *
 * Every (re)computation of a group's assignment table produces one
 * monotonically increasing fencing generation (see
 * `EventBus.partitionAssignment`): the first assignment is generation
 * 1 and every rebalance/revocation bumps it by one. The bus stamps
 * every partitioned group delivery with the generation live at
 * enqueue time, and the drain gate drops a queued delivery whose
 * stamp no longer matches the live generation instead of handing it
 * to the handler — the new owner replays the partition's uncommitted
 * backlog from the watermark, so the drop loses nothing. A member
 * that commits a per-partition offset (`commitOffset` with
 * `{ partition, generation }`) presents the generation it was served
 * under; a mismatch is rejected and audited as
 * `stale_partition_generation`.
 *
 * Generations live only in the assignment table's memory state: a
 * rebalance is a memory-state recomputation and a restart reassigns
 * from scratch, so nothing is journaled. A fully drained group's
 * generation is retained (not reset): a recreated group continues the
 * sequence, so a stale generation from the dead generation can never
 * pass as fresh.
 *
 * Staleness is an equality check against the live generation, never a
 * `<` comparison — so the wrap below cannot make a stale stamp look
 * fresh by ordering. The one residual wrap hazard is inherent to any
 * bounded token: a delivery stamped generation 1 that is still queued
 * after 2^53 - 1 rebalances would read as fresh again. That needs a
 * single message to outlive nine quadrillion rebalances; the counter
 * is wide enough that the hazard is theoretical, and it is documented
 * here rather than papered over with an epoch bit.
 */

/** First fencing generation: a group's first assignment. */
export const FIRST_PARTITION_GENERATION = 1;

/**
 * The generation after `current`. Wraps from
 * `Number.MAX_SAFE_INTEGER` back to 1 — never to 0, so the sequence
 * never regresses and 0 keeps its "no generation" meaning.
 */
export function nextPartitionGeneration(current: number): number {
  return current >= Number.MAX_SAFE_INTEGER ? FIRST_PARTITION_GENERATION : current + 1;
}

/**
 * A partitioned group delivery's fencing stamp: the competing set and
 * partition it was assigned under, and the generation live when the
 * bus stamped it at enqueue time.
 */
export interface PartitionFencingStamp {
  /** Competing-set key (`groupId\0pattern`), as the bus keys groups. */
  groupKey: string;
  /** Logical partition the message was assigned to. */
  partition: number;
  /** Generation live at stamp time. */
  generation: number;
}
