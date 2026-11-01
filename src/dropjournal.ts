/**
 * Subscriber-queue drop journal (EB-225): a bounded in-memory ring of
 * per-message drop records for NON-reliable subscriber queues. Aggregate
 * counters (`getStats().droppedMessages`) say how many messages a queue
 * shed; they cannot say which ones. The journal answers that: every
 * backpressure shed, every backpressure refusal, and every drain-time
 * expiry of a non-reliable subscriber's queue leaves one metadata-only
 * row — topic, subscriber id, per-topic sequence number, reason, bus
 * clock timestamp — never the payload, so large or sensitive payloads
 * stay out of the audit trail.
 *
 * Reason vocabulary:
 * - `'evicted'`: the queue shed this message under `drop-oldest` (count
 *   or byte-budget pressure) to admit a newcomer.
 * - `'refused'`: the queue refused this message — `drop-newest`, the
 *   `'block'` tier, or a lower-precedence newcomer refused to protect
 *   higher-precedence backlog.
 * - `'ttl-expired'`: the message's TTL deadline passed before the drain
 *   could hand it to a handler.
 * - `'deadline-expired'`: the message's hard delivery deadline
 *   (`PublishOptions.deadlineAt`, EB-177) passed before the drain could
 *   hand it to a handler. TTL and hard deadline collapse into one
 *   `expiresAt` at fan-out (the earlier instant wins), so the split is
 *   exact only for messages whose fan-out the bus itself performed: the
 *   bus marks deadline-driven messages at fan-out, and every other
 *   drain expiry (replayed, bridged, or cluster-forwarded messages
 *   whose origin cause is not visible locally) reports as
 *   `'ttl-expired'`.
 *
 * Scope boundaries, all deliberate:
 * - Reliable subscribers are NOT journaled: their loss path is the
 *   dead-letter / ack machinery (EB-28), not the plain queue, and a
 *   journal row for a message the ack machinery still owes would read
 *   as a loss that never happened.
 * - Graceful-drain skips (EB-128), filter skips, and dedup duplicates
 *   are not drops and leave no row — the same accounting the aggregate
 *   counters use.
 * - Shadow subscribers are not journaled: their queues are mirrors,
 *   and their sheds already live in the shadow counters.
 *
 * The ring is always on (no opt-in flag): recording is one array push
 * per drop, and drops are rare relative to deliveries. The bound
 * (`EventBusOptions.dropJournalMaxEntries`, default 1000) evicts the
 * oldest rows first; `stats()` still reports the lifetime totals, so a
 * full ring never hides that drops happened — only which ones, oldest
 * first.
 */

export type DropReason = 'evicted' | 'refused' | 'ttl-expired' | 'deadline-expired';

/** One journal row: message identity plus the drop cause, nothing else. */
export interface DropJournalEntry {
  /** The concrete topic the message was published to (never a pattern). */
  topic: string;
  /** The subscriber whose queue dropped the message. */
  subscriberId: string;
  /** Per-topic sequence number of the dropped message. */
  seq: number;
  /** Why the queue dropped the message (see `DropReason`). */
  reason: DropReason;
  /** Bus-clock milliseconds of the drop. */
  at: number;
}

/** Query filter for `DropJournal.query` / `EventBus.getDropJournal`. */
export interface DropJournalFilter {
  /** Only rows for this subscriber. */
  subscriberId?: string;
  /** Only rows for this topic. */
  topic?: string;
  /** Only rows with this reason. */
  reason?: DropReason;
  /** Only rows at or after this bus-clock timestamp. */
  since?: number;
  /** Only rows at or before this bus-clock timestamp. */
  until?: number;
  /** At most this many rows, oldest first. */
  limit?: number;
}

/** Lifetime totals plus the ring's current fill, for `getStats().dropJournal`. */
export interface DropJournalStats {
  /** Rows recorded since bus construction, including rows since evicted from the ring. */
  total: number;
  /** Rows currently retained in the ring. */
  size: number;
  /** The ring's bound (`EventBusOptions.dropJournalMaxEntries`). */
  capacity: number;
  /** Lifetime totals per reason. */
  byReason: Record<DropReason, number>;
}

/** Default bound of the in-memory ring (oldest rows evicted first). */
export const DROP_JOURNAL_DEFAULT_MAX_ENTRIES = 1000;

/**
 * Validates `EventBusOptions.dropJournalMaxEntries` the way the
 * constructor would (EB-318): a positive integer. Throws `RangeError`
 * on an illegal value — before anything is mutated. Shared by the
 * constructor, `DropJournal.retune`, and `EventBus.setDropJournal`, so
 * a value that could not be set at construction cannot be revised in
 * later either.
 */
export function assertValidDropJournalMaxEntries(value: unknown): void {
  if (!Number.isInteger(value) || (value as number) < 1) {
    throw new RangeError('dropJournalMaxEntries must be a positive integer');
  }
}

const DROP_REASONS: readonly DropReason[] = ['evicted', 'refused', 'ttl-expired', 'deadline-expired'];

export class DropJournal {
  private capacity: number;
  private readonly entries: DropJournalEntry[] = [];
  private totalRecorded = 0;
  private readonly byReasonCount: Record<DropReason, number> = {
    evicted: 0,
    refused: 0,
    'ttl-expired': 0,
    'deadline-expired': 0,
  };

  constructor(maxEntries: number = DROP_JOURNAL_DEFAULT_MAX_ENTRIES) {
    assertValidDropJournalMaxEntries(maxEntries);
    this.capacity = maxEntries;
  }

  /**
   * Retunes the ring's retention bound live (EB-318). Only the bound
   * moves — the drop determination itself is untouched (drops are still
   * decided by each queue path; the journal only records them), and the
   * rows already recorded are retained, never cleared or rewritten:
   * shrinking does not evict immediately — the next `record` evicts the
   * oldest rows down to the live bound — while lifetime totals (`total`,
   * `byReason`) are unaffected by the revision. Validates fail-fast
   * with the construction-time rule, so an illegal value leaves the
   * live bound exactly as it was.
   */
  retune(maxEntries: number): void {
    assertValidDropJournalMaxEntries(maxEntries);
    this.capacity = maxEntries;
  }

  /** The current retention bound — the live value `retune` moves. */
  current(): number {
    return this.capacity;
  }

  /** Records one drop. Never stores the payload — identity only. */
  record(entry: DropJournalEntry): void {
    this.entries.push({ ...entry });
    // Evict down to the live bound, not just one row: after a retune
    // shrinks the bound the ring can hold several rows past it, and the
    // next record must bring it back to the bound in one step. With a
    // fixed bound this loop sheds exactly one row, as before.
    while (this.entries.length > this.capacity) this.entries.shift();
    this.totalRecorded += 1;
    this.byReasonCount[entry.reason] += 1;
  }

  /** Rows matching every set filter field, in recording order. */
  query(filter: DropJournalFilter = {}): DropJournalEntry[] {
    if (filter.limit !== undefined && (!Number.isInteger(filter.limit) || filter.limit < 1)) {
      throw new RangeError('dropJournal query limit must be a positive integer');
    }
    if (filter.since !== undefined && !Number.isFinite(filter.since)) {
      throw new RangeError('dropJournal query since must be a finite number');
    }
    if (filter.until !== undefined && !Number.isFinite(filter.until)) {
      throw new RangeError('dropJournal query until must be a finite number');
    }
    if (filter.reason !== undefined && !DROP_REASONS.includes(filter.reason)) {
      throw new RangeError(`dropJournal query reason must be one of ${DROP_REASONS.join(', ')}`);
    }
    const matched: DropJournalEntry[] = [];
    for (const entry of this.entries) {
      if (filter.subscriberId !== undefined && entry.subscriberId !== filter.subscriberId) continue;
      if (filter.topic !== undefined && entry.topic !== filter.topic) continue;
      if (filter.reason !== undefined && entry.reason !== filter.reason) continue;
      if (filter.since !== undefined && entry.at < filter.since) continue;
      if (filter.until !== undefined && entry.at > filter.until) continue;
      matched.push({ ...entry });
      if (filter.limit !== undefined && matched.length >= filter.limit) break;
    }
    return matched;
  }

  /** Lifetime totals plus the ring's current fill. */
  stats(): DropJournalStats {
    return {
      total: this.totalRecorded,
      size: this.entries.length,
      capacity: this.capacity,
      byReason: { ...this.byReasonCount },
    };
  }
}
