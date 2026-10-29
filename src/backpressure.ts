import { deficitRoundRobinSelect } from './fair.ts';

export type DropPolicy = 'drop-oldest' | 'drop-newest';

/**
 * Slow-consumer backpressure tier (EB-83): what the bus does when a
 * subscriber's bounded queue is full at enqueue time.
 *
 * - `'drop-oldest'`: shed the oldest queued entry to admit the newcomer —
 *   the newcomer is always accepted, the shed is counted.
 * - `'drop-newest'`: refuse the newcomer; the queued backlog is untouched.
 * - `'block'` (default): admit-with-shed on first saturation (exactly the
 *   historical default behavior), then *refuse* further arrivals while the
 *   subscriber stays saturated — the already-queued backlog is preserved
 *   instead of churned, like a closed TCP receive window. The subscriber
 *   unblocks when its queue recedes below the high-water mark.
 */
export type BackpressurePolicy = 'drop-oldest' | 'drop-newest' | 'block';

/** Validates a backpressure tier; throws `RangeError` on illegal values. */
export function assertValidBackpressurePolicy(
  value: unknown,
  where: string,
): asserts value is BackpressurePolicy {
  if (value !== 'drop-oldest' && value !== 'drop-newest' && value !== 'block') {
    throw new RangeError(
      `${where}: backpressure must be 'drop-oldest', 'drop-newest' or 'block'`,
    );
  }
}

/**
 * Message priority for shed decisions. Higher values are shed later:
 * when the queue is full under `drop-oldest`, the oldest entry with the
 * lowest priority is dropped first. The default priority is 0.
 */
export type Priority = number;

export interface BoundedQueueOptions<T> {
  capacity: number;
  policy?: DropPolicy;
  /** Fired once when size >= the high-water mark; re-arms after the size recedes below the mark. */
  onHighWaterMark?: (size: number) => void;
  /**
   * Fired once when the queue recedes below the high-water mark after an
   * excursion — the consumer-may-resume signal. Never fires without a
   * preceding `onHighWaterMark`; re-arms together with it.
   */
  onDrained?: (size: number) => void;
  /**
   * High-water mark as a fraction of capacity (default 0.8). Must be in
   * (0, 1]. Adjustable at runtime via `setHighWaterMarkRatio`.
   */
  highWaterMarkRatio?: number;
  /**
   * Opt-in byte budget for the queued items, in bytes. When set, the queue
   * is bounded by both `capacity` (count) and `maxBytes` (bytes): an item
   * whose admission would push the buffered total over `maxBytes` sheds
   * entries per `policy` until it fits. `drop-newest` discards the incoming
   * item; `drop-oldest` evicts the oldest lowest-priority entries (the
   * incoming item itself when it is strictly the lowest priority). An item
   * larger than the entire budget is never admitted — with `drop-oldest`
   * there is nothing to evict for it. Must be a positive finite number
   * when given. Byte-budget evictions count into `droppedCount` (and
   * `droppedByPriority`) exactly like count evictions.
   */
  maxBytes?: number;
  /**
   * Estimates an item's buffered byte size. Defaults to the item's JSON
   * UTF-8 byte length (unserializable items estimate as 0). Must return a
   * non-negative finite number — a push whose estimate is not finite or
   * negative throws `RangeError`.
   */
  byteSize?: (item: T) => number;
}

interface QueueEntry<T> {
  item: T;
  priority: Priority;
  bytes: number;
  /**
   * Absolute expiry timestamp in milliseconds (same clock as the caller's
   * `nowMs` argument to `drainLive`). Entries without one never expire.
   */
  expiresAt?: number;
}

/** Default byte estimator: JSON UTF-8 byte length, 0 when unserializable. */
function defaultByteSize(item: unknown): number {
  try {
    const serialized = JSON.stringify(item);
    return serialized === undefined ? 0 : Buffer.byteLength(serialized, 'utf8');
  } catch {
    return 0;
  }
}

export class BoundedQueue<T> {
  private entries: QueueEntry<T>[] = [];
  private capacity: number;
  private readonly policy: DropPolicy;
  private readonly onHighWaterMark?: (size: number) => void;
  private readonly onDrained?: (size: number) => void;
  private highWaterMarkFraction: number;
  private highWaterMarkReached = false;
  private dropped = 0;
  private readonly droppedByPriorityCount = new Map<Priority, number>();
  private expired = 0;
  private readonly maxBytes?: number;
  private readonly byteSize: (item: T) => number;
  private bytesTotal = 0;

  constructor(options: BoundedQueueOptions<T>) {
    if (!Number.isInteger(options.capacity) || options.capacity <= 0) {
      throw new RangeError('capacity must be a positive integer');
    }
    this.capacity = options.capacity;
    this.policy = options.policy ?? 'drop-oldest';
    this.onHighWaterMark = options.onHighWaterMark;
    this.onDrained = options.onDrained;
    this.highWaterMarkFraction = options.highWaterMarkRatio ?? 0.8;
    BoundedQueue.checkRatio(this.highWaterMarkFraction);
    if (options.maxBytes !== undefined) {
      if (!Number.isFinite(options.maxBytes) || options.maxBytes <= 0) {
        throw new RangeError('maxBytes must be a positive finite number of bytes');
      }
      this.maxBytes = options.maxBytes;
    }
    if (options.byteSize !== undefined && typeof options.byteSize !== 'function') {
      throw new TypeError('byteSize must be a function');
    }
    this.byteSize = options.byteSize ?? defaultByteSize;
  }

  private static checkRatio(ratio: number): void {
    if (!Number.isFinite(ratio) || ratio <= 0 || ratio > 1) {
      throw new RangeError('highWaterMarkRatio must be a finite number in (0, 1]');
    }
  }

  get size(): number {
    return this.entries.length;
  }

  /**
   * Estimated buffered bytes across the queued entries (see
   * `BoundedQueueOptions.maxBytes` / `byteSize`). Always 0 when no byte
   * budget is configured — the queue does not measure items it does not
   * need to budget.
   */
  get queueBytes(): number {
    return this.bytesTotal;
  }

  get droppedCount(): number {
    return this.dropped;
  }

  /** Total entries discarded by `drainLive` because their TTL expired. */
  get expiredCount(): number {
    return this.expired;
  }

  /** Per-priority drop counts, useful for backpressure observability. */
  get droppedByPriority(): ReadonlyMap<Priority, number> {
    return this.droppedByPriorityCount;
  }

  /** High-water mark as a fraction of capacity, in (0, 1]. */
  get highWaterMarkRatio(): number {
    return this.highWaterMarkFraction;
  }

  /** Absolute high-water mark in items: `capacity * highWaterMarkRatio`. */
  get highWaterMark(): number {
    return this.capacity * this.highWaterMarkFraction;
  }

  /**
   * Adjusts the high-water mark at runtime. If an excursion is in flight
   * and the queue is already below the new mark, the excursion ends
   * immediately: the latch clears and `onDrained` fires synchronously with
   * the current size. Throws `RangeError` for values outside (0, 1].
   */
  setHighWaterMarkRatio(ratio: number): void {
    BoundedQueue.checkRatio(ratio);
    this.highWaterMarkFraction = ratio;
    this.maybeFireDrained();
  }

  /**
   * Adjusts the count capacity at runtime (EB-281, via
   * `EventBus.setQueueSize`). Queued entries are untouched — shrinking
   * below the current size evicts nothing; the new cap governs only
   * future `push` calls, which shed or refuse per `policy` once the
   * size reaches it. Growing above the current size admits immediately,
   * and if an excursion is in flight and the queue is already below
   * the new high-water mark, the excursion ends immediately (the
   * `setHighWaterMarkRatio` precedent). Throws `RangeError` for a
   * non-positive-integer capacity.
   */
  setCapacity(capacity: number): void {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError('capacity must be a positive integer');
    }
    this.capacity = capacity;
    this.maybeFireDrained();
  }

  /**
   * Ends a high-water-mark excursion when the queue has receded below the
   * mark: clears the latch and fires `onDrained` once. No-op when no
   * excursion is in flight.
   */
  private maybeFireDrained(): void {
    if (this.highWaterMarkReached && this.entries.length < this.highWaterMark) {
      this.highWaterMarkReached = false;
      this.onDrained?.(this.entries.length);
    }
  }

  /**
   * Enqueues an item. The queue is bounded by `capacity` (count) and,
   * when configured, by `maxBytes` (bytes) — the count dimension is
   * enforced first, then the byte dimension:
   * - `drop-newest`: the incoming item is discarded on either dimension.
   * - `drop-oldest`: the oldest entry with the lowest priority is shed to
   *   make room — unless the incoming item itself is the strictly
   *   lowest-priority one, in which case the incoming item is discarded
   *   instead. High-priority messages are therefore dropped last.
   *
   * When the byte budget is configured, an item larger than the entire
   * budget is never admitted: `drop-newest` discards it, and `drop-oldest`
   * has nothing to evict for it. Byte-budget evictions count into
   * `droppedCount` (and `droppedByPriority`) exactly like count evictions.
   *
   * `expiresAt` is an absolute timestamp (milliseconds, same clock the caller
   * passes to `drainLive`); entries without one never expire. Expiry is only
   * enforced when the queue drains — an entry keeps its slot until then, so
   * publishing with a TTL does not change backpressure drop behavior.
   */
  push(item: T, priority: Priority = 0, expiresAt?: number): 'accepted' | 'dropped' {
    if (!Number.isFinite(priority)) {
      throw new RangeError('priority must be a finite number');
    }
    if (expiresAt !== undefined && (!Number.isFinite(expiresAt) || expiresAt < 0)) {
      throw new RangeError('expiresAt must be a non-negative finite timestamp');
    }
    const bytes = this.maxBytes === undefined ? 0 : this.measure(item);
    if (this.entries.length >= this.capacity) {
      if (this.policy === 'drop-newest') {
        this.recordDrop(priority);
        return 'dropped';
      }
      const victimIndex = this.findShedVictimIndex(priority);
      if (victimIndex === -1) {
        // The incoming item is strictly lower priority than everything queued:
        // dropping it protects the higher-priority backlog.
        this.recordDrop(priority);
        return 'dropped';
      }
      this.evict(victimIndex);
    }
    if (this.maxBytes !== undefined) {
      // Byte dimension: shed per the drop policy until the incoming item
      // fits. `drop-newest` never evicts for the newcomer; with an empty
      // queue there is nothing to evict — an item larger than the whole
      // budget is never admitted, whichever the policy.
      while (this.bytesTotal + bytes > this.maxBytes) {
        if (this.policy === 'drop-newest' || this.entries.length === 0) {
          this.recordDrop(priority);
          return 'dropped';
        }
        const victimIndex = this.findShedVictimIndex(priority);
        if (victimIndex === -1) {
          this.recordDrop(priority);
          return 'dropped';
        }
        this.evict(victimIndex);
      }
    }
    this.entries.push({ item, priority, bytes, expiresAt });
    this.bytesTotal += bytes;
    this.checkHighWaterMark();
    return 'accepted';
  }

  /** Removes and returns every queued item in FIFO order. */
  drain(): T[] {
    const items = this.entries.map((entry) => entry.item);
    this.entries = [];
    this.bytesTotal = 0;
    this.maybeFireDrained();
    return items;
  }

  /**
   * Removes and returns the oldest queued entry's item, or `undefined`
   * when the queue is empty. Unlike the shed inside `push`, this does NOT
   * count a drop — the caller owns drop accounting (used by `LaneQueue`,
   * which charges the drop to the victim's lane). Bytes are freed. The
   * high-water-mark latch is deliberately untouched, matching the
   * original single-queue shed: the transient dip during evict-then-push
   * must not end the excursion.
   */
  evictOldest(): T | undefined {
    const entry = this.entries[0];
    if (entry === undefined) return undefined;
    this.entries.splice(0, 1);
    this.bytesTotal -= entry.bytes;
    return entry.item;
  }

  /**
   * Subscription-migration support (EB-82): returns the queued items in
   * FIFO order WITHOUT removing them — no drops, no expiry sweep, no
   * high-water-mark side effects. Per-entry TTL deadlines and priorities
   * are not included; see `EventBus.exportSubscription` for the
   * documented TTL boundary.
   */
  snapshot(): T[] {
    return this.entries.map((entry) => entry.item);
  }

  /**
   * Returns the oldest queued item without removing it, or `undefined`
   * when the queue is empty. Entries are kept in push order (append-only
   * with splice removals), so the head is the oldest surviving entry —
   * the one whose queue dwell defines a consumer-lag watermark.
   */
  peekOldest(): T | undefined {
    return this.entries[0]?.item;
  }

  /**
   * Removes and returns every non-expired queued item in FIFO order.
   * Entries whose expiry timestamp is at or before `nowMs` are discarded and
   * counted (see `expiredCount`) instead of being delivered; entries without
   * an expiry timestamp are always live.
   *
   * Expiry boundary semantics: the deadline is inclusive — an entry is
   * expired exactly when `nowMs >= expiresAt`, i.e. a message that has been
   * alive for exactly its TTL is dropped, not delivered.
   *
   * EDF scheduling (EB-204): `deadlineOf`, when given, maps an item to its
   * earliest-deadline-first scheduling deadline (or `undefined` when the
   * item carries none). When at least one queued entry carries a deadline,
   * the drain visits deadline-carrying entries earliest-deadline-first;
   * entries without a deadline keep their relative FIFO order behind them
   * (`deadline ?? +Infinity`, stable by insertion order). The queue's
   * physical order is untouched — survivors are re-assembled in push order
   * — so `peekOldest` and `snapshot` keep their FIFO meaning, and when no
   * entry carries a deadline the drain is byte-identical to plain FIFO.
   */
  drainLive(nowMs: number, deadlineOf?: (item: T) => number | undefined): { live: T[]; expired: T[] } {
    return this.drainLiveUpTo(nowMs, Infinity, undefined, deadlineOf);
  }

  /**
   * Like `drainLive`, but returns at most `maxLive` non-expired entries —
   * the remaining entries stay queued in FIFO order with their expiry
   * timestamps intact, so a later drain picks up exactly where this one
   * stopped. Entries past the live window are not inspected for expiry in
   * this pass; they are evaluated when a later drain reaches them. Used by
   * delivery-side rate shaping to dequeue only what the subscriber's token
   * budget allows in one round.
   *
   * `countAgainstBudget`, when given, marks which dequeued entries count
   * against `maxLive`: an entry for which it returns false flows through
   * freely (EB-117 credit bypass for durable-log replay — replay walks an
   * independent budget, so it never consumes credit but still respects
   * FIFO order and the expiry sweep). Expiry is always evaluated for
   * every entry, budget or not.
   *
   * `deadlineOf` enables EDF scheduling (EB-204) — see `drainLive`.
   */
  drainLiveUpTo(
    nowMs: number,
    maxLive: number,
    countAgainstBudget?: (item: T) => boolean,
    deadlineOf?: (item: T) => number | undefined,
  ): { live: T[]; expired: T[] } {
    const live: T[] = [];
    const expired: T[] = [];
    // EDF visit order (EB-204): computed only when the caller opts in via
    // `deadlineOf` AND at least one queued entry actually carries a
    // deadline — otherwise the drain walks `entries` in plain FIFO order,
    // exactly as before. The order is a drain-time view: survivors are
    // filtered back in push order below, so the physical queue never
    // reorders.
    let visitOrder: number[] | undefined;
    if (deadlineOf !== undefined) {
      let anyDeadline = false;
      const keys = new Array<number | undefined>(this.entries.length);
      for (let i = 0; i < this.entries.length; i += 1) {
        const key = deadlineOf(this.entries[i].item);
        keys[i] = key;
        if (key !== undefined) anyDeadline = true;
      }
      if (anyDeadline) {
        // Stable sort: `deadline ?? +Infinity` puts deadline-free entries
        // behind deadline-carrying ones while preserving FIFO among
        // themselves. `Array.prototype.sort` is stable, so equal deadlines
        // (and equal +Infinity keys) keep insertion order.
        visitOrder = this.entries
          .map((_, i) => i)
          .sort((a, b) => (keys[a] ?? Number.POSITIVE_INFINITY) - (keys[b] ?? Number.POSITIVE_INFINITY));
      }
    }
    const removed = new Set<number>();
    let removedBytes = 0;
    let counted = 0;
    const visit = (index: number): void => {
      const entry = this.entries[index];
      if (entry.expiresAt !== undefined && nowMs >= entry.expiresAt) {
        expired.push(entry.item);
        removedBytes += entry.bytes;
        removed.add(index);
        return;
      }
      const counts = countAgainstBudget === undefined || countAgainstBudget(entry.item);
      if (!counts || counted < maxLive) {
        live.push(entry.item);
        removedBytes += entry.bytes;
        removed.add(index);
        if (counts) counted += 1;
      }
    };
    if (visitOrder === undefined) {
      for (let i = 0; i < this.entries.length; i += 1) visit(i);
    } else {
      for (const i of visitOrder) visit(i);
    }
    this.entries = this.entries.filter((_, i) => !removed.has(i));
    this.bytesTotal -= removedBytes;
    this.maybeFireDrained();
    this.expired += expired.length;
    return { live, expired };
  }

  /**
   * Returns the live entries in FIFO order WITHOUT removing them,
   * partitioned into budget-counting and budget-exempt entries under
   * `countAgainstBudget` (see `drainLiveUpTo`). Entries past their TTL at
   * `nowMs` are excluded but NOT counted as expired and NOT removed — the
   * caller owns the expiry decision; this is a read-only probe for the
   * topic-fair scheduler's batch winner selection (EB-135).
   */
  peekLivePartition(
    nowMs: number,
    countAgainstBudget?: (item: T) => boolean,
  ): { counting: T[]; exempt: T[] } {
    const counting: T[] = [];
    const exempt: T[] = [];
    for (const entry of this.entries) {
      if (entry.expiresAt !== undefined && nowMs >= entry.expiresAt) continue;
      if (countAgainstBudget !== undefined && !countAgainstBudget(entry.item)) {
        exempt.push(entry.item);
      } else {
        counting.push(entry.item);
      }
    }
    return { counting, exempt };
  }

  /**
   * Topic-fair drain (EB-135): like `drainLiveUpTo`, but the
   * budget-counting live entries are selected by deficit round-robin
   * across `groupOf(item)` groups (the message topic) instead of plain
   * FIFO — see `deficitRoundRobinSelect` in src/fair.ts. Expiry semantics
   * are unchanged: every entry is evaluated, expirations are counted and
   * removed exactly as in `drainLiveUpTo`. Budget-exempt entries
   * (`countAgainstBudget` false) flow freely in FIFO order ahead of the
   * scheduled ones, consuming neither budget nor deficit.
   *
   * `deficits` is the caller's persistent per-group carryover map (see
   * `Subscriber.topicFairness`): groups that still hold entries keep
   * their leftover deficit for the next round; groups absent from the
   * remaining backlog are pruned from the map.
   *
   * `onlyGroup`, when given, restricts the whole drain to that group —
   * the batch path (EB-135) uses it so a collected batch never mixes
   * topics: budget-exempt entries of other groups stay queued for a later
   * round instead of flowing freely.
   *
   * Entries the scheduler does not select stay queued in their original
   * FIFO order — the queue is never reordered, so a later drain resumes
   * exactly where this one stopped.
   */
  drainLiveUpToFair(
    nowMs: number,
    maxLive: number,
    groupOf: (item: T) => string,
    weightOf: (group: string) => number,
    deficits: Map<string, number>,
    countAgainstBudget?: (item: T) => boolean,
    onlyGroup?: string,
  ): { live: T[]; expired: T[] } {
    const live: T[] = [];
    const expired: T[] = [];
    const counting: QueueEntry<T>[] = [];
    const exempt: QueueEntry<T>[] = [];
    const rest: QueueEntry<T>[] = [];
    let removedBytes = 0;
    for (const entry of this.entries) {
      if (entry.expiresAt !== undefined && nowMs >= entry.expiresAt) {
        expired.push(entry.item);
        removedBytes += entry.bytes;
        continue;
      }
      if (onlyGroup !== undefined && groupOf(entry.item) !== onlyGroup) {
        // Another group under `onlyGroup`: stays queued untouched.
        rest.push(entry);
        continue;
      }
      if (countAgainstBudget !== undefined && !countAgainstBudget(entry.item)) {
        exempt.push(entry);
      } else {
        counting.push(entry);
      }
    }
    // Phase 1: budget-exempt entries flow freely in FIFO order, ahead of
    // the scheduled ones — consuming neither budget nor deficit.
    for (const entry of exempt) {
      live.push(entry.item);
      removedBytes += entry.bytes;
    }
    // Phase 2: deficit round-robin over the budget-counting entries.
    const selected = deficitRoundRobinSelect(
      counting.map((entry) => entry.item),
      groupOf,
      weightOf,
      deficits,
      maxLive,
    );
    const selectedCounts = new Map<T, number>();
    for (const item of selected) {
      selectedCounts.set(item, (selectedCounts.get(item) ?? 0) + 1);
      live.push(item);
    }
    for (const entry of counting) {
      const remaining = selectedCounts.get(entry.item) ?? 0;
      if (remaining > 0) {
        selectedCounts.set(entry.item, remaining - 1);
        removedBytes += entry.bytes;
      } else {
        // Not selected: stays queued in its original FIFO position —
        // the queue is never reordered.
        rest.push(entry);
      }
    }
    this.entries = rest;
    this.bytesTotal -= removedBytes;
    this.maybeFireDrained();
    this.expired += expired.length;
    // Prune deficits for groups with no remaining backlog (classic DRR:
    // an absent group rejoins at zero). Groups that kept entries — taken
    // or not, in scope or out — keep their carryover.
    const backlogGroups = new Set<string>();
    for (const entry of rest) backlogGroups.add(groupOf(entry.item));
    for (const group of [...deficits.keys()]) {
      if (!backlogGroups.has(group)) deficits.delete(group);
    }
    return { live, expired };
  }

  /**
   * Index of the shed victim among the queued entries: the oldest entry whose
   * priority is minimal. Returns -1 when the incoming item is strictly lower
   * priority than every queued entry (so the incoming item should be dropped).
   */
  private findShedVictimIndex(incomingPriority: Priority): number {
    let minPriority = Infinity;
    for (const entry of this.entries) {
      if (entry.priority < minPriority) minPriority = entry.priority;
    }
    if (incomingPriority < minPriority) return -1;
    return this.entries.findIndex((entry) => entry.priority === minPriority);
  }

  private recordDrop(priority: Priority): void {
    this.dropped += 1;
    this.droppedByPriorityCount.set(
      priority,
      (this.droppedByPriorityCount.get(priority) ?? 0) + 1,
    );
  }

  /**
   * Measures an item's buffered byte size via the configured estimator.
   * Throws `RangeError` when the estimator does not return a non-negative
   * finite number — a silently wrong size would corrupt the budget.
   */
  private measure(item: T): number {
    const bytes = this.byteSize(item);
    if (!Number.isFinite(bytes) || bytes < 0) {
      throw new RangeError('byteSize must return a non-negative finite number');
    }
    return bytes;
  }

  /** Sheds one queued entry: removes it, frees its bytes, counts the drop. */
  private evict(index: number): void {
    const [entry] = this.entries.splice(index, 1);
    this.bytesTotal -= entry.bytes;
    this.recordDrop(entry.priority);
  }

  private checkHighWaterMark(): void {
    // The excursion latch is tracked independently of the callbacks so that
    // `onDrained` works even when no `onHighWaterMark` is registered.
    if (!this.highWaterMarkReached && this.entries.length >= this.highWaterMark) {
      this.highWaterMarkReached = true;
      this.onHighWaterMark?.(this.entries.length);
    }
  }
}
