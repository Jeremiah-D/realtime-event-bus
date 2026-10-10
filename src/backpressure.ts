export type DropPolicy = 'drop-oldest' | 'drop-newest';

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
  private readonly capacity: number;
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
   */
  drainLive(nowMs: number): { live: T[]; expired: T[] } {
    return this.drainLiveUpTo(nowMs, Infinity);
  }

  /**
   * Like `drainLive`, but returns at most `maxLive` non-expired entries —
   * the remaining entries stay queued in FIFO order with their expiry
   * timestamps intact, so a later drain picks up exactly where this one
   * stopped. Entries past the live window are not inspected for expiry in
   * this pass; they are evaluated when a later drain reaches them. Used by
   * delivery-side rate shaping to dequeue only what the subscriber's token
   * budget allows in one round.
   */
  drainLiveUpTo(nowMs: number, maxLive: number): { live: T[]; expired: T[] } {
    const live: T[] = [];
    const expired: T[] = [];
    const rest: QueueEntry<T>[] = [];
    let removedBytes = 0;
    for (const entry of this.entries) {
      if (entry.expiresAt !== undefined && nowMs >= entry.expiresAt) {
        expired.push(entry.item);
        removedBytes += entry.bytes;
        continue;
      }
      if (live.length < maxLive) {
        live.push(entry.item);
        removedBytes += entry.bytes;
      } else {
        rest.push(entry);
      }
    }
    this.entries = rest;
    this.bytesTotal -= removedBytes;
    this.maybeFireDrained();
    this.expired += expired.length;
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
