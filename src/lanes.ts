import { Buffer } from 'node:buffer';
import { BoundedQueue, type DropPolicy } from './backpressure.ts';

/**
 * Per-topic priority lanes (EB-122): `urgent` / `normal` / `bulk`.
 *
 * Lanes live on the per-subscriber queue — the level the drain
 * architecture already operates on (one bounded queue per subscriber,
 * drained per flush round under per-subscriber shaping / throttling /
 * rate-limit budgets). A per-topic shared lane structure could not be
 * drained per subscriber without re-filtering by pattern on every round,
 * so per-subscriber lanes are the coherent choice; per-topic and global
 * lane statistics are maintained as counters aggregated at enqueue /
 * drain / shed / expiry time (see `EventBus`'s `countLane*` helpers).
 *
 * Cross-lane delivery order is priority-based by design: the drain serves
 * lanes in weighted round-robin order, so a bulk message published before
 * an urgent one is still delivered after it. Within one lane FIFO is
 * preserved exactly — same-lane messages always deliver in publish order.
 */
export type MessageLane = 'urgent' | 'normal' | 'bulk';

/** Lanes in drain order: highest drain precedence first. */
export const MESSAGE_LANES: readonly MessageLane[] = ['urgent', 'normal', 'bulk'];

/**
 * Shed precedence for the `drop-oldest` backpressure tier: the oldest
 * entry of the lowest-precedence nonempty lane is shed first, so bulk
 * absorbs pressure before normal, and urgent is shed last. Mirrors the
 * numeric-priority shed contract of `BoundedQueue` (higher value shed
 * later), with lanes mapped to priorities.
 */
const LANE_SHED_PRECEDENCE: Record<MessageLane, number> = {
  urgent: 2,
  normal: 1,
  bulk: 0,
};

/** Per-lane drain weights for the weighted round-robin rotation. */
export interface LaneWeights {
  urgent: number;
  normal: number;
  bulk: number;
}

/** Default weights: 8 urgent : 2 normal : 1 bulk per rotation pass. */
export const DEFAULT_LANE_WEIGHTS: LaneWeights = { urgent: 8, normal: 2, bulk: 1 };

/**
 * Default bulk starvation threshold: when the bulk lane holds messages but
 * drains none for this many consecutive drain rounds, the next round
 * force-interleaves one bulk message ahead of the rotation. 10 rounds is
 * roughly one missed full rotation under the default 8:2:1 weights — long
 * enough that a merely bursty urgent lane never trips it, short enough
 * that a truly starved bulk lane does not wait indefinitely.
 */
export const DEFAULT_BULK_STARVATION_ROUNDS = 10;

/** Per-lane counters, in drain order. */
export interface LaneCounts {
  urgent: number;
  normal: number;
  bulk: number;
}

/** Zeroed lane counters. */
export function zeroedLaneCounts(): LaneCounts {
  return { urgent: 0, normal: 0, bulk: 0 };
}

/** Fail-fast validation for `PublishOptions.priority`. */
export function assertValidMessageLane(
  value: unknown,
  where: string,
): asserts value is MessageLane {
  if (value !== 'urgent' && value !== 'normal' && value !== 'bulk') {
    throw new RangeError(`${where}: priority must be 'urgent', 'normal' or 'bulk'`);
  }
}

/** Bus-level priority-lane tuning (`EventBusOptions.priorityLanes`). */
export interface PriorityLanesOptions {
  /**
   * Per-lane drain weights (positive integers). Defaults to 8:2:1
   * (urgent:normal:bulk). Partial objects are allowed — any lane left
   * undefined keeps its default.
   */
  weights?: { urgent?: number; normal?: number; bulk?: number };
  /**
   * Consecutive drain rounds the bulk lane may sit nonempty-but-undrained
   * before one bulk message is force-interleaved (positive integer,
   * default 10).
   */
  bulkStarvationRounds?: number;
}

/** Validated `PriorityLanesOptions` with every default resolved. */
export interface ResolvedPriorityLanesOptions {
  weights: LaneWeights;
  bulkStarvationRounds: number;
}

/** Fail-fast validation for `EventBusOptions.priorityLanes`. */
export function resolvePriorityLanesOptions(
  opts: PriorityLanesOptions | undefined,
  where: string,
): ResolvedPriorityLanesOptions {
  const weights: LaneWeights = {
    urgent: opts?.weights?.urgent ?? DEFAULT_LANE_WEIGHTS.urgent,
    normal: opts?.weights?.normal ?? DEFAULT_LANE_WEIGHTS.normal,
    bulk: opts?.weights?.bulk ?? DEFAULT_LANE_WEIGHTS.bulk,
  };
  for (const lane of MESSAGE_LANES) {
    const w = weights[lane];
    if (!Number.isInteger(w) || w <= 0) {
      throw new RangeError(
        `${where}: priorityLanes.weights.${lane} must be a positive integer`,
      );
    }
  }
  const bulkStarvationRounds = opts?.bulkStarvationRounds ?? DEFAULT_BULK_STARVATION_ROUNDS;
  if (!Number.isInteger(bulkStarvationRounds) || bulkStarvationRounds <= 0) {
    throw new RangeError(
      `${where}: priorityLanes.bulkStarvationRounds must be a positive integer`,
    );
  }
  return { weights, bulkStarvationRounds };
}

export interface LaneQueueOptions<T> {
  capacity: number;
  policy?: DropPolicy;
  /** High-water mark as a fraction of capacity (default 0.8), on the aggregate size. */
  highWaterMarkRatio?: number;
  /**
   * Opt-in byte budget over the aggregate buffered bytes, in bytes. The
   * count dimension is enforced first, then the byte dimension — the same
   * order `BoundedQueue` uses, applied across lanes instead of one FIFO.
   */
  maxBytes?: number;
  /**
   * Estimates an item's buffered byte size. Defaults to the item's JSON
   * UTF-8 byte length (unserializable items estimate as 0). Must return a
   * non-negative finite number — a push whose estimate is not finite or
   * negative throws `RangeError`.
   */
  byteSize?: (item: T) => number;
  /** Per-lane drain weights (default 8:2:1). */
  weights?: LaneWeights;
  /** Bulk starvation threshold in consecutive undrained rounds (default 10). */
  bulkStarvationRounds?: number;
  /** Fired once when the aggregate size reaches the high-water mark; re-arms below it. */
  onHighWaterMark?: (size: number) => void;
  /** Fired once when the aggregate size recedes below the high-water mark after an excursion. */
  onDrained?: (size: number) => void;
  /**
   * Fired with each message shed under `drop-oldest` (EB-225): the
   * evicted item and the lane it was shed from. Refused newcomers never
   * fire this — they never entered a lane. The bus wires this to the
   * subscriber-queue drop journal; the queue itself only counts sheds.
   */
  onEvicted?: (item: T, lane: MessageLane) => void;
}

/**
 * A subscriber's three priority lanes over one shared capacity budget.
 *
 * Each lane is a `BoundedQueue` (per-lane FIFO, per-entry TTL deadlines),
 * but capacity, the byte budget, the shed policy, and the high-water-mark
 * latch are all coordinated on the aggregate: the three lanes together
 * behave like one bounded queue that drains in weighted round-robin
 * order instead of plain FIFO.
 *
 * Backpressure semantics mirror `BoundedQueue` with lanes mapped to shed
 * precedence: under `drop-oldest` a full queue sheds the oldest entry of
 * the lowest-precedence nonempty lane (bulk first, urgent last); an
 * incoming message whose lane is strictly lower precedence than every
 * queued entry is refused instead. Under `drop-newest` a full queue
 * refuses the newcomer. The aggregate size drives the high-water mark —
 * including the `'block'` tier's unblock signal — exactly as a single
 * queue would.
 *
 * Zero-cost opt-in: when at most one lane holds messages, `drainLiveUpTo`
 * and `drainLive` delegate verbatim to that lane's `BoundedQueue`, so
 * traffic that never uses priorities behaves exactly like the old single
 * queue — the weighted rotation is a no-op until a second lane fills.
 */
export class LaneQueue<T> {
  private readonly lanes: Record<MessageLane, BoundedQueue<T>>;
  private capacity: number;
  private readonly policy: DropPolicy;
  private readonly weights: LaneWeights;
  private readonly bulkStarvationRounds: number;
  /**
   * Consecutive drain rounds the bulk lane ended nonempty without
   * draining anything. Reset whenever bulk drains or the lane empties;
   * reaching `bulkStarvationRounds` force-interleaves one bulk message.
   */
  private bulkStarvedRounds = 0;
  private maxBytes?: number;
  private readonly byteSize: (item: T) => number;
  private bytesTotal = 0;
  private readonly onHighWaterMark?: (size: number) => void;
  private readonly onDrained?: (size: number) => void;
  private readonly onEvicted?: (item: T, lane: MessageLane) => void;
  private highWaterMarkFraction: number;
  private highWaterMarkReached = false;
  private readonly droppedByLaneCount: LaneCounts = zeroedLaneCounts();
  private readonly expiredByLaneCount: LaneCounts = zeroedLaneCounts();

  constructor(options: LaneQueueOptions<T>) {
    if (!Number.isInteger(options.capacity) || options.capacity <= 0) {
      throw new RangeError('capacity must be a positive integer');
    }
    this.capacity = options.capacity;
    this.policy = options.policy ?? 'drop-oldest';
    this.weights = options.weights ?? { ...DEFAULT_LANE_WEIGHTS };
    this.bulkStarvationRounds = options.bulkStarvationRounds ?? DEFAULT_BULK_STARVATION_ROUNDS;
    this.onHighWaterMark = options.onHighWaterMark;
    this.onDrained = options.onDrained;
    this.onEvicted = options.onEvicted;
    this.highWaterMarkFraction = options.highWaterMarkRatio ?? 0.8;
    if (
      !Number.isFinite(this.highWaterMarkFraction) ||
      this.highWaterMarkFraction <= 0 ||
      this.highWaterMarkFraction > 1
    ) {
      throw new RangeError('highWaterMarkRatio must be a finite number in (0, 1]');
    }
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
    // Each lane gets the full capacity: the aggregate check in `push`
    // below guarantees the sum never exceeds it, so a lane's own
    // capacity enforcement never fires — it is unreachable by
    // construction. Lanes carry no byte budget and no HWM callbacks of
    // their own: both are coordinated on the aggregate here.
    const laneOptions = {
      capacity: this.capacity,
      policy: this.policy,
    };
    this.lanes = {
      urgent: new BoundedQueue<T>(laneOptions),
      normal: new BoundedQueue<T>(laneOptions),
      bulk: new BoundedQueue<T>(laneOptions),
    };
  }

  /** Aggregate queued entries across lanes. */
  get size(): number {
    return this.lanes.urgent.size + this.lanes.normal.size + this.lanes.bulk.size;
  }

  /** Queued entries in one lane. */
  laneSize(lane: MessageLane): number {
    return this.lanes[lane].size;
  }

  /** Aggregate buffered bytes across lanes (0 when no byte budget is configured). */
  get queueBytes(): number {
    return this.bytesTotal;
  }

  /** Aggregate shed/refused drops across lanes. */
  get droppedCount(): number {
    return (
      this.droppedByLaneCount.urgent + this.droppedByLaneCount.normal + this.droppedByLaneCount.bulk
    );
  }

  /** Drops charged to one lane (shed victims plus refused newcomers). */
  droppedByLane(lane: MessageLane): number {
    return this.droppedByLaneCount[lane];
  }

  /** Aggregate TTL expirations across lanes. */
  get expiredCount(): number {
    return (
      this.expiredByLaneCount.urgent + this.expiredByLaneCount.normal + this.expiredByLaneCount.bulk
    );
  }

  /** TTL expirations swept from one lane. */
  expiredByLane(lane: MessageLane): number {
    return this.expiredByLaneCount[lane];
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
   * and the aggregate is already below the new mark, the excursion ends
   * immediately: the latch clears and `onDrained` fires synchronously
   * with the current size. Throws `RangeError` for values outside (0, 1].
   */
  setHighWaterMarkRatio(ratio: number): void {
    if (!Number.isFinite(ratio) || ratio <= 0 || ratio > 1) {
      throw new RangeError('highWaterMarkRatio must be a finite number in (0, 1]');
    }
    this.highWaterMarkFraction = ratio;
    this.maybeFireDrained();
  }

  /**
   * Adjusts the aggregate count capacity at runtime (EB-281, via
   * `EventBus.setQueueSize`). Queued entries are untouched — shrinking
   * below the current size evicts nothing; the new cap governs only
   * future `push` calls, which shed or refuse per `policy` once the
   * aggregate reaches it. The per-lane capacities move with the
   * aggregate (they are unreachable by construction — the aggregate
   * check gates first — but a stale lane cap would be a trap for any
   * future direct lane push). Shrinking onto or below the current
   * size arms the high-water-mark excursion latch without re-firing
   * `onHighWaterMark` (the saturation is a revision fact, not a new
   * arrival crossing the mark), so a later drain below the new mark
   * still ends the excursion via `onDrained` — this is what lets the
   * bus's `'block'` tier, armed by the revision, unblock on drain
   * instead of wedging. Growing above the current size admits
   * immediately and ends an in-flight excursion via `maybeFireDrained`
   * when the size already sits below the new mark. Throws `RangeError`
   * for a non-positive-integer capacity.
   */
  setCapacity(capacity: number): void {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError('capacity must be a positive integer');
    }
    this.capacity = capacity;
    for (const lane of MESSAGE_LANES) this.lanes[lane].setCapacity(capacity);
    if (!this.highWaterMarkReached && this.size >= this.highWaterMark) {
      this.highWaterMarkReached = true;
    }
    this.maybeFireDrained();
  }

  /**
   * Adjusts the aggregate byte budget at runtime (EB-282, via
   * `EventBus.setQueueMaxBytes`). Queued entries are untouched — shrinking
   * below the current buffered bytes evicts nothing; the new budget
   * governs only future `push` calls, which shed or refuse per `policy`
   * once the aggregate would exceed it. Growing admits immediately.
   * Clearing (`undefined`) removes the byte dimension entirely, leaving
   * only the count capacity as the admission constraint, and resets the
   * tracked total to 0 so `queueBytes` reads 0 with no budget configured
   * (later drains then have nothing to subtract). A defined budget keeps
   * the tracked total as-is: the bus only revises subscriptions that
   * already carry a budget, so the total is already measured. Throws
   * `RangeError` for a non-positive-finite budget.
   */
  setMaxBytes(maxBytes: number | undefined): void {
    if (maxBytes !== undefined && (!Number.isFinite(maxBytes) || maxBytes <= 0)) {
      throw new RangeError('maxBytes must be a positive finite number of bytes, or undefined to clear it');
    }
    this.maxBytes = maxBytes;
    if (maxBytes === undefined) {
      this.bytesTotal = 0;
    }
  }

  /**
   * Enqueues an item into its lane. The aggregate is bounded by
   * `capacity` (count) and, when configured, by `maxBytes` (bytes) — the
   * count dimension is enforced first, then the byte dimension:
   * - `drop-newest`: the incoming item is discarded on either dimension.
   * - `drop-oldest`: the oldest entry of the lowest-precedence nonempty
   *   lane is shed to make room — unless the incoming item's lane is
   *   strictly lower precedence than every queued entry, in which case
   *   the incoming item is discarded instead. Urgent backlog is therefore
   *   shed last, bulk first.
   *
   * `expiresAt` is an absolute timestamp (milliseconds, same clock the
   * caller passes to `drainLive`); entries without one never expire.
   * Expiry is only enforced when the queue drains — an entry keeps its
   * slot until then, so publishing with a TTL does not change
   * backpressure drop behavior.
   */
  push(item: T, lane: MessageLane, expiresAt?: number): 'accepted' | 'dropped' {
    if (expiresAt !== undefined && (!Number.isFinite(expiresAt) || expiresAt < 0)) {
      throw new RangeError('expiresAt must be a non-negative finite timestamp');
    }
    const bytes = this.maxBytes === undefined ? 0 : this.measure(item);
    // Count dimension, on the aggregate.
    if (this.size >= this.capacity) {
      if (this.policy === 'drop-newest') {
        this.recordDrop(lane);
        return 'dropped';
      }
      const victim = this.findVictimLane(lane);
      if (victim === undefined) {
        // The incoming lane is strictly lower precedence than everything
        // queued: refusing it protects the higher-precedence backlog.
        this.recordDrop(lane);
        return 'dropped';
      }
      this.evictOldest(victim);
    }
    // Byte dimension, on the aggregate bytes.
    if (this.maxBytes !== undefined) {
      while (this.bytesTotal + bytes > this.maxBytes) {
        if (this.policy === 'drop-newest' || this.size === 0) {
          this.recordDrop(lane);
          return 'dropped';
        }
        const victim = this.findVictimLane(lane);
        if (victim === undefined) {
          this.recordDrop(lane);
          return 'dropped';
        }
        this.evictOldest(victim);
      }
    }
    this.lanes[lane].push(item, 0, expiresAt);
    this.bytesTotal += bytes;
    this.checkHighWaterMark();
    return 'accepted';
  }

  /** Removes and returns every queued item, urgent lane first, in per-lane FIFO order. */
  drain(): T[] {
    const items: T[] = [];
    for (const lane of MESSAGE_LANES) items.push(...this.lanes[lane].drain());
    this.bytesTotal = 0;
    this.maybeFireDrained();
    return items;
  }

  /**
   * Returns the queued items WITHOUT removing them — urgent lane first,
   * per-lane FIFO — no drops, no expiry sweep, no high-water-mark side
   * effects. Mirrors `BoundedQueue.snapshot` (subscription migration,
   * EB-82); within-lane order is preserved exactly, which is what the
   * target's re-enqueue needs.
   */
  snapshot(): T[] {
    const items: T[] = [];
    for (const lane of MESSAGE_LANES) items.push(...this.lanes[lane].snapshot());
    return items;
  }

  /**
   * Returns the head of the highest-precedence nonempty lane without
   * removing it, or `undefined` when the queue is empty. Drives the
   * consumer-lag watermark and the replay-bypass head check: an
   * approximation under lanes (a lower-precedence lane may hold an older
   * message), documented as such — priority order wins over arrival
   * order here, matching the drain's precedence.
   */
  peekOldest(): T | undefined {
    for (const lane of MESSAGE_LANES) {
      const head = this.lanes[lane].peekOldest();
      if (head !== undefined) return head;
    }
    return undefined;
  }

  /**
   * Removes and returns every non-expired queued item — urgent lane
   * first, per-lane FIFO. Entries past their TTL are discarded and
   * counted per lane instead of delivered. Used by the shadow drain and
   * the chaos drop-fault path, where the whole backlog goes at once.
   *
   * `deadlineOf` enables EDF scheduling (EB-204) within each lane — see
   * `BoundedQueue.drainLive`.
   */
  drainLive(nowMs: number, deadlineOf?: (item: T) => number | undefined): { live: T[]; expired: T[] } {
    const live: T[] = [];
    const expired: T[] = [];
    for (const lane of MESSAGE_LANES) {
      const result = this.lanes[lane].drainLive(nowMs, deadlineOf);
      live.push(...result.live);
      expired.push(...result.expired);
      for (let i = 0; i < result.expired.length; i += 1) this.recordExpired(lane);
      this.subtractBytes(result.live);
      this.subtractBytes(result.expired);
    }
    this.bulkStarvedRounds = 0;
    this.maybeFireDrained();
    return { live, expired };
  }

  /**
   * Weighted round-robin drain with bulk starvation protection.
   *
   * When at most one lane holds messages this delegates verbatim to that
   * lane's `BoundedQueue.drainLiveUpTo` — traffic that never uses
   * priorities drains exactly like the old single queue, budget predicate
   * and expiry sweep included.
   *
   * Otherwise one rotation pass runs per call: each lane drains up to its
   * weight in budget-counting messages (urgent, then normal, then bulk),
   * bounded by the remaining `maxLive` budget. Messages for which
   * `countAgainstBudget` returns false (the EB-117 replay bypass) flow
   * freely within each lane's turn, exactly like the single-queue
   * semantics — they never consume budget but keep per-lane FIFO order.
   *
   * TTL expiry is swept in every lane on every multi-lane round, so a
   * message does not sit past its deadline only because its lane was not
   * scheduled this round.
   *
   * Bulk starvation protection: when the bulk lane ends a round nonempty
   * without draining anything, the starvation counter advances; when it
   * reaches `bulkStarvationRounds`, the next round force-interleaves one
   * bulk message ahead of the rotation (consuming budget like any other
   * delivery) and resets the counter. The counter also resets whenever
   * bulk drains normally or the lane empties.
   *
   * `deadlineOf` enables EDF scheduling (EB-204) within each lane's
   * share — lanes still win across lanes (urgent before bulk); the
   * deadline only orders messages inside one lane's drain.
   */
  drainLiveUpTo(
    nowMs: number,
    maxLive: number,
    countAgainstBudget?: (item: T) => boolean,
    deadlineOf?: (item: T) => number | undefined,
  ): { live: T[]; expired: T[] } {
    let occupied = 0;
    let only: MessageLane = 'normal';
    for (const lane of MESSAGE_LANES) {
      if (this.lanes[lane].size > 0) {
        occupied += 1;
        only = lane;
      }
    }
    if (occupied <= 1) {
      const result = this.lanes[only].drainLiveUpTo(nowMs, maxLive, countAgainstBudget, deadlineOf);
      for (let i = 0; i < result.expired.length; i += 1) this.recordExpired(only);
      this.subtractBytes(result.live);
      this.subtractBytes(result.expired);
      this.maybeFireDrained();
      if (only === 'bulk' || this.lanes.bulk.size === 0) this.bulkStarvedRounds = 0;
      return result;
    }
    const live: T[] = [];
    const expired: T[] = [];
    // Sweep TTL expiry in every lane first — a message must not sit past
    // its deadline only because its lane was not scheduled this round.
    for (const lane of MESSAGE_LANES) {
      const swept = this.lanes[lane].drainLiveUpTo(nowMs, 0, undefined, deadlineOf);
      expired.push(...swept.expired);
      for (let i = 0; i < swept.expired.length; i += 1) this.recordExpired(lane);
      this.subtractBytes(swept.expired);
    }
    let remaining = maxLive;
    let bulkDrained = 0;
    const bulkLane = this.lanes.bulk;
    const bulkWaiting = bulkLane.size > 0;
    if (bulkWaiting && remaining >= 1 && this.bulkStarvedRounds + 1 >= this.bulkStarvationRounds) {
      // Starved long enough: force-interleave one bulk message ahead of
      // the rotation. It consumes budget like any other delivery — the
      // guard reorders within the budget, it never exceeds it.
      const forced = bulkLane.drainLiveUpTo(nowMs, 1, countAgainstBudget, deadlineOf);
      live.push(...forced.live);
      expired.push(...forced.expired);
      for (let i = 0; i < forced.expired.length; i += 1) this.recordExpired('bulk');
      this.subtractBytes(forced.live);
      this.subtractBytes(forced.expired);
      remaining -= this.countedOf(forced.live, countAgainstBudget);
      bulkDrained += forced.live.length;
    }
    // Weighted rotation: one pass serves urgent, then normal, then bulk,
    // up to each lane's weight in budget-counting messages; passes repeat
    // while budget remains and some lane still yields messages, so an
    // unbounded drain empties every lane in rotation order. The first
    // pass always runs — even at zero budget — so budget-exempt messages
    // (EB-117 replay bypass) keep flowing.
    let progress = true;
    let firstPass = true;
    while (progress && (remaining > 0 || firstPass)) {
      firstPass = false;
      progress = false;
      for (const lane of MESSAGE_LANES) {
        const share = Math.min(this.weights[lane], Math.max(0, remaining));
        // A zero share still runs the lane's drain so budget-exempt
        // messages (EB-117 replay bypass) keep flowing — the lane's own
        // drain only counts budget-consuming entries against the share.
        const taken = this.lanes[lane].drainLiveUpTo(nowMs, share, countAgainstBudget, deadlineOf);
        if (taken.live.length > 0 || taken.expired.length > 0) progress = true;
        live.push(...taken.live);
        expired.push(...taken.expired);
        for (let i = 0; i < taken.expired.length; i += 1) this.recordExpired(lane);
        this.subtractBytes(taken.live);
        this.subtractBytes(taken.expired);
        remaining -= this.countedOf(taken.live, countAgainstBudget);
        if (lane === 'bulk') bulkDrained += taken.live.length;
      }
    }
    if (!bulkWaiting || bulkDrained > 0) {
      this.bulkStarvedRounds = 0;
    } else {
      this.bulkStarvedRounds += 1;
    }
    this.maybeFireDrained();
    return { live, expired };
  }

  /**
   * Topic-fair drain (EB-135): like `drainLiveUpTo`, but the
   * budget-counting entries are selected by deficit round-robin across
   * `groupOf(item)` groups (the message topic) instead of the lane
   * weighted rotation alone — see `deficitRoundRobinSelect` in
   * src/fair.ts. `deficits` is the caller's persistent per-group
   * carryover map, shared across lanes within one drain round.
   *
   * When at most one lane holds messages this delegates verbatim to that
   * lane's `BoundedQueue.drainLiveUpToFair` — traffic that never uses
   * priorities keeps the zero-cost opt-in semantics. Otherwise every lane
   * is expiry-swept first (the existing idiom), then the weighted
   * rotation serves each lane up to its weight in budget-counting
   * messages, each lane's share taken with the fair drain — so lane
   * precedence still wins across lanes (urgent before bulk) while topics
   * share fairly within each lane's share. Bulk starvation protection,
   * `maybeFireDrained`, byte accounting and per-lane expiry counting are
   * preserved exactly as in `drainLiveUpTo`.
   *
   * `onlyGroup`, when given, restricts the whole drain to that group —
   * the batch path (EB-135) passes the winner from `peekFairWinner` so a
   * collected batch never mixes topics, across lanes included.
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
    let occupied = 0;
    let only: MessageLane = 'normal';
    for (const lane of MESSAGE_LANES) {
      if (this.lanes[lane].size > 0) {
        occupied += 1;
        only = lane;
      }
    }
    if (occupied <= 1) {
      const result = this.lanes[only].drainLiveUpToFair(
        nowMs,
        maxLive,
        groupOf,
        weightOf,
        deficits,
        countAgainstBudget,
        onlyGroup,
      );
      for (let i = 0; i < result.expired.length; i += 1) this.recordExpired(only);
      this.subtractBytes(result.live);
      this.subtractBytes(result.expired);
      this.maybeFireDrained();
      if (only === 'bulk' || this.lanes.bulk.size === 0) this.bulkStarvedRounds = 0;
      return result;
    }
    const live: T[] = [];
    const expired: T[] = [];
    // Sweep TTL expiry in every lane first — a message must not sit past
    // its deadline only because its lane was not scheduled this round.
    // The existing idiom: `drainLiveUpTo(nowMs, 0)` with no predicate, so
    // the sweep is expiry-only and `live` stays empty by construction.
    for (const lane of MESSAGE_LANES) {
      const swept = this.lanes[lane].drainLiveUpTo(nowMs, 0);
      expired.push(...swept.expired);
      for (let i = 0; i < swept.expired.length; i += 1) this.recordExpired(lane);
      this.subtractBytes(swept.expired);
    }
    let remaining = maxLive;
    let bulkDrained = 0;
    const bulkLane = this.lanes.bulk;
    const bulkWaiting = bulkLane.size > 0;
    if (bulkWaiting && remaining >= 1 && this.bulkStarvedRounds + 1 >= this.bulkStarvationRounds) {
      // Starved long enough: force-interleave one bulk message ahead of
      // the rotation, taken fairly like any other share — the guard
      // reorders within the budget, it never exceeds it.
      const forced = bulkLane.drainLiveUpToFair(
        nowMs,
        1,
        groupOf,
        weightOf,
        deficits,
        countAgainstBudget,
        onlyGroup,
      );
      live.push(...forced.live);
      expired.push(...forced.expired);
      for (let i = 0; i < forced.expired.length; i += 1) this.recordExpired('bulk');
      this.subtractBytes(forced.live);
      this.subtractBytes(forced.expired);
      remaining -= this.countedOf(forced.live, countAgainstBudget);
      bulkDrained += forced.live.length;
    }
    // Weighted rotation: one pass serves urgent, then normal, then bulk,
    // up to each lane's weight in budget-counting messages; passes repeat
    // while budget remains and some lane still yields messages. The first
    // pass always runs — even at zero budget — so budget-exempt messages
    // (EB-117 replay bypass) keep flowing.
    let progress = true;
    let firstPass = true;
    while (progress && (remaining > 0 || firstPass)) {
      firstPass = false;
      progress = false;
      for (const lane of MESSAGE_LANES) {
        const share = Math.min(this.weights[lane], Math.max(0, remaining));
        const taken = this.lanes[lane].drainLiveUpToFair(
          nowMs,
          share,
          groupOf,
          weightOf,
          deficits,
          countAgainstBudget,
          onlyGroup,
        );
        if (taken.live.length > 0 || taken.expired.length > 0) progress = true;
        live.push(...taken.live);
        expired.push(...taken.expired);
        for (let i = 0; i < taken.expired.length; i += 1) this.recordExpired(lane);
        this.subtractBytes(taken.live);
        this.subtractBytes(taken.expired);
        remaining -= this.countedOf(taken.live, countAgainstBudget);
        if (lane === 'bulk') bulkDrained += taken.live.length;
      }
    }
    if (!bulkWaiting || bulkDrained > 0) {
      this.bulkStarvedRounds = 0;
    } else {
      this.bulkStarvedRounds += 1;
    }
    this.maybeFireDrained();
    return { live, expired };
  }

  /**
   * Topic-fair batch winner probe (EB-135): the group a deficit
   * round-robin drain would dequeue first right now, without removing
   * anything. The batch path resolves the winner once per collection
   * round and drains with it as `onlyGroup`, so a collected batch never
   * mixes topics.
   *
   * The winner is the head group: a DRR round credits every backlogged
   * group with its (positive) weight, so the first group in rotation
   * order always has deficit >= 1 and is served first — the deficits only
   * decide how many each group takes after that. Concretely: the first
   * budget-counting live entry's group across lanes in drain precedence
   * order (urgent → normal → bulk); the first budget-exempt entry's group
   * when no budget-counting entry is live; `undefined` when nothing is
   * live.
   *
   * Cross-collection ordering consequence: collections serve the queue
   * head's topic first, so a continuously backlogged head topic fills
   * consecutive batches before later topics are touched — batches stay
   * single-topic and per-topic FIFO, while the weight ratio governs the
   * plain (non-batch) drain at message granularity.
   */
  peekFairWinner(
    nowMs: number,
    groupOf: (item: T) => string,
    countAgainstBudget?: (item: T) => boolean,
  ): string | undefined {
    let firstExempt: T | undefined;
    for (const lane of MESSAGE_LANES) {
      const part = this.lanes[lane].peekLivePartition(nowMs, countAgainstBudget);
      if (part.counting.length > 0) return groupOf(part.counting[0]);
      if (firstExempt === undefined && part.exempt.length > 0) firstExempt = part.exempt[0];
    }
    return firstExempt === undefined ? undefined : groupOf(firstExempt);
  }

  /**
   * Counts how many of `items` consume budget under `countAgainstBudget`
   * (`items.length` when no predicate is given).
   */
  private countedOf(items: T[], countAgainstBudget?: (item: T) => boolean): number {
    if (countAgainstBudget === undefined) return items.length;
    let counted = 0;
    for (const item of items) {
      if (countAgainstBudget(item)) counted += 1;
    }
    return counted;
  }

  /**
   * Shed-victim lane for an incoming message of `incomingLane`: the
   * lowest-precedence nonempty lane, or `undefined` when the incoming
   * lane is strictly lower precedence than every queued entry (so the
   * newcomer is refused instead of churning higher-precedence backlog).
   */
  private findVictimLane(incomingLane: MessageLane): MessageLane | undefined {
    const incomingPrecedence = LANE_SHED_PRECEDENCE[incomingLane];
    let victim: MessageLane | undefined;
    let victimPrecedence = Infinity;
    for (const lane of MESSAGE_LANES) {
      if (this.lanes[lane].size === 0) continue;
      const precedence = LANE_SHED_PRECEDENCE[lane];
      if (precedence < victimPrecedence) {
        victimPrecedence = precedence;
        victim = lane;
      }
    }
    if (victim === undefined) return undefined;
    return incomingPrecedence < victimPrecedence ? undefined : victim;
  }

  /** Sheds the oldest entry of `lane`: removes it, frees its bytes, counts the drop. */
  private evictOldest(lane: MessageLane): void {
    const evicted = this.lanes[lane].evictOldest();
    if (evicted === undefined) return;
    this.bytesTotal -= this.measure(evicted);
    this.recordDrop(lane);
    // The journal hook fires after the counts, so the drop is counted
    // even if an observer misbehaves; the bus's observer only appends
    // to a bounded ring and never throws.
    this.onEvicted?.(evicted, lane);
    // No maybeFireDrained(): like the original single-queue shed, the
    // transient dip during evict-then-push must not clear the high-water
    // excursion — the queue ends the push back at/above the mark.
  }

  private recordDrop(lane: MessageLane): void {
    this.droppedByLaneCount[lane] += 1;
  }

  private recordExpired(lane: MessageLane): void {
    this.expiredByLaneCount[lane] += 1;
  }

  /**
   * Subtracts the buffered bytes of removed items. Only measured when a
   * byte budget is configured — otherwise every item measures 0 and the
   * total stays 0, matching `BoundedQueue`'s "always 0 without a budget".
   */
  private subtractBytes(items: T[]): void {
    if (this.maxBytes === undefined) return;
    for (const item of items) this.bytesTotal -= this.measure(item);
  }

  private measure(item: T): number {
    const bytes = this.byteSize(item);
    if (!Number.isFinite(bytes) || bytes < 0) {
      throw new RangeError('byteSize must return a non-negative finite number');
    }
    return bytes;
  }

  private checkHighWaterMark(): void {
    if (!this.highWaterMarkReached && this.size >= this.highWaterMark) {
      this.highWaterMarkReached = true;
      this.onHighWaterMark?.(this.size);
    }
  }

  private maybeFireDrained(): void {
    if (this.highWaterMarkReached && this.size < this.highWaterMark) {
      this.highWaterMarkReached = false;
      this.onDrained?.(this.size);
    }
  }
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
