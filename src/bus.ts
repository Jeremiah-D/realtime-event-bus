import { BoundedQueue, type DropPolicy } from './backpressure.ts';
import { AckTracker, type Delivery } from './ack.ts';
import { TokenBucket } from './throttle.ts';
import { DurableTopicLog } from './durablelog.ts';

export type { Delivery } from './ack.ts';

export interface BusMessage {
  topic: string;
  payload: unknown;
  /**
   * Per-topic sequence number assigned by the bus at publish time. Starts
   * at 1 and increases by 1 for every message published to the same topic,
   * no matter how many subscribers receive it. Handlers can compare it with
   * the previously seen number to detect loss (a jump means messages were
   * dropped by backpressure or expired by TTL before delivery) and
   * duplicates / redeliveries (same or lower number than the last seen —
   * expected under at-least-once semantics, never a gap). This is the
   * topic-level message order; `Delivery.seq` on reliable subscriptions is
   * a separate per-subscriber delivery counter.
   */
  seq: number;
}

export type MessageHandler = (msg: BusMessage) => void;

/**
 * Handler for reliable (at-least-once) subscriptions: receives a `Delivery`
 * envelope with `ack()`/`nack()` instead of a bare message.
 */
export type ReliableMessageHandler = (delivery: Delivery<BusMessage>) => void;

export interface SubscribeOptions {
  /** Per-subscriber bounded queue capacity (default 100). */
  queueSize?: number;
  /** Drop policy when the queue is full (default 'drop-oldest'). */
  dropPolicy?: DropPolicy;
  /**
   * Called when the subscriber falls behind: its queue reached the high-water
   * mark (80% of capacity). Fires once per excursion and re-arms after the
   * queue drains below the mark — use it to shed load upstream, alert an
   * operator, or degrade gracefully instead of silently dropping messages.
   */
  onBackpressure?: (event: BackpressureEvent) => void;
  /**
   * Called when the subscriber's queue recedes below the high-water mark
   * after a backpressure excursion — the signal that a throttled consumer
   * may resume full speed. Fires once per excursion, mirroring
   * `onBackpressure`, and re-arms together with it. Also fires synchronously
   * if `setHighWaterMarkRatio` moves the mark above the current queue size
   * mid-excursion.
   */
  onDrained?: (event: DrainedEvent) => void;
  /**
   * High-water-mark ratio for this subscriber's queue (fraction of
   * `queueSize`, default 0.8). Must be in (0, 1]. Adjustable at runtime via
   * `EventBus.setHighWaterMarkRatio`.
   */
  highWaterMarkRatio?: number;
  /**
   * Opt-in adaptive publish-side throttling for a slow subscriber. When the
   * subscriber's queue crosses the high-water mark, the bus rate-limits how
   * many freshly published messages are fanned out to it (per-subscriber
   * token bucket) instead of letting every publish churn through the queue's
   * drop policy. Messages shed by the throttle never reach the queue and are
   * counted separately (see `throttledCount`); they surface as sequence gaps
   * like any other loss. When the queue drains below the mark, throttling
   * disengages and the subscriber resumes full speed — and the drain rate
   * measured during the excursion seeds the next engagement, so the throttle
   * converges on the consumer's real speed. Disabled by default.
   *
   * Pass `true` for the defaults, or a `ThrottleOptions` object to tune the
   * rate bounds. Invalid rates throw `RangeError` from `subscribe`.
   */
  throttle?: boolean | ThrottleOptions;
  /**
   * Called when adaptive throttling engages for this subscriber (see
   * `throttle`): its queue crossed the high-water mark and the bus is now
   * rate-limiting fan-out to it. Fires once per engagement; the enforced
   * rate and whether it was adapted from the subscriber's measured drain
   * rate arrive in the event. Disengagement needs no event: the existing
   * `onDrained` already signals the return to full speed.
   */
  onThrottled?: (event: ThrottleEvent) => void;
  /**
   * Opt-in subscriber health probing. When enabled, the bus watches every
   * delivery to this subscriber: a handler that throws, or one that takes
   * longer than `processingTimeoutMs` to run, counts as one failure.
   * Successful deliveries reset the consecutive-failure counter to 0.
   *
   * When the consecutive failures reach `maxConsecutiveFailures`, delivery
   * to this subscriber auto-pauses: its messages stay queued under the
   * normal backpressure policy (preserved, not dropped) until delivery
   * resumes, and `onDegraded` fires once per degradation. Recovery is
   * manual via `EventBus.resume(subId)`, or automatic after a cooldown when
   * `autoResumeAfterMs` is set.
   *
   * Pass `true` for the defaults, or a `HealthProbeOptions` object to tune
   * the threshold, the processing budget, and the auto-resume cooldown.
   * Invalid values throw `RangeError` from `subscribe`. Disabled by default —
   * without it, a throwing handler behaves exactly as before.
   */
  healthProbe?: boolean | HealthProbeOptions;
  /**
   * Called once when health probing auto-pauses delivery to this
   * subscriber (see `healthProbe`): its handler failed too many times in a
   * row. The event carries the failure count, what tripped the pause, and
   * how many messages are preserved in its queue. Requires `healthProbe`;
   * without it, this callback never fires.
   */
  onDegraded?: (event: DegradedEvent) => void;
  /**
   * Resume-from offset for durable-log replay. When the bus has a durable
   * log (`EventBusOptions.durableLogDir`), the subscriber's queue is
   * pre-filled at subscribe time with every logged message on topics
   * matching its pattern whose per-topic `seq` is greater than
   * `resumeFromSeq` — so a consumer that disconnected (or a process that
   * restarted) picks up where it left off. `0` replays everything logged.
   * Messages keep their original `seq` and TTL deadline; ones whose TTL
   * already expired are dropped as expired at drain time, not resurrected.
   * Cross-topic order follows publish time (`at`), per-topic order follows
   * `seq` — the same per-topic ordering live delivery gives; there is no
   * global total order across topics.
   *
   * Requires `durableLogDir`: passing `resumeFromSeq` without it throws
   * `RangeError` instead of silently replaying nothing. Must be a
   * non-negative integer. For consumer-group members the replay is per
   * member — each member replays into its own queue from its own offset,
   * so seed it from that member's own committed offset (see
   * `commitOffset`), not the group's assignment watermark.
   */
  resumeFromSeq?: number;
}

/**
 * Tuning for adaptive publish-side throttling (`SubscribeOptions.throttle`).
 * All rates are messages per second. Bounds are validated at `subscribe`
 * time; violations throw `RangeError`.
 */
export interface ThrottleOptions {
  /**
   * Floor of the enforced publish rate while throttled. Must be a positive
   * finite number. Default 1 — a throttled subscriber is slowed, never
   * fully stalled.
   */
  minRatePerSec?: number;
  /**
   * Ceiling of the enforced publish rate while throttled. Must be positive;
   * `Infinity` (the default) means no ceiling.
   */
  maxRatePerSec?: number;
  /**
   * Rate enforced on the first backpressure excursion, before any drain rate
   * has been observed for this subscriber. Must be a positive finite number.
   * Defaults to one high-water-mark worth of messages per second
   * (`queueSize * highWaterMarkRatio`, at least 1).
   */
  initialRatePerSec?: number;
}

/** Snapshot delivered to `onThrottled` when adaptive throttling engages. */
export interface ThrottleEvent {
  /** The subscriber whose fan-out is now rate-limited. */
  subscriberId: string;
  /** The topic pattern the throttled subscriber registered. */
  pattern: string;
  /** Publish-side rate now enforced for this subscriber, messages/sec. */
  ratePerSec: number;
  /** Queue size when throttling engaged (at or above the high-water mark). */
  queueSize: number;
  /** Configured per-subscriber queue capacity. */
  capacity: number;
  /**
   * True when the rate was adapted from this subscriber's measured drain
   * rate during its previous excursion; false on the first excursion, when
   * the configured initial rate applies.
   */
  adapted: boolean;
}

/**
 * Tuning for subscriber health probing (`SubscribeOptions.healthProbe`).
 * Validation happens at `subscribe` time; violations throw `RangeError`.
 */
export interface HealthProbeOptions {
  /**
   * Consecutive handler failures (thrown errors and/or processing timeouts)
   * that auto-pause delivery to the subscriber. Must be a positive integer.
   * Default 5.
   */
  maxConsecutiveFailures?: number;
  /**
   * Per-message handler budget in milliseconds, measured with the bus clock
   * (`EventBusOptions.now`). A handler whose execution exceeds the budget
   * counts as one failure (a processing timeout). Opt-in: absent means slow
   * handlers are not counted, only thrown errors. Must be a positive finite
   * number.
   */
  processingTimeoutMs?: number;
  /**
   * Cooldown in milliseconds after which a degraded subscriber auto-resumes
   * delivery. Opt-in: absent means recovery is manual via
   * `EventBus.resume(subId)` only. Must be a positive finite number.
   */
  autoResumeAfterMs?: number;
}

/** Snapshot delivered to `onDegraded` when health probing auto-pauses a subscriber. */
export interface DegradedEvent {
  /** The subscriber whose delivery was paused. */
  subscriberId: string;
  /** The topic pattern the paused subscriber registered. */
  pattern: string;
  /** Consecutive failures that tripped the pause. */
  consecutiveFailures: number;
  /**
   * What tripped the pause: the last delivery threw (`'error'`), or its
   * handler exceeded the processing budget (`'timeout'`).
   */
  reason: 'error' | 'timeout';
  /**
   * Messages preserved in the subscriber's queue at pause time — nothing
   * was dropped, and resuming redelivers them in order.
   */
  pendingMessages: number;
}

/** Point-in-time health snapshot for one subscriber (see `EventBus.subscriberHealth`). */
export interface SubscriberHealth {
  /** The subscriber this snapshot describes. */
  subscriberId: string;
  /** Whether the health probe is enabled for this subscriber. */
  enabled: boolean;
  /** True while delivery is auto-paused after hitting the failure threshold. */
  degraded: boolean;
  /**
   * Current consecutive-failure count. Resets to 0 on every successful
   * delivery and on every resume; always 0 when the probe is disabled.
   */
  consecutiveFailures: number;
}

export interface ReliableSubscribeOptions extends SubscribeOptions {
  /**
   * Milliseconds an unacked delivery may stay outstanding before the bus
   * requeues it automatically. Defaults to 5000. Must be a positive finite
   * number.
   */
  ackTimeoutMs?: number;
}

/** Snapshot delivered to `onRebalance` when a consumer group's membership changes. */
export interface GroupRebalanceEvent {
  /** The group whose membership changed. */
  groupId: string;
  /** The topic pattern this competing set is scoped to. */
  pattern: string;
  /**
   * Member subscription ids in the group after the change, in join order.
   * A snapshot — mutating it does not affect the bus.
   */
  members: string[];
  /** What triggered the rebalance. */
  trigger: 'join' | 'leave';
  /** The subscription id of the member that joined or left. */
  memberId: string;
}

export interface GroupSubscribeOptions extends SubscribeOptions {
  /**
   * Called on every member of the group when its membership changes — a
   * member joins (`trigger: 'join'`, fired on existing members and the
   * newcomer) or unsubscribes (`trigger: 'leave'`, fired on the members
   * that remain). The bus rebalances implicitly (round-robin over the
   * current roster), so this is purely informational: use it to re-derive
   * any local assignment state.
   */
  onRebalance?: (event: GroupRebalanceEvent) => void;
}

/** Snapshot of a subscriber's backpressure state when `onBackpressure` fires. */
export interface BackpressureEvent {
  /** The subscriber whose queue is filling up. */
  subscriberId: string;
  /** The topic pattern the lagging subscriber registered. */
  pattern: string;
  /** Queue size when the event fired. */
  queueSize: number;
  /** Configured per-subscriber queue capacity. */
  capacity: number;
  /** Total messages dropped for this subscriber so far (same as `droppedCount`). */
  dropped: number;
}

/** Snapshot delivered to `onDrained` when a subscriber's queue recovers. */
export interface DrainedEvent {
  /** The subscriber whose queue recovered. */
  subscriberId: string;
  /** The topic pattern the subscriber registered. */
  pattern: string;
  /** Queue size when the event fired (below the high-water mark). */
  queueSize: number;
  /** Configured per-subscriber queue capacity. */
  capacity: number;
  /** Total messages dropped for this subscriber so far (same as `droppedCount`). */
  dropped: number;
  /** Absolute high-water mark (in items) in effect when the event fired. */
  highWaterMark: number;
}

export interface Subscription {
  id: string;
  unsubscribe(): void;
}

/**
 * Options for the {@link EventBus} constructor. Every field is optional;
 * a bus created with no options behaves exactly like before.
 */
export interface EventBusOptions {
  /**
   * Clock source in milliseconds. The bus reads it when a message is
   * published (to stamp its TTL deadline) and when a flush drains the
   * queues (to enforce expiries). Defaults to `Date.now`. Inject a fake
   * clock here for deterministic expiry tests.
   */
  now?: () => number;
  /**
   * Enables the durable topic log: every published message is appended to
   * an append-only per-topic JSONL log under this directory (created when
   * missing), so a restarted process — a new `EventBus` pointed at the same
   * directory — recovers per-topic sequence counters and can replay missed
   * messages to resubscribing consumers via
   * `SubscribeOptions.resumeFromSeq`. Disabled by default; when enabled,
   * every `publish` pays one synchronous file append.
   */
  durableLogDir?: string;
  /**
   * Maximum retained log entries per topic before the log compacts away the
   * oldest entries (default 10000). Only meaningful with `durableLogDir`.
   * Must be a positive integer.
   */
  durableLogMaxEntriesPerTopic?: number;
}

/** Per-topic stats kept live by the bus. */
export interface TopicStats {
  /** The concrete topic name (as published, never a pattern). */
  topic: string;
  /**
   * Subscribers matched by the most recent publish to this topic — the
   * current fan-out width, so operators can see hot topics at a glance.
   * A matching consumer group counts once (its copy goes to a single
   * assigned member), however many members it has.
   */
  subscriberCount: number;
  /** Total messages published to this topic since the bus was created. */
  publishedMessages: number;
  /**
   * Messages on this topic discarded because their TTL expired before
   * delivery. Counts each discarded queue entry once, so a message fanned
   * out to N subscribers whose queues all expire it contributes N here.
   */
  expiredMessages: number;
  /**
   * Highest sequence number published on this topic so far (0 when nothing
   * has been published yet). Compare with the last `seq` a subscriber
   * received to spot a lost tail the gap detector cannot see.
   */
  lastSeq: number;
  /**
   * Sequence numbers observed missing by subscribers of this topic. Every
   * time a delivery arrives with a `seq` more than one above the previously
   * delivered number for the same subscriber and topic, the count of skipped
   * numbers is added here (summed across subscribers). Redeliveries — same
   * or lower `seq` than the last delivered, expected under at-least-once
   * semantics — never count. Messages shed by backpressure or expired by
   * TTL surface here as gaps: from the subscriber's point of view they were
   * lost.
   */
  sequenceGaps: number;
  /**
   * Messages on this topic shed by publish-side rate limiting (see
   * `setTopicRateLimit`): the topic's token bucket was empty when the
   * message was published, so it was never fanned out to any subscriber.
   * The shed consumes a sequence number, so subscribers observe the loss
   * as a sequence gap — the same visibility backpressure drops and TTL
   * expirations get.
   */
  rateLimitedMessages: number;
  /**
   * Publishes to this topic rejected by schema validation (see
   * `setTopicSchema`): the payload failed the topic's validator. Unlike
   * rate-limit sheds, a rejection happens before admission — the payload
   * never became a message, so it consumed no sequence number and is
   * invisible to gap detection.
   */
  rejectedMessages: number;
}

/** Point-in-time snapshot returned by `EventBus.getStats()`. */
export interface BusStats {
  /** Currently active subscriptions. */
  totalSubscribers: number;
  /** Subscriptions grouped by the exact pattern string that was registered. */
  subscribersByPattern: Record<string, number>;
  /** Total messages accepted via `publish`/`publishBatch`. */
  totalPublished: number;
  /**
   * Total messages discarded because their TTL expired before delivery
   * (sum of the per-topic `expiredMessages` counters, counting each
   * discarded queue entry once — see `TopicStats.expiredMessages`).
   */
  expiredMessages: number;
  /**
   * Deliveries currently outstanding — handed to reliable subscribers but
   * not yet acked or nacked — across all subscriptions.
   */
  unackedDeliveries: number;
  /**
   * Number of compiled topic patterns currently held in the pattern cache —
   * one entry per distinct pattern with at least one active subscriber.
   */
  patternCacheSize: number;
  /**
   * Number of distinct literal-prefix keys currently held in the
   * publish-side prefix index — one entry per prefix with at least one
   * active subscriber. Patterns that share a literal prefix (e.g. several
   * `market.btc.*` subscribers) share one key, and patterns starting with
   * a wildcard file under the empty key, so this stays small relative to
   * the subscriber count unless patterns are all prefix-distinct.
   */
  indexSize: number;
  /**
   * Total missing sequence numbers detected across all topics — the sum of
   * every topic's `sequenceGaps`. See `TopicStats.sequenceGaps` for the
   * exact counting rules.
   */
  sequenceGaps: number;
  /**
   * Total messages shed by publish-side per-topic rate limiting — the sum
   * of every topic's `rateLimitedMessages`. See
   * `TopicStats.rateLimitedMessages` for the exact counting rules.
   */
  rateLimitedMessages: number;
  /**
   * Total publishes rejected by schema validation — the sum of every
   * topic's `rejectedMessages`. See `TopicStats.rejectedMessages` for the
   * exact counting rules.
   */
  rejectedMessages: number;
  /**
   * Subscriptions currently under adaptive publish-side throttling (see
   * `SubscribeOptions.throttle`): their queues crossed the high-water mark
   * and the bus is rate-limiting fan-out to them until their queues drain.
   */
  throttledSubscribers: number;
  /**
   * Subscriptions currently auto-paused by subscriber health probing (see
   * `SubscribeOptions.healthProbe`): their handlers failed too many times
   * in a row and delivery is paused until they are resumed manually via
   * `EventBus.resume` or automatically after their configured cooldown.
   */
  degradedSubscribers: number;
  /**
   * Stats for every concrete topic that has seen at least one publish,
   * in order of first publish.
   */
  topics: TopicStats[];
  /**
   * Live consumer groups: one entry per (groupId, pattern) competing set
   * with at least one member, in first-registration order. Empty when no
   * group subscription is active.
   */
  consumerGroups: Array<{ groupId: string; pattern: string; members: number }>;
  /**
   * Durable topic log state (`EventBusOptions.durableLogDir`). Absent when
   * the durable log is disabled.
   */
  durableLog?: {
    /** The log directory, as configured. */
    dir: string;
    /** Topics with at least one logged entry. */
    topics: number;
    /** Total logged entries across all topics. */
    entries: number;
    /** Malformed log lines skipped during recovery/reads. */
    corruptLines: number;
  };
}

interface Subscriber {
  id: string;
  pattern: string;
  /** Compiled form of `pattern`, shared with every subscriber on the pattern. */
  matcher: RegExp;
  handler: MessageHandler;
  queue: BoundedQueue<BusMessage>;
  /**
   * Present for reliable subscriptions: at-least-once delivery state.
   * `redeliveries` counts, per message, how many times this subscriber has
   * requeued it — reported back on each `Delivery` for poison detection.
   */
  reliable?: {
    tracker: AckTracker<BusMessage>;
    redeliveries: WeakMap<BusMessage, number>;
  };
  /**
   * Last per-topic sequence number delivered to this subscriber's handler,
   * for gap detection. A delivery with `seq` more than one above the stored
   * number means messages were lost in between (dropped by backpressure or
   * expired by TTL); the count of missing numbers feeds the topic's
   * `sequenceGaps` stat. Redeliveries (`seq` at or below the stored number)
   * are ignored, never counted as gaps. The first delivery on a topic only
   * establishes the baseline — a subscriber that joins late must not count
   * pre-subscription messages as lost.
   */
  lastDeliveredSeq: Map<string, number>;
  /**
   * Adaptive publish-side throttling state (see `SubscribeOptions.throttle`).
   * Absent when throttling is disabled for this subscriber.
   */
  throttle?: ThrottleState;
  /** Fired once when adaptive throttling engages for this subscriber. */
  onThrottled?: (event: ThrottleEvent) => void;
  /**
   * Per-subscriber health probing state (see `SubscribeOptions.healthProbe`).
   * Absent when the probe is disabled for this subscriber.
   */
  health?: HealthProbeState;
  /** Fired once per degradation when health probing auto-pauses delivery. */
  onDegraded?: (event: DegradedEvent) => void;
  /**
   * Present for consumer-group members: the group this subscription
   * belongs to. Members of the same (groupId, pattern) compete — each
   * message is delivered to exactly one of them (see `subscribeToGroup`).
   */
  groupId?: string;
  /** Fired on group membership changes (see `GroupSubscribeOptions`). */
  onRebalance?: (event: GroupRebalanceEvent) => void;
}

/**
 * Per-subscriber adaptive throttling state. The token bucket is (re)created
 * on every engagement with one second's worth of burst; `observedDrainRatePerSec`
 * survives across excursions so re-engagement starts at the consumer's
 * measured speed.
 */
interface ThrottleState {
  bucket: TokenBucket;
  /** Whether fan-out to this subscriber is currently rate-limited. */
  throttled: boolean;
  /** Rate currently (or last) enforced, messages/sec. */
  ratePerSec: number;
  /** Messages shed at the publish side by the throttle. */
  throttledDrops: number;
  /** Drain rate measured during the previous excursion, messages/sec. */
  observedDrainRatePerSec?: number;
  /** `now()` reading when the current excursion started. */
  backpressureAtMs: number;
  /** Queue size when the current excursion started. */
  sizeAtBackpressure: number;
  minRatePerSec: number;
  maxRatePerSec: number;
  initialRatePerSec: number;
}

/**
 * Per-subscriber health probing state (see `SubscribeOptions.healthProbe`).
 * Only `consecutiveFailures`, `degraded`, and `autoResumeTimer` change over
 * the subscription's lifetime; the rest is the validated configuration.
 */
interface HealthProbeState {
  /** Consecutive handler failures; resets to 0 on every success and resume. */
  consecutiveFailures: number;
  /** True while delivery is auto-paused after hitting the failure threshold. */
  degraded: boolean;
  /** What tripped the current (or most recent) pause. */
  degradedReason?: 'error' | 'timeout';
  maxConsecutiveFailures: number;
  processingTimeoutMs?: number;
  autoResumeAfterMs?: number;
  /** Pending auto-resume timer, if the subscriber degraded with a cooldown. */
  autoResumeTimer?: ReturnType<typeof setTimeout>;
}

function escapeRegExp(literal: string): string {
  return literal.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Compiles a topic pattern into an anchored RegExp with exactly the same
 * matching semantics as the documented wildcard rules: `*` matches a single
 * topic segment, `**` matches zero or more segments (in leading, middle, or
 * trailing position), and every other segment matches literally. A bare `*`
 * or `**` matches every topic.
 *
 * The bus compiles each distinct pattern once and shares the RegExp across
 * all subscribers on that pattern (see `EventBus`), so hot paths pay for
 * pattern parsing a single time instead of on every publish.
 */
export function compilePattern(pattern: string): RegExp {
  // A lone `*` or `**` matches every topic.
  if (pattern === '*' || pattern === '**') return /^.*$/;
  // Consecutive `**` segments are equivalent to a single one (zero-or-more
  // followed by zero-or-more segments is still zero-or-more), so collapse
  // runs first — this keeps the separator bookkeeping below unambiguous.
  const segments: string[] = [];
  for (const seg of pattern.split('.')) {
    if (seg === '**' && segments[segments.length - 1] === '**') continue;
    segments.push(seg);
  }
  let source = '^';
  for (let i = 0; i < segments.length; i += 1) {
    const seg = segments[i];
    if (seg === '**') {
      // Zero or more segments. Leading `**` absorbs the separator that
      // *follows* it (`(?:.*\.)?`); anywhere else it absorbs the separator
      // that *precedes* it (`(?:\..*)?`). Either way, zero segments means
      // no separator is consumed.
      source += i === 0 ? '(?:.*\\.)?' : '(?:\\..*)?';
      continue;
    }
    const atom = seg === '*' ? '[^.]+' : escapeRegExp(seg);
    if (i === 0) {
      source += atom;
    } else if (segments[i - 1] === '**' && i - 1 === 0) {
      // Separator already inside the leading `**` group.
      source += atom;
    } else {
      source += `\\.${atom}`;
    }
  }
  source += '$';
  return new RegExp(source);
}

/**
 * Validates `SubscribeOptions.throttle` and builds the initial per-subscriber
 * throttle state. Returns `undefined` when throttling is disabled. Throws
 * `RangeError` for invalid rates.
 */
function resolveThrottleOptions(
  opt: boolean | ThrottleOptions | undefined,
  capacity: number,
  hwmRatio: number,
  now: () => number,
): ThrottleState | undefined {
  if (opt == null || opt === false) return undefined;
  const o: ThrottleOptions = opt === true ? {} : opt;
  const checkRate = (name: string, value: number | undefined, fallback: number): number => {
    if (value === undefined) return fallback;
    if (!Number.isFinite(value) || value <= 0) {
      throw new RangeError(`throttle.${name} must be a positive finite number of messages/sec`);
    }
    return value;
  };
  const minRatePerSec = checkRate('minRatePerSec', o.minRatePerSec, 1);
  let maxRatePerSec: number;
  if (o.maxRatePerSec === undefined) {
    maxRatePerSec = Infinity;
  } else if (o.maxRatePerSec === Infinity || (Number.isFinite(o.maxRatePerSec) && o.maxRatePerSec > 0)) {
    maxRatePerSec = o.maxRatePerSec;
  } else {
    throw new RangeError('throttle.maxRatePerSec must be a positive number of messages/sec');
  }
  if (minRatePerSec > maxRatePerSec) {
    throw new RangeError('throttle.minRatePerSec must not exceed throttle.maxRatePerSec');
  }
  const initialRatePerSec = checkRate(
    'initialRatePerSec',
    o.initialRatePerSec,
    Math.max(1, Math.floor(capacity * hwmRatio)),
  );
  const clampedInitial = Math.min(Math.max(initialRatePerSec, minRatePerSec), maxRatePerSec);
  return {
    // Created for real on first engagement; the placeholder keeps the
    // state shape uniform until then.
    bucket: new TokenBucket(Math.max(1, Math.ceil(clampedInitial)), clampedInitial, now),
    throttled: false,
    ratePerSec: clampedInitial,
    throttledDrops: 0,
    backpressureAtMs: 0,
    sizeAtBackpressure: 0,
    minRatePerSec,
    maxRatePerSec,
    initialRatePerSec: clampedInitial,
  };
}

/**
 * Validates `SubscribeOptions.healthProbe` and builds the initial
 * per-subscriber probe state. Returns `undefined` when the probe is
 * disabled. Throws `RangeError` for invalid values.
 */
function resolveHealthProbeOptions(
  opt: boolean | HealthProbeOptions | undefined,
): Omit<HealthProbeState, 'consecutiveFailures' | 'degraded' | 'autoResumeTimer'> | undefined {
  if (opt == null || opt === false) return undefined;
  const o: HealthProbeOptions = opt === true ? {} : opt;
  const maxConsecutiveFailures = o.maxConsecutiveFailures ?? 5;
  if (!Number.isInteger(maxConsecutiveFailures) || maxConsecutiveFailures < 1) {
    throw new RangeError('healthProbe.maxConsecutiveFailures must be a positive integer');
  }
  const checkPositiveMs = (name: string, value: number | undefined): number | undefined => {
    if (value === undefined) return undefined;
    if (!Number.isFinite(value) || value <= 0) {
      throw new RangeError(`healthProbe.${name} must be a positive finite number of milliseconds`);
    }
    return value;
  };
  return {
    maxConsecutiveFailures,
    processingTimeoutMs: checkPositiveMs('processingTimeoutMs', o.processingTimeoutMs),
    autoResumeAfterMs: checkPositiveMs('autoResumeAfterMs', o.autoResumeAfterMs),
  };
}

/**
 * Publish-side payload validator for one topic or topic pattern (see
 * `EventBus.setTopicSchema`). Return `true` to admit the publish, `false`
 * to reject it. The topic argument is the concrete topic being published
 * to (never a pattern), so one validator registered for a pattern can
 * branch on it. A validator that throws propagates the error to the
 * publish caller — validation runs before any state is mutated for that
 * message, so a throw never leaves a half-published message behind.
 */
export type SchemaValidator = (payload: unknown, topic: string) => boolean;

export class EventBus {
  private subscribers = new Map<string, Subscriber>();
  private subscribersByPattern = new Map<string, number>();
  /**
   * Compiled topic patterns, keyed by the exact pattern string. Each distinct
   * pattern is compiled once and the RegExp is shared by every subscriber on
   * it; the entry is evicted when its last subscriber unsubscribes so the
   * cache cannot grow without bound.
   */
  private patternCache = new Map<string, RegExp>();
  /**
   * Publish-side prefix inverted index: maps a pattern's literal-segment
   * prefix to the ids of the subscribers registered on patterns with that
   * prefix (see `patternPrefixKey`). `fanOut` consults it to prune the
   * compiled-regex tests down to plausible candidates instead of scanning
   * every subscriber. Keys are evicted when their last subscriber leaves,
   * so the index cannot grow without bound.
   */
  private prefixIndex = new Map<string, Set<string>>();
  private topicStats = new Map<
    string,
    {
      subscriberCount: number;
      publishedMessages: number;
      expiredMessages: number;
      lastSeq: number;
      sequenceGaps: number;
      rateLimitedMessages: number;
      rejectedMessages: number;
    }
  >();
  private totalPublished = 0;
  private totalExpired = 0;
  private totalSequenceGaps = 0;
  /**
   * Per-topic sequence counters. Each message published to a topic takes
   * the next number, so every message carries a topic-scoped monotonic
   * `seq` starting at 1. Grows with the distinct topics ever published to —
   * the same bound as `topicStats`.
   */
  private topicSeq = new Map<string, number>();
  private nextId = 0;
  private flushScheduled = false;
  /**
   * Per-topic message TTLs in milliseconds, keyed by the exact string passed
   * to `setTopicTtl` — either a concrete topic name or a wildcard pattern.
   * Insertion order is the tie-break order when several rules match a topic.
   */
  private ttlRules = new Map<string, number>();
  /**
   * Compiled matchers for the wildcard entries in `ttlRules`, kept apart
   * from the subscriber pattern cache so TTL configuration never inflates
   * `patternCacheSize`.
   */
  private ttlMatcherCache = new Map<string, RegExp>();
  /**
   * Per-topic publish rate limits, keyed by the exact string passed to
   * `setTopicRateLimit` — either a concrete topic name or a wildcard
   * pattern. Match resolution is identical to the TTL rules: an exact-topic
   * rule wins over any pattern; between patterns the earliest-registered
   * rule wins.
   */
  private rateLimitRules = new Map<string, { messagesPerSec: number; burst: number }>();
  /**
   * Compiled matchers for the wildcard entries in `rateLimitRules`, kept
   * apart from the subscriber pattern cache and the TTL cache so rate-limit
   * configuration never inflates `patternCacheSize`.
   */
  private rateLimitMatcherCache = new Map<string, RegExp>();
  /**
   * Token buckets per concrete topic, created lazily on the first publish
   * that falls under a rule and refilled from the injected clock
   * (`EventBusOptions.now`), so rate limiting is deterministic in tests.
   * Setting or clearing any rule drops every bucket: a changed rule starts
   * from a fresh budget rather than inheriting a half-spent one.
   */
  private rateLimitBuckets = new Map<string, TokenBucket>();
  private totalRateLimited = 0;
  /**
   * Per-topic schema validators, keyed by the exact string passed to
   * `setTopicSchema` — either a concrete topic name or a wildcard pattern.
   * Match resolution mirrors the TTL and rate-limit rules: an exact-topic
   * rule wins over any pattern; between patterns the earliest-registered
   * rule wins.
   */
  private schemaRules = new Map<string, SchemaValidator>();
  /**
   * Compiled matchers for the wildcard entries in `schemaRules`, kept apart
   * from the subscriber pattern cache so schema configuration never inflates
   * `patternCacheSize`.
   */
  private schemaMatcherCache = new Map<string, RegExp>();
  /**
   * Messages rejected by schema validation (global total). A rejection
   * happens before admission: the payload never becomes a message, so it
   * consumes no sequence number, is never written to the durable log, and
   * never reaches a subscriber queue.
   */
  private totalRejected = 0;
  /**
   * Original TTL deadlines by message, for redelivery. `fanOut` stamps the
   * deadline here when a TTL rule matches; the reliable-subscription
   * requeue path reads it back so a requeued message keeps its
   * publish-time deadline instead of being resurrected after expiry.
   * Entries die with their message (WeakMap).
   */
  private messageDeadlines = new WeakMap<BusMessage, number>();
  /**
   * Consumer-group membership: `groupKey(groupId, pattern)` -> member
   * subscription ids in join order. A group is scoped to one pattern the
   * same way a Kafka consumer group is scoped to its subscription: the same
   * groupId on two patterns is two independent competing sets.
   */
  private groupMembers = new Map<string, string[]>();
  /**
   * Round-robin cursor per group key. Monotonic — the modulo is applied at
   * use — so join/leave churn never repeats or skips an assignment
   * position. Deleted when the group's last member leaves.
   */
  private groupCursors = new Map<string, number>();
  /**
   * Per-group assignment watermark: groupId -> concrete topic -> highest
   * per-topic `seq` assigned to any member of the group. Bounded by
   * (groups x topics), the same order as `topicStats`.
   */
  private groupOffsets = new Map<string, Map<string, number>>();
  /**
   * Consumer-checkpointed offsets: groupId -> concrete topic -> last
   * processed `seq` as reported by the consumer via `commitOffset`. Pure
   * bookkeeping for operators and resume-after-restart; the bus never acts
   * on it by itself.
   */
  private committedOffsets = new Map<string, Map<string, number>>();
  private readonly now: () => number;
  /**
   * Durable topic log, present only when `EventBusOptions.durableLogDir`
   * was given. Every `fanOut` appends the stamped message here; the
   * per-topic sequence counters are seeded from it at construction so a
   * restart never reuses a sequence number.
   */
  private readonly durableLog?: DurableTopicLog;

  constructor(options?: EventBusOptions) {
    this.now = options?.now ?? Date.now;
    if (options?.durableLogDir != null) {
      this.durableLog = DurableTopicLog.open({
        dir: options.durableLogDir,
        maxEntriesPerTopic: options.durableLogMaxEntriesPerTopic,
      });
      // Recover numbering continuity: the next publish on a logged topic
      // continues the on-disk sequence, and stats reflect the recovered
      // history instead of pretending the bus is brand new.
      for (const topic of this.durableLog.topics()) {
        const lastSeq = this.durableLog.lastSeq(topic);
        this.topicSeq.set(topic, lastSeq);
        this.topicStats.set(topic, {
          subscriberCount: 0,
          publishedMessages: this.durableLog.entryCount(topic),
          expiredMessages: 0,
          lastSeq,
          sequenceGaps: 0,
          rateLimitedMessages: 0,
          rejectedMessages: 0,
        });
      }
    }
  }

  /**
   * Configures how long a message lives after being published, in
   * milliseconds, for one topic or topic pattern. `topicPattern` accepts the
   * same wildcard syntax as `subscribe` (or an exact topic name); a message
   * published to a matching topic gets an expiry deadline of
   * `publishTime + ttlMs`, and is silently dropped — counted as expired —
   * if it has not been delivered by then. Topics without a matching rule
   * keep the previous behavior: messages never expire.
   *
   * When several rules match a topic, an exact-topic rule wins over any
   * pattern; between patterns the earliest-registered rule wins. Re-setting
   * a rule replaces it. Throws when `ttlMs` is negative or not finite.
   */
  setTopicTtl(topicPattern: string, ttlMs: number): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (!Number.isFinite(ttlMs) || ttlMs < 0) {
      throw new RangeError('ttlMs must be a non-negative finite number of milliseconds');
    }
    this.ttlRules.set(topicPattern, ttlMs);
  }

  /**
   * Removes the TTL rule previously registered for `topicPattern`.
   * Returns true when a rule existed and was removed.
   */
  clearTopicTtl(topicPattern: string): boolean {
    return this.ttlRules.delete(topicPattern);
  }

  /**
   * Returns the TTL in milliseconds that applies to `topic`, or `undefined`
   * when no rule matches. An exact-topic rule wins over patterns; between
   * patterns the earliest-registered matching rule wins.
   */
  private ttlForTopic(topic: string): number | undefined {
    const exact = this.ttlRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, ttlMs] of this.ttlRules) {
      let matcher = this.ttlMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.ttlMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return ttlMs;
    }
    return undefined;
  }

  /**
   * Caps how many messages per second may be published to topics matching
   * `topicPattern`, enforced by a per-topic token bucket at the publish
   * side. `topicPattern` accepts the same wildcard syntax as `subscribe`
   * (or an exact topic name). The first `burst` messages publish instantly;
   * afterwards the bucket refills at `messagesPerSec` per second.
   *
   * A publish that finds the bucket empty is shed at the publish side: it
   * is never fanned out to any subscriber, never written to the durable
   * log, and never reaches a queue — so subscriber backpressure policies
   * do not churn on it. The shed consumes a sequence number and is counted
   * in `TopicStats.rateLimitedMessages` (and the global total); subscribers
   * observe the loss as a sequence gap, the same visibility backpressure
   * drops and TTL expirations get. `publish` returns 0 for a shed message.
   *
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * patterns, the earliest-registered matching pattern wins. Re-setting a
   * rule replaces it and resets that topic's budget. Throws when
   * `topicPattern` is empty, when `messagesPerSec` is not a positive finite
   * number, or when `burst` is not a positive finite number.
   */
  setTopicRateLimit(topicPattern: string, messagesPerSec: number, opts?: { burst?: number }): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (!Number.isFinite(messagesPerSec) || messagesPerSec <= 0) {
      throw new RangeError('messagesPerSec must be a positive finite number of messages per second');
    }
    const burst = opts?.burst ?? Math.max(1, Math.ceil(messagesPerSec));
    if (!Number.isFinite(burst) || burst <= 0) {
      throw new RangeError('burst must be a positive finite number of messages');
    }
    this.rateLimitRules.set(topicPattern, { messagesPerSec, burst });
    this.rateLimitBuckets.clear();
  }

  /**
   * Removes the rate-limit rule previously registered for `topicPattern`.
   * Returns true when a rule existed and was removed.
   */
  clearTopicRateLimit(topicPattern: string): boolean {
    const removed = this.rateLimitRules.delete(topicPattern);
    if (removed) this.rateLimitBuckets.clear();
    return removed;
  }

  /**
   * Returns the rate limit that applies to `topic`, or `undefined` when no
   * rule matches. Same precedence as `ttlForTopic`.
   */
  private rateLimitForTopic(topic: string): { messagesPerSec: number; burst: number } | undefined {
    const exact = this.rateLimitRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, limit] of this.rateLimitRules) {
      let matcher = this.rateLimitMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.rateLimitMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return limit;
    }
    return undefined;
  }

  /**
   * Registers a publish-side schema validator for one topic or topic
   * pattern — an admission gate for malformed payloads. `topicPattern`
   * accepts the same wildcard syntax as `subscribe` (or an exact topic
   * name); a publish whose payload makes the validator return `false` is
   * rejected: it is never fanned out, never written to the durable log,
   * and never reaches a subscriber queue — `publish` returns 0 for it.
   *
   * A rejection happens before admission, so it consumes no sequence
   * number (subscribers see no gap), does not burn rate-limit budget, and
   * is counted separately in `TopicStats.rejectedMessages` (and the global
   * total). A validator that throws propagates the error to the publish
   * caller; validation runs before any state is mutated for that message.
   *
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * patterns, the earliest-registered matching pattern wins. Re-setting a
   * rule replaces it. Throws when `topicPattern` is empty or `validator`
   * is not a function.
   */
  setTopicSchema(topicPattern: string, validator: SchemaValidator): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (typeof validator !== 'function') {
      throw new RangeError('validator must be a function');
    }
    this.schemaRules.set(topicPattern, validator);
  }

  /**
   * Removes the schema validator previously registered for `topicPattern`.
   * Returns true when a rule existed and was removed.
   */
  clearTopicSchema(topicPattern: string): boolean {
    return this.schemaRules.delete(topicPattern);
  }

  /**
   * Returns the schema validator that applies to `topic`, or `undefined`
   * when no rule matches. Same precedence as `ttlForTopic`.
   */
  private schemaForTopic(topic: string): SchemaValidator | undefined {
    const exact = this.schemaRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, validator] of this.schemaRules) {
      let matcher = this.schemaMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.schemaMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return validator;
    }
    return undefined;
  }

  /**
   * Registers a handler for topics matching `topicPattern`.
   * `*` matches a single topic segment (`market.*` matches `market.btc`);
   * `**` matches zero or more segments (`market.**` matches `market`,
   * `market.btc` and `market.btc.trades`); a bare `*` or `**` matches every
   * topic.
   *
   * Pass `onBackpressure` in `opts` to be notified when this subscriber falls
   * behind (its queue reaches the high-water mark); `droppedCount` reports how
   * many of its messages were shed.
   */
  subscribe(topicPattern: string, handler: MessageHandler, opts?: SubscribeOptions): Subscription {
    // Validate the durable-log resume options before registering anything:
    // a throw must not leave a half-registered subscriber behind.
    const resumeFromSeq = opts?.resumeFromSeq;
    if (resumeFromSeq !== undefined) {
      if (this.durableLog == null) {
        throw new RangeError('resumeFromSeq requires EventBusOptions.durableLogDir to be set');
      }
      if (!Number.isInteger(resumeFromSeq) || resumeFromSeq < 0) {
        throw new RangeError('resumeFromSeq must be a non-negative integer');
      }
    }
    const id = `sub-${++this.nextId}`;
    const capacity = opts?.queueSize ?? 100;
    const onBackpressure = opts?.onBackpressure;
    const onDrained = opts?.onDrained;
    const onThrottled = opts?.onThrottled;
    const hwmRatio = opts?.highWaterMarkRatio ?? 0.8;
    const throttle = resolveThrottleOptions(opts?.throttle, capacity, hwmRatio, this.now);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const healthProbe = resolveHealthProbeOptions(opts?.healthProbe);
    const onDegraded = opts?.onDegraded;
    const queue = new BoundedQueue<BusMessage>({
      capacity,
      policy: opts?.dropPolicy ?? 'drop-oldest',
      highWaterMarkRatio: opts?.highWaterMarkRatio,
      // The internal wrappers are registered whenever throttling is enabled,
      // even without user callbacks: the bus needs the excursion signals to
      // engage/disengage the throttle.
      ...(onBackpressure == null && throttle == null
        ? {}
        : {
            onHighWaterMark: (size: number) => {
              if (throttle != null) {
                this.engageThrottle(subscriber, size, capacity);
              }
              onBackpressure?.({
                subscriberId: id,
                pattern: topicPattern,
                queueSize: size,
                capacity,
                dropped: queue.droppedCount,
              });
            },
          }),
      ...(onDrained == null && throttle == null
        ? {}
        : {
            onDrained: (size: number) => {
              if (throttle != null) {
                this.releaseThrottle(subscriber, size);
              }
              onDrained?.({
                subscriberId: id,
                pattern: topicPattern,
                queueSize: size,
                capacity,
                dropped: queue.droppedCount,
                highWaterMark: queue.highWaterMark,
              });
            },
          }),
    });
    const subscriber: Subscriber = {
      id,
      pattern: topicPattern,
      matcher: this.compiledMatcher(topicPattern),
      handler,
      queue,
      lastDeliveredSeq: new Map(),
      throttle,
      onThrottled,
      health:
        healthProbe == null
          ? undefined
          : { consecutiveFailures: 0, degraded: false, ...healthProbe },
      onDegraded,
    };
    this.subscribers.set(id, subscriber);
    this.subscribersByPattern.set(topicPattern, (this.subscribersByPattern.get(topicPattern) ?? 0) + 1);
    // File the subscriber in the publish-side prefix index under its
    // pattern's literal prefix (see `patternPrefixKey`).
    const indexKey = EventBus.patternPrefixKey(topicPattern);
    let indexBucket = this.prefixIndex.get(indexKey);
    if (indexBucket == null) {
      indexBucket = new Set<string>();
      this.prefixIndex.set(indexKey, indexBucket);
    }
    indexBucket.add(id);

    // Durable-log resume: pre-fill the queue with logged messages the
    // subscriber missed (seq > resumeFromSeq on matching topics), in
    // publish-time order per topic, before any live message. Queue push
    // applies the subscriber's normal backpressure policy to replayed
    // messages too; a flush is scheduled so they are delivered promptly.
    if (resumeFromSeq !== undefined) {
      this.replayLog(subscriber, resumeFromSeq);
    }

    return {
      id,
      unsubscribe: () => {
        if (!this.subscribers.delete(id)) return;
        // A pending auto-resume must not outlive the subscriber: no phantom
        // resume (and no rescheduled flush) after unsubscribe.
        const health = subscriber.health;
        if (health?.autoResumeTimer !== undefined) {
          clearTimeout(health.autoResumeTimer);
          health.autoResumeTimer = undefined;
        }
        const bucket = this.prefixIndex.get(indexKey);
        if (bucket != null) {
          bucket.delete(id);
          // Evict emptied keys so a churn of short-lived patterns cannot
          // grow the index without bound.
          if (bucket.size === 0) this.prefixIndex.delete(indexKey);
        }
        const remaining = (this.subscribersByPattern.get(topicPattern) ?? 1) - 1;
        if (remaining <= 0) {
          this.subscribersByPattern.delete(topicPattern);
          this.patternCache.delete(topicPattern);
        } else {
          this.subscribersByPattern.set(topicPattern, remaining);
        }
      },
    };
  }

  /**
   * Registers a reliable handler for topics matching `topicPattern`:
   * at-least-once delivery semantics. The handler receives a `Delivery`
   * envelope instead of a bare message — call `delivery.ack()` to confirm
   * receipt, or `delivery.nack()` to requeue the message for redelivery. A
   * delivery that is neither acked nor nacked within `ackTimeoutMs` is
   * requeued automatically, so a stalled consumer cannot lose messages
   * silently.
   *
   * Redelivery preserves FIFO order (requeued at the tail, so a redelivered
   * message never jumps ahead of newer ones) and the message's original TTL
   * deadline — a message that outlived its TTL is dropped as expired on the
   * next drain, not resurrected. Redelivery is at-least-once, not
   * exactly-once: `delivery.redeliveries` counts prior requeues so consumers
   * can spot poison messages. All queue options (`queueSize`, `dropPolicy`,
   * `onBackpressure`) behave as in `subscribe`.
   */
  subscribeReliable(
    topicPattern: string,
    handler: ReliableMessageHandler,
    opts?: ReliableSubscribeOptions,
  ): Subscription {
    // The bus flush calls `Subscriber.handler` with bare messages; wrap it
    // so reliable subscribers transparently get tracked deliveries instead.
    // Assigned synchronously here, before any flush microtask can run.
    let deliver!: (msg: BusMessage) => void;
    const sub = this.subscribe(topicPattern, (msg) => deliver(msg), opts);
    const subscriber = this.subscribers.get(sub.id);
    if (subscriber == null) throw new Error(`unknown subscriber: ${sub.id}`);
    const redeliveries = new WeakMap<BusMessage, number>();
    const tracker = new AckTracker<BusMessage>({
      ackTimeoutMs: opts?.ackTimeoutMs ?? 5000,
      onRedeliver: (msg) => {
        redeliveries.set(msg, (redeliveries.get(msg) ?? 0) + 1);
        subscriber.queue.push(msg, 0, this.messageDeadlines.get(msg));
        this.scheduleFlush();
      },
    });
    subscriber.reliable = { tracker, redeliveries };
    deliver = (msg) => {
      handler(tracker.track(msg, redeliveries.get(msg) ?? 0));
    };
    return {
      id: sub.id,
      unsubscribe: () => {
        // Cancel pending ack timers first: no phantom redeliveries after unsubscribe.
        tracker.clear();
        sub.unsubscribe();
      },
    };
  }

  /**
   * Registers a consumer-group member for topics matching `topicPattern` —
   * Kafka-style competing consumers inside one process.
   *
   * Semantics:
   * - Members of the same `(groupId, topicPattern)` form one competing
   *   set: each published message matching the pattern is delivered to
   *   exactly one member, picked round-robin in join order. Assignment
   *   does not skip slow members — a member whose queue is full sheds via
   *   its own drop policy, exactly as a plain subscriber would.
   * - Different groups on the same pattern each receive a copy (broadcast
   *   across groups); the same groupId on different patterns is an
   *   independent competing set per pattern.
   * - Membership changes rebalance implicitly: a joining member is
   *   appended to the rotation, a leaving member is removed, and every
   *   affected member's `onRebalance` fires with the new roster. No
   *   partition ownership is tracked — the bus assigns per message, so
   *   there is nothing to revoke.
   * - The bus tracks the assignment watermark per group and topic
   *   (`getGroupOffsets`): the highest per-topic `seq` handed to any
   *   member. Consumers checkpoint their own progress with
   *   `commitOffset`; combined with a durable topic log
   *   (`EventBusOptions.durableLogDir` + `SubscribeOptions.resumeFromSeq`)
   *   a rejoining member can resume where it left off.
   *
   * All queue options (`queueSize`, `dropPolicy`, `onBackpressure`,
   * `throttle`, ...) behave per member exactly as in `subscribe`. Throws
   * `RangeError` when `groupId` is empty.
   */
  subscribeToGroup(
    groupId: string,
    topicPattern: string,
    handler: MessageHandler,
    opts?: GroupSubscribeOptions,
  ): Subscription {
    if (groupId.length === 0) {
      throw new RangeError('groupId must be a non-empty string');
    }
    const sub = this.subscribe(topicPattern, handler, opts);
    const subscriber = this.subscribers.get(sub.id);
    if (subscriber == null) throw new Error(`unknown subscriber: ${sub.id}`);
    subscriber.groupId = groupId;
    subscriber.onRebalance = opts?.onRebalance;
    const key = EventBus.groupKey(groupId, topicPattern);
    let members = this.groupMembers.get(key);
    if (members == null) {
      members = [];
      this.groupMembers.set(key, members);
    }
    members.push(sub.id);
    this.fireRebalance(groupId, topicPattern, key, 'join', sub.id);
    return {
      id: sub.id,
      unsubscribe: () => {
        // Second call is a no-op: no double leave-event, no roster churn.
        if (!this.subscribers.has(sub.id)) return;
        this.removeGroupMember(key, sub.id);
        sub.unsubscribe();
        this.fireRebalance(groupId, topicPattern, key, 'leave', sub.id);
      },
    };
  }

  /**
   * Map key for one competing set: groupId and pattern joined by NUL.
   * Neither may contain NUL in practice, so the pairing is unambiguous and
   * reversible (see `getStats`).
   */
  private static groupKey(groupId: string, pattern: string): string {
    return `${groupId}\0${pattern}`;
  }

  /**
   * Removes a member from its competing set. Emptied sets (and their
   * round-robin cursors) are deleted so churn of short-lived groups cannot
   * grow the maps without bound; assignment watermarks and committed
   * offsets are history and are kept.
   */
  private removeGroupMember(key: string, memberId: string): void {
    const members = this.groupMembers.get(key);
    if (members == null) return;
    const idx = members.indexOf(memberId);
    if (idx >= 0) members.splice(idx, 1);
    if (members.length === 0) {
      this.groupMembers.delete(key);
      this.groupCursors.delete(key);
    }
  }

  /**
   * Notifies every current member of a group about a membership change.
   * Each member receives its own snapshot of the post-change roster.
   */
  private fireRebalance(
    groupId: string,
    pattern: string,
    key: string,
    trigger: 'join' | 'leave',
    memberId: string,
  ): void {
    const members = this.groupMembers.get(key);
    if (members == null) return;
    for (const id of members) {
      const subscriber = this.subscribers.get(id);
      subscriber?.onRebalance?.({
        groupId,
        pattern,
        members: [...members],
        trigger,
        memberId,
      });
    }
  }

  /**
   * Picks the next assignee for a group: round-robin over the members that
   * matched this publish, in join order. The cursor is monotonic and the
   * modulo is applied at use, so members joining or leaving mid-stream
   * never cause a repeated or skipped position.
   */
  private assignGroupMember(key: string, members: Subscriber[]): Subscriber {
    const cursor = this.groupCursors.get(key) ?? 0;
    const assignee = members[cursor % members.length];
    this.groupCursors.set(key, cursor + 1);
    return assignee;
  }

  /**
   * Advances a group's assignment watermark for a topic. Assignment order
   * follows publish order, which follows `seq` order, so the newest
   * assignment is always the maximum.
   */
  private recordGroupOffset(groupId: string, topic: string, seq: number): void {
    let offsets = this.groupOffsets.get(groupId);
    if (offsets == null) {
      offsets = new Map<string, number>();
      this.groupOffsets.set(groupId, offsets);
    }
    offsets.set(topic, seq);
  }

  /**
   * Assignment watermark per concrete topic for a group: the highest
   * per-topic `seq` handed to any member of the group so far. Empty when
   * the group has received nothing (or does not exist). This is what a
   * rejoining member resumes from.
   */
  getGroupOffsets(groupId: string): Record<string, number> {
    const offsets = this.groupOffsets.get(groupId);
    return offsets == null ? {} : Object.fromEntries(offsets);
  }

  /**
   * Records a consumer-side checkpoint: the last per-topic `seq` the
   * caller has durably processed for a group. The bus never acts on
   * committed offsets by itself — they exist so operators can observe lag
   * (`getGroupOffsets` minus `getCommittedOffsets`) and so a restarted
   * consumer can resubscribe with `SubscribeOptions.resumeFromSeq` seeded
   * from here. Committing for a group with no live members is allowed:
   * that is exactly the restore-before-rejoin case.
   *
   * Throws `RangeError` on an empty groupId/topic or a non-positive
   * non-integer seq.
   */
  commitOffset(groupId: string, topic: string, seq: number): void {
    if (groupId.length === 0) {
      throw new RangeError('groupId must be a non-empty string');
    }
    if (topic.length === 0) {
      throw new RangeError('topic must be a non-empty string');
    }
    if (!Number.isInteger(seq) || seq < 1) {
      throw new RangeError('seq must be a positive integer sequence number');
    }
    let committed = this.committedOffsets.get(groupId);
    if (committed == null) {
      committed = new Map<string, number>();
      this.committedOffsets.set(groupId, committed);
    }
    committed.set(topic, seq);
  }

  /**
   * Consumer-checkpointed offsets per concrete topic for a group (see
   * `commitOffset`). Empty when nothing has been committed.
   */
  getCommittedOffsets(groupId: string): Record<string, number> {
    const committed = this.committedOffsets.get(groupId);
    return committed == null ? {} : Object.fromEntries(committed);
  }

  /**
   * Subscription ids of the current members of one competing set, in join
   * order. Empty when the group/pattern has no live members.
   */
  getGroupMembers(groupId: string, pattern: string): string[] {
    return [...(this.groupMembers.get(EventBus.groupKey(groupId, pattern)) ?? [])];
  }

  /**
   * Returns the inverted-index key for a topic pattern: the dot-joined
   * literal segments before the pattern's first wildcard (`*` / `**`)
   * segment, or the empty string when the pattern starts with a wildcard.
   * Examples: `market.btc.*` → `market.btc`; `market.**` → `market`;
   * `**`, `*`, `*.foo`, `**.foo` → `''`; exact `a.b` → `a.b`.
   */
  private static patternPrefixKey(pattern: string): string {
    const segments = pattern.split('.');
    let end = 0;
    while (end < segments.length && segments[end] !== '*' && segments[end] !== '**') {
      end += 1;
    }
    return segments.slice(0, end).join('.');
  }

  /**
   * Collects the candidate subscriber ids for a publish to `topic` from the
   * prefix index. Candidate keys are the full topic, every progressively
   * shorter segment prefix, and finally the empty key (patterns whose first
   * segment is a wildcard).
   *
   * No-miss invariant (why the candidates are a safe over-approximation):
   * if a pattern's compiled matcher accepts `topic`, then every literal
   * segment before the pattern's first wildcard must equal the
   * corresponding leading topic segment — the matcher is anchored and
   * consumes the literal segments verbatim before any wildcard can absorb
   * separators. So the pattern's literal prefix is exactly the first j
   * topic segments for some j (j = 0 when the pattern starts with a
   * wildcard), and the pattern's index key is always one of the candidate
   * keys above. `fanOut` therefore tests every true match and never skips
   * one. Non-matching patterns may still appear as candidates (e.g. an
   * exact `a.b` pattern when publishing `a.b.c`); their compiled regex —
   * still the final authority in `fanOut` — rejects them.
   */
  private candidateIds(topic: string): Set<string> {
    const candidates = new Set<string>();
    const segments = topic.split('.');
    let prefix = '';
    for (const segment of segments) {
      prefix = prefix === '' ? segment : `${prefix}.${segment}`;
      const bucket = this.prefixIndex.get(prefix);
      if (bucket != null) {
        for (const id of bucket) candidates.add(id);
      }
    }
    const wildcards = this.prefixIndex.get('');
    if (wildcards != null) {
      for (const id of wildcards) candidates.add(id);
    }
    return candidates;
  }

  /**
   * Returns the cached compiled RegExp for `pattern`, compiling it on first
   * use. Subscribers on the same pattern share one RegExp instance.
   */
  private compiledMatcher(pattern: string): RegExp {
    let matcher = this.patternCache.get(pattern);
    if (matcher == null) {
      matcher = compilePattern(pattern);
      this.patternCache.set(pattern, matcher);
    }
    return matcher;
  }

  /**
   * Replays durable-log records into a resubscribing subscriber's queue:
   * every logged message on a topic matching the subscriber's pattern with
   * `seq` greater than `fromSeq` is re-enqueued with its original `seq` and
   * TTL deadline. Cross-topic order follows publish time (`at`), then seq,
   * then topic name — deterministic; per-topic order follows `seq`, the
   * same order live delivery gives. Expired deadlines are kept on the
   * message so the drain drops them as expired instead of resurrecting
   * stale data.
   *
   * Replayed messages go through the subscriber's normal queue (and its
   * backpressure policy) but bypass consumer-group assignment: group replay
   * is per member, from each member's own offset.
   */
  private replayLog(subscriber: Subscriber, fromSeq: number): void {
    const log = this.durableLog;
    if (log == null) return;
    const matcher = subscriber.matcher;
    const records: Array<{ seq: number; topic: string; at: number; expiresAt?: number; payload: unknown }> = [];
    for (const topic of log.topics()) {
      if (!matcher.test(topic)) continue;
      for (const rec of log.readSince(topic, fromSeq)) records.push(rec);
    }
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    for (const rec of records) {
      const msg: BusMessage = { topic: rec.topic, payload: rec.payload, seq: rec.seq };
      if (rec.expiresAt !== undefined) this.messageDeadlines.set(msg, rec.expiresAt);
      subscriber.queue.push(msg, 0, rec.expiresAt);
    }
    this.scheduleFlush();
  }

  /**
   * Fans the message out to every matching subscriber's bounded queue and
   * returns the number of subscribers whose queue accepted the message.
   * When a queue is full, the subscriber's drop policy sheds a message and
   * counts the drop (see `droppedCount`); delivery happens on the next
   * microtask so slow consumers exert real backpressure. A publish rejected
   * by a schema validator (see `setTopicSchema`) is never fanned out and
   * returns 0.
   */
  publish(topic: string, payload: unknown): number {
    const { accepted } = this.fanOut(topic, payload);
    this.scheduleFlush();
    return accepted;
  }

  /**
   * Fans out every message in the batch to its matching subscribers' bounded
   * queues, then schedules a single flush for the whole batch — so N messages
   * cost one event-loop round instead of N, at the price of one larger drain.
   * Matching, drop policies, and backpressure callbacks behave exactly as
   * they do for `publish`; returns the total number of accepted deliveries.
   * An empty batch is a no-op that schedules no flush.
   *
   * Callers do not supply `seq`: the bus assigns each message its per-topic
   * sequence number at fan-out time, in batch order.
   */
  publishBatch(messages: Array<Omit<BusMessage, 'seq'>>): number {
    if (messages.length === 0) return 0;
    let accepted = 0;
    for (const msg of messages) {
      accepted += this.fanOut(msg.topic, msg.payload).accepted;
    }
    this.scheduleFlush();
    return accepted;
  }

  /**
   * Hands out the next per-topic sequence number, starting at 1. The
   * counter is per concrete topic and never resets, so every message ever
   * published to a topic carries a unique, gap-free number at publish
   * time — gaps only ever appear downstream, when a subscriber's queue
   * drops or expires a message.
   */
  private nextSeq(topic: string): number {
    const seq = (this.topicSeq.get(topic) ?? 0) + 1;
    this.topicSeq.set(topic, seq);
    return seq;
  }

  /**
   * Pushes one message into a subscriber's queue, honoring adaptive
   * publish-side throttling. Returns true when the queue accepted the
   * message. Shared by the plain fan-out path and the consumer-group
   * assignment path so both get identical backpressure semantics.
   */
  private deliverToSubscriber(
    subscriber: Subscriber,
    msg: BusMessage,
    expiresAt: number | undefined,
  ): boolean {
    const throttle = subscriber.throttle;
    if (throttle != null && throttle.throttled && !throttle.bucket.take()) {
      // Publish-side shed: the message never reaches the queue, so the
      // drop policy never churns on it. Counted separately from queue
      // drops; sequence-gap detection surfaces the loss downstream.
      throttle.throttledDrops += 1;
      return false;
    }
    return subscriber.queue.push(msg, 0, expiresAt) === 'accepted';
  }

  /**
   * Pushes one message into every matching subscriber's queue and records
   * per-topic stats. Returns how many subscribers matched and how many
   * queues accepted the message (they differ when backpressure drops kick
   * in). Does not schedule a flush — callers do that once per batch.
   *
   * The message is stamped with its per-topic sequence number here, so all
   * subscribers see the same `seq` for the same publish regardless of queue
   * state. `TopicStats.lastSeq` tracks the highest number handed out.
   *
   * Consumer-group members compete: all members of one (groupId, pattern)
   * that match the topic are collected first, then a single member is
   * picked round-robin and receives the group's one copy. `matched` counts
   * one per matching group — the actual fan-out width.
   *
   * When a TTL rule matches the topic, every enqueued copy is stamped with
   * the same expiry deadline (`publishTime + ttlMs`); the queues discard
   * expired copies at drain time and count them as expired.
   */
  private fanOut(topic: string, payload: unknown): { matched: number; accepted: number } {
    // Publish-side schema validation runs before admission: a rejected
    // payload never becomes a message — no sequence number is consumed
    // (subscribers see no gap), the durable log never sees it, and it
    // does not burn rate-limit budget. Rejection is counted on the
    // topic's stats entry, which is created here when the topic has never
    // published anything valid yet.
    const validator = this.schemaForTopic(topic);
    if (validator !== undefined && !validator(payload, topic)) {
      let rejectedStats = this.topicStats.get(topic);
      if (rejectedStats == null) {
        rejectedStats = {
          subscriberCount: 0,
          publishedMessages: 0,
          expiredMessages: 0,
          lastSeq: 0,
          sequenceGaps: 0,
          rateLimitedMessages: 0,
          rejectedMessages: 0,
        };
        this.topicStats.set(topic, rejectedStats);
      }
      rejectedStats.rejectedMessages += 1;
      this.totalRejected += 1;
      return { matched: 0, accepted: 0 };
    }
    const msg: BusMessage = { topic, payload, seq: this.nextSeq(topic) };
    let stats = this.topicStats.get(topic);
    if (stats == null) {
      stats = {
        subscriberCount: 0,
        publishedMessages: 0,
        expiredMessages: 0,
        lastSeq: 0,
        sequenceGaps: 0,
        rateLimitedMessages: 0,
        rejectedMessages: 0,
      };
      this.topicStats.set(topic, stats);
    }
    stats.publishedMessages += 1;
    stats.lastSeq = msg.seq;
    this.totalPublished += 1;
    // Publish-side per-topic rate limiting: when the topic's token bucket
    // is empty the message is shed here — never fanned out, never logged,
    // never queued. The shed consumes the sequence number stamped above so
    // subscribers observe the loss as a sequence gap. The per-topic stats
    // entry is updated above the shed so `publishedMessages` and `lastSeq`
    // stay consistent with what `getStats` reports for accepted traffic.
    const limit = this.rateLimitForTopic(topic);
    if (limit !== undefined) {
      let bucket = this.rateLimitBuckets.get(topic);
      if (bucket == null) {
        bucket = new TokenBucket(limit.burst, limit.messagesPerSec, this.now);
        this.rateLimitBuckets.set(topic, bucket);
      }
      if (!bucket.take()) {
        stats.rateLimitedMessages += 1;
        this.totalRateLimited += 1;
        return { matched: 0, accepted: 0 };
      }
    }
    let matched = 0;
    let accepted = 0;
    // One clock reading for the publish: TTL deadline and log timestamp stay
    // consistent even if the injected clock moves between the two.
    const nowMs = this.now();
    const ttlMs = this.ttlForTopic(topic);
    const expiresAt = ttlMs === undefined ? undefined : nowMs + ttlMs;
    if (expiresAt !== undefined) this.messageDeadlines.set(msg, expiresAt);
    // Durable log (opt-in): persist the stamped message before fan-out, so a
    // crash between publish and delivery still leaves it replayable. Logging
    // never throws into the publish path — see `DurableTopicLog.append`.
    this.durableLog?.append({ seq: msg.seq, topic, at: nowMs, expiresAt, payload });
    // The prefix index prunes the regex tests down to subscribers whose
    // pattern's literal prefix can plausibly match the topic; the compiled
    // regex stays the final authority, and the no-miss invariant in
    // `candidateIds` keeps matching semantics identical to the old full
    // scan. Iteration order (insertion order) is unchanged.
    const candidates = this.candidateIds(topic);
    // Degenerate case: when every subscriber is a candidate (e.g. all on
    // `**`), the membership check would pass for all of them, so skip it.
    // Semantically identical, avoids a Set lookup per subscriber.
    const prune = candidates.size < this.subscribers.size;
    // Group members are collected per competing set first; the group's
    // single copy is assigned round-robin after the scan.
    const groupHits = new Map<string, { groupId: string; members: Subscriber[] }>();
    for (const subscriber of this.subscribers.values()) {
      if (prune && !candidates.has(subscriber.id)) continue;
      if (!subscriber.matcher.test(topic)) continue;
      if (subscriber.groupId == null) {
        matched += 1;
        if (this.deliverToSubscriber(subscriber, msg, expiresAt)) accepted += 1;
        continue;
      }
      const key = EventBus.groupKey(subscriber.groupId, subscriber.pattern);
      let hit = groupHits.get(key);
      if (hit == null) {
        hit = { groupId: subscriber.groupId, members: [] };
        groupHits.set(key, hit);
      }
      hit.members.push(subscriber);
    }
    for (const [key, hit] of groupHits) {
      matched += 1;
      const assignee = this.assignGroupMember(key, hit.members);
      if (this.deliverToSubscriber(assignee, msg, expiresAt)) accepted += 1;
      this.recordGroupOffset(hit.groupId, topic, msg.seq);
    }
    stats.subscriberCount = matched;
    return { matched, accepted };
  }

  /**
   * Returns a point-in-time snapshot of live bus metrics: active
   * subscriptions (total and per pattern) plus per-topic fan-out widths and
   * publish counts. The snapshot is a plain-data copy — mutating it does
   * not affect the bus.
   */
  getStats(): BusStats {
    let unackedDeliveries = 0;
    let throttledSubscribers = 0;
    let degradedSubscribers = 0;
    for (const subscriber of this.subscribers.values()) {
      unackedDeliveries += subscriber.reliable?.tracker.unackedCount ?? 0;
      if (subscriber.throttle?.throttled === true) throttledSubscribers += 1;
      if (subscriber.health?.degraded === true) degradedSubscribers += 1;
    }
    return {
      totalSubscribers: this.subscribers.size,
      subscribersByPattern: Object.fromEntries(this.subscribersByPattern),
      totalPublished: this.totalPublished,
      expiredMessages: this.totalExpired,
      unackedDeliveries,
      patternCacheSize: this.patternCache.size,
      indexSize: this.prefixIndex.size,
      sequenceGaps: this.totalSequenceGaps,
      rateLimitedMessages: this.totalRateLimited,
      rejectedMessages: this.totalRejected,
      throttledSubscribers,
      degradedSubscribers,
      consumerGroups: [...this.groupMembers.entries()].map(([key, members]) => {
        const sep = key.indexOf('\0');
        return {
          groupId: key.slice(0, sep),
          pattern: key.slice(sep + 1),
          members: members.length,
        };
      }),
      topics: [...this.topicStats.entries()].map(([topic, stats]) => ({
        topic,
        subscriberCount: stats.subscriberCount,
        publishedMessages: stats.publishedMessages,
        expiredMessages: stats.expiredMessages,
        lastSeq: stats.lastSeq,
        sequenceGaps: stats.sequenceGaps,
        rateLimitedMessages: stats.rateLimitedMessages,
        rejectedMessages: stats.rejectedMessages,
      })),
      ...(this.durableLog == null
        ? {}
        : {
            durableLog: {
              dir: this.durableLog.dir,
              ...this.durableLog.stats(),
            },
          }),
    };
  }

  /** Number of active subscriptions. */
  subscriberCount(): number {
    return this.subscribers.size;
  }

  /** Messages currently buffered for a subscriber, awaiting delivery. */
  pendingCount(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return subscriber.queue.size;
  }

  /** Messages dropped for a subscriber due to backpressure. */
  droppedCount(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return subscriber.queue.droppedCount;
  }

  /**
   * Messages shed at the publish side for a subscriber by adaptive
   * throttling (see `SubscribeOptions.throttle`). Always 0 when throttling
   * is disabled for the subscriber.
   */
  throttledCount(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return subscriber.throttle?.throttledDrops ?? 0;
  }

  /**
   * Resumes delivery to a subscriber that health probing auto-paused (see
   * `SubscribeOptions.healthProbe`). The messages preserved in its queue
   * are redelivered in order on the next flush, and its consecutive-failure
   * counter restarts at 0. Returns true when the subscriber was degraded
   * and is now resumed, false when it was not paused (or the probe is
   * disabled for it). Throws on an unknown subscriber id.
   */
  resume(subId: string): boolean {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    if (subscriber.health == null || !subscriber.health.degraded) return false;
    this.resumeDelivery(subscriber);
    return true;
  }

  /**
   * Point-in-time health snapshot for one subscriber (see
   * `SubscribeOptions.healthProbe`). `enabled` is false and the counters
   * are 0 when the probe is not enabled for the subscriber. Throws on an
   * unknown subscriber id.
   */
  subscriberHealth(subId: string): SubscriberHealth {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    const health = subscriber.health;
    if (health == null) {
      return { subscriberId: subId, enabled: false, degraded: false, consecutiveFailures: 0 };
    }
    return {
      subscriberId: subId,
      enabled: true,
      degraded: health.degraded,
      consecutiveFailures: health.consecutiveFailures,
    };
  }

  /**
   * Clears a degraded state — manual (`resume`) or automatic (cooldown
   * timer): the consecutive-failure counter restarts at 0, any pending
   * auto-resume timer is cancelled (it already fired, or is superseded),
   * and a flush is scheduled so the preserved backlog is delivered
   * promptly.
   */
  private resumeDelivery(subscriber: Subscriber): void {
    const health = subscriber.health;
    if (health == null) return;
    health.degraded = false;
    health.consecutiveFailures = 0;
    if (health.autoResumeTimer !== undefined) {
      clearTimeout(health.autoResumeTimer);
      health.autoResumeTimer = undefined;
    }
    this.scheduleFlush();
  }

  /**
   * Adjusts a subscriber's high-water-mark ratio at runtime (fraction of its
   * queue capacity, in (0, 1]). If the subscriber is mid-excursion and its
   * queue is already below the new mark, the excursion ends immediately and
   * `onDrained` fires synchronously.
   */
  setHighWaterMarkRatio(subId: string, ratio: number): void {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    subscriber.queue.setHighWaterMarkRatio(ratio);
  }

  /**
   * Deliveries currently outstanding — handed to this reliable subscriber
   * but not yet acked or nacked. Always 0 for plain `subscribe`
   * subscriptions.
   */
  unackedCount(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return subscriber.reliable?.tracker.unackedCount ?? 0;
  }

  /**
   * Engages adaptive publish-side throttling for a subscriber whose queue
   * crossed the high-water mark. The enforced rate is the drain rate measured
   * during the previous excursion when one exists (adapted to the consumer's
   * real speed), otherwise the configured initial rate. A fresh token bucket
   * with one second's worth of burst starts full, so the engagement itself
   * never sheds the messages already in flight.
   */
  private engageThrottle(subscriber: Subscriber, size: number, capacity: number): void {
    const throttle = subscriber.throttle;
    if (throttle == null || throttle.throttled) return;
    const adapted = throttle.observedDrainRatePerSec !== undefined;
    const target = adapted ? throttle.observedDrainRatePerSec! : throttle.initialRatePerSec;
    throttle.ratePerSec = Math.min(Math.max(target, throttle.minRatePerSec), throttle.maxRatePerSec);
    throttle.bucket = new TokenBucket(
      Math.max(1, Math.ceil(throttle.ratePerSec)),
      throttle.ratePerSec,
      this.now,
    );
    throttle.throttled = true;
    throttle.backpressureAtMs = this.now();
    throttle.sizeAtBackpressure = size;
    subscriber.onThrottled?.({
      subscriberId: subscriber.id,
      pattern: subscriber.pattern,
      ratePerSec: throttle.ratePerSec,
      queueSize: size,
      capacity,
      adapted,
    });
  }

  /**
   * Disengages throttling when the subscriber's queue drains below the
   * high-water mark: full speed resumes immediately, and the drain rate
   * measured over this excursion seeds the next engagement. A zero or
   * negative elapsed time (e.g. the excursion ended via
   * `setHighWaterMarkRatio` in the same millisecond) keeps the previous
   * observation instead of recording a bogus rate.
   */
  private releaseThrottle(subscriber: Subscriber, size: number): void {
    const throttle = subscriber.throttle;
    if (throttle == null || !throttle.throttled) return;
    const elapsedMs = this.now() - throttle.backpressureAtMs;
    if (elapsedMs > 0) {
      const observedPerSec = ((throttle.sizeAtBackpressure - size) / elapsedMs) * 1000;
      if (observedPerSec >= 0) {
        throttle.observedDrainRatePerSec = Math.min(
          Math.max(observedPerSec, throttle.minRatePerSec),
          throttle.maxRatePerSec,
        );
      }
    }
    throttle.throttled = false;
  }

  private scheduleFlush(): void {
    if (this.flushScheduled) return;
    this.flushScheduled = true;
    queueMicrotask(() => {
      this.flushScheduled = false;
      // One clock reading for the whole flush so every queue in this drain
      // round enforces the same expiry cutoff.
      const nowMs = this.now();
      for (const subscriber of this.subscribers.values()) {
        // A degraded subscriber's deliveries are paused: its backlog stays
        // queued under the normal backpressure policy, untouched by the
        // drain, so resuming picks up exactly where delivery paused.
        if (subscriber.health?.degraded === true) continue;
        const { live, expired } = subscriber.queue.drainLive(nowMs);
        for (const msg of expired) {
          this.recordExpired(msg.topic);
        }
        const health = subscriber.health;
        if (health == null) {
          for (const msg of live) {
            this.detectGap(subscriber, msg);
            subscriber.handler(msg);
          }
          continue;
        }
        for (let i = 0; i < live.length; i += 1) {
          this.deliverWithHealth(subscriber, live[i]);
          if (!health.degraded) continue;
          // The threshold tripped mid-drain: everything not yet attempted
          // goes back to the queue, in order, with its original TTL
          // deadline restored from the bus registry — FIFO is preserved and
          // resuming redelivers exactly what was paused. (The queue is empty
          // here — it was just drained — so the requeue cannot drop.)
          for (let j = i + 1; j < live.length; j += 1) {
            subscriber.queue.push(live[j], 0, this.messageDeadlines.get(live[j]));
          }
          this.reportDegraded(subscriber);
          break;
        }
      }
    });
  }

  /**
   * Delivers one message to a health-probed subscriber, counting handler
   * failures: a thrown error counts as one failure, and — when
   * `processingTimeoutMs` is configured — a handler that takes longer than
   * the budget to run counts as one failure (a processing timeout). A throw
   * that also overruns the budget still counts once. A successful delivery
   * resets the consecutive-failure counter to 0; reaching the threshold
   * auto-pauses the subscriber via `pauseForHealth`.
   *
   * The failed message itself is consumed (plain subscriptions have no
   * redelivery — use `subscribeReliable` when a failed message must be
   * requeued); the pause protects everything that follows it.
   */
  private deliverWithHealth(subscriber: Subscriber, msg: BusMessage): void {
    const health = subscriber.health;
    if (health == null) return;
    this.detectGap(subscriber, msg);
    const budgetMs = health.processingTimeoutMs;
    const startedAtMs = budgetMs !== undefined ? this.now() : 0;
    let failed = false;
    let reason: 'error' | 'timeout' = 'error';
    try {
      subscriber.handler(msg);
    } catch {
      failed = true;
    }
    if (!failed && budgetMs !== undefined && this.now() - startedAtMs > budgetMs) {
      failed = true;
      reason = 'timeout';
    }
    if (!failed) {
      health.consecutiveFailures = 0;
      return;
    }
    health.consecutiveFailures += 1;
    if (!health.degraded && health.consecutiveFailures >= health.maxConsecutiveFailures) {
      this.pauseForHealth(subscriber, reason);
    }
  }

  /**
   * Auto-pauses a subscriber whose consecutive failures hit the threshold.
   * Starts the auto-resume cooldown timer when one is configured; the
   * `onDegraded` callback fires separately in `reportDegraded`, after the
   * drain loop has requeued the messages it did not attempt.
   */
  private pauseForHealth(subscriber: Subscriber, reason: 'error' | 'timeout'): void {
    const health = subscriber.health;
    if (health == null || health.degraded) return;
    health.degraded = true;
    health.degradedReason = reason;
    if (health.autoResumeAfterMs !== undefined) {
      this.scheduleAutoResume(subscriber, health.autoResumeAfterMs);
    }
  }

  /**
   * Fires the subscriber's `onDegraded` callback once per degradation with
   * the failure count, the reason, and the preserved backlog size.
   */
  private reportDegraded(subscriber: Subscriber): void {
    const health = subscriber.health;
    if (health == null) return;
    subscriber.onDegraded?.({
      subscriberId: subscriber.id,
      pattern: subscriber.pattern,
      consecutiveFailures: health.consecutiveFailures,
      reason: health.degradedReason ?? 'error',
      pendingMessages: subscriber.queue.size,
    });
  }

  /**
   * Arms the auto-resume cooldown: after `afterMs` the subscriber resumes
   * automatically if it is still degraded and still subscribed. The timer
   * never keeps the process alive on its own, and unsubscribe clears it.
   */
  private scheduleAutoResume(subscriber: Subscriber, afterMs: number): void {
    const health = subscriber.health;
    if (health == null) return;
    const timer = setTimeout(() => {
      health.autoResumeTimer = undefined;
      // The subscriber may have unsubscribed or been manually resumed since
      // the timer was armed — only a still-degraded member resumes here.
      if (!this.subscribers.has(subscriber.id) || !health.degraded) return;
      this.resumeDelivery(subscriber);
    }, afterMs);
    // A paused subscriber must not keep the process alive on its own.
    const handle = timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    health.autoResumeTimer = timer;
  }

  /**
   * Counts one message discarded for TTL expiry against its concrete topic
   * and the global total. Each discarded queue entry counts once (see
   * `TopicStats.expiredMessages`).
   */
  private recordExpired(topic: string): void {
    this.totalExpired += 1;
    const stats = this.topicStats.get(topic);
    if (stats != null) stats.expiredMessages += 1;
  }

  /**
   * Compares a delivered message's topic sequence number against the last
   * number delivered to the same subscriber on the same topic, and counts
   * any skipped numbers into the topic's `sequenceGaps` stat (and the global
   * total). The first delivery on a topic only establishes the baseline —
   * a subscriber that joined after earlier publishes must not count
   * pre-subscription messages as lost. A delivery at or below the last
   * delivered number is a redelivery (at-least-once `nack()` / ack-timeout
   * requeue): expected, never a gap, and it does not move the baseline
   * backwards.
   */
  private detectGap(subscriber: Subscriber, msg: BusMessage): void {
    const last = subscriber.lastDeliveredSeq.get(msg.topic);
    if (last === undefined) {
      subscriber.lastDeliveredSeq.set(msg.topic, msg.seq);
      return;
    }
    if (msg.seq > last + 1) {
      const missing = msg.seq - last - 1;
      this.totalSequenceGaps += missing;
      const stats = this.topicStats.get(msg.topic);
      if (stats != null) stats.sequenceGaps += missing;
    }
    if (msg.seq > last) subscriber.lastDeliveredSeq.set(msg.topic, msg.seq);
  }
}
