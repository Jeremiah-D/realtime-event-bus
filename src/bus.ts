import { BoundedQueue, type DropPolicy } from './backpressure.ts';
import { AckTracker, type Delivery } from './ack.ts';
import { TokenBucket } from './throttle.ts';
import {
  DeliveryLatencyTracker,
  type DeliveryLatencyOptions,
  type DeliveryLatencySummaryStats,
} from './latency.ts';
import { DurableTopicLog } from './durablelog.ts';
import { DelayHeap, type DelayedEntry } from './delayed.ts';
import { PublishRateTable, type HotTopic, type TopicRates } from './rates.ts';
import { deflateSync, inflateSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import { Buffer } from 'node:buffer';

export type { Delivery } from './ack.ts';
export type { DeliveryLatencyOptions, DeliveryLatencySummaryStats } from './latency.ts';

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
 * Handler for batched subscriptions (see `SubscribeOptions.batch`):
 * receives an array of messages — up to `maxSize` — instead of one
 * message per call. The array is a fresh array per batch; the messages
 * inside are the same `BusMessage` objects a plain handler would receive,
 * in publish order.
 */
export type BatchMessageHandler = (messages: BusMessage[]) => void;

/**
 * Subscriber content filter (see `SubscribeOptions.filter`): receives the
 * raw published payload and the concrete topic, returns `true` to accept
 * the message into the subscriber's queue, `false` to skip it. Filters
 * always see the application payload — never a compressed wire envelope —
 * and are expected pure: a throwing filter propagates to the publish call,
 * exactly like a throwing schema validator.
 */
export type MessageFilter = (payload: unknown, topic: string) => boolean;

/**
 * Handler for reliable (at-least-once) subscriptions: receives a `Delivery`
 * envelope with `ack()`/`nack()` instead of a bare message.
 */
export type ReliableMessageHandler = (delivery: Delivery<BusMessage>) => void;

/**
 * Handler for batched reliable subscriptions (see `SubscribeOptions.batch`
 * with `subscribeReliable`): receives one at-least-once `Delivery`
 * envelope per message in the batch, in publish order. Ack or nack each
 * delivery individually — acking every delivery confirms the batch, and
 * nacking requeues that message for redelivery (see `Delivery.nack`),
 * preserving the batch's FIFO order when the whole batch is nacked.
 */
export type ReliableBatchMessageHandler = (deliveries: Delivery<BusMessage>[]) => void;

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
   * Opt-in delivery-side rate shaping for a slow downstream consumer. When
   * enabled, the bus delivers at most `messagesPerSec` messages per second
   * to this subscriber (token bucket, first `burst` messages go at once);
   * messages over budget stay queued — in FIFO order, never dropped — and
   * are delivered on later flush rounds as the bucket refills, so delivery
   * is smoothed to the downstream's pace instead of arriving in bursts.
   *
   * Unlike adaptive publish-side `throttle` (which sheds), shaping never
   * drops: the backlog accumulates under the subscriber's normal
   * backpressure policy, so size the queue for the expected backlog — a
   * full queue still applies its drop policy. Shaping does not extend TTL:
   * a message whose deadline passes while it waits is dropped as expired.
   * Disabled by default.
   *
   * Pass `true` for the defaults (100 messages/sec, one second of burst),
   * or a `DeliveryShapingOptions` object to tune the rate. Invalid values
   * throw `RangeError` from `subscribe`.
   */
  deliveryShaping?: boolean | DeliveryShapingOptions;
  /**
   * Opt-in per-subscriber delivery-latency sampling. When enabled, the bus
   * stamps every message enqueued for this subscriber with the bus clock
   * and records one sample per handler hand-off: the enqueue→delivery
   * queue dwell in milliseconds (handler processing time excluded).
   * `getStats()` exposes the per-subscriber p50/p95/p99 distribution
   * (`deliveryLatency`) and the slowest tracked subscribers (`slowestSubscribers`),
   * and `src/metrics.ts` renders them as Prometheus gauges. Useful for
   * spotting starved consumers before their backpressure queue starts
   * shedding. Disabled by default.
   *
   * Pass `true` for the defaults (1024-sample rolling window per
   * subscriber), or a `DeliveryLatencyOptions` object to tune the window.
   * Invalid values throw `RangeError` from `subscribe`.
   */
  deliveryLatency?: boolean | DeliveryLatencyOptions;
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
  /**
   * Opt-in subscriber-side content filter. When set, every message fanned
   * out to this subscriber first passes the predicate at the publish side:
   * a message the filter rejects never enters the subscriber's queue — it
   * consumes no backpressure budget, burns no adaptive-throttle token, and
   * does not surface as a sequence gap (the per-topic baseline advances
   * over it, exactly as if it had been delivered and deliberately
   * skipped). The filter sees the raw application payload and the concrete
   * topic, so one subscription can narrow a broad pattern (e.g. subscribe
   * `market.**` but only accept `payload.symbol === 'BTC'`).
   *
   * The filter also applies to durable-log replay (`resumeFromSeq`): a
   * replayed message the filter rejects is skipped the same way — never
   * queued, never a gap. For consumer-group members the filter is
   * evaluated on the assigned member only: if the assignee rejects the
   * message, the group's one copy is dropped.
   *
   * Must be a function when provided; anything else throws `TypeError`
   * from `subscribe`. Disabled by default.
   */
  filter?: MessageFilter;
  /**
   * Opt-in subscriber-side batch delivery. When enabled, the drain
   * collects up to `maxSize` queued messages and invokes the handler once
   * with the array, amortizing per-message callback overhead — the same
   * messages a plain handler would receive, in publish order. When fewer
   * than `maxSize` messages are queued, the bus holds the partial batch
   * for up to `maxWaitMs` to let it fill before delivering it; a full
   * batch is always delivered immediately, never held for the timer.
   *
   * Composes with the other delivery features:
   * - reliable (`subscribeReliable`): the handler receives one `Delivery`
   *   envelope per message in the batch — ack each one to confirm the
   *   batch; nacking requeues that message at the tail, so nacking the
   *   whole batch requeues it in FIFO order. Ack timeouts and the DLQ
   *   keep their per-message semantics.
   * - `deliveryShaping`: the batch is bounded by the shaping budget — the
   *   whole batch consumes budget, one token per message.
   * - `throttle`: publish-side shedding is unchanged; only queued
   *   messages are batched.
   * - `healthProbe`: the batch is one handler invocation — a throw (or a
   *   processing timeout) counts as one failure.
   * - `deliveryLatency`: every message in the batch is sampled
   *   individually (enqueue→hand-off queue dwell).
   * - TTL: a message that expires while its batch is filling is dropped
   *   as expired at hand-off, not resurrected.
   *
   * Pass `true` for the defaults (`maxSize` 100, `maxWaitMs` 10), or a
   * `BatchDeliveryOptions` object to tune them. Invalid values throw
   * `RangeError` from `subscribe`. Disabled by default.
   */
  batch?: boolean | BatchDeliveryOptions;
}

/**
 * Tuning for subscriber-side batch delivery (`SubscribeOptions.batch`).
 * Bounds are validated at `subscribe` time; violations throw `RangeError`.
 */
export interface BatchDeliveryOptions {
  /**
   * Maximum messages per handler invocation. The drain collects at most
   * this many queued messages into one batch; a full batch is delivered
   * immediately. Must be a positive integer. Default 100.
   */
  maxSize?: number;
  /**
   * How long a partial batch may wait to fill, in milliseconds. When the
   * drain finds fewer than `maxSize` messages queued, it holds them for up
   * to this long before delivering the partial batch — the classic
   * micro-batch linger. `0` disables lingering: each flush delivers
   * whatever is queued, up to `maxSize`. Must be a finite number `>= 0`.
   * Default 10.
   */
  maxWaitMs?: number;
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

/**
 * Tuning for delivery-side rate shaping (`SubscribeOptions.deliveryShaping`).
 * Bounds are validated at `subscribe` time; violations throw `RangeError`.
 */
export interface DeliveryShapingOptions {
  /**
   * Maximum deliveries per second to the subscriber. Must be a positive
   * finite number. Default 100.
   */
  messagesPerSec?: number;
  /**
   * How many messages may be delivered in one flush round once budget has
   * accumulated — the token bucket capacity. Must be a positive finite
   * number. Defaults to one second's worth of budget
   * (`max(1, ceil(messagesPerSec))`), the same convention as
   * `setTopicRateLimit`.
   */
  burst?: number;
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
  /**
   * Opt-in per-subscriber dead-letter queue for poison messages. Without
   * it, a reliable subscription retries forever (at-least-once): every
   * `nack()` and every ack timeout requeues the message with no upper
   * bound. With it, a message that has been requeued more than
   * `maxRedeliveries` times moves to the subscriber's DLQ instead of being
   * requeued again — inspect it with `getDeadLetterMessages(subId)` and
   * hand it back to the queue with `replayDeadLetter(subId, seq)`.
   *
   * Pass `true` for the defaults (5 redeliveries, 1000 DLQ entries), or a
   * `DeadLetterOptions` object to tune them. Disabled by default.
   */
  deadLetter?: boolean | DeadLetterOptions;
}

/**
 * Tuning for a reliable subscriber's dead-letter queue (see
 * `ReliableSubscribeOptions.deadLetter`).
 */
export interface DeadLetterOptions {
  /**
   * How many times a message may be requeued (via `nack()` or ack timeout)
   * before it moves to the DLQ. The next requeue attempt beyond this
   * budget dead-letters the message instead. Defaults to 5. Must be a
   * non-negative integer — `0` means the first redelivery attempt already
   * dead-letters.
   */
  maxRedeliveries?: number;
  /**
   * Maximum DLQ entries kept per subscriber. When the DLQ is full, the
   * oldest entry is evicted to make room (reported via
   * `DeadLetterEvent.evictedOldest`). Defaults to 1000. Must be a positive
   * integer.
   */
  maxEntries?: number;
  /**
   * Fired when a message enters the subscriber's DLQ — once per
   * dead-lettering, synchronously with the move.
   */
  onDeadLetter?: (event: DeadLetterEvent) => void;
}

/**
 * One message sitting in a subscriber's dead-letter queue: the poison
 * message, preserved in DLQ arrival order with its original TTL deadline.
 * Returned by `getDeadLetterMessages(subId)`; `seq` identifies the entry
 * for `replayDeadLetter(subId, seq)`. The returned objects are snapshots —
 * mutating them does not affect the bus.
 */
export interface DeadLetterEntry {
  /**
   * DLQ-local sequence, starting at 1 per subscriber and increasing by 1
   * for every dead-lettered message. The DLQ preserves arrival order, so a
   * higher `seq` always means "dead-lettered later".
   */
  seq: number;
  /** The concrete topic the message was published to. */
  topic: string;
  /**
   * The published payload. This is the original payload object, shared by
   * reference — the bus treats payloads as opaque everywhere (see
   * `BusMessage.payload`), so it is never cloned. Do not mutate it if you
   * plan to replay the entry.
   */
  payload: unknown;
  /**
   * How many times the message was requeued before it was dead-lettered.
   * Poison-message signal: a high count means the consumer failed the
   * message repeatedly.
   */
  redeliveries: number;
  /** Bus-clock timestamp when the message entered the DLQ. */
  deadLetteredAt: number;
  /**
   * The message's original TTL deadline, preserved so a replayed message
   * expires on the same schedule instead of being resurrected. Absent when
   * the message had no TTL.
   */
  expiresAt?: number;
}

/** Snapshot delivered to `DeadLetterOptions.onDeadLetter`. */
export interface DeadLetterEvent {
  /** The reliable subscription whose DLQ took the message. */
  subscriberId: string;
  /** The subscription's topic pattern. */
  pattern: string;
  /** The entry that was added. */
  entry: DeadLetterEntry;
  /**
   * True when the DLQ was full and the oldest entry was evicted to make
   * room for this one.
   */
  evictedOldest: boolean;
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
  /**
   * Present on `leave` events when the leaving member configured
   * `GroupSubscribeOptions.handoffLingerMs` and had assigned-but-
   * uncommitted work: the bus-clock time (ms) until which that backlog is
   * treated as still in flight — a member that (re)joins and replays from
   * the durable log skips it instead of delivering it a second time.
   */
  lingerUntil?: number;
  /**
   * Present alongside `lingerUntil`: the per-topic backlog ranges held in
   * the linger window — `(fromSeq, toSeq]`, committed-exclusive to
   * assigned-inclusive — that replay skips until `lingerUntil`.
   */
  lingering?: Array<{ topic: string; fromSeq: number; toSeq: number }>;
}

/**
 * One active handoff-linger window for a group (see
 * `GroupSubscribeOptions.handoffLingerMs`): seqs the group assigned to a
 * departed member but nobody committed yet. While `until` is in the
 * future, durable-log replay skips these seqs for members of the group —
 * the leaver is presumed still processing them — so they are never
 * delivered twice. After `until`, the backlog becomes replayable again
 * (the leaver is presumed dead; at-least-once resumes).
 */
interface LingerWindow {
  /** Concrete topic the backlog range covers. */
  topic: string;
  /** Committed seq at leave time (exclusive bound). */
  fromSeq: number;
  /** Assigned seq at leave time (inclusive bound). */
  toSeq: number;
  /** Bus-clock ms; the window is live while this is in the future. */
  until: number;
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
  /**
   * Graceful-handoff window in milliseconds, applied when this member
   * leaves the group. While the window is open, messages the group
   * assigned to the leaving member but that were never committed are
   * treated as still in flight: a member that (re)joins and replays from
   * the durable log (`resumeFromSeq`) skips them instead of delivering
   * them a second time. After the window expires the backlog becomes
   * replayable again — the leaver is presumed dead and at-least-once
   * delivery resumes.
   *
   * `0` (default) disables the window. Only meaningful with
   * `EventBusOptions.durableLogDir`: without a durable log there is
   * nothing to replay, so the linger is inert. Must be a finite number
   * `>= 0`; anything else throws `RangeError`.
   *
   * Operational recipe: `commitOffset` before leaving — committed work
   * needs no handoff window. The linger only covers what the leaver did
   * not commit (the crash case), exactly like a Kafka rebalance revoke.
   */
  handoffLingerMs?: number;
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
  /**
   * Opt-in Kafka-style keyed log compaction for the durable log (see
   * `DurableTopicLog`). A published message may then carry a `key` (see
   * `PublishOptions.key`): only the latest record per (topic, key) is
   * retained and replayed. Only meaningful with `durableLogDir`. Default
   * false.
   */
  durableLogKeyCompaction?: boolean;
  /**
   * Enables publish-side idempotent dedup for `publishIdempotent`: within
   * this many milliseconds of an admitted publish, a retry carrying the
   * same `messageId` on the same topic is suppressed as a duplicate — it
   * consumes no sequence number, is never written to the durable log, and
   * burns no rate-limit budget. The window is measured on the bus clock
   * (`EventBusOptions.now`), so it is deterministic in tests. Must be a
   * positive finite number of milliseconds. Default 60000.
   *
   * The classic use is payment/fintech retry safety: a client that retries
   * an order or settlement publish after a timeout passes the same
   * `messageId`, and the bus guarantees the downstream sees it exactly
   * once within the window — no double charge, no double settlement.
   */
  idempotencyWindowMs?: number;
  /**
   * Maximum entries in the idempotency dedup table (`publishIdempotent`).
   * When the table is full, the oldest (topic, messageId) entry is evicted
   * to make room. Must be a positive integer. Default 10000.
   */
  idempotencyMaxEntries?: number;
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
  /**
   * Idempotent publishes to this topic suppressed as duplicates (see
   * `publishIdempotent`): the retry arrived with a `(topic, messageId)`
   * pair already published within the idempotency window, so it was
   * dropped before admission — no sequence number consumed (subscribers
   * see no gap), nothing written to the durable log, no rate-limit budget
   * burned. The key retry-safety metric: it tells operators how many
   * retries the bus absorbed.
   */
  duplicateMessages: number;
  /**
   * Messages fanned out on this topic that a subscriber's content filter
   * rejected (see `SubscribeOptions.filter`): the message never entered
   * that subscriber's queue — no backpressure budget consumed — and the
   * subscriber's per-topic baseline advanced over it, so it is invisible
   * to sequence-gap detection.
   */
  filteredMessages: number;
  /**
   * Messages on this topic delivered with a compressed payload (see
   * `setTopicCompression`): the payload's serialized size exceeded the
   * topic's `thresholdBytes` and deflate produced a smaller encoding.
   * Compression never changes delivery semantics — the subscriber's
   * handler transparently receives the original payload.
   */
  compressedMessages: number;
  /**
   * Sum of serialized payload bytes before compression, over this topic's
   * compressed messages.
   */
  compressedBytesBefore: number;
  /**
   * Sum of payload bytes after compression (the deflated bytes, before
   * base64), over this topic's compressed messages.
   */
  compressedBytesAfter: number;
  /**
   * `compressedBytesAfter / compressedBytesBefore` — below 1 means the
   * wire shrank. 0 when nothing on this topic has been compressed yet.
   */
  compressionRatio: number;
  /**
   * Mean deflate time per compressed message in milliseconds, measured
   * with the bus clock. 0 when nothing on this topic has been compressed
   * yet.
   */
  meanCompressionMs: number;
  /**
   * Publish rate on this topic in messages per second over the trailing
   * 1s / 1m / 5m windows, load-average style (see `src/rates.ts`). Only
   * admitted publishes count — schema rejections and rate-limit sheds
   * never sample. Read at `getStats()` time from the bus clock, so the
   * values decay as traffic ages out of the windows.
   */
  rates: TopicRates;
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
   * Total messages handed to subscriber handlers. Counts each handler
   * invocation, so at-least-once redeliveries count again; a handler
   * that throws still received the message, and still counts.
   */
  deliveredMessages: number;
  /**
   * Total messages shed by subscriber backpressure queues. Matches the
   * sum of the per-subscriber `droppedCount` values for live subscribers.
   */
  droppedMessages: number;
  /**
   * Total messages shed at the publish side by adaptive publish-side
   * throttling (see `SubscribeOptions.throttle`) — the sum of every
   * subscriber's `throttledCount`. Counted separately from queue drops.
   */
  throttledMessages: number;
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
   * Total idempotent publishes suppressed as duplicates — the sum of
   * every topic's `duplicateMessages`. See `TopicStats.duplicateMessages`
   * for the exact counting rules.
   */
  duplicateMessages: number;
  /**
   * Total messages skipped by subscriber content filters — the sum of
   * every topic's `filteredMessages`. See `TopicStats.filteredMessages`
   * for the exact counting rules.
   */
  filteredMessages: number;
  /**
   * Total messages moved into subscriber dead-letter queues — every
   * reliable-subscription message that exhausted its redelivery budget
   * (see `ReliableSubscribeOptions.deadLetter`).
   */
  deadLetteredMessages: number;
  /**
   * Total messages delivered with a compressed payload — the sum of every
   * topic's `compressedMessages`. See `TopicStats.compressedMessages` for
   * the exact counting rules.
   */
  compressedMessages: number;
  /**
   * Sum of serialized payload bytes before compression, across all
   * compressed messages.
   */
  compressedBytesBefore: number;
  /**
   * Sum of payload bytes after compression (deflated bytes, before
   * base64), across all compressed messages.
   */
  compressedBytesAfter: number;
  /**
   * Global `compressedBytesAfter / compressedBytesBefore` — below 1 means
   * the wire shrank. 0 when nothing has been compressed yet.
   */
  compressionRatio: number;
  /**
   * Mean deflate time per compressed message in milliseconds, measured
   * with the bus clock. 0 when nothing has been compressed yet.
   */
  meanCompressionMs: number;
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
   * Subscriptions currently having messages held back by delivery-side
   * rate shaping (see `SubscribeOptions.deliveryShaping`): their queues
   * still hold messages the token budget did not allow this round.
   */
  shapedSubscribers: number;
  /**
   * Delayed messages currently scheduled (see `publishDelayed`): accepted,
   * not yet due, fanned out, cancelled, or expired. Each one holds a slot
   * in the timer heap until its due time.
   */
  pendingDelayed: number;
  /**
   * Per-subscriber delivery-latency summaries (see
   * `SubscribeOptions.deliveryLatency`): one entry per subscription with
   * sampling enabled, in subscription order. Each entry carries the
   * enqueue→delivery queue-dwell distribution (p50/p95/p99, min/max/mean
   * over the subscriber's bounded rolling window). Empty when no
   * subscriber opts in.
   */
  deliveryLatency: Array<
    { subscriberId: string; pattern: string } & DeliveryLatencySummaryStats
  >;
  /**
   * The slowest latency-tracked subscribers by p99 queue dwell (top 5,
   * descending) — the first place to look when end-to-end lag grows.
   * Only subscribers with at least one sample appear.
   */
  slowestSubscribers: Array<{
    subscriberId: string;
    pattern: string;
    p99Ms: number;
    samples: number;
  }>;
  /**
   * The hottest topics by 1m publish rate (see `src/rates.ts`), hottest
   * first — at most `HOT_TOPICS_LIMIT` (10) entries; topics with no
   * publishes in the trailing minute never appear. The first place to look
   * when deciding where to tighten rate limits or add capacity.
   */
  hotTopics: HotTopic[];
  /**
   * Stats for every concrete topic that has seen at least one publish or
   * schema rejection, in order of first publish.
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
    /** Whether keyed log compaction is enabled (`durableLogKeyCompaction`). */
    keyCompaction: boolean;
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
   * Present for reliable subscriptions with `deadLetter` enabled: the
   * subscriber's poison-message queue. Messages that exhausted their
   * redelivery budget land here in arrival order instead of being
   * requeued forever. Bounded by `maxEntries` — the oldest entry is
   * evicted when full.
   */
  deadLetter?: SubscriberDeadLetter;
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
   * Delivery-side rate shaping state (see `SubscribeOptions.deliveryShaping`).
   * Absent when shaping is disabled for this subscriber.
   */
  deliveryShaping?: DeliveryShapingState;
  /**
   * Delivery-latency sampling state (see `SubscribeOptions.deliveryLatency`).
   * Absent when sampling is disabled for this subscriber.
   */
  latency?: SubscriberLatencyState;
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
  /**
   * Graceful-handoff linger in ms for consumer-group members (see
   * `GroupSubscribeOptions.handoffLingerMs`). Absent (or 0) for plain
   * subscribers and members without a linger.
   */
  handoffLingerMs?: number;
  /**
   * Subscriber-side content filter (see `SubscribeOptions.filter`).
   * Absent when the subscriber has no filter.
   */
  filter?: MessageFilter;
  /**
   * Batch-delivery state (see `SubscribeOptions.batch`). Absent when
   * batching is disabled for this subscriber.
   */
  batch?: BatchDeliveryState;
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
 * Per-subscriber dead-letter queue state (see
 * `ReliableSubscribeOptions.deadLetter`). `entries` holds the poison
 * messages in dead-lettering order; each record keeps the original
 * `BusMessage` so replay requeues the identical object (same per-topic
 * `seq`, same TTL deadline) instead of a reconstructed copy.
 */
interface SubscriberDeadLetter {
  entries: Array<DeadLetterEntry & { msg: BusMessage }>;
  maxRedeliveries: number;
  maxEntries: number;
  nextSeq: number;
  onDeadLetter?: (event: DeadLetterEvent) => void;
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
 * Fresh per-topic counters, all zero. Shared by the bus's lazy stats
 * creation and the schema-rejection counter so the shape cannot drift
 * between call sites.
 */
function zeroedTopicStats(): {
  subscriberCount: number;
  publishedMessages: number;
  expiredMessages: number;
  lastSeq: number;
  sequenceGaps: number;
  rateLimitedMessages: number;
  rejectedMessages: number;
  duplicateMessages: number;
  filteredMessages: number;
  compressedMessages: number;
  compressedBytesBefore: number;
  compressedBytesAfter: number;
  compressionTimeMs: number;
} {
  return {
    subscriberCount: 0,
    publishedMessages: 0,
    expiredMessages: 0,
    lastSeq: 0,
    sequenceGaps: 0,
    rateLimitedMessages: 0,
    rejectedMessages: 0,
    duplicateMessages: 0,
    filteredMessages: 0,
    compressedMessages: 0,
    compressedBytesBefore: 0,
    compressedBytesAfter: 0,
    compressionTimeMs: 0,
  };
}

/**
 * Validates a publish message key (see `PublishOptions.key`): absent is
 * fine, otherwise it must be a non-empty string. Throws `RangeError`
 * before anything is mutated, matching the other publish-option
 * validations.
 */
function validateMessageKey(key: string | undefined, caller: string): void {
  if (key === undefined) return;
  if (typeof key !== 'string' || key.length === 0) {
    throw new RangeError(`${caller}: key must be a non-empty string`);
  }
}

/**
 * Normalizes the `deadLetter` subscribe option into the resolved config,
 * or `null` when the DLQ is disabled. Throws `RangeError` on invalid
 * budgets before anything is mutated, matching the other subscribe-option
 * validations.
 */
function normalizeDeadLetterOptions(
  opt: boolean | DeadLetterOptions | undefined,
  caller: string,
): Pick<SubscriberDeadLetter, 'maxRedeliveries' | 'maxEntries' | 'onDeadLetter'> | null {
  if (opt === undefined || opt === false) return null;
  if (opt !== true && (typeof opt !== 'object' || opt === null)) {
    throw new TypeError(`${caller}: deadLetter must be true or a DeadLetterOptions object`);
  }
  const o: DeadLetterOptions = opt === true ? {} : opt;
  const maxRedeliveries = o.maxRedeliveries ?? 5;
  const maxEntries = o.maxEntries ?? 1000;
  if (!Number.isInteger(maxRedeliveries) || maxRedeliveries < 0) {
    throw new RangeError(`${caller}: deadLetter.maxRedeliveries must be a non-negative integer`);
  }
  if (!Number.isInteger(maxEntries) || maxEntries <= 0) {
    throw new RangeError(`${caller}: deadLetter.maxEntries must be a positive integer`);
  }
  if (o.onDeadLetter !== undefined && typeof o.onDeadLetter !== 'function') {
    throw new TypeError(`${caller}: deadLetter.onDeadLetter must be a function`);
  }
  return { maxRedeliveries, maxEntries, onDeadLetter: o.onDeadLetter };
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
 * Per-subscriber delivery-side rate shaping state (see
 * `SubscribeOptions.deliveryShaping`). The token bucket paces how many
 * queued messages one flush round may deliver; `shaping` is true while the
 * subscriber holds messages back for lack of budget, and `timer` is the
 * pending re-flush scheduled for when the bucket refills.
 */
interface DeliveryShapingState {
  bucket: TokenBucket;
  /** True while queued messages are held back for lack of budget. */
  shaping: boolean;
  messagesPerSec: number;
  burst: number;
  /** Pending re-flush timer, armed while `shaping` is true. */
  timer?: ReturnType<typeof setTimeout>;
}

/**
 * Validates `SubscribeOptions.deliveryShaping` and builds the initial
 * per-subscriber shaping state. Returns `undefined` when shaping is
 * disabled. Throws `RangeError` for invalid rates.
 */
function resolveDeliveryShapingOptions(
  opt: boolean | DeliveryShapingOptions | undefined,
  now: () => number,
): DeliveryShapingState | undefined {
  if (opt == null || opt === false) return undefined;
  const o: DeliveryShapingOptions = opt === true ? {} : opt;
  const messagesPerSec = o.messagesPerSec ?? 100;
  if (!Number.isFinite(messagesPerSec) || messagesPerSec <= 0) {
    throw new RangeError('deliveryShaping.messagesPerSec must be a positive finite number of messages/sec');
  }
  const burst = o.burst ?? Math.max(1, Math.ceil(messagesPerSec));
  if (!Number.isFinite(burst) || burst <= 0) {
    throw new RangeError('deliveryShaping.burst must be a positive finite number of messages');
  }
  return {
    bucket: new TokenBucket(burst, messagesPerSec, now),
    shaping: false,
    messagesPerSec,
    burst,
    timer: undefined,
  };
}

/**
 * Per-subscriber batch-delivery state (see `SubscribeOptions.batch`).
 * `pending` holds messages already dequeued for the current batch while it
 * waits to fill (up to `maxWaitMs`); they are no longer in the queue, so
 * unsubscribe drops them like any other undelivered backlog.
 */
interface BatchDeliveryState {
  maxSize: number;
  maxWaitMs: number;
  /** Messages collected for the current batch, already dequeued, in order. */
  pending: BusMessage[];
  /** Linger timer armed while a partial batch waits to fill. */
  timer?: ReturnType<typeof setTimeout>;
}

/**
 * Validates `SubscribeOptions.batch` and builds the initial batch state.
 * Returns `undefined` when batching is disabled. Throws `RangeError` for
 * invalid sizes or waits.
 */
function resolveBatchDeliveryOptions(
  opt: boolean | BatchDeliveryOptions | undefined,
): BatchDeliveryState | undefined {
  if (opt == null || opt === false) return undefined;
  if (opt !== true && (typeof opt !== 'object' || opt === null)) {
    throw new TypeError('batch must be true or a BatchDeliveryOptions object');
  }
  const o: BatchDeliveryOptions = opt === true ? {} : opt;
  const maxSize = o.maxSize ?? 100;
  if (!Number.isInteger(maxSize) || maxSize <= 0) {
    throw new RangeError('batch.maxSize must be a positive integer');
  }
  const maxWaitMs = o.maxWaitMs ?? 10;
  if (!Number.isFinite(maxWaitMs) || maxWaitMs < 0) {
    throw new RangeError('batch.maxWaitMs must be a finite number of milliseconds >= 0');
  }
  return { maxSize, maxWaitMs, pending: [], timer: undefined };
}

/**
 * Per-subscriber delivery-latency sampling state (see
 * `SubscribeOptions.deliveryLatency`). `enqueuedAt` maps a queued message
 * to the bus-clock reading of its most recent enqueue; entries die with
 * their messages (WeakMap), so no cleanup is needed on unsubscribe.
 */
interface SubscriberLatencyState {
  tracker: DeliveryLatencyTracker;
  enqueuedAt: WeakMap<BusMessage, number>;
}

/**
 * Validates `SubscribeOptions.deliveryLatency` and builds the initial
 * per-subscriber sampling state. Returns `undefined` when sampling is
 * disabled. Throws `RangeError` for an invalid window size.
 */
function resolveDeliveryLatencyOptions(
  opt: boolean | DeliveryLatencyOptions | undefined,
): SubscriberLatencyState | undefined {
  if (opt == null || opt === false) return undefined;
  const o: DeliveryLatencyOptions = opt === true ? {} : opt;
  const windowSize = o.windowSize ?? 1024;
  if (!Number.isInteger(windowSize) || windowSize <= 0) {
    throw new RangeError('deliveryLatency.windowSize must be a positive integer');
  }
  return { tracker: new DeliveryLatencyTracker(windowSize), enqueuedAt: new WeakMap() };
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

/**
 * Why one entry of an atomic batch was rejected: its payload failed the
 * topic's schema validator (`'schema'`), or the topic's rate-limit bucket
 * had no token left for it (`'rate-limit'`). TTL expiry is drain-time and
 * never rejects a publish, so it cannot appear here.
 */
export type AtomicRejectReason = 'schema' | 'rate-limit';

/**
 * Which entry of a `publishAtomic` batch failed admission, and why.
 */
export interface AtomicPublishRejection {
  /** Zero-based position of the rejected entry in the batch. */
  index: number;
  /** Concrete topic of the rejected entry. */
  topic: string;
  /** The admission gate that rejected it. */
  reason: AtomicRejectReason;
}

/**
 * Outcome of `EventBus.publishAtomic`. On success `published` is the number
 * of committed messages — always the whole batch — and `rejected` is
 * absent. On failure `published` is 0, nothing was committed, and
 * `rejected` identifies the entry that failed admission.
 */
export interface AtomicPublishResult {
  published: number;
  rejected?: AtomicPublishRejection;
}

/**
 * Options for `EventBus.publish`: per-message publish-time controls.
 */
export interface PublishOptions {
  /**
   * Compaction key for the durable log's keyed compaction (requires
   * `EventBusOptions.durableLogKeyCompaction`, Kafka-style): only the
   * latest record per (topic, key) is retained and replayed — publishing a
   * new message with an existing key supersedes the old value. Use it for
   * state snapshots (latest price per symbol, latest config per service)
   * rather than event streams. Keyless messages are ordinary log entries.
   *
   * Must be a non-empty string when provided; anything else throws
   * `RangeError` from `publish`. The key is recorded on the durable-log
   * record regardless, but without `durableLogKeyCompaction` it has no
   * behavioral effect.
   */
  key?: string;
}

/**
 * One message in a `publishBatch` / `publishAtomic` batch: topic and
 * payload, with the bus assigning `seq` at fan-out. `key` opts the message
 * into durable-log keyed compaction (see `PublishOptions.key`).
 */
export type BatchMessage = Omit<BusMessage, 'seq'> & { key?: string };

/**
 * When a `publishDelayed` message becomes due. Exactly one of the two
 * fields must be set; passing both or neither throws `RangeError`.
 */
export interface PublishDelayedOptions {
  /**
   * Deliver this many milliseconds after now, measured on the bus clock
   * (`EventBusOptions.now`). Must be a non-negative finite number. `0`
   * means the message is due immediately and fans out on the next flush,
   * exactly like `publish`.
   */
  delayMs?: number;
  /**
   * Deliver at this absolute bus-clock timestamp in milliseconds. A
   * timestamp at or before now means the message is already due and fans
   * out on the next flush. Must be finite.
   */
  deliverAt?: number;
  /**
   * Compaction key for the durable log's keyed compaction (see
   * `PublishOptions.key`): persisted on the schedule record so a restart
   * rebuilds the timer with the key intact, and recorded on the delivery
   * record when the message fans out. Must be a non-empty string when
   * provided; anything else throws `RangeError`.
   */
  key?: string;
}

/**
 * Carried from a delayed schedule into its fan-out: at its due time the
 * message enters the normal publish pipeline — sequence stamping,
 * rate-limit budget, compression, durable log, fan-out — exactly as if it
 * had been published at that moment. Schema validation is NOT re-run here:
 * it already ran once at schedule time (fail-fast), and validators are
 * expected pure (the same contract `publishAtomic` relies on).
 */
interface DelayedFanOut {
  /** The schedule's id; copied onto the delivery's durable-log record. */
  delayId: string;
  /** The due time the message was scheduled for (bus clock). */
  deliverAt: number;
  /**
   * TTL deadline stamped at schedule time, when a TTL rule matched — or
   * `undefined` when no rule matched then. A rule added between schedule
   * and fan-out does not retroactively expire the message.
   */
  expiresAt?: number;
  /**
   * Compaction key carried from the delayed schedule into its fan-out
   * (see `PublishDelayedOptions.key`): stamped onto the delivery's
   * durable-log record.
   */
  key?: string;
}

/**
 * Tuning for opt-in per-topic payload compression (see
 * `EventBus.setTopicCompression`). Compression uses `node:zlib` deflate —
 * zero new dependencies — and only kicks in for payloads whose serialized
 * size exceeds `thresholdBytes`, so small messages never pay deflate CPU.
 */
export interface TopicCompressionOptions {
  /**
   * Payloads whose serialized (UTF-8 JSON) size is strictly greater than
   * this many bytes are deflate-compressed; anything at or below the
   * threshold passes through untouched. Must be a positive finite number
   * of bytes.
   */
  thresholdBytes: number;
  /**
   * zlib deflate compression level, 0–9. Default 6. Higher levels trade
   * CPU for smaller output; level 0 stores without compressing, so its
   * output is never smaller than the input and the bus always passes
   * through (useful for testing the never-adopt-a-larger-encoding guard).
   */
  level?: number;
  /**
   * Optional preset deflate dictionary (zlib `dictionary` option), zero
   * new dependencies. Pre-seeding the compressor with the topic's
   * recurring byte patterns — field names, enum values, venue prefixes —
   * dramatically improves the compression ratio of SMALL JSON messages
   * that carry too little redundancy for deflate to find on its own.
   * Typical use: build it once from a few hundred representative
   * serialized payloads concatenated together (keep it under the 32 KiB
   * zlib cap), then share the same bytes across topics with similar
   * shapes. The bus copies the bytes at registration time, so mutating
   * the caller's buffer afterwards has no effect.
   *
   * The SAME dictionary must be used to inflate: it travels with each
   * live message and is resolved from the bus's dictionary registry for
   * durable-log replay (registered automatically by
   * `setTopicCompression`). A replayed record whose dictionary is no
   * longer registered fails loudly at replay instead of delivering
   * garbage — keep the rule (or re-register the identical dictionary)
   * for as long as you need to replay logs written with it.
   */
  dictionary?: Uint8Array | ArrayBuffer | DataView;
}

/**
 * A compression rule with every option resolved to its concrete runtime
 * form: `level` defaulted to 6, and `dictionary` snapshotted into an owned
 * `Buffer` plus its SHA-256 id (used to resolve the dictionary for
 * durable-log replay, where no live rule is at hand).
 */
interface ResolvedCompressionRule {
  thresholdBytes: number;
  level: number;
  dictionary?: Buffer;
  dictionaryId?: string;
}

/**
 * Maximum zlib preset-dictionary size: 32 KiB, the largest window zlib
 * can reference (2^15). Anything larger cannot help and `deflateSync`
 * would reject it.
 */
const MAX_COMPRESSION_DICTIONARY_BYTES = 32 * 1024;

/**
 * Maximum dictionaries the bus retains for durable-log replay. A
 * dictionary is registered once per distinct byte content; 64 is far above
 * what a process configures (one or two dictionaries per payload family)
 * while keeping the retention bounded when rules are churned.
 */
const MAX_REGISTERED_COMPRESSION_DICTIONARIES = 64;

/**
 * Validates and snapshots a `TopicCompressionOptions.dictionary` value.
 * Returns the owned `Buffer` copy, or `undefined` when no dictionary was
 * given. Throws `RangeError` for anything that is not a non-empty byte
 * view of at most 32 KiB. The bytes are COPIED, not referenced: mutating
 * the caller's buffer after registration cannot silently change what the
 * bus compresses with — a real hazard when the same `Uint8Array` doubles
 * as a scratch buffer elsewhere.
 */
function coerceCompressionDictionary(
  dictionary: Uint8Array | ArrayBuffer | DataView | undefined,
): Buffer | undefined {
  if (dictionary === undefined) return undefined;
  let view: { readonly byteLength: number } | null = null;
  if (dictionary instanceof ArrayBuffer) {
    view = dictionary;
  } else if (ArrayBuffer.isView(dictionary)) {
    view = dictionary as DataView;
  }
  if (view === null || view.byteLength === 0) {
    throw new RangeError('dictionary must be a non-empty byte buffer (Uint8Array, ArrayBuffer or DataView)');
  }
  if (view.byteLength > MAX_COMPRESSION_DICTIONARY_BYTES) {
    throw new RangeError(
      `dictionary must be at most ${MAX_COMPRESSION_DICTIONARY_BYTES} bytes (zlib preset-dictionary cap)`,
    );
  }
  if (dictionary instanceof ArrayBuffer) return Buffer.from(dictionary.slice(0));
  const typed = dictionary as unknown as { buffer: ArrayBuffer; byteOffset: number; byteLength: number };
  return Buffer.from(typed.buffer.slice(typed.byteOffset, typed.byteOffset + typed.byteLength));
}

/**
 * Wire form of a compressed payload: what subscriber queues and the
 * durable log hold between publish and delivery. The `__busCompressed`
 * marker is reserved by the bus — a user payload that happens to carry the
 * exact same shape is NOT treated as compressed (see `EventBus`'s
 * `compressedPayloads` set), but do not hand-craft this envelope: its
 * layout is internal and may change.
 */
interface CompressedPayload {
  /** Reserved marker: this envelope carries zlib-deflated payload bytes. */
  __busCompressed: 'deflate';
  /** Base64 of the raw deflate bytes of the payload's UTF-8 JSON encoding. */
  data: string;
}

/**
 * Inflates a compression envelope back to the application payload. Used
 * when a subscriber content filter must judge a durable-log record whose
 * on-disk bytes are the deflated envelope: the filter always sees the raw
 * payload, exactly like a live filter evaluation. Throws on a malformed
 * envelope — a tampered log must not silently deliver corrupt data.
 * `dictionary` must be the preset dictionary the payload was compressed
 * with (or `undefined` for dictionary-less compression); a wrong
 * dictionary makes zlib throw, never silently mis-decode.
 */
function inflateEnvelopePayload(envelope: CompressedPayload, dictionary?: Buffer): unknown {
  const raw = Buffer.from(envelope.data, 'base64');
  const inflated =
    dictionary === undefined ? inflateSync(raw) : inflateSync(raw, { dictionary });
  return JSON.parse(inflated.toString('utf8'));
}

/**
 * Shape-check for the compression envelope. Strict on purpose (exact key
 * count, exact marker, string data) so a user payload that merely
 * resembles the envelope is never mistaken for one — the bus additionally
 * gates decompression on its own `compressedPayloads` set, so this check
 * is the second line of defense, not the first.
 */
function isCompressedPayload(value: unknown): value is CompressedPayload {
  if (typeof value !== 'object' || value === null) return false;
  const record = value as Record<string, unknown>;
  return (
    Object.keys(record).length === 2 &&
    record.__busCompressed === 'deflate' &&
    typeof record.data === 'string'
  );
}

/**
 * Serializes a payload to its canonical byte form for the compression
 * size check: the UTF-8 JSON encoding. Returns `undefined` for payloads
 * with no JSON encoding (`undefined`, functions, symbols) or ones
 * `JSON.stringify` cannot represent (circular structures, BigInt) — those
 * are uncompressible and pass through untouched, exactly as the durable
 * log already treats them.
 */
function serializeForCompression(payload: unknown): string | undefined {
  try {
    const serialized = JSON.stringify(payload);
    return typeof serialized === 'string' ? serialized : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Options for `EventBus.publishIdempotent`: `PublishOptions` plus the
 * idempotency key.
 */
export interface IdempotentPublishOptions extends PublishOptions {
  /**
   * Idempotency key for this publish. The dedup identity is the pair
   * `(topic, messageId)`: the same `messageId` on different topics is
   * independent, so one payment id can be reused across unrelated topics
   * without interference.
   *
   * Absent or empty disables dedup — the publish behaves exactly like
   * `publish`, still returning an `IdempotentPublishResult` with
   * `duplicate: false`. A non-string value is treated as absent.
   */
  messageId?: string;
}

/**
 * Outcome of `EventBus.publishIdempotent`.
 */
export interface IdempotentPublishResult {
  /**
   * True when this publish was suppressed as a duplicate: the same
   * `(topic, messageId)` was already admitted within the idempotency
   * window, so nothing was published — no sequence number consumed,
   * nothing written to the durable log, no rate-limit budget burned — and
   * `accepted` is 0.
   */
  duplicate: boolean;
  /**
   * Number of subscriber queues that accepted the message (the same value
   * `publish` would return). 0 when `duplicate` is true; also 0 for an
   * admitted publish that the schema validator rejected or the rate
   * limiter shed — neither is a duplicate, and neither suppresses the
   * retry.
   */
  accepted: number;
}

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
      duplicateMessages: number;
      filteredMessages: number;
      compressedMessages: number;
      compressedBytesBefore: number;
      compressedBytesAfter: number;
      /** Sum of deflate times in milliseconds (bus clock); see `meanCompressionMs`. */
      compressionTimeMs: number;
    }
  >();
  private totalPublished = 0;
  private totalExpired = 0;
  /**
   * Messages handed to subscriber handlers (global total). Counts each
   * handler invocation, so at-least-once redeliveries count again; a
   * handler that throws still received the message, and still counts.
   */
  private totalDelivered = 0;
  /**
   * Messages shed by subscriber backpressure queues (global total). Every
   * queue insertion funnels through `enqueueMessage`, so this matches the
   * sum of the per-subscriber `droppedCount` values for live subscribers.
   */
  private totalDropped = 0;
  /**
   * Messages shed at the publish side by adaptive throttling (global
   * total). Incremented next to the per-subscriber `throttledDrops` (see
   * `throttledCount`); counted separately from backpressure queue drops.
   */
  private totalThrottled = 0;
  private totalSequenceGaps = 0;
  /**
   * Per-topic sequence counters. Each message published to a topic takes
   * the next number, so every message carries a topic-scoped monotonic
   * `seq` starting at 1. Grows with the distinct topics ever published to —
   * the same bound as `topicStats`.
   */
  private topicSeq = new Map<string, number>();
  /**
   * Per-topic sliding-window publish rate table (see `src/rates.ts`).
   * Sampled once per accepted publish in `fanOut` — after the admission
   * gates (schema validation, rate-limit budget) — with the bus's injected
   * clock, so rates are deterministic in tests. Grows with the distinct
   * topics ever published to, one fixed-capacity ring per topic — the same
   * bound as `topicStats`.
   */
  private readonly publishRates = new PublishRateTable();
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
   * Idempotent publishes suppressed as duplicates (global total). A
   * duplicate is dropped before admission — no sequence number, no
   * durable-log write, no rate-limit budget — and counted here and on the
   * topic's `duplicateMessages` stat only.
   */
  private totalDuplicates = 0;
  /**
   * Idempotency dedup table for `publishIdempotent`: dedup key ->
   * bus-clock timestamp of the admitted publish that claimed it. The
   * dedup key is `${topic}\0${messageId}`, so the same messageId on
   * different topics is independent. The map is insertion-ordered, which
   * `pruneDedup` and the bounded eviction rely on: the head is the oldest
   * entry (approximately — an injected clock may move non-monotonically,
   * so per-key expiry is always re-checked on lookup).
   *
   * Only admitted messages (past schema validation and the rate-limit
   * budget) occupy slots: a rejected or shed first attempt must not
   * suppress its retry.
   */
  private dedup = new Map<string, number>();
  /**
   * Dedup window in milliseconds (`EventBusOptions.idempotencyWindowMs`):
   * a retry within this long of the admitted publish is a duplicate.
   * Measured on the injected bus clock.
   */
  private readonly idempotencyWindowMs: number;
  /**
   * Maximum entries in the dedup table
   * (`EventBusOptions.idempotencyMaxEntries`): the oldest entry is evicted
   * when the table is full.
   */
  private readonly idempotencyMaxEntries: number;
  /**
   * Messages skipped by subscriber content filters (global total, see
   * `SubscribeOptions.filter`). A filtered message was fanned out but
   * never entered the rejecting subscriber's queue — no backpressure
   * budget consumed, no sequence gap reported.
   */
  private totalFiltered = 0;
  /**
   * Total messages moved into subscriber dead-letter queues (see
   * `ReliableSubscribeOptions.deadLetter`). Counts every dead-lettering,
   * including replays that failed again.
   */
  private totalDeadLettered = 0;
  /**
   * Per-topic payload compression rules, keyed by the exact string passed
   * to `setTopicCompression` — either a concrete topic name or a wildcard
   * pattern. Match resolution mirrors the TTL, rate-limit, and schema
   * rules: an exact-topic rule wins over any pattern; between patterns the
   * earliest-registered rule wins.
   */
  private compressionRules = new Map<string, ResolvedCompressionRule>();
  /**
   * Compiled matchers for the wildcard entries in `compressionRules`, kept
   * apart from the subscriber pattern cache so compression configuration
   * never inflates `patternCacheSize`.
   */
  private compressionMatcherCache = new Map<string, RegExp>();
  /**
   * Preset dictionaries by SHA-256 id, registered by `setTopicCompression`.
   * Lets durable-log replay inflate dictionary-compressed records after a
   * restart, where the rule that wrote them may no longer be configured:
   * the record carries the dictionary id, and the bytes come from here.
   * Bounded (LRU, 64 entries) so churning rules cannot grow the bus
   * without bound; dictionaries are tiny (≤32 KiB) and few in practice.
   */
  private compressionDictionaryRegistry = new Map<string, Buffer>();
  /** Messages whose payload the bus actually compressed (global total). */
  private totalCompressed = 0;
  /** Serialized payload bytes before compression (global total). */
  private totalCompressedBytesBefore = 0;
  /** Deflated payload bytes after compression (global total). */
  private totalCompressedBytesAfter = 0;
  /** Sum of deflate times in milliseconds, bus clock (global total). */
  private totalCompressionTimeMs = 0;
  /**
   * Messages currently carrying a compressed payload envelope. Live
   * messages are registered here by the publish path when they are
   * compressed; replayed messages are registered by `replayLog` when the
   * log record carries an envelope. Decompression at delivery is gated on
   * this set — NOT on the envelope marker alone — so a user payload that
   * happens to share the envelope's shape is never mistaken for a
   * compressed one. Entries die with their message (WeakSet).
   */
  private compressedPayloads = new WeakSet<BusMessage>();
  /**
   * Preset dictionary used to compress each live message, for the
   * messages the publish path actually compressed with one. Inflating
   * requires the EXACT dictionary bytes, so they travel with the message
   * (entries die with it) rather than being re-read from the rule — a
   * rule change between publish and delivery must not corrupt inflation.
   */
  private compressedDictionaries = new WeakMap<BusMessage, Buffer>();
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
   * on it by itself. When a durable log is configured the checkpoints are
   * additionally journaled to disk (see `DurableTopicLog.appendOffset`)
   * and reseeded from the journal at construction.
   */
  private committedOffsets = new Map<string, Map<string, number>>();
  /**
   * Active handoff-linger windows: groupId -> windows (see
   * `GroupSubscribeOptions.handoffLingerMs`). While a window is live,
   * durable-log replay skips the covered seqs for members of the group,
   * so a leaving member's in-flight work is never delivered twice.
   * Bounded: windows are pruned on every read and capped per group.
   */
  private lingerWindows = new Map<string, LingerWindow[]>();
  private readonly now: () => number;
  /**
   * Durable topic log, present only when `EventBusOptions.durableLogDir`
   * was given. Every `fanOut` appends the stamped message here; the
   * per-topic sequence counters are seeded from it at construction so a
   * restart never reuses a sequence number.
   */
  private readonly durableLog?: DurableTopicLog;
  /**
   * Timer heap of scheduled delayed deliveries, ordered by due time (see
   * `publishDelayed`). Entries wait here — consuming no sequence number,
   * no durable-log message record, and no rate-limit budget — until the
   * bus clock reaches their `deliverAt`, when the sweep fans them out
   * through the normal publish pipeline.
   */
  private delayHeap = new DelayHeap();
  /**
   * Pending delayed deliveries by their `publishDelayed` id. The heap may
   * additionally hold lazily-cancelled entries (see `DelayedEntry`), so
   * this map — not the heap size — is the source of truth for
   * `pendingDelayed`.
   */
  private delayedById = new Map<string, DelayedEntry>();
  /** Counter for `publishDelayed` ids (`delayed-1`, …). */
  private nextDelayedId = 0;
  /**
   * Wall-clock timer armed for the heap's next due time. Unref'd — a
   * pending delayed message never keeps the process alive on its own —
   * and re-armed on every schedule/cancel/sweep. The delay is derived
   * from the bus clock; the timer itself is real wall time, like the
   * shaping re-flush timer.
   */
  private delayTimer?: ReturnType<typeof setTimeout>;

  constructor(options?: EventBusOptions) {
    this.now = options?.now ?? Date.now;
    const idempotencyWindowMs = options?.idempotencyWindowMs ?? 60_000;
    if (!Number.isFinite(idempotencyWindowMs) || idempotencyWindowMs <= 0) {
      throw new RangeError(
        'idempotencyWindowMs must be a positive finite number of milliseconds',
      );
    }
    this.idempotencyWindowMs = idempotencyWindowMs;
    const idempotencyMaxEntries = options?.idempotencyMaxEntries ?? 10_000;
    if (!Number.isInteger(idempotencyMaxEntries) || idempotencyMaxEntries < 1) {
      throw new RangeError('idempotencyMaxEntries must be a positive integer');
    }
    this.idempotencyMaxEntries = idempotencyMaxEntries;
    if (options?.durableLogDir != null) {
      this.durableLog = DurableTopicLog.open({
        dir: options.durableLogDir,
        maxEntriesPerTopic: options.durableLogMaxEntriesPerTopic,
        keyCompaction: options.durableLogKeyCompaction,
      });
      // Recover numbering continuity: the next publish on a logged topic
      // continues the on-disk sequence, and stats reflect the recovered
      // history instead of pretending the bus is brand new.
      for (const topic of this.durableLog.topics()) {
        const lastSeq = this.durableLog.lastSeq(topic);
        this.topicSeq.set(topic, lastSeq);
        this.topicStats.set(topic, {
          subscriberCount: 0,
          // `messageCount` excludes the seq-0 delayed-delivery schedule
          // records: a scheduled-but-pending message has not fanned out
          // yet, so it must not count as published.
          publishedMessages: this.durableLog.messageCount(topic),
          expiredMessages: 0,
          lastSeq,
          sequenceGaps: 0,
          rateLimitedMessages: 0,
          rejectedMessages: 0,
          duplicateMessages: 0,
          filteredMessages: 0,
          compressedMessages: 0,
          compressedBytesBefore: 0,
          compressedBytesAfter: 0,
          compressionTimeMs: 0,
        });
      }
      // Rebuild pending delayed-delivery timers from the log: schedules
      // that never fanned out (and were not cancelled) survive the
      // restart. Already-due entries fan out immediately.
      this.recoverDelayed();
      // Reseed consumer-group checkpoints from the offset journal: the
      // highest committed seq per (group, topic) wins, so a restarted bus
      // resumes committed offsets instead of forgetting them — a rejoining
      // member that seeds `resumeFromSeq` from `getCommittedOffsets` picks
      // up exactly where its predecessor committed.
      for (const [key, seq] of this.durableLog.recoveredCommittedOffsets()) {
        const sep = key.indexOf('\0');
        const groupId = key.slice(0, sep);
        const topic = key.slice(sep + 1);
        let committed = this.committedOffsets.get(groupId);
        if (committed == null) {
          committed = new Map<string, number>();
          this.committedOffsets.set(groupId, committed);
        }
        committed.set(topic, seq);
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
   * Opts topics matching `topicPattern` into publish-side payload
   * compression. `topicPattern` accepts the same wildcard syntax as
   * `subscribe` (or an exact topic name). A publish whose payload
   * serializes to more than `opts.thresholdBytes` UTF-8 JSON bytes is
   * deflate-compressed (`node:zlib`, zero new dependencies); smaller
   * payloads pass through untouched and never pay deflate CPU.
   *
   * The compressed bytes — wrapped in a bus-internal envelope — are what
   * get written to the durable log and fanned out to subscriber queues;
   * subscribers transparently receive the original payload (inflated just
   * before delivery), so compression is invisible to handlers. Compression
   * never changes message semantics: it consumes no sequence numbers,
   * moves no TTL deadlines, and is orthogonal to ACK redelivery (a
   * redelivered message is inflated once, on its first delivery).
   *
   * Compression is a no-op unless it actually shrinks the payload: when
   * deflate would not make the bytes smaller (incompressible data, or an
   * absurd level like 0), the message passes through uncompressed and is
   * not counted. Payloads with no JSON encoding (`undefined`, functions,
   * symbols, circular structures, BigInt) are uncompressible and pass
   * through untouched — like the durable log, compression is designed for
   * JSON-shaped payloads.
   *
   * Rule matching mirrors `setTopicTtl`: an exact-topic rule wins over
   * patterns, the earliest-registered matching pattern wins. Re-setting a
   * rule replaces it. Throws when `topicPattern` is empty, when
   * `thresholdBytes` is not a positive finite number, when `level` is
   * not an integer in 0–9, or when `dictionary` is not a non-empty byte
   * buffer of at most 32 KiB.
   */
  setTopicCompression(topicPattern: string, opts: TopicCompressionOptions): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (opts == null || !Number.isFinite(opts.thresholdBytes) || opts.thresholdBytes <= 0) {
      throw new RangeError('thresholdBytes must be a positive finite number of bytes');
    }
    const level = opts.level ?? 6;
    if (!Number.isInteger(level) || level < 0 || level > 9) {
      throw new RangeError('level must be an integer in 0-9');
    }
    const dictionary = coerceCompressionDictionary(opts.dictionary);
    let dictionaryId: string | undefined;
    if (dictionary !== undefined) {
      dictionaryId = createHash('sha256').update(dictionary).digest('hex');
      this.registerCompressionDictionary(dictionaryId, dictionary);
    }
    this.compressionRules.set(topicPattern, { thresholdBytes: opts.thresholdBytes, level, dictionary, dictionaryId });
  }

  /**
   * Registers a preset dictionary for durable-log replay, keyed by its
   * SHA-256 id. LRU-capped at 64 entries: replays resolve the bytes by id,
   * so the registry only needs the dictionaries of rules ever configured
   * on this bus, and churning rules cannot leak memory.
   */
  private registerCompressionDictionary(id: string, dictionary: Buffer): void {
    this.compressionDictionaryRegistry.delete(id);
    this.compressionDictionaryRegistry.set(id, dictionary);
    while (this.compressionDictionaryRegistry.size > MAX_REGISTERED_COMPRESSION_DICTIONARIES) {
      const oldest = this.compressionDictionaryRegistry.keys().next();
      if (oldest.done) break;
      this.compressionDictionaryRegistry.delete(oldest.value);
    }
  }

  /**
   * Resolves the preset dictionary for a durable-log record (by the
   * `dictId` the record carries), or `undefined` for dictionary-less
   * records. A record whose dictionary is no longer registered — rule
   * never configured on this bus, or evicted by registry churn — throws
   * a clear error naming the topic and sequence: inflating with a wrong
   * or missing dictionary would corrupt data, so replay fails loudly
   * instead. Re-registering the rule with the identical dictionary bytes
   * fixes it.
   */
  private dictionaryForRecord(record: { topic: string; seq: number; dictId?: string }): Buffer | undefined {
    const dictId = record.dictId;
    if (dictId === undefined) return undefined;
    const dictionary = this.compressionDictionaryRegistry.get(dictId);
    if (dictionary !== undefined) {
      // LRU touch: a replayed dictionary is live again.
      this.compressionDictionaryRegistry.delete(dictId);
      this.compressionDictionaryRegistry.set(dictId, dictionary);
      return dictionary;
    }
    throw new Error(
      `cannot inflate compressed record for topic "${record.topic}" (seq ${record.seq}): ` +
        'its preset dictionary is not registered on this bus — call setTopicCompression with the ' +
        'same dictionary bytes before replaying this log',
    );
  }

  /**
   * Removes the compression rule previously registered for `topicPattern`.
   * Messages already queued keep whatever form they were published with;
   * only new publishes are affected. Returns true when a rule existed and
   * was removed.
   */
  clearTopicCompression(topicPattern: string): boolean {
    return this.compressionRules.delete(topicPattern);
  }

  /**
   * Returns the compression options that apply to `topic`, or `undefined`
   * when no rule matches. Same precedence as `ttlForTopic`.
   */
  private compressionForTopic(topic: string): ResolvedCompressionRule | undefined {
    const exact = this.compressionRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, rule] of this.compressionRules) {
      let matcher = this.compressionMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.compressionMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return rule;
    }
    return undefined;
  }

  /**
   * Applies the topic's compression rule to a publish, if any. Returns the
   * payload as it should travel on the wire: either the original payload
   * (no rule, below threshold, uncompressible, or deflate did not shrink
   * it) or the compression envelope. When compression is adopted, the
   * message is registered in `compressedPayloads` (the delivery-time
   * decompression gate) and the per-topic + global compression counters
   * are updated.
   *
   * Runs on the admitted message only — after schema validation and after
   * the rate-limit shed — so rejected/shed publishes never pay deflate
   * CPU and never touch compression metrics.
   */
  private compressPayload(
    msg: BusMessage,
    payload: unknown,
    rule: ResolvedCompressionRule,
    stats: { compressedMessages: number; compressedBytesBefore: number; compressedBytesAfter: number; compressionTimeMs: number },
  ): unknown {
    const serialized = serializeForCompression(payload);
    if (serialized === undefined) return payload;
    const bytesBefore = Buffer.byteLength(serialized, 'utf8');
    // Small messages pass through: compression is opt-in per byte saved,
    // not a tax on every publish.
    if (bytesBefore <= rule.thresholdBytes) return payload;
    const startedAtMs = this.now();
    const deflated =
      rule.dictionary === undefined
        ? deflateSync(serialized, { level: rule.level })
        : deflateSync(serialized, { level: rule.level, dictionary: rule.dictionary });
    const elapsedMs = Math.max(0, this.now() - startedAtMs);
    // Never adopt an encoding that does not shrink the payload —
    // incompressible data (or level 0) would only add envelope overhead.
    if (deflated.length >= bytesBefore) return payload;
    const envelope: CompressedPayload = {
      __busCompressed: 'deflate',
      data: deflated.toString('base64'),
    };
    this.compressedPayloads.add(msg);
    // The exact dictionary bytes travel with the message: inflating needs
    // them byte-identical, and a rule change between publish and delivery
    // must not corrupt inflation.
    if (rule.dictionary !== undefined) this.compressedDictionaries.set(msg, rule.dictionary);
    stats.compressedMessages += 1;
    stats.compressedBytesBefore += bytesBefore;
    stats.compressedBytesAfter += deflated.length;
    stats.compressionTimeMs += elapsedMs;
    this.totalCompressed += 1;
    this.totalCompressedBytesBefore += bytesBefore;
    this.totalCompressedBytesAfter += deflated.length;
    this.totalCompressionTimeMs += elapsedMs;
    return envelope;
  }

  /**
   * Restores a message's original payload just before delivery. Only acts
   * on messages the publish path registered in `compressedPayloads` — a
   * plain payload, even one shaped like the envelope, passes through
   * untouched. Idempotent: after the first inflation the payload is the
   * original value, so redeliveries and multi-subscriber fan-out of the
   * same message never inflate twice.
   *
   * A message registered here whose payload is not a well-formed envelope
   * can only come from a tampered durable log (the bus never produces
   * one); it throws rather than delivering corrupt data silently.
   */
  private inflateMessagePayload(msg: BusMessage): void {
    if (!this.compressedPayloads.has(msg)) return;
    this.compressedPayloads.delete(msg);
    const payload = msg.payload;
    if (!isCompressedPayload(payload)) {
      throw new Error(
        `compressed payload envelope for topic "${msg.topic}" (seq ${msg.seq}) is malformed`,
      );
    }
    // A message compressed with a preset dictionary inflates with the
    // bytes carried on the message; one compressed without inflates with
    // `undefined`, exactly like the legacy path.
    const dictionary = this.compressedDictionaries.get(msg);
    this.compressedDictionaries.delete(msg);
    msg.payload = inflateEnvelopePayload(payload, dictionary);
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
  subscribe(topicPattern: string, handler: MessageHandler, opts?: SubscribeOptions): Subscription;
  /**
   * Batched overload: with `batch` enabled the handler receives up to
   * `maxSize` messages per call (see `BatchMessageHandler`).
   */
  subscribe(
    topicPattern: string,
    handler: BatchMessageHandler,
    opts: SubscribeOptions & { batch: true | BatchDeliveryOptions },
  ): Subscription;
  subscribe(
    topicPattern: string,
    handler: MessageHandler | BatchMessageHandler,
    opts?: SubscribeOptions,
  ): Subscription {
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const resumeFromSeq = opts?.resumeFromSeq;
    this.validateResumeFromSeq(resumeFromSeq);
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
    const deliveryShaping = resolveDeliveryShapingOptions(opts?.deliveryShaping, this.now);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const deliveryLatency = resolveDeliveryLatencyOptions(opts?.deliveryLatency);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const batch = resolveBatchDeliveryOptions(opts?.batch);
    const onDegraded = opts?.onDegraded;
    const filter = opts?.filter;
    if (filter !== undefined && typeof filter !== 'function') {
      throw new TypeError('filter must be a function');
    }
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
      // A batched handler receives an array per call; the cast is safe —
      // drainSubscriber only ever passes an array to batched subscribers.
      handler: handler as MessageHandler,
      queue,
      lastDeliveredSeq: new Map(),
      throttle,
      onThrottled,
      deliveryShaping,
      latency: deliveryLatency,
      batch,
      health:
        healthProbe == null
          ? undefined
          : { consecutiveFailures: 0, degraded: false, ...healthProbe },
      onDegraded,
      filter,
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
        // Pending timers must not outlive the subscriber: no phantom
        // resume or re-flush after unsubscribe.
        const health = subscriber.health;
        if (health?.autoResumeTimer !== undefined) {
          clearTimeout(health.autoResumeTimer);
          health.autoResumeTimer = undefined;
        }
        this.clearShapingTimer(subscriber);
        this.clearBatchTimer(subscriber);
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
   *
   * With `deadLetter` enabled, redelivery is bounded: a message requeued
   * more than `maxRedeliveries` times moves to the subscriber's
   * dead-letter queue instead of being requeued forever (see
   * `getDeadLetterMessages` / `replayDeadLetter`).
   */
  subscribeReliable(
    topicPattern: string,
    handler: ReliableMessageHandler,
    opts?: ReliableSubscribeOptions,
  ): Subscription;
  /**
   * Batched reliable overload: with `batch` enabled the handler receives
   * one `Delivery` envelope per message in the batch (see
   * `ReliableBatchMessageHandler`) — ack each delivery to confirm the
   * batch, nack to requeue.
   */
  subscribeReliable(
    topicPattern: string,
    handler: ReliableBatchMessageHandler,
    opts: ReliableSubscribeOptions & { batch: true | BatchDeliveryOptions },
  ): Subscription;
  subscribeReliable(
    topicPattern: string,
    handler: ReliableMessageHandler | ReliableBatchMessageHandler,
    opts?: ReliableSubscribeOptions,
  ): Subscription {
    // The bus flush calls `Subscriber.handler` with bare messages; wrap it
    // so reliable subscribers transparently get tracked deliveries instead.
    // Assigned synchronously here, before any flush microtask can run.
    // Validate the DLQ options before subscribing: a rejected option must
    // not leave a half-registered subscription behind.
    const dlq = normalizeDeadLetterOptions(opts?.deadLetter, 'subscribeReliable');
    let deliver!: (msg: BusMessage | BusMessage[]) => void;
    const sub = this.subscribe(topicPattern, (msg: BusMessage | BusMessage[]) => deliver(msg), opts);
    const subscriber = this.subscribers.get(sub.id);
    if (subscriber == null) throw new Error(`unknown subscriber: ${sub.id}`);
    const redeliveries = new WeakMap<BusMessage, number>();
    const tracker = new AckTracker<BusMessage>({
      ackTimeoutMs: opts?.ackTimeoutMs ?? 5000,
      onRedeliver: (msg) => {
        const count = (redeliveries.get(msg) ?? 0) + 1;
        redeliveries.set(msg, count);
        // The redelivery budget is exhausted: the message is poison
        // (repeated nacks, ack timeouts, or a handler that keeps throwing
        // and never settles). Dead-letter it instead of requeueing
        // forever.
        if (dlq != null && count > dlq.maxRedeliveries) {
          this.moveToDeadLetter(subscriber, msg, count - 1);
          return;
        }
        this.enqueueMessage(subscriber, msg, this.messageDeadlines.get(msg));
        this.scheduleFlush();
      },
    });
    subscriber.reliable = { tracker, redeliveries };
    if (dlq != null) {
      subscriber.deadLetter = { entries: [], nextSeq: 0, ...dlq };
    }
    deliver = (msgOrBatch) => {
      // Batched delivery: one tracked envelope per message in the batch,
      // handed to the handler as a single array call.
      if (Array.isArray(msgOrBatch)) {
        const batchHandler = handler as ReliableBatchMessageHandler;
        const deliveries = msgOrBatch.map((m) => tracker.track(m, redeliveries.get(m) ?? 0));
        if (dlq == null) {
          batchHandler(deliveries);
          return;
        }
        // With a DLQ configured, a synchronously throwing batch handler
        // nacks the whole batch instead of crashing the bus: every message
        // is requeued in batch order, so the redelivery preserves FIFO —
        // the batch analogue of the single-message nack path below.
        try {
          batchHandler(deliveries);
        } catch {
          for (const d of deliveries) d.nack();
        }
        return;
      }
      const msg = msgOrBatch;
      const singleHandler = handler as ReliableMessageHandler;
      const delivery = tracker.track(msg, redeliveries.get(msg) ?? 0);
      if (dlq == null) {
        singleHandler(delivery);
        return;
      }
      // With a DLQ configured, a synchronously throwing handler is an
      // immediate redelivery attempt, not a process crash: without this
      // the throw would escape the flush microtask and kill the process,
      // while the message would be requeued anyway once the ack timer
      // fired. Counting it now keeps the failure fast, deterministic,
      // and inside the redelivery budget — repeated throws land the
      // message in the DLQ instead of crashing the bus.
      try {
        singleHandler(delivery);
      } catch {
        delivery.nack();
      }
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
   *   `commitOffset`; with a durable topic log
   *   (`EventBusOptions.durableLogDir`) checkpoints are additionally
   *   journaled to disk and reseeded on restart, so combined with
   *   `SubscribeOptions.resumeFromSeq` a rejoining member resumes where
   *   it committed — even across process restarts.
   * - A leaving member may open a graceful-handoff linger window
   *   (`GroupSubscribeOptions.handoffLingerMs`): while the window is
   *   open, durable-log replay skips the group's assigned-but-uncommitted
   *   backlog for that member's in-flight work, so it is never delivered
   *   twice. Operational recipe: `commitOffset` before leaving — the
   *   linger only covers what the leaver did not commit.
   *
   * All queue options (`queueSize`, `dropPolicy`, `onBackpressure`,
   * `throttle`, ...) behave per member exactly as in `subscribe`. Throws
   * `RangeError` when `groupId` is empty or `handoffLingerMs` is not a
   * finite number `>= 0`.
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
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const handoffLingerMs = opts?.handoffLingerMs ?? 0;
    if (!Number.isFinite(handoffLingerMs) || handoffLingerMs < 0) {
      throw new RangeError('handoffLingerMs must be a finite number of milliseconds >= 0');
    }
    // The durable-log replay is deferred until after the group assignment
    // is recorded below: `replayLog` skips seqs inside the group's live
    // handoff-linger windows, which needs `subscriber.groupId` to be set.
    const { resumeFromSeq, ...restOpts } = opts ?? {};
    this.validateResumeFromSeq(resumeFromSeq);
    const sub = this.subscribe(topicPattern, handler, restOpts);
    const subscriber = this.subscribers.get(sub.id);
    if (subscriber == null) throw new Error(`unknown subscriber: ${sub.id}`);
    subscriber.groupId = groupId;
    subscriber.onRebalance = opts?.onRebalance;
    subscriber.handoffLingerMs = handoffLingerMs;
    const key = EventBus.groupKey(groupId, topicPattern);
    let members = this.groupMembers.get(key);
    if (members == null) {
      members = [];
      this.groupMembers.set(key, members);
    }
    members.push(sub.id);
    if (resumeFromSeq !== undefined) {
      this.replayLog(subscriber, resumeFromSeq);
    }
    this.fireRebalance(groupId, topicPattern, key, 'join', sub.id);
    return {
      id: sub.id,
      unsubscribe: () => {
        // Second call is a no-op: no double leave-event, no roster churn.
        if (!this.subscribers.has(sub.id)) return;
        this.removeGroupMember(key, sub.id);
        const linger = this.beginLingerHandoff(groupId, subscriber);
        sub.unsubscribe();
        this.fireRebalance(groupId, topicPattern, key, 'leave', sub.id, linger);
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
   * `linger` (leave events only) carries the handoff window opened for
   * the departed member, so the event observes the no-duplicate handoff.
   */
  private fireRebalance(
    groupId: string,
    pattern: string,
    key: string,
    trigger: 'join' | 'leave',
    memberId: string,
    linger?: { until: number; windows: LingerWindow[] },
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
        ...(linger == null
          ? {}
          : {
              lingerUntil: linger.until,
              lingering: linger.windows.map((w) => ({ topic: w.topic, fromSeq: w.fromSeq, toSeq: w.toSeq })),
            }),
      });
    }
  }

  /**
   * Opens a handoff-linger window for a leaving group member (see
   * `GroupSubscribeOptions.handoffLingerMs`). Returns the window summary
   * for the rebalance event, or `undefined` when no window was opened.
   *
   * A window covers, per topic, the seqs the group assigned but nobody
   * committed: `(committed, assigned]`. While any window is live,
   * `replayLog` skips those seqs for members of the group, so a rejoining
   * member's resume never delivers the leaver's in-flight work a second
   * time. After the window expires the backlog is replayable again — the
   * leaver is presumed dead, at-least-once resumes.
   *
   * No window opens when the member configured no linger, when there is
   * no uncommitted backlog, or when no durable log is configured: without
   * a log there is no replay to suppress, so a linger would be inert.
   */
  private beginLingerHandoff(
    groupId: string,
    subscriber: Subscriber,
  ): { until: number; windows: LingerWindow[] } | undefined {
    const lingerMs = subscriber.handoffLingerMs ?? 0;
    if (lingerMs <= 0 || this.durableLog == null) return undefined;
    const assigned = this.groupOffsets.get(groupId);
    if (assigned == null) return undefined;
    const committed = this.committedOffsets.get(groupId);
    const until = this.now() + lingerMs;
    const windows: LingerWindow[] = [];
    for (const [topic, toSeq] of assigned) {
      const fromSeq = committed?.get(topic) ?? 0;
      if (toSeq > fromSeq) windows.push({ topic, fromSeq, toSeq, until });
    }
    if (windows.length === 0) return undefined;
    let list = this.lingerWindows.get(groupId);
    if (list == null) {
      list = [];
      this.lingerWindows.set(groupId, list);
    }
    list.push(...windows);
    // Bound memory: linger windows are transient; when a group churns an
    // absurd number of short-lived members, drop the oldest windows —
    // their handoffs have long expired anyway.
    if (list.length > 1024) list.splice(0, list.length - 1024);
    return { until, windows };
  }

  /**
   * Drops expired linger windows for a group; deletes the group's row
   * when no live window remains. Returns the live windows.
   */
  private pruneLingerWindows(groupId: string): LingerWindow[] {
    const list = this.lingerWindows.get(groupId);
    if (list == null) return [];
    const now = this.now();
    const live = list.filter((w) => w.until > now);
    if (live.length === 0) this.lingerWindows.delete(groupId);
    else this.lingerWindows.set(groupId, live);
    return live;
  }

  /** True when `seq` on `topic` sits inside one of the group's live linger windows. */
  private isLingering(groupId: string, topic: string, seq: number): boolean {
    for (const w of this.pruneLingerWindows(groupId)) {
      if (w.topic === topic && seq > w.fromSeq && seq <= w.toSeq) return true;
    }
    return false;
  }

  /**
   * Validates the durable-log resume option before anything registers: a
   * throw must not leave a half-registered subscriber behind. Shared by
   * `subscribe` and `subscribeToGroup` (the latter replays after recording
   * the group assignment — see below).
   */
  private validateResumeFromSeq(resumeFromSeq: number | undefined): void {
    if (resumeFromSeq === undefined) return;
    if (this.durableLog == null) {
      throw new RangeError('resumeFromSeq requires EventBusOptions.durableLogDir to be set');
    }
    if (!Number.isInteger(resumeFromSeq) || resumeFromSeq < 0) {
      throw new RangeError('resumeFromSeq must be a non-negative integer');
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
   * When a durable log is configured (`EventBusOptions.durableLogDir`)
   * the checkpoint is additionally appended to the log's group-offset
   * journal, so committed offsets survive a process restart (the journal
   * is append-only; the highest seq per (group, topic) wins at recovery).
   * A journal write failure never fails the in-memory commit — the live
   * process keeps serving from memory.
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
    this.durableLog?.appendOffset(groupId, topic, seq, this.now());
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
    const filter = subscriber.filter;
    for (const rec of records) {
      // A subscriber content filter applies to replay exactly as it does
      // to live fan-out: a rejected replayed message is never queued (it
      // must not churn the backpressure budget on reconnect) and advances
      // the per-topic baseline so it never counts as a gap. The log holds
      // the wire payload — inflate a compressed envelope first so the
      // filter judges the raw application payload. A dictionary-compressed
      // record resolves its bytes from the registry here; an unregistered
      // dictionary fails loudly rather than judging garbage.
      if (filter !== undefined) {
        const rawPayload = isCompressedPayload(rec.payload)
          ? inflateEnvelopePayload(rec.payload, this.dictionaryForRecord(rec))
          : rec.payload;
        if (!filter(rawPayload, rec.topic)) {
          this.countFiltered(subscriber, rec.topic, rec.seq);
          continue;
        }
      }
      // Handoff linger: a group member that left within its linger window
      // is presumed still processing these seqs — skip them so the
      // rejoining member's resume never delivers in-flight work twice.
      // Like a content filter, the skip must not churn backpressure or
      // count as a sequence gap: the per-topic baseline advances over it.
      // After the window expires the same replay delivers them normally
      // (the leaver is presumed dead; at-least-once resumes).
      const groupId = subscriber.groupId;
      if (groupId !== undefined && this.isLingering(groupId, rec.topic, rec.seq)) {
        const last = subscriber.lastDeliveredSeq.get(rec.topic);
        if (last === undefined || rec.seq > last) subscriber.lastDeliveredSeq.set(rec.topic, rec.seq);
        continue;
      }
      const msg: BusMessage = { topic: rec.topic, payload: rec.payload, seq: rec.seq };
      // The log stores the wire payload: a record written while a
      // compression rule was active carries the deflated envelope, so
      // re-register it for transparent inflation at delivery — exactly
      // like a live compressed message. Dictionary-compressed records
      // additionally re-register their dictionary from the registry; a
      // missing dictionary throws here (fail closed at replay) instead of
      // corrupting data at delivery.
      if (isCompressedPayload(rec.payload)) {
        this.compressedPayloads.add(msg);
        const dictionary = this.dictionaryForRecord(rec);
        if (dictionary !== undefined) this.compressedDictionaries.set(msg, dictionary);
      }
      if (rec.expiresAt !== undefined) this.messageDeadlines.set(msg, rec.expiresAt);
      this.enqueueMessage(subscriber, msg, rec.expiresAt);
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
   *
   * `opts.key` opts the message into durable-log keyed compaction (see
   * `PublishOptions.key`).
   */
  publish(topic: string, payload: unknown, opts?: PublishOptions): number {
    validateMessageKey(opts?.key, 'publish');
    const { accepted } = this.fanOut(topic, payload, false, undefined, opts?.key);
    this.scheduleFlush();
    return accepted;
  }

  /**
   * Idempotent publish: within the idempotency window
   * (`EventBusOptions.idempotencyWindowMs`), a `(topic, messageId)` pair is
   * published at most once. A retry carrying the same `messageId` on the
   * same topic — the classic payment/fintech pattern where a client retries
   * after a timeout — returns `{ duplicate: true, accepted: 0 }` instead of
   * publishing again, so the downstream sees the message exactly once and
   * a retried order or settlement can never double-execute.
   *
   * The dedup gate runs before admission, ahead of everything else: a
   * duplicate consumes no sequence number (subscribers see no phantom
   * gap), is never written to the durable log, and burns no rate-limit
   * budget. Schema validation is not re-run for a duplicate either — the
   * gate decides on the identity alone.
   *
   * Window and lifetime semantics:
   * - The window starts when a message is ADMITTED (past schema validation
   *   and the rate-limit budget). A first attempt that the schema
   *   validator rejected or the rate limiter shed claims no dedup slot —
   *   its retry is a fresh publish, never suppressed. Suppressing the
   *   retry of a message that never reached anyone would silently lose it;
   *   idempotency must not do that.
   * - When the window expires, the next publish with the same `messageId`
   *   is admitted again and restarts the window.
   * - The table is bounded by `EventBusOptions.idempotencyMaxEntries`:
   *   when full, the oldest entry is evicted. Expired entries are dropped
   *   from the head on every idempotent publish; an injected clock that
   *   moves non-monotonically may leave an expired entry off-head, and the
   *   per-key lookup re-checks expiry so it can never suppress wrongly.
   * - The window is measured on the bus clock (`EventBusOptions.now`), so
   *   dedup is deterministic in tests.
   *
   * Without a `messageId` (absent or empty) this behaves exactly like
   * `publish`, returning `{ duplicate: false, accepted }`. Suppressed
   * duplicates are counted in `TopicStats.duplicateMessages` (and the
   * global total) — the retry-safety observability metric.
   *
   * `opts.key` opts the message into durable-log keyed compaction (see
   * `PublishOptions.key`); like `publish`, an invalid key throws
   * `RangeError` before anything is mutated.
   */
  publishIdempotent(
    topic: string,
    payload: unknown,
    opts?: IdempotentPublishOptions,
  ): IdempotentPublishResult {
    validateMessageKey(opts?.key, 'publishIdempotent');
    const messageId = opts?.messageId;
    // No identity, no dedup: a plain publish that still reports the same
    // result shape.
    if (typeof messageId !== 'string' || messageId.length === 0) {
      const { accepted } = this.fanOut(topic, payload, false, undefined, opts?.key);
      this.scheduleFlush();
      return { duplicate: false, accepted };
    }
    const nowMs = this.now();
    const dedupKey = `${topic}\0${messageId}`;
    this.pruneDedup(nowMs);
    const firstAt = this.dedup.get(dedupKey);
    if (firstAt !== undefined && nowMs - firstAt < this.idempotencyWindowMs) {
      // Suppressed duplicate: dropped before admission — no sequence
      // number, no durable-log write, no rate-limit budget. Counted for
      // observability only.
      this.countDuplicate(topic);
      return { duplicate: true, accepted: 0 };
    }
    const { accepted, admitted } = this.fanOut(topic, payload, false, undefined, opts?.key);
    this.scheduleFlush();
    if (admitted) {
      // Only an admitted message claims a dedup slot: the window starts at
      // its publish time, and a rejected/shed first attempt leaves the
      // retry free. Deleting before re-inserting refreshes the entry's
      // position so the bounded eviction tracks last-publish order.
      this.dedup.delete(dedupKey);
      this.dedup.set(dedupKey, nowMs);
      while (this.dedup.size > this.idempotencyMaxEntries) {
        const oldest = this.dedup.keys().next();
        if (oldest.done) break;
        this.dedup.delete(oldest.value);
      }
    }
    return { duplicate: false, accepted };
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
   *
   * Each entry may carry a `key` opting it into durable-log keyed
   * compaction (see `BatchMessage`).
   */
  publishBatch(messages: Array<BatchMessage>): number {
    if (messages.length === 0) return 0;
    for (const msg of messages) validateMessageKey(msg.key, 'publishBatch');
    let accepted = 0;
    for (const msg of messages) {
      accepted += this.fanOut(msg.topic, msg.payload, false, undefined, msg.key).accepted;
    }
    this.scheduleFlush();
    return accepted;
  }

  /**
   * Cross-topic atomic batch publish: either every entry is published or
   * none is. The batch goes through two phases, both synchronous, so no
   * subscriber ever observes a partial batch:
   *
   * 1. Admission: every entry is checked against the same admission gates
   *    `publish` applies — schema validation, then the per-topic
   *    rate-limit budget — without mutating any bus state. Rate-limit
   *    tokens are charged against a per-batch shadow balance, so a batch
   *    cannot overdraft the bucket with its own entries. The first entry
   *    that would be rejected or shed aborts the whole batch.
   * 2. Commit: the admitted batch runs through the normal `fanOut` path
   *    (per-topic seq stamping, compression, durable log, fan-out, stats),
   *    then one flush is scheduled for the whole batch — like
   *    `publishBatch`, all messages land in subscriber queues before any
   *    delivery happens.
   *
   * On success returns `{ published: entries.length }`. On failure returns
   * `{ published: 0, rejected: { index, topic, reason } }` and the bus is
   * exactly as before the call: no sequence numbers consumed, no
   * rate-limit tokens taken, no durable-log writes, no stats changes — not
   * even the rejection counters move, because nothing was admitted.
   *
   * TTL is drain-time, not an admission gate: a batch containing messages
   * that expire before the flush is still admitted, and each message
   * expires independently at drain exactly as a lone `publish` would.
   * Downstream per-message semantics (queue drop policies, adaptive
   * publish-side throttling, delivery shaping, ACK) apply to each committed
   * message exactly as they do for `publish`/`publishBatch` — atomicity
   * covers admission, not delivery. A throwing validator propagates to the
   * caller, exactly as in `publish`; it always throws during admission, so
   * a throw can never leave a half-committed batch behind.
   *
   * Validators are expected to be pure: admission runs them once per
   * entry, and the commit phase skips re-validation. An empty batch is a
   * no-op returning `{ published: 0 }` without scheduling a flush.
   */
  publishAtomic(entries: Array<BatchMessage>): AtomicPublishResult {
    if (entries.length === 0) return { published: 0 };
    // Key validation first: a throw must leave zero state behind, and
    // validation mutates nothing.
    for (const entry of entries) validateMessageKey(entry.key, 'publishAtomic');
    // Phase 1: admit the whole batch against shadow state.
    const shadowBudget = new Map<string, number>();
    for (let index = 0; index < entries.length; index++) {
      const { topic, payload } = entries[index];
      const reason = this.admissionVerdict(topic, payload, shadowBudget);
      if (reason !== undefined) return { published: 0, rejected: { index, topic, reason } };
    }
    // Phase 2: everything was admitted — commit through the normal
    // publish path in one synchronous turn, then flush once.
    for (const { topic, payload, key } of entries) this.fanOut(topic, payload, true, undefined, key);
    this.scheduleFlush();
    return { published: entries.length };
  }

  /**
   * Runs the admission gate of the publish pipeline — schema validation
   * first (rejections happen before admission: no seq, no rate-limit
   * budget), then the per-topic rate-limit budget — WITHOUT mutating any
   * bus state, so `publishAtomic` can pre-validate a batch before
   * committing anything. Returns the rejection reason when the publish
   * would be rejected or shed, `undefined` when it would be admitted.
   *
   * `shadowBudget` maps a concrete topic to the tokens this batch has
   * already reserved: the balance peeked from the live bucket
   * (`availableTokens` is a read-only lazy-refill view — it never takes a
   * token) minus those reservations must cover one more message. A topic
   * with no live bucket yet is checked against a locally constructed full
   * bucket, mirroring what `fanOut` would create at commit — the shadow
   * bucket is never stored, so even a rejected batch creates no state.
   *
   * Rule resolution is identical to `publish`: the same `schemaForTopic`
   * / `rateLimitForTopic` resolvers run, so exact-topic rules win over
   * patterns and the earliest-registered matching pattern wins. A throwing
   * validator propagates, exactly as in `publish` — nothing has been
   * mutated at that point.
   */
  private admissionVerdict(
    topic: string,
    payload: unknown,
    shadowBudget: Map<string, number>,
  ): AtomicRejectReason | undefined {
    const validator = this.schemaForTopic(topic);
    if (validator !== undefined && !validator(payload, topic)) return 'schema';
    const limit = this.rateLimitForTopic(topic);
    if (limit !== undefined) {
      let bucket = this.rateLimitBuckets.get(topic);
      if (bucket == null) bucket = new TokenBucket(limit.burst, limit.messagesPerSec, this.now);
      const reserved = shadowBudget.get(topic) ?? 0;
      if (bucket.availableTokens - reserved < 1) return 'rate-limit';
      shadowBudget.set(topic, reserved + 1);
    }
    return undefined;
  }

  /**
   * Schedules a message for future delivery: it enters the timer heap and
   * is fanned out — through the normal publish pipeline — once the bus
   * clock reaches its due time. Returns the delay id (pass it to
   * `cancelDelayed`), or `undefined` when the payload was rejected by the
   * topic's schema validator (fail-fast: nothing is scheduled).
   *
   * Timing: pass `{ delayMs }` for a relative delay or `{ deliverAt }`
   * for an absolute bus-clock timestamp; exactly one of the two is
   * required. A due time at or before now fans out on the next flush,
   * exactly like `publish`.
   *
   * Admission semantics, and why they are this way:
   * - Schema validation runs NOW, fail-fast: a rejected payload never
   *   becomes a scheduled delivery. A throwing validator propagates,
   *   exactly as in `publish`. Validation is not re-run at fan-out
   *   (validators are expected pure — the same contract `publishAtomic`
   *   relies on), so a payload accepted here is never rejected at its due
   *   time.
   * - Everything else waits for the due time: the message consumes no
   *   sequence number, is written to no durable-log message record, and
   *   burns no rate-limit budget until it actually fans out (the EB-20 /
   *   EB-19 shed semantics applied to the schedule-then-deliver split).
   *   At fan-out the message goes through the full pipeline — seq
   *   stamping, rate-limit `take()` (which may shed it like any other
   *   publish), compression, durable log, fan-out — exactly as if it had
   *   been published at that moment.
   * - TTL is stamped at schedule time: the message is "published" now and
   *   only *delivered* later. A message whose deadline passes before its
   *   due time is dropped as expired at sweep time — counted in
   *   `expiredMessages` — never delivered.
   *
   * Durability: with `EventBusOptions.durableLogDir` the schedule is
   * persisted before this method returns (a seq-0 record carrying
   * `deliverAt`), so a restart rebuilds the pending timer and still
   * delivers the message. The payload must therefore be JSON-serializable
   * when the durable log is enabled — otherwise the schedule could not
   * survive a restart, and this method throws instead of scheduling
   * something it cannot keep. Without a durable log the schedule lives in
   * memory only, like everything else on a non-durable bus.
   *
   * Throws `RangeError` when neither or both of `delayMs`/`deliverAt` are
   * given, or when the given value is not a valid timestamp/duration.
   */
  publishDelayed(topic: string, payload: unknown, opts: PublishDelayedOptions = {}): string | undefined {
    const nowMs = this.now();
    const hasDelayMs = opts.delayMs !== undefined;
    const hasDeliverAt = opts.deliverAt !== undefined;
    if (hasDelayMs === hasDeliverAt) {
      throw new RangeError('publishDelayed requires exactly one of delayMs or deliverAt');
    }
    let deliverAt: number;
    if (hasDelayMs) {
      const delayMs = opts.delayMs as number;
      if (!Number.isFinite(delayMs) || delayMs < 0) {
        throw new RangeError('delayMs must be a non-negative finite number of milliseconds');
      }
      deliverAt = nowMs + delayMs;
    } else {
      deliverAt = opts.deliverAt as number;
      if (!Number.isFinite(deliverAt)) {
        throw new RangeError('deliverAt must be a finite bus-clock timestamp in milliseconds');
      }
    }
    // Fail fast on schema: a rejected payload never becomes a scheduled
    // delivery — no id, nothing in the heap, nothing on disk. Counted the
    // same way a `publish` rejection is.
    const validator = this.schemaForTopic(topic);
    if (validator !== undefined && !validator(payload, topic)) {
      this.countSchemaRejection(topic);
      return undefined;
    }
    const ttlMs = this.ttlForTopic(topic);
    const expiresAt = ttlMs === undefined ? undefined : nowMs + ttlMs;
    validateMessageKey(opts.key, 'publishDelayed');
    const id = `delayed-${++this.nextDelayedId}`;
    const entry: DelayedEntry = { id, topic, payload, deliverAt, expiresAt, cancelled: false, key: opts.key };
    // Persist the schedule before it is visible anywhere: a crash between
    // here and the due time must still deliver the message after restart.
    // The schedule record carries no sequence number and burns no
    // rate-limit budget — those happen at fan-out, via the normal path.
    // It does carry the compaction key, so a restart rebuilds the timer
    // with the key intact.
    if (this.durableLog != null) {
      const persisted = this.durableLog.append({
        seq: 0,
        topic,
        at: nowMs,
        deliverAt,
        expiresAt,
        delayId: id,
        key: opts.key,
        payload,
      });
      if (!persisted) {
        throw new Error(
          `publishDelayed: the schedule for topic "${topic}" could not be persisted to the ` +
            'durable log (payload is not JSON-serializable or the log write failed); the ' +
            'delayed delivery was not scheduled',
        );
      }
    }
    this.delayedById.set(id, entry);
    this.delayHeap.push(entry);
    // An already-due entry (`delayMs: 0`, past `deliverAt`) fans out on the
    // next flush — the sweep below fans it into the queues synchronously
    // and schedules the flush, exactly like `publish`.
    this.sweepDelayed(nowMs);
    return id;
  }

  /**
   * Cancels a pending delayed delivery scheduled by `publishDelayed`: the
   * message will never fan out. Returns `true` when a pending schedule was
   * cancelled.
   *
   * Contract: returns `false` (a no-op, never a throw) for unknown ids —
   * never scheduled, already fanned out, already cancelled, or already
   * dropped as expired. Cancellation is recorded in the durable log (when
   * enabled) so a restart does not resurrect the schedule.
   */
  cancelDelayed(delayId: string): boolean {
    const entry = this.delayedById.get(delayId);
    if (entry == null) return false;
    // Lazy heap deletion: the entry stays in the heap, marked, and the
    // sweep skips it when it surfaces — cancel is O(1).
    entry.cancelled = true;
    this.delayedById.delete(delayId);
    this.appendDelayTombstone(delayId, entry.topic);
    this.armDelayTimer();
    return true;
  }

  /**
   * Fans out every delayed entry whose due time has arrived (bus clock),
   * in due-time order. Due entries go through the normal `fanOut` path —
   * sequence stamping, rate-limit budget, compression, durable log (whose
   * record carries `deliverAt`/`delayId`), fan-out — so a delayed message
   * is indistinguishable from a publish made at its due time.
   *
   * An entry whose TTL deadline passed while it waited is dropped as
   * expired — counted in `expiredMessages`, never delivered — mirroring
   * the drain-time expiry rule for queued messages.
   *
   * Every entry that leaves the pending set also gets a tombstone in the
   * durable log (when enabled): without one, a restart could rebuild —
   * and recount — a schedule that already resolved. The tombstone is
   * redundant when the fan-out wrote a delivery record (which also closes
   * the schedule via its `delayId`), and essential when it did not (rate-
   * limit shed, expiry drop).
   *
   * `trailingFlush` schedules a flush when anything fanned out; pass
   * `false` when the sweep already runs inside a flush — the drain below
   * it picks the messages up.
   */
  private sweepDelayed(nowMs: number = this.now(), trailingFlush = true): void {
    let fannedOut = false;
    for (;;) {
      const top = this.delayHeap.peek();
      if (top == null) break;
      if (top.cancelled) {
        // Lazy deletion surfacing: already dropped from `delayedById` by
        // `cancelDelayed`; just discard the heap node.
        this.delayHeap.pop();
        continue;
      }
      if (top.deliverAt > nowMs) break;
      this.delayHeap.pop();
      this.delayedById.delete(top.id);
      if (top.expiresAt !== undefined && nowMs >= top.expiresAt) {
        this.recordExpired(top.topic);
        this.appendDelayTombstone(top.id, top.topic);
        continue;
      }
      // `preAdmitted`: schema already ran once at schedule time
      // (fail-fast); validators are expected pure.
      this.fanOut(top.topic, top.payload, true, {
        delayId: top.id,
        deliverAt: top.deliverAt,
        expiresAt: top.expiresAt,
        key: top.key,
      });
      this.appendDelayTombstone(top.id, top.topic);
      fannedOut = true;
    }
    this.armDelayTimer();
    if (fannedOut && trailingFlush) this.scheduleFlush();
  }

  /**
   * Arms the wall-clock timer for the heap's next due time. Cancelled heap
   * heads are discarded first so the timer tracks the next real deadline.
   * The timer never keeps the process alive on its own (unref'd). When it
   * fires, the sweep re-arms it — so a bus clock that has not advanced
   * (e.g. a frozen injected clock in tests) cannot spin the loop: the
   * sweep simply finds nothing due and re-arms for the same deadline.
   */
  private armDelayTimer(): void {
    if (this.delayTimer !== undefined) {
      clearTimeout(this.delayTimer);
      this.delayTimer = undefined;
    }
    for (;;) {
      const top = this.delayHeap.peek();
      if (top == null || !top.cancelled) break;
      this.delayHeap.pop();
    }
    const next = this.delayHeap.peek();
    if (next == null) return;
    const delayMs = Math.max(0, next.deliverAt - this.now());
    // `setTimeout` saturates past 2^31-1 ms (~24.8 days); longer delays
    // re-arm on fire.
    const timer = setTimeout(() => {
      this.delayTimer = undefined;
      this.sweepDelayed();
    }, Math.min(delayMs, 2_147_483_647));
    // A pending delayed message must not keep the process alive on its own.
    const handle = timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    this.delayTimer = timer;
  }

  /**
   * Writes a tombstone closing a delayed-delivery schedule (cancelled,
   * expired while delayed, or shed at fan-out) to the durable log, when
   * one is enabled. Restart recovery treats a tombstoned `delayId` as
   * resolved and never rebuilds it. Best-effort like all durable-log
   * writes: a failed append never throws into the caller.
   */
  private appendDelayTombstone(delayId: string, topic: string): void {
    this.durableLog?.append({ seq: 0, topic, at: this.now(), delayId, cancelled: true, payload: null });
  }

  /**
   * Rebuilds pending delayed-delivery timers from the durable log after a
   * restart. A schedule record (seq 0, `delayId` + `deliverAt`) becomes a
   * live timer unless a later record closed it — a delivery record or a
   * tombstone carrying the same `delayId`. Rebuilt entries keep their
   * original id, so `cancelDelayed` handles handed out before the restart
   * keep working afterwards.
   *
   * Entries already due (the process was down past their `deliverAt`) fan
   * out immediately via the trailing sweep; entries whose TTL expired
   * while the process was down are dropped as expired (counted once — a
   * tombstone closes the schedule so a second restart does not recount).
   */
  private recoverDelayed(): void {
    const log = this.durableLog;
    if (log == null) return;
    const nowMs = this.now();
    const scheduled = new Map<string, { topic: string; deliverAt: number; expiresAt?: number; payload: unknown; key?: string }>();
    const closed = new Set<string>();
    // `readSince(topic, -1)`: the exclusive bound sits below every real
    // seq, so seq-0 schedule records and tombstones come along too.
    for (const topic of log.topics()) {
      for (const rec of log.readSince(topic, -1)) {
        if (rec.delayId == null) continue;
        if (rec.cancelled === true || rec.seq >= 1) {
          closed.add(rec.delayId);
          continue;
        }
        if (!closed.has(rec.delayId) && !scheduled.has(rec.delayId)) {
          scheduled.set(rec.delayId, {
            topic: rec.topic,
            // A well-formed schedule record always carries `deliverAt`
            // (the log parser rejects it otherwise).
            deliverAt: rec.deliverAt as number,
            expiresAt: rec.expiresAt,
            payload: rec.payload,
            key: rec.key,
          });
        }
      }
    }
    for (const [delayId, rec] of scheduled) {
      if (closed.has(delayId)) continue;
      if (rec.expiresAt !== undefined && nowMs >= rec.expiresAt) {
        this.recordExpired(rec.topic);
        this.appendDelayTombstone(delayId, rec.topic);
        continue;
      }
      const entry: DelayedEntry = {
        id: delayId,
        topic: rec.topic,
        payload: rec.payload,
        deliverAt: rec.deliverAt,
        expiresAt: rec.expiresAt,
        cancelled: false,
        key: rec.key,
      };
      this.delayedById.set(delayId, entry);
      this.delayHeap.push(entry);
      // Keep generated ids ahead of recovered ones — `publishDelayed`
      // must never reuse an id the previous process handed out.
      const match = /^delayed-(\d+)$/.exec(delayId);
      if (match != null) {
        this.nextDelayedId = Math.max(this.nextDelayedId, parseInt(match[1], 10) + 1);
      }
    }
    this.sweepDelayed(nowMs);
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
   * Returns the topic's stats entry, creating it zeroed on first use.
   */
  private statsFor(topic: string): ReturnType<typeof zeroedTopicStats> {
    let stats = this.topicStats.get(topic);
    if (stats == null) {
      stats = zeroedTopicStats();
      this.topicStats.set(topic, stats);
    }
    return stats;
  }

  /**
   * Counts one publish rejected by schema validation against the topic and
   * the global total. A rejection happens before admission: the payload
   * never becomes a message — no sequence number is consumed (subscribers
   * see no gap), the durable log never sees it, and it does not burn
   * rate-limit budget.
   */
  private countSchemaRejection(topic: string): void {
    this.statsFor(topic).rejectedMessages += 1;
    this.totalRejected += 1;
  }

  /**
   * Counts one publish suppressed as an idempotency duplicate against the
   * topic and the global total. A duplicate is dropped before admission, so
   * — like a schema rejection — it creates the topic's stats entry when the
   * topic has never published anything yet.
   */
  private countDuplicate(topic: string): void {
    this.statsFor(topic).duplicateMessages += 1;
    this.totalDuplicates += 1;
  }

  /**
   * Drops expired dedup entries from the head of the insertion-ordered
   * table. Entries are inserted in publish order, so with a sane clock the
   * head is the oldest and the scan stops at the first live entry —
   * amortized O(1) per idempotent publish. An injected clock that jumps
   * backwards can leave an expired entry behind the head; the per-key
   * expiry re-check in `publishIdempotent` keeps that harmless.
   */
  private pruneDedup(nowMs: number): void {
    for (const [key, firstAt] of this.dedup) {
      if (nowMs - firstAt < this.idempotencyWindowMs) break;
      this.dedup.delete(key);
    }
  }

  /**
   * Pushes one message into a subscriber's queue and counts bus-level
   * backpressure drops. Every queue insertion in the bus funnels through
   * here — fan-out, redeliveries, log replays, DLQ replays, and mid-drain
   * requeues. Drops are counted by diffing the queue's `droppedCount`
   * rather than by the push result: under `drop-oldest` the incoming item
   * is accepted while an older entry is shed, so the return value alone
   * misses those evictions.
   */
  private enqueueMessage(
    subscriber: Subscriber,
    msg: BusMessage,
    deadline: number | undefined,
  ): 'accepted' | 'dropped' {
    const queue = subscriber.queue;
    const droppedBefore = queue.droppedCount;
    const result = queue.push(msg, 0, deadline);
    this.totalDropped += queue.droppedCount - droppedBefore;
    const latency = subscriber.latency;
    if (latency != null && result === 'accepted') {
      // Stamp the enqueue time for the delivery-latency tracker (see
      // `SubscribeOptions.deliveryLatency`). Only tracked subscribers pay
      // for the clock read; a dropped message never reaches a handler, so
      // it is never stamped. Requeues (nack redelivery, health requeue)
      // re-stamp: each delivery samples its own queue dwell.
      latency.enqueuedAt.set(msg, this.now());
    }
    return result;
  }

  /**
   * Records one enqueue→delivery latency sample for a tracked subscriber,
   * at the moment a message is handed to its handler. The sample measures
   * queue dwell (enqueue to hand-off), never handler processing time.
   * `nowMs` is the flush's single clock reading, so every delivery in one
   * flush round shares the same delivery timestamp.
   */
  private recordDeliveryLatency(subscriber: Subscriber, msg: BusMessage, nowMs: number): void {
    const latency = subscriber.latency;
    if (latency == null) return;
    const enqueuedAt = latency.enqueuedAt.get(msg);
    latency.enqueuedAt.delete(msg);
    if (enqueuedAt === undefined) return;
    latency.tracker.record(nowMs - enqueuedAt);
  }

  /**
   * Pushes one message into a subscriber's queue, honoring the
   * subscriber's content filter and adaptive publish-side throttling.
   * Returns true when the queue accepted the message. Shared by the plain
   * fan-out path and the consumer-group assignment path so both get
   * identical backpressure semantics.
   *
   * `rawPayload` is the application payload as published — never a
   * compressed wire envelope — so the content filter always judges the
   * real payload, exactly like a schema validator does.
   */
  private deliverToSubscriber(
    subscriber: Subscriber,
    msg: BusMessage,
    expiresAt: number | undefined,
    rawPayload: unknown,
  ): boolean {
    const filter = subscriber.filter;
    if (filter !== undefined && !filter(rawPayload, msg.topic)) {
      // Content-filtered: the message never reaches the queue — no
      // backpressure budget consumed, no throttle token burned — and it
      // must not surface as a sequence gap, so the per-topic baseline
      // advances over it (seen, deliberately skipped). A throwing filter
      // propagates to the publish call, like a throwing schema validator.
      this.countFiltered(subscriber, msg.topic, msg.seq);
      return false;
    }
    const throttle = subscriber.throttle;
    if (throttle != null && throttle.throttled && !throttle.bucket.take()) {
      // Publish-side shed: the message never reaches the queue, so the
      // drop policy never churns on it. Counted separately from queue
      // drops; sequence-gap detection surfaces the loss downstream.
      throttle.throttledDrops += 1;
      this.totalThrottled += 1;
      return false;
    }
    return this.enqueueMessage(subscriber, msg, expiresAt) === 'accepted';
  }

  /**
   * Counts one message skipped by a subscriber's content filter (see
   * `SubscribeOptions.filter`) against the topic and the global total,
   * and advances the subscriber's per-topic sequence baseline over it so
   * the deliberately skipped message never counts as a gap.
   *
   * Sampling caveat, shared with every sampling-based gap detector: a
   * message lost (dropped/expired) while every later message on the topic
   * is filtered stays invisible, because the baseline can only move on
   * deliveries and filter decisions.
   */
  private countFiltered(subscriber: Subscriber, topic: string, seq: number): void {
    const last = subscriber.lastDeliveredSeq.get(topic);
    if (last === undefined || seq > last) subscriber.lastDeliveredSeq.set(topic, seq);
    this.statsFor(topic).filteredMessages += 1;
    this.totalFiltered += 1;
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
   *
   * `preAdmitted` is set only by `publishAtomic`, after the batch passed
   * `admissionVerdict`: the schema gate is skipped because validation
   * already ran once per message (validators are expected pure), so the
   * commit phase cannot reject on schema. The rate-limit `take()` still
   * runs and is guaranteed to succeed — the shadow budget ensured every
   * take in this synchronous turn has a token (the bus clock cannot move
   * backwards within the turn, and lazy refill can only add tokens).
   *
   * `delayed` is set only by the delayed-delivery sweep (`sweepDelayed`)
   * for a message whose due time arrived: schema validation already ran
   * once at schedule time (fail-fast, see `publishDelayed`), so it is
   * skipped here for the same purity reason — but the per-topic rate-limit
   * budget IS burned here, at fan-out time, and may shed the message like
   * any other publish. The TTL deadline is the one stamped at schedule
   * time (a rule added in between does not retroactively expire the
   * message), and the durable-log record carries `deliverAt`/`delayId` so
   * restart recovery can tell it apart from a still-pending schedule.
   *
   * `key` is the durable-log compaction key for direct publishes (see
   * `PublishOptions.key`); delayed messages carry theirs on `delayed`
   * instead. The key is stamped onto the delivery's durable-log record.
   */
  private fanOut(
    topic: string,
    payload: unknown,
    preAdmitted = false,
    delayed?: DelayedFanOut,
    key?: string,
  ): { matched: number; accepted: number; admitted: boolean } {
    // Publish-side schema validation runs before admission: a rejected
    // payload never becomes a message — no sequence number is consumed
    // (subscribers see no gap), the durable log never sees it, and it
    // does not burn rate-limit budget. Rejection is counted on the
    // topic's stats entry, which is created here when the topic has never
    // published anything valid yet.
    if (!preAdmitted) {
      const validator = this.schemaForTopic(topic);
      if (validator !== undefined && !validator(payload, topic)) {
        this.countSchemaRejection(topic);
        return { matched: 0, accepted: 0, admitted: false };
      }
    }
    const msg: BusMessage = { topic, payload, seq: this.nextSeq(topic) };
    const stats = this.statsFor(topic);
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
        return { matched: 0, accepted: 0, admitted: false };
      }
    }
    // Accepted-for-publish sampling (EB-34): the message cleared the
    // admission gates (schema validation, rate-limit budget), so it counts
    // as publish traffic in the per-topic sliding-window rate table —
    // rejected and shed messages never reach this line and never pollute
    // the rates. The timestamp comes from the bus's injected clock (one
    // extra clock read per publish; the publish path stays
    // allocation-free). Delayed messages fan out through here at their due
    // time, so their sample lands at actual fan-out — not at schedule
    // time — keeping the table aligned with real publish load rather than
    // scheduled intent.
    this.publishRates.sample(topic, this.now());
    // Publish-side per-topic payload compression (opt-in via
    // `setTopicCompression`). Pipeline order, and why:
    //   1. schema validation ran first and always sees the RAW payload —
    //      validators are written against the application payload shape,
    //      so validating the compressed envelope would break every
    //      existing validator.
    //   2. rate limiting ran before compression: a shed message is dropped
    //      without ever paying deflate CPU, and compression metrics only
    //      count admitted messages.
    //   3. what is persisted to the durable log and fanned out to queues is
    //      the COMPRESSED bytes (envelope); subscribers inflate
    //      transparently just before delivery. Compression consumes no
    //      sequence number and moves no TTL deadline — seq/TTL semantics
    //      are unchanged.
    const compressionRule = this.compressionForTopic(topic);
    // SHA-256 id of the preset dictionary that compressed this message, for
    // the durable-log record: replay resolves the bytes from the bus's
    // dictionary registry (set by `setTopicCompression`).
    let compressedDictId: string | undefined;
    if (compressionRule !== undefined) {
      const wirePayload = this.compressPayload(msg, payload, compressionRule, stats);
      if (wirePayload !== payload) {
        msg.payload = wirePayload;
        if (this.compressedDictionaries.has(msg)) compressedDictId = compressionRule.dictionaryId;
      }
    }
    let matched = 0;
    let accepted = 0;
    // One clock reading for the publish: TTL deadline and log timestamp stay
    // consistent even if the injected clock moves between the two.
    const nowMs = this.now();
    const ttlMs = this.ttlForTopic(topic);
    // A delayed fan-out keeps the TTL deadline stamped at schedule time —
    // it is "published" then and only delivered later. Note the explicit
    // `delayed !== undefined` check instead of `??`: an explicit "no
    // deadline" from schedule time must not fall through to the rules in
    // effect now (a rule added in between does not retroactively expire
    // the message).
    const expiresAt =
      delayed !== undefined ? delayed.expiresAt : ttlMs === undefined ? undefined : nowMs + ttlMs;
    if (expiresAt !== undefined) this.messageDeadlines.set(msg, expiresAt);
    // Durable log (opt-in): persist the stamped message before fan-out, so a
    // crash between publish and delivery still leaves it replayable. Logging
    // never throws into the publish path — see `DurableTopicLog.append`.
    // The log stores the wire payload: when compression is on, the deflated
    // bytes are what hit the disk, and resume replays the envelope —
    // `replayLog` re-registers those messages for transparent inflation at
    // delivery, exactly like live ones.
    // A delayed delivery's record additionally carries its schedule
    // identity (`deliverAt`/`delayId`): restart recovery tells a fulfilled
    // schedule (delivery record present) from a still-pending one
    // (schedule record only). A compaction `key` — from the direct publish
    // options or carried over from the delayed schedule — rides on the
    // delivery record.
    const recordKey = key ?? delayed?.key;
    this.durableLog?.append({
      seq: msg.seq,
      topic,
      at: nowMs,
      expiresAt,
      payload: msg.payload,
      ...(delayed == null ? {} : { deliverAt: delayed.deliverAt, delayId: delayed.delayId }),
      ...(recordKey === undefined ? {} : { key: recordKey }),
      // Only present when a preset dictionary compressed this message:
      // replay needs the same bytes to inflate it.
      ...(compressedDictId === undefined ? {} : { dictId: compressedDictId }),
    });
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
        if (this.deliverToSubscriber(subscriber, msg, expiresAt, payload)) accepted += 1;
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
      if (this.deliverToSubscriber(assignee, msg, expiresAt, payload)) accepted += 1;
      this.recordGroupOffset(hit.groupId, topic, msg.seq);
    }
    stats.subscriberCount = matched;
    return { matched, accepted, admitted: true };
  }

  /**
   * Returns a point-in-time snapshot of live bus metrics: active
   * subscriptions (total and per pattern) plus per-topic fan-out widths and
   * publish counts. The snapshot is a plain-data copy — mutating it does
   * not affect the bus.
   */
  getStats(): BusStats {
    // One clock reading for the rate table: every per-topic window and the
    // hot-topics ranking in this snapshot share the same "now".
    const ratesNow = this.now();
    let unackedDeliveries = 0;
    let throttledSubscribers = 0;
    let degradedSubscribers = 0;
    let shapedSubscribers = 0;
    const deliveryLatency: BusStats['deliveryLatency'] = [];
    for (const subscriber of this.subscribers.values()) {
      unackedDeliveries += subscriber.reliable?.tracker.unackedCount ?? 0;
      if (subscriber.throttle?.throttled === true) throttledSubscribers += 1;
      if (subscriber.health?.degraded === true) degradedSubscribers += 1;
      if (subscriber.deliveryShaping?.shaping === true) shapedSubscribers += 1;
      const latency = subscriber.latency;
      if (latency != null) {
        deliveryLatency.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          ...latency.tracker.summary(),
        });
      }
    }
    const slowestSubscribers = deliveryLatency
      .filter((s) => s.samples > 0)
      .sort((a, b) => b.p99Ms - a.p99Ms)
      .slice(0, 5)
      .map((s) => ({
        subscriberId: s.subscriberId,
        pattern: s.pattern,
        p99Ms: s.p99Ms,
        samples: s.samples,
      }));
    return {
      totalSubscribers: this.subscribers.size,
      subscribersByPattern: Object.fromEntries(this.subscribersByPattern),
      totalPublished: this.totalPublished,
      deliveredMessages: this.totalDelivered,
      droppedMessages: this.totalDropped,
      throttledMessages: this.totalThrottled,
      expiredMessages: this.totalExpired,
      unackedDeliveries,
      patternCacheSize: this.patternCache.size,
      indexSize: this.prefixIndex.size,
      sequenceGaps: this.totalSequenceGaps,
      rateLimitedMessages: this.totalRateLimited,
      rejectedMessages: this.totalRejected,
      duplicateMessages: this.totalDuplicates,
      filteredMessages: this.totalFiltered,
      deadLetteredMessages: this.totalDeadLettered,
      compressedMessages: this.totalCompressed,
      compressedBytesBefore: this.totalCompressedBytesBefore,
      compressedBytesAfter: this.totalCompressedBytesAfter,
      compressionRatio:
        this.totalCompressedBytesBefore > 0
          ? this.totalCompressedBytesAfter / this.totalCompressedBytesBefore
          : 0,
      meanCompressionMs:
        this.totalCompressed > 0 ? this.totalCompressionTimeMs / this.totalCompressed : 0,
      throttledSubscribers,
      degradedSubscribers,
      shapedSubscribers,
      pendingDelayed: this.delayedById.size,
      deliveryLatency,
      slowestSubscribers,
      hotTopics: this.publishRates.hotTopics(ratesNow),
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
        duplicateMessages: stats.duplicateMessages,
        filteredMessages: stats.filteredMessages,
        compressedMessages: stats.compressedMessages,
        compressedBytesBefore: stats.compressedBytesBefore,
        compressedBytesAfter: stats.compressedBytesAfter,
        compressionRatio:
          stats.compressedBytesBefore > 0
            ? stats.compressedBytesAfter / stats.compressedBytesBefore
            : 0,
        meanCompressionMs:
          stats.compressedMessages > 0 ? stats.compressionTimeMs / stats.compressedMessages : 0,
        rates: this.publishRates.ratesFor(topic, ratesNow),
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
   * Moves a message that exhausted its redelivery budget into the
   * subscriber's dead-letter queue (see
   * `ReliableSubscribeOptions.deadLetter`). The original message object
   * is kept — replay requeues the identical object, so its per-topic
   * `seq` and TTL deadline survive the round trip. The DLQ is bounded by
   * `maxEntries`: the oldest entry is evicted when full.
   */
  private moveToDeadLetter(
    subscriber: Subscriber,
    msg: BusMessage,
    redeliveries: number,
  ): void {
    const dlq = subscriber.deadLetter;
    if (dlq == null) return;
    let evictedOldest = false;
    if (dlq.entries.length >= dlq.maxEntries) {
      dlq.entries.shift();
      evictedOldest = true;
    }
    const entry: DeadLetterEntry = {
      seq: ++dlq.nextSeq,
      topic: msg.topic,
      payload: msg.payload,
      redeliveries,
      deadLetteredAt: this.now(),
      expiresAt: this.messageDeadlines.get(msg),
    };
    dlq.entries.push({ ...entry, msg });
    this.totalDeadLettered += 1;
    dlq.onDeadLetter?.({
      subscriberId: subscriber.id,
      pattern: subscriber.pattern,
      entry,
      evictedOldest,
    });
  }

  /**
   * The messages currently sitting in a reliable subscriber's
   * dead-letter queue, in dead-lettering order (see
   * `ReliableSubscribeOptions.deadLetter`). Empty when the subscriber
   * has no DLQ configured or nothing has been dead-lettered. Throws on
   * an unknown subscriber id. The returned array and entry envelopes are
   * snapshots — mutating them does not affect the bus; `payload` is the
   * original published object, shared by reference (see
   * `DeadLetterEntry.payload`).
   */
  getDeadLetterMessages(subId: string): DeadLetterEntry[] {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return (subscriber.deadLetter?.entries ?? []).map(
      ({ msg: _msg, ...entry }) => ({ ...entry }),
    );
  }

  /**
   * Hands one dead-lettered message back to the subscriber's queue for
   * another delivery attempt (see
   * `ReliableSubscribeOptions.deadLetter`). The message keeps its
   * original per-topic `seq` (no false sequence gap) and its original
   * TTL deadline — a message whose deadline already passed is dropped as
   * expired on the next drain, not resurrected. The replayed message
   * gets a fresh redelivery budget: if it fails again it is requeued up
   * to `maxRedeliveries` times before returning to the DLQ.
   *
   * Returns true when the entry was found and requeued, false when the
   * subscriber has no DLQ or no entry with that `seq`. Throws on an
   * unknown subscriber id.
   */
  replayDeadLetter(subId: string, seq: number): boolean {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    const dlq = subscriber.deadLetter;
    if (dlq == null) return false;
    const idx = dlq.entries.findIndex((e) => e.seq === seq);
    if (idx === -1) return false;
    const [record] = dlq.entries.splice(idx, 1);
    // Fresh retry budget: the operator's replay is a new chance, not a
    // continuation of the poison run.
    subscriber.reliable?.redeliveries.delete(record.msg);
    this.enqueueMessage(subscriber, record.msg, this.messageDeadlines.get(record.msg));
    this.scheduleFlush();
    return true;
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
      // Delayed deliveries due by now fan out first, so one flush carries
      // both — and advancing an injected clock past a `deliverAt` and then
      // publishing anything delivers deterministically in tests. No
      // trailing flush: the drain below picks the fanned-out messages up.
      this.sweepDelayed(nowMs, false);
      for (const subscriber of this.subscribers.values()) {
        this.drainSubscriber(subscriber, nowMs);
      }
    });
  }

  /**
   * Drains one subscriber's queue and delivers the dequeued messages,
   * honoring delivery-side rate shaping when enabled. A shaped subscriber
   * dequeues at most what its token budget allows this round (whole tokens
   * only — a fractional token delivers nothing); the remainder stays queued
   * in FIFO order for a later flush, and a re-flush timer is armed so the
   * backlog drains even when no new publishes arrive.
   */
  private drainSubscriber(subscriber: Subscriber, nowMs: number): void {
    // A degraded subscriber's deliveries are paused: its backlog stays
    // queued under the normal backpressure policy, untouched by the
    // drain, so resuming picks up exactly where delivery paused.
    if (subscriber.health?.degraded === true) return;
    // Batched subscribers collect messages across flush rounds instead of
    // delivering one message per handler call (see drainBatchedSubscriber).
    if (subscriber.batch != null) {
      this.drainBatchedSubscriber(subscriber, nowMs);
      return;
    }
    const shaping = subscriber.deliveryShaping;
    const maxLive = shaping == null ? Infinity : Math.floor(shaping.bucket.availableTokens);
    const { live, expired } = subscriber.queue.drainLiveUpTo(nowMs, maxLive);
    for (const msg of expired) {
      this.recordExpired(msg.topic);
    }
    const health = subscriber.health;
    if (health == null) {
      for (const msg of live) {
        // Guaranteed to succeed: the drain above dequeued at most
        // floor(availableTokens), and nothing else consumes this
        // subscriber's bucket in between.
        shaping?.bucket.take();
        // Compressed payloads are inflated here — once per message, before
        // any handler sees it — so every subscriber transparently receives
        // the original payload. See `inflateMessagePayload` for the
        // idempotency and envelope-collision notes.
        this.inflateMessagePayload(msg);
        this.detectGap(subscriber, msg);
        this.totalDelivered += 1;
        // Sampled before the handler runs: queue dwell, not processing time.
        this.recordDeliveryLatency(subscriber, msg, nowMs);
        subscriber.handler(msg);
      }
    } else {
      for (let i = 0; i < live.length; i += 1) {
        shaping?.bucket.take();
        this.inflateMessagePayload(live[i]);
        this.deliverWithHealth(subscriber, live[i], nowMs);
        if (!health.degraded) continue;
        // The threshold tripped mid-drain: everything not yet attempted
        // goes back to the queue, in FIFO order (see requeueUndelivered).
        this.requeueUndelivered(subscriber, live.slice(i + 1));
        this.reportDegraded(subscriber);
        break;
      }
    }
    if (shaping != null) {
      // Expired entries were discarded above, so a non-empty queue means
      // shaping is actively holding this subscriber back for lack of
      // budget — never because of TTL.
      shaping.shaping = subscriber.queue.size > 0;
      if (shaping.shaping) {
        this.armShapingTimer(subscriber);
      } else {
        this.clearShapingTimer(subscriber);
      }
    }
  }

  /**
   * Drain path for batched subscribers (see `SubscribeOptions.batch`):
   * instead of handing each message to the handler, the drain collects
   * queued messages into the subscriber's pending batch. A batch that
   * reaches `maxSize` is delivered immediately; a partial batch lingers
   * for up to `maxWaitMs` so a burst that is still arriving fills it
   * before the handler is invoked.
   *
   * With `deliveryShaping` the collection is bounded by the shaping
   * budget — a batch never delivers more than the budget allows in one
   * round, and the delivered batch consumes one token per message.
   */
  private drainBatchedSubscriber(subscriber: Subscriber, nowMs: number): void {
    const batch = subscriber.batch;
    if (batch == null) return;
    const shaping = subscriber.deliveryShaping;
    const budget = shaping == null ? Infinity : Math.floor(shaping.bucket.availableTokens);
    if (budget >= 1) {
      const room = batch.maxSize - batch.pending.length;
      if (room > 0) {
        const { live, expired } = subscriber.queue.drainLiveUpTo(nowMs, Math.min(room, budget));
        for (const msg of expired) {
          this.recordExpired(msg.topic);
        }
        batch.pending.push(...live);
      }
    } else if (shaping != null) {
      // No shaping budget this round: hold everything back, exactly like
      // the plain path — the pending batch counts as backlog too.
      shaping.shaping = subscriber.queue.size > 0 || batch.pending.length > 0;
      if (shaping.shaping) {
        this.armShapingTimer(subscriber);
      } else {
        this.clearShapingTimer(subscriber);
      }
      return;
    }
    if (batch.pending.length >= batch.maxSize) {
      this.deliverBatch(subscriber, nowMs);
    } else if (batch.pending.length > 0) {
      this.armBatchTimer(subscriber);
    } else {
      this.clearBatchTimer(subscriber);
    }
  }

  /**
   * Hands the subscriber's pending batch to its handler as a single
   * array call. Messages that expired while the batch was filling are
   * dropped as expired at hand-off, not resurrected; an all-expired batch
   * never invokes the handler. Per-message bookkeeping (payload
   * inflation, gap detection, delivery counting, latency sampling) runs
   * exactly as for single deliveries, then the batch goes through the
   * one-call health accounting when a probe is configured.
   *
   * After delivering, a still-non-empty queue schedules another flush so
   * batching continues until the backlog is drained.
   */
  private deliverBatch(subscriber: Subscriber, nowMs: number): void {
    const batch = subscriber.batch;
    if (batch == null) return;
    this.clearBatchTimer(subscriber);
    const pending = batch.pending;
    batch.pending = [];
    if (pending.length === 0) return;
    const live: BusMessage[] = [];
    for (const msg of pending) {
      const deadline = this.messageDeadlines.get(msg);
      if (deadline !== undefined && nowMs >= deadline) {
        this.recordExpired(msg.topic);
      } else {
        live.push(msg);
      }
    }
    if (live.length === 0) return;
    // The whole batch occupies shaping budget: one token per message,
    // mirroring the plain path's one take per delivered message.
    const shaping = subscriber.deliveryShaping;
    if (shaping != null) {
      for (let i = 0; i < live.length; i += 1) shaping.bucket.take();
    }
    for (const msg of live) {
      this.inflateMessagePayload(msg);
      this.detectGap(subscriber, msg);
      this.totalDelivered += 1;
      // Sampled before the handler runs: queue dwell, not processing time.
      this.recordDeliveryLatency(subscriber, msg, nowMs);
    }
    if (subscriber.health == null) {
      subscriber.handler(live as unknown as BusMessage);
    } else {
      this.deliverBatchWithHealth(subscriber, live, nowMs);
    }
    if (subscriber.queue.size > 0) {
      this.scheduleFlush();
    }
  }

  /**
   * Health-probe accounting for one batched handler invocation: the batch
   * is a single delivery — a throw (or a processing-timeout overrun)
   * counts as one failure, a clean return resets the consecutive-failure
   * counter. Like the single-message path, a failed plain batch is
   * consumed; reliable subscriptions requeue through the ACK wrapper's
   * nack path before this accounting runs.
   */
  private deliverBatchWithHealth(subscriber: Subscriber, batch: BusMessage[], nowMs: number): void {
    const health = subscriber.health;
    if (health == null) return;
    const budgetMs = health.processingTimeoutMs;
    const startedAtMs = budgetMs !== undefined ? this.now() : 0;
    let failed = false;
    let reason: 'error' | 'timeout' = 'error';
    try {
      subscriber.handler(batch as unknown as BusMessage);
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
      this.reportDegraded(subscriber);
    }
  }

  /**
   * Arms the linger timer for a partial batch: if no flush fills the
   * batch within `maxWaitMs`, the timer delivers whatever was collected.
   * The timer never keeps the process alive on its own, and unsubscribe
   * clears it (see `clearBatchTimer`).
   */
  private armBatchTimer(subscriber: Subscriber): void {
    const batch = subscriber.batch;
    if (batch == null || batch.timer !== undefined) return;
    const timer = setTimeout(() => {
      batch.timer = undefined;
      // The subscriber may have unsubscribed while the timer was armed —
      // only a still-registered subscriber delivers here.
      if (this.subscribers.get(subscriber.id) !== subscriber) return;
      if (batch.pending.length === 0) return;
      this.deliverBatch(subscriber, this.now());
    }, batch.maxWaitMs);
    const handle = timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    batch.timer = timer;
  }

  /**
   * Cancels a batched subscriber's pending linger timer, if any.
   */
  private clearBatchTimer(subscriber: Subscriber): void {
    const batch = subscriber.batch;
    if (batch?.timer !== undefined) {
      clearTimeout(batch.timer);
      batch.timer = undefined;
    }
  }

  /**
   * Returns messages a drain dequeued but never delivered (health probing
   * tripped mid-drain) to the subscriber's queue, preserving FIFO order.
   * When the drain emptied the queue the messages are simply appended back;
   * when a bounded (shaped) drain left entries queued, the queue is rebuilt
   * as [undelivered..., still-queued...] — the total never exceeds the
   * pre-drain size, so the requeue cannot drop.
   */
  private requeueUndelivered(subscriber: Subscriber, undelivered: BusMessage[]): void {
    if (undelivered.length === 0) return;
    const queue = subscriber.queue;
    if (queue.size === 0) {
      for (const msg of undelivered) this.enqueueMessage(subscriber, msg, this.messageDeadlines.get(msg));
      return;
    }
    const rest = queue.drain();
    for (const msg of undelivered) this.enqueueMessage(subscriber, msg, this.messageDeadlines.get(msg));
    for (const msg of rest) this.enqueueMessage(subscriber, msg, this.messageDeadlines.get(msg));
  }

  /**
   * Arms the re-flush timer for a shaped subscriber that is holding
   * messages back: when the token bucket refills, the timer triggers
   * another flush so the backlog drains even when no new publishes arrive.
   * The delay is derived from the bus clock (consistent with the bucket);
   * the timer itself is a real wall-clock timeout — like the health
   * probe's auto-resume timer — and never keeps the process alive on its
   * own. Firing only schedules a flush when the subscriber is still shaping
   * and the bucket actually has budget; otherwise it re-arms, so a clock
   * that has not advanced cannot spin the flush loop.
   */
  private armShapingTimer(subscriber: Subscriber): void {
    const shaping = subscriber.deliveryShaping;
    if (shaping == null || shaping.timer !== undefined) return;
    const delayMs = Math.max(0, Math.ceil(shaping.bucket.msUntilNextToken()));
    const timer = setTimeout(() => {
      shaping.timer = undefined;
      if (!shaping.shaping || this.subscribers.get(subscriber.id) !== subscriber) return;
      if (shaping.bucket.availableTokens < 1) {
        this.armShapingTimer(subscriber);
        return;
      }
      this.scheduleFlush();
    }, delayMs);
    // A shaped backlog must not keep the process alive on its own.
    const handle = timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    shaping.timer = timer;
  }

  /**
   * Cancels a shaped subscriber's pending re-flush timer, if any.
   */
  private clearShapingTimer(subscriber: Subscriber): void {
    const shaping = subscriber.deliveryShaping;
    if (shaping?.timer !== undefined) {
      clearTimeout(shaping.timer);
      shaping.timer = undefined;
    }
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
  private deliverWithHealth(subscriber: Subscriber, msg: BusMessage, nowMs: number): void {
    const health = subscriber.health;
    if (health == null) return;
    this.detectGap(subscriber, msg);
    const budgetMs = health.processingTimeoutMs;
    const startedAtMs = budgetMs !== undefined ? this.now() : 0;
    let failed = false;
    let reason: 'error' | 'timeout' = 'error';
    this.totalDelivered += 1;
    // Sampled before the handler runs: queue dwell, not processing time.
    this.recordDeliveryLatency(subscriber, msg, nowMs);
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
