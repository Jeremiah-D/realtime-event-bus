import { BoundedQueue, type DropPolicy } from './backpressure.ts';
import { AckTracker, type Delivery } from './ack.ts';
import { TokenBucket } from './throttle.ts';
import { SlidingWindowLimiter } from './ratewindow.ts';
import {
  DeliveryLatencyTracker,
  type DeliveryLatencyOptions,
  type DeliveryLatencySummaryStats,
  type LatencySloMissEvent,
  type ProcessingLatencySloOptions,
} from './latency.ts';
import {
  AckLatencyTracker,
  type AckLatencyOptions,
  type AckLatencySummaryStats,
  type AckSloMissEvent,
} from './acklatency.ts';
import {
  LagTracker,
  resolveLagMonitorOptions,
  type LagEvent,
  type LagMonitorOptions,
  type LagSummaryStats,
  type SubscriberLagState,
} from './lag.ts';
import {
  HOT_KEYS_LIMIT,
  resolveKeyHotspotOptions,
  type HotKeyStat,
  type KeyHotspotOptions,
  type SubscriberKeyHotspotState,
} from './keyhotspot.ts';
import {
  GroupLagMonitor,
  resolveGroupLagOptions,
  type GroupLagStat,
} from './grouplag.ts';
import { DurableTopicLog, type SegmentRotationOptions } from './durablelog.ts';
import {
  NamespaceDeniedError,
  NamespaceHandle,
  NamespaceNotEmptyError,
  resolveNamespaceOptions,
  validateNamespacePrefix,
  type NamespaceInfo,
  type NamespaceOptions,
  type NamespaceStats,
} from './namespace.ts';
import { DelayHeap, type DelayedEntry } from './delayed.ts';
import { stickyPartitionAssignment } from './sticky.ts';
import { PublishRateTable, type HotTopic, type TopicRates } from './rates.ts';
import { deflateSync, inflateSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import { Buffer } from 'node:buffer';
import {
  TraceRecorder,
  formatTraceparent,
  newSpanId,
  resolveTraceOptions,
  validateTraceparent,
  type TraceOptions,
  type TraceSpan,
} from './trace.ts';
import type {
  ClusterConnectOptions,
  ClusterLink,
  ClusterLinkStatus,
  ClusterMessage,
} from './cluster.ts';
import {
  resolveBridgeOptions,
  validateBridgeEnvelope,
  type BridgeEnvelope,
  type BridgeReceiveResult,
  type ResolvedBridgeOptions,
} from './bridge.ts';
import {
  EventTimeWatermark,
  validateAllowedLatenessMs,
  validateEventTime,
  type LateMessageCallback,
  type LateMessageEvent,
} from './watermark.ts';

export type { Delivery, RedeliveryReason } from './ack.ts';
export type {
  BridgeEnvelope,
  BridgeOptions,
  BridgeReceiveReason,
  BridgeReceiveResult,
  BridgeStats,
  BridgeTransport,
} from './bridge.ts';
export type {
  DeliveryLatencyOptions,
  DeliveryLatencySummaryStats,
  LatencySloMissEvent,
  ProcessingLatencySloOptions,
} from './latency.ts';
export type {
  AckLatencyOptions,
  AckLatencySummaryStats,
  AckSloMissEvent,
} from './acklatency.ts';
export type { LagEvent, LagMonitorOptions, LagSummaryStats } from './lag.ts';
export type { LateMessageCallback, LateMessageEvent } from './watermark.ts';
export type {
  TraceOptions,
  TraceSampler,
  TraceSamplingDecision,
  TraceSpan,
} from './trace.ts';

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
  /**
   * Sequence epoch for gap detection. Absent (or empty) for messages
   * published on this node; `hub:<hubEpoch>` for messages received from
   * the cluster hub (see `EventBus.connectToHub`). The bus resets a
   * subscriber's per-topic gap baseline when the epoch changes instead of
   * counting a phantom gap across the local/cluster sequence spaces.
   */
  epoch?: string;
  /**
   * Application-level message identity, set when the publish carried one
   * (`PublishOptions.messageId`, always set by `publishIdempotent` when
   * its `messageId` is present). It rides the envelope end to end —
   * durable log, replay, cluster forwarding, redeliveries — so
   * subscriber-side dedup (`SubscribeOptions.deduplicateMessages`) can
   * recognize the same logical message across replays and redeliveries.
   * Absent for plain publishes without an identity.
   */
  messageId?: string;
  /**
   * Business event time of the message in epoch milliseconds, set when the
   * publish carried one (`PublishOptions.eventTime`). Unlike `seq` — the
   * bus-assigned publish-order number (EB-13) — this is the application's
   * own clock: when the event happened, not when the bus saw it. It rides
   * the envelope end to end (durable log, replay) and drives the
   * per-topic event-time watermark (EB-59): a message whose event time is
   * older than the topic's watermark is delivered normally but counted as
   * late. Absent for publishes that did not carry an event time.
   */
  eventTime?: number;
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
 * Tuning for subscriber-side exactly-once dedup
 * (`SubscribeOptions.deduplicateMessages`). Bounds are validated at
 * `subscribe` time; violations throw `RangeError`.
 */
export interface DeduplicateMessagesOptions {
  /**
   * How long a delivered `messageId` stays in the window, in
   * milliseconds on the bus clock (`EventBusOptions.now`). A re-arrival
   * of the same `(topic, messageId)` at or after `windowMs` is "unknown"
   * — delivered again, no guarantee. Must be a positive finite number.
   * Defaults to the bus's publish-side idempotency window
   * (`EventBusOptions.idempotencyWindowMs`, 60s), so the subscribe-side
   * horizon matches the publisher's retry horizon unless tuned.
   */
  windowMs?: number;
  /**
   * Maximum `(topic, messageId)` entries held per subscriber; the
   * oldest entry is evicted when full. Must be a positive integer.
   * Default 10000.
   */
  maxEntries?: number;
  /**
   * Stable consumer identity for the durable dedup window. When set,
   * every recorded `messageId` is journaled to the durable log
   * directory and rehydrated on subscribe, so a restarted bus does not
   * double-deliver on resume. Requires `EventBusOptions.durableLogDir`
   * — passing it without one throws `RangeError`. Must be a non-empty
   * string when provided. Omit it for a memory-only window.
   */
  consumerId?: string;
}

/**
 * Tuning for opt-in causal (happens-before) delivery (see
 * `SubscribeOptions.causal`).
 */
export interface CausalSubscribeOptions {
  /**
   * Maximum out-of-order messages buffered per causal source while
   * their dependencies are missing. When a new arrival would exceed
   * it, the oldest buffered message is dropped (drop-oldest) and the
   * source's expectation advances past it — the anti-deadlock rule that
   * keeps the stream moving when a dependency never arrives. Must be a
   * positive integer. Default 1000.
   */
  maxBufferPerSource?: number;
}

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
   * Opt-in byte budget for the subscriber's backpressure queue, in bytes.
   * With it, the queue is bounded by both `queueSize` (count) and
   * `queueMaxBytes` (bytes): a message whose admission would push the
   * buffered payload total over the budget sheds entries per
   * `dropPolicy`, exactly like count evictions — the shed counts into
   * `droppedCount` and surfaces as a sequence gap. Payload size is
   * estimated as JSON UTF-8 bytes (unserializable payloads count as 0);
   * mixed big/small message streams can no longer blow memory through a
   * count-only queue. Disabled by default. Must be a positive finite
   * number when given.
   */
  queueMaxBytes?: number;
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
   * Opt-in per-subscriber sliding-window delivery rate limit: at most
   * `maxMessages` deliveries per rolling `perWindowMs` window. Unlike
   * `deliveryShaping` (a token bucket that smooths bursts), the window is
   * exact — a delivery counts for a full `perWindowMs` after it happens, so
   * two quick deliveries still block the window for its whole width; there
   * is no gradual refill. Messages over budget stay queued — in FIFO order,
   * never dropped, never counted as sequence gaps — and are delivered on
   * later flush rounds as the window slides, so the backlog drains even
   * when no new publishes arrive.
   *
   * Composes with `deliveryShaping`: shaping paces each flush round first
   * (smoothing bursts), then the window enforces the hard cap — a message
   * is delivered only when both allow it. Content filters run at fan-out,
   * before either; TTL expiry still drops waiting messages without
   * consuming window budget; a health-probed subscriber that degrades keeps
   * its window (no deliveries happen while paused, so nothing is
   * consumed). `getStats()` exposes the per-subscriber backlog held back
   * by the window (`rateLimitedWaiting`).
   *
   * `maxMessages` must be an integer >= 1 and `perWindowMs` a positive
   * finite number of milliseconds — invalid values throw `RangeError` from
   * `subscribe`. Disabled by default.
   */
  rateLimit?: RateLimitOptions;
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
   * Opt-in per-subscriber end-to-end ack-latency sampling. When enabled,
   * the bus stamps every message accepted into this subscriber's queue
   * with the bus clock and records one sample per message when its
   * delivery's `ack()` finishes: the accepted→ack latency in
   * milliseconds. Unlike `deliveryLatency` (enqueue→handler-hand-off
   * queue dwell), this includes handler processing time and any consumer
   * think time before `ack()` — the full round trip the producer's SLO
   * actually depends on. `getStats()` exposes the per-subscriber
   * p50/p95/p99 distribution plus the SLO attainment rate (`ackLatency`),
   * and `src/metrics.ts` renders them as Prometheus gauges. Only
   * reliable subscriptions (`subscribeReliable`) ever sample — a plain
   * subscription has no `ack()`, so its window stays empty. Disabled by
   * default.
   *
   * Sampling semantics: `nack()` and ack-timeout redeliveries restart the
   * accepted clock, but each message still contributes exactly one sample
   * — repeated deliveries never sample twice, and acking a stale delivery
   * handle (one whose delivery already timed out and requeued) records
   * nothing.
   *
   * Pass `true` for the defaults (1024-sample rolling window per
   * subscriber), or an `AckLatencyOptions` object to tune the window.
   * Invalid values throw `RangeError` from `subscribe`.
   */
  ackLatency?: boolean | AckLatencyOptions;
  /**
   * The ack-latency SLO in milliseconds for this subscriber: samples
   * above it fire `onAckSloMiss` and count against the SLO attainment
   * rate (`sloAttainment` in `getStats().ackLatency`). Defaults to 30000.
   * Must be a positive finite number — invalid values throw `RangeError`
   * from `subscribe`. Only meaningful with `ackLatency` enabled;
   * providing it (or `onAckSloMiss`) without `ackLatency` also throws
   * `RangeError`, since the value could never take effect.
   */
  ackSloMs?: number;
  /**
   * Fired synchronously when an acked delivery's accepted→ack latency
   * exceeds `ackSloMs` — once per over-budget ack, with the sample, the
   * SLO, and the subscriber's identity. Must be a function; only
   * meaningful with `ackLatency` enabled (see `ackSloMs`).
   */
  onAckSloMiss?: (event: AckSloMiss) => void;
  /**
   * Opt-in per-subscriber handler processing-latency p99 SLO alerting.
   * When enabled, the bus measures every handler invocation's processing
   * time — delivery→handler-return for plain subscriptions,
   * delivery→`ack()` completion for reliable ones — into a bounded
   * rolling window (nearest-rank p99, default 1024 samples, `windowSize`
   * tunable) and fires `onLatencySloMiss` once when the windowed p99
   * exceeds `p99ThresholdMs`, re-arming after it drops back to or below
   * the threshold (the `onBackpressure` / `onLag` latch). The alert is
   * advisory only: it never pauses, degrades, or otherwise disturbs
   * delivery, and a throwing callback is swallowed — unlike the other
   * subscriber monitoring callbacks, it cannot propagate into the flush
   * loop. It is orthogonal to `healthProbe` (EB-18): a slow handler trips
   * this alert while only throws and `processingTimeoutMs` overruns count
   * toward health degradation.
   *
   * How it differs from the neighboring latency features: `deliveryLatency`
   * (EB-31) measures the enqueue→hand-off queue dwell (handler time
   * excluded); `ackLatency` (EB-40) measures the accepted→ack round trip
   * per message with a per-sample SLO; this one measures pure consumer
   * processing time and alerts on the windowed p99. All three sample
   * independently — enabling one never double-samples another (a reliable
   * subscriber's processing samples come from `ack()` completion only,
   * never from the synchronous handler-invocation timing).
   *
   * `getStats()` exposes the per-subscriber p99 snapshot
   * (`subscriberLatencyP99`), and `src/metrics.ts` renders
   * `eventbus_subscriber_processing_latency_p99{subscriber,pattern}`.
   * `p99ThresholdMs` is required and must be a positive finite number;
   * invalid values throw `RangeError` from `subscribe` (`TypeError` for a
   * non-function `onLatencySloMiss`). Disabled by default.
   */
  latencySlo?: ProcessingLatencySloOptions;
  /**
   * Opt-in subscriber lag watermark monitoring. When enabled, the bus
   * tracks how long the oldest message currently sitting in this
   * subscriber's queue has been waiting (the live consumer-lag watermark,
   * `now - enqueuedAt(head)`) and samples every drained message's
   * enqueue→drain dwell into a bounded rolling window (p50/p99).
   * `getStats()` exposes the per-subscriber watermark and distribution
   * (`lag`) and the slowest lagging subscribers (`laggingSubscribers`),
   * and `src/metrics.ts` renders them as Prometheus gauges.
   *
   * Where `deliveryLatency` records a historical distribution of completed
   * deliveries, the lag monitor answers the live question — "how far
   * behind is this consumer right now?": a subscriber whose handler is
   * stuck shows a watermark that keeps growing while nothing is delivered.
   * With `thresholdMs` + `onLag`, the watermark also drives alerting: the
   * callback fires once when the watermark reaches the threshold and
   * re-arms after it drops below, mirroring the `onBackpressure` /
   * `onDrained` latch. The watermark is evaluated on every enqueue and
   * every drain, so a crossing is observed on the next bus activity after
   * it happens.
   *
   * Pass `true` for the defaults (1024-sample rolling window, watermark
   * reporting with no alerting), or a `LagMonitorOptions` object to tune
   * the window and arm the threshold alert. Invalid values throw
   * `RangeError` from `subscribe`; `onLag` without `thresholdMs` also
   * throws, since the callback could never fire. Disabled by default.
   */
  lagMonitor?: boolean | LagMonitorOptions;
  /**
   * Opt-in per-(subscriber, key) hotspot monitoring (see `PublishOptions.key`
   * and `src/keyhotspot.ts`). When enabled, the bus samples the depth of
   * each of this subscription's keyed ordering reorder buffers — the
   * number of keyed messages held because an earlier keySeq has not been
   * fanned out yet — and `getStats()` exposes the deepest streams as
   * `hotKeys` (top 10, hottest first), with
   * `eventbus_key_hotspot_buffer_depth` in `src/metrics.ts`. With
   * `onKeyHotspot`, a stream whose buffer reaches `thresholdDepth` fires
   * the callback once per excursion, re-arming after the depth drains
   * below the threshold (mirroring `onBackpressure` / `onLag` latch
   * semantics). Detection only reads buffer depth: it never mutates the
   * reorder buffer, the per-key expectation, or delivery order.
   *
   * Pass `true` for the defaults (threshold 100, depth reporting with no
   * alerting), or a `KeyHotspotOptions` object to tune the threshold and
   * arm the alert. Invalid values throw `RangeError` / `TypeError` from
   * `subscribe`. Disabled by default.
   */
  keyHotspot?: boolean | KeyHotspotOptions;
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
   * Time-based resume for durable-log replay. When the bus has a durable
   * log (`EventBusOptions.durableLogDir`), the subscriber's queue is
   * pre-filled at subscribe time with every logged message on topics
   * matching its pattern published strictly after `resumeFromTime` (a
   * wall-clock timestamp in milliseconds on the bus clock —
   * `EventBusOptions.now`), in publish-time order — so a brand-new
   * consumer (cold start) or a disaster-recovery replay catches up
   * everything since a moment in time without needing a per-topic
   * sequence checkpoint. Messages keep their original `seq` and TTL
   * deadline; ones whose TTL already expired are dropped as expired at
   * drain time, not resurrected. Like `resumeFromSeq`, replay honors
   * the content `filter`, handoff-linger windows, keyed compaction
   * (only the latest value per key replays), compression
   * re-registration and the keyed-ordering baseline, and goes through
   * the subscriber's normal backpressure policy.
   *
   * Mutually exclusive with `resumeFromSeq`: a replay has exactly one
   * cursor, so passing both throws `RangeError`. Requires
   * `durableLogDir`: passing `resumeFromTime` without it throws
   * `RangeError` instead of silently replaying nothing. Must be a
   * finite number of milliseconds `>= 0`. For consumer-group members
   * the replay is per member, like `resumeFromSeq`.
   */
  resumeFromTime?: number;
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
   * Opt-in subscriber-side exactly-once dedup window. When enabled, the
   * bus remembers the `messageId` of every message that entered this
   * subscriber's queue within `windowMs`, and suppresses any later
   * arrival of the same `(topic, messageId)` — across durable-log
   * replays, ack-timeout/nack redeliveries, and health-probe requeues —
   * so the handler sees each logical message at most once per window.
   * Suppressed duplicates never reach the queue (no backpressure budget,
   * no throttle token, no sequence gap — the original delivery already
   * advanced the baseline) and are counted in
   * `getStats().dedupDropped`.
   *
   * Only messages carrying a `messageId` (`PublishOptions.messageId`,
   * always set by `publishIdempotent`) participate: plain publishes
   * without an identity are unaffected. A message whose `messageId`
   * aged out of the window is "unknown" again — the bus no longer
   * guarantees anything about it, and a re-arrival is delivered.
   *
   * Composes with at-least-once (`subscribeReliable`): within the
   * window, dedup wins — a redelivery is suppressed instead of
   * requeued, so combine with `deadLetter` only for the first-delivery
   * poison path (a message whose first delivery already poisoned the
   * handler still dead-letters on redelivery-budget exhaustion when
   * dedup is off). An operator's `replayDeadLetter` is a deliberate
   * fresh chance and bypasses the window.
   *
   * Durability: pass a stable `consumerId` and run the bus with a
   * durable log (`EventBusOptions.durableLogDir`) to persist the window
   * — a restarted bus rehydrates it, so a crash between delivery and
   * processing cannot double-deliver on resume. Without `consumerId`
   * the window is memory-only and a restart starts it empty. Sharing
   * one `consumerId` between two live subscribers is unsupported: each
   * hydrates its own copy of the persisted window and they diverge.
   *
   * Pass `true` for the defaults, or a `DeduplicateMessagesOptions`
   * object to tune the window. Invalid values throw `RangeError` from
   * `subscribe`. Disabled by default.
   */
  deduplicateMessages?: boolean | DeduplicateMessagesOptions;
  /**
   * Opt-in causal (happens-before) delivery. When enabled, the
   * subscriber receives messages carrying a causal clock (see
   * `PublishOptions.causal`) in happens-before order per source: a
   * message is delivered if and only if its clock equals the source's
   * next expected clock (starting at 0); an early arrival waits in a
   * per-source reorder buffer — pre-queue, consuming no backpressure
   * budget, with no filter or throttle evaluated yet — until its
   * dependencies are admitted, and each admission advances the
   * expectation and releases buffered successors in clock order. A
   * regressed clock (below the expectation — a duplicate or a late
   * arrival from before the subscriber's horizon) is delivered
   * immediately without moving the expectation backwards, and counted
   * in `getStats().causalBuffer.regressedMessages`. Messages without a
   * causal clock bypass the gate entirely: they are delivered directly
   * and never disturb any source's expectation.
   *
   * Composes with per-key publish-order delivery (`PublishOptions.key`):
   * for a keyed causal message the causal gate runs first — happens-before
   * order takes precedence over publish (keySeq) order — and an admitted
   * message then goes through the keyed gate as usual. Keyed messages
   * without a causal clock keep the exact keyed behavior they had before.
   *
   * Every wait terminates: the per-source buffer is bounded
   * (`maxBufferPerSource`, default 1000) — when a new arrival would
   * exceed it, the oldest buffered message is dropped (drop-oldest) and
   * the expectation advances past it, so a dependency that never arrives
   * cannot wedge the stream (dropped this way are counted in
   * `getStats().causalBuffer.droppedMessages`). Buffered messages keep
   * their TTL deadline: one that expires while waiting is dropped as
   * expired at release — never resurrected — and the stream advances past
   * it. `getStats().causalBuffer.depth` reports the total buffered
   * messages across subscribers; `causalBufferDepth(subId)` reads one
   * subscriber's depth.
   *
   * Pass `true` for the defaults, or a `CausalSubscribeOptions` object
   * to tune the buffer bound. Invalid values throw `RangeError` from
   * `subscribe`. Disabled by default — without it the subscriber builds
   * no buffers and tracks no clocks: zero overhead, zero behavior change.
   */
  causal?: boolean | CausalSubscribeOptions;
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
  /**
   * Multi-tenant namespace (EB-52, see `EventBus.createNamespace`): the
   * subscription is scoped to the namespace — `topicPattern` is matched
   * against the sub-topic after the `<namespace>/` prefix, so `**`
   * receives everything in the namespace and structurally cannot reach
   * another namespace's topics. The registered pattern is recorded as
   * `<namespace>/<topicPattern>`. The pattern itself must not contain
   * `/` (an attempted escape throws `NamespaceDeniedError`); an unknown
   * namespace throws `RangeError`. A namespace that disallows subscribing
   * (`allowSubscribe: false`) rejects with `NamespaceDeniedError`.
   * Durable-log replays (`resumeFromSeq`/`resumeFromTime`) read the
   * namespace's own log. Deliveries name the concrete
   * `<namespace>/<topic>` topic.
   */
  namespace?: string;
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

/**
 * Tuning for the per-subscriber sliding-window delivery rate limit
 * (`SubscribeOptions.rateLimit`). Both fields are required — there is no
 * sensible default for a hard cap. Bounds are validated at `subscribe`
 * time; violations throw `RangeError`.
 */
export interface RateLimitOptions {
  /**
   * Maximum deliveries in one sliding window. Must be an integer >= 1.
   */
  maxMessages: number;
  /**
   * Width of the sliding window in milliseconds. Must be a positive finite
   * number. A delivery counts against the window for a full `perWindowMs`
   * after it happens — there is no gradual refill, unlike the
   * `deliveryShaping` token bucket.
   */
  perWindowMs: number;
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

/**
 * Fired when a reliable subscriber's acked delivery exceeds its ack-latency
 * SLO (see `SubscribeOptions.onAckSloMiss`): once per over-budget ack,
 * synchronously with the sample. `subscriberId` / `pattern` identify whose
 * SLO was missed; `latencyMs` / `sloMs` / `at` carry the sample.
 */
export interface AckSloMiss extends AckSloMissEvent {
  /** The subscriber whose ack missed the SLO. */
  subscriberId: string;
  /** The topic pattern the subscriber registered. */
  pattern: string;
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
  /**
   * Opt-in centralized poison-message diagnostics (see
   * `ReliableSubscribeOptions.deadLetter`): when a message is
   * dead-lettered, the bus publishes one diagnostic event to this topic —
   * the original payload plus `{ subscriberId, pattern, seq, lastError,
   * redeliveries, traceId, deadLetteredAt }` metadata — so operators can
   * consume and alert on every poison message from a single topic instead
   * of polling per-subscriber DLQs.
   *
   * The diagnostic event goes through the normal publish pipeline (ACL,
   * schema validation, rate limiting); when admission rejects it, no
   * diagnostic is emitted but the DLQ move itself is unaffected. A
   * diagnostic event that itself dead-letters never emits a second
   * diagnostic — recursion is cut at the source. Must be a non-empty
   * string when given.
   */
  diagnosticTopic?: string;
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
   * What the message's final delivery attempt failed with: the handler's
   * thrown error message when the handler threw, otherwise the tracker's
   * redelivery reason (`'nack'` for an explicit `nack()`, `'ack-timeout'`
   * when the ack deadline passed silently). The poison-message signal an
   * operator reads first — it says *why* the message kept failing, not
   * just how often.
   */
  lastError?: string;
  /**
   * The end-to-end trace id the message carried (`EventBusOptions.trace`),
   * so a poison message can still be correlated with its publish → fanout
   * → deliver spans after it leaves the delivery pipeline. Absent when
   * the message was not part of a sampled trace.
   */
  traceId?: string;
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
/**
 * Partition ownership change from one consumer-group rebalance, for
 * partitioned groups (see `GroupSubscribeOptions.partitions`).
 */
export interface PartitionMigration {
  /** The partition whose owner changed. */
  partition: number;
  /** Previous owner's subscription id, null when it was unassigned. */
  from: string | null;
  /** New owner's subscription id. */
  to: string;
  /**
   * Per-topic assignment watermarks for the partition: the highest
   * per-topic `seq` assigned to this partition so far. The new owner
   * replays the partition's uncommitted backlog — `(committed, watermark]`
   * — from the durable log automatically on migration.
   */
  watermarks: Record<string, number>;
}

/**
 * Partition state carried on `GroupRebalanceEvent` for partitioned groups
 * (see `GroupSubscribeOptions.partitions`). Absent for classic
 * round-robin groups.
 */
export interface PartitionRebalanceInfo {
  /** Post-change partition -> member subscription id. */
  assignment: Record<number, string>;
  /** Partitions whose owner changed in this rebalance. */
  migrated: PartitionMigration[];
  /**
   * Assignment strategy that produced this rebalance
   * (`GroupSubscribeOptions.assignment`): `'rendezvous'` (default) or
   * `'sticky'` (EB-54 cooperative). Lets the operator tell a minimal-diff
   * sticky migration apart from a rendezvous recomputation.
   */
  strategy: 'rendezvous' | 'sticky';
}

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
  /**
   * Present for partitioned groups (see `GroupSubscribeOptions.partitions`):
   * the post-change partition assignment and which partitions migrated.
   */
  partitionRebalance?: PartitionRebalanceInfo;
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
  /**
   * Opt-in Kafka-style partition assignment for the competing set. When
   * set, the group's messages are divided into `partitions` logical
   * partitions and each partition is owned by exactly one member, which
   * exclusively consumes it — instead of the default per-message
   * round-robin. A keyed message's partition derives from its key
   * (`hash(key) % partitions`), so one key always lands on one partition;
   * a keyless message's partition derives from `(topic, seq)`.
   *
   * Assignment is deterministic over the member roster (rendezvous hashing
   * by default, or the sticky balanced assignor with
   * `assignment: 'sticky'` — see below): members joining or leaving only
   * migrate the partitions whose winner changed — unaffected partitions
   * keep their owner and their backlog is never replayed. On migration
   * the new owner automatically replays the partition's uncommitted
   * backlog — `(committed, watermark]` per topic —
   * from the durable log (requires `EventBusOptions.durableLogDir`;
   * without it the event still carries the watermarks for the operator).
   * Use `commitOffset(..., { partition })` for per-partition checkpoints.
   *
   * The partition count is fixed by the FIRST `subscribeToGroup` for the
   * (groupId, pattern): a later member that specifies a different count —
   * or specifies one for a round-robin group — throws `RangeError` and its
   * subscription is rolled back. Members that omit it simply join the
   * partitioned group. Must be a positive integer.
   */
  partitions?: number;
  /**
   * Partition assignment strategy for the competing set (EB-54), only
   * meaningful together with `partitions`. Two strategies:
   *
   * - `'rendezvous'` (default): deterministic highest-random-weight
   *   hashing over the member roster. Minimal disruption — a join/leave
   *   only changes the winners it actually affects — but with no balance
   *   guarantee (6 partitions over 3 members can land 3/2/1).
   * - `'sticky'`: deterministic sticky balanced assignment
   *   (`src/sticky.ts`). Every member owns `floor(P/N)` or `ceil(P/N)`
   *   partitions, and a rebalance migrates exactly the minimal diff the
   *   balance requires: partitions that keep their owner are never
   *   touched, so their consumer keeps consuming through the rebalance
   *   with no pause and no replay — the cooperative protocol, no
   *   stop-the-world full reassignment.
   *
   * Like the partition count, the strategy is fixed by the FIRST
   * `subscribeToGroup` for the (groupId, pattern): a later member that
   * specifies a different strategy throws `RangeError` and its
   * subscription is rolled back. Members that omit it inherit the group's
   * strategy. The active strategy is reported on every
   * `partitionRebalance` event (`PartitionRebalanceInfo.strategy`).
   */
  assignment?: 'rendezvous' | 'sticky';
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
   * Opt-in segment rotation for the durable log (see
   * `SegmentRotationOptions` in `src/durablelog.ts`): the per-topic live
   * file holds the current segment, and closed segments are gzipped to
   * `<topic>.seg-<seqStart>-<seqEnd>.jsonl.gz` with an append-only
   * archive index (`__archive_index.jsonl`) by an unref'd background
   * pass — publishing never waits for compression. Replay
   * (`SubscribeOptions.resumeFromSeq` / `resumeFromTime`) reads the live
   * segment by default; archived segments are read on demand via
   * `DurableTopicLog.readSince` / `readSinceTime` with
   * `{ includeArchived: true }`. Only meaningful with `durableLogDir`.
   * Default: absent (no rotation — one live file per topic, as before).
   * Invalid triggers throw `RangeError` at construction.
   */
  durableLogSegmentRotation?: SegmentRotationOptions;
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
  /**
   * Opt-in consumer-group lag alerting (see `src/grouplag.ts`): per
   * (group, topic) lag — the highest per-topic `seq` assigned to any
   * member minus the consumer checkpoint (`commitOffset`) — is computed
   * on every assignment, commit, and rebalance, and `onGroupLag` fires
   * once per threshold excursion, with latch semantics (rearms only after
   * the lag falls back to or below the threshold).
   *
   * Live handoff-linger windows (`GroupSubscribeOptions.handoffLingerMs`)
   * are excluded from lag: backlog a departed member is presumed to still
   * be processing never reads as consumer lag, so a rebalance with a
   * lingering handoff does not false-alarm.
   *
   * `thresholdMessages` is the default lag threshold in messages for all
   * groups (default 100; `0` alerts on any positive lag); `thresholds`
   * overrides it per groupId, also updatable at runtime via
   * `EventBus.setGroupLagThreshold` / `clearGroupLagThreshold`. Values
   * must be finite numbers `>= 0`, else `RangeError` at construction.
   * Lag rows are always observable via `EventBus.getStats().groupLag`
   * regardless of this option; this only enables the alert callback.
   */
  groupLag?: GroupLagMonitorOptions;
  /**
   * Unified publish-side admission-rejection hook: called once for every
   * publish the admission gates drop — ACL denials (`'acl'`, see
   * `setAclRules`), schema-validation rejections
   * (`'schema'`, see `setTopicSchema`), rate-limit sheds (`'rate-limit'`,
   * see `setTopicRateLimit`), idempotency-duplicate suppressions
   * (`'duplicate'`, see `publishIdempotent`), retired-alias rejections
   * (`'alias-retired'`, see `setTopicAlias`), and `publishAtomic` batch
   * rejections (surfaced with the failing entry's gate reason).
   *
   * `reason` maps 1:1 onto the stats counters
   * (`TopicStats.rejectedMessages` / `.rateLimitedMessages` /
   * `.duplicateMessages` / `.aliasRetiredMessages`, and the `BusStats` totals), so hook events
   * reconcile exactly against `getStats()` — every counted rejection
   * fires exactly one event, and vice versa.
   *
   * The hook fires after the counters move. It receives a snapshot
   * (`AdmissionRejectionEvent`); the event carries the rejected payload's
   * byte size, never the payload itself. The callback is error-isolated:
   * a throwing hook is swallowed so a broken observer can never disturb
   * the publish path. Default: unset (no hook — fully backward
   * compatible).
   */
  onAdmissionRejected?: AdmissionRejectionCallback;
  /**
   * Broker-level topic ACL (see `AclRule` and `setAclRules`): per-topic
   * publish/subscribe permissions with allow/deny decisions on wildcard
   * patterns. Rules are evaluated in order; the first matching rule with
   * an explicit decision for the action wins; `defaultPolicy` (default
   * `'allow'`) covers everything no rule decides. Unauthorized publishes
   * return 0 and are counted as rejections (reason `'acl'`);
   * unauthorized subscribes throw `AclDeniedError`. Rule changes via
   * `setAclRules` take effect immediately. Default: unset (no ACL — the
   * bus behaves exactly as before).
   */
  acl?: AclOptions;
  /**
   * Authorization-denial audit hook: called once for every publish or
   * subscribe the ACL denies, with an `AuthzDeniedEvent` carrying the
   * action and the concrete topic (publish) or subscription pattern
   * (subscribe) — never the payload. Publish denials additionally surface
   * on `onAdmissionRejected` with reason `'acl'` (so they stay
   * reconcilable with the rejection counters); subscribe denials throw
   * `AclDeniedError`, so this hook is their only audit channel. The
   * callback is error-isolated: a throwing hook is swallowed so a broken
   * observer can never disturb the publish/subscribe path. Default: unset.
   */
  onAuthzDenied?: AuthzDeniedCallback;
  /**
   * Opt-in delivery-pipeline trace spans (see `src/trace.ts`). When
   * enabled, the bus emits one trace per sampled publish, threading the
   * message through `bus.publish` → `bus.admission` → `bus.fanout` →
   * `bus.enqueue` (per subscriber) → `bus.deliver` (per subscriber) →
   * `bus.ack` (reliable subscriptions, on `ack()`). Each span carries
   * `{ traceId, spanId, parentId, name, at, durationMs, attrs }`, with
   * `traceId` in 32-hex — the same format as the `webhook-relay-ts`
   * WR-18 trace ID, so traces correlate across the two by string
   * equality.
   *
   * Sampling is head-based: the decision is taken once at admission, and
   * every downstream span of the same publish shares the verdict.
   * `sampleRate` (default 1) keeps a fixed fraction of admitted
   * publishes; `sampler` plugs in a custom decision and overrides the
   * rate. A publish may continue an upstream trace via
   * `PublishOptions.traceparent` (W3C `traceparent` header value); a
   * missing or malformed value mints a fresh trace id. Sampled spans go
   * to `onTraceSpan` (error-isolated, like `onAdmissionRejected`) and to
   * a bounded ring buffer exported as `getStats().traceSpans` (oldest
   * evicted first, `bufferSize` default 1024).
   *
   * Disabled by default, and the disabled path is allocation-free: when
   * tracing is off the publish/deliver path pays one branch and nothing
   * else — no WeakMap lookup, no clock read, no object creation.
   *
   * Pass `true` for the defaults (sample everything admitted), or a
   * `TraceOptions` object to tune the rate, plug in a sampler, and arm
   * the callback. Invalid values throw `RangeError` from the constructor.
   */
  trace?: boolean | TraceOptions;
  /**
   * Cross-process fan-out bridge (see `src/bridge.ts`, EB-51): a pluggable
   * `BridgeTransport` that mirrors every admitted local publish to an
   * external broker (Redis Streams, NATS, … — the adapter is implemented
   * by the caller, the core stays zero-dependency) so other nodes can fan
   * it out. Inbound envelopes arrive via `receiveFromBridge` and go
   * through the same admission as local publishes (alias resolution, ACL,
   * schema validation, per-topic rate-limit budget, TTL at drain) with
   * node-local sequence numbers, and are never mirrored back — the bridge
   * stays loop-free. `key` (with its publish-order `keySeq`), `messageId`,
   * the end-to-end `traceId`, and the source TTL deadline ride the
   * envelope. Disabled by default; an invalid config throws `RangeError`
   * from the constructor.
   */
  bridge?: BridgeOptions;
  /**
   * Bus-wide default allowed lateness in milliseconds for event-time
   * watermark tracking (EB-59): a message is late when its business event
   * time (`PublishOptions.eventTime`) is older than the topic's watermark
   * (`max(eventTime) - allowedLatenessMs`). Per-topic overrides via
   * `EventBus.setTopicAllowedLateness` win over this default. Must be a
   * non-negative finite number; anything else throws `RangeError` from the
   * constructor. Default 0 — any event time below the observed peak is
   * late.
   */
  allowedLatenessMs?: number;
  /**
   * Late-message hook for event-time watermark tracking (EB-59): called
   * once for every admitted publish whose event time is older than its
   * topic's watermark. The message is still delivered normally — this is
   * the observability signal, not a gate. The callback is error-isolated:
   * a throwing hook is swallowed so a broken observer can never disturb
   * the publish path. Default: unset (no hook — fully backward
   * compatible).
   */
  onLate?: LateMessageCallback;
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
   * Messages on this topic whose business event time was older than the
   * topic's event-time watermark when admitted (`PublishOptions.eventTime`
   * below `max(eventTime) - allowedLatenessMs`, EB-59). Late messages are
   * still delivered normally — this is the data-quality counter, not a
   * loss counter — and are also reported on `EventBusOptions.onLate`.
   */
  lateMessages: number;
  /**
   * The topic's event-time watermark in epoch milliseconds:
   * `max(eventTime) - allowedLatenessMs` over the admitted publishes that
   * carried an event time (EB-59). `undefined` until the first event-time
   * publish on the topic — messages without an event time never move it.
   * Reflects the currently configured allowed lateness (bus-level default
   * or the per-topic override), so reconfiguring the lateness changes this
   * reading immediately.
   */
  watermark: number | undefined;
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
   * Messages on this topic suppressed by subscriber-side exactly-once
   * dedup (see `SubscribeOptions.deduplicateMessages`): the same
   * `(topic, messageId)` arrived again within a subscriber's dedup
   * window — via replay or redelivery — after the original delivery, so
   * it never entered the queue again. Counts each suppressed queue
   * entry once, like `filteredMessages`.
   */
  dedupDropped: number;
  /**
   * Publishes to this topic rejected because its migration alias expired
   * (see `EventBus.setTopicAlias`): the old topic is read-only, so the
   * publish was refused before admission — no sequence number consumed
   * (subscribers see no gap), the durable log never sees it, and it burns
   * no rate-limit budget. Surfaced on `onAdmissionRejected` with reason
   * `'alias-retired'`.
   */
  aliasRetiredMessages: number;
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
   * Estimated buffered payload bytes across all live subscriber
   * backpressure queues (see `SubscribeOptions.queueMaxBytes`). The
   * memory-pressure signal that a count-only queue hides: with mixed
   * big/small messages, `droppedMessages` stays 0 while this climbs.
   */
  queueBytes: number;
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
   * Total messages admitted with a business event time older than their
   * topic's event-time watermark — the sum of every topic's
   * `lateMessages` (EB-59). Late messages are still delivered; this is the
   * bus-wide data-quality signal (stale producers, clock skew, replayed
   * feeds), also reported per occurrence on `EventBusOptions.onLate`.
   */
  lateMessages: number;
  /**
   * Total publish/subscribe operations denied by the broker-level ACL
   * (see `setAclRules`). Every denied publish is also counted in
   * `rejectedMessages`; every denied subscribe throws `AclDeniedError`.
   * The security-relevant counter: a spike here is an unauthorized-access
   * probe worth alerting on. Denials are audited via
   * `EventBusOptions.onAuthzDenied`.
   */
  authzDenied: number;
  /**
   * Total idempotent publishes suppressed as duplicates — the sum of
   * every topic's `duplicateMessages`. See `TopicStats.duplicateMessages`
   * for the exact counting rules.
   */
  duplicateMessages: number;
  /**
   * Total messages suppressed by subscriber-side exactly-once dedup —
   * the sum of every topic's `dedupDropped`. See
   * `TopicStats.dedupDropped` for the exact counting rules.
   */
  dedupDropped: number;
  /**
   * Total publishes rejected because the target topic's migration alias
   * expired (the old topic is read-only) — the sum of every topic's
   * `aliasRetiredMessages`. See `TopicStats.aliasRetiredMessages` for the
   * exact counting rules.
   */
  aliasRetiredMessages: number;
  /**
   * Every registered topic alias (see `EventBus.setTopicAlias`), live and
   * retired — the zero-downtime migration table. A snapshot; each entry
   * carries its `expiresAt` and whether it has `expired` on the bus clock.
   */
  aliases: TopicAliasInfo[];
  /**
   * Every registered topic route (see `EventBus.setTopicRoute`): the
   * forwarding table with each route's forward count. A snapshot —
   * mutating it does not affect the bus.
   */
  routes: TopicRouteInfo[];
  /**
   * Every registered cross-bus forward rule (see `EventBus.forward`): the
   * forwarding table with each rule's forward count. A snapshot —
   * mutating it does not affect the bus.
   */
  forwards: ForwardRuleInfo[];
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
   * Total DLQ diagnostic events admitted to their diagnostic topic (see
   * `DeadLetterOptions.diagnosticTopic`). Counted on admission, not
   * delivery — the event exists even when nothing subscribes to the
   * diagnostic topic yet. Admission rejections (ACL, schema validation)
   * do not count.
   */
  diagnosticEvents: number;
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
   * Per-subscriber sliding-window rate-limit backlog (see
   * `SubscribeOptions.rateLimit`): one row per subscriber with the limit
   * enabled, carrying the messages currently held back for lack of window
   * budget (queued entries plus, for batched subscribers, messages already
   * collected into the pending batch). 0 when the window has budget — a
   * backlog held back by shaping or the health probe does not count here.
   */
  rateLimitedWaiting: Array<{
    /** The subscriber's id (see `Subscription.id`). */
    subscriberId: string;
    /** The topic pattern the subscription was registered with. */
    pattern: string;
    /** Messages currently held back by the sliding window. */
    waiting: number;
  }>;
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
   * Per-subscriber end-to-end ack-latency summaries (see
   * `SubscribeOptions.ackLatency`): one entry per subscription with
   * sampling enabled, in subscription order. Each entry carries the
   * accepted→ack latency distribution (p50/p95/p99, min/max/mean over the
   * subscriber's bounded rolling window) plus the SLO attainment rate
   * (`withinSlo` / `sloAttainment` against `ackSloMs`, default 30000).
   * Empty when no subscriber opts in.
   */
  ackLatency: Array<
    { subscriberId: string; pattern: string } & AckLatencySummaryStats
  >;
  /**
   * Per-subscriber handler processing-latency p99 snapshots (see
   * `SubscribeOptions.latencySlo`): one entry per subscription with SLO
   * tracking enabled, in subscription order. Each entry carries the
   * windowed nearest-rank p99 of handler processing time (delivery→
   * handler-return for plain subscriptions, delivery→`ack()` completion
   * for reliable ones), the sample count, the configured threshold, and
   * whether the p99 is currently above the threshold (`breaching`, i.e.
   * the alert latch is set). Empty when no subscriber opts in.
   */
  subscriberLatencyP99: Array<{
    subscriberId: string;
    pattern: string;
    /** Windowed nearest-rank p99 handler processing latency, in milliseconds. 0 when empty. */
    p99Ms: number;
    /** Samples currently in the window. */
    samples: number;
    /** The configured `p99ThresholdMs`. */
    thresholdMs: number;
    /** Whether the windowed p99 is currently above the threshold. */
    breaching: boolean;
  }>;
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
   * Per-subscriber lag watermark monitoring (see
   * `SubscribeOptions.lagMonitor`): one entry per subscription with
   * monitoring enabled, in subscription order. Each entry carries the
   * live consumer-lag watermark (`watermarkMs`: how long the oldest
   * currently queued message has been waiting, 0 when the queue is
   * empty) plus the enqueue→drain dwell distribution (p50/p99,
   * min/max/mean over the subscriber's bounded rolling window). Empty
   * when no subscriber opts in.
   */
  lag: Array<
    { subscriberId: string; pattern: string; watermarkMs: number } & LagSummaryStats
  >;
  /**
   * The slowest lag-monitored subscribers by p99 drain dwell (top 5,
   * descending) — the first place to look when the lag watermark keeps
   * growing. Only subscribers with at least one sample appear.
   */
  laggingSubscribers: Array<{
    subscriberId: string;
    pattern: string;
    p99Ms: number;
    watermarkMs: number;
    samples: number;
  }>;
  /**
   * Keyed messages held in per-(subscriber, key) reorder buffers because
   * an earlier keySeq had not been fanned out yet — the observable count
   * of per-key publish-order enforcement (see `PublishOptions.key`).
   * Monotonic.
   */
  keyedReorderedMessages: number;
  /**
   * Causal (happens-before) reorder-buffer state (see
   * `SubscribeOptions.causal`). Always present — zeros when no
   * subscriber opted into causal delivery.
   */
  causalBuffer: {
    /**
     * Messages currently held in per-(subscriber, source) causal
     * reorder buffers, waiting on their dependencies — the live
     * happens-before backlog across all causal subscribers.
     */
    depth: number;
    /**
     * Messages dropped from full causal buffers (drop-oldest
     * anti-deadlock): a dependency that never arrived could not wedge
     * the stream, so the oldest wait was abandoned and the expectation
     * advanced past it.
     */
    droppedMessages: number;
    /**
     * Messages delivered immediately on clock regression: their clock
     * was below the source's expectation (a duplicate, or a late arrival
     * from before the subscriber's horizon), so the gate delivered them
     * without moving the expectation backwards.
     */
    regressedMessages: number;
  };
  /**
   * The hottest per-(subscriber, key) ordering streams by current
   * reorder-buffer depth (see `SubscribeOptions.keyHotspot` and
   * `src/keyhotspot.ts`), hottest first — at most `HOT_KEYS_LIMIT` (10)
   * entries. Only subscriptions with hotspot monitoring enabled are
   * sampled; entries carry the piling-up key, its current buffer depth,
   * and the subscription's alert threshold. Empty when nothing is
   * buffered anywhere monitored. The first place to look when keyed
   * delivery stalls on a hot key.
   */
  hotKeys: HotKeyStat[];
  /**
   * Sampled delivery-trace spans (see `EventBusOptions.trace` and
   * `src/trace.ts`), oldest first: every span of every sampled publish
   * since the bus was created, in completion order, bounded by
   * `TraceOptions.bufferSize` (oldest evicted first). Empty when tracing
   * is disabled. Each span carries `{ traceId, spanId, parentId, name,
   * at, durationMs, attrs }` — the same spans the `onTraceSpan` callback
   * receives. A snapshot — mutating it does not affect the bus.
   */
  traceSpans: TraceSpan[];
  /**
   * The hottest topics by 1m publish rate (see `src/rates.ts`), hottest
   * first — at most `HOT_TOPICS_LIMIT` (10) entries; topics with no
   * publishes in the trailing minute never appear. The first place to look
   * when deciding where to tighten rate limits or add capacity.
   */
  hotTopics: HotTopic[];
  /**
   * Cluster link state, present only while joined to a hub (see
   * `connectToHub`). `connected: false` means the transport dropped and
   * the bus is serving local-only on cached routes (degraded).
   */
  cluster?: {
    connected: boolean;
    degraded: boolean;
    nodeId: string;
    routeVersion: number;
    hubEpoch: string | null;
    remoteMembers: number;
    forwardedMessages: number;
    receivedMessages: number;
    forwardErrors: number;
    receiveDropped: number;
  };
  /**
   * Cross-process bridge counters (see `src/bridge.ts`, EB-51):
   * `outbound` local publishes mirrored to the transport, `inbound`
   * bridge envelopes admitted and fanned out locally, `dropped` inbound
   * envelopes shed on a full ingress buffer. All zero when no bridge is
   * configured.
   */
  bridge: BridgeStats;
  /**
   * Stats for every concrete topic that has seen at least one publish or
   * schema rejection, in order of first publish.
   */
  topics: TopicStats[];
  /**
   * Per-namespace aggregates (EB-52), in namespace registration order.
   * Derived from the per-topic `TopicStats`: every concrete topic of the
   * form `<prefix>/...` belongs to its namespace. Absent when no
   * namespace is registered, so snapshots from a namespace-free bus are
   * unchanged.
   */
  namespaces?: NamespaceStats[];
  /**
   * Live consumer groups: one entry per (groupId, pattern) competing set
   * with at least one member, in first-registration order. Empty when no
   * group subscription is active.
   */
  consumerGroups: Array<{ groupId: string; pattern: string; members: number; partitions?: number }>;
  /**
   * Consumer-group consumption lag (see `src/grouplag.ts`): one row per
   * (group, topic) for classic round-robin groups and one per (group,
   * partition, topic) for partitioned groups, sorted by (groupId,
   * pattern, topic, partition). `lag` is
   * `assignedSeq - max(committedSeq, lingerHeldToSeq)` floored at 0:
   * assigned but not yet consumed, excluding the backlog a live
   * handoff-linger window holds for a departed member. Always present —
   * lag observation needs no opt-in; only `onGroupLag` alerting does (see
   * `EventBusOptions.groupLag`).
   */
  groupLag: GroupLagStat[];
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
    /** Archive files that failed to decompress during archived reads. */
    corruptArchives: number;
    /** Total entries across all archived segments. */
    archived: number;
    /**
     * Per-topic segment view: live entries in the current segment,
     * closed archived segment count, and archived entries.
     */
    segments: Record<string, { live: number; segments: number; archived: number }>;
  };
}

/**
 * One keyed message moving through the per-(subscriber, key) ordering
 * gate: either delivered in keySeq order or held in the reorder buffer
 * until its predecessors are admitted.
 */
interface KeyedDelivery {
  msg: BusMessage;
  expiresAt: number | undefined;
  rawPayload: unknown;
  /**
   * True for durable-log replay: the content filter and handoff-linger
   * checks already ran in `replayLog`, so admission goes straight to the
   * queue — replay never burned throttle budget and must not start now.
   */
  replay: boolean;
}

/**
 * Per-(subscriber, key) publish-order state (see `PublishOptions.key`).
 *
 * `expected` is the next per-key sequence number this subscriber's stream
 * needs; `skipped` holds keySeqs at or above `expected` that will never be
 * fanned out to this subscriber — published to topics its pattern does not
 * match, or lost before fan-out (cancelled/expired/shed delayed schedules,
 * filtered replay) — so the stream advances past them instead of hanging;
 * `buffer` holds early arrivals (keySeq > expected, not skipped) until
 * their predecessors are admitted.
 */
interface KeyOrderState {
  expected: number;
  skipped: Set<number>;
  buffer: Map<number, KeyedDelivery>;
}

/**
 * One causal message moving through the per-(subscriber, source)
 * happens-before gate (see `SubscribeOptions.causal`): either delivered
 * in clock order or held in the reorder buffer until its dependencies are
 * admitted. `keyed` carries the message's per-key sequence number when
 * the publish was also keyed — the causal gate runs first and the keyed
 * gate second (see `admitCausal`).
 */
interface CausalDelivery {
  msg: BusMessage;
  expiresAt: number | undefined;
  rawPayload: unknown;
  /**
   * True for durable-log replay: the content filter and handoff-linger
   * checks already ran in `replayLog`, so admission goes straight to the
   * queue — replay never burned throttle budget and must not start now.
   */
  replay: boolean;
  keyed?: { key: string; keySeq: number };
}

/**
 * Per-subscriber causal (happens-before) delivery state (see
 * `SubscribeOptions.causal`). Created at subscribe time when the
 * subscriber opts in — a subscriber without it pays nothing: no buffer,
 * no clock tracking.
 *
 * `expected` is the next clock this subscriber's stream needs per source
 * (starts at 0 — the producer numbers each source's clocks from 0);
 * `skipped` holds clocks at or above `expected` that will never be fanned
 * out to this subscriber (published to non-matching topics, assigned to a
 * different group member, filtered replay) so the stream advances past
 * them instead of hanging; `buffers` holds early arrivals (clock >
 * expected, not skipped) per source until their dependencies are
 * admitted. Each per-source buffer is bounded by `maxBufferPerSource`.
 */
interface CausalGateState {
  maxBufferPerSource: number;
  expected: Map<string, number>;
  skipped: Map<string, Set<number>>;
  buffers: Map<string, Map<number, CausalDelivery>>;
}

/**
 * Key for one per-(subscriber, key) ordering stream, namespaced by
 * sequence epoch: the node's own publish stream ('') and each hub's
 * forwarded stream (`hub:<hubEpoch>`) order independently, so a keyed
 * message from a foreign epoch can neither wedge nor corrupt the local
 * gate. Epochs never contain NUL, so the first NUL splits unambiguously.
 */
function keyOrderKey(epoch: string, key: string): string {
  return `${epoch}\0${key}`;
}

/** Splits a `keyOrderKey` back into [epoch, key] on the first NUL. */
function splitKeyOrderKey(mapKey: string): [string, string] {
  const sep = mapKey.indexOf('\0');
  return [mapKey.slice(0, sep), mapKey.slice(sep + 1)];
}

interface Subscriber {
  id: string;
  pattern: string;
  /** Compiled form of `pattern`, shared with every subscriber on the pattern. */
  matcher: RegExp;
  /**
   * Namespace prefix this subscription was registered through (EB-52),
   * if any. The stored `pattern` is the translated `<prefix>/<pattern>`
   * form; deliveries name concrete `<prefix>/<topic>` topics.
   */
  namespacePrefix?: string;
  /**
   * Durable log this subscription replays from: the namespace's child
   * log for namespaced subscriptions, otherwise the bus's root log.
   * Undefined when the bus has no `durableLogDir`.
   */
  durableLog?: DurableTopicLog;
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
    /**
     * The failure that triggered the most recent redelivery of each
     * message: the handler's thrown error message, or the tracker's
     * reason (`'nack'` / `'ack-timeout'`) when nothing was thrown.
     * Read-and-cleared when a message is dead-lettered, so
     * `DeadLetterEntry.lastError` always describes the final failure.
     */
    lastFailure: WeakMap<BusMessage, string>;
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
   * Sequence epoch of the last delivery per topic, normalized ('' for the
   * node's own publishes, `hub:<hubEpoch>` for cluster traffic). Pairs
   * with `lastDeliveredSeq`: when a delivery arrives from a different
   * epoch than the previous one on the same topic, the gap baseline is
   * re-established instead of counting the epoch switch as lost messages.
   */
  lastEpoch: Map<string, string>;
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
   * Sliding-window delivery rate limit state (see
   * `SubscribeOptions.rateLimit`). Absent when the limit is disabled for
   * this subscriber.
   */
  rateLimit?: RateWindowState;
  /**
   * Delivery-latency sampling state (see `SubscribeOptions.deliveryLatency`).
   * Absent when sampling is disabled for this subscriber.
   */
  latency?: SubscriberLatencyState;
  /**
   * End-to-end ack-latency sampling state (see
   * `SubscribeOptions.ackLatency`). Absent when sampling is disabled for
   * this subscriber.
   */
  ackLatency?: SubscriberAckLatencyState;
  /**
   * Handler processing-latency p99 SLO state (see
   * `SubscribeOptions.latencySlo`). Absent when SLO tracking is disabled
   * for this subscriber.
   */
  latencySlo?: SubscriberLatencySloState;
  /**
   * Lag watermark monitoring state (see `SubscribeOptions.lagMonitor`).
   * Absent when monitoring is disabled for this subscriber.
   */
  lag?: SubscriberLagState;
  /**
   * Keyed hotspot monitoring state (see `SubscribeOptions.keyHotspot`).
   * Absent when monitoring is disabled for this subscriber.
   */
  keyHotspot?: SubscriberKeyHotspotState;
  /**
   * Per-(subscriber, key) publish-order state for keyed messages (see
   * `PublishOptions.key`). Created lazily — a subscriber that never
   * receives a keyed message pays nothing, and entries only exist for
   * keys actually fanned out to this subscriber.
   */
  keyOrder?: Map<string, KeyOrderState>;
  /**
   * Per-subscriber causal (happens-before) delivery state (see
   * `SubscribeOptions.causal`). Present only when the subscriber opted
   * in — without it the subscriber builds no buffers and tracks no
   * clocks: zero overhead, zero behavior change.
   */
  causal?: CausalGateState;
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
  /**
   * Subscriber-side exactly-once dedup window (see
   * `SubscribeOptions.deduplicateMessages`). Absent when dedup is
   * disabled for this subscriber.
   */
  dedup?: SubscriberDedupState;
}

/**
 * Per-subscriber exactly-once dedup window (see
 * `SubscribeOptions.deduplicateMessages`). `table` maps
 * `${topic}\0${messageId}` to the bus-clock timestamp of the first
 * delivery into this subscriber's queue; entries expire after
 * `windowMs`, and the table is bounded by `maxEntries` (oldest
 * evicted). `consumerId`, when set, journals every recorded entry to
 * the durable log so a restarted bus rehydrates the window.
 */
interface SubscriberDedupState {
  table: Map<string, number>;
  windowMs: number;
  maxEntries: number;
  consumerId?: string;
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
  diagnosticTopic?: string;
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

export function escapeRegExp(literal: string): string {
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
  lateMessages: number;
  duplicateMessages: number;
  filteredMessages: number;
  dedupDropped: number;
  aliasRetiredMessages: number;
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
    lateMessages: 0,
    duplicateMessages: 0,
    filteredMessages: 0,
    dedupDropped: 0,
    aliasRetiredMessages: 0,
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

/** Fail-fast validation for `PublishOptions.causal`. */
function validateCausal(causal: { source?: string; clock: number } | undefined, caller: string): void {
  if (causal === undefined) return;
  if (typeof causal !== 'object' || causal === null) {
    throw new RangeError(`${caller}: causal must be an object`);
  }
  const { source, clock } = causal;
  if (source !== undefined && (typeof source !== 'string' || source.length === 0)) {
    throw new RangeError(`${caller}: causal.source must be a non-empty string`);
  }
  if (!Number.isInteger(clock) || clock < 0) {
    throw new RangeError(`${caller}: causal.clock must be a non-negative integer`);
  }
}

/** An `AclRule` with its pattern pre-compiled for the hot path. */
interface CompiledAclRule {
  pattern: string;
  matcher: RegExp;
  publish?: AclDecision;
  subscribe?: AclDecision;
}

/**
 * Validates an ACL rule list the way a constructor would and pre-compiles
 * the patterns. Throws `RangeError` on the first problem — a broken rule
 * fails at configuration time, never mid-publish. Returns the compiled
 * rules in evaluation order.
 */
function normalizeAclRules(rules: AclRule[] | undefined, caller: string): CompiledAclRule[] {
  if (rules === undefined) return [];
  if (!Array.isArray(rules)) {
    throw new RangeError(`${caller}: acl.rules must be an array`);
  }
  return rules.map((rule, index) => {
    const at = `${caller}: acl.rules[${index}]`;
    if (typeof rule !== 'object' || rule === null) {
      throw new RangeError(`${at} must be an object`);
    }
    if (typeof rule.pattern !== 'string' || rule.pattern.length === 0) {
      throw new RangeError(`${at}.pattern must be a non-empty string`);
    }
    for (const key of ['publish', 'subscribe'] as const) {
      const decision = rule[key];
      if (decision !== undefined && decision !== 'allow' && decision !== 'deny') {
        throw new RangeError(`${at}.${key} must be 'allow' or 'deny'`);
      }
    }
    if (rule.publish === undefined && rule.subscribe === undefined) {
      throw new RangeError(`${at} must decide at least one of publish/subscribe`);
    }
    return {
      pattern: rule.pattern,
      matcher: compilePattern(rule.pattern),
      publish: rule.publish,
      subscribe: rule.subscribe,
    };
  });
}

/**
 * Renders a value a reliable handler threw as a short diagnostic string
 * for `DeadLetterEntry.lastError`. `Error` instances contribute their
 * message (not the stack — the DLQ record stays small and serializable);
 * anything else is stringified as-is.
 */
function thrownErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
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
): Pick<SubscriberDeadLetter, 'maxRedeliveries' | 'maxEntries' | 'onDeadLetter' | 'diagnosticTopic'> | null {
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
  const diagnosticTopic = o.diagnosticTopic;
  if (diagnosticTopic !== undefined && (typeof diagnosticTopic !== 'string' || diagnosticTopic.length === 0)) {
    throw new RangeError(`${caller}: deadLetter.diagnosticTopic must be a non-empty string`);
  }
  return { maxRedeliveries, maxEntries, onDeadLetter: o.onDeadLetter, diagnosticTopic };
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
 * Collapses runs of consecutive `**` segments into one, mirroring
 * `compilePattern` (`a.**.**.b` ≡ `a.**.b`).
 */
function collapseStars(segments: string[]): string[] {
  const out: string[] = [];
  for (const seg of segments) {
    if (seg === '**' && out[out.length - 1] === '**') continue;
    out.push(seg);
  }
  return out;
}

/**
 * Whether two topic patterns can match at least one common topic — the
 * overlap test the broker-level ACL (EB-41) uses for subscriptions. `*`
 * matches any single segment, `**` any (possibly empty) run of segments,
 * other segments match literally; consecutive `**` collapse like in
 * `compilePattern`.
 *
 * Decided by a memoized segment DP: `dp(i, j)` asks whether the pattern
 * suffixes `a[i:]` and `b[j:]` share a common topic. A `**` on either side
 * matches zero segments (`dp(i+1, j)` / `dp(i, j+1)`) or consumes one
 * segment the other side also matches (`dp(i, j+1)` / `dp(i+1, j)`); a
 * `*` or two equal literals consume one segment each. Every call strictly
 * grows `i + j`, so the recursion terminates.
 */
export function patternsOverlap(a: string, b: string): boolean {
  const segsA = collapseStars(a.split('.'));
  const segsB = collapseStars(b.split('.'));
  const memo = new Map<string, boolean>();
  const dp = (i: number, j: number): boolean => {
    const key = `${i},${j}`;
    const cached = memo.get(key);
    if (cached !== undefined) return cached;
    let result: boolean;
    if (i === segsA.length && j === segsB.length) {
      result = true;
    } else if (i === segsA.length) {
      // `a` is exhausted: `b`'s tail must match zero segments.
      result = segsB.slice(j).every((s) => s === '**');
    } else if (j === segsB.length) {
      result = segsA.slice(i).every((s) => s === '**');
    } else {
      const x = segsA[i];
      const y = segsB[j];
      if (x === '**' && y === '**') {
        result = dp(i + 1, j) || dp(i, j + 1) || dp(i + 1, j + 1);
      } else if (x === '**') {
        result = dp(i + 1, j) || dp(i, j + 1);
      } else if (y === '**') {
        result = dp(i + 1, j) || dp(i, j + 1);
      } else if (x === '*' || y === '*' || x === y) {
        result = dp(i + 1, j + 1);
      } else {
        result = false;
      }
    }
    memo.set(key, result);
    return result;
  };
  return dp(0, 0);
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
 * Per-subscriber sliding-window delivery rate limit state (see
 * `SubscribeOptions.rateLimit`). The limiter counts actual deliveries in
 * the rolling window; `rateLimiting` is true while queued messages are
 * held back for lack of window budget, and `timer` is the pending re-flush
 * scheduled for when the oldest delivery slides out of the window.
 */
interface RateWindowState {
  limiter: SlidingWindowLimiter;
  /** True while queued messages are held back for lack of window budget. */
  rateLimiting: boolean;
  /** Pending re-flush timer, armed while `rateLimiting` is true. */
  timer?: ReturnType<typeof setTimeout>;
}

/**
 * Validates `SubscribeOptions.rateLimit` and builds the initial
 * per-subscriber rate-limit state. Returns `undefined` when the limit is
 * disabled. Throws `RangeError` for invalid bounds.
 */
function resolveRateLimitOptions(opt: RateLimitOptions | undefined): RateWindowState | undefined {
  if (opt == null) return undefined;
  if (typeof opt !== 'object') {
    throw new RangeError('rateLimit must be a RateLimitOptions object with maxMessages and perWindowMs');
  }
  const maxMessages = opt.maxMessages;
  if (!Number.isInteger(maxMessages) || maxMessages < 1) {
    throw new RangeError('rateLimit.maxMessages must be an integer >= 1');
  }
  const perWindowMs = opt.perWindowMs;
  if (!Number.isFinite(perWindowMs) || perWindowMs <= 0) {
    throw new RangeError('rateLimit.perWindowMs must be a positive finite number of milliseconds');
  }
  return {
    limiter: new SlidingWindowLimiter(maxMessages, perWindowMs),
    rateLimiting: false,
    timer: undefined,
  };
}

/**
 * Resolves `SubscribeOptions.causal` into the subscriber's gate state.
 * `undefined`/`false` disables the gate (no buffers, no clock tracking —
 * zero overhead); `true` takes the defaults; an object tunes the
 * per-source buffer bound. Anything else throws `RangeError`.
 */
function resolveCausalOptions(
  causal: boolean | CausalSubscribeOptions | undefined,
): CausalGateState | undefined {
  if (causal === undefined || causal === false) return undefined;
  let maxBufferPerSource = 1000;
  if (causal !== true) {
    if (typeof causal !== 'object' || causal === null) {
      throw new RangeError('subscribe: causal must be a boolean or a CausalSubscribeOptions object');
    }
    if (causal.maxBufferPerSource !== undefined) {
      if (!Number.isInteger(causal.maxBufferPerSource) || causal.maxBufferPerSource < 1) {
        throw new RangeError('subscribe: causal.maxBufferPerSource must be a positive integer');
      }
      maxBufferPerSource = causal.maxBufferPerSource;
    }
  }
  return {
    maxBufferPerSource,
    expected: new Map(),
    skipped: new Map(),
    buffers: new Map(),
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
 * Per-subscriber end-to-end ack-latency sampling state (see
 * `SubscribeOptions.ackLatency`). The tracker's per-message accept clocks
 * are keyed by message identity (WeakMap), so entries die with their
 * messages and no cleanup is needed on unsubscribe.
 */
interface SubscriberAckLatencyState {
  tracker: AckLatencyTracker;
}

/**
 * Validates `SubscribeOptions.ackLatency` / `ackSloMs` / `onAckSloMiss`
 * and builds the initial per-subscriber sampling state. Returns
 * `undefined` when sampling is disabled. Throws `RangeError` for an
 * invalid window size, an invalid SLO, or SLO options provided without
 * the opt-in (a value that could never take effect); throws `TypeError`
 * for a non-function `onAckSloMiss`.
 */
function resolveAckLatencyOptions(
  opt: boolean | AckLatencyOptions | undefined,
  ackSloMs: number | undefined,
  onAckSloMiss: ((event: AckSloMiss) => void) | undefined,
  subscriberId: string,
  pattern: string,
  now: () => number,
): SubscriberAckLatencyState | undefined {
  if (ackSloMs !== undefined && (!Number.isFinite(ackSloMs) || ackSloMs <= 0)) {
    throw new RangeError('ackSloMs must be a positive finite number of milliseconds');
  }
  if (onAckSloMiss !== undefined && typeof onAckSloMiss !== 'function') {
    throw new TypeError('onAckSloMiss must be a function');
  }
  if (opt == null || opt === false) {
    if (ackSloMs !== undefined || onAckSloMiss !== undefined) {
      throw new RangeError('ackSloMs/onAckSloMiss require ackLatency to be enabled');
    }
    return undefined;
  }
  const o: AckLatencyOptions = opt === true ? {} : opt;
  const windowSize = o.windowSize ?? 1024;
  if (!Number.isInteger(windowSize) || windowSize <= 0) {
    throw new RangeError('ackLatency.windowSize must be a positive integer');
  }
  return {
    tracker: new AckLatencyTracker({
      windowSize,
      sloMs: ackSloMs ?? 30000,
      now,
      onSloMiss: (event) =>
        onAckSloMiss?.({ subscriberId, pattern, ...event }),
    }),
  };
}

/**
 * Per-subscriber handler processing-latency SLO state (see
 * `SubscribeOptions.latencySlo`). The tracker is EB-31's
 * `DeliveryLatencyTracker` reused for processing-time deltas — the bus
 * computes each delta from its injected clock, so the tracker stays
 * clock-free. `alerted` is the excursion latch (see
 * `checkProcessingLatencySlo`): set while the windowed p99 is above the
 * threshold, cleared when it drops back to or below it.
 */
interface SubscriberLatencySloState {
  tracker: DeliveryLatencyTracker;
  thresholdMs: number;
  alerted: boolean;
  onLatencySloMiss?: (event: LatencySloMissEvent) => void;
}

/**
 * Validates `SubscribeOptions.latencySlo` and builds the initial
 * per-subscriber SLO state. Returns `undefined` when the feature is
 * disabled. Throws `RangeError` for a missing/invalid `p99ThresholdMs`
 * or an invalid window size; throws `TypeError` for a non-function
 * `onLatencySloMiss`. Unlike `ackSloMs`, the threshold has no default —
 * an SLO with no threshold could never fire, so omitting it is a
 * configuration error, not a silent opt-out.
 */
function resolveLatencySloOptions(
  opt: ProcessingLatencySloOptions | undefined,
): SubscriberLatencySloState | undefined {
  if (opt == null) return undefined;
  if (typeof opt !== 'object') {
    throw new TypeError('latencySlo must be an options object');
  }
  const windowSize = opt.windowSize ?? 1024;
  if (!Number.isInteger(windowSize) || windowSize <= 0) {
    throw new RangeError('latencySlo.windowSize must be a positive integer');
  }
  const thresholdMs = opt.p99ThresholdMs;
  if (!Number.isFinite(thresholdMs) || thresholdMs <= 0) {
    throw new RangeError(
      'latencySlo.p99ThresholdMs must be a positive finite number of milliseconds',
    );
  }
  const onLatencySloMiss = opt.onLatencySloMiss;
  if (onLatencySloMiss !== undefined && typeof onLatencySloMiss !== 'function') {
    throw new TypeError('latencySlo.onLatencySloMiss must be a function');
  }
  return {
    tracker: new DeliveryLatencyTracker(windowSize),
    thresholdMs,
    alerted: false,
    onLatencySloMiss,
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
 * Resolves `SubscribeOptions.deduplicateMessages` into per-subscriber
 * dedup state (see `SubscriberDedupState`). `defaultWindowMs` is the
 * bus's publish-side idempotency window — the subscribe-side horizon
 * matches the publisher's retry horizon unless tuned. A `consumerId`
 * without a durable log throws: without a journal there is nothing to
 * persist the window to, so accepting the identity would silently
 * promise crash recovery it cannot deliver.
 */
function resolveDeduplicateMessagesOptions(
  opt: boolean | DeduplicateMessagesOptions | undefined,
  defaultWindowMs: number,
  hasDurableLog: boolean,
): SubscriberDedupState | undefined {
  if (opt == null || opt === false) return undefined;
  const o: DeduplicateMessagesOptions = opt === true ? {} : opt;
  const windowMs = o.windowMs ?? defaultWindowMs;
  if (!Number.isFinite(windowMs) || windowMs <= 0) {
    throw new RangeError('deduplicateMessages.windowMs must be a positive finite number of milliseconds');
  }
  const maxEntries = o.maxEntries ?? 10_000;
  if (!Number.isInteger(maxEntries) || maxEntries < 1) {
    throw new RangeError('deduplicateMessages.maxEntries must be a positive integer');
  }
  const consumerId = o.consumerId;
  if (consumerId !== undefined) {
    if (typeof consumerId !== 'string' || consumerId.length === 0) {
      throw new RangeError('deduplicateMessages.consumerId must be a non-empty string');
    }
    if (!hasDurableLog) {
      throw new RangeError(
        'deduplicateMessages.consumerId requires EventBusOptions.durableLogDir: ' +
          'the dedup window is persisted to the durable log directory',
      );
    }
  }
  return { table: new Map(), windowMs, maxEntries, consumerId };
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
 * Why one entry of an atomic batch was rejected: the ACL denied the publish
 * (`'acl'`), its payload failed the topic's schema validator (`'schema'`),
 * or the topic's rate-limit bucket had no token left for it
 * (`'rate-limit'`). TTL expiry is drain-time and never rejects a publish,
 * so it cannot appear here.
 */
export type AtomicRejectReason = 'acl' | 'schema' | 'rate-limit' | 'alias-retired';

/**
 * Which publish-side admission gate dropped a publish — the unified reason
 * space for `EventBusOptions.onAdmissionRejected`. Each reason maps 1:1
 * onto a stats counter, so hook events reconcile exactly against
 * `getStats()`:
 * - `'acl'` → `TopicStats.rejectedMessages` / `BusStats.rejectedMessages`
 *   (broker-level ACL denied the publish, EB-41; shares the rejection
 *   counter with `'schema'`, distinguished by the reason)
 * - `'schema'` → `TopicStats.rejectedMessages` / `BusStats.rejectedMessages`
 *   (payload failed the topic's schema validator, EB-20)
 * - `'rate-limit'` → `TopicStats.rateLimitedMessages` /
 *   `BusStats.rateLimitedMessages` (topic token bucket empty, EB-19)
 * - `'duplicate'` → `TopicStats.duplicateMessages` /
 *   `BusStats.duplicateMessages` (idempotent-publish suppression, EB-27)
 * - `'alias-retired'` → `TopicStats.aliasRetiredMessages` /
 *   `BusStats.aliasRetiredMessages` (publish to a topic whose migration
 *   alias expired — the old topic is read-only, EB-44)
 *
 * A `publishAtomic` batch rejection surfaces as the failing entry's
 * underlying gate reason (`'acl'` | `'schema'` | `'rate-limit'` |
 * `'alias-retired'`); the batch rejection
 * is counted once against that entry's topic, so it stays reconcilable
 * too.
 */
export type AdmissionRejectReason = 'acl' | 'schema' | 'rate-limit' | 'duplicate' | 'alias-retired';

/**
 * Snapshot delivered to `EventBusOptions.onAdmissionRejected` for every
 * publish dropped by the publish-side admission gates. Carries the
 * rejected payload's size, not the payload itself — the hook is an
 * observability channel, and large or sensitive payloads never travel
 * through it.
 */
export interface AdmissionRejectionEvent {
  /** The concrete topic the rejected publish targeted (never a pattern). */
  topic: string;
  /**
   * Which admission gate rejected it — maps 1:1 onto a stats counter
   * (see `AdmissionRejectReason`).
   */
  reason: AdmissionRejectReason;
  /**
   * UTF-8 JSON byte size of the rejected payload. 0 when the payload has
   * no JSON encoding (`undefined`, functions, circular structures,
   * BigInt).
   */
  payloadBytes: number;
  /** Bus-clock timestamp of the rejection (`EventBusOptions.now`). */
  at: number;
}

/**
 * Publish-side admission-rejection hook (see
 * `EventBusOptions.onAdmissionRejected`). The callback is error-isolated:
 * a throwing callback is swallowed so a broken observer can never disturb
 * the publish path.
 */
export type AdmissionRejectionCallback = (event: AdmissionRejectionEvent) => void;

/**
 * Options for `EventBus.setTopicAlias`: how long the migration alias stays
 * live.
 */
export interface TopicAliasOptions {
  /**
   * How long the alias stays live, in milliseconds on the bus clock
   * (`EventBusOptions.now`), measured from registration. While live,
   * publishes to `oldTopic` resolve to `newTopic` — transparently, before
   * every other admission gate — and fan-out to `newTopic` additionally
   * reaches subscribers of `oldTopic` (the dual-write migration window).
   * Once the TTL passes, the alias is retired: the old topic becomes
   * read-only and publishes to it are rejected with admission reason
   * `'alias-retired'` (see `AdmissionRejectReason`) — they consume no
   * sequence number, never touch the durable log, and burn no rate-limit
   * budget. Absent means the alias never expires. Must be a finite number
   * `>= 0`; anything else throws `RangeError` from `setTopicAlias`.
   */
  ttlMs?: number;
}

/**
 * One registered topic alias (see `EventBus.setTopicAlias`), as exposed by
 * `getStats().aliases`. A snapshot — mutating it does not affect the bus.
 */
export interface TopicAliasInfo {
  /**
   * The migrating topic name: while the alias is live, publishes to it
   * resolve to `newTopic`; once retired, publishes to it are rejected
   * (admission reason `'alias-retired'`).
   */
  oldTopic: string;
  /** The migration target: the real publish topic while the alias is live. */
  newTopic: string;
  /**
   * Bus-clock timestamp (ms) when the alias retires. Absent when the alias
   * never expires.
   */
  expiresAt?: number;
  /**
   * True once `expiresAt` has passed on the bus clock — the alias no
   * longer forwards or mirrors, and the old topic is read-only.
   */
  expired: boolean;
}

/**
 * Filter for a topic route (see `EventBus.setTopicRoute`): decides which
 * admitted messages on the source topic are forwarded to the destination.
 * Receives the message payload and a small metadata record (`topic` is the
 * alias-resolved source topic, `seq` the message's per-topic sequence
 * number, `messageId` the envelope identity when the publish carried
 * one). Return `true` to forward, `false` to skip. A predicate that throws
 * propagates the error to the publish caller — like a throwing schema
 * validator — after the source message itself was admitted and fanned
 * out; keep predicates pure and total.
 */
export type RoutePredicate = (
  payload: unknown,
  meta: { topic: string; seq: number; messageId?: string },
) => boolean;

/** Options for `EventBus.setTopicRoute`. */
export interface TopicRouteOptions {
  /**
   * Optional per-message filter (see `RoutePredicate`): only messages the
   * predicate accepts are forwarded. Absent means every admitted message
   * on the source topic is forwarded.
   */
  predicate?: RoutePredicate;
}

/**
 * One registered topic route (see `EventBus.setTopicRoute`), as exposed by
 * `getStats().routes`. A snapshot — mutating it does not affect the bus.
 */
export interface TopicRouteInfo {
  /** The source topic: admitted messages here are forwarded. */
  src: string;
  /** The destination topic: the forwarded publish's topic. */
  dst: string;
  /** Whether the route carries a predicate filter. */
  predicate: boolean;
  /**
   * How many source messages this route forwarded: incremented every time
   * the route matched and a destination publish was initiated — including
   * attempts the destination's admission gates (ACL, schema, rate-limit)
   * then rejected. Re-registering the route resets the count.
   */
  forwarded: number;
}

/** Options for `EventBus.forward`. */
export interface ForwardOptions {
  /**
   * The destination topic on the destination bus. Defaults to the
   * published (alias-resolved) topic when omitted.
   */
  dstTopic?: string;
}

/**
 * One registered cross-bus forward rule (see `EventBus.forward`), as
 * exposed by `getStats().forwards`. A snapshot — mutating it does not
 * affect the bus.
 */
export interface ForwardRuleInfo {
  /** Source pattern: admitted messages on matching topics are forwarded. */
  srcPattern: string;
  /**
   * Destination topic on the destination bus; absent when the rule
   * forwards to the published topic unchanged.
   */
  dstTopic?: string;
  /**
   * How many source messages this rule forwarded: incremented every time
   * the rule matched and a destination forward was initiated — including
   * attempts the destination's admission gates (ACL, schema, rate-limit)
   * then rejected. Re-registering the rule resets the count.
   */
  forwarded: number;
}

/**
 * The envelope carried across a cross-bus forward registration (EB-56),
 * from the source bus's forward rule to the destination bus's
 * `receiveForward`. INTERNAL USE ONLY — constructed solely by forward
 * registrations; application code must not build or send it.
 */
export interface ForwardFrame {
  /** Destination topic on the destination bus. */
  topic: string;
  /** The raw application payload. */
  payload: unknown;
  /**
   * Application message key — the destination bus draws its own per-key
   * sequence number, so per-key publish order is preserved per bus.
   */
  key?: string;
  /** Continues the source-side trace on the destination bus. */
  traceparent?: string;
  /** Application message identity, carried end to end. */
  messageId?: string;
  /**
   * The source message's TTL deadline, verbatim — a forward never resets
   * it. Absent when the source message had no deadline, in which case the
   * destination bus's TTL rules apply normally.
   */
  expiresAt?: number;
  /**
   * Business event time in epoch milliseconds (see
   * `PublishOptions.eventTime`): the forwarded message is the same logical
   * event, so its business time rides along verbatim — the destination bus
   * observes it into its own per-topic event-time watermark (EB-59).
   */
  eventTime?: number;
}

/**
 * Broker-level topic ACL (EB-41): per-topic publish/subscribe permissions
 * with allow/deny decisions on wildcard patterns.
 *
 * Rules are evaluated in registration order; the first rule whose pattern
 * matches — and that carries an explicit decision for the action being
 * checked — decides. A rule may decide only one action (the other falls
 * through to later rules). When no rule decides, `AclOptions.defaultPolicy`
 * applies (default `'allow'`, so a bus without ACL configured behaves
 * exactly as before).
 *
 * Matching differs by action, because a publish names a concrete topic
 * while a subscription names a pattern:
 * - publish: the rule pattern is matched against the concrete topic with
 *   the bus's usual wildcard semantics (`compilePattern`).
 * - subscribe: the rule pattern is matched against the *subscription
 *   pattern* with overlap semantics (`patternsOverlap`) — the rule governs
 *   the subscription when the two patterns can match at least one common
 *   topic. This is deliberately conservative for denies: a deny rule
 *   covering *part* of a subscription's scope denies the whole
 *   subscription, so a broad pattern cannot silently slip past a narrow
 *   deny. Design allow-lists with this in mind (prefer exact or narrow
 *   patterns for subscriptions under `defaultPolicy: 'deny'`).
 *
 * Unauthorized publishes are rejected before admission — `publish`
 * returns 0, the rejection is counted in `TopicStats.rejectedMessages`
 * (and the global total), surfaced on `onAdmissionRejected` with reason
 * `'acl'`, and audited via `onAuthzDenied`. Unauthorized subscribes throw
 * `AclDeniedError` before anything registers, and are audited via
 * `onAuthzDenied`. Rule changes via `setAclRules` take effect immediately
 * for subsequent publishes and subscribes — no cached verdicts.
 */
export type AclDecision = 'allow' | 'deny';

/** One ACL rule: a topic pattern with allow/deny decisions per action. */
export interface AclRule {
  /**
   * Topic pattern this rule governs (`*` = one segment, `**` = zero or
   * more segments, same syntax as `subscribe`). Must be non-empty.
   */
  pattern: string;
  /**
   * Publish decision for topics matching `pattern`; `undefined` means
   * this rule says nothing about publishing (falls through to later
   * rules). At least one of `publish`/`subscribe` must be set.
   */
  publish?: AclDecision;
  /**
   * Subscribe decision for subscription patterns overlapping `pattern`;
   * `undefined` falls through to later rules.
   */
  subscribe?: AclDecision;
}

/** ACL configuration for `EventBusOptions.acl`. */
export interface AclOptions {
  /**
   * Rules in evaluation order — first matching rule with an explicit
   * decision for the action wins. Invalid rules throw `RangeError` at
   * configuration time.
   */
  rules?: AclRule[];
  /**
   * Verdict when no rule decides an action. Default `'allow'` (a bus
   * without ACL behaves exactly as before); `'deny'` turns the rule set
   * into a whitelist.
   */
  defaultPolicy?: AclDecision;
}

/**
 * Thrown by `subscribe` (and the subscribe-family wrappers) when the
 * broker-level ACL denies the subscription. The denial is audited via
 * `EventBusOptions.onAuthzDenied` before the throw, so observers see it
 * even though the call never returns a subscription.
 */
export class AclDeniedError extends Error {
  /** The denied action — always `'subscribe'` (publishes return 0). */
  readonly action: 'publish' | 'subscribe';
  /** The subscription pattern that was denied. */
  readonly pattern: string;

  constructor(action: 'publish' | 'subscribe', pattern: string) {
    super(`ACL denied ${action} on pattern "${pattern}"`);
    this.name = 'AclDeniedError';
    this.action = action;
    this.pattern = pattern;
  }
}

/**
 * Authorization-denial audit event (`EventBusOptions.onAuthzDenied`): one
 * per denied publish or subscribe. Carries the action and the concrete
 * topic (publish) or subscription pattern (subscribe) — never the
 * payload. The callback is error-isolated: a throwing hook is swallowed
 * so a broken observer can never disturb the publish/subscribe path.
 */
export interface AuthzDeniedEvent {
  /** Which operation was denied. */
  action: 'publish' | 'subscribe';
  /** The concrete topic of a denied publish (absent for subscribes). */
  topic?: string;
  /** The subscription pattern of a denied subscribe (absent for publishes). */
  pattern?: string;
  /** Bus-clock timestamp of the denial (`EventBusOptions.now`). */
  at: number;
}

/** Authorization-denial audit hook (see `EventBusOptions.onAuthzDenied`). */
export type AuthzDeniedCallback = (event: AuthzDeniedEvent) => void;

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
   * Message key with two roles:
   *
   * 1. Compaction key for the durable log's keyed compaction (requires
   *    `EventBusOptions.durableLogKeyCompaction`, Kafka-style): only the
   *    latest record per (topic, key) is retained and replayed — publishing
   *    a new message with an existing key supersedes the old value. Use it
   *    for state snapshots (latest price per symbol, latest config per
   *    service) rather than event streams. Keyless messages are ordinary
   *    log entries.
   * 2. Publish-order key: every keyed message carries a per-key sequence
   *    number assigned by the bus at admission (publish order — schedule
   *    time for delayed deliveries, fan-out for direct publishes), and each
   *    subscriber receives same-key messages in strict publish order across
   *    topics. When a keyed message would arrive out of order (a delayed
   *    schedule fanning out after a live publish with a higher keySeq), the
   *    bus holds it in a per-(subscriber, key) reorder buffer — pre-queue,
   *    consuming no backpressure budget — until its predecessors are
   *    enqueued. Different keys have independent buffers and never block
   *    each other; unkeyed messages are unaffected.
   *
   * Must be a non-empty string when provided; anything else throws
   * `RangeError` from `publish`. The key is recorded on the durable-log
   * record regardless, but without `durableLogKeyCompaction` it has no
   * compaction effect (ordering always applies).
   */
  key?: string;
  /**
   * Causal clock for happens-before delivery (see
   * `SubscribeOptions.causal`). `clock` is the message's Lamport
   * timestamp within its source's causal stream: the producer numbers
   * each source's messages with monotonically increasing integers
   * starting at 0, and a causal subscriber delivers them in that order —
   * a message whose dependencies have not arrived yet waits in the
   * subscriber's reorder buffer instead of being delivered out of order.
   * `source` names the causal stream and defaults to the (alias-resolved)
   * topic; use an explicit source to order messages across topics (a
   * saga's steps, a multi-topic transaction). `clock` must be an integer
   * `>= 0`; anything else throws `RangeError` from `publish`. The clock
   * rides the durable-log record, so replay restores happens-before
   * order across restarts. Subscribers that did not opt into
   * `SubscribeOptions.causal` receive the message normally — the clock
   * is inert for them.
   */
  causal?: { source?: string; clock: number };
  /**
   * Upstream W3C `traceparent` header value (`00-<32hex trace
   * id>-<16hex span id>-<flags>`) for delivery tracing (see
   * `EventBusOptions.trace`): when tracing is enabled and the value is
   * valid, the publish continues that trace — every span of the delivery
   * shares the header's trace id and the `bus.publish` root span's
   * `parentId` is the header's span id. A missing or malformed value
   * mints a fresh trace id (the same lenient rule `webhook-relay-ts`
   * applies to its `x-trace-id` header); a non-string throws `RangeError`.
   * Inert when tracing is disabled.
   */
  traceparent?: string;
  /**
   * Application-level message identity for this publish. It is stamped
   * onto the message envelope (`BusMessage.messageId`) and carried end
   * to end — durable log, replay, cluster forwarding, redeliveries — so
   * subscriber-side dedup (`SubscribeOptions.deduplicateMessages`) can
   * recognize the same logical message across replays and redeliveries.
   * Unlike `publishIdempotent`'s `messageId` (which suppresses the
   * publish itself when the `(topic, messageId)` pair was already
   * admitted), this field never suppresses anything on the publish side:
   * it only labels the message for downstream dedup. A non-string or
   * empty value is treated as absent.
   */
  messageId?: string;
  /**
   * Marks this publish as already-routed (see `EventBus.setTopicRoute`):
   * a routed message never triggers topic routing again, so a route
   * chain (`a → b → c`) forwards one hop per message and a publish can
   * never be amplified into a loop by the route table. The bus sets this
   * on the forwarded publish itself; setting it on a direct publish opts
   * that publish out of routing (an escape hatch for producers that
   * already handled the fan-out themselves). Inert when no route matches
   * the topic.
   */
  routed?: boolean;
  /**
   * Multi-tenant namespace (EB-52, see `EventBus.createNamespace`): the
   * publish is scoped to the namespace — `topic` resolves to the
   * concrete topic `<namespace>/<topic>` and runs the full normal
   * admission pipeline (ACL, schema, rate limit, TTL, idempotency,
   * durable log) on that concrete topic. The topic itself must not
   * contain `/` (an attempted escape, e.g. `t2/x` with
   * `namespace: 't1'`, throws `NamespaceDeniedError`); an unknown
   * namespace throws `RangeError`. A namespace that disallows publishing
   * (`allowPublish: false`) rejects with `NamespaceDeniedError`.
   */
  namespace?: string;
  /**
   * Business event time of the message in epoch milliseconds — when the
   * event happened, not when the bus sees it (Kafka/Flink event-time
   * semantics, EB-59). The bus tracks a per-topic event-time watermark —
   * `max(eventTime) - allowedLatenessMs` — and treats a message with
   * `eventTime < watermark` as late: it is still delivered normally, but
   * it is counted (`TopicStats.lateMessages`, `BusStats.lateMessages`)
   * and reported on `EventBusOptions.onLate`. Out-of-order *arrival* with
   * on-time business times is not lateness — the watermark is orthogonal
   * to the publish-order sequence `BusMessage.seq` (EB-13). Publishes
   * without an event time never move the watermark. Stamped onto the
   * envelope (`BusMessage.eventTime`) and persisted on the durable-log
   * record so replay restores it. Must be a finite number `>= 0` when
   * provided; anything else throws `RangeError` from `publish`.
   */
  eventTime?: number;
}

/**
 * One message in a `publishBatch` / `publishAtomic` batch: topic and
 * payload, with the bus assigning `seq` at fan-out. `key` opts the message
 * into durable-log keyed compaction and per-key publish-order delivery
 * (see `PublishOptions.key`); `traceparent` continues an upstream trace
 * (see `PublishOptions.traceparent`).
 */
export type BatchMessage = Omit<BusMessage, 'seq'> & { key?: string; traceparent?: string };

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
   * record when the message fans out. The per-key sequence number is
   * assigned at schedule time (publish order), so a delayed keyed message
   * keeps its schedule-order position even when it fans out after live
   * publishes with higher keySeqs. Must be a non-empty string when
   * provided; anything else throws `RangeError`.
   */
  key?: string;
  /**
   * Upstream W3C `traceparent` header value, carried on the schedule and
   * applied when the message fans out (see `PublishOptions.traceparent`).
   * A non-string throws `RangeError` at schedule time.
   */
  traceparent?: string;
  /**
   * Application-level message identity, carried on the schedule and
   * stamped onto the envelope when the message fans out (see
   * `PublishOptions.messageId`). A non-string or empty value is treated
   * as absent.
   */
  messageId?: string;
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
  /**
   * Per-key sequence number assigned at schedule time (see
   * `DelayedEntry.keySeq`). Present exactly when `key` is present; the
   * fan-out must use it verbatim — reassigning at fan-out would renumber
   * the message into fan-out order and break publish-order delivery.
   */
  keySeq?: number;
  /**
   * Application-level message identity carried from the delayed
   * schedule into its fan-out (see `PublishDelayedOptions.messageId`):
   * stamped onto the envelope at fan-out, exactly as if it had been
   * published with `PublishOptions.messageId` at that moment.
   */
  messageId?: string;
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
  /**
   * Opt-in automatic dictionary training (EB-60): while enabled, the bus
   * samples the serialized payloads of admitted publishes on the topic
   * into a bounded sliding window and periodically derives a fresh preset
   * deflate dictionary from the recent corpus — no manual dictionary
   * building, zero new dependencies. Disabled by default; see
   * `CompressionAutoTrainOptions`.
   */
  autoTrain?: CompressionAutoTrainOptions;
}

/**
 * Tuning for automatic per-topic deflate-dictionary training (see
 * `EventBus.setTopicCompression`). While enabled, every admitted publish
 * on the topic contributes its serialized (UTF-8 JSON) payload to a
 * sliding sample window — bounded by BOTH `sampleCount` and
 * `sampleMaxBytes`, oldest evicted first, always retaining the newest
 * sample — and a background (unref'd) training pass derives a new
 * dictionary from the corpus whenever a retrain trigger fires.
 *
 * The derivation is a trailing-window heuristic: the samples are
 * concatenated and the last up-to-32 KiB are taken as the dictionary
 * candidate — a standard sampling-based approach that needs no new
 * dependencies, but honestly a heuristic: it captures the topic's recent
 * recurring byte patterns (field names, enum values, venue prefixes)
 * well, and a coverage-based trainer (like `zstd --train`) would do
 * better on adversarial shapes. Training never runs inline in the
 * publish path: publishes always compress with the dictionary in force
 * when they were admitted, and a completed training atomically swaps the
 * rule to the new dictionary (new SHA-256 `dictId`, version incremented)
 * while old dictionaries stay in the registry, so durable-log replay
 * fail-closed semantics are unchanged.
 */
export interface CompressionAutoTrainOptions {
  /**
   * How many of the most recent sampled payloads the training window
   * retains. Must be a positive integer.
   */
  sampleCount: number;
  /**
   * Maximum total serialized bytes retained across the window; oldest
   * samples are evicted first (the newest sample is always kept, even if
   * it alone exceeds the cap). Must be a positive finite number of bytes.
   */
  sampleMaxBytes: number;
  /**
   * Retrain after this many newly sampled publishes since the last
   * training completed. Must be a positive integer when given. Optional —
   * when neither trigger is set, the bus retrains on a 60-second cadence
   * instead.
   */
  retrainEveryMessages?: number;
  /**
   * Retrain cadence in milliseconds: the bus re-derives the dictionary at
   * most this often, and only when new samples arrived since the last
   * training. Must be a positive finite number of milliseconds when
   * given. Optional — defaults to 60_000 when `retrainEveryMessages` is
   * also unset.
   */
  retrainEveryMs?: number;
  /**
   * Called after every completed training with the new dictionary's
   * details. A throwing callback is swallowed — user code must never
   * break the bus — and the callback never keeps the process alive.
   */
  onTrained?: (event: CompressionAutoTrainedEvent) => void;
}

/**
 * Delivered to `CompressionAutoTrainOptions.onTrained` after a training
 * pass completes and the new dictionary is live on the rule.
 */
export interface CompressionAutoTrainedEvent {
  /** The `topicPattern` the rule was registered with. */
  topicPattern: string;
  /**
   * 1-based training count for this rule: increments on every completed
   * training, even when the derived bytes hash to the already-active
   * `dictId` (a stable corpus re-derives the same dictionary).
   */
  version: number;
  /** SHA-256 id of the newly registered dictionary (the EB-30 registry key). */
  dictionaryId: string;
  /** Samples in the window the dictionary was derived from. */
  sampleMessages: number;
  /** Serialized bytes across those samples. */
  sampleBytes: number;
  /** Size of the derived dictionary in bytes (≤ 32 KiB). */
  dictionaryBytes: number;
  /** Bus-clock time the training completed. */
  trainedAt: number;
}

/**
 * Per-topic auto-train state as exposed by `getStats()` — present on a
 * topic row only when the topic's matching compression rule has
 * `autoTrain` enabled.
 */
export interface CompressionAutoTrainState {
  /** Completed trainings for the current rule (0 = none yet). */
  version: number;
  /** SHA-256 id of the dictionary currently compressing publishes, if any. */
  dictionaryId?: string;
  /** Bus-clock time of the last completed training, if any. */
  lastTrainedAt?: number;
  /** Lifetime payloads admitted into the sample window. */
  sampledMessages: number;
  /** Serialized bytes currently retained in the window. */
  sampledBytes: number;
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
  /** Auto-trainer for the rule (EB-60), present when `autoTrain` was enabled. */
  autoTrainState?: CompressionAutoTrainer;
}

/**
 * Default auto-train retrain cadence: 60 seconds, used when neither
 * `retrainEveryMessages` nor `retrainEveryMs` is configured.
 */
const DEFAULT_AUTO_TRAIN_RETRAIN_MS = 60_000;

/** Resolved (validated, defaulted) form of `CompressionAutoTrainOptions`. */
interface ResolvedAutoTrainOptions {
  sampleCount: number;
  sampleMaxBytes: number;
  retrainEveryMessages?: number;
  retrainEveryMs?: number;
  onTrained?: (event: CompressionAutoTrainedEvent) => void;
}

/**
 * Validates `CompressionAutoTrainOptions` fail-fast with `RangeError`,
 * like every other options object on this bus. Returns the resolved
 * options with the retrain-cadence default applied.
 */
function resolveAutoTrainOptions(autoTrain: CompressionAutoTrainOptions, where: string): ResolvedAutoTrainOptions {
  if (autoTrain == null || typeof autoTrain !== 'object') {
    throw new RangeError(`${where}: autoTrain must be an object`);
  }
  const { sampleCount, sampleMaxBytes, retrainEveryMessages, retrainEveryMs, onTrained } = autoTrain;
  if (!Number.isInteger(sampleCount) || sampleCount < 1) {
    throw new RangeError(`${where}: autoTrain.sampleCount must be a positive integer`);
  }
  if (!Number.isFinite(sampleMaxBytes) || sampleMaxBytes <= 0) {
    throw new RangeError(`${where}: autoTrain.sampleMaxBytes must be a positive finite number of bytes`);
  }
  if (retrainEveryMessages !== undefined && (!Number.isInteger(retrainEveryMessages) || retrainEveryMessages < 1)) {
    throw new RangeError(`${where}: autoTrain.retrainEveryMessages must be a positive integer`);
  }
  if (retrainEveryMs !== undefined && (!Number.isFinite(retrainEveryMs) || retrainEveryMs <= 0)) {
    throw new RangeError(`${where}: autoTrain.retrainEveryMs must be a positive finite number of milliseconds`);
  }
  if (onTrained !== undefined && typeof onTrained !== 'function') {
    throw new RangeError(`${where}: autoTrain.onTrained must be a function`);
  }
  return {
    sampleCount,
    sampleMaxBytes,
    retrainEveryMessages,
    retrainEveryMs: retrainEveryMs ?? (retrainEveryMessages === undefined ? DEFAULT_AUTO_TRAIN_RETRAIN_MS : undefined),
    onTrained,
  };
}

/**
 * Marks a timer as not keeping the process alive, tolerating timer-like
 * objects without `unref` (the same defensive shape used by the delayed
 * delivery and rate-window timers).
 */
function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
  const handle = timer as unknown as { unref?: () => unknown };
  if (typeof handle.unref === 'function') handle.unref();
}

/**
 * Per-rule automatic dictionary trainer (EB-60). Owns the sample window,
 * the retrain triggers, and the background training pass; the bus owns
 * the rule table and the dictionary registry, reached through `hooks`.
 *
 * Threading model: training is DEFERRED, never inline. Both triggers —
 * `retrainEveryMessages` reached in the publish path, and the
 * `retrainEveryMs` cadence timer — only schedule a `setTimeout(0)` pass,
 * so the publish stack never pays derivation CPU and a publish admitted
 * while a training is pending still compresses with the old dictionary.
 * Single-threaded JS makes the swap atomic with respect to publishes:
 * the rule table entry is replaced between publish calls, never during
 * one.
 */
class CompressionAutoTrainer {
  private readonly pattern: string;
  private readonly config: ResolvedAutoTrainOptions;
  private readonly hooks: {
    now: () => number;
    registerDictionary: (id: string, dictionary: Buffer) => void;
    swapDictionary: (pattern: string, dictionary: Buffer, dictionaryId: string) => void;
  };
  /** Sliding window of serialized payloads, oldest first. */
  private samples: string[] = [];
  /** Serialized bytes currently retained in `samples`. */
  private sampleBytes = 0;
  /** Lifetime payloads admitted into the window (for stats). */
  private sampledMessagesTotal = 0;
  /** Samples admitted since the last completed training. */
  private sinceLastTrain = 0;
  private version = 0;
  private lastTrainedAt?: number;
  private dictionaryId?: string;
  /** Deferred training pass, if one is queued. */
  private pendingTimer?: ReturnType<typeof setTimeout>;
  private trainingQueued = false;
  /** Cadence timer for `retrainEveryMs`, if configured. */
  private cadenceTimer?: ReturnType<typeof setTimeout>;

  constructor(
    pattern: string,
    config: ResolvedAutoTrainOptions,
    hooks: {
      now: () => number;
      registerDictionary: (id: string, dictionary: Buffer) => void;
      swapDictionary: (pattern: string, dictionary: Buffer, dictionaryId: string) => void;
    },
    initialDictionaryId?: string,
  ) {
    this.pattern = pattern;
    this.config = config;
    this.hooks = hooks;
    this.dictionaryId = initialDictionaryId;
    if (config.retrainEveryMs !== undefined) this.armCadenceTimer();
  }

  /**
   * Admits one published payload into the sample window. Runs in the
   * publish path, so it is deliberately cheap: one serialization plus
   * amortized-constant window maintenance — no deflate, no hashing, no
   * timer work beyond arming. Unserializable payloads (see
   * `serializeToJson`) contribute nothing to the corpus.
   */
  sample(payload: unknown): void {
    const serialized = serializeToJson(payload);
    if (serialized === undefined) return;
    const bytes = Buffer.byteLength(serialized, 'utf8');
    this.samples.push(serialized);
    this.sampleBytes += bytes;
    this.sampledMessagesTotal += 1;
    this.sinceLastTrain += 1;
    // Dual-bounded window: evict oldest first, but always retain the
    // newest sample — a single payload larger than `sampleMaxBytes` still
    // trains, it just trains alone.
    while (
      this.samples.length > 1 &&
      (this.samples.length > this.config.sampleCount || this.sampleBytes > this.config.sampleMaxBytes)
    ) {
      const evicted = this.samples.shift();
      if (evicted !== undefined) this.sampleBytes -= Buffer.byteLength(evicted, 'utf8');
    }
    const every = this.config.retrainEveryMessages;
    if (every !== undefined && this.sinceLastTrain >= every) this.requestTraining();
  }

  /** Retires the trainer: drops a queued training and clears the cadence timer. */
  clear(): void {
    if (this.pendingTimer !== undefined) {
      clearTimeout(this.pendingTimer);
      this.pendingTimer = undefined;
    }
    this.trainingQueued = false;
    if (this.cadenceTimer !== undefined) {
      clearTimeout(this.cadenceTimer);
      this.cadenceTimer = undefined;
    }
  }

  /** Snapshot for `getStats()`. */
  snapshot(): CompressionAutoTrainState {
    return {
      version: this.version,
      ...(this.dictionaryId === undefined ? {} : { dictionaryId: this.dictionaryId }),
      ...(this.lastTrainedAt === undefined ? {} : { lastTrainedAt: this.lastTrainedAt }),
      sampledMessages: this.sampledMessagesTotal,
      sampledBytes: this.sampleBytes,
    };
  }

  /**
   * Schedules one training pass on an unref'd zero-delay timer — off the
   * publish stack, and deduped so a burst of publishes past the message
   * trigger queues exactly one pass.
   */
  private requestTraining(): void {
    if (this.trainingQueued) return;
    this.trainingQueued = true;
    const timer = setTimeout(() => {
      this.trainingQueued = false;
      this.pendingTimer = undefined;
      this.train();
    }, 0);
    unrefTimer(timer);
    this.pendingTimer = timer;
  }

  /** Arms (or re-arms) the wall-clock cadence timer. Unref'd: a training cadence never keeps the process alive. */
  private armCadenceTimer(): void {
    const everyMs = this.config.retrainEveryMs;
    if (everyMs === undefined) return;
    const timer = setTimeout(() => {
      this.cadenceTimer = undefined;
      // Only train on fresh signal: a quiet topic re-derives nothing.
      if (this.sinceLastTrain > 0) this.requestTraining();
      this.armCadenceTimer();
    }, Math.min(everyMs, 2_147_483_647));
    unrefTimer(timer);
    this.cadenceTimer = timer;
  }

  /**
   * Derives a dictionary from the current window and swaps it into the
   * rule through the EB-30 registration path. Runs on the background
   * timer, never in the publish path.
   */
  private train(): void {
    if (this.sinceLastTrain === 0 || this.samples.length === 0) return;
    // Trailing-window heuristic: the corpus's most recent bytes carry the
    // topic's current recurring patterns. Documented as a heuristic, not
    // a coverage-based trainer — see `CompressionAutoTrainOptions`.
    let dictionary = Buffer.from(this.samples.join('\n'), 'utf8');
    if (dictionary.length > MAX_COMPRESSION_DICTIONARY_BYTES) {
      dictionary = dictionary.subarray(dictionary.length - MAX_COMPRESSION_DICTIONARY_BYTES);
    }
    const dictionaryId = createHash('sha256').update(dictionary).digest('hex');
    // EB-30 registration: the id is new (or re-registered idempotently
    // when the corpus re-derives identical bytes), and old dictionaries
    // stay in the registry — replay fail-closed semantics unchanged.
    this.hooks.registerDictionary(dictionaryId, dictionary);
    this.hooks.swapDictionary(this.pattern, dictionary, dictionaryId);
    this.version += 1;
    this.lastTrainedAt = this.hooks.now();
    this.dictionaryId = dictionaryId;
    this.sinceLastTrain = 0;
    const onTrained = this.config.onTrained;
    if (onTrained !== undefined) {
      const event: CompressionAutoTrainedEvent = {
        topicPattern: this.pattern,
        version: this.version,
        dictionaryId,
        sampleMessages: this.samples.length,
        sampleBytes: this.sampleBytes,
        dictionaryBytes: dictionary.length,
        trainedAt: this.lastTrainedAt,
      };
      try {
        onTrained(event);
      } catch {
        // Swallowed: a user callback must never break the bus (same rule
        // as `onGroupLag`).
      }
    }
  }
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
 * Serializes a payload to its canonical byte form: the UTF-8 JSON
 * encoding. Returns `undefined` for payloads with no JSON encoding
 * (`undefined`, functions, symbols) or ones `JSON.stringify` cannot
 * represent (circular structures, BigInt) — exactly as the durable log
 * already treats them.
 */
function serializeToJson(payload: unknown): string | undefined {
  try {
    const serialized = JSON.stringify(payload);
    return typeof serialized === 'string' ? serialized : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Estimates a payload's buffered byte size as JSON UTF-8 bytes — the
 * estimator the bus hands to byte-budgeted backpressure queues (see
 * `SubscribeOptions.queueMaxBytes`). Unserializable payloads estimate as
 * 0 (see `serializeToJson`): the byte budget is a JSON-shape measure, and
 * pretending an unknown size is a real number would corrupt it.
 */
function payloadByteSize(payload: unknown): number {
  const serialized = serializeToJson(payload);
  return serialized === undefined ? 0 : Buffer.byteLength(serialized, 'utf8');
}

/**
 * Options for `EventBus.publishIdempotent`: `PublishOptions` plus the
 * idempotency key.
 *
 * The idempotency key is `PublishOptions.messageId`: the dedup identity
 * is the pair `(topic, messageId)` — the same `messageId` on different
 * topics is independent, so one payment id can be reused across unrelated
 * topics without interference.
 *
 * Absent or empty disables dedup — the publish behaves exactly like
 * `publish`, still returning an `IdempotentPublishResult` with
 * `duplicate: false`. A non-string value is treated as absent.
 */
export interface IdempotentPublishOptions extends PublishOptions {}

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

/**
 * One durable-log record as the bus replays it. `dictId` rides along for
 * dictionary-compressed records (resolved from the registry at admit).
 */
interface ReplayRecord {
  seq: number;
  topic: string;
  at: number;
  expiresAt?: number;
  payload: unknown;
  key?: string;
  keySeq?: number;
  dictId?: string;
  messageId?: string;
  eventTime?: number;
  causal?: { source: string; clock: number };
}

/**
 * Resolved namespace scope for one subscription (EB-52): everything the
 * subscribe path needs after `resolveNamespaceForSubscribe` translates
 * the user's pattern into the concrete namespaced topic space.
 */
interface NamespaceSubscriptionScope {
  /** The namespace prefix. */
  prefix: string;
  /** Registered pattern string: `<prefix>/<pattern>`. */
  internalPattern: string;
  /** Matcher over concrete `<prefix>/<topic>` topics. */
  matcher: RegExp;
  /** Publish-side prefix-index key (EB-15 no-miss invariant). */
  indexKey: string;
  /** Durable log replays read from (undefined without `durableLogDir`). */
  log?: DurableTopicLog;
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
      dedupDropped: number;
      aliasRetiredMessages: number;
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
   * Next per-key sequence number to assign, per key. KeySeqs are assigned
   * at admission — in publish order: direct publishes assign in `fanOut`
   * after the admission gates; `publishAtomic` draws from a shadow copy so
   * a rejected batch consumes nothing; `publishDelayed` assigns at schedule
   * time. Seeded from the durable log at construction so numbering never
   * restarts across restarts (mirrors the per-topic `topicSeq` recovery).
   */
  private keyCursors = new Map<string, number>();
  /**
   * Keyed messages held in per-(subscriber, key) reorder buffers because
   * an earlier keySeq had not been fanned out yet — the observable count
   * of publish-order enforcement (delayed schedules interleaved with live
   * publishes). Monotonic.
   */
  private totalKeyedReordered = 0;
  /**
   * Causal messages dropped from full per-source reorder buffers
   * (drop-oldest anti-deadlock) — the observable count of causal waits
   * that were abandoned instead of hanging. Monotonic.
   */
  private totalCausalDropped = 0;
  /**
   * Causal messages delivered immediately on clock regression (clock
   * below the source's expectation — a duplicate or a late arrival from
   * before the subscriber's horizon). Monotonic.
   */
  private totalCausalRegressed = 0;
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
   * Bus-wide default allowed lateness in milliseconds for event-time
   * watermark tracking (see `EventBusOptions.allowedLatenessMs`).
   * Per-topic overrides (`setTopicAllowedLateness`) win over this at
   * observation time.
   */
  private readonly allowedLatenessMs: number;
  /**
   * Bus-level late-message hook (see `EventBusOptions.onLate`), fired
   * error-isolated once per late publish.
   */
  private readonly onLate?: LateMessageCallback;
  /**
   * Per-topic allowed-lateness overrides, keyed by the exact string passed
   * to `setTopicAllowedLateness` — either a concrete topic name or a
   * wildcard pattern. Match resolution mirrors the TTL and rate-limit
   * rules: an exact-topic rule wins over any pattern; between patterns the
   * earliest-registered rule wins.
   */
  private allowedLatenessRules = new Map<string, number>();
  /**
   * Compiled matchers for the wildcard entries in
   * `allowedLatenessRules`, kept apart from the subscriber pattern cache
   * so lateness configuration never inflates `patternCacheSize`.
   */
  private allowedLatenessMatcherCache = new Map<string, RegExp>();
  /**
   * Per-topic event-time watermark state (EB-59). An entry appears on the
   * first admitted publish to the topic that carries an event time and
   * persists for the bus's lifetime — topics without event-time publishes
   * never appear here, and their watermark reads as unknown. Bounded by
   * the distinct topics ever published with an event time, the same bound
   * as `topicStats`.
   */
  private readonly eventWatermarks = new Map<string, EventTimeWatermark>();
  /**
   * Messages admitted with a business event time older than their topic's
   * event-time watermark (global total). Late messages are still
   * delivered — this is the bus-wide data-quality signal.
   */
  private totalLateMessages = 0;
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
   * Unified publish-side admission-rejection hook
   * (`EventBusOptions.onAdmissionRejected`), or `undefined` when unset —
   * the bus is then exactly as before (stats counting only).
   */
  private readonly onAdmissionRejected: AdmissionRejectionCallback | undefined;
  /**
   * Authorization-denial audit hook (`EventBusOptions.onAuthzDenied`), or
   * `undefined` when unset.
   */
  private readonly onAuthzDenied: AuthzDeniedCallback | undefined;
  /**
   * Delivery-trace recorder (`EventBusOptions.trace`), or `undefined`
   * when tracing is disabled — every instrumentation site checks this one
   * field first, which is what keeps the disabled path allocation-free.
   */
  private readonly trace: TraceRecorder | undefined;
  /**
   * Compiled broker-level ACL rules in evaluation order
   * (`EventBusOptions.acl` / `setAclRules`). Empty means no ACL — every
   * verdict falls through to `aclDefault`.
   */
  private aclRules: CompiledAclRule[] = [];
  /** Verdict when no ACL rule decides an action (default `'allow'`). */
  private aclDefault: AclDecision = 'allow';
  /**
   * Registered topic aliases (EB-44), keyed by old topic: while live,
   * publishes to the old topic resolve to `newTopic` before every other
   * admission gate, and fan-out to the resolved topic additionally reaches
   * subscribers of the old topic (the dual-write migration window). An
   * expired entry retires the old topic instead: publishes to it are
   * rejected with admission reason `'alias-retired'` until the alias is
   * cleared or re-registered.
   */
  private topicAliases = new Map<string, { newTopic: string; expiresAt?: number }>();
  /**
   * Reverse alias index: resolved (post-walk) topic → the live old topics
   * whose aliases resolve to it. Rebuilt by `setTopicAlias` /
   * `clearTopicAlias`; entries whose TTL lapsed since the rebuild are
   * filtered by `liveAliasSources`, so the index can never resurrect a
   * retired alias.
   */
  private aliasReverse = new Map<string, Set<string>>();
  /**
   * Registered topic routes (EB-50), keyed by source topic: at most one
   * route per source — re-registering replaces it (and resets its
   * forwarded count). A route fires when a non-routed message is admitted
   * on `src`: the message is forwarded to `dst` as a normal destination
   * publish (full dst admission, dst-side sequence and rate-limit budget),
   * carrying the source message's trace id and TTL deadline.
   */
  private topicRoutes = new Map<string, { dst: string; predicate?: RoutePredicate; forwarded: number }>();
  /**
   * Cross-bus forward rules (EB-56): per destination bus, per source
   * pattern, the forwarding table. Unlike `topicRoutes` (same-bus,
   * one route per source), a bus may forward to several destination
   * buses and hold several patterns per destination. A message admitted
   * on this bus and matching a rule's pattern is additionally offered to
   * that destination bus's `receiveForward` — the destination runs its
   * own full admission pipeline on it. The source pattern always matches
   * the published (alias-resolved) topic with the bus's wildcard
   * semantics (`compilePattern`).
   */
  private forwardRules = new Map<
    EventBus,
    Map<string, { srcPattern: string; matcher: RegExp; dstTopic?: string; forwarded: number }>
  >();
  /**
   * Publishes rejected because the target topic's migration alias expired
   * (global total, see `BusStats.aliasRetiredMessages`). Like a schema
   * rejection it consumes no sequence number, never touches the durable
   * log, and burns no rate-limit budget.
   */
  private totalAliasRetired = 0;
  /**
   * Publish/subscribe operations denied by the ACL (global total, see
   * `BusStats.authzDenied`). Denied publishes are additionally counted in
   * `totalRejected`; denied subscribes throw `AclDeniedError`.
   */
  private totalAuthzDenied = 0;
  /**
   * Messages skipped by subscriber content filters (global total, see
   * `SubscribeOptions.filter`). A filtered message was fanned out but
   * never entered the rejecting subscriber's queue — no backpressure
   * budget consumed, no sequence gap reported.
   */
  private totalFiltered = 0;
  /**
   * Total messages suppressed by subscriber-side exactly-once dedup
   * (global total, see `SubscribeOptions.deduplicateMessages`). A
   * suppressed duplicate was already delivered once — it never re-enters
   * the subscriber's queue, consumes no backpressure budget, and is
   * invisible to sequence-gap detection.
   */
  private totalDedupDropped = 0;
  /**
   * Total messages moved into subscriber dead-letter queues (see
   * `ReliableSubscribeOptions.deadLetter`). Counts every dead-lettering,
   * including replays that failed again.
   */
  private totalDeadLettered = 0;
  /**
   * Messages flagged as DLQ diagnostics at creation (see
   * `DeadLetterOptions.diagnosticTopic`). A dead-lettered diagnostic
   * message never emits a second diagnostic — the recursion cut. The mark
   * is bus-side so a user payload can never collide with it.
   */
  private readonly diagnosticMessages = new WeakSet<BusMessage>();
  /**
   * Total DLQ diagnostic events admitted to their diagnostic topic (see
   * `DeadLetterOptions.diagnosticTopic`). Counted on admission, not
   * delivery: the event exists even when nobody subscribes to the topic
   * yet. Admission rejections (ACL/schema) do not count.
   */
  private totalDiagnosticEvents = 0;
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
   * Active auto-trainers (EB-60), keyed by the exact `topicPattern` the
   * rule was registered with. One trainer per auto-trained rule; retired
   * (timers cleared) when the rule is replaced or cleared.
   */
  private compressionAutoTrainers = new Map<string, CompressionAutoTrainer>();
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
   * Consumer-group lag alert monitor (see `src/grouplag.ts`). Rows for
   * `getStats().groupLag` are always computed; `onGroupLag` alerting only
   * fires when `EventBusOptions.groupLag.onGroupLag` is configured.
   */
  private readonly groupLagMonitor: GroupLagMonitor;
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
  /**
   * Partition count per competing set (`groupKey(groupId, pattern)`), for
   * groups that opted into `GroupSubscribeOptions.partitions`. Fixed by
   * the group's first member; absent for classic round-robin groups.
   * Retained after the last member leaves so a rejoining group resumes
   * with the same partition count.
   */
  private groupPartitions = new Map<string, number>();
  /**
   * Cached rendezvous partition assignment per competing set:
   * partition -> owner member id. Invalidated on every join/leave.
   */
  private partitionAssignmentCache = new Map<string, Map<number, string>>();
  /**
   * Last sticky assignment per competing set (EB-54): the `previous` input
   * for the next sticky recomputation. Updated only when a sticky
   * assignment is (re)computed; dropped when the group fully drains so a
   * dead generation never pins partitions to departed members.
   */
  private previousPartitionAssignment = new Map<string, Map<number, string>>();
  /**
   * Partition assignment strategy per competing set
   * (`groupKey(groupId, pattern)`), for groups that opted into
   * `GroupSubscribeOptions.assignment`. Fixed by the group's first member
   * (like the partition count); absent means the default rendezvous.
   * Retained after the last member leaves, mirroring `groupPartitions`.
   */
  private groupAssignmentStrategy = new Map<string, 'rendezvous' | 'sticky'>();
  /**
   * Per-partition assignment watermarks: groupKey -> partition ->
   * concrete topic -> highest per-topic `seq` assigned to that partition.
   * Bounded by (groups x partitions x topics). Drives migration replay:
   * the new owner replays `(committed, watermark]` per topic.
   */
  private groupPartitionOffsets = new Map<string, Map<number, Map<string, number>>>();
  /**
   * Per-partition consumer checkpoints: groupKey -> partition ->
   * concrete topic -> last processed `seq` (see `commitOffset` with
   * `{ partition }`). Falls back to the group-level checkpoint when a
   * partition was never committed.
   */
  private partitionCommittedOffsets = new Map<string, Map<number, Map<string, number>>>();
  private readonly now: () => number;
  /**
   * Durable topic log, present only when `EventBusOptions.durableLogDir`
   * was given. Every `fanOut` appends the stamped message here; the
   * per-topic sequence counters are seeded from it at construction so a
   * restart never reuses a sequence number.
   */
  private readonly durableLog?: DurableTopicLog;
  /**
   * `durableLogMaxEntriesPerTopic` as configured — namespace child logs
   * (EB-52) open with the same per-topic budget as the root log.
   */
  private readonly durableLogMaxEntriesPerTopic?: number;
  /**
   * `durableLogKeyCompaction` as configured — namespace child logs
   * (EB-52) open with the same compaction mode as the root log.
   */
  private readonly durableLogKeyCompaction: boolean = false;
  /**
   * `durableLogSegmentRotation` as configured — namespace child logs
   * (EB-52) open with the same rotation triggers as the root log.
   */
  private readonly durableLogSegmentRotation?: SegmentRotationOptions;
  /**
   * Registered multi-tenant namespaces (EB-52): prefix → effective
   * flags. Concrete namespaced topics are `<prefix>/<topic>`; the `/`
   * separator is matched literally by the wildcard engine, so one
   * namespace's patterns structurally cannot cross into another's.
   */
  private readonly namespaces = new Map<string, { allowPublish: boolean; allowSubscribe: boolean }>();
  /**
   * Per-namespace child durable logs, opened under
   * `<durableLogDir>/namespaces/<url-encoded-prefix>/` when the bus has
   * a `durableLogDir`. Namespaced publishes append here; namespaced
   * replays read here. Present exactly for the registered namespaces
   * when the root log exists.
   */
  private readonly namespaceLogs = new Map<string, DurableTopicLog>();
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
  /**
   * Cluster hub link, present after `connectToHub` resolves and until
   * `disconnectCluster`. Loaded lazily (dynamic import) so buses that
   * never cluster never load the TCP/TLS code.
   */
  private clusterLink: ClusterLink | null = null;
  /**
   * Cross-process bridge config (see `src/bridge.ts`, EB-51), present when
   * `EventBusOptions.bridge` was given. The transport is caller-owned:
   * the bus never closes it.
   */
  private readonly bridge: ResolvedBridgeOptions | undefined;
  /** Envelopes mirrored to the bridge transport (see `BridgeStats`). */
  private bridgeOutbound = 0;
  /** Bridge envelopes admitted and fanned out locally. */
  private bridgeInbound = 0;
  /** Inbound bridge envelopes shed on a full ingress buffer. */
  private bridgeDropped = 0;
  /**
   * Bounded bridge ingress buffer. Envelopes queue here until the
   * microtask drain admits them; a full buffer sheds the newest envelope.
   */
  private readonly bridgeInboundQueue: BridgeEnvelope[] = [];
  private bridgeDrainScheduled = false;

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
    const onAdmissionRejected = options?.onAdmissionRejected;
    if (onAdmissionRejected !== undefined && typeof onAdmissionRejected !== 'function') {
      throw new RangeError('onAdmissionRejected must be a function');
    }
    this.onAdmissionRejected = onAdmissionRejected;
    const onAuthzDenied = options?.onAuthzDenied;
    if (onAuthzDenied !== undefined && typeof onAuthzDenied !== 'function') {
      throw new RangeError('onAuthzDenied must be a function');
    }
    this.onAuthzDenied = onAuthzDenied;
    const allowedLatenessMs = options?.allowedLatenessMs ?? 0;
    validateAllowedLatenessMs(allowedLatenessMs, 'EventBus');
    this.allowedLatenessMs = allowedLatenessMs;
    const onLate = options?.onLate;
    if (onLate !== undefined && typeof onLate !== 'function') {
      throw new TypeError('onLate must be a function');
    }
    this.onLate = onLate;
    const resolvedTrace = resolveTraceOptions(options?.trace, 'EventBus');
    this.trace = resolvedTrace === undefined ? undefined : new TraceRecorder(resolvedTrace);
    this.bridge = resolveBridgeOptions(options?.bridge, 'EventBus');
    this.groupLagMonitor = new GroupLagMonitor(resolveGroupLagOptions(options?.groupLag, 'EventBus'));
    const aclDefault = options?.acl?.defaultPolicy ?? 'allow';
    if (aclDefault !== 'allow' && aclDefault !== 'deny') {
      throw new RangeError("acl.defaultPolicy must be 'allow' or 'deny'");
    }
    this.aclDefault = aclDefault;
    this.aclRules = normalizeAclRules(options?.acl?.rules, 'EventBus');
    if (options?.durableLogDir != null) {
      const log = DurableTopicLog.open({
        dir: options.durableLogDir,
        maxEntriesPerTopic: options.durableLogMaxEntriesPerTopic,
        keyCompaction: options.durableLogKeyCompaction,
        segmentRotation: options.durableLogSegmentRotation,
      });
      this.durableLog = log;
      // Remembered for namespace child logs (EB-52): children open with
      // the same per-topic budget, compaction mode and rotation triggers
      // as the root log.
      this.durableLogMaxEntriesPerTopic = options.durableLogMaxEntriesPerTopic;
      this.durableLogKeyCompaction = options.durableLogKeyCompaction ?? false;
      this.durableLogSegmentRotation = options.durableLogSegmentRotation;
      // Recover numbering continuity and per-key cursors from the log —
      // shared with namespace child logs (see `recoverLogState`).
      this.recoverLogState(log);
      // Rebuild pending delayed-delivery timers from the log: schedules
      // that never fanned out (and were not cancelled) survive the
      // restart. Already-due entries fan out immediately.
      this.recoverDelayed();
      // Reseed consumer-group checkpoints from the offset journal: the
      // highest committed seq per checkpoint wins, so a restarted bus
      // resumes committed offsets instead of forgetting them — a rejoining
      // member that seeds `resumeFromSeq` from `getCommittedOffsets` picks
      // up exactly where its predecessor committed. Per-partition commits
      // (4-part keys) reseed the per-partition checkpoints.
      for (const [key, seq] of this.durableLog.recoveredCommittedOffsets()) {
        const parts = key.split('\0');
        if (parts.length === 4) {
          const [groupId, topic, partitionStr, pattern] = parts;
          const partition = Number(partitionStr);
          if (!Number.isInteger(partition) || partition < 0 || pattern.length === 0) continue;
          const groupKey = `${groupId}\0${pattern}`;
          let byPartition = this.partitionCommittedOffsets.get(groupKey);
          if (byPartition == null) {
            byPartition = new Map();
            this.partitionCommittedOffsets.set(groupKey, byPartition);
          }
          let byTopic = byPartition.get(partition);
          if (byTopic == null) {
            byTopic = new Map();
            byPartition.set(partition, byTopic);
          }
          byTopic.set(topic, seq);
          continue;
        }
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
   * Sets the allowed lateness in milliseconds for event-time watermark
   * tracking (EB-59) on one topic or topic pattern. `topicPattern`
   * accepts the same wildcard syntax as `subscribe` (or an exact topic
   * name): a message is late when its business event time
   * (`PublishOptions.eventTime`) is older than the topic's watermark
   * (`max(eventTime) - allowedLatenessMs`). Late messages are still
   * delivered normally — they are counted (`TopicStats.lateMessages`,
   * `BusStats.lateMessages`) and reported on `EventBusOptions.onLate`.
   *
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * any pattern; between patterns the earliest-registered rule wins.
   * Re-setting a rule replaces it; the change is reflected in the next
   * observation and in the next `getStats()` watermark reading
   * immediately — no restart, no re-observation. Throws when
   * `topicPattern` is empty or `allowedLatenessMs` is not a non-negative
   * finite number.
   */
  setTopicAllowedLateness(topicPattern: string, allowedLatenessMs: number): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    validateAllowedLatenessMs(allowedLatenessMs, 'setTopicAllowedLateness');
    this.allowedLatenessRules.set(topicPattern, allowedLatenessMs);
  }

  /**
   * Removes the allowed-lateness rule previously registered for
   * `topicPattern`. The topic falls back to the next matching rule, or to
   * the bus-wide default (`EventBusOptions.allowedLatenessMs`). Returns
   * true when a rule existed and was removed.
   */
  clearTopicAllowedLateness(topicPattern: string): boolean {
    return this.allowedLatenessRules.delete(topicPattern);
  }

  /**
   * Returns the allowed lateness in milliseconds that applies to `topic`:
   * the exact-topic rule first, then the earliest-registered matching
   * pattern, then the bus-wide default. Never undefined — the default is
   * always a valid number.
   */
  private allowedLatenessForTopic(topic: string): number {
    const exact = this.allowedLatenessRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, ms] of this.allowedLatenessRules) {
      let matcher = this.allowedLatenessMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.allowedLatenessMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return ms;
    }
    return this.allowedLatenessMs;
  }

  /**
   * Folds one admitted publish's event time into the topic's event-time
   * watermark (EB-59). A message older than the watermark is late: it is
   * counted on the topic and the bus total, and the bus-level `onLate`
   * hook fires — error-isolated, so a broken observer can never disturb
   * the publish path. Lateness never blocks delivery; that decision was
   * already made by the fan-out below.
   */
  private observeEventTime(topic: string, seq: number, eventTime: number): void {
    let state = this.eventWatermarks.get(topic);
    if (state == null) {
      state = new EventTimeWatermark();
      this.eventWatermarks.set(topic, state);
    }
    const allowedLatenessMs = this.allowedLatenessForTopic(topic);
    const observation = state.observe(eventTime, allowedLatenessMs);
    if (!observation.late) return;
    this.statsFor(topic).lateMessages += 1;
    this.totalLateMessages += 1;
    const onLate = this.onLate;
    if (onLate === undefined) return;
    try {
      // A late message never advances the watermark, so the watermark on
      // the event is exactly the one the message was judged against.
      onLate({ topic, seq, eventTime, watermark: observation.watermark, allowedLatenessMs });
    } catch {
      // Swallowed: a failing observer must never break publishing.
    }
  }

  /**
   * Registers a topic alias for zero-downtime topic migration: `oldTopic`
   * becomes an alias of `newTopic`. While the alias is live:
   * - publishes to `oldTopic` resolve to `newTopic` before every other
   *   admission gate (ACL, schema, rate-limit, TTL, compression, the
   *   durable log and the per-topic sequence all key off the resolved
   *   topic) — producers still writing the old name are transparently
   *   redirected;
   * - fan-out to `newTopic` additionally reaches subscribers of
   *   `oldTopic` (the dual-write window) — consumers still on the old
   *   name keep flowing. One fan-out pass tests both the resolved topic
   *   and the aliased old topics, so a subscriber matching via old and new
   *   patterns is still visited exactly once: the admitted message keeps a
   *   single `(topic, seq)` identity and is never double-delivered.
   *
   * Alias chains are supported: when `newTopic` is itself a live old
   * topic, publishes walk the whole live chain to its final target. A
   * registration that would close a cycle (`a → b` live, then `b → a`)
   * throws `RangeError`, as does a self-alias (`oldTopic === newTopic`).
   * Only live links count for cycle detection — an expired alias is inert
   * and can neither forward nor close a cycle.
   *
   * With `opts.ttlMs` the alias retires that many milliseconds after
   * registration (bus clock): the old topic becomes read-only and
   * publishes to it are rejected with admission reason `'alias-retired'`
   * (no sequence number consumed, counted in
   * `TopicStats.aliasRetiredMessages` / `BusStats.aliasRetiredMessages`,
   * surfaced on `onAdmissionRejected`). Without `ttlMs` the alias never
   * expires.
   *
   * Re-registering an existing `oldTopic` replaces its alias (and
   * restarts its TTL). Durable-log replay resolves record topics through
   * live aliases too, so a new consumer subscribing with the new topic
   * name replays history logged under the old name.
   *
   * Throws `RangeError` on empty topic names, a self-alias, a cycle, or
   * an invalid `ttlMs` — before anything is mutated.
   */
  setTopicAlias(oldTopic: string, newTopic: string, opts?: TopicAliasOptions): void {
    if (typeof oldTopic !== 'string' || oldTopic.length === 0) {
      throw new RangeError('oldTopic must be a non-empty string');
    }
    if (typeof newTopic !== 'string' || newTopic.length === 0) {
      throw new RangeError('newTopic must be a non-empty string');
    }
    if (oldTopic === newTopic) {
      throw new RangeError(`topic alias "${oldTopic}" cannot target itself`);
    }
    const ttlMs = opts?.ttlMs;
    if (ttlMs !== undefined && (!Number.isFinite(ttlMs) || ttlMs < 0)) {
      throw new RangeError('ttlMs must be a non-negative finite number of milliseconds');
    }
    // Cycle check: following the LIVE links from newTopic must not reach
    // oldTopic, or the new registration would close a forwarding loop.
    // The entry being replaced (if any) is ignored — it cannot be part of
    // the walk from newTopic unless some other live link points back to
    // oldTopic, which is exactly the cycle being rejected.
    let cursor: string | undefined = newTopic;
    const seen = new Set<string>();
    while (cursor !== undefined && !seen.has(cursor)) {
      if (cursor === oldTopic) {
        throw new RangeError(
          `topic alias "${oldTopic}" -> "${newTopic}" would close an alias cycle`,
        );
      }
      seen.add(cursor);
      const entry = this.topicAliases.get(cursor);
      cursor = entry !== undefined && this.aliasEntryLive(entry) ? entry.newTopic : undefined;
    }
    const expiresAt = ttlMs === undefined ? undefined : this.now() + ttlMs;
    this.topicAliases.set(oldTopic, { newTopic, expiresAt });
    this.rebuildAliasReverse();
  }

  /**
   * Removes the topic alias previously registered for `oldTopic`.
   * Returns true when an alias existed and was removed. After removal the
   * old topic is an ordinary topic again: publishes to it are no longer
   * resolved or rejected, and fan-out to the former target no longer
   * mirrors to its subscribers.
   */
  clearTopicAlias(oldTopic: string): boolean {
    const removed = this.topicAliases.delete(oldTopic);
    if (removed) this.rebuildAliasReverse();
    return removed;
  }

  /**
   * Registers a topic route: every message admitted on `src` is
   * automatically forwarded to `dst` (EB-50). The forward is a normal
   * `dst` publish — it goes through the destination's full admission
   * pipeline (broker-level ACL, schema validation, rate-limit budget) and
   * consumes the destination's sequence number and rate-limit budget, so
   * destination-side gates see the routed message exactly as if a
   * producer had published it there directly. The routed message keeps
   * the source message's identity where it matters:
   * - the end-to-end trace id is propagated (the forwarded publish
   *   continues the source message's trace — its `bus.publish` root span
   *   shares the trace id), and the application `key` / `messageId` ride
   *   along so per-key publish order (EB-36) and subscriber-side dedup
   *   keep working across the route;
   * - the TTL deadline is carried over verbatim — routing never resets
   *   it. When the source message had no deadline, the destination's TTL
   *   rules apply normally (ordinary `publish(dst)` semantics).
   *
   * The forwarded message is marked `routed` (`PublishOptions.routed`), so
   * it never triggers routing again: a route chain (`a → b → c`) forwards
   * one hop per message, and the route table can never amplify a message
   * into a loop. Registration additionally rejects cycles outright — a
   * route that would close a loop (`a → b` live, then `b → a`; longer
   * chains like `a → b → c → a` too) throws `RangeError`, as does a
   * self-route (`src === dst`).
   *
   * Routes are matched against the alias-resolved publish topic (see
   * `setTopicAlias`): like schema, rate-limit and TTL, routing keys off
   * the real topic, so a route registered on an old topic name does not
   * fire while that name is aliased elsewhere.
   *
   * With `opts.predicate` only messages the predicate accepts are
   * forwarded (see `RoutePredicate`); without it every admitted message
   * forwards. The predicate runs after the source message is admitted and
   * fanned out — a throwing predicate propagates to the publish caller,
   * like a throwing schema validator.
   *
   * Re-registering an existing `src` replaces its route (and resets its
   * forwarded count). `getStats().routes` exposes the live route table.
   *
   * Throws `RangeError` on empty topic names, a self-route, a cycle, or a
   * non-function `predicate` — before anything is mutated.
   */
  setTopicRoute(src: string, dst: string, opts?: TopicRouteOptions): void {
    if (typeof src !== 'string' || src.length === 0) {
      throw new RangeError('src must be a non-empty string');
    }
    if (typeof dst !== 'string' || dst.length === 0) {
      throw new RangeError('dst must be a non-empty string');
    }
    if (src === dst) {
      throw new RangeError(`topic route "${src}" cannot target itself`);
    }
    const predicate = opts?.predicate;
    if (predicate !== undefined && typeof predicate !== 'function') {
      throw new RangeError('predicate must be a function');
    }
    // Cycle check: following the registered routes from dst must not reach
    // src, or the new registration would close a forwarding loop. At most
    // one route exists per source, so the walk is a simple chain; reaching
    // src throws before the table is mutated.
    let cursor: string | undefined = dst;
    const seen = new Set<string>();
    while (cursor !== undefined && !seen.has(cursor)) {
      if (cursor === src) {
        throw new RangeError(
          `topic route "${src}" -> "${dst}" would close a routing cycle`,
        );
      }
      seen.add(cursor);
      cursor = this.topicRoutes.get(cursor)?.dst;
    }
    this.topicRoutes.set(src, { dst, predicate, forwarded: 0 });
  }

  /**
   * Removes the topic route previously registered for `src`. Returns true
   * when a route existed and was removed. After removal, publishes to
   * `src` are no longer forwarded.
   */
  clearTopicRoute(src: string): boolean {
    return this.topicRoutes.delete(src);
  }

  /**
   * Registers a cross-bus forward rule (EB-56): every admitted,
   * non-routed message on this bus whose alias-resolved topic matches
   * `srcPattern` is additionally forwarded to `dstBus` — on topic
   * `opts.dstTopic`, or the published topic when omitted.
   *
   * Same-process multi-instance fan-out. `setTopicRoute` is the same-bus
   * mechanism; EB-51's bridge is the cross-process one. The forwarded
   * message runs the destination bus's FULL admission pipeline (ACL,
   * schema, rate-limit, TTL, ...) exactly as if a producer published it
   * there directly, stamped `routed` so it never triggers the
   * destination's routes or forwards again — one hop per message, no
   * amplification. The forward carries the source message's trace id
   * (continues the same end-to-end trace), its application key and
   * message id, and its TTL deadline verbatim (a forward never resets
   * the deadline; when the source message had none, the destination's
   * TTL rules apply normally).
   *
   * `forwarded` counts every forward attempt — including attempts the
   * destination's admission gates then reject. Re-registering an
   * identical (`dstBus`, `srcPattern`) rule replaces it and resets its
   * `forwarded` count (mirrors `setTopicRoute`). `getStats().forwards`
   * exposes the live forward table.
   *
   * Throws before anything is mutated: `TypeError` when `dstBus` is not
   * an `EventBus`; `RangeError` on a self-forward, an empty or invalid
   * `srcPattern`, an empty `dstTopic`, or a registration that would
   * close a forwarding cycle (the forward graph is walked
   * bus-level, pattern-agnostic, so transitive A→B→C→A cycles are
   * caught too).
   */
  forward(dstBus: EventBus, srcPattern: string, opts?: ForwardOptions): void {
    if (!(dstBus instanceof EventBus)) {
      throw new TypeError('dstBus must be an EventBus instance');
    }
    if (dstBus === this) {
      throw new RangeError(
        'forward cannot target the same bus — use setTopicRoute for same-bus routing',
      );
    }
    if (typeof srcPattern !== 'string' || srcPattern.length === 0) {
      throw new RangeError('srcPattern must be a non-empty string');
    }
    const dstTopic = opts?.dstTopic;
    if (dstTopic !== undefined && (typeof dstTopic !== 'string' || dstTopic.length === 0)) {
      throw new RangeError('dstTopic must be a non-empty string when given');
    }
    // compilePattern is total for non-empty patterns (same wildcard
    // semantics as subscriptions); compiling here also surfaces any
    // future pattern-shape rejection before the table is mutated.
    const matcher = compilePattern(srcPattern);
    // Cycle check: walking the forward graph from dstBus must not reach
    // this bus, or the new rule would let a message loop back here.
    // Conservative and pattern-agnostic — one edge per registered rule —
    // so transitive cycles are caught the same way.
    const seen = new Set<EventBus>();
    const frontier: EventBus[] = [dstBus];
    while (frontier.length > 0) {
      const cursor = frontier.pop() as EventBus;
      if (cursor === this) {
        throw new RangeError('forward would close a cross-bus forwarding cycle');
      }
      if (seen.has(cursor)) continue;
      seen.add(cursor);
      for (const next of cursor.forwardRules.keys()) frontier.push(next);
    }
    let rules = this.forwardRules.get(dstBus);
    if (rules === undefined) {
      rules = new Map();
      this.forwardRules.set(dstBus, rules);
    }
    rules.set(srcPattern, { srcPattern, matcher, dstTopic, forwarded: 0 });
  }

  /**
   * Removes the forward rule previously registered for (`dstBus`,
   * `srcPattern`). Returns true when a rule existed and was removed.
   * After removal, messages matching `srcPattern` are no longer forwarded
   * to `dstBus`.
   */
  clearForward(dstBus: EventBus, srcPattern: string): boolean {
    const rules = this.forwardRules.get(dstBus);
    if (rules === undefined) return false;
    const removed = rules.delete(srcPattern);
    if (rules.size === 0) this.forwardRules.delete(dstBus);
    return removed;
  }

  /**
   * Receives one forwarded message from another bus's forward rule
   * (EB-56) — the destination-side half of `forward()`. INTERNAL USE
   * ONLY: only forward registrations call this; producers must use
   * `publish()`.
   *
   * The frame goes through this bus's full publish admission pipeline
   * (alias resolution, ACL, schema, rate-limit, TTL, ...) with this
   * bus's own sequence numbers and rate-limit budget, marked `routed` so
   * it never triggers this bus's routes or forwards again (one hop per
   * message). The source TTL deadline rides verbatim (never reset by a
   * forward); when the source message had none, this bus's TTL rules
   * apply normally. A malformed frame is dropped silently — admission
   * rejections are a normal, counted outcome, never an exception.
   */
  receiveForward(frame: ForwardFrame): void {
    if (frame === null || typeof frame !== 'object') return;
    const topic = frame.topic;
    if (typeof topic !== 'string' || topic.length === 0) return;
    validateEventTime(frame.eventTime, 'receiveForward');
    this.fanOut(
      topic,
      frame.payload,
      false, // preAdmitted: full destination admission — ACL, schema, rate-limit, TTL
      undefined, // delayed
      frame.key,
      undefined, // preassignedKeySeq: the destination draws its own key sequence
      frame.traceparent,
      frame.messageId,
      true, // routed: never triggers destination routes/forwards again
      frame.expiresAt, // verbatim TTL deadline from the source message
      false, // fromBridge
      false, // diagnostic
      frame.eventTime, // same logical event: business event time rides along verbatim
    );
    this.scheduleFlush();
  }

  /** True while the alias entry still forwards on the bus clock. */
  private aliasEntryLive(entry: { newTopic: string; expiresAt?: number }): boolean {
    return entry.expiresAt === undefined || this.now() < entry.expiresAt;
  }

  /**
   * Follows live alias links from `topic` to the final publish topic.
   * Expired links end the walk at their source — they are inert, so the
   * walk never forwards through a retired alias. The `seen` guard is
   * purely defensive: `setTopicAlias` rejects cycles at registration, so a
   * live cycle can never exist.
   */
  private walkLiveLinks(topic: string): string {
    let cursor = topic;
    const seen = new Set<string>([cursor]);
    for (;;) {
      const entry = this.topicAliases.get(cursor);
      if (entry === undefined || !this.aliasEntryLive(entry)) return cursor;
      const next = entry.newTopic;
      if (seen.has(next)) return cursor;
      seen.add(next);
      cursor = next;
    }
  }

  /**
   * Resolves `topic` through live alias links — the publish-side view. A
   * topic whose own alias entry expired is retired: publishes naming it
   * are rejected (admission reason `'alias-retired'`), never forwarded.
   * An expired link deeper in a chain only ends the walk at its source —
   * retirement gates the published name, not the whole chain.
   */
  private resolvePublishTopic(topic: string): { topic: string; retired: boolean } {
    const entry = this.topicAliases.get(topic);
    if (entry !== undefined && !this.aliasEntryLive(entry)) return { topic, retired: true };
    return { topic: this.walkLiveLinks(topic), retired: false };
  }

  /** Alias resolution for matching (fan-out, replay): never "retired" — an
   * expired link just ends the walk, so history stays attributable. */
  private resolveLiveTopic(topic: string): string {
    return this.walkLiveLinks(topic);
  }

  /**
   * Rebuilds the reverse alias index: resolved topic → the live old topics
   * that resolve to it. Only live entries are indexed; anything whose TTL
   * lapsed since the last rebuild is additionally filtered by
   * `liveAliasSources` at use time.
   */
  private rebuildAliasReverse(): void {
    this.aliasReverse.clear();
    for (const [oldTopic, entry] of this.topicAliases) {
      if (!this.aliasEntryLive(entry)) continue;
      const resolved = this.walkLiveLinks(oldTopic);
      let sources = this.aliasReverse.get(resolved);
      if (sources === undefined) {
        sources = new Set();
        this.aliasReverse.set(resolved, sources);
      }
      sources.add(oldTopic);
    }
  }

  /**
   * The live old topics whose aliases resolve to `resolvedTopic` — the
   * mirror set for fan-out and replay. Re-verifies liveness and the
   * resolved target at use time, so a TTL that lapsed since the last
   * index rebuild can never resurrect a retired alias.
   */
  private liveAliasSources(resolvedTopic: string): string[] {
    const sources = this.aliasReverse.get(resolvedTopic);
    if (sources === undefined || sources.size === 0) return [];
    const live: string[] = [];
    for (const oldTopic of sources) {
      const entry = this.topicAliases.get(oldTopic);
      if (entry === undefined || !this.aliasEntryLive(entry)) continue;
      if (this.walkLiveLinks(oldTopic) !== resolvedTopic) continue;
      live.push(oldTopic);
    }
    return live;
  }

  /**
   * Alias-aware topic matching for fan-out and durable-log replay: the
   * pattern matches when it matches the (resolved) topic itself or any
   * live old topic that resolves to it. `topic` is the concrete topic as
   * carried by the message/record, `effectiveTopic` its live-alias
   * resolution (identical when no alias applies).
   */
  private topicMatchesWithAliases(
    matcher: RegExp,
    topic: string,
    effectiveTopic: string,
  ): boolean {
    if (matcher.test(topic)) return true;
    if (effectiveTopic !== topic && matcher.test(effectiveTopic)) return true;
    for (const oldTopic of this.liveAliasSources(effectiveTopic)) {
      if (oldTopic !== topic && matcher.test(oldTopic)) return true;
    }
    return false;
  }

  /**
   * Counts one publish rejected because its topic's migration alias
   * expired (the old topic is read-only) against the topic and the global
   * total, then fires the admission-rejection hook with reason
   * `'alias-retired'`. Like a schema rejection it is dropped before
   * admission: no sequence number consumed (subscribers see no gap), the
   * durable log never sees it, no rate-limit budget burned.
   */
  private countAliasRetired(topic: string, payload: unknown): void {
    this.statsFor(topic).aliasRetiredMessages += 1;
    this.totalAliasRetired += 1;
    this.emitAdmissionRejected(topic, 'alias-retired', payload);
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
   * Replaces the broker-level ACL rule list (see `AclRule`). The new rules
   * take effect immediately: the very next `publish`/`subscribe` is judged
   * by them — no cached verdicts, no restart needed. This is the runtime
   * half of the ACL; the initial list comes from `EventBusOptions.acl`
   * (note: `defaultPolicy` is set once via the constructor options and is
   * not changed here).
   *
   * Rules are evaluated in array order; the first rule whose pattern
   * matches — with an explicit decision for the action — wins. Publish
   * checks match the rule pattern against the concrete topic; subscribe
   * checks use overlap semantics (`patternsOverlap`): a deny rule covering
   * any part of the subscription's scope denies the whole subscription.
   * Throws `RangeError` on invalid rules, before anything is replaced.
   */
  setAclRules(rules: AclRule[]): void {
    this.aclRules = normalizeAclRules(rules, 'setAclRules');
  }

  /**
   * Returns the current ACL rule list (a copy — mutating it changes
   * nothing; use `setAclRules` to replace).
   */
  getAclRules(): AclRule[] {
    return this.aclRules.map((rule) => ({
      pattern: rule.pattern,
      ...(rule.publish !== undefined ? { publish: rule.publish } : {}),
      ...(rule.subscribe !== undefined ? { subscribe: rule.subscribe } : {}),
    }));
  }

  /**
   * ACL verdict for a concrete publish topic: the first rule (in
   * registration order) whose pattern matches the topic and that carries
   * an explicit publish decision wins; when no rule decides, the default
   * policy applies.
   */
  private aclAllowsPublish(topic: string): boolean {
    for (const rule of this.aclRules) {
      if (rule.publish !== undefined && rule.matcher.test(topic)) {
        return rule.publish === 'allow';
      }
    }
    return this.aclDefault === 'allow';
  }

  /**
   * ACL verdict for a subscription pattern: the first rule (in
   * registration order) whose pattern overlaps the subscription pattern
   * (`patternsOverlap`) and that carries an explicit subscribe decision
   * wins; when no rule decides, the default policy applies.
   */
  private aclAllowsSubscribe(pattern: string): boolean {
    for (const rule of this.aclRules) {
      if (rule.subscribe !== undefined && patternsOverlap(rule.pattern, pattern)) {
        return rule.subscribe === 'allow';
      }
    }
    return this.aclDefault === 'allow';
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
    // Replacing a rule retires its auto-trainer first: the old cadence
    // and pending timers are cleared so a replaced rule can never train
    // again, and its samples are dropped with it.
    const previousTrainer = this.compressionAutoTrainers.get(topicPattern);
    if (previousTrainer !== undefined) {
      previousTrainer.clear();
      this.compressionAutoTrainers.delete(topicPattern);
    }
    let autoTrainState: CompressionAutoTrainer | undefined;
    if (opts.autoTrain !== undefined) {
      const resolved = resolveAutoTrainOptions(opts.autoTrain, `setTopicCompression("${topicPattern}")`);
      autoTrainState = new CompressionAutoTrainer(
        topicPattern,
        resolved,
        {
          now: () => this.now(),
          registerDictionary: (id, dict) => this.registerCompressionDictionary(id, dict),
          swapDictionary: (pattern, dict, id) => this.applyTrainedDictionary(pattern, dict, id),
        },
        dictionaryId,
      );
      this.compressionAutoTrainers.set(topicPattern, autoTrainState);
    }
    this.compressionRules.set(topicPattern, { thresholdBytes: opts.thresholdBytes, level, dictionary, dictionaryId, autoTrainState });
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
   * Atomically swaps a rule's preset dictionary after an auto-training
   * pass (EB-60), through the EB-30 registration path: the rule keeps
   * compressing with the new bytes under a new SHA-256 id, while every
   * previous dictionary stays in the registry — in-flight messages carry
   * their own bytes and durable-log replay keeps resolving old ids, so
   * fail-closed replay semantics are unchanged. The swap replaces the
   * rule-table entry between publishes (never during one), so no publish
   * ever compresses with half-old state. A no-op when the rule was
   * replaced or cleared while the training was queued.
   */
  private applyTrainedDictionary(pattern: string, dictionary: Buffer, dictionaryId: string): void {
    const rule = this.compressionRules.get(pattern);
    if (rule === undefined || rule.autoTrainState === undefined) return;
    if (this.compressionAutoTrainers.get(pattern) !== rule.autoTrainState) return;
    this.compressionRules.set(pattern, { ...rule, dictionary, dictionaryId });
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
    const trainer = this.compressionAutoTrainers.get(topicPattern);
    if (trainer !== undefined) {
      // Retire the auto-trainer with the rule: a queued training is
      // dropped, the cadence timer is cleared, and later publishes are no
      // longer sampled — a cleared rule never trains again.
      trainer.clear();
      this.compressionAutoTrainers.delete(topicPattern);
    }
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
    const serialized = serializeToJson(payload);
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

  // ------------------------------------------------------------------
  // Multi-tenant topic namespaces (EB-52)
  // ------------------------------------------------------------------

  /**
   * Registers a tenant namespace and returns its handle. From then on,
   * `handle.publish('orders', …)` publishes to the concrete topic
   * `t1/orders`, and `handle.subscribe('**', …)` receives everything in
   * the namespace — one tenant's patterns structurally cannot match
   * another tenant's topics, because the `/` separator is matched
   * literally by the wildcard engine (segments split on `.` only).
   *
   * The global bus (no namespace) is the admin view: it still sees every
   * topic, including namespaced ones. Isolation is between tenants, not
   * between a tenant and the operator.
   *
   * Invalid prefixes (empty, blank, containing `/` or `*`) and duplicate
   * registration throw `RangeError`. When the bus has a `durableLogDir`,
   * the namespace gets its own child durable log under
   * `<durableLogDir>/namespaces/<url-encoded-prefix>/`, recovering any
   * history already on disk.
   */
  createNamespace(prefix: string, opts?: NamespaceOptions): NamespaceHandle {
    validateNamespacePrefix(prefix);
    if (this.namespaces.has(prefix)) {
      throw new RangeError(`namespace "${prefix}" is already registered`);
    }
    const resolved = resolveNamespaceOptions(opts);
    this.namespaces.set(prefix, resolved);
    if (this.durableLog != null) {
      const child = DurableTopicLog.open({
        dir: `${this.durableLog.dir}/namespaces/${encodeURIComponent(prefix)}`,
        maxEntriesPerTopic: this.durableLogMaxEntriesPerTopic,
        keyCompaction: this.durableLogKeyCompaction,
        segmentRotation: this.durableLogSegmentRotation,
      });
      this.namespaceLogs.set(prefix, child);
      // A restarted bus reopens the namespace's on-disk history: reseed
      // the bus-side sequence/stats cursors from it, like the root log's
      // construction-time recovery, so numbering continues instead of
      // restarting at 1.
      this.recoverLogState(child);
    }
    return new NamespaceHandle(this, prefix);
  }

  /**
   * Deletes a namespace registration. Throws `RangeError` for an unknown
   * prefix, and `NamespaceNotEmptyError` when the namespace still has
   * live subscribers or retained durable-log entries — drain those first;
   * a namespace is never deleted out from under live consumers. The
   * child log directory is left on disk (it holds no live state once the
   * checks pass) and is recovered if the namespace is re-registered.
   */
  deleteNamespace(prefix: string): void {
    if (!this.namespaces.has(prefix)) {
      throw new RangeError(`unknown namespace "${prefix}"`);
    }
    for (const subscriber of this.subscribers.values()) {
      if (subscriber.namespacePrefix === prefix) {
        throw new NamespaceNotEmptyError(prefix, 'subscribers');
      }
    }
    const child = this.namespaceLogs.get(prefix);
    if (child !== undefined) {
      for (const topic of child.topics()) {
        if (child.messageCount(topic) > 0) {
          throw new NamespaceNotEmptyError(prefix, 'durable-entries');
        }
      }
    }
    this.namespaces.delete(prefix);
    this.namespaceLogs.delete(prefix);
  }

  /**
   * Snapshot of the registered namespaces, in registration order: the
   * prefix, its effective flags, and the current live subscriber count.
   */
  getNamespaces(): NamespaceInfo[] {
    const infos: NamespaceInfo[] = [];
    for (const [prefix, flags] of this.namespaces) {
      let subscribers = 0;
      for (const subscriber of this.subscribers.values()) {
        if (subscriber.namespacePrefix === prefix) subscribers += 1;
      }
      infos.push({
        prefix,
        allowPublish: flags.allowPublish,
        allowSubscribe: flags.allowSubscribe,
        subscribers,
      });
    }
    return infos;
  }

  /**
   * Compiles a namespaced subscription pattern into its matcher.
   *
   * `compilePattern` always yields an anchored `^…$` source, so the
   * namespace is enforced by anchoring the literal `prefix/` in front of
   * the user's pattern with the leading `^` stripped: the pattern then
   * matches the sub-topic after the prefix. A namespaced `**` becomes
   * `^t1/.*$` — every topic in the namespace, structurally unable to
   * match `t2/…` (or `t10/x`: the anchor plus the literal `/` keeps the
   * prefix boundary exact).
   *
   * Note this is deliberately NOT the string pattern `t1/**`: segments
   * split on `.` only, so `t1/**` is a single literal segment matching
   * nothing but the exact topic `t1/**` — never `t1/orders`. The
   * registered pattern *string* is still the readable `t1/<pattern>`
   * form; only the matcher takes this compiled shape.
   */
  private compileNamespacedMatcher(prefix: string, pattern: string): RegExp {
    const inner = compilePattern(pattern).source;
    const body = inner.startsWith('^') ? inner.slice(1) : inner;
    return new RegExp(`^${escapeRegExp(`${prefix}/`)}${body}`);
  }

  /**
   * Resolves `PublishOptions.namespace` to the concrete topic. Returns
   * `topic` unchanged when no namespace is given (the namespace-free
   * path). Unknown namespaces throw `RangeError`; a publish-disallowed
   * namespace or a topic containing `/` (an attempted escape — including
   * naming another registered namespace, e.g. `t2/x` with
   * `namespace: 't1'`) throws `NamespaceDeniedError`.
   */
  private resolveNamespaceForPublish(namespace: string | undefined, topic: string): string {
    if (namespace === undefined) return topic;
    const entry = this.namespaces.get(namespace);
    if (entry === undefined) {
      throw new RangeError(`unknown namespace "${String(namespace)}"`);
    }
    if (!entry.allowPublish) {
      throw new NamespaceDeniedError('publish', namespace, { topic });
    }
    if (topic.includes('/')) {
      throw new NamespaceDeniedError('publish', namespace, { topic });
    }
    return `${namespace}/${topic}`;
  }

  /**
   * Resolves `SubscribeOptions.namespace` to the subscription scope, or
   * `undefined` when no namespace is given. Throws `RangeError` for an
   * unknown namespace and `NamespaceDeniedError` for a
   * subscribe-disallowed namespace or a pattern containing `/` (an
   * attempted escape). The returned scope carries the translated
   * `<prefix>/<pattern>` registration string, the compiled matcher, the
   * publish-side prefix-index key, and the durable log replays read
   * from.
   */
  private resolveNamespaceForSubscribe(
    namespace: string | undefined,
    pattern: string,
  ): NamespaceSubscriptionScope | undefined {
    if (namespace === undefined) return undefined;
    const entry = this.namespaces.get(namespace);
    if (entry === undefined) {
      throw new RangeError(`unknown namespace "${String(namespace)}"`);
    }
    if (!entry.allowSubscribe) {
      throw new NamespaceDeniedError('subscribe', namespace, { pattern });
    }
    if (pattern.includes('/')) {
      throw new NamespaceDeniedError('subscribe', namespace, { pattern });
    }
    // Prefix-index key (EB-15): the pattern's literal dot-prefix, glued
    // under the namespace — `orders.*` in `t1` files under `t1/orders`,
    // a candidate key of every concrete `t1/orders.…` topic, so the
    // no-miss invariant holds. A leading-wildcard pattern files under
    // the empty key, like its global counterpart.
    const literal = EventBus.patternPrefixKey(pattern);
    return {
      prefix: namespace,
      internalPattern: `${namespace}/${pattern}`,
      matcher: this.compileNamespacedMatcher(namespace, pattern),
      indexKey: literal === '' ? '' : `${namespace}/${literal}`,
      log: this.namespaceLogs.get(namespace),
    };
  }

  /**
   * The durable log a topic's records belong in: the namespace's child
   * log when the topic names a registered namespace (`<prefix>/…`),
   * otherwise the root log. With no namespaces registered this is a
   * single branch to the root log — the namespace-free path pays for one
   * predictable branch and nothing else.
   */
  private durableLogForTopic(topic: string): DurableTopicLog | undefined {
    const root = this.durableLog;
    if (root === undefined || this.namespaces.size === 0) return root;
    for (const prefix of this.namespaces.keys()) {
      // The trailing `/` is structural: prefix `t1` never matches topic
      // `t10/x` — the same anchor property the wildcard engine relies on
      // to keep namespaces from crossing.
      if (topic.startsWith(`${prefix}/`)) {
        return this.namespaceLogs.get(prefix) ?? root;
      }
    }
    return root;
  }

  /**
   * Per-namespace aggregate for `getStats()`, derived from the existing
   * per-topic `TopicStats`: every concrete topic starting with
   * `<prefix>/` belongs to the namespace.
   */
  private namespaceStatsFor(prefix: string): NamespaceStats {
    const marker = `${prefix}/`;
    let topics = 0;
    let publishedMessages = 0;
    for (const [topic, stats] of this.topicStats) {
      if (topic.startsWith(marker)) {
        topics += 1;
        publishedMessages += stats.publishedMessages;
      }
    }
    let subscribers = 0;
    for (const subscriber of this.subscribers.values()) {
      if (subscriber.namespacePrefix === prefix) subscribers += 1;
    }
    return { namespace: prefix, topics, subscribers, publishedMessages };
  }

  /**
   * Reseeds bus-side cursors from a durable log: per-topic sequence
   * numbers and stats entries (so the next publish continues the
   * on-disk numbering and stats reflect recovered history), plus the
   * per-key sequence cursors (so keyed publishes never restart at 1 and
   * corrupt per-key ordering). The root log gets this at construction;
   * a namespace child log gets it at `createNamespace`.
   */
  private recoverLogState(log: DurableTopicLog): void {
    for (const topic of log.topics()) {
      const lastSeq = log.lastSeq(topic);
      this.topicSeq.set(topic, lastSeq);
      this.topicStats.set(topic, {
        subscriberCount: 0,
        // `messageCount` excludes the seq-0 delayed-delivery schedule
        // records: a scheduled-but-pending message has not fanned out
        // yet, so it must not count as published.
        publishedMessages: log.messageCount(topic),
        expiredMessages: 0,
        lastSeq,
        sequenceGaps: 0,
        rateLimitedMessages: 0,
        rejectedMessages: 0,
        lateMessages: 0,
        duplicateMessages: 0,
        aliasRetiredMessages: 0,
        filteredMessages: 0,
        dedupDropped: 0,
        compressedMessages: 0,
        compressedBytesBefore: 0,
        compressedBytesAfter: 0,
        compressionTimeMs: 0,
      });
    }
    for (const [key, maxSeq] of log.recoveredKeySeqs()) {
      this.keyCursors.set(key, maxSeq);
    }
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
    // Multi-tenant namespace (EB-52): resolve the scope first — unknown
    // namespaces, disallowed actions and escape attempts throw here, so
    // a throw leaves no half-registered subscriber behind. The
    // registered pattern becomes the translated `<prefix>/<pattern>`
    // form; matching, the prefix index and replays all operate on the
    // concrete namespaced topic space.
    const ns = this.resolveNamespaceForSubscribe(opts?.namespace, topicPattern);
    if (ns !== undefined) topicPattern = ns.internalPattern;
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const resumeFromSeq = opts?.resumeFromSeq;
    this.validateResumeFromSeq(resumeFromSeq);
    const resumeFromTime = opts?.resumeFromTime;
    this.validateResumeFromTime(resumeFromTime, resumeFromSeq);
    // Broker-level ACL: an unauthorized subscription is audited and then
    // rejected with a clear error before anything registers — the caller
    // must not mistake silence for success. Applies to the subscribe
    // family uniformly (subscribeReliable, consumer groups, ...), since
    // they all funnel through here.
    if (!this.aclAllowsSubscribe(topicPattern)) {
      this.emitAuthzDenied('subscribe', undefined, topicPattern);
      throw new AclDeniedError('subscribe', topicPattern);
    }
    const id = `sub-${++this.nextId}`;
    const capacity = opts?.queueSize ?? 100;
    const queueMaxBytes = opts?.queueMaxBytes;
    if (queueMaxBytes !== undefined && (!Number.isFinite(queueMaxBytes) || queueMaxBytes <= 0)) {
      throw new RangeError('subscribe: queueMaxBytes must be a positive finite number of bytes');
    }
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
    const rateLimit = resolveRateLimitOptions(opts?.rateLimit);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const deliveryLatency = resolveDeliveryLatencyOptions(opts?.deliveryLatency);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const ackLatency = resolveAckLatencyOptions(
      opts?.ackLatency,
      opts?.ackSloMs,
      opts?.onAckSloMiss,
      id,
      topicPattern,
      this.now,
    );
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const latencySlo = resolveLatencySloOptions(opts?.latencySlo);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const lagMonitor = resolveLagMonitorOptions(opts?.lagMonitor);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const keyHotspot = resolveKeyHotspotOptions(opts?.keyHotspot);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const causal = resolveCausalOptions(opts?.causal);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const batch = resolveBatchDeliveryOptions(opts?.batch);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const dedup = resolveDeduplicateMessagesOptions(
      opts?.deduplicateMessages,
      this.idempotencyWindowMs,
      this.durableLog != null,
    );
    const onDegraded = opts?.onDegraded;
    const filter = opts?.filter;
    if (filter !== undefined && typeof filter !== 'function') {
      throw new TypeError('filter must be a function');
    }
    const queue = new BoundedQueue<BusMessage>({
      capacity,
      policy: opts?.dropPolicy ?? 'drop-oldest',
      highWaterMarkRatio: opts?.highWaterMarkRatio,
      ...(queueMaxBytes === undefined
        ? {}
        : {
            maxBytes: queueMaxBytes,
            byteSize: (msg: BusMessage) => payloadByteSize(msg.payload),
          }),
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
      // Namespaced subscriptions (EB-52) match with the compiled
      // namespace matcher; everything else shares the pattern cache.
      matcher: ns?.matcher ?? this.compiledMatcher(topicPattern),
      namespacePrefix: ns?.prefix,
      durableLog: ns?.log,
      // A batched handler receives an array per call; the cast is safe —
      // drainSubscriber only ever passes an array to batched subscribers.
      handler: handler as MessageHandler,
      queue,
      lastDeliveredSeq: new Map(),
      lastEpoch: new Map(),
      throttle,
      onThrottled,
      deliveryShaping,
      rateLimit,
      latency: deliveryLatency,
      ackLatency,
      latencySlo,
      lag: lagMonitor,
      keyHotspot,
      causal,
      batch,
      health:
        healthProbe == null
          ? undefined
          : { consecutiveFailures: 0, degraded: false, ...healthProbe },
      onDegraded,
      filter,
      dedup,
    };
    this.subscribers.set(id, subscriber);
    // Durable dedup window: rehydrate the subscriber's window from the
    // journal before any replay runs, so a resumed consumer does not
    // double-deliver messages it already saw before the restart. Entries
    // that aged out of the window are "unknown" — dropped here, never
    // rehydrated. Namespaced subscribers (EB-52) rehydrate from their
    // namespace's child log.
    const replaySource = ns?.log ?? this.durableLog;
    if (dedup?.consumerId !== undefined && replaySource != null) {
      const nowMs = this.now();
      for (const entry of replaySource.dedupWindowFor(dedup.consumerId)) {
        if (nowMs - entry.at < dedup.windowMs) {
          dedup.table.set(`${entry.topic}\0${entry.messageId}`, entry.at);
        }
      }
      while (dedup.table.size > dedup.maxEntries) {
        const oldest = dedup.table.keys().next();
        if (oldest.done) break;
        dedup.table.delete(oldest.value);
      }
    }
    this.subscribersByPattern.set(topicPattern, (this.subscribersByPattern.get(topicPattern) ?? 0) + 1);
    // File the subscriber in the publish-side prefix index under its
    // pattern's literal prefix (see `patternPrefixKey`) — or the
    // namespace-computed key for namespaced subscriptions, which keeps
    // the no-miss invariant over concrete `<prefix>/…` topics.
    const indexKey = ns?.indexKey ?? EventBus.patternPrefixKey(topicPattern);
    let indexBucket = this.prefixIndex.get(indexKey);
    if (indexBucket == null) {
      indexBucket = new Set<string>();
      this.prefixIndex.set(indexKey, indexBucket);
    }
    indexBucket.add(id);
    // A new pattern changes what this node advertises to the cluster hub.
    this.advertiseClusterPatterns();

    // Durable-log resume: pre-fill the queue with logged messages the
    // subscriber missed (seq > resumeFromSeq on matching topics, or —
    // with resumeFromTime — at > resumeFromTime), in publish-time order
    // per topic, before any live message. Queue push applies the
    // subscriber's normal backpressure policy to replayed messages too;
    // a flush is scheduled so they are delivered promptly.
    if (resumeFromTime !== undefined) {
      this.replayLogSinceTime(subscriber, resumeFromTime);
    } else if (resumeFromSeq !== undefined) {
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
        this.clearRateLimitTimer(subscriber);
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
        // The advertised pattern set may have shrunk.
        this.advertiseClusterPatterns();
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
    // The failure behind the most recent redelivery of each message: the
    // handler's thrown error message when it threw, otherwise the
    // tracker's reason ('nack' / 'ack-timeout'). Read-and-cleared when the
    // message is dead-lettered, so `DeadLetterEntry.lastError` always
    // describes the final failure, never a stale one from an earlier
    // delivery round.
    const lastFailure = new WeakMap<BusMessage, string>();
    // Which delivery handle is currently outstanding per message. A
    // redelivery (nack / ack timeout) retires the previous handle, so only
    // the live one may sample the ack-latency tracker when its ack()
    // finishes — a late ack() on a stale handle records nothing against
    // the restarted clock.
    const liveDeliveries = new WeakMap<BusMessage, { live: boolean }>();
    const tracker = new AckTracker<BusMessage>({
      ackTimeoutMs: opts?.ackTimeoutMs ?? 5000,
      now: this.now,
      onRedeliver: (msg, reason) => {
        // EB-45: the outstanding delivery's `bus.ack` span never
        // completes — abandon it. The requeue below emits a fresh
        // `bus.enqueue` span in the same trace, and the redelivery opens
        // a new ack span when it is handed to the handler.
        this.trace?.abandonAckSpan(msg);
        const live = liveDeliveries.get(msg);
        if (live !== undefined) live.live = false;
        const count = (redeliveries.get(msg) ?? 0) + 1;
        redeliveries.set(msg, count);
        // The failure that triggered this redelivery: the handler's
        // thrown error when it threw on this delivery, otherwise the
        // tracker's reason. Cleared now so a later explicit nack() does
        // not inherit a stale throw from an earlier round.
        const thrown = lastFailure.get(msg);
        lastFailure.delete(msg);
        const lastError = thrown ?? reason;
        // The redelivery budget is exhausted: the message is poison
        // (repeated nacks, ack timeouts, or a handler that keeps throwing
        // and never settles). Dead-letter it instead of requeueing
        // forever.
        if (dlq != null && count > dlq.maxRedeliveries) {
          this.moveToDeadLetter(subscriber, msg, count - 1, lastError);
          return;
        }
        this.enqueueMessage(subscriber, msg, this.messageDeadlines.get(msg));
        this.scheduleFlush();
      },
    });
    subscriber.reliable = { tracker, redeliveries, lastFailure };
    if (dlq != null) {
      subscriber.deadLetter = { entries: [], nextSeq: 0, ...dlq };
    }
    // Tracks one delivery, wrapping the handle so the ack-latency tracker
    // (when enabled) samples when ack() finishes. Each message contributes
    // at most one sample, no matter how many times it is redelivered.
    const ackState = subscriber.ackLatency;
    const trackDelivery = (msg: BusMessage): Delivery<BusMessage> => {
      const delivery = tracker.track(msg, redeliveries.get(msg) ?? 0);
      // EB-45: open the `bus.ack` span when the message is part of a
      // sampled trace. The span completes when `ack()` finishes; `nack()`
      // and ack timeouts abandon it (see `onRedeliver` above). Only the
      // currently outstanding delivery may close it — a stale handle's
      // `ack()` finds nothing open and emits nothing, mirroring the
      // ack-latency tracker's single-sample rule.
      const tracer = this.trace;
      const ackTraced =
        tracer !== undefined && tracer.hasTrace(msg)
          ? tracer.openAckSpan(msg, redeliveries.get(msg) ?? 0, this.now())
          : false;
      // EB-53: stamp the hand-off when SLO tracking is enabled, so the
      // sample taken on ack() completion measures delivery→ack
      // processing time, queue dwell excluded. A redelivery re-stamps
      // here; a stale handle's ack() records nothing (the live.live
      // guard), mirroring the ack-latency tracker's single-sample rule.
      const sloState = subscriber.latencySlo;
      const deliveredAtMs = sloState === undefined ? 0 : this.now();
      if (ackState == null && !ackTraced && sloState === undefined) return delivery;
      const live = { live: true };
      liveDeliveries.set(msg, live);
      return {
        ...delivery,
        ack: () => {
          delivery.ack();
          if (live.live) {
            if (ackState != null) ackState.tracker.sampleOnAck(msg);
            if (sloState !== undefined) {
              this.recordProcessingLatency(subscriber, sloState, deliveredAtMs);
            }
            tracer?.closeAckSpan(subscriber, msg, this.now());
          }
          // A stale handle (acked after nack/timeout already requeued)
          // leaves the trace alone: the redelivery owns the open ack
          // span now, and abandoning it here would steal its span.
        },
        nack: () => {
          tracer?.abandonAckSpan(msg);
          delivery.nack();
        },
      };
    };
    deliver = (msgOrBatch) => {
      // Batched delivery: one tracked envelope per message in the batch,
      // handed to the handler as a single array call.
      if (Array.isArray(msgOrBatch)) {
        const batchHandler = handler as ReliableBatchMessageHandler;
        const deliveries = msgOrBatch.map((m) => trackDelivery(m));
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
        } catch (err) {
          const message = thrownErrorMessage(err);
          for (const m of msgOrBatch) lastFailure.set(m, message);
          for (const d of deliveries) d.nack();
        }
        return;
      }
      const msg = msgOrBatch;
      const singleHandler = handler as ReliableMessageHandler;
      const delivery = trackDelivery(msg);
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
      } catch (err) {
        lastFailure.set(msg, thrownErrorMessage(err));
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
    const partitions = opts?.partitions;
    if (partitions !== undefined && (!Number.isInteger(partitions) || partitions < 1)) {
      throw new RangeError('partitions must be a positive integer');
    }
    const assignment = opts?.assignment;
    if (assignment !== undefined && assignment !== 'rendezvous' && assignment !== 'sticky') {
      throw new RangeError("assignment must be 'rendezvous' or 'sticky'");
    }
    // The durable-log replay is deferred until after the group assignment
    // is recorded below: `replayLog` skips seqs inside the group's live
    // handoff-linger windows, which needs `subscriber.groupId` to be set.
    const { resumeFromSeq, resumeFromTime, partitions: _partitions, assignment: _assignment, ...restOpts } = opts ?? {};
    this.validateResumeFromSeq(resumeFromSeq);
    this.validateResumeFromTime(resumeFromTime, resumeFromSeq);
    const sub = this.subscribe(topicPattern, handler, restOpts);
    const key = EventBus.groupKey(groupId, topicPattern);
    // Partition mode is fixed by the group's first member; a disagreeing
    // joiner fails fast here, rolling back the just-registered subscriber
    // so no half-joined member is left behind.
    try {
      this.checkPartitionConfig(key, partitions);
      this.checkAssignmentConfig(key, assignment);
    } catch (err) {
      sub.unsubscribe();
      throw err;
    }
    const subscriber = this.subscribers.get(sub.id);
    if (subscriber == null) throw new Error(`unknown subscriber: ${sub.id}`);
    subscriber.groupId = groupId;
    subscriber.onRebalance = opts?.onRebalance;
    subscriber.handoffLingerMs = handoffLingerMs;
    let members = this.groupMembers.get(key);
    if (members == null) {
      members = [];
      this.groupMembers.set(key, members);
    }
    const partitioned = this.groupPartitions.get(key) !== undefined;
    const beforeAssignment = partitioned ? new Map(this.partitionAssignment(key)) : undefined;
    members.push(sub.id);
    this.invalidatePartitionAssignment(key);
    let partitionRebalance: PartitionRebalanceInfo | undefined;
    if (partitioned) {
      const after = this.partitionAssignment(key);
      const migrated = this.diffPartitionAssignment(key, beforeAssignment!, after);
      partitionRebalance = {
        assignment: Object.fromEntries(after),
        migrated,
        strategy: this.groupAssignmentStrategy.get(key) ?? 'rendezvous',
      };
      // Automatic watermark replay: partitions the newcomer just took over
      // get their uncommitted backlog `(committed, watermark]` from the
      // durable log. Skipped when the member chose its own resume point —
      // its explicit window wins over the automatic one.
      if (resumeFromSeq === undefined && resumeFromTime === undefined) {
        for (const m of migrated) {
          if (m.to === sub.id) this.replayPartitionBacklog(subscriber, key, m.partition);
        }
      }
    }
    if (resumeFromTime !== undefined) {
      if (partitioned) this.replayLogSinceTimeForPartitionedMember(subscriber, key, resumeFromTime);
      else this.replayLogSinceTime(subscriber, resumeFromTime);
    } else if (resumeFromSeq !== undefined) {
      if (partitioned) this.replayLogForPartitionedMember(subscriber, key, resumeFromSeq);
      else this.replayLog(subscriber, resumeFromSeq);
    }
    this.fireRebalance(groupId, topicPattern, key, 'join', sub.id, undefined, partitionRebalance);
    return {
      id: sub.id,
      unsubscribe: () => {
        // Second call is a no-op: no double leave-event, no roster churn.
        if (!this.subscribers.has(sub.id)) return;
        const wasPartitioned = this.groupPartitions.get(key) !== undefined;
        const before = wasPartitioned ? new Map(this.partitionAssignment(key)) : undefined;
        this.removeGroupMember(key, sub.id);
        let pr: PartitionRebalanceInfo | undefined;
        if (wasPartitioned) {
          const after = this.partitionAssignment(key);
          const migrated = this.diffPartitionAssignment(key, before!, after);
          pr = {
            assignment: Object.fromEntries(after),
            migrated,
            strategy: this.groupAssignmentStrategy.get(key) ?? 'rendezvous',
          };
          for (const m of migrated) {
            const owner = this.subscribers.get(m.to);
            if (owner !== undefined) this.replayPartitionBacklog(owner, key, m.partition);
          }
        }
        const linger = this.beginLingerHandoff(groupId, subscriber);
        sub.unsubscribe();
        this.fireRebalance(groupId, topicPattern, key, 'leave', sub.id, linger, pr);
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
   * round-robin cursors and cached partition assignments) are deleted so
   * churn of short-lived groups cannot grow the maps without bound;
   * assignment watermarks and committed offsets are history and are kept.
   */
  private removeGroupMember(key: string, memberId: string): void {
    const members = this.groupMembers.get(key);
    if (members == null) return;
    const idx = members.indexOf(memberId);
    if (idx >= 0) members.splice(idx, 1);
    this.invalidatePartitionAssignment(key);
    if (members.length === 0) {
      this.groupMembers.delete(key);
      this.groupCursors.delete(key);
      // Dead generation: a rejoining group must not pin partitions to
      // departed members — the sticky assignor starts fresh. The strategy
      // itself is retained, mirroring `groupPartitions`.
      this.previousPartitionAssignment.delete(key);
    }
  }

  /**
   * Notifies every current member of a group about a membership change.
   * Each member receives its own snapshot of the post-change roster.
   * `linger` (leave events only) carries the handoff window opened for
   * the departed member, so the event observes the no-duplicate handoff.
   * `partitionRebalance` (partitioned groups only) carries the new
   * partition assignment and which partitions migrated.
   */
  private fireRebalance(
    groupId: string,
    pattern: string,
    key: string,
    trigger: 'join' | 'leave',
    memberId: string,
    linger?: { until: number; windows: LingerWindow[] },
    partitionRebalance?: PartitionRebalanceInfo,
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
        ...(partitionRebalance == null ? {} : { partitionRebalance }),
      });
    }
    // Membership changed: the assignment-to-commitment balance for the
    // group just shifted (a member's uncommitted backlog may now be
    // redelivered by someone else), so re-evaluate lag alerts. A
    // linger window opened for the leaver is already recorded by now, so
    // the held backlog is excluded and the rebalance itself never
    // false-alarms.
    this.checkGroupLagForGroup(groupId);
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
   * Validates the time-based durable-log resume option before anything
   * registers: a throw must not leave a half-registered subscriber
   * behind. A replay has exactly one cursor, so `resumeFromTime` and
   * `resumeFromSeq` are mutually exclusive; otherwise the same contract
   * as `validateResumeFromSeq` — it requires `durableLogDir` and a sane
   * value (a finite millisecond timestamp `>= 0`).
   */
  private validateResumeFromTime(
    resumeFromTime: number | undefined,
    resumeFromSeq: number | undefined,
  ): void {
    if (resumeFromTime === undefined) return;
    if (resumeFromSeq !== undefined) {
      throw new RangeError('resumeFromTime and resumeFromSeq are mutually exclusive');
    }
    if (this.durableLog == null) {
      throw new RangeError('resumeFromTime requires EventBusOptions.durableLogDir to be set');
    }
    if (!Number.isFinite(resumeFromTime) || resumeFromTime < 0) {
      throw new RangeError('resumeFromTime must be a finite number of milliseconds >= 0');
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
    // The group's assigned watermark moved: lag may have crossed the
    // alert threshold (no-op unless alerting is enabled).
    this.checkGroupLagForGroup(groupId);
  }

  /**
   * Rendezvous (highest-random-weight) score of a member for a partition:
   * SHA-256 over `partition\0memberId`, first 8 bytes as uint64. Every
   * node computes the same winner from the same roster, and adding or
   * removing a member only changes the winner where the newcomer/leaver
   * actually wins — the minimal-disruption property rebalancing wants.
   */
  private static rendezvousScore(partition: number, memberId: string): bigint {
    const digest = createHash('sha256').update(`${partition}\0${memberId}`, 'utf8').digest();
    return digest.readBigUInt64BE(0);
  }

  /** SHA-256 based 64-bit hash for partition mapping (see `partitionForMessage`). */
  private static hash64(preimage: string): bigint {
    return createHash('sha256').update(preimage, 'utf8').digest().readBigUInt64BE(0);
  }

  /**
   * Deterministic partition assignment for one competing set: every
   * partition goes to the member picked by the group's assignment
   * strategy. Cached per group key; invalidated on join/leave. Empty when
   * the group is not partitioned or has no members.
   *
   * Strategies (see `GroupSubscribeOptions.assignment`):
   * - `'rendezvous'` (default): highest-random-weight hashing over the
   *   roster — deterministic from the roster alone.
   * - `'sticky'` (EB-54): sticky balanced assignment (`src/sticky.ts`) —
   *   `floor(P/N)`/`ceil(P/N)` per member, maximally sticky across
   *   rebalances, computed from the previous assignment.
   */
  private partitionAssignment(groupKey: string): Map<number, string> {
    const cached = this.partitionAssignmentCache.get(groupKey);
    if (cached !== undefined) return cached;
    const strategy = this.groupAssignmentStrategy.get(groupKey) ?? 'rendezvous';
    if (strategy === 'sticky') {
      const assignment = stickyPartitionAssignment({
        partitions: this.groupPartitions.get(groupKey) ?? 0,
        members: this.groupMembers.get(groupKey) ?? [],
        previous: this.previousPartitionAssignment.get(groupKey),
      });
      // Defensive copy: the cached map is shared with readers, the prev
      // map must stay a pristine snapshot of this generation.
      this.previousPartitionAssignment.set(groupKey, new Map(assignment));
      this.partitionAssignmentCache.set(groupKey, assignment);
      return assignment;
    }
    const assignment = new Map<number, string>();
    const n = this.groupPartitions.get(groupKey) ?? 0;
    const members = this.groupMembers.get(groupKey) ?? [];
    for (let p = 0; p < n; p++) {
      let best: string | undefined;
      let bestScore = -1n;
      for (const m of members) {
        const score = EventBus.rendezvousScore(p, m);
        // Strictly greater: join order breaks the (astronomically
        // unlikely) tie deterministically.
        if (score > bestScore) {
          bestScore = score;
          best = m;
        }
      }
      if (best !== undefined) assignment.set(p, best);
    }
    this.partitionAssignmentCache.set(groupKey, assignment);
    return assignment;
  }

  /** Drops the cached assignment for a competing set (join/leave). */
  private invalidatePartitionAssignment(groupKey: string): void {
    this.partitionAssignmentCache.delete(groupKey);
  }

  /**
   * Which partition a message belongs to: keyed messages hash their key
   * (one key always lands on one partition, so per-key order is preserved
   * within the partition's exclusive consumer); keyless messages hash
   * `(topic, seq)` for an even deterministic spread.
   */
  private partitionForMessage(n: number, topic: string, seq: number, key: string | undefined): number {
    const preimage = key !== undefined ? `k\0${key}` : `t\0${topic}\0${seq}`;
    return Number(EventBus.hash64(preimage) % BigInt(n));
  }

  /**
   * Advances a partition's per-topic assignment watermark. Assignment
   * follows publish order which follows `seq` order, so the newest
   * assignment per (partition, topic) is always the maximum.
   */
  private recordPartitionOffset(groupKey: string, partition: number, topic: string, seq: number): void {
    let byPartition = this.groupPartitionOffsets.get(groupKey);
    if (byPartition == null) {
      byPartition = new Map();
      this.groupPartitionOffsets.set(groupKey, byPartition);
    }
    let byTopic = byPartition.get(partition);
    if (byTopic == null) {
      byTopic = new Map();
      byPartition.set(partition, byTopic);
    }
    const prev = byTopic.get(topic) ?? 0;
    if (seq > prev) byTopic.set(topic, seq);
    // The partition's assigned watermark moved: lag may have crossed the
    // alert threshold (no-op unless alerting is enabled).
    this.checkGroupLagForGroup(groupKey.slice(0, groupKey.indexOf('\0')));
  }

  /**
   * Partitions whose owner changed between two assignments, with the
   * per-topic watermarks the new owner replays from. Sorted by partition.
   */
  private diffPartitionAssignment(
    groupKey: string,
    before: Map<number, string>,
    after: Map<number, string>,
  ): PartitionMigration[] {
    const out: PartitionMigration[] = [];
    const partitions = new Set<number>([...before.keys(), ...after.keys()]);
    for (const p of partitions) {
      const from = before.get(p) ?? null;
      const to = after.get(p);
      if (to === undefined || from === to) continue;
      const watermarks: Record<string, number> = {};
      for (const [topic, seq] of this.groupPartitionOffsets.get(groupKey)?.get(p) ?? []) {
        watermarks[topic] = seq;
      }
      out.push({ partition: p, from, to, watermarks });
    }
    out.sort((a, b) => a.partition - b.partition);
    return out;
  }

  /**
   * Validates a joining member's `partitions` option against the group's
   * fixed configuration. The group's mode is fixed by its first member:
   * a new group records the choice (or its absence); a later member that
   * disagrees throws `RangeError`. Called before the member joins the
   * competing set, so the caller can roll back the subscription.
   */
  private checkPartitionConfig(groupKey: string, partitions: number | undefined): void {
    if (!this.groupMembers.has(groupKey)) {
      // New (or fully drained and recreated) group: the first member
      // fixes the mode; a stale config from a dead generation is dropped.
      if (partitions !== undefined) this.groupPartitions.set(groupKey, partitions);
      else this.groupPartitions.delete(groupKey);
      return;
    }
    const configured = this.groupPartitions.get(groupKey);
    if (partitions !== undefined && partitions !== configured) {
      throw new RangeError(
        configured === undefined
          ? `consumer group is not partitioned (round-robin): cannot join with partitions=${partitions}`
          : `partition count mismatch: group uses ${configured} partitions, got ${partitions}`,
      );
    }
  }

  /**
   * Validates a joining member's `assignment` strategy against the group's
   * fixed configuration (see `checkPartitionConfig`). The group's strategy
   * is fixed by its first member; a later member that specifies a
   * different strategy throws `RangeError`. Called before the member
   * joins the competing set, so the caller can roll back the subscription.
   */
  private checkAssignmentConfig(
    groupKey: string,
    assignment: 'rendezvous' | 'sticky' | undefined,
  ): void {
    if (!this.groupMembers.has(groupKey)) {
      // New (or fully drained and recreated) group: the first member fixes
      // the strategy; a stale strategy from a dead generation is dropped.
      if (assignment !== undefined) this.groupAssignmentStrategy.set(groupKey, assignment);
      else this.groupAssignmentStrategy.delete(groupKey);
      return;
    }
    const configured = this.groupAssignmentStrategy.get(groupKey) ?? 'rendezvous';
    if (assignment !== undefined && assignment !== configured) {
      throw new RangeError(
        `partition assignment strategy mismatch: group uses '${configured}', got '${assignment}'`,
      );
    }
  }

  /**
   * Resolves the competing-set key for a per-partition offset commit. The
   * commit API takes only the groupId, so the bus scans the partitioned
   * groups: exactly one candidate must exist, otherwise the commit is
   * ambiguous and fails fast.
   */
  private partitionGroupKey(groupId: string): string {
    const prefix = `${groupId}\0`;
    const candidates = [...this.groupPartitions.keys()].filter((k) => k.startsWith(prefix));
    if (candidates.length === 1) return candidates[0];
    if (candidates.length === 0) {
      throw new RangeError(`no partitioned consumer group found for groupId "${groupId}"`);
    }
    throw new RangeError(
      `groupId "${groupId}" has ${candidates.length} partitioned groups; per-partition commits are ambiguous`,
    );
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
   * With `opts.partition` (partitioned groups only), the checkpoint is
   * per-partition instead of group-level: the bus replays a migrated
   * partition's backlog from `(partitionCommitted, watermark]`, so
   * per-partition commits are what make migrations precise. Falls back to
   * the group-level checkpoint for partitions that were never committed.
   *
   * When a durable log is configured (`EventBusOptions.durableLogDir`)
   * the checkpoint is additionally appended to the log's group-offset
   * journal, so committed offsets survive a process restart (the journal
   * is append-only; the highest seq per checkpoint wins at recovery).
   * A journal write failure never fails the in-memory commit — the live
   * process keeps serving from memory.
   *
   * Throws `RangeError` on an empty groupId/topic, a non-positive
   * non-integer seq, a partition for a non-partitioned group, or a
   * partition index outside the group's partition count.
   */
  commitOffset(groupId: string, topic: string, seq: number, opts?: { partition?: number }): void {
    if (groupId.length === 0) {
      throw new RangeError('groupId must be a non-empty string');
    }
    if (topic.length === 0) {
      throw new RangeError('topic must be a non-empty string');
    }
    if (!Number.isInteger(seq) || seq < 1) {
      throw new RangeError('seq must be a positive integer sequence number');
    }
    const partition = opts?.partition;
    if (partition !== undefined) {
      if (!Number.isInteger(partition) || partition < 0) {
        throw new RangeError('partition must be a non-negative integer');
      }
      const groupKey = this.partitionGroupKey(groupId);
      const n = this.groupPartitions.get(groupKey) ?? 0;
      if (partition >= n) {
        throw new RangeError(`partition ${partition} out of range for a ${n}-partition group`);
      }
      let byPartition = this.partitionCommittedOffsets.get(groupKey);
      if (byPartition == null) {
        byPartition = new Map();
        this.partitionCommittedOffsets.set(groupKey, byPartition);
      }
      let byTopic = byPartition.get(partition);
      if (byTopic == null) {
        byTopic = new Map();
        byPartition.set(partition, byTopic);
      }
      byTopic.set(topic, seq);
      const sep = groupKey.indexOf('\0');
      this.durableLog?.appendOffset(groupId, topic, seq, this.now(), {
        partition,
        pattern: groupKey.slice(sep + 1),
      });
      // A checkpoint moved: lag may have fallen back below the threshold
      // (rearming the alert latch), so re-evaluate.
      this.checkGroupLagForGroup(groupId);
      return;
    }
    let committed = this.committedOffsets.get(groupId);
    if (committed == null) {
      committed = new Map<string, number>();
      this.committedOffsets.set(groupId, committed);
    }
    committed.set(topic, seq);
    this.durableLog?.appendOffset(groupId, topic, seq, this.now());
    // Same re-evaluation as the partitioned branch above.
    this.checkGroupLagForGroup(groupId);
  }

  /**
   * Sets (or replaces) the lag-alert threshold for one consumer group at
   * runtime (see `EventBusOptions.groupLag`): `onGroupLag` fires when the
   * group's lag first exceeds this many messages. Re-evaluates immediately,
   * so a lowered threshold alerts on an already-lagging group without
   * waiting for new publishes; a raised threshold rearms a firing alarm
   * the same way. Throws `RangeError` on an empty groupId or a non-finite
   * threshold `< 0`.
   */
  setGroupLagThreshold(groupId: string, thresholdMessages: number): void {
    this.groupLagMonitor.setThreshold(groupId, thresholdMessages);
    this.checkGroupLagForGroup(groupId);
  }

  /**
   * Clears a runtime per-group lag threshold set via `setGroupLagThreshold`,
   * restoring the group's configured (or default) threshold. Re-evaluates
   * immediately. Throws `RangeError` on an empty groupId.
   */
  clearGroupLagThreshold(groupId: string): void {
    if (groupId.length === 0) {
      throw new RangeError('groupId must be a non-empty string');
    }
    this.groupLagMonitor.clearThreshold(groupId);
    this.checkGroupLagForGroup(groupId);
  }

  /**
   * One lag row per (group, topic) for classic round-robin groups and per
   * (group, partition, topic) for partitioned groups (see
   * `src/grouplag.ts`). Live handoff-linger windows are excluded from lag
   * (see `lingerHeldToSeq`). Rows are sorted by (groupId, pattern, topic,
   * partition) so snapshots are deterministic. `groupId` narrows to one
   * group; `undefined` scans all groups.
   */
  private groupLagRowsFor(groupId: string | undefined): GroupLagStat[] {
    const rows: GroupLagStat[] = [];
    // Highest live linger-window `toSeq` per (group, topic), cached across
    // the topics scanned for one call (pruning reads the bus clock).
    const lingerHeld = new Map<string, number>();
    const lingerHeldToSeq = (gid: string, topic: string): number => {
      const cacheKey = `${gid}\0${topic}`;
      let held = lingerHeld.get(cacheKey);
      if (held === undefined) {
        held = 0;
        for (const w of this.pruneLingerWindows(gid)) {
          if (w.topic === topic && w.toSeq > held) held = w.toSeq;
        }
        lingerHeld.set(cacheKey, held);
      }
      return held;
    };
    const pushRow = (
      gid: string,
      pattern: string,
      topic: string,
      assignedSeq: number,
      committedSeq: number,
      partition?: number,
    ): void => {
      const heldToSeq = lingerHeldToSeq(gid, topic);
      rows.push({
        groupId: gid,
        pattern,
        topic,
        ...(partition === undefined ? {} : { partition }),
        assignedSeq,
        committedSeq,
        lingerHeldToSeq: heldToSeq,
        lag: Math.max(0, assignedSeq - Math.max(committedSeq, heldToSeq)),
      });
    };
    // Classic rows: assignment watermarks are keyed by group only, so the
    // pattern is attributable only when the group runs one competing set.
    const patternsByGroup = new Map<string, string[]>();
    for (const key of this.groupMembers.keys()) {
      const sep = key.indexOf('\0');
      const gid = key.slice(0, sep);
      if (groupId !== undefined && gid !== groupId) continue;
      let list = patternsByGroup.get(gid);
      if (list == null) {
        list = [];
        patternsByGroup.set(gid, list);
      }
      list.push(key.slice(sep + 1));
    }
    for (const [gid, byTopic] of this.groupOffsets) {
      if (groupId !== undefined && gid !== groupId) continue;
      const patterns = patternsByGroup.get(gid) ?? [];
      const pattern = patterns.length === 1 ? patterns[0] : '';
      const committed = this.committedOffsets.get(gid);
      for (const [topic, assignedSeq] of byTopic) {
        pushRow(gid, pattern, topic, assignedSeq, committed?.get(topic) ?? 0);
      }
    }
    // Partitioned rows: one per (partition, topic), with per-partition
    // checkpoints falling back to the group-level checkpoint.
    for (const [key, byPartition] of this.groupPartitionOffsets) {
      const sep = key.indexOf('\0');
      const gid = key.slice(0, sep);
      if (groupId !== undefined && gid !== groupId) continue;
      const pattern = key.slice(sep + 1);
      const groupCommitted = this.committedOffsets.get(gid);
      const partitionCommitted = this.partitionCommittedOffsets.get(key);
      for (const [partition, byTopic] of byPartition) {
        const pCommitted = partitionCommitted?.get(partition);
        for (const [topic, assignedSeq] of byTopic) {
          pushRow(gid, pattern, topic, assignedSeq, pCommitted?.get(topic) ?? groupCommitted?.get(topic) ?? 0, partition);
        }
      }
    }
    rows.sort(
      (a, b) =>
        (a.groupId < b.groupId ? -1 : a.groupId > b.groupId ? 1 : 0) ||
        (a.pattern < b.pattern ? -1 : a.pattern > b.pattern ? 1 : 0) ||
        (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0) ||
        (a.partition ?? -1) - (b.partition ?? -1),
    );
    return rows;
  }

  /**
   * Feeds one group's current lag rows to the alert monitor. No-op unless
   * `onGroupLag` alerting is enabled, so the publish/commit/rebalance hot
   * paths stay cheap for buses that only observe lag via `getStats()`.
   */
  private checkGroupLagForGroup(groupId: string): void {
    if (!this.groupLagMonitor.enabled()) return;
    this.groupLagMonitor.check(this.groupLagRowsFor(groupId));
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
   * Current partition -> member subscription id for a partitioned
   * competing set (see `GroupSubscribeOptions.partitions`). Empty when
   * the group is not partitioned or has no members.
   */
  getPartitionAssignment(groupId: string, pattern: string): Record<number, string> {
    const key = EventBus.groupKey(groupId, pattern);
    if (this.groupPartitions.get(key) === undefined) return {};
    return Object.fromEntries(this.partitionAssignment(key));
  }

  /**
   * Per-partition assignment watermarks for a competing set: partition ->
   * concrete topic -> highest per-topic `seq` assigned to that partition.
   * Empty when the group is not partitioned or nothing was assigned yet.
   * This is what a migration replays from (see `PartitionMigration`).
   */
  getPartitionWatermarks(groupId: string, pattern: string): Record<number, Record<string, number>> {
    const key = EventBus.groupKey(groupId, pattern);
    const byPartition = this.groupPartitionOffsets.get(key);
    if (byPartition == null) return {};
    const out: Record<number, Record<string, number>> = {};
    for (const [p, byTopic] of byPartition) out[p] = Object.fromEntries(byTopic);
    return out;
  }

  /**
   * Per-partition consumer checkpoints for a competing set (see
   * `commitOffset` with `{ partition }`): partition -> concrete topic ->
   * last processed `seq`. Empty when nothing was committed per partition.
   */
  getPartitionCommittedOffsets(groupId: string, pattern: string): Record<number, Record<string, number>> {
    const key = EventBus.groupKey(groupId, pattern);
    const byPartition = this.partitionCommittedOffsets.get(key);
    if (byPartition == null) return {};
    const out: Record<number, Record<string, number>> = {};
    for (const [p, byTopic] of byPartition) out[p] = Object.fromEntries(byTopic);
    return out;
  }

  /**
   * Subscription ids of the current members of one competing set, in join
   * order. Empty when the group/pattern has no live members.
   */
  getGroupMembers(groupId: string, pattern: string): string[] {
    return [...(this.groupMembers.get(EventBus.groupKey(groupId, pattern)) ?? [])];
  }

  /**
   * Joins this bus to a cluster hub (EB-37): the node advertises its
   * subscribed topic patterns, receives the hub's route table, and starts
   * forwarding locally-published messages whose topic matches subscribers
   * on other members. Messages forwarded through the hub carry
   * hub-assigned per-topic (and per-key) sequence numbers, globally
   * monotonic across the cluster; purely local traffic keeps node-local
   * numbers. See `src/cluster.ts` for the protocol.
   *
   * The initial connect honors `options.reconnect`: with the default policy
   * it rides the backoff and rejects only when the policy gives up; with
   * `reconnect: false` it is a single attempt that rejects when the hub is
   * unreachable. After a successful connect, an unexpected transport drop
   * degrades the bus to local-only mode (cached routes retained) and
   * reconnects with backoff unless `options.reconnect` is `false`.
   *
   * Throws when already connected — `disconnectCluster()` first.
   */
  async connectToHub(options: ClusterConnectOptions): Promise<ClusterLink> {
    if (this.clusterLink != null) {
      throw new Error('already connected to a cluster hub; call disconnectCluster() first');
    }
    // Dynamic import: buses that never cluster never load the TCP/TLS
    // code, and it keeps the module graph acyclic (cluster.ts imports
    // compilePattern from here).
    const cluster = await import('./cluster.ts');
    const link = new cluster.ClusterLink({
      ...options,
      onMessage: (msg) => this.receiveClusterMessage(msg),
      getPatterns: () => [...this.subscribersByPattern.keys()],
    });
    await link.connect();
    this.clusterLink = link;
    // A bus that subscribed before connecting joins with its full route
    // table, not an empty one.
    link.advertisePatterns([...this.subscribersByPattern.keys()]);
    return link;
  }

  /**
   * Leaves the cluster: sends goodbye, closes the transport, stops
   * reconnecting. The bus keeps serving locally. Idempotent.
   */
  async disconnectCluster(): Promise<void> {
    const link = this.clusterLink;
    this.clusterLink = null;
    if (link != null) await link.disconnect();
  }

  /**
   * Cluster link status: connection state, cached route version, hub
   * epoch, and known members. `connected: false` after any drop —
   * `degraded` tells whether the bus is serving local-only on cached
   * routes (as opposed to never having connected).
   */
  clusterStatus(): (ClusterLinkStatus & { everConnected: boolean }) | null {
    const link = this.clusterLink;
    if (link == null) return null;
    const status = link.getStatus();
    return { ...status, everConnected: status.connects > 0 };
  }

  /**
   * Delivers one hub-forwarded message to this node's matching
   * subscribers. Runs the same matching/filter/backpressure/group path
   * as local publishes (`deliverMatched`); the message keeps the hub's
   * sequence numbers and epoch, so gap detection and key ordering treat
   * it as the cluster stream, not the local one.
   *
   * Cluster-received messages are intentionally NOT written to this
   * node's durable log: the log captures the node's own publish stream
   * (one sequence space per log), and the sending node already logged the
   * message under its local numbers.
   *
   * Returns false when the frame is invalid or cannot be inflated (a
   * dictionary-compressed message whose dictionary this node never
   * registered fails closed) — the link counts the drop.
   */
  private receiveClusterMessage(frame: ClusterMessage): boolean {
    if (typeof frame.topic !== 'string' || frame.topic.length === 0 || frame.topic.length > 1024) {
      return false;
    }
    if (!Number.isInteger(frame.seq) || frame.seq < 1) return false;
    const epoch =
      typeof frame.epoch === 'string' && frame.epoch.length > 0 ? frame.epoch : 'hub:unknown';
    const payload = frame.payload;
    // The wire carries the compressed envelope when compression was on;
    // the content filter must judge the application payload, so inflate
    // first. Dictionary bytes come from this node's own registry — every
    // node in the cluster must register the same preset dictionaries
    // (identical bytes hash to the same dictId).
    let rawPayload: unknown = payload;
    if (frame.compressed === true) {
      if (!isCompressedPayload(payload)) return false;
      let dictionary: Buffer | undefined;
      if (frame.dictId !== undefined) {
        dictionary = this.compressionDictionaryRegistry.get(frame.dictId);
        if (dictionary === undefined) return false;
      }
      try {
        rawPayload = inflateEnvelopePayload(payload, dictionary);
      } catch {
        return false;
      }
    }
    const msg: BusMessage = {
      topic: frame.topic,
      payload,
      seq: frame.seq,
      epoch,
      // The hub validated the identity's shape; a remote message without
      // one simply has no identity, like a local plain publish.
      ...(typeof frame.messageId === 'string' && frame.messageId.length > 0
        ? { messageId: frame.messageId }
        : {}),
    };
    if (frame.expiresAt !== undefined) this.messageDeadlines.set(msg, frame.expiresAt);
    if (frame.compressed === true) {
      // Re-register for transparent inflation at delivery, exactly like a
      // locally compressed message — gated on this set, never on the
      // envelope shape alone.
      this.compressedPayloads.add(msg);
      if (frame.dictId !== undefined) {
        const dictionary = this.compressionDictionaryRegistry.get(frame.dictId);
        if (dictionary !== undefined) this.compressedDictionaries.set(msg, dictionary);
      }
    }
    const keyed =
      frame.key !== undefined && frame.keySeq !== undefined
        ? { key: frame.key, keySeq: frame.keySeq }
        : undefined;
    this.deliverMatched(msg, frame.expiresAt, rawPayload, keyed);
    this.scheduleFlush();
    return true;
  }

  /**
   * Receives one message from the bridge transport (EB-51): the inbound
   * half of the cross-process fan-out bridge. The envelope is validated
   * (a malformed envelope is reported, never thrown — a remote peer is
   * not trusted input), then queued in the bounded ingress buffer and
   * admitted on a microtask: admission is the SAME `fanOut` local
   * publishes go through — alias resolution, ACL, schema validation,
   * per-topic rate-limit budget, TTL at drain — with node-local sequence
   * numbers, and the message is never mirrored back to the bridge
   * (`fromBridge`), which keeps a multi-node bridge loop-free. The
   * source TTL deadline rides verbatim when the envelope carries one
   * (a message that expires in flight expires at drain instead of being
   * resurrected); otherwise the node's own TTL rules apply. The
   * `traceId` continues the end-to-end trace as a `traceparent`.
   *
   * Backpressure: a full ingress buffer sheds the NEWEST envelope
   * (drop-newest keeps the older publish order intact) and counts it in
   * `getStats().bridge.dropped`. Envelopes rejected by admission are not
   * counted as inbound — they surface on the normal admission counters
   * and `onAdmissionRejected`, like local publishes.
   *
   * Returns `{ accepted: false, reason }` when the envelope was not
   * queued: `'not-configured'` (no `EventBusOptions.bridge`),
   * `'invalid-envelope'` (shape validation failed), or `'shed'`
   * (ingress buffer full).
   */
  receiveFromBridge(envelope: BridgeEnvelope): BridgeReceiveResult {
    const bridge = this.bridge;
    if (bridge === undefined) {
      return { accepted: false, reason: 'not-configured' };
    }
    const validated = validateBridgeEnvelope(envelope);
    if (!validated.ok) {
      return { accepted: false, reason: 'invalid-envelope' };
    }
    if (this.bridgeInboundQueue.length >= bridge.maxInboundQueue) {
      this.bridgeDropped += 1;
      return { accepted: false, reason: 'shed' };
    }
    this.bridgeInboundQueue.push(validated.envelope);
    this.scheduleBridgeDrain();
    return { accepted: true };
  }

  /**
   * Arms the microtask that drains the bridge ingress buffer. Deferred
   * (not synchronous) so a transport that delivers synchronously from
   * inside a publish — e.g. an in-process loopback — can never recurse
   * into the bus; the buffer absorbs the burst and the shed path above
   * bounds it.
   */
  private scheduleBridgeDrain(): void {
    if (this.bridgeDrainScheduled) return;
    this.bridgeDrainScheduled = true;
    queueMicrotask(() => {
      this.bridgeDrainScheduled = false;
      this.drainBridgeInbound();
    });
  }

  /** Admits every queued bridge envelope through `fanOut`, in order. */
  private drainBridgeInbound(): void {
    while (this.bridgeInboundQueue.length > 0) {
      const env = this.bridgeInboundQueue.shift() as BridgeEnvelope;
      // The traceId continues the end-to-end trace: a fresh parent span
      // under the source's trace, in the same W3C format `publish` takes.
      const traceparent =
        env.traceId === undefined ? undefined : formatTraceparent(env.traceId, newSpanId());
      const { admitted } = this.fanOut(
        env.topic,
        env.payload,
        false,
        undefined,
        env.key,
        env.keySeq,
        traceparent,
        env.messageId,
        false,
        env.expiresAt,
        true,
      );
      if (admitted) {
        this.bridgeInbound += 1;
        // A remote keySeq becomes this node's high-water mark for the
        // key: the next LOCAL keyed publish draws past it instead of
        // reusing a number a subscriber already consumed from the bridge.
        // Only admitted messages advance the cursor — a rejected message
        // never reached any subscriber, so reusing its number locally
        // cannot disturb per-key order.
        if (env.key !== undefined && env.keySeq !== undefined) {
          const cursor = this.keyCursors.get(env.key) ?? 0;
          if (env.keySeq > cursor) this.keyCursors.set(env.key, env.keySeq);
        }
      }
    }
    this.scheduleFlush();
  }

  /** Re-announces the node's patterns when the set changed. */
  private advertiseClusterPatterns(): void {
    this.clusterLink?.advertisePatterns([...this.subscribersByPattern.keys()]);
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
    // Namespaced subscribers (EB-52) replay from their namespace's child
    // log; everyone else replays from the root log.
    const log = subscriber.durableLog ?? this.durableLog;
    if (log == null) return;
    const matcher = subscriber.matcher;
    const records: ReplayRecord[] = [];
    for (const topic of log.topics()) {
      // Topic aliases (EB-44): replay attributes each record to its
      // alias-resolved topic — a subscriber on the new topic name replays
      // history logged under the old name, and an old-topic subscriber
      // still replays the mirrored new-topic history (the live fan-out's
      // dual-write window, applied to history). The record keeps its
      // logged topic and per-topic seq identity; only matching widens.
      if (!this.topicMatchesWithAliases(matcher, topic, this.resolveLiveTopic(topic))) continue;
      for (const rec of log.readSince(topic, fromSeq)) records.push(rec);
    }
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    this.admitReplayRecords(subscriber, records);
    this.scheduleFlush();
  }

  /**
   * Replays the durable log for a member of a partitioned group, filtered
   * to the partitions currently assigned to it: a resume must not deliver
   * another member's partitions. Used when a partitioned-group member
   * subscribes with `resumeFromSeq`.
   */
  private replayLogForPartitionedMember(subscriber: Subscriber, groupKey: string, fromSeq: number): void {
    const log = this.durableLog;
    if (log == null) return;
    const n = this.groupPartitions.get(groupKey);
    if (n === undefined) {
      this.replayLog(subscriber, fromSeq);
      return;
    }
    const assignment = this.partitionAssignment(groupKey);
    const mine = new Set<number>();
    for (const [p, memberId] of assignment) {
      if (memberId === subscriber.id) mine.add(p);
    }
    const matcher = subscriber.matcher;
    const records: ReplayRecord[] = [];
    for (const topic of log.topics()) {
      // Alias-aware matching (EB-44): records are attributed to their
      // alias-resolved topic, mirroring the live fan-out's dual-write
      // window for history. See `replayLog`.
      if (!this.topicMatchesWithAliases(matcher, topic, this.resolveLiveTopic(topic))) continue;
      for (const rec of log.readSince(topic, fromSeq)) {
        if (mine.has(this.partitionForMessage(n, rec.topic, rec.seq, rec.key))) records.push(rec);
      }
    }
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    this.admitReplayRecords(subscriber, records);
    this.scheduleFlush();
  }

  /**
   * Replays durable-log records into a resubscribing subscriber's queue
   * by wall-clock time: every logged message on a topic matching the
   * subscriber's pattern with `at` strictly greater than `fromTime` is
   * re-enqueued with its original `seq` and TTL deadline, in
   * publish-time (`at`, then `seq`, then topic) order — deterministic,
   * and the same cross-topic order `replayLog` uses. The cold-start /
   * disaster-recovery counterpart to `replayLog`'s seq-based resume
   * (see `SubscribeOptions.resumeFromTime`). Expired deadlines are
   * kept on the message so the drain drops them as expired instead of
   * resurrecting stale data. Shares `admitReplayRecords` with every
   * other replay path, so the content filter, handoff linger,
   * compression re-registration and keyed ordering behave identically.
   */
  private replayLogSinceTime(subscriber: Subscriber, fromTime: number): void {
    // Namespaced subscribers (EB-52) replay from their namespace's child
    // log; everyone else replays from the root log.
    const log = subscriber.durableLog ?? this.durableLog;
    if (log == null) return;
    const matcher = subscriber.matcher;
    const records: ReplayRecord[] = [];
    for (const topic of log.topics()) {
      // Alias-aware matching (EB-44): records are attributed to their
      // alias-resolved topic, mirroring the live fan-out's dual-write
      // window for history. See `replayLog`.
      if (!this.topicMatchesWithAliases(matcher, topic, this.resolveLiveTopic(topic))) continue;
      for (const rec of log.readSinceTime(topic, fromTime)) records.push(rec);
    }
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    this.admitReplayRecords(subscriber, records);
    this.scheduleFlush();
  }

  /**
   * Replays the durable log for a member of a partitioned group by
   * wall-clock time, filtered to the partitions currently assigned to
   * it: a resume must not deliver another member's partitions. The
   * time-based counterpart of `replayLogForPartitionedMember` (see
   * `SubscribeOptions.resumeFromTime`).
   */
  private replayLogSinceTimeForPartitionedMember(
    subscriber: Subscriber,
    groupKey: string,
    fromTime: number,
  ): void {
    const log = this.durableLog;
    if (log == null) return;
    const n = this.groupPartitions.get(groupKey);
    if (n === undefined) {
      this.replayLogSinceTime(subscriber, fromTime);
      return;
    }
    const assignment = this.partitionAssignment(groupKey);
    const mine = new Set<number>();
    for (const [p, memberId] of assignment) {
      if (memberId === subscriber.id) mine.add(p);
    }
    const matcher = subscriber.matcher;
    const records: ReplayRecord[] = [];
    for (const topic of log.topics()) {
      // Alias-aware matching (EB-44): records are attributed to their
      // alias-resolved topic, mirroring the live fan-out's dual-write
      // window for history. See `replayLog`.
      if (!this.topicMatchesWithAliases(matcher, topic, this.resolveLiveTopic(topic))) continue;
      for (const rec of log.readSinceTime(topic, fromTime)) {
        if (mine.has(this.partitionForMessage(n, rec.topic, rec.seq, rec.key))) records.push(rec);
      }
    }
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    this.admitReplayRecords(subscriber, records);
    this.scheduleFlush();
  }

  /**
   * Replays one migrated partition's uncommitted backlog into its new
   * owner: per topic, the log records with `seq` in
   * `(committed, watermark]` that map to this partition. Committed comes
   * from the per-partition checkpoint when present, else the group-level
   * checkpoint. No durable log, no watermarks, or an empty window replays
   * nothing. The handoff-linger skip does not apply: a migration is a
   * deliberate partition takeover, not a resume.
   */
  private replayPartitionBacklog(subscriber: Subscriber, groupKey: string, partition: number): void {
    const log = this.durableLog;
    if (log == null) return;
    const n = this.groupPartitions.get(groupKey);
    if (n === undefined) return;
    const sep = groupKey.indexOf('\0');
    const groupId = groupKey.slice(0, sep);
    const watermarks = this.groupPartitionOffsets.get(groupKey)?.get(partition);
    if (watermarks == null) return;
    const partitionCommitted = this.partitionCommittedOffsets.get(groupKey)?.get(partition);
    const groupCommitted = this.committedOffsets.get(groupId);
    const matcher = subscriber.matcher;
    const records: ReplayRecord[] = [];
    for (const topic of log.topics()) {
      if (!matcher.test(topic)) continue;
      const watermark = watermarks.get(topic);
      if (watermark === undefined) continue;
      const committed = partitionCommitted?.get(topic) ?? groupCommitted?.get(topic) ?? 0;
      if (watermark <= committed) continue;
      // readSince yields ascending seq: stop at the watermark.
      for (const rec of log.readSince(topic, committed)) {
        if (rec.seq > watermark) break;
        if (this.partitionForMessage(n, rec.topic, rec.seq, rec.key) === partition) records.push(rec);
      }
    }
    if (records.length === 0) return;
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    this.admitReplayRecords(subscriber, records, { ignoreLinger: true });
    this.scheduleFlush();
  }

  /**
   * Admits already-collected durable-log records into a subscriber's
   * queue: key-baseline seeding, content filter, handoff-linger skip,
   * compression re-registration, keyed ordering gate. Shared by
   * `replayLog` (subscribe-time resume), `replayLogForPartitionedMember`,
   * and `replayPartitionBacklog` (migration replay).
   */
  private admitReplayRecords(
    subscriber: Subscriber,
    records: ReplayRecord[],
    opts?: { ignoreLinger?: boolean },
  ): void {
    // Keyed-ordering replay baseline (see `PublishOptions.key`): seed each
    // replayed key's expectation at its smallest replayed keySeq, so
    // replayed messages are released in keySeq order even when the log's
    // at-order differs from keySeq order (delayed deliveries are logged at
    // fan-out but numbered at schedule time). Records without a keySeq —
    // logs written before keyed ordering, or unkeyed messages — replay
    // exactly as before.
    for (const rec of records) {
      if (rec.key === undefined || rec.keySeq === undefined) continue;
      // Replayed records come from this node's own log: the local stream.
      const mapKey = keyOrderKey('', rec.key);
      let order = subscriber.keyOrder?.get(mapKey);
      if (order === undefined) {
        if (subscriber.keyOrder === undefined) subscriber.keyOrder = new Map();
        order = { expected: rec.keySeq, skipped: new Set<number>(), buffer: new Map() };
        subscriber.keyOrder.set(mapKey, order);
      } else if (rec.keySeq < order.expected) {
        order.expected = rec.keySeq;
      }
    }
    // Causal-ordering replay baseline: seed each replayed source's
    // expectation at its smallest replayed clock, so replayed messages
    // are released in clock order even when the log's at-order differs
    // from clock order. Records without a clock — logs written before
    // causal clocks, or non-causal messages — replay exactly as before.
    // Only seeds sources the subscriber has no live state for: a
    // resubscribing consumer's live expectation stays authoritative.
    const causalGate = subscriber.causal;
    if (causalGate !== undefined) {
      for (const rec of records) {
        if (rec.causal === undefined) continue;
        const prev = causalGate.expected.get(rec.causal.source);
        if (prev === undefined || rec.causal.clock < prev) {
          causalGate.expected.set(rec.causal.source, rec.causal.clock);
        }
      }
    }
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
          // A filtered replayed message is deliberately skipped: advance
          // the key baseline past it like the live path does, or a later
          // keySeq for the same key would buffer forever.
          if (rec.key !== undefined && rec.keySeq !== undefined) {
            this.skipKeySeq(subscriber, rec.key, rec.keySeq);
          }
          // Same for a filtered causal message: its clock will never be
          // fanned out to this subscriber, so the source's expectation
          // advances past it instead of hanging on a missing dependency.
          if (rec.causal !== undefined) {
            this.skipCausalClock(subscriber, rec.causal.source, rec.causal.clock);
          }
          continue;
        }
      }
      // Handoff linger: a group member that left within its linger window
      // is presumed still processing these seqs — skip them so the
      // rejoining member's resume never delivers in-flight work twice.
      // Like a content filter, the skip must not churn backpressure or
      // count as a sequence gap: the per-topic baseline advances over it.
      // After the window expires the same replay delivers them normally
      // (the leaver is presumed dead; at-least-once resumes). Migration
      // replay (`ignoreLinger`) is a deliberate partition takeover, not a
      // resume, so it bypasses the linger.
      const groupId = subscriber.groupId;
      if (!opts?.ignoreLinger && groupId !== undefined && this.isLingering(groupId, rec.topic, rec.seq)) {
        this.advanceBaseline(subscriber, rec.topic, rec.seq);
        if (rec.key !== undefined && rec.keySeq !== undefined) {
          this.skipKeySeq(subscriber, rec.key, rec.keySeq);
        }
        // A lingered causal message replays after the window expires: the
        // skip advances the expectation now, and the later replay arrives
        // as a (harmless, immediately delivered) regression.
        if (rec.causal !== undefined) {
          this.skipCausalClock(subscriber, rec.causal.source, rec.causal.clock);
        }
        continue;
      }
      const msg: BusMessage = {
        topic: rec.topic,
        payload: rec.payload,
        seq: rec.seq,
        // Restores the message identity logged at publish time, so a
        // resumed dedup window recognizes replays of already-delivered
        // messages (see `SubscribeOptions.deduplicateMessages`).
        ...(typeof rec.messageId === 'string' && rec.messageId.length > 0
          ? { messageId: rec.messageId }
          : {}),
        // Restores the business event time logged at publish time, so a
        // resumed subscriber sees the same event-time metadata the live
        // delivery carried (see `PublishOptions.eventTime`).
        ...(typeof rec.eventTime === 'number' && Number.isFinite(rec.eventTime) && rec.eventTime >= 0
          ? { eventTime: rec.eventTime }
          : {}),
      };
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
      if (rec.causal !== undefined && subscriber.causal !== undefined) {
        // Causal replay: the filter and linger checks above already ran,
        // so the gate admits straight into the queue — replay never burned
        // throttle budget and must not start now. Keyed causal messages
        // run the keyed gate second, exactly like the live path.
        this.deliverCausal(
          subscriber,
          {
            msg,
            expiresAt: rec.expiresAt,
            rawPayload: rec.payload,
            replay: true,
            ...(rec.key !== undefined && rec.keySeq !== undefined
              ? { keyed: { key: rec.key, keySeq: rec.keySeq } }
              : {}),
          },
          rec.causal.source,
          rec.causal.clock,
        );
      } else if (rec.key !== undefined && rec.keySeq !== undefined) {
        // Keyed replay: the filter and linger checks above already ran, so
        // the ordering gate admits straight into the queue — replay never
        // burned throttle budget and must not start now.
        this.deliverKeyed(
          subscriber,
          { msg, expiresAt: rec.expiresAt, rawPayload: rec.payload, replay: true },
          rec.key,
          rec.keySeq,
        );
      } else {
        this.enqueueMessage(subscriber, msg, rec.expiresAt);
      }
    }
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
    validateTraceparent(opts?.traceparent, 'publish');
    validateEventTime(opts?.eventTime, 'publish');
    validateCausal(opts?.causal, 'publish');
    // Multi-tenant namespace (EB-52): resolve to the concrete
    // `<namespace>/<topic>` before admission — the full pipeline below
    // (ACL, schema, rate limit, TTL, idempotency, durable log) then
    // operates on the concrete topic, exactly as for a global publish.
    topic = this.resolveNamespaceForPublish(opts?.namespace, topic);
    const { accepted } = this.fanOut(
      topic,
      payload,
      false,
      undefined,
      opts?.key,
      undefined,
      opts?.traceparent,
      opts?.messageId,
      opts?.routed ?? false,
      undefined,
      false,
      false,
      opts?.eventTime,
      opts?.causal,
    );
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
   * The topic alias (if any) resolves before the dedup gate, and the
   * broker-level ACL runs before the dedup gate: a denied publish
   * claims no dedup slot and reports `{ duplicate: false, accepted: 0 }`.
   * The dedup gate then runs before admission, ahead of everything else:
   * a duplicate consumes no sequence number (subscribers see no phantom
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
    validateTraceparent(opts?.traceparent, 'publishIdempotent');
    validateEventTime(opts?.eventTime, 'publishIdempotent');
    validateCausal(opts?.causal, 'publishIdempotent');
    const messageId = opts?.messageId;
    // Multi-tenant namespace (EB-52): the `(topic, messageId)` dedup
    // identity below is scoped to the concrete `<namespace>/<topic>`, so
    // the same messageId in two namespaces never collides.
    topic = this.resolveNamespaceForPublish(opts?.namespace, topic);
    // Topic aliases (EB-44) resolve before everything else: the resolved
    // topic is the real publish topic — the ACL check, the dedup identity
    // (`(topic, messageId)`), and `fanOut` all see it. A publish to a
    // retired old topic claims no dedup slot and reports
    // `{ duplicate: false, accepted: 0 }` — it was refused, not
    // deduplicated, and its retry stays a fresh publish.
    const aliasResolution = this.resolvePublishTopic(topic);
    if (aliasResolution.retired) {
      this.countAliasRetired(topic, payload);
      return { duplicate: false, accepted: 0 };
    }
    topic = aliasResolution.topic;
    // No identity, no dedup: a plain publish that still reports the same
    // result shape.
    if (typeof messageId !== 'string' || messageId.length === 0) {
      const { accepted } = this.fanOut(
        topic,
        payload,
        false,
        undefined,
        opts?.key,
        undefined,
        opts?.traceparent,
        opts?.messageId,
        opts?.routed ?? false,
        undefined,
        false,
        false,
        opts?.eventTime,
      );
      this.scheduleFlush();
      return { duplicate: false, accepted };
    }
    // Authorization precedes identity: a denied publish claims no dedup
    // slot and reports `{ duplicate: false, accepted: 0 }` — it was
    // denied, not deduplicated, and its retry stays a fresh publish.
    if (!this.aclAllowsPublish(topic)) {
      this.countAclDenied(topic, payload);
      return { duplicate: false, accepted: 0 };
    }
    const nowMs = this.now();
    const dedupKey = `${topic}\0${messageId}`;
    this.pruneDedup(nowMs);
    const firstAt = this.dedup.get(dedupKey);
    if (firstAt !== undefined && nowMs - firstAt < this.idempotencyWindowMs) {
      // Suppressed duplicate: dropped before admission — no sequence
      // number, no durable-log write, no rate-limit budget. Counted for
      // observability only, and surfaced on the admission-rejection hook.
      this.countDuplicate(topic, payload);
      return { duplicate: true, accepted: 0 };
    }
    const { accepted, admitted } = this.fanOut(
      topic,
      payload,
      false,
      undefined,
      opts?.key,
      undefined,
      opts?.traceparent,
      messageId,
      opts?.routed ?? false,
      undefined,
      false,
      false,
      opts?.eventTime,
      opts?.causal,
    );
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
    for (const msg of messages) {
      validateMessageKey(msg.key, 'publishBatch');
      validateTraceparent(msg.traceparent, 'publishBatch');
      validateEventTime(msg.eventTime, 'publishBatch');
    }
    let accepted = 0;
    for (const msg of messages) {
      accepted += this.fanOut(
        msg.topic,
        msg.payload,
        false,
        undefined,
        msg.key,
        undefined,
        msg.traceparent,
        msg.messageId,
        false,
        undefined,
        false,
        false,
        msg.eventTime,
      ).accepted;
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
   *    `publish` applies — the broker-level ACL, then schema validation,
   *    then the per-topic rate-limit budget — without mutating any bus
   *    state. Rate-limit
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
   * exactly as before the call for everything delivery-side: no sequence
   * numbers consumed, no rate-limit tokens taken, no durable-log writes —
   * but the rejection IS counted once against the failing entry's
   * admission-gate counters (`rejectedMessages` / `rateLimitedMessages`)
   * and surfaced on `EventBusOptions.onAdmissionRejected`, so an atomic
   * batch rejection stays reconcilable with stats like any other admission
   * rejection.
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
    for (const entry of entries) {
      validateMessageKey(entry.key, 'publishAtomic');
      validateTraceparent(entry.traceparent, 'publishAtomic');
      validateEventTime(entry.eventTime, 'publishAtomic');
    }
    // Phase 1: admit the whole batch against shadow state.
    const shadowBudget = new Map<string, number>();
    // Per-key sequence numbers are drawn in entry order (publish order)
    // against a shadow cursor: a rejected batch leaves the live cursors —
    // and every subscriber's key baseline — exactly untouched.
    const shadowKeyCursors = new Map<string, number>();
    const keySeqs: Array<number | undefined> = new Array(entries.length);
    // Alias-resolved topics, one per entry: the commit phase publishes the
    // resolved topics, never the raw entry topics.
    const resolvedTopics: string[] = new Array(entries.length);
    for (let index = 0; index < entries.length; index++) {
      const { topic, payload, key } = entries[index];
      // Topic aliases (EB-44) resolve before every admission gate: the
      // resolved topic is what the shadow admission checks (ACL, schema,
      // rate-limit) and what the commit phase publishes. A retired old
      // topic aborts the batch like any other admission failure.
      const aliasResolution = this.resolvePublishTopic(topic);
      if (aliasResolution.retired) {
        this.countAliasRetired(topic, payload);
        return { published: 0, rejected: { index, topic, reason: 'alias-retired' } };
      }
      const resolvedTopic = aliasResolution.topic;
      resolvedTopics[index] = resolvedTopic;
      const reason = this.admissionVerdict(resolvedTopic, payload, shadowBudget);
      if (reason !== undefined) {
        // The batch is rejected on the failing entry: count it once against
        // that entry's admission-gate counters and surface it on the unified
        // admission-rejection hook, so an atomic rejection stays reconcilable
        // with stats like any other admission rejection. Everything else
        // stays untouched: no sequence numbers, no rate-limit tokens, no
        // durable-log writes.
        if (reason === 'schema') this.countSchemaRejection(topic, payload);
        else if (reason === 'acl') this.countAclDenied(topic, payload);
        else if (reason === 'alias-retired') this.countAliasRetired(topic, payload);
        else this.countRateLimited(topic, payload);
        return { published: 0, rejected: { index, topic, reason } };
      }
      if (key !== undefined) {
        const keySeq = (shadowKeyCursors.get(key) ?? this.keyCursors.get(key) ?? 0) + 1;
        shadowKeyCursors.set(key, keySeq);
        keySeqs[index] = keySeq;
      }
    }
    // Phase 2: everything was admitted — commit through the normal
    // publish path in one synchronous turn, then flush once.
    for (const [key, next] of shadowKeyCursors) this.keyCursors.set(key, next);
    for (let index = 0; index < entries.length; index++) {
      const { payload, key, traceparent, messageId, eventTime } = entries[index];
      this.fanOut(
        resolvedTopics[index],
        payload,
        true,
        undefined,
        key,
        keySeqs[index],
        traceparent,
        messageId,
        false,
        undefined,
        false,
        false,
        eventTime,
      );
    }
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
    // The ACL is the first admission gate everywhere (after topic-alias
    // resolution, which rewrites the topic before the gates), including
    // the atomic batch's shadow admission: an unauthorized entry aborts
    // the batch.
    if (!this.aclAllowsPublish(topic)) return 'acl';
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
   * - Topic-alias resolution runs first (see `setTopicAlias`): the schedule
   *   is filed under the resolved topic, and a retired old topic is
   *   refused with admission reason `'alias-retired'` before anything is
   *   scheduled.
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
    // Topic aliases (EB-44) resolve before every other gate: the schedule
    // is filed under the resolved topic — the fail-fast ACL/schema checks
    // below, the stamped TTL, and the fan-out at due time all see it. A
    // publish to a retired old topic never becomes a scheduled delivery.
    const aliasResolution = this.resolvePublishTopic(topic);
    if (aliasResolution.retired) {
      this.countAliasRetired(topic, payload);
      return undefined;
    }
    topic = aliasResolution.topic;
    // Fail fast on ACL: an unauthorized publish never becomes a scheduled
    // delivery — no id, nothing in the heap, nothing on disk. Counted the
    // same way a `publish` ACL denial is.
    if (!this.aclAllowsPublish(topic)) {
      this.countAclDenied(topic, payload);
      return undefined;
    }
    // Fail fast on schema: a rejected payload never becomes a scheduled
    // delivery — no id, nothing in the heap, nothing on disk. Counted the
    // same way a `publish` rejection is.
    const validator = this.schemaForTopic(topic);
    if (validator !== undefined && !validator(payload, topic)) {
      this.countSchemaRejection(topic, payload);
      return undefined;
    }
    const ttlMs = this.ttlForTopic(topic);
    const expiresAt = ttlMs === undefined ? undefined : nowMs + ttlMs;
    validateMessageKey(opts.key, 'publishDelayed');
    validateTraceparent(opts.traceparent, 'publishDelayed');
    const key = opts.key;
    // The per-key sequence number is assigned at schedule time: for keyed
    // messages, publish order is schedule order, so a delayed keyed
    // message keeps its schedule-order position even when it fans out
    // after live publishes with higher keySeqs.
    const keySeq = key === undefined ? undefined : this.nextKeySeq(key);
    const id = `delayed-${++this.nextDelayedId}`;
    const entry: DelayedEntry = {
      id,
      topic,
      payload,
      deliverAt,
      expiresAt,
      cancelled: false,
      key,
      keySeq,
      traceparent: opts.traceparent,
      // Normalized at fan-out (see `fanOut`); stored raw on the schedule
      // so the record round-trips byte-identically.
      messageId: opts.messageId,
    };
    // Persist the schedule before it is visible anywhere: a crash between
    // here and the due time must still deliver the message after restart.
    // The schedule record carries no sequence number and burns no
    // rate-limit budget — those happen at fan-out, via the normal path.
    // It does carry the compaction key and the per-key sequence number, so
    // a restart rebuilds the timer with both intact.
    if (this.durableLog != null) {
      const persisted = this.durableLogForTopic(topic)?.append({
        seq: 0,
        topic,
        at: nowMs,
        deliverAt,
        expiresAt,
        delayId: id,
        key,
        keySeq,
        payload,
        // The identity rides the schedule record so a restart rebuilds
        // the timer with it intact (see `DelayedEntry.messageId`).
        ...(typeof opts.messageId === 'string' && opts.messageId.length > 0
          ? { messageId: opts.messageId }
          : {}),
      });
      if (!persisted) {
        // The schedule died before it existed: its keySeq will never fan
        // out — release every subscriber's expectation past the phantom
        // sequence so no key stream hangs on it.
        if (key !== undefined && keySeq !== undefined) this.releaseKeySequence(key, keySeq);
        throw new Error(
          `publishDelayed: the schedule for topic "${topic}" could not be persisted to the ` +
            'durable log (payload is not JSON-serializable or the log write failed); the ' +
            'delayed delivery was not scheduled',
        );
      }
    }
    this.delayedById.set(id, entry);
    this.delayHeap.push(entry);
    // A keyed schedule on a topic a subscriber's pattern does not match
    // advances that subscriber's per-key baseline NOW (schedule time), not
    // at fan-out: otherwise a subscriber buffering a later keySeq for the
    // same key would stall until this schedule's due time waiting for a
    // predecessor it will never receive. Matching subscribers learn the
    // admitted keySeq now, so a later keySeq fanned out first cannot
    // establish the baseline above it (see `initKeyBaseline`).
    if (key !== undefined && keySeq !== undefined) {
      for (const subscriber of this.subscribers.values()) {
        this.initKeyBaseline(subscriber, key, keySeq, subscriber.matcher.test(topic));
      }
    }
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
    // A cancelled keyed schedule never fans out: release every
    // subscriber's per-key expectation past its keySeq, or a subscriber
    // buffering a later keySeq for the same key would wait forever. The
    // release enqueues buffered successors, so flush afterwards.
    if (entry.key !== undefined && entry.keySeq !== undefined) {
      this.releaseKeySequence(entry.key, entry.keySeq);
      this.scheduleFlush();
    }
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
        // A keyed schedule that expired while delayed never fans out:
        // release every subscriber's per-key expectation past its keySeq.
        // The release enqueues buffered successors, so the sweep must
        // flush afterwards exactly as if it had fanned something out.
        if (top.key !== undefined && top.keySeq !== undefined) {
          this.releaseKeySequence(top.key, top.keySeq);
          fannedOut = true;
        }
        continue;
      }
      // `preAdmitted`: schema already ran once at schedule time
      // (fail-fast); validators are expected pure.
      this.fanOut(
        top.topic,
        top.payload,
        true,
        {
          delayId: top.id,
          deliverAt: top.deliverAt,
          expiresAt: top.expiresAt,
          key: top.key,
          keySeq: top.keySeq,
          messageId: top.messageId,
        },
        undefined,
        undefined,
        top.traceparent,
        top.messageId,
      );
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
    this.durableLogForTopic(topic)?.append({ seq: 0, topic, at: this.now(), delayId, cancelled: true, payload: null });
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
    const scheduled = new Map<string, { topic: string; deliverAt: number; expiresAt?: number; payload: unknown; key?: string; keySeq?: number }>();
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
            keySeq: rec.keySeq,
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
        // The schedule consumed its keySeq before the restart; the bus's
        // key cursors were reseeded from the log, so this entry fans out
        // with its original number — never renumbered, never colliding.
        keySeq: rec.keySeq,
        messageId: rec.messageId,
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
   * Draws the next per-key sequence number for `key` (see `keyCursors`).
   * KeySeqs start at 1 per key and increase by 1 for every admitted keyed
   * message, regardless of topic — this is the cross-topic publish order
   * that per-(subscriber, key) delivery enforces.
   */
  private nextKeySeq(key: string): number {
    const seq = (this.keyCursors.get(key) ?? 0) + 1;
    this.keyCursors.set(key, seq);
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
   * the global total, then fires the admission-rejection hook. A rejection
   * happens before admission: the payload never becomes a message — no
   * sequence number is consumed (subscribers see no gap), the durable log
   * never sees it, and it does not burn rate-limit budget.
   */
  private countSchemaRejection(topic: string, payload: unknown): void {
    this.statsFor(topic).rejectedMessages += 1;
    this.totalRejected += 1;
    this.emitAdmissionRejected(topic, 'schema', payload);
  }

  /**
   * Counts one publish shed by publish-side rate limiting against the
   * topic and the global total, then fires the admission-rejection hook.
   * The shed consumes a sequence number (subscribers see a gap) — the
   * caller stamps it before this runs, so the stats entry must already
   * exist here.
   */
  private countRateLimited(topic: string, payload: unknown): void {
    this.statsFor(topic).rateLimitedMessages += 1;
    this.totalRateLimited += 1;
    this.emitAdmissionRejected(topic, 'rate-limit', payload);
  }

  /**
   * Counts one publish suppressed as an idempotency duplicate against the
   * topic and the global total, then fires the admission-rejection hook.
   * A duplicate is dropped before admission, so — like a schema rejection
   * — it creates the topic's stats entry when the topic has never
   * published anything yet.
   */
  private countDuplicate(topic: string, payload: unknown): void {
    this.statsFor(topic).duplicateMessages += 1;
    this.totalDuplicates += 1;
    this.emitAdmissionRejected(topic, 'duplicate', payload);
  }

  /**
   * Counts one publish denied by the broker-level ACL against the topic
   * and the global totals. A denial is a rejection before admission — like
   * a schema rejection it consumes no sequence number, never touches the
   * durable log, and burns no rate-limit budget — so it lands in
   * `rejectedMessages` (the backlog-visible "rejected" metric), surfaces on
   * the admission-rejection hook with reason `'acl'`, and additionally
   * fires the authz audit hook.
   */
  private countAclDenied(topic: string, payload: unknown): void {
    this.statsFor(topic).rejectedMessages += 1;
    this.totalRejected += 1;
    this.emitAdmissionRejected(topic, 'acl', payload);
    this.emitAuthzDenied('publish', topic, undefined);
  }

  /**
   * Fires the authorization-denial audit hook (`authz_denied`) and counts
   * the denial in `BusStats.authzDenied`. Runs after the stats counters
   * moved. Error-isolated: a throwing hook is swallowed — a broken
   * observer must never disturb the publish/subscribe path. No-op when no
   * hook is configured (the counter still moves).
   */
  private emitAuthzDenied(
    action: 'publish' | 'subscribe',
    topic: string | undefined,
    pattern: string | undefined,
  ): void {
    this.totalAuthzDenied += 1;
    const hook = this.onAuthzDenied;
    if (hook === undefined) return;
    try {
      hook({ action, topic, pattern, at: this.now() });
    } catch {
      // Swallowed: the hook is an observer, not part of the path.
    }
  }

  /**
   * Fires the unified admission-rejection hook for one publish-side
   * rejection. Runs after the stats counters moved, so hook events and
   * `getStats()` stay exactly reconcilable. Error-isolated: a throwing
   * hook is swallowed — a broken observer must never disturb the publish
   * path. No-op when no hook is configured.
   */
  private emitAdmissionRejected(
    topic: string,
    reason: AdmissionRejectReason,
    payload: unknown,
  ): void {
    const hook = this.onAdmissionRejected;
    if (hook === undefined) return;
    const serialized = serializeToJson(payload);
    const payloadBytes = serialized === undefined ? 0 : Buffer.byteLength(serialized, 'utf8');
    try {
      hook({ topic, reason, payloadBytes, at: this.now() });
    } catch {
      // Swallowed: the hook is an observer, not part of the publish path.
    }
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
  /**
   * Subscriber-side exactly-once dedup (see
   * `SubscribeOptions.deduplicateMessages`). Returns `true` when the
   * message was suppressed as a within-window duplicate — the caller
   * must not queue it. Otherwise records the `(topic, messageId)` in
   * the subscriber's window (journaling it when the subscriber has a
   * durable `consumerId`) and returns `false`.
   *
   * Only messages carrying a `messageId` participate; everything else
   * passes through untouched.
   */
  private checkSubscriberDedup(subscriber: Subscriber, msg: BusMessage): boolean {
    const dedup = subscriber.dedup;
    const messageId = msg.messageId;
    if (dedup === undefined || typeof messageId !== 'string' || messageId.length === 0) {
      return false;
    }
    const nowMs = this.now();
    const key = `${msg.topic}\0${messageId}`;
    // Expire old entries first: the head of the insertion-ordered table
    // is the oldest, so the first unexpired entry ends the scan.
    for (const [k, seenAt] of dedup.table) {
      if (nowMs - seenAt < dedup.windowMs) break;
      dedup.table.delete(k);
    }
    if (dedup.table.has(key)) {
      // Already delivered within the window: suppress. A re-published
      // duplicate carries a fresh sequence number, so — like a
      // content-filtered message — the per-topic baseline advances over
      // the deliberate skip; otherwise the subscriber would count a
      // phantom gap for a message it chose not to receive. A requeued or
      // replayed copy keeps the original seq, and `advanceBaseline` only
      // moves forward, so those stay no-ops.
      this.advanceBaseline(subscriber, msg.topic, msg.seq, msg.epoch ?? '');
      this.statsFor(msg.topic).dedupDropped += 1;
      this.totalDedupDropped += 1;
      return true;
    }
    // Deleting before re-inserting refreshes the entry's position so the
    // bounded eviction tracks last-delivery order.
    dedup.table.delete(key);
    dedup.table.set(key, nowMs);
    while (dedup.table.size > dedup.maxEntries) {
      const oldest = dedup.table.keys().next();
      if (oldest.done) break;
      dedup.table.delete(oldest.value);
    }
    if (dedup.consumerId !== undefined && this.durableLog != null) {
      // The journal lives on the subscriber's own log: a namespaced
      // subscriber (EB-52) rehydrates from its namespace's child log, so
      // its sightings must be journaled there too.
      const journal = subscriber.durableLog ?? this.durableLog;
      journal.appendDedup({
        consumer: dedup.consumerId,
        topic: msg.topic,
        messageId,
        at: nowMs,
      });
    }
    return false;
  }

  private enqueueMessage(
    subscriber: Subscriber,
    msg: BusMessage,
    deadline: number | undefined,
    skipDedup = false,
  ): 'accepted' | 'dropped' | 'duplicate' {
    // Every queue insertion funnels through here — fan-out, redeliveries,
    // log replays, DLQ replays, and mid-drain requeues — so the dedup
    // check here covers every path a duplicate can arrive on. Internal
    // queue rebuilds (`requeueUndelivered`) and the operator's deliberate
    // fresh chance (`replayDeadLetter`) bypass it via `skipDedup`: the
    // former never re-delivers, the latter is explicit operator intent.
    if (!skipDedup && this.checkSubscriberDedup(subscriber, msg)) {
      return 'duplicate';
    }
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
    const lag = subscriber.lag;
    if (lag != null && result === 'accepted') {
      // Stamp the enqueue time for the lag watermark monitor (see
      // `SubscribeOptions.lagMonitor`): the watermark reads the oldest
      // queued message's stamp, so every accepted enqueue is stamped.
      // Evaluate the watermark on every enqueue: a message arriving while
      // the head has been waiting past the threshold trips the alert here,
      // without waiting for the next drain.
      const nowMs = this.now();
      lag.enqueuedAt.set(msg, nowMs);
      this.checkLag(subscriber, lag, nowMs);
    }
    const ackLatency = subscriber.ackLatency;
    if (ackLatency != null && result === 'accepted') {
      // Stamp the accepted time for the ack-latency tracker (see
      // `SubscribeOptions.ackLatency`). Only tracked subscribers pay for
      // the clock read; a dropped message never reaches a handler, so it
      // is never stamped. Requeues (nack redelivery, ack-timeout requeue)
      // re-stamp: the ack clock restarts on every redelivery, while the
      // tracker still records at most one sample per message.
      ackLatency.tracker.accepted(msg);
    }
    const tracer = this.trace;
    if (tracer !== undefined && result === 'accepted' && tracer.hasTrace(msg)) {
      // One `bus.enqueue` span per subscriber that accepted the message,
      // parented to the publish root (see `EventBusOptions.trace`). A
      // dropped message never reaches a handler, so it leaves no span —
      // the drop is already visible in the drop counters. Requeues (nack /
      // ack-timeout redelivery) emit again: the retry is a new enqueue
      // event in the same trace.
      tracer.enqueueSpan(subscriber, msg, this.now(), 0);
    }
    return result;
  }

  /**
   * Invokes a subscriber's handler wrapped in a `bus.deliver` trace span
   * when the message is part of a sampled trace (EB-45). Untraced
   * messages — including every message when tracing is disabled — pay a
   * single branch: no allocation, no clock read.
   */
  private deliverTraced(subscriber: Subscriber, msg: BusMessage, invoke: () => void): void {
    const tracer = this.trace;
    if (tracer === undefined || !tracer.hasTrace(msg)) {
      invoke();
      return;
    }
    const at = this.now();
    invoke();
    tracer.deliverSpan(subscriber, msg, at, this.now() - at);
  }

  /**
   * Batch variant of `deliverTraced`: one handler invocation delivers the
   * whole batch, so every traced message in it shares the invocation's
   * span window — each message still gets its own `bus.deliver` span,
   * parented to its own `bus.enqueue` span.
   */
  private deliverBatchTraced(
    subscriber: Subscriber,
    batch: BusMessage[],
    invoke: () => void,
  ): void {
    const tracer = this.trace;
    if (tracer === undefined) {
      invoke();
      return;
    }
    let anyTraced = false;
    for (const msg of batch) {
      if (tracer.hasTrace(msg)) {
        anyTraced = true;
        break;
      }
    }
    if (!anyTraced) {
      invoke();
      return;
    }
    const at = this.now();
    invoke();
    const durationMs = this.now() - at;
    for (const msg of batch) {
      if (tracer.hasTrace(msg)) tracer.deliverSpan(subscriber, msg, at, durationMs);
    }
  }

  /**
   * Records one enqueue→drain dwell sample for a lag-monitored
   * subscriber, at the moment a message leaves its queue for delivery.
   * `nowMs` is the flush's single clock reading, so every sample in one
   * flush round shares the same drain timestamp.
   */
  private recordLagSample(subscriber: Subscriber, msg: BusMessage, nowMs: number): void {
    const lag = subscriber.lag;
    if (lag == null) return;
    const enqueuedAt = lag.enqueuedAt.get(msg);
    lag.enqueuedAt.delete(msg);
    if (enqueuedAt === undefined) return;
    lag.tracker.record(nowMs - enqueuedAt);
  }

  /**
   * The subscriber's live consumer-lag watermark: how long the oldest
   * message currently sitting in its queue has been waiting, in
   * milliseconds. 0 when the queue is empty or monitoring is disabled.
   * Messages already dequeued into a batch subscriber's pending batch are
   * past the queue and no longer count.
   */
  private lagWatermarkMs(subscriber: Subscriber, nowMs: number): number {
    const lag = subscriber.lag;
    if (lag == null) return 0;
    const head = subscriber.queue.peekOldest();
    if (head === undefined) return 0;
    const enqueuedAt = lag.enqueuedAt.get(head);
    if (enqueuedAt === undefined) return 0;
    return Math.max(0, nowMs - enqueuedAt);
  }

  /**
   * Evaluates a lag-monitored subscriber's watermark against its alert
   * threshold. Fires `onLag` once per excursion — when the watermark
   * reaches `thresholdMs` while the latch is clear — and re-arms the latch
   * when the watermark drops below the threshold. No threshold (or no
   * callback) means no alerting; the watermark is still reported by
   * `getStats()`.
   */
  private checkLag(subscriber: Subscriber, lag: SubscriberLagState, nowMs: number): void {
    const thresholdMs = lag.thresholdMs;
    const onLag = lag.onLag;
    if (thresholdMs === undefined || onLag === undefined) return;
    const watermarkMs = this.lagWatermarkMs(subscriber, nowMs);
    if (watermarkMs >= thresholdMs) {
      if (!lag.alerted) {
        lag.alerted = true;
        onLag({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          lagMs: watermarkMs,
          thresholdMs,
          queueSize: subscriber.queue.size,
          at: nowMs,
        });
      }
    } else {
      lag.alerted = false;
    }
  }

  /**
   * Evaluates one (subscriber, key) ordering stream's reorder-buffer depth
   * against the hotspot alert threshold (see `SubscribeOptions.keyHotspot`
   * and `src/keyhotspot.ts`). Fires `onKeyHotspot` once per excursion —
   * when the depth reaches `thresholdDepth` while the stream's latch is
   * clear — and re-arms the latch when the depth drops below the
   * threshold. A pure read of `order.buffer.size`: the reorder buffer,
   * the per-key expectation, and delivery order are never touched, so
   * detection cannot disturb per-key publish-order enforcement. A
   * throwing callback propagates to the caller, like the other subscriber
   * monitoring callbacks.
   */
  private checkKeyHotspot(subscriber: Subscriber, mapKey: string, order: KeyOrderState): void {
    const monitor = subscriber.keyHotspot;
    if (monitor === undefined) return;
    const depth = order.buffer.size;
    if (depth >= monitor.thresholdDepth) {
      if (!monitor.alertedKeys.has(mapKey)) {
        monitor.alertedKeys.add(mapKey);
        const onKeyHotspot = monitor.onKeyHotspot;
        if (onKeyHotspot !== undefined) {
          const [, key] = splitKeyOrderKey(mapKey);
          onKeyHotspot({
            subscriberId: subscriber.id,
            pattern: subscriber.pattern,
            key,
            bufferedDepth: depth,
            thresholdDepth: monitor.thresholdDepth,
            at: this.now(),
          });
        }
      }
    } else {
      monitor.alertedKeys.delete(mapKey);
    }
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
   * Records one handler processing-time sample for an SLO-tracked
   * subscriber: `now - startedAtMs`, where `startedAtMs` is the bus-clock
   * reading taken immediately before the handler invocation (plain and
   * batched paths) or before the delivery was handed out (reliable path,
   * sampled on `ack()` completion). A clock that moved backwards clamps
   * the sample at 0. Call sites only invoke this when SLO tracking is
   * enabled for the subscriber — untracked subscribers pay no clock read
   * and no branch beyond the call-site check. Evaluates the SLO after
   * every sample, so a threshold crossing fires on the sample that
   * crossed it.
   */
  private recordProcessingLatency(
    subscriber: Subscriber,
    slo: SubscriberLatencySloState,
    startedAtMs: number,
  ): void {
    slo.tracker.record(Math.max(0, this.now() - startedAtMs));
    this.checkProcessingLatencySlo(subscriber, slo);
  }

  /**
   * The invocation-time SLO state to sample, if any: reliable
   * subscribers sample exclusively on `ack()` completion (see the ack
   * wrapper in `subscribeReliable`), so the synchronous
   * handler-invocation timing must not sample them too — otherwise every
   * acked message would contribute two samples. Plain (and batched)
   * subscribers sample on handler invocation.
   */
  private invocationLatencySlo(subscriber: Subscriber): SubscriberLatencySloState | undefined {
    return subscriber.reliable == null ? subscriber.latencySlo : undefined;
  }

  /**
   * Evaluates a subscriber's windowed processing-latency p99 against its
   * SLO threshold (`SubscribeOptions.latencySlo`). Fires
   * `onLatencySloMiss` once per excursion — when the p99 exceeds the
   * threshold while the latch is clear — and re-arms the latch when the
   * p99 drops back to or below the threshold, mirroring the `onLag`
   * latch in `checkLag`.
   *
   * Advisory only: the alert never pauses, degrades, or otherwise
   * disturbs delivery — it is orthogonal to the health probe (a slow
   * handler trips this alert; only throws and `processingTimeoutMs`
   * overruns count toward health degradation). A throwing callback is
   * swallowed deliberately: unlike the other subscriber monitoring
   * callbacks, an SLO alert must never propagate into the flush loop.
   */
  private checkProcessingLatencySlo(
    subscriber: Subscriber,
    slo: SubscriberLatencySloState,
  ): void {
    const summary = slo.tracker.summary();
    if (summary.p99Ms > slo.thresholdMs) {
      if (!slo.alerted) {
        slo.alerted = true;
        const onLatencySloMiss = slo.onLatencySloMiss;
        if (onLatencySloMiss !== undefined) {
          try {
            onLatencySloMiss({
              subscriberId: subscriber.id,
              pattern: subscriber.pattern,
              p99Ms: summary.p99Ms,
              thresholdMs: slo.thresholdMs,
              samples: summary.samples,
              at: this.now(),
            });
          } catch {
            // Advisory-only alert: a throwing SLO callback must never
            // disturb the delivery flow.
          }
        }
      }
    } else {
      slo.alerted = false;
    }
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
   *
   * `keyed` carries the message's per-key sequence number (see
   * `PublishOptions.key`): when present, delivery goes through the
   * per-(subscriber, key) ordering gate (`deliverKeyed`) instead of
   * straight to the queue.
   *
   * `causal` carries the message's causal clock (see
   * `PublishOptions.causal`): when the subscriber opted into
   * `SubscribeOptions.causal`, delivery goes through the per-(subscriber,
   * source) happens-before gate (`deliverCausal`) first — causal order
   * takes precedence over keyed publish order — and an admitted message
   * then continues through the keyed gate when keyed.
   */
  private deliverToSubscriber(
    subscriber: Subscriber,
    msg: BusMessage,
    expiresAt: number | undefined,
    rawPayload: unknown,
    keyed?: { key: string; keySeq: number },
    causal?: { source: string; clock: number },
  ): boolean {
    if (causal !== undefined && subscriber.causal !== undefined) {
      return this.deliverCausal(
        subscriber,
        { msg, expiresAt, rawPayload, replay: false, keyed },
        causal.source,
        causal.clock,
      );
    }
    if (keyed !== undefined) {
      return this.deliverKeyed(
        subscriber,
        { msg, expiresAt, rawPayload, replay: false },
        keyed.key,
        keyed.keySeq,
      );
    }
    return this.deliverUnkeyed(subscriber, msg, expiresAt, rawPayload);
  }

  /**
   * The unordered delivery path: content filter, then adaptive
   * publish-side throttling, then the queue. Keyed messages reach it
   * through `deliverKeyed` once the ordering gate admits them.
   */
  private deliverUnkeyed(
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
      this.countFiltered(subscriber, msg.topic, msg.seq, msg.epoch ?? '');
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
   * Per-(subscriber, key) publish-order delivery gate (see
   * `PublishOptions.key`). KeySeqs are assigned at admission in publish
   * order, but fan-out order can differ — a delayed schedule fans out
   * after live publishes with higher keySeqs — so a keyed message whose
   * predecessors have not been fanned out yet waits in the per-key reorder
   * buffer (pre-queue: no backpressure budget consumed, no filter or
   * throttle evaluated yet) until they are admitted.
   *
   * Baseline rule, mirroring gap detection: the first keyed message fanned
   * out to this subscriber for a key establishes `expected` at its keySeq —
   * a subscriber that joins late must not hang on keySeqs admitted before
   * it existed. A keySeq below `expected` is a late arrival (admitted
   * before the baseline, e.g. a delayed message scheduled before this
   * subscriber subscribed): it is delivered immediately, never blocking
   * the stream on the past.
   *
   * Every wait terminates: a keySeq that will never be fanned out to this
   * subscriber is marked skipped (`skipKeySeq`) — published to a
   * non-matching topic, or lost before fan-out (cancelled/expired/shed
   * delayed schedule, filtered replay) — and the expectation cascades past
   * contiguous skips, releasing buffered successors in order.
   */
  private deliverKeyed(
    subscriber: Subscriber,
    delivery: KeyedDelivery,
    key: string,
    keySeq: number,
  ): boolean {
    // Per-epoch ordering streams: the node's own publish stream and each
    // hub's forwarded stream number the same key independently, so each
    // epoch gets its own expectation — a foreign epoch can neither wedge
    // this gate nor corrupt its order.
    const mapKey = keyOrderKey(delivery.msg.epoch ?? '', key);
    let order = subscriber.keyOrder?.get(mapKey);
    if (order === undefined) {
      order = { expected: keySeq, skipped: new Set<number>(), buffer: new Map() };
      if (subscriber.keyOrder === undefined) subscriber.keyOrder = new Map();
      subscriber.keyOrder.set(mapKey, order);
    }
    if (order.skipped.delete(keySeq)) {
      // Defensive: a keySeq marked as never-arriving showed up anyway —
      // deliver it rather than hang the key on a stale mark.
      return this.admitKeyed(subscriber, delivery);
    }
    if (keySeq < order.expected) {
      return this.admitKeyed(subscriber, delivery);
    }
    if (keySeq > order.expected) {
      order.buffer.set(keySeq, delivery);
      this.totalKeyedReordered += 1;
      // Keyed hotspot sampling (see `SubscribeOptions.keyHotspot`): a pure
      // read of the buffer depth — the reorder buffer itself is untouched,
      // so detection can never disturb per-key publish-order enforcement.
      this.checkKeyHotspot(subscriber, mapKey, order);
      // Admitted into the ordering layer — it will reach the queue once
      // its predecessors are admitted, so it counts as accepted for the
      // fan-out width, exactly like a queued message.
      return true;
    }
    const accepted = this.admitKeyed(subscriber, delivery);
    order.expected = keySeq + 1;
    this.cascadeKeyExpected(subscriber, order, mapKey);
    return accepted;
  }

  /**
   * Admits one keyed delivery whose turn has come: live messages run the
   * filter/throttle/queue gate (`deliverUnkeyed`); replayed messages go
   * straight to the queue (the filter and linger checks already ran in
   * `replayLog`, and replay never burned throttle budget).
   */
  private admitKeyed(subscriber: Subscriber, delivery: KeyedDelivery): boolean {
    if (delivery.replay) {
      return this.enqueueMessage(subscriber, delivery.msg, delivery.expiresAt) === 'accepted';
    }
    return this.deliverUnkeyed(subscriber, delivery.msg, delivery.expiresAt, delivery.rawPayload);
  }

  /**
   * Advances a per-key expectation past everything now deliverable:
   * contiguous skipped keySeqs are dropped, then contiguous buffered
   * messages are admitted in keySeq order. Each iteration strictly moves
   * `expected` forward, so the loop always terminates. `mapKey` is the
   * epoch-scoped ordering-stream key (`keyOrderKey`), used for the
   * hotspot depth check at the end — the buffer only ever shrinks here,
   * so one end-of-cascade evaluation covers every release.
   */
  private cascadeKeyExpected(subscriber: Subscriber, order: KeyOrderState, mapKey: string): void {
    for (;;) {
      if (order.skipped.delete(order.expected)) {
        order.expected += 1;
        continue;
      }
      const next = order.buffer.get(order.expected);
      if (next === undefined) break;
      order.buffer.delete(order.expected);
      this.admitKeyed(subscriber, next);
      order.expected += 1;
    }
    // The cascade only ever shrinks the buffer, so a single end-of-cascade
    // evaluation covers every release — the re-arm point for the hotspot
    // alert latch (see `SubscribeOptions.keyHotspot`).
    this.checkKeyHotspot(subscriber, mapKey, order);
  }

  /**
   * Ensures a per-(subscriber, key) ordering entry exists at keyed
   * admission time. Used by `publishDelayed`: admission (schedule) and
   * fan-out (due time) are separated, so a matching subscriber must learn
   * the admitted keySeq NOW — otherwise a later keySeq fanned out first
   * would establish the baseline there and the delayed message would
   * arrive as a "late" out-of-order delivery. A non-matching subscriber's
   * baseline moves past the keySeq (it will never be fanned out to it).
   * Direct publishes need no eager init: admission and fan-out are one
   * synchronous step, so `deliverKeyed`'s baseline rule establishes the
   * same expectation at fan-out.
   */
  private initKeyBaseline(
    subscriber: Subscriber,
    key: string,
    keySeq: number,
    matches: boolean,
  ): void {
    // Schedule-time admission is always the node's own publish stream.
    const mapKey = keyOrderKey('', key);
    let order = subscriber.keyOrder?.get(mapKey);
    if (order === undefined) {
      if (subscriber.keyOrder === undefined) subscriber.keyOrder = new Map();
      order = { expected: matches ? keySeq : keySeq + 1, skipped: new Set(), buffer: new Map() };
      subscriber.keyOrder.set(mapKey, order);
      return;
    }
    if (!matches) this.skipKeySeq(subscriber, key, keySeq);
    // Matching with an existing entry: the baseline already covers this
    // keySeq's position — nothing to do.
  }

  /**
   * Marks one keySeq as never-to-be-fanned-out for this subscriber and
   * cascades the expectation past it. Called when a keyed message is
   * published to a topic the subscriber's pattern does not match (at
   * schedule time for delayed messages, at fan-out time for direct ones),
   * and when a keyed schedule dies before fan-out (cancelled, TTL-expired,
   * or shed by the rate limiter). Without this, a subscriber buffering a
   * later keySeq would wait forever for a predecessor it will never see.
   *
   * `epoch` selects the ordering stream ('' for the node's own publishes,
   * `hub:<hubEpoch>` for hub-forwarded traffic): a keySeq that will never
   * arrive on one stream must not disturb the other's expectation.
   *
   * No entry (the subscriber never received this key) needs no mark: the
   * baseline rule in `deliverKeyed` establishes `expected` at the first
   * keyed message actually fanned out to it.
   */
  private skipKeySeq(subscriber: Subscriber, key: string, keySeq: number, epoch = ''): void {
    const order = subscriber.keyOrder?.get(keyOrderKey(epoch, key));
    if (order === undefined || keySeq < order.expected || order.skipped.has(keySeq)) return;
    order.skipped.add(keySeq);
    this.cascadeKeyExpected(subscriber, order, keyOrderKey(epoch, key));
  }

  /**
   * Releases every subscriber's per-key expectation past a keySeq that
   * will never fan out: a cancelled or TTL-expired delayed schedule, a
   * delayed message shed by the rate limiter at fan-out, or a schedule
   * whose durable-log write failed. O(subscribers) on rare paths only.
   * Only the node's own publish stream ('') is released — hub-forwarded
   * streams never originate here.
   */
  private releaseKeySequence(key: string, keySeq: number): void {
    for (const subscriber of this.subscribers.values()) {
      const keyOrder = subscriber.keyOrder;
      if (keyOrder === undefined) continue;
      for (const [mapKey, order] of keyOrder) {
        const [epoch, k] = splitKeyOrderKey(mapKey);
        if (epoch === '' && k === key) {
          if (keySeq >= order.expected && !order.skipped.has(keySeq)) {
            order.skipped.add(keySeq);
            this.cascadeKeyExpected(subscriber, order, mapKey);
          }
        }
      }
    }
  }

  /**
   * Per-(subscriber, source) happens-before delivery gate (see
   * `SubscribeOptions.causal` and `PublishOptions.causal`). A message is
   * deliverable if and only if its clock equals the source's next
   * expected clock (starting at 0 — producers number each source's
   * clocks from 0); an early arrival waits in the per-source reorder
   * buffer (pre-queue: no backpressure budget consumed, no filter or
   * throttle evaluated yet) until its dependencies are admitted.
   *
   * Clock regression (clock below the expectation — a duplicate, or a
   * late arrival from before the subscriber's horizon) is delivered
   * immediately without moving the expectation backwards, and counted.
   *
   * Every wait terminates: each per-source buffer is bounded by
   * `maxBufferPerSource` — when a new arrival would exceed it, the
   * oldest buffered message is dropped (drop-oldest) and the expectation
   * advances past it, so a dependency that never arrives cannot wedge
   * the stream. Clocks that will never be fanned out to this subscriber
   * are marked skipped (`skipCausalClock`) — published to non-matching
   * topics, assigned to a different group member, filtered replay — and
   * the expectation cascades past them instead of hanging.
   */
  private deliverCausal(
    subscriber: Subscriber,
    delivery: CausalDelivery,
    source: string,
    clock: number,
  ): boolean {
    // The caller (`deliverToSubscriber`) guarantees the gate exists.
    const gate = subscriber.causal as CausalGateState;
    const skipped = gate.skipped.get(source);
    if (skipped !== undefined && skipped.delete(clock)) {
      // Defensive: a clock marked as never-arriving showed up anyway —
      // deliver it rather than hang the source on a stale mark.
      if (skipped.size === 0) gate.skipped.delete(source);
      this.admitCausal(subscriber, delivery);
      return true;
    }
    const expected = gate.expected.get(source) ?? 0;
    if (clock < expected) {
      // Clock regression: the dependency already passed — deliver
      // immediately, never move the expectation backwards.
      this.totalCausalRegressed += 1;
      this.admitCausal(subscriber, delivery);
      return true;
    }
    if (clock > expected) {
      let buffer = gate.buffers.get(source);
      if (buffer === undefined) {
        buffer = new Map();
        gate.buffers.set(source, buffer);
      }
      buffer.set(clock, delivery);
      // Bounded buffer (anti-deadlock): drop the oldest buffered clock
      // and advance the expectation past it, releasing whatever the
      // advance unblocks. At most one drop per arrival — the buffer was
      // within budget before this insert — but the loop is defensive.
      let nextExpected = expected;
      while (buffer.size > gate.maxBufferPerSource) {
        let oldest = Infinity;
        for (const c of buffer.keys()) {
          if (c < oldest) oldest = c;
        }
        buffer.delete(oldest);
        this.totalCausalDropped += 1;
        nextExpected = oldest + 1;
      }
      gate.expected.set(source, nextExpected);
      this.cascadeCausal(subscriber, gate, source);
      // Admitted into the ordering layer — it will reach the queue once
      // its dependencies are admitted, so it counts as accepted for the
      // fan-out width, exactly like a queued message.
      return true;
    }
    this.admitCausal(subscriber, delivery);
    gate.expected.set(source, expected + 1);
    this.cascadeCausal(subscriber, gate, source);
    return true;
  }

  /**
   * Admits one causal delivery whose turn has come: keyed messages run
   * the per-(subscriber, key) ordering gate next — happens-before order
   * takes precedence over publish (keySeq) order — and everything else
   * goes through the unordered path (live) or straight to the queue
   * (replay: the filter and linger checks already ran in `replayLog`,
   * and replay never burned throttle budget).
   */
  private admitCausal(subscriber: Subscriber, delivery: CausalDelivery): boolean {
    const keyed = delivery.keyed;
    if (keyed !== undefined) {
      return this.deliverKeyed(
        subscriber,
        { msg: delivery.msg, expiresAt: delivery.expiresAt, rawPayload: delivery.rawPayload, replay: delivery.replay },
        keyed.key,
        keyed.keySeq,
      );
    }
    if (delivery.replay) {
      return this.enqueueMessage(subscriber, delivery.msg, delivery.expiresAt) === 'accepted';
    }
    return this.deliverUnkeyed(subscriber, delivery.msg, delivery.expiresAt, delivery.rawPayload);
  }

  /**
   * Advances a source's expectation past everything now deliverable:
   * contiguous skipped clocks are dropped, then contiguous buffered
   * messages are admitted in clock order. Each iteration strictly moves
   * `expected` forward, so the loop always terminates. A buffered message
   * whose TTL expired while waiting is dropped as expired at release —
   * never resurrected — and the stream advances past it instead of
   * deadlocking on a dependency that can never arrive in time.
   */
  private cascadeCausal(subscriber: Subscriber, gate: CausalGateState, source: string): void {
    const buffer = gate.buffers.get(source);
    const skipped = gate.skipped.get(source);
    let expected = gate.expected.get(source) ?? 0;
    if (buffer === undefined || buffer.size === 0) {
      // No buffer: still advance past contiguous skips, or a skipped
      // clock below the next arrival would wedge the stream.
      if (skipped !== undefined) {
        while (skipped.delete(expected)) expected += 1;
        if (skipped.size === 0) gate.skipped.delete(source);
        gate.expected.set(source, expected);
      }
      return;
    }
    const nowMs = this.now();
    for (;;) {
      if (skipped !== undefined && skipped.delete(expected)) {
        expected += 1;
        continue;
      }
      const next = buffer.get(expected);
      if (next === undefined) break;
      buffer.delete(expected);
      if (next.expiresAt !== undefined && nowMs >= next.expiresAt) {
        this.recordExpired(next.msg.topic);
      } else {
        this.admitCausal(subscriber, next);
      }
      expected += 1;
    }
    gate.expected.set(source, expected);
    if (skipped !== undefined && skipped.size === 0) gate.skipped.delete(source);
    if (buffer.size === 0) gate.buffers.delete(source);
  }

  /**
   * Marks one causal clock as never-to-be-fanned-out for this subscriber
   * and cascades the source's expectation past it. Called when a causal
   * message is published to a topic the subscriber's pattern does not
   * match, when it is assigned to a different group member, and when a
   * replayed causal message is filtered out — otherwise a subscriber
   * buffering a later clock for the same source would wait for a
   * dependency it will never see.
   *
   * No gate (the subscriber did not opt into `SubscribeOptions.causal`)
   * needs no mark: non-causal subscribers never consult clocks.
   */
  private skipCausalClock(subscriber: Subscriber, source: string, clock: number): void {
    const gate = subscriber.causal;
    if (gate === undefined) return;
    const expected = gate.expected.get(source) ?? 0;
    if (clock < expected) return;
    let skipped = gate.skipped.get(source);
    if (skipped === undefined) {
      skipped = new Set();
      gate.skipped.set(source, skipped);
    }
    if (skipped.has(clock)) return;
    skipped.add(clock);
    this.cascadeCausal(subscriber, gate, source);
  }

  /**
   * Advances a subscriber's per-topic gap baseline over a deliberately
   * skipped message (content filter, handoff linger): the skip must not
   * churn backpressure or count as a sequence gap. Epoch-aware like
   * `detectGap` — a skip from a different epoch re-establishes the
   * baseline instead of comparing across numbering spaces.
   */
  private advanceBaseline(subscriber: Subscriber, topic: string, seq: number, epoch = ''): void {
    const last = subscriber.lastDeliveredSeq.get(topic);
    const lastEpoch = subscriber.lastEpoch.get(topic);
    if (last === undefined || lastEpoch !== epoch) {
      subscriber.lastDeliveredSeq.set(topic, seq);
      subscriber.lastEpoch.set(topic, epoch);
    } else if (seq > last) {
      subscriber.lastDeliveredSeq.set(topic, seq);
    }
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
  private countFiltered(subscriber: Subscriber, topic: string, seq: number, epoch = ''): void {
    this.advanceBaseline(subscriber, topic, seq, epoch);
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
   * `key` is the message key for direct publishes (see `PublishOptions.key`);
   * delayed messages carry theirs on `delayed` instead. The key is stamped
   * onto the delivery's durable-log record. `preassignedKeySeq` carries a
   * per-key sequence number drawn during `publishAtomic` admission — the
   * commit phase must reuse it verbatim so the batch keeps its admission
   * order; direct publishes draw theirs below, after the admission gates.
   *
   * `routed` marks a publish that already travelled a topic route (see
   * `PublishOptions.routed` / `EventBus.setTopicRoute`): routed messages
   * never trigger routing again, so one message forwards at most one hop
   * and the route table cannot amplify it into a loop. `routedExpiresAt`
   * carries a TTL deadline stamped by an earlier publish in the chain —
   * when defined it is used verbatim (a route forward never resets the
   * deadline); when undefined the destination's own TTL rules apply
   * normally. Only the route forwarder sets it.
   *
   * `diagnostic` marks a message published by the DLQ diagnostic emitter
   * (see `DeadLetterOptions.diagnosticTopic`): a dead-lettered diagnostic
   * message never emits a second diagnostic, cutting the recursion at the
   * source. Only the bus's own diagnostic emitter sets it.
   */
  private fanOut(
    topic: string,
    payload: unknown,
    preAdmitted = false,
    delayed?: DelayedFanOut,
    key?: string,
    preassignedKeySeq?: number,
    traceparent?: string,
    messageId?: string,
    routed = false,
    routedExpiresAt?: number,
    fromBridge = false,
    diagnostic = false,
    eventTime?: number,
    causal?: { source?: string; clock: number },
  ): { matched: number; accepted: number; admitted: boolean } {
    // Delivery tracing (EB-45): one clock read for the publish span, taken
    // only when tracing is enabled — the disabled path pays this single
    // branch and nothing else (no allocation, no WeakMap lookup).
    const tracer = this.trace;
    const fanOutStart = tracer !== undefined ? this.now() : 0;
    // Topic aliases (EB-44) resolve before every other admission gate: the
    // resolved topic is the real publish topic — ACL, schema validation,
    // rate-limit budget, TTL, compression, the durable log and the
    // per-topic sequence number all key off it. A publish naming a retired
    // (TTL-expired) old topic is rejected here with reason 'alias-retired':
    // it consumes no sequence number (subscribers see no gap), never
    // touches the durable log, and burns no rate-limit budget. Resolution
    // is idempotent, so pre-admitted paths (atomic commit, delayed
    // fan-out) that already resolved at their own admission simply no-op
    // here — while an alias registered in between still takes effect, and
    // an alias that retired in between still refuses the publish.
    const aliasResolution = this.resolvePublishTopic(topic);
    if (aliasResolution.retired) {
      this.countAliasRetired(topic, payload);
      return { matched: 0, accepted: 0, admitted: false };
    }
    topic = aliasResolution.topic;
    // Causal clock (see `PublishOptions.causal`): validated by the
    // publishing entry point; the source defaults to the resolved topic,
    // so alias resolution keeps the stream identity stable.
    const causalStamp =
      causal === undefined ? undefined : { source: causal.source ?? topic, clock: causal.clock };
    // Publish-side schema validation runs before admission: a rejected
    // payload never becomes a message — no sequence number is consumed
    // (subscribers see no gap), the durable log never sees it, and it
    // does not burn rate-limit budget. Rejection is counted on the
    // topic's stats entry, which is created here when the topic has never
    // published anything valid yet.
    if (!preAdmitted) {
      // Broker-level ACL runs before every other admission gate (topic
      // aliases already resolved above): an
      // unauthorized publish is rejected before schema validation — it
      // consumes no sequence number (subscribers see no gap), never touches
      // the durable log, and burns no rate-limit budget. Counted as a
      // rejection with reason 'acl' (see countAclDenied).
      if (!this.aclAllowsPublish(topic)) {
        this.countAclDenied(topic, payload);
        return { matched: 0, accepted: 0, admitted: false };
      }
      const validator = this.schemaForTopic(topic);
      if (validator !== undefined && !validator(payload, topic)) {
        this.countSchemaRejection(topic, payload);
        return { matched: 0, accepted: 0, admitted: false };
      }
    }
    // The message identity rides the envelope end to end (durable log,
    // replay, cluster forwarding, redeliveries) for subscriber-side
    // dedup. Only non-empty strings count — anything else is absent,
    // mirroring `publishIdempotent`'s lenient treatment.
    const cleanMessageId = typeof messageId === 'string' && messageId.length > 0 ? messageId : undefined;
    const msg: BusMessage = {
      topic,
      payload,
      seq: this.nextSeq(topic),
      ...(cleanMessageId === undefined ? {} : { messageId: cleanMessageId }),
      // Business event time rides the envelope (see `PublishOptions.eventTime`):
      // undefined stays absent, so messages without one never participate in
      // the watermark.
      ...(eventTime === undefined ? {} : { eventTime }),
    };
    // EB-57: mark DLQ diagnostic messages at creation (the envelope marker
    // is bus-side, so a user's payload can never collide with it) — a
    // dead-lettered diagnostic message must not emit a second diagnostic.
    if (diagnostic) this.diagnosticMessages.add(msg);
    const stats = this.statsFor(topic);
    stats.publishedMessages += 1;
    stats.lastSeq = msg.seq;
    this.totalPublished += 1;
    // A delayed fan-out carries the per-key sequence number assigned at
    // schedule time (publish order) — it is used verbatim below, and it is
    // what the release hook needs when the rate limiter sheds the message.
    const delayedKeyed =
      delayed?.key !== undefined && delayed.keySeq !== undefined
        ? { key: delayed.key, keySeq: delayed.keySeq }
        : undefined;
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
        this.countRateLimited(topic, payload);
        // The keySeq was consumed at schedule time but the message never
        // fans out: release every subscriber's expectation past it, or a
        // subscriber buffering a later keySeq for the same key would wait
        // forever. (Direct publishes draw their keySeq after this gate,
        // so a shed direct publish consumes nothing.)
        if (delayedKeyed !== undefined) {
          this.releaseKeySequence(delayedKeyed.key, delayedKeyed.keySeq);
        }
        return { matched: 0, accepted: 0, admitted: false };
      }
    }
    // Delivery-trace sampling decision (EB-45): head-based, taken once the
    // message is admitted — a schema rejection or rate-limit shed above
    // never starts a trace. Every downstream span of this publish shares
    // the verdict. `beginPublish` registers the trace record before
    // fan-out, so the enqueue spans below can find it.
    let openTrace: ReturnType<TraceRecorder['beginPublish']> | undefined;
    if (tracer !== undefined) {
      const sampled = tracer.sample(topic, traceparent);
      if (sampled !== undefined) {
        openTrace = tracer.beginPublish(msg, topic, msg.seq, sampled, fanOutStart);
        openTrace.endAdmission(this.now());
      }
    }
    // Per-key publish-order sequence (see `PublishOptions.key`). Delayed
    // messages reuse their schedule-time keySeq; `publishAtomic` reuses its
    // admission-time keySeq; direct publishes draw here — after the
    // admission gates, so a schema rejection or rate-limit shed never
    // consumes a key sequence (a consumed-but-never-fanned-out keySeq
    // would hang every subscriber buffering a later keySeq for the key).
    const keyed =
      delayedKeyed ??
      (key === undefined ? undefined : { key, keySeq: preassignedKeySeq ?? this.nextKeySeq(key) });
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
    // Event-time watermark (EB-59): observed once per accepted publish —
    // after the admission gates (schema validation, rate-limit budget), so
    // a rejected or shed message never moves the watermark, exactly like
    // the publish-rate table above. The watermark is orthogonal to the
    // publish-order `seq` stamped at the top: `seq` orders arrivals, the
    // watermark orders business time.
    if (eventTime !== undefined) {
      this.observeEventTime(topic, msg.seq, eventTime);
    }
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
      // Auto-trained dictionaries (EB-60): admitted publishes feed the
      // topic's sample window. Sampling costs one serialization per
      // publish on auto-trained topics — the price of a training corpus —
      // and nothing more on this path: training itself runs on an unref'd
      // background timer, and publishes always compress with the
      // dictionary in force when they were admitted.
      const autoTrainer = compressionRule.autoTrainState;
      if (autoTrainer !== undefined) autoTrainer.sample(payload);
    }
    // One clock reading for the publish: TTL deadline and log timestamp stay
    // consistent even if the injected clock moves between the two.
    const nowMs = this.now();
    const ttlMs = this.ttlForTopic(topic);
    // A delayed fan-out keeps the TTL deadline stamped at schedule time —
    // it is "published" then and only delivered later. Note the explicit
    // `delayed !== undefined` check instead of `??`: an explicit "no
    // deadline" from schedule time must not fall through to the rules in
    // effect now (a rule added in between does not retroactively expire
    // the message). A routed forward carries the source message's deadline
    // verbatim — routing never resets it — so it wins over the
    // destination's own TTL rules; when the source message had no
    // deadline, the destination's rules apply normally.
    const expiresAt =
      routedExpiresAt !== undefined
        ? routedExpiresAt
        : delayed !== undefined
          ? delayed.expiresAt
          : ttlMs === undefined
            ? undefined
            : nowMs + ttlMs;
    if (expiresAt !== undefined) this.messageDeadlines.set(msg, expiresAt);
    // Cross-process bridge (EB-51): an admitted local publish is mirrored
    // to the configured transport so other nodes can fan it out. The RAW
    // application payload is mirrored — never the compressed envelope —
    // and the receiving node applies its own admission (schema,
    // rate-limit, TTL), compression, and durable-log rules. Identity
    // rides along: the key with its publish-order keySeq (per-key ordering
    // survives the hop when each key has a single publishing node), the
    // messageId, the end-to-end traceId, and the source TTL deadline
    // verbatim. A mirror failure (throw or rejected promise) is swallowed:
    // the bus keeps serving locally, exactly like the cluster forward
    // path. Envelopes arriving FROM the bridge (`receiveFromBridge`) are
    // never mirrored back — that is what keeps a multi-node bridge
    // loop-free.
    const bridge = this.bridge;
    if (bridge !== undefined && !fromBridge) {
      this.bridgeOutbound += 1;
      const traceId = this.trace?.traceIdOf(msg);
      const envelope: BridgeEnvelope = {
        topic,
        payload,
        ...(keyed === undefined
          ? {}
          : { key: keyed.key, keySeq: keyed.keySeq }),
        ...(cleanMessageId === undefined ? {} : { messageId: cleanMessageId }),
        ...(traceId === undefined ? {} : { traceId }),
        ...(expiresAt === undefined ? {} : { expiresAt }),
      };
      try {
        const result = bridge.transport.publish(envelope);
        if (result !== undefined && typeof (result as Promise<void>).catch === 'function') {
          (result as Promise<void>).catch(() => {
            // Swallowed: a failing transport must never break publishing.
          });
        }
      } catch {
        // Swallowed: see above.
      }
    }
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
    // delivery record. A message `key` — from the direct publish options
    // or carried over from the delayed schedule — rides on the delivery
    // record together with its per-key sequence number, so restart
    // recovery and replay can preserve per-key publish order.
    // Namespaced topics (EB-52) append to their namespace's child log.
    this.durableLogForTopic(topic)?.append({
      seq: msg.seq,
      topic,
      at: nowMs,
      expiresAt,
      payload: msg.payload,
      ...(delayed == null ? {} : { deliverAt: delayed.deliverAt, delayId: delayed.delayId }),
      ...(keyed === undefined ? {} : { key: keyed.key, keySeq: keyed.keySeq }),
      // Only present when a preset dictionary compressed this message:
      // replay needs the same bytes to inflate it.
      ...(compressedDictId === undefined ? {} : { dictId: compressedDictId }),
      // The message identity is logged with the record so replay
      // restores it onto the envelope — a resumed dedup window can only
      // suppress what it recognizes.
      ...(cleanMessageId === undefined ? {} : { messageId: cleanMessageId }),
      // The business event time rides the log record so replay restores it
      // onto the envelope (see `PublishOptions.eventTime`). The runtime
      // watermark itself is not persisted — a restarted bus rebuilds it
      // from new publishes.
      ...(eventTime === undefined ? {} : { eventTime }),
      // The causal clock rides the log record so replay restores it —
      // a causal subscriber keeps happens-before order across restarts
      // and archived-segment reads.
      ...(causalStamp === undefined ? {} : { causal: causalStamp }),
    });
    // Cluster federation (EB-37): when the hub link is up and the cached
    // route table shows subscribers for this topic on OTHER members, the
    // admitted message is additionally forwarded to the hub. The hub
    // stamps hub-global per-topic (and per-key) sequence numbers and
    // forwards only to members whose advertised patterns match — the
    // sender never receives its own forward back. Local delivery below is
    // unaffected: the node's own subscribers are served from the local
    // publish stream with node-local sequence numbers, so a transport
    // failure can never lose an admitted message locally. A forward
    // failure is counted on the link, never thrown into the publish path.
    const clusterLink = this.clusterLink;
    if (clusterLink != null && clusterLink.isConnected()) {
      const remoteMembers = clusterLink.matchingRemoteMembers(topic);
      if (remoteMembers.length > 0) {
        clusterLink.forwardPublish({
          topic,
          payload: msg.payload,
          ...(keyed === undefined ? {} : { key: keyed.key }),
          ...(expiresAt === undefined ? {} : { expiresAt }),
          ...(this.compressedPayloads.has(msg) ? { compressed: true as const } : {}),
          ...(compressedDictId === undefined ? {} : { dictId: compressedDictId }),
          ...(cleanMessageId === undefined ? {} : { messageId: cleanMessageId }),
        });
      }
    }
    // The prefix index prunes the regex tests down to subscribers whose
    // pattern's literal prefix can plausibly match the topic; the compiled
    // regex stays the final authority, and the no-miss invariant in
    // `candidateIds` keeps matching semantics identical to the old full
    // scan. Iteration order (insertion order) is unchanged.
    const { matched, accepted } = this.deliverMatched(msg, expiresAt, payload, keyed, causalStamp);
    stats.subscriberCount = matched;
    if (openTrace !== undefined) {
      // Spans emit in completion order — fanout, then the publish root —
      // so the ring buffer reads as a causal narrative per trace.
      const traceEnd = this.now();
      openTrace.endFanout(traceEnd, matched, accepted);
      openTrace.endPublish(traceEnd);
    }
    // Topic routes (EB-50): a non-routed message admitted on a routed
    // topic is forwarded to the route's destination as a normal dst
    // publish — full dst admission (ACL, schema, rate-limit), dst-side
    // sequence and rate-limit budget. Placement is deliberate: routing runs
    // after the source message is admitted, logged and fanned out, so a
    // throwing predicate can never corrupt the source delivery — it
    // propagates to the publish caller, like a throwing schema validator.
    // The forward carries the source message's trace id (the forwarded
    // publish continues the same trace) and its TTL deadline verbatim
    // (never reset by routing); the application key and message id ride
    // along so per-key publish order and subscriber-side dedup keep
    // working across the route. The forwarded message is marked routed,
    // so it never triggers routing again — one hop per message. The
    // forward is a side effect: it does not change this call's returned
    // accepted count, and the outer publish still schedules a single
    // flush for both messages.
    if (!routed && this.topicRoutes.size > 0) {
      const route = this.topicRoutes.get(topic);
      if (route !== undefined) {
        const routeMeta = {
          topic,
          seq: msg.seq,
          ...(cleanMessageId === undefined ? {} : { messageId: cleanMessageId }),
        };
        if (route.predicate === undefined || route.predicate(payload, routeMeta)) {
          route.forwarded += 1;
          this.fanOut(
            route.dst,
            payload,
            false,
            undefined,
            keyed?.key,
            undefined,
            openTrace === undefined
              ? undefined
              : formatTraceparent(openTrace.traceId, newSpanId()),
            cleanMessageId,
            true,
            expiresAt,
            false,
            false,
            // The forwarded message is the same logical event on the
            // destination topic: its business event time rides along so the
            // destination's event-time watermark sees the same stream.
            eventTime,
          );
        }
      }
    }
    // Cross-bus forward rules (EB-56): a non-routed message admitted on a
    // bus holding forward rules is additionally offered to every matching
    // rule's destination bus — same placement as the EB-50 topic routes
    // above, after the source message is admitted, logged and fanned out.
    // A rule fires when its srcPattern matches the published
    // (alias-resolved) topic. The destination bus runs its own full
    // admission pipeline on the forwarded message (`receiveForward`), so
    // the forward honors the destination's ACL, schema, rate-limit and
    // TTL rules with destination-side sequence numbers and budgets; the
    // forwarded message is stamped routed, so it never triggers the
    // destination's routes or forwards again — one hop per message. The
    // forward carries the source message's trace id (continues the same
    // end-to-end trace), its application key and message id, and its TTL
    // deadline verbatim (never reset by a forward; when the source had
    // none, the destination's TTL rules apply normally). The forward is a
    // side effect: it does not change this call's returned accepted
    // count, and each bus schedules its own flush.
    if (!routed && this.forwardRules.size > 0) {
      for (const [dstBus, rules] of this.forwardRules) {
        for (const rule of rules.values()) {
          if (!rule.matcher.test(topic)) continue;
          rule.forwarded += 1;
          dstBus.receiveForward({
            topic: rule.dstTopic ?? topic,
            payload,
            ...(keyed === undefined ? {} : { key: keyed.key }),
            ...(openTrace === undefined
              ? {}
              : { traceparent: formatTraceparent(openTrace.traceId, newSpanId()) }),
            ...(cleanMessageId === undefined ? {} : { messageId: cleanMessageId }),
            ...(expiresAt === undefined ? {} : { expiresAt }),
            ...(eventTime === undefined ? {} : { eventTime }),
          });
        }
      }
    }
    return { matched, accepted, admitted: true };
  }

  /**
   * Matches `msg` against every subscriber and delivers it: plain
   * subscribers each get their own copy; consumer-group members compete
   * for the group's single copy (round-robin over the competing set).
   * Shared by the local publish path (`fanOut`) and the cluster receive
   * path (`receiveClusterMessage`), so both get identical matching,
   * filtering, backpressure, and group semantics — the only difference is
   * the message's sequence epoch (`msg.epoch`), which the key-ordering
   * gate and gap detection already honor per epoch.
   *
   * `rawPayload` is the application payload as published — never a
   * compressed wire envelope — so content filters always judge the real
   * payload. `keyed` carries the per-key sequence number for the ordering
   * gate; its epoch is read from `msg.epoch`.
   */
  private deliverMatched(
    msg: BusMessage,
    expiresAt: number | undefined,
    rawPayload: unknown,
    keyed: { key: string; keySeq: number } | undefined,
    causal?: { source: string; clock: number },
  ): { matched: number; accepted: number } {
    const topic = msg.topic;
    const epoch = msg.epoch ?? '';
    let matched = 0;
    let accepted = 0;
    // Topic aliases (EB-44): the mirror side of the dual-write window. A
    // message on the resolved topic additionally reaches subscribers of
    // every live old topic that resolves to it — old consumers keep
    // flowing while producers and new consumers move to the new name.
    // One fan-out pass tests the resolved topic and the aliased old topics
    // together, so a subscriber matching via both is still visited exactly
    // once: the admitted message keeps a single (topic, seq) identity and
    // can never be double-delivered. `effectiveTopic` differs from `topic`
    // only on the cluster receive path, where a remote message may still
    // name an old topic — local publishes are already resolved in
    // `fanOut`.
    const effectiveTopic = this.resolveLiveTopic(topic);
    const aliasSources = this.liveAliasSources(effectiveTopic);
    const candidates = this.candidateIds(topic);
    if (effectiveTopic !== topic) {
      for (const id of this.candidateIds(effectiveTopic)) candidates.add(id);
    }
    for (const oldTopic of aliasSources) {
      if (oldTopic === topic || oldTopic === effectiveTopic) continue;
      for (const id of this.candidateIds(oldTopic)) candidates.add(id);
    }
    // Degenerate case: when every subscriber is a candidate (e.g. all on
    // `**`), the membership check would pass for all of them, so skip it.
    // Semantically identical, avoids a Set lookup per subscriber.
    const prune = candidates.size < this.subscribers.size;
    // Group members are collected per competing set first; the group's
    // single copy is assigned round-robin after the scan.
    const groupHits = new Map<string, { groupId: string; members: Subscriber[] }>();
    for (const subscriber of this.subscribers.values()) {
      if (prune && !candidates.has(subscriber.id)) {
        // Pruned by the prefix index: under the no-miss invariant the
        // pattern cannot match this topic — a definite non-match, so a
        // keyed message advances the subscriber's per-key baseline past
        // its keySeq, exactly like the matcher-tested non-match below.
        if (keyed !== undefined) this.skipKeySeq(subscriber, keyed.key, keyed.keySeq, epoch);
        // A causal message on a topic this subscriber never sees will
        // never arrive: advance its per-source expectation past the clock
        // the same way, or a buffered later clock would wait forever.
        if (causal !== undefined) this.skipCausalClock(subscriber, causal.source, causal.clock);
        continue;
      }
      if (!this.topicMatchesWithAliases(subscriber.matcher, topic, effectiveTopic)) {
        // A keyed message on a topic this subscriber never sees must still
        // advance its per-key baseline past this keySeq — otherwise a
        // subscriber buffering a later keySeq for the same key would wait
        // for a predecessor that will never be fanned out to it.
        if (keyed !== undefined) this.skipKeySeq(subscriber, keyed.key, keyed.keySeq, epoch);
        // Same for a causal message: its clock will never be fanned out
        // to this subscriber, so the source's expectation advances past
        // it instead of hanging on a dependency that cannot arrive.
        if (causal !== undefined) this.skipCausalClock(subscriber, causal.source, causal.clock);
        continue;
      }
      if (subscriber.groupId == null) {
        matched += 1;
        if (this.deliverToSubscriber(subscriber, msg, expiresAt, rawPayload, keyed, causal)) accepted += 1;
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
      const partitionCount = this.groupPartitions.get(key);
      if (partitionCount !== undefined) {
        // Partitioned competing set: the message belongs to exactly one
        // partition, consumed exclusively by its owner. The assignment is
        // deterministic from the roster (rendezvous by default, sticky
        // balanced when the group opted in — see
        // `GroupSubscribeOptions.assignment`), so every node agrees on the
        // owner without coordination.
        const partition = this.partitionForMessage(partitionCount, topic, msg.seq, keyed?.key);
        const ownerId = this.partitionAssignment(key).get(partition);
        const assignee = hit.members.find((m) => m.id === ownerId) ?? hit.members[0];
        if (keyed !== undefined) {
          for (const member of hit.members) {
            if (member !== assignee) this.skipKeySeq(member, keyed.key, keyed.keySeq, epoch);
          }
        }
        // A causal message assigned to one member will never be fanned
        // out to the others: their per-source expectations advance past
        // its clock, exactly like the keyed baseline above — otherwise a
        // member buffering a later clock for the same source would wait
        // for a dependency assigned to a different member.
        if (causal !== undefined) {
          for (const member of hit.members) {
            if (member !== assignee) this.skipCausalClock(member, causal.source, causal.clock);
          }
        }
        if (this.deliverToSubscriber(assignee, msg, expiresAt, rawPayload, keyed, causal)) accepted += 1;
        this.recordPartitionOffset(key, partition, topic, msg.seq);
        this.recordGroupOffset(hit.groupId, topic, msg.seq);
        continue;
      }
      const assignee = this.assignGroupMember(key, hit.members);
      if (keyed !== undefined) {
        // Consumer-group members compete per message: a keyed message
        // assigned to one member will never be fanned out to the others,
        // so their per-key baselines advance past its keySeq — otherwise a
        // member buffering a later keySeq would wait for a predecessor
        // that went to a different member. Per-(subscriber, key) ordering
        // is per member: each member observes an ordered subsequence of
        // the key's stream.
        for (const member of hit.members) {
          if (member !== assignee) this.skipKeySeq(member, keyed.key, keyed.keySeq, epoch);
        }
      }
      // Same for causal clocks: per member, each member observes a
      // causally ordered subsequence of the source's stream.
      if (causal !== undefined) {
        for (const member of hit.members) {
          if (member !== assignee) this.skipCausalClock(member, causal.source, causal.clock);
        }
      }
      if (this.deliverToSubscriber(assignee, msg, expiresAt, rawPayload, keyed, causal)) accepted += 1;
      this.recordGroupOffset(hit.groupId, topic, msg.seq);
    }
    return { matched, accepted };
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
    const ackLatency: BusStats['ackLatency'] = [];
    const subscriberLatencyP99: BusStats['subscriberLatencyP99'] = [];
    const lag: BusStats['lag'] = [];
    const rateLimitedWaiting: BusStats['rateLimitedWaiting'] = [];
    const hotKeys: HotKeyStat[] = [];
    let causalBufferDepth = 0;
    for (const subscriber of this.subscribers.values()) {
      unackedDeliveries += subscriber.reliable?.tracker.unackedCount ?? 0;
      if (subscriber.throttle?.throttled === true) throttledSubscribers += 1;
      if (subscriber.health?.degraded === true) degradedSubscribers += 1;
      if (subscriber.deliveryShaping?.shaping === true) shapedSubscribers += 1;
      const causalGate = subscriber.causal;
      if (causalGate !== undefined) {
        for (const buffer of causalGate.buffers.values()) causalBufferDepth += buffer.size;
      }
      const rateLimit = subscriber.rateLimit;
      if (rateLimit != null) {
        rateLimitedWaiting.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          waiting: rateLimit.rateLimiting
            ? subscriber.queue.size + (subscriber.batch?.pending.length ?? 0)
            : 0,
        });
      }
      const latency = subscriber.latency;
      if (latency != null) {
        deliveryLatency.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          ...latency.tracker.summary(),
        });
      }
      const ack = subscriber.ackLatency;
      if (ack != null) {
        ackLatency.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          ...ack.tracker.summary(),
        });
      }
      const slo = subscriber.latencySlo;
      if (slo != null) {
        const p99 = slo.tracker.summary();
        subscriberLatencyP99.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          p99Ms: p99.p99Ms,
          samples: p99.samples,
          thresholdMs: slo.thresholdMs,
          breaching: slo.alerted,
        });
      }
      const lagState = subscriber.lag;
      if (lagState != null) {
        // One clock reading for every watermark in the snapshot, shared
        // with the rate table above.
        lag.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          watermarkMs: this.lagWatermarkMs(subscriber, ratesNow),
          ...lagState.tracker.summary(),
        });
      }
      // Keyed hotspot sampling (see `SubscribeOptions.keyHotspot`): only
      // monitored subscriptions are scanned — a pure read of each
      // (subscriber, key) stream's reorder-buffer depth, so the ordering
      // gate is never disturbed.
      const keyHotspot = subscriber.keyHotspot;
      if (keyHotspot != null && subscriber.keyOrder !== undefined) {
        for (const [mapKey, order] of subscriber.keyOrder) {
          const depth = order.buffer.size;
          if (depth === 0) continue;
          const [, key] = splitKeyOrderKey(mapKey);
          hotKeys.push({
            subscriberId: subscriber.id,
            pattern: subscriber.pattern,
            key,
            bufferedDepth: depth,
            thresholdDepth: keyHotspot.thresholdDepth,
          });
        }
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
    const laggingSubscribers = lag
      .filter((s) => s.samples > 0)
      .sort((a, b) => b.p99Ms - a.p99Ms)
      .slice(0, 5)
      .map((s) => ({
        subscriberId: s.subscriberId,
        pattern: s.pattern,
        p99Ms: s.p99Ms,
        watermarkMs: s.watermarkMs,
        samples: s.samples,
      }));
    // Hottest first; ties break on (subscriberId, key) so the ranking is
    // deterministic across snapshots.
    const hottestKeys = hotKeys
      .sort((a, b) => {
        if (b.bufferedDepth !== a.bufferedDepth) return b.bufferedDepth - a.bufferedDepth;
        if (a.subscriberId !== b.subscriberId) return a.subscriberId < b.subscriberId ? -1 : 1;
        return a.key < b.key ? -1 : a.key > b.key ? 1 : 0;
      })
      .slice(0, HOT_KEYS_LIMIT);
    return {
      totalSubscribers: this.subscribers.size,
      subscribersByPattern: Object.fromEntries(this.subscribersByPattern),
      totalPublished: this.totalPublished,
      deliveredMessages: this.totalDelivered,
      droppedMessages: this.totalDropped,
      queueBytes: [...this.subscribers.values()].reduce(
        (sum, subscriber) => sum + subscriber.queue.queueBytes,
        0,
      ),
      throttledMessages: this.totalThrottled,
      expiredMessages: this.totalExpired,
      unackedDeliveries,
      patternCacheSize: this.patternCache.size,
      indexSize: this.prefixIndex.size,
      sequenceGaps: this.totalSequenceGaps,
      rateLimitedMessages: this.totalRateLimited,
      rejectedMessages: this.totalRejected,
      lateMessages: this.totalLateMessages,
      authzDenied: this.totalAuthzDenied,
      duplicateMessages: this.totalDuplicates,
      dedupDropped: this.totalDedupDropped,
      aliasRetiredMessages: this.totalAliasRetired,
      bridge: {
        inbound: this.bridgeInbound,
        outbound: this.bridgeOutbound,
        dropped: this.bridgeDropped,
      },
      aliases: [...this.topicAliases.entries()].map(([oldTopic, entry]) => ({
        oldTopic,
        newTopic: entry.newTopic,
        ...(entry.expiresAt === undefined ? {} : { expiresAt: entry.expiresAt }),
        expired: !this.aliasEntryLive(entry),
      })),
      routes: [...this.topicRoutes.entries()].map(([src, entry]) => ({
        src,
        dst: entry.dst,
        predicate: entry.predicate !== undefined,
        forwarded: entry.forwarded,
      })),
      forwards: [...this.forwardRules.values()].flatMap((rules) =>
        [...rules.values()].map((rule) => ({
          srcPattern: rule.srcPattern,
          ...(rule.dstTopic === undefined ? {} : { dstTopic: rule.dstTopic }),
          forwarded: rule.forwarded,
        })),
      ),
      filteredMessages: this.totalFiltered,
      deadLetteredMessages: this.totalDeadLettered,
      diagnosticEvents: this.totalDiagnosticEvents,
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
      rateLimitedWaiting,
      pendingDelayed: this.delayedById.size,
      deliveryLatency,
      ackLatency,
      subscriberLatencyP99,
      slowestSubscribers,
      lag,
      laggingSubscribers,
      keyedReorderedMessages: this.totalKeyedReordered,
      causalBuffer: {
        depth: causalBufferDepth,
        droppedMessages: this.totalCausalDropped,
        regressedMessages: this.totalCausalRegressed,
      },
      hotKeys: hottestKeys,
      traceSpans: this.trace?.snapshot() ?? [],
      hotTopics: this.publishRates.hotTopics(ratesNow),
      ...(this.clusterLink == null
        ? {}
        : {
            cluster: (() => {
              const s = this.clusterLink!.getStats();
              return {
                connected: s.connected,
                degraded: s.degraded,
                nodeId: s.nodeId,
                routeVersion: s.routeVersion,
                hubEpoch: s.hubEpoch,
                remoteMembers: s.members.filter((m) => m !== s.nodeId).length,
                forwardedMessages: s.forwardedMessages,
                receivedMessages: s.receivedMessages,
                forwardErrors: s.forwardErrors,
                receiveDropped: s.receiveDropped,
              };
            })(),
          }),
      consumerGroups: [...this.groupMembers.entries()].map(([key, members]) => {
        const sep = key.indexOf('\0');
        const partitions = this.groupPartitions.get(key);
        return {
          groupId: key.slice(0, sep),
          pattern: key.slice(sep + 1),
          members: members.length,
          ...(partitions === undefined ? {} : { partitions }),
        };
      }),
      groupLag: this.groupLagRowsFor(undefined),
      topics: [...this.topicStats.entries()].map(([topic, stats]) => ({
        topic,
        subscriberCount: stats.subscriberCount,
        publishedMessages: stats.publishedMessages,
        expiredMessages: stats.expiredMessages,
        lastSeq: stats.lastSeq,
        sequenceGaps: stats.sequenceGaps,
        rateLimitedMessages: stats.rateLimitedMessages,
        rejectedMessages: stats.rejectedMessages,
        lateMessages: stats.lateMessages,
        // Resolved with the currently configured allowed lateness, so a
        // reconfigured `setTopicAllowedLateness` is reflected here
        // immediately; `undefined` when no event-time publish has ever
        // been admitted on the topic.
        watermark: this.eventWatermarks.get(topic)?.watermarkFor(this.allowedLatenessForTopic(topic)),
        duplicateMessages: stats.duplicateMessages,
        aliasRetiredMessages: stats.aliasRetiredMessages,
        filteredMessages: stats.filteredMessages,
        dedupDropped: stats.dedupDropped,
        compressedMessages: stats.compressedMessages,
        compressedBytesBefore: stats.compressedBytesBefore,
        compressedBytesAfter: stats.compressedBytesAfter,
        compressionRatio:
          stats.compressedBytesBefore > 0
            ? stats.compressedBytesAfter / stats.compressedBytesBefore
            : 0,
        meanCompressionMs:
          stats.compressedMessages > 0 ? stats.compressionTimeMs / stats.compressedMessages : 0,
        // Auto-trained dictionary state (EB-60): present only when the
        // topic's matching compression rule has `autoTrain` enabled.
        // Omitted otherwise, so stats snapshots without auto-train are
        // byte-identical to before.
        ...(() => {
          const trainer = this.compressionForTopic(topic)?.autoTrainState;
          return trainer === undefined ? {} : { autoTrain: trainer.snapshot() };
        })(),
        rates: this.publishRates.ratesFor(topic, ratesNow),
      })),
      // Per-namespace aggregates (EB-52), in registration order — derived
      // from the per-topic stats above. Omitted when no namespace is
      // registered, so namespace-free snapshots are byte-identical to
      // before.
      ...(this.namespaces.size === 0
        ? {}
        : {
            namespaces: [...this.namespaces.keys()].map((prefix) => this.namespaceStatsFor(prefix)),
          }),
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
   * Messages currently held in a subscriber's causal reorder buffers
   * (see `SubscribeOptions.causal`), waiting on their dependencies —
   * 0 for subscribers that did not opt into causal delivery. Throws
   * for an unknown subscriber, like `pendingCount`.
   */
  causalBufferDepth(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    const gate = subscriber.causal;
    if (gate === undefined) return 0;
    let depth = 0;
    for (const buffer of gate.buffers.values()) depth += buffer.size;
    return depth;
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
    lastError?: string,
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
      lastError,
      traceId: this.trace?.traceIdOf(msg),
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
    // EB-57: centralized poison-message diagnostics. A dead-lettered
    // diagnostic message never emits a second diagnostic — the recursion
    // cut above guarantees the fan-out here cannot loop back into itself.
    // The diagnostic goes through the normal publish pipeline (ACL,
    // schema, rate limit): an admission rejection means no diagnostic is
    // emitted, and the DLQ move itself is unaffected. This runs inside the
    // flush that dead-lettered the message; fan-out plus scheduleFlush is
    // the same pattern the redelivery path below already uses.
    if (dlq.diagnosticTopic !== undefined && !this.diagnosticMessages.has(msg)) {
      const { admitted } = this.fanOut(
        dlq.diagnosticTopic,
        {
          payload: msg.payload,
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          seq: entry.seq,
          lastError: entry.lastError,
          redeliveries: entry.redeliveries,
          traceId: entry.traceId,
          deadLetteredAt: entry.deadLetteredAt,
        },
        false, // preAdmitted
        undefined, // delayed
        undefined, // key
        undefined, // preassignedKeySeq
        undefined, // traceparent
        undefined, // messageId
        false, // routed
        undefined, // routedExpiresAt
        false, // fromBridge
        true, // diagnostic
      );
      this.scheduleFlush();
      if (admitted) this.totalDiagnosticEvents += 1;
    }
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
   *
   * `opts.limit` caps the result to the most recently dead-lettered
   * entries — poison-message triage usually wants the newest failures
   * first. Must be a positive integer when given.
   */
  getDeadLetterMessages(subId: string, opts?: { limit?: number }): DeadLetterEntry[] {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    if (opts?.limit !== undefined) {
      const limit = opts.limit;
      if (!Number.isInteger(limit) || limit <= 0) {
        throw new RangeError('getDeadLetterMessages: limit must be a positive integer');
      }
    }
    const entries = subscriber.deadLetter?.entries ?? [];
    const windowed =
      opts?.limit === undefined ? entries : entries.slice(-opts.limit);
    return windowed.map(({ msg: _msg, ...entry }) => ({ ...entry }));
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
    // continuation of the poison run — and it bypasses the dedup window
    // for the same reason: an explicit operator replay is deliberate
    // intent, not an accidental redelivery.
    subscriber.reliable?.redeliveries.delete(record.msg);
    subscriber.reliable?.lastFailure.delete(record.msg);
    this.enqueueMessage(subscriber, record.msg, this.messageDeadlines.get(record.msg), true);
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
    } else {
      const shaping = subscriber.deliveryShaping;
      const rateLimit = subscriber.rateLimit;
      // Shaping paces the round first (smoothing bursts); the sliding
      // window then enforces the hard cap — a message is delivered only
      // when both allow it.
      const shapingMaxLive = shaping == null ? Infinity : Math.floor(shaping.bucket.availableTokens);
      const rateLimitMaxLive = rateLimit == null ? Infinity : rateLimit.limiter.budget(nowMs);
      const { live, expired } = subscriber.queue.drainLiveUpTo(
        nowMs,
        Math.min(shapingMaxLive, rateLimitMaxLive),
      );
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
          // The window counts actual deliveries, mirroring the shaping
          // take: dequeued-but-expired messages never reach this loop, so
          // TTL expiry consumes no window budget.
          rateLimit?.limiter.record(nowMs);
          // Compressed payloads are inflated here — once per message, before
          // any handler sees it — so every subscriber transparently receives
          // the original payload. See `inflateMessagePayload` for the
          // idempotency and envelope-collision notes.
          this.inflateMessagePayload(msg);
          this.detectGap(subscriber, msg);
          this.totalDelivered += 1;
          // Sampled before the handler runs: queue dwell, not processing time.
          this.recordDeliveryLatency(subscriber, msg, nowMs);
          this.recordLagSample(subscriber, msg, nowMs);
          // Handler processing time (EB-53): sampled per invocation for
          // plain subscribers. Reliable subscribers sample on ack()
          // completion instead (see invocationLatencySlo), so the
          // synchronous timing here must not sample them too. A throwing
          // handler still records its partial processing time — the
          // invocation occupied the consumer that long — and the throw
          // propagates exactly as before.
          const invocationSlo = this.invocationLatencySlo(subscriber);
          if (invocationSlo == null) {
            this.deliverTraced(subscriber, msg, () => subscriber.handler(msg));
          } else {
            const processingStartedAtMs = this.now();
            try {
              this.deliverTraced(subscriber, msg, () => subscriber.handler(msg));
            } finally {
              this.recordProcessingLatency(subscriber, invocationSlo, processingStartedAtMs);
            }
          }
        }
      } else {
        for (let i = 0; i < live.length; i += 1) {
          shaping?.bucket.take();
          rateLimit?.limiter.record(nowMs);
          this.inflateMessagePayload(live[i]);
          this.deliverWithHealth(subscriber, live[i], nowMs);
          if (!health.degraded) continue;
          // The threshold tripped mid-drain: everything not yet attempted
          // goes back to the queue, in FIFO order (see requeueUndelivered).
          // Only attempted messages were recorded in the window, so the
          // requeued backlog keeps its full budget on resume.
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
      if (rateLimit != null) {
        // Held back by the window only when the queue is non-empty AND the
        // window shows no budget left: a backlog held back by shaping (or
        // requeued by a mid-drain health trip) while the window still has
        // room is not the rate limit's doing.
        rateLimit.rateLimiting =
          subscriber.queue.size > 0 && rateLimit.limiter.budget(nowMs) < 1;
        if (rateLimit.rateLimiting) {
          this.armRateLimitTimer(subscriber, nowMs);
        } else {
          this.clearRateLimitTimer(subscriber);
        }
      }
    }
    // Re-evaluate the lag watermark after the drain: the head may have
    // moved (re-arming a fired alert) or the remaining backlog may still
    // sit past the threshold. Runs for plain, health-probed, shaped, and
    // batched subscribers alike.
    const lag = subscriber.lag;
    if (lag != null) this.checkLag(subscriber, lag, nowMs);
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
    const rateLimit = subscriber.rateLimit;
    const shapingBudget = shaping == null ? Infinity : Math.floor(shaping.bucket.availableTokens);
    // Pending messages already reserve window budget (recorded at hand-off
    // in deliverBatch), so collection must not exceed what is still free —
    // otherwise a batch filled across several rounds could over-deliver the
    // window before any delivery is recorded.
    const rateLimitBudget =
      rateLimit == null ? Infinity : Math.max(0, rateLimit.limiter.budget(nowMs) - batch.pending.length);
    const budget = Math.min(shapingBudget, rateLimitBudget);
    if (budget >= 1) {
      const room = batch.maxSize - batch.pending.length;
      if (room > 0) {
        const { live, expired } = subscriber.queue.drainLiveUpTo(nowMs, Math.min(room, budget));
        for (const msg of expired) {
          this.recordExpired(msg.topic);
        }
        batch.pending.push(...live);
      }
    } else {
      // No shaping or window budget this round: hold everything back,
      // exactly like the plain path — the pending batch counts as backlog
      // too.
      if (shaping != null) {
        shaping.shaping = subscriber.queue.size > 0 || batch.pending.length > 0;
        if (shaping.shaping) {
          this.armShapingTimer(subscriber);
        } else {
          this.clearShapingTimer(subscriber);
        }
      }
      if (rateLimit != null) {
        rateLimit.rateLimiting = subscriber.queue.size > 0 || batch.pending.length > 0;
        if (rateLimit.rateLimiting) {
          this.armRateLimitTimer(subscriber, nowMs);
        } else {
          this.clearRateLimitTimer(subscriber);
        }
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
    // The whole batch occupies window budget the same way: one recorded
    // delivery per message, stamped at hand-off — a message that expired
    // while the batch was filling is dropped above and never counted, so
    // TTL expiry consumes no window budget.
    const rateLimit = subscriber.rateLimit;
    if (rateLimit != null) {
      for (let i = 0; i < live.length; i += 1) rateLimit.limiter.record(nowMs);
    }
    for (const msg of live) {
      this.inflateMessagePayload(msg);
      this.detectGap(subscriber, msg);
      this.totalDelivered += 1;
      // Sampled before the handler runs: queue dwell, not processing time.
      this.recordDeliveryLatency(subscriber, msg, nowMs);
      this.recordLagSample(subscriber, msg, nowMs);
    }
    if (subscriber.health == null) {
      this.invokeBatchWithProcessingLatency(subscriber, () =>
        this.deliverBatchTraced(subscriber, live, () =>
          subscriber.handler(live as unknown as BusMessage),
        ),
      );
    } else {
      this.invokeBatchWithProcessingLatency(subscriber, () =>
        this.deliverBatchTraced(subscriber, live, () =>
          this.deliverBatchWithHealth(subscriber, live, nowMs),
        ),
      );
    }
    if (subscriber.queue.size > 0) {
      this.scheduleFlush();
    }
  }

  /**
   * Times one batched handler invocation for SLO-tracked subscribers
   * (EB-53): a batch is a single handler call, so it contributes one
   * processing-time sample covering the whole batch — not one per
   * message. Reliable subscribers sample on `ack()` completion instead
   * (see `invocationLatencySlo`). Untracked subscribers pay no clock read.
   */
  private invokeBatchWithProcessingLatency(subscriber: Subscriber, invoke: () => void): void {
    const slo = this.invocationLatencySlo(subscriber);
    if (slo == null) {
      invoke();
      return;
    }
    const startedAtMs = this.now();
    try {
      invoke();
    } finally {
      this.recordProcessingLatency(subscriber, slo, startedAtMs);
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
    // skipDedup: this is an internal queue rebuild, not a new delivery —
    // the messages were already admitted (and dedup-recorded) when first
    // enqueued; suppressing them here would silently lose them.
    if (queue.size === 0) {
      for (const msg of undelivered)
        this.enqueueMessage(subscriber, msg, this.messageDeadlines.get(msg), true);
      return;
    }
    const rest = queue.drain();
    for (const msg of undelivered)
      this.enqueueMessage(subscriber, msg, this.messageDeadlines.get(msg), true);
    for (const msg of rest) this.enqueueMessage(subscriber, msg, this.messageDeadlines.get(msg), true);
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
   * Arms the re-flush timer for a rate-limited subscriber that is holding
   * messages back: when the oldest delivery slides out of the window, the
   * timer triggers another flush so the backlog drains even when no new
   * publishes arrive. The delay is derived from the bus clock (consistent
   * with the window); the timer itself is a real wall-clock timeout — like
   * the shaping re-flush timer — and never keeps the process alive on its
   * own. Firing only schedules a flush when the subscriber is still
   * rate-limited and the window actually has budget; otherwise it re-arms,
   * so a clock that has not advanced cannot spin the flush loop.
   */
  private armRateLimitTimer(subscriber: Subscriber, nowMs: number): void {
    const rateLimit = subscriber.rateLimit;
    if (rateLimit == null || rateLimit.timer !== undefined) return;
    const delayMs = Math.max(0, Math.ceil(rateLimit.limiter.msUntilBudget(nowMs)));
    const timer = setTimeout(() => {
      rateLimit.timer = undefined;
      if (!rateLimit.rateLimiting || this.subscribers.get(subscriber.id) !== subscriber) return;
      if (rateLimit.limiter.budget(this.now()) < 1) {
        this.armRateLimitTimer(subscriber, this.now());
        return;
      }
      this.scheduleFlush();
    }, delayMs);
    // A rate-limited backlog must not keep the process alive on its own.
    const handle = timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    rateLimit.timer = timer;
  }

  /**
   * Cancels a rate-limited subscriber's pending re-flush timer, if any.
   */
  private clearRateLimitTimer(subscriber: Subscriber): void {
    const rateLimit = subscriber.rateLimit;
    if (rateLimit?.timer !== undefined) {
      clearTimeout(rateLimit.timer);
      rateLimit.timer = undefined;
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
    // The health probe reads the clock only when it enforces a processing
    // budget; the SLO tracker needs the same reading on every invocation.
    // A failed invocation (throw or timeout overrun) still records its
    // processing time — the SLO measures consumer load, not success — but
    // only failures count toward health degradation: the alert itself is
    // advisory and orthogonal (EB-53).
    const invocationSlo = this.invocationLatencySlo(subscriber);
    const startedAtMs = budgetMs !== undefined || invocationSlo != null ? this.now() : 0;
    let failed = false;
    let reason: 'error' | 'timeout' = 'error';
    this.totalDelivered += 1;
    // Sampled before the handler runs: queue dwell, not processing time.
    this.recordDeliveryLatency(subscriber, msg, nowMs);
    this.recordLagSample(subscriber, msg, nowMs);
    this.deliverTraced(subscriber, msg, () => {
      try {
        subscriber.handler(msg);
      } catch {
        failed = true;
      }
    });
    if (invocationSlo != null) {
      this.recordProcessingLatency(subscriber, invocationSlo, startedAtMs);
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
   *
   * Sequence epochs: the node's own publishes and each hub's forwarded
   * traffic number the same topic independently. When a delivery arrives
   * from a different epoch than the previous one on the same topic, the
   * baseline is re-established — the epoch switch itself is never counted
   * as lost messages.
   */
  private detectGap(subscriber: Subscriber, msg: BusMessage): void {
    const epoch = msg.epoch ?? '';
    const lastEpoch = subscriber.lastEpoch.get(msg.topic);
    const last = subscriber.lastDeliveredSeq.get(msg.topic);
    if (last === undefined || lastEpoch !== epoch) {
      subscriber.lastDeliveredSeq.set(msg.topic, msg.seq);
      subscriber.lastEpoch.set(msg.topic, epoch);
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
