import {
  BoundedQueue,
  assertValidBackpressurePolicy,
  type BackpressurePolicy,
  type DropPolicy,
} from './backpressure.ts';
import { LeaseManager, type LeaseExpiredCallback, type LeaseExpiredEvent } from './lease.ts';
import {
  IdleReclaimer,
  type IdleActivityMode,
  type IdleExpiredCallback,
  type IdleExpiredEvent,
} from './idle.ts';
import { AckTracker, type Delivery } from './ack.ts';
import { TokenBucket } from './throttle.ts';
import { SlidingWindowLimiter } from './ratewindow.ts';
import {
  createSlowStartGate,
  type SlowStartGate,
  type SlowStartOptions,
} from './slowstart.ts';
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
import {
  diagnoseBusHealth,
  type BusHealthReport,
  type DoctorInput,
  type DoctorOptions,
} from './doctor.ts';
import { DurableTopicLog, NAMESPACE_LOG_KEY_PREFIX, type DurableLogRecord, type SegmentRotationOptions } from './durablelog.ts';
import { OutboxRelay, type OutboxRelayOptions, type OutboxStore } from './outbox.ts';
import type { DurableStorageBackend } from './storage.ts';
import type { EncryptedPayload, PayloadEncryptor } from './encrypt.ts';
import {
  BUS_CONFIG_SNAPSHOT_VERSION,
  ConfigApplyError,
  assertValidSnapshot,
  cloneSnapshot,
  configValueEqual,
  diffConfigSnapshots,
  type BusConfigSnapshot,
  type ConfigApplyResult,
  type ConfigChange,
  type ConfigDrift,
  type NamespaceConfig,
  type TopicCompressionConfig,
  type TopicRateLimitConfig,
  type TopicRuleConfig,
  type TopicSigningKeyConfig,
} from './config.ts';
import {
  SubscriptionAuditLog,
  assertValidActor,
  type SubscriptionAuditEvent,
  type SubscriptionAuditQuery,
} from './subaudit.ts';
import {
  TopicRegistry,
  assertValidTopicRegistration,
  type TopicInfo,
  type TopicRegistration,
} from './topics.ts';
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
  newTraceId,
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
import {
  assertValidSigningKeys,
  normalizeSigningSecret,
  signPublisherMessage,
  signTopicMessage,
  verifyPublisherMessage,
  verifyTopicMessage,
  type MessageSignature,
  type StoredSigningKey,
  type TopicSigningKeysConfig,
} from './signing.ts';

export type { Delivery, RedeliveryReason } from './ack.ts';
export type {
  BridgeEnvelope,
  BridgeMessageSignature,
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
export type {
  BusHealthReport,
  CheckStatus,
  DoctorInput,
  DoctorOptions,
  HealthCheck,
  HealthStatus,
} from './doctor.ts';
export type { SlowStartOptions, SlowStartStrategy } from './slowstart.ts';
export type { LeaseExpiredCallback, LeaseExpiredEvent } from './lease.ts';
export type { IdleActivityMode, IdleExpiredCallback, IdleExpiredEvent } from './idle.ts';
export type { LateMessageCallback, LateMessageEvent } from './watermark.ts';
export type {
  TraceOptions,
  TraceSampler,
  TraceSamplingDecision,
  TraceSpan,
} from './trace.ts';
export type { EncryptedPayload, PayloadEncryptor } from './encrypt.ts';
export { AesGcmEncryptor, NoopEncryptor, isEncryptedPayload, randomAes256Key } from './encrypt.ts';
export type {
  BusConfigSnapshot,
  ConfigApplyResult,
  ConfigChange,
  ConfigDrift,
  NamespaceConfig,
  TopicAliasConfig,
  TopicCompressionConfig,
  TopicRateLimitConfig,
  TopicRouteConfig,
  TopicRuleConfig,
  TopicSigningKeyConfig,
} from './config.ts';
export { BUS_CONFIG_SNAPSHOT_VERSION, ConfigApplyError } from './config.ts';
export type {
  MessageSignature,
  StoredSigningKey,
  TopicSigningKeyInput,
  TopicSigningKeysConfig,
} from './signing.ts';
export { assertValidSigningKeys, SIGNING_KID_PATTERN, SIGNATURE_HEX_LENGTH } from './signing.ts';

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
   * Publish-side partition the message was assigned to (EB-90): the
   * per-(topic, partition) sequence counter that stamped `seq`. `0` for
   * every message on a topic with a single partition (the default) and
   * for messages admitted through paths that do not route partitions
   * (bridge/cluster ingress, window aggregates). Gap detection baselines
   * are tracked per (topic, partition), so a subscriber sees a gap only
   * when numbers skip inside one partition.
   */
  partition?: number;
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
   * Publisher identity, set when the publish carried one
   * (`PublishOptions.publisherId`). A producer-side label — service name,
   * client id, tenant — aggregated into per-publisher publish counters
   * (`getStats().publishers`, `eventbus_publisher_published_messages_total`).
   * It rides the envelope end to end (durable log, replay, delayed
   * schedules, cross-bus forwards) so a message admitted once keeps its
   * attribution across restarts and hops. Absent for anonymous publishes.
   */
  publisherId?: string;
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
  /**
   * Publish-side HMAC signature (see `EventBus.setTopicSigningKey`,
   * EB-84): `{ kid, sig }` covering `(topic, seq, wire bytes)` — the
   * wire bytes are the post-compression form, so a compressed envelope's
   * signature covers the ciphertext. Present exactly when a signing rule
   * matched the topic at publish time. It rides the envelope end to end
   * (durable log, replay, bridge mirror) so a `verifySignature`
   * subscriber checks the same bytes the publisher signed. Never a
   * secret — only the key id and the hex digest.
   */
  signature?: MessageSignature;
  /**
   * Publisher-identity HMAC signature (see
   * `EventBus.setPublisherSigningKey`, EB-97): `{ kid, sig }` covering
   * `(topic, seq, publisherId, wire bytes)` — orthogonal to `signature`:
   * a message can carry both, binding integrity (the topic key) and
   * identity (the publisher's key) independently. Present exactly when
   * the message carried a `publisherId` with a registered signing key
   * set at publish time. It rides the envelope end to end (durable log,
   * replay, retained snapshot) so a `verifyPublisher` subscriber checks
   * the same bytes the publisher signed; a bridge hop re-admits under
   * the destination bus's rules and re-signs there. Never a
   * secret — only the key id and the hex digest.
   */
  publisherSignature?: MessageSignature;
  /**
   * MQTT-style retained-message marker (see `EventBus.setTopicRetained`,
   * EB-87): `true` exactly when this delivery is the topic's retained
   * snapshot handed to a late-joining subscriber at subscribe time —
   * never on live publishes, replays, or redeliveries. Absent
   * (`undefined`) otherwise, so existing consumers see no shape change.
   */
  retained?: boolean;
  /**
   * Barrier cut marker (see `PublishOptions.barrier`, EB-98): present
   * exactly on barrier control messages — `{ epoch }`, the cut's
   * sequence number. A barrier is a control-plane signal, not business
   * data: it carries no business sequence number (`seq` is 0 and the
   * per-topic counter never advances for it), so it neither consumes a
   * number nor creates a sequence gap — gap detection skips barrier
   * messages entirely. The marker is bus-side (like the EB-57 diagnostic
   * marker), so a user payload can never collide with it; handlers
   * recognize a barrier control message by `msg.barrier !== undefined`
   * and read the cut metadata from `msg.payload` verbatim.
   */
  barrier?: { epoch: number };
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
 * Tuning for opt-in barrier (cut-marker) delivery (see
 * `SubscribeOptions.barrier`, EB-98).
 */
export interface BarrierSubscribeOptions {
  /**
   * Maximum messages held behind one armed barrier per subscriber. When
   * a new arrival would exceed it, the gate fails open: everything held
   * so far is released at once, the barrier is disarmed, and the cut is
   * abandoned (counted in `getStats().barrier.overflowedHolds`) — losing
   * the cut is safer than losing the data or growing the hold without
   * bound. Must be a positive integer. Default 1000.
   */
  maxHeldMessages?: number;
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

/**
 * Multi-pattern composite subscription options (EB-70): include/exclude
 * topic-pattern matching evaluated per message, after the subscriber's
 * content `filter` (see `SubscribeOptions.patterns`). The include list is
 * matched in `mode` — `'any'` (default) delivers when any include pattern
 * matches the message's topic, `'all'` only when every include pattern
 * does — and any message whose topic matches one of `exclude` is never
 * delivered, e.g. include `['market.*']` with `exclude: ['market.btc.*']`
 * to subscribe to the market feed while dropping the noisy BTC leg.
 *
 * The include/exclude patterns use the same wildcard semantics as
 * `subscribe` patterns (`compilePattern`): `*` matches one segment,
 * `**` zero or more. Each distinct pattern is compiled once and shared
 * across composite subscribers through a dedicated composite cache —
 * separate from the `patternCache` that backs single-pattern
 * subscriptions, so `getStats().patternCacheSize` keeps its meaning
 * (see `EventBus`).
 *
 * Layering: the `subscribe` topic-pattern argument stays the fan-out
 * pre-filter (prefix index + compiled matcher), and the composite match
 * is an additional delivery-time gate — a message must pass both. Pick
 * the base pattern at least as broad as the union of the include
 * patterns (e.g. `'**'`); a narrower base pattern silently pre-filters
 * messages the composite would have accepted.
 */
export interface CompositeSubscribeOptions {
  /**
   * Include patterns: at least one non-empty pattern string. `'any'`
   * mode delivers when any of them matches; `'all'` mode requires all
   * of them to match.
   */
  patterns: string[];
  /**
   * Include-match mode: `'any'` (default) or `'all'`. Anything else
   * throws `RangeError` at subscribe time.
   */
  mode?: 'any' | 'all';
  /**
   * Exclude patterns: a message whose topic matches any of them is
   * never delivered, regardless of the include match. Optional; an
   * empty array behaves as no exclusions.
   */
  exclude?: string[];
}

export interface SubscribeOptions {
  /** Per-subscriber bounded queue capacity (default 100). */
  queueSize?: number;
  /**
   * Slow-consumer backpressure tier (EB-83): what happens when the
   * subscriber's queue is full at enqueue time.
   *
   * - `'drop-oldest'`: shed the oldest queued entry to admit the newcomer.
   * - `'drop-newest'`: refuse the newcomer; the backlog is untouched.
   * - `'block'` (default): admit-with-shed on first saturation — exactly
   *   the historical default behavior — then refuse further arrivals
   *   while the subscriber stays saturated, preserving the already-queued
   *   backlog instead of churning it (like a closed TCP receive window).
   *   The subscriber unblocks when its queue recedes below the
   *   high-water mark. Refusals count into `droppedMessages` (and the
   *   dedicated `blockedMessages` counter) and are recorded as
   *   `backpressure_dropped` audit events.
   *
   * The tier composes orthogonally with `maxInflight` (EB-67, delivery-side
   * cap — still enforced on the preserved backlog) and with publish-side
   * admission sheds (EB-79 publisher rate limit — those never reach the
   * queue). Anything else throws `RangeError` at subscribe time; an
   * explicit `dropPolicy` that contradicts an explicit drop tier also
   * throws.
   */
  backpressure?: BackpressurePolicy;
  /**
   * Drop policy when the queue is full (default 'drop-oldest'). This is the
   * queue-level shed policy used inside the `'block'` tier's
   * admit-with-shed step and by the two drop tiers when they are selected
   * via `backpressure`; setting `backpressure` to `'drop-oldest'` /
   * `'drop-newest'` selects the same policy directly.
   */
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
   * Opt-in subscription lease, in milliseconds (see `src/lease.ts`,
   * EB-69): the subscription automatically unsubscribes when the lease
   * expires, exactly as if `unsubscribe()` had been called — consumer
   * group leave handling, reliable-delivery ack-tracker teardown and
   * queue disposal all run identically, and messages still queued at
   * expiry are discarded with the queue (the subscriber's own
   * `dropPolicy` semantics). `EventBusOptions.onLeaseExpired` fires once
   * per expired lease (error-isolated), and `getStats().expiredLeases`
   * counts expiries. `renewLease(subId, ms)` extends the lease from the
   * renewal moment; `unsubscribe()` clears it. The expiry timer is
   * `unref()`'d, so a pending lease never keeps the process alive.
   * Disabled by default (absent). Must be a positive integer when
   * given — anything else throws `RangeError` at subscribe time.
   */
  leaseMs?: number;
  /**
   * Opt-in idle-subscriber auto-reclamation, in milliseconds (see
   * `src/idle.ts`, EB-72): the complement of the fixed-duration lease
   * above. The subscription automatically unsubscribes once the time
   * since its last qualifying activity exceeds `maxIdleMs` — every
   * activity re-arms the window from the activity moment, so a busy
   * subscriber never expires while it is active. Expiry runs the exact
   * same unsubscribe path a manual `unsubscribe()` takes (consumer-group
   * leave handling, reliable-delivery ack-tracker teardown and queue
   * disposal all run identically; messages still queued at expiry are
   * discarded with the queue, per the subscriber's own `dropPolicy`
   * semantics). `EventBusOptions.onIdleExpired` fires once per expiry
   * (error-isolated), and `getStats().idleExpired` counts expiries.
   * `unsubscribe()` clears the timer, so no phantom expiry can fire
   * afterwards. The timer is `unref()`'d, so a pending idle window never
   * keeps the process alive. Disabled by default (absent). Must be a
   * positive integer when given — anything else throws `RangeError` at
   * subscribe time.
   *
   * A subscriber may hold both `leaseMs` and `maxIdleMs`: the two are
   * independent opt-ins and whichever fires first wins; the other is
   * cleared by the shared unsubscribe path.
   */
  maxIdleMs?: number;
  /**
   * Which subscriber events count as activity for idle reclamation (see
   * `maxIdleMs`, EB-72): `'deliver'` = the handler returned without
   * throwing (dropped/expired/shed messages never reach the handler, and
   * a throwing handler is a failed delivery — neither counts);
   * `'ack'` = `ack()` settled a live delivery (reliable subscriptions
   * only — `nack()`, ack timeouts and no-op acks do not count);
   * `'any'` = either (the default). Requires `maxIdleMs`; anything else
   * throws `RangeError` at subscribe time.
   */
  idleActivity?: IdleActivityMode;
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
   * Opt-in subscriber slow-start: cold-start protection for a fresh
   * subscription (EB-75). After the subscription is created, delivery is
   * paced by a ramping gate — the allowed rate climbs from `initialPerSec`
   * up to the subscriber's full speed over `rampMs` (`'linear'` interpolates
   * the rate, `'exponential'` grows it geometrically), so a new downstream
   * is never flooded before it has warmed up. When `deliveryShaping` is
   * configured the ramp targets its cap; without shaping the ramp has no
   * finite ceiling and opens fully when the window elapses. Once the ramp
   * completes the gate is dropped — zero overhead afterwards.
   *
   * Composes orthogonally with `deliveryShaping` and `maxInflight`: a
   * message is delivered only when every enabled gate allows it (the
   * effective budget is the minimum). Messages held back stay queued in
   * FIFO order, never dropped — TTL expiry still applies while they wait.
   * `getStats().rampingSubscribers` counts subscriptions still ramping.
   *
   * Pass `true` for the defaults (10 messages/sec initial, 1s linear
   * ramp), or a `SlowStartOptions` object to tune. Invalid values throw
   * `RangeError` from `subscribe`. Cannot be combined with `shadow`.
   * Disabled by default.
   */
  slowStart?: boolean | SlowStartOptions;
  /**
   * Opt-in per-subscriber cap on simultaneously unsettled handler
   * invocations during drain (EB-67). When the number of handler
   * invocations that have not settled reaches `maxInflight`, the drain
   * stops pulling new messages for this subscriber — messages stay queued
   * in FIFO order, never dropped — and pulls more once a handler settles.
   *
   * Composes orthogonally with `deliveryShaping`: shaping paces each flush
   * round (how many messages may leave the queue), the cap bounds how many
   * handler invocations may be unsettled at once — a message is delivered
   * only when both allow it.
   *
   * What "settles" an invocation depends on the subscription: for reliable
   * subscriptions (`subscribeReliable`) a delivery settles on `ack()`,
   * `nack()`, or the ack timeout — the slot is released on the tracker's
   * single settle path, so all three release uniformly. For plain
   * subscriptions the invocation settles when the handler returns; a
   * returned promise holds its slot until the promise settles.
   * `getStats()` exposes the per-subscriber usage (`inFlightUsage`).
   *
   * Must be an integer >= 1 — anything else throws `RangeError` from
   * `subscribe`. Cannot be combined with `batch` or `shadow` (both throw
   * `RangeError`). Disabled by default.
   */
  maxInflight?: number;
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
   * `commitOffset`), not the group's assignment watermark. Replay
   * progress for the pass is observable per subscriber in
   * `getStats().replayProgress` (see `onReplayProgress`, EB-71).
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
   * the replay is per member, like `resumeFromSeq`. Replay progress for
   * the pass is observable per subscriber in
   * `getStats().replayProgress` (see `onReplayProgress`, EB-71).
   */
  resumeFromTime?: number;
  /**
   * Opt-in durable-log replay progress observer (EB-71). When set, the
   * subscriber's `resumeFromSeq` / `resumeFromTime` replay pass reports
   * its progress as `{ subscriberId, pattern, total, replayed, skipped,
   * done, failed, error? }`: `total` records were read for the pass,
   * `replayed` admitted into the queue, `skipped` deliberately not
   * queued (content filter, composite pattern, handoff-linger window, or
   * an already-expired TTL — never a failure). The callback fires at
   * most every `progressInterval` processed entries (default 100), plus
   * exactly once when the pass finishes (`done: true`) or aborts on an
   * error (`failed: true`, `error` carries the message — the error still
   * propagates to the `subscribe` caller, replay stays fail-closed).
   * Error-isolated like `onAdmissionRejected`: a throwing hook is
   * swallowed so a broken observer can never disturb replay. The same
   * progress is always visible in `getStats().replayProgress`, even
   * without this callback.
   *
   * Requires a resume cursor: passing it without `resumeFromSeq` /
   * `resumeFromTime` throws `RangeError` instead of silently never
   * firing. Must be a function when provided.
   */
  onReplayProgress?: ReplayProgressCallback;
  /**
   * Replay entries processed between `onReplayProgress` fires (EB-71):
   * a throttle so a large replay does not spam the observer. Must be a
   * positive integer; defaults to 100. The completion/failure event
   * always fires exactly once, regardless of the interval. Requires
   * `onReplayProgress` — passing it alone throws `RangeError`.
   */
  progressInterval?: number;
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
   * Opt-in envelope signature verification (EB-84, backend security).
   * When true, every message fanned out to this subscriber is checked
   * against the topic's HMAC signing rules (`setTopicSigningKey`)
   * BEFORE any content processing: the envelope's `{kid, sig}` must
   * verify over `(topic, seq, wire bytes)` — the wire bytes are the
   * post-compression form, so a compressed message is verified against
   * the ciphertext and only inflated after the check passes.
   *
   * A message that fails verification is dropped — never queued, never
   * delivered — and counted in `TopicStats.signatureFailures` (plus the
   * global total) with a `signature_verification_failed` subscription
   * audit event. The loss also surfaces as a sequence gap: a failed
   * verification is a genuine loss, not a deliberate skip.
   *
   * Fail-closed: a topic with no signing rule, or a message carrying no
   * signature, never verifies — a misconfigured verifying subscription
   * drops loudly instead of delivering unverified messages. Applies to
   * live fan-out, durable-log replay (`resumeFromSeq`/`resumeFromTime`),
   * and bridge/forward inbound (the destination bus re-admits and
   * re-signs under its own rules; the subscriber verifies the
   * destination bus's signature). Shadow mirrors (`SubscribeOptions.shadow`)
   * never verify — they mirror production traffic byte-for-byte.
   *
   * Must be a boolean when provided; anything else throws `TypeError`
   * from `subscribe`. Disabled by default: a non-verifying subscriber
   * pays a single boolean check per delivery.
   */
  verifySignature?: boolean;
  /**
   * Opt-in publisher-identity verification (EB-97, backend security).
   * When true, every message fanned out to this subscriber is checked
   * against the publisher's HMAC signing keys (`setPublisherSigningKey`)
   * BEFORE any content processing: the envelope's `publisherSignature`
   * must verify over `(topic, seq, publisherId, wire bytes)`.
   * Orthogonal to `verifySignature` — a message can (and usually does)
   * carry both signatures; each is checked independently when its
   * option is on.
   *
   * A message that fails verification is dropped — never queued, never
   * delivered — and counted in `TopicStats.signatureFailures` (plus the
   * global total) with a `signature_verification_failed` subscription
   * audit event (`detail.kind: 'publisher'`). The loss surfaces as a
   * sequence gap, exactly like a failed topic-signature check.
   *
   * Fail-closed: a message with no `publisherId`, or from a publisher
   * with no registered key set, never verifies — an impersonated
   * identity (a `publisherId` whose signature does not verify) is
   * dropped, not delivered. Applies to live fan-out, durable-log replay
   * (`resumeFromSeq`/`resumeFromTime`), retained snapshots, and
   * bridge/forward inbound. Shadow mirrors never verify.
   *
   * Ordering note: sensitive-field redaction (`setTopicRedaction`,
   * EB-68) runs at admission, BEFORE signing at fan-out — both
   * signatures cover the redacted wire bytes, and the subscriber
   * verifies those same bytes, so redaction never breaks verification.
   *
   * Must be a boolean when provided; anything else throws `TypeError`
   * from `subscribe`. Disabled by default.
   */
  verifyPublisher?: boolean;
  /**
   * Opt-in multi-pattern composite subscription (EB-70). When set, every
   * message fanned out to this subscriber — after it passes the content
   * `filter` above — is additionally tested against include/exclude topic
   * patterns: `mode: 'any'` (default) delivers when any include pattern
   * matches the topic, `mode: 'all'` only when every include pattern
   * does, and any `exclude` match drops the message. A composite-rejected
   * message is skipped exactly like a filter rejection — never queued,
   * no backpressure budget, no throttle token, no sequence gap (the
   * per-topic baseline advances over it) — and counts into
   * `getStats().filteredMessages`. Applies to durable-log replay too,
   * and for shadow subscribers to the mirror copy. `getStats()` exposes
   * the live composite-subscription count as `compositeSubscriptions`.
   *
   * Invalid combinations throw `RangeError` from `subscribe`: an empty
   * `patterns` array, a non-`'any'`/`'all'` mode, a non-string/empty
   * pattern, or an `exclude` set that covers the include space so no
   * message could ever be delivered (mode `'any'`: every include pattern
   * covered by some exclude pattern; mode `'all'`: one exclude pattern
   * covering every include pattern — coverage decided by
   * `patternCovers`).
   *
   * See `CompositeSubscribeOptions` for the shape and the layering with
   * the `subscribe` topic-pattern argument. Disabled by default.
   */
  patterns?: CompositeSubscribeOptions;
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
   * Opt-in barrier (cut-marker) delivery (EB-98): the subscriber honors
   * barrier control messages (see `PublishOptions.barrier`) as
   * consistency cut points across its whole delivery stream — the
   * cross-topic barrier the causal clock's per-source ordering cannot
   * express. Protocol:
   *
   * - A barrier with a higher epoch than any seen before arms the gate:
   *   the barrier itself is delivered to the handler as a control message
   *   (`msg.barrier.epoch`, `seq` 0, user cut metadata in `msg.payload`
   *   verbatim), and every message arriving after it in delivery order is
   *   held in a pre-queue side buffer instead of being enqueued.
   * - When the barrier is handed to the handler (drained), everything
   *   held behind it is released at once — re-injected through the normal
   *   pipeline (causal gate, keyed gate, filter, throttle, queue), in
   *   arrival order — and the gate disarms.
   * - Barrier epochs must be monotonically increasing per subscriber: an
   *   out-of-order barrier (epoch at or below the highest seen) is
   *   discarded — never delivered, never arming — counted in
   *   `getStats().barrier.droppedBarriers`, and recorded in the
   *   subscription audit log as `barrier_dropped`.
   *
   * The cut orders the *delivery stream*, not publish time: a delayed
   * message (EB-24) whose due time falls after the barrier armed arrives
   * after the barrier in delivery order and is held behind it, even when
   * it was scheduled earlier. Retained snapshots (EB-87) and durable-log
   * replays (EB-17/EB-42) run at subscribe time — before any barrier can
   * arm — and bypass the gate (see the README's barrier section for the
   * full combination matrix).
   *
   * Composes with causal delivery (`SubscribeOptions.causal`): the
   * barrier gate runs first, then the causal gate — a held message
   * re-enters the pipeline after the barrier gate at release time, so
   * happens-before order is preserved on both sides of the cut and the
   * barrier marker itself is never reordered behind data. With per-key
   * ordering the same layering applies (barrier, then causal, then
   * keyed).
   *
   * Every hold terminates: the side buffer is bounded
   * (`maxHeldMessages`, default 1000) — on overflow the gate fails open
   * (everything held is released at once and the barrier disarmed,
   * counted in `getStats().barrier.overflowedHolds`): abandoning the cut
   * is safer than abandoning the data. A barrier dropped by backpressure
   * before delivery also releases the hold fail-open. Barriers are never
   * written to the durable log and never cross the bridge — they are
   * live, node-local coordination signals.
   *
   * Pass `true` for the defaults, or a `BarrierSubscribeOptions` object
   * to tune the hold bound. Invalid values throw `RangeError` from
   * `subscribe`. Disabled by default — subscribers that did not opt in
   * never see barrier control messages at all.
   */
  barrier?: boolean | BarrierSubscribeOptions;
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
   * Opt-in subscriber-side tumbling-window aggregation (EB-73): downstream
   * QPS smoothing for hot topics. The first admitted message opens a
   * window `[windowStart, windowStart + windowMs)` on the bus clock
   * (`EventBusOptions.now`); every admitted message folds into the
   * window's accumulators instead of being queued; when the window
   * closes the subscriber receives a single aggregate message whose
   * payload is a `WindowAggregateResult`
   * (`{ windowStart, windowEnd, count, value }`). A window with no
   * messages never delivers. The aggregate then travels the subscriber's
   * normal delivery path — backpressure queue, reliable delivery,
   * drain-time shaping — exactly like a published message.
   *
   * Aggregation happens at fan-out, before the queue: the content
   * `filter` still applies to each raw message (a rejected message never
   * enters the window), publish-side compression and the durable log see
   * the raw publishes untouched, and the aggregate itself is synthetic —
   * it is not re-filtered, carries no `messageId`, and takes no TTL
   * deadline. A message already past its TTL at admission is counted as
   * expired and never enters the window (a message that outlives its TTL
   * inside the short window still contributes — expiry is evaluated at
   * admission, not at close). The aggregate's `seq` is the window's last
   * raw message `seq`, so sequence-gap detection keeps working across
   * windows; its `topic` is that message's topic.
   *
   * Composes with the other delivery features:
   * - `filter`: applies to raw messages before aggregation (orthogonal).
   * - `throttle`: publish-side shedding still protects the subscriber —
   *   shed messages never enter the window and surface as sequence gaps.
   * - reliable (`subscribeReliable`): the aggregate arrives as one
   *   `Delivery` — ack it to confirm the whole window.
   * - `batch`: aggregates are batched at drain like any queued message.
   * - `deliveryShaping` / `rateLimit` / `maxInflight`: apply to the
   *   aggregate delivery (one invocation per window).
   *
   * Unsubscribing clears the window timer and discards the open window's
   * buffered state, exactly like discarding a queued backlog. The
   * window-close timer is `unref()`'d, so an open window never keeps the
   * process alive. `getStats().aggregatedWindows` counts completed
   * (non-empty) windows. Invalid values throw `RangeError` from
   * `subscribe`. Disabled by default.
   */
  window?: WindowAggregateOptions;
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
  /**
   * Opt-in shadow traffic mirroring (EB-63): the subscription becomes a
   * shadow subscriber — it receives a full copy of production traffic for
   * regression / load-test validation, in its own independent queue with
   * its own backpressure policy, and it can never disturb production.
   *
   * Isolation guarantees, all enforced by the bus:
   * - the shadow's copy is never counted in `publish()`'s matched /
   *   accepted fan-out width, in `deliveredMessages`, `droppedMessages`,
   *   `sequenceGaps`, or any topic/group stat — shadow deliveries are
   *   tracked in independent counters only (`shadowed` / `dropped` per
   *   subscriber, `shadowedMessages` / `shadowDroppedMessages` bus-wide,
   *   plus `eventbus_shadowed_messages_total` and
   *   `eventbus_shadow_dropped_messages_total` in `src/metrics.ts`);
   * - a shadow subscriber never joins group assignment: it cannot steal a
   *   consumer group's copy, advance the group's assignment watermark, or
   *   touch committed offsets (`commitOffset`, EB-32);
   * - a throwing shadow handler is swallowed — it never breaks the flush
   *   loop and never affects production delivery; the message still
   *   counts as shadowed, exactly like production counts a throwing
   *   handler's message as delivered.
   *
   * The shadow queue is the subscription's own queue, sized by
   * `ShadowSubscribeOptions` (falling back to the subscription's
   * `queueSize` / `dropPolicy`): a saturated shadow sheds only its own
   * copy per its own drop policy. The shadow copy bypasses the
   * per-subscriber ordering gates (keyed / causal) — it mirrors fan-out
   * order — and skips sequence-gap tracking (loss is visible in the
   * shadow `dropped` counter). TTL expiry still applies: a shadow copy
   * whose deadline passes before the drain is discarded, uncounted.
   *
   * Incompatible options throw `RangeError` from `subscribe` (a shadow
   * mirror is a plain live-traffic tap): `subscribeReliable` and
   * `subscribeToGroup` reject `shadow` outright, and within `subscribe`
   * so do `resumeFromSeq` / `resumeFromTime`, `throttle`,
   * `deliveryShaping`, `rateLimit`, `healthProbe`, `batch`, `causal`,
   * `deduplicateMessages`, `keyHotspot`, and `ackLatency`. The passive
   * observability knobs compose normally: `filter` (a throwing filter
   * still propagates to the publish call, like production),
   * `deliveryLatency`, `lagMonitor`, `latencySlo`, `onBackpressure`,
   * `onDrained`.
   *
   * Pass `true` for the defaults (the subscription's own `queueSize` /
   * `dropPolicy`), or a `ShadowSubscribeOptions` object to size the
   * shadow queue independently. Disabled by default.
   */
  shadow?: boolean | ShadowSubscribeOptions;
  /**
   * Actor recorded in the subscription lifecycle audit log (EB-78) for
   * this subscription's `subscribe` event — e.g. the service or operator
   * that created it. Defaults to `'system'` when omitted. Must be a
   * non-empty string when provided.
   */
  actor?: string;
}

/**
 * Tuning for a shadow subscriber's independent backpressure queue (see
 * `SubscribeOptions.shadow`). Unset fields fall back to the
 * subscription's own `queueSize` / `dropPolicy`, so `shadow: true` needs
 * no tuning at all; set them to size the mirror independently of any
 * production subscriber's queue. Bounds are validated at `subscribe`
 * time; violations throw `RangeError`.
 */
export interface ShadowSubscribeOptions {
  /**
   * Capacity of the shadow queue. Must be a positive integer. Defaults
   * to the subscription's `queueSize` (100).
   */
  queueSize?: number;
  /**
   * Drop policy of the shadow queue when full. Must be `'drop-oldest'`
   * or `'drop-newest'`. Defaults to the subscription's `dropPolicy`
   * (`'drop-oldest'`).
   */
  dropPolicy?: DropPolicy;
}

/**
 * Per-shadow-subscriber mirror counters (see `SubscribeOptions.shadow`),
 * in subscription order — one row per subscription with `shadow`
 * enabled. The independent stats surface for shadow traffic: these never
 * mix into the production delivery/drop counters.
 */
export interface ShadowSubscriberStat {
  /** The shadow subscription this row describes. */
  subscriberId: string;
  /** The topic pattern the shadow subscriber registered. */
  pattern: string;
  /** Messages handed to the shadow handler (a throwing handler still counts). */
  shadowed: number;
  /** Messages shed by the shadow queue's independent backpressure policy. */
  dropped: number;
  /** Current depth of the shadow queue. */
  queueSize: number;
}

/**
 * Default number of processed replay entries between
 * `SubscribeOptions.onReplayProgress` fires (EB-71). Overridable with
 * `SubscribeOptions.progressInterval`.
 */
const REPLAY_PROGRESS_INTERVAL_DEFAULT = 100;

/** Format marker carried by every `SubscriptionSnapshot` (EB-82). */
const SUBSCRIPTION_SNAPSHOT_KIND = 'realtime-event-bus/subscription-snapshot' as const;

/**
 * Subscription migration (EB-82): splits a subscribe-options bag into
 * the serializable subset that can travel in a `SubscriptionSnapshot`
 * and the names of the function-valued options that cannot. Only own
 * enumerable top-level properties are considered; nested option objects
 * are kept by reference (option bags are small plain data — the snapshot
 * documents the no-deep-clone rule). `undefined` values are dropped so
 * the snapshot only carries meaningful settings; a null/undefined bag
 * yields an empty options object and no unsupported names.
 */
function serializableSubscribeOptions(opts: SubscribeOptions | undefined): {
  options: Record<string, unknown>;
  unsupported: string[];
} {
  const options: Record<string, unknown> = {};
  const unsupported: string[] = [];
  if (opts == null) return { options, unsupported };
  for (const key of Object.keys(opts)) {
    const value = (opts as Record<string, unknown>)[key];
    if (typeof value === 'function') {
      unsupported.push(key);
      continue;
    }
    if (value !== undefined) options[key] = value;
  }
  return { options, unsupported };
}

/**
 * Subscription migration (EB-82): validates an inbound snapshot before
 * `importSubscription` touches it. Anything malformed — wrong kind or
 * version, bad handoff token, missing pattern, non-object options —
 * throws `RangeError`, fail-fast, before anything registers.
 */
function assertValidSubscriptionSnapshot(snap: unknown): asserts snap is SubscriptionSnapshot {
  const fail = (why: string): never => {
    throw new RangeError(`importSubscription: invalid snapshot (${why})`);
  };
  if (snap === null || typeof snap !== 'object' || Array.isArray(snap)) fail('not an object');
  const s = snap as Record<string, unknown>;
  if (s.kind !== SUBSCRIPTION_SNAPSHOT_KIND) fail('bad kind');
  if (s.version !== 1) fail('unsupported version');
  if (typeof s.handoffToken !== 'string' || !/^[0-9a-f]{32}$/.test(s.handoffToken)) {
    fail('bad handoffToken');
  }
  if (typeof s.pattern !== 'string' || s.pattern.length === 0) fail('bad pattern');
  if (s.namespace !== undefined && (typeof s.namespace !== 'string' || s.namespace.length === 0)) {
    fail('bad namespace');
  }
  if (typeof s.reliable !== 'boolean') fail('bad reliable flag');
  if (s.options === null || typeof s.options !== 'object' || Array.isArray(s.options)) {
    fail('bad options');
  }
  if (
    !Array.isArray(s.unsupportedOptions) ||
    !s.unsupportedOptions.every((n) => typeof n === 'string')
  ) {
    fail('bad unsupportedOptions');
  }
  if (
    s.ackCommittedSeq !== undefined &&
    (!Number.isInteger(s.ackCommittedSeq) || (s.ackCommittedSeq as number) < 0)
  ) {
    fail('bad ackCommittedSeq');
  }
  if (typeof s.leaseRemainingMs !== 'number' || !Number.isFinite(s.leaseRemainingMs) || s.leaseRemainingMs < 0) {
    fail('bad leaseRemainingMs');
  }
  if (!Array.isArray(s.pendingMessages)) fail('bad pendingMessages');
  for (const msg of s.pendingMessages as unknown[]) {
    if (msg === null || typeof msg !== 'object') fail('bad pendingMessages entry');
  }
}

/**
 * Subscription migration (EB-82): one outstanding export on the source
 * instance, keyed by handoff token until `completeMigration` or
 * `cancelMigration` runs.
 */
interface MigrationRecord {
  /** Source subscription id the snapshot was exported from. */
  subId: string;
  /** Bus-clock reading at export. */
  exportedAtMs: number;
  /** Queue depth at export — anything above it at complete time arrived mid-migration. */
  exportedQueueDepth: number;
  /**
   * In-flight backlog taken at export, in delivery order: outstanding
   * reliable deliveries (ascending delivery seq) followed by dequeued-
   * but-undelivered batch accumulator messages. Travels in
   * `snapshot.pendingMessages`; re-enqueued on the source queue when the
   * migration is cancelled, so cancelling loses nothing.
   */
  takenInFlight: BusMessage[];
}

/**
 * One subscriber's durable-log replay progress (see
 * `SubscribeOptions.onReplayProgress`, EB-71): `total` records were
 * read from the durable log for this replay pass, `replayed` were
 * admitted into the subscriber's queue, and `skipped` were deliberately
 * not queued — content-filter rejections, composite-pattern rejections,
 * handoff-linger skips, or records whose TTL had already expired (an
 * expired TTL is never a failure). `done` is true once the pass
 * finished without error; `failed` is true when the pass aborted on an
 * error, with `error` carrying the aborting error's message. Counting is
 * per subscriber: consumer-group members and partitioned members each
 * track their own replay.
 */
export interface ReplayProgressState {
  /** Records read from the durable log for this replay pass. */
  total: number;
  /** Records admitted into the subscriber's queue. */
  replayed: number;
  /**
   * Records deliberately not queued: content-filter rejections,
   * composite-pattern rejections, handoff-linger skips, and records
   * whose TTL had already expired. None of these is a failure.
   */
  skipped: number;
  /** True once the replay pass finished without error. */
  done: boolean;
  /** True when the replay pass aborted on an error. */
  failed: boolean;
  /** The aborting error's message, present only when `failed`. */
  error?: string;
}

/**
 * Snapshot delivered to `SubscribeOptions.onReplayProgress` (EB-71):
 * the subscriber's replay progress plus its identity.
 */
export interface ReplayProgressEvent extends ReplayProgressState {
  /** The subscriber's id (see `Subscription.id`). */
  subscriberId: string;
  /** The topic pattern the subscription was registered with. */
  pattern: string;
}

/** Callback type for `SubscribeOptions.onReplayProgress` (EB-71). */
export type ReplayProgressCallback = (event: ReplayProgressEvent) => void;

/**
 * One row of `BusStats.replayProgress` (EB-71): a subscriber's latest
 * durable-log replay progress, in subscription order. Only
 * subscriptions that ran a `resumeFromSeq` / `resumeFromTime` replay
 * appear — a subscriber that never replayed leaves no row.
 */
export interface ReplayProgressStat extends ReplayProgressState {
  /** The subscriber's id (see `Subscription.id`). */
  subscriberId: string;
  /** The topic pattern the subscription was registered with. */
  pattern: string;
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
   * Publish-side alignment (EB-93): a message published with an explicit
   * publish-side partition (EB-90) keeps that partition through group
   * consumption instead of being re-hashed — the producer's partition is
   * authoritative. When the group's partition count differs from the
   * topic's, the publish partition folds into the group's space with a
   * stable modulo (`publishPartition % partitions`), so the group's
   * per-partition watermarks line up with the publish-side sequence
   * space. Messages with no publish-side partition (legacy records,
   * single-partition topics) keep the hash routing described above.
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
 * Point-in-time backlog reading for one subscriber, shared by
 * `SubscriptionSnapshot.stats` and `CompletedSubscriptionMigration.stats`
 * (EB-82).
 */
export interface MigrationBacklogStats {
  /** Messages sitting in the subscriber's backpressure queue. */
  queueDepth: number;
  /** Cumulative messages shed by the queue's drop policy. */
  droppedCount: number;
  /** Cumulative messages discarded by TTL expiry. */
  expiredCount: number;
  /**
   * Deliveries outstanding (handed to a reliable handler, not yet
   * acked/nacked) at the reading. Always 0 on an export snapshot — the
   * outstanding deliveries travel inside `pendingMessages` instead.
   */
  unackedCount: number;
  /** Estimated buffered payload bytes in the queue. */
  queueBytes: number;
}

/**
 * A portable snapshot of one subscription, produced by
 * `EventBus.exportSubscription` and consumed by
 * `EventBus.importSubscription` on another instance (EB-82).
 *
 * The snapshot is a plain data bag: `options` holds the serializable
 * subscribe options, `pendingMessages` holds the live `BusMessage`
 * envelopes by reference (no deep clone). Transporting a snapshot
 * across processes (e.g. as JSON) requires JSON-safe payloads and
 * option values — that is the caller's responsibility; inside one
 * process the references are used as-is.
 */
export interface SubscriptionSnapshot {
  /** Format marker: `'realtime-event-bus/subscription-snapshot'`. */
  kind: 'realtime-event-bus/subscription-snapshot';
  /** Snapshot format version; currently 1. */
  version: 1;
  /**
   * Freshly minted 32-hex handoff token (same format as trace ids).
   * Single-use per importing instance: importing the same snapshot
   * twice on one instance throws, so a snapshot can never
   * double-subscribe the target.
   */
  handoffToken: string;
  /**
   * The topic pattern as registered (before namespace translation), so
   * the target re-registers it through the same namespace rules.
   */
  pattern: string;
  /** Namespace name when the source subscription was namespaced. */
  namespace?: string;
  /** True when the source subscription was reliable (`subscribeReliable`). */
  reliable: boolean;
  /**
   * The serializable subscribe options the source subscription was
   * created with. The target re-subscribes with these (plus `namespace`
   * above), so every validation the normal subscribe path runs —
   * unknown namespaces, ACL denials, bad option values — fails fast on
   * import exactly like a fresh subscribe.
   */
  options: Record<string, unknown>;
  /**
   * Names of function-valued subscribe options that were set on the
   * source subscription (content filters, lifecycle callbacks) and did
   * NOT migrate — functions cannot cross instances. Listed openly here,
   * never dropped silently; the target must re-attach its own.
   */
  unsupportedOptions: string[];
  /**
   * Committed ack position for reliable subscriptions: the highest
   * per-subscriber delivery sequence ever assigned on the source.
   * Messages acked at or below it are gone — never re-delivered — and
   * the target seeds its delivery counter from it, so `Delivery.seq`
   * continues where the source left off. Absent for plain subscriptions.
   */
  ackCommittedSeq?: number;
  /**
   * Remaining lease time in milliseconds at export, for subscriptions
   * created with `leaseMs`. The target re-arms the lease with this
   * remaining time, not the source's original full duration. 0 when the
   * source subscription had no lease.
   */
  leaseRemainingMs: number;
  /** Per-subscriber backlog reading at export time. */
  stats: MigrationBacklogStats;
  /**
   * Undelivered messages in delivery order: in-flight reliable
   * deliveries first (ascending delivery seq — they were dequeued
   * earliest), then the queued backlog (FIFO). Acked messages are
   * absent by construction: they were settled on the source and are
   * never re-delivered. Per-message TTL deadlines do NOT travel (see
   * `exportSubscription`); neither do dead-letter-queue contents,
   * open window aggregates, or per-key reorder-buffer holdings.
   */
  pendingMessages: BusMessage[];
  /** Bus-clock reading (`EventBusOptions.now`) at export. */
  exportedAtMs: number;
}

/** Result of `EventBus.importSubscription` (EB-82). */
export interface SubscriptionImportResult {
  /** Id of the recreated subscription on the target instance. */
  subId: string;
  /** The snapshot's handoff token, now consumed on this instance. */
  handoffToken: string;
}

/**
 * Result of `EventBus.completeMigration` (EB-82): the source
 * subscription has been unsubscribed through the normal teardown path.
 */
export interface CompletedSubscriptionMigration {
  /** Id of the source subscription that was unsubscribed. */
  subId: string;
  /** The handoff token whose migration just completed. */
  handoffToken: string;
  /** Final backlog reading, taken just before teardown. */
  stats: MigrationBacklogStats;
  /**
   * Messages that arrived on the source AFTER the export snapshot was
   * taken and were discarded with the source queue: they were never
   * part of the snapshot. Quiesce producers (or accept the loss) between
   * export and complete — this count makes the choice visible.
   */
  stragglers: number;
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
   * EB-96: pluggable persistence for the durable topic log. When set,
   * every object the log keeps — the per-topic live log, archived
   * segments, the group-offset journal, the subscriber-dedup journal,
   * and the publish-dedup journal — is read and written through this
   * backend instead of the local filesystem; recovery enumerates the
   * backend (`list()`), and namespace child logs use
   * `backend.scoped(...)`. Defaults to the filesystem rooted at
   * `durableLogDir` (the historical on-disk layout, byte for byte).
   * Requires `durableLogDir` (still the human-readable label and the
   * default backend's root). See `src/storage.ts` (`DurableStorageBackend`)
   * and the S3-compatible example in `src/storage-s3.ts`.
   */
  durableLogStorageBackend?: DurableStorageBackend;
  /**
   * Enables disk persistence for the subscription lifecycle audit log
   * (EB-78): every subscribe / unsubscribe / pattern hot-update /
   * group join-leave / rebalance / topic-registry change is appended to
   * `audit-YYYY-MM-DD.jsonl` (one file per UTC day, created when
   * missing) under this directory. Without it the audit log still
   * records into its bounded in-memory index — `getSubscriptionAudit()`
   * works either way — but a restart loses the history. Disabled by
   * default; when enabled, every audited change pays one synchronous
   * file append (error-isolated: a failed write is counted, never
   * thrown into the subscribe path).
   */
  subscriptionAuditDir?: string;
  /**
   * Bounded in-memory index size for the subscription audit log
   * (default 1000): the oldest entries are evicted past this size.
   * Applies whether or not `subscriptionAuditDir` persists to disk.
   * Must be a positive integer.
   */
  subscriptionAuditMaxEntries?: number;
  /**
   * Maximum retained log entries per topic before the log compacts away the
   * oldest entries (default 10000). Only meaningful with `durableLogDir`.
   * Must be a positive integer.
   */
  durableLogMaxEntriesPerTopic?: number;
  /**
   * Opt-in time-based retention for the durable log (EB-85): records older
   * than `now - durableLogMaxAgeMs` (bus clock, `EventBusOptions.now`) are
   * evicted when their topic compacts, alongside the count bound above —
   * a compact keeps records satisfying both. Delayed-delivery schedule
   * records (`seq: 0`) never age out, so pending timers survive. Change it
   * at runtime with `setDurableLogMaxAgeMs` (the change is recorded in the
   * subscription audit log as `retention_updated`); evictions are counted
   * in `getStats().durableLog.retentionEvicted`. Only meaningful with
   * `durableLogDir`. Must be a positive finite number of milliseconds.
   */
  durableLogMaxAgeMs?: number;
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
   * Opt-in at-rest encryption for the durable log (EB-80): when set,
   * every per-topic JSONL data line, every archived segment's record
   * payload, and the publish-dedup journal are sealed with this
   * encryptor before they touch disk (see `DurableTopicLog` for the
   * exact scope). Requires `durableLogDir` — setting it without a log
   * directory throws `RangeError` at construction. The same encryptor
   * (same key) must open the directory again; opening a sealed
   * directory without it throws loudly at recovery. `NoopEncryptor`
   * (from `src/encrypt.ts`) is for tests only — it protects nothing.
   */
  durableLogEncryptor?: PayloadEncryptor;
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
   * (`'schema'`, see `setTopicSchema`), per-topic rate-limit sheds
   * (`'rate-limit'`, see `setTopicRateLimit`), per-publisher rate-limit
   * sheds (`'publisher-rate-limit'`, see `setPublisherRateLimit`),
   * idempotency-duplicate suppressions
   * (`'duplicate'`, see `publishIdempotent`), retired-alias rejections
   * (`'alias-retired'`, see `setTopicAlias`), and `publishAtomic` batch
   * rejections (surfaced with the failing entry's gate reason).
   *
   * `reason` maps 1:1 onto the stats counters
   * (`TopicStats.rejectedMessages` / `.rateLimitedMessages` /
   * `.publisherRateLimited` / `.duplicateMessages` / `.aliasRetiredMessages`,
   * and the `BusStats` totals), so hook events
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
   * Opt-in hook fired when a message is published to a topic with no live
   * registration in the topic catalog (EB-77). The publish is never
   * blocked — the hook is an observability channel for typo'd topic names
   * and producers that forgot to register. Every such publish is also
   * counted in `getStats().unregisteredTopicPublishes`, with or without
   * the hook. Error-isolated like `onAdmissionRejected`: a throwing hook
   * is swallowed so a broken observer can never disturb the publish path.
   * Default: unset (counting only — fully backward compatible).
   */
  onUnregisteredTopic?: UnregisteredTopicCallback;
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
   * Fires once per expired subscription lease (see
   * `SubscribeOptions.leaseMs`, EB-69): the bus has already unsubscribed
   * the subscriber through its normal unsubscribe path when this fires,
   * so the subscription is gone and `getStats().expiredLeases` has
   * already been incremented. The callback is error-isolated: a throwing
   * hook is swallowed so a broken observer can never disturb the
   * unsubscribe path. Default: unset (no hook — fully backward
   * compatible).
   */
  onLeaseExpired?: LeaseExpiredCallback;
  /**
   * Fires once per idle-expired subscription (see
   * `SubscribeOptions.maxIdleMs`, EB-72): the bus has already
   * unsubscribed the subscriber through its normal unsubscribe path
   * when this fires, so the subscription is gone and
   * `getStats().idleExpired` has already been incremented. The callback
   * is error-isolated: a throwing hook is swallowed so a broken observer
   * can never disturb the unsubscribe path. Default: unset (no hook —
   * fully backward compatible).
   */
  onIdleExpired?: IdleExpiredCallback;
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
   * Messages on this topic shed by per-publisher publish-side rate
   * limiting (see `setPublisherRateLimit`): the publisher's token bucket
   * was empty when the message was published, so it was never fanned out
   * to any subscriber. The shed happens in the admission phase — before
   * sequence-number assignment — so unlike a topic rate-limit shed it
   * consumes no sequence number and subscribers see no gap.
   */
  publisherRateLimited: number;
  /**
   * Publishes to this topic shed by a topic pause in shed mode (see
   * `EventBus.pauseTopic`, EB-88): the topic was paused when the message
   * was published, so it was rejected outright — never fanned out to any
   * subscriber. The shed consumes a sequence number, so subscribers
   * observe the loss as a sequence gap (the EB-19 rate-limit shed
   * semantics, not the schema-rejection semantics). Surfaced on
   * `onAdmissionRejected` with reason `'topic-paused'`.
   */
  pausedMessages: number;
  /**
   * Publishes to this topic rejected by admission gates that fire before
   * admission — schema validation (see `setTopicSchema`), the broker-level
   * ACL (see `setAclRules`), the per-topic message-size cap (see
   * `setTopicMaxMessageBytes`), and expired migration aliases (see
   * `EventBus.setTopicAlias`). Unlike rate-limit sheds, a rejection happens
   * before admission — the payload never became a message, so it consumed
   * no sequence number and is invisible to gap detection. The
   * `onAdmissionRejected` hook's `reason` distinguishes the gates sharing
   * this counter.
   */
  rejectedMessages: number;
  /**
   * Publishes to this topic admitted under a superseded-but-graced
   * schema version (see `setTopicSchemaVersion`): the payload validated
   * against an older validator inside its `graceMs` migration window.
   * These are successful admissions — the message was published — so
   * they move no other counter; the operator signal that some producers
   * still speak an old schema and should migrate.
   */
  deprecatedSchemaVersions: number;
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
   * rejected (see `SubscribeOptions.filter`), or that a composite
   * subscription's include/exclude patterns rejected (see
   * `SubscribeOptions.patterns`): the message never entered that
   * subscriber's queue — no backpressure budget consumed — and the
   * subscriber's per-topic baseline advanced over it, so it is invisible
   * to sequence-gap detection.
   */
  filteredMessages: number;
  /**
   * Messages on this topic discarded by `verifySignature` subscribers
   * (see `SubscribeOptions.verifySignature`, EB-84) because the
   * envelope's HMAC signature was missing or invalid: the message never
   * entered that subscriber's queue. Counts each discarded delivery
   * once, so a message fanned out to N verifying subscribers that all
   * reject it contributes N here. The loss also surfaces as a sequence
   * gap on each rejecting subscriber: a failed verification is a genuine
   * loss, not a deliberate skip.
   */
  signatureFailures: number;
  /**
   * Per-topic HMAC signing configuration (see `setTopicSigningKey`,
   * EB-84): present only when a signing rule currently matches the
   * topic. Carries the key ids and the grace window — never the secrets.
   * Omitted otherwise, so stats snapshots without signing are
   * byte-identical to before.
   */
  signing?: { keyIds: string[]; graceMs: number };
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
   * Sensitive fields replaced with the redaction sentinel on this topic
   * (see `setTopicRedaction`, EB-68): the number of field paths actually
   * masked across admitted publishes. Counted at admission — a field
   * replaced on a publish that a later gate then rejects still counts,
   * and a field already holding the sentinel is left untouched and
   * uncounted (redaction is idempotent across route/bridge hops).
   * Non-object payloads never count. 0 when no redaction rule matches
   * the topic.
   */
  redactedFields: number;
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
  /**
   * Per-partition publish counters (EB-90), in ascending partition order.
   * Present only when the topic resolves to more than one partition
   * (`EventBus.setTopicPartitions`) — single-partition topics omit it so
   * their stats snapshots are byte-identical to before. `publishedMessages`
   * counts admitted publishes per partition (a publish-side shed consumes
   * its partition's sequence number, so it counts here exactly like it
   * counts in the topic total); `lastSeq` is the highest per-partition
   * sequence number assigned so far.
   */
  partitions?: Array<{ partition: number; publishedMessages: number; lastSeq: number }>;
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
   * Total messages refused by saturated subscribers under the `'block'`
   * backpressure tier (see `SubscribeOptions.backpressure`, EB-83) — the
   * backlog-preserving alternative to shedding. Included in
   * `droppedMessages`; tracked separately for backpressure forensics.
   */
  blockedMessages: number;
  /** Subscribers currently refusing new arrivals under the `'block'` tier. */
  blockedSubscribers: number;
  /**
   * Total messages handed to shadow-subscriber handlers (see
   * `SubscribeOptions.shadow`) — the mirror deliveries. Tracked
   * independently: never counted in `deliveredMessages`, so validating a
   * new handler version against production traffic cannot move the
   * production delivery counter.
   */
  shadowedMessages: number;
  /**
   * Total messages shed by shadow subscribers' independent backpressure
   * queues. Never counted in `droppedMessages` — a saturated shadow
   * mirror sheds only its own copy.
   */
  shadowDroppedMessages: number;
  /**
   * Per-shadow-subscriber mirror counters (see `SubscribeOptions.shadow`),
   * in subscription order — one row per subscription with `shadow`
   * enabled. Empty when no shadow subscription is live.
   */
  shadows: ShadowSubscriberStat[];
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
   * Total subscription leases that expired and auto-unsubscribed their
   * subscriber (see `SubscribeOptions.leaseMs`, EB-69). Counts each
   * expiry once, after the unsubscribe ran and `onLeaseExpired` fired.
   * 0 when no lease was ever armed — the field is always present, so
   * readers never need a fallback.
   */
  expiredLeases: number;
  /**
   * Total subscriptions that idle-expired and auto-unsubscribed (see
   * `SubscribeOptions.maxIdleMs`, EB-72). Counts each expiry once,
   * after the unsubscribe ran and `onIdleExpired` fired. 0 when no idle
   * window was ever armed — the field is always present, so readers
   * never need a fallback.
   */
  idleExpired: number;
  /**
   * Completed (non-empty) tumbling windows across all windowed
   * subscribers (see `SubscribeOptions.window`, EB-73). Counts each
   * window once when it closes and produces its aggregate — 0 when no
   * subscriber ever opted into window aggregation. The field is always
   * present, so readers never need a fallback.
   */
  aggregatedWindows: number;
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
   * Currently active subscriptions with composite include/exclude patterns
   * (see `SubscribeOptions.patterns`, EB-70). The composite patterns are
   * compiled through a dedicated composite cache that never feeds
   * `patternCacheSize`, so this count is the only place composite
   * subscriptions surface in the cache stats.
   */
  compositeSubscriptions: number;
  /**
   * Currently active subscriptions paused for cross-instance migration
   * (see `exportSubscription`, EB-82): delivery is paused but the
   * subscription still counts as live everywhere else.
   */
  migratingSubscribers: number;
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
   * Total messages shed by per-publisher publish-side rate limiting — the
   * sum of every topic's `publisherRateLimited`. See
   * `TopicStats.publisherRateLimited` for the exact counting rules.
   */
  publisherRateLimited: number;
  /**
   * Total publishes shed by topic pauses in shed mode — the sum of every
   * topic's `pausedMessages`. See `TopicStats.pausedMessages` for the
   * exact counting rules.
   */
  pausedMessages: number;
  /**
   * Currently active topic-pause rules (see `EventBus.pauseTopic`,
   * EB-88), in registration order: the rule key, its mode, and how many
   * messages its durable-side buffer currently holds (queue mode).
   */
  pausedTopics: PausedTopicRule[];
  /**
   * Total publishes rejected by admission gates before admission — schema
   * validation, the broker-level ACL, the per-topic message-size cap, and
   * expired migration aliases — the sum of every topic's
   * `rejectedMessages`. See `TopicStats.rejectedMessages` for the exact
   * counting rules.
   */
  rejectedMessages: number;
  /**
   * Total publishes admitted under a superseded-but-graced schema
   * version — the sum of every topic's `deprecatedSchemaVersions` (see
   * `setTopicSchemaVersion`). The bus-wide migrate-your-producers
   * signal.
   */
  deprecatedSchemaVersions: number;
  /**
   * Total publishes to topics with no live registration in the topic
   * catalog (EB-77). Advisory only — these publishes were delivered
   * normally (and counted in `totalPublished` too); this is the
   * typo'd-topic / forgot-to-register signal, also surfaced per occurrence
   * on `EventBusOptions.onUnregisteredTopic`. Always present, so readers
   * never need a fallback.
   */
  unregisteredTopicPublishes: number;
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
   * Per-publisher admitted-publish totals (see
   * `PublishOptions.publisherId`): every identity that has admitted at
   * least one publish, with its total — most-published first, ties broken
   * on the identity for a deterministic snapshot. Anonymous publishes
   * move no per-publisher counter; their total is `totalPublished`.
   */
  publishers: Array<{ publisher: string; publishedMessages: number }>;
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
   * Total messages skipped by subscriber content filters and composite
   * include/exclude patterns — the sum of every topic's
   * `filteredMessages`. See `TopicStats.filteredMessages` for the exact
   * counting rules.
   */
  filteredMessages: number;
  /**
   * Total messages discarded by `verifySignature` subscribers because the
   * envelope's HMAC signature was missing or invalid — the sum of every
   * topic's `signatureFailures`. See `TopicStats.signatureFailures` for
   * the exact counting rules.
   */
  signatureFailures: number;
  /**
   * Total messages moved into subscriber dead-letter queues — every
   * reliable-subscription message that exhausted its redelivery budget
   * (see `ReliableSubscribeOptions.deadLetter`).
   */
  deadLetteredMessages: number;
  /**
   * Total cumulative-ack calls (`ackUpTo`, EB-76) on known reliable
   * subscribers — one per call, regardless of how many deliveries the
   * range actually acked.
   */
  ackedRanges: number;
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
   * Total sensitive fields replaced with the redaction sentinel — the sum
   * of every topic's `redactedFields`. See `TopicStats.redactedFields`
   * for the exact counting rules.
   */
  redactedFields: number;
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
   * Subscriptions currently in their slow-start ramp (see
   * `SubscribeOptions.slowStart`): the ramp window has not elapsed yet,
   * so delivery is still paced below the subscriber's full speed. Once
   * the ramp completes the gate is dropped and the subscriber leaves
   * this count.
   */
  rampingSubscribers: number;
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
   * Per-subscriber in-flight delivery usage (see
   * `SubscribeOptions.maxInflight`): one row per subscriber with the cap
   * enabled, in subscription order, carrying the configured cap
   * (`maxInflight`) and how many handler invocations are currently
   * unsettled (`inFlight`). Empty when no subscriber opts in.
   */
  inFlightUsage: Array<{
    /** The subscriber's id (see `Subscription.id`). */
    subscriberId: string;
    /** The topic pattern the subscription was registered with. */
    pattern: string;
    /** The configured cap on unsettled handler invocations. */
    maxInflight: number;
    /** Handler invocations currently unsettled. */
    inFlight: number;
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
   * Barrier (cut-marker) gate state (see `SubscribeOptions.barrier`,
   * EB-98). Always present — zeros when no subscriber opted into barrier
   * delivery.
   */
  barrier: {
    /**
     * Barrier control messages published (fanned out to opted-in
     * subscribers). Control-plane: never counted in `publishedMessages`,
     * never advancing any per-topic seq.
     */
    publishedBarriers: number;
    /**
     * Out-of-order barriers discarded for epoch monotonicity — each one
     * recorded in the subscription audit log as `barrier_dropped`.
     */
    droppedBarriers: number;
    /**
     * Messages currently held behind armed barriers, across all
     * subscribers — the live cut backlog.
     */
    heldMessages: number;
    /**
     * Messages released from barrier holds (drained barriers plus
     * fail-open releases), re-injected through the normal pipeline.
     */
    releasedMessages: number;
    /**
     * Holds abandoned by fail-open overflow: the hold buffer hit
     * `maxHeldMessages`, so everything held was released at once and the
     * barrier disarmed.
     */
    overflowedHolds: number;
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
   * Per-topic subscriber caps (EB-81): one entry per registered
   * `setTopicMaxSubscribers` rule, in registration order — the rule key,
   * its cap, and the live subscriber count currently resolved against
   * it. Empty when no cap is configured.
   */
  topicSubscriberLimits: Array<{ rule: string; maxSubscribers: number; currentSubscribers: number }>;
  /**
   * Versioned schema registry (EB-86): one row per `setTopicSchemaVersion`
   * registration key (concrete topic or pattern), in registration order —
   * the rule key, the latest version, and every registered version
   * ascending with its `registeredAt`, `graceMs`, and `supersededAt`
   * (present only once a greater version superseded it), plus the EB-95
   * compatibility gate result (`compatGate`: `'passed'` when the version
   * registered with `compatibilityCorpus` and its validator accepted
   * every sample, `'not-tested'` when no corpus was supplied;
   * `compatCorpusSize` counts the gated samples). Validators are
   * functions and are never exposed — metadata only. Empty when no
   * versioned schema is registered.
   */
  topicSchemas: Array<{
    rule: string;
    latest: number;
    versions: Array<{
      version: number;
      registeredAt: number;
      graceMs: number;
      supersededAt?: number;
      compatGate: 'passed' | 'not-tested';
      compatCorpusSize: number;
    }>;
  }>;
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
   * Per-subscriber durable-log replay progress (EB-71): one row per
   * subscription that ran a `resumeFromSeq` / `resumeFromTime` replay,
   * in subscription order. Each row carries the pass's `total`,
   * `replayed`, `skipped`, and `done` / `failed` state (plus the
   * aborting `error` when failed). Counting is independent per
   * subscriber — consumer-group members and partitioned members each
   * have their own row. Always present; empty when no subscriber has
   * replayed yet.
   */
  replayProgress: ReplayProgressStat[];
  /**
   * MQTT-style retained messages (EB-87): one row per concrete topic
   * currently holding a retained message — the topic, the retained
   * message's per-topic `seq`, and its publish timestamp (`at`, bus
   * clock) — sorted by topic. Empty when no topic holds a retained
   * message. A retained message whose TTL deadline has passed is
   * filtered out of this view (the snapshot never resurrects it).
   */
  retainedTopics: Array<{ topic: string; seq: number; at: number }>;
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
    /** Whether at-rest encryption is enabled (`durableLogEncryptor`, EB-80). */
    encrypted: boolean;
    /**
     * Records evicted by time-based retention (`durableLogMaxAgeMs`,
     * EB-85), across compactions. 0 when no age bound is configured.
     */
    retentionEvicted: number;
    /** Archive files that failed to decompress during archived reads. */
    corruptArchives: number;
    /**
     * Archived segments isolated by checksum verification (EB-94): a
     * sidecar mismatch, during recovery or archived reads. Skipped,
     * never fatal.
     */
    corruptSegments: number;
    /** Total entries across all archived segments. */
    archived: number;
    /**
     * Per-topic segment view: live entries in the current segment,
     * closed archived segment count, and archived entries.
     */
    segments: Record<string, { live: number; segments: number; archived: number }>;
  };
  /**
   * Outbox records sitting in the attached outbox store, not yet relayed
   * (`seq > cursor`). 0 when no outbox relay is attached (see
   * `attachOutboxRelay`, EB-102). Always present, so readers never need
   * a fallback.
   */
  outboxPending: number;
  /**
   * Age of the oldest unrelayed outbox record in ms. 0 when nothing is
   * pending or no relay is attached. The stall signal: a climbing lag
   * with a stuck cursor means the relay is wedged on a poison record.
   */
  outboxLagMs: number;
  /**
   * Last outbox cursor acked after a successful `publishAtomic` commit.
   * 0 when no relay is attached or nothing has been relayed yet.
   */
  outboxCursor: number;
  /**
   * Total outbox records committed via `publishAtomic` since the relay
   * was attached. 0 when no relay is attached.
   */
  outboxRelayed: number;
}

/**
 * One entry of the MQTT-style retained-message table (EB-87): the latest
 * retained message for a concrete topic. Stored is the wire form — the
 * compressed envelope when a compression rule applied, plus the publish
 * timestamp, TTL deadline, dictionary id and signature — so a snapshot
 * delivery is byte-identical to the live delivery (transparent inflation
 * and signature verification apply at snapshot time, exactly like
 * replay).
 */
interface RetainedEntry {
  /** Concrete (alias-resolved) topic. */
  topic: string;
  /** Wire payload: the compressed envelope when compression applied, else the raw payload. */
  payload: unknown;
  /**
   * Binary-payload marker (EB-92): `true` when `payload` is raw bytes
   * (`Uint8Array`) rather than a JSON value. Restored from the v2 log
   * line on restart; re-marks the snapshot envelope at delivery so
   * inflation and signature verification stay byte-oriented.
   */
  binary?: boolean;
  /** The original publish's per-topic sequence number. */
  seq: number;
  /**
   * The original publish's partition (EB-90): the per-(topic, partition)
   * sequence space `seq` was drawn from. Restored onto the snapshot
   * envelope so gap detection keys the snapshot against the right
   * partition baseline. Absent for retained messages written before
   * partition routing — treated as partition 0.
   */
  partition?: number;
  /** Bus-clock publish timestamp, in milliseconds. */
  at: number;
  /**
   * TTL deadline stamped at publish time, when a TTL rule matched. A
   * retained message past its deadline is pruned and never snapshotted —
   * the snapshot must not resurrect an expired message.
   */
  expiresAt?: number;
  /** SHA-256 id of the preset dictionary that compressed `payload`. */
  dictId?: string;
  /** Publish-time HMAC signature, when a signing rule matched. */
  signature?: { kid: string; sig: string };
  /** Publish-time publisher-identity HMAC signature, when a publisher key set matched. */
  publisherSignature?: { kid: string; sig: string };
  /** Application-level message identity, when the publish carried one. */
  messageId?: string;
  /** Publisher identity, when the publish carried one. */
  publisherId?: string;
  /** Business event time, when the publish carried one. */
  eventTime?: number;
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
 * One message held behind an armed barrier (see
 * `SubscribeOptions.barrier`, EB-98): everything the pipeline needs to
 * re-inject it after the barrier gate at release time — the barrier gate
 * runs first, so a released message still passes through the causal
 * gate, the keyed gate, the filter, the throttle and the queue exactly
 * like a live arrival.
 */
interface BarrierHeld {
  msg: BusMessage;
  expiresAt: number | undefined;
  rawPayload: unknown;
  keyed?: { key: string; keySeq: number };
  causal?: { source: string; clock: number };
}

/**
 * Per-subscriber barrier (cut-marker) delivery state (see
 * `SubscribeOptions.barrier`, EB-98). Created at subscribe time when the
 * subscriber opts in — a subscriber without it pays nothing: no hold
 * buffer, no epoch tracking, and barrier control messages are never even
 * fanned out to it.
 *
 * `lastEpoch` is the highest barrier epoch ever armed on this
 * subscriber's stream (starts at -1 — epoch 0 is a valid first barrier);
 * `armedEpoch` is the epoch currently holding the stream (`null` when no
 * hold is active); `held` buffers post-cut arrivals in delivery order
 * until the armed barrier is handed to the handler. The cut is
 * per-subscriber and spans topics: one epoch sequence orders the whole
 * delivery stream, which is what makes the barrier the cross-topic cut
 * the per-source causal clocks cannot express.
 */
interface BarrierGateState {
  maxHeldMessages: number;
  lastEpoch: number;
  armedEpoch: number | null;
  held: BarrierHeld[];
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

/**
 * Compiled composite include/exclude patterns for one subscriber (see
 * `SubscribeOptions.patterns`, EB-70). The RegExps are shared instances
 * from the bus's composite pattern cache; `cacheKeys` are the (possibly
 * namespace-translated) pattern strings this subscription holds
 * references on, released on unsubscribe.
 */
interface CompositeSubscriberState {
  /** Compiled include patterns, in `SubscribeOptions.patterns` order. */
  include: RegExp[];
  /** Compiled exclude patterns, in `SubscribeOptions.patterns` order. */
  exclude: RegExp[];
  /** Include-match mode: `'any'` or `'all'`. */
  mode: 'any' | 'all';
  /** Composite-cache keys held by this subscription. */
  cacheKeys: string[];
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
   * Slow-consumer backpressure tier (see `SubscribeOptions.backpressure`,
   * EB-83): what the bus does when `queue` is full at enqueue time.
   */
  backpressure: BackpressurePolicy;
  /**
   * Set while the `'block'` tier refuses new arrivals: the subscriber
   * saturated and its backlog is being preserved instead of churned.
   * Cleared when the queue recedes below the high-water mark. Only the
   * `'block'` tier ever sets this; the drop tiers never block.
   */
  blocked: boolean;
  /**
   * Messages refused while `blocked` (the `'block'` tier only). Counted
   * into the bus-wide `droppedMessages` and the dedicated
   * `blockedMessages` stat, and included in `droppedCount(subId)`.
   */
  blockedCount: number;
  /** The queue's count capacity, for saturation checks at enqueue time. */
  queueCapacity: number;
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
   * Slow-start ramp gate (see `SubscribeOptions.slowStart`). Present only
   * while the ramp window has not elapsed — once the ramp completes the
   * gate is dropped, so a warmed-up subscriber pays nothing for it.
   */
  slowStart?: SlowStartGate;
  /**
   * Max in-flight delivery cap state (see `SubscribeOptions.maxInflight`).
   * Absent when the cap is disabled for this subscriber.
   */
  maxInflight?: MaxInflightState;
  /**
   * Idle-reclamation activity mode (see `SubscribeOptions.maxIdleMs` /
   * `idleActivity`, EB-72). Present exactly when the subscriber opted
   * into an idle window — `recordIdleActivity` gates on it, so
   * non-opt-in subscribers pay a single property check per delivery, no
   * allocation and no map lookup. The live timer itself lives in the
   * bus's `IdleReclaimer`, keyed by subscription id.
   */
  idleActivity?: IdleActivityMode;
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
   * Per-subscriber barrier (cut-marker) delivery state (see
   * `SubscribeOptions.barrier`, EB-98). Present only when the subscriber
   * opted in — without it the subscriber holds nothing, tracks no
   * epochs, and barrier control messages are never fanned out to it:
   * zero overhead, zero behavior change.
   */
  barrier?: BarrierGateState;
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
   * Opt-in envelope signature verification (see
   * `SubscribeOptions.verifySignature`, EB-84). False by default — a
   * non-verifying subscriber pays a single boolean check per delivery.
   */
  verifySignature: boolean;
  /**
   * Opt-in publisher-identity verification (see
   * `SubscribeOptions.verifyPublisher`, EB-97). False by default — a
   * non-verifying subscriber pays a single boolean check per delivery.
   */
  verifyPublisher: boolean;
  /**
   * Opt-in durable-log replay progress observer (see
   * `SubscribeOptions.onReplayProgress`, EB-71). Absent unless the
   * subscriber opted in.
   */
  onReplayProgress?: ReplayProgressCallback;
  /**
   * Processed replay entries between `onReplayProgress` fires (see
   * `SubscribeOptions.progressInterval`, EB-71). Defaults to 100.
   */
  progressInterval: number;
  /**
   * This subscriber's latest durable-log replay progress (EB-71).
   * Absent until the first resume replay (`resumeFromSeq` /
   * `resumeFromTime`) runs; kept after the pass finishes so
   * `getStats().replayProgress` stays observable.
   */
  replayProgress?: ReplayProgressState;
  /**
   * Compiled composite include/exclude patterns (see
   * `SubscribeOptions.patterns`, EB-70). Absent when the subscriber has
   * no composite patterns.
   */
  composite?: CompositeSubscriberState;
  /**
   * Batch-delivery state (see `SubscribeOptions.batch`). Absent when
   * batching is disabled for this subscriber.
   */
  batch?: BatchDeliveryState;
  /**
   * Tumbling-window aggregation state (see `SubscribeOptions.window`,
   * EB-73). Absent when the subscriber did not opt in.
   */
  window?: SubscriberWindowState;
  /**
   * Subscriber-side exactly-once dedup window (see
   * `SubscribeOptions.deduplicateMessages`). Absent when dedup is
   * disabled for this subscriber.
   */
  dedup?: SubscriberDedupState;
  /**
   * Shadow traffic mirroring state (see `SubscribeOptions.shadow`).
   * Present only for shadow subscriptions: the subscriber receives a
   * full copy of production traffic in its own queue, tracked in
   * independent counters, with a throwing handler isolated from the
   * flush loop.
   */
  shadow?: ShadowState;
  /**
   * Subscription migration (EB-82): set while an `exportSubscription`
   * snapshot is outstanding for this subscriber. Delivery is paused —
   * `drainSubscriber` skips the subscriber, leaving its backlog queued
   * under the normal backpressure policy — until `completeMigration`
   * unsubscribes it or `cancelMigration` resumes it. The subscriber
   * keeps counting as live in `getStats()` (see `migratingSubscribers`).
   */
  migrating?: boolean;
  /**
   * Subscription migration (EB-82): the outermost unsubscribe closure
   * for this subscription — the plain `subscribe` closure, replaced by
   * the subscribe-family wrappers (`subscribeReliable`) with their
   * outermost teardown. Lets `completeMigration` run the normal
   * unsubscribe path without reimplementing it.
   */
  teardown?: () => void;
  /**
   * Subscription migration (EB-82): the serializable subset of the
   * options this subscription was created with, captured at subscribe
   * time. Function-valued options are excluded (their names are kept in
   * `unsupportedMigrationOptions`); nested option objects are shared by
   * reference with the caller's options bag, not deep-cloned.
   */
  migrationOptions?: Record<string, unknown>;
  /**
   * Subscription migration (EB-82): names of function-valued subscribe
   * options that were set on this subscription (filters, callbacks) and
   * therefore cannot travel in a migration snapshot. Surfaced on the
   * snapshot as `unsupportedOptions` — dropped openly, never silently.
   */
  unsupportedMigrationOptions?: string[];
  /**
   * MQTT-style retained snapshot (EB-87): per-topic `seq` of the retained
   * messages already handed to this subscriber's handler synchronously at
   * subscribe time. A durable-log replay (`resumeFromSeq` /
   * `resumeFromTime`) skips these exact records — the snapshot came
   * first, so replay must never deliver them twice. Absent when the
   * subscription received no retained snapshot.
   */
  retainedDelivered?: Map<string, number>;
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
 * Per-subscriber shadow mirroring state (see `SubscribeOptions.shadow`).
 * Present only for shadow subscriptions. `delivered` / `dropped` are the
 * shadow's independent counters — they never feed the bus-wide
 * `totalDelivered` / `totalDropped`, so a saturated or failing shadow
 * mirror cannot move production stats.
 */
interface ShadowState {
  /** Messages handed to the shadow handler (a throwing handler still counts). */
  delivered: number;
  /** Messages shed by the shadow queue's independent backpressure policy. */
  dropped: number;
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
  publisherRateLimited: number;
  pausedMessages: number;
  rejectedMessages: number;
  deprecatedSchemaVersions: number;
  lateMessages: number;
  duplicateMessages: number;
  filteredMessages: number;
  signatureFailures: number;
  dedupDropped: number;
  aliasRetiredMessages: number;
  redactedFields: number;
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
    publisherRateLimited: 0,
    pausedMessages: 0,
    rejectedMessages: 0,
    deprecatedSchemaVersions: 0,
    lateMessages: 0,
    duplicateMessages: 0,
    filteredMessages: 0,
    signatureFailures: 0,
    dedupDropped: 0,
    aliasRetiredMessages: 0,
    redactedFields: 0,
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
 * Fail-fast type validation for `PublishOptions.partitionKey` /
 * `PublishOptions.partition` (EB-90). Absent is fine; a `partitionKey`
 * must be a non-empty string and a `partition` must be an integer
 * `>= 0`. The range check against the topic's partition count
 * (`< partitionCount`) needs the alias-resolved topic, so it runs in
 * `fanOut` right after alias resolution — still before anything is
 * mutated. Throws `RangeError` before anything is mutated, matching the
 * other publish-option validations.
 */
function validatePartitionOptions(
  partitionKey: string | undefined,
  partition: number | undefined,
  caller: string,
): void {
  if (partitionKey !== undefined && (typeof partitionKey !== 'string' || partitionKey.length === 0)) {
    throw new RangeError(`${caller}: partitionKey must be a non-empty string`);
  }
  if (partition !== undefined && (!Number.isInteger(partition) || partition < 0)) {
    throw new RangeError(`${caller}: partition must be a non-negative integer`);
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

/** Fail-fast validation for `PublishOptions.barrier` (EB-98). */
function validateBarrier(barrier: { epoch?: number } | undefined, caller: string): void {
  if (barrier === undefined) return;
  if (typeof barrier !== 'object' || barrier === null) {
    throw new RangeError(`${caller}: barrier must be an object`);
  }
  const { epoch } = barrier;
  if (!Number.isInteger(epoch) || (epoch as number) < 0) {
    throw new RangeError(`${caller}: barrier.epoch must be a non-negative integer`);
  }
}

/**
 * Publisher identity alphabet: 1–64 ASCII alphanumerics, `_` or `-`.
 * A `publisherId` rides the publish path as an opaque producer label —
 * never interpreted, never a security principal — so the validation is a
 * shape check, not an authorization decision.
 */
const PUBLISHER_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

/** Returns the value when it is a valid publisher identity, else `undefined`. */
function cleanPublisherId(value: unknown): string | undefined {
  return typeof value === 'string' && PUBLISHER_ID_PATTERN.test(value) ? value : undefined;
}

/**
 * Validates `PublishOptions.publisherId` (and its batch/delayed twins).
 * Absent (`undefined`) means an anonymous publish; anything else must be
 * 1–64 ASCII alphanumeric/`_`/`-` characters — anything invalid throws
 * `RangeError` before the publish mutates any state.
 */
function validatePublisherId(publisherId: string | undefined, caller: string): void {
  if (publisherId === undefined) return;
  if (cleanPublisherId(publisherId) === undefined) {
    throw new RangeError(
      `${caller}: publisherId must be 1-64 ASCII alphanumeric, '_' or '-' characters`,
    );
  }
}

/**
 * Validates `PublishOptions.schemaVersion` (and its batch/delayed twins).
 * Absent (`undefined`) means "the latest registered version"; anything
 * else must be a non-negative integer — anything invalid throws
 * `RangeError` before the publish mutates any state.
 */
function validateSchemaVersion(schemaVersion: number | undefined, caller: string): void {
  if (schemaVersion === undefined) return;
  if (!Number.isInteger(schemaVersion) || schemaVersion < 0) {
    throw new RangeError(`${caller}: schemaVersion must be a non-negative integer`);
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
 * Whether every topic matched by pattern `sub` is also matched by pattern
 * `cover` — the language-inclusion test the composite-subscription (EB-70)
 * validation uses to reject exclude sets that could never let a message
 * through. Uses exactly the `compilePattern` wildcard semantics: `*`
 * matches one segment, `**` zero or more, other segments literally; a
 * bare `*` or `**` matches every topic; consecutive `**` collapse like in
 * `compilePattern`.
 *
 * Decided by NFA subset construction: both patterns become segment NFAs
 * (`**` is an epsilon plus an any-segment self-loop), the cover NFA is
 * determinized, and a product BFS looks for a sub-accepting state paired
 * with a cover-rejecting DFA state — i.e. a witness topic in
 * `L(sub) \ L(cover)`. The alphabet is every literal segment appearing in
 * either pattern plus one symbol for "any other segment" (a single fresh
 * segment works as the witness wherever that symbol appears, because no
 * literal transition can fire on it). Finite state space, so the search
 * always terminates.
 */
export function patternCovers(cover: string, sub: string): boolean {
  // Normalize the bare-`*` special case (`compilePattern` treats a lone
  // `*` like `**`: matches every topic) and collapse `**` runs, exactly
  // as `compilePattern` does.
  const normalize = (pattern: string): string[] =>
    collapseStars((pattern === '*' ? '**' : pattern).split('.'));
  const cSegs = normalize(cover);
  const sSegs = normalize(sub);
  // One NFA per pattern: states 0..n, n accepting. `**` at j contributes
  // an epsilon j -> j+1 and an any-segment self-loop j -> j; `*` an
  // any-segment j -> j+1; a literal an l-labelled j -> j+1.
  const buildNfa = (segs: string[]) => {
    const n = segs.length;
    const eps: number[][] = Array.from({ length: n + 1 }, () => []);
    const any: number[][] = Array.from({ length: n + 1 }, () => []);
    const lit: Array<Map<string, number[]>> = Array.from(
      { length: n + 1 },
      () => new Map<string, number[]>(),
    );
    for (let j = 0; j < n; j++) {
      const seg = segs[j];
      if (seg === '**') {
        eps[j].push(j + 1);
        any[j].push(j);
      } else if (seg === '*') {
        any[j].push(j + 1);
      } else {
        const targets = lit[j].get(seg);
        if (targets === undefined) lit[j].set(seg, [j + 1]);
        else targets.push(j + 1);
      }
    }
    return { n, eps, any, lit };
  };
  const cNfa = buildNfa(cSegs);
  const sNfa = buildNfa(sSegs);
  // The OTHER symbol stands for any segment that is none of the literals
  // appearing in either pattern.
  const OTHER = '\0';
  const alphabet = new Set<string>([OTHER]);
  for (const seg of [...cSegs, ...sSegs]) {
    if (seg !== '*' && seg !== '**') alphabet.add(seg);
  }
  const closure = (
    nfa: ReturnType<typeof buildNfa>,
    states: Iterable<number>,
  ): Set<number> => {
    const closed = new Set<number>(states);
    const stack = [...closed];
    while (stack.length > 0) {
      const q = stack.pop() as number;
      for (const next of nfa.eps[q]) {
        if (!closed.has(next)) {
          closed.add(next);
          stack.push(next);
        }
      }
    }
    return closed;
  };
  const nfaMove = (
    nfa: ReturnType<typeof buildNfa>,
    states: Set<number>,
    symbol: string,
  ): Set<number> => {
    const targets = new Set<number>();
    for (const q of states) {
      for (const next of nfa.any[q]) targets.add(next);
      if (symbol !== OTHER) {
        const litTargets = nfa.lit[q].get(symbol);
        if (litTargets !== undefined) {
          for (const next of litTargets) targets.add(next);
        }
      }
    }
    return closure(nfa, targets);
  };
  // Determinize the cover NFA: a DFA state is the epsilon-closed set of
  // NFA states, keyed by its sorted join.
  const dfaKey = (states: Set<number>): string =>
    [...states].sort((a, b) => a - b).join(',');
  const dfaStart = closure(cNfa, [0]);
  const dfaMoveCache = new Map<string, Set<number>>();
  const dfaMove = (states: Set<number>, symbol: string): Set<number> => {
    const key = `${dfaKey(states)}@${symbol}`;
    const cached = dfaMoveCache.get(key);
    if (cached !== undefined) return cached;
    const moved = nfaMove(cNfa, states, symbol);
    dfaMoveCache.set(key, moved);
    return moved;
  };
  // Product BFS over (sub-NFA state set, cover-DFA state): a witness is
  // a reachable pair where the sub side accepts and the cover side does
  // not — a topic `sub` matches that `cover` rejects.
  const sStart = closure(sNfa, [0]);
  const seen = new Set<string>([`${dfaKey(sStart)}|${dfaKey(dfaStart)}`]);
  const queue: Array<[Set<number>, Set<number>]> = [[sStart, dfaStart]];
  while (queue.length > 0) {
    const [sStates, cStates] = queue.pop() as [Set<number>, Set<number>];
    for (const symbol of alphabet) {
      const sNext = nfaMove(sNfa, sStates, symbol);
      const cNext = dfaMove(cStates, symbol);
      if (sNext.has(sNfa.n) && !cNext.has(cNfa.n)) {
        // Witness topic found: `cover` does not cover `sub`.
        return false;
      }
      const seenKey = `${dfaKey(sNext)}|${dfaKey(cNext)}`;
      if (!seen.has(seenKey)) {
        seen.add(seenKey);
        queue.push([sNext, cNext]);
      }
    }
  }
  return true;
}

/**
 * Validates `SubscribeOptions.patterns` (see `CompositeSubscribeOptions`,
 * EB-70) and returns the resolved include/exclude lists with the effective
 * mode. Returns `undefined` when the option is absent. Throws `TypeError`
 * for a non-object option and `RangeError` for invalid combinations: an
 * empty `patterns` array, a non-`'any'`/`'all'` mode, a non-string or
 * empty pattern, or an exclude set that covers the include space so no
 * message could ever be delivered (mode `'any'`: every include pattern
 * covered by some exclude pattern; mode `'all'`: one exclude pattern
 * covering every include pattern — coverage decided by `patternCovers`).
 *
 * The returned lists are copies: later mutation of the caller's arrays
 * cannot change the subscription.
 */
function resolveCompositeOptions(
  opt: CompositeSubscribeOptions | undefined,
): { patterns: string[]; exclude: string[]; mode: 'any' | 'all' } | undefined {
  if (opt === undefined) return undefined;
  const caller = 'subscribe';
  if (opt === null || typeof opt !== 'object' || Array.isArray(opt)) {
    throw new TypeError(`${caller}: patterns must be an object`);
  }
  const { patterns, mode, exclude } = opt;
  if (!Array.isArray(patterns) || patterns.length === 0) {
    throw new RangeError(
      `${caller}: patterns.patterns must be a non-empty array of topic patterns`,
    );
  }
  for (const pattern of patterns) {
    if (typeof pattern !== 'string' || pattern.length === 0) {
      throw new RangeError(
        `${caller}: patterns.patterns must contain only non-empty pattern strings`,
      );
    }
  }
  const resolvedMode = mode ?? 'any';
  if (resolvedMode !== 'any' && resolvedMode !== 'all') {
    throw new RangeError(`${caller}: patterns.mode must be 'any' or 'all'`);
  }
  const resolvedExclude = exclude ?? [];
  if (!Array.isArray(resolvedExclude)) {
    throw new RangeError(`${caller}: patterns.exclude must be an array of topic patterns`);
  }
  for (const pattern of resolvedExclude) {
    if (typeof pattern !== 'string' || pattern.length === 0) {
      throw new RangeError(
        `${caller}: patterns.exclude must contain only non-empty pattern strings`,
      );
    }
  }
  // An exclude set that covers the include space makes the subscription
  // permanently silent — every message the includes admit, an exclude
  // drops — so fail fast instead of registering a dead subscription.
  const coversAll =
    resolvedMode === 'any'
      ? patterns.every((p) => resolvedExclude.some((e) => patternCovers(e, p)))
      : resolvedExclude.some((e) => patterns.every((p) => patternCovers(e, p)));
  if (coversAll) {
    throw new RangeError(
      `${caller}: patterns.exclude covers the include patterns — no message could ever be delivered`,
    );
  }
  return { patterns: [...patterns], exclude: [...resolvedExclude], mode: resolvedMode };
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
 * Per-subscriber max in-flight delivery state (see
 * `SubscribeOptions.maxInflight`). `max` is the configured cap on
 * simultaneously unsettled handler invocations; `inFlight` is how many
 * handler invocations are currently unsettled.
 */
interface MaxInflightState {
  max: number;
  inFlight: number;
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
 * Validates `SubscribeOptions.maxInflight` and returns the configured cap.
 * Returns `undefined` when the cap is disabled. Throws `RangeError` for
 * non-integer or < 1 values.
 */
function resolveMaxInflightOptions(opt: number | undefined): number | undefined {
  if (opt == null) return undefined;
  if (!Number.isInteger(opt) || opt < 1) {
    throw new RangeError('subscribe: maxInflight must be an integer >= 1');
  }
  return opt;
}

/**
 * Returns true when `value` is a thenable (has a callable `then`).
 * Used to detect async plain handlers, whose returned promise holds an
 * in-flight slot until it settles (see `SubscribeOptions.maxInflight`).
 */
function isThenable(value: unknown): value is Promise<unknown> {
  return (
    value !== null &&
    (typeof value === 'object' || typeof value === 'function') &&
    typeof (value as { then?: unknown }).then === 'function'
  );
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
 * Validates `SubscribeOptions.barrier` and builds the initial barrier
 * gate state (EB-98). Returns `undefined` when the subscriber did not opt
 * in. Throws `RangeError` for invalid values, before anything registers.
 */
function resolveBarrierOptions(
  barrier: boolean | BarrierSubscribeOptions | undefined,
): BarrierGateState | undefined {
  if (barrier === undefined || barrier === false) return undefined;
  let maxHeldMessages = 1000;
  if (barrier !== true) {
    if (typeof barrier !== 'object' || barrier === null) {
      throw new RangeError('subscribe: barrier must be a boolean or a BarrierSubscribeOptions object');
    }
    if (barrier.maxHeldMessages !== undefined) {
      if (!Number.isInteger(barrier.maxHeldMessages) || barrier.maxHeldMessages < 1) {
        throw new RangeError('subscribe: barrier.maxHeldMessages must be a positive integer');
      }
      maxHeldMessages = barrier.maxHeldMessages;
    }
  }
  return {
    maxHeldMessages,
    lastEpoch: -1,
    armedEpoch: null,
    held: [],
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
 * Aggregation functions for subscriber-side tumbling windows (see
 * `SubscribeOptions.window`, EB-73). `count` counts the window's
 * messages; the numeric four fold the extracted numeric values;
 * `last` takes the last message's extracted value; `concat`
 * string-concatenates every extracted value.
 */
export type WindowAggregateKind =
  | 'count'
  | 'sum'
  | 'avg'
  | 'min'
  | 'max'
  | 'last'
  | 'concat';

const WINDOW_AGGREGATES: ReadonlySet<string> = new Set([
  'count',
  'sum',
  'avg',
  'min',
  'max',
  'last',
  'concat',
]);

/**
 * Opt-in subscriber-side tumbling-window aggregation (see
 * `SubscribeOptions.window`, EB-73).
 */
export interface WindowAggregateOptions {
  /**
   * Window length in milliseconds on the bus clock
   * (`EventBusOptions.now`). The first admitted message opens a window
   * `[windowStart, windowStart + windowMs)`; when the window closes the
   * subscriber receives a single aggregate message
   * (`WindowAggregateResult`) instead of the raw messages. Must be a
   * positive finite number.
   */
  windowMs: number;
  /**
   * How the window's messages fold into `WindowAggregateResult.value`.
   * Must be one of `'count' | 'sum' | 'avg' | 'min' | 'max' | 'last' |
   * 'concat'`.
   */
  aggregate: WindowAggregateKind;
  /**
   * Dot-separated payload field the aggregation reads (e.g.
   * `"metrics.price"`); when absent the whole payload is the value.
   * For the numeric aggregates (`sum`/`avg`/`min`/`max`) a message
   * whose extracted value is not a finite number is ignored entirely
   * (not counted); for `concat` a message whose extracted value is
   * `undefined` is ignored; `count` and `last` count every admitted
   * message. `__proto__`/`constructor`/`prototype` segments are
   * rejected with `RangeError` at subscribe time.
   */
  field?: string;
}

/**
 * The single message a tumbling window delivers when it closes (see
 * `SubscribeOptions.window`, EB-73). It travels the subscriber's normal
 * delivery path — backpressure queue, reliable delivery, drain-time
 * shaping — exactly like a published message.
 */
export interface WindowAggregateResult {
  /** Bus-clock (`EventBusOptions.now`) start of the closed window. */
  windowStart: number;
  /** `windowStart + windowMs`: the window's exclusive end. */
  windowEnd: number;
  /** Messages that contributed to this aggregate. */
  count: number;
  /**
   * The folded value: the count itself for `'count'`; the
   * sum/mean/min/max of the extracted finite numbers for the numeric
   * aggregates (`null` when no message carried a finite number); the
   * last message's extracted value for `'last'`; the concatenated
   * string forms for `'concat'`.
   */
  value: unknown;
}

/**
 * Field-path segments that must never be traversed when extracting a
 * window aggregation field: honoring them would read object prototypes
 * instead of data. Rejected at subscribe time (fail-fast `RangeError`).
 */
const FORBIDDEN_WINDOW_FIELD_SEGMENTS = new Set(['__proto__', 'constructor', 'prototype']);

/**
 * Parses and validates a `WindowAggregateOptions.field` dot path.
 * Returns the segments, or `undefined` when no field was given. Throws
 * `RangeError` for empty paths, empty segments, and forbidden
 * prototype-polluting segments.
 */
function parseWindowFieldPath(field: string | undefined): string[] | undefined {
  if (field === undefined) return undefined;
  if (typeof field !== 'string' || field.length === 0) {
    throw new RangeError('subscribe: window.field must be a non-empty string');
  }
  const segments = field.split('.');
  for (const segment of segments) {
    if (segment.length === 0) {
      throw new RangeError(`subscribe: window.field "${field}" contains an empty segment`);
    }
    if (FORBIDDEN_WINDOW_FIELD_SEGMENTS.has(segment)) {
      throw new RangeError(
        `subscribe: window.field "${field}" uses the forbidden segment "${segment}"`,
      );
    }
  }
  return segments;
}

/**
 * Reads the aggregation value out of a raw payload along a parsed field
 * path. Returns the whole payload when no field was given; `undefined`
 * when any segment is missing (arrays are opaque — never indexed).
 */
function extractWindowValue(payload: unknown, fieldPath: string[] | undefined): unknown {
  if (fieldPath === undefined) return payload;
  let node: unknown = payload;
  for (const segment of fieldPath) {
    if (typeof node !== 'object' || node === null) return undefined;
    node = (node as Record<string, unknown>)[segment];
  }
  return node;
}

/**
 * Per-subscriber tumbling-window aggregation state (see
 * `SubscribeOptions.window`, EB-73). Absent when the subscriber did not
 * opt in — non-opt-in subscribers pay a single property check per
 * delivery, no allocation and no timer.
 *
 * The accumulators are O(1) per message (`concat` additionally keeps
 * the stringified parts): a hot topic fills no per-message queue, which
 * is the point — downstream QPS collapses to ~1/windowMs.
 */
interface SubscriberWindowState {
  windowMs: number;
  aggregate: WindowAggregateKind;
  fieldPath: string[] | undefined;
  /** Bus-clock start of the open window; 0 when no window is open. */
  windowStart: number;
  /** `windowStart + windowMs`: the open window's exclusive end. */
  windowEnd: number;
  /** Messages that contributed to the open window. */
  count: number;
  /** Numeric fold accumulators (only for sum/avg/min/max). */
  sum: number;
  numericCount: number;
  min: number;
  max: number;
  /** Last extracted value (only for `last`). */
  last: unknown;
  /** Stringified parts (only for `concat`). */
  parts: string[];
  /** Topic/seq/epoch of the last admitted message: the aggregate's envelope. */
  lastTopic: string;
  lastSeq: number;
  lastEpoch: string;
  /** Window-close timer, armed while a window is open. Unref'd. */
  timer?: ReturnType<typeof setTimeout>;
}

/**
 * Validates `SubscribeOptions.window` and builds the initial window
 * state. Returns `undefined` when window aggregation is disabled.
 * Throws `TypeError` for a non-object value and `RangeError` for an
 * invalid `windowMs`, `aggregate`, or `field`.
 */
function resolveWindowOptions(
  opt: WindowAggregateOptions | undefined,
): SubscriberWindowState | undefined {
  if (opt === undefined) return undefined;
  if (typeof opt !== 'object' || opt === null) {
    throw new TypeError('subscribe: window must be a WindowAggregateOptions object');
  }
  const windowMs = opt.windowMs;
  if (!Number.isFinite(windowMs) || windowMs <= 0) {
    throw new RangeError('subscribe: window.windowMs must be a positive finite number of milliseconds');
  }
  const aggregate = opt.aggregate;
  if (typeof aggregate !== 'string' || !WINDOW_AGGREGATES.has(aggregate)) {
    throw new RangeError(
      "subscribe: window.aggregate must be one of 'count' | 'sum' | 'avg' | 'min' | 'max' | 'last' | 'concat'",
    );
  }
  const fieldPath = parseWindowFieldPath(opt.field);
  return {
    windowMs,
    aggregate: aggregate as WindowAggregateKind,
    fieldPath,
    windowStart: 0,
    windowEnd: 0,
    count: 0,
    sum: 0,
    numericCount: 0,
    min: Infinity,
    max: -Infinity,
    last: undefined,
    parts: [],
    lastTopic: '',
    lastSeq: 0,
    lastEpoch: '',
    timer: undefined,
  };
}

/**
 * Validates `SubscribeOptions.shadow` and returns the shadow queue's
 * explicit overrides (unset fields fall back to the subscription's own
 * `queueSize` / `dropPolicy` at the `subscribe` call site). Returns
 * `undefined` when shadowing is disabled. Throws `TypeError` for a
 * non-object value and `RangeError` for an invalid queue size or drop
 * policy.
 */
function resolveShadowOptions(
  shadow: boolean | ShadowSubscribeOptions | undefined,
): { queueSize?: number; dropPolicy?: DropPolicy } | undefined {
  if (shadow === undefined || shadow === false) return undefined;
  if (shadow === true) return {};
  if (typeof shadow !== 'object' || shadow === null) {
    throw new TypeError('subscribe: shadow must be a boolean or a ShadowSubscribeOptions object');
  }
  const queueSize = shadow.queueSize;
  if (queueSize !== undefined && (!Number.isInteger(queueSize) || queueSize <= 0)) {
    throw new RangeError('subscribe: shadow.queueSize must be a positive integer');
  }
  const dropPolicy = shadow.dropPolicy;
  if (dropPolicy !== undefined && dropPolicy !== 'drop-oldest' && dropPolicy !== 'drop-newest') {
    throw new RangeError("subscribe: shadow.dropPolicy must be 'drop-oldest' or 'drop-newest'");
  }
  return { queueSize, dropPolicy };
}

/**
 * Resolves the slow-consumer backpressure tier (EB-83). Defaults to
 * `'block'` — backward compatible: a single saturation still sheds
 * exactly like the historical default, and the tier only changes behavior
 * under sustained saturation (refuse-new instead of churning the backlog).
 * An explicit `dropPolicy` that contradicts an explicitly chosen drop tier
 * is a configuration error — fail fast instead of silently picking one.
 */
function resolveBackpressurePolicy(
  backpressure: unknown,
  dropPolicy: unknown,
): BackpressurePolicy {
  if (backpressure === undefined) return 'block';
  assertValidBackpressurePolicy(backpressure, 'subscribe');
  if (
    dropPolicy !== undefined &&
    backpressure !== 'block' &&
    dropPolicy !== backpressure
  ) {
    throw new RangeError(
      `subscribe: backpressure '${backpressure}' conflicts with dropPolicy '${dropPolicy}'`,
    );
  }
  return backpressure;
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
 * Options for one versioned schema registration (see
 * `EventBus.setTopicSchemaVersion`).
 */
export interface TopicSchemaVersionOptions {
  /**
   * Migration window this registration grants to the versions it
   * supersedes (bus-clock milliseconds): when a greater version
   * registers, every older version in the same rule set remains
   * admissible for this long — a publish carrying an old version is
   * still validated and admitted inside the window (counted in
   * `deprecatedSchemaVersions`), and rejected past it. The window
   * boundary is inclusive. Must be a non-negative finite number.
   * Default 0: superseded versions are rejected immediately — strict,
   * fail-fast migration.
   */
  graceMs?: number;
  /**
   * Opt-in schema-compatibility regression gate (EB-95): the sample set of
   * payloads that older versions of this topic's schema accepted (callers
   * usually capture them from production traffic or a shared fixture).
   * At registration time the new `validator` is run against every sample
   * (with the registration `topicPattern` as the `topic` argument); if the
   * validator rejects — or throws on — any sample, registration fails with
   * `RangeError` and nothing is registered: existing versions keep their
   * state untouched and the failing version never becomes visible. A
   * backfilled lower version is gated the same way as a new latest one.
   * Omit the field (or pass `undefined`) to skip the gate entirely —
   * skipping is the default and is reported in stats as `'not-tested'`.
   */
  compatibilityCorpus?: unknown[];
}

/**
 * One registered schema version inside a topic's (or topic pattern's)
 * versioned rule set. `supersededAt` is stamped on the bus clock when the
 * first strictly greater version registers (a backfilled lower version is
 * born superseded); `graceMs` is the effective migration window — the
 * grant from the superseding registration once superseded, the dormant
 * grant this registration will give when it supersedes others while it
 * is the latest.
 */
interface SchemaVersionRegistration {
  version: number;
  validator: SchemaValidator;
  registeredAt: number;
  supersededAt?: number;
  graceMs: number;
  /**
   * Compatibility regression gate result (EB-95): `'passed'` when the
   * version registered with `compatibilityCorpus` and its validator
   * accepted every sample at registration time, `'not-tested'` when no
   * corpus was supplied. A failed gate never registers, so no row ever
   * persists a failed result.
   */
  compatGate: 'passed' | 'not-tested';
  /** Number of corpus samples the gate ran (0 when `compatGate` is `'not-tested'`). */
  compatCorpusSize: number;
}

/**
 * Why one entry of an atomic batch was rejected: the publisher's
 * rate-limit budget had no token left for it (`'publisher-rate-limit'`),
 * the ACL denied the publish (`'acl'`), its payload failed the topic's
 * schema validator (`'schema'`), its payload exceeded the topic's
 * per-message size cap (`'message-too-big'`), or the topic's rate-limit
 * bucket had no token left for it (`'rate-limit'`). TTL expiry is
 * drain-time and never rejects a publish, so it cannot appear here.
 */
export type AtomicRejectReason =
  | 'acl'
  | 'schema'
  | 'message-too-big'
  | 'rate-limit'
  | 'publisher-rate-limit'
  | 'alias-retired'
  | 'topic-paused';

/**
 * Pause mode for `EventBus.pauseTopic` (EB-88, ops window):
 * - `'shed'` (default): publishes to a matching topic are rejected
 *   outright while the pause is active — counted in
 *   `TopicStats.pausedMessages` / `BusStats.pausedMessages`, surfaced on
 *   `onAdmissionRejected` with reason `'topic-paused'`, and consuming a
 *   sequence number so subscribers observe the loss as a gap (the EB-19
 *   shed semantics, not the schema-rejection semantics).
 * - `'queue'`: publishes to a matching topic are held in a durable-side
 *   buffer instead of being rejected — no sequence number, no
 *   schema/rate-limit budget burned — and fan out in original order when
 *   the topic resumes. Requires `EventBusOptions.durableLogDir`.
 */
export type TopicPauseMode = 'shed' | 'queue';

/** Options for `EventBus.pauseTopic`. */
export interface TopicPauseOptions {
  /**
   * Pause behavior while the rule is active: `'shed'` (default) rejects
   * publishes outright; `'queue'` buffers them for the resume. Anything
   * else throws `RangeError` at call time.
   */
  mode?: TopicPauseMode;
}

/**
 * One active topic-pause rule, as exposed by
 * `BusStats.pausedTopics` (`EventBus.getStats()`).
 */
export interface PausedTopicRule {
  /** The exact rule key passed to `EventBus.pauseTopic` (topic or pattern). */
  topicPattern: string;
  /** The rule's pause mode (`'shed'` | `'queue'`). */
  mode: TopicPauseMode;
  /** Messages currently held in the rule's durable-side buffer (queue mode only; 0 for shed). */
  bufferedMessages: number;
}

/**
 * One message held by a queue-mode pause rule (EB-88). Carries everything
 * the resume fan-out needs to re-run the full admission pipeline: the
 * admission-redacted payload (redaction ran at pause time, like the
 * delayed-delivery schedule path), the TTL deadline stamped at pause
 * admission (`expiresAt` — never recomputed at resume), and the publish
 * options that ride a normal `fanOut`. No sequence number and no
 * rate-limit budget was burned at pause time; both are drawn at resume.
 * Internal-only flags (`routed`, `fromBridge`, `diagnostic`) are runtime
 * state: they do not survive restart — recovery fans the message out as
 * an ordinary publish.
 */
interface PauseBufferEntry {
  /** Opaque handle (`paused-1`, …), also the durable-log identity. */
  pauseId: string;
  /** Global FIFO ordinal: resume fans out in `pauseOrder` across topics. */
  pauseOrder: number;
  /** Concrete (alias-resolved) topic the message was published to. */
  topic: string;
  /** Payload as redacted at pause admission. */
  payload: unknown;
  /**
   * Binary-payload marker (EB-92): `true` when `payload` is raw bytes.
   * Carried through the buffer, the pause journal, and restart recovery
   * so the resume fan-out stays byte-oriented.
   */
  binary?: boolean;
  /**
   * TTL expiry deadline stamped at pause admission (bus clock), when a
   * TTL rule matched the topic. A message whose deadline passes while
   * buffered is dropped as expired at resume — counted in
   * `expiredMessages`, never resurrected (the EB-32 precedent).
   */
  expiresAt?: number;
  key?: string;
  traceparent?: string;
  messageId?: string;
  eventTime?: number;
  causal?: { source?: string; clock: number };
  publisherId?: string;
  schemaVersion?: number;
  retain?: boolean;
  routed?: boolean;
  fromBridge?: boolean;
  diagnostic?: boolean;
}

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
 * - `'message-too-big'` → `TopicStats.rejectedMessages` /
 *   `BusStats.rejectedMessages` (payload's JSON byte size exceeded the
 *   topic's per-message size cap, EB-64; shares the rejection counter with
 *   `'schema'`, distinguished by the reason)
 * - `'rate-limit'` → `TopicStats.rateLimitedMessages` /
 *   `BusStats.rateLimitedMessages` (topic token bucket empty, EB-19)
 * - `'publisher-rate-limit'` → `TopicStats.publisherRateLimited` /
 *   `BusStats.publisherRateLimited` (per-publisher token bucket empty,
 *   EB-79; shed in the admission phase — no sequence number consumed, so
 *   unlike a `'rate-limit'` shed it is invisible to gap detection)
 * - `'duplicate'` → `TopicStats.duplicateMessages` /
 *   `BusStats.duplicateMessages` (idempotent-publish suppression, EB-27)
 * - `'alias-retired'` → `TopicStats.aliasRetiredMessages` /
 *   `BusStats.aliasRetiredMessages` (publish to a topic whose migration
 *   alias expired — the old topic is read-only, EB-44)
 * - `'topic-paused'` → `TopicStats.pausedMessages` /
 *   `BusStats.pausedMessages` (publish to a topic paused in shed mode —
 *   the shed consumes a sequence number like a `'rate-limit'` shed, so
 *   subscribers see the loss as a gap, EB-88)
 *
 * A `publishAtomic` batch rejection surfaces as the failing entry's
 * underlying gate reason (`'acl'` | `'schema'` | `'message-too-big'` |
 * `'rate-limit'` | `'publisher-rate-limit'` | `'alias-retired'` |
 * `'topic-paused'`); the batch rejection
 * is counted once against that entry's topic, so it stays reconcilable
 * too.
 */
export type AdmissionRejectReason =
  | 'acl'
  | 'schema'
  | 'message-too-big'
  | 'rate-limit'
  | 'publisher-rate-limit'
  | 'duplicate'
  | 'alias-retired'
  | 'topic-paused';

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
  /**
   * Publisher identity the rejected publish carried
   * (`PublishOptions.publisherId`), when it carried one — so an admission
   * rejection stays attributable to its producer. Absent for anonymous
   * publishes.
   */
  publisher?: string;
}

/**
 * Publish-side admission-rejection hook (see
 * `EventBusOptions.onAdmissionRejected`). The callback is error-isolated:
 * a throwing callback is swallowed so a broken observer can never disturb
 * the publish path.
 */
export type AdmissionRejectionCallback = (event: AdmissionRejectionEvent) => void;

/**
 * Snapshot delivered to `EventBusOptions.onUnregisteredTopic` when a
 * message is published to a topic with no live registration (EB-77). The
 * publish is not blocked — this is the observability channel for typo'd
 * topic names and producers that forgot to register.
 */
export interface UnregisteredTopicEvent {
  /** The concrete topic the publish targeted (post-namespace resolution). */
  topic: string;
  /** Bus-clock timestamp of the publish (`EventBusOptions.now`). */
  at: number;
  /**
   * Running total of publishes to unregistered topics, including this
   * one — always in sync with `getStats().unregisteredTopicPublishes`.
   */
  unregisteredPublishes: number;
}

/**
 * Unregistered-topic publish hook (see
 * `EventBusOptions.onUnregisteredTopic`). Error-isolated like
 * `onAdmissionRejected`: a throwing callback is swallowed so a broken
 * observer can never disturb the publish path.
 */
export type UnregisteredTopicCallback = (event: UnregisteredTopicEvent) => void;

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
   * Publisher identity (see `PublishOptions.publisherId`): the forwarded
   * message is the same logical publish, so it keeps its producer
   * attribution on the destination bus. Sanitized on receipt — a frame
   * carrying an invalid identity forwards as anonymous rather than
   * failing the forward.
   */
  publisherId?: string;
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
  /**
   * Binary-payload marker (EB-92): `true` when `payload` is raw bytes.
   * The destination bus re-marks the message so its admission stays
   * byte-oriented.
   */
  binary?: boolean;
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
 * Thrown by `subscribe` (and the subscribe-family wrappers) when the
 * subscription pattern falls under a per-topic subscriber cap
 * (`setTopicMaxSubscribers`) that is already full. The check runs before
 * registration, so the call never returns a subscription and no
 * half-registered state is left behind. Lowering a cap never kicks
 * existing subscribers — only new registrations are rejected.
 */
export class SubscriptionLimitError extends Error {
  /** The subscription pattern that was rejected. */
  readonly pattern: string;
  /** The cap that was already reached. */
  readonly limit: number;
  /** Live subscribers counted against the cap at rejection time. */
  readonly current: number;

  constructor(pattern: string, limit: number, current: number) {
    super(
      `subscriber limit reached for pattern "${pattern}": ${current}/${limit} subscribers`,
    );
    this.name = 'SubscriptionLimitError';
    this.pattern = pattern;
    this.limit = limit;
    this.current = current;
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
   * Publisher identity for this publish (EB-65). A producer-side label —
   * service name, client id, tenant — aggregated into per-publisher
   * publish counters (`getStats().publishers` and the
   * `eventbus_publisher_published_messages_total` exposition series), and
   * carried on admission-rejection events (`onAdmissionRejected`) so a
   * rejected publish is attributable to its producer. The identity is
   * persisted on the durable-log record and restored on replay, so
   * attribution survives restarts (delayed schedules included). It is a
   * label, not a credential: the bus never authenticates it. Must be 1–64
   * ASCII alphanumeric, `_` or `-` characters; anything else throws
   * `RangeError` from the publish entry point before anything is
   * mutated. Absent (`undefined`) means an anonymous publish — no
   * per-publisher counting, no label on the log record.
   */
  publisherId?: string;
  /**
   * Schema version for versioned topic validators (see
   * `setTopicSchemaVersion`). The publish is validated against this
   * version's validator; absent means the latest registered version.
   * An unknown version — or a superseded version past its grace window —
   * is rejected exactly like a schema failure: counted in
   * `rejectedMessages`, no sequence number consumed, nothing written to
   * the durable log. A publish admitted under a superseded-but-graced
   * version is counted in `deprecatedSchemaVersions` (per-topic and
   * global) — the migrate-your-producers signal. Ignored when the topic
   * has no versioned validators registered (the legacy
   * `setTopicSchema` validator, if any, still applies). Must be a
   * non-negative integer when provided; anything else throws
   * `RangeError` from the publish entry point before anything is
   * mutated.
   */
  schemaVersion?: number;
  /**
   * MQTT-style retained message (see `EventBus.setTopicRetained`,
   * EB-87): when `true`, this admitted publish additionally becomes the
   * topic's retained message — the latest value a late-joining
   * subscriber receives as a snapshot at subscribe time, replacing any
   * previous retained value for the exact (alias-resolved) topic.
   * Publishing an empty payload with `retain: true` clears the topic's
   * retained message instead ("empty" is measured in JSON-serialized
   * bytes: `undefined` and values `JSON.stringify` cannot represent
   * serialize to zero bytes; `null` serializes to `null` and is not
   * empty). The retain flag is honored only when a retained rule
   * resolves to enabled for the topic (`setTopicRetained`); otherwise
   * the publish is an ordinary publish and the flag is ignored. The
   * message itself always goes through the normal publish pipeline
   * (admission, fan-out, durable log) — a rejected or shed publish
   * never touches the retained store. Only `publish` carries this
   * option; batch/atomic/delayed publishes do not retain.
   */
  retain?: boolean;
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
  /**
   * Publish-side partition routing (EB-90, real-time systems): assigns
   * this publish to one of the topic's partitions
   * (`EventBus.setTopicPartitions`, default 1). Two opt-in selectors:
   *
   * - `partitionKey`: a consistent-hash selector — the partition is
   *   `sha256(partitionKey) mod partitionCount`, so the same key always
   *   lands on the same partition (the Kafka-style sticky routing a
   *   sharded consumer tier needs). Must be a non-empty string when
   *   provided; anything else throws `RangeError` from `publish`.
   * - `partition`: an explicit partition number that wins over
   *   `partitionKey` when both are set. Must be an integer `>= 0` and
   *   `<` the topic's partition count; anything else throws `RangeError`
   *   from the publish call before anything is mutated. On a topic with
   *   the default single partition the only valid explicit value is `0`,
   *   and a `partitionKey` always hashes to `0`.
   *
   * Each (topic, partition) pair has an independent sequence counter
   * (the EB-13 per-topic seq, extended): `BusMessage.seq` is scoped to
   * the message's partition, and subscriber gap detection baselines are
   * tracked per (topic, partition). The partition is a seq/routing
   * dimension only — it composes with per-key publish-order delivery
   * (`PublishOptions.key`, EB-36): same-key ordering is enforced by the
   * keySeq mechanism regardless of partition, so one key keeps its
   * publish order even when its messages land on different partitions.
   * The assigned partition is stamped on the envelope
   * (`BusMessage.partition`) and persisted on the durable-log record, so
   * replay restores it and numbering never restarts across restarts.
   */
  partitionKey?: string;
  partition?: number;
  /**
   * Binary payload passthrough (EB-92, opt-in): the payload is raw bytes
   * (`Uint8Array`, or an `ArrayBuffer` which is wrapped in a zero-copy
   * view) that travel the whole pipeline without JSON serialization —
   * no `JSON.stringify` at admission, in compression, in signing, or in
   * the durable log. Anything else as payload with `binary: true`
   * throws `TypeError` from the publish entry point, before anything is
   * mutated.
   *
   * Pipeline semantics for binary messages:
   * - the schema validator (EB-20) receives the `Uint8Array` — a binary
   *   topic's validator is a *byte* validator, written against bytes;
   * - the max-message-bytes cap (EB-64) and the compression threshold
   *   (EB-22) measure wire bytes, not JSON length;
   * - the HMAC topic signature (EB-84) covers the wire bytes;
   * - the durable log stores a v2 line (`payload_b64`, explicit type
   *   marker) — a JSON reader never mistakes the bytes for a value;
   * - redaction (EB-68) is skipped: bytes have no fields;
   * - subscribers receive the message with `payload` as a `Uint8Array`
   *   (compressed binary messages are inflated transparently, still as
   *   bytes — never `JSON.parse`d).
   *
   * Only `publish` / `publishIdempotent` accept it; batch, atomic and
   * delayed publishes do not carry per-message binary flags, and the
   * bridge transport does not encode binary payloads (a JSON-based
   * transport would need its own framing — binary messages are an
   * in-process feature).
   */
  binary?: boolean;
  /**
   * Barrier cut marker (EB-98, opt-in): this publish is a barrier control
   * message, not business data — a consistency cut point for
   * barrier-opted-in subscribers (see `SubscribeOptions.barrier`), e.g.
   * a config switch or a snapshot boundary. `epoch` is the cut's
   * sequence number and must be a non-negative integer (anything else
   * throws `RangeError` from `publish`, before anything is mutated);
   * subscribers require strictly increasing epochs and discard
   * out-of-order barriers (audited as `barrier_dropped`).
   *
   * A barrier publish is control-plane: it consumes no per-topic
   * sequence number (subscribers see no gap), is never written to the
   * durable log (replay restores data, not live coordination cuts),
   * never becomes a retained message (`retain` is ignored), skips schema
   * validation / compression / signing, and never crosses the bridge or
   * the cluster link. It still passes the ACL — an unauthorized
   * publisher cannot inject cut markers. The barrier is delivered to
   * each opted-in subscriber as a control message (`msg.barrier.epoch`,
   * `seq` 0) with `payload` carried verbatim as the cut metadata;
   * subscribers that did not opt in never see it.
   *
   * Only `publish` honors it; `publishIdempotent`, batch, atomic and
   * delayed publishes ignore the flag.
   */
  barrier?: { epoch: number };
}

/**
 * One message in a `publishBatch` / `publishAtomic` batch: topic and
 * payload, with the bus assigning `seq` at fan-out. `key` opts the message
 * into durable-log keyed compaction and per-key publish-order delivery
 * (see `PublishOptions.key`); `traceparent` continues an upstream trace
 * (see `PublishOptions.traceparent`).
 */
export type BatchMessage = Omit<BusMessage, 'seq'> & {
  key?: string;
  traceparent?: string;
  /**
   * Consistent-hash partition selector (see `PublishOptions.partitionKey`):
   * validated per entry at the publish entry point (`publishBatch`,
   * `publishAtomic`). The entry's `partition` field (inherited from
   * `BusMessage`) carries an explicit partition request and wins over the
   * key, exactly like `PublishOptions.partition`.
   */
  partitionKey?: string;
  /**
   * Schema version for versioned topic validators (see
   * `PublishOptions.schemaVersion`): validated per entry at the publish
   * entry point (`publishBatch`, `publishAtomic`).
   */
  schemaVersion?: number;
};

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
  /**
   * Publisher identity, carried on the schedule record and stamped onto
   * the envelope when the schedule fans out (see
   * `PublishOptions.publisherId`): a delayed publish keeps its producer
   * attribution across restarts. Same validation as `publish`.
   */
  publisherId?: string;
  /**
   * Schema version for versioned topic validators (see
   * `PublishOptions.schemaVersion`): validated fail-fast at schedule
   * time, not re-run at fan-out. Same validation as `publish`.
   */
  schemaVersion?: number;
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
   * `serializeToJson`) contribute nothing to the corpus. Binary payloads
   * (EB-92) are skipped: the corpus is a newline-joined UTF-8 text, and
   * training a deflate dictionary from mis-encoded bytes would produce
   * a useless dictionary — binary topics should use an explicit
   * dictionary via `setTopicCompression` instead.
   */
  sample(payload: unknown, binary = false): void {
    if (binary) return;
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
 * Estimates a payload's buffered byte size — the estimator the bus hands
 * to byte-budgeted backpressure queues (see
 * `SubscribeOptions.queueMaxBytes`) and to the max-message-bytes
 * admission cap (EB-64). Binary payloads (EB-92, `Uint8Array`) measure
 * as their wire bytes; everything else measures as JSON UTF-8 bytes.
 * Unserializable payloads estimate as 0 (see `serializeToJson`): the
 * byte budget is a JSON-shape measure, and pretending an unknown size
 * is a real number would corrupt it.
 */
function payloadByteSize(payload: unknown): number {
  if (payload instanceof Uint8Array) return payload.byteLength;
  const serialized = serializeToJson(payload);
  return serialized === undefined ? 0 : Buffer.byteLength(serialized, 'utf8');
}

/**
 * The canonical wire bytes of a payload (EB-92): a binary message's
 * raw bytes (zero-copy view over the published buffer), or the UTF-8
 * JSON encoding for ordinary messages. Returns `undefined` for
 * payloads with no JSON encoding — exactly the `serializeToJson`
 * contract. Compression, HMAC signing and the durable-log v2 encoding
 * all hash/cover these same bytes, so "wire bytes" means one thing
 * everywhere.
 */
function wireBytesOfPayload(payload: unknown, binary: boolean): Buffer | undefined {
  if (binary) {
    return payload instanceof Uint8Array
      ? Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength)
      : undefined;
  }
  const serialized = serializeToJson(payload);
  return serialized === undefined ? undefined : Buffer.from(serialized, 'utf8');
}

/**
 * Validates `PublishOptions.binary` at the publish entry points:
 * `binary: true` requires a `Uint8Array` payload (`ArrayBuffer` is
 * wrapped in a zero-copy view); anything else throws `TypeError` before
 * anything is mutated. Returns the normalized payload bytes (or the
 * original payload when `binary` is not set).
 */
function normalizeBinaryPayload(payload: unknown, binary: boolean | undefined, where: string): unknown {
  if (binary !== true) return payload;
  if (payload instanceof Uint8Array) return payload;
  if (payload instanceof ArrayBuffer) return new Uint8Array(payload);
  throw new TypeError(`${where}: binary publish requires a Uint8Array (or ArrayBuffer) payload`);
}

/**
 * The placeholder written into every redacted sensitive field (see
 * `EventBus.setTopicRedaction`, EB-68). Exported so downstream consumers
 * and tests can match on it. A field that already holds the sentinel is
 * left untouched and uncounted — redaction is idempotent, so a payload
 * that crossed a route/bridge hop is never double-counted.
 */
export const REDACTION_SENTINEL = '***';

/**
 * Field-path segments that must never be traversed or written: honoring
 * them would mutate object prototypes instead of data. Rejected at
 * registration time (fail-fast `RangeError`).
 */
const FORBIDDEN_REDACTION_SEGMENTS = new Set(['__proto__', 'constructor', 'prototype']);

/**
 * Parses and validates one redaction field path (dot-separated object
 * path, e.g. `"user.email"`). Throws `RangeError` for empty or
 * non-string paths, empty segments, and prototype-polluting segments.
 */
function parseRedactionFieldPath(field: string, caller: string): string[] {
  if (typeof field !== 'string' || field.length === 0) {
    throw new RangeError(`${caller}: redaction field paths must be non-empty strings`);
  }
  const segments = field.split('.');
  for (const segment of segments) {
    if (segment.length === 0) {
      throw new RangeError(`${caller}: redaction field path "${field}" contains an empty segment`);
    }
    if (FORBIDDEN_REDACTION_SEGMENTS.has(segment)) {
      throw new RangeError(
        `${caller}: redaction field path "${field}" uses the forbidden segment "${segment}"`,
      );
    }
  }
  return segments;
}

/**
 * Replaces one field path inside an object with the redaction sentinel,
 * cloning on the write path only. Returns the (possibly new) node and
 * whether a replacement happened. Arrays are opaque — their elements are
 * never indexed — and a missing intermediate or leaf is a no-op. The
 * input object is never mutated.
 */
function redactPath(
  node: unknown,
  segments: string[],
  index: number,
): { node: unknown; changed: boolean } {
  if (node === null || typeof node !== 'object' || Array.isArray(node)) {
    return { node, changed: false };
  }
  const record = node as Record<string, unknown>;
  if (index === segments.length - 1) {
    const key = segments[index];
    if (!Object.prototype.hasOwnProperty.call(record, key)) return { node, changed: false };
    // Idempotent: a field already holding the sentinel keeps it and is
    // not counted again.
    if (record[key] === REDACTION_SENTINEL) return { node, changed: false };
    const copy = { ...record };
    copy[key] = REDACTION_SENTINEL;
    return { node: copy, changed: true };
  }
  const descended = redactPath(record[segments[index]], segments, index + 1);
  if (!descended.changed) return { node, changed: false };
  const copy = { ...record };
  copy[segments[index]] = descended.node;
  return { node: copy, changed: true };
}

/**
 * Applies redaction field paths to a publish payload (EB-68). Only object
 * payloads are touched: a non-object payload is returned untouched with a
 * zero count, and array element paths are unsupported (arrays are opaque).
 * The input is never mutated — each replaced branch is cloned with
 * structural sharing, so untouched subtrees keep their identity. Returns
 * the (possibly new) payload and the number of fields actually replaced.
 */
function redactFieldPaths(
  payload: unknown,
  paths: string[][],
): { value: unknown; redacted: number } {
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    return { value: payload, redacted: 0 };
  }
  let value: unknown = payload;
  let redacted = 0;
  for (const segments of paths) {
    const result = redactPath(value, segments, 0);
    if (result.changed) {
      value = result.node;
      redacted += 1;
    }
  }
  return { value, redacted };
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
  /**
   * Binary-payload marker (EB-92): `true` when the record is a v2 log
   * line and `payload` is raw bytes (`Uint8Array`) — possibly encoding
   * a compression envelope for binary+compressed publishes.
   */
  binary?: boolean;
  key?: string;
  keySeq?: number;
  dictId?: string;
  messageId?: string;
  eventTime?: number;
  causal?: { source: string; clock: number };
  /**
   * Publish-side partition logged at publish time (EB-90); absent on
   * records written before partition routing (replay treats those as
   * partition 0).
   */
  partition?: number;
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

/**
 * One entry of a `subscribeMany` batch (EB-100): an independent
 * subscription.
 *
 * Contrast with composite patterns (EB-70, `SubscribeOptions.patterns`):
 * a composite is a *single* subscription matching several patterns with
 * OR/AND/exclusion semantics and one handler; a `subscribeMany` entry is
 * a *separate* subscription with its own handler, queue, backpressure
 * policy and lifecycle — the batch is only an atomic registration
 * mechanism.
 */
export interface SubscribeManyEntry {
  /** Topic pattern, same syntax as `subscribe`. */
  pattern: string;
  /** Handler for this entry's subscription (batch handlers allowed). */
  handler: MessageHandler | BatchMessageHandler;
  /**
   * Per-entry options, shallow-merged over `subscribeMany`'s
   * `commonOpts` (entry wins). `group` is batch-local: when set, the
   * entry joins that consumer group (EB-16) via `subscribeToGroup`
   * instead of a plain `subscribe`. Shadow (EB-63) and namespace (EB-52)
   * options behave exactly as in `subscribe`.
   */
  opts?: SubscribeOptions & { group?: string };
}

export class EventBus {
  private subscribers = new Map<string, Subscriber>();
  private subscribersByPattern = new Map<string, number>();
  /**
   * Outstanding subscription migrations (EB-82), keyed by handoff token:
   * one entry per `exportSubscription` whose `completeMigration` or
   * `cancelMigration` has not run yet.
   */
  private readonly migrations = new Map<string, MigrationRecord>();
  /**
   * Handoff tokens already imported by THIS instance (EB-82). A token is
   * single-use per instance, so one snapshot can never double-subscribe
   * the target, however many times it is presented.
   */
  private readonly importedHandoffTokens = new Set<string>();
  /**
   * Compiled topic patterns, keyed by the exact pattern string. Each distinct
   * pattern is compiled once and the RegExp is shared by every subscriber on
   * it; the entry is evicted when its last subscriber unsubscribes so the
   * cache cannot grow without bound.
   */
  private patternCache = new Map<string, RegExp>();
  /**
   * Dedicated compiled-pattern cache for composite subscriptions (see
   * `SubscribeOptions.patterns`, EB-70): one entry per distinct
   * include/exclude pattern with at least one active composite
   * subscriber, shared across those subscribers like `patternCache`.
   * Deliberately separate from `patternCache` — composite patterns never
   * inflate `getStats().patternCacheSize`, whose semantics stay
   * single-pattern-only. Entries are reference-counted
   * (`compositePatternRefs`) and evicted when their last composite
   * subscriber leaves, mirroring the `patternCache` lifecycle.
   */
  private compositePatternCache = new Map<string, RegExp>();
  private compositePatternRefs = new Map<string, number>();
  /** Live subscriptions with `SubscribeOptions.patterns` (EB-70). */
  private compositeSubscriptionCount = 0;
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
      publisherRateLimited: number;
      pausedMessages: number;
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
  /**
   * Per-publisher admitted-publish totals (see `PublishOptions.publisherId`).
   * Only publishes that clear admission count; a schema rejection or
   * rate-limit shed never reaches the counting line. Anonymous publishes
   * move no per-publisher counter — the total is in `totalPublished`.
   */
  private readonly publisherCounts = new Map<string, number>();
  private totalExpired = 0;
  /**
   * Subscription leases that expired and auto-unsubscribed their
   * subscriber (see `src/lease.ts`, EB-69). Surfaced as
   * `getStats().expiredLeases`.
   */
  private totalExpiredLeases = 0;
  /**
   * Active subscription-lease timers (see `src/lease.ts`, EB-69), keyed
   * by subscription id. Armed at subscribe time, cleared from every
   * unsubscribe path, and renewed via `renewLease`.
   */
  private readonly leases: LeaseManager;
  /** Observer hook for expired leases (`EventBusOptions.onLeaseExpired`). */
  private readonly onLeaseExpired?: LeaseExpiredCallback;
  /**
   * Idle windows that expired and auto-unsubscribed their subscriber
   * (see `src/idle.ts`, EB-72). Surfaced as `getStats().idleExpired`.
   */
  private totalIdleExpired = 0;
  /** Completed non-empty tumbling windows (see `SubscribeOptions.window`, EB-73). */
  private totalAggregatedWindows = 0;
  /**
   * Active idle-reclamation timers (see `src/idle.ts`, EB-72), keyed by
   * subscription id. Armed at subscribe time, re-armed on qualifying
   * activity, cleared from every unsubscribe path.
   */
  private readonly idleReclaimer: IdleReclaimer;
  /** Observer hook for idle-expired subscriptions (`EventBusOptions.onIdleExpired`). */
  private readonly onIdleExpired?: IdleExpiredCallback;
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
   * Messages refused by saturated subscribers under the `'block'`
   * backpressure tier (global total, EB-83) — the backlog-preserving
   * alternative to shedding. Included in `totalDropped`; tracked
   * separately for backpressure forensics.
   */
  private totalBlocked = 0;
  /**
   * Messages handed to shadow-subscriber handlers (global total). A
   * shadow handler that throws still received the message, and still
   * counts — mirroring `totalDelivered`'s rule. Never mixed into
   * `totalDelivered`: shadow traffic is observability plumbing, not
   * production fan-out.
   */
  private totalShadowed = 0;
  /**
   * Messages shed by shadow subscribers' independent backpressure queues
   * (global total). Never mixed into `totalDropped`.
   */
  private totalShadowDropped = 0;
  /**
   * Messages shed at the publish side by adaptive throttling (global
   * total). Incremented next to the per-subscriber `throttledDrops` (see
   * `throttledCount`); counted separately from backpressure queue drops.
   */
  private totalThrottled = 0;
  private totalSequenceGaps = 0;
  /**
   * Per-(topic, partition) sequence counters (EB-90 — the EB-13 per-topic
   * seq, extended). Keyed by `partitionSeqKey(topic, partition)`: each
   * message published to a (topic, partition) pair takes the next number,
   * so every message carries a partition-scoped monotonic `seq` starting
   * at 1. A topic with the default single partition keeps the historical
   * behavior exactly (every message lands on partition 0). Grows with the
   * distinct (topic, partition) pairs ever published to — the same bound
   * as `partitionPublished`.
   */
  private topicSeq = new Map<string, number>();

  /**
   * Key for `topicSeq`: the per-(topic, partition) sequence counter
   * identity. The NUL separator keeps the decimal partition suffix
   * unambiguous against the topic name.
   */
  private static partitionSeqKey(topic: string, partition: number): string {
    return `${topic}\0${partition}`;
  }
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
   * Barrier control messages published (EB-98) — fanned out to
   * barrier-opted-in subscribers. Monotonic. Barriers are control-plane:
   * they never touch `totalPublished` or any per-topic seq counter.
   */
  private totalBarriersPublished = 0;
  /**
   * Out-of-order barriers discarded (EB-98, epoch at or below the
   * subscriber's highest seen) — the observable count of monotonicity
   * violations, each recorded in the subscription audit log as
   * `barrier_dropped`. Monotonic.
   */
  private totalBarriersDropped = 0;
  /**
   * Messages released from barrier holds (EB-98) — re-injected through
   * the pipeline when their barrier drained (or on a fail-open path).
   * Monotonic.
   */
  private totalBarrierReleased = 0;
  /**
   * Barrier holds abandoned by fail-open overflow (EB-98): the hold
   * buffer hit `maxHeldMessages`, so everything held was released at
   * once and the barrier disarmed. Monotonic.
   */
  private totalBarrierOverflows = 0;
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
   * MQTT-style retained-message rules (EB-87), keyed by the exact string
   * passed to `setTopicRetained` — either a concrete topic name or a
   * wildcard pattern. `true` enables retained publishes for matching
   * topics, `false` disables them (overriding a broader `true` pattern).
   * Match resolution mirrors the TTL rules: an exact-topic rule wins over
   * any pattern; between patterns the earliest-registered rule wins. No
   * rule means retained publishes are ignored (the publish itself is an
   * ordinary publish).
   */
  private retainedRules = new Map<string, boolean>();
  /**
   * Compiled matchers for the wildcard entries in `retainedRules`, kept
   * apart from the subscriber pattern cache and the other rule caches so
   * retained configuration never inflates `patternCacheSize`.
   */
  private retainedMatcherCache = new Map<string, RegExp>();
  /**
   * Publish-side partition counts (EB-90), keyed by the exact string
   * passed to `setTopicPartitions` — either a concrete topic name or a
   * wildcard pattern. Insertion order is the tie-break order when several
   * rules match a topic. Match resolution mirrors `setTopicTtl`: an
   * exact-topic rule wins over any pattern; between patterns the
   * earliest-registered rule wins.
   */
  private partitionRules = new Map<string, number>();
  /**
   * Compiled matchers for the wildcard entries in `partitionRules`, kept
   * apart from the subscriber pattern cache and the other rule caches so
   * partition configuration never inflates `patternCacheSize`.
   */
  private partitionMatcherCache = new Map<string, RegExp>();
  /**
   * Per-(topic, partition) publish counters (EB-90): admitted publishes
   * and the highest per-partition sequence number assigned. Keyed by
   * concrete topic, then partition. Grows with the distinct (topic,
   * partition) pairs ever published to — bounded by the partition counts
   * in force. A publish-side shed consumes its partition's sequence
   * number, so it counts here exactly like it counts in the topic total.
   */
  private partitionPublished = new Map<string, Map<number, { publishedMessages: number; lastSeq: number }>>();
  /**
   * The live retained-message table (EB-87): the latest retained message
   * per concrete (alias-resolved) topic. Rebuilt from the durable log on
   * restart (seq-0 retained records + tombstones).
   */
  private retainedMessages = new Map<string, RetainedEntry>();
  /**
   * Set while `subscribeReliable` runs its inner `subscribe`: the
   * retained snapshot is deferred until the at-least-once wrapper is
   * installed, so the snapshot is delivered as tracked deliveries through
   * the ready wrapper instead of crashing on the not-yet-assigned one.
   */
  private retainedSnapshotDeferred = false;
  /**
   * Per-topic HMAC signing rules (EB-84), keyed by the exact string passed
   * to `setTopicSigningKey` — either a concrete topic name or a wildcard
   * pattern. Match resolution mirrors the TTL rules: an exact-topic rule
   * wins over any pattern; between patterns the earliest-registered rule
   * wins. `keys[0]` is the live signing key; later keys are retired keys
   * kept for their grace window (`retiredAtMs`, stamped at rotation).
   */
  private signingRules = new Map<string, { keys: StoredSigningKey[]; graceMs: number }>();
  /**
   * Per-publisher HMAC signing keys (EB-97), keyed by exact publisher id
   * (validated like `PublishOptions.publisherId` — no patterns: an
   * identity is a concrete string). `keys[0]` is the live signing key;
   * later keys are retired keys kept for their grace window
   * (`retiredAtMs`, stamped at rotation), the WR-20/EB-84 convention.
   */
  private publisherSigningKeys = new Map<string, { keys: StoredSigningKey[]; graceMs: number }>();
  /**
   * Compiled matchers for the wildcard entries in `signingRules`, kept
   * apart from the subscriber pattern cache and the other rule caches so
   * signing configuration never inflates `patternCacheSize`.
   */
  private signingMatcherCache = new Map<string, RegExp>();
  /**
   * Messages discarded by `verifySignature` subscribers because the
   * envelope's HMAC signature was missing or invalid (global total).
   */
  private totalSignatureFailures = 0;
  /**
   * Per-topic publish rate limits, keyed by the exact string passed to
   * `setTopicRateLimit` — either a concrete topic name or a wildcard
   * pattern. Match resolution is identical to the TTL rules: an exact-topic
   * rule wins over any pattern; between patterns the earliest-registered
   * rule wins.
   */
  private rateLimitRules = new Map<string, { messagesPerSec: number; burst: number }>();
  /**
   * Per-topic subscriber caps (EB-81), keyed by the exact string passed
   * to `setTopicMaxSubscribers` — either a concrete topic name or a
   * wildcard pattern. Match resolution mirrors `setTopicTtl`: an
   * exact-topic rule wins over any pattern; between patterns the
   * earliest-registered rule wins. Counted per registering subscription
   * (group members and shadow mirrors each count one) against the
   * subscription's live pattern.
   */
  private subscriberLimitRules = new Map<string, number>();
  /**
   * Compiled matchers for the wildcard entries in `subscriberLimitRules`,
   * kept apart from the subscriber pattern cache and the other rule
   * caches so limit configuration never inflates `patternCacheSize`.
   */
  private subscriberLimitMatcherCache = new Map<string, RegExp>();
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
   * Per-publisher publish rate limits (EB-79), keyed by the cleaned
   * publisher identity (`cleanPublisherId`). Unlike the per-topic rules
   * there are no wildcards: a publisher either has a configured budget or
   * is unlimited.
   */
  private publisherRateLimitRules = new Map<string, { messagesPerSec: number; burst: number }>();
  /**
   * Token buckets per publisher identity, created lazily on the first
   * publish carrying that identity that reaches the gate and refilled
   * from the injected clock (`EventBusOptions.now`), so publisher rate
   * limiting is deterministic in tests. Re-setting a publisher's rule
   * resets only that publisher's bucket — other publishers keep their
   * remaining budget.
   */
  private publisherRateLimitBuckets = new Map<string, TokenBucket>();
  /**
   * Messages shed by per-publisher publish-side rate limiting (global
   * total). A shed happens in the admission phase — before sequence-number
   * assignment — so unlike a topic rate-limit shed it consumes no
   * sequence number and subscribers see no gap.
   */
  private totalPublisherRateLimited = 0;
  /**
   * Active topic-pause rules (EB-88), keyed by the exact string passed to
   * `pauseTopic` — either a concrete topic name or a wildcard pattern.
   * Match resolution mirrors the schema rules: an exact-topic rule wins
   * over any pattern; between patterns the earliest-registered rule wins
   * (the `Map` keeps registration order). Queue-mode rules additionally
   * hold their buffered messages in FIFO publish order.
   */
  private pauseRules = new Map<string, { mode: TopicPauseMode; buffer: PauseBufferEntry[] }>();
  /**
   * Compiled matchers for the wildcard entries in `pauseRules`, kept
   * apart from the subscriber pattern cache so pause configuration never
   * inflates `patternCacheSize`.
   */
  private pauseMatcherCache = new Map<string, RegExp>();
  /**
   * Publishes shed by topic pauses in shed mode (global total). The shed
   * consumes a sequence number (gap-visible, like the EB-19 rate-limit
   * shed) — unlike the pre-admission rejections counted in
   * `totalRejected`.
   */
  private totalPaused = 0;
  /** Counter for pause-buffer message ids (`paused-1`, …). */
  private nextPauseId = 0;
  /**
   * Global FIFO ordinal for pause-buffered messages: resume fans out in
   * `pauseOrder`, preserving the original publish order across concrete
   * topics under one pattern rule.
   */
  private nextPauseOrder = 0;
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
   * Versioned schema validators (see `setTopicSchemaVersion`), keyed by
   * the exact string passed at registration — either a concrete topic
   * name or a wildcard pattern — to that key's version map. Match
   * resolution mirrors `schemaRules`: an exact-topic rule set wins over
   * any pattern; between patterns the earliest-registered rule set wins.
   * When a topic resolves to a versioned rule set it takes precedence
   * over the legacy `schemaRules` validator; topics with no versioned
   * registrations keep the legacy behavior untouched.
   */
  private schemaVersionRules = new Map<string, Map<number, SchemaVersionRegistration>>();
  /**
   * Compiled matchers for the wildcard entries in `schemaVersionRules`,
   * kept apart from the other caches for the same reason.
   */
  private schemaVersionMatcherCache = new Map<string, RegExp>();
  /**
   * Per-topic maximum single-message size caps (UTF-8 JSON bytes), keyed by
   * the exact string passed to `setTopicMaxMessageBytes` — either a
   * concrete topic name or a wildcard pattern. Match resolution mirrors the
   * schema rules: an exact-topic rule wins over any pattern; between
   * patterns the earliest-registered rule wins.
   */
  private maxMessageBytesRules = new Map<string, number>();
  /**
   * Compiled matchers for the wildcard entries in `maxMessageBytesRules`,
   * kept apart from the subscriber pattern cache so size-cap configuration
   * never inflates `patternCacheSize`.
   */
  private maxMessageBytesMatcherCache = new Map<string, RegExp>();
  /**
   * Per-topic sensitive-field redaction rules (EB-68), keyed by the exact
   * string passed to `setTopicRedaction` — either a concrete topic name or
   * a wildcard pattern. Each rule is the list of parsed dot-paths whose
   * values are replaced with the redaction sentinel at admission, before
   * schema validation. Match resolution mirrors the schema rules: an
   * exact-topic rule wins over any pattern; between patterns the
   * earliest-registered rule wins.
   */
  private redactionRules = new Map<string, string[][]>();
  /**
   * Compiled matchers for the wildcard entries in `redactionRules`, kept
   * apart from the subscriber pattern cache so redaction configuration
   * never inflates `patternCacheSize`.
   */
  private redactionMatcherCache = new Map<string, RegExp>();
  /**
   * Total sensitive fields replaced by redaction across all topics (global
   * total). Counted at admission: a field replaced on a publish that a
   * later gate then rejects still counts — the replacement happened.
   */
  private totalRedactedFields = 0;
  /**
   * Messages rejected by schema validation (global total). A rejection
   * happens before admission: the payload never becomes a message, so it
   * consumes no sequence number, is never written to the durable log, and
   * never reaches a subscriber queue.
   */
  private totalRejected = 0;
  /**
   * Global total of publishes admitted under a superseded-but-graced
   * schema version (EB-86) — the sum of every topic's
   * `deprecatedSchemaVersions`.
   */
  private totalDeprecatedSchemaVersions = 0;
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
   * Cumulative-ack calls (`ackUpTo`, EB-76): one per call on a known
   * reliable subscriber, regardless of how many deliveries the range
   * actually acked.
   */
  private totalAckedRanges = 0;
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
   * Messages carrying a binary (raw-bytes) payload (EB-92). Registered
   * by the publish path when `PublishOptions.binary` is set, and by
   * replay when the log record is a v2 line. Bus-side, like
   * `compressedPayloads` — a user's payload can never collide with it.
   * Drives every binary-aware branch downstream: byte-size measurement,
   * compression input, signature wire bytes, durable-log v2 encoding,
   * and delivery-time inflation (bytes stay bytes, never `JSON.parse`d).
   * Entries die with their message (WeakSet).
   */
  private binaryMessages = new WeakSet<BusMessage>();
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
   * Attached outbox relay (EB-102), present only after
   * `attachOutboxRelay`. Its lag feeds `getStats()` (`outboxPending` /
   * `outboxLagMs` / `outboxCursor` / `outboxRelayed`) and the Prometheus
   * exposition.
   */
  private outboxRelay?: OutboxRelay;
  /**
   * `durableLogMaxEntriesPerTopic` as configured — namespace child logs
   * (EB-52) open with the same per-topic budget as the root log.
   */
  private readonly durableLogMaxEntriesPerTopic?: number;
  /**
   * `durableLogMaxAgeMs` as configured — namespace child logs (EB-52)
   * open with the same time-based retention bound as the root log; kept
   * in sync by `setDurableLogMaxAgeMs`.
   */
  private durableLogMaxAgeMs?: number;
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
   * `durableLogEncryptor` as configured — namespace child logs (EB-52)
   * seal with the same encryptor as the root log.
   */
  private readonly durableLogEncryptor?: PayloadEncryptor;
  /**
   * Subscription lifecycle audit log (EB-78): always present, always
   * records into its bounded in-memory index; persists to daily JSONL
   * files only when `EventBusOptions.subscriptionAuditDir` was given.
   * Every audit site funnels through `auditSubscription`, which is
   * error-isolated so a broken log can never disturb the subscribe path.
   */
  private readonly subAudit: SubscriptionAuditLog;
  /**
   * Topic metadata registry (EB-77): the operator-side catalog of known
   * topics. Advisory only — publishing to an unregistered topic is
   * counted and surfaced on `onUnregisteredTopic`, never blocked.
   */
  private readonly topicRegistry = new TopicRegistry();
  /**
   * Unregistered-topic publish hook (`EventBusOptions.onUnregisteredTopic`),
   * or `undefined` when unset — the bus is then exactly as before (stats
   * counting only).
   */
  private readonly onUnregisteredTopic: UnregisteredTopicCallback | undefined;
  /**
   * Total publishes to topics with no live registration
   * (`getStats().unregisteredTopicPublishes`). Counted with or without
   * the hook, so the counter and the hook events reconcile exactly.
   */
  private unregisteredTopicPublishes = 0;
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
    const onUnregisteredTopic = options?.onUnregisteredTopic;
    if (onUnregisteredTopic !== undefined && typeof onUnregisteredTopic !== 'function') {
      throw new RangeError('onUnregisteredTopic must be a function');
    }
    this.onUnregisteredTopic = onUnregisteredTopic;
    const onLeaseExpired = options?.onLeaseExpired;
    if (onLeaseExpired !== undefined && typeof onLeaseExpired !== 'function') {
      throw new RangeError('onLeaseExpired must be a function');
    }
    this.onLeaseExpired = onLeaseExpired;
    this.leases = new LeaseManager((expired) => this.handleLeaseExpiry(expired));
    const onIdleExpired = options?.onIdleExpired;
    if (onIdleExpired !== undefined && typeof onIdleExpired !== 'function') {
      throw new RangeError('onIdleExpired must be a function');
    }
    this.onIdleExpired = onIdleExpired;
    this.idleReclaimer = new IdleReclaimer((expired) => this.handleIdleExpiry(expired));
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
    if (options?.durableLogDir == null && options?.durableLogEncryptor !== undefined) {
      throw new RangeError('durableLogEncryptor requires EventBusOptions.durableLogDir to be set');
    }
    if (options?.durableLogDir == null && options?.durableLogMaxAgeMs !== undefined) {
      throw new RangeError('durableLogMaxAgeMs requires EventBusOptions.durableLogDir to be set');
    }
    if (options?.durableLogDir == null && options?.durableLogStorageBackend !== undefined) {
      throw new RangeError('durableLogStorageBackend requires EventBusOptions.durableLogDir to be set');
    }
    if (options?.durableLogDir != null) {
      const log = DurableTopicLog.open({
        dir: options.durableLogDir,
        // EB-96: pluggable storage backend; the default is the local
        // filesystem rooted at `durableLogDir`.
        backend: options.durableLogStorageBackend,
        maxEntriesPerTopic: options.durableLogMaxEntriesPerTopic,
        // Time-based retention runs on the bus clock, so retention stays
        // deterministic under an injected `EventBusOptions.now` (EB-85).
        maxAgeMs: options.durableLogMaxAgeMs,
        now: this.now,
        keyCompaction: options.durableLogKeyCompaction,
        segmentRotation: options.durableLogSegmentRotation,
        encryptor: options.durableLogEncryptor,
        // The publish-dedup journal's size guard tracks the publish-side
        // idempotency window it persists (EB-66).
        publishDedupMaxEntries: this.idempotencyMaxEntries,
        // EB-94: a checksum-failed archived segment is isolated by the
        // log and reported here — one audit event per segment, so an
        // operator can see exactly which history was quarantined.
        // Construction-time recoveries run before the audit log exists;
        // those segments are still counted in stats().corruptSegments.
        onSegmentChecksumFailed: (info) =>
          this.auditSubscription({
            action: 'segment_checksum_failed',
            actor: 'system',
            topic: info.topic,
            detail: { path: info.path, segmentId: info.segmentId },
          }),
      });
      this.durableLog = log;
      // Remembered for namespace child logs (EB-52): children open with
      // the same per-topic budget, compaction mode, rotation triggers,
      // retention bound and encryptor as the root log.
      this.durableLogMaxEntriesPerTopic = options.durableLogMaxEntriesPerTopic;
      this.durableLogMaxAgeMs = options.durableLogMaxAgeMs;
      this.durableLogKeyCompaction = options.durableLogKeyCompaction ?? false;
      this.durableLogSegmentRotation = options.durableLogSegmentRotation;
      this.durableLogEncryptor = options.durableLogEncryptor;
      // Reseed the publish-side idempotent-dedup window from the journal
      // (EB-66): a restarted bus keeps suppressing within-window retries.
      // Claims that aged out of the window are "unknown" — dropped here,
      // never rehydrated. Bounded to `idempotencyMaxEntries`: the journal
      // yields claims in claim order, so taking the tail keeps the most
      // recently claimed identities — the exact analog of the live
      // table's insertion-ordered eviction.
      {
        const nowMs = this.now();
        const live: Array<{ key: string; at: number }> = [];
        for (const entry of log.recoveredPublishDedup()) {
          if (nowMs - entry.at < this.idempotencyWindowMs) {
            live.push({ key: `${entry.topic}\0${entry.messageId}`, at: entry.at });
          }
        }
        for (const { key, at } of live.slice(-this.idempotencyMaxEntries)) {
          this.dedup.set(key, at);
        }
      }
      // Recover numbering continuity and per-key cursors from the log —
      // shared with namespace child logs (see `recoverLogState`).
      this.recoverLogState(log);
      // Rebuild pending delayed-delivery timers from the log: schedules
      // that never fanned out (and were not cancelled) survive the
      // restart. Already-due entries fan out immediately.
      this.recoverDelayed();
      // Rebuild queue-mode pause buffers from the log (EB-88): buffered
      // messages that never drained survive the restart and fan out
      // immediately — pause rules themselves are runtime state and do not
      // survive, so the new process starts unpaused.
      this.recoverPaused();
      // Rebuild the retained-message table from the log (EB-87): the
      // latest retained record per topic wins; tombstones and
      // TTL-expired entries rebuild nothing.
      this.recoverRetained();
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
    // Subscription lifecycle audit log (EB-78): always constructed — the
    // in-memory index records every change even without a disk directory.
    // Validated before construction, so a throw leaves no half-built bus.
    const subscriptionAuditDir = options?.subscriptionAuditDir;
    if (subscriptionAuditDir !== undefined && typeof subscriptionAuditDir !== 'string') {
      throw new RangeError('subscriptionAuditDir must be a non-empty string');
    }
    if (subscriptionAuditDir === '') {
      throw new RangeError('subscriptionAuditDir must be a non-empty string');
    }
    const subscriptionAuditMaxEntries = options?.subscriptionAuditMaxEntries;
    if (
      subscriptionAuditMaxEntries !== undefined &&
      (!Number.isInteger(subscriptionAuditMaxEntries) || subscriptionAuditMaxEntries < 1)
    ) {
      throw new RangeError('subscriptionAuditMaxEntries must be a positive integer');
    }
    this.subAudit = new SubscriptionAuditLog({
      dir: subscriptionAuditDir,
      maxEntries: subscriptionAuditMaxEntries,
      now: this.now,
    });
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
   * Sets how many publish-side partitions a topic has (EB-90, real-time
   * systems): the topic's publishes are spread over `n` partitions, each
   * with an independent sequence counter (the EB-13 per-topic seq,
   * extended). Producers opt into a partition per publish with
   * `PublishOptions.partitionKey` (consistent hash) or
   * `PublishOptions.partition` (explicit); without either, every publish
   * lands on partition 0 — so a topic whose count stays at the default 1
   * behaves exactly as before.
   *
   * `topicPattern` accepts the same wildcard syntax as `subscribe` (or
   * an exact topic name); the rule key is the exact string passed here.
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * any pattern; between patterns the earliest-registered rule wins
   * (independent matcher cache — never inflates `patternCacheSize`).
   * Re-setting a rule replaces it. Reducing the count does not renumber
   * history: per-partition sequence counters never reset, so a later
   * publish to a surviving partition continues its own numbering and a
   * restarted bus never reuses a number (see `recoverLogState`).
   *
   * Throws `RangeError` when `topicPattern` is empty or `n` is not a
   * positive integer. Audited as `partition_rule_set`.
   */
  setTopicPartitions(topicPattern: string, n: number): void {
    if (typeof topicPattern !== 'string' || topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (!Number.isInteger(n) || n < 1) {
      throw new RangeError('partition count must be a positive integer');
    }
    this.partitionRules.set(topicPattern, n);
    this.auditSubscription({
      action: 'partition_rule_set',
      actor: 'system',
      pattern: topicPattern,
      detail: { partitions: n },
    });
  }

  /**
   * Removes the partition-count rule previously registered for
   * `topicPattern`. Returns true when a rule existed and was removed.
   * Already-published sequence numbers are untouched — only future
   * publishes resolve against the remaining rules (default: 1 partition).
   * Audited as `partition_rule_cleared` when a rule existed.
   */
  clearTopicPartitions(topicPattern: string): boolean {
    const removed = this.partitionRules.delete(topicPattern);
    if (removed) {
      this.auditSubscription({
        action: 'partition_rule_cleared',
        actor: 'system',
        pattern: topicPattern,
      });
    }
    return removed;
  }

  /**
   * Returns the publish-side partition count that applies to `topic`,
   * or 1 when no rule matches. An exact-topic rule wins over patterns;
   * between patterns the earliest-registered matching rule wins.
   */
  private partitionsForTopic(topic: string): number {
    const exact = this.partitionRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, n] of this.partitionRules) {
      let matcher = this.partitionMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.partitionMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return n;
    }
    return 1;
  }

  /**
   * Resolves one publish's partition (EB-90): an explicit `partition`
   * wins over the `partitionKey` consistent hash
   * (`sha256(partitionKey) mod partitionCount`); with neither, the
   * publish lands on partition 0. The types were already validated by
   * the publish entry point (`validatePartitionOptions`); the range
   * check needs the alias-resolved topic, so it runs here — still before
   * anything is mutated — and throws `RangeError` when the explicit
   * partition is negative or `>=` the topic's partition count.
   */
  private resolvePartition(
    topic: string,
    partitionKey: string | undefined,
    partition: number | undefined,
  ): number {
    const count = this.partitionsForTopic(topic);
    if (partition !== undefined) {
      if (!Number.isInteger(partition) || partition < 0 || partition >= count) {
        throw new RangeError(
          `publish: partition ${partition} out of range for topic '${topic}' (${count} partition(s))`,
        );
      }
      return partition;
    }
    if (partitionKey !== undefined) {
      return Number(EventBus.hash64(partitionKey) % BigInt(count));
    }
    return 0;
  }

  /**
   * Seeds one (topic, partition) publish counter during log recovery
   * (EB-90). See `recoverLogState` for why the recovered sequence high
   * doubles as the recovered publish count.
   */
  private seedPartitionPublished(topic: string, partition: number, seq: number): void {
    let perTopic = this.partitionPublished.get(topic);
    if (perTopic == null) {
      perTopic = new Map();
      this.partitionPublished.set(topic, perTopic);
    }
    perTopic.set(partition, { publishedMessages: seq, lastSeq: seq });
  }

  /**
   * Counts one admitted publish against its (topic, partition) pair
   * (EB-90): a publish-side shed consumes its partition's sequence
   * number, so it counts here exactly like it counts in the topic total
   * — the per-partition rows in `getStats()` reconcile with
   * `TopicStats.publishedMessages`.
   */
  private countPartitionPublish(topic: string, partition: number, seq: number): void {
    let perTopic = this.partitionPublished.get(topic);
    if (perTopic == null) {
      perTopic = new Map();
      this.partitionPublished.set(topic, perTopic);
    }
    const row = perTopic.get(partition);
    if (row === undefined) {
      perTopic.set(partition, { publishedMessages: 1, lastSeq: seq });
    } else {
      row.publishedMessages += 1;
      if (seq > row.lastSeq) row.lastSeq = seq;
    }
  }

  /**
   * Opts topics matching `topicPattern` into (or out of) MQTT-style
   * retained messages (EB-87, real-time systems): a `publish` carrying
   * `retain: true` on a matching topic additionally stores the message
   * as that topic's retained message — the latest value a late-joining
   * subscriber receives as a snapshot at subscribe time, before any live
   * message. Publishing an empty payload with `retain: true` clears the
   * topic's retained message (see `PublishOptions.retain` for the exact
   * "empty" definition).
   *
   * `topicPattern` accepts the same wildcard syntax as `subscribe` (or
   * an exact topic name); the rule key is the exact string passed here.
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * any pattern; between patterns the earliest-registered rule wins
   * (independent matcher cache — never inflates `patternCacheSize`).
   * With no matching rule a `retain: true` publish is an ordinary
   * publish — the flag is silently ignored — so retained storage is
   * strictly opt-in. Re-setting a rule replaces it.
   *
   * Removing a rule does NOT delete already-stored retained messages:
   * it only stops future retained publishes from updating the table.
   * Retained state survives restarts via the durable log (seq-0
   * retained records + tombstones, like the delayed-delivery
   * convention) when `durableLogDir` is configured.
   *
   * Throws `RangeError` when `topicPattern` is empty or `enabled` is
   * not a boolean. Audited as `retained_rule_set`.
   */
  setTopicRetained(topicPattern: string, enabled: boolean): void {
    if (typeof topicPattern !== 'string' || topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (typeof enabled !== 'boolean') {
      throw new RangeError('enabled must be a boolean');
    }
    this.retainedRules.set(topicPattern, enabled);
    this.auditSubscription({
      action: 'retained_rule_set',
      actor: 'system',
      pattern: topicPattern,
      detail: { enabled },
    });
  }

  /**
   * Removes the retained-message rule previously registered for
   * `topicPattern`. Returns true when a rule existed and was removed.
   * Already-stored retained messages are left untouched — only future
   * retained publishes stop updating the table. Audited as
   * `retained_rule_cleared` when a rule existed.
   */
  clearTopicRetained(topicPattern: string): boolean {
    const removed = this.retainedRules.delete(topicPattern);
    if (removed) {
      this.auditSubscription({
        action: 'retained_rule_cleared',
        actor: 'system',
        pattern: topicPattern,
      });
    }
    return removed;
  }

  /**
   * Returns whether retained publishes are enabled for `topic`: the
   * resolved rule value, or `undefined` when no rule matches. An
   * exact-topic rule wins over patterns; between patterns the
   * earliest-registered matching rule wins.
   */
  private retainedForTopic(topic: string): boolean | undefined {
    const exact = this.retainedRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, enabled] of this.retainedRules) {
      let matcher = this.retainedMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.retainedMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return enabled;
    }
    return undefined;
  }

  /**
   * Enables publish-side HMAC-SHA256 message signing for topics matching
   * `topicPattern` (EB-84, backend security): every admitted publish to a
   * matching topic is signed over `(topic, seq, wire bytes)` — the wire
   * bytes are the post-compression form, so a compressed envelope's
   * signature covers the ciphertext — and the `{kid, sig}` rides the
   * `BusMessage` envelope, the durable log, and the bridge mirror.
   *
   * `topicPattern` accepts the same wildcard syntax as `subscribe` (or an
   * exact topic name). `config.keys` lists the signing keys newest-first:
   * index 0 is the live signing key; every later key is a retired key
   * that still verifies inside `graceMs` (default 0) from the moment it
   * stopped being newest — key rotation without a verification outage.
   * Re-setting a rule rotates: previously stored keys the new set drops
   * are retired as of now (already-retired keys keep their original
   * `retiredAtMs` — rotation is monotonic, re-setting never extends a
   * grace window), and keys whose grace window already fully elapsed are
   * pruned.
   *
   * A `verifySignature` subscriber checks the signature before any content
   * processing; a failed check drops the message, counts it in
   * `TopicStats.signatureFailures` (plus the global total), and files a
   * `signature_verification_failed` audit event. Fail-closed: a topic
   * with no signing rule, or a message carrying no signature, never
   * verifies.
   *
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * patterns, the earliest-registered matching pattern wins. Throws
   * `RangeError` when `topicPattern` is empty, when `keys` is empty, when
   * a `kid` is empty/duplicate/malformed, when a secret is empty, or when
   * `graceMs` is negative — fail-fast at configuration time, never
   * mid-publish.
   */
  setTopicSigningKey(topicPattern: string, config: TopicSigningKeysConfig): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    assertValidSigningKeys(config?.keys, `setTopicSigningKey("${topicPattern}")`);
    const graceMs = config.graceMs ?? 0;
    if (!Number.isFinite(graceMs) || graceMs < 0) {
      throw new RangeError('graceMs must be a non-negative finite number of milliseconds');
    }
    const nowMs = this.now();
    // Rotation stamping: the newest key (index 0) is the live signing
    // key. Every other listed key — and every previously stored key the
    // new set drops outright — is retired as of now, unless it was
    // already retired earlier (rotation is monotonic: re-setting never
    // extends a grace window). Keys whose grace window already fully
    // elapsed are pruned — they can never verify again.
    const previous = this.signingRules.get(topicPattern);
    const prevByKid = new Map((previous?.keys ?? []).map((k) => [k.kid, k]));
    const keys: StoredSigningKey[] = config.keys.map((input, index) => {
      const prev = prevByKid.get(input.kid);
      prevByKid.delete(input.kid);
      return {
        kid: input.kid,
        secret: normalizeSigningSecret(
          input.secret,
          `setTopicSigningKey("${topicPattern}"): keys[${index}]`,
        ),
        ...(index === 0 ? {} : { retiredAtMs: prev?.retiredAtMs ?? nowMs }),
      };
    });
    for (const old of prevByKid.values()) {
      keys.push({ kid: old.kid, secret: old.secret, retiredAtMs: old.retiredAtMs ?? nowMs });
    }
    this.signingRules.set(topicPattern, {
      keys: keys.filter((k) => k.retiredAtMs === undefined || nowMs - k.retiredAtMs <= graceMs),
      graceMs,
    });
  }

  /**
   * Removes the signing rule previously registered for `topicPattern`.
   * Returns true when a rule existed and was removed.
   */
  clearTopicSigningKey(topicPattern: string): boolean {
    return this.signingRules.delete(topicPattern);
  }

  /**
   * Registers (or rotates) the HMAC signing key set for one publisher
   * identity (EB-97, backend security). Every message published with
   * `PublishOptions.publisherId` equal to `publisherId` is then signed
   * with the set's newest key over
   * `(topic, seq, publisherId, wire bytes)` — orthogonal to the
   * per-topic signature (`setTopicSigningKey`): a message can carry
   * both, binding integrity and identity independently.
   *
   * The key-id alphabet, rotation stamping, and grace semantics are the
   * `setTopicSigningKey` convention verbatim: `keys[0]` is the live
   * signing key; re-setting rotates — previously stored keys the new
   * set drops are retired as of now (already-retired keys keep their
   * original `retiredAtMs`; re-setting never extends a grace window),
   * and keys whose grace window fully elapsed are pruned.
   *
   * A `verifyPublisher` subscriber checks the signature before any
   * content processing; a failed check drops the message, counts it in
   * `TopicStats.signatureFailures` (plus the global total, audit
   * `detail.kind: 'publisher'`), and files a
   * `signature_verification_failed` audit event. Fail-closed: a
   * publisher with no key set, or a message carrying no publisher
   * signature, never verifies.
   *
   * Redaction ordering: `setTopicRedaction` (EB-68) runs at admission,
   * BEFORE this signature is computed at fan-out — both signatures
   * cover the redacted wire bytes, so redaction never breaks
   * verification.
   *
   * Throws `RangeError` when `publisherId` is not a valid publisher
   * identity (1–64 ASCII alphanumeric/`_`/`-`), when `keys` is empty,
   * when a `kid` is empty/duplicate/malformed, when a secret is empty,
   * or when `graceMs` is negative — fail-fast at configuration time,
   * never mid-publish.
   */
  setPublisherSigningKey(publisherId: string, config: TopicSigningKeysConfig): void {
    const clean = cleanPublisherId(publisherId);
    if (clean === undefined) {
      throw new RangeError(
        'setPublisherSigningKey: publisherId must be 1-64 ASCII alphanumeric, \'_\' or \'-\' characters',
      );
    }
    assertValidSigningKeys(config?.keys, `setPublisherSigningKey("${clean}")`);
    const graceMs = config.graceMs ?? 0;
    if (!Number.isFinite(graceMs) || graceMs < 0) {
      throw new RangeError('graceMs must be a non-negative finite number of milliseconds');
    }
    const nowMs = this.now();
    // Rotation stamping mirrors `setTopicSigningKey` exactly: the
    // newest key (index 0) is live; every other listed key — and every
    // previously stored key the new set drops — is retired as of now
    // unless already retired earlier; fully-elapsed keys are pruned.
    const previous = this.publisherSigningKeys.get(clean);
    const prevByKid = new Map((previous?.keys ?? []).map((k) => [k.kid, k]));
    const keys: StoredSigningKey[] = config.keys.map((input, index) => {
      const prev = prevByKid.get(input.kid);
      prevByKid.delete(input.kid);
      return {
        kid: input.kid,
        secret: normalizeSigningSecret(
          input.secret,
          `setPublisherSigningKey("${clean}"): keys[${index}]`,
        ),
        ...(index === 0 ? {} : { retiredAtMs: prev?.retiredAtMs ?? nowMs }),
      };
    });
    for (const old of prevByKid.values()) {
      keys.push({ kid: old.kid, secret: old.secret, retiredAtMs: old.retiredAtMs ?? nowMs });
    }
    this.publisherSigningKeys.set(clean, {
      keys: keys.filter((k) => k.retiredAtMs === undefined || nowMs - k.retiredAtMs <= graceMs),
      graceMs,
    });
  }

  /**
   * Removes the signing key set previously registered for `publisherId`.
   * Returns true when a key set existed and was removed.
   */
  clearPublisherSigningKey(publisherId: string): boolean {
    const clean = cleanPublisherId(publisherId);
    if (clean === undefined) return false;
    return this.publisherSigningKeys.delete(clean);
  }

  /**
   * Returns the signing rule that applies to `topic`, or `undefined`
   * when no rule matches. An exact-topic rule wins over patterns; between
   * patterns the earliest-registered matching rule wins.
   */
  private signingForTopic(topic: string): { keys: StoredSigningKey[]; graceMs: number } | undefined {
    const exact = this.signingRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, rule] of this.signingRules) {
      let matcher = this.signingMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.signingMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return rule;
    }
    return undefined;
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
   * Caps how many live subscriptions may register under topics matching
   * `topicPattern` (EB-81) — a fail-fast guard against subscription
   * storms (a runaway service subscribing in a loop, a misconfigured
   * fan-out). Once the cap is reached, `subscribe` (and the
   * subscribe-family wrappers: `subscribeReliable`, `subscribeToGroup`,
   * shadow mirrors) throws `SubscriptionLimitError` before registering —
   * the caller sees the rejection instead of silently piling on.
   *
   * `topicPattern` accepts the same wildcard syntax as `subscribe` (or
   * an exact topic name). Match resolution mirrors `setTopicTtl`: an
   * exact-topic rule wins over patterns, the earliest-registered
   * matching pattern wins. The count is per subscription, keyed on the
   * subscription's live pattern as resolved to the same rule —
   * consumer-group members each count one, shadow mirrors count one.
   *
   * Lowering a cap never kicks existing subscribers (only new
   * registrations are rejected); clearing a cap lifts it entirely.
   * Throws `RangeError` when `topicPattern` is empty or
   * `maxSubscribers` is not a positive integer.
   */
  setTopicMaxSubscribers(topicPattern: string, maxSubscribers: number): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (!Number.isInteger(maxSubscribers) || maxSubscribers < 1) {
      throw new RangeError('maxSubscribers must be a positive integer');
    }
    this.subscriberLimitRules.set(topicPattern, maxSubscribers);
  }

  /**
   * Removes the subscriber cap previously registered for `topicPattern`.
   * Returns true when a rule existed and was removed.
   */
  clearTopicMaxSubscribers(topicPattern: string): boolean {
    return this.subscriberLimitRules.delete(topicPattern);
  }

  /**
   * The subscriber-cap rule key that applies to a subscription `pattern`,
   * or `undefined` when no rule matches. Mirrors `ttlForTopic`: an
   * exact-topic rule wins over patterns; between patterns the
   * earliest-registered matching rule wins.
   */
  private subscriberLimitRuleFor(pattern: string): string | undefined {
    if (this.subscriberLimitRules.has(pattern)) return pattern;
    for (const ruleKey of this.subscriberLimitRules.keys()) {
      let matcher = this.subscriberLimitMatcherCache.get(ruleKey);
      if (matcher == null) {
        matcher = compilePattern(ruleKey);
        this.subscriberLimitMatcherCache.set(ruleKey, matcher);
      }
      if (matcher.test(pattern)) return ruleKey;
    }
    return undefined;
  }

  /**
   * Enforces the subscriber cap for one registering subscription pattern
   * (EB-81). Runs after every other subscribe-time validation and before
   * anything registers — a throw leaves no half-registered subscriber
   * behind (in particular, before the composite refcounts are acquired).
   * Unsubscribing frees a slot automatically since the count is derived
   * from the live subscriber table.
   */
  private checkSubscriberLimit(pattern: string): void {
    const ruleKey = this.subscriberLimitRuleFor(pattern);
    if (ruleKey === undefined) return;
    const max = this.subscriberLimitRules.get(ruleKey) as number;
    let current = 0;
    for (const sub of this.subscribers.values()) {
      if (this.subscriberLimitRuleFor(sub.pattern) === ruleKey) current += 1;
    }
    if (current >= max) {
      throw new SubscriptionLimitError(pattern, max, current);
    }
  }

  /**
   * Caps how many messages per second one publisher may publish (EB-79),
   * enforced by a per-publisher token bucket at the publish side.
   * `publisherId` is a producer identity (`PublishOptions.publisherId` —
   * the same 1–64 ASCII alphanumeric/`_`/`-` shape as
   * `validatePublisherId`); an invalid identity throws `RangeError`
   * before any rule is touched. The first `burst` messages (default: one
   * second of budget) publish instantly; afterwards the bucket refills at
   * `messagesPerSec` per second. Publishes without a `publisherId` — and
   * publishers with no configured limit — are unaffected.
   *
   * A publish that finds the publisher's bucket empty is shed in the
   * admission phase, ahead of every other admission gate: it consumes no
   * sequence number (subscribers see no gap), is never fanned out, never
   * written to the durable log, never queued, and burns no topic
   * rate-limit budget; `publish` returns 0 for it. The shed is counted in
   * `TopicStats.publisherRateLimited` (and the global total) and
   * surfaced on the admission-rejection hook with reason
   * `'publisher-rate-limit'`. Re-setting a publisher's rule replaces it
   * and resets only that publisher's budget.
   *
   * `publishBatch` sheds over-budget entries one by one while the rest
   * still publish; `publishAtomic` checks the batch in shadow admission —
   * an over-budget entry aborts the whole batch with reason
   * `'publisher-rate-limit'`; `publishDelayed` fails fast at schedule
   * time; a suppressed idempotent duplicate never reaches the gate, so
   * retries of duplicates burn no publisher budget. Throws when
   * `publisherId` is invalid, when `messagesPerSec` is not a positive
   * finite number, or when `burst` is not a positive finite number.
   */
  setPublisherRateLimit(publisherId: string, opts: { messagesPerSec: number; burst?: number }): void {
    const clean = cleanPublisherId(publisherId);
    if (clean === undefined) {
      throw new RangeError(
        "setPublisherRateLimit: publisherId must be 1-64 ASCII alphanumeric, '_' or '-' characters",
      );
    }
    if (!Number.isFinite(opts.messagesPerSec) || opts.messagesPerSec <= 0) {
      throw new RangeError(
        'setPublisherRateLimit: messagesPerSec must be a positive finite number of messages per second',
      );
    }
    const burst = opts.burst ?? Math.max(1, Math.ceil(opts.messagesPerSec));
    if (!Number.isFinite(burst) || burst <= 0) {
      throw new RangeError('setPublisherRateLimit: burst must be a positive finite number of messages');
    }
    this.publisherRateLimitRules.set(clean, { messagesPerSec: opts.messagesPerSec, burst });
    // Reset only this publisher's budget: other publishers keep the
    // tokens they have left.
    this.publisherRateLimitBuckets.delete(clean);
  }

  /**
   * Removes the rate-limit rule previously registered for `publisherId`.
   * Returns true when a rule existed and was removed; an unknown (or
   * invalid — it could never have matched a rule) publisher id is a
   * no-op returning false.
   */
  clearPublisherRateLimit(publisherId: string): boolean {
    const clean = cleanPublisherId(publisherId);
    if (clean === undefined) return false;
    const removed = this.publisherRateLimitRules.delete(clean);
    if (removed) this.publisherRateLimitBuckets.delete(clean);
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
      undefined, // causal: the frame re-stamps nothing causal
      // A frame carrying an invalid identity forwards as anonymous — a
      // malformed label never fails the forward, it just drops the label.
      cleanPublisherId(frame.publisherId),
      undefined, // schemaVersion: the destination admission resolves the version
      undefined, // retain: forwards never retain
      undefined, // pausedExpiresAt
      undefined, // partitionKey
      undefined, // partition
      frame.binary ?? false,
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
  private countAliasRetired(topic: string, payload: unknown, publisherId?: string): void {
    this.statsFor(topic).aliasRetiredMessages += 1;
    this.totalAliasRetired += 1;
    this.emitAdmissionRejected(topic, 'alias-retired', payload, publisherId);
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
   * Changes the durable log's time-based retention bound at runtime
   * (EB-85): records older than `now - maxAgeMs` (bus clock) are evicted
   * at the next compact of each topic. Pass `undefined` to clear the age
   * bound and return to count-only retention. The change applies to the
   * root log and every namespace child log, and is recorded in the
   * subscription audit log as `retention_updated` (with the previous and
   * new bound in `detail`), so retention policy changes stay attributable.
   * Throws `RangeError` for a bad actor, for a non-positive-finite
   * `maxAgeMs`, or when the bus has no `durableLogDir`.
   */
  setDurableLogMaxAgeMs(maxAgeMs: number | undefined, actor = 'system'): void {
    assertValidActor(actor, 'setDurableLogMaxAgeMs');
    const log = this.durableLog;
    if (log == null) {
      throw new RangeError('setDurableLogMaxAgeMs requires EventBusOptions.durableLogDir to be set');
    }
    const previous = log.setMaxAgeMs(maxAgeMs);
    for (const child of this.namespaceLogs.values()) child.setMaxAgeMs(maxAgeMs);
    this.durableLogMaxAgeMs = maxAgeMs;
    this.auditSubscription({
      action: 'retention_updated',
      actor,
      detail: { from: previous ?? null, to: maxAgeMs ?? null },
    });
  }

  /**
   * Registers one versioned schema validator for `topicPattern` (EB-86):
   * a concrete topic name or a wildcard pattern, with the same match
   * resolution as `setTopicSchema` (exact-topic rule set wins over
   * patterns; earliest-registered matching pattern wins). `version` must
   * be a non-negative integer; registering the same version twice for
   * the same pattern throws `RangeError` — versions are never silently
   * replaced, re-register under a new version instead.
   *
   * A publish carrying `PublishOptions.schemaVersion` is validated
   * against that version's validator; without it the latest registered
   * version applies. When a strictly greater version registers, every
   * older version in the same rule set is marked superseded on the bus
   * clock and granted this registration's `graceMs` as its migration
   * window (`opts.graceMs`, default 0): inside the window (boundary
   * inclusive) the old version is still admitted (counted in
   * `deprecatedSchemaVersions`), past it — or for a version that was
   * never registered — the publish is rejected exactly like a schema
   * failure (counted in `rejectedMessages`, no sequence number
   * consumed). A throwing validator propagates to the publish caller,
   * exactly like the legacy `setTopicSchema` validator.
   *
   * Opt-in compatibility regression gate (EB-95): pass
   * `opts.compatibilityCorpus` — the sample set of payloads older versions
   * accepted — and the new validator is run against every sample at
   * registration time (with the registration `topicPattern` as the `topic`
   * argument). Any rejected or throwing sample throws `RangeError` and
   * registers nothing: the version never becomes visible and no existing
   * version is stamped superseded — zero side effects. A backfilled lower
   * version is gated identically to a new latest version.
   *
   * A topic that resolves to a versioned rule set ignores the legacy
   * `setTopicSchema` validator; topics with no versioned registrations
   * keep the legacy behavior. Throws `RangeError` when `topicPattern`
   * is empty, `version` is not a non-negative integer, `validator` is
   * not a function, `graceMs` is not a non-negative finite number, the
   * version is already registered for the pattern, `compatibilityCorpus`
   * is present but not an array, or the compatibility gate rejects (or
   * throws on) any corpus sample — in the gate-failure case nothing is
   * registered and no existing version is superseded.
   */
  setTopicSchemaVersion(
    topicPattern: string,
    version: number,
    validator: SchemaValidator,
    opts?: TopicSchemaVersionOptions,
  ): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (!Number.isInteger(version) || version < 0) {
      throw new RangeError('schema version must be a non-negative integer');
    }
    if (typeof validator !== 'function') {
      throw new RangeError('validator must be a function');
    }
    const graceMs = opts?.graceMs ?? 0;
    if (!Number.isFinite(graceMs) || graceMs < 0) {
      throw new RangeError('graceMs must be a non-negative finite number');
    }
    const corpus = opts?.compatibilityCorpus;
    if (corpus !== undefined && !Array.isArray(corpus)) {
      throw new RangeError('compatibilityCorpus must be an array of payloads');
    }
    const existing = this.schemaVersionRules.get(topicPattern);
    if (existing !== undefined && existing.has(version)) {
      throw new RangeError(
        `schema version ${version} is already registered for "${topicPattern}"`,
      );
    }
    // Compatibility regression gate (EB-95, opt-in): runs before any
    // registration state is touched, so a failing gate leaves the rule
    // set — including the `supersededAt` stamps of existing versions —
    // exactly as it was. The gate failure throws RangeError.
    const compat = this.checkSchemaCompatibility(topicPattern, version, validator, corpus);
    let versions = existing;
    if (versions === undefined) {
      versions = new Map();
      this.schemaVersionRules.set(topicPattern, versions);
    }
    const nowMs = this.now();
    const registration: SchemaVersionRegistration = {
      version,
      validator,
      registeredAt: nowMs,
      graceMs,
      compatGate: compat.gate,
      compatCorpusSize: compat.samples,
    };
    let isLatest = true;
    for (const [existingVersion, existing] of versions) {
      if (existingVersion > version) {
        // A backfilled lower version is born superseded: its migration
        // window starts now, it never becomes latest.
        isLatest = false;
      } else if (existingVersion < version && existing.supersededAt === undefined) {
        // This registration supersedes every older version in the set:
        // stamp the supersede moment and grant this registration's
        // `graceMs` as their migration window — the migration action
        // declares the window, so the grant always reflects the latest
        // operator intent.
        existing.supersededAt = nowMs;
        existing.graceMs = graceMs;
      }
    }
    if (!isLatest) registration.supersededAt = nowMs;
    versions.set(version, registration);
  }

  /**
   * Opt-in schema-compatibility regression gate (EB-95): runs the
   * registering `validator` against every payload in `corpus` — the sample
   * set of payloads older versions accepted — with `topicPattern` as the
   * `topic` argument. Any rejected sample or any throwing sample makes
   * this throw `RangeError` identifying the failing sample index. Without
   * a corpus the gate is skipped. Pure: touches no bus state, so the
   * caller can run it before mutating the registry to guarantee zero
   * side effects on failure.
   */
  private checkSchemaCompatibility(
    topicPattern: string,
    version: number,
    validator: SchemaValidator,
    corpus: unknown[] | undefined,
  ): { gate: 'passed' | 'not-tested'; samples: number } {
    if (corpus === undefined) return { gate: 'not-tested', samples: 0 };
    for (let i = 0; i < corpus.length; i++) {
      let accepted: boolean;
      try {
        accepted = validator(corpus[i], topicPattern);
      } catch {
        throw new RangeError(
          `schema version ${version} compatibility gate failed: validator threw on corpus sample ${i} for "${topicPattern}" — version is not registered`,
        );
      }
      if (!accepted) {
        throw new RangeError(
          `schema version ${version} compatibility gate failed: validator rejected corpus sample ${i} for "${topicPattern}" — version is not registered`,
        );
      }
    }
    return { gate: 'passed', samples: corpus.length };
  }

  /**
   * Removes versioned schema validators previously registered for
   * `topicPattern`: with `version`, removes just that version (the next
   * lower version does NOT become un-superseded — migration windows
   * never reopen); without it, removes the whole rule set, restoring the
   * legacy `setTopicSchema` behavior for matching topics. Returns true
   * when anything was removed.
   */
  clearTopicSchemaVersion(topicPattern: string, version?: number): boolean {
    const versions = this.schemaVersionRules.get(topicPattern);
    if (versions === undefined) return false;
    if (version === undefined) {
      this.schemaVersionRules.delete(topicPattern);
      this.schemaVersionMatcherCache.delete(topicPattern);
      return true;
    }
    const removed = versions.delete(version);
    if (versions.size === 0) {
      this.schemaVersionRules.delete(topicPattern);
      this.schemaVersionMatcherCache.delete(topicPattern);
    }
    return removed;
  }

  /**
   * Caps the JSON byte size of a single publish to topics matching
   * `topicPattern` — an admission gate for oversized payloads. `topicPattern`
   * accepts the same wildcard syntax as `subscribe` (or an exact topic
   * name); a publish whose payload serializes to more than `maxBytes` UTF-8
   * JSON bytes is rejected: it is never fanned out, never written to the
   * durable log, and never reaches a subscriber queue — `publish` returns 0
   * for it.
   *
   * A rejection happens before admission, so it consumes no sequence number
   * (subscribers see no gap), does not burn rate-limit budget, and is counted
   * in `TopicStats.rejectedMessages` (and the global total) — the same
   * rejection semantics as a schema rejection — surfaced on
   * `onAdmissionRejected` with reason `'message-too-big'`. The gate runs
   * after schema validation and before sequence-number assignment, so an
   * oversized publish cannot hide behind a validator pass or disturb the
   * rate limiter. Payloads with no JSON encoding (`undefined`, functions,
   * symbols, circular structures, BigInt) measure 0 bytes (see
   * `payloadByteSize`) and never trip the cap.
   *
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * patterns, the earliest-registered matching pattern wins. Re-setting a
   * rule replaces it. Throws `RangeError` when `topicPattern` is empty or
   * `maxBytes` is not a positive integer.
   */
  setTopicMaxMessageBytes(topicPattern: string, maxBytes: number): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (!Number.isInteger(maxBytes) || maxBytes <= 0) {
      throw new RangeError('maxBytes must be a positive integer number of bytes');
    }
    this.maxMessageBytesRules.set(topicPattern, maxBytes);
  }

  /**
   * Removes the per-topic message-size cap previously registered for
   * `topicPattern`. Returns true when a rule existed and was removed.
   */
  clearTopicMaxMessageBytes(topicPattern: string): boolean {
    return this.maxMessageBytesRules.delete(topicPattern);
  }

  /**
   * Pauses publishing to one topic or topic pattern (EB-88, real-time
   * systems / ops): while the rule is active, publishes to a matching
   * topic never reach subscribers.
   *
   * `topicPattern` accepts the same wildcard syntax as `subscribe` (or an
   * exact topic name); the rule key is the exact string passed here.
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * any pattern; between patterns the earliest-registered matching rule
   * wins (independent matcher cache — never inflates `patternCacheSize`).
   * Re-setting a rule replaces its mode; a queue-mode buffer already held
   * under the key is kept (it drains on resume).
   *
   * Modes (`TopicPauseOptions.mode`, default `'shed'`):
   * - `'shed'`: publishes are rejected outright — counted in
   *   `TopicStats.pausedMessages` / `BusStats.pausedMessages`, surfaced on
   *   `onAdmissionRejected` with reason `'topic-paused'`, `publish`
   *   returns 0, and the shed consumes a sequence number so subscribers
   *   observe the loss as a gap (the EB-19 rate-limit shed semantics).
   * - `'queue'`: publishes are held in a durable-side buffer instead —
   *   no sequence number consumed, no schema/rate-limit budget burned —
   *   and fan out in original order when the topic resumes, each with
   *   the TTL deadline stamped at pause admission (a message whose
   *   deadline passed while buffered is counted expired at resume, never
   *   resurrected — the EB-32 precedent). Queue mode requires
   *   `EventBusOptions.durableLogDir`: the buffer is persisted as seq-0
   *   pause records (tombstoned as each message drains), so a restart
   *   rebuilds the buffer instead of losing it.
   *
   * The pause check is the first gate in the publish admission pipeline —
   * before the per-publisher rate limiter (EB-79), so a paused shed never
   * burns publisher budget — and applies uniformly: `publish`,
   * `publishBatch` (per entry), `publishAtomic` (any paused entry rejects
   * the whole batch — a queued message cannot be committed synchronously,
   * so atomicity forces the batch to fail), `publishDelayed` (shed mode
   * fails fast at schedule time; queue mode accepts the schedule and
   * defers the due-time fan-out into the pause buffer while the pause is
   * still active), and `publishIdempotent` (the pause check runs before
   * the dedup claim, so a paused attempt never occupies a dedup slot).
   *
   * Audited as `topic_paused` (`{ mode }`). Throws `TypeError` when
   * `topicPattern` is not a string; throws `RangeError` when it is empty,
   * when `mode` is not `'shed'`/`'queue'`, or when queue mode is requested
   * without a durable log.
   */
  pauseTopic(topicPattern: string, opts?: TopicPauseOptions): void {
    if (typeof topicPattern !== 'string') {
      throw new TypeError('pauseTopic: topicPattern must be a string');
    }
    if (topicPattern.length === 0) {
      throw new RangeError('pauseTopic: topicPattern must be a non-empty string');
    }
    const mode = opts?.mode ?? 'shed';
    if (typeof mode !== 'string') {
      throw new TypeError("pauseTopic: mode must be 'shed' or 'queue'");
    }
    if (mode !== 'shed' && mode !== 'queue') {
      throw new RangeError("pauseTopic: mode must be 'shed' or 'queue'");
    }
    if (mode === 'queue' && this.durableLog == null) {
      throw new RangeError('pauseTopic: queue mode requires EventBusOptions.durableLogDir');
    }
    const existing = this.pauseRules.get(topicPattern);
    // Re-setting keeps a queue-mode buffer already held under the key:
    // the mode is ops intent, the buffered messages are data — replacing
    // the rule must not silently drop them. They drain on resume.
    this.pauseRules.set(topicPattern, { mode, buffer: existing?.buffer ?? [] });
    this.auditSubscription({
      action: 'topic_paused',
      actor: 'system',
      pattern: topicPattern,
      detail: { mode },
    });
  }

  /**
   * Resumes a paused topic or pattern: removes the rule registered by
   * `pauseTopic` and — for queue-mode rules — fans the buffered messages
   * out in original (publish) order through the full admission pipeline
   * (schema, rate limit, TTL), each keeping the TTL deadline stamped at
   * pause admission. A buffered message whose deadline passed while
   * queued is counted expired, never delivered. Buffered idempotent
   * messages re-run the dedup suppression gate, so a retry buffered
   * twice still delivers once.
   *
   * Returns true when a rule existed and was removed. Audited as
   * `topic_resumed` (`{ mode, drainedMessages }`). Never throws for an
   * unknown pattern — it is a no-op returning false.
   */
  resumeTopic(topicPattern: string): boolean {
    const rule = this.pauseRules.get(topicPattern);
    if (rule === undefined) return false;
    this.pauseRules.delete(topicPattern);
    const drained = this.drainPauseBuffer(rule);
    this.auditSubscription({
      action: 'topic_resumed',
      actor: 'system',
      pattern: topicPattern,
      detail: { mode: rule.mode, drainedMessages: drained },
    });
    return true;
  }

  /**
   * Returns the pause rule that applies to `topic`, or `undefined` when
   * the topic is not paused. Same precedence as `ttlForTopic`: an
   * exact-topic rule wins over patterns; between patterns the
   * earliest-registered matching rule wins.
   */
  private pauseForTopic(topic: string): { mode: TopicPauseMode; buffer: PauseBufferEntry[] } | undefined {
    const exact = this.pauseRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, rule] of this.pauseRules) {
      let matcher = this.pauseMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.pauseMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return rule;
    }
    return undefined;
  }

  /**
   * Registers per-topic sensitive-field redaction (EB-68): every publish
   * admitted to a matching topic has the listed field paths replaced with
   * `"***"` before anything else can observe the payload. `topicPattern`
   * accepts the same wildcard syntax as `subscribe` (or an exact topic
   * name); `fields` lists dot-separated object paths (e.g.
   * `"user.email"`, `"payment.card.number"`). Array element paths are not
   * supported — arrays are opaque to redaction — and non-object payloads
   * pass through untouched.
   *
   * Redaction runs at admission, before the schema validator (see
   * `setTopicSchema`): the schema sees the redacted payload, so a
   * validator cannot depend on a field that redaction masks. The size
   * cap, rate limiter, compression, the bridge mirror, the durable log,
   * and fan-out likewise all see only the redacted bytes — the original
   * values are never persisted and can never be resurrected by replay.
   * The caller's payload object is never mutated. Unset by default
   * (fully backward compatible).
   *
   * Match resolution mirrors `setTopicTtl`: an exact-topic rule wins over
   * patterns, the earliest-registered matching pattern wins. Re-setting a
   * rule replaces it. `clearTopicRedaction` removes a rule. Throws
   * `RangeError` when `topicPattern` is empty, `fields` is not an array,
   * or any field path is empty, not a string, has an empty segment, or
   * uses a forbidden segment (`__proto__`, `constructor`, `prototype`).
   */
  setTopicRedaction(topicPattern: string, fields: string[]): void {
    if (topicPattern.length === 0) {
      throw new RangeError('topicPattern must be a non-empty string');
    }
    if (!Array.isArray(fields)) {
      throw new RangeError('fields must be an array of field paths');
    }
    const parsed = fields.map((field) => parseRedactionFieldPath(field, 'setTopicRedaction'));
    this.redactionRules.set(topicPattern, parsed);
  }

  /**
   * Removes the sensitive-field redaction rule previously registered for
   * `topicPattern`. Messages already queued or logged keep whatever form
   * they were published with; only new publishes are affected. Returns
   * true when a rule existed and was removed.
   */
  clearTopicRedaction(topicPattern: string): boolean {
    return this.redactionRules.delete(topicPattern);
  }

  /**
   * Returns the redaction field paths that apply to `topic`, or `undefined`
   * when no rule matches. Same precedence as `schemaForTopic`.
   */
  private redactionForTopic(topic: string): string[][] | undefined {
    const exact = this.redactionRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, paths] of this.redactionRules) {
      let matcher = this.redactionMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.redactionMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return paths;
    }
    return undefined;
  }

  /**
   * Applies the topic's sensitive-field redaction rules to a publish
   * payload at admission time (see `setTopicRedaction`, EB-68). Returns
   * the payload with every matching field replaced by the redaction
   * sentinel — the input is never mutated — and counts each replaced
   * field on the topic's stats entry and the global total
   * (`TopicStats.redactedFields` / `BusStats.redactedFields`). A no-op
   * when no rule matches the topic; non-object payloads pass through
   * untouched and uncounted.
   */
  private redactAdmissionPayload(topic: string, payload: unknown): unknown {
    const paths = this.redactionForTopic(topic);
    if (paths === undefined) return payload;
    const { value, redacted } = redactFieldPaths(payload, paths);
    if (redacted > 0) {
      this.statsFor(topic).redactedFields += redacted;
      this.totalRedactedFields += redacted;
    }
    return value;
  }

  /**
   * Returns the per-message byte cap that applies to `topic`, or `undefined`
   * when no rule matches. Same precedence as `ttlForTopic`.
   */
  private maxMessageBytesForTopic(topic: string): number | undefined {
    const exact = this.maxMessageBytesRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, maxBytes] of this.maxMessageBytesRules) {
      let matcher = this.maxMessageBytesMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.maxMessageBytesMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return maxBytes;
    }
    return undefined;
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
   * Returns the versioned rule set that applies to `topic`, or
   * `undefined` when no versioned registration matches. Same precedence
   * as `schemaForTopic`: an exact-topic rule set wins over patterns, the
   * earliest-registered matching pattern wins.
   */
  private schemaVersionsForTopic(topic: string): Map<number, SchemaVersionRegistration> | undefined {
    const exact = this.schemaVersionRules.get(topic);
    if (exact !== undefined) return exact;
    for (const [pattern, versions] of this.schemaVersionRules) {
      let matcher = this.schemaVersionMatcherCache.get(pattern);
      if (matcher == null) {
        matcher = compilePattern(pattern);
        this.schemaVersionMatcherCache.set(pattern, matcher);
      }
      if (matcher.test(topic)) return versions;
    }
    return undefined;
  }

  /**
   * Highest registered version in a versioned rule set. Rule sets are
   * never empty (the last version's removal deletes the set), so this
   * always finds one.
   */
  private latestSchemaVersion(versions: Map<number, SchemaVersionRegistration>): number {
    let latest = -1;
    for (const version of versions.keys()) {
      if (version > latest) latest = version;
    }
    return latest;
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
    // The compression input is the wire bytes (EB-92): a binary message's
    // raw bytes, or the UTF-8 JSON encoding for ordinary messages — so
    // the threshold is always a byte count, never a JSON-length
    // approximation. An unserializable non-binary payload passes through
    // untouched, as before.
    const wireBytes = wireBytesOfPayload(payload, this.binaryMessages.has(msg));
    if (wireBytes === undefined) return payload;
    const bytesBefore = wireBytes.length;
    // Small messages pass through: compression is opt-in per byte saved,
    // not a tax on every publish.
    if (bytesBefore <= rule.thresholdBytes) return payload;
    const startedAtMs = this.now();
    const deflated =
      rule.dictionary === undefined
        ? deflateSync(wireBytes, { level: rule.level })
        : deflateSync(wireBytes, { level: rule.level, dictionary: rule.dictionary });
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
   * Inflates a replayed compression envelope (EB-92): a binary record's
   * envelope inflates back to raw bytes — never `JSON.parse`d — so a
   * binary content filter sees the `Uint8Array` the publisher sent.
   */
  private inflateReplayEnvelope(
    envelope: CompressedPayload,
    rec: { topic: string; seq: number; dictId?: string; binary?: boolean },
  ): unknown {
    const dictionary = this.dictionaryForRecord(rec);
    if (rec.binary === true) {
      const raw = Buffer.from(envelope.data, 'base64');
      return dictionary === undefined ? inflateSync(raw) : inflateSync(raw, { dictionary });
    }
    return inflateEnvelopePayload(envelope, dictionary);
  }

  /**
   * Resolves a durable-log record's payload to its wire form (EB-92): a
   * binary record's v2 bytes may encode a compression envelope
   * (binary+compressed publishes persist the envelope's JSON as the v2
   * bytes) — parse it back to the envelope so the compressed/inflate
   * machinery works unchanged; uncompressed binary records stay raw
   * bytes. Non-binary records pass through untouched.
   */
  private wirePayloadOfRecord(rec: { binary?: boolean; payload: unknown }): unknown {
    if (rec.binary !== true || !(rec.payload instanceof Uint8Array)) return rec.payload;
    let parsed: unknown;
    try {
      parsed = JSON.parse(
        Buffer.from(rec.payload.buffer, rec.payload.byteOffset, rec.payload.byteLength).toString(
          'utf8',
        ),
      );
    } catch {
      return rec.payload;
    }
    return isCompressedPayload(parsed) ? parsed : rec.payload;
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
    if (this.binaryMessages.has(msg)) {
      // Binary messages (EB-92) inflate back to raw bytes — never
      // `JSON.parse`d. The subscriber receives a `Uint8Array`, the same
      // shape the publisher sent.
      const raw = Buffer.from(payload.data, 'base64');
      msg.payload = dictionary === undefined ? inflateSync(raw) : inflateSync(raw, { dictionary });
    } else {
      msg.payload = inflateEnvelopePayload(payload, dictionary);
    }
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
        // EB-96: the namespace child lives in a scoped view of the root
        // log's backend — one backend serves the root log and every
        // namespace, including custom (e.g. S3) backends.
        backend: this.durableLog.storageBackend.scoped(`${NAMESPACE_LOG_KEY_PREFIX}${encodeURIComponent(prefix)}`),
        maxEntriesPerTopic: this.durableLogMaxEntriesPerTopic,
        maxAgeMs: this.durableLogMaxAgeMs,
        now: this.now,
        keyCompaction: this.durableLogKeyCompaction,
        segmentRotation: this.durableLogSegmentRotation,
        encryptor: this.durableLogEncryptor,
        // EB-94: namespace child logs report checksum failures to the
        // same audit trail as the root log.
        onSegmentChecksumFailed: (info) =>
          this.auditSubscription({
            action: 'segment_checksum_failed',
            actor: 'system',
            topic: info.topic,
            detail: { path: info.path, segmentId: info.segmentId },
          }),
      });
      this.namespaceLogs.set(prefix, child);
      // A restarted bus reopens the namespace's on-disk history: reseed
      // the bus-side sequence/stats cursors from it, like the root log's
      // construction-time recovery, so numbering continues instead of
      // restarting at 1.
      this.recoverLogState(child);
      // Rebuild the namespace's retained-message table from its child
      // log (EB-87), like the root log's construction-time recovery.
      this.recoverRetainedFromLog(child);
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
      // Per-(topic, partition) sequence recovery (EB-90): seed each
      // partition's counter from the log's per-partition highs, so
      // numbering continues across restarts — never restarts, never
      // reuses. Logs written before partition routing carry no
      // per-partition data: their topic max seeds partition 0, the
      // partition every pre-partitioning message belonged to.
      const partitionSeqs = log.recoveredPartitionSeqs(topic);
      if (partitionSeqs.size === 0) {
        if (lastSeq > 0) {
          this.topicSeq.set(EventBus.partitionSeqKey(topic, 0), lastSeq);
          this.seedPartitionPublished(topic, 0, lastSeq);
        }
      } else {
        for (const [partition, seq] of partitionSeqs) {
          this.topicSeq.set(EventBus.partitionSeqKey(topic, partition), seq);
          // Every sequence draw is paired with exactly one publish count
          // (admitted publishes and publish-side sheds both draw), so the
          // recovered per-partition high doubles as the recovered publish
          // count — the per-partition rows reconcile with the topic total
          // exactly like the live counters do.
          this.seedPartitionPublished(topic, partition, seq);
        }
      }
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
        publisherRateLimited: 0,
        pausedMessages: 0,
        rejectedMessages: 0,
        lateMessages: 0,
        duplicateMessages: 0,
        aliasRetiredMessages: 0,
        redactedFields: 0,
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
    // Durable-log replay progress (EB-71): validated before anything
    // registers, so a throw leaves no half-registered subscriber behind.
    this.validateReplayProgressOptions(
      opts?.onReplayProgress,
      opts?.progressInterval,
      resumeFromSeq,
      resumeFromTime,
    );
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
    // Shadow traffic mirroring (EB-63): validated before anything
    // registers, so a throw leaves no half-registered subscriber behind.
    // A shadow mirror is a plain live-traffic tap — the delivery-control
    // and ordering features below would either silently do nothing or
    // need shadow-aware plumbing the spec does not ask for, so combining
    // them fails fast instead of pretending to work. Durable-log replay
    // would pre-fill the mirror with history through the shared
    // backpressure counters, so it is rejected too: a shadow subscribes
    // to live production traffic, nothing else.
    const shadowOpts = resolveShadowOptions(opts?.shadow);
    if (shadowOpts !== undefined) {
      if (resumeFromSeq !== undefined || resumeFromTime !== undefined) {
        throw new RangeError(
          'subscribe: shadow cannot be combined with resumeFromSeq/resumeFromTime',
        );
      }
      const incompatible: Array<[string, unknown]> = [
        ['throttle', opts?.throttle],
        ['deliveryShaping', opts?.deliveryShaping],
        ['slowStart', opts?.slowStart],
        ['rateLimit', opts?.rateLimit],
        ['healthProbe', opts?.healthProbe],
        ['batch', opts?.batch],
        ['window', opts?.window],
        ['causal', opts?.causal],
        ['deduplicateMessages', opts?.deduplicateMessages],
        ['keyHotspot', opts?.keyHotspot],
        ['ackLatency', opts?.ackLatency],
        ['maxInflight', opts?.maxInflight],
      ];
      for (const [name, value] of incompatible) {
        if (value !== undefined && value !== false) {
          throw new RangeError(`subscribe: shadow cannot be combined with ${name}`);
        }
      }
    }
    const capacity = shadowOpts?.queueSize ?? opts?.queueSize ?? 100;
    const queueMaxBytes = opts?.queueMaxBytes;
    if (queueMaxBytes !== undefined && (!Number.isFinite(queueMaxBytes) || queueMaxBytes <= 0)) {
      throw new RangeError('subscribe: queueMaxBytes must be a positive finite number of bytes');
    }
    // Subscription lease (EB-69): validated before anything registers, so
    // a throw leaves no half-registered subscriber behind.
    const leaseMs = opts?.leaseMs;
    if (leaseMs !== undefined && (!Number.isInteger(leaseMs) || leaseMs <= 0)) {
      throw new RangeError('subscribe: leaseMs must be a positive integer number of milliseconds');
    }
    // Idle subscriber auto-reclamation (EB-72): validated before
    // anything registers, so a throw leaves no half-registered
    // subscriber behind.
    const maxIdleMs = opts?.maxIdleMs;
    if (maxIdleMs !== undefined && (!Number.isInteger(maxIdleMs) || maxIdleMs <= 0)) {
      throw new RangeError('subscribe: maxIdleMs must be a positive integer number of milliseconds');
    }
    const idleActivity = opts?.idleActivity;
    if (maxIdleMs === undefined && idleActivity !== undefined) {
      throw new RangeError('subscribe: idleActivity requires maxIdleMs');
    }
    if (
      idleActivity !== undefined &&
      idleActivity !== 'deliver' &&
      idleActivity !== 'ack' &&
      idleActivity !== 'any'
    ) {
      throw new RangeError("subscribe: idleActivity must be 'deliver', 'ack' or 'any'");
    }
    const idleMode: IdleActivityMode = idleActivity ?? 'any';
    // Subscription audit actor (EB-78): validated before anything
    // registers, so a throw leaves no half-registered subscriber behind.
    const actor = opts?.actor ?? 'system';
    assertValidActor(actor, 'subscribe');
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
    // half-registered subscriber behind. The ramp targets the shaping
    // cap when shaping is configured, full speed otherwise.
    const slowStart = createSlowStartGate(
      opts?.slowStart,
      deliveryShaping?.messagesPerSec,
      this.now,
    );
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const maxInflight = resolveMaxInflightOptions(opts?.maxInflight);
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
    const barrier = resolveBarrierOptions(opts?.barrier);
    // Validated before anything registers, so a throw leaves no
    // half-registered subscriber behind.
    const batch = resolveBatchDeliveryOptions(opts?.batch);
    // Tumbling-window aggregation (EB-73): validated before anything
    // registers, so a throw leaves no half-registered subscriber behind.
    const window = resolveWindowOptions(opts?.window);
    // The in-flight cap counts single handler invocations; a batch hands
    // many messages to one call, so the two cannot share a coherent
    // accounting — fail fast instead of pretending the cap applies.
    if (batch != null && maxInflight !== undefined) {
      throw new RangeError('subscribe: maxInflight cannot be combined with batch');
    }
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
    // Envelope signature verification (EB-84): validated before anything
    // registers, so a throw leaves no half-registered subscriber behind.
    const verifySignature = opts?.verifySignature ?? false;
    if (typeof verifySignature !== 'boolean') {
      throw new TypeError('verifySignature must be a boolean');
    }
    // Publisher-identity verification (EB-97): same fail-fast contract.
    const verifyPublisher = opts?.verifyPublisher ?? false;
    if (typeof verifyPublisher !== 'boolean') {
      throw new TypeError('verifyPublisher must be a boolean');
    }
    // Durable-log replay progress (EB-71): the observer was validated
    // above; the interval falls back to the 100-entry default.
    const onReplayProgress = opts?.onReplayProgress;
    const progressInterval = opts?.progressInterval ?? REPLAY_PROGRESS_INTERVAL_DEFAULT;
    // Multi-pattern composite subscription (EB-70): validated before
    // anything registers, so a throw leaves no half-registered subscriber
    // behind.
    const compositeOpts = resolveCompositeOptions(opts?.patterns);
    // Slow-consumer backpressure tier (EB-83): validated before anything
    // registers, so a throw leaves no half-registered subscriber behind.
    const backpressure = resolveBackpressurePolicy(opts?.backpressure, opts?.dropPolicy);
    // The queue-level shed policy: an explicit drop tier selects it
    // directly; the 'block' tier sheds per the subscriber's own
    // `dropPolicy` on first saturation (the historical default behavior).
    // The shadow queue mirrors the production shed policy unless the
    // shadow overrides it independently.
    const queuePolicy: DropPolicy =
      shadowOpts?.dropPolicy ??
      (backpressure === 'block' ? (opts?.dropPolicy ?? 'drop-oldest') : backpressure);
    const queue = new BoundedQueue<BusMessage>({
      capacity,
      // The shadow queue's backpressure policy is independently tunable
      // via `ShadowSubscribeOptions`; otherwise the subscription's own
      // `dropPolicy` applies, exactly as for a production subscriber.
      policy: queuePolicy,
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
                // The subscriber's live pattern, not the registration-time
                // capture: `updateSubscriptionPattern` (EB-74) rewrites it
                // in place, and events must name the pattern the
                // subscription actually matches on.
                subscriberId: id,
                pattern: subscriber.pattern,
                queueSize: size,
                capacity,
                dropped: queue.droppedCount,
              });
            },
          }),
      ...(onDrained == null && throttle == null && backpressure !== 'block'
        ? {}
        : {
            onDrained: (size: number) => {
              // EB-83: the 'block' tier unblocks here — the queue receded
              // below the high-water mark, so the subscriber may accept
              // new arrivals again. Cleared before the throttle/user hooks
              // so observers see the already-unblocked state.
              if (backpressure === 'block' && subscriber.blocked) {
                subscriber.blocked = false;
              }
              if (throttle != null) {
                this.releaseThrottle(subscriber, size);
              }
              onDrained?.({
                // Live pattern (see onBackpressure above): a hot pattern
                // update must not leave events naming a stale pattern.
                subscriberId: id,
                pattern: subscriber.pattern,
                queueSize: size,
                capacity,
                dropped: queue.droppedCount,
                highWaterMark: queue.highWaterMark,
              });
            },
          }),
    });
    // Per-topic subscriber caps (EB-81): enforced after every other
    // subscribe-time validation and before anything registers — a throw
    // leaves no half-registered subscriber behind, and in particular
    // runs before the composite refcounts below are acquired.
    this.checkSubscriberLimit(topicPattern);
    // Composite patterns (EB-70): translated under the namespace exactly
    // like the base pattern, then compiled through the dedicated
    // composite cache (reference-counted; released on unsubscribe). Like
    // `compiledMatcher` above, the refs are acquired before registration —
    // nothing between here and `subscribers.set` throws.
    const composite =
      compositeOpts === undefined ? undefined : this.acquireComposite(compositeOpts, ns?.prefix);
    // Subscription migration (EB-82): capture the serializable subset of
    // the subscribe options now — after every validation above ran, so a
    // rejected subscribe never captures anything. Function-valued options
    // (filters, callbacks) are reported separately, never silently dropped.
    const { options: migrationOptions, unsupported: unsupportedMigrationOptions } =
      serializableSubscribeOptions(opts);
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
      // Slow-consumer backpressure tier (EB-83): the 'block' tier starts
      // unblocked; saturation arms it in `enqueueMessage`.
      backpressure,
      blocked: false,
      blockedCount: 0,
      queueCapacity: capacity,
      lastDeliveredSeq: new Map(),
      lastEpoch: new Map(),
      throttle,
      onThrottled,
      deliveryShaping,
      slowStart,
      maxInflight: maxInflight == null ? undefined : { max: maxInflight, inFlight: 0 },
      // Idle auto-reclamation (EB-72): the mode marker doubles as the
      // opt-in flag — recordIdleActivity gates on it, so non-opt-in
      // subscribers pay a single property check per delivery.
      idleActivity: maxIdleMs === undefined ? undefined : idleMode,
      rateLimit,
      latency: deliveryLatency,
      ackLatency,
      latencySlo,
      lag: lagMonitor,
      keyHotspot,
      causal,
      // Barrier cut-marker gate (EB-98): opt-in per subscriber.
      barrier,
      batch,
      window,
      health:
        healthProbe == null
          ? undefined
          : { consecutiveFailures: 0, degraded: false, ...healthProbe },
      onDegraded,
      filter,
      // Envelope signature verification (EB-84): opt-in integrity check
      // at the publish side — a failed check drops the message before
      // any content processing, counted and audited. False by default:
      // a non-verifying subscriber pays a single boolean check per
      // delivery.
      verifySignature,
      // Publisher-identity verification (EB-97): opt-in check that the
      // envelope's publisherSignature verifies under the publisher's
      // registered keys — dropped, counted, and audited on failure.
      verifyPublisher,
      // Durable-log replay progress observability (EB-71): the
      // per-subscriber progress state appears on the first resume
      // replay (see `admitReplayRecords`).
      onReplayProgress,
      progressInterval,
      // Composite include/exclude patterns (EB-70), evaluated per message
      // after the content filter in `deliverUnkeyed`/`deliverShadow`.
      composite,
      dedup,
      // Shadow traffic mirroring (EB-63): the independent per-subscriber
      // counters start at zero; the queue above is already the shadow's
      // own, sized by its independent backpressure policy.
      shadow: shadowOpts === undefined ? undefined : { delivered: 0, dropped: 0 },
      // Subscription migration (EB-82): the serializable options this
      // subscription was created with, for `exportSubscription`.
      migrationOptions,
      unsupportedMigrationOptions,
    };
    this.subscribers.set(id, subscriber);
    if (composite !== undefined) this.compositeSubscriptionCount += 1;
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

    // MQTT-style retained messages (EB-87): a late joiner first receives
    // the latest retained message of every matching topic — synchronously,
    // bypassing the backpressure queue — before any replay below and
    // before any live message, so the snapshot-then-live order is
    // guaranteed by construction. (Deferred for reliable subscribers:
    // `subscribeReliable` performs it through the at-least-once wrapper
    // once installed.)
    this.deliverRetainedSnapshot(subscriber);

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

    const subscription: Subscription = {
      id,
      unsubscribe: () => {
        if (!this.subscribers.delete(id)) return;
        // Subscription lifecycle audit (EB-78): name the live pattern —
        // `updateSubscriptionPattern` rewrites it in place, and the audit
        // must reflect what the subscription actually matched on.
        this.auditSubscription({
          action: 'unsubscribe',
          subId: id,
          pattern: subscriber.pattern,
          actor: 'system',
        });
        // A lease must not outlive its subscriber: no phantom expiry
        // after unsubscribe (EB-69).
        this.leases.clear(id);
        // An idle window must not outlive its subscriber either: no
        // phantom expiry after unsubscribe (EB-72).
        this.idleReclaimer.clear(id);
        // Pending timers must not outlive the subscriber: no phantom
        // resume or re-flush after unsubscribe.
        const health = subscriber.health;
        if (health?.autoResumeTimer !== undefined) {
          clearTimeout(health.autoResumeTimer);
          health.autoResumeTimer = undefined;
        }
        this.clearShapingTimer(subscriber);
        this.clearSlowStartTimer(subscriber);
        this.clearRateLimitTimer(subscriber);
        this.clearBatchTimer(subscriber);
        this.clearWindowTimer(subscriber);
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
        // Composite subscriptions (EB-70): release the composite-cache
        // references and drop the live count.
        if (subscriber.composite !== undefined) {
          this.releaseComposite(subscriber.composite);
          this.compositeSubscriptionCount -= 1;
        }
        // The advertised pattern set may have shrunk.
        this.advertiseClusterPatterns();
      },
    };
    // Subscription migration (EB-82): the plain teardown, replaced with
    // the outermost wrapper by the subscribe family below
    // (`subscribeReliable`). `completeMigration` runs this closure so the
    // source subscription always unsubscribes through the normal path.
    subscriber.teardown = subscription.unsubscribe;
    // Subscription lease (EB-69): opt-in only. Armed after the
    // subscription object exists so expiry can unsubscribe through the
    // outermost wrapper (see `subscribeReliable` / `subscribeToGroup`,
    // which adopt their outer closure below).
    if (leaseMs !== undefined) {
      this.leases.arm(id, leaseMs, topicPattern, subscription.unsubscribe);
    }
    // Idle auto-reclamation (EB-72): opt-in only. Armed after the
    // subscription object exists so expiry can unsubscribe through the
    // outermost wrapper (see `subscribeReliable` / `subscribeToGroup`,
    // which adopt their outer closure below) — exactly like the lease
    // path. A subscriber may hold both a lease and an idle window; they
    // are independent opt-ins and whichever fires first wins, the other
    // being cleared by the shared unsubscribe path.
    if (maxIdleMs !== undefined) {
      this.idleReclaimer.arm(id, maxIdleMs, idleMode, topicPattern, subscription.unsubscribe);
    }
    // Subscription lifecycle audit (EB-78): the subscriber is fully
    // registered — record the (translated) pattern the matcher actually
    // uses, so a namespaced subscription audits under its concrete form.
    this.auditSubscription({ action: 'subscribe', subId: id, pattern: topicPattern, actor });
    return subscription;
  }

  /**
   * Renews the subscription lease for `subId` (see
   * `SubscribeOptions.leaseMs`, EB-69): the expiry timer restarts,
   * measuring a fresh `ms` window from the renewal moment. Returns
   * `true` on success, `false` when `subId` holds no active lease
   * (unknown subscription id, or a subscription that never opted into a
   * lease). Throws `RangeError` for a non-positive-integer `ms`.
   */
  renewLease(subId: string, ms: number): boolean {
    if (!Number.isInteger(ms) || ms <= 0) {
      throw new RangeError('renewLease: ms must be a positive integer number of milliseconds');
    }
    return this.leases.renew(subId, ms);
  }

  /**
   * Exports one subscription into a portable `SubscriptionSnapshot` for
   * cross-instance migration (EB-82) — step 1 of the flow:
   *
   * 1. `exportSubscription(subId)` on the source — pauses delivery, takes
   *    the snapshot.
   * 2. `importSubscription(snapshot, handler)` on the target — rebuilds
   *    the subscription and replays the backlog.
   * 3. `completeMigration(token)` on the source — unsubscribes the source
   *    subscription (or `cancelMigration(token)` to resume it instead).
   *
   * The snapshot captures the pattern (pre-namespace-translation),
   * namespace, serializable subscribe options, committed ack position,
   * remaining lease time, backlog stats, and every undelivered message
   * in delivery order. Function-valued options (content filters,
   * lifecycle callbacks) cannot cross instances: they are listed openly
   * in `snapshot.unsupportedOptions`, never dropped silently.
   *
   * Export puts the source subscription into "migrating" state: the
   * drain skips it, so delivery pauses while the backlog stays queued —
   * nothing is dropped by the pause itself. For reliable subscriptions
   * the outstanding (unacked) deliveries are taken out of the ack
   * tracker WITHOUT requeueing, so the source never redelivers them;
   * they travel in `snapshot.pendingMessages` and the target delivers
   * each exactly once. Acked messages are absent by construction and
   * are never re-delivered.
   *
   * Returns `null` (paralleling `renewLease`) when there is nothing to
   * export: an unknown subscription id, a consumer-group member (group
   * membership cannot migrate — the roster is bus-local), or a
   * subscription that already has an export in flight.
   *
   * Honest boundaries (see the README's "Subscription migration"
   * section): per-message TTL deadlines do not travel; dead-letter-queue
   * contents, open window aggregates, per-key reorder-buffer holdings and
   * per-message redelivery counters do not migrate; in-flight handler
   * executions at export time are the caller's responsibility; publishes
   * arriving between export and complete are queued on the source but are
   * NOT in the snapshot — quiesce producers or accept the reported
   * straggler count at complete time.
   */
  exportSubscription(subId: string): SubscriptionSnapshot | null {
    const subscriber = this.subscribers.get(subId);
    // Parallels `renewLease`: an unknown id is not an error — there is
    // simply nothing to export.
    if (subscriber === undefined) return null;
    // Consumer-group memberships cannot migrate: the competing-consumer
    // roster, assignment watermarks and rebalance protocol are bus-local
    // (see `subscribeToGroup`).
    if (subscriber.groupId !== undefined) return null;
    // An export is already in flight for this subscription — the first
    // snapshot is the valid one. Exporting twice would fork the backlog
    // (two snapshots, one queue), so the second export reports null.
    if (subscriber.migrating === true) return null;

    // A batched subscriber's linger timer delivers outside the drain
    // loop — disarm it so the pause below is total; the drain re-arms it
    // on resume (see drainBatchedSubscriber).
    this.clearBatchTimer(subscriber);
    const reliable = subscriber.reliable;
    // Take outstanding reliable deliveries out of the tracker: timers
    // cancelled, forgotten WITHOUT requeueing, so the source never
    // redelivers them. They travel in the snapshot; the target delivers
    // each exactly once. Their open ack trace spans are abandoned — the
    // source will never close them.
    const takenTracker = reliable?.tracker.takePending() ?? [];
    for (const msg of takenTracker) this.trace?.abandonAckSpan(msg);
    // A batched subscriber may hold dequeued-but-undelivered messages in
    // its batch accumulator — move them into the snapshot too, ahead of
    // the queued backlog (they were dequeued earliest).
    const batch = subscriber.batch;
    const takenBatched = batch === undefined ? [] : batch.pending.splice(0);
    const takenInFlight = [...takenTracker, ...takenBatched];
    const queued = subscriber.queue.snapshot();
    const namespace = subscriber.namespacePrefix;
    const prefix = namespace === undefined ? undefined : `${namespace}/`;
    const handoffToken = newTraceId();
    const exportedAtMs = this.now();
    subscriber.migrating = true;
    this.migrations.set(handoffToken, {
      subId,
      exportedAtMs,
      exportedQueueDepth: queued.length,
      takenInFlight,
    });
    return {
      kind: SUBSCRIPTION_SNAPSHOT_KIND,
      version: 1,
      handoffToken,
      // The registration-time pattern: strip the namespace translation
      // so the target re-registers through the same namespace rules.
      pattern:
        prefix === undefined || !subscriber.pattern.startsWith(prefix)
          ? subscriber.pattern
          : subscriber.pattern.slice(prefix.length),
      ...(namespace === undefined ? {} : { namespace }),
      reliable: reliable !== undefined,
      options: { ...(subscriber.migrationOptions ?? {}) },
      unsupportedOptions: [...(subscriber.unsupportedMigrationOptions ?? [])],
      ...(reliable === undefined ? {} : { ackCommittedSeq: reliable.tracker.highestSeq }),
      leaseRemainingMs: this.leases.remainingMs(subId),
      stats: this.migrationBacklogStats(subscriber, takenTracker.length),
      pendingMessages: [...takenInFlight, ...queued],
      exportedAtMs,
    };
  }

  /**
   * Rebuilds a subscription on the target instance from an
   * `exportSubscription` snapshot (EB-82) — step 2 of the flow.
   *
   * The handler cannot travel in the snapshot (functions do not cross
   * instances) and must be supplied here: a reliable (`Delivery`)
   * handler for reliable snapshots, a batch handler for batched ones.
   * The target re-subscribes with the snapshot's serializable options,
   * so every validation the normal subscribe path runs — unknown
   * namespaces, ACL denials, invalid option values — fails fast exactly
   * like a fresh subscribe, before anything registers. The lease is
   * re-armed with its REMAINING time, not the source's original full
   * duration; durable-log resume cursors do not transfer (the snapshot
   * already carries the complete undelivered backlog — replaying from
   * the cursor would re-deliver acked messages).
   *
   * The committed ack position is restored: acked messages are absent
   * from the snapshot (never re-delivered) and the target's delivery
   * counter continues the source's numbering. The backlog replays in
   * snapshot order through the normal admission path.
   *
   * The handoff token is single-use per instance: importing the same
   * snapshot twice throws `RangeError` without creating a second
   * subscription. A malformed snapshot throws `RangeError`; a non-
   * function handler throws `RangeError`.
   */
  importSubscription(
    snapshot: SubscriptionSnapshot,
    handler:
      | MessageHandler
      | ReliableMessageHandler
      | BatchMessageHandler
      | ReliableBatchMessageHandler,
  ): SubscriptionImportResult {
    assertValidSubscriptionSnapshot(snapshot);
    if (typeof handler !== 'function') {
      throw new RangeError('importSubscription: handler must be a function');
    }
    // Single-use per instance: a snapshot that already built a
    // subscription here can never build a second one.
    if (this.importedHandoffTokens.has(snapshot.handoffToken)) {
      throw new RangeError('importSubscription: handoff token was already imported by this instance');
    }
    const importOpts: Record<string, unknown> = { ...snapshot.options };
    if (snapshot.namespace !== undefined) importOpts.namespace = snapshot.namespace;
    // The lease resumes with its REMAINING time, not the source's
    // original full duration. A fully-elapsed remaining time means the
    // lease is effectively over — import without one rather than arming
    // a zero-length lease.
    if (snapshot.leaseRemainingMs > 0) {
      importOpts.leaseMs = Math.max(1, Math.floor(snapshot.leaseRemainingMs));
    } else {
      delete importOpts.leaseMs;
    }
    // Durable-log resume cursors do not transfer: the snapshot already
    // carries the complete undelivered backlog, and replaying from the
    // cursor would re-deliver messages the source already acked.
    delete importOpts.resumeFromSeq;
    delete importOpts.resumeFromTime;
    // Fail-fast exactly like a fresh subscribe: unknown namespaces, ACL
    // denials and invalid option values throw before anything registers —
    // a throw leaves no half-imported subscription behind, and the token
    // stays unconsumed so the caller can fix the cause and retry.
    const subscription = snapshot.reliable
      ? this.subscribeReliable(
          snapshot.pattern,
          handler as ReliableMessageHandler,
          importOpts as ReliableSubscribeOptions,
        )
      : this.subscribe(snapshot.pattern, handler as MessageHandler, importOpts as SubscribeOptions);
    const subscriber = this.subscribers.get(subscription.id);
    if (subscriber === undefined) {
      throw new Error(`importSubscription: subscription ${subscription.id} vanished after subscribe`);
    }
    // Resume from the committed ack position: acked messages are absent
    // from the snapshot by construction (never re-delivered), and the
    // target's delivery counter continues the source's numbering.
    const reliable = subscriber.reliable;
    if (reliable !== undefined && snapshot.ackCommittedSeq !== undefined) {
      reliable.tracker.seedHighestSeq(snapshot.ackCommittedSeq);
    }
    // Re-enqueue the migrated backlog in snapshot order through the
    // normal admission path. The dedup check is bypassed — these
    // envelopes were already admitted on the source (mirrors
    // `requeueUndelivered`); a full target queue applies the
    // subscriber's own drop policy, exactly like any other admission.
    // Per-message TTL deadlines do not survive migration (documented).
    for (const msg of snapshot.pendingMessages) {
      this.enqueueMessage(subscriber, msg, undefined, true);
    }
    this.importedHandoffTokens.add(snapshot.handoffToken);
    this.scheduleFlush();
    return { subId: subscription.id, handoffToken: snapshot.handoffToken };
  }

  /**
   * Completes a migration on the SOURCE instance (EB-82) — step 3a of
   * the flow, run after the target has taken over. Verifies the handoff
   * token, then unsubscribes the source subscription through the normal
   * teardown path (outermost wrapper: ack-tracker clear, lease/idle
   * clears, queue disposal — exactly like a manual `unsubscribe()`),
   * and returns the final stats.
   *
   * Messages that arrived on the source AFTER the export snapshot are
   * reported as `stragglers`: they were never part of the snapshot and
   * are discarded with the source queue. Quiesce producers between
   * export and complete, or accept the count.
   *
   * Returns `false` (never throws) when the token is unknown — or when
   * the source subscription is already gone (a lease/idle expiry or a
   * manual unsubscribe won the race; the end state the caller asked for
   * already holds).
   */
  completeMigration(handoffToken: string): CompletedSubscriptionMigration | false {
    const record = this.migrations.get(handoffToken);
    if (record === undefined) return false;
    this.migrations.delete(handoffToken);
    const subscriber = this.subscribers.get(record.subId);
    // The source subscription is already gone — a lease/idle expiry or a
    // manual unsubscribe won the race with the migration. The end state
    // the caller asked for (the source no longer delivers) already holds.
    if (subscriber === undefined || subscriber.migrating !== true) return false;
    const stats = this.migrationBacklogStats(
      subscriber,
      subscriber.reliable?.tracker.unackedCount ?? 0,
    );
    // Anything queued above the export-time depth arrived mid-migration:
    // never part of the snapshot, discarded with the source queue.
    const stragglers = Math.max(0, subscriber.queue.size - record.exportedQueueDepth);
    const teardown = subscriber.teardown;
    if (teardown === undefined) {
      throw new Error(`completeMigration: no teardown recorded for ${record.subId}`);
    }
    teardown();
    return { subId: record.subId, handoffToken, stats, stragglers };
  }

  /**
   * Aborts a migration on the SOURCE instance (EB-82) — step 3b of the
   * flow, the alternative to `completeMigration`. The in-flight backlog
   * taken at export is restored ahead of the never-delivered queue (in
   * original relative order) and delivery resumes; the snapshot's
   * handoff token is forgotten. Nothing is lost by cancelling.
   *
   * At-least-once caveat: a source handler execution that was already
   * in flight at export time may have processed its message — the
   * restored copy is then a redelivery, exactly like any ack-timeout
   * redelivery. Consumers must be idempotent.
   *
   * Returns `false` (never throws) when the token is unknown or the
   * source subscription is already gone.
   */
  cancelMigration(handoffToken: string): boolean {
    const record = this.migrations.get(handoffToken);
    if (record === undefined) return false;
    this.migrations.delete(handoffToken);
    const subscriber = this.subscribers.get(record.subId);
    if (subscriber === undefined || subscriber.migrating !== true) return false;
    // Restore the taken in-flight backlog ahead of the never-delivered
    // queue, in original relative order (the taken messages were dequeued
    // earliest). Re-admission bypasses the dedup check — these envelopes
    // were already admitted on this instance (mirrors
    // `requeueUndelivered`); per-message TTL deadlines are not restored.
    if (record.takenInFlight.length > 0) {
      const queued = subscriber.queue.drain();
      for (const msg of record.takenInFlight) this.enqueueMessage(subscriber, msg, undefined, true);
      for (const msg of queued) this.enqueueMessage(subscriber, msg, undefined, true);
    }
    subscriber.migrating = false;
    this.scheduleFlush();
    return true;
  }

  /**
   * Subscription migration (EB-82): point-in-time backlog reading for
   * one subscriber. `unackedCount` is passed in because an export
   * snapshot reports the pre-take outstanding count while the tracker
   * itself has already been emptied into the snapshot.
   */
  private migrationBacklogStats(
    subscriber: Subscriber,
    unackedCount: number,
  ): MigrationBacklogStats {
    return {
      queueDepth: subscriber.queue.size,
      droppedCount: subscriber.queue.droppedCount,
      expiredCount: subscriber.queue.expiredCount,
      unackedCount,
      queueBytes: subscriber.queue.queueBytes,
    };
  }

  /**
   * Cumulatively acknowledges a reliable subscriber's outstanding
   * deliveries up to a delivery sequence (EB-76): every outstanding
   * delivery with `seq <= seq` is acked at once — the high-throughput
   * alternative to acking each `Delivery` handle individually when the
   * consumer processes messages in order.
   *
   * Returns the number of deliveries actually acked. Delivery seqs that
   * are not outstanding — never delivered, already acked/nacked, timed
   * out and redelivered under a new seq, or moved to the dead-letter
   * queue after exhausting their redelivery budget (EB-28) — are
   * skipped silently, so gaps in the range never error. A `seq` outside
   * the subscriber's delivery history (not a positive integer, or beyond
   * the highest delivery seq ever tracked) is a `RangeError`; calling it
   * on a non-reliable subscription is a `RangeError` too. An unknown
   * subscriber id returns 0 (paralleling `renewLease`).
   *
   * Composition:
   * - ack-latency (EB-40): the range contributes a single sample — the
   *   last acked message's accepted→ack latency, sampled once at the
   *   range-ack moment — instead of one sample per delivery;
   * - trace spans (EB-45): each completed delivery's open ack span is
   *   closed, like an individual `ack()` would;
   * - the tracker's settle path runs per delivery, so `maxInflight`
   *   (EB-67) slots release and idle-ack activity (EB-72) records
   *   exactly as with individual acks. The per-message processing
   *   latency SLO (EB-53) is intentionally not sampled: a range ack is
   *   not a per-message processing event.
   *
   * `getStats().ackedRanges` counts the calls.
   */
  ackUpTo(subId: string, seq: number): number {
    if (!Number.isInteger(seq) || seq <= 0) {
      throw new RangeError('ackUpTo: seq must be a positive integer delivery sequence');
    }
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) return 0;
    const reliable = subscriber.reliable;
    if (reliable == null) {
      throw new RangeError('ackUpTo: subscriber is not a reliable (subscribeReliable) subscription');
    }
    const tracker = reliable.tracker;
    if (seq > tracker.highestSeq) {
      throw new RangeError(
        `ackUpTo: seq ${seq} is beyond the highest delivery seq ${tracker.highestSeq} tracked for this subscriber`,
      );
    }
    this.totalAckedRanges += 1;
    const acked = tracker.ackUpTo(seq);
    const ackState = subscriber.ackLatency;
    const last = acked.length - 1;
    for (let i = 0; i < acked.length; i++) {
      const msg = acked[i].msg;
      if (this.trace !== undefined && this.trace.hasTrace(msg)) {
        this.trace.closeAckSpan(subscriber, msg, this.now());
      }
      // EB-40: single sample for the whole range — the last acked
      // message's accepted→ack latency at this moment.
      if (i === last && ackState != null) {
        ackState.tracker.sampleOnAck(msg);
      }
    }
    return acked.length;
  }

  /**
   * Atomically replaces a live subscription's topic pattern (EB-74): from
   * the next publish on, the subscriber matches `newPattern` instead of
   * its registration pattern — no unsubscribe/resubscribe round-trip, no
   * delivery gap in between.
   *
   * The subscription is NOT rebuilt: its queue (queued messages intact),
   * per-topic sequence baselines, lease, idle window, health-probe state,
   * reliable ACK state and every other opt-in survive the swap untouched.
   * Only the match surface changes:
   * - the compiled matcher comes from the shared `patternCache`, so
   *   subscribers converging on the same pattern keep sharing one RegExp
   *   instance (a pattern whose last subscriber moved away is evicted,
   *   the same lifecycle as unsubscribe);
   * - the subscriber is re-filed in the publish-side prefix index under
   *   the new pattern's literal prefix;
   * - the cluster pattern advertisement is refreshed.
   *
   * Group members (`subscribeToGroup`) keep their identity and move
   * competing sets: the member leaves `(groupId, oldPattern)` and joins
   * `(groupId, newPattern)`, and both sets get their rebalance event
   * (`'leave'` on the old set — with partition redistribution and linger
   * handoff — `'join'` on the new one). Composite subscriptions (EB-70)
   * keep their include/exclude rules; only the base pattern is swapped.
   * Namespaced subscriptions (EB-52) take `newPattern` in their namespace
   * scope: the pattern is re-translated under the subscription's stored
   * prefix, so a hot update can never cross a namespace boundary.
   *
   * Like `subscribe`, the new pattern passes the broker-level ACL on its
   * registered (translated) form — `AclDeniedError`, audited via
   * `onAuthzDenied` — so a hot update cannot bypass subscribe-time
   * authorization. All validation runs before anything is mutated: an
   * invalid `newPattern` throws `RangeError` (non-string or empty; a `/`
   * inside a namespaced pattern) and the old pattern stays in force.
   *
   * Returns `true` on success — including a no-op when `newPattern`
   * equals the current pattern — and `false` when `subId` names no live
   * subscription.
   */
  updateSubscriptionPattern(subId: string, newPattern: string): boolean {
    const subscriber = this.subscribers.get(subId);
    if (subscriber === undefined) return false;
    if (typeof newPattern !== 'string' || newPattern.length === 0) {
      throw new RangeError('updateSubscriptionPattern: newPattern must be a non-empty string');
    }
    const prefix = subscriber.namespacePrefix;
    // Resolve the registered form, the matcher and the prefix-index key
    // exactly the way `subscribe` registers them — a namespaced
    // subscription's relative pattern is re-translated under its prefix.
    let registered: string;
    let matcher: RegExp;
    let newIndexKey: string;
    if (prefix !== undefined) {
      if (newPattern.includes('/')) {
        throw new NamespaceDeniedError('subscribe', prefix, { pattern: newPattern });
      }
      registered = `${prefix}/${newPattern}`;
      matcher = this.compileNamespacedMatcher(prefix, newPattern);
      const literal = EventBus.patternPrefixKey(newPattern);
      newIndexKey = literal === '' ? '' : `${prefix}/${literal}`;
    } else {
      registered = newPattern;
      matcher = this.compiledMatcher(newPattern);
      newIndexKey = EventBus.patternPrefixKey(newPattern);
    }
    const oldPattern = subscriber.pattern;
    if (registered === oldPattern) return true;
    // Broker-level ACL on the registered (translated) form, mirroring
    // `subscribe`: a hot update must not bypass subscribe-time
    // authorization. Audited before the throw, like `subscribe`.
    if (!this.aclAllowsSubscribe(registered)) {
      this.emitAuthzDenied('subscribe', undefined, registered);
      throw new AclDeniedError('subscribe', registered);
    }
    // --- All validation done; mutate below. ---
    // 1. Prefix index: move the id from the old bucket to the new one,
    //    evicting emptied keys (same invariant as unsubscribe).
    let oldIndexKey: string;
    if (prefix !== undefined) {
      const oldRelative = oldPattern.slice(prefix.length + 1);
      const literal = EventBus.patternPrefixKey(oldRelative);
      oldIndexKey = literal === '' ? '' : `${prefix}/${literal}`;
    } else {
      oldIndexKey = EventBus.patternPrefixKey(oldPattern);
    }
    const oldBucket = this.prefixIndex.get(oldIndexKey);
    if (oldBucket != null) {
      oldBucket.delete(subId);
      if (oldBucket.size === 0) this.prefixIndex.delete(oldIndexKey);
    }
    let newBucket = this.prefixIndex.get(newIndexKey);
    if (newBucket == null) {
      newBucket = new Set<string>();
      this.prefixIndex.set(newIndexKey, newBucket);
    }
    newBucket.add(subId);
    // 2. Pattern counts + compile cache (unsubscribe's lifecycle).
    const remaining = (this.subscribersByPattern.get(oldPattern) ?? 1) - 1;
    if (remaining <= 0) {
      this.subscribersByPattern.delete(oldPattern);
      this.patternCache.delete(oldPattern);
    } else {
      this.subscribersByPattern.set(oldPattern, remaining);
    }
    this.subscribersByPattern.set(registered, (this.subscribersByPattern.get(registered) ?? 0) + 1);
    // 3. The subscriber itself: pattern + matcher swap in place. The
    //    queue, sequence baselines, lease, idle window, health probe,
    //    reliable ACK state and every opt-in survive untouched.
    subscriber.pattern = registered;
    subscriber.matcher = matcher;
    // 4. Group members move competing sets: leave `(groupId, oldPattern)`
    //    and join `(groupId, newPattern)`, each with its rebalance event.
    const groupId = subscriber.groupId;
    if (groupId !== undefined) {
      this.moveGroupMember(subId, subscriber, groupId, oldPattern, registered);
    }
    // 5. Lease / idle expiry events name the live pattern; the timers
    //    themselves are preserved (relabelled, not re-armed).
    this.leases.relabelPattern(subId, registered);
    this.idleReclaimer.relabelPattern(subId, registered);
    // 6. The advertised pattern set may have changed.
    this.advertiseClusterPatterns();
    // Subscription lifecycle audit (EB-78): group moves (step 4) already
    // audited their leave/join rebalances inside `fireRebalance`; this
    // entry names the pattern change itself.
    this.auditSubscription({
      action: 'update_pattern',
      subId,
      pattern: registered,
      actor: 'system',
      detail: { from: oldPattern },
    });
    return true;
  }

  /**
   * Moves a consumer-group member from `(groupId, oldPattern)` to
   * `(groupId, newPattern)` without touching the subscriber object
   * itself (EB-74). The leave side mirrors the group unsubscribe path —
   * roster removal, partition redistribution with backlog replay to the
   * new owners, linger handoff, `'leave'` rebalance event — and the join
   * side mirrors `subscribeToGroup`'s join: roster add, assignment
   * invalidation, backlog replay for partitions migrating to the mover,
   * `'join'` rebalance event. A moved member cannot dictate the target
   * set's partition/strategy configuration, so both checks run with
   * `undefined` options: they pass for an existing set and leave a new
   * set non-partitioned with the default strategy.
   */
  private moveGroupMember(
    subId: string,
    subscriber: Subscriber,
    groupId: string,
    oldPattern: string,
    newPattern: string,
  ): void {
    const oldKey = EventBus.groupKey(groupId, oldPattern);
    const wasPartitioned = this.groupPartitions.get(oldKey) !== undefined;
    const beforeLeave = wasPartitioned ? new Map(this.partitionAssignment(oldKey)) : undefined;
    this.removeGroupMember(oldKey, subId);
    let leaveRebalance: PartitionRebalanceInfo | undefined;
    if (wasPartitioned) {
      const after = this.partitionAssignment(oldKey);
      const migrated = this.diffPartitionAssignment(oldKey, beforeLeave!, after);
      leaveRebalance = {
        assignment: Object.fromEntries(after),
        migrated,
        strategy: this.groupAssignmentStrategy.get(oldKey) ?? 'rendezvous',
      };
      for (const m of migrated) {
        const owner = this.subscribers.get(m.to);
        if (owner !== undefined) this.replayPartitionBacklog(owner, oldKey, m.partition);
      }
    }
    const linger = this.beginLingerHandoff(groupId, subscriber);
    this.fireRebalance(groupId, oldPattern, oldKey, 'leave', subId, linger, leaveRebalance);
    const newKey = EventBus.groupKey(groupId, newPattern);
    this.checkPartitionConfig(newKey, undefined);
    this.checkAssignmentConfig(newKey, undefined);
    let members = this.groupMembers.get(newKey);
    if (members == null) {
      members = [];
      this.groupMembers.set(newKey, members);
    }
    const partitioned = this.groupPartitions.get(newKey) !== undefined;
    const beforeJoin = partitioned ? new Map(this.partitionAssignment(newKey)) : undefined;
    members.push(subId);
    this.invalidatePartitionAssignment(newKey);
    let joinRebalance: PartitionRebalanceInfo | undefined;
    if (partitioned) {
      const after = this.partitionAssignment(newKey);
      const migrated = this.diffPartitionAssignment(newKey, beforeJoin!, after);
      joinRebalance = {
        assignment: Object.fromEntries(after),
        migrated,
        strategy: this.groupAssignmentStrategy.get(newKey) ?? 'rendezvous',
      };
      for (const m of migrated) {
        if (m.to === subId) this.replayPartitionBacklog(subscriber, newKey, m.partition);
      }
    }
    this.fireRebalance(groupId, newPattern, newKey, 'join', subId, undefined, joinRebalance);
  }

  /**
   * Records one qualifying activity event for idle reclamation (EB-72):
   * `'deliver'` from a successful handler invocation, `'ack'` from the
   * ack tracker's ack-settle hook. Zero overhead for non-opt-in
   * subscribers — a single property check, no allocation, no map
   * lookup. Events that do not match the subscriber's configured
   * `idleActivity` mode are ignored here; the rest re-arm the idle
   * window from the activity moment.
   */
  private recordIdleActivity(subscriber: Subscriber, kind: 'deliver' | 'ack'): void {
    const mode = subscriber.idleActivity;
    if (mode === undefined || (mode !== 'any' && mode !== kind)) return;
    this.idleReclaimer.activity(subscriber.id);
  }

  /**
   * Runs once per idle expiry, after `IdleReclaimer` disarmed the entry
   * and ran the subscription's outermost unsubscribe closure. Counts the
   * expiry, then fires `EventBusOptions.onIdleExpired` (error-isolated:
   * a throwing hook is swallowed so a broken observer can never disturb
   * the unsubscribe path, the same rule as `onLeaseExpired`).
   */
  private handleIdleExpiry(expired: {
    subId: string;
    maxIdleMs: number;
    pattern: string;
    idleActivity: IdleActivityMode;
  }): void {
    this.totalIdleExpired += 1;
    const onIdleExpired = this.onIdleExpired;
    if (onIdleExpired === undefined) return;
    const event: IdleExpiredEvent = {
      subscriberId: expired.subId,
      pattern: expired.pattern,
      maxIdleMs: expired.maxIdleMs,
      idleActivity: expired.idleActivity,
    };
    try {
      onIdleExpired(event);
    } catch {
      // Swallowed: a user callback must never break the bus.
    }
  }

  /**
   * Runs once per expired lease, after `LeaseManager` disarmed the entry
   * and ran the subscription's outermost unsubscribe closure. Counts the
   * expiry, then fires `EventBusOptions.onLeaseExpired` (error-isolated:
   * a throwing hook is swallowed so a broken observer can never disturb
   * the unsubscribe path, the same rule as `onAdmissionRejected`).
   */
  private handleLeaseExpiry(expired: { subId: string; leaseMs: number; pattern: string }): void {
    this.totalExpiredLeases += 1;
    const onLeaseExpired = this.onLeaseExpired;
    if (onLeaseExpired === undefined) return;
    const event: LeaseExpiredEvent = {
      subscriberId: expired.subId,
      pattern: expired.pattern,
      leaseMs: expired.leaseMs,
    };
    try {
      onLeaseExpired(event);
    } catch {
      // Swallowed: a user callback must never break the bus.
    }
  }

  /**
   * Registers many independent subscriptions atomically (EB-100): either
   * every entry registers or none does. On the first failure (invalid
   * pattern, ACL denial, namespace violation, bad options, …) every
   * already-registered entry is unsubscribed and the original error is
   * rethrown — the bus is left exactly as before the call.
   *
   * Each entry is an independent subscription (own handler, queue and
   * lifecycle), unlike composite patterns (EB-70) which are one
   * subscription with multi-pattern matching. Per-entry `opts` are
   * shallow-merged over `commonOpts` (entry wins); an entry `opts.group`
   * joins that consumer group (EB-16) as an independent member.
   *
   * Returns the subscriptions in registration order.
   */
  subscribeMany(
    entries: SubscribeManyEntry[],
    commonOpts?: SubscribeOptions,
  ): Subscription[] {
    if (!Array.isArray(entries) || entries.length === 0) {
      throw new RangeError('subscribeMany: entries must be a non-empty array');
    }
    if (
      commonOpts !== undefined &&
      (typeof commonOpts !== 'object' || commonOpts === null)
    ) {
      throw new TypeError('subscribeMany: commonOpts must be an object');
    }
    // All-or-nothing: register one by one; on the first failure roll back
    // everything registered so far (reverse order) and rethrow. `subscribe`
    // itself throws before registering on any validation failure, so the
    // rollback only ever undoes fully-registered subscriptions.
    const registered: Subscription[] = [];
    try {
      for (const entry of entries) {
        if (typeof entry !== 'object' || entry === null) {
          throw new TypeError('subscribeMany: each entry must be an object');
        }
        const { pattern, handler } = entry;
        if (typeof pattern !== 'string' || pattern.length === 0) {
          throw new RangeError('subscribeMany: entry pattern must be a non-empty string');
        }
        if (typeof handler !== 'function') {
          throw new TypeError('subscribeMany: entry handler must be a function');
        }
        const entryOpts = entry.opts;
        if (entryOpts !== undefined && (typeof entryOpts !== 'object' || entryOpts === null)) {
          throw new TypeError('subscribeMany: entry opts must be an object');
        }
        // Per-entry opts win over commonOpts (shallow merge).
        const merged = { ...(commonOpts ?? {}), ...(entryOpts ?? {}) } as SubscribeOptions & {
          group?: unknown;
          batch?: unknown;
        };
        const { group, ...rest } = merged;
        if (group !== undefined) {
          // Consumer-group batch join (EB-16): every entry with the same
          // `group` joins that group as an independent member.
          if (typeof group !== 'string' || group.length === 0) {
            throw new RangeError('subscribeMany: group must be a non-empty string');
          }
          registered.push(
            this.subscribeToGroup(
              group,
              pattern,
              handler as MessageHandler,
              rest as GroupSubscribeOptions,
            ),
          );
        } else if (rest.batch !== undefined && rest.batch !== false) {
          registered.push(
            this.subscribe(
              pattern,
              handler as BatchMessageHandler,
              rest as SubscribeOptions & { batch: true | BatchDeliveryOptions },
            ),
          );
        } else {
          registered.push(this.subscribe(pattern, handler as MessageHandler, rest));
        }
      }
    } catch (err) {
      for (let i = registered.length - 1; i >= 0; i--) {
        try {
          registered[i].unsubscribe();
        } catch {
          // Teardown never throws into the rollback path.
        }
      }
      throw err;
    }
    return registered;
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
    // Shadow mirroring (EB-63) is incoherent with at-least-once delivery:
    // a mirror copy has no acks, no redelivery budget, and no DLQ — it is
    // a fire-and-forget tap for validation, so combining them fails fast.
    if (opts?.shadow !== undefined && opts.shadow !== false) {
      throw new RangeError('subscribeReliable: shadow cannot be combined with reliable delivery');
    }
    const dlq = normalizeDeadLetterOptions(opts?.deadLetter, 'subscribeReliable');
    let deliver!: (msg: BusMessage | BusMessage[]) => void;
    // The retained snapshot (EB-87) is deferred until the at-least-once
    // wrapper below is installed: delivering through the not-yet-assigned
    // wrapper inside `subscribe` would throw. The snapshot runs after the
    // wrapper is ready — still synchronously inside this call, so it lands
    // before any replay or live delivery — as tracked deliveries the
    // subscriber must ack.
    this.retainedSnapshotDeferred = true;
    let sub: Subscription;
    try {
      sub = this.subscribe(topicPattern, (msg: BusMessage | BusMessage[]) => deliver(msg), opts);
    } finally {
      this.retainedSnapshotDeferred = false;
    }
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
      // EB-67: the tracker's single settle path (ack/nack/ack-timeout)
      // releases one in-flight slot and re-flushes so queued messages are
      // pulled into it. A no-op unless the subscriber opted into
      // maxInflight — with the cap disabled the tracker behaves exactly
      // as before.
      onSettle: () => {
        const cap = subscriber.maxInflight;
        if (cap == null) return;
        cap.inFlight = Math.max(0, cap.inFlight - 1);
        this.scheduleFlush();
      },
      // EB-72: ack-settle activity for idle reclamation. Allocated only
      // when the subscriber opted into maxIdleMs — otherwise the tracker
      // behaves exactly as before, zero overhead.
      ...(opts?.maxIdleMs === undefined
        ? {}
        : {
            onAck: () => this.recordIdleActivity(subscriber, 'ack'),
          }),
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
          // EB-72: the batch handler returned without throwing — idle
          // activity. The drain-level hooks skip reliable subscribers
          // (their wrapper can swallow throws via the DLQ nack path), so
          // this is the single recording point for reliable batches.
          this.recordIdleActivity(subscriber, 'deliver');
          return;
        }
        // With a DLQ configured, a synchronously throwing batch handler
        // nacks the whole batch instead of crashing the bus: every message
        // is requeued in batch order, so the redelivery preserves FIFO —
        // the batch analogue of the single-message nack path below.
        try {
          batchHandler(deliveries);
          // A nacked batch is a failed delivery, not activity — only the
          // clean return counts.
          this.recordIdleActivity(subscriber, 'deliver');
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
        // EB-72: the handler returned without throwing — idle activity
        // (see the batch path above for why this lives in the wrapper).
        this.recordIdleActivity(subscriber, 'deliver');
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
        // A nacked delivery is a failed delivery, not activity — only
        // the clean return counts.
        this.recordIdleActivity(subscriber, 'deliver');
      } catch (err) {
        lastFailure.set(msg, thrownErrorMessage(err));
        delivery.nack();
      }
    };
    // MQTT-style retained snapshot (EB-87), deferred from `subscribe`
    // above: the wrapper is installed now, so the snapshot is delivered
    // as tracked at-least-once deliveries — still synchronously, before
    // any replay or live message reaches the handler.
    this.deliverRetainedSnapshot(subscriber);
    const reliableSubscription: Subscription = {
      id: sub.id,
      unsubscribe: () => {
        // Cancel pending ack timers first: no phantom redeliveries after unsubscribe.
        tracker.clear();
        sub.unsubscribe();
      },
    };
    // Lease expiry must run the outermost teardown (ack-tracker clear
    // above); the inner closure alone would skip it (EB-69).
    this.leases.adoptUnsubscribe(sub.id, reliableSubscription.unsubscribe);
    // Idle expiry must run the outermost teardown too (EB-72).
    this.idleReclaimer.adoptUnsubscribe(sub.id, reliableSubscription.unsubscribe);
    // Subscription migration (EB-82): `completeMigration` must run the
    // outermost teardown — the inner closure alone would skip the
    // ack-tracker clear above.
    subscriber.teardown = reliableSubscription.unsubscribe;
    return reliableSubscription;
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
    // Shadow mirroring (EB-63) can never be a competing consumer: a shadow
    // must not steal a group's copy, advance the assignment watermark, or
    // touch committed offsets (EB-32) — so group membership rejects it
    // before anything registers.
    if (opts?.shadow !== undefined && opts.shadow !== false) {
      throw new RangeError('subscribeToGroup: shadow cannot be combined with consumer groups');
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
    const { resumeFromSeq, resumeFromTime, partitions: _partitions, assignment: _assignment, onReplayProgress: replayObserver, progressInterval: replayInterval, ...restOpts } = opts ?? {};
    this.validateResumeFromSeq(resumeFromSeq);
    this.validateResumeFromTime(resumeFromTime, resumeFromSeq);
    // Durable-log replay progress (EB-71): validated before anything
    // registers; stripped from `restOpts` like the resume cursors (the
    // replay runs below, after the group assignment is recorded) and
    // re-attached to the subscriber afterwards.
    this.validateReplayProgressOptions(replayObserver, replayInterval, resumeFromSeq, resumeFromTime);
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
    // Durable-log replay progress observer (EB-71): stripped from
    // `restOpts` above so the inner `subscribe` does not replay early —
    // the observer is attached here, before the group replay runs below.
    subscriber.onReplayProgress = replayObserver;
    if (replayInterval !== undefined) subscriber.progressInterval = replayInterval;
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
    // Subscription lifecycle audit (EB-78): the group membership on top
    // of the 'subscribe' the inner call recorded. Audited before the
    // rebalance fires, so the join entry precedes the 'rebalance' entry
    // the rebalance records.
    this.auditSubscription({
      action: 'group_join',
      subId: sub.id,
      groupId,
      pattern: topicPattern,
      actor: opts?.actor ?? 'system',
    });
    this.fireRebalance(groupId, topicPattern, key, 'join', sub.id, undefined, partitionRebalance);
    const groupSubscription: Subscription = {
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
        // Subscription lifecycle audit (EB-78): the leave itself, on top
        // of the 'unsubscribe' the inner closure recorded. Audited before
        // the rebalance fires, so the leave entry precedes it.
        this.auditSubscription({
          action: 'group_leave',
          subId: sub.id,
          groupId,
          pattern: topicPattern,
          actor: 'system',
        });
        this.fireRebalance(groupId, topicPattern, key, 'leave', sub.id, linger, pr);
      },
    };
    // Lease expiry must run the outermost teardown (group leave above);
    // the inner closure alone would leak the roster entry (EB-69).
    this.leases.adoptUnsubscribe(sub.id, groupSubscription.unsubscribe);
    // Idle expiry must run the outermost teardown too, so a group
    // member that goes idle leaves the competing set and triggers the
    // normal rebalance (EB-72).
    this.idleReclaimer.adoptUnsubscribe(sub.id, groupSubscription.unsubscribe);
    return groupSubscription;
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
    // Subscription lifecycle audit (EB-78): one entry per rebalance, not
    // per notified member — the group roster change is a single fact.
    this.auditSubscription({
      action: 'rebalance',
      groupId,
      pattern,
      actor: 'system',
      detail: { trigger, memberId, members: members.length },
    });
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
   * Validates the opt-in durable-log replay progress observer (EB-71)
   * before anything registers: a throw must not leave a
   * half-registered subscriber behind. The observer only observes a
   * resume replay, so it requires a resume cursor
   * (`resumeFromSeq` / `resumeFromTime`) — otherwise it would silently
   * never fire; the throttle interval only tunes the observer, so it
   * requires the observer itself.
   */
  private validateReplayProgressOptions(
    onReplayProgress: ReplayProgressCallback | undefined,
    progressInterval: number | undefined,
    resumeFromSeq: number | undefined,
    resumeFromTime: number | undefined,
  ): void {
    if (onReplayProgress !== undefined && typeof onReplayProgress !== 'function') {
      throw new RangeError('onReplayProgress must be a function');
    }
    if (progressInterval !== undefined && (!Number.isInteger(progressInterval) || progressInterval < 1)) {
      throw new RangeError('progressInterval must be a positive integer');
    }
    if (progressInterval !== undefined && onReplayProgress === undefined) {
      throw new RangeError('progressInterval requires onReplayProgress to be set');
    }
    if (onReplayProgress !== undefined && resumeFromSeq === undefined && resumeFromTime === undefined) {
      throw new RangeError('onReplayProgress requires resumeFromSeq or resumeFromTime to be set');
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
   * EB-93: the partition a message belongs to in a partitioned group,
   * publish-side aligned. A message carrying an explicit publish-side
   * partition (EB-90) — i.e. published to a topic with more than one
   * partition — is consumed on that partition instead of being re-hashed
   * on the consume side: modulo the group's partition count, so group
   * and topic partition counts may differ. Alignment preserves the
   * producer's partition-local ordering end to end and keys every
   * partition-dependent group state (keyed baseline skips, causal clock
   * skips, per-partition watermarks, replay filters) off the partition
   * the producer chose. Messages with no publish-side partition — a
   * single-partition topic (where 0 is the default, not an assignment)
   * or a record written before partition routing — keep the existing
   * hash routing (`partitionForMessage`), so unpartitioned traffic still
   * spreads deterministically across the group.
   */
  private alignedGroupPartition(
    n: number,
    topic: string,
    seq: number,
    publishPartition: number | undefined,
    key: string | undefined,
  ): number {
    if (publishPartition !== undefined && this.partitionsForTopic(topic) > 1) {
      return publishPartition % n;
    }
    return this.partitionForMessage(n, topic, seq, key);
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
   * `'invalid-envelope'` (shape validation failed),
   * `'signature-verification-failed'` (the envelope carried a source
   * signature that did not verify against this bus's signing rules for
   * the topic), or `'shed'` (ingress buffer full).
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
    // Cross-bus signature verification (EB-84): an inbound envelope
    // carrying the source bus's signature is verified against this bus's
    // signing rules for the (alias-resolved) destination topic — the
    // signature proves a key holder published it, across an untrusted
    // transport. The check recomputes the canonical raw payload bytes
    // from the carried payload, so a tampered payload fails it. A failed
    // check rejects the envelope outright. An envelope with no signature,
    // or a topic with no local signing rule, skips this check: the
    // destination's own publish-side signing still applies when the
    // message is admitted below, so signatures stay per-bus and a bridge
    // hop is a new admission.
    const inboundSignature = validated.envelope.signature;
    if (inboundSignature !== undefined) {
      const resolvedTopic = this.resolvePublishTopic(validated.envelope.topic).topic;
      const inboundRule = this.signingForTopic(resolvedTopic);
      if (inboundRule !== undefined) {
        const raw = serializeToJson(validated.envelope.payload);
        const inboundOk =
          raw !== undefined &&
          verifyTopicMessage(
            inboundRule.keys,
            validated.envelope.topic,
            inboundSignature.seq,
            Buffer.from(raw, 'utf8'),
            { kid: inboundSignature.kid, sig: inboundSignature.sig },
            this.now(),
            inboundRule.graceMs,
          );
        if (!inboundOk) return { accepted: false, reason: 'signature-verification-failed' };
      }
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
   * Compiles one composite subscription's include/exclude patterns (see
   * `SubscribeOptions.patterns`, EB-70) through the dedicated composite
   * cache, reference-counting each distinct pattern. `namespacePrefix`
   * translates the patterns exactly like the base pattern of a namespaced
   * subscription (EB-52), so composite matching operates on the concrete
   * `<prefix>/…` topic space. The caller releases the references with
   * `releaseComposite` on unsubscribe.
   */
  private acquireComposite(
    opts: { patterns: string[]; exclude: string[]; mode: 'any' | 'all' },
    namespacePrefix: string | undefined,
  ): CompositeSubscriberState {
    const cacheKeys: string[] = [];
    const compileAll = (patterns: string[]): RegExp[] =>
      patterns.map((pattern) => {
        const key = namespacePrefix === undefined ? pattern : `${namespacePrefix}/${pattern}`;
        let matcher = this.compositePatternCache.get(key);
        if (matcher == null) {
          matcher = compilePattern(key);
          this.compositePatternCache.set(key, matcher);
        }
        this.compositePatternRefs.set(key, (this.compositePatternRefs.get(key) ?? 0) + 1);
        cacheKeys.push(key);
        return matcher;
      });
    return {
      include: compileAll(opts.patterns),
      exclude: compileAll(opts.exclude),
      mode: opts.mode,
      cacheKeys,
    };
  }

  /**
   * Releases one composite subscription's composite-cache references,
   * evicting patterns whose last composite subscriber left — the
   * composite-cache counterpart of the `patternCache` eviction in the
   * unsubscribe path.
   */
  private releaseComposite(state: CompositeSubscriberState): void {
    for (const key of state.cacheKeys) {
      const remaining = (this.compositePatternRefs.get(key) ?? 1) - 1;
      if (remaining <= 0) {
        this.compositePatternRefs.delete(key);
        this.compositePatternCache.delete(key);
      } else {
        this.compositePatternRefs.set(key, remaining);
      }
    }
  }

  /**
   * Evaluates a subscriber's composite include/exclude patterns against a
   * concrete topic (EB-70): any exclude match rejects the message;
   * otherwise `'any'` mode accepts when some include pattern matches and
   * `'all'` mode only when every include pattern matches.
   */
  private matchesComposite(state: CompositeSubscriberState, topic: string): boolean {
    for (const matcher of state.exclude) {
      if (matcher.test(topic)) return false;
    }
    if (state.mode === 'all') {
      for (const matcher of state.include) {
        if (!matcher.test(topic)) return false;
      }
      return true;
    }
    for (const matcher of state.include) {
      if (matcher.test(topic)) return true;
    }
    return false;
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
    this.admitReplayRecords(subscriber, records, { trackProgress: true });
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
        if (mine.has(this.alignedGroupPartition(n, rec.topic, rec.seq, rec.partition, rec.key))) records.push(rec);
      }
    }
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    this.admitReplayRecords(subscriber, records, { trackProgress: true });
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
      for (const rec of log.readSinceTime(topic, fromTime)) {
        // Retained-message state (EB-87) is not a message: the seq-0
        // retained record never replays — its message already fanned out
        // as an ordinary publish (deduped against the snapshot in
        // `admitReplayRecords`), and late joiners get the snapshot.
        // Delayed-delivery schedule records keep their existing
        // pass-through behavior.
        if (rec.seq === 0 && rec.retained === true) continue;
        records.push(rec);
      }
    }
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    this.admitReplayRecords(subscriber, records, { trackProgress: true });
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
        // Retained-message state (EB-87) never replays — see
        // `replayLogSinceTime`.
        if (rec.seq === 0 && rec.retained === true) continue;
        if (mine.has(this.alignedGroupPartition(n, rec.topic, rec.seq, rec.partition, rec.key))) records.push(rec);
      }
    }
    records.sort((a, b) => a.at - b.at || a.seq - b.seq || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    this.admitReplayRecords(subscriber, records, { trackProgress: true });
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
        if (this.alignedGroupPartition(n, rec.topic, rec.seq, rec.partition, rec.key) === partition) records.push(rec);
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
   * `replayLogSinceTime`, `replayLogSinceTimeForPartitionedMember`, and
   * `replayPartitionBacklog` (migration replay).
   *
   * With `trackProgress` (EB-71) the pass is observed per subscriber:
   * the state lives on `subscriber.replayProgress` and the opt-in
   * `onReplayProgress` observer fires throttled plus once on completion
   * or abort. Only resume replays pass it — the migration replay
   * (`replayPartitionBacklog`) is a deliberate partition takeover, not
   * a resume, and stays unobserved. An aborting record (e.g. a
   * dictionary-compressed record whose dictionary is not registered —
   * replay stays fail-closed) marks the pass failed, surfaces the error
   * on the observer, and rethrows.
   */
  private admitReplayRecords(
    subscriber: Subscriber,
    records: ReplayRecord[],
    opts?: { ignoreLinger?: boolean; trackProgress?: boolean },
  ): void {
    // Durable-log replay progress (EB-71): recorded on the subscriber
    // so `getStats().replayProgress` stays observable after the pass.
    const observe = opts?.trackProgress === true;
    const progress: ReplayProgressState | undefined = observe
      ? { total: records.length, replayed: 0, skipped: 0, done: false, failed: false }
      : undefined;
    if (progress !== undefined) subscriber.replayProgress = progress;
    const onReplayProgress = observe ? subscriber.onReplayProgress : undefined;
    const progressInterval = subscriber.progressInterval;
    let nextProgressFireAt = progressInterval;
    // One clock reading for the whole pass: every record's TTL is
    // judged against the same "now", like the rate table in `getStats`.
    const replayNow = this.now();
    const fireReplayProgress = (done: boolean, failed: boolean, error?: string): void => {
      if (progress === undefined || onReplayProgress === undefined) return;
      const event: ReplayProgressEvent = {
        subscriberId: subscriber.id,
        pattern: subscriber.pattern,
        total: progress.total,
        replayed: progress.replayed,
        skipped: progress.skipped,
        done,
        failed,
        ...(error === undefined ? {} : { error }),
      };
      try {
        // Error-isolated, like `onAdmissionRejected`: a broken observer
        // must never disturb replay.
        onReplayProgress(event);
      } catch {
        // Swallowed.
      }
    };
    const maybeFireThrottled = (): void => {
      if (progress === undefined) return;
      if (progress.replayed + progress.skipped >= nextProgressFireAt) {
        nextProgressFireAt += progressInterval;
        fireReplayProgress(false, false);
      }
    };
    const countReplaySkipped = (): void => {
      if (progress === undefined) return;
      progress.skipped += 1;
      maybeFireThrottled();
    };
    const countReplayed = (): void => {
      if (progress === undefined) return;
      progress.replayed += 1;
      maybeFireThrottled();
    };
    // Keyed-ordering replay baseline (see `PublishOptions.key`): seed each
    // replayed key's expectation at its smallest replayed keySeq, so
    // replayed messages are released in keySeq order even when the log's
    // at-order differs from keySeq order (delayed deliveries are logged at
    // fan-out but numbered at schedule time). Records without a keySeq —
    // logs written before keyed ordering, or unkeyed messages — replay
    // exactly as before.
    for (const rec of records) {
      // MQTT-style retained snapshot (EB-87): this exact record was
      // already handed to the subscriber synchronously at subscribe
      // time — exclude it from the replay baselines as well as the
      // delivery below, so it is never delivered twice.
      if (subscriber.retainedDelivered?.get(rec.topic) === rec.seq) continue;
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
        // Retained-snapshot dedup (EB-87): see the key-order seed above.
        if (subscriber.retainedDelivered?.get(rec.topic) === rec.seq) continue;
        if (rec.causal === undefined) continue;
        const prev = causalGate.expected.get(rec.causal.source);
        if (prev === undefined || rec.causal.clock < prev) {
          causalGate.expected.set(rec.causal.source, rec.causal.clock);
        }
      }
    }
    const filter = subscriber.filter;
    try {
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
          // The log holds the wire payload — resolve a binary record's
          // v2 bytes first (a binary+compressed record's bytes encode
          // the envelope), then inflate a compressed envelope so the
          // filter judges the raw application payload. A binary filter
          // sees the `Uint8Array`, never a misparsed value.
          const wirePayload = this.wirePayloadOfRecord(rec);
          const rawPayload = isCompressedPayload(wirePayload)
            ? this.inflateReplayEnvelope(wirePayload, rec)
            : wirePayload;
          if (!filter(rawPayload, rec.topic)) {
            this.countFiltered(subscriber, rec.topic, rec.seq, '', rec.partition ?? 0);
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
            countReplaySkipped();
            continue;
          }
        }
        const composite = subscriber.composite;
        if (composite !== undefined && !this.matchesComposite(composite, rec.topic)) {
          // A composite-rejected replayed message is deliberately skipped,
          // exactly like a filter rejection above: never queued (it must not
          // churn the backpressure budget on reconnect), never a sequence
          // gap, and the key/causal baselines advance past it like the live
          // path does.
          this.countFiltered(subscriber, rec.topic, rec.seq, '', rec.partition ?? 0);
          if (rec.key !== undefined && rec.keySeq !== undefined) {
            this.skipKeySeq(subscriber, rec.key, rec.keySeq);
          }
          if (rec.causal !== undefined) {
            this.skipCausalClock(subscriber, rec.causal.source, rec.causal.clock);
          }
          countReplaySkipped();
          continue;
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
          this.advanceBaseline(subscriber, rec.topic, rec.seq, '', rec.partition ?? 0);
          if (rec.key !== undefined && rec.keySeq !== undefined) {
            this.skipKeySeq(subscriber, rec.key, rec.keySeq);
          }
          // A lingered causal message replays after the window expires: the
          // skip advances the expectation now, and the later replay arrives
          // as a (harmless, immediately delivered) regression.
          if (rec.causal !== undefined) {
            this.skipCausalClock(subscriber, rec.causal.source, rec.causal.clock);
          }
          countReplaySkipped();
          continue;
        }
        // MQTT-style retained snapshot (EB-87): the subscriber already
        // received this exact record synchronously at subscribe time,
        // before replay started — never deliver it twice. Like a content
        // filter skip, the per-topic baseline advances over it (it was
        // seen, not lost) and the key/causal baselines advance past it
        // so ordering gates never hang on a message that will never be
        // queued.
        if (subscriber.retainedDelivered?.get(rec.topic) === rec.seq) {
          this.advanceBaseline(subscriber, rec.topic, rec.seq, '', rec.partition ?? 0);
          if (rec.key !== undefined && rec.keySeq !== undefined) {
            this.skipKeySeq(subscriber, rec.key, rec.keySeq);
          }
          if (rec.causal !== undefined) {
            this.skipCausalClock(subscriber, rec.causal.source, rec.causal.clock);
          }
          countReplaySkipped();
          continue;
        }
        // An already-expired TTL is deliberately not resurrected: the
        // record still goes through the normal queue so the drain drops
        // it as expired (unchanged behavior), but for replay progress it
        // counts as skipped — never failed, and never replayed, so
        // `replayed + skipped` always equals the records this pass saw.
        const ttlExpired = rec.expiresAt !== undefined && rec.expiresAt <= replayNow;
        if (ttlExpired) {
          countReplaySkipped();
        }
        const wirePayload = this.wirePayloadOfRecord(rec);
        const msg: BusMessage = {
          topic: rec.topic,
          payload: wirePayload,
          seq: rec.seq,
          // Restores the partition logged at publish time (EB-90): the
          // replayed message keeps its publish-order identity per
          // partition, so gap detection and per-partition numbering stay
          // consistent across restarts. Records written before partition
          // routing carry no partition — they replay as partition 0.
          ...(rec.partition === undefined ? {} : { partition: rec.partition }),
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
          // Restores the publisher identity logged at publish time, so a
          // resumed subscriber sees the same producer attribution the live
          // delivery carried (see `PublishOptions.publisherId`).
          ...(typeof rec.publisherId === 'string' && rec.publisherId.length > 0
            ? { publisherId: rec.publisherId }
            : {}),
        };
        // The log stores the wire payload: a record written while a
        // compression rule was active carries the deflated envelope, so
        // re-register it for transparent inflation at delivery — exactly
        // like a live compressed message. Dictionary-compressed records
        // additionally re-register their dictionary from the registry; a
        // missing dictionary throws here (fail closed at replay) instead of
        // corrupting data at delivery. A binary (v2) record re-marks the
        // envelope so delivery inflates to bytes and verifies signatures
        // against the raw bytes.
        if (rec.binary === true) this.binaryMessages.add(msg);
        if (isCompressedPayload(wirePayload)) {
          this.compressedPayloads.add(msg);
          const dictionary = this.dictionaryForRecord(rec);
          if (dictionary !== undefined) this.compressedDictionaries.set(msg, dictionary);
        }
        // The publish-time HMAC signature rides the log record: restore it
        // onto the envelope so a `verifySignature` subscriber checks the
        // replayed message against the same (topic, seq, wire bytes) the
        // publisher signed. `parseLogLine` already validated the shape.
        if (rec.signature !== undefined) {
          msg.signature = { kid: rec.signature.kid, sig: rec.signature.sig };
        }
        // The publisher-identity HMAC signature rides the log record:
        // restore it onto the envelope so a `verifyPublisher`
        // subscriber checks the replayed message against the same
        // (topic, seq, publisherId, wire bytes) the publisher signed.
        if (rec.publisherSignature !== undefined) {
          msg.publisherSignature = {
            kid: rec.publisherSignature.kid,
            sig: rec.publisherSignature.sig,
          };
        }
        if (rec.expiresAt !== undefined) this.messageDeadlines.set(msg, rec.expiresAt);
        // Subscriber-side signature verification (EB-84) applies to replay
        // exactly as to live fan-out: a replayed message that fails
        // verification is dropped — never queued, never delivered —
        // counted and audited. Like the other deliberate replay skips, the
        // key/causal baselines advance past it so ordering gates never hang
        // on a message that will never arrive; unlike a filter skip, the
        // gap baseline does not advance — a failed verification is a
        // genuine loss. Already-expired records skip verification: they
        // were counted as skipped above and the drain drops them as
        // expired — a dead message needs no integrity check.
        if (!ttlExpired && subscriber.verifySignature && !this.deliverySignatureValid(msg)) {
          this.countSignatureFailure(subscriber, msg, msg.signature?.kid);
          if (rec.key !== undefined && rec.keySeq !== undefined) {
            this.skipKeySeq(subscriber, rec.key, rec.keySeq);
          }
          if (rec.causal !== undefined) {
            this.skipCausalClock(subscriber, rec.causal.source, rec.causal.clock);
          }
          countReplaySkipped();
          continue;
        }
        // Publisher-identity verification (EB-97) on replay: same skip
        // semantics as the topic-signature check — key/causal baselines
        // advance, the gap baseline does not (genuine loss).
        if (!ttlExpired && subscriber.verifyPublisher && !this.deliveryPublisherSignatureValid(msg)) {
          this.countSignatureFailure(subscriber, msg, msg.publisherSignature?.kid, 'publisher');
          if (rec.key !== undefined && rec.keySeq !== undefined) {
            this.skipKeySeq(subscriber, rec.key, rec.keySeq);
          }
          if (rec.causal !== undefined) {
            this.skipCausalClock(subscriber, rec.causal.source, rec.causal.clock);
          }
          countReplaySkipped();
          continue;
        }
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
        // An already-expired record was counted as skipped above: it was
        // queued (so the drain still drops it as expired) but never
        // replayed to the handler.
        if (!ttlExpired) {
          countReplayed();
        }
      }
    } catch (err) {
      // An aborting record (e.g. a dictionary-compressed record whose
      // dictionary is not registered — replay stays fail-closed) marks
      // the pass failed and surfaces the error on the observer, then
      // rethrows so the `subscribe` caller still sees the original
      // failure.
      if (progress !== undefined) {
        progress.failed = true;
        const message = err instanceof Error ? err.message : String(err);
        progress.error = message;
        fireReplayProgress(false, true, message);
      }
      throw err;
    }
    if (progress !== undefined) {
      progress.done = true;
      fireReplayProgress(true, false);
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
    validatePublisherId(opts?.publisherId, 'publish');
    validateSchemaVersion(opts?.schemaVersion, 'publish');
    validatePartitionOptions(opts?.partitionKey, opts?.partition, 'publish');
    // Barrier cut marker (EB-98): fail-fast, before anything is mutated.
    validateBarrier(opts?.barrier, 'publish');
    // Binary payload passthrough (EB-92): validated and normalized here
    // — `binary: true` with a non-bytes payload throws `TypeError`
    // before anything is mutated.
    payload = normalizeBinaryPayload(payload, opts?.binary, 'publish');
    const binary = opts?.binary === true;
    // Multi-tenant namespace (EB-52): resolve to the concrete
    // `<namespace>/<topic>` before admission — the full pipeline below
    // (ACL, schema, rate limit, TTL, idempotency, durable log) then
    // operates on the concrete topic, exactly as for a global publish.
    topic = this.resolveNamespaceForPublish(opts?.namespace, topic);
    // Topic registry (EB-77): advisory only — an unregistered topic is
    // counted and surfaced on the opt-in hook, never blocked.
    this.checkTopicRegistered(topic);
    // Barrier control message (EB-98): control-plane, not business data —
    // it takes a dedicated slim fan-out (no seq, no durable log, no
    // retained update, no schema/rate-limit/compression/signing) instead
    // of the full admission pipeline below.
    if (opts?.barrier !== undefined) {
      const accepted = this.fanOutBarrier(topic, payload, opts.barrier.epoch);
      this.scheduleFlush();
      return accepted;
    }
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
      opts?.publisherId,
      opts?.schemaVersion,
      // MQTT-style retained message (EB-87): honored only when a
      // retained rule resolves to enabled for the topic.
      opts?.retain,
      undefined, // pausedExpiresAt: only the pause-buffer drain passes this
      // Publish-side partition routing (EB-90): the explicit partition
      // wins over the partitionKey consistent hash; the range check runs
      // in `fanOut` against the alias-resolved topic.
      opts?.partitionKey,
      opts?.partition,
      binary,
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
   *   and the rate-limit budgets). A first attempt that the schema
   *   validator rejected or either rate limiter shed claims no dedup slot —
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
   * - With `EventBusOptions.durableLogDir` the window survives restarts:
   *   every admitted claim is journaled to `__publish_dedup.jsonl` in the
   *   log directory (best-effort — the in-memory table stays the source of
   *   truth), and a bus opened over the same directory rehydrates the
   *   unexpired claims at construction (see EB-66).
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
    validatePublisherId(opts?.publisherId, 'publishIdempotent');
    validateSchemaVersion(opts?.schemaVersion, 'publishIdempotent');
    validatePartitionOptions(opts?.partitionKey, opts?.partition, 'publishIdempotent');
    // Binary payload passthrough (EB-92): same contract as `publish`.
    payload = normalizeBinaryPayload(payload, opts?.binary, 'publishIdempotent');
    const binary = opts?.binary === true;
    const messageId = opts?.messageId;
    const publisherId = opts?.publisherId;
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
      this.countAliasRetired(topic, payload, publisherId);
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
        opts?.causal,
        publisherId,
        opts?.schemaVersion,
        undefined, // retain: publishIdempotent carries no retain flag
        undefined, // pausedExpiresAt: only the pause-buffer drain passes this
        opts?.partitionKey,
        opts?.partition,
        binary,
      );
      this.scheduleFlush();
      return { duplicate: false, accepted };
    }
    // Authorization precedes identity: a denied publish claims no dedup
    // slot and reports `{ duplicate: false, accepted: 0 }` — it was
    // denied, not deduplicated, and its retry stays a fresh publish.
    if (!this.aclAllowsPublish(topic)) {
      this.countAclDenied(topic, payload, publisherId);
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
      this.countDuplicate(topic, payload, publisherId);
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
      publisherId,
      opts?.schemaVersion,
      undefined, // retain: publishIdempotent carries no retain flag
      undefined, // pausedExpiresAt: only the pause-buffer drain passes this
      opts?.partitionKey,
      opts?.partition,
      binary,
    );
    // EB-27/EB-79 interaction: the dedup gate above runs before `fanOut`,
    // and the per-publisher rate-limit gate lives inside `fanOut` — so a
    // suppressed duplicate is a zero-side-effect suppression that never
    // reaches the gate, and retries burn no publisher budget: the budget
    // is spent only by messages that will actually be published. A
    // publisher-shed first attempt is not admitted, so it claims no dedup
    // slot (see the `admitted` check below) — its retry stays a fresh
    // publish.
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
      // Publish-side dedup journal (EB-66): persist the claim so a restart
      // keeps suppressing within-window retries. Best-effort — the
      // in-memory table is the source of truth, so a failed journal write
      // never fails the publish. The identity is already namespaced to the
      // concrete topic above, so the single root journal covers every
      // namespace.
      this.durableLog?.appendPublishDedup({ topic, messageId, at: nowMs });
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
   *
   * Admission gates apply per entry, exactly as for `publish`: an entry
   * whose publisher is over its per-publisher rate limit (EB-79) is shed
   * on its own — the rest of the batch still publishes.
   */
  publishBatch(messages: Array<BatchMessage>): number {
    if (messages.length === 0) return 0;
    for (const msg of messages) {
      validateMessageKey(msg.key, 'publishBatch');
      validateTraceparent(msg.traceparent, 'publishBatch');
      validateEventTime(msg.eventTime, 'publishBatch');
      validatePublisherId(msg.publisherId, 'publishBatch');
      validateSchemaVersion(msg.schemaVersion, 'publishBatch');
      validatePartitionOptions(msg.partitionKey, msg.partition, 'publishBatch');
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
        undefined, // causal: batch messages carry no causal clock
        msg.publisherId,
        msg.schemaVersion,
        undefined, // retain: batch entries carry no retain flag
        undefined, // pausedExpiresAt: only the pause-buffer drain passes this
        // Publish-side partition routing (EB-90): the entry's `partition`
        // (explicit) wins over `partitionKey` (consistent hash), exactly
        // like `PublishOptions`.
        msg.partitionKey,
        msg.partition,
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
   * 1. Admission: every entry is redacted first (`setTopicRedaction`,
   *    EB-68 — the schema validator and every later gate see the redacted
   *    bytes), then checked against the same admission gates `publish`
   *    applies — the per-publisher rate-limit budget, then the
   *    broker-level ACL, then schema validation, then the
   *    per-topic message-size cap, then the per-topic rate-limit budget —
   *    without mutating any bus
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
   * admission-gate counters (`rejectedMessages` / `rateLimitedMessages` /
   * `publisherRateLimited`)
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
      validatePublisherId(entry.publisherId, 'publishAtomic');
      validateSchemaVersion(entry.schemaVersion, 'publishAtomic');
      validatePartitionOptions(entry.partitionKey, entry.partition, 'publishAtomic');
    }
    // Phase 1: admit the whole batch against shadow state.
    const shadowBudget = new Map<string, number>();
    // Per-publisher rate-limit reservations for this batch (EB-79): a
    // batch cannot overdraft one publisher's bucket with its own entries.
    const publisherShadowBudget = new Map<string, number>();
    // Per-key sequence numbers are drawn in entry order (publish order)
    // against a shadow cursor: a rejected batch leaves the live cursors —
    // and every subscriber's key baseline — exactly untouched.
    const shadowKeyCursors = new Map<string, number>();
    const keySeqs: Array<number | undefined> = new Array(entries.length);
    // Alias-resolved topics, one per entry: the commit phase publishes the
    // resolved topics, never the raw entry topics.
    const resolvedTopics: string[] = new Array(entries.length);
    // Redacted payloads, one per entry: shadow admission redacts on the
    // resolved topic before the schema gate (EB-68); the commit phase
    // fans out these redacted bytes through pre-admitted fanOut, so the
    // redaction count moves exactly once per entry.
    const redactedPayloads: unknown[] = new Array(entries.length);
    for (let index = 0; index < entries.length; index++) {
      const { topic, key } = entries[index];
      let payload: unknown = entries[index].payload;
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
      // Publish-side partition routing (EB-90): the range check runs here,
      // in admission — an out-of-range explicit partition aborts the batch
      // before the commit phase, so a throw can never leave a
      // half-committed batch behind. The commit phase re-resolves the
      // same request against the same resolved topic (deterministic), so
      // no per-entry partition needs storing.
      this.resolvePartition(resolvedTopic, entries[index].partitionKey, entries[index].partition);
      // Sensitive-field redaction (EB-68) runs in shadow admission, on
      // the resolved topic, before the schema gate: the schema verdict,
      // the size cap, and the commit phase all see the redacted bytes.
      payload = this.redactAdmissionPayload(resolvedTopic, payload);
      redactedPayloads[index] = payload;
      const reason = this.admissionVerdict(
        resolvedTopic,
        payload,
        shadowBudget,
        entries[index].publisherId,
        publisherShadowBudget,
        entries[index].schemaVersion,
      );
      if (reason !== undefined) {
        // The batch is rejected on the failing entry: count it once against
        // that entry's admission-gate counters and surface it on the unified
        // admission-rejection hook, so an atomic rejection stays reconcilable
        // with stats like any other admission rejection. Everything else
        // stays untouched: no sequence numbers, no rate-limit tokens, no
        // durable-log writes.
        const publisherId = entries[index].publisherId;
        if (reason === 'schema') this.countSchemaRejection(topic, payload, publisherId);
        else if (reason === 'acl') this.countAclDenied(topic, payload, publisherId);
        else if (reason === 'alias-retired') this.countAliasRetired(topic, payload, publisherId);
        else if (reason === 'message-too-big') this.countMessageTooBig(topic, payload, publisherId);
        else if (reason === 'publisher-rate-limit')
          this.countPublisherRateLimited(topic, payload, publisherId);
        else if (reason === 'topic-paused') this.countTopicPaused(topic, payload, publisherId);
        else this.countRateLimited(topic, payload, publisherId);
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
      const { key, traceparent, messageId, eventTime, publisherId, schemaVersion, partitionKey, partition } =
        entries[index];      // Deprecated-schema accounting (EB-86): the shadow admission ran
      // the validator, so the commit only re-resolves the version
      // bookkeeping — an aborted batch never reaches here, so every
      // committed deprecated admission is counted exactly once.
      if (this.isDeprecatedSchemaAdmission(resolvedTopics[index], schemaVersion)) {
        this.countDeprecatedSchema(resolvedTopics[index]);
      }
      this.fanOut(
        resolvedTopics[index],
        // The shadow-admission redacted bytes: the commit phase is
        // pre-admitted and must not redact again (EB-68).
        redactedPayloads[index],
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
        undefined, // causal: atomic entries carry no causal clock
        publisherId,
        schemaVersion,
        undefined, // retain: atomic entries carry no retain flag
        undefined, // pausedExpiresAt: only the pause-buffer drain passes this
        // Publish-side partition routing (EB-90): the request validated
        // in admission re-resolves here deterministically — it cannot
        // throw at commit.
        partitionKey,
        partition,
      );
    }
    this.scheduleFlush();
    return { published: entries.length };
  }

  /**
   * Runs the admission gate of the publish pipeline — the per-publisher
   * rate-limit budget first (EB-79), then schema validation
   * (rejections happen before admission: no seq, no rate-limit
   * budget), then the per-topic message-size cap, then the per-topic
   * rate-limit budget — WITHOUT mutating any
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
   * `publisherShadowBudget` does the same per publisher identity, so a
   * batch cannot overdraft one publisher's bucket with its own entries;
   * publishers without a configured limit are skipped.
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
    publisherId: string | undefined,
    publisherShadowBudget: Map<string, number>,
    schemaVersion: number | undefined,
  ): AtomicRejectReason | undefined {
    // Topic pause (EB-88) is the first shadow gate, mirroring `fanOut`:
    // any paused entry — shed or queue mode — rejects the whole batch. A
    // queued message cannot be committed synchronously, so atomicity
    // forces the batch to fail rather than partially buffer; the entry's
    // reason is 'topic-paused' either way.
    if (this.pauseForTopic(topic) !== undefined) return 'topic-paused';
    // Per-publisher rate limiting (EB-79) is the second shadow gate, as in
    // `fanOut`: the peek/reserve mirrors `takePublisherToken` without
    // mutating the live bucket, and the shadow bucket is never stored —
    // a rejected batch creates no publisher budget state either.
    if (publisherId !== undefined) {
      const publisherLimit = this.publisherRateLimitRules.get(publisherId);
      if (publisherLimit !== undefined) {
        let bucket = this.publisherRateLimitBuckets.get(publisherId);
        if (bucket == null) {
          bucket = new TokenBucket(publisherLimit.burst, publisherLimit.messagesPerSec, this.now);
        }
        const reserved = publisherShadowBudget.get(publisherId) ?? 0;
        if (bucket.availableTokens - reserved < 1) return 'publisher-rate-limit';
        publisherShadowBudget.set(publisherId, reserved + 1);
      }
    }
    // The ACL is the second admission gate everywhere (after topic-alias
    // resolution, which rewrites the topic before the gates), including
    // the atomic batch's shadow admission: an unauthorized entry aborts
    // the batch.
    if (!this.aclAllowsPublish(topic)) return 'acl';
    // Versioned schema validators (EB-86): the shadow admission runs the
    // pure verdict — no counting here. A rejection is counted once by the
    // caller; a deprecated admission is counted in the commit phase (see
    // `isDeprecatedSchemaAdmission`), so an aborted batch never moves the
    // counter for messages that were never published.
    if (!this.schemaVerdict(topic, payload, schemaVersion).admitted) return 'schema';
    // Per-message size cap, same position as in `fanOut` (after schema, and
    // before any sequence number or rate-limit budget is touched): an
    // oversized entry aborts the batch like a schema rejection.
    const maxBytes = this.maxMessageBytesForTopic(topic);
    if (maxBytes !== undefined && payloadByteSize(payload) > maxBytes) return 'message-too-big';
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
   * - Sensitive-field redaction (`setTopicRedaction`, EB-68) runs NOW, on
   *   the resolved topic, before the fail-fast gates: the schema
   *   validator, the size cap, the in-memory schedule, and the persisted
   *   schedule record all see the redacted payload — the original values
   *   never reach the durable log, so replay can never resurrect them.
   *   The due-time fan-out (pre-admitted) does not redact again.
   * - Schema validation runs NOW, fail-fast: a rejected payload never
   *   becomes a scheduled delivery. A throwing validator propagates,
   *   exactly as in `publish`. Validation is not re-run at fan-out
   *   (validators are expected pure — the same contract `publishAtomic`
   *   relies on), so a payload accepted here is never rejected at its due
   *   time.
   * - The per-topic message-size cap (`setTopicMaxMessageBytes`) also runs
   *   NOW, fail-fast, right after schema validation: an oversized payload
   *   never becomes a scheduled delivery and is not re-checked at fan-out.
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
    // The producer label is validated before any fail-fast gate: a throw
    // must leave zero state behind — no schedule, no counted rejection.
    validatePublisherId(opts?.publisherId, 'publishDelayed');
    validateSchemaVersion(opts?.schemaVersion, 'publishDelayed');
    const publisherId = opts?.publisherId;
    // Topic aliases (EB-44) resolve before every other gate: the schedule
    // is filed under the resolved topic — the fail-fast ACL/schema checks
    // below, the stamped TTL, and the fan-out at due time all see it. A
    // publish to a retired old topic never becomes a scheduled delivery.
    const aliasResolution = this.resolvePublishTopic(topic);
    if (aliasResolution.retired) {
      this.countAliasRetired(topic, payload, publisherId);
      return undefined;
    }
    topic = aliasResolution.topic;
    // Sensitive-field redaction (EB-68) at schedule time: the fail-fast
    // gates below, the in-memory schedule, and the persisted schedule
    // record all see the redacted payload — the original values never
    // reach the durable log, and the due-time fan-out (pre-admitted) does
    // not redact again. Redacting at schedule time (rather than at
    // fan-out) is what keeps the persisted bytes redacted.
    payload = this.redactAdmissionPayload(topic, payload);
    // Topic pause (EB-88): the first schedule-time gate, before the
    // per-publisher rate limiter. In shed mode the schedule is refused
    // fail-fast — no id, nothing in the heap, nothing on disk — counted
    // like a shed with reason 'topic-paused' (no sequence number: delayed
    // schedules never consume one before fan-out). In queue mode the
    // schedule is accepted normally; the due-time sweep defers the
    // fan-out into the pause buffer while the pause is still active.
    const pauseRule = this.pauseForTopic(topic);
    if (pauseRule !== undefined && pauseRule.mode === 'shed') {
      this.countTopicPaused(topic, payload, publisherId);
      return undefined;
    }
    // Fail fast on the per-publisher rate limit (EB-79): an over-budget
    // publisher never becomes a scheduled delivery — no id, nothing in
    // the heap, nothing on disk. The token is spent at schedule time, so
    // the due-time fan-out (pre-admitted) neither re-checks nor burns
    // budget again. Counted the same way a `publish` publisher-rate-limit
    // shed is, with reason 'publisher-rate-limit'.
    //
    // Design intent (EB-88): the topic-pause check above runs before this
    // gate — a schedule for a paused topic never takes a publisher token,
    // the same ordering as the live publish path.
    if (this.takePublisherToken(publisherId) !== undefined) {
      this.countPublisherRateLimited(topic, payload, publisherId);
      return undefined;
    }
    // Fail fast on ACL: an unauthorized publish never becomes a scheduled
    // delivery — no id, nothing in the heap, nothing on disk. Counted the
    // same way a `publish` ACL denial is.
    if (!this.aclAllowsPublish(topic)) {
      this.countAclDenied(topic, payload, publisherId);
      return undefined;
    }
    // Fail fast on schema: a rejected payload never becomes a scheduled
    // delivery — no id, nothing in the heap, nothing on disk. Versioned
    // validators (EB-86) resolve the requested version here; a
    // deprecated-version admission is counted now (the due-time fan-out
    // is pre-admitted and never recounts). Counted the same way a
    // `publish` rejection is.
    if (!this.applySchemaVerdict(topic, payload, opts?.schemaVersion, publisherId)) {
      return undefined;
    }
    // Fail fast on the per-topic message-size cap: an oversized payload
    // never becomes a scheduled delivery — same zero-side-effect semantics
    // as the schema fail-fast above.
    const maxBytes = this.maxMessageBytesForTopic(topic);
    if (maxBytes !== undefined && payloadByteSize(payload) > maxBytes) {
      this.countMessageTooBig(topic, payload, publisherId);
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
      // The producer label rides the schedule record so a restart rebuilds
      // the timer with the attribution intact.
      publisherId,
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
        // The producer label rides the schedule record so the rebuilt
        // timer fans out with the original attribution (see
        // `DelayedEntry.publisherId`).
        ...(publisherId === undefined ? {} : { publisherId }),
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
   * Cancels every pending delayed delivery scheduled for one topic or
   * topic pattern (EB-89): the messages will never fan out. Returns
   * `{ cancelled, delayIds }` — the number of schedules cancelled and
   * their ids in schedule order.
   *
   * Only pending, not-yet-fanned-out schedules are cancelled: entries
   * that already fanned out, already expired, or were already cancelled
   * are not counted. Topic matching takes an exact concrete topic for a
   * direct match, or a wildcard pattern (the same syntax as `subscribe`)
   * that expands to every concrete topic with pending schedules it
   * matches.
   *
   * Each cancellation reuses `cancelDelayed`, so every cancelled schedule
   * is tombstoned to the durable log (the EB-24 convention) — a restart
   * never resurrects them — and keyed schedules release their subscribers'
   * per-key expectations. An empty result returns `{ cancelled: 0,
   * delayIds: [] }` — never a throw. Recorded in the subscription audit
   * log as `delayed_cancelled` (`{ cancelled, delayIds, topicPattern }`).
   *
   * Throws `TypeError` when `topicPattern` is not a string, `RangeError`
   * when it is empty.
   */
  cancelDelayedByTopic(topicPattern: string): { cancelled: number; delayIds: string[] } {
    if (typeof topicPattern !== 'string') {
      throw new TypeError('cancelDelayedByTopic: topicPattern must be a string');
    }
    if (topicPattern.length === 0) {
      throw new RangeError('cancelDelayedByTopic: topicPattern must be a non-empty string');
    }
    const matcher = compilePattern(topicPattern);
    const delayIds: string[] = [];
    // `delayedById` holds exactly the pending schedules, in schedule
    // order; deleting during iteration is safe on a Map. `cancelDelayed`
    // can only return false for an id that left the pending set, which
    // cannot happen mid-iteration here — the guard is defensive.
    for (const [delayId, entry] of this.delayedById) {
      if (entry.topic === topicPattern || matcher.test(entry.topic)) {
        if (this.cancelDelayed(delayId)) delayIds.push(delayId);
      }
    }
    this.auditSubscription({
      action: 'delayed_cancelled',
      actor: 'system',
      pattern: topicPattern,
      detail: { cancelled: delayIds.length, delayIds, topicPattern },
    });
    return { cancelled: delayIds.length, delayIds };
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
      // Topic pause (EB-88) at due time: the pause gate is live admission
      // state, so a schedule accepted before the pause still honors it
      // when its due time arrives during the window. In shed mode the
      // entry is shed here — consuming a sequence number (gap-visible,
      // like the EB-19 fan-out shed) — counted with reason
      // 'topic-paused' and tombstoned so a restart never resurrects it.
      // In queue mode the fan-out is deferred: the entry moves into the
      // pause buffer (keeping the TTL deadline stamped at schedule time)
      // and drains in order at resume. Either way the delay schedule is
      // closed — it never fans out twice.
      const pauseRule = this.pauseForTopic(top.topic);
      if (pauseRule !== undefined) {
        if (pauseRule.mode === 'shed') {
          const stats = this.statsFor(top.topic);
          stats.publishedMessages += 1;
          // Delayed schedules carry no partition routing (EB-90): the
          // shed consumes a partition-0 sequence number, like every
          // other unpartitioned publish.
          const shedSeq = this.nextSeq(top.topic, 0);
          stats.lastSeq = Math.max(stats.lastSeq, shedSeq);
          this.totalPublished += 1;
          this.countPartitionPublish(top.topic, 0, shedSeq);
          if (top.key !== undefined && top.keySeq !== undefined) {
            this.releaseKeySequence(top.key, top.keySeq);
            fannedOut = true;
          }
          this.countTopicPaused(top.topic, top.payload, top.publisherId);
        } else {
          this.bufferPausedMessage(pauseRule, top.topic, top.payload, {
            key: top.key,
            traceparent: top.traceparent,
            messageId: top.messageId,
            publisherId: top.publisherId,
            // The schedule-time deadline rides along verbatim — the
            // message was "published" at schedule time, not at deferral.
            expiresAtOverride: top.expiresAt,
          });
        }
        this.appendDelayTombstone(top.id, top.topic);
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
        false, // routed: a delayed fan-out re-enters the routing table like a fresh publish
        undefined, // routedExpiresAt
        false, // fromBridge
        false, // diagnostic
        undefined, // eventTime: delayed schedules carry no event-time stamp
        undefined, // causal: delayed schedules carry no causal clock
        top.publisherId, // the schedule's producer attribution survives the timer
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
   * Writes a tombstone closing a pause-buffered message (drained at
   * resume or dropped as expired) to the durable log, when one is
   * enabled. Restart recovery treats a tombstoned `pauseId` as resolved
   * and never rebuilds it — the same convention as the delayed-delivery
   * tombstones. Best-effort like all durable-log writes: a failed append
   * never throws into the caller.
   */
  private appendPauseTombstone(pauseId: string, topic: string): void {
    this.durableLogForTopic(topic)?.append({ seq: 0, topic, at: this.now(), paused: true, pauseId, cancelled: true, payload: null });
  }

  /**
   * Holds one publish in a queue-mode pause rule's durable-side buffer
   * (EB-88). The payload arrives admission-redacted (the caller redacts
   * before this runs — `fanOut` redacts before its gates, `publishDelayed`
   * at schedule time); nothing else about admission has run, so no
   * sequence number, no schema verdict, no rate-limit budget is burned
   * here — the message gets its full admission when it fans out at
   * resume. The TTL deadline is stamped NOW (pause admission), and the
   * buffer record carries it verbatim so resume preserves the original
   * deadline instead of recomputing one.
   *
   * `expiresAtOverride` is for the delayed-delivery sweep transfer: a
   * delayed message deferred into the pause buffer keeps the deadline
   * stamped at its schedule time, not a fresh one.
   *
   * The buffer record is persisted before the entry is visible anywhere
   * (queue mode requires the durable log for exactly this reason): a
   * crash between pause and resume rebuilds the buffer instead of losing
   * the messages.
   */
  private bufferPausedMessage(
    rule: { mode: TopicPauseMode; buffer: PauseBufferEntry[] },
    topic: string,
    payload: unknown,
    opts: {
      key?: string;
      traceparent?: string;
      messageId?: string;
      eventTime?: number;
      causal?: { source?: string; clock: number };
      publisherId?: string;
      schemaVersion?: number;
      retain?: boolean;
      routed?: boolean;
      fromBridge?: boolean;
      diagnostic?: boolean;
      expiresAtOverride?: number;
      /** Binary-payload marker (EB-92), propagated to the buffer entry. */
      binary?: boolean;
    },
  ): void {
    const nowMs = this.now();
    const ttlMs = this.ttlForTopic(topic);
    // Only non-empty message ids participate in dedup, mirroring
    // `fanOut`'s `cleanMessageId` normalization.
    const messageId =
      typeof opts.messageId === 'string' && opts.messageId.length > 0 ? opts.messageId : undefined;
    const entry: PauseBufferEntry = {
      pauseId: `paused-${++this.nextPauseId}`,
      pauseOrder: ++this.nextPauseOrder,
      topic,
      payload,
      expiresAt:
        opts.expiresAtOverride !== undefined
          ? opts.expiresAtOverride
          : ttlMs === undefined
            ? undefined
            : nowMs + ttlMs,
      ...(opts.key === undefined ? {} : { key: opts.key }),
      ...(opts.traceparent === undefined ? {} : { traceparent: opts.traceparent }),
      ...(messageId === undefined ? {} : { messageId }),
      ...(opts.eventTime === undefined ? {} : { eventTime: opts.eventTime }),
      ...(opts.causal === undefined ? {} : { causal: opts.causal }),
      ...(opts.publisherId === undefined ? {} : { publisherId: opts.publisherId }),
      ...(opts.schemaVersion === undefined ? {} : { schemaVersion: opts.schemaVersion }),
      ...(opts.retain === undefined ? {} : { retain: opts.retain }),
      ...(opts.routed === undefined ? {} : { routed: opts.routed }),
      ...(opts.fromBridge === undefined ? {} : { fromBridge: opts.fromBridge }),
      ...(opts.diagnostic === undefined ? {} : { diagnostic: opts.diagnostic }),
      // Binary payloads (EB-92): the marker rides the in-memory entry
      // and the pause journal, so the resume fan-out — in this process
      // or after a restart — stays byte-oriented.
      ...(opts.binary === true ? { binary: true } : {}),
    };
    // Internal-only flags (`routed`, `fromBridge`, `diagnostic`) and the
    // traceparent are runtime state: they ride the in-memory entry but are
    // not persisted — recovery fans the message out as an ordinary
    // publish (documented on `PauseBufferEntry`).
    this.durableLogForTopic(topic)?.append({
      seq: 0,
      topic,
      at: nowMs,
      paused: true,
      pauseId: entry.pauseId,
      pauseOrder: entry.pauseOrder,
      expiresAt: entry.expiresAt,
      payload,
      // Binary payloads (EB-92) persist as v2 lines so restart recovery
      // restores raw bytes.
      ...(entry.binary === true ? { binary: true } : {}),
      ...(entry.key === undefined ? {} : { key: entry.key }),
      ...(entry.messageId === undefined ? {} : { messageId: entry.messageId }),
      ...(entry.publisherId === undefined ? {} : { publisherId: entry.publisherId }),
      ...(entry.eventTime === undefined ? {} : { eventTime: entry.eventTime }),
      ...(entry.causal === undefined ? {} : { causal: entry.causal }),
      ...(entry.schemaVersion === undefined ? {} : { schemaVersion: entry.schemaVersion }),
    });
    rule.buffer.push(entry);
  }

  /**
   * Fans out every message buffered under a pause rule in original
   * (publish) order — called by `resumeTopic` after the rule is removed,
   * so the entries re-enter the full admission pipeline (including the
   * pause gate itself: a message whose topic still matches another active
   * pause rule is shed or re-buffered under that rule). Each entry keeps
   * the TTL deadline stamped at pause admission (`pausedExpiresAt`):
   * an entry whose deadline passed while buffered is counted expired and
   * never delivered (the EB-32 precedent — never resurrected). Buffered
   * idempotent messages re-run the dedup suppression gate and claim
   * their slot only when admitted, mirroring `publishIdempotent`.
   *
   * Every entry is tombstoned as it leaves the buffer, so a restart
   * cannot rebuild — and re-deliver — it. Returns the number of entries
   * fanned out (admitted or shed — both left the buffer; expired and
   * duplicate-suppressed entries drained without fan-out).
   */
  private drainPauseBuffer(rule: { mode: TopicPauseMode; buffer: PauseBufferEntry[] }): number {
    // Splice the buffer up front: re-entrant fan-out below can buffer
    // into *another* rule's buffer, but never back into this one (the
    // rule is already removed from `pauseRules`).
    const entries = rule.buffer.splice(0);
    if (entries.length === 0) return 0;
    const nowMs = this.now();
    let drained = 0;
    for (const entry of entries) {
      if (entry.expiresAt !== undefined && nowMs >= entry.expiresAt) {
        // Expired while buffered: counted, never delivered, tombstoned.
        this.recordExpired(entry.topic);
        this.appendPauseTombstone(entry.pauseId, entry.topic);
        continue;
      }
      // Idempotent suppression, mirroring `publishIdempotent`'s gate: a
      // retry buffered twice still delivers once. The slot is claimed
      // only by an admitted fan-out below.
      let suppressed = false;
      if (entry.messageId !== undefined) {
        const dedupKey = `${entry.topic}\0${entry.messageId}`;
        this.pruneDedup(nowMs);
        const firstAt = this.dedup.get(dedupKey);
        if (firstAt !== undefined && nowMs - firstAt < this.idempotencyWindowMs) {
          this.countDuplicate(entry.topic, entry.payload, entry.publisherId);
          suppressed = true;
        }
      }
      if (!suppressed) {
        // Full admission at resume: schema, rate-limit budget, and
        // sequence numbers are all drawn here — none were burned at pause
        // time. `pausedExpiresAt` preserves the pause-time TTL deadline.
        const { admitted } = this.fanOut(
          entry.topic,
          entry.payload,
          false, // preAdmitted
          undefined, // delayed
          entry.key,
          undefined, // preassignedKeySeq: drawn at resume, in drain order
          entry.traceparent,
          entry.messageId,
          entry.routed ?? false,
          undefined, // routedExpiresAt
          entry.fromBridge ?? false,
          entry.diagnostic ?? false,
          entry.eventTime,
          entry.causal,
          entry.publisherId,
          entry.schemaVersion,
          entry.retain,
          entry.expiresAt, // pausedExpiresAt: the original TTL deadline, verbatim
          undefined, // partitionKey
          undefined, // partition
          entry.binary ?? false,
        );
        if (admitted && entry.messageId !== undefined) {
          // Claim the dedup slot exactly like `publishIdempotent`: only
          // an admitted message suppresses its retry.
          const dedupKey = `${entry.topic}\0${entry.messageId}`;
          this.dedup.delete(dedupKey);
          this.dedup.set(dedupKey, nowMs);
          while (this.dedup.size > this.idempotencyMaxEntries) {
            const oldest = this.dedup.keys().next();
            if (oldest.done) break;
            this.dedup.delete(oldest.value);
          }
          this.durableLog?.appendPublishDedup({ topic: entry.topic, messageId: entry.messageId, at: nowMs });
        }
        drained += 1;
      }
      this.appendPauseTombstone(entry.pauseId, entry.topic);
    }
    this.scheduleFlush();
    return drained;
  }

  /**
   * Records an admitted `retain: true` publish as the concrete topic's
   * retained message (EB-87). Called from `fanOut` after the message is
   * fully admitted and stamped — a rejected or shed publish never
   * reaches here, so it can never touch the retained store.
   *
   * The rule gate runs first: without a rule resolving to `true` the
   * retain flag is ignored and the publish stays an ordinary publish.
   * An empty payload clears the retained message instead of storing it
   * (see `PublishOptions.retain` for the "empty" definition). Otherwise
   * the latest value wins — the previous retained message for the topic
   * is replaced.
   *
   * The stored envelope is the wire form (compressed bytes when a
   * compression rule applied, plus the HMAC signature when a signing
   * rule matched): snapshot delivery re-registers it for transparent
   * inflation and verifies the signature against the same bytes the
   * publisher signed, exactly like durable-log replay. The state is
   * journaled as a seq-0 retained record (the delayed-delivery
   * convention) so a restart rebuilds the table; best-effort like all
   * durable-log writes.
   */
  private updateRetained(
    topic: string,
    msg: BusMessage,
    rawPayload: unknown,
    nowMs: number,
    expiresAt: number | undefined,
    dictId: string | undefined,
  ): void {
    if (this.retainedForTopic(topic) !== true) return;
    if (payloadByteSize(rawPayload) === 0) {
      this.clearRetained(topic, nowMs);
      return;
    }
    const signature =
      msg.signature === undefined
        ? undefined
        : { kid: msg.signature.kid, sig: msg.signature.sig };
    const publisherSignature =
      msg.publisherSignature === undefined
        ? undefined
        : { kid: msg.publisherSignature.kid, sig: msg.publisherSignature.sig };
    const binary = this.binaryMessages.has(msg);
    const entry: RetainedEntry = {
      topic,
      payload: msg.payload,
      seq: msg.seq,
      ...(binary ? { binary: true } : {}),
      // The assigned partition rides the retained entry (EB-90): the
      // snapshot keeps the message's publish-order identity per
      // partition for gap detection.
      ...(msg.partition === undefined ? {} : { partition: msg.partition }),
      at: nowMs,
      ...(expiresAt === undefined ? {} : { expiresAt }),
      ...(dictId === undefined ? {} : { dictId }),
      ...(signature === undefined ? {} : { signature }),
      ...(publisherSignature === undefined ? {} : { publisherSignature }),
      ...(msg.messageId === undefined ? {} : { messageId: msg.messageId }),
      ...(msg.publisherId === undefined ? {} : { publisherId: msg.publisherId }),
      ...(msg.eventTime === undefined ? {} : { eventTime: msg.eventTime }),
    };
    this.retainedMessages.set(topic, entry);
    this.durableLogForTopic(topic)?.append({
      seq: 0,
      topic,
      at: nowMs,
      retained: true,
      retainedSeq: msg.seq,
      payload: msg.payload,
      // Binary payloads (EB-92) persist as v2 lines so restart recovery
      // restores raw bytes.
      ...(binary ? { binary: true } : {}),
      // The assigned partition rides the seq-0 retained record (EB-90)
      // so restart recovery rebuilds the table with the message's
      // partition identity intact.
      ...(msg.partition === undefined ? {} : { partition: msg.partition }),
      ...(expiresAt === undefined ? {} : { expiresAt }),
      ...(dictId === undefined ? {} : { dictId }),
      ...(signature === undefined ? {} : { signature }),
      ...(publisherSignature === undefined ? {} : { publisherSignature }),
      ...(msg.messageId === undefined ? {} : { messageId: msg.messageId }),
      ...(msg.publisherId === undefined ? {} : { publisherId: msg.publisherId }),
      ...(msg.eventTime === undefined ? {} : { eventTime: msg.eventTime }),
    });
  }

  /**
   * Clears the concrete topic's retained message (EB-87): drops it from
   * the live table and journals a seq-0 tombstone so a restart does not
   * resurrect the cleared value (the delayed-delivery tombstone
   * convention). The tombstone is written even when the live table held
   * no entry — after a restart the table is rebuilt from disk, and a
   * clear must still close the on-disk record. Audited as
   * `retained_cleared` when an entry actually existed.
   */
  private clearRetained(topic: string, nowMs: number): void {
    const existed = this.retainedMessages.delete(topic);
    this.durableLogForTopic(topic)?.append({
      seq: 0,
      topic,
      at: nowMs,
      retained: true,
      cancelled: true,
      payload: null,
    });
    if (existed) {
      this.auditSubscription({
        action: 'retained_cleared',
        actor: 'system',
        topic,
        detail: { via: 'publish' },
      });
    }
  }

  /**
   * Hands a late-joining subscriber its retained snapshot (EB-87): the
   * latest retained message of every matching concrete topic, delivered
   * synchronously at subscribe time — before any replay
   * (`resumeFromSeq` / `resumeFromTime`) and before any live message —
   * so ordering is guaranteed by construction.
   *
   * Snapshot deliveries bypass the backpressure queue entirely: they
   * burn no backpressure budget and occupy no queue capacity — the
   * handler is invoked directly. Content filters, composite
   * include/exclude patterns, and `verifySignature` apply exactly as on
   * the live path (verified against the wire bytes, before inflation);
   * keyed/causal ordering gates are intentionally bypassed — the
   * snapshot is a point-in-time catch-up, and the first live keyed or
   * causal message seeds those gates fresh, so nothing can hang on an
   * unseen predecessor. Each delivered message carries
   * `retained: true`, establishes the per-topic gap baseline like a
   * first delivery (a late joiner never counts pre-subscription messages
   * as gaps), and counts into `deliveredMessages` like replayed
   * messages do. A throwing handler is isolated — the flush loop never
   * lets a handler throw escape either.
   *
   * Shadow mirrors are skipped: like replay (which shadows reject), a
   * retained snapshot is history, not live traffic. Group members each
   * receive their own snapshot — mirroring per-member replay — so every
   * competing set still sees each retained message exactly once per
   * joining member.
   *
   * The delivered (topic, seq) pairs are recorded on the subscriber so
   * the replay pass skips those exact records — the snapshot came first,
   * it must never be delivered twice.
   */
  private deliverRetainedSnapshot(subscriber: Subscriber): void {
    // Deferred for reliable subscribers: `subscribeReliable` installs
    // the at-least-once wrapper after `subscribe` returns; it performs
    // the snapshot itself through the ready wrapper (tracked
    // deliveries).
    if (this.retainedSnapshotDeferred) return;
    if (this.retainedMessages.size === 0) return;
    if (subscriber.shadow !== undefined) return;
    const nowMs = this.now();
    // Lazy TTL prune: a retained message past its deadline is gone — the
    // live drain would drop it as expired, so the snapshot never
    // resurrects it. Restart recovery applies the same rule.
    for (const [topic, entry] of this.retainedMessages) {
      if (entry.expiresAt !== undefined && entry.expiresAt <= nowMs) {
        this.retainedMessages.delete(topic);
      }
    }
    const matched: RetainedEntry[] = [];
    for (const entry of this.retainedMessages.values()) {
      if (!this.topicMatchesWithAliases(subscriber.matcher, entry.topic, entry.topic)) continue;
      const composite = subscriber.composite;
      if (composite !== undefined && !this.matchesComposite(composite, entry.topic)) continue;
      matched.push(entry);
    }
    if (matched.length === 0) return;
    // Deterministic snapshot order: publish time, then topic.
    matched.sort((a, b) => a.at - b.at || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    const outgoing: BusMessage[] = [];
    const delivered = new Map<string, number>();
    for (const entry of matched) {
      const msg: BusMessage = {
        topic: entry.topic,
        payload: entry.payload,
        seq: entry.seq,
        retained: true,
        // The retained message's partition (EB-90): the snapshot keeps
        // the publish-order identity its seq was drawn from, so gap
        // detection keys the snapshot against the right partition.
        ...(entry.partition === undefined ? {} : { partition: entry.partition }),
        ...(entry.messageId === undefined ? {} : { messageId: entry.messageId }),
        ...(entry.publisherId === undefined ? {} : { publisherId: entry.publisherId }),
        ...(entry.eventTime === undefined ? {} : { eventTime: entry.eventTime }),
        ...(entry.signature === undefined
          ? {}
          : { signature: { kid: entry.signature.kid, sig: entry.signature.sig } }),
        ...(entry.publisherSignature === undefined
          ? {}
          : {
              publisherSignature: {
                kid: entry.publisherSignature.kid,
                sig: entry.publisherSignature.sig,
              },
            }),
      };
      // The table stores the wire payload: re-register a compressed
      // envelope for transparent inflation at delivery, exactly like
      // replay. A missing preset dictionary fails closed here, like
      // replay — never silently raw. A binary entry re-marks the
      // envelope so delivery inflates to bytes and verifies signatures
      // against the raw bytes.
      if (entry.binary === true) this.binaryMessages.add(msg);
      if (isCompressedPayload(entry.payload)) {
        this.compressedPayloads.add(msg);
        if (entry.dictId !== undefined) {
          const dictionary = this.compressionDictionaryRegistry.get(entry.dictId);
          if (dictionary !== undefined) this.compressedDictionaries.set(msg, dictionary);
        }
      }
      // Envelope signature verification (EB-84) applies to the snapshot
      // exactly as to live fan-out and replay: checked on the wire bytes
      // before inflation — a failed check drops the snapshot copy,
      // counted and audited.
      if (subscriber.verifySignature && !this.deliverySignatureValid(msg)) {
        this.countSignatureFailure(subscriber, msg, msg.signature?.kid);
        continue;
      }
      // Publisher-identity verification (EB-97) applies to the snapshot
      // exactly as to live fan-out and replay: checked on the wire bytes
      // before inflation — a failed check drops the snapshot copy,
      // counted and audited (`detail.kind: 'publisher'`).
      if (subscriber.verifyPublisher && !this.deliveryPublisherSignatureValid(msg)) {
        this.countSignatureFailure(subscriber, msg, msg.publisherSignature?.kid, 'publisher');
        continue;
      }
      this.inflateMessagePayload(msg);
      const filter = subscriber.filter;
      if (filter !== undefined && !filter(msg.payload, msg.topic)) {
        // Content-filtered: seen and deliberately skipped — never queued
        // (no backpressure budget), never a sequence gap.
        this.countFiltered(subscriber, msg.topic, msg.seq, '', msg.partition ?? 0);
        continue;
      }
      this.noteMessageDelivered(subscriber, msg);
      this.totalDelivered += 1;
      delivered.set(entry.topic, entry.seq);
      outgoing.push(msg);
    }
    if (outgoing.length === 0) return;
    subscriber.retainedDelivered = delivered;
    // Direct handler invocation — never the backpressure queue.
    // Synchronous, so every later live message lands after the snapshot.
    // A throwing handler is isolated exactly like the flush loop
    // isolates a throwing live handler; only a clean return counts as
    // idle activity (reliable subscribers record inside their
    // at-least-once wrapper instead).
    const invokeQuietly = (invoke: () => void): boolean => {
      try {
        invoke();
        return true;
      } catch {
        return false;
      }
    };
    const noteActivity = (): void => {
      if (subscriber.reliable === undefined) this.recordIdleActivity(subscriber, 'deliver');
    };
    if (subscriber.batch !== undefined) {
      if (
        invokeQuietly(() => (subscriber.handler as unknown as (msgs: BusMessage[]) => void)(outgoing))
      ) {
        noteActivity();
      }
    } else {
      for (const msg of outgoing) {
        if (invokeQuietly(() => subscriber.handler(msg))) noteActivity();
      }
    }
  }

  /**
   * Rebuilds the retained-message table from one durable log after a
   * restart (EB-87). Per topic the latest retained record wins; a
   * tombstone (`cancelled: true`) or an entry whose TTL deadline passed
   * while the process was down rebuilds nothing. Delayed-delivery
   * schedule records are ignored here (they carry no `retained` marker).
   */
  private recoverRetainedFromLog(log: DurableTopicLog): void {
    const nowMs = this.now();
    for (const topic of log.topics()) {
      let latest: DurableLogRecord | undefined;
      // `readSince(topic, -1)`: the exclusive bound sits below every
      // real seq, so the seq-0 retained records and tombstones come along
      // too. File order means the last retained record wins.
      for (const rec of log.readSince(topic, -1)) {
        if (rec.retained !== true) continue;
        latest = rec;
      }
      if (latest === undefined || latest.cancelled === true) continue;
      if (latest.expiresAt !== undefined && latest.expiresAt <= nowMs) continue;
      // The parser guarantees `retainedSeq` on a non-tombstone retained
      // record; guard anyway — a rebuilt table must never carry seq 0.
      if (latest.retainedSeq === undefined) continue;
      this.retainedMessages.set(topic, {
        topic,
        // A binary record's v2 bytes may encode a compression envelope
        // (binary+compressed publishes persist the envelope's JSON as
        // the v2 bytes) — resolve back to the envelope so the
        // compressed/inflate machinery works unchanged.
        payload: this.wirePayloadOfRecord(latest),
        seq: latest.retainedSeq,
        at: latest.at,
        ...(latest.binary === true ? { binary: true } : {}),
        ...(latest.partition === undefined ? {} : { partition: latest.partition }),
        ...(latest.expiresAt === undefined ? {} : { expiresAt: latest.expiresAt }),
        ...(latest.dictId === undefined ? {} : { dictId: latest.dictId }),
        ...(latest.signature === undefined ? {} : { signature: { ...latest.signature } }),
        ...(latest.publisherSignature === undefined
          ? {}
          : { publisherSignature: { ...latest.publisherSignature } }),
        ...(latest.messageId === undefined ? {} : { messageId: latest.messageId }),
        ...(latest.publisherId === undefined ? {} : { publisherId: latest.publisherId }),
        ...(latest.eventTime === undefined ? {} : { eventTime: latest.eventTime }),
      });
    }
  }

  /**
   * Rebuilds the retained-message table from the durable log after a
   * restart (EB-87). Runs at construction, after the delayed-delivery
   * rebuild; namespace child logs rebuild through
   * `recoverRetainedFromLog` at `createNamespace`.
   */
  private recoverRetained(): void {
    const log = this.durableLog;
    if (log == null) return;
    this.recoverRetainedFromLog(log);
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
    const scheduled = new Map<string, { topic: string; deliverAt: number; expiresAt?: number; payload: unknown; key?: string; keySeq?: number; messageId?: string; publisherId?: string }>();
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
            // The identity and the publisher label ride the schedule
            // record so the rebuilt timer fans out with both intact
            // (EB-65; the fold previously dropped `messageId` here — the
            // entry below always intended to carry it).
            messageId: rec.messageId,
            publisherId: rec.publisherId,
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
        // The producer attribution rides the rebuilt timer so the
        // fan-out counts the publish under the original publisher.
        publisherId: rec.publisherId,
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
   * Rebuilds queue-mode pause buffers from the durable log after a
   * restart (EB-88). A pause-buffer record (seq 0, `paused: true`) becomes
   * a live buffered entry unless a later tombstone closed it. Pause rules
   * themselves are runtime ops state and do not survive a restart — so a
   * recovered buffer cannot stay paused under a rule that no longer
   * exists: the entries fan out immediately through the full admission
   * pipeline, in global `pauseOrder` (the original publish order across
   * topics), each keeping the TTL deadline stamped at pause admission.
   * Entries whose deadline passed while the process was down are dropped
   * as expired — counted once, tombstoned so a second restart does not
   * recount — and never resurrected (the EB-32 precedent).
   *
   * Design intent: the pause was a gate on the old process; the new
   * process starts unpaused. The alternative — rebuilding the rules too —
   * would resurrect ops intent across restarts, surprising an operator
   * who paused a topic, restarted for an unrelated deploy, and found the
   * topic still paused with no record of why.
   */
  private recoverPaused(): void {
    const log = this.durableLog;
    if (log == null) return;
    const nowMs = this.now();
    const tombstoned = new Set<string>();
    const buffered: DurableLogRecord[] = [];
    // `readSince(topic, -1)`: the exclusive bound sits below every real
    // seq, so the seq-0 pause records and tombstones come along too.
    for (const topic of log.topics()) {
      for (const rec of log.readSince(topic, -1)) {
        if (rec.paused !== true || rec.pauseId == null) continue;
        if (rec.cancelled === true) {
          tombstoned.add(rec.pauseId);
          continue;
        }
        buffered.push(rec);
      }
    }
    // Oldest first: `pauseOrder` is the cross-topic publish order the
    // resume drain would have used.
    buffered.sort((a, b) => (a.pauseOrder ?? 0) - (b.pauseOrder ?? 0));
    let fannedOut = false;
    for (const rec of buffered) {
      const pauseId = rec.pauseId as string;
      if (tombstoned.has(pauseId)) continue;
      // Keep generated ids ahead of recovered ones — `pauseTopic` must
      // never reuse an id the previous process handed out.
      const match = /^paused-(\d+)$/.exec(pauseId);
      if (match != null) {
        this.nextPauseId = Math.max(this.nextPauseId, parseInt(match[1], 10) + 1);
      }
      if (rec.pauseOrder !== undefined) {
        this.nextPauseOrder = Math.max(this.nextPauseOrder, rec.pauseOrder + 1);
      }
      if (rec.expiresAt !== undefined && nowMs >= rec.expiresAt) {
        this.recordExpired(rec.topic);
        this.appendPauseTombstone(pauseId, rec.topic);
        continue;
      }
      // Full admission on the new process: schema, rate-limit budget and
      // sequence numbers are drawn here, against the rules in force now.
      // Internal-only flags (`routed`, `fromBridge`, `diagnostic`) and the
      // traceparent were runtime state and are not recovered — the message
      // fans out as an ordinary publish (see `PauseBufferEntry`).
      this.fanOut(
        rec.topic,
        this.wirePayloadOfRecord(rec),
        false, // preAdmitted
        undefined, // delayed
        rec.key,
        undefined, // preassignedKeySeq
        undefined, // traceparent: not persisted, like delayed schedules
        rec.messageId,
        false, // routed
        undefined, // routedExpiresAt
        false, // fromBridge
        false, // diagnostic
        rec.eventTime,
        rec.causal,
        rec.publisherId,
        rec.schemaVersion,
        undefined, // retain: runtime-only, not persisted
        rec.expiresAt, // pausedExpiresAt: the original TTL deadline, verbatim
        undefined, // partitionKey
        undefined, // partition
        rec.binary ?? false,
      );
      this.appendPauseTombstone(pauseId, rec.topic);
      fannedOut = true;
    }
    if (fannedOut) this.scheduleFlush();
  }

  /**
   * Hands out the next per-(topic, partition) sequence number, starting at
   * 1 (EB-90 — the EB-13 per-topic seq, extended). The counter is per
   * concrete (topic, partition) pair and never resets, so every message
   * ever published to the pair carries a unique, gap-free number at
   * publish time — gaps only ever appear downstream, when a subscriber's
   * queue drops or expires a message.
   */
  private nextSeq(topic: string, partition: number): number {
    const key = EventBus.partitionSeqKey(topic, partition);
    const seq = (this.topicSeq.get(key) ?? 0) + 1;
    this.topicSeq.set(key, seq);
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
  private countSchemaRejection(topic: string, payload: unknown, publisherId?: string): void {
    this.statsFor(topic).rejectedMessages += 1;
    this.totalRejected += 1;
    this.emitAdmissionRejected(topic, 'schema', payload, publisherId);
  }

  /**
   * Pure schema verdict for one publish (EB-86): resolves the versioned
   * rule set for `topic` (falling back to the legacy validator), selects
   * the requested version (`schemaVersion`, default latest), and runs its
   * validator. No counting here — callers count. A throwing validator
   * propagates, exactly like the legacy path.
   *
   * Returns `{ admitted, deprecated }`: `deprecated` is true only when
   * the publish was admitted under a superseded-but-graced version — the
   * migrate-your-producers signal.
   */
  private schemaVerdict(
    topic: string,
    payload: unknown,
    schemaVersion: number | undefined,
  ): { admitted: boolean; deprecated: boolean } {
    const versions = this.schemaVersionsForTopic(topic);
    if (versions === undefined) {
      const legacy = this.schemaForTopic(topic);
      return { admitted: legacy === undefined || legacy(payload, topic), deprecated: false };
    }
    const latest = this.latestSchemaVersion(versions);
    const requested = schemaVersion ?? latest;
    const registration = versions.get(requested);
    if (registration === undefined) return { admitted: false, deprecated: false };
    if (requested === latest) {
      return { admitted: registration.validator(payload, topic), deprecated: false };
    }
    // A superseded version: admissible only inside its grace window,
    // anchored at the moment a greater version registered it away (the
    // window boundary is inclusive, the WR-20 convention).
    const supersededAt = registration.supersededAt ?? registration.registeredAt;
    if (this.now() - supersededAt > registration.graceMs) {
      return { admitted: false, deprecated: false };
    }
    return { admitted: registration.validator(payload, topic), deprecated: true };
  }

  /**
   * Applies the schema verdict with counting: rejections go through
   * `countSchemaRejection` (the `rejectedMessages` counter + the
   * `onAdmissionRejected` hook with reason `'schema'`), admissions under
   * a superseded-but-graced version additionally bump
   * `deprecatedSchemaVersions`. Returns true when the publish is
   * admitted.
   */
  private applySchemaVerdict(
    topic: string,
    payload: unknown,
    schemaVersion: number | undefined,
    publisherId?: string,
  ): boolean {
    const verdict = this.schemaVerdict(topic, payload, schemaVersion);
    if (!verdict.admitted) {
      this.countSchemaRejection(topic, payload, publisherId);
      return false;
    }
    if (verdict.deprecated) this.countDeprecatedSchema(topic);
    return true;
  }

  /**
   * Pure deprecated-admission check for the `publishAtomic` commit phase:
   * the shadow admission already ran the validator, so this re-resolves
   * only the version bookkeeping (never re-runs the validator) to decide
   * whether the committed entry deserves the `deprecatedSchemaVersions`
   * count. Exactly-once with `applySchemaVerdict` — the commit phase is
   * the only place an atomic entry is counted.
   */
  private isDeprecatedSchemaAdmission(topic: string, schemaVersion: number | undefined): boolean {
    const versions = this.schemaVersionsForTopic(topic);
    if (versions === undefined || schemaVersion === undefined) return false;
    const latest = this.latestSchemaVersion(versions);
    if (schemaVersion >= latest) return false;
    const registration = versions.get(schemaVersion);
    if (registration === undefined) return false;
    const supersededAt = registration.supersededAt ?? registration.registeredAt;
    return this.now() - supersededAt <= registration.graceMs;
  }

  /**
   * Counts one publish admitted under a superseded-but-graced schema
   * version (EB-86) against the topic and the global total. Unlike a
   * rejection this is a successful admission — the message was validated
   * and published — so it moves no other counter and fires no hook; it is
   * the operator signal that some producers still speak an old schema.
   */
  private countDeprecatedSchema(topic: string): void {
    this.statsFor(topic).deprecatedSchemaVersions += 1;
    this.totalDeprecatedSchemaVersions += 1;
  }

  /**
   * Counts one publish rejected by the per-topic message-size cap against
   * the topic and the global total, then fires the admission-rejection
   * hook. Like a schema rejection this happens before admission: the
   * payload never becomes a message — no sequence number is consumed
   * (subscribers see no gap), the durable log never sees it, and it does
   * not burn rate-limit budget. The rejection shares the `rejectedMessages`
   * counter with schema rejections, distinguished by the `'message-too-big'`
   * hook reason.
   */
  private countMessageTooBig(topic: string, payload: unknown, publisherId?: string): void {
    this.statsFor(topic).rejectedMessages += 1;
    this.totalRejected += 1;
    this.emitAdmissionRejected(topic, 'message-too-big', payload, publisherId);
  }

  /**
   * Counts one publish shed by publish-side rate limiting against the
   * topic and the global total, then fires the admission-rejection hook.
   * The shed consumes a sequence number (subscribers see a gap) — the
   * caller stamps it before this runs, so the stats entry must already
   * exist here.
   */
  private countRateLimited(topic: string, payload: unknown, publisherId?: string): void {
    this.statsFor(topic).rateLimitedMessages += 1;
    this.totalRateLimited += 1;
    this.emitAdmissionRejected(topic, 'rate-limit', payload, publisherId);
  }

  /**
   * Counts one publish shed by per-publisher rate limiting (EB-79)
   * against the topic and the global total, then fires the
   * admission-rejection hook. The shed happens before admission: the
   * payload never becomes a message — no sequence number is consumed
   * (subscribers see no gap), the durable log never sees it, and it burns
   * no topic rate-limit budget. The per-topic stats entry is created here
   * when the topic has never published anything yet (see
   * `countSchemaRejection`).
   */
  private countPublisherRateLimited(topic: string, payload: unknown, publisherId?: string): void {
    this.statsFor(topic).publisherRateLimited += 1;
    this.totalPublisherRateLimited += 1;
    this.emitAdmissionRejected(topic, 'publisher-rate-limit', payload, publisherId);
  }

  /**
   * Counts one publish shed by a topic pause in shed mode (EB-88)
   * against the topic and the global total, then fires the
   * admission-rejection hook with reason `'topic-paused'`. Like the
   * EB-19 rate-limit shed this consumes a sequence number — the caller
   * stamps it before this runs, so the stats entry must already exist
   * here — and subscribers observe the loss as a gap. Unlike the
   * pre-admission rejections it lands in the dedicated `pausedMessages`
   * counter, not `rejectedMessages`.
   */
  private countTopicPaused(topic: string, payload: unknown, publisherId?: string): void {
    this.statsFor(topic).pausedMessages += 1;
    this.totalPaused += 1;
    this.emitAdmissionRejected(topic, 'topic-paused', payload, publisherId);
  }

  /**
   * Takes one token from the publisher's rate-limit bucket (EB-79),
   * creating the bucket lazily from the configured rule on first use —
   * refilled from the injected clock (`EventBusOptions.now`), so the gate
   * stays deterministic in tests. Returns `'publisher-rate-limit'` when
   * the publisher has a configured limit and the bucket was empty (no
   * token taken), `undefined` when the publish may proceed — either a
   * token was taken or no limit is configured for this publisher. This
   * never counts and never fires the hook: the caller decides how to
   * surface the shed, so the fail-fast `publishDelayed` path and the
   * `fanOut` admission gate share the same budget semantics.
   */
  private takePublisherToken(publisherId: string | undefined): 'publisher-rate-limit' | undefined {
    if (publisherId === undefined) return undefined;
    const limit = this.publisherRateLimitRules.get(publisherId);
    if (limit === undefined) return undefined;
    let bucket = this.publisherRateLimitBuckets.get(publisherId);
    if (bucket == null) {
      bucket = new TokenBucket(limit.burst, limit.messagesPerSec, this.now);
      this.publisherRateLimitBuckets.set(publisherId, bucket);
    }
    return bucket.take() ? undefined : 'publisher-rate-limit';
  }

  /**
   * Counts one publish suppressed as an idempotency duplicate against the
   * topic and the global total, then fires the admission-rejection hook.
   * A duplicate is dropped before admission, so — like a schema rejection
   * — it creates the topic's stats entry when the topic has never
   * published anything yet.
   */
  private countDuplicate(topic: string, payload: unknown, publisherId?: string): void {
    this.statsFor(topic).duplicateMessages += 1;
    this.totalDuplicates += 1;
    this.emitAdmissionRejected(topic, 'duplicate', payload, publisherId);
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
  private countAclDenied(topic: string, payload: unknown, publisherId?: string): void {
    this.statsFor(topic).rejectedMessages += 1;
    this.totalRejected += 1;
    this.emitAdmissionRejected(topic, 'acl', payload, publisherId);
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
   * Records one subscription lifecycle audit event (EB-78). Fully
   * error-isolated: `SubscriptionAuditLog.append` never throws by
   * construction, and this wrapper swallows defensively anyway, so a
   * broken audit log can never disturb the subscribe/unsubscribe path.
   */
  private auditSubscription(event: Omit<SubscriptionAuditEvent, 'v' | 'at'>): void {
    try {
      this.subAudit.append(event);
    } catch {
      // Swallowed: the audit log is an observer, not part of the path.
    }
  }

  /**
   * Records one `backpressure_dropped` audit event (EB-83): a message shed
   * or refused because a subscriber's queue was saturated — under any of
   * the three backpressure tiers. Fully error-isolated like all audit
   * emission: the enqueue path never throws for observability.
   */
  private auditBackpressureDrop(
    subscriber: Subscriber,
    msg: BusMessage,
    policy: BackpressurePolicy,
    dropped: number,
  ): void {
    this.auditSubscription({
      action: 'backpressure_dropped',
      actor: 'system',
      subId: subscriber.id,
      pattern: subscriber.pattern,
      topic: msg.topic,
      detail: { policy, dropped, queueSize: subscriber.queue.size },
    });
  }

  /**
   * Queries the subscription lifecycle audit log (EB-78): the bounded
   * in-memory index of every subscribe / unsubscribe / pattern
   * hot-update / group join-leave / rebalance / topic-registry change /
   * backpressure drop (EB-83),
   * optionally persisted to `subscriptionAuditDir` daily JSONL files.
   * Filters by time window and action; `limit` keeps the most recent
   * matches. Entries come back oldest-first. The index is bounded
   * (`subscriptionAuditMaxEntries`, default 1000) — deep history lives in
   * the on-disk files when persistence is enabled. Throws `RangeError`
   * on illegal query values.
   */
  getSubscriptionAudit(query?: SubscriptionAuditQuery): SubscriptionAuditEvent[] {
    return this.subAudit.query(query ?? {});
  }

  /**
   * Registers a topic in the operator-side catalog (EB-77): the concrete
   * topic name plus optional ownership metadata (`owner`, `description`,
   * `schemaVersion`, `tags`). Registering an already-registered topic
   * updates its metadata in place; a deregistered topic re-registers
   * fresh (keeping its original `registeredAt`).
   *
   * Returns `true` when the topic was newly registered, `false` when an
   * existing registration was updated. The change is recorded in the
   * subscription audit log as `topic_register` (with the metadata and an
   * `updated` flag in `detail`). Throws `RangeError` on illegal
   * registration values or a bad actor.
   */
  registerTopic(reg: TopicRegistration, actor = 'system'): boolean {
    assertValidActor(actor, 'registerTopic');
    const created = this.topicRegistry.register(reg, this.now());
    this.auditSubscription({
      action: 'topic_register',
      topic: reg.topic,
      actor,
      detail: {
        owner: reg.owner,
        description: reg.description,
        schemaVersion: reg.schemaVersion,
        tags: reg.tags,
        updated: !created,
      },
    });
    return created;
  }

  /**
   * Deregisters a topic (EB-77): the registration is removed but its
   * last-known metadata is kept as a tombstone, so `getTopicInfo` still
   * answers for a retired topic (marked `registered: false`).
   *
   * Returns `true` when a live registration was removed, `false` when
   * the topic was never registered or already deregistered. The change
   * is recorded in the subscription audit log as `topic_deregister`.
   */
  deregisterTopic(topic: string, actor = 'system'): boolean {
    assertValidActor(actor, 'deregisterTopic');
    const removed = this.topicRegistry.deregister(topic, this.now());
    if (removed) {
      const info = this.topicRegistry.get(topic);
      this.auditSubscription({
        action: 'topic_deregister',
        topic,
        actor,
        detail: {
          owner: info?.owner,
          description: info?.description,
        },
      });
    }
    return removed;
  }

  /**
   * Full record for a topic (EB-77), tombstones included — `undefined`
   * when the topic was never registered.
   */
  getTopicInfo(topic: string): TopicInfo | undefined {
    return this.topicRegistry.get(topic);
  }

  /**
   * The operator catalog (EB-77): every live registration in registration
   * order. With `includeDeregistered: true`, tombstones are appended after
   * the live entries, each marked `registered: false`.
   */
  topicCatalog(options?: { includeDeregistered?: boolean }): TopicInfo[] {
    return this.topicRegistry.catalog(options?.includeDeregistered ?? false);
  }

  /**
   * Counts one publish to an unregistered topic and fires the opt-in
   * hook (EB-77). Advisory only — never blocks the publish. The hook is
   * error-isolated: a throwing observer is swallowed.
   */
  private checkTopicRegistered(topic: string): void {
    if (this.topicRegistry.has(topic)) return;
    this.unregisteredTopicPublishes += 1;
    const hook = this.onUnregisteredTopic;
    if (hook === undefined) return;
    try {
      hook({
        topic,
        at: this.now(),
        unregisteredPublishes: this.unregisteredTopicPublishes,
      });
    } catch {
      // Swallowed: the hook is an observer, not part of the publish path.
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
    publisherId?: string,
  ): void {
    const hook = this.onAdmissionRejected;
    if (hook === undefined) return;
    const serialized = serializeToJson(payload);
    const payloadBytes = serialized === undefined ? 0 : Buffer.byteLength(serialized, 'utf8');
    try {
      hook({
        topic,
        reason,
        payloadBytes,
        at: this.now(),
        // The rejected publish's producer label, when the publish carried
        // one — so admission rejections stay attributable to their source.
        ...(publisherId === undefined ? {} : { publisher: publisherId }),
      });
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
      this.advanceBaseline(subscriber, msg.topic, msg.seq, msg.epoch ?? '', msg.partition ?? 0);
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
    // EB-83 'block' tier: a saturated subscriber refuses new arrivals —
    // the already-queued backlog is preserved instead of churned. The
    // refusal counts as a backpressure drop (the message never reaches a
    // handler) and is audited like any other drop.
    if (subscriber.blocked) {
      this.totalDropped += 1;
      this.totalBlocked += 1;
      subscriber.blockedCount += 1;
      this.auditBackpressureDrop(subscriber, msg, 'block', 1);
      return 'dropped';
    }
    // Count-saturation check BEFORE the push: the 'block' tier arms only
    // when the push faces a count-full queue. A byte-budget shed with
    // count headroom does not arm it — memory pressure always sheds per
    // policy, independently of the tier.
    const wasFull = queue.size >= subscriber.queueCapacity;
    const droppedBefore = queue.droppedCount;
    const result = queue.push(msg, 0, deadline);
    const shed = queue.droppedCount - droppedBefore;
    this.totalDropped += shed;
    if (shed > 0 || result === 'dropped') {
      // Every queue-level drop is audited (EB-83): shed evictions and
      // refused newcomers alike, with the tier that was in force.
      this.auditBackpressureDrop(subscriber, msg, subscriber.backpressure, shed > 0 ? shed : 1);
    }
    if (subscriber.backpressure === 'block' && wasFull) {
      // First count-saturation under the 'block' tier: the admit-with-shed
      // step behaved exactly like the historical default; from here the
      // subscriber blocks new arrivals until the queue recedes below the
      // high-water mark (`onDrained` clears the flag).
      subscriber.blocked = true;
    }
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
  private deliverTraced<T>(subscriber: Subscriber, msg: BusMessage, invoke: () => T): T {
    const tracer = this.trace;
    if (tracer === undefined || !tracer.hasTrace(msg)) {
      return invoke();
    }
    const at = this.now();
    const result = invoke();
    tracer.deliverSpan(subscriber, msg, at, this.now() - at);
    return result;
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
    barrier?: { epoch: number },
  ): boolean {
    // Barrier gate (EB-98) runs FIRST — before the causal gate (EB-62)
    // and the keyed gate. Design intent: the barrier cuts the
    // subscriber's whole delivery stream (cross-topic, which per-source
    // causal clocks cannot express), so it must see every message before
    // any ordering gate does. A message held behind the barrier
    // re-enters the pipeline AFTER this gate at release time (the gate
    // is disarmed by then), so happens-before order is still enforced by
    // the causal gate — causality is preserved on both sides of the cut,
    // and the cut marker itself is never reordered behind data.
    const barrierGate = subscriber.barrier;
    if (barrier !== undefined) {
      // Barrier control message: only barrier-opted-in subscribers ever
      // see it — for everyone else it is invisible (never queued, never
      // a gap, never counted).
      if (barrierGate === undefined) return false;
      return this.admitBarrier(subscriber, barrierGate, msg, barrier.epoch);
    }
    if (barrierGate !== undefined && barrierGate.armedEpoch !== null) {
      // A barrier is armed: this message arrived after the cut in
      // delivery order — hold it behind the barrier until the barrier
      // itself is handed to the handler.
      return this.holdBehindBarrier(subscriber, barrierGate, {
        msg,
        expiresAt,
        rawPayload,
        keyed,
        causal,
      });
    }
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
   * Admits a barrier control message for a barrier-opted-in subscriber
   * (EB-98): enforces epoch monotonicity, arms the hold, and enqueues
   * the barrier itself as a control message.
   *
   * An out-of-order barrier (epoch at or below the highest epoch ever
   * armed on this subscriber's stream) is discarded — never delivered,
   * never arming — counted, and recorded in the subscription audit log
   * as `barrier_dropped`. Monotonicity is per subscriber (like the
   * causal gate's per-subscriber expectations): each subscriber tracks
   * its own stream position, so a late joiner that first sees epoch 5
   * treats a subsequent epoch 3 as out-of-order for its own stream.
   *
   * The barrier bypasses the content filter, composite matching and the
   * publish-side throttle: it is a control signal the subscriber opted
   * into, not business content — a filter must never be able to swallow
   * the cut marker and wedge the hold behind it. It IS subject to the
   * subscriber's backpressure queue: if the queue sheds the barrier, the
   * hold releases fail-open immediately (abandoning the cut beats
   * abandoning the data — the stream was already lossy).
   */
  private admitBarrier(
    subscriber: Subscriber,
    gate: BarrierGateState,
    msg: BusMessage,
    epoch: number,
  ): boolean {
    if (epoch <= gate.lastEpoch) {
      this.auditBarrierDropped(subscriber, msg.topic, epoch, gate.lastEpoch);
      return false;
    }
    gate.lastEpoch = epoch;
    gate.armedEpoch = epoch;
    // Straight to the queue: no filter, no throttle, no signature check
    // (barriers are unsigned by design — authenticated at publish by the
    // ACL, see `fanOutBarrier`).
    if (this.enqueueMessage(subscriber, msg, undefined) !== 'accepted') {
      // The barrier never reached the queue, so it can never drain —
      // release the hold fail-open now instead of wedging it forever.
      this.releaseBarrierHold(subscriber);
      return false;
    }
    return true;
  }

  /**
   * Holds one message behind an armed barrier (EB-98): buffered
   * pre-queue in arrival order, consuming no backpressure budget, with
   * no filter or throttle evaluated yet — exactly like the causal gate's
   * reorder buffer. Counts as accepted for the fan-out width: it will
   * reach the queue when the barrier drains.
   *
   * Every hold terminates: when the buffer would exceed
   * `maxHeldMessages` the gate fails open — everything held so far is
   * released at once, the barrier is disarmed (the cut is abandoned, a
   * single counted event), and the current message flows through the
   * normal pipeline.
   */
  private holdBehindBarrier(
    subscriber: Subscriber,
    gate: BarrierGateState,
    held: BarrierHeld,
  ): boolean {
    if (gate.held.length >= gate.maxHeldMessages) {
      this.totalBarrierOverflows += 1;
      this.releaseBarrierHold(subscriber);
      // Gate disarmed by the release: flow through the normal pipeline.
      return this.deliverToSubscriber(
        subscriber,
        held.msg,
        held.expiresAt,
        held.rawPayload,
        held.keyed,
        held.causal,
      );
    }
    gate.held.push(held);
    return true;
  }

  /**
   * Releases everything held behind the subscriber's armed barrier, at
   * once, in arrival order (EB-98). Called when the barrier control
   * message is handed to the handler (see `noteMessageDelivered`), and
   * on the fail-open paths (barrier shed by backpressure, hold-buffer
   * overflow). Each released message re-enters the pipeline AFTER the
   * barrier gate — the gate is disarmed first, so there is no
   * re-entrancy — and still passes through the causal gate, the keyed
   * gate, the filter, the throttle and the queue: "released at once"
   * describes the single release action, not a bypass of the pipeline.
   * A no-op when no barrier is armed (e.g. the barrier was redelivered
   * after a nack — arming happens once, at pipeline time).
   */
  private releaseBarrierHold(subscriber: Subscriber): void {
    const gate = subscriber.barrier;
    if (gate === undefined || gate.armedEpoch === null) return;
    gate.armedEpoch = null;
    const held = gate.held;
    gate.held = [];
    if (held.length === 0) return;
    this.totalBarrierReleased += held.length;
    for (const h of held) {
      this.deliverToSubscriber(subscriber, h.msg, h.expiresAt, h.rawPayload, h.keyed, h.causal);
    }
    this.scheduleFlush();
  }

  /**
   * Delivery-time bookkeeping for one message handed toward the handler:
   * sequence-gap detection for business messages; barrier release for
   * barrier control messages. A barrier carries no business seq, so gap
   * detection never sees it — and the moment the barrier reaches the
   * handler is the moment everything held behind it is released at once
   * ("drained, then released"). Called at every drain path that invokes
   * (or batches) the handler, next to where `detectGap` used to run.
   */
  private noteMessageDelivered(subscriber: Subscriber, msg: BusMessage): void {
    if (msg.barrier !== undefined) {
      this.releaseBarrierHold(subscriber);
      return;
    }
    this.detectGap(subscriber, msg);
  }

  /**
   * Records an out-of-order barrier discard in the subscription audit log
   * (EB-98, `barrier_dropped`).
   */
  private auditBarrierDropped(
    subscriber: Subscriber,
    topic: string,
    epoch: number,
    lastEpoch: number,
  ): void {
    this.totalBarriersDropped += 1;
    this.auditSubscription({
      action: 'barrier_dropped',
      actor: 'system',
      subId: subscriber.id,
      pattern: subscriber.pattern,
      topic,
      detail: { epoch, lastEpoch },
    });
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
    // Envelope signature verification (EB-84): a verifying subscriber
    // checks integrity before any content processing — a failed check
    // drops the message before the queue, the filter, and the throttle.
    // Counted (per-topic + global) and audited. The gap baseline does
    // NOT advance: unlike a deliberate filter skip, a failed verification
    // is a genuine loss, so the next delivery surfaces it as a sequence
    // gap — the same visibility backpressure drops and TTL expirations
    // get.
    if (subscriber.verifySignature && !this.deliverySignatureValid(msg)) {
      this.countSignatureFailure(subscriber, msg, msg.signature?.kid);
      return false;
    }
    // Publisher-identity verification (EB-97): a verifying subscriber
    // checks identity before any content processing — same drop/count/
    // audit semantics as the topic-signature check above, and the same
    // genuine-loss gap visibility (the gap baseline does NOT advance).
    if (subscriber.verifyPublisher && !this.deliveryPublisherSignatureValid(msg)) {
      this.countSignatureFailure(subscriber, msg, msg.publisherSignature?.kid, 'publisher');
      return false;
    }
    const filter = subscriber.filter;
    if (filter !== undefined && !filter(rawPayload, msg.topic)) {
      // Content-filtered: the message never reaches the queue — no
      // backpressure budget consumed, no throttle token burned — and it
      // must not surface as a sequence gap, so the per-topic baseline
      // advances over it (seen, deliberately skipped). A throwing filter
      // propagates to the publish call, like a throwing schema validator.
      this.countFiltered(subscriber, msg.topic, msg.seq, msg.epoch ?? '', msg.partition ?? 0);
      return false;
    }
    const composite = subscriber.composite;
    if (composite !== undefined && !this.matchesComposite(composite, msg.topic)) {
      // Composite-pattern (EB-70) rejection, evaluated after the content
      // filter: the message never reaches the queue — no backpressure
      // budget consumed, no throttle token burned — and the per-topic
      // baseline advances over it so the deliberate skip never counts as
      // a sequence gap. Counted like a filter rejection: both are
      // subscriber-side narrowing, invisible to gap detection.
      this.countFiltered(subscriber, msg.topic, msg.seq, msg.epoch ?? '', msg.partition ?? 0);
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
    const window = subscriber.window;
    if (window !== undefined) {
      // Tumbling-window aggregation (EB-73): the message folds into the
      // subscriber's open window instead of being queued — the aggregate
      // is enqueued when the window closes. Filter, composite matching
      // and publish-side throttling above already ran on the raw message,
      // so windowing composes orthogonally with all three.
      return this.accumulateWindowed(subscriber, window, msg, expiresAt, rawPayload);
    }
    return this.enqueueMessage(subscriber, msg, expiresAt) === 'accepted';
  }

  /**
   * The shadow-mirror delivery path (see `SubscribeOptions.shadow`): a
   * content filter (when the shadow subscriber has one), then the
   * shadow's independent backpressure queue. Unlike `deliverUnkeyed` it
   * touches no shared counters — no throttle shedding, no gap-baseline
   * bookkeeping, no filtered-message accounting — so the mirror can
   * never move production stats. A throwing filter propagates to the
   * publish call, exactly like a production filter: filters are expected
   * pure.
   */
  private deliverShadow(
    subscriber: Subscriber,
    msg: BusMessage,
    expiresAt: number | undefined,
    rawPayload: unknown,
  ): void {
    const filter = subscriber.filter;
    if (filter !== undefined && !filter(rawPayload, msg.topic)) {
      // A filter-rejected shadow copy is deliberately skipped: never
      // queued, never counted anywhere. Shadow deliveries track no
      // sequence gaps, so — unlike production — there is no baseline to
      // advance over the skip.
      return;
    }
    const composite = subscriber.composite;
    if (composite !== undefined && !this.matchesComposite(composite, msg.topic)) {
      // A composite-rejected shadow copy is deliberately skipped, exactly
      // like a filter rejection above: never queued, never counted
      // anywhere, so the mirror cannot move production stats.
      return;
    }
    this.enqueueShadowMessage(subscriber, msg, expiresAt);
  }

  /**
   * Pushes one message into a shadow subscriber's independent queue. The
   * shed accounting is shadow-local: a full shadow queue counts into the
   * shadow's own `dropped` counter and the bus-wide
   * `totalShadowDropped` — never into the bus-wide `droppedMessages` —
   * so a saturated mirror cannot move production stats. Delivery-latency
   * and lag-watermark stamping compose exactly as for production (both
   * are per-subscriber state), and a sampled trace still gets its
   * per-subscriber `bus.enqueue` span for the mirror copy. Dedup and
   * ack-latency stamping are absent: both are rejected as shadow
   * combinations at `subscribe` time, so there is nothing to stamp.
   */
  private enqueueShadowMessage(
    subscriber: Subscriber,
    msg: BusMessage,
    deadline: number | undefined,
  ): 'accepted' | 'dropped' {
    const shadow = subscriber.shadow;
    if (shadow === undefined) return 'dropped';
    const queue = subscriber.queue;
    // Diff the queue's drop counter around the push: with `drop-oldest`
    // a full queue sheds an OLD entry to admit the newcomer, so `push`
    // returns 'accepted' while still shedding — the eviction is only
    // visible in `droppedCount` (same accounting as `enqueueMessage`).
    const droppedBefore = queue.droppedCount;
    const result = queue.push(msg, 0, deadline);
    const shed = queue.droppedCount - droppedBefore;
    if (shed > 0) {
      shadow.dropped += shed;
      this.totalShadowDropped += shed;
    }
    const latency = subscriber.latency;
    if (latency != null && result === 'accepted') {
      latency.enqueuedAt.set(msg, this.now());
    }
    const lag = subscriber.lag;
    if (lag != null && result === 'accepted') {
      const nowMs = this.now();
      lag.enqueuedAt.set(msg, nowMs);
      this.checkLag(subscriber, lag, nowMs);
    }
    const tracer = this.trace;
    if (tracer !== undefined && result === 'accepted' && tracer.hasTrace(msg)) {
      tracer.enqueueSpan(subscriber, msg, this.now(), 0);
    }
    return result;
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
  private advanceBaseline(
    subscriber: Subscriber,
    topic: string,
    seq: number,
    epoch = '',
    partition = 0,
  ): void {
    const gapKey = EventBus.gapKey(topic, partition);
    const last = subscriber.lastDeliveredSeq.get(gapKey);
    const lastEpoch = subscriber.lastEpoch.get(gapKey);
    if (last === undefined || lastEpoch !== epoch) {
      subscriber.lastDeliveredSeq.set(gapKey, seq);
      subscriber.lastEpoch.set(gapKey, epoch);
    } else if (seq > last) {
      subscriber.lastDeliveredSeq.set(gapKey, seq);
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
  private countFiltered(
    subscriber: Subscriber,
    topic: string,
    seq: number,
    epoch = '',
    partition = 0,
  ): void {
    this.advanceBaseline(subscriber, topic, seq, epoch, partition);
    this.statsFor(topic).filteredMessages += 1;
    this.totalFiltered += 1;
  }

  /**
   * Pure envelope signature check (EB-84): does `msg` carry a signature
   * that verifies under the topic's current signing rules? Never counts
   * — callers count on failure. A missing rule, a missing signature, or
   * an unserializable payload all verify as false: a `verifySignature`
   * subscriber is fail-closed.
   */
  private deliverySignatureValid(msg: BusMessage): boolean {
    const rule = this.signingForTopic(msg.topic);
    const sig = msg.signature;
    if (rule === undefined || sig === undefined) return false;
    // Verifies against the same wire bytes the publisher signed (EB-92):
    // a binary message's raw bytes (inflated first when compressed —
    // inflation runs before delivery), or the post-compression wire form
    // for ordinary messages.
    const wireBytes = wireBytesOfPayload(msg.payload, this.binaryMessages.has(msg));
    if (wireBytes === undefined) return false;
    return verifyTopicMessage(
      rule.keys,
      msg.topic,
      msg.seq,
      wireBytes,
      sig,
      this.now(),
      rule.graceMs,
    );
  }

  /**
   * Pure publisher-identity signature check (EB-97): does `msg` carry a
   * publisher signature that verifies under the publisher's current key
   * set? Never counts — callers count on failure. A missing
   * `publisherId`, a missing key set, a missing signature, or an
   * unserializable payload all verify as false: a `verifyPublisher`
   * subscriber is fail-closed, so an impersonated identity is dropped,
   * never delivered.
   */
  private deliveryPublisherSignatureValid(msg: BusMessage): boolean {
    const publisherId = msg.publisherId;
    if (publisherId === undefined) return false;
    const rule = this.publisherSigningKeys.get(publisherId);
    const sig = msg.publisherSignature;
    if (rule === undefined || sig === undefined) return false;
    // The same wire bytes the publisher signed (EB-92): a binary
    // message's raw bytes (inflated first when compressed — inflation
    // runs before delivery), or the post-compression wire form for
    // ordinary messages.
    const wireBytes = wireBytesOfPayload(msg.payload, this.binaryMessages.has(msg));
    if (wireBytes === undefined) return false;
    return verifyPublisherMessage(
      rule.keys,
      msg.topic,
      msg.seq,
      publisherId,
      wireBytes,
      sig,
      this.now(),
      rule.graceMs,
    );
  }

  /**
   * Counts one signature-verification failure on the topic and the
   * global total, and files the error-isolated
   * `signature_verification_failed` subscription audit event.
   * `kind: 'publisher'` marks a failed publisher-identity check
   * (EB-97); the topic-signature check (EB-84) leaves it absent, so
   * existing consumers see no shape change.
   */
  private countSignatureFailure(
    subscriber: Subscriber,
    msg: BusMessage,
    kid: string | undefined,
    kind?: 'publisher',
  ): void {
    this.statsFor(msg.topic).signatureFailures += 1;
    this.totalSignatureFailures += 1;
    this.auditSubscription({
      action: 'signature_verification_failed',
      actor: 'system',
      subId: subscriber.id,
      pattern: subscriber.pattern,
      topic: msg.topic,
      detail: {
        seq: msg.seq,
        ...(kid === undefined ? {} : { kid }),
        ...(kind === undefined ? {} : { kind }),
      },
    });
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
  /**
   * Slim fan-out for barrier control messages (EB-98): a barrier is
   * control-plane, not business data, so it deliberately skips the full
   * admission pipeline — no per-topic sequence number (the EB-13 counter
   * never advances, subscribers see no gap), no durable-log write
   * (replay restores data, not live coordination cuts), no retained
   * update, no schema validation, no rate-limit budget, no compression,
   * no signing, no TTL deadline, no bridge/cluster mirror. What it keeps:
   * topic-alias resolution (a barrier to a live old topic cuts the new
   * topic's stream) and the broker-level ACL (an unauthorized publisher
   * cannot inject cut markers — denied barriers are counted exactly like
   * denied publishes). The barrier rides `deliverMatched` so group
   * routing, the prefix index and the shadow mirror behave exactly as for
   * data; the subscriber-side barrier gate (`deliverToSubscriber`) arms
   * the hold and delivers the control message.
   *
   * Returns the number of opted-in subscribers that accepted the barrier.
   */
  private fanOutBarrier(topic: string, payload: unknown, epoch: number): number {
    // Topic aliases (EB-44) resolve before the ACL, exactly like the full
    // pipeline: a barrier naming a retired old topic is refused and claims
    // nothing — it must not cut a stream that no longer exists.
    const aliasResolution = this.resolvePublishTopic(topic);
    if (aliasResolution.retired) {
      this.countAliasRetired(topic, payload, undefined);
      return 0;
    }
    topic = aliasResolution.topic;
    // Broker-level ACL: the one admission gate a control message keeps.
    if (!this.aclAllowsPublish(topic)) {
      this.countAclDenied(topic, payload, undefined);
      return 0;
    }
    const msg: BusMessage = {
      topic,
      payload,
      // No business seq: 0 is never a valid per-topic sequence number
      // (they start at 1), so the counter is untouched and gap detection
      // — which skips barrier messages — never sees a gap.
      seq: 0,
      barrier: { epoch },
    };
    this.totalBarriersPublished += 1;
    const { accepted } = this.deliverMatched(msg, undefined, payload, undefined, undefined, { epoch });
    return accepted;
  }

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
    // Already-validated publisher identity (see `validatePublisherId`):
    // the publish entry points validate, `fanOut` never does. Anonymous
    // inbound (bridge, cluster) and internal replays pass `undefined`.
    publisherId?: string,
    // Already-validated schema version (see `validateSchemaVersion`):
    // the publish entry points validate, `fanOut` never does. Internal
    // paths (bridge, forward, route, diagnostic, delayed fan-out) pass
    // `undefined` — admission runs against the latest registered version.
    schemaVersion?: number,
    // MQTT-style retained message (see `PublishOptions.retain`, EB-87):
    // only `publish` passes this — every other path leaves it undefined
    // and never touches the retained store.
    retain?: boolean,
    // Pause-buffer TTL deadline (see `EventBus.pauseTopic`, EB-88): only
    // the pause-buffer drain passes this — the message was "published" at
    // pause time and only *delivered* at resume, so the deadline stamped
    // at pause admission wins verbatim over the TTL rules in force now
    // (a rule added in between does not retroactively extend the
    // message). Every other path leaves it undefined.
    pausedExpiresAt?: number,
    // Publish-side partition routing (see `PublishOptions.partitionKey` /
    // `PublishOptions.partition`, EB-90): the publish entry points
    // validate the types (`validatePartitionOptions`); `fanOut` resolves
    // the partition against the alias-resolved topic right after alias
    // resolution (explicit partition wins over the `partitionKey`
    // consistent hash; default 0). Internal paths (bridge, cluster,
    // routes, forwards, delayed fan-out, pause-buffer drain) leave both
    // undefined and land on partition 0.
    partitionKey?: string,
    partition?: number,
    // Binary payload passthrough (see `PublishOptions.binary`, EB-92):
    // already validated/normalized by the publish entry points —
    // `fanOut` never validates. Internal re-fan-out paths (route,
    // pause-buffer drain, cross-bus forward) propagate the source
    // message's flag so a binary payload is never reinterpreted as
    // JSON mid-pipeline.
    binary = false,
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
      this.countAliasRetired(topic, payload, publisherId);
      return { matched: 0, accepted: 0, admitted: false };
    }
    topic = aliasResolution.topic;
    // Publish-side partition routing (EB-90): resolves the message's
    // partition from the explicit `partition` (wins) or the consistent
    // hash of `partitionKey` over the topic's configured partition count
    // (`setTopicPartitions`, default 1). Throws `RangeError` on an
    // out-of-range explicit partition — placed here, right after alias
    // resolution and before the pause gate, so an invalid partition never
    // buffers, never sheds, never logs, and never draws a sequence
    // number. The resolved partition selects the per-(topic, partition)
    // sequence counter below and rides the envelope, the durable log,
    // and the per-partition stats.
    const resolvedPartition = this.resolvePartition(topic, partitionKey, partition);
    // Sensitive-field redaction (EB-68): admission-time, before every
    // gate. The schema validator, the size cap, the rate limiter,
    // compression, the bridge mirror, the durable log, and fan-out all
    // see the redacted payload only — nothing downstream can ever observe
    // or persist the original field values, and replay reads
    // already-redacted bytes from the log (the original is never
    // resurrected). Skipped on pre-admitted paths (`publishAtomic`
    // commit, delayed fan-out): those already redacted at their own
    // admission — re-running here would double-count. Skipped for
    // binary payloads (EB-92): bytes have no fields to redact.
    if (!preAdmitted && !binary) {
      payload = this.redactAdmissionPayload(topic, payload);
    }
    // Topic pause (EB-88): the FIRST admission gate — before the
    // per-publisher rate limiter (EB-79), the ACL, the schema validator
    // and the size cap. In shed mode the publish is rejected outright:
    // counted in `pausedMessages`, surfaced on the admission-rejection
    // hook with reason `'topic-paused'`, and consuming a sequence number
    // so subscribers observe the loss as a gap (the EB-19 rate-limit
    // shed semantics — the stats entry is updated here, exactly as the
    // rate-limit shed does below). In queue mode the publish is held in
    // the rule's durable-side buffer instead: no sequence number, no
    // schema verdict, no rate-limit budget — the message gets its full
    // admission when it fans out at resume.
    if (!preAdmitted) {
      const pauseRule = this.pauseForTopic(topic);
      if (pauseRule !== undefined) {
        if (pauseRule.mode === 'shed') {
          const stats = this.statsFor(topic);
          stats.publishedMessages += 1;
          const shedSeq = this.nextSeq(topic, resolvedPartition);
          stats.lastSeq = Math.max(stats.lastSeq, shedSeq);
          this.totalPublished += 1;
          this.countPartitionPublish(topic, resolvedPartition, shedSeq);
          this.countTopicPaused(topic, payload, publisherId);
          return { matched: 0, accepted: 0, admitted: false };
        }
        this.bufferPausedMessage(pauseRule, topic, payload, {
          key,
          traceparent,
          messageId,
          eventTime,
          causal,
          publisherId,
          schemaVersion,
          retain,
          routed,
          fromBridge,
          diagnostic,
          binary,
        });
        return { matched: 0, accepted: 0, admitted: false };
      }
    }
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
      // Per-publisher rate limiting (EB-79) is the first admission gate:
      // a cheap pre-filter that protects the bus from a single hot
      // publisher, so an over-budget publish is shed before the ACL, the
      // schema validator and the size cap even see it. Only publishes
      // carrying a configured publisher identity are subject to it —
      // anonymous publishes and unconfigured publishers are unlimited.
      // Like every other pre-admission gate this consumes no sequence
      // number (subscribers see no gap), writes nothing to the durable
      // log, and burns no topic rate-limit budget. Surfaced on the
      // admission-rejection hook with reason 'publisher-rate-limit'.
      //
      // Design intent (EB-88): the topic-pause gate above runs before
      // this one — a publish to a paused topic never takes a publisher
      // token. The pause is an operator-level "stop" on the topic; letting
      // it burn a producer's budget would punish the producer for the
      // operator's window, and would make the shed's attribution
      // ambiguous (paused vs. over-budget).
      if (this.takePublisherToken(publisherId) !== undefined) {
        this.countPublisherRateLimited(topic, payload, publisherId);
        return { matched: 0, accepted: 0, admitted: false };
      }
      // Broker-level ACL runs before every other admission gate except
      // the per-publisher rate limiter above it (EB-79); topic aliases
      // were already resolved above. An
      // unauthorized publish is rejected before schema validation — it
      // consumes no sequence number (subscribers see no gap), never touches
      // the durable log, and burns no rate-limit budget. Counted as a
      // rejection with reason 'acl' (see countAclDenied).
      if (!this.aclAllowsPublish(topic)) {
        this.countAclDenied(topic, payload, publisherId);
        return { matched: 0, accepted: 0, admitted: false };
      }
      // Publish-side schema validation runs before admission: a rejected
      // payload never becomes a message — no sequence number is consumed
      // (subscribers see no gap), the durable log never sees it, and it
      // does not burn rate-limit budget. Versioned validators (EB-86)
      // resolve the requested version here; admissions under a
      // superseded-but-graced version are counted in
      // `deprecatedSchemaVersions`.
      if (!this.applySchemaVerdict(topic, payload, schemaVersion, publisherId)) {
        return { matched: 0, accepted: 0, admitted: false };
      }
      // Per-topic maximum message size runs after schema validation and
      // before sequence-number assignment: an oversized payload is rejected
      // before admission — no sequence number consumed (subscribers see no
      // gap), the durable log never sees it, and it burns no rate-limit
      // budget — exactly the schema-rejection semantics. Counted in
      // `rejectedMessages`, surfaced on the admission-rejection hook with
      // reason 'message-too-big'.
      const maxBytes = this.maxMessageBytesForTopic(topic);
      if (maxBytes !== undefined && payloadByteSize(payload) > maxBytes) {
        this.countMessageTooBig(topic, payload, publisherId);
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
      // Per-(topic, partition) sequence number (EB-90): the envelope's
      // partition is always stamped — 0 for unpartitioned traffic — so
      // gap detection, replay and the durable log key off one field.
      seq: this.nextSeq(topic, resolvedPartition),
      partition: resolvedPartition,
      ...(cleanMessageId === undefined ? {} : { messageId: cleanMessageId }),
      // The producer label rides the envelope so attribution survives
      // durable-log replay, delayed schedules and cross-bus forwards.
      ...(publisherId === undefined ? {} : { publisherId }),
      // Business event time rides the envelope (see `PublishOptions.eventTime`):
      // undefined stays absent, so messages without one never participate in
      // the watermark.
      ...(eventTime === undefined ? {} : { eventTime }),
    };
    // EB-57: mark DLQ diagnostic messages at creation (the envelope marker
    // is bus-side, so a user's payload can never collide with it) — a
    // dead-lettered diagnostic message must not emit a second diagnostic.
    if (diagnostic) this.diagnosticMessages.add(msg);
    // Binary payload passthrough (EB-92): bus-side marker driving every
    // binary-aware branch downstream — byte-size measurement, compression
    // input, signature wire bytes, durable-log v2 encoding, and
    // delivery-time inflation (bytes stay bytes, never `JSON.parse`d).
    if (binary) this.binaryMessages.add(msg);
    const stats = this.statsFor(topic);
    stats.publishedMessages += 1;
    // Topic-level `lastSeq` stays the highest number handed out on any
    // partition — the per-partition highs live in `partitionPublished`.
    stats.lastSeq = Math.max(stats.lastSeq, msg.seq);
    this.totalPublished += 1;
    this.countPartitionPublish(topic, resolvedPartition, msg.seq);
    // Per-publisher publish counting (EB-65): only admitted publishes
    // count — a schema rejection or rate-limit shed above never reaches
    // this line. Anonymous publishes move no per-publisher counter.
    if (publisherId !== undefined) {
      this.publisherCounts.set(publisherId, (this.publisherCounts.get(publisherId) ?? 0) + 1);
    }
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
        this.countRateLimited(topic, payload, publisherId);
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
      if (autoTrainer !== undefined) autoTrainer.sample(payload, this.binaryMessages.has(msg));
    }
    // Publish-side per-topic HMAC message signing (EB-84, opt-in via
    // `setTopicSigningKey`). The signature covers (topic, seq, wire
    // bytes): the wire bytes are the post-compression form, so a
    // compressed envelope's signature covers the ciphertext and a
    // verifier checks those same bytes before inflating; for a binary
    // message (EB-92) the wire bytes are the raw payload bytes. Signed
    // with the rule's newest key; the {kid, sig} rides the envelope, the
    // durable log, and the bridge mirror. A payload with no JSON encoding
    // cannot be signed — it travels unsigned and fails closed on
    // `verifySignature` subscribers.
    const signingRule = this.signingForTopic(topic);
    if (signingRule !== undefined) {
      // The signature covers the wire bytes (EB-92): a binary message's
      // raw bytes, or the post-compression wire form for ordinary
      // messages — a verifier checks those same bytes before inflating.
      const wireBytes = wireBytesOfPayload(msg.payload, this.binaryMessages.has(msg));
      if (wireBytes !== undefined) {
        msg.signature = signTopicMessage(signingRule.keys[0], topic, msg.seq, wireBytes);
      }
    }
    // Publish-side per-publisher HMAC identity signing (EB-97, opt-in
    // via `setPublisherSigningKey`). Orthogonal to the topic signature
    // above: a message can carry both. The signature covers
    // (topic, seq, publisherId, wire bytes) — the same post-compression
    // wire bytes the topic signature covers — and rides the envelope end
    // to end (durable log, replay, retained snapshot). A bridge hop
    // re-admits under the destination bus's rules, so the destination
    // re-signs with its own publisher keys — signatures are per-bus.
    // Runs AFTER redaction
    // (EB-68, admission-time): the signature covers the redacted bytes
    // and a `verifyPublisher` subscriber checks those same bytes, so
    // redaction never breaks verification. A publisher with no
    // registered key set, or a payload with no JSON encoding, travels
    // unsigned and fails closed on `verifyPublisher` subscribers.
    const signerPublisherId = msg.publisherId;
    if (signerPublisherId !== undefined) {
      const publisherRule = this.publisherSigningKeys.get(signerPublisherId);
      if (publisherRule !== undefined) {
        const wireBytes = wireBytesOfPayload(msg.payload, this.binaryMessages.has(msg));
        if (wireBytes !== undefined) {
          msg.publisherSignature = signPublisherMessage(
            publisherRule.keys[0],
            topic,
            msg.seq,
            signerPublisherId,
            wireBytes,
          );
        }
      }
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
    // deadline, the destination's rules apply normally. A pause-buffer
    // drain (EB-88) carries the deadline stamped at pause admission the
    // same way — the message was "published" at pause time and only
    // delivered at resume — so it wins over every other source.
    const expiresAt =
      pausedExpiresAt !== undefined
        ? pausedExpiresAt
        : routedExpiresAt !== undefined
          ? routedExpiresAt
          : delayed !== undefined
            ? delayed.expiresAt
            : ttlMs === undefined
              ? undefined
              : nowMs + ttlMs;
    if (expiresAt !== undefined) this.messageDeadlines.set(msg, expiresAt);
    // MQTT-style retained messages (EB-87): the message above cleared
    // every admission gate and is fully stamped (wire payload, signature,
    // TTL deadline) — a `retain: true` publish now becomes (or clears)
    // the topic's retained message. The message itself was already an
    // ordinary publish; only the retained-store update is conditional.
    if (retain === true) {
      this.updateRetained(topic, msg, payload, nowMs, expiresAt, compressedDictId);
    }
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
        // Cross-bus signature (EB-84): when a signing rule matched, the
        // bridge envelope carries a signature over (topic, seq, canonical
        // RAW payload bytes) — the wire bytes the envelope signature
        // covers are post-compression and cannot be recomputed by the
        // receiver, but the raw canonical bytes can, so the receiving bus
        // verifies this signature against its own signing rules for the
        // topic (see `receiveFromBridge`). The destination still re-admits
        // and re-signs under its own rules — signatures are per-bus, and
        // a bridge hop is a new admission.
        ...(() => {
          if (signingRule === undefined) return {};
          const raw = serializeToJson(payload);
          if (raw === undefined) return {};
          const bridgeSig = signTopicMessage(
            signingRule.keys[0],
            topic,
            msg.seq,
            Buffer.from(raw, 'utf8'),
          );
          return { signature: { kid: bridgeSig.kid, sig: bridgeSig.sig, seq: msg.seq } };
        })(),
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
      // Binary payloads (EB-92) persist as v2 log lines (`payload_b64`,
      // explicit type marker) so replay restores raw bytes.
      ...(binary ? { binary: true } : {}),
      // The assigned partition rides the record (EB-90): replay restores
      // it onto the envelope and restart recovery rebuilds the
      // per-(topic, partition) sequence counters from it, so numbering
      // never restarts and never reuses a number across restarts.
      partition: resolvedPartition,
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
      // The publisher identity rides the log record so replay (and the
      // delayed-schedule rebuild) restores the message's producer
      // attribution instead of losing it across restarts.
      ...(publisherId === undefined ? {} : { publisherId }),
      // The publish-time HMAC signature rides the log record so replay
      // restores it onto the envelope — a `verifySignature` subscriber
      // checks a replayed message against the same (topic, seq, wire
      // bytes) the publisher signed.
      ...(msg.signature === undefined
        ? {}
        : { signature: { kid: msg.signature.kid, sig: msg.signature.sig } }),
      // The publisher-identity HMAC signature rides the log record so
      // replay restores it onto the envelope — a `verifyPublisher`
      // subscriber checks a replayed message against the same
      // (topic, seq, publisherId, wire bytes) the publisher signed.
      ...(msg.publisherSignature === undefined
        ? {}
        : {
            publisherSignature: {
              kid: msg.publisherSignature.kid,
              sig: msg.publisherSignature.sig,
            },
          }),
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
            // The publisher identity rides along too — the forward is a
            // new admission on the destination, attributable to the same
            // producer.
            eventTime,
            undefined, // causal: the route forward re-stamps nothing causal
            publisherId,
            undefined, // schemaVersion: the destination admission resolves the version
            undefined, // retain: routes never retain
            undefined, // pausedExpiresAt
            undefined, // partitionKey
            undefined, // partition
            binary,
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
            // Same logical publish, new bus: the producer attribution
            // crosses with the frame.
            ...(publisherId === undefined ? {} : { publisherId }),
            // Binary payloads (EB-92) cross as bytes with the marker, so
            // the destination bus stays byte-oriented.
            ...(binary ? { binary: true } : {}),
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
    // Barrier cut marker (EB-98): present exactly when `msg` is a barrier
    // control message — threaded through to `deliverToSubscriber`, where
    // the subscriber-side barrier gate arms the hold. Data paths leave it
    // undefined.
    barrier?: { epoch: number },
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
        if (subscriber.shadow !== undefined) {
          // Shadow mirror (EB-63): the subscription receives a full copy
          // of production traffic in its own queue, but it is not
          // production fan-out — never counted in matched/accepted, never
          // competing with a group, never advancing group assignment
          // watermarks or committed offsets (EB-32). The shadow copy
          // bypasses the per-subscriber ordering gates (keyed/causal): it
          // mirrors fan-out order into an independent queue.
          this.deliverShadow(subscriber, msg, expiresAt, rawPayload);
          continue;
        }
        matched += 1;
        if (this.deliverToSubscriber(subscriber, msg, expiresAt, rawPayload, keyed, causal, barrier)) accepted += 1;
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
        const partition = this.alignedGroupPartition(partitionCount, topic, msg.seq, msg.partition, keyed?.key);
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
        if (this.deliverToSubscriber(assignee, msg, expiresAt, rawPayload, keyed, causal, barrier)) accepted += 1;
        // A barrier control message (EB-98) carries no business seq — it
        // must not move the group's data offsets: recording seq 0 would
        // regress the committed offset below real deliveries.
        if (barrier === undefined) {
          this.recordPartitionOffset(key, partition, topic, msg.seq);
          this.recordGroupOffset(hit.groupId, topic, msg.seq);
        }
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
      if (this.deliverToSubscriber(assignee, msg, expiresAt, rawPayload, keyed, causal, barrier)) accepted += 1;
      // Barrier control messages (EB-98) carry no business seq — see the
      // partitioned branch above: they never move group offsets.
      if (barrier === undefined) {
        this.recordGroupOffset(hit.groupId, topic, msg.seq);
      }
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
    let rampingSubscribers = 0;
    let migratingSubscribers = 0;
    const deliveryLatency: BusStats['deliveryLatency'] = [];
    const ackLatency: BusStats['ackLatency'] = [];
    const subscriberLatencyP99: BusStats['subscriberLatencyP99'] = [];
    const lag: BusStats['lag'] = [];
    const rateLimitedWaiting: BusStats['rateLimitedWaiting'] = [];
    const inFlightUsage: BusStats['inFlightUsage'] = [];
    const shadows: BusStats['shadows'] = [];
    const replayProgress: BusStats['replayProgress'] = [];
    const hotKeys: HotKeyStat[] = [];
    let causalBufferDepth = 0;
    let barrierHeldMessages = 0;
    for (const subscriber of this.subscribers.values()) {
      unackedDeliveries += subscriber.reliable?.tracker.unackedCount ?? 0;
      // Subscription migration (EB-82): a paused-for-export subscription
      // still counts as live everywhere else.
      if (subscriber.migrating === true) migratingSubscribers += 1;
      if (subscriber.throttle?.throttled === true) throttledSubscribers += 1;
      if (subscriber.health?.degraded === true) degradedSubscribers += 1;
      if (subscriber.deliveryShaping?.shaping === true) shapedSubscribers += 1;
      const slowStart = subscriber.slowStart;
      if (slowStart !== undefined && !slowStart.isComplete(ratesNow)) rampingSubscribers += 1;
      // Shadow mirror counters (see `SubscribeOptions.shadow`): the
      // independent stats surface for shadow traffic, in subscription
      // order — one row per live shadow subscription.
      const shadow = subscriber.shadow;
      if (shadow !== undefined) {
        shadows.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          shadowed: shadow.delivered,
          dropped: shadow.dropped,
          queueSize: subscriber.queue.size,
        });
      }
      // Durable-log replay progress (EB-71): one row per subscription
      // that ran a resume replay — a snapshot of its latest pass, kept
      // on the subscriber after the pass finishes.
      const rp = subscriber.replayProgress;
      if (rp !== undefined) {
        replayProgress.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          total: rp.total,
          replayed: rp.replayed,
          skipped: rp.skipped,
          done: rp.done,
          failed: rp.failed,
          ...(rp.error === undefined ? {} : { error: rp.error }),
        });
      }
      const causalGate = subscriber.causal;
      if (causalGate !== undefined) {
        for (const buffer of causalGate.buffers.values()) causalBufferDepth += buffer.size;
      }
      // Barrier hold depth (EB-98): messages currently held behind armed
      // barriers, per opted-in subscriber.
      const barrierGate = subscriber.barrier;
      if (barrierGate !== undefined) {
        barrierHeldMessages += barrierGate.held.length;
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
      // In-flight cap usage (see `SubscribeOptions.maxInflight`): only
      // subscribers that opted in appear — a pure read of the live
      // counter, so the accounting is never disturbed.
      const inflight = subscriber.maxInflight;
      if (inflight != null) {
        inFlightUsage.push({
          subscriberId: subscriber.id,
          pattern: subscriber.pattern,
          maxInflight: inflight.max,
          inFlight: inflight.inFlight,
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
      blockedMessages: this.totalBlocked,
      blockedSubscribers: [...this.subscribers.values()].filter((s) => s.blocked).length,
      shadowedMessages: this.totalShadowed,
      shadowDroppedMessages: this.totalShadowDropped,
      shadows,
      queueBytes: [...this.subscribers.values()].reduce(
        (sum, subscriber) => sum + subscriber.queue.queueBytes,
        0,
      ),
      throttledMessages: this.totalThrottled,
      expiredMessages: this.totalExpired,
      expiredLeases: this.totalExpiredLeases,
      idleExpired: this.totalIdleExpired,
      aggregatedWindows: this.totalAggregatedWindows,
      unackedDeliveries,
      patternCacheSize: this.patternCache.size,
      compositeSubscriptions: this.compositeSubscriptionCount,
      migratingSubscribers,
      indexSize: this.prefixIndex.size,
      sequenceGaps: this.totalSequenceGaps,
      rateLimitedMessages: this.totalRateLimited,
      publisherRateLimited: this.totalPublisherRateLimited,
      pausedMessages: this.totalPaused,
      // Active topic-pause rules (EB-88), in registration order.
      pausedTopics: [...this.pauseRules.entries()].map(([topicPattern, rule]) => ({
        topicPattern,
        mode: rule.mode,
        bufferedMessages: rule.buffer.length,
      })),
      rejectedMessages: this.totalRejected,
      deprecatedSchemaVersions: this.totalDeprecatedSchemaVersions,
      unregisteredTopicPublishes: this.unregisteredTopicPublishes,
      lateMessages: this.totalLateMessages,
      authzDenied: this.totalAuthzDenied,
      duplicateMessages: this.totalDuplicates,
      dedupDropped: this.totalDedupDropped,
      // Per-publisher publish totals (EB-65), most-published first; ties
      // break on the identity so the ranking is deterministic across
      // snapshots.
      publishers: [...this.publisherCounts.entries()]
        .map(([publisher, publishedMessages]) => ({ publisher, publishedMessages }))
        .sort((a, b) => {
          if (b.publishedMessages !== a.publishedMessages) return b.publishedMessages - a.publishedMessages;
          return a.publisher < b.publisher ? -1 : 1;
        }),
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
      signatureFailures: this.totalSignatureFailures,
      deadLetteredMessages: this.totalDeadLettered,
      ackedRanges: this.totalAckedRanges,
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
      redactedFields: this.totalRedactedFields,
      throttledSubscribers,
      degradedSubscribers,
      shapedSubscribers,
      rampingSubscribers,
      rateLimitedWaiting,
      inFlightUsage,
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
      barrier: {
        publishedBarriers: this.totalBarriersPublished,
        droppedBarriers: this.totalBarriersDropped,
        heldMessages: barrierHeldMessages,
        releasedMessages: this.totalBarrierReleased,
        overflowedHolds: this.totalBarrierOverflows,
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
      replayProgress,
      // MQTT-style retained messages (EB-87): one row per concrete
      // topic currently holding a retained message, sorted by topic. A
      // retained message past its TTL deadline is filtered out — the
      // snapshot never resurrects it.
      retainedTopics: [...this.retainedMessages.values()]
        .filter((entry) => entry.expiresAt === undefined || entry.expiresAt > ratesNow)
        .sort((a, b) => (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0))
        .map((entry) => ({ topic: entry.topic, seq: entry.seq, at: entry.at })),
      topicSubscriberLimits: [...this.subscriberLimitRules.entries()].map(([rule, maxSubscribers]) => {
        let currentSubscribers = 0;
        for (const sub of this.subscribers.values()) {
          if (this.subscriberLimitRuleFor(sub.pattern) === rule) currentSubscribers += 1;
        }
        return { rule, maxSubscribers, currentSubscribers };
      }),
      // Versioned schema registry (EB-86): one row per registration key
      // (concrete topic or pattern, in registration order), versions
      // ascending, validators never exposed — only the metadata. Empty
      // when no versioned schema is registered.
      topicSchemas: [...this.schemaVersionRules.entries()].map(([rule, versions]) => {
        const latest = this.latestSchemaVersion(versions);
        return {
          rule,
          latest,
          versions: [...versions.values()]
            .sort((a, b) => a.version - b.version)
            .map((r) => ({
              version: r.version,
              registeredAt: r.registeredAt,
              graceMs: r.graceMs,
              ...(r.supersededAt === undefined ? {} : { supersededAt: r.supersededAt }),
              // Compatibility gate result (EB-95): 'passed' means the
              // version's validator accepted the full opt-in corpus at
              // registration time; 'not-tested' means no corpus was given.
              compatGate: r.compatGate,
              compatCorpusSize: r.compatCorpusSize,
            })),
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
        publisherRateLimited: stats.publisherRateLimited,
        pausedMessages: stats.pausedMessages,
        rejectedMessages: stats.rejectedMessages,
        deprecatedSchemaVersions: stats.deprecatedSchemaVersions,
        lateMessages: stats.lateMessages,
        // Resolved with the currently configured allowed lateness, so a
        // reconfigured `setTopicAllowedLateness` is reflected here
        // immediately; `undefined` when no event-time publish has ever
        // been admitted on the topic.
        watermark: this.eventWatermarks.get(topic)?.watermarkFor(this.allowedLatenessForTopic(topic)),
        duplicateMessages: stats.duplicateMessages,
        aliasRetiredMessages: stats.aliasRetiredMessages,
        redactedFields: stats.redactedFields,
        filteredMessages: stats.filteredMessages,
        signatureFailures: stats.signatureFailures,
        // Per-topic HMAC signing state (EB-84): present only when a
        // signing rule currently matches the topic — secrets never leave
        // the rule, only the key ids and the grace window. Omitted
        // otherwise, so stats snapshots without signing are byte-identical
        // to before.
        ...(() => {
          const rule = this.signingForTopic(topic);
          return rule === undefined
            ? {}
            : { signing: { keyIds: rule.keys.map((k) => k.kid), graceMs: rule.graceMs } };
        })(),
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
        // Per-partition publish counters (EB-90): present only when the
        // topic resolves to more than one partition — single-partition
        // topics omit the field so their snapshots are byte-identical to
        // before. Rows cover every configured partition in ascending
        // order (zero when a partition has seen no publish yet), so the
        // shape is stable for a given rule rather than sparse.
        ...(() => {
          const partitionCount = this.partitionsForTopic(topic);
          if (partitionCount <= 1) return {};
          const perTopic = this.partitionPublished.get(topic);
          const partitions: Array<{ partition: number; publishedMessages: number; lastSeq: number }> =
            [];
          for (let p = 0; p < partitionCount; p++) {
            const row = perTopic?.get(p);
            partitions.push({
              partition: p,
              publishedMessages: row?.publishedMessages ?? 0,
              lastSeq: row?.lastSeq ?? 0,
            });
          }
          return { partitions };
        })(),
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
      // Outbox relay lag (EB-102): probed only when a relay is attached;
      // a DB-backed store answers with one bounded scan.
      ...(this.outboxRelay === undefined
        ? {
            outboxPending: 0,
            outboxLagMs: 0,
            outboxCursor: 0,
            outboxRelayed: 0,
          }
        : (() => {
            const lag = this.outboxRelay.getLag();
            return {
              outboxPending: lag.pending,
              outboxLagMs: lag.oldestMs,
              outboxCursor: lag.cursor,
              outboxRelayed: lag.relayed,
            };
          })()),
    };
  }

  /**
   * One-shot structured health diagnosis (EB-101): folds the current
   * bus state into a graded report `{ status, at, checks }` — `healthy`
   * when every check passes, `degraded` when any check warns, `critical`
   * when any check fails. Each check carries its evidence (`detail`) and
   * an operator action (`suggestion`).
   *
   * Purely read-only: it assembles a snapshot from `getStats()` and a
   * few read-only internal views (DLQ occupancy, per-subscriber lag
   * thresholds) and hands it to `diagnoseBusHealth` in `src/doctor.ts`.
   * It never publishes, never writes the durable log, never mutates
   * state, and never fires callbacks.
   */
  exportBusHealth(options?: DoctorOptions): BusHealthReport {
    const stats = this.getStats();
    const dlqBacklog: DoctorInput['dlqBacklog'] = [];
    const lagThresholdMs = new Map<string, number>();
    for (const sub of this.subscribers.values()) {
      if (sub.deadLetter !== undefined) {
        dlqBacklog.push({
          subscriberId: sub.id,
          pattern: sub.pattern,
          entries: sub.deadLetter.entries.length,
          maxEntries: sub.deadLetter.maxEntries,
        });
      }
      if (sub.lag?.thresholdMs !== undefined) {
        lagThresholdMs.set(sub.id, sub.lag.thresholdMs);
      }
    }
    const input: DoctorInput = {
      at: this.now(),
      aliases: stats.aliases,
      slowestSubscribers: stats.slowestSubscribers,
      lagWatermarks: stats.lag.map((row) => ({
        subscriberId: row.subscriberId,
        pattern: row.pattern,
        watermarkMs: row.watermarkMs,
      })),
      breachingSubscribers: stats.subscriberLatencyP99
        .filter((row) => row.breaching)
        .map((row) => ({
          subscriberId: row.subscriberId,
          pattern: row.pattern,
          p99Ms: row.p99Ms,
          thresholdMs: row.thresholdMs,
        })),
      lagThresholdMs,
      dlqBacklog,
      groupLag: stats.groupLag,
      groupLagThresholdFor: (groupId: string) => this.groupLagMonitor.thresholdFor(groupId),
      consumerGroups: stats.consumerGroups,
      namespaces: stats.namespaces,
      durableLog: stats.durableLog,
    };
    return diagnoseBusHealth(input, options);
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

  /**
   * Messages dropped for a subscriber due to backpressure: queue sheds
   * plus messages refused while blocked under the `'block'` tier (EB-83).
   */
  droppedCount(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return subscriber.queue.droppedCount + subscriber.blockedCount;
  }

  /**
   * Messages refused for a subscriber while blocked under the `'block'`
   * backpressure tier (EB-83) — the backlog-preserving alternative to
   * shedding. Throws for an unknown subscriber, like `droppedCount`.
   */
  blockedCount(subId: string): number {
    const subscriber = this.subscribers.get(subId);
    if (subscriber == null) throw new Error(`unknown subscriber: ${subId}`);
    return subscriber.blockedCount;
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
   * Drain path for shadow subscribers (see `SubscribeOptions.shadow`):
   * dequeues the mirror copy and hands each message to the shadow
   * handler with throw isolation. Shadow deliveries are tracked in the
   * shadow's independent counters only — never in `totalDelivered`, and
   * never in the topic's `sequenceGaps` (a shadow tracks no gap
   * baseline; loss is visible in its `dropped` counter). A shadow copy
   * whose TTL deadline passed before the drain is discarded silently:
   * the production copy was already accounted for, so counting it again
   * would double-count one logical expiry.
   */
  private drainShadowSubscriber(
    subscriber: Subscriber,
    shadow: ShadowState,
    nowMs: number,
  ): void {
    const { live } = subscriber.queue.drainLive(nowMs);
    for (const msg of live) {
      // Compressed payloads inflate once per message before any handler
      // sees them — idempotent, so the production copy's earlier
      // inflation is harmless (see `inflateMessagePayload`).
      this.inflateMessagePayload(msg);
      // Sampled before the handler runs: queue dwell, not processing
      // time — same rule as production.
      this.recordDeliveryLatency(subscriber, msg, nowMs);
      this.recordLagSample(subscriber, msg, nowMs);
      shadow.delivered += 1;
      this.totalShadowed += 1;
      const invocationSlo = this.invocationLatencySlo(subscriber);
      if (invocationSlo == null) {
        this.invokeShadowHandler(subscriber, msg);
      } else {
        const processingStartedAtMs = this.now();
        try {
          this.invokeShadowHandler(subscriber, msg);
        } finally {
          this.recordProcessingLatency(subscriber, invocationSlo, processingStartedAtMs);
        }
      }
    }
    // Re-evaluate the lag watermark after the drain, mirroring the
    // production path's tail.
    const lag = subscriber.lag;
    if (lag != null) this.checkLag(subscriber, lag, nowMs);
  }

  /**
   * Invokes a shadow subscriber's handler with throw isolation: a
   * throwing shadow handler must never break the flush loop or affect
   * production delivery. The swallow lives inside the traced invocation
   * so a sampled trace still records the `bus.deliver` span for the
   * mirror copy. The message still counts as shadowed — it was handed to
   * the handler, exactly like production's deliveredMessages counting a
   * throwing handler's message.
   */
  private invokeShadowHandler(subscriber: Subscriber, msg: BusMessage): void {
    let delivered = false;
    this.deliverTraced(subscriber, msg, () => {
      try {
        subscriber.handler(msg);
        delivered = true;
      } catch {
        // Swallowed: shadow validation code must never disturb
        // production delivery or the flush loop.
      }
    });
    // EB-72: a mirror copy handed to the shadow handler without a throw
    // is idle activity too — a shadow that keeps receiving mirrors is
    // not idle. (Shadow subscriptions can never be reliable, so no
    // wrapper carve-out is needed here.)
    if (delivered) this.recordIdleActivity(subscriber, 'deliver');
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
    // Subscription migration (EB-82): an exported subscription pauses
    // delivery — its backlog stays queued under the normal backpressure
    // policy, untouched by the drain, so the target instance picks up
    // exactly where the source paused. Nothing is dropped by the pause
    // itself; queued messages wait for `completeMigration` (teardown) or
    // `cancelMigration` (resume). Mirrors the degraded health-probe pause
    // below.
    if (subscriber.migrating === true) return;
    // Shadow subscribers drain through their own isolated path: their
    // deliveries never touch production counters, and a throwing shadow
    // handler never escapes into this flush loop.
    const shadow = subscriber.shadow;
    if (shadow !== undefined) {
      this.drainShadowSubscriber(subscriber, shadow, nowMs);
      return;
    }
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
      const maxInflight = subscriber.maxInflight;
      const slowStart = subscriber.slowStart;
      // Shaping paces the round first (smoothing bursts); the sliding
      // window then enforces the hard cap; the in-flight cap bounds how
      // many unsettled handler invocations one round may open; the
      // slow-start gate (EB-75) paces a fresh subscription's ramp — a
      // message is delivered only when all four allow it. At the
      // in-flight cap the drain pulls nothing new: queued messages wait
      // in FIFO order, never dropped, until a handler settles and frees
      // a slot.
      const shapingMaxLive = shaping == null ? Infinity : Math.floor(shaping.bucket.availableTokens);
      const rateLimitMaxLive = rateLimit == null ? Infinity : rateLimit.limiter.budget(nowMs);
      const inflightMaxLive =
        maxInflight == null ? Infinity : Math.max(0, maxInflight.max - maxInflight.inFlight);
      const slowStartMaxLive =
        slowStart == null ? Infinity : Math.floor(slowStart.availableTokens(nowMs));
      const { live, expired } = subscriber.queue.drainLiveUpTo(
        nowMs,
        Math.min(shapingMaxLive, rateLimitMaxLive, inflightMaxLive, slowStartMaxLive),
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
          // The slow-start gate consumes one token per delivered message,
          // mirroring the shaping take — same guarantee, same reasoning.
          slowStart?.take(nowMs);
          // The window counts actual deliveries, mirroring the shaping
          // take: dequeued-but-expired messages never reach this loop, so
          // TTL expiry consumes no window budget.
          rateLimit?.limiter.record(nowMs);
          // Compressed payloads are inflated here — once per message, before
          // any handler sees it — so every subscriber transparently receives
          // the original payload. See `inflateMessagePayload` for the
          // idempotency and envelope-collision notes.
          this.inflateMessagePayload(msg);
          this.noteMessageDelivered(subscriber, msg);
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
          // EB-67: one in-flight slot per handler invocation; the slot is
          // released when the invocation settles (see
          // invokeWithInflightSlot).
          this.invokeWithInflightSlot(subscriber, (): unknown => {
            let result: unknown;
            if (invocationSlo == null) {
              result = this.deliverTraced(subscriber, msg, () => subscriber.handler(msg));
            } else {
              const processingStartedAtMs = this.now();
              try {
                result = this.deliverTraced(subscriber, msg, () => subscriber.handler(msg));
              } finally {
                this.recordProcessingLatency(subscriber, invocationSlo, processingStartedAtMs);
              }
            }
            // EB-72: the handler returned without throwing — a successful
            // delivery counts as idle activity. Not for reliable
            // subscribers: their at-least-once wrapper can swallow a
            // throwing handler (the DLQ nack path), so reliable deliveries
            // record at the user handler's own success point inside the
            // wrapper instead.
            if (subscriber.reliable == null) this.recordIdleActivity(subscriber, 'deliver');
            return result;
          });
        }
      } else {
        for (let i = 0; i < live.length; i += 1) {
          shaping?.bucket.take();
          slowStart?.take(nowMs);
          rateLimit?.limiter.record(nowMs);
          this.inflateMessagePayload(live[i]);
          this.invokeWithInflightSlot(subscriber, (): unknown =>
            this.deliverWithHealth(subscriber, live[i], nowMs),
          );
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
      if (slowStart != null) {
        this.updateSlowStart(subscriber, slowStart, nowMs, subscriber.queue.size > 0);
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
    const slowStart = subscriber.slowStart;
    const shapingBudget = shaping == null ? Infinity : Math.floor(shaping.bucket.availableTokens);
    // Pending messages already reserve window budget (recorded at hand-off
    // in deliverBatch), so collection must not exceed what is still free —
    // otherwise a batch filled across several rounds could over-deliver the
    // window before any delivery is recorded.
    const rateLimitBudget =
      rateLimit == null ? Infinity : Math.max(0, rateLimit.limiter.budget(nowMs) - batch.pending.length);
    const slowStartBudget =
      slowStart == null ? Infinity : Math.floor(slowStart.availableTokens(nowMs));
    const budget = Math.min(shapingBudget, rateLimitBudget, slowStartBudget);
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
      if (slowStart != null) {
        this.updateSlowStart(
          subscriber,
          slowStart,
          nowMs,
          subscriber.queue.size > 0 || batch.pending.length > 0,
        );
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
    if (slowStart != null) {
      this.updateSlowStart(
        subscriber,
        slowStart,
        nowMs,
        subscriber.queue.size > 0 || batch.pending.length > 0,
      );
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
    // The whole batch occupies slow-start budget the same way: one token
    // per message, mirroring the plain path.
    const slowStart = subscriber.slowStart;
    if (slowStart != null) {
      for (let i = 0; i < live.length; i += 1) slowStart.take(nowMs);
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
      this.noteMessageDelivered(subscriber, msg);
      this.totalDelivered += 1;
      // Sampled before the handler runs: queue dwell, not processing time.
      this.recordDeliveryLatency(subscriber, msg, nowMs);
      this.recordLagSample(subscriber, msg, nowMs);
    }
    if (subscriber.health == null) {
      this.invokeBatchWithProcessingLatency(subscriber, () => {
        this.deliverBatchTraced(subscriber, live, () =>
          subscriber.handler(live as unknown as BusMessage),
        );
        // EB-72: the batch handler returned without throwing — idle
        // activity. Reliable batches record inside their at-least-once
        // wrapper instead (see drainSubscriber).
        if (subscriber.reliable == null) this.recordIdleActivity(subscriber, 'deliver');
      });
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
      // EB-72: a clean batch-handler return is idle activity (reliable
      // batches record inside their at-least-once wrapper instead).
      if (subscriber.reliable == null) this.recordIdleActivity(subscriber, 'deliver');
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
   * Folds one admitted message into a windowed subscriber's open
   * tumbling window (see `SubscribeOptions.window`, EB-73). Opens the
   * window on the first message; a message arriving at or past the
   * open window's end closes it first (tumbling, never overlapping —
   * this is what makes the window boundary follow the injected bus
   * clock even when the real-time close timer has not fired yet).
   * Returns `true`: the message was admitted to the subscriber's
   * window, like a queue admission.
   */
  private accumulateWindowed(
    subscriber: Subscriber,
    window: SubscriberWindowState,
    msg: BusMessage,
    expiresAt: number | undefined,
    rawPayload: unknown,
  ): boolean {
    const nowMs = this.now();
    // Admission-time expiry: a message already past its TTL never enters
    // the window — counted as expired, like a queue drop at drain. A
    // message that outlives its TTL inside the short window still
    // contributes (expiry is not re-evaluated at close).
    if (expiresAt !== undefined && nowMs >= expiresAt) {
      this.recordExpired(msg.topic);
      return true;
    }
    if (window.count > 0 && nowMs >= window.windowEnd) {
      this.closeWindow(subscriber, window);
    }
    if (window.count === 0) {
      window.windowStart = nowMs;
      window.windowEnd = nowMs + window.windowMs;
      this.armWindowTimer(subscriber, window);
    }
    const value = extractWindowValue(rawPayload, window.fieldPath);
    switch (window.aggregate) {
      case 'count':
        window.count += 1;
        break;
      case 'sum':
      case 'avg':
      case 'min':
      case 'max':
        // Only finite numbers fold: a non-numeric message is ignored
        // entirely (not counted), so a numeric aggregate always describes
        // its numeric substream.
        if (typeof value === 'number' && Number.isFinite(value)) {
          window.count += 1;
          window.numericCount += 1;
          window.sum += value;
          if (value < window.min) window.min = value;
          if (value > window.max) window.max = value;
        }
        break;
      case 'last':
        window.count += 1;
        window.last = value;
        break;
      case 'concat':
        // An undefined extraction contributes nothing printable: skip it
        // rather than concatenating the string "undefined".
        if (value !== undefined) {
          window.count += 1;
          window.parts.push(String(value));
        }
        break;
    }
    window.lastTopic = msg.topic;
    window.lastSeq = msg.seq;
    window.lastEpoch = msg.epoch ?? '';
    return true;
  }

  /**
   * Folds the open window's accumulators into the aggregate value for
   * `WindowAggregateResult.value`.
   */
  private windowValue(window: SubscriberWindowState): unknown {
    switch (window.aggregate) {
      case 'count':
        return window.count;
      case 'sum':
        return window.numericCount === 0 ? null : window.sum;
      case 'avg':
        return window.numericCount === 0 ? null : window.sum / window.numericCount;
      case 'min':
        return window.numericCount === 0 ? null : window.min;
      case 'max':
        return window.numericCount === 0 ? null : window.max;
      case 'last':
        return window.last;
      case 'concat':
        return window.parts.join('');
    }
  }

  /**
   * Closes a subscriber's open tumbling window: builds the aggregate
   * message and enqueues it through the subscriber's normal delivery
   * path (backpressure queue, reliable delivery, drain-time shaping —
   * the aggregate is one ordinary queued message). An empty window
   * delivers nothing and counts nothing. Increments
   * `getStats().aggregatedWindows` for every completed non-empty
   * window.
   */
  private closeWindow(subscriber: Subscriber, window: SubscriberWindowState): void {
    this.clearWindowTimer(subscriber);
    const count = window.count;
    const windowStart = window.windowStart;
    const windowEnd = window.windowEnd;
    const value = this.windowValue(window);
    const topic = window.lastTopic;
    const seq = window.lastSeq;
    const epoch = window.lastEpoch;
    this.resetWindowState(window);
    if (count === 0) return;
    // The subscriber may have unsubscribed between the timer firing and
    // this close (or re-entrantly during close): only a still-registered
    // subscriber enqueues the aggregate.
    if (this.subscribers.get(subscriber.id) !== subscriber) return;
    const aggregate: BusMessage = {
      topic,
      payload: { windowStart, windowEnd, count, value } satisfies WindowAggregateResult,
      seq,
      ...(epoch === '' ? {} : { epoch }),
    };
    this.totalAggregatedWindows += 1;
    this.enqueueMessage(subscriber, aggregate, undefined);
    // The aggregate was enqueued outside any publish fan-out — no flush
    // is scheduled yet. Without this the aggregate would sit in the
    // queue until unrelated traffic arrived.
    this.scheduleFlush();
  }

  /**
   * Resets a window's accumulators after close (or when its subscriber
   * goes away): no window is open afterwards.
   */
  private resetWindowState(window: SubscriberWindowState): void {
    window.windowStart = 0;
    window.windowEnd = 0;
    window.count = 0;
    window.sum = 0;
    window.numericCount = 0;
    window.min = Infinity;
    window.max = -Infinity;
    window.last = undefined;
    window.parts = [];
    window.lastTopic = '';
    window.lastSeq = 0;
    window.lastEpoch = '';
  }

  /**
   * Arms the window-close timer for a newly opened window. The timer is
   * `unref()`'d, so an open window never keeps the process alive; on
   * fire it closes the window only if the subscriber is still
   * registered.
   */
  private armWindowTimer(subscriber: Subscriber, window: SubscriberWindowState): void {
    if (window.timer !== undefined) return;
    const timer = setTimeout(() => {
      window.timer = undefined;
      // The subscriber may have unsubscribed while the timer was armed —
      // closeWindow re-checks registration before enqueuing.
      this.closeWindow(subscriber, window);
    }, window.windowMs);
    const handle = timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    window.timer = timer;
  }

  /**
   * Cancels a windowed subscriber's pending close timer, if any.
   */
  private clearWindowTimer(subscriber: Subscriber): void {
    const window = subscriber.window;
    if (window?.timer !== undefined) {
      clearTimeout(window.timer);
      window.timer = undefined;
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
   * Post-drain slow-start bookkeeping (EB-75): drops the gate once the
   * ramp window has elapsed (the subscriber runs at full speed from here
   * on); otherwise arms the re-flush timer exactly when the gate is the
   * binding constraint — backlog present AND no budget left. A backlog
   * held back by shaping, the window or the in-flight cap while the gate
   * still has room is not slow-start's doing, so no timer spins there
   * (the same precision the rate-limit timer uses).
   */
  private updateSlowStart(
    subscriber: Subscriber,
    slowStart: SlowStartGate,
    nowMs: number,
    hasBacklog: boolean,
  ): void {
    if (slowStart.isComplete(nowMs)) {
      this.clearSlowStartTimer(subscriber);
      subscriber.slowStart = undefined;
      return;
    }
    if (hasBacklog && slowStart.availableTokens(nowMs) < 1) {
      this.armSlowStartTimer(subscriber);
    } else {
      this.clearSlowStartTimer(subscriber);
    }
  }

  /**
   * Arms the re-flush timer for a slow-starting subscriber that is holding
   * messages back: when the ramp accrues budget, the timer triggers
   * another flush so the backlog drains even when no new publishes arrive.
   * The delay is derived from the bus clock (consistent with the gate);
   * the timer itself is a real wall-clock timeout — like the shaping
   * re-flush timer — and never keeps the process alive on its own. Firing
   * only schedules a flush when the gate is still ramping and actually has
   * budget; otherwise it re-arms, so a clock that has not advanced cannot
   * spin the flush loop. When the ramp completes mid-wait the gate is
   * dropped and one flush runs the backlog at full speed.
   */
  private armSlowStartTimer(subscriber: Subscriber): void {
    const slowStart = subscriber.slowStart;
    if (slowStart == null || slowStart.timer !== undefined) return;
    const nowMs = this.now();
    if (slowStart.isComplete(nowMs)) {
      this.clearSlowStartTimer(subscriber);
      subscriber.slowStart = undefined;
      this.scheduleFlush();
      return;
    }
    const delayMs = Math.max(0, Math.ceil(slowStart.msUntilNextToken(nowMs)));
    const timer = setTimeout(() => {
      slowStart.timer = undefined;
      // The subscriber may have unsubscribed while the timer was armed —
      // only a still-registered subscriber holding the same gate proceeds.
      if (this.subscribers.get(subscriber.id) !== subscriber) return;
      const gate = subscriber.slowStart;
      if (gate == null) return;
      const at = this.now();
      if (gate.isComplete(at)) {
        this.clearSlowStartTimer(subscriber);
        subscriber.slowStart = undefined;
        this.scheduleFlush();
        return;
      }
      if (gate.availableTokens(at) < 1) {
        this.armSlowStartTimer(subscriber);
        return;
      }
      this.scheduleFlush();
    }, delayMs);
    // A ramping backlog must not keep the process alive on its own.
    const handle = timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    slowStart.timer = timer;
  }

  /**
   * Cancels a slow-starting subscriber's pending re-flush timer, if any.
   */
  private clearSlowStartTimer(subscriber: Subscriber): void {
    const slowStart = subscriber.slowStart;
    if (slowStart?.timer !== undefined) {
      clearTimeout(slowStart.timer);
      slowStart.timer = undefined;
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
  private deliverWithHealth(subscriber: Subscriber, msg: BusMessage, nowMs: number): unknown {
    const health = subscriber.health;
    if (health == null) return undefined;
    this.noteMessageDelivered(subscriber, msg);
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
    // The handler's return value is captured so the in-flight cap (see
    // `SubscribeOptions.maxInflight`) can hold the slot until a returned
    // promise settles; a throw is still swallowed into the failure
    // accounting exactly as before.
    let result: unknown;
    this.deliverTraced(subscriber, msg, () => {
      try {
        result = subscriber.handler(msg);
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
      // EB-72: a clean handler return is idle activity (reliable
      // deliveries record inside their at-least-once wrapper instead —
      // see drainSubscriber).
      if (subscriber.reliable == null) this.recordIdleActivity(subscriber, 'deliver');
      return result;
    }
    health.consecutiveFailures += 1;
    if (!health.degraded && health.consecutiveFailures >= health.maxConsecutiveFailures) {
      this.pauseForHealth(subscriber, reason);
    }
    return result;
  }

  /**
   * EB-67: invokes the handler for one single-message delivery while
   * holding one in-flight slot when `SubscribeOptions.maxInflight` is
   * enabled. The slot is acquired before the handler runs and released
   * when the invocation settles:
   * - reliable subscriptions settle through the tracker's single settle
   *   path (`ack()` / `nack()` / ack-timeout), which releases the slot via
   *   the tracker's `onSettle` hook (see `subscribeReliable`) — the drain
   *   must not release it here, or a slow consumer's slot would be freed
   *   while its delivery is still outstanding;
   * - plain subscriptions settle when the handler returns: a returned
   *   thenable holds the slot until it settles (fulfilled or rejected), a
   *   synchronous return releases it immediately, and a throwing handler
   *   releases its slot with the error propagating exactly as before.
   * With the cap disabled the handler runs untouched — zero overhead,
   * zero behavior change.
   */
  private invokeWithInflightSlot(subscriber: Subscriber, invoke: () => unknown): void {
    const cap = subscriber.maxInflight;
    if (cap == null) {
      invoke();
      return;
    }
    cap.inFlight += 1;
    if (subscriber.reliable != null) {
      // The tracker's onSettle hook owns the release.
      invoke();
      return;
    }
    // Hoisted before the try (never declared inside it): a Node v24/V8
    // ReferenceError awaits bindings declared in `try` and read in
    // `finally`.
    let result: unknown;
    try {
      result = invoke();
    } catch (err) {
      cap.inFlight -= 1;
      throw err;
    }
    if (isThenable(result)) {
      const release = (): void => this.releaseInflightSlot(subscriber);
      result.then(release, release);
    } else {
      cap.inFlight -= 1;
    }
  }

  /**
   * EB-67: releases one in-flight slot and schedules a flush so queued
   * messages are pulled into the freed slot. Called from the tracker's
   * `onSettle` hook (reliable: ack/nack/ack-timeout) and when a plain
   * handler's returned promise settles.
   */
  private releaseInflightSlot(subscriber: Subscriber): void {
    const cap = subscriber.maxInflight;
    if (cap == null) return;
    cap.inFlight = Math.max(0, cap.inFlight - 1);
    this.scheduleFlush();
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
   * Compares a delivered message's partition-scoped sequence number
   * against the last number delivered to the same subscriber on the same
   * (topic, partition), and counts any skipped numbers into the topic's
   * `sequenceGaps` stat (and the global total). Baselines are tracked per
   * (topic, partition) (EB-90): partitions number independently, so a
   * subscriber interleaving partitions never counts one partition's
   * numbers as another's gaps. The first delivery on a (topic, partition)
   * only establishes the baseline — a subscriber that joined after earlier
   * publishes must not count pre-subscription messages as lost. A delivery
   * at or below the last delivered number is a redelivery (at-least-once
   * `nack()` / ack-timeout requeue): expected, never a gap, and it does
   * not move the baseline backwards.
   *
   * Sequence epochs: the node's own publishes and each hub's forwarded
   * traffic number the same topic independently. When a delivery arrives
   * from a different epoch than the previous one on the same topic, the
   * baseline is re-established — the epoch switch itself is never counted
   * as lost messages.
   */
  private detectGap(subscriber: Subscriber, msg: BusMessage): void {
    const epoch = msg.epoch ?? '';
    const gapKey = EventBus.gapKey(msg.topic, msg.partition ?? 0);
    const lastEpoch = subscriber.lastEpoch.get(gapKey);
    const last = subscriber.lastDeliveredSeq.get(gapKey);
    if (last === undefined || lastEpoch !== epoch) {
      subscriber.lastDeliveredSeq.set(gapKey, msg.seq);
      subscriber.lastEpoch.set(gapKey, epoch);
      return;
    }
    if (msg.seq > last + 1) {
      const missing = msg.seq - last - 1;
      this.totalSequenceGaps += missing;
      const stats = this.topicStats.get(msg.topic);
      if (stats != null) stats.sequenceGaps += missing;
    }
    if (msg.seq > last) subscriber.lastDeliveredSeq.set(gapKey, msg.seq);
  }

  /**
   * Key for the per-subscriber gap baselines (`lastDeliveredSeq` /
   * `lastEpoch`): the (topic, partition) pair. A message without an
   * assigned partition (bridge/cluster ingress, window aggregates) keys
   * as partition 0 — the same partition `fanOut` assigns unpartitioned
   * publishes.
   */
  private static gapKey(topic: string, partition: number): string {
    return `${topic}\0${partition}`;
  }

  // ------------------------------------------------------------------
  // Declarative configuration snapshots (EB-91)
  // ------------------------------------------------------------------

  /**
   * The last snapshot successfully applied via `applyConfig` — the
   * baseline `detectConfigDrift()` compares against when called without
   * an explicit snapshot. A deep clone, so later caller-side mutation
   * of the applied snapshot cannot move the baseline.
   */
  private lastAppliedConfig: BusConfigSnapshot | undefined;

  /**
   * Exports the bus's declarative configuration as a JSON-safe snapshot
   * (EB-91): ACL rules, namespace registrations, per-publisher rate
   * limits, durable-log retention, and the per-topic rule tables (TTL,
   * retained, rate limit, max subscribers, allowed lateness, compression,
   * signing-key *metadata*, alias, route, partitions).
   *
   * Security: the snapshot never contains key material — signing keys
   * export as `{ keyIds, graceMs }` only, and compression dictionaries
   * export as an id only. Route predicates and schema validators are
   * functions and are not exported (see `applyConfig` for the
   * fail-closed restore rules).
   */
  /**
   * Attach a transactional outbox relay (EB-102): the relay drains
   * `store` into this bus — one `publishAtomic` batch per drain cycle —
   * and its lag flows into `getStats()` and the Prometheus exposition.
   *
   * The relay recovers the store's durable cursor on `start()` and
   * starts draining immediately (then on the poll timer, unless
   * `pollIntervalMs <= 0`). Attaching twice throws: stop and detach the
   * old relay first with `detachOutboxRelay()`.
   */
  attachOutboxRelay(store: OutboxStore, opts: OutboxRelayOptions = {}): OutboxRelay {
    if (this.outboxRelay !== undefined) {
      throw new Error(
        'attachOutboxRelay: a relay is already attached — detachOutboxRelay() first',
      );
    }
    const relay = new OutboxRelay(this, store, { now: this.now, ...opts });
    this.outboxRelay = relay;
    return relay;
  }

  /**
   * Detach the attached outbox relay, stopping its poll timer. The
   * store's durable cursor is untouched — re-attaching later resumes
   * from it.
   */
  detachOutboxRelay(): void {
    this.outboxRelay?.stop();
    this.outboxRelay = undefined;
  }

  exportBusConfig(): BusConfigSnapshot {
    const patterns = new Set<string>();
    for (const table of [
      this.ttlRules,
      this.retainedRules,
      this.partitionRules,
      this.signingRules,
      this.rateLimitRules,
      this.subscriberLimitRules,
      this.allowedLatenessRules,
      this.topicAliases,
      this.topicRoutes,
      this.compressionRules,
    ]) {
      for (const key of table.keys()) patterns.add(key);
    }
    const topicRules: Record<string, TopicRuleConfig> = {};
    for (const pattern of [...patterns].sort()) {
      const rule: TopicRuleConfig = {};
      const ttlMs = this.ttlRules.get(pattern);
      if (ttlMs !== undefined) rule.ttlMs = ttlMs;
      const retained = this.retainedRules.get(pattern);
      if (retained !== undefined) rule.retained = retained;
      const rateLimit = this.rateLimitRules.get(pattern);
      if (rateLimit !== undefined) {
        rule.rateLimit = { messagesPerSec: rateLimit.messagesPerSec, burst: rateLimit.burst };
      }
      const maxSubscribers = this.subscriberLimitRules.get(pattern);
      if (maxSubscribers !== undefined) rule.maxSubscribers = maxSubscribers;
      const allowedLatenessMs = this.allowedLatenessRules.get(pattern);
      if (allowedLatenessMs !== undefined) rule.allowedLatenessMs = allowedLatenessMs;
      const compression = this.compressionRules.get(pattern);
      if (compression !== undefined) {
        rule.compression = {
          thresholdBytes: compression.thresholdBytes,
          level: compression.level,
          ...(compression.dictionaryId === undefined ? {} : { dictionaryId: compression.dictionaryId }),
        };
      }
      const signing = this.signingRules.get(pattern);
      if (signing !== undefined) {
        // Metadata only — the secret bytes are never exported.
        rule.signingKeys = {
          keyIds: signing.keys.map((k) => k.kid),
          graceMs: signing.graceMs,
        };
      }
      const alias = this.topicAliases.get(pattern);
      if (alias !== undefined) {
        rule.alias = {
          newTopic: alias.newTopic,
          ...(alias.expiresAt === undefined ? {} : { expiresAt: alias.expiresAt }),
        };
      }
      const route = this.topicRoutes.get(pattern);
      if (route !== undefined) {
        // A predicate-bearing route is config-opaque: the predicate is a
        // function and cannot be serialized. The dst is still exported so
        // drift detection sees dst changes; `applyConfig` refuses to
        // touch such routes (fail-closed).
        rule.route = { dst: route.dst };
      }
      const partitions = this.partitionRules.get(pattern);
      if (partitions !== undefined) rule.partitions = partitions;
      topicRules[pattern] = rule;
    }
    const namespaces: Record<string, NamespaceConfig> = {};
    for (const [prefix, ns] of [...this.namespaces.entries()].sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
    )) {
      namespaces[prefix] = { allowPublish: ns.allowPublish, allowSubscribe: ns.allowSubscribe };
    }
    const publisherRateLimits: Record<string, TopicRateLimitConfig> = {};
    for (const [id, rule] of [...this.publisherRateLimitRules.entries()].sort(([a], [b]) =>
      a < b ? -1 : a > b ? 1 : 0,
    )) {
      publisherRateLimits[id] = { messagesPerSec: rule.messagesPerSec, burst: rule.burst };
    }
    return {
      v: BUS_CONFIG_SNAPSHOT_VERSION,
      exportedAt: this.now(),
      acl: this.getAclRules(),
      namespaces,
      publisherRateLimits,
      ...(this.durableLogMaxAgeMs === undefined ? {} : { durableLogMaxAgeMs: this.durableLogMaxAgeMs }),
      topicRules,
    };
  }

  /**
   * Applies a declarative configuration snapshot (EB-91): diffs the
   * snapshot against the live configuration and applies each change
   * through the same setters the imperative API uses, so every
   * validation rule (`RangeError` on bad values) applies unchanged.
   *
   * Semantics:
   * - `{ dryRun: true }` computes the change list without mutating
   *   anything and without auditing — a preview for operators.
   * - On success the applied paths are returned in application order
   *   and audited as `config_applied`; the snapshot becomes the new
   *   drift-detection baseline.
   * - On failure the first throwing change aborts the apply and every
   *   already-applied change is rolled back to the pre-apply state
   *   (audited as `config_rolled_back`); a `ConfigApplyError` is thrown
   *   carrying the failed path, the applied paths, and whether the
   *   rollback succeeded. The bus is never left half-configured.
   *
   * Fail-closed restores (a snapshot never carries enough to rebuild
   * these, so changing them via `applyConfig` is refused rather than
   * guessed):
   * - signing keys: snapshots carry key ids only, never secrets —
   *   restore keys with `setTopicSigningKey`;
   * - compression dictionaries: snapshots carry the dictionary id only —
   *   re-register bytes with `setTopicCompression`;
   * - routes whose live registration carries a predicate.
   */
  applyConfig(snapshot: BusConfigSnapshot, opts?: { dryRun?: boolean }): ConfigApplyResult {
    assertValidSnapshot(snapshot);
    const before = this.exportBusConfig();
    const changes = diffConfigSnapshots(before, snapshot);
    if (opts?.dryRun === true) {
      return { applied: [], dryRun: true, changes };
    }
    const applied: string[] = [];
    let failedChange: ConfigChange | undefined;
    try {
      for (const change of changes) {
        failedChange = change;
        this.applyConfigChange(change);
        applied.push(change.path);
      }
    } catch (cause) {
      const failedPath = failedChange?.path ?? '<unknown>';
      let rolledBack = false;
      try {
        const rollbackChanges = diffConfigSnapshots(this.exportBusConfig(), before);
        for (const change of rollbackChanges) this.applyConfigChange(change);
        rolledBack = true;
      } catch {
        rolledBack = false;
      }
      this.auditSubscription({
        action: 'config_rolled_back',
        actor: 'config-apply',
        detail: { failedPath, applied, rolledBack },
      });
      throw new ConfigApplyError(failedPath, cause, applied, rolledBack);
    }
    this.lastAppliedConfig = cloneSnapshot(snapshot);
    this.auditSubscription({
      action: 'config_applied',
      actor: 'config-apply',
      detail: { applied },
    });
    return { applied, dryRun: false };
  }

  /**
   * Detects configuration drift (EB-91): diffs the live configuration
   * against a baseline snapshot and returns one entry per drifted
   * setting (`expected` = baseline, `actual` = live). Without an
   * explicit snapshot the baseline is the last successfully applied
   * snapshot; when neither exists a plain `Error` is thrown — there is
   * nothing to compare against.
   */
  detectConfigDrift(baseline?: BusConfigSnapshot): ConfigDrift[] {
    const base = baseline ?? this.lastAppliedConfig;
    if (base === undefined) {
      throw new Error(
        'detectConfigDrift: no baseline snapshot — pass one explicitly or run applyConfig() first',
      );
    }
    assertValidSnapshot(base);
    const current = this.exportBusConfig();
    return diffConfigSnapshots(base, current).map((c) => ({
      path: c.path,
      expected: c.before,
      actual: c.after,
    }));
  }

  /**
   * Applies one granular config change through the imperative setters.
   * Used for both forward applies and rollbacks — the rollback path
   * only ever sees changes diffed against a previously-live snapshot,
   * so the fail-closed branches below cannot trigger on it.
   */
  private applyConfigChange(change: ConfigChange): void {
    const { kind, path, after } = change;
    switch (kind) {
      case 'acl': {
        this.setAclRules(after as BusConfigSnapshot['acl']);
        return;
      }
      case 'retention': {
        this.setDurableLogMaxAgeMs(after as number | undefined, 'config-apply');
        return;
      }
      case 'namespace': {
        const prefix = bracketKeyOf(path);
        if (after === undefined) {
          this.deleteNamespace(prefix);
          return;
        }
        const cfg = after as NamespaceConfig;
        // `createNamespace` rejects duplicates, so a modify is
        // delete-then-create. `deleteNamespace` refuses to delete a
        // namespace with live subscribers — that surfaces here as a
        // failed change and triggers the apply-wide rollback.
        if (this.namespaces.has(prefix)) this.deleteNamespace(prefix);
        this.createNamespace(prefix, {
          allowPublish: cfg.allowPublish,
          allowSubscribe: cfg.allowSubscribe,
        });
        return;
      }
      case 'publisherRateLimit': {
        const id = bracketKeyOf(path);
        if (after === undefined) {
          this.clearPublisherRateLimit(id);
          return;
        }
        const rule = after as TopicRateLimitConfig;
        this.setPublisherRateLimit(id, { messagesPerSec: rule.messagesPerSec, burst: rule.burst });
        return;
      }
      case 'topicRule': {
        const { pattern, ruleKey } = topicRulePathOf(path);
        this.applyTopicRuleChange(pattern, ruleKey, change.before, after);
        return;
      }
    }
  }

  /**
   * Applies one per-topic rule change. The fail-closed branches
   * (signing keys, compression dictionaries, predicate routes) refuse
   * to reconstruct unrestorable state from a snapshot rather than
   * guessing — the operator restores those imperatively.
   */
  private applyTopicRuleChange(
    pattern: string,
    ruleKey: string,
    before: unknown,
    after: unknown,
  ): void {
    switch (ruleKey) {
      case 'ttlMs':
        if (after === undefined) this.clearTopicTtl(pattern);
        else this.setTopicTtl(pattern, after as number);
        return;
      case 'retained':
        if (after === undefined) this.clearTopicRetained(pattern);
        else this.setTopicRetained(pattern, after as boolean);
        return;
      case 'rateLimit':
        if (after === undefined) this.clearTopicRateLimit(pattern);
        else {
          const rule = after as TopicRateLimitConfig;
          this.setTopicRateLimit(pattern, rule.messagesPerSec, { burst: rule.burst });
        }
        return;
      case 'maxSubscribers':
        if (after === undefined) this.clearTopicMaxSubscribers(pattern);
        else this.setTopicMaxSubscribers(pattern, after as number);
        return;
      case 'allowedLatenessMs':
        if (after === undefined) this.clearTopicAllowedLateness(pattern);
        else this.setTopicAllowedLateness(pattern, after as number);
        return;
      case 'partitions':
        if (after === undefined) this.clearTopicPartitions(pattern);
        else this.setTopicPartitions(pattern, after as number);
        return;
      case 'compression': {
        const a = after as TopicCompressionConfig | undefined;
        if (a === undefined) {
          this.clearTopicCompression(pattern);
          return;
        }
        if (a.dictionaryId !== undefined) {
          throw new Error(
            `cannot apply compression rule for pattern "${pattern}" from a snapshot: ` +
              'snapshots carry the dictionary id only, never the dictionary bytes — ' +
              're-register with setTopicCompression',
          );
        }
        this.setTopicCompression(pattern, { thresholdBytes: a.thresholdBytes, level: a.level });
        return;
      }
      case 'signingKeys': {
        if (!configValueEqual(before, after)) {
          throw new Error(
            `cannot apply signing keys for pattern "${pattern}" from a snapshot: ` +
              'snapshots never carry key material — restore keys with setTopicSigningKey',
          );
        }
        return;
      }
      case 'alias': {
        const a = after as TopicAliasConfig | undefined;
        if (a === undefined) {
          this.clearTopicAlias(pattern);
          return;
        }
        // `expiresAt` is absolute bus-clock time; the setter takes a
        // relative TTL. An already-expired alias is equivalent to no
        // alias — clear it instead of registering a stillborn rule.
        const ttlMs = a.expiresAt === undefined ? undefined : a.expiresAt - this.now();
        if (ttlMs !== undefined && ttlMs <= 0) {
          this.clearTopicAlias(pattern);
          return;
        }
        this.setTopicAlias(pattern, a.newTopic, ttlMs === undefined ? undefined : { ttlMs });
        return;
      }
      case 'route': {
        const live = this.topicRoutes.get(pattern);
        if (live?.predicate !== undefined) {
          throw new Error(
            `cannot apply route for "${pattern}" from a snapshot: ` +
              'the live route carries a predicate, which snapshots cannot represent — ' +
              'restore it with setTopicRoute',
          );
        }
        const a = after as TopicRouteConfig | undefined;
        if (a === undefined) {
          this.clearTopicRoute(pattern);
          return;
        }
        this.setTopicRoute(pattern, a.dst);
        return;
      }
      default:
        throw new Error(`unknown topic rule key in config path: ${ruleKey}`);
    }
  }
}

/**
 * Extracts the quoted key from a `section["key"]` config path.
 */
function bracketKeyOf(path: string): string {
  const match = /^[a-zA-Z]+\["((?:[^"\\]|\\.)*)"\]$/.exec(path);
  if (match === null) throw new Error(`malformed config path: ${path}`);
  return JSON.parse(`"${match[1]}"`) as string;
}

/**
 * Splits a `topicRules["<pattern>"].<ruleKey>` config path.
 */
function topicRulePathOf(path: string): { pattern: string; ruleKey: string } {
  const match = /^topicRules\["((?:[^"\\]|\\.)*)"\]\.([A-Za-z]+)$/.exec(path);
  if (match === null) throw new Error(`malformed config path: ${path}`);
  return { pattern: JSON.parse(`"${match[1]}"`) as string, ruleKey: match[2] };
}
