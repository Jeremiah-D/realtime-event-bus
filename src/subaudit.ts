/**
 * Subscription lifecycle audit log (EB-78): an append-only JSONL journal of
 * every subscription change on the bus — subscribe / unsubscribe / pattern
 * hot-updates (EB-74) / consumer-group join-leave / group rebalances / topic
 * registry changes (EB-77) / backpressure drops (EB-83,
 * `backpressure_dropped`) / signature-verification failures (EB-84,
 * `signature_verification_failed`) / durable-log retention policy changes
 * (EB-85, `retention_updated`) / retained-message rule and state changes
 * (EB-87, `retained_rule_set` / `retained_rule_cleared` /
 * `retained_cleared`) / topic pause/resume (EB-88, `topic_paused` /
 * `topic_resumed`) / bulk delayed-delivery cancellation (EB-89,
 * `delayed_cancelled`) / publish-side partition-count rule changes (EB-90,
 * `partition_rule_set` / `partition_rule_cleared`) / out-of-order barrier
 * discards (EB-98, `barrier_dropped`) / manual subscriber pause-resume
 * (EB-113, `subscriber_paused` / `subscriber_resumed`) / manual
 * consumer-offset seek (EB-130, `subscriber_seek`) / handler fault
 * isolation quarantine (EB-197, `subscriber_quarantined` /
 * `subscriber_quarantine_resumed`).
 *
 * This complements `EventBusOptions.onAuthzDenied`, which only fires for
 * *rejected* authorization decisions: the audit log records the normal,
 * successful subscription lifecycle that the denial hook deliberately
 * ignores. Operators answering "who subscribed to what, and when" query
 * here instead of reconstructing it from delivery metrics.
 *
 * Two layers:
 * - an always-on bounded in-memory ring (default 1000 entries, oldest
 *   evicted) that `EventBus.getSubscriptionAudit()` queries — zero
 *   configuration, zero disk I/O;
 * - opt-in disk persistence via `dir`: one `audit-YYYY-MM-DD.jsonl` file
 *   per UTC day (daily rotation is implicit — each event lands in the file
 *   named by its own timestamp), so a restarted bus keeps its audit
 *   trail. On construction the in-memory ring is back-filled from the
 *   newest on-disk lines, skipping corrupt lines.
 *
 * All timestamps come from the bus clock (`now`), so tests stay
 * deterministic. Disk writes are synchronous and error-isolated: a failed
 * append never throws into the subscribe path — it is counted in
 * `writeErrors()` instead.
 */
import { appendFileSync, mkdirSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import {
  AUDIT_CHAIN_GENESIS,
  attachChainLink,
  effectiveRecordHash,
  isHashString,
  verifyChainLinks,
  type ChainVerification,
} from './auditchain.ts';

/** Every lifecycle action the audit log records. */
export type SubscriptionAuditAction =
  | 'subscribe'
  | 'unsubscribe'
  | 'update_pattern'
  | 'group_join'
  | 'group_leave'
  | 'group_session_expired'
  | 'group_paused'
  | 'group_resumed'
  | 'rebalance'
  | 'topic_register'
  | 'topic_deregister'
  | 'topic_created'
  | 'topic_deleted'
  | 'topic_renamed'
  | 'topic_forked'
  | 'topics_merged'
  | 'topics_split'
  | 'topic_data_imported'
  | 'backpressure_dropped'
  | 'signature_verification_failed'
  | 'retention_updated'
  | 'retained_rule_set'
  | 'retained_rule_cleared'
  | 'retained_cleared'
  | 'topic_paused'
  | 'topic_resumed'
  | 'delayed_cancelled'
  | 'delayed_rescheduled'
  | 'delayed_ttl_extended'
  | 'ack_deadline_extended'
  | 'ack_timeout_revised'
  | 'redelivery_backoff_revised'
  | 'delivery_window_revised'
  | 'max_inflight_revised'
  | 'filter_revised'
  | 'delivery_shaping_revised'
  | 'health_probe_revised'
  | 'throttle_revised'
  | 'batch_revised'
  | 'retry_ladder_revised'
  | 'rate_limit_revised'
  | 'delivery_priority_revised'
  | 'lag_monitor_revised'
  | 'slow_handler_revised'
  | 'handler_fault_isolation_revised'
  | 'delivery_latency_revised'
  | 'delivery_concurrency_revised'
  | 'delayed_topic_held'
  | 'delayed_topic_resumed'
  | 'delayed_key_held'
  | 'delayed_key_resumed'
  | 'delayed_predicate_held'
  | 'delayed_predicate_resumed'
  | 'partition_rule_set'
  | 'partition_rule_cleared'
  | 'partition_expansion'
  | 'topic_partition_scaled'
  | 'config_applied'
  | 'config_rolled_back'
  | 'segment_checksum_failed'
  | 'barrier_dropped'
  | 'subscriber_paused'
  | 'subscriber_resumed'
  | 'subscriber_seek'
  | 'subscriber_quarantined'
  | 'subscriber_quarantine_resumed'
  | 'dead_letter_replay_requested'
  | 'dead_letter_replay_approved'
  | 'dead_letter_replay_rejected'
  | 'delivery_sample_rate_updated'
  | 'header_filter_updated'
  | 'subscriber_labels_updated'
  | 'client_pressure'
  | 'client_pressure_recovered'
  | 'tenant_quota_warning'
  | 'bridge_unhealthy'
  | 'bridge_recovered'
  | 'mirror_throttled'
  | 'topic_key_rotated'
  | 'decrypt_failed'
  | 'auto_remediate_resume'
  | 'auto_remediate_linger_pruned'
  | 'auto_remediate_rebalance'
  | 'auto_remediate_run';

/**
 * One audit entry. `detail` carries action-specific context without
 * bloating the flat schema:
 * - `update_pattern`: `{ from: <old pattern> }`
 * - `rebalance`: `{ trigger: 'join'|'leave', memberId, members: <count> }`
 * - `group_session_expired`: `{ instanceId }` (static membership, EB-104:
 *   the held roster slot was released after the session window)
 * - `topic_register`: `{ owner?, description?, schemaVersion?, tags?, updated: <bool> }`
 * - `topic_deregister`: `{ owner?, description? }` (last-known metadata)
 * - `topic_created`: `{ updated: <bool>, ttlMs?, retentionMaxAgeMs?,
 *   compression: <bool>, signing: <bool> }` — a topic lifecycle
 *   provisioning (EB-126); the `topic` field carries the concrete topic
 * - `topic_deleted`: `{ purge: <bool>, invalidatedSubscriptions: <count>,
 *   hadProvisioning: <bool> }` — a topic lifecycle deletion (EB-126); the
 *   `topic` field carries the concrete topic. Per invalidated wildcard
 *   subscription a second event is recorded with `subId`/`pattern` and
 *   `detail: { reason: 'wildcard_subscription_invalidated', purge }`
 * - `backpressure_dropped`: `{ policy: <backpressure tier>, dropped: <count>, queueSize: <number> }`
 * - `signature_verification_failed`: `{ seq: <per-topic seq>, kid?: <key id that signed, when present> }`
 * - `topic_key_rotated`: `{ oldKeyId, newKeyId, rotatedAtMs }` — a
 *   per-topic DEK rotation (EB-179); the `topic` field carries the rule's
 *   topic pattern, `actor` is `'system'`
 * - `decrypt_failed`: `{ seq: <per-topic seq>, keyId?: <key id on the
 *   envelope, when present> }` — a sealed message dropped fail-closed at
 *   delivery (EB-179): unknown key id, retired past its grace window, or
 *   tampered envelope
 * - `auto_remediate_resume`: `{ reason }` — the doctor auto-remediator
 *   (EB-211) resumed a stuck health-probe-degraded subscriber; `subId`
 *   carries the subscriber
 * - `auto_remediate_linger_pruned`: `{ pruned: <count>, reason }` — the
 *   remediator dropped expired handoff-linger windows; `groupId` carries
 *   the group
 * - `auto_remediate_rebalance`: `{ reason }` — the remediator forced a
 *   rebalance (`trigger: 'remediate'`) for a skewed partitioned group;
 *   `groupId` carries the group
 * - `auto_remediate_run`: `{ dryRun, healthBefore, planned, applied,
 *   capped, maxActions }` — one `autoRemediate()` run's summary,
 *   including dry-run previews
 * - `retention_updated`: `{ from: <previous maxAgeMs, null when unset>, to: <new maxAgeMs, null when cleared> }`
 * - `retained_rule_set`: `{ enabled: <boolean> }` (the `pattern` field carries the rule key)
 * - `retained_rule_cleared`: no detail (the `pattern` field carries the rule key)
 * - `retained_cleared`: `{ via: 'publish' }` (the `topic` field carries the concrete topic)
 * - `partition_rule_set`: `{ partitions: <count> }` (the `pattern` field carries the rule key)
 * - `partition_rule_cleared`: no detail (the `pattern` field carries the rule key)
 * - `partition_expansion`: `{ partitionsBefore, partitionsAfter, members }`
 *   (drained-group expansion; the live path audits `rebalance` instead)
 * - `topic_partition_scaled`: `{ partitionsBefore, partitionsAfter, groups: <groupId[]> }`
 *   (the `pattern` and `topic` fields carry the scaled topic)
 * - `topic_paused`: `{ mode: 'shed'|'queue' }` (the `pattern` field carries the rule key)
 * - `topic_resumed`: `{ mode: 'shed'|'queue', drainedMessages: <count> }` (the `pattern` field carries the rule key)
 * - `delayed_cancelled`: `{ cancelled: <count>, delayIds: <string[]>, topicPattern: <string> }`
 * - `delayed_topic_held`: `{ topicPattern: <string> }` — the delayed-delivery sweep was held for the pattern (EB-250; the `pattern` field carries the rule key)
 * - `ack_deadline_extended`: `{ seq, extensionMs, deadlineMs }` — one outstanding reliable delivery's ack timeout was reset (EB-255; `subId` carries the subscriber, `deadlineMs` is the new bus-clock deadline)
 * - `ack_timeout_revised`: `{ ackTimeoutMs, oldAckTimeoutMs }` — a reliable subscriber's ack-timeout default for future deliveries was revised (EB-259; already-armed timers are untouched)
 * - `redelivery_backoff_revised`: `{ redeliveryBackoff: <normalized config or null>, oldRedeliveryBackoff: <normalized config or null> }` — a reliable subscriber's redelivery-backoff policy for future redeliveries was revised (EB-261; `null` on either side means "no backoff, requeue immediately"; already-armed backoff delays keep the delay they were armed with)
 * - `delivery_window_revised`: `{ deliveryWindows: <resolved windows or null>, oldDeliveryWindows: <resolved windows> }` — a windowed subscriber's delivery windows were revised (EB-263; resolved windows carry `startMin`/`endMin`/`days`/`timeZone` plus `maxHoldMs` when set; `null` means the windows were cleared back to all-day delivery; held messages move to the new gate, or are released immediately on a clear)
 * - `max_inflight_revised`: `{ maxInflight: <number or null>, oldMaxInflight: <number> }` — a capped subscriber's max in-flight cap was revised (EB-264; `null` means the cap was cleared back to unlimited; the live in-flight count and the subscribe-time delivery-concurrency cap are untouched)
 * - `filter_revised`: `{ filter: 'set' | null, oldFilter: 'set' }` — a filtered subscriber's content predicate was revised (EB-265; `null` means the predicate was cleared back to unfiltered delivery; the predicate itself is a function and is never serialized into the audit record; already-queued messages keep their original admission, and durable replay reads the live predicate per record)
 * - `delivery_shaping_revised`: `{ deliveryShaping: { messagesPerSec, burst } | null, oldDeliveryShaping: { messagesPerSec, burst } }` — a shaped subscriber's delivery-shaping rate was revised (EB-266; `null` means the shaping was cleared back to unshaped delivery; a partial revision keeps the fields it leaves out; the bucket keeps its stored budget, capped at the new burst, and the new rate applies from the next refill; queued messages and the in-flight count are untouched)
 * - `health_probe_revised`: `{ healthProbe: { maxConsecutiveFailures, processingTimeoutMs?, autoResumeAfterMs? } | null, oldHealthProbe: { ... } }` — a probed subscriber's health-probe parameters were revised (EB-267; `null` means the probe was cleared back to unprobed delivery and a degraded pause it held is released; a partial revision keeps the fields it leaves out; the consecutive-failure counter and the degraded flag are untouched; the new timeout budget applies to the next handler call, a tightened threshold trips on the next failure, and a new cooldown applies from the next degradation)
 * - `throttle_revised`: `{ throttle: { minRatePerSec, maxRatePerSec, initialRatePerSec } | null, oldThrottle: { minRatePerSec, maxRatePerSec, initialRatePerSec } }` — a throttled subscriber's adaptive-throttle rates were revised (EB-268; `null` means the throttling was cleared back to unthrottled publish; a partial revision keeps the rates it leaves out; a current excursion keeps its bucket and enforced rate until it ends, and the new rates apply from the next engagement; the drop count, the observed drain rate, and the queue are untouched)
 * - `batch_revised`: `{ batch: { maxSize, maxWaitMs } | null, oldBatch: { maxSize, maxWaitMs } }` — a batched subscriber's batch-delivery parameters were revised (EB-269; `null` means batching was cleared back to one-at-a-time delivery, after immediately delivering the pending partial batch; a partial revision keeps the fields it leaves out; the new maxSize applies from the next collection round, with an already-over-limit pending batch delivered whole first, and a new maxWaitMs applies from the next linger arming; the pending batch and the queue are untouched by a revision)
 * - `retry_ladder_revised`: `{ retryLadder: number[] | null, oldRetryLadder: number[] }` — a reliable subscriber's pre-DLQ retry ladder was revised (EB-270; `null` means the ladder was cleared back to immediate dead-lettering on budget exhaustion, by an explicit `null` or an empty array; only the ladder instance is replaced, so an already-armed rung still fires once on its old delay and the next rung arms from the new ladder, and a degraded pause re-arms the same rung from the live ladder; pending rungs, messages, sequences, TTL deadlines, and redelivery counts are untouched)
 * - `rate_limit_revised`: `{ rateLimit: { maxMessages, perWindowMs } | null, oldRateLimit: { maxMessages, perWindowMs } }` — a rate-limited subscriber's sliding-window ceiling was revised (EB-271; `null` means the limit was cleared back to unlimited, after cancelling the armed re-flush timer; only the limiter instance is replaced and the state object retained, so an already-armed re-flush timer stays valid and the window starts fresh — tightening never retroactively blocks what was already delivered, loosening admits held messages on the next flush; the revision must be complete, and passing the current values still resets the window)
 * - `delivery_priority_revised`: `{ deliveryPriority: { weight, label }, oldDeliveryPriority: { weight, label } }` — a subscriber's drain priority was revised (EB-272; `null` restores the default `'normal'` / weight 2, recorded as the default; the drain reads the live weight from the next flush round, the DRR deficit carryover is untouched, and a revision crossing the default boundary moves the non-default count with the weight so the fast path never silently ignores a revised weight; the same weight and label is a no-op with no audit)
 * - `lag_monitor_revised`: `{ lagMonitor: { windowSize, thresholdMs?, onLag: 'set'? } | null, oldLagMonitor: { windowSize, thresholdMs?, onLag: 'set'? } }` — a lag-monitored subscriber's watermark parameters were revised (EB-273; `null` means monitoring was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the callback itself is never serialized, only its presence; the dwell samples, the enqueue stamps, and the alerted latch are untouched — shrinking the window evicts only the oldest samples, and the new threshold applies from the next watermark evaluation; a revision identical to the current parameters is a no-op with no audit)
 * - `slow_handler_revised`: `{ slowHandler: { thresholdMs, onSlowHandler: 'set'? } | null, oldSlowHandler: { thresholdMs, onSlowHandler: 'set'? } }` — a slow-handler subscriber's offload threshold and callback were revised (EB-274; `null` means the offload was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the callback itself is never serialized, only its presence; the state object is mutated in place, so an armed macrotask continuation keeps its `continueArmed` flag and the new threshold applies from the next handler execution; a revision identical to the current parameters is a no-op with no audit)
 * - `handler_fault_isolation_revised`: `{ handlerFaultIsolation: { maxConsecutiveErrors, onQuarantined: 'set'? } | null, oldHandlerFaultIsolation: { maxConsecutiveErrors, onQuarantined: 'set'? } }` — an isolated subscriber's error budget and quarantine callback were revised (EB-275; `null` means the isolation was cleared, refused while the subscriber is quarantined — it must be resumed via `resumeQuarantined` first — and a revision after a clear is refused until the subscriber opts in again at subscribe time; a partial revision keeps the fields it leaves out; the callback itself is never serialized, only its presence; the state object is mutated in place, so the consecutive-error counter and the quarantined latch are untouched and the new budget applies from the next handler-failure evaluation; a revision identical to the current parameters is a no-op with no audit)
 * - `delivery_latency_revised`: `{ deliveryLatency: { windowSize } | null, oldDeliveryLatency: { windowSize } }` — a sampled subscriber's delivery-latency window size was revised (EB-276; `null` means sampling was cleared, and a revision after a clear is refused until the subscriber opts in again at subscribe time; shrinking the window evicts only the oldest samples, widening evicts nothing, and the enqueue clocks of in-flight messages are untouched — the new window applies from the next sample and summary; a revision identical to the current window is a no-op with no audit)
 * - `delivery_concurrency_revised`: `{ deliveryConcurrency: <number>, oldDeliveryConcurrency: <number> }` — a subscriber's handler-concurrency cap was revised (EB-277; always armed with default 1, so there is no opt-in gate — a subscription without an explicitly set cap can be revised; `null` restores the parse-chain default, the current `maxInflight` cap or 1; only `state.max` moves, `inFlight` and `keysInFlight` are untouched, so lowering never revokes in-flight calls and raising is picked up immediately by the live-max read in `tryAcquireDeliverySlot` plus a scheduled flush; same-key ordering is unchanged; a revision identical to the current cap is a no-op with no audit)
 * - `delayed_topic_resumed`: `{ topicPattern: <string> }` — the delayed-delivery sweep hold was lifted (EB-250; the `pattern` field carries the rule key)
 * - `delayed_key_held`: `{ topicPattern: <string>, key: <string> }` — the delayed-delivery sweep was held for one key in the pattern (EB-252; the `pattern` field carries the topic pattern)
 * - `delayed_key_resumed`: `{ topicPattern: <string>, key: <string> }` — the delayed-delivery sweep hold for one key in the pattern was lifted (EB-252; the `pattern` field carries the topic pattern)
 * - `delayed_predicate_held`: `{ topicPattern: <string>, delayId: <string> }` — one pending delayed schedule was held by predicate (EB-262; the `pattern` field carries the topic pattern, `topic` the schedule's concrete topic, one event per held schedule)
 * - `delayed_predicate_resumed`: `{ topicPattern: <string>, delayId: <string> }` — one predicate-held delayed schedule was released (EB-262; same field convention as the hold event)
 * - `config_applied`: `{ applied: <string[]> }` — paths applied by
 *   `applyConfig` (EB-91), in application order; empty on a no-op
 * - `config_rolled_back`: `{ failedPath: <string>, applied: <string[]>,
 *   rolledBack: <boolean> }` — a failed `applyConfig` (EB-91) restored
 *   the pre-apply state after `failedPath` threw
 * - `segment_checksum_failed`: `{ path: <archive file>, segmentId }` —
 *   an archived segment failed checksum verification (EB-94) and was
 *   isolated: skipped, never fatal (the `topic` field carries the topic)
 * - `barrier_dropped`: `{ epoch: <discarded epoch>, lastEpoch: <highest
 *   epoch seen> }` — an out-of-order barrier was discarded for epoch
 *   monotonicity (EB-98): never delivered, never arming (the `topic`
 *   field carries the barrier's topic, `subId` the subscriber)
 * - `subscriber_paused`: `{ reason?: <operator reason> }` — delivery to
 *   one subscriber was manually paused (EB-113); `subId`/`pattern`
 *   identify the subscription
 * - `subscriber_resumed`: no detail — a manual subscriber pause (EB-113)
 *   was cleared; `subId`/`pattern` identify the subscription
 * - `subscriber_seek`: `{ mode: 'earliest'|'latest'|'seq'|'timestamp',
 *   topics: [{ topic, from, requested, position, replayed, droppedQueued,
 *   clampedByCommitted? }], invalidatedInflight }` — a manual
 *   consumer-offset seek (EB-130) repositioned the subscriber's
 *   consumption position; `subId`/`pattern` identify the subscription
 * - `subscriber_quarantined`: `{ consecutiveErrors: <count> }` — handler
 *   fault isolation (EB-197) auto-paused delivery after the consecutive
 *   error budget tripped; `subId`/`pattern` identify the subscription
 * - `subscriber_quarantine_resumed`: no detail — a
 *   `resumeQuarantined` (EB-197) cleared the quarantine;
 *   `subId`/`pattern` identify the subscription
 * - `delivery_sample_rate_updated`: `{ from: <old rate>, to: <new rate> }`
 *   — a `setDeliverySampleRate` (EB-152) hot-update changed the
 *   subscriber's delivery-sampling divisor; `subId`/`pattern` identify
 *   the subscription
 * - `header_filter_updated`: `{ rules: <rule count after the update> }`
 *   — a `setHeaderFilter` (EB-153) hot-update replaced the subscriber's
 *   header-filter rules (`0` when the filter was cleared);
 *   `subId`/`pattern` identify the subscription
 * - `subscriber_labels_updated`: `{ previous: <labels before>, labels: <labels after> }`
 *   — a `setSubscriberLabels` (EB-213) hot-update replaced the
 *   subscriber's labels (`{}` when cleared); `subId`/`pattern` identify
 *   the subscription
 * - `client_pressure`: `{ remoteAddress?, queuedBytes, watermarkBytes,
 *   pausedSubscriptions }` — a gateway (EB-99) connection's outbound
 *   queue crossed its client-pressure watermark (EB-161): the
 *   connection's bus subscriptions were paused so a slow client can no
 *   longer grow the gateway process's memory; `pausedSubscriptions` is
 *   the number of subscriptions this event paused
 * - `client_pressure_recovered`: `{ remoteAddress?, queuedBytes,
 *   recoveryWatermarkBytes, resumedSubscriptions }` — the same
 *   connection's outbound queue drained back under the recovery
 *   watermark and the pressure-paused subscriptions were resumed
 * - `tenant_quota_warning`: `{ namespace, pattern, usedMessages,
 *   usedBytes, softMessages?, softBytes?, windowMs }` — a namespace
 *   tenant quota (EB-162) reached its soft threshold; latched per
 *   excursion (the `topic` field carries the concrete topic that
 *   crossed the threshold)
 * - `bridge_unhealthy`: `{ transport, consecutiveFailures,
 *   lastProbeAt? }` — the bridge transport health monitor (EB-168)
 *   reached its consecutive-failure threshold and paused outbound
 *   mirroring; local delivery is unaffected
 * - `bridge_recovered`: `{ transport, consecutiveFailures,
 *   lastProbeAt? }` — a later probe or mirror succeeded and outbound
 *   mirroring resumed
 * - `mirror_throttled`: `{ srcPattern, dstTemplate, messagesPerSec }` —
 *   a mirror frame shed by its rule's traffic throttle (EB-172) before
 *   reaching admission; the `pattern` field carries the source pattern
 */
export interface SubscriptionAuditEvent {
  v: 1;
  /** Milliseconds on the bus clock. */
  at: number;
  action: SubscriptionAuditAction;
  subId?: string;
  groupId?: string;
  pattern?: string;
  topic?: string;
  /** Who made the change; defaults to `'system'` when the caller supplies none. */
  actor: string;
  detail?: Record<string, unknown>;
  /**
   * Tamper-evident hash chain (EB-198): `prevHash` is the previous
   * event's `hash` (the fixed genesis for the first event), `hash`
   * covers the whole event. Both are always present on events written
   * by `append()`; events recovered from pre-chain journal files carry
   * neither and verify as chain anchors.
   */
  prevHash?: string;
  hash?: string;
}

/** Query filters for `SubscriptionAuditLog.query`. */
export interface SubscriptionAuditQuery {
  /** Inclusive lower bound on `at`. */
  since?: number;
  /** Exclusive upper bound on `at`. */
  until?: number;
  /** One action or a set of actions. */
  action?: SubscriptionAuditAction | SubscriptionAuditAction[];
  /** Return at most this many of the most recent matching entries. */
  limit?: number;
}

/** Options for `SubscriptionAuditLog`. */
export interface SubscriptionAuditOptions {
  /** Opt-in disk directory for the daily JSONL files. */
  dir?: string;
  /** Bounded in-memory index size; oldest entries evicted first. */
  maxEntries?: number;
  /** Clock source; defaults to `Date.now`. */
  now?: () => number;
}

export const SUBSCRIPTION_AUDIT_DEFAULT_MAX_ENTRIES = 1000;

const ALL_ACTIONS: ReadonlySet<string> = new Set<string>([
  'subscribe',
  'unsubscribe',
  'update_pattern',
  'group_join',
  'group_leave',
  'group_paused',
  'group_resumed',
  'rebalance',
  'topic_register',
  'topic_deregister',
  'topic_created',
  'topic_deleted',
  'topic_renamed',
  'topic_forked',
  'topics_merged',
  'topics_split',
  'topic_data_imported',
  'backpressure_dropped',
  'signature_verification_failed',
  'retention_updated',
  'retained_rule_set',
  'retained_rule_cleared',
  'retained_cleared',
  'topic_paused',
  'topic_resumed',
  'delayed_cancelled',
  'delayed_rescheduled',
  'delayed_ttl_extended',
  'ack_deadline_extended',
  'ack_timeout_revised',
  'redelivery_backoff_revised',
  'delivery_window_revised',
  'max_inflight_revised',
  'filter_revised',
  'delivery_shaping_revised',
  'health_probe_revised',
  'throttle_revised',
  'batch_revised',
  'retry_ladder_revised',
  'rate_limit_revised',
  'delivery_priority_revised',
  'lag_monitor_revised',
  'slow_handler_revised',
  'handler_fault_isolation_revised',
  'delivery_latency_revised',
  'delivery_concurrency_revised',
  'delayed_topic_held',
  'delayed_topic_resumed',
  'delayed_key_held',
  'delayed_key_resumed',
  'delayed_predicate_held',
  'delayed_predicate_resumed',
  'partition_rule_set',
  'partition_rule_cleared',
  'partition_expansion',
  'topic_partition_scaled',
  'barrier_dropped',
  'subscriber_paused',
  'subscriber_resumed',
  'subscriber_seek',
  'subscriber_quarantined',
  'subscriber_quarantine_resumed',
  'delivery_sample_rate_updated',
  'header_filter_updated',
  'subscriber_labels_updated',
  'client_pressure',
  'client_pressure_recovered',
  'tenant_quota_warning',
  'bridge_unhealthy',
  'bridge_recovered',
  'mirror_throttled',
  'topic_key_rotated',
  'decrypt_failed',
  'auto_remediate_resume',
  'auto_remediate_linger_pruned',
  'auto_remediate_rebalance',
  'auto_remediate_run',
  'dead_letter_replay_requested',
  'dead_letter_replay_approved',
  'dead_letter_replay_rejected',
]);

function isAuditAction(value: unknown): value is SubscriptionAuditAction {
  return typeof value === 'string' && ALL_ACTIONS.has(value);
}

function utcDay(at: number): string {
  const d = new Date(at);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/** Validates an audit query; throws `RangeError` on illegal values. */
export function assertValidSubscriptionAuditQuery(query: SubscriptionAuditQuery): void {
  const { since, until, action, limit } = query;
  if (since !== undefined && (!Number.isFinite(since) || since < 0)) {
    throw new RangeError('getSubscriptionAudit: since must be a finite non-negative timestamp');
  }
  if (until !== undefined && (!Number.isFinite(until) || until < 0)) {
    throw new RangeError('getSubscriptionAudit: until must be a finite non-negative timestamp');
  }
  if (action !== undefined) {
    const actions = Array.isArray(action) ? action : [action];
    for (const a of actions) {
      if (!isAuditAction(a)) {
        throw new RangeError(`getSubscriptionAudit: unknown action ${JSON.stringify(a)}`);
      }
    }
  }
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) {
    throw new RangeError('getSubscriptionAudit: limit must be a positive integer');
  }
}

/** Validates an actor name; throws `RangeError` on illegal values. */
export function assertValidActor(actor: unknown, where: string): asserts actor is string {
  if (typeof actor !== 'string' || actor.length === 0) {
    throw new RangeError(`${where}: actor must be a non-empty string`);
  }
}

export class SubscriptionAuditLog {
  private readonly dir: string | undefined;
  private readonly maxEntries: number;
  private readonly now: () => number;
  private readonly entries: SubscriptionAuditEvent[] = [];
  private diskWriteErrors = 0;
  /**
   * Tamper-evident chain head (EB-198): the effective hash of the most
   * recently appended or recovered event — the `prevHash` of the next
   * event. Starts at the genesis; recovery advances it past the on-disk
   * history so the chain continues across restarts.
   */
  private headHash: string = AUDIT_CHAIN_GENESIS;

  constructor(options: SubscriptionAuditOptions = {}) {
    const { dir, maxEntries = SUBSCRIPTION_AUDIT_DEFAULT_MAX_ENTRIES, now = Date.now } = options;
    if (dir !== undefined && (typeof dir !== 'string' || dir.length === 0)) {
      throw new RangeError('subscriptionAuditDir must be a non-empty string');
    }
    if (!Number.isInteger(maxEntries) || maxEntries < 1) {
      throw new RangeError('subscriptionAuditMaxEntries must be a positive integer');
    }
    if (typeof now !== 'function') {
      throw new RangeError('subscriptionAudit now must be a function');
    }
    this.dir = dir;
    this.maxEntries = maxEntries;
    this.now = now;
    if (dir !== undefined) {
      this.recover(dir);
    }
  }

  /**
   * Appends one event. The timestamp is stamped here on the bus clock, so
   * callers pass everything except `v`/`at`. The event is tamper-evident
   * chained (EB-198): `prevHash`/`hash` are attached before the
   * in-memory append and the disk write, so both layers carry the same
   * links. Never throws: disk failures are counted in `writeErrors()`
   * and the in-memory entry always lands.
   */
  append(event: Omit<SubscriptionAuditEvent, 'v' | 'at'> & { at?: number }): void {
    const full: SubscriptionAuditEvent = {
      v: 1,
      at: event.at ?? this.now(),
      action: event.action,
      actor: event.actor,
      ...(event.subId === undefined ? {} : { subId: event.subId }),
      ...(event.groupId === undefined ? {} : { groupId: event.groupId }),
      ...(event.pattern === undefined ? {} : { pattern: event.pattern }),
      ...(event.topic === undefined ? {} : { topic: event.topic }),
      ...(event.detail === undefined ? {} : { detail: event.detail }),
    };
    const linked = attachChainLink(this.headHash, full);
    this.headHash = linked.hash;
    this.entries.push(linked);
    if (this.entries.length > this.maxEntries) {
      this.entries.splice(0, this.entries.length - this.maxEntries);
    }
    if (this.dir !== undefined) {
      this.appendToDisk(linked);
    }
  }

  /**
   * Queries the in-memory index: filters by time window and action, then
   * returns at most `limit` of the most recent matches (oldest first).
   * Throws `RangeError` on illegal query values.
   */
  query(query: SubscriptionAuditQuery = {}): SubscriptionAuditEvent[] {
    assertValidSubscriptionAuditQuery(query);
    const { since, until, action, limit } = query;
    const actions =
      action === undefined ? undefined : new Set(Array.isArray(action) ? action : [action]);
    const matched: SubscriptionAuditEvent[] = [];
    for (const e of this.entries) {
      if (since !== undefined && e.at < since) continue;
      if (until !== undefined && e.at >= until) continue;
      if (actions !== undefined && !actions.has(e.action)) continue;
      matched.push(e);
    }
    if (limit !== undefined && matched.length > limit) {
      return matched.slice(matched.length - limit);
    }
    return matched;
  }

  /** In-memory index size. */
  size(): number {
    return this.entries.length;
  }

  /** Disk append failures since construction (0 when no `dir` configured). */
  writeErrors(): number {
    return this.diskWriteErrors;
  }

  private fileFor(at: number): string {
    return join(this.dir as string, `audit-${utcDay(at)}.jsonl`);
  }

  private appendToDisk(event: SubscriptionAuditEvent): void {
    try {
      appendFileSync(this.fileFor(event.at), JSON.stringify(event) + '\n', 'utf8');
    } catch {
      this.diskWriteErrors += 1;
    }
  }

  /**
   * Back-fills the in-memory ring from existing daily files (oldest file
   * first, newest lines last), skipping corrupt lines. Failures here are
   * silent by design: a damaged audit directory must never prevent the
   * bus from starting.
   */
  private recover(dir: string): void {
    let files: string[];
    try {
      mkdirSync(dir, { recursive: true });
      files = readdirSync(dir)
        .filter((f) => f.startsWith('audit-') && f.endsWith('.jsonl'))
        .sort();
    } catch {
      return;
    }
    const recovered: SubscriptionAuditEvent[] = [];
    for (const file of files) {
      let text: string;
      try {
        text = readFileSync(join(dir, file), 'utf8');
      } catch {
        continue;
      }
      for (const line of text.split('\n')) {
        const trimmed = line.trim();
        if (trimmed === '') continue;
        const event = parseAuditLine(trimmed);
        if (event !== undefined) recovered.push(event);
      }
    }
    if (recovered.length > this.maxEntries) {
      recovered.splice(0, recovered.length - this.maxEntries);
    }
    this.entries.push(...recovered);
    if (recovered.length > 0) {
      // The chain continues across restarts: the next appended event
      // links to the last recovered one (see `headHash`).
      this.headHash = effectiveRecordHash(recovered[recovered.length - 1]);
    }
  }

  /**
   * Verifies the tamper-evident hash chain (EB-198) over the in-memory
   * index: detects rewritten fields (`content-tampered`), deleted or
   * spliced events (`link-broken`), and reports the first break. The
   * oldest retained event is the segment anchor — its link to evicted
   * predecessors cannot be checked (see `src/auditchain.ts`).
   */
  verifyAuditChain(): ChainVerification {
    return verifyChainLinks(this.entries);
  }
}

/**
 * Parses one JSONL audit line; returns `undefined` for corrupt lines
 * (bad JSON, wrong shape) so recovery skips them instead of dying.
 */
export function parseAuditLine(line: string): SubscriptionAuditEvent | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null) return undefined;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return undefined;
  if (!isAuditAction(o['action'])) return undefined;
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return undefined;
  if (typeof o['actor'] !== 'string' || o['actor'].length === 0) return undefined;
  const event: SubscriptionAuditEvent = {
    v: 1,
    at: o['at'],
    action: o['action'],
    actor: o['actor'],
  };
  for (const key of ['subId', 'groupId', 'pattern', 'topic'] as const) {
    const value = o[key];
    if (value !== undefined) {
      if (typeof value !== 'string') return undefined;
      event[key] = value;
    }
  }
  if (o['detail'] !== undefined) {
    if (typeof o['detail'] !== 'object' || o['detail'] === null) return undefined;
    event.detail = o['detail'] as Record<string, unknown>;
  }
  // Tamper-evident chain fields (EB-198): optional for backward
  // compatibility with pre-chain journal files; when present they must
  // be well-formed digests, otherwise the line is corrupt.
  for (const key of ['prevHash', 'hash'] as const) {
    const value = o[key];
    if (value !== undefined) {
      if (!isHashString(value)) return undefined;
      event[key] = value;
    }
  }
  return event;
}

