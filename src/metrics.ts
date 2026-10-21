/**
 * Prometheus text exposition (0.0.4), hand-written, zero dependencies —
 * the metrics surface for an `EventBus`:
 *
 * Counters (bus-level, monotonic):
 * - `eventbus_published_messages_total`: messages accepted via
 *   `publish`/`publishBatch`.
 * - `eventbus_delivered_messages_total`: messages handed to subscriber
 *   handlers (each handler invocation; at-least-once redeliveries count
 *   again).
 * - `eventbus_dropped_messages_total`: messages shed by subscriber
 *   backpressure queues.
 * - `eventbus_shadowed_messages_total`: messages handed to shadow-subscriber
 *   handlers — the mirror deliveries (see `SubscribeOptions.shadow`); never
 *   counted in `eventbus_delivered_messages_total`.
 * - `eventbus_shadow_dropped_messages_total`: messages shed by shadow
 *   subscribers' independent backpressure queues; never counted in
 *   `eventbus_dropped_messages_total`.
 * - `eventbus_expired_messages_total`: messages discarded by TTL before
 *   delivery.
 * - `eventbus_throttled_messages_total`: messages shed at the publish
 *   side by adaptive throttling.
 * - `eventbus_rejected_messages_total`: publishes rejected by admission
 *   gates before admission (schema validation, broker ACL, per-topic
 *   message-size caps, expired migration aliases).
 * - `eventbus_deprecated_schema_messages_total`: publishes admitted
 *   under a superseded-but-graced schema version (EB-86) — successful
 *   admissions, the migrate-your-producers signal.
 * - `eventbus_late_messages_total`: messages admitted with a business
 *   event time older than their topic's event-time watermark (EB-59).
 * - `eventbus_rate_limited_messages_total`: messages shed by publish-side
 *   per-topic rate limiting.
 * - `eventbus_global_rate_limited_messages_total`: messages shed by the
 *   bus-level global publish rate limit (EB-180) — the backstop under
 *   the per-topic and per-publisher limits.
 * - `eventbus_sampled_messages_total`: non-critical publishes shed by
 *   publish-side adaptive sampling (EB-38) — no sequence number
 *   consumed, no rate-limit token burned.
 * - `eventbus_paused_messages_total`: publishes shed by topic pauses in
 *   shed mode (EB-88).
 * - `eventbus_circuit_broken_messages_total`: publishes refused by
 *   publish circuit breakers (EB-116) — the breaker was open, so
 *   `publish` threw `CircuitBreakerOpenError` (fast-fail).
 * - `eventbus_circuit_breaker_trips_total`: times a publish circuit
 *   breaker opened (EB-116).
 * - `eventbus_duplicate_messages_total`: idempotent publishes suppressed
 *   as duplicates.
 * - `eventbus_filtered_messages_total`: messages skipped by subscriber
 *   content filters.
 * - `eventbus_header_filtered_messages_total`: messages skipped by
 *   subscriber header filters (EB-153); the per-topic
 *   `eventbus_header_filtered_messages_total{topic="..."}` series is
 *   activity-gated (renders only when some topic has a skip).
 * - `eventbus_dead_lettered_messages_total`: reliable messages moved into
 *   subscriber dead-letter queues.
 * - `eventbus_sequence_gaps_total`: sequence numbers observed missing by
 *   subscribers (see `TopicStats.sequenceGaps` for the counting rules).
 * - `eventbus_expiring_soon_alerts_total`: TTL expiring-soon warnings
 *   fired for subscriber queue heads (EB-110).
 * - `eventbus_keyed_reordered_messages_total`: keyed messages held in
 *   per-(subscriber, key) reorder buffers because an earlier keySeq had not
 *   been fanned out yet — the observable count of per-key publish-order
 *   enforcement (see `PublishOptions.key`).
 * - `eventbus_causal_buffer_dropped_messages_total`: causal messages
 *   dropped from full per-source reorder buffers (drop-oldest
 *   anti-deadlock; see `SubscribeOptions.causal`).
 * - `eventbus_causal_buffer_regressed_messages_total`: causal messages
 *   delivered immediately on clock regression (see
 *   `SubscribeOptions.causal`).
 * - `eventbus_autocommitted_offsets_total`: consumer-group offset
 *   checkpoints written by offset auto-commit
 *   (`GroupSubscribeOptions.autoCommit`, EB-127) — every one went through
 *   `commitOffset`, so the offset journal and the `OffsetStore`
 *   write-through saw exactly the same writes as manual commits.
 * - `eventbus_topic_published_messages_total{topic}`: publishes per topic.
 * - `eventbus_idempotent_hits_total{topic}`: publishes per topic
 *   suppressed by explicit idempotency keys (`PublishOptions.idempotencyKey`,
 *   EB-151) — the retry arrived with a `(topic, idempotencyKey)` pair
 *   already claimed within the idempotency window, so it returned the
 *   first publish's result instead of publishing again. Distinct from
 *   `eventbus_duplicate_messages_total` (EB-27/EB-146). Activity-gated:
 *   the series (and its HELP/TYPE block) render only when at least one
 *   topic has a hit, so a bus without idempotency-key traffic renders
 *   byte-identical exposition to before.
 * - `eventbus_fair_scheduled_messages_total{topic}`: messages per topic
 *   dequeued by the topic-fair scheduler (EB-135; "scheduled", not
 *   "delivered" — see `BusStats.topicFairness`); only emitted for topics
 *   with at least one scheduled message, so buses without fair
 *   scheduling render no series at all.
 * - `eventbus_subscriber_priority_dispatch_total{class}`: drain visits
 *   scheduled per subscriber priority class (EB-166; "scheduled", not
 *   "delivered" — a paused subscriber's visit still counts); only
 *   emitted when a non-default priority class has at least one dispatch,
 *   so buses without drain priorities render no series at all.
 * - `eventbus_delivery_sampled_messages_total{topic}`: messages per
 *   topic admitted by subscriber-side delivery-sampling gates
 *   (`SubscribeOptions.deliverySampleRate`, EB-152) — the `seq % N === 0`
 *   decision let them through toward the queue. Activity-gated: the
 *   series (and its HELP/TYPE block) render only when at least one topic
 *   has an admission, so a bus without delivery sampling renders
 *   byte-identical exposition to before. Distinct from
 *   `eventbus_sampled_messages_total` (EB-38, publish-side adaptive
 *   sampling shed before fan-out).
 * - `eventbus_delivery_sample_dropped_total{topic}`: messages per topic
 *   skipped by subscriber-side delivery-sampling gates (EB-152) —
 *   deliberately skipped, never queued, never a gap, no group offset
 *   recorded. Activity-gated like the sampled series above.
 * - `eventbus_topic_partition_published_total{topic,partition}`: admitted
 *   publishes per topic partition (EB-90; only emitted for topics with
 *   more than one configured partition — single-partition topics render
 *   byte-identical exposition to before).
 * - `eventbus_topic_paused_messages_total{topic}`: publishes per topic
 *   shed by topic pauses in shed mode (EB-88).
 * - `eventbus_topic_circuit_broken_messages_total{topic}`: publishes per
 *   topic refused by its publish circuit breaker (EB-116).
 * - `eventbus_topic_late_messages_total{topic}`: messages per topic whose
 *   business event time was older than the topic's event-time watermark
 *   (EB-59; still delivered — a data-quality counter, not a loss
 *   counter).
 * - `eventbus_topic_event_time_watermark{topic}`: the topic's event-time
 *   watermark in epoch milliseconds (EB-59); emitted only for topics that
 *   have seen at least one event-time publish.
 * - `eventbus_payload_bytes_bucket{topic,le}`: cumulative payload-size
 *   histogram of admitted publishes per topic, in bytes (EB-143) —
 *   `le` from the topic's configured buckets plus `+Inf`; only emitted
 *   for topics with a matching histogram rule.
 * - `eventbus_payload_bytes_count{topic}`: total sampled publishes per
 *   topic (EB-143).
 * - `eventbus_payload_bytes_sum{topic}`: total sampled payload bytes per
 *   topic (EB-143).
 * - `eventbus_namespace_published_messages_total{namespace}`: publishes
 *   per namespace (EB-52; only emitted when namespaces are registered).
 * - `eventbus_namespace_subscribers{namespace}`: live namespaced
 *   subscriptions per namespace (EB-52; only emitted when namespaces
 *   are registered).
 * - `eventbus_publisher_published_messages_total{publisher}`: admitted
 *   publishes per publisher identity (EB-65; only emitted when at least
 *   one publisher has admitted a publish — anonymous publishes move no
 *   per-publisher counter).
 * - `eventbus_cep_matches_total`: sequence matches completed and emitted
 * - `eventbus_cep_absence_matches_total`: absence steps satisfied (EB-174)
 *   (EB-170; only emitted once a pattern was defined).
 * - `eventbus_statestore_evicted_keys_total`: keys evicted by the
 *   materialized state store's `maxKeys` bound (EB-171; only emitted when
 *   the store is enabled).
 *
 * Gauges (point-in-time):
 * - `eventbus_subscribers`: currently active subscriptions.
 * - `eventbus_retained_topics`: topics currently holding an MQTT-style
 *   retained message (EB-87).
 * - `eventbus_unacked_deliveries`: reliable deliveries handed out but not
 *   yet acked or nacked.
 * - `eventbus_throttled_subscribers`: subscriptions under adaptive
 *   publish-side throttling.
 * - `eventbus_degraded_subscribers`: subscriptions auto-paused by health
 *   probing.
 * - `eventbus_paused_subscribers`: subscriptions manually paused by the
 *   operator (see `EventBus.pauseSubscriber`, EB-113).
 * - `eventbus_shaped_subscribers`: subscriptions held back by
 *   delivery-side rate shaping.
 * - `eventbus_credit_exhausted_subscribers`: subscriptions out of
 *   delivery credit (EB-117) — delivery held until more credit is
 *   granted.
 * - `eventbus_pending_delayed`: delayed messages scheduled but not yet
 *   due.
 * - `eventbus_paused_topics`: currently active topic-pause rules (EB-88).
 * - `eventbus_circuit_breakers_open`: publish circuit breakers currently
 *   open (fast-failing; EB-116).
 * - `eventbus_causal_buffer_depth`: messages currently held in
 *   per-(subscriber, source) causal reorder buffers, waiting on their
 *   dependencies.
 * - `eventbus_topic_subscribers{topic}`: subscribers matched by the most
 *   recent publish to the topic — the current fan-out width, so hot
 *   topics are visible at a glance. A matching consumer group counts once
 *   (its copy goes to a single assigned member).
 * - `eventbus_topic_rate_msg_per_sec{topic,window}`: per-topic publish
 *   rate (messages/sec) over the trailing 1s / 1m / 5m windows
 *   (`window` is "1s", "1m" or "5m"), load-average style — the real-time
 *   signal for rate-limit/scaling decisions. Only the hot-topics set
 *   (top 10 by 1m rate, see `BusStats.hotTopics`); 30 series max.
 * - `eventbus_delivery_latency_ms{quantile,subscriber,pattern}`: per-subscriber
 *   enqueue→delivery queue-dwell distribution for subscriptions with
 *   `deliveryLatency` enabled — nearest-rank p50/p95/p99 over each
 *   subscriber's bounded rolling window (`quantile` is "0.5", "0.95" or
 *   "0.99"). The bus-level slow-consumer view; only tracked subscribers
 *   appear.
 * - `eventbus_delivery_latency_samples{subscriber,pattern}`: samples
 *   currently in each tracked subscriber's latency window.
 * - `eventbus_publish_admission_latency_ms{quantile,topic}`: per-topic
 *   publish-admission latency (EB-182) — first admission gate to the
 *   admission decision, nearest-rank p50/p95/p99 over each topic's
 *   bounded rolling window (`quantile` is "0.5", "0.95" or "0.99").
 *   Rendered only when the bus was built with
 *   `EventBusOptions.publishLatency` and the topic has samples.
 * - `eventbus_publish_admission_latency_samples{topic}`: samples
 *   currently in each topic's admission-latency window.
 * - `eventbus_publish_admission_slow_total{topic}`: samples at or above
 *   the configured slow-admission threshold in each topic's window.
 * - `eventbus_publish_admission_gate_latency_ms{quantile,topic,gate}`:
 *   per-(topic, gate) admission-gate call latency (EB-182) —
 *   nearest-rank p50/p95/p99; the gate-attribution breakdown for slow
 *   admissions (`gate` is one of `webhook`, `publisher-rate-limit`,
 *   `acl`, `schema`, `size`, `sampling`, `pressure`, `tenant-quota`,
 *   `global-rate-limit`, `topic-rate-limit`, `idempotency-dedup`).
 * - `eventbus_delayed_schedule_skew_ms{quantile}`: delayed-delivery
 *   schedule-skew distribution (EB-136) — each fanned-out schedule's
 *   actual fan-out time minus its due time (`deliverAt`), nearest-rank
 *   p50/p99 over the bus's bounded rolling window (`quantile` is "0.5"
 *   or "0.99"). The timer-fidelity signal for due-time SLAs. Rendered
 *   only when the bus was built with `EventBusOptions.scheduleSkew` and
 *   at least one schedule has been sampled; cancelled, expired,
 *   pause-shed and pause-buffered schedules never fan out and are never
 *   sampled.
 * - `eventbus_ack_latency_ms{quantile,subscriber,pattern}`: per-subscriber
 *   accepted→ack end-to-end latency distribution for reliable
 *   subscriptions with `ackLatency` enabled — nearest-rank p50/p95/p99
 *   over each subscriber's bounded rolling window (`quantile` is "0.5",
 *   "0.95" or "0.99"). The full round trip the producer's SLO depends
 *   on: queue dwell plus handler processing and consumer think time.
 *   Only ack-tracked subscribers appear.
 * - `eventbus_ack_latency_samples{subscriber,pattern}`: samples
 *   currently in each tracked subscriber's ack-latency window.
 * - `eventbus_subscriber_processing_latency_p99{subscriber,pattern}`:
 *   per-subscriber handler processing-latency p99 for subscriptions with
 *   `latencySlo` enabled — the windowed nearest-rank p99 the SLO alert
 *   fires on. Only SLO-tracked subscribers appear.
 * - `eventbus_lag_ms{quantile,subscriber,pattern}`: per-subscriber
 *   enqueue→drain dwell distribution for subscriptions with `lagMonitor`
 *   enabled — nearest-rank p50/p99 over each subscriber's bounded rolling
 *   window (`quantile` is "0.5" or "0.99"). Historical view of how long
 *   drained messages had waited.
 * - `eventbus_lag_samples{subscriber,pattern}`: samples currently in each
 *   lag-monitored subscriber's dwell window.
 * - `eventbus_lag_watermark_ms{subscriber,pattern}`: the live
 *   consumer-lag watermark — how long the oldest currently queued message
 *   has been waiting (0 when the queue is empty). The alerting signal;
 *   only subscriptions with `lagMonitor` enabled appear.
 * - `eventbus_key_hotspot_buffer_depth{subscriber,pattern,key}`: the
 *   current per-(subscriber, key) reorder-buffer depth for the hottest
 *   keyed ordering streams (see `BusStats.hotKeys`) — the live signal
 *   for per-key publish-order pile-up; only subscriptions with
 *   `keyHotspot` enabled appear.
 * - `eventbus_topic_rate_msg_per_sec{topic,window}`: per-topic publish
 *   rate in messages per second over the trailing 1s / 1m / 5m windows
 *   (`window` is "1s", "1m" or "5m"), load-average style. Only the hot
 *   topics carry this series — the bus's top-N by 1m rate (see
 *   `BusStats.hotTopics`) — so at most `HOT_TOPICS_LIMIT * 3` series.
 *
 * - `eventbus_group_lag_messages{group,topic}`: consumer-group
 *   consumption lag for classic round-robin groups — the highest
 *   per-topic `seq` assigned to any member minus the consumer checkpoint
 *   (`commitOffset`), excluding backlog held by live handoff-linger
 *   windows (see `BusStats.groupLag`). The Kafka-style lag number an
 *   operator pages on.
 * - `eventbus_group_partition_lag_messages{group,topic,partition}`: the
 *   same, per partition, for groups that opted into
 *   `GroupSubscribeOptions.partitions`.
 * - `eventbus_consumer_lag_slo_breach_total`: consumer-group lag SLO
 *   breach episodes fired (EB-112, see `EventBusOptions.groupLag.slo`):
 *   each edge-triggered `onLagSloBreach` firing moves this by one.
 * - `eventbus_mirror_conflicts_total{topic}`: mirror writes discarded as
 *   stale by per-key last-writer-wins arbitration on the mirror receive
 *   side (EB-131, see `BusStats.mirrorConflictsByTopic`) — the signal
 *   that two buses are writing the same key concurrently. Only topics
 *   with at least one conflict carry a series.
 * - `eventbus_tenant_quota_bytes_total{namespace,pattern}`: admitted
 *   payload bytes inside the sliding window per namespace tenant-quota
 *   rule (EB-162, see `BusStats.namespaceQuotas`) — the multi-tenant
 *   capacity signal; one series per registered rule.
 * - `eventbus_cep_patterns`: sequence patterns currently defined
 *   (EB-170; only emitted once a pattern was defined).
 * - `eventbus_cep_partial_matches`: partial sequence matches currently
 *   tracked (EB-170; only emitted once a pattern was defined).
 * - `eventbus_cep_absence_patterns`: sequence patterns with at least one
 *   absence step (EB-174; only emitted once a pattern was defined).
 * - `eventbus_cep_absence_matches_total`: absence steps satisfied —
 *   deadline passed with no matching event for the key (EB-174; only
 *   emitted once a pattern was defined).
 * - `eventbus_statestore_keys`: keys currently tracked by the
 *   materialized state store (EB-171; only emitted when the store is
 *   enabled).
 *
 * Cardinality note: the per-topic series grow with the number of distinct
 * topics ever published to — the same bound as `BusStats.topics` — so a
 * bus fanning out over millions of ad-hoc topic names will grow the
 * series count. Topic names come from the publisher, never from subscriber
 * input, so this is bounded by the application's own topic space. The
 * per-subscriber latency series (four per latency-tracked subscription),
 * the per-subscriber ack-latency series (four per ack-tracked reliable
 * subscription), and the per-subscriber lag series (four per
 * lag-monitored subscription) are bounded by the opted-in subscriber
 * count — monitoring is opt-in per subscription, so unmonitored
 * subscribers add no series. The per-subscriber processing-latency p99
 * series (one per `latencySlo`-tracked subscription) is bounded the same
 * way. The
 * `eventbus_topic_rate_msg_per_sec` series are the exception to the
 * per-topic rule: they are deliberately limited to the hot-topics set
 * (10 topics x 3 windows = 30 series max), because rate decisions need
 * the busiest topics, not the full topic space. The
 * `eventbus_key_hotspot_buffer_depth` series are capped the same way at
 * `HOT_KEYS_LIMIT` (10) by `BusStats.hotKeys`, and only hotspot-monitored
 * subscriptions are sampled at all. The
 * `eventbus_topic_partition_published_total` series are bounded by the
 * topic's configured partition count — they are only emitted for topics
 * with more than one partition, so unpartitioned topics add no series.
 *
 * `renderPrometheus` tolerates a stats object that predates the rate
 * fields (a missing `hotTopics` renders no rate series instead of
 * throwing) — the same defensive stance that keeps hand-built `BusStats`
 * fixtures working across new fields.
 *
 * The bus has no HTTP server of its own; wire the returned text into your
 * scrape endpoint with `PROMETHEUS_CONTENT_TYPE`, e.g.:
 *
 * ```ts
 * import { EventBus } from './src/bus.ts';
 * import { renderPrometheus, PROMETHEUS_CONTENT_TYPE } from './src/metrics.ts';
 *
 * const bus = new EventBus();
 * // ... in your HTTP handler:
 * res.writeHead(200, { 'content-type': PROMETHEUS_CONTENT_TYPE });
 * res.end(renderPrometheus(bus.getStats()));
 * ```
 */
import type { BusStats } from './bus.ts';

/** Content-Type for a Prometheus exposition response body. */
export const PROMETHEUS_CONTENT_TYPE = 'text/plain; version=0.0.4; charset=utf-8';

/** Escape a label value per the Prometheus exposition format. */
function escapeLabelValue(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/"/g, '\\"');
}

/** Render a `BusStats` snapshot as Prometheus text exposition (0.0.4). */
export function renderPrometheus(stats: BusStats): string {
  const lines: string[] = [];
  const counter = (name: string, help: string, value: number): void => {
    lines.push(`# HELP ${name} ${help}`);
    lines.push(`# TYPE ${name} counter`);
    lines.push(`${name} ${value}`);
  };
  const gauge = (name: string, help: string, value: number): void => {
    lines.push(`# HELP ${name} ${help}`);
    lines.push(`# TYPE ${name} gauge`);
    lines.push(`${name} ${value}`);
  };

  counter(
    'eventbus_published_messages_total',
    'Total messages accepted via publish/publishBatch.',
    stats.totalPublished,
  );
  counter(
    'eventbus_delivered_messages_total',
    'Total messages handed to subscriber handlers (each handler invocation; at-least-once redeliveries count again).',
    stats.deliveredMessages,
  );
  counter(
    'eventbus_dropped_messages_total',
    'Total messages shed by subscriber backpressure queues.',
    stats.droppedMessages,
  );
  counter(
    'eventbus_shadowed_messages_total',
    'Total messages handed to shadow-subscriber handlers (mirror deliveries; never counted in delivered).',
    stats.shadowedMessages ?? 0,
  );
  counter(
    'eventbus_shadow_dropped_messages_total',
    "Total messages shed by shadow subscribers' independent backpressure queues.",
    stats.shadowDroppedMessages ?? 0,
  );
  counter(
    'eventbus_expired_messages_total',
    'Total messages discarded because their TTL expired before delivery.',
    stats.expiredMessages,
  );
  counter(
    'eventbus_throttled_messages_total',
    'Total messages shed at the publish side by adaptive throttling.',
    stats.throttledMessages,
  );
  counter(
    'eventbus_rejected_messages_total',
    'Total publishes rejected by admission gates before admission (schema validation, broker ACL, per-topic message-size caps, expired migration aliases).',
    stats.rejectedMessages,
  );
  counter(
    'eventbus_deprecated_schema_messages_total',
    'Total publishes admitted under a superseded-but-graced schema version (EB-86).',
    stats.deprecatedSchemaVersions ?? 0,
  );
  counter(
    'eventbus_unregistered_topic_publishes_total',
    'Total publishes to topics with no live registration in the topic catalog (advisory only; these publishes were delivered normally).',
    stats.unregisteredTopicPublishes ?? 0,
  );
  counter(
    'eventbus_late_messages_total',
    'Total messages admitted with a business event time older than their topic event-time watermark (EB-59).',
    stats.lateMessages ?? 0,
  );
  counter(
    'eventbus_rate_limited_messages_total',
    'Total messages shed by publish-side per-topic rate limiting.',
    stats.rateLimitedMessages,
  );
  counter(
    'eventbus_publisher_rate_limited_messages_total',
    'Total messages shed by per-publisher publish-side rate limiting (EB-79).',
    stats.publisherRateLimited,
  );
  counter(
    'eventbus_global_rate_limited_messages_total',
    'Total messages shed by the bus-level global publish rate limit (EB-180): shed in the admission phase after every per-topic gate, so no sequence number is consumed and no per-topic rate-limit token is burned.',
    stats.globalRateLimitedMessages ?? 0,
  );
  counter(
    'eventbus_sampled_messages_total',
    'Total non-critical publishes shed by publish-side adaptive sampling (EB-38): sampled before sequence-number assignment, so subscribers see no gap, and before the topic rate-limit budget, so no token is burned.',
    stats.sampledMessages ?? 0,
  );
  counter(
    'eventbus_paused_messages_total',
    'Total publishes shed by topic pauses in shed mode (EB-88).',
    stats.pausedMessages ?? 0,
  );
  counter(
    'eventbus_circuit_broken_messages_total',
    'Total publishes refused by publish circuit breakers (EB-116): the topic breaker was open (every subscriber degraded or shedding), so publish threw CircuitBreakerOpenError (fast-fail).',
    stats.circuitBrokenMessages ?? 0,
  );
  counter(
    'eventbus_circuit_breaker_trips_total',
    'Total times a publish circuit breaker opened (EB-116).',
    stats.circuitBreakerTrips ?? 0,
  );
  counter(
    'eventbus_duplicate_messages_total',
    'Total idempotent publishes suppressed as duplicates.',
    stats.duplicateMessages,
  );
  counter(
    'eventbus_filtered_messages_total',
    "Total messages skipped by subscriber content filters.",
    stats.filteredMessages,
  );
  counter(
    'eventbus_header_filtered_messages_total',
    'Total messages skipped by subscriber header filters (EB-153).',
    stats.headerFilteredMessages ?? 0,
  );
  counter(
    'eventbus_signature_failures_total',
    'Total messages discarded by verifySignature subscribers because the envelope HMAC signature was missing or invalid (EB-84).',
    stats.signatureFailures ?? 0,
  );
  counter(
    'eventbus_schema_migration_failures_total',
    'Total messages discarded because no schema-version migration path reached a subscriber-declared expected version (EB-186).',
    stats.migrationFailures ?? 0,
  );
  counter(
    'eventbus_dedup_dropped_messages_total',
    'Total messages suppressed by subscriber-side exactly-once dedup windows.',
    stats.dedupDropped,
  );
  counter(
    'eventbus_dead_lettered_messages_total',
    'Total reliable messages moved into subscriber dead-letter queues.',
    stats.deadLetteredMessages,
  );
  counter(
    'eventbus_diagnostic_events_total',
    'Total DLQ diagnostic events admitted to their diagnostic topic.',
    stats.diagnosticEvents ?? 0,
  );
  counter(
    'eventbus_admission_dlq_events_total',
    'Total admission-DLQ diagnostic records emitted to dead-letter topics (EB-139).',
    stats.admissionDlqEvents ?? 0,
  );
  counter(
    'eventbus_sequence_gaps_total',
    'Total sequence numbers observed missing by subscribers.',
    stats.sequenceGaps,
  );
  counter(
    'eventbus_expiring_soon_alerts_total',
    'Total TTL expiring-soon warnings fired for subscriber queue heads (EB-110).',
    stats.expiringSoonAlerts ?? 0,
  );
  counter(
    'eventbus_consumer_lag_slo_breach_total',
    'Total consumer-group lag SLO breach episodes fired (EB-112, see EventBusOptions.groupLag.slo).',
    stats.lagSloBreaches ?? 0,
  );
  counter(
    'eventbus_keyed_reordered_messages_total',
    'Total keyed messages held in per-(subscriber, key) reorder buffers because an earlier keySeq had not been fanned out yet.',
    stats.keyedReorderedMessages,
  );
  counter(
    'eventbus_offset_store_write_errors_total',
    'Total failed OffsetStore.set calls (EB-114, see EventBusOptions.offsetStore): a throwing custom store never breaks the in-memory commit.',
    stats.offsetStoreWriteErrors,
  );
  counter(
    'eventbus_autocommitted_offsets_total',
    'Total consumer-group offset checkpoints written by offset auto-commit (GroupSubscribeOptions.autoCommit, EB-127): each went through commitOffset, so the offset journal and OffsetStore write-through saw the same writes.',
    stats.autoCommittedOffsets ?? 0,
  );
  counter(
    'eventbus_hotkey_mitigations_total',
    'Total hot-key auto-mitigation engagements: a (subscriber, key) reorder buffer reached the hotspot threshold while auto-mitigation was enabled (EB-115).',
    stats.hotKeyMitigations,
  );
  counter(
    'eventbus_hotkey_shed_messages_total',
    'Total keyed messages shed by hot-key auto-mitigation instead of piling onto a mitigated reorder buffer (EB-115).',
    stats.hotKeyShedMessages,
  );
  counter(
    'eventbus_causal_buffer_dropped_messages_total',
    'Total causal messages dropped from full per-source reorder buffers (drop-oldest anti-deadlock).',
    stats.causalBuffer?.droppedMessages ?? 0,
  );
  counter(
    'eventbus_causal_buffer_regressed_messages_total',
    'Total causal messages delivered immediately on clock regression.',
    stats.causalBuffer?.regressedMessages ?? 0,
  );

  gauge('eventbus_subscribers', 'Currently active subscriptions.', stats.totalSubscribers);
  gauge(
    'eventbus_retained_topics',
    'Topics currently holding an MQTT-style retained message (EB-87).',
    stats.retainedTopics?.length ?? 0,
  );
  gauge(
    'eventbus_unacked_deliveries',
    'Reliable deliveries handed out but not yet acked or nacked.',
    stats.unackedDeliveries,
  );
  gauge(
    'eventbus_throttled_subscribers',
    'Subscriptions currently under adaptive publish-side throttling.',
    stats.throttledSubscribers,
  );
  gauge(
    'eventbus_degraded_subscribers',
    'Subscriptions currently auto-paused by health probing.',
    stats.degradedSubscribers,
  );
  gauge(
    'eventbus_paused_subscribers',
    'Subscriptions currently manually paused by the operator (see EventBus.pauseSubscriber, EB-113).',
    stats.pausedSubscribers ?? 0,
  );
  gauge(
    'eventbus_shaped_subscribers',
    'Subscriptions currently held back by delivery-side rate shaping.',
    stats.shapedSubscribers,
  );
  gauge(
    'eventbus_credit_exhausted_subscribers',
    'Subscriptions currently out of delivery credit (EB-117): the balance reached zero through deliveries, so delivery is held — the backlog stays queued under the normal backpressure policy — until more credit is granted via grantCredits.',
    stats.creditExhaustedSubscribers ?? 0,
  );
  gauge(
    'eventbus_pending_delayed',
    'Delayed messages scheduled but not yet due.',
    stats.pendingDelayed,
  );
  gauge(
    'eventbus_paused_topics',
    'Currently active topic-pause rules (EB-88).',
    stats.pausedTopics?.length ?? 0,
  );
  gauge(
    'eventbus_circuit_breakers_open',
    'Publish circuit breakers currently open (fast-failing; EB-116).',
    (stats.circuitBreakers ?? []).filter((row) => row.state === 'open').length,
  );
  gauge(
    'eventbus_queue_bytes',
    'Estimated buffered payload bytes across all live subscriber backpressure queues.',
    stats.queueBytes ?? 0,
  );
  gauge(
    'eventbus_causal_buffer_depth',
    'Messages currently held in per-(subscriber, source) causal reorder buffers, waiting on their dependencies.',
    stats.causalBuffer?.depth ?? 0,
  );

  // Per-subscriber delivery-latency series (only subscriptions with
  // `deliveryLatency` enabled), in subscription order (same as
  // BusStats.deliveryLatency).
  lines.push(
    '# HELP eventbus_delivery_latency_ms Per-subscriber enqueue-to-delivery queue-dwell distribution (nearest-rank quantiles over the rolling sample window).',
  );
  lines.push('# TYPE eventbus_delivery_latency_ms gauge');
  lines.push(
    '# HELP eventbus_delivery_latency_samples Samples currently in the subscriber latency window.',
  );
  lines.push('# TYPE eventbus_delivery_latency_samples gauge');
  for (const s of stats.deliveryLatency) {
    const labels = `subscriber="${escapeLabelValue(s.subscriberId)}",pattern="${escapeLabelValue(s.pattern)}"`;
    lines.push(`eventbus_delivery_latency_ms{quantile="0.5",${labels}} ${s.p50Ms}`);
    lines.push(`eventbus_delivery_latency_ms{quantile="0.95",${labels}} ${s.p95Ms}`);
    lines.push(`eventbus_delivery_latency_ms{quantile="0.99",${labels}} ${s.p99Ms}`);
    lines.push(`eventbus_delivery_latency_samples{${labels}} ${s.samples}`);
  }

  // Publish-side admission latency (EB-182, opt-in via
  // `EventBusOptions.publishLatency`): per-topic total-span distribution
  // plus the per-(topic, gate) breakdown. HELP/TYPE always render like
  // the other opt-in blocks; the series render only for topics with
  // samples — a disabled bus, or one whose window is still empty,
  // contributes no series. The `?? []` keeps the renderer tolerant of a
  // stats object that predates the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_publish_admission_latency_ms Per-topic publish-admission latency: first admission gate to the admission decision, in milliseconds — nearest-rank p50/p95/p99 over the rolling sample window. Never double-samples the delivery-side distributions.',
  );
  lines.push('# TYPE eventbus_publish_admission_latency_ms gauge');
  lines.push(
    '# HELP eventbus_publish_admission_latency_samples Samples currently in the topic admission-latency window.',
  );
  lines.push('# TYPE eventbus_publish_admission_latency_samples gauge');
  lines.push(
    '# HELP eventbus_publish_admission_slow_total Samples at or above the configured slow-admission threshold in the topic admission-latency window.',
  );
  lines.push('# TYPE eventbus_publish_admission_slow_total gauge');
  for (const s of stats.publishLatency ?? []) {
    const labels = `topic="${escapeLabelValue(s.topic)}"`;
    lines.push(`eventbus_publish_admission_latency_ms{quantile="0.5",${labels}} ${s.p50Ms}`);
    lines.push(`eventbus_publish_admission_latency_ms{quantile="0.95",${labels}} ${s.p95Ms}`);
    lines.push(`eventbus_publish_admission_latency_ms{quantile="0.99",${labels}} ${s.p99Ms}`);
    lines.push(`eventbus_publish_admission_latency_samples{${labels}} ${s.samples}`);
    lines.push(`eventbus_publish_admission_slow_total{${labels}} ${s.slowCount}`);
  }
  lines.push(
    '# HELP eventbus_publish_admission_gate_latency_ms Per-(topic, gate) admission-gate call latency in milliseconds — nearest-rank p50/p95/p99 over the rolling sample window; the gate-attribution breakdown for slow admissions.',
  );
  lines.push('# TYPE eventbus_publish_admission_gate_latency_ms gauge');
  for (const s of stats.publishLatencyGates ?? []) {
    const labels = `topic="${escapeLabelValue(s.topic)}",gate="${s.gate}"`;
    lines.push(`eventbus_publish_admission_gate_latency_ms{quantile="0.5",${labels}} ${s.p50Ms}`);
    lines.push(`eventbus_publish_admission_gate_latency_ms{quantile="0.95",${labels}} ${s.p95Ms}`);
    lines.push(`eventbus_publish_admission_gate_latency_ms{quantile="0.99",${labels}} ${s.p99Ms}`);
  }

  // Delayed-delivery schedule-skew distribution (EB-136): bus-wide,
  // opt-in via `EventBusOptions.scheduleSkew`. HELP/TYPE always render
  // like the other opt-in blocks; the series render only once at least
  // one schedule has been sampled (`samples > 0`) — a disabled bus, or
  // one whose window is still empty, contributes no series. The `??`
  // keeps the renderer tolerant of a stats object that predates the
  // field (hand-built fixtures included).
  lines.push(
    "# HELP eventbus_delayed_schedule_skew_ms Delayed-delivery schedule skew: actual fan-out time minus the schedule's due time (deliverAt), in milliseconds — nearest-rank p50/p99 over the rolling sample window. Only schedules that actually fan out are sampled.",
  );
  lines.push('# TYPE eventbus_delayed_schedule_skew_ms gauge');
  const skew = stats.delayedScheduleSkew ?? undefined;
  if (skew !== undefined && skew.samples > 0) {
    lines.push(`eventbus_delayed_schedule_skew_ms{quantile="0.5"} ${skew.p50Ms}`);
    lines.push(`eventbus_delayed_schedule_skew_ms{quantile="0.99"} ${skew.p99Ms}`);
  }

  // Per-subscriber ack-latency series (only reliable subscriptions with
  // `ackLatency` enabled), in subscription order (same as
  // BusStats.ackLatency). The `?? []` keeps the renderer tolerant of a
  // stats object that predates the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_ack_latency_ms Per-subscriber accepted-to-ack end-to-end latency distribution for reliable subscriptions (nearest-rank quantiles over the rolling sample window).',
  );
  lines.push('# TYPE eventbus_ack_latency_ms gauge');
  lines.push(
    '# HELP eventbus_ack_latency_samples Samples currently in the subscriber ack-latency window.',
  );
  lines.push('# TYPE eventbus_ack_latency_samples gauge');
  for (const s of stats.ackLatency ?? []) {
    const labels = `subscriber="${escapeLabelValue(s.subscriberId)}",pattern="${escapeLabelValue(s.pattern)}"`;
    lines.push(`eventbus_ack_latency_ms{quantile="0.5",${labels}} ${s.p50Ms}`);
    lines.push(`eventbus_ack_latency_ms{quantile="0.95",${labels}} ${s.p95Ms}`);
    lines.push(`eventbus_ack_latency_ms{quantile="0.99",${labels}} ${s.p99Ms}`);
    lines.push(`eventbus_ack_latency_samples{${labels}} ${s.samples}`);
  }

  // Per-subscriber handler processing-latency p99 (only subscriptions
  // with `latencySlo` enabled), in subscription order (same as
  // BusStats.subscriberLatencyP99). One series per tracked subscriber —
  // the SLO alerting signal; unmonitored subscribers add none. The `?? []`
  // keeps the renderer tolerant of a stats object that predates the field
  // (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_subscriber_processing_latency_p99 Per-subscriber handler processing-latency p99 over the rolling sample window (nearest-rank); the latency-SLO alerting signal.',
  );
  lines.push('# TYPE eventbus_subscriber_processing_latency_p99 gauge');
  for (const s of stats.subscriberLatencyP99 ?? []) {
    const labels = `subscriber="${escapeLabelValue(s.subscriberId)}",pattern="${escapeLabelValue(s.pattern)}"`;
    lines.push(`eventbus_subscriber_processing_latency_p99{${labels}} ${s.p99Ms}`);
  }

  // Per-subscriber nack-reason counters (EB-129): one series per
  // (subscriber, reason) with a non-zero count, in subscription order
  // (same as BusStats.nackReasons). Zero-count classes emit no series,
  // and reliable subscribers with zero counts emit none at all, so
  // cardinality stays proportional to actual redelivery activity.
  // Diagnostic only — orthogonal to the EB-18 health probe. The `?? []`
  // keeps the renderer tolerant of a stats object that predates the
  // field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_nack_reasons_total Total redeliveries per reliable subscriber, classified by trigger: handler_throw (the handler threw synchronously), explicit_nack (the consumer called nack()/nackBatch()), ack_timeout (the ack-timeout timer requeued the delivery). Diagnostic only — orthogonal to the EB-18 health probe.',
  );
  lines.push('# TYPE eventbus_nack_reasons_total counter');
  for (const s of stats.nackReasons ?? []) {
    const labels = `subscriber="${escapeLabelValue(s.subscriberId)}"`;
    if (s.handlerThrow > 0) {
      lines.push(`eventbus_nack_reasons_total{${labels},reason="handler_throw"} ${s.handlerThrow}`);
    }
    if (s.explicitNack > 0) {
      lines.push(`eventbus_nack_reasons_total{${labels},reason="explicit_nack"} ${s.explicitNack}`);
    }
    if (s.ackTimeout > 0) {
      lines.push(`eventbus_nack_reasons_total{${labels},reason="ack_timeout"} ${s.ackTimeout}`);
    }
  }

  // Per-subscriber lag watermark series (only subscriptions with
  // `lagMonitor` enabled), in subscription order (same as BusStats.lag).
  // The `?? []` keeps the renderer tolerant of a stats object that
  // predates the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_lag_ms Per-subscriber enqueue-to-drain dwell distribution (nearest-rank p50/p99 over the rolling sample window).',
  );
  lines.push('# TYPE eventbus_lag_ms gauge');
  lines.push(
    '# HELP eventbus_lag_samples Samples currently in the subscriber lag dwell window.',
  );
  lines.push('# TYPE eventbus_lag_samples gauge');
  lines.push(
    '# HELP eventbus_lag_watermark_ms Live consumer-lag watermark: how long the oldest currently queued message has been waiting (0 when the queue is empty).',
  );
  lines.push('# TYPE eventbus_lag_watermark_ms gauge');
  for (const s of stats.lag ?? []) {
    const labels = `subscriber="${escapeLabelValue(s.subscriberId)}",pattern="${escapeLabelValue(s.pattern)}"`;
    lines.push(`eventbus_lag_ms{quantile="0.5",${labels}} ${s.p50Ms}`);
    lines.push(`eventbus_lag_ms{quantile="0.99",${labels}} ${s.p99Ms}`);
    lines.push(`eventbus_lag_samples{${labels}} ${s.samples}`);
    lines.push(`eventbus_lag_watermark_ms{${labels}} ${s.watermarkMs}`);
  }

  // Per-(subscriber, key) keyed ordering reorder-buffer depth (EB-47, only
  // subscriptions with `keyHotspot` enabled), hottest first (same order as
  // BusStats.hotKeys) — the live signal for per-key publish-order pile-up:
  // a key whose depth keeps growing has a predecessor that is not fanning
  // out. The `?? []` keeps the renderer tolerant of a stats object that
  // predates the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_key_hotspot_buffer_depth Current per-(subscriber, key) reorder-buffer depth for the hottest keyed ordering streams (hottest first, at most 10).',
  );
  lines.push('# TYPE eventbus_key_hotspot_buffer_depth gauge');
  for (const h of stats.hotKeys ?? []) {
    const labels = `subscriber="${escapeLabelValue(h.subscriberId)}",pattern="${escapeLabelValue(h.pattern)}",key="${escapeLabelValue(h.key)}"`;
    lines.push(`eventbus_key_hotspot_buffer_depth{${labels}} ${h.bufferedDepth}`);
  }

  // Per-topic series, in first-publish order (same as BusStats.topics).
  lines.push(
    '# HELP eventbus_topic_published_messages_total Total messages published to the topic.',
  );
  lines.push('# TYPE eventbus_topic_published_messages_total counter');
  lines.push(
    '# HELP eventbus_topic_paused_messages_total Total publishes to the topic shed by topic pauses in shed mode (EB-88).',
  );
  lines.push('# TYPE eventbus_topic_paused_messages_total counter');
  lines.push(
    '# HELP eventbus_topic_circuit_broken_messages_total Total publishes to the topic refused by its publish circuit breaker (EB-116).',
  );
  lines.push('# TYPE eventbus_topic_circuit_broken_messages_total counter');
  lines.push(
    '# HELP eventbus_topic_subscribers Subscribers matched by the most recent publish to the topic (fan-out width).',
  );
  lines.push('# TYPE eventbus_topic_subscribers gauge');
  lines.push(
    '# HELP eventbus_topic_late_messages_total Messages whose business event time was older than the topic event-time watermark (EB-59).',
  );
  lines.push('# TYPE eventbus_topic_late_messages_total counter');
  lines.push(
    '# HELP eventbus_topic_event_time_watermark The topic event-time watermark in epoch milliseconds (EB-59); absent until the first event-time publish.',
  );
  lines.push('# TYPE eventbus_topic_event_time_watermark gauge');
  for (const t of stats.topics) {
    const label = `topic="${escapeLabelValue(t.topic)}"`;
    lines.push(`eventbus_topic_published_messages_total{${label}} ${t.publishedMessages}`);
    lines.push(`eventbus_topic_paused_messages_total{${label}} ${t.pausedMessages ?? 0}`);
    // `?? 0` keeps the renderer tolerant of a stats object that predates
    // the field (hand-built fixtures included), mirroring the late-message
    // idiom below.
    lines.push(`eventbus_topic_circuit_broken_messages_total{${label}} ${t.circuitBrokenMessages ?? 0}`);
    lines.push(`eventbus_topic_subscribers{${label}} ${t.subscriberCount}`);
    // `?? 0` keeps the renderer tolerant of a stats object that predates
    // the field (hand-built fixtures included), mirroring the namespaces
    // idiom below.
    lines.push(`eventbus_topic_late_messages_total{${label}} ${t.lateMessages ?? 0}`);
    // Shares the `eventbus_signature_failures_total` name with the global
    // counter above — same metric, per-topic label set — so no second
    // HELP/TYPE block is emitted here.
    lines.push(`eventbus_signature_failures_total{${label}} ${t.signatureFailures ?? 0}`);
    // Shares the `eventbus_schema_migration_failures_total` name with
    // the global counter above — same convention, no second HELP/TYPE.
    lines.push(`eventbus_schema_migration_failures_total{${label}} ${t.migrationFailures ?? 0}`);
    if (t.watermark !== undefined) {
      lines.push(`eventbus_topic_event_time_watermark{${label}} ${t.watermark}`);
    }
  }

  // Explicit idempotency-key hits (EB-151): per-topic suppression counts.
  // Activity-gated — the HELP/TYPE block and the series render only when
  // at least one topic has a hit, so a bus without idempotency-key
  // traffic renders byte-identical exposition to before. `?? 0` keeps
  // the renderer tolerant of a stats object that predates the field
  // (hand-built fixtures included), mirroring the per-topic idiom above.
  const idempotentHitTopics = stats.topics.filter((t) => (t.idempotentHits ?? 0) > 0);
  if (idempotentHitTopics.length > 0) {
    lines.push(
      '# HELP eventbus_idempotent_hits_total Publishes suppressed by explicit idempotency keys (EB-151): the retry arrived with a (topic, idempotencyKey) pair already claimed within the idempotency window, so it returned the first publish result instead of publishing again.',
    );
    lines.push('# TYPE eventbus_idempotent_hits_total counter');
    for (const t of stats.topics) {
      const label = `topic="${escapeLabelValue(t.topic)}"`;
      lines.push(`eventbus_idempotent_hits_total{${label}} ${t.idempotentHits ?? 0}`);
    }
  }

  // Subscriber-side delivery sampling (EB-152): per-topic admitted /
  // skipped counts. Activity-gated — each HELP/TYPE block and its series
  // render only when at least one topic has a count, so a bus without
  // delivery sampling renders byte-identical exposition to before.
  // `?? 0` keeps the renderer tolerant of a stats object that predates
  // the field (hand-built fixtures included), mirroring the per-topic
  // idiom above. The names deliberately avoid the EB-38 publish-side
  // `eventbus_sampled_messages_total` series.
  const deliverySampledTopics = stats.topics.filter((t) => (t.sampledDeliveries ?? 0) > 0);
  if (deliverySampledTopics.length > 0) {
    lines.push(
      '# HELP eventbus_delivery_sampled_messages_total Messages admitted by subscriber-side delivery-sampling gates (EB-152): the seq % N decision let them through toward the queue (they may still be filtered, throttled or shed downstream).',
    );
    lines.push('# TYPE eventbus_delivery_sampled_messages_total counter');
    for (const t of stats.topics) {
      const label = `topic="${escapeLabelValue(t.topic)}"`;
      lines.push(`eventbus_delivery_sampled_messages_total{${label}} ${t.sampledDeliveries ?? 0}`);
    }
  }
  const deliverySampleDroppedTopics = stats.topics.filter((t) => (t.sampleDropped ?? 0) > 0);
  if (deliverySampleDroppedTopics.length > 0) {
    lines.push(
      '# HELP eventbus_delivery_sample_dropped_total Messages skipped by subscriber-side delivery-sampling gates (EB-152): deliberately skipped — never queued, no backpressure/throttle/shaping/credit budget burned, no ack obligation, never the DLQ, never a sequence gap, no consumer-group offset recorded.',
    );
    lines.push('# TYPE eventbus_delivery_sample_dropped_total counter');
    for (const t of stats.topics) {
      const label = `topic="${escapeLabelValue(t.topic)}"`;
      lines.push(`eventbus_delivery_sample_dropped_total{${label}} ${t.sampleDropped ?? 0}`);
    }
  }

  // Subscriber-side header filters (EB-153): per-topic skip counts.
  // Activity-gated — the series render only when at least one topic has
  // a header-filtered message, so a bus without header filters renders
  // byte-identical exposition to before. Shares the
  // `eventbus_header_filtered_messages_total` name with the global
  // counter above — same metric, per-topic label set — so no second
  // HELP/TYPE block is emitted here. `?? 0` keeps the renderer tolerant
  // of a stats object that predates the field (hand-built fixtures
  // included), mirroring the per-topic idiom above.
  const headerFilteredTopics = stats.topics.filter((t) => (t.headerFilteredMessages ?? 0) > 0);
  if (headerFilteredTopics.length > 0) {
    for (const t of stats.topics) {
      const label = `topic="${escapeLabelValue(t.topic)}"`;
      lines.push(
        `eventbus_header_filtered_messages_total{${label}} ${t.headerFilteredMessages ?? 0}`,
      );
    }
  }

  // Per-topic payload-size histograms (EB-143): cumulative buckets in
  // bytes — `le="+Inf"` equals the total sample count — plus the
  // `_count`/`_sum` pair, the Prometheus histogram convention. Only
  // topics with a matching histogram rule carry series, so a bus without
  // histogram rules renders no series at all. `t.payloadHistogram`
  // stays optional so the renderer tolerates a stats object that
  // predates the field (hand-built fixtures included).
  const histogramBuckets: Array<{ topic: string; le: string; count: number }> = [];
  const histogramTotals: Array<{ topic: string; count: number; sum: number }> = [];
  for (const t of stats.topics) {
    const h = t.payloadHistogram;
    if (h === undefined) continue;
    for (const b of h.buckets) {
      histogramBuckets.push({ topic: t.topic, le: String(b.le), count: b.count });
    }
    histogramBuckets.push({ topic: t.topic, le: '+Inf', count: h.count });
    histogramTotals.push({ topic: t.topic, count: h.count, sum: h.sum });
  }
  if (histogramBuckets.length > 0) {
    lines.push(
      '# HELP eventbus_payload_bytes_bucket Cumulative payload-size histogram of admitted publishes per topic, in bytes (EB-143; opt-in via setTopicPayloadHistogram; le="+Inf" equals the total sample count).',
    );
    lines.push('# TYPE eventbus_payload_bytes_bucket histogram');
    for (const r of histogramBuckets) {
      lines.push(
        `eventbus_payload_bytes_bucket{topic="${escapeLabelValue(r.topic)}",le="${r.le}"} ${r.count}`,
      );
    }
    for (const r of histogramTotals) {
      const label = `topic="${escapeLabelValue(r.topic)}"`;
      lines.push(`eventbus_payload_bytes_count{${label}} ${r.count}`);
      lines.push(`eventbus_payload_bytes_sum{${label}} ${r.sum}`);
    }
  }

  // Topic-fair scheduling (EB-135): per-topic scheduled-message counts,
  // in first-scheduled order (same as BusStats.topicFairness). Only
  // topics with at least one scheduled message render a series — a bus
  // with no fair-scheduling subscriber emits just the HELP/TYPE block.
  // `?? {}` keeps the renderer tolerant of a stats object that predates
  // the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_fair_scheduled_messages_total Messages dequeued by the topic-fair scheduler for the topic (EB-135; "scheduled", not "delivered" — see BusStats.topicFairness).',
  );
  lines.push('# TYPE eventbus_fair_scheduled_messages_total counter');
  const fairScheduled = stats.topicFairness?.scheduledByTopic ?? {};
  for (const topic of Object.keys(fairScheduled)) {
    const scheduled = fairScheduled[topic];
    if (scheduled > 0) {
      lines.push(
        `eventbus_fair_scheduled_messages_total{topic="${escapeLabelValue(topic)}"} ${scheduled}`,
      );
    }
  }

  // Subscriber drain priority (EB-166): per-class drain-visit counts.
  // Activity-gated — the HELP/TYPE block and the series render only when
  // a non-default priority class has at least one dispatch, so a bus
  // without drain priorities renders byte-identical exposition to before.
  // `?? {}` keeps the renderer tolerant of a stats object that predates
  // the field (hand-built fixtures included).
  const priorityDispatches = stats.subscriberPriority?.dispatches ?? {};
  const priorityActive =
    (priorityDispatches.low ?? 0) > 0 ||
    (priorityDispatches.high ?? 0) > 0 ||
    (priorityDispatches.custom ?? 0) > 0;
  if (priorityActive) {
    lines.push(
      '# HELP eventbus_subscriber_priority_dispatch_total Drain visits scheduled per subscriber priority class (EB-166; "scheduled", not "delivered" — a visit whose subscriber was paused still counts).',
    );
    lines.push('# TYPE eventbus_subscriber_priority_dispatch_total counter');
    for (const label of ['low', 'normal', 'high', 'custom'] as const) {
      lines.push(
        `eventbus_subscriber_priority_dispatch_total{class="${label}"} ${priorityDispatches[label] ?? 0}`,
      );
    }
  }

  // Per-(topic, partition) admitted publishes (EB-90), in first-publish
  // topic order and ascending partition order (same as
  // BusStats.topics[].partitions). Only topics with more than one
  // configured partition carry this series — a single-partition topic
  // renders byte-identical exposition to before. The `?? []` keeps the
  // renderer tolerant of a stats object that predates the field
  // (hand-built fixtures included).
  const partitionRows: Array<{ topic: string; partition: number; publishedMessages: number }> = [];
  for (const t of stats.topics) {
    for (const p of t.partitions ?? []) {
      partitionRows.push({ topic: t.topic, partition: p.partition, publishedMessages: p.publishedMessages });
    }
  }
  if (partitionRows.length > 0) {
    lines.push(
      '# HELP eventbus_topic_partition_published_total Total admitted publishes per topic partition (publish-side partition routing, EB-90).',
    );
    lines.push('# TYPE eventbus_topic_partition_published_total counter');
    for (const r of partitionRows) {
      const labels = `topic="${escapeLabelValue(r.topic)}",partition="${r.partition}"`;
      lines.push(`eventbus_topic_partition_published_total{${labels}} ${r.publishedMessages}`);
    }
  }

  // Per-namespace aggregates (EB-52), in registration order (same as
  // BusStats.namespaces). Emitted only when the bus has namespaces
  // registered — a namespace-free bus renders byte-identical exposition
  // to before. The `?? []` keeps the renderer tolerant of a stats object
  // that predates the field (hand-built fixtures included).
  const namespaces = stats.namespaces ?? [];
  if (namespaces.length > 0) {
    lines.push(
      '# HELP eventbus_namespace_published_messages_total Total messages published to topics in the namespace.',
    );
    lines.push('# TYPE eventbus_namespace_published_messages_total counter');
    lines.push(
      '# HELP eventbus_namespace_subscribers Currently active subscriptions registered through the namespace.',
    );
    lines.push('# TYPE eventbus_namespace_subscribers gauge');
    for (const ns of namespaces) {
      const label = `namespace="${escapeLabelValue(ns.namespace)}"`;
      lines.push(`eventbus_namespace_published_messages_total{${label}} ${ns.publishedMessages}`);
      lines.push(`eventbus_namespace_subscribers{${label}} ${ns.subscribers}`);
    }
  }

  // Per-publisher admitted publishes (EB-65), in getStats().publishers
  // order (most-published first, ties on the identity). Emitted only when
  // at least one publisher has admitted a publish — a publisher-free bus
  // renders byte-identical exposition to before. The `?? []` keeps the
  // renderer tolerant of a stats object that predates the field
  // (hand-built fixtures included).
  const publishers = stats.publishers ?? [];
  if (publishers.length > 0) {
    lines.push(
      '# HELP eventbus_publisher_published_messages_total Total admitted publishes per publisher identity (see PublishOptions.publisherId); anonymous publishes move no per-publisher counter.',
    );
    lines.push('# TYPE eventbus_publisher_published_messages_total counter');
    for (const p of publishers) {
      const label = `publisher="${escapeLabelValue(p.publisher)}"`;
      lines.push(`eventbus_publisher_published_messages_total{${label}} ${p.publishedMessages}`);
    }
  }

  // Per-topic sliding-window publish rates (EB-34), in hot-topics order
  // (hottest first, same as BusStats.hotTopics). Only the hot set carries
  // these series — emitting them for every topic ever published to would
  // tie series cardinality to the topic space; the set is capped at
  // HOT_TOPICS_LIMIT (10) topics, so at most 30 series. The `?? []`
  // keeps the renderer tolerant of a stats object that predates the
  // field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_topic_rate_msg_per_sec Per-topic publish rate in messages per second over the trailing window (load-average style); only the hottest topics by 1m rate.',
  );
  lines.push('# TYPE eventbus_topic_rate_msg_per_sec gauge');
  for (const h of stats.hotTopics ?? []) {
    const label = `topic="${escapeLabelValue(h.topic)}"`;
    lines.push(`eventbus_topic_rate_msg_per_sec{${label},window="1s"} ${h.r1s}`);
    lines.push(`eventbus_topic_rate_msg_per_sec{${label},window="1m"} ${h.r1m}`);
    lines.push(`eventbus_topic_rate_msg_per_sec{${label},window="5m"} ${h.r5m}`);
  }

  // Consumer-group consumption lag (EB-48), one series per (group, topic)
  // row plus one per (group, partition, topic) row for partitioned
  // groups. The `?? []` keeps the renderer tolerant of a stats object
  // that predates the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_group_lag_messages Consumer-group consumption lag: highest assigned per-topic seq minus the consumer checkpoint, excluding backlog held by live handoff-linger windows.',
  );
  lines.push('# TYPE eventbus_group_lag_messages gauge');
  lines.push(
    '# HELP eventbus_group_partition_lag_messages Per-partition consumer-group consumption lag (see eventbus_group_lag_messages).',
  );
  lines.push('# TYPE eventbus_group_partition_lag_messages gauge');
  for (const g of stats.groupLag ?? []) {
    const label = `group="${escapeLabelValue(g.groupId)}",topic="${escapeLabelValue(g.topic)}"`;
    if (g.partition === undefined) {
      lines.push(`eventbus_group_lag_messages{${label}} ${g.lag}`);
    } else {
      lines.push(`eventbus_group_partition_lag_messages{${label},partition="${g.partition}"} ${g.lag}`);
    }
  }

  // Cross-process bridge counters (EB-51). The `??` keeps the renderer
  // tolerant of a stats object that predates the field (hand-built
  // fixtures included).
  const bridge = stats.bridge ?? { inbound: 0, outbound: 0, dropped: 0 };
  counter(
    'eventbus_bridge_outbound_total',
    'Total local admitted publishes mirrored to the bridge transport.',
    bridge.outbound,
  );
  counter(
    'eventbus_bridge_inbound_total',
    'Total bridge envelopes admitted and fanned out locally.',
    bridge.inbound,
  );
  counter(
    'eventbus_bridge_dropped_total',
    'Total inbound bridge envelopes shed on a full ingress buffer.',
    bridge.dropped,
  );

  // Mirror active-active write conflicts (EB-131): per-topic stale-write
  // drops from per-key last-writer-wins arbitration on the mirror
  // receive side. Only topics with at least one conflict carry a series,
  // so cardinality tracks conflict activity, not the topic space. The
  // `?? []` keeps the renderer tolerant of a stats object that predates
  // the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_mirror_conflicts_total Mirror writes discarded as stale by per-key last-writer-wins arbitration on the mirror receive side.',
  );
  lines.push('# TYPE eventbus_mirror_conflicts_total counter');
  for (const row of stats.mirrorConflictsByTopic ?? []) {
    const label = `topic="${escapeLabelValue(row.topic)}"`;
    lines.push(`eventbus_mirror_conflicts_total{${label}} ${row.conflicts}`);
  }

  // Mirror traffic throttling (EB-172): per-rule shed frames from the
  // mirror receive path. Only rules with at least one shed carry a
  // series, so cardinality tracks throttle activity, not the rule
  // space. The `?? 0` keeps the renderer tolerant of a stats object
  // that predates the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_mirror_throttled_total Mirror frames shed by per-rule traffic throttling on the mirror receive path.',
  );
  lines.push('# TYPE eventbus_mirror_throttled_total counter');
  for (const row of stats.mirrors ?? []) {
    const throttled = row.mirroredThrottled ?? 0;
    if (throttled === 0) continue;
    const label =
      `src_pattern="${escapeLabelValue(row.srcPattern)}",` +
      `dst_template="${escapeLabelValue(row.dstTemplate)}"`;
    lines.push(`eventbus_mirror_throttled_total{${label}} ${throttled}`);
  }

  // Transactional outbox relay lag (EB-102). Always rendered — zeros when
  // no relay is attached — so the alert rule `eventbus_outbox_lag_ms > X`
  // exists from the first scrape. The `?? 0` keeps the renderer tolerant
  // of a stats object that predates the field (hand-built fixtures
  // included).
  gauge(
    'eventbus_outbox_pending',
    'Outbox records sitting in the attached outbox store, not yet relayed (seq > cursor).',
    stats.outboxPending ?? 0,
  );
  gauge(
    'eventbus_outbox_lag_ms',
    'Age of the oldest unrelayed outbox record in ms; a climbing lag with a stuck cursor means the relay is wedged on a poison record.',
    stats.outboxLagMs ?? 0,
  );
  counter(
    'eventbus_outbox_relayed_total',
    'Total outbox records committed via publishAtomic since the relay was attached.',
    stats.outboxRelayed ?? 0,
  );

  // Namespace tenant quotas (EB-162): admitted payload bytes inside each
  // rule's sliding window, one series per registered (namespace,
  // topic-pattern) rule — the multi-tenant capacity signal. The
  // `?? []` keeps the renderer tolerant of a stats object that predates
  // the field (hand-built fixtures included).
  lines.push(
    '# HELP eventbus_tenant_quota_bytes_total Admitted payload bytes inside the sliding window per namespace tenant-quota rule (namespace, pattern).',
  );
  lines.push('# TYPE eventbus_tenant_quota_bytes_total gauge');
  for (const q of stats.namespaceQuotas ?? []) {
    const label = `namespace="${escapeLabelValue(q.namespace)}",pattern="${escapeLabelValue(q.pattern)}"`;
    lines.push(`eventbus_tenant_quota_bytes_total{${label}} ${q.usedBytes}`);
  }

  // CEP sequence detection (EB-170): pattern/partial/match counters.
  // Rendered only when a pattern was defined — a CEP-free bus renders
  // exactly what it always did.
  if (stats.cep !== undefined) {
    gauge(
      'eventbus_cep_patterns',
      'Sequence patterns currently defined (EB-170).',
      stats.cep.activePatterns,
    );
    gauge(
      'eventbus_cep_partial_matches',
      'Partial sequence matches currently tracked (EB-170).',
      stats.cep.partialMatches,
    );
    counter(
      'eventbus_cep_matches_total',
      'Sequence matches completed and emitted (EB-170).',
      stats.cep.matches,
    );
    gauge(
      'eventbus_cep_absence_patterns',
      'Sequence patterns with at least one absence step (EB-174).',
      stats.cep.absencePatterns,
    );
    counter(
      'eventbus_cep_absence_matches_total',
      'Absence steps satisfied — deadline passed with no matching event for the key (EB-174).',
      stats.cep.absenceMatches,
    );
  }

  // Materialized state store (EB-171): key counts. Rendered only when the
  // store is enabled — a store-free bus renders exactly what it always
  // did.
  if (stats.stateStore !== undefined) {
    gauge(
      'eventbus_statestore_keys',
      'Keys currently tracked by the materialized state store (EB-171).',
      stats.stateStore.keys,
    );
    counter(
      'eventbus_statestore_evicted_keys_total',
      'Keys evicted by the state-store maxKeys bound (EB-171).',
      stats.stateStore.evictedKeys,
    );
  }

  return lines.join('\n') + '\n';
}
