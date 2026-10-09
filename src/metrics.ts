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
 * - `eventbus_expired_messages_total`: messages discarded by TTL before
 *   delivery.
 * - `eventbus_throttled_messages_total`: messages shed at the publish
 *   side by adaptive throttling.
 * - `eventbus_rejected_messages_total`: publishes rejected by schema
 *   validation.
 * - `eventbus_rate_limited_messages_total`: messages shed by publish-side
 *   per-topic rate limiting.
 * - `eventbus_duplicate_messages_total`: idempotent publishes suppressed
 *   as duplicates.
 * - `eventbus_filtered_messages_total`: messages skipped by subscriber
 *   content filters.
 * - `eventbus_dead_lettered_messages_total`: reliable messages moved into
 *   subscriber dead-letter queues.
 * - `eventbus_sequence_gaps_total`: sequence numbers observed missing by
 *   subscribers (see `TopicStats.sequenceGaps` for the counting rules).
 * - `eventbus_keyed_reordered_messages_total`: keyed messages held in
 *   per-(subscriber, key) reorder buffers because an earlier keySeq had not
 *   been fanned out yet — the observable count of per-key publish-order
 *   enforcement (see `PublishOptions.key`).
 * - `eventbus_topic_published_messages_total{topic}`: publishes per topic.
 *
 * Gauges (point-in-time):
 * - `eventbus_subscribers`: currently active subscriptions.
 * - `eventbus_unacked_deliveries`: reliable deliveries handed out but not
 *   yet acked or nacked.
 * - `eventbus_throttled_subscribers`: subscriptions under adaptive
 *   publish-side throttling.
 * - `eventbus_degraded_subscribers`: subscriptions auto-paused by health
 *   probing.
 * - `eventbus_shaped_subscribers`: subscriptions held back by
 *   delivery-side rate shaping.
 * - `eventbus_pending_delayed`: delayed messages scheduled but not yet
 *   due.
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
 * - `eventbus_ack_latency_ms{quantile,subscriber,pattern}`: per-subscriber
 *   accepted→ack end-to-end latency distribution for reliable
 *   subscriptions with `ackLatency` enabled — nearest-rank p50/p95/p99
 *   over each subscriber's bounded rolling window (`quantile` is "0.5",
 *   "0.95" or "0.99"). The full round trip the producer's SLO depends
 *   on: queue dwell plus handler processing and consumer think time.
 *   Only ack-tracked subscribers appear.
 * - `eventbus_ack_latency_samples{subscriber,pattern}`: samples
 *   currently in each tracked subscriber's ack-latency window.
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
 * - `eventbus_topic_rate_msg_per_sec{topic,window}`: per-topic publish
 *   rate in messages per second over the trailing 1s / 1m / 5m windows
 *   (`window` is "1s", "1m" or "5m"), load-average style. Only the hot
 *   topics carry this series — the bus's top-N by 1m rate (see
 *   `BusStats.hotTopics`) — so at most `HOT_TOPICS_LIMIT * 3` series.
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
 * subscribers add no series. The
 * `eventbus_topic_rate_msg_per_sec` series are the exception to the
 * per-topic rule: they are deliberately limited to the hot-topics set
 * (10 topics x 3 windows = 30 series max), because rate decisions need
 * the busiest topics, not the full topic space.
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
    'Total publishes rejected by schema validation.',
    stats.rejectedMessages,
  );
  counter(
    'eventbus_rate_limited_messages_total',
    'Total messages shed by publish-side per-topic rate limiting.',
    stats.rateLimitedMessages,
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
    'eventbus_dead_lettered_messages_total',
    'Total reliable messages moved into subscriber dead-letter queues.',
    stats.deadLetteredMessages,
  );
  counter(
    'eventbus_sequence_gaps_total',
    'Total sequence numbers observed missing by subscribers.',
    stats.sequenceGaps,
  );
  counter(
    'eventbus_keyed_reordered_messages_total',
    'Total keyed messages held in per-(subscriber, key) reorder buffers because an earlier keySeq had not been fanned out yet.',
    stats.keyedReorderedMessages,
  );

  gauge('eventbus_subscribers', 'Currently active subscriptions.', stats.totalSubscribers);
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
    'eventbus_shaped_subscribers',
    'Subscriptions currently held back by delivery-side rate shaping.',
    stats.shapedSubscribers,
  );
  gauge(
    'eventbus_pending_delayed',
    'Delayed messages scheduled but not yet due.',
    stats.pendingDelayed,
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

  // Per-topic series, in first-publish order (same as BusStats.topics).
  lines.push(
    '# HELP eventbus_topic_published_messages_total Total messages published to the topic.',
  );
  lines.push('# TYPE eventbus_topic_published_messages_total counter');
  lines.push(
    '# HELP eventbus_topic_subscribers Subscribers matched by the most recent publish to the topic (fan-out width).',
  );
  lines.push('# TYPE eventbus_topic_subscribers gauge');
  for (const t of stats.topics) {
    const label = `topic="${escapeLabelValue(t.topic)}"`;
    lines.push(`eventbus_topic_published_messages_total{${label}} ${t.publishedMessages}`);
    lines.push(`eventbus_topic_subscribers{${label}} ${t.subscriberCount}`);
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

  return lines.join('\n') + '\n';
}
