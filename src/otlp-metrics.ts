/**
 * OTLP/HTTP JSON metrics exporter (EB-370).
 *
 * Pushes bus-level `BusStats` snapshots to an OpenTelemetry collector
 * over OTLP/HTTP (`POST <endpoint>`, JSON encoding, `/v1/metrics`) —
 * the push complement to the pull-mode Prometheus exposition in
 * `src/metrics.ts` (`renderPrometheus`) and to the span exporter in
 * `src/otlp.ts` (EB-105), which only ships traces. A deployment that
 * runs an OTLP collector and never scrapes Prometheus gets the bus's
 * counters, gauges, latency distributions, and per-topic payload
 * histograms from here instead of having to poll `getStats()` by hand.
 * Hand-written OTLP mapping, zero new dependencies — no OTel SDK.
 *
 * Wire-up: `EventBusOptions.otlpMetrics` (opt-in). The bus passes a
 * snapshot supplier (`() => this.getStats()`), so every tick exports
 * the latest snapshot.
 *
 * Delivery semantics are best-effort, mirroring the span exporter
 * (otlp.ts): a failed POST drops that snapshot and counts it in
 * `failedExports` — the exporter does not retry, and a down collector
 * surfaces as a rising `failedExports` counter in
 * `getStats().otlpMetrics`, not as backpressure on delivery. Metrics
 * are snapshots, not an event stream, so there is deliberately NO
 * buffering: each tick exports only the newest snapshot and an old
 * snapshot has no retained value — hence no `batchSize`, no
 * `maxBuffered`, and no `sampleRate` (the knobs the span exporter
 * needs for its buffered event stream do not apply here). Export never
 * throws and never blocks the publish/deliver path.
 *
 * OTLP mapping (metric names reuse the Prometheus names from
 * `renderPrometheus` — same name, only the type changes — so a
 * collector can align the push and scrape series directly):
 *
 * - Counters → Sum (Cumulative, Monotonic=true): the `*_total`
 *   counters (`eventbus_published_messages_total`,
 *   `eventbus_delivered_messages_total`, …) plus the latency sample
 *   counts (`eventbus_delivery_latency_samples`,
 *   `eventbus_publish_admission_latency_samples`,
 *   `eventbus_ack_latency_samples`).
 * - Gauges → Gauge: the instant water levels
 *   (`eventbus_degraded_subscribers`, `eventbus_circuit_breakers_open`,
 *   `eventbus_credit_exhausted_subscribers`,
 *   `eventbus_causal_buffer_depth`, …) and the latency distributions
 *   (`eventbus_delivery_latency_ms`,
 *   `eventbus_publish_admission_latency_ms`,
 *   `eventbus_ack_latency_ms` — one data point per quantile
 *   `0.5`/`0.95`/`0.99`, in milliseconds). The latency trackers keep
 *   only tdigest-style summaries (p50/p95/p99, no bucket counts), so
 *   they map honestly to Gauges, never to an OTLP Histogram.
 * - Per-topic payload histograms (EB-143) → OTLP Histogram: the
 *   Prometheus cumulative buckets (`metrics.ts`) converted to
 *   per-bucket differential counts, `explicit_bounds` the configured
 *   bounds plus `+Inf` (protobuf-JSON `"+Inf"`), `count`/`sum` from the
 *   same source.
 * - Per-topic counters (`eventbus_published_messages_total` with a
 *   `topic` attribute) cover only the hottest topics — hottest first by
 *   1m publish rate (`getStats().hotTopics`), at most `maxTopics`
 *   (default 20); `perTopic: false` emits bus-level aggregates only.
 *   `getStats().hotTopics` itself caps at `HOT_TOPICS_LIMIT` (10), so
 *   the effective per-topic ceiling is `min(maxTopics, 10)` today.
 *   This bounds collector cardinality: a bus with thousands of topics
 *   never fans out thousands of series. Per-topic delivered/dropped
 *   counts are NOT exported: `TopicStats` carries no per-topic
 *   delivered/dropped counters (bus-level only), so the exporter
 *   reports only what `BusStats` actually measures rather than
 *   inventing per-topic splits.
 *
 * Resource attributes carry only `service.name` — the span exporter
 * does the same (otlp.ts), keeping the export identity consistent
 * across signals. Sum `startTimeUnixNano` is the exporter construction
 * time: an approximation of the bus start, documented as such, since
 * `getStats()` counters accumulate over the process lifetime.
 */

import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { Buffer } from 'node:buffer';
import type { BusStats } from './bus.ts';

/** Tuning for opt-in OTLP metrics export (`EventBusOptions.otlpMetrics`). */
export interface OtlpMetricsExporterOptions {
  /**
   * OTLP/HTTP metrics endpoint, e.g.
   * `http://collector:4318/v1/metrics`. Required. Must be an
   * `http:`/`https:` URL.
   */
  endpoint: string;
  /**
   * `service.name` resource attribute. Must be a non-empty string.
   * Default `'realtime-event-bus'`.
   */
  serviceName?: string;
  /**
   * Periodic export interval in milliseconds. Must be a finite number
   * > 0. Default 5000. The timer is unref'd and never holds the
   * process open; call `flush()`/`shutdown()` for a deterministic
   * final export.
   */
  flushIntervalMs?: number;
  /**
   * Per-request timeout in milliseconds. Must be a finite number > 0.
   * Default 5000.
   */
  timeoutMs?: number;
  /**
   * Extra HTTP headers for the export POST (e.g. `Authorization`).
   * Header names/values must be strings. Default `{}`.
   */
  headers?: Record<string, string>;
  /**
   * Per-topic series: `false` disables every per-topic series (bus
   * aggregates only); `true` (or omitted) enables them for the
   * hottest topics, at most `maxTopics` (default 20). `maxTopics`
   * must be a positive integer. Default `{ maxTopics: 20 }`.
   */
  perTopic?: boolean | { maxTopics?: number };
}

/** Normalized OTLP metrics exporter options; `undefined` means disabled. */
export interface ResolvedOtlpMetricsExporterOptions {
  endpoint: URL;
  serviceName: string;
  flushIntervalMs: number;
  timeoutMs: number;
  headers: Record<string, string>;
  perTopic: { enabled: boolean; maxTopics: number };
}

/** Exporter counters, surfaced as `getStats().otlpMetrics`. */
export interface OtlpMetricsExportStats {
  /** The configured endpoint, as a string. */
  endpoint: string;
  /** Metric data points successfully POSTed to the collector. */
  exportedMetrics: number;
  /** Export POSTs that failed (best-effort: the snapshot was dropped). */
  failedExports: number;
  /** Epoch milliseconds of the last successful export; 0 when never exported. */
  lastExportAtMs: number;
  /** Live export cadence in milliseconds (revisable via `setOtlpMetrics`, EB-374). */
  flushIntervalMs: number;
  /** Live per-request timeout in milliseconds (revisable via `setOtlpMetrics`, EB-374). */
  timeoutMs: number;
  /** Live per-topic series tuning (revisable via `setOtlpMetrics`, EB-374). */
  perTopic: { enabled: boolean; maxTopics: number };
}

/**
 * Revision for the bus-level OTLP metrics export tuning (see
 * `EventBus.setOtlpMetrics`, EB-374). Only the fields provided move;
 * the rest keep their live values. `endpoint`, `headers`, and
 * `serviceName` are not revisable here — they are the export
 * destination and identity fixed at construction. `perTopic: false`
 * disables every per-topic series (bus aggregates only); `true`
 * re-enables them at the live `maxTopics`; `{ maxTopics }` keeps
 * per-topic series enabled and moves the cap.
 */
export interface OtlpMetricsRevision {
  /**
   * Periodic export interval in milliseconds. Must be a finite number
   * > 0 when provided — the construction-time rule. Revising it
   * rebuilds the flush timer at the new interval without rebuilding
   * the exporter: counters and the Sum start time are retained, and
   * the new cadence governs from the next tick.
   */
  flushIntervalMs?: number;
  /**
   * Per-request timeout in milliseconds. Must be a finite number > 0
   * when provided — the construction-time rule. The next export POST
   * times out at the live value.
   */
  timeoutMs?: number;
  /**
   * Per-topic series: `false` disables them, `true` (re-)enables
   * them at the live `maxTopics`, `{ maxTopics }` moves the cap
   * (a positive integer — the construction-time rule). The next
   * export covers the hottest topics under the live cap.
   */
  perTopic?: boolean | { maxTopics?: number };
}

const DEFAULT_SERVICE_NAME = 'realtime-event-bus';
const DEFAULT_FLUSH_INTERVAL_MS = 5000;
const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_MAX_TOPICS = 20;

/**
 * Normalizes `EventBusOptions.otlpMetrics`. Returns `undefined` when
 * export is disabled (absent, `null`, `false`); anything else invalid
 * throws `RangeError` at construction time.
 */
export function resolveOtlpMetricsOptions(
  raw: OtlpMetricsExporterOptions | false | null | undefined,
  where = 'EventBus',
): ResolvedOtlpMetricsExporterOptions | undefined {
  if (raw == null || raw === false) return undefined;
  if (typeof raw !== 'object') {
    throw new RangeError(`${where}: otlpMetrics must be an options object or false`);
  }
  if (typeof raw.endpoint !== 'string' || raw.endpoint.length === 0) {
    throw new RangeError(`${where}: otlpMetrics.endpoint must be a non-empty URL string`);
  }
  let endpoint: URL;
  try {
    endpoint = new URL(raw.endpoint);
  } catch {
    throw new RangeError(`${where}: otlpMetrics.endpoint must be a valid URL`);
  }
  if (endpoint.protocol !== 'http:' && endpoint.protocol !== 'https:') {
    throw new RangeError(`${where}: otlpMetrics.endpoint must be an http(s) URL`);
  }
  const serviceName = raw.serviceName ?? DEFAULT_SERVICE_NAME;
  if (typeof serviceName !== 'string' || serviceName.length === 0) {
    throw new RangeError(`${where}: otlpMetrics.serviceName must be a non-empty string`);
  }
  const flushIntervalMs = raw.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
  if (!Number.isFinite(flushIntervalMs) || flushIntervalMs <= 0) {
    throw new RangeError(`${where}: otlpMetrics.flushIntervalMs must be a finite number > 0`);
  }
  const timeoutMs = raw.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new RangeError(`${where}: otlpMetrics.timeoutMs must be a finite number > 0`);
  }
  const headers = raw.headers ?? {};
  if (typeof headers !== 'object' || headers === null || Array.isArray(headers)) {
    throw new RangeError(`${where}: otlpMetrics.headers must be a string-valued object`);
  }
  for (const [k, v] of Object.entries(headers)) {
    if (typeof k !== 'string' || typeof v !== 'string') {
      throw new RangeError(`${where}: otlpMetrics.headers must be a string-valued object`);
    }
  }
  let perTopic: { enabled: boolean; maxTopics: number };
  const rawPerTopic = raw.perTopic;
  if (rawPerTopic === undefined || rawPerTopic === true) {
    perTopic = { enabled: true, maxTopics: DEFAULT_MAX_TOPICS };
  } else if (rawPerTopic === false) {
    perTopic = { enabled: false, maxTopics: DEFAULT_MAX_TOPICS };
  } else if (typeof rawPerTopic === 'object' && rawPerTopic !== null && !Array.isArray(rawPerTopic)) {
    const maxTopics = rawPerTopic.maxTopics ?? DEFAULT_MAX_TOPICS;
    if (!Number.isInteger(maxTopics) || maxTopics < 1) {
      throw new RangeError(`${where}: otlpMetrics.perTopic.maxTopics must be a positive integer`);
    }
    perTopic = { enabled: true, maxTopics };
  } else {
    throw new RangeError(`${where}: otlpMetrics.perTopic must be a boolean or { maxTopics }`);
  }
  return {
    endpoint,
    serviceName,
    flushIntervalMs,
    timeoutMs,
    headers: { ...headers },
    perTopic,
  };
}

/** Milliseconds since the epoch → OTLP `timeUnixNano` (decimal string). */
function toUnixNano(ms: number): string {
  const safe = Number.isFinite(ms) ? Math.max(0, Math.round(ms)) : 0;
  return (BigInt(safe) * 1_000_000n).toString();
}

type OtlpAttribute = { key: string; value: { stringValue: string } };

function attr(key: string, value: string): OtlpAttribute {
  return { key, value: { stringValue: value } };
}

interface SumDataPoint {
  attributes: OtlpAttribute[];
  startTimeUnixNano: string;
  timeUnixNano: string;
  asInt: string;
}

interface GaugeDataPoint {
  attributes: OtlpAttribute[];
  timeUnixNano: string;
  asDouble: number;
}

interface HistogramDataPoint {
  attributes: OtlpAttribute[];
  startTimeUnixNano: string;
  timeUnixNano: string;
  count: string;
  sum: number;
  bucketCounts: string[];
  explicitBounds: Array<number | string>;
}

type OtlpMetric =
  | { name: string; sum: { dataPoints: SumDataPoint[]; aggregationTemporality: 2; isMonotonic: true } }
  | { name: string; gauge: { dataPoints: GaugeDataPoint[] } }
  | {
      name: string;
      histogram: { dataPoints: HistogramDataPoint[]; aggregationTemporality: 2 };
    };

function sumMetric(name: string, points: SumDataPoint[]): OtlpMetric {
  return {
    name,
    sum: { dataPoints: points, aggregationTemporality: 2, isMonotonic: true },
  };
}

function gaugeMetric(name: string, points: GaugeDataPoint[]): OtlpMetric {
  return { name, gauge: { dataPoints: points } };
}

function histogramMetric(name: string, points: HistogramDataPoint[]): OtlpMetric {
  return {
    name,
    histogram: { dataPoints: points, aggregationTemporality: 2 },
  };
}

/**
 * One `BusStats` snapshot → the OTLP/HTTP `MetricsData` JSON body.
 * Returns the body plus how many data points it carries (for the
 * `exportedMetrics` counter). Pure function of the snapshot, so the
 * mapping is testable without a bus.
 */
export function encodeMetrics(
  serviceName: string,
  stats: BusStats,
  perTopic: { enabled: boolean; maxTopics: number },
  startTimeUnixNano: string,
  nowMs: number,
): { body: string; dataPoints: number } {
  const timeUnixNano = toUnixNano(nowMs);
  const metrics: OtlpMetric[] = [];
  let dataPoints = 0;

  const sum = (name: string, value: number, attributes: OtlpAttribute[] = []): void => {
    metrics.push(
      sumMetric(name, [
        { attributes, startTimeUnixNano, timeUnixNano, asInt: String(Math.trunc(value)) },
      ]),
    );
    dataPoints += 1;
  };
  const gauge = (name: string, value: number, attributes: OtlpAttribute[] = []): void => {
    metrics.push(gaugeMetric(name, [{ attributes, timeUnixNano, asDouble: value }]));
    dataPoints += 1;
  };

  // Bus-level counters → Sum (Cumulative, Monotonic). Names reuse the
  // Prometheus series from `renderPrometheus` (metrics.ts); values
  // read the same `BusStats` fields.
  const counters: Array<[string, number]> = [
    // `eventbus_published_messages_total` is emitted below with its
    // per-topic points in the same Sum (see the per-topic section).
    ['eventbus_delivered_messages_total', stats.deliveredMessages],
    ['eventbus_dropped_messages_total', stats.droppedMessages],
    ['eventbus_dead_lettered_messages_total', stats.deadLetteredMessages],
    ['eventbus_expired_messages_total', stats.expiredMessages],
    ['eventbus_rejected_messages_total', stats.rejectedMessages],
    ['eventbus_filtered_messages_total', stats.filteredMessages],
    ['eventbus_sampled_messages_total', stats.sampledMessages],
    ['eventbus_duplicate_messages_total', stats.duplicateMessages],
    ['eventbus_dedup_dropped_messages_total', stats.dedupDropped],
    ['eventbus_gap_repaired_messages_total', stats.gapRepaired],
    ['eventbus_shadowed_messages_total', stats.shadowedMessages ?? 0],
    ['eventbus_shadow_dropped_messages_total', stats.shadowDroppedMessages ?? 0],
    ['eventbus_missed_deadline_messages_total', stats.missedDeadlineMessages],
    ['eventbus_throttled_messages_total', stats.throttledMessages],
    ['eventbus_rate_limited_messages_total', stats.rateLimitedMessages],
    ['eventbus_publisher_rate_limited_messages_total', stats.publisherRateLimited],
    ['eventbus_key_rate_limited_messages_total', stats.keyRateLimited ?? 0],
    ['eventbus_global_rate_limited_messages_total', stats.globalRateLimitedMessages ?? 0],
    ['eventbus_paused_messages_total', stats.pausedMessages],
    ['eventbus_circuit_broken_messages_total', stats.circuitBrokenMessages],
    ['eventbus_circuit_breaker_trips_total', stats.circuitBreakerTrips],
    ['eventbus_cas_mismatch_total', stats.casMismatches],
    ['eventbus_header_filtered_messages_total', stats.headerFilteredMessages ?? 0],
    ['eventbus_subscriber_oversize_dropped_total', stats.subscriberOversizeDropped ?? 0],
    ['eventbus_signature_failures_total', stats.signatureFailures ?? 0],
    ['eventbus_schema_migration_failures_total', stats.migrationFailures ?? 0],
    ['eventbus_stream_join_matches_total', stats.totalStreamJoinMatches ?? 0],
    ['eventbus_stream_join_late_total', stats.totalStreamJoinLate ?? 0],
    ['eventbus_stream_join_evicted_total', stats.totalStreamJoinEvicted ?? 0],
    ['eventbus_sequence_gaps_total', stats.sequenceGaps],
    ['eventbus_diagnostic_events_total', stats.diagnosticEvents ?? 0],
    ['eventbus_admission_dlq_events_total', stats.admissionDlqEvents ?? 0],
    ['eventbus_deprecated_schema_messages_total', stats.deprecatedSchemaVersions ?? 0],
    ['eventbus_unregistered_topic_publishes_total', stats.unregisteredTopicPublishes ?? 0],
    ['eventbus_late_messages_total', stats.lateMessages ?? 0],
    ['eventbus_expiring_soon_alerts_total', stats.expiringSoonAlerts ?? 0],
    ['eventbus_causal_buffer_dropped_messages_total', stats.causalBuffer?.droppedMessages ?? 0],
    ['eventbus_causal_buffer_regressed_messages_total', stats.causalBuffer?.regressedMessages ?? 0],
  ];
  for (const [name, value] of counters) sum(name, value);

  // Instant water levels → Gauge.
  gauge('eventbus_subscribers', stats.totalSubscribers);
  gauge('eventbus_retained_topics', stats.retainedTopics?.length ?? 0);
  gauge('eventbus_unacked_deliveries', stats.unackedDeliveries);
  gauge('eventbus_throttled_subscribers', stats.throttledSubscribers);
  gauge('eventbus_degraded_subscribers', stats.degradedSubscribers);
  gauge('eventbus_slow_handler_last_ms', stats.slowHandlers?.lastMs ?? 0);
  gauge('eventbus_paused_subscribers', stats.pausedSubscribers ?? 0);
  gauge('eventbus_shaped_subscribers', stats.shapedSubscribers);
  gauge('eventbus_credit_exhausted_subscribers', stats.creditExhaustedSubscribers ?? 0);
  gauge('eventbus_pending_delayed', stats.pendingDelayed);
  gauge('eventbus_paused_topics', stats.pausedTopics?.length ?? 0);
  gauge(
    'eventbus_circuit_breakers_open',
    (stats.circuitBreakers ?? []).filter((row) => row.state === 'open').length,
  );
  gauge('eventbus_queue_bytes', stats.queueBytes ?? 0);
  gauge('eventbus_causal_buffer_depth', stats.causalBuffer?.depth ?? 0);

  // Latency distributions → Gauge (one data point per quantile, ms)
  // with the window sample count as a Sum.
  const latencyPoints = (
    rows: Array<{ p50Ms: number; p95Ms: number; p99Ms: number; samples: number }>,
    labels: (row: (typeof rows)[number]) => OtlpAttribute[],
    gaugeName: string,
    samplesName: string,
  ): void => {
    const gaugePoints: GaugeDataPoint[] = [];
    const samplePoints: SumDataPoint[] = [];
    for (const row of rows) {
      const base = labels(row);
      gaugePoints.push(
        { attributes: [...base, attr('quantile', '0.5')], timeUnixNano, asDouble: row.p50Ms },
        { attributes: [...base, attr('quantile', '0.95')], timeUnixNano, asDouble: row.p95Ms },
        { attributes: [...base, attr('quantile', '0.99')], timeUnixNano, asDouble: row.p99Ms },
      );
      samplePoints.push({
        attributes: base,
        startTimeUnixNano,
        timeUnixNano,
        asInt: String(row.samples),
      });
    }
    if (gaugePoints.length > 0) {
      metrics.push(gaugeMetric(gaugeName, gaugePoints));
      dataPoints += gaugePoints.length;
    }
    if (samplePoints.length > 0) {
      metrics.push(sumMetric(samplesName, samplePoints));
      dataPoints += samplePoints.length;
    }
  };
  latencyPoints(
    stats.deliveryLatency ?? [],
    (row) => [attr('subscriber', row.subscriberId), attr('pattern', row.pattern)],
    'eventbus_delivery_latency_ms',
    'eventbus_delivery_latency_samples',
  );
  latencyPoints(
    stats.publishLatency ?? [],
    (row) => [attr('topic', row.topic)],
    'eventbus_publish_admission_latency_ms',
    'eventbus_publish_admission_latency_samples',
  );
  latencyPoints(
    stats.ackLatency ?? [],
    (row) => [attr('subscriber', row.subscriberId), attr('pattern', row.pattern)],
    'eventbus_ack_latency_ms',
    'eventbus_ack_latency_samples',
  );

  // Per-topic series: hottest topics only, bounded by maxTopics.
  // `TopicStats` carries no per-topic delivered/dropped counters, so
  // the per-topic counter is published only — see the module header.
  // The per-topic points ride the SAME `eventbus_published_messages_total`
  // Sum as the bus-level point (one metric, several label sets — the
  // Prometheus convention `renderPrometheus` uses for the same name),
  // never a second same-named metric.
  const publishedPoints: SumDataPoint[] = [
    {
      attributes: [],
      startTimeUnixNano,
      timeUnixNano,
      asInt: String(Math.trunc(stats.totalPublished)),
    },
  ];
  const histogramPoints: HistogramDataPoint[] = [];
  if (perTopic.enabled) {
    const byTopic = new Map(stats.topics.map((t) => [t.topic, t]));
    const hottest = (stats.hotTopics ?? []).slice(0, perTopic.maxTopics);
    for (const hot of hottest) {
      const topicStats = byTopic.get(hot.topic);
      if (topicStats === undefined) continue;
      publishedPoints.push({
        attributes: [attr('topic', hot.topic)],
        startTimeUnixNano,
        timeUnixNano,
        asInt: String(topicStats.publishedMessages),
      });
      const h = topicStats.payloadHistogram;
      if (h !== undefined) histogramPoints.push(histogramPoint(h, [attr('topic', hot.topic)], startTimeUnixNano, timeUnixNano));
    }
  }
  metrics.push(sumMetric('eventbus_published_messages_total', publishedPoints));
  dataPoints += publishedPoints.length;

  // Bus-wide payload-size histogram aggregate (EB-143): the same
  // shape as the per-topic snapshots, accumulated across every
  // sampled topic — one more data point of the same
  // `eventbus_payload_bytes` Histogram, no `topic` attribute.
  const busHistogram = stats.payloadHistogram;
  if (busHistogram !== undefined) {
    histogramPoints.push(histogramPoint(busHistogram, [], startTimeUnixNano, timeUnixNano));
  }
  if (histogramPoints.length > 0) {
    metrics.push(histogramMetric('eventbus_payload_bytes', histogramPoints));
    dataPoints += histogramPoints.length;
  }

  const body = JSON.stringify({
    resourceMetrics: [
      {
        resource: {
          attributes: [{ key: 'service.name', value: { stringValue: serviceName } }],
        },
        scopeMetrics: [
          {
            scope: { name: 'realtime-event-bus' },
            metrics,
          },
        ],
      },
    ],
  });
  return { body, dataPoints };
}

/**
 * One `PayloadSizeHistogram` snapshot → one OTLP Histogram data point.
 * The source buckets are cumulative (Prometheus convention); OTLP
 * `bucket_counts` are per-bucket differentials, so each cumulative
 * count is differenced against its predecessor. `explicit_bounds`
 * carries the configured bounds plus `+Inf` (protobuf-JSON `"+Inf"`,
 * the last bucket); `bucket_counts` therefore has one more entry
 * than the configured bounds.
 */
function histogramPoint(
  h: { buckets: Array<{ le: number; count: number }>; count: number; sum: number },
  attributes: OtlpAttribute[],
  startTimeUnixNano: string,
  timeUnixNano: string,
): HistogramDataPoint {
  const explicitBounds: Array<number | string> = h.buckets.map((b) => b.le);
  explicitBounds.push('+Inf');
  const bucketCounts: string[] = [];
  let previous = 0;
  for (const b of h.buckets) {
    bucketCounts.push(String(Math.max(0, b.count - previous)));
    previous = b.count;
  }
  bucketCounts.push(String(Math.max(0, h.count - previous)));
  return {
    attributes,
    startTimeUnixNano,
    timeUnixNano,
    count: String(h.count),
    sum: h.sum,
    bucketCounts,
    explicitBounds,
  };
}

/**
 * Best-effort OTLP/HTTP metrics exporter. Constructed once per bus
 * when `EventBusOptions.otlpMetrics` is set; the bus supplies the
 * snapshot via `collect`.
 */
export class OtlpMetricsExporter {
  private readonly endpoint: URL;
  private readonly serviceName: string;
  private flushIntervalMs: number;
  private timeoutMs: number;
  private readonly headers: Record<string, string>;
  private perTopic: { enabled: boolean; maxTopics: number };
  private readonly collect: () => BusStats;
  private readonly startTimeUnixNano: string;
  private timer: ReturnType<typeof setInterval>;
  private stopped = false;
  private flushPromise: Promise<void> | undefined;
  private exportedMetrics = 0;
  private failedExports = 0;
  private lastExportAtMs = 0;

  constructor(resolved: ResolvedOtlpMetricsExporterOptions, collect: () => BusStats) {
    this.endpoint = resolved.endpoint;
    this.serviceName = resolved.serviceName;
    this.flushIntervalMs = resolved.flushIntervalMs;
    this.timeoutMs = resolved.timeoutMs;
    this.headers = resolved.headers;
    this.perTopic = resolved.perTopic;
    this.collect = collect;
    // Sum start time: the exporter construction time, an approximation
    // of the bus start (counters accumulate over the process lifetime).
    this.startTimeUnixNano = toUnixNano(Date.now());
    this.timer = this.armFlushTimer(resolved.flushIntervalMs);
  }

  /**
   * Arms the periodic export timer at `intervalMs`. Unref'd: export
   * must never hold the process open on its own. Every arm goes
   * through here so a retune (EB-374) rebuilds the same timer the
   * constructor armed.
   */
  private armFlushTimer(intervalMs: number): ReturnType<typeof setInterval> {
    const timer = setInterval(() => {
      void this.flush();
    }, intervalMs);
    if (typeof timer.unref === 'function') timer.unref();
    return timer;
  }

  /** The live export tuning, for bus-level revision (EB-374). */
  current(): ResolvedOtlpMetricsExporterOptions {
    return {
      endpoint: this.endpoint,
      serviceName: this.serviceName,
      flushIntervalMs: this.flushIntervalMs,
      timeoutMs: this.timeoutMs,
      headers: { ...this.headers },
      perTopic: { ...this.perTopic },
    };
  }

  /**
   * Retunes the three live export bounds (EB-374). The caller must
   * have validated `next` through `resolveOtlpMetricsOptions` first —
   * this method applies unconditionally. The counters
   * (`exportedMetrics`/`failedExports`/`lastExportAtMs`) and the Sum
   * `startTimeUnixNano` are retained — a retune never restarts the
   * cumulative series, so the collector-side rate computation is
   * uninterrupted — and an in-flight flush is untouched: the next
   * export POST times out at the live `timeoutMs`, the next export
   * covers the hottest topics under the live `perTopic` cap, and a
   * revised `flushIntervalMs` rebuilds the flush timer at the new
   * interval (the old timer is cleared first, so a tick never fires
   * twice and no tick is lost — the new cadence governs from the
   * next tick). `endpoint`, `headers`, and `serviceName` are never
   * moved here. A retune after `shutdown()` moves the values but
   * does not re-arm the timer — a stopped exporter stays stopped.
   */
  retune(next: ResolvedOtlpMetricsExporterOptions): void {
    this.timeoutMs = next.timeoutMs;
    this.perTopic = { ...next.perTopic };
    if (next.flushIntervalMs !== this.flushIntervalMs) {
      this.flushIntervalMs = next.flushIntervalMs;
      if (!this.stopped) {
        clearInterval(this.timer);
        this.timer = this.armFlushTimer(next.flushIntervalMs);
      }
    }
  }

  /**
   * Exports the latest snapshot to the collector now instead of
   * waiting for the timer. Concurrent callers share one in-flight
   * export. Never throws: a failed POST drops that snapshot and
   * counts it in `failedExports`. A no-op after `shutdown()`.
   */
  flush(): Promise<void> {
    if (this.stopped) return Promise.resolve();
    if (this.flushPromise !== undefined) return this.flushPromise;
    const run = this.flushInner().catch(() => {
      // Defensive: flushInner already counts the failure, so this
      // only guards against an unexpected internal error.
    });
    this.flushPromise = run.finally(() => {
      this.flushPromise = undefined;
    });
    return this.flushPromise;
  }

  private async flushInner(): Promise<void> {
    let stats: BusStats;
    try {
      stats = this.collect();
    } catch {
      // Best-effort: a failing snapshot supplier must not throw into
      // the timer or the caller — count it as a failed export.
      this.failedExports += 1;
      return;
    }
    const { body, dataPoints } = encodeMetrics(
      this.serviceName,
      stats,
      this.perTopic,
      this.startTimeUnixNano,
      Date.now(),
    );
    try {
      await this.postMetrics(body);
      this.exportedMetrics += dataPoints;
      this.lastExportAtMs = Date.now();
    } catch {
      // Best-effort: drop the failed snapshot, count it, move on —
      // a down collector must not wedge the exporter or the bus.
      this.failedExports += 1;
    }
  }

  /**
   * Stops the periodic timer. Posts nothing: metrics are snapshots,
   * not a buffered stream, so there is nothing to drain — call
   * `flush()` first for a final snapshot (the bus does exactly that
   * on `close()`). A stopped exporter never exports again; a second
   * `shutdown()` is a no-op.
   */
  shutdown(): void {
    if (this.stopped) return;
    this.stopped = true;
    clearInterval(this.timer);
  }

  /** Exporter counters. A snapshot — mutating it is safe. */
  getStats(): OtlpMetricsExportStats {
    return {
      endpoint: this.endpoint.toString(),
      exportedMetrics: this.exportedMetrics,
      failedExports: this.failedExports,
      lastExportAtMs: this.lastExportAtMs,
      flushIntervalMs: this.flushIntervalMs,
      timeoutMs: this.timeoutMs,
      perTopic: { ...this.perTopic },
    };
  }

  private postMetrics(body: string): Promise<void> {
    const useHttps = this.endpoint.protocol === 'https:';
    const doRequest = useHttps ? httpsRequest : httpRequest;
    return new Promise<void>((resolve, reject) => {
      const req = doRequest(
        this.endpoint,
        {
          method: 'POST',
          // No connection pooling: one export POST per flush keeps the
          // exporter free of the keep-alive lifecycle (and of the
          // close-before-pool-drain hang class in tests).
          agent: false,
          headers: {
            'content-type': 'application/json',
            'content-length': Buffer.byteLength(body),
            ...this.headers,
          },
        },
        (res) => {
          // Drain the body so the socket can close cleanly.
          res.resume();
          res.on('end', () => {
            const status = res.statusCode ?? 0;
            if (status >= 200 && status < 300) resolve();
            else reject(new Error(`OTLP metrics export failed with status ${status}`));
          });
        },
      );
      req.on('error', reject);
      req.setTimeout(this.timeoutMs, () => {
        req.destroy(new Error(`OTLP metrics export timed out after ${this.timeoutMs}ms`));
      });
      req.end(body);
    });
  }
}
