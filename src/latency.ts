/**
 * Per-subscriber delivery-latency sampling (EB-31).
 *
 * Measures the queue dwell of each delivered message: from the moment a
 * message is enqueued into a subscriber's queue (see
 * `SubscribeOptions.deliveryLatency`) to the moment it is handed to that
 * subscriber's handler. The latency is a queueing metric — handler
 * processing time is excluded, so a high p99 means the subscriber is
 * starved behind its own backlog (slow consumer), not that its handler
 * is slow.
 *
 * Sampling is opt-in per subscriber. When enabled, the bus stamps every
 * accepted enqueue with the bus clock and records one sample per handler
 * hand-off. The clock is the bus's own injected clock (`EventBusOptions.now`
 * — the same injection point `TokenBucket` in `throttle.ts` uses), so tests
 * drive deterministic latencies by advancing a controllable clock; the
 * tracker itself takes no clock because it only ever sees already-computed
 * deltas.
 *
 * Semantics worth knowing:
 * - One sample per delivery: at-least-once redeliveries (nack / ack
 *   timeout / health requeue) sample again, because each delivery had its
 *   own queue dwell.
 * - Messages that never reach a handler — backpressure drops, TTL
 *   expiries, publish-side throttle sheds, content-filter skips — are
 *   never sampled. This is a *delivery* latency distribution, not a
 *   wait-to-die one.
 * - A clock that moved backwards between enqueue and delivery clamps the
 *   sample at 0.
 *
 * EB-53 reuses this tracker for handler *processing*-latency p99 SLO
 * alerting (`SubscribeOptions.latencySlo`): the bus computes
 * delivery→completion deltas from its injected clock and feeds them here,
 * exactly like the queue-dwell samples above. The tracker itself stays
 * delta-based and clock-free, so both features share one window
 * implementation with no duplication.
 */

/** Tuning for per-subscriber delivery-latency sampling. */
export interface DeliveryLatencyOptions {
  /**
   * How many of the most recent samples to keep per subscriber; the
   * oldest samples are evicted past this bound. Must be a positive
   * integer. Default 1024.
   */
  windowSize?: number;
}

/** A latency distribution summary over one subscriber's sample window. */
export interface DeliveryLatencySummaryStats {
  /** Samples currently in the window. */
  samples: number;
  /** Nearest-rank p50/p95/p99 of the window, in milliseconds. 0 when empty. */
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  /** Minimum / maximum / arithmetic mean of the window, in milliseconds. 0 when empty. */
  minMs: number;
  maxMs: number;
  meanMs: number;
}

/** Nearest-rank quantile of a sorted (ascending) sample array. */
function quantileRank(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.ceil(q * sorted.length) - 1;
  return sorted[Math.min(Math.max(rank, 0), sorted.length - 1)];
}

/**
 * Bounded rolling window of enqueue→delivery latency samples for one
 * subscriber. Samples are plain millisecond deltas; the bus computes them
 * from its injected clock.
 */
export class DeliveryLatencyTracker {
  private readonly samples: number[] = [];
  private readonly windowSize: number;
  private sumMs = 0;

  constructor(windowSize = 1024) {
    if (!Number.isInteger(windowSize) || windowSize <= 0) {
      throw new RangeError('windowSize must be a positive integer');
    }
    this.windowSize = windowSize;
  }

  /** Records one enqueue→delivery latency sample. Negative inputs (clock skew) clamp to 0. */
  record(latencyMs: number): void {
    const sample = Math.max(0, latencyMs);
    this.samples.push(sample);
    this.sumMs += sample;
    if (this.samples.length > this.windowSize) {
      const evicted = this.samples.splice(0, this.samples.length - this.windowSize);
      for (const e of evicted) this.sumMs -= e;
    }
  }

  /** Summarizes the current window. Percentiles use nearest-rank. */
  summary(): DeliveryLatencySummaryStats {
    const n = this.samples.length;
    if (n === 0) {
      return { samples: 0, p50Ms: 0, p95Ms: 0, p99Ms: 0, minMs: 0, maxMs: 0, meanMs: 0 };
    }
    const sorted = [...this.samples].sort((a, b) => a - b);
    return {
      samples: n,
      p50Ms: quantileRank(sorted, 0.5),
      p95Ms: quantileRank(sorted, 0.95),
      p99Ms: quantileRank(sorted, 0.99),
      minMs: sorted[0],
      maxMs: sorted[n - 1],
      meanMs: this.sumMs / n,
    };
  }
}

/**
 * Tuning for per-subscriber handler processing-latency p99 SLO alerting
 * (EB-53). Opt-in by presence: `subscribe(..., { latencySlo: {
 * p99ThresholdMs: 50 } })`.
 *
 * The bus measures every handler invocation's processing time —
 * delivery→handler-return for plain subscriptions,
 * delivery→`ack()` completion for reliable ones — and feeds the deltas
 * into a `DeliveryLatencyTracker` window (shared with the queue-dwell
 * sampler above; each feature keeps its own window, so enabling one never
 * double-samples the other). When the windowed nearest-rank p99 exceeds
 * `p99ThresholdMs`, the bus fires `onLatencySloMiss` once per excursion
 * (re-arming after the p99 drops back to or below the threshold) — an
 * advisory alert that never disturbs delivery.
 */
export interface ProcessingLatencySloOptions {
  /**
   * How many of the most recent processing-time samples to keep per
   * subscriber; the oldest samples are evicted past this bound. Must be
   * a positive integer. Default 1024.
   */
  windowSize?: number;
  /**
   * The p99 SLO in milliseconds: the alert fires when the windowed
   * nearest-rank p99 exceeds this. Required; must be a positive finite
   * number.
   */
  p99ThresholdMs: number;
  /**
   * Fired once per excursion when the windowed p99 exceeds
   * `p99ThresholdMs`, with the p99, the threshold, and the subscriber's
   * identity. Advisory only and error-isolated: a throwing callback is
   * swallowed and never disturbs the delivery flow.
   */
  onLatencySloMiss?: (event: LatencySloMissEvent) => void;
}

/**
 * Fired when a subscriber's windowed handler processing-latency p99
 * exceeds its SLO (`SubscribeOptions.latencySlo`). The bus fills in the
 * subscriber's identity when it fires the callback.
 */
export interface LatencySloMissEvent {
  /** The subscriber whose processing-latency p99 missed the SLO. */
  subscriberId: string;
  /** The topic pattern the subscriber registered. */
  pattern: string;
  /** The windowed nearest-rank p99 handler processing latency, in milliseconds. */
  p99Ms: number;
  /** The SLO threshold it exceeded, in milliseconds. */
  thresholdMs: number;
  /** Samples in the window at the moment the alert fired. */
  samples: number;
  /** Bus-clock reading at the moment the alert fired. */
  at: number;
}
