/**
 * End-to-end ack-latency sampling for reliable subscribers (EB-40).
 *
 * Measures the full accepted→ack round trip of each reliably delivered
 * message: from the moment the message is accepted into the subscriber's
 * queue to the moment its delivery is acked. Unlike the delivery-latency
 * tracker in `latency.ts` — which records the enqueue→handler-hand-off
 * queue dwell (handler processing time excluded) — this metric includes
 * everything the consumer did: queueing, handler processing, downstream
 * calls, and any think time before `ack()`. A high p99 here with a low
 * delivery-latency p99 means the consumer itself is slow, not starved.
 *
 * Sampling is opt-in per subscriber (`SubscribeOptions.ackLatency`).
 * The bus stamps every accepted enqueue with the bus clock
 * (`EventBusOptions.now` — the same injection point `TokenBucket` in
 * `throttle.ts` uses) and the tracker records one sample when the
 * delivery's `ack()` finishes. The tracker takes its own injected clock
 * (defaulting to the bus clock the bus passes in) so tests drive
 * deterministic latencies by advancing a controllable clock.
 *
 * Semantics worth knowing:
 * - One sample per message, end to end. `nack()` and ack-timeout
 *   redeliveries restart the accepted clock (the requeue re-stamps), but
 *   the eventual `ack()` still records a single sample — repeated
 *   deliveries of the same message never sample twice. Acking a stale
 *   delivery handle (one whose delivery already timed out and requeued)
 *   records nothing: the bus only lets the currently outstanding
 *   delivery sample.
 * - A clock that moved backwards between accept and ack clamps the
 *   sample at 0.
 * - Messages that are never acked — nacked forever, timed out into the
 *   dead-letter queue, dropped by backpressure, or expired by TTL — are
 *   never sampled. This is an *ack* latency distribution, not a
 *   wait-to-die one.
 * - Each tracker is an independent metric from the delivery-latency
 *   tracker in `latency.ts`: a message contributes at most one sample
 *   to each, and enabling one never double-samples the other.
 */

/** Tuning for per-subscriber ack-latency sampling. */
export interface AckLatencyOptions {
  /**
   * How many of the most recent samples to keep per subscriber; the
   * oldest samples are evicted past this bound. Must be a positive
   * integer. Default 1024.
   */
  windowSize?: number;
}

/**
 * Fired when an acked delivery's accepted→ack latency exceeds the
 * subscriber's SLO (`SubscribeOptions.ackSloMs`). The bus enriches this
 * with the subscriber's identity before calling `onAckSloMiss`.
 */
export interface AckSloMissEvent {
  /** The sampled accepted→ack latency, in milliseconds. */
  latencyMs: number;
  /** The SLO it exceeded, in milliseconds. */
  sloMs: number;
  /** Bus-clock reading at the moment of the ack. */
  at: number;
}

/** An ack-latency distribution summary over one subscriber's sample window. */
export interface AckLatencySummaryStats {
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
  /**
   * Samples whose latency was within the SLO (`latencyMs <= sloMs`).
   * Equals `samples` when no SLO is configured.
   */
  withinSlo: number;
  /**
   * `withinSlo / samples` — the SLO attainment rate. 0 when the window is
   * empty; 1 when no SLO is configured and the window is non-empty.
   */
  sloAttainment: number;
}

/** Nearest-rank quantile of a sorted (ascending) sample array. */
function quantileRank(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.ceil(q * sorted.length) - 1;
  return sorted[Math.min(Math.max(rank, 0), sorted.length - 1)];
}

interface MessageClock {
  /** Bus-clock reading of the most recent accept (restarted on redelivery). */
  acceptedAt: number;
  /** Whether this message already contributed its one sample. */
  sampled: boolean;
}

/** Options for {@link AckLatencyTracker}. */
export interface AckLatencyTrackerOptions {
  /** Rolling window bound; must be a positive integer. Default 1024. */
  windowSize?: number;
  /**
   * The ack-latency SLO in milliseconds. Samples above it fire
   * `onSloMiss` and count against `sloAttainment`. Must be a positive
   * finite number when provided; absent means no SLO is enforced.
   */
  sloMs?: number;
  /** Clock reading in milliseconds; defaults to `Date.now`. */
  now?: () => number;
  /** Fired synchronously when a sample exceeds `sloMs`. */
  onSloMiss?: (event: AckSloMissEvent) => void;
}

/**
 * Bounded rolling window of accepted→ack latency samples for one
 * subscriber, plus per-message accept-clock bookkeeping so that each
 * message contributes at most one sample no matter how many times it is
 * redelivered. Message records are keyed by object identity and held in
 * a `WeakMap`, so they die with their messages and need no cleanup.
 */
export class AckLatencyTracker {
  private readonly samples: number[] = [];
  private readonly windowSize: number;
  private readonly sloMs: number | undefined;
  private readonly now: () => number;
  private readonly onSloMiss: ((event: AckSloMissEvent) => void) | undefined;
  private readonly clocks = new WeakMap<object, MessageClock>();
  private sumMs = 0;
  private withinSloCount = 0;

  constructor(options: AckLatencyTrackerOptions = {}) {
    const windowSize = options.windowSize ?? 1024;
    if (!Number.isInteger(windowSize) || windowSize <= 0) {
      throw new RangeError('windowSize must be a positive integer');
    }
    const sloMs = options.sloMs;
    if (sloMs !== undefined && (!Number.isFinite(sloMs) || sloMs <= 0)) {
      throw new RangeError('sloMs must be a positive finite number of milliseconds');
    }
    this.windowSize = windowSize;
    this.sloMs = sloMs;
    this.now = options.now ?? Date.now;
    this.onSloMiss = options.onSloMiss;
  }

  /**
   * Stamps (or restarts) a message's accepted clock. Called on every
   * accepted enqueue — the first accept starts the clock, a redelivery
   * requeue restarts it. Never clears the sampled flag: a message that
   * already contributed its sample cannot contribute another.
   */
  accepted(key: object): void {
    const record = this.clocks.get(key);
    if (record === undefined) {
      this.clocks.set(key, { acceptedAt: this.now(), sampled: false });
    } else {
      record.acceptedAt = this.now();
    }
  }

  /**
   * Records one accepted→ack sample for a message: `now - acceptedAt`,
   * clamped at 0 on clock skew. At most one sample per message — repeats
   * and samples for never-accepted messages are ignored. Returns the
   * recorded sample, or `undefined` when nothing was recorded. Fires
   * `onSloMiss` when the sample exceeds the SLO.
   */
  sampleOnAck(key: object): number | undefined {
    const record = this.clocks.get(key);
    if (record === undefined || record.sampled) return undefined;
    record.sampled = true;
    const latencyMs = Math.max(0, this.now() - record.acceptedAt);
    this.samples.push(latencyMs);
    this.sumMs += latencyMs;
    if (this.sloMs === undefined || latencyMs <= this.sloMs) {
      this.withinSloCount += 1;
    }
    if (this.samples.length > this.windowSize) {
      const evicted = this.samples.splice(0, this.samples.length - this.windowSize);
      for (const e of evicted) {
        this.sumMs -= e;
        if (this.sloMs === undefined || e <= this.sloMs) {
          this.withinSloCount -= 1;
        }
      }
    }
    if (this.sloMs !== undefined && latencyMs > this.sloMs) {
      this.onSloMiss?.({ latencyMs, sloMs: this.sloMs, at: this.now() });
    }
    return latencyMs;
  }

  /** Summarizes the current window. Percentiles use nearest-rank. */
  summary(): AckLatencySummaryStats {
    const n = this.samples.length;
    if (n === 0) {
      return {
        samples: 0,
        p50Ms: 0,
        p95Ms: 0,
        p99Ms: 0,
        minMs: 0,
        maxMs: 0,
        meanMs: 0,
        withinSlo: 0,
        sloAttainment: 0,
      };
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
      withinSlo: this.withinSloCount,
      sloAttainment: this.withinSloCount / n,
    };
  }
}
