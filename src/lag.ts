import type { BusMessage } from './bus.ts';

/**
 * Subscriber lag watermark monitoring (EB-35).
 *
 * Where `SubscribeOptions.deliveryLatency` (see `src/latency.ts`) records a
 * *historical* distribution of per-delivery queue dwells, the lag monitor
 * answers the operator's live question — "how far behind is this consumer
 * *right now*?" — Kafka-consumer-lag style:
 *
 * - **Live watermark**: `now - enqueuedAt(oldest queued message)`, read at
 *   `getStats()` time from the bus clock. 0 when the queue is empty. This
 *   is a gauge of current backlog age, not a sample of completed
 *   deliveries: a subscriber whose handler is stuck shows a watermark that
 *   keeps growing, while its delivery-latency distribution (if enabled)
 *   shows nothing at all — nothing is being delivered.
 * - **Threshold alerting**: `onLag` fires when the watermark reaches
 *   `thresholdMs`. It fires once per excursion and re-arms after the
 *   watermark drops below the threshold, mirroring the `onBackpressure` /
 *   `onDrained` latch semantics. The watermark is evaluated on every
 *   enqueue and every drain, so a crossing is observed on the next bus
 *   activity after it happens — a completely idle bus cannot observe its
 *   own lag growing, which is inherent to a clock-read design with no
 *   background timer (the bus never keeps the process alive for
 *   observability alone).
 * - **Dwell distribution**: every drained message also records one
 *   enqueue→drain dwell sample into a bounded rolling window, summarized
 *   as p50/p99 (nearest-rank). This complements the watermark: the
 *   watermark says how old the current backlog head is, the distribution
 *   says how long drained messages had waited.
 *
 * Sampling is opt-in per subscriber and costs one clock read per enqueue
 * plus one WeakMap entry per queued message for tracked subscribers only.
 * The clock is the bus's own injected clock (`EventBusOptions.now`), so
 * tests drive deterministic watermarks and alerts by advancing a
 * controllable clock.
 *
 * Semantics worth knowing:
 * - The watermark covers messages sitting in the subscriber's bounded
 *   queue. Messages already dequeued into a batch subscriber's pending
 *   batch (`SubscribeOptions.batch`) are past the queue — they no longer
 *   count toward the watermark, even while the batch lingers for
 *   `maxWaitMs`.
 * - A message dropped by backpressure or expired by TTL leaves the queue
 *   and stops contributing; the watermark then reflects the next oldest
 *   survivor.
 * - At-least-once redeliveries (nack / ack timeout / health requeue)
 *   re-stamp on requeue, exactly like the delivery-latency tracker: each
 *   delivery samples its own dwell, and the watermark measures the current
 *   queue residency, not the message's first arrival.
 * - A throwing `onLag` propagates to the caller (the publish call on the
 *   enqueue path, the flush driver on the drain path) — the same
 *   convention as the other subscriber callbacks.
 */

/** Tuning for subscriber lag watermark monitoring. */
export interface LagMonitorOptions {
  /**
   * How many of the most recent drain-dwell samples to keep per
   * subscriber; the oldest samples are evicted past this bound. Must be a
   * positive integer. Default 1024.
   */
  windowSize?: number;
  /**
   * Watermark threshold in milliseconds: `onLag` fires when the
   * subscriber's live lag watermark reaches or exceeds it. Absent means
   * no alerting — the watermark and the dwell distribution are still
   * reported, but no callback ever fires. Must be a non-negative finite
   * number when provided.
   */
  thresholdMs?: number;
  /**
   * Called once per lag excursion, when the watermark reaches
   * `thresholdMs`; re-arms after the watermark drops below the threshold.
   * Requires `thresholdMs` — without it the callback can never fire, and
   * subscribing with `onLag` but no `thresholdMs` throws `RangeError`
   * (fail-fast on a config that looks armed but is not).
   */
  onLag?: (event: LagEvent) => void;
}

/** Fired when a subscriber's lag watermark crosses its threshold. */
export interface LagEvent {
  /** The lagging subscription's id. */
  subscriberId: string;
  /** The pattern the subscription was registered with. */
  pattern: string;
  /** The watermark that tripped the alert, in milliseconds. */
  lagMs: number;
  /** The configured threshold, in milliseconds. */
  thresholdMs: number;
  /** Queue size when the alert fired. */
  queueSize: number;
  /** Bus-clock reading of the alert, in milliseconds. */
  at: number;
}

/** A lag dwell distribution summary over one subscriber's sample window. */
export interface LagSummaryStats {
  /** Samples currently in the window. */
  samples: number;
  /** Nearest-rank p50/p99 of the window, in milliseconds. 0 when empty. */
  p50Ms: number;
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
 * Bounded rolling window of enqueue→drain dwell samples for one
 * subscriber. Samples are plain millisecond deltas; the bus computes them
 * from its injected clock.
 */
export class LagTracker {
  private readonly samples: number[] = [];
  private readonly windowSize: number;
  private sumMs = 0;

  constructor(windowSize = 1024) {
    if (!Number.isInteger(windowSize) || windowSize <= 0) {
      throw new RangeError('windowSize must be a positive integer');
    }
    this.windowSize = windowSize;
  }

  /** Records one enqueue→drain dwell sample. Negative inputs (clock skew) clamp to 0. */
  record(dwellMs: number): void {
    const sample = Math.max(0, dwellMs);
    this.samples.push(sample);
    this.sumMs += sample;
    if (this.samples.length > this.windowSize) {
      const evicted = this.samples.splice(0, this.samples.length - this.windowSize);
      for (const e of evicted) this.sumMs -= e;
    }
  }

  /** Summarizes the current window. Percentiles use nearest-rank. */
  summary(): LagSummaryStats {
    const n = this.samples.length;
    if (n === 0) {
      return { samples: 0, p50Ms: 0, p99Ms: 0, minMs: 0, maxMs: 0, meanMs: 0 };
    }
    const sorted = [...this.samples].sort((a, b) => a - b);
    return {
      samples: n,
      p50Ms: quantileRank(sorted, 0.5),
      p99Ms: quantileRank(sorted, 0.99),
      minMs: sorted[0],
      maxMs: sorted[n - 1],
      meanMs: this.sumMs / n,
    };
  }
}

/**
 * Per-subscriber lag monitoring state (see `SubscribeOptions.lagMonitor`).
 * `enqueuedAt` maps a queued message to the bus-clock reading of its most
 * recent enqueue; entries die with their messages (WeakMap), so no cleanup
 * is needed on unsubscribe. `alerted` is the threshold latch: set when
 * `onLag` fires, cleared when the watermark drops below the threshold.
 */
export interface SubscriberLagState {
  tracker: LagTracker;
  enqueuedAt: WeakMap<BusMessage, number>;
  thresholdMs?: number;
  onLag?: (event: LagEvent) => void;
  alerted: boolean;
}

/**
 * Validates `SubscribeOptions.lagMonitor` and builds the initial
 * per-subscriber monitoring state. Returns `undefined` when monitoring is
 * disabled. Throws `RangeError` for an invalid window size, an invalid
 * threshold, or an `onLag` callback with no `thresholdMs` to arm it.
 */
export function resolveLagMonitorOptions(
  opt: boolean | LagMonitorOptions | undefined,
): SubscriberLagState | undefined {
  if (opt == null || opt === false) return undefined;
  if (opt !== true && (typeof opt !== 'object' || opt === null)) {
    throw new TypeError('lagMonitor must be true or a LagMonitorOptions object');
  }
  const o: LagMonitorOptions = opt === true ? {} : opt;
  const windowSize = o.windowSize ?? 1024;
  if (!Number.isInteger(windowSize) || windowSize <= 0) {
    throw new RangeError('lagMonitor.windowSize must be a positive integer');
  }
  const thresholdMs = o.thresholdMs;
  if (thresholdMs !== undefined && (!Number.isFinite(thresholdMs) || thresholdMs < 0)) {
    throw new RangeError('lagMonitor.thresholdMs must be a non-negative finite number of milliseconds');
  }
  const onLag = o.onLag;
  if (onLag !== undefined && typeof onLag !== 'function') {
    throw new TypeError('lagMonitor.onLag must be a function');
  }
  if (onLag !== undefined && thresholdMs === undefined) {
    throw new RangeError('lagMonitor.onLag requires lagMonitor.thresholdMs: a callback with no threshold can never fire');
  }
  return {
    tracker: new LagTracker(windowSize),
    enqueuedAt: new WeakMap(),
    thresholdMs,
    onLag,
    alerted: false,
  };
}
