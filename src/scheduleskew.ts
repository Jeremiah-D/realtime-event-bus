/**
 * Delayed-delivery schedule-skew sampling (EB-136).
 *
 * A delayed schedule is *supposed* to fan out exactly at its due time
 * (`DelayedEntry.deliverAt` — the schedule's "expected fan-out moment"; no
 * redundant timestamp field is stamped, the documentation states
 * `scheduledFor === deliverAt`). In practice the bus fans it out on the
 * first sweep *at or after* its due time, so the observed fan-out lags the
 * plan by a little: the schedule skew, `fanOutAt - deliverAt`. This module
 * measures that skew per fanned-out schedule and keeps a bounded rolling
 * window of the most recent samples, with nearest-rank p50/p99 — the
 * timer-fidelity signal an operator pages on when due-time SLAs slip.
 *
 * Sampling is opt-in bus-wide (`EventBusOptions.scheduleSkew`). The bus
 * computes every skew from its injected clock (`EventBusOptions.now`) —
 * the same injection point `TokenBucket` in `throttle.ts` uses — so the
 * tracker itself takes no clock: it only ever sees already-computed
 * deltas, exactly like `DeliveryLatencyTracker` in `latency.ts` (whose
 * bounded-window/nearest-rank/sum-mean machinery this file mirrors for a
 * bus-wide, skew-shaped window; the per-subscriber tracker is not reused
 * because its window belongs to one subscription, not to the bus).
 *
 * Semantics worth knowing:
 * - One sample per *actual* fan-out: the bus records the skew only on the
 *   sweep path that really calls `fanOut` (`sweepDelayed`). Schedules that
 *   never fan out — cancelled (lazy heap deletion), expired while
 *   delayed (TTL deadline passed before the due time), shed under a
 *   shed-mode topic pause, or deferred into a queue-mode pause buffer —
 *   have no actual fan-out moment, so they are never sampled. This is a
 *   *fan-out* skew distribution, not a wait-to-die one.
 * - Schedules rebuilt from the durable log after a restart (EB-55) sample
 *   exactly like live schedules: `recoverDelayed` rebuilds the heap and
 *   sweeps it, so the recovery fan-out is the same code path.
 * - A clock that moved backwards between schedule time and the sweep
 *   clamps the sample at 0 (`Math.max(0, now - deliverAt)` on the sweep,
 *   and `record` clamps again defensively).
 * - The alert is per-sample: every sample whose skew exceeds
 *   `alertThresholdMs` fires `onScheduleSkew` once. It is advisory and
 *   error-isolated — a throwing callback is swallowed by the bus and
 *   never disturbs the fan-out.
 */

/** Tuning for delayed-delivery schedule-skew sampling. Opt-in by presence. */
export interface ScheduleSkewOptions {
  /**
   * How many of the most recent skew samples to keep; the oldest samples
   * are evicted past this bound. Must be a positive integer.
   * Default 1024.
   */
  windowSize?: number;
  /**
   * The skew alert threshold in milliseconds: every sample whose skew
   * exceeds this fires `onScheduleSkew` once. Must be a non-negative
   * finite number. Default `undefined` (no alerting). Required when
   * `onScheduleSkew` is set — a callback with no threshold could never
   * fire, which is almost certainly a configuration bug.
   */
  alertThresholdMs?: number;
  /**
   * Fired once per sample whose skew exceeds `alertThresholdMs`, with
   * the schedule's due time, the actual fan-out time, the skew, and the
   * schedule's identity. Advisory only and error-isolated: the bus
   * swallows a throwing callback and fans out anyway.
   */
  onScheduleSkew?: (event: ScheduleSkewEvent) => void;
}

/**
 * Revision for the bus-level schedule-skew tuning (see
 * `EventBus.setScheduleSkew`, EB-303). Only the fields provided move;
 * the rest keep their live values. `undefined` is not a value — a
 * partial object keeps the fields it leaves out — so there is no way
 * to unset the callback through this API: a bus that armed a callback
 * keeps alerting until the callback is replaced by another one.
 */
export interface ScheduleSkewRevision {
  /**
   * How many of the most recent skew samples to keep. Must be a
   * positive integer when provided — the construction-time rule.
   */
  windowSize?: number;
  /**
   * The skew alert threshold in milliseconds. Must be a non-negative
   * finite number when provided — the construction-time rule.
   */
  alertThresholdMs?: number;
  /**
   * Fired once per sample whose skew exceeds `alertThresholdMs`. Must
   * be a function when provided, and requires an alert threshold to
   * be in force (provided here or already live) — the
   * construction-time rule.
   */
  onScheduleSkew?: (event: ScheduleSkewEvent) => void;
}

/** One schedule-skew alert / audit observation. */
export interface ScheduleSkewEvent {
  /**
   * The schedule's expected fan-out moment — `DelayedEntry.deliverAt`
   * (there is no redundant timestamp field; `scheduledFor === deliverAt`
   * by definition).
   */
  scheduledFor: number;
  /** Bus-clock reading at the moment the schedule actually fanned out. */
  fannedOutAtMs: number;
  /** `max(0, fannedOutAtMs - scheduledFor)`, in milliseconds. */
  skewMs: number;
  /** Concrete topic the schedule fanned out to. */
  topic: string;
  /** The schedule's id (`delayed-1`, …). */
  delayId: string;
}

/** A skew distribution summary over the bus's sample window. */
export interface ScheduleSkewSummaryStats {
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

/** The zeroed summary a bus without `scheduleSkew` reports (and renders). */
export function emptyScheduleSkewSummary(): ScheduleSkewSummaryStats {
  return { samples: 0, p50Ms: 0, p99Ms: 0, minMs: 0, maxMs: 0, meanMs: 0 };
}

/** Nearest-rank quantile of a sorted (ascending) sample array. */
function quantileRank(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.ceil(q * sorted.length) - 1;
  return sorted[Math.min(Math.max(rank, 0), sorted.length - 1)];
}

/**
 * Bounded rolling window of delayed-schedule skew samples for the whole
 * bus. Samples are plain millisecond deltas; the bus computes them from
 * its injected clock at sweep time.
 */
export class ScheduleSkewTracker {
  private readonly samples: number[] = [];
  // Live-tuned by `retune` (EB-303): `record` reads this field on every
  // call, so a revision governs from the next sample without draining
  // or rebuilding the window — the samples are untouched by a retune.
  private windowSize: number;
  private sumMs = 0;

  constructor(windowSize = 1024) {
    if (!Number.isInteger(windowSize) || windowSize <= 0) {
      throw new RangeError('windowSize must be a positive integer');
    }
    this.windowSize = windowSize;
  }

  /** The live window bound, for bus-level revision (EB-303). */
  current(): { windowSize: number } {
    return { windowSize: this.windowSize };
  }

  /**
   * Retunes the window bound live (EB-303). The caller must have
   * validated `windowSize` through `resolveScheduleSkewOptions` first —
   * this method applies unconditionally. The samples are untouched: a
   * tightened window truncates the oldest samples down to the new
   * bound at the next `record`, so the window converges to the new
   * bound instead of staying oversized forever.
   */
  retune(windowSize: number): void {
    this.windowSize = windowSize;
  }

  /**
   * Records one schedule's skew. Negative inputs (clock moved backwards
   * between schedule time and the sweep) clamp to 0.
   */
  record(skewMs: number): void {
    const sample = Math.max(0, skewMs);
    this.samples.push(sample);
    this.sumMs += sample;
    if (this.samples.length > this.windowSize) {
      const evicted = this.samples.splice(0, this.samples.length - this.windowSize);
      for (const e of evicted) this.sumMs -= e;
    }
  }

  /** Summarizes the current window. Percentiles use nearest-rank. */
  summary(): ScheduleSkewSummaryStats {
    const n = this.samples.length;
    if (n === 0) return emptyScheduleSkewSummary();
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

/** `ScheduleSkewOptions` with defaults resolved and validation applied. */
export interface ResolvedScheduleSkewOptions {
  windowSize: number;
  alertThresholdMs: number | undefined;
  onScheduleSkew: ((event: ScheduleSkewEvent) => void) | undefined;
}

/**
 * Validates bus-level schedule-skew options, applying defaults.
 * `undefined` (absent option) disables sampling entirely and returns
 * `undefined` — the bus keeps no tracker, `getStats()` reports zeros,
 * and no Prometheus series is rendered.
 */
export function resolveScheduleSkewOptions(
  opt: ScheduleSkewOptions | undefined,
): ResolvedScheduleSkewOptions | undefined {
  if (opt === undefined) return undefined;
  if (typeof opt !== 'object' || opt === null) {
    throw new TypeError('scheduleSkew must be a ScheduleSkewOptions object');
  }
  const windowSize = opt.windowSize ?? 1024;
  if (!Number.isInteger(windowSize) || windowSize <= 0) {
    throw new RangeError('scheduleSkew.windowSize must be a positive integer');
  }
  const alertThresholdMs = opt.alertThresholdMs;
  if (alertThresholdMs !== undefined && (!Number.isFinite(alertThresholdMs) || alertThresholdMs < 0)) {
    throw new RangeError('scheduleSkew.alertThresholdMs must be a non-negative finite number of milliseconds');
  }
  const onScheduleSkew = opt.onScheduleSkew;
  if (onScheduleSkew !== undefined && typeof onScheduleSkew !== 'function') {
    throw new TypeError('scheduleSkew.onScheduleSkew must be a function');
  }
  if (onScheduleSkew !== undefined && alertThresholdMs === undefined) {
    throw new RangeError('scheduleSkew.onScheduleSkew requires scheduleSkew.alertThresholdMs: a callback with no threshold can never fire');
  }
  return { windowSize, alertThresholdMs, onScheduleSkew };
}
