import type { BusMessage } from './bus.ts';

/**
 * Subscriber lag forecasting (EB-183): predictive lag monitoring.
 *
 * Where the lag watermark monitor (`src/lag.ts`, EB-35) answers the
 * operator's live question — "how far behind is this consumer *right
 * now*?" — the forecaster answers the earlier question: "at the current
 * growth rate, *when* will this consumer's lag reach the threshold?"
 *
 * - **Watermark samples**: on every accepted enqueue the bus stamps the
 *   message (its own per-subscriber `WeakMap`, independent of the lag
 *   monitor's) and records one `(atMs, watermarkMs)` sample, where
 *   `watermarkMs` is the queue-head dwell — `now - enqueuedAt(oldest
 *   queued message)`, the same watermark `lag.ts` reports. The window is
 *   bounded; the oldest samples are evicted past it.
 * - **Least-squares trend extrapolation**: the tracker fits a line
 *   `lag = slope·t + intercept` over the window and solves for the
 *   crossing time `t*` where the line reaches `thresholdMs`. The result
 *   is the predicted remaining time `forecastMsAtThreshold = t* − now`
 *   plus the fit's R² as `confidence` (0..1).
 * - **Early alerting**: `onLagForecast` fires once when a crossing is
 *   predicted within `forecastHorizonMs` — strictly *before* the
 *   watermark reaches the threshold — and re-arms when the prediction
 *   clears. A throwing callback propagates to the caller, the same
 *   convention as the lag monitor's `onLag`.
 *
 * Sampling is opt-in per subscriber (`SubscribeOptions.lagForecast`) and
 * orthogonal to EB-35 (current-waterlevel observation) and EB-48 (group
 * lag): this module predicts, those observe. The clock is the bus's own
 * injected clock (`EventBusOptions.now`), so tests drive deterministic
 * forecasts by advancing a controllable clock; the tracker itself takes
 * no clock because it only ever sees already-computed stamps.
 *
 * Semantics worth knowing:
 * - The forecast needs a *growing* watermark: a flat or shrinking trend
 *   (`slope <= 0`) predicts no crossing and yields no forecast — a
 *   subscriber keeping up needs no early warning. Fewer than
 *   `minSamples` samples also yields no forecast: two points make a
 *   line, but a useful trend needs a few more.
 * - The forecast never fires once the watermark has already reached the
 *   threshold — the lag monitor's `onLag` owns that signal; this is the
 *   *early* warning, not a second alarm for the same excursion.
 * - Like the lag monitor, the forecast is evaluated on every accepted
 *   enqueue and every drain: a crossing predicted on new backlog growth
 *   fires without waiting for the next drain, and a drain that flattens
 *   the trend re-arms the latch. When the queue drains empty the sample
 *   window resets — the backlog episode is over, and its watermark
 *   history would poison the next episode's trend fit. A completely idle
 *   bus cannot observe its own lag growing — inherent to a clock-read
 *   design with no background timer (the bus never keeps the process
 *   alive for observability alone).
 * - Requeues (nack redelivery, health requeue) re-stamp like the lag
 *   monitor: each delivery samples its own queue residency.
 */

/** Tuning for subscriber lag forecasting. */
export interface LagForecastOptions {
  /**
   * How many of the most recent (atMs, watermarkMs) samples to keep;
   * the oldest samples are evicted past this bound. Must be a positive
   * integer. Default 64.
   */
  windowSize?: number;
  /**
   * Minimum samples before the least-squares fit runs. Must be an
   * integer >= 2. Default 4.
   */
  minSamples?: number;
  /**
   * The lag level, in milliseconds, whose crossing is forecast. The
   * callback only fires while the watermark is still *below* this
   * level. Must be a positive finite number when provided.
   */
  thresholdMs?: number;
  /**
   * The callback fires only when the predicted crossing is within this
   * many milliseconds. Must be a positive finite number when provided.
   * Defaults to `thresholdMs`.
   */
  forecastHorizonMs?: number;
  /**
   * Called once per predicted excursion, when the watermark is forecast
   * to reach `thresholdMs` within `forecastHorizonMs`; re-arms when the
   * prediction clears. Requires `thresholdMs` — without it the callback
   * can never fire, and subscribing with `onLagForecast` but no
   * `thresholdMs` throws `RangeError` (fail-fast on a config that looks
   * armed but is not).
   */
  onLagForecast?: (event: LagForecastEvent) => void;
}

/** Fired when a subscriber's lag is forecast to reach its threshold. */
export interface LagForecastEvent {
  /** The forecasting subscription's id. */
  subscriberId: string;
  /** The pattern the subscription was registered with. */
  pattern: string;
  /**
   * Predicted milliseconds until the watermark reaches `thresholdMs`
   * at the current growth rate. Always within `forecastHorizonMs`.
   */
  forecastMsAtThreshold: number;
  /** Least-squares R² of the trend fit, 0..1. */
  confidence: number;
  /** The watermark when the forecast fired, in milliseconds. */
  watermarkMs: number;
  /** The configured threshold, in milliseconds. */
  thresholdMs: number;
  /** Bus-clock reading of the forecast, in milliseconds. */
  at: number;
}

/** One watermark sample: bus-clock stamp and queue-head dwell. */
interface WatermarkSample {
  atMs: number;
  watermarkMs: number;
}

/** The predicted crossing, or undefined when none is predicted. */
export interface LagForecastPrediction {
  /** Predicted milliseconds from `nowMs` until the watermark reaches the threshold. */
  forecastMsAtThreshold: number;
  /** Least-squares R² of the trend fit, 0..1. */
  confidence: number;
  /** The fitted slope: ms of lag growth per ms of wall time. */
  slopeMsPerMs: number;
}

/**
 * Bounded rolling window of watermark samples with least-squares trend
 * extrapolation. Samples are plain (timestamp, dwell) pairs; the bus
 * computes them from its injected clock.
 */
export class LagForecastTracker {
  private readonly samples: WatermarkSample[] = [];
  // Mutable only via `resize` (EB-278): the bounds are part of the
  // revision surface, the sample array itself is never replaced.
  private windowSize: number;
  private minSamples: number;

  constructor(windowSize = 64, minSamples = 4) {
    if (!Number.isInteger(windowSize) || windowSize <= 0) {
      throw new RangeError('lagForecast.windowSize must be a positive integer');
    }
    if (!Number.isInteger(minSamples) || minSamples < 2) {
      throw new RangeError('lagForecast.minSamples must be an integer >= 2');
    }
    if (minSamples > windowSize) {
      throw new RangeError('lagForecast.minSamples must not exceed lagForecast.windowSize');
    }
    this.windowSize = windowSize;
    this.minSamples = minSamples;
  }

  /** Records one (atMs, watermarkMs) sample. Negative dwells clamp to 0. */
  record(atMs: number, watermarkMs: number): void {
    this.samples.push({ atMs, watermarkMs: Math.max(0, watermarkMs) });
    if (this.samples.length > this.windowSize) {
      this.samples.splice(0, this.samples.length - this.windowSize);
    }
  }

  /** Samples currently in the window. */
  get sampleCount(): number {
    return this.samples.length;
  }

  /** The window bound the tracker currently enforces (EB-278). */
  windowSizeLimit(): number {
    return this.windowSize;
  }

  /** The minimum-sample bound the tracker currently enforces (EB-278). */
  minSamplesLimit(): number {
    return this.minSamples;
  }

  /**
   * Retunes the window and minimum-sample bounds in place (EB-278,
   * `setLagForecast`): shrinking the window evicts only the oldest
   * samples past the new bound — the same oldest-first eviction
   * `record` applies, via `splice` so the sample array itself is
   * never replaced — and growing only raises the bound and keeps
   * every sample, never restoring samples a shrink already evicted.
   * Raising `minSamples` alone changes no sample, it only raises the
   * bar for the next fit. The tracker object itself never moves, so
   * a subscriber's forecast history survives a parameter revision.
   */
  resize(windowSize: number, minSamples: number = this.minSamples): void {
    if (!Number.isInteger(windowSize) || windowSize <= 0) {
      throw new RangeError('lagForecast.windowSize must be a positive integer');
    }
    if (!Number.isInteger(minSamples) || minSamples < 2) {
      throw new RangeError('lagForecast.minSamples must be an integer >= 2');
    }
    if (minSamples > windowSize) {
      throw new RangeError('lagForecast.minSamples must not exceed lagForecast.windowSize');
    }
    if (windowSize < this.samples.length) {
      this.samples.splice(0, this.samples.length - windowSize);
    }
    this.windowSize = windowSize;
    this.minSamples = minSamples;
  }

  /**
   * Clears the sample window. Called when the subscriber's queue drains
   * empty: the backlog episode is over, and its watermark history would
   * poison the next episode's trend fit.
   */
  reset(): void {
    this.samples.length = 0;
  }

  /**
   * Least-squares linear extrapolation of the watermark trend.
   * Returns the predicted crossing of `thresholdMs` — remaining
   * milliseconds from `nowMs` plus the fit's R² — or `undefined` when no
   * crossing is predicted: too few samples, a non-growing trend
   * (`slope <= 0`), a degenerate (frozen-clock) sample set, or the
   * threshold already reached (the lag monitor owns that signal).
   */
  forecast(nowMs: number, thresholdMs: number): LagForecastPrediction | undefined {
    const n = this.samples.length;
    if (n < this.minSamples) return undefined;
    let sumT = 0;
    let sumW = 0;
    let sumTW = 0;
    let sumT2 = 0;
    for (const s of this.samples) {
      sumT += s.atMs;
      sumW += s.watermarkMs;
      sumTW += s.atMs * s.watermarkMs;
      sumT2 += s.atMs * s.atMs;
    }
    const denom = n * sumT2 - sumT * sumT;
    if (!(denom > 0)) return undefined; // frozen clock: no trend to fit
    const slope = (n * sumTW - sumT * sumW) / denom;
    if (!(slope > 0)) return undefined; // flat or shrinking: no crossing ahead
    const intercept = (sumW - slope * sumT) / n;
    const crossAt = (thresholdMs - intercept) / slope;
    const forecastMsAtThreshold = crossAt - nowMs;
    if (!(forecastMsAtThreshold >= 0)) return undefined; // already there: lag monitor's signal
    // R² of the fit.
    const meanW = sumW / n;
    let ssRes = 0;
    let ssTot = 0;
    for (const s of this.samples) {
      const fitted = slope * s.atMs + intercept;
      ssRes += (s.watermarkMs - fitted) * (s.watermarkMs - fitted);
      ssTot += (s.watermarkMs - meanW) * (s.watermarkMs - meanW);
    }
    const confidence = ssTot <= 0 ? 1 : Math.min(1, Math.max(0, 1 - ssRes / ssTot));
    return { forecastMsAtThreshold, confidence, slopeMsPerMs: slope };
  }
}

/**
 * Per-subscriber lag forecasting state (see
 * `SubscribeOptions.lagForecast`). `enqueuedAt` maps a queued message to
 * the bus-clock reading of its most recent enqueue; entries die with
 * their messages (WeakMap), so no cleanup is needed on unsubscribe.
 * `alerted` is the forecast latch: set when `onLagForecast` fires,
 * cleared when the prediction no longer shows a crossing within the
 * horizon.
 */
export interface SubscriberLagForecastState {
  tracker: LagForecastTracker;
  enqueuedAt: WeakMap<BusMessage, number>;
  thresholdMs?: number;
  forecastHorizonMs: number;
  onLagForecast?: (event: LagForecastEvent) => void;
  alerted: boolean;
}

/**
 * Validates `SubscribeOptions.lagForecast` and builds the initial
 * per-subscriber forecasting state. Returns `undefined` when forecasting
 * is disabled. Throws `RangeError`/`TypeError` for invalid options, or
 * for an `onLagForecast` callback with no `thresholdMs` to arm it.
 */
export function resolveLagForecastOptions(
  opt: boolean | LagForecastOptions | undefined,
): SubscriberLagForecastState | undefined {
  if (opt == null || opt === false) return undefined;
  if (opt !== true && (typeof opt !== 'object' || opt === null)) {
    throw new TypeError('lagForecast must be true or a LagForecastOptions object');
  }
  const o: LagForecastOptions = opt === true ? {} : opt;
  const windowSize = o.windowSize ?? 64;
  if (!Number.isInteger(windowSize) || windowSize <= 0) {
    throw new RangeError('lagForecast.windowSize must be a positive integer');
  }
  const minSamples = o.minSamples ?? 4;
  if (!Number.isInteger(minSamples) || minSamples < 2) {
    throw new RangeError('lagForecast.minSamples must be an integer >= 2');
  }
  if (minSamples > windowSize) {
    throw new RangeError('lagForecast.minSamples must not exceed lagForecast.windowSize');
  }
  const thresholdMs = o.thresholdMs;
  if (thresholdMs !== undefined && (!Number.isFinite(thresholdMs) || thresholdMs <= 0)) {
    throw new RangeError('lagForecast.thresholdMs must be a positive finite number of milliseconds');
  }
  const forecastHorizonMs = o.forecastHorizonMs ?? thresholdMs;
  if (forecastHorizonMs !== undefined && (!Number.isFinite(forecastHorizonMs) || forecastHorizonMs <= 0)) {
    throw new RangeError(
      'lagForecast.forecastHorizonMs must be a positive finite number of milliseconds',
    );
  }
  const onLagForecast = o.onLagForecast;
  if (onLagForecast !== undefined && typeof onLagForecast !== 'function') {
    throw new TypeError('lagForecast.onLagForecast must be a function');
  }
  if (onLagForecast !== undefined && thresholdMs === undefined) {
    throw new RangeError(
      'lagForecast.onLagForecast requires lagForecast.thresholdMs: a callback with no threshold can never fire',
    );
  }
  return {
    tracker: new LagForecastTracker(windowSize, minSamples),
    enqueuedAt: new WeakMap(),
    thresholdMs,
    forecastHorizonMs: forecastHorizonMs ?? Number.POSITIVE_INFINITY,
    onLagForecast,
    alerted: false,
  };
}
