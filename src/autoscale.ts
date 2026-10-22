/**
 * Consumer-lag autoscale signals (EB-192).
 *
 * Turns three existing observability signals into one advisory
 * scale recommendation per topic / subscriber — the input an operator
 * or HPA needs to decide whether to add or remove consumer capacity:
 *
 * - Consumer lag: `LagTracker` dwell distribution (EB-35, `src/lag.ts`)
 *   and the live queue-head watermark — "how far behind is this
 *   consumer right now?" (p99 drain dwell; 0 when the backlog is empty).
 * - Publish load: `PublishRateTable` (EB-34, `src/rates.ts`) trailing
 *   1m rate — "is the load still arriving?" (distinguishes a sustained
 *   overload from a spike that is already draining).
 * - Delivery latency: `DeliveryLatencyTracker` (EB-31, `src/latency.ts`)
 *   enqueue→delivery p99 — a fallback lag source for subscribers that
 *   sample delivery latency but not the lag monitor.
 *
 * This module is a *pure signal*: it never scales anything. The bus
 * exposes it via `EventBus.getScalingSignal()` and Prometheus via
 * `eventbus_autoscale_signal`; the scaling decision stays with the
 * operator. When the inputs are missing (no lag monitor, no delivery
 * latency sampling, no publish-rate data) the signal is `hold` and the
 * reason says so — a missing signal is never mistaken for "all clear".
 *
 * Decision table (with defaults `lagThresholdMs = 1000`,
 * `rateThreshold = 1`, `idleLagMs = 10`, `idleRate = 0.1`):
 *
 * | effective lag p99 | publish rate (r1m) | signal      |
 * |-------------------|------------------|-------------|
 * | > lagThresholdMs  | >= rateThreshold   | `scale_up`  |
 * | > lagThresholdMs  | < rateThreshold    | `hold`      |
 * | > lagThresholdMs  | unknown            | `hold`      |
 * | <= idleLagMs      | < idleRate         | `scale_down`|
 * | anything else     | any                | `hold`      |
 *
 * Effective lag p99 is the lag-monitor dwell p99 when the subscriber
 * has drain-dwell samples, else the delivery-latency p99 when it has
 * delivery samples, else the live watermark. The `scale_down` row
 * additionally requires the live watermark (when a lag monitor is
 * present) to be at or below `idleLagMs` — a drained distribution with
 * a still-growing queue head is not idle.
 *
 * Zero dependencies. All syntax is Node type-stripping safe: no enums,
 * no namespaces, no parameter properties.
 */

/** The autoscale recommendation: add capacity, remove capacity, or do nothing. */
export type ScalingDecision = 'scale_up' | 'scale_down' | 'hold';

/**
 * Short English reason codes carried on `ScalingSignal.reason`; the
 * human-readable text lives on `ScalingSignal.reasonDetail`.
 *
 * - `no-data`: no lag or delivery-latency samples at all — the signal
 *   cannot be evaluated (missing instrumentation, or nothing delivered
 *   yet).
 * - `no-rate-data`: lag is above threshold but the topic has no
 *   publish-rate history, so a sustained load cannot be confirmed.
 * - `lag-above-threshold`: lag p99 exceeds `lagThresholdMs` while the
 *   publish rate stays at or above `rateThreshold` — sustained overload.
 * - `lag-high-rate-dropping`: lag p99 exceeds `lagThresholdMs` but the
 *   publish rate already fell below `rateThreshold` — the backlog is
 *   draining on its own.
 * - `lag-normal`: lag is within its bands — neither hot nor idle.
 * - `drained-idle`: lag drained to `idleLagMs` and the publish rate is
 *   below `idleRate` — excess consumer capacity.
 */
export type ScalingReason =
  | 'no-data'
  | 'no-rate-data'
  | 'lag-above-threshold'
  | 'lag-high-rate-dropping'
  | 'lag-normal'
  | 'drained-idle';

/**
 * Tunable thresholds for the autoscale signal. All optional; unset
 * fields take the defaults below.
 */
export interface AutoscaleThresholds {
  /**
   * Lag p99 (ms) above which a subscriber is considered overloaded.
   * Must be a positive finite number. Default 1000.
   */
  lagThresholdMs?: number;
  /**
   * Trailing-1m publish rate (msg/s) at or above which the load counts
   * as sustained rather than draining. Must be a finite number >= 0.
   * Default 1.
   */
  rateThreshold?: number;
  /**
   * Lag p99 (ms) at or below which a subscriber counts as drained.
   * Must be a finite number >= 0 and strictly below `lagThresholdMs`.
   * Default 10.
   */
  idleLagMs?: number;
  /**
   * Trailing-1m publish rate (msg/s) below which the topic counts as
   * idle. Must be a finite number >= 0. Default 0.1.
   */
  idleRate?: number;
}

/** `AutoscaleThresholds` with every field resolved to its effective value. */
export interface ResolvedAutoscaleThresholds {
  lagThresholdMs: number;
  rateThreshold: number;
  idleLagMs: number;
  idleRate: number;
}

const DEFAULT_LAG_THRESHOLD_MS = 1000;
const DEFAULT_RATE_THRESHOLD = 1;
const DEFAULT_IDLE_LAG_MS = 10;
const DEFAULT_IDLE_RATE = 0.1;

/**
 * Validates `AutoscaleThresholds` and resolves defaults. Throws
 * `RangeError` for a non-positive/non-finite `lagThresholdMs`, a
 * negative/non-finite `rateThreshold`/`idleRate`, a negative/non-finite
 * `idleLagMs`, or an `idleLagMs` that is not strictly below
 * `lagThresholdMs` (the scale-up band must sit above the idle band).
 * Throws `TypeError` for a non-object options value.
 */
export function resolveAutoscaleOptions(
  opt?: AutoscaleThresholds,
): ResolvedAutoscaleThresholds {
  if (opt === undefined || opt === null) {
    return {
      lagThresholdMs: DEFAULT_LAG_THRESHOLD_MS,
      rateThreshold: DEFAULT_RATE_THRESHOLD,
      idleLagMs: DEFAULT_IDLE_LAG_MS,
      idleRate: DEFAULT_IDLE_RATE,
    };
  }
  if (typeof opt !== 'object') {
    throw new TypeError('autoscale options must be an object');
  }
  const lagThresholdMs = opt.lagThresholdMs ?? DEFAULT_LAG_THRESHOLD_MS;
  if (!Number.isFinite(lagThresholdMs) || lagThresholdMs <= 0) {
    throw new RangeError(
      'autoscale.lagThresholdMs must be a positive finite number of milliseconds',
    );
  }
  const rateThreshold = opt.rateThreshold ?? DEFAULT_RATE_THRESHOLD;
  if (!Number.isFinite(rateThreshold) || rateThreshold < 0) {
    throw new RangeError(
      'autoscale.rateThreshold must be a finite number of messages per second >= 0',
    );
  }
  const idleLagMs = opt.idleLagMs ?? DEFAULT_IDLE_LAG_MS;
  if (!Number.isFinite(idleLagMs) || idleLagMs < 0) {
    throw new RangeError(
      'autoscale.idleLagMs must be a finite number of milliseconds >= 0',
    );
  }
  const idleRate = opt.idleRate ?? DEFAULT_IDLE_RATE;
  if (!Number.isFinite(idleRate) || idleRate < 0) {
    throw new RangeError(
      'autoscale.idleRate must be a finite number of messages per second >= 0',
    );
  }
  if (idleLagMs >= lagThresholdMs) {
    throw new RangeError(
      'autoscale.idleLagMs must be strictly below autoscale.lagThresholdMs: the scale-up band sits above the idle band',
    );
  }
  return { lagThresholdMs, rateThreshold, idleLagMs, idleRate };
}

/**
 * The evaluated inputs of one autoscale signal. Any field may be
 * absent: absent lag fields mean no instrumentation/samples, absent
 * `rate1m` means the topic has no publish-rate history.
 */
export interface ScalingSignalInput {
  /**
   * Lag p99 in milliseconds: the lag-monitor drain-dwell p99, falling
   * back to the delivery-latency p99. Undefined when neither has
   * samples.
   */
  lagP99Ms?: number;
  /**
   * The live queue-head watermark in milliseconds (0 when the queue is
   * empty). Undefined when the subscriber has no lag monitor.
   */
  watermarkMs?: number;
  /** Trailing-1m publish rate in messages per second. */
  rate1m?: number;
}

/** One computed autoscale signal — advisory only, never acts. */
export interface ScalingSignal {
  /** The recommendation: add capacity, remove capacity, or do nothing. */
  signal: ScalingDecision;
  /** Short English reason code (see `ScalingReason`). */
  reason: ScalingReason;
  /** Human-readable explanation of the decision. */
  reasonDetail: string;
  /** The lag p99 the decision was based on, when available. */
  lagP99Ms?: number;
  /** The trailing-1m publish rate the decision was based on, when available. */
  rate1m?: number;
  /** Bus-clock reading of the evaluation, in milliseconds. */
  at: number;
}

/**
 * Computes the autoscale signal from one set of inputs. Pure and
 * clock-free: the caller passes the evaluation instant as `atMs`.
 * Never throws on missing data — missing inputs yield `hold` with a
 * `no-data` / `no-rate-data` reason.
 */
export function computeScalingSignal(
  input: ScalingSignalInput,
  thresholds: ResolvedAutoscaleThresholds,
  atMs: number,
): ScalingSignal {
  const { lagP99Ms, watermarkMs, rate1m } = input;
  const shared = { lagP99Ms, rate1m, at: atMs };
  if (lagP99Ms === undefined && watermarkMs === undefined) {
    return {
      signal: 'hold',
      reason: 'no-data',
      reasonDetail:
        'no lag or delivery-latency samples: the subscription has neither lagMonitor nor deliveryLatency sampling enabled, or nothing has been delivered yet',
      ...shared,
    };
  }
  // lagP99Ms is defined here unless only a bare watermark exists.
  const effectiveLagMs = lagP99Ms ?? (watermarkMs as number);
  if (effectiveLagMs > thresholds.lagThresholdMs) {
    if (rate1m === undefined) {
      return {
        signal: 'hold',
        reason: 'no-rate-data',
        reasonDetail: `lag p99 ${effectiveLagMs}ms is above the ${thresholds.lagThresholdMs}ms threshold but the topic has no publish-rate history: a sustained load cannot be confirmed`,
        ...shared,
      };
    }
    if (rate1m >= thresholds.rateThreshold) {
      return {
        signal: 'scale_up',
        reason: 'lag-above-threshold',
        reasonDetail: `lag p99 ${effectiveLagMs}ms is above the ${thresholds.lagThresholdMs}ms threshold with a sustained publish rate of ${rate1m}/s: add consumer capacity`,
        ...shared,
      };
    }
    return {
      signal: 'hold',
      reason: 'lag-high-rate-dropping',
      reasonDetail: `lag p99 ${effectiveLagMs}ms is above the ${thresholds.lagThresholdMs}ms threshold but the publish rate already fell to ${rate1m}/s: the backlog is draining on its own`,
      ...shared,
    };
  }
  const watermarkDrained = (watermarkMs ?? 0) <= thresholds.idleLagMs;
  if (
    lagP99Ms !== undefined &&
    lagP99Ms <= thresholds.idleLagMs &&
    watermarkDrained &&
    rate1m !== undefined &&
    rate1m < thresholds.idleRate
  ) {
    return {
      signal: 'scale_down',
      reason: 'drained-idle',
      reasonDetail: `lag p99 ${lagP99Ms}ms drained to the ${thresholds.idleLagMs}ms idle band with a publish rate of ${rate1m}/s: excess consumer capacity`,
      ...shared,
    };
  }
  return {
    signal: 'hold',
    reason: 'lag-normal',
    reasonDetail: `lag p99 ${effectiveLagMs}ms is within its bands: neither overloaded nor idle`,
    ...shared,
  };
}

/**
 * Deterministic numeric encoding of a `ScalingDecision` for the
 * `eventbus_autoscale_signal` Prometheus gauge: `scale_up` = 1,
 * `hold` = 0, `scale_down` = -1.
 */
export const SCALING_SIGNAL_VALUES: Record<ScalingDecision, number> = {
  scale_up: 1,
  hold: 0,
  scale_down: -1,
};

/** One row of the `eventbus_autoscale_signal` gauge series. */
export interface ScalingSignalRow {
  /** The computed decision for this row. */
  signal: ScalingDecision;
  /** Present for per-subscriber rows. */
  subscriberId?: string;
  /** Present for per-topic rows. */
  topic?: string;
}
