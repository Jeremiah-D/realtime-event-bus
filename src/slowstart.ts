/**
 * Subscriber slow-start (EB-75): cold-start protection for a fresh
 * subscription. After the subscription is created, delivery is paced by a
 * dedicated token gate whose refill rate ramps from `initialPerSec` up to
 * the subscriber's full speed over `rampMs`, so a new (or re-created)
 * downstream is never flooded before it has warmed up. Once the ramp
 * window elapses the gate is dropped and the subscriber runs at full
 * speed (or its `deliveryShaping` cap).
 *
 * The gate composes orthogonally with `deliveryShaping` (EB-21),
 * `rateLimit` (EB-43) and `maxInflight` (EB-67): a message is delivered
 * only when every enabled gate allows it — the effective budget is the
 * minimum. When `deliveryShaping` is configured the ramp targets its
 * `messagesPerSec` cap; without shaping the ramp has no finite ceiling
 * (see `SlowStartGate.rateAt`).
 *
 * Zero dependencies; the bus clock is injected so tests stay
 * deterministic.
 */

/** Ramp curve shape for `SlowStartOptions.strategy`. */
export type SlowStartStrategy = 'linear' | 'exponential';

/**
 * Tuning for subscriber slow-start (`SubscribeOptions.slowStart`). Bounds
 * are validated at `subscribe` time; violations throw `RangeError`.
 */
export interface SlowStartOptions {
  /**
   * Delivery rate at the start of the ramp, messages/sec. Must be a
   * positive finite number. Default 10. The first `initialPerSec`
   * messages may go at once (one second of initial budget, the same
   * burst convention as the shaping token bucket).
   */
  initialPerSec?: number;
  /**
   * How long the ramp lasts, in milliseconds. Must be a positive finite
   * number. Default 1000. When the window elapses the gate is removed
   * and the subscriber runs at full speed.
   */
  rampMs?: number;
  /**
   * Ramp curve: `'linear'` interpolates the rate linearly from
   * `initialPerSec` to the target; `'exponential'` grows it
   * geometrically (doubling every `rampMs` when there is no finite
   * target). Default `'linear'`.
   */
  strategy?: SlowStartStrategy;
}

/** Validated slow-start configuration. */
export interface ResolvedSlowStartOptions {
  initialPerSec: number;
  rampMs: number;
  strategy: SlowStartStrategy;
}

/**
 * Validates `SubscribeOptions.slowStart` and returns the resolved
 * configuration, or `undefined` when slow-start is disabled. `true` takes
 * the documented defaults. Throws `RangeError` for invalid values.
 */
export function resolveSlowStartOptions(
  opt: boolean | SlowStartOptions | undefined,
): ResolvedSlowStartOptions | undefined {
  if (opt == null || opt === false) return undefined;
  const o: SlowStartOptions = opt === true ? {} : opt;
  if (typeof o !== 'object' || o === null) {
    throw new RangeError('slowStart must be a boolean or a SlowStartOptions object');
  }
  const initialPerSec = o.initialPerSec ?? 10;
  if (!Number.isFinite(initialPerSec) || initialPerSec <= 0) {
    throw new RangeError('slowStart.initialPerSec must be a positive finite number of messages/sec');
  }
  const rampMs = o.rampMs ?? 1000;
  if (!Number.isFinite(rampMs) || rampMs <= 0) {
    throw new RangeError('slowStart.rampMs must be a positive finite number of milliseconds');
  }
  const strategy = o.strategy ?? 'linear';
  if (strategy !== 'linear' && strategy !== 'exponential') {
    throw new RangeError("slowStart.strategy must be 'linear' or 'exponential'");
  }
  return { initialPerSec, rampMs, strategy };
}

/**
 * The ramping delivery gate for one subscriber. A fractional token
 * allowance refills lazily at the ramp's instantaneous rate; each
 * delivered message consumes one token. The refill is capped at one
 * second of the current rate, so idle time never banks more than the
 * `TokenBucket` burst convention allows.
 */
export class SlowStartGate {
  private tokens: number;
  private lastRefillMs: number;
  private readonly initialPerSec: number;
  private readonly targetPerSec: number;
  private readonly rampMs: number;
  private readonly strategy: SlowStartStrategy;
  private readonly startedAtMs: number;
  private readonly now: () => number;
  /** Pending re-flush timer, armed while the gate is holding messages back. */
  timer?: ReturnType<typeof setTimeout>;

  constructor(resolved: ResolvedSlowStartOptions, targetPerSec: number, now: () => number) {
    this.initialPerSec = resolved.initialPerSec;
    this.targetPerSec = targetPerSec;
    this.rampMs = resolved.rampMs;
    this.strategy = resolved.strategy;
    this.now = now;
    this.startedAtMs = now();
    this.lastRefillMs = this.startedAtMs;
    // One second of initial budget up front, mirroring the shaping
    // bucket's burst convention.
    this.tokens = resolved.initialPerSec;
  }

  /**
   * The instantaneous allowed delivery rate at `nowMs`, messages/sec.
   * Before the ramp window elapses the rate follows the configured
   * curve from `initialPerSec` toward the target; once the window has
   * elapsed the gate is complete and the target applies. Without a
   * finite target (no `deliveryShaping` cap) the curve keeps growing —
   * linear doubles the rate across the ramp, exponential doubles it
   * every `rampMs` — and the gate opens fully when the window elapses.
   */
  rateAt(nowMs: number): number {
    const elapsed = Math.max(0, nowMs - this.startedAtMs);
    if (elapsed >= this.rampMs) return this.targetPerSec;
    const a = this.initialPerSec;
    const t = elapsed / this.rampMs;
    if (this.strategy === 'linear') {
      if (!Number.isFinite(this.targetPerSec)) return a * (1 + t);
      return a + (this.targetPerSec - a) * t;
    }
    if (!Number.isFinite(this.targetPerSec)) return a * 2 ** t;
    return a * (this.targetPerSec / a) ** t;
  }

  /** True once the ramp window has fully elapsed — the gate is done. */
  isComplete(nowMs: number): boolean {
    return nowMs - this.startedAtMs >= this.rampMs;
  }

  /** Tokens after lazily refilling up to `nowMs`, capped at one second of the current rate. */
  private refilledTokens(nowMs: number): number {
    const elapsedMs = Math.max(0, nowMs - this.lastRefillMs);
    const rate = this.rateAt(nowMs);
    const cap = Math.max(1, rate);
    return Math.min(cap, this.tokens + (elapsedMs * rate) / 1000);
  }

  /** Tokens currently available (fractional), after applying lazy refill. */
  availableTokens(nowMs: number): number {
    return this.refilledTokens(nowMs);
  }

  /**
   * Consumes one token when available. Returns true when the caller may
   * deliver, false when the ramp currently allows nothing. Never consumes
   * a token on a false return.
   */
  take(nowMs: number): boolean {
    this.tokens = this.refilledTokens(nowMs);
    this.lastRefillMs = nowMs;
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return true;
    }
    return false;
  }

  /**
   * Milliseconds until one more whole token is available at the current
   * ramp rate. Returns 0 when at least one token is available now. Used
   * to schedule the next delivery round without polling.
   */
  msUntilNextToken(nowMs: number): number {
    const tokens = this.refilledTokens(nowMs);
    if (tokens >= 1) return 0;
    const rate = this.rateAt(nowMs);
    if (!(rate > 0)) return Number.POSITIVE_INFINITY;
    return ((1 - tokens) * 1000) / rate;
  }
}

/**
 * Builds the slow-start gate for a new subscription, or `undefined` when
 * slow-start is disabled — or degenerate: when `initialPerSec` is already
 * at or above the finite ramp target there is no ramp to run, so the
 * option is a documented no-op instead of a decreasing ramp.
 *
 * `targetPerSec` is the ramp's ceiling: the subscriber's
 * `deliveryShaping` cap when configured, otherwise `undefined` for full
 * speed (no finite ceiling).
 */
export function createSlowStartGate(
  opt: boolean | SlowStartOptions | undefined,
  targetPerSec: number | undefined,
  now: () => number,
): SlowStartGate | undefined {
  const resolved = resolveSlowStartOptions(opt);
  if (resolved === undefined) return undefined;
  const target = targetPerSec ?? Number.POSITIVE_INFINITY;
  if (resolved.initialPerSec >= target) return undefined;
  return new SlowStartGate(resolved, target, now);
}
