/**
 * Bus-level global publish rate limit (EB-180).
 *
 * A single token bucket shared by every publish on the bus — the
 * backstop under the per-topic (EB-19) and per-publisher (EB-79) rate
 * limits. Where the per-topic limit protects one hot topic and the
 * per-publisher limit protects against one hot producer, the global
 * limit protects the bus itself: a fleet of well-behaved topics whose
 * *aggregate* publish rate would still overwhelm the subscriber fleet
 * (hot-tenant flood protection, real-time systems / backend
 * engineering).
 *
 * The gate runs in `fanOut`'s admission phase as the last pre-admission
 * gate — after every per-topic check (ACL, schema, size caps, sampling,
 * pressure budget, tenant quota), so only publishes that would otherwise
 * be admitted consume global budget — and before sequence-number
 * assignment and the per-topic rate-limit budget (EB-19). A shed publish
 * is never fanned out, never written to the durable log, never queued,
 * consumes no sequence number (subscribers see no gap), and burns no
 * per-topic rate-limit token. It is counted in
 * `BusStats.globalRateLimitedMessages` and surfaced on
 * `EventBusOptions.onAdmissionRejected` with reason
 * `'global-rate-limit'`.
 *
 * Opt-in via `EventBusOptions.globalPublishRateLimit`; absent means
 * unlimited (backward compatible). The clock is the bus's injected
 * `EventBusOptions.now`, so tests drive the bucket deterministically.
 */

import { TokenBucket } from './throttle.ts';

/** Tuning for the bus-level global publish rate limit. */
export interface GlobalPublishRateLimitOptions {
  /**
   * Sustained global publish rate, in messages per second, across every
   * topic on the bus. Must be a positive finite number.
   */
  messagesPerSecond: number;
  /**
   * Burst capacity: how many publishes may pass in one instant before
   * the sustained rate applies. Defaults to
   * `max(1, ceil(messagesPerSecond))`. Must be a positive finite number
   * when given.
   */
  burst?: number;
}

/** Validated global publish rate-limit configuration. */
export interface ResolvedGlobalPublishRateLimit {
  messagesPerSecond: number;
  burst: number;
}

/**
 * Revision for the bus-level global publish rate limit (see
 * `EventBus.setGlobalPublishRateLimit`, EB-301). Only the fields
 * provided move; the rest keep their live values. In particular a
 * partial revision that only moves `messagesPerSecond` keeps the live
 * `burst` — it is *not* recomputed from the new rate's default
 * (`max(1, ceil(messagesPerSecond))`).
 */
export interface GlobalPublishRateLimitRevision {
  /**
   * Sustained global publish rate, in messages per second. Must be a
   * positive finite number when provided — the construction-time rule.
   */
  messagesPerSecond?: number;
  /**
   * Burst capacity: how many publishes may pass in one instant. Must
   * be a positive finite number when provided — the construction-time
   * rule.
   */
  burst?: number;
}

/**
 * Validates `EventBusOptions.globalPublishRateLimit`. Returns
 * `undefined` when unset (unlimited); anything invalid throws
 * `RangeError` at construction time.
 */
export function resolveGlobalPublishRateLimit(
  raw: GlobalPublishRateLimitOptions | undefined,
  where = 'EventBus',
): ResolvedGlobalPublishRateLimit | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new RangeError(`${where}: globalPublishRateLimit must be an object`);
  }
  const { messagesPerSecond, burst } = raw;
  if (!Number.isFinite(messagesPerSecond) || messagesPerSecond <= 0) {
    throw new RangeError(
      `${where}: globalPublishRateLimit.messagesPerSecond must be a positive finite number of messages per second`,
    );
  }
  const resolvedBurst = burst ?? Math.max(1, Math.ceil(messagesPerSecond));
  if (!Number.isFinite(resolvedBurst) || resolvedBurst <= 0) {
    throw new RangeError(
      `${where}: globalPublishRateLimit.burst must be a positive finite number of messages`,
    );
  }
  return { messagesPerSecond, burst: resolvedBurst };
}

/**
 * The bus-wide token bucket. One instance per `EventBus`, created only
 * when the limit is configured — the unset path keeps the publish path
 * to a single `undefined` check.
 */
export class GlobalPublishRateLimiter {
  private readonly bucket: TokenBucket;
  // Live-tuned by `retune` (EB-301): the admission gates read the
  // bucket on every publish, so a revision governs from the next
  // `tryTake` without draining or rebuilding the limiter. The stored
  // token budget is untouched by a retune except for the
  // `TokenBucket.retune` clamp (EB-266).
  private messagesPerSecond: number;
  private burst: number;

  constructor(resolved: ResolvedGlobalPublishRateLimit, now: () => number = Date.now) {
    this.bucket = new TokenBucket(resolved.burst, resolved.messagesPerSecond, now);
    this.messagesPerSecond = resolved.messagesPerSecond;
    this.burst = resolved.burst;
  }

  /** The live rate-limit tuning, for bus-level revision (EB-301). */
  current(): ResolvedGlobalPublishRateLimit {
    return { messagesPerSecond: this.messagesPerSecond, burst: this.burst };
  }

  /**
   * Retunes the live bucket (EB-301). The caller must have validated
   * `next` through `resolveGlobalPublishRateLimit` first — this method
   * applies unconditionally. The stored token budget is kept: a lowered
   * burst clamps the banked tokens to the new burst, a raised burst
   * does not top the bucket up, and the new rate applies from the next
   * refill — see `TokenBucket.retune` (EB-266).
   */
  retune(next: ResolvedGlobalPublishRateLimit): void {
    this.bucket.retune(next.burst, next.messagesPerSecond);
    this.messagesPerSecond = next.messagesPerSecond;
    this.burst = next.burst;
  }

  /**
   * Consumes one global token when available. Returns true when the
   * publish may proceed, false when the bus is over its global budget.
   * Never consumes a token on a false return.
   */
  tryTake(): boolean {
    return this.bucket.take();
  }

  /** Tokens currently available (fractional), after lazy refill. */
  get availableTokens(): number {
    return this.bucket.availableTokens;
  }
}
