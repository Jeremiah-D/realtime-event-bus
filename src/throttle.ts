/**
 * Per-subscriber publish-side rate limiting for the event bus.
 *
 * When a subscriber's queue crosses its high-water mark, the bus engages
 * adaptive throttling for that subscriber: a token bucket caps how many
 * freshly published messages are fanned out to it per second. Messages
 * that arrive with no token available are shed at the publish side (never
 * queued) and counted, so a slow consumer degrades to a sustainable rate
 * instead of churning its queue through the drop policy on every publish.
 * When the queue drains back below the mark, throttling disengages and the
 * subscriber resumes full speed; the measured drain rate seeds the next
 * engagement, so the throttle converges on the consumer's real speed.
 */

/**
 * Token bucket with lazy refill and an injectable clock. `take()` returns
 * true and consumes one token when at least one whole token is available,
 * false otherwise. Fractional tokens accumulate across calls.
 */
export class TokenBucket {
  private tokens: number;
  private lastRefillMs: number;
  private readonly capacityTokens: number;
  private readonly refillTokensPerMs: number;
  private readonly now: () => number;

  constructor(capacityTokens: number, refillTokensPerSec: number, now: () => number = Date.now) {
    if (!Number.isFinite(capacityTokens) || capacityTokens <= 0) {
      throw new RangeError('capacityTokens must be a positive finite number');
    }
    if (!Number.isFinite(refillTokensPerSec) || refillTokensPerSec <= 0) {
      throw new RangeError('refillTokensPerSec must be a positive finite number');
    }
    this.tokens = capacityTokens;
    this.capacityTokens = capacityTokens;
    this.refillTokensPerMs = refillTokensPerSec / 1000;
    this.now = now;
    this.lastRefillMs = now();
  }

  /** Tokens currently available (fractional), after applying lazy refill. */
  get availableTokens(): number {
    return this.refilledTokens(this.now());
  }

  /**
   * Milliseconds until one more whole token is available at the current
   * refill rate. Returns 0 when at least one token is available now. Used
   * to schedule the next delivery round for a rate-shaped subscriber
   * without polling.
   */
  msUntilNextToken(): number {
    const nowMs = this.now();
    const tokens = this.refilledTokens(nowMs);
    if (tokens >= 1) return 0;
    return (1 - tokens) / this.refillTokensPerMs;
  }

  /**
   * Consumes one token when available. Returns true when the caller may
   * proceed, false when the caller is over budget. Never consumes a token
   * on a false return. Refills are capped at the bucket capacity, so idle
   * time never banks more than one burst.
   */
  take(): boolean {
    const nowMs = this.now();
    this.tokens = this.refilledTokens(nowMs);
    this.lastRefillMs = nowMs;
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return true;
    }
    return false;
  }

  /** Tokens after lazily refilling up to `nowMs`, capped at capacity. */
  private refilledTokens(nowMs: number): number {
    const elapsedMs = Math.max(0, nowMs - this.lastRefillMs);
    return Math.min(this.capacityTokens, this.tokens + elapsedMs * this.refillTokensPerMs);
  }
}
