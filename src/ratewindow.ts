/**
 * Per-subscriber sliding-window delivery rate limiter (EB-43).
 *
 * Answers "how many more deliveries may this subscriber receive right now?"
 * with an exact rolling window: a delivery counts against the budget for a
 * full `perWindowMs` after it happens, then slides out. There is no gradual
 * refill — unlike the token bucket in `src/throttle.ts` (used by
 * `SubscribeOptions.deliveryShaping` to smooth bursts), two quick
 * deliveries still block the window for its whole width. That makes the
 * limiter a hard cap rather than a pacer: pair it with `deliveryShaping`
 * when a downstream needs both smoothed delivery *and* a strict ceiling.
 *
 * Window-edge semantics: a delivery at timestamp `t` counts while
 * `nowMs - t <= perWindowMs` — a delivery exactly `perWindowMs` old still
 * counts, one millisecond older does not. This is the same boundary
 * convention as the publish-rate table in `src/rates.ts`.
 *
 * All methods take the reading explicitly, so the bus passes its injected
 * clock (`EventBusOptions.now`) and the limiter stays deterministic in
 * tests. The stored timestamps are bounded: callers only record when
 * `budget(nowMs) > 0`, so at most `maxMessages` timestamps are ever held.
 * Pruning is a linear filter over that bounded array, which also keeps the
 * limiter correct when the clock moves non-monotonically (no early-exit
 * assumption to corrupt).
 */

/**
 * Exact sliding-window delivery counter with an injectable clock reading.
 * `budget()` reports how many more deliveries the window allows right now;
 * `record()` stamps one delivery; `msUntilBudget()` tells a re-flush timer
 * how long until the oldest delivery slides out of the window.
 */
export class SlidingWindowLimiter {
  private readonly maxMessages: number;
  private readonly perWindowMs: number;
  /** Delivery timestamps inside the trailing window, in delivery order. */
  private deliveries: number[] = [];

  constructor(maxMessages: number, perWindowMs: number) {
    if (!Number.isInteger(maxMessages) || maxMessages < 1) {
      throw new RangeError('maxMessages must be an integer >= 1');
    }
    if (!Number.isFinite(perWindowMs) || perWindowMs <= 0) {
      throw new RangeError('perWindowMs must be a positive finite number of milliseconds');
    }
    this.maxMessages = maxMessages;
    this.perWindowMs = perWindowMs;
  }

  /** Deliveries still inside the trailing window at `nowMs` (prunes first). */
  count(nowMs: number): number {
    this.prune(nowMs);
    return this.deliveries.length;
  }

  /**
   * How many more deliveries the window allows at `nowMs` — never
   * negative. A defensive clamp: callers record only when this is > 0, but
   * a backwards clock can otherwise report a transient over-count.
   */
  budget(nowMs: number): number {
    return Math.max(0, this.maxMessages - this.count(nowMs));
  }

  /**
   * Records one delivery at `nowMs`. The caller must have checked
   * `budget(nowMs) > 0` first; recording beyond budget would break the
   * `deliveries.length <= maxMessages` bound the prune relies on.
   */
  record(nowMs: number): void {
    this.prune(nowMs);
    this.deliveries.push(nowMs);
  }

  /**
   * Milliseconds until the window frees at least one delivery slot at the
   * current clock reading. Returns 0 when budget is available now. Used to
   * schedule the next delivery round for a rate-limited subscriber without
   * polling: the window frees a slot exactly when the oldest delivery
   * slides out.
   */
  msUntilBudget(nowMs: number): number {
    this.prune(nowMs);
    if (this.deliveries.length < this.maxMessages) return 0;
    return Math.max(0, this.deliveries[0] + this.perWindowMs - nowMs);
  }

  /** Drops timestamps that have slid out of the window at `nowMs`. */
  private prune(nowMs: number): void {
    if (this.deliveries.length === 0) return;
    const cutoff = nowMs - this.perWindowMs;
    this.deliveries = this.deliveries.filter((t) => t >= cutoff);
  }
}
