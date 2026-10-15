/**
 * TTL expiring-soon warnings (EB-110).
 *
 * Where `SubscribeOptions.deliveryLatency` (see `src/latency.ts`) records a
 * *historical* distribution of per-delivery queue dwells and the lag
 * monitor (see `src/lag.ts`) watches how long the queue head has been
 * *waiting*, the expiring-soon monitor watches how much *life* the queue
 * head has left: it warns the operator that the oldest queued message is
 * about to expire under its TTL (EB-10) — typically because the consumer
 * is not keeping up — so an intervention (scale the consumer, raise the
 * TTL, shed the backlog) can happen before the messages are silently
 * discarded as `expired`.
 *
 * - **Live reading**: the head message's remaining TTL,
 *   `deadline(head) - now`, read at `getStats()` time from the bus clock.
 *   Messages without a TTL never warn (`Infinity` remaining). 0 (or
 *   negative, if the deadline already passed) when the head is already
 *   expired — the warning fires for those too: an expired head is the
 *   loudest possible signal that the consumer is behind.
 * - **Threshold alerting**: `onExpiringSoon` fires when the head's
 *   remaining TTL is at or below `expiringSoonMs`. It fires once per
 *   excursion and re-arms when the queue drains empty (`alerted` latch —
 *   note this differs from the lag monitor, which re-arms when the
 *   watermark drops below its threshold: here a fresh head entering the
 *   window while the queue never drained does not re-fire, by design).
 * - **Purely read-only**: the warning never changes delivery or TTL
 *   semantics — it observes, it does not intervene. The message still
 *   delivers if the consumer catches up, still expires if it does not.
 * - **Error isolation**: a throwing `onExpiringSoon` is swallowed — a
 *   monitoring hook must never break the publish or drain path.
 *
 * The remaining TTL is evaluated on every enqueue and after every drain,
 * so a crossing is observed on the next bus activity after it happens — a
 * completely idle bus cannot observe its own deadlines approaching, which
 * is inherent to a clock-read design with no background timer (the bus
 * never keeps the process alive for observability alone).
 *
 * Sampling is opt-in per subscriber and costs one queue-head peek plus one
 * WeakMap lookup per evaluation for opted-in subscribers only. The clock
 * is the bus's own injected clock (`EventBusOptions.now`), so tests drive
 * deterministic warnings by advancing a controllable clock.
 */

/** Tuning for TTL expiring-soon warnings. */
export interface ExpiringSoonOptions {
  /**
   * Warn when the oldest queued message's remaining TTL is at or below
   * this many milliseconds. Must be a non-negative finite number.
   */
  expiringSoonMs: number;
  /**
   * Called once when the head message's remaining TTL enters the warning
   * window; re-arms after the queue drains empty. A throwing callback is
   * swallowed (error-isolated) so monitoring can never break the
   * publish/drain path.
   */
  onExpiringSoon: (event: ExpiringSoonEvent) => void;
}

/** Fired when a subscriber's queue head enters the TTL warning window. */
export interface ExpiringSoonEvent {
  /** The warning subscription's id. */
  subscriberId: string;
  /** The pattern the subscription was registered with. */
  pattern: string;
  /**
   * The head message's remaining TTL in milliseconds. May be 0 or
   * negative when the head already passed its deadline — still a warning,
   * never a delivery.
   */
  remainingMs: number;
  /** The configured warning threshold, in milliseconds. */
  expiringSoonMs: number;
  /** Queue size when the warning fired. */
  queueDepth: number;
  /** Bus-clock reading of the warning, in milliseconds. */
  at: number;
}

/** Per-subscriber expiring-soon monitoring state. */
export interface SubscriberExpiringState {
  expiringSoonMs: number;
  onExpiringSoon: (event: ExpiringSoonEvent) => void;
  /**
   * Latch: set when the warning fires, cleared when the queue drains
   * empty. While set, further evaluations do not re-fire — one warning
   * per backlog, even as the head turns over.
   */
  alerted: boolean;
}

/**
 * Validates `SubscribeOptions.expiringSoon` and builds the initial
 * per-subscriber monitoring state. Returns `undefined` when monitoring is
 * disabled. Throws `TypeError` for a non-object option or a non-function
 * callback, `RangeError` for a negative/non-finite threshold — fail-fast
 * at subscribe time, never mid-drain.
 */
export function resolveExpiringSoonOptions(
  opt: ExpiringSoonOptions | undefined,
  where: string,
): SubscriberExpiringState | undefined {
  if (opt === undefined) return undefined;
  if (typeof opt !== 'object' || opt === null) {
    throw new TypeError(`${where}: expiringSoon must be an ExpiringSoonOptions object`);
  }
  const { expiringSoonMs, onExpiringSoon } = opt;
  if (!Number.isFinite(expiringSoonMs) || expiringSoonMs < 0) {
    throw new RangeError(
      `${where}: expiringSoon.expiringSoonMs must be a non-negative finite number of milliseconds`,
    );
  }
  if (typeof onExpiringSoon !== 'function') {
    throw new TypeError(`${where}: expiringSoon.onExpiringSoon must be a function`);
  }
  return { expiringSoonMs, onExpiringSoon, alerted: false };
}
