/**
 * Per-subscriber redelivery backoff (EB-220).
 *
 * A reliable subscriber's instant redelivery path (`nack()` / ack timeout)
 * historically requeued the message immediately: a downstream that is
 * flapping — up just long enough to accept a delivery, down again before
 * it can process it — gets hammered by its own redeliveries at full
 * speed, and a hot poison message spins the queue. The redelivery
 * backoff is the opt-in brake: within the instant redelivery budget
 * (`DeadLetterOptions.maxRedeliveries`, EB-28) each redelivery waits an
 * exponentially growing delay before it is requeued — first redelivery
 * `baseMs`, then `baseMs * 2`, `baseMs * 4`, … capped at `capMs` — so a
 * struggling downstream gets breathing room between attempts.
 *
 * This module is the bus-agnostic scheduling core: validation of the
 * backoff config and the delay computation, plus a scheduler that arms
 * one delayed requeue per redelivery. The delivery semantics (original
 * seq / TTL deadline preserved, per-message redelivery budget untouched,
 * DLQ entry on budget exhaustion) live in the bus's `onRedeliver` /
 * `fireRedeliveryBackoff` (see `src/bus.ts`), which requeue the
 * identical message object through the normal pipeline so every existing
 * guarantee (TTL expiry at hand-off, FIFO, dedup bypass, fresh ACK
 * envelope per attempt) holds verbatim.
 *
 * Orthogonality with the pre-DLQ retry ladder (EB-155): the backoff
 * spaces redeliveries *inside* the instant budget; the ladder only runs
 * *after* the budget is exhausted, spacing extra chances before the DLQ.
 * A message therefore walks backoff delays first, then ladder rungs, then
 * the DLQ — never both at once, and the backoff never delays a
 * dead-lettering. Composes with the ladder, the auto-replay (EB-134)
 * and the nack-reason counters (EB-129) without touching them.
 *
 * Durability note: the backoff is in-memory by design (see the
 * `redeliveryBackoff` README section). A process restart loses armed
 * delays; the message itself is only ever delayed, never lost — a fired
 * delay is either acked, requeued, expired, or dead-lettered through the
 * normal paths.
 */

/** Cancel handle for one armed backoff delay. */
export interface RedeliveryBackoffTimer {
  /** Cancels the delay before it fires. Idempotent. */
  cancel(): void;
}

/** Timer primitives the scheduler runs on. Injectable for deterministic tests. */
export interface RedeliveryBackoffTimerFactory {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

const defaultTimerFactory: RedeliveryBackoffTimerFactory = {
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

/** Largest delay a JS timer can honor (2^31 - 1 ms); larger computations clamp here. */
const MAX_TIMER_DELAY_MS = 2147483647;

/**
 * User-facing redelivery-backoff config (see
 * `ReliableSubscribeOptions.redeliveryBackoff`).
 */
export interface RedeliveryBackoffConfig {
  /**
   * Delay before the first redelivery, in milliseconds. Every further
   * redelivery doubles it until `capMs` is reached. Must be a positive
   * finite number.
   */
  baseMs: number;
  /**
   * Ceiling for the exponential growth, in milliseconds. Must be a
   * finite number >= `baseMs` when given; absent means uncapped
   * (pure exponential growth, still clamped to the timer maximum).
   */
  capMs?: number;
  /**
   * Full jitter (AWS-style): when true the armed delay is a uniform
   * random draw in [0, computed delay) instead of the computed delay
   * itself, so a fleet of subscribers hammering the same downstream
   * does not re-synchronize. Defaults to false. The randomness comes
   * from the injected `random` source, so tests can pin it.
   */
  jitter?: boolean;
}

/** Normalized config: every field present, `capMs` defaulted, `jitter` defaulted. */
export interface NormalizedRedeliveryBackoffConfig {
  baseMs: number;
  capMs: number;
  jitter: boolean;
}

/** Scheduler options: injectable clock, timers, and randomness. */
export interface RedeliveryBackoffSchedulerOptions {
  /**
   * Clock the scheduler reads for observability stamps (`armedAtMs`).
   * The scheduling itself rides the timer factory, not this clock — it
   * exists so tests can pin the reported timestamps.
   */
  now?: () => number;
  /** Timer primitives. Defaults to the global timers. */
  timers?: RedeliveryBackoffTimerFactory;
  /** Random source for jitter. Defaults to `Math.random`. */
  random?: () => number;
}

/**
 * Validates `ReliableSubscribeOptions.redeliveryBackoff` (EB-220).
 * Returns the normalized config, or `undefined` when the option is
 * absent — absent means "disabled", preserving the pre-EB-220 behavior
 * of requeueing immediately on `nack()` / ack timeout.
 *
 * Throws `RangeError` for a non-object value, a missing or illegal
 * `baseMs` (not a positive finite number), an illegal `capMs` (not a
 * finite number >= `baseMs`), or a non-boolean `jitter`.
 */
export function validateRedeliveryBackoff(
  value: unknown,
  caller: string,
): NormalizedRedeliveryBackoffConfig | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new RangeError(`${caller}: redeliveryBackoff must be an object with baseMs`);
  }
  const { baseMs, capMs, jitter } = value as {
    baseMs?: unknown;
    capMs?: unknown;
    jitter?: unknown;
  };
  if (typeof baseMs !== 'number' || !Number.isFinite(baseMs) || baseMs <= 0) {
    throw new RangeError(`${caller}: redeliveryBackoff.baseMs must be a positive finite number of milliseconds`);
  }
  let cap = Number.POSITIVE_INFINITY;
  if (capMs !== undefined) {
    if (typeof capMs !== 'number' || !Number.isFinite(capMs) || capMs < baseMs) {
      throw new RangeError(
        `${caller}: redeliveryBackoff.capMs must be a finite number >= baseMs when given`,
      );
    }
    cap = capMs;
  }
  if (jitter !== undefined && typeof jitter !== 'boolean') {
    throw new RangeError(`${caller}: redeliveryBackoff.jitter must be a boolean when given`);
  }
  return { baseMs, capMs: cap, jitter: jitter ?? false };
}

/**
 * Per-subscriber redelivery-backoff scheduler. One instance per
 * reliable subscription that opted into `redeliveryBackoff`; the bus
 * keeps the per-message armed state and calls `delayFor` / `schedule`
 * from its redelivery path.
 */
export class RedeliveryBackoff {
  private readonly config: NormalizedRedeliveryBackoffConfig;
  private readonly now: () => number;
  private readonly timers: RedeliveryBackoffTimerFactory;
  private readonly random: () => number;

  constructor(config: NormalizedRedeliveryBackoffConfig, options?: RedeliveryBackoffSchedulerOptions) {
    this.config = config;
    this.now = options?.now ?? Date.now;
    this.timers = options?.timers ?? defaultTimerFactory;
    this.random = options?.random ?? Math.random;
  }

  /**
   * The delay in milliseconds before redelivery attempt `attempt`
   * (1-based: attempt 1 is the first redelivery) is requeued:
   * `min(capMs, baseMs * 2^(attempt - 1))`, clamped to the timer
   * maximum, and — with jitter — a uniform draw in [0, that delay).
   * Attempt numbers below 1 behave like attempt 1 (defensive: the bus
   * only ever passes counts >= 1).
   */
  delayFor(attempt: number): number {
    const n = Math.max(1, Math.floor(attempt));
    const grown = this.config.baseMs * 2 ** (n - 1);
    const capped = Math.min(this.config.capMs, grown, MAX_TIMER_DELAY_MS);
    if (!this.config.jitter) return capped;
    return this.random() * capped;
  }

  /**
   * Arms the delay for redelivery attempt `attempt`: `fire` runs once
   * after the attempt's delay. Returns the armed delay, the armed
   * timestamp (from the injected clock), and a cancel handle;
   * cancelling after the delay fired is a no-op.
   */
  schedule(
    attempt: number,
    fire: () => void,
  ): RedeliveryBackoffTimer & { delayMs: number; armedAtMs: number } {
    const delayMs = this.delayFor(attempt);
    let settled = false;
    const handle = this.timers.setTimeout(() => {
      settled = true;
      fire();
    }, delayMs);
    return {
      delayMs,
      armedAtMs: this.now(),
      cancel: () => {
        if (settled) return;
        settled = true;
        this.timers.clearTimeout(handle);
      },
    };
  }
}
