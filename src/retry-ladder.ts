/**
 * Pre-DLQ delayed retry ladder (EB-155).
 *
 * A reliable subscriber's instant redelivery budget (`DeadLetterOptions.maxRedeliveries`,
 * EB-28) covers messages that fail fast. When the budget is exhausted the bus
 * historically dead-lettered the message immediately — the right call for a true
 * poison message, but wasteful for a downstream that only hiccups: a few seconds
 * of backoff would have let the same message succeed without ever touching the
 * DLQ. The retry ladder is the opt-in middle ground: after the instant budget is
 * spent, the message walks a short ladder of *delayed* single retries — one
 * delivery per rung, spaced by the configured rung delays — and only enters the
 * DLQ when every rung has failed too.
 *
 * This module is the bus-agnostic scheduling core: validation of the rung
 * delays, and an armer that fires one rung after its delay. The delivery
 * semantics (original seq / TTL deadline preserved, per-message redelivery
 * budget untouched, DLQ entry on full failure) live in the bus's
 * `enterRetryLadderOrDeadLetter` / `fireLadderRung` (see `src/bus.ts`), which
 * requeue the identical message object through the normal pipeline so every
 * existing guarantee (TTL expiry at hand-off, FIFO, dedup bypass, fresh ACK
 * envelope per rung) holds verbatim.
 *
 * Durability note: the ladder is in-memory by design (see the `retryLadder`
 * README section). A process restart loses armed rungs; the message itself is
 * only ever lost from the ladder, never from the bus — a rung that fired is
 * either acked, requeued, expired, or dead-lettered through the normal paths.
 */

/** Cancel handle for one armed ladder rung. */
export interface RetryLadderTimer {
  /** Cancels the rung before it fires. Idempotent. */
  cancel(): void;
}

/** Timer primitives the scheduler runs on. Injectable for deterministic tests. */
export interface RetryLadderTimerFactory {
  setTimeout(callback: () => void, delayMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

const defaultTimerFactory: RetryLadderTimerFactory = {
  setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

export interface RetryLadderOptions {
  /**
   * Clock the ladder reads for observability stamps (`armedAtMs`). The
   * scheduling itself rides the timer factory, not this clock — it exists
   * so tests can pin the reported timestamps.
   */
  now?: () => number;
  /** Timer primitives. Defaults to the global timers. */
  timers?: RetryLadderTimerFactory;
}

/**
 * Validates `DeadLetterOptions.retryLadder` (EB-155): an array of per-rung
 * delays in milliseconds, e.g. `[1000, 5000, 30000]`. Returns the (copied)
 * rung delays, or `undefined` when the option is absent or an empty array —
 * both mean "disabled", preserving the pre-ladder behavior of dead-lettering
 * immediately when the instant redelivery budget is exhausted.
 *
 * Throws `RangeError` for a non-array value or for any delay that is not a
 * non-negative finite number (negatives, `NaN`, `Infinity`, non-numbers).
 */
export function validateRetryLadder(ladder: unknown, caller: string): number[] | undefined {
  if (ladder === undefined) return undefined;
  if (!Array.isArray(ladder)) {
    throw new RangeError(`${caller}: deadLetter.retryLadder must be an array of delay milliseconds`);
  }
  const rungs: number[] = [];
  for (const delay of ladder) {
    if (typeof delay !== 'number' || !Number.isFinite(delay) || delay < 0) {
      throw new RangeError(
        `${caller}: deadLetter.retryLadder delays must be non-negative finite numbers of milliseconds`,
      );
    }
    rungs.push(delay);
  }
  return rungs.length === 0 ? undefined : rungs;
}

/**
 * Per-subscriber retry-ladder scheduler. One instance per reliable
 * subscription that opted into `deadLetter.retryLadder`; the bus keeps the
 * per-message rung state (`rung` index + armed timer) and calls `arm` /
 * `delayFor` from its redelivery path.
 */
export class RetryLadder {
  private readonly rungs: number[];
  private readonly now: () => number;
  private readonly timers: RetryLadderTimerFactory;

  constructor(rungs: number[], options?: RetryLadderOptions) {
    this.rungs = [...rungs];
    this.now = options?.now ?? Date.now;
    this.timers = options?.timers ?? defaultTimerFactory;
  }

  /** How many rungs the ladder has. */
  get size(): number {
    return this.rungs.length;
  }

  /** The delay in milliseconds before rung `rung` fires. */
  delayFor(rung: number): number {
    return this.rungs[rung];
  }

  /**
   * Arms rung `rung`: `fire` runs once after the rung's delay. Returns a
   * cancel handle; cancelling after the rung fired is a no-op. The armed
   * timestamp (`armedAtMs`) is exposed for observability and is read from
   * the injected clock.
   */
  arm(rung: number, fire: () => void): RetryLadderTimer & { armedAtMs: number } {
    const delayMs = this.rungs[rung];
    let settled = false;
    const handle = this.timers.setTimeout(() => {
      settled = true;
      fire();
    }, delayMs);
    return {
      armedAtMs: this.now(),
      cancel: () => {
        if (settled) return;
        settled = true;
        this.timers.clearTimeout(handle);
      },
    };
  }
}
