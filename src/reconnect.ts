export type ReconnectState = 'idle' | 'connecting' | 'connected' | 'backoff';

/**
 * Pluggable backoff policy. Implement this to fully control how long the
 * controller waits between reconnect attempts (linear, constant, decorrelated
 * jitter, …). A strategy is chosen per controller via `ReconnectOptions.strategy`.
 */
export interface BackoffStrategy {
  /**
   * Delay in ms to wait before attempt `attempt` (1-based, so attempt 1 is the
   * first retry after the initial connection failed). Must be non-negative;
   * the controller schedules the returned value verbatim with setTimeout.
   */
  delayMs(attempt: number): number;
}

export interface ExponentialBackoffOptions {
  /** Base backoff delay in ms; attempt n waits baseDelay * 2^(n-1) plus jitter. */
  baseDelayMs?: number;
  /** Hard cap for the backoff delay in ms. */
  maxDelayMs?: number;
  /** Max random jitter added to each delay in ms. */
  jitterMs?: number;
  /** Random source in [0, 1); injectable so delay sequences are deterministic in tests. */
  random?: () => number;
}

/** The default strategy: exponential backoff with a hard cap and bounded jitter. */
export class ExponentialBackoff implements BackoffStrategy {
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;
  private readonly jitterMs: number;
  private readonly random: () => number;

  constructor(options: ExponentialBackoffOptions = {}) {
    this.baseDelayMs = options.baseDelayMs ?? 1000;
    this.maxDelayMs = options.maxDelayMs ?? 30000;
    this.jitterMs = options.jitterMs ?? 250;
    this.random = options.random ?? Math.random;
  }

  delayMs(attempt: number): number {
    const n = Math.max(1, Math.floor(attempt));
    const exponential = this.baseDelayMs * 2 ** (n - 1);
    const capped = Math.min(exponential, this.maxDelayMs);
    return capped + this.random() * this.jitterMs;
  }
}

/** Live revision of the exponential backoff policy (EB-340): any field may be omitted to keep its live value. */
export interface ReconnectBackoffRevision {
  /** Base backoff delay in ms; attempt n waits baseDelay * 2^(n-1) plus jitter. */
  baseDelayMs?: number;
  /** Hard cap for the backoff delay in ms. */
  maxDelayMs?: number;
  /** Connection attempts before giving up and returning to idle. */
  maxAttempts?: number;
}

export interface ReconnectOptions {
  /** Async connection attempt; injected so tests can fake success/failure. */
  connectFn: () => Promise<void>;
  /**
   * Custom backoff strategy. When provided it takes precedence over the legacy
   * baseDelayMs / maxDelayMs / jitterMs numeric options, which are otherwise
   * used to build the default {@link ExponentialBackoff}.
   */
  strategy?: BackoffStrategy;
  /** Base backoff delay in ms for the default strategy (attempt n waits baseDelay * 2^(n-1) plus jitter). */
  baseDelayMs?: number;
  /** Hard cap for the default strategy's delay in ms. */
  maxDelayMs?: number;
  /** Max random jitter added to each delay in ms for the default strategy. */
  jitterMs?: number;
  /** Connection attempts before giving up and returning to idle. */
  maxAttempts?: number;
  /** Called once maxAttempts is exhausted. */
  onGiveUp?: (attempts: number) => void;
}

/**
 * State machine: idle -> connecting -> connected -> backoff -> connecting ...
 * A failed attempt schedules the next try with exponential backoff + jitter.
 */
export class ReconnectController {
  private state: ReconnectState = 'idle';
  private attempt = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;

  private readonly connectFn: () => Promise<void>;
  private strategy: BackoffStrategy;
  private maxAttempts: number;
  private readonly onGiveUp?: (attempts: number) => void;
  /**
   * Live numeric backoff parameters (EB-340). These describe the
   * default exponential strategy: when the controller was built with a
   * custom `strategy` they are the constructor-provided numbers (which
   * that strategy ignores) until a retune replaces the strategy — see
   * `retuneBackoff`.
   */
  private backoffBaseDelayMs: number;
  private backoffMaxDelayMs: number;

  constructor(options: ReconnectOptions) {
    this.connectFn = options.connectFn;
    this.backoffBaseDelayMs = options.baseDelayMs ?? 1000;
    this.backoffMaxDelayMs = options.maxDelayMs ?? 30000;
    this.strategy =
      options.strategy ??
      new ExponentialBackoff({
        baseDelayMs: this.backoffBaseDelayMs,
        maxDelayMs: this.backoffMaxDelayMs,
        jitterMs: options.jitterMs,
      });
    this.maxAttempts = options.maxAttempts ?? 10;
    this.onGiveUp = options.onGiveUp;
  }

  getState(): ReconnectState {
    return this.state;
  }

  /** Current attempt number (0 when idle or connected). */
  getAttempt(): number {
    return this.attempt;
  }

  /** Delay in ms before attempt `attempt` (1-based); exposed for testability. */
  computeDelayMs(attempt: number): number {
    return this.strategy.delayMs(attempt);
  }

  /**
   * The live reconnect backoff policy (EB-340): `{ baseDelayMs,
   * maxDelayMs, maxAttempts }`. Reads the tracked numeric parameters
   * and the live attempt budget — after a retune this is the revised
   * policy, before one it is the connect-time policy.
   */
  getReconnectBackoff(): { baseDelayMs: number; maxDelayMs: number; maxAttempts: number } {
    return {
      baseDelayMs: this.backoffBaseDelayMs,
      maxDelayMs: this.backoffMaxDelayMs,
      maxAttempts: this.maxAttempts,
    };
  }

  /**
   * Retunes the reconnect backoff policy live (EB-340) without
   * resetting the attempt counter, touching an armed backoff timer,
   * or changing `onGiveUp`: a field left out keeps its live value.
   *
   * Validation reuses the documented connect-time rule — `baseDelayMs`
   * / `maxDelayMs` positive finite numbers, `maxAttempts` a positive
   * integer — and is checked before anything mutates, so an illegal
   * revision throws `RangeError` with the live policy untouched. (The
   * constructor itself never validated these numbers, so this rule is
   * the revision-time contract; connect-time values outside it keep
   * their old unchecked behavior until first revised.)
   *
   * Effect: when `baseDelayMs` or `maxDelayMs` moves, the strategy is
   * rebuilt as a fresh `ExponentialBackoff` at the merged values —
   * which replaces a custom strategy, if one was installed, and resets
   * the jitter to the default 250ms with `Math.random` (jitter is not
   * part of the revision surface). A revision that only moves
   * `maxAttempts` leaves the strategy object — custom or default —
   * exactly as it is. A backoff timer already armed keeps running at
   * the delay it was armed with; the new policy governs from the next
   * `enterBackoff`, which reads the live strategy, and the next
   * failure reads the live `maxAttempts` — so lowering the budget
   * below the current attempt count gives up on the next failure,
   * while raising it extends the runway mid-flight.
   */
  retuneBackoff(revision: ReconnectBackoffRevision): void {
    const baseDelayMs = revision.baseDelayMs ?? this.backoffBaseDelayMs;
    const maxDelayMs = revision.maxDelayMs ?? this.backoffMaxDelayMs;
    const maxAttempts = revision.maxAttempts ?? this.maxAttempts;
    if (!Number.isFinite(baseDelayMs) || baseDelayMs <= 0) {
      throw new RangeError('retuneBackoff: baseDelayMs must be a positive finite number');
    }
    if (!Number.isFinite(maxDelayMs) || maxDelayMs <= 0) {
      throw new RangeError('retuneBackoff: maxDelayMs must be a positive finite number');
    }
    if (!Number.isInteger(maxAttempts) || maxAttempts <= 0) {
      throw new RangeError('retuneBackoff: maxAttempts must be a positive integer');
    }
    if (baseDelayMs !== this.backoffBaseDelayMs || maxDelayMs !== this.backoffMaxDelayMs) {
      this.backoffBaseDelayMs = baseDelayMs;
      this.backoffMaxDelayMs = maxDelayMs;
      this.strategy = new ExponentialBackoff({ baseDelayMs, maxDelayMs });
    }
    this.maxAttempts = maxAttempts;
  }

  start(): void {
    if (this.state !== 'idle') return;
    this.attempt = 0;
    void this.tryConnect();
  }

  stop(): void {
    this.clearTimer();
    this.state = 'idle';
    this.attempt = 0;
  }

  /** Call when the transport drops while connected; enters the backoff flow. */
  notifyDisconnected(): void {
    if (this.state !== 'connected') return;
    this.attempt = 0;
    this.enterBackoff();
  }

  private async tryConnect(): Promise<void> {
    this.state = 'connecting';
    this.attempt += 1;
    try {
      await this.connectFn();
    } catch {
      if (this.attempt >= this.maxAttempts) {
        this.state = 'idle';
        this.attempt = 0;
        this.onGiveUp?.(this.maxAttempts);
        return;
      }
      this.enterBackoff();
      return;
    }
    // Success: the attempt counter resets for the next disconnect cycle.
    this.state = 'connected';
    this.attempt = 0;
  }

  private enterBackoff(): void {
    if (this.state === 'idle') return;
    this.state = 'backoff';
    const delay = this.computeDelayMs(this.attempt + 1);
    this.clearTimer();
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.tryConnect();
    }, delay);
    if (typeof this.timer === 'object' && this.timer != null && 'unref' in this.timer) {
      (this.timer as { unref(): void }).unref();
    }
  }

  private clearTimer(): void {
    if (this.timer != null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}
