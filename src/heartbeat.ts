import type { ReconnectController } from './reconnect.ts';

export interface HeartbeatOptions {
  /** How often to send a ping while the connection is up (ms). */
  intervalMs?: number;
  /** Max wait for a pong before the connection is declared dead (ms). */
  timeoutMs?: number;
  /**
   * Transport ping; injected by the caller. Must resolve when the peer
   * answers the pong and may reject on transport-level failure. Tests fake
   * a hung or flaky peer by returning a never-resolving or rejecting promise.
   */
  pingFn: () => Promise<void>;
  /** Called after every successful pong with the observed round-trip latency. */
  onPong?: (latencyMs: number) => void;
  /**
   * Called after every missed pong with the consecutive miss count (1, 2, …).
   * Resets to 0 on the next successful pong.
   */
  onTimeout?: (missed: number) => void;
}

/**
 * Heartbeat / keep-alive monitor for a `ReconnectController`.
 *
 * While the controller reports `connected`, the monitor sends one ping every
 * `intervalMs` through the injected `pingFn`. A pong that does not arrive
 * within `timeoutMs` counts as a missed heartbeat: the miss counter grows,
 * `onTimeout` fires, and the connection is handed back to the reconnect
 * state machine via `controller.notifyDisconnected()` — which moves the
 * controller into `backoff` and drives the normal reconnect flow.
 *
 * Pings are skipped whenever the controller is not `connected` (idle,
 * connecting, backoff), so heartbeats never fire during a reconnect cycle.
 * An in-flight ping blocks the next tick, so a hung peer cannot stack up
 * overlapping pings.
 *
 * The cadence and timeout are revisable live via `retune` (EB-394); see
 * that method. This class has no consumers on the bus — it is a public
 * exported API exercised directly by its tests.
 */
export class HeartbeatMonitor {
  private timer: ReturnType<typeof setInterval> | null = null;
  private pingInFlight = false;
  private missed = 0;

  private readonly controller: ReconnectController;
  // Live-tuned by `retune` (EB-394): `start` arms the timer at the live
  // `intervalMs` and `tick` reads the live `timeoutMs` for each new
  // ping's race, so a revision governs from the next tick / next ping.
  // The miss counter is never moved by a retune.
  private intervalMs: number;
  private timeoutMs: number;
  private readonly pingFn: () => Promise<void>;
  private readonly onPong?: (latencyMs: number) => void;
  private readonly onTimeout?: (missed: number) => void;

  constructor(controller: ReconnectController, options: HeartbeatOptions) {
    this.controller = controller;
    const intervalMs = options.intervalMs ?? 30_000;
    const timeoutMs = options.timeoutMs ?? 5_000;
    assertValidHeartbeatMs(intervalMs, 'intervalMs');
    assertValidHeartbeatMs(timeoutMs, 'timeoutMs');
    this.intervalMs = intervalMs;
    this.timeoutMs = timeoutMs;
    this.pingFn = options.pingFn;
    this.onPong = options.onPong;
    this.onTimeout = options.onTimeout;
  }

  /** Consecutive missed pongs since the last successful pong. */
  getMissed(): number {
    return this.missed;
  }

  /** The live ping cadence, in milliseconds. */
  getIntervalMs(): number {
    return this.intervalMs;
  }

  /** The live pong timeout, in milliseconds. */
  getTimeoutMs(): number {
    return this.timeoutMs;
  }

  /**
   * Retunes the ping cadence and pong timeout live (EB-394). The
   * revision is partial: a field left out keeps its value.
   *
   * A revised `intervalMs` rebuilds the ping timer at the new cadence
   * (the old timer is cleared first, so a tick never fires twice and
   * no tick is lost — the new cadence governs from the next tick). A
   * monitor that has not been started only stores the values for the
   * next `start()`; a stopped monitor stays stopped. A revised
   * `timeoutMs` governs from the next ping's race: an in-flight ping
   * keeps the timeout it was armed with.
   *
   * Fail-closed: validation reuses the construction-time rule (a
   * positive finite number of milliseconds) — an illegal value throws
   * `RangeError` with both live values untouched, even when the other
   * field is legal. Revising to the current values is a no-op (the
   * timer is not rebuilt). The miss counter is never reset.
   */
  retune(next: { intervalMs?: number; timeoutMs?: number }): void {
    // Validate both fields before moving either.
    if (next.intervalMs !== undefined) assertValidHeartbeatMs(next.intervalMs, 'intervalMs');
    if (next.timeoutMs !== undefined) assertValidHeartbeatMs(next.timeoutMs, 'timeoutMs');
    if (next.timeoutMs !== undefined) this.timeoutMs = next.timeoutMs;
    if (next.intervalMs !== undefined && next.intervalMs !== this.intervalMs) {
      this.intervalMs = next.intervalMs;
      if (this.timer != null) {
        clearInterval(this.timer);
        this.timer = this.armTimer();
      }
    }
  }

  /** Start the ping interval; a second call is a no-op. */
  start(): void {
    if (this.timer != null) return;
    this.timer = this.armTimer();
  }

  /**
   * Stop the ping interval. The miss counter keeps its last value for
   * post-mortem inspection (see `getMissed()`); the next successful pong
   * resets it to 0.
   */
  stop(): void {
    if (this.timer != null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.pingInFlight = false;
  }

  /**
   * Arms the ping timer at the live `intervalMs`. Unref'd: a heartbeat
   * must never hold the process open on its own. Every arm goes through
   * here so a retune (EB-394) rebuilds the same timer `start` armed.
   */
  private armTimer(): ReturnType<typeof setInterval> {
    const timer = setInterval(() => this.tick(), this.intervalMs);
    if (typeof timer === 'object' && timer != null && 'unref' in timer) {
      (timer as { unref(): void }).unref();
    }
    return timer;
  }

  private tick(): void {
    // Only heartbeat a live connection; reconnect cycles are left alone.
    if (this.controller.getState() !== 'connected') return;
    // Never stack pings on a hung peer.
    if (this.pingInFlight) return;
    this.pingInFlight = true;
    const sentAt = Date.now();
    Promise.race([this.pingFn(), timeoutReject(this.timeoutMs)]).then(
      () => {
        this.pingInFlight = false;
        this.missed = 0;
        this.onPong?.(Date.now() - sentAt);
      },
      () => {
        this.pingInFlight = false;
        this.missed += 1;
        this.onTimeout?.(this.missed);
        // Hand the dead connection to the reconnect state machine.
        this.controller.notifyDisconnected();
      },
    );
  }
}

/** Shared ping-cadence / pong-timeout validation for the constructor and `retune` (EB-394). */
function assertValidHeartbeatMs(value: unknown, name: string): asserts value is number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    throw new RangeError(`HeartbeatMonitor: ${name} must be a positive finite number of milliseconds`);
  }
}

function timeoutReject(ms: number): Promise<never> {
  return new Promise<never>((_, reject) => {
    const id = setTimeout(() => reject(new Error('heartbeat timeout')), ms);
    if (typeof id === 'object' && id != null && 'unref' in id) {
      (id as { unref(): void }).unref();
    }
  });
}
