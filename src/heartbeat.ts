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
 */
export class HeartbeatMonitor {
  private timer: ReturnType<typeof setInterval> | null = null;
  private pingInFlight = false;
  private missed = 0;

  private readonly controller: ReconnectController;
  private readonly intervalMs: number;
  private readonly timeoutMs: number;
  private readonly pingFn: () => Promise<void>;
  private readonly onPong?: (latencyMs: number) => void;
  private readonly onTimeout?: (missed: number) => void;

  constructor(controller: ReconnectController, options: HeartbeatOptions) {
    this.controller = controller;
    this.intervalMs = options.intervalMs ?? 30_000;
    this.timeoutMs = options.timeoutMs ?? 5_000;
    this.pingFn = options.pingFn;
    this.onPong = options.onPong;
    this.onTimeout = options.onTimeout;
  }

  /** Consecutive missed pongs since the last successful pong. */
  getMissed(): number {
    return this.missed;
  }

  /** Start the ping interval; a second call is a no-op. */
  start(): void {
    if (this.timer != null) return;
    this.timer = setInterval(() => this.tick(), this.intervalMs);
    if (typeof this.timer === 'object' && this.timer != null && 'unref' in this.timer) {
      (this.timer as { unref(): void }).unref();
    }
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

function timeoutReject(ms: number): Promise<never> {
  return new Promise<never>((_, reject) => {
    const id = setTimeout(() => reject(new Error('heartbeat timeout')), ms);
    if (typeof id === 'object' && id != null && 'unref' in id) {
      (id as { unref(): void }).unref();
    }
  });
}
