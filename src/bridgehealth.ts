/**
 * Bridge transport health monitor (EB-168).
 *
 * The cross-process bridge (EB-51) mirrors every admitted local publish
 * to a caller-owned `BridgeTransport`, and a mirror failure (throw or
 * rejected promise) is swallowed — the bus keeps serving locally. That
 * silence is a feature for the publish path but a blind spot for
 * operators: a dead transport looks exactly like a healthy one until
 * someone notices the peer stopped receiving.
 *
 * This module adds the opt-in health signal. A `BridgeHealthMonitor`
 * watches every mirror outcome (success resets the consecutive-failure
 * counter, failure increments it) and additionally runs a periodic
 * probe — either the transport's own `probe()` when it implements one,
 * or an envelope round-trip: a real probe envelope published through
 * `transport.publish`, where the transport's accept (resolve/return)
 * vs. reject (throw/reject) is the health signal. When consecutive
 * failures reach `failureThreshold` the bridge is marked `unhealthy`
 * and the bus pauses outbound mirroring (local delivery is unaffected;
 * paused mirrors are not queued — mirroring is best-effort by design,
 * and the outage window simply is not mirrored). The next successful
 * probe marks it `healthy` again and mirroring resumes for new
 * publishes — recovery is probe-driven, because a paused bridge never
 * attempts mirrors.
 *
 * Probe envelopes are real envelopes on the `__bridge.probe` topic:
 * peers admit them through the normal path and fan them out to zero
 * subscribers (a `**` subscriber would observe them — the topic name is
 * the contract). With a custom `probe()` no envelope is ever sent.
 *
 * Notes on the probe's honesty: a custom probe measures whatever the
 * transport implements (a broker ping, a socket check). The
 * envelope-round-trip fallback only measures the transport's publish
 * path — a transport that swallows broker failures internally will
 * always look healthy, which is why a custom probe is recommended for
 * production transports.
 *
 * Disabled by default: without `BridgeOptions.health` the bus never
 * constructs a monitor and the mirror path is byte-for-byte the old
 * behavior.
 */
import type { BridgeTransport } from './bridge.ts';

/** Topic of the envelope-round-trip probe (see module docs). */
export const BRIDGE_PROBE_TOPIC = '__bridge.probe';

/** Default interval between health probes (30s). */
export const DEFAULT_BRIDGE_HEALTH_INTERVAL_MS = 30_000;

/** Default consecutive failures before the bridge is marked unhealthy. */
export const DEFAULT_BRIDGE_HEALTH_FAILURE_THRESHOLD = 3;

/** Health state of the bridge transport. */
export type BridgeHealthState = 'healthy' | 'unhealthy';

/** `getStats().bridge.health`: the monitor's observable state. */
export interface BridgeHealthStats {
  /** Current state; `'disabled'` when no monitor is configured. */
  state: BridgeHealthState | 'disabled';
  /** Bus-clock milliseconds of the last probe run, if any probe ran yet. */
  lastProbeAt?: number;
  /** Consecutive mirror/probe failures; reset to 0 on any success. */
  consecutiveFailures: number;
}

/** Tuning for the opt-in bridge health monitor (`BridgeOptions.health`). */
export interface BridgeHealthOptions {
  /**
   * Interval between periodic probes in milliseconds. Must be a
   * positive finite number. Default 30000.
   */
  intervalMs?: number;
  /**
   * Consecutive mirror/probe failures before the bridge is marked
   * unhealthy and outbound mirroring pauses. Must be a positive
   * integer. Default 3.
   */
  failureThreshold?: number;
  /**
   * Clock for probe timestamps. Defaults to `Date.now`; the bus passes
   * its own clock when the option is absent, so bus tests stay
   * deterministic.
   */
  now?: () => number;
  /**
   * Timer injection for tests. Defaults to an unref'd `setTimeout` so
   * the probe never holds the process open.
   */
  setTimer?: (fn: () => void, ms: number) => unknown;
  /** Timer-clear injection for tests. Defaults to `clearTimeout`. */
  clearTimer?: (handle: unknown) => void;
}

/** A health config validated the way the constructor would. */
export interface ResolvedBridgeHealthOptions {
  intervalMs: number;
  failureThreshold: number;
  now?: () => number;
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
}

/**
 * Partial revision for `EventBus.setBridgeHealth` (EB-305). Only the
 * fields provided move; `now` / `setTimer` / `clearTimer` are fixed at
 * construction and are not revisable here.
 */
export interface BridgeHealthRevision {
  intervalMs?: number;
  failureThreshold?: number;
}

/**
 * Validates `BridgeOptions.health` the way the constructor would:
 * throws `RangeError` on the first problem — a broken health config
 * fails at construction time, never mid-probe. Returns `undefined`
 * when no health monitoring was configured.
 */
export function resolveBridgeHealthOptions(
  options: BridgeHealthOptions | undefined,
  where: string,
): ResolvedBridgeHealthOptions | undefined {
  if (options === undefined) return undefined;
  if (typeof options !== 'object' || options === null || Array.isArray(options)) {
    throw new RangeError(`${where}: bridge.health must be an object`);
  }
  const { intervalMs, failureThreshold, now, setTimer, clearTimer } = options;
  const interval = intervalMs ?? DEFAULT_BRIDGE_HEALTH_INTERVAL_MS;
  if (!Number.isFinite(interval) || interval <= 0) {
    throw new RangeError(
      `${where}: bridge.health.intervalMs must be a positive finite number of milliseconds`,
    );
  }
  const threshold = failureThreshold ?? DEFAULT_BRIDGE_HEALTH_FAILURE_THRESHOLD;
  if (!Number.isInteger(threshold) || threshold < 1) {
    throw new RangeError(`${where}: bridge.health.failureThreshold must be a positive integer`);
  }
  if (now !== undefined && typeof now !== 'function') {
    throw new RangeError(`${where}: bridge.health.now must be a function`);
  }
  if (setTimer !== undefined && typeof setTimer !== 'function') {
    throw new RangeError(`${where}: bridge.health.setTimer must be a function`);
  }
  if (clearTimer !== undefined && typeof clearTimer !== 'function') {
    throw new RangeError(`${where}: bridge.health.clearTimer must be a function`);
  }
  return { intervalMs: interval, failureThreshold: threshold, now, setTimer, clearTimer };
}

function defaultSetTimer(fn: () => void, ms: number): unknown {
  const timer = setTimeout(fn, ms);
  // Never hold the process open for a health probe; the bus stops the
  // monitor on close().
  if (typeof timer.unref === 'function') timer.unref();
  return timer;
}

function defaultClearTimer(handle: unknown): void {
  clearTimeout(handle as ReturnType<typeof setTimeout>);
}

export interface BridgeHealthMonitorCallbacks {
  /** Fired once when the bridge transitions healthy -> unhealthy. */
  onUnhealthy: () => void;
  /** Fired once when the bridge transitions unhealthy -> healthy. */
  onRecovered: () => void;
}

/**
 * Owns the bridge health state machine, the periodic probe, and the
 * mirror-outcome accounting. One instance per `EventBus`, created only
 * when `BridgeOptions.health` is configured.
 *
 * All callbacks are invoked synchronously from the state transitions;
 * the bus keeps them error-isolated (audit emission never throws).
 */
export class BridgeHealthMonitor {
  private readonly transport: BridgeTransport;
  // Live-tuned by `retune` (EB-305): the probe schedule reads
  // `intervalMs` on every (re-)arm and `recordFailure` reads
  // `failureThreshold` on every failure, so a revision governs from
  // the next probe / failure without resetting any health state.
  // `now` / `setTimer` / `clearTimer` stay fixed for the monitor's
  // lifetime.
  private intervalMs: number;
  private failureThreshold: number;
  private readonly now: () => number;
  private readonly setTimer: (fn: () => void, ms: number) => unknown;
  private readonly clearTimer: (handle: unknown) => void;
  private readonly callbacks: BridgeHealthMonitorCallbacks;

  private state: BridgeHealthState = 'healthy';
  private consecutiveFailures = 0;
  private lastProbeAt: number | undefined;
  private timer: unknown = undefined;
  private stopped = false;

  constructor(
    transport: BridgeTransport,
    health: ResolvedBridgeHealthOptions,
    now: () => number,
    callbacks: BridgeHealthMonitorCallbacks,
  ) {
    this.transport = transport;
    this.intervalMs = health.intervalMs;
    this.failureThreshold = health.failureThreshold;
    this.now = health.now ?? now;
    this.setTimer = health.setTimer ?? defaultSetTimer;
    this.clearTimer = health.clearTimer ?? defaultClearTimer;
    this.callbacks = callbacks;
  }

  /** Arms one probe tick at the live interval. */
  private armTimer(): void {
    const tick = (): void => {
      this.timer = undefined;
      if (this.stopped) return;
      // The probe never rejects (it records its own outcome); the
      // `finally` re-arms the schedule so one slow probe never stacks
      // another behind it.
      void this.probeOnce().finally(() => {
        if (!this.stopped) this.armTimer();
      });
    };
    this.timer = this.setTimer(tick, this.intervalMs);
  }

  /** Starts periodic probing; idempotent. The first probe fires after one full interval. */
  start(): void {
    if (this.timer !== undefined || this.stopped) return;
    this.armTimer();
  }

  /** The two live tuning values, for bus-level revision (EB-305). */
  current(): { intervalMs: number; failureThreshold: number } {
    return { intervalMs: this.intervalMs, failureThreshold: this.failureThreshold };
  }

  /**
   * Retunes the probe interval and failure threshold live (EB-305).
   * The caller must have validated `next` through
   * `resolveBridgeHealthOptions` first — this method applies
   * unconditionally. Health state is untouched: the consecutive-failure
   * counter, the last-probe timestamp, and the healthy/unhealthy state
   * all survive, so a tightened threshold trips on the next failure
   * past it (a counter already at or above the new threshold trips on
   * the next failure, not retroactively) and a loosened one simply
   * needs more failures from here on. When a probe timer is currently
   * armed it is cleared and re-armed at the new interval — no extra
   * probe fires; a probe already in flight re-arms itself at the new
   * interval from its `finally`, and a stopped monitor stays stopped.
   * A threshold-only revision leaves the pending probe's countdown
   * untouched — only a moved interval re-arms the schedule.
   */
  retune(next: { intervalMs: number; failureThreshold: number }): void {
    const intervalChanged = next.intervalMs !== this.intervalMs;
    this.intervalMs = next.intervalMs;
    this.failureThreshold = next.failureThreshold;
    // Only a moved interval re-arms the schedule: a threshold-only
    // revision leaves the pending probe's countdown untouched.
    if (intervalChanged && this.timer !== undefined && !this.stopped) {
      try {
        this.clearTimer(this.timer);
      } catch {
        // A broken timer injection must never break the retune path.
      }
      this.timer = undefined;
      this.armTimer();
    }
  }

  /** Stops periodic probing; idempotent. In-flight probes still record their outcome. */
  stop(): void {
    this.stopped = true;
    if (this.timer !== undefined) {
      try {
        this.clearTimer(this.timer);
      } catch {
        // A broken timer injection must never break the bus close path.
      }
      this.timer = undefined;
    }
  }

  /**
   * Runs one probe and records its outcome. Uses the transport's custom
   * `probe()` when present; otherwise publishes a probe envelope and
   * treats the transport's accept/reject as the signal (see module
   * docs). Never rejects.
   */
  async probeOnce(): Promise<void> {
    this.lastProbeAt = this.now();
    try {
      const customProbe = this.transport.probe;
      if (customProbe !== undefined) {
        const ok = await customProbe();
        if (ok) this.recordSuccess();
        else this.recordFailure();
        return;
      }
      const result = this.transport.publish({
        topic: BRIDGE_PROBE_TOPIC,
        payload: { probe: true, at: this.lastProbeAt },
      });
      if (result !== undefined && typeof (result as Promise<void>).then === 'function') {
        await result;
      }
      this.recordSuccess();
    } catch {
      this.recordFailure();
    }
  }

  /** Records one successful mirror (or probe): resets the failure counter, recovers if unhealthy. */
  recordSuccess(): void {
    this.consecutiveFailures = 0;
    if (this.state === 'unhealthy') {
      this.state = 'healthy';
      this.callbacks.onRecovered();
    }
  }

  /** Records one failed mirror (or probe): trips to unhealthy at the threshold. */
  recordFailure(): void {
    this.consecutiveFailures += 1;
    if (this.state === 'healthy' && this.consecutiveFailures >= this.failureThreshold) {
      this.state = 'unhealthy';
      this.callbacks.onUnhealthy();
    }
  }

  /** Whether outbound mirroring may proceed (false while unhealthy). */
  healthy(): boolean {
    return this.state === 'healthy';
  }

  /** Observable monitor state for `getStats().bridge.health`. */
  getStats(): BridgeHealthStats {
    return {
      state: this.state,
      ...(this.lastProbeAt === undefined ? {} : { lastProbeAt: this.lastProbeAt }),
      consecutiveFailures: this.consecutiveFailures,
    };
  }
}
