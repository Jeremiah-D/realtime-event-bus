/**
 * Subscription leases (EB-69).
 *
 * A lease is an opt-in lifetime bound on a subscription
 * (`SubscribeOptions.leaseMs`): when the lease expires, the bus
 * unsubscribes the subscriber automatically — through the exact same
 * unsubscribe path a manual `unsubscribe()` takes, so consumer-group
 * leave handling, reliable-delivery ack-tracker teardown and queue
 * disposal all behave identically. Messages still sitting in the
 * subscriber's queue at expiry are discarded with the queue itself
 * (the subscriber's own `dropPolicy` semantics); no new drop semantic
 * is invented for expiry.
 *
 * The manager owns nothing but timers:
 * - `arm` starts the expiry timer for a subscription; the timer is
 *   `unref()`'d so a pending lease never keeps the process alive.
 * - `renew` cancels the pending timer and starts a fresh one measured
 *   from the renewal moment; it returns `false` for an unknown
 *   subscription id (including subscriptions that never had a lease).
 * - `clear` cancels the timer — called from every unsubscribe path, so
 *   no phantom expiry can fire after the subscriber is gone.
 * - `adoptUnsubscribe` lets the subscribe-family wrappers
 *   (`subscribeReliable`, `subscribeToGroup`) replace the stored
 *   unsubscribe closure with their outermost one, so expiry runs the
 *   full teardown (group leave, ack-tracker clear) exactly once.
 *
 * Expiry fires exactly once per armed lease: the entry is disarmed
 * before the bus-level expiry handler runs, and a stale timer fire
 * (after `clear`/`renew`) is ignored. The bus increments
 * `getStats().expiredLeases` and fires `EventBusOptions.onLeaseExpired`
 * from its handler; the callback is error-isolated there.
 *
 * Leases are measured on real wall-clock time (`setTimeout`), not on
 * the bus's injected `now` clock: expiry must happen even while the
 * bus is completely idle, and a test-injected clock never advances on
 * its own.
 */

/** Snapshot delivered to `EventBusOptions.onLeaseExpired`. */
export interface LeaseExpiredEvent {
  /** Id of the subscription whose lease expired. */
  subscriberId: string;
  /** Topic pattern the subscription was registered for. */
  pattern: string;
  /** The lease duration (ms) that just expired. */
  leaseMs: number;
}

/** Callback type for `EventBusOptions.onLeaseExpired`. */
export type LeaseExpiredCallback = (event: LeaseExpiredEvent) => void;

interface LeaseEntry {
  timer: ReturnType<typeof setTimeout>;
  /** Outermost unsubscribe closure for the subscription. */
  unsubscribe: () => void;
  /** Configured lease duration, surfaced on the expiry event. */
  leaseMs: number;
  /** Topic pattern, surfaced on the expiry event. */
  pattern: string;
}

export class LeaseManager {
  private readonly entries = new Map<string, LeaseEntry>();
  private readonly onExpiry: (expired: { subId: string; leaseMs: number; pattern: string }) => void;

  /**
   * `onExpiry` runs once per expired lease, after the entry is disarmed
   * and the stored unsubscribe closure has run. It must be synchronous
   * and non-throwing with respect to the bus (the bus isolates its own
   * observer callback).
   */
  constructor(
    onExpiry: (expired: { subId: string; leaseMs: number; pattern: string }) => void,
  ) {
    this.onExpiry = onExpiry;
  }

  /** Arms (or re-arms) the lease timer for `subId`. */
  arm(subId: string, leaseMs: number, pattern: string, unsubscribe: () => void): void {
    this.clear(subId);
    const timer = setTimeout(() => this.fire(subId), leaseMs);
    // A lease must never keep the process alive on its own — mirrors the
    // health-probe auto-resume timer's unref convention.
    unrefTimer(timer);
    this.entries.set(subId, { timer, unsubscribe, leaseMs, pattern });
  }

  /**
   * Renews the lease for `subId`, measuring a fresh `leaseMs` window from
   * the renewal moment. Returns `false` when `subId` holds no active
   * lease (unknown id, or a subscription that never opted in).
   */
  renew(subId: string, leaseMs: number): boolean {
    const entry = this.entries.get(subId);
    if (entry === undefined) return false;
    clearTimeout(entry.timer);
    const timer = setTimeout(() => this.fire(subId), leaseMs);
    unrefTimer(timer);
    entry.timer = timer;
    entry.leaseMs = leaseMs;
    return true;
  }

  /**
   * Replaces the stored unsubscribe closure for `subId` with the
   * subscribe family's outermost one. No-op when `subId` holds no lease.
   */
  adoptUnsubscribe(subId: string, unsubscribe: () => void): void {
    const entry = this.entries.get(subId);
    if (entry !== undefined) entry.unsubscribe = unsubscribe;
  }

  /** Cancels the lease timer for `subId`, if any. Idempotent. */
  clear(subId: string): void {
    const entry = this.entries.get(subId);
    if (entry === undefined) return;
    this.entries.delete(subId);
    clearTimeout(entry.timer);
  }

  /** Whether `subId` currently holds an active lease. */
  has(subId: string): boolean {
    return this.entries.has(subId);
  }

  private fire(subId: string): void {
    const entry = this.entries.get(subId);
    // Stale fire: the lease was cleared or renewed since this timer was
    // armed — the current entry (if any) owns its own timer now.
    if (entry === undefined) return;
    this.entries.delete(subId);
    // Unsubscribe first, exactly as a manual unsubscribe: group leave
    // handling, reliable ack-tracker teardown and queue disposal all run
    // before any observer sees the expiry.
    entry.unsubscribe();
    this.onExpiry({ subId, leaseMs: entry.leaseMs, pattern: entry.pattern });
  }
}

function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
  const handle = timer as unknown as { unref?: () => unknown };
  if (typeof handle.unref === 'function') handle.unref();
}
