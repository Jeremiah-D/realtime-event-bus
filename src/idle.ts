/**
 * Idle subscriber auto-reclamation (EB-72).
 *
 * The complement of the fixed-duration lease (EB-69, `src/lease.ts`): an
 * opt-in inactivity bound on a subscription (`SubscribeOptions.maxIdleMs`).
 * Every qualifying activity re-arms the timer from the activity moment —
 * a busy subscriber never expires while it is active — and when the timer
 * fires, the bus unsubscribes the subscriber automatically, through the
 * exact same unsubscribe path a manual `unsubscribe()` takes, so
 * consumer-group leave handling, reliable-delivery ack-tracker teardown
 * and queue disposal all behave identically. Messages still sitting in
 * the subscriber's queue at expiry are discarded with the queue itself
 * (the subscriber's own `dropPolicy` semantics); no new drop semantic
 * is invented for expiry.
 *
 * Which events count as activity is configurable per subscription
 * (`SubscribeOptions.idleActivity`, default `'any'`):
 * - `'deliver'`: the handler returned without throwing. Dropped, expired
 *   or shed messages never reach the handler and do not count; a
 *   throwing handler is a failed delivery and does not count either.
 * - `'ack'`: `ack()` settled a live delivery (reliable subscriptions,
 *   EB-11). `nack()`, ack timeouts, and no-op acks on already-settled or
 *   stale handles do not count.
 * - `'any'`: either of the above.
 *
 * The manager owns nothing but timers:
 * - `arm` starts the expiry timer for a subscription; the timer is
 *   `unref()`'d so a pending idle window never keeps the process alive.
 * - `activity` re-arms the timer from the activity moment; it returns
 *   `false` for an unknown subscription id (including subscriptions that
 *   never opted in). The bus decides which events qualify before calling
 *   it, so the manager never sees non-qualifying traffic.
 * - `clear` cancels the timer — called from every unsubscribe path, so
 *   no phantom expiry can fire after the subscriber is gone.
 * - `adoptUnsubscribe` lets the subscribe-family wrappers
 *   (`subscribeReliable`, `subscribeToGroup`) replace the stored
 *   unsubscribe closure with their outermost one, so expiry runs the
 *   full teardown (group leave, ack-tracker clear) exactly once.
 *
 * Expiry fires exactly once per armed window: the entry is disarmed
 * before the bus-level expiry handler runs, and a stale timer fire
 * (after `clear`/`activity`) is ignored. The bus increments
 * `getStats().idleExpired` and fires `EventBusOptions.onIdleExpired`
 * from its handler; the callback is error-isolated there.
 *
 * Idle windows are measured on real wall-clock time (`setTimeout`), not
 * on the bus's injected `now` clock: expiry must happen even while the
 * bus is completely idle, and a test-injected clock never advances on
 * its own. A subscriber that holds both a lease (`leaseMs`) and an idle
 * window (`maxIdleMs`) arms both independently — whichever fires first
 * wins, and the other is cleared by the shared unsubscribe path.
 */

/** Which subscriber events re-arm the idle timer. */
export type IdleActivityKind = 'deliver' | 'ack';

/** The `SubscribeOptions.idleActivity` mode: which kinds count. */
export type IdleActivityMode = IdleActivityKind | 'any';

/** Snapshot delivered to `EventBusOptions.onIdleExpired`. */
export interface IdleExpiredEvent {
  /** Id of the subscription that went idle. */
  subscriberId: string;
  /** Topic pattern the subscription was registered for. */
  pattern: string;
  /** The idle threshold (ms) that was exceeded. */
  maxIdleMs: number;
  /** The activity mode that was in effect. */
  idleActivity: IdleActivityMode;
}

/** Callback type for `EventBusOptions.onIdleExpired`. */
export type IdleExpiredCallback = (event: IdleExpiredEvent) => void;

interface IdleEntry {
  timer: ReturnType<typeof setTimeout>;
  /** Outermost unsubscribe closure for the subscription. */
  unsubscribe: () => void;
  /** Configured idle threshold, surfaced on the expiry event. */
  maxIdleMs: number;
  /** Topic pattern, surfaced on the expiry event. */
  pattern: string;
  /** Activity mode, surfaced on the expiry event. */
  idleActivity: IdleActivityMode;
}

export class IdleReclaimer {
  private readonly entries = new Map<string, IdleEntry>();
  private readonly onExpiry: (expired: {
    subId: string;
    maxIdleMs: number;
    pattern: string;
    idleActivity: IdleActivityMode;
  }) => void;

  /**
   * `onExpiry` runs once per idle expiry, after the entry is disarmed
   * and the stored unsubscribe closure has run. It must be synchronous
   * and non-throwing with respect to the bus (the bus isolates its own
   * observer callback).
   */
  constructor(
    onExpiry: (expired: {
      subId: string;
      maxIdleMs: number;
      pattern: string;
      idleActivity: IdleActivityMode;
    }) => void,
  ) {
    this.onExpiry = onExpiry;
  }

  /** Arms the idle timer for `subId`, measuring from the subscribe moment. */
  arm(
    subId: string,
    maxIdleMs: number,
    idleActivity: IdleActivityMode,
    pattern: string,
    unsubscribe: () => void,
  ): void {
    this.clear(subId);
    const timer = setTimeout(() => this.fire(subId), maxIdleMs);
    // An idle window must never keep the process alive on its own —
    // mirrors the lease timer's unref convention.
    unrefTimer(timer);
    this.entries.set(subId, { timer, unsubscribe, maxIdleMs, pattern, idleActivity });
  }

  /**
   * Records qualifying activity for `subId`: the pending timer is
   * cancelled and a fresh `maxIdleMs` window starts from the activity
   * moment. Returns `false` when `subId` holds no active idle window
   * (unknown id, or a subscription that never opted in).
   */
  activity(subId: string): boolean {
    const entry = this.entries.get(subId);
    if (entry === undefined) return false;
    clearTimeout(entry.timer);
    const timer = setTimeout(() => this.fire(subId), entry.maxIdleMs);
    unrefTimer(timer);
    entry.timer = timer;
    return true;
  }

  /**
   * Replaces the stored unsubscribe closure for `subId` with the
   * subscribe family's outermost one. No-op when `subId` holds no idle
   * window.
   */
  adoptUnsubscribe(subId: string, unsubscribe: () => void): void {
    const entry = this.entries.get(subId);
    if (entry !== undefined) entry.unsubscribe = unsubscribe;
  }

  /** Cancels the idle timer for `subId`, if any. Idempotent. */
  clear(subId: string): void {
    const entry = this.entries.get(subId);
    if (entry === undefined) return;
    this.entries.delete(subId);
    clearTimeout(entry.timer);
  }

  /** Whether `subId` currently holds an active idle window. */
  has(subId: string): boolean {
    return this.entries.has(subId);
  }

  private fire(subId: string): void {
    const entry = this.entries.get(subId);
    // Stale fire: the window was cleared or re-armed since this timer
    // was scheduled — the current entry (if any) owns its own timer now.
    if (entry === undefined) return;
    this.entries.delete(subId);
    // Unsubscribe first, exactly as a manual unsubscribe: group leave
    // handling, reliable ack-tracker teardown and queue disposal all run
    // before any observer sees the expiry.
    entry.unsubscribe();
    this.onExpiry({
      subId,
      maxIdleMs: entry.maxIdleMs,
      pattern: entry.pattern,
      idleActivity: entry.idleActivity,
    });
  }
}

function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
  const handle = timer as unknown as { unref?: () => unknown };
  if (typeof handle.unref === 'function') handle.unref();
}
