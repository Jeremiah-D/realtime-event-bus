/**
 * Rebalance storm protection (EB-169).
 *
 * A consumer group whose members flap — rolling restarts, deploy waves,
 * autoscaler churn — fires one rebalance event per membership change.
 * Each event makes every member re-derive its local assignment state,
 * so a burst of N joins/leaves costs N full re-derivations even though
 * only the final roster matters. The debouncer merges them: the first
 * change inside a quiet period opens a window, further changes are
 * absorbed (counted, not fired), and a single merged rebalance event
 * fires when the window ends, built from the then-current roster.
 *
 * What is deferred and what is not:
 * - Deferred: the `onRebalance` fan-out, the per-rebalance audit entry,
 *   the lag-alert re-evaluation, and the pre-rebalance offset flush —
 *   all of these observe the final state, so running them once per
 *   window is equivalent to running them per change.
 * - NOT deferred: roster mutation, partition-assignment recomputation,
 *   backlog replay to new owners, and handoff-linger windows. Those run
 *   eagerly per change, exactly as without debounce — so the final
 *   assignment after a window is identical to the assignment sequential
 *   rebalances would have produced. Only the event stream is merged.
 *
 * The merged event's identity comes from the latest absorbed change
 * (its `trigger`/`memberId`, and its linger window when the latest
 * change was a leave that opened one); `suppressedRebalances` counts the
 * changes absorbed after the window-opening one.
 *
 * Static membership (EB-104) needs no special case here: a static
 * member's session-takeover reclaim fires no rebalance at all, so it
 * never reaches the debouncer and is never counted as churn. Only real
 * roster changes (join/leave, including static session expiry) are
 * recorded.
 *
 * The window is measured on real wall-clock time (`setTimeout`), not on
 * the bus's injected `now` clock — the same rationale as the lease
 * manager (`src/lease.ts`) and `sessionTimeoutMs`: a debounce window is
 * a liveness bound, it must fire while the bus is completely idle, and a
 * test-injected clock never advances on its own. The timer is `unref()`'d
 * so a pending window never keeps the process alive.
 */

/** One membership change offered to the debouncer. */
export interface DebouncedRebalanceChange {
  /** What triggered the change. */
  trigger: 'join' | 'leave';
  /** The roster key of the member that joined or left. */
  memberId: string;
  /**
   * The handoff-linger window the change opened, if any (leave changes
   * only). Structural twin of the bus's linger summary — kept generic so
   * this module does not import the bus.
   */
  linger?: {
    until: number;
    windows: Array<{ topic: string; fromSeq: number; toSeq: number; until: number }>;
  };
}

/** What the bus receives when a debounce window ends. */
export interface DebouncedRebalanceFlush {
  /** The group the window belonged to. */
  groupId: string;
  /** The topic pattern the competing set is scoped to. */
  pattern: string;
  /**
   * Membership changes absorbed after the window-opening one. The
   * window-opening change itself is not suppressed — its event is the
   * merged one, merely delayed.
   */
  suppressed: number;
  /** The latest absorbed change's trigger — the merged event's trigger. */
  trigger: 'join' | 'leave';
  /** The latest absorbed change's member — the merged event's memberId. */
  memberId: string;
  /** The latest absorbed change's linger window, if any. */
  linger?: DebouncedRebalanceChange['linger'];
}

interface PendingWindow {
  timer: ReturnType<typeof setTimeout>;
  groupId: string;
  pattern: string;
  suppressed: number;
  last: DebouncedRebalanceChange;
}

/**
 * Marks a timer as not keeping the process alive, tolerating timer-like
 * objects without `unref`. Duplicated from the bus rather than imported:
 * the bus imports this module, so importing back would be a cycle.
 */
function unrefTimer(timer: ReturnType<typeof setTimeout>): void {
  const handle = timer as unknown as { unref?: () => unknown };
  if (typeof handle.unref === 'function') handle.unref();
}

/**
 * Per-group rebalance debounce windows. Owned by the bus; the bus
 * configures each group's window (`configure`), offers every membership
 * change (`record`), and emits the merged event when `onFlush` fires.
 */
export class RebalanceDebouncer {
  /** Configured window per group key; absent means no debounce. */
  private readonly windows = new Map<string, number>();
  /** Currently open window per group key. */
  private readonly pending = new Map<string, PendingWindow>();
  /** Called when a window ends so the bus can emit the merged event. */
  private readonly onFlush: (groupKey: string, flush: DebouncedRebalanceFlush) => void;

  constructor(onFlush: (groupKey: string, flush: DebouncedRebalanceFlush) => void) {
    this.onFlush = onFlush;
  }

  /** The configured window for a group, or `undefined` when not debounced. */
  getWindowMs(groupKey: string): number | undefined {
    return this.windows.get(groupKey);
  }

  /**
   * Fixes (or clears) a group's debounce window. Clearing also drops a
   * pending window without flushing — used when a group's generation
   * ends or its configuration is re-fixed.
   */
  configure(groupKey: string, windowMs: number | undefined): void {
    if (windowMs === undefined) this.windows.delete(groupKey);
    else this.windows.set(groupKey, windowMs);
    this.clearGroup(groupKey);
  }

  /**
   * Offers a membership change. Returns `true` when the change was
   * absorbed into a debounce window (the caller must not emit now) —
   * either by opening a fresh window or by being counted as suppressed.
   * Returns `false` when the group is not debounced.
   */
  record(
    groupKey: string,
    groupId: string,
    pattern: string,
    change: DebouncedRebalanceChange,
  ): boolean {
    const windowMs = this.windows.get(groupKey);
    if (windowMs === undefined) return false;
    let pending = this.pending.get(groupKey);
    if (pending === undefined) {
      const timer = setTimeout(() => this.flush(groupKey), windowMs);
      unrefTimer(timer);
      pending = { timer, groupId, pattern, suppressed: 0, last: change };
      this.pending.set(groupKey, pending);
      return true;
    }
    pending.suppressed += 1;
    pending.last = change;
    return true;
  }

  /**
   * Drops a group's pending window without flushing. Called when the
   * group's generation ends (roster fully drained): a window must never
   * fire into the next generation's roster. The membership facts stay in
   * the per-change `group_join`/`group_leave` audit entries.
   */
  clearGroup(groupKey: string): void {
    const pending = this.pending.get(groupKey);
    if (pending === undefined) return;
    clearTimeout(pending.timer);
    this.pending.delete(groupKey);
  }

  /** Drops every pending window without flushing (bus shutdown). */
  close(): void {
    for (const pending of this.pending.values()) clearTimeout(pending.timer);
    this.pending.clear();
  }

  private flush(groupKey: string): void {
    const pending = this.pending.get(groupKey);
    if (pending === undefined) return;
    this.pending.delete(groupKey);
    this.onFlush(groupKey, {
      groupId: pending.groupId,
      pattern: pending.pattern,
      suppressed: pending.suppressed,
      trigger: pending.last.trigger,
      memberId: pending.last.memberId,
      linger: pending.last.linger,
    });
  }
}
