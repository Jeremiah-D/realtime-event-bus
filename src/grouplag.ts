/**
 * Consumer-group lag monitoring (EB-48).
 *
 * Lag is the gap between what a group was *assigned* and what it *consumed*:
 * per (group, topic) `assignedSeq` is the highest per-topic `seq` the bus
 * handed to any member (see `EventBus.getGroupOffsets`), `committedSeq` is
 * the consumer checkpoint reported via `EventBus.commitOffset` (see
 * `EventBus.getCommittedOffsets`). Partitioned groups (see
 * `GroupSubscribeOptions.partitions`) report one row per (partition, topic):
 * the partition's assignment watermark minus the partition checkpoint,
 * falling back to the group-level checkpoint for partitions that were
 * never committed.
 *
 * Live handoff-linger windows (see `GroupSubscribeOptions.handoffLingerMs`)
 * are excluded from lag: seqs a departed member is presumed to still be
 * processing are held out of replay on purpose, so they must not read as
 * consumer lag. They surface separately as `lingerHeldToSeq` so the
 * operator can tell "held for a graceful handoff" apart from "consumer is
 * falling behind".
 *
 * Alerting is opt-in (`EventBusOptions.groupLag`): `onGroupLag` fires once
 * per threshold excursion — when a row's lag first crosses above its
 * threshold — and rearms only after the lag falls back to or below the
 * threshold, exactly the latch semantics of `onBackpressure`. The threshold
 * is configurable per group (`thresholds` / `EventBus.setGroupLagThreshold`).
 */

/** One row of consumer-group lag, as exposed by `EventBus.getStats().groupLag`. */
export interface GroupLagStat {
  /** The consumer group. */
  groupId: string;
  /**
   * The topic pattern of the competing set this row belongs to. Partitioned
   * rows always carry it; classic round-robin rows carry it when the group
   * has exactly one live competing set, otherwise `''` — assignment
   * watermarks (`EventBus.getGroupOffsets`) are keyed by group, not by
   * competing set, so a row cannot be attributed when one group runs
   * several patterns.
   */
  pattern: string;
  /** The concrete topic the row's seqs are scoped to. */
  topic: string;
  /** Present for partitioned groups: the partition this row covers. */
  partition?: number;
  /** Highest per-topic `seq` assigned to any member (partition owner for partitioned groups). */
  assignedSeq: number;
  /** Consumer checkpoint (`EventBus.commitOffset`), 0 when nothing was committed. */
  committedSeq: number;
  /**
   * Highest seq held out of lag accounting by a live handoff-linger
   * window for this (group, topic): the linger covers
   * `(committedSeq, lingerHeldToSeq]`. 0 when no window is live.
   */
  lingerHeldToSeq: number;
  /**
   * `assignedSeq - max(committedSeq, lingerHeldToSeq)`, floored at 0: the
   * messages the group was handed but has not consumed, excluding the
   * lingered handoff backlog.
   */
  lag: number;
}

/** Snapshot delivered to `GroupLagMonitorOptions.onGroupLag` on a threshold excursion. */
export type GroupLagEvent = GroupLagStat;

export interface GroupLagMonitorOptions {
  /**
   * Default lag threshold in messages: `onGroupLag` fires when a row's lag
   * first exceeds this. Must be a finite number `>= 0`; 0 alerts on any
   * positive lag. Default 100.
   */
  thresholdMessages?: number;
  /**
   * Per-group threshold overrides, keyed by groupId. Each value must be a
   * finite number `>= 0`; a groupId present here uses it instead of
   * `thresholdMessages`. Also updatable at runtime via
   * `EventBus.setGroupLagThreshold` / `clearGroupLagThreshold`.
   */
  thresholds?: Record<string, number>;
  /**
   * Fired once when a (group, topic) row's lag crosses above its
   * threshold, and again only after the lag fell back to or below the
   * threshold and crossed again (latch semantics). A snapshot — mutating
   * it does not affect the bus. Throwing inside the callback is swallowed
   * and isolated: monitoring never breaks the publish path.
   */
  onGroupLag?: (event: GroupLagEvent) => void;
}

export interface ResolvedGroupLagOptions {
  defaultThreshold: number;
  thresholds: Map<string, number>;
  onGroupLag?: (event: GroupLagEvent) => void;
}

export const DEFAULT_GROUP_LAG_THRESHOLD = 100;

function assertValidThreshold(value: unknown, what: string, where: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new RangeError(`${where}: ${what} must be a finite number >= 0`);
  }
  return value;
}

/**
 * Validates `EventBusOptions.groupLag`, fail-fast. Returns `undefined`
 * when the option is absent — lag rows are still exposed via
 * `EventBus.getStats().groupLag`; only alerting is opt-in.
 */
export function resolveGroupLagOptions(
  options: GroupLagMonitorOptions | undefined,
  where: string,
): ResolvedGroupLagOptions | undefined {
  if (options === undefined) return undefined;
  if (options === null || typeof options !== 'object') {
    throw new RangeError(`${where}: groupLag must be an options object`);
  }
  const defaultThreshold = assertValidThreshold(
    options.thresholdMessages ?? DEFAULT_GROUP_LAG_THRESHOLD,
    'groupLag.thresholdMessages',
    where,
  );
  const thresholds = new Map<string, number>();
  if (options.thresholds !== undefined) {
    if (options.thresholds === null || typeof options.thresholds !== 'object') {
      throw new RangeError(`${where}: groupLag.thresholds must be an object keyed by groupId`);
    }
    for (const [groupId, value] of Object.entries(options.thresholds)) {
      if (groupId.length === 0) {
        throw new RangeError(`${where}: groupLag.thresholds keys must be non-empty groupIds`);
      }
      thresholds.set(groupId, assertValidThreshold(value, `groupLag.thresholds[${groupId}]`, where));
    }
  }
  const onGroupLag = options.onGroupLag;
  if (onGroupLag !== undefined && typeof onGroupLag !== 'function') {
    throw new RangeError(`${where}: groupLag.onGroupLag must be a function`);
  }
  return { defaultThreshold, thresholds, onGroupLag };
}

/** Latch key for one lag row. */
function latchKey(row: GroupLagStat): string {
  return `${row.groupId}\0${row.pattern}\0${row.topic}\0${row.partition ?? ''}`;
}

/**
 * Thresholds plus per-row alarm latch. The bus feeds it already-computed
 * rows; it decides which excursions are new and emits `onGroupLag`.
 */
export class GroupLagMonitor {
  /** Alarm state per row: present only while the row's lag is above its threshold. */
  private readonly alarmed = new Map<string, boolean>();
  private resolved: ResolvedGroupLagOptions | undefined;

  constructor(resolved: ResolvedGroupLagOptions | undefined) {
    this.resolved = resolved;
  }

  /** True when alerting is configured (an `onGroupLag` callback exists). */
  enabled(): boolean {
    return this.resolved?.onGroupLag != null;
  }

  /** Effective threshold for a group: its override, else the default. */
  thresholdFor(groupId: string): number {
    return this.resolved?.thresholds.get(groupId) ?? this.resolved?.defaultThreshold ?? DEFAULT_GROUP_LAG_THRESHOLD;
  }

  /** Runtime per-group threshold override (see `EventBus.setGroupLagThreshold`). */
  setThreshold(groupId: string, thresholdMessages: number): void {
    if (groupId.length === 0) throw new RangeError('groupId must be a non-empty string');
    const value = assertValidThreshold(thresholdMessages, 'thresholdMessages', 'setGroupLagThreshold');
    if (this.resolved === undefined) {
      this.resolved = { defaultThreshold: DEFAULT_GROUP_LAG_THRESHOLD, thresholds: new Map() };
    }
    this.resolved.thresholds.set(groupId, value);
  }

  /** Drops a runtime per-group threshold override, restoring the default. */
  clearThreshold(groupId: string): void {
    this.resolved?.thresholds.delete(groupId);
  }

  /**
   * Evaluates one batch of lag rows. Fires `onGroupLag` for each row whose
   * lag crossed above its threshold since the last evaluation and clears
   * the latch for rows back at or below threshold. Callback errors are
   * swallowed: alerting must never break the publish or commit path.
   */
  check(rows: GroupLagStat[]): void {
    const onGroupLag = this.resolved?.onGroupLag;
    if (onGroupLag == null) return;
    for (const row of rows) {
      const key = latchKey(row);
      if (row.lag > this.thresholdFor(row.groupId)) {
        if (!this.alarmed.has(key)) {
          this.alarmed.set(key, true);
          try {
            onGroupLag(row);
          } catch {
            // Monitoring is best-effort: a throwing alert hook must not
            // propagate into the fan-out, commit, or rebalance path.
          }
        }
      } else {
        this.alarmed.delete(key);
      }
    }
  }
}
