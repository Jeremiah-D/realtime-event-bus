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
   * Consumer-lag SLO alerting (EB-112), layered on top of the lag rows:
   * `onLagSloBreach` fires once per SLO breach episode, edge-triggered,
   * after the breach condition has held for `breachSamples` consecutive
   * check samples, and rearms only after the breach clears. Independent
   * of `onGroupLag` — either, both, or neither may be configured.
   */
  slo?: LagSloOptions;
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
  slo?: ResolvedLagSloOptions;
}

/** One consumer-lag SLO breach, as delivered to `LagSloOptions.onLagSloBreach`. */
export interface LagSloBreachEvent extends GroupLagStat {
  /** When the breach fired, on the bus clock. */
  at: number;
  /** Which SLO bound was violated when the breach fired. */
  violated: 'maxLagMessages' | 'maxLagMs' | 'both';
  /** Consecutive breach samples observed when the callback fired. */
  consecutiveSamples: number;
  /** The configured debounce (`LagSloOptions.breachSamples`). */
  breachSamples: number;
}

/**
 * Consumer-lag SLO alerting options (EB-112), configured under
 * `EventBusOptions.groupLag.slo`.
 *
 * At least one of `maxLagMessages` / `maxLagMs` is required; the breach
 * condition is their disjunction:
 * - `maxLagMessages`: breach when a row's message lag exceeds this;
 * - `maxLagMs`: breach when a row stays above zero lag continuously for
 *   at least this long (bus-clock milliseconds) — the consumer has been
 *   behind for the whole window.
 *
 * The condition must then hold for `breachSamples` consecutive check
 * samples (one sample per lag evaluation — on publish, commit, and
 * rebalance) before `onLagSloBreach` fires: the debounce keeps a single
 * slow commit from paging. The callback fires once per breach episode
 * (edge-triggered latch) and rearms only after the breach clears.
 */
export interface LagSloOptions {
  /**
   * Breach when a lag row's message lag exceeds this. Must be a finite
   * number `>= 0`.
   */
  maxLagMessages?: number;
  /**
   * Breach when a lag row's continuous above-zero-lag episode lasts at
   * least this long (bus-clock milliseconds). Must be a finite number
   * `>= 0`; 0 breaches on any positive lag.
   */
  maxLagMs?: number;
  /**
   * Consecutive breach samples required before the callback fires.
   * Must be an integer `>= 1`. Default 3.
   */
  breachSamples?: number;
  /**
   * Fired once when a (group, topic) row's SLO breach is confirmed, and
   * again only after the breach cleared and re-occurred (latch
   * semantics). A snapshot — mutating it does not affect the bus.
   * Throwing inside the callback is swallowed and isolated: monitoring
   * never breaks the publish path.
   */
  onLagSloBreach?: (event: LagSloBreachEvent) => void;
}

export interface ResolvedLagSloOptions {
  maxLagMessages?: number;
  maxLagMs?: number;
  breachSamples: number;
  onLagSloBreach?: (event: LagSloBreachEvent) => void;
}

export const DEFAULT_LAG_SLO_BREACH_SAMPLES = 3;

/**
 * Validates `GroupLagMonitorOptions.slo`, fail-fast. Returns `undefined`
 * when absent — SLO alerting is opt-in on top of the always-on lag rows.
 */
export function resolveLagSloOptions(
  options: LagSloOptions | undefined,
  where: string,
): ResolvedLagSloOptions | undefined {
  if (options === undefined) return undefined;
  if (options === null || typeof options !== 'object') {
    throw new RangeError(`${where}: groupLag.slo must be an options object`);
  }
  const maxLagMessages =
    options.maxLagMessages === undefined
      ? undefined
      : assertValidThreshold(options.maxLagMessages, 'groupLag.slo.maxLagMessages', where);
  const maxLagMs =
    options.maxLagMs === undefined
      ? undefined
      : assertValidThreshold(options.maxLagMs, 'groupLag.slo.maxLagMs', where);
  if (maxLagMessages === undefined && maxLagMs === undefined) {
    throw new RangeError(`${where}: groupLag.slo needs at least one of maxLagMessages / maxLagMs`);
  }
  const breachSamples = options.breachSamples ?? DEFAULT_LAG_SLO_BREACH_SAMPLES;
  if (!Number.isInteger(breachSamples) || breachSamples < 1) {
    throw new RangeError(`${where}: groupLag.slo.breachSamples must be an integer >= 1`);
  }
  const onLagSloBreach = options.onLagSloBreach;
  if (onLagSloBreach !== undefined && typeof onLagSloBreach !== 'function') {
    throw new RangeError(`${where}: groupLag.slo.onLagSloBreach must be a function`);
  }
  return { maxLagMessages, maxLagMs, breachSamples, onLagSloBreach };
}

/** Per-row SLO episode state, keyed by the same latch key as `GroupLagMonitor`. */
interface LagSloRowState {
  /** Edge-triggered alarm: true after the breach fired, until recovery. */
  breached: boolean;
  /** Consecutive samples with the breach condition holding. */
  consecutive: number;
  /**
   * Bus-clock timestamp when the current continuous above-zero-lag
   * episode began; `undefined` while the row has no lag. Drives the
   * `maxLagMs` duration bound.
   */
  episodeStartAt: number | undefined;
}

/**
 * Consumer-lag SLO breach detector (EB-112). The bus feeds it the same
 * already-computed lag rows as `GroupLagMonitor` — one `check` call is
 * one sample — and it applies the SLO's message and duration bounds
 * with a consecutive-sample debounce and an edge-triggered latch.
 */
export class LagSloMonitor {
  /** Episode state per row: present only while the row has lag or an unrecovered breach. */
  private readonly rows = new Map<string, LagSloRowState>();
  /** Total SLO breach firings (edge-triggered count). */
  private breaches = 0;
  private readonly slo: ResolvedLagSloOptions | undefined;

  constructor(slo: ResolvedLagSloOptions | undefined) {
    this.slo = slo;
  }

  /** True when SLO alerting is configured (an `onLagSloBreach` callback exists). */
  enabled(): boolean {
    return this.slo?.onLagSloBreach != null;
  }

  /** Total SLO breach episodes fired so far. */
  breachCount(): number {
    return this.breaches;
  }

  /**
   * Evaluates one sample of lag rows on the bus clock. Tracks each row's
   * continuous above-zero-lag episode for the `maxLagMs` bound, debounces
   * the breach condition over `breachSamples` consecutive samples, fires
   * `onLagSloBreach` once per episode, and rearms on recovery. Callback
   * errors are swallowed: alerting must never break the publish, commit,
   * or rebalance path.
   */
  check(lagRows: GroupLagStat[], nowMs: number): void {
    const slo = this.slo;
    const onLagSloBreach = slo?.onLagSloBreach;
    if (slo == null || onLagSloBreach == null) return;
    for (const row of lagRows) {
      const key = latchKey(row);
      let state = this.rows.get(key);
      if (state === undefined) {
        state = { breached: false, consecutive: 0, episodeStartAt: undefined };
      }
      const hasLag = row.lag > 0;
      if (hasLag) {
        if (state.episodeStartAt === undefined) state.episodeStartAt = nowMs;
      } else {
        state.episodeStartAt = undefined;
      }
      const overMessages = slo.maxLagMessages !== undefined && row.lag > slo.maxLagMessages;
      const overTime =
        slo.maxLagMs !== undefined &&
        state.episodeStartAt !== undefined &&
        nowMs - state.episodeStartAt >= slo.maxLagMs;
      const breach = overMessages || overTime;
      state.consecutive = breach ? state.consecutive + 1 : 0;
      if (state.consecutive >= slo.breachSamples && !state.breached) {
        state.breached = true;
        this.breaches += 1;
        const violated = overMessages && overTime ? 'both' : overMessages ? 'maxLagMessages' : 'maxLagMs';
        try {
          onLagSloBreach({
            ...row,
            at: nowMs,
            violated,
            consecutiveSamples: state.consecutive,
            breachSamples: slo.breachSamples,
          });
        } catch {
          // Monitoring is best-effort: a throwing alert hook must not
          // propagate into the fan-out, commit, or rebalance path.
        }
      }
      // Rearm on recovery: the latch clears only when the breach
      // condition no longer holds.
      if (!breach) state.breached = false;
      if (!hasLag && !state.breached && state.consecutive === 0) {
        this.rows.delete(key);
      } else {
        this.rows.set(key, state);
      }
    }
  }
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
  return {
    defaultThreshold,
    thresholds,
    onGroupLag,
    slo: resolveLagSloOptions(options.slo, where),
  };
}

/** Latch key for one lag row. */
function latchKey(row: GroupLagStat): string {
  return `${row.groupId}\0${row.pattern}\0${row.topic}\0${row.partition ?? ''}`;
}

/**
 * Thresholds plus per-row alarm latch. The bus feeds it already-computed
 * rows; it decides which excursions are new and emits `onGroupLag`. Also
 * owns the SLO breach detector (`LagSloMonitor`), fed the same rows.
 */
export class GroupLagMonitor {
  /** Alarm state per row: present only while the row's lag is above its threshold. */
  private readonly alarmed = new Map<string, boolean>();
  private readonly sloMonitor: LagSloMonitor;
  private resolved: ResolvedGroupLagOptions | undefined;

  constructor(resolved: ResolvedGroupLagOptions | undefined) {
    this.resolved = resolved;
    this.sloMonitor = new LagSloMonitor(resolved?.slo);
  }

  /**
   * True when any alerting is configured (an `onGroupLag` or an
   * `onLagSloBreach` callback exists).
   */
  enabled(): boolean {
    return this.resolved?.onGroupLag != null || this.sloMonitor.enabled();
  }

  /** Total consumer-lag SLO breach episodes fired (see `LagSloOptions`). */
  lagSloBreaches(): number {
    return this.sloMonitor.breachCount();
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
   * Evaluates one batch of lag rows on the bus clock: one threshold
   * sample for `onGroupLag` and one SLO sample for `onLagSloBreach`.
   * Fires `onGroupLag` for each row whose lag crossed above its
   * threshold since the last evaluation and clears the latch for rows
   * back at or below threshold. Callback errors are swallowed:
   * alerting must never break the publish or commit path.
   */
  check(rows: GroupLagStat[], nowMs: number): void {
    const onGroupLag = this.resolved?.onGroupLag;
    if (onGroupLag != null) {
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
    this.sloMonitor.check(rows, nowMs);
  }
}
