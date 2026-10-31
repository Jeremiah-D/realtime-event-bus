/**
 * Doctor auto-remediation (EB-211).
 *
 * `diagnoseBusHealth` (EB-101) is deliberately read-only: it folds one
 * snapshot into a graded report and never touches bus state. This module
 * adds the opt-in remediation layer on top of that diagnosis — the bus
 * method `EventBus.autoRemediate()` runs the diagnosis first, then
 * derives a bounded list of *safe, reversible* repair actions from the
 * findings and applies them, each one audited:
 *
 * - `resume-subscriber`: a health-probe-degraded subscriber (EB-18)
 *   whose probe has no auto-resume cooldown armed is stuck degraded
 *   forever without operator intervention — resuming it is the only
 *   recovery path, and it is safe by construction: `resume()` only
 *   clears the degraded flag (a manually-paused subscriber stays
 *   paused), and the probe re-degrades the subscriber if the fault
 *   persists. Subscribers whose `autoResumeAfterMs` timer is already
 *   armed are skipped — the bus recovers those on its own.
 * - `prune-linger-windows`: expired handoff-linger windows (EB-32) are
 *   otherwise pruned lazily on read; force-dropping them reclaims the
 *   bookkeeping and stops replay from consulting dead windows.
 * - `rebalance-group`: a partitioned group running fewer live members
 *   than partitions (the doctor's `partition-coverage` finding) gets an
 *   operator-initiated rebalance (`trigger: 'remediate'`), re-running
 *   the cooperative protocol — pending auto-commit checkpoints flush,
 *   members re-observe the assignment — so member-local state re-syncs.
 *
 * Findings with no safe automatic fix (slow subscribers, DLQ backlog,
 * lagging groups, alias cycles, corrupt durable-log segments) are
 * deliberately left to the operator: the planner only emits the three
 * action kinds above. Every run is bounded by `maxActions` (default 10)
 * and supports `dryRun` previews with zero state changes. Absent the
 * opt-in — the `autoRemediate` method is never called and no schedule
 * is configured — the bus behaves exactly as before EB-211.
 */

import type { BusHealthReport, DoctorOptions, HealthStatus } from './doctor.ts';

/** The three safe automatic repairs the planner may emit. */
export type RemediationActionKind =
  | 'resume-subscriber'
  | 'prune-linger-windows'
  | 'rebalance-group';

/**
 * One planned remediation action, derived from a doctor finding (or the
 * live bus view behind one).
 */
export interface RemediationAction {
  kind: RemediationActionKind;
  /**
   * The action's target: a subscription id for `resume-subscriber`, a
   * group id for `prune-linger-windows` and `rebalance-group`.
   */
  target: string;
  /**
   * The competing set's topic pattern, for `rebalance-group` only —
   * the bus resolves the full group key from it.
   */
  pattern?: string;
  /**
   * Expired windows observed at plan time, for `prune-linger-windows`
   * only — reported in the audit.
   */
  expiredWindows?: number;
  /** Human-readable reason, citing the doctor finding behind the action. */
  reason: string;
}

/** A planned action plus its application outcome. */
export interface RemediationActionResult extends RemediationAction {
  /** True when the action was actually applied (never in dry-run). */
  applied: boolean;
  /**
   * Why the action was not applied: `'dry-run'`, the maxActions cap,
   * or the target vanishing between planning and application.
   */
  skippedReason?: string;
}

/** Options for `EventBus.autoRemediate()`. */
export interface AutoRemediateOptions {
  /**
   * Preview only: plan and report the actions without applying any of
   * them. The run is still audited as a dry-run summary.
   */
  dryRun?: boolean;
  /**
   * Per-run action cap — the circuit breaker: at most this many
   * actions are applied, the rest are reported as capped. Must be a
   * non-negative integer; default 10.
   */
  maxActions?: number;
  /** Tuning forwarded to the underlying `diagnoseBusHealth` call. */
  doctor?: DoctorOptions;
}

/**
 * Opt-in periodic schedule for `EventBusOptions.autoRemediate`: when
 * present, the bus runs `autoRemediate()` on an unref'd timer every
 * `intervalMs`. Absent (the default) nothing is scheduled — zero
 * behavior change.
 */
export interface AutoRemediateScheduleOptions extends AutoRemediateOptions {
  /** Positive finite milliseconds between remediation runs. */
  intervalMs: number;
}

/**
 * A live revision of the auto-remediation schedule (EB-306): the
 * partial update `EventBus.setAutoRemediate()` applies to a bus that
 * already enabled the `EventBusOptions.autoRemediate` opt-in. Only the
 * fields present move; the rest keep their live values. The `doctor`
 * tuning is not revisable here — it was fixed at construction.
 */
export interface AutoRemediateRevision {
  /** Positive finite milliseconds between remediation runs. */
  intervalMs?: number;
  /** Per-run action cap — a non-negative integer. */
  maxActions?: number;
  /** Preview only: plan without applying. */
  dryRun?: boolean;
}

/** Default per-run action cap for `autoRemediate()`. */
export const DEFAULT_REMEDIATE_MAX_ACTIONS = 10;

/** The outcome of one `autoRemediate()` run. */
export interface RemediationReport {
  /** Bus-clock reading (ms) the run started at. */
  at: number;
  /** Whether this run was a dry-run preview. */
  dryRun: boolean;
  /** The per-run cap that was in force. */
  maxActions: number;
  /** The doctor's grade before any action was applied. */
  healthBefore: HealthStatus;
  /** The doctor report the plan was derived from. */
  healthReport: BusHealthReport;
  /** Every planned action, in application order. */
  planned: RemediationAction[];
  /** Per-action outcomes, aligned with `planned`. */
  results: RemediationActionResult[];
  /** True when the plan exceeded `maxActions` and was cut. */
  capped: boolean;
}

/** One degraded subscriber as seen by the planner. */
export interface DegradedSubscriberView {
  subscriberId: string;
  pattern: string;
  consecutiveFailures: number;
  /** True when the probe's own auto-resume cooldown is armed. */
  autoResumeArmed: boolean;
}

/** One group holding expired handoff-linger windows. */
export interface ExpiredLingerView {
  groupId: string;
  expiredWindows: number;
}

/** One partitioned group running fewer members than partitions. */
export interface SkewedGroupView {
  groupId: string;
  pattern: string;
  partitions: number;
  members: number;
}

/** The structured inputs `planRemediation` folds into actions. */
export interface RemediationPlanInput {
  report: BusHealthReport;
  degradedSubscribers: DegradedSubscriberView[];
  groupsWithExpiredLinger: ExpiredLingerView[];
  skewedGroups: SkewedGroupView[];
}

/**
 * Derives the remediation plan from one doctor report plus the live
 * bus views behind its findings. Pure: no bus state is read or written
 * here — the caller supplies the views. Findings with no safe automatic
 * fix produce no actions; they stay in `report` for the operator.
 *
 * Ordering is by blast radius, smallest first: subscriber resumes,
 * then linger hygiene, then group rebalances.
 */
export function planRemediation(input: RemediationPlanInput): RemediationAction[] {
  const actions: RemediationAction[] = [];
  for (const s of input.degradedSubscribers) {
    // The probe's own cooldown recovers armed subscribers by itself —
    // only the stuck ones (no timer armed) need the remediator.
    if (s.autoResumeArmed) continue;
    actions.push({
      kind: 'resume-subscriber',
      target: s.subscriberId,
      reason:
        `health-probe degraded subscriber "${s.subscriberId}" (pattern "${s.pattern}") ` +
        `with no auto-resume armed after ${s.consecutiveFailures} consecutive failure(s): ` +
        `resume is the only recovery path; the probe re-degrades it if the fault persists`,
    });
  }
  for (const g of input.groupsWithExpiredLinger) {
    actions.push({
      kind: 'prune-linger-windows',
      target: g.groupId,
      expiredWindows: g.expiredWindows,
      reason:
        `${g.expiredWindows} expired handoff-linger window(s) for group "${g.groupId}": ` +
        `drop them so replay stops consulting dead windows`,
    });
  }
  for (const g of input.skewedGroups) {
    actions.push({
      kind: 'rebalance-group',
      target: g.groupId,
      pattern: g.pattern,
      reason:
        `partitioned group "${g.groupId}" (pattern "${g.pattern}") runs ${g.members} ` +
        `member(s) for ${g.partitions} partitions (doctor partition-coverage finding): ` +
        `force a rebalance to re-sync member state`,
    });
  }
  return actions;
}

/**
 * Validates `AutoRemediateScheduleOptions` for the
 * `EventBusOptions.autoRemediate` opt-in. Throws `RangeError` on a
 * non-positive or non-finite `intervalMs`, or an invalid `maxActions`.
 */
export function assertValidRemediateSchedule(
  raw: unknown,
  where: string,
): asserts raw is AutoRemediateScheduleOptions {
  if (typeof raw !== 'object' || raw === null) {
    throw new RangeError(`${where}: autoRemediate must be an object with { intervalMs, ... }`);
  }
  const { intervalMs, maxActions } = raw as { intervalMs?: unknown; maxActions?: unknown };
  if (typeof intervalMs !== 'number' || !Number.isFinite(intervalMs) || intervalMs <= 0) {
    throw new RangeError(`${where}: autoRemediate.intervalMs must be a positive finite number of milliseconds`);
  }
  if (maxActions !== undefined && (!Number.isInteger(maxActions) || (maxActions as number) < 0)) {
    throw new RangeError(`${where}: autoRemediate.maxActions must be a non-negative integer`);
  }
}
