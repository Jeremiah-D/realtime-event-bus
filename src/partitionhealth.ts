/**
 * Publish-side partition health-aware routing (EB-217, real-time systems).
 *
 * EB-90's publish-side partition assignment (`EventBus.setTopicPartitions` /
 * `resolvePartition`) had no health concept: a `partitionKey` hashes to a
 * partition and the publish goes there, even when that partition's
 * downstream is known-bad. This module tracks per-(topic, partition)
 * health from reported outcomes and lets the publish path skip unhealthy
 * partitions.
 *
 * A health signal is one outcome per (topic, partition):
 * - `success`: the partition served the outcome fine;
 * - `failure`: a hard failure (e.g. the downstream consumer reported a
 *   poisoned offset, the segment write failed);
 * - `timeout`: the outcome did not complete in time (e.g. a slow consumer
 *   made the partition lag past its SLO).
 *
 * The signals are *reported in* (`reportPartitionFailure` /
 * `reportPartitionTimeout` / `reportPartitionSuccess` on the bus) because
 * an in-memory publish-side bus cannot observe downstream partition
 * health itself — the operator, a monitoring sidecar, or a bridge to an
 * external log (Kafka, Redpanda) is the honest source of these signals.
 * The tracker deliberately never fabricates signals it cannot observe.
 *
 * Health over a sliding window of the most recent outcomes (bounded by
 * `windowSize`, oldest evicted first): a partition is unhealthy when its
 * bad-outcome rate — `(failures + timeouts) / window` — reaches
 * `unhealthyThreshold`, with at least `minSamples` outcomes so a fresh
 * partition is never condemned on one bad report. Once unhealthy, the
 * partition needs a *cool-down*: `cooldownSuccesses` consecutive
 * successful outcomes before it is cut back into routing — a single
 * success after a failure storm is not recovery, and a bad outcome
 * during the cool-down restarts it. On recovery the window is cleared so
 * stale bad outcomes cannot immediately re-condemn the partition.
 *
 * The tracker is count-based (no clock): the same outcome sequence
 * always yields the same health, which keeps tests deterministic and
 * keeps the module free of the bus clock — exactly like
 * `ScheduleSkewTracker` in `scheduleskew.ts`.
 *
 * Tracking is opt-in bus-wide (`EventBusOptions.partitionHealth`). With
 * no options object the tracker is never constructed — no health state,
 * no rerouting, no new stats fields, no new Prometheus series.
 */

export type PartitionOutcome = 'success' | 'failure' | 'timeout';

/** Tuning for publish-side partition health tracking. Opt-in by presence. */
export interface PartitionHealthOptions {
  /**
   * Sliding window of the most recent outcomes kept per (topic,
   * partition); the oldest outcomes are evicted past this bound. Must be
   * a positive integer. Default 20.
   */
  windowSize?: number;
  /**
   * The bad-outcome rate — `(failures + timeouts) / window` — at or
   * above which a partition is marked unhealthy. Must be a number in
   * (0, 1]. Default 0.5.
   */
  unhealthyThreshold?: number;
  /**
   * Minimum outcomes in the window before a partition can be marked
   * unhealthy, so one bad report never condemns a fresh partition. Must
   * be a positive integer and may not exceed `windowSize`. Default 10.
   */
  minSamples?: number;
  /**
   * Consecutive successful outcomes required to recover an unhealthy
   * partition (the cool-down before it is cut back into routing). A bad
   * outcome during the cool-down restarts it. Must be a positive
   * integer. Default 5.
   */
  cooldownSuccesses?: number;
}

/** Fully-resolved tuning — every field concrete. */
export interface ResolvedPartitionHealthOptions {
  windowSize: number;
  unhealthyThreshold: number;
  minSamples: number;
  cooldownSuccesses: number;
}

/**
 * Revision for the bus-level partition-health tuning (see
 * `EventBus.setPartitionHealth`, EB-298). Only the fields provided move;
 * the rest keep their live values.
 */
export interface PartitionHealthRevision {
  /**
   * Sliding window of the most recent outcomes kept per (topic,
   * partition). Must be a positive integer when provided — the
   * construction-time rule.
   */
  windowSize?: number;
  /**
   * The bad-outcome rate at or above which a partition is marked
   * unhealthy. Must be a number in (0, 1] when provided — the
   * construction-time rule.
   */
  unhealthyThreshold?: number;
  /**
   * Minimum outcomes in the window before a partition can be marked
   * unhealthy. Must be a positive integer no larger than the (merged)
   * `windowSize` when provided — the construction-time rule.
   */
  minSamples?: number;
  /**
   * Consecutive successful outcomes required to recover an unhealthy
   * partition. Must be a positive integer when provided — the
   * construction-time rule.
   */
  cooldownSuccesses?: number;
}

/**
 * Validates `EventBusOptions.partitionHealth` fail-fast. Returns
 * `undefined` when the option is absent (tracking disabled); throws
 * `TypeError`/`RangeError` on a malformed options object so a bad
 * config can never half-wire a bus.
 */
export function resolvePartitionHealthOptions(
  opt: PartitionHealthOptions | undefined,
): ResolvedPartitionHealthOptions | undefined {
  if (opt === undefined) return undefined;
  if (typeof opt !== 'object' || opt === null) {
    throw new TypeError('partitionHealth must be a PartitionHealthOptions object');
  }
  const windowSize = opt.windowSize ?? 20;
  if (!Number.isInteger(windowSize) || windowSize <= 0) {
    throw new RangeError('partitionHealth.windowSize must be a positive integer');
  }
  const unhealthyThreshold = opt.unhealthyThreshold ?? 0.5;
  if (
    typeof unhealthyThreshold !== 'number' ||
    !Number.isFinite(unhealthyThreshold) ||
    unhealthyThreshold <= 0 ||
    unhealthyThreshold > 1
  ) {
    throw new RangeError('partitionHealth.unhealthyThreshold must be a number in (0, 1]');
  }
  const minSamples = opt.minSamples ?? 10;
  if (!Number.isInteger(minSamples) || minSamples <= 0) {
    throw new RangeError('partitionHealth.minSamples must be a positive integer');
  }
  if (minSamples > windowSize) {
    throw new RangeError(
      'partitionHealth.minSamples must not exceed partitionHealth.windowSize: a window that small can never condemn a partition',
    );
  }
  const cooldownSuccesses = opt.cooldownSuccesses ?? 5;
  if (!Number.isInteger(cooldownSuccesses) || cooldownSuccesses <= 0) {
    throw new RangeError('partitionHealth.cooldownSuccesses must be a positive integer');
  }
  return { windowSize, unhealthyThreshold, minSamples, cooldownSuccesses };
}

interface PartitionHealthRecord {
  /** Oldest-first sliding window of recent outcomes, bounded by `windowSize`. */
  outcomes: PartitionOutcome[];
  /** Failures currently in the window (mirrors `outcomes`, kept for O(1) rate math). */
  failures: number;
  /** Timeouts currently in the window. */
  timeouts: number;
  /** Whether the partition is currently cut out of routing. */
  unhealthy: boolean;
  /** Successful outcomes still needed to end the cool-down (0 when healthy). */
  cooldownRemaining: number;
}

/** One row of the health snapshot — the `getStats().partitionHealth` shape. */
export interface PartitionHealthRow {
  /** Concrete topic (alias-resolved, exactly as the publish path sees it). */
  topic: string;
  /** Partition index. */
  partition: number;
  /** False while the partition is cut out of routing. */
  healthy: boolean;
  /** `(failures + timeouts) / window`, 0 when no outcomes are recorded. */
  failureRate: number;
  /** Failures currently in the window. */
  failures: number;
  /** Timeouts currently in the window. */
  timeouts: number;
  /** Outcomes currently in the window (window fill). */
  samples: number;
}

/**
 * Sliding-window per-(topic, partition) health. Unknown partitions are
 * healthy by definition — health is only ever ascribed, never assumed.
 */
export class PartitionHealthTracker {
  // Live-tuned by `retune` (EB-298): `record` reads these fields on
  // every call, so a revision governs from the next outcome without
  // draining or rebuilding any record — the records table (in-flight
  // outcome windows, cool-down counters) is untouched by a retune.
  private opts: ResolvedPartitionHealthOptions;
  private readonly records = new Map<string, PartitionHealthRecord>();

  constructor(opts: ResolvedPartitionHealthOptions) {
    this.opts = opts;
  }

  /** The four live tuning values, for bus-level revision (EB-298). */
  current(): ResolvedPartitionHealthOptions {
    return { ...this.opts };
  }

  /**
   * Retunes the four numeric bounds live (EB-298). The caller must have
   * validated `next` through `resolvePartitionHealthOptions` first —
   * this method applies unconditionally. The records table is untouched:
   * already-recorded outcomes stay recorded, and the new values govern
   * from the next `record` onward — a tightened `windowSize` truncates
   * the oldest outcomes down to the new window at the next record, a
   * tightened `unhealthyThreshold`/`minSamples` condemns at the next
   * classify, and a revised `cooldownSuccesses` arms the cool-down of
   * the next condemnation (an in-flight cool-down counter is not
   * rewritten).
   */
  retune(next: ResolvedPartitionHealthOptions): void {
    this.opts = { ...next };
  }

  private key(topic: string, partition: number): string {
    return `${topic}\0${partition}`;
  }

  private recordFor(topic: string, partition: number): PartitionHealthRecord {
    const k = this.key(topic, partition);
    let r = this.records.get(k);
    if (r === undefined) {
      r = { outcomes: [], failures: 0, timeouts: 0, unhealthy: false, cooldownRemaining: 0 };
      this.records.set(k, r);
    }
    return r;
  }

  /**
   * Records one outcome for a (topic, partition) pair. Evicts the oldest
   * outcome past `windowSize` first, then classifies: a healthy partition
   * with at least `minSamples` outcomes and a bad-outcome rate at or
   * above `unhealthyThreshold` is marked unhealthy and enters its
   * cool-down; an unhealthy partition ticks its cool-down down on
   * success (recovering and clearing the window when it reaches zero) or
   * restarts the cool-down on a bad outcome.
   */
  record(topic: string, partition: number, outcome: PartitionOutcome): void {
    const r = this.recordFor(topic, partition);
    r.outcomes.push(outcome);
    // Evict down to the live window: in the steady state this removes
    // at most one oldest outcome per record, and after a retune that
    // tightened `windowSize` (EB-298) it converges to the new window
    // at the next record instead of staying oversized forever.
    while (r.outcomes.length > this.opts.windowSize) {
      const evicted = r.outcomes.shift() as PartitionOutcome;
      if (evicted === 'failure') r.failures -= 1;
      else if (evicted === 'timeout') r.timeouts -= 1;
    }
    if (outcome === 'failure') r.failures += 1;
    else if (outcome === 'timeout') r.timeouts += 1;

    if (!r.unhealthy) {
      const total = r.outcomes.length;
      if (total >= this.opts.minSamples && (r.failures + r.timeouts) / total >= this.opts.unhealthyThreshold) {
        r.unhealthy = true;
        r.cooldownRemaining = this.opts.cooldownSuccesses;
      }
      return;
    }
    if (outcome === 'success') {
      r.cooldownRemaining -= 1;
      if (r.cooldownRemaining <= 0) {
        // Cool-down complete: cut the partition back into routing with a
        // clean slate — the stale bad outcomes it recovered from must not
        // re-condemn it on the first new report.
        r.unhealthy = false;
        r.cooldownRemaining = 0;
        r.outcomes = [];
        r.failures = 0;
        r.timeouts = 0;
      }
    } else {
      // A bad outcome during the cool-down restarts it — recovery needs
      // *consecutive* successes.
      r.cooldownRemaining = this.opts.cooldownSuccesses;
    }
  }

  /** Whether the partition currently takes routed publishes. */
  isHealthy(topic: string, partition: number): boolean {
    const r = this.records.get(this.key(topic, partition));
    return r === undefined || !r.unhealthy;
  }

  /** `(failures + timeouts) / window` for the pair; 0 when unrecorded. */
  failureRate(topic: string, partition: number): number {
    const r = this.records.get(this.key(topic, partition));
    if (r === undefined || r.outcomes.length === 0) return 0;
    return (r.failures + r.timeouts) / r.outcomes.length;
  }

  /**
   * One row per (topic, partition) pair with at least one recorded
   * outcome, in first-report order. Pairs that were never reported are
   * healthy by definition and carry no row — the snapshot stays bounded
   * by actual signal, not by the configured partition space.
   */
  snapshot(): PartitionHealthRow[] {
    const rows: PartitionHealthRow[] = [];
    for (const [k, r] of this.records) {
      const sep = k.indexOf('\0');
      rows.push({
        topic: k.slice(0, sep),
        partition: Number(k.slice(sep + 1)),
        healthy: !r.unhealthy,
        failureRate: r.outcomes.length === 0 ? 0 : (r.failures + r.timeouts) / r.outcomes.length,
        failures: r.failures,
        timeouts: r.timeouts,
        samples: r.outcomes.length,
      });
    }
    return rows;
  }
}
