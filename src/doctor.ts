/**
 * Bus health doctor (EB-101).
 *
 * One-shot structured diagnosis of an `EventBus`: `diagnoseBusHealth`
 * folds one read-only snapshot into a graded report
 * `{ status: 'healthy' | 'degraded' | 'critical', checks: [...] }`.
 *
 * The function itself is pure — it takes a `DoctorInput` snapshot and
 * returns a fresh report, touching no bus state. The `EventBus` method
 * `bus.exportBusHealth()` (see `src/bus.ts`) assembles that snapshot
 * from `getStats()` plus a few read-only internal views (DLQ sizes,
 * per-subscriber lag thresholds) and delegates here, so the whole call
 * is read-only: no publishes, no durable-log writes, no callbacks fired,
 * no state mutated.
 *
 * Grading: every check reports `pass` / `warn` / `fail`; the overall
 * status is `healthy` when all checks pass, `degraded` when any check
 * warns, `critical` when any check fails.
 */
import type { BusStats } from './bus.ts';
import type { GroupLagStat } from './grouplag.ts';

/** Overall bus health grade. */
export type HealthStatus = 'healthy' | 'degraded' | 'critical';

/** Per-check grade. */
export type CheckStatus = 'pass' | 'warn' | 'fail';

/** One diagnostic check in the health report. */
export interface HealthCheck {
  /** Stable machine-readable check name. */
  name: string;
  /** The check's grade. */
  status: CheckStatus;
  /** Human-readable evidence: what was observed, with numbers. */
  detail: string;
  /** What the operator should do about a non-pass. */
  suggestion: string;
}

/** The structured diagnosis report returned by `exportBusHealth()`. */
export interface BusHealthReport {
  /** Overall grade: all pass → healthy, any warn → degraded, any fail → critical. */
  status: HealthStatus;
  /** Bus-clock reading (ms) the snapshot was taken at. */
  at: number;
  /** The checks, in a stable order. */
  checks: HealthCheck[];
}

/** One row of the EB-31 top-slowest subscriber ranking. */
export interface SlowSubscriberInput {
  subscriberId: string;
  pattern: string;
  p99Ms: number;
  samples: number;
}

/** One monitored subscriber's live lag watermark (EB-35 `lag` stats row). */
export interface LagWatermarkInput {
  subscriberId: string;
  pattern: string;
  watermarkMs: number;
}

/** One EB-53 latency-SLO-breaching subscriber (windowed p99 above its own threshold). */
export interface BreachingSubscriberInput {
  subscriberId: string;
  pattern: string;
  p99Ms: number;
  thresholdMs: number;
}

/** One reliable subscriber's current dead-letter queue occupancy. */
export interface DlqBacklogInput {
  subscriberId: string;
  pattern: string;
  /** Entries currently held in the DLQ. */
  entries: number;
  /** `DeadLetterOptions.maxEntries`: the queue is full (evicting oldest) at this size. */
  maxEntries: number;
}

/**
 * The read-only snapshot `diagnoseBusHealth` folds into a report.
 * `EventBus.exportBusHealth()` builds this from `getStats()` and
 * read-only internal views; tests may construct it directly for
 * deterministic fault injection.
 */
export interface DoctorInput {
  /** Bus-clock reading (ms) the snapshot was taken at; reported as `BusHealthReport.at`. */
  at: number;
  aliases: BusStats['aliases'];
  slowestSubscribers: SlowSubscriberInput[];
  /**
   * Every lag-monitored subscriber's live watermark (EB-35 `lag` rows).
   * The watermark — not the top-5 drain-dwell ranking — is the right
   * signal here: a subscriber whose handler is stuck shows a watermark
   * that keeps growing while nothing is ever drained.
   */
  lagWatermarks: LagWatermarkInput[];
  breachingSubscribers: BreachingSubscriberInput[];
  /**
   * Per-subscriber EB-35 `lagMonitor.thresholdMs`, for subscribers that
   * configured one. Absent entries fall back to
   * `DoctorOptions.lagWatermarkDefaultMs`.
   */
  lagThresholdMs: Map<string, number>;
  dlqBacklog: DlqBacklogInput[];
  groupLag: GroupLagStat[];
  /** Effective EB-48 threshold for a group (per-group override, else the bus default). */
  groupLagThresholdFor: (groupId: string) => number;
  consumerGroups: BusStats['consumerGroups'];
  namespaces?: BusStats['namespaces'];
  durableLog?: BusStats['durableLog'];
}

/** Tuning for `diagnoseBusHealth` / `exportBusHealth()`. */
export interface DoctorOptions {
  /**
   * p99 queue-dwell (ms) above which a top-slowest subscriber warns.
   * Default 1000: a full second of queue dwell means the consumer is
   * starved behind its own backlog. No module defines a bus-wide
   * threshold for this ranking, so the doctor owns the default —
   * per-subscriber handler-p99 SLOs (EB-53) are reused where configured.
   */
  slowSubscriberP99Ms?: number;
  /**
   * Lag watermark (ms) above which a lag-monitored subscriber warns when
   * it configured no `lagMonitor.thresholdMs` of its own. Default 30000:
   * EB-35 deliberately leaves the threshold absent (no alerting) unless
   * the subscriber opts in, so the doctor needs its own fallback for
   * diagnosis.
   */
  lagWatermarkDefaultMs?: number;
}

/**
 * Default p99 queue-dwell (ms) flagging a slow subscriber. See
 * `DoctorOptions.slowSubscriberP99Ms`.
 */
export const DEFAULT_SLOW_SUBSCRIBER_P99_MS = 1000;

/**
 * Default lag-watermark (ms) flagging a lagging subscriber without its
 * own configured threshold. See `DoctorOptions.lagWatermarkDefaultMs`.
 */
export const DEFAULT_LAG_WATERMARK_MS = 30_000;

/** A DLQ is "nearly full" at this occupancy fraction — eviction is imminent. */
const DLQ_NEAR_FULL_FRACTION = 0.9;

function fmt(n: number): string {
  return Number.isInteger(n) ? String(n) : n.toFixed(1);
}

function checkSlowSubscribers(
  input: DoctorInput,
  opts: Required<Pick<DoctorOptions, 'slowSubscriberP99Ms'>>,
): HealthCheck {
  const slow = input.slowestSubscribers.filter((s) => s.p99Ms >= opts.slowSubscriberP99Ms);
  const parts: string[] = [];
  for (const s of slow) {
    parts.push(`${s.subscriberId} (pattern "${s.pattern}"): p99 queue dwell ${fmt(s.p99Ms)}ms over ${s.samples} samples`);
  }
  for (const b of input.breachingSubscribers) {
    parts.push(
      `${b.subscriberId} (pattern "${b.pattern}"): handler p99 ${fmt(b.p99Ms)}ms breaching its latency-SLO threshold ${fmt(b.thresholdMs)}ms`,
    );
  }
  if (parts.length === 0) {
    return {
      name: 'slow-subscribers',
      status: 'pass',
      detail: `no slow subscribers: top-${input.slowestSubscribers.length} p99 queue dwell all below ${opts.slowSubscriberP99Ms}ms and no latency-SLO breaches`,
      suggestion: 'none',
    };
  }
  return {
    name: 'slow-subscribers',
    status: 'warn',
    detail: `${parts.length} slow subscriber(s): ${parts.join('; ')}`,
    suggestion:
      'inspect the handler throughput of the listed subscribers; consider maxInflight / backpressure tuning, or scaling consumers',
  };
}

function checkSubscriberLag(
  input: DoctorInput,
  opts: Required<Pick<DoctorOptions, 'lagWatermarkDefaultMs'>>,
): HealthCheck {
  const over = input.lagWatermarks.filter(
    (s) => s.watermarkMs >= (input.lagThresholdMs.get(s.subscriberId) ?? opts.lagWatermarkDefaultMs),
  );
  if (over.length === 0) {
    return {
      name: 'subscriber-lag',
      status: 'pass',
      detail: `${input.lagWatermarks.length} lag-monitored subscriber(s), all watermarks within threshold`,
      suggestion: 'none',
    };
  }
  const parts = over.map((s) => {
    const threshold = input.lagThresholdMs.get(s.subscriberId) ?? opts.lagWatermarkDefaultMs;
    return `${s.subscriberId} (pattern "${s.pattern}"): watermark ${fmt(s.watermarkMs)}ms >= threshold ${fmt(threshold)}ms`;
  });
  return {
    name: 'subscriber-lag',
    status: 'warn',
    detail: `${over.length} subscriber(s) over lag threshold: ${parts.join('; ')}`,
    suggestion:
      'drain the backlog: check whether the handler is stuck (healthProbe), unblock the queue, or add consumer parallelism',
  };
}

function checkDeadLetterBacklog(input: DoctorInput): HealthCheck {
  const holding = input.dlqBacklog.filter((d) => d.entries > 0);
  const full = holding.filter((d) => d.entries >= d.maxEntries);
  const nearFull = holding.filter(
    (d) => d.entries < d.maxEntries && d.entries >= d.maxEntries * DLQ_NEAR_FULL_FRACTION,
  );
  const total = holding.reduce((acc, d) => acc + d.entries, 0);
  if (holding.length === 0) {
    return {
      name: 'dead-letter-backlog',
      status: 'pass',
      detail: 'no messages held in any dead-letter queue',
      suggestion: 'none',
    };
  }
  const parts = holding.map(
    (d) => `${d.subscriberId} (pattern "${d.pattern}"): ${d.entries}/${d.maxEntries} entries`,
  );
  if (full.length > 0) {
    return {
      name: 'dead-letter-backlog',
      status: 'fail',
      detail: `${total} dead-lettered message(s) held; DLQ FULL for ${full.map((d) => d.subscriberId).join(', ')} — oldest entries are being evicted: ${parts.join('; ')}`,
      suggestion:
        'raise deadLetter.maxEntries or drain urgently: inspect getDeadLetterMessages(subId), fix the poison payloads, replayDeadLetter the recoverable ones',
    };
  }
  return {
    name: 'dead-letter-backlog',
    status: 'warn',
    detail: `${total} dead-lettered message(s) held${nearFull.length > 0 ? `; nearly full: ${nearFull.map((d) => d.subscriberId).join(', ')}` : ''}: ${parts.join('; ')}`,
    suggestion:
      'inspect getDeadLetterMessages(subId) for the poison messages, fix the root cause, replayDeadLetter the recoverable ones',
  };
}

function checkConsumerGroupLag(input: DoctorInput): HealthCheck {
  const over = input.groupLag.filter((row) => row.lag > input.groupLagThresholdFor(row.groupId));
  if (over.length === 0) {
    return {
      name: 'consumer-group-lag',
      status: 'pass',
      detail: `${input.groupLag.length} group-lag row(s), all within threshold`,
      suggestion: 'none',
    };
  }
  const shown = over.slice(0, 5);
  const parts = shown.map((row) => {
    const threshold = input.groupLagThresholdFor(row.groupId);
    const where = row.partition !== undefined ? `partition ${row.partition} of ` : '';
    return `group "${row.groupId}" ${where}topic "${row.topic}": lag ${row.lag} > threshold ${threshold}`;
  });
  return {
    name: 'consumer-group-lag',
    status: 'warn',
    detail: `${over.length} group-lag row(s) over threshold${over.length > shown.length ? ` (showing ${shown.length})` : ''}: ${parts.join('; ')}`,
    suggestion:
      'add consumers to the lagging groups, or commit offsets for work already done; tune thresholds via setGroupLagThreshold',
  };
}

function checkTopicAliases(input: DoctorInput): HealthCheck {
  // Re-verify the alias table is acyclic: EB-44 already prevents cycles
  // at registration, so this is a defense-in-depth re-check, not new
  // logic. Walk every oldTopic -> newTopic chain.
  const target = new Map<string, string>();
  for (const a of input.aliases) target.set(a.oldTopic, a.newTopic);
  let maxDepth = 0;
  for (const start of target.keys()) {
    const seen = new Set<string>();
    let cur: string | undefined = start;
    let depth = 0;
    while (cur !== undefined && target.has(cur)) {
      if (cur === target.get(cur)) {
        return {
          name: 'topic-aliases',
          status: 'fail',
          detail: `alias self-loop: "${cur}" aliases to itself`,
          suggestion: 'remove the self-referencing alias; alias targets must be real topics',
        };
      }
      if (seen.has(cur)) {
        return {
          name: 'topic-aliases',
          status: 'fail',
          detail: `alias cycle detected: ${[...seen, cur].join(' -> ')}`,
          suggestion: 'break the cycle by deleting one alias in the loop; EB-44 prevents new cycles at registration',
        };
      }
      seen.add(cur);
      cur = target.get(cur);
      depth += 1;
    }
    maxDepth = Math.max(maxDepth, depth);
  }
  return {
    name: 'topic-aliases',
    status: 'pass',
    detail: `${input.aliases.length} alias(es) verified, longest chain ${maxDepth}, no cycles (re-verified; EB-44 prevents cycles at registration)`,
    suggestion: 'none',
  };
}

function checkNamespaces(input: DoctorInput): HealthCheck {
  const namespaces = input.namespaces ?? [];
  if (namespaces.length === 0) {
    return {
      name: 'namespaces',
      status: 'pass',
      detail: 'no namespaces registered',
      suggestion: 'none',
    };
  }
  const empty = namespaces.filter(
    (n) => n.topics === 0 && n.subscribers === 0 && n.publishedMessages === 0,
  );
  if (empty.length === 0) {
    return {
      name: 'namespaces',
      status: 'pass',
      detail: `${namespaces.length} namespace(s), all have topics, subscribers, or published traffic`,
      suggestion: 'none',
    };
  }
  return {
    name: 'namespaces',
    status: 'warn',
    detail: `${empty.length} empty namespace(s): ${empty.map((n) => `"${n.namespace}"`).join(', ')} — registered but with no topics, subscribers, or published messages`,
    suggestion: 'delete the unused namespaces or check the tenant wiring that should be publishing into them',
  };
}

function checkDurableLog(input: DoctorInput): HealthCheck {
  const log = input.durableLog;
  if (log === undefined) {
    return {
      name: 'durable-log',
      status: 'pass',
      detail: 'durable log disabled',
      suggestion: 'none',
    };
  }
  const evidence: string[] = [];
  if (log.corruptSegments > 0) evidence.push(`${log.corruptSegments} corrupt segment(s) isolated by checksum (EB-94 sidecar)`);
  if (log.corruptArchives > 0) evidence.push(`${log.corruptArchives} corrupt archive(s)`);
  if (log.corruptLines > 0) evidence.push(`${log.corruptLines} malformed line(s) skipped`);
  if (evidence.length === 0) {
    return {
      name: 'durable-log',
      status: 'pass',
      detail: `durable log healthy: ${log.entries} entries across ${log.topics} topic(s), no corrupt segments/lines/archives`,
      suggestion: 'none',
    };
  }
  const hard = log.corruptSegments > 0 || log.corruptArchives > 0;
  return {
    name: 'durable-log',
    status: hard ? 'fail' : 'warn',
    detail: `durable log integrity: ${evidence.join('; ')} (${log.entries} entries across ${log.topics} topic(s))`,
    suggestion: hard
      ? 'investigate disk/hardware health: corrupt segments were skipped and their data is unavailable for replay — restore from backup if the data matters'
      : 'malformed lines were skipped during recovery; check the producer encoding if the count keeps growing',
  };
}

function checkPartitionCoverage(input: DoctorInput): HealthCheck {
  // EB-101 asks for a "quorum" check. This bus has no quorum concept at
  // all (verified: the word "quorum" appears nowhere in src/) — there is
  // no consensus layer to fall short of. The honest mapping is EB-90
  // partition coverage: a partitioned consumer group configures P
  // partitions but runs M live members; when M < P no partition has a
  // dedicated owner and partitions must share consumers, which is the
  // closest analog to "not reaching the expected parallel consumer
  // count". This check flags exactly that.
  const partitioned = input.consumerGroups.filter((g) => g.partitions !== undefined);
  const thin = partitioned.filter((g) => g.members < (g.partitions ?? 0));
  if (thin.length === 0) {
    return {
      name: 'partition-coverage',
      status: 'pass',
      detail:
        partitioned.length === 0
          ? 'no partitioned consumer groups (no quorum concept exists on this bus; mapped to EB-90 partition coverage)'
          : `${partitioned.length} partitioned group(s), every group has at least as many live members as partitions (no quorum concept exists on this bus; mapped to EB-90 partition coverage)`,
      suggestion: 'none',
    };
  }
  const parts = thin.map(
    (g) => `group "${g.groupId}" (pattern "${g.pattern}"): ${g.partitions} partitions but only ${g.members} live member(s) — no partition has a dedicated owner`,
  );
  return {
    name: 'partition-coverage',
    status: 'warn',
    detail: `${thin.length} partitioned group(s) under-covered (no quorum concept exists on this bus; mapped to EB-90 partition coverage): ${parts.join('; ')}`,
    suggestion: 'add group members until members >= partitions, or reduce the group partition count',
  };
}

/**
 * Folds one read-only `DoctorInput` snapshot into a graded health
 * report. Pure: no bus state is read or written here — the caller
 * supplies the snapshot.
 */
export function diagnoseBusHealth(input: DoctorInput, options?: DoctorOptions): BusHealthReport {
  const opts = {
    slowSubscriberP99Ms: options?.slowSubscriberP99Ms ?? DEFAULT_SLOW_SUBSCRIBER_P99_MS,
    lagWatermarkDefaultMs: options?.lagWatermarkDefaultMs ?? DEFAULT_LAG_WATERMARK_MS,
  };
  const checks: HealthCheck[] = [
    checkSlowSubscribers(input, opts),
    checkSubscriberLag(input, opts),
    checkDeadLetterBacklog(input),
    checkConsumerGroupLag(input),
    checkTopicAliases(input),
    checkNamespaces(input),
    checkDurableLog(input),
    checkPartitionCoverage(input),
  ];
  const status: HealthStatus = checks.some((c) => c.status === 'fail')
    ? 'critical'
    : checks.some((c) => c.status === 'warn')
      ? 'degraded'
      : 'healthy';
  return { status, at: input.at, checks };
}
