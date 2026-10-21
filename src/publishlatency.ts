/**
 * Publish-side admission latency sampling (EB-182).
 *
 * Where `SubscribeOptions.deliveryLatency` (see `src/latency.ts`) records a
 * *historical* distribution of per-delivery queue dwells and the lag monitor
 * (`src/lag.ts`) answers "how far behind is this consumer right now", the
 * admission-latency tracker answers the operator's publish-side question —
 * "how long does the *admission phase* cost per publish, and which gate is
 * hot?" — Kafka-producer-latency style, but decomposed per admission gate:
 *
 * - **Total admission span**: one sample per publish admission, from the
 *   first admission gate to the admission decision (admitted or rejected).
 *   The span covers the gate section every publish path funnels through:
 *   `publish`, `publishBatch` (per entry), `publishIdempotent` (admitted
 *   path), `publishDelayed` (due-time fan-out), and `publishAtomic` (via
 *   the shadow admission verdict). It is a *gate* latency, not an
 *   end-to-end publish latency — sequence-number assignment, the durable
 *   log, compression and fan-out to subscribers are all past the decision
 *   point and never sampled. Orthogonal to EB-31/EB-40/EB-53 (delivery
 *   side): nothing is double-sampled.
 * - **Per-gate breakdown**: each gate's own call latency is sampled into a
 *   per-(topic, gate) window, so a slow admission can be attributed —
 *   e.g. a topic whose schema validator is heavy shows a hot `schema`
 *   gate while its `acl` gate stays flat.
 * - **Slow-admission top-N**: `topSlowTopics` ranks topics by total-span
 *   p99, the admission-gate hotspot list.
 *
 * Sampling is opt-in per bus (`EventBusOptions.publishLatency`). When
 * disabled — the default — the publish path pays one branch per gate and
 * zero clock reads. When enabled, the bus computes deltas from its
 * injected clock (`EventBusOptions.now` — the same injection point the
 * delivery-latency tracker uses), so tests drive deterministic latencies
 * by advancing a controllable clock; the tracker itself takes no clock
 * because it only ever sees already-computed deltas.
 *
 * Semantics worth knowing:
 * - One total-span sample per admission decision: admitted publishes
 *   sample the full gate section; rejected publishes sample up to the
 *   refusing gate (a schema rejection never pays the gates after it).
 * - `publishIdempotent`'s pre-`fanOut` checks (the ACL pre-check and the
 *   idempotency-dedup table probe) are sampled as gate executions too —
 *   the ACL genuinely runs twice on the admitted path (pre-dedup
 *   authorization, then `fanOut` admission), and both executions are
 *   real gate work.
 * - `publishAtomic` entries are sampled through the shadow admission
 *   verdict (`EventBus.admissionVerdict`): the same `acl`/`schema` gate
 *   executions the batch's admission actually ran. `previewPublish`
 *   reuses that verdict, so a preview also contributes gate samples —
 *   a preview is an admission rehearsal, and the gates really executed.
 * - `publishDelayed` samples the fail-fast gates at schedule time (the
 *   due-time fan-out is pre-admitted — schema ran once at schedule
 *   time — so only the topic-rate-limit gate is re-sampled there).
 * - A clock that moved backwards between span start and end clamps the
 *   sample at 0.
 */

/** The admission gates whose call latency is sampled. */
export type AdmissionGate =
  | 'webhook'
  | 'publisher-rate-limit'
  | 'acl'
  | 'schema'
  | 'size'
  | 'sampling'
  | 'pressure'
  | 'tenant-quota'
  | 'global-rate-limit'
  | 'topic-rate-limit'
  | 'idempotency-dedup';

/** Tuning for publish-side admission latency sampling. */
export interface AdmissionLatencyOptions {
  /**
   * How many of the most recent total-span samples to keep per topic;
   * the oldest samples are evicted past this bound. Must be a positive
   * integer. Default 1024.
   */
  windowSize?: number;
  /**
   * How many of the most recent per-gate samples to keep per
   * (topic, gate); the oldest samples are evicted past this bound.
   * Must be a positive integer. Default 1024.
   */
  gateWindowSize?: number;
  /**
   * A total-span sample at or above this many milliseconds counts as a
   * slow admission (`slowCount`). Must be a non-negative finite number.
   * Default 100.
   */
  slowAdmissionThresholdMs?: number;
}

/** A latency distribution summary over one sample window. */
export interface AdmissionLatencyStats {
  /** Samples currently in the window. */
  samples: number;
  /** Nearest-rank p50/p95/p99 of the window, in milliseconds. 0 when empty. */
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  /** Minimum / maximum / arithmetic mean of the window, in milliseconds. 0 when empty. */
  minMs: number;
  maxMs: number;
  meanMs: number;
  /** Samples at or above `slowAdmissionThresholdMs`. */
  slowCount: number;
}

/** One row of the slow-admission hotspot ranking. */
export interface SlowAdmissionTopic {
  /** The topic. */
  topic: string;
  /** Nearest-rank p99 of the topic's total-span window, in milliseconds. */
  p99Ms: number;
  /** Samples currently in the topic's total-span window. */
  samples: number;
  /** Slow-admission samples in the topic's total-span window. */
  slowCount: number;
}

/** Nearest-rank quantile of a sorted (ascending) sample array. */
function quantileRank(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const rank = Math.ceil(q * sorted.length) - 1;
  return sorted[Math.min(Math.max(rank, 0), sorted.length - 1)];
}

/** Bounded rolling window of millisecond samples with slow counting. */
class SampleWindow {
  private readonly samples: number[] = [];
  private readonly windowSize: number;
  private readonly slowThresholdMs: number;
  private sumMs = 0;
  private slow = 0;

  constructor(windowSize: number, slowThresholdMs: number) {
    this.windowSize = windowSize;
    this.slowThresholdMs = slowThresholdMs;
  }

  record(latencyMs: number): void {
    const sample = Math.max(0, latencyMs);
    this.samples.push(sample);
    this.sumMs += sample;
    if (sample >= this.slowThresholdMs) this.slow += 1;
    if (this.samples.length > this.windowSize) {
      const evicted = this.samples.splice(0, this.samples.length - this.windowSize);
      for (const e of evicted) {
        this.sumMs -= e;
        if (e >= this.slowThresholdMs) this.slow -= 1;
      }
    }
  }

  summary(): AdmissionLatencyStats {
    const n = this.samples.length;
    if (n === 0) {
      return {
        samples: 0,
        p50Ms: 0,
        p95Ms: 0,
        p99Ms: 0,
        minMs: 0,
        maxMs: 0,
        meanMs: 0,
        slowCount: 0,
      };
    }
    const sorted = [...this.samples].sort((a, b) => a - b);
    return {
      samples: n,
      p50Ms: quantileRank(sorted, 0.5),
      p95Ms: quantileRank(sorted, 0.95),
      p99Ms: quantileRank(sorted, 0.99),
      minMs: sorted[0],
      maxMs: sorted[n - 1],
      meanMs: this.sumMs / n,
      slowCount: this.slow,
    };
  }
}

/**
 * Per-topic publish-admission latency state: one total-span window per
 * topic plus one window per (topic, gate). All inputs are millisecond
 * deltas computed by the caller from its injected clock.
 */
export class AdmissionLatencyTracker {
  private readonly totals = new Map<string, SampleWindow>();
  private readonly gates = new Map<string, Map<AdmissionGate, SampleWindow>>();
  private readonly windowSize: number;
  private readonly gateWindowSize: number;
  private readonly slowThresholdMs: number;

  constructor(options?: AdmissionLatencyOptions) {
    const windowSize = options?.windowSize ?? 1024;
    if (!Number.isInteger(windowSize) || windowSize <= 0) {
      throw new RangeError('publishLatency.windowSize must be a positive integer');
    }
    const gateWindowSize = options?.gateWindowSize ?? 1024;
    if (!Number.isInteger(gateWindowSize) || gateWindowSize <= 0) {
      throw new RangeError('publishLatency.gateWindowSize must be a positive integer');
    }
    const slowThresholdMs = options?.slowAdmissionThresholdMs ?? 100;
    if (!Number.isFinite(slowThresholdMs) || slowThresholdMs < 0) {
      throw new RangeError(
        'publishLatency.slowAdmissionThresholdMs must be a non-negative finite number of milliseconds',
      );
    }
    this.windowSize = windowSize;
    this.gateWindowSize = gateWindowSize;
    this.slowThresholdMs = slowThresholdMs;
  }

  /** Records one total admission-span sample for a topic. */
  record(topic: string, latencyMs: number): void {
    let window = this.totals.get(topic);
    if (window === undefined) {
      window = new SampleWindow(this.windowSize, this.slowThresholdMs);
      this.totals.set(topic, window);
    }
    window.record(latencyMs);
  }

  /** Records one gate call-latency sample for a (topic, gate). */
  recordGate(topic: string, gate: AdmissionGate, latencyMs: number): void {
    let perGate = this.gates.get(topic);
    if (perGate === undefined) {
      perGate = new Map();
      this.gates.set(topic, perGate);
    }
    let window = perGate.get(gate);
    if (window === undefined) {
      window = new SampleWindow(this.gateWindowSize, this.slowThresholdMs);
      perGate.set(gate, window);
    }
    window.record(latencyMs);
  }

  /** Summarizes a topic's total-span window. Zeroed when the topic has no samples. */
  summary(topic: string): AdmissionLatencyStats {
    return this.totals.get(topic)?.summary() ?? new SampleWindow(1, this.slowThresholdMs).summary();
  }

  /**
   * Summarizes a topic's per-gate windows. Gates with no samples report
   * the zeroed summary, so callers can render a full gate table without
   * probing.
   */
  gateSummary(topic: string): Record<AdmissionGate, AdmissionLatencyStats> {
    const perGate = this.gates.get(topic);
    const zero = (): AdmissionLatencyStats => ({
      samples: 0,
      p50Ms: 0,
      p95Ms: 0,
      p99Ms: 0,
      minMs: 0,
      maxMs: 0,
      meanMs: 0,
      slowCount: 0,
    });
    const gates: AdmissionGate[] = [
      'webhook',
      'publisher-rate-limit',
      'acl',
      'schema',
      'size',
      'sampling',
      'pressure',
      'tenant-quota',
      'global-rate-limit',
      'topic-rate-limit',
      'idempotency-dedup',
    ];
    const out = {} as Record<AdmissionGate, AdmissionLatencyStats>;
    for (const gate of gates) out[gate] = perGate?.get(gate)?.summary() ?? zero();
    return out;
  }

  /**
   * Topics with at least one sample (total-span or per-gate), in
   * first-seen order.
   */
  topics(): string[] {
    const seen = new Set<string>(this.totals.keys());
    for (const topic of this.gates.keys()) seen.add(topic);
    return [...seen];
  }

  /**
   * The slow-admission hotspot ranking: topics with samples, sorted by
   * total-span p99 descending — the admission-gate hotspot list. Topics
   * whose schema validators (or ACL rule sets) are heavy surface here.
   */
  topSlowTopics(limit = 5): SlowAdmissionTopic[] {
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new RangeError('topSlowTopics limit must be a positive integer');
    }
    return [...this.totals.entries()]
      .map(([topic, window]) => {
        const s = window.summary();
        return { topic, p99Ms: s.p99Ms, samples: s.samples, slowCount: s.slowCount };
      })
      .filter((row) => row.samples > 0)
      .sort((a, b) => b.p99Ms - a.p99Ms || b.samples - a.samples)
      .slice(0, limit);
  }
}

/**
 * Validates `EventBusOptions.publishLatency` and returns the normalized
 * options, or `undefined` when sampling is disabled. `true` arms the
 * defaults; an options object tunes the windows and the slow threshold.
 * Anything else throws — a misconfigured observability switch must never
 * silently stay off (or on).
 */
export function resolveAdmissionLatencyOptions(
  opt: boolean | AdmissionLatencyOptions | undefined,
): AdmissionLatencyOptions | undefined {
  if (opt === undefined || opt === false) return undefined;
  if (opt === true) return {};
  if (typeof opt !== 'object' || opt === null) {
    throw new TypeError('publishLatency must be true or an AdmissionLatencyOptions object');
  }
  // Eager validation: the tracker constructor throws the same
  // RangeErrors, but failing here keeps the error attributable to the
  // option rather than to first publish.
  const windowSize = opt.windowSize ?? 1024;
  if (!Number.isInteger(windowSize) || windowSize <= 0) {
    throw new RangeError('publishLatency.windowSize must be a positive integer');
  }
  const gateWindowSize = opt.gateWindowSize ?? 1024;
  if (!Number.isInteger(gateWindowSize) || gateWindowSize <= 0) {
    throw new RangeError('publishLatency.gateWindowSize must be a positive integer');
  }
  const slowThresholdMs = opt.slowAdmissionThresholdMs ?? 100;
  if (!Number.isFinite(slowThresholdMs) || slowThresholdMs < 0) {
    throw new RangeError(
      'publishLatency.slowAdmissionThresholdMs must be a non-negative finite number of milliseconds',
    );
  }
  return { windowSize, gateWindowSize, slowAdmissionThresholdMs: slowThresholdMs };
}
