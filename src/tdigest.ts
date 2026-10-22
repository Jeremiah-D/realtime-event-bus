/**
 * t-digest quantile sketch (EB-199).
 *
 * Per-subscriber latency tracking (`DeliveryLatencyTracker` in
 * `latency.ts`) keeps an exact window of samples and sorts it for every
 * nearest-rank percentile. That is exact but memory-unbounded per
 * subscriber: at high cardinality (many topics x many subscribers) the
 * windows dominate memory, and the per-summary sort costs O(n log n).
 *
 * This module is the estimation-algorithm alternative: a t-digest keeps a
 * small set of centroids (mean + count) whose size stays bounded by the
 * compression parameter delta, independent of how many samples were added.
 * Quantiles are estimated by interpolation between centroids instead of
 * exact sorting. It is orthogonal to the window statistics in `latency.ts`
 * — that module is a *window* (which samples are kept), this module is an
 * *estimator* (how quantiles are computed from the samples). The two can
 * be combined (a window feeding a digest) or used independently.
 *
 * The implementation is the classic Dunning t-digest with the K_1 scale
 * function: greedy single-pass compression of a sort buffer, centroid
 * size bound `4 * n * q * (1 - q) / delta` at quantile q. Zero
 * dependencies; the digest itself takes no clock because it only ever
 * sees already-computed deltas (milliseconds, like `latency.ts`).
 */

/** Tuning for a {@link TDigest}. */
export interface TDigestOptions {
  /**
   * Compression parameter delta. Higher delta means more centroids and
   * tighter error bounds; the centroid count stays O(delta) no matter how
   * many samples are added. Must be a positive finite number. Default 100
   * (the classic default: ~a few hundred centroids, sub-1% relative error
   * on smooth distributions at p50/p95/p99).
   */
  compression?: number;
  /**
   * How many points to buffer before a compression pass. Larger buffers
   * compress better (fewer, tighter centroids) at the cost of a bigger
   * transient sort. Must be a positive integer. Default:
   * `max(64, 8 * compression)`.
   */
  bufferSize?: number;
}

/** One t-digest centroid: a cluster of nearby samples. */
export interface TDigestCentroid {
  /** Weighted mean of the samples in this centroid. */
  mean: number;
  /** Total weight of the samples in this centroid. */
  count: number;
}

interface MutableCentroid {
  mean: number;
  count: number;
}

/**
 * Streaming quantile sketch with bounded memory. Add millisecond deltas
 * with {@link add}; estimate quantiles with {@link quantile}. The digest
 * is deterministic: the same insert sequence always yields the same
 * centroids.
 */
export class TDigest {
  private readonly compression: number;
  private readonly bufferSize: number;
  private centroids: MutableCentroid[] = [];
  private buffer: MutableCentroid[] = [];
  private total = 0;

  constructor(options: TDigestOptions = {}) {
    const compression = options.compression ?? 100;
    if (!Number.isFinite(compression) || compression <= 0) {
      throw new RangeError("TDigest: `compression` must be a positive finite number");
    }
    const bufferSize = options.bufferSize ?? Math.max(64, 8 * Math.ceil(compression));
    if (!Number.isInteger(bufferSize) || bufferSize <= 0) {
      throw new RangeError("TDigest: `bufferSize` must be a positive integer");
    }
    this.compression = compression;
    this.bufferSize = bufferSize;
  }

  /**
   * Records one sample. `weight` lets one call stand in for several
   * identical samples (default 1). Negative samples are allowed (the
   * sketch is order-based, not value-based); non-finite values and
   * non-positive weights are rejected.
   */
  add(value: number, weight = 1): void {
    if (!Number.isFinite(value)) {
      throw new RangeError("TDigest.add: `value` must be finite");
    }
    if (!Number.isFinite(weight) || weight <= 0) {
      throw new RangeError("TDigest.add: `weight` must be a positive finite number");
    }
    this.buffer.push({ mean: value, count: weight });
    this.total += weight;
    if (this.buffer.length >= this.bufferSize) {
      this.compress();
    }
  }

  /**
   * Estimates the q-th quantile (q in [0, 1]) by interpolation between
   * centroid means. Returns 0 on an empty digest (consistent with
   * `DeliveryLatencyTracker.summary()`).
   */
  quantile(q: number): number {
    if (!Number.isFinite(q) || q < 0 || q > 1) {
      throw new RangeError("TDigest.quantile: `q` must be in [0, 1]");
    }
    if (this.total === 0) return 0;
    this.compress();
    const cs = this.centroids;
    if (cs.length === 1) return cs[0].mean;
    // Each centroid i sits at cumulative position (soFar + count/2) / n
    // with value mean_i; walk the positions and interpolate linearly.
    const target = q;
    let prevPos = 0;
    let prevMean = cs[0].mean;
    let soFar = 0;
    for (const c of cs) {
      const pos = (soFar + c.count / 2) / this.total;
      if (target <= pos) {
        if (pos === prevPos) return c.mean;
        const f = (target - prevPos) / (pos - prevPos);
        return prevMean + f * (c.mean - prevMean);
      }
      prevPos = pos;
      prevMean = c.mean;
      soFar += c.count;
    }
    return cs[cs.length - 1].mean;
  }

  /** Total weight added (buffered + compressed). */
  get count(): number {
    return this.total;
  }

  /** Number of centroids currently held (after a compression pass). */
  get centroidCount(): number {
    this.compress();
    return this.centroids.length;
  }

  /** A sorted copy of the current centroids (mean ascending). */
  getCentroids(): TDigestCentroid[] {
    this.compress();
    return this.centroids.map((c) => ({ mean: c.mean, count: c.count }));
  }

  /** Drops all samples and centroids. */
  clear(): void {
    this.centroids = [];
    this.buffer = [];
    this.total = 0;
  }

  /**
   * Greedy K_1 merge: sort buffered points with existing centroids and
   * absorb each point into the open centroid while the merged weight
   * stays under `4 * n * q * (1 - q) / compression` at its right-edge
   * quantile q (at least 1, so tail singletons survive and tails stay
   * accurate).
   */
  private compress(): void {
    if (this.buffer.length === 0) return;
    const points = [...this.centroids, ...this.buffer].sort((a, b) => a.mean - b.mean);
    this.buffer = [];
    const n = this.total;
    const merged: MutableCentroid[] = [];
    let cur: MutableCentroid | null = null;
    let soFar = 0;
    for (const p of points) {
      if (cur !== null) {
        const q1 = (soFar + p.count) / n;
        const limit = Math.max(1, (4 * n * q1 * (1 - q1)) / this.compression);
        if (cur.count + p.count <= limit) {
          cur.mean = (cur.mean * cur.count + p.mean * p.count) / (cur.count + p.count);
          cur.count += p.count;
          soFar += p.count;
          continue;
        }
      }
      if (cur !== null) merged.push(cur);
      cur = { mean: p.mean, count: p.count };
      soFar += p.count;
    }
    if (cur !== null) merged.push(cur);
    this.centroids = merged;
  }
}
