/**
 * Dedup Bloom pre-filter benchmark (EB-214): measures the memory win of
 * the `BloomFilter` pre-filter in front of the EB-27 `(topic, messageId)`
 * dedup map.
 *
 * Compares, for N distinct `(topic, messageId)` claims:
 * - a `Map<string, number>` holding every key (the pre-EB-214 shape), vs
 * - a `BloomFilter` sized for N at p = 0.01 holding the same claims.
 *
 * Also measures the probe cost: `map.get` alone vs `bloom.has` +
 * `map.get` for absent keys (the common case — a first-time publish).
 *
 * Run: `node [--expose-gc] bench/bloom-dedup.bench.ts` (Node >= 22, no
 * build step). With `--expose-gc` the heap is collected between phases
 * for cleaner deltas; without it the numbers are noisier but the ratio
 * is the same order of magnitude. Numbers below were measured on this
 * machine; see README's idempotency section for the published table.
 */
import { BloomFilter } from '../src/bloom.ts';

const N = 100_000;

function messageId(i: number): string {
  // 64-char messageIds, injective in i by construction: the index is
  // embedded verbatim so absent keys can never collide with inserted
  // ones (a colliding generator would corrupt the probe benchmark).
  // The shared suffix mirrors real messageId schemes with common
  // prefixes — the harder case for the hash distribution.
  return i.toString(16).padStart(16, '0') + '9e3779b97f4a7c15'.repeat(3);
}

function percentile(sorted: number[], p: number): number {
  const rank = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, rank))];
}

const keys: string[] = [];
for (let i = 0; i < N; i += 1) keys.push(`orders.eu\0${messageId(i)}`);

// Phase 1+2: retained-size measurement. Each phase builds its structure,
// measures the heap with it alive, drops it, and measures again — the
// delta is the structure's retained size. A plain gc-before/gc-after
// around a local variable does not work: V8's precise GC collects the
// local before the second reading (it is dead after its last use), and a
// single `before` baseline includes whatever garbage the key corpus left
// behind. The `holder` object keeps the structure reachable across the
// gc() calls so the delta is honest.
function retainedBytes(build: () => unknown): number {
  const holder: { v: unknown } = { v: undefined };
  globalThis.gc?.();
  globalThis.gc?.();
  holder.v = build();
  globalThis.gc?.();
  globalThis.gc?.();
  const withStructure = process.memoryUsage().heapUsed;
  holder.v = undefined;
  globalThis.gc?.();
  globalThis.gc?.();
  const withoutStructure = process.memoryUsage().heapUsed;
  return withStructure - withoutStructure;
}

// The key corpus is shared by both phases, so neither phase's retained
// size includes the key strings themselves — the comparison is the
// container overhead: the Map's buckets + entry nodes vs the filter's
// bit array. That is the honest question: how much extra memory does the
// dedup structure cost on top of the keys the publisher already holds?
const mapBytes = retainedBytes(() => {
  const map = new Map<string, number>();
  for (let i = 0; i < N; i += 1) map.set(keys[i], i);
  return map;
});
console.log(`map: ${(mapBytes / 1024 / 1024).toFixed(2)} MiB retained for ${N} entries`);
const bloomBytes = retainedBytes(() => {
  const bloom = new BloomFilter(N, 0.01);
  for (let i = 0; i < N; i += 1) bloom.add(keys[i]);
  return bloom;
});
console.log(`bloom: ${(bloomBytes / 1024).toFixed(2)} KiB retained for ${N} claims (p=0.01)`);
console.log(`memory ratio: ${(mapBytes / Math.max(1, bloomBytes)).toFixed(1)}x smaller`);
// A fresh filter for the probe phase (rebuilt — the measured one was
// dropped by the retained-size measurement above).
const bloom = new BloomFilter(N, 0.01);
for (let i = 0; i < N; i += 1) bloom.add(keys[i]);

// Phase 3: probe latency for absent keys (first-time publishes).
const ABSENT = 20_000;
const absentKeys: string[] = [];
for (let i = 0; i < ABSENT; i += 1) absentKeys.push(`orders.eu\0${messageId(N + i)}`);
const map2 = new Map<string, number>();
for (let i = 0; i < N; i += 1) map2.set(keys[i], i);

function timeProbe(labeled: boolean): number[] {
  const times: number[] = [];
  // 11 reps, median — the probe is microseconds-fast, so scheduling noise
  // dominates any single rep.
  for (let r = 0; r < 11; r += 1) {
    const t0 = process.hrtime.bigint();
    let hits = 0;
    for (let i = 0; i < ABSENT; i += 1) {
      const k = absentKeys[i];
      if (labeled) {
        if (!bloom.has(k)) continue;
      }
      if (map2.get(k) !== undefined) hits += 1;
    }
    const t1 = process.hrtime.bigint();
    times.push(Number(t1 - t0) / 1e6);
    if (hits !== 0) throw new Error('unexpected hit');
  }
  return times.sort((a, b) => a - b);
}

const mapOnly = timeProbe(false);
const withBloom = timeProbe(true);
console.log(
  `absent-key probe p50: map-only ${percentile(mapOnly, 50).toFixed(2)}ms vs ` +
    `bloom+map ${percentile(withBloom, 50).toFixed(2)}ms ` +
    `for ${ABSENT} probes (${(percentile(mapOnly, 50) / Math.max(0.001, percentile(withBloom, 50))).toFixed(2)}x)`,
);
console.log(`bloom estimated FPR at ${N} insertions: ${bloom.estimatedFalsePositiveRate().toFixed(4)}`);
