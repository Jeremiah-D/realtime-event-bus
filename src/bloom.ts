/**
 * Bloom filter pre-filter for the publish-side idempotency dedup window
 * (EB-214, see `EventBusOptions.dedupBloom`): a space-efficient
 * probabilistic membership test in front of the exact EB-27
 * `(topic, messageId)` map.
 *
 * A negative answer is definitive — the key was never added — so the map
 * lookup is skipped. A positive answer falls through to the exact map,
 * which stays authoritative: a false positive costs one extra map lookup,
 * never a wrong suppression (zero false kills). The filter is a pure
 * read-through cache over the map's claim stream — it never changes what
 * the bus admits or suppresses.
 *
 * Zero dependencies: FNV-1a 32-bit with double hashing. The filter cannot
 * delete, so evicted/expired map entries leave stale bits behind; the bus
 * rebuilds the filter from the live table once insertions pass 2x the
 * sized capacity and exceed the live table size (see
 * `EventBus.refreshDedupBloom`), which restores the configured
 * false-positive rate with an amortized O(table) cost — the table itself
 * is bounded by `idempotencyMaxEntries`. When the live table itself
 * exceeds the sized capacity the filter is undersized by configuration;
 * rebuilding cannot help there, and the map stays authoritative so
 * correctness never degrades — only the pre-filter hit rate.
 *
 * Memory math (the point of the feature): sizing for n insertions at
 * false-positive rate p needs m = -n·ln(p)/(ln 2)² bits — about 9.6 bits
 * per entry at p = 0.01 — versus a `Map` entry holding the full
 * `topic\0messageId` string plus map overhead (hundreds of bytes per
 * entry for long messageIds). See `bench/bloom-dedup.bench.ts` for the
 * measured comparison.
 */

/** Sizing/accuracy knobs for `EventBusOptions.dedupBloom` (EB-214). */
export interface DedupBloomOptions {
  /**
   * How many distinct `(topic, messageId)` claims the filter is sized
   * for. Must be an integer >= 1. Defaults to the bus's
   * `idempotencyMaxEntries`.
   */
  expectedInsertions?: number;
  /**
   * Target false-positive rate, 0 < p < 1. A lower rate costs more bits
   * per entry (m/n = -ln(p)/(ln 2)²). Defaults to 0.01.
   */
  falsePositiveRate?: number;
}

/** Validated form of `EventBusOptions.dedupBloom` (EB-214). */
export interface ResolvedDedupBloomOptions {
  expectedInsertions: number;
  falsePositiveRate: number;
}

/**
 * Validates `EventBusOptions.dedupBloom` (EB-214). `true` selects the
 * defaults (sized for `idempotencyMaxEntries` at p = 0.01); `false` /
 * `undefined` disables the pre-filter; an object tunes the sizing.
 * Throws `RangeError` for a non-positive/non-integer
 * `expectedInsertions` or a `falsePositiveRate` outside (0, 1), and
 * `TypeError` for any other shape — fail-fast in the constructor, before
 * the bus exists.
 */
export function resolveDedupBloomOptions(
  raw: boolean | DedupBloomOptions | undefined,
  idempotencyMaxEntries: number,
  caller: string,
): ResolvedDedupBloomOptions | undefined {
  if (raw === undefined || raw === false) return undefined;
  let expectedInsertions = idempotencyMaxEntries;
  let falsePositiveRate = 0.01;
  if (raw === true) {
    // Defaults above.
  } else if (raw !== null && typeof raw === 'object' && !Array.isArray(raw)) {
    if (raw.expectedInsertions !== undefined) expectedInsertions = raw.expectedInsertions;
    if (raw.falsePositiveRate !== undefined) falsePositiveRate = raw.falsePositiveRate;
  } else {
    throw new TypeError(
      `${caller}: dedupBloom must be a boolean or an options object`,
    );
  }
  if (!Number.isInteger(expectedInsertions) || expectedInsertions < 1) {
    throw new RangeError(
      `${caller}: dedupBloom.expectedInsertions must be an integer >= 1`,
    );
  }
  if (
    !Number.isFinite(falsePositiveRate) ||
    falsePositiveRate <= 0 ||
    falsePositiveRate >= 1
  ) {
    throw new RangeError(
      `${caller}: dedupBloom.falsePositiveRate must be in the open interval (0, 1)`,
    );
  }
  return { expectedInsertions, falsePositiveRate };
}

/**
 * FNV-1a 32-bit hash of a string with a seed (EB-214): the two hash
 * functions for double hashing are the same primitive with different
 * seeds. 32-bit `Math.imul` arithmetic — no BigInt, no allocation.
 */
function fnv1a32(key: string, seed: number): number {
  let h = (0x811c9dc5 ^ seed) >>> 0;
  for (let i = 0; i < key.length; i += 1) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/**
 * A Bloom filter over string keys (EB-214). Not safe for concurrent
 * mutation — the bus only touches it from its synchronous publish paths.
 */
export class BloomFilter {
  private readonly bits: Uint32Array;
  private readonly bitSize: number;
  private readonly hashFunctions: number;
  private insertionCount = 0;

  constructor(expectedInsertions: number, falsePositiveRate: number) {
    const resolved = resolveDedupBloomOptions(
      { expectedInsertions, falsePositiveRate },
      expectedInsertions,
      'BloomFilter',
    ) as ResolvedDedupBloomOptions;
    const n = resolved.expectedInsertions;
    const p = resolved.falsePositiveRate;
    // m = -n·ln(p)/(ln 2)² bits, k = (m/n)·ln 2 hash functions — the
    // textbook sizing; k is at least 1 and capped so absurd configs stay
    // computable.
    const m = Math.max(64, Math.ceil((-n * Math.log(p)) / Math.LN2 ** 2));
    const k = Math.min(30, Math.max(1, Math.round((m / n) * Math.LN2)));
    this.bitSize = m;
    this.hashFunctions = k;
    this.bits = new Uint32Array(Math.ceil(m / 32));
  }

  /** Positions of `key` in the bit array (double hashing). */
  private positions(key: string): number[] {
    const h1 = fnv1a32(key, 0);
    // h2 must be odd and non-zero so the probe sequence covers the table;
    // the `>>> 0` keeps it unsigned — a negative h2 would make positions
    // negative, and out-of-bounds bit-array writes are silently dropped,
    // which would corrupt into false negatives.
    const h2 = (fnv1a32(key, 0x9e3779b9) | 1) >>> 0;
    const out: number[] = [];
    // Iterative double hashing: h_i = (h1 + i*h2) mod 2^32, kept unsigned
    // throughout — positions are always in [0, bitSize).
    let h = h1;
    for (let i = 0; i < this.hashFunctions; i += 1) {
      out.push(h % this.bitSize);
      h = (h + h2) >>> 0;
    }
    return out;
  }

  /** Records `key` as present. Idempotent — re-adding is harmless. */
  add(key: string): void {
    for (const pos of this.positions(key)) {
      this.bits[pos >>> 5] |= 1 << (pos & 31);
    }
    this.insertionCount += 1;
  }

  /**
   * Membership probe: `false` is definitive (the key was never added);
   * `true` means "maybe" — the caller must confirm against the exact
   * store. Never a false negative.
   */
  has(key: string): boolean {
    for (const pos of this.positions(key)) {
      if ((this.bits[pos >>> 5] & (1 << (pos & 31))) === 0) return false;
    }
    return true;
  }

  /** Claims recorded since construction / the last `rebuild`. */
  get insertions(): number {
    return this.insertionCount;
  }

  /** Bit array length. */
  get size(): number {
    return this.bitSize;
  }

  /** Number of hash functions (probe positions per key). */
  get hashes(): number {
    return this.hashFunctions;
  }

  /**
   * Estimated false-positive rate at the current insertion count:
   * (1 − e^(−k·n/m))^k. The bus uses it for the saturation guard —
   * callers must not treat it as a measured rate.
   */
  estimatedFalsePositiveRate(): number {
    const { bitSize: m, hashFunctions: k, insertionCount: n } = this;
    if (n === 0) return 0;
    return (1 - Math.exp((-k * n) / m)) ** k;
  }

  /**
   * Clears the filter and re-adds `keys` (EB-214 saturation guard /
   * EB-66 durable recovery): the only way to drop stale bits, since the
   * filter cannot delete. `insertions` restarts from the rebuilt count.
   */
  rebuild(keys: Iterable<string>): void {
    this.bits.fill(0);
    this.insertionCount = 0;
    for (const key of keys) this.add(key);
  }
}
