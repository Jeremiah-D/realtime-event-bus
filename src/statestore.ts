/**
 * Materialized state store — the KTable-style query side of keyed publish
 * (EB-171).
 *
 * Kafka-Streams-style semantics: every admitted keyed publish updates the
 * per-(topic, key) latest value, and `getState`/`scan` read it back without
 * touching subscriber queues. This is the *query* dual of EB-26's keyed
 * log compaction (the log keeps the latest per key on disk; the store keeps
 * the latest per key in memory for reads) — it does not rewrite the log,
 * it observes it.
 *
 * Update semantics (see `StateStore.apply`):
 * - One update per admitted keyed publish, at the bus's admission point
 *   (post-gates, post-sequence-assignment, pre-compression): the store
 *   sees the raw application payload with its final publish-order seq.
 *   Rejected or shed publishes never reach it.
 * - `version` is a per-(topic, key) update counter starting at 1 — the
 *   changelog consumer's ordering key.
 * - Memory is bounded by `maxKeys`: the least-recently-updated key is
 *   evicted first (LRU); evictions are counted, never silent.
 * - An opt-in `changelogTopic` emits one record per update
 *   (`{ topic, key, value, oldValue, seq, at, version }`). Changelog
 *   records are *unkeyed*, so they can never feed back into the store —
 *   the recursion is cut by construction, and the key rides in the
 *   payload instead.
 * - Durable restore: the bus replays the durable log's per-(topic, key)
 *   latest records into the store when the store is enabled over a log
 *   (`enableStateStore` rebuilds), so a restarted process keeps its
 *   table. TTL-expired records are dead — they never resurrect state.
 *
 * The class is pure in-memory state: the bus owns the observation point,
 * the changelog emission (its own publish pipeline) and the durable-log
 * rebuild. All the class needs per observation is the topic, key,
 * payload and seq.
 */

/** One row of the materialized table. */
export interface StateEntry {
  /** The alias-resolved concrete topic the update was published to. */
  topic: string;
  /** The publish key. */
  key: string;
  /** The latest admitted payload for (topic, key). */
  value: unknown;
  /** The publish-order sequence number of the update that wrote this row. */
  seq: number;
  /** Bus-clock time of the update. */
  at: number;
  /** Per-(topic, key) update counter, starting at 1. */
  version: number;
}

/** Options for `enableStateStore`. */
export interface StateStoreOptions {
  /**
   * Maximum keys tracked across all topics (default 10000). Past it, the
   * least-recently-updated key is evicted (`evictedKeys` counts it).
   * Must be a positive integer.
   */
  maxKeys?: number;
  /**
   * Opt-in changelog topic: every state update emits one record there —
   * `{ topic, key, value, oldValue, seq, at, version }` — unkeyed, so it
   * never feeds back into the store. Old-value tracking doubles the
   * retained payloads; omit the topic when the table is read-only.
   */
  changelogTopic?: string;
}

/** Live counters for the state-store surface (`getStats().stateStore`). */
export interface StateStoreStats {
  /** Keys currently tracked. */
  keys: number;
  /** Keys evicted by the `maxKeys` bound since the store was created. */
  evictedKeys: number;
}

const DEFAULT_MAX_KEYS = 10000;

interface StoredRow {
  entry: StateEntry;
}

export interface StateStoreDeps {
  /** Bus clock in milliseconds (`EventBusOptions.now`). */
  now: () => number;
}

/**
 * Validates state-store options the way a bus setter would: absent is
 * fine, otherwise `RangeError` — thrown before anything is enabled.
 */
export function validateStateStoreOptions(opts: StateStoreOptions | undefined): void {
  if (opts === undefined) return;
  if (opts === null || typeof opts !== 'object') {
    throw new RangeError('state store options must be an object');
  }
  if (opts.maxKeys !== undefined) {
    if (typeof opts.maxKeys !== 'number' || !Number.isInteger(opts.maxKeys) || opts.maxKeys <= 0) {
      throw new RangeError('maxKeys must be a positive integer');
    }
  }
  if (opts.changelogTopic !== undefined) {
    if (typeof opts.changelogTopic !== 'string' || opts.changelogTopic.length === 0) {
      throw new RangeError('changelogTopic must be a non-empty string');
    }
  }
}

export class StateStore {
  private readonly deps: StateStoreDeps;
  private readonly maxKeys: number;
  /** Composite key `${topic}\u0000${key}` → row. Map iteration order is the LRU order (oldest first). */
  private readonly rows = new Map<string, StoredRow>();
  private evictedKeys = 0;

  constructor(deps: StateStoreDeps, opts?: StateStoreOptions) {
    this.deps = deps;
    this.maxKeys = opts?.maxKeys ?? DEFAULT_MAX_KEYS;
  }

  /**
   * Applies one admitted publish to the table. Returns the written entry,
   * or `undefined` when the publish carried no key (unkeyed publishes
   * never participate — this is also what keeps changelog records out).
   *
   * LRU order: a touched row moves to the most-recently-used end, so the
   * `maxKeys` eviction always drops the least-recently-updated key.
   */
  apply(topic: string, key: string | undefined, value: unknown, seq: number): StateEntry | undefined {
    if (key === undefined) return undefined;
    const now = this.deps.now();
    const composite = `${topic}\u0000${key}`;
    const existing = this.rows.get(composite);
    const entry: StateEntry = {
      topic,
      key,
      value,
      seq,
      at: now,
      version: (existing?.entry.version ?? 0) + 1,
    };
    // Re-insert to refresh LRU position: Map keeps insertion order, so
    // delete + set moves the row to the newest end.
    if (existing !== undefined) this.rows.delete(composite);
    this.rows.set(composite, { entry });
    // Bound the table: evict the least-recently-updated rows first.
    while (this.rows.size > this.maxKeys) {
      const oldest = this.rows.keys().next();
      if (oldest.done) break;
      this.rows.delete(oldest.value);
      this.evictedKeys += 1;
    }
    return entry;
  }

  /**
   * Rebuilds one row from a durable-log record (restart recovery): the
   * record's seq wins over the live row only when it is newer — records
   * replay in ascending seq order, so the last write per key wins
   * naturally. TTL-expired records are skipped by the caller.
   */
  restore(topic: string, key: string, value: unknown, seq: number, at: number): void {
    const composite = `${topic}\u0000${key}`;
    const existing = this.rows.get(composite);
    if (existing !== undefined && existing.entry.seq >= seq) return;
    const entry: StateEntry = {
      topic,
      key,
      value,
      seq,
      at,
      version: (existing?.entry.version ?? 0) + 1,
    };
    if (existing !== undefined) this.rows.delete(composite);
    this.rows.set(composite, { entry });
    while (this.rows.size > this.maxKeys) {
      const oldest = this.rows.keys().next();
      if (oldest.done) break;
      this.rows.delete(oldest.value);
      this.evictedKeys += 1;
    }
  }

  /** Reads the latest entry for (topic, key), or `undefined`. */
  get(topic: string, key: string): StateEntry | undefined {
    return this.rows.get(`${topic}\u0000${key}`)?.entry;
  }

  /**
   * Scans the table in deterministic order (topic, then key). Both
   * filters are optional; `keyPrefix` matches the key's start.
   */
  scan(topic?: string, keyPrefix?: string): StateEntry[] {
    const out: StateEntry[] = [];
    for (const { entry } of this.rows.values()) {
      if (topic !== undefined && entry.topic !== topic) continue;
      if (keyPrefix !== undefined && !entry.key.startsWith(keyPrefix)) continue;
      out.push(entry);
    }
    out.sort((a, b) => (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    return out;
  }

  /** Live counters for `getStats().stateStore`. */
  snapshot(): StateStoreStats {
    return { keys: this.rows.size, evictedKeys: this.evictedKeys };
  }

  /** Clears the table (used by `disableStateStore`); eviction history is kept. */
  clear(): void {
    this.rows.clear();
  }
}
