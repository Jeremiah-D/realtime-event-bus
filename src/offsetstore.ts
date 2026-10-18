/**
 * Pluggable backing store for consumer-group committed offsets (EB-114).
 *
 * `EventBus.commitOffset` checkpoints how far each consumer group has
 * processed each topic. Historically those checkpoints lived only in the
 * bus's in-memory map, so a restart — or a second bus instance behind a
 * load balancer — forgot them and consumers replayed from scratch.
 * `OffsetStore` extracts that storage behind an interface: the bus
 * mirrors every group-level `commitOffset` into the store and reseeds
 * from it via `EventBus.recoverFromOffsetStore()`, so a caller can plug
 * in a Redis/Postgres-backed store shared across processes and restarts
 * without changing the bus's synchronous commit API.
 *
 * The contract is deliberately narrow — `get`/`set` per (groupId, topic)
 * — mirroring the WR-108 `DedupStore` seam in webhook-relay-ts:
 *
 * - The bus's in-memory map stays the read path (`getCommittedOffsets`,
 *   group-lag rows), so behavior is identical with the default store.
 * - `set` runs synchronously on the commit path and is never awaited:
 *   a remote-backed store should apply the write to a local mirror
 *   synchronously and flush to the backend in the background (the same
 *   fire-and-forget convention as the durable-log journal append). A
 *   `set` that throws is counted in
 *   `getStats().offsetStoreWriteErrors` and never propagates into
 *   `commitOffset` — the live process keeps serving from memory.
 * - `entries()` is the optional recovery hook: `recoverFromOffsetStore`
 *   awaits it (it may be async for remote backends) and merges the
 *   triples into the in-memory map, highest-seq-wins, never regressing
 *   an offset the live process already committed past. Absent means no
 *   recovery — the default in-memory store returns `[]`.
 *
 * Scope note: only group-level checkpoints (`commitOffset` without
 * `partition`/`instanceId`) go through the store. Per-partition and
 * per-static-member checkpoints are internal rebalance bookkeeping tied
 * to a live roster, so they stay in memory by design.
 */

/** One persisted consumer-group checkpoint. */
export interface OffsetStoreEntry {
  /** Consumer group id. */
  groupId: string;
  /** Concrete topic. */
  topic: string;
  /** Last processed sequence number (a positive integer). */
  seq: number;
}

/**
 * Pluggable backing store for consumer-group committed offsets
 * (EB-114). The store owns durability and cross-instance visibility;
 * the bus owns the (groupId, topic) key shape and the in-memory read
 * path. Implementations must never throw on normal inputs.
 */
export interface OffsetStore {
  /**
   * The last committed seq for (groupId, topic), or `undefined` when the
   * store has no record. Synchronous: the bus never awaits the commit
   * path.
   */
  get(groupId: string, topic: string): number | undefined;
  /**
   * Persists `seq` for (groupId, topic). Called synchronously on the
   * `commitOffset` path and never awaited — see the module doc for the
   * remote-backend convention. Must never throw on normal inputs.
   */
  set(groupId: string, topic: string, seq: number): void;
  /**
   * Optional recovery hook: every persisted checkpoint. Called by
   * `EventBus.recoverFromOffsetStore()`; may be async for remote
   * backends. Absent means the store offers no recovery.
   */
  entries?(): OffsetStoreEntry[] | Promise<OffsetStoreEntry[]>;
  /**
   * Optional topic-rename hook (EB-142): moves every checkpoint filed
   * under `oldTopic` to `newTopic`. The bus calls it when present;
   * stores without it keep serving their old entries (the bus's own
   * in-memory checkpoints are still renamed, so the live process reads
   * the new name). Must never throw on normal inputs.
   */
  renameTopic?(oldTopic: string, newTopic: string): void;
}

/**
 * The default `OffsetStore` (EB-114): the exact in-memory map the bus
 * used before stores were pluggable, moved here verbatim. Group id ->
 * topic -> last committed seq. As a recovery source it yields nothing —
 * it only mirrors this process's commits.
 */
export class InMemoryOffsetStore implements OffsetStore {
  /** Group id -> concrete topic -> last committed `seq`. */
  private readonly offsets = new Map<string, Map<string, number>>();

  get(groupId: string, topic: string): number | undefined {
    return this.offsets.get(groupId)?.get(topic);
  }

  set(groupId: string, topic: string, seq: number): void {
    let byTopic = this.offsets.get(groupId);
    if (byTopic == null) {
      byTopic = new Map<string, number>();
      this.offsets.set(groupId, byTopic);
    }
    byTopic.set(topic, seq);
  }

  entries(): OffsetStoreEntry[] {
    // A pure mirror of this process's commits: nothing to recover that
    // the bus's own in-memory map does not already hold.
    return [];
  }

  /**
   * Live checkpoints, oldest group first. Not part of the `OffsetStore`
   * contract — it feeds tests and operator introspection of what this
   * process mirrored.
   */
  liveEntries(): OffsetStoreEntry[] {
    const out: OffsetStoreEntry[] = [];
    for (const [groupId, byTopic] of this.offsets) {
      for (const [topic, seq] of byTopic) out.push({ groupId, topic, seq });
    }
    return out;
  }

  /** Moves every checkpoint filed under `oldTopic` to `newTopic` (EB-142). */
  renameTopic(oldTopic: string, newTopic: string): void {
    if (oldTopic === newTopic) return;
    for (const byTopic of this.offsets.values()) {
      const seq = byTopic.get(oldTopic);
      if (seq !== undefined && !byTopic.has(newTopic)) {
        byTopic.delete(oldTopic);
        byTopic.set(newTopic, seq);
      }
    }
  }
}

/**
 * Validates `EventBusOptions.offsetStore` (EB-114), fail-fast: an invalid
 * store must never half-wire a bus. Returns `undefined` for absent,
 * so the bus falls back to `InMemoryOffsetStore`.
 */
export function resolveOffsetStore(store: unknown, owner: string): OffsetStore | undefined {
  if (store === undefined || store === null) return undefined;
  if (
    typeof store !== 'object' ||
    typeof (store as OffsetStore).get !== 'function' ||
    typeof (store as OffsetStore).set !== 'function'
  ) {
    throw new TypeError(
      `${owner}: offsetStore must implement OffsetStore.get() and OffsetStore.set(), got ${store === null ? 'null' : typeof store}`,
    );
  }
  return store as OffsetStore;
}
