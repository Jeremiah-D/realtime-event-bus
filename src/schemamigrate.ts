/**
 * Schema-version payload auto-migration (EB-186).
 *
 * Schema validators (EB-86, `setTopicSchemaVersion`) let many payload
 * versions coexist on one topic, but they only *validate* — every
 * consumer still sees the exact bytes the producer wrote. When a schema
 * evolves, old consumers keep speaking the old shape and new consumers
 * the new shape; without a transform layer one side must upgrade in
 * lockstep with the producer.
 *
 * This module is the transform layer: operators register per-hop
 * migration functions between adjacent schema versions
 * (`EventBus.setSchemaMigration`), and the bus resolves multi-hop
 * paths (upcasts and downcasts alike) so each message is migrated from
 * its *write version* (`PublishOptions.schemaVersion`, stamped on the
 * envelope at admission) to the *expected version* a subscriber declares
 * (`SubscribeOptions.schemaVersion`) — at delivery time, after
 * decompression and signature verification, before the handler runs.
 *
 * Design notes:
 * - Hops are registered as `(fromVersion, toVersion)` edges; the
 *   registry finds the shortest hop path with BFS, so a direct `1→3`
 *   registration and a stepwise `1→2, 2→3` chain compose freely. Each
 *   hop's function sees `(payload, fromVersion, toVersion)`.
 * - Fail-closed: a missing hop — or a hop function that throws — drops
 *   the message for that subscriber and counts it in
 *   `migrationFailures` (per-topic and global). A half-migrated payload
 *   is never delivered.
 * - Zero-copy for the common case: a subscriber that declares no
 *   version, or whose version equals the write version, gets the
 *   original payload untouched.
 * - Signed topics (EB-84): the signature is verified against the stored
 *   write-version wire bytes *before* migration, and the migrated view
 *   is a local deterministic transform of that authenticated payload.
 *   The signature is deliberately not recomputed over the migrated
 *   payload: signatures bind `(topic, seq, wire bytes)` on the shared
 *   envelope, and the migrated view never re-enters the wire — the
 *   durable log and replay keep the write version. A verify-then-migrate
 *   ordering is what makes this sound: authenticity is established on
 *   the stored bytes first, migration second.
 * - Pure: no bus state, no clock — the bus owns rule storage, pattern
 *   matching, delivery-time application, and stats.
 */

export type SchemaMigrateFn = (
  payload: unknown,
  fromVersion: number,
  toVersion: number,
) => unknown;

export interface SchemaMigrationStep {
  fromVersion: number;
  toVersion: number;
  migrate: SchemaMigrateFn;
}

export interface SchemaMigrationApplied {
  ok: true;
  payload: unknown;
}

export interface SchemaMigrationFailed {
  ok: false;
  /** The hop version the walk was stuck at. */
  missingFrom: number;
  /** The hop version that had no registered edge into it. */
  missingTo: number;
}

export type SchemaMigrationResult = SchemaMigrationApplied | SchemaMigrationFailed;

function stepKey(fromVersion: number, toVersion: number): string {
  return `${fromVersion}->${toVersion}`;
}

function checkVersion(version: number, name: string): void {
  if (!Number.isInteger(version) || version < 0) {
    throw new RangeError(`${name} must be a non-negative integer`);
  }
}

/**
 * One topic (or topic pattern)'s migration graph. Edges are directed
 * `(fromVersion → toVersion)` hops; `resolvePath` finds the shortest
 * path for either direction, so upcasts and downcasts share one graph.
 */
export class SchemaMigrationRegistry {
  private readonly steps = new Map<string, SchemaMigrationStep>();

  /**
   * Registers one migration hop. Throws `RangeError` when a version is
   * not a non-negative integer, `fromVersion === toVersion`, the hop is
   * already registered, or `migrate` is not a function — fail-fast at
   * configuration time, never mid-delivery.
   */
  register(fromVersion: number, toVersion: number, migrate: SchemaMigrateFn): void {
    checkVersion(fromVersion, 'fromVersion');
    checkVersion(toVersion, 'toVersion');
    if (fromVersion === toVersion) {
      throw new RangeError('fromVersion and toVersion must differ');
    }
    if (typeof migrate !== 'function') {
      throw new RangeError('migrate must be a function');
    }
    const key = stepKey(fromVersion, toVersion);
    if (this.steps.has(key)) {
      throw new RangeError(
        `migration hop ${fromVersion}->${toVersion} is already registered`,
      );
    }
    this.steps.set(key, { fromVersion, toVersion, migrate });
  }

  /** Removes every registered hop. */
  clear(): void {
    this.steps.clear();
  }

  /** Registered hop count. */
  get size(): number {
    return this.steps.size;
  }

  private adjacency(): Map<number, SchemaMigrationStep[]> {
    const adj = new Map<number, SchemaMigrationStep[]>();
    for (const step of this.steps.values()) {
      const list = adj.get(step.fromVersion);
      if (list === undefined) adj.set(step.fromVersion, [step]);
      else list.push(step);
    }
    // Deterministic BFS: lowest target version first.
    for (const list of adj.values()) list.sort((a, b) => a.toVersion - b.toVersion);
    return adj;
  }

  /**
   * Shortest hop path from `fromVersion` to `toVersion` (BFS over the
   * registered edges); `[]` when both are equal; `undefined` when no
   * path exists. Never throws: a missing hop is data, reported by
   * `migrate`.
   */
  resolvePath(fromVersion: number, toVersion: number): SchemaMigrationStep[] | undefined {
    if (fromVersion === toVersion) return [];
    const adj = this.adjacency();
    const visited = new Set<number>([fromVersion]);
    const prev = new Map<number, SchemaMigrationStep>();
    const queue: number[] = [fromVersion];
    while (queue.length > 0) {
      const current = queue.shift() as number;
      for (const step of adj.get(current) ?? []) {
        if (visited.has(step.toVersion)) continue;
        visited.add(step.toVersion);
        prev.set(step.toVersion, step);
        if (step.toVersion === toVersion) {
          const path: SchemaMigrationStep[] = [];
          let v = toVersion;
          while (v !== fromVersion) {
            const s = prev.get(v) as SchemaMigrationStep;
            path.unshift(s);
            v = s.fromVersion;
          }
          return path;
        }
        queue.push(step.toVersion);
      }
    }
    return undefined;
  }

  /**
   * Migrates `payload` from `fromVersion` to `toVersion` along the
   * resolved path. Fail-closed: a missing hop — or a hop function that
   * throws — yields `{ ok: false }` with the unbridgeable hop; the
   * caller drops the message and counts it. A half-migrated payload is
   * never returned.
   */
  migrate(
    payload: unknown,
    fromVersion: number,
    toVersion: number,
  ): SchemaMigrationResult {
    if (fromVersion === toVersion) return { ok: true, payload };
    const path = this.resolvePath(fromVersion, toVersion);
    if (path === undefined) {
      // Report the first unbridgeable hop: walk greedily from the write
      // version until no registered edge leaves the current version.
      const adj = this.adjacency();
      let stuck = fromVersion;
      for (;;) {
        const next = (adj.get(stuck) ?? [])[0];
        if (next === undefined || next.toVersion === stuck) break;
        stuck = next.toVersion;
        if (stuck === toVersion) break;
      }
      return { ok: false, missingFrom: stuck, missingTo: toVersion };
    }
    let current = payload;
    for (const step of path) {
      try {
        current = step.migrate(current, step.fromVersion, step.toVersion);
      } catch {
        return { ok: false, missingFrom: step.fromVersion, missingTo: step.toVersion };
      }
    }
    return { ok: true, payload: current };
  }
}
