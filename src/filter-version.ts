/**
 * Versioned subscription filter expressions (EB-229).
 *
 * A topic's subscription filter predicate is a living rule: operators
 * tighten it, loosen it, or fix it, and every change rewrites which
 * messages a subscriber would have admitted. Evaluating old records
 * under the *current* predicate would silently rewrite history — a
 * replay that drops a message the old predicate admitted, or admits
 * one the old predicate rejected, and no audit trail could tell the
 * difference. So this module versions predicates:
 *
 * - The first predicate for a topic is version 1. `updateTopicFilter`
 *   appends version N+1; older versions are never mutated or deleted,
 *   so the full history stays auditable (`history()`).
 * - `rollback` does NOT rewind the version counter: it appends a new
 *   version whose source copies the target historical version. The
 *   rollback itself is therefore part of the audit trail, and a later
 *   `history()` shows exactly when the old predicate came back.
 * - Replay/backfill never consults the current predicate. `stamp`
 *   records the filter version in force when a record was admitted;
 *   `evaluateRecord` re-evaluates that record under exactly that
 *   version. A record stamped under an unknown version (or a version
 *   from a different topic) fails closed (`false`) instead of silently
 *   falling back to the current predicate — a silent fallback would
 *   be precisely the history-rewriting this module exists to prevent.
 * - A record admitted with no filter in force stamps version 0, which
 *   always evaluates to admitted: "no filter" is a judgment too, and
 *   replay must not invent a filter after the fact.
 *
 * Orthogonality, by design:
 * - EB-95 (schema compatibility gate) judges message *structure*;
 *   this module judges the subscription *predicate* and says nothing
 *   about structure.
 * - EB-20 (SchemaValidator) validates a message against one schema
 *   version at publish time; this module versions the subscriber-side
 *   predicate, not the schema.
 * - EB-147 (publish-side transform) rewrites payloads before fan-out;
 *   this module versions the subscriber-side predicate that runs
 *   after fan-out. A transform version and a filter version are
 *   independent counters.
 *
 * Zero dependencies. This module never touches the bus: it owns only
 * the predicate lifecycle, so a topic's filter history can be audited
 * without a live bus instance.
 */

import {
  compileFilterExpression,
  type CompiledFilterExpression,
} from './filterexpr.ts';

/** Version stamped on records admitted while no filter was in force. */
export const NO_FILTER_VERSION = 0;

/**
 * One version of a topic's filter predicate. Immutable: once recorded,
 * a version's source and compiled predicate never change, so an audit
 * of `history()` is an audit of what actually ran.
 */
export interface FilterVersionRecord {
  /** Monotonically increasing per topic, starting at 1. */
  readonly version: number;
  /** The expression source, verbatim. */
  readonly source: string;
  /** The compiled predicate, stamped with this version. */
  readonly compiled: CompiledFilterExpression;
  /** Epoch milliseconds when this version was recorded. */
  readonly createdAt: number;
  /**
   * The version that superseded this one, or `undefined` while this
   * version is current. Set on the old record when a newer version
   * lands, so the chain current → superseded is walkable both ways.
   */
  readonly supersededBy: number | undefined;
  /**
   * When this version is a rollback, the historical version it copies;
   * `undefined` for ordinary creates and updates.
   */
  readonly rolledBackFrom: number | undefined;
}

/**
 * A record stamped at admit time with the filter version in force
 * then. Replay and backfill evaluate through `evaluateRecord`, never
 * through the topic's current predicate.
 */
export interface FilterVersionedRecord {
  readonly payload: unknown;
  /** The admit-time filter version, or 0 when no filter was in force. */
  readonly filterVersion: number;
}

function assertTopic(topic: string, what: string): void {
  if (typeof topic !== 'string' || topic.length === 0) {
    throw new TypeError(`${what}: topic must be a non-empty string`);
  }
}

/**
 * Per-topic lifecycle for versioned filter predicates.
 *
 * All state lives in memory: this is the predicate-governance layer,
 * not a persistence layer. A host that needs the history to survive a
 * restart snapshots `history()` and rebuilds with `createTopicFilter`
 * / `updateTopicFilter` in version order (sources are verbatim, so a
 * replayed history reproduces identical predicates).
 */
export class TopicFilterVersions {
  private readonly chains = new Map<string, FilterVersionRecord[]>();

  /**
   * Records the first predicate for a topic as version 1.
   *
   * @throws `RangeError` when the topic already has a filter (use
   * `updateTopicFilter`), and rethrows the compiler's `RangeError` /
   * `TypeError` for an invalid expression.
   */
  createTopicFilter(topic: string, expr: string, now = Date.now()): FilterVersionRecord {
    assertTopic(topic, 'createTopicFilter');
    if (this.chains.has(topic)) {
      throw new RangeError(
        `createTopicFilter: topic "${topic}" already has a filter (version ${this.currentVersion(topic)}); use updateTopicFilter`,
      );
    }
    const record = this.append(topic, expr, 1, now, undefined);
    return record;
  }

  /**
   * Appends a new predicate version (current + 1) for a topic. Old
   * versions are retained untouched for audit and rollback.
   *
   * @throws `RangeError` when the topic has no filter yet (use
   * `createTopicFilter`), and rethrows the compiler's errors for an
   * invalid expression.
   */
  updateTopicFilter(topic: string, expr: string, now = Date.now()): FilterVersionRecord {
    assertTopic(topic, 'updateTopicFilter');
    const chain = this.chains.get(topic);
    if (chain === undefined) {
      throw new RangeError(
        `updateTopicFilter: topic "${topic}" has no filter; use createTopicFilter first`,
      );
    }
    const next = chain[chain.length - 1].version + 1;
    return this.append(topic, expr, next, now, undefined);
  }

  /**
   * Rolls a topic back to a historical version by appending a new
   * version that copies that version's source. The version counter
   * never rewinds, so the rollback is itself auditable.
   *
   * @throws `RangeError` when the topic or the target version is
   * unknown.
   */
  rollback(topic: string, version: number, now = Date.now()): FilterVersionRecord {
    assertTopic(topic, 'rollback');
    const target = this.getVersion(topic, version);
    if (target === undefined) {
      throw new RangeError(
        `rollback: topic "${topic}" has no filter version ${version}`,
      );
    }
    const chain = this.chains.get(topic) as FilterVersionRecord[];
    const next = chain[chain.length - 1].version + 1;
    return this.append(topic, target.source, next, now, version);
  }

  /** The topic's current predicate version, or `undefined` when unset. */
  current(topic: string): FilterVersionRecord | undefined {
    assertTopic(topic, 'current');
    const chain = this.chains.get(topic);
    return chain === undefined ? undefined : chain[chain.length - 1];
  }

  /** The topic's current version number, or `undefined` when unset. */
  currentVersion(topic: string): number | undefined {
    return this.current(topic)?.version;
  }

  /** One historical version, or `undefined` when unknown. */
  getVersion(topic: string, version: number): FilterVersionRecord | undefined {
    assertTopic(topic, 'getVersion');
    const chain = this.chains.get(topic);
    if (chain === undefined) return undefined;
    return chain.find((r) => r.version === version);
  }

  /**
   * The topic's full version history in version order. The returned
   * records are the stored (immutable) records; callers must not
   * mutate them, and this method never returns a copy that could
   * drift from the audit trail.
   */
  history(topic: string): readonly FilterVersionRecord[] {
    assertTopic(topic, 'history');
    return this.chains.get(topic) ?? [];
  }

  /** Topics that have at least one filter version, in creation order. */
  topics(): string[] {
    return [...this.chains.keys()];
  }

  /**
   * Evaluates a payload under the topic's current predicate. A topic
   * with no filter admits everything: absence of a predicate is not a
   * rejection.
   */
  evaluateCurrent(topic: string, payload: unknown): boolean {
    const record = this.current(topic);
    if (record === undefined) return true;
    return record.compiled.test(payload);
  }

  /**
   * Evaluates a payload under one historical version. Unknown topics
   * and unknown versions fail closed (`false`): replay must never
   * silently substitute the current predicate for a version it cannot
   * find.
   */
  evaluateAtVersion(topic: string, version: number, payload: unknown): boolean {
    if (version === NO_FILTER_VERSION) return true;
    const record = this.getVersion(topic, version);
    if (record === undefined) return false;
    return record.compiled.test(payload);
  }

  /**
   * Stamps a payload with the filter version in force at admit time.
   * The stamp is what replay and backfill later evaluate against.
   */
  stamp(topic: string, payload: unknown): FilterVersionedRecord {
    return { payload, filterVersion: this.currentVersion(topic) ?? NO_FILTER_VERSION };
  }

  /**
   * Re-evaluates an admit-stamped record under exactly the version it
   * was admitted with — never the current predicate. A stamp for an
   * unknown version fails closed, so a replay cannot silently rewrite
   * a historical admit/reject judgment.
   */
  evaluateRecord(topic: string, record: FilterVersionedRecord): boolean {
    if (record === null || typeof record !== 'object') return false;
    return this.evaluateAtVersion(topic, record.filterVersion, record.payload);
  }

  private append(
    topic: string,
    expr: string,
    version: number,
    now: number,
    rolledBackFrom: number | undefined,
  ): FilterVersionRecord {
    const compiled = compileFilterExpression(expr, { version });
    const chain = this.chains.get(topic);
    if (chain !== undefined) {
      const prev = chain[chain.length - 1];
      // Replace the previous head with a copy carrying the superseded
      // link; the old record object itself is never mutated, so any
      // previously handed-out reference keeps its original shape.
      chain[chain.length - 1] = { ...prev, supersededBy: version };
    }
    const record: FilterVersionRecord = {
      version,
      source: expr,
      compiled,
      createdAt: now,
      supersededBy: undefined,
      rolledBackFrom,
    };
    if (chain === undefined) {
      this.chains.set(topic, [record]);
    } else {
      chain.push(record);
    }
    return record;
  }
}
