/**
 * Declarative bus configuration snapshots (EB-91).
 *
 * Operators need configuration as data: capture everything that shapes how
 * the bus admits, routes and delivers traffic, diff it, apply a desired
 * state, and notice when the runtime drifted away from the last applied
 * state. This module holds the snapshot schema and the pure
 * diff/validate/compare logic; the `EventBus` methods
 * (`exportBusConfig` / `applyConfig` / `detectConfigDrift` in `bus.ts`)
 * do the actual reading and writing because they need the bus's private
 * rule tables.
 *
 * What a snapshot covers (exactly the operator surface named by EB-91):
 * - global: ACL rules, namespace registrations, per-publisher rate
 *   limits, durable-log retention (`durableLogMaxAgeMs`);
 * - per topic pattern: TTL, retained-message rule, publish rate limit,
 *   max subscribers, allowed lateness, compression rule, signing-key
 *   *metadata* (key ids + grace window — never key material), topic
 *   alias, topic route, partition count.
 *
 * Deliberately NOT covered: schema validators and route predicates are
 * functions and cannot be serialized — a snapshot round-trips data, never
 * code. Per-subscriber runtime state (queues, health, offsets) is
 * operational state, not configuration.
 *
 * Security invariant: `exportBusConfig` never includes key material. The
 * signing-key section carries only `{ keyIds, graceMs }`, and
 * `applyConfig` refuses to *change* signing keys from a snapshot
 * (fail-closed `ConfigApplyError`): keys are restored with
 * `setTopicSigningKey`, which takes the actual secrets. The same
 * fail-closed rule applies to compression dictionaries (referenced by id
 * only) and routes carrying a predicate.
 */

import type { AclRule } from './bus.ts';

/** Snapshot format version. Bumped only for breaking schema changes. */
export const BUS_CONFIG_SNAPSHOT_VERSION = 1;

/** Per-topic publish rate-limit rule, as stored by `setTopicRateLimit`. */
export interface TopicRateLimitConfig {
  messagesPerSec: number;
  burst: number;
}

/** Per-topic compression rule metadata (see `setTopicCompression`). */
export interface TopicCompressionConfig {
  thresholdBytes: number;
  level: number;
  /**
   * SHA-256 id of the preset deflate dictionary, when the rule uses one.
   * The dictionary *bytes* are never exported — only the id, so drift
   * detection can tell a dictionary change apart from a threshold change.
   */
  dictionaryId?: string;
}

/**
 * Signing-key *metadata* for a topic pattern. The secret bytes are never
 * part of a snapshot — this is an inventory list, not a backup.
 */
export interface TopicSigningKeyConfig {
  keyIds: string[];
  graceMs: number;
}

/** Topic alias rule, as stored by `setTopicAlias`. */
export interface TopicAliasConfig {
  newTopic: string;
  /** Absolute bus-clock expiry, when the alias was registered with a TTL. */
  expiresAt?: number;
}

/** Topic route rule (predicate-bearing routes are never exported). */
export interface TopicRouteConfig {
  dst: string;
}

/**
 * The declarative rule set for one topic pattern. Every field is
 * optional — absent means "no rule of this kind".
 */
export interface TopicRuleConfig {
  ttlMs?: number;
  retained?: boolean;
  rateLimit?: TopicRateLimitConfig;
  maxSubscribers?: number;
  allowedLatenessMs?: number;
  compression?: TopicCompressionConfig;
  signingKeys?: TopicSigningKeyConfig;
  alias?: TopicAliasConfig;
  route?: TopicRouteConfig;
  partitions?: number;
}

/** Namespace registration, as stored by `createNamespace`. */
export interface NamespaceConfig {
  allowPublish: boolean;
  allowSubscribe: boolean;
}

/**
 * A declarative snapshot of the bus's configuration. JSON-safe by
 * construction (validated by `assertValidSnapshot`), so it can be
 * written to disk, checked into version control, or shipped to another
 * process.
 */
export interface BusConfigSnapshot {
  v: typeof BUS_CONFIG_SNAPSHOT_VERSION;
  /** Bus-clock millisecond timestamp of the export. */
  exportedAt: number;
  acl: AclRule[];
  namespaces: Record<string, NamespaceConfig>;
  publisherRateLimits: Record<string, TopicRateLimitConfig>;
  /** Absent when no retention policy is configured. */
  durableLogMaxAgeMs?: number;
  topicRules: Record<string, TopicRuleConfig>;
}

/** Which configuration section a change belongs to. */
export type ConfigChangeKind =
  | 'acl'
  | 'namespace'
  | 'publisherRateLimit'
  | 'retention'
  | 'topicRule';

/**
 * One atomic configuration change between two snapshots.
 * `before`/`after` are `undefined` for removals/additions respectively.
 * `path` is a stable dotted address, e.g. `acl`,
 * `namespaces["t1"]`, `publisherRateLimits["svc-a"]`,
 * `durableLogMaxAgeMs`, `topicRules["market.**"].ttlMs`.
 */
export interface ConfigChange {
  path: string;
  kind: ConfigChangeKind;
  before: unknown;
  after: unknown;
}

/** One drifted setting: what the baseline says vs. what the bus has now. */
export interface ConfigDrift {
  path: string;
  expected: unknown;
  actual: unknown;
}

/** Successful outcome of `applyConfig`. */
export interface ConfigApplyResult {
  /** Paths applied, in application order. Empty on a no-op. */
  applied: string[];
  /** True for a dry run — nothing was mutated. */
  dryRun: boolean;
  /** The computed change list; present only on a dry run. */
  changes?: ConfigChange[];
}

/**
 * Thrown by `applyConfig` when a change fails partway. The bus first
 * rolls every already-applied change back to the pre-apply state, so the
 * bus is left exactly as it was before the call — never half-configured.
 */
export class ConfigApplyError extends Error {
  /** Paths successfully applied before the failure (all rolled back). */
  readonly applied: string[];
  /** The change path whose application failed. */
  readonly failedPath: string;
  /** Whether the rollback restored the pre-apply state. */
  readonly rolledBack: boolean;
  constructor(
    failedPath: string,
    cause: unknown,
    applied: string[],
    rolledBack: boolean,
  ) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    super(
      `applyConfig failed at ${failedPath}: ${reason} ` +
        `(rolled back ${applied.length} applied change(s): ${rolledBack ? 'ok' : 'FAILED'})`,
    );
    this.name = 'ConfigApplyError';
    this.applied = applied;
    this.failedPath = failedPath;
    this.rolledBack = rolledBack;
  }
}

/**
 * Recursively sorts object keys so `JSON.stringify` comparisons are
 * order-insensitive. Arrays keep their order (ACL rule order is
 * semantically significant).
 */
function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = canonicalize((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}

/** Order-insensitive deep equality for snapshot fragments. */
export function configValueEqual(a: unknown, b: unknown): boolean {
  return JSON.stringify(canonicalize(a)) === JSON.stringify(canonicalize(b));
}

/** Deep-clones a snapshot (snapshots are JSON-safe by construction). */
export function cloneSnapshot(snapshot: BusConfigSnapshot): BusConfigSnapshot {
  return JSON.parse(JSON.stringify(snapshot)) as BusConfigSnapshot;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function assertFiniteNumber(value: unknown, where: string): void {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new TypeError(`invalid bus config snapshot: ${where} must be a finite number`);
  }
}

/**
 * Fail-fast shape validation for an inbound snapshot. Throws
 * `TypeError`/`RangeError` on anything malformed — `applyConfig`
 * validates before mutating a single rule, so a bad snapshot can never
 * leave the bus half-configured.
 */
export function assertValidSnapshot(snapshot: unknown): asserts snapshot is BusConfigSnapshot {
  if (!isRecord(snapshot)) {
    throw new TypeError('invalid bus config snapshot: must be an object');
  }
  if (snapshot.v !== BUS_CONFIG_SNAPSHOT_VERSION) {
    throw new RangeError(
      `unsupported bus config snapshot version: ${String(snapshot.v)} (expected ${BUS_CONFIG_SNAPSHOT_VERSION})`,
    );
  }
  assertFiniteNumber(snapshot.exportedAt, 'exportedAt');
  if (!Array.isArray(snapshot.acl)) {
    throw new TypeError('invalid bus config snapshot: acl must be an array');
  }
  for (const section of ['namespaces', 'publisherRateLimits', 'topicRules'] as const) {
    if (!isRecord(snapshot[section])) {
      throw new TypeError(`invalid bus config snapshot: ${section} must be an object`);
    }
  }
  if (
    snapshot.durableLogMaxAgeMs !== undefined &&
    (typeof snapshot.durableLogMaxAgeMs !== 'number' ||
      !Number.isFinite(snapshot.durableLogMaxAgeMs) ||
      snapshot.durableLogMaxAgeMs <= 0)
  ) {
    throw new TypeError('invalid bus config snapshot: durableLogMaxAgeMs must be a positive finite number');
  }
  const rateLimits = snapshot.publisherRateLimits as Record<string, unknown>;
  for (const [id, rule] of Object.entries(rateLimits)) {
    assertRateLimit(rule, `publisherRateLimits[${JSON.stringify(id)}]`);
  }
  const topicRules = snapshot.topicRules as Record<string, unknown>;
  for (const [pattern, rule] of Object.entries(topicRules)) {
    assertTopicRule(rule, `topicRules[${JSON.stringify(pattern)}]`);
  }
  const namespaces = snapshot.namespaces as Record<string, unknown>;
  for (const [prefix, ns] of Object.entries(namespaces)) {
    if (!isRecord(ns) || typeof ns.allowPublish !== 'boolean' || typeof ns.allowSubscribe !== 'boolean') {
      throw new TypeError(
        `invalid bus config snapshot: namespaces[${JSON.stringify(prefix)}] must have boolean allowPublish/allowSubscribe`,
      );
    }
  }
}

function assertRateLimit(rule: unknown, where: string): void {
  if (!isRecord(rule)) throw new TypeError(`invalid bus config snapshot: ${where} must be an object`);
  assertFiniteNumber(rule.messagesPerSec, `${where}.messagesPerSec`);
  assertFiniteNumber(rule.burst, `${where}.burst`);
  if ((rule.messagesPerSec as number) <= 0 || (rule.burst as number) <= 0) {
    throw new RangeError(`invalid bus config snapshot: ${where} budgets must be positive`);
  }
}

function assertTopicRule(rule: unknown, where: string): void {
  if (!isRecord(rule)) throw new TypeError(`invalid bus config snapshot: ${where} must be an object`);
  const r = rule as Record<string, unknown>;
  if (r.ttlMs !== undefined) assertFiniteNumber(r.ttlMs, `${where}.ttlMs`);
  if (r.retained !== undefined && typeof r.retained !== 'boolean') {
    throw new TypeError(`invalid bus config snapshot: ${where}.retained must be a boolean`);
  }
  if (r.rateLimit !== undefined) assertRateLimit(r.rateLimit, `${where}.rateLimit`);
  if (r.maxSubscribers !== undefined) assertFiniteNumber(r.maxSubscribers, `${where}.maxSubscribers`);
  if (r.allowedLatenessMs !== undefined) assertFiniteNumber(r.allowedLatenessMs, `${where}.allowedLatenessMs`);
  if (r.partitions !== undefined) assertFiniteNumber(r.partitions, `${where}.partitions`);
  if (r.compression !== undefined) {
    if (!isRecord(r.compression)) throw new TypeError(`invalid bus config snapshot: ${where}.compression must be an object`);
    assertFiniteNumber((r.compression as Record<string, unknown>).thresholdBytes, `${where}.compression.thresholdBytes`);
    const level = (r.compression as Record<string, unknown>).level;
    if (level !== undefined && (!Number.isInteger(level) || (level as number) < 0 || (level as number) > 9)) {
      throw new RangeError(`invalid bus config snapshot: ${where}.compression.level must be an integer in 0-9`);
    }
    const dictionaryId = (r.compression as Record<string, unknown>).dictionaryId;
    if (dictionaryId !== undefined && typeof dictionaryId !== 'string') {
      throw new TypeError(`invalid bus config snapshot: ${where}.compression.dictionaryId must be a string`);
    }
  }
  if (r.signingKeys !== undefined) {
    if (!isRecord(r.signingKeys)) throw new TypeError(`invalid bus config snapshot: ${where}.signingKeys must be an object`);
    const sk = r.signingKeys as Record<string, unknown>;
    if (!Array.isArray(sk.keyIds) || !sk.keyIds.every((k) => typeof k === 'string')) {
      throw new TypeError(`invalid bus config snapshot: ${where}.signingKeys.keyIds must be a string array`);
    }
    assertFiniteNumber(sk.graceMs, `${where}.signingKeys.graceMs`);
  }
  if (r.alias !== undefined) {
    if (!isRecord(r.alias) || typeof (r.alias as Record<string, unknown>).newTopic !== 'string') {
      throw new TypeError(`invalid bus config snapshot: ${where}.alias must have a string newTopic`);
    }
    const expiresAt = (r.alias as Record<string, unknown>).expiresAt;
    if (expiresAt !== undefined) assertFiniteNumber(expiresAt, `${where}.alias.expiresAt`);
  }
  if (r.route !== undefined) {
    if (!isRecord(r.route) || typeof (r.route as Record<string, unknown>).dst !== 'string') {
      throw new TypeError(`invalid bus config snapshot: ${where}.route must have a string dst`);
    }
  }
}

/**
 * Computes the granular change list that turns `before` into `after`.
 * Pure — no bus access. Changes are ordered: global sections first
 * (acl, namespaces, publisher rate limits, retention), then per-topic
 * rules in sorted pattern order with a stable per-rule key order, so
 * application order is deterministic.
 */
export function diffConfigSnapshots(
  before: BusConfigSnapshot,
  after: BusConfigSnapshot,
): ConfigChange[] {
  const changes: ConfigChange[] = [];
  if (!configValueEqual(before.acl, after.acl)) {
    changes.push({ path: 'acl', kind: 'acl', before: before.acl, after: after.acl });
  }
  diffRecords(before.namespaces, after.namespaces, 'namespaces', 'namespace', changes);
  diffRecords(
    before.publisherRateLimits,
    after.publisherRateLimits,
    'publisherRateLimits',
    'publisherRateLimit',
    changes,
  );
  if (!configValueEqual(before.durableLogMaxAgeMs, after.durableLogMaxAgeMs)) {
    changes.push({
      path: 'durableLogMaxAgeMs',
      kind: 'retention',
      before: before.durableLogMaxAgeMs,
      after: after.durableLogMaxAgeMs,
    });
  }
  const patterns = new Set([...Object.keys(before.topicRules), ...Object.keys(after.topicRules)]);
  const ruleKeyOrder: (keyof TopicRuleConfig)[] = [
    'ttlMs',
    'retained',
    'rateLimit',
    'maxSubscribers',
    'allowedLatenessMs',
    'compression',
    'signingKeys',
    'alias',
    'route',
    'partitions',
  ];
  for (const pattern of [...patterns].sort()) {
    const b = before.topicRules[pattern] as TopicRuleConfig | undefined;
    const a = after.topicRules[pattern] as TopicRuleConfig | undefined;
    for (const key of ruleKeyOrder) {
      const bv = b?.[key];
      const av = a?.[key];
      if (!configValueEqual(bv, av)) {
        changes.push({
          path: `topicRules[${JSON.stringify(pattern)}].${key}`,
          kind: 'topicRule',
          before: bv,
          after: av,
        });
      }
    }
  }
  return changes;
}

function diffRecords(
  before: Record<string, unknown>,
  after: Record<string, unknown>,
  section: string,
  kind: ConfigChangeKind,
  changes: ConfigChange[],
): void {
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const key of [...keys].sort()) {
    const bv = before[key];
    const av = after[key];
    if (!configValueEqual(bv, av)) {
      changes.push({ path: `${section}[${JSON.stringify(key)}]`, kind, before: bv, after: av });
    }
  }
}
