/**
 * Topic lifecycle management (EB-126): the provisioning side of topics.
 *
 * EB-77's `TopicRegistry` is an advisory operator catalog — publishing to
 * an unregistered topic is never blocked. This registry is the opposite:
 * it tracks which concrete topics have been *provisioned* (their
 * retention/TTL/compression/signing policies installed in one call) and
 * which have been *deleted*. A deletion leaves a tombstone, and the
 * tombstone is a real admission gate: `fanOut` refuses publishes to a
 * deleted topic (reusing the `'alias-retired'` rejection reason — the
 * vocabulary's only "this name is retired from service" item), while the
 * EB-77 catalog stays advisory and untouched by either call.
 *
 * Names here are concrete, never patterns: lifecycle operates on the
 * namespace-resolved global topic name, the same string the publish path
 * admits on. Anything containing `*` is rejected with `RangeError`.
 */
import type { TopicCompressionOptions } from './bus.ts';
import type { TopicSigningKeysConfig } from './signing.ts';

/** Validates a lifecycle topic name; throws `RangeError` on illegal values. */
export function assertValidLifecycleTopicName(name: unknown, where: string): asserts name is string {
  if (typeof name !== 'string' || name.length === 0) {
    throw new RangeError(`${where}: topic must be a non-empty string`);
  }
  if (name.includes('*')) {
    throw new RangeError(`${where}: topic must be a concrete topic name, not a pattern ('*' is not allowed)`);
  }
}

/** Advisory ownership metadata recorded with a provisioning. */
export interface TopicProvisioningMetadata {
  owner?: string;
  description?: string;
  tags?: string[];
}

/**
 * Retention half of a provisioning bundle. Retention is a bus-global
 * facility in this codebase (EB-85 `setDurableLogMaxAgeMs`, EB-119
 * `durableLogMaxBytesPerTopic`) — there is no per-topic retention rule
 * API to apply. `createTopic` therefore maps `retention.maxAgeMs` onto
 * the shared durable-log age bound (exactly `setDurableLogMaxAgeMs`,
 * same validation), and the option is documented as global, not
 * per-topic. Pass it only when you mean to move the bound for every
 * topic on the bus.
 */
export interface TopicRetentionOptions {
  /** Positive finite milliseconds; `undefined` leaves the bound untouched. */
  maxAgeMs?: number;
}

/**
 * The one-call policy bundle for `createTopic`. Every policy is applied
 * through the existing per-topic configuration API it names, keyed by
 * the exact topic name:
 * - `ttlMs` → `setTopicTtl`
 * - `compression` → `setTopicCompression`
 * - `signing` → `setTopicSigningKey`
 * - `retention.maxAgeMs` → `setDurableLogMaxAgeMs` (bus-global; see above)
 *
 * All options are validated before the first policy is applied, so a bad
 * bundle throws `RangeError` with zero side effects — the application is
 * atomic. Re-provisioning an already-provisioned topic re-applies every
 * supplied policy (absent options leave the current rule in place; use
 * the `clearTopic*` APIs to remove a rule outright).
 */
export interface TopicProvisioningOptions {
  /** Advisory metadata for the lifecycle record. */
  metadata?: TopicProvisioningMetadata;
  /** Message TTL in milliseconds (`setTopicTtl`). */
  ttlMs?: number;
  /** Durable-log retention (bus-global — see `TopicRetentionOptions`). */
  retention?: TopicRetentionOptions;
  /** Payload compression (`setTopicCompression`). */
  compression?: TopicCompressionOptions;
  /** Publish-side HMAC signing (`setTopicSigningKey`). */
  signing?: TopicSigningKeysConfig;
  /** Audit actor; default `'system'`. */
  actor?: string;
}

/** Options for `deleteTopic`. */
export interface DeleteTopicOptions {
  /**
   * When true, the topic's durable history is destroyed: the live log
   * file, archived segments (+ checksum sidecars), its archive-index and
   * segment-chain entries, the compression dictionary registered for the
   * topic, and every in-memory sequence counter. When false (default) the
   * history is kept and stays replayable (`replay`, `resumeFromTime`).
   */
  purge?: boolean;
  /** Audit actor; default `'system'`. */
  actor?: string;
}

/** The outcome of `createTopic`. */
export interface CreateTopicResult {
  /** The concrete topic name, as passed in. */
  topic: string;
  /**
   * `true` when the call established a live provisioning — a first-time
   * provision or a re-provision after deletion. `false` when it updated
   * the policies of an already-provisioned topic (upsert, never an
   * error).
   */
  created: boolean;
}

/** The outcome of `deleteTopic`. */
export interface DeleteTopicResult {
  /** The concrete topic name, as passed in. */
  topic: string;
  /**
   * `true` when the call tore anything down — a live provisioning, durable
   * state, matching wildcard subscriptions, or exact-name policy rules.
   * `false` only when the name was completely unknown to the bus (no-op).
   * A soft-deleted topic deleted again with `purge: true` reports `true`
   * when retained history was actually destroyed.
   */
  deleted: boolean;
  /** `true` when durable data was purged by this call. */
  purged: boolean;
  /**
   * Wildcard subscriptions invalidated because their compiled matcher
   * still hit the topic (each removed through the normal unsubscribe
   * path and audited with `topic_deleted`).
   */
  invalidatedSubscriptions: number;
}

/** A provisioned topic's lifecycle record, tombstones included. */
export interface TopicLifecycleInfo {
  /** Concrete topic name. */
  topic: string;
  /** `false` for tombstones left by `deleteTopic`. */
  provisioned: boolean;
  /** Advisory metadata from the provisioning call. */
  metadata?: TopicProvisioningMetadata;
  /** Bus-clock timestamp of the first provisioning. */
  provisionedAt: number;
  /** Bus-clock timestamp of the last provisioning update. */
  updatedAt: number;
  /** Bus-clock timestamp of the deletion, when `provisioned` is false. */
  deletedAt?: number;
  /** Whether the deletion purged durable data, when deleted. */
  purged?: boolean;
}

/** Validates provisioning metadata; throws `RangeError` on illegal values. */
export function assertValidProvisioningMetadata(
  metadata: TopicProvisioningMetadata | undefined,
  where: string,
): void {
  if (metadata === undefined) return;
  if (typeof metadata !== 'object' || metadata === null) {
    throw new RangeError(`${where}: metadata must be an object`);
  }
  if (metadata.owner !== undefined && (typeof metadata.owner !== 'string' || metadata.owner.length === 0)) {
    throw new RangeError(`${where}: metadata.owner must be a non-empty string`);
  }
  if (
    metadata.description !== undefined &&
    (typeof metadata.description !== 'string' || metadata.description.length === 0)
  ) {
    throw new RangeError(`${where}: metadata.description must be a non-empty string`);
  }
  if (metadata.tags !== undefined) {
    if (!Array.isArray(metadata.tags)) {
      throw new RangeError(`${where}: metadata.tags must be an array of strings`);
    }
    for (const tag of metadata.tags) {
      if (typeof tag !== 'string' || tag.length === 0) {
        throw new RangeError(`${where}: metadata.tags must be non-empty strings`);
      }
    }
  }
}

/**
 * The lifecycle registry itself. Timestamps are stamped by the caller's
 * clock so the bus can inject its own (`EventBusOptions.now`) for
 * deterministic tests. Tombstones are kept forever: "never existed" and
 * "deleted" stay distinguishable, and the admission gate consults them.
 */
export class TopicLifecycleRegistry {
  private readonly topics = new Map<string, TopicLifecycleInfo>();

  /**
   * Provisions a topic, or re-applies the metadata of a live provisioning.
   * A tombstoned topic re-provisions fresh (the tombstone is replaced —
   * the topic accepts publishes again). Returns `created: true` when the
   * topic was not provisioned before this call.
   */
  provision(
    topic: string,
    metadata: TopicProvisioningMetadata | undefined,
    nowMs: number,
  ): { created: boolean } {
    const existing = this.topics.get(topic);
    const snapshot =
      metadata === undefined
        ? undefined
        : {
            ...(metadata.owner === undefined ? {} : { owner: metadata.owner }),
            ...(metadata.description === undefined ? {} : { description: metadata.description }),
            ...(metadata.tags === undefined ? {} : { tags: [...metadata.tags] }),
          };
    if (existing !== undefined && existing.provisioned) {
      if (snapshot !== undefined) existing.metadata = snapshot;
      existing.updatedAt = nowMs;
      return { created: false };
    }
    this.topics.set(topic, {
      topic,
      provisioned: true,
      ...(snapshot === undefined ? {} : { metadata: snapshot }),
      provisionedAt: existing?.provisionedAt ?? nowMs,
      updatedAt: nowMs,
    });
    return { created: true };
  }

  /**
   * Deletes a topic's provisioning, leaving a tombstone (`provisioned:
   * false`). Returns `true` when a live provisioning was removed, `false`
   * when the topic was never provisioned or already deleted.
   */
  remove(topic: string, nowMs: number, purged: boolean): boolean {
    const existing = this.topics.get(topic);
    if (existing !== undefined && existing.provisioned) {
      existing.provisioned = false;
      existing.deletedAt = nowMs;
      existing.purged = purged;
      return true;
    }
    return false;
  }

  /**
   * Records a tombstone for a topic that was never provisioned (deleted
   * for its durable data / subscriptions / rules alone). A no-op when a
   * tombstone already exists.
   */
  tombstone(topic: string, nowMs: number, purged: boolean): void {
    const existing = this.topics.get(topic);
    if (existing !== undefined) return;
    this.topics.set(topic, {
      topic,
      provisioned: false,
      provisionedAt: nowMs,
      updatedAt: nowMs,
      deletedAt: nowMs,
      purged,
    });
  }

  /** `true` when the topic holds a live provisioning. */
  isProvisioned(topic: string): boolean {
    return this.topics.get(topic)?.provisioned === true;
  }

  /** `true` when the topic carries a deletion tombstone (the publish gate). */
  isDeleted(topic: string): boolean {
    const info = this.topics.get(topic);
    return info !== undefined && !info.provisioned;
  }

  /** Full record for a topic, tombstones included; `undefined` when unknown. */
  get(topic: string): TopicLifecycleInfo | undefined {
    const info = this.topics.get(topic);
    return info === undefined
      ? undefined
      : { ...info, metadata: info.metadata === undefined ? undefined : { ...info.metadata, tags: info.metadata.tags === undefined ? undefined : [...info.metadata.tags] } };
  }
}
