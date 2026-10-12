/**
 * Topic metadata registry (EB-77): the operator-side catalog of known
 * topics. Each registration names a concrete topic (exact string, not a
 * pattern) plus optional ownership metadata — who owns it, what it
 * carries, which schema version producers must honor, and free-form tags
 * for discovery.
 *
 * The registry is advisory, never a gate: publishing to an unregistered
 * topic is not blocked — it is counted (`getStats().unregisteredTopicPublishes`)
 * and surfaced on the opt-in `EventBusOptions.onUnregisteredTopic` hook,
 * so a typo'd topic name or a producer that forgot to register shows up
 * in monitoring instead of silently flowing. Registration changes are
 * recorded in the subscription lifecycle audit log (EB-78) as
 * `topic_register` / `topic_deregister`.
 *
 * Deregistration keeps the last-known metadata as a tombstone: history
 * is never rewritten, and `getTopicInfo` still answers for a retired
 * topic (marked `registered: false`) so operators can tell "never
 * existed" apart from "used to exist".
 */
export interface TopicRegistration {
  /** Concrete topic name; must be a non-empty string. */
  topic: string;
  /** Owning team or service, e.g. `'payments-svc'`. */
  owner?: string;
  /** Human description of what flows on this topic. */
  description?: string;
  /** Schema version producers must honor; a finite number >= 0. */
  schemaVersion?: number;
  /** Free-form tags for discovery, e.g. `['payments', 'pii']`. */
  tags?: string[];
}

/** A registered (or formerly registered) topic's full record. */
export interface TopicInfo {
  topic: string;
  owner?: string;
  description?: string;
  schemaVersion?: number;
  tags?: string[];
  /** `false` for tombstones kept after `deregisterTopic`. */
  registered: boolean;
  /** Bus-clock timestamp of the first registration. */
  registeredAt: number;
  /** Bus-clock timestamp of the last registration update. */
  updatedAt: number;
  /** Bus-clock timestamp of the deregistration, when `registered` is false. */
  deregisteredAt?: number;
}

/** Validates a topic registration; throws `RangeError` on illegal values. */
export function assertValidTopicRegistration(reg: TopicRegistration): void {
  if (typeof reg !== 'object' || reg === null) {
    throw new RangeError('registerTopic: registration must be an object');
  }
  if (typeof reg.topic !== 'string' || reg.topic.length === 0) {
    throw new RangeError('registerTopic: topic must be a non-empty string');
  }
  if (reg.owner !== undefined && (typeof reg.owner !== 'string' || reg.owner.length === 0)) {
    throw new RangeError('registerTopic: owner must be a non-empty string');
  }
  if (
    reg.description !== undefined &&
    (typeof reg.description !== 'string' || reg.description.length === 0)
  ) {
    throw new RangeError('registerTopic: description must be a non-empty string');
  }
  if (
    reg.schemaVersion !== undefined &&
    (!Number.isFinite(reg.schemaVersion) || reg.schemaVersion < 0)
  ) {
    throw new RangeError('registerTopic: schemaVersion must be a finite number >= 0');
  }
  if (reg.tags !== undefined) {
    if (!Array.isArray(reg.tags)) {
      throw new RangeError('registerTopic: tags must be an array of strings');
    }
    for (const tag of reg.tags) {
      if (typeof tag !== 'string' || tag.length === 0) {
        throw new RangeError('registerTopic: tags must be non-empty strings');
      }
    }
  }
}

/**
 * The registry itself. Timestamps are stamped by the caller's clock so
 * the bus can inject its own (`EventBusOptions.now`) for deterministic
 * tests.
 */
export class TopicRegistry {
  private readonly topics = new Map<string, TopicInfo>();

  /**
   * Registers a topic, or updates its metadata when already registered
   * (a deregistered topic re-registers fresh, keeping its original
   * `registeredAt`). Returns `true` when the topic was newly registered,
   * `false` when an existing registration was updated.
   */
  register(reg: TopicRegistration, nowMs: number): boolean {
    assertValidTopicRegistration(reg);
    const existing = this.topics.get(reg.topic);
    if (existing !== undefined && existing.registered) {
      existing.owner = reg.owner;
      existing.description = reg.description;
      existing.schemaVersion = reg.schemaVersion;
      existing.tags = reg.tags === undefined ? undefined : [...reg.tags];
      existing.updatedAt = nowMs;
      return false;
    }
    this.topics.set(reg.topic, {
      topic: reg.topic,
      owner: reg.owner,
      description: reg.description,
      schemaVersion: reg.schemaVersion,
      tags: reg.tags === undefined ? undefined : [...reg.tags],
      registered: true,
      registeredAt: existing?.registeredAt ?? nowMs,
      updatedAt: nowMs,
      // Keep the tombstone's deregistration moment on the fresh record:
      // the topic was retired once, and the history says when.
      ...(existing?.deregisteredAt === undefined ? {} : { deregisteredAt: existing.deregisteredAt }),
    });
    return true;
  }

  /**
   * Deregisters a topic, keeping its last-known metadata as a tombstone
   * (`registered: false`). Returns `true` when a live registration was
   * removed, `false` when the topic was never registered or already
   * deregistered.
   */
  deregister(topic: string, nowMs: number): boolean {
    if (typeof topic !== 'string' || topic.length === 0) {
      throw new RangeError('deregisterTopic: topic must be a non-empty string');
    }
    const existing = this.topics.get(topic);
    if (existing === undefined || !existing.registered) return false;
    existing.registered = false;
    existing.deregisteredAt = nowMs;
    return true;
  }

  /** `true` when the topic holds a live registration. */
  has(topic: string): boolean {
    return this.topics.get(topic)?.registered === true;
  }

  /** Full record for a topic, tombstones included; `undefined` when unknown. */
  get(topic: string): TopicInfo | undefined {
    const info = this.topics.get(topic);
    return info === undefined
      ? undefined
      : { ...info, tags: info.tags === undefined ? undefined : [...info.tags] };
  }

  /**
   * The operator catalog: every live registration in registration order.
   * With `includeDeregistered`, tombstones are appended after the live
   * entries, each marked `registered: false`.
   */
  catalog(includeDeregistered = false): TopicInfo[] {
    const live: TopicInfo[] = [];
    const dead: TopicInfo[] = [];
    for (const info of this.topics.values()) {
      const copy: TopicInfo = {
        ...info,
        tags: info.tags === undefined ? undefined : [...info.tags],
      };
      (info.registered ? live : dead).push(copy);
    }
    return includeDeregistered ? [...live, ...dead] : live;
  }

  /** Number of live registrations. */
  size(): number {
    let n = 0;
    for (const info of this.topics.values()) {
      if (info.registered) n += 1;
    }
    return n;
  }
}
