/**
 * Multi-tenant topic namespace isolation (EB-52).
 *
 * A namespace is a tenant scope over the topic space: topics published
 * inside namespace `t1` are stored on the bus as `t1/<topic>` — the `/`
 * separator is matched literally by the wildcard engine (segments split
 * on `.` only), so a namespace forms a structural subtree that patterns
 * from another namespace cannot cross.
 *
 * Isolation is between tenants: the global bus (no namespace) is the
 * admin view and still sees every topic, including namespaced ones.
 *
 * This module holds the namespace surface that does not need the bus's
 * internals: options, validation, errors, stats shapes, and the thin
 * `NamespaceHandle`. The admission plumbing (topic/pattern translation,
 * per-namespace durable logs, stats aggregation) lives in `bus.ts` and
 * imports from here; the handle refers to `EventBus` by type only, so
 * there is no runtime import cycle.
 */
import type {
  EventBus,
  IdempotentPublishOptions,
  IdempotentPublishResult,
  MessageHandler,
  PublishOptions,
  SubscribeOptions,
  Subscription,
} from './bus.ts';

/** Options for `EventBus.createNamespace`. */
export interface NamespaceOptions {
  /**
   * Whether publishes through this namespace are admitted. Defaults to
   * `true`; `false` turns the namespace into a read-only (subscribe-only)
   * scope — publishes are rejected with `NamespaceDeniedError`.
   */
  allowPublish?: boolean;
  /**
   * Whether subscriptions through this namespace are admitted. Defaults
   * to `true`; `false` turns the namespace into a write-only
   * (publish-only) scope — subscribes are rejected with
   * `NamespaceDeniedError`.
   */
  allowSubscribe?: boolean;
}

/**
 * Thrown when a publish or subscribe is refused by namespace isolation:
 * the namespace disallows the action (`allowPublish`/`allowSubscribe`),
 * or the topic/pattern tried to escape the namespace (it contains the
 * `/` separator, e.g. `t2/x` published with `namespace: 't1'`). Follows
 * the `AclDeniedError` shape: the action, the offending subject, and the
 * scope that refused it.
 */
export class NamespaceDeniedError extends Error {
  /** The denied action. */
  readonly action: 'publish' | 'subscribe';
  /** The denied publish topic (absent for subscribes). */
  readonly topic?: string;
  /** The denied subscription pattern (absent for publishes). */
  readonly pattern?: string;
  /** The namespace that refused the operation. */
  readonly namespace: string;

  constructor(
    action: 'publish' | 'subscribe',
    namespace: string,
    subject?: { topic?: string; pattern?: string },
  ) {
    const what =
      subject?.topic !== undefined
        ? `topic "${subject.topic}"`
        : `pattern "${subject?.pattern}"`;
    super(`namespace "${namespace}" denied ${action} on ${what}`);
    this.name = 'NamespaceDeniedError';
    this.action = action;
    this.namespace = namespace;
    this.topic = subject?.topic;
    this.pattern = subject?.pattern;
  }
}

/**
 * Thrown by `EventBus.deleteNamespace` when the namespace still owns
 * live state: active subscriptions, or retained durable-log entries.
 * Drain the subscribers (and let the durable entries age out or clear
 * the log) before deleting — a namespace is never deleted out from
 * under live consumers.
 */
export class NamespaceNotEmptyError extends Error {
  /** The namespace that refused deletion. */
  readonly namespace: string;
  /** What still holds the namespace open. */
  readonly reason: 'subscribers' | 'durable-entries';

  constructor(namespace: string, reason: 'subscribers' | 'durable-entries') {
    super(
      `namespace "${namespace}" is not empty: ${reason === 'subscribers' ? 'it still has live subscribers' : 'it still has retained durable-log entries'}`,
    );
    this.name = 'NamespaceNotEmptyError';
    this.namespace = namespace;
    this.reason = reason;
  }
}

/**
 * Validates a namespace prefix for `EventBus.createNamespace`. A prefix
 * is a single path segment: non-empty, no `/` (the separator), no `*`
 * (never a wildcard), and not blank. Anything else throws `RangeError`
 * — the bus's usual configuration-error signal.
 */
export function validateNamespacePrefix(prefix: string): void {
  if (typeof prefix !== 'string' || prefix.length === 0) {
    throw new RangeError('namespace prefix must be a non-empty string');
  }
  if (prefix.trim().length === 0) {
    throw new RangeError('namespace prefix must not be blank');
  }
  if (prefix.includes('/')) {
    throw new RangeError(`namespace prefix must not contain "/": "${prefix}"`);
  }
  if (prefix.includes('*')) {
    throw new RangeError(`namespace prefix must not contain "*": "${prefix}"`);
  }
}

/** Resolved namespace flags: both default to `true`. */
export interface ResolvedNamespaceOptions {
  allowPublish: boolean;
  allowSubscribe: boolean;
}

/**
 * Resolves `NamespaceOptions` to its effective flags. A non-object or a
 * non-boolean flag throws `RangeError`, matching the bus's options
 * validation style.
 */
export function resolveNamespaceOptions(opts?: NamespaceOptions): ResolvedNamespaceOptions {
  if (opts === undefined) return { allowPublish: true, allowSubscribe: true };
  if (typeof opts !== 'object' || opts === null) {
    throw new RangeError('namespace options must be an object');
  }
  const allowPublish = opts.allowPublish ?? true;
  const allowSubscribe = opts.allowSubscribe ?? true;
  if (typeof allowPublish !== 'boolean') {
    throw new RangeError('namespace allowPublish must be a boolean');
  }
  if (typeof allowSubscribe !== 'boolean') {
    throw new RangeError('namespace allowSubscribe must be a boolean');
  }
  return { allowPublish, allowSubscribe };
}

/**
 * Point-in-time namespace descriptor returned by
 * `EventBus.getNamespaces()`.
 */
export interface NamespaceInfo {
  /** The registered prefix. */
  prefix: string;
  /** Whether publishes through this namespace are admitted. */
  allowPublish: boolean;
  /** Whether subscriptions through this namespace are admitted. */
  allowSubscribe: boolean;
  /** Currently active subscriptions registered through this namespace. */
  subscribers: number;
}

/**
 * Per-namespace aggregate published by `EventBus.getStats()` (see
 * `BusStats.namespaces`), derived from the existing per-topic
 * `TopicStats`: every concrete topic of the form `<prefix>/<topic>`
 * belongs to the namespace.
 */
export interface NamespaceStats {
  /** The registered prefix. */
  namespace: string;
  /** Distinct concrete topics under `<prefix>/` seen so far. */
  topics: number;
  /** Currently active subscriptions registered through this namespace. */
  subscribers: number;
  /** Total messages published to `<prefix>/` topics since bus creation. */
  publishedMessages: number;
}

/**
 * Thin tenant handle returned by `EventBus.createNamespace`: `publish`
 * and `subscribe` inject the namespace, so tenant code never spells the
 * `prefix/` concrete topic itself. Topics and patterns passed through the
 * handle must not contain `/` — an attempted escape throws
 * `NamespaceDeniedError`.
 *
 * Deliveries (and stats) name the concrete `prefix/topic` form: the
 * handle is a scope, not a rename.
 */
export class NamespaceHandle {
  /** The namespace prefix this handle publishes/subscribes under. */
  readonly prefix: string;
  private readonly bus: EventBus;

  constructor(bus: EventBus, prefix: string) {
    this.bus = bus;
    this.prefix = prefix;
  }

  /**
   * Publishes `payload` to `<prefix>/<topic>` through the bus's full
   * admission pipeline (ACL, schema, rate limit, TTL, idempotency,
   * durable log) — the namespace only scopes the topic, it never
   * bypasses admission.
   */
  publish(topic: string, payload: unknown, opts?: PublishOptions): number {
    return this.bus.publish(topic, payload, { ...opts, namespace: this.prefix });
  }

  /**
   * Idempotent publish into the namespace: the `(topic, messageId)`
   * dedup identity is scoped to the concrete `<prefix>/<topic>`, so the
   * same `messageId` in two namespaces never collides.
   */
  publishIdempotent(
    topic: string,
    payload: unknown,
    opts?: IdempotentPublishOptions,
  ): IdempotentPublishResult {
    return this.bus.publishIdempotent(topic, payload, { ...opts, namespace: this.prefix });
  }

  /**
   * Subscribes to `<prefix>/<pattern>`: the pattern is matched against
   * the sub-topic after the prefix, so `**` receives everything in this
   * namespace and structurally cannot reach another namespace's topics.
   * Accepts the full `SubscribeOptions`, including
   * `resumeFromSeq`/`resumeFromTime` — replays read the namespace's own
   * durable log.
   */
  subscribe(pattern: string, handler: MessageHandler, opts?: SubscribeOptions): Subscription {
    return this.bus.subscribe(pattern, handler, { ...opts, namespace: this.prefix });
  }
}
