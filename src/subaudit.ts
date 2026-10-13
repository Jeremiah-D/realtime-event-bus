/**
 * Subscription lifecycle audit log (EB-78): an append-only JSONL journal of
 * every subscription change on the bus — subscribe / unsubscribe / pattern
 * hot-updates (EB-74) / consumer-group join-leave / group rebalances / topic
 * registry changes (EB-77) / backpressure drops (EB-83,
 * `backpressure_dropped`) / signature-verification failures (EB-84,
 * `signature_verification_failed`).
 *
 * This complements `EventBusOptions.onAuthzDenied`, which only fires for
 * *rejected* authorization decisions: the audit log records the normal,
 * successful subscription lifecycle that the denial hook deliberately
 * ignores. Operators answering "who subscribed to what, and when" query
 * here instead of reconstructing it from delivery metrics.
 *
 * Two layers:
 * - an always-on bounded in-memory ring (default 1000 entries, oldest
 *   evicted) that `EventBus.getSubscriptionAudit()` queries — zero
 *   configuration, zero disk I/O;
 * - opt-in disk persistence via `dir`: one `audit-YYYY-MM-DD.jsonl` file
 *   per UTC day (daily rotation is implicit — each event lands in the file
 *   named by its own timestamp), so a restarted bus keeps its audit
 *   trail. On construction the in-memory ring is back-filled from the
 *   newest on-disk lines, skipping corrupt lines.
 *
 * All timestamps come from the bus clock (`now`), so tests stay
 * deterministic. Disk writes are synchronous and error-isolated: a failed
 * append never throws into the subscribe path — it is counted in
 * `writeErrors()` instead.
 */
import { appendFileSync, mkdirSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

/** Every lifecycle action the audit log records. */
export type SubscriptionAuditAction =
  | 'subscribe'
  | 'unsubscribe'
  | 'update_pattern'
  | 'group_join'
  | 'group_leave'
  | 'rebalance'
  | 'topic_register'
  | 'topic_deregister'
  | 'backpressure_dropped'
  | 'signature_verification_failed';

/**
 * One audit entry. `detail` carries action-specific context without
 * bloating the flat schema:
 * - `update_pattern`: `{ from: <old pattern> }`
 * - `rebalance`: `{ trigger: 'join'|'leave', memberId, members: <count> }`
 * - `topic_register`: `{ owner?, description?, schemaVersion?, tags?, updated: <bool> }`
 * - `topic_deregister`: `{ owner?, description? }` (last-known metadata)
 * - `backpressure_dropped`: `{ policy: <backpressure tier>, dropped: <count>, queueSize: <number> }`
 * - `signature_verification_failed`: `{ seq: <per-topic seq>, kid?: <key id that signed, when present> }`
 */
export interface SubscriptionAuditEvent {
  v: 1;
  /** Milliseconds on the bus clock. */
  at: number;
  action: SubscriptionAuditAction;
  subId?: string;
  groupId?: string;
  pattern?: string;
  topic?: string;
  /** Who made the change; defaults to `'system'` when the caller supplies none. */
  actor: string;
  detail?: Record<string, unknown>;
}

/** Query filters for `SubscriptionAuditLog.query`. */
export interface SubscriptionAuditQuery {
  /** Inclusive lower bound on `at`. */
  since?: number;
  /** Exclusive upper bound on `at`. */
  until?: number;
  /** One action or a set of actions. */
  action?: SubscriptionAuditAction | SubscriptionAuditAction[];
  /** Return at most this many of the most recent matching entries. */
  limit?: number;
}

/** Options for `SubscriptionAuditLog`. */
export interface SubscriptionAuditOptions {
  /** Opt-in disk directory for the daily JSONL files. */
  dir?: string;
  /** Bounded in-memory index size; oldest entries evicted first. */
  maxEntries?: number;
  /** Clock source; defaults to `Date.now`. */
  now?: () => number;
}

export const SUBSCRIPTION_AUDIT_DEFAULT_MAX_ENTRIES = 1000;

const ALL_ACTIONS: ReadonlySet<string> = new Set<string>([
  'subscribe',
  'unsubscribe',
  'update_pattern',
  'group_join',
  'group_leave',
  'rebalance',
  'topic_register',
  'topic_deregister',
  'backpressure_dropped',
  'signature_verification_failed',
]);

function isAuditAction(value: unknown): value is SubscriptionAuditAction {
  return typeof value === 'string' && ALL_ACTIONS.has(value);
}

function utcDay(at: number): string {
  const d = new Date(at);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

/** Validates an audit query; throws `RangeError` on illegal values. */
export function assertValidSubscriptionAuditQuery(query: SubscriptionAuditQuery): void {
  const { since, until, action, limit } = query;
  if (since !== undefined && (!Number.isFinite(since) || since < 0)) {
    throw new RangeError('getSubscriptionAudit: since must be a finite non-negative timestamp');
  }
  if (until !== undefined && (!Number.isFinite(until) || until < 0)) {
    throw new RangeError('getSubscriptionAudit: until must be a finite non-negative timestamp');
  }
  if (action !== undefined) {
    const actions = Array.isArray(action) ? action : [action];
    for (const a of actions) {
      if (!isAuditAction(a)) {
        throw new RangeError(`getSubscriptionAudit: unknown action ${JSON.stringify(a)}`);
      }
    }
  }
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) {
    throw new RangeError('getSubscriptionAudit: limit must be a positive integer');
  }
}

/** Validates an actor name; throws `RangeError` on illegal values. */
export function assertValidActor(actor: unknown, where: string): asserts actor is string {
  if (typeof actor !== 'string' || actor.length === 0) {
    throw new RangeError(`${where}: actor must be a non-empty string`);
  }
}

export class SubscriptionAuditLog {
  private readonly dir: string | undefined;
  private readonly maxEntries: number;
  private readonly now: () => number;
  private readonly entries: SubscriptionAuditEvent[] = [];
  private diskWriteErrors = 0;

  constructor(options: SubscriptionAuditOptions = {}) {
    const { dir, maxEntries = SUBSCRIPTION_AUDIT_DEFAULT_MAX_ENTRIES, now = Date.now } = options;
    if (dir !== undefined && (typeof dir !== 'string' || dir.length === 0)) {
      throw new RangeError('subscriptionAuditDir must be a non-empty string');
    }
    if (!Number.isInteger(maxEntries) || maxEntries < 1) {
      throw new RangeError('subscriptionAuditMaxEntries must be a positive integer');
    }
    if (typeof now !== 'function') {
      throw new RangeError('subscriptionAudit now must be a function');
    }
    this.dir = dir;
    this.maxEntries = maxEntries;
    this.now = now;
    if (dir !== undefined) {
      this.recover(dir);
    }
  }

  /**
   * Appends one event. The timestamp is stamped here on the bus clock, so
   * callers pass everything except `v`/`at`. Never throws: disk failures
   * are counted in `writeErrors()` and the in-memory entry always lands.
   */
  append(event: Omit<SubscriptionAuditEvent, 'v' | 'at'> & { at?: number }): void {
    const full: SubscriptionAuditEvent = {
      v: 1,
      at: event.at ?? this.now(),
      action: event.action,
      actor: event.actor,
      ...(event.subId === undefined ? {} : { subId: event.subId }),
      ...(event.groupId === undefined ? {} : { groupId: event.groupId }),
      ...(event.pattern === undefined ? {} : { pattern: event.pattern }),
      ...(event.topic === undefined ? {} : { topic: event.topic }),
      ...(event.detail === undefined ? {} : { detail: event.detail }),
    };
    this.entries.push(full);
    if (this.entries.length > this.maxEntries) {
      this.entries.splice(0, this.entries.length - this.maxEntries);
    }
    if (this.dir !== undefined) {
      this.appendToDisk(full);
    }
  }

  /**
   * Queries the in-memory index: filters by time window and action, then
   * returns at most `limit` of the most recent matches (oldest first).
   * Throws `RangeError` on illegal query values.
   */
  query(query: SubscriptionAuditQuery = {}): SubscriptionAuditEvent[] {
    assertValidSubscriptionAuditQuery(query);
    const { since, until, action, limit } = query;
    const actions =
      action === undefined ? undefined : new Set(Array.isArray(action) ? action : [action]);
    const matched: SubscriptionAuditEvent[] = [];
    for (const e of this.entries) {
      if (since !== undefined && e.at < since) continue;
      if (until !== undefined && e.at >= until) continue;
      if (actions !== undefined && !actions.has(e.action)) continue;
      matched.push(e);
    }
    if (limit !== undefined && matched.length > limit) {
      return matched.slice(matched.length - limit);
    }
    return matched;
  }

  /** In-memory index size. */
  size(): number {
    return this.entries.length;
  }

  /** Disk append failures since construction (0 when no `dir` configured). */
  writeErrors(): number {
    return this.diskWriteErrors;
  }

  private fileFor(at: number): string {
    return join(this.dir as string, `audit-${utcDay(at)}.jsonl`);
  }

  private appendToDisk(event: SubscriptionAuditEvent): void {
    try {
      appendFileSync(this.fileFor(event.at), JSON.stringify(event) + '\n', 'utf8');
    } catch {
      this.diskWriteErrors += 1;
    }
  }

  /**
   * Back-fills the in-memory ring from existing daily files (oldest file
   * first, newest lines last), skipping corrupt lines. Failures here are
   * silent by design: a damaged audit directory must never prevent the
   * bus from starting.
   */
  private recover(dir: string): void {
    let files: string[];
    try {
      mkdirSync(dir, { recursive: true });
      files = readdirSync(dir)
        .filter((f) => f.startsWith('audit-') && f.endsWith('.jsonl'))
        .sort();
    } catch {
      return;
    }
    const recovered: SubscriptionAuditEvent[] = [];
    for (const file of files) {
      let text: string;
      try {
        text = readFileSync(join(dir, file), 'utf8');
      } catch {
        continue;
      }
      for (const line of text.split('\n')) {
        const trimmed = line.trim();
        if (trimmed === '') continue;
        const event = parseAuditLine(trimmed);
        if (event !== undefined) recovered.push(event);
      }
    }
    if (recovered.length > this.maxEntries) {
      recovered.splice(0, recovered.length - this.maxEntries);
    }
    this.entries.push(...recovered);
  }
}

/**
 * Parses one JSONL audit line; returns `undefined` for corrupt lines
 * (bad JSON, wrong shape) so recovery skips them instead of dying.
 */
export function parseAuditLine(line: string): SubscriptionAuditEvent | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null) return undefined;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return undefined;
  if (!isAuditAction(o['action'])) return undefined;
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return undefined;
  if (typeof o['actor'] !== 'string' || o['actor'].length === 0) return undefined;
  const event: SubscriptionAuditEvent = {
    v: 1,
    at: o['at'],
    action: o['action'],
    actor: o['actor'],
  };
  for (const key of ['subId', 'groupId', 'pattern', 'topic'] as const) {
    const value = o[key];
    if (value !== undefined) {
      if (typeof value !== 'string') return undefined;
      event[key] = value;
    }
  }
  if (o['detail'] !== undefined) {
    if (typeof o['detail'] !== 'object' || o['detail'] === null) return undefined;
    event.detail = o['detail'] as Record<string, unknown>;
  }
  return event;
}

