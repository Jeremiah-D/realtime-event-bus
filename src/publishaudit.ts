/**
 * Publish audit journal (EB-138): an opt-in append-only JSONL journal of
 * every publish admission decision on the bus — admitted publishes and
 * admission rejections (the EB-39 `AdmissionRejectReason` vocabulary).
 * The journal answers "what did this bus admit, when, from whom" without
 * re-reading the message log: each line carries the topic, the per-topic
 * sequence number (admitted only), the publisher identity, the
 * end-to-end trace id, the message identity and key — never the payload
 * body, so large or sensitive payloads stay out of the audit trail.
 *
 * Two layers, mirroring the subscription audit log (EB-78):
 * - an always-on bounded in-memory ring (default 10000 entries, oldest
 *   evicted) that `EventBus.getPublishAudit()` queries;
 * - disk persistence under the bus's `durableLogDir`: one live
 *   `__publish_audit.jsonl` file, rotated (renamed, never deleted — the
 *   EB-61 archive convention) to `__publish_audit.<YYYYMMDD>.<n>.jsonl`
 *   on UTC-day rollover or when the live file would exceed its byte
 *   budget. On construction the in-memory ring is back-filled from the
 *   on-disk files (archived first, live last), skipping corrupt lines.
 *
 * All timestamps come from the bus clock (`now`), so tests stay
 * deterministic. Disk writes are synchronous and error-isolated: a
 * failed append or rotation never throws into the publish path — it is
 * counted in `writeErrors()` instead (best-effort journaling).
 */
import { appendFileSync, mkdirSync, readdirSync, readFileSync, renameSync } from 'node:fs';
import { join } from 'node:path';
import { Buffer } from 'node:buffer';
import {
  AUDIT_CHAIN_GENESIS,
  attachChainLink,
  effectiveRecordHash,
  isHashString,
  verifyChainLinks,
  type ChainVerification,
} from './auditchain.ts';

/** Live journal file name inside the log directory. */
export const PUBLISH_AUDIT_LIVE_NAME = '__publish_audit.jsonl';

/** Default bound of the in-memory read view (oldest entries evicted first). */
export const PUBLISH_AUDIT_DEFAULT_MAX_ENTRIES = 10_000;

/** Default live-file byte budget before rotation (64 MiB). */
export const PUBLISH_AUDIT_DEFAULT_MAX_BYTES = 64 * 1024 * 1024;

/**
 * One publish admission record. `outcome` decides which of `seq` /
 * `reason` is present:
 * - `admitted`: the publish fanned out; `seq` is its per-topic sequence
 *   number.
 * - `rejected`: an admission gate refused it; `reason` names the gate in
 *   the EB-39 vocabulary (`'acl' | 'schema' | 'message-too-big' |
 *   'payload-too-large' | 'rate-limit' | 'publisher-rate-limit' |
 *   'duplicate' | 'alias-retired' | 'topic-paused' | 'circuit-breaker' |
 *   'sampled'`).
 */
export interface PublishAuditEntry {
  v: 1;
  outcome: 'admitted' | 'rejected';
  /** The concrete topic the publish targeted (never a pattern). */
  topic: string;
  /** Per-topic sequence number; present only when admitted. */
  seq?: number;
  /** The refusing gate (EB-39 vocabulary); present only when rejected. */
  reason?: string;
  /** Publisher identity, when the publish carried one. */
  publisherId?: string;
  /** End-to-end trace id (parsed from the publish's traceparent). */
  traceId?: string;
  /** Application message identity, when the publish carried one. */
  messageId?: string;
  /** Message key, when the publish carried one. */
  key?: string;
  /** Bus-clock milliseconds of the admission decision. */
  admittedAtMs: number;
  /**
   * Tamper-evident hash chain (EB-198): `prevHash` is the previous
   * record's `hash` (the fixed genesis for the first record), `hash`
   * covers the whole record. Both are always present on records written
   * by `record()`; records recovered from pre-chain journal files carry
   * neither and verify as chain anchors.
   */
  prevHash?: string;
  hash?: string;
}

/** Query filters for `PublishAuditLog.query`. */
export interface PublishAuditQuery {
  /** Exact topic match. */
  topic?: string;
  /** Exact publisher identity match. */
  publisherId?: string;
  /** Inclusive lower bound on `admittedAtMs`. */
  since?: number;
  /** Exclusive upper bound on `admittedAtMs`. */
  until?: number;
  /** Return at most this many of the most recent matching entries. */
  limit?: number;
}

/** Options for `PublishAuditLog`. */
export interface PublishAuditOptions {
  /** Bounded in-memory read view; oldest entries evicted first. */
  maxEntries?: number;
  /** Live-file byte budget; the file rotates past it. */
  maxBytes?: number;
  /** Clock source; defaults to `Date.now`. */
  now?: () => number;
}

/** The two numeric bounds of the publish-audit tuning, in resolved form. */
export interface PublishAuditTuning {
  /** Bounded in-memory read view; oldest entries evicted first. */
  maxEntries: number;
  /** Live-file byte budget; the file rotates past it. */
  maxBytes: number;
}

/**
 * A partial bus-level revision of the publish-audit tuning, for
 * `EventBus.setPublishAudit` (EB-309). Only the fields provided move;
 * the rest keep their live values. The journal directory and the clock
 * are not revisable here — they are fixed at construction.
 */
export interface PublishAuditRevision {
  /**
   * Bounded in-memory read view. Must be a positive integer when
   * provided — the construction-time rule.
   */
  maxEntries?: number;
  /**
   * Live-file byte budget. Must be a positive finite number of bytes
   * when provided — the construction-time rule.
   */
  maxBytes?: number;
}

function utcDay(at: number): string {
  const d = new Date(at);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function archivedName(day: string, n: number): string {
  return `__publish_audit.${day}.${n}.jsonl`;
}

function isArchivedAuditFile(name: string): boolean {
  return (
    name.startsWith('__publish_audit.') &&
    name.endsWith('.jsonl') &&
    /^__publish_audit\.\d{8}\.\d+\.jsonl$/.test(name)
  );
}

/** Validates a publish-audit query; throws `RangeError` on illegal values. */
export function assertValidPublishAuditQuery(query: PublishAuditQuery): void {
  const { topic, publisherId, since, until, limit } = query;
  if (topic !== undefined && (typeof topic !== 'string' || topic.length === 0)) {
    throw new RangeError('getPublishAudit: topic must be a non-empty string');
  }
  if (publisherId !== undefined && (typeof publisherId !== 'string' || publisherId.length === 0)) {
    throw new RangeError('getPublishAudit: publisherId must be a non-empty string');
  }
  if (since !== undefined && (!Number.isFinite(since) || since < 0)) {
    throw new RangeError('getPublishAudit: since must be a finite non-negative timestamp');
  }
  if (until !== undefined && (!Number.isFinite(until) || until < 0)) {
    throw new RangeError('getPublishAudit: until must be a finite non-negative timestamp');
  }
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) {
    throw new RangeError('getPublishAudit: limit must be a positive integer');
  }
}

/**
 * Parses one JSONL journal line; returns `undefined` for corrupt lines
 * (bad JSON, wrong shape) so recovery skips them instead of dying.
 */
export function parsePublishAuditLine(line: string): PublishAuditEntry | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null) return undefined;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return undefined;
  if (o['outcome'] !== 'admitted' && o['outcome'] !== 'rejected') return undefined;
  if (typeof o['topic'] !== 'string' || (o['topic'] as string).length === 0) return undefined;
  if (typeof o['admittedAtMs'] !== 'number' || !Number.isFinite(o['admittedAtMs'])) return undefined;
  const entry: PublishAuditEntry = {
    v: 1,
    outcome: o['outcome'],
    topic: o['topic'] as string,
    admittedAtMs: o['admittedAtMs'] as number,
  };
  if (o['outcome'] === 'admitted') {
    if (!Number.isInteger(o['seq']) || (o['seq'] as number) < 1) return undefined;
    entry.seq = o['seq'] as number;
    if (o['reason'] !== undefined) return undefined;
  } else {
    if (typeof o['reason'] !== 'string' || (o['reason'] as string).length === 0) return undefined;
    entry.reason = o['reason'] as string;
    if (o['seq'] !== undefined) return undefined;
  }
  for (const key of ['publisherId', 'traceId', 'messageId', 'key'] as const) {
    const value = o[key];
    if (value !== undefined) {
      if (typeof value !== 'string') return undefined;
      entry[key] = value;
    }
  }
  // Tamper-evident chain fields (EB-198): optional for backward
  // compatibility with pre-chain journal files; when present they must
  // be well-formed digests, otherwise the line is corrupt.
  for (const key of ['prevHash', 'hash'] as const) {
    const value = o[key];
    if (value !== undefined) {
      if (!isHashString(value)) return undefined;
      entry[key] = value;
    }
  }
  return entry;
}

export class PublishAuditLog {
  private readonly dir: string;
  // Live-tuned by `retune` (EB-309): the record and rotation sites
  // read these fields on every call, so a revision governs from the
  // next record without draining or rebuilding the journal. `dir` and
  // `now` stay fixed for the log's lifetime.
  private maxEntries: number;
  private maxBytes: number;
  private readonly now: () => number;
  private readonly entries: PublishAuditEntry[] = [];
  private diskWriteErrors = 0;
  private corruptLines = 0;
  /** UTC day of the live file's newest line; null when the live file is empty/absent. */
  private liveDay: string | null = null;
  /** Byte size of the live file. */
  private liveBytes = 0;
  /**
   * Tamper-evident chain head (EB-198): the effective hash of the most
   * recently recorded or recovered entry — the `prevHash` of the next
   * record. Starts at the genesis; recovery advances it past the
   * on-disk history so the chain continues across restarts.
   */
  private headHash: string = AUDIT_CHAIN_GENESIS;

  constructor(dir: string, options: PublishAuditOptions = {}) {
    if (typeof dir !== 'string' || dir.length === 0) {
      throw new RangeError('publishAuditDir must be a non-empty string');
    }
    const { maxEntries = PUBLISH_AUDIT_DEFAULT_MAX_ENTRIES, maxBytes = PUBLISH_AUDIT_DEFAULT_MAX_BYTES, now = Date.now } = options;
    if (!Number.isInteger(maxEntries) || maxEntries < 1) {
      throw new RangeError('publishAuditMaxEntries must be a positive integer');
    }
    if (!Number.isFinite(maxBytes) || maxBytes < 1) {
      throw new RangeError('publishAuditMaxBytes must be a positive finite number of bytes');
    }
    if (typeof now !== 'function') {
      throw new RangeError('publishAudit now must be a function');
    }
    this.dir = dir;
    this.maxEntries = maxEntries;
    this.maxBytes = maxBytes;
    this.now = now;
    this.recover();
  }

  /** The two live tuning values, for bus-level revision (EB-309). */
  current(): PublishAuditTuning {
    return { maxEntries: this.maxEntries, maxBytes: this.maxBytes };
  }

  /**
   * Retunes the two numeric bounds live (EB-309). The caller passes
   * the merged values (current values with the revision applied); this
   * method re-validates them with the construction-time rule
   * (`maxEntries` a positive integer, `maxBytes` a positive finite
   * number of bytes — note the constructor accepts any positive finite
   * byte budget, not only integers, and so does this) and applies them
   * only when valid, so a failed call leaves every live value
   * untouched. In-flight entries are retained, never cleared or
   * rewritten: shrinking `maxEntries` does not evict immediately —
   * the next `record()` evicts down to the live bound — and a revised
   * `maxBytes` is honored by the next `record()`'s rotation check, so
   * a tightened budget may rotate the live file at the next record
   * while a loosened one simply defers the next rotation.
   */
  retune(next: PublishAuditTuning): void {
    if (!Number.isInteger(next.maxEntries) || next.maxEntries < 1) {
      throw new RangeError('publishAuditMaxEntries must be a positive integer');
    }
    if (!Number.isFinite(next.maxBytes) || next.maxBytes < 1) {
      throw new RangeError('publishAuditMaxBytes must be a positive finite number of bytes');
    }
    this.maxEntries = next.maxEntries;
    this.maxBytes = next.maxBytes;
  }

  /**
   * Records one admission decision. The timestamp is stamped here on the
   * bus clock, so callers pass everything except `v`/`admittedAtMs`.
   * The record is tamper-evident chained (EB-198): `prevHash`/`hash` are
   * attached before the in-memory append and the disk write, so both
   * layers carry the same links. Never throws: disk failures (append or
   * rotation) are counted in `writeErrors()` and the in-memory entry
   * always lands — the publish path is never blocked by the journal.
   */
  record(entry: Omit<PublishAuditEntry, 'v' | 'admittedAtMs'> & { admittedAtMs?: number }): void {
    const full: PublishAuditEntry = {
      v: 1,
      outcome: entry.outcome,
      topic: entry.topic,
      admittedAtMs: entry.admittedAtMs ?? this.now(),
      ...(entry.seq === undefined ? {} : { seq: entry.seq }),
      ...(entry.reason === undefined ? {} : { reason: entry.reason }),
      ...(entry.publisherId === undefined ? {} : { publisherId: entry.publisherId }),
      ...(entry.traceId === undefined ? {} : { traceId: entry.traceId }),
      ...(entry.messageId === undefined ? {} : { messageId: entry.messageId }),
      ...(entry.key === undefined ? {} : { key: entry.key }),
    };
    const linked = attachChainLink(this.headHash, full);
    this.headHash = linked.hash;
    this.entries.push(linked);
    if (this.entries.length > this.maxEntries) {
      this.entries.splice(0, this.entries.length - this.maxEntries);
    }
    try {
      const line = JSON.stringify(full) + '\n';
      const lineBytes = Buffer.byteLength(line, 'utf8');
      this.rotateIfNeeded(full.admittedAtMs, lineBytes);
      appendFileSync(join(this.dir, PUBLISH_AUDIT_LIVE_NAME), line, 'utf8');
      this.liveDay = utcDay(full.admittedAtMs);
      this.liveBytes += lineBytes;
    } catch {
      this.diskWriteErrors += 1;
    }
  }

  /**
   * Queries the in-memory read view: filters by topic, publisher, and
   * time window, then returns at most `limit` of the most recent matches
   * (oldest first). Throws `RangeError` on illegal query values.
   */
  query(query: PublishAuditQuery = {}): PublishAuditEntry[] {
    assertValidPublishAuditQuery(query);
    const { topic, publisherId, since, until, limit } = query;
    const matched: PublishAuditEntry[] = [];
    for (const e of this.entries) {
      if (topic !== undefined && e.topic !== topic) continue;
      if (publisherId !== undefined && e.publisherId !== publisherId) continue;
      if (since !== undefined && e.admittedAtMs < since) continue;
      if (until !== undefined && e.admittedAtMs >= until) continue;
      matched.push(e);
    }
    if (limit !== undefined && matched.length > limit) {
      return matched.slice(matched.length - limit);
    }
    return matched;
  }

  /** In-memory read-view size. */
  size(): number {
    return this.entries.length;
  }

  /** Disk append/rotation failures since construction. */
  writeErrors(): number {
    return this.diskWriteErrors;
  }

  /** Corrupt JSONL lines skipped during recovery. */
  corruptLineCount(): number {
    return this.corruptLines;
  }

  /**
   * Rotates the live file when the incoming line belongs to a new UTC
   * day or would push the live file past its byte budget. Rotation is a
   * rename to `__publish_audit.<day>.<n>.jsonl` (EB-61 archive
   * convention: archived, never deleted); a failed rename is counted
   * and the line still lands in the live file — data preservation beats
   * rotation hygiene.
   */
  private rotateIfNeeded(admittedAtMs: number, lineBytes: number): void {
    const day = utcDay(admittedAtMs);
    if (this.liveDay === day && this.liveBytes + lineBytes <= this.maxBytes) return;
    // Nothing to rotate: no live file yet (or it is empty).
    if (this.liveBytes === 0) {
      this.liveDay = day;
      return;
    }
    try {
      const archivedDay = this.liveDay ?? day;
      const name = archivedName(archivedDay, this.nextArchiveIndex(archivedDay));
      renameSync(join(this.dir, PUBLISH_AUDIT_LIVE_NAME), join(this.dir, name));
      this.liveBytes = 0;
      this.liveDay = day;
    } catch {
      this.diskWriteErrors += 1;
    }
  }

  /** Next archive sequence number for `day` (max existing + 1). */
  private nextArchiveIndex(day: string): number {
    let max = -1;
    let files: string[];
    try {
      files = readdirSync(this.dir);
    } catch {
      return 0;
    }
    const prefix = `__publish_audit.${day}.`;
    for (const f of files) {
      if (!f.startsWith(prefix) || !f.endsWith('.jsonl')) continue;
      const n = parseInt(f.slice(prefix.length, -'.jsonl'.length), 10);
      if (Number.isInteger(n) && n > max) max = n;
    }
    return max + 1;
  }

  /**
   * Back-fills the in-memory read view from the on-disk journal
   * (archived files first, live file last), skipping corrupt lines.
   * Failures here are silent by design: a damaged journal directory
   * must never prevent the bus from starting.
   */
  private recover(): void {
    let files: string[];
    try {
      mkdirSync(this.dir, { recursive: true });
      files = readdirSync(this.dir);
    } catch {
      return;
    }
    const archived = files.filter(isArchivedAuditFile).sort();
    const ordered = files.includes(PUBLISH_AUDIT_LIVE_NAME) ? [...archived, PUBLISH_AUDIT_LIVE_NAME] : archived;
    const recovered: PublishAuditEntry[] = [];
    for (const file of ordered) {
      let text: string;
      try {
        text = readFileSync(join(this.dir, file), 'utf8');
      } catch {
        continue;
      }
      if (file === PUBLISH_AUDIT_LIVE_NAME) {
        this.liveBytes = Buffer.byteLength(text, 'utf8');
      }
      for (const line of text.split('\n')) {
        const trimmed = line.trim();
        if (trimmed === '') continue;
        const entry = parsePublishAuditLine(trimmed);
        if (entry === undefined) {
          this.corruptLines += 1;
          continue;
        }
        recovered.push(entry);
      }
    }
    if (recovered.length > this.maxEntries) {
      recovered.splice(0, recovered.length - this.maxEntries);
    }
    this.entries.push(...recovered);
    if (recovered.length > 0) {
      this.liveDay = utcDay(recovered[recovered.length - 1].admittedAtMs);
      // The chain continues across restarts: the next recorded entry
      // links to the last recovered one (see `headHash`).
      this.headHash = effectiveRecordHash(recovered[recovered.length - 1]);
    }
  }

  /**
   * Verifies the tamper-evident hash chain (EB-198) over the in-memory
   * read view: detects rewritten fields (`content-tampered`), deleted or
   * spliced records (`link-broken`), and reports the first break. The
   * oldest retained entry is the segment anchor — its link to evicted
   * predecessors cannot be checked (see `src/auditchain.ts`).
   */
  verifyAuditChain(): ChainVerification {
    return verifyChainLinks(this.entries);
  }
}
