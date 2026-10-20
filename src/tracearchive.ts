/**
 * Trace span archive journal (EB-167): opt-in disk persistence for the
 * sampled delivery-pipeline trace spans (EB-45).
 *
 * The `TraceRecorder` (see `src/trace.ts`) only keeps spans in a bounded
 * in-memory ring (`getStats().traceSpans`) plus the `onTraceSpan`
 * callback — a process restart loses the whole trace history. This
 * module adds the durable layer: every sampled span is appended to an
 * append-only JSONL journal under the bus's `durableLogDir`, so
 * post-incident analysis can still reconstruct "which publish fanned out
 * where, how long each stage took" after a restart.
 *
 * Two layers, mirroring the publish audit journal (EB-138):
 * - an always-on bounded in-memory ring (default 10000 spans, oldest
 *   evicted) that `EventBus.getTraceArchive()` queries;
 * - disk persistence: one live `__trace_spans.jsonl` file, rotated
 *   (renamed, never deleted — the EB-61 archive convention) to
 *   `__trace_spans.<YYYYMMDD>.<n>.jsonl` on UTC-day rollover or when
 *   the live file would exceed its byte budget. On construction the
 *   in-memory ring is back-filled from the on-disk files (archived
 *   first, live last), skipping corrupt lines.
 *
 * Decoupled from the publish audit journal by design: the publish audit
 * (EB-138) records admission *decisions* (admitted/rejected, per
 * publish), while this journal records pipeline *spans* (publish,
 * admission, fanout, enqueue, deliver, ack — several per publish). The
 * two answer different questions and query by different keys
 * (`topic`/`publisherId` vs `traceId`/time).
 *
 * All timestamps come from the spans themselves (the bus clock at span
 * creation), so tests stay deterministic. Disk writes are synchronous
 * and error-isolated: a failed append or rotation never throws into the
 * span pipeline — it is counted in `writeErrors()` instead
 * (best-effort journaling).
 */
import { appendFileSync, mkdirSync, readdirSync, readFileSync, renameSync } from 'node:fs';
import { join } from 'node:path';
import { Buffer } from 'node:buffer';
import type { TraceSpan } from './trace.ts';

/** Live journal file name inside the log directory. */
export const TRACE_ARCHIVE_LIVE_NAME = '__trace_spans.jsonl';

/** Default bound of the in-memory read view (oldest spans evicted first). */
export const TRACE_ARCHIVE_DEFAULT_MAX_ENTRIES = 10_000;

/** Default live-file byte budget before rotation (64 MiB). */
export const TRACE_ARCHIVE_DEFAULT_MAX_BYTES = 64 * 1024 * 1024;

/**
 * One archived span: the `TraceSpan` wire shape plus the journal
 * version. `attrs` values are the span's own (`string | number |
 * boolean`); anything else on a journal line marks it corrupt.
 */
export interface TraceArchiveEntry {
  v: 1;
  traceId: string;
  spanId: string;
  parentId?: string;
  name: string;
  at: number;
  durationMs: number;
  attrs: Record<string, string | number | boolean>;
}

/** Query filters for `TraceSpanJournal.query`. */
export interface TraceArchiveQuery {
  /** Exact trace id match (32 lowercase hex). */
  traceId?: string;
  /** Inclusive lower bound on the span start timestamp. */
  since?: number;
  /** Exclusive upper bound on the span start timestamp. */
  until?: number;
  /** Return at most this many of the most recent matching spans. */
  limit?: number;
}

/** Options for `TraceSpanJournal`. */
export interface TraceArchiveOptions {
  /** Bounded in-memory read view; oldest spans evicted first. */
  maxEntries?: number;
  /** Live-file byte budget; the file rotates past it. */
  maxBytes?: number;
  /** Clock source; defaults to `Date.now`. */
  now?: () => number;
}

function utcDay(at: number): string {
  const d = new Date(at);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function archivedName(day: string, n: number): string {
  return `__trace_spans.${day}.${n}.jsonl`;
}

function isArchivedSpanFile(name: string): boolean {
  // NOTE (EB-167): the day segment carries dashes (`2026-10-20`), so the
  // pattern matches the dashed form — unlike the EB-138 publish-audit
  // regex, whose `\d{8}` never matches the dashed names that module
  // actually writes (archived files are silently skipped on recovery
  // there; logged as a future backlog candidate, not fixed here).
  return (
    name.startsWith('__trace_spans.') &&
    name.endsWith('.jsonl') &&
    /^__trace_spans\.\d{4}-\d{2}-\d{2}\.\d+\.jsonl$/.test(name)
  );
}

const HEX32 = /^[0-9a-f]{32}$/;
const HEX16 = /^[0-9a-f]{16}$/;

/** Validates a trace-archive query; throws `RangeError` on illegal values. */
export function assertValidTraceArchiveQuery(query: TraceArchiveQuery): void {
  const { traceId, since, until, limit } = query;
  if (traceId !== undefined && (typeof traceId !== 'string' || !HEX32.test(traceId))) {
    throw new RangeError('getTraceArchive: traceId must be a 32-hex-char trace id');
  }
  if (since !== undefined && (!Number.isFinite(since) || since < 0)) {
    throw new RangeError('getTraceArchive: since must be a finite non-negative timestamp');
  }
  if (until !== undefined && (!Number.isFinite(until) || until < 0)) {
    throw new RangeError('getTraceArchive: until must be a finite non-negative timestamp');
  }
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) {
    throw new RangeError('getTraceArchive: limit must be a positive integer');
  }
}

/**
 * Validates `TraceArchiveOptions` the way the bus constructor would:
 * throws `RangeError` on the first problem — a broken archive config
 * fails at construction time, never mid-publish.
 */
export function assertValidTraceArchiveOptions(
  options: TraceArchiveOptions,
  where: string,
): void {
  if (typeof options !== 'object' || options === null || Array.isArray(options)) {
    throw new RangeError(`${where}: trace.archive must be true or a TraceArchiveOptions object`);
  }
  const { maxEntries, maxBytes, now } = options;
  if (maxEntries !== undefined && (!Number.isInteger(maxEntries) || maxEntries < 1)) {
    throw new RangeError(`${where}: trace.archive.maxEntries must be a positive integer`);
  }
  if (maxBytes !== undefined && (!Number.isFinite(maxBytes) || maxBytes < 1)) {
    throw new RangeError(`${where}: trace.archive.maxBytes must be a positive finite number of bytes`);
  }
  if (now !== undefined && typeof now !== 'function') {
    throw new RangeError(`${where}: trace.archive.now must be a function`);
  }
}

/**
 * Parses one JSONL journal line; returns `undefined` for corrupt lines
 * (bad JSON, wrong shape) so recovery skips them instead of dying.
 */
export function parseTraceSpanLine(line: string): TraceArchiveEntry | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null) return undefined;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return undefined;
  if (typeof o['traceId'] !== 'string' || !HEX32.test(o['traceId'])) return undefined;
  if (typeof o['spanId'] !== 'string' || !HEX16.test(o['spanId'])) return undefined;
  if (o['parentId'] !== undefined && typeof o['parentId'] !== 'string') return undefined;
  if (typeof o['name'] !== 'string' || (o['name'] as string).length === 0) return undefined;
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return undefined;
  if (typeof o['durationMs'] !== 'number' || !Number.isFinite(o['durationMs']) || (o['durationMs'] as number) < 0) {
    return undefined;
  }
  const attrs = o['attrs'];
  if (typeof attrs !== 'object' || attrs === null || Array.isArray(attrs)) return undefined;
  for (const value of Object.values(attrs as Record<string, unknown>)) {
    const t = typeof value;
    if (t !== 'string' && t !== 'number' && t !== 'boolean') return undefined;
  }
  return o as TraceArchiveEntry;
}

export class TraceSpanJournal {
  private readonly dir: string;
  private readonly maxEntries: number;
  private readonly maxBytes: number;
  private readonly now: () => number;
  private readonly entries: TraceArchiveEntry[] = [];
  private diskWriteErrors = 0;
  private corruptLines = 0;
  /** UTC day of the live file's newest line; null when the live file is empty/absent. */
  private liveDay: string | null = null;
  /** Byte size of the live file. */
  private liveBytes = 0;

  constructor(dir: string, options: TraceArchiveOptions = {}) {
    if (typeof dir !== 'string' || dir.length === 0) {
      throw new RangeError('traceArchiveDir must be a non-empty string');
    }
    assertValidTraceArchiveOptions(options, 'EventBus');
    const {
      maxEntries = TRACE_ARCHIVE_DEFAULT_MAX_ENTRIES,
      maxBytes = TRACE_ARCHIVE_DEFAULT_MAX_BYTES,
      now = Date.now,
    } = options;
    this.dir = dir;
    this.maxEntries = maxEntries;
    this.maxBytes = maxBytes;
    this.now = now;
    this.recover();
  }

  /**
   * Journals one sampled span. The span already carries its timestamps
   * (the bus clock at creation), so nothing is stamped here. Never
   * throws: disk failures (append or rotation) are counted in
   * `writeErrors()` and the in-memory entry always lands — the span
   * pipeline is never blocked by the journal.
   */
  record(span: TraceSpan): void {
    const full: TraceArchiveEntry = {
      v: 1,
      traceId: span.traceId,
      spanId: span.spanId,
      ...(span.parentId === undefined ? {} : { parentId: span.parentId }),
      name: span.name,
      at: span.at,
      durationMs: span.durationMs,
      attrs: { ...span.attrs },
    };
    this.entries.push(full);
    if (this.entries.length > this.maxEntries) {
      this.entries.splice(0, this.entries.length - this.maxEntries);
    }
    try {
      const line = JSON.stringify(full) + '\n';
      const lineBytes = Buffer.byteLength(line, 'utf8');
      this.rotateIfNeeded(full.at, lineBytes);
      appendFileSync(join(this.dir, TRACE_ARCHIVE_LIVE_NAME), line, 'utf8');
      this.liveDay = utcDay(full.at);
      this.liveBytes += lineBytes;
    } catch {
      this.diskWriteErrors += 1;
    }
  }

  /**
   * Queries the in-memory read view: filters by exact trace id and time
   * window, then returns at most `limit` of the most recent matches
   * (oldest first). Throws `RangeError` on illegal query values.
   */
  query(query: TraceArchiveQuery = {}): TraceArchiveEntry[] {
    assertValidTraceArchiveQuery(query);
    const { traceId, since, until, limit } = query;
    const matched: TraceArchiveEntry[] = [];
    for (const e of this.entries) {
      if (traceId !== undefined && e.traceId !== traceId) continue;
      if (since !== undefined && e.at < since) continue;
      if (until !== undefined && e.at >= until) continue;
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
   * Rotates the live file when the incoming span belongs to a new UTC
   * day or would push the live file past its byte budget. Rotation is a
   * rename to `__trace_spans.<day>.<n>.jsonl` (EB-61 archive
   * convention: archived, never deleted); a failed rename is counted
   * and the line still lands in the live file — data preservation beats
   * rotation hygiene.
   */
  private rotateIfNeeded(spanAt: number, lineBytes: number): void {
    const day = utcDay(spanAt);
    if (this.liveDay === day && this.liveBytes + lineBytes <= this.maxBytes) return;
    // Nothing to rotate: no live file yet (or it is empty).
    if (this.liveBytes === 0) {
      this.liveDay = day;
      return;
    }
    try {
      const archivedDay = this.liveDay ?? day;
      const name = archivedName(archivedDay, this.nextArchiveIndex(archivedDay));
      renameSync(join(this.dir, TRACE_ARCHIVE_LIVE_NAME), join(this.dir, name));
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
    const prefix = `__trace_spans.${day}.`;
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
    const archived = files.filter(isArchivedSpanFile).sort();
    const ordered = files.includes(TRACE_ARCHIVE_LIVE_NAME) ? [...archived, TRACE_ARCHIVE_LIVE_NAME] : archived;
    const recovered: TraceArchiveEntry[] = [];
    for (const file of ordered) {
      let text: string;
      try {
        text = readFileSync(join(this.dir, file), 'utf8');
      } catch {
        continue;
      }
      if (file === TRACE_ARCHIVE_LIVE_NAME) {
        this.liveBytes = Buffer.byteLength(text, 'utf8');
      }
      for (const line of text.split('\n')) {
        const trimmed = line.trim();
        if (trimmed === '') continue;
        const entry = parseTraceSpanLine(trimmed);
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
      this.liveDay = utcDay(recovered[recovered.length - 1].at);
    }
  }
}
