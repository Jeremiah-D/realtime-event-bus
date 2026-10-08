import { appendFileSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

/**
 * One persisted message in the durable topic log.
 */
export interface DurableLogRecord {
  /**
   * Per-topic sequence number assigned by the bus at publish time.
   * `0` is reserved for delayed-delivery schedule records (see `delayId`):
   * they are timer intents, not messages — they carry no sequence number,
   * are invisible to `readSince` replay (which only returns `seq >= 1`),
   * and never move the per-topic sequence counter.
   */
  seq: number;
  /** Concrete topic name, as published. */
  topic: string;
  /** Publish timestamp in milliseconds (the bus clock). */
  at: number;
  /** TTL expiry deadline in milliseconds, when a TTL rule matched at publish. */
  expiresAt?: number;
  /**
   * Delayed-delivery due time in milliseconds (bus clock), present on the
   * schedule record written by `publishDelayed` and copied onto the
   * delivery record when the message fans out at its due time.
   */
  deliverAt?: number;
  /**
   * Identifies one delayed delivery (`delayed-1`, …). Present on the seq-0
   * schedule record, on the delivery record written at fan-out, and on the
   * tombstone record written when the schedule is cancelled or dropped as
   * expired — so restart recovery can tell a still-pending schedule from a
   * fulfilled or closed one.
   */
  delayId?: string;
  /**
   * Tombstone for a delayed-delivery schedule: `true` on a seq-0 record
   * means the schedule with this `delayId` is closed (cancelled, expired
   * while delayed, or shed at fan-out) and must not be rebuilt on restart.
   */
  cancelled?: boolean;
  /** The published payload, JSON-serialized. */
  payload: unknown;
}

export interface DurableLogOptions {
  /**
   * Directory holding one append-only JSONL file per topic. Created when
   * missing.
   */
  dir: string;
  /**
   * Maximum retained entries per topic. When a topic's entry count grows
   * past twice this value the file is compacted down to the newest
   * `maxEntriesPerTopic` entries, so a hot topic cannot grow the log
   * without bound. Must be a positive integer. Default 10000.
   */
  maxEntriesPerTopic?: number;
}

/** On-disk envelope for one log line. `v` pins the format for future readers. */
interface LogLine {
  v: 1;
  seq: number;
  topic: string;
  at: number;
  expiresAt?: number;
  deliverAt?: number;
  delayId?: string;
  cancelled?: boolean;
  payload: unknown;
}

/**
 * Append-only, per-topic durable log for the event bus.
 *
 * Each concrete topic gets one JSONL file (`<url-encoded topic>.log`) under
 * the log directory; every published message is one line. Opening a log over
 * an existing directory recovers the per-topic sequence counters, so a new
 * `EventBus` pointed at the same directory continues numbering where the
 * previous process left off — no sequence reuse, no replay ambiguity.
 *
 * Delayed deliveries (`EventBus.publishDelayed`) additionally persist
 * seq-0 schedule records (`delayId` + `deliverAt`) and seq-0 tombstones
 * (`cancelled`) in the same files, so a restarted bus can rebuild its
 * pending timers. Schedule records are timer intents, not messages: they
 * never move the sequence counters and `readSince` never returns them.
 *
 * Durability note: appends are synchronous (`appendFileSync`), so a record
 * is handed to the OS before `append` returns. A power loss can still lose
 * whatever the OS had not flushed to disk — this is crash recovery for
 * process restarts, not a write-ahead log with `fsync` per message.
 *
 * Supported mode is one writer per directory per process. Two live logs over
 * the same directory interleave lines safely (O_APPEND) but their in-memory
 * counters diverge; do not do that.
 *
 * Payloads that `JSON.stringify` cannot represent (BigInt, circular
 * structures) are delivered live but skipped by the log — `append` returns
 * `false` for them instead of throwing into the publish path.
 */
export class DurableTopicLog {
  private readonly logDir: string;
  private readonly maxEntriesPerTopic: number;
  /** Topics known to the log (recovered from disk + appended since open). */
  private readonly topicsSeen = new Set<string>();
  /** Entry counts per topic, used for compaction and stats. */
  private readonly entryCounts = new Map<string, number>();
  /**
   * Entries with `seq >= 1` per topic — actual messages, excluding the seq-0
   * delayed-delivery schedule records and tombstones. Used to seed the
   * bus's per-topic `publishedMessages` on recovery: a scheduled-but-pending
   * message has not fanned out yet, so it must not count as published.
   */
  private readonly messageCounts = new Map<string, number>();
  /** Highest seq observed per topic (recovered + appended). */
  private readonly lastSeqs = new Map<string, number>();
  /** Lines that failed to parse during recovery/reads. Skipped, never fatal. */
  private corruptLines = 0;

  private constructor(dir: string, maxEntriesPerTopic: number) {
    this.logDir = dir;
    this.maxEntriesPerTopic = maxEntriesPerTopic;
    mkdirSync(dir, { recursive: true });
    this.recover();
  }

  /**
   * Opens (or creates) the durable log at `dir`. Throws `RangeError` for an
   * empty `dir` or a non-positive-integer `maxEntriesPerTopic`.
   */
  static open(options: DurableLogOptions): DurableTopicLog {
    const dir = options.dir;
    if (typeof dir !== 'string' || dir.length === 0) {
      throw new RangeError('durableLogDir must be a non-empty string');
    }
    const maxEntriesPerTopic = options.maxEntriesPerTopic ?? 10000;
    if (!Number.isInteger(maxEntriesPerTopic) || maxEntriesPerTopic < 1) {
      throw new RangeError('durableLogMaxEntriesPerTopic must be a positive integer');
    }
    return new DurableTopicLog(dir, maxEntriesPerTopic);
  }

  /** The log directory, as configured. */
  get dir(): string {
    return this.logDir;
  }

  /** File name for a topic: URL-encoded so any topic string is a safe name. */
  private fileFor(topic: string): string {
    return join(this.logDir, `${encodeURIComponent(topic)}.log`);
  }

  /**
   * Scans the directory and rebuilds the in-memory index: topics, entry
   * counts, and per-topic highest seq. Corrupt lines are counted and
   * skipped; a corrupt file never prevents the log from opening.
   */
  private recover(): void {
    let names: string[];
    try {
      names = readdirSync(this.logDir);
    } catch {
      return;
    }
    for (const name of names) {
      if (!name.endsWith('.log')) continue;
      let topic: string;
      try {
        topic = decodeURIComponent(name.slice(0, -'.log'.length));
      } catch {
        this.corruptLines += 1;
        continue;
      }
      const records = this.readFileRecords(topic);
      this.topicsSeen.add(topic);
      this.entryCounts.set(topic, records.length);
      let last = 0;
      let messages = 0;
      for (const rec of records) {
        if (rec.seq > last) last = rec.seq;
        // seq 0 schedule records and tombstones are log lines, not messages.
        if (rec.seq >= 1) messages += 1;
      }
      this.lastSeqs.set(topic, last);
      this.messageCounts.set(topic, messages);
    }
  }

  /** Reads and parses every well-formed record for a topic. */
  private readFileRecords(topic: string): DurableLogRecord[] {
    let text: string;
    try {
      text = readFileSync(this.fileFor(topic), 'utf8');
    } catch {
      return [];
    }
    const records: DurableLogRecord[] = [];
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      const rec = parseLogLine(line, topic);
      if (rec == null) {
        this.corruptLines += 1;
        continue;
      }
      records.push(rec);
    }
    return records;
  }

  /**
   * Appends one message to its topic's log. Returns `true` when the record
   * was persisted, `false` when the payload could not be JSON-serialized
   * (the message is still delivered live — only its durability is lost).
   * Never throws for I/O problems either: a full disk must not take down
   * the publish path, so write failures are swallowed here and the message
   * is delivered live. Operators watching for this should monitor
   * `stats().entries` growth.
   */
  append(record: DurableLogRecord): boolean {
    let line: string;
    try {
      line = `${JSON.stringify(logLineOf(record))}\n`;
    } catch {
      return false;
    }
    try {
      appendFileSync(this.fileFor(record.topic), line, 'utf8');
    } catch {
      return false;
    }
    this.topicsSeen.add(record.topic);
    const count = (this.entryCounts.get(record.topic) ?? 0) + 1;
    this.entryCounts.set(record.topic, count);
    if (record.seq >= 1) {
      this.messageCounts.set(record.topic, (this.messageCounts.get(record.topic) ?? 0) + 1);
    }
    if (record.seq > (this.lastSeqs.get(record.topic) ?? 0)) {
      this.lastSeqs.set(record.topic, record.seq);
    }
    // Amortized compaction: only rewrite when the file has grown to twice
    // the budget, keeping the newest `maxEntriesPerTopic` entries, so a
    // steady stream of appends does not pay a rewrite on every message.
    if (count > this.maxEntriesPerTopic * 2) {
      this.compact(record.topic);
    }
    return true;
  }

  /** Rewrites a topic's file keeping only the newest entries. */
  private compact(topic: string): void {
    const records = this.readFileRecords(topic);
    const kept = records.slice(-this.maxEntriesPerTopic);
    const text = kept.map((rec) => `${JSON.stringify(logLineOf(rec))}\n`).join('');
    try {
      writeFileSync(this.fileFor(topic), text, 'utf8');
    } catch {
      return;
    }
    this.entryCounts.set(topic, kept.length);
    this.messageCounts.set(topic, kept.filter((rec) => rec.seq >= 1).length);
  }

  /**
   * Every logged record for `topic` with `seq` strictly greater than
   * `fromSeqExclusive`, in ascending seq order. Used to refill a
   * resubscribing consumer's queue from where it left off.
   */
  readSince(topic: string, fromSeqExclusive: number): DurableLogRecord[] {
    return this.readFileRecords(topic).filter((rec) => rec.seq > fromSeqExclusive);
  }

  /** Topics with at least one log file, in first-seen order. */
  topics(): string[] {
    return [...this.topicsSeen];
  }

  /** Highest seq logged for a topic, 0 when the topic is unknown. */
  lastSeq(topic: string): number {
    return this.lastSeqs.get(topic) ?? 0;
  }

  /** Entries retained for a topic, 0 when the topic is unknown. */
  entryCount(topic: string): number {
    return this.entryCounts.get(topic) ?? 0;
  }

  /**
   * Messages retained for a topic — entries with `seq >= 1`, excluding the
   * seq-0 delayed-delivery schedule records and tombstones. 0 when the
   * topic is unknown.
   */
  messageCount(topic: string): number {
    return this.messageCounts.get(topic) ?? 0;
  }

  /** Point-in-time log stats. */
  stats(): { topics: number; entries: number; corruptLines: number } {
    let entries = 0;
    for (const count of this.entryCounts.values()) entries += count;
    return { topics: this.topicsSeen.size, entries, corruptLines: this.corruptLines };
  }
}

/** Builds the on-disk envelope for a record. */
function logLineOf(record: DurableLogRecord): LogLine {
  const line: LogLine = { v: 1, seq: record.seq, topic: record.topic, at: record.at, payload: record.payload };
  if (record.expiresAt !== undefined) line.expiresAt = record.expiresAt;
  if (record.deliverAt !== undefined) line.deliverAt = record.deliverAt;
  if (record.delayId !== undefined) line.delayId = record.delayId;
  if (record.cancelled !== undefined) line.cancelled = record.cancelled;
  return line;
}

/**
 * Parses one log line. Returns `null` for anything malformed — wrong shape,
 * wrong version, a topic that does not match the file it was read from (a
 * swapped/renamed file must not silently corrupt reads), or a delayed-
 * delivery schedule record missing its required fields.
 *
 * `seq` 0 is reserved for delayed-delivery schedule records: they must
 * carry a non-empty `delayId`, and — unless they are a `cancelled`
 * tombstone — a finite `deliverAt`. A tombstone (`cancelled: true`) is only
 * valid with `seq` 0.
 */
function parseLogLine(line: string, expectedTopic: string): DurableLogRecord | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return null;
  if (typeof o['topic'] !== 'string' || o['topic'] !== expectedTopic) return null;
  const seq = o['seq'];
  const isScheduleRecord = seq === 0;
  if (isScheduleRecord) {
    if (typeof o['delayId'] !== 'string' || (o['delayId'] as string).length === 0) return null;
  } else if (!Number.isInteger(seq) || (seq as number) < 1) {
    return null;
  }
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return null;
  const record: DurableLogRecord = {
    seq: seq as number,
    topic: o['topic'] as string,
    at: o['at'] as number,
    payload: o['payload'],
  };
  if (o['expiresAt'] !== undefined) {
    if (typeof o['expiresAt'] !== 'number' || !Number.isFinite(o['expiresAt'])) return null;
    record.expiresAt = o['expiresAt'] as number;
  }
  if (o['delayId'] !== undefined) {
    if (typeof o['delayId'] !== 'string' || (o['delayId'] as string).length === 0) return null;
    record.delayId = o['delayId'] as string;
  }
  if (o['deliverAt'] !== undefined) {
    if (typeof o['deliverAt'] !== 'number' || !Number.isFinite(o['deliverAt'])) return null;
    record.deliverAt = o['deliverAt'] as number;
  }
  if (o['cancelled'] !== undefined) {
    // Tombstones are schedule records: `cancelled` is meaningless on a
    // real message and rejected there.
    if (o['cancelled'] !== true || !isScheduleRecord) return null;
    record.cancelled = true;
  }
  // A non-tombstone schedule record must say when it is due.
  if (isScheduleRecord && record.cancelled !== true && record.deliverAt === undefined) return null;
  return record;
}
