import {
  appendFileSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { gunzipSync, gzipSync } from 'node:zlib';
import { join } from 'node:path';
import type { EncryptedPayload, PayloadEncryptor } from './encrypt.ts';
import { isEncryptedPayload } from './encrypt.ts';

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
  /**
   * Application-level message identity (see `PublishOptions.messageId`),
   * present when the publish carried one. Replay restores it onto the
   * envelope so a resumed subscriber-side dedup window
   * (`SubscribeOptions.deduplicateMessages`) recognizes replays of
   * already-delivered messages. Also carried on seq-0 delayed-delivery
   * schedule records so a restart rebuilds the timer with the identity
   * intact.
   */
  messageId?: string;
  /**
   * Publisher identity (see `PublishOptions.publisherId`), present when
   * the publish carried one. Replay and the delayed-schedule rebuild
   * restore it onto the envelope, so producer attribution survives
   * restarts. Also carried on seq-0 delayed-delivery schedule records so
   * a restart rebuilds the timer with the attribution intact.
   */
  publisherId?: string;
  /**
   * Requested schema version (see `PublishOptions.schemaVersion`),
   * present when the publish carried one. Pause-buffered messages
   * (EB-88) carry it so restart recovery re-validates against the
   * requested version instead of silently falling back to latest.
   */
  schemaVersion?: number;
  /** Concrete topic name, as published. */
  topic: string;
  /**
   * Business event time in epoch milliseconds (see
   * `PublishOptions.eventTime`), present when the publish carried one.
   * Replay restores it onto the envelope so subscribers keep seeing the
   * original business time; the runtime event-time watermark itself is
   * not persisted — a restarted bus rebuilds it from new publishes.
   */
  eventTime?: number;
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
   * Also the tombstone marker for an MQTT-style retained message (see
   * `retained`): `true` on a seq-0 retained record means the topic's
   * retained message was cleared and must not be rebuilt on restart.
   * Also the tombstone marker for a pause-buffered message (see `paused`):
   * `true` on a seq-0 pause record means the buffered message with this
   * `pauseId` was drained at resume or dropped as expired and must not be
   * rebuilt on restart.
   */
  cancelled?: boolean;
  /**
   * Pause-buffer marker (EB-88): `true` on a seq-0 record means this line
   * is a message held by a queue-mode topic pause (`EventBus.pauseTopic`
   * with `mode: 'queue'`) — state, not a message: it carries no sequence
   * number, is invisible to `readSince` replay (which only returns
   * `seq >= 1`), and never moves the per-topic sequence counter.
   * Identified by `pauseId`; ordered across topics by `pauseOrder`. A
   * pause record never carries `delayId` or `retained` — the three seq-0
   * families are disjoint by construction. Never age-evicted by
   * time-based retention (all seq-0 records are kept): evicting a
   * buffered message would lose it on restart.
   */
  paused?: boolean;
  /**
   * Identifies one pause-buffered message (`paused-1`, …). Present on the
   * seq-0 buffer record and on its tombstone, so restart recovery can tell
   * a still-buffered message from one already drained at resume or dropped
   * as expired.
   */
  pauseId?: string;
  /**
   * Global FIFO ordinal of the pause-buffer record: buffered messages fan
   * out at resume in `pauseOrder`, preserving the original publish order
   * even across concrete topics under one pattern rule. Present on
   * non-tombstone pause records (the bus always writes it).
   */
  pauseOrder?: number;
  /**
   * MQTT-style retained-message marker (EB-87): `true` on a seq-0 record
   * means this line is retained state for the topic, not a message and
   * not a delayed-delivery schedule. A retained set record carries the
   * retained message's wire payload and its original sequence number
   * (`retainedSeq`); a retained tombstone (`cancelled: true`) carries no
   * payload and closes the retained state. Retained records are invisible
   * to `readSince` replay (which only returns `seq >= 1`) and never move
   * the per-topic sequence counter — like the delayed-delivery seq-0
   * records, they are state, not messages. Never age-evicted by
   * time-based retention: dropping retained state by age would lose the
   * snapshot on restart.
   */
  retained?: boolean;
  /**
   * The retained message's original per-topic sequence number, present
   * exactly on a non-tombstone seq-0 retained record (`retained: true`,
   * `cancelled` absent). Restart recovery rebuilds the retained table
   * with this seq so snapshot deliveries keep the message's publish-order
   * identity for gap detection.
   */
  retainedSeq?: number;
  /**
   * Publish-side partition the message was assigned to (see
   * `PublishOptions.partitionKey` / `PublishOptions.partition`, EB-90):
   * the per-(topic, partition) sequence space `seq` was drawn from.
   * Present on message records (`seq >= 1`) and on seq-0 retained-message
   * set records; absent on records written before partition routing
   * (replay treats those as partition 0). Restart recovery rebuilds the
   * per-(topic, partition) sequence counters from it, so numbering never
   * restarts and never reuses a number across restarts.
   */
  partition?: number;
  /**
   * Compaction key for keyed log compaction (see
   * `DurableLogOptions.keyCompaction`): records sharing a (topic, key)
   * keep only the latest — Kafka-style log compaction for state snapshots
   * (latest price per symbol, latest config per service). Keyless records
   * are ordinary log entries. A keyed delayed-delivery schedule record
   * (seq 0) carries the key of the message it will become; the timer
   * intent itself is never compacted away.
   */
  key?: string;
  /**
   * Per-key publish-order sequence number for keyed messages (see
   * `PublishOptions.key`): assigned by the bus at admission — at schedule
   * time for delayed deliveries, at fan-out for direct publishes — so
   * subscribers can deliver same-key messages in strict publish order
   * across topics. Present exactly when `key` is present (on message
   * records and on delayed-delivery schedule records); a line carrying
   * `keySeq` without `key` is corrupt.
   */
  keySeq?: number;
  /**
   * SHA-256 id of the preset dictionary that compressed this record's
   * payload (see `setTopicCompression`'s `dictionary` option). Present
   * only on dictionary-compressed records. The bytes themselves live in
   * the bus's dictionary registry — replay resolves them by id, so the
   * log stays small while inflation stays byte-exact. A restarted bus
   * re-registers the same dictionary by re-setting the rule.
   */
  dictId?: string;
  /**
   * Causal clock carried by the message (see `PublishOptions.causal`):
   * `{ source, clock }` — the Lamport timestamp that orders this message
   * within its source's causal stream. Persisted so replay (live or from
   * an archived segment) restores the clock and a causal subscriber keeps
   * happens-before order across restarts.
   */
  causal?: { source: string; clock: number };
  /**
   * Publish-time HMAC signature (see `EventBus.setTopicSigningKey`,
   * EB-84): `{ kid, sig }` covering (topic, seq, wire bytes). Persisted
   * so replay restores it onto the envelope and a `verifySignature`
   * subscriber checks a replayed message against the same bytes the
   * publisher signed. Never a secret — only the key id and the digest.
   */
  signature?: { kid: string; sig: string };
  /**
   * The published payload: the JSON value for ordinary messages, the raw
   * bytes (`Uint8Array`) for binary messages (EB-92, persisted as a v2
   * line), or the compression envelope when the publish was compressed.
   */
  payload: unknown;
  /**
   * Binary-payload marker (EB-92): `true` when `payload` is raw bytes
   * (`Uint8Array`) rather than a JSON value. Persisted as a v2 log line
   * (`payload_b64`) so replay restores bytes, never a misparsed value.
   */
  binary?: boolean;
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
  /**
   * Opt-in time-based retention: records whose `at` timestamp is older
   * than `now - maxAgeMs` are evicted when the topic compacts (see
   * `maxEntriesPerTopic`), so a quiet topic's stale history does not sit
   * on disk forever. The two bounds are dual constraints — a compact
   * keeps the records that satisfy BOTH (newest `maxEntriesPerTopic`
   * entries AND within the age window). Delayed-delivery schedule records
   * (`seq: 0`, timer intents rather than messages) are never evicted by
   * age: dropping a pending schedule would lose its timer on restart.
   * Retention is enforced at compact time only — a topic with no new
   * appends keeps its history until the next compact, the same
   * amortized semantics as the count bound. Evicted records are counted
   * in `stats().retentionEvicted`. Must be a positive finite number of
   * milliseconds. Absent by default (no age bound).
   */
  maxAgeMs?: number;
  /**
   * Clock source for time-based retention (`maxAgeMs`): the eviction
   * cutoff is `now() - maxAgeMs`, so an injectable clock keeps retention
   * deterministic in tests. The bus passes its own
   * `EventBusOptions.now`. Defaults to `Date.now`.
   */
  now?: () => number;
  /**
   * Bound for the publish-dedup journal (`__publish_dedup.jsonl`, EB-66):
   * the journal compacts itself to the latest claim per (topic, messageId)
   * when it grows past twice this value. The bus passes its own
   * `idempotencyMaxEntries` here so the journal's size guard tracks the
   * publish-side dedup window it persists. Must be a positive integer.
   * Default 10000.
   */
  publishDedupMaxEntries?: number;
  /**
   * Opt-in Kafka-style keyed compaction: when a published message carries
   * a `key` (see `DurableLogRecord.key`), only the latest record per
   * (topic, key) is retained and replayed — older values for the same key
   * are superseded. Compaction rewrites the file keeping the latest record
   * per key plus the newest `maxEntriesPerTopic` keyless messages;
   * `readSince` never resurrects a superseded value, and restart recovery
   * rebuilds the key index from disk. Keyless topics behave exactly as
   * before. Default false.
   */
  keyCompaction?: boolean;
  /**
   * Opt-in segment rotation for the durable log (see
   * `SegmentRotationOptions`): the per-topic live file holds the current
   * segment; when a rotation trigger fires the live file is closed and
   * archived (gzipped) in the background, and a fresh live segment
   * starts. Absent by default — without it the log keeps one live file
   * per topic exactly as before.
   */
  segmentRotation?: SegmentRotationOptions;
  /**
   * Opt-in at-rest encryption for the durable log (EB-80): when set,
   * every per-topic JSONL data line, every archived segment's record
   * payload, and the publish-dedup journal (`__publish_dedup.jsonl`) are
   * sealed with this encryptor (AES-256-GCM, the WR-11 convention:
   * random IV per line, auth tag, tampering fails loudly) before they
   * touch disk. Replay and recovery decrypt transparently; a sealed line
   * that cannot be decrypted is skipped and counted in `corruptLines`.
   * Plaintext lines written before the encryptor was configured are still
   * read (backward compatible). Opening a directory that holds sealed
   * lines WITHOUT an encryptor throws loudly — a sealed journal must
   * never be silently treated as empty or corrupt. Only meaningful with
   * `dir`; the group-offset and subscriber-dedup journals carry no
   * message content and stay plaintext.
   */
  encryptor?: PayloadEncryptor;
}

/**
 * Opt-in rotation triggers for the durable log's per-topic segments.
 * At least one trigger must be set; an empty object is a `RangeError`
 * (a rotation with no trigger is meaningless).
 */
export interface SegmentRotationOptions {
  /**
   * Maximum entries in the live segment. When an append brings the live
   * segment to this many entries it is closed and archived, and the next
   * append starts a fresh segment. Must be a positive integer.
   */
  maxEntriesPerSegment?: number;
  /**
   * Maximum age of the live segment in milliseconds, measured on the
   * record timestamps (`DurableLogRecord.at`, i.e. the bus clock): when an
   * append's timestamp is at least this far past the segment's first
   * record, the segment is closed and archived. Must be a positive finite
   * number.
   */
  maxSegmentAgeMs?: number;
}

/** On-disk envelope for one sealed log line (EB-80). `v: 2` pins the sealed format. */
interface SealedLine {
  v: 2;
  enc: EncryptedPayload;
}

/**
 * True when `o` is a sealed-line envelope — a parsed JSON object with
 * `v: 2` and a well-formed encrypted payload. Plaintext v1 lines return
 * false; malformed sealed envelopes are not sealed lines at all (they are
 * corrupt, never silently plaintext).
 */
function isSealedLine(o: Record<string, unknown>): o is { v: 2; enc: EncryptedPayload } {
  return o['v'] === 2 && isEncryptedPayload(o['enc']);
}

/** On-disk envelope for one log line. `v` pins the format for future readers. */
interface LogLine {
  /**
   * Line format version: `1` for JSON payloads, `2` for binary payloads
   * (EB-92) whose wire bytes ride as base64 in `payload_b64`.
   */
  v: 1 | 2;
  seq: number;
  topic: string;
  at: number;
  expiresAt?: number;
  deliverAt?: number;
  delayId?: string;
  cancelled?: boolean;
  /** Retained-state marker (see `DurableLogRecord.retained`). */
  retained?: boolean;
  /** The retained message's original per-topic seq (see `DurableLogRecord.retainedSeq`). */
  retainedSeq?: number;
  /** Pause-buffer marker (see `DurableLogRecord.paused`). */
  paused?: boolean;
  /** Pause-buffer message identity (see `DurableLogRecord.pauseId`). */
  pauseId?: string;
  /** Pause-buffer FIFO ordinal (see `DurableLogRecord.pauseOrder`). */
  pauseOrder?: number;
  key?: string;
  /** Per-key publish-order sequence number, present exactly when `key` is. */
  keySeq?: number;
  /**
   * Publish-side partition the message was assigned to (see
   * `DurableLogRecord.partition`). Absent on records written before
   * partition routing.
   */
  partition?: number;
  /**
   * SHA-256 id of the preset dictionary that compressed `payload`, when
   * the payload is a dictionary-compressed envelope. The bytes live in
   * the bus's registry; the id is all replay needs.
   */
  dictId?: string;
  /**
   * Application-level message identity (see
   * `DurableLogRecord.messageId`).
   */
  messageId?: string;
  /** Requested schema version (see `DurableLogRecord.schemaVersion`). */
  schemaVersion?: number;
  /**
   * Business event time in epoch milliseconds (see
   * `DurableLogRecord.eventTime`).
   */
  eventTime?: number;
  /**
   * Causal clock (see `DurableLogRecord.causal`): the source stream and
   * Lamport timestamp, present exactly when the publish carried one.
   */
  causal?: { source: string; clock: number };
  /**
   * Publish-time HMAC signature (see `DurableLogRecord.signature`):
   * `{ kid, sig }`, present exactly when the publish was signed.
   */
  signature?: { kid: string; sig: string };
  /**
   * v1 only: the JSON application payload. Absent on v2 lines, which
   * carry `payload_b64` instead.
   */
  payload?: unknown;
  /**
   * v2 only (binary messages, EB-92): base64 of the wire bytes — the raw
   * payload bytes, or the UTF-8 JSON of the compression envelope when the
   * message was compressed. The explicit version marker is what tells a
   * JSON reader "these bytes are not JSON" — a JSON-side consumer that
   * cannot handle binary sees `{ v: 2, payload_b64 }` rather than a
   * silently misinterpreted payload. Never present on v1 lines.
   */
  payload_b64?: string;
}

/**
 * One persisted consumer-group offset commit: the last per-topic `seq`
 * a consumer of `groupId` durably processed on `topic`. `partition` is
 * present for partitioned consumer groups (see
 * `GroupSubscribeOptions.partitions`): a per-partition checkpoint, with
 * `pattern` identifying which (groupId, pattern) competing set it belongs
 * to. Absent for classic group-level commits.
 */
export interface DurableOffsetCommit {
  /** The consumer group that reported the checkpoint. */
  groupId: string;
  /** Concrete topic the checkpoint covers. */
  topic: string;
  /** Last per-topic `seq` the consumer durably processed. */
  seq: number;
  /** Commit timestamp in milliseconds (the bus clock). */
  at: number;
  /** Partition index for partitioned groups; absent for group-level commits. */
  partition?: number;
  /** The competing set's pattern, present exactly when `partition` is. */
  pattern?: string;
}

/** On-disk envelope for one offset-commit line. `v` pins the format. */
interface OffsetLine {
  v: 1;
  group: string;
  topic: string;
  seq: number;
  at: number;
  partition?: number;
  pattern?: string;
}

/**
 * Key for one checkpoint: groupId and topic joined by NUL, with the
 * partition and pattern appended for per-partition commits. Neither may
 * contain NUL in practice, so the pairing is unambiguous and reversible
 * (see `recoveredCommittedOffsets`).
 */
function offsetKey(groupId: string, topic: string, partition?: number, pattern?: string): string {
  const base = `${groupId}\0${topic}`;
  return partition === undefined ? base : `${base}\0${partition}\0${pattern ?? ''}`;
}

/** Builds the on-disk envelope for an offset commit. */
function offsetLineOf(commit: DurableOffsetCommit): OffsetLine {
  return {
    v: 1,
    group: commit.groupId,
    topic: commit.topic,
    seq: commit.seq,
    at: commit.at,
    ...(commit.partition === undefined ? {} : { partition: commit.partition, pattern: commit.pattern }),
  };
}

/**
 * Parses one offset-journal line. Returns `null` for anything malformed —
 * wrong version, empty group/topic, a non-positive-integer seq, a
 * non-finite timestamp, a malformed partition, or a partition without its
 * pattern. Corrupt lines are skipped, never fatal.
 */
function parseOffsetLine(line: string): DurableOffsetCommit | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return null;
  if (typeof o['group'] !== 'string' || (o['group'] as string).length === 0) return null;
  if (typeof o['topic'] !== 'string' || (o['topic'] as string).length === 0) return null;
  const seq = o['seq'];
  if (!Number.isInteger(seq) || (seq as number) < 1) return null;
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return null;
  const partition = o['partition'];
  const pattern = o['pattern'];
  if (partition !== undefined || pattern !== undefined) {
    if (!Number.isInteger(partition) || (partition as number) < 0) return null;
    if (typeof pattern !== 'string' || (pattern as string).length === 0) return null;
  }
  return {
    groupId: o['group'] as string,
    topic: o['topic'] as string,
    seq: seq as number,
    at: o['at'] as number,
    ...(partition === undefined
      ? {}
      : { partition: partition as number, pattern: pattern as string }),
  };
}

/**
 * File name of the group-offset journal inside the log directory. A
 * `.jsonl` name — not `.log` — so topic-file recovery never mistakes it
 * for a topic file, and it can never collide with a topic file either
 * (`fileFor` always appends `.log`).
 */
const OFFSET_JOURNAL_NAME = '__group_offsets.jsonl';

/**
 * File name of the subscriber-dedup journal (`__dedup.jsonl`) inside the
 * log directory. A `.jsonl` name — not `.log` — so topic-file recovery
 * never mistakes it for a topic file, and it can never collide with a
 * topic file either (`fileFor` always appends `.log`).
 */
const DEDUP_JOURNAL_NAME = '__dedup.jsonl';

/**
 * File name of the segment-archive index (`__archive_index.jsonl`)
 * inside the log directory. A `.jsonl` name — not `.log` — so topic-file
 * recovery never mistakes it for a topic file, and it can never collide
 * with a topic file either (`fileFor` always appends `.log`).
 */
const ARCHIVE_INDEX_NAME = '__archive_index.jsonl';

/**
 * One closed, gzipped log segment: the on-disk archive of a rotated
 * per-topic live segment, described by one line of the archive index.
 */
export interface ArchiveEntry {
  /** Concrete topic the segment belongs to. */
  topic: string;
  /** Segment identity: `<seqStart>-<seqEnd>` of the segment's records. */
  segmentId: string;
  /** Archive file name inside the log directory. */
  path: string;
  /** First record's `seq` in the segment. */
  seqStart: number;
  /** Last record's `seq` in the segment. */
  seqEnd: number;
  /** First record's `at` (bus clock) in the segment. */
  atStart: number;
  /** Last record's `at` (bus clock) in the segment. */
  atEnd: number;
  /** Records in the segment, including seq-0 schedule records. */
  entries: number;
  /** Messages in the segment (`seq >= 1`), excluding schedule records. */
  messages: number;
  /** Highest per-key sequence number per key observed in the segment. */
  keySeqs: Map<string, number>;
  /**
   * Highest sequence number per partition observed in the segment
   * (EB-90). Archived seq bounds fold into the per-(topic, partition)
   * cursors, so a restart never reuses a sequence number from an
   * archived segment. Records without partition data count toward
   * partition 0.
   */
  partitionSeqs: Map<number, number>;
}

/** On-disk envelope for one archive-index line. `v` pins the format. */
interface ArchiveIndexLine {
  v: 1;
  topic: string;
  segmentId: string;
  path: string;
  seqStart: number;
  seqEnd: number;
  atStart: number;
  atEnd: number;
  entries: number;
  messages: number;
  /** Highest per-key sequence number per key in the segment, when any. */
  keySeqs?: Record<string, number>;
  /**
   * Highest sequence number per partition in the segment (EB-90), when
   * any — keys are the decimal partition numbers. Absent on index lines
   * written before partition routing (their records all belonged to
   * partition 0).
   */
  partitionSeqs?: Record<string, number>;
}

/** Builds the on-disk envelope for an archive index entry. */
function archiveIndexLineOf(entry: ArchiveEntry): ArchiveIndexLine {
  const line: ArchiveIndexLine = {
    v: 1,
    topic: entry.topic,
    segmentId: entry.segmentId,
    path: entry.path,
    seqStart: entry.seqStart,
    seqEnd: entry.seqEnd,
    atStart: entry.atStart,
    atEnd: entry.atEnd,
    entries: entry.entries,
    messages: entry.messages,
  };
  if (entry.keySeqs.size > 0) {
    const keySeqs: Record<string, number> = {};
    for (const [key, seq] of entry.keySeqs) keySeqs[key] = seq;
    line.keySeqs = keySeqs;
  }
  if (entry.partitionSeqs.size > 0) {
    const partitionSeqs: Record<string, number> = {};
    for (const [partition, seq] of entry.partitionSeqs) partitionSeqs[String(partition)] = seq;
    line.partitionSeqs = partitionSeqs;
  }
  return line;
}

/**
 * Parses one archive-index line. Returns `null` for anything malformed —
 * wrong version, an empty topic/segmentId/path, non-integer seq bounds,
 * non-finite time bounds, a non-positive-integer entry count, a negative
 * message count, a malformed keySeqs table, or a malformed partitionSeqs
 * table. Corrupt lines are skipped, never fatal.
 */
function parseArchiveIndexLine(line: string): ArchiveEntry | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return null;
  for (const field of ['topic', 'segmentId', 'path'] as const) {
    if (typeof o[field] !== 'string' || (o[field] as string).length === 0) return null;
  }
  for (const field of ['seqStart', 'seqEnd'] as const) {
    if (!Number.isInteger(o[field]) || (o[field] as number) < 0) return null;
  }
  for (const field of ['atStart', 'atEnd'] as const) {
    if (typeof o[field] !== 'number' || !Number.isFinite(o[field])) return null;
  }
  if (!Number.isInteger(o['entries']) || (o['entries'] as number) < 1) return null;
  if (!Number.isInteger(o['messages']) || (o['messages'] as number) < 0) return null;
  const keySeqs = new Map<string, number>();
  if (o['keySeqs'] !== undefined) {
    if (typeof o['keySeqs'] !== 'object' || o['keySeqs'] === null) return null;
    for (const [key, seq] of Object.entries(o['keySeqs'] as Record<string, unknown>)) {
      if (key.length === 0 || !Number.isInteger(seq) || (seq as number) < 1) return null;
      keySeqs.set(key, seq as number);
    }
  }
  const partitionSeqs = new Map<number, number>();
  if (o['partitionSeqs'] !== undefined) {
    // Keys are decimal partition numbers; values are positive-integer
    // seqs. Malformed tables make the line corrupt — the bus only ever
    // writes well-formed ones.
    if (typeof o['partitionSeqs'] !== 'object' || o['partitionSeqs'] === null) return null;
    for (const [key, seq] of Object.entries(o['partitionSeqs'] as Record<string, unknown>)) {
      if (!/^(0|[1-9][0-9]*)$/.test(key) || !Number.isInteger(seq) || (seq as number) < 1) {
        return null;
      }
      partitionSeqs.set(parseInt(key, 10), seq as number);
    }
  }
  return {
    topic: o['topic'] as string,
    segmentId: o['segmentId'] as string,
    path: o['path'] as string,
    seqStart: o['seqStart'] as number,
    seqEnd: o['seqEnd'] as number,
    atStart: o['atStart'] as number,
    atEnd: o['atEnd'] as number,
    entries: o['entries'] as number,
    messages: o['messages'] as number,
    keySeqs,
    partitionSeqs,
  };
}

/**
 * Validates `SegmentRotationOptions`, returning the resolved triggers.
 * `undefined` stays `undefined` (rotation disabled — fully backward
 * compatible). Anything else must be an object with at least one valid
 * trigger, else `RangeError`.
 */
function resolveSegmentRotationOptions(
  options: SegmentRotationOptions | undefined,
): SegmentRotationOptions | undefined {
  if (options === undefined) return undefined;
  if (typeof options !== 'object' || options === null) {
    throw new RangeError('durableLogSegmentRotation must be an object');
  }
  const { maxEntriesPerSegment, maxSegmentAgeMs } = options;
  if (maxEntriesPerSegment === undefined && maxSegmentAgeMs === undefined) {
    throw new RangeError(
      'durableLogSegmentRotation needs at least one of maxEntriesPerSegment / maxSegmentAgeMs',
    );
  }
  if (
    maxEntriesPerSegment !== undefined &&
    (!Number.isInteger(maxEntriesPerSegment) || maxEntriesPerSegment < 1)
  ) {
    throw new RangeError('durableLogSegmentRotation.maxEntriesPerSegment must be a positive integer');
  }
  if (
    maxSegmentAgeMs !== undefined &&
    (typeof maxSegmentAgeMs !== 'number' || !Number.isFinite(maxSegmentAgeMs) || maxSegmentAgeMs <= 0)
  ) {
    throw new RangeError(
      'durableLogSegmentRotation.maxSegmentAgeMs must be a positive finite number of milliseconds',
    );
  }
  return { ...(maxEntriesPerSegment === undefined ? {} : { maxEntriesPerSegment }), ...(maxSegmentAgeMs === undefined ? {} : { maxSegmentAgeMs }) };
}

/**
 * Splits an archived-segment file name back into its topic and
 * `<seqStart>-<seqEnd>` identity. The split is on the LAST `.seg-`
 * occurrence: the URL-encoded topic may itself contain `.seg-`
 * (`encodeURIComponent` leaves dots unescaped), while seq bounds are
 * non-negative integers and can never contain a dash. Returns `null`
 * when the name is not a segment file.
 */
function parseSegmentFileName(name: string): { topic: string; seqStart: number; seqEnd: number } | null {
  const gzSuffix = '.jsonl.gz';
  if (!name.endsWith(gzSuffix)) return null;
  const stem = name.slice(0, -gzSuffix.length);
  const marker = stem.lastIndexOf('.seg-');
  if (marker < 0) return null;
  let topic: string;
  try {
    topic = decodeURIComponent(stem.slice(0, marker));
  } catch {
    return null;
  }
  const bounds = stem.slice(marker + '.seg-'.length).split('-');
  if (bounds.length !== 2) return null;
  const seqStart = Number(bounds[0]);
  const seqEnd = Number(bounds[1]);
  if (!Number.isInteger(seqStart) || seqStart < 0 || !Number.isInteger(seqEnd) || seqEnd < 0) {
    return null;
  }
  return { topic, seqStart, seqEnd };
}

/**
 * One persisted subscriber-dedup sighting: the `(consumer, topic,
 * messageId)` window entry of
 * `SubscribeOptions.deduplicateMessages`. `at` is the bus-clock
 * timestamp of the first delivery into the subscriber's queue — the
 * window's expiry is evaluated against it at rehydration time.
 */
export interface DedupEntry {
  /** Stable consumer identity (`DeduplicateMessagesOptions.consumerId`). */
  consumer: string;
  /** Concrete topic the message was published to. */
  topic: string;
  /** Application-level message identity. */
  messageId: string;
  /** First-delivery timestamp in milliseconds (the bus clock). */
  at: number;
}

/** On-disk envelope for one dedup-journal line. `v` pins the format. */
interface DedupLine {
  v: 1;
  consumer: string;
  topic: string;
  messageId: string;
  at: number;
}

/**
 * Key for one dedup sighting: consumer, topic and messageId joined by
 * NUL. None may contain NUL in practice (the bus only journals
 * identities it normalized; the parser below rejects empties), so the
 * pairing is unambiguous and reversible.
 */
function dedupKey(consumer: string, topic: string, messageId: string): string {
  return `${consumer}\0${topic}\0${messageId}`;
}

/**
 * One persisted publish-side idempotent-dedup claim: the
 * `(topic, messageId)` window entry of `EventBus.publishIdempotent`
 * (EB-27). `at` is the bus-clock timestamp of the first admitted publish —
 * the window's expiry is evaluated against it at rehydration time.
 */
export interface PublishDedupEntry {
  /** Concrete topic the message was published to. */
  topic: string;
  /** Application-level message identity. */
  messageId: string;
  /** First-admission timestamp in milliseconds (the bus clock). */
  at: number;
}

/** On-disk envelope for one publish-dedup-journal line. `v` pins the format. */
interface PublishDedupLine {
  v: 1;
  topic: string;
  messageId: string;
  at: number;
}

/**
 * Key for one publish-dedup claim: topic and messageId joined by NUL —
 * the same identity the bus's in-memory dedup table keys on
 * (`${topic}\0${messageId}`), so journal and memory agree exactly.
 */
function publishDedupKey(topic: string, messageId: string): string {
  return `${topic}\0${messageId}`;
}

/** Builds the on-disk envelope for a publish-dedup claim. */
function publishDedupLineOf(entry: PublishDedupEntry): PublishDedupLine {
  return {
    v: 1,
    topic: entry.topic,
    messageId: entry.messageId,
    at: entry.at,
  };
}

/**
 * Parses one publish-dedup-journal line. Returns `null` for anything
 * malformed — wrong version, an empty topic/messageId, or a non-finite
 * timestamp. Corrupt lines are skipped, never fatal.
 */
function parsePublishDedupLine(line: string): PublishDedupEntry | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return null;
  if (typeof o['topic'] !== 'string' || (o['topic'] as string).length === 0) return null;
  if (typeof o['messageId'] !== 'string' || (o['messageId'] as string).length === 0) return null;
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return null;
  return {
    topic: o['topic'] as string,
    messageId: o['messageId'] as string,
    at: o['at'] as number,
  };
}

/** Journal file name for the publish-side idempotent-dedup claims. */
const PUBLISH_DEDUP_JOURNAL_NAME = '__publish_dedup.jsonl';

/** Builds the on-disk envelope for a dedup sighting. */
function dedupLineOf(entry: DedupEntry): DedupLine {
  return {
    v: 1,
    consumer: entry.consumer,
    topic: entry.topic,
    messageId: entry.messageId,
    at: entry.at,
  };
}

/**
 * Parses one dedup-journal line. Returns `null` for anything malformed —
 * wrong version, an empty consumer/topic/messageId, or a non-finite
 * timestamp. Corrupt lines are skipped, never fatal.
 */
function parseDedupLine(line: string): DedupEntry | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 1) return null;
  if (typeof o['consumer'] !== 'string' || (o['consumer'] as string).length === 0) return null;
  if (typeof o['topic'] !== 'string' || (o['topic'] as string).length === 0) return null;
  if (typeof o['messageId'] !== 'string' || (o['messageId'] as string).length === 0) return null;
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return null;
  return {
    consumer: o['consumer'] as string,
    topic: o['topic'] as string,
    messageId: o['messageId'] as string,
    at: o['at'] as number,
  };
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
 * Keyed compaction (`DurableLogOptions.keyCompaction`) is Kafka-style log
 * compaction for state snapshots: a published message may carry a `key`,
 * and the log then retains only the latest record per (topic, key).
 * Superseded records linger on disk until the next compaction pass — reads
 * (`readSince`) and restart recovery dedupe them in memory, so a
 * superseded value is never replayed even before the file is rewritten.
 *
 * Besides the per-topic message files, the log maintains one group-offset
 * journal (`__group_offsets.jsonl`): every consumer-group offset commit
 * (`EventBus.commitOffset`) is appended as one line, and opening the log
 * recovers the highest committed seq per (group, topic). A restarted bus
 * seeded from the same directory therefore resumes its committed offsets —
 * a rejoining consumer that replays from `getCommittedOffsets` does not
 * lose its checkpoint across restarts. The journal compacts itself to the
 * latest commit per (group, topic) when it grows past twice
 * `maxEntriesPerTopic` lines.
 *
 * The log also maintains one publish-dedup journal
 * (`__publish_dedup.jsonl`): every publish-side idempotent-dedup claim
 * (`EventBus.publishIdempotent`) is appended as one line, and opening the
 * log recovers the claims that have not aged out of the idempotency
 * window. A restarted bus seeded from the same directory therefore keeps
 * suppressing within-window retries — the payment/fintech retry pattern
 * survives process restarts. The journal compacts itself to the newest
 * `publishDedupMaxEntries` claims when it grows past twice that many
 * lines.
 *
 * Segment archiving (`DurableLogOptions.segmentRotation`, opt-in): the
 * per-topic live file holds the current segment. When an append reaches
 * `maxEntriesPerSegment` entries or the segment's age reaches
 * `maxSegmentAgeMs` (measured on the record timestamps), the live file is
 * atomically renamed aside and a fresh live segment starts — the publish
 * path only does the O(1) rename. An unref'd background pass then gzips
 * the closed segment to `<topic>.seg-<seqStart>-<seqEnd>.jsonl.gz` and
 * appends one line to the append-only archive index
 * (`__archive_index.jsonl`): `{topic, segmentId, path, seqStart, seqEnd,
 * atStart, atEnd, entries, messages}` plus the segment's per-key seq
 * highs. Replay (`readSince`/`readSinceTime`) reads the live segment by
 * default; `{ includeArchived: true }` decompresses the archived segments
 * on demand and merges them in seq order ahead of the live records, under
 * the same TTL/keyed-compaction rules. Archive files that fail to
 * decompress are skipped and counted (`stats().corruptArchives`), never
 * fatal; restart recovery rebuilds the archive view from the index and
 * adopts orphan archives, so seq/keySeq numbering never reuses an
 * archived number.
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
  private readonly keyCompaction: boolean;
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
  /**
   * Highest seq observed per (topic, partition) (EB-90; recovered +
   * appended). Records written before partition routing carry no
   * partition and count toward partition 0 — the partition every
   * pre-partitioning message belonged to. The bus seeds its
   * per-(topic, partition) sequence counters from this so numbering
   * continues across restarts instead of restarting at 1 (which would
   * collide with replayed seqs). Like `lastSeqs` this is a max-ever
   * cursor: compaction never moves it backwards.
   */
  private readonly maxPartitionSeqs = new Map<string, Map<number, number>>();
  /**
   * Latest message seq per (topic, key), rebuilt on recovery and
   * maintained on append — the in-memory side of keyed compaction. Only
   * populated when `keyCompaction` is on; seq-0 schedule records never
   * enter it (they are timer intents, not messages).
   */
  private readonly keyIndex = new Map<string, Map<string, number>>();
  /**
   * Highest per-key sequence number observed per key (recovered + appended).
   * Seeds the bus's key cursors on restart so per-key publish-order
   * numbering never restarts (mirrors the per-topic `lastSeqs` recovery).
   * Tracked for every keyed record — schedule records included, since a
   * delayed schedule already consumed its keySeq at schedule time.
   */
  private readonly maxKeySeqs = new Map<string, number>();
  /**
   * Per-topic count of keyed records superseded by a newer record for the
   * same key and still sitting in the file. Drives the compaction trigger
   * for keyed topics: waiting for the 2x entry-count trigger would let a
   * hot single key bloat the file with dead records.
   */
  private readonly supersededCounts = new Map<string, number>();
  /** Lines that failed to parse during recovery/reads. Skipped, never fatal. */
  private corruptLines = 0;
  /**
   * Path of the group-offset journal (`__group_offsets.jsonl`) inside the
   * log directory. One append-only file for every group, separate from the
   * per-topic message files.
   */
  private readonly offsetFile: string;
  /**
   * Latest committed offset per (group, topic), keyed by
   * `offsetKey(groupId, topic)` — the in-memory side of the offset
   * journal, rebuilt on open and maintained on append.
   */
  private readonly offsetCommits = new Map<string, DurableOffsetCommit>();
  /** Lines currently in the offset journal, used for compaction. */
  private offsetEntries = 0;
  /**
   * Path of the subscriber-dedup journal (`__dedup.jsonl`) inside the
   * log directory. One append-only file for every dedup-enabled
   * consumer, separate from the per-topic message files.
   */
  private readonly dedupFile: string;
  /**
   * Latest dedup sighting per (consumer, topic, messageId), keyed by
   * `dedupKey(...)` — the in-memory side of the dedup journal, rebuilt
   * on open and maintained on append.
   */
  private readonly dedupSeen = new Map<string, DedupEntry>();
  /** Lines currently in the dedup journal, used for compaction. */
  private dedupEntries = 0;
  /**
   * Path of the publish-dedup journal (`__publish_dedup.jsonl`) inside
   * the log directory. One append-only file for the whole bus —
   * publish-side dedup identities are scoped to concrete topics (which
   * already include any namespace prefix), so a single root journal
   * covers every namespace.
   */
  private readonly publishDedupFile: string;
  /**
   * Latest claim per (topic, messageId), keyed by
   * `publishDedupKey(...)` — the in-memory side of the publish-dedup
   * journal, rebuilt on open and maintained on append.
   */
  private readonly publishDedupSeen = new Map<string, PublishDedupEntry>();
  /** Lines currently in the publish-dedup journal, used for compaction. */
  private publishDedupEntries = 0;
  /**
   * Publish-dedup window bound (see `DurableLogOptions.publishDedupMaxEntries`):
   * the journal compacts itself to the latest claim per (topic, messageId)
   * when it grows past twice this value, so an idempotent-heavy producer
   * cannot grow the journal without bound.
   */
  private readonly publishDedupMaxEntries: number;
  /**
   * Opt-in segment rotation triggers, `undefined` when rotation is
   * disabled (the default — one live file per topic, as before).
   */
  private readonly segmentRotation?: SegmentRotationOptions;
  /**
   * Entries appended to the current live segment per topic. Reset on
   * every rotation; seeded from the live file on recovery.
   */
  private readonly segmentEntries = new Map<string, number>();
  /** First record's `seq` of the current live segment per topic. */
  private readonly segmentStartSeq = new Map<string, number>();
  /** First record's `at` of the current live segment per topic. */
  private readonly segmentStartAt = new Map<string, number>();
  /**
   * Closed archive segments in index order — the in-memory view of
   * `__archive_index.jsonl`, rebuilt on open.
   */
  private readonly archives: ArchiveEntry[] = [];
  /** Archive segments per topic, in ascending `seqStart` order. */
  private readonly archivesByTopic = new Map<string, ArchiveEntry[]>();
  /**
   * Archive files that failed to decompress during archived reads.
   * Skipped, never fatal.
   */
  private corruptArchives = 0;
  /** Path of the archive index (`__archive_index.jsonl`) in the log dir. */
  private readonly archiveIndexFile: string;
  /** Set while a background archive pass is scheduled. */
  private archiveScheduled = false;
  /**
   * At-rest encryptor (EB-80), `undefined` when the log stores plaintext
   * (the default — fully backward compatible).
   */
  private readonly encryptor?: PayloadEncryptor;
  /**
   * Time-based retention bound in milliseconds (`maxAgeMs`), `undefined`
   * when no age bound is configured. Mutable at runtime via
   * `setMaxAgeMs` — the bus exposes it as `setDurableLogMaxAgeMs` with a
   * subscription-audit trail.
   */
  private maxAgeMs: number | undefined;
  /** Clock source for the retention cutoff; defaults to `Date.now`. */
  private readonly now: () => number;
  /** Records evicted by time-based retention, across compactions. */
  private retentionEvicted = 0;

  private constructor(
    dir: string,
    maxEntriesPerTopic: number,
    keyCompaction: boolean,
    segmentRotation?: SegmentRotationOptions,
    publishDedupMaxEntries = 10000,
    encryptor?: PayloadEncryptor,
    maxAgeMs?: number,
    now: () => number = Date.now,
  ) {
    this.logDir = dir;
    this.maxEntriesPerTopic = maxEntriesPerTopic;
    this.keyCompaction = keyCompaction;
    this.segmentRotation = segmentRotation;
    this.publishDedupMaxEntries = publishDedupMaxEntries;
    this.encryptor = encryptor;
    this.maxAgeMs = maxAgeMs;
    this.now = now;
    this.offsetFile = join(dir, OFFSET_JOURNAL_NAME);
    this.dedupFile = join(dir, DEDUP_JOURNAL_NAME);
    this.publishDedupFile = join(dir, PUBLISH_DEDUP_JOURNAL_NAME);
    this.archiveIndexFile = join(dir, ARCHIVE_INDEX_NAME);
    mkdirSync(dir, { recursive: true });
    this.recover();
    this.recoverArchiveIndex();
    this.recoverOffsets();
    this.recoverDedup();
    this.recoverPublishDedup();
  }

  /**
   * Opens (or creates) the durable log at `dir`. Throws `RangeError` for an
   * empty `dir`, a non-positive-integer `maxEntriesPerTopic`, a
   * non-positive-integer `publishDedupMaxEntries`, invalid
   * `segmentRotation` triggers, a non-positive-finite `maxAgeMs`, a
   * non-function `now`, or an `encryptor` that is not a usable
   * `PayloadEncryptor`.
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
    const publishDedupMaxEntries = options.publishDedupMaxEntries ?? 10000;
    if (!Number.isInteger(publishDedupMaxEntries) || publishDedupMaxEntries < 1) {
      throw new RangeError('publishDedupMaxEntries must be a positive integer');
    }
    const maxAgeMs = options.maxAgeMs;
    if (maxAgeMs !== undefined && (!Number.isFinite(maxAgeMs) || maxAgeMs <= 0)) {
      throw new RangeError('durableLogMaxAgeMs must be a positive finite number of milliseconds');
    }
    const now = options.now ?? Date.now;
    if (typeof now !== 'function') {
      throw new RangeError('durableLog now must be a function');
    }
    const encryptor = options.encryptor;
    if (
      encryptor !== undefined &&
      (typeof encryptor !== 'object' ||
        encryptor === null ||
        typeof (encryptor as PayloadEncryptor).encrypt !== 'function' ||
        typeof (encryptor as PayloadEncryptor).decrypt !== 'function')
    ) {
      throw new RangeError('durableLogEncryptor must be a PayloadEncryptor with encrypt/decrypt methods');
    }
    return new DurableTopicLog(
      dir,
      maxEntriesPerTopic,
      options.keyCompaction ?? false,
      resolveSegmentRotationOptions(options.segmentRotation),
      publishDedupMaxEntries,
      encryptor,
      maxAgeMs,
      now,
    );
  }

  /**
   * Changes the time-based retention bound at runtime (`undefined`
   * clears it, restoring count-only retention). Returns the previous
   * bound. Throws `RangeError` for a non-positive-finite value. The
   * change takes effect on the next compact — already-compacted history
   * is not re-scanned. The bus wraps this as `setDurableLogMaxAgeMs`
   * and records the change in the subscription audit log
   * (`retention_updated`).
   */
  setMaxAgeMs(maxAgeMs: number | undefined): number | undefined {
    if (maxAgeMs !== undefined && (!Number.isFinite(maxAgeMs) || maxAgeMs <= 0)) {
      throw new RangeError('durableLogMaxAgeMs must be a positive finite number of milliseconds');
    }
    const previous = this.maxAgeMs;
    this.maxAgeMs = maxAgeMs;
    return previous;
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
   * Seals one plaintext JSONL line (including its trailing newline) into
   * a sealed envelope line (v2) when an encryptor is configured; a no-op
   * passthrough otherwise, so plaintext logs are byte-identical to
   * before. Every line gets its own fresh IV, so identical records seal
   * to different bytes.
   */
  private sealLine(plaintextLine: string): string {
    if (this.encryptor === undefined) return plaintextLine;
    const enc = this.encryptor.encrypt(Buffer.from(plaintextLine, 'utf8'));
    const sealed: SealedLine = { v: 2, enc };
    return `${JSON.stringify(sealed)}\n`;
  }

  /**
   * Opens one on-disk line back into its plaintext JSONL form (without
   * the trailing newline). Plaintext v1 lines pass through untouched —
   * a log that predates encryption stays readable.
   *
   * Returns `null` when the line is corrupt: unparseable JSON, a
   * malformed sealed envelope, or a sealed payload this encryptor
   * cannot open (wrong key or tampered bytes — the WR-11 loud-decrypt
   * convention, surfaced as a skipped line, never garbage).
   *
   * Throws loudly when the line IS a sealed envelope but this log has
   * no encryptor configured: a sealed journal recovered without its key
   * must never be silently treated as empty or corrupt — that is the
   * WR-11 dead-letter loud-failure convention, fail-closed. Callers let
   * this propagate (recovery fails at open) rather than swallowing it.
   */
  private openLine(line: string): string | null {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      return null;
    }
    if (typeof parsed !== 'object' || parsed === null) return null;
    const o = parsed as Record<string, unknown>;
    if (!isSealedLine(o)) return line;
    if (this.encryptor === undefined) {
      throw new Error(
        'DurableTopicLog: sealed log line found but no encryptor is configured — ' +
          'refusing to read an encrypted journal without its key',
      );
    }
    try {
      return this.encryptor.decrypt(o.enc).toString('utf8');
    } catch {
      return null;
    }
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
        this.noteKeySeq(rec.key, rec.keySeq);
        this.notePartitionSeq(topic, rec.partition, rec.seq);
      }
      this.lastSeqs.set(topic, last);
      this.messageCounts.set(topic, messages);
      if (this.keyCompaction) this.rebuildKeyIndex(topic, records);
      if (this.segmentRotation !== undefined && records.length > 0) {
        // The live file is the current segment: its first record opens
        // the segment's identity (start seq/time), its length is the
        // segment's entry count.
        this.segmentEntries.set(topic, records.length);
        this.segmentStartSeq.set(topic, records[0].seq);
        this.segmentStartAt.set(topic, records[0].at);
      }
    }
  }

  /**
   * Rebuilds the archive view from `__archive_index.jsonl`: every
   * well-formed line becomes an in-memory segment entry, corrupt lines
   * are counted and skipped. Archive files on disk but missing from the
   * index (a crash between the `.gz` write and the index append) are
   * adopted: their records are read to rebuild the entry and the index
   * line is appended, so the view heals itself. Leftover pending
   * segments (`*.jsonl.tmp` — a crash before the archive pass ran) are
   * re-queued for background archiving. Archived seq bounds fold into
   * the per-topic seq/message cursors, so a restart never reuses a
   * sequence number from an archived segment.
   */
  private recoverArchiveIndex(): void {
    let text: string | undefined;
    try {
      text = readFileSync(this.archiveIndexFile, 'utf8');
    } catch {
      text = undefined;
    }
    if (text !== undefined) {
      for (const line of text.split('\n')) {
        if (line.length === 0) continue;
        const entry = parseArchiveIndexLine(line);
        if (entry == null) {
          this.corruptLines += 1;
          continue;
        }
        this.addArchiveEntry(entry);
      }
    }
    let names: string[];
    try {
      names = readdirSync(this.logDir);
    } catch {
      return;
    }
    let pending = false;
    for (const name of names) {
      if (name.endsWith('.jsonl.tmp')) {
        pending = true;
        continue;
      }
      const parsed = parseSegmentFileName(name);
      if (parsed == null) continue;
      if (this.archives.some((e) => e.path === name)) continue;
      const adopted = this.readAdoptedSegment(name, parsed.topic);
      if (adopted == null) continue;
      this.addArchiveEntry(adopted);
      try {
        appendFileSync(this.archiveIndexFile, `${JSON.stringify(archiveIndexLineOf(adopted))}\n`, 'utf8');
      } catch {
        // The in-memory view is rebuilt; the index rewrite is retried on
        // the next adoption — a missing line never loses data, the file
        // itself is the archive.
      }
    }
    // Leftover pending segments (a crash before the archive pass ran)
    // are re-queued even when rotation is now disabled: they are closed
    // history from a previous configuration, and leaving them would lose
    // their records to `includeArchived` reads.
    if (pending) this.scheduleArchive();
  }

  /**
   * Reads an on-disk archive that the index does not know about (orphan
   * adoption during recovery). Returns `null` — and counts one corrupt
   * archive — when the file cannot be decompressed or holds no records.
   */
  private readAdoptedSegment(
    name: string,
    topic: string,
  ): ArchiveEntry | null {
    let text: string;
    try {
      text = gunzipSync(readFileSync(join(this.logDir, name))).toString('utf8');
    } catch {
      this.corruptArchives += 1;
      return null;
    }
    const records: DurableLogRecord[] = [];
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      const inner = this.openLine(line);
      if (inner == null) {
        this.corruptLines += 1;
        continue;
      }
      const rec = parseLogLine(inner, topic);
      if (rec == null) {
        this.corruptLines += 1;
        continue;
      }
      records.push(rec);
    }
    if (records.length === 0) return null;
    return this.summarizeSegment(topic, name, records);
  }

  /**
   * Records one archive segment in the in-memory view: per-topic
   * grouping (kept in ascending `seqStart` order), the topic's seq and
   * message cursors, and the per-key seq cursors — so restarts continue
   * numbering exactly where the archived history left off.
   */
  private addArchiveEntry(entry: ArchiveEntry): void {
    this.archives.push(entry);
    let byTopic = this.archivesByTopic.get(entry.topic);
    if (byTopic === undefined) {
      byTopic = [];
      this.archivesByTopic.set(entry.topic, byTopic);
    }
    byTopic.push(entry);
    byTopic.sort((a, b) => a.seqStart - b.seqStart);
    this.topicsSeen.add(entry.topic);
    if (entry.seqEnd > (this.lastSeqs.get(entry.topic) ?? 0)) {
      this.lastSeqs.set(entry.topic, entry.seqEnd);
    }
    this.messageCounts.set(entry.topic, (this.messageCounts.get(entry.topic) ?? 0) + entry.messages);
    for (const [key, seq] of entry.keySeqs) this.noteKeySeq(key, seq);
    // Archived seq bounds fold into the per-(topic, partition) cursors
    // (EB-90), so a restart never reuses a sequence number from an
    // archived segment. Segments archived before partition routing carry
    // no per-partition data: their seqEnd belongs to partition 0, like
    // their records did.
    if (entry.partitionSeqs.size === 0) {
      this.notePartitionSeq(entry.topic, 0, entry.seqEnd);
    } else {
      for (const [partition, seq] of entry.partitionSeqs) {
        this.notePartitionSeq(entry.topic, partition, seq);
      }
    }
  }

  /** Summarizes parsed records into an archive entry. */
  private summarizeSegment(topic: string, path: string, records: DurableLogRecord[]): ArchiveEntry {
    const first = records[0];
    const last = records[records.length - 1];
    let messages = 0;
    const keySeqs = new Map<string, number>();
    const partitionSeqs = new Map<number, number>();
    for (const rec of records) {
      if (rec.seq >= 1) messages += 1;
      if (rec.key !== undefined && rec.keySeq !== undefined && rec.keySeq > (keySeqs.get(rec.key) ?? 0)) {
        keySeqs.set(rec.key, rec.keySeq);
      }
      // Seq-0 schedule records carry no sequence number; records without
      // partition data count toward partition 0.
      if (rec.seq >= 1) {
        const p = rec.partition ?? 0;
        if (rec.seq > (partitionSeqs.get(p) ?? 0)) partitionSeqs.set(p, rec.seq);
      }
    }
    return {
      topic,
      segmentId: `${first.seq}-${last.seq}`,
      path,
      seqStart: first.seq,
      seqEnd: last.seq,
      atStart: first.at,
      atEnd: last.at,
      entries: records.length,
      messages,
      keySeqs,
      partitionSeqs,
    };
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
      // EB-80: sealed lines are opened transparently; a sealed line with
      // no encryptor throws loudly here (fail-closed at recovery).
      const inner = this.openLine(line);
      if (inner == null) {
        this.corruptLines += 1;
        continue;
      }
      const rec = parseLogLine(inner, topic);
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
      // EB-80: sealed per line when an encryptor is configured.
      line = this.sealLine(`${JSON.stringify(logLineOf(record))}\n`);
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
    this.noteKeySeq(record.key, record.keySeq);
    this.notePartitionSeq(record.topic, record.partition, record.seq);
    if (this.keyCompaction && record.key !== undefined && record.seq >= 1) {
      let keys = this.keyIndex.get(record.topic);
      if (keys == null) {
        keys = new Map();
        this.keyIndex.set(record.topic, keys);
      }
      if (keys.has(record.key)) {
        // The previous record for this key is now dead weight in the
        // file — it stays on disk until compaction, but reads and
        // recovery already ignore it.
        this.supersededCounts.set(
          record.topic,
          (this.supersededCounts.get(record.topic) ?? 0) + 1,
        );
      }
      keys.set(record.key, record.seq);
    }
    // Amortized compaction: only rewrite when the file has grown to twice
    // the budget, keeping the newest `maxEntriesPerTopic` entries, so a
    // steady stream of appends does not pay a rewrite on every message.
    // With keyed compaction a hot single key would otherwise bloat the
    // file with superseded records long before the 2x trigger, so a full
    // budget's worth of dead keyed records compacts too.
    //
    // Segment rotation (opt-in) runs before the compaction trigger: the
    // triggering record belongs to the closed segment, so a rotation
    // resets the live counts and the trigger below must read them fresh
    // — never the stale pre-rotation count.
    if (this.segmentRotation !== undefined) this.noteSegmentAppend(record);
    const liveCount = this.entryCounts.get(record.topic) ?? 0;
    const superseded = this.supersededCounts.get(record.topic) ?? 0;
    if (liveCount > this.maxEntriesPerTopic * 2 || (this.keyCompaction && superseded >= this.maxEntriesPerTopic)) {
      this.compact(record.topic);
    }
    return true;
  }

  /**
   * File name for a topic's archived segment: the URL-encoded topic plus
   * the deterministic `<seqStart>-<seqEnd>` identity, so re-archiving the
   * same content (a crash between the index append and the pending-file
   * delete) addresses the identical file and index entry — archive is
   * idempotent by construction.
   */
  private archivedSegmentName(topic: string, seqStart: number, seqEnd: number): string {
    return `${encodeURIComponent(topic)}.seg-${seqStart}-${seqEnd}.jsonl.gz`;
  }

  /** Pending (not yet gzipped) name for a closed segment awaiting archive. */
  private pendingSegmentName(topic: string, seqStart: number, seqEnd: number): string {
    return `${encodeURIComponent(topic)}.seg-${seqStart}-${seqEnd}.jsonl.tmp`;
  }

  /**
   * Segment-rotation bookkeeping for one append (rotation enabled only).
   * The publish path does nothing heavier than integer comparisons here:
   * closing a segment is an atomic file rename and the gzip + index
   * write happen on the background archive pass, so archiving can never
   * block publishing.
   */
  private noteSegmentAppend(record: DurableLogRecord): void {
    const rotation = this.segmentRotation as SegmentRotationOptions;
    const topic = record.topic;
    if (!this.segmentStartSeq.has(topic)) {
      this.segmentStartSeq.set(topic, record.seq);
      this.segmentStartAt.set(topic, record.at);
    }
    const entries = (this.segmentEntries.get(topic) ?? 0) + 1;
    this.segmentEntries.set(topic, entries);
    const maxEntries = rotation.maxEntriesPerSegment;
    const maxAge = rotation.maxSegmentAgeMs;
    const startAt = this.segmentStartAt.get(topic) as number;
    if (
      (maxEntries !== undefined && entries >= maxEntries) ||
      (maxAge !== undefined && record.at - startAt >= maxAge)
    ) {
      this.rotateSegment(topic);
    }
  }

  /**
   * Closes the topic's live segment: the live file is atomically renamed
   * to its pending archive name and the in-memory live counts reset — a
   * fresh live segment starts with the next append. The rename is O(1);
   * the expensive work (gzip + index) runs on the background archive
   * pass. Never throws: a failed rename just leaves the records in the
   * live file and the segment keeps growing until the next append
   * retries the rotation.
   */
  private rotateSegment(topic: string): void {
    const seqStart = this.segmentStartSeq.get(topic) ?? 0;
    const seqEnd = this.lastSeqs.get(topic) ?? seqStart;
    const live = this.fileFor(topic);
    const pending = join(this.logDir, this.pendingSegmentName(topic, seqStart, seqEnd));
    try {
      renameSync(live, pending);
    } catch {
      return;
    }
    // The archived records leave the live view: live counts reset, while
    // `lastSeqs` keeps the max so numbering never reuses an archived seq.
    // The live file is empty again, so its key index and superseded
    // counts reset too — archived keyed records are superseded-agnostic
    // now, resolved by the merged read instead.
    this.entryCounts.set(topic, 0);
    this.messageCounts.set(topic, 0);
    this.segmentEntries.set(topic, 0);
    this.segmentStartSeq.delete(topic);
    this.segmentStartAt.delete(topic);
    if (this.keyCompaction) {
      this.keyIndex.delete(topic);
      this.supersededCounts.delete(topic);
    }
    this.scheduleArchive();
  }

  /**
   * Arms the background archive pass on an unref'd timer: pending
   * segments are gzipped and indexed without ever blocking the publish
   * path, and a pending archive never keeps the process alive on its
   * own. Coalesced — one timer drains every pending segment.
   */
  private scheduleArchive(): void {
    if (this.archiveScheduled) return;
    this.archiveScheduled = true;
    const timer = setTimeout(() => {
      this.archiveScheduled = false;
      this.drainArchiveQueue();
    }, 0);
    timer.unref();
  }

  /** Archives every pending segment (`*.jsonl.tmp`) in the log directory. */
  private drainArchiveQueue(): void {
    let names: string[];
    try {
      names = readdirSync(this.logDir);
    } catch {
      return;
    }
    for (const name of names) {
      if (name.endsWith('.jsonl.tmp')) this.archiveOne(name);
    }
  }

  /**
   * Archives one pending segment: parses its records, gzips them to the
   * deterministic archive name, appends the index line, and deletes the
   * pending file. Crash-safe ordering — `.gz` first, index second,
   * pending delete last — so a crash either leaves an orphan `.gz`
   * (adopted on the next open) or a leftover pending file (re-archived
   * idempotently: the same content addresses the same archive name, and
   * an already-indexed segment just drops its pending file). Never
   * throws: a failure leaves the pending file for the next pass.
   */
  private archiveOne(pendingName: string): void {
    const pendingPath = join(this.logDir, pendingName);
    let text: string;
    try {
      text = readFileSync(pendingPath, 'utf8');
    } catch {
      return;
    }
    // The topic rides the file name (see `pendingSegmentName`); records
    // are validated against it, so a renamed file can never corrupt reads.
    const marker = pendingName.lastIndexOf('.seg-');
    if (marker < 0) return;
    let topic: string;
    try {
      topic = decodeURIComponent(pendingName.slice(0, marker));
    } catch {
      return;
    }
    const records: DurableLogRecord[] = [];
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      // The pending file holds sealed lines when encryption is on (it is
      // the renamed live file) — open each line before parsing.
      const inner = this.openLine(line);
      if (inner == null) {
        this.corruptLines += 1;
        continue;
      }
      const rec = parseLogLine(inner, topic);
      if (rec == null) {
        this.corruptLines += 1;
        continue;
      }
      records.push(rec);
    }
    if (records.length === 0) {
      try {
        unlinkSync(pendingPath);
      } catch {
        // Left for the next pass.
      }
      return;
    }
    const first = records[0];
    const last = records[records.length - 1];
    const entry = this.summarizeSegment(
      topic,
      this.archivedSegmentName(topic, first.seq, last.seq),
      records,
    );
    if (this.archives.some((e) => e.path === entry.path && e.entries === entry.entries)) {
      // Already indexed (crash between the index append and the pending
      // delete): drop the leftover, the archive itself is intact.
      try {
        unlinkSync(pendingPath);
      } catch {
        // Left for the next pass.
      }
      return;
    }
    const gzPath = join(this.logDir, entry.path);
    try {
      // EB-80: the archived record bytes are sealed per line before
      // compression, so the archive at rest is encrypted end to end.
      const canonical = records.map((rec) => this.sealLine(`${JSON.stringify(logLineOf(rec))}\n`)).join('');
      writeFileSync(gzPath, gzipSync(canonical), { flag: 'wx' });
    } catch {
      // Either the gzip/write failed or the archive already exists (a
      // concurrent pass won the race): leave the pending file for the
      // next pass, which dedupes against the index.
      return;
    }
    try {
      appendFileSync(this.archiveIndexFile, `${JSON.stringify(archiveIndexLineOf(entry))}\n`, 'utf8');
    } catch {
      // The `.gz` is safely on disk; the index append is retried via
      // orphan adoption on the next open.
      return;
    }
    try {
      unlinkSync(pendingPath);
    } catch {
      // The archive and index are complete; the leftover pending file is
      // dropped idempotently by the next pass.
    }
    this.addArchiveEntry(entry);
  }

  /**
   * Rewrites a topic's file keeping only the newest entries. Time-based
   * retention (`maxAgeMs`) applies first — records older than
   * `now - maxAgeMs` are evicted, except the seq-0 delayed-delivery
   * schedule records, which are timer intents and never age out — then
   * the count bound trims the survivors, so both constraints hold.
   */
  private compact(topic: string): void {
    const records = this.readFileRecords(topic);
    const retained = this.applyRetention(records);
    const kept = this.keyCompaction
      ? compactKeyed(retained, this.maxEntriesPerTopic)
      : retained.slice(-this.maxEntriesPerTopic);
    const text = kept.map((rec) => this.sealLine(`${JSON.stringify(logLineOf(rec))}\n`)).join('');
    try {
      writeFileSync(this.fileFor(topic), text, 'utf8');
    } catch {
      return;
    }
    this.entryCounts.set(topic, kept.length);
    this.messageCounts.set(topic, kept.filter((rec) => rec.seq >= 1).length);
    if (this.keyCompaction) this.rebuildKeyIndex(topic, kept);
  }

  /**
   * Evicts records older than the time-based retention window
   * (`now - maxAgeMs`), counting them in `retentionEvicted`. Seq-0
   * delayed-delivery schedule records (and their tombstones) are timer
   * intents, not messages — they are never evicted by age, or a restart
   * would lose pending timers. A record exactly on the cutoff
   * (`at === now - maxAgeMs`) is kept; only strictly older records go.
   * No-op when no age bound is configured.
   */
  private applyRetention(records: DurableLogRecord[]): DurableLogRecord[] {
    const maxAgeMs = this.maxAgeMs;
    if (maxAgeMs === undefined) return records;
    const cutoff = this.now() - maxAgeMs;
    const retained: DurableLogRecord[] = [];
    for (const rec of records) {
      if (rec.seq === 0 || rec.at >= cutoff) {
        retained.push(rec);
        continue;
      }
      this.retentionEvicted += 1;
    }
    return retained;
  }

  /**
   * Rebuilds the key index and superseded count for a topic from its
   * records (recovery and post-compaction). Only message records (seq >= 1)
   * participate: seq-0 schedule records are timer intents, and tombstones
   * carry no key. Records are in file order, so the last write per key
   * wins — the same "latest per key" rule `readSince` applies.
   */
  private rebuildKeyIndex(topic: string, records: DurableLogRecord[]): void {
    const keys = new Map<string, number>();
    let keyed = 0;
    for (const rec of records) {
      if (rec.key === undefined || rec.seq < 1) continue;
      keyed += 1;
      keys.set(rec.key, rec.seq);
    }
    if (keys.size > 0) this.keyIndex.set(topic, keys);
    else this.keyIndex.delete(topic);
    // By construction a freshly compacted file has no superseded records;
    // after recovery this counts the dead weight already on disk.
    this.supersededCounts.set(topic, keyed - keys.size);
  }

  /**
   * Every logged record for `topic` with `seq` strictly greater than
   * `fromSeqExclusive`, in ascending seq order. Used to refill a
   * resubscribing consumer's queue from where it left off.
   *
   * By default only the live segment is read: closed segments are
   * history, and a resume must not pay decompression for data it already
   * consumed. Pass `{ includeArchived: true }` to also read the archived
   * segments on demand — archived records are decompressed, filtered by
   * the same cursor, and merged in ascending seq order ahead of the live
   * records. An archive file that fails to decompress is skipped and
   * counted (`stats().corruptArchives`), never fatal.
   *
   * With keyed compaction on, only the latest record per key is returned:
   * a superseded value is never replayed, even before the file is
   * rewritten — and an archived value superseded by a live record is not
   * resurrected by `includeArchived` either. Keyless records pass through
   * untouched, and seq-0 schedule records (timer intents, requested via a
   * negative bound by delayed-delivery recovery) are never deduped.
   */
  readSince(
    topic: string,
    fromSeqExclusive: number,
    opts?: { includeArchived?: boolean },
  ): DurableLogRecord[] {
    const live = this.readFileRecords(topic).filter((rec) => rec.seq > fromSeqExclusive);
    let records = live;
    if (opts?.includeArchived === true) {
      const archived = this.readArchivedRecords(topic, fromSeqExclusive);
      // Archived segments predate the live segment; the stable sort keeps
      // file order for equal seqs (which cannot happen across segments —
      // seq ranges are disjoint — but defensively).
      records = [...archived, ...live].sort((a, b) => a.seq - b.seq);
    }
    if (!this.keyCompaction) return records;
    const latestByKey = new Map<string, DurableLogRecord>();
    for (const rec of records) {
      // Records arrive in file (ascending seq) order, so the last write
      // per key wins.
      if (rec.key !== undefined && rec.seq >= 1) latestByKey.set(rec.key, rec);
    }
    if (latestByKey.size === 0) return records;
    return records.filter((rec) => rec.key === undefined || rec.seq < 1 || latestByKey.get(rec.key) === rec);
  }

  /**
   * Reads one topic's archived segments: every record with `seq`
   * strictly greater than `fromSeqExclusive`, in ascending seq order.
   * Pending (rotated but not yet gzipped) segments are read too — they
   * are logically archived, only the compression is still in flight.
   */
  private readArchivedRecords(topic: string, fromSeqExclusive: number): DurableLogRecord[] {
    const out: DurableLogRecord[] = [];
    const segments = this.archivesByTopic.get(topic) ?? [];
    for (const segment of segments) {
      if (segment.seqEnd <= fromSeqExclusive) continue;
      let text: string;
      try {
        text = gunzipSync(readFileSync(join(this.logDir, segment.path))).toString('utf8');
      } catch {
        this.corruptArchives += 1;
        continue;
      }
      for (const line of text.split('\n')) {
        if (line.length === 0) continue;
        // EB-80: archived record bytes are sealed per line; opened
        // transparently, undecryptable lines skipped as corrupt.
        const inner = this.openLine(line);
        if (inner == null) {
          this.corruptLines += 1;
          continue;
        }
        const rec = parseLogLine(inner, topic);
        if (rec == null) {
          this.corruptLines += 1;
          continue;
        }
        if (rec.seq > fromSeqExclusive) out.push(rec);
      }
    }
    // Pending segments: rotated, awaiting the background archive pass.
    // The rename is atomic, so the file is complete and safe to read.
    const prefix = `${encodeURIComponent(topic)}.seg-`;
    let names: string[];
    try {
      names = readdirSync(this.logDir);
    } catch {
      return out;
    }
    for (const name of names) {
      if (!name.startsWith(prefix) || !name.endsWith('.jsonl.tmp')) continue;
      let text: string;
      try {
        text = readFileSync(join(this.logDir, name), 'utf8');
      } catch {
        continue;
      }
      for (const line of text.split('\n')) {
        if (line.length === 0) continue;
        const inner = this.openLine(line);
        if (inner == null) {
          this.corruptLines += 1;
          continue;
        }
        const rec = parseLogLine(inner, topic);
        if (rec == null) {
          this.corruptLines += 1;
          continue;
        }
        if (rec.seq > fromSeqExclusive) out.push(rec);
      }
    }
    out.sort((a, b) => a.seq - b.seq);
    return out;
  }

  /**
   * Every logged record for `topic` with `at` strictly greater than
   * `fromTimeExclusive`, in file (ascending seq) order. Used for
   * time-based catch-up replay
   * (`SubscribeOptions.resumeFromTime`): a new consumer (cold start)
   * or a disaster-recovery replay starts from a wall-clock moment on
   * the bus clock instead of a per-topic sequence checkpoint.
   *
   * With keyed compaction on, only the latest record per key is
   * returned — the same "latest per key" rule `readSince` applies, so
   * a superseded value is never replayed by time either. seq-0
   * schedule records (timer intents) pass through untouched.
   */
  readSinceTime(
    topic: string,
    fromTimeExclusive: number,
    opts?: { includeArchived?: boolean },
  ): DurableLogRecord[] {
    const live = this.readFileRecords(topic).filter((rec) => rec.at > fromTimeExclusive);
    let records = live;
    if (opts?.includeArchived === true) {
      // Time cursors have no seq bound: read every archived record and
      // filter by time, like the live path.
      const archived = this.readArchivedRecords(topic, -1).filter(
        (rec) => rec.at > fromTimeExclusive,
      );
      records = [...archived, ...live].sort((a, b) => a.at - b.at || a.seq - b.seq);
    }
    if (!this.keyCompaction) return records;
    const latestByKey = new Map<string, DurableLogRecord>();
    // Dedup in seq order: the last write per key wins, regardless of the
    // time-ordered return order above.
    for (const rec of [...records].sort((a, b) => a.seq - b.seq)) {
      if (rec.key !== undefined && rec.seq >= 1) latestByKey.set(rec.key, rec);
    }
    if (latestByKey.size === 0) return records;
    return records.filter((rec) => rec.key === undefined || rec.seq < 1 || latestByKey.get(rec.key) === rec);
  }

  /** Topics with at least one log file, in first-seen order. */
  topics(): string[] {
    return [...this.topicsSeen];
  }

  /**
   * Scans the offset journal and rebuilds the in-memory checkpoint index:
   * the highest committed seq per (group, topic) wins, so a restarted bus
   * resumes from where its consumers committed, not from zero. Corrupt
   * lines are counted and skipped; a corrupt journal never prevents the
   * log from opening.
   */
  private recoverOffsets(): void {
    let text: string;
    try {
      text = readFileSync(this.offsetFile, 'utf8');
    } catch {
      return;
    }
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      this.offsetEntries += 1;
      const commit = parseOffsetLine(line);
      if (commit == null) {
        this.corruptLines += 1;
        continue;
      }
      const key = offsetKey(commit.groupId, commit.topic, commit.partition, commit.pattern);
      const prev = this.offsetCommits.get(key);
      if (prev == null || commit.seq > prev.seq) {
        this.offsetCommits.set(key, {
          groupId: commit.groupId,
          topic: commit.topic,
          seq: commit.seq,
          at: commit.at,
          ...(commit.partition === undefined
            ? {}
            : { partition: commit.partition, pattern: commit.pattern as string }),
        });
      }
    }
  }

  /**
   * Persists one consumer-group offset commit to the append-only offset
   * journal. Returns `true` when the commit was persisted, `false` when
   * the commit could not be serialized or the write failed — the caller
   * keeps the in-memory checkpoint regardless, so a full disk must not
   * fail the commit path. Never throws.
   *
   * `opts.partition` (with `opts.pattern`) persists a per-partition
   * checkpoint for partitioned consumer groups; without it the commit is
   * the classic group-level checkpoint.
   *
   * When the journal grows past twice `maxEntriesPerTopic` lines it is
   * compacted down to the latest commit per checkpoint — per
   * (group, topic), or per (group, topic, partition) for partitioned
   * commits: a group that commits per message must not grow the journal
   * without bound.
   */
  appendOffset(
    groupId: string,
    topic: string,
    seq: number,
    at: number,
    opts?: { partition?: number; pattern?: string },
  ): boolean {
    const commit: DurableOffsetCommit = { groupId, topic, seq, at, ...opts };
    let line: string;
    try {
      line = `${JSON.stringify(offsetLineOf(commit))}\n`;
    } catch {
      return false;
    }
    try {
      appendFileSync(this.offsetFile, line, 'utf8');
    } catch {
      return false;
    }
    this.offsetEntries += 1;
    const key = offsetKey(groupId, topic, opts?.partition, opts?.pattern);
    const prev = this.offsetCommits.get(key);
    if (prev == null || seq > prev.seq) {
      this.offsetCommits.set(key, { groupId, topic, seq, at, ...opts });
    }
    if (this.offsetEntries > this.maxEntriesPerTopic * 2) {
      this.compactOffsets();
    }
    return true;
  }

  /**
   * Rewrites the offset journal keeping only the latest commit per
   * checkpoint — per (group, topic), or per (group, topic, partition) for
   * partitioned commits.
   */
  private compactOffsets(): void {
    const text = [...this.offsetCommits.values()]
      .map((c) => `${JSON.stringify(offsetLineOf(c))}\n`)
      .join('');
    try {
      writeFileSync(this.offsetFile, text, 'utf8');
    } catch {
      return;
    }
    this.offsetEntries = this.offsetCommits.size;
  }

  /**
   * Committed offsets recovered from the offset journal: the highest
   * committed seq per checkpoint, keyed by the NUL-joined checkpoint key
   * — `${groupId}\0${topic}` for group-level commits, or
   * `${groupId}\0${topic}\0${partition}\0${pattern}` for per-partition
   * commits (see `offsetKey`).
   */
  recoveredCommittedOffsets(): Map<string, number> {
    const out = new Map<string, number>();
    for (const [key, c] of this.offsetCommits) out.set(key, c.seq);
    return out;
  }

  /** Lines currently in the offset journal, 0 when no offset was ever committed. */
  offsetEntryCount(): number {
    return this.offsetEntries;
  }

  private recoverDedup(): void {
    let text: string;
    try {
      text = readFileSync(this.dedupFile, 'utf8');
    } catch {
      return;
    }
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      this.dedupEntries += 1;
      const entry = parseDedupLine(line);
      if (entry == null) {
        this.corruptLines += 1;
        continue;
      }
      const key = dedupKey(entry.consumer, entry.topic, entry.messageId);
      const prev = this.dedupSeen.get(key);
      if (prev == null || entry.at > prev.at) {
        this.dedupSeen.set(key, entry);
      }
    }
  }

  /**
   * Persists one subscriber-dedup sighting to the append-only dedup
   * journal. Returns `true` when the sighting was persisted, `false`
   * when it could not be serialized or the write failed — the caller
   * keeps the in-memory window entry regardless, so a full disk must
   * not fail the delivery path. Never throws.
   *
   * When the journal grows past twice `maxEntriesPerTopic` lines it is
   * compacted down to the latest sighting per (consumer, topic,
   * messageId): a consumer that deduplicates every message must not
   * grow the journal without bound.
   */
  appendDedup(entry: DedupEntry): boolean {
    let line: string;
    try {
      line = `${JSON.stringify(dedupLineOf(entry))}\n`;
    } catch {
      return false;
    }
    try {
      appendFileSync(this.dedupFile, line, 'utf8');
    } catch {
      return false;
    }
    const key = dedupKey(entry.consumer, entry.topic, entry.messageId);
    const prev = this.dedupSeen.get(key);
    if (prev == null || entry.at > prev.at) {
      this.dedupSeen.set(key, entry);
    }
    this.dedupEntries += 1;
    if (this.dedupEntries > this.maxEntriesPerTopic * 2) {
      this.compactDedup();
    }
    return true;
  }

  private compactDedup(): void {
    const text = [...this.dedupSeen.values()]
      .map((e) => `${JSON.stringify(dedupLineOf(e))}\n`)
      .join('');
    try {
      writeFileSync(this.dedupFile, text, 'utf8');
    } catch {
      return;
    }
    this.dedupEntries = this.dedupSeen.size;
  }

  /**
   * Dedup sightings recovered for one consumer — the persisted side of
   * its `deduplicateMessages` window. The caller (the bus, at subscribe
   * time) prunes entries older than the subscriber's `windowMs`: an
   * aged-out identity is "unknown" and must not suppress a re-arrival.
   */
  dedupWindowFor(consumer: string): DedupEntry[] {
    const out: DedupEntry[] = [];
    for (const entry of this.dedupSeen.values()) {
      if (entry.consumer === consumer) out.push(entry);
    }
    return out;
  }

  /** Lines currently in the dedup journal, 0 when no sighting was ever journaled. */
  dedupEntryCount(): number {
    return this.dedupEntries;
  }

  private recoverPublishDedup(): void {
    let text: string;
    try {
      text = readFileSync(this.publishDedupFile, 'utf8');
    } catch {
      return;
    }
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      this.publishDedupEntries += 1;
      // EB-80: the publish-dedup journal is sealed at rest when an
      // encryptor is configured — opened transparently here.
      const inner = this.openLine(line);
      if (inner == null) {
        this.corruptLines += 1;
        continue;
      }
      const entry = parsePublishDedupLine(inner);
      if (entry == null) {
        this.corruptLines += 1;
        continue;
      }
      const key = publishDedupKey(entry.topic, entry.messageId);
      const prev = this.publishDedupSeen.get(key);
      // A re-claimed identity restarts its window: the fold keeps the
      // latest claim and refreshes its position, exactly like the bus's
      // in-memory delete+set — so the fold's order tracks last-claim
      // order, which the bounded compaction relies on.
      if (prev == null || entry.at >= prev.at) {
        this.publishDedupSeen.delete(key);
        this.publishDedupSeen.set(key, entry);
      }
    }
  }

  /**
   * Persists one publish-side idempotent-dedup claim to the append-only
   * publish-dedup journal. Returns `true` when the claim was persisted,
   * `false` when it could not be serialized or the write failed — the
   * caller keeps the in-memory window entry regardless, so a full disk
   * must not fail the publish path. Never throws.
   *
   * When the journal grows past twice `publishDedupMaxEntries` lines it is
   * compacted down to the newest `publishDedupMaxEntries` claims: an
   * idempotent-heavy producer must not grow the journal without bound.
   */
  appendPublishDedup(entry: PublishDedupEntry): boolean {
    let line: string;
    try {
      // EB-80: sealed at rest when an encryptor is configured.
      line = this.sealLine(`${JSON.stringify(publishDedupLineOf(entry))}\n`);
    } catch {
      return false;
    }
    try {
      appendFileSync(this.publishDedupFile, line, 'utf8');
    } catch {
      return false;
    }
    const key = publishDedupKey(entry.topic, entry.messageId);
    const prev = this.publishDedupSeen.get(key);
    // Same fold as recovery: latest claim wins, position refreshed.
    if (prev == null || entry.at >= prev.at) {
      this.publishDedupSeen.delete(key);
      this.publishDedupSeen.set(key, entry);
    }
    this.publishDedupEntries += 1;
    if (this.publishDedupEntries > this.publishDedupMaxEntries * 2) {
      this.compactPublishDedup();
    }
    return true;
  }

  private compactPublishDedup(): void {
    // Bounded like the in-memory window it persists (EB-66): keep the
    // most recently claimed `publishDedupMaxEntries` identities, in claim
    // order — the exact analog of the bus's insertion-ordered eviction,
    // which also evicts by claim order rather than by timestamp (a
    // timestamp sort would misorder same-tick claims).
    const kept = [...this.publishDedupSeen.values()].slice(-this.publishDedupMaxEntries);
    const text = kept.map((e) => this.sealLine(`${JSON.stringify(publishDedupLineOf(e))}\n`)).join('');
    try {
      writeFileSync(this.publishDedupFile, text, 'utf8');
    } catch {
      return;
    }
    this.publishDedupSeen.clear();
    for (const e of kept) this.publishDedupSeen.set(publishDedupKey(e.topic, e.messageId), e);
    this.publishDedupEntries = kept.length;
  }

  /**
   * Every publish-dedup claim recovered from the journal, folded to the
   * latest claim per (topic, messageId). The caller (the bus, at
   * construction) prunes claims that aged out of the idempotency window:
   * an expired identity is "unknown" and must not suppress a re-arrival.
   */
  recoveredPublishDedup(): PublishDedupEntry[] {
    return [...this.publishDedupSeen.values()];
  }

  /** Lines currently in the publish-dedup journal, 0 when no claim was ever journaled. */
  publishDedupEntryCount(): number {
    return this.publishDedupEntries;
  }

  /** Highest seq logged for a topic, 0 when the topic is unknown. */
  lastSeq(topic: string): number {
    return this.lastSeqs.get(topic) ?? 0;
  }

  /**
   * Highest per-key sequence number observed per key, recovered from disk
   * and maintained on append. The bus seeds its key cursors from this so
   * per-key publish-order numbering continues across restarts instead of
   * restarting at 1 (which would collide with replayed keySeqs).
   */
  recoveredKeySeqs(): Map<string, number> {
    return new Map(this.maxKeySeqs);
  }

  /**
   * Highest sequence number observed per partition for a topic (EB-90),
   * recovered from disk and maintained on append. The bus seeds its
   * per-(topic, partition) sequence counters from this so numbering
   * continues across restarts instead of restarting at 1 (which would
   * collide with replayed seqs). Empty when the topic is unknown or when
   * none of its records carried partition data (pre-partitioning logs —
   * the bus falls back to seeding partition 0 from `lastSeq`).
   */
  recoveredPartitionSeqs(topic: string): Map<number, number> {
    return new Map(this.maxPartitionSeqs.get(topic));
  }

  /** Records the highest keySeq seen for a key; ignores keyless records. */
  private noteKeySeq(key: string | undefined, keySeq: number | undefined): void {
    if (key === undefined || keySeq === undefined) return;
    if (keySeq > (this.maxKeySeqs.get(key) ?? 0)) this.maxKeySeqs.set(key, keySeq);
  }

  /**
   * Records the highest seq seen for a (topic, partition) pair; ignores
   * seq-0 schedule records (they carry no sequence number). A record
   * without a partition counts toward partition 0.
   */
  private notePartitionSeq(topic: string, partition: number | undefined, seq: number): void {
    if (seq < 1) return;
    const p = partition ?? 0;
    let perTopic = this.maxPartitionSeqs.get(topic);
    if (perTopic === undefined) {
      perTopic = new Map();
      this.maxPartitionSeqs.set(topic, perTopic);
    }
    if (seq > (perTopic.get(p) ?? 0)) perTopic.set(p, seq);
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
  stats(): {
    topics: number;
    entries: number;
    corruptLines: number;
    keyCompaction: boolean;
    /** Lines in the group-offset journal (`__group_offsets.jsonl`). */
    offsetEntries: number;
    /** Lines in the subscriber-dedup journal (`__dedup.jsonl`). */
    dedupEntries: number;
    /** Lines in the publish-dedup journal (`__publish_dedup.jsonl`). */
    publishDedupEntries: number;
    /** Whether at-rest encryption is enabled (EB-80). */
    encrypted: boolean;
    /**
     * Records evicted by time-based retention (`maxAgeMs`) across
     * compactions (EB-85). 0 when no age bound is configured.
     */
    retentionEvicted: number;
    /** Archive files that failed to decompress during archived reads. */
    corruptArchives: number;
    /** Total entries across all archived segments. */
    archived: number;
    /**
     * Per-topic segment view: live entries in the current segment,
     * closed archived segment count, and archived entries — in
     * first-seen order.
     */
    segments: Record<string, { live: number; segments: number; archived: number }>;
  } {
    let entries = 0;
    for (const count of this.entryCounts.values()) entries += count;
    const segments: Record<string, { live: number; segments: number; archived: number }> = {};
    for (const topic of this.topicsSeen) {
      segments[topic] = {
        live: this.entryCounts.get(topic) ?? 0,
        segments: this.archivesByTopic.get(topic)?.length ?? 0,
        archived: 0,
      };
    }
    let archived = 0;
    for (const entry of this.archives) {
      archived += entry.entries;
      const view = segments[entry.topic];
      if (view !== undefined) view.archived += entry.entries;
    }
    return {
      topics: this.topicsSeen.size,
      entries,
      corruptLines: this.corruptLines,
      keyCompaction: this.keyCompaction,
      offsetEntries: this.offsetEntries,
      dedupEntries: this.dedupEntries,
      publishDedupEntries: this.publishDedupEntries,
      encrypted: this.encryptor !== undefined,
      retentionEvicted: this.retentionEvicted,
      corruptArchives: this.corruptArchives,
      archived,
      segments,
    };
  }
}

/**
 * Keyed compaction rewrite: keeps the latest record per key, the newest
 * `maxEntries` keyless messages, and every seq-0 delayed-delivery schedule
 * record / tombstone (timer intents are never compacted away — dropping a
 * pending schedule would lose its timer on restart). Keyed survivors are
 * not capped by `maxEntries`: like Kafka, the key space itself bounds the
 * retained set. Relative order of the survivors is preserved.
 */
function compactKeyed(records: DurableLogRecord[], maxEntries: number): DurableLogRecord[] {
  const keep = new Set<DurableLogRecord>();
  const seenKeys = new Set<string>();
  let keylessKept = 0;
  // Newest-first: the first record seen for a key is its survivor.
  for (const rec of [...records].reverse()) {
    if (rec.seq === 0) {
      keep.add(rec);
      continue;
    }
    if (rec.key !== undefined) {
      if (seenKeys.has(rec.key)) continue;
      seenKeys.add(rec.key);
      keep.add(rec);
      continue;
    }
    if (keylessKept < maxEntries) {
      keylessKept += 1;
      keep.add(rec);
    }
  }
  return records.filter((rec) => keep.has(rec));
}

/** Builds the on-disk envelope for a record. */
/**
 * Encodes a binary record's wire payload as base64 (EB-92): raw bytes
 * pass through untouched (zero-copy view, no JSON detour); a compressed
 * message's payload is the compression envelope, whose canonical JSON
 * UTF-8 form is the wire representation — replay parses the envelope
 * first, then inflates, exactly like the live path.
 */
function base64OfWireBytes(payload: unknown): string {
  if (payload instanceof Uint8Array) {
    return Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength).toString('base64');
  }
  return Buffer.from(JSON.stringify(payload) ?? 'null', 'utf8').toString('base64');
}

function logLineOf(record: DurableLogRecord): LogLine {
  const binary = record.binary === true;
  const line: LogLine = binary
    ? {
        v: 2,
        seq: record.seq,
        topic: record.topic,
        at: record.at,
        payload_b64: base64OfWireBytes(record.payload),
      }
    : { v: 1, seq: record.seq, topic: record.topic, at: record.at, payload: record.payload };
  if (record.expiresAt !== undefined) line.expiresAt = record.expiresAt;
  if (record.deliverAt !== undefined) line.deliverAt = record.deliverAt;
  if (record.delayId !== undefined) line.delayId = record.delayId;
  if (record.cancelled !== undefined) line.cancelled = record.cancelled;
  if (record.retained !== undefined) line.retained = record.retained;
  if (record.retainedSeq !== undefined) line.retainedSeq = record.retainedSeq;
  if (record.paused !== undefined) line.paused = record.paused;
  if (record.pauseId !== undefined) line.pauseId = record.pauseId;
  if (record.pauseOrder !== undefined) line.pauseOrder = record.pauseOrder;
  if (record.schemaVersion !== undefined) line.schemaVersion = record.schemaVersion;
  if (record.key !== undefined) line.key = record.key;
  if (record.keySeq !== undefined) line.keySeq = record.keySeq;
  if (record.partition !== undefined) line.partition = record.partition;
  if (record.dictId !== undefined) line.dictId = record.dictId;
  if (record.messageId !== undefined) line.messageId = record.messageId;
  if (record.eventTime !== undefined) line.eventTime = record.eventTime;
  if (record.causal !== undefined) line.causal = { source: record.causal.source, clock: record.causal.clock };
  if (record.publisherId !== undefined) line.publisherId = record.publisherId;
  if (record.signature !== undefined)
    line.signature = { kid: record.signature.kid, sig: record.signature.sig };
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
 *
 * `seq` 0 is also reserved for MQTT-style retained-message records
 * (`retained: true`, EB-87): a retained set record carries the retained
 * message's original per-topic sequence number in `retainedSeq` (a
 * positive integer — required, since recovery rebuilds the table with
 * it); a retained tombstone (`cancelled: true`) carries no `retainedSeq`
 * and closes the retained state. `retained` is only valid with `seq` 0,
 * and a retained record never carries a `delayId` — the two seq-0
 * families are disjoint by construction.
 *
 * `seq` 0 is also reserved for pause-buffer records (`paused: true`,
 * EB-88): a buffered set record carries the buffered message's identity
 * in `pauseId` (a non-empty string) and its global FIFO ordinal in
 * `pauseOrder` (a positive integer — the bus always writes it); a pause
 * tombstone (`cancelled: true`) carries `pauseId` and closes the buffered
 * message without fanning it out. `paused` is only valid with `seq` 0,
 * and a pause record never carries `delayId` or `retained` — the three
 * seq-0 families are disjoint by construction.
 *
 * A `key`, when present, must be a non-empty string; anything else makes
 * the line corrupt. A `keySeq`, when present, must be a positive integer
 * and requires `key` — the bus always writes both together.
 */
/**
 * Strict base64 shape check for v2 `payload_b64` lines: canonical
 * alphabet with correct padding. `Buffer.from(x, 'base64')` is lenient
 * (it silently skips bad characters), so the shape is validated first —
 * a tampered line must read as corrupt, not as truncated bytes.
 */
const BASE64_PATTERN = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

function parseLogLine(line: string, expectedTopic: string): DurableLogRecord | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const o = parsed as Record<string, unknown>;
  const version = o['v'];
  if (version !== 1 && version !== 2) return null;
  if (typeof o['topic'] !== 'string' || o['topic'] !== expectedTopic) return null;
  const seq = o['seq'];
  const isScheduleRecord = seq === 0;
  const isRetainedRecord = o['retained'] === true;
  const isPauseRecord = o['paused'] === true;
  if (isRetainedRecord && !isScheduleRecord) return null;
  if (isPauseRecord && !isScheduleRecord) return null;
  if (isRetainedRecord && isPauseRecord) return null;
  if (isScheduleRecord) {
    if (isRetainedRecord) {
      // Retained set records carry the retained message's original seq;
      // retained tombstones carry no message identity.
      if (o['cancelled'] === true) {
        if (o['retainedSeq'] !== undefined) return null;
      } else if (!Number.isInteger(o['retainedSeq']) || (o['retainedSeq'] as number) < 1) {
        return null;
      }
    } else if (isPauseRecord) {
      // Pause-buffer set records carry the buffered message's identity
      // and FIFO ordinal; pause tombstones carry only the identity.
      if (typeof o['pauseId'] !== 'string' || (o['pauseId'] as string).length === 0) return null;
      if (o['cancelled'] !== true) {
        if (!Number.isInteger(o['pauseOrder']) || (o['pauseOrder'] as number) < 1) return null;
      }
    } else if (typeof o['delayId'] !== 'string' || (o['delayId'] as string).length === 0) {
      return null;
    }
  } else if (!Number.isInteger(seq) || (seq as number) < 1) {
    return null;
  }
  if (typeof o['at'] !== 'number' || !Number.isFinite(o['at'])) return null;
  let payload: unknown;
  let binary = false;
  if (version === 2) {
    // Binary record (EB-92): the wire bytes ride as base64 with an
    // explicit version marker — a JSON reader never mistakes them for a
    // JSON value. Malformed base64 is corruption, not a payload.
    const b64 = o['payload_b64'];
    if (typeof b64 !== 'string' || o['payload'] !== undefined || !BASE64_PATTERN.test(b64)) {
      return null;
    }
    payload = new Uint8Array(Buffer.from(b64, 'base64'));
    binary = true;
  } else {
    payload = o['payload'];
  }
  const record: DurableLogRecord = {
    seq: seq as number,
    topic: o['topic'] as string,
    at: o['at'] as number,
    payload,
    ...(binary ? { binary: true } : {}),
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
  if (isRetainedRecord) {
    record.retained = true;
    // Validated above: present exactly on non-tombstone retained records.
    if (o['retainedSeq'] !== undefined) record.retainedSeq = o['retainedSeq'] as number;
  } else if (o['retained'] !== undefined) {
    // The bus only ever writes `retained: true`; any other value means
    // the line was tampered with or miswritten.
    return null;
  }
  if (isPauseRecord) {
    record.paused = true;
    // Validated above: `pauseId` always present, `pauseOrder` present on
    // non-tombstone records.
    record.pauseId = o['pauseId'] as string;
    if (o['pauseOrder'] !== undefined) record.pauseOrder = o['pauseOrder'] as number;
  } else {
    // The bus only ever writes `paused: true`; any other value — or a
    // stray `pauseId`/`pauseOrder` on a non-pause record — means the line
    // was tampered with or miswritten.
    if (o['paused'] !== undefined || o['pauseId'] !== undefined || o['pauseOrder'] !== undefined) {
      return null;
    }
  }
  if (o['schemaVersion'] !== undefined) {
    if (!Number.isInteger(o['schemaVersion']) || (o['schemaVersion'] as number) < 1) return null;
    record.schemaVersion = o['schemaVersion'] as number;
  }
  if (o['key'] !== undefined) {
    if (typeof o['key'] !== 'string' || (o['key'] as string).length === 0) return null;
    record.key = o['key'] as string;
  }
  if (o['keySeq'] !== undefined) {
    // A per-key sequence number is meaningless without its key: the bus
    // always writes both together, so a lone keySeq is a corrupt line,
    // never a keyed message.
    if (!Number.isInteger(o['keySeq']) || (o['keySeq'] as number) < 1 || record.key === undefined) {
      return null;
    }
    record.keySeq = o['keySeq'] as number;
  }
  if (o['partition'] !== undefined) {
    // The partition is routing metadata, not ordering metadata on its
    // own — but a malformed value still makes the line corrupt: the bus
    // only ever writes non-negative integers, so a bad one means the line
    // was tampered with or miswritten.
    if (!Number.isInteger(o['partition']) || (o['partition'] as number) < 0) return null;
    record.partition = o['partition'] as number;
  }
  if (o['dictId'] !== undefined) {
    // A preset-dictionary id is the SHA-256 hex of the dictionary bytes:
    // 64 lowercase hex chars. Anything else is a corrupt line, not a
    // dictionary — replay must never resolve a forged id.
    if (typeof o['dictId'] !== 'string' || !/^[0-9a-f]{64}$/.test(o['dictId'] as string)) return null;
    record.dictId = o['dictId'] as string;
  }
  if (o['messageId'] !== undefined) {
    // The identity is advisory dedup metadata, not structural: a
    // malformed value is dropped (the message replays without an
    // identity) rather than failing the whole line.
    if (typeof o['messageId'] === 'string' && (o['messageId'] as string).length > 0) {
      record.messageId = o['messageId'] as string;
    }
  }
  if (o['eventTime'] !== undefined) {
    // The event time is advisory business metadata, not structural: a
    // malformed value is dropped (the message replays without an event
    // time) rather than failing the whole line.
    const eventTime = o['eventTime'] as number;
    if (typeof eventTime === 'number' && Number.isFinite(eventTime) && eventTime >= 0) {
      record.eventTime = eventTime;
    }
  }
  if (o['causal'] !== undefined) {
    // The causal clock is structural ordering metadata: a malformed
    // value makes the line corrupt — replay must never invent or drop a
    // happens-before edge.
    const causal = o['causal'] as Record<string, unknown>;
    if (
      typeof causal !== 'object' ||
      causal === null ||
      typeof causal['source'] !== 'string' ||
      (causal['source'] as string).length === 0 ||
      !Number.isInteger(causal['clock']) ||
      (causal['clock'] as number) < 0
    ) {
      return null;
    }
    record.causal = { source: causal['source'] as string, clock: causal['clock'] as number };
  }
  if (o['publisherId'] !== undefined) {
    // The publisher identity is an attribution label, not ordering
    // metadata — but a malformed value still makes the line corrupt: the
    // bus only ever writes valid identities, so a bad one means the line
    // was tampered with or miswritten.
    if (typeof o['publisherId'] !== 'string' || (o['publisherId'] as string).length === 0) {
      return null;
    }
    record.publisherId = o['publisherId'] as string;
  }
  if (o['signature'] !== undefined) {
    // The signature is integrity metadata: a malformed value means the
    // line was tampered with or miswritten — the bus only ever writes
    // well-formed signatures — so the line is corrupt, never silently
    // unsigned.
    const signature = o['signature'] as Record<string, unknown>;
    if (
      typeof signature !== 'object' ||
      signature === null ||
      typeof signature['kid'] !== 'string' ||
      (signature['kid'] as string).length === 0 ||
      typeof signature['sig'] !== 'string' ||
      !/^[0-9a-f]{64}$/.test(signature['sig'] as string)
    ) {
      return null;
    }
    record.signature = {
      kid: signature['kid'] as string,
      sig: signature['sig'] as string,
    };
  }
  // A non-tombstone delayed-delivery schedule record must say when it is
  // due. Retained-message records (EB-87) and pause-buffer records
  // (EB-88) are the other seq-0 families and carry no due time — only
  // their own identity fields, validated above.
  if (
    isScheduleRecord &&
    !isRetainedRecord &&
    !isPauseRecord &&
    record.cancelled !== true &&
    record.deliverAt === undefined
  ) {
    return null;
  }
  return record;
}
