import { gunzipSync, gzipSync } from 'node:zlib';
import { createHash } from 'node:crypto';
import { Buffer } from 'node:buffer';
import type { MessageLane } from './lanes.ts';
import {
  FilesystemStorageBackend,
  type DurableStorageBackend,
} from './storage.ts';
import type { RemoteSegmentS3Client } from './storage-s3.ts';

/**
 * EB-96: reserved storage-key prefix for namespace child logs. The bus
 * opens one child `DurableTopicLog` per namespace in a scoped backend
 * view under this prefix, so a root log's own enumeration (recovery,
 * archive scans) skips these keys — they belong to the child logs, and
 * a child log's keys are relative to its scope so it never sees them.
 * A concrete topic can never produce such a key: topic names are
 * URL-encoded into the key, so a literal `/` in a topic becomes `%2F`.
 */
export const NAMESPACE_LOG_KEY_PREFIX = 'namespaces/';
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
   * Application message headers (see `PublishOptions.headers`, EB-153),
   * present when the publish carried them. Replay restores them onto the
   * envelope so a resumed subscriber's header filter judges the same
   * headers the live fan-out saw. Also carried on seq-0 retained-message
   * set records so a restart rebuilds the retained table with the
   * routing metadata intact.
   */
  headers?: Record<string, string>;
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
   * Per-message TTL duration in milliseconds (EB-149): present on a
   * delayed-delivery schedule record created with
   * `PublishDelayedOptions.ttlMs`. The schedule carries the duration, not
   * a deadline — the due-time fan-out stamps `fanOutTime + ttlMs`, so the
   * message's life starts at delivery, not at schedule time. Never
   * present on delivery records: those carry the stamped `expiresAt`.
   */
  ttlMs?: number;
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
   * EB-124: recurring-job family marker — `true` on a seq-0 record means
   * this line is recurring-job state, not a message and not a
   * delayed-delivery schedule. A schedule record (`recurringJobId` +
   * `cronExpr`) is written when `EventBus.scheduleRecurring` registers a
   * job; a trigger record (`recurringTrigger: true`) is written each time
   * the job fires (audit trail, never re-armed); a tombstone (`cancelled:
   * true`) closes the job. Like the other seq-0 families these records
   * are invisible to `readSince` replay and never move the per-topic
   * sequence counter. A recurring record never carries `delayId`,
   * `retained`, or `paused` — the four seq-0 families are disjoint by
   * construction.
   */
  recurring?: boolean;
  /**
   * EB-124: identifies one recurring job (`recurring-1`, …). Present on
   * the seq-0 schedule record, on every trigger record, and on the
   * tombstone — so restart recovery can tell a cancelled job (never
   * revived) from one whose schedule record has no tombstone.
   */
  recurringJobId?: string;
  /**
   * EB-124: the cron expression the job was scheduled with. Present on
   * the seq-0 schedule record (restart recovery recomputes the next
   * trigger from it) and on trigger records (audit). The payload factory
   * is in-process state and is never persisted — a restarted bus cannot
   * rebuild a live timer from this record (see `EventBus.scheduleRecurring`).
   */
  cronExpr?: string;
  /**
   * EB-124: trigger audit marker — `true` on a seq-0 recurring record
   * means the job with this `recurringJobId` fired once at `at`. Trigger
   * records never close the job and never re-arm anything; they exist so
   * the log shows the job's fire history next to its schedule record.
   */
  recurringTrigger?: boolean;
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
   * Also the tombstone marker for a recurring job (see `recurring`):
   * `true` on a seq-0 recurring record means the job with this
   * `recurringJobId` was cancelled and must not be rebuilt on restart.
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
   * Priority lane (see `PublishOptions.priority`, EB-122): `'urgent'` or
   * `'bulk'` when the publish carried a non-normal lane — absent means
   * `normal`, including on records written before priority lanes existed.
   * Replay restores it onto the envelope so a resumed subscriber keeps
   * the message's lane; delayed-delivery schedule records and
   * pause-buffer records carry it so restart recovery rebuilds their
   * timers/buffers with the lane intact.
   */
  lane?: MessageLane;
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
   * Key-TTL tombstone marker (EB-221): present exactly on seq-0 records
   * compaction leaves behind for a key it evicted — the key's seq
   * baseline (`keySeq` of its latest evicted record) so restart
   * recovery never renumbers the key. A tombstone carries no payload
   * and no timer intent: it is invisible to `readSince` replay (seq 0)
   * and to every seq-0 family recovery (it carries no `delayId`,
   * `retained`, `paused`, or `recurring` marker), and it survives
   * future compactions like every other seq-0 record.
   */
  keyTtlEvicted?: boolean;
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
   * Publish-time publisher-identity HMAC signature (see
   * `EventBus.setPublisherSigningKey`, EB-97): `{ kid, sig }` covering
   * (topic, seq, publisherId, wire bytes). Persisted so replay restores
   * it onto the envelope and a `verifyPublisher` subscriber checks a
   * replayed message against the same bytes the publisher signed.
   * Never a secret — only the key id and the digest.
   */
  publisherSignature?: { kid: string; sig: string };
  /**
   * EB-227: the topic this record was split from, present exactly on
   * records written by [`DurableTopicLog.splitTopics`]. A split copy
   * is a new record in a new topic, so it carries its provenance
   * explicitly: a reader of either split target can tell which
   * source topic the record came from without consulting the audit
   * log. Advisory metadata like `messageId` — a malformed value on
   * disk is dropped on parse rather than failing the line.
   */
  splitSource?: string;
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
   * EB-119: opt-in per-topic byte budget for the live segment — the count
   * bound (`maxEntriesPerTopic`) counts records, this one counts bytes.
   * When a topic's live bytes grow past twice this value the topic
   * compacts, keeping the newest records whose sealed on-disk bytes fit
   * the budget (dual constraints with the count and age bounds, same
   * amortized semantics as `maxAgeMs`: enforced at compact time only).
   * With segment rotation on, crossing the budget archives the live
   * segment instead of evicting it — the oldest data lands in a
   * `.jsonl.gz` segment, still replayable via `readSince`/`readSinceTime`
   * with `{ includeArchived: true }`. Byte cost is the actual on-disk
   * line bytes, so compressed records count their compressed wire bytes
   * and binary records their base64 line bytes. Seq-0 records
   * (delayed-delivery schedules, retained snapshots, pause buffers) are
   * never evicted by the byte bound — the same rationale as `maxAgeMs` —
   * and under keyed compaction the latest record per key is exempt too
   * (the key space itself bounds retention, Kafka-style). Must be a
   * positive finite number of bytes. Absent by default (no byte bound).
   */
  maxBytesPerTopic?: number;
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
  /**
   * EB-94: hook fired when an archived segment fails its checksum
   * verification (or its bytes cannot be read for verification). The
   * segment is isolated — skipped, never fatal — and counted in
   * `stats().corruptSegments`. The bus wires this to the subscription
   * audit log (`segment_checksum_failed`).
   */
  onSegmentChecksumFailed?: (info: SegmentChecksumFailure) => void;
  /**
   * EB-96: pluggable storage backend for every object the log keeps —
   * the per-topic live log (EB-17), archived segments (EB-61), the
   * group-offset journal (EB-32), the subscriber-dedup journal, and the
   * publish-dedup journal (EB-66). Keys are the former file basenames
   * (`<urlencoded-topic>.log`, `__group_offsets.jsonl`, …); recovery
   * enumerates them with `backend.list()`. Defaults to a
   * `FilesystemStorageBackend` rooted at `dir`, which preserves the
   * exact on-disk layout the log always used. Encryption (EB-80) and
   * per-segment checksums (EB-94) are computed above this layer and are
   * unaffected by which backend is in use.
   */
  backend?: DurableStorageBackend;
  /**
   * EB-163: opt-in remote offload of sealed (archived) segments to an
   * S3-compatible object store. When set, every archived segment is
   * uploaded under a deterministic object key after the archive pass
   * lands it, a remote manifest records each segment's SHA-256, and the
   * log keeps only the newest `retainLocalSegments` local copies per
   * topic (default: all of them — current semantics). Reads of a
   * locally-trimmed segment (`readSince`/`readSinceTime` with
   * `{ includeArchived: true }`) pull the bytes back down and verify
   * them against the manifest hash; a segment missing locally AND
   * remotely throws instead of silently truncating the replay.
   * Requires `segmentRotation` — only sealed segments are offloaded.
   */
  segmentOffload?: SegmentOffloadOptions;
}

/**
 * EB-163: remote offload of sealed (archived) topic segments to an
 * S3-compatible object store.
 */
export interface SegmentOffloadOptions {
  /**
   * Minimal synchronous S3 object client (see `RemoteSegmentS3Client` in
   * `src/storage-s3.ts`): `SigV4S3Client` for real uploads,
   * `DryRunS3Client` for rehearsals, or a custom implementation (a
   * memory fake in tests). Required.
   */
  s3: RemoteSegmentS3Client;
  /**
   * Remote key prefix, e.g. `bus-durable-log`. The deterministic object
   * key for a segment is
   * `<keyPrefix>/[<namespace>/]<urlencoded-topic>.seg-<seqStart>-<seqEnd>.jsonl.gz`
   * and the manifest lives at `<keyPrefix>/[<namespace>/]manifest.json`.
   * Default `'durable-log-offload'`.
   */
  keyPrefix?: string;
  /**
   * Namespace component of the remote key. The log itself has no
   * namespace concept — a namespace child log is a scoped backend view
   * the log cannot introspect — so a namespace's operator passes its
   * prefix here explicitly and each namespace offloads under its own
   * key space. Default `''` (the root log's key space).
   */
  namespace?: string;
  /**
   * How many of the newest archived segments to keep locally per topic.
   * Segments beyond the newest N are deleted locally (the `.gz` and its
   * checksum sidecar) only after their bytes are confirmed on the
   * remote; the archive index and chain manifest keep their lines, so
   * the segment stays known and replayable from the remote. Default
   * `Infinity` — current semantics: every archived segment stays local
   * and the remote is a pure replica. Must be a non-negative integer or
   * `Infinity`.
   *
   * `purgeTopic` does not delete remote objects: destroying a topic's
   * remote history is the operator deleting the topic's remote key
   * prefix.
   */
  retainLocalSegments?: number;
  /**
   * EB-218: bounds for the local readback cache. A replay that needs a
   * locally-trimmed segment (`readSince`/`readSinceTime` with
   * `{ includeArchived: true }`) pulls the segment from the remote —
   * sealed segments are immutable and hash-verified against the
   * manifest, so a fetched copy is always current and is cached here,
   * keyed by (topic, segmentId), least-recently-used first. Repeated
   * replays over the same range (multiple subscribers resuming from old
   * offsets) then hit memory instead of re-downloading synchronously.
   *
   * `readbackCacheMaxSegments` caps how many segments the cache holds
   * (default 16), `readbackCacheMaxBytes` caps the total downloaded
   * byte size it holds (default 64 MiB — accounted against the
   * downloaded bytes of each segment, a proxy for the cache's memory
   * cost). Either bound may be 0 to disable the cache entirely:
   * every readback then downloads from the remote. Must be
   * non-negative integers.
   */
  readbackCacheMaxSegments?: number;
  /**
   * EB-218: total downloaded-byte budget for the local readback cache.
   * See `readbackCacheMaxSegments`.
   */
  readbackCacheMaxBytes?: number;
}

/**
 * EB-163: one line of the remote offload manifest
 * (`<keyPrefix>/[<namespace>/]manifest.json`) — the remote source of
 * truth for which segments landed where and what they hash to.
 */
export interface RemoteOffloadManifestEntry {
  /** Manifest line format version. */
  v: 1;
  /** Concrete topic the segment belongs to. */
  topic: string;
  /** Segment identity: `<seqStart>-<seqEnd>` of the segment's records. */
  segmentId: string;
  /** Full remote object key of the segment's `.jsonl.gz`. */
  key: string;
  /** First record's `seq` in the segment. */
  seqStart: number;
  /** Last record's `seq` in the segment. */
  seqEnd: number;
  /** SHA-256 hex of the uploaded `.jsonl.gz` bytes. */
  sha256: string;
  /** Records in the segment, including seq-0 schedule records. */
  entries: number;
  /** Gzipped archive size in bytes. */
  bytes: number;
}

/**
 * Parses one remote-manifest entry. Returns `null` for anything
 * malformed — wrong version, an empty topic/segmentId/key, negative
 * seq bounds, a non-digest `sha256`, a non-positive-integer entry
 * count, or a negative byte count. A corrupt remote manifest fails the
 * offload pass closed (never overwritten blindly).
 */
function parseRemoteManifestEntry(o: unknown): RemoteOffloadManifestEntry | null {
  if (typeof o !== 'object' || o === null) return null;
  const e = o as Record<string, unknown>;
  if (e['v'] !== 1) return null;
  for (const field of ['topic', 'segmentId', 'key'] as const) {
    if (typeof e[field] !== 'string' || (e[field] as string).length === 0) return null;
  }
  for (const field of ['seqStart', 'seqEnd'] as const) {
    if (!Number.isInteger(e[field]) || (e[field] as number) < 0) return null;
  }
  if (typeof e['sha256'] !== 'string' || !SHA256_HEX_PATTERN.test(e['sha256'] as string)) return null;
  if (!Number.isInteger(e['entries']) || (e['entries'] as number) < 1) return null;
  if (!Number.isInteger(e['bytes']) || (e['bytes'] as number) < 0) return null;
  return {
    v: 1,
    topic: e['topic'] as string,
    segmentId: e['segmentId'] as string,
    key: e['key'] as string,
    seqStart: e['seqStart'] as number,
    seqEnd: e['seqEnd'] as number,
    sha256: e['sha256'] as string,
    entries: e['entries'] as number,
    bytes: e['bytes'] as number,
  };
}

/** EB-163: validated `SegmentOffloadOptions` with defaults applied. */
interface ResolvedSegmentOffload {
  s3: RemoteSegmentS3Client;
  keyPrefix: string;
  namespace: string;
  retainLocalSegments: number;
  /** EB-218: max cached segments for the local readback cache. */
  readbackCacheMaxSegments: number;
  /** EB-218: downloaded-byte budget for the local readback cache. */
  readbackCacheMaxBytes: number;
}

/**
 * Validates `SegmentOffloadOptions`, returning the resolved
 * configuration. `undefined` stays `undefined` (offload disabled —
 * fully backward compatible). Anything else must carry a usable S3
 * client and, since only sealed segments are offloaded, the log must
 * have `segmentRotation` enabled — else `RangeError`.
 */
function resolveSegmentOffloadOptions(
  options: SegmentOffloadOptions | undefined,
  segmentRotation: SegmentRotationOptions | undefined,
): ResolvedSegmentOffload | undefined {
  if (options === undefined) return undefined;
  if (typeof options !== 'object' || options === null) {
    throw new RangeError('segmentOffload must be an object');
  }
  const { s3, keyPrefix, namespace, retainLocalSegments } = options;
  const methods = ['putObject', 'getObject', 'headObject', 'listObjects', 'deleteObject'] as const;
  if (
    typeof s3 !== 'object' ||
    s3 === null ||
    !methods.every((m) => typeof (s3 as Record<string, unknown>)[m] === 'function')
  ) {
    throw new RangeError(
      'segmentOffload.s3 must be a RemoteSegmentS3Client (putObject/getObject/headObject/listObjects/deleteObject)',
    );
  }
  if (segmentRotation === undefined) {
    throw new RangeError(
      'segmentOffload requires segmentRotation: only sealed (archived) segments are offloaded',
    );
  }
  const resolvedPrefix = keyPrefix ?? 'durable-log-offload';
  if (typeof resolvedPrefix !== 'string' || resolvedPrefix.length === 0) {
    throw new RangeError('segmentOffload.keyPrefix must be a non-empty string');
  }
  const resolvedNamespace = namespace ?? '';
  if (typeof resolvedNamespace !== 'string') {
    throw new RangeError('segmentOffload.namespace must be a string');
  }
  const retain = retainLocalSegments ?? Infinity;
  if (!(retain === Infinity || (Number.isInteger(retain) && retain >= 0))) {
    throw new RangeError('segmentOffload.retainLocalSegments must be a non-negative integer or Infinity');
  }
  const readbackCacheMaxSegments = options.readbackCacheMaxSegments ?? 16;
  if (!Number.isInteger(readbackCacheMaxSegments) || readbackCacheMaxSegments < 0) {
    throw new RangeError('segmentOffload.readbackCacheMaxSegments must be a non-negative integer');
  }
  const readbackCacheMaxBytes = options.readbackCacheMaxBytes ?? 64 * 1024 * 1024;
  if (!Number.isInteger(readbackCacheMaxBytes) || readbackCacheMaxBytes < 0) {
    throw new RangeError('segmentOffload.readbackCacheMaxBytes must be a non-negative integer');
  }
  return {
    s3,
    keyPrefix: resolvedPrefix,
    namespace: resolvedNamespace,
    retainLocalSegments: retain,
    readbackCacheMaxSegments,
    readbackCacheMaxBytes,
  };
}

/**
 * EB-94: one archived segment that failed checksum verification.
 */
export interface SegmentChecksumFailure {
  /** Concrete topic the segment belongs to. */
  topic: string;
  /** Archive file name (`<topic>.seg-<start>-<end>.jsonl.gz`). */
  path: string;
  /** Segment identity (`<seqStart>-<seqEnd>`). */
  segmentId: string;
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
  /**
   * Per-message TTL duration in milliseconds (EB-149): present on
   * delayed-delivery schedule records — see `DurableLogRecord.ttlMs`.
   */
  ttlMs?: number;
  deliverAt?: number;
  delayId?: string;
  cancelled?: boolean;
  /** Recurring-job family marker (see `DurableLogRecord.recurring`). */
  recurring?: boolean;
  /** Recurring-job identity (see `DurableLogRecord.recurringJobId`). */
  recurringJobId?: string;
  /** The job's cron expression (see `DurableLogRecord.cronExpr`). */
  cronExpr?: string;
  /** Recurring trigger audit marker (see `DurableLogRecord.recurringTrigger`). */
  recurringTrigger?: boolean;
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
  /** Key-TTL tombstone marker (see `DurableLogRecord.keyTtlEvicted`). */
  keyTtlEvicted?: boolean;
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
   * Publish-time publisher-identity HMAC signature (see
   * `DurableLogRecord.publisherSignature`): `{ kid, sig }`, present
   * exactly when the publish carried a signed publisher identity.
   */
  publisherSignature?: { kid: string; sig: string };
  /**
   * Split provenance (see `DurableLogRecord.splitSource`, EB-227):
   * the topic a split copy was split from.
   */
  splitSource?: string;
  /**
   * Priority lane (see `DurableLogRecord.lane`, EB-122): `'urgent'` or
   * `'bulk'`, present exactly when the publish carried a non-normal
   * lane. Absent means `normal` — including on records written before
   * priority lanes existed.
   */
  lane?: MessageLane;
  /**
   * Application message headers (see `DurableLogRecord.headers`).
   */
  headers?: Record<string, string>;
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
  /**
   * Static member identity (EB-104) for per-instance checkpoints;
   * absent for group-level commits.
   */
  instanceId?: string;
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
  /** Static member identity (EB-104) for per-instance checkpoints. */
  instance?: string;
}

/**
 * Key for one checkpoint: groupId and topic joined by NUL, with the
 * partition and pattern appended for per-partition commits, and an
 * `instance:<id>` segment for per-static-member commits (EB-104).
 * Neither may contain NUL in practice, so the pairing is unambiguous
 * and reversible (see `recoveredCommittedOffsets`). The `instance:`
 * marker can never collide with a partition key: partition segments are
 * numeric, the marker never is.
 */
function offsetKey(
  groupId: string,
  topic: string,
  partition?: number,
  pattern?: string,
  instanceId?: string,
): string {
  const base = `${groupId}\0${topic}`;
  const instanceSuffix = instanceId === undefined ? '' : `\0instance:${instanceId}`;
  return partition === undefined
    ? `${base}${instanceSuffix}`
    : `${base}\0${partition}\0${pattern ?? ''}${instanceSuffix}`;
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
    ...(commit.instanceId === undefined ? {} : { instance: commit.instanceId }),
  };
}

/**
 * Parses one offset-journal line. Returns `null` for anything malformed —
 * wrong version, empty group/topic, a non-positive-integer seq, a
 * non-finite timestamp, a malformed partition, a partition without its
 * pattern, or a malformed static-member instance identity. Corrupt lines
 * are skipped, never fatal.
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
  // Static-member checkpoint (EB-104): a non-empty NUL-free identity.
  const instance = o['instance'];
  if (instance !== undefined) {
    if (typeof instance !== 'string' || instance.length === 0 || instance.includes('\0')) return null;
  }
  return {
    groupId: o['group'] as string,
    topic: o['topic'] as string,
    seq: seq as number,
    at: o['at'] as number,
    ...(partition === undefined
      ? {}
      : { partition: partition as number, pattern: pattern as string }),
    ...(instance === undefined ? {} : { instanceId: instance as string }),
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
 * EB-118: file name of the segment-chain manifest (`__segment_chain.jsonl`)
 * inside the log directory. A `.jsonl` name — not `.log` — so topic-file
 * recovery never mistakes it for a topic file, and it can never collide
 * with a topic file either (`fileFor` always appends `.log`).
 */
const CHAIN_MANIFEST_NAME = '__segment_chain.jsonl';

/**
 * EB-118: the `prevHash` of the first segment in each topic's chain — a
 * fixed, deterministic marker, never a real digest (real digests are 64
 * lowercase hex chars; this marker is not). The genesis anchor is
 * reproducible by construction: any verifier recomputing the chain starts
 * from this constant.
 */
export const SEGMENT_CHAIN_GENESIS_PREV = 'genesis';

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
 * EB-118: one link of a topic's archived-segment hash chain. Archived
 * segments link in landing order — `prevHash` is the previous segment's
 * `sha256`, the first segment's is `SEGMENT_CHAIN_GENESIS_PREV` — so a
 * reordered, dropped, or swapped segment breaks the chain. This is the
 * inter-segment ordering integrity that per-segment checksums (EB-94)
 * cannot see: EB-94 proves each segment's bytes are intact, the chain
 * proves the sequence of segments is the one that landed.
 */
export interface SegmentChainEntry {
  /** Concrete topic the segment belongs to. */
  topic: string;
  /** Segment identity: `<seqStart>-<seqEnd>` of the segment's records. */
  segmentId: string;
  /** Archive file name (`<topic>.seg-<start>-<end>.jsonl.gz`). */
  path: string;
  /**
   * SHA-256 hex of the archived segment's gzipped bytes — the same byte
   * range the EB-94 sidecar covers. The sidecar file itself is metadata
   * and is NOT hashed: a crash between the `.gz` write and the sidecar
   * write must not break the chain.
   */
  sha256: string;
  /** Previous segment's `sha256` in this topic's chain (`'genesis'` for the first). */
  prevHash: string;
  /** Records in the segment, including seq-0 schedule records. */
  entries: number;
  /** Gzipped archive size in bytes. */
  bytes: number;
}

/** On-disk envelope for one chain-manifest line. `v: 2` pins the format. */
interface ChainManifestLine {
  v: 2;
  topic: string;
  segmentId: string;
  path: string;
  sha256: string;
  prevHash: string;
  entries: number;
  bytes: number;
}

/** Builds the on-disk envelope for a chain entry. */
function chainManifestLineOf(entry: SegmentChainEntry): ChainManifestLine {
  return {
    v: 2,
    topic: entry.topic,
    segmentId: entry.segmentId,
    path: entry.path,
    sha256: entry.sha256,
    prevHash: entry.prevHash,
    entries: entry.entries,
    bytes: entry.bytes,
  };
}

const SHA256_HEX_PATTERN = /^[0-9a-f]{64}$/;

/**
 * Parses one chain-manifest line. Returns `null` for anything malformed —
 * wrong version, an empty topic/segmentId/path, a non-digest `sha256`, an
 * empty `prevHash`, a non-positive-integer entry count, or a negative byte
 * count. Corrupt lines are skipped, never fatal.
 */
function parseChainManifestLine(line: string): SegmentChainEntry | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (typeof parsed !== 'object' || parsed === null) return null;
  const o = parsed as Record<string, unknown>;
  if (o['v'] !== 2) return null;
  for (const field of ['topic', 'segmentId', 'path'] as const) {
    if (typeof o[field] !== 'string' || (o[field] as string).length === 0) return null;
  }
  if (typeof o['sha256'] !== 'string' || !SHA256_HEX_PATTERN.test(o['sha256'] as string)) {
    return null;
  }
  if (typeof o['prevHash'] !== 'string' || (o['prevHash'] as string).length === 0) return null;
  if (!Number.isInteger(o['entries']) || (o['entries'] as number) < 1) return null;
  if (!Number.isInteger(o['bytes']) || (o['bytes'] as number) < 0) return null;
  return {
    topic: o['topic'] as string,
    segmentId: o['segmentId'] as string,
    path: o['path'] as string,
    sha256: o['sha256'] as string,
    prevHash: o['prevHash'] as string,
    entries: o['entries'] as number,
    bytes: o['bytes'] as number,
  };
}

/**
 * EB-118: one problem found by offline chain verification. `kind` is the
 * failure mode:
 * - `link`: a manifest line's `prevHash` does not match the previous
 *   segment's digest — the manifest was reordered or a line was forged;
 * - `hash`: the segment's bytes no longer match its manifest digest — the
 *   segment was replaced or tampered with;
 * - `missing`: the manifest names a segment file that is gone from the
 *   directory;
 * - `rogue`: a segment file on disk has no manifest line — landed outside
 *   the chained archive path;
 * - `unreadable`: the segment file exists but its bytes cannot be read.
 * `topic`/`segmentId` are empty on manifest-level problems (a corrupt
 * manifest line).
 */
export interface SegmentChainIssue {
  /** Concrete topic the segment belongs to. */
  topic: string;
  /** Segment identity (`<seqStart>-<seqEnd>`). */
  segmentId: string;
  /** Archive file name, or the manifest name for manifest-level problems. */
  path: string;
  kind: 'link' | 'hash' | 'missing' | 'rogue' | 'unreadable';
  /** What was expected and what was found — the operator evidence. */
  detail: string;
}

/**
 * EB-118: the offline chain-verification report. `ok` is true only when
 * no issue was found; `segments` counts the manifest lines checked.
 * Verification is O(segments) manifest lines plus one full byte read per
 * archived segment — the I/O cost is proportional to archived data, so
 * this is an offline/operator tool, not a hot-path check.
 */
export interface SegmentChainReport {
  ok: boolean;
  segments: number;
  issues: SegmentChainIssue[];
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
  /**
   * Canonical payload hash (EB-151): present on explicit
   * idempotency-key claims (`key:` messageId prefix) — the SHA-256 of the
   * first claim's canonical payload bytes, for post-restart conflict
   * detection. Absent on messageId and content-hash claims.
   */
  hash?: string;
  /**
   * First publish's accepted-delivery count (EB-151): present on explicit
   * idempotency-key claims — returned to within-window retries after a
   * restart. Absent on messageId and content-hash claims.
   */
  accepted?: number;
}

/** On-disk envelope for one publish-dedup-journal line. `v` pins the format. */
interface PublishDedupLine {
  v: 1;
  topic: string;
  messageId: string;
  at: number;
  hash?: string;
  accepted?: number;
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
    ...(entry.hash === undefined ? {} : { hash: entry.hash }),
    ...(entry.accepted === undefined ? {} : { accepted: entry.accepted }),
  };
}

/**
 * Parses one publish-dedup-journal line. Returns `null` for anything
 * malformed — wrong version, an empty topic/messageId, a non-finite
 * timestamp, or a non-string `hash` / non-finite `accepted` when those
 * optional fields are present. Corrupt lines are skipped, never fatal.
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
  if (o['hash'] !== undefined && typeof o['hash'] !== 'string') return null;
  if (o['accepted'] !== undefined && (typeof o['accepted'] !== 'number' || !Number.isFinite(o['accepted'])))
    return null;
  return {
    topic: o['topic'] as string,
    messageId: o['messageId'] as string,
    at: o['at'] as number,
    ...(typeof o['hash'] === 'string' ? { hash: o['hash'] as string } : {}),
    ...(typeof o['accepted'] === 'number' ? { accepted: o['accepted'] as number } : {}),
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
 * Segment hash chaining (EB-118): every archived segment additionally
 * appends one line to the versioned chain manifest
 * (`__segment_chain.jsonl`, line format `v: 2`) —
 * `{topic, segmentId, path, sha256, prevHash, entries, bytes}` — linking
 * each topic's segments in landing order (`prevHash` is the previous
 * segment's SHA-256, the first segment's is the deterministic genesis
 * marker `SEGMENT_CHAIN_GENESIS_PREV`). The digest covers the gzipped
 * archive bytes (the same byte range the EB-94 sidecar covers; the
 * sidecar itself is metadata and is not hashed). `DurableTopicLog.
 * verifySegmentChain(dir)` replays the manifest offline and reports
 * reordered lines, missing/replaced/unreadable segments, and rogue
 * (unchained) segment files — O(segments) plus one byte read per
 * segment, an operator tool, not a hot-path check. Rotation and
 * compaction never break the chain: rotation appends new links, and
 * compaction only rewrites the live file.
 *
 * Durability note: appends are synchronous through the storage backend
 * (EB-96 — `DurableStorageBackend.append`, the filesystem default is a
 * single `appendFileSync`), so a record is handed to the OS before
 * `append` returns. A power loss can still lose whatever the OS had not
 * flushed to disk — this is crash recovery for process restarts, not a
 * write-ahead log with `fsync` per message.
 *
 * Supported mode is one writer per directory per process. Two live logs over
 * the same directory interleave lines safely (O_APPEND) but their in-memory
 * counters diverge; do not do that.
 *
 * Payloads that `JSON.stringify` cannot represent (BigInt, circular
 * structures) are delivered live but skipped by the log — `append` returns
 * `false` for them instead of throwing into the publish path.
 */

/** Options for [`DurableTopicLog.forkTopic`] (EB-224). */
export interface ForkTopicOptions {
  /**
   * Exclusive lower bound on the source seq: only records with
   * `seq > fromSeq` copy. Defaults to 0 (the whole live history).
   * Must be a non-negative integer.
   */
  fromSeq?: number;
  /**
   * Exclusive lower bound on the publish timestamp: only records
   * with `at > fromTime` copy. Composes with `fromSeq` (a record
   * copies only when it passes both).
   */
  fromTime?: number;
  /**
   * When true, keep only the latest record per key (by source seq);
   * keyless records always copy. Defaults to false (full history).
   */
  keyedLatest?: boolean;
}

/** Result of [`DurableTopicLog.mergeTopics`] (EB-226). */
export interface MergeTopicsResult {
  /** The two topics the history was merged from (unchanged by the merge). */
  sourceTopics: [string, string];
  /** The new topic holding the merged history. */
  targetTopic: string;
  /** Number of records copied into the target. */
  copied: number;
  /** Each source topic's last seq at merge time, in source order. */
  sourceLastSeqs: [number, number];
  /** The target topic's last seq after the merge (0 when nothing copied). */
  targetLastSeq: number;
}

/**
 * Predicate for [`DurableTopicLog.splitTopics`] (EB-227): called once
 * per live source record, in source seq order, before anything is
 * copied — so a throwing predicate leaves both targets untouched.
 * Receives the full log record (payload, seq, publish timestamp,
 * headers, key, partition, …), not just the payload, because a split
 * criterion is often metadata ("all keyed records", "everything
 * before this timestamp") rather than payload content. Returns true
 * for records that go to the first target, false for the second.
 */
export type SplitTopicPredicate = (record: DurableLogRecord) => boolean;

/** Result of [`DurableTopicLog.splitTopics`] (EB-227). */
export interface SplitTopicsResult {
  /** The topic the history was split from (unchanged by the split). */
  sourceTopic: string;
  /** The target holding the records the predicate matched. */
  targetA: string;
  /** The target holding the records the predicate did not match. */
  targetB: string;
  /** Records copied into the first target. */
  copiedA: number;
  /** Records copied into the second target. */
  copiedB: number;
  /** Total records copied (`copiedA + copiedB`, always the source's live count). */
  copied: number;
  /** The source topic's last seq at split time (its history is untouched). */
  sourceLastSeq: number;
  /** The first target's last seq after the split (0 when nothing matched). */
  targetALastSeq: number;
  /** The second target's last seq after the split (0 when everything matched). */
  targetBLastSeq: number;
}

/** Result of [`DurableTopicLog.forkTopic`] (EB-224). */
export interface ForkTopicResult {
  /** The topic the history was copied from (unchanged by the fork). */
  sourceTopic: string;
  /** The new topic holding the copied history. */
  targetTopic: string;
  /** Number of records copied into the target. */
  copied: number;
  /** The source topic's last seq at fork time (its history is untouched). */
  sourceLastSeq: number;
  /** The target topic's last seq after the fork (0 when nothing copied). */
  targetLastSeq: number;
  /** The `fromSeq` option as passed, if any. */
  fromSeq?: number;
  /** The `fromTime` option as passed, if any. */
  fromTime?: number;
  /** Whether `keyedLatest` filtering applied. */
  keyedLatest: boolean;
}

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
   * The storage backend every object lives behind (EB-96): the per-topic
   * live log, archived segments, and all journals. Defaults to the local
   * filesystem rooted at `dir`.
   */
  private readonly backend: DurableStorageBackend;
  /**
   * Storage key of the group-offset journal (`__group_offsets.jsonl`).
   * One append-only object for every group, separate from the per-topic
   * message objects.
   */
  private readonly offsetKey = OFFSET_JOURNAL_NAME;
  /**
   * Latest committed offset per (group, topic), keyed by
   * `offsetKey(groupId, topic)` — the in-memory side of the offset
   * journal, rebuilt on open and maintained on append.
   */
  private readonly offsetCommits = new Map<string, DurableOffsetCommit>();
  /** Lines currently in the offset journal, used for compaction. */
  private offsetEntries = 0;
  /**
   * Storage key of the subscriber-dedup journal (`__dedup.jsonl`). One
   * append-only object for every dedup-enabled consumer, separate from
   * the per-topic message objects.
   */
  private readonly dedupKey = DEDUP_JOURNAL_NAME;
  /**
   * Latest dedup sighting per (consumer, topic, messageId), keyed by
   * `dedupKey(...)` — the in-memory side of the dedup journal, rebuilt
   * on open and maintained on append.
   */
  private readonly dedupSeen = new Map<string, DedupEntry>();
  /** Lines currently in the dedup journal, used for compaction. */
  private dedupEntries = 0;
  /**
   * EB-205: in-memory (topic, seq) → byte-offset index for the live
   * log file. Lets `readSince` seek directly to the first record with
   * `seq > fromSeqExclusive` instead of parsing the whole file — O(1)
   * point lookup vs O(n) scan. Built during `recover()` by tracking
   * byte offsets while parsing, maintained on `append()`. A seq missing
   * from the index (gap from compaction or corruption) falls back to a
   * full scan.
   */
  private readonly seqIndex = new Map<string, Map<number, number>>();
  /**
   * Storage key of the publish-dedup journal (`__publish_dedup.jsonl`).
   * One append-only object for the whole bus — publish-side dedup
   * identities are scoped to concrete topics (which already include any
   * namespace prefix), so a single root journal covers every namespace.
   */
  private readonly publishDedupKey = PUBLISH_DEDUP_JOURNAL_NAME;
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
  /**
   * EB-94: archived segments isolated by checksum verification — a
   * mismatch (or unreadable bytes with a sidecar present) skips the
   * segment, never fatal. Surfaced in `stats().corruptSegments`.
   */
  private corruptSegments = 0;
  /**
   * EB-94: archive paths rejected by checksum verification during the
   * index pass of recovery — the orphan-adoption scan skips them instead
   * of re-verifying and re-counting.
   */
  private readonly checksumRejected = new Set<string>();
  /** EB-94: observer hook for checksum failures; never throws into the read path. */
  private readonly onSegmentChecksumFailed?: (info: SegmentChecksumFailure) => void;
  /** Storage key of the archive index (`__archive_index.jsonl`). */
  private readonly archiveIndexKey = ARCHIVE_INDEX_NAME;
  /**
   * EB-118: the in-memory view of `__segment_chain.jsonl`, in manifest
   * (landing) order — rebuilt on open, healed for pre-chain archives.
   */
  private readonly chainEntries: SegmentChainEntry[] = [];
  /**
   * EB-118: each topic's chain tail — the `sha256` of its most recently
   * archived segment, the `prevHash` of the next link. Absent until the
   * topic's first segment archives (its `prevHash` is the genesis
   * marker, `SEGMENT_CHAIN_GENESIS_PREV`).
   */
  private readonly chainTails = new Map<string, string>();
  /**
   * EB-119: live-segment bytes per topic — the actual on-disk line bytes
   * (sealed when encryption is on), so compressed records count their
   * compressed wire bytes and binary records their base64 line bytes.
   * Maintained on append, reset by rotation, recomputed by compaction,
   * seeded from the file bytes on recovery.
   */
  private readonly topicBytes = new Map<string, number>();
  /**
   * EB-119: per-topic live-byte budget (`maxBytesPerTopic`), `undefined`
   * when no byte bound is configured (the default — count/age retention
   * behave exactly as before).
   */
  private readonly maxBytesPerTopic: number | undefined;
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
  /**
   * EB-221: per-topic key TTL in milliseconds, exact topics only — the
   * bus resolves its pattern rules to concrete topics and pushes them
   * here (see `setKeyTtl`). A key whose latest message record is older
   * than `now - ttlMs` is evicted at compact time and skipped by
   * `readSince` even before compaction. Absent per topic by default
   * (no TTL). Runtime-only, like the bus's partition rules: restart
   * recovery does not restore it — the bus re-applies its rules on
   * first access, and evicted keys keep their seq baseline via
   * tombstones, not via this map.
   */
  private readonly keyTtls = new Map<string, number>();
  /** EB-221: keys evicted by per-key TTL across compactions. */
  private keyTtlEvicted = 0;
  /**
   * EB-163: resolved remote-offload configuration, `undefined` when
   * offload is disabled (the default — the log never touches the
   * network and keeps every archived segment locally).
   */
  private readonly offload?: ResolvedSegmentOffload;
  /** EB-163: archived segments successfully uploaded to the remote. */
  private offloadUploaded = 0;
  /** EB-163: offload attempts that failed (upload or manifest write). */
  private offloadFailed = 0;
  /**
   * EB-218: local readback cache — remotely-fetched (locally-trimmed)
   * segments, keyed `<topic>\0<segmentId>`, in LRU order (the tail is
   * the most recently used). Sealed segments are immutable and
   * hash-verified against the manifest, so a cached copy is always
   * current; the cache is only populated on a verified remote pull.
   */
  private readonly readbackCache = new Map<string, { records: DurableLogRecord[]; bytes: number }>();
  /** EB-218: downloaded bytes currently held by the readback cache. */
  private readbackCacheBytes = 0;
  /** EB-218: remote segment pulls that reached S3 (cache misses). */
  private s3Readbacks = 0;
  /** EB-218: total bytes downloaded by remote segment pulls. */
  private s3ReadbackBytes = 0;
  /** EB-218: readback reads served from the local cache (no S3 call). */
  private s3ReadbackCacheHits = 0;

  private constructor(
    dir: string,
    maxEntriesPerTopic: number,
    keyCompaction: boolean,
    segmentRotation?: SegmentRotationOptions,
    publishDedupMaxEntries = 10000,
    encryptor?: PayloadEncryptor,
    maxAgeMs?: number,
    now: () => number = Date.now,
    onSegmentChecksumFailed?: (info: SegmentChecksumFailure) => void,
    backend?: DurableStorageBackend,
    maxBytesPerTopic?: number,
    segmentOffload?: SegmentOffloadOptions,
  ) {
    this.logDir = dir;
    this.maxEntriesPerTopic = maxEntriesPerTopic;
    this.keyCompaction = keyCompaction;
    this.segmentRotation = segmentRotation;
    this.publishDedupMaxEntries = publishDedupMaxEntries;
    this.encryptor = encryptor;
    this.maxAgeMs = maxAgeMs;
    this.now = now;
    this.onSegmentChecksumFailed = onSegmentChecksumFailed;
    this.maxBytesPerTopic = maxBytesPerTopic;
    // EB-163: offload needs the resolved rotation — only sealed
    // segments are ever offloaded.
    this.offload = resolveSegmentOffloadOptions(segmentOffload, this.segmentRotation);
    // EB-96: every object the log keeps goes through the backend. The
    // default is the local filesystem rooted at `dir` — the exact
    // on-disk layout the log always used — so existing directories keep
    // working byte for byte.
    this.backend = backend ?? new FilesystemStorageBackend(dir);
    this.recover();
    this.recoverArchiveIndex();
    // EB-118: the chain view is rebuilt after the archive view — healing
    // links segments the manifest does not know in archive-index order.
    this.recoverChainManifest();
    this.recoverOffsets();
    this.recoverDedup();
    this.recoverPublishDedup();
    // EB-163: one background offload pass on open — segments archived
    // before this log opened (a crash between archive and offload) get
    // uploaded without blocking `open` or keeping the process alive.
    if (this.offload !== undefined) this.scheduleOffload();
  }

  /**
   * EB-163: arms a background offload pass on an unref'd timer — the
   * same fire-and-forget pattern as `scheduleArchive`. Never throws
   * into the timer: a remote outage only counts failures, the next
   * pass retries.
   */
  private scheduleOffload(): void {
    const timer = setTimeout(() => {
      try {
        this.offloadNow();
      } catch {
        // Counted inside `offloadNow` where possible; never fatal.
      }
    }, 0);
    timer.unref();
  }

  /**
   * Opens (or creates) the durable log at `dir`. Throws `RangeError` for an
   * empty `dir`, a non-positive-integer `maxEntriesPerTopic`, a
   * non-positive-integer `publishDedupMaxEntries`, invalid
   * `segmentRotation` triggers, a non-positive-finite `maxAgeMs`, a
   * non-positive-finite `maxBytesPerTopic`, a non-function `now`, or an
   * `encryptor` that is not a usable `PayloadEncryptor`.
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
    const maxBytesPerTopic = options.maxBytesPerTopic;
    if (
      maxBytesPerTopic !== undefined &&
      (!Number.isFinite(maxBytesPerTopic) || maxBytesPerTopic <= 0)
    ) {
      throw new RangeError('durableLogMaxBytesPerTopic must be a positive finite number of bytes');
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
    const backend = options.backend;
    if (
      backend !== undefined &&
      (typeof backend !== 'object' ||
        backend === null ||
        typeof (backend as DurableStorageBackend).write !== 'function' ||
        typeof (backend as DurableStorageBackend).read !== 'function' ||
        typeof (backend as DurableStorageBackend).list !== 'function')
    ) {
      throw new RangeError('durableLog backend must be a DurableStorageBackend');
    }
    const segmentRotation = resolveSegmentRotationOptions(options.segmentRotation);
    return new DurableTopicLog(
      dir,
      maxEntriesPerTopic,
      options.keyCompaction ?? false,
      segmentRotation,
      publishDedupMaxEntries,
      encryptor,
      maxAgeMs,
      now,
      options.onSegmentChecksumFailed,
      backend,
      maxBytesPerTopic,
      options.segmentOffload,
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

  /**
   * EB-221: sets (or, with `undefined`, clears) the per-key TTL for one
   * exact topic, returning the previous value. The bus exposes this as
   * `setTopicKeyTtl` with pattern resolution and a subscription-audit
   * trail. Throws `RangeError` for a non-positive-finite `ttlMs`.
   * Takes effect on the next compact for the topic and immediately for
   * `readSince` filtering — a key that expired since the last compact
   * is skipped in replay even before the file is rewritten.
   */
  setKeyTtl(topic: string, ttlMs: number | undefined): number | undefined {
    if (ttlMs !== undefined && (!Number.isFinite(ttlMs) || ttlMs <= 0)) {
      throw new RangeError('keyTtlMs must be a positive finite number of milliseconds');
    }
    const previous = this.keyTtls.get(topic);
    if (ttlMs === undefined) this.keyTtls.delete(topic);
    else this.keyTtls.set(topic, ttlMs);
    return previous;
  }

  /** EB-221: the per-key TTL configured for one exact topic, if any. */
  keyTtl(topic: string): number | undefined {
    return this.keyTtls.get(topic);
  }

  /** The log directory, as configured. */
  get dir(): string {
    return this.logDir;
  }

  /**
   * The storage backend this log reads and writes through (EB-96). The
   * bus scopes it for namespace child logs (`backend.scoped(...)`).
   */
  get storageBackend(): DurableStorageBackend {
    return this.backend;
  }

  /**
   * Pushes every write so far to stable storage (EB-154, fsync
   * semantics): after `flush()` returns, records appended through this
   * log survive a process crash, not just an OS-buffered return.
   * Appends were already synchronous through the storage backend (a
   * record is handed to the OS before `append` returns); `flush()` adds
   * the OS → disk push the class's durability note always excluded.
   * Namespace child logs share the root log's backend (EB-96), so
   * flushing the root log covers every namespace. A no-op for backends
   * without `sync` (their writes are already durable at write time).
   * Never throws for a backend with nothing to flush.
   */
  flush(): void {
    this.backend.sync?.();
  }

  /** Storage key for a topic's live log: URL-encoded so any topic string is a safe key. */
  private fileFor(topic: string): string {
    return `${encodeURIComponent(topic)}.log`;
  }

  /**
   * Reads a UTF-8 text object from the backend, or `undefined` when the
   * key is absent or unreadable. The backend never throws for a missing
   * key; this also swallows decode-level surprises so recovery stays
   * non-fatal.
   */
  private readText(key: string): string | undefined {
    const bytes = this.backend.read(key);
    if (bytes === undefined) return undefined;
    return Buffer.from(bytes).toString('utf8');
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
      // EB-96: recovery enumerates the backend — the same listing a
      // custom (e.g. S3) backend serves, not a directory read.
      names = this.backend.list();
    } catch {
      return;
    }
    for (const name of names) {
      // EB-96: namespace child logs live under the reserved prefix —
      // their keys belong to the child log's scoped view, not this one.
      if (name.startsWith(NAMESPACE_LOG_KEY_PREFIX)) continue;
      if (!name.endsWith('.log')) continue;
      let topic: string;
      try {
        topic = decodeURIComponent(name.slice(0, -'.log'.length));
      } catch {
        this.corruptLines += 1;
        continue;
      }
      const text = this.readText(this.fileFor(topic));
      // EB-119: byte accounting starts from the actual on-disk bytes —
      // the same measure `append` maintains per line.
      this.topicBytes.set(topic, text === undefined ? 0 : Buffer.byteLength(text));
      // EB-205: build the seq→offset index while parsing.
      const records = text === undefined ? [] : this.parseFileRecordsWithIndex(topic, text);
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
    const text = this.readText(this.archiveIndexKey);
    if (text !== undefined) {
      for (const line of text.split('\n')) {
        if (line.length === 0) continue;
        const entry = parseArchiveIndexLine(line);
        if (entry == null) {
          this.corruptLines += 1;
          continue;
        }
        // EB-94: every indexed segment is checksum-verified at recovery.
        // A mismatch isolates the segment — it never enters the
        // in-memory view, so no read can ever touch it — and counts it in
        // `corruptSegments`, never fatal. Rejected paths are remembered
        // so the orphan-adoption scan below does not re-verify (and
        // re-count) them.
        if (!this.verifySegmentChecksum(entry.path)) {
          this.noteChecksumFailure(entry.topic, entry.path, entry.segmentId);
          this.checksumRejected.add(entry.path);
          continue;
        }
        this.addArchiveEntry(entry);
      }
    }
    let names: string[];
    try {
      names = this.backend.list();
    } catch {
      return;
    }
    let pending = false;
    for (const name of names) {
      // EB-96: namespace child logs live under the reserved prefix —
      // their segments belong to the child log's scoped view.
      if (name.startsWith(NAMESPACE_LOG_KEY_PREFIX)) continue;
      if (name.endsWith('.jsonl.tmp')) {
        pending = true;
        continue;
      }
      const parsed = parseSegmentFileName(name);
      if (parsed == null) continue;
      // EB-94: already rejected by the index pass — not an orphan, an
      // isolated segment. Skipping avoids a second verification count.
      if (this.checksumRejected.has(name)) continue;
      if (this.archives.some((e) => e.path === name)) continue;
      const adopted = this.readAdoptedSegment(name, parsed.topic);
      if (adopted == null) continue;
      this.addArchiveEntry(adopted);
      try {
        this.backend.append(this.archiveIndexKey, `${JSON.stringify(archiveIndexLineOf(adopted))}\n`);
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
   * EB-118: rebuilds the in-memory chain view from
   * `__segment_chain.jsonl` (corrupt lines counted and skipped), then
   * heals it: archived segments the manifest does not know — archived
   * before the chain existed, or a crash between the index append and
   * the chain append — are linked in archive-index (landing) order with
   * their bytes re-hashed from disk. Checksum-isolated segments
   * (`checksumRejected`) never enter the chain: they are not history the
   * log will ever read.
   */
  private recoverChainManifest(): void {
    const text = this.readText(CHAIN_MANIFEST_NAME);
    if (text !== undefined) {
      for (const line of text.split('\n')) {
        if (line.length === 0) continue;
        const entry = parseChainManifestLine(line);
        if (entry == null) {
          this.corruptLines += 1;
          continue;
        }
        this.chainEntries.push(entry);
        this.chainTails.set(entry.topic, entry.sha256);
      }
    }
    this.healChainManifest();
  }

  /**
   * EB-118: links every archived segment missing from the chain manifest,
   * in archive-index order. Healing reads each such segment's bytes once
   * — O(archived data), one time per process start — and appends the
   * missing line, so a directory that predates the chain (or survived a
   * crash mid-archive) becomes fully chained without operator action.
   * Never throws: a failed manifest append is healed again on the next
   * open, and the archive view itself is already complete.
   */
  private healChainManifest(): void {
    const known = new Set(this.chainEntries.map((e) => e.path));
    for (const entry of this.archives) {
      if (known.has(entry.path)) continue;
      // EB-94: an isolated segment must not anchor a chain link.
      if (this.checksumRejected.has(entry.path)) continue;
      let raw: Buffer | undefined;
      try {
        const bytes = this.backend.read(entry.path);
        raw = bytes === undefined ? undefined : Buffer.from(bytes);
      } catch {
        raw = undefined;
      }
      // Unreadable now: left out of the chain — `verifySegmentChain`
      // reports it as missing/unreadable for the operator.
      if (raw === undefined) continue;
      this.appendChainLink(
        entry.topic,
        entry,
        createHash('sha256').update(raw).digest('hex'),
        raw.length,
      );
    }
  }

  /**
   * EB-118: appends one chain-manifest line for a freshly archived
   * segment and advances the topic's in-memory chain tail. Returns
   * `false` when the manifest append failed — the caller leaves the
   * archive's pending file for the next pass, and the next open heals
   * the missing line in index order. Never throws.
   */
  private appendChainLink(
    topic: string,
    entry: ArchiveEntry,
    segmentHash: string,
    bytes: number,
  ): boolean {
    const link: SegmentChainEntry = {
      topic,
      segmentId: entry.segmentId,
      path: entry.path,
      sha256: segmentHash,
      // The chain is per topic, in landing order: the previous segment's
      // digest, or the deterministic genesis marker for the first.
      prevHash: this.chainTails.get(topic) ?? SEGMENT_CHAIN_GENESIS_PREV,
      entries: entry.entries,
      bytes,
    };
    try {
      this.backend.append(CHAIN_MANIFEST_NAME, `${JSON.stringify(chainManifestLineOf(link))}\n`);
    } catch {
      return false;
    }
    this.chainEntries.push(link);
    this.chainTails.set(topic, segmentHash);
    return true;
  }

  /**
   * Reads an on-disk archive that the index does not know about (orphan
   * adoption during recovery). Returns `null` — and counts one corrupt
   * archive — when the file cannot be decompressed or holds no records.
   * EB-94: adopted orphans are checksum-verified exactly like indexed
   * segments — a mismatch isolates the segment (`corruptSegments`) and
   * the file is left for the operator, never adopted.
   */
  private readAdoptedSegment(
    name: string,
    topic: string,
  ): ArchiveEntry | null {
    let raw: Buffer | undefined;
    try {
      const bytes = this.backend.read(name);
      raw = bytes === undefined ? undefined : Buffer.from(bytes);
    } catch {
      raw = undefined;
    }
    if (raw === undefined) {
      this.corruptArchives += 1;
      return null;
    }
    const parsed = parseSegmentFileName(name);
    if (!this.verifySegmentChecksum(name)) {
      this.noteChecksumFailure(topic, name, parsed == null ? name : `${parsed.seqStart}-${parsed.seqEnd}`);
      return null;
    }
    let text: string;
    try {
      text = gunzipSync(raw).toString('utf8');
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
    const text = this.readText(this.fileFor(topic));
    if (text === undefined) return [];
    return this.parseFileRecords(topic, text);
  }

  /** Parses every well-formed record in one topic file's text. */
  private parseFileRecords(topic: string, text: string): DurableLogRecord[] {
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
   * EB-205: parses records and builds the (seq → byte offset) index.
   * Tracks the byte offset of each line's start so `readSince` can
   * slice the file text directly instead of re-scanning from the
   * beginning. Only indexes records with `seq >= 1` (the ones
   * `readSince` can return). For duplicate seqs (e.g. same seq on
   * different partitions), keeps the FIRST offset — `readSince` wants
   * everything with `seq > fromSeqExclusive`, so starting at the
   * first occurrence is correct.
   */
  private parseFileRecordsWithIndex(topic: string, text: string): DurableLogRecord[] {
    const records: DurableLogRecord[] = [];
    const index = new Map<number, number>();
    let offset = 0;
    // Split keeping the delimiter so offsets stay exact. JSON log
    // lines are ASCII-safe, so byte offset == string index.
    const lines = text.split('\n');
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const lineOffset = offset;
      // +1 for the '\n' we split on (last line may not have it, but
      // the offset math stays correct because we only use offsets
      // of lines that exist).
      offset += Buffer.byteLength(line) + 1;
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
      if (rec.seq >= 1 && !index.has(rec.seq)) {
        // Keep the first offset for each seq — duplicates (same seq
        // on different partitions) must not overwrite it, or
        // `readSince` would skip the earlier record.
        index.set(rec.seq, lineOffset);
      }
    }
    this.seqIndex.set(topic, index);
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
      // The backend append is synchronous — the record is on stable
      // storage before this returns, the same durability the log always
      // relied on.
      // EB-205: capture the byte offset before appending for the seq index.
      const offsetBefore = this.topicBytes.get(record.topic) ?? 0;
      this.backend.append(this.fileFor(record.topic), line);
      // EB-205: maintain the seq→offset index. Keep the first offset
      // for each seq — duplicates (same seq on different partitions)
      // must not overwrite it.
      if (record.seq >= 1) {
        let index = this.seqIndex.get(record.topic);
        if (index == null) {
          index = new Map();
          this.seqIndex.set(record.topic, index);
        }
        if (!index.has(record.seq)) {
          index.set(record.seq, offsetBefore);
        }
      }
    } catch {
      return false;
    }
    this.topicsSeen.add(record.topic);
    const count = (this.entryCounts.get(record.topic) ?? 0) + 1;
    this.entryCounts.set(record.topic, count);
    // EB-119: the line's actual on-disk bytes — sealed when encryption is
    // on — so compressed/binary records count their wire bytes.
    this.topicBytes.set(record.topic, (this.topicBytes.get(record.topic) ?? 0) + Buffer.byteLength(line));
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
    if (this.segmentRotation !== undefined) {
      this.noteSegmentAppend(record);
      // EB-119: with rotation the byte budget archives the oldest live
      // data instead of evicting it — the rotated segment stays
      // replayable via `readSince`/`readSinceTime` with
      // `{ includeArchived: true }`. The rename is O(1); without
      // rotation the byte bound is enforced by compaction below.
      if (
        this.maxBytesPerTopic !== undefined &&
        (this.topicBytes.get(record.topic) ?? 0) > this.maxBytesPerTopic
      ) {
        this.rotateSegment(record.topic);
      }
    }
    const liveCount = this.entryCounts.get(record.topic) ?? 0;
    const liveBytes = this.topicBytes.get(record.topic) ?? 0;
    const superseded = this.supersededCounts.get(record.topic) ?? 0;
    if (
      liveCount > this.maxEntriesPerTopic * 2 ||
      (this.maxBytesPerTopic !== undefined && liveBytes > this.maxBytesPerTopic * 2) ||
      (this.keyCompaction && superseded >= this.maxEntriesPerTopic)
    ) {
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
   * EB-94: checksum sidecar key for an archived segment — `<archive
   * name>.sha256` holding the hex SHA-256 of the segment's gzipped bytes
   * (`node:crypto`, zero new dependencies). Written right after the
   * `.gz`, before the index line.
   */
  private checksumKey(archiveName: string): string {
    return `${archiveName}.sha256`;
  }

  /**
   * EB-94: verifies an archived segment against its checksum sidecar.
   * Returns `true` when the bytes match, and also when no sidecar
   * exists — segments archived before checksums, or a crash between the
   * `.gz` write and the sidecar write, are unverifiable and read exactly
   * as before. Returns `false` when a sidecar exists but the bytes are
   * missing, unreadable, or mismatched: the segment is corrupt and must
   * be isolated, never read. The sidecar and the bytes both come from
   * the storage backend (EB-96) — verification itself is unchanged.
   */
  private verifySegmentChecksum(archiveName: string): boolean {
    const expected = this.readText(this.checksumKey(archiveName))?.trim();
    if (expected === undefined) return true;
    const bytes = this.backend.read(archiveName);
    if (bytes === undefined) return false;
    return createHash('sha256').update(bytes).digest('hex') === expected;
  }

  /**
   * EB-94: isolates one checksum-failed segment — counted in
   * `corruptSegments` and reported to the observer hook. Never throws:
   * a broken hook must not disturb the read/recovery path.
   */
  private noteChecksumFailure(topic: string, path: string, segmentId: string): void {
    this.corruptSegments += 1;
    const hook = this.onSegmentChecksumFailed;
    if (hook === undefined) return;
    try {
      hook({ topic, path, segmentId });
    } catch {
      // The hook is an observer, not part of the read path.
    }
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
    const pending = this.pendingSegmentName(topic, seqStart, seqEnd);
    try {
      // EB-96: the rename is the commit point on every backend — a
      // crash leaves the live object or the complete pending object.
      this.backend.rename(live, pending);
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
    // EB-119: the archived records leave the live byte budget too.
    this.topicBytes.set(topic, 0);
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

  /** Archives every pending segment (`*.jsonl.tmp`) in the log's key space. */
  private drainArchiveQueue(): void {
    let names: string[];
    try {
      names = this.backend.list();
    } catch {
      return;
    }
    for (const name of names) {
      // EB-96: a child log drains its own pending segments.
      if (name.startsWith(NAMESPACE_LOG_KEY_PREFIX)) continue;
      if (name.endsWith('.jsonl.tmp')) this.archiveOne(name);
    }
    // EB-163: newly archived segments go remote on the same background
    // pass. Never throws into the timer: a remote outage only counts
    // failures, the next pass retries.
    if (this.offload !== undefined) {
      try {
        this.offloadNow();
      } catch {
        // Counted inside `offloadNow` where possible; the pass itself
        // must stay non-fatal.
      }
    }
  }

  /**
   * EB-163: the deterministic remote object key for one archived
   * segment — `<keyPrefix>/[<namespace>/]<urlencoded-topic>.seg-<seqStart>-<seqEnd>.jsonl.gz`.
   * The same content always addresses the same key, so re-uploads are
   * idempotent and two logs can never collide: topic names are
   * URL-encoded and the seq range is part of the key.
   */
  private remoteSegmentKey(entry: ArchiveEntry): string {
    const offload = this.offload as ResolvedSegmentOffload;
    const ns = offload.namespace === '' ? '' : `${encodeURIComponent(offload.namespace)}/`;
    return `${offload.keyPrefix}/${ns}${encodeURIComponent(entry.topic)}.seg-${entry.seqStart}-${entry.seqEnd}.jsonl.gz`;
  }

  /** EB-163: the remote manifest key — `<keyPrefix>/[<namespace>/]manifest.json`. */
  private remoteManifestKey(): string {
    const offload = this.offload as ResolvedSegmentOffload;
    const ns = offload.namespace === '' ? '' : `${encodeURIComponent(offload.namespace)}/`;
    return `${offload.keyPrefix}/${ns}manifest.json`;
  }

  /**
   * EB-163: loads the remote offload manifest, keyed by
   * `<topic>\0<segmentId>`. Absent manifest → empty (fresh key space).
   * `corrupt: true` when the object exists but is not a well-formed
   * manifest — the offload pass fails closed on that, never overwrites
   * a manifest it cannot read (that would orphan every segment it
   * lists). Throws on transport failure.
   */
  private loadRemoteManifest(): {
    entries: Map<string, RemoteOffloadManifestEntry>;
    corrupt: boolean;
  } {
    const offload = this.offload as ResolvedSegmentOffload;
    const raw = offload.s3.getObject(this.remoteManifestKey());
    const entries = new Map<string, RemoteOffloadManifestEntry>();
    if (raw === undefined) return { entries, corrupt: false };
    let parsed: unknown;
    try {
      parsed = JSON.parse(Buffer.from(raw).toString('utf8'));
    } catch {
      return { entries, corrupt: true };
    }
    if (typeof parsed !== 'object' || parsed === null) return { entries, corrupt: true };
    const segments = (parsed as { segments?: unknown }).segments;
    if (!Array.isArray(segments)) return { entries, corrupt: true };
    for (const item of segments) {
      const entry = parseRemoteManifestEntry(item);
      if (entry === null) return { entries, corrupt: true };
      entries.set(`${entry.topic}\0${entry.segmentId}`, entry);
    }
    return { entries, corrupt: false };
  }

  /**
   * EB-163: uploads every archived segment the remote manifest does not
   * know yet, then trims local copies beyond `retainLocalSegments`.
   * Idempotent: the same bytes always address the same remote key and
   * the manifest is upserted by (topic, segmentId), so a retry or a
   * second log over the same key space re-uploads harmlessly. A dry-run
   * client only records intended keys — no manifest write, no local
   * trim, no upload accounting. Never throws: per-segment failures are
   * counted in `stats().offloadFailed` and retried on the next pass.
   * Returns what this pass did, for operators and tests.
   */
  offloadNow(): { uploaded: number; failed: number; keys: string[] } {
    const offload = this.offload;
    const result = { uploaded: 0, failed: 0, keys: [] as string[] };
    if (offload === undefined) return result;
    const dryRun = offload.s3.isDryRun === true;
    let manifest: Map<string, RemoteOffloadManifestEntry>;
    try {
      const loaded = this.loadRemoteManifest();
      if (loaded.corrupt) {
        // Fail closed: overwriting an unreadable manifest would orphan
        // the segments it lists.
        this.offloadFailed += 1;
        result.failed += 1;
        return result;
      }
      manifest = loaded.entries;
    } catch {
      this.offloadFailed += 1;
      result.failed += 1;
      return result;
    }
    let manifestDirty = false;
    for (const entry of this.archives) {
      const id = `${entry.topic}\0${entry.segmentId}`;
      if (manifest.has(id)) continue;
      const key = this.remoteSegmentKey(entry);
      let bytes: Uint8Array | undefined;
      try {
        bytes = this.backend.read(entry.path);
      } catch {
        bytes = undefined;
      }
      if (bytes === undefined) {
        // Locally gone but never manifested (e.g. trimmed by an older
        // build): nothing to upload — reads of it fail explicitly.
        continue;
      }
      try {
        offload.s3.putObject(key, bytes);
      } catch {
        this.offloadFailed += 1;
        result.failed += 1;
        continue;
      }
      if (dryRun) {
        // Rehearsal: the client recorded the intended key; no remote
        // state changes, no local trim.
        result.keys.push(key);
        continue;
      }
      manifest.set(id, {
        v: 1,
        topic: entry.topic,
        segmentId: entry.segmentId,
        key,
        seqStart: entry.seqStart,
        seqEnd: entry.seqEnd,
        sha256: createHash('sha256').update(bytes).digest('hex'),
        entries: entry.entries,
        bytes: bytes.length,
      });
      manifestDirty = true;
      this.offloadUploaded += 1;
      result.uploaded += 1;
      result.keys.push(key);
    }
    if (manifestDirty) {
      try {
        offload.s3.putObject(
          this.remoteManifestKey(),
          Buffer.from(JSON.stringify({ v: 1, segments: [...manifest.values()] }), 'utf8'),
        );
      } catch {
        // The segment bytes are safely remote; the manifest write is
        // retried on the next pass (the segments re-upload idempotently).
        this.offloadFailed += 1;
        result.failed += 1;
      }
    }
    if (!dryRun) this.trimLocalSegments(manifest);
    return result;
  }

  /**
   * EB-163: deletes local copies of archived segments beyond the newest
   * `retainLocalSegments` per topic — the `.gz` and its checksum
   * sidecar together (a sidecar without its bytes would read as a
   * checksum failure at recovery). Only trims segments the manifest
   * confirms as remote; the archive-index and chain-manifest lines stay
   * — the segment remains known and replayable from the remote.
   */
  private trimLocalSegments(manifest: Map<string, RemoteOffloadManifestEntry>): void {
    const offload = this.offload as ResolvedSegmentOffload;
    const keep = offload.retainLocalSegments;
    if (keep === Infinity) return;
    for (const segments of this.archivesByTopic.values()) {
      const ordered = [...segments].sort((a, b) => b.seqStart - a.seqStart);
      for (let i = keep; i < ordered.length; i++) {
        const entry = ordered[i];
        if (!manifest.has(`${entry.topic}\0${entry.segmentId}`)) continue;
        try {
          this.backend.delete(entry.path);
          this.backend.delete(this.checksumKey(entry.path));
        } catch {
          // Left for the next pass.
        }
      }
    }
  }

  /**
   * EB-163: reads one locally-trimmed segment back from the remote and
   * verifies it against the manifest before returning its records.
   * Throws explicitly — never silently truncates — when the manifest
   * does not list the segment, the object is missing remotely, or the
   * downloaded bytes do not match the manifest hash. Transport errors
   * propagate as-is: also explicit, never a quiet gap.
   */
  private readRemoteSegmentRecords(
    segment: ArchiveEntry,
    fromSeqExclusive: number,
  ): DurableLogRecord[] {
    const offload = this.offload as ResolvedSegmentOffload;
    const id = `${segment.topic}\0${segment.segmentId}`;
    // EB-218: serve from the local readback cache when this segment was
    // pulled before — sealed segments are immutable and hash-verified,
    // so a cached copy is always current.
    const cached = this.readbackCache.get(id);
    if (cached !== undefined) {
      this.s3ReadbackCacheHits += 1;
      // LRU touch: re-insert at the tail.
      this.readbackCache.delete(id);
      this.readbackCache.set(id, cached);
      return cached.records.filter((rec) => rec.seq > fromSeqExclusive);
    }
    const key = this.remoteSegmentKey(segment);
    const refuse = (detail: string): Error =>
      new Error(
        `durable log S3 offload: segment ${segment.topic} ${segment.segmentId} ${detail} ` +
          '(refusing to truncate replay)',
      );
    const manifestEntry = this.loadRemoteManifest().entries.get(id);
    if (manifestEntry === undefined) {
      throw refuse(`is not listed in the remote manifest (key ${key})`);
    }
    if (!offload.s3.headObject(key)) {
      throw refuse(`is missing remotely (key ${key})`);
    }
    const bytes = offload.s3.getObject(key);
    if (bytes === undefined) {
      throw refuse(`is missing remotely (key ${key})`);
    }
    const actual = createHash('sha256').update(bytes).digest('hex');
    if (actual !== manifestEntry.sha256) {
      throw refuse(`hash mismatch: manifest ${manifestEntry.sha256}, remote bytes ${actual}`);
    }
    let text: string;
    try {
      text = gunzipSync(bytes).toString('utf8');
    } catch (err) {
      // The bytes are hash-verified: a decompress failure here is a
      // genuine corruption worth failing on, not a skippable archive.
      throw refuse(`downloaded bytes failed to decompress: ${(err as Error).message}`);
    }
    // EB-218: parse the whole segment, cache the verified records,
    // then filter by cursor — identical to parsing with the cursor
    // directly (`rec.seq > fromSeqExclusive`).
    const records: DurableLogRecord[] = [];
    this.parseArchivedSegmentText(text, segment.topic, -1, records);
    this.s3Readbacks += 1;
    this.s3ReadbackBytes += bytes.length;
    this.cacheReadbackSegment(id, records, bytes.length);
    return records.filter((rec) => rec.seq > fromSeqExclusive);
  }

  /**
   * EB-218: inserts one hash-verified remote segment into the local
   * readback cache, evicting least-recently-used entries first until
   * both the segment-count and downloaded-byte budgets hold. A cache
   * bound of 0 disables caching; a segment larger than the whole byte
   * budget is never cached.
   */
  private cacheReadbackSegment(id: string, records: DurableLogRecord[], bytes: number): void {
    const offload = this.offload as ResolvedSegmentOffload;
    const maxSegments = offload.readbackCacheMaxSegments;
    const maxBytes = offload.readbackCacheMaxBytes;
    if (maxSegments === 0 || maxBytes === 0) return;
    if (bytes > maxBytes) return;
    while (this.readbackCache.size >= maxSegments || this.readbackCacheBytes + bytes > maxBytes) {
      const oldest = this.readbackCache.keys().next();
      if (oldest.done) break;
      const evicted = this.readbackCache.get(oldest.value);
      this.readbackCache.delete(oldest.value);
      if (evicted !== undefined) this.readbackCacheBytes -= evicted.bytes;
    }
    this.readbackCache.set(id, { records, bytes });
    this.readbackCacheBytes += bytes;
  }

  /**
   * EB-163: parses one decompressed archived segment's lines into
   * records newer than `fromSeqExclusive`, in file order. Shared by the
   * local and remote read paths — the bytes differ, the line format does
   * not.
   */
  private parseArchivedSegmentText(
    text: string,
    topic: string,
    fromSeqExclusive: number,
    out: DurableLogRecord[],
  ): void {
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

  /**
   * Archives one pending segment: parses its records, gzips them to the
   * deterministic archive name, writes the checksum sidecar (EB-94),
   * appends the index line, and deletes the pending file. Crash-safe
   * ordering — `.gz` first, sidecar second, index third, pending delete
   * last — so a crash either leaves an orphan `.gz` (adopted on the next
   * open) or a leftover pending file (re-archived idempotently: the same
   * content addresses the same archive name, and an already-indexed
   * segment just drops its pending file). Never throws: a failure leaves
   * the pending file for the next pass.
   */
  private archiveOne(pendingName: string): void {
    const text = this.readText(pendingName);
    if (text === undefined) return;
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
        this.backend.delete(pendingName);
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
        this.backend.delete(pendingName);
      } catch {
        // Left for the next pass.
      }
      return;
    }
    const canonical = records.map((rec) => this.sealLine(`${JSON.stringify(logLineOf(rec))}\n`)).join('');
    let gzBytes: Buffer;
    try {
      gzBytes = gzipSync(canonical);
    } catch {
      // Compression failed: leave the pending file for the next pass.
      return;
    }
    try {
      // EB-80: the archived record bytes are sealed per line before
      // compression, so the archive at rest is encrypted end to end.
      // EB-96: exclusive write through the backend — a racing pass
      // that already archived this segment loses the race benignly.
      this.backend.write(entry.path, gzBytes, { exclusive: true });
    } catch {
      // Either the write failed or the archive already exists (a
      // concurrent pass won the race): leave the pending file for the
      // next pass, which dedupes against the index.
      return;
    }
    // EB-94: checksum sidecar — after the `.gz`, before the index line,
    // extending the crash-safe ordering (.gz -> sidecar -> index ->
    // pending delete). Best-effort: a failed sidecar write leaves the
    // segment unverifiable but fully readable, never lost.
    //
    // EB-118: the chain digest covers the gzipped archive bytes — the
    // same byte range the sidecar covers. The sidecar file itself is
    // metadata and is NOT hashed: a crash between the `.gz` write and the
    // sidecar write must not break the chain.
    const segmentHash = createHash('sha256').update(gzBytes).digest('hex');
    try {
      this.backend.write(this.checksumKey(entry.path), segmentHash);
    } catch {
      // The archive itself is intact; it just reads without verification.
    }
    try {
      this.backend.append(this.archiveIndexKey, `${JSON.stringify(archiveIndexLineOf(entry))}\n`);
    } catch {
      // The `.gz` is safely on disk; the index append is retried via
      // orphan adoption on the next open.
      return;
    }
    // EB-118: chain manifest line — after the index line, before the
    // pending delete, extending the crash-safe ordering (.gz -> sidecar
    // -> index -> chain -> pending delete). A crash here leaves an indexed
    // segment without a chain line; the next open heals it in index
    // order, and the leftover pending file is dropped idempotently by the
    // next pass (the archive itself is already complete).
    if (!this.appendChainLink(topic, entry, segmentHash, gzBytes.length)) return;
    try {
      this.backend.delete(pendingName);
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
    // EB-221: key-TTL eviction runs before age retention — a key older
    // than its TTL leaves a tombstone (seq baseline preserved), while a
    // key older than `maxAgeMs` but younger than its TTL is
    // retention-dropped without one (the pre-EB-221 `maxAgeMs` behavior;
    // set the key TTL <= `maxAgeMs` to keep the baseline guarantee for
    // every key).
    const retained = this.applyRetention(this.applyKeyTtl(topic, records));
    const kept = this.applyBytesBound(
      this.keyCompaction
        ? compactKeyed(retained, this.maxEntriesPerTopic)
        : retained.slice(-this.maxEntriesPerTopic),
    );
    const text = kept.map((rec) => this.sealLine(`${JSON.stringify(logLineOf(rec))}\n`)).join('');
    try {
      // EB-96: atomic replace through the backend — readers never see a
      // half-compacted object.
      this.backend.write(this.fileFor(topic), text);
    } catch {
      return;
    }
    this.entryCounts.set(topic, kept.length);
    this.messageCounts.set(topic, kept.filter((rec) => rec.seq >= 1).length);
    // EB-119: byte accounting restarts from the rewritten file's actual bytes.
    this.topicBytes.set(topic, Buffer.byteLength(text));
    if (this.keyCompaction) this.rebuildKeyIndex(topic, kept);
    // EB-205: the file was rewritten — rebuild the seq→offset index.
    this.parseFileRecordsWithIndex(topic, text);
  }

  /**
   * EB-221: evicts cold keys for a topic — every key whose latest
   * message record is older than `now - keyTtl(topic)` loses all of
   * its records (latest plus superseded dead weight) and leaves one
   * seq-0 tombstone carrying the evicted key's `keySeq` baseline, so
   * restart recovery never renumbers the key (EB-36 per-key order
   * depends on strictly increasing keySeqs). Stale tombstones for the
   * same keys are replaced by the fresh ones, so churned keys leave at
   * most one tombstone each. Seq-0 records that are NOT tombstones
   * (delayed-delivery schedules, retained/pause/recurring state) are
   * never touched even when they carry an evicted key — they are timer
   * intents and state, not the key's value. A key exactly on the cutoff
   * (`at === now - ttlMs`) is kept; only strictly older keys go.
   * No-op when keyed compaction is off or the topic has no TTL.
   */
  private applyKeyTtl(topic: string, records: DurableLogRecord[]): DurableLogRecord[] {
    if (!this.keyCompaction) return records;
    const ttlMs = this.keyTtls.get(topic);
    if (ttlMs === undefined) return records;
    const cutoff = this.now() - ttlMs;
    // Latest message record per key — file order, so the last write wins.
    const latest = new Map<string, DurableLogRecord>();
    for (const rec of records) {
      if (rec.key !== undefined && rec.seq >= 1) latest.set(rec.key, rec);
    }
    const evicted = new Set<string>();
    for (const [key, rec] of latest) {
      if (rec.at < cutoff) evicted.add(key);
    }
    if (evicted.size === 0) return records;
    const nowMs = this.now();
    const kept: DurableLogRecord[] = [];
    for (const rec of records) {
      if (rec.key !== undefined && evicted.has(rec.key)) {
        // Drop the key's messages and its stale tombstones; keep any
        // other seq-0 record carrying the key (a pending schedule's
        // timer intent must survive its key's value eviction).
        if (rec.seq >= 1 || rec.keyTtlEvicted === true) continue;
      }
      kept.push(rec);
    }
    for (const key of evicted) {
      const rec = latest.get(key) as DurableLogRecord;
      kept.push({
        seq: 0,
        topic,
        at: nowMs,
        payload: null,
        key,
        ...(rec.keySeq !== undefined ? { keySeq: rec.keySeq } : {}),
        keyTtlEvicted: true,
      });
      this.keyTtlEvicted += 1;
    }
    return kept;
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
   * EB-119: trims records to the per-topic byte budget — the newest
   * records whose sealed on-disk bytes fit. Byte cost is the actual
   * sealed line length, so compressed records count their compressed
   * wire bytes and binary records their base64 line bytes. Walked
   * newest-first so the survivors are always the newest records that
   * fit; relative order is preserved.
   *
   * Two exemptions, both deliberate: seq-0 records are state
   * (delayed-delivery schedules, retained snapshots, pause buffers) and
   * are never evicted by the byte bound — the same rationale as
   * `maxAgeMs` — and under keyed compaction the latest record per key is
   * exempt too (the key space itself bounds retention, Kafka-style, so a
   * hot key's current value is never evicted by size). Exempt state can
   * push live bytes over the budget; the budget bounds evictable
   * history, not pinned state.
   *
   * No-op when no byte bound is configured.
   */
  private applyBytesBound(records: DurableLogRecord[]): DurableLogRecord[] {
    const budget = this.maxBytesPerTopic;
    if (budget === undefined) return records;
    const costs = new Array<number>(records.length);
    for (let i = 0; i < records.length; i++) {
      costs[i] = Buffer.byteLength(this.sealLine(`${JSON.stringify(logLineOf(records[i]))}\n`));
    }
    const keep = new Array<boolean>(records.length).fill(false);
    let bytes = 0;
    for (let i = records.length - 1; i >= 0; i--) {
      const rec = records[i];
      const exempt = rec.seq === 0 || (this.keyCompaction && rec.key !== undefined);
      if (exempt || bytes + costs[i] <= budget) {
        keep[i] = true;
        bytes += costs[i];
      }
    }
    return records.filter((_, i) => keep[i]);
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
    // EB-205: try the seq→offset index for a point lookup. If the
    // target seq is indexed, slice the file text at its offset and
    // parse only the tail — O(records after cursor) instead of O(all).
    // A missing index entry (gap from compaction/corruption) falls
    // back to the full scan below.
    const live = this.readFileRecordsSince(topic, fromSeqExclusive);
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
    let deduped = records.filter((rec) => rec.key === undefined || rec.seq < 1 || latestByKey.get(rec.key) === rec);
    // EB-221: a key whose latest value expired is skipped here, before
    // any compact rewrites the file — replay never resurrects a cold
    // key, including an archived value whose live successor was already
    // TTL-evicted (the merged latest is still older than the cutoff).
    // A record exactly on the cutoff is kept, mirroring compact.
    const ttlMs = this.keyTtls.get(topic);
    if (ttlMs !== undefined) {
      const cutoff = this.now() - ttlMs;
      deduped = deduped.filter((rec) => rec.key === undefined || rec.seq < 1 || rec.at >= cutoff);
    }
    return deduped;
  }

  /**
   * EB-205: reads live records with `seq > fromSeqExclusive`, using the
   * seq→offset index when available. Falls back to a full scan when the
   * index has no entry for the target seq (gap).
   */
  private readFileRecordsSince(topic: string, fromSeqExclusive: number): DurableLogRecord[] {
    const targetSeq = fromSeqExclusive + 1;
    const index = this.seqIndex.get(topic);
    const offset = index?.get(targetSeq);
    if (offset !== undefined) {
      // Point lookup hit: slice at the indexed offset and parse the tail.
      const text = this.readText(this.fileFor(topic));
      if (text === undefined) return [];
      // The offset is a byte offset; JSON log lines are ASCII-safe so
      // it doubles as a string index. Clamp defensively — a stale
      // index entry (e.g. after external file modification) falls back
      // to the full scan instead of throwing or returning garbage.
      if (offset < 0 || offset > text.length) {
        return this.readFileRecords(topic).filter((rec) => rec.seq > fromSeqExclusive);
      }
      const tail = text.slice(offset);
      // The tail starts exactly at the target record's line.
      return this.parseFileRecords(topic, tail).filter((rec) => rec.seq > fromSeqExclusive);
    }
    // Index miss (gap): full scan.
    return this.readFileRecords(topic).filter((rec) => rec.seq > fromSeqExclusive);
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
      let bytes: Uint8Array | undefined;
      try {
        bytes = this.backend.read(segment.path);
      } catch {
        bytes = undefined;
      }
      if (bytes === undefined) {
        if (this.offload !== undefined) {
          // EB-163: locally trimmed — pull from the remote and verify
          // against the manifest hash. Throws explicitly on a missing
          // or tampered remote segment: never a silent gap in replay.
          for (const rec of this.readRemoteSegmentRecords(segment, fromSeqExclusive)) {
            out.push(rec);
          }
          continue;
        }
        this.corruptArchives += 1;
        continue;
      }
      // EB-94: a checksum mismatch isolates the segment — skipped and
      // counted (`corruptSegments`), never fatal, exactly like a failed
      // decompress below.
      if (!this.verifySegmentChecksum(segment.path)) {
        this.noteChecksumFailure(segment.topic, segment.path, segment.segmentId);
        continue;
      }
      let text: string;
      try {
        text = gunzipSync(bytes).toString('utf8');
      } catch {
        this.corruptArchives += 1;
        continue;
      }
      this.parseArchivedSegmentText(text, topic, fromSeqExclusive, out);
    }
    // Pending segments: rotated, awaiting the background archive pass.
    // The rename is atomic, so the object is complete and safe to read.
    const prefix = `${encodeURIComponent(topic)}.seg-`;
    let names: string[];
    try {
      names = this.backend.list();
    } catch {
      return out;
    }
    for (const name of names) {
      // EB-96: a child log reads its own pending segments.
      if (name.startsWith(NAMESPACE_LOG_KEY_PREFIX)) continue;
      if (!name.startsWith(prefix) || !name.endsWith('.jsonl.tmp')) continue;
      const text = this.readText(name);
      if (text === undefined) continue;
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
   * Destroys one topic's durable state (EB-126 `deleteTopic` with
   * `purge: true`): the live log file (`<topic>.log`), pending and
   * archived segments (`<topic>.seg-<start>-<end>.jsonl.tmp/.gz`) and
   * their EB-94 checksum sidecars (`.sha256`), the topic's lines in the
   * archive index (`__archive_index.jsonl`) and the EB-118 segment-chain
   * manifest (`__segment_chain.jsonl`), and every in-memory counter keyed
   * by the topic (`lastSeqs` and friends — numbering restarts from 1 if
   * the topic is ever re-provisioned). The two journals are rewritten
   * without the topic's lines rather than deleted; every other topic's
   * data is untouched.
   *
   * Key matching is exact: the URL-encoded topic plus the `.log` / `.seg-`
   * suffixes, so topic `orders` never takes `orders.eu`'s files with it.
   * Returns what was destroyed, for the audit trail.
   */
  purgeTopic(topic: string): { filesDeleted: number; indexLinesRemoved: number; chainLinesRemoved: number } {
    const enc = encodeURIComponent(topic);
    const liveKey = `${enc}.log`;
    const segmentPrefix = `${enc}.seg-`;
    let filesDeleted = 0;
    for (const key of this.backend.list()) {
      if (key === liveKey || key.startsWith(segmentPrefix)) {
        try {
          this.backend.delete(key);
          filesDeleted += 1;
        } catch {
          // Best effort: the in-memory state below is still cleared so
          // the bus never serves a half-purged topic.
        }
      }
    }
    // The topic's archive-index and chain-manifest lines go; unparseable
    // lines are kept verbatim (recovery already skips them — never fatal).
    let indexLinesRemoved = 0;
    const indexText = this.readText(this.archiveIndexKey);
    if (indexText !== undefined) {
      const kept: string[] = [];
      for (const line of indexText.split('\n')) {
        if (line.trim() === '') continue;
        const entry = parseArchiveIndexLine(line);
        if (entry !== null && entry.topic === topic) {
          indexLinesRemoved += 1;
          continue;
        }
        kept.push(line);
      }
      this.backend.write(this.archiveIndexKey, kept.length === 0 ? '' : `${kept.join('\n')}\n`);
    }
    let chainLinesRemoved = 0;
    const chainText = this.readText(CHAIN_MANIFEST_NAME);
    if (chainText !== undefined) {
      const kept: string[] = [];
      for (const line of chainText.split('\n')) {
        if (line.trim() === '') continue;
        const entry = parseChainManifestLine(line);
        if (entry !== null && entry.topic === topic) {
          chainLinesRemoved += 1;
          continue;
        }
        kept.push(line);
      }
      this.backend.write(CHAIN_MANIFEST_NAME, kept.length === 0 ? '' : `${kept.join('\n')}\n`);
    }
    // In-memory state: the topic must read as never-seen afterwards.
    this.topicsSeen.delete(topic);
    this.entryCounts.delete(topic);
    this.messageCounts.delete(topic);
    this.lastSeqs.delete(topic);
    this.maxPartitionSeqs.delete(topic);
    this.keyIndex.delete(topic);
    // EB-221: the topic's key TTL goes with it — the bus re-applies its
    // rules on next access if one still matches, exactly like a fresh
    // topic. (maxKeySeqs is untouched: purging one topic must not
    // renumber keys, same as below.)
    this.keyTtls.delete(topic);
    // maxKeySeqs is keyed by message key, not topic — key sequence numbers
    // are a cross-topic publish-order space (mirrors the bus's keyCursors),
    // so purging one topic must not touch it.
    this.supersededCounts.delete(topic);
    this.segmentEntries.delete(topic);
    this.segmentStartSeq.delete(topic);
    this.segmentStartAt.delete(topic);
    this.topicBytes.delete(topic);
    this.archivesByTopic.delete(topic);
    this.chainTails.delete(topic);
    for (const path of [...this.checksumRejected]) {
      if (path.startsWith(segmentPrefix)) this.checksumRejected.delete(path);
    }
    for (let i = this.archives.length - 1; i >= 0; i--) {
      if (this.archives[i].topic === topic) this.archives.splice(i, 1);
    }
    for (let i = this.chainEntries.length - 1; i >= 0; i--) {
      if (this.chainEntries[i].topic === topic) this.chainEntries.splice(i, 1);
    }
    // EB-218: drop the purged topic's readback-cached segments — the
    // topic must read as never-seen afterwards, from the cache too.
    for (const id of [...this.readbackCache.keys()]) {
      if (id.startsWith(`${topic}\0`)) {
        const evicted = this.readbackCache.get(id);
        this.readbackCache.delete(id);
        if (evicted !== undefined) this.readbackCacheBytes -= evicted.bytes;
      }
    }
    return { filesDeleted, indexLinesRemoved, chainLinesRemoved };
  }

  /**
   * Renames a topic's durable state (EB-142): the live log file and every
   * archived segment (checksum sidecars included) move atomically via
   * `backend.rename`; the archive-index and segment-chain manifest lines
   * are rewritten to the new name; and every in-memory index keyed by
   * topic is re-keyed — sequence cursors (`lastSeqs`, per-partition seqs),
   * entry/message counts, key indexes, archives, chain entries, and the
   * offset / dedup / publish-dedup journals (rewritten from the re-keyed
   * in-memory state, serialized exactly the way their compactors write).
   *
   * All-or-nothing: validation runs before any mutation; the file moves
   * run before the in-memory re-key, and a failed move rolls the
   * completed moves back and throws with in-memory state untouched.
   * Sequence numbering continues under the new name — a bus reopening this
   * log recovers `lastSeqs` for the new topic, so no seq is ever reused.
   *
   * Throws `RangeError` when `oldTopic` is unknown to the log or
   * `newTopic` is already in use (a known topic or an existing file).
   */
  renameTopic(
    oldTopic: string,
    newTopic: string,
  ): {
    filesRenamed: number;
    indexLinesRewritten: number;
    chainLinesRewritten: number;
    offsetCommitsMoved: number;
    dedupEntriesMoved: number;
    publishDedupEntriesMoved: number;
  } {
    if (!this.topicsSeen.has(oldTopic)) {
      throw new RangeError(`renameTopic: unknown topic "${oldTopic}"`);
    }
    if (this.topicsSeen.has(newTopic)) {
      throw new RangeError(`renameTopic: target topic "${newTopic}" is already in use`);
    }
    const oldEnc = encodeURIComponent(oldTopic);
    const newEnc = encodeURIComponent(newTopic);
    const liveKey = `${oldEnc}.log`;
    const newLiveKey = `${newEnc}.log`;
    const segmentPrefix = `${oldEnc}.seg-`;
    const newSegmentPrefix = `${newEnc}.seg-`;
    // The target must not collide with an existing file either (a topic
    // the log never saw could still have a stray file on disk).
    const keys = this.backend.list();
    if (keys.includes(newLiveKey) || keys.some((k) => k.startsWith(newSegmentPrefix))) {
      throw new RangeError(`renameTopic: target topic "${newTopic}" already has log files`);
    }
    // --- File phase: transform contents in memory, write the new files,
    // then delete the old ones. Nothing in memory is touched until every
    // new file has landed: a throw before the delete phase leaves the old
    // files intact, and the catch removes the new files already written —
    // the directory is never left half-renamed.
    const renamedKey = (key: string): string =>
      key.startsWith(segmentPrefix) ? `${newSegmentPrefix}${key.slice(segmentPrefix.length)}` : key;
    // Rewrites one topic file's JSONL records to the new topic name.
    // Sealed lines are opened and re-sealed (the log holds the
    // encryptor); unparseable lines pass through verbatim — recovery
    // already skips them, never fatal.
    const rewriteRecords = (text: string): string => {
      const out: string[] = [];
      for (const line of text.split('\n')) {
        if (line.length === 0) continue;
        let inner: string | null;
        try {
          inner = this.openLine(line);
        } catch {
          // A sealed line without an encryptor configured: keep it
          // verbatim rather than corrupting what we cannot read.
          out.push(line);
          continue;
        }
        if (inner == null) {
          out.push(line);
          continue;
        }
        let obj: Record<string, unknown>;
        try {
          obj = JSON.parse(inner) as Record<string, unknown>;
        } catch {
          out.push(line);
          continue;
        }
        if (obj['topic'] === oldTopic) obj['topic'] = newTopic;
        out.push(this.sealLine(`${JSON.stringify(obj)}\n`));
      }
      return out.join('');
    };
    // Phase A (pure compute): transform every file's contents and work
    // out the new chain hashes. No file is written and no in-memory
    // state is touched here.
    interface SegmentPlan {
      entry: SegmentChainEntry;
      newPath: string;
      gz: Uint8Array;
      hash: string;
    }
    const segmentPlans: SegmentPlan[] = [];
    let prevHash = 'genesis';
    const topicChain = this.chainEntries
      .filter((e) => e.topic === oldTopic)
      .sort((a, b) => a.seqStart - b.seqStart || a.seqEnd - b.seqEnd);
    for (const entry of topicChain) {
      const raw = this.backend.read(entry.path);
      if (raw === undefined) throw new Error(`renameTopic: missing segment file "${entry.path}"`);
      let text: string;
      try {
        text = gunzipSync(raw).toString('utf8');
      } catch (err) {
        throw new Error(`renameTopic: cannot decompress segment "${entry.path}": ${(err as Error).message}`);
      }
      const gz = gzipSync(rewriteRecords(text));
      const hash = createHash('sha256').update(gz).digest('hex');
      segmentPlans.push({ entry, newPath: renamedKey(entry.path), gz, hash });
      prevHash = hash;
    }
    // The planned chain entries: the old lines are replaced by these
    // (new bytes, new hashes, unbroken prevHash links).
    prevHash = 'genesis';
    const plannedChain: SegmentChainEntry[] = segmentPlans.map((p) => {
      const planned: SegmentChainEntry = {
        ...p.entry,
        topic: newTopic,
        path: p.newPath,
        sha256: p.hash,
        prevHash,
        bytes: p.gz.length,
      };
      prevHash = p.hash;
      return planned;
    });
    // Pending (rotated but not yet gzipped) segments are plaintext JSONL:
    // same record rewrite, no re-hash.
    const pendingPlans: Array<{ oldKey: string; newKey: string; text: string }> = [];
    for (const key of keys) {
      if (!key.startsWith(segmentPrefix) || key === liveKey) continue;
      if (key.endsWith('.jsonl.gz') || key.endsWith('.sha256')) continue;
      const text = this.readText(key);
      if (text === undefined) throw new Error(`renameTopic: cannot read segment "${key}"`);
      pendingPlans.push({ oldKey: key, newKey: renamedKey(key), text: rewriteRecords(text) });
    }
    // The live log file.
    let livePlan: { newKey: string; text: string } | null = null;
    if (keys.includes(liveKey)) {
      const text = this.readText(liveKey);
      if (text === undefined) throw new Error(`renameTopic: cannot read log file "${liveKey}"`);
      livePlan = { newKey: newLiveKey, text: rewriteRecords(text) };
    }
    // Archive-index lines: the topic's lines are rewritten (new name, new
    // paths); every other topic's lines pass through verbatim.
    const rewriteArchiveIndex = (): { text: string; rewritten: number } => {
      const text = this.readText(this.archiveIndexKey);
      if (text === undefined) return { text: '', rewritten: 0 };
      const kept: string[] = [];
      let rewritten = 0;
      for (const line of text.split('\n')) {
        if (line.trim() === '') continue;
        const entry = parseArchiveIndexLine(line);
        if (entry === null || entry.topic !== oldTopic) {
          kept.push(line);
          continue;
        }
        entry.topic = newTopic;
        if (entry.path.startsWith(segmentPrefix)) entry.path = renamedKey(entry.path);
        kept.push(JSON.stringify(archiveIndexLineOf(entry)));
        rewritten += 1;
      }
      return { text: kept.length === 0 ? '' : `${kept.join('\n')}\n`, rewritten };
    };
    // Chain-manifest lines: the topic's lines are replaced by the planned
    // (re-hashed) entries; every other topic's lines pass through.
    const rewriteChainManifest = (): { text: string; rewritten: number } => {
      const text = this.readText(CHAIN_MANIFEST_NAME);
      const kept: string[] = [];
      if (text !== undefined) {
        for (const line of text.split('\n')) {
          if (line.trim() === '') continue;
          const entry = parseChainManifestLine(line);
          if (entry === null || entry.topic !== oldTopic) {
            kept.push(line);
            continue;
          }
        }
      }
      for (const planned of plannedChain) kept.push(JSON.stringify(chainManifestLineOf(planned)));
      return {
        text: kept.length === 0 ? '' : `${kept.join('\n')}\n`,
        rewritten: plannedChain.length,
      };
    };
    const index = rewriteArchiveIndex();
    const chain = rewriteChainManifest();
    // Phase B (file writes): every new file lands before any old file is
    // deleted. On failure the written files are removed and the throw
    // propagates with the old files — and all in-memory state — intact.
    const written: string[] = [];
    const stale: string[] = [];
    try {
      for (const plan of segmentPlans) {
        this.backend.write(plan.newPath, plan.gz);
        written.push(plan.newPath);
        this.backend.write(this.checksumKey(plan.newPath), plan.hash);
        written.push(this.checksumKey(plan.newPath));
        stale.push(plan.entry.path, this.checksumKey(plan.entry.path));
      }
      for (const plan of pendingPlans) {
        this.backend.write(plan.newKey, plan.text);
        written.push(plan.newKey);
        stale.push(plan.oldKey);
      }
      if (livePlan !== null) {
        this.backend.write(livePlan.newKey, livePlan.text);
        written.push(livePlan.newKey);
        stale.push(liveKey);
      }
      this.backend.write(this.archiveIndexKey, index.text);
      this.backend.write(CHAIN_MANIFEST_NAME, chain.text);
      for (const key of stale) {
        try {
          this.backend.delete(key);
        } catch {
          // Best effort: the new files are all in place; a leftover old
          // file is orphaned data, never served (nothing references the
          // old name anymore).
        }
      }
    } catch (err) {
      for (const key of written) {
        try {
          this.backend.delete(key);
        } catch {
          // Best effort: the throw below still reports the original
          // failure, and the caller (the bus) aborts the rename.
        }
      }
      throw err;
    }
    // Phase C (in-memory): apply the planned chain mutations, then re-key
    // every topic-keyed index. Only runs after every file write
    // succeeded.
    for (let i = 0; i < segmentPlans.length; i++) {
      const plan = segmentPlans[i];
      const planned = plannedChain[i];
      plan.entry.topic = planned.topic;
      plan.entry.path = planned.path;
      plan.entry.sha256 = planned.sha256;
      plan.entry.prevHash = planned.prevHash;
      plan.entry.bytes = planned.bytes;
    }
    const filesRenamed = written.length;
    // --- All file work done; re-key the in-memory state. ---
    const moveValue = <K, V>(map: Map<K, V>, from: K, to: K): void => {
      if (map.has(from)) {
        map.set(to, map.get(from) as V);
        map.delete(from);
      }
    };
    this.topicsSeen.delete(oldTopic);
    this.topicsSeen.add(newTopic);
    moveValue(this.entryCounts, oldTopic, newTopic);
    moveValue(this.messageCounts, oldTopic, newTopic);
    moveValue(this.lastSeqs, oldTopic, newTopic);
    moveValue(this.maxPartitionSeqs, oldTopic, newTopic);
    moveValue(this.keyIndex, oldTopic, newTopic);
    // `maxKeySeqs` is keyed by message key, not topic — a cross-topic
    // publish-order space (mirrors the bus's keyCursors), so renaming one
    // topic must not touch it. Same reasoning as `purgeTopic`.
    moveValue(this.supersededCounts, oldTopic, newTopic);
    moveValue(this.segmentEntries, oldTopic, newTopic);
    moveValue(this.segmentStartSeq, oldTopic, newTopic);
    moveValue(this.segmentStartAt, oldTopic, newTopic);
    moveValue(this.topicBytes, oldTopic, newTopic);
    moveValue(this.archivesByTopic, oldTopic, newTopic);
    moveValue(this.chainTails, oldTopic, newTopic);
    for (const entry of this.archives) {
      if (entry.topic === oldTopic) {
        entry.topic = newTopic;
        if (entry.path.startsWith(segmentPrefix)) {
          entry.path = `${newSegmentPrefix}${entry.path.slice(segmentPrefix.length)}`;
        }
      }
    }
    for (const entry of this.chainEntries) {
      if (entry.topic === oldTopic) {
        entry.topic = newTopic;
        if (entry.path.startsWith(segmentPrefix)) {
          entry.path = `${newSegmentPrefix}${entry.path.slice(segmentPrefix.length)}`;
        }
      }
    }
    for (const path of [...this.checksumRejected]) {
      if (path.startsWith(segmentPrefix)) {
        this.checksumRejected.delete(path);
        this.checksumRejected.add(`${newSegmentPrefix}${path.slice(segmentPrefix.length)}`);
      }
    }
    // Offset checkpoints follow the topic: re-key the commits and rewrite
    // the journal from the re-keyed state (the same serialization the
    // compactor uses).
    let offsetCommitsMoved = 0;
    for (const [key, commit] of [...this.offsetCommits]) {
      if (commit.topic !== oldTopic) continue;
      const renamed: DurableOffsetCommit = { ...commit, topic: newTopic };
      const newKey = offsetKey(
        renamed.groupId,
        newTopic,
        renamed.partition,
        renamed.pattern,
        renamed.instanceId,
      );
      this.offsetCommits.delete(key);
      this.offsetCommits.set(newKey, renamed);
      offsetCommitsMoved += 1;
    }
    if (offsetCommitsMoved > 0) {
      const text = [...this.offsetCommits.values()]
        .map((c) => `${JSON.stringify(offsetLineOf(c))}\n`)
        .join('');
      try {
        this.backend.write(this.offsetKey, text);
      } catch {
        // Best effort, mirroring the compactors: the in-memory commits
        // are authoritative and a later compaction rewrites the journal.
      }
      this.offsetEntries = this.offsetCommits.size;
    }
    // Dedup journals follow the same pattern.
    let dedupEntriesMoved = 0;
    for (const [key, entry] of [...this.dedupSeen]) {
      if (entry.topic !== oldTopic) continue;
      const renamed: DedupEntry = { ...entry, topic: newTopic };
      this.dedupSeen.delete(key);
      this.dedupSeen.set(dedupKey(renamed.consumer, newTopic, renamed.messageId), renamed);
      dedupEntriesMoved += 1;
    }
    if (dedupEntriesMoved > 0) {
      const text = [...this.dedupSeen.values()].map((e) => `${JSON.stringify(dedupLineOf(e))}\n`).join('');
      try {
        this.backend.write(this.dedupKey, text);
      } catch {
        // Best effort, mirroring `compactDedup`.
      }
      this.dedupEntries = this.dedupSeen.size;
    }
    let publishDedupEntriesMoved = 0;
    for (const [key, entry] of [...this.publishDedupSeen]) {
      if (entry.topic !== oldTopic) continue;
      const renamed: PublishDedupEntry = { ...entry, topic: newTopic };
      this.publishDedupSeen.delete(key);
      this.publishDedupSeen.set(publishDedupKey(newTopic, renamed.messageId), renamed);
      publishDedupEntriesMoved += 1;
    }
    if (publishDedupEntriesMoved > 0) {
      const text = [...this.publishDedupSeen.values()]
        .map((e) => `${JSON.stringify(publishDedupLineOf(e))}\n`)
        .join('');
      try {
        this.backend.write(this.publishDedupKey, text);
      } catch {
        // Best effort, mirroring `compactDedup`.
      }
      this.publishDedupEntries = this.publishDedupSeen.size;
    }
    return {
      filesRenamed,
      indexLinesRewritten: index.rewritten,
      chainLinesRewritten: chain.rewritten,
      offsetCommitsMoved,
      dedupEntriesMoved,
      publishDedupEntriesMoved,
    };
  }

  /**
   * Forks one topic's live history into a brand-new topic (EB-224):
   * a one-time copy of the source topic's message records into the
   * target topic, after which the two topics evolve independently —
   * no ongoing sync, no shared sequence space, no alias (unlike the
   * EB-44 migration alias, which keeps routing, and unlike mirror,
   * which replicates across buses). The blue-green / topic-evolution
   * primitive: fork the live topic, point new consumers at the fork,
   * and let the fork's own TTL / schema / rate-limit / compression
   * rules (applied later through the bus's per-topic APIs) govern
   * everything published after the fork.
   *
   * Copy semantics, each deliberate:
   * - Only live message records (`seq >= 1`) copy. The seq-0
   *   families — delayed-delivery schedules, retained state, pause
   *   buffers, recurring jobs, key-TTL tombstones — are per-topic
   *   runtime state, not history: a fork that resurrected another
   *   topic's pending timers would double-fire them. Archived
   *   (rotated) segments do not copy either: the fork is the live
   *   log's snapshot, and cold segments stay replayable on the
   *   source via `readSince` with `{ includeArchived: true }`.
   * - The target gets its own sequence space: records are re-seqed
   *   from 1 per partition (the bus's per-(topic, partition)
   *   numbering, EB-90), in source seq order, so the fork reads as
   *   a topic that was always the fork. The source topic is
   *   untouched — its seq counters, files and indexes do not move.
   * - `fromSeq` is an exclusive lower bound on the *source* seq
   *   (records with `seq > fromSeq` copy), mirroring `readSince`'s
   *   `fromSeqExclusive`; `fromTime` keeps records with `at >
   *   fromTime`, mirroring `readSinceTime`'s exclusive bound. Both
   *   filters compose: a record copies only when it passes both.
   * - `keyedLatest` keeps, per key, only the latest record (by
   *   source seq) and drops superseded keyed records; keyless
   *   records always copy. This is the fork-time equivalent of the
   *   keyed-compaction read view, applied explicitly instead of
   *   depending on the log's `keyCompaction` setting — the fork's
   *   contents are the same whether or not the log compacts.
   * - Everything else about a record travels with it: payload,
   *   headers, messageId, publisherId, eventTime, partition, lane,
   *   key and keySeq, causal clock and expiry deadline. `keySeq`
   *   is preserved, not renumbered: it is global per key across
   *   topics (the cross-topic publish order), and the bus cursor
   *   already covers the source's range, so a preserved keySeq can
   *   never collide with a future publish.
   * - `signature` / `publisherSignature` do NOT travel: both bind
   *   (topic, seq, wire bytes) (EB-84 / EB-97), and a re-seqed copy
   *   under a new topic is a different signed statement. A forked
   *   record that carried the source's signature would fail
   *   verification on the target — worse, it would look verified
   *   until checked. Forked records are unsigned; a verifier on the
   *   fork sees exactly that.
   *
   * Fail-closed, before any copy: an unknown source (no live log),
   * an already-known target, a target with stray log files on disk,
   * or an invalid option all throw `RangeError` with the target
   * untouched. A failed append mid-copy purges the partial target
   * (best effort) and throws, so a fork never leaves a half-copied
   * topic behind: the result is all copied records or no fork.
   */
  forkTopic(
    sourceTopic: string,
    targetTopic: string,
    opts?: ForkTopicOptions,
  ): ForkTopicResult {
    const o = opts ?? {};
    if (!this.topicsSeen.has(sourceTopic)) {
      throw new RangeError(`forkTopic: unknown source topic "${sourceTopic}"`);
    }
    if (this.topicsSeen.has(targetTopic)) {
      throw new RangeError(`forkTopic: target topic "${targetTopic}" is already in use`);
    }
    if (
      o.fromSeq !== undefined &&
      (!Number.isInteger(o.fromSeq) || o.fromSeq < 0)
    ) {
      throw new RangeError('forkTopic: fromSeq must be a non-negative integer');
    }
    if (
      o.fromTime !== undefined &&
      (typeof o.fromTime !== 'number' || !Number.isFinite(o.fromTime) || o.fromTime < 0)
    ) {
      throw new RangeError('forkTopic: fromTime must be a non-negative finite number');
    }
    const targetEnc = encodeURIComponent(targetTopic);
    const targetLiveKey = `${targetEnc}.log`;
    const targetSegmentPrefix = `${targetEnc}.seg-`;
    const keys = this.backend.list();
    if (keys.includes(targetLiveKey) || keys.some((k) => k.startsWith(targetSegmentPrefix))) {
      throw new RangeError(`forkTopic: target topic "${targetTopic}" already has log files`);
    }
    const fromSeq = o.fromSeq ?? 0;
    const sourceLastSeq = this.lastSeq(sourceTopic);
    // Raw live records in source seq order (not the `readSince`
    // view: that view applies the log's key-compaction dedup, which
    // would silently change the fork's contents with the log's
    // compaction setting — the fork's filters below are explicit).
    const sourceRecords = this.readFileRecordsSince(sourceTopic, fromSeq).filter(
      (rec) => rec.seq >= 1,
    );
    let selected = sourceRecords;
    if (o.fromTime !== undefined) {
      const fromTime = o.fromTime;
      selected = selected.filter((rec) => rec.at > fromTime);
    }
    if (o.keyedLatest === true) {
      const latestByKey = new Map<string, DurableLogRecord>();
      for (const rec of selected) {
        if (rec.key !== undefined) latestByKey.set(rec.key, rec);
      }
      if (latestByKey.size > 0) {
        selected = selected.filter(
          (rec) => rec.key === undefined || latestByKey.get(rec.key) === rec,
        );
      }
    }
    // Re-seq per partition from 1, in source seq order.
    const nextSeqByPartition = new Map<number, number>();
    let copied = 0;
    try {
      for (const rec of selected) {
        const partition = rec.partition ?? 0;
        const seq = (nextSeqByPartition.get(partition) ?? 0) + 1;
        nextSeqByPartition.set(partition, seq);
        const { signature, publisherSignature, ...rest } = rec;
        void signature;
        void publisherSignature;
        const ok = this.append({ ...rest, topic: targetTopic, seq });
        if (!ok) {
          throw new Error(`forkTopic: failed to append record seq ${rec.seq} to "${targetTopic}"`);
        }
        copied += 1;
      }
    } catch (err) {
      // All-or-nothing: a partial fork is worse than no fork — a
      // consumer that found half a history would trust it. Purge
      // best effort; the throw below is the signal regardless.
      try {
        this.purgeTopic(targetTopic);
      } catch {
        // The purge itself failed; the original error still reports.
      }
      throw err;
    }
    return {
      sourceTopic,
      targetTopic,
      copied,
      sourceLastSeq,
      targetLastSeq: this.lastSeq(targetTopic),
      fromSeq: o.fromSeq,
      fromTime: o.fromTime,
      keyedLatest: o.keyedLatest ?? false,
    };
  }

  /**
   * Merges two topics' live histories into a new topic (EB-226):
   * `mergeTopics('orders.eu', 'orders.us', 'orders.all')` copies
   * both sources' live messages into the target with a fresh
   * per-partition sequence space, ordered by publish timestamp
   * (ties break toward the first source, then source seq — the
   * merge is stable, so neither source's internal order ever
   * inverts). The sources are untouched: this method only reads
   * them — tombstoning them is the bus's job (`EventBus.
   * mergeTopics`), which is why the merge, unlike the fork, has
   * no filter options: a merge is a whole-history union, and a
   * filtered merge would silently drop one source's tail.
   * Signatures are dropped like the fork's (a merged record is a
   * new record in a new topic, not a re-attestation of the old
   * one), seq-0 families and archived segments are excluded like
   * the fork's, and a failed merge purges the partial target —
   * a consumer that found half a merged history would trust it.
   */
  mergeTopics(
    sourceA: string,
    sourceB: string,
    targetTopic: string,
  ): MergeTopicsResult {
    if (!this.topicsSeen.has(sourceA)) {
      throw new RangeError(`mergeTopics: unknown source topic "${sourceA}"`);
    }
    if (!this.topicsSeen.has(sourceB)) {
      throw new RangeError(`mergeTopics: unknown source topic "${sourceB}"`);
    }
    if (this.topicsSeen.has(targetTopic)) {
      throw new RangeError(`mergeTopics: target topic "${targetTopic}" is already in use`);
    }
    const targetEnc = encodeURIComponent(targetTopic);
    const targetLiveKey = `${targetEnc}.log`;
    const targetSegmentPrefix = `${targetEnc}.seg-`;
    const keys = this.backend.list();
    if (keys.includes(targetLiveKey) || keys.some((k) => k.startsWith(targetSegmentPrefix))) {
      throw new RangeError(`mergeTopics: target topic "${targetTopic}" already has log files`);
    }
    // Raw live records in source seq order (not the `readSince`
    // view: that view applies the log's key-compaction dedup, which
    // would silently change the merge's contents with the log's
    // compaction setting — the merge's order below is explicit).
    // The first source's records come first so the stable sort
    // below breaks timestamp ties toward it.
    const recordsA = this.readFileRecordsSince(sourceA, 0).filter(
      (rec) => rec.seq >= 1,
    );
    const recordsB = this.readFileRecordsSince(sourceB, 0).filter(
      (rec) => rec.seq >= 1,
    );
    const merged = [...recordsA, ...recordsB].sort((x, y) => x.at - y.at);
    const sourceALastSeq = this.lastSeq(sourceA);
    const sourceBLastSeq = this.lastSeq(sourceB);
    // Re-seq per partition from 1, in merged timestamp order.
    const nextSeqByPartition = new Map<number, number>();
    let copied = 0;
    try {
      for (const rec of merged) {
        const partition = rec.partition ?? 0;
        const seq = (nextSeqByPartition.get(partition) ?? 0) + 1;
        nextSeqByPartition.set(partition, seq);
        const { signature, publisherSignature, ...rest } = rec;
        void signature;
        void publisherSignature;
        const ok = this.append({ ...rest, topic: targetTopic, seq });
        if (!ok) {
          throw new Error(`mergeTopics: failed to append record seq ${rec.seq} to "${targetTopic}"`);
        }
        copied += 1;
      }
    } catch (err) {
      // All-or-nothing, like the fork: purge best effort; the
      // throw below is the signal regardless.
      try {
        this.purgeTopic(targetTopic);
      } catch {
        // The purge itself failed; the original error still reports.
      }
      throw err;
    }
    return {
      sourceTopics: [sourceA, sourceB],
      targetTopic,
      copied,
      sourceLastSeqs: [sourceALastSeq, sourceBLastSeq],
      targetLastSeq: this.lastSeq(targetTopic),
    };
  }

  /**
   * Splits one topic's live history into two new topics by a single
   * predicate (EB-227): `splitTopics('events', isError, 'events.err',
   * 'events.ok')` copies every live record exactly once — matches to
   * the first target, non-matches to the second — so the split is
   * lossless and disjoint by construction, with no record dropped
   * and none duplicated. This is the one-to-two counterpart of
   * `mergeTopics` (two-to-one) and the partitioning counterpart of
   * `forkTopic` (one-to-one copy, source stays live): unlike the
   * fork, a split retires its source — tombstoning it is the bus's
   * job (`EventBus.splitTopic`), mirroring the merge — and unlike
   * EB-90's in-topic partitions, the split produces two independent
   * topics, each with its own sequence space.
   *
   * Each target is re-seqed per partition from 1, in source seq
   * order, so each target reads as a topic that was always that
   * target. Message metadata travels like the fork's (payload,
   * headers, messageId, publisherId, eventTime, partition, lane,
   * key and keySeq, causal clock, expiry deadline); signatures
   * are dropped like the fork's (a re-seqed copy under a new topic
   * is a different signed statement). Every copied record is
   * stamped `splitSource` with the source topic — the split's
   * provenance record on the record itself, not just in the audit
   * log. Seq-0 families and archived segments are excluded like
   * the fork's.
   *
   * Fail-closed, before any copy: an unknown source, an
   * already-known target, a target with stray log files on disk,
   * duplicate names, or a non-function predicate all throw
   * `RangeError` with both targets untouched. The predicate runs
   * over the whole history before the first append, so a throwing
   * predicate also leaves both targets untouched; a failed append
   * mid-copy purges both partial targets, like the fork — a split
   * is all copied records or no split.
   */
  splitTopics(
    sourceTopic: string,
    predicate: SplitTopicPredicate,
    targetA: string,
    targetB: string,
  ): SplitTopicsResult {
    if (!this.topicsSeen.has(sourceTopic)) {
      throw new RangeError(`splitTopics: unknown source topic "${sourceTopic}"`);
    }
    if (typeof predicate !== 'function') {
      throw new RangeError('splitTopics: predicate must be a function');
    }
    if (sourceTopic === targetA || sourceTopic === targetB) {
      throw new RangeError('splitTopics: target topics must differ from the source topic');
    }
    if (targetA === targetB) {
      throw new RangeError('splitTopics: target topics must differ');
    }
    if (this.topicsSeen.has(targetA)) {
      throw new RangeError(`splitTopics: target topic "${targetA}" is already in use`);
    }
    if (this.topicsSeen.has(targetB)) {
      throw new RangeError(`splitTopics: target topic "${targetB}" is already in use`);
    }
    const keys = this.backend.list();
    for (const targetTopic of [targetA, targetB]) {
      const targetEnc = encodeURIComponent(targetTopic);
      if (
        keys.includes(`${targetEnc}.log`) ||
        keys.some((k) => k.startsWith(`${targetEnc}.seg-`))
      ) {
        throw new RangeError(`splitTopics: target topic "${targetTopic}" already has log files`);
      }
    }
    // Raw live records in source seq order (not the `readSince`
    // view, for the same key-compaction reason as the fork's).
    const sourceRecords = this.readFileRecordsSince(sourceTopic, 0).filter(
      (rec) => rec.seq >= 1,
    );
    const sourceLastSeq = this.lastSeq(sourceTopic);
    // Partition first, copy second: the predicate is evaluated
    // exactly once per record and any throw aborts before either
    // target gains a record — a half-split pair would be worse
    // than no split, because the two targets must jointly cover
    // the source exactly once.
    const recordsA: DurableLogRecord[] = [];
    const recordsB: DurableLogRecord[] = [];
    for (const rec of sourceRecords) {
      if (predicate(rec)) recordsA.push(rec);
      else recordsB.push(rec);
    }
    let copiedA = 0;
    let copiedB = 0;
    try {
      for (const [records, targetTopic] of [
        [recordsA, targetA],
        [recordsB, targetB],
      ] as const) {
        const nextSeqByPartition = new Map<number, number>();
        for (const rec of records) {
          const partition = rec.partition ?? 0;
          const seq = (nextSeqByPartition.get(partition) ?? 0) + 1;
          nextSeqByPartition.set(partition, seq);
          const { signature, publisherSignature, ...rest } = rec;
          void signature;
          void publisherSignature;
          const ok = this.append({
            ...rest,
            topic: targetTopic,
            seq,
            splitSource: sourceTopic,
          });
          if (!ok) {
            throw new Error(`splitTopics: failed to append record seq ${rec.seq} to "${targetTopic}"`);
          }
          if (targetTopic === targetA) copiedA += 1;
          else copiedB += 1;
        }
      }
    } catch (err) {
      // All-or-nothing across both targets, like the fork's purge.
      for (const targetTopic of [targetA, targetB]) {
        try {
          this.purgeTopic(targetTopic);
        } catch {
          // The purge itself failed; the original error still reports.
        }
      }
      throw err;
    }
    return {
      sourceTopic,
      targetA,
      targetB,
      copiedA,
      copiedB,
      copied: copiedA + copiedB,
      sourceLastSeq,
      targetALastSeq: this.lastSeq(targetA),
      targetBLastSeq: this.lastSeq(targetB),
    };
  }

  /**
   * Scans the offset journal and rebuilds the in-memory checkpoint index:
   * the highest committed seq per (group, topic) wins, so a restarted bus
   * resumes from where its consumers committed, not from zero. Corrupt
   * lines are counted and skipped; a corrupt journal never prevents the
   * log from opening.
   */
  private recoverOffsets(): void {
    const text = this.readText(this.offsetKey);
    if (text === undefined) return;
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      this.offsetEntries += 1;
      const commit = parseOffsetLine(line);
      if (commit == null) {
        this.corruptLines += 1;
        continue;
      }
      const key = offsetKey(
        commit.groupId,
        commit.topic,
        commit.partition,
        commit.pattern,
        commit.instanceId,
      );
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
          ...(commit.instanceId === undefined ? {} : { instanceId: commit.instanceId }),
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
    opts?: { partition?: number; pattern?: string; instanceId?: string },
  ): boolean {
    const commit: DurableOffsetCommit = { groupId, topic, seq, at, ...opts };
    let line: string;
    try {
      line = `${JSON.stringify(offsetLineOf(commit))}\n`;
    } catch {
      return false;
    }
    try {
      this.backend.append(this.offsetKey, line);
    } catch {
      return false;
    }
    this.offsetEntries += 1;
    const key = offsetKey(groupId, topic, opts?.partition, opts?.pattern, opts?.instanceId);
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
      this.backend.write(this.offsetKey, text);
    } catch {
      return;
    }
    this.offsetEntries = this.offsetCommits.size;
  }

  /**
   * Committed offsets recovered from the offset journal: the highest
   * committed seq per checkpoint, keyed by the NUL-joined checkpoint key
   * — `${groupId}\0${topic}` for group-level commits,
   * `${groupId}\0${topic}\0${partition}\0${pattern}` for per-partition
   * commits, `${groupId}\0${topic}\0instance:${instanceId}` for
   * per-static-member commits (EB-104), and
   * `${groupId}\0${topic}\0${partition}\0${pattern}\0instance:${instanceId}`
   * for per-partition per-member commits (see `offsetKey`).
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
    const text = this.readText(this.dedupKey);
    if (text === undefined) return;
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
      this.backend.append(this.dedupKey, line);
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
      this.backend.write(this.dedupKey, text);
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
    const text = this.readText(this.publishDedupKey);
    if (text === undefined) return;
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
      this.backend.append(this.publishDedupKey, line);
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
      this.backend.write(this.publishDedupKey, text);
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
    /**
     * EB-221: keys evicted by per-key TTL (`setKeyTtl`) across
     * compactions. 0 when no key TTL is configured. Counts keys, not
     * records — one evicted key drops its latest record plus its
     * superseded dead weight and leaves one tombstone.
     */
    keyTtlEvicted: number;
    /** Archive files that failed to decompress during archived reads. */
    corruptArchives: number;
    /**
     * EB-94: archived segments isolated by checksum verification — a
     * sidecar mismatch (or unreadable bytes with a sidecar present),
     * during recovery or archived reads. Skipped, never fatal.
     */
    corruptSegments: number;
    /** Total entries across all archived segments. */
    archived: number;
    /**
     * EB-119: live-segment bytes across all topics — the actual on-disk
     * line bytes (the budget's scope). Archived segments are history and
     * are not counted.
     */
    bytes: number;
    /**
     * EB-119: the configured per-topic byte budget (`maxBytesPerTopic`).
     * Absent when no byte bound is configured.
     */
    bytesLimit?: number;
    /**
     * Per-topic segment view: live entries in the current segment,
     * closed archived segment count, and archived entries — in
     * first-seen order.
     */
    segments: Record<string, { live: number; segments: number; archived: number }>;
    /**
     * EB-163: archived segments successfully uploaded to the remote.
     * 0 when offload is disabled.
     */
    offloadUploaded: number;
    /**
     * EB-163: offload attempts that failed (segment upload or manifest
     * write). 0 when offload is disabled.
     */
    offloadFailed: number;
    /**
     * EB-218: remote segment pulls (readbacks) that reached S3 — a
     * locally-trimmed segment pulled down during replay. Counts cache
     * misses only; cache hits are counted separately. 0 when offload
     * is disabled.
     */
    s3Readbacks: number;
    /**
     * EB-218: total bytes downloaded by remote segment pulls. 0 when
     * offload is disabled.
     */
    s3ReadbackBytes: number;
    /**
     * EB-218: readback reads served from the local readback cache —
     * no S3 request. 0 when offload is disabled.
     */
    s3ReadbackCacheHits: number;
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
    // EB-119: live bytes across topics — the budget's scope.
    let bytes = 0;
    for (const b of this.topicBytes.values()) bytes += b;
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
      keyTtlEvicted: this.keyTtlEvicted,
      corruptArchives: this.corruptArchives,
      corruptSegments: this.corruptSegments,
      archived,
      segments,
      bytes,
      offloadUploaded: this.offloadUploaded,
      offloadFailed: this.offloadFailed,
      s3Readbacks: this.s3Readbacks,
      s3ReadbackBytes: this.s3ReadbackBytes,
      s3ReadbackCacheHits: this.s3ReadbackCacheHits,
      ...(this.maxBytesPerTopic === undefined ? {} : { bytesLimit: this.maxBytesPerTopic }),
    };
  }

  /**
   * EB-118: offline chain verification over a log directory — no log
   * instance needed. Reads `__segment_chain.jsonl` and, per topic in
   * manifest order, checks that every line's `prevHash` links to the
   * previous segment's digest (the first links to the genesis marker),
   * that every named segment file exists, and that its bytes still hash
   * to the manifest digest. Segment files on disk with no manifest line
   * are reported as rogue. A directory with no manifest (no archives, or
   * archives that predate the chain and were never healed) verifies as an
   * empty chain — backward compatible by construction.
   *
   * The chain hash is recomputed from the archive bytes here — the
   * verifier does not trust the EB-94 sidecar, which is a separate
   * integrity layer with its own lifecycle.
   *
   * Cost: O(segments) manifest lines plus one full byte read per archived
   * segment — I/O proportional to archived data. An offline/operator
   * tool, never a hot-path check.
   */
  static verifySegmentChain(dir: string): SegmentChainReport {
    const backend = new FilesystemStorageBackend(dir);
    const issues: SegmentChainIssue[] = [];
    let text: string | undefined;
    try {
      const bytes = backend.read(CHAIN_MANIFEST_NAME);
      text = bytes === undefined ? undefined : Buffer.from(bytes).toString('utf8');
    } catch {
      text = undefined;
    }
    const entries: SegmentChainEntry[] = [];
    if (text !== undefined) {
      let lineNo = 0;
      for (const line of text.split('\n')) {
        lineNo += 1;
        if (line.length === 0) continue;
        const entry = parseChainManifestLine(line);
        if (entry == null) {
          // A corrupt manifest line breaks the chain exactly like a
          // reordered one: the linkage from here on is unverifiable.
          issues.push({
            topic: '',
            segmentId: '',
            path: CHAIN_MANIFEST_NAME,
            kind: 'link',
            detail: `corrupt chain manifest line ${lineNo}`,
          });
          continue;
        }
        entries.push(entry);
      }
    }
    const expectedPrev = new Map<string, string>();
    const knownPaths = new Set<string>();
    for (const entry of entries) {
      const want = expectedPrev.get(entry.topic) ?? SEGMENT_CHAIN_GENESIS_PREV;
      if (entry.prevHash !== want) {
        issues.push({
          topic: entry.topic,
          segmentId: entry.segmentId,
          path: entry.path,
          kind: 'link',
          detail:
            `prevHash ${entry.prevHash} does not link to the previous segment's ` +
            `digest ${want} — the manifest was reordered or the line was forged`,
        });
      }
      knownPaths.add(entry.path);
      let raw: Buffer | undefined;
      let unreadable = false;
      try {
        const bytes = backend.read(entry.path);
        raw = bytes === undefined ? undefined : Buffer.from(bytes);
      } catch {
        unreadable = true;
      }
      if (unreadable) {
        issues.push({
          topic: entry.topic,
          segmentId: entry.segmentId,
          path: entry.path,
          kind: 'unreadable',
          detail: 'the segment file exists but its bytes cannot be read',
        });
      } else if (raw === undefined) {
        issues.push({
          topic: entry.topic,
          segmentId: entry.segmentId,
          path: entry.path,
          kind: 'missing',
          detail: 'the manifest names a segment file that is gone from the log directory',
        });
      } else {
        const actual = createHash('sha256').update(raw).digest('hex');
        if (actual !== entry.sha256) {
          issues.push({
            topic: entry.topic,
            segmentId: entry.segmentId,
            path: entry.path,
            kind: 'hash',
            detail:
              `segment bytes hash ${actual} != manifest digest ${entry.sha256} — ` +
              `the segment was replaced or tampered with`,
          });
        }
      }
      expectedPrev.set(entry.topic, entry.sha256);
    }
    // Rogue segments: archived on disk but never chained.
    let names: string[];
    try {
      names = backend.list();
    } catch {
      names = [];
    }
    for (const name of names) {
      const parsed = parseSegmentFileName(name);
      if (parsed == null) continue;
      if (!knownPaths.has(name)) {
        issues.push({
          topic: parsed.topic,
          segmentId: `${parsed.seqStart}-${parsed.seqEnd}`,
          path: name,
          kind: 'rogue',
          detail: 'an archived segment on disk has no chain manifest line',
        });
      }
    }
    return { ok: issues.length === 0, segments: entries.length, issues };
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
  if (record.ttlMs !== undefined) line.ttlMs = record.ttlMs;
  if (record.deliverAt !== undefined) line.deliverAt = record.deliverAt;
  if (record.delayId !== undefined) line.delayId = record.delayId;
  if (record.cancelled !== undefined) line.cancelled = record.cancelled;
  if (record.recurring !== undefined) line.recurring = record.recurring;
  if (record.recurringJobId !== undefined) line.recurringJobId = record.recurringJobId;
  if (record.cronExpr !== undefined) line.cronExpr = record.cronExpr;
  if (record.recurringTrigger !== undefined) line.recurringTrigger = record.recurringTrigger;
  if (record.retained !== undefined) line.retained = record.retained;
  if (record.retainedSeq !== undefined) line.retainedSeq = record.retainedSeq;
  if (record.paused !== undefined) line.paused = record.paused;
  if (record.pauseId !== undefined) line.pauseId = record.pauseId;
  if (record.pauseOrder !== undefined) line.pauseOrder = record.pauseOrder;
  if (record.schemaVersion !== undefined) line.schemaVersion = record.schemaVersion;
  if (record.key !== undefined) line.key = record.key;
  if (record.keySeq !== undefined) line.keySeq = record.keySeq;
  if (record.keyTtlEvicted !== undefined) line.keyTtlEvicted = record.keyTtlEvicted;
  if (record.partition !== undefined) line.partition = record.partition;
  if (record.dictId !== undefined) line.dictId = record.dictId;
  if (record.messageId !== undefined) line.messageId = record.messageId;
  if (record.eventTime !== undefined) line.eventTime = record.eventTime;
  if (record.causal !== undefined) line.causal = { source: record.causal.source, clock: record.causal.clock };
  if (record.publisherId !== undefined) line.publisherId = record.publisherId;
  // The application headers (EB-153) ride the record so replay restores
  // them onto the envelope. Copied defensively — the caller may reuse
  // the record object; the log line must be immutable.
  if (record.headers !== undefined) line.headers = { ...record.headers };
  if (record.signature !== undefined)
    line.signature = { kid: record.signature.kid, sig: record.signature.sig };
  if (record.publisherSignature !== undefined)
    line.publisherSignature = { kid: record.publisherSignature.kid, sig: record.publisherSignature.sig };
  if (record.splitSource !== undefined) line.splitSource = record.splitSource;
  // The priority lane (EB-122): written only for non-normal lanes —
  // absent means `normal`, so pre-lane lines stay byte-identical.
  if (record.lane === 'urgent' || record.lane === 'bulk') line.lane = record.lane;
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
 * `seq` 0 is also reserved for recurring-job records (`recurring: true`,
 * EB-124): a schedule record carries the job's identity (`recurringJobId`,
 * a non-empty string) and its cron expression (`cronExpr`, a non-empty
 * string); a trigger record (`recurringTrigger: true`) is per-fire audit
 * state and also carries `cronExpr`; a tombstone (`cancelled: true`)
 * carries only `recurringJobId` and closes the job. `recurring` is only
 * valid with `seq` 0, and a recurring record never carries `delayId`,
 * `retained`, or `paused` — the four seq-0 families are disjoint by
 * construction.
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
  const isRecurringRecord = o['recurring'] === true;
  const isKeyTtlTombstone = o['keyTtlEvicted'] === true;
  if (isRetainedRecord && !isScheduleRecord) return null;
  if (isPauseRecord && !isScheduleRecord) return null;
  if (isRetainedRecord && isPauseRecord) return null;
  if (isRecurringRecord && !isScheduleRecord) return null;
  if (isRecurringRecord && (isRetainedRecord || isPauseRecord)) return null;
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
    } else if (isRecurringRecord) {
      // Recurring-job records carry the job's identity. A schedule record
      // and a trigger record carry the cron expression; a tombstone
      // carries neither (and a trigger is never a tombstone). A recurring
      // record never carries `delayId` — the families are disjoint by
      // construction.
      if (typeof o['recurringJobId'] !== 'string' || (o['recurringJobId'] as string).length === 0) {
        return null;
      }
      if (o['delayId'] !== undefined) return null;
      const isTrigger = o['recurringTrigger'] === true;
      const isTombstone = o['cancelled'] === true;
      if (isTrigger && isTombstone) return null;
      if (!isTombstone) {
        if (typeof o['cronExpr'] !== 'string' || (o['cronExpr'] as string).length === 0) return null;
      } else if (o['cronExpr'] !== undefined || o['recurringTrigger'] !== undefined) {
        return null;
      }
    } else if (isKeyTtlTombstone) {
      // Key-TTL tombstone (EB-221): a seq-0 marker compaction leaves for
      // an evicted key, carrying the key's seq baseline so restart
      // recovery never renumbers the key. It carries no timer intent —
      // no delayId, no deliverAt, no cancellation — and it never mixes
      // with the retained/pause/recurring families (those branches ran
      // first). A tombstone without its key retains nothing, so the key
      // is required here (the later key validation re-checks it).
      if (
        typeof o['key'] !== 'string' ||
        (o['key'] as string).length === 0 ||
        o['delayId'] !== undefined ||
        o['deliverAt'] !== undefined ||
        o['cancelled'] !== undefined
      ) {
        return null;
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
  if (o['ttlMs'] !== undefined) {
    // The writer only ever persists a validated per-message TTL; a
    // non-finite or negative value on disk means the line was tampered
    // with or miswritten — reject the line rather than silently
    // resurrecting or mis-timing a schedule.
    if (typeof o['ttlMs'] !== 'number' || !Number.isFinite(o['ttlMs']) || o['ttlMs'] < 0) return null;
    record.ttlMs = o['ttlMs'] as number;
  }
  if (o['delayId'] !== undefined) {
    if (typeof o['delayId'] !== 'string' || (o['delayId'] as string).length === 0) return null;
    record.delayId = o['delayId'] as string;
  }
  if (o['recurring'] !== undefined) {
    // The bus only ever writes `recurring: true` on seq-0 recurring-job
    // records; any other value means the line was tampered with or
    // miswritten.
    if (o['recurring'] !== true || !isRecurringRecord) return null;
    record.recurring = true;
  }
  if (o['recurringJobId'] !== undefined) {
    // Validated above: present and non-empty exactly on recurring-job
    // records — a stray identity on any other line is corruption.
    if (!isRecurringRecord) return null;
    record.recurringJobId = o['recurringJobId'] as string;
  }
  if (o['cronExpr'] !== undefined) {
    // Validated above: a non-empty cron expression on schedule and
    // trigger records only — anywhere else is corruption.
    if (!isRecurringRecord) return null;
    record.cronExpr = o['cronExpr'] as string;
  }
  if (o['recurringTrigger'] !== undefined) {
    // The bus only ever writes `recurringTrigger: true` on seq-0
    // recurring trigger records.
    if (o['recurringTrigger'] !== true || !isRecurringRecord) return null;
    record.recurringTrigger = true;
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
    // Version 0 is legal (EB-86 registers non-negative integers), so the
    // floor is 0, not 1 — a version-0 record must survive a restart.
    if (!Number.isInteger(o['schemaVersion']) || (o['schemaVersion'] as number) < 0) return null;
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
  if (o['keyTtlEvicted'] !== undefined) {
    // The bus only ever writes `keyTtlEvicted: true` on seq-0 tombstone
    // records (EB-221); any other value — or the flag on a real message
    // or on a retained/pause/recurring record — means the line was
    // tampered with or miswritten.
    if (
      o['keyTtlEvicted'] !== true ||
      !isScheduleRecord ||
      isRetainedRecord ||
      isPauseRecord ||
      isRecurringRecord
    ) {
      return null;
    }
    record.keyTtlEvicted = true;
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
  if (o['headers'] !== undefined) {
    // The application headers (EB-153) are routing metadata: the bus
    // only ever writes a string-to-string map, so a malformed value
    // means the line was tampered with or miswritten — the line is
    // corrupt, never silently headerless. Copied defensively so the
    // returned record cannot alias the parsed line object.
    const headers = o['headers'] as Record<string, unknown>;
    if (typeof headers !== 'object' || headers === null || Array.isArray(headers)) {
      return null;
    }
    const copy: Record<string, string> = {};
    for (const [key, value] of Object.entries(headers)) {
      if (typeof value !== 'string') return null;
      copy[key] = value;
    }
    record.headers = copy;
  }
  if (o['lane'] !== undefined) {
    // The priority lane (EB-122): the bus only ever writes 'urgent' or
    // 'bulk' — a malformed value means the line was tampered with or
    // miswritten, so the line is corrupt. A 'normal' value (never
    // written) is accepted leniently and dropped: absent means `normal`.
    const lane = o['lane'];
    if (lane !== 'urgent' && lane !== 'normal' && lane !== 'bulk') return null;
    if (lane !== 'normal') record.lane = lane;
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
  if (o['splitSource'] !== undefined) {
    // Split provenance (EB-227) is advisory metadata like `messageId`:
    // a malformed value is dropped (the record replays without
    // provenance) rather than failing the whole line.
    if (typeof o['splitSource'] === 'string' && (o['splitSource'] as string).length > 0) {
      record.splitSource = o['splitSource'] as string;
    }
  }
  if (o['publisherSignature'] !== undefined) {
    // Same integrity-metadata rule as `signature`: a malformed value
    // means tampering or miswriting — the line is corrupt, never
    // silently unsigned.
    const publisherSignature = o['publisherSignature'] as Record<string, unknown>;
    if (
      typeof publisherSignature !== 'object' ||
      publisherSignature === null ||
      typeof publisherSignature['kid'] !== 'string' ||
      (publisherSignature['kid'] as string).length === 0 ||
      typeof publisherSignature['sig'] !== 'string' ||
      !/^[0-9a-f]{64}$/.test(publisherSignature['sig'] as string)
    ) {
      return null;
    }
    record.publisherSignature = {
      kid: publisherSignature['kid'] as string,
      sig: publisherSignature['sig'] as string,
    };
  }
  // A non-tombstone delayed-delivery schedule record must say when it is
  // due. Retained-message records (EB-87), pause-buffer records (EB-88),
  // and recurring-job records (EB-124) are the other seq-0 families and
  // carry no due time — only their own identity fields, validated above.
  // Key-TTL tombstones (EB-221) are a further due-less family: they carry
  // a key and its seq baseline, never a delivery time.
  if (
    isScheduleRecord &&
    !isRetainedRecord &&
    !isPauseRecord &&
    !isRecurringRecord &&
    !isKeyTtlTombstone &&
    record.cancelled !== true &&
    record.deliverAt === undefined
  ) {
    return null;
  }
  return record;
}
