import type { DurableTopicLog } from './durablelog.ts';

/**
 * Topic data export/import (EB-228): the durable-log history of a
 * topic as portable JSONL — the migration primitive, not a live
 * replication channel. Export snapshots one topic's live message
 * history into lines a file, an object store, or another bus can
 * hold; import replays those lines into a brand-new topic on any
 * bus with a durable log, so a topic's history can move between
 * buses, environments, or names without either bus staying up.
 *
 * This is distinct from the neighbouring history primitives, and
 * from the snapshot exports it is easy to confuse it with:
 * - EB-224 fork / EB-226 merge / EB-227 split copy history
 *   between topics of the SAME log, in-process, with full record
 *   metadata; export/import serializes history to a portable
 *   format and replays it elsewhere, with only the four fields a
 *   migration needs.
 * - EB-82 exports a subscription snapshot (who is subscribed,
 *   offsets); this exports the topic's MESSAGE history.
 * - EB-148 is the pull read path (live consumption); this is a
 *   landable data format, readable without a bus.
 * - EB-91 snapshots configuration; this snapshots messages.
 * - EB-163 stores sealed segments remotely; this is the
 *   export/import interface over the live log, not a storage
 *   backend.
 *
 * Format: one JSON object per line, exactly four fields —
 * `seq` (the source topic's per-topic sequence number, for
 * ordering and provenance only — import renumbers from 1),
 * `payload` (the stored JSON payload, verbatim), `key` (the
 * compaction key, when the record carried one), and `timestamp`
 * (the record's publish timestamp in milliseconds, the log's
 * `at`). No signature fields, ever: `signature` /
 * `publisherSignature` (EB-84 / EB-97) bind (topic, seq, wire
 * bytes), and an imported record is a new record under a new
 * topic with a new seq — a migrated signature would fail
 * verification on the target, or worse, look verified until
 * checked. Imported records are unsigned, following the
 * EB-226/EB-227 convention of not migrating signatures.
 *
 * Fidelity notes, each deliberate:
 * - Live message records only (`seq >= 1`): the seq-0 families
 *   (delayed-delivery schedules, retained state, pause buffers,
 *   recurring jobs, key-TTL tombstones) are per-topic runtime
 *   state, not history — importing them would resurrect another
 *   topic's pending timers. Archived (rotated) segments are not
 *   exported either: the export is the live log's snapshot, like
 *   the fork's.
 * - The export reads the log's RAW live records (see
 *   `DurableTopicLog.readLiveRecords`), not the `readSince`
 *   view: under keyed compaction the read view drops superseded
 *   keyed values, and an export that silently dropped history
 *   would not be a migration.
 * - Payloads travel verbatim as stored: a compression envelope
 *   travels as its JSON object, not decompressed — the format
 *   carries JSON, not wire bytes. Binary records (EB-92) cannot
 *   be represented in JSONL at all, so an export that meets one
 *   fails closed with `RangeError` instead of emitting a mangled
 *   payload: a half-faithful export is worse than no export.
 * - Unknown fields on an imported record are ignored: only the
 *   four format fields migrate, so a record hand-edited with
 *   extra metadata imports cleanly — but a record that fails the
 *   type checks below is bad, and one bad record rejects the
 *   whole batch before anything is written.
 */

/** One exported topic-data line: the EB-228 JSONL record shape. */
export interface TopicDataRecord {
  /** Source topic's per-topic sequence number (provenance only). */
  seq: number;
  /** The stored JSON payload, verbatim. */
  payload: unknown;
  /** Compaction key, present exactly when the record carried one. */
  key?: string;
  /** Publish timestamp in milliseconds (the log record's `at`). */
  timestamp: number;
}

/** Result of [`importTopicData`]. */
export interface ImportTopicDataResult {
  /** The new topic the records landed in. */
  targetTopic: string;
  /** Records imported (== the batch size; import is all-or-nothing). */
  imported: number;
  /** The target's last sequence number after import (== `imported`). */
  targetLastSeq: number;
}

/**
 * Validates one parsed record object. Returns the record in
 * canonical form, or throws `RangeError` naming the defect — a
 * bad record is a batch-level failure, never a skipped line, so
 * the caller can validate the whole batch before writing
 * anything. `where` identifies the record for the error message
 * (a JSONL line number, or an array index).
 */
function validateTopicDataRecord(record: unknown, where: string): TopicDataRecord {
  const label = `importTopicData: record ${where}`;
  if (typeof record !== 'object' || record === null || Array.isArray(record)) {
    throw new RangeError(`${label} must be a JSON object`);
  }
  const o = record as Record<string, unknown>;
  const seq = o['seq'];
  if (!Number.isInteger(seq) || (seq as number) < 1) {
    throw new RangeError(`${label} has an invalid seq (must be a positive integer)`);
  }
  if (!Object.prototype.hasOwnProperty.call(o, 'payload')) {
    throw new RangeError(`${label} is missing its payload`);
  }
  const timestamp = o['timestamp'];
  if (typeof timestamp !== 'number' || !Number.isFinite(timestamp) || timestamp < 0) {
    throw new RangeError(`${label} has an invalid timestamp (must be a non-negative finite number)`);
  }
  const key = o['key'];
  if (key !== undefined && (typeof key !== 'string' || key.length === 0)) {
    throw new RangeError(`${label} has an invalid key (must be a non-empty string when present)`);
  }
  // Only the four format fields migrate. Anything else — including
  // signature fields, which never migrate (see the module doc) —
  // is dropped, not carried: an imported record is a new record,
  // not a re-attestation of the source's.
  return {
    seq: seq as number,
    payload: o['payload'],
    ...(key === undefined ? {} : { key: key as string }),
    timestamp: timestamp as number,
  };
}

/**
 * Parses a batch of records into validated form, before any
 * write. Accepts the JSONL text an export produced, or an array
 * of already-parsed record objects — both validate identically,
 * so a caller that builds records in code gets the same
 * fail-closed guarantees as one replaying a file. Anything else
 * is a `RangeError`, not an empty import.
 */
function parseTopicDataBatch(records: string | unknown[]): TopicDataRecord[] {
  if (typeof records === 'string') {
    const parsed: TopicDataRecord[] = [];
    const lines = records.split('\n');
    for (let i = 0; i < lines.length; i += 1) {
      // A trailing newline ends the text with an empty segment;
      // CRLF line endings leave a `\r` — neither is a record.
      const line = (lines[i] as string).endsWith('\r')
        ? (lines[i] as string).slice(0, -1)
        : (lines[i] as string);
      if (line.length === 0) continue;
      let record: unknown;
      try {
        record = JSON.parse(line);
      } catch {
        throw new RangeError(`importTopicData: line ${i + 1} is not valid JSON`);
      }
      parsed.push(validateTopicDataRecord(record, `on line ${i + 1}`));
    }
    return parsed;
  }
  if (Array.isArray(records)) {
    return records.map((record, i) => validateTopicDataRecord(record, `at index ${i}`));
  }
  throw new RangeError('importTopicData: records must be JSONL text or an array of record objects');
}

/**
 * Exports one topic's durable-log history as JSONL: one line per
 * live message record, in source seq order, each line carrying
 * only `seq`/`payload`/`key`/`timestamp` — no signature fields
 * (they bind the source topic and seq and never migrate), no
 * seq-0 runtime state, no archived segments. An unknown topic
 * (no live log) is a `RangeError`; a topic whose history holds
 * a binary record fails closed too (JSONL cannot carry raw
 * bytes — see the module doc). A topic with no messages
 * exports as the empty string, which imports as zero records.
 */
export function exportTopicData(log: DurableTopicLog, topic: string): string {
  if (!log.topics().includes(topic)) {
    throw new RangeError(`exportTopicData: unknown topic "${topic}"`);
  }
  const lines: string[] = [];
  // File order is publish order, which is seq order within one
  // partition — but a partitioned topic interleaves per-partition
  // seqs, so sort ascending to guarantee the export is in seq
  // order. The sort is stable, so records sharing a seq (one per
  // partition) keep their publish order.
  const records = log.readLiveRecords(topic).sort((a, b) => a.seq - b.seq);
  for (const rec of records) {
    if (rec.seq < 1) continue;
    if (rec.binary === true) {
      throw new RangeError(
        `exportTopicData: topic "${topic}" record seq ${rec.seq} is binary and cannot be exported as JSONL`,
      );
    }
    const line: TopicDataRecord = {
      seq: rec.seq,
      payload: rec.payload,
      ...(rec.key === undefined ? {} : { key: rec.key }),
      timestamp: rec.at,
    };
    lines.push(JSON.stringify(line));
  }
  return lines.length === 0 ? '' : `${lines.join('\n')}\n`;
}

/**
 * Imports records into a brand-new topic, in batch order, with
 * seq renumbered from 1 in import order — so the target reads
 * as a topic that was always the target, whatever seqs the
 * source numbered its history with. The source seqs are
 * provenance only: they order nothing on import (the batch
 * order is the order), and they are not preserved.
 *
 * Fail-closed, before any write: an already-existing target
 * (a live log, or a topic the log already knows), a malformed
 * batch, or any single bad record all throw `RangeError` with
 * zero records written — the batch validates completely before
 * the first append, so a bad record can never leave a partial
 * target behind. A failed append mid-copy purges the partial
 * target (best effort) and throws, like the fork's: the result
 * is all imported records or no target.
 */
export function importTopicData(
  log: DurableTopicLog,
  targetTopic: string,
  records: string | unknown[],
): ImportTopicDataResult {
  if (typeof targetTopic !== 'string' || targetTopic.length === 0) {
    throw new RangeError('importTopicData: targetTopic must be a non-empty string');
  }
  if (log.topics().includes(targetTopic)) {
    throw new RangeError(`importTopicData: target topic "${targetTopic}" is already in use`);
  }
  // Validate the whole batch before the first write: a bad
  // record must reject the batch, not truncate it.
  const batch = parseTopicDataBatch(records);
  let imported = 0;
  try {
    for (const record of batch) {
      const ok = log.append({
        seq: imported + 1,
        topic: targetTopic,
        at: record.timestamp,
        payload: record.payload,
        ...(record.key === undefined ? {} : { key: record.key }),
      });
      if (!ok) {
        throw new Error(
          `importTopicData: failed to append record ${imported + 1} to "${targetTopic}"`,
        );
      }
      imported += 1;
    }
  } catch (err) {
    // All-or-nothing: a partial target would read as a history
    // that never existed. Purge best effort; the throw below is
    // the signal regardless.
    try {
      log.purgeTopic(targetTopic);
    } catch {
      // The purge itself failed; the original error still reports.
    }
    throw err;
  }
  return {
    targetTopic,
    imported,
    targetLastSeq: log.lastSeq(targetTopic),
  };
}
