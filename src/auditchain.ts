/**
 * Tamper-evident hash chaining for the bus's audit journals (EB-198):
 * the publish audit journal (EB-138, `src/publishaudit.ts`) and the
 * subscription audit log (EB-78, `src/subaudit.ts`) chain every record
 * with SHA-256, mirroring the ledger's audit chain (ledger-api-go LG-11)
 * and the webhook relay's audit-sink hash-chain mode.
 *
 * Each record carries:
 * - `prevHash`: the previous record's `hash` (the fixed genesis below
 *   for the first record);
 * - `hash`: SHA-256 over (`prevHash` + `'\n'` + canonical JSON of the
 *   record without the chain fields).
 *
 * Rewriting any journaled field, deleting a record, or splicing records
 * breaks the prevHash/hash continuity, and `verifyChainLinks` detects it
 * by recomputation. The chain covers both layers in sync: the
 * in-memory ring and the on-disk JSONL lines carry the same links, so a
 * restarted bus recovers a verifiable chain (files are read oldest
 * first, matching append order).
 *
 * Bounded-ring caveat: the in-memory ring evicts old entries, so
 * verification covers the retained window as a contiguous segment — the
 * oldest retained entry is the segment anchor (its link to the evicted
 * predecessor cannot be checked). Records written before chaining
 * existed carry no chain fields and verify as anchors: their content
 * still anchors the records after them. `node:crypto` only — zero new
 * dependencies.
 */
import { createHash } from 'node:crypto';

/**
 * Fixed `prevHash` of the first chained record — the "genesis",
 * mirroring the ledger chain's 32 zero bytes (LG-11) in hex form.
 */
export const AUDIT_CHAIN_GENESIS = '0'.repeat(64);

/** Chain fields a journal record carries once chained. */
export interface ChainedAuditRecord {
  prevHash?: string;
  hash?: string;
}

/** True for a 64-character lowercase hex SHA-256 digest. */
export function isHashString(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{64}$/.test(value);
}

/**
 * Canonical JSON: object keys sorted recursively, so the hash is
 * stable regardless of the key order the record was built with.
 * `undefined` values are dropped (matching `JSON.stringify`); other
 * values follow `JSON.stringify` semantics.
 */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null';
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(',')}]`;
  }
  const entries: string[] = [];
  for (const key of Object.keys(value).sort()) {
    const item = (value as Record<string, unknown>)[key];
    if (item === undefined) continue;
    entries.push(`${JSON.stringify(key)}:${canonicalJson(item)}`);
  }
  return `{${entries.join(',')}}`;
}

/** SHA-256 hex digest of (`prevHash` + `'\n'` + canonical body). */
export function chainHash(prevHash: string, canonicalBody: string): string {
  return createHash('sha256').update(prevHash, 'utf8').update('\n', 'utf8').update(canonicalBody, 'utf8').digest('hex');
}

function stripChainFields<T extends object>(record: T): Record<string, unknown> {
  const body: Record<string, unknown> = {};
  for (const key of Object.keys(record)) {
    if (key === 'prevHash' || key === 'hash') continue;
    body[key] = (record as Record<string, unknown>)[key];
  }
  return body;
}

/**
 * Computes the record's link hash: SHA-256 over (`prevHash` + `'\n'` +
 * canonical JSON of the record without the chain fields). `prevHash`
 * defaults to the genesis for pre-chain records.
 */
export function hashChainedRecord<T extends object>(prevHash: string | undefined, record: T): string {
  return chainHash(prevHash ?? AUDIT_CHAIN_GENESIS, canonicalJson(stripChainFields(record)));
}

/**
 * Attaches a fresh chain link to a record: `prevHash` is the previous
 * record's hash (or the genesis), `hash` covers the record content.
 * Returns the same record type with the two chain fields set.
 */
export function attachChainLink<T extends object>(
  prevHash: string,
  record: T,
): T & { prevHash: string; hash: string } {
  const linked = record as T & { prevHash: string; hash: string };
  linked.prevHash = prevHash;
  linked.hash = hashChainedRecord(prevHash, record);
  return linked;
}

/**
 * The record's effective hash for chaining: its stored `hash`, or the
 * recomputed link hash for pre-chain records (which carry no chain
 * fields but still anchor the records after them).
 */
export function effectiveRecordHash<T extends ChainedAuditRecord & object>(record: T): string {
  if (record.hash !== undefined) return record.hash;
  return hashChainedRecord(record.prevHash, record);
}

/** The outcome of `verifyChainLinks`. */
export interface ChainVerification {
  /** True when every checked link verifies. */
  ok: boolean;
  /** Number of records checked. */
  checked: number;
  /** Index of the first broken record (when `!ok`). */
  brokenAt?: number;
  /** `content-tampered`: the record's content no longer matches its hash. */
  reason?: 'content-tampered' | 'link-broken';
  /** `link-broken`: a record was deleted or spliced out of the chain. */
  expected?: string;
  actual?: string;
}

/**
 * Verifies a contiguous in-memory chain segment (oldest first):
 * - every record carrying a `hash` must match the recomputed content
 *   hash (`content-tampered` otherwise — a field was rewritten);
 * - every record after the first carrying a `prevHash` must link to the
 *   previous record's effective hash (`link-broken` otherwise — a record
 *   was deleted or spliced).
 *
 * The first record is the segment anchor: its link to whatever came
 * before the retained window (evicted entries, or the genesis) cannot be
 * checked. Pre-chain records (no chain fields) verify as anchors and
 * still anchor their successors.
 */
export function verifyChainLinks<T extends ChainedAuditRecord & object>(
  records: readonly T[],
): ChainVerification {
  for (let i = 0; i < records.length; i += 1) {
    const record = records[i];
    if (record.hash !== undefined) {
      const recomputed = hashChainedRecord(record.prevHash, record);
      if (recomputed !== record.hash) {
        return {
          ok: false,
          checked: i + 1,
          brokenAt: i,
          reason: 'content-tampered',
          expected: record.hash,
          actual: recomputed,
        };
      }
    }
    if (i > 0 && record.prevHash !== undefined) {
      const expected = effectiveRecordHash(records[i - 1]);
      if (record.prevHash !== expected) {
        return {
          ok: false,
          checked: i + 1,
          brokenAt: i,
          reason: 'link-broken',
          expected,
          actual: record.prevHash,
        };
      }
    }
  }
  return { ok: true, checked: records.length };
}
