/**
 * Payload structural fingerprinting (EB-200): observe-only detection of
 * per-topic payload shape drift.
 *
 * A "fingerprint" is the stable hash of a payload's field-path set — the
 * sorted list of every reachable field path (`a.b.c` for objects,
 * `a[]` / `a[].b` for arrays). Two payloads have the same fingerprint
 * iff they expose the same set of fields, regardless of values, key
 * order, or array lengths.
 *
 * This is intentionally orthogonal to the schema validators (EB-20 /
 * EB-86) and the compatibility gate (EB-95): those *admit or reject*
 * publishes, while fingerprinting only *observes*. A drift never blocks
 * a publish — it fires the registered `onDrift` callback and bumps
 * `getStats().schemaDrifts`, so operators get early warning when a
 * producer silently changes shape in production.
 *
 * Zero dependencies; the hash is FNV-1a over the sorted path list.
 */

/** A detected structural drift on a watched topic. */
export interface FingerprintDrift {
  /** The concrete topic the drifting payload was published to. */
  topic: string;
  /** The new payload's structural fingerprint (hex). */
  fingerprint: string;
  /** Field paths present now but absent from the baseline, sorted. */
  added: string[];
  /** Field paths in the baseline but absent now, sorted. */
  removed: string[];
}

/** Callback fired when a watched topic's payload structure drifts. */
export type FingerprintDriftCallback = (drift: FingerprintDrift) => void;

/** A per-subscriber oversize skip observed on a watched topic (EB-208). */
export interface FingerprintOversize {
  /** The concrete topic the oversized message was published to. */
  topic: string;
  /** The subscriber id that skipped the message. */
  subscriberId: string;
  /** The message payload's size in UTF-8 JSON bytes (the EB-120 yardstick). */
  payloadBytes: number;
  /** The cap that was exceeded, in bytes. */
  capBytes: number;
  /**
   * Where the cap came from: `'subscriber'` = the subscriber's explicit
   * `subscriberMaxPayloadBytes`, `'topic'` = the inherited topic-level
   * payload cap (`setTopicMaxPayloadBytes`, EB-120).
   */
  capSource: 'subscriber' | 'topic';
}

/** Callback fired when a watched topic sees a subscriber oversize skip. */
export type FingerprintOversizeCallback = (oversize: FingerprintOversize) => void;

/** Options for `EventBus.setTopicSchemaFingerprint`. */
export interface TopicFingerprintOptions {
  /** Fired (error-isolated) on every structural drift after promotion. */
  onDrift: FingerprintDriftCallback;
  /**
   * Fired (error-isolated) whenever a subscriber skips an oversize
   * message on a matching topic (EB-208 linkage): the message exceeded
   * the subscriber's payload cap — its explicit
   * `subscriberMaxPayloadBytes`, or the inherited EB-120 topic cap.
   * Optional: a watcher that only cares about structural drift omits it.
   * Never fires for publish-side admission rejections (those never reach
   * a subscriber).
   */
  onOversize?: FingerprintOversizeCallback;
}

/**
 * Extracts the sorted set of field paths reachable from `payload`.
 * Objects contribute `a.b.c` paths; arrays contribute `a[]` plus the
 * union of their elements' shapes as `a[].b` (array *length* is not
 * structural — `[1, 2]` and `[1, 2, 3]` share a fingerprint). Primitives,
 * `null`, and `undefined` contribute no paths.
 */
export function extractFieldPaths(payload: unknown): string[] {
  const paths = new Set<string>();
  collectPaths(payload, '', paths);
  return [...paths].sort();
}

function collectPaths(value: unknown, prefix: string, out: Set<string>): void {
  if (value === null || typeof value !== 'object') return;
  if (Array.isArray(value)) {
    if (prefix !== '') out.add(`${prefix}[]`);
    const elemPrefix = prefix === '' ? '[]' : `${prefix}[]`;
    for (const item of value) collectPaths(item, elemPrefix, out);
    return;
  }
  const record = value as Record<string, unknown>;
  for (const key of Object.keys(record)) {
    const path = prefix === '' ? key : `${prefix}.${key}`;
    out.add(path);
    collectPaths(record[key], path, out);
  }
}

/**
 * Computes the stable structural fingerprint of a payload: FNV-1a
 * (32-bit, hex) over the sorted field-path set. Identical shapes hash
 * identically across processes and restarts.
 */
export function computePayloadFingerprint(payload: unknown): string {
  return hashFieldPaths(extractFieldPaths(payload));
}

function hashFieldPaths(paths: string[]): string {
  let h = 0x811c9dc5;
  for (const p of paths) {
    for (let i = 0; i < p.length; i++) {
      h ^= p.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    // Field separator so ['ab', 'c'] != ['a', 'bc'].
    h ^= 0xff;
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, '0');
}

/**
 * Diffs two field-path sets: paths in `newPaths` but not `oldPaths`
 * are `added`, the reverse are `removed`. Both lists are sorted.
 */
export function diffFieldPaths(
  oldPaths: string[],
  newPaths: string[],
): { added: string[]; removed: string[] } {
  const oldSet = new Set(oldPaths);
  const newSet = new Set(newPaths);
  const added = newPaths.filter((p) => !oldSet.has(p));
  const removed = oldPaths.filter((p) => !newSet.has(p));
  return { added, removed };
}
