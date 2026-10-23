/**
 * Conditional publish — compare-and-set (EB-212).
 *
 * `EventBus.publishCompareAndSet(topic, key, expected, payload)` reads
 * the materialized state store (EB-171 KTable) for `(topic, key)` and
 * only runs the normal publish pipeline when the current row matches
 * `expected`. This is the primitive for distributed config rollout and
 * keyed state synchronization: a producer reads the current value +
 * version, computes the next value, and publishes only if nobody else
 * moved the key in between — no lock, no round-trip to a coordinator.
 *
 * `expected` takes two forms (or both — they AND):
 * - `{ version: n }` — the row's per-(topic, key) update counter must
 *   equal `n`. A missing row reads as version 0, so
 *   `{ version: 0 }` is the create-if-absent guard.
 * - `{ value: v }` — the row's latest value must be structurally equal
 *   to `v` (see `payloadEquals`). A missing row reads as `undefined`,
 *   so `{ value: undefined }` is the value-form create-if-absent guard.
 *
 * The matching is pure (this module has no bus dependency): the bus
 * reads the store and runs the publish in one synchronous call stack,
 * so the check and the admission share a critical section — no
 * interleaving publish can slip between them (TOCTOU-safe on the
 * event-loop model).
 *
 * A mismatch is a publish-side admission rejection: it never consumes
 * a sequence number, never writes the durable log, and never burns
 * rate-limit budget — the EB-39 `onAdmissionRejected` vocabulary gains
 * reason `'cas-mismatch'`, counted in `casMismatches` (per-topic and
 * global) so hook events reconcile 1:1 against `getStats()`.
 */

/** What `publishCompareAndSet` compares the state-store row against. */
export interface CompareAndSetExpected {
  /** The row's per-(topic, key) update counter must equal this. */
  version?: number;
  /** The row's latest value must be structurally equal to this. */
  value?: unknown;
}

/**
 * Validates the `expected` argument fail-fast, following the bus
 * convention: a malformed expectation throws `RangeError` before
 * anything is read or mutated.
 */
export function validateCompareAndSetExpected(
  expected: CompareAndSetExpected,
  caller: string,
): void {
  if (expected === null || typeof expected !== 'object' || Array.isArray(expected)) {
    throw new RangeError(`${caller}: expected must be an object like { version } or { value }`);
  }
  const hasVersion = 'version' in expected;
  const hasValue = 'value' in expected;
  if (!hasVersion && !hasValue) {
    throw new RangeError(`${caller}: expected must carry version and/or value`);
  }
  if (hasVersion) {
    const version = (expected as { version: unknown }).version;
    if (typeof version !== 'number' || !Number.isInteger(version) || version < 0) {
      throw new RangeError(`${caller}: expected.version must be a non-negative integer`);
    }
  }
}

/**
 * Structural equality for the value predicate: JSON-shaped values
 * compare by structure (object key order is insignificant), typed
 * arrays compare byte-wise, `NaN` equals `NaN`, `Date`s compare by
 * time value. Functions and class instances only equal themselves —
 * the predicate is meant for data payloads, not behavior.
 */
export function payloadEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a === 'number' && typeof b === 'number') {
    return Number.isNaN(a) && Number.isNaN(b);
  }
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') {
    return false;
  }
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
  if (ArrayBuffer.isView(a) && ArrayBuffer.isView(b)) {
    const av = new Uint8Array(a.buffer, a.byteOffset, a.byteLength);
    const bv = new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
    if (av.length !== bv.length) return false;
    for (let i = 0; i < av.length; i++) {
      if (av[i] !== bv[i]) return false;
    }
    return true;
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (!payloadEquals(a[i], b[i])) return false;
    }
    return true;
  }
  const aKeys = Object.keys(a);
  const bKeys = Object.keys(b as object);
  if (aKeys.length !== bKeys.length) return false;
  const bSet = new Set(bKeys);
  for (const k of aKeys) {
    if (!bSet.has(k)) return false;
    if (
      !payloadEquals(
        (a as Record<string, unknown>)[k],
        (b as Record<string, unknown>)[k],
      )
    ) {
      return false;
    }
  }
  return true;
}

/** The row shape `compareAndSetMatches` reads (the `StateEntry` subset). */
export interface CompareAndSetRow {
  value: unknown;
  version: number;
}

/**
 * Pure match: does the current state-store row satisfy `expected`? A
 * missing row (`undefined`) reads as `{ value: undefined, version: 0 }`
 * — the create-if-absent semantics. When both `version` and `value`
 * are present they must both hold.
 */
export function compareAndSetMatches(
  row: CompareAndSetRow | undefined,
  expected: CompareAndSetExpected,
): boolean {
  const version = row?.version ?? 0;
  const value = row?.value;
  if ('version' in expected && (expected as { version: unknown }).version !== version) {
    return false;
  }
  if ('value' in expected && !payloadEquals(value, (expected as { value: unknown }).value)) {
    return false;
  }
  return true;
}
