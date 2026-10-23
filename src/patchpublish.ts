/**
 * JSON Patch incremental publish (EB-202): per-key state synchronization
 * without resending the whole payload.
 *
 * A producer registers a key (`EventBus.registerPatchKey`) with an
 * optional baseline snapshot, then publishes RFC 6902 patches
 * (`EventBus.publishPatch`) instead of full payloads. Every patch (and
 * every full publish on the key) advances one per-key version counter;
 * subscribers (`EventBus.subscribePatch`) keep a local snapshot and
 * apply patches in version order, detecting gaps and stale redeliveries.
 *
 * Only a subset of RFC 6902 is implemented — `add`, `remove` and
 * `replace` on JSON-pointer (RFC 6901) paths. `move`, `copy` and `test`
 * are rejected at validation time: a minimal, auditable surface beats a
 * half-tested full implementation, and the three ops cover every
 * incremental state-sync shape in practice.
 *
 * Validation is fail-fast, following the bus convention: structural
 * problems (non-array ops, unknown op, malformed pointer) throw
 * `RangeError` before anything is mutated; apply-time problems (missing
 * path, type conflict) throw `JsonPatchApplyError` without mutating the
 * target — `applyJsonPatch` works on a deep clone and only returns the
 * new document when every op applied cleanly.
 */

export const PATCH_HEADER = 'x-eb-patch';
export const PATCH_VERSION_HEADER = 'x-eb-patch-version';

/** The RFC 6902 operations supported by this module. */
export type JsonPatchOpType = 'add' | 'remove' | 'replace';

/** One RFC 6902 JSON Patch operation. */
export interface JsonPatchOp {
  op: JsonPatchOpType;
  /** JSON pointer (RFC 6901), e.g. `/items/0/name`; `''` addresses the root. */
  path: string;
  /** The value for `add` / `replace`; ignored (and not required) for `remove`. */
  value?: unknown;
}

/** Why a patch could not be applied. */
export type JsonPatchFailureReason =
  | 'missing-path'
  | 'type-conflict'
  | 'invalid-index'
  | 'root-mismatch';

/**
 * Thrown when a structurally valid patch cannot be applied to the
 * target: the addressed path does not exist, or the value at the path
 * has a type the operation cannot work with. The target is never
 * mutated — application is atomic.
 */
export class JsonPatchApplyError extends Error {
  readonly opIndex: number;
  readonly reason: JsonPatchFailureReason;
  constructor(message: string, opIndex: number, reason: JsonPatchFailureReason) {
    super(message);
    this.name = 'JsonPatchApplyError';
    this.opIndex = opIndex;
    this.reason = reason;
  }
}

/**
 * The wire envelope for a patch publish: the payload `publishPatch`
 * admits. Plain subscribers see it as an ordinary object payload; the
 * `x-eb-patch` / `x-eb-patch-version` headers mark it for
 * `subscribePatch`.
 */
export interface PatchEnvelope {
  patch: true;
  /** The patch key this envelope belongs to. */
  key: string;
  /** The per-key version this patch advances the key to. */
  version: number;
  ops: JsonPatchOp[];
}

/** Per-key patch bookkeeping, exposed by `EventBus.getPatchStats`. */
export interface PatchKeyStats {
  /** Current per-key version (full publishes and patches share it). */
  version: number;
  /** Whether a baseline snapshot exists (false until the first full publish or an explicit baseline). */
  hasBaseline: boolean;
  /** Patches applied (publisher commits + subscriber applies). */
  appliedPatches: number;
  /** Version gaps observed on the subscriber side. */
  patchGaps: number;
  /** Patch apply failures (missing path / type conflict). */
  applyErrors: number;
  /** Stale (already-seen) patch versions skipped on the subscriber side. */
  staleSkips: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function deepClone<T>(value: T): T {
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(deepClone) as unknown as T;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    out[k] = deepClone(v);
  }
  return out as T;
}

/**
 * Structural validation of a patch op list. Throws `RangeError` on
 * anything that is not a well-formed RFC 6902 (subset) patch — never
 * returns normally for bad input, never throws for good input.
 */
export function validateJsonPatchOps(ops: unknown): asserts ops is JsonPatchOp[] {
  if (!Array.isArray(ops)) {
    throw new RangeError('jsonPatch: ops must be an array of RFC 6902 operations');
  }
  if (ops.length === 0) {
    throw new RangeError('jsonPatch: ops must not be empty');
  }
  ops.forEach((op, i) => {
    if (!isRecord(op)) {
      throw new RangeError(`jsonPatch: ops[${i}] must be an object`);
    }
    if (op.op !== 'add' && op.op !== 'remove' && op.op !== 'replace') {
      throw new RangeError(
        `jsonPatch: ops[${i}].op must be "add", "remove" or "replace" (got ${JSON.stringify(op.op)})`,
      );
    }
    if (typeof op.path !== 'string') {
      throw new RangeError(`jsonPatch: ops[${i}].path must be a string`);
    }
    // Throws RangeError on a malformed pointer — validated here so a bad
    // pointer fails fast instead of mid-apply.
    parseJsonPointer(op.path);
    if ((op.op === 'add' || op.op === 'replace') && !('value' in op)) {
      throw new RangeError(`jsonPatch: ops[${i}] ("${op.op}") requires a "value" member`);
    }
  });
}

/**
 * Parses an RFC 6901 JSON pointer into path segments. `''` addresses the
 * document root. Throws `RangeError` on a malformed pointer.
 */
export function parseJsonPointer(path: string): string[] {
  if (typeof path !== 'string') {
    throw new RangeError('jsonPatch: path must be a string');
  }
  if (path === '') return [];
  if (!path.startsWith('/')) {
    throw new RangeError(`jsonPatch: invalid JSON pointer "${path}" — must be empty or start with '/'`);
  }
  // RFC 6901 §3: `~1` decodes to `/` and `~0` to `~`; the `~1` mapping
  // is evaluated first so a literal `~01` becomes `~1`, not `/`.
  return path
    .slice(1)
    .split('/')
    .map((segment) => segment.replace(/~1/g, '/').replace(/~0/g, '~'));
}

function parseArrayIndex(
  segment: string,
  length: number,
  forAdd: boolean,
  op: JsonPatchOp,
  opIndex: number,
): number {
  if (segment === '-') {
    if (!forAdd) {
      throw new JsonPatchApplyError(
        `jsonPatch: "-" is only valid for "add" (op #${opIndex} "${op.path}")`,
        opIndex,
        'invalid-index',
      );
    }
    return length;
  }
  if (!/^(0|[1-9][0-9]*)$/.test(segment)) {
    throw new JsonPatchApplyError(
      `jsonPatch: invalid array index "${segment}" (op #${opIndex} "${op.path}")`,
      opIndex,
      'invalid-index',
    );
  }
  const index = Number(segment);
  const limit = forAdd ? length : length - 1;
  if (index > limit) {
    throw new JsonPatchApplyError(
      `jsonPatch: array index ${index} out of bounds (length ${length}, op #${opIndex} "${op.path}")`,
      opIndex,
      'invalid-index',
    );
  }
  return index;
}

/** Resolves one traversal step; throws on missing path or type conflict. */
function descendInto(parent: unknown, segment: string, op: JsonPatchOp, opIndex: number): unknown {
  if (Array.isArray(parent)) {
    const index = parseArrayIndex(segment, parent.length, false, op, opIndex);
    return parent[index];
  }
  if (isRecord(parent)) {
    if (!Object.prototype.hasOwnProperty.call(parent, segment)) {
      throw new JsonPatchApplyError(
        `jsonPatch: path "${op.path}" does not exist (op #${opIndex})`,
        opIndex,
        'missing-path',
      );
    }
    return parent[segment];
  }
  throw new JsonPatchApplyError(
    `jsonPatch: cannot traverse into a non-container at "${op.path}" (op #${opIndex})`,
    opIndex,
    'type-conflict',
  );
}

function applySingle(root: unknown, op: JsonPatchOp, opIndex: number): unknown {
  const segments = parseJsonPointer(op.path);
  if (segments.length === 0) {
    // Root-addressing ops.
    if (op.op === 'remove') {
      throw new JsonPatchApplyError(
        'jsonPatch: cannot remove the document root',
        opIndex,
        'root-mismatch',
      );
    }
    if (op.op === 'replace' && root === undefined) {
      throw new JsonPatchApplyError(
        'jsonPatch: replace target does not exist (document root is undefined)',
        opIndex,
        'missing-path',
      );
    }
    return deepClone(op.value);
  }
  let parent: unknown = root;
  for (let d = 0; d < segments.length - 1; d++) {
    parent = descendInto(parent, segments[d], op, opIndex);
  }
  const last = segments[segments.length - 1];
  if (Array.isArray(parent)) {
    const index = parseArrayIndex(last, parent.length, op.op === 'add', op, opIndex);
    if (op.op === 'remove') {
      parent.splice(index, 1);
    } else if (op.op === 'replace') {
      parent[index] = deepClone(op.value);
    } else {
      parent.splice(index, 0, deepClone(op.value));
    }
    return root;
  }
  if (isRecord(parent)) {
    if (op.op === 'remove' || op.op === 'replace') {
      if (!Object.prototype.hasOwnProperty.call(parent, last)) {
        throw new JsonPatchApplyError(
          `jsonPatch: path "${op.path}" does not exist (op #${opIndex})`,
          opIndex,
          'missing-path',
        );
      }
    }
    if (op.op === 'remove') {
      delete parent[last];
    } else {
      parent[last] = deepClone(op.value);
    }
    return root;
  }
  throw new JsonPatchApplyError(
    `jsonPatch: cannot address "${op.path}" inside a non-container (op #${opIndex})`,
    opIndex,
    'type-conflict',
  );
}

/**
 * Applies an RFC 6902 patch to `target` and returns the new document.
 * Atomic: the target is deep-cloned first, and a `JsonPatchApplyError`
 * leaves the original untouched — callers can retry with a corrected
 * patch without repairing half-applied state.
 */
export function applyJsonPatch<T>(target: T, ops: JsonPatchOp[]): T {
  validateJsonPatchOps(ops);
  let root: unknown = deepClone(target);
  ops.forEach((op, i) => {
    root = applySingle(root, op, i);
  });
  return root as T;
}

/** Structural check for a patch envelope (never throws). */
export function isPatchEnvelope(value: unknown): value is PatchEnvelope {
  if (!isRecord(value)) return false;
  return (
    value.patch === true &&
    typeof value.key === 'string' &&
    typeof value.version === 'number' &&
    Array.isArray(value.ops)
  );
}

/**
 * Per-key version + snapshot bookkeeping for JSON Patch incremental
 * publish (EB-202). Full publishes and patches share one version
 * counter per (topic, key): a full publish establishes a new baseline
 * snapshot and bumps the version; a patch validates against the
 * baseline, fails fast on an invalid patch, and bumps the version only
 * once the publish is admitted.
 *
 * The publisher side and the subscriber side are the same class with
 * different entry points: the publisher uses `stagePatch` /
 * `commitStaged` / `noteFullPublish`; a patch subscriber uses
 * `consumeRemote` / `noteFullPublish`.
 */
export class PatchVersionTracker {
  version = 0;
  snapshot: unknown;
  appliedPatches = 0;
  patchGaps = 0;
  applyErrors = 0;
  staleSkips = 0;

  constructor(baseline?: unknown) {
    if (baseline !== undefined) this.snapshot = deepClone(baseline);
  }

  /**
   * Publisher side: a full publish on the key establishes a new
   * baseline snapshot and bumps the version. Returns the new version.
   */
  noteFullPublish(payload: unknown): number {
    this.snapshot = deepClone(payload);
    this.version += 1;
    return this.version;
  }

  /**
   * Publisher side, first phase of `publishPatch`: validates the patch
   * against the current baseline and computes the staged result WITHOUT
   * mutating the tracker. Throws `RangeError` when no baseline exists
   * and `JsonPatchApplyError` on an invalid patch — both fail-fast with
   * zero side effects, so no version is consumed and nothing publishes.
   */
  stagePatch(ops: JsonPatchOp[]): { version: number; snapshot: unknown } {
    if (this.snapshot === undefined) {
      throw new RangeError(
        'publishPatch: no baseline snapshot for this key — publish a full message first or registerPatchKey with a baseline',
      );
    }
    const snapshot = applyJsonPatch(this.snapshot, ops);
    return { version: this.version + 1, snapshot };
  }

  /** Publisher side, second phase: commits a staged patch after the publish was admitted. */
  commitStaged(version: number, snapshot: unknown): void {
    this.version = version;
    this.snapshot = snapshot;
    this.appliedPatches += 1;
  }

  /**
   * Subscriber side: consumes a patch at the given per-key version.
   * Returns `'applied'` when the patch advanced the snapshot,
   * `'stale'` for an already-seen (or older) version — ignored, never
   * an error — and `'gap'` when the version jumps ahead of the expected
   * next version, meaning the subscriber missed at least one full
   * publish or patch for this key. On a gap the snapshot is left
   * untouched: applying an out-of-order RFC 6902 patch would corrupt
   * state, so the caller must surface the gap (and can rebuild with
   * `rebuildPatchSnapshot`).
   */
  consumeRemote(version: number, ops: JsonPatchOp[]): 'applied' | 'stale' | 'gap' {
    if (version <= this.version) {
      this.staleSkips += 1;
      return 'stale';
    }
    if (version > this.version + 1) {
      this.patchGaps += 1;
      return 'gap';
    }
    if (this.snapshot === undefined) {
      // No baseline yet: the version advanced exactly by one but there
      // is nothing to apply onto — the subscriber missed the baseline
      // full publish. Counted as a gap so the operator sees it.
      this.patchGaps += 1;
      return 'gap';
    }
    try {
      this.snapshot = applyJsonPatch(this.snapshot, ops);
    } catch (error) {
      // The version is consumed so the stream does not wedge on one bad
      // patch; the failure is counted and rethrown for the caller to
      // surface — the operator rebuilds from the durable log.
      this.applyErrors += 1;
      this.version = version;
      throw error;
    }
    this.version = version;
    this.appliedPatches += 1;
    return 'applied';
  }

  toStats(): PatchKeyStats {
    return {
      version: this.version,
      hasBaseline: this.snapshot !== undefined,
      appliedPatches: this.appliedPatches,
      patchGaps: this.patchGaps,
      applyErrors: this.applyErrors,
      staleSkips: this.staleSkips,
    };
  }
}
