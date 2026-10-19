/**
 * Pluggable storage backends for the durable topic log (EB-96).
 *
 * The durable log used to talk to `node:fs` directly: the per-topic live
 * log (EB-17), archived segments (EB-61), the group-offset journal
 * (EB-32), the subscriber-dedup journal, and the publish-dedup journal
 * (EB-66) all lived as files under one directory. This module extracts
 * that persistence surface into the `DurableStorageBackend` interface —
 * the log now reads and writes opaque byte blobs through it, and the
 * local filesystem is just the default implementation.
 *
 * The interface is deliberately synchronous: the log's durability
 * contract is that a publish-time append is on stable storage before
 * `append` returns, and the bus's publish path is synchronous end to
 * end. A backend that cannot serve an operation synchronously (most
 * network stores) must bridge that gap itself — see
 * `src/storage-s3.ts` for the documented S3-compatible example, which
 * shells out to `curl` synchronously.
 *
 * Keys are backend-relative, slash-free names like
 * `market.btc.trades.log`, `__group_offsets.jsonl`, or
 * `t.seg-1-500.jsonl.gz`; a backend maps them to whatever its store
 * needs (paths, object names). Keys never contain `..` segments or
 * leading slashes — `assertValidStorageKey` enforces that at the
 * boundary, so a filesystem backend can never escape its root.
 *
 * Encryption (EB-80) and per-segment checksums (EB-94) live ABOVE this
 * layer: the log seals/compresses/checksums bytes before handing them
 * to the backend, so a custom backend stores opaque blobs and the
 * at-rest guarantees do not depend on which backend is in use.
 */
import {
  appendFileSync,
  closeSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, sep } from 'node:path';
import { Buffer } from 'node:buffer';

/** Options for `DurableStorageBackend.write`. */
export interface StorageWriteOptions {
  /**
   * When true the write fails (throws) if the key already exists —
   * the `O_EXCL` semantic the archive pass relies on so two racing
   * passes never clobber each other's segment. Defaults to false
   * (atomic replace).
   */
  exclusive?: boolean;
}

/**
 * What the durable topic log needs from persistence: atomic
 * write/replace, append, read, enumeration, delete, and rename — all
 * synchronous, per the module contract above.
 */
export interface DurableStorageBackend {
  /**
   * Atomically stores `data` under `key`, replacing any previous value.
   * A reader never observes a torn object: the bytes appear all at
   * once. With `exclusive: true` the write throws when the key already
   * exists instead of replacing it.
   */
  write(key: string, data: Uint8Array | string, opts?: StorageWriteOptions): void;
  /** Appends `data` to `key`, creating it when absent. */
  append(key: string, data: Uint8Array | string): void;
  /**
   * The raw bytes stored under `key`, or `undefined` when the key is
   * absent. Never throws for a missing key; I/O errors are reported as
   * `undefined` too — the log treats an unreadable object as absent
   * and keeps serving.
   */
  read(key: string): Uint8Array | undefined;
  /**
   * Every key starting with `prefix` (an empty prefix lists
   * everything). Order is not guaranteed. Used by recovery to
   * enumerate the log's objects.
   */
  list(prefix?: string): string[];
  /** Removes `key`; a no-op when the key is absent. */
  delete(key: string): void;
  /**
   * Atomically moves `oldKey` to `newKey`. The log uses it to close a
   * live segment (the rename is the commit point — a crash either
   * leaves the live file or the complete pending segment, never half
   * of each).
   */
  rename(oldKey: string, newKey: string): void;
  /**
   * A view of this backend whose keys all live under `prefix` — every
   * operation transparently prepends it. The bus uses it for namespace
   * child logs so one backend serves the root log and every namespace.
   */
  scoped(prefix: string): DurableStorageBackend;
  /**
   * Pushes every write so far to stable storage (EB-154, fsync
   * semantics): after `sync()` returns, data written through this
   * backend survives a process crash, not just an OS-buffered return.
   * Optional — backends whose writes are already durable when the write
   * call returns (e.g. the S3-compatible backend, whose upload completes
   * before `write`/`append` return) omit it, and callers treat an absent
   * `sync` as a no-op. Never throws for a backend that has nothing to
   * flush.
   */
  sync?(): void;
}

/**
 * Validates a storage key: non-empty, no leading slash, no `..`
 * segments — the invariant that keeps every backend's key mapping from
 * escaping its root. Throws `RangeError` on violation.
 */
export function assertValidStorageKey(key: unknown, where: string): void {
  if (typeof key !== 'string' || key.length === 0) {
    throw new RangeError(`${where}: storage key must be a non-empty string`);
  }
  if (key.startsWith('/')) {
    throw new RangeError(`${where}: storage key must be relative, got "${key}"`);
  }
  for (const segment of key.split('/')) {
    if (segment === '..' || segment.length === 0) {
      throw new RangeError(`${where}: storage key must not contain empty or ".." segments, got "${key}"`);
    }
  }
}

/** Normalizes a storage prefix: non-empty, slash-terminated. */
export function normalizeStoragePrefix(prefix: unknown, where: string): string {
  if (typeof prefix !== 'string' || prefix.length === 0) {
    throw new RangeError(`${where}: storage prefix must be a non-empty string`);
  }
  assertValidStorageKey(prefix.replace(/\/$/, ''), where);
  return prefix.endsWith('/') ? prefix : `${prefix}/`;
}

function toBytes(data: Uint8Array | string): Uint8Array {
  return typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
}

let tmpCounter = 0;

/**
 * The default `DurableStorageBackend`: keys become files directly under
 * `root`, created on construction. Atomic `write` goes through a
 * temp-file-plus-rename in the same directory, so a crash never leaves
 * a torn object behind; `append` is a single `appendFileSync`, the same
 * durability the log always relied on.
 */
export class FilesystemStorageBackend implements DurableStorageBackend {
  private readonly root: string;
  /** Parent directories already created, so nested keys cost one Set lookup. */
  private readonly dirsMade = new Set<string>();
  /**
   * Keys written since the last `sync()` (EB-154): `sync()` fsyncs
   * exactly these files, then clears the set. Appends and atomic
   * writes are already synchronous (`appendFileSync` / temp-rename),
   * so this is only the OS-buffer → stable-storage push.
   */
  private readonly dirtyKeys = new Set<string>();

  constructor(root: string) {
    if (typeof root !== 'string' || root.length === 0) {
      throw new RangeError('FilesystemStorageBackend root must be a non-empty string');
    }
    this.root = root;
    mkdirSync(root, { recursive: true });
    this.dirsMade.add(root);
  }

  /** The root directory, as configured. */
  get dir(): string {
    return this.root;
  }

  private pathFor(key: string): string {
    assertValidStorageKey(key, 'FilesystemStorageBackend');
    return join(this.root, key);
  }

  /** Creates the key's parent directory on first use (scoped keys nest). */
  private ensureParent(path: string): void {
    const dir = dirname(path);
    if (this.dirsMade.has(dir)) return;
    mkdirSync(dir, { recursive: true });
    this.dirsMade.add(dir);
  }

  write(key: string, data: Uint8Array | string, opts?: StorageWriteOptions): void {
    const path = this.pathFor(key);
    this.ensureParent(path);
    const bytes = toBytes(data);
    if (opts?.exclusive === true) {
      writeFileSync(path, bytes, { flag: 'wx' });
      this.dirtyKeys.add(key);
      return;
    }
    // Atomic replace: the temp file lives in the same directory so the
    // rename never crosses filesystems, and a crash leaves either the
    // old object or the complete new one.
    tmpCounter += 1;
    const tmp = `${path}.tmp-${process.pid}-${tmpCounter}`;
    try {
      writeFileSync(tmp, bytes);
      renameSync(tmp, path);
    } catch (err) {
      try {
        unlinkSync(tmp);
      } catch {
        // Best-effort cleanup; the temp name is unique per process.
      }
      throw err;
    }
    this.dirtyKeys.add(key);
  }

  append(key: string, data: Uint8Array | string): void {
    const path = this.pathFor(key);
    this.ensureParent(path);
    appendFileSync(path, toBytes(data));
    this.dirtyKeys.add(key);
  }

  /**
   * Pushes every write since the last `sync()` to stable storage
   * (EB-154, fsync semantics): each dirty file is opened read-only and
   * fsynced, then the dirty set is cleared. A key deleted after being
   * written is skipped silently — there is nothing left to sync. Never
   * throws for an empty dirty set.
   */
  sync(): void {
    if (this.dirtyKeys.size === 0) return;
    const keys = [...this.dirtyKeys];
    this.dirtyKeys.clear();
    for (const key of keys) {
      let fd: number | undefined;
      try {
        fd = openSync(this.pathFor(key), 'r');
        fsyncSync(fd);
      } catch {
        // Best-effort: a key deleted (or never created, e.g. a failed
        // exclusive write) between the write and the sync has nothing
        // to push. The write itself already threw at write time when it
        // failed — a sync must not invent a second failure.
      } finally {
        if (fd !== undefined) {
          try {
            closeSync(fd);
          } catch {
            // Best-effort close; the fsync already ran.
          }
        }
      }
    }
  }

  read(key: string): Uint8Array | undefined {
    try {
      return readFileSync(this.pathFor(key));
    } catch {
      return undefined;
    }
  }

  list(prefix = ''): string[] {
    if (typeof prefix !== 'string') {
      throw new RangeError('FilesystemStorageBackend.list: prefix must be a string');
    }
    // Recursive: scoped views store keys in nested directories, so a
    // flat readdir would miss them. Only files are keys — directories
    // created by other tools are not log objects.
    const out: string[] = [];
    const walk = (dir: string, rel: string): void => {
      let entries: import('node:fs').Dirent[];
      try {
        entries = readdirSync(dir, { withFileTypes: true });
      } catch {
        return;
      }
      for (const entry of entries) {
        const key = rel === '' ? entry.name : `${rel}/${entry.name}`;
        if (entry.isDirectory()) {
          walk(join(dir, entry.name), key);
        } else if (entry.isFile() && key.startsWith(prefix)) {
          out.push(key);
        }
      }
    };
    walk(this.root, '');
    return out;
  }

  delete(key: string): void {
    try {
      unlinkSync(this.pathFor(key));
    } catch {
      // Idempotent: absent keys are a no-op.
    }
  }

  rename(oldKey: string, newKey: string): void {
    const target = this.pathFor(newKey);
    this.ensureParent(target);
    renameSync(this.pathFor(oldKey), target);
  }

  scoped(prefix: string): DurableStorageBackend {
    return new PrefixedStorageBackend(this, normalizeStoragePrefix(prefix, 'scoped'));
  }
}

/**
 * A generic scoping wrapper: presents `inner` with every key transparently
 * prefixed. Used by `FilesystemStorageBackend.scoped` and available to
 * any backend that does not want to implement scoping natively.
 */
export class PrefixedStorageBackend implements DurableStorageBackend {
  private readonly inner: DurableStorageBackend;
  private readonly prefix: string;

  constructor(inner: DurableStorageBackend, prefix: string) {
    if (inner == null || typeof (inner as DurableStorageBackend).write !== 'function') {
      throw new RangeError('PrefixedStorageBackend requires a DurableStorageBackend');
    }
    this.inner = inner;
    this.prefix = normalizeStoragePrefix(prefix, 'PrefixedStorageBackend');
  }

  /** The prefix this view prepends to every key. */
  get scopePrefix(): string {
    return this.prefix;
  }

  write(key: string, data: Uint8Array | string, opts?: StorageWriteOptions): void {
    this.inner.write(`${this.prefix}${key}`, data, opts);
  }

  append(key: string, data: Uint8Array | string): void {
    this.inner.append(`${this.prefix}${key}`, data);
  }

  read(key: string): Uint8Array | undefined {
    return this.inner.read(`${this.prefix}${key}`);
  }

  list(prefix = ''): string[] {
    return this.inner
      .list(`${this.prefix}${prefix}`)
      .map((key) => key.slice(this.prefix.length));
  }

  delete(key: string): void {
    this.inner.delete(`${this.prefix}${key}`);
  }

  rename(oldKey: string, newKey: string): void {
    this.inner.rename(`${this.prefix}${oldKey}`, `${this.prefix}${newKey}`);
  }

  scoped(prefix: string): DurableStorageBackend {
    return new PrefixedStorageBackend(
      this.inner,
      `${this.prefix}${normalizeStoragePrefix(prefix, 'scoped')}`,
    );
  }

  /**
   * Delegates to the wrapped backend (EB-154): a scoped view shares its
   * inner backend's durability, so syncing the inner backend covers every
   * scope. A no-op when the inner backend has no `sync`.
   */
  sync(): void {
    this.inner.sync?.();
  }
}

/**
 * Builds a unique temp-file path inside `dir` for synchronous
 * shell-out transports (see `src/storage-s3.ts`).
 */
export function tempFilePath(dir: string, tag: string): string {
  tmpCounter += 1;
  return join(dir, `.${tag}-${process.pid}-${tmpCounter}.tmp`);
}

/** Writable temp directory for synchronous transports. */
export function systemTempDir(): string {
  return tmpdir();
}
