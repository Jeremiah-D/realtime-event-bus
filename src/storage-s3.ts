/**
 * S3-compatible storage backend — a worked EXAMPLE (EB-96), not a
 * production S3 client.
 *
 * It shows how a caller plugs an S3-compatible object store (AWS S3,
 * MinIO, R2, …) behind `DurableStorageBackend` with zero new npm
 * dependencies:
 *
 * - **Signing is the caller's job.** S3 needs SigV4 (or the store's
 *   equivalent); the secret key must never live in this library, so the
 *   caller injects `signRequest` and owns credential handling.
 * - **HTTP is the caller's job too, by default.** The bundled transport
 *   shells out to the `curl` binary synchronously (`execFileSync`) —
 *   the only way to stay synchronous in Node without new dependencies,
 *   which the `DurableStorageBackend` contract requires. A caller with
 *   their own sync transport (a native binding, a sidecar) injects it
 *   via `transport`.
 *
 * Operation mapping (all keys are flat object names under
 * `s3://bucket/prefix`):
 *
 * - `write` → `PUT` (atomic by S3 semantics); `exclusive: true` does a
 *   `HEAD` first and throws when the object exists — best-effort, not a
 *   distributed lock: two racing writers can still both pass the check,
 *   exactly like `O_EXCL` on NFS. The archive pass treats a lost race
 *   as "another pass won" and moves on, so this is sufficient.
 * - `append` → read-modify-write (`GET` + `PUT`). S3 has no append
 *   operation: this is correct but O(object) per append, so an
 *   S3-backed log wants small live segments (tight
 *   `durableLogSegmentRotation`) or a buffering shim in front. The
 *   example does the honest simple thing and documents the cost.
 * - `read` → `GET` (`404` → `undefined`).
 * - `list` → `ListObjectsV2` with prefix, following continuation
 *   tokens (bounded: 100 pages).
 * - `delete` → `DELETE` (idempotent by S3 semantics).
 * - `rename` → server-side `COPY` (`x-amz-copy-source`) + `DELETE`
 *   (not atomic across the two calls — the log's rotation only needs
 *   "the bytes end up under the new name", which holds).
 * - `scoped` → `PrefixedStorageBackend` key prefixing.
 *
 * Production hardening this example deliberately omits: retries with
 * backoff, request timeouts, `Content-MD5`/`x-amz-checksum-*` on PUT,
 * multipart upload for large segments, and credential rotation. The
 * durable log's own guarantees — per-line encryption (EB-80) and
 * per-segment checksums (EB-94) — are computed above this layer and
 * travel with the bytes regardless of backend.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync, unlinkSync, writeFileSync } from 'node:fs';
import { Buffer } from 'node:buffer';
import {
  PrefixedStorageBackend,
  normalizeStoragePrefix,
  systemTempDir,
  tempFilePath,
  type DurableStorageBackend,
  type StorageWriteOptions,
} from './storage.ts';

/**
 * Signs one S3 request. The caller implements SigV4 (or the store's
 * scheme) and returns the headers to send — typically adding
 * `Authorization`, `x-amz-date`, and any content hashes. `body` is
 * `undefined` for bodyless requests.
 */
export type S3RequestSigner = (
  method: string,
  url: string,
  headers: Record<string, string>,
  body: Uint8Array | undefined,
) => Record<string, string>;

/** One synchronous HTTP round trip. */
export interface S3HttpResponse {
  status: number;
  headers: Record<string, string>;
  body: Uint8Array;
}

/** Synchronous HTTP transport, injectable for tests and custom stacks. */
export interface S3HttpTransport {
  request(
    method: string,
    url: string,
    headers: Record<string, string>,
    body?: Uint8Array,
  ): S3HttpResponse;
}

/** Options for {@link createS3CompatibleBackend}. */
export interface S3CompatibleBackendOptions {
  /**
   * Store endpoint, e.g. `https://s3.amazonaws.com` or
   * `http://localhost:9000` for MinIO. No trailing slash.
   */
  endpoint: string;
  /** Bucket holding the log objects. Must already exist. */
  bucket: string;
  /**
   * Key prefix inside the bucket, e.g. `bus-logs/prod/`. Defaults to
   * `''` (bucket root). Always slash-terminated internally.
   */
  prefix?: string;
  /** Caller-provided request signer (SigV4). Required. */
  signRequest: S3RequestSigner;
  /**
   * Synchronous HTTP transport. Defaults to a `curl`-based transport
   * (`curlSyncTransport`) — the binary must be on `PATH`.
   */
  transport?: S3HttpTransport;
}

function encodeKey(key: string): string {
  return key.split('/').map(encodeURIComponent).join('/');
}

function decodeXmlEntities(text: string): string {
  return text
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'");
}

/**
 * The default transport: synchronous HTTP via the `curl` binary. Every
 * request runs `curl` as a child process (no shell — argv array, so
 * URLs and header values cannot inject commands), captures the status
 * code from `-w`, headers from `-D`, and the body from `-o`. Throws on
 * transport-level failure (non-zero exit); HTTP error statuses are
 * returned as values so the backend can map `404` → absent.
 */
export function curlSyncTransport(): S3HttpTransport {
  return {
    request(method, url, headers, body) {
      const dir = systemTempDir();
      const headerFile = tempFilePath(dir, 's3-headers');
      const bodyFile = tempFilePath(dir, 's3-body');
      const stdinFile = body === undefined ? undefined : tempFilePath(dir, 's3-stdin');
      const args = [
        '-sS',
        '-X',
        method,
        '-D',
        headerFile,
        '-o',
        bodyFile,
        '-w',
        '%{http_code}',
      ];
      for (const [name, value] of Object.entries(headers)) {
        args.push('-H', `${name}: ${value}`);
      }
      if (stdinFile !== undefined) {
        writeFileSync(stdinFile, Buffer.from(body as Uint8Array));
        args.push('--data-binary', `@${stdinFile}`);
      }
      args.push(url);
      let stdout: string;
      try {
        stdout = execFileSync('curl', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
      } catch (err) {
        throw new Error(
          `s3 transport: curl failed for ${method} ${url}: ${(err as Error).message}`,
        );
      } finally {
        if (stdinFile !== undefined) {
          try {
            unlinkSync(stdinFile);
          } catch {
            // Best-effort cleanup.
          }
        }
      }
      const status = Number.parseInt(stdout.trim().slice(-3), 10);
      let rawHeaders = '';
      try {
        rawHeaders = readFileSync(headerFile, 'utf8');
      } catch {
        // A failed header capture surfaces as status 0 below.
      }
      let responseBody = new Uint8Array(0);
      try {
        responseBody = readFileSync(bodyFile);
      } catch {
        // Missing body file: treat as empty, status check decides.
      }
      for (const f of [headerFile, bodyFile]) {
        try {
          unlinkSync(f);
        } catch {
          // Best-effort cleanup.
        }
      }
      if (!Number.isInteger(status)) {
        throw new Error(`s3 transport: could not parse curl status for ${method} ${url}`);
      }
      const parsedHeaders: Record<string, string> = {};
      for (const line of rawHeaders.split('\r\n')) {
        const idx = line.indexOf(':');
        if (idx > 0) {
          parsedHeaders[line.slice(0, idx).trim().toLowerCase()] = line.slice(idx + 1).trim();
        }
      }
      return { status, headers: parsedHeaders, body: responseBody };
    },
  };
}

/** Parses `<Key>` entries out of a ListObjectsV2 XML response. */
export function parseListObjectsV2Keys(xml: string): string[] {
  const keys: string[] = [];
  const re = /<Key>([^<]*)<\/Key>/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(xml)) !== null) {
    keys.push(decodeXmlEntities(match[1]));
  }
  return keys;
}

/** Parses `<IsTruncated>` / `<NextContinuationToken>` from the same XML. */
export function parseListObjectsV2Continuation(xml: string): string | undefined {
  if (/<IsTruncated>true<\/IsTruncated>/.test(xml)) {
    const m = /<NextContinuationToken>([^<]*)<\/NextContinuationToken>/.exec(xml);
    if (m !== null) return decodeXmlEntities(m[1]);
  }
  return undefined;
}

/** Maximum ListObjectsV2 pages followed by one `list` call. */
export const S3_LIST_MAX_PAGES = 100;

class S3CompatibleStorageBackend implements DurableStorageBackend {
  private readonly endpoint: string;
  private readonly bucket: string;
  private readonly prefix: string;
  private readonly signRequest: S3RequestSigner;
  private readonly transport: S3HttpTransport;

  constructor(options: S3CompatibleBackendOptions) {
    const { endpoint, bucket, signRequest } = options;
    if (typeof endpoint !== 'string' || endpoint.length === 0 || endpoint.endsWith('/')) {
      throw new RangeError('S3CompatibleBackend: endpoint must be a non-empty URL without trailing slash');
    }
    if (typeof bucket !== 'string' || bucket.length === 0) {
      throw new RangeError('S3CompatibleBackend: bucket must be a non-empty string');
    }
    if (typeof signRequest !== 'function') {
      throw new RangeError('S3CompatibleBackend: signRequest must be a function');
    }
    this.endpoint = endpoint;
    this.bucket = bucket;
    this.prefix = options.prefix === undefined || options.prefix === '' ? '' : normalizeStoragePrefix(options.prefix, 'S3CompatibleBackend');
    this.signRequest = signRequest;
    this.transport = options.transport ?? curlSyncTransport();
  }

  private objectUrl(key: string): string {
    return `${this.endpoint}/${this.bucket}/${this.prefix}${encodeKey(key)}`;
  }

  private request(
    method: string,
    url: string,
    headers: Record<string, string>,
    body?: Uint8Array,
  ): S3HttpResponse {
    return this.transport.request(method, url, this.signRequest(method, url, headers, body), body);
  }

  private headObject(key: string): boolean {
    const res = this.request('HEAD', this.objectUrl(key), {});
    if (res.status === 200) return true;
    if (res.status === 404) return false;
    throw new Error(`s3 HEAD ${key}: unexpected status ${res.status}`);
  }

  write(key: string, data: Uint8Array | string, opts?: StorageWriteOptions): void {
    if (opts?.exclusive === true && this.headObject(key)) {
      // Best-effort exclusivity (see module docs): a concurrent writer
      // can still win the race; the archive pass treats that as benign.
      throw new Error(`s3 write: object already exists: ${key}`);
    }
    const body = typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
    const res = this.request('PUT', this.objectUrl(key), { 'content-length': String(body.length) }, body);
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`s3 PUT ${key}: unexpected status ${res.status}`);
    }
  }

  append(key: string, data: Uint8Array | string): void {
    // S3 has no append: read-modify-write. O(object) per append — see
    // the module docs on why an S3-backed log wants small segments.
    const prev = this.read(key) ?? new Uint8Array(0);
    const extra = typeof data === 'string' ? Buffer.from(data, 'utf8') : data;
    const merged = Buffer.concat([Buffer.from(prev), Buffer.from(extra)]);
    this.write(key, merged);
  }

  read(key: string): Uint8Array | undefined {
    const res = this.request('GET', this.objectUrl(key), {});
    if (res.status === 404) return undefined;
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`s3 GET ${key}: unexpected status ${res.status}`);
    }
    return res.body;
  }

  list(prefix = ''): string[] {
    const out: string[] = [];
    const fullPrefix = `${this.prefix}${prefix}`;
    let token: string | undefined;
    for (let page = 0; page < S3_LIST_MAX_PAGES; page += 1) {
      const params = new URLSearchParams({
        'list-type': '2',
        prefix: fullPrefix,
        'max-keys': '1000',
      });
      if (token !== undefined) params.set('continuation-token', token);
      const url = `${this.endpoint}/${this.bucket}?${params.toString()}`;
      const res = this.request('GET', url, {});
      if (res.status < 200 || res.status >= 300) {
        throw new Error(`s3 LIST prefix=${fullPrefix}: unexpected status ${res.status}`);
      }
      const xml = Buffer.from(res.body).toString('utf8');
      for (const objectKey of parseListObjectsV2Keys(xml)) {
        if (objectKey.startsWith(fullPrefix)) {
          out.push(objectKey.slice(fullPrefix.length));
        }
      }
      token = parseListObjectsV2Continuation(xml);
      if (token === undefined) return out;
    }
    throw new Error(`s3 LIST prefix=${fullPrefix}: exceeded ${S3_LIST_MAX_PAGES} pages`);
  }

  delete(key: string): void {
    // S3 DELETE is idempotent: deleting a missing object is 204.
    const res = this.request('DELETE', this.objectUrl(key), {});
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`s3 DELETE ${key}: unexpected status ${res.status}`);
    }
  }

  rename(oldKey: string, newKey: string): void {
    // Server-side copy then delete. Not atomic across the two calls —
    // the log only needs "bytes end up under the new name".
    const copySource = `/${this.bucket}/${this.prefix}${encodeKey(oldKey)}`;
    const res = this.request('PUT', this.objectUrl(newKey), { 'x-amz-copy-source': copySource });
    if (res.status < 200 || res.status >= 300) {
      throw new Error(`s3 COPY ${oldKey} -> ${newKey}: unexpected status ${res.status}`);
    }
    this.delete(oldKey);
  }

  scoped(prefix: string): DurableStorageBackend {
    return new PrefixedStorageBackend(this, normalizeStoragePrefix(prefix, 'scoped'));
  }
}

/**
 * Builds the S3-compatible example backend. See the module docs for the
 * contract, the mapping, and what production hardening is deliberately
 * left to the caller.
 */
export function createS3CompatibleBackend(
  options: S3CompatibleBackendOptions,
): DurableStorageBackend {
  return new S3CompatibleStorageBackend(options);
}
