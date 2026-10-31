/**
 * External admission webhook (EB-157): K8s-admission-style allow/deny
 * decisions for publishes, made by an operator-run HTTP(S) endpoint.
 *
 * When `EventBusOptions.admissionWebhook` is set, every fresh publish
 * admission POSTs a small JSON decision request to the configured URL
 * and honors the response: `{"allowed": true}` admits, `{"allowed":
 * false}` sheds the publish with admission reason `'webhook-deny'`
 * (zero side effects — no sequence number, no durable-log write, no
 * rate-limit budget).
 *
 * The publish pipeline is synchronous, so the HTTP call is performed
 * synchronously: the request runs in a worker thread and the calling
 * thread blocks on an `Atomics.wait` with the configured timeout. A
 * timeout or transport error applies `failureMode` (`'fail-closed'`
 * denies, `'fail-open'` admits) — never a silent hang, never a throw
 * into the publish path.
 */
import { Worker } from 'node:worker_threads';

/**
 * Options for the external admission webhook (see
 * `EventBusOptions.admissionWebhook`, EB-157).
 */
export interface AdmissionWebhookOptions {
  /**
   * The webhook endpoint. Must be an `http:`/`https:` URL. The bus
   * POSTs `application/json` and expects a JSON body with a boolean
   * `allowed` field. Required.
   */
  url: string;
  /**
   * Milliseconds to wait for the webhook response before applying
   * `failureMode`. Must be a finite number > 0. Default: 1000.
   */
  timeoutMs?: number;
  /**
   * What to do when the webhook times out or the request fails:
   * - `'fail-closed'` (default): deny the publish (shed with
   *   `'webhook-deny'`) — safe for policy enforcement.
   * - `'fail-open'`: admit the publish — safe for availability when the
   *   webhook is advisory.
   */
  failureMode?: 'fail-open' | 'fail-closed';
  /**
   * Extra headers sent with the decision request (e.g. an
   * `Authorization` bearer token). Never logged.
   */
  headers?: Record<string, string>;
}

/**
 * A partial bus-level revision of the admission webhook tuning, for
 * `EventBus.setAdmissionWebhook` (EB-308). Only the fields provided
 * move; the rest keep their live values. `url` is not revisable here —
 * it is the policy endpoint's identity fixed at construction.
 */
export interface AdmissionWebhookRevision {
  /**
   * Milliseconds to wait for the webhook response before applying
   * `failureMode`. Must be a finite number > 0 when provided — the
   * construction-time rule.
   */
  timeoutMs?: number;
  /**
   * What to do when the webhook times out or the request fails. Must
   * be `'fail-open'` or `'fail-closed'` when provided — the
   * construction-time rule.
   */
  failureMode?: 'fail-open' | 'fail-closed';
  /**
   * Extra headers sent with the decision request. Must map strings to
   * strings when provided — the construction-time rule. Replaces the
   * whole header set; there is no per-header merge. Header values are
   * never logged or audited.
   */
  headers?: Record<string, string>;
}

/**
 * The decision request POSTed to the webhook. Carries the admission
 * context — never the payload body (the hook is a policy channel, and
 * large or sensitive payloads never travel through it).
 */
export interface AdmissionWebhookRequest {
  /** The alias-resolved concrete topic the publish targets. */
  topic: string;
  /** SHA-256 hex of the admission-time payload bytes. */
  payloadSha256: string;
  /** UTF-8 JSON byte size of the payload. */
  payloadBytes: number;
  /** Publisher identity, when the publish carried one. */
  publisherId?: string;
  /** Client-supplied message id, when present. */
  messageId?: string;
  /** W3C trace id, when the publish carried a traceparent. */
  traceId?: string;
}

/**
 * The webhook's decision, parsed from its JSON response body.
 */
export interface AdmissionWebhookDecision {
  /** Whether the publish is admitted. */
  allowed: boolean;
  /** Operator-supplied reason, surfaced in metrics when denied. */
  reason?: string;
}

/** Result of one synchronous webhook call. */
export interface AdmissionWebhookCallResult {
  /** Whether the HTTP round-trip completed (any 2xx with a body). */
  ok: boolean;
  /** The parsed decision, when `ok`. */
  decision?: AdmissionWebhookDecision;
  /** HTTP status, when the round-trip completed. */
  status?: number;
  /** Transport/timeout/parse error, when not `ok`. */
  error?: string;
}

/**
 * Validates `AdmissionWebhookOptions` at bus-construction time.
 * Throws `RangeError`/`TypeError` fail-fast, before any publish.
 */
export function validateAdmissionWebhookOptions(
  opts: AdmissionWebhookOptions | undefined,
  where: string,
): void {
  if (opts === undefined) return;
  if (typeof opts !== 'object' || opts === null) {
    throw new TypeError(`${where}: admissionWebhook must be an object`);
  }
  if (typeof opts.url !== 'string' || opts.url.length === 0) {
    throw new TypeError(`${where}: admissionWebhook.url must be a non-empty string`);
  }
  let protocol: string;
  try {
    protocol = new URL(opts.url).protocol;
  } catch {
    throw new RangeError(`${where}: admissionWebhook.url must be a valid URL`);
  }
  if (protocol !== 'http:' && protocol !== 'https:') {
    throw new RangeError(`${where}: admissionWebhook.url must be an http(s) URL`);
  }
  if (opts.timeoutMs !== undefined) {
    if (typeof opts.timeoutMs !== 'number' || !Number.isFinite(opts.timeoutMs) || opts.timeoutMs <= 0) {
      throw new RangeError(`${where}: admissionWebhook.timeoutMs must be a finite number > 0`);
    }
  }
  if (opts.failureMode !== undefined && opts.failureMode !== 'fail-open' && opts.failureMode !== 'fail-closed') {
    throw new RangeError(`${where}: admissionWebhook.failureMode must be 'fail-open' or 'fail-closed'`);
  }
  if (opts.headers !== undefined) {
    if (typeof opts.headers !== 'object' || opts.headers === null) {
      throw new TypeError(`${where}: admissionWebhook.headers must be an object`);
    }
    for (const [k, v] of Object.entries(opts.headers)) {
      if (typeof k !== 'string' || typeof v !== 'string') {
        throw new TypeError(`${where}: admissionWebhook.headers must map strings to strings`);
      }
    }
  }
}

const WORKER_SCRIPT = `
const { workerData } = require('node:worker_threads');
const { sab, url, body, headers } = workerData;
const flag = new Int32Array(sab, 0, 1);
const lenView = new Int32Array(sab, 4, 1);
const bytesView = new Uint8Array(sab, 8);
async function main() {
  let result;
  try {
    // No AbortController here: the main thread enforces the timeout via
    // Atomics.wait and terminates a hung worker. Keeping the worker
    // simple avoids timer/abort edge cases in eval'd workers.
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body,
    });
    const text = await res.text();
    result = { ok: true, status: res.status, body: text };
  } catch (err) {
    const message = String((err && err.message) || err);
    result = { ok: false, error: message };
  } finally {
    // Write the JSON result into shared memory: length prefix + UTF-8 bytes.
    // The main thread is blocked in Atomics.wait and cannot process a
    // 'message' event, so shared memory is the only channel.
    try {
      const json = JSON.stringify(result);
      const bytes = Buffer.from(json, 'utf8');
      const maxLen = bytesView.length;
      const writeLen = Math.min(bytes.length, maxLen);
      bytesView.set(bytes.subarray(0, writeLen));
      lenView[0] = writeLen;
      // If truncated, mark as error.
      if (writeLen < bytes.length) {
        const trunc = JSON.stringify({ ok: false, error: 'webhook response too large' });
        const truncBytes = Buffer.from(trunc, 'utf8');
        bytesView.set(truncBytes.subarray(0, Math.min(truncBytes.length, maxLen)));
        lenView[0] = Math.min(truncBytes.length, maxLen);
      }
    } catch (e) {
      const fallback = JSON.stringify({ ok: false, error: 'failed to serialize result' });
      const fb = Buffer.from(fallback, 'utf8');
      bytesView.set(fb.subarray(0, Math.min(fb.length, bytesView.length)));
      lenView[0] = Math.min(fb.length, bytesView.length);
    }
    Atomics.store(flag, 0, 1);
    Atomics.notify(flag, 0, 1);
  }
}
main();
`;

/**
 * Performs one synchronous JSON POST to the admission webhook.
 * Blocks the calling thread (via `Atomics.wait` on a worker-thread
 * signal) for at most `timeoutMs`. Never throws — transport failures
 * surface as `{ ok: false, error }`.
 *
 * The result travels via shared memory (not a 'message' event) because
 * `Atomics.wait` blocks the event loop: the worker writes the JSON
 * result bytes into the shared buffer, then sets the flag. The timeout
 * is enforced by the main thread's `Atomics.wait`; a hung worker is
 * terminated. The worker itself uses no timers or AbortController to
 * avoid edge cases in eval'd workers.
 */
export function syncAdmissionWebhookPost(
  url: string,
  body: string,
  headers: Record<string, string>,
  timeoutMs: number,
): AdmissionWebhookCallResult {
  // Layout: [0] flag (Int32), [1] result byte length (Int32),
  // [2..] result JSON UTF-8 bytes (64KB).
  const sab = new SharedArrayBuffer(4 + 4 + 65536);
  const flag = new Int32Array(sab, 0, 1);
  const lenView = new Int32Array(sab, 4, 1);
  const bytesView = new Uint8Array(sab, 8);
  let worker: Worker | undefined;
  try {
    worker = new Worker(WORKER_SCRIPT, {
      eval: true,
      // Note: timeoutMs is NOT passed to the worker — the main thread
      // enforces the timeout via Atomics.wait below and terminates a
      // hung worker. This avoids AbortController/timer edge cases in
      // eval'd workers.
      workerData: { sab, url, body, headers },
    });
    worker.on('error', () => {
      // The flag will still be set by the worker's finally block; if
      // the worker dies before that, the wait below times out.
    });
    // The worker signals via the flag when the result bytes are
    // written. The timeout is enforced here: if the worker hasn't
    // finished in time, we terminate it and report a timeout.
    const waitResult = Atomics.wait(flag, 0, 0, timeoutMs);
    if (waitResult === 'timed-out') {
      return { ok: false, error: 'timeout' };
    }
    const len = lenView[0];
    if (len <= 0 || len > bytesView.length) {
      return { ok: false, error: 'webhook returned no result' };
    }
    const json = Buffer.from(bytesView.subarray(0, len)).toString('utf8');
    try {
      return JSON.parse(json) as AdmissionWebhookCallResult;
    } catch {
      return { ok: false, error: 'failed to parse webhook result' };
    }
  } catch (err) {
    return { ok: false, error: String((err as Error)?.message ?? err) };
  } finally {
    try {
      worker?.terminate();
    } catch {
      // Terminating a finished worker is best-effort.
    }
  }
}

/**
 * Calls the admission webhook and interprets the response as an
 * allow/deny decision. Applies `failureMode` on timeout, transport
 * error, non-2xx status, or unparsable body. Never throws.
 */
export function decideAdmission(
  options: AdmissionWebhookOptions,
  request: AdmissionWebhookRequest,
): { allowed: boolean; error?: string; webhookReason?: string } {
  const timeoutMs = options.timeoutMs ?? 1000;
  const failureMode = options.failureMode ?? 'fail-closed';
  const failClosed = failureMode === 'fail-closed';

  const call = syncAdmissionWebhookPost(
    options.url,
    JSON.stringify(request),
    options.headers ?? {},
    timeoutMs,
  );
  if (!call.ok) {
    return { allowed: !failClosed, error: call.error ?? 'webhook call failed' };
  }
  if (call.status === undefined || call.status < 200 || call.status >= 300) {
    return { allowed: !failClosed, error: `webhook returned HTTP ${call.status}` };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(call.body ?? '');
  } catch {
    return { allowed: !failClosed, error: 'webhook returned unparsable JSON' };
  }
  if (typeof parsed !== 'object' || parsed === null || typeof (parsed as { allowed?: unknown }).allowed !== 'boolean') {
    return { allowed: !failClosed, error: 'webhook response missing boolean `allowed`' };
  }
  const decision = parsed as AdmissionWebhookDecision;
  return { allowed: decision.allowed, webhookReason: decision.reason };
}
