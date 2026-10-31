/**
 * OTLP/HTTP JSON span exporter (EB-105).
 *
 * Ships the delivery-pipeline trace spans produced by `src/trace.ts`
 * (EB-45) to an OpenTelemetry collector over OTLP/HTTP (`POST
 * <endpoint>`, JSON encoding). Hand-written OTLP mapping, zero new
 * dependencies — no OTel SDK.
 *
 * Wire-up: `EventBusOptions.otlp` (opt-in). The bus requires tracing to
 * be enabled (`EventBusOptions.trace`) when `otlp` is set — spans only
 * exist when tracing samples them — and composes the exporter behind the
 * trace pipeline's `onTraceSpan` fan-out, so a user-supplied
 * `onTraceSpan` keeps working alongside export.
 *
 * Delivery semantics are best-effort, mirroring the trace pipeline's own
 * observer contract: `exportSpan` never throws and never blocks the
 * publish/deliver path. Spans buffer in memory (bounded, drop-oldest)
 * and flush on a timer (unref'd, so it never holds the process open) or
 * when the buffer reaches `batchSize`. A failed flush drops that batch
 * and counts it in `failedSpans` — the exporter does not retry, and a
 * down collector surfaces as a rising `failedSpans` counter in
 * `getStats().otlp`, not as backpressure on delivery.
 *
 * `traceId`/`spanId` need no translation: EB-45 already mints 32-hex
 * trace ids and 16-hex span ids — exactly the OTLP 128-bit/64-bit
 * format — so a trace exported from here joins the same trace a
 * `webhook-relay-ts` WR-18/WR-83 pipeline reports by string equality.
 */

import { request as httpRequest } from 'node:http';
import { request as httpsRequest } from 'node:https';
import { Buffer } from 'node:buffer';
import type { TraceSpan } from './trace.ts';

/** Tuning for opt-in OTLP span export (`EventBusOptions.otlp`). */
export interface OtlpSpanExporterOptions {
  /**
   * OTLP/HTTP traces endpoint, e.g.
   * `http://collector:4318/v1/traces`. Required. Must be an
   * `http:`/`https:` URL.
   */
  endpoint: string;
  /**
   * Export-side sampling rate: the fraction of spans handed to the
   * exporter that are actually buffered for export. Independent of the
   * trace pipeline's head-based sampling — trace everything locally,
   * export a fraction. Must be a finite number in [0, 1]. Default 1.
   */
  sampleRate?: number;
  /**
   * Spans per export POST. A flush also fires as soon as the buffer
   * reaches this size, so a busy bus does not wait for the timer. Must
   * be a positive integer. Default 512.
   */
  batchSize?: number;
  /**
   * Periodic flush interval in milliseconds. Must be a finite number >
   * 0. Default 5000. The timer is unref'd and never holds the process
   * open; call `flush()`/`shutdown()` for a deterministic final drain.
   */
  flushIntervalMs?: number;
  /**
   * Per-request timeout in milliseconds. Must be a finite number > 0.
   * Default 5000.
   */
  timeoutMs?: number;
  /**
   * Extra HTTP headers for the export POST (e.g. `Authorization`).
   * Header names/values must be strings. Default `{}`.
   */
  headers?: Record<string, string>;
  /**
   * `service.name` resource attribute. Must be a non-empty string.
   * Default `'realtime-event-bus'`.
   */
  serviceName?: string;
  /**
   * Maximum buffered spans. Past this bound the oldest spans are
   * dropped and counted in `droppedSpans`. Must be a positive integer.
   * Default 10000.
   */
  maxBuffered?: number;
}

/** Normalized OTLP exporter options; `undefined` means disabled. */
export interface ResolvedOtlpSpanExporterOptions {
  endpoint: URL;
  sampleRate: number;
  batchSize: number;
  flushIntervalMs: number;
  timeoutMs: number;
  headers: Record<string, string>;
  serviceName: string;
  maxBuffered: number;
}

/**
 * Revision for the bus-level OTLP export tuning (see
 * `EventBus.setOtlp`, EB-299). Only the fields provided move; the rest
 * keep their live values. `endpoint`, `headers`, and `serviceName` are
 * not revisable here — they are the export destination and identity
 * fixed at construction — and neither is `flushIntervalMs`: the flush
 * timer is armed once at construction, so changing the interval means
 * rebuilding the exporter (and the bus).
 */
export interface OtlpRevision {
  /**
   * Export-side sampling rate. Must be a finite number in [0, 1] when
   * provided — the construction-time rule.
   */
  sampleRate?: number;
  /**
   * Spans per export POST. Must be a positive integer when provided —
   * the construction-time rule.
   */
  batchSize?: number;
  /**
   * Per-request timeout in milliseconds. Must be a finite number > 0
   * when provided — the construction-time rule.
   */
  timeoutMs?: number;
  /**
   * Maximum buffered spans. Must be a positive integer when provided —
   * the construction-time rule.
   */
  maxBuffered?: number;
}

/** Exporter counters, surfaced as `getStats().otlp`. */
export interface OtlpSpanExportStats {
  /** The configured endpoint, as a string. */
  endpoint: string;
  /** Spans currently buffered, awaiting flush. */
  bufferedSpans: number;
  /** Spans successfully POSTed to the collector. */
  exportedSpans: number;
  /** Spans dropped because their export POST failed (best-effort). */
  failedSpans: number;
  /** Spans dropped because the buffer was full (drop-oldest). */
  droppedSpans: number;
}

const DEFAULT_SERVICE_NAME = 'realtime-event-bus';
const DEFAULT_BATCH_SIZE = 512;
const DEFAULT_FLUSH_INTERVAL_MS = 5000;
const DEFAULT_TIMEOUT_MS = 5000;
const DEFAULT_MAX_BUFFERED = 10000;

/**
 * Normalizes `EventBusOptions.otlp`. Returns `undefined` when export is
 * disabled (absent, `null`, `false`); anything else invalid throws
 * `RangeError` at construction time.
 */
export function resolveOtlpOptions(
  raw: OtlpSpanExporterOptions | false | null | undefined,
  where = 'EventBus',
): ResolvedOtlpSpanExporterOptions | undefined {
  if (raw == null || raw === false) return undefined;
  if (typeof raw !== 'object') {
    throw new RangeError(`${where}: otlp must be an options object or false`);
  }
  if (typeof raw.endpoint !== 'string' || raw.endpoint.length === 0) {
    throw new RangeError(`${where}: otlp.endpoint must be a non-empty URL string`);
  }
  let endpoint: URL;
  try {
    endpoint = new URL(raw.endpoint);
  } catch {
    throw new RangeError(`${where}: otlp.endpoint must be a valid URL`);
  }
  if (endpoint.protocol !== 'http:' && endpoint.protocol !== 'https:') {
    throw new RangeError(`${where}: otlp.endpoint must be an http(s) URL`);
  }
  const sampleRate = raw.sampleRate ?? 1;
  if (!Number.isFinite(sampleRate) || sampleRate < 0 || sampleRate > 1) {
    throw new RangeError(`${where}: otlp.sampleRate must be a finite number in [0, 1]`);
  }
  const batchSize = raw.batchSize ?? DEFAULT_BATCH_SIZE;
  if (!Number.isInteger(batchSize) || batchSize < 1) {
    throw new RangeError(`${where}: otlp.batchSize must be a positive integer`);
  }
  const flushIntervalMs = raw.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS;
  if (!Number.isFinite(flushIntervalMs) || flushIntervalMs <= 0) {
    throw new RangeError(`${where}: otlp.flushIntervalMs must be a finite number > 0`);
  }
  const timeoutMs = raw.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new RangeError(`${where}: otlp.timeoutMs must be a finite number > 0`);
  }
  const headers = raw.headers ?? {};
  if (typeof headers !== 'object' || headers === null || Array.isArray(headers)) {
    throw new RangeError(`${where}: otlp.headers must be a string-valued object`);
  }
  for (const [k, v] of Object.entries(headers)) {
    if (typeof k !== 'string' || typeof v !== 'string') {
      throw new RangeError(`${where}: otlp.headers must be a string-valued object`);
    }
  }
  const serviceName = raw.serviceName ?? DEFAULT_SERVICE_NAME;
  if (typeof serviceName !== 'string' || serviceName.length === 0) {
    throw new RangeError(`${where}: otlp.serviceName must be a non-empty string`);
  }
  const maxBuffered = raw.maxBuffered ?? DEFAULT_MAX_BUFFERED;
  if (!Number.isInteger(maxBuffered) || maxBuffered < 1) {
    throw new RangeError(`${where}: otlp.maxBuffered must be a positive integer`);
  }
  return {
    endpoint,
    sampleRate,
    batchSize,
    flushIntervalMs,
    timeoutMs,
    headers: { ...headers },
    serviceName,
    maxBuffered,
  };
}

/** Milliseconds since the epoch → OTLP `timeUnixNano` (decimal string). */
function toUnixNano(ms: number): string {
  const safe = Number.isFinite(ms) ? Math.max(0, Math.round(ms)) : 0;
  return (BigInt(safe) * 1_000_000n).toString();
}

type OtlpAttributeValue =
  | { stringValue: string }
  | { intValue: string }
  | { doubleValue: number }
  | { boolValue: boolean };

/** Maps a trace-span attribute to its OTLP `AnyValue` JSON shape. */
function toOtlpAttributeValue(value: string | number | boolean): OtlpAttributeValue {
  if (typeof value === 'string') return { stringValue: value };
  if (typeof value === 'boolean') return { boolValue: value };
  if (Number.isInteger(value)) return { intValue: String(value) };
  return { doubleValue: value };
}

/** One `TraceSpan` → one OTLP `Span` JSON object. */
function encodeSpan(span: TraceSpan): Record<string, unknown> {
  const attributes = Object.entries(span.attrs).map(([key, value]) => ({
    key,
    value: toOtlpAttributeValue(value),
  }));
  const encoded: Record<string, unknown> = {
    traceId: span.traceId,
    spanId: span.spanId,
    name: span.name,
    kind: 1, // SPAN_KIND_INTERNAL
    startTimeUnixNano: toUnixNano(span.at),
    endTimeUnixNano: toUnixNano(span.at + span.durationMs),
    attributes,
    status: {},
  };
  if (span.parentId !== undefined) encoded.parentSpanId = span.parentId;
  return encoded;
}

/** A batch of spans → the OTLP/HTTP `TracesData` JSON body. */
function encodeBatch(serviceName: string, batch: TraceSpan[]): string {
  return JSON.stringify({
    resourceSpans: [
      {
        resource: {
          attributes: [{ key: 'service.name', value: { stringValue: serviceName } }],
        },
        scopeSpans: [
          {
            scope: { name: 'realtime-event-bus' },
            spans: batch.map(encodeSpan),
          },
        ],
      },
    ],
  });
}

/**
 * Best-effort OTLP/HTTP span exporter. Constructed once per bus when
 * `EventBusOptions.otlp` is set; the bus feeds it through the trace
 * pipeline's span fan-out.
 */
export class OtlpSpanExporter {
  private readonly endpoint: URL;
  // Live-tuned by `retune` (EB-299): every use point reads these fields
  // on each call — `exportSpan` samples at the live `sampleRate` and
  // drops the oldest past the live `maxBuffered`, flushes at the live
  // `batchSize`, `flushInner` slices at the live `batchSize`, and
  // `postBatch` times out at the live `timeoutMs` — so a revision
  // governs from the next admission/flush/POST without draining or
  // rebuilding the exporter. `endpoint`, `headers`, and `serviceName`
  // stay fixed for the exporter's lifetime.
  private sampleRate: number;
  private batchSize: number;
  private readonly flushIntervalMs: number;
  private timeoutMs: number;
  private readonly headers: Record<string, string>;
  private readonly serviceName: string;
  private maxBuffered: number;
  private readonly buffer: TraceSpan[] = [];
  private readonly timer: ReturnType<typeof setInterval>;
  private flushPromise: Promise<void> | undefined;
  private exportedSpans = 0;
  private failedSpans = 0;
  private droppedSpans = 0;

  constructor(resolved: ResolvedOtlpSpanExporterOptions) {
    this.endpoint = resolved.endpoint;
    this.sampleRate = resolved.sampleRate;
    this.batchSize = resolved.batchSize;
    this.flushIntervalMs = resolved.flushIntervalMs;
    this.timeoutMs = resolved.timeoutMs;
    this.headers = resolved.headers;
    this.serviceName = resolved.serviceName;
    this.maxBuffered = resolved.maxBuffered;
    // Unref'd: export must never hold the process open on its own.
    this.timer = setInterval(() => {
      void this.flush();
    }, resolved.flushIntervalMs);
    if (typeof this.timer.unref === 'function') this.timer.unref();
  }

  /** The live export tuning, for bus-level revision (EB-299). */
  current(): ResolvedOtlpSpanExporterOptions {
    return {
      endpoint: this.endpoint,
      sampleRate: this.sampleRate,
      batchSize: this.batchSize,
      flushIntervalMs: this.flushIntervalMs,
      timeoutMs: this.timeoutMs,
      headers: { ...this.headers },
      serviceName: this.serviceName,
      maxBuffered: this.maxBuffered,
    };
  }

  /**
   * Retunes the four numeric export bounds live (EB-299). The caller
   * must have validated `next` through `resolveOtlpOptions` first —
   * this method applies unconditionally. In-flight buffered spans are
   * untouched: already-buffered spans stay buffered, and the new values
   * govern from the next decision onward — the next span admission
   * samples at the live `sampleRate` and drops the oldest past the live
   * `maxBuffered`, the next flush triggers and slices at the live
   * `batchSize`, and the next export POST times out at the live
   * `timeoutMs`. `endpoint`, `headers`, `serviceName`, and
   * `flushIntervalMs` are never moved here.
   */
  retune(next: ResolvedOtlpSpanExporterOptions): void {
    this.sampleRate = next.sampleRate;
    this.batchSize = next.batchSize;
    this.timeoutMs = next.timeoutMs;
    this.maxBuffered = next.maxBuffered;
  }

  /**
   * Buffers one span for export. Best-effort and allocation-light on
   * the hot path: the export-side sampling decision first, then a
   * bounded push. Never throws — an exporter must not disturb the
   * publish/deliver path it observes.
   */
  exportSpan(span: TraceSpan): void {
    try {
      if (Math.random() >= this.sampleRate) return;
      this.buffer.push(span);
      if (this.buffer.length > this.maxBuffered) {
        const overflow = this.buffer.length - this.maxBuffered;
        this.buffer.splice(0, overflow);
        this.droppedSpans += overflow;
      }
      if (this.buffer.length >= this.batchSize) void this.flush();
    } catch {
      // Swallowed: observation must never break delivery.
    }
  }

  /**
   * Flushes every buffered span to the collector, in `batchSize`
   * batches. Concurrent callers share one in-flight flush. Never
   * throws: a failed batch is dropped and counted in `failedSpans`.
   */
  flush(): Promise<void> {
    if (this.flushPromise !== undefined) return this.flushPromise;
    const run = this.flushInner().catch(() => {
      // Defensive: flushInner already counts per-batch failures, so this
      // only guards against an unexpected internal error.
    });
    this.flushPromise = run.finally(() => {
      this.flushPromise = undefined;
    });
    return this.flushPromise;
  }

  private async flushInner(): Promise<void> {
    while (this.buffer.length > 0) {
      const batch = this.buffer.splice(0, this.batchSize);
      try {
        await this.postBatch(batch);
        this.exportedSpans += batch.length;
      } catch {
        // Best-effort: drop the failed batch, count it, keep draining
        // the rest — a down collector must not wedge the buffer.
        this.failedSpans += batch.length;
      }
    }
  }

  /** Stops the flush timer and drains the buffer one last time. */
  shutdown(): Promise<void> {
    clearInterval(this.timer);
    return this.flush();
  }

  /** Exporter counters. A snapshot — mutating it is safe. */
  getStats(): OtlpSpanExportStats {
    return {
      endpoint: this.endpoint.toString(),
      bufferedSpans: this.buffer.length,
      exportedSpans: this.exportedSpans,
      failedSpans: this.failedSpans,
      droppedSpans: this.droppedSpans,
    };
  }

  private postBatch(batch: TraceSpan[]): Promise<void> {
    const body = encodeBatch(this.serviceName, batch);
    const useHttps = this.endpoint.protocol === 'https:';
    const doRequest = useHttps ? httpsRequest : httpRequest;
    return new Promise<void>((resolve, reject) => {
      const req = doRequest(
        this.endpoint,
        {
          method: 'POST',
          // No connection pooling: one export POST per flush keeps the
          // exporter free of the keep-alive lifecycle (and of the
          // close-before-pool-drain hang class in tests).
          agent: false,
          headers: {
            'content-type': 'application/json',
            'content-length': Buffer.byteLength(body),
            ...this.headers,
          },
        },
        (res) => {
          // Drain the body so the socket can close cleanly.
          res.resume();
          res.on('end', () => {
            const status = res.statusCode ?? 0;
            if (status >= 200 && status < 300) resolve();
            else reject(new Error(`OTLP export failed with status ${status}`));
          });
        },
      );
      req.on('error', reject);
      req.setTimeout(this.timeoutMs, () => {
        req.destroy(new Error(`OTLP export timed out after ${this.timeoutMs}ms`));
      });
      req.end(body);
    });
  }
}
