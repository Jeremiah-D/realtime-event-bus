/**
 * Opt-in delivery-pipeline trace spans (EB-45).
 *
 * When enabled (`EventBusOptions.trace`), the bus emits one trace per
 * sampled publish, threading the message through the delivery pipeline —
 * `bus.publish` → `bus.admission` → `bus.fanout` → `bus.enqueue` (per
 * subscriber) → `bus.deliver` (per subscriber) → `bus.ack` (reliable
 * subscriptions, on `ack()`). Each span carries the W3C-style shape
 * `{ traceId, spanId, parentId, name, at, durationMs, attrs }`:
 *
 * - `traceId` is 32 lowercase hex chars — the same format as the
 *   `webhook-relay-ts` WR-18 trace ID (a UUID with the dashes stripped),
 *   so a trace that crosses from this bus into the relay correlates by
 *   string equality.
 * - `spanId` is 16 lowercase hex chars, unique per span.
 * - `parentId` links each span to its parent (`bus.publish` is the root;
 *   an incoming `traceparent` continues an upstream trace, so the root's
 *   `parentId` is the upstream span).
 *
 * Sampling is head-based: the decision is taken once, at admission time,
 * and every downstream span of the same publish shares the verdict. The
 * default head sampler keeps a fixed `sampleRate` fraction
 * (`Math.random() < sampleRate`); pass `sampler` to plug in a custom
 * decision (it overrides the rate). Sampled spans go to the
 * `onTraceSpan` callback and to a bounded ring buffer exported as
 * `getStats().traceSpans` (oldest evicted first).
 *
 * Disabled by default, and the disabled path is allocation-free: every
 * instrumentation site first reads the bus's single `trace` field and
 * returns past one branch — no WeakMap lookup, no clock read, no object
 * creation. When enabled, an unsampled message pays only the one-time
 * sampling decision plus one WeakMap lookup per pipeline stage it passes
 * through — still no span allocation, no clock read, no callback.
 *
 * Context propagation: a publish may carry an upstream W3C `traceparent`
 * header (`PublishOptions.traceparent`, `00-<32hex>-<16hex>-<flags>`); a
 * valid header continues that trace (the root span's `parentId` is the
 * header's span id), a missing or malformed one mints a fresh trace id —
 * the same lenient rule `webhook-relay-ts` applies to its `x-trace-id`
 * header. `formatTraceparent` renders the current context for forwarding
 * downstream. The per-message trace record rides in a `WeakMap` keyed by
 * the `BusMessage`, so replayed / cluster-received messages (different
 * objects) never inherit a trace, and records die with their message.
 */

import { randomBytes } from 'node:crypto';

/** One span of a sampled delivery trace. */
export interface TraceSpan {
  /**
   * 32 lowercase hex chars, shared by every span of one trace. Same
   * format as the `webhook-relay-ts` WR-18 trace ID, so traces correlate
   * across the two by string equality.
   */
  traceId: string;
  /** 16 lowercase hex chars, unique per span. */
  spanId: string;
  /**
   * The parent span's id. Absent on the `bus.publish` root span, unless
   * the publish continued an upstream trace via `traceparent` — then it
   * is the upstream span id.
   */
  parentId?: string;
  /**
   * Pipeline stage: `'bus.publish'` (the whole publish call),
   * `'bus.admission'` (the admission gates), `'bus.fanout'` (matching +
   * enqueue fan-out), `'bus.enqueue'` (one per subscriber that accepted
   * the message), `'bus.deliver'` (one per subscriber hand-off to its
   * handler), `'bus.ack'` (one per acked reliable delivery).
   */
  name: string;
  /** Bus-clock timestamp (`EventBusOptions.now`) when the span started. */
  at: number;
  /** Span duration in milliseconds on the bus clock, clamped at 0. */
  durationMs: number;
  /** Stage attributes: topic, seq, subscriberId / pattern, matched counts… */
  attrs: Record<string, string | number | boolean>;
}

/** Input to a custom head sampler (`TraceOptions.sampler`). */
export interface TraceSamplingDecision {
  /** The trace id this publish would start (or continue). */
  traceId: string;
  /** The concrete topic being published. */
  topic: string;
}

/**
 * Pluggable head sampler: returns `true` to trace this publish.
 * Overrides `sampleRate` when provided. A throwing sampler propagates to
 * the publish call, like a throwing content filter or schema validator.
 */
export type TraceSampler = (decision: TraceSamplingDecision) => boolean;

/** Tuning for opt-in delivery tracing (`EventBusOptions.trace`). */
export interface TraceOptions {
  /**
   * Master switch. An options object implies `true`; pass `false`
   * explicitly (or omit `trace`) to disable. Default `true` when the
   * object form is used.
   */
  enabled?: boolean;
  /**
   * Fixed head-based sampling rate: the fraction of admitted publishes
   * that start a trace (`Math.random() < sampleRate`). Must be a finite
   * number in [0, 1]. Default 1 (trace everything admitted). Ignored when
   * `sampler` is provided.
   */
  sampleRate?: number;
  /**
   * Custom head sampler, overriding `sampleRate`. Receives the candidate
   * trace id and topic; return `true` to trace the publish.
   */
  sampler?: TraceSampler;
  /**
   * Called synchronously with every sampled span, in completion order.
   * Error-isolated: a throwing callback is swallowed so a broken observer
   * can never disturb the publish/deliver path.
   */
  onTraceSpan?: (span: TraceSpan) => void;
  /**
   * Ring-buffer capacity for `getStats().traceSpans`: the oldest spans
   * are evicted past this bound. Must be a positive integer. Default
   * 1024.
   */
  bufferSize?: number;
}

/** Normalized trace options; `undefined` (from the resolver) means disabled. */
export interface ResolvedTraceOptions {
  sampleRate: number;
  sampler?: TraceSampler;
  onTraceSpan?: (span: TraceSpan) => void;
  bufferSize: number;
}

/**
 * Minimal subscriber identity a trace span needs. The bus's private
 * `Subscriber` satisfies this structurally.
 */
export interface TraceSubscriberRef {
  id: string;
  pattern: string;
}

/** W3C traceparent: `00-<32hex trace id>-<16hex parent id>-<2hex flags>`. */
const TRACEPARENT_PATTERN = /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
const ZERO_TRACE_ID = '0'.repeat(32);
const ZERO_SPAN_ID = '0'.repeat(16);

/** Fresh 32-hex-char trace id — the WR-18 format (UUID sans dashes). */
export function newTraceId(): string {
  return randomBytes(16).toString('hex');
}

/** Fresh 16-hex-char span id. */
export function newSpanId(): string {
  return randomBytes(8).toString('hex');
}

/**
 * Parses a W3C `traceparent` header value. Returns the trace id and the
 * upstream span id, or `undefined` for a missing/malformed value (wrong
 * shape, `ff` version, all-zero ids) — the caller then mints a fresh
 * trace, the same lenient rule `webhook-relay-ts` applies to `x-trace-id`.
 */
export function parseTraceparent(
  header: string,
): { traceId: string; parentSpanId: string } | undefined {
  if (typeof header !== 'string') return undefined;
  const match = TRACEPARENT_PATTERN.exec(header.trim());
  if (match == null) return undefined;
  const [, version, traceId, parentSpanId] = match;
  if (version === 'ff' || traceId === ZERO_TRACE_ID || parentSpanId === ZERO_SPAN_ID) {
    return undefined;
  }
  return { traceId, parentSpanId };
}

/** Renders a W3C `traceparent` header value for this trace/span. */
export function formatTraceparent(traceId: string, spanId: string): string {
  return `00-${traceId}-${spanId}-01`;
}

/**
 * Validates a user-supplied `traceparent` publish option. A malformed
 * *value* is tolerated (it mints a fresh trace at sample time), but a
 * non-string is a programmer error and throws `RangeError` — mirroring
 * `validateMessageKey`.
 */
export function validateTraceparent(value: unknown, where: string): void {
  if (value === undefined) return;
  if (typeof value !== 'string') {
    throw new RangeError(`${where}: traceparent must be a string`);
  }
}

/**
 * Normalizes `EventBusOptions.trace`. Returns `undefined` when tracing is
 * disabled (absent, `null`, `false`, or `{ enabled: false }`); anything
 * else invalid throws `RangeError` at construction time.
 */
export function resolveTraceOptions(
  raw: boolean | TraceOptions | null | undefined,
  where = 'EventBus',
): ResolvedTraceOptions | undefined {
  if (raw == null || raw === false) return undefined;
  if (raw === true) {
    return { sampleRate: 1, sampler: undefined, onTraceSpan: undefined, bufferSize: 1024 };
  }
  if (typeof raw !== 'object') {
    throw new RangeError(`${where}: trace must be a boolean or an options object`);
  }
  const enabled = raw.enabled ?? true;
  const sampleRate = raw.sampleRate ?? 1;
  if (!Number.isFinite(sampleRate) || sampleRate < 0 || sampleRate > 1) {
    throw new RangeError(`${where}: trace.sampleRate must be a finite number in [0, 1]`);
  }
  const sampler = raw.sampler;
  if (sampler !== undefined && typeof sampler !== 'function') {
    throw new RangeError(`${where}: trace.sampler must be a function`);
  }
  const onTraceSpan = raw.onTraceSpan;
  if (onTraceSpan !== undefined && typeof onTraceSpan !== 'function') {
    throw new RangeError(`${where}: trace.onTraceSpan must be a function`);
  }
  const bufferSize = raw.bufferSize ?? 1024;
  if (!Number.isInteger(bufferSize) || bufferSize < 1) {
    throw new RangeError(`${where}: trace.bufferSize must be a positive integer`);
  }
  if (enabled === false) return undefined;
  return { sampleRate, sampler, onTraceSpan, bufferSize };
}

/** Per-message trace record, keyed by the `BusMessage` in a `WeakMap`. */
interface MessageTraceState {
  traceId: string;
  publishSpanId: string;
  topic: string;
  seq: number;
  /** subscriberId -> the `bus.enqueue` span that admitted this message. */
  enqueues: Map<string, { spanId: string; at: number }>;
  /** Open `bus.ack` span for the currently outstanding reliable delivery. */
  openAck?: { spanId: string; at: number; redeliveries: number };
}

/** The sampled context handed from the sampling decision into `beginPublish`. */
export interface SampledTraceContext {
  traceId: string;
  parentSpanId?: string;
}

/**
 * Owns the sampling decision, the per-message trace records, the span
 * ring buffer, and the `onTraceSpan` fan-out. One instance per `EventBus`,
 * created only when tracing is enabled — the bus keeps the field
 * `undefined` otherwise, which is what makes the disabled path
 * allocation-free.
 */
export class TraceRecorder {
  private readonly sampleRate: number;
  private readonly sampler: TraceSampler | undefined;
  private readonly onTraceSpan: ((span: TraceSpan) => void) | undefined;
  private readonly traces = new WeakMap<object, MessageTraceState>();
  private readonly ring: TraceSpan[] = [];
  private ringHead = 0;
  private readonly capacity: number;

  constructor(resolved: ResolvedTraceOptions) {
    this.sampleRate = resolved.sampleRate;
    this.sampler = resolved.sampler;
    this.onTraceSpan = resolved.onTraceSpan;
    this.capacity = resolved.bufferSize;
  }

  /**
   * Head-based sampling decision, taken once at admission. Returns the
   * trace context for a sampled publish, or `undefined` when this
   * publish is not traced. A valid incoming `traceparent` continues its
   * trace; otherwise a fresh 32-hex trace id is minted.
   */
  sample(topic: string, traceparent?: string): SampledTraceContext | undefined {
    const incoming = traceparent === undefined ? undefined : parseTraceparent(traceparent);
    const traceId = incoming?.traceId ?? newTraceId();
    const keep =
      this.sampler !== undefined
        ? this.sampler({ traceId, topic })
        : Math.random() < this.sampleRate;
    if (!keep) return undefined;
    return { traceId, parentSpanId: incoming?.parentSpanId };
  }

  /**
   * Opens the `bus.publish` root span for a sampled, admitted message and
   * registers its trace record so the later pipeline stages (enqueue /
   * deliver / ack) can find it by message identity. Returns a handle whose
   * `end*` methods emit the spans in completion order: admission, fanout,
   * then the publish root.
   */
  beginPublish(
    msg: object,
    topic: string,
    seq: number,
    sampled: SampledTraceContext,
    at: number,
  ): {
    readonly traceId: string;
    endAdmission(endAt: number): void;
    endFanout(endAt: number, matched: number, accepted: number): void;
    endPublish(endAt: number): void;
  } {
    const traceId = sampled.traceId;
    const publishSpanId = newSpanId();
    this.traces.set(msg, {
      traceId,
      publishSpanId,
      topic,
      seq,
      enqueues: new Map(),
    });
    const emit = (span: TraceSpan): void => this.emit(span);
    const baseAttrs = { topic, seq };
    const admissionStart = at;
    let admissionEnd = -1;
    return {
      traceId,
      endAdmission(endAt: number): void {
        admissionEnd = endAt;
        const span: TraceSpan = {
          traceId,
          spanId: newSpanId(),
          parentId: publishSpanId,
          name: 'bus.admission',
          at: admissionStart,
          durationMs: Math.max(0, endAt - admissionStart),
          attrs: { ...baseAttrs },
        };
        emit(span);
      },
      endFanout(endAt: number, matched: number, accepted: number): void {
        const fanoutStart = admissionEnd >= 0 ? admissionEnd : admissionStart;
        const span: TraceSpan = {
          traceId,
          spanId: newSpanId(),
          parentId: publishSpanId,
          name: 'bus.fanout',
          at: fanoutStart,
          durationMs: Math.max(0, endAt - fanoutStart),
          attrs: { ...baseAttrs, matched, accepted },
        };
        emit(span);
      },
      endPublish(endAt: number): void {
        const span: TraceSpan = {
          traceId,
          spanId: publishSpanId,
          name: 'bus.publish',
          at: admissionStart,
          durationMs: Math.max(0, endAt - admissionStart),
          attrs: { ...baseAttrs },
        };
        if (sampled.parentSpanId !== undefined) span.parentId = sampled.parentSpanId;
        emit(span);
      },
    };
  }

  /** Whether this message is part of a sampled trace. */
  hasTrace(msg: object): boolean {
    return this.traces.get(msg) !== undefined;
  }

  /**
   * The trace id of a traced message, or `undefined` when the message is
   * not part of a sampled trace. Lets consumers that outlive the
   * delivery pipeline — the dead-letter queue, for example — keep the
   * end-to-end trace correlation for a message.
   */
  traceIdOf(msg: object): string | undefined {
    return this.traces.get(msg)?.traceId;
  }

  /**
   * Emits one `bus.enqueue` span for a subscriber that accepted the
   * message, and records it so the later `bus.deliver` / `bus.ack` spans
   * can parent to it. No-op when the message is not traced.
   */
  enqueueSpan(
    subscriber: TraceSubscriberRef,
    msg: object,
    at: number,
    durationMs: number,
  ): void {
    const state = this.traces.get(msg);
    if (state === undefined) return;
    const spanId = newSpanId();
    state.enqueues.set(subscriber.id, { spanId, at });
    this.emit({
      traceId: state.traceId,
      spanId,
      parentId: state.publishSpanId,
      name: 'bus.enqueue',
      at,
      durationMs: Math.max(0, durationMs),
      attrs: {
        topic: state.topic,
        seq: state.seq,
        subscriberId: subscriber.id,
        pattern: subscriber.pattern,
      },
    });
  }

  /**
   * Emits one `bus.deliver` span for a subscriber hand-off to its
   * handler. Parents to the subscriber's `bus.enqueue` span (falling
   * back to the publish root when the enqueue record is missing, e.g. a
   * redelivery whose enqueue span predates a buffer eviction — the
   * record, not the span, is what matters here). No-op when untraced.
   */
  deliverSpan(
    subscriber: TraceSubscriberRef,
    msg: object,
    at: number,
    durationMs: number,
  ): void {
    const state = this.traces.get(msg);
    if (state === undefined) return;
    const enqueue = state.enqueues.get(subscriber.id);
    this.emit({
      traceId: state.traceId,
      spanId: newSpanId(),
      parentId: enqueue?.spanId ?? state.publishSpanId,
      name: 'bus.deliver',
      at,
      durationMs: Math.max(0, durationMs),
      attrs: {
        topic: state.topic,
        seq: state.seq,
        subscriberId: subscriber.id,
        pattern: subscriber.pattern,
        // Queue dwell: hand-off time minus the enqueue stamp. 0 when the
        // enqueue record is missing (same fallback as the parent above).
        dwellMs: enqueue === undefined ? 0 : Math.max(0, at - enqueue.at),
      },
    });
  }

  /**
   * Opens the `bus.ack` span when a reliable delivery is handed to its
   * handler. Returns `true` when the message is traced (the bus then
   * wraps the delivery's `ack()`/`nack()` to close or abandon it).
   */
  openAckSpan(msg: object, redeliveries: number, at: number): boolean {
    const state = this.traces.get(msg);
    if (state === undefined) return false;
    state.openAck = { spanId: newSpanId(), at, redeliveries };
    return true;
  }

  /**
   * Completes and emits the open `bus.ack` span when `ack()` finishes.
   * Only the currently outstanding delivery may close it — a stale
   * handle's `ack()` (after nack/timeout already requeued) finds nothing
   * open and emits nothing, mirroring the ack-latency tracker's
   * single-sample rule.
   */
  closeAckSpan(subscriber: TraceSubscriberRef, msg: object, at: number): void {
    const state = this.traces.get(msg);
    const open = state?.openAck;
    if (state === undefined || open === undefined) return;
    state.openAck = undefined;
    const enqueue = state.enqueues.get(subscriber.id);
    this.emit({
      traceId: state.traceId,
      spanId: open.spanId,
      parentId: enqueue?.spanId ?? state.publishSpanId,
      name: 'bus.ack',
      at: open.at,
      durationMs: Math.max(0, at - open.at),
      attrs: {
        topic: state.topic,
        seq: state.seq,
        subscriberId: subscriber.id,
        pattern: subscriber.pattern,
        redeliveries: open.redeliveries,
      },
    });
  }

  /** Drops the open `bus.ack` span without emitting (nack / ack timeout). */
  abandonAckSpan(msg: object): void {
    const state = this.traces.get(msg);
    if (state !== undefined) state.openAck = undefined;
  }

  /** Emits a span: ring buffer first, then the `onTraceSpan` callback. */
  private emit(span: TraceSpan): void {
    if (this.ring.length < this.capacity) {
      this.ring.push(span);
    } else {
      this.ring[this.ringHead] = span;
      this.ringHead = (this.ringHead + 1) % this.capacity;
    }
    const hook = this.onTraceSpan;
    if (hook !== undefined) {
      try {
        hook(span);
      } catch {
        // A broken observer must never disturb the publish/deliver path.
      }
    }
  }

  /** Ring-buffer contents, oldest first. A snapshot — mutating it is safe. */
  snapshot(): TraceSpan[] {
    if (this.ring.length < this.capacity) return [...this.ring];
    return [...this.ring.slice(this.ringHead), ...this.ring.slice(0, this.ringHead)];
  }
}
