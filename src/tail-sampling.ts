/**
 * Tail-based delivery-pipeline trace sampling (EB-181).
 *
 * The complement to head-based sampling (EB-45): instead of deciding at
 * admission time whether to trace a publish, the tail sampler buffers
 * every admitted publish's spans in memory until the publish's delivery
 * reaches a terminal state — then decides. A trace that saw a handler
 * error, an ack timeout, or a dead-letter is always kept; a trace whose
 * deliveries all succeeded is kept with probability `successSampleRate`.
 * Kept traces flow to the `onTraceSpan` callback channel (shared with
 * head-based tracing) and to a bounded ring buffer exported as
 * `getStats().tailSampledSpans`.
 *
 * Terminal states, and the bus hooks that report them:
 * - plain delivery hand-off completes → `'delivered'` (handler returned)
 *   or `'error'` (handler threw, or the health probe's processing budget
 *   was exceeded);
 * - reliable delivery settles → `'delivered'` (`ack()`), `'nack'`
 *   (explicit nack — the redelivery opens a new leg), `'ack-timeout'`
 *   (the ack timer fired — always kept);
 * - the subscriber dead-letter queue takes the message → `'dead-letter'`
 *   (always kept; the leg already closed at the redelivery that
 *   exhausted the budget);
 * - the message expires in a subscriber queue → `'expired'`;
 * - the delivery is invalidated by a migration snapshot or a seek replay
 *   → `'invalidated'` (the replay/re-delivery opens a new leg).
 *
 * A trace closes when its fan-out is done and every open delivery leg
 * has reached a terminal state. Two bounds keep memory honest:
 * `maxSpansPerTrace` caps the spans buffered per trace (past it, new
 * spans are dropped and the trace is marked truncated), and
 * `maxOpenTraces` caps the open traces — when it is exceeded the oldest
 * open trace is force-decided immediately (the decision horizon, the
 * same pattern OTel's tail-sampling processor uses for its
 * decision-wait timeout).
 *
 * Disabled by default, and the disabled path is allocation-free: every
 * instrumentation site first reads the bus's single `tailSampler` field
 * and returns past one branch — no WeakMap lookup, no clock read, no
 * object creation.
 */

import { newSpanId, newTraceId, parseTraceparent } from './trace.ts';
import type { TraceSpan, TraceSubscriberRef } from './trace.ts';

/** Tuning for opt-in tail-based trace sampling (`EventBusOptions.tailSampling`). */
export interface TailSamplingOptions {
  /**
   * Master switch. An options object implies `true`; pass `false`
   * explicitly (or omit `tailSampling`) to disable.
   */
  enabled?: boolean;
  /**
   * Fraction of *successful* traces to keep (`Math.random() <
   * successSampleRate`). Traces that saw a handler error, an ack
   * timeout, or a dead-letter are always kept, regardless of this rate.
   * Must be a finite number in [0, 1]. Default 1 (keep every successful
   * trace — lower it in production, e.g. 0.01).
   */
  successSampleRate?: number;
  /**
   * Called synchronously with every kept span, in completion order.
   * Error-isolated: a throwing callback is swallowed so a broken
   * observer can never disturb the publish/deliver path. When absent,
   * the bus falls back to the head-based `trace.onTraceSpan` (the shared
   * callback channel).
   */
  onTraceSpan?: (span: TraceSpan) => void;
  /**
   * Cap on simultaneously open traces. When it is exceeded the oldest
   * open trace is force-decided immediately. Must be a positive
   * integer. Default 4096.
   */
  maxOpenTraces?: number;
  /**
   * Cap on spans buffered per trace. Past it, new spans are dropped and
   * the kept trace is marked `tailTruncated`. Must be a positive
   * integer. Default 512.
   */
  maxSpansPerTrace?: number;
  /**
   * Ring-buffer capacity for `getStats().tailSampledSpans`: the oldest
   * kept spans are evicted past this bound. Must be a positive integer.
   * Default 1024.
   */
  bufferSize?: number;
}

/** Normalized tail-sampling options; `undefined` (from the resolver) means disabled. */
export interface ResolvedTailSamplingOptions {
  successSampleRate: number;
  onTraceSpan?: (span: TraceSpan) => void;
  maxOpenTraces: number;
  maxSpansPerTrace: number;
  bufferSize: number;
}

/**
 * Normalizes `EventBusOptions.tailSampling`. Returns `undefined` when
 * disabled (absent, `null`, `false`, or `{ enabled: false }`); anything
 * else invalid throws `RangeError` at construction time.
 */
export function resolveTailSamplingOptions(
  raw: boolean | TailSamplingOptions | null | undefined,
  where = 'EventBus',
): ResolvedTailSamplingOptions | undefined {
  if (raw == null || raw === false) return undefined;
  if (raw === true) {
    return {
      successSampleRate: 1,
      onTraceSpan: undefined,
      maxOpenTraces: 4096,
      maxSpansPerTrace: 512,
      bufferSize: 1024,
    };
  }
  if (typeof raw !== 'object') {
    throw new RangeError(`${where}: tailSampling must be a boolean or an options object`);
  }
  if (raw.enabled === false) return undefined;
  const successSampleRate = raw.successSampleRate ?? 1;
  if (!Number.isFinite(successSampleRate) || successSampleRate < 0 || successSampleRate > 1) {
    throw new RangeError(`${where}: tailSampling.successSampleRate must be a finite number in [0, 1]`);
  }
  const onTraceSpan = raw.onTraceSpan;
  if (onTraceSpan !== undefined && typeof onTraceSpan !== 'function') {
    throw new RangeError(`${where}: tailSampling.onTraceSpan must be a function`);
  }
  const maxOpenTraces = raw.maxOpenTraces ?? 4096;
  if (!Number.isInteger(maxOpenTraces) || maxOpenTraces < 1) {
    throw new RangeError(`${where}: tailSampling.maxOpenTraces must be a positive integer`);
  }
  const maxSpansPerTrace = raw.maxSpansPerTrace ?? 512;
  if (!Number.isInteger(maxSpansPerTrace) || maxSpansPerTrace < 1) {
    throw new RangeError(`${where}: tailSampling.maxSpansPerTrace must be a positive integer`);
  }
  const bufferSize = raw.bufferSize ?? 1024;
  if (!Number.isInteger(bufferSize) || bufferSize < 1) {
    throw new RangeError(`${where}: tailSampling.bufferSize must be a positive integer`);
  }
  return { successSampleRate, onTraceSpan, maxOpenTraces, maxSpansPerTrace, bufferSize };
}

/** Terminal outcome of one delivery leg, for the tail decision. */
export type TailLegOutcome =
  | 'delivered'
  | 'error'
  | 'nack'
  | 'ack-timeout'
  | 'dead-letter'
  | 'expired'
  | 'invalidated';

/** Why a trace was kept. */
export type TailKeepReason = 'error' | 'ack-timeout' | 'dead-letter' | 'sampled';

/** Per-message tail trace record, keyed by the `BusMessage` in a `WeakMap`. */
interface TailTraceRecord {
  traceId: string;
  publishSpanId: string;
  /** The buffered `bus.publish` root span — annotated at decision time. */
  publishSpan: TraceSpan;
  spans: TraceSpan[];
  /** Delivery legs not yet at a terminal state. */
  openLegs: number;
  /** False once `endPublish` ran. */
  fanoutOpen: boolean;
  /** Set by handler errors, ack timeouts, and dead-letters. */
  interesting: boolean;
  /** Which interesting event fired first. */
  interestingReason?: 'error' | 'ack-timeout' | 'dead-letter';
  truncated: boolean;
  decided: boolean;
  topic: string;
  seq: number;
  /** Open `bus.ack` span for the currently outstanding reliable delivery. */
  openAck?: { spanId: string; at: number; redeliveries: number };
}

/**
 * Buffers per-trace spans until the delivery terminal state, then
 * decides. One instance per `EventBus`, created only when tail sampling
 * is enabled — the bus keeps the field `undefined` otherwise, which is
 * what makes the disabled path allocation-free.
 */
export class TailSampler {
  private readonly successSampleRate: number;
  private readonly onTraceSpan: ((span: TraceSpan) => void) | undefined;
  private readonly maxOpenTraces: number;
  private readonly maxSpansPerTrace: number;
  private readonly traces = new WeakMap<object, TailTraceRecord>();
  /** Insertion-ordered open traces, for the `maxOpenTraces` horizon. */
  private readonly openOrder = new Map<object, TailTraceRecord>();
  private readonly ring: TraceSpan[] = [];
  private ringHead = 0;
  private readonly capacity: number;
  private readonly now: () => number;

  constructor(resolved: ResolvedTailSamplingOptions, now: () => number = Date.now) {
    this.successSampleRate = resolved.successSampleRate;
    this.onTraceSpan = resolved.onTraceSpan;
    this.maxOpenTraces = resolved.maxOpenTraces;
    this.maxSpansPerTrace = resolved.maxSpansPerTrace;
    this.capacity = resolved.bufferSize;
    this.now = now;
  }

  /** Whether this message has an open (undecided) tail trace. */
  hasTrace(msg: object): boolean {
    const record = this.traces.get(msg);
    return record !== undefined && !record.decided;
  }

  /**
   * Opens the tail trace for an admitted publish and returns a handle
   * whose `end*` methods buffer the publish/admission/fanout spans —
   * mirroring `TraceRecorder.beginPublish`, but nothing is emitted yet.
   * Returns `undefined` when the open-trace budget is exhausted (the
   * oldest open trace is force-decided to make room, and this publish
   * goes untraced).
   */
  openTrace(
    msg: object,
    topic: string,
    seq: number,
    traceparent: string | undefined,
    at: number,
  ):
    | {
        readonly traceId: string;
        endAdmission(endAt: number): void;
        endFanout(endAt: number, matched: number, accepted: number): void;
        endPublish(endAt: number): void;
      }
    | undefined {
    this.enforceOpenBudget();
    const incoming = traceparent === undefined ? undefined : parseTraceparent(traceparent);
    const traceId = incoming?.traceId ?? newTraceId();
    const publishSpanId = newSpanId();
    const publishSpan: TraceSpan = {
      traceId,
      spanId: publishSpanId,
      name: 'bus.publish',
      at,
      durationMs: 0,
      attrs: { topic, seq },
    };
    if (incoming?.parentSpanId !== undefined) publishSpan.parentId = incoming.parentSpanId;
    const record: TailTraceRecord = {
      traceId,
      publishSpanId,
      publishSpan,
      spans: [],
      openLegs: 0,
      fanoutOpen: true,
      interesting: false,
      truncated: false,
      decided: false,
      topic,
      seq,
    };
    this.traces.set(msg, record);
    this.openOrder.set(msg, record);
    const buffer = (span: TraceSpan): void => this.bufferSpan(record, span);
    const baseAttrs = { topic, seq };
    const admissionStart = at;
    let admissionEnd = -1;
    const sampler = this;
    return {
      traceId,
      endAdmission(endAt: number): void {
        admissionEnd = endAt;
        buffer({
          traceId,
          spanId: newSpanId(),
          parentId: publishSpanId,
          name: 'bus.admission',
          at: admissionStart,
          durationMs: Math.max(0, endAt - admissionStart),
          attrs: { ...baseAttrs },
        });
      },
      endFanout(endAt: number, matched: number, accepted: number): void {
        const fanoutStart = admissionEnd >= 0 ? admissionEnd : admissionStart;
        buffer({
          traceId,
          spanId: newSpanId(),
          parentId: publishSpanId,
          name: 'bus.fanout',
          at: fanoutStart,
          durationMs: Math.max(0, endAt - fanoutStart),
          attrs: { ...baseAttrs, matched, accepted },
        });
      },
      endPublish(endAt: number): void {
        publishSpan.durationMs = Math.max(0, endAt - admissionStart);
        // The publish root is buffered last so the kept trace reads in
        // completion order — fanout, then the publish root — like the
        // head-based ring buffer.
        buffer(publishSpan);
        record.fanoutOpen = false;
        sampler.maybeDecide(msg, record);
      },
    };
  }

  /**
   * Buffers one `bus.enqueue` span for a subscriber that accepted the
   * message and opens a delivery leg. When the message has no open
   * trace — a DLQ replay after the original trace was decided, for
   * example — a fresh trace is opened so the new delivery lifecycle is
   * still observable.
   */
  enqueueSpan(subscriber: TraceSubscriberRef, msg: object, at: number, durationMs: number): void {
    let record = this.traces.get(msg);
    if (record === undefined || record.decided) {
      const fresh = this.openTrace(
        msg,
        (msg as { topic: string }).topic,
        (msg as { seq: number }).seq,
        undefined,
        at,
      );
      if (fresh === undefined) return;
      // A replayed delivery starts its own admission/fanout narrative at
      // the enqueue point.
      fresh.endAdmission(at);
      fresh.endFanout(at, 0, 0);
      fresh.endPublish(at);
      record = this.traces.get(msg);
      if (record === undefined || record.decided) return;
    }
    record.openLegs += 1;
    this.bufferSpan(record, {
      traceId: record.traceId,
      spanId: newSpanId(),
      parentId: record.publishSpanId,
      name: 'bus.enqueue',
      at,
      durationMs: Math.max(0, durationMs),
      attrs: {
        topic: record.topic,
        seq: record.seq,
        subscriberId: subscriber.id,
        pattern: subscriber.pattern,
      },
    });
  }

  /**
   * Buffers one `bus.deliver` span for a plain (non-reliable) delivery
   * and closes its leg. `outcome` is `'error'` when the handler threw
   * (or the health probe's processing budget was exceeded) — an
   * interesting trace, always kept.
   */
  deliverSpan(
    subscriber: TraceSubscriberRef,
    msg: object,
    at: number,
    durationMs: number,
    outcome: 'delivered' | 'error',
  ): void {
    const record = this.traces.get(msg);
    if (record === undefined || record.decided) return;
    this.bufferSpan(record, {
      traceId: record.traceId,
      spanId: newSpanId(),
      parentId: record.publishSpanId,
      name: 'bus.deliver',
      at,
      durationMs: Math.max(0, durationMs),
      attrs: {
        topic: record.topic,
        seq: record.seq,
        subscriberId: subscriber.id,
        pattern: subscriber.pattern,
        outcome,
      },
    });
    this.closeLeg(msg, record, outcome);
  }

  /**
   * Records the open `bus.ack` span for a reliable delivery. The span is
   * buffered when the delivery settles (`closeAckSpan`); the leg stays
   * open until then. No-op when the message has no open trace.
   */
  openAckSpan(msg: object, redeliveries: number, at: number): void {
    const record = this.traces.get(msg);
    if (record === undefined || record.decided) return;
    record.openAck = { spanId: newSpanId(), at, redeliveries };
  }

  /**
   * Buffers the `bus.ack` span for an acked reliable delivery and closes
   * its leg as delivered. Only the currently outstanding delivery may
   * close it — a stale handle's `ack()` finds nothing open and does
   * nothing, mirroring the head-based single-settle rule.
   */
  closeAckSpan(subscriber: TraceSubscriberRef, msg: object, at: number): void {
    const record = this.traces.get(msg);
    const open = record?.openAck;
    if (record === undefined || record.decided || open === undefined) return;
    record.openAck = undefined;
    this.bufferSpan(record, {
      traceId: record.traceId,
      spanId: open.spanId,
      parentId: record.publishSpanId,
      name: 'bus.ack',
      at: open.at,
      durationMs: Math.max(0, at - open.at),
      attrs: {
        topic: record.topic,
        seq: record.seq,
        subscriberId: subscriber.id,
        pattern: subscriber.pattern,
        redeliveries: open.redeliveries,
        outcome: 'delivered',
      },
    });
    this.closeLeg(msg, record, 'delivered');
  }

  /**
   * Drops the open `bus.ack` span and closes the leg with the given
   * outcome. `'ack-timeout'` marks the trace interesting (always kept);
   * `'nack'` and `'invalidated'` just close the leg — the redelivery or
   * replay opens a new one.
   */
  abandonAckSpan(msg: object, outcome: 'nack' | 'ack-timeout' | 'dead-letter' | 'invalidated'): void {
    const record = this.traces.get(msg);
    if (record === undefined || record.decided) return;
    record.openAck = undefined;
    this.closeLeg(msg, record, outcome);
  }

  /**
   * Marks the trace interesting: the message entered the subscriber
   * dead-letter queue. The leg already closed at the redelivery that
   * exhausted the budget — this only flips the keep decision. Always
   * kept.
   */
  noteDeadLetter(msg: object): void {
    const record = this.traces.get(msg);
    if (record === undefined || record.decided) return;
    this.markInteresting(record, 'dead-letter');
    this.maybeDecide(msg, record);
  }

  /** Closes one delivery leg: the message expired in a subscriber queue. */
  noteExpired(msg: object): void {
    const record = this.traces.get(msg);
    if (record === undefined || record.decided) return;
    this.closeLeg(msg, record, 'expired');
  }

  /** Ring-buffer contents (kept traces' spans), oldest first. A snapshot. */
  snapshot(): TraceSpan[] {
    if (this.ring.length < this.capacity) return [...this.ring];
    return [...this.ring.slice(this.ringHead), ...this.ring.slice(0, this.ringHead)];
  }

  /** Buffers a span, dropping past `maxSpansPerTrace` (marks truncated). */
  private bufferSpan(record: TailTraceRecord, span: TraceSpan): void {
    if (record.spans.length >= this.maxSpansPerTrace) {
      record.truncated = true;
      return;
    }
    record.spans.push(span);
  }

  /** Closes one delivery leg and decides when the trace is complete. */
  private closeLeg(msg: object, record: TailTraceRecord, outcome: TailLegOutcome): void {
    if (outcome === 'error') this.markInteresting(record, 'error');
    else if (outcome === 'ack-timeout') this.markInteresting(record, 'ack-timeout');
    else if (outcome === 'dead-letter') this.markInteresting(record, 'dead-letter');
    record.openLegs = Math.max(0, record.openLegs - 1);
    this.maybeDecide(msg, record);
  }

  private markInteresting(record: TailTraceRecord, reason: 'error' | 'ack-timeout' | 'dead-letter'): void {
    record.interesting = true;
    if (record.interestingReason === undefined) record.interestingReason = reason;
  }

  /**
   * Decides a trace whose fan-out is done and whose legs are all
   * terminal: interesting traces are always kept, successful ones with
   * probability `successSampleRate`. Kept spans go to the ring buffer
   * and the `onTraceSpan` callback, in completion order.
   */
  private maybeDecide(msg: object, record: TailTraceRecord): void {
    if (record.decided || record.fanoutOpen || record.openLegs > 0) return;
    record.decided = true;
    this.openOrder.delete(msg);
    const keepReason: TailKeepReason | undefined = record.interesting
      ? (record.interestingReason ?? 'error')
      : Math.random() < this.successSampleRate
        ? 'sampled'
        : undefined;
    if (keepReason === undefined) return;
    record.publishSpan.attrs.tailKeepReason = keepReason;
    if (record.truncated) record.publishSpan.attrs.tailTruncated = true;
    for (const span of record.spans) this.emit(span);
  }

  /**
   * The open-trace budget: force-decide the oldest open trace when the
   * budget is exceeded, so a pathological backlog of never-settling
   * deliveries cannot grow memory without bound. The forced decision
   * uses the normal rule — an interesting trace is still always kept.
   */
  private enforceOpenBudget(): void {
    while (this.openOrder.size >= this.maxOpenTraces) {
      const oldest = this.openOrder.keys().next();
      if (oldest.done) return;
      const msg = oldest.value;
      const record = this.openOrder.get(msg);
      if (record === undefined) {
        this.openOrder.delete(msg);
        continue;
      }
      // Force the horizon: pretend the fan-out finished and no legs are
      // outstanding — whatever the trace knows now is what gets decided.
      record.fanoutOpen = false;
      record.openLegs = 0;
      this.maybeDecide(msg, record);
      // maybeDecide always decides here (fanoutOpen=false, openLegs=0);
      // the guard below is belt-and-braces against future changes.
      if (!record.decided) {
        record.decided = true;
        this.openOrder.delete(msg);
      }
    }
  }

  /** Emits a kept span: ring buffer first, then the `onTraceSpan` callback. */
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
}
