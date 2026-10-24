/**
 * Event-time ordered emit (EB-222).
 *
 * The watermark (`src/watermark.ts`) only *judges* lateness — it never
 * blocks delivery, so a subscriber that cares about business-time order
 * still sees arrivals out of order: a 09:00:05 event can arrive after a
 * 09:00:10 event and be delivered first. This module is the opt-in
 * buffer that fixes that, per topic, without touching the bus's
 * default path: with no `EventTimeOrderedBuffer` in use, nothing here
 * runs and delivery order is unchanged.
 *
 * Semantics, in the order they are applied on each `push`:
 *
 * - **No event time, no buffering.** A message without `eventTime`
 *   passes straight through (`ready: [message]`). It never moves the
 *   watermark and can never be held — the same rule the watermark
 *   itself follows, so untimed traffic pays no ordering cost.
 * - **Too late, still delivered.** A message whose event time is
 *   strictly below the current watermark (`maxEventTime -
 *   allowedLatenessMs`) is late beyond the grace: it is reported on
 *   `onLate` (the existing late-message callback shape — message,
 *   event time, watermark) and returned immediately with `late: true`.
 *   It is never buffered and never silently dropped; lateness is an
 *   observability signal, not a reason to lose a message.
 * - **Otherwise, buffer and release in event-time order.** An on-time
 *   message is held until the watermark passes it (`eventTime <=
 *   watermark` after folding the new peak in), then released with
 *   every other releasable message sorted by `eventTime`, ties broken
 *   by arrival order. A message is therefore never released before an
 *   earlier-event-time message that is still within its lateness
 *   grace could arrive — that is the whole point of waiting for the
 *   watermark rather than releasing on arrival.
 * - **Maximum hold, the escape hatch.** A watermark that never
 *   advances — a source that goes quiet after one early event — would
 *   otherwise hold messages forever. Any buffered message whose hold
 *   reaches `maxHoldMs` (measured from its arrival, on the caller's
 *   clock) is released on the next `push` or `drainDue`, in
 *   event-time order, watermark or not. Bounded lateness beats
 *   perfect order when the stream stalls.
 *
 * Orthogonality: the watermark decides what counts as late; this
 * buffer decides what order on-time messages are released in. It is
 * also not the subscriber-side per-key reorder buffer (`keyhotspot.ts`
 * / keyed delivery, EB-47): that one reorders by *publish sequence*
 * within one key for one subscriber, while this one reorders by
 * *business event time* across a whole topic before fan-out. The two
 * can stack — event-time order first, then per-key sequence — without
 * either knowing about the other.
 *
 * The buffer is a pure in-memory helper: it owns no clock (the caller
 * passes `nowMs`, so tests and bus clocks stay deterministic) and no
 * subscriber state. A throwing `onLate` propagates to the `push`
 * caller, matching the other subscriber monitoring callbacks.
 */

/** Options for one per-topic event-time ordered buffer. */
export interface EventTimeOrderOptions {
  /**
   * Grace in milliseconds, mirroring the watermark's
   * `allowedLatenessMs`: a message is late only when its event time
   * is strictly below `maxEventTime - allowedLatenessMs`. Must be a
   * non-negative finite number. Default 0.
   */
  allowedLatenessMs?: number;
  /**
   * Longest a message may sit buffered, in milliseconds, before it
   * is released regardless of the watermark. Must be a positive
   * finite number. Default 60000.
   */
  maxHoldMs?: number;
  /**
   * Called once per message that arrives late beyond the grace,
   * before the message is returned as ready. Absent means late
   * messages are still returned immediately, just unreported.
   */
  onLate?: (event: EventTimeLateEvent) => void;
}

/** A message that arrived late beyond the allowed lateness. */
export interface EventTimeLateEvent {
  /** The late message itself (returned to the caller as ready). */
  message: unknown;
  /** The message's business event time in epoch milliseconds. */
  eventTime: number;
  /** The watermark the message was judged against. */
  watermark: number;
}

/** Outcome of one `push` call. */
export interface EventTimePushResult<T> {
  /**
   * Messages released by this push, in event-time order (ties by
   * arrival order). Empty when the pushed message was buffered and
   * nothing became releasable.
   */
  ready: T[];
  /**
   * True when the pushed message itself was late beyond the grace
   * (and therefore returned immediately in `ready`). False for
   * buffered, passthrough, and released-by-watermark messages.
   */
  late: boolean;
}

interface Buffered<T> {
  message: T;
  eventTime: number;
  arrivedAtMs: number;
  seq: number;
}

/**
 * Per-topic event-time ordering buffer. One instance per topic;
 * `push` is called once per message in arrival order.
 */
export class EventTimeOrderedBuffer<T extends { eventTime?: number }> {
  private readonly allowedLatenessMs: number;
  private readonly maxHoldMs: number;
  private readonly onLate: ((event: EventTimeLateEvent) => void) | undefined;
  private maxEventTime: number | undefined = undefined;
  private buffered: Buffered<T>[] = [];
  private nextSeq = 0;

  constructor(options: EventTimeOrderOptions = {}) {
    const allowedLatenessMs = options.allowedLatenessMs ?? 0;
    if (typeof allowedLatenessMs !== 'number' || !Number.isFinite(allowedLatenessMs) || allowedLatenessMs < 0) {
      throw new RangeError(
        `EventTimeOrderedBuffer: allowedLatenessMs must be a non-negative finite number of milliseconds`,
      );
    }
    const maxHoldMs = options.maxHoldMs ?? 60000;
    if (typeof maxHoldMs !== 'number' || !Number.isFinite(maxHoldMs) || maxHoldMs <= 0) {
      throw new RangeError(
        `EventTimeOrderedBuffer: maxHoldMs must be a positive finite number of milliseconds`,
      );
    }
    if (options.onLate !== undefined && typeof options.onLate !== 'function') {
      throw new TypeError(`EventTimeOrderedBuffer: onLate must be a function`);
    }
    this.allowedLatenessMs = allowedLatenessMs;
    this.maxHoldMs = maxHoldMs;
    this.onLate = options.onLate;
  }

  /** Current watermark (`maxEventTime - allowedLatenessMs`), `undefined` until the first event-time message. */
  get watermark(): number | undefined {
    return this.maxEventTime === undefined ? undefined : this.maxEventTime - this.allowedLatenessMs;
  }

  /** Number of messages currently held in the buffer. */
  get pendingCount(): number {
    return this.buffered.length;
  }

  /**
   * Offers one message to the buffer. `nowMs` is the caller's clock
   * (arrival time for hold measurement). See the module header for
   * the push semantics: no event time passes through, a late message
   * is reported and returned immediately, otherwise the message is
   * buffered until the watermark passes it or its hold expires.
   */
  push(message: T, nowMs: number): EventTimePushResult<T> {
    const eventTime = message.eventTime;
    // No event time: straight through, watermark untouched.
    if (eventTime === undefined) {
      return { ready: [message], late: false };
    }
    if (typeof eventTime !== 'number' || !Number.isFinite(eventTime) || eventTime < 0) {
      throw new RangeError(`EventTimeOrderedBuffer: eventTime must be a finite number of milliseconds >= 0`);
    }
    if (typeof nowMs !== 'number' || !Number.isFinite(nowMs) || nowMs < 0) {
      throw new RangeError(`EventTimeOrderedBuffer: nowMs must be a finite number of milliseconds >= 0`);
    }
    // Late beyond grace: report and deliver immediately, never buffered.
    const watermarkBefore = this.watermark;
    if (watermarkBefore !== undefined && eventTime < watermarkBefore) {
      if (this.onLate !== undefined) {
        this.onLate({ message, eventTime, watermark: watermarkBefore });
      }
      return { ready: [message], late: true };
    }
    if (this.maxEventTime === undefined || eventTime > this.maxEventTime) {
      this.maxEventTime = eventTime;
    }
    this.buffered.push({ message, eventTime, arrivedAtMs: nowMs, seq: this.nextSeq++ });
    return { ready: this.drainReleasable(nowMs), late: false };
  }

  /**
   * Releases buffered messages whose hold has expired as of `nowMs`
   * (plus anything the watermark has since made releasable), in
   * event-time order. Call this on a timer when arrivals stop, so a
   * quiet stream still drains through the max-hold escape hatch.
   */
  drainDue(nowMs: number): T[] {
    if (typeof nowMs !== 'number' || !Number.isFinite(nowMs) || nowMs < 0) {
      throw new RangeError(`EventTimeOrderedBuffer: nowMs must be a finite number of milliseconds >= 0`);
    }
    return this.drainReleasable(nowMs);
  }

  /**
   * Releases every buffered message in event-time order, regardless
   * of watermark or hold. For shutdown / drain only — normal flow
   * never needs it, since the watermark and max-hold paths already
   * release everything eventually.
   */
  flushAll(): T[] {
    const sorted = [...this.buffered].sort(compareBuffered);
    this.buffered = [];
    return sorted.map((entry) => entry.message);
  }

  private drainReleasable(nowMs: number): T[] {
    const watermark = this.watermark;
    const ready: Buffered<T>[] = [];
    const kept: Buffered<T>[] = [];
    for (const entry of this.buffered) {
      const byWatermark = watermark !== undefined && entry.eventTime <= watermark;
      const byHold = nowMs - entry.arrivedAtMs >= this.maxHoldMs;
      if (byWatermark || byHold) ready.push(entry);
      else kept.push(entry);
    }
    this.buffered = kept;
    ready.sort(compareBuffered);
    return ready.map((entry) => entry.message);
  }
}

function compareBuffered<T>(a: Buffered<T>, b: Buffered<T>): number {
  if (a.eventTime !== b.eventTime) return a.eventTime - b.eventTime;
  return a.seq - b.seq;
}
