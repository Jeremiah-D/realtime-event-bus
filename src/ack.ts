/**
 * At-least-once delivery tracking for reliable subscribers.
 *
 * A reliable handler receives a {@link Delivery} instead of a bare message.
 * The delivery stays outstanding until the consumer settles it: `ack()`
 * confirms receipt, `nack()` asks for redelivery. A delivery that is still
 * outstanding after `ackTimeoutMs` is requeued automatically, so a crashed
 * or stalled consumer cannot lose messages silently.
 *
 * Redelivery is at-least-once, not exactly-once: a slow consumer may see the
 * same message twice (once from the original delivery, once from the
 * timeout requeue). `Delivery.redeliveries` reports how many times the
 * message was requeued before this delivery, so consumers can spot poison
 * messages and route them elsewhere instead of nacking forever.
 */

/** One at-least-once delivery handed to a reliable subscriber. */
export interface Delivery<T> {
  /** The delivered message. */
  readonly msg: T;
  /**
   * Per-subscriber monotonic delivery sequence, starting at 1. Useful for
   * logging; it is not a cross-subscriber ordering guarantee.
   */
  readonly seq: number;
  /** How many times this message was requeued before this delivery. */
  readonly redeliveries: number;
  /**
   * Bus-clock reading (`EventBusOptions.now`) of the moment this delivery
   * was handed to the handler. The bus injects its clock into the tracker,
   * so every redelivery re-stamps: this is the restartable accepted clock
   * the ack-latency tracker (`src/acklatency.ts`) samples against.
   */
  readonly acceptedAtMs: number;
  /**
   * Confirms receipt. Cancels the redelivery timer and the tracker forgets
   * the delivery. Calling it more than once — or after the delivery already
   * timed out — is a no-op.
   */
  ack(): void;
  /**
   * Requests redelivery. The message is requeued immediately and the
   * redelivery timer is cancelled. Calling it more than once — or after the
   * delivery already timed out — is a no-op.
   */
  nack(): void;
}

/**
 * Why a message is being requeued. The tracker reports the trigger so
 * callers can tell an explicit `nack()` from a silent ack timeout —
 * poison-message diagnosis (see the dead-letter queue) needs to know
 * which one kept failing.
 */
export type RedeliveryReason = 'nack' | 'ack-timeout';

export interface AckTrackerOptions<T> {
  /**
   * Milliseconds an unacked delivery may stay outstanding before it is
   * requeued automatically. Must be a positive finite number.
   */
  ackTimeoutMs: number;
  /**
   * Requeues a message for redelivery. Called with the message on `nack()`
   * (reason `'nack'`) and on ack timeout (reason `'ack-timeout'`); the
   * tracker has already forgotten the delivery when this runs, so
   * reentrancy is safe.
   */
  onRedeliver: (msg: T, reason: RedeliveryReason) => void;
  /**
   * Clock used to stamp `Delivery.acceptedAtMs`. Defaults to `Date.now`;
   * the bus passes its own injected clock (`EventBusOptions.now`) so the
   * stamp shares the bus's time base.
   */
  now?: () => number;
  /**
   * Called exactly once per tracked delivery when it settles — from
   * `ack()`, `nack()`, or the ack-timeout timer — after the tracker
   * forgets the delivery and before `onRedeliver` runs for requeueing
   * settles. The single convergence point of the three settle paths, so
   * per-subscriber in-flight accounting (see
   * `SubscribeOptions.maxInflight`) hooks here instead of in three
   * places. Not called by `clear()`.
   */
  onSettle?: () => void;
  /**
   * Called exactly once when `ack()` settles a live delivery — after the
   * tracker forgets the delivery, alongside `onSettle`. Not called for
   * `nack()`, ack timeouts, or no-op acks on already-settled or stale
   * handles, and not called by `clear()`. The single choke point for
   * "the consumer confirmed this delivery", so idle-reclamation activity
   * (`SubscribeOptions.maxIdleMs` with `idleActivity: 'ack' | 'any'`,
   * EB-72) hooks here instead of in the bus's ack wrapper — the wrapper
   * is bypassed when no ack-latency/trace/SLO tracking is enabled.
   */
  onAck?: () => void;
}

interface PendingDelivery<T> {
  msg: T;
  timer?: ReturnType<typeof setTimeout>;
  settled: boolean;
}

/**
 * Tracks the outstanding at-least-once deliveries of one subscriber. Owns
 * the ack-timeout timers; `clear()` cancels them all — call it on
 * unsubscribe so a gone subscriber never receives phantom redeliveries.
 */
export class AckTracker<T> {
  private readonly ackTimeoutMs: number;
  private readonly onRedeliver: (msg: T, reason: RedeliveryReason) => void;
  private readonly onSettle?: () => void;
  private readonly onAck?: () => void;
  private readonly now: () => number;
  private readonly pending = new Map<number, PendingDelivery<T>>();
  private nextSeq = 0;

  constructor(options: AckTrackerOptions<T>) {
    if (!Number.isFinite(options.ackTimeoutMs) || options.ackTimeoutMs <= 0) {
      throw new RangeError('ackTimeoutMs must be a positive finite number of milliseconds');
    }
    this.ackTimeoutMs = options.ackTimeoutMs;
    this.onRedeliver = options.onRedeliver;
    this.onSettle = options.onSettle;
    this.onAck = options.onAck;
    this.now = options.now ?? Date.now;
  }

  /** Deliveries currently outstanding, awaiting `ack()` or `nack()`. */
  get unackedCount(): number {
    return this.pending.size;
  }

  /**
   * The highest delivery sequence ever assigned (by `track`), including
   * deliveries that already settled. Cumulative acks (`ackUpTo`) beyond
   * this are out of range.
   */
  get highestSeq(): number {
    return this.nextSeq;
  }

  /**
   * The single settle path shared by `ack()`, `nack()`, the ack-timeout
   * timer, and `ackUpTo`: forgets the delivery, fires `onAck` (ack
   * settles only) and `onSettle` (every settle), then requeues when
   * asked. Idempotent per delivery — a second settle is a no-op.
   */
  private settleOne(seq: number, record: PendingDelivery<T>, requeue: boolean, reason?: RedeliveryReason): void {
    if (record.settled) return;
    record.settled = true;
    if (record.timer !== undefined) clearTimeout(record.timer);
    this.pending.delete(seq);
    // The uniform settle hook: ack(), nack(), and the ack-timeout path
    // all converge here, so one call covers every way a delivery can
    // settle — before any requeue work runs.
    //
    // `onAck` is narrower: only an `ack()` reaching a live delivery
    // settles without requeueing, so `!requeue` after the settled guard
    // is exactly "ack() settled" — no-ops on settled/stale handles
    // returned above, nacks and timeouts requeue. (EB-72 idle activity.)
    if (!requeue) this.onAck?.();
    this.onSettle?.();
    if (requeue) this.onRedeliver(record.msg, reason ?? 'nack');
  }

  /**
   * Registers a new outstanding delivery and returns its handle, starting
   * the ack-timeout timer. `redeliveries` is reported back on the handle so
   * callers can surface how many times the message was requeued before.
   */
  track(msg: T, redeliveries: number): Delivery<T> {
    const seq = ++this.nextSeq;
    const record: PendingDelivery<T> = { msg, settled: false };
    record.timer = setTimeout(() => this.settleOne(seq, record, true, 'ack-timeout'), this.ackTimeoutMs);
    // An unacked delivery must not keep the process alive on its own.
    const handle = record.timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    this.pending.set(seq, record);
    return {
      msg,
      seq,
      redeliveries,
      acceptedAtMs: this.now(),
      ack: () => this.settleOne(seq, record, false),
      nack: () => this.settleOne(seq, record, true),
    };
  }

  /**
   * Cumulatively acknowledges every outstanding delivery with sequence
   * `<= seq`, in ascending sequence order (EB-76): the high-throughput
   * alternative to acking each `Delivery` handle when the consumer
   * processes messages in order.
   *
   * Each acked delivery settles exactly like an individual `ack()` —
   * same timer cleanup, same `onAck`/`onSettle` hooks, no requeue.
   * Deliveries that are not outstanding (never delivered, already
   * settled, or dead-lettered after exhausting their redelivery budget)
   * are simply absent from the pending map and skipped silently.
   * Returns the acked deliveries in sequence order, so the caller can
   * run per-delivery side effects (trace spans, ack-latency sampling).
   */
  ackUpTo(seq: number): Array<{ seq: number; msg: T }> {
    const acked: Array<{ seq: number; msg: T }> = [];
    // The pending map preserves insertion order and sequences ascend
    // monotonically, so the first entry past `seq` ends the range.
    for (const [s, record] of this.pending) {
      if (s > seq) break;
      this.settleOne(s, record, false);
      acked.push({ seq: s, msg: record.msg });
    }
    return acked;
  }

  /**
   * Cancels every outstanding timer without requeueing. Returns how many
   * deliveries were outstanding.
   */
  clear(): number {
    const outstanding = this.pending.size;
    for (const record of this.pending.values()) {
      record.settled = true;
      if (record.timer !== undefined) clearTimeout(record.timer);
    }
    this.pending.clear();
    return outstanding;
  }

  /**
   * Subscription-migration support (EB-82): takes every outstanding
   * delivery out of the tracker and returns their messages in ascending
   * delivery-seq order (the pending map preserves insertion order, and
   * sequences ascend monotonically). Timers are cancelled and the
   * deliveries forgotten WITHOUT requeueing — the caller moves the
   * messages into the migration snapshot, so the source instance never
   * redelivers them.
   *
   * Each taken delivery counts as settled for `onSettle` (in-flight slot
   * accounting must observe the settle, exactly as for `ack()`), but it
   * was NOT acked: `onAck` (idle `'ack'` activity, ack-latency sampling)
   * does not fire, and `onRedeliver` does not run. Stale `Delivery`
   * handles kept by the source consumer settle to silent no-ops through
   * the usual settled guard.
   */
  takePending(): T[] {
    const msgs: T[] = [];
    for (const [seq, record] of this.pending) {
      record.settled = true;
      if (record.timer !== undefined) clearTimeout(record.timer);
      this.pending.delete(seq);
      this.onSettle?.();
      msgs.push(record.msg);
    }
    return msgs;
  }

  /**
   * Subscription-migration support (EB-82): seeds the delivery-sequence
   * counter with the source instance's committed position, so a freshly
   * imported subscription continues the source's `Delivery.seq`
   * numbering instead of restarting at 1. Only ever raises the counter —
   * a value at or below the current one is a no-op — and non-integer or
   * negative input throws `RangeError`.
   */
  seedHighestSeq(seq: number): void {
    if (!Number.isInteger(seq) || seq < 0) {
      throw new RangeError('seedHighestSeq: seq must be a non-negative integer');
    }
    if (seq > this.nextSeq) this.nextSeq = seq;
  }
}
