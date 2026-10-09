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

export interface AckTrackerOptions<T> {
  /**
   * Milliseconds an unacked delivery may stay outstanding before it is
   * requeued automatically. Must be a positive finite number.
   */
  ackTimeoutMs: number;
  /**
   * Requeues a message for redelivery. Called with the message on `nack()`
   * and on ack timeout; the tracker has already forgotten the delivery when
   * this runs, so reentrancy is safe.
   */
  onRedeliver: (msg: T) => void;
  /**
   * Clock used to stamp `Delivery.acceptedAtMs`. Defaults to `Date.now`;
   * the bus passes its own injected clock (`EventBusOptions.now`) so the
   * stamp shares the bus's time base.
   */
  now?: () => number;
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
  private readonly onRedeliver: (msg: T) => void;
  private readonly now: () => number;
  private readonly pending = new Map<number, PendingDelivery<T>>();
  private nextSeq = 0;

  constructor(options: AckTrackerOptions<T>) {
    if (!Number.isFinite(options.ackTimeoutMs) || options.ackTimeoutMs <= 0) {
      throw new RangeError('ackTimeoutMs must be a positive finite number of milliseconds');
    }
    this.ackTimeoutMs = options.ackTimeoutMs;
    this.onRedeliver = options.onRedeliver;
    this.now = options.now ?? Date.now;
  }

  /** Deliveries currently outstanding, awaiting `ack()` or `nack()`. */
  get unackedCount(): number {
    return this.pending.size;
  }

  /**
   * Registers a new outstanding delivery and returns its handle, starting
   * the ack-timeout timer. `redeliveries` is reported back on the handle so
   * callers can surface how many times the message was requeued before.
   */
  track(msg: T, redeliveries: number): Delivery<T> {
    const seq = ++this.nextSeq;
    const record: PendingDelivery<T> = { msg, settled: false };
    const settle = (requeue: boolean): void => {
      if (record.settled) return;
      record.settled = true;
      if (record.timer !== undefined) clearTimeout(record.timer);
      this.pending.delete(seq);
      if (requeue) this.onRedeliver(msg);
    };
    record.timer = setTimeout(() => settle(true), this.ackTimeoutMs);
    // An unacked delivery must not keep the process alive on its own.
    const handle = record.timer as unknown as { unref?: () => unknown };
    if (typeof handle.unref === 'function') handle.unref();
    this.pending.set(seq, record);
    return {
      msg,
      seq,
      redeliveries,
      acceptedAtMs: this.now(),
      ack: () => settle(false),
      nack: () => settle(true),
    };
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
}
