/**
 * Event-time watermark tracking (EB-59).
 *
 * Stream-processing semantics, Kafka/Flink-style: producers stamp each
 * message with its business event time (`PublishOptions.eventTime`, epoch
 * milliseconds), and the bus tracks a per-topic watermark — the highest
 * event time observed so far, minus the configured `allowedLatenessMs`
 * grace. A message whose event time is older than the watermark is *late*:
 * it is still delivered (lateness never blocks delivery), but it is
 * counted and reported, so operators can tell a data-quality problem — a
 * stale producer, clock skew, a replayed feed — apart from a transport
 * problem.
 *
 * The watermark is deliberately orthogonal to the publish-order sequence
 * (`BusMessage.seq`, EB-13): `seq` orders *arrivals*; the watermark orders
 * *business time*. Two publishes arriving out of publish order whose event
 * times still fit inside the watermark are on time — only a message whose
 * business time trails the stream's observed peak by more than the allowed
 * lateness is late.
 *
 * Observation semantics (see `EventTimeWatermark.observe`):
 * - Only publishes carrying an `eventTime` participate. Messages without
 *   one never move the watermark and can never count as late — the
 *   watermark stays unknown (`undefined`) until the first event-time
 *   publish on the topic.
 * - The watermark is monotone: it only advances when a new maximum event
 *   time arrives. A lower event time that still sits inside the lateness
 *   grace leaves the watermark untouched.
 * - Lateness is judged against the watermark *before* the message is
 *   folded in: `eventTime < watermarkBefore` is late, equality is on time.
 * - Observation happens once per admitted publish in `fanOut` — after the
 *   admission gates (schema validation, rate-limit budget). A rejected or
 *   shed publish never moves the watermark, exactly like the publish-rate
 *   table.
 *
 * The class is a pure in-memory tracker: the bus owns the
 * `allowedLatenessMs` resolution (bus-level default from
 * `EventBusOptions.allowedLatenessMs`, per-topic overrides from
 * `EventBus.setTopicAllowedLateness`) and the late-message callback
 * (`EventBusOptions.onLate`, error-isolated). All it needs per
 * observation is the event time and the resolved lateness.
 */

/**
 * Fired when a publish carries an event time older than its topic's
 * event-time watermark (see `EventBusOptions.onLate`). Delivery is
 * unaffected — the message is still fanned out normally; this is the
 * observability signal that the event arrived too late for the stream's
 * business-time frontier.
 */
export interface LateMessageEvent {
  /** The concrete topic the message was published to. */
  topic: string;
  /** The publish-order sequence number stamped on the message (EB-13). */
  seq: number;
  /** The message's business event time in epoch milliseconds. */
  eventTime: number;
  /**
   * The topic's event-time watermark the message was judged against
   * (`max(eventTime) - allowedLatenessMs` before this message was folded
   * in). A late message never advances it, so this is also the watermark
   * after the observation.
   */
  watermark: number;
  /** The allowed lateness in effect for the topic when judged. */
  allowedLatenessMs: number;
}

/** Late-message hook (see `EventBusOptions.onLate`). */
export type LateMessageCallback = (event: LateMessageEvent) => void;

/**
 * Validates an allowed-lateness value the way a constructor or setter
 * would: absent is fine, otherwise it must be a non-negative finite
 * number of milliseconds. Throws `RangeError` before anything is
 * mutated, matching the other option validations.
 */
export function validateAllowedLatenessMs(value: unknown, context: string): void {
  if (value === undefined) return;
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new RangeError(
      `${context}: allowedLatenessMs must be a non-negative finite number of milliseconds`,
    );
  }
}

/**
 * Validates a publish event time (see `PublishOptions.eventTime`): absent
 * is fine, otherwise it must be a finite number of milliseconds `>= 0`.
 * Throws `RangeError` before anything is mutated, matching the other
 * publish-option validations.
 */
export function validateEventTime(value: unknown, context: string): void {
  if (value === undefined) return;
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
    throw new RangeError(
      `${context}: eventTime must be a finite number of milliseconds >= 0`,
    );
  }
}

/** Outcome of one `EventTimeWatermark.observe` call. */
export interface WatermarkObservation {
  /**
   * True when the observed event time was older than the watermark before
   * this observation — a late message. Late messages still advance
   * nothing: they are counted by the bus, never folded into the peak.
   */
  late: boolean;
  /**
   * The watermark after the observation (`maxEventTime -
   * allowedLatenessMs`). For a late message this equals the watermark the
   * message was judged against.
   */
  watermark: number;
}

/**
 * Per-topic event-time watermark state. One instance per topic, owned by
 * the bus; `observe` is called once per admitted publish that carries an
 * `eventTime`.
 */
export class EventTimeWatermark {
  /** Highest event time observed on the topic so far; `undefined` until the first event-time publish. */
  private maxEventTime: number | undefined = undefined;
  /** Late messages observed on the topic (monotonic). */
  private lateMessages = 0;

  /**
   * Folds one event time into the tracker. Lateness is judged against the
   * watermark before the fold: `eventTime < maxEventTime -
   * allowedLatenessMs` is late (equality is on time). A late message is
   * counted but does not move the peak; a new peak advances the watermark
   * monotonically.
   */
  observe(eventTime: number, allowedLatenessMs: number): WatermarkObservation {
    if (this.maxEventTime === undefined) {
      this.maxEventTime = eventTime;
      return { late: false, watermark: eventTime - allowedLatenessMs };
    }
    const watermarkBefore = this.maxEventTime - allowedLatenessMs;
    const late = eventTime < watermarkBefore;
    if (late) {
      this.lateMessages += 1;
      return { late, watermark: watermarkBefore };
    }
    if (eventTime > this.maxEventTime) this.maxEventTime = eventTime;
    return { late: false, watermark: this.maxEventTime - allowedLatenessMs };
  }

  /**
   * The current watermark (`maxEventTime - allowedLatenessMs`),
   * `undefined` until the first event-time publish on the topic. Needs
   * the topic's current allowed lateness to compute — the bus resolves it
   * per read, so a reconfigured lateness is reflected immediately.
   */
  watermarkFor(allowedLatenessMs: number): number | undefined {
    return this.maxEventTime === undefined ? undefined : this.maxEventTime - allowedLatenessMs;
  }

  /** Late messages observed so far (monotonic). */
  get lateCount(): number {
    return this.lateMessages;
  }
}
