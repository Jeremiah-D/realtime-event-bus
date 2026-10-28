/**
 * Subscriber delivery windows (EB-230).
 *
 * A delivery window is a recurring, clock-time traffic shape for one
 * subscriber: messages published outside the subscriber's windows wait
 * in a pre-queue hold — never dropped, never queued — and flush in
 * arrival order when a window opens. The default is all-day delivery:
 * a subscriber without `deliveryWindows` behaves exactly as before.
 *
 * A window is one `HH:MM-HH:MM` interval in a named IANA time zone,
 * on a set of weekdays:
 *
 * - `start` is inclusive, `end` is exclusive — a message at exactly
 *   `start` delivers, a message at exactly `end` waits for the next
 *   window.
 * - An overnight window (`start` later than `end`, e.g. `22:00-02:00`)
 *   spans midnight: the after-midnight hours belong to the day the
 *   window started, so a Friday-night window covers Saturday morning.
 * - `start` equal to `end` means the whole day (a window that never
 *   closes), on the listed weekdays — the degenerate case is all-day,
 *   not empty, so a misconfigured window cannot silently starve a
 *   subscriber forever.
 * - `days` defaults to every weekday; `timeZone` defaults to `UTC`.
 *   An unknown time zone is a `RangeError` at subscribe time, never a
 *   silent fallback to the server's local zone.
 *
 * What the window is not, stated plainly:
 *
 * - Not a one-shot delay: `delayed.ts` schedules a single future
 *   fan-out; a window is a recurring calendar shape the subscriber
 *   lives inside. A delayed message that fans out while the window
 *   is closed is held like any live message — the window gate sits
 *   in `deliverToSubscriber`, downstream of the delayed fan-out.
 * - Not backpressure: the hold is pre-queue (like the EB-98 barrier
 *   hold), so it consumes no queue budget, no drop-policy shed, and
 *   no throttle credit. Backpressure still applies at flush time,
 *   when the held message finally enters the normal pipeline.
 * - Not an ack clock: a held message has no delivery handle yet, so
 *   a reliable subscriber's ack timer starts when the message
 *   flushes into the handler, not when it arrived — window waiting
 *   never counts toward an ack timeout.
 * - Not a replay gate: durable-log replays and backfills call
 *   `deliverUnkeyed` directly and bypass the window, like they
 *   bypass the barrier gate — history catch-up is not live
 *   traffic shaping. Barrier control messages bypass it too: a cut
 *   marker must never be held behind a calendar.
 */

const TIME_OF_DAY = /^([01]\d|2[0-3]):([0-5]\d)$/;
const WEEKDAY_NAMES = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] as const;

/** One recurring delivery window (see the module docs). */
export interface DeliveryWindow {
  /** Inclusive start, `HH:MM` 24-hour local time. */
  start: string;
  /** Exclusive end, `HH:MM` 24-hour local time. */
  end: string;
  /** Weekdays the window applies to, 0 = Sunday … 6 = Saturday. Defaults to all days. */
  days?: number[];
  /** IANA time zone the window is evaluated in. Defaults to `UTC`. */
  timeZone?: string;
}

/** `SubscribeOptions.deliveryWindows` (EB-230). */
export interface DeliveryWindowOptions {
  /** At least one window; an empty list means all-day delivery. */
  windows: DeliveryWindow[];
  /**
   * Maximum milliseconds a message may wait in the window hold.
   * When the oldest held message has waited longer than this, new
   * arrivals are refused fail-closed (counted, never silently
   * dropped) instead of joining an ever-growing hold. The already
   * held messages stay — they still flush when the window opens.
   * Absent means unbounded holding.
   */
  maxHoldMs?: number;
}

/** A validated, normalized delivery window. */
export interface ResolvedDeliveryWindow {
  startMin: number;
  endMin: number;
  days: ReadonlySet<number>;
  timeZone: string;
}

/** A validated `DeliveryWindowOptions`, ready for the gate. */
export interface ResolvedDeliveryWindowOptions {
  windows: ResolvedDeliveryWindow[];
  maxHoldMs?: number;
}

function parseTimeOfDay(value: unknown, field: string): number {
  if (typeof value !== 'string' || !TIME_OF_DAY.test(value)) {
    throw new RangeError(`delivery window ${field} must be HH:MM (24-hour), got ${JSON.stringify(value)}`);
  }
  const [h, m] = value.split(':').map(Number);
  return h * 60 + m;
}

function resolveTimeZone(value: unknown): string {
  const zone = value === undefined ? 'UTC' : value;
  if (typeof zone !== 'string' || zone.length === 0) {
    throw new RangeError(`delivery window timeZone must be a non-empty IANA name, got ${JSON.stringify(value)}`);
  }
  try {
    // Throws RangeError for an unknown zone — that is the validation.
    new Intl.DateTimeFormat('en-US', { timeZone: zone });
  } catch {
    throw new RangeError(`delivery window timeZone is not a known IANA zone: ${zone}`);
  }
  return zone;
}

function resolveDays(value: unknown): ReadonlySet<number> {
  if (value === undefined) return new Set([0, 1, 2, 3, 4, 5, 6]);
  if (!Array.isArray(value) || value.length === 0) {
    throw new RangeError('delivery window days must be a non-empty array of weekday numbers 0-6');
  }
  const days = new Set<number>();
  for (const d of value) {
    if (!Number.isInteger(d) || d < 0 || d > 6) {
      throw new RangeError(`delivery window days must be integers 0-6, got ${JSON.stringify(d)}`);
    }
    days.add(d);
  }
  return days;
}

/** Validates one window; throws `RangeError` on any malformed field. */
export function resolveDeliveryWindow(window: DeliveryWindow): ResolvedDeliveryWindow {
  if (window === null || typeof window !== 'object' || Array.isArray(window)) {
    throw new RangeError('delivery window must be an object with start and end');
  }
  return {
    startMin: parseTimeOfDay(window.start, 'start'),
    endMin: parseTimeOfDay(window.end, 'end'),
    days: resolveDays(window.days),
    timeZone: resolveTimeZone(window.timeZone),
  };
}

/** Validates `DeliveryWindowOptions`; throws `RangeError` on bad input. */
export function resolveDeliveryWindowOptions(
  opts: DeliveryWindowOptions | undefined,
): ResolvedDeliveryWindowOptions | undefined {
  if (opts === undefined) return undefined;
  if (opts === null || typeof opts !== 'object' || Array.isArray(opts)) {
    throw new RangeError('deliveryWindows must be an object with a windows array');
  }
  if (!Array.isArray(opts.windows)) {
    throw new RangeError('deliveryWindows.windows must be an array');
  }
  let maxHoldMs: number | undefined;
  if (opts.maxHoldMs !== undefined) {
    if (!Number.isFinite(opts.maxHoldMs) || opts.maxHoldMs < 0) {
      throw new RangeError('deliveryWindows.maxHoldMs must be a finite number of milliseconds >= 0');
    }
    maxHoldMs = opts.maxHoldMs;
  }
  return {
    windows: opts.windows.map(resolveDeliveryWindow),
    maxHoldMs,
  };
}

interface LocalParts {
  weekday: number;
  minutes: number;
}

function localParts(timeZone: string, nowMs: number): LocalParts {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    weekday: 'short',
    hour: 'numeric',
    minute: 'numeric',
    hourCycle: 'h23',
  }).formatToParts(new Date(nowMs));
  let weekday = -1;
  let hour = 0;
  let minute = 0;
  for (const part of parts) {
    if (part.type === 'weekday') weekday = WEEKDAY_NAMES.indexOf(part.value as (typeof WEEKDAY_NAMES)[number]);
    if (part.type === 'hour') hour = Number(part.value) % 24;
    if (part.type === 'minute') minute = Number(part.value);
  }
  if (weekday < 0) throw new RangeError(`delivery window: could not resolve weekday for zone ${timeZone}`);
  return { weekday, minutes: hour * 60 + minute };
}

/** True when `nowMs` falls inside this window (see the module docs). */
export function isInDeliveryWindow(window: ResolvedDeliveryWindow, nowMs: number): boolean {
  const { weekday, minutes } = localParts(window.timeZone, nowMs);
  if (window.startMin === window.endMin) {
    // Degenerate window: the whole listed day.
    return window.days.has(weekday);
  }
  if (window.startMin < window.endMin) {
    return minutes >= window.startMin && minutes < window.endMin && window.days.has(weekday);
  }
  // Overnight window: the evening leg belongs to `weekday`, the
  // after-midnight leg belongs to the previous day's window.
  if (minutes >= window.startMin) return window.days.has(weekday);
  if (minutes < window.endMin) return window.days.has((weekday + 6) % 7);
  return false;
}

/** True when `nowMs` falls inside any of the windows (empty = all day). */
export function isInDeliveryWindows(windows: readonly ResolvedDeliveryWindow[], nowMs: number): boolean {
  if (windows.length === 0) return true;
  return windows.some((w) => isInDeliveryWindow(w, nowMs));
}

/**
 * The next millisecond at or after `nowMs` when any window opens,
 * scanning minute by minute (at most 8 days out — a weekly window
 * always recurs within a week). Returns `undefined` for an empty
 * window list, which is already always open.
 */
export function nextDeliveryWindowOpenMs(
  windows: readonly ResolvedDeliveryWindow[],
  nowMs: number,
): number | undefined {
  if (windows.length === 0) return undefined;
  if (isInDeliveryWindows(windows, nowMs)) return nowMs;
  const startMinute = Math.floor(nowMs / 60000) * 60000;
  for (let i = 1; i <= 8 * 24 * 60; i++) {
    const candidate = startMinute + i * 60000;
    if (isInDeliveryWindows(windows, candidate)) return candidate;
  }
  return undefined;
}

/** One message held behind a closed delivery window, in arrival order. */
export interface DeliveryWindowHeld {
  msg: unknown;
  expiresAt: number | undefined;
  rawPayload: unknown;
  keyed?: { key: string; keySeq: number };
  causal?: { source: string; clock: number };
  /** Bus-clock timestamp when the message entered the hold. */
  heldAtMs: number;
}

/**
 * The per-subscriber window hold: a FIFO of messages that arrived
 * while the subscriber's windows were closed. Pre-queue, so it
 * consumes no backpressure budget; flushed messages re-enter the
 * normal pipeline, so filters, throttles and queues still apply
 * at flush time.
 */
export class DeliveryWindowGate {
  readonly windows: readonly ResolvedDeliveryWindow[];
  readonly maxHoldMs?: number;
  private held: DeliveryWindowHeld[] = [];
  /** Messages ever held by this gate. */
  heldCount = 0;
  /** Messages ever flushed by this gate. */
  flushedCount = 0;
  /** Arrivals refused by the max-hold fail-closed rule. */
  rejectedCount = 0;

  constructor(options: ResolvedDeliveryWindowOptions) {
    this.windows = options.windows;
    this.maxHoldMs = options.maxHoldMs;
  }

  /** Messages currently waiting in the hold, oldest first. */
  get heldMessages(): number {
    return this.held.length;
  }

  /** True when the subscriber's windows are open at `nowMs`. */
  isOpen(nowMs: number): boolean {
    return isInDeliveryWindows(this.windows, nowMs);
  }

  /** Held entries whose wait already exceeds `maxHoldMs` at `nowMs`. */
  overlongCount(nowMs: number): number {
    if (this.maxHoldMs === undefined) return 0;
    return this.held.filter((h) => nowMs - h.heldAtMs > this.maxHoldMs).length;
  }

  /**
   * Holds one arrival. Returns `'held'`, or `'rejected'` when the
   * max-hold rule fires: the oldest held message has already waited
   * longer than `maxHoldMs`, so the hold is full in time, not in
   * count — the newcomer is refused (counted) rather than silently
   * queued behind a stale backlog.
   */
  hold(entry: DeliveryWindowHeld, nowMs: number): 'held' | 'rejected' {
    if (
      this.maxHoldMs !== undefined &&
      this.held.length > 0 &&
      nowMs - this.held[0].heldAtMs > this.maxHoldMs
    ) {
      this.rejectedCount += 1;
      return 'rejected';
    }
    this.held.push(entry);
    this.heldCount += 1;
    return 'held';
  }

  /**
   * Removes and returns everything held, in arrival order, when the
   * windows are open at `nowMs`; otherwise returns an empty array
   * and leaves the hold untouched.
   */
  flush(nowMs: number): DeliveryWindowHeld[] {
    if (!this.isOpen(nowMs)) return [];
    const out = this.held;
    this.held = [];
    this.flushedCount += out.length;
    return out;
  }

  /**
   * Removes and returns everything currently held, regardless of
   * whether the windows are open (EB-263): the revision path uses
   * this to move held messages into a replacement gate — or, on a
   * clear, straight into delivery — so a window revision never
   * silently drops a held message. `heldCount` keeps counting this
   * gate's ever-held history; the receiving gate counts adopted
   * entries in its own `heldCount` via `adoptHeld`.
   */
  takeHeld(): DeliveryWindowHeld[] {
    const out = this.held;
    this.held = [];
    return out;
  }

  /**
   * Adopts entries taken from a previous gate (EB-263): arrival
   * order and `heldAtMs` are preserved, so the new gate's max-hold
   * clock keeps running from the original arrival, not from the
   * revision. The adopted entries count toward this gate's
   * `heldCount`, so lifetime hold accounting survives the swap.
   */
  adoptHeld(entries: DeliveryWindowHeld[]): void {
    for (const entry of entries) this.held.push(entry);
    this.heldCount += entries.length;
  }
}
