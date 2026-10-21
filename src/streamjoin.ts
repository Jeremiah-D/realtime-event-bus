/**
 * Dual-stream window join (EB-187).
 *
 * Stream-processing semantics, Flink-interval-join-style: `joinTopics`
 * declares two input topic patterns (`left` / `right`) and the engine
 * correlates their messages by key inside a sliding time window. Every
 * admitted publish is observed once (see `StreamJoinEngine.observe`):
 * the arrival probes the *opposite* side's window buffer for same-key
 * entries and, on a hit, derives a combined message onto the bus
 * (full admission — sequence number, durable log, subscriber fan-out —
 * so a join result is a first-class bus message, e.g. an order-payment
 * reconciliation record).
 *
 * Observation semantics (see `StreamJoinEngine.observe`):
 * - One observation per admitted publish in `fanOut` — after the
 *   admission gates and sequence assignment, before compression
 *   rewrites the envelope — so the join sees the raw application
 *   payload with a final sequence number. Rejected or shed publishes
 *   never feed a join, exactly like the publish-rate table.
 * - Correlation key comes from the join's `on` function, evaluated on
 *   the `BusMessage`. A throw is error-isolated: the join skips that
 *   message and counts it (`keyErrors`) — a broken key function must
 *   never disturb the bus.
 * - Window matching is on the *effective timestamp*
 *   (`msg.eventTime ??` observation time): a left arrival with
 *   effective time `t` probes the right buffer for same-key entries
 *   with effective times in `[t - windowMs, t]`. Each matching pair
 *   emits exactly once — the arrival probes only the opposite buffer
 *   and buffers itself afterwards, so a pair is never double-emitted.
 * - Lateness: an arrival whose effective time is older than
 *   `now - windowMs` can never match anything (its whole window has
 *   already been swept) — it is dropped and counted (`joinLate`),
 *   never buffered.
 * - Memory is bounded per key per side by `maxBufferedPerKey`: when a
 *   key's buffer is full the oldest entry is evicted first (FIFO) and
 *   counted (`joinEvicted`). Expired entries (older than
 *   `now - windowMs`) are swept lazily on every observation and by an
 *   unref'd periodic timer, so an idle bus holds no stale state and
 *   never holds the process open.
 * - `how: 'left'` is a left-outer join: a left arrival with no
 *   in-window right peer still emits, with `right: null` and
 *   `rightSeq: null`. `how: 'inner'` emits only on a hit.
 * - Join outputs never re-enter join observation (`joinDerived`): a
 *   join can never trigger itself — one hop, like topic routes and CEP
 *   matches. CEP sequence patterns (EB-170) *do* still observe join
 *   outputs, so matches can feed downstream pattern detection.
 *
 * Orthogonality notes:
 * - EB-73 (subscriber-side tumbling-window aggregation) composes
 *   freely: it runs per subscriber at delivery time, while joins run
 *   once per admitted publish — a join output topic can itself be
 *   subscribed with `SubscribeOptions.window`.
 * - EB-59 (event-time watermark) is a per-topic lateness *measure*;
 *   the join window is an arrival-pairing *predicate*. Both read
 *   `eventTime` when present, but neither changes the other's
 *   semantics.
 *
 * The class is pure in-memory state: the bus owns topic matching
 * (`compilePattern` is injected — this module must not import
 * `bus.ts` at runtime, or it closes an import cycle), the clock
 * (`EventBusOptions.now`) and join emission (which publishes through
 * the bus's own pipeline).
 */

import type { BusMessage } from './bus.ts';

/** Correlation-key extractor for a join: receives the admitted message. */
export type StreamJoinKeyFn = (msg: BusMessage) => string | undefined;

/** Join flavor: `'inner'` emits only on a hit; `'left'` also emits left rows with no right peer. */
export type StreamJoinHow = 'inner' | 'left';

/** Options for `EventBus.joinTopics`. */
export interface StreamJoinOptions {
  /**
   * Stable join id; defaults to `join-<n>`. Must match
   * `/^[A-Za-z0-9_-]{1,64}$/`; unique per bus.
   */
  id?: string;
  /**
   * Left input: a topic pattern (`compilePattern` syntax). When the
   * join is defined via `EventBus.joinTopics`, the `topics` argument
   * wins over this field.
   */
  left?: string;
  /**
   * Right input: a topic pattern (`compilePattern` syntax). Same
   * override rule as `left`.
   */
  right?: string;
  /**
   * Correlation key for an admitted message. May return `undefined`
   * (treated as `''`). A throw is error-isolated: the join skips that
   * message and counts `keyErrors`.
   */
  on: StreamJoinKeyFn;
  /**
   * Sliding window in milliseconds: a left/right pair matches when the
   * opposite side's effective timestamp falls in
   * `[arrivalTs - windowMs, arrivalTs]`. Must be a positive finite
   * number.
   */
  windowMs: number;
  /** `'inner'` or `'left'` (left-outer). */
  how: StreamJoinHow;
  /**
   * Topic the combined messages are published to — through the bus's
   * normal admission pipeline (ACL, schema, rate limit, TTL, durable
   * log). Must be a non-empty string.
   */
  outputTopic: string;
  /**
   * Bound on buffered entries per key per side (default 1000); the
   * oldest entry is evicted first when exceeded. Must be a positive
   * integer.
   */
  maxBufferedPerKey?: number;
}

/** One buffered join input. */
interface BufferedJoinEvent {
  /** Effective timestamp (`eventTime ??` observation time). */
  at: number;
  /** Raw application payload at admission time. */
  payload: unknown;
  /** The publish-order sequence number stamped on the message. */
  seq: number;
}

/** One derived join output, published to the join's output topic. */
export interface StreamJoinOutput {
  /** The join that derived this output. */
  joinId: string;
  /** The correlation key both sides matched on. */
  key: string;
  /** Left-side payload (always present). */
  left: unknown;
  /** Right-side payload, or `null` when `how: 'left'` found no right peer. */
  right: unknown;
  /** Left message's publish-order sequence number. */
  leftSeq: number;
  /** Right message's sequence number, or `null` when there was no right peer. */
  rightSeq: number | null;
  /** The probing arrival's window start (`arrivalTs - windowMs`). */
  windowStart: number;
  /** Bus-clock time the join was derived. */
  joinedAt: number;
}

/** Per-join counters for `getStats().joins`. */
export interface StreamJoinStats {
  joinId: string;
  left: string;
  right: string;
  outputTopic: string;
  how: StreamJoinHow;
  windowMs: number;
  /** Combined messages derived. */
  joined: number;
  /** Arrivals dropped as too late to ever match. */
  joinLate: number;
  /** Buffered entries evicted by the per-key capacity bound. */
  joinEvicted: number;
  /** Arrivals skipped because the key function threw. */
  keyErrors: number;
  /** Entries currently buffered per side. */
  bufferedLeft: number;
  bufferedRight: number;
}

export interface StreamJoinEngineDeps {
  /** Bus clock in milliseconds (`EventBusOptions.now`). */
  now: () => number;
  /** Topic-pattern compiler (`compilePattern` from the bus). */
  compile: (pattern: string) => RegExp;
  /** Emits a derived join output back onto the bus (join-derived publish). */
  emitJoin: (joinId: string, outputTopic: string, output: StreamJoinOutput) => void;
  /** Timer for periodic window sweeps. Defaults to an unref'd `setTimeout`. */
  setTimer?: (fn: () => void, ms: number) => unknown;
  /** Timer-clear for window sweeps. Defaults to `clearTimeout`. */
  clearTimer?: (handle: unknown) => void;
}

/** Default sweep timer: unref'd, so an idle join never holds the process open. */
function defaultSetTimer(fn: () => void, ms: number): unknown {
  const timer = setTimeout(fn, ms);
  if (typeof (timer as { unref?: () => void }).unref === 'function') {
    (timer as unknown as { unref: () => void }).unref();
  }
  return timer;
}

function defaultClearTimer(handle: unknown): void {
  clearTimeout(handle as ReturnType<typeof setTimeout>);
}

/** Sweep cadence cap: an idle join re-sweeps at most this often. */
const MAX_SWEEP_INTERVAL_MS = 60_000;

/** Join-id alphabet: 1–64 ASCII alphanumerics, `_` or `-`. */
const JOIN_ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

/**
 * Validates join options the way a bus setter would: `RangeError` on
 * anything illegal, thrown before anything is registered.
 */
export function validateStreamJoinOptions(
  opts: StreamJoinOptions,
  compile: (pattern: string) => RegExp,
): asserts opts is StreamJoinOptions {
  if (opts === null || typeof opts !== 'object') {
    throw new RangeError('join options must be an object');
  }
  if (opts.id !== undefined && (typeof opts.id !== 'string' || !JOIN_ID_PATTERN.test(opts.id))) {
    throw new RangeError("join id must match /^[A-Za-z0-9_-]{1,64}$/");
  }
  for (const [name, value] of [
    ['left', opts.left],
    ['right', opts.right],
    ['outputTopic', opts.outputTopic],
  ] as const) {
    if (value !== undefined && (typeof value !== 'string' || value.length === 0)) {
      throw new RangeError(`${name} must be a non-empty string`);
    }
  }
  if (typeof opts.on !== 'function') {
    throw new RangeError('on must be a key function');
  }
  if (!Number.isFinite(opts.windowMs) || opts.windowMs <= 0) {
    throw new RangeError('windowMs must be a positive finite number of milliseconds');
  }
  if (opts.how !== 'inner' && opts.how !== 'left') {
    throw new RangeError("how must be 'inner' or 'left'");
  }
  const maxBuffered = opts.maxBufferedPerKey ?? 1000;
  if (!Number.isInteger(maxBuffered) || maxBuffered <= 0) {
    throw new RangeError('maxBufferedPerKey must be a positive integer');
  }
  // Compile eagerly so a bad pattern fails fast here, not mid-observation.
  compile(opts.left);
  compile(opts.right);
}

interface CompiledJoin {
  id: string;
  left: RegExp;
  right: RegExp;
  leftPattern: string;
  rightPattern: string;
  keyOf: StreamJoinKeyFn;
  windowMs: number;
  how: StreamJoinHow;
  outputTopic: string;
  maxBufferedPerKey: number;
  leftBuf: Map<string, BufferedJoinEvent[]>;
  rightBuf: Map<string, BufferedJoinEvent[]>;
  leftBuffered: number;
  rightBuffered: number;
  joined: number;
  late: number;
  evicted: number;
  keyErrors: number;
  sweepTimer: unknown;
}

export class StreamJoinEngine {
  private readonly deps: StreamJoinEngineDeps;
  private readonly setTimer: (fn: () => void, ms: number) => unknown;
  private readonly clearTimer: (handle: unknown) => void;
  private readonly joins = new Map<string, CompiledJoin>();
  private idCounter = 0;
  private totalJoined = 0;
  private totalLate = 0;
  private totalEvicted = 0;

  constructor(deps: StreamJoinEngineDeps) {
    this.deps = deps;
    this.setTimer = deps.setTimer ?? defaultSetTimer;
    this.clearTimer = deps.clearTimer ?? defaultClearTimer;
  }

  /**
   * Defines a join: validates, compiles, starts the unref'd sweep
   * timer, and returns the join id. Throws `RangeError` on invalid
   * options or a duplicate id — before anything is registered.
   */
  define(topics: { left: string; right: string }, opts: StreamJoinOptions): string {
    validateStreamJoinOptions({ ...opts, left: topics.left, right: topics.right }, this.deps.compile);
    const id = opts.id ?? `join-${++this.idCounter}`;
    if (this.joins.has(id)) {
      throw new RangeError(`join id "${id}" is already defined`);
    }
    const join: CompiledJoin = {
      id,
      left: this.deps.compile(topics.left),
      right: this.deps.compile(topics.right),
      leftPattern: topics.left,
      rightPattern: topics.right,
      keyOf: opts.on,
      windowMs: opts.windowMs,
      how: opts.how,
      outputTopic: opts.outputTopic,
      maxBufferedPerKey: opts.maxBufferedPerKey ?? 1000,
      leftBuf: new Map(),
      rightBuf: new Map(),
      leftBuffered: 0,
      rightBuffered: 0,
      joined: 0,
      late: 0,
      evicted: 0,
      keyErrors: 0,
      sweepTimer: undefined,
    };
    this.joins.set(id, join);
    this.startSweep(join);
    return id;
  }

  /**
   * Removes a join: stops its sweep timer and drops all buffered
   * state. Returns false when the id is unknown.
   */
  remove(joinId: string): boolean {
    const join = this.joins.get(joinId);
    if (join === undefined) return false;
    this.stopSweep(join);
    this.joins.delete(joinId);
    return true;
  }

  /** Joins currently defined. */
  get size(): number {
    return this.joins.size;
  }

  /** Starts the unref'd periodic window sweep for a join. */
  private startSweep(join: CompiledJoin): void {
    const interval = Math.min(join.windowMs, MAX_SWEEP_INTERVAL_MS);
    const tick = (): void => {
      // The join may have been removed while the timer was in flight.
      if (!this.joins.has(join.id)) return;
      this.sweepJoin(join, this.deps.now());
      join.sweepTimer = this.setTimer(tick, interval);
    };
    join.sweepTimer = this.setTimer(tick, interval);
  }

  /** Stops the sweep timer; buffered state is dropped by `remove`. */
  private stopSweep(join: CompiledJoin): void {
    if (join.sweepTimer !== undefined) {
      this.clearTimer(join.sweepTimer);
      join.sweepTimer = undefined;
    }
  }

  /**
   * Observes one admitted publish for every defined join. A message
   * matching the left pattern, the right pattern, or both is processed
   * once per matched role: probe the opposite buffer, emit on hits
   * (or on a left miss for `how: 'left'`), then buffer itself.
   */
  observe(msg: BusMessage): void {
    const now = this.deps.now();
    for (const join of this.joins.values()) {
      const leftMatch = join.left.test(msg.topic);
      const rightMatch = join.right.test(msg.topic);
      if (!leftMatch && !rightMatch) continue;
      // Effective timestamp: business event time when the publish
      // carried one, otherwise the observation time.
      const eventTime = msg.eventTime;
      const ts =
        typeof eventTime === 'number' && Number.isFinite(eventTime) && eventTime >= 0
          ? eventTime
          : now;
      if (ts < now - join.windowMs) {
        // Too late: its whole window has already been swept — it can
        // never match. Dropped, counted, never buffered.
        join.late += 1;
        this.totalLate += 1;
        continue;
      }
      let key: string | undefined;
      try {
        key = join.keyOf(msg);
      } catch {
        // Error-isolated, like CEP's keyOf: a broken key function
        // skips the message instead of disturbing the bus.
        join.keyErrors += 1;
        continue;
      }
      const k = key ?? '';
      this.sweepJoin(join, now);
      if (leftMatch) {
        this.arrive(join, 'left', msg, k, ts, now);
      }
      if (rightMatch) {
        this.arrive(join, 'right', msg, k, ts, now);
      }
    }
  }

  /**
   * Processes one arrival in one role: probes the opposite side's
   * buffer for same-key entries inside `[ts - windowMs, ts]`, emits a
   * combined message per hit (or one left-outer row for `how:
   * 'left'`), then buffers the arrival for future opposite-side
   * probes.
   */
  private arrive(
    join: CompiledJoin,
    side: 'left' | 'right',
    msg: BusMessage,
    key: string,
    ts: number,
    now: number,
  ): void {
    const ownBuf = side === 'left' ? join.leftBuf : join.rightBuf;
    const otherBuf = side === 'left' ? join.rightBuf : join.leftBuf;
    const peers = otherBuf.get(key) ?? [];
    const windowStart = ts - join.windowMs;
    let matched = false;
    for (const peer of peers) {
      // The window is [ts - windowMs, ts]: a peer from the "future"
      // (larger effective time, e.g. a skewed eventTime) does not
      // match — the pair will be reconsidered when the later side
      // arrives and probes back.
      if (peer.at < windowStart || peer.at > ts) continue;
      matched = true;
      this.emitJoined(join, side, msg, peer, key, windowStart, now);
    }
    if (!matched && side === 'left' && join.how === 'left') {
      this.emitJoined(join, side, msg, undefined, key, windowStart, now);
    }
    // Buffer the arrival for future opposite-side probes — after
    // probing, so it never matches itself.
    let list = ownBuf.get(key);
    if (list === undefined) {
      list = [];
      ownBuf.set(key, list);
    }
    list.push({ at: ts, payload: msg.payload, seq: msg.seq });
    if (side === 'left') join.leftBuffered += 1;
    else join.rightBuffered += 1;
    // Capacity bound: evict the oldest first (FIFO), counted.
    while (list.length > join.maxBufferedPerKey) {
      list.shift();
      join.evicted += 1;
      this.totalEvicted += 1;
      if (side === 'left') join.leftBuffered -= 1;
      else join.rightBuffered -= 1;
    }
  }

  /** Derives and emits one combined message for a matched pair. */
  private emitJoined(
    join: CompiledJoin,
    side: 'left' | 'right',
    msg: BusMessage,
    peer: BufferedJoinEvent | undefined,
    key: string,
    windowStart: number,
    now: number,
  ): void {
    const output: StreamJoinOutput = {
      joinId: join.id,
      key,
      left: side === 'left' ? msg.payload : (peer as BufferedJoinEvent).payload,
      right: side === 'right' ? msg.payload : (peer === undefined ? null : peer.payload),
      leftSeq: side === 'left' ? msg.seq : (peer as BufferedJoinEvent).seq,
      rightSeq: side === 'right' ? msg.seq : peer === undefined ? null : peer.seq,
      windowStart,
      joinedAt: now,
    };
    join.joined += 1;
    this.totalJoined += 1;
    try {
      this.deps.emitJoin(join.id, join.outputTopic, output);
    } catch {
      // The bus's publish never throws into observers — but a custom
      // emitJoin might; a broken emitter must not disturb observation.
    }
  }

  /**
   * Drops entries older than `now - windowMs`: they can never match a
   * future arrival (any future arrival's window starts at or after
   * `now - windowMs`). Runs lazily on every observation and on the
   * unref'd sweep timer.
   */
  private sweepJoin(join: CompiledJoin, now: number): void {
    const horizon = now - join.windowMs;
    for (const [buf, side] of [
      [join.leftBuf, 'left'],
      [join.rightBuf, 'right'],
    ] as const) {
      for (const [key, list] of buf) {
        let expired = 0;
        while (expired < list.length && (list[expired] as BufferedJoinEvent).at < horizon) {
          expired += 1;
        }
        if (expired > 0) {
          list.splice(0, expired);
          if (side === 'left') join.leftBuffered -= expired;
          else join.rightBuffered -= expired;
          if (list.length === 0) buf.delete(key);
        }
      }
    }
  }

  /** Per-join stats snapshot for `getStats().joins`. */
  snapshot(): StreamJoinStats[] {
    return [...this.joins.values()].map((join) => ({
      joinId: join.id,
      left: join.leftPattern,
      right: join.rightPattern,
      outputTopic: join.outputTopic,
      how: join.how,
      windowMs: join.windowMs,
      joined: join.joined,
      joinLate: join.late,
      joinEvicted: join.evicted,
      keyErrors: join.keyErrors,
      bufferedLeft: join.leftBuffered,
      bufferedRight: join.rightBuffered,
    }));
  }

  /** Global counters for the Prometheus exposition. */
  totals(): { joined: number; late: number; evicted: number } {
    return { joined: this.totalJoined, late: this.totalLate, evicted: this.totalEvicted };
  }
}
