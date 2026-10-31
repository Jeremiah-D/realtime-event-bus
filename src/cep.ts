/**
 * CEP sequence detection (EB-170).
 *
 * Stream-processing semantics, Flink-CEP-style: `defineSequence` declares a
 * pattern of steps (each a topic pattern matched against the
 * alias-resolved concrete topic), and the engine tracks *partial matches* —
 * per-key state machines that advance as admitted publishes arrive. When a
 * partial reaches the last step inside `withinMs` of its first matched
 * event, the engine derives a match event onto the bus (topic
 * `cep.<patternId>`, full admission — sequence number, durable log,
 * subscriber fan-out — so a match is a first-class bus message).
 *
 * Observation semantics (see `CepEngine.observe`):
 * - One observation per admitted publish in `fanOut` — after the admission
 *   gates and sequence assignment. Rejected or shed publishes never feed
 *   pattern detection, exactly like the publish-rate table.
 * - Correlation defaults to the publish key (`BusMessage.key ?? ''`), so
 *   "the same key saw A then B" is the primitive; `keyOf` overrides it.
 * - A partial older than `withinMs` can never complete: it is dropped
 *   lazily on the next observation (no background timer — the bus only
 *   sweeps when it observes, so an idle bus burns no CPU).
 * - Memory is bounded per pattern by `maxPartialMatches`: the oldest
 *   partial is evicted first (FIFO).
 * - Match events never re-enter pattern detection (`cepDerived`): a match
 *   can never trigger itself, and CEP patterns never observe each other's
 *   derived traffic — one hop, like topic routes.
 *
 * Absence semantics (EB-174): a step with `absence: true` is a negation.
 * When a partial reaches it, the engine arms an unref'd deadline timer
 * (`absenceMs`, default `withinMs`). If a message matching the step's
 * topic arrives for the key first, the partial is killed — the absence can
 * never fire. If the deadline passes event-free, the absence is recorded
 * and the partial advances (completing the pattern when the absence step
 * was last). The timer re-checks the pattern window on fire, so a partial
 * that outlived `withinMs` while the bus was idle is dropped instead of
 * deriving a stale absence. `getStats().cep` exposes `absencePatterns`
 * and `absenceMatches`.
 *
 * The class is pure in-memory state: the bus owns topic matching
 * (`compilePattern` is injected — this module must not import
 * `bus.ts` at runtime, or it closes an import cycle), the clock
 * (`EventBusOptions.now`) and match emission (which publishes through
 * the bus's own pipeline).
 */

import type { BusMessage } from './bus.ts';

/** One step of a sequence pattern: the topic pattern a message must match. */
export interface SequenceStep {
  /**
   * Topic pattern matched against the message's alias-resolved concrete
   * topic (`compilePattern` semantics: `*` one segment, `**` any run of
   * segments, other segments literal). Must be a non-empty string.
   */
  topic: string;
  /**
   * Negation step (EB-174): when true, the partial advances only if NO
   * message matching `topic` arrives for the key within `absenceMs`
   * after the previous step matched. A matching arrival kills the
   * partial instead — the absence can never fire. The first step can
   * never be an absence step: there is no anchor to start the window
   * from.
   */
  absence?: boolean;
  /**
   * Absence window in milliseconds, measured from when the partial
   * reaches this step. Defaults to the pattern's `withinMs`. Must be a
   * positive finite number when set on an absence step; ignored on
   * positive steps.
   */
  absenceMs?: number;
}

/** Options for `defineSequence`. */
export interface SequencePatternOptions {
  /**
   * Stable pattern id; the match topic is `cep.<id>`. Defaults to
   * `seq-<n>`. Must match `/^[A-Za-z0-9_-]{1,64}$/` so the derived topic
   * stays a single dot-segment; unique per bus.
   */
  id?: string;
  /** At least two steps; a partial advances when the message's topic matches the next step's pattern. */
  steps: SequenceStep[];
  /**
   * Maximum span in milliseconds between the first and last matched event.
   * A partial older than this can never complete and is dropped. Must be
   * a positive finite number.
   */
  withinMs: number;
  /**
   * Correlation key for a matched message. Defaults to the publish key
   * (`msg.key ?? ''`): messages sharing a key advance the same partial
   * state machines. May return `undefined` (treated as `''`). A throw is
   * error-isolated: the pattern skips that message.
   */
  keyOf?: (msg: BusMessage) => string | undefined;
  /**
   * Bound on live partial matches for this pattern (default 1000); the
   * oldest partial is evicted first when exceeded. Must be a positive
   * integer.
   */
  maxPartialMatches?: number;
}

/**
 * Revision for one defined sequence pattern (see
 * `EventBus.updateCepSequencePattern`, EB-297). Only the fields provided
 * move; the rest keep their live values. The steps, the correlation key,
 * and the pattern id are immutable — changing the pattern definition
 * itself requires `clear` + `define`.
 */
export interface SequencePatternRevision {
  /**
   * Maximum span in milliseconds between the first and last matched
   * event. Must be a positive finite number when provided — the
   * define-time rule.
   */
  withinMs?: number;
  /**
   * Bound on live partial matches for this pattern. Must be a positive
   * integer when provided — the define-time rule.
   */
  maxPartialMatches?: number;
}

/** One event that completed a sequence step, carried on the match. */
export interface CepMatchedEvent {
  /** The alias-resolved concrete topic the message was published to. */
  topic: string;
  /** The publish-order sequence number stamped on the message. */
  seq: number;
  /** The message's publish key, when it carried one. */
  key?: string;
  /** The raw application payload at admission time. */
  payload: unknown;
  /** Bus-clock time the message was observed. */
  at: number;
}

/** One satisfied absence condition, carried on the derived match. */
export interface AbsenceMatchInfo {
  /** Index of the absence step that was satisfied. */
  stepIndex: number;
  /** The absence step's topic pattern — the event that did NOT occur. */
  topic: string;
  /** The absence window in milliseconds that elapsed event-free. */
  windowMs: number;
}

/** A completed sequence match, emitted by the engine. */
export interface SequenceMatchEvent {
  /** The pattern that completed. */
  patternId: string;
  /** The correlation key the partial was tracked under. */
  key: string;
  /** The matched events, in step order. */
  events: CepMatchedEvent[];
  /** Milliseconds from the first to the last matched event. */
  durationMs: number;
  /** Bus-clock time the match completed. */
  at: number;
  /**
   * Satisfied absence conditions, in step order (EB-174). Present only
   * when the match completed through at least one absence step — a
   * purely positive match carries no `absences` field.
   */
  absences?: AbsenceMatchInfo[];
}

/** Live counters for the CEP surface (`getStats().cep`). */
export interface CepStats {
  /** Patterns currently defined. */
  activePatterns: number;
  /** Partial matches alive across all patterns. */
  partialMatches: number;
  /** Match events emitted since the engine was created. */
  matches: number;
  /** Patterns with at least one absence step (EB-174). */
  absencePatterns: number;
  /**
   * Absence steps satisfied since the engine was created: each deadline
   * that passed with no matching event for the key (EB-174).
   */
  absenceMatches: number;
}

const PATTERN_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;
const DEFAULT_MAX_PARTIAL_MATCHES = 1000;

interface PartialMatch {
  key: string;
  /** Index of the next step this partial is waiting for. */
  stepIndex: number;
  events: CepMatchedEvent[];
  /** Bus-clock time the first event was observed. */
  startedAt: number;
  /** Absence conditions satisfied so far, in step order (EB-174). */
  absences: AbsenceMatchInfo[];
  /**
   * Armed absence-deadline timer while `stepIndex` points at an absence
   * step; `undefined` otherwise. Disarmed on kill, sweep, eviction and
   * clear — a dead partial never fires.
   */
  absenceTimer: unknown;
  /**
   * Set when the partial is dead (killed, swept, evicted, cleared,
   * completed). The timer callback checks it: a stale timer is a no-op.
   */
  resolved: boolean;
}

interface CompiledPattern {
  id: string;
  steps: Array<{ topic: string; matcher: RegExp; absence: boolean; absenceMs?: number }>;
  withinMs: number;
  keyOf: (msg: BusMessage) => string | undefined;
  maxPartialMatches: number;
  partials: Map<string, PartialMatch[]>;
  /** Live partial count (the per-pattern memory bound). */
  partialCount: number;
}

export interface CepEngineDeps {
  /** Bus clock in milliseconds (`EventBusOptions.now`). */
  now: () => number;
  /** Topic-pattern compiler (`compilePattern` from the bus). */
  compile: (pattern: string) => RegExp;
  /** Emits a completed match back onto the bus (cep-derived publish). */
  emitMatch: (patternId: string, match: SequenceMatchEvent) => void;
  /**
   * Timer for absence deadlines (EB-174). Defaults to an unref'd
   * `setTimeout`, so a pending absence never holds the process open.
   */
  setTimer?: (fn: () => void, ms: number) => unknown;
  /** Timer-clear for absence deadlines. Defaults to `clearTimeout`. */
  clearTimer?: (handle: unknown) => void;
}

/** Default absence timer: unref'd, so a pending absence never holds the process open. */
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

/**
 * Validates sequence-pattern options the way a bus setter would: absent
 * is fine only where optional, otherwise `RangeError` — and throws before
 * anything is registered.
 */
export function validateSequencePatternOptions(
  opts: SequencePatternOptions,
  compile: (pattern: string) => RegExp,
): asserts opts is SequencePatternOptions {
  if (opts === null || typeof opts !== 'object') {
    throw new RangeError('sequence pattern options must be an object');
  }
  if (opts.id !== undefined && (typeof opts.id !== 'string' || !PATTERN_ID_RE.test(opts.id))) {
    throw new RangeError('sequence pattern id must match /^[A-Za-z0-9_-]{1,64}$/');
  }
  if (!Array.isArray(opts.steps) || opts.steps.length < 2) {
    throw new RangeError('sequence pattern needs at least two steps');
  }
  for (let i = 0; i < opts.steps.length; i++) {
    const step = opts.steps[i];
    if (step === null || typeof step !== 'object' || typeof step.topic !== 'string' || step.topic.length === 0) {
      throw new RangeError('each sequence step needs a non-empty topic pattern');
    }
    // Compile eagerly so a malformed pattern fails at define time, not at
    // match time. `compilePattern` never throws for ordinary patterns;
    // the call still pins the compiler's interpretation up front.
    compile(step.topic);
    if (step.absence !== undefined && typeof step.absence !== 'boolean') {
      throw new RangeError('sequence step absence must be a boolean');
    }
    if (i === 0 && step.absence === true) {
      throw new RangeError('the first sequence step cannot be an absence step');
    }
    if (step.absence === true && step.absenceMs !== undefined) {
      if (typeof step.absenceMs !== 'number' || !Number.isFinite(step.absenceMs) || step.absenceMs <= 0) {
        throw new RangeError('absenceMs must be a positive finite number of milliseconds');
      }
    }
  }
  if (typeof opts.withinMs !== 'number' || !Number.isFinite(opts.withinMs) || opts.withinMs <= 0) {
    throw new RangeError('withinMs must be a positive finite number of milliseconds');
  }
  if (opts.keyOf !== undefined && typeof opts.keyOf !== 'function') {
    throw new RangeError('keyOf must be a function');
  }
  if (opts.maxPartialMatches !== undefined) {
    if (
      typeof opts.maxPartialMatches !== 'number' ||
      !Number.isInteger(opts.maxPartialMatches) ||
      opts.maxPartialMatches <= 0
    ) {
      throw new RangeError('maxPartialMatches must be a positive integer');
    }
  }
}

export class CepEngine {
  private readonly deps: CepEngineDeps;
  private readonly setTimer: (fn: () => void, ms: number) => unknown;
  private readonly clearTimer: (handle: unknown) => void;
  private readonly patterns = new Map<string, CompiledPattern>();
  private idCounter = 0;
  private totalMatches = 0;
  private totalAbsenceMatches = 0;

  constructor(deps: CepEngineDeps) {
    this.deps = deps;
    this.setTimer = deps.setTimer ?? defaultSetTimer;
    this.clearTimer = deps.clearTimer ?? defaultClearTimer;
  }

  /** The topic a pattern's match events are emitted on. */
  static matchTopic(patternId: string): string {
    return `cep.${patternId}`;
  }

  /** Registers a pattern; returns its id. Throws `RangeError` on invalid options or a duplicate id. */
  define(opts: SequencePatternOptions): string {
    validateSequencePatternOptions(opts, this.deps.compile);
    const id = opts.id ?? `seq-${(this.idCounter += 1)}`;
    if (this.patterns.has(id)) {
      throw new RangeError(`sequence pattern "${id}" is already defined`);
    }
    this.patterns.set(id, {
      id,
      steps: opts.steps.map((step) => ({
        topic: step.topic,
        matcher: this.deps.compile(step.topic),
        absence: step.absence === true,
        ...(step.absenceMs === undefined ? {} : { absenceMs: step.absenceMs }),
      })),
      withinMs: opts.withinMs,
      keyOf: opts.keyOf ?? ((msg) => msg.key ?? ''),
      maxPartialMatches: opts.maxPartialMatches ?? DEFAULT_MAX_PARTIAL_MATCHES,
      partials: new Map(),
      partialCount: 0,
    });
    return id;
  }

  /**
   * Removes a pattern and drops its partials; returns whether one existed.
   * Pending absence timers are disarmed — a cleared pattern derives
   * nothing (EB-174).
   */
  clear(patternId: string): boolean {
    const pattern = this.patterns.get(patternId);
    if (pattern === undefined) return false;
    for (const partials of pattern.partials.values()) {
      for (const partial of partials) this.killPartial(pattern, partial);
    }
    return this.patterns.delete(patternId);
  }

  /**
   * Revises one defined pattern's window and partial bound (EB-297):
   * only `withinMs` / `maxPartialMatches` move — the steps, the
   * correlation key, and the pattern id are immutable. The new values
   * govern from the next observation: `observe` sweeps expired partials
   * against the live `withinMs` before every message (shrinking the
   * window expires in-flight partials on the next observation, widening
   * it extends their life), `startPartial` evicts against the live
   * `maxPartialMatches` (tightening it evicts the oldest partials on
   * the next new partial), and an armed absence deadline re-checks the
   * live `withinMs` on fire — an absence step without an explicit
   * `absenceMs` follows the live window from the next arming. In-flight
   * partials are untouched by the revision itself: already-recorded
   * events, timers, and absence records stay as they are.
   *
   * Returns whether anything moved, with the old and new values for the
   * audit record. A patch that provides neither field, and a patch
   * identical to the current values, both report `changed: false`.
   * Throws `RangeError` for an unknown pattern id, a non-object patch,
   * or an illegal value — the define-time rule for the two fields
   * (`withinMs` a positive finite number, `maxPartialMatches` a
   * positive integer) — and a failed call leaves the live values
   * untouched.
   */
  update(
    patternId: string,
    patch: SequencePatternRevision,
  ): {
    changed: boolean;
    withinMs: number;
    oldWithinMs: number;
    maxPartialMatches: number;
    oldMaxPartialMatches: number;
  } {
    const pattern = this.patterns.get(patternId);
    if (pattern === undefined) {
      throw new RangeError(`sequence pattern "${patternId}" is not defined`);
    }
    // An absent patch asks nothing to move.
    if (patch == null) {
      return {
        changed: false,
        withinMs: pattern.withinMs,
        oldWithinMs: pattern.withinMs,
        maxPartialMatches: pattern.maxPartialMatches,
        oldMaxPartialMatches: pattern.maxPartialMatches,
      };
    }
    if (typeof patch !== 'object' || Array.isArray(patch)) {
      throw new RangeError('sequence pattern revision must be an object with withinMs and/or maxPartialMatches');
    }
    // Validate before touching anything: `undefined` is not a value — a
    // partial patch keeps the fields it leaves out — so resolution
    // happens here, against the merged values, before anything moves.
    const withinMs = patch.withinMs ?? pattern.withinMs;
    const maxPartialMatches = patch.maxPartialMatches ?? pattern.maxPartialMatches;
    // Re-affirming the current values is a no-op: no change, no audit.
    if (withinMs === pattern.withinMs && maxPartialMatches === pattern.maxPartialMatches) {
      return {
        changed: false,
        withinMs,
        oldWithinMs: pattern.withinMs,
        maxPartialMatches,
        oldMaxPartialMatches: pattern.maxPartialMatches,
      };
    }
    if (typeof withinMs !== 'number' || !Number.isFinite(withinMs) || withinMs <= 0) {
      throw new RangeError('withinMs must be a positive finite number of milliseconds');
    }
    if (
      typeof maxPartialMatches !== 'number' ||
      !Number.isInteger(maxPartialMatches) ||
      maxPartialMatches <= 0
    ) {
      throw new RangeError('maxPartialMatches must be a positive integer');
    }
    const oldWithinMs = pattern.withinMs;
    const oldMaxPartialMatches = pattern.maxPartialMatches;
    // Only the window and the bound move; the compiled steps, the key
    // extractor, and the live partials are untouched, and every
    // observation reads the live values from here on.
    pattern.withinMs = withinMs;
    pattern.maxPartialMatches = maxPartialMatches;
    return { changed: true, withinMs, oldWithinMs, maxPartialMatches, oldMaxPartialMatches };
  }

  /** Pattern ids in definition order. */
  patternIds(): string[] {
    return [...this.patterns.keys()];
  }

  /**
   * Observes one admitted publish. Advances per-key partials, completes
   * matches, and lazily expires partials older than `withinMs`.
   *
   * Advance-then-start ordering: a message that matches both a later step
   * and step 0 advances only the pre-existing partials, then starts fresh
   * ones — one message never advances a partial it just created.
   */
  observe(msg: BusMessage): void {
    const now = this.deps.now();
    for (const pattern of this.patterns.values()) {
      this.sweepExpired(pattern, now);
      let key: string;
      try {
        key = pattern.keyOf(msg) ?? '';
      } catch {
        // Error-isolated, like every bus observer callback: a broken
        // key extractor skips the pattern for this message, never the
        // publish path.
        continue;
      }
      const steps = pattern.steps;
      // Advance pre-existing partials waiting for a later step.
      const existing = pattern.partials.get(key);
      if (existing !== undefined) {
        const survivors: PartialMatch[] = [];
        for (const partial of existing) {
          const step = steps[partial.stepIndex];
          if (step === undefined) {
            survivors.push(partial);
            continue;
          }
          if (step.absence) {
            // Negation (EB-174): the awaited event arrived — the absence
            // can never fire. Kill the partial and disarm its deadline.
            if (step.matcher.test(msg.topic)) {
              this.killPartial(pattern, partial);
            } else {
              survivors.push(partial);
            }
            continue;
          }
          if (step.matcher.test(msg.topic)) {
            const events = [...partial.events, this.matchedEvent(msg, now)];
            if (partial.stepIndex === steps.length - 1) {
              this.complete(pattern, partial, events, key, now);
            } else {
              const advanced: PartialMatch = { ...partial, stepIndex: partial.stepIndex + 1, events };
              // The next step may be an absence step — arm its deadline.
              const next = steps[advanced.stepIndex];
              if (next !== undefined && next.absence) this.armAbsence(pattern, advanced);
              survivors.push(advanced);
            }
          } else {
            survivors.push(partial);
          }
        }
        if (survivors.length === 0) {
          pattern.partials.delete(key);
        } else {
          pattern.partials.set(key, survivors);
        }
        pattern.partialCount -= existing.length - survivors.length;
      }
      // Start new partials on step 0 (never an absence step — validated).
      if (steps[0].matcher.test(msg.topic)) {
        this.startPartial(pattern, key, this.matchedEvent(msg, now), now);
      }
    }
  }

  /** Live counters for `getStats().cep`. */
  snapshot(): CepStats {
    let partialMatches = 0;
    let absencePatterns = 0;
    for (const pattern of this.patterns.values()) {
      partialMatches += pattern.partialCount;
      if (pattern.steps.some((s) => s.absence)) absencePatterns += 1;
    }
    return {
      activePatterns: this.patterns.size,
      partialMatches,
      matches: this.totalMatches,
      absencePatterns,
      absenceMatches: this.totalAbsenceMatches,
    };
  }

  /**
   * Arms the absence deadline for a partial waiting at an absence step
   * (EB-174). The window runs `absenceMs` (default: the pattern's
   * `withinMs`) from now, on the bus clock. The timer re-checks the
   * pattern window on fire, so a partial that outlived `withinMs` while
   * the bus was idle is dropped instead of deriving a stale absence.
   */
  private armAbsence(pattern: CompiledPattern, partial: PartialMatch): void {
    const step = pattern.steps[partial.stepIndex];
    const windowMs = step.absenceMs ?? pattern.withinMs;
    partial.absenceTimer = this.setTimer(() => this.onAbsenceDeadline(pattern, partial), windowMs);
  }

  /**
   * Fires when an absence window elapses with no matching event for the
   * key (EB-174). The absence is satisfied: record it and advance the
   * partial — completing the pattern when the absence step was last.
   * A consecutive absence step arms immediately, so `[A, NOT B, NOT C]`
   * chains without another anchor event.
   */
  private onAbsenceDeadline(pattern: CompiledPattern, partial: PartialMatch): void {
    if (partial.resolved) return;
    partial.absenceTimer = undefined;
    const now = this.deps.now();
    if (now - partial.startedAt > pattern.withinMs) {
      // The lazy sweep never ran while the bus was idle: the partial is
      // older than the pattern window, so it dies deriving nothing.
      this.dropPartial(pattern, partial);
      return;
    }
    const step = pattern.steps[partial.stepIndex];
    const windowMs = step.absenceMs ?? pattern.withinMs;
    partial.absences.push({ stepIndex: partial.stepIndex, topic: step.topic, windowMs });
    this.totalAbsenceMatches += 1;
    if (partial.stepIndex === pattern.steps.length - 1) {
      partial.resolved = true;
      this.complete(pattern, partial, partial.events, partial.key, now);
      this.dropPartial(pattern, partial);
    } else {
      partial.stepIndex += 1;
      const next = pattern.steps[partial.stepIndex];
      if (next !== undefined && next.absence) this.armAbsence(pattern, partial);
    }
  }

  /**
   * Kills a partial: disarms its absence deadline (if any) and marks it
   * resolved so a stale timer is a no-op. The caller removes it from the
   * pattern index (observe's survivor list, clear's map drop).
   */
  private killPartial(pattern: CompiledPattern, partial: PartialMatch): void {
    if (partial.absenceTimer !== undefined) {
      this.clearTimer(partial.absenceTimer);
      partial.absenceTimer = undefined;
    }
    partial.resolved = true;
  }

  /** Removes one dead partial from the pattern index. */
  private dropPartial(pattern: CompiledPattern, partial: PartialMatch): void {
    partial.resolved = true;
    const list = pattern.partials.get(partial.key);
    if (list === undefined) return;
    const idx = list.indexOf(partial);
    if (idx !== -1) {
      list.splice(idx, 1);
      pattern.partialCount -= 1;
      if (list.length === 0) pattern.partials.delete(partial.key);
    }
  }

  private matchedEvent(msg: BusMessage, at: number): CepMatchedEvent {
    return {
      topic: msg.topic,
      seq: msg.seq,
      ...(msg.key === undefined ? {} : { key: msg.key }),
      payload: msg.payload,
      at,
    };
  }

  private startPartial(pattern: CompiledPattern, key: string, event: CepMatchedEvent, now: number): void {
    let list = pattern.partials.get(key);
    if (list === undefined) {
      list = [];
      pattern.partials.set(key, list);
    }
    const partial: PartialMatch = {
      key,
      stepIndex: 1,
      events: [event],
      startedAt: now,
      absences: [],
      absenceTimer: undefined,
      resolved: false,
    };
    // The second step may be an absence step — the anchor just arrived,
    // so arm its deadline immediately (EB-174).
    if (pattern.steps[1].absence) this.armAbsence(pattern, partial);
    list.push(partial);
    pattern.partialCount += 1;
    // Memory bound: evict the oldest partial first (FIFO across keys),
    // so a hot key can never starve cold keys' partials out of the bound.
    while (pattern.partialCount > pattern.maxPartialMatches) {
      let oldestKey: string | undefined;
      let oldestAt = Infinity;
      for (const [k, partials] of pattern.partials) {
        if (partials.length > 0 && partials[0].startedAt < oldestAt) {
          oldestAt = partials[0].startedAt;
          oldestKey = k;
        }
      }
      if (oldestKey === undefined) break;
      const victim = pattern.partials.get(oldestKey)!;
      const evicted = victim.shift()!;
      // An evicted partial derives nothing — disarm its deadline (EB-174).
      this.killPartial(pattern, evicted);
      pattern.partialCount -= 1;
      if (victim.length === 0) pattern.partials.delete(oldestKey);
    }
  }

  private sweepExpired(pattern: CompiledPattern, now: number): void {
    for (const [key, partials] of pattern.partials) {
      const live: PartialMatch[] = [];
      for (const p of partials) {
        if (now - p.startedAt <= pattern.withinMs) {
          live.push(p);
        } else {
          // Expired: disarm any pending absence — an expired partial
          // derives nothing (EB-174).
          this.killPartial(pattern, p);
        }
      }
      if (live.length === 0) {
        pattern.partials.delete(key);
      } else if (live.length !== partials.length) {
        pattern.partials.set(key, live);
      }
      pattern.partialCount -= partials.length - live.length;
    }
  }

  private complete(
    pattern: CompiledPattern,
    partial: PartialMatch,
    events: CepMatchedEvent[],
    key: string,
    now: number,
  ): void {
    this.totalMatches += 1;
    const match: SequenceMatchEvent = {
      patternId: pattern.id,
      key,
      events,
      durationMs: now - partial.startedAt,
      at: now,
      // A match that completed through absence steps carries the
      // satisfied conditions; a purely positive match omits the field.
      ...(partial.absences.length === 0 ? {} : { absences: [...partial.absences] }),
    };
    try {
      this.deps.emitMatch(pattern.id, match);
    } catch {
      // The match itself is counted even when emission fails — the
      // pattern fired; the failure is the bus's to report (the emit path
      // is the bus's own publish pipeline).
    }
  }
}
