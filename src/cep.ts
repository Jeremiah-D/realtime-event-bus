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
}

/** Live counters for the CEP surface (`getStats().cep`). */
export interface CepStats {
  /** Patterns currently defined. */
  activePatterns: number;
  /** Partial matches alive across all patterns. */
  partialMatches: number;
  /** Match events emitted since the engine was created. */
  matches: number;
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
}

interface CompiledPattern {
  id: string;
  steps: Array<{ topic: string; matcher: RegExp }>;
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
  for (const step of opts.steps) {
    if (step === null || typeof step !== 'object' || typeof step.topic !== 'string' || step.topic.length === 0) {
      throw new RangeError('each sequence step needs a non-empty topic pattern');
    }
    // Compile eagerly so a malformed pattern fails at define time, not at
    // match time. `compilePattern` never throws for ordinary patterns;
    // the call still pins the compiler's interpretation up front.
    compile(step.topic);
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
  private readonly patterns = new Map<string, CompiledPattern>();
  private idCounter = 0;
  private totalMatches = 0;

  constructor(deps: CepEngineDeps) {
    this.deps = deps;
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
      steps: opts.steps.map((step) => ({ topic: step.topic, matcher: this.deps.compile(step.topic) })),
      withinMs: opts.withinMs,
      keyOf: opts.keyOf ?? ((msg) => msg.key ?? ''),
      maxPartialMatches: opts.maxPartialMatches ?? DEFAULT_MAX_PARTIAL_MATCHES,
      partials: new Map(),
      partialCount: 0,
    });
    return id;
  }

  /** Removes a pattern and drops its partials; returns whether one existed. */
  clear(patternId: string): boolean {
    return this.patterns.delete(patternId);
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
          if (step !== undefined && step.matcher.test(msg.topic)) {
            const events = [...partial.events, this.matchedEvent(msg, now)];
            if (partial.stepIndex === steps.length - 1) {
              this.complete(pattern, partial, events, key, now);
            } else {
              survivors.push({ ...partial, stepIndex: partial.stepIndex + 1, events });
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
      // Start new partials on step 0.
      if (steps[0].matcher.test(msg.topic)) {
        this.startPartial(pattern, key, this.matchedEvent(msg, now), now);
      }
    }
  }

  /** Live counters for `getStats().cep`. */
  snapshot(): CepStats {
    let partialMatches = 0;
    for (const pattern of this.patterns.values()) partialMatches += pattern.partialCount;
    return { activePatterns: this.patterns.size, partialMatches, matches: this.totalMatches };
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
    list.push({ key, stepIndex: 1, events: [event], startedAt: now });
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
      victim.shift();
      pattern.partialCount -= 1;
      if (victim.length === 0) pattern.partials.delete(oldestKey);
    }
  }

  private sweepExpired(pattern: CompiledPattern, now: number): void {
    for (const [key, partials] of pattern.partials) {
      const live = partials.filter((p) => now - p.startedAt <= pattern.withinMs);
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
