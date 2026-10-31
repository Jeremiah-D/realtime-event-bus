/**
 * Deterministic fault-injection harness (EB-103).
 *
 * `FaultInjector` drives scripted failures into the bus so tests and
 * pre-production drills can deterministically reproduce the failure
 * paths that are otherwise timing-dependent:
 * - degraded subscribers (EB-18 health probe) via `{ type: 'error', target: 'handler' }`,
 * - dead-lettering (EB-28) via the same fault on a reliable subscription,
 * - backpressure (queue saturation, `onBackpressure`, drop accounting)
 *   via `{ type: 'stall', target: 'drain' }`.
 *
 * Every scenario carries its own seeded RNG stream (mulberry32): the same
 * seed evaluated in the same call order yields the identical fault
 * sequence, so a failing drill is reproducible bit for bit. `rate` is the
 * per-evaluation injection probability in `[0, 1]`; `1` always fires and
 * `0` never fires, which is how most tests pin behavior.
 *
 * Supported (type, target) pairs — anything else is rejected at
 * construction with `RangeError` (fail fast, never silently ignore):
 * - `admission`: `'error'` (the publish throws `ChaosInjectionError`),
 *   `'drop'` (the publish is shed: it returns its zero value and nothing
 *   is fanned out, sequenced, or logged).
 * - `drain`: `'stall'` (the drain round is skipped; the queue keeps
 *   accumulating under the normal backpressure policy), `'drop'` (the
 *   subscriber's live queue is discarded — the loss stays visible as a
 *   sequence gap), `'latency'` (the drain round is deferred by
 *   `latencyMs`; re-armed through the bus so it can never livelock).
 * - `handler`: `'error'` (the handler invocation throws
 *   `ChaosInjectionError` instead of running user code — the health
 *   probe, the reliable-delivery envelope, and the batch wrappers all
 *   observe it exactly like a real throwing handler), `'drop'` (user
 *   code never runs; the message is consumed as delivered).
 *
 * When no scenarios are configured the bus holds no injector and every
 * hook site is a single `undefined` check — zero behavioral difference,
 * zero overhead. Chaos never touches shadow subscribers, migrating
 * (EB-82 export-paused) subscriptions, or durable-log replay paths: it
 * faults live delivery only.
 *
 * TEST / PRE-PRODUCTION USE ONLY. Never enable on a bus carrying real
 * traffic: injected faults deliberately violate delivery guarantees.
 */

export type ChaosFaultType = 'latency' | 'error' | 'drop' | 'stall';

export type ChaosTarget = 'handler' | 'admission' | 'drain';

export interface ChaosScenario {
  /** What goes wrong when the fault fires. */
  type: ChaosFaultType;
  /** Where the fault is injected. */
  target: ChaosTarget;
  /**
   * Per-evaluation injection probability in `[0, 1]`. `1` fires on every
   * evaluation, `0` never fires.
   */
  rate: number;
  /**
   * Seed for this scenario's private RNG stream. Same seed + same
   * evaluation order = identical fault sequence. Defaults to a fixed
   * constant, so drills are deterministic unless the caller opts out.
   */
  seed?: number;
  /**
   * Required for `'latency'` faults: the drain deferral in milliseconds
   * (must be a positive finite number). Must be absent for every other
   * fault type.
   */
  latencyMs?: number;
}

export interface ChaosOptions {
  /** Fault scenarios, evaluated in registration order per target. Empty (or absent) disables chaos entirely. */
  scenarios?: ChaosScenario[];
}

/** Injection counters, exposed on `BusStats.chaos`. */
export interface ChaosStats {
  /** Total faults injected across all targets and types. */
  injected: number;
  /** Faults injected per target. */
  byTarget: Record<ChaosTarget, number>;
}

/**
 * Thrown by `'error'` faults at the `admission` and `handler` targets.
 * A distinct class so drills (and operators) can tell an injected fault
 * apart from a genuine application error.
 */
export class ChaosInjectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ChaosInjectionError';
  }
}

const FAULT_TYPES: readonly ChaosFaultType[] = ['latency', 'error', 'drop', 'stall'];
const TARGETS: readonly ChaosTarget[] = ['handler', 'admission', 'drain'];

/** Which fault types are meaningful at each injection target. */
const TARGET_FAULT_TYPES: Record<ChaosTarget, readonly ChaosFaultType[]> = {
  // A publish is synchronous: it can fail loudly or shed silently, but
  // it cannot be delayed or stalled.
  admission: ['error', 'drop'],
  // A drain round can be slowed, skipped, or have its backlog discarded;
  // raising from inside the flush loop would corrupt iteration state, so
  // 'error' is not supported here — fault the handler instead.
  drain: ['latency', 'drop', 'stall'],
  // User code can throw or be skipped; delaying a synchronous handler
  // without rewriting the drain loop is impossible, so 'latency'/'stall'
  // are not supported here — fault the drain instead.
  handler: ['error', 'drop'],
};

const DEFAULT_SEED = 0x9e3779b9;

/**
 * Validates one scenario, fail-fast. `where` names the option for error
 * messages (e.g. `EventBus.chaos.scenarios[2]`).
 */
export function assertValidChaosScenario(scenario: ChaosScenario, where: string): void {
  if (typeof scenario !== 'object' || scenario === null) {
    throw new RangeError(`${where}: chaos scenario must be an object`);
  }
  const { type, target, rate, seed, latencyMs } = scenario;
  if (!FAULT_TYPES.includes(type)) {
    throw new RangeError(
      `${where}: type must be one of ${FAULT_TYPES.join(', ')}`,
    );
  }
  if (!TARGETS.includes(target)) {
    throw new RangeError(
      `${where}: target must be one of ${TARGETS.join(', ')}`,
    );
  }
  if (!TARGET_FAULT_TYPES[target].includes(type)) {
    throw new RangeError(
      `${where}: type '${type}' is not supported at target '${target}' ` +
        `(supported: ${TARGET_FAULT_TYPES[target].join(', ')})`,
    );
  }
  if (typeof rate !== 'number' || !Number.isFinite(rate) || rate < 0 || rate > 1) {
    throw new RangeError(`${where}: rate must be a finite number in [0, 1]`);
  }
  if (seed !== undefined && (typeof seed !== 'number' || !Number.isFinite(seed))) {
    throw new RangeError(`${where}: seed must be a finite number`);
  }
  if (type === 'latency') {
    if (latencyMs === undefined || typeof latencyMs !== 'number' || !Number.isFinite(latencyMs) || latencyMs <= 0) {
      throw new RangeError(`${where}: latencyMs must be a positive finite number for 'latency' faults`);
    }
  } else if (latencyMs !== undefined) {
    throw new RangeError(`${where}: latencyMs is only meaningful for 'latency' faults`);
  }
}

/** Deterministic 32-bit RNG (mulberry32). Zero dependencies. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

interface ResolvedScenario {
  type: ChaosFaultType;
  target: ChaosTarget;
  rate: number;
  latencyMs?: number;
  rng: () => number;
  /** The caller's scenario as given (copied) — for `current()` and revision comparison. */
  source: ChaosScenario;
}

/**
 * Canonical (fully defaulted) form of a chaos scenario, for revision
 * comparison and audit detail (EB-300): an omitted `seed` is the
 * default seed, so two scenarios that differ only by spelling the
 * default seed compare equal.
 */
export interface CanonicalChaosScenario {
  type: ChaosFaultType;
  target: ChaosTarget;
  rate: number;
  seed: number;
  latencyMs?: number;
}

/** Defaults one scenario: fills the seed, drops nothing else. */
export function canonicalChaosScenario(scenario: ChaosScenario): CanonicalChaosScenario {
  const out: CanonicalChaosScenario = {
    type: scenario.type,
    target: scenario.target,
    rate: scenario.rate,
    seed: scenario.seed ?? DEFAULT_SEED,
  };
  if (scenario.latencyMs !== undefined) out.latencyMs = scenario.latencyMs;
  return out;
}

/**
 * Whether two scenario lists are the same revision (EB-300): same
 * length, same order, same canonical form per scenario. Callers must
 * have validated both lists through `assertValidChaosScenario` first.
 */
export function chaosScenariosEqual(
  a: readonly ChaosScenario[],
  b: readonly ChaosScenario[],
): boolean {
  if (a.length !== b.length) return false;
  return a.every((s, i) => {
    const x = canonicalChaosScenario(s);
    const y = canonicalChaosScenario(b[i]);
    return (
      x.type === y.type &&
      x.target === y.target &&
      x.rate === y.rate &&
      x.seed === y.seed &&
      x.latencyMs === y.latencyMs
    );
  });
}

/** Resolves scenarios into evaluation state: one private RNG stream per scenario. */
function resolveScenarios(scenarios: ChaosScenario[]): ResolvedScenario[] {
  return scenarios.map((s) => ({
    type: s.type,
    target: s.target,
    rate: s.rate,
    latencyMs: s.latencyMs,
    rng: mulberry32(s.seed ?? DEFAULT_SEED),
    source: { ...s },
  }));
}

export interface ChaosFault {
  type: ChaosFaultType;
  latencyMs?: number;
}

/**
 * Evaluates chaos scenarios against injection targets. Each scenario owns
 * a private seeded RNG stream, so the fault sequence is a pure function
 * of (seed, evaluation order) — deterministic across runs.
 */
export class FaultInjector {
  // Live-tuned by `retune` (EB-300): the whole scenario list is
  // replaced, each scenario with a fresh private RNG stream from its
  // seed, so the fault sequence restarts deterministically from the new
  // seeds. The injection counters are retained, not reset.
  private scenarios: ResolvedScenario[];
  private injected = 0;
  private readonly byTarget: Record<ChaosTarget, number> = {
    handler: 0,
    admission: 0,
    drain: 0,
  };

  constructor(scenarios: ChaosScenario[]) {
    this.scenarios = resolveScenarios(scenarios);
  }

  /** The live scenario list, for bus-level revision (EB-300). Copies — mutating them is safe. */
  current(): ChaosScenario[] {
    return this.scenarios.map((s) => ({ ...s.source }));
  }

  /**
   * Replaces the scenario list live (EB-300). The caller must have
   * validated `scenarios` through `assertValidChaosScenario` first —
   * this method applies unconditionally. Each scenario gets a fresh
   * private RNG stream from its seed, so the fault sequence restarts
   * deterministically from the new seeds; the injection counters are
   * retained, not reset. An empty list disables injection (`faultFor`
   * always returns `null`) while keeping the injector — and its
   * counters — alive, so a later revision can re-arm it without
   * rebuilding the bus. An already-triggered in-flight fault is
   * unaffected: the retune only changes what the next `faultFor`
   * evaluation sees.
   */
  retune(scenarios: ChaosScenario[]): void {
    this.scenarios = resolveScenarios(scenarios);
  }

  /** True when at least one scenario targets `target`. Lets call sites skip evaluation entirely. */
  hasTarget(target: ChaosTarget): boolean {
    return this.scenarios.some((s) => s.target === target);
  }

  /**
   * Evaluates the scenarios registered for `target`, in registration
   * order; the first scenario whose draw falls below its `rate` fires.
   * Returns the fault, or `null` when nothing fired. One RNG draw per
   * evaluated scenario, in order — the stream stays deterministic.
   */
  faultFor(target: ChaosTarget): ChaosFault | null {
    for (const s of this.scenarios) {
      if (s.target !== target) continue;
      if (s.rng() < s.rate) {
        this.injected += 1;
        this.byTarget[target] += 1;
        return { type: s.type, latencyMs: s.latencyMs };
      }
    }
    return null;
  }

  /**
   * Runs one handler invocation under `handler`-target faults: an
   * `'error'` fault throws `ChaosInjectionError` instead of invoking, a
   * `'drop'` fault skips the invocation and reports it as delivered
   * (returns `undefined`). Zero-allocation fast path when nothing fires.
   */
  invokeHandler<T>(invoke: () => T): T | undefined {
    const fault = this.faultFor('handler');
    if (fault === null) return invoke();
    if (fault.type === 'error') {
      throw new ChaosInjectionError('chaos: injected handler fault');
    }
    return undefined;
  }

  /** Injection counters for `BusStats.chaos`. */
  snapshot(): ChaosStats {
    return { injected: this.injected, byTarget: { ...this.byTarget } };
  }
}

/**
 * Validates `EventBusOptions.chaos` and builds the injector, or returns
 * `undefined` when chaos is absent/empty — the bus then holds no injector
 * and every hook site is a single `undefined` check.
 */
export function resolveChaosOptions(
  options: ChaosOptions | undefined,
  where: string,
): FaultInjector | undefined {
  if (options === undefined) return undefined;
  if (typeof options !== 'object' || options === null) {
    throw new RangeError(`${where}: chaos must be an object`);
  }
  const scenarios = options.scenarios ?? [];
  if (!Array.isArray(scenarios)) {
    throw new RangeError(`${where}: chaos.scenarios must be an array`);
  }
  scenarios.forEach((s, i) => assertValidChaosScenario(s, `${where}.chaos.scenarios[${i}]`));
  if (scenarios.length === 0) return undefined;
  return new FaultInjector(scenarios);
}
