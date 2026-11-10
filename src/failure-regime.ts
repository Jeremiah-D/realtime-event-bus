/**
 * Online changepoint detection for per-topic delivery failure rates
 * (EB-442).
 *
 * Complements the fixed-threshold alerting of `src/lag.ts` /
 * `src/grouplag.ts` (fixed lag waterlines), `src/ops-alert.ts`
 * (EB-132, fixed SLO alerts), and `src/partitionhealth.ts` (whose
 * `failureRate()` is only a point-in-time reading): a fixed threshold
 * only fires after the failure rate crosses a hard line, while a
 * downstream can degrade *below* that line for a long time — a slow
 * creep from 1% to 8% failures never reaches a 10% line, yet it is
 * exactly the degradation an operator wants to hear about early. This
 * module watches the per-topic delivery-outcome stream
 * (success/failure/timeout) with a Page-Hinkley test and fires
 * `onFailureRateRegimeChange` when the *distribution* shifts — topic,
 * detection time, and before/after failure rates with sample counts.
 *
 * It is orthogonal to the fixed-threshold alerting (thresholds alert
 * on levels, this alerts on shifts), to `src/latency.ts` (which records
 * latency distributions but detects nothing), and to the error-budget
 * quarantine (which counts consecutive subscriber errors — this watches
 * topic-level rate drift). It only alerts: it never pauses, isolates,
 * or reroutes a topic — disposition stays with quarantine and the
 * operator.
 *
 * The outcomes are *reported in* (`EventBus.reportDeliveryOutcome`),
 * like the partition-health signals in `src/partitionhealth.ts`:
 * an in-memory bus cannot observe downstream delivery health itself,
 * so the operator, a monitoring sidecar, or a bridge to an external
 * log is the honest source of the stream. A timeout counts as a
 * failure: an outcome that never completed is a failed delivery for
 * regime purposes.
 *
 * Zero dependencies. The clock is injectable for deterministic tests.
 * Opt-in at the bus level (`EventBusOptions.failureRegime`): without
 * it no monitor exists and reporting throws, so the default bus pays
 * nothing.
 */

/** One delivery outcome on the per-topic result stream. */
export type DeliveryOutcome = 'success' | 'failure' | 'timeout';

/** Direction of a detected failure-rate regime change. */
export type FailureRegimeDirection = 'increase' | 'decrease';

/** Failure-rate snapshot of one regime's samples. */
export interface FailureRateSnapshot {
  /** Fraction of samples that were failures or timeouts. */
  failureRate: number;
  /** Samples in the snapshot (bounded by `windowSamples`). */
  samples: number;
}

/** One detected failure-rate regime change. */
export interface FailureRateRegimeChange {
  /** The topic whose delivery failure rate shifted. */
  topic: string;
  direction: FailureRegimeDirection;
  /** Detection time, from the injected clock. */
  detectedAtMs: number;
  /** Failure-rate regime before the change point. */
  before: FailureRateSnapshot;
  /** Failure-rate regime after the change point. */
  after: FailureRateSnapshot;
}

/** Options for {@link FailureRateRegimeDetector}. */
export interface FailureRegimeOptions {
  /**
   * Minimum failure-rate shift (fraction, e.g. 0.05 = five points)
   * worth detecting. Default: 0.05. Raise this on noisy topics where
   * small sustained shifts are noise.
   */
  delta?: number;
  /**
   * Alarm threshold on the Page-Hinkley cumulative sum. Default: 5.
   * One failure contributes roughly 1 to the sum, so the threshold is
   * also the burst tolerance: a cluster of fewer than `lambda`
   * failures with successes around them never alarms. A larger
   * threshold needs a longer or bigger shift before alarming; a
   * smaller one alarms faster but is more burst-sensitive.
   */
  lambda?: number;
  /**
   * Observations used to estimate the baseline before detection starts.
   * Default: 50. The baseline rate is the *mean* of these samples —
   * for binary outcomes the median can only be 0 or 1 and carries no
   * rate information, so the mean is the honest center here; a single
   * warmup outlier moves it by at most 1/`warmupSamples` (0.02 at the
   * default), which the alarm threshold absorbs without alarming (see
   * the module doc).
   */
  warmupSamples?: number;
  /**
   * Bounded window for the before/after failure-rate snapshots.
   * Default: 50 (must be ≥ 2).
   */
  windowSamples?: number;
  /** Called on every detected regime change. */
  onFailureRateRegimeChange?: (change: FailureRateRegimeChange) => void;
  /** Clock source for `detectedAtMs`; defaults to Date.now. Injectable for tests. */
  nowMs?: () => number;
}

export const DEFAULT_FAILURE_REGIME_DELTA = 0.05;
export const DEFAULT_FAILURE_REGIME_LAMBDA = 5;
export const DEFAULT_FAILURE_REGIME_WARMUP_SAMPLES = 50;
export const DEFAULT_FAILURE_REGIME_WINDOW_SAMPLES = 50;

/** Validates a delivery outcome. Throws `RangeError` on anything else. */
export function assertValidDeliveryOutcome(
  outcome: unknown,
  where: string,
): asserts outcome is DeliveryOutcome {
  if (outcome !== 'success' && outcome !== 'failure' && outcome !== 'timeout') {
    throw new RangeError(`${where}: outcome must be 'success', 'failure' or 'timeout'`);
  }
}

/**
 * Validates `EventBusOptions.failureRegime` fail-fast. Returns
 * `undefined` when the option is absent (detection disabled); throws
 * `TypeError`/`RangeError` on a malformed options object so a bad
 * config can never half-wire a bus.
 */
export function resolveFailureRegimeOptions(
  opt: FailureRegimeOptions | undefined,
): FailureRegimeOptions | undefined {
  if (opt === undefined) return undefined;
  if (typeof opt !== 'object' || opt === null || Array.isArray(opt)) {
    throw new TypeError('failureRegime must be a FailureRegimeOptions object');
  }
  const delta = opt.delta ?? DEFAULT_FAILURE_REGIME_DELTA;
  if (typeof delta !== 'number' || !Number.isFinite(delta) || delta <= 0 || delta >= 1) {
    throw new RangeError('failureRegime.delta must be a number in (0, 1)');
  }
  const lambda = opt.lambda ?? DEFAULT_FAILURE_REGIME_LAMBDA;
  if (typeof lambda !== 'number' || !Number.isFinite(lambda) || lambda <= 0) {
    throw new RangeError('failureRegime.lambda must be a positive number');
  }
  const warmupSamples = opt.warmupSamples ?? DEFAULT_FAILURE_REGIME_WARMUP_SAMPLES;
  if (!Number.isInteger(warmupSamples) || warmupSamples < 1) {
    throw new RangeError('failureRegime.warmupSamples must be an integer >= 1');
  }
  const windowSamples = opt.windowSamples ?? DEFAULT_FAILURE_REGIME_WINDOW_SAMPLES;
  if (!Number.isInteger(windowSamples) || windowSamples < 2) {
    throw new RangeError('failureRegime.windowSamples must be an integer >= 2');
  }
  if (
    opt.onFailureRateRegimeChange !== undefined &&
    typeof opt.onFailureRateRegimeChange !== 'function'
  ) {
    throw new RangeError('failureRegime.onFailureRateRegimeChange must be a function');
  }
  if (opt.nowMs !== undefined && typeof opt.nowMs !== 'function') {
    throw new RangeError('failureRegime.nowMs must be a function');
  }
  return { ...opt };
}

function snapshotOf(values: number[]): FailureRateSnapshot {
  const bad = values.reduce((a, b) => a + b, 0);
  return { failureRate: values.length === 0 ? 0 : bad / values.length, samples: values.length };
}

interface TrackedSample {
  seq: number;
  /** 1 for failure/timeout, 0 for success. */
  value: number;
}

/**
 * Page-Hinkley changepoint detector for one topic's delivery-outcome
 * stream.
 *
 * Formulation (one-sided, applied symmetrically to both directions):
 * with a baseline rate μ (the failure rate), the increase statistic is
 *   S⁺ₙ = max(0, S⁺ₙ₋₁ + (xₙ − μ − δ/2))
 * and the decrease statistic mirrors it with (μ − xₙ − δ/2), where
 * xₙ is 1 for a failure/timeout and 0 for a success. An alarm fires
 * when either exceeds λ. The baseline is the *mean* of a warmup
 * window and then stays fixed until an alarm — deliberately not a
 * running average: a running mean would chase a slow creep and absorb
 * exactly the shift this detector exists to catch. For binary outcomes
 * the median can only be 0 or 1 and carries no rate information, so
 * the mean is the honest warmup center; a single warmup outlier moves
 * it by at most 1/warmupSamples (0.02 at the default 50), which the
 * alarm threshold absorbs without alarming.
 *
 * The two sums track their own runs: a run is the stretch since the
 * corresponding sum last hit zero, so the before/after split of an
 * alarm is the run that actually produced it — the increase and
 * decrease sums blip independently during a steady regime and must
 * not share one run.
 *
 * After an alarm the baseline is re-estimated from a fresh window,
 * not from the after-window: the after-window at alarm time is short
 * (a dozen samples) and its rate is noisy — re-baselining straight
 * onto it can overshoot the true new rate by more than δ/2 and
 * false-alarm the opposite direction on the steady new regime. So
 * the detector *settles*: detection pauses for the next
 * `windowSamples` post-alarm samples while their failure rate
 * confirms the new baseline, then detection resumes against it. A
 * further shift inside the settling window is still detected — after
 * settling, against the confirmed baseline — though its reported
 * before-rate may blend the two regimes, since the settling window
 * straddled them. A steady new regime never re-alarms.
 *
 * Residual limitation, stated plainly: the settled rate is a
 * 50-sample estimate and carries binomial noise (at a 50% rate the
 * standard error is ~0.07, wider than δ/2). If it overshoots the
 * true new rate by more than δ/2, the steady new regime accumulates
 * the opposite sum and fires one spurious opposite-direction alarm,
 * after which it re-settles onto the correct rate. The spurious
 * alarm is self-limiting and its snapshots expose it (before and
 * after rates within a few points of each other), but on a noisy
 * high-rate topic it can happen. Raising `windowSamples` shrinks the
 * noise at the cost of a longer settling pause.
 *
 * Memory is bounded: at most `2 * windowSamples` recent samples are kept.
 * Honest limitation: the fixed baseline cannot tell a legitimate
 * gradual drift from a creep — a failure rate inching up 0.1pp per
 * week forever eventually alarms once the cumulative shift passes
 * δ/2. That is the intended trade-off (a slow creep is the signal,
 * not the noise), but a topic whose "normal" genuinely drifts
 * seasonally will alarm on the drift. A second one: with a zero
 * baseline a lone failure contributes ~1 to the sum, so `lambda` is
 * the burst tolerance — keep it above the largest failure burst the
 * topic legitimately produces.
 */
export class FailureRateRegimeDetector {
  private readonly topic: string;
  private readonly delta: number;
  private readonly lambda: number;
  private readonly warmupSamples: number;
  private readonly windowSamples: number;
  /**
   * The regime-change alert callback. Deliberately *not* readonly
   * (EB-445): `FailureRateRegimeMonitor.setOnChange` redirects it
   * live, so an already-built detector hears about the swap without
   * being rebuilt — rebuilding would lose its baseline, warmup, and
   * settling state. The detector captures the callback at
   * construction, so swapping only the monitor's stored options
   * would leave already-built detectors on the old callback (the
   * WR-372 tracker-capture trap); the fan-out below is the fix.
   */
  private onChange?: (change: FailureRateRegimeChange) => void;
  private readonly nowMs: () => number;

  private seq = 0;
  /** Samples seen during warmup — the baseline is fixed after this. */
  private n = 0;
  private mean = 0;
  private sumInc = 0;
  private sumDec = 0;
  /** seq at which the current increase run began (undefined while S⁺ is 0). */
  private incRunStartSeq: number | undefined;
  /** seq at which the current decrease run began (undefined while S⁻ is 0). */
  private decRunStartSeq: number | undefined;
  /** Post-alarm settling: confirming the new baseline over a full window. */
  private settling = false;
  private settlingCount = 0;
  private settlingBad = 0;
  private samples: TrackedSample[] = [];
  private warmup: number[] = [];

  constructor(topic: string, options: FailureRegimeOptions = {}) {
    if (typeof topic !== 'string' || topic === '') {
      throw new RangeError('FailureRateRegimeDetector: topic must be a non-empty string');
    }
    const resolved = resolveFailureRegimeOptions(options);
    // resolveFailureRegimeOptions never returns undefined here: the
    // argument is defaulted to {} above, so only its validation runs.
    const opts = resolved as FailureRegimeOptions;
    this.topic = topic;
    this.delta = opts.delta ?? DEFAULT_FAILURE_REGIME_DELTA;
    this.lambda = opts.lambda ?? DEFAULT_FAILURE_REGIME_LAMBDA;
    this.warmupSamples = opts.warmupSamples ?? DEFAULT_FAILURE_REGIME_WARMUP_SAMPLES;
    this.windowSamples = opts.windowSamples ?? DEFAULT_FAILURE_REGIME_WINDOW_SAMPLES;
    this.onChange = opts.onFailureRateRegimeChange;
    this.nowMs = opts.nowMs ?? Date.now;
  }

  /**
   * Observe one delivery outcome for this topic. Returns the detected
   * {@link FailureRateRegimeChange} when this outcome triggers an
   * alarm, otherwise `undefined`. The optional
   * `onFailureRateRegimeChange` callback fires on the same event.
   * A timeout counts as a failure.
   */
  observe(outcome: DeliveryOutcome): FailureRateRegimeChange | undefined {
    assertValidDeliveryOutcome(outcome, 'FailureRateRegimeDetector.observe');
    const x = outcome === 'success' ? 0 : 1;
    this.seq += 1;
    const sample: TrackedSample = { seq: this.seq, value: x };
    this.samples.push(sample);
    const cap = this.windowSamples * 2;
    if (this.samples.length > cap) {
      this.samples.splice(0, this.samples.length - cap);
    }

    if (this.n < this.warmupSamples) {
      // Baseline estimation: no detection yet.
      this.warmup.push(x);
      this.n += 1;
      if (this.n === this.warmupSamples) {
        // Mean baseline rate. For binary outcomes the median can only
        // be 0 or 1, so the mean is the honest center; a single warmup
        // outlier moves it by at most 1/warmupSamples.
        const bad = this.warmup.reduce((a, b) => a + b, 0);
        this.mean = bad / this.warmup.length;
        this.warmup = [];
      }
      return undefined;
    }

    if (this.settling) {
      // Settling: confirm the new baseline over a full window before
      // detecting again. Detection is paused — the after-window rate
      // at alarm time is too noisy to detect against, and a run that
      // starts on its overshoot would mix the old and new regimes in
      // the next alarm's snapshots.
      this.settlingCount += 1;
      this.settlingBad += x;
      if (this.settlingCount >= this.windowSamples) {
        this.mean = this.settlingBad / this.settlingCount;
        this.settling = false;
        this.settlingCount = 0;
        this.settlingBad = 0;
        this.sumInc = 0;
        this.sumDec = 0;
        this.incRunStartSeq = undefined;
        this.decRunStartSeq = undefined;
      }
      return undefined;
    }

    // The baseline stays fixed between alarms: a running average would
    // chase a slow creep and absorb exactly the shift this detector
    // exists to catch.
    const half = this.delta / 2;
    this.sumInc = Math.max(0, this.sumInc + (x - this.mean - half));
    this.sumDec = Math.max(0, this.sumDec + (this.mean - x - half));

    // Each direction tracks its own run: the sums blip independently
    // during a steady regime, so a shared run would drag samples from
    // the wrong direction into the before/after split.
    if (this.sumInc > 0) {
      this.incRunStartSeq ??= this.seq;
    } else {
      this.incRunStartSeq = undefined;
    }
    if (this.sumDec > 0) {
      this.decRunStartSeq ??= this.seq;
    } else {
      this.decRunStartSeq = undefined;
    }

    let direction: FailureRegimeDirection | undefined;
    if (this.sumInc > this.lambda) {
      direction = 'increase';
    } else if (this.sumDec > this.lambda) {
      direction = 'decrease';
    }
    if (direction === undefined) {
      return undefined;
    }

    const change = this.buildChange(direction);

    // Restart against the new regime: pause detection while a full
    // window of post-alarm samples confirms the new baseline (see
    // the settling branch above). The mean is left stale meanwhile
    // and replaced when settling completes.
    this.sumInc = 0;
    this.sumDec = 0;
    this.incRunStartSeq = undefined;
    this.decRunStartSeq = undefined;
    this.settling = true;
    this.settlingCount = 0;
    this.settlingBad = 0;
    this.samples = this.samples.filter((s) => s.seq >= change.afterSeqStart);

    // A throwing callback is swallowed here: the callback is an
    // observer, not part of the detection path, so a broken observer
    // never breaks the outcome stream or the reporting caller
    // (EB-445, along the `fireRebalanceListener` / `emitRebalance`
    // family rule). The change is still returned to the caller.
    try {
      this.onChange?.(change.event);
    } catch {
      // Observation never breaks detection.
    }
    return change.event;
  }

  /**
   * Live-redirects the regime-change alert callback (EB-445): the
   * next alarm fires the new callback; the baseline, warmup, and
   * settling state are untouched. `undefined` clears back to
   * audit-only — detection keeps running, just with no callback
   * fired. Called by `FailureRateRegimeMonitor.setOnChange`, which
   * fans the swap out to every already-built detector.
   */
  setOnChange(cb: ((change: FailureRateRegimeChange) => void) | undefined): void {
    this.onChange = cb;
  }

  private buildChange(direction: FailureRegimeDirection): {
    event: FailureRateRegimeChange;
    afterSeqStart: number;
  } {
    const runStartSeq =
      direction === 'increase' ? this.incRunStartSeq : this.decRunStartSeq;
    // The alarming sum is above zero, so its run start is always set.
    const start = runStartSeq ?? this.seq;
    const recent = this.samples.slice(-this.windowSamples * 2);
    let beforeVals = recent
      .filter((s) => s.seq < start)
      .slice(-this.windowSamples)
      .map((s) => s.value);
    let afterVals = recent
      .filter((s) => s.seq >= start)
      .slice(-this.windowSamples)
      .map((s) => s.value);
    if (afterVals.length === 0) {
      // Cannot happen in practice (the alarming sample is always in the
      // run), but never build a snapshot from nothing.
      afterVals = [recent[recent.length - 1].value];
    }
    if (beforeVals.length === 0) {
      // The run outgrew the retained window: fall back to the oldest
      // retained samples — the best available old-regime approximation.
      beforeVals = recent.slice(0, this.windowSamples).map((s) => s.value);
    }
    const afterSeqStart = recent.find((s) => s.seq >= start)?.seq ?? start;
    const event: FailureRateRegimeChange = {
      topic: this.topic,
      direction,
      detectedAtMs: this.nowMs(),
      before: snapshotOf(beforeVals),
      after: snapshotOf(afterVals),
    };
    return { event, afterSeqStart };
  }
}

/**
 * Per-topic registry of failure-rate regime detectors. One call site
 * feeds delivery outcomes; each topic gets an isolated detector, so a
 * regime change on one topic never alarms another. Topics that never
 * receive an outcome never get a detector — an idle topic cannot alarm.
 */
export class FailureRateRegimeMonitor {
  private readonly detectors = new Map<string, FailureRateRegimeDetector>();
  private readonly options: FailureRegimeOptions;

  constructor(options: FailureRegimeOptions = {}) {
    this.options = options;
  }

  /**
   * Observe one delivery outcome for `topic`. Returns the detected
   * change, if any. Lazily creates the topic's detector on first use.
   */
  observe(topic: string, outcome: DeliveryOutcome): FailureRateRegimeChange | undefined {
    let d = this.detectors.get(topic);
    if (d === undefined) {
      d = new FailureRateRegimeDetector(topic, this.options);
      this.detectors.set(topic, d);
    }
    return d.observe(outcome);
  }

  /**
   * Live-redirects the regime-change alert callback (EB-445) for
   * every detector — already-built and later ones. The callback is
   * captured per-detector at construction, so swapping only the
   * monitor's stored options would leave already-built detectors on
   * the old callback (the WR-372 tracker-capture trap): this stores
   * the swap for detectors built later and fans it out to every
   * live detector now. Baselines, warmup, and settling state are
   * untouched on every detector; `undefined` clears back to
   * audit-only — detection keeps running, just with no callback
   * fired.
   */
  setOnChange(cb: ((change: FailureRateRegimeChange) => void) | undefined): void {
    this.options.onFailureRateRegimeChange = cb;
    for (const detector of this.detectors.values()) {
      detector.setOnChange(cb);
    }
  }

  /** The currently installed alert callback, if any. */
  getOnChange(): ((change: FailureRateRegimeChange) => void) | undefined {
    return this.options.onFailureRateRegimeChange;
  }

  /** Topics with a live detector. */
  topics(): string[] {
    return [...this.detectors.keys()];
  }
}
