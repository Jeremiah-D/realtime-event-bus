/**
 * Per-topic sliding-window publish rate table (EB-34).
 *
 * Answers "how fast is this topic publishing right now?" — messages per
 * second over trailing 1s / 1m / 5m windows, load-average style (rate =
 * count-in-window / window-seconds). The bus uses it for real-time signal
 * on publish load: where to tighten `setTopicRateLimit` budgets, where to
 * add subscribers/capacity, which topics are spiking. Exposed in
 * `EventBus.getStats()` as per-topic `rates` and the bus-level
 * `hotTopics` ranking, and in `src/metrics.ts` as the
 * `eventbus_topic_rate_msg_per_sec{topic,window}` gauge series.
 *
 * Hot path (see `EventBus.fanOut`): `sample` is O(1) and allocation-free
 * once the topic's ring exists — a `Map.get`, one indexed store into a
 * preallocated `Float64Array`, and two counter updates. No arrays, no
 * objects, no closures per publish. The first publish on a topic creates
 * its ring (one amortized allocation per topic; topics that never publish
 * allocate nothing).
 *
 * Clock: the bus passes its injected clock (`EventBusOptions.now`) at the
 * sample point, so the table is deterministic in tests.
 *
 * Bounded memory, and the sizing tradeoff: one fixed-capacity ring per
 * topic that has ever published, `capacity` timestamps (8 bytes each) —
 * with the default 60_000 slots that is 480 KiB per topic. The capacity
 * sets how far back the widest window can count exactly: the 5m window
 * needs 300 seconds of events, so it is exact up to 200 msg/s sustained
 * per topic; the 1m window up to 1_000 msg/s; the 1s window up to
 * 60_000 msg/s. Past that the ring wraps and the oldest events evict —
 * the 5m rate then degrades to a lower bound (it can never report more
 * than `capacity / 300` msg/s), while the narrower windows stay exact
 * much longer. The default is deliberately generous for an in-process bus:
 * a topic sustaining >200 msg/s for five straight minutes is either a
 * load test or a runaway publisher — and even then the table still tracks
 * the spike's shape, just with a floored 5m average.
 *
 * Window-edge semantics: a sample counts for a window while
 * `nowMs - t <= windowMs` — a message exactly `windowMs` old still
 * counts, one millisecond older does not.
 */

/**
 * Sliding-window widths in milliseconds, keyed by the label used in
 * `getStats()` and the Prometheus `window` label.
 */
export const RATE_WINDOWS = {
  '1s': 1_000,
  '1m': 60_000,
  '5m': 300_000,
} as const;

/** Labels for the sliding windows, in increasing width. */
export type RateWindowLabel = keyof typeof RATE_WINDOWS;

/** Seconds per window — the denominator of each rate. */
const WINDOW_SECONDS: Record<RateWindowLabel, number> = {
  '1s': 1,
  '1m': 60,
  '5m': 300,
};

/**
 * Default per-topic ring capacity: 60_000 timestamps = 480 KiB per topic
 * that has ever published. See the module header for the sizing tradeoff.
 */
export const DEFAULT_RATE_RING_CAPACITY = 60_000;

/**
 * How many topics `EventBus.getStats()` carries in `hotTopics`, and how
 * many topics get the `eventbus_topic_rate_msg_per_sec` series in
 * `src/metrics.ts`. Small on purpose: the series are the bus's busiest
 * topics, the ones a rate-limit/scaling decision actually needs.
 */
export const HOT_TOPICS_LIMIT = 10;

/**
 * Messages per second over the trailing 1s / 1m / 5m windows,
 * load-average style: `r1m` is the average publish rate over the last
 * minute, not an instantaneous gauge.
 */
export interface TopicRates {
  r1s: number;
  r1m: number;
  r5m: number;
}

/** All-zero rates — the value for a topic that has never published. */
export const ZERO_RATES: TopicRates = { r1s: 0, r1m: 0, r5m: 0 };

/**
 * Fixed-capacity ring buffer of publish timestamps for one topic. Writes
 * are O(1) and allocation-free; reads scan the whole ring (no early exit)
 * so they stay correct even if the injected clock moved non-monotonically.
 */
class RateRing {
  private readonly times: Float64Array;
  /** Next write slot. */
  private head = 0;
  /** Valid entries in the ring, at most `times.length`. */
  private len = 0;

  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError('rate ring capacity must be a positive integer');
    }
    this.times = new Float64Array(capacity);
  }

  /**
   * Records one publish at `nowMs`. O(1): one indexed store, no
   * allocation. Past capacity the oldest sample is overwritten (the
   * tradeoff documented at the top of this module).
   */
  record(nowMs: number): void {
    this.times[this.head] = nowMs;
    this.head += 1;
    if (this.head === this.times.length) this.head = 0;
    if (this.len < this.times.length) this.len += 1;
  }

  /**
   * Counts samples with `nowMs - t <= windowMs`. Full linear scan — slot
   * order is irrelevant for a count, so the wrap needs no special
   * handling, and a backwards clock cannot corrupt an early exit that
   * does not exist.
   */
  countInWindow(nowMs: number, windowMs: number): number {
    const cutoff = nowMs - windowMs;
    let n = 0;
    for (let i = 0; i < this.len; i++) {
      if (this.times[i] >= cutoff) n += 1;
    }
    return n;
  }
}

/** One row of the hot-topics ranking. */
export interface HotTopic {
  /** The concrete topic name (as published, never a pattern). */
  topic: string;
  r1s: number;
  r1m: number;
  r5m: number;
}

/**
 * Per-topic sliding-window publish rate table. Keyed by concrete topic
 * name (as published, never a pattern).
 */
export class PublishRateTable {
  private readonly rings = new Map<string, RateRing>();
  private readonly capacity: number;

  constructor(capacity: number = DEFAULT_RATE_RING_CAPACITY) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError('rate table capacity must be a positive integer');
    }
    this.capacity = capacity;
  }

  /**
   * Samples one accepted publish at `nowMs`. O(1) and allocation-free on
   * the hot path once the topic's ring exists; the first sample for a
   * topic creates its ring (one amortized allocation per topic).
   */
  sample(topic: string, nowMs: number): void {
    let ring = this.rings.get(topic);
    if (ring === undefined) {
      ring = new RateRing(this.capacity);
      this.rings.set(topic, ring);
    }
    ring.record(nowMs);
  }

  /**
   * The topic's rates at `nowMs`. A topic that has never published gets
   * all-zero rates. Allocates one small result object — a snapshot-time
   * cost, never a publish-path cost.
   */
  ratesFor(topic: string, nowMs: number): TopicRates {
    const ring = this.rings.get(topic);
    if (ring === undefined) return { ...ZERO_RATES };
    return {
      r1s: ring.countInWindow(nowMs, RATE_WINDOWS['1s']) / WINDOW_SECONDS['1s'],
      r1m: ring.countInWindow(nowMs, RATE_WINDOWS['1m']) / WINDOW_SECONDS['1m'],
      r5m: ring.countInWindow(nowMs, RATE_WINDOWS['5m']) / WINDOW_SECONDS['5m'],
    };
  }

  /**
   * The hottest topics by 1m publish rate, hottest first — at most
   * `limit` rows. Topics with no publishes in the trailing minute never
   * appear. Ties break on topic name so the ranking is deterministic.
   */
  hotTopics(nowMs: number, limit: number = HOT_TOPICS_LIMIT): HotTopic[] {
    const rows: HotTopic[] = [];
    for (const [topic, ring] of this.rings) {
      const r1m = ring.countInWindow(nowMs, RATE_WINDOWS['1m']) / WINDOW_SECONDS['1m'];
      if (r1m === 0) continue;
      rows.push({
        topic,
        r1s: ring.countInWindow(nowMs, RATE_WINDOWS['1s']) / WINDOW_SECONDS['1s'],
        r1m,
        r5m: ring.countInWindow(nowMs, RATE_WINDOWS['5m']) / WINDOW_SECONDS['5m'],
      });
    }
    rows.sort((a, b) => b.r1m - a.r1m || (a.topic < b.topic ? -1 : a.topic > b.topic ? 1 : 0));
    return rows.slice(0, Math.max(0, limit));
  }

  /** Topics with at least one sample. */
  get topicCount(): number {
    return this.rings.size;
  }
}
