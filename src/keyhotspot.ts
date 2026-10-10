/**
 * Keyed delivery hotspot detection (EB-47).
 *
 * Per-key publish-order enforcement (see `PublishOptions.key`) holds a
 * keyed message in the per-(subscriber, key) reorder buffer while an
 * earlier keySeq has not been fanned out yet. That buffer is normally
 * shallow — a predecessor arrives a moment later and the cascade releases
 * everything. When it is *not* shallow, something upstream is stuck: a
 * delayed schedule whose due time keeps sliding, a predecessor shed by a
 * rate limiter and only lazily skipped, or a producer that stopped
 * publishing a key mid-sequence. The symptom is per-key queue pile-up,
 * Kafka-hot-partition style, and it is invisible to the bus-wide counters:
 * `keyedReorderedMessages` only says the gate engaged, never *where*.
 *
 * Hotspot monitoring is opt-in per subscription and samples the reorder
 * buffer *depth* — the number of messages currently held for one
 * (subscriber, key) ordering stream:
 *
 * - **Threshold alerting**: `onKeyHotspot` fires when a stream's buffer
 *   depth reaches `thresholdDepth`. It fires once per excursion and
 *   re-arms after the depth drops below the threshold, mirroring the
 *   `onLag` / `onBackpressure` latch semantics. The depth is evaluated on
 *   every buffer mutation (growth in `deliverKeyed`, shrink in
 *   `cascadeKeyExpected`), so a crossing is observed synchronously with
 *   the fan-out activity that caused it.
 * - **`getStats().hotKeys`**: the top-`HOT_KEYS_LIMIT` (10) deepest
 *   buffers across all monitored subscriptions, hottest first — the first
 *   place to look when keyed delivery stalls. Only subscriptions with
 *   `keyHotspot` enabled are sampled, so unmonitored keyed traffic adds
 *   no observation cost and no Prometheus series.
 *
 * Non-interference with the ordering gate is by construction: detection
 * only ever reads `buffer.size`. It never adds to, removes from, or
 * reorders the buffer, never touches `expected` / `skipped`, and never
 * holds the bus clock — the single `at` reading comes from the bus's own
 * injected clock. Enabling monitoring changes nothing about delivery
 * order, admission, or backpressure.
 *
 * Semantics worth knowing:
 * - The depth counts messages held *pre-queue*: they have not yet run the
 *   content filter, adaptive throttling, or the subscriber's backpressure
 *   queue. A deep buffer is an ordering-gate problem, not a
 *   slow-consumer problem — compare with `SubscribeOptions.lagMonitor`
 *   for the consumer side.
 * - Cluster-forwarded streams (`hub:<hubEpoch>`) are latched per epoch,
 *   but `hotKeys` reports the user-facing key: two epochs piling up on
 *   the same key produce two alert excursions and the deeper of the two
 *   depths wins the ranking slot.
 * - A throwing `onKeyHotspot` propagates to the caller (the publish call
 *   on the fan-out path) — the same convention as the other subscriber
 *   monitoring callbacks.
 */

/** Tuning for per-(subscriber, key) hotspot monitoring. */
export interface KeyHotspotOptions {
  /**
   * Buffer-depth threshold: `onKeyHotspot` fires when a (subscriber, key)
   * reorder buffer reaches this depth. Must be a positive integer.
   * Default 100.
   */
  thresholdDepth?: number;
  /**
   * Called once per excursion, when a stream's buffer depth reaches
   * `thresholdDepth`; re-arms after the depth drops below the threshold.
   * Absent means no alerting — depths are still reported in
   * `getStats().hotKeys` and `eventbus_key_hotspot_buffer_depth`.
   * Must be a function.
   */
  onKeyHotspot?: (event: KeyHotspotEvent) => void;
}

/** Fired when a (subscriber, key) reorder buffer reaches its threshold. */
export interface KeyHotspotEvent {
  /** The monitored subscription's id. */
  subscriberId: string;
  /** The pattern the subscription was registered with. */
  pattern: string;
  /** The piling-up key. */
  key: string;
  /** The buffer depth that tripped the alert. */
  bufferedDepth: number;
  /** The configured threshold. */
  thresholdDepth: number;
  /** Bus-clock reading of the alert, in milliseconds. */
  at: number;
}

/** One entry of `BusStats.hotKeys`: a piling-up (subscriber, key) stream. */
export interface HotKeyStat {
  /** The monitored subscription's id. */
  subscriberId: string;
  /** The pattern the subscription was registered with. */
  pattern: string;
  /** The piling-up key. */
  key: string;
  /** Messages currently held in the stream's reorder buffer. */
  bufferedDepth: number;
  /** The subscription's configured hotspot threshold. */
  thresholdDepth: number;
}

/** Per-subscriber hotspot monitoring state (see `SubscribeOptions.keyHotspot`). */
export interface SubscriberKeyHotspotState {
  thresholdDepth: number;
  onKeyHotspot?: (event: KeyHotspotEvent) => void;
  /**
   * Epoch-scoped ordering-stream keys (`keyOrderKey`) currently in alert:
   * the excursion latch — set when the alert fires, cleared when the
   * depth drops below the threshold.
   */
  alertedKeys: Set<string>;
}

/** How many of the deepest buffers `BusStats.hotKeys` reports. */
export const HOT_KEYS_LIMIT = 10;

/** Default `KeyHotspotOptions.thresholdDepth`. */
export const DEFAULT_KEY_HOTSPOT_THRESHOLD_DEPTH = 100;

/**
 * Validates `SubscribeOptions.keyHotspot` and builds the initial
 * per-subscriber monitoring state. Returns `undefined` when monitoring is
 * disabled. Throws `RangeError` for a non-positive-integer threshold and
 * `TypeError` for a non-function callback.
 */
export function resolveKeyHotspotOptions(
  opt: boolean | KeyHotspotOptions | undefined,
): SubscriberKeyHotspotState | undefined {
  if (opt == null || opt === false) return undefined;
  if (opt !== true && (typeof opt !== 'object' || opt === null)) {
    throw new TypeError('keyHotspot must be true or a KeyHotspotOptions object');
  }
  const o: KeyHotspotOptions = opt === true ? {} : opt;
  const thresholdDepth = o.thresholdDepth ?? DEFAULT_KEY_HOTSPOT_THRESHOLD_DEPTH;
  if (!Number.isInteger(thresholdDepth) || thresholdDepth <= 0) {
    throw new RangeError('keyHotspot.thresholdDepth must be a positive integer');
  }
  const onKeyHotspot = o.onKeyHotspot;
  if (onKeyHotspot !== undefined && typeof onKeyHotspot !== 'function') {
    throw new TypeError('keyHotspot.onKeyHotspot must be a function');
  }
  return { thresholdDepth, onKeyHotspot, alertedKeys: new Set() };
}
