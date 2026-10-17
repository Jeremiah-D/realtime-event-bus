/**
 * Topic-level fair scheduling for subscribers (EB-135): deficit
 * round-robin (DRR) across the topics queued for one subscriber, with a
 * quantum of exactly one message.
 *
 * The scheduler is opt-in per subscription (see
 * `SubscribeOptions.topicFairness`): without it the drain keeps its
 * historical FIFO order. When enabled, each drain round walks the live
 * backlog's topics in first-appearance order, crediting every topic with
 * its configured weight and dequeuing one message per unit of deficit —
 * so a `{ hot: 1, cold: 3 }` subscription drains one `hot` message for
 * every three `cold` ones, interleaved at single-message granularity,
 * while each topic's own messages stay in publish order.
 *
 * The scheduler only decides *which* queued messages the round's budget
 * covers — the budget itself is still the minimum of the delivery gates
 * (shaping, rate limit, in-flight cap, slow start, credits), exactly as
 * for the plain drain. Budget-exempt entries (the EB-117 replay bypass)
 * flow freely and never consume budget or deficit.
 *
 * This module owns the option validation and the pure DRR selector. The
 * queue adapters live where the queues live:
 * `BoundedQueue.drainLiveUpToFair` (src/backpressure.ts) and
 * `LaneQueue.drainLiveUpToFair` (src/lanes.ts); the deficit carryover map
 * is persisted on the subscriber (`Subscriber.topicFairness`) so deficits
 * survive across drain rounds.
 */

/**
 * `SubscribeOptions.topicFairness`: opt-in topic-level fair scheduling
 * (EB-135). Pass `true` for fair scheduling with every topic at weight
 * 1, or an options object to tune per-topic weights. Disabled by default.
 */
export interface TopicFairnessOptions {
  /**
   * Per-topic weights, as `{ [topic]: weight }`. A topic absent from the
   * map drains at weight 1. Every weight must be a positive integer —
   * anything else throws `RangeError` from `subscribe`.
   */
  weights?: Record<string, number>;
}

/** Validated `TopicFairnessOptions`: every default resolved. */
export interface ResolvedTopicFairnessOptions {
  /**
   * The drain weight for `topic`: the configured weight, or 1 when the
   * topic has no configured weight.
   */
  weightOf(topic: string): number;
  /** The configured weights, for observability. */
  readonly weights: ReadonlyMap<string, number>;
}

/**
 * Fail-fast validation for `SubscribeOptions.topicFairness`. Returns
 * `undefined` when fairness is disabled (`undefined`/`false`), so the
 * non-opt-in path pays a single property check per drain.
 */
export function resolveTopicFairnessOptions(
  value: boolean | TopicFairnessOptions | undefined,
  where: string,
): ResolvedTopicFairnessOptions | undefined {
  if (value === undefined || value === false) return undefined;
  let weights: Record<string, number> | undefined;
  if (value === true) {
    weights = undefined;
  } else if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
    weights = value.weights;
  } else {
    throw new RangeError(`${where}: topicFairness must be true or an options object`);
  }
  const map = new Map<string, number>();
  if (weights !== undefined) {
    if (weights === null || typeof weights !== 'object' || Array.isArray(weights)) {
      throw new RangeError(
        `${where}: topicFairness.weights must be an object mapping topic names to weights`,
      );
    }
    for (const topic of Object.keys(weights)) {
      const weight = weights[topic];
      if (topic.length === 0) {
        throw new RangeError(`${where}: topicFairness.weights keys must be non-empty topic names`);
      }
      if (!Number.isInteger(weight) || weight <= 0) {
        throw new RangeError(
          `${where}: topicFairness.weights[${JSON.stringify(topic)}] must be a positive integer`,
        );
      }
      map.set(topic, weight);
    }
  }
  return {
    weights: map,
    weightOf: (topic: string) => map.get(topic) ?? 1,
  };
}

/**
 * Deficit round-robin selection (EB-135): picks up to `maxTake` items
 * from per-group FIFO queues in weighted fair order, quantum = 1 item.
 *
 * `items` are the candidates in queue order; `groupOf` maps an item to
 * its group (the topic); `weightOf` gives the group's drain weight.
 * Groups are visited in first-appearance order. Each visit credits the
 * group with its weight, then dequeues while the deficit is at least 1 —
 * one unit of deficit per message — so the long-run service ratio
 * matches the weight ratio at single-message granularity, and every
 * group's own order is preserved.
 *
 * Pure except for the `deficits` carryover map, which the caller owns and
 * persists across rounds: a group that still holds items keeps its
 * leftover deficit for the next round; a group that drained completely
 * has its deficit cleared (classic DRR — a returning group rejoins at
 * zero instead of banking credit while absent). Groups absent from
 * `items` keep whatever deficit they had; the queue adapter prunes those
 * once their backlog is gone.
 *
 * Non-positive or non-integer weights are treated as 1 (the queue
 * adapters only ever pass validated weights; the clamp keeps the pure
 * function total).
 */
export function deficitRoundRobinSelect<T>(
  items: T[],
  groupOf: (item: T) => string,
  weightOf: (group: string) => number,
  deficits: Map<string, number>,
  maxTake: number,
): T[] {
  if (maxTake <= 0 || items.length === 0) return [];
  // Per-group FIFO queues, in first-appearance (rotation) order.
  const order: string[] = [];
  const queues = new Map<string, T[]>();
  for (const item of items) {
    const group = groupOf(item);
    let queue = queues.get(group);
    if (queue === undefined) {
      queue = [];
      queues.set(group, queue);
      order.push(group);
    }
    queue.push(item);
  }
  const selected: T[] = [];
  let remaining = maxTake;
  let progressed = true;
  while (remaining > 0 && progressed) {
    progressed = false;
    for (const group of order) {
      const queue = queues.get(group);
      if (queue === undefined || queue.length === 0) continue;
      let deficit = deficits.get(group) ?? 0;
      const rawWeight = weightOf(group);
      const weight = Number.isInteger(rawWeight) && rawWeight > 0 ? rawWeight : 1;
      deficit += weight;
      while (deficit >= 1 && queue.length > 0 && remaining > 0) {
        const next = queue.shift();
        // `queue.length > 0` guards the shift; the non-null assertion is
        // safe by construction.
        selected.push(next as T);
        deficit -= 1;
        remaining -= 1;
        progressed = true;
      }
      deficits.set(group, deficit);
    }
  }
  // A group that drained completely loses its deficit — it rejoins at
  // zero when new messages arrive.
  for (const group of order) {
    if ((queues.get(group)?.length ?? 0) === 0) deficits.delete(group);
  }
  return selected;
}
