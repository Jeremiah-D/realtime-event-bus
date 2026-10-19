/**
 * Subscriber drain priority (EB-166): per-subscriber drain-visit
 * scheduling weight for the flush loop.
 *
 * The flush loop drains every subscriber once per round. `deliveryPriority`
 * only reorders *which* subscriber is visited first — it never changes how
 * much a visit drains (the shaping / throttling / rate-limit / in-flight /
 * slow-start / credit budgets are untouched), so it composes orthogonally
 * with all of them. Under contention (many backlogged subscribers) a
 * high-priority subscriber's handlers run earlier in the round, which is
 * the whole point: lower end-to-end latency for the traffic you care
 * about. Every subscriber is still visited every round — a low-priority
 * subscriber is served later, never starved.
 *
 * The visit order is deficit round-robin over the subscriber weights.
 * With one drain visit per subscriber per round the DRR provably reduces
 * to weight-priority order (higher weight first, insertion-stable within
 * a class): each round credits every live subscriber with its weight,
 * visits in deficit-descending order, and debits each visited subscriber
 * by its own weight. The deficit map is real state — it keeps the
 * order correct if weights ever change at runtime — but for the
 * single-visit rounds the scheduler runs today the observable order is
 * exactly "higher weight first, ties by subscription order".
 *
 * This is orthogonal to `src/lanes.ts` (EB-122): lanes decide *which
 * message* a subscriber is served next (message-level priority);
 * `deliveryPriority` decides *which subscriber* the flush loop serves
 * next (subscriber-level priority).
 *
 * This module owns the option validation and the pure DRR ordering. The
 * drain-visit loop lives in `EventBus.scheduleFlush` (src/bus.ts); the
 * deficit carryover map is persisted on the bus (`priorityDeficits`) so
 * deficits survive across flush rounds, and entries are dropped on
 * unsubscribe.
 */

/** Named subscriber drain priorities for `SubscribeOptions.deliveryPriority`. */
export type SubscriberPriorityName = 'low' | 'normal' | 'high';

/** The `SubscribeOptions.deliveryPriority` value after validation. */
export interface ResolvedDeliveryPriority {
  /** The DRR drain weight. */
  readonly weight: number;
  /**
   * The observability label: the named priority, or `'custom'` for an
   * explicit integer weight.
   */
  readonly label: 'low' | 'normal' | 'high' | 'custom';
}

/** DRR drain weight of a default (`'normal'`) subscriber. */
export const DEFAULT_DELIVERY_PRIORITY_WEIGHT = 2;

/** DRR drain weights for the named priorities: 4 high : 2 normal : 1 low. */
export const DELIVERY_PRIORITY_WEIGHTS: Record<SubscriberPriorityName, number> = {
  low: 1,
  normal: 2,
  high: 4,
};

/**
 * Fail-fast validation for `SubscribeOptions.deliveryPriority`. Returns
 * `undefined` when the option is absent, so the non-opt-in path pays a
 * single property check per subscribe.
 *
 * Accepts `'low' | 'normal' | 'high'` or a positive integer weight.
 * Anything else throws `RangeError`.
 */
export function resolveDeliveryPriority(
  value: unknown,
  where: string,
): ResolvedDeliveryPriority | undefined {
  if (value === undefined) return undefined;
  if (value === 'low' || value === 'normal' || value === 'high') {
    return { weight: DELIVERY_PRIORITY_WEIGHTS[value], label: value };
  }
  if (typeof value === 'number' && Number.isInteger(value) && value > 0) {
    return { weight: value, label: 'custom' };
  }
  throw new RangeError(
    `${where}: deliveryPriority must be 'low', 'normal', 'high' or a positive integer weight`,
  );
}

/**
 * A drain-visit candidate for the DRR ordering below: the subscriber id
 * (the deficit-map key), its DRR weight, and its insertion index for the
 * stable tie-break.
 */
export interface DrainVisitCandidate {
  readonly id: string;
  readonly weight: number;
  readonly insertionIndex: number;
}

/**
 * Deficit round-robin drain-visit order (EB-166): credits every candidate
 * with its weight, orders by deficit descending (stable by insertion
 * index), then debits each visited candidate by its own weight.
 *
 * `deficits` is the caller's carryover map, persisted across rounds. With
 * one drain visit per candidate per round the DRR reduces to
 * weight-priority order — higher weight first, ties by subscription order
 * (stable) — and every visited candidate's deficit returns to its
 * pre-round value, so deficits stay bounded. A candidate absent from
 * `candidates` keeps its deficit (it rejoins with credit intact); the
 * caller prunes those when their subscription goes away. The DRR framing
 * stays correct if per-visit quanta are ever introduced.
 *
 * Pure except for the `deficits` map. Returns the candidates in visit
 * order; the input array is not mutated.
 */
export function orderDrainVisits(
  candidates: readonly DrainVisitCandidate[],
  deficits: Map<string, number>,
): DrainVisitCandidate[] {
  if (candidates.length === 0) return [];
  for (const candidate of candidates) {
    const weight = Number.isInteger(candidate.weight) && candidate.weight > 0 ? candidate.weight : 1;
    deficits.set(candidate.id, (deficits.get(candidate.id) ?? 0) + weight);
  }
  const ordered = [...candidates].sort((a, b) => {
    const deficitDelta = (deficits.get(b.id) ?? 0) - (deficits.get(a.id) ?? 0);
    if (deficitDelta !== 0) return deficitDelta;
    return a.insertionIndex - b.insertionIndex;
  });
  for (const candidate of ordered) {
    const weight = Number.isInteger(candidate.weight) && candidate.weight > 0 ? candidate.weight : 1;
    deficits.set(candidate.id, (deficits.get(candidate.id) ?? 0) - weight);
  }
  return ordered;
}
