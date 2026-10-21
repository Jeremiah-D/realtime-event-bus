/**
 * Message lineage (EB-184): end-to-end provenance for messages that cross
 * processing boundaries — topic routes, cross-bus forwards, bridge hops,
 * transform middleware, and mirror ingress.
 *
 * A lineage rides `PublishOptions.lineage` and is stamped onto the
 * admitted envelope (`BusMessage.lineage`). Every hop records the topic
 * the message was admitted on and the bus-clock instant of the hop, so an
 * operator can reconstruct the full forwarding chain of any delivered
 * message — which topics it visited, in which order, and when.
 *
 * Lineage is deliberately orthogonal to the two existing correlation
 * mechanisms:
 * - `trace.ts` (EB-45) carries the request-scoped trace id — one trace
 *   spans the whole request, including retries and redeliveries.
 * - `correlationId` (EB-178) is an RPC-scoped passthrough field.
 * Lineage instead answers "which topics did this message pass through",
 * a per-message forwarding history, not a request identifier.
 *
 * Cycle detection runs at admission: a publish whose lineage already
 * contains the target topic *and left it* is rejected outright (the
 * message came back to a topic it already visited — a loop the
 * registration-time checks cannot see, e.g. across independently
 * configured bridge peers), as is a lineage that already reached
 * `maxLineageHops`. The trailing run of the target topic is exempt —
 * those hops are transform stages of the current admission, or a
 * legitimate same-topic re-admission (a bridge/mirror hop onto the same
 * topic name on another bus). Both rejections are zero-side-effect (no
 * sequence number, no durable-log write, no rate-limit budget) and
 * counted in `BusStats.lineageCycleRejections`.
 *
 * This module is runtime-dependency-free.
 */

/** One processing hop in a message's forwarding chain. */
export interface LineageHop {
  /** The topic the message was admitted on at this hop. */
  topic: string;
  /** Bus-clock milliseconds when the hop was recorded. */
  at: number;
}

/**
 * The forwarding history of one message. `originTopic` is the topic of
 * the original publish (before any alias resolution); when the input
 * omits it, admission defaults it to the publish topic.
 * `parentMessageId` carries the origin publish's application message
 * identity when the publisher supplied one.
 */
export interface MessageLineage {
  originTopic: string;
  parentMessageId?: string;
  /** Ordered hops, oldest first. The first hop is the origin admission. */
  hops: LineageHop[];
}

/** Default cap on lineage hops per message (see `EventBusOptions.maxLineageHops`). */
export const DEFAULT_MAX_LINEAGE_HOPS = 32;

/**
 * Sanity cap on inbound hop counts — a peer-supplied lineage longer than
 * this is rejected as malformed, not checked. Keeps a hostile or buggy
 * peer from forcing unbounded work in the cycle scan.
 */
const MAX_INBOUND_HOPS = 4096;

/** Result of the admission-time lineage check. */
export type LineageCheckVerdict =
  /** The publish may proceed; the hop will be appended. */
  | 'ok'
  /** The target topic already appears in `hops` — a forwarding loop. */
  | 'cycle'
  /** `hops` already reached the configured cap. */
  | 'depth';

/**
 * Validates a caller-supplied lineage. Returns a defensive copy (the
 * caller's object and hop array are never aliased), or `undefined` when
 * no lineage was supplied.
 *
 * @throws `TypeError` when `value` is neither an object nor `undefined`.
 * @throws `RangeError` on any malformed field — before anything is
 * mutated, the usual publish-option fail-fast convention.
 */
export function assertValidLineage(value: unknown, caller: string): MessageLineage | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'object' || value === null) {
    throw new TypeError(`${caller}: lineage must be an object`);
  }
  const v = value as Record<string, unknown>;
  const originTopic = v['originTopic'];
  // An empty string means "default to the publish topic at admission" —
  // accepted and normalized to ''.
  if (
    originTopic !== undefined &&
    originTopic !== '' &&
    (typeof originTopic !== 'string' || originTopic.length > 1024)
  ) {
    throw new RangeError(`${caller}: lineage.originTopic must be a non-empty string`);
  }
  const parentMessageId = v['parentMessageId'];
  if (
    parentMessageId !== undefined &&
    (typeof parentMessageId !== 'string' || parentMessageId.length === 0)
  ) {
    throw new RangeError(`${caller}: lineage.parentMessageId must be a non-empty string`);
  }
  const hopsRaw = v['hops'] === undefined ? [] : v['hops'];
  if (!Array.isArray(hopsRaw)) {
    throw new RangeError(`${caller}: lineage.hops must be an array`);
  }
  if (hopsRaw.length > MAX_INBOUND_HOPS) {
    throw new RangeError(
      `${caller}: lineage.hops exceeds the inbound sanity cap of ${MAX_INBOUND_HOPS}`,
    );
  }
  const hops: LineageHop[] = hopsRaw.map((hop: unknown, index: number) => {
    if (typeof hop !== 'object' || hop === null) {
      throw new RangeError(`${caller}: lineage.hops[${index}] must be an object`);
    }
    const h = hop as Record<string, unknown>;
    const topic = h['topic'];
    const at = h['at'];
    if (typeof topic !== 'string' || topic.length === 0 || topic.length > 1024) {
      throw new RangeError(`${caller}: lineage.hops[${index}].topic must be a non-empty string`);
    }
    if (typeof at !== 'number' || !Number.isFinite(at)) {
      throw new RangeError(`${caller}: lineage.hops[${index}].at must be a finite number`);
    }
    return { topic, at };
  });
  return {
    // The bus defaults a missing origin to the publish topic at admission.
    originTopic: (originTopic as string | undefined) ?? '',
    ...(parentMessageId === undefined ? {} : { parentMessageId: parentMessageId as string }),
    hops,
  };
}

/**
 * Validates `EventBusOptions.maxLineageHops`: a positive integer.
 * Returns the resolved cap (default `DEFAULT_MAX_LINEAGE_HOPS`).
 */
export function resolveMaxLineageHops(value: unknown): number {
  if (value === undefined) return DEFAULT_MAX_LINEAGE_HOPS;
  if (!Number.isInteger(value) || (value as number) <= 0) {
    throw new RangeError('maxLineageHops must be a positive integer');
  }
  return value as number;
}

/**
 * Admission-time lineage check: `topic` is the alias-resolved publish
 * topic, `maxHops` the configured cap.
 *
 * A topic repeating in `hops` means the message is looping back to a
 * topic it already visited — the forwarding topologies whose cycles the
 * registration-time checks cannot see (bridge peers, mirrors) terminate
 * here instead of amplifying forever. The trailing run of `topic` is
 * exempt: those hops are transform stages of the current admission (or a
 * same-topic re-admission, e.g. a bridge/mirror hop onto the same topic
 * name on another bus) — a repeat only counts as a cycle when the topic
 * was visited and then *left*. A hop count already at the cap means the
 * chain ran away (including a same-topic republish loop the cycle rule
 * deliberately permits); the message is rejected before the hop that
 * would exceed it.
 */
export function checkLineageCycle(
  lineage: MessageLineage,
  topic: string,
  maxHops: number,
): LineageCheckVerdict {
  // Strip the trailing run of `topic` — transform stages of this
  // admission, not a loop.
  let end = lineage.hops.length;
  while (end > 0 && lineage.hops[end - 1]?.topic === topic) end -= 1;
  for (let i = 0; i < end; i += 1) {
    if (lineage.hops[i]?.topic === topic) return 'cycle';
  }
  if (lineage.hops.length >= maxHops) return 'depth';
  return 'ok';
}

/**
 * Returns a new lineage with `{ topic, at }` appended — the input is
 * never mutated, so a forwarded message's lineage cannot alias its
 * source's. An empty `originTopic` (caller omitted it) is defaulted to
 * the hop's topic: the first hop always names the origin.
 */
export function appendLineageHop(
  lineage: MessageLineage,
  topic: string,
  at: number,
): MessageLineage {
  return {
    originTopic: lineage.originTopic === '' ? topic : lineage.originTopic,
    ...(lineage.parentMessageId === undefined ? {} : { parentMessageId: lineage.parentMessageId }),
    hops: [...lineage.hops, { topic, at }],
  };
}

/**
 * Non-throwing shape check for untrusted (peer-supplied) lineages —
 * bridge envelopes and forward frames validate with this and drop the
 * envelope/frame on failure instead of throwing into the transport.
 */
export function isValidLineageShape(value: unknown): value is MessageLineage {
  try {
    assertValidLineage(value, 'lineage');
    return true;
  } catch {
    return false;
  }
}

/**
 * Bounded rolling sample of lineage hop counts, for the `hops` depth
 * distribution on `BusStats` (`lineageDepthP50` / `lineageDepthP95`).
 * Nearest-rank quantiles over the last 1024 samples; 0 when empty.
 */
export class LineageDepthSampler {
  private readonly samples: number[] = [];
  private cursor = 0;

  add(depth: number): void {
    if (this.samples.length < 1024) {
      this.samples.push(depth);
      return;
    }
    this.samples[this.cursor] = depth;
    this.cursor = (this.cursor + 1) % 1024;
  }

  private quantile(q: number): number {
    if (this.samples.length === 0) return 0;
    const sorted = [...this.samples].sort((a, b) => a - b);
    const rank = Math.min(sorted.length - 1, Math.max(0, Math.ceil(q * sorted.length) - 1));
    return sorted[rank] as number;
  }

  p50(): number {
    return this.quantile(0.5);
  }

  p95(): number {
    return this.quantile(0.95);
  }
}
