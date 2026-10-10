/**
 * Cross-process fan-out bridge (EB-51).
 *
 * The cluster link (see `src/cluster.ts`, EB-37) federates buses through a
 * dedicated TCP hub with hub-assigned global sequence numbers. The bridge
 * is the generic alternative: a pluggable `BridgeTransport` that mirrors
 * every admitted local publish to an external broker — Redis Streams, NATS,
 * a message queue, a second process over a socket — with the core staying
 * zero-dependency. The adapter is implemented by the caller; this module
 * only defines the contract, the envelope validation, and the option
 * resolution.
 *
 * Direction of travel:
 * - Outbound: after a local publish clears admission (alias resolution,
 *   ACL, schema validation, rate-limit budget), the bus hands a
 *   `BridgeEnvelope` carrying the RAW application payload to
 *   `transport.publish`. Compression is never mirrored: the receiving node
 *   applies its own compression, TTL, and durable-log rules. A mirror
 *   failure (throw or rejected promise) is swallowed — the bus keeps
 *   serving locally, exactly like the cluster forward path.
 * - Inbound: the transport hands remote envelopes to
 *   `EventBus.receiveFromBridge`. They go through the SAME admission the
 *   local publishes go through (alias resolution, ACL, schema validation,
 *   per-topic rate-limit budget, TTL at drain), get node-local sequence
 *   numbers, and are never mirrored back — that is what keeps a
 *   multi-node bridge loop-free.
 *
 * Identity passthrough: `key` (with its publish-order `keySeq`, so
 * per-key ordering survives the hop when each key has a single publishing
 * node), `messageId`, and the end-to-end `traceId` (32 lowercase hex, the
 * same format `src/trace.ts` uses) ride the envelope. The source TTL
 * deadline rides along verbatim when present — a message that expires in
 * flight expires at drain instead of being resurrected; when absent, the
 * receiving node's own TTL rules apply.
 *
 * Backpressure: inbound envelopes land in a bounded ingress buffer
 * (`maxInboundQueue`, default 1024). A full buffer sheds the NEWEST
 * envelope (drop-newest keeps the older publish order intact) and counts
 * it in `BridgeStats.dropped`. The buffer drains on a microtask, so a
 * transport that delivers synchronously from inside a publish can never
 * recurse into the bus.
 */

/** One message crossing the bridge, in either direction. */
export interface BridgeEnvelope {
  /** The topic, as published. 1–1024 chars. */
  topic: string;
  /** The application payload — never a compressed wire envelope. */
  payload: unknown;
  /** Keyed-publish key (see `PublishOptions.key`). */
  key?: string;
  /**
   * The key's publish-order sequence number, assigned by the publishing
   * node at admission. Present exactly when `key` is present. The
   * receiving node honors it (instead of drawing its own) so per-key
   * ordering survives out-of-order transport delivery — as long as each
   * key has a single publishing node; two nodes publishing the same key
   * draw from independent cursors and their keySeqs may collide.
   */
  keySeq?: number;
  /** Application-level message identity (see `PublishOptions.messageId`). */
  messageId?: string;
  /** End-to-end trace id, 32 lowercase hex chars. */
  traceId?: string;
  /**
   * The source's TTL deadline in bus-clock milliseconds, carried verbatim
   * (see EB-50's routed deadline). Absent when the source message had no
   * deadline — then the receiving node's TTL rules apply normally.
   */
  expiresAt?: number;
}

/**
 * The caller-implemented adapter between the bus and the external broker.
 * Redis Streams, NATS, an in-process loopback for tests — anything that
 * can move a `BridgeEnvelope` to the peers' `receiveFromBridge`.
 */
export interface BridgeTransport {
  /** Human-readable transport name, for logs and debugging. */
  readonly name: string;
  /**
   * Mirror one admitted publish to the broker. May be sync or async; a
   * throw or a rejected promise never reaches the publish path.
   */
  publish(envelope: BridgeEnvelope): void | Promise<void>;
  /** Release transport resources (sockets, clients). Optional. */
  close?(): void | Promise<void>;
}

export interface BridgeOptions {
  /** The adapter carrying envelopes to the other nodes. Required. */
  transport: BridgeTransport;
  /**
   * Bound on the inbound envelope buffer (see module docs). Must be a
   * positive integer. Default 1024.
   */
  maxInboundQueue?: number;
}

/** `getStats().bridge`: the bridge counters. */
export interface BridgeStats {
  /**
   * Envelopes received from the bridge that cleared admission and fanned
   * out locally. Envelopes rejected by admission (schema, ACL,
   * rate-limit) are NOT counted here — they surface on the normal
   * admission counters and `onAdmissionRejected`, like local publishes.
   */
  inbound: number;
  /** Local admitted publishes mirrored to the transport. */
  outbound: number;
  /** Inbound envelopes shed because the ingress buffer was full. */
  dropped: number;
}

/** Why `receiveFromBridge` refused an envelope. */
export type BridgeReceiveReason =
  /** The bus was constructed without `EventBusOptions.bridge`. */
  | 'not-configured'
  /** The envelope failed shape validation (never admitted, never counted). */
  | 'invalid-envelope'
  /** The ingress buffer was full; counted in `BridgeStats.dropped`. */
  | 'shed';

export interface BridgeReceiveResult {
  accepted: boolean;
  reason?: BridgeReceiveReason;
}

/** Default bound on the bridge ingress buffer. */
export const DEFAULT_BRIDGE_MAX_INBOUND_QUEUE = 1024;

/** A bridge config validated the way the constructor would. */
export interface ResolvedBridgeOptions {
  transport: BridgeTransport;
  maxInboundQueue: number;
}

const TRACE_ID_PATTERN = /^[0-9a-f]{32}$/;
const ZERO_TRACE_ID = '0'.repeat(32);

/**
 * Validates `EventBusOptions.bridge` the way the constructor would:
 * throws `RangeError` on the first problem — a broken bridge config fails
 * at construction time, never mid-publish. Returns `undefined` when no
 * bridge was configured.
 */
export function resolveBridgeOptions(
  options: BridgeOptions | undefined,
  where: string,
): ResolvedBridgeOptions | undefined {
  if (options === undefined) return undefined;
  if (typeof options !== 'object' || options === null) {
    throw new RangeError(`${where}: bridge must be an object`);
  }
  const { transport, maxInboundQueue } = options;
  if (typeof transport !== 'object' || transport === null) {
    throw new RangeError(`${where}: bridge.transport must be an object`);
  }
  if (typeof (transport as BridgeTransport).publish !== 'function') {
    throw new RangeError(`${where}: bridge.transport.publish must be a function`);
  }
  const queue = maxInboundQueue ?? DEFAULT_BRIDGE_MAX_INBOUND_QUEUE;
  if (!Number.isInteger(queue) || queue < 1) {
    throw new RangeError(`${where}: bridge.maxInboundQueue must be a positive integer`);
  }
  return { transport: transport as BridgeTransport, maxInboundQueue: queue };
}

export interface ValidatedBridgeEnvelope {
  ok: true;
  envelope: BridgeEnvelope;
}

export interface InvalidBridgeEnvelope {
  ok: false;
}

/**
 * Validates an inbound envelope's shape. Returns the normalized envelope
 * (empty-string `messageId` normalized to absent, mirroring `fanOut`'s
 * lenient treatment) or `{ ok: false }`. Never throws — a remote peer is
 * not trusted input, so a malformed envelope is reported, not raised.
 */
export function validateBridgeEnvelope(
  envelope: unknown,
): ValidatedBridgeEnvelope | InvalidBridgeEnvelope {
  if (typeof envelope !== 'object' || envelope === null) return { ok: false };
  const env = envelope as Record<string, unknown>;
  const topic = env.topic;
  if (typeof topic !== 'string' || topic.length === 0 || topic.length > 1024) {
    return { ok: false };
  }
  const out: BridgeEnvelope = { topic, payload: env.payload };
  const key = env.key;
  if (key !== undefined) {
    // Same rule as `validateMessageKey` for local publishes.
    if (typeof key !== 'string' || key.length === 0) return { ok: false };
    out.key = key;
    const keySeq = env.keySeq;
    if (keySeq === undefined) return { ok: false };
    if (!Number.isInteger(keySeq) || (keySeq as number) < 1) return { ok: false };
    out.keySeq = keySeq as number;
  } else if (env.keySeq !== undefined) {
    // A sequence number without a key is meaningless — reject, don't guess.
    return { ok: false };
  }
  const messageId = env.messageId;
  if (messageId !== undefined) {
    if (typeof messageId !== 'string') return { ok: false };
    if (messageId.length > 0) out.messageId = messageId;
  }
  const traceId = env.traceId;
  if (traceId !== undefined) {
    // The all-zero id is invalid per the W3C trace-context rules
    // `src/trace.ts` follows — it can never identify a real trace.
    if (typeof traceId !== 'string' || !TRACE_ID_PATTERN.test(traceId) || traceId === ZERO_TRACE_ID) {
      return { ok: false };
    }
    out.traceId = traceId;
  }
  const expiresAt = env.expiresAt;
  if (expiresAt !== undefined) {
    if (typeof expiresAt !== 'number' || !Number.isFinite(expiresAt)) return { ok: false };
    out.expiresAt = expiresAt;
  }
  return { ok: true, envelope: out };
}
