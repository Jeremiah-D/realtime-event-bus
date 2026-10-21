/**
 * Request–reply RPC over the bus (EB-178): correlation-id addressed
 * request/response messaging, zero new dependencies.
 *
 * A requester calls `EventBus.requestResponse(topic, payload, { timeoutMs })`.
 * The bus stamps a `correlationId` and a `replyTo` topic on the request
 * envelope; a responder answers with `EventBus.reply(requestMsg, payload)`,
 * which publishes the response to `replyTo` carrying the same
 * `correlationId`. A lazily-created router subscription on the reserved
 * `_bus.replies` topic settles the first response per correlation id —
 * late arrivals (after the request timed out) and duplicate responses are
 * discarded by correlation id, never delivered twice.
 *
 * This module is pure: no bus state, no timers of its own. The bus owns
 * the pending table, the settled ring, and the router subscription; the
 * helpers here make the routing decision testable in isolation.
 */
import { randomBytes } from 'node:crypto';

/**
 * Reserved reply inbox for `requestResponse` traffic. Requesters publish
 * here via `replyTo`; one router subscription per bus routes responses
 * back to their pending requests by `correlationId`. It is an ordinary
 * topic — a `**` subscriber will see the responses — but application
 * code should never publish to it directly (use `EventBus.reply`).
 */
export const RPC_REPLY_TOPIC = '_bus.replies';

/** Maximum settled correlation ids remembered for late/duplicate discard. */
export const RPC_SETTLED_RING_SIZE = 4096;

/**
 * How long a settled correlation id keeps discarding late responses, in
 * milliseconds. After this the id may be forgotten; a response arriving
 * later is dropped as unknown instead of counted as a late duplicate.
 */
export const RPC_SETTLED_TTL_MS = 5 * 60 * 1000;

/**
 * Rejection of `EventBus.requestResponse` when no response arrived within
 * `timeoutMs`. Carries the `correlationId` so callers can correlate the
 * failure with their own logs.
 */
export class RequestTimeoutError extends Error {
  /** The correlation id of the timed-out request. */
  readonly correlationId: string;
  /** The timeout that elapsed, in milliseconds. */
  readonly timeoutMs: number;

  constructor(correlationId: string, timeoutMs: number) {
    super(`requestResponse timed out after ${timeoutMs}ms (correlationId "${correlationId}")`);
    this.name = 'RequestTimeoutError';
    this.correlationId = correlationId;
    this.timeoutMs = timeoutMs;
  }
}

/**
 * Options for `EventBus.requestResponse`.
 */
export interface RequestResponseOptions {
  /**
   * How long to wait for the first response before rejecting with
   * `RequestTimeoutError`. Must be a finite number greater than 0.
   */
  timeoutMs: number;
  /**
   * Caller-supplied correlation id. Must be a non-empty string when
   * given; a duplicate of an already-pending request's id throws
   * `RangeError`. Defaults to 32 random hex characters.
   */
  correlationId?: string;
  /**
   * Extra publish options for the request message (headers, priority,
   * ttlMs, …). Typed as `unknown` here to keep this module free of the
   * bus's `PublishOptions` import cycle — `EventBus.requestResponse`
   * accepts `PublishOptions` and casts. `correlationId`/`replyTo` inside
   * are ignored: the RPC layer stamps its own.
   */
  publishOptions?: unknown;
  /**
   * Opt-in reliable (at-least-once) delivery for the router's reply
   * subscription: responses are acked on receipt, so a lost ack
   * redelivers instead of losing the response. `true` uses the default
   * ack timeout; an object tunes it. Composes with a responder that
   * uses `subscribeReliable` on the request topic.
   */
  reliable?: boolean | { ackTimeoutMs?: number };
  /**
   * Clock override for deterministic tests: `() => number` epoch
   * milliseconds. Defaults to `Date.now`.
   */
  now?: () => number;
}

/**
 * Lenient correlation-id cleanup, mirroring `PublishOptions.messageId`:
 * a non-string, an empty string, or a string longer than 1024
 * characters is treated as absent; anything else is used verbatim.
 * (Overlong ids are dropped rather than throwing because a request id
 * is application data, like `messageId` — `requestResponse` validates
 * its own generated ids strictly instead.)
 */
export function cleanCorrelationId(value: unknown): string | undefined {
  if (typeof value !== 'string' || value.length === 0 || value.length > 1024) return undefined;
  return value;
}

/** Strict correlation-id check for `requestResponse`'s explicit option. */
export function assertCorrelationId(value: string, caller: string): void {
  if (typeof value !== 'string' || value.length === 0 || value.length > 1024) {
    throw new RangeError(
      `${caller}: correlationId must be a non-empty string of at most 1024 characters`,
    );
  }
}

/** Generates a fresh 32-hex-char correlation id. */
export function randomCorrelationId(): string {
  return randomBytes(16).toString('hex');
}

/**
 * The router's decision for one message arriving on the reply topic,
 * given the bus's pending table and settled ring. Pure — the bus
 * applies the side effects (settle, discard, ack).
 */
export type RpcRouteDecision =
  | { kind: 'settle' }
  | { kind: 'discard-late' }
  | { kind: 'discard-unknown' };

export function routeRpcResponse(
  correlationId: string | undefined,
  pending: ReadonlyMap<string, unknown>,
  settled: ReadonlyMap<string, number>,
): RpcRouteDecision {
  if (correlationId === undefined) return { kind: 'discard-unknown' };
  if (pending.has(correlationId)) return { kind: 'settle' };
  if (settled.has(correlationId)) return { kind: 'discard-late' };
  return { kind: 'discard-unknown' };
}
