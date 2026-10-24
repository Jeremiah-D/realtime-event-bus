/**
 * Ops alert webhook outlet (EB-132): the bus's operational signals —
 * consumer-lag SLO breaches (on top of the EB-112 in-process callback),
 * dead-letter queue growth, and gateway disconnect storms — delivered
 * as signed HTTP POSTs to a configured ops endpoint.
 *
 * The design follows the AV-118 keeper-notifier convention:
 *
 * - [`OpsAlertSink`] is the opt-in pluggable transport trait. The core
 *   defines the trait plus a deterministic in-memory
 *   [`RecordingAlertSink`] for tests; the *caller* implements the
 *   actual HTTP delivery (POST the delivery JSON, set
 *   [`OPS_SIGNATURE_HEADER`]). Zero dependencies on the core side
 *   (`node:crypto` only, like the rest of the repo) — the bus never
 *   touches the network.
 * - Every delivery is authenticated: HMAC-SHA256 (RFC 2104) over the
 *   canonical alert bytes, travelling in [`OPS_SIGNATURE_HEADER`]
 *   (`x-ops-signature`), with an optional key id for secret rotation.
 *   [`verifyDelivery`] is the receiver-side check.
 * - Delivery is *bounded*: each alert is attempted at most
 *   `maxAttempts` times with capped exponential backoff, then dropped
 *   and counted. `note` never throws and never blocks the caller — a
 *   dead ops endpoint can neither stall nor break the publish path.
 * - A dedup window collapses bursts: repeated alerts for the same
 *   `(kind, key)` inside `dedupWindowMs` are suppressed and counted,
 *   so a flapping SLO or a DLQ flood produces one alert per window,
 *   not one per event.
 * - Opt-in by construction: without `EventBusOptions.opsAlert` the bus
 *   creates no alerter and behaves byte-identically to before.
 *
 * This module is pure apart from `node:crypto`: no bus state, no timers
 * that keep the process alive (retry backoff uses unref'd timeouts).
 */

import { createHmac, timingSafeEqual } from 'node:crypto';
import { Buffer } from 'node:buffer';

/** The HTTP header carrying the delivery signature. */
export const OPS_SIGNATURE_HEADER = 'x-ops-signature';

/** What the ops outlet can report. */
export type OpsAlertKind =
  | 'lag-slo-breach'
  | 'dead-letter-growth'
  | 'gateway-disconnect-storm'
  | 'processing-latency-slo-breach'
  | 'subscriber-breaker-tripped';

/** One operational alert, as built by the bus. */
export interface OpsAlert {
  /** Which signal fired. */
  kind: OpsAlertKind;
  /** When the alert was raised, on the bus clock (epoch millis). */
  at: number;
  /** `busId` of the bus that raised it. */
  busId: string;
  /**
   * Signal-specific detail — JSON-safe primitives only. `lag-slo-breach`
   * carries the breach event fields (groupId/pattern/topic/partition/
   * lag/violated/...); `dead-letter-growth` carries
   * subscriberId/pattern/topic/redeliveries/lastError;
   * `gateway-disconnect-storm` carries disconnects/windowMs/threshold;
   * `processing-latency-slo-breach` carries
   * subscriberId/pattern/p99Ms/thresholdMs/samples/phaseAttribution;
   * `subscriber-breaker-tripped` carries
   * subscriberId/pattern/consecutiveFailures/pendingMessages.
   */
  detail: Record<string, unknown>;
}

/** An alert plus its authentication envelope, as handed to the sink. */
export interface OpsAlertDelivery {
  /** The alert that fired. */
  alert: OpsAlert;
  /** Key id that signed the delivery, when the bus was configured with one. */
  keyId?: string;
  /** Lowercase hex HMAC-SHA256 over the canonical alert bytes. */
  signature: string;
}

/**
 * Pluggable alert transport (the AV-118 trait seam): the caller
 * implements the actual HTTP POST. `send` must POST the delivery JSON
 * to the ops endpoint with [`OPS_SIGNATURE_HEADER`] set to
 * `delivery.signature`. A rejected promise (or a throw) counts as a
 * failed attempt and is retried within the bound; it never propagates
 * to the bus.
 */
export interface OpsAlertSink {
  send(delivery: OpsAlertDelivery): Promise<void>;
}

/** In-memory sink for tests: records every delivery it was handed. */
export class RecordingAlertSink implements OpsAlertSink {
  /** Deliveries the sink accepted, in send order. */
  readonly deliveries: OpsAlertDelivery[] = [];
  /** Number of `send` calls that failed (test-controlled). */
  failuresLeft: number;
  constructor(failuresLeft = 0) {
    this.failuresLeft = failuresLeft;
  }
  async send(delivery: OpsAlertDelivery): Promise<void> {
    if (this.failuresLeft > 0) {
      this.failuresLeft -= 1;
      throw new Error('recording sink: injected failure');
    }
    this.deliveries.push(delivery);
  }
}

/**
 * Deterministic JSON serialization: object keys sorted recursively,
 * so the same alert always signs to the same bytes. Only JSON-safe
 * values are expected — `undefined`, functions and symbols encode as
 * `null` rather than throwing.
 */
export function canonicalAlertJson(value: unknown): string {
  const canonicalize = (v: unknown): unknown => {
    if (v === null || v === undefined) return null;
    if (Array.isArray(v)) return v.map(canonicalize);
    if (typeof v === 'object') {
      const out: Record<string, unknown> = {};
      for (const k of Object.keys(v as Record<string, unknown>).sort()) {
        out[k] = canonicalize((v as Record<string, unknown>)[k]);
      }
      return out;
    }
    if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') return v;
    return null;
  };
  return JSON.stringify(canonicalize(value));
}

/** HMAC-SHA256 hex of the canonical alert bytes under `secret`. */
export function signAlert(alert: OpsAlert, secret: string | Buffer): string {
  const key = typeof secret === 'string' ? Buffer.from(secret, 'utf8') : secret;
  return createHmac('sha256', key).update(canonicalAlertJson(alert), 'utf8').digest('hex');
}

/**
 * Receiver-side check: recomputes the signature over the delivery's
 * alert and compares in constant time. Returns false (never throws)
 * on any malformed delivery.
 */
export function verifyDelivery(delivery: OpsAlertDelivery, secret: string | Buffer): boolean {
  try {
    if (delivery === null || typeof delivery !== 'object') return false;
    const { alert, signature } = delivery;
    if (typeof signature !== 'string' || !/^[0-9a-f]{64}$/.test(signature)) return false;
    const expected = signAlert(alert, secret);
    const a = Buffer.from(signature, 'utf8');
    const b = Buffer.from(expected, 'utf8');
    return a.length === b.length && timingSafeEqual(a, b);
  } catch {
    return false;
  }
}

/** Options for [`OpsAlerter`], mirroring `EventBusOptions.opsAlert`. */
export interface OpsAlerterOptions {
  /** Where to POST alerts — the caller implements the HTTP delivery. */
  sink: OpsAlertSink;
  /** HMAC secret signing every delivery. A string is UTF-8 encoded. */
  secret: string | Buffer;
  /** Key id annotation for secret rotation. */
  keyId?: string;
  /** Bus id stamped on every alert. */
  busId: string;
  /** Clock, epoch millis — the bus injects its own for determinism. */
  now?: () => number;
  /** Burst-collapse window per (kind, key). Default 5 minutes. */
  dedupWindowMs?: number;
  /** Max delivery attempts per alert, including the first. Default 3. */
  maxAttempts?: number;
  /** Base backoff between attempts; doubled per attempt, capped at 30s. Default 1s. */
  retryBaseMs?: number;
  /** Disconnects inside the window that declare a storm. Default 10. */
  gatewayDisconnectThreshold?: number;
  /** Sliding window for the disconnect-storm detector. Default 60s. */
  gatewayDisconnectWindowMs?: number;
}

/** Validates options the way `EventBus` would: throws `RangeError` first. */
export function resolveOpsAlerterOptions(
  options: OpsAlerterOptions,
  where: string,
): Required<
  Pick<
    OpsAlerterOptions,
    | 'sink'
    | 'secret'
    | 'busId'
    | 'now'
    | 'dedupWindowMs'
    | 'maxAttempts'
    | 'retryBaseMs'
    | 'gatewayDisconnectThreshold'
    | 'gatewayDisconnectWindowMs'
  >
> & { keyId?: string } {
  if (options === null || typeof options !== 'object') {
    throw new RangeError(`${where}: opsAlert must be an options object`);
  }
  const { sink, secret, keyId, busId } = options;
  if (sink === null || typeof sink !== 'object' || typeof (sink as OpsAlertSink).send !== 'function') {
    throw new RangeError(`${where}: opsAlert.sink must be an object with a send(delivery) method`);
  }
  const secretOk =
    (typeof secret === 'string' && secret.length > 0) ||
    (typeof secret !== 'string' && Buffer.isBuffer(secret) && secret.length > 0);
  if (!secretOk) {
    throw new RangeError(`${where}: opsAlert.secret must be a non-empty string or Buffer`);
  }
  if (keyId !== undefined && (typeof keyId !== 'string' || keyId.length === 0)) {
    throw new RangeError(`${where}: opsAlert.keyId must be a non-empty string when given`);
  }
  if (typeof busId !== 'string' || busId.length === 0) {
    throw new RangeError(`${where}: opsAlert.busId must be a non-empty string`);
  }
  const now = options.now ?? Date.now;
  if (typeof now !== 'function') {
    throw new RangeError(`${where}: opsAlert.now must be a function`);
  }
  const dedupWindowMs = options.dedupWindowMs ?? 5 * 60_000;
  if (!Number.isFinite(dedupWindowMs) || dedupWindowMs <= 0) {
    throw new RangeError(`${where}: opsAlert.dedupWindowMs must be a positive finite number`);
  }
  const maxAttempts = options.maxAttempts ?? 3;
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new RangeError(`${where}: opsAlert.maxAttempts must be a positive integer`);
  }
  const retryBaseMs = options.retryBaseMs ?? 1000;
  if (!Number.isFinite(retryBaseMs) || retryBaseMs <= 0) {
    throw new RangeError(`${where}: opsAlert.retryBaseMs must be a positive finite number`);
  }
  const gatewayDisconnectThreshold = options.gatewayDisconnectThreshold ?? 10;
  if (!Number.isInteger(gatewayDisconnectThreshold) || gatewayDisconnectThreshold < 1) {
    throw new RangeError(`${where}: opsAlert.gatewayDisconnectThreshold must be a positive integer`);
  }
  const gatewayDisconnectWindowMs = options.gatewayDisconnectWindowMs ?? 60_000;
  if (!Number.isFinite(gatewayDisconnectWindowMs) || gatewayDisconnectWindowMs <= 0) {
    throw new RangeError(`${where}: opsAlert.gatewayDisconnectWindowMs must be a positive finite number`);
  }
  return {
    sink: sink as OpsAlertSink,
    secret: secret as string | Buffer,
    keyId,
    busId,
    now,
    dedupWindowMs,
    maxAttempts,
    retryBaseMs,
    gatewayDisconnectThreshold,
    gatewayDisconnectWindowMs,
  };
}

/** Delivery counters for `getStats().opsAlert`. */
export interface OpsAlertStats {
  /** Alerts handed to the sink and accepted. */
  sent: number;
  /** Alerts dropped after exhausting the retry bound. */
  failed: number;
  /** Notes suppressed by the dedup window. */
  suppressed: number;
}

/**
 * The ops alert outlet. `note` is synchronous, infallible, and never
 * blocks: the delivery runs on a detached promise chain with bounded
 * retries, and every failure mode is counted, never thrown.
 */
export class OpsAlerter {
  private readonly sink: OpsAlertSink;
  private readonly secret: string | Buffer;
  private readonly keyId: string | undefined;
  private readonly busId: string;
  private readonly now: () => number;
  private readonly dedupWindowMs: number;
  private readonly maxAttempts: number;
  private readonly retryBaseMs: number;
  private readonly stormThreshold: number;
  private readonly stormWindowMs: number;
  /** Last alert time per `kind\0key` — the dedup window state. */
  private readonly lastAlertAt = new Map<string, number>();
  /** Recent gateway-disconnect timestamps — the storm detector's window. */
  private disconnectTimes: number[] = [];
  /** True while a storm alert is active (edge-triggered, rearms below threshold). */
  private stormActive = false;
  private sent = 0;
  private failed = 0;
  private suppressed = 0;

  constructor(options: OpsAlerterOptions) {
    const r = resolveOpsAlerterOptions(options, 'OpsAlerter');
    this.sink = r.sink;
    this.secret = r.secret;
    this.keyId = r.keyId;
    this.busId = r.busId;
    this.now = r.now;
    this.dedupWindowMs = r.dedupWindowMs;
    this.maxAttempts = r.maxAttempts;
    this.retryBaseMs = r.retryBaseMs;
    this.stormThreshold = r.gatewayDisconnectThreshold;
    this.stormWindowMs = r.gatewayDisconnectWindowMs;
  }

  /** Counter snapshot — mutating it changes nothing. */
  getStats(): OpsAlertStats {
    return { sent: this.sent, failed: this.failed, suppressed: this.suppressed };
  }

  /**
   * Raises an alert for `kind`, deduplicated per `key`: a repeat for
   * the same `(kind, key)` inside the dedup window is suppressed and
   * counted. Synchronous and infallible — delivery is detached.
   */
  note(kind: OpsAlertKind, key: string, detail: Record<string, unknown>): void {
    try {
      const nowMs = this.now();
      const mapKey = `${kind}\0${key}`;
      const last = this.lastAlertAt.get(mapKey);
      if (last !== undefined && nowMs - last < this.dedupWindowMs) {
        this.suppressed += 1;
        return;
      }
      this.lastAlertAt.set(mapKey, nowMs);
      const alert: OpsAlert = { kind, at: nowMs, busId: this.busId, detail };
      const delivery: OpsAlertDelivery = {
        alert,
        ...(this.keyId === undefined ? {} : { keyId: this.keyId }),
        signature: signAlert(alert, this.secret),
      };
      void this.deliver(delivery, 1);
    } catch {
      // `note` is infallible by contract: a throwing clock or detail
      // getter must not propagate into the publish path.
    }
  }

  /**
   * Records one gateway disconnect for the storm detector: when at
   * least `gatewayDisconnectThreshold` disconnects land inside
   * `gatewayDisconnectWindowMs`, one `gateway-disconnect-storm` alert
   * fires (edge-triggered; it rearms when the window drains below
   * threshold). Synchronous and infallible.
   */
  noteGatewayDisconnect(): void {
    try {
      const nowMs = this.now();
      const cutoff = nowMs - this.stormWindowMs;
      this.disconnectTimes = this.disconnectTimes.filter((t) => t > cutoff);
      this.disconnectTimes.push(nowMs);
      if (this.disconnectTimes.length >= this.stormThreshold) {
        if (!this.stormActive) {
          this.stormActive = true;
          this.note('gateway-disconnect-storm', 'storm', {
            disconnects: this.disconnectTimes.length,
            windowMs: this.stormWindowMs,
            threshold: this.stormThreshold,
          });
        }
      } else {
        this.stormActive = false;
      }
    } catch {
      // Infallible by contract — see `note`.
    }
  }

  /**
   * Delivers one alert with bounded retries: attempts are
   * `retryBaseMs * 2^(attempt-1)`, capped at 30s, unref'd so they never
   * keep the process alive. Never throws — exhaustion is counted.
   */
  private async deliver(delivery: OpsAlertDelivery, attempt: number): Promise<void> {
    try {
      await this.sink.send(delivery);
      this.sent += 1;
    } catch {
      if (attempt >= this.maxAttempts) {
        this.failed += 1;
        return;
      }
      const delayMs = Math.min(this.retryBaseMs * 2 ** (attempt - 1), 30_000);
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, delayMs);
        if (typeof timer === 'object' && 'unref' in timer) {
          (timer as { unref(): void }).unref();
        }
      });
      return this.deliver(delivery, attempt + 1);
    }
  }
}
