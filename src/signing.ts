/**
 * Per-topic HMAC message signing (EB-84): publish-side authenticity for
 * bus messages, zero new dependencies (`node:crypto` only).
 *
 * A topic whose pattern matches a signing rule (see
 * `EventBus.setTopicSigningKey`) has every admitted publish signed with
 * HMAC-SHA256 over `(topic, seq, wire bytes)`:
 *
 *   base = topic + "\0" + String(seq) + wireBytes
 *   sig  = hex(HMAC-SHA256(secret, base))
 *
 * The wire bytes are the post-compression form — a compressed envelope's
 * signature covers the ciphertext, and a subscriber verifies those same
 * bytes before inflating. `"\0"` separates the topic from the sequence
 * number so no two `(topic, seq)` pairs share a base string.
 *
 * Key rotation: a rule holds several keys at once, each identified by a
 * `kid`. The newest key (index 0) signs; older keys keep verifying inside
 * their grace window (`graceMs` from the moment they stopped being
 * newest — `retiredAtMs`, inclusive, the WR-20 convention). Verification:
 * a `kid` hint verifies against that single key with no fallback; no
 * hint tries the active keys newest-first.
 *
 * This module is pure: no bus state, no clock of its own — callers pass
 * `nowMs`. The bus (`src/bus.ts`) owns rule storage, rotation stamping,
 * and the subscribe-side opt-in (`SubscribeOptions.verifySignature`).
 */
import { createHmac, timingSafeEqual } from 'node:crypto';
import { Buffer } from 'node:buffer';

/** One signing key as supplied to `setTopicSigningKey`. */
export interface TopicSigningKeyInput {
  /**
   * Key identifier, 1–64 chars of `[A-Za-z0-9_-]`. Rides the envelope so
   * the verifier can select the key — it is a label, never a secret.
   */
  kid: string;
  /**
   * HMAC secret. A string is UTF-8 encoded; a Buffer is used as-is.
   * Must be non-empty.
   */
  secret: string | Buffer;
}

/** Configuration for `EventBus.setTopicSigningKey`. */
export interface TopicSigningKeysConfig {
  /**
   * Signing keys, newest first: index 0 is the live signing key, every
   * later key is a retired key that still verifies inside the grace
   * window. At least one key is required.
   */
  keys: TopicSigningKeyInput[];
  /**
   * How long after a key stops being newest its signatures still verify,
   * in milliseconds. Non-negative finite number, defaults to 0 (a
   * rotated-out key stops verifying immediately).
   */
  graceMs?: number;
}

/**
 * A stored signing key: the normalized secret plus the bus-stamped
 * retirement time. `retiredAtMs` is `undefined` for the live signing
 * key (index 0); the bus sets it when the key stops being newest.
 */
export interface StoredSigningKey {
  kid: string;
  secret: Buffer;
  retiredAtMs?: number;
}

/** The signature riding a `BusMessage` envelope. */
export interface MessageSignature {
  /** Key id that signed the message. */
  kid: string;
  /** Lowercase hex HMAC-SHA256 (64 chars). */
  sig: string;
}

/** Key-id alphabet: 1–64 ASCII alphanumerics, `_` or `-`. */
export const SIGNING_KID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

/** HMAC-SHA256 hex digest length. */
export const SIGNATURE_HEX_LENGTH = 64;

const SIGNATURE_HEX_PATTERN = /^[0-9a-f]{64}$/;

/**
 * Validates a key list the way `setTopicSigningKey` does: throws
 * `RangeError` on an empty list, an empty/duplicate/malformed `kid`, or
 * an empty secret — fail-fast at configuration time, never mid-publish.
 */
export function assertValidSigningKeys(keys: unknown, where: string): void {
  if (!Array.isArray(keys) || keys.length === 0) {
    throw new RangeError(`${where}: keys must be a non-empty array`);
  }
  const seen = new Set<string>();
  keys.forEach((key, i) => {
    const label = `${where}: keys[${i}]`;
    if (key === null || typeof key !== 'object') {
      throw new RangeError(`${label} must be an object`);
    }
    const { kid, secret } = key as { kid: unknown; secret: unknown };
    if (typeof kid !== 'string' || !SIGNING_KID_PATTERN.test(kid)) {
      throw new RangeError(
        `${label}: kid must be 1-64 chars of [A-Za-z0-9_-]`,
      );
    }
    if (seen.has(kid)) {
      throw new RangeError(`${label}: duplicate kid "${kid}"`);
    }
    seen.add(kid);
    normalizeSigningSecret(secret, label); // throws on empty secret
  });
}

/** Normalizes a key secret to a Buffer; throws `RangeError` when empty. */
export function normalizeSigningSecret(secret: unknown, where: string): Buffer {
  if (typeof secret === 'string') {
    if (secret.length === 0) throw new RangeError(`${where}: secret must be non-empty`);
    return Buffer.from(secret, 'utf8');
  }
  if (Buffer.isBuffer(secret)) {
    if (secret.length === 0) throw new RangeError(`${where}: secret must be non-empty`);
    return secret;
  }
  throw new RangeError(`${where}: secret must be a non-empty string or Buffer`);
}

/**
 * The canonical wire bytes of a payload: the UTF-8 JSON encoding.
 * Returns `undefined` for payloads with no JSON encoding (`undefined`,
 * functions, symbols) or ones `JSON.stringify` cannot represent
 * (circular structures, BigInt) — the same rule the durable log uses,
 * so anything the log cannot persist cannot be signed either.
 */
export function canonicalWireBytes(value: unknown): Buffer | undefined {
  let serialized: string | undefined;
  try {
    const s = JSON.stringify(value);
    serialized = typeof s === 'string' ? s : undefined;
  } catch {
    return undefined;
  }
  if (serialized === undefined) return undefined;
  return Buffer.from(serialized, 'utf8');
}

/**
 * Builds the signed base: `topic + "\0" + seq + wireBytes`. The NUL
 * separator keeps `(topic, seq)` unambiguous — no two pairs collide.
 */
export function signingBaseBytes(topic: string, seq: number, wireBytes: Buffer): Buffer {
  return Buffer.concat([
    Buffer.from(topic, 'utf8'),
    Buffer.from([0]),
    Buffer.from(String(seq), 'utf8'),
    wireBytes,
  ]);
}

/**
 * Signs one message with a single key. The caller selects the key (the
 * bus always signs with the rule's newest key); the returned `kid`
 * identifies it for the verifier.
 */
export function signTopicMessage(
  key: { kid: string; secret: Buffer },
  topic: string,
  seq: number,
  wireBytes: Buffer,
): MessageSignature {
  const sig = createHmac('sha256', key.secret)
    .update(signingBaseBytes(topic, seq, wireBytes))
    .digest('hex');
  return { kid: key.kid, sig };
}

/** Whether a key still verifies at `nowMs` (the WR-20 inclusive rule). */
export function isSigningKeyActive(
  key: { retiredAtMs?: number },
  nowMs: number,
  graceMs: number,
): boolean {
  if (key.retiredAtMs === undefined) return true;
  return nowMs - key.retiredAtMs <= graceMs;
}

/** True when `sig` is a well-formed 64-char lowercase hex digest. */
export function isWellFormedSignature(sig: unknown): sig is string {
  return typeof sig === 'string' && SIGNATURE_HEX_PATTERN.test(sig);
}

/**
 * Verifies an envelope signature against a rule's keys:
 * - a `kid` hint verifies against that single key only — no fallback to
 *   other keys, and a retired-beyond-grace hint fails outright;
 * - no hint tries the active keys newest-first and accepts the first
 *   match.
 *
 * Never throws: malformed input (missing signature, bad hex, unknown
 * kid) verifies as `false` — a remote peer is not trusted input.
 */
export function verifyTopicMessage(
  keys: StoredSigningKey[],
  topic: string,
  seq: number,
  wireBytes: Buffer,
  signature: { kid?: string; sig: string } | undefined,
  nowMs: number,
  graceMs: number,
): boolean {
  if (signature === undefined || signature === null) return false;
  const { kid, sig } = signature;
  if (!isWellFormedSignature(sig)) return false;
  const base = signingBaseBytes(topic, seq, wireBytes);
  const expected = (secret: Buffer): Buffer =>
    createHmac('sha256', secret).update(base).digest();
  const matches = (secret: Buffer): boolean => {
    const digest = expected(secret);
    const presented = Buffer.from(sig, 'hex');
    // `timingSafeEqual` throws on length mismatch — a well-formed hex
    // signature is always 32 bytes, but guard anyway.
    return presented.length === digest.length && timingSafeEqual(digest, presented);
  };
  if (typeof kid === 'string' && kid.length > 0) {
    const key = keys.find((k) => k.kid === kid);
    if (key === undefined || !isSigningKeyActive(key, nowMs, graceMs)) return false;
    return matches(key.secret);
  }
  for (const key of keys) {
    if (!isSigningKeyActive(key, nowMs, graceMs)) continue;
    if (matches(key.secret)) return true;
  }
  return false;
}
