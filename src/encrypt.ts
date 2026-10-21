import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

/**
 * Encrypted data envelope. JSON-serializable, so it can be stored inline
 * in a durable-log line without leaking plaintext.
 */
export interface EncryptedPayload {
  /** Algorithm id, e.g. `"aes-256-gcm"` or `"none"`. */
  alg: string;
  /** base64 nonce/IV ("" when the scheme needs none). */
  iv: string;
  /** base64 ciphertext. */
  data: string;
  /** base64 authentication tag (AEAD schemes). */
  tag?: string;
  /**
   * Key version that sealed this envelope (see `RotatingDekEncryptor`).
   * Absent for single-key encryptors (`AesGcmEncryptor`) — a versioned
   * decryptor rejects a keyless envelope explicitly instead of guessing.
   */
  keyId?: string;
}

/**
 * A data encryptor: one named scheme that seals raw bytes into an
 * {@link EncryptedPayload} and opens them back up. Implementations must
 * throw from `decrypt` on tampered or unrecognized envelopes (never return
 * garbage silently) — the WR-11 "tampering fails loudly" convention.
 */
export interface PayloadEncryptor {
  /** Unique scheme name, e.g. `"aes-256-gcm"`. */
  readonly name: string;
  /** Seal `plaintext` into a JSON-serializable envelope. Never throws on valid input. */
  encrypt(plaintext: Buffer): EncryptedPayload;
  /** Open `envelope` back into the original bytes. Throws on any mismatch. */
  decrypt(envelope: EncryptedPayload): Buffer;
}

/** True when `v` has the shape of an {@link EncryptedPayload}. */
export function isEncryptedPayload(v: unknown): v is EncryptedPayload {
  if (!v || typeof v !== 'object') return false;
  const e = v as Record<string, unknown>;
  return (
    typeof e.alg === 'string' &&
    typeof e.iv === 'string' &&
    typeof e.data === 'string' &&
    (e.tag === undefined || typeof e.tag === 'string')
  );
}

/**
 * AES-256-GCM payload encryption, `node:crypto` only. Each `encrypt` uses a
 * fresh random 12-byte IV, so identical plaintexts produce different
 * envelopes; the 16-byte auth tag makes tampering fail `decrypt` loudly.
 */
export class AesGcmEncryptor implements PayloadEncryptor {
  readonly name = 'aes-256-gcm';
  private readonly key: Buffer;

  /** `key` must be exactly 32 bytes (throws `RangeError` otherwise). */
  constructor(key: Buffer) {
    if (!Buffer.isBuffer(key) || key.length !== 32) {
      throw new RangeError(
        `AesGcmEncryptor needs a 32-byte key, got ${Buffer.isBuffer(key) ? `${key.length} bytes` : typeof key}`,
      );
    }
    this.key = Buffer.from(key); // copy: later mutation of the caller's buffer can't weaken us
  }

  encrypt(plaintext: Buffer): EncryptedPayload {
    const iv = randomBytes(12);
    const cipher = createCipheriv('aes-256-gcm', this.key, iv);
    const data = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    return {
      alg: this.name,
      iv: iv.toString('base64'),
      data: data.toString('base64'),
      tag: cipher.getAuthTag().toString('base64'),
    };
  }

  decrypt(envelope: EncryptedPayload): Buffer {
    try {
      if (!isEncryptedPayload(envelope) || envelope.alg !== this.name) {
        throw new Error(`unsupported envelope alg: ${(envelope as { alg?: unknown }).alg}`);
      }
      const iv = Buffer.from(envelope.iv, 'base64');
      const tag = Buffer.from(envelope.tag ?? '', 'base64');
      if (iv.length !== 12) throw new Error(`bad IV length: ${iv.length}`);
      if (tag.length !== 16) throw new Error(`bad auth tag length: ${tag.length}`);
      const decipher = createDecipheriv('aes-256-gcm', this.key, iv);
      decipher.setAuthTag(tag);
      return Buffer.concat([
        decipher.update(Buffer.from(envelope.data, 'base64')),
        decipher.final(),
      ]);
    } catch (err) {
      throw new Error(
        `AesGcmEncryptor.decrypt failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }
}

/** Generate a fresh 32-byte key for {@link AesGcmEncryptor}. */
export function randomAes256Key(): Buffer {
  return randomBytes(32);
}

/**
 * One data-encryption key (DEK) as supplied to {@link RotatingDekEncryptor}.
 */
export interface DekKeyInput {
  /**
   * Key version id, 1–64 chars of `[A-Za-z0-9_-]`. Rides the envelope so
   * the decryptor can select the key — it is a label, never a secret.
   * Must be unique within the ring.
   */
  keyId: string;
  /** The 32-byte AES-256 key. Copied defensively on registration. */
  key: Buffer;
}

/** Key-id alphabet: 1–64 ASCII alphanumerics, `_` or `-` (the signing.ts convention). */
const KEY_ID_RE = /^[A-Za-z0-9_-]{1,64}$/;

/** Fail-fast key-id validation shared by the ring's entry points. */
export function assertValidKeyId(keyId: unknown, caller: string): asserts keyId is string {
  if (typeof keyId !== 'string' || !KEY_ID_RE.test(keyId)) {
    throw new RangeError(
      `${caller}: keyId must be 1-64 ASCII alphanumeric, '_' or '-' characters`,
    );
  }
}

function assertValidDekKey(key: unknown, caller: string): Buffer {
  if (!Buffer.isBuffer(key) || key.length !== 32) {
    throw new RangeError(
      `${caller}: DEK must be a 32-byte Buffer, got ${Buffer.isBuffer(key) ? `${(key as Buffer).length} bytes` : typeof key}`,
    );
  }
  return Buffer.from(key); // copy: later mutation of the caller's buffer can't weaken us
}

interface StoredDek {
  keyId: string;
  encryptor: AesGcmEncryptor;
  /** Set when the key stopped being the live key; `undefined` for the live key. */
  retiredAtMs?: number;
}

/**
 * A key-id-versioned ring of AES-256-GCM data-encryption keys (DEKs).
 *
 * Index 0 is always the live key: `encrypt` seals with it and stamps
 * its `keyId` on the envelope. `rotate()` generates a fresh random DEK,
 * makes it live, and retires the previous keys as of now — they keep
 * decrypting inside the grace window (`graceMs` from `retiredAtMs`,
 * inclusive), so messages sealed just before rotation still open.
 * `decrypt` selects the key by the envelope's `keyId` and fails loudly
 * on an unknown key id, a keyless envelope, or a key whose grace
 * window fully elapsed — never returning garbage or guessing.
 *
 * Implements `PayloadEncryptor` with `Date.now()` as the default clock;
 * pass `nowMs` explicitly for deterministic tests. Zero new
 * dependencies (`node:crypto` only).
 */
export class RotatingDekEncryptor implements PayloadEncryptor {
  readonly name = 'aes-256-gcm';
  private readonly keys: StoredDek[] = [];
  private readonly graceMs: number;

  /**
   * `keys[0]` is the live DEK; every later key starts retired as of
   * `nowMs` (a restored ring). At least one key is required; key ids
   * must be unique and valid; every key must be 32 bytes.
   * `graceMs` (default 0) is how long a retired key keeps decrypting.
   */
  constructor(keys: DekKeyInput[], opts?: { graceMs?: number; nowMs?: number }) {
    if (!Array.isArray(keys) || keys.length === 0) {
      throw new RangeError('RotatingDekEncryptor needs at least one DEK');
    }
    const graceMs = opts?.graceMs ?? 0;
    if (!Number.isFinite(graceMs) || graceMs < 0) {
      throw new RangeError('graceMs must be a non-negative finite number of milliseconds');
    }
    const nowMs = opts?.nowMs ?? Date.now();
    const seen = new Set<string>();
    keys.forEach((input, index) => {
      assertValidKeyId(input?.keyId, `RotatingDekEncryptor: keys[${index}]`);
      if (seen.has(input.keyId)) {
        throw new RangeError(`RotatingDekEncryptor: duplicate keyId "${input.keyId}"`);
      }
      seen.add(input.keyId);
      this.keys.push({
        keyId: input.keyId,
        encryptor: new AesGcmEncryptor(assertValidDekKey(input?.key, `RotatingDekEncryptor: keys[${index}]`)),
        ...(index === 0 ? {} : { retiredAtMs: nowMs }),
      });
    });
    this.graceMs = graceMs;
  }

  /** The key id `encrypt` currently seals with. */
  get liveKeyId(): string {
    return this.keys[0].keyId;
  }

  /** Key ids currently held, live first. */
  keyIds(): string[] {
    return this.keys.map((k) => k.keyId);
  }

  /** Seal `plaintext` with the live DEK; the envelope carries its `keyId`. */
  encrypt(plaintext: Buffer): EncryptedPayload {
    const live = this.keys[0];
    return { ...live.encryptor.encrypt(plaintext), keyId: live.keyId };
  }

  /**
   * Open `envelope` with the DEK its `keyId` names. Throws on a keyless
   * envelope, an unknown key id, a retired key past its grace window, or
   * any tampering — the WR-11 "fail loudly" convention.
   */
  decrypt(envelope: EncryptedPayload, nowMs: number = Date.now()): Buffer {
    const keyId = (envelope as { keyId?: unknown } | null)?.keyId;
    if (typeof keyId !== 'string' || keyId.length === 0) {
      throw new Error('RotatingDekEncryptor.decrypt failed: envelope carries no keyId');
    }
    const stored = this.keys.find((k) => k.keyId === keyId);
    if (stored === undefined) {
      throw new Error(`RotatingDekEncryptor.decrypt failed: unknown key id "${keyId}"`);
    }
    if (stored.retiredAtMs !== undefined && nowMs - stored.retiredAtMs > this.graceMs) {
      throw new Error(
        `RotatingDekEncryptor.decrypt failed: key id "${keyId}" is retired (grace window elapsed)`,
      );
    }
    try {
      return stored.encryptor.decrypt(envelope);
    } catch (err) {
      throw new Error(
        `RotatingDekEncryptor.decrypt failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  /**
   * Generate a fresh random 32-byte DEK and make it the live key,
   * retiring the previous live key as of `nowMs`. Already-retired keys
   * keep their original `retiredAtMs` — rotation never extends a grace
   * window. Returns the new key id (`k<N>` when `newKeyId` is omitted,
   * smallest `N >= 1` not already taken).
   */
  rotate(newKeyId?: string, nowMs: number = Date.now()): string {
    let keyId = newKeyId;
    if (keyId === undefined) {
      let n = 1;
      const taken = new Set(this.keys.map((k) => k.keyId));
      while (taken.has(`k${n}`)) n += 1;
      keyId = `k${n}`;
    }
    assertValidKeyId(keyId, 'RotatingDekEncryptor.rotate');
    if (this.keys.some((k) => k.keyId === keyId)) {
      throw new RangeError(`RotatingDekEncryptor.rotate: keyId "${keyId}" is already held`);
    }
    for (const k of this.keys) {
      if (k.retiredAtMs === undefined) k.retiredAtMs = nowMs;
    }
    this.keys.unshift({ keyId, encryptor: new AesGcmEncryptor(randomAes256Key()) });
    return keyId;
  }

  /**
   * Replace the whole key set (see `EventBus.setTopicEncryptionKey`):
   * `keys[0]` becomes the live DEK; listed keys already held keep their
   * original `retiredAtMs`, newly listed non-live keys retire as of
   * `nowMs`; previously held keys the new set drops retire as of
   * `nowMs` (already-retired ones keep their stamp — re-setting never
   * extends a grace window); keys whose grace fully elapsed are pruned.
   * The same rotation-stamping convention as `signing.ts`.
   */
  rekey(keys: DekKeyInput[], nowMs: number = Date.now()): void {
    if (!Array.isArray(keys) || keys.length === 0) {
      throw new RangeError('RotatingDekEncryptor.rekey needs at least one DEK');
    }
    const seen = new Set<string>();
    keys.forEach((input, index) => {
      assertValidKeyId(input?.keyId, `RotatingDekEncryptor.rekey: keys[${index}]`);
      if (seen.has(input.keyId)) {
        throw new RangeError(`RotatingDekEncryptor.rekey: duplicate keyId "${input.keyId}"`);
      }
      seen.add(input.keyId);
    });
    const prevById = new Map(this.keys.map((k) => [k.keyId, k]));
    const next: StoredDek[] = keys.map((input, index) => {
      const prev = prevById.get(input.keyId);
      prevById.delete(input.keyId);
      const encryptor =
        prev !== undefined
          ? prev.encryptor
          : new AesGcmEncryptor(assertValidDekKey(input?.key, `RotatingDekEncryptor.rekey: keys[${index}]`));
      // Index 0 is the live key, unconditionally: a re-listed key is
      // promoted back to live (its old retirement stamp is cleared).
      // Every other listed key retires as of now unless it was already
      // retired earlier — re-setting never extends a grace window.
      const retiredAtMs =
        index === 0 ? undefined : (prev?.retiredAtMs ?? nowMs);
      return { keyId: input.keyId, encryptor, ...(retiredAtMs === undefined ? {} : { retiredAtMs }) };
    });
    for (const old of prevById.values()) {
      next.push({
        keyId: old.keyId,
        encryptor: old.encryptor,
        retiredAtMs: old.retiredAtMs ?? nowMs,
      });
    }
    this.keys.length = 0;
    for (const k of next) {
      if (k.retiredAtMs === undefined || nowMs - k.retiredAtMs <= this.graceMs) {
        this.keys.push(k);
      }
    }
    if (this.keys.length === 0) {
      // Cannot happen: keys[0] is always live (retiredAtMs undefined).
      throw new Error('RotatingDekEncryptor.rekey: internal error — live key pruned');
    }
  }

  /**
   * Drop retired keys whose grace window fully elapsed as of `nowMs`.
   * Never drops the live key. Returns how many were pruned.
   */
  pruneRetiredKeys(nowMs: number = Date.now()): number {
    let pruned = 0;
    for (let i = this.keys.length - 1; i >= 1; i -= 1) {
      const k = this.keys[i];
      if (k.retiredAtMs !== undefined && nowMs - k.retiredAtMs > this.graceMs) {
        this.keys.splice(i, 1);
        pruned += 1;
      }
    }
    return pruned;
  }
}

/**
 * Passthrough encryptor for tests and fixtures. The "ciphertext" is the
 * plaintext base64'd — it protects nothing. Never use in production.
 */
export class NoopEncryptor implements PayloadEncryptor {
  readonly name = 'none';

  encrypt(plaintext: Buffer): EncryptedPayload {
    return { alg: this.name, iv: '', data: plaintext.toString('base64') };
  }

  decrypt(envelope: EncryptedPayload): Buffer {
    if (!isEncryptedPayload(envelope) || envelope.alg !== this.name) {
      throw new Error(`NoopEncryptor: unsupported envelope alg: ${(envelope as { alg?: unknown }).alg}`);
    }
    return Buffer.from(envelope.data, 'base64');
  }
}
