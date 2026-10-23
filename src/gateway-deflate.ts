/**
 * WebSocket permessage-deflate (RFC 7692) for the gateway (EB-206).
 *
 * Zero new dependencies (`node:zlib` only).
 *
 * Negotiation model (server side, strict):
 *
 * - The client offers `permessage-deflate` in `Sec-WebSocket-Extensions`.
 *   The server selects it and answers with
 *   `permessage-deflate; client_no_context_takeover` plus any agreed
 *   window-bits parameters. **Client context takeover is never used**:
 *   the response always requires `client_no_context_takeover`, and the
 *   server decompresses every message with a fresh inflate context, so a
 *   client that reuses context across messages simply fails to decode.
 *   The server likewise never reuses its deflate context across
 *   messages (a client's `server_no_context_takeover` offer, when
 *   present, is echoed — we keep that promise for free).
 * - Window bits: `server_max_window_bits` / `client_max_window_bits`,
 *   default 15, validated 8–15 (per RFC 7692 §7.1.2). Out-of-range or
 *   non-numeric values, and any unknown parameter, make the offer
 *   invalid: `negotiateServerDeflate` throws and the handshake must
 *   fail rather than run on parameters the server cannot honor.
 * - Clients that do not offer the extension connect normally in
 *   plaintext (backwards compatible); a server with the extension
 *   disabled simply never selects it.
 *
 * Wire form (RFC 7692 §7.2.1): compress with raw DEFLATE ending in a
 * `Z_SYNC_FLUSH`, then strip the trailing empty stored block
 * (`00 00 ff ff`); set RSV1 on the frame. Decompression appends the
 * trailer back and inflates with a fresh context.
 *
 * Compression is opportunistic: payloads below `compressThresholdBytes`
 * go out untouched, and a payload whose compressed form is not smaller
 * than the original also goes out untouched — a failed deflate or a
 * non-shrinking result never breaks frame semantics, it just falls
 * back to plaintext.
 */

import {
  createDeflateRaw,
  createInflateRaw,
  constants as zlibConstants,
} from 'node:zlib';

/** The 4 trailer octets stripped after compression, re-appended before inflation. */
export const DEFLATE_TRAILER = Buffer.from([0x00, 0x00, 0xff, 0xff]);

/** Default payload size (bytes) at which permessage-deflate kicks in. */
export const DEFAULT_COMPRESS_THRESHOLD_BYTES = 1024;

/**
 * Default cap (bytes) on the inflated output of one decompressed
 * message: 8MiB sits far above any legitimate gateway envelope while
 * keeping a decompression bomb cheap to reject — the output
 * accumulator short-circuits instead of building an unbounded Buffer.
 */
export const DEFAULT_MAX_INFLATED_BYTES = 8 * 1024 * 1024;

/** Tuning for the gateway's permessage-deflate. `true` enables with defaults. */
export interface PerMessageDeflateOptions {
  /**
   * Payloads smaller than this are sent as-is. Default 1024. Must be a
   * positive integer.
   */
  compressThresholdBytes?: number;
  /**
   * LZ77 window bits the server uses when compressing to a client.
   * Default 15. Must be an integer 8–15 (per RFC 7692 §7.1.2).
   */
  serverMaxWindowBits?: number;
  /**
   * Maximum LZ77 window bits the server lets a client use when
   * compressing to us. Default 15. Must be an integer 8–15.
   */
  clientMaxWindowBits?: number;
  /**
   * Cap on the inflated bytes of one decompressed message
   * (decompression-bomb guard). Default 8MiB. Must be a positive
   * integer. The output accumulator checks every chunk and aborts the
   * inflate as soon as the running total passes the cap.
   */
  maxInflatedBytes?: number;
}

/** Validated deflate options, or `undefined` when the extension is off. */
export interface ResolvedPerMessageDeflate {
  compressThresholdBytes: number;
  serverMaxWindowBits: number;
  clientMaxWindowBits: number;
  maxInflatedBytes: number;
}

function fail(msg: string): never {
  throw new RangeError(`invalid permessageDeflate options: ${msg}`);
}

function checkWindowBits(name: string, value: unknown, fallback: number): number {
  if (value === undefined) return fallback;
  if (!Number.isInteger(value) || (value as number) < 8 || (value as number) > 15) {
    fail(`${name} must be an integer 8-15, got ${JSON.stringify(value)}`);
  }
  return value as number;
}

/**
 * Validate the `permessageDeflate` gateway option once, at construction.
 * `true` enables with defaults; `false`/`undefined` disables. Throws
 * `RangeError` on bad values.
 */
export function resolvePerMessageDeflate(
  opt: boolean | PerMessageDeflateOptions | undefined,
): ResolvedPerMessageDeflate | undefined {
  if (opt === undefined || opt === false) return undefined;
  if (opt === true) {
    return {
      compressThresholdBytes: DEFAULT_COMPRESS_THRESHOLD_BYTES,
      serverMaxWindowBits: 15,
      clientMaxWindowBits: 15,
      maxInflatedBytes: DEFAULT_MAX_INFLATED_BYTES,
    };
  }
  if (opt === null || typeof opt !== 'object' || Array.isArray(opt)) {
    fail('must be a boolean or an options object');
  }
  const o = opt as PerMessageDeflateOptions;
  const compressThresholdBytes =
    o.compressThresholdBytes === undefined
      ? DEFAULT_COMPRESS_THRESHOLD_BYTES
      : o.compressThresholdBytes;
  if (!Number.isInteger(compressThresholdBytes) || compressThresholdBytes <= 0) {
    fail(`compressThresholdBytes must be a positive integer, got ${JSON.stringify(o.compressThresholdBytes)}`);
  }
  const maxInflatedBytes =
    o.maxInflatedBytes === undefined ? DEFAULT_MAX_INFLATED_BYTES : o.maxInflatedBytes;
  if (!Number.isInteger(maxInflatedBytes) || maxInflatedBytes <= 0) {
    fail(`maxInflatedBytes must be a positive integer, got ${JSON.stringify(o.maxInflatedBytes)}`);
  }
  return {
    compressThresholdBytes,
    serverMaxWindowBits: checkWindowBits('serverMaxWindowBits', o.serverMaxWindowBits, 15),
    clientMaxWindowBits: checkWindowBits('clientMaxWindowBits', o.clientMaxWindowBits, 15),
    maxInflatedBytes,
  };
}

/** What the server selected for one connection's permessage-deflate. */
export interface NegotiatedServerDeflate {
  /**
   * Value of the `Sec-WebSocket-Extensions` response header. Always
   * carries `client_no_context_takeover`; window-bits parameters appear
   * only for the directions the client offered them in.
   */
  response: string;
  /** LZ77 window bits the server must use when compressing to this client. */
  serverWindowBits: number;
}

function negotiateFail(detail: string): never {
  throw new Error(`websocket: invalid permessage-deflate offer (${detail})`);
}

function parseWindowBitsValue(raw: string, param: string): number {
  if (raw === '') negotiateFail(`${param} requires a numeric value`);
  const bits = Number(raw);
  if (!Number.isInteger(bits) || bits < 8 || bits > 15) {
    negotiateFail(`${param}=${JSON.stringify(raw)} out of range 8-15`);
  }
  return bits;
}

/**
 * Parse the client's `Sec-WebSocket-Extensions` header and select
 * permessage-deflate for this connection.
 *
 * Returns `null` when the client did not offer permessage-deflate (the
 * connection then works uncompressed). Returns the negotiated
 * parameters when it did. Throws when the offer is invalid: an unknown
 * parameter, a missing/non-numeric/out-of-range window-bits value —
 * the handshake must fail rather than run on parameters the server
 * cannot honor.
 *
 * The first `permessage-deflate` offer in the header is evaluated;
 * offers are alternative proposals and only the selected one is
 * negotiated.
 */
export function negotiateServerDeflate(
  clientHeader: string | undefined,
  server: ResolvedPerMessageDeflate,
): NegotiatedServerDeflate | null {
  if (clientHeader === undefined || clientHeader.trim() === '') return null;
  // Split extensions on commas; parameter values here are simple tokens
  // or quoted strings without commas, so a plain split is sound.
  for (const ext of clientHeader.split(',')) {
    const tokens = ext.split(';').map((t) => t.trim());
    if (tokens[0].toLowerCase() !== 'permessage-deflate') continue;
    let serverNoTakeoverOffered = false;
    let serverWindowOffered = false;
    let serverWindowBits = server.serverMaxWindowBits;
    let clientWindowOffered = false;
    let clientWindowBits = server.clientMaxWindowBits;
    for (const token of tokens.slice(1)) {
      const eq = token.indexOf('=');
      const name = (eq < 0 ? token : token.slice(0, eq)).trim().toLowerCase();
      const rawValue = eq < 0 ? '' : token.slice(eq + 1).trim();
      // Strip one layer of DQUOTE, per RFC 6455 quoted-string values.
      const value =
        rawValue.length >= 2 && rawValue.startsWith('"') && rawValue.endsWith('"')
          ? rawValue.slice(1, -1)
          : rawValue;
      switch (name) {
        case 'client_no_context_takeover':
          // Always required by our response; an offered one is a no-op.
          if (eq >= 0) negotiateFail('client_no_context_takeover takes no value');
          break;
        case 'server_no_context_takeover':
          // We never reuse context, so we can echo this promise for free.
          if (eq >= 0) negotiateFail('server_no_context_takeover takes no value');
          serverNoTakeoverOffered = true;
          break;
        case 'server_max_window_bits':
          serverWindowOffered = true;
          serverWindowBits =
            eq < 0 ? server.serverMaxWindowBits : parseWindowBitsValue(value, name);
          break;
        case 'client_max_window_bits':
          clientWindowOffered = true;
          // The server must not answer above the client's offered value.
          clientWindowBits =
            eq < 0
              ? server.clientMaxWindowBits
              : Math.min(server.clientMaxWindowBits, parseWindowBitsValue(value, name));
          break;
        default:
          negotiateFail(`unknown parameter ${JSON.stringify(name)}`);
      }
    }
    const parts = ['permessage-deflate', 'client_no_context_takeover'];
    if (serverNoTakeoverOffered) parts.push('server_no_context_takeover');
    if (serverWindowOffered) parts.push(`server_max_window_bits=${serverWindowBits}`);
    if (clientWindowOffered) parts.push(`client_max_window_bits=${clientWindowBits}`);
    return { response: parts.join('; '), serverWindowBits };
  }
  return null;
}

/**
 * Compress one message per RFC 7692 §7.2.1: fresh raw-deflate context,
 * `Z_SYNC_FLUSH` terminator, trailing `00 00 ff ff` stripped. Never
 * reuses context across messages (the server requires
 * `client_no_context_takeover` and keeps none itself either). Resolves
 * to the exact wire bytes for an RSV1 frame.
 *
 * Implementation note (Node v24): `deflateRawSync` ignores the `flush`
 * option, so this streams: the write callback fires once the payload
 * reached the native layer and only then is the flush guaranteed to
 * cover all of it; the readable buffer is drained synchronously inside
 * the flush callback (data events race the callback); the stream is
 * destroyed before resolving — never read after destroy.
 */
export function compressMessage(payload: Buffer, windowBits = 15): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    if (!Number.isInteger(windowBits) || windowBits < 8 || windowBits > 15) {
      reject(new Error(`websocket: windowBits out of range: ${windowBits}`));
      return;
    }
    const deflate = createDeflateRaw({ windowBits });
    const chunks: Buffer[] = [];
    deflate.on('error', (err) => {
      deflate.destroy();
      reject(err);
    });
    deflate.write(payload, () => {
      deflate.flush(zlibConstants.Z_SYNC_FLUSH, () => {
        let chunk: Buffer | null;
        while ((chunk = deflate.read()) !== null) chunks.push(chunk);
        deflate.destroy();
        const full = Buffer.concat(chunks);
        // The sync flush always ends the stream with the empty stored
        // block 00 00 ff ff; strip it per RFC 7692 §7.2.1.
        resolve(full.subarray(0, full.length - DEFLATE_TRAILER.length));
      });
    });
  });
}

/**
 * One inflate's output passed `maxInflatedBytes`: the peer sent a
 * decompression bomb. The accumulator already destroyed the stream, so
 * the caller must fail the connection instead of retrying the message.
 */
export class WsInflateBombError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WsInflateBombError';
  }
}

/**
 * Decompress one RSV1 message: append the `00 00 ff ff` trailer stripped
 * by the compressor, then inflate with a fresh context (the server
 * requires `client_no_context_takeover`, so messages are independent).
 * Rejects on corrupt input — a tampered compressed frame fails the
 * delivery instead of producing garbage bytes.
 *
 * Decompression-bomb guard: `maxInflatedBytes` caps the inflated
 * output. Every output chunk is accounted during the accumulation
 * drain and the inflate is destroyed the moment the running total
 * passes the cap, so a small compressed frame that expands past it
 * never materializes as a full Buffer. Rejects with
 * {@link WsInflateBombError} in that case, `RangeError` on an illegal
 * cap.
 *
 * Never `end()` the inflater: ending Z_FINISHes it and rejects the
 * unterminated sync-flushed stream with Z_BUF_ERROR.
 */
export function decompressMessage(
  payload: Buffer,
  maxInflatedBytes: number = DEFAULT_MAX_INFLATED_BYTES,
): Promise<Buffer> {
  return new Promise<Buffer>((resolve, reject) => {
    if (!Number.isInteger(maxInflatedBytes) || maxInflatedBytes <= 0) {
      reject(
        new RangeError(
          `websocket: maxInflatedBytes must be a positive integer, got ${JSON.stringify(maxInflatedBytes)}`,
        ),
      );
      return;
    }
    const inflate = createInflateRaw();
    const chunks: Buffer[] = [];
    let inflatedBytes = 0;
    inflate.on('error', (err) => {
      inflate.destroy();
      reject(new Error(`websocket: permessage-deflate decompression failed: ${err.message}`));
    });
    // write() (not end()): ending the stream would Z_FINISH the inflate
    // and reject the unterminated sync-flushed stream with Z_BUF_ERROR.
    inflate.write(payload, () => {
      inflate.write(DEFLATE_TRAILER, () => {
        inflate.flush(zlibConstants.Z_SYNC_FLUSH, () => {
          let chunk: Buffer | null;
          // Drain synchronously: at flush completion every output byte
          // is already in the readable buffer. Account each chunk as it
          // lands — the bomb guard fires during accumulation, before
          // the pieces are concatenated.
          while ((chunk = inflate.read()) !== null) {
            inflatedBytes += chunk.length;
            if (inflatedBytes > maxInflatedBytes) {
              inflate.destroy();
              reject(
                new WsInflateBombError(
                  `websocket: permessage-deflate output exceeds maxInflatedBytes ${maxInflatedBytes}`,
                ),
              );
              return;
            }
            chunks.push(chunk);
          }
          inflate.destroy();
          resolve(Buffer.concat(chunks));
        });
      });
    });
  });
}

/**
 * Opportunistic compression for one outbound message: compress when the
 * payload reaches `thresholdBytes` *and* the compressed form is
 * strictly smaller — otherwise return the original bytes. A failed
 * deflate also falls back to the original: deflate must never expand
 * the wire and never break frame semantics.
 */
export async function maybeCompressMessage(
  payload: Buffer,
  thresholdBytes: number,
  windowBits = 15,
): Promise<{ bytes: Buffer; compressed: boolean }> {
  if (payload.length < thresholdBytes) return { bytes: payload, compressed: false };
  let compressed: Buffer;
  try {
    compressed = await compressMessage(payload, windowBits);
  } catch {
    return { bytes: payload, compressed: false };
  }
  if (compressed.length >= payload.length) return { bytes: payload, compressed: false };
  return { bytes: compressed, compressed: true };
}
