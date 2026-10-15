/**
 * WebSocket gateway (EB-99): exposes an `EventBus` over RFC 6455 WebSocket,
 * so remote processes (other languages, browsers, edge workers) can
 * publish and subscribe without embedding the bus.
 *
 * Zero new dependencies: `node:net` + `node:crypto` only. The server-side
 * frame codec is adapted from the client-side codec in webhook-relay-ts
 * (`src/websocket.ts`, WR-45): same opcodes, same masking rules (clients
 * mask, servers never do), same incremental parsing.
 *
 * Wire protocol — every frame is a JSON text frame:
 *
 *   client -> server
 *   - `{type:'subscribe', id, pattern, opts?}` — subscribes `pattern`
 *     (EB-41 ACL enforced, `AclDeniedError` -> error frame). `opts` is a
 *     `SubscribeOptions` passthrough, so `resumeFromSeq` / `resumeFromTime`
 *     (EB-17/EB-42) work for reconnecting clients, as do namespaces
 *     (EB-52). `opts.reliable` (gateway-local, stripped before the bus
 *     sees it) routes through `subscribeReliable` for at-least-once
 *     delivery; each message then carries a `deliveryId` the client must
 *     `ack` / `nack`. `opts.binary` (gateway-local, stripped before the
 *     bus sees it) switches this subscription's deliveries to binary
 *     frames (see below). Function-valued options (filters, hooks) cannot
 *     cross the wire and are ignored.
 *   - `{type:'unsubscribe', id, subId}`
 *   - `{type:'publish', id, topic, payload, opts?}` — full admission
 *     pipeline (ACL, schema, rate-limit, TTL, dedup): `publish` when
 *     `opts.messageId` is absent, `publishIdempotent` (EB-27) when present.
 *   - `{type:'publishBatch', id, entries:[{topic,payload,opts?}]}` —
 *     `publishBatch` (EB-03).
 *   - `{type:'ack', deliveryId}` / `{type:'nack', deliveryId}` — settle a
 *     reliable delivery. Unknown ids are ignored: stale acks are normal
 *     under at-least-once semantics.
 *   - `{type:'ping', id?}` — answered with `{type:'pong', id?}`.
 *
 *   Binary frames (EB-109): a binary frame is
 *   `[u32 BE headerLen][UTF-8 JSON header][raw payload bytes]`.
 *
 *   client -> server (binary)
 *   - `publish`: header `{type:'publish', id?, topic, opts?}`, body = raw
 *     payload bytes. Published via `bus.publish(topic, bytes,
 *     {...opts, binary: true})` (EB-92), so the whole admission pipeline
 *     runs on the raw bytes: byte validators (EB-20), the byte-size cap
 *     (EB-64) and compression threshold (EB-22) measure wire bytes, and
 *     the HMAC topic signature (EB-84) covers those same wire bytes.
 *     `opts.messageId` routes through `publishIdempotent` like the text
 *     form. Answered with the text frame
 *     `{type:'published', id, delivered, duplicate?}`.
 *
 *   server -> client (binary): only for subscriptions whose `subscribe`
 *   carried `opts.binary: true`. Each delivery is a binary frame whose
 *   header is `{type:'message', subId, topic, seq,
 *   encoding: 'binary'|'json', deliveryId?, redeliveries?}`:
 *   - `encoding: 'binary'` — the message was a binary (EB-92) publish and
 *     the body is its raw wire bytes, untouched (compressed binary
 *     messages are inflated by the bus before the gateway sees them).
 *   - `encoding: 'json'` — any other payload, UTF-8 JSON-encoded, so a
 *     binary subscriber on a mixed topic never breaks.
 *   Malformed binary frames get an `error` frame (`bad_frame` /
 *   `unknown_frame` / `invalid_request`); the connection stays open.
 *   `maxFrameBytes` still bounds binary frames (1009 close past it), and
 *   the backpressure queue (`maxPendingBytes`) counts binary frames by
 *   their byte size exactly like text frames.
 *
 *   server -> client
 *   - `{type:'subscribed', id, subId}` / `{type:'unsubscribed', id, subId}`
 *   - `{type:'message', subId, topic, payload, seq}` — live delivery;
 *     reliable deliveries add `deliveryId` and `redeliveries`.
 *   - `{type:'published', id, delivered, duplicate?}` — `delivered` is
 *     the subscriber count (0 when admission refused the message);
 *     `duplicate` reports EB-27 suppression.
 *   - `{type:'error', id?, code, message}` — `code` is one of
 *     `auth_failed | bad_frame | unknown_frame | invalid_request |
 *     subscribe_denied | subscribe_failed | publish_failed | not_found`.
 *
 * Authentication is opt-in: with `authToken` set, the handshake must
 * carry `Authorization: Bearer <token>` (constant-time compare), otherwise
 * the server answers 401 and drops the connection.
 *
 * Backpressure (server -> client): frames are written while the socket
 * buffer stays under `sendHighWaterMarkBytes`; past it the connection
 * pauses and frames accumulate in a bounded per-connection queue. When
 * the queue exceeds `maxPendingBytes` the newest frame is dropped and
 * counted in `droppedFrames` — the gateway sheds load instead of growing
 * memory without bound. `GatewayServer.getStats()` reports
 * `{connections, framesReceived, framesSent, droppedFrames,
 * authFailures, protocolErrors}`.
 */
import {
  createServer as createTcpServer,
  type Server as TcpServer,
  type Socket,
} from 'node:net';
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import {
  AclDeniedError,
  EventBus,
  type BatchMessage,
  type BusMessage,
  type Delivery,
  type IdempotentPublishOptions,
  type PublishOptions,
  type ReliableSubscribeOptions,
  type SubscribeOptions,
  type Subscription,
} from './bus.ts';

// --- RFC 6455 codec (server side) ------------------------------------------

const OP_CONT = 0x0;
const OP_TEXT = 0x1;
const OP_BINARY = 0x2;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xa;
const WS_GUID = '258EAFA5-E91447DA-C5AB0DC85B11';

function wsAcceptKey(key: string): string {
  return createHash('sha1').update(key + WS_GUID).digest('base64');
}

/** Encode one server-to-client frame: servers never mask (RFC 6455 §5.3). */
function encodeFrame(opcode: number, payload: Buffer): Buffer {
  const len = payload.length;
  const b0 = 0x80 | opcode;
  let header: Buffer;
  if (len < 126) {
    header = Buffer.from([b0, len]);
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = b0;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = b0;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

interface WsMessage {
  opcode: number;
  payload: Buffer;
}

/**
 * Binary frame envelope (EB-109): `[u32 BE headerLen][UTF-8 JSON
 * header][raw payload bytes]`. `encodeBinaryFrame` builds one;
 * `parseBinaryFrame` splits one, throwing on a truncated or
 * out-of-bounds header length (the caller turns that into a `bad_frame`
 * error — the connection stays open).
 */
function encodeBinaryFrame(header: object, body: Buffer): Buffer {
  const headerBytes = Buffer.from(JSON.stringify(header), 'utf8');
  const out = Buffer.alloc(4 + headerBytes.length + body.length);
  out.writeUInt32BE(headerBytes.length, 0);
  headerBytes.copy(out, 4);
  body.copy(out, 4 + headerBytes.length);
  return out;
}

function parseBinaryFrame(payload: Buffer): { header: unknown; body: Buffer } {
  if (payload.length < 4) throw new Error('binary frame too short for header length');
  const headerLen = payload.readUInt32BE(0);
  if (headerLen > payload.length - 4) throw new Error('binary frame header length out of bounds');
  const header = JSON.parse(payload.subarray(4, 4 + headerLen).toString('utf8'));
  // Copy the body: the parser's chunk buffer is transient, and a queued
  // message must not pin it.
  return { header, body: Buffer.from(payload.subarray(4 + headerLen)) };
}

/**
 * Encodes one outbound binary message body (EB-109). A binary (EB-92)
 * publish's `Uint8Array` payload travels as raw wire bytes
 * (`encoding: 'binary'`); anything else is UTF-8 JSON-encoded
 * (`encoding: 'json'`) so a binary subscriber on a mixed topic never
 * breaks. Unserializable payloads encode as `null` — the bus never
 * delivers those to a handler anyway, this is a wire-level fallback.
 */
function encodeBinaryMessageBody(payload: unknown): { bytes: Buffer; encoding: 'binary' | 'json' } {
  if (payload instanceof Uint8Array) {
    return { bytes: Buffer.from(payload), encoding: 'binary' };
  }
  const serialized = JSON.stringify(payload);
  return { bytes: Buffer.from(serialized ?? 'null', 'utf8'), encoding: 'json' };
}

/**
 * Incremental frame parser with fragmentation assembly: feed socket
 * chunks, pull out complete messages. Masked inbound frames are unmasked
 * (clients must mask; unmasked frames are tolerated). Throws on frames
 * whose declared length exceeds `maxFrameBytes`.
 */
class FrameParser {
  private buf = Buffer.alloc(0);
  private fragments: Buffer[] = [];
  private fragmentOpcode = -1;
  private readonly maxFrameBytes: number;

  constructor(maxFrameBytes: number) {
    this.maxFrameBytes = maxFrameBytes;
  }

  push(chunk: Buffer): WsMessage[] {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    const out: WsMessage[] = [];
    for (;;) {
      const frame = this.nextFrame();
      if (frame === undefined) return out;
      const { fin, opcode, payload } = frame;
      if (opcode === OP_CONT) {
        if (this.fragmentOpcode === -1) throw new Error('websocket: unexpected continuation');
        this.fragments.push(payload);
        if (fin) {
          out.push({ opcode: this.fragmentOpcode, payload: Buffer.concat(this.fragments) });
          this.fragments = [];
          this.fragmentOpcode = -1;
        }
        continue;
      }
      if (!fin && (opcode === OP_TEXT || opcode === OP_BINARY)) {
        this.fragments = [payload];
        this.fragmentOpcode = opcode;
        continue;
      }
      out.push({ opcode, payload });
    }
  }

  private nextFrame(): { fin: boolean; opcode: number; payload: Buffer } | undefined {
    const b = this.buf;
    if (b.length < 2) return undefined;
    const fin = (b[0] & 0x80) !== 0;
    const rsv = b[0] & 0x70;
    if (rsv !== 0) throw new Error('websocket: RSV bits set without negotiated extension');
    const opcode = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f;
    let off = 2;
    if (len === 126) {
      if (b.length < 4) return undefined;
      len = b.readUInt16BE(2);
      off = 4;
    } else if (len === 127) {
      if (b.length < 10) return undefined;
      const hi = b.readUInt32BE(2);
      const lo = b.readUInt32BE(6);
      if (hi !== 0 || lo > 0x7fffffff) throw new Error('websocket: frame too large');
      len = lo;
      off = 10;
    }
    if (len > this.maxFrameBytes) throw new Error('websocket: frame exceeds maxFrameBytes');
    const maskOff = masked ? 4 : 0;
    if (b.length < off + maskOff + len) return undefined;
    let payload = b.subarray(off + maskOff, off + maskOff + len);
    if (masked) {
      const mask = b.subarray(off, off + 4);
      const unmasked = Buffer.alloc(len);
      for (let i = 0; i < len; i++) unmasked[i] = payload[i] ^ mask[i % 4];
      payload = unmasked;
    }
    this.buf = b.subarray(off + maskOff + len);
    return { fin, opcode, payload };
  }
}

// --- Public types -----------------------------------------------------------

/** Client -> server `subscribe` options: `SubscribeOptions` passthrough. */
export interface GatewaySubscribeOptions extends SubscribeOptions {
  /**
   * Gateway-local: route through `subscribeReliable` for at-least-once
   * delivery (`true` for defaults, or `ReliableSubscribeOptions`).
   * Stripped before the bus sees the options.
   */
  reliable?: boolean | ReliableSubscribeOptions;
  /**
   * Gateway-local: deliver this subscription's messages as binary frames
   * (EB-109) instead of JSON text frames. Stripped before the bus sees
   * the options.
   */
  binary?: boolean;
}

/** Client -> server `publish` options: `PublishOptions` passthrough. */
export interface GatewayPublishOptions extends PublishOptions {
  /**
   * When a non-empty string, the publish goes through
   * `publishIdempotent` (EB-27) instead of `publish` and the reply
   * carries `duplicate`.
   */
  messageId?: string;
}

export interface GatewayOptions {
  host?: string;
  port?: number;
  /**
   * Opt-in connection-level bearer auth. When set, the handshake must
   * carry `Authorization: Bearer <token>`; otherwise the server answers
   * 401 and drops the connection.
   */
  authToken?: string;
  /**
   * Socket-buffer bytes past which a connection pauses pushing frames
   * and queues them locally instead. Default 256 KiB.
   */
  sendHighWaterMarkBytes?: number;
  /**
   * Cap on the per-connection local queue while paused; past it the
   * newest frame is dropped and counted. Default 1 MiB.
   */
  maxPendingBytes?: number;
  /**
   * Largest single inbound frame accepted. Past it the connection is
   * closed with 1009. Default 1 MiB.
   */
  maxFrameBytes?: number;
}

export interface GatewayStats {
  connections: number;
  framesReceived: number;
  framesSent: number;
  /** Frames dropped (newest-first) while a connection's queue overflowed. */
  droppedFrames: number;
  authFailures: number;
  protocolErrors: number;
}

interface ConnectionSubscription {
  sub: Subscription;
  pendingAcks: Map<number, Delivery<BusMessage>>;
  /** Whether this subscription's deliveries go out as binary frames. */
  binary: boolean;
}

interface Connection {
  socket: Socket;
  parser: FrameParser;
  handshook: boolean;
  closed: boolean;
  paused: boolean;
  pending: Buffer[];
  pendingBytes: number;
  subscriptions: Map<string, ConnectionSubscription>;
}

type ErrorCode =
  | 'auth_failed'
  | 'bad_frame'
  | 'unknown_frame'
  | 'invalid_request'
  | 'subscribe_denied'
  | 'subscribe_failed'
  | 'publish_failed'
  | 'not_found';

const HANDSHAKE_TIMEOUT_MS = 10_000;
const MAX_HANDSHAKE_BYTES = 8 * 1024;

function fail(msg: string): never {
  throw new RangeError(`invalid gateway options: ${msg}`);
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null;
}

// --- Gateway -----------------------------------------------------------------

export class GatewayServer {
  private readonly bus: EventBus;
  private readonly host: string;
  private readonly port: number;
  private readonly authToken: string | undefined;
  private readonly sendHighWaterMarkBytes: number;
  private readonly maxPendingBytes: number;
  private readonly maxFrameBytes: number;
  private server: TcpServer | null = null;
  private readonly connections = new Set<Connection>();
  private readonly stats: GatewayStats = {
    connections: 0,
    framesReceived: 0,
    framesSent: 0,
    droppedFrames: 0,
    authFailures: 0,
    protocolErrors: 0,
  };

  constructor(bus: EventBus, options: GatewayOptions = {}) {
    this.bus = bus;
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 0;
    const authToken = options.authToken;
    if (authToken !== undefined && (typeof authToken !== 'string' || authToken.length === 0)) {
      fail('authToken must be a non-empty string');
    }
    this.authToken = authToken;
    const sendHighWaterMarkBytes = options.sendHighWaterMarkBytes ?? 256 * 1024;
    if (!Number.isFinite(sendHighWaterMarkBytes) || sendHighWaterMarkBytes <= 0) {
      fail('sendHighWaterMarkBytes must be a positive finite number');
    }
    this.sendHighWaterMarkBytes = sendHighWaterMarkBytes;
    const maxPendingBytes = options.maxPendingBytes ?? 1024 * 1024;
    if (!Number.isFinite(maxPendingBytes) || maxPendingBytes <= 0) {
      fail('maxPendingBytes must be a positive finite number');
    }
    this.maxPendingBytes = maxPendingBytes;
    const maxFrameBytes = options.maxFrameBytes ?? 1024 * 1024;
    if (!Number.isFinite(maxFrameBytes) || maxFrameBytes <= 0) {
      fail('maxFrameBytes must be a positive finite number');
    }
    this.maxFrameBytes = maxFrameBytes;
  }

  /** Starts listening; resolves with the bound port. */
  listen(): Promise<number> {
    if (this.server != null) throw new Error('GatewayServer is already listening');
    return new Promise((resolve, reject) => {
      const server = createTcpServer((socket) => this.handleConnection(socket));
      this.server = server;
      server.once('error', reject);
      server.listen(this.port, this.host, () => {
        server.off('error', reject);
        const addr = server.address();
        const boundPort = typeof addr === 'object' && addr != null ? addr.port : this.port;
        resolve(boundPort);
      });
    });
  }

  /** Drops every connection (unsubscribing its bus subscriptions) and stops. */
  async close(): Promise<void> {
    for (const conn of this.connections) this.destroyConnection(conn);
    this.connections.clear();
    const server = this.server;
    this.server = null;
    if (server == null) return;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }

  getStats(): GatewayStats {
    return { ...this.stats };
  }

  // --- connection lifecycle ------------------------------------------------

  private handleConnection(socket: Socket): void {
    socket.setNoDelay(true);
    const conn: Connection = {
      socket,
      parser: new FrameParser(this.maxFrameBytes),
      handshook: false,
      closed: false,
      paused: false,
      pending: [],
      pendingBytes: 0,
      subscriptions: new Map(),
    };
    this.connections.add(conn);
    this.stats.connections += 1;
    let handshakeBuf = Buffer.alloc(0);
    const handshakeTimer = setTimeout(() => {
      if (!conn.handshook) this.destroyConnection(conn);
    }, HANDSHAKE_TIMEOUT_MS);
    if (typeof handshakeTimer === 'object' && 'unref' in handshakeTimer) {
      (handshakeTimer as { unref(): void }).unref();
    }

    socket.on('data', (chunk: Buffer) => {
      if (conn.closed) return;
      if (!conn.handshook) {
        handshakeBuf = Buffer.concat([handshakeBuf, chunk]);
        if (handshakeBuf.length > MAX_HANDSHAKE_BYTES) {
          this.destroyConnection(conn);
          return;
        }
        const end = handshakeBuf.indexOf('\r\n\r\n');
        if (end === -1) return;
        const head = handshakeBuf.subarray(0, end).toString('latin1');
        handshakeBuf = Buffer.alloc(0);
        clearTimeout(handshakeTimer);
        this.handleHandshake(conn, head);
        return;
      }
      let messages: WsMessage[] = [];
      try {
        messages = conn.parser.push(chunk);
      } catch {
        this.stats.protocolErrors += 1;
        this.closeWithCode(conn, 1009, 'frame too large');
        return;
      }
      for (const msg of messages) this.handleWsMessage(conn, msg);
    });
    socket.on('drain', () => {
      if (conn.closed || conn.handshook === false) return;
      conn.paused = false;
      this.flushPending(conn);
    });
    socket.on('error', () => {});
    socket.on('close', () => this.destroyConnection(conn));
  }

  private handleHandshake(conn: Connection, head: string): void {
    const lines = head.split('\r\n');
    const requestLine = lines[0] ?? '';
    const headers = new Map<string, string>();
    for (const line of lines.slice(1)) {
      const idx = line.indexOf(':');
      if (idx === -1) continue;
      headers.set(line.slice(0, idx).trim().toLowerCase(), line.slice(idx + 1).trim());
    }
    const reject = (status: string) => {
      conn.socket.write(`HTTP/1.1 ${status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
      this.destroyConnection(conn);
    };
    if (!/^GET \S+ HTTP\/1\.1$/.test(requestLine)) return reject('400 Bad Request');
    const upgrade = headers.get('upgrade') ?? '';
    if (!upgrade.toLowerCase().split(',').map((s) => s.trim()).includes('websocket')) {
      return reject('400 Bad Request');
    }
    if ((headers.get('sec-websocket-version') ?? '').trim() !== '13') {
      return reject('426 Upgrade Required');
    }
    const key = (headers.get('sec-websocket-key') ?? '').trim();
    let keyBytes: Buffer;
    try {
      keyBytes = Buffer.from(key, 'base64');
    } catch {
      return reject('400 Bad Request');
    }
    if (keyBytes.length !== 16) return reject('400 Bad Request');
    if (this.authToken !== undefined) {
      const presented = headers.get('authorization') ?? '';
      const expected = `Bearer ${this.authToken}`;
      const ok =
        presented.length === expected.length &&
        timingSafeEqual(Buffer.from(presented), Buffer.from(expected));
      if (!ok) {
        this.stats.authFailures += 1;
        return reject('401 Unauthorized');
      }
    }
    conn.socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${wsAcceptKey(key)}\r\n` +
        '\r\n',
    );
    conn.handshook = true;
  }

  private destroyConnection(conn: Connection): void {
    if (conn.closed) return;
    conn.closed = true;
    this.connections.delete(conn);
    for (const { sub } of conn.subscriptions.values()) {
      try {
        sub.unsubscribe();
      } catch {
        // Unsubscribe is idempotent-safe; never let teardown throw.
      }
    }
    conn.subscriptions.clear();
    conn.pending = [];
    conn.pendingBytes = 0;
    try {
      conn.socket.destroy();
    } catch {
      // Already gone.
    }
  }

  private closeWithCode(conn: Connection, code: number, reason: string): void {
    const payload = Buffer.alloc(2 + Buffer.byteLength(reason));
    payload.writeUInt16BE(code, 0);
    payload.write(reason, 2);
    try {
      conn.socket.write(encodeFrame(OP_CLOSE, payload));
    } catch {
      // Best effort; the destroy below finishes the job.
    }
    this.destroyConnection(conn);
  }

  // --- outbound -------------------------------------------------------------

  private sendJson(conn: Connection, value: object): void {
    this.sendRaw(conn, encodeFrame(OP_TEXT, Buffer.from(JSON.stringify(value), 'utf8')));
  }

  private sendError(conn: Connection, code: ErrorCode, message: string, id?: unknown): void {
    this.sendJson(conn, { type: 'error', id, code, message });
  }

  /**
   * Writes a frame honoring backpressure: while the socket buffer is past
   * the high-water mark (or a previous write left us paused), frames queue
   * locally; past `maxPendingBytes` the newest frame is dropped and
   * counted. `drain` resumes the queue.
   */
  private sendRaw(conn: Connection, frame: Buffer): void {
    if (conn.closed) return;
    if (!conn.paused && conn.socket.bufferSize <= this.sendHighWaterMarkBytes) {
      let ok = true;
      try {
        ok = conn.socket.write(frame);
      } catch {
        this.destroyConnection(conn);
        return;
      }
      this.stats.framesSent += 1;
      if (!ok || conn.socket.bufferSize > this.sendHighWaterMarkBytes) conn.paused = true;
      return;
    }
    conn.paused = true;
    if (conn.pendingBytes + frame.length > this.maxPendingBytes) {
      this.stats.droppedFrames += 1;
      return;
    }
    conn.pending.push(frame);
    conn.pendingBytes += frame.length;
  }

  private flushPending(conn: Connection): void {
    while (conn.pending.length > 0 && !conn.paused && !conn.closed) {
      const frame = conn.pending.shift()!;
      conn.pendingBytes -= frame.length;
      let ok = true;
      try {
        ok = conn.socket.write(frame);
      } catch {
        this.destroyConnection(conn);
        return;
      }
      this.stats.framesSent += 1;
      if (!ok || conn.socket.bufferSize > this.sendHighWaterMarkBytes) conn.paused = true;
    }
  }

  // --- inbound ---------------------------------------------------------------

  private handleWsMessage(conn: Connection, msg: WsMessage): void {
    if (conn.closed) return;
    switch (msg.opcode) {
      case OP_TEXT: {
        this.stats.framesReceived += 1;
        let frame: unknown;
        try {
          frame = JSON.parse(msg.payload.toString('utf8'));
        } catch {
          this.stats.protocolErrors += 1;
          this.sendError(conn, 'bad_frame', 'frame is not valid JSON');
          return;
        }
        this.handleFrame(conn, frame);
        return;
      }
      case OP_BINARY: {
        this.stats.framesReceived += 1;
        let header: unknown;
        let body: Buffer;
        try {
          ({ header, body } = parseBinaryFrame(msg.payload));
        } catch {
          this.stats.protocolErrors += 1;
          this.sendError(conn, 'bad_frame', 'malformed binary frame');
          return;
        }
        this.handleBinaryFrame(conn, header, body);
        return;
      }
      case OP_PING:
        try {
          conn.socket.write(encodeFrame(OP_PONG, msg.payload));
        } catch {
          this.destroyConnection(conn);
        }
        return;
      case OP_PONG:
        return;
      case OP_CLOSE:
        this.destroyConnection(conn);
        return;
      default:
        this.stats.protocolErrors += 1;
        this.closeWithCode(conn, 1002, 'unknown opcode');
    }
  }

  private handleFrame(conn: Connection, frame: unknown): void {
    if (!isRecord(frame) || typeof frame.type !== 'string') {
      this.sendError(conn, 'bad_frame', 'frame must be an object with a string type');
      return;
    }
    switch (frame.type) {
      case 'subscribe':
        this.handleSubscribe(conn, frame);
        return;
      case 'unsubscribe':
        this.handleUnsubscribe(conn, frame);
        return;
      case 'publish':
        this.handlePublish(conn, frame);
        return;
      case 'publishBatch':
        this.handlePublishBatch(conn, frame);
        return;
      case 'ack':
        this.handleSettle(conn, frame, true);
        return;
      case 'nack':
        this.handleSettle(conn, frame, false);
        return;
      case 'ping':
        this.sendJson(conn, { type: 'pong', id: frame.id });
        return;
      default:
        this.sendError(conn, 'unknown_frame', `unknown frame type: ${frame.type}`, frame.id);
    }
  }

  private handleSubscribe(conn: Connection, frame: Record<string, unknown>): void {
    const { id, pattern, opts } = frame;
    if (typeof pattern !== 'string' || pattern.length === 0) {
      this.sendError(conn, 'invalid_request', 'subscribe requires a non-empty pattern', id);
      return;
    }
    if (opts !== undefined && !isRecord(opts)) {
      this.sendError(conn, 'invalid_request', 'subscribe opts must be an object', id);
      return;
    }
    const wireOpts = (opts ?? {}) as GatewaySubscribeOptions;
    const { reliable, binary, ...subOpts } = wireOpts;
    const binaryDelivery = binary === true;
    // Retained snapshots (EB-87) invoke the handler synchronously inside
    // `subscribe()` — before the `Subscription` (and its id) exists. Live
    // deliveries arrive later via the async flush. A `registering` flag
    // routes synchronous deliveries into an outbox that is flushed with
    // the real subId right after registration, so wire order stays
    // snapshot-then-`subscribed`-then-live.
    interface OutboxItem {
      topic: string;
      payload: unknown;
      seq: number;
      delivery?: Delivery<BusMessage>;
      redeliveries?: number;
    }
    const outbox: OutboxItem[] = [];
    let registering = true;
    const pendingAcks = new Map<number, Delivery<BusMessage>>();
    const emitLive = (item: OutboxItem, subId: string): void => {
      if (binaryDelivery) {
        const { bytes, encoding } = encodeBinaryMessageBody(item.payload);
        this.sendRaw(
          conn,
          encodeFrame(
            OP_BINARY,
            encodeBinaryFrame(
              {
                type: 'message',
                subId,
                topic: item.topic,
                seq: item.seq,
                encoding,
                ...(item.delivery !== undefined
                  ? { deliveryId: item.delivery.seq, redeliveries: item.redeliveries }
                  : {}),
              },
              bytes,
            ),
          ),
        );
        return;
      }
      this.sendJson(conn, {
        type: 'message',
        subId,
        ...(item.delivery !== undefined
          ? { deliveryId: item.delivery.seq, redeliveries: item.redeliveries }
          : {}),
        topic: item.topic,
        payload: item.payload,
        seq: item.seq,
      });
    };
    const onMessage = (item: OutboxItem, subId: string | undefined): void => {
      if (registering || subId === undefined) {
        outbox.push(item);
        return;
      }
      emitLive(item, subId);
    };
    let sub: Subscription;
    try {
      if (reliable !== undefined && reliable !== false) {
        const reliableOpts: ReliableSubscribeOptions =
          reliable === true ? {} : (reliable as ReliableSubscribeOptions);
        const merged: ReliableSubscribeOptions = { ...subOpts, ...reliableOpts };
        let subId: string | undefined;
        sub = this.bus.subscribeReliable(
          pattern,
          (delivery: Delivery<BusMessage>) => {
            pendingAcks.set(delivery.seq, delivery);
            onMessage(
              {
                topic: delivery.msg.topic,
                payload: delivery.msg.payload,
                seq: delivery.msg.seq,
                delivery,
                redeliveries: delivery.redeliveries,
              },
              subId,
            );
          },
          merged,
        );
        subId = sub.id;
      } else {
        let subId: string | undefined;
        sub = this.bus.subscribe(
          pattern,
          (msg: BusMessage) => {
            onMessage({ topic: msg.topic, payload: msg.payload, seq: msg.seq }, subId);
          },
          subOpts,
        );
        subId = sub.id;
      }
    } catch (err) {
      if (err instanceof AclDeniedError) {
        this.sendError(conn, 'subscribe_denied', (err as Error).message, id);
      } else if (err instanceof RangeError || err instanceof TypeError) {
        this.sendError(conn, 'invalid_request', (err as Error).message, id);
      } else {
        this.sendError(conn, 'subscribe_failed', (err as Error).message, id);
      }
      return;
    }
    registering = false;
    conn.subscriptions.set(sub.id, { sub, pendingAcks, binary: binaryDelivery });
    for (const item of outbox) emitLive(item, sub.id);
    this.sendJson(conn, { type: 'subscribed', id, subId: sub.id });
  }

  private handleUnsubscribe(conn: Connection, frame: Record<string, unknown>): void {
    const { id, subId } = frame;
    if (typeof subId !== 'string') {
      this.sendError(conn, 'invalid_request', 'unsubscribe requires a subId', id);
      return;
    }
    const entry = conn.subscriptions.get(subId);
    if (entry === undefined) {
      this.sendError(conn, 'not_found', `unknown subscription: ${subId}`, id);
      return;
    }
    conn.subscriptions.delete(subId);
    try {
      entry.sub.unsubscribe();
    } catch (err) {
      this.sendError(conn, 'subscribe_failed', (err as Error).message, id);
      return;
    }
    this.sendJson(conn, { type: 'unsubscribed', id, subId });
  }

  private handlePublish(conn: Connection, frame: Record<string, unknown>): void {
    const { id, topic, payload } = frame;
    const opts = frame.opts;
    if (typeof topic !== 'string' || topic.length === 0) {
      this.sendError(conn, 'invalid_request', 'publish requires a non-empty topic', id);
      return;
    }
    if (opts !== undefined && !isRecord(opts)) {
      this.sendError(conn, 'invalid_request', 'publish opts must be an object', id);
      return;
    }
    const publishOpts = (opts ?? {}) as GatewayPublishOptions;
    let delivered = 0;
    let duplicate = false;
    try {
      if (typeof publishOpts.messageId === 'string' && publishOpts.messageId.length > 0) {
        const res = this.bus.publishIdempotent(
          topic,
          payload,
          publishOpts as IdempotentPublishOptions,
        );
        delivered = res.accepted;
        duplicate = res.duplicate;
      } else {
        delivered = this.bus.publish(topic, payload, publishOpts as PublishOptions);
      }
    } catch (err) {
      if (err instanceof RangeError || err instanceof TypeError) {
        this.sendError(conn, 'invalid_request', (err as Error).message, id);
      } else {
        this.sendError(conn, 'publish_failed', (err as Error).message, id);
      }
      return;
    }
    this.sendJson(conn, { type: 'published', id, delivered, duplicate });
  }

  private handleBinaryFrame(conn: Connection, header: unknown, body: Buffer): void {
    if (!isRecord(header) || typeof header.type !== 'string') {
      this.sendError(conn, 'bad_frame', 'binary frame header must be an object with a string type');
      return;
    }
    if (header.type !== 'publish') {
      this.sendError(conn, 'unknown_frame', `unknown binary frame type: ${header.type}`);
      return;
    }
    this.handleBinaryPublish(conn, header, body);
  }

  /**
   * Binary `publish` (EB-109): the frame body is the raw payload bytes,
   * published with `binary: true` (EB-92) so the full admission pipeline
   * — schema, rate-limit, TTL, idempotent dedup — runs on the wire bytes:
   * byte validators see the bytes, the byte-size cap and compression
   * threshold measure them, and the HMAC topic signature (EB-84) covers
   * them. Answered with the same text `published` frame as text publish.
   */
  private handleBinaryPublish(conn: Connection, header: Record<string, unknown>, body: Buffer): void {
    const { id, topic, opts } = header;
    if (typeof topic !== 'string' || topic.length === 0) {
      this.sendError(conn, 'invalid_request', 'publish requires a non-empty topic', id);
      return;
    }
    if (opts !== undefined && !isRecord(opts)) {
      this.sendError(conn, 'invalid_request', 'publish opts must be an object', id);
      return;
    }
    const publishOpts: GatewayPublishOptions = { ...((opts ?? {}) as GatewayPublishOptions), binary: true };
    let delivered = 0;
    let duplicate = false;
    try {
      if (typeof publishOpts.messageId === 'string' && publishOpts.messageId.length > 0) {
        const res = this.bus.publishIdempotent(
          topic,
          body,
          publishOpts as IdempotentPublishOptions,
        );
        delivered = res.accepted;
        duplicate = res.duplicate;
      } else {
        delivered = this.bus.publish(topic, body, publishOpts as PublishOptions);
      }
    } catch (err) {
      if (err instanceof RangeError || err instanceof TypeError) {
        this.sendError(conn, 'invalid_request', (err as Error).message, id);
      } else {
        this.sendError(conn, 'publish_failed', (err as Error).message, id);
      }
      return;
    }
    this.sendJson(conn, { type: 'published', id, delivered, duplicate });
  }

  private handlePublishBatch(conn: Connection, frame: Record<string, unknown>): void {
    const { id, entries } = frame;
    if (!Array.isArray(entries) || entries.length === 0) {
      this.sendError(conn, 'invalid_request', 'publishBatch requires a non-empty entries array', id);
      return;
    }
    const messages: BatchMessage[] = [];
    for (const entry of entries) {
      if (
        !isRecord(entry) ||
        typeof entry.topic !== 'string' ||
        entry.topic.length === 0 ||
        (entry.opts !== undefined && !isRecord(entry.opts))
      ) {
        this.sendError(conn, 'invalid_request', 'each entry needs a non-empty topic', id);
        return;
      }
      messages.push({ topic: entry.topic, payload: entry.payload, opts: entry.opts as PublishOptions });
    }
    let delivered = 0;
    try {
      delivered = this.bus.publishBatch(messages);
    } catch (err) {
      if (err instanceof RangeError || err instanceof TypeError) {
        this.sendError(conn, 'invalid_request', (err as Error).message, id);
      } else {
        this.sendError(conn, 'publish_failed', (err as Error).message, id);
      }
      return;
    }
    this.sendJson(conn, { type: 'published', id, delivered });
  }

  private handleSettle(conn: Connection, frame: Record<string, unknown>, ack: boolean): void {
    const deliveryId = frame.deliveryId;
    if (typeof deliveryId !== 'number' || !Number.isInteger(deliveryId)) return;
    for (const { pendingAcks } of conn.subscriptions.values()) {
      const delivery = pendingAcks.get(deliveryId);
      if (delivery !== undefined) {
        pendingAcks.delete(deliveryId);
        try {
          if (ack) delivery.ack();
          else delivery.nack();
        } catch {
          // Settle is specified no-op on stale handles; never fail the frame.
        }
        return;
      }
    }
    // Unknown delivery id: stale acks are normal under at-least-once
    // semantics (a redelivery already retired the handle). Ignore.
  }
}
