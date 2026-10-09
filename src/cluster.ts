/**
 * Cross-process cluster federation for the event bus (EB-37).
 *
 * Multiple Node processes form a cluster through a central TCP hub
 * (`ClusterHub`). Each node (`ClusterLink`, attached to an `EventBus` via
 * `bus.connectToHub`) advertises the topic patterns it subscribes to; the
 * hub keeps the authoritative route table — member roster plus patterns —
 * and broadcasts it with a monotonically increasing version, so every node
 * holds a cached copy for route-aware forwarding.
 *
 * Message flow (route-aware, "nearest" forwarding):
 * - A publish whose topic matches only local subscribers never touches the
 *   network: local delivery with the node's own per-topic sequence numbers.
 * - A publish whose topic matches subscribers on OTHER members is
 *   additionally forwarded to the hub. The hub stamps a hub-global
 *   per-topic sequence number (and, for keyed messages, a hub-global
 *   per-key sequence number) and forwards the message only to the members
 *   whose advertised patterns match — never a blind broadcast. The sending
 *   node does not receive its own forward back; its local subscribers were
 *   already served locally.
 *
 * Sequence spaces: cluster traffic carries hub-assigned sequence numbers,
 * local traffic carries node-local ones. A subscriber that observes both
 * (its node publishes locally while receiving other nodes' traffic on the
 * same topic) sees a sequence-epoch change; the bus resets its gap-detection
 * baseline on the change instead of counting a phantom gap (see
 * `BusMessage.epoch`).
 *
 * Degraded mode: when the hub connection drops, the node keeps serving
 * locally with its cached route table (remote members are simply
 * unreachable — no phantom delivery). Reconnect re-announces the current
 * patterns and re-syncs routes; a hub restart is detected via a new hub
 * epoch, which supersedes the route version.
 *
 * Wire protocol: 4-byte big-endian length prefix + UTF-8 JSON frame.
 * Frames: hello / welcome / routes / heartbeat / heartbeat-ack /
 * publish / message / goodbye / error. Payloads must be JSON-serializable;
 * `Buffer` payloads (e.g. compressed envelopes) travel as
 * `{ "__buf": "<base64>" }`.
 *
 * Zero new dependencies: `node:net` / `node:tls` only.
 */
import { createServer as createTcpServer, connect as tcpConnect, type Server as TcpServer, type Socket } from 'node:net';
import { createServer as createTlsServer, connect as tlsConnect, type Server as TlsServer, type TLSSocket } from 'node:tls';
import { randomBytes } from 'node:crypto';
import { compilePattern } from './bus.ts';
import { ReconnectController } from './reconnect.ts';

export interface ClusterHubOptions {
  /** Interface to bind (default 127.0.0.1). */
  host?: string;
  /** Port to listen on; 0 (default) picks an ephemeral port. */
  port?: number;
  /** How often members must heartbeat, ms (default 5000). */
  heartbeatIntervalMs?: number;
  /** Silence after which the hub drops a member, ms (default 15000). */
  heartbeatTimeoutMs?: number;
  /** When set, the hub listens with TLS using this key/cert. */
  tls?: { key: string | Buffer; cert: string | Buffer };
}

export interface ClusterConnectOptions {
  /** `tcp://host:port` or `tls://host:port`. */
  url: string;
  /** Stable node identity; random when omitted. */
  nodeId?: string;
  /** TLS client options (for `tls://` URLs): CA chain and verification. */
  tls?: { ca?: string | Buffer | Array<string | Buffer>; rejectUnauthorized?: boolean };
  /** Heartbeat interval ms (default 5000). Must be positive. */
  heartbeatMs?: number;
  /**
   * Reconnect behavior. `false` disables reconnecting (the link stays
   * degraded after a drop, and the initial `connect()` is a single
   * attempt); otherwise the initial connect AND later drops all ride the
   * backoff policy (`{ baseDelayMs, maxDelayMs, maxAttempts }`, defaults
   * 200 / 5000 / 10) — the initial `connect()` rejects only when the
   * policy gives up.
   */
  reconnect?: false | { baseDelayMs?: number; maxDelayMs?: number; maxAttempts?: number };
  /** Fired on connect / disconnect / route-table updates (best effort). */
  onStatusChange?: (status: ClusterLinkStatus) => void;
}

export interface ClusterLinkStatus {
  connected: boolean;
  /** True when the link dropped and is serving local-only (or backing off). */
  degraded: boolean;
  nodeId: string;
  routeVersion: number;
  hubEpoch: string | null;
  /** Node ids in the cached route table (includes self). */
  members: string[];
}

export interface ClusterLinkStats extends ClusterLinkStatus {
  forwardedMessages: number;
  receivedMessages: number;
  /** Forwards dropped because the payload was not JSON-serializable. */
  forwardErrors: number;
  /** Hub-forwarded messages dropped on receipt (invalid frame / inflate failure). */
  receiveDropped: number;
  /** Successful (re)connects, including the first. */
  connects: number;
}

export interface ClusterHubStats {
  members: number;
  routeVersion: number;
  hubEpoch: string;
  /** Messages forwarded hub -> members (excludes the sender). */
  forwardedMessages: number;
  topics: number;
}

interface HubMember {
  nodeId: string;
  patterns: string[];
  matchers: RegExp[];
  socket: Socket | TLSSocket;
  lastHeartbeat: number;
}

interface RouteEntry {
  nodeId: string;
  patterns: string[];
}

type HubFrame =
  | { type: 'hello'; nodeId: string; patterns: string[] }
  | { type: 'heartbeat'; nodeId: string }
  | { type: 'goodbye'; nodeId: string }
  | { type: 'publish'; topic: string; payload: unknown; key?: string; expiresAt?: number; compressed?: boolean; dictId?: string };

type NodeFrame =
  | { type: 'welcome'; nodeId: string; routeVersion: number; hubEpoch: string }
  | { type: 'routes'; version: number; hubEpoch: string; members: RouteEntry[] }
  | { type: 'heartbeat-ack'; nodeId: string }
  | {
      type: 'message';
      topic: string;
      payload: unknown;
      key?: string;
      keySeq?: number;
      seq: number;
      expiresAt?: number;
      epoch: string;
      compressed?: boolean;
      dictId?: string;
    }
  | { type: 'error'; message: string };

export interface ClusterMessage {
  topic: string;
  payload: unknown;
  key?: string;
  keySeq?: number;
  seq: number;
  expiresAt?: number;
  /** `hub:<hubEpoch>` — the sequence epoch for gap-detection baselines. */
  epoch: string;
  compressed?: boolean;
  dictId?: string;
}

/** Revives `{ "__buf": "<base64>" }` markers back into Buffers. */
function reviveBuffers(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(reviveBuffers);
  if (value !== null && typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj);
    if (keys.length === 1 && keys[0] === '__buf' && typeof obj['__buf'] === 'string') {
      return Buffer.from(obj['__buf'] as string, 'base64');
    }
    const out: Record<string, unknown> = {};
    for (const k of keys) out[k] = reviveBuffers(obj[k]);
    return out;
  }
  return value;
}

/** Encodes Buffers as `{ "__buf": "<base64>" }` so frames stay pure JSON. */
function encodeBuffers(value: unknown): unknown {
  if (Buffer.isBuffer(value)) return { __buf: value.toString('base64') };
  if (Array.isArray(value)) return value.map(encodeBuffers);
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) out[k] = encodeBuffers(v);
    return out;
  }
  return value;
}

function encodeFrame(frame: object): Buffer {
  const body = Buffer.from(JSON.stringify(encodeBuffers(frame)), 'utf8');
  const out = Buffer.allocUnsafe(4 + body.length);
  out.writeUInt32BE(body.length, 0);
  body.copy(out, 4);
  return out;
}

/**
 * Incremental frame decoder: feed TCP chunks, pull out complete frames.
 * Malformed JSON in a frame is reported to `onError` and skipped — one
 * bad frame must not desynchronize the stream.
 */
class FrameDecoder {
  private pending = Buffer.alloc(0);
  private readonly onFrame: (frame: unknown) => void;
  private readonly onError?: (err: Error) => void;
  constructor(onFrame: (frame: unknown) => void, onError?: (err: Error) => void) {
    this.onFrame = onFrame;
    this.onError = onError;
  }
  push(chunk: Buffer): void {
    this.pending = Buffer.concat([this.pending, chunk]);
    for (;;) {
      if (this.pending.length < 4) return;
      const len = this.pending.readUInt32BE(0);
      if (len > 16 * 1024 * 1024) {
        this.onError?.(new Error(`cluster frame too large: ${len} bytes`));
        this.pending = Buffer.alloc(0);
        return;
      }
      if (this.pending.length < 4 + len) return;
      const body = this.pending.subarray(4, 4 + len);
      this.pending = this.pending.subarray(4 + len);
      try {
        this.onFrame(reviveBuffers(JSON.parse(body.toString('utf8'))));
      } catch (err) {
        this.onError?.(err instanceof Error ? err : new Error(String(err)));
      }
    }
  }
}

function isValidPatterns(patterns: unknown): patterns is string[] {
  return (
    Array.isArray(patterns) &&
    patterns.every((p) => typeof p === 'string' && p.length > 0 && p.length <= 1024)
  );
}

/**
 * Central cluster hub: accepts member connections, keeps the authoritative
 * route table (member -> subscribed patterns), and forwards published
 * messages to the members whose patterns match — stamping hub-global
 * per-topic (and per-key) sequence numbers on the way.
 */
export class ClusterHub {
  private server: TcpServer | TlsServer | null = null;
  private readonly members = new Map<string, HubMember>();
  private routeVersion = 0;
  private readonly hubEpoch = `hub-${randomBytes(8).toString('hex')}`;
  private readonly topicSeq = new Map<string, number>();
  private readonly keySeqs = new Map<string, number>();
  private forwardedMessages = 0;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private readonly heartbeatIntervalMs: number;
  private readonly heartbeatTimeoutMs: number;
  private readonly tls: ClusterHubOptions['tls'];
  private readonly host: string;
  private readonly port: number;

  constructor(options: ClusterHubOptions = {}) {
    const heartbeatIntervalMs = options.heartbeatIntervalMs ?? 5000;
    if (!Number.isFinite(heartbeatIntervalMs) || heartbeatIntervalMs <= 0) {
      throw new RangeError('heartbeatIntervalMs must be a positive finite number');
    }
    const heartbeatTimeoutMs = options.heartbeatTimeoutMs ?? heartbeatIntervalMs * 3;
    if (!Number.isFinite(heartbeatTimeoutMs) || heartbeatTimeoutMs <= 0) {
      throw new RangeError('heartbeatTimeoutMs must be a positive finite number');
    }
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    this.tls = options.tls;
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 0;
  }

  /** Starts listening; resolves with the bound port. */
  listen(): Promise<number> {
    if (this.server != null) throw new Error('ClusterHub is already listening');
    return new Promise((resolve, reject) => {
      const onConnection = (socket: Socket | TLSSocket) => this.handleConnection(socket);
      this.server = this.tls
        ? createTlsServer({ key: this.tls.key, cert: this.tls.cert }, onConnection)
        : createTcpServer(onConnection);
      this.server.once('error', reject);
      this.server.listen(this.port, this.host, () => {
        this.server?.off('error', reject);
        const addr = this.server?.address();
        const boundPort = typeof addr === 'object' && addr != null ? addr.port : this.port;
        this.sweepTimer = setInterval(() => this.sweepSilentMembers(), this.heartbeatIntervalMs);
        if (typeof this.sweepTimer === 'object' && this.sweepTimer !== null && 'unref' in this.sweepTimer) {
          (this.sweepTimer as { unref(): void }).unref();
        }
        resolve(boundPort);
      });
    });
  }

  /** Stops the hub and drops every member connection. */
  async close(): Promise<void> {
    if (this.sweepTimer != null) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    for (const member of this.members.values()) member.socket.destroy();
    this.members.clear();
    const server = this.server;
    this.server = null;
    if (server == null) return;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }

  getStats(): ClusterHubStats {
    return {
      members: this.members.size,
      routeVersion: this.routeVersion,
      hubEpoch: this.hubEpoch,
      forwardedMessages: this.forwardedMessages,
      topics: this.topicSeq.size,
    };
  }

  private handleConnection(socket: Socket | TLSSocket): void {
    let nodeId: string | null = null;
    const decoder = new FrameDecoder(
      (frame) => {
        const next = this.handleFrame(socket, frame as HubFrame, nodeId);
        if (next !== undefined) nodeId = next;
      },
      () => {
        socket.write(encodeFrame({ type: 'error', message: 'malformed frame' }));
      },
    );
    socket.on('data', (chunk: Buffer) => decoder.push(chunk));
    socket.on('error', () => {});
    socket.on('close', () => {
      if (nodeId != null && this.members.get(nodeId)?.socket === socket) {
        this.removeMember(nodeId, 'transport closed');
      }
    });
  }

  /**
   * Handles one hub-side frame. Returns the newly bound nodeId when a
   * hello was accepted (so the connection handler can track ownership).
   */
  private handleFrame(socket: Socket | TLSSocket, frame: HubFrame, currentNodeId: string | null): string | undefined {
    if (frame == null || typeof frame !== 'object' || typeof (frame as { type?: unknown }).type !== 'string') {
      socket.write(encodeFrame({ type: 'error', message: 'frame without type' }));
      return undefined;
    }
    switch (frame.type) {
      case 'hello': {
        const { nodeId, patterns } = frame;
        if (typeof nodeId !== 'string' || nodeId.length === 0 || nodeId.length > 256 || !isValidPatterns(patterns)) {
          socket.write(encodeFrame({ type: 'error', message: 'invalid hello' }));
          return undefined;
        }
        // Re-hello (pattern change or reconnect with the same nodeId)
        // replaces the member's patterns: last-writer-wins at the hub.
        const prev = this.members.get(nodeId);
        if (prev != null && prev.socket !== socket) prev.socket.destroy();
        let matchers: RegExp[];
        try {
          matchers = patterns.map((p) => compilePattern(p));
        } catch {
          socket.write(encodeFrame({ type: 'error', message: 'invalid pattern' }));
          return undefined;
        }
        this.members.set(nodeId, { nodeId, patterns: [...patterns], matchers, socket, lastHeartbeat: Date.now() });
        this.bumpRoutes();
        socket.write(encodeFrame({ type: 'welcome', nodeId, routeVersion: this.routeVersion, hubEpoch: this.hubEpoch } satisfies NodeFrame));
        this.broadcastRoutes();
        return nodeId;
      }
      case 'heartbeat': {
        if (typeof frame.nodeId !== 'string') return undefined;
        const member = this.members.get(frame.nodeId);
        if (member != null && member.socket === socket) {
          member.lastHeartbeat = Date.now();
          socket.write(encodeFrame({ type: 'heartbeat-ack', nodeId: frame.nodeId } satisfies NodeFrame));
        }
        return undefined;
      }
      case 'publish': {
        if (currentNodeId == null) {
          socket.write(encodeFrame({ type: 'error', message: 'hello first' }));
          return undefined;
        }
        this.handlePublish(currentNodeId, frame);
        return undefined;
      }
      case 'goodbye': {
        if (typeof frame.nodeId === 'string') this.removeMember(frame.nodeId, 'goodbye');
        return undefined;
      }
      default:
        socket.write(encodeFrame({ type: 'error', message: `unknown frame type: ${(frame as { type: string }).type}` }));
        return undefined;
    }
  }

  private handlePublish(senderId: string, frame: Extract<HubFrame, { type: 'publish' }>): void {
    const { topic, payload, key, expiresAt, compressed, dictId } = frame;
    if (typeof topic !== 'string' || topic.length === 0 || topic.length > 1024) return;
    const seq = (this.topicSeq.get(topic) ?? 0) + 1;
    this.topicSeq.set(topic, seq);
    let keySeq: number | undefined;
    if (key !== undefined) {
      if (typeof key !== 'string' || key.length === 0 || key.length > 1024) return;
      keySeq = (this.keySeqs.get(key) ?? 0) + 1;
      this.keySeqs.set(key, keySeq);
    }
    const out: NodeFrame = {
      type: 'message',
      topic,
      payload,
      seq,
      epoch: this.hubEpoch,
      ...(key === undefined ? {} : { key, keySeq }),
      ...(expiresAt === undefined ? {} : { expiresAt }),
      ...(compressed === true ? { compressed: true } : {}),
      ...(dictId === undefined ? {} : { dictId }),
    };
    const bytes = encodeFrame(out);
    for (const member of this.members.values()) {
      if (member.nodeId === senderId) continue;
      let matches = false;
      for (const m of member.matchers) {
        if (m.test(topic)) {
          matches = true;
          break;
        }
      }
      if (!matches) continue;
      member.socket.write(bytes);
      this.forwardedMessages += 1;
    }
  }

  private removeMember(nodeId: string, _reason: string): void {
    if (!this.members.delete(nodeId)) return;
    this.bumpRoutes();
    this.broadcastRoutes();
  }

  private sweepSilentMembers(): void {
    const now = Date.now();
    let swept = false;
    for (const [nodeId, member] of this.members) {
      if (now - member.lastHeartbeat > this.heartbeatTimeoutMs) {
        member.socket.destroy();
        this.members.delete(nodeId);
        swept = true;
      }
    }
    if (swept) {
      this.bumpRoutes();
      this.broadcastRoutes();
    }
  }

  private bumpRoutes(): void {
    this.routeVersion += 1;
  }

  private broadcastRoutes(): void {
    const members: RouteEntry[] = [...this.members.values()].map((m) => ({ nodeId: m.nodeId, patterns: [...m.patterns] }));
    const frame: NodeFrame = { type: 'routes', version: this.routeVersion, hubEpoch: this.hubEpoch, members };
    const bytes = encodeFrame(frame);
    for (const member of this.members.values()) member.socket.write(bytes);
  }
}

interface CachedRoutes {
  version: number;
  hubEpoch: string | null;
  members: Array<{ nodeId: string; patterns: string[]; matchers: RegExp[] }>;
}

/**
 * Node-side hub connection, owned by one `EventBus` (see
 * `EventBus.connectToHub`). Forwards locally-published messages whose topic
 * matches remote members, and delivers hub-forwarded messages into the
 * bus. Drops into degraded local-only mode on transport loss; reconnects
 * with backoff unless disabled.
 */
export class ClusterLink {
  private socket: Socket | TLSSocket | null = null;
  private decoder: FrameDecoder | null = null;
  private readonly nodeId: string;
  private readonly url: URL;
  private readonly tls: ClusterConnectOptions['tls'];
  private readonly heartbeatMs: number;
  private readonly onMessage: (msg: ClusterMessage) => boolean;
  private readonly getPatterns: () => string[];
  private readonly onStatusChange?: (status: ClusterLinkStatus) => void;
  private routes: CachedRoutes = { version: 0, hubEpoch: null, members: [] };
  private advertised: string[] | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectController: ReconnectController | null = null;
  private welcomeResolve: (() => void) | null = null;
  private welcomeReject: ((err: Error) => void) | null = null;
  private closed = false;
  private connectedFlag = false;
  private forwardedMessages = 0;
  private receivedMessages = 0;
  private forwardErrors = 0;
  private receiveDropped = 0;
  private connects = 0;
  private initialResolve: (() => void) | null = null;
  private initialReject: ((err: Error) => void) | null = null;

  constructor(
    options: ClusterConnectOptions & {
      onMessage: (msg: ClusterMessage) => boolean;
      getPatterns: () => string[];
    },
  ) {
    let url: URL;
    try {
      url = new URL(options.url);
    } catch {
      throw new RangeError(`cluster url must be tcp://host:port or tls://host:port, got: ${options.url}`);
    }
    if (url.protocol !== 'tcp:' && url.protocol !== 'tls:') {
      throw new RangeError(`cluster url must be tcp://host:port or tls://host:port, got: ${options.url}`);
    }
    if (url.port === '') throw new RangeError(`cluster url must include a port: ${options.url}`);
    const heartbeatMs = options.heartbeatMs ?? 5000;
    if (!Number.isFinite(heartbeatMs) || heartbeatMs <= 0) {
      throw new RangeError('heartbeatMs must be a positive finite number');
    }
    this.url = url;
    this.nodeId = options.nodeId ?? `node-${randomBytes(8).toString('hex')}`;
    if (this.nodeId.length === 0 || this.nodeId.length > 256) throw new RangeError('nodeId must be 1-256 characters');
    this.tls = options.tls;
    this.heartbeatMs = heartbeatMs;
    this.onMessage = options.onMessage;
    this.getPatterns = options.getPatterns;
    this.onStatusChange = options.onStatusChange;
    if (options.reconnect !== false) {
      const rc = options.reconnect ?? {};
      const maxAttempts = rc.maxAttempts ?? 10;
      this.reconnectController = new ReconnectController({
        connectFn: async () => {
          await this.establish();
        },
        baseDelayMs: rc.baseDelayMs ?? 200,
        maxDelayMs: rc.maxDelayMs ?? 5000,
        maxAttempts,
        onGiveUp: () => {
          this.initialReject?.(new Error(`cluster connect gave up after ${maxAttempts} attempts`));
          this.initialReject = null;
          this.initialResolve = null;
          this.emitStatus();
        },
      });
    }
  }

  /**
   * Opens the transport, hellos, and waits for the hub's welcome. With
   * the default reconnect policy the initial connect rides the backoff
   * (rejecting only when the policy gives up); with `reconnect: false`
   * it is a single attempt.
   */
  async connect(): Promise<void> {
    if (this.closed) throw new Error('ClusterLink is closed');
    if (this.reconnectController == null) {
      await this.establish();
      return;
    }
    await new Promise<void>((resolve, reject) => {
      this.initialResolve = resolve;
      this.initialReject = reject;
      this.reconnectController!.start();
    });
  }

  private establish(): Promise<void> {
    return new Promise((resolve, reject) => {
      const host = this.url.hostname || '127.0.0.1';
      const port = Number(this.url.port);
      const useTls = this.url.protocol === 'tls:';
      const socket: Socket | TLSSocket = useTls
        ? tlsConnect({ host, port, ca: this.tls?.ca, rejectUnauthorized: this.tls?.rejectUnauthorized ?? true })
        : tcpConnect({ host, port });
      const onError = (err: Error) => {
        socket.destroy();
        this.welcomeReject?.(err);
        this.welcomeReject = null;
        reject(err);
      };
      socket.once('error', onError);
      const onReady = () => {
        socket.off('error', onError);
        socket.on('error', () => {});
        this.attach(socket);
        this.welcomeResolve = resolve as () => void;
        this.welcomeReject = reject;
        const timer = setTimeout(() => {
          if (this.welcomeReject != null) {
            this.welcomeReject(new Error('cluster welcome timeout'));
            this.welcomeReject = null;
            this.welcomeResolve = null;
            socket.destroy();
          }
        }, 5000);
        if (typeof timer === 'object' && timer !== null && 'unref' in timer) (timer as { unref(): void }).unref();
        const originalResolve = this.welcomeResolve;
        this.welcomeResolve = () => {
          clearTimeout(timer);
          this.welcomeResolve = null;
          this.welcomeReject = null;
          originalResolve();
        };
        // (Re)announce the CURRENT patterns: they may have changed while
        // disconnected, and the hub replaces our entry last-writer-wins.
        this.sendHello();
      };
      if (useTls) (socket as TLSSocket).once('secureConnect', onReady);
      else socket.once('connect', onReady);
    });
  }

  private attach(socket: Socket | TLSSocket): void {
    this.socket = socket;
    this.decoder = new FrameDecoder(
      (frame) => this.handleNodeFrame(frame as NodeFrame),
      () => {},
    );
    socket.on('data', (chunk: Buffer) => this.decoder?.push(chunk));
    socket.on('close', () => this.handleTransportLoss());
    socket.on('error', () => {});
  }

  private handleTransportLoss(): void {
    const was = this.connectedFlag;
    this.connectedFlag = false;
    this.socket = null;
    this.decoder = null;
    this.stopHeartbeat();
    this.welcomeReject?.(new Error('cluster transport lost before welcome'));
    this.welcomeReject = null;
    this.welcomeResolve = null;
    if (this.closed) return;
    // Degraded: keep the cached routes (the bus serves local-only now) and
    // back off towards a reconnect unless the link was built without one.
    if (was) this.emitStatus();
    this.reconnectController?.notifyDisconnected();
    if (this.reconnectController == null) this.emitStatus();
  }

  private handleNodeFrame(frame: NodeFrame): void {
    if (frame == null || typeof frame !== 'object' || typeof (frame as { type?: unknown }).type !== 'string') return;
    switch (frame.type) {
      case 'welcome': {
        this.connectedFlag = true;
        this.connects += 1;
        this.startHeartbeat();
        this.welcomeResolve?.();
        // The initial connect() resolves on the first welcome; later
        // welcomes are reconnects (the controller already counted them).
        this.initialResolve?.();
        this.initialResolve = null;
        this.initialReject = null;
        this.emitStatus();
        break;
      }
      case 'routes': {
        this.applyRoutes(frame.version, frame.hubEpoch, frame.members);
        break;
      }
      case 'heartbeat-ack':
        break;
      case 'message': {
        if (!this.connectedFlag) break;
        const msg = frame as Extract<NodeFrame, { type: 'message' }>;
        if (typeof msg.topic !== 'string' || !Number.isInteger(msg.seq) || (msg.seq as number) < 1) break;
        this.receivedMessages += 1;
        if (
          !this.onMessage({
            topic: msg.topic,
            payload: msg.payload,
            seq: msg.seq as number,
            epoch: msg.epoch,
            ...(msg.key === undefined ? {} : { key: msg.key, keySeq: msg.keySeq }),
            ...(msg.expiresAt === undefined ? {} : { expiresAt: msg.expiresAt }),
            ...(msg.compressed === true ? { compressed: true } : {}),
            ...(msg.dictId === undefined ? {} : { dictId: msg.dictId }),
          })
        ) {
          this.receiveDropped += 1;
        }
        break;
      }
      case 'error':
        break;
    }
  }

  /**
   * Applies a hub route broadcast. The hub is authoritative; a broadcast
   * is applied only when it is newer than the cache (higher version), or
   * when the hub epoch changed (a restarted hub resets its version, and
   * the epoch supersedes it) — this is the version-conflict merge.
   */
  private applyRoutes(version: number, hubEpoch: string, members: RouteEntry[]): void {
    const epochChanged = this.routes.hubEpoch !== null && this.routes.hubEpoch !== hubEpoch;
    if (!epochChanged && version <= this.routes.version) return;
    const compiled: CachedRoutes['members'] = [];
    for (const m of members) {
      if (typeof m.nodeId !== 'string' || !isValidPatterns(m.patterns)) continue;
      let matchers: RegExp[];
      try {
        matchers = m.patterns.map((p) => compilePattern(p));
      } catch {
        continue;
      }
      compiled.push({ nodeId: m.nodeId, patterns: [...m.patterns], matchers });
    }
    this.routes = { version, hubEpoch, members: compiled };
    this.emitStatus();
  }

  /**
   * Sends a hello with the CURRENT patterns (read live from the bus),
   * unconditionally. Used on (re)connect, where the link cannot rely on
   * the connected-flag guard in `advertisePatterns`.
   */
  private sendHello(): void {
    const socket = this.socket;
    if (socket == null) return;
    const patterns = [...this.getPatterns()].sort();
    this.advertised = patterns;
    socket.write(encodeFrame({ type: 'hello', nodeId: this.nodeId, patterns } satisfies HubFrame));
  }

  /**
   * (Re)announces the node's subscribed patterns to the hub. No-op when
   * the set is unchanged since the last announcement — subscribe churn on
   * an already-advertised pattern costs nothing. Before the welcome, the
   * announcement is deferred: the pending (re)connect sends a hello with
   * the live pattern set anyway.
   */
  advertisePatterns(patterns: string[]): void {
    const sorted = [...patterns].sort();
    if (this.advertised != null && this.advertised.length === sorted.length && this.advertised.every((p, i) => p === sorted[i])) {
      return;
    }
    if (this.socket == null || !this.connectedFlag) return;
    this.advertised = sorted;
    this.socket.write(encodeFrame({ type: 'hello', nodeId: this.nodeId, patterns: sorted } satisfies HubFrame));
  }

  /** Node ids (excluding self) with at least one pattern matching `topic`. */
  matchingRemoteMembers(topic: string): string[] {
    const out: string[] = [];
    for (const m of this.routes.members) {
      if (m.nodeId === this.nodeId) continue;
      for (const matcher of m.matchers) {
        if (matcher.test(topic)) {
          out.push(m.nodeId);
          break;
        }
      }
    }
    return out;
  }

  /**
   * Forwards one locally-admitted message to the hub for remote fan-out.
   * Returns false (counted, never thrown) when the payload is not
   * JSON-serializable or the transport is down — local delivery already
   * happened, so a forward failure must not fail the publish.
   */
  forwardPublish(msg: {
    topic: string;
    payload: unknown;
    key?: string;
    expiresAt?: number;
    compressed?: boolean;
    dictId?: string;
  }): boolean {
    const socket = this.socket;
    if (socket == null || !this.connectedFlag) {
      this.forwardErrors += 1;
      return false;
    }
    let frame: Buffer;
    try {
      frame = encodeFrame({ type: 'publish', ...msg } satisfies HubFrame);
    } catch {
      this.forwardErrors += 1;
      return false;
    }
    socket.write(frame);
    this.forwardedMessages += 1;
    return true;
  }

  isConnected(): boolean {
    return this.connectedFlag;
  }

  getStatus(): ClusterLinkStatus {
    return {
      connected: this.connectedFlag,
      degraded: !this.closed && !this.connectedFlag && this.connects > 0,
      nodeId: this.nodeId,
      routeVersion: this.routes.version,
      hubEpoch: this.routes.hubEpoch,
      members: this.routes.members.map((m) => m.nodeId),
    };
  }

  getStats(): ClusterLinkStats {
    return {
      ...this.getStatus(),
      forwardedMessages: this.forwardedMessages,
      receivedMessages: this.receivedMessages,
      forwardErrors: this.forwardErrors,
      receiveDropped: this.receiveDropped,
      connects: this.connects,
    };
  }

  /** Leaves the cluster: no goodbye-storm, no reconnect. Idempotent. */
  async disconnect(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.reconnectController?.stop();
    this.reconnectController = null;
    this.stopHeartbeat();
    const socket = this.socket;
    this.socket = null;
    this.connectedFlag = false;
    if (socket != null) {
      try {
        socket.write(encodeFrame({ type: 'goodbye', nodeId: this.nodeId } satisfies HubFrame));
      } catch {
        /* best effort */
      }
      socket.destroy();
    }
    this.emitStatus();
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      const socket = this.socket;
      if (socket == null) return;
      try {
        socket.write(encodeFrame({ type: 'heartbeat', nodeId: this.nodeId } satisfies HubFrame));
      } catch {
        /* the transport-loss handler deals with dead sockets */
      }
    }, this.heartbeatMs);
    if (typeof this.heartbeatTimer === 'object' && this.heartbeatTimer !== null && 'unref' in this.heartbeatTimer) {
      (this.heartbeatTimer as { unref(): void }).unref();
    }
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer != null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private emitStatus(): void {
    try {
      this.onStatusChange?.(this.getStatus());
    } catch {
      /* status listeners must not break the link */
    }
  }
}
