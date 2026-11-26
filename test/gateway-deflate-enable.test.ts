/**
 * Gateway permessage-deflate live enable (EB-561).
 *
 * `setGatewayDeflate(true)` — or an object — on a gateway that never
 * enabled permessage-deflate gains the extension live, without
 * restarting the server: `true` takes every construction default
 * (threshold 1024, both window bits 15, bomb cap 8 MiB), a partial
 * object falls back per field to the construction defaults. Only
 * handshakes that start after the enable negotiate — a connection
 * that is already open stays plaintext, and RFC 7692 has no
 * mid-connection renegotiation. The enable is one-way and live-only:
 * there is no path to clear or disable the extension through this
 * setter, and a restart drops it. An illegal value fails closed with
 * `RangeError` and the gateway stays disabled, so a later valid call
 * still enables it; `true` on an already-enabled gateway is a no-op
 * (it does not reset the tuning — tuning moves only through an
 * object revision, along EB-545's repeat-enable). There is no audit
 * event — the live tuning is observable in `getStats().deflate`.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';
import { compressMessage, decompressMessage } from '../src/gateway-deflate.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<T>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), ms);
    if (typeof timer === 'object' && 'unref' in timer) (timer as { unref(): void }).unref();
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(timer));
}

/** Client -> server: masked frame, optionally with RSV1 set. */
function encodeClientFrame(payload: Buffer, opcode = 0x1, rsv1 = false): Buffer {
  const len = payload.length;
  const b0 = 0x80 | (rsv1 ? 0x40 : 0) | opcode;
  let header: Buffer;
  if (len < 126) {
    header = Buffer.from([b0, 0x80 | len]);
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = b0;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = b0;
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  const mask = randomBytes(4);
  const masked = Buffer.alloc(len);
  for (let i = 0; i < len; i++) masked[i] = payload[i] ^ mask[i % 4];
  return Buffer.concat([header, mask, masked]);
}

interface RawFrame {
  opcode: number;
  rsv1: boolean;
  payload: Buffer;
}

/** Minimal raw-frame client (server frames are never masked). */
class RawClient {
  private buf = Buffer.alloc(0);
  private readonly queue: RawFrame[] = [];
  private waiters: Array<(f: RawFrame) => void> = [];
  private closeWaiters: Array<() => void> = [];
  private closed = false;
  readonly socket: Socket;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.push(chunk)) {
        if (frame.opcode === 0x8) {
          this.closed = true;
          for (const w of this.closeWaiters) w();
          this.closeWaiters = [];
        }
        const waiter = this.waiters.shift();
        if (waiter !== undefined) waiter(frame);
        else this.queue.push(frame);
      }
    });
    socket.on('close', () => {
      this.closed = true;
      for (const w of this.closeWaiters) w();
      this.closeWaiters = [];
    });
    socket.on('error', () => {});
  }

  private push(chunk: Buffer): RawFrame[] {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    const out: RawFrame[] = [];
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return out;
      const rsv1 = (b[0] & 0x40) !== 0;
      const opcode = b[0] & 0x0f;
      let len = b[1] & 0x7f;
      let off = 2;
      if (len === 126) {
        if (b.length < 4) return out;
        len = b.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (b.length < 10) return out;
        len = Number(b.readBigUInt64BE(2));
        off = 10;
      }
      if (b.length < off + len) return out;
      out.push({ opcode, rsv1, payload: b.subarray(off, off + len) });
      this.buf = b.subarray(off + len);
    }
  }

  sendText(value: unknown, rsv1 = false): void {
    const bytes =
      typeof value === 'string' ? Buffer.from(value, 'utf8') : Buffer.from(JSON.stringify(value), 'utf8');
    this.socket.write(encodeClientFrame(bytes, 0x1, rsv1));
  }

  /** Client -> server compressed text frame (client compresses with a fresh context). */
  async sendCompressedText(value: unknown): Promise<void> {
    const bytes = Buffer.from(JSON.stringify(value), 'utf8');
    const compressed = await compressMessage(bytes, 15);
    this.socket.write(encodeClientFrame(compressed, 0x1, true));
  }

  nextFrame(): Promise<RawFrame> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.closed) return Promise.reject(new Error('connection closed'));
    return withTimeout(
      new Promise<RawFrame>((resolve) => this.waiters.push(resolve)),
      5000,
      'next frame',
    );
  }

  async nextJson(): Promise<{ frame: RawFrame; obj: Record<string, unknown> }> {
    for (;;) {
      const frame = await this.nextFrame();
      if (frame.opcode !== 0x1) continue;
      const payload = frame.rsv1 ? await decompressMessage(frame.payload) : frame.payload;
      return { frame, obj: JSON.parse(payload.toString('utf8')) as Record<string, unknown> };
    }
  }

  waitClosed(): Promise<void> {
    if (this.closed) return Promise.resolve();
    return withTimeout(
      new Promise<void>((resolve) => this.closeWaiters.push(resolve)),
      5000,
      'connection close',
    );
  }

  destroy(): void {
    this.socket.destroy();
  }
}

interface ConnectedClient {
  client: RawClient;
  statusLine: string;
  headers: Map<string, string>;
}

async function rawConnect(port: number, extOffer?: string): Promise<ConnectedClient> {
  const socket = tcpConnect(port, '127.0.0.1');
  await withTimeout(
    new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve);
      socket.once('error', reject);
    }),
    3000,
    'tcp connect',
  );
  const key = randomBytes(16).toString('base64');
  const lines = [
    'GET /ws HTTP/1.1',
    'Host: 127.0.0.1',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Key: ${key}`,
    'Sec-WebSocket-Version: 13',
    ...(extOffer !== undefined ? [`Sec-WebSocket-Extensions: ${extOffer}`] : []),
    '',
    '',
  ];
  socket.write(lines.join('\r\n'));
  const head = await withTimeout(
    new Promise<string>((resolve, reject) => {
      let buf = Buffer.alloc(0);
      const onData = (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        const end = buf.indexOf('\r\n\r\n');
        if (end !== -1) {
          socket.off('data', onData);
          socket.off('error', onError);
          const rest = buf.subarray(end + 4);
          resolve(buf.subarray(0, end).toString('latin1'));
          if (rest.length > 0) socket.emit('data', rest);
        }
      };
      const onError = (err: Error) => {
        socket.off('data', onData);
        reject(err);
      };
      socket.on('data', onData);
      socket.once('error', onError);
    }),
    3000,
    'handshake response',
  );
  const statusLine = head.split('\r\n')[0] ?? '';
  const headers = new Map<string, string>();
  for (const line of head.split('\r\n').slice(1)) {
    const idx = line.indexOf(':');
    if (idx !== -1) headers.set(line.slice(0, idx).trim().toLowerCase(), line.slice(idx + 1).trim());
  }
  return { client: new RawClient(socket), statusLine, headers };
}

type GatewayCtorOptions = ConstructorParameters<typeof GatewayServer>[1];

async function withGateway(
  bus: EventBus,
  options: GatewayCtorOptions,
  fn: (gw: GatewayServer, port: number) => Promise<void>,
): Promise<void> {
  const gw = new GatewayServer(bus, options);
  const port = await gw.listen();
  try {
    await fn(gw, port);
  } finally {
    await gw.close();
  }
}

const DEFAULT_DEFLATE = {
  compressThresholdBytes: 1024,
  serverMaxWindowBits: 15,
  clientMaxWindowBits: 15,
  maxInflatedBytes: 8 * 1024 * 1024,
};

test('true enables with construction defaults; a later handshake negotiates and compresses', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    assert.equal(gw.getStats().deflate, undefined);
    gw.setGatewayDeflate(true);
    assert.deepEqual(gw.getStats().deflate, DEFAULT_DEFLATE);

    const { client, statusLine, headers } = await rawConnect(port, 'permessage-deflate');
    assert.equal(statusLine, 'HTTP/1.1 101 Switching Protocols');
    assert.equal(
      headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover',
    );
    client.sendText({ type: 'subscribe', id: 1, pattern: 'en.*' });
    await client.nextJson();

    // Above the default 1024 threshold the frame goes out compressed.
    bus.publish('en.x', 'y'.repeat(5000));
    const compressed = await client.nextFrame();
    assert.equal(compressed.rsv1, true);
    assert.deepEqual(
      JSON.parse((await decompressMessage(compressed.payload)).toString('utf8')).payload,
      'y'.repeat(5000),
    );

    // The newly negotiated connection also accepts inbound compressed frames.
    const payload = { hello: 'compressed world '.repeat(200) };
    await client.sendCompressedText({ type: 'publish', id: 7, topic: 'en.t', payload });
    const reply = await client.nextJson();
    assert.equal(reply.obj.type, 'published');

    client.destroy();
    await flush();
  });
});

test('partial object enable falls back per field to the construction defaults', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  gw.setGatewayDeflate({ compressThresholdBytes: 512, clientMaxWindowBits: 9 });
  assert.deepEqual(gw.getStats().deflate, {
    compressThresholdBytes: 512,
    serverMaxWindowBits: 15,
    clientMaxWindowBits: 9,
    maxInflatedBytes: 8 * 1024 * 1024,
  });
  // An explicitly `undefined` field is not a value either.
  const gw2 = new GatewayServer(bus, {});
  gw2.setGatewayDeflate({ serverMaxWindowBits: undefined, maxInflatedBytes: 4096 });
  assert.deepEqual(gw2.getStats().deflate, { ...DEFAULT_DEFLATE, maxInflatedBytes: 4096 });
});

test('illegal value is rejected, the gateway stays disabled, and a later valid call still enables', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  const bad: Array<unknown> = [
    { compressThresholdBytes: 0 },
    { compressThresholdBytes: -1 },
    { compressThresholdBytes: 1.5 },
    { compressThresholdBytes: '1024' },
    { serverMaxWindowBits: 7 },
    { serverMaxWindowBits: 16 },
    { serverMaxWindowBits: 8.5 },
    { clientMaxWindowBits: 7 },
    { clientMaxWindowBits: 16 },
    { maxInflatedBytes: 0 },
    { maxInflatedBytes: -1 },
    { maxInflatedBytes: 1.5 },
    // One bad field poisons the whole enable, even with good ones.
    { compressThresholdBytes: 2048, serverMaxWindowBits: 7 },
    { maxInflatedBytes: 100, clientMaxWindowBits: 99 },
  ];
  for (const revision of bad) {
    assert.throws(
      () => gw.setGatewayDeflate(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    assert.equal(gw.getStats().deflate, undefined);
  }
  // The failed enables changed nothing: a valid call still enables.
  gw.setGatewayDeflate(true);
  assert.deepEqual(gw.getStats().deflate, DEFAULT_DEFLATE);
});

test('non-object and array revisions are rejected on a disabled gateway', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  for (const bad of ['nope', 42, [], false]) {
    assert.throws(() => gw.setGatewayDeflate(bad as never), RangeError);
    assert.equal(gw.getStats().deflate, undefined);
  }
});

test('null and undefined are silent no-ops on a disabled gateway', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  gw.setGatewayDeflate(null as never);
  gw.setGatewayDeflate(undefined as never);
  assert.equal(gw.getStats().deflate, undefined);
  // Still enableable afterwards.
  gw.setGatewayDeflate({ compressThresholdBytes: 2048 });
  assert.deepEqual(gw.getStats().deflate, { ...DEFAULT_DEFLATE, compressThresholdBytes: 2048 });
});

test('a connection established before the enable stays plaintext afterwards', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    // The offer is ignored while the extension is disabled.
    const { client, headers } = await rawConnect(port, 'permessage-deflate');
    assert.equal(headers.get('sec-websocket-extensions'), undefined);
    client.sendText({ type: 'subscribe', id: 1, pattern: 'pre.*' });
    await client.nextJson();

    gw.setGatewayDeflate(true);
    assert.deepEqual(gw.getStats().deflate, DEFAULT_DEFLATE);

    // The pre-enable connection never compresses, even over threshold.
    bus.publish('pre.x', 'y'.repeat(5000));
    const stillPlain = await client.nextFrame();
    assert.equal(stillPlain.rsv1, false);
    assert.deepEqual(JSON.parse(stillPlain.payload.toString('utf8')).payload, 'y'.repeat(5000));
    client.destroy();

    // A connection established after the enable negotiates.
    const fresh = await rawConnect(port, 'permessage-deflate');
    assert.equal(
      fresh.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover',
    );
    fresh.client.destroy();
    await flush();
  });
});

test('object revision after enable merges against the live enabled values', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  gw.setGatewayDeflate(true);
  gw.setGatewayDeflate({ compressThresholdBytes: 2048 });
  assert.deepEqual(gw.getStats().deflate, { ...DEFAULT_DEFLATE, compressThresholdBytes: 2048 });
  gw.setGatewayDeflate({ serverMaxWindowBits: 9, clientMaxWindowBits: 12 });
  assert.deepEqual(gw.getStats().deflate, {
    ...DEFAULT_DEFLATE,
    compressThresholdBytes: 2048,
    serverMaxWindowBits: 9,
    clientMaxWindowBits: 12,
  });
  // An empty revision is still a no-op after enable.
  gw.setGatewayDeflate({});
  assert.deepEqual(gw.getStats().deflate, {
    ...DEFAULT_DEFLATE,
    compressThresholdBytes: 2048,
    serverMaxWindowBits: 9,
    clientMaxWindowBits: 12,
  });
});

test('true on an enabled gateway is a no-op and does not reset the tuning', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {
    permessageDeflate: { compressThresholdBytes: 512, serverMaxWindowBits: 10 },
  });
  const tuned = {
    compressThresholdBytes: 512,
    serverMaxWindowBits: 10,
    clientMaxWindowBits: 15,
    maxInflatedBytes: 8 * 1024 * 1024,
  };
  assert.deepEqual(gw.getStats().deflate, tuned);
  gw.setGatewayDeflate(true);
  assert.deepEqual(gw.getStats().deflate, tuned);
  // Tuning still moves only through an object revision.
  gw.setGatewayDeflate({ compressThresholdBytes: 2048 });
  assert.deepEqual(gw.getStats().deflate, { ...tuned, compressThresholdBytes: 2048 });
  gw.setGatewayDeflate(true);
  assert.deepEqual(gw.getStats().deflate, { ...tuned, compressThresholdBytes: 2048 });
});

test('getStats().deflate flips from undefined to the live four fields on enable', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  assert.equal(gw.getStats().deflate, undefined);
  gw.setGatewayDeflate({ maxInflatedBytes: 4096 });
  assert.deepEqual(gw.getStats().deflate, { ...DEFAULT_DEFLATE, maxInflatedBytes: 4096 });
  // The stats object is a copy: mutating it never moves the live tuning.
  const stats = gw.getStats();
  (stats.deflate as { compressThresholdBytes: number }).compressThresholdBytes = 1;
  assert.equal(gw.getStats().deflate!.compressThresholdBytes, 1024);
});

test('restart drops the live enable: a fresh gateway reports deflate undefined', async () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  await gw.listen();
  try {
    gw.setGatewayDeflate(true);
    assert.deepEqual(gw.getStats().deflate, DEFAULT_DEFLATE);
  } finally {
    await gw.close();
  }
  // A restart is a fresh GatewayServer without the construction option.
  const restarted = new GatewayServer(bus, {});
  assert.equal(restarted.getStats().deflate, undefined);
  const port2 = await restarted.listen();
  try {
    const { client, headers } = await rawConnect(port2, 'permessage-deflate');
    assert.equal(headers.get('sec-websocket-extensions'), undefined);
    client.destroy();
  } finally {
    await restarted.close();
  }
});
