import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import {
  createDeflateRaw,
  constants as zlibConstants,
} from 'node:zlib';
import { EventBus } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';
import {
  compressMessage,
  decompressMessage,
  maybeCompressMessage,
  negotiateServerDeflate,
  resolvePerMessageDeflate,
  WsInflateBombError,
} from '../src/gateway-deflate.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function withTimeout<T>(p: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<T>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), ms);
    if (typeof timer === 'object' && 'unref' in timer) (timer as { unref(): void }).unref();
  });
  return Promise.race([p, timeout]).finally(() => clearTimeout(timer));
}

/** Deterministic high-entropy bytes: xorshift32 with a fixed seed. */
function pseudoRandomBytes(n: number, seed: number): Buffer {
  let s = seed >>> 0;
  const out = Buffer.alloc(n);
  for (let i = 0; i < n; i++) {
    s ^= s << 13; s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5; s >>>= 0;
    out[i] = s & 0xff;
  }
  return out;
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

  /** Client -> server binary publish frame, optionally RSV1-compressed. */
  async sendBinaryPublish(header: object, body: Buffer, rsv1 = false): Promise<void> {
    const headerBytes = Buffer.from(JSON.stringify(header), 'utf8');
    const envelope = Buffer.alloc(4 + headerBytes.length + body.length);
    envelope.writeUInt32BE(headerBytes.length, 0);
    headerBytes.copy(envelope, 4);
    body.copy(envelope, 4 + headerBytes.length);
    const payload = rsv1 ? await compressMessage(envelope, 15) : envelope;
    this.socket.write(encodeClientFrame(payload, 0x2, rsv1));
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

// --- unit: option resolution -------------------------------------------------

test('resolvePerMessageDeflate: defaults and validation', () => {
  assert.equal(resolvePerMessageDeflate(undefined), undefined);
  assert.equal(resolvePerMessageDeflate(false), undefined);
  const d = resolvePerMessageDeflate(true)!;
  assert.equal(d.compressThresholdBytes, 1024);
  assert.equal(d.serverMaxWindowBits, 15);
  assert.equal(d.clientMaxWindowBits, 15);
  assert.equal(d.maxInflatedBytes, 8 * 1024 * 1024);

  const custom = resolvePerMessageDeflate({
    compressThresholdBytes: 10,
    serverMaxWindowBits: 8,
    clientMaxWindowBits: 12,
    maxInflatedBytes: 100,
  })!;
  assert.equal(custom.compressThresholdBytes, 10);
  assert.equal(custom.serverMaxWindowBits, 8);
  assert.equal(custom.clientMaxWindowBits, 12);

  for (const bad of [0, -1, 1.5, Number.NaN, '1024']) {
    assert.throws(
      () => resolvePerMessageDeflate({ compressThresholdBytes: bad as number }),
      RangeError,
      `threshold ${String(bad)}`,
    );
  }
  for (const bits of [7, 16, 0, 1.5, '15']) {
    assert.throws(
      () => resolvePerMessageDeflate({ serverMaxWindowBits: bits as number }),
      RangeError,
      `server bits ${String(bits)}`,
    );
    assert.throws(
      () => resolvePerMessageDeflate({ clientMaxWindowBits: bits as number }),
      RangeError,
      `client bits ${String(bits)}`,
    );
  }
  // Boundary values 8 and 15 are legal.
  assert.equal(resolvePerMessageDeflate({ serverMaxWindowBits: 8 })!.serverMaxWindowBits, 8);
  assert.equal(resolvePerMessageDeflate({ clientMaxWindowBits: 15 })!.clientMaxWindowBits, 15);
  assert.throws(() => resolvePerMessageDeflate(null as never), RangeError);
  assert.throws(() => resolvePerMessageDeflate([] as never), RangeError);
  assert.throws(() => new GatewayServer(new EventBus(), { permessageDeflate: { serverMaxWindowBits: 7 } }), RangeError);
  assert.throws(() => new GatewayServer(new EventBus(), { permessageDeflate: { compressThresholdBytes: 0 } }), RangeError);
});

// --- unit: negotiation --------------------------------------------------------

test('negotiateServerDeflate: offer parsing and invalid offers', () => {
  const server = resolvePerMessageDeflate(true)!;
  assert.equal(negotiateServerDeflate(undefined, server), null);
  assert.equal(negotiateServerDeflate('', server), null);
  assert.equal(negotiateServerDeflate('x-other-ext', server), null);

  const bare = negotiateServerDeflate('permessage-deflate', server)!;
  assert.equal(bare.response, 'permessage-deflate; client_no_context_takeover');
  assert.equal(bare.serverWindowBits, 15);

  // Case-insensitive extension name; other extensions skipped.
  const mixed = negotiateServerDeflate('x-foo, Permessage-Deflate', server)!;
  assert.equal(mixed.response, 'permessage-deflate; client_no_context_takeover');

  // server_max_window_bits offered with a value: server adopts it.
  const smwb = negotiateServerDeflate('permessage-deflate; server_max_window_bits=10', server)!;
  assert.equal(smwb.response, 'permessage-deflate; client_no_context_takeover; server_max_window_bits=10');
  assert.equal(smwb.serverWindowBits, 10);

  // server_max_window_bits without value: server uses its configured bits.
  const smwbBare = negotiateServerDeflate(
    'permessage-deflate; server_max_window_bits',
    resolvePerMessageDeflate({ serverMaxWindowBits: 9 })!,
  )!;
  assert.equal(smwbBare.response, 'permessage-deflate; client_no_context_takeover; server_max_window_bits=9');
  assert.equal(smwbBare.serverWindowBits, 9);

  // client_max_window_bits without value: server picks its configured cap.
  const cmwb = negotiateServerDeflate(
    'permessage-deflate; client_max_window_bits',
    resolvePerMessageDeflate({ clientMaxWindowBits: 12 })!,
  )!;
  assert.equal(cmwb.response, 'permessage-deflate; client_no_context_takeover; client_max_window_bits=12');

  // client_max_window_bits with a value: server must not exceed it.
  const cmwbVal = negotiateServerDeflate('permessage-deflate; client_max_window_bits=9', server)!;
  assert.equal(cmwbVal.response, 'permessage-deflate; client_no_context_takeover; client_max_window_bits=9');

  // server_no_context_takeover offered: echoed (the server keeps it for free).
  const snct = negotiateServerDeflate('permessage-deflate; server_no_context_takeover', server)!;
  assert.equal(snct.response, 'permessage-deflate; client_no_context_takeover; server_no_context_takeover');

  // Invalid offers throw: the handshake must fail.
  for (const bad of [
    'permessage-deflate; server_max_window_bits=7',
    'permessage-deflate; server_max_window_bits=16',
    'permessage-deflate; server_max_window_bits=abc',
    'permessage-deflate; client_max_window_bits=3',
    'permessage-deflate; client_max_window_bits=99',
    'permessage-deflate; client_max_window_bits=',
    'permessage-deflate; x-unknown-param',
    'permessage-deflate; client_no_context_takeover=true',
  ]) {
    assert.throws(() => negotiateServerDeflate(bad, server), Error, bad);
  }
});

// --- unit: codec --------------------------------------------------------------

test('compressMessage/decompressMessage round-trip (RFC 7692 wire form)', async () => {
  const payload = Buffer.from('hello world, '.repeat(500), 'utf8');
  const compressed = await compressMessage(payload, 15);
  assert.ok(compressed.length < payload.length);
  // Trailing 00 00 ff ff stripped per RFC 7692 §7.2.1.
  assert.ok(!compressed.subarray(-4).equals(Buffer.from([0x00, 0x00, 0xff, 0xff])));
  const back = await decompressMessage(compressed);
  assert.ok(back.equals(payload));

  // Empty-ish payloads round-trip too.
  const tiny = await compressMessage(Buffer.from('x'), 9);
  assert.ok((await decompressMessage(tiny)).equals(Buffer.from('x')));

  await assert.rejects(decompressMessage(Buffer.from([0xde, 0xad, 0xbe, 0xef])), Error);
  await assert.rejects(decompressMessage(Buffer.alloc(0), 0), RangeError);
  // Bomb guard: a small frame that would expand past the cap rejects.
  await assert.rejects(
    decompressMessage(compressed, 10),
    (err: unknown) => err instanceof WsInflateBombError,
  );
});

test('maybeCompressMessage: threshold, shrink-only, and failure fallback', async () => {
  const repetitive = Buffer.from('ab'.repeat(2000), 'utf8');
  const below = await maybeCompressMessage(repetitive.subarray(0, 100), 1024, 15);
  assert.equal(below.compressed, false);
  assert.ok(below.bytes.equals(repetitive.subarray(0, 100)));

  const shrinks = await maybeCompressMessage(repetitive, 1024, 15);
  assert.equal(shrinks.compressed, true);
  assert.ok(shrinks.bytes.length < repetitive.length);

  // High-entropy payload above threshold: deflate cannot shrink it.
  const random = pseudoRandomBytes(4096, 1234);
  const noShrink = await maybeCompressMessage(random, 1024, 15);
  assert.equal(noShrink.compressed, false);
  assert.ok(noShrink.bytes.equals(random));
});

// --- integration: handshake ---------------------------------------------------

test('handshake: valid offer negotiates, invalid offer fails, no offer stays plaintext', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (_gw, port) => {
    // Valid offer -> 101 + extension response.
    const good = await rawConnect(port, 'permessage-deflate');
    assert.match(good.statusLine, /^HTTP\/1\.1 101/);
    assert.equal(
      good.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover',
    );
    good.client.destroy();

    // Invalid offer -> handshake failure.
    const bad = await rawConnect(port, 'permessage-deflate; server_max_window_bits=3');
    assert.match(bad.statusLine, /^HTTP\/1\.1 400/);
    await bad.client.waitClosed();

    const bad2 = await rawConnect(port, 'permessage-deflate; frobnicate');
    assert.match(bad2.statusLine, /^HTTP\/1\.1 400/);
    await bad2.client.waitClosed();

    // No offer -> plaintext 101 without the extension header.
    const plain = await rawConnect(port);
    assert.match(plain.statusLine, /^HTTP\/1\.1 101/);
    assert.equal(plain.headers.get('sec-websocket-extensions'), undefined);
    plain.client.destroy();
    await flush();
  });
});

test('handshake: disabled server ignores the offer (backwards compatible)', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const c = await rawConnect(port, 'permessage-deflate');
    assert.match(c.statusLine, /^HTTP\/1\.1 101/);
    assert.equal(c.headers.get('sec-websocket-extensions'), undefined);
    c.client.sendText({ type: 'ping', id: 1 });
    const { obj } = await c.client.nextJson();
    assert.equal(obj.type, 'pong');
    c.client.destroy();
    await flush();
    const stats = gw.getStats();
    assert.equal(stats.compressedFrames, 0);
    assert.equal(stats.deflatedBytes, 0);
    assert.equal(stats.savedBytes, 0);
  });
});

// --- integration: server -> client compression --------------------------------

test('server->client: threshold gates compression; round-trip content equality', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate');
    client.sendText({ type: 'subscribe', id: 1, pattern: 'news.*' });
    const sub = await client.nextJson();
    assert.equal(sub.obj.type, 'subscribed');

    // Below threshold (default 1024): plaintext frame, RSV1 clear.
    const smallPayload = 's'.repeat(100);
    bus.publish('news.small', smallPayload);
    const small = await client.nextFrame();
    assert.equal(small.opcode, 0x1);
    assert.equal(small.rsv1, false);
    assert.deepEqual(JSON.parse(small.payload.toString('utf8')).payload, smallPayload);

    // Above threshold and compressible: RSV1 frame.
    const bigPayload = { text: 'lorem ipsum '.repeat(300), n: 42, tags: ['a', 'b', 'c'] };
    bus.publish('news.big', bigPayload);
    const big = await client.nextFrame();
    assert.equal(big.opcode, 0x1);
    assert.equal(big.rsv1, true);
    const inflated = await decompressMessage(big.payload);
    const msg = JSON.parse(inflated.toString('utf8')) as Record<string, unknown>;
    assert.equal(msg.type, 'message');
    assert.deepEqual(msg.payload, bigPayload);

    // Wire bytes are really deflated: the plaintext must not appear.
    assert.ok(!big.payload.includes(Buffer.from('lorem ipsum')));

    client.destroy();
    await flush();
    const stats = gw.getStats();
    assert.ok(stats.compressedFrames >= 1);
    assert.ok(stats.deflatedBytes > 0);
    assert.ok(stats.savedBytes > 0);
  });
});

test('server->client: custom compressThresholdBytes disables compression for small payloads', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: { compressThresholdBytes: 1_000_000 } }, async (_gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate');
    client.sendText({ type: 'subscribe', id: 1, pattern: 't.*' });
    await client.nextJson();
    bus.publish('t.x', 'y'.repeat(5000));
    const frame = await client.nextFrame();
    assert.equal(frame.rsv1, false);
    assert.deepEqual(JSON.parse(frame.payload.toString('utf8')).payload, 'y'.repeat(5000));
    client.destroy();
    await flush();
  });
});

test('server->client: fresh deflate context per message (no context takeover)', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (_gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate');
    client.sendText({ type: 'subscribe', id: 1, pattern: 'dup.*' });
    await client.nextJson();
    const payload = 'identical payload '.repeat(200);
    bus.publish('dup.a', payload);
    bus.publish('dup.b', payload);
    const first = await client.nextFrame();
    const second = await client.nextFrame();
    assert.equal(first.rsv1, true);
    assert.equal(second.rsv1, true);
    const inflated1 = await decompressMessage(first.payload);
    const inflated2 = await decompressMessage(second.payload);
    const m1 = JSON.parse(inflated1.toString('utf8')) as Record<string, unknown>;
    const m2 = JSON.parse(inflated2.toString('utf8')) as Record<string, unknown>;
    assert.equal(m1.payload, payload);
    assert.equal(m2.payload, payload);
    // The server's bytes must be reproducible from a fresh deflate
    // context: recompressing the inflated message independently yields
    // the exact same wire bytes — proof no state leaked across messages.
    const fresh = await compressMessage(inflated1, 15);
    assert.ok(fresh.equals(first.payload));
    client.destroy();
    await flush();
  });
});

test('stats: deflatedBytes + savedBytes equals original wire bytes', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate');
    client.sendText({ type: 'subscribe', id: 1, pattern: 'st.*' });
    await client.nextJson();
    bus.publish('st.x', 'z'.repeat(3000));
    const frame = await client.nextFrame();
    assert.equal(frame.rsv1, true);
    const inflated = await decompressMessage(frame.payload);
    const originalLen = inflated.length;
    client.destroy();
    await flush();
    const stats = gw.getStats();
    assert.equal(stats.compressedFrames, 1);
    assert.equal(stats.deflatedBytes, frame.payload.length);
    assert.equal(stats.savedBytes, originalLen - frame.payload.length);
    assert.equal(stats.deflatedBytes + stats.savedBytes, originalLen);
  });
});

test('server->client: binary frames compress (EB-109 delivery)', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (_gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate');
    client.sendText({ type: 'subscribe', id: 1, pattern: 'bin.*', opts: { binary: true } });
    const sub = await client.nextJson();
    assert.equal(sub.obj.type, 'subscribed');

    const raw = Buffer.from('binary-body '.repeat(400), 'utf8');
    bus.publish('bin.x', raw, { binary: true });
    const frame = await client.nextFrame();
    assert.equal(frame.opcode, 0x2);
    assert.equal(frame.rsv1, true);
    const envelope = await decompressMessage(frame.payload);
    const headerLen = envelope.readUInt32BE(0);
    const header = JSON.parse(envelope.subarray(4, 4 + headerLen).toString('utf8'));
    assert.equal(header.encoding, 'binary');
    assert.ok(envelope.subarray(4 + headerLen).equals(raw));
    client.destroy();
    await flush();
  });
});

// --- integration: client -> server compressed frames --------------------------

test('client->server: compressed text publish is accepted and fanned out', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (_gw, port) => {
    const pub = await rawConnect(port, 'permessage-deflate');
    const sub = await rawConnect(port); // plaintext subscriber, no offer
    sub.client.sendText({ type: 'subscribe', id: 1, pattern: 'c2s.*' });
    await sub.client.nextJson();

    const payload = { hello: 'compressed world '.repeat(200) };
    await pub.client.sendCompressedText({ type: 'publish', id: 7, topic: 'c2s.t', payload });
    const reply = await pub.client.nextJson();
    assert.equal(reply.obj.type, 'published');
    assert.equal(reply.obj.delivered, 1);

    const msg = await sub.client.nextFrame();
    assert.equal(msg.rsv1, false); // subscriber did not negotiate: plaintext
    assert.deepEqual(JSON.parse(msg.payload.toString('utf8')).payload, payload);

    pub.client.destroy();
    sub.client.destroy();
    await flush();
  });
});

test('client->server: compressed binary publish (EB-109) is accepted', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (_gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate');
    const body = Buffer.from('raw-bytes-payload '.repeat(300), 'utf8');
    await client.sendBinaryPublish({ type: 'publish', id: 3, topic: 'c2s.bin' }, body, true);
    const reply = await client.nextJson();
    assert.equal(reply.obj.type, 'published');
    assert.equal(reply.obj.delivered, 0); // no subscribers, but admission ran
    client.destroy();
    await flush();
  });
});

test('client->server: context reuse across messages fails (client_no_context_takeover)', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (_gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate');
    const payload = Buffer.from('context-dependent '.repeat(200), 'utf8');

    // One raw deflate stream kept open across two messages: the second
    // message's LZ77 matches point into the first message's window.
    const stream = createDeflateRaw({ windowBits: 15 });
    const compressOnStream = (data: Buffer): Promise<Buffer> =>
      new Promise((resolve, reject) => {
        const chunks: Buffer[] = [];
        stream.on('error', reject);
        stream.write(data, () => {
          stream.flush(zlibConstants.Z_SYNC_FLUSH, () => {
            let chunk: Buffer | null;
            while ((chunk = stream.read()) !== null) chunks.push(chunk);
            const full = Buffer.concat(chunks);
            resolve(full.subarray(0, full.length - 4));
          });
        });
      });
    const pub1 = { type: 'publish', id: 1, topic: 'ctx.a', payload: payload.toString('utf8') };
    const c1 = await compressOnStream(Buffer.from(JSON.stringify(pub1), 'utf8'));
    client.socket.write(encodeClientFrame(c1, 0x1, true));
    const reply1 = await client.nextJson();
    assert.equal(reply1.obj.type, 'published');

    // Second message reuses the compressor's context: the server
    // inflates with a fresh context, so the back-references dangle
    // and the frame must fail (1007), closing the connection.
    const pub2 = { type: 'publish', id: 2, topic: 'ctx.b', payload: payload.toString('utf8') };
    const c2 = await compressOnStream(Buffer.from(JSON.stringify(pub2), 'utf8'));
    stream.destroy();
    client.socket.write(encodeClientFrame(c2, 0x1, true));
    const closeFrame = await client.nextFrame();
    assert.equal(closeFrame.opcode, 0x8);
    assert.equal(closeFrame.payload.readUInt16BE(0), 1007);
    await client.waitClosed();
    await flush();
  });
});

// --- integration: fallback when deflate does not shrink -----------------------

test('fallback: incompressible payload above threshold goes out plaintext', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: true }, async (_gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate');
    client.sendText({ type: 'subscribe', id: 1, pattern: 'nc.*' });
    await client.nextJson();

    // Deterministic high-entropy payload: deflate cannot shrink it.
    const payload = pseudoRandomBytes(4096, 987654321).toString('base64');

    bus.publish('nc.x', payload);
    const frame = await client.nextFrame();
    assert.equal(frame.opcode, 0x1);
    const plaintext = frame.rsv1 ? await decompressMessage(frame.payload) : frame.payload;
    // The server's decision must equal the pure function on the exact bytes.
    const expected = await maybeCompressMessage(plaintext, 1024, 15);
    assert.equal(frame.rsv1, expected.compressed);
    assert.deepEqual(JSON.parse(plaintext.toString('utf8')).payload, payload);
    client.destroy();
    await flush();
  });
});

// --- integration: window-bits negotiation on the wire -------------------------

test('handshake: window-bits offers are honored in the response', async () => {
  const bus = new EventBus();
  await withGateway(
    bus,
    { permessageDeflate: { serverMaxWindowBits: 12, clientMaxWindowBits: 11 } },
    async (_gw, port) => {
      const c1 = await rawConnect(port, 'permessage-deflate; server_max_window_bits=10');
      assert.equal(
        c1.headers.get('sec-websocket-extensions'),
        'permessage-deflate; client_no_context_takeover; server_max_window_bits=10',
      );
      c1.client.destroy();

      const c2 = await rawConnect(port, 'permessage-deflate; client_max_window_bits');
      assert.equal(
        c2.headers.get('sec-websocket-extensions'),
        'permessage-deflate; client_no_context_takeover; client_max_window_bits=11',
      );
      c2.client.destroy();

      // Bare offer: no window-bits params in the response.
      const c3 = await rawConnect(port, 'permessage-deflate');
      assert.equal(
        c3.headers.get('sec-websocket-extensions'),
        'permessage-deflate; client_no_context_takeover',
      );
      c3.client.destroy();
      await flush();
    },
  );
});

test('server compresses with the negotiated server window bits (round-trip)', async () => {
  const bus = new EventBus();
  await withGateway(bus, { permessageDeflate: { serverMaxWindowBits: 9 } }, async (_gw, port) => {
    const { client } = await rawConnect(port, 'permessage-deflate; server_max_window_bits=9');
    // A client that offers nothing still connects in plaintext on an enabled server.
    const plain = await rawConnect(port);
    assert.equal(plain.headers.get('sec-websocket-extensions'), undefined);
    plain.client.destroy();
    client.sendText({ type: 'subscribe', id: 1, pattern: 'wb.*' });
    await client.nextJson();
    const payload = 'window-bits-nine '.repeat(300);
    bus.publish('wb.x', payload);
    const frame = await client.nextFrame();
    assert.equal(frame.rsv1, true);
    const msg = JSON.parse((await decompressMessage(frame.payload)).toString('utf8'));
    assert.equal(msg.payload, payload);
    client.destroy();
    await flush();
  });
});
