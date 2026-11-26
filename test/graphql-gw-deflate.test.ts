/**
 * GraphQL gateway permessage-deflate (RFC 7692, EB-562).
 *
 * Mirrors `test/gateway-deflate.test.ts` (WS gateway) at the wire
 * level: raw TCP + handshake, masked client frames, raw server frames
 * with the RSV1 flag visible. The gateway reuses the shared helpers
 * in `src/gateway-deflate.ts` (EB-206) — option resolution, offer
 * negotiation, the RFC 7692 wire codec and the bomb guard — so the
 * unit coverage of those helpers lives there; this file pins the
 * GraphQL gateway's own wiring: handshake selection, outbound
 * compression of `next` frames, inbound decompression of client
 * frames, fragmentation, and the `getStats()` observability.
 *
 * One behaviour cannot be driven through the GraphQL wire: the
 * shrink-only fallback (a payload whose compressed form is not
 * smaller goes out plaintext). Every JSON text frame compresses at
 * scale — a JSON string carries under 8 bits of entropy per byte, and
 * the fixed `next` envelope always saves ~25 bytes — so no JSON
 * payload can make `compressed.length >= payload.length`. The
 * fallback is pinned at the helper the gateway calls
 * (`maybeCompressMessage`, unit-tested in
 * `test/gateway-deflate.test.ts`) instead; see the
 * `maybeCompressMessage fallback` test below.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import {
  createDeflateRaw,
  constants as zlibConstants,
} from 'node:zlib';
import { EventBus } from '../src/bus.ts';
import { GraphqlGateway } from '../src/graphql-gw.ts';
import {
  compressMessage,
  decompressMessage,
  maybeCompressMessage,
} from '../src/gateway-deflate.ts';

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

/** Client -> server: masked frame, optionally RSV1, optionally fragmented. */
function encodeClientFrame(
  payload: Buffer,
  opcode = 0x1,
  rsv1 = false,
  fin = true,
): Buffer {
  const len = payload.length;
  const b0 = (fin ? 0x80 : 0) | (rsv1 ? 0x40 : 0) | opcode;
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
  closeCode: number | undefined;
  readonly socket: Socket;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.push(chunk)) {
        if (frame.opcode === 0x8) {
          this.closed = true;
          this.closeCode =
            frame.payload.length >= 2 ? frame.payload.readUInt16BE(0) : undefined;
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

  sendJson(value: unknown): void {
    this.socket.write(
      encodeClientFrame(Buffer.from(JSON.stringify(value), 'utf8')),
    );
  }

  /** Client -> server compressed text frame (fresh context per message). */
  async sendCompressedJson(value: unknown): Promise<void> {
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

  /** Next text frame, decompressed when RSV1 — skips control frames. */
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

/**
 * Compresses several messages with ONE shared deflate context — the
 * shape a client using context takeover would produce. The server
 * requires `client_no_context_takeover`, so the second message must
 * fail to decode against a fresh inflate context.
 */
class SharedContextCompressor {
  private readonly deflate = createDeflateRaw({ windowBits: 15 });

  compress(payload: Buffer): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
      this.deflate.once('error', reject);
      this.deflate.write(payload, () => {
        this.deflate.flush(zlibConstants.Z_SYNC_FLUSH, () => {
          const chunks: Buffer[] = [];
          let chunk: Buffer | null;
          while ((chunk = this.deflate.read()) !== null) chunks.push(chunk);
          const full = Buffer.concat(chunks);
          resolve(full.subarray(0, full.length - 4));
        });
      });
    });
  }

  destroy(): void {
    this.deflate.destroy();
  }
}

interface ConnectedClient {
  client: RawClient;
  statusLine: string;
  headers: Map<string, string>;
}

async function rawGqlConnect(
  port: number,
  extOffer?: string,
): Promise<ConnectedClient> {
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
    'GET /graphql HTTP/1.1',
    'Host: 127.0.0.1',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Key: ${key}`,
    'Sec-WebSocket-Version: 13',
    'Sec-WebSocket-Protocol: graphql-ws',
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

type GatewayCtorOptions = ConstructorParameters<typeof GraphqlGateway>[1];

async function withGqlGateway(
  bus: EventBus,
  options: GatewayCtorOptions,
  fn: (gw: GraphqlGateway, port: number) => Promise<void>,
): Promise<void> {
  const gw = new GraphqlGateway(bus, options);
  await gw.start();
  try {
    await fn(gw, gw.getPort());
  } finally {
    await gw.stop();
  }
}

async function waitForSubscribers(bus: EventBus, n: number, what: string): Promise<void> {
  const deadline = Date.now() + 5000;
  for (;;) {
    if (bus.getStats().totalSubscribers >= n) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(10);
  }
}

const SUB = (topicArg: string, fields = 'topic payload') =>
  `subscription { events(${topicArg}) { ${fields} } }`;

/** Connect + connection_init, returning the client after the ack. */
async function connectAndInit(
  port: number,
  extOffer?: string,
): Promise<{ client: RawClient; headers: Map<string, string> }> {
  const { client, statusLine, headers } = await rawGqlConnect(port, extOffer);
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  client.sendJson({ type: 'connection_init' });
  const { obj } = await client.nextJson();
  assert.deepEqual(obj, { type: 'connection_ack' });
  return { client, headers };
}

// --- handshake ---------------------------------------------------------------

test('handshake: valid offer negotiates, invalid offer fails, no offer stays plaintext', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (_gw, port) => {
    // Valid offer -> 101 + extension response.
    const good = await rawGqlConnect(port, 'permessage-deflate');
    assert.match(good.statusLine, /^HTTP\/1\.1 101/);
    assert.equal(
      good.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover',
    );
    good.client.destroy();

    // Invalid window bits -> handshake failure.
    const bad = await rawGqlConnect(port, 'permessage-deflate; server_max_window_bits=3');
    assert.match(bad.statusLine, /^HTTP\/1\.1 400/);
    await bad.client.waitClosed();

    // Unknown parameter -> handshake failure.
    const bad2 = await rawGqlConnect(port, 'permessage-deflate; frobnicate');
    assert.match(bad2.statusLine, /^HTTP\/1\.1 400/);
    await bad2.client.waitClosed();

    // No offer -> plaintext 101 without the extension header.
    const plain = await rawGqlConnect(port);
    assert.match(plain.statusLine, /^HTTP\/1\.1 101/);
    assert.equal(plain.headers.get('sec-websocket-extensions'), undefined);
    plain.client.destroy();
  });
});

test('handshake: client_no_context_takeover is always required in the response', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (_gw, port) => {
    // Bare offer: the server mandates client_no_context_takeover.
    const bare = await rawGqlConnect(port, 'permessage-deflate');
    assert.equal(
      bare.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover',
    );
    bare.client.destroy();

    // Offered explicitly: not duplicated.
    const offered = await rawGqlConnect(port, 'permessage-deflate; client_no_context_takeover');
    assert.equal(
      offered.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover',
    );
    offered.client.destroy();

    // server_no_context_takeover is echoed (the server keeps it for free).
    const snct = await rawGqlConnect(port, 'permessage-deflate; server_no_context_takeover');
    assert.equal(
      snct.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover; server_no_context_takeover',
    );
    snct.client.destroy();
  });
});

test('handshake: disabled server ignores the offer (backwards compatible)', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const c = await rawGqlConnect(port, 'permessage-deflate');
    assert.match(c.statusLine, /^HTTP\/1\.1 101/);
    assert.equal(c.headers.get('sec-websocket-extensions'), undefined);
    assert.equal(gw.getStats().deflate, undefined);
    c.client.destroy();
  });
});

// --- outbound ----------------------------------------------------------------

test('outbound: large next frame goes out RSV1-compressed, small stays plaintext', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (_gw, port) => {
    const { client } = await connectAndInit(port, 'permessage-deflate');
    try {
      client.sendJson({
        type: 'subscribe',
        id: 's1',
        payload: { query: SUB('topic: "t"') },
      });
      await waitForSubscribers(bus, 1, 'subscribe s1');

      // Large repetitive payload: above the 1024 default threshold and
      // compressible -> RSV1, decompresses to the original next frame.
      const big = 'x'.repeat(4000);
      bus.publish('t', big);
      const frame = await client.nextFrame();
      assert.equal(frame.opcode, 0x1);
      assert.equal(frame.rsv1, true);
      const obj = JSON.parse(
        (await decompressMessage(frame.payload)).toString('utf8'),
      ) as Record<string, unknown>;
      assert.equal(obj.type, 'next');
      assert.equal(obj.id, 's1');
      assert.deepEqual(obj.payload, { topic: 't', payload: big });

      // Small payload: below threshold -> plaintext.
      bus.publish('t', 'hi');
      const small = await client.nextFrame();
      assert.equal(small.opcode, 0x1);
      assert.equal(small.rsv1, false);
      const smallObj = JSON.parse(small.payload.toString('utf8')) as Record<string, unknown>;
      assert.equal(smallObj.type, 'next');
      assert.deepEqual(smallObj.payload, { topic: 't', payload: 'hi' });
    } finally {
      client.destroy();
    }
  });
});

test('outbound: no-offer connection stays plaintext even for large payloads', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (_gw, port) => {
    const { client } = await connectAndInit(port);
    try {
      client.sendJson({
        type: 'subscribe',
        id: 's1',
        payload: { query: SUB('topic: "t"') },
      });
      await waitForSubscribers(bus, 1, 'subscribe s1');
      bus.publish('t', 'y'.repeat(4000));
      const frame = await client.nextFrame();
      assert.equal(frame.opcode, 0x1);
      assert.equal(frame.rsv1, false);
      const obj = JSON.parse(frame.payload.toString('utf8')) as Record<string, unknown>;
      assert.equal(obj.type, 'next');
      assert.equal((obj.payload as Record<string, unknown>).payload, 'y'.repeat(4000));
    } finally {
      client.destroy();
    }
  });
});

test('maybeCompressMessage fallback: incompressible bytes stay plaintext', async () => {
  // The gateway sends every outbound frame through
  // `maybeCompressMessage`; a JSON wire frame can never trigger the
  // fallback (see the file header), so this pins the helper contract
  // the gateway relies on: high-entropy bytes above threshold are
  // returned untouched, never expanded.
  const random = pseudoRandomBytes(4096, 1234);
  const res = await maybeCompressMessage(random, 1024, 15);
  assert.equal(res.compressed, false);
  assert.ok(res.bytes.equals(random));
});

// --- inbound -----------------------------------------------------------------

test('inbound: client compressed frames are decompressed and work', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (_gw, port) => {
    const { client } = await connectAndInit(port, 'permessage-deflate');
    try {
      // Compressed connection_init is already covered by
      // connectAndInit's plaintext form; here the subscribe itself
      // arrives compressed (fresh context per message).
      await client.sendCompressedJson({
        type: 'subscribe',
        id: 's1',
        payload: { query: SUB('topic: "t"') },
      });
      await waitForSubscribers(bus, 1, 'compressed subscribe s1');
      bus.publish('t', 'hello');
      const { obj } = await client.nextJson();
      assert.equal(obj.type, 'next');
      assert.deepEqual(obj.payload, { topic: 't', payload: 'hello' });
    } finally {
      client.destroy();
    }
  });
});

test('inbound: compressed connection_init works (fresh context per message)', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (_gw, port) => {
    const { client } = await rawGqlConnect(port, 'permessage-deflate');
    try {
      await client.sendCompressedJson({ type: 'connection_init' });
      const { obj } = await client.nextJson();
      assert.deepEqual(obj, { type: 'connection_ack' });
    } finally {
      client.destroy();
    }
  });
});

test('inbound: fragmented compressed message reassembles and decompresses', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (_gw, port) => {
    const { client } = await connectAndInit(port, 'permessage-deflate');
    try {
      // Compress one large subscribe, then split the compressed bytes
      // across two frames: RSV1 only on the first fragment (RFC 7692).
      const bytes = Buffer.from(
        JSON.stringify({
          type: 'subscribe',
          id: 's1',
          payload: { query: SUB('topic: "t"') },
        }),
        'utf8',
      );
      const compressed = await compressMessage(bytes, 15);
      const half = Math.floor(compressed.length / 2);
      client.socket.write(
        encodeClientFrame(compressed.subarray(0, half), 0x1, true, false),
      );
      client.socket.write(
        encodeClientFrame(compressed.subarray(half), 0x0, false, true),
      );
      await waitForSubscribers(bus, 1, 'fragmented subscribe s1');
      bus.publish('t', 'frag-ok');
      const { obj } = await client.nextJson();
      assert.equal(obj.type, 'next');
      assert.deepEqual(obj.payload, { topic: 't', payload: 'frag-ok' });
    } finally {
      client.destroy();
    }
  });
});

test('inbound: reusing context across messages fails the connection (1007)', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (_gw, port) => {
    const { client } = await rawGqlConnect(port, 'permessage-deflate');
    const shared = new SharedContextCompressor();
    try {
      // Both messages carry the same long run, so the second message's
      // compressed form references the first message's window — a
      // fresh inflate context cannot decode it.
      const run = 'a'.repeat(500);
      const init = await shared.compress(
        Buffer.from(
          JSON.stringify({ type: 'connection_init', payload: { seed: run } }),
          'utf8',
        ),
      );
      client.socket.write(encodeClientFrame(init, 0x1, true));
      const { obj } = await client.nextJson();
      assert.deepEqual(obj, { type: 'connection_ack' });

      const sub = await shared.compress(
        Buffer.from(
          JSON.stringify({
            type: 'subscribe',
            id: 's1',
            payload: { query: SUB(`topic: "${run}"`) },
          }),
          'utf8',
        ),
      );
      client.socket.write(encodeClientFrame(sub, 0x1, true));
      await client.waitClosed();
      assert.equal(client.closeCode, 1007);
      assert.ok(_gw.getStats().protocolErrors >= 1);
    } finally {
      shared.destroy();
      client.destroy();
    }
  });
});

test('inbound: decompression bomb closes the connection (1009)', async () => {
  const bus = new EventBus();
  await withGqlGateway(
    bus,
    { port: 0, permessageDeflate: { maxInflatedBytes: 64 } },
    async (gw, port) => {
      const { client } = await connectAndInit(port, 'permessage-deflate');
      try {
        // The subscribe inflates to ~105 bytes, past the 64-byte cap.
        await client.sendCompressedJson({
          type: 'subscribe',
          id: 's1',
          payload: { query: SUB('topic: "t"') },
        });
        await client.waitClosed();
        assert.equal(client.closeCode, 1009);
        assert.ok(gw.getStats().protocolErrors >= 1);
      } finally {
        client.destroy();
      }
    },
  );
});

test('inbound: RSV1 without negotiation closes the connection (4400)', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (gw, port) => {
    // No offer: the connection is plaintext.
    const { client } = await rawGqlConnect(port);
    try {
      await client.sendCompressedJson({ type: 'connection_init' });
      await client.waitClosed();
      assert.equal(client.closeCode, 4400);
      assert.ok(gw.getStats().protocolErrors >= 1);
    } finally {
      client.destroy();
    }
  });
});

// --- observability + construction --------------------------------------------

test('getStats: deflate reflects construction; connectionsDetail flags negotiation', async () => {
  const bus = new EventBus();
  await withGqlGateway(
    bus,
    { port: 0, permessageDeflate: { compressThresholdBytes: 512 } },
    async (gw, port) => {
      assert.deepEqual(gw.getStats().deflate, {
        compressThresholdBytes: 512,
        serverMaxWindowBits: 15,
        clientMaxWindowBits: 15,
        maxInflatedBytes: 8 * 1024 * 1024,
      });
      const negotiated = await connectAndInit(port, 'permessage-deflate');
      const plain = await connectAndInit(port);
      try {
        assert.deepEqual(
          gw.getStats().connectionsDetail.map((row) => row.deflate),
          [true, false],
        );
      } finally {
        negotiated.client.destroy();
        plain.client.destroy();
      }
    },
  );

  const bus2 = new EventBus();
  await withGqlGateway(bus2, { port: 0 }, async (gw) => {
    assert.equal(gw.getStats().deflate, undefined);
  });
});

test('construction: invalid permessageDeflate values throw RangeError', () => {
  assert.throws(
    () => new GraphqlGateway(new EventBus(), { port: 0, permessageDeflate: { serverMaxWindowBits: 7 } }),
    RangeError,
  );
  assert.throws(
    () => new GraphqlGateway(new EventBus(), { port: 0, permessageDeflate: { clientMaxWindowBits: 16 } }),
    RangeError,
  );
  assert.throws(
    () => new GraphqlGateway(new EventBus(), { port: 0, permessageDeflate: { compressThresholdBytes: 0 } }),
    RangeError,
  );
  assert.throws(
    () => new GraphqlGateway(new EventBus(), { port: 0, permessageDeflate: { maxInflatedBytes: -1 } }),
    RangeError,
  );
  assert.throws(
    () => new GraphqlGateway(new EventBus(), { port: 0, permessageDeflate: null as never }),
    RangeError,
  );
});
