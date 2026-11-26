/**
 * GraphQL gateway permessage-deflate tuning revision (EB-563).
 *
 * `setGraphqlGatewayDeflate` retunes `compressThresholdBytes`,
 * `serverMaxWindowBits`, `clientMaxWindowBits` and `maxInflatedBytes`
 * live — without restarting the server. Only the fields provided move;
 * an illegal value fails closed with `RangeError` and the live tuning
 * untouched; an empty or identical revision is a no-op. The threshold
 * and the bomb cap govern from the next message / frame on every
 * connection, existing ones included; the window bits bind only
 * connections established after the revision — a connection that is
 * already open keeps the window bits its handshake negotiated. Only a
 * gateway that already enabled permessage-deflate can be revised: a
 * never-enabled gateway refuses the revision with `RangeError` (live
 * enable is EB-564), and the extension can never be cleared or
 * disabled through a revision (an explicitly `undefined` field keeps
 * its live value).
 * There is no audit event for this revision — the live tuning is
 * observable in `getStats().deflate` instead.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GraphqlGateway } from '../src/graphql-gw.ts';
import { compressMessage, decompressMessage } from '../src/gateway-deflate.ts';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

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

const DEFAULT_DEFLATE = {
  compressThresholdBytes: 1024,
  serverMaxWindowBits: 15,
  clientMaxWindowBits: 15,
  maxInflatedBytes: 8 * 1024 * 1024,
};

test('setGraphqlGatewayDeflate on a never-enabled gateway refuses the revision (EB-563)', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0 });
  assert.equal(gw.getStats().deflate, undefined);
  // An absent revision asks nothing to move, even on a disabled gateway.
  gw.setGraphqlGatewayDeflate(null as never);
  gw.setGraphqlGatewayDeflate(undefined as never);
  assert.equal(gw.getStats().deflate, undefined);
  // A non-object revision still throws on a disabled gateway, and the
  // gateway stays disabled.
  assert.throws(() => gw.setGraphqlGatewayDeflate('nope' as never), RangeError);
  assert.equal(gw.getStats().deflate, undefined);
  // A well-formed revision on a never-enabled gateway throws — the
  // setter revises only; live enable is EB-564.
  assert.throws(() => gw.setGraphqlGatewayDeflate({ compressThresholdBytes: 2048 }), RangeError);
  assert.equal(gw.getStats().deflate, undefined);
  // Even an empty revision refuses: there is nothing live to revise.
  assert.throws(() => gw.setGraphqlGatewayDeflate({}), RangeError);
  assert.equal(gw.getStats().deflate, undefined);
});

test('setGraphqlGatewayDeflate rejects illegal values and leaves the live tuning untouched', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0, permessageDeflate: true });
  assert.deepEqual(gw.getStats().deflate, DEFAULT_DEFLATE);
  const bad: Array<unknown> = [
    { compressThresholdBytes: 0 },
    { compressThresholdBytes: -1 },
    { compressThresholdBytes: 1.5 },
    { compressThresholdBytes: Number.NaN },
    { compressThresholdBytes: '1024' },
    { compressThresholdBytes: Infinity },
    { serverMaxWindowBits: 7 },
    { serverMaxWindowBits: 16 },
    { serverMaxWindowBits: 8.5 },
    { serverMaxWindowBits: '12' },
    { serverMaxWindowBits: Number.NaN },
    { clientMaxWindowBits: 7 },
    { clientMaxWindowBits: 16 },
    { clientMaxWindowBits: 8.5 },
    { clientMaxWindowBits: '12' },
    { maxInflatedBytes: 0 },
    { maxInflatedBytes: -1 },
    { maxInflatedBytes: 1.5 },
    { maxInflatedBytes: Number.NaN },
    { maxInflatedBytes: '100' },
    { maxInflatedBytes: Infinity },
    // One bad field poisons the whole revision, even with good ones.
    { compressThresholdBytes: 2048, serverMaxWindowBits: 7 },
    { maxInflatedBytes: 100, clientMaxWindowBits: 99 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => gw.setGraphqlGatewayDeflate(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    assert.deepEqual(gw.getStats().deflate, DEFAULT_DEFLATE);
  }
});

test('partial revision moves only the fields provided', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0, permessageDeflate: true });
  gw.setGraphqlGatewayDeflate({ compressThresholdBytes: 2048 });
  assert.deepEqual(gw.getStats().deflate, { ...DEFAULT_DEFLATE, compressThresholdBytes: 2048 });
  gw.setGraphqlGatewayDeflate({ serverMaxWindowBits: 9, clientMaxWindowBits: 12 });
  assert.deepEqual(gw.getStats().deflate, {
    ...DEFAULT_DEFLATE,
    compressThresholdBytes: 2048,
    serverMaxWindowBits: 9,
    clientMaxWindowBits: 12,
  });
  gw.setGraphqlGatewayDeflate({ maxInflatedBytes: 4096 });
  assert.deepEqual(gw.getStats().deflate, {
    compressThresholdBytes: 2048,
    serverMaxWindowBits: 9,
    clientMaxWindowBits: 12,
    maxInflatedBytes: 4096,
  });
  // Explicitly `undefined` is not a value: the field keeps its live
  // value — there is no way to clear a field through a revision.
  gw.setGraphqlGatewayDeflate({ compressThresholdBytes: undefined, maxInflatedBytes: 8192 });
  assert.deepEqual(gw.getStats().deflate, {
    compressThresholdBytes: 2048,
    serverMaxWindowBits: 9,
    clientMaxWindowBits: 12,
    maxInflatedBytes: 8192,
  });
});

test('identical and empty revisions are no-ops', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, {
    port: 0,
    permessageDeflate: { compressThresholdBytes: 512, serverMaxWindowBits: 10 },
  });
  const live = {
    compressThresholdBytes: 512,
    serverMaxWindowBits: 10,
    clientMaxWindowBits: 15,
    maxInflatedBytes: 8 * 1024 * 1024,
  };
  assert.deepEqual(gw.getStats().deflate, live);
  gw.setGraphqlGatewayDeflate({});
  gw.setGraphqlGatewayDeflate({ compressThresholdBytes: 512 });
  gw.setGraphqlGatewayDeflate({ ...live });
  gw.setGraphqlGatewayDeflate(null as never);
  gw.setGraphqlGatewayDeflate(undefined as never);
  assert.deepEqual(gw.getStats().deflate, live);
});

test('getStats().deflate reports the live tuning', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, {
    port: 0,
    permessageDeflate: {
      compressThresholdBytes: 100,
      serverMaxWindowBits: 8,
      clientMaxWindowBits: 12,
      maxInflatedBytes: 4096,
    },
  });
  assert.deepEqual(gw.getStats().deflate, {
    compressThresholdBytes: 100,
    serverMaxWindowBits: 8,
    clientMaxWindowBits: 12,
    maxInflatedBytes: 4096,
  });
  gw.setGraphqlGatewayDeflate({ compressThresholdBytes: 200, maxInflatedBytes: 8192 });
  assert.deepEqual(gw.getStats().deflate, {
    compressThresholdBytes: 200,
    serverMaxWindowBits: 8,
    clientMaxWindowBits: 12,
    maxInflatedBytes: 8192,
  });
  // The stats object is a copy: mutating it never moves the live tuning.
  const stats = gw.getStats();
  (stats.deflate as { compressThresholdBytes: number }).compressThresholdBytes = 1;
  assert.equal(gw.getStats().deflate!.compressThresholdBytes, 200);
});

test('compressThresholdBytes governs the next message on an existing connection', async () => {
  const bus = new EventBus();
  await withGqlGateway(
    bus,
    { port: 0, permessageDeflate: { compressThresholdBytes: 1_000_000 } },
    async (gw, port) => {
      const { client } = await connectAndInit(port, 'permessage-deflate');
      try {
        client.sendJson({
          type: 'subscribe',
          id: 's1',
          payload: { query: SUB('topic: "t"') },
        });
        await waitForSubscribers(bus, 1, 'subscribe s1');

        // Above the old 1 MiB threshold nothing compresses.
        bus.publish('t', 'y'.repeat(5000));
        const plain = await client.nextFrame();
        assert.equal(plain.opcode, 0x1);
        assert.equal(plain.rsv1, false);
        assert.deepEqual(
          JSON.parse(plain.payload.toString('utf8')).payload,
          { topic: 't', payload: 'y'.repeat(5000) },
        );

        // Lower the threshold: the next message on this same connection compresses.
        gw.setGraphqlGatewayDeflate({ compressThresholdBytes: 100 });
        assert.equal(gw.getStats().deflate!.compressThresholdBytes, 100);
        bus.publish('t', 'y'.repeat(5000));
        const compressed = await client.nextFrame();
        assert.equal(compressed.opcode, 0x1);
        assert.equal(compressed.rsv1, true);
        const inflated = await decompressMessage(compressed.payload);
        assert.deepEqual(
          JSON.parse(inflated.toString('utf8')).payload,
          { topic: 't', payload: 'y'.repeat(5000) },
        );

        // Raise it back: the next message is plaintext again.
        gw.setGraphqlGatewayDeflate({ compressThresholdBytes: 1_000_000 });
        bus.publish('t', 'y'.repeat(5000));
        const plainAgain = await client.nextFrame();
        assert.equal(plainAgain.opcode, 0x1);
        assert.equal(plainAgain.rsv1, false);
      } finally {
        client.destroy();
      }
    },
  );
});

test('maxInflatedBytes governs the next inbound frame on an existing connection', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (gw, port) => {
    const { client } = await connectAndInit(port, 'permessage-deflate');
    try {
      // A compressed subscribe proves compressed inbound works.
      await client.sendCompressedJson({
        type: 'subscribe',
        id: 's1',
        payload: { query: SUB('topic: "in.t"') },
      });
      await waitForSubscribers(bus, 1, 'subscribe s1');
      const payload = { hello: 'compressed world '.repeat(200) };
      bus.publish('in.t', payload);
      const fanned = await client.nextFrame();
      assert.equal(fanned.opcode, 0x1);
      assert.equal(fanned.rsv1, true);
      assert.deepEqual(
        JSON.parse((await decompressMessage(fanned.payload)).toString('utf8')).payload,
        { topic: 'in.t', payload },
      );

      // Tighten the bomb cap below the inflated size: the next
      // compressed frame from this same connection is rejected.
      gw.setGraphqlGatewayDeflate({ maxInflatedBytes: 100 });
      assert.equal(gw.getStats().deflate!.maxInflatedBytes, 100);
      await client.sendCompressedJson({
        type: 'ping',
        payload: { pad: 'x'.repeat(2000) },
      });
      const closeFrame = await client.nextFrame();
      assert.equal(closeFrame.opcode, 0x8);
      assert.equal(closeFrame.payload.readUInt16BE(0), 1009);
      await client.waitClosed();
    } finally {
      client.destroy();
    }
  });
});

test('window bits bind only connections established after the revision', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (gw, port) => {
    const existing = await connectAndInit(port, 'permessage-deflate; server_max_window_bits=10');
    assert.equal(
      existing.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover; server_max_window_bits=10',
    );
    try {
      existing.client.sendJson({
        type: 'subscribe',
        id: 's1',
        payload: { query: SUB('topic: "wb.x"') },
      });
      await waitForSubscribers(bus, 1, 'subscribe s1');
      const payload = 'window-bits-ten '.repeat(300);
      bus.publish('wb.x', payload);
      const before = await existing.client.nextFrame();
      assert.equal(before.opcode, 0x1);
      assert.equal(before.rsv1, true);
      const inflatedBefore = await decompressMessage(before.payload);
      assert.deepEqual(
        JSON.parse(inflatedBefore.toString('utf8')).payload,
        { topic: 'wb.x', payload },
      );
      // The pre-revision frame was compressed with window bits 10.
      assert.ok((await compressMessage(inflatedBefore, 10)).equals(before.payload));

      gw.setGraphqlGatewayDeflate({ serverMaxWindowBits: 9, clientMaxWindowBits: 12 });
      assert.deepEqual(gw.getStats().deflate, {
        ...DEFAULT_DEFLATE,
        serverMaxWindowBits: 9,
        clientMaxWindowBits: 12,
      });

      // The existing connection keeps the window bits it negotiated.
      bus.publish('wb.x', payload);
      const after = await existing.client.nextFrame();
      assert.equal(after.opcode, 0x1);
      assert.equal(after.rsv1, true);
      const inflatedAfter = await decompressMessage(after.payload);
      assert.deepEqual(
        JSON.parse(inflatedAfter.toString('utf8')).payload,
        { topic: 'wb.x', payload },
      );
      assert.ok((await compressMessage(inflatedAfter, 10)).equals(after.payload));
    } finally {
      existing.client.destroy();
    }

    // A connection established after the revision negotiates the new bits.
    const freshServer = await rawGqlConnect(port, 'permessage-deflate; server_max_window_bits');
    assert.equal(
      freshServer.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover; server_max_window_bits=9',
    );
    freshServer.client.destroy();

    const freshClient = await rawGqlConnect(port, 'permessage-deflate; client_max_window_bits');
    assert.equal(
      freshClient.headers.get('sec-websocket-extensions'),
      'permessage-deflate; client_no_context_takeover; client_max_window_bits=12',
    );
    freshClient.client.destroy();
  });
});

test('there is no way to clear or disable the extension through a revision', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, permessageDeflate: true }, async (gw, port) => {
    const { client } = await connectAndInit(port, 'permessage-deflate');
    try {
      client.sendJson({
        type: 'subscribe',
        id: 's1',
        payload: { query: SUB('topic: "t"') },
      });
      await waitForSubscribers(bus, 1, 'subscribe s1');
      // An all-undefined revision is a no-op: the live tuning — and the
      // negotiated connection — are untouched.
      gw.setGraphqlGatewayDeflate({
        compressThresholdBytes: undefined,
        serverMaxWindowBits: undefined,
        clientMaxWindowBits: undefined,
        maxInflatedBytes: undefined,
      });
      assert.deepEqual(gw.getStats().deflate, DEFAULT_DEFLATE);
      assert.equal(gw.getStats().connectionsDetail[0]?.deflate, true);
      // The still-negotiated connection keeps receiving compressed frames.
      const big = 'z'.repeat(4000);
      bus.publish('t', big);
      const frame = await client.nextFrame();
      assert.equal(frame.opcode, 0x1);
      assert.equal(frame.rsv1, true);
      const obj = JSON.parse(
        (await decompressMessage(frame.payload)).toString('utf8'),
      ) as Record<string, unknown>;
      assert.deepEqual(obj.payload, { topic: 't', payload: big });
    } finally {
      client.destroy();
    }
  });
});
