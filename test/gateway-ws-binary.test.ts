import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';

/**
 * Binary frames (EB-109): the gateway accepts binary `publish` frames
 * (`[u32 BE headerLen][UTF-8 JSON header][raw bytes]`) and delivers to
 * binary subscribers as binary `message` frames with the same envelope.
 * This file carries its own minimal WS client because the shared one in
 * `test/gateway-ws.test.ts` only speaks text frames.
 */

const WS_GUID = '258EAFA5-E91447DA-C5AB0DC85B11';

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

/** Client -> server: masked frame (RFC 6455 clients must mask). */
function encodeClientFrame(payload: Buffer, opcode = 0x1): Buffer {
  const len = payload.length;
  const b0 = 0x80 | opcode;
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

/** Builds a client->server binary `publish` frame body. */
function encodeBinaryPublish(topic: string, body: Buffer, opts?: object): Buffer {
  const header = Buffer.from(JSON.stringify({ type: 'publish', id: 'p1', topic, opts }), 'utf8');
  const out = Buffer.alloc(4 + header.length + body.length);
  out.writeUInt32BE(header.length, 0);
  header.copy(out, 4);
  body.copy(out, 4 + header.length);
  return out;
}

function parseBinaryEnvelope(frame: Buffer): { header: Record<string, unknown>; body: Buffer } {
  const headerLen = frame.readUInt32BE(0);
  const header = JSON.parse(frame.subarray(4, 4 + headerLen).toString('utf8')) as Record<
    string,
    unknown
  >;
  return { header, body: Buffer.from(frame.subarray(4 + headerLen)) };
}

interface RawFrame {
  opcode: number;
  payload: Buffer;
}

/** Minimal client-side frame parser (server frames are never masked). */
class ClientParser {
  private buf = Buffer.alloc(0);

  push(chunk: Buffer): RawFrame[] {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    const out: RawFrame[] = [];
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return out;
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
      out.push({ opcode, payload: b.subarray(off, off + len) });
      this.buf = b.subarray(off + len);
    }
  }
}

class BinaryWsClient {
  private readonly parser = new ClientParser();
  private readonly queue: RawFrame[] = [];
  private waiters: Array<(f: RawFrame) => void> = [];
  private closed = false;
  private readonly socket: Socket;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.parser.push(chunk)) {
        if (frame.opcode === 0x8) {
          this.closed = true;
          for (const w of this.waiters) w({ opcode: 0x8, payload: Buffer.alloc(0) });
          this.waiters = [];
          continue;
        }
        if (frame.opcode === 0x9) {
          socket.write(encodeClientFrame(frame.payload, 0xa));
          continue;
        }
        const waiter = this.waiters.shift();
        if (waiter !== undefined) waiter(frame);
        else this.queue.push(frame);
      }
    });
    socket.on('close', () => {
      this.closed = true;
    });
    socket.on('error', () => {});
  }

  sendJson(obj: unknown): void {
    this.socket.write(encodeClientFrame(Buffer.from(JSON.stringify(obj), 'utf8'), 0x1));
  }

  sendBinaryFrame(frame: Buffer): void {
    this.socket.write(encodeClientFrame(frame, 0x2));
  }

  /** Raw socket write, for TCP-fragmentation tests. */
  writeRaw(chunk: Buffer): void {
    this.socket.write(chunk);
  }

  nextFrame(): Promise<RawFrame> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.closed) return Promise.reject(new Error('connection closed'));
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  isClosed(): boolean {
    return this.closed;
  }

  pause(): void {
    this.socket.pause();
  }

  destroy(): void {
    this.socket.destroy();
  }
}

async function connect(port: number): Promise<BinaryWsClient> {
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
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  return new BinaryWsClient(socket);
}

async function withGateway(
  bus: EventBus,
  options: ConstructorParameters<typeof GatewayServer>[1],
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

async function nextJson(client: BinaryWsClient, what: string): Promise<Record<string, unknown>> {
  const frame = await withTimeout(client.nextFrame(), 3000, what);
  assert.equal(frame.opcode, 0x1, `expected text frame, got opcode ${frame.opcode}`);
  return JSON.parse(frame.payload.toString('utf8')) as Record<string, unknown>;
}

async function nextBinary(
  client: BinaryWsClient,
  what: string,
): Promise<{ header: Record<string, unknown>; body: Buffer }> {
  const frame = await withTimeout(client.nextFrame(), 3000, what);
  assert.equal(frame.opcode, 0x2, `expected binary frame, got opcode ${frame.opcode}`);
  return parseBinaryEnvelope(frame.payload);
}

test('binary publish round-trips exact bytes to a binary subscriber', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connect(port);
    try {
      client.sendJson({ type: 'subscribe', id: 's1', pattern: 'bin.**', opts: { binary: true } });
      const subscribed = await nextJson(client, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');

      const bytes = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0x41, 0x42]);
      client.sendBinaryFrame(encodeBinaryPublish('bin.t', bytes));
      const published = await nextJson(client, 'published');
      assert.equal(published.type, 'published');
      assert.equal(published.delivered, 1);

      const { header, body } = await nextBinary(client, 'message');
      assert.equal(header.type, 'message');
      assert.equal(header.topic, 'bin.t');
      assert.equal(header.encoding, 'binary');
      assert.deepEqual(body, bytes);
      // The connection must NOT be closed: binary frames are supported now.
      assert.equal(client.isClosed(), false);
      const stats = gw.getStats();
      assert.ok(stats.framesReceived >= 1);
      assert.equal(stats.protocolErrors, 0);
    } finally {
      client.destroy();
    }
  });
});

test('binary subscriber receives a non-binary publish as JSON bytes', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.sendJson({ type: 'subscribe', id: 's1', pattern: 'mix.**', opts: { binary: true } });
      await nextJson(client, 'subscribed');
      bus.publish('mix.t', { hello: 'world' });
      await flush();
      const { header, body } = await nextBinary(client, 'message');
      assert.equal(header.encoding, 'json');
      assert.deepEqual(JSON.parse(body.toString('utf8')), { hello: 'world' });
    } finally {
      client.destroy();
    }
  });
});

test('binary publish with messageId is idempotent', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.sendJson({ type: 'subscribe', id: 's1', pattern: 'idem.**', opts: { binary: true } });
      await nextJson(client, 'subscribed');
      const bytes = Buffer.from('idempotent-bytes');
      const frame = encodeBinaryPublish('idem.t', bytes, { messageId: 'm-1' });
      client.sendBinaryFrame(frame);
      const first = await nextJson(client, 'published #1');
      assert.equal(first.delivered, 1);
      assert.equal(first.duplicate, false);
      const { body } = await nextBinary(client, 'message');
      assert.equal(body.toString('utf8'), 'idempotent-bytes');
      client.sendBinaryFrame(frame);
      const second = await nextJson(client, 'published #2');
      assert.equal(second.duplicate, true);
      assert.equal(second.delivered, 0);
    } finally {
      client.destroy();
    }
  });
});

test('malformed binary frame gets an error frame and the connection survives', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connect(port);
    try {
      // Header length claims more than the frame carries.
      const bad = Buffer.alloc(8);
      bad.writeUInt32BE(1000, 0);
      client.sendBinaryFrame(bad);
      const err = await nextJson(client, 'error frame');
      assert.equal(err.type, 'error');
      assert.equal(err.code, 'bad_frame');
      assert.equal(gw.getStats().protocolErrors, 1);
      // Connection still alive: a good binary publish works afterwards.
      client.sendJson({ type: 'subscribe', id: 's1', pattern: 'ok.**', opts: { binary: true } });
      await nextJson(client, 'subscribed');
      client.sendBinaryFrame(encodeBinaryPublish('ok.t', Buffer.from('still-alive')));
      const published = await nextJson(client, 'published');
      assert.equal(published.delivered, 1);
      const { body } = await nextBinary(client, 'message');
      assert.equal(body.toString('utf8'), 'still-alive');
      assert.equal(client.isClosed(), false);
    } finally {
      client.destroy();
    }
  });
});

test('binary frame split across TCP chunks is reassembled', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.sendJson({ type: 'subscribe', id: 's1', pattern: 'frag.**', opts: { binary: true } });
      await nextJson(client, 'subscribed');
      const frame = encodeClientFrame(encodeBinaryPublish('frag.t', Buffer.from('fragmented')), 0x2);
      // Split mid-header so the gateway must buffer a partial frame.
      client.writeRaw(frame.subarray(0, 5));
      await sleep(20);
      client.writeRaw(frame.subarray(5));
      const published = await nextJson(client, 'published');
      assert.equal(published.delivered, 1);
      const { body } = await nextBinary(client, 'message');
      assert.equal(body.toString('utf8'), 'fragmented');
    } finally {
      client.destroy();
    }
  });
});

test('binary publish runs admission: byte schema validator rejects', async () => {
  const bus = new EventBus();
  // Byte validators (EB-20) receive the Uint8Array: reject anything not
  // starting with the magic byte 0x42.
  bus.setTopicSchema('raw.**', (payload) => payload instanceof Uint8Array && payload[0] === 0x42);
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.sendBinaryFrame(encodeBinaryPublish('raw.t', Buffer.from([0x00, 0x01])));
      const rejected = await nextJson(client, 'published rejected');
      assert.equal(rejected.delivered, 0);
      client.sendBinaryFrame(encodeBinaryPublish('raw.t', Buffer.from([0x42, 0x01])));
      const accepted = await nextJson(client, 'published accepted');
      assert.equal(accepted.delivered, 0); // no subscribers, but admitted
      const stats = bus.getStats();
      assert.ok(stats.rejectedMessages >= 1, 'schema rejection counted');
    } finally {
      client.destroy();
    }
  });
});

test('binary publish runs admission: publish-side rate limit sheds', async () => {
  const bus = new EventBus();
  bus.setTopicRateLimit('hot.**', 1, { burst: 1 });
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      for (let i = 0; i < 5; i++) {
        client.sendBinaryFrame(encodeBinaryPublish('hot.t', Buffer.from([i])));
      }
      let deliveredSum = 0;
      for (let i = 0; i < 5; i++) {
        const res = await nextJson(client, `published ${i}`);
        deliveredSum += (res.delivered as number) ?? 0;
      }
      // Burst of 1 admits the first publish; the rest shed at the gate.
      assert.ok(deliveredSum <= 1, `expected at most 1 admitted, got ${deliveredSum}`);
      assert.ok(
        bus.getStats().rateLimitedMessages >= 4,
        'rate-limited sheds counted',
      );
    } finally {
      client.destroy();
    }
  });
});

test('reliable binary subscriber carries deliveryId in the binary header', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.sendJson({
        type: 'subscribe',
        id: 's1',
        pattern: 'rel.**',
        opts: { binary: true, reliable: true },
      });
      await nextJson(client, 'subscribed');
      client.sendBinaryFrame(encodeBinaryPublish('rel.t', Buffer.from('at-least-once')));
      const published = await nextJson(client, 'published');
      assert.equal(published.delivered, 1);
      const { header, body } = await nextBinary(client, 'message');
      assert.equal(header.encoding, 'binary');
      assert.equal(body.toString('utf8'), 'at-least-once');
      assert.equal(typeof header.deliveryId, 'number');
      assert.equal(typeof header.redeliveries, 'number');
      // Settle via the text ack frame; no error frame must follow.
      client.sendJson({ type: 'ack', deliveryId: header.deliveryId });
      await sleep(50);
      assert.equal(client.isClosed(), false);
    } finally {
      client.destroy();
    }
  });
});

test('slow client: binary frames honor maxPendingBytes like text frames', async () => {
  const bus = new EventBus();
  await withGateway(
    bus,
    { sendHighWaterMarkBytes: 1, maxPendingBytes: 64 },
    async (gw, port) => {
      const client = await connect(port);
      try {
        client.sendJson({
          type: 'subscribe',
          id: 's1',
          pattern: 'flood.**',
          opts: { binary: true, queueSize: 5000 },
        });
        await nextJson(client, 'subscribed');
        // Stop reading: the client kernel buffer fills, backpressure
        // propagates to the gateway's send path.
        client.pause();
        const big = Buffer.alloc(8 * 1024, 0xab);
        for (let i = 0; i < 2000; i++) bus.publish('flood.t', big, { binary: true });
        await sleep(500);
        const stats = gw.getStats();
        assert.ok(stats.framesSent >= 1, 'some frames were sent');
        assert.ok(stats.droppedFrames > 0, `expected drops, got ${stats.droppedFrames}`);
      } finally {
        client.destroy();
      }
    },
  );
});
