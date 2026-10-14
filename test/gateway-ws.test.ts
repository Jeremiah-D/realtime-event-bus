import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';

const WS_GUID = '258EAFA5-E91447DA-C5AB0DC85B11';

/** Yields until the bus's scheduled microtask flush has run. */
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

/** Client -> server: masked text frame (RFC 6455 clients must mask). */
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

interface ParsedFrame {
  opcode: number;
  payload: Buffer;
}

/** Minimal client-side frame parser (server frames are never masked). */
class ClientParser {
  private buf = Buffer.alloc(0);

  push(chunk: Buffer): ParsedFrame[] {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    const out: ParsedFrame[] = [];
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

class WsClient {
  private readonly parser = new ClientParser();
  private readonly queue: Array<Record<string, unknown>> = [];
  private waiters: Array<(v: Record<string, unknown>) => void> = [];
  private closed = false;
  private closeWaiters: Array<() => void> = [];
  private readonly socket: Socket;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.parser.push(chunk)) {
        if (frame.opcode === 0x8) {
          this.closed = true;
          for (const w of this.closeWaiters) w();
          this.closeWaiters = [];
          for (const w of this.waiters) w({ type: '__closed__' });
          this.waiters = [];
          continue;
        }
        if (frame.opcode === 0x9) {
          socket.write(encodeClientFrame(frame.payload, 0xa));
          continue;
        }
        if (frame.opcode !== 0x1) continue;
        const obj = JSON.parse(frame.payload.toString('utf8')) as Record<string, unknown>;
        const waiter = this.waiters.shift();
        if (waiter !== undefined) waiter(obj);
        else this.queue.push(obj);
      }
    });
    socket.on('close', () => {
      this.closed = true;
      for (const w of this.closeWaiters) w();
      this.closeWaiters = [];
      for (const w of this.waiters) w({ type: '__closed__' });
      this.waiters = [];
    });
    socket.on('error', () => {});
  }

  send(obj: unknown): void {
    this.socket.write(encodeClientFrame(Buffer.from(JSON.stringify(obj), 'utf8')));
  }

  sendRawText(text: string): void {
    this.socket.write(encodeClientFrame(Buffer.from(text, 'utf8')));
  }

  next(): Promise<Record<string, unknown>> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.closed) return Promise.reject(new Error('connection closed'));
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  waitClosed(): Promise<void> {
    if (this.closed) return Promise.resolve();
    return new Promise((resolve) => this.closeWaiters.push(resolve));
  }

  pause(): void {
    this.socket.pause();
  }

  destroy(): void {
    this.socket.destroy();
  }
}

interface ConnectOptions {
  authToken?: string;
  extraHeaders?: string[];
}

/** Raw TCP connect + WS handshake; returns the client and the status line. */
async function rawConnect(
  port: number,
  opts: ConnectOptions = {},
): Promise<{ client: WsClient; statusLine: string; acceptKey: string | undefined }> {
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
    ...(opts.authToken !== undefined ? [`Authorization: Bearer ${opts.authToken}`] : []),
    ...(opts.extraHeaders ?? []),
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
          // Any bytes after the handshake head belong to frames; re-emit.
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
  let acceptKey: string | undefined;
  for (const line of head.split('\r\n').slice(1)) {
    const idx = line.indexOf(':');
    if (idx !== -1 && line.slice(0, idx).trim().toLowerCase() === 'sec-websocket-accept') {
      acceptKey = line.slice(idx + 1).trim();
    }
  }
  return { client: new WsClient(socket), statusLine, acceptKey };
}

async function connect(port: number, opts: ConnectOptions = {}): Promise<WsClient> {
  const { client, statusLine, acceptKey } = await rawConnect(port, opts);
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  return client;
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

test('handshake negotiates Sec-WebSocket-Accept and ping/pong works', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const key = randomBytes(16).toString('base64');
    const socket = tcpConnect(port, '127.0.0.1');
    await withTimeout(
      new Promise<void>((resolve, reject) => {
        socket.once('connect', resolve);
        socket.once('error', reject);
      }),
      3000,
      'tcp connect',
    );
    socket.write(
      [
        'GET /ws HTTP/1.1',
        'Host: x',
        'Upgrade: websocket',
        'Connection: Upgrade',
        `Sec-WebSocket-Key: ${key}`,
        'Sec-WebSocket-Version: 13',
        '',
        '',
      ].join('\r\n'),
    );
    const head: string = await withTimeout(
      new Promise<string>((resolve) => {
        let buf = Buffer.alloc(0);
        socket.on('data', (chunk: Buffer) => {
          buf = Buffer.concat([buf, chunk]);
          if (buf.indexOf('\r\n\r\n') !== -1) resolve(buf.toString('latin1'));
        });
      }),
      3000,
      'handshake',
    );
    assert.match(head, /^HTTP\/1\.1 101/);
    const expected = createHash('sha1').update(key + WS_GUID).digest('base64');
    assert.ok(head.includes(`Sec-WebSocket-Accept: ${expected}`));
    const client = new WsClient(socket);
    try {
      client.send({ type: 'ping', id: 7 });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.deepEqual(pong, { type: 'pong', id: 7 });
    } finally {
      client.destroy();
    }
  });
});

test('subscribe then bus.publish delivers a message frame', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'news.**' });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      assert.equal(subscribed.id, 's1');
      const subId = subscribed.subId as string;
      assert.ok(typeof subId === 'string');

      bus.publish('news.tech', { headline: 'hello' });
      const msg = await withTimeout(client.next(), 3000, 'message');
      assert.equal(msg.type, 'message');
      assert.equal(msg.subId, subId);
      assert.equal(msg.topic, 'news.tech');
      assert.deepEqual(msg.payload, { headline: 'hello' });
      assert.equal(msg.seq, 1);

      const stats = gw.getStats();
      assert.equal(stats.connections, 1);
      assert.ok(stats.framesReceived >= 1);
      assert.ok(stats.framesSent >= 2);
    } finally {
      client.destroy();
    }
  });
});

test('gateway publish reaches a local subscriber; reply carries delivered count', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'publish', id: 'p1', topic: 'orders.new', payload: { id: 42 } });
      const reply = await withTimeout(client.next(), 3000, 'published');
      assert.deepEqual(reply, { type: 'published', id: 'p1', delivered: 1, duplicate: false });
      await flush();
      assert.deepEqual(received, [{ id: 42 }]);
    } finally {
      client.destroy();
    }
  });
});

test('gateway publish with messageId goes through publishIdempotent', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('pay.**', (msg) => received.push(msg.payload));
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({
        type: 'publish',
        id: 'p1',
        topic: 'pay.done',
        payload: { amt: 5 },
        opts: { messageId: 'm-1' },
      });
      const first = await withTimeout(client.next(), 3000, 'published 1');
      assert.deepEqual(first, { type: 'published', id: 'p1', delivered: 1, duplicate: false });
      client.send({
        type: 'publish',
        id: 'p2',
        topic: 'pay.done',
        payload: { amt: 5 },
        opts: { messageId: 'm-1' },
      });
      const second = await withTimeout(client.next(), 3000, 'published 2');
      assert.deepEqual(second, { type: 'published', id: 'p2', delivered: 0, duplicate: true });
      await flush();
      assert.deepEqual(received, [{ amt: 5 }]);
    } finally {
      client.destroy();
    }
  });
});

test('publishBatch fans out every entry', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('batch.**', (msg) => received.push(msg.topic));
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({
        type: 'publishBatch',
        id: 'b1',
        entries: [
          { topic: 'batch.a', payload: 1 },
          { topic: 'batch.b', payload: 2 },
        ],
      });
      const reply = await withTimeout(client.next(), 3000, 'published batch');
      assert.equal(reply.type, 'published');
      assert.equal(reply.delivered, 2);
      await flush();
      assert.deepEqual(received, ['batch.a', 'batch.b']);
    } finally {
      client.destroy();
    }
  });
});

test('ACL-denied subscribe returns a subscribe_denied error frame', async () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'secret.**', subscribe: 'deny' }] },
  });
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'secret.**' });
      const err = await withTimeout(client.next(), 3000, 'error frame');
      assert.equal(err.type, 'error');
      assert.equal(err.id, 's1');
      assert.equal(err.code, 'subscribe_denied');
      assert.equal(bus.getStats().totalSubscribers, 0);
      // The connection stays usable after a denied subscribe.
      client.send({ type: 'subscribe', id: 's2', pattern: 'public.**' });
      const ok = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(ok.type, 'subscribed');
    } finally {
      client.destroy();
    }
  });
});

test('auth token: handshake without it is rejected with 401', async () => {
  const bus = new EventBus();
  await withGateway(bus, { authToken: 's3cr3t' }, async (gw, port) => {
    const bad = await rawConnect(port);
    assert.match(bad.statusLine, /^HTTP\/1\.1 401/);
    bad.client.destroy();
    await sleep(50);
    assert.equal(gw.getStats().authFailures, 1);

    const good = await connect(port, { authToken: 's3cr3t' });
    try {
      good.send({ type: 'ping' });
      const pong = await withTimeout(good.next(), 3000, 'pong');
      assert.equal(pong.type, 'pong');
    } finally {
      good.destroy();
    }
  });
});

test('reliable subscribe: ack settles, nack redelivers', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({
        type: 'subscribe',
        id: 's1',
        pattern: 'jobs.**',
        opts: { reliable: { ackTimeoutMs: 60_000 } },
      });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');

      bus.publish('jobs.a', { n: 1 });
      const first = await withTimeout(client.next(), 3000, 'first delivery');
      assert.equal(first.type, 'message');
      const deliveryId = first.deliveryId as number;
      assert.ok(typeof deliveryId === 'number');
      assert.equal(first.redeliveries, 0);

      // nack -> immediate redelivery with a fresh delivery id
      client.send({ type: 'nack', deliveryId });
      const redelivered = await withTimeout(client.next(), 3000, 'redelivery');
      assert.equal(redelivered.type, 'message');
      assert.equal(redelivered.redeliveries, 1);
      assert.notEqual(redelivered.deliveryId, deliveryId);

      // ack -> no further delivery within a comfortable window
      client.send({ type: 'ack', deliveryId: redelivered.deliveryId });
      await sleep(150);
      // Stale ack of the retired handle is a silent no-op.
      client.send({ type: 'ack', deliveryId });
      client.send({ type: 'ping', id: 'after' });
      const pong = await withTimeout(client.next(), 3000, 'pong after settle');
      assert.equal(pong.type, 'pong');
    } finally {
      client.destroy();
    }
  });
});

test('unsubscribe stops delivery and unknown subId errors', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 't.**' });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      const subId = subscribed.subId as string;

      client.send({ type: 'unsubscribe', id: 'u1', subId: 'nope' });
      const notFound = await withTimeout(client.next(), 3000, 'not_found');
      assert.equal(notFound.code, 'not_found');

      client.send({ type: 'unsubscribe', id: 'u2', subId });
      const unsub = await withTimeout(client.next(), 3000, 'unsubscribed');
      assert.deepEqual(unsub, { type: 'unsubscribed', id: 'u2', subId });
      assert.equal(bus.getStats().totalSubscribers, 0);

      bus.publish('t.x', 1);
      await flush();
      client.send({ type: 'ping', id: 'p' });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.equal(pong.type, 'pong');
    } finally {
      client.destroy();
    }
  });
});

test('malformed JSON and unknown frame types get error frames, connection survives', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connect(port);
    try {
      client.sendRawText('{not json');
      const bad = await withTimeout(client.next(), 3000, 'bad_frame error');
      assert.equal(bad.type, 'error');
      assert.equal(bad.code, 'bad_frame');

      client.send({ type: 'frobnicate', id: 'x' });
      const unknown = await withTimeout(client.next(), 3000, 'unknown_frame error');
      assert.equal(unknown.type, 'error');
      assert.equal(unknown.code, 'unknown_frame');
      assert.equal(unknown.id, 'x');

      assert.ok(gw.getStats().protocolErrors >= 1);

      client.send({ type: 'ping', id: 'alive' });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.equal(pong.type, 'pong');
    } finally {
      client.destroy();
    }
  });
});

test('resumeFromSeq replays durable-log history to a reconnecting client', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-gw-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('hist.a', { n: 1 });
  bus.publish('hist.a', { n: 2 });
  bus.publish('hist.a', { n: 3 });
  await flush();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'hist.**', opts: { resumeFromSeq: 0 } });
      // The durable-log replay pre-fills the queue asynchronously, so the
      // subscribed ack lands first, then the replayed history in order.
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      const seen: number[] = [];
      for (let i = 0; i < 3; i++) {
        const msg = await withTimeout(client.next(), 3000, `replay ${i}`);
        assert.equal(msg.type, 'message');
        seen.push((msg.payload as { n: number }).n);
      }
      assert.deepEqual(seen, [1, 2, 3]);
    } finally {
      client.destroy();
    }
  });
});

test('slow client: gateway pauses and drops, counting droppedFrames', async () => {
  const bus = new EventBus();
  await withGateway(
    bus,
    { sendHighWaterMarkBytes: 1, maxPendingBytes: 64 },
    async (gw, port) => {
      const client = await connect(port);
      try {
        client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
        const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
        assert.equal(subscribed.type, 'subscribed');
        // Stop reading: the client kernel buffer fills, backpressure
        // propagates to the gateway's send path.
        client.pause();
        const big = 'x'.repeat(8 * 1024);
        for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big });
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

test('closing the connection unsubscribes its bus subscriptions', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    client.send({ type: 'subscribe', id: 's1', pattern: 'z.**' });
    const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
    assert.equal(subscribed.type, 'subscribed');
    assert.equal(bus.getStats().totalSubscribers, 1);
    client.destroy();
    await sleep(100);
    assert.equal(bus.getStats().totalSubscribers, 0);
  });
});

test('invalid gateway options fail fast with RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => new GatewayServer(bus, { authToken: '' }), RangeError);
  assert.throws(() => new GatewayServer(bus, { sendHighWaterMarkBytes: 0 }), RangeError);
  assert.throws(() => new GatewayServer(bus, { maxPendingBytes: -1 }), RangeError);
  assert.throws(() => new GatewayServer(bus, { maxFrameBytes: NaN }), RangeError);
});

test('invalid subscribe/publish requests get invalid_request errors', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: '' });
      const e1 = await withTimeout(client.next(), 3000, 'empty pattern error');
      assert.equal(e1.code, 'invalid_request');

      client.send({ type: 'publish', id: 'p1', topic: '', payload: 1 });
      const e2 = await withTimeout(client.next(), 3000, 'empty topic error');
      assert.equal(e2.code, 'invalid_request');

      client.send({ type: 'publishBatch', id: 'b1', entries: [] });
      const e3 = await withTimeout(client.next(), 3000, 'empty batch error');
      assert.equal(e3.code, 'invalid_request');
    } finally {
      client.destroy();
    }
  });
});
