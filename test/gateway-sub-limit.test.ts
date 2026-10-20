import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';

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
  private readonly socket: Socket;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.parser.push(chunk)) {
        if (frame.opcode === 0x8) {
          this.closed = true;
          for (const w of this.waiters) w({ type: '__closed__' });
          this.waiters = [];
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
      for (const w of this.waiters) w({ type: '__closed__' });
      this.waiters = [];
    });
    socket.on('error', () => {});
  }

  send(obj: unknown): void {
    this.socket.write(encodeClientFrame(Buffer.from(JSON.stringify(obj), 'utf8')));
  }

  next(): Promise<Record<string, unknown>> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.closed) return Promise.reject(new Error('connection closed'));
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  destroy(): void {
    this.socket.destroy();
  }
}

/** Raw TCP connect + WS handshake; returns a ready client. */
async function connect(port: number): Promise<WsClient> {
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
  socket.write(
    [
      'GET /ws HTTP/1.1',
      'Host: 127.0.0.1',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Key: ${key}`,
      'Sec-WebSocket-Version: 13',
      '',
      '',
    ].join('\r\n'),
  );
  const head = await withTimeout(
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
  return new WsClient(socket);
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

test('invalid maxSubscriptionsPerConnection fails fast with RangeError', () => {
  const bus = new EventBus();
  for (const value of [0, -1, 1.5, NaN, Infinity, 'many']) {
    assert.throws(
      () => new GatewayServer(bus, { maxSubscriptionsPerConnection: value as never }),
      RangeError,
      `value ${String(value)}`,
    );
  }
  // Positive integers are accepted (and 1 is usable, not "disabled").
  new GatewayServer(bus, { maxSubscriptionsPerConnection: 1 });
});

test('over-cap subscribe gets subscribe_denied; the connection stays open', async () => {
  const bus = new EventBus();
  await withGateway(bus, { maxSubscriptionsPerConnection: 2 }, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'a.**' });
      assert.equal((await withTimeout(client.next(), 3000, 's1')).type, 'subscribed');
      client.send({ type: 'subscribe', id: 's2', pattern: 'b.**' });
      assert.equal((await withTimeout(client.next(), 3000, 's2')).type, 'subscribed');

      client.send({ type: 'subscribe', id: 's3', pattern: 'c.**' });
      const denied = await withTimeout(client.next(), 3000, 's3');
      assert.equal(denied.type, 'error');
      assert.equal(denied.code, 'subscribe_denied');
      assert.equal(denied.id, 's3');
      assert.ok(typeof denied.message === 'string' && denied.message.length > 0);

      // The connection is still usable: ping round-trips.
      client.send({ type: 'ping', id: 7 });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.deepEqual(pong, { type: 'pong', id: 7 });
    } finally {
      client.destroy();
    }
  });
});

test('unsubscribe frees a slot; the over-cap retry then succeeds', async () => {
  const bus = new EventBus();
  await withGateway(bus, { maxSubscriptionsPerConnection: 2 }, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'a.**' });
      const sub1 = await withTimeout(client.next(), 3000, 's1');
      client.send({ type: 'subscribe', id: 's2', pattern: 'b.**' });
      await withTimeout(client.next(), 3000, 's2');

      client.send({ type: 'unsubscribe', id: 'u1', subId: sub1.subId });
      const unsub = await withTimeout(client.next(), 3000, 'u1');
      assert.equal(unsub.type, 'unsubscribed');

      client.send({ type: 'subscribe', id: 's3', pattern: 'c.**' });
      const sub3 = await withTimeout(client.next(), 3000, 's3');
      assert.equal(sub3.type, 'subscribed');
      assert.equal(sub3.id, 's3');
      assert.equal(bus.getStats().totalSubscribers, 2);
    } finally {
      client.destroy();
    }
  });
});

test('getStats exposes the per-connection subscription count', async () => {
  const bus = new EventBus();
  await withGateway(bus, { maxSubscriptionsPerConnection: 5 }, async (gw, port) => {
    const client = await connect(port);
    try {
      // The connection row exists with zero subscriptions right after connect.
      assert.equal(gw.getStats().connectionQueues.length, 1);
      assert.equal(gw.getStats().connectionQueues[0].subscriptions, 0);
      client.send({ type: 'subscribe', id: 's1', pattern: 'a.**' });
      const sub1 = await withTimeout(client.next(), 3000, 's1');
      client.send({ type: 'subscribe', id: 's2', pattern: 'b.**' });
      await withTimeout(client.next(), 3000, 's2');

      let queues = gw.getStats().connectionQueues;
      assert.equal(queues.length, 1);
      assert.equal(queues[0].subscriptions, 2);

      client.send({ type: 'unsubscribe', id: 'u1', subId: sub1.subId });
      await withTimeout(client.next(), 3000, 'u1');
      queues = gw.getStats().connectionQueues;
      assert.equal(queues[0].subscriptions, 1);
    } finally {
      client.destroy();
    }
  });
});

test('unset cap keeps the historical unlimited behavior', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (_gw, port) => {
    const client = await connect(port);
    try {
      for (let i = 0; i < 5; i++) {
        client.send({ type: 'subscribe', id: `s${i}`, pattern: `t${i}.**` });
        const reply = await withTimeout(client.next(), 3000, `s${i}`);
        assert.equal(reply.type, 'subscribed');
      }
      assert.equal(bus.getStats().totalSubscribers, 5);
    } finally {
      client.destroy();
    }
  });
});
