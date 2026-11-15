import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GatewayServer, type GatewayOptions } from '../src/gateway-ws.ts';

/**
 * Single-connection live disconnect (EB-514):
 * `GatewayServer.disconnectGatewayConnection(id)` drops one connection
 * by its gateway-assigned id — the id `getStats().connectionQueues`
 * exposes — without touching the others. The teardown is the shared
 * one: a clean close frame (code 1000, `'operator disconnect'`) then
 * `destroyConnection`, so subscriptions unsubscribe, the socket is
 * destroyed, and an established connection fires `onDisconnect` with
 * the id. Unknown ids (including stale ones) throw `RangeError`.
 */

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
  private closeWaiters: Array<(code: number | undefined) => void> = [];
  private readonly socket: Socket;
  /** Close code from the server's close frame, when one arrived. */
  closeCode: number | undefined = undefined;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.parser.push(chunk)) {
        if (frame.opcode === 0x8) {
          this.closed = true;
          this.closeCode =
            frame.payload.length >= 2 ? frame.payload.readUInt16BE(0) : undefined;
          for (const w of this.closeWaiters) w(this.closeCode);
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
      for (const w of this.closeWaiters) w(this.closeCode);
      this.closeWaiters = [];
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

  waitClosed(): Promise<number | undefined> {
    if (this.closed) return Promise.resolve(this.closeCode);
    return new Promise((resolve) => this.closeWaiters.push(resolve));
  }

  pause(): void {
    this.socket.pause();
  }

  resume(): void {
    this.socket.resume();
  }

  destroy(): void {
    this.socket.destroy();
  }
}

/** Raw TCP connect + WS handshake; returns the client and the status line. */
async function rawConnect(
  port: number,
): Promise<{ client: WsClient; statusLine: string }> {
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
  return { client: new WsClient(socket), statusLine };
}

async function connect(port: number): Promise<WsClient> {
  const { client, statusLine } = await rawConnect(port);
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  return client;
}

/** Raw TCP connect without sending a handshake (pre-handshake connection). */
async function rawTcp(port: number): Promise<Socket> {
  const socket = tcpConnect(port, '127.0.0.1');
  await withTimeout(
    new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve);
      socket.once('error', reject);
    }),
    3000,
    'tcp connect',
  );
  return socket;
}

async function waitSocketClosed(socket: Socket): Promise<void> {
  if (socket.destroyed) return;
  // A socket with no data listener sits paused: the close frame the
  // server wrote stays buffered, so 'end'/'close' never fire until the
  // stream is resumed and the buffered bytes are consumed.
  socket.resume();
  await withTimeout(
    new Promise<void>((resolve) => socket.once('close', () => resolve())),
    3000,
    'raw socket close',
  );
}

async function withGateway(
  bus: EventBus,
  options: GatewayOptions,
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

test('EB-514: two clients get incrementing distinct ids, both in connectionQueues', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const first = await connect(port);
    const second = await connect(port);
    try {
      const stats = gw.getStats();
      assert.equal(stats.connections, 2);
      assert.deepEqual(
        stats.connectionQueues.map((row) => row.id),
        [1, 2],
      );
      assert.ok(stats.connectionQueues.every((row) => row.remoteAddress === '127.0.0.1'));
    } finally {
      first.destroy();
      second.destroy();
    }
  });
});

test('EB-514: disconnecting one closes its socket and leaves the other alive', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const first = await connect(port);
    const second = await connect(port);
    try {
      second.send({ type: 'subscribe', id: 's1', pattern: 'b.**' });
      const subscribed = await withTimeout(second.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      gw.disconnectGatewayConnection(1);
      const code = await withTimeout(first.waitClosed(), 3000, 'first client close');
      assert.equal(code, 1000);
      assert.equal(gw.getStats().connections, 1);
      assert.deepEqual(
        gw.getStats().connectionQueues.map((row) => row.id),
        [2],
      );
      // The surviving connection still receives.
      bus.publish('b.x', 'hello');
      await flush();
      const msg = await withTimeout(second.next(), 3000, 'message on survivor');
      assert.equal(msg.type, 'message');
      assert.equal(msg.topic, 'b.x');
    } finally {
      first.destroy();
      second.destroy();
    }
  });
});

test("EB-514: the disconnected connection's bus subscriptions stop receiving", async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'a.**' });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      assert.equal(bus.publish('a.x', 'before'), 1);
      gw.disconnectGatewayConnection(1);
      await withTimeout(client.waitClosed(), 3000, 'client close');
      // The subscription was unsubscribed at teardown: no acceptor left.
      assert.equal(bus.publish('a.x', 'after'), 0);
      assert.equal(gw.getStats().connections, 0);
    } finally {
      client.destroy();
    }
  });
});

test('EB-514: an unknown id throws RangeError and touches nothing', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connect(port);
    try {
      assert.throws(() => gw.disconnectGatewayConnection(999), RangeError);
      assert.throws(() => gw.disconnectGatewayConnection(0), RangeError);
      assert.throws(() => gw.disconnectGatewayConnection(-1), RangeError);
      assert.equal(gw.getStats().connections, 1);
      assert.deepEqual(
        gw.getStats().connectionQueues.map((row) => row.id),
        [1],
      );
    } finally {
      client.destroy();
    }
  });
});

test('EB-514: disconnecting an already-disconnected id throws RangeError', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connect(port);
    try {
      gw.disconnectGatewayConnection(1);
      await withTimeout(client.waitClosed(), 3000, 'client close');
      assert.throws(() => gw.disconnectGatewayConnection(1), RangeError);
      assert.equal(gw.getStats().connections, 0);
    } finally {
      client.destroy();
    }
  });
});

test('EB-514: a handshook disconnect fires onDisconnect with the id', async () => {
  const bus = new EventBus();
  const events: unknown[] = [];
  await withGateway(
    bus,
    { onDisconnect: (event) => events.push(event) },
    async (gw, port) => {
      const client = await connect(port);
      try {
        gw.disconnectGatewayConnection(1);
        await withTimeout(client.waitClosed(), 3000, 'client close');
        assert.equal(events.length, 1);
        const event = events[0] as { id?: unknown; remoteAddress?: unknown };
        assert.equal(event.id, 1);
        assert.ok(event.remoteAddress === undefined || event.remoteAddress === '127.0.0.1');
      } finally {
        client.destroy();
      }
    },
  );
});

test('EB-514: a pre-handshake disconnect works and fires no onDisconnect', async () => {
  const bus = new EventBus();
  const events: unknown[] = [];
  await withGateway(
    bus,
    { onDisconnect: (event) => events.push(event) },
    async (gw, port) => {
      const socket = await rawTcp(port);
      try {
        assert.deepEqual(
          gw.getStats().connectionQueues.map((row) => row.id),
          [1],
        );
        gw.disconnectGatewayConnection(1);
        await waitSocketClosed(socket);
        assert.equal(gw.getStats().connections, 0);
        assert.deepEqual(gw.getStats().connectionQueues, []);
        assert.equal(events.length, 0, 'a handshake that never completed is not a disconnect');
        // The armed handshake timer later no-ops behind the closed guard.
        await sleep(50);
        assert.equal(gw.getStats().connections, 0);
      } finally {
        socket.destroy();
      }
    },
  );
});

test('EB-514: the client receives a code 1000 close frame', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connect(port);
    try {
      gw.disconnectGatewayConnection(1);
      const code = await withTimeout(client.waitClosed(), 3000, 'client close');
      assert.equal(code, 1000);
    } finally {
      client.destroy();
    }
  });
});

const PRESSURE_OPTS: GatewayOptions = {
  sendHighWaterMarkBytes: 1,
  // Big enough that nothing is dropped: frames queue instead.
  maxPendingBytes: 64 * 1024 * 1024,
  clientPressureWatermarkBytes: 128 * 1024,
};

const BIG = 'x'.repeat(8 * 1024);

async function waitForQueuedBytes(
  gw: GatewayServer,
  minBytes: number,
  what: string,
): Promise<void> {
  const deadline = Date.now() + 8000;
  for (;;) {
    const row = gw.getStats().connectionQueues[0];
    if (row !== undefined && row.queuedBytes >= minBytes) return;
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for queued bytes: ${what}`);
    }
    await sleep(50);
  }
}

test('EB-514: a pressured/backlogged connection disconnects and its pending clears', async () => {
  const bus = new EventBus();
  await withGateway(bus, PRESSURE_OPTS, async (gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      client.pause();
      for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
      await waitForQueuedBytes(gw, 128 * 1024, 'first flood');
      const queued = gw.getStats().connectionQueues[0].queuedBytes;
      assert.ok(queued >= 128 * 1024, `queued ${queued}`);
      gw.disconnectGatewayConnection(1);
      // The client socket is paused with a backlog buffered: resume it
      // so the buffered frames drain and the close/FIN reaches 'close'.
      // The close frame itself may have been shed server-side while the
      // socket buffer was full, so only closure — not the code — is
      // asserted here.
      client.resume();
      await withTimeout(client.waitClosed(), 3000, 'client close');
      assert.equal(gw.getStats().connections, 0);
      assert.deepEqual(gw.getStats().connectionQueues, []);
    } finally {
      client.destroy();
    }
  });
});

test('EB-514: connection ids are never reused', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const first = await connect(port);
    try {
      gw.disconnectGatewayConnection(1);
      await withTimeout(first.waitClosed(), 3000, 'first client close');
    } finally {
      first.destroy();
    }
    const second = await connect(port);
    try {
      assert.deepEqual(
        gw.getStats().connectionQueues.map((row) => row.id),
        [2],
        'the stale id 1 can never address the new connection',
      );
      assert.throws(() => gw.disconnectGatewayConnection(1), RangeError);
      assert.equal(gw.getStats().connections, 1);
    } finally {
      second.destroy();
    }
  });
});

test('EB-514: gateway close() still drops every connection', async () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  const port = await gw.listen();
  try {
    const first = await connect(port);
    const second = await connect(port);
    assert.equal(gw.getStats().connections, 2);
    const firstClosed = first.waitClosed();
    const secondClosed = second.waitClosed();
    await gw.close();
    await withTimeout(firstClosed, 3000, 'first client close');
    await withTimeout(secondClosed, 3000, 'second client close');
    assert.equal(gw.getStats().connections, 0);
    assert.deepEqual(gw.getStats().connectionQueues, []);
    first.destroy();
    second.destroy();
  } finally {
    await gw.close();
  }
});
