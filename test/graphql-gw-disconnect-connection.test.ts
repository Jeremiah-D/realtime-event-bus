import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GraphqlGateway } from '../src/graphql-gw.ts';

/**
 * Single-connection live disconnect (EB-515):
 * `GraphqlGateway.disconnectGraphqlConnection(id)` drops one
 * connection by its gateway-assigned id — the id
 * `getStats().connectionsDetail` exposes — without touching the
 * others. The teardown is the shared one: a clean close frame (the
 * normal WebSocket close code 1000, `'operator disconnect'`, not one
 * of the 44xx graphql-ws protocol codes) then socket destroy, so
 * subscriptions unsubscribe and the client sees a normal close
 * rather than the connection vanishing. Unknown ids (including stale
 * ones) throw `RangeError`.
 */

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

class GqlClient {
  private readonly parser = new ClientParser();
  private readonly messages: Array<Record<string, unknown>> = [];
  private messageWaiters: Array<(v: Record<string, unknown>) => void> = [];
  private closed = false;
  private closeWaiters: Array<() => void> = [];
  closeCode: number | undefined;
  private readonly socket: Socket;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.parser.push(chunk)) {
        if (frame.opcode === 0x8) {
          this.closed = true;
          this.closeCode =
            frame.payload.length >= 2 ? frame.payload.readUInt16BE(0) : undefined;
          this.settleClosed();
          continue;
        }
        if (frame.opcode === 0x9) {
          socket.write(encodeClientFrame(frame.payload, 0xa));
          continue;
        }
        if (frame.opcode !== 0x1) continue;
        const obj = JSON.parse(frame.payload.toString('utf8')) as Record<string, unknown>;
        const waiter = this.messageWaiters.shift();
        if (waiter !== undefined) waiter(obj);
        else this.messages.push(obj);
      }
    });
    socket.on('close', () => {
      this.closed = true;
      this.settleClosed();
    });
    socket.on('error', () => {});
  }

  private settleClosed(): void {
    for (const w of this.closeWaiters) w();
    this.closeWaiters = [];
    for (const w of this.messageWaiters) w({ type: '__closed__' });
    this.messageWaiters = [];
  }

  send(obj: unknown): void {
    this.socket.write(encodeClientFrame(Buffer.from(JSON.stringify(obj), 'utf8')));
  }

  next(): Promise<Record<string, unknown>> {
    const queued = this.messages.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.closed) return Promise.reject(new Error('connection closed'));
    return new Promise((resolve) => this.messageWaiters.push(resolve));
  }

  /** Messages that arrived without a waiter — for asserting silence. */
  drain(): Array<Record<string, unknown>> {
    return this.messages.splice(0, this.messages.length);
  }

  waitClosed(): Promise<void> {
    if (this.closed) return Promise.resolve();
    return new Promise((resolve) => this.closeWaiters.push(resolve));
  }

  destroy(): void {
    this.socket.destroy();
  }
}

interface RawConnectOptions {
  protocol?: string;
}

/** Raw TCP connect + WS handshake; returns the client and the status line. */
async function rawGqlConnect(
  port: number,
  opts: RawConnectOptions = {},
): Promise<{ client: GqlClient; statusLine: string }> {
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
    ...(opts.protocol !== undefined ? [`Sec-WebSocket-Protocol: ${opts.protocol}`] : []),
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
  return { client: new GqlClient(socket), statusLine };
}

async function connectGql(port: number): Promise<GqlClient> {
  const { client, statusLine } = await rawGqlConnect(port, { protocol: 'graphql-ws' });
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  return client;
}

async function initAck(client: GqlClient): Promise<void> {
  client.send({ type: 'connection_init' });
  const ack = await withTimeout(client.next(), 3000, 'connection_ack');
  assert.deepEqual(ack, { type: 'connection_ack' });
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

async function withGqlGateway(
  bus: EventBus,
  options: ConstructorParameters<typeof GraphqlGateway>[1],
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

/**
 * Waits until the bus reports at least `n` subscribers. A `subscribe`
 * frame is processed asynchronously on the server (TCP roundtrip), so
 * tests must not `bus.publish` until the subscription is registered —
 * otherwise the publish races ahead of the subscribe and is lost.
 */
async function waitForSubscribers(bus: EventBus, n: number, what: string): Promise<void> {
  const deadline = Date.now() + 5000;
  for (;;) {
    if (bus.getStats().totalSubscribers >= n) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(10);
  }
}

const SUB = (topicArg: string, fields = 'topic payload seq') =>
  `subscription { events(${topicArg}) { ${fields} } }`;

test('EB-515: two clients get incrementing distinct ids, both in connectionsDetail', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const first = await connectGql(port);
    const second = await connectGql(port);
    try {
      await initAck(first);
      const stats = gw.getStats();
      assert.deepEqual(
        stats.connectionsDetail.map((row) => row.id),
        [1, 2],
      );
      assert.deepEqual(stats.connectionsDetail, [
        { id: 1, remoteAddress: '127.0.0.1', subscriptions: 0, initialized: true },
        { id: 2, remoteAddress: '127.0.0.1', subscriptions: 0, initialized: false },
      ]);
      // `connections` stays the cumulative accept count (EB-515 keeps
      // it; the live per-connection view is connectionsDetail).
      assert.equal(stats.connections, 2);
    } finally {
      first.destroy();
      second.destroy();
    }
  });
});

test('EB-515: disconnecting one closes its socket and leaves the other alive', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const first = await connectGql(port);
    const second = await connectGql(port);
    try {
      await initAck(first);
      await initAck(second);
      second.send({ type: 'subscribe', id: 's1', payload: { query: SUB('topic: "b.*"') } });
      await waitForSubscribers(bus, 1, 'subscribe s1');
      gw.disconnectGraphqlConnection(1);
      await withTimeout(first.waitClosed(), 3000, 'first client close');
      assert.equal(first.closeCode, 1000);
      assert.deepEqual(
        gw.getStats().connectionsDetail.map((row) => row.id),
        [2],
      );
      // The surviving connection still receives.
      bus.publish('b.x', 'hello');
      await flush();
      const msg = await withTimeout(second.next(), 3000, 'message on survivor');
      assert.equal(msg.type, 'next');
      assert.equal(msg.id, 's1');
    } finally {
      first.destroy();
      second.destroy();
    }
  });
});

test("EB-515: the disconnected connection's bus subscriptions stop receiving", async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({ type: 'subscribe', id: 's1', payload: { query: SUB('topic: "a"') } });
      await waitForSubscribers(bus, 1, 'subscribe s1');
      assert.equal(bus.publish('a', 'before'), 1);
      gw.disconnectGraphqlConnection(1);
      await withTimeout(client.waitClosed(), 3000, 'client close');
      // The subscription was unsubscribed at teardown: no acceptor left.
      assert.equal(bus.publish('a', 'after'), 0);
      assert.deepEqual(gw.getStats().connectionsDetail, []);
    } finally {
      client.destroy();
    }
  });
});

test('EB-515: an unknown id throws RangeError and touches nothing', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      assert.throws(() => gw.disconnectGraphqlConnection(999), RangeError);
      assert.throws(() => gw.disconnectGraphqlConnection(0), RangeError);
      assert.throws(() => gw.disconnectGraphqlConnection(-1), RangeError);
      assert.deepEqual(
        gw.getStats().connectionsDetail.map((row) => row.id),
        [1],
      );
      // Still alive: ping answers.
      client.send({ type: 'ping' });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.equal(pong.type, 'pong');
    } finally {
      client.destroy();
    }
  });
});

test('EB-515: disconnecting an already-disconnected id throws RangeError', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      gw.disconnectGraphqlConnection(1);
      await withTimeout(client.waitClosed(), 3000, 'client close');
      assert.throws(() => gw.disconnectGraphqlConnection(1), RangeError);
      assert.deepEqual(gw.getStats().connectionsDetail, []);
    } finally {
      client.destroy();
    }
  });
});

test('EB-515: an un-initialized connection disconnects cleanly', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      // No connection_init sent: the id exists from accept, and the
      // armed init timer is cleared by closeWithCode.
      assert.deepEqual(
        gw.getStats().connectionsDetail.map((row) => row.initialized),
        [false],
      );
      gw.disconnectGraphqlConnection(1);
      await withTimeout(client.waitClosed(), 3000, 'client close');
      assert.equal(client.closeCode, 1000);
      assert.deepEqual(gw.getStats().connectionsDetail, []);
      // The cleared init timer never fires a late close on the gateway.
      await sleep(50);
      assert.deepEqual(gw.getStats().connectionsDetail, []);
    } finally {
      client.destroy();
    }
  });
});

test('EB-515: the client receives a code 1000 close frame', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      gw.disconnectGraphqlConnection(1);
      await withTimeout(client.waitClosed(), 3000, 'client close');
      assert.equal(client.closeCode, 1000);
    } finally {
      client.destroy();
    }
  });
});

test('EB-515: a pre-handshake connection disconnects and its socket closes', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const socket = await rawTcp(port);
    try {
      assert.deepEqual(
        gw.getStats().connectionsDetail.map((row) => row.id),
        [1],
      );
      gw.disconnectGraphqlConnection(1);
      await waitSocketClosed(socket);
      assert.deepEqual(gw.getStats().connectionsDetail, []);
      // The armed handshake timer later no-ops behind the closed guard.
      await sleep(50);
      assert.deepEqual(gw.getStats().connectionsDetail, []);
    } finally {
      socket.destroy();
    }
  });
});

test('EB-515: connection ids are never reused', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const first = await connectGql(port);
    try {
      await initAck(first);
      gw.disconnectGraphqlConnection(1);
      await withTimeout(first.waitClosed(), 3000, 'first client close');
    } finally {
      first.destroy();
    }
    const second = await connectGql(port);
    try {
      assert.deepEqual(
        gw.getStats().connectionsDetail.map((row) => row.id),
        [2],
        'the stale id 1 can never address the new connection',
      );
      assert.throws(() => gw.disconnectGraphqlConnection(1), RangeError);
      assert.equal(gw.getStats().connections, 2, 'cumulative accept count keeps counting');
    } finally {
      second.destroy();
    }
  });
});

test('EB-515: connectionsDetail tracks subscriptions and initialized per connection', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const first = await connectGql(port);
    const second = await connectGql(port);
    try {
      await initAck(first);
      first.send({ type: 'subscribe', id: 's1', payload: { query: SUB('topic: "a"') } });
      first.send({ type: 'subscribe', id: 's2', payload: { query: SUB('topic: "b"') } });
      await waitForSubscribers(bus, 2, 'subscribe s1+s2');
      assert.deepEqual(gw.getStats().connectionsDetail, [
        { id: 1, remoteAddress: '127.0.0.1', subscriptions: 2, initialized: true },
        { id: 2, remoteAddress: '127.0.0.1', subscriptions: 0, initialized: false },
      ]);
      assert.equal(gw.getStats().subscriptions, 2);
    } finally {
      first.destroy();
      second.destroy();
    }
  });
});

test('EB-515: gateway stop() still drops every connection', async () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0 });
  await gw.start();
  try {
    const first = await connectGql(gw.getPort());
    const second = await connectGql(gw.getPort());
    await initAck(first);
    await initAck(second);
    assert.equal(gw.getStats().connectionsDetail.length, 2);
    const firstClosed = first.waitClosed();
    const secondClosed = second.waitClosed();
    await gw.stop();
    await withTimeout(firstClosed, 3000, 'first client close');
    await withTimeout(secondClosed, 3000, 'second client close');
    assert.deepEqual(gw.getStats().connectionsDetail, []);
    first.destroy();
    second.destroy();
  } finally {
    await gw.stop();
  }
});
