import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GraphqlGateway } from '../src/graphql-gw.ts';

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
  private readonly rawFrames: RawFrame[] = [];
  private rawWaiters: Array<(f: RawFrame) => void> = [];
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
        if (frame.opcode === 0xa) {
          const waiter = this.rawWaiters.shift();
          if (waiter !== undefined) waiter(frame);
          else this.rawFrames.push(frame);
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
    for (const w of this.rawWaiters) w({ opcode: 0x8, payload: Buffer.alloc(0) });
    this.rawWaiters = [];
  }

  send(obj: unknown): void {
    this.socket.write(encodeClientFrame(Buffer.from(JSON.stringify(obj), 'utf8')));
  }

  sendRawText(text: string): void {
    this.socket.write(encodeClientFrame(Buffer.from(text, 'utf8')));
  }

  sendPingFrame(payload: Buffer = Buffer.alloc(0)): void {
    this.socket.write(encodeClientFrame(payload, 0x9));
  }

  next(): Promise<Record<string, unknown>> {
    const queued = this.messages.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.closed) return Promise.reject(new Error('connection closed'));
    return new Promise((resolve) => this.messageWaiters.push(resolve));
  }

  nextRaw(): Promise<RawFrame> {
    const queued = this.rawFrames.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.closed) return Promise.reject(new Error('connection closed'));
    return new Promise((resolve) => this.rawWaiters.push(resolve));
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

async function initAck(client: GqlClient, payload?: unknown): Promise<void> {
  client.send(
    payload === undefined ? { type: 'connection_init' } : { type: 'connection_init', payload },
  );
  const ack = await withTimeout(client.next(), 3000, 'connection_ack');
  assert.deepEqual(ack, { type: 'connection_ack' });
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

// --- handshake -----------------------------------------------------------------

test('handshake without the graphql-ws subprotocol is rejected with 400', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const missing = await rawGqlConnect(port);
    assert.match(missing.statusLine, /^HTTP\/1\.1 400/);
    missing.client.destroy();

    const wrong = await rawGqlConnect(port, { protocol: 'graphql-transport-ws' });
    assert.match(wrong.statusLine, /^HTTP\/1\.1 400/);
    wrong.client.destroy();

    const ok = await rawGqlConnect(port, { protocol: 'graphql-ws' });
    assert.match(ok.statusLine, /^HTTP\/1\.1 101/);
    ok.client.destroy();
  });
});

test('connection_init is answered with connection_ack', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
    } finally {
      client.destroy();
    }
  });
});

test('missing connection_init closes the connection with 4429', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, connectionInitTimeoutMs: 150 }, async (_gw, port) => {
    const client = await connectGql(port);
    await withTimeout(client.waitClosed(), 3000, 'init timeout close');
    assert.equal(client.closeCode, 4429);
  });
});

test('a second connection_init closes the connection with 4400', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    await initAck(client);
    client.send({ type: 'connection_init' });
    await withTimeout(client.waitClosed(), 3000, 'duplicate init close');
    assert.equal(client.closeCode, 4400);
  });
});

test('subscribe before connection_init closes the connection with 4401', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    client.send({ type: 'subscribe', id: 's1', payload: { query: SUB('topic: "x"') } });
    await withTimeout(client.waitClosed(), 3000, 'pre-init subscribe close');
    assert.equal(client.closeCode, 4401);
  });
});

test('maxConnections refuses new sockets with 503', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxConnections: 1 }, async (_gw, port) => {
    const first = await connectGql(port);
    try {
      const second = await rawGqlConnect(port, { protocol: 'graphql-ws' });
      assert.match(second.statusLine, /^HTTP\/1\.1 503/);
      second.client.destroy();
      // The first connection is unaffected.
      await initAck(first);
    } finally {
      first.destroy();
    }
  });
});

// --- subscribe / next / complete -------------------------------------------------

test('subscribe -> publish -> next, then complete unsubscribes', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({ type: 'subscribe', id: 's1', payload: { query: SUB('topic: "news.*"') } });
      await waitForSubscribers(bus, 1, 'subscribe s1');
      bus.publish('news.tech', { headline: 'hello' });
      const next = await withTimeout(client.next(), 3000, 'next');
      assert.equal(next.type, 'next');
      assert.equal(next.id, 's1');
      assert.deepEqual(next.payload, {
        topic: 'news.tech',
        payload: { headline: 'hello' },
        seq: 1,
      });
      assert.equal(gw.getStats().subscriptions, 1);
      assert.equal(gw.getStats().messagesSent, 1);
      assert.equal(bus.getStats().totalSubscribers, 1);

      client.send({ type: 'complete', id: 's1' });
      const done = await withTimeout(client.next(), 3000, 'complete');
      assert.deepEqual(done, { type: 'complete', id: 's1' });
      assert.equal(gw.getStats().subscriptions, 0);
      assert.equal(bus.getStats().totalSubscribers, 0);

      // No more deliveries after complete.
      bus.publish('news.tech', { headline: 'late' });
      client.send({ type: 'ping' });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.equal(pong.type, 'pong');
      assert.deepEqual(client.drain(), []);
    } finally {
      client.destroy();
    }
  });
});

test('complete for an unknown id is ignored, connection stays alive', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({ type: 'complete', id: 'nope' });
      client.send({ type: 'ping' });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.equal(pong.type, 'pong');
    } finally {
      client.destroy();
    }
  });
});

test('duplicate subscribe id closes the connection with 4409', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({ type: 'subscribe', id: 'dup', payload: { query: SUB('topic: "a"') } });
      await waitForSubscribers(bus, 1, 'subscribe dup');
      client.send({ type: 'subscribe', id: 'dup', payload: { query: SUB('topic: "b"') } });
      await withTimeout(client.waitClosed(), 3000, 'duplicate id close');
      assert.equal(client.closeCode, 4409);
      await sleep(100);
      assert.equal(bus.getStats().totalSubscribers, 0);
    } finally {
      client.destroy();
    }
  });
});

// --- query mapping ----------------------------------------------------------------

test('filter argument narrows deliveries via the bus filter path', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({
        type: 'subscribe',
        id: 'f1',
        payload: { query: SUB('topic: "mkt.*", filter: "price > 100"', 'payload') },
      });
      await waitForSubscribers(bus, 1, 'subscribe f1');
      bus.publish('mkt.a', { price: 50 });
      bus.publish('mkt.b', { price: 150 });
      const next = await withTimeout(client.next(), 3000, 'next');
      assert.equal(next.type, 'next');
      assert.deepEqual(next.payload, { payload: { price: 150 } });
      // The rejected message never reaches the client.
      client.send({ type: 'ping' });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.equal(pong.type, 'pong');
      assert.deepEqual(client.drain(), []);
    } finally {
      client.destroy();
    }
  });
});

test('variables substitute into arguments', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({
        type: 'subscribe',
        id: 'v1',
        payload: {
          query: 'subscription ($t: String!, $f: String) { feed(topic: $t, filter: $f) { topic } }',
          variables: { t: 'var.*', f: 'n > 1' },
        },
      });
      await waitForSubscribers(bus, 1, 'subscribe v1');
      bus.publish('var.a', { n: 0 });
      bus.publish('var.b', { n: 2 });
      const next = await withTimeout(client.next(), 3000, 'next');
      assert.equal(next.type, 'next');
      assert.deepEqual(next.payload, { topic: 'var.b' });
    } finally {
      client.destroy();
    }
  });
});

test('operationName selects one operation out of several', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({
        type: 'subscribe',
        id: 'o1',
        payload: {
          query: [
            'subscription Alpha { a(topic: "op.a") { seq } }',
            'subscription Beta { b(topic: "op.b") { seq } }',
          ].join('\n'),
          operationName: 'Beta',
        },
      });
      await waitForSubscribers(bus, 1, 'subscribe o1');
      bus.publish('op.a', 1);
      bus.publish('op.b', 2);
      const next = await withTimeout(client.next(), 3000, 'next');
      assert.equal(next.type, 'next');
      assert.deepEqual(next.payload, { seq: 1 });
      // Nothing from op.a arrives.
      client.send({ type: 'ping' });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.equal(pong.type, 'pong');
      assert.deepEqual(client.drain(), []);
    } finally {
      client.destroy();
    }
  });
});

test('selection set prunes the next payload shape', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({
        type: 'subscribe',
        id: 'p1',
        payload: { query: SUB('topic: "prune.t"', 'topic seq') },
      });
      await waitForSubscribers(bus, 1, 'subscribe p1');
      bus.publish('prune.t', { big: 'dropped' });
      const next = await withTimeout(client.next(), 3000, 'next');
      assert.deepEqual(next.payload, { topic: 'prune.t', seq: 1 });
      assert.ok(!('payload' in (next.payload as Record<string, unknown>)));
    } finally {
      client.destroy();
    }
  });
});

test('invalid queries get error frames and the connection stays usable', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      const cases: Array<[string, string]> = [
        ['missing topic', SUB('')],
        ['missing topic (no args)', 'subscription { events { topic } }'],
        ['non-subscription', 'query { events(topic: "x") { topic } }'],
        ['anonymous shorthand', '{ events(topic: "x") { topic } }'],
        ['bad filter', SUB('topic: "x", filter: "price >>"')],
        ['unknown field', SUB('topic: "x"', 'topic bogus')],
        ['unknown argument', SUB('topic: "x", limit: 5')],
        ['two root fields', 'subscription { a(topic: "x") { topic } b(topic: "y") { topic } }'],
        ['multiple ops, no operationName', 'subscription A { a(topic: "x") { topic } }\nsubscription B { b(topic: "y") { topic } }'],
        ['unknown operationName', 'subscription A { a(topic: "x") { topic } }'],
        ['missing variable', 'subscription ($t: String!) { e(topic: $t) { topic } }'],
        ['garbage', 'subscription { events(topic: "x"'],
      ];
      for (const [label, query] of cases) {
        const payload: Record<string, unknown> = { query };
        if (label === 'unknown operationName') payload.operationName = 'Nope';
        client.send({ type: 'subscribe', id: `bad-${label}`, payload });
        const err = await withTimeout(client.next(), 3000, `error for ${label}`);
        assert.equal(err.type, 'error', label);
        assert.equal(err.id, `bad-${label}`, label);
        assert.ok(Array.isArray(err.payload), label);
        assert.ok(typeof (err.payload as Array<{ message: unknown }>)[0].message === 'string', label);
      }
      assert.equal(bus.getStats().totalSubscribers, 0);
      // The connection is still usable after all those errors.
      client.send({ type: 'subscribe', id: 'good', payload: { query: SUB('topic: "ok.*"') } });
      await waitForSubscribers(bus, 1, 'subscribe good');
      bus.publish('ok.1', { v: 1 });
      const next = await withTimeout(client.next(), 3000, 'next after errors');
      assert.equal(next.type, 'next');
      assert.equal(next.id, 'good');
    } finally {
      client.destroy();
    }
  });
});

// --- auth ---------------------------------------------------------------------------

test('authToken: init without a valid bearer closes with 4401', async () => {
  const bus = new EventBus();
  const token = 'test-token-9';
  await withGqlGateway(bus, { port: 0, authToken: token }, async (gw, port) => {
    const missing = await connectGql(port);
    missing.send({ type: 'connection_init' });
    await withTimeout(missing.waitClosed(), 3000, 'missing auth close');
    assert.equal(missing.closeCode, 4401);

    const wrong = await connectGql(port);
    wrong.send({ type: 'connection_init', payload: { authorization: 'Bearer wrong' } });
    await withTimeout(wrong.waitClosed(), 3000, 'wrong auth close');
    assert.equal(wrong.closeCode, 4401);
    assert.equal(gw.getStats().authFailures, 2);

    const good = await connectGql(port);
    try {
      await initAck(good, { authorization: `Bearer ${token}` });
      good.send({ type: 'subscribe', id: 's', payload: { query: SUB('topic: "auth.*"') } });
      await waitForSubscribers(bus, 1, 'subscribe s');
      bus.publish('auth.x', 1);
      const next = await withTimeout(good.next(), 3000, 'next');
      assert.equal(next.type, 'next');
      assert.equal(gw.getStats().authFailures, 2);
    } finally {
      good.destroy();
    }
  });
});

// --- ping/pong ------------------------------------------------------------------------

test('protocol ping is answered with pong', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({ type: 'ping', payload: { n: 3 } });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.deepEqual(pong, { type: 'pong', payload: { n: 3 } });
      // Client pong needs no answer; the connection stays alive.
      client.send({ type: 'pong' });
      client.send({ type: 'ping' });
      const pong2 = await withTimeout(client.next(), 3000, 'pong2');
      assert.equal(pong2.type, 'pong');
    } finally {
      client.destroy();
    }
  });
});

test('websocket ping frame is answered with a pong frame', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.sendPingFrame(Buffer.from('hb'));
      const pong = await withTimeout(client.nextRaw(), 3000, 'pong frame');
      assert.equal(pong.opcode, 0xa);
      assert.deepEqual(pong.payload, Buffer.from('hb'));
    } finally {
      client.destroy();
    }
  });
});

// --- lifecycle / backpressure -----------------------------------------------------------

test('closing the connection unsubscribes its bus subscriptions', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    await initAck(client);
    client.send({ type: 'subscribe', id: 's1', payload: { query: SUB('topic: "z.*"') } });
    client.send({ type: 'subscribe', id: 's2', payload: { query: SUB('topic: "z.*"', 'seq') } });
    await waitForSubscribers(bus, 2, 'two subscribes');
    client.destroy();
    await sleep(100);
    assert.equal(bus.getStats().totalSubscribers, 0);
  });
});

test('rapid publishes arrive in order through the normal subscriber path', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (_gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({
        type: 'subscribe',
        id: 'burst',
        payload: { query: SUB('topic: "burst.t"', 'seq payload') },
      });
      // The gateway rides bus.subscribe with default backpressure
      // options: this must be a real bus subscription.
      await waitForSubscribers(bus, 1, 'subscribe burst');
      const n = 30;
      for (let i = 0; i < n; i++) bus.publish('burst.t', { n: i });
      const seen: number[] = [];
      for (let i = 0; i < n; i++) {
        const next = await withTimeout(client.next(), 3000, `next ${i}`);
        assert.equal(next.type, 'next');
        const payload = next.payload as { seq: number; payload: { n: number } };
        assert.equal(payload.seq, i + 1);
        seen.push(payload.payload.n);
      }
      assert.deepEqual(seen, Array.from({ length: n }, (_, i) => i));
    } finally {
      client.destroy();
    }
  });
});

test('malformed frames close with 4400 and count protocolErrors', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const badJson = await connectGql(port);
    await initAck(badJson);
    badJson.sendRawText('{not json');
    await withTimeout(badJson.waitClosed(), 3000, 'bad json close');
    assert.equal(badJson.closeCode, 4400);

    const unknownType = await connectGql(port);
    await initAck(unknownType);
    unknownType.send({ type: 'frobnicate' });
    await withTimeout(unknownType.waitClosed(), 3000, 'unknown type close');
    assert.equal(unknownType.closeCode, 4400);

    assert.ok(gw.getStats().protocolErrors >= 2);
  });
});

test('stats report connections, subscriptions, messagesSent and authFailures', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, authToken: 'stats-token' }, async (gw, port) => {
    const before = gw.getStats();
    const client = await connectGql(port);
    try {
      await initAck(client, { authorization: 'Bearer stats-token' });
      client.send({ type: 'subscribe', id: 'st', payload: { query: SUB('topic: "stats.*"') } });
      await waitForSubscribers(bus, 1, 'subscribe st');
      bus.publish('stats.a', 1);
      bus.publish('stats.b', 2);
      await withTimeout(client.next(), 3000, 'next 1');
      await withTimeout(client.next(), 3000, 'next 2');
      const stats = gw.getStats();
      assert.equal(stats.connections, before.connections + 1);
      assert.equal(stats.subscriptions, 1);
      assert.equal(stats.messagesSent, before.messagesSent + 2);
      assert.equal(stats.protocolErrors, before.protocolErrors);
      assert.equal(stats.authFailures, before.authFailures);
    } finally {
      client.destroy();
    }
  });
});

test('invalid gateway options fail fast with RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => new GraphqlGateway(bus, { port: -1 }), RangeError);
  assert.throws(() => new GraphqlGateway(bus, { port: 1.5 }), RangeError);
  assert.throws(() => new GraphqlGateway(bus, { port: 0, authToken: '' }), RangeError);
  assert.throws(() => new GraphqlGateway(bus, { port: 0, maxConnections: 0 }), RangeError);
  assert.throws(() => new GraphqlGateway(bus, { port: 0, connectionInitTimeoutMs: -5 }), RangeError);
});

test('start twice throws; stop is idempotent', async () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0 });
  await gw.start();
  assert.ok(gw.getPort() > 0);
  assert.throws(() => {
    void gw.start();
  }, /already started/);
  await gw.stop();
  await gw.stop();
});
