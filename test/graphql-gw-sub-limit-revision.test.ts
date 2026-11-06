/**
 * GraphQL gateway per-connection subscription cap live revision (EB-402).
 *
 * `GraphqlGateway.setGraphqlGatewayMaxSubscriptionsPerConnection`
 * retunes the opt-in `maxSubscriptionsPerConnection` cap live — without
 * restarting the server. Before EB-402 the GraphQL gateway had no
 * per-connection subscription limit at all: a single client could open
 * unbounded subscriptions, each holding a bus subscription and a queue.
 * The WS gateway has had the same cap since EB-173 (revisable live
 * since EB-327); this closes the asymmetry on the GraphQL gateway.
 *
 * The new cap governs from the next `subscribe` frame on every
 * connection, existing ones included: tightening denies only new
 * subscribes — a connection that already holds more subscriptions
 * than the new cap keeps them — and loosening admits the next
 * subscribe on an over-cap connection at once. A denied subscribe
 * gets an `error` frame (`subscription limit reached
 * (maxSubscriptionsPerConnection=<cap>)`) and the connection stays
 * open, so the client can `complete` a subscription and retry. An
 * explicitly `undefined` revision clears the cap back to unlimited.
 * There is no audit event (the numeric-tuning convention:
 * `setGraphqlGatewayLimits` / `setGraphqlGatewayHandshake`); the live
 * value is observable in `getStats().limits`.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GraphqlGateway } from '../src/graphql-gw.ts';

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

  waitClosed(): Promise<void> {
    if (this.closed) return Promise.resolve();
    return new Promise((resolve) => this.closeWaiters.push(resolve));
  }

  destroy(): void {
    this.socket.destroy();
  }
}

/** Raw TCP connect + WS handshake; returns the client and the status line. */
async function rawGqlConnect(
  port: number,
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
    'Sec-WebSocket-Protocol: graphql-ws',
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
  const { client, statusLine } = await rawGqlConnect(port);
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

function subscribe(client: GqlClient, id: string, topic: string): void {
  client.send({ type: 'subscribe', id, payload: { query: SUB(`topic: "${topic}"`) } });
}

async function expectDenied(client: GqlClient, id: string, cap: number): Promise<void> {
  const err = await withTimeout(client.next(), 3000, 'subscription limit error frame');
  assert.deepEqual(err, {
    type: 'error',
    id,
    payload: [{ message: `subscription limit reached (maxSubscriptionsPerConnection=${cap})` }],
  });
}

async function expectNext(
  client: GqlClient,
  id: string,
  topic: string,
  payload: unknown,
): Promise<void> {
  const next = await withTimeout(client.next(), 3000, 'next');
  assert.equal(next.type, 'next');
  assert.equal(next.id, id);
  const p = next.payload as Record<string, unknown>;
  assert.equal(p.topic, topic);
  assert.deepEqual(p.payload, payload);
}

test('construction-time cap is enforced from the first subscribe', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 1 }, async (gw, port) => {
    assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 1);
    const client = await connectGql(port);
    try {
      await initAck(client);
      subscribe(client, 's1', 'cap.a');
      await waitForSubscribers(bus, 1, 'subscribe s1');
      assert.equal(gw.getStats().subscriptions, 1);
      // The second subscribe is denied with an error frame — and no
      // bus subscription is registered for it.
      subscribe(client, 's2', 'cap.b');
      await expectDenied(client, 's2', 1);
      assert.equal(gw.getStats().subscriptions, 1);
      assert.equal(bus.getStats().totalSubscribers, 1);
      // The connection stays open: ping is answered with pong, and the
      // live subscription still delivers.
      client.send({ type: 'ping' });
      assert.deepEqual(await withTimeout(client.next(), 3000, 'pong'), { type: 'pong' });
      bus.publish('cap.a', 7);
      await expectNext(client, 's1', 'cap.a', 7);
    } finally {
      client.destroy();
    }
  });
});

test('live revision sets the cap: the N+1th subscribe is denied', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, undefined);
    const client = await connectGql(port);
    try {
      await initAck(client);
      subscribe(client, 's1', 'live.a');
      subscribe(client, 's2', 'live.b');
      await waitForSubscribers(bus, 2, 'subscribe s1+s2');
      gw.setGraphqlGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 2 });
      assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 2);
      subscribe(client, 's3', 'live.c');
      await expectDenied(client, 's3', 2);
      assert.equal(gw.getStats().subscriptions, 2);
      assert.equal(bus.getStats().totalSubscribers, 2);
    } finally {
      client.destroy();
    }
  });
});

test('completing a subscription frees its slot for a new subscribe', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 1 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      subscribe(client, 's1', 'free.a');
      await waitForSubscribers(bus, 1, 'subscribe s1');
      subscribe(client, 's2', 'free.b');
      await expectDenied(client, 's2', 1);
      // Complete s1: the slot frees at once (the complete frame is
      // sent after the server drops the subscription).
      client.send({ type: 'complete', id: 's1' });
      assert.deepEqual(await withTimeout(client.next(), 3000, 'complete'), {
        type: 'complete',
        id: 's1',
      });
      assert.equal(gw.getStats().subscriptions, 0);
      subscribe(client, 's2', 'free.b');
      await waitForSubscribers(bus, 1, 'subscribe s2 after complete');
      bus.publish('free.b', 'again');
      await expectNext(client, 's2', 'free.b', 'again');
    } finally {
      client.destroy();
    }
  });
});

test('tightening keeps existing subscriptions and denies only new ones', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      subscribe(client, 's1', 'tight.a');
      subscribe(client, 's2', 'tight.b');
      await waitForSubscribers(bus, 2, 'subscribe s1+s2');
      gw.setGraphqlGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 1 });
      subscribe(client, 's3', 'tight.c');
      await expectDenied(client, 's3', 1);
      // Both pre-existing subscriptions are kept and still deliver.
      assert.equal(gw.getStats().subscriptions, 2);
      bus.publish('tight.a', 1);
      await expectNext(client, 's1', 'tight.a', 1);
      bus.publish('tight.b', 2);
      await expectNext(client, 's2', 'tight.b', 2);
    } finally {
      client.destroy();
    }
  });
});

test('loosening admits the next subscribe on an over-cap connection', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 1 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      subscribe(client, 's1', 'loose.a');
      await waitForSubscribers(bus, 1, 'subscribe s1');
      subscribe(client, 's2', 'loose.b');
      await expectDenied(client, 's2', 1);
      gw.setGraphqlGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 2 });
      subscribe(client, 's2', 'loose.b');
      await waitForSubscribers(bus, 2, 'subscribe s2 after loosening');
      bus.publish('loose.b', 'in');
      await expectNext(client, 's2', 'loose.b', 'in');
    } finally {
      client.destroy();
    }
  });
});

test('explicitly undefined clears the cap back to unlimited', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 1 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      subscribe(client, 's1', 'clear.a');
      await waitForSubscribers(bus, 1, 'subscribe s1');
      subscribe(client, 's2', 'clear.b');
      await expectDenied(client, 's2', 1);
      gw.setGraphqlGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: undefined });
      assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, undefined);
      subscribe(client, 's2', 'clear.b');
      await waitForSubscribers(bus, 2, 'subscribe s2 after clearing');
      subscribe(client, 's3', 'clear.c');
      await waitForSubscribers(bus, 3, 'subscribe s3 after clearing');
      assert.equal(gw.getStats().subscriptions, 3);
    } finally {
      client.destroy();
    }
  });
});

test('illegal values throw RangeError and leave the live cap untouched', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 2 });
  assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 2);
  const bad: Array<unknown> = [
    { maxSubscriptionsPerConnection: 0 },
    { maxSubscriptionsPerConnection: -1 },
    { maxSubscriptionsPerConnection: 1.5 },
    { maxSubscriptionsPerConnection: Number.NaN },
    { maxSubscriptionsPerConnection: Infinity },
    { maxSubscriptionsPerConnection: '2' },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => gw.setGraphqlGatewayMaxSubscriptionsPerConnection(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 2);
  }
  // Construction rejects the same values.
  assert.throws(
    () => new GraphqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 0 }),
    RangeError,
  );
  assert.throws(
    () => new GraphqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 1.5 }),
    RangeError,
  );
});

test('empty, identical and absent revisions are no-ops', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 2 });
  gw.setGraphqlGatewayMaxSubscriptionsPerConnection({});
  assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 2);
  gw.setGraphqlGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 2 });
  assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 2);
  gw.setGraphqlGatewayMaxSubscriptionsPerConnection(null as never);
  gw.setGraphqlGatewayMaxSubscriptionsPerConnection(undefined as never);
  assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 2);
  // From unlimited, an empty revision keeps it unlimited.
  const bare = new GraphqlGateway(bus, { port: 0 });
  bare.setGraphqlGatewayMaxSubscriptionsPerConnection({});
  assert.equal(bare.getStats().limits.maxSubscriptionsPerConnection, undefined);
});

test('getStats().limits follows the live cap', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0 });
  assert.deepEqual(gw.getStats().limits, {
    maxConnections: undefined,
    connectionInitTimeoutMs: 5000,
    maxFrameBytes: 1024 * 1024,
    handshakeTimeoutMs: 10_000,
    maxHandshakeBytes: 8 * 1024,
    maxSubscriptionsPerConnection: undefined,
  });
  gw.setGraphqlGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 3 });
  assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 3);
  gw.setGraphqlGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 1 });
  assert.equal(gw.getStats().limits.maxSubscriptionsPerConnection, 1);
  gw.setGraphqlGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: undefined });
  assert.deepEqual(gw.getStats().limits, {
    maxConnections: undefined,
    connectionInitTimeoutMs: 5000,
    maxFrameBytes: 1024 * 1024,
    handshakeTimeoutMs: 10_000,
    maxHandshakeBytes: 8 * 1024,
    maxSubscriptionsPerConnection: undefined,
  });
});

test('a duplicate subscription id still closes with 4409 before the cap is checked', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 1 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client);
      subscribe(client, 's1', 'dup.a');
      await waitForSubscribers(bus, 1, 'subscribe s1');
      // Same id again: the duplicate-id close (4409) wins over the
      // cap — the connection is already at the cap, but the check
      // order reports the duplicate first.
      subscribe(client, 's1', 'dup.b');
      await withTimeout(client.waitClosed(), 3000, 'duplicate-id close');
      assert.equal(client.closeCode, 4409);
      // The close tears the connection down, so its subscription is
      // unsubscribed with it — the point is the 4409, not the count.
    } finally {
      client.destroy();
    }
  });
});

test('a subscribe before connection_init still closes with 4401 before the cap is checked', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxSubscriptionsPerConnection: 1 }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      // Never sends connection_init: the 4401 close wins over the cap.
      subscribe(client, 's1', 'early.a');
      await withTimeout(client.waitClosed(), 3000, 'uninitialized close');
      assert.equal(client.closeCode, 4401);
      assert.equal(gw.getStats().subscriptions, 0);
      assert.equal(bus.getStats().totalSubscribers, 0);
    } finally {
      client.destroy();
    }
  });
});
