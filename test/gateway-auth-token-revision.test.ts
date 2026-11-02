/**
 * Dual-gateway bearer-token live rotation (EB-350).
 *
 * `setAuthToken` rotates (non-empty string) or clears (`null`) the
 * bearer token on `GatewayServer` and `GraphqlGateway` live — without
 * restarting the server. The new value governs only handshakes after
 * the call: the old token is rejected from then on (401 on the WS
 * gateway, 4401 on the GraphQL gateway), a cleared gateway admits
 * unauthenticated handshakes, and a connection that already handshook
 * is never re-authenticated or disconnected. An invalid value fails
 * closed with `RangeError` and the live token untouched; re-affirming
 * the current token is a no-op with no audit. Every real rotation is
 * audited as `auth_token_revised` with `{ gateway, changed }` — the
 * token value itself never enters the audit log.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';
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

/** Minimal WS client shared by both gateways' tests. */
class MiniClient {
  private readonly parser = new ClientParser();
  private readonly queue: Array<Record<string, unknown>> = [];
  private waiters: Array<(v: Record<string, unknown>) => void> = [];
  private closeWaiters: Array<() => void> = [];
  closed = false;
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

  destroy(): void {
    this.socket.destroy();
  }
}

/** Raw TCP connect + WS handshake; returns the client and status line. */
async function rawConnect(
  port: number,
  opts: { path?: string; authToken?: string; protocol?: string } = {},
): Promise<{ client: MiniClient; statusLine: string }> {
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
    `GET ${opts.path ?? '/ws'} HTTP/1.1`,
    'Host: 127.0.0.1',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Key: ${key}`,
    'Sec-WebSocket-Version: 13',
    ...(opts.authToken !== undefined ? [`Authorization: Bearer ${opts.authToken}`] : []),
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
  return { client: new MiniClient(socket), statusLine: head.split('\r\n')[0] ?? '' };
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

async function connectWs(port: number, authToken?: string): Promise<MiniClient> {
  const { client, statusLine } = await rawConnect(port, { authToken });
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  return client;
}

async function connectGql(port: number): Promise<MiniClient> {
  const { client, statusLine } = await rawConnect(port, {
    path: '/graphql',
    protocol: 'graphql-ws',
  });
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  return client;
}

async function initAck(client: MiniClient, payload?: unknown): Promise<void> {
  client.send(
    payload === undefined ? { type: 'connection_init' } : { type: 'connection_init', payload },
  );
  const ack = await withTimeout(client.next(), 3000, 'connection_ack');
  assert.deepEqual(ack, { type: 'connection_ack' });
}

/** Waits until the bus reports at least `n` subscribers. */
async function waitForSubscribers(bus: EventBus, n: number, what: string): Promise<void> {
  const deadline = Date.now() + 5000;
  for (;;) {
    if (bus.getStats().totalSubscribers >= n) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(10);
  }
}

/** The `auth_token_revised` audit events on this bus, oldest-first. */
function tokenRevisions(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'auth_token_revised' });
}

// --- WS gateway ---------------------------------------------------------------

test('ws: rotating the token rejects the old token and admits the new one', async () => {
  const bus = new EventBus();
  await withGateway(bus, { authToken: 'ws-old-token' }, async (gw, port) => {
    assert.equal(gw.getStats().authEnabled, true);

    const stale = await rawConnect(port, { authToken: 'ws-old-token' });
    assert.match(stale.statusLine, /^HTTP\/1\.1 101/);
    stale.client.destroy();

    gw.setAuthToken('ws-new-token');
    assert.equal(gw.getStats().authEnabled, true);

    const old = await rawConnect(port, { authToken: 'ws-old-token' });
    assert.match(old.statusLine, /^HTTP\/1\.1 401/);
    old.client.destroy();
    assert.equal(gw.getStats().authFailures, 1);

    const fresh = await connectWs(port, 'ws-new-token');
    try {
      assert.equal(gw.getStats().authFailures, 1);
    } finally {
      fresh.destroy();
    }

    const events = tokenRevisions(bus);
    assert.equal(events.length, 1);
    assert.equal(events[0]?.action, 'auth_token_revised');
    assert.deepEqual(events[0]?.detail, { gateway: 'ws', changed: 'rotated' });
    // The token value itself never enters the audit log.
    assert.ok(!JSON.stringify(events).includes('ws-old-token'));
    assert.ok(!JSON.stringify(events).includes('ws-new-token'));
  });
});

test('ws: clearing the token admits unauthenticated handshakes and leaves established connections alone', async () => {
  const bus = new EventBus();
  await withGateway(bus, { authToken: 'ws-old-token' }, async (gw, port) => {
    const client = await connectWs(port, 'ws-old-token');
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'news.**' });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');

      gw.setAuthToken(null);
      assert.equal(gw.getStats().authEnabled, false);

      // The established connection is never re-authenticated or
      // disconnected: it keeps receiving after the clear.
      bus.publish('news.tech', { headline: 'still here' });
      const msg = await withTimeout(client.next(), 3000, 'message after clear');
      assert.equal(msg.type, 'message');
      assert.equal(msg.topic, 'news.tech');
      assert.equal(client.closed, false);

      const bare = await connectWs(port);
      bare.destroy();

      const events = tokenRevisions(bus);
      assert.equal(events.length, 1);
      assert.deepEqual(events[0]?.detail, { gateway: 'ws', changed: 'cleared' });
      assert.ok(!JSON.stringify(events).includes('ws-old-token'));
    } finally {
      client.destroy();
    }
  });
});

test('ws: invalid values throw RangeError and leave the live token untouched', async () => {
  const bus = new EventBus();
  await withGateway(bus, { authToken: 'ws-old-token' }, async (gw, port) => {
    assert.throws(() => gw.setAuthToken(''), RangeError);
    assert.throws(() => gw.setAuthToken(42 as unknown as string), RangeError);
    assert.throws(() => gw.setAuthToken(undefined as unknown as string), RangeError);
    assert.throws(() => gw.setAuthToken({} as unknown as string), RangeError);
    assert.equal(tokenRevisions(bus).length, 0);

    // The live token is untouched: the old token still handshakes.
    const good = await connectWs(port, 'ws-old-token');
    try {
      assert.equal(gw.getStats().authEnabled, true);
      assert.equal(gw.getStats().authFailures, 0);
    } finally {
      good.destroy();
    }
    assert.equal(tokenRevisions(bus).length, 0);
  });
});

test('ws: re-affirming the current token is a no-op with no audit', async () => {
  const bus = new EventBus();
  await withGateway(bus, { authToken: 'ws-old-token' }, async (gw, port) => {
    gw.setAuthToken('ws-old-token');
    assert.equal(tokenRevisions(bus).length, 0);
    assert.equal(gw.getStats().authEnabled, true);

    // Clearing a gateway that never armed auth is a no-op too.
    const bare = new GatewayServer(bus, {});
    const barePort = await bare.listen();
    try {
      assert.equal(bare.getStats().authEnabled, false);
      bare.setAuthToken(null);
      assert.equal(tokenRevisions(bus).length, 0);
      assert.equal(bare.getStats().authEnabled, false);
      const client = await connectWs(barePort);
      client.destroy();
    } finally {
      await bare.close();
    }
  });
});

// --- GraphQL gateway ----------------------------------------------------------

test('graphql: rotating the token closes the old token with 4401 and admits the new one', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, authToken: 'gql-old-token' }, async (gw, port) => {
    assert.equal(gw.getStats().authEnabled, true);

    const stale = await connectGql(port);
    try {
      await initAck(stale, { authorization: 'Bearer gql-old-token' });
    } finally {
      stale.destroy();
    }

    gw.setAuthToken('gql-new-token');
    assert.equal(gw.getStats().authEnabled, true);

    const old = await connectGql(port);
    old.send({ type: 'connection_init', payload: { authorization: 'Bearer gql-old-token' } });
    await withTimeout(old.waitClosed(), 3000, 'old token close');
    assert.equal(old.closeCode, 4401);
    old.destroy();
    assert.equal(gw.getStats().authFailures, 1);

    const fresh = await connectGql(port);
    try {
      await initAck(fresh, { authorization: 'Bearer gql-new-token' });
      assert.equal(gw.getStats().authFailures, 1);
    } finally {
      fresh.destroy();
    }

    const events = tokenRevisions(bus);
    assert.equal(events.length, 1);
    assert.equal(events[0]?.action, 'auth_token_revised');
    assert.deepEqual(events[0]?.detail, { gateway: 'graphql', changed: 'rotated' });
    assert.ok(!JSON.stringify(events).includes('gql-old-token'));
    assert.ok(!JSON.stringify(events).includes('gql-new-token'));
  });
});

test('graphql: clearing the token admits bare inits and leaves established connections alone', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, authToken: 'gql-old-token' }, async (gw, port) => {
    const client = await connectGql(port);
    try {
      await initAck(client, { authorization: 'Bearer gql-old-token' });
      client.send({
        type: 'subscribe',
        id: 's',
        payload: { query: 'subscription { events(topic: "auth.*") { topic payload seq } }' },
      });
      await waitForSubscribers(bus, 1, 'subscribe s');

      gw.setAuthToken(null);
      assert.equal(gw.getStats().authEnabled, false);

      // The established connection keeps receiving after the clear.
      bus.publish('auth.x', 1);
      const next = await withTimeout(client.next(), 3000, 'next after clear');
      assert.equal(next.type, 'next');
      assert.equal(client.closed, false);

      const bare = await connectGql(port);
      try {
        await initAck(bare);
      } finally {
        bare.destroy();
      }

      const events = tokenRevisions(bus);
      assert.equal(events.length, 1);
      assert.deepEqual(events[0]?.detail, { gateway: 'graphql', changed: 'cleared' });
      assert.ok(!JSON.stringify(events).includes('gql-old-token'));
    } finally {
      client.destroy();
    }
  });
});

test('graphql: invalid values throw RangeError and leave the live token untouched', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, authToken: 'gql-old-token' }, async (gw, port) => {
    assert.throws(() => gw.setAuthToken(''), RangeError);
    assert.throws(() => gw.setAuthToken(42 as unknown as string), RangeError);
    assert.throws(() => gw.setAuthToken(undefined as unknown as string), RangeError);
    assert.throws(() => gw.setAuthToken({} as unknown as string), RangeError);
    assert.equal(tokenRevisions(bus).length, 0);

    const good = await connectGql(port);
    try {
      await initAck(good, { authorization: 'Bearer gql-old-token' });
      assert.equal(gw.getStats().authEnabled, true);
      assert.equal(gw.getStats().authFailures, 0);
    } finally {
      good.destroy();
    }
    assert.equal(tokenRevisions(bus).length, 0);
  });
});

test('graphql: re-affirming the current token is a no-op with no audit', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, authToken: 'gql-old-token' }, async (gw, port) => {
    gw.setAuthToken('gql-old-token');
    assert.equal(tokenRevisions(bus).length, 0);
    assert.equal(gw.getStats().authEnabled, true);

    const bare = new GraphqlGateway(bus, { port: 0 });
    await bare.start();
    try {
      assert.equal(bare.getStats().authEnabled, false);
      bare.setAuthToken(null);
      assert.equal(tokenRevisions(bus).length, 0);
      assert.equal(bare.getStats().authEnabled, false);
      const client = await connectGql(bare.getPort());
      try {
        await initAck(client);
      } finally {
        client.destroy();
      }
    } finally {
      await bare.stop();
    }
  });
});
