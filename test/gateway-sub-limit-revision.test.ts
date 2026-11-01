/**
 * Gateway per-connection subscription-cap revision (EB-327).
 *
 * `setGatewayMaxSubscriptionsPerConnection` retunes
 * `maxSubscriptionsPerConnection` live — without restarting the server.
 * Only the field provided moves; an illegal value fails closed with
 * `RangeError` and the live cap untouched; an empty or identical
 * revision is a no-op. An explicitly `undefined` cap clears it back to
 * unlimited. The new cap governs from the next `subscribe` frame on
 * every connection, existing ones included: tightening denies only new
 * subscribes (a connection that already holds more subscriptions than
 * the new cap keeps them), loosening admits the next subscribe on an
 * over-cap connection at once.
 */
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

/**
 * Test-only introspection (mirrors `gateway-limits-revision.test.ts`):
 * the live subscription cap `setGatewayMaxSubscriptionsPerConnection`
 * revises. `getStats()` exposes the per-connection subscription count,
 * not the cap itself, so the tests read the field directly.
 */
function liveCap(gw: GatewayServer): number | undefined {
  return (
    gw as unknown as { maxSubscriptionsPerConnection: number | undefined }
  ).maxSubscriptionsPerConnection;
}

/** Subscribes `pattern` and returns the server's reply frame. */
async function subscribe(
  client: WsClient,
  id: string,
  pattern: string,
): Promise<Record<string, unknown>> {
  client.send({ type: 'subscribe', id, pattern });
  return withTimeout(client.next(), 3000, id);
}

test('setGatewayMaxSubscriptionsPerConnection rejects illegal values and leaves the live cap untouched', () => {
  const bus = new EventBus();
  // From unlimited: only a positive integer (or an explicit clear) moves.
  const gw = new GatewayServer(bus, {});
  assert.equal(liveCap(gw), undefined);
  const bad: Array<unknown> = [
    0,
    -1,
    1.5,
    Number.NaN,
    Infinity,
    'many',
    'nope',
    [],
  ];
  for (const value of bad) {
    assert.throws(
      () =>
        gw.setGatewayMaxSubscriptionsPerConnection({
          maxSubscriptionsPerConnection: value as never,
        }),
      RangeError,
      `expected RangeError for ${String(value)}`,
    );
    assert.equal(liveCap(gw), undefined);
  }
  assert.throws(
    () => gw.setGatewayMaxSubscriptionsPerConnection('nope' as never),
    RangeError,
  );
  assert.equal(liveCap(gw), undefined);

  // From a configured cap: the same values fail closed.
  const configured = new GatewayServer(bus, { maxSubscriptionsPerConnection: 2 });
  assert.equal(liveCap(configured), 2);
  for (const value of bad) {
    assert.throws(
      () =>
        configured.setGatewayMaxSubscriptionsPerConnection({
          maxSubscriptionsPerConnection: value as never,
        }),
      RangeError,
      `expected RangeError for ${String(value)}`,
    );
    assert.equal(liveCap(configured), 2);
  }
});

test('revision moves the cap; clearing and identical or empty revisions are no-ops', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, { maxSubscriptionsPerConnection: 5 });
  gw.setGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 3 });
  assert.equal(liveCap(gw), 3);

  // An empty revision, and a revision identical to the live cap, move nothing.
  gw.setGatewayMaxSubscriptionsPerConnection({});
  gw.setGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 3 });
  gw.setGatewayMaxSubscriptionsPerConnection(null as never);
  gw.setGatewayMaxSubscriptionsPerConnection(undefined as never);
  assert.equal(liveCap(gw), 3);

  // An explicitly `undefined` cap clears it back to unlimited.
  gw.setGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: undefined });
  assert.equal(liveCap(gw), undefined);

  // Clearing an already-unlimited cap is a no-op.
  gw.setGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: undefined });
  assert.equal(liveCap(gw), undefined);

  // And a cap can be set again after clearing.
  gw.setGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 2 });
  assert.equal(liveCap(gw), 2);
});

test('tightening the cap denies only new subscribes; existing subscriptions are kept', async () => {
  const bus = new EventBus();
  await withGateway(bus, { maxSubscriptionsPerConnection: 5 }, async (gw, port) => {
    const client = await connect(port);
    try {
      assert.equal((await subscribe(client, 's1', 'a.**')).type, 'subscribed');
      assert.equal((await subscribe(client, 's2', 'b.**')).type, 'subscribed');
      assert.equal(gw.getStats().connectionQueues[0].subscriptions, 2);

      // Tighten below the live count: the two existing subscriptions
      // stay, only the next subscribe is denied.
      gw.setGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 1 });
      assert.equal(liveCap(gw), 1);
      assert.equal(gw.getStats().connectionQueues[0].subscriptions, 2);
      assert.equal(bus.getStats().totalSubscribers, 2);

      const denied = await subscribe(client, 's3', 'c.**');
      assert.equal(denied.type, 'error');
      assert.equal(denied.code, 'subscribe_denied');
      assert.equal(denied.id, 's3');
      // The denial names the live cap, not the construction one.
      assert.match(denied.message as string, /maxSubscriptionsPerConnection=1/);
      assert.equal(gw.getStats().connectionQueues[0].subscriptions, 2);

      // The connection is still usable: ping round-trips.
      client.send({ type: 'ping', id: 7 });
      const pong = await withTimeout(client.next(), 3000, 'pong');
      assert.deepEqual(pong, { type: 'pong', id: 7 });
    } finally {
      client.destroy();
    }
  });
});

test('loosening the cap admits the next subscribe on an over-cap connection', async () => {
  const bus = new EventBus();
  await withGateway(bus, { maxSubscriptionsPerConnection: 1 }, async (gw, port) => {
    const client = await connect(port);
    try {
      assert.equal((await subscribe(client, 's1', 'a.**')).type, 'subscribed');
      const denied = await subscribe(client, 's2', 'b.**');
      assert.equal(denied.code, 'subscribe_denied');

      // Loosen the cap: the very next subscribe on this same
      // connection is admitted — no reconnect needed.
      gw.setGatewayMaxSubscriptionsPerConnection({ maxSubscriptionsPerConnection: 2 });
      assert.equal(liveCap(gw), 2);
      const admitted = await subscribe(client, 's2', 'b.**');
      assert.equal(admitted.type, 'subscribed');
      assert.equal(gw.getStats().connectionQueues[0].subscriptions, 2);
      assert.equal(bus.getStats().totalSubscribers, 2);

      // And the new cap binds the next subscribe after that.
      const deniedAgain = await subscribe(client, 's3', 'c.**');
      assert.equal(deniedAgain.code, 'subscribe_denied');
    } finally {
      client.destroy();
    }
  });
});

test('clearing the cap restores unlimited subscribes on an existing connection', async () => {
  const bus = new EventBus();
  await withGateway(bus, { maxSubscriptionsPerConnection: 1 }, async (gw, port) => {
    const client = await connect(port);
    try {
      assert.equal((await subscribe(client, 's1', 'a.**')).type, 'subscribed');
      const denied = await subscribe(client, 's2', 'b.**');
      assert.equal(denied.code, 'subscribe_denied');

      gw.setGatewayMaxSubscriptionsPerConnection({
        maxSubscriptionsPerConnection: undefined,
      });
      assert.equal(liveCap(gw), undefined);

      for (const [id, pattern] of [
        ['s2', 'b.**'],
        ['s3', 'c.**'],
        ['s4', 'd.**'],
      ] as const) {
        const reply = await subscribe(client, id, pattern);
        assert.equal(reply.type, 'subscribed', `expected ${id} to subscribe`);
      }
      assert.equal(gw.getStats().connectionQueues[0].subscriptions, 4);
      assert.equal(bus.getStats().totalSubscribers, 4);
    } finally {
      client.destroy();
    }
  });
});
