/**
 * GraphQL gateway connection-limit live revision (EB-349).
 *
 * `setGraphqlGatewayLimits` retunes `maxConnections` and
 * `connectionInitTimeoutMs` live — without restarting the server. Only
 * the fields provided move; an explicitly `undefined` cap clears it
 * back to unlimited. The new cap governs from the next connection
 * (already-open connections are never dropped); the new init timeout
 * arms only handshakes that start after the revision. An illegal value
 * fails closed with `RangeError` and the live limits untouched; an
 * empty or identical revision is a no-op. The live tuning is
 * observable in `getStats().limits`.
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

test('tightening maxConnections refuses new sockets with 503; the open one is unaffected', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxConnections: 2 }, async (gw, port) => {
    const first = await connectGql(port);
    const second = await connectGql(port);
    try {
      gw.setGraphqlGatewayLimits({ maxConnections: 1 });
      assert.deepEqual(gw.getStats().limits, {
        maxConnections: 1,
        connectionInitTimeoutMs: 5000,
      });
      const third = await rawGqlConnect(port, { protocol: 'graphql-ws' });
      assert.match(third.statusLine, /^HTTP\/1\.1 503/);
      third.client.destroy();
      // Both open connections are unaffected by the tightened cap.
      await initAck(first);
      await initAck(second);
    } finally {
      first.destroy();
      second.destroy();
    }
  });
});

test('loosening the cap admits new connections again', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxConnections: 1 }, async (gw, port) => {
    const first = await connectGql(port);
    try {
      const refused = await rawGqlConnect(port, { protocol: 'graphql-ws' });
      assert.match(refused.statusLine, /^HTTP\/1\.1 503/);
      refused.client.destroy();
      gw.setGraphqlGatewayLimits({ maxConnections: 2 });
      const second = await connectGql(port);
      try {
        await initAck(second);
      } finally {
        second.destroy();
      }
    } finally {
      first.destroy();
    }
  });
});

test('clearing the cap with explicit undefined restores unlimited', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxConnections: 1 }, async (gw, port) => {
    const first = await connectGql(port);
    try {
      gw.setGraphqlGatewayLimits({ maxConnections: undefined });
      assert.equal(gw.getStats().limits.maxConnections, undefined);
      const second = await connectGql(port);
      const third = await connectGql(port);
      try {
        await initAck(second);
        await initAck(third);
      } finally {
        second.destroy();
        third.destroy();
      }
    } finally {
      first.destroy();
    }
  });
});

test('the new init timeout arms only new handshakes; an armed timer keeps the old value', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    // A connects under the default 5000ms timeout; its init timer is armed.
    const oldTimer = await connectGql(port);
    try {
      gw.setGraphqlGatewayLimits({ connectionInitTimeoutMs: 150 });
      assert.equal(gw.getStats().limits.connectionInitTimeoutMs, 150);
      // B connects after the revision and never sends connection_init:
      // the new 150ms timeout closes it with 4429.
      const fresh = await connectGql(port);
      await withTimeout(fresh.waitClosed(), 3000, 'init timeout close');
      assert.equal(fresh.closeCode, 4429);
      // A survived past the 150ms mark: its timer still runs the old
      // 5000ms value, so connection_init still answers.
      await sleep(400);
      await initAck(oldTimer);
    } finally {
      oldTimer.destroy();
    }
  });
});

test('illegal values throw RangeError and leave the live limits untouched', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxConnections: 2 }, async (gw) => {
    const live = { maxConnections: 2, connectionInitTimeoutMs: 5000 };
    assert.deepEqual(gw.getStats().limits, live);
    for (const bad of [0, -1, 1.5, NaN, Infinity, '2' as never]) {
      assert.throws(
        () => gw.setGraphqlGatewayLimits({ maxConnections: bad as never }),
        RangeError,
        `maxConnections=${bad}`,
      );
      assert.deepEqual(gw.getStats().limits, live);
    }
    for (const bad of [0, -5, 2.5, NaN, Infinity, '150' as never]) {
      assert.throws(
        () => gw.setGraphqlGatewayLimits({ connectionInitTimeoutMs: bad as never }),
        RangeError,
        `connectionInitTimeoutMs=${bad}`,
      );
      assert.deepEqual(gw.getStats().limits, live);
    }
    // A non-object revision and an array revision both throw.
    assert.throws(() => gw.setGraphqlGatewayLimits('x' as never), RangeError);
    assert.throws(() => gw.setGraphqlGatewayLimits([1] as never), RangeError);
    assert.deepEqual(gw.getStats().limits, live);
  });
});

test('an empty revision, an identical revision, and an absent one are no-ops', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0, maxConnections: 2 }, async (gw) => {
    const live = { maxConnections: 2, connectionInitTimeoutMs: 5000 };
    gw.setGraphqlGatewayLimits({});
    assert.deepEqual(gw.getStats().limits, live);
    gw.setGraphqlGatewayLimits({ maxConnections: 2 });
    assert.deepEqual(gw.getStats().limits, live);
    gw.setGraphqlGatewayLimits({ connectionInitTimeoutMs: 5000 });
    assert.deepEqual(gw.getStats().limits, live);
    gw.setGraphqlGatewayLimits(null as never);
    gw.setGraphqlGatewayLimits(undefined as never);
    assert.deepEqual(gw.getStats().limits, live);
  });
});

test('getStats reflects the live limits, including a cleared cap', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw) => {
    assert.deepEqual(gw.getStats().limits, {
      maxConnections: undefined,
      connectionInitTimeoutMs: 5000,
    });
    gw.setGraphqlGatewayLimits({ maxConnections: 3, connectionInitTimeoutMs: 2500 });
    assert.deepEqual(gw.getStats().limits, {
      maxConnections: 3,
      connectionInitTimeoutMs: 2500,
    });
    // Clearing only the cap keeps the revised timeout.
    gw.setGraphqlGatewayLimits({ maxConnections: undefined });
    assert.deepEqual(gw.getStats().limits, {
      maxConnections: undefined,
      connectionInitTimeoutMs: 2500,
    });
  });
});

test('a revision before start applies once the gateway starts', async () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0, maxConnections: 2 });
  gw.setGraphqlGatewayLimits({ maxConnections: 1 });
  await gw.start();
  try {
    const first = await connectGql(gw.getPort());
    try {
      const second = await rawGqlConnect(gw.getPort(), { protocol: 'graphql-ws' });
      assert.match(second.statusLine, /^HTTP\/1\.1 503/);
      second.client.destroy();
      await initAck(first);
    } finally {
      first.destroy();
    }
  } finally {
    await gw.stop();
  }
});
