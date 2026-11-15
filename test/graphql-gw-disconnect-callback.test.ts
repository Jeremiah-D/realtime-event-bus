/**
 * GraphQL gateway disconnect-hook live revision (EB-517): the
 * GraphQL mirror of the WS gateway's `setDisconnectCallback`
 * (EB-451).
 *
 * `GraphqlGateway.setDisconnectCallback` swaps (function) or clears
 * (`null`) the `onDisconnect` hook live — without restarting the
 * server. The new hook governs from the very next teardown of an
 * already-initialized connection, and the old hook never fires again.
 * Replacement is total: the new hook does not automatically chain
 * `bus.noteGatewayDisconnect()`, so a hook that does not call it
 * leaves the EB-132 disconnect-storm detector blind. An invalid
 * value fails closed with `RangeError` and the live hook untouched;
 * re-installing the same hook reference is a no-op with no audit.
 * Every real change is audited as
 * `gateway_disconnect_callback_revised` with
 * `{ gateway: 'graphql', changed: 'set' | 'cleared' }` — the function
 * itself never enters the audit log. Uninitialized teardowns (init
 * timeout, pre-handshake disconnect) never fire the hook.
 */

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

/** Waits until `calls` holds at least `n` entries (server-side teardown). */
async function waitForCalls(calls: unknown[], n: number, what: string): Promise<void> {
  const deadline = Date.now() + 5000;
  for (;;) {
    if (calls.length >= n) return;
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${what} (got ${calls.length}, want ${n})`);
    }
    await sleep(10);
  }
}

/** Waits until the bus reports no subscribers. */
async function waitForNoSubscribers(bus: EventBus, what: string): Promise<void> {
  const deadline = Date.now() + 5000;
  for (;;) {
    if (bus.getStats().totalSubscribers === 0) return;
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${what}`);
    }
    await sleep(10);
  }
}

/** The `gateway_disconnect_callback_revised` audit events on this bus, oldest-first. */
function hookRevisions(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'gateway_disconnect_callback_revised' });
}

test('construction-time onDisconnect fires on initialized-connection teardown with id and remote address', async () => {
  const bus = new EventBus();
  const calls: Array<{ id: number; remoteAddress?: string }> = [];
  await withGqlGateway(
    bus,
    { port: 0, onDisconnect: (event) => calls.push(event) },
    async (gw, port) => {
      const client = await connectGql(port);
      try {
        await initAck(client);
        const id = gw.getStats().connectionsDetail[0]?.id;
        assert.equal(typeof id, 'number');
        client.destroy();
        await waitForCalls(calls, 1, 'constructor hook teardown');
        assert.equal(calls[0]?.id, id);
        // The remote address is reported when the socket still has
        // one at teardown time (it may already be gone).
        assert.ok(
          calls[0]?.remoteAddress === undefined || calls[0]?.remoteAddress === '127.0.0.1',
        );
        // Exactly once per connection: no second fire after a pause.
        await sleep(200);
        assert.equal(calls.length, 1);
      } finally {
        client.destroy();
      }
    },
  );
});

test('construction-time non-function onDisconnect throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () => new GraphqlGateway(bus, { port: 0, onDisconnect: 42 as unknown as (event: never) => void }),
    RangeError,
  );
  assert.throws(
    () =>
      new GraphqlGateway(bus, {
        port: 0,
        onDisconnect: 'hook' as unknown as (event: never) => void,
      }),
    RangeError,
  );
});

test('replacing the hook: the new hook fires on the next teardown and the old one never fires', async () => {
  const bus = new EventBus();
  const oldCalls: unknown[] = [];
  const newCalls: unknown[] = [];
  await withGqlGateway(
    bus,
    { port: 0, onDisconnect: (event) => oldCalls.push(event) },
    async (gw, port) => {
      const client = await connectGql(port);
      try {
        await initAck(client);
        gw.setDisconnectCallback((event) => newCalls.push(event));
        client.destroy();
        await waitForCalls(newCalls, 1, 'new hook teardown');
        assert.equal(oldCalls.length, 0);
        assert.equal(newCalls.length, 1);

        const events = hookRevisions(bus);
        assert.equal(events.length, 1);
        assert.equal(events[0]?.action, 'gateway_disconnect_callback_revised');
        assert.deepEqual(events[0]?.detail, { gateway: 'graphql', changed: 'set' });
      } finally {
        client.destroy();
      }
    },
  );
});

test('null clears the hook: teardown fires nothing and throws nothing', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  await withGqlGateway(
    bus,
    { port: 0, onDisconnect: (event) => calls.push(event) },
    async (gw, port) => {
      gw.setDisconnectCallback(null);
      const client = await connectGql(port);
      try {
        await initAck(client);
        client.destroy();
        await client.waitClosed();
        // Give the server-side teardown a chance to fire a stale hook.
        await sleep(200);
        assert.equal(calls.length, 0);

        const events = hookRevisions(bus);
        assert.equal(events.length, 1);
        assert.deepEqual(events[0]?.detail, { gateway: 'graphql', changed: 'cleared' });
      } finally {
        client.destroy();
      }
    },
  );
});

test('re-installing the same hook reference is a no-op with no audit', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  const hook = (event: { id: number }) => calls.push(event);
  await withGqlGateway(bus, { port: 0, onDisconnect: hook }, async (gw, port) => {
    gw.setDisconnectCallback(hook);
    assert.equal(hookRevisions(bus).length, 0);

    // The hook still works after the no-op re-install.
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.destroy();
      await waitForCalls(calls, 1, 'hook teardown after no-op');
      assert.equal(hookRevisions(bus).length, 0);
    } finally {
      client.destroy();
    }
  });

  // Clearing a gateway that holds no hook is a no-op too.
  const bare = new GraphqlGateway(bus, { port: 0 });
  await bare.start();
  try {
    bare.setDisconnectCallback(null);
    assert.equal(hookRevisions(bus).length, 0);
  } finally {
    await bare.stop();
  }
});

test('non-function values throw RangeError and leave the live hook untouched', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  await withGqlGateway(
    bus,
    { port: 0, onDisconnect: (event) => calls.push(event) },
    async (gw, port) => {
      assert.throws(() => gw.setDisconnectCallback(42 as unknown as null), RangeError);
      assert.throws(
        () => gw.setDisconnectCallback('hook' as unknown as null),
        RangeError,
      );
      assert.throws(() => gw.setDisconnectCallback({} as unknown as null), RangeError);
      assert.throws(
        () => gw.setDisconnectCallback(undefined as unknown as null),
        RangeError,
      );
      assert.equal(hookRevisions(bus).length, 0);

      // The live hook is untouched: it still fires on teardown.
      const client = await connectGql(port);
      try {
        await initAck(client);
        client.destroy();
        await waitForCalls(calls, 1, 'live hook teardown after failed revisions');
        assert.equal(hookRevisions(bus).length, 0);
      } finally {
        client.destroy();
      }
    },
  );
});

test('an uninitialized teardown fires neither the old nor the new hook', async () => {
  const bus = new EventBus();
  const oldCalls: unknown[] = [];
  const newCalls: unknown[] = [];
  await withGqlGateway(
    bus,
    { port: 0, connectionInitTimeoutMs: 150, onDisconnect: (event) => oldCalls.push(event) },
    async (gw, port) => {
      // Init timeout (closeWithCode path): never initialized, never fires.
      const client = await connectGql(port);
      await withTimeout(client.waitClosed(), 3000, 'init timeout close');
      assert.equal(client.closeCode, 4429);
      await sleep(200);
      assert.equal(oldCalls.length, 0);

      // Revise the hook, then disconnect before any connection_init.
      gw.setDisconnectCallback((event) => newCalls.push(event));
      const raw = await connectGql(port);
      raw.destroy();
      await sleep(200);
      assert.equal(oldCalls.length, 0);
      assert.equal(newCalls.length, 0);
    },
  );
});

test('a throwing hook is swallowed and teardown still unsubscribes', async () => {
  const bus = new EventBus();
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    gw.setDisconnectCallback(() => {
      throw new Error('hook boom');
    });
    const client = await connectGql(port);
    try {
      await initAck(client);
      client.send({ type: 'subscribe', id: 's1', payload: { query: SUB('topic: "a"') } });
      await waitForSubscribers(bus, 1, 'graphql subscribe');
      client.destroy();
      // Teardown completed despite the throwing hook: the bus
      // subscription is gone.
      await waitForNoSubscribers(bus, 'unsubscribe after throwing hook');
      assert.equal(bus.getStats().totalSubscribers, 0);
    } finally {
      client.destroy();
    }
  });
});

test('disconnectGraphqlConnection triggers the live hook with the connection id', async () => {
  const bus = new EventBus();
  const calls: Array<{ id: number; remoteAddress?: string }> = [];
  await withGqlGateway(
    bus,
    { port: 0, onDisconnect: (event) => calls.push(event) },
    async (gw, port) => {
      const client = await connectGql(port);
      const other = await connectGql(port);
      try {
        await initAck(client);
        await initAck(other);
        const id = gw.getStats().connectionsDetail.find(
          (row) => row.initialized,
        )?.id;
        assert.equal(typeof id, 'number');
        gw.disconnectGraphqlConnection(id as number);
        await waitForCalls(calls, 1, 'operator disconnect hook');
        assert.equal(calls[0]?.id, id);
        // Exactly once: the client's own socket close must not re-fire.
        await sleep(200);
        assert.equal(calls.length, 1);
        assert.equal(gw.getStats().connectionsDetail.length, 1);
      } finally {
        client.destroy();
        other.destroy();
      }
    },
  );
});

test('stop() on initialized connections triggers the hook', async () => {
  const bus = new EventBus();
  const calls: Array<{ id: number }> = [];
  const gw = new GraphqlGateway(bus, {
    port: 0,
    onDisconnect: (event) => calls.push(event),
  });
  await gw.start();
  try {
    const port = gw.getPort();
    const client = await connectGql(port);
    const other = await connectGql(port);
    try {
      await initAck(client);
      await initAck(other);
      const ids = gw.getStats().connectionsDetail.map((row) => row.id).sort();
      await gw.stop();
      await waitForCalls(calls, 2, 'stop() hook teardown');
      assert.deepEqual(
        calls.map((c) => c.id).sort(),
        ids,
      );
    } finally {
      client.destroy();
      other.destroy();
    }
  } finally {
    await gw.stop();
  }
});

test('audit detail is exact and never carries the function', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  await withGqlGateway(bus, { port: 0 }, async (gw, port) => {
    gw.setDisconnectCallback((event) => calls.push(event));
    gw.setDisconnectCallback(null);
    const events = hookRevisions(bus);
    assert.equal(events.length, 2);
    assert.deepEqual(events[0]?.detail, { gateway: 'graphql', changed: 'set' });
    assert.deepEqual(events[1]?.detail, { gateway: 'graphql', changed: 'cleared' });
    for (const event of events) {
      assert.equal(event?.action, 'gateway_disconnect_callback_revised');
      assert.doesNotMatch(JSON.stringify(event?.detail), /function/);
    }
    // The WS gateway's revisions on the same bus stay 'ws'.
    assert.equal(
      events.filter((e) => (e?.detail as { gateway?: string }).gateway === 'ws').length,
      0,
    );
  });
});
