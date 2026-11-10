/**
 * WS gateway disconnect-hook live revision (EB-451).
 *
 * `setDisconnectCallback` swaps (function) or clears (`null`) the
 * `onDisconnect` hook on `GatewayServer` live — without restarting
 * the server. The new hook governs from the very next teardown of an
 * already-handshook connection, and the old hook never fires again.
 * Replacement is total: the new hook does not automatically chain
 * `bus.noteGatewayDisconnect()`, so a hook that does not call it
 * leaves the EB-132 disconnect-storm detector blind. An invalid
 * value fails closed with `RangeError` and the live hook untouched;
 * re-installing the same hook reference is a no-op with no audit.
 * Every real change is audited as
 * `gateway_disconnect_callback_revised` with
 * `{ gateway: 'ws', changed: 'set' | 'cleared' }` — the function
 * itself never enters the audit log.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';
import { RecordingAlertSink } from '../src/ops-alert.ts';

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

/** Minimal WS client shared by the gateway tests. */
class MiniClient {
  private readonly parser = new ClientParser();
  private readonly queue: Array<Record<string, unknown>> = [];
  private waiters: Array<(v: Record<string, unknown>) => void> = [];
  private closeWaiters: Array<() => void> = [];
  closed = false;
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

async function connectWs(port: number): Promise<MiniClient> {
  const { client, statusLine } = await rawConnect(port);
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  return client;
}

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

test('replacing the hook: the new hook fires on the next teardown and the old one never fires', async () => {
  const bus = new EventBus();
  const oldCalls: unknown[] = [];
  const newCalls: unknown[] = [];
  await withGateway(
    bus,
    { onDisconnect: (event) => oldCalls.push(event) },
    async (gw, port) => {
      const client = await connectWs(port);
      try {
        gw.setDisconnectCallback((event) => newCalls.push(event));
        client.destroy();
        await waitForCalls(newCalls, 1, 'new hook teardown');
        assert.equal(oldCalls.length, 0);
        assert.equal(newCalls.length, 1);
        // The event carries the remote address when the socket still
        // reports one at teardown time (it may already be gone).
        const event = newCalls[0] as { remoteAddress?: string };
        assert.equal(typeof event, 'object');
        assert.ok(event.remoteAddress === undefined || event.remoteAddress === '127.0.0.1');

        const events = hookRevisions(bus);
        assert.equal(events.length, 1);
        assert.equal(events[0]?.action, 'gateway_disconnect_callback_revised');
        assert.deepEqual(events[0]?.detail, { gateway: 'ws', changed: 'set' });
      } finally {
        client.destroy();
      }
    },
  );
});

test('null clears the hook: teardown fires nothing and throws nothing', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  await withGateway(
    bus,
    { onDisconnect: (event) => calls.push(event) },
    async (gw, port) => {
      gw.setDisconnectCallback(null);
      const client = await connectWs(port);
      try {
        client.destroy();
        await client.waitClosed();
        // Give the server-side teardown a chance to fire a stale hook.
        await sleep(200);
        assert.equal(calls.length, 0);

        const events = hookRevisions(bus);
        assert.equal(events.length, 1);
        assert.deepEqual(events[0]?.detail, { gateway: 'ws', changed: 'cleared' });
      } finally {
        client.destroy();
      }
    },
  );
});

test('re-installing the same hook reference is a no-op with no audit', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  const hook = (event: { remoteAddress?: string }) => calls.push(event);
  await withGateway(bus, { onDisconnect: hook }, async (gw, port) => {
    gw.setDisconnectCallback(hook);
    assert.equal(hookRevisions(bus).length, 0);

    // The hook still works after the no-op re-install.
    const client = await connectWs(port);
    try {
      client.destroy();
      await waitForCalls(calls, 1, 'hook teardown after no-op');
      assert.equal(hookRevisions(bus).length, 0);
    } finally {
      client.destroy();
    }
  });

  // Clearing a gateway that holds no hook is a no-op too.
  const bare = new GatewayServer(bus, {});
  const barePort = await bare.listen();
  try {
    bare.setDisconnectCallback(null);
    assert.equal(hookRevisions(bus).length, 0);
  } finally {
    await bare.close();
  }
});

test('non-function values throw RangeError and leave the live hook untouched', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  await withGateway(
    bus,
    { onDisconnect: (event) => calls.push(event) },
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
      const client = await connectWs(port);
      try {
        client.destroy();
        await waitForCalls(calls, 1, 'live hook teardown after failed revisions');
        assert.equal(hookRevisions(bus).length, 0);
      } finally {
        client.destroy();
      }
    },
  );
});

test('an unhandshaken teardown fires neither the old nor the new hook', async () => {
  const bus = new EventBus();
  const oldCalls: unknown[] = [];
  const newCalls: unknown[] = [];
  await withGateway(
    bus,
    { onDisconnect: (event) => oldCalls.push(event) },
    async (gw, port) => {
      // TCP connected, handshake never sent: not a disconnect.
      const raw = tcpConnect(port, '127.0.0.1');
      raw.on('error', () => {});
      await withTimeout(
        new Promise<void>((resolve, reject) => {
          raw.once('connect', resolve);
          raw.once('error', reject);
        }),
        3000,
        'tcp connect',
      );
      raw.destroy();
      await sleep(200);
      assert.equal(oldCalls.length, 0);

      // Revise the hook, then fail another handshake the same way.
      gw.setDisconnectCallback((event) => newCalls.push(event));
      const raw2 = tcpConnect(port, '127.0.0.1');
      raw2.on('error', () => {});
      await withTimeout(
        new Promise<void>((resolve, reject) => {
          raw2.once('connect', resolve);
          raw2.once('error', reject);
        }),
        3000,
        'tcp connect',
      );
      raw2.destroy();
      await sleep(200);
      assert.equal(oldCalls.length, 0);
      assert.equal(newCalls.length, 0);
      // The only audit event is the revision itself, not the teardowns.
      assert.equal(hookRevisions(bus).length, 1);
    },
  );
});

test('a throwing new hook is swallowed and teardown still completes the unsubscribe', async () => {
  const bus = new EventBus();
  await withGateway(bus, {}, async (gw, port) => {
    const client = await connectWs(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'news.**' });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      assert.equal(bus.getStats().totalSubscribers, 1);

      gw.setDisconnectCallback(() => {
        throw new Error('EB451_THROWING_HOOK');
      });
      client.destroy();
      // Teardown must not fail: the subscription is gone afterwards.
      await waitForNoSubscribers(bus, 'unsubscribe after throwing hook');
      assert.equal(bus.getStats().totalSubscribers, 0);
    } finally {
      client.destroy();
    }
  });
});

test('audit action and detail never contain a function value', async () => {
  const bus = new EventBus();
  const markerHook = () => {
    void 'EB451_MARKER_FN_BODY';
  };
  await withGateway(bus, {}, async (gw, port) => {
    gw.setDisconnectCallback(markerHook);
    gw.setDisconnectCallback(null);
    const events = hookRevisions(bus);
    assert.equal(events.length, 2);
    assert.deepEqual(events[0]?.detail, { gateway: 'ws', changed: 'set' });
    assert.deepEqual(events[1]?.detail, { gateway: 'ws', changed: 'cleared' });
    for (const event of events) {
      assert.equal(event.action, 'gateway_disconnect_callback_revised');
      assert.ok(Object.values(event.detail ?? {}).every((v) => typeof v !== 'function'));
    }
    // The hook's source never enters the audit log.
    assert.ok(!JSON.stringify(events).includes('EB451_MARKER_FN_BODY'));
    assert.ok(!JSON.stringify(events).includes('markerHook'));
  });
});

test('a hook can be installed after construction when none was configured', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  await withGateway(bus, {}, async (gw, port) => {
    // No hook at construction: teardown fires nothing.
    const first = await connectWs(port);
    try {
      first.destroy();
      await first.waitClosed();
      await sleep(200);
      assert.equal(calls.length, 0);
    } finally {
      first.destroy();
    }

    gw.setDisconnectCallback((event) => calls.push(event));
    assert.equal(hookRevisions(bus).length, 1);

    const second = await connectWs(port);
    try {
      second.destroy();
      await waitForCalls(calls, 1, 'late-installed hook teardown');
      const event = calls[0] as { remoteAddress?: string };
      assert.equal(typeof event, 'object');
      assert.ok(event.remoteAddress === undefined || event.remoteAddress === '127.0.0.1');
    } finally {
      second.destroy();
    }
  });
});

test('replacement is total: a new hook that does not chain noteGatewayDisconnect leaves the EB-132 storm detector blind', async () => {
  const sink = new RecordingAlertSink();
  const bus = new EventBus({
    busId: 'ops-gw-eb451',
    opsAlert: {
      sink,
      secret: 's',
      dedupWindowMs: 60_000,
      gatewayDisconnectThreshold: 1,
      gatewayDisconnectWindowMs: 60_000,
    },
  });
  const plainCalls: unknown[] = [];
  const chainedCalls: unknown[] = [];
  await withGateway(
    bus,
    { onDisconnect: () => bus.noteGatewayDisconnect() },
    async (gw, port) => {
      // Replace the chaining hook with one that never calls
      // noteGatewayDisconnect: the detector must see nothing.
      gw.setDisconnectCallback((event) => plainCalls.push(event));
      const first = await connectWs(port);
      try {
        first.destroy();
        await waitForCalls(plainCalls, 1, 'plain hook teardown');
        await sleep(100);
        assert.equal(sink.deliveries.length, 0);
      } finally {
        first.destroy();
      }

      // Chain it yourself and the detector sees the disconnect again.
      gw.setDisconnectCallback((event) => {
        chainedCalls.push(event);
        bus.noteGatewayDisconnect();
      });
      const second = await connectWs(port);
      try {
        second.destroy();
        await waitForCalls(chainedCalls, 1, 'chained hook teardown');
        const deadline = Date.now() + 5000;
        for (;;) {
          if (sink.deliveries.length >= 1) break;
          if (Date.now() > deadline) {
            throw new Error('timed out waiting for storm alert');
          }
          await sleep(10);
        }
        assert.equal(sink.deliveries[0].alert.kind, 'gateway-disconnect-storm');
      } finally {
        second.destroy();
      }
    },
  );
});

test('post-close behaviour follows the sibling setters (EB-154): revising after close still works and audits', async () => {
  const bus = new EventBus();
  const oldCalls: unknown[] = [];
  const newCalls: unknown[] = [];
  const gw = new GatewayServer(bus, { onDisconnect: (event) => oldCalls.push(event) });
  const port = await gw.listen();
  const client = await connectWs(port);
  try {
    // The live hook governs the close-driven teardown.
    gw.setDisconnectCallback((event) => newCalls.push(event));
    await gw.close();
    assert.equal(newCalls.length, 1);
    assert.equal(oldCalls.length, 0);

    // Like setAuthToken, the setter keeps working after close and audits.
    const afterCalls: unknown[] = [];
    gw.setDisconnectCallback((event) => afterCalls.push(event));
    gw.setDisconnectCallback(null);
    const events = hookRevisions(bus);
    assert.equal(events.length, 3);
    assert.deepEqual(
      events.map((e) => e.detail),
      [
        { gateway: 'ws', changed: 'set' },
        { gateway: 'ws', changed: 'set' },
        { gateway: 'ws', changed: 'cleared' },
      ],
    );
  } finally {
    client.destroy();
    await bus.close();
  }
});
