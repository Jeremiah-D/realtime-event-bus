/**
 * Gateway client-pressure watermark revision (EB-326).
 *
 * `setGatewayClientPressure` retunes `clientPressureWatermarkBytes` and
 * `clientPressureRecoveryWatermarkBytes` live — without restarting the
 * server. Only the fields provided move; an illegal value fails closed
 * with `RangeError` and the live watermarks untouched; an empty or
 * identical revision is a no-op. An explicitly `undefined` pressure
 * clears the mode (releasing any in-flight pressure pause at once, so
 * the connection falls back to the historical `maxPendingBytes`
 * shed-newest behavior). The new watermarks govern from the next
 * pressure check on every connection, existing ones included.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GatewayServer, type GatewayOptions } from '../src/gateway-ws.ts';

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
function encodeClientFrame(payload: Buffer): Buffer {
  const len = payload.length;
  const b0 = 0x80 | 0x1;
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

/** Minimal client-side frame parser (server frames are never masked). */
class ClientParser {
  private buf = Buffer.alloc(0);

  push(chunk: Buffer): Array<{ opcode: number; payload: Buffer }> {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    const out: Array<{ opcode: number; payload: Buffer }> = [];
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
  private readonly socket: Socket;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.parser.push(chunk)) {
        if (frame.opcode !== 0x1) continue;
        const obj = JSON.parse(frame.payload.toString('utf8')) as Record<string, unknown>;
        const waiter = this.waiters.shift();
        if (waiter !== undefined) waiter(obj);
        else this.queue.push(obj);
      }
    });
    socket.on('error', () => {});
  }

  send(obj: unknown): void {
    this.socket.write(encodeClientFrame(Buffer.from(JSON.stringify(obj), 'utf8')));
  }

  next(): Promise<Record<string, unknown>> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    return new Promise((resolve) => this.waiters.push(resolve));
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
  await withTimeout(
    new Promise<void>((resolve, reject) => {
      let buf = Buffer.alloc(0);
      const onData = (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        const end = buf.indexOf('\r\n\r\n');
        if (end !== -1) {
          socket.off('data', onData);
          const rest = buf.subarray(end + 4);
          if (rest.length > 0) socket.emit('data', rest);
          resolve();
        }
      };
      socket.on('data', onData);
      socket.once('error', reject);
    }),
    3000,
    'handshake response',
  );
  return new WsClient(socket);
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

/**
 * Test-only introspection (mirrors `gateway-limits-revision.test.ts`):
 * the live pressure watermarks `setGatewayClientPressure` revises.
 * `getStats()` does not expose them — they are a tuning pair, not a
 * reported limit — so the tests read the fields directly.
 */
function livePressure(gw: GatewayServer): {
  pressure: number | undefined;
  recovery: number;
} {
  const g = gw as unknown as {
    clientPressureWatermarkBytes: number | undefined;
    clientPressureRecoveryWatermarkBytes: number;
  };
  return { pressure: g.clientPressureWatermarkBytes, recovery: g.clientPressureRecoveryWatermarkBytes };
}

const PRESSURE_OPTS: GatewayOptions = {
  sendHighWaterMarkBytes: 1,
  // Big enough that nothing is dropped: pressure must pause instead.
  maxPendingBytes: 64 * 1024 * 1024,
  clientPressureWatermarkBytes: 128 * 1024,
};

const BIG = 'x'.repeat(8 * 1024);

async function waitForPressure(gw: GatewayServer, what: string): Promise<void> {
  const deadline = Date.now() + 8000;
  for (;;) {
    if (gw.getStats().pausedConnections === 1) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for pressure: ${what}`);
    await sleep(50);
  }
}

async function waitForNoPressure(gw: GatewayServer, what: string): Promise<void> {
  const deadline = Date.now() + 10000;
  for (;;) {
    if (gw.getStats().pausedConnections === 0) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for pressure to clear: ${what}`);
    await sleep(50);
  }
}

async function subscribeFlood(client: WsClient): Promise<void> {
  client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
  const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
  assert.equal(subscribed.type, 'subscribed');
}

test('setGatewayClientPressure rejects illegal values and leaves the live watermarks untouched', () => {
  const bus = new EventBus();
  // From the disabled default: only a pressure can be set, and a
  // recovery alone is inert (construction stores it as given).
  const gw = new GatewayServer(bus, {});
  assert.deepEqual(livePressure(gw), { pressure: undefined, recovery: 0 });
  const badFromDisabled: Array<unknown> = [
    { clientPressureWatermarkBytes: 0 },
    { clientPressureWatermarkBytes: -1 },
    { clientPressureWatermarkBytes: Number.NaN },
    { clientPressureWatermarkBytes: Infinity },
    { clientPressureWatermarkBytes: '1024' },
    // A recovery without a pressure is inert, but a bad pressure next
    // to it still poisons the whole revision.
    { clientPressureWatermarkBytes: 0, clientPressureRecoveryWatermarkBytes: 100 },
    'nope',
    42,
    [],
  ];
  for (const revision of badFromDisabled) {
    assert.throws(
      () => gw.setGatewayClientPressure(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    assert.deepEqual(livePressure(gw), { pressure: undefined, recovery: 0 });
  }

  // From a configured pair: every illegal value fails closed.
  const configured = new GatewayServer(bus, {
    clientPressureWatermarkBytes: 128 * 1024,
    clientPressureRecoveryWatermarkBytes: 64 * 1024,
  });
  assert.deepEqual(livePressure(configured), {
    pressure: 128 * 1024,
    recovery: 64 * 1024,
  });
  const badFromConfigured: Array<unknown> = [
    { clientPressureWatermarkBytes: 0 },
    { clientPressureWatermarkBytes: -1 },
    { clientPressureWatermarkBytes: Number.NaN },
    { clientPressureWatermarkBytes: Infinity },
    { clientPressureWatermarkBytes: '1024' },
    { clientPressureRecoveryWatermarkBytes: 0 },
    { clientPressureRecoveryWatermarkBytes: -1 },
    { clientPressureRecoveryWatermarkBytes: Number.NaN },
    { clientPressureRecoveryWatermarkBytes: Infinity },
    // Recovery must stay strictly below the pressure watermark.
    { clientPressureRecoveryWatermarkBytes: 128 * 1024 },
    { clientPressureRecoveryWatermarkBytes: 200 * 1024 },
    { clientPressureWatermarkBytes: 64 * 1024, clientPressureRecoveryWatermarkBytes: 64 * 1024 },
    { clientPressureWatermarkBytes: 32 * 1024, clientPressureRecoveryWatermarkBytes: 64 * 1024 },
    // One bad field poisons the whole revision, even with a good one.
    { clientPressureWatermarkBytes: 256 * 1024, clientPressureRecoveryWatermarkBytes: 0 },
    'nope',
    42,
    [],
  ];
  for (const revision of badFromConfigured) {
    assert.throws(
      () => configured.setGatewayClientPressure(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    assert.deepEqual(livePressure(configured), {
      pressure: 128 * 1024,
      recovery: 64 * 1024,
    });
  }
});

test('partial revision moves only the fields provided; pressure-only keeps a valid recovery, re-derives otherwise', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {
    clientPressureWatermarkBytes: 128 * 1024,
    clientPressureRecoveryWatermarkBytes: 64 * 1024,
  });

  // Raising only the pressure keeps the live recovery (still below it).
  gw.setGatewayClientPressure({ clientPressureWatermarkBytes: 256 * 1024 });
  assert.deepEqual(livePressure(gw), { pressure: 256 * 1024, recovery: 64 * 1024 });

  // Revising only the recovery moves just that field.
  gw.setGatewayClientPressure({ clientPressureRecoveryWatermarkBytes: 32 * 1024 });
  assert.deepEqual(livePressure(gw), { pressure: 256 * 1024, recovery: 32 * 1024 });

  // Lowering the pressure below the live recovery re-derives the
  // recovery to the construction default, half the new pressure.
  gw.setGatewayClientPressure({ clientPressureWatermarkBytes: 16 * 1024 });
  assert.deepEqual(livePressure(gw), { pressure: 16 * 1024, recovery: 8 * 1024 });

  // An explicitly `undefined` recovery is not a value: it keeps the
  // live recovery (here it also keeps the pressure, a no-op overall).
  gw.setGatewayClientPressure({ clientPressureRecoveryWatermarkBytes: undefined });
  assert.deepEqual(livePressure(gw), { pressure: 16 * 1024, recovery: 8 * 1024 });

  // From the disabled default, setting only a pressure derives the
  // recovery as half of it, matching construction.
  const fresh = new GatewayServer(bus, {});
  fresh.setGatewayClientPressure({ clientPressureWatermarkBytes: 1000 });
  assert.deepEqual(livePressure(fresh), { pressure: 1000, recovery: 500 });

  // An explicitly `undefined` pressure clears the mode, and the
  // recovery resets to the construction default for disabled pressure.
  gw.setGatewayClientPressure({ clientPressureWatermarkBytes: undefined });
  assert.deepEqual(livePressure(gw), { pressure: undefined, recovery: 0 });
});

test('identical and empty revisions are no-ops', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {
    clientPressureWatermarkBytes: 4096,
    clientPressureRecoveryWatermarkBytes: 1024,
  });
  gw.setGatewayClientPressure({});
  gw.setGatewayClientPressure({ clientPressureWatermarkBytes: 4096 });
  gw.setGatewayClientPressure({ clientPressureRecoveryWatermarkBytes: 1024 });
  gw.setGatewayClientPressure({
    clientPressureWatermarkBytes: 4096,
    clientPressureRecoveryWatermarkBytes: 1024,
  });
  gw.setGatewayClientPressure(null as never);
  gw.setGatewayClientPressure(undefined as never);
  assert.deepEqual(livePressure(gw), { pressure: 4096, recovery: 1024 });
});

test('lowered pressure pauses an already-queued connection at the next check', async () => {
  const bus = new EventBus();
  // Start with a pressure watermark far above anything the queue will
  // reach, so the connection is unpressured before the revision.
  await withGateway(
    bus,
    {
      sendHighWaterMarkBytes: 1,
      maxPendingBytes: 64 * 1024 * 1024,
      clientPressureWatermarkBytes: 4 * 1024 * 1024,
    },
    async (gw, port) => {
      const client = await connect(port);
      try {
        await subscribeFlood(client);
        client.pause();
        // Flood only until the queue first passes 200 KiB — enough to
        // cross the revised watermark, far below the live one.
        const deadline = Date.now() + 15000;
        let published = 0;
        for (;;) {
          for (let i = 0; i < 100; i++) bus.publish('flood.t', { i: published, big: BIG });
          published += 100;
          await flush();
          const queued = gw.getStats().connectionQueues[0].queuedBytes;
          if (queued > 200 * 1024) break;
          if (published >= 4000 || Date.now() > deadline) {
            throw new Error(`queue never formed (published ${published}, queued ${queued})`);
          }
          await sleep(10);
        }
        assert.equal(gw.getStats().pausedConnections, 0);
        assert.equal(bus.getStats().pausedSubscribers, 0);

        // Lower the watermark below the already-queued bytes. The
        // revision alone does not pause anything — the new watermark
        // governs from the next pressure check.
        gw.setGatewayClientPressure({ clientPressureWatermarkBytes: 128 * 1024 });
        assert.deepEqual(livePressure(gw), { pressure: 128 * 1024, recovery: 64 * 1024 });
        assert.equal(gw.getStats().pausedConnections, 0);

        // One more publish forces the next check: the connection is
        // already over the new watermark, so it pauses now.
        bus.publish('flood.t', { i: -1, big: BIG });
        await waitForPressure(gw, 'lowered watermark');
        const stats = gw.getStats();
        assert.equal(stats.pausedConnections, 1);
        assert.equal(stats.connectionQueues[0].pressured, true);
        assert.ok(stats.connectionQueues[0].queuedBytes >= 128 * 1024);
        assert.equal(stats.droppedFrames, 0);
        assert.equal(bus.getStats().pausedSubscribers, 1);

        const audit = bus.getSubscriptionAudit({ action: 'client_pressure' });
        assert.equal(audit.length, 1);
        const detail = audit[0].detail as Record<string, unknown>;
        assert.equal(detail.watermarkBytes, 128 * 1024);
        assert.equal(detail.pausedSubscriptions, 1);
      } finally {
        client.destroy();
      }
    },
  );
});

test('revised recovery governs the resume of a pressured connection', async () => {
  const bus = new EventBus();
  await withGateway(bus, PRESSURE_OPTS, async (gw, port) => {
    const client = await connect(port);
    try {
      await subscribeFlood(client);
      client.pause();
      for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
      await waitForPressure(gw, 'first flood');

      // Raise the recovery watermark well above the construction
      // default (64 KiB) while keeping it below the pressure watermark.
      gw.setGatewayClientPressure({ clientPressureRecoveryWatermarkBytes: 120 * 1024 });
      assert.deepEqual(livePressure(gw), { pressure: 128 * 1024, recovery: 120 * 1024 });

      client.resume();
      await waitForNoPressure(gw, 'revised recovery');
      assert.equal(bus.getStats().pausedSubscribers, 0);

      // The resume was judged against the revised recovery, not the
      // construction default: the audit carries the new watermark.
      const recovered = bus.getSubscriptionAudit({ action: 'client_pressure_recovered' });
      assert.equal(recovered.length, 1);
      const detail = recovered[0].detail as Record<string, unknown>;
      assert.equal(detail.watermarkBytes, 120 * 1024);
      assert.ok((detail.queuedBytes as number) <= 120 * 1024);
      assert.equal(detail.pausedSubscriptions, 1);
    } finally {
      client.destroy();
    }
  });
});

test('clearing pressure releases an in-flight pause at once', async () => {
  const bus = new EventBus();
  await withGateway(bus, PRESSURE_OPTS, async (gw, port) => {
    const client = await connect(port);
    try {
      await subscribeFlood(client);
      client.pause();
      for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
      await waitForPressure(gw, 'first flood');
      assert.equal(bus.getStats().pausedSubscribers, 1);

      // Clearing the mode releases the pause immediately — no later
      // pressure check would ever run once the watermark is gone.
      gw.setGatewayClientPressure({ clientPressureWatermarkBytes: undefined });
      assert.deepEqual(livePressure(gw), { pressure: undefined, recovery: 0 });
      assert.equal(gw.getStats().pausedConnections, 0);
      assert.equal(bus.getStats().pausedSubscribers, 0);

      const recovered = bus.getSubscriptionAudit({ action: 'client_pressure_recovered' });
      assert.equal(recovered.length, 1);
      const detail = recovered[0].detail as Record<string, unknown>;
      assert.equal(detail.watermarkBytes, 64 * 1024); // the pre-clear recovery
      assert.equal(detail.pausedSubscriptions, 1);

      // Live delivery works again once the client reads.
      client.resume();
      bus.publish('flood.t', { n: 'live' });
      let found = false;
      const readDeadline = Date.now() + 8000;
      while (!found) {
        if (Date.now() > readDeadline) throw new Error('timed out waiting for live message');
        const frame = await withTimeout(client.next(), 8000, 'message frame');
        if (
          frame.type === 'message' &&
          (frame.payload as { n?: string } | undefined)?.n === 'live'
        ) {
          found = true;
        }
      }
      assert.ok(found);
    } finally {
      client.destroy();
    }
  });
});

test('cleared mode falls back to shed-newest instead of pausing', async () => {
  const bus = new EventBus();
  await withGateway(
    bus,
    {
      sendHighWaterMarkBytes: 1,
      maxPendingBytes: 64 * 1024,
      clientPressureWatermarkBytes: 32 * 1024,
    },
    async (gw, port) => {
      const client = await connect(port);
      try {
        await subscribeFlood(client);
        client.pause();
        for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
        await waitForPressure(gw, 'first flood');
        assert.equal(bus.getStats().pausedSubscribers, 1);
        // The synchronous burst collects into one bus batch, so the
        // already-collected batch keeps flowing past the 32 KiB
        // watermark until the pause stops the next batch — a few frames
        // shed past the small 64 KiB cap before the pause lands. That
        // overshoot is pre-existing bus batching, not the revision.
        const droppedAtClear = gw.getStats().droppedFrames;

        gw.setGatewayClientPressure({ clientPressureWatermarkBytes: undefined });
        assert.equal(gw.getStats().pausedConnections, 0);
        assert.equal(bus.getStats().pausedSubscribers, 0);

        // With the mode cleared, the same flood sheds the newest
        // frames past `maxPendingBytes` instead of pausing again: the
        // queue is already at the cap, so the next deliveries shed at
        // once and the drop count grows.
        const deadline = Date.now() + 10000;
        let published = 0;
        for (;;) {
          for (let i = 0; i < 200; i++) bus.publish('flood.t', { i: 2000 + published, big: BIG });
          published += 200;
          if (gw.getStats().droppedFrames > droppedAtClear) break;
          if (published >= 4000 || Date.now() > deadline) {
            throw new Error('expected drops after clearing pressure mode');
          }
          await sleep(50);
        }
        assert.equal(gw.getStats().pausedConnections, 0);
        assert.equal(bus.getStats().pausedSubscribers, 0);
      } finally {
        client.destroy();
      }
    },
  );
});
