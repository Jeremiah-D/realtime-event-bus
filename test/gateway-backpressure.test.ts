/**
 * Gateway client pressure (EB-161): a connection whose queued outbound
 * bytes cross the opt-in `clientPressureWatermarkBytes` pauses its bus
 * subscriptions (delivery-side hold, nothing dropped) instead of
 * growing the gateway process's memory; draining back under the
 * recovery watermark resumes exactly the pressure-paused subscriptions.
 * Both transitions are audited as `client_pressure` /
 * `client_pressure_recovered`.
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

test('client pressure: queued bytes over the watermark pause subscriptions and audit the transition', async () => {
  const bus = new EventBus();
  await withGateway(bus, PRESSURE_OPTS, async (gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      client.pause();
      for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
      await waitForPressure(gw, 'first flood');

      const stats = gw.getStats();
      assert.equal(stats.pausedConnections, 1);
      assert.equal(stats.connectionQueues.length, 1);
      assert.equal(stats.connectionQueues[0].pressured, true);
      assert.ok(
        stats.connectionQueues[0].queuedBytes >= 128 * 1024,
        `queued ${stats.connectionQueues[0].queuedBytes}`,
      );
      // Nothing dropped: pressure paused delivery instead of shedding.
      assert.equal(stats.droppedFrames, 0);
      // The bus-side subscriber is manually paused (delivery-side hold).
      assert.equal(bus.getStats().pausedSubscribers, 1);

      const audit = bus.getSubscriptionAudit({ action: 'client_pressure' });
      assert.equal(audit.length, 1);
      const detail = audit[0].detail as Record<string, unknown>;
      assert.ok((detail.queuedBytes as number) >= 128 * 1024);
      assert.equal(detail.watermarkBytes, 128 * 1024);
      assert.equal(detail.pausedSubscriptions, 1);
    } finally {
      client.destroy();
    }
  });
});

test('client pressure: paused subscriptions queue no new frames (bounded, no drops)', async () => {
  const bus = new EventBus();
  await withGateway(bus, PRESSURE_OPTS, async (gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      client.pause();
      for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
      await waitForPressure(gw, 'first flood');
      const q1 = gw.getStats().connectionQueues[0].queuedBytes;

      // A second flood lands on the paused subscriber: the gateway queue
      // must not grow (it may shrink as the drain poll moves frames into
      // the kernel) and nothing is dropped.
      for (let i = 2000; i < 4000; i++) bus.publish('flood.t', { i, big: BIG });
      await sleep(800);
      const stats = gw.getStats();
      assert.equal(stats.pausedConnections, 1);
      const q2 = stats.connectionQueues[0].queuedBytes;
      assert.ok(q2 <= q1, `queue grew under pressure: ${q1} -> ${q2}`);
      assert.equal(stats.droppedFrames, 0);
    } finally {
      client.destroy();
    }
  });
});

test('client pressure: recovery resumes subscriptions, drains the backlog, and audits', async () => {
  const bus = new EventBus();
  await withGateway(bus, PRESSURE_OPTS, async (gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      client.pause();
      for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
      await waitForPressure(gw, 'first flood');

      // The client starts reading again: the queue drains under the
      // recovery watermark and the paused subscription resumes.
      client.resume();
      const deadline = Date.now() + 10000;
      for (;;) {
        const s = gw.getStats();
        if (s.pausedConnections === 0 && s.connectionQueues[0].queuedBytes === 0) break;
        if (Date.now() > deadline) {
          throw new Error(
            `timed out waiting for recovery: ${JSON.stringify({ paused: s.pausedConnections, queued: s.connectionQueues[0]?.queuedBytes })}`,
          );
        }
        await sleep(50);
      }
      assert.equal(bus.getStats().pausedSubscribers, 0);

      const recovered = bus.getSubscriptionAudit({ action: 'client_pressure_recovered' });
      assert.equal(recovered.length, 1);
      const detail = recovered[0].detail as Record<string, unknown>;
      assert.equal(detail.watermarkBytes, 64 * 1024); // default: half the watermark
      assert.equal(detail.pausedSubscriptions, 1);

      // Live delivery works again after recovery.
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

test('client pressure: recovery never clears an operator manual pause', async () => {
  const bus = new EventBus();
  await withGateway(bus, PRESSURE_OPTS, async (gw, port) => {
    const client = await connect(port);
    try {
      // Two subscriptions on one connection; B is manually paused by an
      // operator before pressure engages.
      client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
      const sub1Frame = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(sub1Frame.type, 'subscribed');
      client.send({ type: 'subscribe', id: 's2', pattern: 'flood.**', opts: { queueSize: 5000 } });
      const sub2 = (await withTimeout(client.next(), 3000, 'subscribed')) as {
        subId: string;
      };
      assert.ok(bus.pauseSubscriber(sub2.subId, { reason: 'operator-hold' }));
      client.pause();
      for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
      await waitForPressure(gw, 'first flood');
      // A is pressure-paused, B was already operator-paused.
      assert.equal(bus.getStats().pausedSubscribers, 2);
      const audit = bus.getSubscriptionAudit({ action: 'client_pressure' });
      assert.equal((audit[0].detail as Record<string, unknown>).pausedSubscriptions, 1);

      client.resume();
      const deadline = Date.now() + 10000;
      for (;;) {
        if (gw.getStats().pausedConnections === 0) break;
        if (Date.now() > deadline) throw new Error('timed out waiting for recovery');
        await sleep(50);
      }
      // Only the pressure pause was cleared; the operator hold survives.
      assert.equal(bus.getStats().pausedSubscribers, 1);
      const resumed = bus.getSubscriptionAudit({ action: 'client_pressure_recovered' });
      assert.equal((resumed[0].detail as Record<string, unknown>).pausedSubscriptions, 1);
    } finally {
      client.destroy();
    }
  });
});

test('client pressure: disabled by default (historical shed behavior unchanged)', async () => {
  const bus = new EventBus();
  await withGateway(
    bus,
    { sendHighWaterMarkBytes: 1, maxPendingBytes: 64 },
    async (gw, port) => {
      const client = await connect(port);
      try {
        client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
        const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
        assert.equal(subscribed.type, 'subscribed');
        client.pause();
        for (let i = 0; i < 2000; i++) bus.publish('flood.t', { i, big: BIG });
        await sleep(500);
        const stats = gw.getStats();
        assert.equal(stats.pausedConnections, 0);
        assert.equal(stats.connectionQueues[0].pressured, false);
        // Historical behavior: the queue sheds the newest frames.
        assert.ok(stats.droppedFrames > 0, `expected drops, got ${stats.droppedFrames}`);
        assert.equal(bus.getStats().pausedSubscribers, 0);
      } finally {
        client.destroy();
      }
    },
  );
});

test('client pressure: option validation', () => {
  const bus = new EventBus();
  assert.throws(() => new GatewayServer(bus, { clientPressureWatermarkBytes: 0 }), RangeError);
  assert.throws(() => new GatewayServer(bus, { clientPressureWatermarkBytes: -5 }), RangeError);
  assert.throws(() => new GatewayServer(bus, { clientPressureWatermarkBytes: NaN }), RangeError);
  assert.throws(
    () =>
      new GatewayServer(bus, {
        clientPressureWatermarkBytes: 1000,
        clientPressureRecoveryWatermarkBytes: 1000,
      }),
    RangeError,
  );
  assert.throws(
    () =>
      new GatewayServer(bus, {
        clientPressureWatermarkBytes: 1000,
        clientPressureRecoveryWatermarkBytes: 2000,
      }),
    RangeError,
  );
  assert.throws(
    () =>
      new GatewayServer(bus, {
        clientPressureWatermarkBytes: 1000,
        clientPressureRecoveryWatermarkBytes: 0,
      }),
    RangeError,
  );
  // A valid pair constructs fine.
  const gw = new GatewayServer(bus, {
    clientPressureWatermarkBytes: 1000,
    clientPressureRecoveryWatermarkBytes: 400,
  });
  assert.ok(gw.getStats().pausedConnections === 0);
  void bus;
});
