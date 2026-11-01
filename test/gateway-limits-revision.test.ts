/**
 * Gateway frame and queue limit revision (EB-325).
 *
 * `setGatewayLimits` retunes `maxFrameBytes`, `maxPendingBytes` and
 * `sendHighWaterMarkBytes` live — without restarting the server. Only
 * the fields provided move; an illegal value fails closed with
 * `RangeError` and the live limits untouched; an empty or identical
 * revision is a no-op. `sendHighWaterMarkBytes` and `maxPendingBytes`
 * govern from the next write on every connection, existing ones
 * included; `maxFrameBytes` binds only connections established after
 * the revision — a connection that is already open keeps the bound its
 * `FrameParser` captured at connect time until it reconnects.
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
  private closed = false;
  private closeWaiters: Array<() => void> = [];
  private readonly socket: Socket;
  /** Close code from the server's close frame, when one arrived. */
  closeCode: number | undefined;

  constructor(socket: Socket) {
    this.socket = socket;
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.parser.push(chunk)) {
        if (frame.opcode === 0x8) {
          this.closed = true;
          if (frame.payload.length >= 2) this.closeCode = frame.payload.readUInt16BE(0);
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

const DEFAULT_LIMITS = {
  maxFrameBytes: 1024 * 1024,
  maxPendingBytes: 1024 * 1024,
  sendHighWaterMarkBytes: 256 * 1024,
};

const BIG = 'x'.repeat(8 * 1024);
const SMALL = 'x'.repeat(1024);

test('setGatewayLimits rejects illegal values and leaves the live limits untouched', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  assert.deepEqual(gw.getStats().limits, DEFAULT_LIMITS);
  const bad: Array<unknown> = [
    { maxFrameBytes: 0 },
    { maxFrameBytes: -1 },
    { maxFrameBytes: Number.NaN },
    { maxFrameBytes: Infinity },
    { maxFrameBytes: '1024' },
    { maxPendingBytes: 0 },
    { maxPendingBytes: -100 },
    { maxPendingBytes: Number.NaN },
    { maxPendingBytes: Infinity },
    { sendHighWaterMarkBytes: 0 },
    { sendHighWaterMarkBytes: -1 },
    { sendHighWaterMarkBytes: Number.NaN },
    { sendHighWaterMarkBytes: Infinity },
    // One bad field poisons the whole revision, even with a good one.
    { maxFrameBytes: 4096, maxPendingBytes: 0 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => gw.setGatewayLimits(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    assert.deepEqual(gw.getStats().limits, DEFAULT_LIMITS);
  }
});

test('partial revision moves only the fields provided', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  gw.setGatewayLimits({ maxPendingBytes: 4096 });
  assert.deepEqual(gw.getStats().limits, { ...DEFAULT_LIMITS, maxPendingBytes: 4096 });
  gw.setGatewayLimits({ sendHighWaterMarkBytes: 1024, maxFrameBytes: 2048 });
  assert.deepEqual(gw.getStats().limits, {
    maxFrameBytes: 2048,
    maxPendingBytes: 4096,
    sendHighWaterMarkBytes: 1024,
  });
  // Explicitly `undefined` is not a value: the field keeps its live value.
  gw.setGatewayLimits({ maxPendingBytes: undefined, maxFrameBytes: 8192 });
  assert.deepEqual(gw.getStats().limits, {
    maxFrameBytes: 8192,
    maxPendingBytes: 4096,
    sendHighWaterMarkBytes: 1024,
  });
});

test('identical and empty revisions are no-ops', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, { maxFrameBytes: 4096 });
  assert.equal(gw.getStats().limits.maxFrameBytes, 4096);
  gw.setGatewayLimits({});
  gw.setGatewayLimits({ maxFrameBytes: 4096 });
  gw.setGatewayLimits({
    maxFrameBytes: 4096,
    maxPendingBytes: 1024 * 1024,
    sendHighWaterMarkBytes: 256 * 1024,
  });
  gw.setGatewayLimits(null as never);
  gw.setGatewayLimits(undefined as never);
  assert.deepEqual(gw.getStats().limits, { ...DEFAULT_LIMITS, maxFrameBytes: 4096 });
});

test('maxFrameBytes binds only connections established after the revision', async () => {
  const bus = new EventBus();
  await withGateway(bus, { maxFrameBytes: 128 }, async (gw, port) => {
    // A subscribe frame of ~240 bytes: over the 128-byte bound, under
    // the 4096-byte bound the revision will set.
    const oversizedSubscribe = {
      type: 'subscribe',
      id: 's1',
      pattern: 'a'.repeat(200),
    };
    assert.ok(Buffer.byteLength(JSON.stringify(oversizedSubscribe), 'utf8') > 128);

    const existing = await connect(port);
    try {
      existing.send({ type: 'subscribe', id: 's0', pattern: 'flood.t' });
      const subscribed = await withTimeout(existing.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');

      gw.setGatewayLimits({ maxFrameBytes: 4096 });
      assert.equal(gw.getStats().limits.maxFrameBytes, 4096);

      // The pre-revision connection still carries its old bound: the
      // oversized frame closes it with 1009.
      existing.send(oversizedSubscribe);
      await withTimeout(existing.waitClosed(), 3000, 'existing connection 1009 close');
      assert.equal(existing.closeCode, 1009);
      assert.equal(gw.getStats().protocolErrors, 1);
    } finally {
      existing.destroy();
    }

    // A connection established after the revision gets the new bound:
    // the same frame subscribes fine.
    const fresh = await connect(port);
    try {
      fresh.send(oversizedSubscribe);
      const subscribed = await withTimeout(fresh.next(), 3000, 'fresh subscribed');
      assert.equal(subscribed.type, 'subscribed');
    } finally {
      fresh.destroy();
    }
  });
});

test('maxPendingBytes governs the next write on an existing connection', async () => {
  const bus = new EventBus();
  await withGateway(
    bus,
    { sendHighWaterMarkBytes: 1, maxPendingBytes: 2048 },
    async (gw, port) => {
      const client = await connect(port);
      try {
        client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
        const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
        assert.equal(subscribed.type, 'subscribed');
        client.pause();

        // Flood until the old 2048-byte queue bound starts dropping:
        // each ~8.2 KiB frame exceeds the whole bound, so once the
        // connection pauses every further frame is shed.
        let published = 0;
        const floodDeadline = Date.now() + 10_000;
        while (gw.getStats().droppedFrames === 0) {
          for (let i = 0; i < 200; i++) bus.publish('flood.t', { i: published, big: BIG });
          published += 200;
          if (published >= 4000 || Date.now() > floodDeadline) break;
          await sleep(50);
        }
        assert.ok(gw.getStats().droppedFrames > 0, 'expected drops under the old bound');

        // Settle until the drop count stops moving (subscriber queue drained).
        let prev = gw.getStats().droppedFrames;
        const settleDeadline = Date.now() + 5000;
        for (;;) {
          await sleep(200);
          const cur = gw.getStats().droppedFrames;
          if (cur === prev) break;
          prev = cur;
          if (Date.now() > settleDeadline) throw new Error('timed out waiting for drops to settle');
        }
        const s0 = gw.getStats().framesSent;
        const d0 = gw.getStats().droppedFrames;
        const q0 = gw.getStats().connectionQueues[0].queuedBytes;

        // Widen the bound: the next frames on this same connection must
        // queue (or send) instead of dropping — under the old bound
        // every one of them would have dropped.
        gw.setGatewayLimits({ maxPendingBytes: 64 * 1024 * 1024 });
        for (let i = 0; i < 5; i++) bus.publish('flood.t', { i: 10000 + i, big: BIG });
        await withTimeout(
          (async () => {
            const deadline = Date.now() + 5000;
            for (;;) {
              const stats = gw.getStats();
              const accounted =
                stats.framesSent - s0 +
                Math.floor(Math.max(0, stats.connectionQueues[0].queuedBytes - q0) / 8000);
              if (stats.droppedFrames === d0 && accounted >= 3) return;
              if (stats.droppedFrames !== d0) {
                throw new Error(`frames dropped after widening: ${d0} -> ${stats.droppedFrames}`);
              }
              if (Date.now() > deadline) throw new Error('timed out waiting for widened bound');
              await sleep(20);
            }
          })(),
          5000,
          'widened bound takes effect',
        );

        // Shrink the bound, then flood again: once the kernel fills and
        // the connection pauses, the next frame on this same connection
        // must drop — under the widened bound it would have queued.
        gw.setGatewayLimits({ maxPendingBytes: 64 });
        const shrinkDeadline = Date.now() + 10_000;
        published = 0;
        while (gw.getStats().droppedFrames === d0) {
          for (let i = 0; i < 200; i++) bus.publish('flood.t', { i: 20000 + published, big: BIG });
          published += 200;
          if (published >= 4000 || Date.now() > shrinkDeadline) break;
          await sleep(50);
        }
        assert.ok(
          gw.getStats().droppedFrames > d0,
          `expected drops under the shrunk bound (still ${d0})`,
        );
      } finally {
        client.destroy();
      }
    },
  );
});

/**
 * Test-only introspection (mirrors `fanout-chunk-revision.test.ts`):
 * the live per-connection send state the gateway's write path reads.
 * `bufferSize` is the socket's libuv write-queue size — 0 after a
 * successful write, over the 16 KiB writable mark only once a write
 * returned false (kernel full).
 */
interface LiveConnState {
  paused: boolean;
  pendingBytes: number;
  bufferSize: number;
}

function liveConnState(gw: GatewayServer): LiveConnState {
  const conns = (
    gw as unknown as {
      connections: Set<{
        paused: boolean;
        pendingBytes: number;
        socket: { bufferSize: number };
      }>;
    }
  ).connections;
  const first = [...conns][0];
  return {
    paused: first.paused,
    pendingBytes: first.pendingBytes,
    bufferSize: first.socket.bufferSize,
  };
}

/**
 * Floods a paused client's connection until its gateway queue first
 * goes non-empty, then snapshots the live send state in the same tick.
 * Returns the queued bytes and the live state at that moment.
 */
async function floodUntilQueued(
  bus: EventBus,
  gw: GatewayServer,
): Promise<{ queuedBytes: number; live: LiveConnState }> {
  const deadline = Date.now() + 15_000;
  let published = 0;
  for (;;) {
    for (let i = 0; i < 100; i++) bus.publish('flood.t', { i: published, big: BIG });
    published += 100;
    await flush();
    const queuedBytes = gw.getStats().connectionQueues[0].queuedBytes;
    if (queuedBytes > 0) return { queuedBytes, live: liveConnState(gw) };
    if (published >= 4000 || Date.now() > deadline) {
      throw new Error(`queue never formed (published ${published})`);
    }
    await sleep(10);
  }
}

test('sendHighWaterMarkBytes governs the next write on an existing connection', async () => {
  // The mark decides the direct-vs-queue branch only through the live
  // socket buffer size, so the two marks are told apart by HOW the
  // queue first forms on an already-open connection:
  //
  // - With the revised 1-byte mark, the first write that finds the
  //   kernel full leaves a small libuv remainder (at most one 8 KiB
  //   frame, a `true` write): the post-write check sees it past the
  //   1-byte mark and pauses the connection — no write ever returns
  //   false, so the socket buffer stays at or below Node's 16 KiB
  //   writable mark and no `drain` event is ever armed.
  // - With the old 1 MiB mark, the same remainder writes keep going
  //   out directly (8 KiB << 1 MiB) until the libuv queue grows past
  //   16 KiB and a write returns false — only then does the
  //   connection pause and queue. The socket buffer is past 16 KiB
  //   at that moment.
  //
  // Both connections flood identically; the live socket buffer size
  // at the moment the queue first forms separates them.
  const bus = new EventBus();
  const opts: GatewayOptions = {
    sendHighWaterMarkBytes: 1024 * 1024,
    maxPendingBytes: 64 * 1024 * 1024,
  };
  const revised = await (async () => {
    const gw = new GatewayServer(bus, opts);
    const port = await gw.listen();
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      // The connection already exists when the revision lands.
      gw.setGatewayLimits({ sendHighWaterMarkBytes: 1 });
      assert.equal(gw.getStats().limits.sendHighWaterMarkBytes, 1);
      assert.equal(
        (gw as unknown as { sendHighWaterMarkBytes: number }).sendHighWaterMarkBytes,
        1,
      );
      client.pause();
      const formed = await floodUntilQueued(bus, gw);
      return { gw, port, client, formed };
    } catch (err) {
      client.destroy();
      await gw.close();
      throw err;
    }
  })();
  try {
    assert.equal(revised.formed.live.paused, true);
    assert.ok(
      revised.formed.live.bufferSize <= 16 * 1024,
      `revised mark: expected no false write (bufferSize ${revised.formed.live.bufferSize})`,
    );
    assert.equal(revised.gw.getStats().droppedFrames, 0);
    // The queue drains once the client reads again, and the
    // connection stays usable under the revised mark.
    revised.client.resume();
    await withTimeout(
      (async () => {
        const deadline = Date.now() + 5000;
        for (;;) {
          if (revised.gw.getStats().connectionQueues[0].queuedBytes === 0) return;
          if (Date.now() > deadline) throw new Error('timed out waiting for queue to drain');
          await sleep(20);
        }
      })(),
      5000,
      'queue drains under revised mark',
    );
    await sleep(100);
    const sentBefore = revised.gw.getStats().framesSent;
    bus.publish('flood.t', { i: -1, big: SMALL });
    await withTimeout(
      (async () => {
        const deadline = Date.now() + 3000;
        for (;;) {
          if (revised.gw.getStats().framesSent > sentBefore) return;
          if (Date.now() > deadline) throw new Error('timed out waiting for delivery');
          await sleep(20);
        }
      })(),
      3000,
      'delivery after revision',
    );
  } finally {
    revised.client.destroy();
    await revised.gw.close();
  }

  // Control: the same flood against the unrevised 1 MiB mark queues
  // only after a write returns false, with the socket buffer past
  // the 16 KiB writable mark.
  const controlBus = new EventBus();
  await withGateway(controlBus, opts, async (gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'subscribe', id: 's1', pattern: 'flood.**', opts: { queueSize: 5000 } });
      const subscribed = await withTimeout(client.next(), 3000, 'subscribed');
      assert.equal(subscribed.type, 'subscribed');
      client.pause();
      const formed = await floodUntilQueued(controlBus, gw);
      assert.equal(formed.live.paused, true);
      assert.ok(
        formed.live.bufferSize > 16 * 1024,
        `control mark: expected a false write (bufferSize ${formed.live.bufferSize})`,
      );
      assert.equal(gw.getStats().droppedFrames, 0);
    } finally {
      client.destroy();
    }
  });
});
