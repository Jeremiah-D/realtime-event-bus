import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect, type Socket } from 'node:net';
import { randomBytes } from 'node:crypto';
import { EventBus } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';

/**
 * Bus-level default publish priority (EB-513):
 * `setDefaultPublishPriority` installs the lane a new publish without
 * an explicit `priority` falls back to — the priority chain is
 * publish-explicit > bus default > `'normal'` — and a revision never
 * moves an already-queued message's lane or a scheduled delayed
 * record. The default never crosses a bus boundary: bridge/cluster
 * inbound frames and route forwards still land in `normal`.
 *
 * Lane assertions follow `lanes.test.ts`: `getStats().lanes.delivered`
 * reads the resolved lane off the drain, and the subscriber envelope
 * carries `lane` only for non-normal traffic.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time. Due
 * sweeps run at the start of every flush, so advancing this clock past
 * a `deliverAt` and then publishing anything delivers deterministically.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test("EB-513: the factory default is unset (a plain publish rides normal)", async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultPublishPriority(), undefined);
  const received: Array<{ payload: unknown; lane?: string }> = [];
  bus.subscribe('t', (msg) => received.push({ payload: msg.payload, lane: msg.lane }));
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(received, [{ payload: 'x', lane: undefined }]);
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 0, normal: 1, bulk: 0 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_publish_priority_revised' }).length,
    0,
  );
});

test("EB-513: set('urgent') lifts plain publishes into the urgent lane", async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  assert.equal(bus.getDefaultPublishPriority(), 'urgent');
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Bulk first: the rotation must still serve the urgent default first.
  bus.publish('t', 'b1', { priority: 'bulk' });
  bus.publish('t', 'u1');
  await flush();
  assert.deepEqual(received, ['u1', 'b1']);
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 1, normal: 0, bulk: 1 });
});

test("EB-513: an explicit 'bulk' beats an 'urgent' default", async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  const received: Array<{ payload: unknown; lane?: string }> = [];
  bus.subscribe('t', (msg) => received.push({ payload: msg.payload, lane: msg.lane }));
  bus.publish('t', 'b1', { priority: 'bulk' });
  await flush();
  assert.deepEqual(received, [{ payload: 'b1', lane: 'bulk' }]);
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 0, normal: 0, bulk: 1 });
});

test("EB-513: an explicit 'normal' beats an 'urgent' default", async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  const received: Array<{ payload: unknown; lane?: string }> = [];
  bus.subscribe('t', (msg) => received.push({ payload: msg.payload, lane: msg.lane }));
  bus.publish('t', 'n1', { priority: 'normal' });
  await flush();
  assert.deepEqual(received, [{ payload: 'n1', lane: undefined }]);
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 0, normal: 1, bulk: 0 });
});

test("EB-513: set(null) clears the default back to normal", async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  bus.setDefaultPublishPriority(null);
  assert.equal(bus.getDefaultPublishPriority(), undefined);
  bus.subscribe('t', () => {});
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 0, normal: 1, bulk: 0 });
});

test('EB-513: illegal values throw RangeError and leave the live default untouched', async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  assert.throws(
    () => bus.setDefaultPublishPriority('critical' as never),
    RangeError,
  );
  assert.throws(() => bus.setDefaultPublishPriority(42 as never), RangeError);
  assert.throws(() => bus.setDefaultPublishPriority({} as never), RangeError);
  assert.throws(() => bus.setDefaultPublishPriority('urgent', ''), RangeError);
  assert.equal(bus.getDefaultPublishPriority(), 'urgent');
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_publish_priority_revised' }).length,
    1,
    'the failed calls audited nothing',
  );
  bus.subscribe('t', () => {});
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 1, normal: 0, bulk: 0 });
});

test('EB-513: re-affirming the current default is a no-op with no audit', async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('normal');
  bus.setDefaultPublishPriority(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_publish_priority_revised' }).length,
    0,
    "setting 'normal' on a factory bus and clearing an already-unset default both audit nothing",
  );
  assert.equal(bus.getDefaultPublishPriority(), undefined);
  bus.setDefaultPublishPriority('urgent');
  bus.setDefaultPublishPriority('urgent');
  const audit = bus.getSubscriptionAudit({ action: 'default_publish_priority_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, { previous: null, priority: 'urgent' });
});

test("EB-513: a revision never moves an already-queued message's lane", async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 'queued-before');
  bus.setDefaultPublishPriority('urgent');
  bus.publish('t', 'published-after');
  await flush();
  // The queued message keeps its normal lane; only the later publish
  // takes the new default — so the later (urgent) message drains first.
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 1, normal: 1, bulk: 0 });
});

test('EB-513: publishDelayed stamps the default at schedule time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: Array<{ payload: unknown; lane?: string }> = [];
  bus.subscribe('t', (msg) => received.push({ payload: msg.payload, lane: msg.lane }));
  bus.setDefaultPublishPriority('urgent');
  bus.publishDelayed('t', 'd1', { delayMs: 1000 });
  // Revising after scheduling must not move the scheduled record.
  bus.setDefaultPublishPriority('bulk');
  clock.nowMs = 2_000;
  bus.publish('__poke__', 'poke');
  await flush();
  assert.deepEqual(
    received.filter((r) => r.payload !== 'poke'),
    [{ payload: 'd1', lane: 'urgent' }],
  );
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('EB-513: publishDedupedByContent and publishIdempotent take the default', async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  bus.subscribe('t', () => {});
  const deduped = bus.publishDedupedByContent('t', 'same');
  assert.deepEqual(deduped, { duplicate: false, accepted: 1 });
  const dedupedAgain = bus.publishDedupedByContent('t', 'same');
  assert.deepEqual(dedupedAgain, { duplicate: true, accepted: 0 });
  const withId = bus.publishIdempotent('t', 'id-1', { messageId: 'm-1' });
  assert.deepEqual(withId, { duplicate: false, accepted: 1 });
  const noId = bus.publishIdempotent('t', 'id-2');
  assert.deepEqual(noId, { duplicate: false, accepted: 1 });
  await flush();
  // One deduped + two idempotent publishes, all in the urgent lane; the
  // suppressed duplicate delivered nothing.
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 3, normal: 0, bulk: 0 });
});

test('EB-513: bridge inbound still lands in the normal lane', async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  bus.subscribe('t', () => {});
  bus.receiveForward({ topic: 't', payload: 'forwarded' });
  await flush();
  assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 0, normal: 1, bulk: 0 });
});

test('EB-513: audit records previous/priority exactly and the getter follows live', async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  bus.setDefaultPublishPriority('bulk');
  bus.setDefaultPublishPriority('normal');
  bus.setDefaultPublishPriority('urgent');
  bus.setDefaultPublishPriority(null);
  const audit = bus.getSubscriptionAudit({ action: 'default_publish_priority_revised' });
  assert.equal(audit.length, 5);
  assert.deepEqual(audit[0].detail, { previous: null, priority: 'urgent' });
  assert.deepEqual(audit[1].detail, { previous: 'urgent', priority: 'bulk' });
  assert.deepEqual(audit[2].detail, { previous: 'bulk', priority: 'normal' });
  assert.deepEqual(audit[3].detail, { previous: 'normal', priority: 'urgent' });
  assert.deepEqual(audit[4].detail, { previous: 'urgent', priority: null });
  assert.equal(bus.getDefaultPublishPriority(), undefined);
  // Clearing a bus already at the effective 'normal' is a no-op: the
  // fifth entry above already cleared it, so a second clear audits nothing.
  bus.setDefaultPublishPriority(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_publish_priority_revised' }).length,
    5,
  );
});

// --- Minimal raw-TCP WebSocket client (same shape as gateway-ws.test.ts) ---

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
  private closeWaiters: Array<() => void> = [];
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

  destroy(): void {
    this.socket.destroy();
  }
}

/** Raw TCP connect + WS handshake; returns the client and the status line. */
async function rawConnect(
  port: number,
): Promise<{ client: WsClient; statusLine: string }> {
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
    'GET /ws HTTP/1.1',
    'Host: 127.0.0.1',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Key: ${key}`,
    'Sec-WebSocket-Version: 13',
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
  return { client: new WsClient(socket), statusLine };
}

async function connect(port: number): Promise<WsClient> {
  const { client, statusLine } = await rawConnect(port);
  assert.match(statusLine, /^HTTP\/1\.1 101/, `expected 101, got: ${statusLine}`);
  return client;
}

async function withGateway(
  bus: EventBus,
  fn: (gw: GatewayServer, port: number) => Promise<void>,
): Promise<void> {
  const gw = new GatewayServer(bus, {});
  const port = await gw.listen();
  try {
    await fn(gw, port);
  } finally {
    await gw.close();
  }
}

test('EB-513: a gateway publish without a priority takes the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultPublishPriority('urgent');
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));
  await withGateway(bus, async (_gw, port) => {
    const client = await connect(port);
    try {
      client.send({ type: 'publish', id: 'p1', topic: 'orders.new', payload: { id: 42 } });
      const reply = await withTimeout(client.next(), 3000, 'published');
      assert.deepEqual(reply, { type: 'published', id: 'p1', delivered: 1, duplicate: false });
      await flush();
      assert.deepEqual(received, [{ id: 42 }]);
      assert.deepEqual(bus.getStats().lanes.delivered, { urgent: 1, normal: 0, bulk: 0 });
    } finally {
      client.destroy();
    }
  });
});
