/**
 * Gateway handshake admission live revision (EB-390).
 *
 * `GatewayServer.setGatewayHandshake` and
 * `GraphqlGateway.setGraphqlGatewayHandshake` retune
 * `handshakeTimeoutMs` (default 10_000ms) and `maxHandshakeBytes`
 * (default 8 KiB) live — without restarting the server. Before
 * EB-390 both were module constants, not even construction options,
 * so the only way to admit a slow client behind a TLS-terminating
 * load balancer, or a multi-cookie handshake head, was to restart
 * the gateway and drop every live connection.
 *
 * Only the fields provided move; an illegal value fails closed with
 * `RangeError` and the live bounds untouched; an empty or identical
 * revision is a no-op. Both bounds govern from the next connection:
 * the handshake timer is armed and the head cap captured once, at
 * accept — an in-flight handshake keeps the values it started with.
 * There is no audit event for this revision (the numeric-tuning
 * convention: `setGatewayLimits` / `setGraphqlGatewayLimits`); the
 * live values are observable in `getStats().limits`. The
 * `connection_init` timeout is untouched (EB-349).
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

function buildHead(extraHeaders: string[] = []): string {
  const key = randomBytes(16).toString('base64');
  return [
    'GET /ws HTTP/1.1',
    'Host: 127.0.0.1',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Key: ${key}`,
    'Sec-WebSocket-Version: 13',
    ...extraHeaders,
    '',
    '',
  ].join('\r\n');
}

/** A ~615-byte multi-cookie head: over a 256-byte cap, under 4 KiB. */
function buildMultiCookieHead(protocol = false): string {
  const cookies = [0, 1, 2].map(
    (i) => `Cookie: session_${i}=${'v'.repeat(130)}; trace_${i}=${'t'.repeat(40)}`,
  );
  const head = buildHead([
    ...cookies,
    ...(protocol ? ['Sec-WebSocket-Protocol: graphql-ws'] : []),
  ]);
  assert.ok(head.length > 256 && head.length < 4096, `head is ${head.length} bytes`);
  return head;
}

async function tcpSocket(port: number): Promise<Socket> {
  const socket = tcpConnect(port, '127.0.0.1');
  socket.on('error', () => {});
  await withTimeout(
    new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve);
      socket.once('error', reject);
    }),
    3000,
    'tcp connect',
  );
  return socket;
}

/**
 * Resolves with the HTTP status line once the head arrives, or `null`
 * if the socket closes first (the server drops over-cap / timed-out
 * handshakes silently, with no HTTP answer).
 */
function waitForHead(socket: Socket, timeoutMs = 3000): Promise<string | null> {
  return withTimeout(
    new Promise<string | null>((resolve) => {
      let buf = Buffer.alloc(0);
      let done = false;
      const finish = (v: string | null) => {
        if (done) return;
        done = true;
        socket.off('data', onData);
        socket.off('close', onClose);
        resolve(v);
      };
      const onData = (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        const end = buf.indexOf('\r\n\r\n');
        if (end !== -1) finish(buf.subarray(0, end).toString('latin1').split('\r\n')[0] ?? '');
      };
      const onClose = () => finish(null);
      socket.on('data', onData);
      socket.on('close', onClose);
    }),
    timeoutMs,
    'handshake response',
  );
}

/** Resolves true when the socket closes within `timeoutMs`, else false. */
function waitClosed(socket: Socket, timeoutMs = 3000): Promise<boolean> {
  if (socket.destroyed) return Promise.resolve(true);
  return new Promise<boolean>((resolve) => {
    const timer = setTimeout(() => {
      socket.off('close', onClose);
      resolve(false);
    }, timeoutMs);
    if (typeof timer === 'object' && 'unref' in timer) (timer as { unref(): void }).unref();
    const onClose = () => {
      clearTimeout(timer);
      resolve(true);
    };
    socket.once('close', onClose);
  });
}

/** Resolves once `needle` arrives on the socket (post-handshake frames). */
function waitForBytes(socket: Socket, needle: string, timeoutMs = 3000): Promise<void> {
  return withTimeout(
    new Promise<void>((resolve, reject) => {
      let buf = Buffer.alloc(0);
      const onData = (chunk: Buffer) => {
        buf = Buffer.concat([buf, chunk]);
        if (buf.includes(needle)) {
          socket.off('data', onData);
          socket.off('close', onClose);
          resolve();
        }
      };
      const onClose = () => {
        socket.off('data', onData);
        reject(new Error(`socket closed before ${needle} arrived`));
      };
      socket.on('data', onData);
      socket.once('close', onClose);
    }),
    timeoutMs,
    needle,
  );
}

const DEFAULT_HANDSHAKE = { handshakeTimeoutMs: 10_000, maxHandshakeBytes: 8 * 1024 };

test('setGatewayHandshake rejects illegal values and leaves the live bounds untouched', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  assert.deepEqual(gw.getStats().limits, {
    maxFrameBytes: 1024 * 1024,
    maxPendingBytes: 1024 * 1024,
    sendHighWaterMarkBytes: 256 * 1024,
    ...DEFAULT_HANDSHAKE,
  });
  const bad: Array<unknown> = [
    // Timeout: positive finite (a fractional timeout is valid).
    { handshakeTimeoutMs: 0 },
    { handshakeTimeoutMs: -1 },
    { handshakeTimeoutMs: Number.NaN },
    { handshakeTimeoutMs: Infinity },
    { handshakeTimeoutMs: '5000' },
    // Head cap: positive integer.
    { maxHandshakeBytes: 0 },
    { maxHandshakeBytes: -1 },
    { maxHandshakeBytes: 1.5 },
    { maxHandshakeBytes: Number.NaN },
    { maxHandshakeBytes: Infinity },
    { maxHandshakeBytes: '8192' },
    // One bad field poisons the whole revision, even with a good one.
    { handshakeTimeoutMs: 5000, maxHandshakeBytes: 0 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => gw.setGatewayHandshake(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    assert.deepEqual(gw.getStats().limits.handshakeTimeoutMs, DEFAULT_HANDSHAKE.handshakeTimeoutMs);
    assert.deepEqual(gw.getStats().limits.maxHandshakeBytes, DEFAULT_HANDSHAKE.maxHandshakeBytes);
  }
});

test('partial revision moves only the fields provided; getters follow the live values', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  gw.setGatewayHandshake({ handshakeTimeoutMs: 250 });
  assert.deepEqual(gw.getStats().limits, {
    maxFrameBytes: 1024 * 1024,
    maxPendingBytes: 1024 * 1024,
    sendHighWaterMarkBytes: 256 * 1024,
    handshakeTimeoutMs: 250,
    maxHandshakeBytes: 8 * 1024,
  });
  gw.setGatewayHandshake({ maxHandshakeBytes: 4096 });
  assert.equal(gw.getStats().limits.handshakeTimeoutMs, 250);
  assert.equal(gw.getStats().limits.maxHandshakeBytes, 4096);
  // Explicitly `undefined` is not a value: the field keeps its live value.
  gw.setGatewayHandshake({ handshakeTimeoutMs: undefined, maxHandshakeBytes: 1024 });
  assert.equal(gw.getStats().limits.handshakeTimeoutMs, 250);
  assert.equal(gw.getStats().limits.maxHandshakeBytes, 1024);
});

test('identical, empty and absent handshake revisions are no-ops', () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, { handshakeTimeoutMs: 250, maxHandshakeBytes: 4096 });
  assert.equal(gw.getStats().limits.handshakeTimeoutMs, 250);
  assert.equal(gw.getStats().limits.maxHandshakeBytes, 4096);
  gw.setGatewayHandshake({});
  gw.setGatewayHandshake({ handshakeTimeoutMs: 250 });
  gw.setGatewayHandshake({ handshakeTimeoutMs: 250, maxHandshakeBytes: 4096 });
  gw.setGatewayHandshake(null as never);
  gw.setGatewayHandshake(undefined as never);
  assert.equal(gw.getStats().limits.handshakeTimeoutMs, 250);
  assert.equal(gw.getStats().limits.maxHandshakeBytes, 4096);
});

test('tightened timeout disconnects a slow client', async () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  const port = await gw.listen();
  try {
    gw.setGatewayHandshake({ handshakeTimeoutMs: 120 });
    const socket = await tcpSocket(port);
    try {
      // No handshake bytes at all: the armed 120ms timer must drop it.
      assert.equal(await waitClosed(socket, 3000), true, 'expected the socket to close');
    } finally {
      socket.destroy();
    }
  } finally {
    await gw.close();
  }
});

test('relaxed timeout lets a slow client handshake succeed', async () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  const port = await gw.listen();
  try {
    // Tight first: a client that takes 400ms to send its head is dropped.
    gw.setGatewayHandshake({ handshakeTimeoutMs: 120 });
    const slow1 = await tcpSocket(port);
    try {
      const head1 = waitForHead(slow1);
      await sleep(400);
      slow1.write(buildHead());
      assert.equal(await head1, null, 'expected no 101 under the tight timeout');
    } finally {
      slow1.destroy();
    }

    // Relaxed: the same slow client completes the handshake, and the
    // connection is usable afterwards.
    gw.setGatewayHandshake({ handshakeTimeoutMs: 5000 });
    const slow2 = await tcpSocket(port);
    try {
      const head2 = waitForHead(slow2);
      await sleep(400);
      slow2.write(buildHead());
      assert.match((await head2) ?? '', /^HTTP\/1\.1 101/);
      slow2.write(
        encodeClientFrame(Buffer.from(JSON.stringify({ type: 'subscribe', id: 's1', pattern: 't' }), 'utf8')),
      );
      await waitForBytes(slow2, '"subscribed"');
    } finally {
      slow2.destroy();
    }
  } finally {
    await gw.close();
  }
});

test('tightened header cap disconnects an over-limit handshake', async () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  const port = await gw.listen();
  try {
    gw.setGatewayHandshake({ maxHandshakeBytes: 256 });
    const socket = await tcpSocket(port);
    try {
      const head = waitForHead(socket);
      socket.write(buildMultiCookieHead());
      assert.equal(await head, null, 'expected no 101 under the tight cap');
      assert.equal(await waitClosed(socket, 1000), true);
    } finally {
      socket.destroy();
    }
  } finally {
    await gw.close();
  }
});

test('relaxed header cap lets a multi-cookie handshake pass', async () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  const port = await gw.listen();
  try {
    // Tight first: the same head is dropped.
    gw.setGatewayHandshake({ maxHandshakeBytes: 256 });
    const tight = await tcpSocket(port);
    try {
      const head = waitForHead(tight);
      tight.write(buildMultiCookieHead());
      assert.equal(await head, null, 'expected no 101 under the tight cap');
    } finally {
      tight.destroy();
    }

    // Relaxed: the same head handshakes, and the connection is usable.
    gw.setGatewayHandshake({ maxHandshakeBytes: 4096 });
    const relaxed = await tcpSocket(port);
    try {
      const head = waitForHead(relaxed);
      relaxed.write(buildMultiCookieHead());
      assert.match((await head) ?? '', /^HTTP\/1\.1 101/);
      relaxed.write(
        encodeClientFrame(Buffer.from(JSON.stringify({ type: 'subscribe', id: 's1', pattern: 't' }), 'utf8')),
      );
      await waitForBytes(relaxed, '"subscribed"');
    } finally {
      relaxed.destroy();
    }
  } finally {
    await gw.close();
  }
});

test('in-flight handshakes keep the bounds they started with', async () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  const port = await gw.listen();
  try {
    // Timeout: a socket accepted under a 150ms timeout keeps it even
    // after the live value is relaxed to 10s.
    gw.setGatewayHandshake({ handshakeTimeoutMs: 150 });
    const armed = await tcpSocket(port);
    try {
      gw.setGatewayHandshake({ handshakeTimeoutMs: 10_000 });
      assert.equal(
        await waitClosed(armed, 3000),
        true,
        'expected the armed socket to close on its original timeout',
      );
    } finally {
      armed.destroy();
    }
    // ...while a connection accepted after the revision gets the new
    // timeout: a 400ms-delayed handshake succeeds.
    const fresh = await tcpSocket(port);
    try {
      const head = waitForHead(fresh);
      await sleep(400);
      fresh.write(buildHead());
      assert.match((await head) ?? '', /^HTTP\/1\.1 101/);
    } finally {
      fresh.destroy();
    }

    // Head cap: a socket that already buffered 300 bytes under the
    // 8 KiB cap keeps it after the live cap is tightened to 100 —
    // one more byte must not drop it, and the head must still 101.
    const partial = await tcpSocket(port);
    try {
      const head = buildHead();
      partial.write(head.slice(0, 300));
      await sleep(50);
      gw.setGatewayHandshake({ maxHandshakeBytes: 100 });
      partial.write(head.slice(300, 301));
      await sleep(150);
      assert.equal(
        await waitClosed(partial, 50),
        false,
        'in-flight head must survive the tightened cap',
      );
      const status = waitForHead(partial);
      partial.write(head.slice(301));
      assert.match((await status) ?? '', /^HTTP\/1\.1 101/);
    } finally {
      partial.destroy();
    }
  } finally {
    await gw.close();
  }
});

test('GraphQL gateway mirrors the handshake revision: validation, partial moves, no-ops', () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0 });
  assert.deepEqual(gw.getStats().limits, {
    maxConnections: undefined,
    connectionInitTimeoutMs: 5000,
    maxFrameBytes: 1024 * 1024,
    ...DEFAULT_HANDSHAKE,
    maxSubscriptionsPerConnection: undefined,
  });
  const bad: Array<unknown> = [
    { handshakeTimeoutMs: 0 },
    { handshakeTimeoutMs: -1 },
    { handshakeTimeoutMs: Number.NaN },
    { handshakeTimeoutMs: Infinity },
    { handshakeTimeoutMs: '5000' },
    { maxHandshakeBytes: 0 },
    { maxHandshakeBytes: -1 },
    { maxHandshakeBytes: 1.5 },
    { maxHandshakeBytes: Number.NaN },
    { maxHandshakeBytes: Infinity },
    { maxHandshakeBytes: '8192' },
    { handshakeTimeoutMs: 5000, maxHandshakeBytes: 0 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => gw.setGraphqlGatewayHandshake(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
    assert.equal(gw.getStats().limits.handshakeTimeoutMs, DEFAULT_HANDSHAKE.handshakeTimeoutMs);
    assert.equal(gw.getStats().limits.maxHandshakeBytes, DEFAULT_HANDSHAKE.maxHandshakeBytes);
  }
  // Partial revision moves only the field provided; `undefined` keeps.
  gw.setGraphqlGatewayHandshake({ handshakeTimeoutMs: 250 });
  assert.equal(gw.getStats().limits.handshakeTimeoutMs, 250);
  assert.equal(gw.getStats().limits.maxHandshakeBytes, 8 * 1024);
  gw.setGraphqlGatewayHandshake({ maxHandshakeBytes: undefined, handshakeTimeoutMs: 300 });
  assert.equal(gw.getStats().limits.handshakeTimeoutMs, 300);
  assert.equal(gw.getStats().limits.maxHandshakeBytes, 8 * 1024);
  // Identical, empty and absent revisions are no-ops.
  gw.setGraphqlGatewayHandshake({});
  gw.setGraphqlGatewayHandshake({ handshakeTimeoutMs: 300, maxHandshakeBytes: 8 * 1024 });
  gw.setGraphqlGatewayHandshake(null as never);
  gw.setGraphqlGatewayHandshake(undefined as never);
  assert.equal(gw.getStats().limits.handshakeTimeoutMs, 300);
  assert.equal(gw.getStats().limits.maxHandshakeBytes, 8 * 1024);
});

test('GraphQL tightened timeout disconnects a slow client; relaxed timeout admits one', async () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0 });
  await gw.start();
  const port = gw.getPort();
  try {
    gw.setGraphqlGatewayHandshake({ handshakeTimeoutMs: 120 });
    const slow1 = await tcpSocket(port);
    try {
      const head1 = waitForHead(slow1);
      await sleep(400);
      slow1.write(buildHead(['Sec-WebSocket-Protocol: graphql-ws']));
      assert.equal(await head1, null, 'expected no 101 under the tight timeout');
    } finally {
      slow1.destroy();
    }

    gw.setGraphqlGatewayHandshake({ handshakeTimeoutMs: 5000 });
    const slow2 = await tcpSocket(port);
    try {
      const head2 = waitForHead(slow2);
      await sleep(400);
      slow2.write(buildHead(['Sec-WebSocket-Protocol: graphql-ws']));
      assert.match((await head2) ?? '', /^HTTP\/1\.1 101/);
      // The connection is usable: connection_init is acked.
      slow2.write(
        encodeClientFrame(Buffer.from(JSON.stringify({ type: 'connection_init', payload: {} }), 'utf8')),
      );
      await waitForBytes(slow2, 'connection_ack');
    } finally {
      slow2.destroy();
    }
  } finally {
    await gw.stop();
  }
});

test('GraphQL header cap: tightened disconnects, relaxed admits a multi-cookie handshake', async () => {
  const bus = new EventBus();
  const gw = new GraphqlGateway(bus, { port: 0 });
  await gw.start();
  const port = gw.getPort();
  try {
    gw.setGraphqlGatewayHandshake({ maxHandshakeBytes: 256 });
    const tight = await tcpSocket(port);
    try {
      const head = waitForHead(tight);
      tight.write(buildMultiCookieHead(true));
      assert.equal(await head, null, 'expected no 101 under the tight cap');
      assert.equal(await waitClosed(tight, 1000), true);
    } finally {
      tight.destroy();
    }

    gw.setGraphqlGatewayHandshake({ maxHandshakeBytes: 4096 });
    const relaxed = await tcpSocket(port);
    try {
      const head = waitForHead(relaxed);
      relaxed.write(buildMultiCookieHead(true));
      assert.match((await head) ?? '', /^HTTP\/1\.1 101/);
      relaxed.write(
        encodeClientFrame(Buffer.from(JSON.stringify({ type: 'connection_init', payload: {} }), 'utf8')),
      );
      await waitForBytes(relaxed, 'connection_ack');
    } finally {
      relaxed.destroy();
    }
  } finally {
    await gw.stop();
  }
});
