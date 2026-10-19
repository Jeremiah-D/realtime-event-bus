/**
 * Graceful shutdown (EB-154): `EventBus.close({ drainTimeoutMs })`.
 *
 * - Subscriber queues drain and in-flight reliable deliveries settle
 *   before the promise resolves.
 * - After close every publish/subscribe-family entry point throws
 *   `BusClosedError` synchronously — never a silent queue.
 * - `close()` is idempotent: a second call resolves with the same result.
 * - Past `drainTimeoutMs` the close is forced and the uncompleted counts
 *   are reported.
 * - The durable log is flushed to stable storage before resources release.
 * - Attached WebSocket gateways close orderly (connections drop, the
 *   listener stops).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { connect as tcpConnect } from 'node:net';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, BusClosedError } from '../src/bus.ts';
import { GatewayServer } from '../src/gateway-ws.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
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

function assertBusClosed(fn: () => unknown, action: string): void {
  assert.throws(fn, (err: unknown) => {
    assert.ok(err instanceof BusClosedError, `expected BusClosedError, got ${String(err)}`);
    assert.equal((err as Error).name, 'BusClosedError');
    assert.equal((err as BusClosedError).action, action);
    assert.equal((err as BusClosedError).code, 'BUS_CLOSED');
    return true;
  });
}

test('close drains plain-subscriber queues before resolving', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribe('orders.**', (msg) => {
    seen.push(msg.payload as number);
  });
  for (let i = 0; i < 50; i++) bus.publish('orders.created', i);
  const result = await withTimeout(bus.close(), 5000, 'close');
  assert.equal(result.drained, true);
  assert.equal(result.pendingMessages, 0);
  assert.equal(result.unackedDeliveries, 0);
  assert.equal(result.cancelledDelayed, 0);
  assert.equal(result.cancelledRecurring, 0);
  // Every queued message reached the handler before close resolved.
  assert.equal(seen.length, 50);
  assert.deepEqual(seen, [...Array(50).keys()]);
});

test('close waits for slow reliable handlers: in-flight deliveries settle first', async () => {
  const bus = new EventBus();
  const acked: number[] = [];
  let closeResolvedAt = 0;
  bus.subscribeReliable(
    'jobs.**',
    (delivery) => {
      const n = delivery.msg.payload as number;
      // Slow consumer: the ack lands one macrotask later, so the drain
      // must wait out the in-flight deliveries instead of resolving
      // with the queue merely empty.
      setTimeout(() => {
        delivery.ack();
        acked.push(n);
      }, 20);
    },
    { ackTimeoutMs: 60_000 },
  );
  for (let i = 0; i < 5; i++) bus.publish('jobs.run', i);
  const result = await withTimeout(
    bus.close({ drainTimeoutMs: 10_000 }).then((r) => {
      closeResolvedAt = Date.now();
      return r;
    }),
    15_000,
    'close',
  );
  assert.equal(result.drained, true);
  assert.equal(result.unackedDeliveries, 0);
  assert.deepEqual(acked.sort((a, b) => a - b), [0, 1, 2, 3, 4]);
  assert.ok(closeResolvedAt > 0, 'close resolved');
});

test('close is idempotent: a second call resolves with the same result', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 1);
  const first = await withTimeout(bus.close(), 5000, 'first close');
  assert.equal(first.drained, true);
  const second = await withTimeout(bus.close(), 1000, 'second close');
  // Same promise, so the same result object — nothing re-ran.
  assert.equal(second, first);
});

test('close validates drainTimeoutMs before closing anything', async () => {
  const bus = new EventBus();
  assert.throws(() => bus.close({ drainTimeoutMs: -1 }), RangeError);
  assert.throws(() => bus.close({ drainTimeoutMs: Number.NaN }), RangeError);
  // The failed validation never half-closed the bus: it still admits work.
  bus.publish('t', 1);
  const result = await withTimeout(bus.close(), 5000, 'close');
  assert.equal(result.drained, true);
});

test('close adopts a subscriber already mid-drain instead of double-draining', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribe('t', (msg) => {
    seen.push(msg.payload as number);
  });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  // A user-initiated graceful unsubscribe-drain is already in flight when
  // close() runs: the close must adopt it (one drain, one teardown), not
  // orphan its timer with a second startDrain.
  sub.unsubscribe({ drain: true, drainTimeoutMs: 10_000 });
  const result = await withTimeout(bus.close({ drainTimeoutMs: 5000 }), 8000, 'close');
  assert.equal(result.drained, true);
  assert.equal(seen.length, 5);
});

test('drain timeout forces the close and reports uncompleted counts', async () => {
  const bus = new EventBus();
  const deliveries: Array<{ ack(): void; nack(): void }> = [];
  bus.subscribeReliable(
    'stuck.**',
    (delivery) => {
      deliveries.push(delivery);
      // Never settles: the drain must time out instead of hanging.
    },
    { ackTimeoutMs: 600_000 },
  );
  for (let i = 0; i < 3; i++) bus.publish('stuck.job', i);
  await flush();
  assert.equal(deliveries.length, 3);
  const result = await withTimeout(bus.close({ drainTimeoutMs: 80 }), 5000, 'close');
  assert.equal(result.drained, false);
  assert.equal(result.unackedDeliveries, 3);
  assert.equal(result.pendingMessages, 0);
  // Post-close ack/nack on orphaned handles is a silent no-op: the forced
  // teardown cleared the ack tracker without requeueing, so nothing is
  // delivered again and nothing throws.
  deliveries[0].ack();
  deliveries[1].nack();
  await sleep(50);
  assert.equal(deliveries.length, 3);
});

test('publish and subscribe entry points throw BusClosedError after close', async () => {
  const bus = new EventBus();
  await withTimeout(bus.close(), 5000, 'close');
  assertBusClosed(() => bus.publish('t', {}), 'publish');
  assertBusClosed(() => bus.publishWithConfirmation('t', {}), 'publish');
  assertBusClosed(() => bus.publishIdempotent('t', {}, { messageId: 'm1' }), 'publish');
  assertBusClosed(() => bus.publishDedupedByContent('t', {}, { messageId: 'm2' }), 'publish');
  assertBusClosed(() => bus.publishBatch([{ topic: 't', payload: {} }]), 'publish');
  assertBusClosed(() => bus.publishAtomic([{ topic: 't', payload: {} }]), 'publish');
  assertBusClosed(() => bus.publishDelayed('t', {}, { delayMs: 1000 }), 'publish');
  assertBusClosed(() => bus.subscribe('t', () => {}), 'subscribe');
  assertBusClosed(() => bus.subscribeMany([{ pattern: 't', handler: () => {} }]), 'subscribe');
  assertBusClosed(
    () =>
      bus.subscribeReliable('t', (d) => {
        d.ack();
      }),
    'subscribe',
  );
  assertBusClosed(() => bus.subscribeToGroup('g', 't', () => {}), 'subscribe');
  assertBusClosed(() => bus.scheduleRecurring('t', () => ({}), '* * * * *'), 'schedule');
});

test('pending delayed deliveries are cancelled at close, never published', async () => {
  const bus = new EventBus();
  let delivered = 0;
  bus.subscribe('later.**', () => {
    delivered += 1;
  });
  const id = bus.publishDelayed('later.event', { n: 1 }, { delayMs: 60_000 });
  assert.ok(typeof id === 'string' && id.length > 0);
  const result = await withTimeout(bus.close(), 5000, 'close');
  assert.equal(result.cancelledDelayed, 1);
  // The schedule never fans out after close — not even if the clock runs on.
  await sleep(20);
  assert.equal(delivered, 0);
});

test('recurring schedules are cancelled at close', async () => {
  const bus = new EventBus();
  const handle = bus.scheduleRecurring('cron.event', () => ({ tick: 1 }), '* * * * *');
  assert.ok(handle.id.startsWith('recurring-'));
  const result = await withTimeout(bus.close(), 5000, 'close');
  assert.equal(result.cancelledRecurring, 1);
});

test('durable log is flushed to stable storage before resources release', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb154-'));
  const bus = new EventBus({ durableLogDir: dir });
  const seen: unknown[] = [];
  bus.subscribe('persist.**', (msg) => {
    seen.push(msg.payload);
  });
  for (let i = 0; i < 10; i++) bus.publish('persist.event', { i });
  const result = await withTimeout(bus.close(), 5000, 'close');
  assert.equal(result.drained, true);
  assert.equal(seen.length, 10);
  // The log file on disk holds every record after close — reopen it
  // directly and read the topic back.
  const log = DurableTopicLog.open({ dir });
  const records = log.readSince('persist.event', 0).filter((r) => r.seq >= 1);
  assert.equal(records.length, 10);
  assert.deepEqual(
    records.map((r) => (r.payload as { i: number }).i),
    [...Array(10).keys()],
  );
});

test('attached gateways close orderly: connections drop and the listener stops', async () => {
  const bus = new EventBus();
  const gw = new GatewayServer(bus, {});
  bus.attachGateway(gw);
  const port = await gw.listen();
  const socket = tcpConnect(port, '127.0.0.1');
  await withTimeout(
    new Promise<void>((resolve, reject) => {
      socket.once('connect', resolve);
      socket.once('error', reject);
    }),
    3000,
    'socket connect',
  );
  let socketClosed = false;
  socket.once('close', () => {
    socketClosed = true;
  });
  await withTimeout(bus.close(), 5000, 'close');
  await withTimeout(
    (async () => {
      while (!socketClosed) await sleep(10);
    })(),
    3000,
    'socket close',
  );
  // The listener is gone: a fresh connection is refused.
  await withTimeout(
    new Promise<void>((resolve, reject) => {
      const probe = tcpConnect(port, '127.0.0.1');
      probe.once('error', () => {
        probe.destroy();
        resolve();
      });
      probe.once('connect', () => {
        probe.destroy();
        reject(new Error('gateway listener still accepting connections after close'));
      });
    }),
    3000,
    'refused connect',
  );
  socket.destroy();
  // The gateway was released by the close.
  assert.equal(bus.detachGateway(gw), false);
});

test('attachGateway after close throws BusClosedError', async () => {
  const bus = new EventBus();
  await withTimeout(bus.close(), 5000, 'close');
  const gw = new GatewayServer(bus, {});
  assertBusClosed(() => bus.attachGateway(gw), 'gateway');
  assert.throws(() => bus.attachGateway(null as never), TypeError);
});
