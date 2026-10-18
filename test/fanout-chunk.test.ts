import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * Per-subscriber queue depths in subscription order, read synchronously —
 * the observable for "this chunk ran inside `publish`, not on a later
 * microtask". Test-only introspection (mirrors `getStats().shadows[].queueSize`).
 */
const queueSizes = (bus: EventBus): number[] =>
  [...(bus as unknown as { subscribers: Map<string, { queue: { size: number } }> }).subscribers.values()].map(
    (s) => s.queue.size,
  );

test('chunked fan-out: first chunk synchronously, rest on microtasks, order preserved', async () => {
  const bus = new EventBus({ fanOutChunkSize: 2 });
  const seen: number[] = [];
  for (let i = 0; i < 5; i++) {
    const idx = i;
    bus.subscribe('t', () => {
      seen.push(idx);
    });
  }
  // Chunking engages: publish returns the MATCHED count synchronously.
  const n = bus.publish('t', 'x');
  assert.equal(n, 5);
  // First chunk (2 deliveries) ran inside publish; the rest are pending.
  assert.deepEqual(queueSizes(bus), [1, 1, 0, 0, 0]);
  assert.deepEqual(seen, []);
  await flush();
  // All five delivered, in subscription order — the chunked path replays
  // the scan order exactly.
  assert.deepEqual(seen, [0, 1, 2, 3, 4]);
  assert.deepEqual(queueSizes(bus), [0, 0, 0, 0, 0]);
});

test('fanOutYields counts one per scheduled microtask step', async () => {
  const bus = new EventBus({ fanOutChunkSize: 2 });
  for (let i = 0; i < 5; i++) bus.subscribe('t', () => {});
  bus.publish('t', 'x');
  await flush();
  // 5 tasks, chunk size 2: chunk 1 sync, then 2 chained microtask steps.
  assert.equal(bus.getStats().fanOutYields, 2);
  // A second publish yields again.
  bus.publish('t', 'y');
  await flush();
  assert.equal(bus.getStats().fanOutYields, 4);
});

test('default bus stays fully synchronous with zero yields', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  for (let i = 0; i < 5; i++) {
    const idx = i;
    bus.subscribe('t', () => {
      seen.push(idx);
    });
  }
  const n = bus.publish('t', 'x');
  assert.equal(n, 5);
  // No chunking: all five queues filled synchronously, zero microtask yields.
  assert.deepEqual(queueSizes(bus), [1, 1, 1, 1, 1]);
  assert.equal(bus.getStats().fanOutYields, 0);
  await flush();
  assert.deepEqual(seen, [0, 1, 2, 3, 4]);
});

test('invalid fanOutChunkSize throws RangeError at construction', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, 'x']) {
    assert.throws(() => new EventBus({ fanOutChunkSize: bad as number }), RangeError, String(bad));
  }
  // Absent, Infinity (the spelled-out default), and positive integers are fine.
  assert.doesNotThrow(() => new EventBus());
  assert.doesNotThrow(() => new EventBus({ fanOutChunkSize: Infinity }));
  assert.doesNotThrow(() => new EventBus({ fanOutChunkSize: 1 }));
  assert.doesNotThrow(() => new EventBus({ fanOutChunkSize: 10000 }));
});

test('consumer group on a chunked topic still gets exactly one copy', async () => {
  const bus = new EventBus({ fanOutChunkSize: 2 });
  const plain: number[] = [];
  const members: number[][] = [[], [], []];
  bus.subscribe('t', () => {
    plain.push(1);
  });
  bus.subscribe('t', () => {
    plain.push(2);
  });
  for (let i = 0; i < 3; i++) {
    const idx = i;
    bus.subscribeToGroup('workers', 't', () => {
      members[idx].push(1);
    });
  }
  // 2 plain deliveries + 1 group competing set = 3 tasks > chunk size 2.
  const n = bus.publish('t', 'x');
  assert.equal(n, 3);
  await flush();
  assert.deepEqual(plain, [1, 2]);
  // The group's single copy went to exactly one member.
  assert.equal(
    members.reduce((sum, m) => sum + m.length, 0),
    1,
  );
  assert.equal(bus.getStats().fanOutYields, 1);
});

test('chunkSize=1 engages on every multi-subscriber publish', async () => {
  const bus = new EventBus({ fanOutChunkSize: 1 });
  const seen: number[] = [];
  for (let i = 0; i < 3; i++) {
    const idx = i;
    bus.subscribe('t', () => {
      seen.push(idx);
    });
  }
  const n = bus.publish('t', 'x');
  assert.equal(n, 3);
  assert.deepEqual(queueSizes(bus), [1, 0, 0]);
  await flush();
  assert.deepEqual(seen, [0, 1, 2]);
  // 3 tasks, chunk size 1: chunk 1 sync, then 2 chained microtask steps.
  assert.equal(bus.getStats().fanOutYields, 2);
});

test('matched == chunkSize exactly does not chunk', async () => {
  const bus = new EventBus({ fanOutChunkSize: 3 });
  const seen: number[] = [];
  for (let i = 0; i < 3; i++) {
    const idx = i;
    bus.subscribe('t', () => {
      seen.push(idx);
    });
  }
  const n = bus.publish('t', 'x');
  assert.equal(n, 3);
  // At (not over) the chunk size: fully synchronous, no yields.
  assert.deepEqual(queueSizes(bus), [1, 1, 1]);
  assert.equal(bus.getStats().fanOutYields, 0);
  await flush();
  assert.deepEqual(seen, [0, 1, 2]);
});

test('barrier control messages bypass chunking and stay fully synchronous', async () => {
  const bus = new EventBus({ fanOutChunkSize: 1 });
  const received: unknown[] = [];
  for (let i = 0; i < 3; i++) bus.subscribe('t', (m) => received.push(m.payload), { barrier: true });
  // A barrier is control-plane: it must cut in strict publish order, so it
  // never rides the chunked path even when the chunk size would engage.
  const accepted = bus.publish('t', { cut: 'cfg-v2' }, { barrier: { epoch: 1 } });
  assert.equal(accepted, 3);
  assert.deepEqual(queueSizes(bus), [1, 1, 1]);
  assert.equal(bus.getStats().fanOutYields, 0);
  await flush();
  assert.equal(received.length, 3);
});
