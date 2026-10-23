import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-replay-'));

test('replayRange: replays history in order with the replayed marker', async () => {
  const bus = new EventBus();
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  bus.publish('t', { n: 3 });
  const received: Array<{ seq: number; replayed: boolean; payload: unknown }> = [];
  // Subscribe after the publishes so the live stream carries nothing;
  // everything the handler sees must come from the replay.
  bus.subscribe('t', (msg) => received.push({ seq: msg.seq, replayed: msg.replayed === true, payload: msg.payload }));
  const report = await bus.replayRange('t', { from: 1, to: 3, speed: 1000 });
  assert.equal(report.requested, 3);
  assert.equal(report.subscriberCount, 1);
  assert.equal(report.delivered, 3);
  assert.equal(report.handlerErrors, 0);
  assert.deepEqual(
    received.map((r) => [r.seq, r.replayed]),
    [
      [1, true],
      [2, true],
      [3, true],
    ],
  );
  assert.deepEqual(
    received.map((r) => r.payload),
    [{ n: 1 }, { n: 2 }, { n: 3 }],
  );
});

test('replayRange: respects from/to bounds', async () => {
  const bus = new EventBus();
  for (let i = 1; i <= 5; i++) bus.publish('t', { n: i });
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  const report = await bus.replayRange('t', { from: 2, to: 4, speed: 1000 });
  assert.equal(report.requested, 3);
  assert.deepEqual(seqs, [2, 3, 4]);
});

test('replayRange: replayed messages do not pollute sequence-gap stats', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  await bus.replayRange('t', { from: 1, to: 2, speed: 1000 });
  bus.publish('t', 'c');
  await flush();
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('replayRange: catch-up mode pauses live delivery and resumes seamlessly', async () => {
  const bus = new EventBus();
  bus.publish('t', 'h1');
  await new Promise((r) => setTimeout(r, 30));
  bus.publish('t', 'h2');
  const order: string[] = [];
  bus.subscribe('t', (msg: BusMessage) => order.push(msg.replayed === true ? `R${msg.seq}` : `L${msg.seq}`), {
    catchUpOnReplay: true,
  });
  await flush();
  // The 30ms original gap at speed 3 takes ~10ms: publish live right in
  // the middle of the replay.
  const replay = bus.replayRange('t', { from: 1, to: 2, speed: 3 });
  await new Promise((r) => setTimeout(r, 4));
  bus.publish('t', 'live');
  await replay;
  await flush();
  await flush();
  // History first, the live message only after the replay finished.
  assert.deepEqual(order, ['R1', 'R2', 'L3']);
});

test('replayRange: no history is a clean no-op', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const report = await bus.replayRange('t', { from: 1, to: 10, speed: 10 });
  assert.equal(report.requested, 0);
  assert.equal(report.delivered, 0);
});

test('replayRange: targets specific subscriptions by id', async () => {
  const bus = new EventBus();
  bus.publish('t', 'x');
  const a: unknown[] = [];
  const b: unknown[] = [];
  const subA = bus.subscribe('t', (msg) => a.push(msg.payload));
  bus.subscribe('t', (msg) => b.push(msg.payload));
  const report = await bus.replayRange('t', { from: 1, to: 1, speed: 1000 }, { subscribers: [subA.id] });
  assert.equal(report.subscriberCount, 1);
  assert.equal(report.delivered, 1);
  assert.deepEqual(a, ['x']);
  assert.deepEqual(b, []);
});

test('replayRange: throwing handlers are counted, not fatal', async () => {
  const bus = new EventBus();
  bus.publish('t', 1);
  bus.publish('t', 2);
  const good: unknown[] = [];
  let calls = 0;
  bus.subscribe('t', (msg) => {
    calls += 1;
    if (calls === 1) throw new Error('boom');
    good.push(msg.payload);
  });
  const report = await bus.replayRange('t', { from: 1, to: 2, speed: 1000 });
  assert.equal(report.handlerErrors, 1);
  assert.equal(report.delivered, 1);
  assert.deepEqual(good, [2]);
});

test('replayRange: reads from the durable log when configured', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  const seen: Array<{ payload: unknown; replayed: boolean }> = [];
  bus.subscribe('t', (msg) => seen.push({ payload: msg.payload, replayed: msg.replayed === true }));
  const report = await bus.replayRange('t', { from: 1, to: 2, speed: 1000 });
  assert.equal(report.requested, 2);
  assert.deepEqual(
    seen.map((s) => [s.payload, s.replayed]),
    [
      [{ n: 1 }, true],
      [{ n: 2 }, true],
    ],
  );
});

test('replayRange: validation fails fast', async () => {
  const bus = new EventBus();
  await assert.rejects(() => bus.replayRange('', { speed: 1 }), RangeError);
  await assert.rejects(() => bus.replayRange('t', { from: 0, speed: 1 }), RangeError);
  await assert.rejects(() => bus.replayRange('t', { from: 3, to: 2, speed: 1 }), RangeError);
  await assert.rejects(() => bus.replayRange('t', { speed: 0 }), RangeError);
  await assert.rejects(() => bus.replayRange('t', { speed: -1 }), RangeError);
  await assert.rejects(() => bus.replayRange('t', { from: 1 }, { subscribers: ['nope'] }), RangeError);
  await assert.rejects(() => bus.replayRange('t', { from: 1 }, { subscribers: [] }), RangeError);
});

test('replayRange: concurrent replays on one topic fail fast', async () => {
  const bus = new EventBus();
  bus.publish('t', 'a');
  await new Promise((r) => setTimeout(r, 20));
  bus.publish('t', 'b');
  bus.subscribe('t', () => {});
  // The 20ms gap at speed 2 keeps the first replay in flight.
  const first = bus.replayRange('t', { from: 1, to: 2, speed: 2 });
  await assert.rejects(() => bus.replayRange('t', { from: 1, to: 2, speed: 1000 }), RangeError);
  await first;
});

test('subscribe: catchUpOnReplay must be a boolean', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('t', () => {}, { catchUpOnReplay: 'yes' as unknown as boolean }),
    RangeError,
  );
  // And the option is accepted when valid.
  bus.subscribe('t', () => {}, { catchUpOnReplay: true }).unsubscribe();
});
