import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-replay-key-'));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('replayRange key: only records with the matching key are delivered (pull ring)', async () => {
  const bus = new EventBus();
  bus.publish('t', { n: 'a1' }, { key: 'entity-1' });
  bus.publish('t', { n: 'b1' }, { key: 'entity-2' });
  bus.publish('t', { n: 'a2' }, { key: 'entity-1' });
  bus.publish('t', { n: 'plain' });
  bus.publish('t', { n: 'b2' }, { key: 'entity-2' });
  const received: Array<{ seq: number; key?: string; replayed: boolean; payload: unknown }> = [];
  bus.subscribe('t', (msg) =>
    received.push({ seq: msg.seq, key: msg.key, replayed: msg.replayed === true, payload: msg.payload }),
  );
  const report = await bus.replayRange('t', { key: 'entity-1', speed: 1000 });
  assert.equal(report.requested, 5);
  assert.equal(report.matched, 2);
  assert.equal(report.delivered, 2);
  assert.equal(report.handlerErrors, 0);
  assert.deepEqual(
    received.map((r) => [r.seq, r.key, r.replayed]),
    [
      [1, 'entity-1', true],
      [3, 'entity-1', true],
    ],
  );
  assert.deepEqual(
    received.map((r) => r.payload),
    [{ n: 'a1' }, { n: 'a2' }],
  );
});

test('replayRange key: non-keyed records never match, even for another key', async () => {
  const bus = new EventBus();
  bus.publish('t', 'plain-1');
  bus.publish('t', 'b', { key: 'other' });
  bus.publish('t', 'plain-2');
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const report = await bus.replayRange('t', { key: 'entity-1', speed: 1000 });
  assert.equal(report.requested, 3);
  assert.equal(report.matched, 0);
  assert.equal(report.delivered, 0);
  assert.deepEqual(received, []);
});

test('replayRange key: filters durable-log history the same way (EB-36 keySeq persistence)', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.publish('t', { n: 'a1' }, { key: 'entity-1' });
  bus.publish('t', { n: 'b1' }, { key: 'entity-2' });
  bus.publish('t', { n: 'a2' }, { key: 'entity-1' });
  bus.publish('t', { n: 'plain' });
  const received: Array<{ seq: number; key?: string }> = [];
  bus.subscribe('t', (msg) => received.push({ seq: msg.seq, key: msg.key }));
  const report = await bus.replayRange('t', { key: 'entity-1', speed: 1000 });
  assert.equal(report.requested, 4);
  assert.equal(report.matched, 2);
  assert.equal(report.delivered, 2);
  assert.deepEqual(received, [
    { seq: 1, key: 'entity-1' },
    { seq: 3, key: 'entity-1' },
  ]);
});

test('replayRange key: composes with from/to bounds', async () => {
  const bus = new EventBus();
  for (let i = 1; i <= 6; i++) {
    bus.publish('t', { n: i }, { key: i % 2 === 0 ? 'even' : 'odd' });
  }
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  const report = await bus.replayRange('t', { from: 2, to: 5, key: 'even', speed: 1000 });
  // Range [2, 5] holds seqs 2, 3, 4, 5; the even-keyed ones are 2 and 4.
  assert.equal(report.requested, 4);
  assert.equal(report.matched, 2);
  assert.deepEqual(seqs, [2, 4]);
});

test('replayRange key: composes with subscribers targeting', async () => {
  const bus = new EventBus();
  bus.publish('t', 'a1', { key: 'entity-1' });
  bus.publish('t', 'b1', { key: 'entity-2' });
  bus.publish('t', 'a2', { key: 'entity-1' });
  const a: unknown[] = [];
  const b: unknown[] = [];
  const subA = bus.subscribe('t', (msg) => a.push(msg.payload));
  bus.subscribe('t', (msg) => b.push(msg.payload));
  const report = await bus.replayRange('t', { key: 'entity-1', speed: 1000 }, { subscribers: [subA.id] });
  assert.equal(report.subscriberCount, 1);
  assert.equal(report.matched, 2);
  assert.equal(report.delivered, 2);
  assert.deepEqual(a, ['a1', 'a2']);
  assert.deepEqual(b, []);
});

test('replayRange key: composes with namespace', async () => {
  const bus = new EventBus();
  const t1 = bus.createNamespace('t1');
  const t2 = bus.createNamespace('t2');
  t1.publish('orders', { id: 1 }, { key: 'entity-1' });
  t1.publish('orders', { id: 2 }, { key: 'entity-2' });
  t2.publish('orders', { id: 3 }, { key: 'entity-1' });
  const got: Array<{ topic: string; payload: unknown }> = [];
  t1.subscribe('orders', (msg) => got.push({ topic: msg.topic, payload: msg.payload }));
  const report = await bus.replayRange('orders', { key: 'entity-1', speed: 1000 }, { namespace: 't1' });
  assert.equal(report.topic, 't1/orders');
  assert.equal(report.requested, 2);
  assert.equal(report.matched, 1);
  assert.equal(report.delivered, 1);
  assert.deepEqual(got, [{ topic: 't1/orders', payload: { id: 1 } }]);
});

test('replayRange key: pacing follows the matched messages original gaps only', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  // Matched pair 1s apart; then a long tail of unmatched records spaced
  // 10s apart. Unfiltered pacing would sleep ~100ms at speed 1000; the
  // key filter must converge on the matched 1s gap alone (~1ms).
  bus.publish('t', 'a1', { key: 'entity-1' });
  clock.nowMs += 1_000;
  bus.publish('t', 'a2', { key: 'entity-1' });
  for (let i = 0; i < 10; i++) {
    clock.nowMs += 10_000;
    bus.publish('t', `noise-${i}`);
  }
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const started = Date.now();
  const report = await bus.replayRange('t', { key: 'entity-1', speed: 1000 });
  const elapsedMs = Date.now() - started;
  assert.equal(report.requested, 12);
  assert.equal(report.matched, 2);
  assert.deepEqual(received, ['a1', 'a2']);
  // Matched gap 1000ms / speed 1000 = 1ms; the 100s of unmatched tail
  // must cost nothing. 50ms leaves ample headroom over the ~1ms sleep
  // while staying far below the ~101ms an unfiltered replay would take.
  assert.ok(elapsedMs < 50, `key-filtered replay took ${elapsedMs}ms, expected < 50ms`);
});

test('replayRange key: speed still compresses the matched gaps', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publish('t', 'a1', { key: 'entity-1' });
  clock.nowMs += 10_000;
  bus.publish('t', 'a2', { key: 'entity-1' });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const started = Date.now();
  const report = await bus.replayRange('t', { key: 'entity-1', speed: 100 });
  const elapsedMs = Date.now() - started;
  assert.equal(report.matched, 2);
  assert.deepEqual(received, ['a1', 'a2']);
  // Matched gap 10s / speed 100 = 100ms; allow scheduling slack above.
  assert.ok(elapsedMs >= 80, `expected ~100ms of pacing, took ${elapsedMs}ms`);
  assert.ok(elapsedMs < 500, `expected ~100ms of pacing, took ${elapsedMs}ms`);
});

test('replayRange key: empty or non-string key throws RangeError', async () => {
  const bus = new EventBus();
  bus.publish('t', 'x', { key: 'entity-1' });
  bus.subscribe('t', () => {});
  await assert.rejects(() => bus.replayRange('t', { key: '', speed: 1000 }), RangeError);
  await assert.rejects(
    () => bus.replayRange('t', { key: 42 as unknown as string, speed: 1000 }),
    RangeError,
  );
  await assert.rejects(
    () => bus.replayRange('t', { key: null as unknown as string, speed: 1000 }),
    RangeError,
  );
});

test('replayRange key: replayed marker and sequence-gap rules unchanged', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  bus.publish('t', 'a', { key: 'entity-1' });
  bus.publish('t', 'b', { key: 'entity-2' });
  await flush();
  const seen: BusMessage[] = [];
  bus.subscribe('t', (msg) => seen.push(msg));
  await bus.replayRange('t', { key: 'entity-1', speed: 1000 });
  assert.equal(seen.length, 1);
  assert.equal(seen[0].replayed, true);
  bus.publish('t', 'c', { key: 'entity-1' });
  await flush();
  assert.equal(bus.getStats().sequenceGaps, 0);
  sub.unsubscribe();
});

test('replayRange key: no key filter keeps matched equal to requested', async () => {
  const bus = new EventBus();
  bus.publish('t', 'a', { key: 'entity-1' });
  bus.publish('t', 'b');
  bus.publish('t', 'c', { key: 'entity-2' });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const report = await bus.replayRange('t', { speed: 1000 });
  assert.equal(report.requested, 3);
  assert.equal(report.matched, 3);
  assert.equal(report.delivered, 3);
  assert.deepEqual(received, ['a', 'b', 'c']);
});
