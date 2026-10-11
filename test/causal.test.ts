import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-causal-'));

test('causal: out-of-order arrivals are delivered in happens-before order', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('**', (m) => received.push(m.payload as string), { causal: true });
  bus.publish('a', 'c0', { causal: { source: 's', clock: 0 } });
  bus.publish('b', 'c2', { causal: { source: 's', clock: 2 } }); // early: buffered
  bus.publish('a', 'c1', { causal: { source: 's', clock: 1 } }); // releases c2
  await flush();
  assert.deepEqual(received, ['c0', 'c1', 'c2']);
  assert.equal(bus.getStats().causalBuffer.depth, 0);
});

test('causal: sources are independent — one stalled stream never blocks another', async () => {
  const bus = new EventBus();
  const received: Array<[string, number]> = [];
  const sub = bus.subscribe(
    '*',
    (m) => {
      const p = m.payload as { source: string; clock: number };
      received.push([p.source, p.clock]);
    },
    { causal: true },
  );
  bus.publish('t', { source: 'a', clock: 1 }, { causal: { source: 'a', clock: 1 } });
  bus.publish('t', { source: 'b', clock: 1 }, { causal: { source: 'b', clock: 1 } });
  assert.equal(bus.causalBufferDepth(sub.id), 2);
  bus.publish('t', { source: 'a', clock: 0 }, { causal: { source: 'a', clock: 0 } });
  bus.publish('t', { source: 'b', clock: 0 }, { causal: { source: 'b', clock: 0 } });
  await flush();
  // Each source ordered independently; a's cascade drains synchronously
  // at fan-out, so it completes before b's dependency arrives.
  assert.deepEqual(received, [
    ['a', 0],
    ['a', 1],
    ['b', 0],
    ['b', 1],
  ]);
  assert.equal(bus.causalBufferDepth(sub.id), 0);
});

test('causal: source defaults to the topic name', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (m) => received.push(`${m.topic}:${m.payload}`), { causal: true });
  // 't1' stream starts at clock 1 (buffered); 't2' stream starts at 0 —
  // the buffered t1 message never blocks t2.
  bus.publish('t1', 'x', { causal: { clock: 1 } });
  bus.publish('t2', 'y', { causal: { clock: 0 } });
  await flush();
  assert.deepEqual(received, ['t2:y']);
  bus.publish('t1', 'z', { causal: { clock: 0 } });
  await flush();
  assert.deepEqual(received, ['t2:y', 't1:z', 't1:x']);
});

test('causal: regressed clocks deliver immediately without moving the expectation back', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (m) => received.push(m.payload as string), { causal: true });
  bus.publish('t', 'c0', { causal: { clock: 0 } });
  bus.publish('t', 'c1', { causal: { clock: 1 } });
  await flush();
  assert.deepEqual(received, ['c0', 'c1']);
  // A duplicate / late arrival from before the horizon: delivered at
  // once, expectation stays at 2.
  bus.publish('t', 'c0-again', { causal: { clock: 0 } });
  await flush();
  assert.deepEqual(received, ['c0', 'c1', 'c0-again']);
  assert.equal(bus.getStats().causalBuffer.regressedMessages, 1);
  // The stream continues undisturbed.
  bus.publish('t', 'c2', { causal: { clock: 2 } });
  await flush();
  assert.deepEqual(received, ['c0', 'c1', 'c0-again', 'c2']);
});

test('causal: non-causal messages bypass the gate and never disturb expectations', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (m) => received.push(m.payload as string), { causal: true });
  bus.publish('t', 'c0', { causal: { clock: 0 } });
  bus.publish('t', 'plain'); // no clock: straight through
  bus.publish('t', 'c2', { causal: { clock: 2 } }); // buffered: 2 > 1
  await flush();
  assert.deepEqual(received, ['c0', 'plain']);
  assert.equal(bus.getStats().causalBuffer.depth, 1);
  bus.publish('t', 'c1', { causal: { clock: 1 } });
  await flush();
  assert.deepEqual(received, ['c0', 'plain', 'c1', 'c2']);
});

test('causal: disabled by default — zero behavior change, zero overhead', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string));
  // Out-of-order clocks arrive in arrival order for a non-causal subscriber.
  bus.publish('t', 'c0', { causal: { clock: 0 } });
  bus.publish('t', 'c2', { causal: { clock: 2 } });
  bus.publish('t', 'c1', { causal: { clock: 1 } });
  await flush();
  assert.deepEqual(received, ['c0', 'c2', 'c1']);
  assert.deepEqual(bus.getStats().causalBuffer, {
    depth: 0,
    droppedMessages: 0,
    regressedMessages: 0,
  });
  assert.equal(bus.causalBufferDepth(sub.id), 0);
});

test('causal: composes with keyed ordering — happens-before wins over publish order', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (m) => received.push(m.payload as string), { causal: true });
  // Publish order: A (keySeq 1) then B (keySeq 2) — but A's causal clock
  // (1) depends on B's (0).
  bus.publish('t', 'A', { key: 'k', causal: { clock: 1 } });
  bus.publish('t', 'B', { key: 'k', causal: { clock: 0 } });
  await flush();
  // Causal order wins: B (clock 0) before A (clock 1), against keySeq order.
  assert.deepEqual(received, ['B', 'A']);
});

test('causal: keyed messages without clocks keep exact keyed behavior', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (m) => received.push(m.payload as string), { causal: true });
  bus.publishDelayed('a', 'delayed', { delayMs: 100, key: 'k' });
  bus.publish('b', 'live', { key: 'k' });
  await flush();
  assert.deepEqual(received, []); // keyed gate still holds 'live'
  assert.equal(bus.getStats().keyedReorderedMessages, 1);
});

test('causal: buffer depth is observable globally and per subscriber', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('*', () => {}, { causal: true });
  const plain = bus.subscribe('*', () => {});
  bus.publish('t', 'c1', { causal: { clock: 1 } });
  bus.publish('t', 'c2', { causal: { clock: 2 } });
  assert.equal(bus.causalBufferDepth(sub.id), 2);
  assert.equal(bus.causalBufferDepth(plain.id), 0);
  assert.equal(bus.getStats().causalBuffer.depth, 2);
  bus.publish('t', 'c0', { causal: { clock: 0 } });
  await flush();
  assert.equal(bus.causalBufferDepth(sub.id), 0);
  assert.equal(bus.getStats().causalBuffer.depth, 0);
  assert.throws(() => bus.causalBufferDepth('sub-nope'), /unknown subscriber/);
});

test('causal: over-full buffers drop the oldest wait and advance past it (anti-deadlock)', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: { maxBufferPerSource: 2 },
  });
  bus.publish('t', 'c0', { causal: { clock: 0 } }); // delivered, next = 1
  bus.publish('t', 'c3', { causal: { clock: 3 } }); // buffered
  bus.publish('t', 'c4', { causal: { clock: 4 } }); // buffered (at budget)
  assert.equal(bus.getStats().causalBuffer.depth, 2);
  // c5 overflows the budget: the oldest wait (clock 3) is dropped and the
  // expectation jumps past it, releasing c4 and c5 — the stream survives
  // a dependency that never arrives.
  bus.publish('t', 'c5', { causal: { clock: 5 } });
  await flush();
  assert.deepEqual(received, ['c0', 'c4', 'c5']);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  assert.equal(bus.getStats().causalBuffer.depth, 0);
});

test('causal: a buffered message that expires is dropped as expired, never resurrected', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: string[] = [];
  bus.subscribe('*', (m) => received.push(m.payload as string), { causal: true });
  bus.setTopicTtl('t', 100);
  bus.publish('t', 'c0', { causal: { clock: 0 } }); // t=1000, deadline 1100
  bus.publish('t', 'c2', { causal: { clock: 2 } }); // buffered, deadline 1100
  await flush(); // c0 delivered before its deadline; c2 waits
  assert.deepEqual(received, ['c0']);
  assert.equal(bus.getStats().causalBuffer.depth, 1);
  clock.nowMs = 1200; // past c2's deadline
  bus.publish('t', 'c1', { causal: { clock: 1 } }); // deadline 1300: deliverable
  await flush();
  // c1 arrived in time and released c2 — but c2 had expired while
  // waiting, so it is dropped as expired instead of resurrected, and the
  // stream advances past it instead of deadlocking.
  assert.deepEqual(received, ['c0', 'c1']);
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.equal(bus.getStats().causalBuffer.depth, 0);
  // The stream is live: the next clock delivers without hanging.
  bus.publish('t', 'c3', { causal: { clock: 3 } });
  await flush();
  assert.deepEqual(received, ['c0', 'c1', 'c3']);
});

test('causal: group members each observe an ordered subsequence', async () => {
  const bus = new EventBus();
  const a: string[] = [];
  const b: string[] = [];
  bus.subscribeToGroup('g', '*', (m) => a.push(m.payload as string), { causal: true });
  bus.subscribeToGroup('g', '*', (m) => b.push(m.payload as string), { causal: true });
  // Round-robin: c0 -> A, c1 -> B, c2 -> A. Each member skips the clocks
  // assigned to the other instead of hanging on them.
  bus.publish('t', 'c0', { causal: { clock: 0 } });
  bus.publish('t', 'c1', { causal: { clock: 1 } });
  bus.publish('t', 'c2', { causal: { clock: 2 } });
  await flush();
  assert.deepEqual(a, ['c0', 'c2']);
  assert.deepEqual(b, ['c1']);
});

test('causal: invalid publish and subscribe options throw RangeError', async () => {
  const bus = new EventBus();
  for (const bad of [
    { clock: -1 },
    { clock: 1.5 },
    { clock: Number.NaN },
    { source: '', clock: 0 },
    { source: 42 as unknown as string, clock: 0 },
  ]) {
    assert.throws(() => bus.publish('t', 'x', { causal: bad }), RangeError);
  }
  assert.throws(() => bus.publish('t', 'x', { causal: 'nope' as unknown as { clock: number } }), RangeError);
  assert.throws(() => bus.subscribe('*', () => {}, { causal: { maxBufferPerSource: 0 } }), RangeError);
  assert.throws(() => bus.subscribe('*', () => {}, { causal: { maxBufferPerSource: 1.5 } }), RangeError);
  assert.throws(() => bus.subscribe('*', () => {}, { causal: 'yes' as unknown as boolean }), RangeError);
  // Failed subscribes leave no half-registered subscriber behind.
  assert.equal(bus.subscriberCount(), 0);
});

test('causal: the clock persists on the durable log and replay restores happens-before order', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  // Logged in arrival order (1, 0, 2) — the clock, not the log order,
  // decides delivery order.
  bus.publish('t', 'c1', { causal: { clock: 1 } });
  bus.publish('t', 'c0', { causal: { clock: 0 } });
  bus.publish('t', 'c2', { causal: { clock: 2 } });
  const log = DurableTopicLog.open({ dir });
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.causal),
    [
      { source: 't', clock: 1 },
      { source: 't', clock: 0 },
      { source: 't', clock: 2 },
    ],
  );
  const resumed: BusMessage[] = [];
  bus.subscribe('t', (m) => resumed.push(m), { causal: true, resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(
    resumed.map((m) => m.payload),
    ['c0', 'c1', 'c2'],
  );
});

test('causal: metrics exposition renders the causal buffer gauges', () => {
  const bus = new EventBus();
  bus.subscribe('*', () => {}, { causal: true });
  bus.publish('t', 'c1', { causal: { clock: 1 } });
  const exposition = renderPrometheus(bus.getStats());
  assert.ok(exposition.includes('eventbus_causal_buffer_depth 1'));
  assert.ok(exposition.includes('eventbus_causal_buffer_dropped_messages_total 0'));
  assert.ok(exposition.includes('eventbus_causal_buffer_regressed_messages_total 0'));
});
