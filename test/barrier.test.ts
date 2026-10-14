import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-barrier-'));

const byTopic = (bus: EventBus) =>
  Object.fromEntries(bus.getStats().topics.map((t) => [t.topic, t]));

test('barrier: control message is delivered with its epoch marker and consumes no seq', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m), { barrier: true });
  bus.publish('t', 'm1');
  const accepted = bus.publish('t', { cut: 'cfg-v2' }, { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  assert.equal(accepted, 1);
  await flush();
  assert.equal(received.length, 3);
  assert.equal(received[0].payload, 'm1');
  assert.equal(received[0].seq, 1);
  assert.equal(received[2].payload, 'm2');
  assert.equal(received[2].seq, 2);
  // The barrier itself: control message, epoch marker on the envelope,
  // user cut metadata in the payload verbatim, no business seq.
  const barrier = received[1];
  assert.deepEqual(barrier.barrier, { epoch: 1 });
  assert.deepEqual(barrier.payload, { cut: 'cfg-v2' });
  assert.equal(barrier.seq, 0);
  // No seq consumed: the per-topic counter advanced only for m1/m2, and
  // no phantom gap was recorded.
  const topics = byTopic(bus);
  assert.equal(topics['t'].lastSeq, 2);
  assert.equal(topics['t'].publishedMessages, 2);
  assert.equal(topics['t'].sequenceGaps, 0);
  assert.equal(bus.getStats().barrier.publishedBarriers, 1);
});

test('barrier: subscribers that did not opt in never see barrier messages', async () => {
  const bus = new EventBus();
  const plain: unknown[] = [];
  const opted: BusMessage[] = [];
  bus.subscribe('t', (m) => plain.push(m.payload));
  bus.subscribe('t', (m) => opted.push(m), { barrier: true });
  const accepted = bus.publish('t', 'm1');
  assert.equal(accepted, 2); // both subscribers accept the data message
  const bAccepted = bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  assert.equal(bAccepted, 1); // only the opted-in subscriber counts
  await flush();
  assert.deepEqual(plain, ['m1']);
  assert.equal(opted.length, 2);
  assert.deepEqual(opted[1].barrier, { epoch: 1 });
});

test('barrier: epochs must be monotonic — increasing epochs all arm', async () => {
  const bus = new EventBus();
  const epochs: number[] = [];
  bus.subscribe('t', (m) => {
    if (m.barrier !== undefined) epochs.push(m.barrier.epoch);
  }, { barrier: true });
  bus.publish('t', 'b0', { barrier: { epoch: 0 } });
  bus.publish('t', 'b1', { barrier: { epoch: 1 } });
  bus.publish('t', 'b2', { barrier: { epoch: 2 } });
  await flush();
  assert.deepEqual(epochs, [0, 1, 2]);
  assert.equal(bus.getStats().barrier.droppedBarriers, 0);
});

test('barrier: out-of-order barrier is discarded and audited', async () => {
  const bus = new EventBus();
  const epochs: number[] = [];
  const sub = bus.subscribe('t', (m) => {
    if (m.barrier !== undefined) epochs.push(m.barrier.epoch);
  }, { barrier: true });
  bus.publish('t', 'b2', { barrier: { epoch: 2 } });
  bus.publish('t', 'b1', { barrier: { epoch: 1 } }); // regressed: discarded
  bus.publish('t', 'b2again', { barrier: { epoch: 2 } }); // equal: discarded
  bus.publish('t', 'b3', { barrier: { epoch: 3 } });
  await flush();
  assert.deepEqual(epochs, [2, 3]);
  assert.equal(bus.getStats().barrier.droppedBarriers, 2);
  const events = bus.getSubscriptionAudit({ action: 'barrier_dropped' });
  assert.equal(events.length, 2);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].topic, 't');
  assert.deepEqual(events[0].detail, { epoch: 1, lastEpoch: 2 });
  assert.deepEqual(events[1].detail, { epoch: 2, lastEpoch: 2 });
});

test('barrier: post-cut messages are held until the barrier drains, then released at once', async () => {
  const bus = new EventBus();
  const received: Array<{ payload: unknown; barrier?: { epoch: number } }> = [];
  bus.subscribe('t', (m) => received.push({ payload: m.payload, barrier: m.barrier }), {
    barrier: true,
  });
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  // Synchronously observable hold: m2/m3 are behind the armed barrier,
  // not in the subscriber's queue yet.
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  await flush();
  assert.deepEqual(
    received.map((r) => r.payload),
    ['m1', 'cut', 'm2', 'm3'],
  );
  assert.deepEqual(received[1].barrier, { epoch: 1 });
  assert.equal(bus.getStats().barrier.heldMessages, 0);
  assert.equal(bus.getStats().barrier.releasedMessages, 2);
});

test('barrier: held messages re-enter the pipeline on release (filter still applies)', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  // The barrier itself bypasses the content filter (a filter must never
  // swallow the cut marker and wedge the hold); held data still filters
  // normally at release.
  bus.subscribe('t', (m) => received.push(m.payload), {
    barrier: true,
    filter: (p) => p !== 'm2',
  });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  await flush();
  assert.deepEqual(received, ['cut']);
  assert.equal(bus.getStats().barrier.releasedMessages, 1);
});

test('barrier: invalid epochs and options fail fast with RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.publish('t', 'x', { barrier: { epoch: -1 } }), RangeError);
  assert.throws(() => bus.publish('t', 'x', { barrier: { epoch: 1.5 } }), RangeError);
  assert.throws(() => bus.publish('t', 'x', { barrier: 'nope' } as never), RangeError);
  assert.throws(() => bus.publish('t', 'x', { barrier: {} } as never), RangeError);
  assert.throws(() => bus.subscribe('t', () => {}, { barrier: 'yes' as never }), RangeError);
  assert.throws(
    () => bus.subscribe('t', () => {}, { barrier: { maxHeldMessages: 0 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', () => {}, { barrier: { maxHeldMessages: 1.5 } }),
    RangeError,
  );
  // Nothing was mutated by the failed publishes.
  assert.equal(bus.getStats().barrier.publishedBarriers, 0);
});

test('barrier: hold-buffer overflow fails open — releases held, disarms, counts', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload), {
    barrier: { maxHeldMessages: 2 },
  });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm1');
  bus.publish('t', 'm2');
  bus.publish('t', 'm3'); // overflows: fail-open release of m1/m2, gate disarms
  await flush();
  assert.deepEqual(received, ['cut', 'm1', 'm2', 'm3']);
  const barrier = bus.getStats().barrier;
  assert.equal(barrier.overflowedHolds, 1);
  assert.equal(barrier.releasedMessages, 2);
  assert.equal(barrier.heldMessages, 0);
});

test('barrier + causal: barrier gate first, causal order preserved after release', async () => {
  const bus = new EventBus();
  const received: Array<{ payload: unknown; barrier?: { epoch: number } }> = [];
  bus.subscribe(
    't',
    (m) => received.push({ payload: m.payload, barrier: m.barrier }),
    { barrier: true, causal: true },
  );
  bus.publish('t', 'c0', { causal: { source: 's', clock: 0 } });
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  // Both arrive after the cut in delivery order: held first, then ordered
  // by the causal gate at release — c3 waits for c2 even though it was
  // held first.
  bus.publish('t', 'c3', { causal: { source: 's', clock: 3 } });
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  await flush();
  assert.deepEqual(
    received.map((r) => r.payload),
    ['c0', 'c1', 'cut', 'c2', 'c3'],
  );
  assert.deepEqual(received[2].barrier, { epoch: 1 });
  assert.equal(bus.getStats().barrier.releasedMessages, 2);
  assert.equal(bus.getStats().causalBuffer.depth, 0);
});

test('barrier + delayed: a schedule due after the barrier armed is held behind it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: Array<{ payload: unknown; barrier?: { epoch: number } }> = [];
  bus.subscribe('t', (m) => received.push({ payload: m.payload, barrier: m.barrier }), {
    barrier: true,
  });
  bus.publish('t', 'm1');
  bus.publishDelayed('t', 'late', { delayMs: 1000 }); // due at t=2000
  bus.publish('t', 'cut', { barrier: { epoch: 1 } }); // arms; flush not run yet
  clock.nowMs = 2000;
  await flush(); // sweep fans out 'late' while the barrier is armed -> held
  assert.deepEqual(
    received.map((r) => r.payload),
    ['m1', 'cut', 'late'],
  );
  assert.deepEqual(received[1].barrier, { epoch: 1 });
  // Proof it went through the hold, not just queue order:
  assert.equal(bus.getStats().barrier.releasedMessages, 1);
  assert.equal(bus.getStats().barrier.heldMessages, 0);
});

test('barrier + retained: snapshot bypasses the gate; retain flag ignored on barriers', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', 'v1', { retain: true });
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m), { barrier: true });
  // Retained snapshot is handed over synchronously at subscribe time —
  // before any barrier can arm — bypassing the barrier gate.
  assert.equal(received.length, 1);
  assert.equal(received[0].payload, 'v1');
  assert.equal(received[0].retained, true);
  assert.equal(received[0].seq, 1);
  // A barrier publish with retain:true ignores the retain flag: it is a
  // control marker, not a value snapshot.
  bus.publish('t', 'cut', { barrier: { epoch: 5 }, retain: true });
  await flush();
  assert.equal(received.length, 2);
  assert.deepEqual(received[1].barrier, { epoch: 5 });
  // A late joiner gets the v1 value snapshot — never the barrier.
  const late: BusMessage[] = [];
  bus.subscribe('t', (m) => late.push(m), { barrier: true });
  assert.equal(late.length, 1);
  assert.equal(late[0].payload, 'v1');
  assert.equal(late[0].retained, true);
  assert.equal(late[0].barrier, undefined);
});

test('barrier + replay: barriers are not logged and never replay', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const live: BusMessage[] = [];
  bus.subscribe('t', (m) => live.push(m), { barrier: true });
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  await flush();
  assert.equal(live.length, 3);
  assert.equal(bus.getStats().barrier.publishedBarriers, 1);
  // A resuming subscriber replays data only: the barrier was never
  // written to the durable log, and replay bypasses the barrier gate.
  const replayed: BusMessage[] = [];
  bus.subscribe('t', (m) => replayed.push(m), { barrier: true, resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(
    replayed.map((m) => m.payload),
    ['m1', 'm2'],
  );
  assert.deepEqual(
    replayed.map((m) => m.seq),
    [1, 2],
  );
  assert.ok(replayed.every((m) => m.barrier === undefined));
  assert.equal(bus.getStats().barrier.heldMessages, 0);
});

test('barrier: cross-topic cut — one epoch sequence orders the whole stream', async () => {
  const bus = new EventBus();
  const received: Array<{ topic: string; payload: unknown; barrier?: { epoch: number } }> = [];
  bus.subscribe('**', (m) => received.push({ topic: m.topic, payload: m.payload, barrier: m.barrier }), {
    barrier: true,
  });
  bus.publish('a', 'a1');
  bus.publish('b', 'cut', { barrier: { epoch: 1 } });
  bus.publish('a', 'a2');
  bus.publish('b', 'b2');
  assert.equal(bus.getStats().barrier.heldMessages, 2);
  await flush();
  assert.deepEqual(
    received.map((r) => [r.topic, r.payload]),
    [
      ['a', 'a1'],
      ['b', 'cut'],
      ['a', 'a2'],
      ['b', 'b2'],
    ],
  );
  assert.deepEqual(received[1].barrier, { epoch: 1 });
});

test('barrier + batch: the hold releases when the barrier batch is handed off', async () => {
  const bus = new EventBus();
  const batches: BusMessage[][] = [];
  bus.subscribe('t', (msgs) => batches.push(msgs as unknown as BusMessage[]), {
    barrier: true,
    batch: { maxSize: 2, maxWaitMs: 10_000 },
  });
  bus.publish('t', 'm1');
  bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  bus.publish('t', 'm2');
  bus.publish('t', 'm3');
  await flush();
  assert.equal(batches.length, 2);
  assert.deepEqual(
    batches[0].map((m) => m.payload),
    ['m1', 'cut'],
  );
  assert.deepEqual(batches[0][1].barrier, { epoch: 1 });
  // The barrier's batch hand-off released m2/m3 at once into the next batch.
  assert.deepEqual(
    batches[1].map((m) => m.payload),
    ['m2', 'm3'],
  );
  assert.equal(bus.getStats().barrier.releasedMessages, 2);
});

test('barrier + consumer group: one member gets the cut, group offsets never move on seq 0', async () => {
  const bus = new EventBus();
  const gotA: BusMessage[] = [];
  const gotB: BusMessage[] = [];
  bus.subscribeToGroup('g', 't', (m) => gotA.push(m), { barrier: true });
  bus.subscribeToGroup('g', 't', (m) => gotB.push(m), { barrier: true });
  bus.publish('t', 'm1');
  const accepted = bus.publish('t', 'cut', { barrier: { epoch: 1 } });
  assert.equal(accepted, 1); // competing group: exactly one member receives the barrier
  bus.publish('t', 'm2');
  await flush();
  const barrierDeliveries = [...gotA, ...gotB].filter((m) => m.barrier !== undefined);
  assert.equal(barrierDeliveries.length, 1);
  assert.deepEqual(barrierDeliveries[0].barrier, { epoch: 1 });
  // Both data messages still routed; the barrier never regressed offsets.
  const dataCount = [...gotA, ...gotB].filter((m) => m.barrier === undefined).length;
  assert.equal(dataCount, 2);
  const offsets = bus.getGroupOffsets('g');
  assert.ok(Object.values(offsets).every((seq) => seq >= 1));
});
