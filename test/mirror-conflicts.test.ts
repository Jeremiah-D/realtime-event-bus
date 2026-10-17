import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const flushes = async (n: number) => {
  for (let i = 0; i < n; i++) await flush();
};

function collect() {
  const messages: BusMessage[] = [];
  return {
    messages,
    handler: (msg: BusMessage) => {
      messages.push(msg);
    },
  };
}

test('every admitted message carries an HLC stamped by its bus', async () => {
  const bus = new EventBus({ busId: 'hlc-a' });
  const c = collect();
  bus.subscribe('t', c.handler);
  bus.publish('t', { v: 1 });
  bus.publish('t', { v: 2 });
  await flushes(2);
  assert.equal(c.messages.length, 2);
  const [first, second] = c.messages;
  assert.equal(first.hlc?.node, 'hlc-a');
  assert.equal(second.hlc?.node, 'hlc-a');
  // Monotonic per bus: the second write orders after the first.
  assert.ok(first.hlc!.pt <= second.hlc!.pt);
  assert.notDeepEqual(first.hlc, second.hlc);
});

test('mirror preserves the origin write HLC verbatim and carries the key', async () => {
  const src = new EventBus({ busId: 'src-hlc' });
  const dst = new EventBus({ busId: 'dst-hlc' });
  const c = collect();
  dst.subscribe('t', c.handler);
  dst.mirror(src, [{ srcPattern: 't' }]);
  src.publish('t', { v: 1 }, { key: 'order-7' });
  await flushes(3);
  assert.equal(c.messages.length, 1);
  const msg = c.messages[0];
  assert.equal(msg.key, 'order-7');
  assert.equal(msg.hlc?.node, 'src-hlc'); // origin HLC, not the destination's tick
  assert.equal(msg.mirror?.busId, 'src-hlc');
});

test('active-active: the older concurrent write is dropped as a conflict', async () => {
  // B's clock runs behind A's: without HLC merge + arbitration, wall-clock
  // order would be ambiguous. LWW must converge on A's newer write.
  let nowA = 10_000;
  let nowB = 1_000;
  const a = new EventBus({ busId: 'aa-a', now: () => nowA });
  const b = new EventBus({ busId: 'aa-b', now: () => nowB });
  const ca = collect();
  const cb = collect();
  a.subscribe('t', ca.handler);
  b.subscribe('t', cb.handler);
  a.mirror(b, [{ srcPattern: 't' }]);
  b.mirror(a, [{ srcPattern: 't' }]);
  b.publish('t', { v: 'b-write' }, { key: 'k' });
  a.publish('t', { v: 'a-write' }, { key: 'k' });
  await flushes(6);
  // A never sees B's older write; B converges to A's newer write.
  assert.deepEqual(ca.messages.map((m) => m.payload), [{ v: 'a-write' }]);
  assert.deepEqual(cb.messages.map((m) => m.payload), [{ v: 'b-write' }, { v: 'a-write' }]);
  assert.equal(a.getStats().mirrorConflicts, 2); // stale b-write + looped-back a-write
  assert.equal(b.getStats().mirrorConflicts, 0);
  assert.deepEqual(a.getStats().mirrorConflictsByTopic, [{ topic: 't', conflicts: 2 }]);
  assert.deepEqual(b.getStats().mirrorConflictsByTopic, []);
});

test('HLC merge: a local write after seeing a remote write wins', async () => {
  // A publishes first; B sees it via the mirror, then publishes locally.
  // B's merged clock orders its write after A's, so A accepts it.
  let nowA = 5_000;
  let nowB = 1_000; // behind, but the merge fixes the order
  const a = new EventBus({ busId: 'mg-a', now: () => nowA });
  const b = new EventBus({ busId: 'mg-b', now: () => nowB });
  const ca = collect();
  a.subscribe('t', ca.handler);
  a.mirror(b, [{ srcPattern: 't' }]);
  b.mirror(a, [{ srcPattern: 't' }]);
  a.publish('t', { v: 'a1' }, { key: 'k' });
  await flushes(4);
  b.publish('t', { v: 'b1' }, { key: 'k' }); // after seeing a1
  await flushes(6);
  assert.deepEqual(ca.messages.map((m) => m.payload), [{ v: 'a1' }, { v: 'b1' }]);
  assert.equal(a.getStats().mirrorConflicts, 1); // only the looped-back b1
});

test('loop-dropped messages still arbitrate: loopsPrevented and mirrorConflicts both move', async () => {
  const a = new EventBus({ busId: 'lp-a' });
  const b = new EventBus({ busId: 'lp-b' });
  a.mirror(b, [{ srcPattern: 't' }]);
  b.mirror(a, [{ srcPattern: 't' }]);
  b.publish('t', { v: 1 }, { key: 'k' });
  await flushes(6);
  // B's write loops back to B via A: trail ['lp-b'] trips the loop
  // detector, and the arbitration sees the same HLC twice (stale).
  const bMirrors = b.getStats().mirrors;
  assert.equal(bMirrors[0].loopsPrevented, 1);
  assert.equal(b.getStats().mirrorConflicts, 1);
  assert.equal(a.getStats().mirrorConflicts, 0);
});

test('unkeyed messages skip arbitration entirely', async () => {
  const src = new EventBus({ busId: 'uk-src' });
  const dst = new EventBus({ busId: 'uk-dst' });
  const c = collect();
  dst.subscribe('t', c.handler);
  dst.mirror(src, [{ srcPattern: 't' }]);
  src.publish('t', { v: 1 });
  src.publish('t', { v: 2 });
  await flushes(3);
  assert.equal(c.messages.length, 2);
  assert.equal(dst.getStats().mirrorConflicts, 0);
  assert.equal(dst.getStats().mirroredMessages, 2);
});

test('arbitration is per (topic, key): different keys do not conflict', async () => {
  let now = 1000;
  const src = new EventBus({ busId: 'pk-src', now: () => now });
  const dst = new EventBus({ busId: 'pk-dst', now: () => now });
  const c = collect();
  dst.subscribe('t', c.handler);
  dst.mirror(src, [{ srcPattern: 't' }]);
  // Same HLC millisecond: logical ticks order the writes.
  src.publish('t', { v: 1 }, { key: 'k1' });
  src.publish('t', { v: 2 }, { key: 'k2' });
  await flushes(3);
  assert.equal(c.messages.length, 2);
  assert.equal(dst.getStats().mirrorConflicts, 0);
});

test('stale duplicate of the same write is dropped, not redelivered', async () => {
  // Two mirror rules on the same source topic deliver the same logical
  // write twice; the second arrival carries the identical HLC and must
  // not be delivered again.
  const src = new EventBus({ busId: 'dup-src' });
  const dst = new EventBus({ busId: 'dup-dst' });
  const c = collect();
  dst.subscribe('t', c.handler);
  dst.mirror(src, [{ srcPattern: 't' }, { srcPattern: 't', rename: 't' }]);
  src.publish('t', { v: 1 }, { key: 'k' });
  await flushes(4);
  assert.equal(c.messages.length, 1);
  assert.equal(dst.getStats().mirrorConflicts, 1);
  assert.equal(dst.getStats().mirroredMessages, 1);
});

test('Prometheus renders eventbus_mirror_conflicts_total{topic}', async () => {
  let nowA = 10_000;
  let nowB = 1_000;
  const a = new EventBus({ busId: 'pm-a', now: () => nowA });
  const b = new EventBus({ busId: 'pm-b', now: () => nowB });
  a.mirror(b, [{ srcPattern: 't' }]);
  b.mirror(a, [{ srcPattern: 't' }]);
  b.publish('t', { v: 'old' }, { key: 'k' });
  a.publish('t', { v: 'new' }, { key: 'k' });
  await flushes(6);
  const text = renderPrometheus(a.getStats());
  assert.ok(text.includes('# HELP eventbus_mirror_conflicts_total'));
  assert.ok(text.includes('# TYPE eventbus_mirror_conflicts_total counter'));
  assert.ok(text.includes('eventbus_mirror_conflicts_total{topic="t"} 2'));
  // A bus with no conflicts renders the headers but no series.
  const clean = renderPrometheus(b.getStats());
  assert.ok(clean.includes('# HELP eventbus_mirror_conflicts_total'));
  assert.equal(
    clean.split('\n').filter((l) => l.startsWith('eventbus_mirror_conflicts_total{')).length,
    0,
  );
});

test('mirror conflict counting does not disturb the local key register', async () => {
  // A local keyed publish after a dropped stale mirror write still
  // records its (newer) HLC — the register is max, not first-wins.
  let nowA = 10_000;
  let nowB = 1_000;
  const a = new EventBus({ busId: 'rg-a', now: () => nowA });
  const b = new EventBus({ busId: 'rg-b', now: () => nowB });
  const ca = collect();
  a.subscribe('t', ca.handler);
  a.mirror(b, [{ srcPattern: 't' }]);
  a.publish('t', { v: 'a1' }, { key: 'k' });
  b.publish('t', { v: 'stale' }, { key: 'k' }); // older clock: dropped at A
  await flushes(4);
  assert.equal(a.getStats().mirrorConflicts, 1);
  nowA = 20_000;
  a.publish('t', { v: 'a2' }, { key: 'k' }); // newer local write: admitted
  await flushes(2);
  assert.deepEqual(ca.messages.map((m) => m.payload), [{ v: 'a1' }, { v: 'a2' }]);
});
