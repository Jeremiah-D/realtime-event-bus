import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type StreamJoinOutput } from '../src/bus.ts';
import { StreamJoinEngine, validateStreamJoinOptions } from '../src/streamjoin.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const keyOf = (msg: BusMessage) => (msg.payload as { orderId?: string }).orderId;

function collectOutputs(bus: EventBus, topic: string) {
  const seen: StreamJoinOutput[] = [];
  bus.subscribe(topic, (msg) => {
    seen.push(msg.payload as StreamJoinOutput);
  });
  return seen;
}

// ---------------------------------------------------------------------------
// inner join
// ---------------------------------------------------------------------------

test('join: inner hit derives one combined message with lineage metadata', async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  const id = bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { id: 'recon', on: keyOf, windowMs: 60_000, how: 'inner', outputTopic: 'joined' },
  );
  assert.equal(id, 'recon');
  bus.publish('orders.created', { orderId: 'o1', item: 'book' });
  bus.publish('payments.done', { orderId: 'o1', amount: 42 });
  await flush();
  assert.equal(out.length, 1);
  const j = out[0];
  assert.equal(j.joinId, 'recon');
  assert.equal(j.key, 'o1');
  assert.deepEqual(j.left, { orderId: 'o1', item: 'book' });
  assert.deepEqual(j.right, { orderId: 'o1', amount: 42 });
  assert.equal(j.leftSeq, 1);
  assert.equal(j.rightSeq, 1);
  assert.ok(j.windowStart <= j.joinedAt);
  const stats = bus.getStats().joins!;
  assert.equal(stats.length, 1);
  assert.equal(stats[0].joined, 1);
  assert.equal(stats[0].joinLate, 0);
  assert.equal(stats[0].joinEvicted, 0);
  assert.equal(bus.getStats().totalStreamJoinMatches, 1);
});

test('join: right-first arrival matches symmetrically', async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 60_000, how: 'inner', outputTopic: 'joined' },
  );
  bus.publish('payments.done', { orderId: 'o9', amount: 7 });
  bus.publish('orders.created', { orderId: 'o9', item: 'pen' });
  await flush();
  assert.equal(out.length, 1);
  assert.equal(out[0].key, 'o9');
  // Left/right roles stay fixed regardless of arrival order.
  assert.deepEqual(out[0].left, { orderId: 'o9', item: 'pen' });
  assert.deepEqual(out[0].right, { orderId: 'o9', amount: 7 });
});

test('join: inner miss on different keys derives nothing', async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 60_000, how: 'inner', outputTopic: 'joined' },
  );
  bus.publish('orders.created', { orderId: 'o1' });
  bus.publish('payments.done', { orderId: 'o2' });
  await flush();
  assert.equal(out.length, 0);
  assert.equal(bus.getStats().totalStreamJoinMatches, 0);
});

test('join: one arrival matching several peers emits one output per pair', async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 60_000, how: 'inner', outputTopic: 'joined' },
  );
  bus.publish('payments.done', { orderId: 'o1', amount: 1 });
  bus.publish('payments.done', { orderId: 'o1', amount: 2 });
  bus.publish('orders.created', { orderId: 'o1', item: 'book' });
  await flush();
  assert.equal(out.length, 2);
  assert.deepEqual(
    out.map((o) => (o.right as { amount: number }).amount).sort(),
    [1, 2],
  );
});

// ---------------------------------------------------------------------------
// left join
// ---------------------------------------------------------------------------

test("join: how='left' emits left rows with right=null when no peer matches", async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 60_000, how: 'left', outputTopic: 'joined' },
  );
  bus.publish('orders.created', { orderId: 'o1', item: 'book' });
  await flush();
  assert.equal(out.length, 1);
  assert.deepEqual(out[0].left, { orderId: 'o1', item: 'book' });
  assert.equal(out[0].right, null);
  assert.equal(out[0].rightSeq, null);
  assert.equal(out[0].leftSeq, 1);
});

test("join: how='left' still emits combined rows on a hit", async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 60_000, how: 'left', outputTopic: 'joined' },
  );
  bus.publish('orders.created', { orderId: 'o1' });
  bus.publish('payments.done', { orderId: 'o1', amount: 5 });
  await flush();
  // The left arrival first emits its unmatched row (right=null), then
  // the right arrival pairs with the buffered left row: streaming
  // left-join emits per arrival, so the pair yields two outputs.
  assert.equal(out.length, 2);
  assert.equal(out[0].right, null);
  assert.deepEqual(out[1].right, { orderId: 'o1', amount: 5 });
  assert.equal(out[1].rightSeq, 1);
});

// ---------------------------------------------------------------------------
// window, lateness, eviction
// ---------------------------------------------------------------------------

test('join: entries outside the window do not match', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 500, how: 'inner', outputTopic: 'joined' },
  );
  bus.publish('payments.done', { orderId: 'o1', amount: 1 }, { eventTime: clock.nowMs });
  clock.nowMs += 1_000; // the right entry is now outside any 500ms window
  bus.publish('orders.created', { orderId: 'o1' }, { eventTime: clock.nowMs });
  await flush();
  assert.equal(out.length, 0);
  assert.equal(bus.getStats().joins![0].bufferedRight, 0); // swept
});

test('join: late arrivals (eventTime older than now-windowMs) are dropped and counted', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 500, how: 'inner', outputTopic: 'joined' },
  );
  bus.publish('payments.done', { orderId: 'o1', amount: 1 }, { eventTime: clock.nowMs });
  clock.nowMs += 1_000;
  // A left event stamped at the old time: its whole window is in the past.
  bus.publish('orders.created', { orderId: 'o1' }, { eventTime: clock.nowMs - 1_000 });
  await flush();
  assert.equal(out.length, 0);
  const stats = bus.getStats().joins![0];
  assert.equal(stats.joinLate, 1);
  assert.equal(stats.bufferedLeft, 0); // never buffered
  assert.equal(bus.getStats().totalStreamJoinLate, 1);
});

test('join: maxBufferedPerKey evicts the oldest first and counts joinEvicted', async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 60_000, how: 'inner', outputTopic: 'joined', maxBufferedPerKey: 2 },
  );
  bus.publish('orders.created', { orderId: 'o1', n: 1 });
  bus.publish('orders.created', { orderId: 'o1', n: 2 });
  bus.publish('orders.created', { orderId: 'o1', n: 3 }); // evicts n:1
  bus.publish('payments.done', { orderId: 'o1', amount: 9 });
  await flush();
  const stats = bus.getStats().joins![0];
  assert.equal(stats.joinEvicted, 1);
  assert.equal(bus.getStats().totalStreamJoinEvicted, 1);
  // The evicted peer never matches: only the two survivors pair up.
  assert.equal(out.length, 2);
  assert.deepEqual(
    out.map((o) => (o.left as { n: number }).n).sort(),
    [2, 3],
  );
});

// ---------------------------------------------------------------------------
// isolation, lifecycle, validation
// ---------------------------------------------------------------------------

test('join: a throwing key function is isolated and counted', async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    {
      on: () => {
        throw new Error('bad key');
      },
      windowMs: 60_000,
      how: 'inner',
      outputTopic: 'joined',
    },
  );
  bus.publish('orders.created', { orderId: 'o1' });
  bus.publish('payments.done', { orderId: 'o1' });
  await flush();
  assert.equal(out.length, 0);
  assert.equal(bus.getStats().joins![0].keyErrors, 2);
});

test('join: leaveJoin stops observation; unknown ids return false', async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'joined');
  const id = bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 60_000, how: 'inner', outputTopic: 'joined' },
  );
  assert.equal(bus.leaveJoin('nope'), false);
  bus.publish('orders.created', { orderId: 'o1' });
  assert.equal(bus.leaveJoin(id), true);
  assert.equal(bus.leaveJoin(id), false);
  bus.publish('payments.done', { orderId: 'o1' });
  await flush();
  assert.equal(out.length, 0);
  assert.deepEqual(bus.getStats().joins, []);
});

test('join: derived outputs never re-enter join observation (no self-trigger)', async () => {
  const bus = new EventBus();
  const out = collectOutputs(bus, 'in.joined');
  bus.joinTopics(
    { left: 'in.*', right: 'other.*' },
    { on: keyOf, windowMs: 60_000, how: 'inner', outputTopic: 'in.joined' },
  );
  bus.publish('in.a', { orderId: 'o1' });
  bus.publish('other.b', { orderId: 'o1' });
  await flush();
  // Exactly one output: the derived message on in.joined (which matches
  // left pattern in.*) did not feed the join again.
  assert.equal(out.length, 1);
  assert.equal(bus.getStats().totalStreamJoinMatches, 1);
});

test('joinTopics validates fail-fast with RangeError', () => {
  const bus = new EventBus();
  const good = { on: keyOf, windowMs: 1000, how: 'inner' as const, outputTopic: 'out' };
  assert.throws(() => bus.joinTopics({ left: '', right: 'b' }, good), RangeError);
  assert.throws(() => bus.joinTopics({ left: 'a', right: '' }, good), RangeError);
  assert.throws(() => bus.joinTopics({ left: 'a', right: 'b' }, { ...good, windowMs: 0 }), RangeError);
  assert.throws(() => bus.joinTopics({ left: 'a', right: 'b' }, { ...good, windowMs: -5 }), RangeError);
  assert.throws(
    () => bus.joinTopics({ left: 'a', right: 'b' }, { ...good, how: 'outer' as never }),
    RangeError,
  );
  assert.throws(
    () => bus.joinTopics({ left: 'a', right: 'b' }, { ...good, on: 'x' as never }),
    RangeError,
  );
  assert.throws(
    () => bus.joinTopics({ left: 'a', right: 'b' }, { ...good, outputTopic: '' }),
    RangeError,
  );
  assert.throws(
    () => bus.joinTopics({ left: 'a', right: 'b' }, { ...good, maxBufferedPerKey: 0 }),
    RangeError,
  );
  bus.joinTopics({ left: 'a', right: 'b' }, { ...good, id: 'dup' });
  assert.throws(() => bus.joinTopics({ left: 'a', right: 'b' }, { ...good, id: 'dup' }), RangeError);
});

test('join: stats shape is absent without joins; metrics render the series', async () => {
  const bus = new EventBus();
  assert.equal(bus.getStats().joins, undefined);
  const out = collectOutputs(bus, 'joined');
  bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { on: keyOf, windowMs: 60_000, how: 'inner', outputTopic: 'joined' },
  );
  bus.publish('orders.created', { orderId: 'o1' });
  bus.publish('payments.done', { orderId: 'o1' });
  await flush();
  assert.equal(out.length, 1);
  const exposition = renderPrometheus(bus.getStats());
  assert.match(exposition, /eventbus_stream_join_matches_total 1/);
  assert.match(exposition, /eventbus_stream_join_late_total 0/);
  assert.match(exposition, /eventbus_stream_join_evicted_total 0/);
});

// ---------------------------------------------------------------------------
// engine unit tests (injectable clock/timers)
// ---------------------------------------------------------------------------

test('engine: validateStreamJoinOptions rejects bad options', () => {
  const compile = (p: string) => new RegExp(p);
  const good = { on: keyOf, windowMs: 100, how: 'inner' as const, outputTopic: 'o' };
  assert.throws(() => validateStreamJoinOptions({ ...good, windowMs: NaN }, compile), RangeError);
  assert.throws(
    () => validateStreamJoinOptions({ ...good, windowMs: Infinity }, compile),
    RangeError,
  );
  assert.throws(() => validateStreamJoinOptions({ ...good, id: 'bad id!' }, compile), RangeError);
  assert.throws(() => validateStreamJoinOptions(null as never, compile), RangeError);
});

test('engine: sweep timer is unrefd and cleared on remove', async () => {
  let cleared = 0;
  const timers: Array<() => void> = [];
  const engine = new StreamJoinEngine({
    now: () => 1_000_000,
    compile: (p: string) => new RegExp(p.replace(/\*/g, '.*')),
    emitJoin: () => {},
    setTimer: (fn) => {
      timers.push(fn);
      return { unref: () => {} };
    },
    clearTimer: () => {
      cleared += 1;
    },
  });
  const id = engine.define(
    { left: 'a', right: 'b' },
    { on: keyOf, windowMs: 50, how: 'inner', outputTopic: 'out' },
  );
  assert.equal(timers.length, 1);
  assert.equal(engine.remove(id), true);
  assert.equal(cleared, 1);
  // A timer firing after remove is a no-op.
  timers[0]();
  assert.deepEqual(engine.snapshot(), []);
});
