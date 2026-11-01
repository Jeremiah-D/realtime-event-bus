import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type StreamJoinOutput } from '../src/bus.ts';
import { StreamJoinEngine } from '../src/streamjoin.ts';

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

function defineRecon(bus: EventBus, windowMs: number): string {
  return bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { id: 'recon', on: keyOf, windowMs, how: 'inner', outputTopic: 'joined' },
  );
}

function joinStats(bus: EventBus) {
  const joins = bus.getStats().joins;
  assert.ok(joins !== undefined && joins.length === 1);
  return joins[0];
}

describe('setStreamJoinWindow (EB-336)', () => {
  test('widening lets a previously-out-of-window pair match', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 500);
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    await flush();

    // Two seconds later the left entry is outside the old 500ms window
    // but inside the revised 5s one.
    clock.nowMs += 2_000;
    bus.setStreamJoinWindow('recon', 5_000);
    bus.publish('payments.done', { orderId: 'o1', amount: 42 }, { eventTime: 1_000_000 });
    await flush();

    assert.equal(out.length, 1);
    assert.equal(out[0].key, 'o1');
    assert.deepEqual(out[0].left, { orderId: 'o1', item: 'book' });
    assert.deepEqual(out[0].right, { orderId: 'o1', amount: 42 });
    assert.equal(joinStats(bus).windowMs, 5_000);

    const audit = bus.getSubscriptionAudit({ action: 'stream_join_window_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, { joinId: 'recon', windowMs: 5_000, oldWindowMs: 500 });
  });

  test('control: without the revision the same pair is late-dropped', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 500);
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    await flush();

    clock.nowMs += 2_000;
    bus.publish('payments.done', { orderId: 'o1', amount: 42 }, { eventTime: 1_000_000 });
    await flush();

    assert.equal(out.length, 0, 'the right arrival is too late under the old window');
    assert.equal(joinStats(bus).joinLate, 1);
  });

  test('narrowing drops a previously-in-window pair as late', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 5_000);
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    await flush();

    clock.nowMs += 2_000;
    bus.setStreamJoinWindow('recon', 500);
    bus.publish('payments.done', { orderId: 'o1', amount: 42 }, { eventTime: 1_000_000 });
    await flush();

    assert.equal(out.length, 0, 'the right arrival is too late under the new window');
    assert.equal(joinStats(bus).joinLate, 1);
    assert.equal(joinStats(bus).windowMs, 500);
  });

  test('a pair already derived is never re-derived or withdrawn', async () => {
    const bus = new EventBus();
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 5_000);
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    bus.publish('payments.done', { orderId: 'o1', amount: 42 });
    await flush();
    assert.equal(out.length, 1);
    const derived = { ...out[0] };

    bus.setStreamJoinWindow('recon', 500);
    await flush();
    assert.equal(out.length, 1, 'the revision derives nothing new');
    assert.deepEqual(out[0], derived, 'the derived output is untouched');
    assert.equal(joinStats(bus).joined, 1);
  });

  test('narrowing does not evict buffered entries immediately', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 5_000);
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    await flush();
    assert.equal(joinStats(bus).bufferedLeft, 1);

    // Narrow at the same clock time: the buffer is not trimmed yet.
    bus.setStreamJoinWindow('recon', 500);
    assert.equal(joinStats(bus).bufferedLeft, 1, 'no immediate eviction on retune');

    // The next observation sweeps against the new horizon and drops
    // the now-stale left entry; the fresh right arrival buffers.
    clock.nowMs += 2_000;
    bus.publish('payments.done', { orderId: 'o2', amount: 7 });
    await flush();
    assert.equal(joinStats(bus).bufferedLeft, 0);
    assert.equal(joinStats(bus).bufferedRight, 1);
    assert.equal(out.length, 0);
  });

  test('engine: the sweep timer is re-armed at the new cadence', () => {
    const intervals: number[] = [];
    let cleared = 0;
    const engine = new StreamJoinEngine({
      now: () => 1_000_000,
      compile: (p: string) => new RegExp(p.replace(/\*/g, '.*')),
      emitJoin: () => {},
      setTimer: (_fn, ms) => {
        intervals.push(ms);
        return { unref: () => {} };
      },
      clearTimer: () => {
        cleared += 1;
      },
    });
    const id = engine.define(
      { left: 'a', right: 'b' },
      { on: keyOf, windowMs: 5_000, how: 'inner', outputTopic: 'out' },
    );
    assert.deepEqual(intervals, [5_000], 'define arms the sweep at min(windowMs, 60s)');
    assert.equal(engine.getWindowMs(id), 5_000);

    engine.retuneWindow(id, 500);
    assert.equal(engine.getWindowMs(id), 500);
    assert.equal(cleared, 1, 'the old timer is cleared before re-arming');
    assert.deepEqual(intervals, [5_000, 500], 'the sweep is re-armed at the new window');

    engine.retuneWindow(id, 120_000);
    assert.equal(engine.getWindowMs(id), 120_000);
    assert.deepEqual(intervals, [5_000, 500, 60_000], 'the sweep cadence caps at 60s');

    assert.throws(() => engine.retuneWindow('nope', 500), RangeError);
    assert.throws(() => engine.retuneWindow(id, 0), RangeError);
    assert.throws(() => engine.retuneWindow(id, Number.NaN), RangeError);
    assert.equal(engine.getWindowMs('nope'), undefined);
    assert.equal(engine.getWindowMs(id), 120_000, 'failed retunes leave the live window untouched');
  });

  test('an identical revision is a no-op with no audit', () => {
    const bus = new EventBus();
    defineRecon(bus, 5_000);
    bus.setStreamJoinWindow('recon', 5_000);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'stream_join_window_revised' }).length,
      0,
      're-affirming the current window audits nothing',
    );
  });

  test('is fail-closed: unknown joins and illegal windows leave the window untouched', () => {
    const bus = new EventBus();
    defineRecon(bus, 5_000);

    assert.throws(() => bus.setStreamJoinWindow('nope', 100), RangeError);
    assert.throws(() => bus.setStreamJoinWindow('', 100), RangeError);
    for (const bad of [0, -5, Number.NaN, Number.POSITIVE_INFINITY, 'x' as unknown as number]) {
      assert.throws(
        () => bus.setStreamJoinWindow('recon', bad),
        RangeError,
        `expected RangeError for ${String(bad)}`,
      );
    }
    assert.equal(joinStats(bus).windowMs, 5_000, 'the live window is untouched');
    assert.equal(
      bus.getSubscriptionAudit({ action: 'stream_join_window_revised' }).length,
      0,
      'failed revisions audit nothing',
    );

    // A bus with no joins at all has nothing to revise.
    const empty = new EventBus();
    assert.throws(() => empty.setStreamJoinWindow('recon', 100), RangeError);
  });
});
