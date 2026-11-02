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

function defineRecon(bus: EventBus, windowMs: number, maxBufferedPerKey?: number): string {
  return bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    {
      id: 'recon',
      on: keyOf,
      windowMs,
      how: 'inner',
      outputTopic: 'joined',
      ...(maxBufferedPerKey === undefined ? {} : { maxBufferedPerKey }),
    },
  );
}

function joinStats(bus: EventBus) {
  const joins = bus.getStats().joins;
  assert.ok(joins !== undefined && joins.length === 1);
  return joins[0];
}

describe('setStreamJoinMaxBufferedPerKey (EB-345)', () => {
  test('tightening does not evict buffered entries until the next arrival', async () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 60_000, 3);
    // Three left arrivals for the same order, 100ms apart.
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    clock.nowMs += 100;
    bus.publish('orders.created', { orderId: 'o1', item: 'pen' });
    clock.nowMs += 100;
    bus.publish('orders.created', { orderId: 'o1', item: 'lamp' });
    await flush();
    assert.equal(joinStats(bus).bufferedLeft, 3);
    assert.equal(joinStats(bus).joinEvicted, 0);

    // Tighten to 1: the three buffered entries stay put.
    bus.setStreamJoinMaxBufferedPerKey('recon', 1);
    assert.equal(joinStats(bus).maxBufferedPerKey, 1);
    assert.equal(joinStats(bus).bufferedLeft, 3, 'no immediate eviction on retune');
    assert.equal(joinStats(bus).joinEvicted, 0);
    assert.equal(joinStats(bus).windowMs, 60_000, 'the window is untouched');

    const audit = bus.getSubscriptionAudit({ action: 'stream_join_max_buffered_per_key_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      joinId: 'recon',
      maxBufferedPerKey: 1,
      oldMaxBufferedPerKey: 3,
    });

    // The next arrival on that key trims the list down to the new
    // bound, oldest first — only the newest left entry survives.
    clock.nowMs += 100;
    bus.publish('orders.created', { orderId: 'o1', item: 'desk' });
    await flush();
    assert.equal(joinStats(bus).bufferedLeft, 1);
    assert.equal(joinStats(bus).joinEvicted, 3);

    // The survivor is the newest entry: a right arrival matching it
    // derives exactly one output carrying the newest payload.
    bus.publish('payments.done', { orderId: 'o1', amount: 42 }, { eventTime: clock.nowMs });
    await flush();
    assert.equal(out.length, 1);
    assert.deepEqual(out[0].left, { orderId: 'o1', item: 'desk' });
    assert.deepEqual(out[0].right, { orderId: 'o1', amount: 42 });
  });

  test('loosening expands capacity immediately', async () => {
    const bus = new EventBus();
    defineRecon(bus, 60_000, 2);
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    bus.publish('orders.created', { orderId: 'o1', item: 'pen' });
    bus.publish('orders.created', { orderId: 'o1', item: 'lamp' });
    await flush();
    assert.equal(joinStats(bus).bufferedLeft, 2, 'the third arrival evicted the oldest');
    assert.equal(joinStats(bus).joinEvicted, 1);

    bus.setStreamJoinMaxBufferedPerKey('recon', 5);
    assert.equal(joinStats(bus).maxBufferedPerKey, 5);
    assert.equal(joinStats(bus).bufferedLeft, 2, 'the revision keeps what is buffered');

    bus.publish('orders.created', { orderId: 'o1', item: 'desk' });
    bus.publish('orders.created', { orderId: 'o1', item: 'chair' });
    bus.publish('orders.created', { orderId: 'o1', item: 'rug' });
    await flush();
    assert.equal(joinStats(bus).bufferedLeft, 5, 'all three new arrivals fit under the new bound');
    assert.equal(joinStats(bus).joinEvicted, 1, 'nothing more was evicted');

    const audit = bus.getSubscriptionAudit({ action: 'stream_join_max_buffered_per_key_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, {
      joinId: 'recon',
      maxBufferedPerKey: 5,
      oldMaxBufferedPerKey: 2,
    });
  });

  test('a pair already derived is never re-derived or withdrawn', async () => {
    const bus = new EventBus();
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 60_000, 2);
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    bus.publish('payments.done', { orderId: 'o1', amount: 42 });
    await flush();
    assert.equal(out.length, 1);
    const derived = { ...out[0] };

    bus.setStreamJoinMaxBufferedPerKey('recon', 1);
    await flush();
    assert.equal(out.length, 1, 'the revision derives nothing new');
    assert.deepEqual(out[0], derived, 'the derived output is untouched');
    assert.equal(joinStats(bus).joined, 1);
  });

  test('engine: the sweep timer is untouched by a bound revision', () => {
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
    assert.equal(engine.getMaxBufferedPerKey(id), 1000, 'the default bound is 1000');
    assert.equal(engine.getMaxBufferedPerKey('nope'), undefined);

    engine.retuneMaxBufferedPerKey(id, 10);
    assert.equal(engine.getMaxBufferedPerKey(id), 10);
    assert.deepEqual(intervals, [5_000], 'retuning the bound does not re-arm the sweep timer');
    assert.equal(cleared, 0, 'the old timer is not cleared');

    assert.throws(() => engine.retuneMaxBufferedPerKey('nope', 10), RangeError);
    assert.throws(() => engine.retuneMaxBufferedPerKey(id, 0), RangeError);
    assert.throws(() => engine.retuneMaxBufferedPerKey(id, 1.5), RangeError);
    assert.throws(() => engine.retuneMaxBufferedPerKey(id, Number.NaN), RangeError);
    assert.equal(engine.getMaxBufferedPerKey(id), 10, 'failed retunes leave the live bound untouched');
  });

  test('an identical revision is a no-op with no audit', () => {
    const bus = new EventBus();
    defineRecon(bus, 60_000, 3);
    bus.setStreamJoinMaxBufferedPerKey('recon', 3);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'stream_join_max_buffered_per_key_revised' }).length,
      0,
      're-affirming the current bound audits nothing',
    );
    assert.equal(joinStats(bus).maxBufferedPerKey, 3);
  });

  test('is fail-closed: unknown joins and illegal bounds leave the bound untouched', () => {
    const bus = new EventBus();
    defineRecon(bus, 60_000);

    assert.throws(() => bus.setStreamJoinMaxBufferedPerKey('nope', 10), RangeError);
    assert.throws(() => bus.setStreamJoinMaxBufferedPerKey('', 10), RangeError);
    for (const bad of [0, -5, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 'x' as unknown as number]) {
      assert.throws(
        () => bus.setStreamJoinMaxBufferedPerKey('recon', bad),
        RangeError,
        `expected RangeError for ${String(bad)}`,
      );
    }
    assert.equal(joinStats(bus).maxBufferedPerKey, 1000, 'the live bound is untouched');
    assert.equal(joinStats(bus).windowMs, 60_000, 'the window is untouched');
    assert.equal(
      bus.getSubscriptionAudit({ action: 'stream_join_max_buffered_per_key_revised' }).length,
      0,
      'failed revisions audit nothing',
    );

    // A bus with no joins at all has nothing to revise.
    const empty = new EventBus();
    assert.throws(() => empty.setStreamJoinMaxBufferedPerKey('recon', 10), RangeError);
  });
});
