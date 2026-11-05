import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type StreamJoinHow, type StreamJoinOutput } from '../src/bus.ts';
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

function defineRecon(bus: EventBus, windowMs: number, how: StreamJoinHow): string {
  return bus.joinTopics(
    { left: 'orders.*', right: 'payments.*' },
    { id: 'recon', on: keyOf, windowMs, how, outputTopic: 'joined' },
  );
}

function joinStats(bus: EventBus) {
  const joins = bus.getStats().joins;
  assert.ok(joins !== undefined && joins.length === 1);
  return joins[0];
}

describe('setStreamJoinHow (EB-383)', () => {
  test('inner→left: an unmatched left arrival after the flip derives a right: null row', async () => {
    const bus = new EventBus();
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 60_000, 'inner');

    // Under 'inner' an unmatched left arrival only buffers.
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    await flush();
    assert.equal(out.length, 0);
    assert.equal(joinStats(bus).bufferedLeft, 1);

    bus.setStreamJoinHow('recon', 'left');
    assert.equal(joinStats(bus).how, 'left');
    bus.publish('orders.created', { orderId: 'o2', item: 'pen' });
    await flush();

    assert.equal(out.length, 1);
    assert.equal(out[0].key, 'o2');
    assert.deepEqual(out[0].left, { orderId: 'o2', item: 'pen' });
    assert.equal(out[0].right, null);
    assert.equal(out[0].rightSeq, null);
    assert.equal(joinStats(bus).joined, 1);

    const audit = bus.getSubscriptionAudit({ action: 'stream_join_how_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, { joinId: 'recon', how: 'left', oldHow: 'inner' });
  });

  test('left→inner: an unmatched left arrival after the flip only buffers', async () => {
    const bus = new EventBus();
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 60_000, 'left');

    // Control: under 'left' an unmatched left arrival derives a null row.
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    await flush();
    assert.equal(out.length, 1);
    assert.equal(out[0].right, null);

    bus.setStreamJoinHow('recon', 'inner');
    assert.equal(joinStats(bus).how, 'inner');
    bus.publish('orders.created', { orderId: 'o2', item: 'pen' });
    await flush();

    assert.equal(out.length, 1, 'no null row is derived under inner');
    assert.equal(joinStats(bus).bufferedLeft, 2, 'the unmatched left arrival still buffers');
    assert.equal(joinStats(bus).joined, 1);
  });

  test('a buffered left entry is not backfilled when the flavor flips to left', async () => {
    const bus = new EventBus();
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 60_000, 'inner');
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    await flush();
    assert.equal(joinStats(bus).bufferedLeft, 1);

    // Flipping to 'left' derives nothing for the entry buffered under
    // 'inner' — a left-outer row is derived only at arrival time.
    bus.setStreamJoinHow('recon', 'left');
    await flush();
    assert.equal(out.length, 0, 'the flip backfills nothing');
    assert.equal(joinStats(bus).bufferedLeft, 1);

    // An unrelated right arrival does not resurrect it either.
    bus.publish('payments.done', { orderId: 'o2', amount: 7 });
    await flush();
    assert.equal(out.length, 0);
    assert.equal(joinStats(bus).bufferedLeft, 1);
    assert.equal(joinStats(bus).bufferedRight, 1);
  });

  test('a pair already derived is never re-derived or withdrawn', async () => {
    const bus = new EventBus();
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 60_000, 'inner');
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    bus.publish('payments.done', { orderId: 'o1', amount: 42 });
    await flush();
    assert.equal(out.length, 1);
    const derived = { ...out[0] };

    bus.setStreamJoinHow('recon', 'left');
    await flush();
    assert.equal(out.length, 1, 'the revision derives nothing new');
    assert.deepEqual(out[0], derived, 'the derived output is untouched');
    assert.equal(joinStats(bus).joined, 1);

    bus.setStreamJoinHow('recon', 'inner');
    await flush();
    assert.equal(out.length, 1, 'flipping back derives nothing either');
    assert.deepEqual(out[0], derived);
  });

  test('a right arrival never derives a null row, on either side of a flip', async () => {
    const bus = new EventBus();
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 60_000, 'left');

    // Under 'left' a right arrival with no left peer only buffers —
    // the left-outer rule applies to left arrivals only.
    bus.publish('payments.done', { orderId: 'o1', amount: 42 });
    await flush();
    assert.equal(out.length, 0);
    assert.equal(joinStats(bus).bufferedRight, 1);

    bus.setStreamJoinHow('recon', 'inner');
    bus.publish('payments.done', { orderId: 'o2', amount: 7 });
    await flush();
    assert.equal(out.length, 0);
    assert.equal(joinStats(bus).bufferedRight, 2);

    bus.setStreamJoinHow('recon', 'left');
    bus.publish('payments.done', { orderId: 'o3', amount: 9 });
    await flush();
    assert.equal(out.length, 0, 'flipping back does not make right arrivals derive null rows');
    assert.equal(joinStats(bus).bufferedRight, 3);
    assert.equal(joinStats(bus).joined, 0);
  });

  test('flipping back and forth keeps pairing intact', async () => {
    const bus = new EventBus();
    const out = collectOutputs(bus, 'joined');
    defineRecon(bus, 60_000, 'inner');

    // A left entry buffered under 'inner' still pairs after the flip
    // to 'left' — with the real right payload, not a null row.
    bus.publish('orders.created', { orderId: 'o1', item: 'book' });
    await flush();
    bus.setStreamJoinHow('recon', 'left');
    bus.publish('payments.done', { orderId: 'o1', amount: 42 });
    await flush();
    assert.equal(out.length, 1);
    assert.deepEqual(out[0].left, { orderId: 'o1', item: 'book' });
    assert.deepEqual(out[0].right, { orderId: 'o1', amount: 42 });
    assert.notEqual(out[0].rightSeq, null);

    // And a left entry buffered under 'left' after the flip back to
    // 'inner' still pairs with its later right arrival.
    bus.setStreamJoinHow('recon', 'inner');
    bus.publish('orders.created', { orderId: 'o2', item: 'pen' });
    await flush();
    assert.equal(out.length, 1, 'no null row under inner');
    bus.publish('payments.done', { orderId: 'o2', amount: 7 });
    await flush();
    assert.equal(out.length, 2);
    assert.deepEqual(out[1].right, { orderId: 'o2', amount: 7 });
    assert.equal(joinStats(bus).joined, 2);
  });

  test('engine: the sweep timer is untouched by a flavor revision', () => {
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
    assert.equal(engine.getHow(id), 'inner');
    assert.equal(engine.getHow('nope'), undefined);

    engine.retuneHow(id, 'left');
    assert.equal(engine.getHow(id), 'left');
    assert.deepEqual(intervals, [5_000], 'retuning the flavor does not re-arm the sweep timer');
    assert.equal(cleared, 0, 'the old timer is not cleared');

    engine.retuneHow(id, 'inner');
    assert.equal(engine.getHow(id), 'inner');
    assert.deepEqual(intervals, [5_000]);
    assert.equal(cleared, 0);

    assert.throws(() => engine.retuneHow('nope', 'left'), RangeError);
    for (const bad of ['outer', 'INNER', '', 42, null, undefined] as unknown as StreamJoinHow[]) {
      assert.throws(() => engine.retuneHow(id, bad), RangeError, `expected RangeError for ${String(bad)}`);
    }
    assert.equal(engine.getHow(id), 'inner', 'failed retunes leave the live flavor untouched');
  });

  test('an identical revision is a no-op with no audit', () => {
    const bus = new EventBus();
    defineRecon(bus, 60_000, 'left');
    bus.setStreamJoinHow('recon', 'left');
    assert.equal(
      bus.getSubscriptionAudit({ action: 'stream_join_how_revised' }).length,
      0,
      're-affirming the current flavor audits nothing',
    );
    assert.equal(joinStats(bus).how, 'left');
    assert.equal(joinStats(bus).windowMs, 60_000, 'the window is untouched');
  });

  test('is fail-closed: unknown joins and illegal flavors leave the flavor untouched', () => {
    const bus = new EventBus();
    defineRecon(bus, 60_000, 'inner');

    assert.throws(() => bus.setStreamJoinHow('nope', 'left'), RangeError);
    assert.throws(() => bus.setStreamJoinHow('', 'left'), RangeError);
    for (const bad of ['outer', 'INNER', '', 42, null, undefined] as unknown as StreamJoinHow[]) {
      assert.throws(
        () => bus.setStreamJoinHow('recon', bad),
        RangeError,
        `expected RangeError for ${String(bad)}`,
      );
    }
    assert.equal(joinStats(bus).how, 'inner', 'the live flavor is untouched');
    assert.equal(joinStats(bus).windowMs, 60_000, 'the window is untouched');
    assert.equal(joinStats(bus).maxBufferedPerKey, 1000, 'the buffer bound is untouched');
    assert.equal(
      bus.getSubscriptionAudit({ action: 'stream_join_how_revised' }).length,
      0,
      'failed revisions audit nothing',
    );

    // A bus with no joins at all has nothing to revise.
    const empty = new EventBus();
    assert.throws(() => empty.setStreamJoinHow('recon', 'left'), RangeError);
  });

  test('audit pins previous and current across flips in both directions', () => {
    const bus = new EventBus();
    defineRecon(bus, 60_000, 'inner');
    bus.setStreamJoinHow('recon', 'left');
    bus.setStreamJoinHow('recon', 'inner');
    bus.setStreamJoinHow('recon', 'left');

    const audit = bus.getSubscriptionAudit({ action: 'stream_join_how_revised' });
    assert.equal(audit.length, 3);
    assert.deepEqual(audit[0].detail, { joinId: 'recon', how: 'left', oldHow: 'inner' });
    assert.deepEqual(audit[1].detail, { joinId: 'recon', how: 'inner', oldHow: 'left' });
    assert.deepEqual(audit[2].detail, { joinId: 'recon', how: 'left', oldHow: 'inner' });
  });

  test('getStats().joins.how reflects the live flavor immediately', () => {
    const clock = controllableClock();
    const bus = new EventBus({ now: clock.now });
    defineRecon(bus, 60_000, 'inner');
    assert.equal(joinStats(bus).how, 'inner');

    bus.setStreamJoinHow('recon', 'left');
    assert.equal(joinStats(bus).how, 'left', 'the flip is visible without any arrival');
    assert.equal(joinStats(bus).windowMs, 60_000, 'the window is untouched');
    assert.equal(joinStats(bus).maxBufferedPerKey, 1000, 'the buffer bound is untouched');

    bus.setStreamJoinHow('recon', 'inner');
    assert.equal(joinStats(bus).how, 'inner');
  });
});
