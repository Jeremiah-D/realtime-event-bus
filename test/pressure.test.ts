import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type AdmissionRejectReason,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  return {
    events,
    hook: (event: AdmissionRejectionEvent) => {
      events.push(event);
    },
  };
}

test('publish sheds atomically at the budget boundary', async () => {
  const sink = makeSink();
  const bus = new EventBus({ maxPendingMessages: 5, onAdmissionRejected: sink.hook });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => {
    received.push(m.payload);
  });
  // Synchronous publishes pile backlog (the flush runs on a microtask):
  // the first 5 fit (pending 0..4 + width 1 <= 5), the rest shed.
  const results: number[] = [];
  for (let i = 0; i < 8; i++) results.push(bus.publish('t', i));
  assert.deepEqual(results, [1, 1, 1, 1, 1, 0, 0, 0]);
  const stats = bus.getStats();
  assert.equal(stats.pendingPressure, 3);
  assert.equal(stats.rejectedMessages, 3);
  // The shed surfaces on the EB-39 channel with reason 'pressure'.
  assert.equal(sink.events.length, 3);
  for (const event of sink.events) {
    assert.equal(event.topic, 't');
    assert.equal(event.reason, 'pressure');
  }
  // No sequence number consumed by the shed: the admitted backlog drains
  // gap-free and the next publish continues the sequence.
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
  assert.equal(bus.getStats().sequenceGaps, 0);
  assert.equal(bus.publish('t', 'again'), 1);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 'again']);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('a publish that matches nobody is never shed, even at budget 0', () => {
  const bus = new EventBus({ maxPendingMessages: 0 });
  bus.subscribe('a', () => {});
  // Width 0: nothing would queue, so the budget cannot refuse it.
  bus.publish('b', 'x');
  assert.equal(bus.getStats().pendingPressure, 0);
  // But a publish that would queue is shed at budget 0.
  assert.equal(bus.publish('a', 'y'), 0);
  assert.equal(bus.getStats().pendingPressure, 1);
});

test('default (absent) budget is unlimited — zero behavior change', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => {
    received.push(m.payload);
  });
  for (let i = 0; i < 50; i++) assert.equal(bus.publish('t', i), 1);
  await flush();
  assert.equal(received.length, 50);
  assert.equal(bus.getStats().pendingPressure, 0);
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('publishAtomic is all-or-nothing against the budget', async () => {
  const sink = makeSink();
  const bus = new EventBus({ maxPendingMessages: 5, onAdmissionRejected: sink.hook });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => {
    received.push(m.payload);
  });
  // Batch width 3 fits the empty budget: admitted whole.
  const first = bus.publishAtomic([
    { topic: 't', payload: 'a' },
    { topic: 't', payload: 'b' },
    { topic: 't', payload: 'c' },
  ]);
  assert.equal(first.published, 3);
  assert.equal(first.rejected, undefined);
  // Batch width 3 no longer fits (pending 3 + 3 > 5): rejected whole —
  // zero partial fan-out, nothing committed.
  const second = bus.publishAtomic([
    { topic: 't', payload: 'd' },
    { topic: 't', payload: 'e' },
    { topic: 't', payload: 'f' },
  ]);
  assert.equal(second.published, 0);
  assert.deepEqual(second.rejected, { index: 0, topic: 't', reason: 'pressure' });
  assert.equal(bus.getStats().pendingPressure, 1);
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].reason, 'pressure');
  await flush();
  assert.deepEqual(received, ['a', 'b', 'c']);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('setMaxPendingMessages retunes at runtime; lowering never discards in-flight backlog', async () => {
  const bus = new EventBus({ maxPendingMessages: 10 });
  const received: unknown[] = [];
  bus.subscribe('t', (m) => {
    received.push(m.payload);
  });
  for (let i = 0; i < 8; i++) assert.equal(bus.publish('t', i), 1);
  // Lower below the in-flight pile-up: nothing is discarded, but new
  // publishes shed until the backlog drains under the new budget.
  bus.setMaxPendingMessages(5);
  assert.equal(bus.publish('t', 'shed-me'), 0);
  assert.equal(bus.getStats().pendingPressure, 1);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5, 6, 7]);
  // Drained: the budget admits again.
  assert.equal(bus.publish('t', 'fits'), 1);
  // Raising (or clearing) restores headroom.
  bus.setMaxPendingMessages(undefined);
  for (let i = 0; i < 20; i++) assert.equal(bus.publish('t', i), 1);
  assert.equal(bus.getStats().pendingPressure, 1);
  await flush();
});

test('setMaxPendingMessages and the constructor reject illegal values', () => {
  for (const bad of [-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, '5' as unknown as number]) {
    assert.throws(() => new EventBus({ maxPendingMessages: bad }), RangeError);
    const bus = new EventBus();
    assert.throws(() => bus.setMaxPendingMessages(bad), RangeError);
  }
  // 0 is legal (sheds everything that would queue), undefined clears.
  const bus = new EventBus({ maxPendingMessages: 0 });
  bus.setMaxPendingMessages(undefined);
});

test('a consumer group counts one delivery against the budget (assignee queue)', async () => {
  const bus = new EventBus({ maxPendingMessages: 1 });
  const seenA: unknown[] = [];
  const seenB: unknown[] = [];
  bus.subscribeToGroup('workers', 'jobs.*', (m) => {
    seenA.push(m.payload);
  });
  bus.subscribeToGroup('workers', 'jobs.*', (m) => {
    seenB.push(m.payload);
  });
  // Two members compete, but one publish is one assignee delivery:
  // width 1 fits the empty budget.
  assert.equal(bus.publish('jobs.1', 'w1'), 1);
  // The assignee's queue now holds it: the next publish would exceed.
  assert.equal(bus.publish('jobs.2', 'w2'), 0);
  assert.equal(bus.getStats().pendingPressure, 1);
  await flush();
  assert.equal(seenA.length + seenB.length, 1);
});

test('the budget spans subscribers: width is the sum of matched deliveries', async () => {
  const bus = new EventBus({ maxPendingMessages: 3 });
  bus.subscribe('t', () => {});
  bus.subscribe('t', () => {});
  // Width 2 per publish: two fit (pending 0+2, 2+2 <= 3 is false — only
  // one fits: pending 0 + 2 <= 3, then 2 + 2 = 4 > 3 sheds).
  assert.equal(bus.publish('t', 'a'), 2);
  assert.equal(bus.publish('t', 'b'), 0);
  assert.equal(bus.getStats().pendingPressure, 1);
  await flush();
});

test("'pressure' is a first-class admission-rejection reason", () => {
  const reasons: AdmissionRejectReason[] = ['pressure'];
  assert.ok(reasons.includes('pressure'));
  const sink = makeSink();
  const bus = new EventBus({ maxPendingMessages: 1, onAdmissionRejected: sink.hook });
  bus.subscribe('t', () => {});
  bus.publish('t', 'a');
  bus.publish('t', 'b'); // shed
  const [event] = sink.events;
  assert.equal(event.reason, 'pressure');
  assert.equal(typeof event.payloadBytes, 'number');
  assert.equal(typeof event.at, 'number');
});
