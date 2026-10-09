import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type AdmissionRejectReason,
} from '../src/bus.ts';

/** Injectable clock so the hook's `at` timestamp is deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

/** Collects hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  const hook = (event: AdmissionRejectionEvent) => {
    events.push(event);
  };
  return { events, hook };
}

test('constructor rejects a non-function onAdmissionRejected', () => {
  assert.throws(
    () => new EventBus({ onAdmissionRejected: 42 as unknown as () => void }),
    RangeError,
  );
});

test('without the hook, admission rejections count stats only (backward compatible)', () => {
  const bus = new EventBus();
  bus.setTopicSchema('orders', (p) => typeof p === 'object' && p !== null);
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  bus.publish('orders', 'bad');
  bus.publish('hot', 'a');
  bus.publish('hot', 'b'); // shed
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 1);
  assert.equal(stats.rateLimitedMessages, 1);
  // No hook configured: nothing threw, nothing else changed.
});

test('schema rejection fires the hook with a full snapshot', () => {
  const clock = makeClock(1_700_000_000_000);
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.setTopicSchema('orders', (p) => typeof p === 'object' && p !== null);
  assert.equal(bus.publish('orders', 'bad'), 0);
  assert.equal(sink.events.length, 1);
  const [event] = sink.events;
  assert.equal(event.topic, 'orders');
  assert.equal(event.reason, 'schema');
  assert.equal(event.payloadBytes, Buffer.byteLength('"bad"', 'utf8'));
  assert.equal(event.at, 1_700_000_000_000);
  // The event and the stats counter stay reconcilable.
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('rate-limit shed fires the hook with reason rate-limit', () => {
  const clock = makeClock(5_000);
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  bus.publish('hot', { n: 1 }); // takes the single token
  assert.equal(bus.publish('hot', { n: 2 }), 0); // shed
  assert.equal(sink.events.length, 1);
  const [event] = sink.events;
  assert.equal(event.topic, 'hot');
  assert.equal(event.reason, 'rate-limit');
  assert.equal(event.payloadBytes, Buffer.byteLength('{"n":2}', 'utf8'));
  assert.equal(event.at, 5_000);
  assert.equal(bus.getStats().rateLimitedMessages, 1);
});

test('idempotency duplicate suppression fires the hook with reason duplicate', () => {
  const clock = makeClock();
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.subscribe('pay.orders', () => {});
  const first = bus.publishIdempotent('pay.orders', { id: 1 }, { messageId: 'm1' });
  assert.deepEqual(first, { duplicate: false, accepted: 1 });
  assert.equal(sink.events.length, 0); // admitted publishes never fire
  const retry = bus.publishIdempotent('pay.orders', { id: 1 }, { messageId: 'm1' });
  assert.deepEqual(retry, { duplicate: true, accepted: 0 });
  assert.equal(sink.events.length, 1);
  const [event] = sink.events;
  assert.equal(event.topic, 'pay.orders');
  assert.equal(event.reason, 'duplicate');
  assert.equal(event.payloadBytes, Buffer.byteLength('{"id":1}', 'utf8'));
  assert.equal(bus.getStats().duplicateMessages, 1);
});

test('publishAtomic schema rejection counts and fires the hook once', () => {
  const clock = makeClock(7_000);
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.setTopicSchema('orders', (p) => typeof p === 'object' && p !== null);
  const res = bus.publishAtomic([
    { topic: 'orders', payload: { id: 1 } },
    { topic: 'orders', payload: 'bad' }, // rejected
  ]);
  assert.deepEqual(res, { published: 0, rejected: { index: 1, topic: 'orders', reason: 'schema' } });
  assert.equal(sink.events.length, 1);
  const [event] = sink.events;
  assert.equal(event.topic, 'orders');
  assert.equal(event.reason, 'schema');
  assert.equal(event.at, 7_000);
  assert.equal(bus.getStats().rejectedMessages, 1);
  // Delivery-side state stayed untouched: no seq consumed.
  assert.equal(bus.getStats().totalPublished, 0);
});

test('publishAtomic rate-limit rejection counts and fires the hook once', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  bus.setTopicRateLimit('hot', 10, { burst: 1 });
  const res = bus.publishAtomic([
    { topic: 'hot', payload: 'a' },
    { topic: 'hot', payload: 'b' }, // overdrafts the shadow budget
  ]);
  assert.deepEqual(res, { published: 0, rejected: { index: 1, topic: 'hot', reason: 'rate-limit' } });
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].reason, 'rate-limit');
  assert.equal(sink.events[0].topic, 'hot');
  assert.equal(bus.getStats().rateLimitedMessages, 1);
});

test('publishDelayed schema rejection fires the hook', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  bus.setTopicSchema('orders', (p) => typeof p === 'object' && p !== null);
  assert.equal(bus.publishDelayed('orders', 'bad', { delayMs: 1_000 }), undefined);
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].reason, 'schema');
  assert.equal(sink.events[0].topic, 'orders');
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('a throwing hook is isolated: the publish path is unaffected', () => {
  const bus = new EventBus({
    onAdmissionRejected: () => {
      throw new Error('broken observer');
    },
  });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicSchema('t', (p) => typeof p === 'string');
  // Rejection path: counted, hook throw swallowed, no propagation.
  assert.equal(bus.publish('t', 42), 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  // Admission path: the bus still publishes and delivers normally.
  assert.equal(bus.publish('t', 'ok'), 1);
  return new Promise<void>((resolve) => {
    setImmediate(() => {
      assert.deepEqual(received, ['ok']);
      resolve();
    });
  });
});

test('unserializable payloads report payloadBytes 0', () => {
  const sink = makeSink();
  const bus = new EventBus({ onAdmissionRejected: sink.hook });
  bus.setTopicSchema('t', () => false); // rejects everything
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  assert.equal(bus.publish('t', circular), 0);
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].payloadBytes, 0);
});

test('hook events reconcile exactly with stats across all gates', () => {
  const sink = makeSink();
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.setTopicSchema('orders', (p) => typeof p === 'object' && p !== null);
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  bus.subscribe('pay', () => {});
  // 2 schema rejections
  bus.publish('orders', 'bad-1');
  bus.publish('orders', 'bad-2');
  // 2 rate-limit sheds
  bus.publish('hot', 'first'); // admitted
  bus.publish('hot', 'shed-1');
  bus.publish('hot', 'shed-2');
  // 2 duplicate suppressions
  bus.publishIdempotent('pay', { id: 1 }, { messageId: 'm1' });
  bus.publishIdempotent('pay', { id: 1 }, { messageId: 'm1' });
  bus.publishIdempotent('pay', { id: 2 }, { messageId: 'm2' });
  bus.publishIdempotent('pay', { id: 2 }, { messageId: 'm2' });
  // 1 atomic schema rejection + 1 atomic rate-limit rejection
  bus.publishAtomic([{ topic: 'orders', payload: 'bad' }]);
  bus.publishAtomic([
    { topic: 'hot', payload: 'a' },
    { topic: 'hot', payload: 'b' },
  ]);
  const stats = bus.getStats();
  const byReason = new Map<AdmissionRejectReason, number>();
  for (const event of sink.events) {
    byReason.set(event.reason, (byReason.get(event.reason) ?? 0) + 1);
  }
  assert.equal(sink.events.length, 8);
  assert.equal(byReason.get('schema'), stats.rejectedMessages);
  assert.equal(stats.rejectedMessages, 3);
  assert.equal(byReason.get('rate-limit'), stats.rateLimitedMessages);
  assert.equal(stats.rateLimitedMessages, 3);
  assert.equal(byReason.get('duplicate'), stats.duplicateMessages);
  assert.equal(stats.duplicateMessages, 2);
});
