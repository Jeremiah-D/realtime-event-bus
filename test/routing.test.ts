import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type BusMessage,
  type TraceSpan,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function collect() {
  const messages: BusMessage[] = [];
  return {
    messages,
    handler: (msg: BusMessage) => {
      messages.push(msg);
    },
  };
}

test('basic route: admitted src messages forward to dst as normal dst publishes', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('orders.raw', 'orders.clean');
  const src = collect();
  const dst = collect();
  bus.subscribe('orders.raw', src.handler);
  bus.subscribe('orders.clean', dst.handler);
  const accepted = bus.publish('orders.raw', { id: 1 });
  await flush();
  // The source publish is unaffected by the forward: it reports its own
  // accepted count.
  assert.equal(accepted, 1);
  assert.equal(src.messages.length, 1);
  assert.equal(dst.messages.length, 1);
  assert.deepEqual(dst.messages[0].payload, { id: 1 });
  assert.equal(dst.messages[0].topic, 'orders.clean');
  // The forwarded message consumes the DESTINATION's sequence: each side
  // keeps its own per-topic seq.
  assert.equal(src.messages[0].seq, 1);
  assert.equal(dst.messages[0].seq, 1);
  bus.publish('orders.raw', { id: 2 });
  await flush();
  assert.equal(dst.messages[1].seq, 2);
  assert.deepEqual(bus.getStats().routes, [
    { src: 'orders.raw', dst: 'orders.clean', predicate: false, forwarded: 2 },
  ]);
});

test('unrouted topics are unaffected when routes exist', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('a', 'b');
  const other = collect();
  bus.subscribe('other', other.handler);
  bus.publish('other', 'x');
  await flush();
  assert.equal(other.messages.length, 1);
  assert.deepEqual(bus.getStats().routes, [
    { src: 'a', dst: 'b', predicate: false, forwarded: 0 },
  ]);
});

test('forwarded publish goes through dst schema admission', async () => {
  const rejections: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({ onAdmissionRejected: (e) => rejections.push(e) });
  bus.setTopicSchema('b', (p) => typeof p === 'object' && p !== null && 'ok' in p);
  bus.setTopicRoute('a', 'b');
  const dst = collect();
  bus.subscribe('b', dst.handler);
  bus.publish('a', { nope: true });
  await flush();
  assert.equal(dst.messages.length, 0);
  assert.equal(rejections.length, 1);
  assert.equal(rejections[0].topic, 'b');
  assert.equal(rejections[0].reason, 'schema');
  // The route fired (the attempt counts) even though dst rejected it.
  assert.equal(bus.getStats().routes[0].forwarded, 1);
  bus.publish('a', { ok: 1 });
  await flush();
  assert.equal(dst.messages.length, 1);
  assert.deepEqual(dst.messages[0].payload, { ok: 1 });
  assert.equal(bus.getStats().routes[0].forwarded, 2);
});

test('forwarded publish burns dst rate-limit budget', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRateLimit('b', 1, { burst: 1 });
  bus.setTopicRoute('a', 'b');
  const dst = collect();
  bus.subscribe('b', dst.handler);
  bus.publish('a', 1);
  bus.publish('a', 2);
  await flush();
  assert.equal(dst.messages.length, 1);
  assert.equal(dst.messages[0].payload, 1);
  const bTopic = bus.getStats().topics.find((t) => t.topic === 'b');
  assert.equal(bTopic?.rateLimitedMessages, 1);
  assert.equal(bus.getStats().routes[0].forwarded, 2);
});

test('forwarded publish is ACL-gated on dst', async () => {
  const rejections: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'b', publish: 'deny' }] },
    onAdmissionRejected: (e) => rejections.push(e),
  });
  bus.setTopicRoute('a', 'b');
  const dst = collect();
  bus.subscribe('b', dst.handler);
  // The source side is allowed; only the dst forward is denied.
  bus.publish('a', 'x');
  await flush();
  assert.equal(dst.messages.length, 0);
  assert.equal(rejections.length, 1);
  assert.equal(rejections[0].topic, 'b');
  assert.equal(rejections[0].reason, 'acl');
});

test('traceId propagates to the forwarded message (same end-to-end trace)', async () => {
  const spans: TraceSpan[] = [];
  const bus = new EventBus({ trace: { onTraceSpan: (s) => spans.push(s) } });
  bus.setTopicRoute('a', 'b');
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  bus.publish('a', 'x');
  await flush();
  const roots = spans.filter((s) => s.name === 'bus.publish');
  const aRoot = roots.filter((s) => s.attrs.topic === 'a');
  const bRoot = roots.filter((s) => s.attrs.topic === 'b');
  assert.equal(aRoot.length, 1);
  assert.equal(bRoot.length, 1);
  assert.equal(bRoot[0].traceId, aRoot[0].traceId);
  // The forwarded root is a child span inside the source trace.
  assert.ok(bRoot[0].parentId);
});

test('TTL deadline is carried over verbatim, never reset by routing', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('a', 500); // deadline t=1500
  bus.setTopicTtl('b', 60_000); // would be t=61000 if routing reset the TTL
  bus.setTopicRoute('a', 'b');
  const src = collect();
  const dst = collect();
  bus.subscribe('a', src.handler);
  bus.subscribe('b', dst.handler);
  bus.publish('a', 'x'); // published at t=1000
  clock.nowMs = 1_600; // past the src deadline, long before dst's hypothetical one
  await flush();
  assert.deepEqual(src.messages, []);
  assert.deepEqual(dst.messages, []);
  assert.equal(bus.getStats().expiredMessages, 2);
});

test('no src deadline: dst TTL rules apply normally to the forward', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('b', 500); // only dst has a rule
  bus.setTopicRoute('a', 'b');
  const dst = collect();
  bus.subscribe('b', dst.handler);
  bus.publish('a', 'x'); // no deadline on src; forward stamps dst's rule at t=1000
  clock.nowMs = 1_600;
  await flush();
  assert.deepEqual(dst.messages, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('registration rejects self-routes and cycles', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicRoute('a', 'a'), RangeError);
  bus.setTopicRoute('a', 'b');
  assert.throws(() => bus.setTopicRoute('b', 'a'), RangeError);
  bus.setTopicRoute('b', 'c');
  assert.throws(() => bus.setTopicRoute('c', 'a'), RangeError);
  // Longer chains: with a -> b -> c -> d live, both d -> b and d -> a
  // would close a forwarding loop.
  bus.setTopicRoute('c', 'd');
  assert.throws(() => bus.setTopicRoute('d', 'b'), RangeError);
  assert.throws(() => bus.setTopicRoute('d', 'a'), RangeError);
  // Failed registrations mutate nothing.
  assert.deepEqual(
    bus.getStats().routes.map((r) => [r.src, r.dst]),
    [
      ['a', 'b'],
      ['b', 'c'],
      ['c', 'd'],
    ],
  );
});

test('registration validates inputs', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicRoute('', 'b'), RangeError);
  assert.throws(() => bus.setTopicRoute('a', ''), RangeError);
  assert.throws(
    () => bus.setTopicRoute('a', 'b', { predicate: 'yes' as never }),
    RangeError,
  );
  assert.deepEqual(bus.getStats().routes, []);
});

test('routed messages never trigger routing again: one hop per message', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('a', 'b');
  bus.setTopicRoute('b', 'c');
  const b = collect();
  const c = collect();
  bus.subscribe('b', b.handler);
  bus.subscribe('c', c.handler);
  bus.publish('a', 'x');
  await flush();
  assert.equal(b.messages.length, 1);
  assert.equal(c.messages.length, 0);
  assert.equal(bus.getStats().routes.find((r) => r.src === 'b')?.forwarded, 0);
  // A direct publish marked routed opts out of routing too.
  bus.publish('b', 'y', { routed: true });
  await flush();
  assert.equal(b.messages.length, 2);
  assert.equal(c.messages.length, 0);
  // Without the mark, a direct publish to b forwards normally.
  bus.publish('b', 'z');
  await flush();
  assert.equal(c.messages.length, 1);
  assert.deepEqual(c.messages[0].payload, 'z');
});

test('predicate filters which messages forward', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('a', 'b', {
    predicate: (payload) => (payload as { level?: string }).level === 'high',
  });
  const dst = collect();
  bus.subscribe('b', dst.handler);
  bus.publish('a', { level: 'low' });
  await flush();
  assert.equal(dst.messages.length, 0);
  bus.publish('a', { level: 'high' });
  await flush();
  assert.equal(dst.messages.length, 1);
  assert.deepEqual(bus.getStats().routes, [
    { src: 'a', dst: 'b', predicate: true, forwarded: 1 },
  ]);
});

test('predicate receives topic, seq and messageId metadata', () => {
  const bus = new EventBus();
  const seen: Array<{ topic: string; seq: number; messageId?: string }> = [];
  bus.setTopicRoute('a', 'b', {
    predicate: (_payload, meta) => {
      seen.push({ ...meta });
      return true;
    },
  });
  bus.publish('a', 'x', { messageId: 'm1' });
  bus.publish('a', 'y');
  assert.deepEqual(seen, [
    { topic: 'a', seq: 1, messageId: 'm1' },
    { topic: 'a', seq: 2 },
  ]);
});

test('messageId rides along to the forwarded message', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('a', 'b');
  const dst = collect();
  bus.subscribe('b', dst.handler);
  bus.publish('a', 'x', { messageId: 'm1' });
  await flush();
  assert.equal(dst.messages.length, 1);
  assert.equal(dst.messages[0].messageId, 'm1');
});

test('clearTopicRoute removes the route', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('a', 'b');
  assert.equal(bus.clearTopicRoute('a'), true);
  assert.equal(bus.clearTopicRoute('a'), false);
  assert.deepEqual(bus.getStats().routes, []);
  const dst = collect();
  bus.subscribe('b', dst.handler);
  bus.publish('a', 'x');
  await flush();
  assert.equal(dst.messages.length, 0);
});

test('re-registering a src replaces the route and resets its count', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('a', 'b');
  const b = collect();
  const c = collect();
  bus.subscribe('b', b.handler);
  bus.subscribe('c', c.handler);
  bus.publish('a', 1);
  await flush();
  bus.setTopicRoute('a', 'c');
  bus.publish('a', 2);
  await flush();
  assert.equal(b.messages.length, 1);
  assert.equal(c.messages.length, 1);
  assert.deepEqual(bus.getStats().routes, [
    { src: 'a', dst: 'c', predicate: false, forwarded: 1 },
  ]);
});

test('routes match the alias-resolved topic', async () => {
  const bus = new EventBus();
  bus.setTopicAlias('old', 'new');
  bus.setTopicRoute('new', 'b');
  bus.setTopicRoute('old', 'b2');
  const dst = collect();
  const dst2 = collect();
  bus.subscribe('b', dst.handler);
  bus.subscribe('b2', dst2.handler);
  // Publishes to 'old' resolve to 'new' before every gate — routing keys
  // off the real topic, so the 'new' route fires and the 'old' one never
  // sees the message.
  bus.publish('old', 'x');
  await flush();
  assert.equal(dst.messages.length, 1);
  assert.equal(dst2.messages.length, 0);
  assert.deepEqual(
    bus.getStats().routes.map((r) => [r.src, r.forwarded]),
    [
      ['new', 1],
      ['old', 0],
    ],
  );
});
