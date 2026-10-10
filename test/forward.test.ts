import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type BusMessage,
  type TraceSpan,
} from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

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

test('basic forward: admitted src messages reach the dst bus as normal dst publishes', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  src.forward(dst, 'orders.raw');
  const srcSub = collect();
  const dstSub = collect();
  src.subscribe('orders.raw', srcSub.handler);
  dst.subscribe('orders.raw', dstSub.handler);
  const accepted = src.publish('orders.raw', { id: 1 });
  await flush();
  // The source publish is unaffected by the forward: it reports its own
  // accepted count.
  assert.equal(accepted, 1);
  assert.equal(srcSub.messages.length, 1);
  assert.equal(dstSub.messages.length, 1);
  assert.deepEqual(dstSub.messages[0].payload, { id: 1 });
  assert.equal(dstSub.messages[0].topic, 'orders.raw');
  // Each bus keeps its own per-topic sequence.
  assert.equal(srcSub.messages[0].seq, 1);
  assert.equal(dstSub.messages[0].seq, 1);
  src.publish('orders.raw', { id: 2 });
  await flush();
  assert.equal(dstSub.messages[1].seq, 2);
  assert.deepEqual(src.getStats().forwards, [
    { srcPattern: 'orders.raw', forwarded: 2 },
  ]);
});

test('dstTopic remap', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  src.forward(dst, 'orders.raw', { dstTopic: 'orders.clean' });
  const dstSub = collect();
  dst.subscribe('orders.clean', dstSub.handler);
  src.publish('orders.raw', { id: 7 });
  await flush();
  assert.equal(dstSub.messages.length, 1);
  assert.equal(dstSub.messages[0].topic, 'orders.clean');
  assert.deepEqual(dstSub.messages[0].payload, { id: 7 });
  assert.deepEqual(src.getStats().forwards, [
    { srcPattern: 'orders.raw', dstTopic: 'orders.clean', forwarded: 1 },
  ]);
});

test('wildcard srcPattern', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  src.forward(dst, 'orders.*');
  const dstSub = collect();
  dst.subscribe('orders.*', dstSub.handler);
  src.publish('orders.a', 'a');
  src.publish('orders.b', 'b');
  src.publish('other', 'x');
  await flush();
  assert.deepEqual(
    dstSub.messages.map((m) => m.payload),
    ['a', 'b'],
  );
  assert.equal(src.getStats().forwards[0].forwarded, 2);
});

test('srcPattern matches the alias-resolved topic', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  src.setTopicAlias('orders.old', 'orders.new');
  src.forward(dst, 'orders.new');
  const dstSub = collect();
  dst.subscribe('orders.new', dstSub.handler);
  src.publish('orders.old', 'v');
  await flush();
  assert.equal(dstSub.messages.length, 1);
  assert.equal(dstSub.messages[0].topic, 'orders.new');
  assert.equal(src.getStats().forwards[0].forwarded, 1);
});

test('routed messages never forward again: one hop per message', async () => {
  const a = new EventBus();
  const b = new EventBus();
  const c = new EventBus();
  a.forward(b, 't');
  b.forward(c, 't');
  const bSub = collect();
  const cSub = collect();
  b.subscribe('t', bSub.handler);
  c.subscribe('t', cSub.handler);
  // A message forwarded A→B arrives on B marked routed: B's own forward
  // rule to C must not fire.
  a.publish('t', 'from-a');
  await flush();
  assert.equal(bSub.messages.length, 1);
  assert.equal(cSub.messages.length, 0);
  assert.deepEqual(a.getStats().forwards, [{ srcPattern: 't', forwarded: 1 }]);
  assert.deepEqual(b.getStats().forwards, [{ srcPattern: 't', forwarded: 0 }]);
  // A direct (non-routed) publish on B still forwards to C.
  b.publish('t', 'from-b');
  await flush();
  assert.equal(cSub.messages.length, 1);
  assert.equal(cSub.messages[0].payload, 'from-b');
  assert.equal(b.getStats().forwards[0].forwarded, 1);
});

test('direct cycle is rejected and leaves no rule behind', () => {
  const a = new EventBus();
  const b = new EventBus();
  a.forward(b, 't');
  assert.throws(() => b.forward(a, 't'), RangeError);
  assert.deepEqual(b.getStats().forwards, []);
  assert.deepEqual(a.getStats().forwards, [{ srcPattern: 't', forwarded: 0 }]);
});

test('transitive cycle is rejected', () => {
  const a = new EventBus();
  const b = new EventBus();
  const c = new EventBus();
  a.forward(b, 't');
  b.forward(c, 't');
  // A→B→C→A would close the loop.
  assert.throws(() => c.forward(a, 't'), RangeError);
  assert.deepEqual(c.getStats().forwards, []);
  // A diamond that does NOT reach back is fine: C→D is allowed.
  const d = new EventBus();
  c.forward(d, 't');
  assert.deepEqual(c.getStats().forwards, [{ srcPattern: 't', forwarded: 0 }]);
});

test('self-forward is rejected', () => {
  const a = new EventBus();
  assert.throws(() => a.forward(a, 't'), RangeError);
  assert.deepEqual(a.getStats().forwards, []);
});

test('registration validation errors', () => {
  const a = new EventBus();
  const b = new EventBus();
  assert.throws(() => a.forward(null as never, 't'), TypeError);
  assert.throws(() => a.forward({} as never, 't'), TypeError);
  assert.throws(() => a.forward('x' as never, 't'), TypeError);
  assert.throws(() => a.forward(b, ''), RangeError);
  assert.throws(() => a.forward(b, 42 as never), RangeError);
  assert.throws(() => a.forward(b, 't', { dstTopic: '' }), RangeError);
  assert.throws(() => a.forward(b, 't', { dstTopic: 42 as never }), RangeError);
  // Every failed registration leaves the table untouched.
  assert.deepEqual(a.getStats().forwards, []);
});

test('dst-side admission gates: ACL deny blocks delivery but the forward is counted', async () => {
  const rejections: AdmissionRejectionEvent[] = [];
  const src = new EventBus();
  const dst = new EventBus({
    acl: { rules: [{ pattern: 'secret.**', publish: 'deny' }] },
    onAdmissionRejected: (e) => rejections.push(e),
  });
  src.forward(dst, 'orders', { dstTopic: 'secret.orders' });
  const dstSub = collect();
  dst.subscribe('secret.orders', dstSub.handler);
  src.publish('orders', { id: 1 });
  await flush();
  assert.equal(dstSub.messages.length, 0);
  assert.equal(rejections.length, 1);
  assert.equal(rejections[0].topic, 'secret.orders');
  assert.equal(rejections[0].reason, 'acl');
  // The forward was attempted — the dst rejection does not erase the count.
  assert.deepEqual(src.getStats().forwards, [
    { srcPattern: 'orders', dstTopic: 'secret.orders', forwarded: 1 },
  ]);
});

test('dst-side schema rejection is counted too', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  dst.setTopicSchema('b', (p) => typeof p === 'object' && p !== null && 'ok' in p);
  src.forward(dst, 'a', { dstTopic: 'b' });
  const dstSub = collect();
  dst.subscribe('b', dstSub.handler);
  src.publish('a', { nope: true });
  await flush();
  assert.equal(dstSub.messages.length, 0);
  assert.equal(src.getStats().forwards[0].forwarded, 1);
});

test('traceId continues the source trace across the forward', async () => {
  const srcSpans: TraceSpan[] = [];
  const dstSpans: TraceSpan[] = [];
  const src = new EventBus({ trace: { onTraceSpan: (s) => srcSpans.push(s) } });
  const dst = new EventBus({ trace: { onTraceSpan: (s) => dstSpans.push(s) } });
  src.forward(dst, 'a');
  src.subscribe('a', () => {});
  dst.subscribe('a', () => {});
  src.publish('a', 'x');
  await flush();
  const srcRoots = srcSpans.filter((s) => s.name === 'bus.publish');
  const dstRoots = dstSpans.filter((s) => s.name === 'bus.publish');
  assert.equal(srcRoots.length, 1);
  assert.equal(dstRoots.length, 1);
  assert.equal(dstRoots[0].traceId, srcRoots[0].traceId);
  assert.ok(dstRoots[0].parentId);
});

test('key and messageId pass through to the destination', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-forward-'));
  const src = new EventBus();
  const dst = new EventBus({ durableLogDir: dir });
  src.forward(dst, 't');
  const dstSub = collect();
  dst.subscribe('t', dstSub.handler);
  src.publish('t', 'payload', { key: 'k1', messageId: 'mid-1' });
  await flush();
  assert.equal(dstSub.messages.length, 1);
  assert.equal(dstSub.messages[0].messageId, 'mid-1');
  // The key rides the destination's durable-log record.
  const records = DurableTopicLog.open({ dir }).readSince('t', 0);
  assert.equal(records.length, 1);
  assert.equal(records[0].key, 'k1');
});

test('TTL deadline is carried verbatim, never reset by a forward', async () => {
  const clock = controllableClock();
  const src = new EventBus({ now: clock.now });
  const dst = new EventBus({ now: clock.now });
  src.setTopicTtl('t', 100); // published at t=1000: deadline t=1100
  dst.setTopicTtl('t', 10_000); // a reset would have landed at t=11000
  src.forward(dst, 't');
  const dstSub = collect();
  dst.subscribe('t', dstSub.handler);
  src.publish('t', 'x');
  clock.nowMs = 1_200; // past the source deadline, before the dst rule's
  await flush();
  assert.deepEqual(
    dstSub.messages.map((m) => m.payload),
    [],
  );
  assert.equal(dst.getStats().expiredMessages, 1);
});

test('without a source deadline the destination TTL rules apply normally', async () => {
  const clock = controllableClock();
  const src = new EventBus({ now: clock.now });
  const dst = new EventBus({ now: clock.now });
  dst.setTopicTtl('t', 100); // the forward stamps no deadline of its own
  src.forward(dst, 't');
  const dstSub = collect();
  dst.subscribe('t', dstSub.handler);
  src.publish('t', 'live');
  await flush(); // t=1000 < deadline t=1100
  assert.deepEqual(
    dstSub.messages.map((m) => m.payload),
    ['live'],
  );
  src.publish('t', 'late');
  clock.nowMs = 1_200;
  await flush();
  assert.equal(dstSub.messages.length, 1); // the second message expired on dst rules
  assert.equal(dst.getStats().expiredMessages, 1);
});

test('clearForward removes the rule', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  src.forward(dst, 'a');
  src.forward(dst, 'b');
  const dstSub = collect();
  dst.subscribe('*', dstSub.handler);
  src.publish('a', 'x');
  await flush();
  assert.equal(dstSub.messages.length, 1);
  assert.equal(src.clearForward(dst, 'a'), true);
  src.publish('a', 'y');
  src.publish('b', 'z');
  await flush();
  assert.deepEqual(
    dstSub.messages.map((m) => m.payload),
    ['x', 'z'],
  );
  assert.equal(src.clearForward(dst, 'a'), false);
  assert.equal(src.clearForward(dst, 'nope'), false);
  assert.equal(src.clearForward(new EventBus(), 'b'), false);
});

test('re-registering a rule replaces it and resets the forward count', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  src.forward(dst, 'a');
  const dstSub = collect();
  dst.subscribe('*', dstSub.handler);
  src.publish('a', 'x');
  await flush();
  assert.equal(src.getStats().forwards[0].forwarded, 1);
  src.forward(dst, 'a', { dstTopic: 'b' });
  assert.deepEqual(src.getStats().forwards, [
    { srcPattern: 'a', dstTopic: 'b', forwarded: 0 },
  ]);
  src.publish('a', 'y');
  await flush();
  assert.deepEqual(
    dstSub.messages.map((m) => [m.topic, m.payload]),
    [
      ['a', 'x'],
      ['b', 'y'],
    ],
  );
  assert.equal(src.getStats().forwards[0].forwarded, 1);
});

test('getStats().forwards is a snapshot: mutating it does not affect the bus', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  src.forward(dst, 'a', { dstTopic: 'b' });
  const snap = src.getStats().forwards;
  assert.deepEqual(snap, [{ srcPattern: 'a', dstTopic: 'b', forwarded: 0 }]);
  snap[0].forwarded = 99;
  (snap[0] as { dstTopic?: string }).dstTopic = 'hacked';
  snap.length = 0;
  dst.subscribe('b', () => {});
  src.publish('a', 'x');
  await flush();
  assert.deepEqual(src.getStats().forwards, [
    { srcPattern: 'a', dstTopic: 'b', forwarded: 1 },
  ]);
});
