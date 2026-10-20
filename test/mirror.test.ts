import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { resolveMirrorRule } from '../src/mirror.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function collect() {
  const messages: BusMessage[] = [];
  return {
    messages,
    handler: (msg: BusMessage) => {
      messages.push(msg);
    },
  };
}

test('basic mirror: rename rule, provenance, and stats', async () => {
  const src = new EventBus({ busId: 'src-a' });
  const dst = new EventBus({ busId: 'dst-b' });
  const dstSub = collect();
  dst.subscribe('mirror.orders.raw', dstSub.handler);
  const handle = dst.mirror(src, [{ srcPattern: 'orders.raw', rename: 'mirror.orders.raw' }]);
  assert.equal(handle.closed, false);

  src.publish('orders.raw', { id: 1 });
  src.publish('orders.raw', { id: 2 });
  await flush();

  assert.equal(dstSub.messages.length, 2);
  const first = dstSub.messages[0];
  assert.equal(first.topic, 'mirror.orders.raw');
  assert.deepEqual(first.payload, { id: 1 });
  // The destination draws its own per-topic seq; the origin seq rides
  // the provenance for downstream gap detection.
  assert.equal(first.seq, 1);
  assert.deepEqual(first.mirror, {
    busId: 'src-a',
    topic: 'orders.raw',
    seq: 1,
    trail: ['src-a'],
  });
  assert.equal(first.epoch, 'mirror:src-a');

  const stats = dst.getStats();
  assert.equal(stats.mirroredMessages, 2);
  assert.deepEqual(stats.mirrors, [
    {
      srcPattern: 'orders.raw',
      dstTemplate: 'mirror.orders.raw',
      mirrored: 2,
      loopsPrevented: 0,
      mirroredThrottled: 0,
    },
  ]);
  assert.deepEqual(handle.rules(), stats.mirrors);
  // The source bus is untouched by the mirror: no provenance leaks back.
  assert.equal(src.getStats().mirroredMessages, 0);
  assert.deepEqual(src.getStats().mirrors, []);
});

test('mirror without rename lands on the source topic', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const dstSub = collect();
  dst.subscribe('t', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't' }]);
  src.publish('t', 'hello');
  await flush();
  assert.equal(dstSub.messages.length, 1);
  assert.equal(dstSub.messages[0].topic, 't');
  assert.equal(dstSub.messages[0].mirror?.topic, 't');
  assert.equal(dst.getStats().mirrors[0].dstTemplate, '(source topic)');
});

test('rename templates expand * and ** captures', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const single = collect();
  const multi = collect();
  dst.subscribe('m.eu', single.handler);
  dst.subscribe('archived.**', multi.handler);
  dst.mirror(src, [
    { srcPattern: 'orders.*', rename: 'm.$1' },
    { srcPattern: 'events.**', rename: 'archived.$1' },
  ]);
  src.publish('orders.eu', 1);
  src.publish('orders.us', 2);
  src.publish('events.us.east', 3);
  src.publish('events', 4); // `**` matches zero segments too
  await flush();
  assert.deepEqual(single.messages.map((m) => m.topic), ['m.eu']);
  assert.deepEqual(
    multi.messages.map((m) => m.topic),
    ['archived.us.east', 'archived.'],
  );
  // Per-topic origin seqs: each source topic starts its own sequence.
  assert.deepEqual(multi.messages.map((m) => m.mirror?.seq), [1, 1]);
});

test('$$ escapes a literal dollar in the rename template', () => {
  const rule = resolveMirrorRule({ srcPattern: 'a.*', rename: 'x.$$.$1' }, 'test');
  assert.equal(rule.apply('a.b'), 'x.$.b');
  assert.equal(rule.apply('nope'), null);
});

test('mirror cycle A<->B terminates after one pass', async () => {
  const a = new EventBus({ busId: 'a' });
  const b = new EventBus({ busId: 'b' });
  const aSub = collect();
  const bSub = collect();
  a.subscribe('t', aSub.handler);
  b.subscribe('t', bSub.handler);
  a.mirror(b, [{ srcPattern: 't' }]);
  b.mirror(a, [{ srcPattern: 't' }]);

  a.publish('t', 'ping');
  await flush();
  await flush(); // settle any second-order fan-out

  // B got the mirror copy; A never got the bounce back.
  assert.equal(aSub.messages.length, 1);
  assert.equal(bSub.messages.length, 1);
  assert.equal(bSub.messages[0].payload, 'ping');
  assert.equal(bSub.messages[0].mirror?.busId, 'a');
  assert.equal(a.getStats().mirrors[0].loopsPrevented, 1);
  assert.equal(a.getStats().mirrors[0].mirrored, 0);
  assert.equal(b.getStats().mirrors[0].mirrored, 1);
  assert.equal(b.getStats().mirroredMessages, 1);
  assert.equal(a.getStats().mirroredMessages, 0);

  // Steady state: nothing else arrives.
  await flush();
  assert.equal(aSub.messages.length, 1);
  assert.equal(bSub.messages.length, 1);
});

test('self-mirror onto its own source topic bounces once, then stops', async () => {
  const a = new EventBus({ busId: 'a' });
  const sub = collect();
  a.subscribe('t', sub.handler);
  a.mirror(a, [{ srcPattern: 't' }]);
  a.publish('t', 'x');
  await flush();
  await flush();
  // Original + exactly one mirror copy; the copy's trail stops the loop.
  assert.equal(sub.messages.length, 2);
  assert.equal(sub.messages[0].mirror, undefined);
  assert.deepEqual(sub.messages[1].mirror?.trail, ['a']);
  assert.equal(a.getStats().mirrors[0].loopsPrevented, 1);
  await flush();
  assert.equal(sub.messages.length, 2);
});

test('multi-hop mirror preserves the origin provenance', async () => {
  const a = new EventBus({ busId: 'a' });
  const b = new EventBus({ busId: 'b' });
  const c = new EventBus({ busId: 'c' });
  const cSub = collect();
  c.subscribe('t', cSub.handler);
  b.mirror(a, [{ srcPattern: 't' }]);
  c.mirror(b, [{ srcPattern: 't' }]);
  a.publish('t', 'deep');
  await flush();
  await flush();
  assert.equal(cSub.messages.length, 1);
  assert.deepEqual(cSub.messages[0].mirror, {
    busId: 'a',
    topic: 't',
    seq: 1,
    trail: ['a', 'b'],
  });
  assert.equal(cSub.messages[0].epoch, 'mirror:a');
});

test('origin seq survives interleaved local traffic; no phantom gaps', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const sub = collect();
  dst.subscribe('t', sub.handler);
  dst.mirror(src, [{ srcPattern: 't' }]);
  dst.publish('t', 'local-1');
  src.publish('t', 'm-1');
  src.publish('t', 'm-2');
  dst.publish('t', 'local-2');
  await flush();
  const mirrored = sub.messages.filter((m) => m.mirror !== undefined);
  // Downstream gap detection against the origin stream sees 1, 2.
  assert.deepEqual(mirrored.map((m) => m.mirror?.seq), [1, 2]);
  // The epoch switch re-baselines the built-in gap detector — local and
  // mirror sequence spaces never count phantom gaps against each other.
  assert.equal(dst.getStats().sequenceGaps, 0);
});

test('message identity and metadata ride the mirror', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const sub = collect();
  dst.subscribe('t', sub.handler);
  dst.mirror(src, [{ srcPattern: 't' }]);
  src.publish('t', 'x', { messageId: 'm-1', eventTime: 1_234_567, publisherId: 'svc' });
  await flush();
  const msg = sub.messages[0];
  assert.equal(msg.messageId, 'm-1');
  assert.equal(msg.eventTime, 1_234_567);
  assert.equal(msg.publisherId, 'svc');
});

test('binary payloads stay byte-oriented across the mirror', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const sub = collect();
  dst.subscribe('bin', sub.handler);
  dst.mirror(src, [{ srcPattern: 'bin' }]);
  const bytes = new Uint8Array([1, 2, 3, 250, 251, 0, 255]);
  src.publish('bin', bytes, { binary: true });
  await flush();
  assert.equal(sub.messages.length, 1);
  assert.ok(sub.messages[0].payload instanceof Uint8Array);
  assert.deepEqual([...(sub.messages[0].payload as Uint8Array)], [1, 2, 3, 250, 251, 0, 255]);
});

test('mirror-ingressed messages never re-trigger the destination forwards', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const third = new EventBus({ busId: 'e' });
  const thirdSub = collect();
  third.subscribe('t', thirdSub.handler);
  dst.mirror(src, [{ srcPattern: 't' }]);
  dst.forward(third, 't');
  src.publish('t', 'x');
  await flush();
  // One hop per message: the mirror copy is stamped routed, so the
  // destination's forward rule does not fire on it.
  assert.equal(thirdSub.messages.length, 0);
  assert.equal(dst.getStats().mirroredMessages, 1);
});

test('mirror rule validation fails fast with nothing half-registered', () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const before = src.getStats().totalSubscribers;
  assert.throws(() => dst.mirror({} as never, []), TypeError);
  assert.throws(() => dst.mirror(src, 'nope' as never), RangeError);
  assert.throws(() => dst.mirror(src, [{ srcPattern: '' }]), RangeError);
  assert.throws(() => dst.mirror(src, [null as never]), RangeError);
  assert.throws(
    () => dst.mirror(src, [{ srcPattern: 'a.*', rename: 'x.$2' }]),
    RangeError,
  );
  assert.throws(() => dst.mirror(src, [{ srcPattern: 'a', rename: 'x.$' }]), RangeError);
  assert.throws(() => dst.mirror(src, [{ srcPattern: 'a', rename: '' }]), RangeError);
  // A bad second rule does not leave the first rule subscribed.
  assert.throws(() =>
    dst.mirror(src, [{ srcPattern: 'ok' }, { srcPattern: '' }]),
  );
  assert.equal(src.getStats().totalSubscribers, before);
  assert.deepEqual(dst.getStats().mirrors, []);
  // An empty rule table is a no-op registration.
  const handle = dst.mirror(src, []);
  assert.deepEqual(dst.getStats().mirrors, []);
  handle.close();
});

test('closing the handle stops the mirror', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const sub = collect();
  dst.subscribe('t', sub.handler);
  const before = src.getStats().totalSubscribers;
  const handle = dst.mirror(src, [{ srcPattern: 't' }]);
  assert.equal(src.getStats().totalSubscribers, before + 1);
  src.publish('t', 'before');
  await flush();
  assert.equal(sub.messages.length, 1);
  handle.close();
  assert.equal(handle.closed, true);
  handle.close(); // idempotent
  assert.equal(src.getStats().totalSubscribers, before);
  assert.deepEqual(dst.getStats().mirrors, []);
  src.publish('t', 'after');
  await flush();
  assert.equal(sub.messages.length, 1);
});

test('busId defaults are unique; explicit ids are honored', () => {
  const a = new EventBus();
  const b = new EventBus();
  assert.notEqual(a.busId, b.busId);
  assert.ok(a.busId.length > 0);
  const named = new EventBus({ busId: 'edge-1' });
  assert.equal(named.busId, 'edge-1');
  assert.throws(() => new EventBus({ busId: '' }), RangeError);
});
