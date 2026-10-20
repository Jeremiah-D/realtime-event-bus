import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { resolveMirrorRule } from '../src/mirror.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

function collect() {
  const messages: Array<{ topic: string; payload: unknown }> = [];
  return {
    messages,
    handler: (msg: { topic: string; payload: unknown }) => {
      messages.push({ topic: msg.topic, payload: msg.payload });
    },
  };
}

test('throttle registration validation: RangeError fail-fast, no half-registration', () => {
  const bad: Array<[unknown, string]> = [
    ['fast', 'non-object'],
    [{ messagesPerSec: 0 }, 'zero rate'],
    [{ messagesPerSec: -1 }, 'negative rate'],
    [{ messagesPerSec: NaN }, 'NaN rate'],
    [{ messagesPerSec: 'fast' }, 'string rate'],
    [{}, 'missing rate'],
    [{ messagesPerSec: 2, burst: 0 }, 'zero burst'],
    [{ messagesPerSec: 2, burst: -5 }, 'negative burst'],
    [{ messagesPerSec: 2, burst: NaN }, 'NaN burst'],
  ];
  for (const [throttle, what] of bad) {
    assert.throws(
      () =>
        resolveMirrorRule({ srcPattern: 't', throttle: throttle as never }, 'EventBus.mirror[0]'),
      RangeError,
      what,
    );
  }
  // Valid shapes do not throw; burst defaults to one second of budget.
  assert.deepEqual(resolveMirrorRule({ srcPattern: 't', throttle: { messagesPerSec: 2 } }, 'x').throttle, {
    messagesPerSec: 2,
    burst: 2,
  });
  assert.deepEqual(
    resolveMirrorRule({ srcPattern: 't', throttle: { messagesPerSec: 2, burst: 5 } }, 'x').throttle,
    { messagesPerSec: 2, burst: 5 },
  );

  // A bad rule fails the whole mirror() call before any subscribe lands
  // on the source bus — no half-registered mirror.
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  assert.throws(
    () =>
      dst.mirror(src, [
        { srcPattern: 'ok' },
        { srcPattern: 't', throttle: { messagesPerSec: 0 } },
      ]),
    RangeError,
  );
  assert.equal(src.getStats().totalSubscribers, 0);
  assert.deepEqual(dst.getStats().mirrors, []);
});

test('throttled mirror frames are shed: burst + refill, injectable clock', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  const dstSub = collect();
  dst.subscribe('m', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 2, burst: 2 } }]);

  src.publish('t', { n: 1 });
  src.publish('t', { n: 2 });
  src.publish('t', { n: 3 });
  await flush();

  assert.equal(dstSub.messages.length, 2);
  assert.deepEqual(dstSub.messages.map((m) => m.payload), [{ n: 1 }, { n: 2 }]);
  const [row] = dst.getStats().mirrors;
  assert.equal(row.mirrored, 2);
  assert.equal(row.mirroredThrottled, 1);
  assert.equal(row.loopsPrevented, 0);

  // One second of refill restores the budget.
  nowMs += 1000;
  src.publish('t', { n: 4 });
  await flush();
  assert.equal(dstSub.messages.length, 3);
  assert.deepEqual(dstSub.messages[2].payload, { n: 4 });
  assert.equal(dst.getStats().mirrors[0].mirrored, 3);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 1);
});

test('throttle drops never reach destination admission; source delivery unaffected', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  const srcSub = collect();
  src.subscribe('t', srcSub.handler);
  const dstSub = collect();
  dst.subscribe('m', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 1, burst: 1 } }]);

  src.publish('t', 'a');
  src.publish('t', 'b');
  src.publish('t', 'c');
  await flush();

  // The source's own subscribers see every publish — the mirror throttle
  // lives on the destination's receive path.
  assert.equal(srcSub.messages.length, 3);
  // The destination admitted only the budgeted frame.
  assert.equal(dstSub.messages.length, 1);
  assert.equal(dst.getStats().mirroredMessages, 1);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 2);
});

test('loop-prevented drops neither consume throttle budget nor count as throttled', async () => {
  let nowMs = 1_000_000;
  const a = new EventBus({ busId: 'a' });
  const b = new EventBus({ busId: 'b', now: () => nowMs });
  // b pulls from a with a 1-frame throttle; a pulls from b to close the loop.
  b.mirror(a, [{ srcPattern: 't', throttle: { messagesPerSec: 1, burst: 1 } }]);
  const aMirror = a.mirror(b, [{ srcPattern: 't' }]);

  a.publish('t', 'x');
  await flush();
  await flush();

  // b admitted exactly one frame (its throttle budget), throttled none;
  // a loop-dropped the echo back.
  const bRow = b.getStats().mirrors[0];
  assert.equal(bRow.mirrored, 1);
  assert.equal(bRow.mirroredThrottled, 0);
  assert.equal(bRow.loopsPrevented, 0);
  const aRow = aMirror.rules()[0];
  assert.equal(aRow.mirrored, 0);
  assert.equal(aRow.loopsPrevented, 1);
  assert.equal(aRow.mirroredThrottled, 0);
});

test('mirror_throttled audit events are recorded per shed frame', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  dst.subscribe('m', () => {});
  dst.mirror(src, [{ srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 1, burst: 1 } }]);

  src.publish('t', 'a');
  src.publish('t', 'b');
  await flush();

  const events = dst.getSubscriptionAudit({ action: 'mirror_throttled' });
  assert.equal(events.length, 1);
  const [ev] = events;
  assert.equal(ev.pattern, 't');
  assert.equal(ev.topic, 't');
  assert.equal(ev.actor, 'system');
  assert.deepEqual(ev.detail, { srcPattern: 't', dstTemplate: 'm', messagesPerSec: 1 });
});

test('prometheus exposition carries per-rule throttled series (activity-gated)', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  dst.subscribe('m', () => {});
  dst.mirror(src, [{ srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 1, burst: 1 } }]);
  dst.mirror(src, [{ srcPattern: 'other', rename: 'o' }]);

  src.publish('t', 'a');
  src.publish('t', 'b');
  await flush();

  const exposition = renderPrometheus(dst.getStats());
  const line = exposition
    .split('\n')
    .find((l) => l.startsWith('eventbus_mirror_throttled_total{'));
  assert.ok(line !== undefined, 'expected a throttled series');
  assert.ok(line.includes('src_pattern="t"'));
  assert.ok(line.includes('dst_template="m"'));
  assert.ok(line.endsWith(' 1'));
  // The unthrottled rule renders no series — cardinality follows activity.
  assert.equal(
    exposition.split('\n').filter((l) => l.startsWith('eventbus_mirror_throttled_total{')).length,
    1,
  );
});

test('unthrottled mirror rules keep historical behavior and zero throttled', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const dstSub = collect();
  dst.subscribe('t', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't' }]);

  src.publish('t', 'a');
  src.publish('t', 'b');
  await flush();

  assert.equal(dstSub.messages.length, 2);
  assert.deepEqual(dst.getStats().mirrors, [
    { srcPattern: 't', dstTemplate: '(source topic)', mirrored: 2, loopsPrevented: 0, mirroredThrottled: 0 },
  ]);
});
