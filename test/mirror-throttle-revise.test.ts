import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

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

/** Publish `n` numbered frames on the source topic, then flush. */
async function publishN(src: EventBus, topic: string, n: number, from = 1): Promise<void> {
  for (let i = 0; i < n; i += 1) src.publish(topic, { n: from + i });
  await flush();
}

test('unknown srcPattern throws RangeError, including after close', () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  // No live rule at all.
  assert.throws(() => dst.setMirrorThrottle('t', { messagesPerSec: 2 }), RangeError);
  // A rule for another pattern does not count.
  const handle = dst.mirror(src, [{ srcPattern: 'other' }]);
  assert.throws(() => dst.setMirrorThrottle('t', { messagesPerSec: 2 }), RangeError);
  // A closed mirror's rules are gone.
  handle.close();
  assert.throws(() => dst.setMirrorThrottle('other', { messagesPerSec: 2 }), RangeError);
  // Empty / non-string patterns fail fast too.
  assert.throws(() => dst.setMirrorThrottle('', { messagesPerSec: 2 }), RangeError);
  assert.throws(() => dst.setMirrorThrottle(42 as never, { messagesPerSec: 2 }), RangeError);
  assert.equal(dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' }).length, 0);
});

test('illegal throttle values throw RangeError and leave the live throttle untouched', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  const dstSub = collect();
  dst.subscribe('m', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 2, burst: 2 } }]);

  const bad: unknown[] = [
    'fast',
    undefined,
    { messagesPerSec: 0 },
    { messagesPerSec: -1 },
    { messagesPerSec: NaN },
    { messagesPerSec: Infinity },
    { messagesPerSec: 'fast' },
    {},
    { messagesPerSec: 2, burst: 0 },
    { messagesPerSec: 2, burst: -5 },
    { messagesPerSec: 2, burst: NaN },
  ];
  for (const value of bad) {
    assert.throws(() => dst.setMirrorThrottle('t', value as never), RangeError);
  }
  // The live throttle is exactly as constructed — no audit, no retune.
  assert.deepEqual(dst.getStats().mirrors[0].throttle, { messagesPerSec: 2, burst: 2 });
  assert.equal(dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' }).length, 0);
  // Behavior proves the budget was not reset or refilled: exactly the
  // two frames of the original full bucket pass, the third is shed.
  await publishN(src, 't', 3);
  assert.equal(dstSub.messages.length, 2);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 1);
});

test('re-affirming the current throttle is a no-op with no audit', () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  // Burst defaults to one second of budget at construction, so a bare
  // rate matches the resolved value.
  dst.mirror(src, [{ srcPattern: 't', throttle: { messagesPerSec: 2 } }]);
  dst.setMirrorThrottle('t', { messagesPerSec: 2 });
  dst.setMirrorThrottle('t', { messagesPerSec: 2, burst: 2 });
  assert.equal(dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' }).length, 0);
  assert.deepEqual(dst.getStats().mirrors[0].throttle, { messagesPerSec: 2, burst: 2 });

  // Clearing an already-unthrottled rule is the same kind of no-op.
  dst.mirror(src, [{ srcPattern: 'u' }]);
  dst.setMirrorThrottle('u', null);
  assert.equal(dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' }).length, 0);
  assert.equal('throttle' in dst.getStats().mirrors[1], false);
});

test('lowering the rate does not top the bucket up (old-rate refill, new-burst cap)', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  const dstSub = collect();
  dst.subscribe('m', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 10, burst: 10 } }]);

  // Drain the bucket dry.
  await publishN(src, 't', 10);
  assert.equal(dstSub.messages.length, 10);

  // Half a second of refill at the OLD rate banks 5 tokens.
  nowMs += 500;
  dst.setMirrorThrottle('t', { messagesPerSec: 1, burst: 1 });
  // Retune caps the 5 banked tokens at the new burst of 1 — and proves
  // the refill ran at the old rate: at the new 1/s rate only 0.5 tokens
  // would have accrued, admitting nothing.
  await publishN(src, 't', 2, 11);
  assert.equal(dstSub.messages.length, 11);
  assert.deepEqual(dstSub.messages.map((m) => m.payload), [
    { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 },
    { n: 6 }, { n: 7 }, { n: 8 }, { n: 9 }, { n: 10 }, { n: 11 },
  ]);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 1);
  assert.deepEqual(dst.getStats().mirrors[0].throttle, { messagesPerSec: 1, burst: 1 });

  const [ev] = dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' });
  assert.equal(ev.pattern, 't');
  assert.deepEqual(ev.detail, {
    srcPattern: 't',
    throttle: { messagesPerSec: 1, burst: 1 },
    oldThrottle: { messagesPerSec: 10, burst: 10 },
  });
});

test('raising the rate does not top the bucket up', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  const dstSub = collect();
  dst.subscribe('m', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 10, burst: 10 } }]);

  // Drain the bucket dry, then bank a single token at the old rate.
  await publishN(src, 't', 10);
  nowMs += 100;
  dst.setMirrorThrottle('t', { messagesPerSec: 100, burst: 100 });
  // One banked token survives the retune — a topped-up bucket would
  // have admitted both frames.
  await publishN(src, 't', 2, 11);
  assert.equal(dstSub.messages.length, 11);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 1);
  assert.deepEqual(dst.getStats().mirrors[0].throttle, { messagesPerSec: 100, burst: 100 });
});

test('clearing the throttle restores unlimited passthrough', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  const dstSub = collect();
  dst.subscribe('m', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 1, burst: 1 } }]);

  await publishN(src, 't', 2);
  assert.equal(dstSub.messages.length, 1);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 1);

  dst.setMirrorThrottle('t', null);
  await publishN(src, 't', 5, 3);
  assert.equal(dstSub.messages.length, 6);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 1);
  // The row reads unthrottled again — no throttle key at all.
  assert.deepEqual(dst.getStats().mirrors, [
    {
      srcPattern: 't',
      dstTemplate: 'm',
      mirrored: 6,
      loopsPrevented: 0,
      mirroredThrottled: 1,
    },
  ]);

  const [ev] = dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' });
  assert.deepEqual(ev.detail, {
    srcPattern: 't',
    throttle: null,
    oldThrottle: { messagesPerSec: 1, burst: 1 },
  });
});

test('an unthrottled rule gains a throttle starting from a full bucket', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  const dstSub = collect();
  dst.subscribe('m', dstSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm' }]);

  await publishN(src, 't', 2);
  assert.equal(dstSub.messages.length, 2);

  dst.setMirrorThrottle('t', { messagesPerSec: 2, burst: 2 });
  // The new bucket starts full: exactly two frames pass, the third sheds.
  await publishN(src, 't', 3, 3);
  assert.equal(dstSub.messages.length, 4);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 1);
  assert.deepEqual(dst.getStats().mirrors[0].throttle, { messagesPerSec: 2, burst: 2 });

  const [ev] = dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' });
  assert.deepEqual(ev.detail, {
    srcPattern: 't',
    throttle: { messagesPerSec: 2, burst: 2 },
    oldThrottle: null,
  });
});

test('every live rule sharing a pattern is revised, each audited once', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  dst.subscribe('a', () => {});
  dst.subscribe('b', () => {});
  dst.mirror(src, [
    { srcPattern: 't', rename: 'a', throttle: { messagesPerSec: 2, burst: 2 } },
    { srcPattern: 't', rename: 'b', throttle: { messagesPerSec: 5, burst: 5 } },
  ]);

  // Only the rule that actually moves is audited — the {2, 2} rule is
  // already at the target value.
  dst.setMirrorThrottle('t', { messagesPerSec: 2, burst: 2 });
  let events = dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, {
    srcPattern: 't',
    throttle: { messagesPerSec: 2, burst: 2 },
    oldThrottle: { messagesPerSec: 5, burst: 5 },
  });

  // Now both move, and both are audited.
  dst.setMirrorThrottle('t', { messagesPerSec: 3, burst: 3 });
  events = dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' });
  assert.equal(events.length, 3);
  assert.deepEqual(events[1].detail?.oldThrottle, { messagesPerSec: 2, burst: 2 });
  assert.deepEqual(events[2].detail?.oldThrottle, { messagesPerSec: 2, burst: 2 });
  assert.deepEqual(
    dst.getStats().mirrors.map((row) => row.throttle),
    [
      { messagesPerSec: 3, burst: 3 },
      { messagesPerSec: 3, burst: 3 },
    ],
  );

  // Both buckets enforce the new budget independently — and the retune
  // kept the 2 banked tokens rather than topping up to the new burst
  // of 3, so exactly 2 frames pass per rule.
  await publishN(src, 't', 5);
  const [rowA, rowB] = dst.getStats().mirrors;
  assert.equal(rowA.mirrored, 2);
  assert.equal(rowA.mirroredThrottled, 3);
  assert.equal(rowB.mirrored, 2);
  assert.equal(rowB.mirroredThrottled, 3);
});

test('audit trail and getStats follow revisions; snapshots cannot move the live throttle', () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const handle = dst.mirror(src, [{ srcPattern: 't', throttle: { messagesPerSec: 2, burst: 2 } }]);

  dst.setMirrorThrottle('t', { messagesPerSec: 4, burst: 4 });
  assert.deepEqual(handle.rules()[0].throttle, { messagesPerSec: 4, burst: 4 });
  // Mutating a returned snapshot leaves the live throttle alone.
  const snap = dst.getStats().mirrors[0];
  assert.ok(snap.throttle !== undefined);
  snap.throttle.messagesPerSec = 999;
  assert.deepEqual(dst.getStats().mirrors[0].throttle, { messagesPerSec: 4, burst: 4 });
  assert.deepEqual(handle.rules()[0].throttle, { messagesPerSec: 4, burst: 4 });

  dst.setMirrorThrottle('t', null);
  assert.equal('throttle' in dst.getStats().mirrors[0], false);
  assert.equal('throttle' in handle.rules()[0], false);

  const events = dst.getSubscriptionAudit({ action: 'mirror_throttle_revised' });
  assert.equal(events.length, 2);
  assert.deepEqual(events[0].detail, {
    srcPattern: 't',
    throttle: { messagesPerSec: 4, burst: 4 },
    oldThrottle: { messagesPerSec: 2, burst: 2 },
  });
  assert.deepEqual(events[1].detail, {
    srcPattern: 't',
    throttle: null,
    oldThrottle: { messagesPerSec: 4, burst: 4 },
  });
  assert.equal(events[0].actor, 'system');
});
