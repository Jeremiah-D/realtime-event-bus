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

test('retargeting moves the next frame to the new topic with exact $1 expansion', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const oldSub = collect();
  const newSub = collect();
  dst.subscribe('m.eu', oldSub.handler);
  dst.subscribe('archived.eu', newSub.handler);
  const handle = dst.mirror(src, [{ srcPattern: 'orders.*', rename: 'm.$1' }]);

  await publishN(src, 'orders.eu', 1);
  assert.deepEqual(oldSub.messages.map((m) => m.payload), [{ n: 1 }]);
  assert.equal(newSub.messages.length, 0);

  dst.reviseMirrorRule('orders.*', { rename: 'archived.$1' });
  await publishN(src, 'orders.eu', 1, 2);
  assert.equal(oldSub.messages.length, 1);
  assert.deepEqual(newSub.messages.map((m) => m.payload), [{ n: 2 }]);
  assert.equal(newSub.messages[0].topic, 'archived.eu');

  assert.equal(dst.getStats().mirrors[0].dstTemplate, 'archived.$1');
  assert.equal(handle.rules()[0].dstTemplate, 'archived.$1');
  assert.equal(dst.getStats().mirrors[0].mirrored, 2);

  const [ev] = dst.getSubscriptionAudit({ action: 'mirror_rule_revised' });
  assert.equal(ev.pattern, 'orders.*');
  assert.equal(ev.actor, 'system');
  assert.deepEqual(ev.detail, {
    srcPattern: 'orders.*',
    rename: 'archived.$1',
    oldRename: 'm.$1',
  });
});

test('clearing the template returns frames to the source topic', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const oldSub = collect();
  const srcSub = collect();
  dst.subscribe('m', oldSub.handler);
  dst.subscribe('t', srcSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm' }]);

  await publishN(src, 't', 1);
  assert.equal(oldSub.messages.length, 1);

  // The actor rides the audit event.
  dst.reviseMirrorRule('t', { rename: null }, 'ops');
  await publishN(src, 't', 1, 2);
  assert.equal(oldSub.messages.length, 1);
  assert.deepEqual(srcSub.messages.map((m) => m.payload), [{ n: 2 }]);
  assert.equal(dst.getStats().mirrors[0].dstTemplate, '(source topic)');

  const [ev] = dst.getSubscriptionAudit({ action: 'mirror_rule_revised' });
  assert.equal(ev.actor, 'ops');
  assert.deepEqual(ev.detail, { srcPattern: 't', rename: null, oldRename: 'm' });
});

test('unknown srcPattern throws RangeError, including after close', () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  // No live rule at all.
  assert.throws(() => dst.reviseMirrorRule('t', { rename: 'm' }), RangeError);
  // A rule for another pattern does not count.
  const handle = dst.mirror(src, [{ srcPattern: 'other' }]);
  assert.throws(() => dst.reviseMirrorRule('t', { rename: 'm' }), RangeError);
  // A closed mirror's rules are gone.
  handle.close();
  assert.throws(() => dst.reviseMirrorRule('other', { rename: 'm' }), RangeError);
  // Empty / non-string patterns fail fast too.
  assert.throws(() => dst.reviseMirrorRule('', { rename: 'm' }), RangeError);
  assert.throws(() => dst.reviseMirrorRule(42 as never, { rename: 'm' }), RangeError);
  assert.equal(dst.getSubscriptionAudit({ action: 'mirror_rule_revised' }).length, 0);
});

test('malformed patches and illegal templates throw RangeError and leave the live template untouched', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const oldSub = collect();
  const newSub = collect();
  dst.subscribe('m', oldSub.handler);
  dst.subscribe('n', newSub.handler);
  dst.mirror(src, [{ srcPattern: 'orders.*', rename: 'm' }]);

  const badPatches: unknown[] = [
    'm',
    null,
    undefined,
    42,
    {},
    { rename: undefined },
    { rename: 42 },
    { rename: '' },
    { rename: 'm.$' },
    // The pattern yields one capture; $9 does not exist.
    { rename: 'm.$9' },
    { rename: 'm.$2' },
  ];
  for (const patch of badPatches) {
    assert.throws(() => dst.reviseMirrorRule('orders.*', patch as never), RangeError);
  }
  // The live template is exactly as constructed — no audit, no retarget.
  assert.equal(dst.getStats().mirrors[0].dstTemplate, 'm');
  assert.equal(dst.getSubscriptionAudit({ action: 'mirror_rule_revised' }).length, 0);
  // Behavior proves the template was not touched: the next frame still
  // lands on the old destination.
  await publishN(src, 'orders.eu', 1);
  assert.equal(oldSub.messages.length, 1);
  assert.equal(newSub.messages.length, 0);
});

test('re-affirming the current template is a no-op with no audit', () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  dst.mirror(src, [{ srcPattern: 't', rename: 'm' }]);
  dst.reviseMirrorRule('t', { rename: 'm' });
  assert.equal(dst.getSubscriptionAudit({ action: 'mirror_rule_revised' }).length, 0);
  assert.equal(dst.getStats().mirrors[0].dstTemplate, 'm');

  // Clearing an already-unrenamed rule is the same kind of no-op.
  dst.mirror(src, [{ srcPattern: 'u' }]);
  dst.reviseMirrorRule('u', { rename: null });
  assert.equal(dst.getSubscriptionAudit({ action: 'mirror_rule_revised' }).length, 0);
  assert.equal(dst.getStats().mirrors[1].dstTemplate, '(source topic)');
});

test('counters and throttle survive a revision', async () => {
  let nowMs = 1_000_000;
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd', now: () => nowMs });
  const oldSub = collect();
  const newSub = collect();
  dst.subscribe('m', oldSub.handler);
  dst.subscribe('n', newSub.handler);
  const handle = dst.mirror(src, [
    { srcPattern: 't', rename: 'm', throttle: { messagesPerSec: 1, burst: 1 } },
  ]);

  // One frame passes, the second sheds on the frozen clock.
  await publishN(src, 't', 2);
  assert.equal(oldSub.messages.length, 1);
  assert.equal(dst.getStats().mirrors[0].mirrored, 1);
  assert.equal(dst.getStats().mirrors[0].mirroredThrottled, 1);

  dst.reviseMirrorRule('t', { rename: 'n' });
  // The bucket was not refilled or dropped: with no time passing, both
  // new frames still shed — and the counters kept counting.
  await publishN(src, 't', 2, 3);
  assert.equal(newSub.messages.length, 0);
  assert.deepEqual(dst.getStats().mirrors[0], {
    srcPattern: 't',
    dstTemplate: 'n',
    mirrored: 1,
    loopsPrevented: 0,
    mirroredThrottled: 3,
    throttle: { messagesPerSec: 1, burst: 1 },
  });
  assert.deepEqual(handle.rules()[0].throttle, { messagesPerSec: 1, burst: 1 });

  const [ev] = dst.getSubscriptionAudit({ action: 'mirror_rule_revised' });
  assert.deepEqual(ev.detail, { srcPattern: 't', rename: 'n', oldRename: 'm' });
});

test('every live rule sharing a pattern is revised, each audited once', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const cSub = collect();
  dst.subscribe('c', cSub.handler);
  dst.mirror(src, [
    { srcPattern: 't', rename: 'a' },
    { srcPattern: 't', rename: 'b' },
  ]);

  // Only the rule that actually moves is audited — the 'a' rule is
  // already at the target template.
  dst.reviseMirrorRule('t', { rename: 'a' });
  let events = dst.getSubscriptionAudit({ action: 'mirror_rule_revised' });
  assert.equal(events.length, 1);
  assert.deepEqual(events[0].detail, { srcPattern: 't', rename: 'a', oldRename: 'b' });

  // Now both move, and both are audited.
  dst.reviseMirrorRule('t', { rename: 'c' });
  events = dst.getSubscriptionAudit({ action: 'mirror_rule_revised' });
  assert.equal(events.length, 3);
  assert.deepEqual(events[1].detail?.oldRename, 'a');
  assert.deepEqual(events[2].detail?.oldRename, 'a');
  assert.deepEqual(
    dst.getStats().mirrors.map((row) => row.dstTemplate),
    ['c', 'c'],
  );

  // Both rules still mirror independently onto the new destination.
  await publishN(src, 't', 1);
  assert.equal(cSub.messages.length, 2);
  assert.deepEqual(dst.getStats().mirrors.map((row) => row.mirrored), [1, 1]);
});

test('an already-admitted frame keeps its old destination; the next frame takes the new one', async () => {
  const src = new EventBus({ busId: 's' });
  const dst = new EventBus({ busId: 'd' });
  const oldSub = collect();
  const newSub = collect();
  dst.subscribe('m', oldSub.handler);
  dst.subscribe('n', newSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'm' }]);

  // Frame 1 is admitted on the destination under the old template
  // before the revision runs.
  await publishN(src, 't', 1);
  assert.deepEqual(oldSub.messages.map((m) => m.payload), [{ n: 1 }]);

  dst.reviseMirrorRule('t', { rename: 'n' });
  await publishN(src, 't', 1, 2);
  assert.equal(oldSub.messages.length, 1);
  assert.deepEqual(newSub.messages.map((m) => m.payload), [{ n: 2 }]);
  assert.equal(dst.getStats().mirrors[0].mirrored, 2);
});

test('loop protection still counts under the new template', async () => {
  const bus = new EventBus({ busId: 'self' });
  const tSub = collect();
  const mSub = collect();
  bus.subscribe('t', tSub.handler);
  bus.subscribe('m', mSub.handler);
  // A self-mirror onto a different topic is loop-free.
  bus.mirror(bus, [{ srcPattern: 't', rename: 'm' }]);
  bus.publish('t', 1);
  await flush();
  assert.equal(mSub.messages.length, 1);
  assert.equal(bus.getStats().mirrors[0].loopsPrevented, 0);
  assert.equal(bus.getStats().mirrors[0].mirrored, 1);

  // Retarget onto the source topic itself: the mirrored frame now
  // re-triggers the tap, its trail holds this bus id, and the loop
  // detector drops it — counted, not amplified.
  bus.reviseMirrorRule('t', { rename: 't' });
  bus.publish('t', 2);
  await flush();
  await flush(); // settle the second-order fan-out
  assert.equal(bus.getStats().mirrors[0].mirrored, 2);
  assert.equal(bus.getStats().mirrors[0].loopsPrevented, 1);
  // The 't' subscribers saw the original publish plus the one
  // mirrored first pass — the looped-back pass never delivered.
  assert.equal(tSub.messages.length, 3);
  assert.equal(mSub.messages.length, 1);
});

test('the arbitration key moves with the destination', async () => {
  // The source clock runs behind the destination clock, so a remote
  // write carries an HLC older than a local write at the same key.
  const src = new EventBus({ busId: 'arb-src', now: () => 1_000 });
  const dst = new EventBus({ busId: 'arb-dst', now: () => 10_000 });
  const oldSub = collect();
  const newSub = collect();
  dst.subscribe('old', oldSub.handler);
  dst.subscribe('new', newSub.handler);
  dst.mirror(src, [{ srcPattern: 't', rename: 'old' }]);

  // A local write at the old destination records a newer HLC for
  // (old, k) in the destination's LWW register.
  dst.publish('old', { v: 'local' }, { key: 'k' });
  await flush();
  assert.deepEqual(oldSub.messages.map((m) => m.payload), [{ v: 'local' }]);

  // The remote write is stale at the old destination: suppressed.
  src.publish('t', { v: 'remote-1' }, { key: 'k' });
  await flush();
  assert.equal(oldSub.messages.length, 1);
  assert.equal(dst.getStats().mirrorConflicts, 1);
  assert.deepEqual(dst.getStats().mirrorConflictsByTopic, [{ topic: 'old', conflicts: 1 }]);

  // After retargeting, the same key is arbitrated at the new
  // destination — where the register holds nothing — so the remote
  // write is no longer suppressed by the write at the old one.
  dst.reviseMirrorRule('t', { rename: 'new' });
  src.publish('t', { v: 'remote-2' }, { key: 'k' });
  await flush();
  assert.deepEqual(newSub.messages.map((m) => m.payload), [{ v: 'remote-2' }]);
  assert.equal(dst.getStats().mirrorConflicts, 1);
  assert.equal(oldSub.messages.length, 1);
});
