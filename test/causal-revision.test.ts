import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Causal buffer-bound revision (EB-291): `setCausal` retunes a
 * causal subscriber's per-source reorder-buffer bound without
 * re-subscribing — only `causal.maxBufferPerSource` moves, the
 * per-source expectations, the skipped-clock sets, and the
 * already-buffered messages are untouched, and the new bound
 * governs from the next early arrival (which reads the live
 * value). Lowering the bound below the live buffer depth sheds
 * on the next arrival from that source; raising it admits more
 * early arrivals at once.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('setCausal rejects unknown, empty, non-causal, and invalid revisions fail-closed', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: { maxBufferPerSource: 2 },
  });
  const plain = bus.subscribe('t2', () => {});
  for (const bad of [
    () => bus.setCausal('no-such-sub', 2),
    () => bus.setCausal('', 2),
    () => bus.setCausal(plain.id, 2),
    () => bus.setCausal(sub.id, 0),
    () => bus.setCausal(sub.id, -1),
    () => bus.setCausal(sub.id, 2.5),
    () => bus.setCausal(sub.id, Number.NaN),
    () => bus.setCausal(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setCausal(sub.id, '2' as never),
    () => bus.setCausal(sub.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'causal_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live bound is untouched: clocks 1 and 2 buffer, and the
  // third early arrival sheds the oldest under the old bound of 2.
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  assert.equal(bus.causalBufferDepth(sub.id), 2);
  bus.publish('t', 'c3', { causal: { source: 's', clock: 3 } });
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  await flush();
  assert.deepEqual(received, ['c2', 'c3']);
  sub.unsubscribe();
  plain.unsubscribe();
});

test('lowering the bound sheds on the next arrival without touching buffered messages first', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: { maxBufferPerSource: 3 },
  });
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  bus.publish('t', 'c3', { causal: { source: 's', clock: 3 } });
  assert.equal(bus.causalBufferDepth(sub.id), 3);
  bus.setCausal(sub.id, 1);
  const audit = bus.getSubscriptionAudit({ action: 'causal_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].pattern, '*');
  assert.deepEqual(audit[0].detail, { maxBufferPerSource: 1, oldMaxBufferPerSource: 3 });
  // The revision itself does not clean up: the three buffered
  // messages stay buffered until the next arrival sheds them.
  assert.equal(bus.causalBufferDepth(sub.id), 3);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 0);
  // The next arrival overflows the new bound: the shed loop drops
  // the oldest buffered clocks (1, 2, 3) and the expectation
  // advances past them, releasing clock 4 at once.
  bus.publish('t', 'c4', { causal: { source: 's', clock: 4 } });
  assert.equal(bus.causalBufferDepth(sub.id), 0);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 3);
  await flush();
  assert.deepEqual(received, ['c4']);
  sub.unsubscribe();
});

test('raising the bound admits more early arrivals at once', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: { maxBufferPerSource: 1 },
  });
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  assert.equal(bus.causalBufferDepth(sub.id), 1);
  bus.setCausal(sub.id, 3);
  // Under the old bound the next arrival would have shed clock 1;
  // under the new bound clocks 2 and 3 buffer alongside it.
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  bus.publish('t', 'c3', { causal: { source: 's', clock: 3 } });
  assert.equal(bus.causalBufferDepth(sub.id), 3);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 0);
  // The missing dependency arrives: the whole stream drains in order.
  bus.publish('t', 'c0', { causal: { source: 's', clock: 0 } });
  await flush();
  assert.deepEqual(received, ['c0', 'c1', 'c2', 'c3']);
  sub.unsubscribe();
});

test('re-affirming the current bound is a no-op with no audit', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: { maxBufferPerSource: 2 },
  });
  bus.setCausal(sub.id, 2);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'causal_revised' }).length,
    0,
    'a same-bound revision must not audit',
  );
  // The bound is unchanged: two early arrivals buffer, the third sheds.
  bus.publish('t', 'c1', { causal: { source: 's', clock: 1 } });
  bus.publish('t', 'c2', { causal: { source: 's', clock: 2 } });
  bus.publish('t', 'c3', { causal: { source: 's', clock: 3 } });
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 1);
  await flush();
  assert.deepEqual(received, ['c2', 'c3']);
  sub.unsubscribe();
});

test('the revised bound applies per source and leaves other sources alone', async () => {
  const bus = new EventBus();
  const received: string[] = [];
  const sub = bus.subscribe('*', (m) => received.push(m.payload as string), {
    causal: { maxBufferPerSource: 2 },
  });
  // Source 'a' piles up two early arrivals; source 'b' one.
  bus.publish('t', 'a1', { causal: { source: 'a', clock: 1 } });
  bus.publish('t', 'a2', { causal: { source: 'a', clock: 2 } });
  bus.publish('t', 'b1', { causal: { source: 'b', clock: 1 } });
  assert.equal(bus.causalBufferDepth(sub.id), 3);
  bus.setCausal(sub.id, 1);
  // Source 'a' sheds on its next arrival; source 'b' is untouched
  // until its own next arrival.
  bus.publish('t', 'a3', { causal: { source: 'a', clock: 3 } });
  assert.equal(bus.causalBufferDepth(sub.id), 1);
  assert.equal(bus.getStats().causalBuffer.droppedMessages, 2);
  bus.publish('t', 'b0', { causal: { source: 'b', clock: 0 } });
  await flush();
  assert.deepEqual(received, ['a3', 'b0', 'b1']);
  sub.unsubscribe();
});
