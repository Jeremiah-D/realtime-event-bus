/**
 * Bus-level lineage hop cap revision (EB-317).
 *
 * `setLineage` retunes the per-message hop cap live — without restarting
 * the bus. The lineage records themselves are never rewritten: hop
 * collection is unchanged, and every admission check (the formal check
 * and the `publishAtomic` shadow check) already reads the cap live, so
 * the new value governs from the next admission check onward. A
 * tightened cap rejects a chain that already reached the new bound at
 * the next admission; a widened one admits a chain the old cap would
 * have depth-rejected. Messages already admitted keep their lineage —
 * an in-flight message is never re-judged, and a message already
 * rejected is never revived. Illegal values fail closed with the live
 * cap and the audit log untouched; an identical (or empty) revision is
 * a no-op with no audit. Cycle rejections are independent of the cap:
 * a forwarding loop is still a loop after the cap moves.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const audit = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'lineage_revised' });

/** A lineage of `n` hops over distinct topics, none of them `topic`. */
function lineageOf(n: number) {
  return {
    originTopic: 'h0',
    hops: Array.from({ length: n }, (_, i) => ({ topic: `h${i}`, at: 1 + i })),
  };
}

test('constructor still rejects illegal maxLineageHops', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, '32']) {
    assert.throws(() => new EventBus({ maxLineageHops: bad as never }), RangeError);
  }
});

test('setLineage rejects illegal values and leaves the live cap untouched', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  // The default cap is 32: a 32-hop chain is depth-rejected.
  assert.equal(bus.publish('t', { n: 1 }, { lineage: lineageOf(32) }), 0);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  const bad: Array<unknown> = [
    { maxLineageHops: 0 },
    { maxLineageHops: -1 },
    { maxLineageHops: 1.5 },
    { maxLineageHops: Number.NaN },
    { maxLineageHops: '32' },
    // One bad field is enough: there is no sibling to save the call.
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setLineage(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: same cap, no audit, and a 31-hop
  // chain is still admitted under the untouched cap.
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.publish('t', { n: 2 }, { lineage: lineageOf(31) }), 1);
  await flush();
  assert.deepEqual(received, [{ n: 2 }]);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  // A valid revision afterwards still works on the untouched cap.
  bus.setLineage({ maxLineageHops: 8 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    maxLineageHops: 8,
    oldMaxLineageHops: 32,
  });
  bus.close();
});

test('setLineage with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus({ maxLineageHops: 2 });
  bus.setLineage({ maxLineageHops: 2 });
  bus.setLineage({});
  bus.setLineage(null as never);
  assert.equal(audit(bus).length, 0);
  bus.close();
});

test('revision audits the old and new cap', () => {
  const bus = new EventBus();
  bus.setLineage({ maxLineageHops: 8 });
  bus.setLineage({ maxLineageHops: 64 });
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, { maxLineageHops: 8, oldMaxLineageHops: 32 });
  assert.deepEqual(entries[1].detail, { maxLineageHops: 64, oldMaxLineageHops: 8 });
  bus.close();
});

test('tightening the cap rejects a chain at the new bound at the next admission', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  // A 2-hop chain is admitted under the default cap of 32.
  assert.equal(bus.publish('t', { n: 1 }, { lineage: lineageOf(2) }), 1);
  bus.setLineage({ maxLineageHops: 2 });
  // The same 2-hop chain now reaches the tightened cap: depth-rejected.
  assert.equal(bus.publish('t', { n: 2 }, { lineage: lineageOf(2) }), 0);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  // The boundary is exact: one hop under the cap is still admitted.
  assert.equal(bus.publish('t', { n: 3 }, { lineage: lineageOf(1) }), 1);
  await flush();
  assert.deepEqual(received, [{ n: 1 }, { n: 3 }]);
  bus.close();
});

test('widening the cap admits a chain the old cap depth-rejected', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  // A 32-hop chain reaches the default cap of 32: depth-rejected.
  assert.equal(bus.publish('t', { n: 1 }, { lineage: lineageOf(32) }), 0);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  bus.setLineage({ maxLineageHops: 64 });
  // The same chain is now well under the widened cap: admitted.
  assert.equal(bus.publish('t', { n: 2 }, { lineage: lineageOf(32) }), 1);
  await flush();
  assert.deepEqual(received, [{ n: 2 }]);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  bus.close();
});

test('already-admitted messages are never re-judged against the new cap', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload));
  bus.pauseSubscriber(sub.id);
  // Admitted under the default cap: the origin hop is stamped, so the
  // queued message carries 3 hops.
  assert.equal(bus.publish('t', { n: 1 }, { lineage: lineageOf(2) }), 1);
  assert.deepEqual(received, []);
  // Tighten the cap below the queued message's hop count.
  bus.setLineage({ maxLineageHops: 2 });
  // The in-flight message is still delivered: admission is not rerun.
  bus.resumeSubscriber(sub.id);
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(received, [{ n: 1 }]);
  assert.equal(bus.getStats().lineageCycleRejections, 0);
  bus.close();
});

test('cycle rejections are untouched by a widened cap', () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('x', (m) => received.push(m.payload));
  // The lineage visited 'x' and left it for 'y': a forwarding loop.
  const looping = {
    originTopic: 'x',
    hops: [
      { topic: 'x', at: 1 },
      { topic: 'y', at: 2 },
    ],
  };
  assert.equal(bus.publish('x', { n: 1 }, { lineage: looping }), 0);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  bus.setLineage({ maxLineageHops: 64 });
  // Widening the cap does not excuse the loop: still cycle-rejected.
  assert.equal(bus.publish('x', { n: 2 }, { lineage: looping }), 0);
  assert.equal(bus.getStats().lineageCycleRejections, 2);
  assert.deepEqual(received, []);
  bus.close();
});

test('shadow admission in publishAtomic reads the live cap', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.setLineage({ maxLineageHops: 2 });
  // A 2-hop entry reaches the tightened cap: the whole batch aborts.
  const rejected = bus.publishAtomic([
    { topic: 't', payload: { n: 1 }, lineage: lineageOf(2) },
    { topic: 't', payload: { n: 2 } },
  ]);
  assert.deepEqual(rejected, {
    published: 0,
    rejected: { index: 0, topic: 't', reason: 'lineage-depth' },
  });
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  // Widen the cap and the same batch commits whole.
  bus.setLineage({ maxLineageHops: 64 });
  const committed = bus.publishAtomic([
    { topic: 't', payload: { n: 1 }, lineage: lineageOf(2) },
    { topic: 't', payload: { n: 2 } },
  ]);
  assert.deepEqual(committed, { published: 2 });
  await flush();
  assert.deepEqual(received, [{ n: 1 }, { n: 2 }]);
  assert.equal(bus.getStats().lineageCycleRejections, 1);
  bus.close();
});
