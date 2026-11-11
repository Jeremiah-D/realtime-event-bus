import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const nOf = (msg: BusMessage): number => (msg.payload as { n: number }).n;

/** An async handler whose invocations the test settles by hand. */
function deferredHandler() {
  const seen: number[] = [];
  const releases: Array<() => void> = [];
  const handler = (msg: BusMessage) => {
    seen.push(nOf(msg));
    return new Promise<void>((resolve) => {
      releases.push(resolve);
    });
  };
  return { seen, releases, handler };
}

// EB-466: bus-level terminal max in-flight default live revision.

test('EB-466: the factory bus default is unset (no cap)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxInFlight(), undefined);
});

test('EB-466: a subscriber that omits maxInflight takes the bus default, cap and stats row included', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(1);
  const { seen, releases, handler } = deferredHandler();
  const sub = bus.subscribe('t', handler);
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(seen, [1], 'the bus-default cap admits one invocation');
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 1, inFlight: 1 },
  ]);
  for (const release of releases.splice(0)) release();
  await flush();
  sub.unsubscribe();
});

test('EB-466: an explicit maxInflight beats the bus default in both directions', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(1);
  const { seen, releases, handler } = deferredHandler();
  // Explicit 3 beats a bus default of 1: all three admitted.
  const sub = bus.subscribe('t', handler, { maxInflight: 3, maxConcurrentDeliveries: 3 });
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  assert.equal(bus.getStats().inFlightUsage[0].maxInflight, 3);
  for (const release of releases.splice(0)) release();
  await flush();
  sub.unsubscribe();

  // Explicit 1 beats a bus default of 3: only one admitted.
  bus.setDefaultMaxInFlight(3);
  const second = deferredHandler();
  const sub2 = bus.subscribe('u', second.handler, { maxInflight: 1, maxConcurrentDeliveries: 3 });
  for (let n = 1; n <= 3; n += 1) bus.publish('u', { n });
  await flush();
  assert.deepEqual(second.seen, [1]);
  assert.equal(
    bus.getStats().inFlightUsage.find((row) => row.subscriberId === sub2.id)?.maxInflight,
    1,
  );
  for (const release of second.releases.splice(0)) release();
  await flush();
  sub2.unsubscribe();
});

test('EB-466: clearing with null returns new subscribers to unlimited', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(2);
  bus.setDefaultMaxInFlight(null);
  assert.equal(bus.getDefaultMaxInFlight(), undefined);
  const sub = bus.subscribe('t', () => {});
  assert.deepEqual(
    bus.getStats().inFlightUsage.filter((row) => row.subscriberId === sub.id),
    [],
    'no default means no cap row',
  );
  sub.unsubscribe();
});

test('EB-466: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_max_inflight_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultMaxInFlight(4);
  bus.setDefaultMaxInFlight(4);
  const audit = bus.getSubscriptionAudit({ action: 'default_max_inflight_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, maxInflight: 4 });
});

test('EB-466: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(2);
  for (const bad of [0, -1, 1.5, Number.NaN, Infinity, '3'] as unknown[]) {
    assert.throws(
      () => bus.setDefaultMaxInFlight(bad as number),
      RangeError,
      `max=${String(bad)}`,
    );
    assert.equal(bus.getDefaultMaxInFlight(), 2);
  }
  assert.throws(() => bus.setDefaultMaxInFlight(3, ''), RangeError);
  assert.equal(bus.getDefaultMaxInFlight(), 2);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_max_inflight_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-466: revising the bus default never moves an already-subscribed cap', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(1);
  const { seen, releases, handler } = deferredHandler();
  const sub = bus.subscribe('t', handler);
  bus.setDefaultMaxInFlight(5);
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(seen, [1], 'the subscriber keeps the cap it was baked with');
  assert.equal(bus.getStats().inFlightUsage[0].maxInflight, 1);
  for (const release of releases.splice(0)) release();
  await flush();
  sub.unsubscribe();
});

test('EB-466: the bus default feeds the EB-159 concurrency default', () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(3);
  const sub = bus.subscribe('t', () => {});
  const row = bus.getStats().deliveryConcurrency.find((r) => r.subscriberId === sub.id);
  assert.equal(row?.maxConcurrentDeliveries, 3);
  sub.unsubscribe();
});

test('EB-466: an explicit maxConcurrentDeliveries still wins over the bus default', () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(3);
  const sub = bus.subscribe('t', () => {}, { maxConcurrentDeliveries: 1 });
  const row = bus.getStats().deliveryConcurrency.find((r) => r.subscriberId === sub.id);
  assert.equal(row?.maxConcurrentDeliveries, 1);
  sub.unsubscribe();
});

test('EB-466: setMaxInflight revises a default-capped subscriber and leaves the bus default alone', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(1);
  const { seen, releases, handler } = deferredHandler();
  const sub = bus.subscribe('t', handler, { maxConcurrentDeliveries: 3 });
  bus.publish('t', { n: 1 });
  await flush();
  assert.deepEqual(seen, [1]);

  bus.setMaxInflight(sub.id, 2);
  assert.equal(bus.getDefaultMaxInFlight(), 1, 'the per-subscriber revision must not move the default');
  bus.publish('t', { n: 2 });
  bus.publish('t', { n: 3 });
  await flush();
  assert.deepEqual(seen, [1, 2], 'the raised per-subscriber cap admits one more');
  for (const release of releases.splice(0)) release();
  await flush();
  sub.unsubscribe();
});

test('EB-466: setDefaultMaxInFlight audits each revision with its old and new cap and actor', () => {
  const bus = new EventBus();
  bus.setDefaultMaxInFlight(2, 'ops');
  bus.setDefaultMaxInFlight(null, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'default_max_inflight_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, maxInflight: 2 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 2, maxInflight: null });
});
