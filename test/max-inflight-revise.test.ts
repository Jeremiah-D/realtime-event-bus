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

test('setMaxInflight raises the cap for new deliveries but keeps unsettled slots', async () => {
  const bus = new EventBus();
  // An explicit concurrency cap above the in-flight cap, so the
  // concurrency gate (EB-159) never masks the cap under revision.
  const { seen, releases, handler } = deferredHandler();
  const sub = bus.subscribe('t', handler, { maxInflight: 1, maxConcurrentDeliveries: 3 });
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(seen, [1], 'the original cap admits one invocation');
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 1, inFlight: 1 },
  ]);

  bus.setMaxInflight(sub.id, 2);
  bus.publish('t', { n: 4 });
  await flush();
  assert.deepEqual(seen, [1, 2], 'the raised cap admits one more queued message');
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 2, inFlight: 2 },
  ]);

  // Settling the first invocation pulls the next queued message; the
  // live in-flight count survives the revision untouched.
  releases[0]();
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 2);

  const events = bus.getSubscriptionAudit({ action: 'max_inflight_revised' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.deepEqual(events[0].detail, { maxInflight: 2, oldMaxInflight: 1 });
  for (const release of releases.splice(0)) release();
  await flush();
  sub.unsubscribe();
});

test('setMaxInflight with null clears the cap and a later call re-enables it (EB-537)', async () => {
  const bus = new EventBus();
  const { seen, releases, handler } = deferredHandler();
  const sub = bus.subscribe('t', handler, { maxInflight: 1, maxConcurrentDeliveries: 3 });
  bus.publish('t', { n: 1 });
  await flush();
  assert.deepEqual(seen, [1]);

  bus.setMaxInflight(sub.id, null);
  assert.deepEqual(
    bus.getStats().inFlightUsage.filter((row) => row.subscriberId === sub.id),
    [],
    'clearing removes the cap row, not just its value',
  );
  // EB-537: a clear no longer refuses the next call — the cap
  // re-installs with a null previous, exactly like a first enable.
  // The still-pending invocation is pre-enable, so the count starts
  // at 0 (see maxinflight-enable.test.ts).
  bus.setMaxInflight(sub.id, 3);
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 3, inFlight: 0 },
  ]);

  const events = bus.getSubscriptionAudit({ action: 'max_inflight_revised' });
  assert.equal(events.length, 2);
  assert.deepEqual(events[0].detail, { maxInflight: null, oldMaxInflight: 1 });
  assert.deepEqual(events[1].detail, { maxInflight: 3, oldMaxInflight: null });
  for (const release of releases.splice(0)) release();
  await flush();
  sub.unsubscribe();
});

test('setMaxInflight leaves the subscribe-time delivery-concurrency cap alone', () => {
  const bus = new EventBus();
  // No explicit concurrency option: it defaults from maxInflight (1).
  const sub = bus.subscribe('t', () => {}, { maxInflight: 1 });
  bus.setMaxInflight(sub.id, 5);
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 5, inFlight: 0 },
  ]);
  assert.deepEqual(
    bus.getStats().deliveryConcurrency.filter((row) => row.subscriberId === sub.id),
    [{ subscriberId: sub.id, pattern: 't', maxConcurrentDeliveries: 1, inFlight: 0, keysInFlight: {} }],
    'the concurrency cap keeps its subscribe-time value',
  );
  sub.unsubscribe();
});

test('setMaxInflight rejects unknown and illegal revisions fail-closed', async () => {
  const bus = new EventBus();
  const { seen, releases, handler } = deferredHandler();
  const sub = bus.subscribe('t', handler, { maxInflight: 1, maxConcurrentDeliveries: 3 });
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(seen, [1]);

  assert.throws(() => bus.setMaxInflight('', 2), RangeError);
  assert.throws(() => bus.setMaxInflight('no-such-sub', 2), RangeError);
  assert.throws(() => bus.setMaxInflight(sub.id, 0), RangeError);
  assert.throws(() => bus.setMaxInflight(sub.id, -1), RangeError);
  assert.throws(() => bus.setMaxInflight(sub.id, 1.5), RangeError);
  assert.throws(() => bus.setMaxInflight(sub.id, Number.NaN), RangeError);
  assert.throws(() => bus.setMaxInflight(sub.id, undefined as unknown as null), RangeError);
  assert.deepEqual(bus.getSubscriptionAudit({ action: 'max_inflight_revised' }), []);

  // The failed revisions left the original cap intact: a fresh
  // publish still queues behind the one unsettled invocation.
  bus.publish('t', { n: 3 });
  await flush();
  assert.deepEqual(seen, [1], 'failed revisions must not move the cap');
  assert.equal(bus.getStats().inFlightUsage[0].maxInflight, 1);
  for (const release of releases.splice(0)) release();
  await flush();
  sub.unsubscribe();
});
