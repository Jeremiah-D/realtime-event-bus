import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { type Delivery } from '../src/ack.ts';

/**
 * Subscriber maxInflight live enable (EB-537):
 * `setMaxInflight` on a subscription that never opted into
 * `maxInflight` installs a cap instead of refusing: a number
 * installs `{ max, inFlight: 0 }` — only invocations admitted after
 * the enable count, so a slow handler already executing when the cap
 * lands is not in the count and its settle cannot strand it (see the
 * note on `setMaxInflight`); `null` on a never-capped subscriber is
 * a silent no-op with no audit; a batch subscriber refuses the
 * enable, exactly as subscribe time refuses the combination. The
 * enable is live-only: a restart restores the uncapped state.
 * Audited as `max_inflight_revised` with `oldMaxInflight: null`.
 *
 * Determinism tricks (as in `max-inflight-revise.test.ts`): a
 * deferred handler settles invocations by hand, and an explicit
 * `maxConcurrentDeliveries` keeps the EB-159 gate from masking the
 * cap under test.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const nOf = (msg: BusMessage): number => (msg.payload as { n: number }).n;

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-maxinflight-enable-'));

const revisionAudits = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'max_inflight_revised' });

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

test('EB-537: enabling caps the drain from the next flush and never wedges the backlog', async () => {
  const bus = new EventBus();
  const { seen, releases, handler } = deferredHandler();
  const sub = bus.subscribe('t', handler, { maxConcurrentDeliveries: 2 });
  for (let n = 1; n <= 5; n += 1) bus.publish('t', { n });
  await flush();
  // No cap yet: the concurrency gate alone admits two executions.
  assert.deepEqual(seen, [1, 2]);
  assert.deepEqual(bus.getStats().inFlightUsage, []);

  bus.setMaxInflight(sub.id, 2);
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, { maxInflight: 2, oldMaxInflight: null });
  // The two handlers already executing are pre-enable: they hold no
  // in-flight slot, so the count starts at 0, not 2.
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 2, inFlight: 0 },
  ]);

  // Settling a pre-enable invocation frees its delivery slot and the
  // drain admits exactly one more — the cap, not the settled
  // invocation, is what bounds the new admission.
  releases.shift()!();
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 1);
  releases.shift()!();
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 2);
  // Settling a post-enable invocation frees its slot for the last one.
  releases.shift()!();
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  for (const release of releases.splice(0)) release();
  await flush();
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);
  sub.unsubscribe();
});

test('EB-537: pre-enable executions are not in the count — the concurrency row still holds them', async () => {
  const bus = new EventBus();
  const { seen, releases, handler } = deferredHandler();
  const sub = bus.subscribe('t', handler, { maxConcurrentDeliveries: 2 });
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(seen, [1, 2]);

  bus.setMaxInflight(sub.id, 2);
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 2, inFlight: 0 },
  ]);
  // The two executing invocations are still visible — under the
  // delivery-concurrency row, where they were counted all along.
  assert.deepEqual(
    bus.getStats().deliveryConcurrency.filter((row) => row.subscriberId === sub.id),
    [{ subscriberId: sub.id, pattern: 't', maxConcurrentDeliveries: 2, inFlight: 2, keysInFlight: {} }],
  );
  for (const release of releases.splice(0)) release();
  await flush();
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);
  sub.unsubscribe();
});

test('EB-537: a batch subscriber refuses the enable, live and audit untouched', async () => {
  const bus = new EventBus();
  const received: BusMessage[][] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg as BusMessage[]), { batch: true });
  assert.throws(() => bus.setMaxInflight(sub.id, 2), RangeError);
  assert.deepEqual(bus.getStats().inFlightUsage, []);
  assert.deepEqual(revisionAudits(bus), []);
  // `null` on a batch subscriber is the same silent no-op as on any
  // never-capped subscriber — there is nothing to clear.
  bus.setMaxInflight(sub.id, null);
  assert.deepEqual(revisionAudits(bus), []);
  sub.unsubscribe();
});

test('EB-537: null on a never-capped subscriber is a silent no-op with no audit', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  const sub = bus.subscribe('t', (msg) => seen.push(nOf(msg)));
  bus.setMaxInflight(sub.id, null);
  assert.deepEqual(revisionAudits(bus), []);
  assert.deepEqual(bus.getStats().inFlightUsage, []);
  // Still uncapped: a burst flows without the cap gating it.
  for (let n = 1; n <= 3; n += 1) bus.publish('t', { n });
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  sub.unsubscribe();
});

test('EB-537: null on an enabled subscriber still clears, and a later call re-enables (EB-264 clear is no longer terminal)', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  bus.setMaxInflight(sub.id, 2);
  bus.setMaxInflight(sub.id, null);
  assert.deepEqual(
    bus.getStats().inFlightUsage.filter((row) => row.subscriberId === sub.id),
    [],
    'clearing removes the cap row, not just its value',
  );
  let events = revisionAudits(bus);
  assert.equal(events.length, 2);
  assert.deepEqual(events[1].detail, { maxInflight: null, oldMaxInflight: 2 });

  // A clear no longer refuses the next call: the cap re-installs
  // with a null previous, exactly like a first-time enable.
  bus.setMaxInflight(sub.id, 3);
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 3, inFlight: 0 },
  ]);
  events = revisionAudits(bus);
  assert.equal(events.length, 3);
  assert.deepEqual(events[2].detail, { maxInflight: 3, oldMaxInflight: null });

  // And a revision on the re-enabled cap still moves only the cap.
  bus.setMaxInflight(sub.id, 1);
  assert.deepEqual(revisionAudits(bus)[3].detail, { maxInflight: 1, oldMaxInflight: 3 });
  sub.unsubscribe();
});

test('EB-537: unknown, empty, and illegal enables throw RangeError and leave the subscriber uncapped', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  for (const bad of [
    () => bus.setMaxInflight('', 2),
    () => bus.setMaxInflight('no-such-sub', 2),
    () => bus.setMaxInflight(sub.id, 0),
    () => bus.setMaxInflight(sub.id, -1),
    () => bus.setMaxInflight(sub.id, 1.5),
    () => bus.setMaxInflight(sub.id, Number.NaN),
    () => bus.setMaxInflight(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setMaxInflight(sub.id, undefined as unknown as null),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.deepEqual(revisionAudits(bus), [], 'failed enables must not audit');
  assert.deepEqual(bus.getStats().inFlightUsage, [], 'failed enables must not install a cap');
  // The subscriber is still cleanly uncapped: a valid enable works.
  bus.setMaxInflight(sub.id, 2);
  assert.equal(revisionAudits(bus).length, 1);
  sub.unsubscribe();
});

test('EB-537: enabling never recomputes the subscribe-time delivery-concurrency cap', () => {
  const bus = new EventBus();
  // Explicit concurrency above the cap: the enable must not move it.
  const explicit = bus.subscribe('a', () => {}, { maxConcurrentDeliveries: 3 });
  bus.setMaxInflight(explicit.id, 2);
  assert.deepEqual(
    bus.getStats().deliveryConcurrency.filter((row) => row.subscriberId === explicit.id),
    [{ subscriberId: explicit.id, pattern: 'a', maxConcurrentDeliveries: 3, inFlight: 0, keysInFlight: {} }],
  );
  // Defaulted concurrency (1, serial) stays 1 as well.
  const plain = bus.subscribe('b', () => {});
  bus.setMaxInflight(plain.id, 2);
  assert.deepEqual(
    bus.getStats().deliveryConcurrency.filter((row) => row.subscriberId === plain.id),
    [{ subscriberId: plain.id, pattern: 'b', maxConcurrentDeliveries: 1, inFlight: 0, keysInFlight: {} }],
  );
  explicit.unsubscribe();
  plain.unsubscribe();
});

test('EB-537: a reliable subscriber settles pre-enable deliveries without underflowing the count', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      seen.push(nOf(d.msg));
    },
    { ackTimeoutMs: 5000 },
  );
  bus.publish('t', { n: 1 });
  await flush();
  assert.deepEqual(seen, [1]);

  // One delivery is outstanding unacked — and not executing, so the
  // enable starts the count at 0, not 1.
  bus.setMaxInflight(sub.id, 1);
  assert.deepEqual(bus.getStats().inFlightUsage, [
    { subscriberId: sub.id, pattern: 't', maxInflight: 1, inFlight: 0 },
  ]);
  // Settling it must not drive the count below 0: the tracker's
  // onSettle hook clamps the release.
  deliveries[0].ack();
  await flush();
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);

  // The cap now binds new admissions: one flows, the next waits.
  bus.publish('t', { n: 2 });
  bus.publish('t', { n: 3 });
  await flush();
  assert.deepEqual(seen, [1, 2]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 1);
  deliveries[1].ack();
  await flush();
  assert.deepEqual(seen, [1, 2, 3]);
  for (const d of deliveries) d.ack();
  await flush();
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);
  sub.unsubscribe();
});

test('EB-537: the cap admits max more on top of pre-enable unacked reliable deliveries', async () => {
  const bus = new EventBus();
  const deliveries: Array<Delivery<BusMessage>> = [];
  const seen: number[] = [];
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      seen.push(nOf(d.msg));
    },
    { ackTimeoutMs: 5000 },
  );
  bus.publish('t', { n: 1 });
  bus.publish('t', { n: 2 });
  await flush();
  assert.deepEqual(seen, [1, 2]);

  // Two deliveries are outstanding unacked; the enable admits two
  // more on top of them — the cap counts only post-enable admissions.
  bus.setMaxInflight(sub.id, 2);
  bus.publish('t', { n: 3 });
  bus.publish('t', { n: 4 });
  bus.publish('t', { n: 5 });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4]);
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 2);
  // Acking a pre-enable delivery releases a slot (clamped), so the
  // last queued message flows.
  deliveries[0].ack();
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5]);
  for (const d of deliveries) d.ack();
  await flush();
  assert.equal(bus.getStats().inFlightUsage[0].inFlight, 0);
  sub.unsubscribe();
});

test('EB-537: the live enable is gone after a restart', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const sub = bus.subscribe('t', () => {});
  bus.setMaxInflight(sub.id, 2);
  assert.equal(revisionAudits(bus).length, 1);
  sub.unsubscribe();

  // A fresh bus — the restart — has no record of the live enable: a
  // plain re-subscription runs uncapped, so a burst flows whole.
  const bus2 = new EventBus({ durableLogDir: dir });
  const seen: number[] = [];
  const deliveries: Array<Delivery<BusMessage>> = [];
  const sub2 = bus2.subscribeReliable(
    't',
    (d) => {
      deliveries.push(d);
      seen.push(nOf(d.msg));
    },
    { ackTimeoutMs: 5000 },
  );
  for (let n = 1; n <= 5; n += 1) bus2.publish('t', { n });
  await flush();
  assert.deepEqual(seen, [1, 2, 3, 4, 5], 'the enable must not survive a restart');
  assert.deepEqual(bus2.getStats().inFlightUsage, []);
  assert.deepEqual(revisionAudits(bus2), []);
  sub2.unsubscribe();
});
