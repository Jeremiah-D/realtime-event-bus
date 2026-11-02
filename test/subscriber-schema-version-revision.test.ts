import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Collects every delivered message on the pattern. */
const collect = (bus: EventBus, pattern: string, opts?: { schemaVersion?: number }) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(
    pattern,
    (msg) => {
      received.push(msg);
    },
    opts === undefined ? undefined : { schemaVersion: opts.schemaVersion },
  );
  return { received, sub };
};

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'subscriber_schema_version_revised' });

test('setSubscriberSchemaVersion rejects unknown, empty, and invalid revisions fail-closed', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const { received, sub } = collect(bus, 'orders', { schemaVersion: 2 });
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { v: 2, item: 'book' });
  for (const bad of [
    () => bus.setSubscriberSchemaVersion('no-such-sub', 2),
    () => bus.setSubscriberSchemaVersion('', 2),
    () => bus.setSubscriberSchemaVersion(sub.id, -1),
    () => bus.setSubscriberSchemaVersion(sub.id, 1.5),
    () => bus.setSubscriberSchemaVersion(sub.id, Number.NaN),
    () => bus.setSubscriberSchemaVersion(sub.id, Number.POSITIVE_INFINITY),
    () => bus.setSubscriberSchemaVersion(sub.id, undefined as never),
    () => bus.setSubscriberSchemaVersion(sub.id, '2' as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(audit(bus).length, 0, 'failed revisions must not audit');
  // The live version is untouched: the next v1 write still migrates to v2.
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 2);
  assert.deepEqual(received[1].payload, { v: 2, item: 'book' });
  assert.equal(bus.getStats().migrationFailures, 0);
  sub.unsubscribe();
});

test('setSubscriberSchemaVersion retunes delivery from the next publish, with full audit fields', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const { received, sub } = collect(bus, 'orders', { schemaVersion: 1 });
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { v: 1, item: 'book' });
  bus.setSubscriberSchemaVersion(sub.id, 2, 'ops');
  const events = audit(bus);
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 'orders');
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { schemaVersion: 2, oldSchemaVersion: 1 });
  // The next v1 write migrates to the new expected version.
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 2);
  assert.deepEqual(received[1].payload, { v: 2, item: 'book' });
  assert.equal(bus.getStats().migrationFailures, 0);
  sub.unsubscribe();
});

test('setSubscriberSchemaVersion judges an already-queued message by the live version at delivery', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const { received, sub } = collect(bus, 'orders', { schemaVersion: 1 });
  // The fan-out gate passes under the old version (write == expected),
  // so the message is queued; the revision lands before the flush.
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  bus.setSubscriberSchemaVersion(sub.id, 2);
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { v: 2, item: 'book' });
  assert.equal(bus.getStats().migrationFailures, 0);
  sub.unsubscribe();
});

test('setSubscriberSchemaVersion to an unmigratable version drops fail-closed from the next publish', async () => {
  const bus = new EventBus();
  // Only the 1->2 hop exists: expecting v3 has no path from a v1 write.
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const { received, sub } = collect(bus, 'orders', { schemaVersion: 1 });
  bus.publish('orders', { v: 1 }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 1);
  bus.setSubscriberSchemaVersion(sub.id, 3);
  assert.deepEqual(audit(bus)[0].detail, { schemaVersion: 3, oldSchemaVersion: 1 });
  bus.publish('orders', { v: 1 }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 1, 'no path to the new version: dropped fail-closed');
  assert.equal(bus.getStats().migrationFailures, 1);
  sub.unsubscribe();
});

test('setSubscriberSchemaVersion null clears to undeclared pass-through delivery', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const { received, sub } = collect(bus, 'orders', { schemaVersion: 2 });
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { v: 2, item: 'book' });
  bus.setSubscriberSchemaVersion(sub.id, null);
  assert.deepEqual(audit(bus)[0].detail, { schemaVersion: null, oldSchemaVersion: 2 });
  // No version declared any more: the raw write payload delivers.
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 2);
  assert.deepEqual(received[1].payload, { v: 1, item: 'book' });
  assert.equal(bus.getStats().migrationFailures, 0);
  sub.unsubscribe();
});

test('setSubscriberSchemaVersion same-value is a no-op with no audit', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const { received, sub } = collect(bus, 'orders', { schemaVersion: 2 });
  const plain = bus.subscribe('orders', () => {});
  bus.setSubscriberSchemaVersion(sub.id, 2);
  bus.setSubscriberSchemaVersion(sub.id, 2);
  // `null` on a subscriber that declared no version restates "undeclared".
  bus.setSubscriberSchemaVersion(plain.id, null);
  assert.equal(audit(bus).length, 0, 'restating the current version must not audit');
  // The live versions still govern: the v2 subscriber migrates, the
  // undeclared one sees the raw write.
  bus.publish('orders', { v: 1 }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { v: 2 });
  sub.unsubscribe();
  plain.unsubscribe();
});

test('setSubscriberSchemaVersion lets an undeclared subscriber gain an expected version', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const { received, sub } = collect(bus, 'orders');
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].payload, { v: 1, item: 'book' });
  bus.setSubscriberSchemaVersion(sub.id, 2);
  assert.deepEqual(audit(bus)[0].detail, { schemaVersion: 2, oldSchemaVersion: null });
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(received.length, 2);
  assert.deepEqual(received[1].payload, { v: 2, item: 'book' });
  sub.unsubscribe();
});

test('setSubscriberSchemaVersion rejects an illegal actor with the live version untouched', () => {
  const bus = new EventBus();
  const { sub } = collect(bus, 'orders', { schemaVersion: 1 });
  assert.throws(() => bus.setSubscriberSchemaVersion(sub.id, 2, ''), RangeError);
  assert.equal(audit(bus).length, 0);
  // The live version still governs: revising to the same value is a
  // no-op, so the original declaration stands.
  bus.setSubscriberSchemaVersion(sub.id, 1);
  assert.equal(audit(bus).length, 0);
  sub.unsubscribe();
});
