/**
 * Materialized state-store capacity-bound revision (EB-329).
 *
 * `setStateStoreMaxKeys(maxKeys, actor?)` retunes how many keys the
 * table tracks before the least-recently-updated key is evicted —
 * without restarting the bus. Only the bound moves: `apply` /
 * `restore` keep reading the bound live, surviving rows keep their
 * content, `version` and `seq`, and the changelog topic is untouched.
 * Tightening evicts the least-recently-updated rows down to the new
 * bound immediately (counted in `evictedKeys`); loosening only raises
 * the ceiling. A bus whose state store is not enabled refuses
 * fail-closed; illegal values fail closed with the live bound
 * untouched; an identical revision is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { StateStore } from '../src/statestore.ts';

/** Yields until the bus's async flush has delivered. */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'state_store_max_keys_revised' });

test('StateStore.setMaxKeys validates, returns the old bound, and leaves it untouched on illegal values', () => {
  const store = new StateStore({ now: () => 42 }, { maxKeys: 5 });
  assert.equal(store.currentMaxKeys(), 5);
  assert.equal(store.setMaxKeys(3), 5);
  assert.equal(store.currentMaxKeys(), 3);
  for (const bad of [0, -1, 1.5, Number.NaN, '3', Infinity]) {
    assert.throws(() => store.setMaxKeys(bad as never), RangeError);
  }
  // The failed calls changed nothing.
  assert.equal(store.currentMaxKeys(), 3);
  assert.deepEqual(store.snapshot(), { keys: 0, evictedKeys: 0, maxKeys: 3 });
});

test('StateStore.setMaxKeys tightening evicts LRU immediately and counts it; loosening only raises the bound', () => {
  const store = new StateStore({ now: () => 42 }, { maxKeys: 3 });
  store.apply('t', 'k1', 'v1', 1);
  store.apply('t', 'k2', 'v2', 2);
  store.apply('t', 'k3', 'v3', 3);
  store.apply('t', 'k2', 'v2b', 4); // k2 becomes most-recently-used
  assert.equal(store.setMaxKeys(2), 3);
  // k1 was least-recently-updated: evicted; survivors keep version/seq.
  assert.equal(store.get('t', 'k1'), undefined);
  assert.equal(store.get('t', 'k2')!.version, 2);
  assert.equal(store.get('t', 'k2')!.seq, 4);
  assert.equal(store.get('t', 'k3')!.version, 1);
  assert.equal(store.get('t', 'k3')!.seq, 3);
  assert.deepEqual(store.snapshot(), { keys: 2, evictedKeys: 1, maxKeys: 2 });
  // Loosening evicts nothing and only raises the ceiling.
  assert.equal(store.setMaxKeys(10), 2);
  assert.deepEqual(store.snapshot(), { keys: 2, evictedKeys: 1, maxKeys: 10 });
  store.apply('t', 'k4', 'v4', 5);
  assert.equal(store.snapshot().keys, 3);
});

test('setStateStoreMaxKeys requires an enabled store and a valid actor', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.setStateStoreMaxKeys(5),
    /state store is not enabled/,
  );
  assert.throws(() => bus.setStateStoreMaxKeys(5, ''), /actor must be a non-empty string/);
  assert.throws(() => bus.setStateStoreMaxKeys(5, 42 as never), RangeError);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getStats().stateStore, undefined);

  bus.enableStateStore();
  // The default bound is 10000.
  assert.equal(bus.getStats().stateStore!.maxKeys, 10000);
  assert.throws(() => bus.setStateStoreMaxKeys(5, '' as never), RangeError);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getStats().stateStore!.maxKeys, 10000);
});

test('revision rejects illegal values and leaves the live bound and audit untouched', () => {
  const bus = new EventBus();
  bus.enableStateStore({ maxKeys: 3 });
  for (const bad of [0, -1, 1.5, Number.NaN, '100', Infinity]) {
    assert.throws(() => bus.setStateStoreMaxKeys(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getStats().stateStore!.maxKeys, 3);
  // A valid revision afterwards still works on the untouched bound.
  bus.setStateStoreMaxKeys(7, 'ops');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    maxKeys: 7,
    oldMaxKeys: 3,
    evictedKeys: 0,
  });
  assert.equal(audit(bus)[0].actor, 'ops');
  assert.equal(bus.getStats().stateStore!.maxKeys, 7);
});

test('an identical revision is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.enableStateStore({ maxKeys: 5 });
  bus.setStateStoreMaxKeys(5);
  assert.equal(audit(bus).length, 0);

  const defaulted = new EventBus();
  defaulted.enableStateStore();
  // The default bound is 10000: re-affirming it is a no-op.
  defaulted.setStateStoreMaxKeys(10000);
  assert.equal(audit(defaulted).length, 0);
});

test('tightening evicts LRU rows immediately and keeps survivors intact', () => {
  const bus = new EventBus();
  bus.enableStateStore({ maxKeys: 3 });
  bus.publish('t', 'v1', { key: 'k1' });
  bus.publish('t', 'v2', { key: 'k2' });
  bus.publish('t', 'v3', { key: 'k3' });
  bus.publish('t', 'v1b', { key: 'k1' }); // k1 becomes most-recently-used
  bus.setStateStoreMaxKeys(2, 'ops');
  // k2 was least-recently-updated: evicted at once, not on next publish.
  assert.equal(bus.getState('t', 'k2'), undefined);
  assert.equal(bus.getState('t', 'k1')!.version, 2);
  assert.equal(bus.getState('t', 'k1')!.seq, 4);
  assert.equal(bus.getState('t', 'k3')!.version, 1);
  assert.deepEqual(bus.getStats().stateStore, { keys: 2, evictedKeys: 1, maxKeys: 2 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    maxKeys: 2,
    oldMaxKeys: 3,
    evictedKeys: 1,
  });
});

test('loosening keeps every row and only raises the ceiling', () => {
  const bus = new EventBus();
  bus.enableStateStore({ maxKeys: 2 });
  bus.publish('t', 1, { key: 'k1' });
  bus.publish('t', 2, { key: 'k2' });
  bus.setStateStoreMaxKeys(5);
  assert.equal(bus.getStats().stateStore!.keys, 2);
  assert.equal(bus.getStats().stateStore!.evictedKeys, 0);
  bus.publish('t', 3, { key: 'k3' });
  bus.publish('t', 4, { key: 'k4' });
  bus.publish('t', 5, { key: 'k5' });
  assert.equal(bus.getStats().stateStore!.keys, 5);
  assert.equal(bus.getStats().stateStore!.evictedKeys, 0);
  assert.equal(audit(bus)[0].detail.evictedKeys, 0);
});

test('a tightened bound still governs the next publish', () => {
  const bus = new EventBus();
  bus.enableStateStore({ maxKeys: 3 });
  bus.publish('t', 1, { key: 'k1' });
  bus.publish('t', 2, { key: 'k2' });
  bus.publish('t', 3, { key: 'k3' });
  bus.setStateStoreMaxKeys(2);
  assert.equal(bus.getStats().stateStore!.keys, 2);
  // The next publish evicts under the new bound.
  bus.publish('t', 4, { key: 'k4' });
  assert.equal(bus.getStats().stateStore!.keys, 2);
  assert.equal(bus.getStats().stateStore!.evictedKeys, 2);
  assert.equal(bus.getState('t', 'k1'), undefined);
  assert.notEqual(bus.getState('t', 'k4'), undefined);
});

test('revision leaves the changelog topic working', async () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  bus.subscribe('changelog', (m) => seen.push(m));
  bus.enableStateStore({ maxKeys: 2, changelogTopic: 'changelog' });
  bus.publish('orders', { status: 'new' }, { key: 'o1' });
  bus.publish('orders', { status: 'new' }, { key: 'o2' });
  bus.setStateStoreMaxKeys(1);
  assert.equal(bus.getStats().stateStore!.keys, 1);
  bus.publish('orders', { status: 'new' }, { key: 'o3' });
  await flush();
  // The post-revision update still emits a changelog record.
  assert.equal(seen.length, 3);
  const last = seen[2].payload as Record<string, unknown>;
  assert.equal(last.key, 'o3');
  assert.equal(last.version, 1);
  // The surviving row kept its version.
  assert.equal(bus.getState('orders', 'o3')!.version, 1);
  assert.equal(bus.getStats().stateStore!.evictedKeys, 2);
});

test('getStats().stateStore.maxKeys tracks the live bound', () => {
  const bus = new EventBus();
  bus.enableStateStore();
  assert.equal(bus.getStats().stateStore!.maxKeys, 10000);
  bus.setStateStoreMaxKeys(250);
  assert.equal(bus.getStats().stateStore!.maxKeys, 250);
  bus.setStateStoreMaxKeys(5000);
  assert.equal(bus.getStats().stateStore!.maxKeys, 5000);
});
