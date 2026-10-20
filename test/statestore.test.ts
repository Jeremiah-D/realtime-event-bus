import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { StateStore } from '../src/statestore.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's async flush has delivered. */
const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 20));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-statestore-'));

test('state store: keyed publishes materialize latest-per-key', () => {
  const bus = new EventBus();
  bus.enableStateStore();
  assert.equal(bus.getState('orders', 'o1'), undefined);
  bus.publish('orders', { status: 'new' }, { key: 'o1' });
  bus.publish('orders', { status: 'paid' }, { key: 'o1' });
  const entry = bus.getState('orders', 'o1')!;
  assert.deepEqual(entry.value, { status: 'paid' });
  assert.equal(entry.topic, 'orders');
  assert.equal(entry.key, 'o1');
  assert.equal(entry.seq, 2);
  assert.equal(entry.version, 2);
  assert.ok(entry.at > 0);
  const stats = bus.getStats().stateStore!;
  assert.equal(stats.keys, 1);
  assert.equal(stats.evictedKeys, 0);
});

test('state store: unkeyed publishes never create rows', () => {
  const bus = new EventBus();
  bus.enableStateStore();
  bus.publish('orders', { status: 'new' });
  assert.equal(bus.scanState().length, 0);
  assert.equal(bus.getStats().stateStore!.keys, 0);
});

test('state store: scan filters and deterministic order', () => {
  const bus = new EventBus();
  bus.enableStateStore();
  bus.publish('b', 1, { key: 'k2' });
  bus.publish('a', 1, { key: 'k2' });
  bus.publish('a', 1, { key: 'k1' });
  bus.publish('b', 1, { key: 'k1' });
  assert.deepEqual(
    bus.scanState().map((e) => `${e.topic}/${e.key}`),
    ['a/k1', 'a/k2', 'b/k1', 'b/k2'],
  );
  assert.deepEqual(
    bus.scanState('a').map((e) => e.key),
    ['k1', 'k2'],
  );
  assert.deepEqual(
    bus.scanState(undefined, 'k1').map((e) => `${e.topic}/${e.key}`),
    ['a/k1', 'b/k1'],
  );
  assert.deepEqual(
    bus.scanState('b', 'k2').map((e) => e.key),
    ['k2'],
  );
});

test('state store: maxKeys evicts least-recently-updated first', () => {
  const bus = new EventBus();
  bus.enableStateStore({ maxKeys: 2 });
  bus.publish('t', 1, { key: 'k1' });
  bus.publish('t', 1, { key: 'k2' });
  bus.publish('t', 2, { key: 'k1' }); // k1 becomes most-recently-used
  bus.publish('t', 1, { key: 'k3' }); // evicts k2
  assert.equal(bus.getState('t', 'k2'), undefined);
  assert.notEqual(bus.getState('t', 'k1'), undefined);
  assert.notEqual(bus.getState('t', 'k3'), undefined);
  assert.equal(bus.getStats().stateStore!.evictedKeys, 1);
  assert.equal(bus.getStats().stateStore!.keys, 2);
});

test('state store: changelog topic receives unkeyed records with oldValue', async () => {
  const bus = new EventBus();
  const seen: BusMessage[] = [];
  bus.subscribe('changelog', (m) => seen.push(m));
  bus.enableStateStore({ changelogTopic: 'changelog' });
  bus.publish('orders', { status: 'new' }, { key: 'o1' });
  bus.publish('orders', { status: 'paid' }, { key: 'o1' });
  await flush();
  assert.equal(seen.length, 2);
  // Changelog records are unkeyed — they can never feed back into the store.
  for (const m of seen) assert.equal(m.key, undefined);
  const first = seen[0].payload as Record<string, unknown>;
  assert.equal(first.topic, 'orders');
  assert.equal(first.key, 'o1');
  assert.deepEqual(first.value, { status: 'new' });
  assert.equal(first.oldValue, undefined);
  assert.equal(first.version, 1);
  const second = seen[1].payload as Record<string, unknown>;
  assert.deepEqual(second.value, { status: 'paid' });
  assert.deepEqual(second.oldValue, { status: 'new' });
  assert.equal(second.version, 2);
  // No feedback loop: the changelog topic itself has no state rows.
  assert.equal(bus.getState('changelog', 'o1'), undefined);
  assert.equal(bus.scanState('changelog').length, 0);
});

test('state store: durable restore rebuilds the table across restarts', () => {
  const dir = freshDir();
  const bus1 = new EventBus({ durableLogDir: dir });
  bus1.enableStateStore();
  bus1.publish('orders', { status: 'new' }, { key: 'o1' });
  bus1.publish('orders', { status: 'paid' }, { key: 'o1' });
  bus1.publish('orders', { status: 'new' }, { key: 'o2' });
  bus1.publish('untracked', 1); // unkeyed: no row even after restore

  const bus2 = new EventBus({ durableLogDir: dir });
  bus2.enableStateStore();
  assert.deepEqual(bus2.getState('orders', 'o1')!.value, { status: 'paid' });
  assert.deepEqual(bus2.getState('orders', 'o2')!.value, { status: 'new' });
  // Restore replays history: two restored records count as two updates.
  assert.equal(bus2.getState('orders', 'o1')!.version, 2);
  assert.equal(bus2.getState('untracked', ''), undefined);
  // Live updates keep working on the rebuilt table.
  bus2.publish('orders', { status: 'shipped' }, { key: 'o1' });
  assert.deepEqual(bus2.getState('orders', 'o1')!.value, { status: 'shipped' });
  assert.equal(bus2.getState('orders', 'o1')!.version, 3);
});

test('state store: TTL-expired records never resurrect state on restore', () => {
  const dir = freshDir();
  let now = 1_000_000;
  const bus1 = new EventBus({ durableLogDir: dir, now: () => now });
  bus1.enableStateStore();
  bus1.setTopicTtl('orders', 1000);
  bus1.publish('orders', { status: 'new' }, { key: 'o1' });
  now += 5000; // the record is dead

  const bus2 = new EventBus({ durableLogDir: dir, now: () => now });
  bus2.enableStateStore();
  assert.equal(bus2.getState('orders', 'o1'), undefined);
});

test('state store: restore honors per-topic compression envelopes', () => {
  const dir = freshDir();
  const bus1 = new EventBus({ durableLogDir: dir });
  bus1.enableStateStore();
  bus1.setTopicCompression('big', { thresholdBytes: 10 });
  const big = { blob: 'x'.repeat(200) };
  bus1.publish('big', big, { key: 'k' });

  const bus2 = new EventBus({ durableLogDir: dir });
  bus2.enableStateStore();
  assert.deepEqual(bus2.getState('big', 'k')!.value, big);
});

test('state store: enable validates and refuses double-enable', () => {
  const bus = new EventBus();
  assert.throws(() => bus.enableStateStore({ maxKeys: 0 }), RangeError);
  assert.throws(() => bus.enableStateStore({ maxKeys: -1 }), RangeError);
  assert.throws(() => bus.enableStateStore({ changelogTopic: '' }), RangeError);
  assert.throws(() => bus.enableStateStore(null as never), RangeError);
  bus.enableStateStore();
  assert.throws(() => bus.enableStateStore(), RangeError);
});

test('state store: disabled bus reads empty; stats/metrics gated', () => {
  const bus = new EventBus();
  bus.publish('t', 1, { key: 'k' });
  assert.equal(bus.getState('t', 'k'), undefined);
  assert.deepEqual(bus.scanState(), []);
  assert.equal(bus.getStats().stateStore, undefined);
  assert.ok(!renderPrometheus(bus.getStats()).includes('eventbus_statestore_'));

  bus.enableStateStore();
  bus.publish('t', 2, { key: 'k' });
  const prom = renderPrometheus(bus.getStats());
  assert.ok(prom.includes('eventbus_statestore_keys 1'));
  assert.ok(prom.includes('eventbus_statestore_evicted_keys_total 0'));
});

test('state store: unit-level apply/scan semantics', () => {
  const store = new StateStore({ now: () => 42 }, { maxKeys: 10 });
  assert.equal(store.apply('t', undefined, 1, 1), undefined);
  const e1 = store.apply('t', 'k', 'v1', 1)!;
  assert.equal(e1.version, 1);
  assert.equal(e1.at, 42);
  const e2 = store.apply('t', 'k', 'v2', 2)!;
  assert.equal(e2.version, 2);
  assert.deepEqual(store.scan('t'), [e2]);
  assert.deepEqual(store.snapshot(), { keys: 1, evictedKeys: 0 });
});
