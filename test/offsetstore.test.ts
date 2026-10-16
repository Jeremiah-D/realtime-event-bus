import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import { InMemoryOffsetStore, resolveOffsetStore } from '../src/offsetstore.ts';
import type { OffsetStore, OffsetStoreEntry } from '../src/offsetstore.ts';

/** Recording store: captures every set call behind a real in-memory map. */
class SpyStore implements OffsetStore {
  sets: Array<{ groupId: string; topic: string; seq: number }> = [];
  private readonly backing = new Map<string, Map<string, number>>();

  get(groupId: string, topic: string): number | undefined {
    return this.backing.get(groupId)?.get(topic);
  }

  set(groupId: string, topic: string, seq: number): void {
    this.sets.push({ groupId, topic, seq });
    let byTopic = this.backing.get(groupId);
    if (byTopic == null) {
      byTopic = new Map<string, number>();
      this.backing.set(groupId, byTopic);
    }
    byTopic.set(topic, seq);
  }

  entries(): OffsetStoreEntry[] {
    const out: OffsetStoreEntry[] = [];
    for (const [groupId, byTopic] of this.backing) {
      for (const [topic, seq] of byTopic) out.push({ groupId, topic, seq });
    }
    return out;
  }
}

test('offsetStore: group-level commits are mirrored to the injected store', () => {
  const store = new SpyStore();
  const bus = new EventBus({ offsetStore: store });
  bus.commitOffset('workers', 'jobs.email', 1);
  bus.commitOffset('workers', 'jobs.sms', 2);
  bus.commitOffset('workers', 'jobs.email', 3);
  assert.deepEqual(store.sets, [
    { groupId: 'workers', topic: 'jobs.email', seq: 1 },
    { groupId: 'workers', topic: 'jobs.sms', seq: 2 },
    { groupId: 'workers', topic: 'jobs.email', seq: 3 },
  ]);
  // The read path is unchanged: getCommittedOffsets still serves the
  // bus's in-memory map.
  assert.deepEqual(bus.getCommittedOffsets('workers'), { 'jobs.email': 3, 'jobs.sms': 2 });
  assert.equal(bus.getStats().offsetStoreWriteErrors, 0);
});

test('offsetStore: per-partition and per-instance commits stay in memory', () => {
  const store = new SpyStore();
  const bus = new EventBus({ offsetStore: store });
  bus.subscribeToGroup('g', 't.*', () => {}, { partitions: 2 });
  bus.commitOffset('g', 't.a', 7, { partition: 0 });
  bus.commitOffset('g', 't.a', 9, { instanceId: 'm1' });
  // The store contract is per (groupId, topic) only — rebalance
  // bookkeeping never leaves the process.
  assert.deepEqual(store.sets, []);
  // ...but the checkpoints themselves still work exactly as before.
  assert.deepEqual(bus.getPartitionCommittedOffsets('g', 't.*'), { 0: { 't.a': 7 } });
  assert.deepEqual(bus.getInstanceCommittedOffsets('g', 'm1'), { 't.a': 9 });
});

test('offsetStore: default is in-memory, zero behavior change', () => {
  const bus = new EventBus();
  bus.commitOffset('g', 't', 5);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 5 });
  assert.equal(bus.getStats().offsetStoreWriteErrors, 0);
});

test('offsetStore: invalid stores throw TypeError at construction', () => {
  assert.throws(() => new EventBus({ offsetStore: {} as never }), TypeError);
  assert.throws(() => new EventBus({ offsetStore: { get: () => 1 } as never }), TypeError);
  assert.throws(() => new EventBus({ offsetStore: { set: () => {} } as never }), TypeError);
  assert.throws(() => new EventBus({ offsetStore: 42 as never }), TypeError);
  // null/undefined = absent (default store).
  assert.ok(new EventBus({ offsetStore: null as never }) instanceof EventBus);
});

test('offsetStore: a throwing set never breaks the commit', () => {
  const bad: OffsetStore = {
    get: () => undefined,
    set: () => {
      throw new Error('redis down');
    },
  };
  const bus = new EventBus({ offsetStore: bad });
  bus.commitOffset('g', 't', 5); // must not throw
  bus.commitOffset('g', 't', 6); // must not throw
  // The live process keeps serving from memory; the sick store is only
  // visible in the counter.
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 6 });
  assert.equal(bus.getStats().offsetStoreWriteErrors, 2);
  assert.ok(
    renderPrometheus(bus.getStats()).includes('eventbus_offset_store_write_errors_total 2'),
  );
});

test('offsetStore: recoverFromOffsetStore reseeds a fresh bus, highest-seq-wins', async () => {
  const store = new SpyStore();
  store.set('g', 't.a', 4);
  store.set('g', 't.b', 2);
  const bus = new EventBus({ offsetStore: store });
  assert.deepEqual(bus.getCommittedOffsets('g'), {});
  await bus.recoverFromOffsetStore();
  assert.deepEqual(bus.getCommittedOffsets('g'), { 't.a': 4, 't.b': 2 });
  // Never regresses: a live commit past the stored seq wins.
  bus.commitOffset('g', 't.a', 10);
  await bus.recoverFromOffsetStore();
  assert.deepEqual(bus.getCommittedOffsets('g'), { 't.a': 10, 't.b': 2 });
});

test('offsetStore: recoverFromOffsetStore awaits async entries and skips malformed', async () => {
  const store: OffsetStore = {
    get: () => undefined,
    set: () => {},
    entries: async () => [
      { groupId: 'g', topic: 't.ok', seq: 3 },
      { groupId: '', topic: 't.bad', seq: 3 },
      { groupId: 'g', topic: '', seq: 3 },
      { groupId: 'g', topic: 't.zero', seq: 0 },
      { groupId: 'g', topic: 't.neg', seq: -1 },
      { groupId: 'g', topic: 't.frac', seq: 2.5 },
    ],
  };
  const bus = new EventBus({ offsetStore: store });
  await bus.recoverFromOffsetStore();
  assert.deepEqual(bus.getCommittedOffsets('g'), { 't.ok': 3 });
});

test('offsetStore: recoverFromOffsetStore is a no-op without entries()', async () => {
  const store: OffsetStore = {
    get: () => undefined,
    set: () => {},
  };
  const bus = new EventBus({ offsetStore: store });
  await bus.recoverFromOffsetStore(); // must not throw
  assert.deepEqual(bus.getCommittedOffsets('g'), {});
  // Same for the default in-memory store.
  const plain = new EventBus();
  await plain.recoverFromOffsetStore();
  assert.deepEqual(plain.getCommittedOffsets('g'), {});
});

test('InMemoryOffsetStore mirrors commits and exposes live entries', () => {
  const s = new InMemoryOffsetStore();
  s.set('g', 't', 7);
  assert.equal(s.get('g', 't'), 7);
  assert.equal(s.get('g', 'nope'), undefined);
  assert.equal(s.get('nope', 't'), undefined);
  assert.deepEqual(s.liveEntries(), [{ groupId: 'g', topic: 't', seq: 7 }]);
  // As a recovery source it yields nothing — it only mirrors this
  // process's commits.
  assert.deepEqual(s.entries(), []);
  assert.equal(resolveOffsetStore(undefined, 'X'), undefined);
  assert.equal(resolveOffsetStore(null, 'X'), undefined);
});
