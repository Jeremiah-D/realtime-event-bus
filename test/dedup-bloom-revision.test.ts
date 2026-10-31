/**
 * Bus-level dedup bloom sizing revision (EB-312).
 *
 * `setDedupBloom` resizes the EB-214 bloom pre-filter live — without
 * restarting the bus. Only a bus that enabled `dedupBloom` can be
 * revised; illegal values fail closed with the live filter and the
 * audit log untouched; an identical (or empty) revision is a no-op
 * with no audit. The revision builds a fresh filter at the merged
 * sizing and rebuilds it from the live dedup table, so no dedup
 * decision is lost — the map stays authoritative — and the insertion
 * count restarts from the live table size.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { BloomFilter } from '../src/bloom.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const bloomStats = (bus: EventBus) => bus.getStats().bloomStats!;
const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'dedup_bloom_revised' });

test('setDedupBloom fails closed when dedupBloom is not enabled', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setDedupBloom({ expectedInsertions: 500 }), RangeError);
  assert.throws(() => bus.setDedupBloom({ falsePositiveRate: 0.001 }), RangeError);
  assert.equal(audit(bus).length, 0);
  bus.close();
});

test('setDedupBloom rejects illegal values and leaves the live filter untouched', () => {
  const bus = new EventBus({
    dedupBloom: { expectedInsertions: 1_000, falsePositiveRate: 0.01 },
  });
  bus.subscribe('t', () => {});
  for (let i = 0; i < 5; i += 1) {
    bus.publishIdempotent('t', { n: 1 }, { messageId: `msg-${i}` });
  }
  const before = bloomStats(bus);
  assert.equal(before.insertions, 5);
  const bad: Array<unknown> = [
    { expectedInsertions: 0 },
    { expectedInsertions: -5 },
    { expectedInsertions: 1.5 },
    { expectedInsertions: Number.NaN },
    { expectedInsertions: '100' },
    { falsePositiveRate: 0 },
    { falsePositiveRate: 1 },
    { falsePositiveRate: -0.1 },
    { falsePositiveRate: 1.5 },
    { falsePositiveRate: Number.NaN },
    { falsePositiveRate: '0.01' },
    // One good field does not save a bad sibling: the whole call fails.
    { expectedInsertions: 500, falsePositiveRate: 0 },
    { expectedInsertions: 0, falsePositiveRate: 0.001 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setDedupBloom(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: same sizing, same claims, no audit.
  assert.deepEqual(bloomStats(bus), before);
  assert.equal(audit(bus).length, 0);
  // A valid revision afterwards still works on the untouched filter.
  bus.setDedupBloom({ expectedInsertions: 2_000 });
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(2_000, 0.01).size);
  assert.equal(bloomStats(bus).insertions, 5);
  assert.equal(audit(bus).length, 1);
  bus.close();
});

test('setDedupBloom with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus({
    dedupBloom: { expectedInsertions: 500, falsePositiveRate: 0.02 },
  });
  bus.setDedupBloom({ expectedInsertions: 500 });
  bus.setDedupBloom({ falsePositiveRate: 0.02 });
  bus.setDedupBloom({ expectedInsertions: 500, falsePositiveRate: 0.02 });
  bus.setDedupBloom({});
  bus.setDedupBloom(null as never);
  assert.equal(audit(bus).length, 0);
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(500, 0.02).size);
  bus.close();
});

test('partial revision moves only the provided field', () => {
  const bus = new EventBus({
    dedupBloom: { expectedInsertions: 1_000, falsePositiveRate: 0.01 },
  });
  bus.setDedupBloom({ falsePositiveRate: 0.001 });
  let entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    expectedInsertions: 1_000,
    oldExpectedInsertions: 1_000,
    falsePositiveRate: 0.001,
    oldFalsePositiveRate: 0.01,
  });
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(1_000, 0.001).size);
  bus.setDedupBloom({ expectedInsertions: 100 });
  entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[1].detail, {
    expectedInsertions: 100,
    oldExpectedInsertions: 1_000,
    falsePositiveRate: 0.001,
    oldFalsePositiveRate: 0.001,
  });
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(100, 0.001).size);
  bus.close();
});

test('revision resizes the filter: bit size and hash count follow the new sizing', () => {
  const bus = new EventBus({ dedupBloom: true });
  // Defaults: sized for idempotencyMaxEntries (10_000) at p = 0.01.
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(10_000, 0.01).size);
  assert.equal(bloomStats(bus).hashFunctions, new BloomFilter(10_000, 0.01).hashes);
  bus.setDedupBloom({ expectedInsertions: 100, falsePositiveRate: 0.001 });
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(100, 0.001).size);
  assert.equal(bloomStats(bus).hashFunctions, new BloomFilter(100, 0.001).hashes);
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    expectedInsertions: 100,
    oldExpectedInsertions: 10_000,
    falsePositiveRate: 0.001,
    oldFalsePositiveRate: 0.01,
  });
  bus.close();
});

test('revision rebuilds from the live table: dedup decisions are unchanged', () => {
  const bus = new EventBus({
    dedupBloom: { expectedInsertions: 1_000, falsePositiveRate: 0.01 },
  });
  bus.subscribe('t', () => {});
  const ids: string[] = [];
  for (let i = 0; i < 50; i += 1) ids.push(`msg-${i}`);
  for (const id of ids) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: id }).duplicate,
      false,
    );
  }
  assert.equal(bloomStats(bus).insertions, 50);
  // Resize well below the live claim count — the filter is now
  // undersized by configuration, but the map stays authoritative.
  bus.setDedupBloom({ expectedInsertions: 10, falsePositiveRate: 0.5 });
  assert.equal(bloomStats(bus).insertions, 50);
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(10, 0.5).size);
  // Every retry is still suppressed exactly once...
  for (const id of ids) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: id }).duplicate,
      true,
      `retry of ${id} must still be suppressed after the revision`,
    );
  }
  // ...and fresh identities are still admitted.
  for (let i = 50; i < 60; i += 1) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: `msg-${i}` }).duplicate,
      false,
    );
  }
  bus.close();
});

test('revision resets the insertion count to the live table size', () => {
  const clock = controllableClock();
  const bus = new EventBus({
    now: clock.now,
    dedupBloom: { expectedInsertions: 1_000, falsePositiveRate: 0.01 },
    idempotencyWindowMs: 1_000,
  });
  bus.subscribe('t', () => {});
  for (let i = 0; i < 20; i += 1) {
    bus.publishIdempotent('t', { n: 1 }, { messageId: `msg-${i}` });
  }
  assert.equal(bloomStats(bus).insertions, 20);
  // Expire every claim, then publish once more: pruneDedup drops the
  // expired table entries, but the saturation guard does not fire at
  // this sizing (21 insertions < 2x the sized capacity), so the filter
  // still carries 21 stale insertions over a 1-entry live table.
  clock.nowMs += 2_000;
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-new' });
  assert.equal(bloomStats(bus).insertions, 21);
  bus.setDedupBloom({ expectedInsertions: 100 });
  // The rebuild restarts the count from the live table: 1 live claim.
  assert.equal(bloomStats(bus).insertions, 1);
  // The surviving claim still suppresses its retry exactly.
  assert.equal(
    bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-new' }).duplicate,
    true,
  );
  // And the expired identities are publishable again.
  assert.equal(
    bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-0' }).duplicate,
    false,
  );
  bus.close();
});

test('revision on a bus with no claims leaves insertions at zero', () => {
  const bus = new EventBus({
    dedupBloom: { expectedInsertions: 1_000, falsePositiveRate: 0.01 },
  });
  assert.equal(bloomStats(bus).insertions, 0);
  bus.setDedupBloom({ expectedInsertions: 50, falsePositiveRate: 0.001 });
  assert.equal(bloomStats(bus).insertions, 0);
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(50, 0.001).size);
  assert.equal(audit(bus).length, 1);
  bus.close();
});

test('dedup stays exact under heavy false positives after a shrink', () => {
  // A tiny filter at p = 0.5 over 200 claims: false positives are
  // guaranteed, so every lookup exercises the map fallback path — and
  // resizing down to it mid-flight must not change any decision.
  const bus = new EventBus({
    dedupBloom: { expectedInsertions: 1_000, falsePositiveRate: 0.01 },
  });
  bus.subscribe('t', () => {});
  const ids: string[] = [];
  for (let i = 0; i < 200; i += 1) ids.push(`msg-${i}`);
  for (const id of ids) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: id }).duplicate,
      false,
    );
  }
  bus.setDedupBloom({ expectedInsertions: 10, falsePositiveRate: 0.5 });
  for (const id of ids) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: id }).duplicate,
      true,
      `retry of ${id} must be suppressed after shrinking the filter`,
    );
  }
  for (let i = 200; i < 210; i += 1) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: `msg-${i}` }).duplicate,
      false,
    );
  }
  bus.close();
});

test('the saturation guard keeps using the revised sizing', () => {
  const clock = controllableClock();
  const bus = new EventBus({
    now: clock.now,
    dedupBloom: { expectedInsertions: 1_000, falsePositiveRate: 0.01 },
    idempotencyWindowMs: 1_000,
  });
  bus.subscribe('t', () => {});
  // Shrink the sizing so the guard's 2x threshold is 20 insertions.
  bus.setDedupBloom({ expectedInsertions: 10 });
  for (let i = 0; i < 100; i += 1) {
    bus.publishIdempotent('t', { n: 1 }, { messageId: `msg-${i}` });
  }
  assert.ok(bloomStats(bus).insertions >= 100);
  // Expire every claim, then publish once more: the guard rebuilds the
  // filter from the (now tiny) live table under the revised sizing.
  clock.nowMs += 2_000;
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-new' });
  assert.ok(
    bloomStats(bus).insertions <= 2,
    `guard should rebuild under the revised sizing, got ${bloomStats(bus).insertions} insertions`,
  );
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(10, 0.01).size);
  bus.close();
});
