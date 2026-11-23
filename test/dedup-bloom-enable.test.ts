/**
 * Dedup bloom live enable (EB-546):
 * `setDedupBloom` on a bus that never enabled `dedupBloom` installs
 * the filter live — no restart, no lost subscriptions — with the
 * revision resolved through the construction-time rule
 * (`resolveDedupBloomOptions`: `true` takes every default, a partial
 * object falls back per field). The fresh filter is rebuilt from the
 * live dedup table, so claims made before the enable are already in
 * it, and the enable is audited with both old values `null`. It is
 * live-only: a restart restores the unconfigured state. Claims
 * admitted before the enable keep their claims — the map was
 * authoritative all along — so nothing is decided retroactively.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { BloomFilter } from '../src/bloom.ts';

const bloomStats = (bus: EventBus) => bus.getStats().bloomStats!;
const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'dedup_bloom_revised' });

const DEFAULTS_DETAIL = {
  expectedInsertions: 10_000,
  oldExpectedInsertions: null,
  falsePositiveRate: 0.01,
  oldFalsePositiveRate: null,
};

test('the first enable with {} takes the construction defaults', () => {
  const bus = new EventBus();
  bus.setDedupBloom({});
  assert.equal(bloomStats(bus).enabled, true);
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(10_000, 0.01).size);
  assert.equal(bloomStats(bus).hashFunctions, new BloomFilter(10_000, 0.01).hashes);
  assert.equal(bloomStats(bus).insertions, 0);
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.equal(entries[0].actor, 'system');
  assert.deepEqual(entries[0].detail, DEFAULTS_DETAIL);
  bus.close();
});

test('setDedupBloom(true) enables with the defaults', () => {
  const bus = new EventBus();
  bus.setDedupBloom(true);
  assert.equal(bloomStats(bus).enabled, true);
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(10_000, 0.01).size);
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, DEFAULTS_DETAIL);
  bus.close();
});

test('a partial enable falls back to the defaults per field', () => {
  const bus = new EventBus();
  bus.setDedupBloom({ expectedInsertions: 500 });
  const entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    expectedInsertions: 500,
    oldExpectedInsertions: null,
    falsePositiveRate: 0.01,
    oldFalsePositiveRate: null,
  });
  // A post-enable call revises from the live enabled values — the
  // enable is not one-shot, and the old values are now real.
  bus.setDedupBloom({ falsePositiveRate: 0.001 });
  const revised = audit(bus);
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    expectedInsertions: 500,
    oldExpectedInsertions: 500,
    falsePositiveRate: 0.001,
    oldFalsePositiveRate: 0.01,
  });
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(500, 0.001).size);
  bus.close();
});

test('null on a never-enabled bus is a silent no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDedupBloom(null);
  assert.equal(audit(bus).length, 0);
  // Still unconfigured: no filter is installed.
  assert.equal(bloomStats(bus).enabled, false);
  assert.equal(bloomStats(bus).insertions, 0);
  // A valid enable afterwards still works.
  bus.setDedupBloom({});
  assert.equal(audit(bus).length, 1);
  assert.equal(bloomStats(bus).enabled, true);
  bus.close();
});

test('an illegal value is rejected and the bus stays unconfigured', () => {
  const bus = new EventBus();
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
  // The failed calls changed nothing: still unconfigured, no audit.
  assert.equal(bloomStats(bus).enabled, false);
  assert.equal(audit(bus).length, 0);
  // A valid enable afterwards still works.
  bus.setDedupBloom({ expectedInsertions: 2_000 });
  assert.equal(bloomStats(bus).enabled, true);
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(2_000, 0.01).size);
  assert.equal(audit(bus).length, 1);
  bus.close();
});

test('after enabling, the next publish is governed by the new filter', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  // Before the enable there is no filter to probe: the map alone
  // decides, and the stats report the filter disabled.
  assert.equal(
    bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-0' }).duplicate,
    false,
  );
  assert.equal(bloomStats(bus).enabled, false);
  assert.equal(bloomStats(bus).insertions, 0);
  bus.setDedupBloom(true);
  // The claim made before the enable is already in the rebuilt
  // filter, so its retry is still suppressed exactly once...
  assert.equal(
    bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-0' }).duplicate,
    true,
  );
  // ...and fresh identities are admitted through the new filter.
  assert.equal(
    bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' }).duplicate,
    false,
  );
  assert.equal(bloomStats(bus).insertions, 2);
  bus.close();
});

test('claims made before the enable are already in the filter', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const ids: string[] = [];
  for (let i = 0; i < 50; i += 1) ids.push(`msg-${i}`);
  for (const id of ids) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: id }).duplicate,
      false,
    );
  }
  assert.equal(bloomStats(bus).enabled, false);
  bus.setDedupBloom({ expectedInsertions: 1_000, falsePositiveRate: 0.01 });
  // The rebuild starts the insertion count from the live table size.
  assert.equal(bloomStats(bus).insertions, 50);
  // Every retry is still suppressed exactly once...
  for (const id of ids) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: id }).duplicate,
      true,
      `retry of ${id} must still be suppressed after the enable`,
    );
  }
  // ...and fresh identities are still admitted.
  for (let i = 50; i < 60; i += 1) {
    assert.equal(
      bus.publishIdempotent('t', { n: 1 }, { messageId: `msg-${i}` }).duplicate,
      false,
    );
  }
  assert.equal(bloomStats(bus).insertions, 60);
  bus.close();
});

test('there is no live clear: null after enabling leaves the filter live', () => {
  // Verified by grep: no clearDedupBloom exists anywhere — the
  // constructor is the only place that assigns the filter, and
  // `null` is a no-op on both sides of the enable. The nearest
  // faithful equivalent of "clear then re-enable" is: a failed enable
  // leaves the bus unconfigured and a later valid call still enables
  // (pinned above), while `null` after enabling changes nothing.
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setDedupBloom({ expectedInsertions: 1_000, falsePositiveRate: 0.01 });
  bus.setDedupBloom(null);
  assert.equal(audit(bus).length, 1);
  assert.equal(bloomStats(bus).enabled, true);
  assert.equal(
    bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-0' }).duplicate,
    false,
  );
  assert.equal(
    bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-0' }).duplicate,
    true,
    'the filter is still live after null: the retry is suppressed',
  );
  // And a later valid revision still moves the live sizing.
  bus.setDedupBloom({ expectedInsertions: 100 });
  assert.equal(audit(bus).length, 2);
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(100, 0.01).size);
  bus.close();
});

test('a same-value revision after enabling is a no-op with no second audit', () => {
  const bus = new EventBus();
  bus.setDedupBloom({ expectedInsertions: 500, falsePositiveRate: 0.02 });
  bus.setDedupBloom({ expectedInsertions: 500 });
  bus.setDedupBloom({ falsePositiveRate: 0.02 });
  bus.setDedupBloom({ expectedInsertions: 500, falsePositiveRate: 0.02 });
  bus.setDedupBloom({});
  bus.setDedupBloom(null);
  assert.equal(audit(bus).length, 1);
  assert.equal(bloomStats(bus).bitSize, new BloomFilter(500, 0.02).size);
  bus.close();
});

test('true on a bus that already has a filter is refused', () => {
  // EB-540 / EB-536 family convention: `true` is an enable-only
  // token — a configured bus retunes through an object.
  const constructed = new EventBus({ dedupBloom: true });
  assert.throws(() => constructed.setDedupBloom(true), RangeError);
  assert.equal(audit(constructed).length, 0);
  assert.equal(bloomStats(constructed).bitSize, new BloomFilter(10_000, 0.01).size);

  const enabled = new EventBus();
  enabled.setDedupBloom({ expectedInsertions: 500 });
  assert.throws(() => enabled.setDedupBloom(true), RangeError);
  assert.equal(audit(enabled).length, 1);
  // The live sizing is untouched by the refused call.
  assert.equal(bloomStats(enabled).bitSize, new BloomFilter(500, 0.01).size);
  enabled.close();
  constructed.close();
});

test('the enable is live-only: a fresh bus is unconfigured again', () => {
  const bus = new EventBus();
  bus.setDedupBloom({ expectedInsertions: 500 });
  assert.equal(audit(bus).length, 1);
  assert.equal(bloomStats(bus).enabled, true);
  // A restart restores the unconfigured state: a fresh bus built from
  // the same construction options holds no filter.
  const fresh = new EventBus();
  assert.equal(bloomStats(fresh).enabled, false);
  assert.equal(audit(fresh).length, 0);
  // The fresh bus can still be enabled the same way.
  fresh.setDedupBloom(true);
  assert.equal(audit(fresh).length, 1);
  assert.equal(bloomStats(fresh).enabled, true);
  bus.close();
  fresh.close();
});

test('a closed bus refuses the enable and the revision with RangeError', async () => {
  const unconfigured = new EventBus();
  await unconfigured.close();
  assert.throws(
    () => unconfigured.setDedupBloom({ expectedInsertions: 500 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(audit(unconfigured).length, 0);

  const configured = new EventBus({ dedupBloom: true });
  await configured.close();
  assert.throws(
    () => configured.setDedupBloom({ expectedInsertions: 500 }),
    (e: unknown) => e instanceof RangeError && /bus is closed/.test((e as Error).message),
  );
  assert.equal(audit(configured).length, 0);
});
