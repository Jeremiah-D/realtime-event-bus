/**
 * Bloom pre-filter for the publish-side idempotency dedup window (EB-214):
 * `EventBusOptions.dedupBloom` adds a space-efficient probabilistic
 * membership test in front of the exact EB-27 `(topic, messageId)` map.
 * A negative answer skips the map; a positive answer falls through to the
 * authoritative map — so the filter can never suppress wrongly (zero
 * false kills), and a restart rebuilds it from the EB-66 journal.
 *
 * Run: `node --test test/bloom-dedup.test.ts` (Node >= 22, no build step).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { BloomFilter, resolveDedupBloomOptions } from '../src/bloom.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-bloom-'));

test('dedupBloom option validation is fail-fast', () => {
  // Bad sizing.
  for (const dedupBloom of [
    { expectedInsertions: 0 },
    { expectedInsertions: -5 },
    { expectedInsertions: 1.5 },
    { expectedInsertions: Number.NaN },
    { falsePositiveRate: 0 },
    { falsePositiveRate: 1 },
    { falsePositiveRate: -0.1 },
    { falsePositiveRate: 1.5 },
    { falsePositiveRate: Number.NaN },
  ]) {
    assert.throws(() => new EventBus({ dedupBloom }), RangeError);
  }
  // Wrong shape.
  for (const dedupBloom of ['yes', 42, ['x']]) {
    assert.throws(
      () => new EventBus({ dedupBloom: dedupBloom as unknown as boolean }),
      TypeError,
    );
  }
  // `true` selects defaults; `false`/omitted disables.
  const enabled = new EventBus({ dedupBloom: true });
  assert.equal(enabled.getStats().bloomStats?.enabled, true);
  enabled.close();
  const disabled = new EventBus();
  assert.deepEqual(disabled.getStats().bloomStats, {
    enabled: false,
    insertions: 0,
    estimatedFalsePositiveRate: 0,
    bitSize: 0,
    hashFunctions: 0,
  });
  disabled.close();
});

test('BloomFilter has no false negatives and a sane FPR estimate', () => {
  const bloom = new BloomFilter(1000, 0.01);
  const keys: string[] = [];
  for (let i = 0; i < 500; i += 1) keys.push(`t\0msg-${i}`);
  for (const k of keys) bloom.add(k);
  // No false negatives: everything added is reported present.
  for (const k of keys) assert.equal(bloom.has(k), true);
  assert.equal(bloom.insertions, 500);
  // The estimate matches the textbook formula at the current count.
  const m = bloom.size;
  const k = bloom.hashes;
  const expected = (1 - Math.exp((-k * 500) / m)) ** k;
  assert.ok(Math.abs(bloom.estimatedFalsePositiveRate() - expected) < 1e-12);
  // Rebuild drops the old bits: previously-added keys are forgotten.
  bloom.rebuild(['only-this']);
  assert.equal(bloom.insertions, 1);
  assert.equal(bloom.has('only-this'), true);
  assert.equal(bloom.has(keys[0]), false);
  // Constructor validation mirrors the option validation.
  assert.throws(() => new BloomFilter(0, 0.01), RangeError);
  assert.throws(() => new BloomFilter(100, 0), RangeError);
  assert.throws(() => new BloomFilter(100, 1), RangeError);
});

test('resolveDedupBloomOptions defaults and overrides', () => {
  assert.equal(resolveDedupBloomOptions(undefined, 10_000, 't'), undefined);
  assert.equal(resolveDedupBloomOptions(false, 10_000, 't'), undefined);
  assert.deepEqual(resolveDedupBloomOptions(true, 10_000, 't'), {
    expectedInsertions: 10_000,
    falsePositiveRate: 0.01,
  });
  assert.deepEqual(resolveDedupBloomOptions({ expectedInsertions: 500 }, 10_000, 't'), {
    expectedInsertions: 500,
    falsePositiveRate: 0.01,
  });
});

test('the filter never suppresses wrongly: exactness under heavy false positives', () => {
  // A tiny filter at p = 0.5 over 200 claims: false positives are
  // guaranteed, so every lookup exercises the map fallback path.
  const bus = new EventBus({
    dedupBloom: { expectedInsertions: 10, falsePositiveRate: 0.5 },
  });
  bus.subscribe('t', () => {});
  const ids: string[] = [];
  for (let i = 0; i < 200; i += 1) ids.push(`msg-${i}`);
  for (const id of ids) {
    const r = bus.publishIdempotent('t', { n: 1 }, { messageId: id });
    assert.equal(r.duplicate, false, `first publish of ${id} must be admitted`);
  }
  // Every retry is still suppressed exactly once — no false kill ever
  // drops a genuinely new message, and no duplicate ever slips through.
  for (const id of ids) {
    const r = bus.publishIdempotent('t', { n: 1 }, { messageId: id });
    assert.equal(r.duplicate, true, `retry of ${id} must be suppressed`);
  }
  // Fresh identities are admitted even though the tiny filter is
  // saturated with stale bits.
  for (let i = 200; i < 210; i += 1) {
    const r = bus.publishIdempotent('t', { n: 1 }, { messageId: `msg-${i}` });
    assert.equal(r.duplicate, false);
  }
  bus.close();
});

test('saturation guard rebuilds the filter once entries expire', () => {
  const clock = controllableClock();
  const bus = new EventBus({
    now: clock.now,
    dedupBloom: { expectedInsertions: 10, falsePositiveRate: 0.01 },
    idempotencyWindowMs: 1_000,
  });
  bus.subscribe('t', () => {});
  // Fill the filter well past its sized capacity with live claims.
  for (let i = 0; i < 100; i += 1) {
    bus.publishIdempotent('t', { n: 1 }, { messageId: `msg-${i}` });
  }
  assert.ok((bus.getStats().bloomStats?.insertions ?? 0) >= 100);
  // Expire every claim, then publish once more: pruneDedup drops the
  // expired table entries, and the guard rebuilds the filter from the
  // (now tiny) live table instead of carrying 100 stale bits.
  clock.nowMs += 2_000;
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-new' });
  const stats = bus.getStats().bloomStats;
  assert.ok(stats !== undefined && stats.enabled);
  assert.ok(
    stats.insertions <= 2,
    `filter should be rebuilt from the live table, got ${stats.insertions} insertions`,
  );
  assert.ok(
    stats.estimatedFalsePositiveRate < 0.05,
    `estimated FPR ${stats.estimatedFalsePositiveRate} should be near the configured 0.01`,
  );
  // The fresh claim suppresses its retry exactly.
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-new' }).duplicate, true);
  // And the expired identities are publishable again (window semantics
  // unchanged by the filter).
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-0' }).duplicate, false);
  bus.close();
});

test('previewPublish consults the filter read-only', () => {
  const bus = new EventBus({ dedupBloom: true });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', 'x', { messageId: 'm1' });
  const before = bus.getStats().bloomStats?.insertions ?? -1;
  // A preview of the claimed identity reports duplicate...
  assert.equal(bus.previewPublish('t', 'x', { messageId: 'm1' }).admitted, false);
  // ...a preview of a fresh identity reports admitted...
  assert.equal(bus.previewPublish('t', 'x', { messageId: 'm2' }).admitted, true);
  // ...and neither preview adds to the filter.
  assert.equal(bus.getStats().bloomStats?.insertions, before);
  bus.close();
});

test('restart rebuilds the filter from the EB-66 journal', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    idempotencyWindowMs: 60_000,
    dedupBloom: true,
  });
  bus1.subscribe('t', () => {});
  assert.deepEqual(bus1.publishIdempotent('t', 'x', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  assert.ok((bus1.getStats().bloomStats?.insertions ?? 0) >= 1);
  bus1.close();
  // Restart before the window expires: the filter is rebuilt from the
  // journal, so the retry is still suppressed — via the pre-filter path.
  const bus2 = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    idempotencyWindowMs: 60_000,
    dedupBloom: true,
  });
  bus2.subscribe('t', () => {});
  assert.ok((bus2.getStats().bloomStats?.insertions ?? 0) >= 1, 'filter rebuilt from journal');
  assert.deepEqual(bus2.publishIdempotent('t', 'x', { messageId: 'm1' }), {
    duplicate: true,
    accepted: 0,
  });
  bus2.close();
});

test('pause-resume path mirrors claims into the filter', async () => {
  const dir = freshDir();
  const bus = new EventBus({ dedupBloom: true, durableLogDir: dir });
  bus.subscribe('t', () => {});
  bus.pauseTopic('t', { mode: 'queue' });
  bus.publish('t', { n: 1 }, { messageId: 'm1' });
  await new Promise<void>((resolve) => setImmediate(resolve));
  bus.resumeTopic('t');
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.ok((bus.getStats().bloomStats?.insertions ?? 0) >= 1);
  // The resumed claim suppresses the retry exactly.
  assert.deepEqual(bus.publishIdempotent('t', { n: 1 }, { messageId: 'm1' }), {
    duplicate: true,
    accepted: 0,
  });
  bus.close();
});
