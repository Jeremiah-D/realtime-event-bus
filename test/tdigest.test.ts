import test from 'node:test';
import assert from 'node:assert/strict';
import { TDigest } from '../src/tdigest.ts';

/** Deterministic xorshift32 PRNG (seeded, so the error-bound pins below are exact). */
function rng32(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s ^= s << 13;
    s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5;
    s >>>= 0;
    return s / 0x100000000;
  };
}

/** Nearest-rank quantile of a sorted array (the exact baseline from latency.ts). */
function exactRank(sorted: number[], q: number): number {
  const rank = Math.ceil(q * sorted.length) - 1;
  return sorted[Math.min(Math.max(rank, 0), sorted.length - 1)];
}

test('constructor rejects invalid options', () => {
  assert.throws(() => new TDigest({ compression: 0 }), RangeError);
  assert.throws(() => new TDigest({ compression: -5 }), RangeError);
  assert.throws(() => new TDigest({ compression: NaN }), RangeError);
  assert.throws(() => new TDigest({ compression: Infinity }), RangeError);
  assert.throws(() => new TDigest({ bufferSize: 0 }), RangeError);
  assert.throws(() => new TDigest({ bufferSize: 1.5 }), RangeError);
});

test('empty digest reports 0 quantiles and zero counts', () => {
  const td = new TDigest();
  assert.equal(td.count, 0);
  assert.equal(td.centroidCount, 0);
  assert.equal(td.quantile(0.5), 0);
  assert.equal(td.quantile(0), 0);
  assert.equal(td.quantile(1), 0);
  assert.deepEqual(td.getCentroids(), []);
});

test('quantile rejects out-of-range q', () => {
  const td = new TDigest();
  td.add(5);
  assert.throws(() => td.quantile(-0.1), RangeError);
  assert.throws(() => td.quantile(1.1), RangeError);
  assert.throws(() => td.quantile(NaN), RangeError);
});

test('add rejects non-finite values and non-positive weights', () => {
  const td = new TDigest();
  assert.throws(() => td.add(NaN), RangeError);
  assert.throws(() => td.add(Infinity), RangeError);
  assert.throws(() => td.add(1, 0), RangeError);
  assert.throws(() => td.add(1, -2), RangeError);
  assert.throws(() => td.add(1, NaN), RangeError);
});

test('single value is exact at every quantile', () => {
  const td = new TDigest();
  td.add(42);
  assert.equal(td.quantile(0), 42);
  assert.equal(td.quantile(0.5), 42);
  assert.equal(td.quantile(1), 42);
});

test('small sets interpolate between sample means', () => {
  const td = new TDigest();
  for (const v of [1, 2, 3, 4]) td.add(v);
  assert.equal(td.quantile(0), 1);
  assert.equal(td.quantile(0.5), 2.5);
  assert.equal(td.quantile(1), 4);
});

test('weights accumulate in count and interpolation', () => {
  const td = new TDigest();
  td.add(10, 100);
  td.add(20, 100);
  assert.equal(td.count, 200);
  assert.equal(td.quantile(0.5), 15);
});

test('clear resets the digest', () => {
  const td = new TDigest();
  td.add(1);
  td.add(2);
  td.clear();
  assert.equal(td.count, 0);
  assert.equal(td.centroidCount, 0);
  assert.equal(td.quantile(0.99), 0);
});

test('identical insert sequences yield identical centroids (deterministic)', () => {
  const build = () => {
    const td = new TDigest();
    const r = rng32(42);
    for (let i = 0; i < 5000; i++) td.add(r() * 100);
    return td.getCentroids().map((c) => `${c.mean.toFixed(9)}:${c.count}`).join('|');
  };
  assert.equal(build(), build());
});

test('error bounds vs exact sorting on skewed latencies (delta=100, n=20000)', () => {
  const r = rng32(0xc10c);
  const td = new TDigest({ compression: 100 });
  const exact: number[] = [];
  for (let i = 0; i < 20000; i++) {
    // Lognormal-ish queue dwell: exp of a near-normal, right-skewed like real latencies.
    const z = (r() + r() + r() + r() - 2) * 2;
    const v = Math.exp(0.9 * z) * 12;
    td.add(v);
    exact.push(v);
  }
  exact.sort((a, b) => a - b);
  // Measured on Node v24: 0.050% / 0.172% / 0.268% — pin well under 1%.
  for (const q of [0.5, 0.95, 0.99]) {
    const e = exactRank(exact, q);
    const relErr = Math.abs(td.quantile(q) - e) / e;
    assert.ok(relErr < 0.01, `q=${q}: relErr=${(relErr * 100).toFixed(3)}% exceeds 1%`);
  }
  // Measured 485 centroids: memory stays O(compression), far below n=20000.
  assert.ok(td.centroidCount < 600, `centroids=${td.centroidCount} exceeds bound`);
});

test('centroid count stays bounded as n grows 10x', () => {
  const r = rng32(99);
  const td = new TDigest();
  for (let i = 0; i < 200000; i++) td.add(Math.exp(r() * 6) * 5);
  // Measured 629: grows logarithmically, not with n.
  assert.ok(td.centroidCount < 800, `centroids=${td.centroidCount} exceeds bound`);
});

test('uniform distribution median lands near the middle', () => {
  const r = rng32(7);
  const td = new TDigest();
  for (let i = 0; i < 10000; i++) td.add(r() * 1000);
  const med = td.quantile(0.5);
  assert.ok(Math.abs(med - 500) / 500 < 0.05, `median=${med} too far from 500`);
});

test('heavy-tail spike stays visible at extreme quantiles', () => {
  const r = rng32(5);
  const td = new TDigest({ compression: 50 });
  for (let i = 0; i < 50000; i++) td.add(r() < 0.001 ? 100000 : r() * 100);
  assert.ok(td.quantile(0.5) < 60, `median=${td.quantile(0.5)} pulled by spike`);
  assert.ok(td.quantile(0.999) > 50000, `q0.999=${td.quantile(0.999)} missed the spike`);
});

test('retune: current() follows construction; partial revision keeps the other field', () => {
  const td = new TDigest();
  assert.deepEqual(td.current(), { compression: 100, bufferSize: 800 });
  td.retune({ compression: 50 });
  // A compression-only revision keeps the live bufferSize — it is NOT
  // recomputed from the new compression's construction default (80).
  assert.deepEqual(td.current(), { compression: 50, bufferSize: 800 });
  td.retune({ bufferSize: 64 });
  assert.deepEqual(td.current(), { compression: 50, bufferSize: 64 });
  td.retune({ compression: 200, bufferSize: 4096 });
  assert.deepEqual(td.current(), { compression: 200, bufferSize: 4096 });
});

test('retune: illegal compression throws RangeError and leaves live tuning untouched', () => {
  const td = new TDigest({ compression: 100, bufferSize: 64 });
  for (const compression of [0, -5, Number.NaN, Number.POSITIVE_INFINITY, 'x']) {
    assert.throws(
      () => td.retune({ compression: compression as unknown as number }),
      RangeError,
    );
    assert.deepEqual(td.current(), { compression: 100, bufferSize: 64 });
  }
});

test('retune: illegal bufferSize throws RangeError and leaves live tuning untouched', () => {
  const td = new TDigest({ compression: 100, bufferSize: 64 });
  for (const bufferSize of [0, -2, 1.5, Number.NaN, 'x']) {
    assert.throws(
      () => td.retune({ bufferSize: bufferSize as unknown as number }),
      RangeError,
    );
    assert.deepEqual(td.current(), { compression: 100, bufferSize: 64 });
  }
});

test('retune: non-object patch throws; empty and same-value patches are silent no-ops', () => {
  const td = new TDigest({ compression: 100, bufferSize: 64 });
  for (let i = 0; i < 10; i++) td.add(i);
  const before = td.getCentroids();
  assert.throws(() => td.retune(null as unknown as { compression: number }), RangeError);
  assert.throws(() => td.retune('nope' as unknown as { compression: number }), RangeError);
  assert.throws(() => td.retune([] as unknown as { compression: number }), RangeError);
  td.retune({});
  td.retune({ compression: 100 });
  td.retune({ bufferSize: 64 });
  td.retune({ compression: 100, bufferSize: 64 });
  assert.deepEqual(td.current(), { compression: 100, bufferSize: 64 });
  // The no-op retunes never touched the digest itself.
  assert.equal(td.count, 10);
  assert.deepEqual(td.getCentroids(), before);
});

test('retune: tightening compression refines the next merges — more centroids, tighter p99', () => {
  const r = rng32(42);
  const td = new TDigest({ compression: 10 });
  const exact: number[] = [];
  for (let i = 0; i < 2000; i++) {
    const v = Math.exp(r() * 6) * 5;
    td.add(v);
    exact.push(v);
  }
  const coarseCount = td.centroidCount;
  const sorted = [...exact].sort((a, b) => a - b);
  const coarseErr = Math.abs(td.quantile(0.99) - exactRank(sorted, 0.99)) / exactRank(sorted, 0.99);
  td.retune({ compression: 1000 });
  for (let i = 0; i < 2000; i++) {
    const v = Math.exp(r() * 6) * 5;
    td.add(v);
    exact.push(v);
  }
  const fineCount = td.centroidCount;
  const sorted2 = [...exact].sort((a, b) => a - b);
  const fineErr = Math.abs(td.quantile(0.99) - exactRank(sorted2, 0.99)) / exactRank(sorted2, 0.99);
  // Measured on Node v24: 47 -> 1257 centroids, p99 error 0.23% -> 0.04%.
  assert.ok(fineCount > coarseCount * 10, `centroids ${coarseCount} -> ${fineCount}`);
  assert.ok(fineErr < coarseErr, `p99 err ${(coarseErr * 100).toFixed(2)}% -> ${(fineErr * 100).toFixed(2)}%`);
});

test('retune: loosening compression lets the next pass merge far more aggressively', () => {
  const r = rng32(42);
  const td = new TDigest({ compression: 1000 });
  for (let i = 0; i < 2000; i++) td.add(Math.exp(r() * 6) * 5);
  const fineCount = td.centroidCount;
  td.retune({ compression: 10 });
  for (let i = 0; i < 2000; i++) td.add(Math.exp(r() * 6) * 5);
  const coarseCount = td.centroidCount;
  // Measured on Node v24: 1999 -> 45 centroids.
  assert.ok(coarseCount < fineCount / 10, `centroids ${fineCount} -> ${coarseCount}`);
});

test('retune: bufferSize governs from the next add()', () => {
  const td = new TDigest({ bufferSize: 3 });
  const buffered = () => (td as unknown as { buffer: unknown[] }).buffer.length;
  td.add(1);
  td.add(2);
  assert.equal(buffered(), 2);
  td.add(3);
  assert.equal(buffered(), 0, 'third add filled the buffer and compressed');
  td.retune({ bufferSize: 10 });
  for (let i = 0; i < 5; i++) td.add(i + 10);
  assert.equal(buffered(), 5, 'loosened buffer holds five points without compressing');
  td.retune({ bufferSize: 2 });
  td.add(99);
  assert.equal(buffered(), 0, 'tightened buffer compresses on the very next add');
  td.add(100);
  assert.equal(buffered(), 1);
});

test('retune: buffered samples and count survive the retune itself', () => {
  const td = new TDigest({ compression: 100, bufferSize: 1024 });
  for (let i = 0; i < 100; i++) td.add(i);
  const buffered = () => (td as unknown as { buffer: unknown[] }).buffer.length;
  assert.equal(buffered(), 100);
  assert.equal(td.count, 100);
  td.retune({ compression: 10, bufferSize: 8 });
  // The retune moved the tuning and nothing else: no compression pass
  // ran, no buffered point was dropped or rewritten.
  assert.equal(buffered(), 100);
  assert.equal(td.count, 100);
  assert.deepEqual(td.current(), { compression: 10, bufferSize: 8 });
});
