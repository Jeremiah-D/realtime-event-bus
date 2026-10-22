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
