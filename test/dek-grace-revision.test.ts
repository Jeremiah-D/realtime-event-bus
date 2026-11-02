/**
 * DEK grace-window live revision (EB-354).
 *
 * `RotatingDekEncryptor.setGraceMs(graceMs)` revises how long a retired
 * key keeps decrypting — without rebuilding the ring or re-sealing
 * anything. Only the window moves: the key ring and every retirement
 * stamp are untouched, so widening does not resurrect a key `rekey` /
 * `pruneRetiredKeys` already dropped, and narrowing only changes the
 * next `decrypt` decision. The live window is observable via the
 * `graceMs` getter. Validation reuses the construction-time rule (a
 * non-negative finite number of milliseconds): an illegal value throws
 * `RangeError` with the live window untouched, and revising to the
 * current window is a no-op.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { RotatingDekEncryptor, randomAes256Key } from '../src/encrypt.ts';

/** A ring with k1 live, retired at `retiredAtMs` after one rotation. */
const retiredRing = (graceMs: number, retiredAtMs = 1000) => {
  const ring = new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], {
    graceMs,
    nowMs: 0,
  });
  const sealed = ring.encrypt(Buffer.from('v1'));
  ring.rotate(undefined, retiredAtMs);
  return { ring, sealed };
};

test('constructor still rejects illegal graceMs and the getter reads the live window', () => {
  for (const bad of [-1, Number.NaN, Infinity, '1000']) {
    assert.throws(
      () =>
        new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], {
          graceMs: bad as never,
        }),
      RangeError,
    );
  }
  const ring = new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], {
    graceMs: 1000,
  });
  assert.equal(ring.graceMs, 1000);
  const defaulted = new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }]);
  assert.equal(defaulted.graceMs, 0);
});

test('widening the window lets a just-expired key decrypt again', () => {
  const { ring, sealed } = retiredRing(1000);
  // k1 retired at 1000; at 2500 it is 1500ms past retirement — past grace.
  assert.throws(() => ring.decrypt(sealed, 2500), /grace window elapsed/);
  ring.setGraceMs(2000);
  assert.equal(ring.graceMs, 2000);
  assert.deepEqual(ring.decrypt(sealed, 2500), Buffer.from('v1'));
  // The boundary is inclusive, like the construction-time window.
  assert.deepEqual(ring.decrypt(sealed, 3000), Buffer.from('v1'));
  assert.throws(() => ring.decrypt(sealed, 3001), /grace window elapsed/);
});

test('narrowing the window refuses a retired key on the next decrypt', () => {
  const { ring, sealed } = retiredRing(5000);
  assert.deepEqual(ring.decrypt(sealed, 2500), Buffer.from('v1'));
  ring.setGraceMs(500);
  assert.equal(ring.graceMs, 500);
  assert.throws(() => ring.decrypt(sealed, 2500), /grace window elapsed/);
  // The live key is unaffected by any window.
  assert.deepEqual(ring.decrypt(ring.encrypt(Buffer.from('v2')), 2500), Buffer.from('v2'));
});

test('illegal values throw RangeError and leave the live window untouched', () => {
  const { ring, sealed } = retiredRing(1000);
  for (const bad of [0 - 1, Number.NaN, Infinity, '1000', -1000]) {
    assert.throws(() => ring.setGraceMs(bad as never), RangeError);
  }
  assert.equal(ring.graceMs, 1000);
  // The untouched window still governs: expired at 2500, open at 1500.
  assert.throws(() => ring.decrypt(sealed, 2500), /grace window elapsed/);
  assert.deepEqual(ring.decrypt(sealed, 1500), Buffer.from('v1'));
});

test('revising to the current window is a no-op', () => {
  const { ring, sealed } = retiredRing(1000);
  ring.setGraceMs(1000);
  assert.equal(ring.graceMs, 1000);
  assert.deepEqual(ring.decrypt(sealed, 1500), Buffer.from('v1'));
  assert.throws(() => ring.decrypt(sealed, 2500), /grace window elapsed/);
});

test('a key retired after the revision is measured against the new window', () => {
  const ring = new RotatingDekEncryptor([{ keyId: 'k1', key: randomAes256Key() }], {
    graceMs: 1000,
    nowMs: 0,
  });
  const sealed1 = ring.encrypt(Buffer.from('v1'));
  ring.rotate(undefined, 1000); // k1 retired at 1000, k2 live.
  ring.setGraceMs(3000);
  const sealed2 = ring.encrypt(Buffer.from('v2'));
  ring.rotate(undefined, 3500); // k2 retired at 3500, k3 live.
  // k1 is 5000ms past retirement at 6000 — past the new window.
  assert.throws(() => ring.decrypt(sealed1, 6000), /grace window elapsed/);
  // k2 is 2500ms past retirement at 6000 — inside the new window.
  assert.deepEqual(ring.decrypt(sealed2, 6000), Buffer.from('v2'));
  // And the boundary is the new window, not the old one.
  assert.throws(() => ring.decrypt(sealed2, 6501), /grace window elapsed/);
});

test('pruneRetiredKeys follows the revised window', () => {
  const { ring, sealed } = retiredRing(1000);
  // Widen first: nothing is past the new window yet.
  ring.setGraceMs(5000);
  assert.equal(ring.pruneRetiredKeys(2000), 0);
  assert.deepEqual(ring.keyIds(), ['k2', 'k1']);
  // Narrow below the key's age: the next prune drops it.
  ring.setGraceMs(500);
  assert.equal(ring.pruneRetiredKeys(2000), 1);
  assert.deepEqual(ring.keyIds(), ['k2']);
  assert.throws(() => ring.decrypt(sealed, 2000), /unknown key id/);
});

test('widening does not resurrect a key pruned before the revision', () => {
  const { ring, sealed } = retiredRing(1000);
  assert.equal(ring.pruneRetiredKeys(3001), 1);
  assert.deepEqual(ring.keyIds(), ['k2']);
  ring.setGraceMs(60_000);
  assert.deepEqual(ring.keyIds(), ['k2']);
  assert.throws(() => ring.decrypt(sealed, 3001), /unknown key id/);
});

test('rekey prunes fully-elapsed keys against the revised window', () => {
  const keys = [
    { keyId: 'k1', key: randomAes256Key() },
    { keyId: 'k2', key: randomAes256Key() },
  ];
  // k2 starts retired as of 0 (a restored ring).
  const ring = new RotatingDekEncryptor(keys, { graceMs: 1000, nowMs: 0 });
  // At 5000, k2 is past the old window: rekey drops it.
  ring.rekey(keys, 5000);
  assert.deepEqual(ring.keyIds(), ['k1']);
  // Widen, then restore k2 via rekey: it comes back retired as of now.
  ring.setGraceMs(10_000);
  ring.rekey(keys, 5000);
  assert.deepEqual(ring.keyIds(), ['k1', 'k2']);
  const sealed2 = ring.encrypt(Buffer.from('x'));
  assert.equal(sealed2.keyId, 'k1');
});
