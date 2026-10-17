import test from 'node:test';
import assert from 'node:assert/strict';
import {
  HybridLogicalClock,
  compareHlc,
  formatHlc,
  isHlcTimestamp,
  parseHlc,
  type HlcTimestamp,
} from '../src/hlc.ts';

function ts(pt: number, l: number, node: string): HlcTimestamp {
  return { pt, l, node };
}

test('tick: same physical time increments the logical component', () => {
  const clock = new HybridLogicalClock('n1', () => 1000);
  const a = clock.tick();
  const b = clock.tick();
  assert.deepEqual(a, { pt: 1000, l: 0, node: 'n1' });
  assert.deepEqual(b, { pt: 1000, l: 1, node: 'n1' });
  assert.equal(compareHlc(a, b), -1);
});

test('tick: advancing physical time resets the logical component', () => {
  let now = 1000;
  const clock = new HybridLogicalClock('n1', () => now);
  clock.tick();
  clock.tick();
  now = 2000;
  const c = clock.tick();
  assert.deepEqual(c, { pt: 2000, l: 0, node: 'n1' });
});

test('tick: monotonic even when the injected clock runs backward', () => {
  let now = 5000;
  const clock = new HybridLogicalClock('n1', () => now);
  const a = clock.tick();
  now = 1000; // clock skew backward
  const b = clock.tick();
  assert.equal(compareHlc(a, b), -1);
  assert.equal(b.pt, 5000);
});

test('receive: merges a remote timestamp from the future', () => {
  const clock = new HybridLogicalClock('n1', () => 100);
  clock.tick(); // {pt:100,l:0}
  const merged = clock.receive(ts(5000, 3, 'n2'));
  assert.equal(merged.pt, 5000);
  assert.equal(merged.l, 4); // max(0,3)+1
  assert.equal(merged.node, 'n1');
  // A later local tick orders after the remote write.
  const next = clock.tick();
  assert.equal(compareHlc(ts(5000, 3, 'n2'), next), -1);
});

test('receive: ties on both predecessors take max logical + 1', () => {
  const clock = new HybridLogicalClock('n1', () => 1000);
  clock.tick(); // l:0
  clock.tick(); // l:1
  const merged = clock.receive(ts(1000, 5, 'n2'));
  assert.deepEqual(merged, { pt: 1000, l: 6, node: 'n1' });
});

test('receive: ignores malformed remote timestamps', () => {
  const clock = new HybridLogicalClock('n1', () => 1000);
  const before = clock.tick();
  const after = clock.receive({ pt: NaN, l: -1, node: '' } as unknown as HlcTimestamp);
  assert.deepEqual(after, before);
});

test('compareHlc: physical, then logical, then node id', () => {
  assert.equal(compareHlc(ts(1, 0, 'b'), ts(2, 0, 'a')), -1);
  assert.equal(compareHlc(ts(2, 0, 'a'), ts(1, 9, 'b')), 1);
  assert.equal(compareHlc(ts(5, 1, 'a'), ts(5, 2, 'a')), -1);
  assert.equal(compareHlc(ts(5, 2, 'a'), ts(5, 2, 'b')), -1);
  assert.equal(compareHlc(ts(5, 2, 'b'), ts(5, 2, 'a')), 1);
  assert.equal(compareHlc(ts(5, 2, 'a'), ts(5, 2, 'a')), 0);
});

test('formatHlc/parseHlc round-trip; parse rejects malformed input', () => {
  const t = ts(1700000000000, 42, 'bus-7');
  assert.equal(parseHlc(formatHlc(t)) !== null, true);
  assert.deepEqual(parseHlc(formatHlc(t)), t);
  assert.equal(parseHlc(''), null);
  assert.equal(parseHlc('abc'), null);
  assert.equal(parseHlc('1.2'), null);
  assert.equal(parseHlc('.2.n'), null);
  assert.equal(parseHlc('1..n'), null);
  assert.equal(parseHlc('1.2.'), null);
  assert.equal(parseHlc('x.2.n'), null);
  assert.equal(parseHlc('1.x.n'), null);
  assert.equal(parseHlc(123 as unknown as string), null);
});

test('isHlcTimestamp validates shape', () => {
  assert.equal(isHlcTimestamp(ts(1, 0, 'n')), true);
  assert.equal(isHlcTimestamp(null), false);
  assert.equal(isHlcTimestamp({}), false);
  assert.equal(isHlcTimestamp({ pt: 1, l: 0 }), false);
  assert.equal(isHlcTimestamp({ pt: 1, l: -1, node: 'n' }), false);
  assert.equal(isHlcTimestamp({ pt: 1, l: 1.5, node: 'n' }), false);
  assert.equal(isHlcTimestamp({ pt: Infinity, l: 0, node: 'n' }), false);
  assert.equal(isHlcTimestamp({ pt: 1, l: 0, node: '' }), false);
});

test('constructor validates nodeId and clock', () => {
  assert.throws(() => new HybridLogicalClock('', () => 1), RangeError);
  assert.throws(
    () => new HybridLogicalClock('n', 'not-a-function' as unknown as () => number),
    RangeError,
  );
});

test('latest returns a copy', () => {
  const clock = new HybridLogicalClock('n1', () => 1000);
  clock.tick();
  const copy = clock.latest();
  copy.pt = 999999;
  assert.notEqual(clock.latest().pt, 999999);
});
