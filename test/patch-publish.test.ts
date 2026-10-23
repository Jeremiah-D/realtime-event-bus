import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  applyJsonPatch,
  validateJsonPatchOps,
  parseJsonPointer,
  isPatchEnvelope,
  PatchVersionTracker,
  JsonPatchApplyError,
  type JsonPatchOp,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-patch-'));

test('parseJsonPointer: RFC 6901 unescaping', () => {
  assert.deepEqual(parseJsonPointer(''), []);
  assert.deepEqual(parseJsonPointer('/a/b/0'), ['a', 'b', '0']);
  assert.deepEqual(parseJsonPointer('/a~1b/c~0d'), ['a/b', 'c~d']);
  assert.throws(() => parseJsonPointer('nope'), RangeError);
});

test('validateJsonPatchOps: rejects malformed patches', () => {
  assert.throws(() => validateJsonPatchOps('nope'), RangeError);
  assert.throws(() => validateJsonPatchOps([]), RangeError);
  assert.throws(() => validateJsonPatchOps([{ op: 'move', path: '/a' }]), RangeError);
  assert.throws(() => validateJsonPatchOps([{ op: 'add', path: 'a' }]), RangeError);
  assert.throws(() => validateJsonPatchOps([{ op: 'add', path: '/a' }]), RangeError);
  validateJsonPatchOps([{ op: 'add', path: '/a', value: 1 }]);
});

test('applyJsonPatch: add/remove/replace on objects and arrays', () => {
  assert.deepEqual(applyJsonPatch({ a: 1 }, [{ op: 'replace', path: '/a', value: 2 }]), { a: 2 });
  assert.deepEqual(applyJsonPatch({ a: 1 }, [{ op: 'add', path: '/b', value: 3 }]), { a: 1, b: 3 });
  assert.deepEqual(applyJsonPatch({ a: 1, b: 2 }, [{ op: 'remove', path: '/a' }]), { b: 2 });
  assert.deepEqual(applyJsonPatch({ l: [1, 2, 3] }, [{ op: 'add', path: '/l/1', value: 9 }]), {
    l: [1, 9, 2, 3],
  });
  assert.deepEqual(applyJsonPatch({ l: [1] }, [{ op: 'add', path: '/l/-', value: 2 }]), { l: [1, 2] });
  assert.deepEqual(applyJsonPatch({ l: [1, 2] }, [{ op: 'remove', path: '/l/0' }]), { l: [2] });
  assert.deepEqual(applyJsonPatch({ a: { b: { c: 1 } } }, [{ op: 'replace', path: '/a/b/c', value: 5 }]), {
    a: { b: { c: 5 } },
  });
  // Root replacement.
  assert.deepEqual(applyJsonPatch({ a: 1 }, [{ op: 'replace', path: '', value: { z: 0 } }]), { z: 0 });
});

test('applyJsonPatch: fail-fast without mutating the target', () => {
  const target = { a: { b: 1 } };
  assert.throws(
    () => applyJsonPatch(target, [{ op: 'replace', path: '/missing', value: 1 }]),
    (e: unknown) => e instanceof JsonPatchApplyError && e.reason === 'missing-path',
  );
  assert.throws(
    () => applyJsonPatch(target, [{ op: 'replace', path: '/a/b/c', value: 1 }]),
    (e: unknown) => e instanceof JsonPatchApplyError && e.reason === 'type-conflict',
  );
  assert.throws(
    () => applyJsonPatch({ l: [1] }, [{ op: 'remove', path: '/l/5' }]),
    (e: unknown) => e instanceof JsonPatchApplyError && e.reason === 'invalid-index',
  );
  // The original is untouched after the failures.
  assert.deepEqual(target, { a: { b: 1 } });
});

test('PatchVersionTracker: stage/commit and remote consume ordering', () => {
  const tracker = new PatchVersionTracker({ n: 1 });
  const staged = tracker.stagePatch([{ op: 'replace', path: '/n', value: 2 }]);
  assert.equal(staged.version, 1);
  assert.equal(tracker.version, 0); // not committed yet
  tracker.commitStaged(staged.version, staged.snapshot);
  assert.equal(tracker.version, 1);
  assert.deepEqual(tracker.snapshot, { n: 2 });

  const remote = new PatchVersionTracker({ n: 2 });
  assert.equal(remote.consumeRemote(1, [{ op: 'replace', path: '/n', value: 3 }]), 'applied');
  assert.equal(remote.consumeRemote(1, [{ op: 'replace', path: '/n', value: 3 }]), 'stale');
  assert.equal(remote.consumeRemote(5, [{ op: 'replace', path: '/n', value: 3 }]), 'gap');
  assert.equal(remote.patchGaps, 1);
  assert.equal(remote.staleSkips, 1);
});

test('publishPatch: patches flow to patch subscribers in version order', async () => {
  const bus = new EventBus();
  bus.registerPatchKey('state.devices', 't1', { temp: 21, mode: 'auto' });
  const seen: unknown[] = [];
  bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot));
  // The baseline must travel on the wire: a full publish with the key
  // establishes the subscriber's snapshot; patches then apply in order.
  bus.publish('state.devices', { temp: 21, mode: 'auto' }, { key: 't1' });
  bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 23 }]);
  bus.publishPatch('state.devices', 't1', [{ op: 'add', path: '/humidity', value: 55 }]);
  await flush();
  await flush();
  await flush();
  assert.deepEqual(seen, [
    { temp: 21, mode: 'auto' },
    { temp: 23, mode: 'auto' },
    { temp: 23, mode: 'auto', humidity: 55 },
  ]);
  assert.deepEqual(bus.getPatchSnapshot('state.devices', 't1'), {
    temp: 23,
    mode: 'auto',
    humidity: 55,
  });
  assert.equal(bus.getPatchStats('state.devices', 't1').version, 3);
});

test('subscribePatch: a patch before any baseline is a gap, not silent corruption', async () => {
  const bus = new EventBus();
  bus.registerPatchKey('state.devices', 't1', { temp: 21, mode: 'auto' });
  const gaps: number[] = [];
  const seen: unknown[] = [];
  bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot), {
    onPatchGap: () => gaps.push(1),
  });
  // No full publish ever went out: the subscriber has no baseline, so
  // the patch cannot be applied — it surfaces as a gap.
  bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 23 }]);
  await flush();
  await flush();
  assert.equal(gaps.length, 1);
  assert.deepEqual(seen, []);
});

test('publishPatch: mixed full publish and patch share one version counter', async () => {
  const bus = new EventBus();
  bus.registerPatchKey('state.devices', 't1');
  const seen: unknown[] = [];
  bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot));
  bus.publish('state.devices', { temp: 20 }, { key: 't1' });
  bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 21 }]);
  bus.publish('state.devices', { temp: 30, mode: 'eco' }, { key: 't1' });
  bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/mode', value: 'turbo' }]);
  await flush();
  await flush();
  await flush();
  assert.deepEqual(seen, [
    { temp: 20 },
    { temp: 21 },
    { temp: 30, mode: 'eco' },
    { temp: 30, mode: 'turbo' },
  ]);
  assert.equal(bus.getPatchStats('state.devices', 't1').version, 4);
});

test('publishPatch: invalid patch fails fast with zero side effects', async () => {
  const bus = new EventBus();
  bus.registerPatchKey('state.devices', 't1', { temp: 21 });
  const seen: unknown[] = [];
  bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot));
  await flush();
  assert.throws(
    () => bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/nope', value: 1 }]),
    JsonPatchApplyError,
  );
  assert.throws(
    () => bus.publishPatch('state.devices', 't1', 'nope' as unknown as JsonPatchOp[]),
    RangeError,
  );
  await flush();
  // No version consumed, nothing published, snapshot untouched.
  assert.equal(bus.getPatchStats('state.devices', 't1').version, 0);
  assert.deepEqual(bus.getPatchSnapshot('state.devices', 't1'), { temp: 21 });
  assert.deepEqual(seen, []);
});

test('publishPatch: unregistered key and duplicate registration fail fast', () => {
  const bus = new EventBus();
  assert.throws(() => bus.publishPatch('t', 'k', [{ op: 'add', path: '/a', value: 1 }]), RangeError);
  bus.registerPatchKey('t', 'k');
  assert.throws(() => bus.registerPatchKey('t', 'k'), RangeError);
  assert.throws(
    () => bus.publishPatch('t', 'k', [{ op: 'add', path: '/a', value: 1 }]),
    /no baseline snapshot/,
  );
});

test('subscribePatch: late joiner detects the version gap', async () => {
  const bus = new EventBus();
  bus.registerPatchKey('state.devices', 't1', { temp: 21 });
  bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 22 }]);
  bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 23 }]);
  const gaps: Array<{ expectedVersion: number; receivedVersion: number }> = [];
  const seen: unknown[] = [];
  bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot), {
    onPatchGap: (g) => gaps.push(g),
  });
  bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 24 }]);
  await flush();
  await flush();
  assert.equal(gaps.length, 1);
  assert.deepEqual(
    { expectedVersion: gaps[0].expectedVersion, receivedVersion: gaps[0].receivedVersion },
    { expectedVersion: 1, receivedVersion: 3 },
  );
  // The out-of-order patch is not applied: the snapshot stays unset.
  assert.deepEqual(seen, []);
});

test('rebuildPatchSnapshot: restores the snapshot from the durable log', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.registerPatchKey('state.devices', 't1');
  bus.publish('state.devices', { temp: 20 }, { key: 't1' });
  bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 21 }]);
  bus.publishPatch('state.devices', 't1', [{ op: 'add', path: '/mode', value: 'auto' }]);
  await flush();
  const rebuilt = bus.rebuildPatchSnapshot('state.devices', 't1');
  assert.deepEqual(rebuilt, { temp: 21, mode: 'auto' });
  assert.equal(bus.getPatchStats('state.devices', 't1').version, 3);
});

test('rebuildPatchSnapshot: no records returns the current snapshot', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.registerPatchKey('state.devices', 't1', { temp: 21 });
  assert.deepEqual(bus.rebuildPatchSnapshot('state.devices', 't1'), { temp: 21 });
});

test('isPatchEnvelope: identifies patch payloads', () => {
  assert.equal(isPatchEnvelope({ patch: true, key: 'k', version: 1, ops: [] }), true);
  assert.equal(isPatchEnvelope({ patch: true, key: 'k' }), false);
  assert.equal(isPatchEnvelope({ a: 1 }), false);
  assert.equal(isPatchEnvelope(null), false);
});
