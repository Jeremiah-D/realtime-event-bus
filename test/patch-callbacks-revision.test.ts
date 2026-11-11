import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  JsonPatchApplyError,
  type PatchGapEvent,
  type PatchApplyErrorEvent,
} from '../src/bus.ts';

/**
 * Live revision of JSON Patch observation callbacks (EB-458):
 * `revisePatchCallbacks(subId, patch)` swaps `onPatchGap` and/or
 * `onPatchApplyError` in place, without re-subscribing — the patch
 * `wrapped` handler reads a mutable holder at call time, so the new
 * callbacks govern from the next gap / apply failure, while the
 * `PatchVersionTracker`'s version count and snapshot baseline survive
 * untouched. The gate is the subscription kind itself: only a
 * `subscribePatch` subscription carries the holder, so a plain
 * `subscribe` is refused until it re-subscribes via `subscribePatch`
 * to opt in — while a patch subscription that installed no callback
 * can gain one after the fact.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const flushTimes = async (n: number): Promise<void> => {
  for (let i = 0; i < n; i += 1) await flush();
};

/** The `patch_callbacks_revised` audit events on the bus. */
function patchRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'patch_callbacks_revised' });
}

/** A bus with a registered patch key and its baseline. */
function patchBus(baseline: unknown = { temp: 21, mode: 'auto' }): EventBus {
  const bus = new EventBus();
  bus.registerPatchKey('state.devices', 't1', baseline);
  return bus;
}

describe('live revision of JSON Patch observation callbacks (EB-458)', () => {
  test('swapping onPatchGap retargets the next gap with exact event fields', async () => {
    const bus = patchBus();
    const oldGaps: PatchGapEvent[] = [];
    const newGaps: PatchGapEvent[] = [];
    const seen: unknown[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot), {
      onPatchGap: (g) => oldGaps.push(g),
    });
    // No baseline ever published: the first patch is a gap.
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 22 }]);
    await flushTimes(2);
    assert.equal(oldGaps.length, 1);
    assert.deepEqual(oldGaps[0], {
      topic: 'state.devices',
      key: 't1',
      expectedVersion: 1,
      receivedVersion: 1,
    });

    bus.revisePatchCallbacks(sub.id, { onPatchGap: (g) => newGaps.push(g) });
    assert.deepEqual(patchRevisionAudits(bus)[0].detail, {
      onPatchGap: 'set',
      oldOnPatchGap: 'set',
      onPatchApplyError: 'unchanged',
      oldOnPatchApplyError: 'absent',
    });
    // The next gap goes to the new callback, with the tracker version
    // still 0 — the revision did not reset the version count.
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 23 }]);
    await flushTimes(2);
    assert.equal(oldGaps.length, 1);
    assert.equal(newGaps.length, 1);
    assert.deepEqual(newGaps[0], {
      topic: 'state.devices',
      key: 't1',
      expectedVersion: 1,
      receivedVersion: 2,
    });
    // The out-of-order patch was never applied: no snapshot delivery.
    assert.deepEqual(seen, []);
  });

  test('swapping onPatchApplyError retargets the next apply failure with exact event fields', async () => {
    const bus = patchBus();
    const oldErrors: PatchApplyErrorEvent[] = [];
    const newErrors: PatchApplyErrorEvent[] = [];
    const seen: unknown[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot), {
      onPatchApplyError: (e) => oldErrors.push(e),
    });
    // A patch envelope whose ops cannot apply to the subscriber's
    // snapshot: published raw, so it bypasses the publisher-side
    // staging validation and reaches the subscriber's apply path.
    const failingEnvelope = (version: number) => ({
      patch: true,
      key: 't1',
      version,
      ops: [{ op: 'replace', path: '/missing', value: 1 }],
    });
    const failingHeaders = (version: number) => ({
      'x-eb-patch': '1',
      'x-eb-patch-version': String(version),
    });
    bus.publish('state.devices', { temp: 21, mode: 'auto' }, { key: 't1' });
    bus.publish('state.devices', failingEnvelope(2), { headers: failingHeaders(2) });
    await flushTimes(2);
    assert.equal(oldErrors.length, 1);
    assert.equal(oldErrors[0].topic, 'state.devices');
    assert.equal(oldErrors[0].key, 't1');
    assert.equal(oldErrors[0].version, 2);
    assert.ok(oldErrors[0].error instanceof JsonPatchApplyError);
    assert.equal((oldErrors[0].error as JsonPatchApplyError).reason, 'missing-path');
    // The failed patch never reached the handler.
    assert.deepEqual(seen, [{ temp: 21, mode: 'auto' }]);

    bus.revisePatchCallbacks(sub.id, { onPatchApplyError: (e) => newErrors.push(e) });
    assert.deepEqual(patchRevisionAudits(bus)[0].detail, {
      onPatchGap: 'unchanged',
      oldOnPatchGap: 'absent',
      onPatchApplyError: 'set',
      oldOnPatchApplyError: 'set',
    });
    // The failed patch consumed its version, so the next envelope is
    // still in order — and still cannot apply on this snapshot.
    bus.publish('state.devices', failingEnvelope(3), { headers: failingHeaders(3) });
    await flushTimes(2);
    assert.equal(oldErrors.length, 1);
    assert.equal(newErrors.length, 1);
    assert.equal(newErrors[0].version, 3);
    assert.ok(newErrors[0].error instanceof JsonPatchApplyError);
    assert.deepEqual(seen, [{ temp: 21, mode: 'auto' }]);
  });

  test('an invalid version header reaches the revised onPatchApplyError with version NaN', async () => {
    const bus = patchBus();
    const oldErrors: PatchApplyErrorEvent[] = [];
    const newErrors: PatchApplyErrorEvent[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', () => {}, {
      onPatchApplyError: (e) => oldErrors.push(e),
    });
    bus.revisePatchCallbacks(sub.id, { onPatchApplyError: (e) => newErrors.push(e) });
    bus.publish(
      'state.devices',
      { patch: true, key: 't1', version: 1, ops: [{ op: 'replace', path: '/temp', value: 99 }] },
      { headers: { 'x-eb-patch': '1', 'x-eb-patch-version': 'bogus' } },
    );
    await flushTimes(2);
    assert.equal(oldErrors.length, 0);
    assert.equal(newErrors.length, 1);
    assert.equal(newErrors[0].topic, 'state.devices');
    assert.equal(newErrors[0].key, 't1');
    assert.equal(Number.isNaN(newErrors[0].version), true);
    assert.ok(newErrors[0].error instanceof RangeError);
  });

  test('clearing both callbacks leaves gaps and failures silent while delivery and snapshot continue', async () => {
    const bus = patchBus();
    const gaps: PatchGapEvent[] = [];
    const errors: PatchApplyErrorEvent[] = [];
    const seen: unknown[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot), {
      onPatchGap: (g) => gaps.push(g),
      onPatchApplyError: (e) => errors.push(e),
    });
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 22 }]);
    await flushTimes(2);
    assert.equal(gaps.length, 1);

    bus.revisePatchCallbacks(sub.id, { onPatchGap: null, onPatchApplyError: null });
    assert.deepEqual(patchRevisionAudits(bus)[0].detail, {
      onPatchGap: 'cleared',
      oldOnPatchGap: 'set',
      onPatchApplyError: 'cleared',
      oldOnPatchApplyError: 'set',
    });
    // Another gap, then a full publish that re-establishes the
    // snapshot, then an apply failure — all silent now.
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 23 }]);
    bus.publish('state.devices', { temp: 30, mode: 'eco' }, { key: 't1' });
    bus.publish(
      'state.devices',
      { patch: true, key: 't1', version: 4, ops: [{ op: 'replace', path: '/missing', value: 1 }] },
      { headers: { 'x-eb-patch': '1', 'x-eb-patch-version': '4' } },
    );
    await flushTimes(3);
    assert.equal(gaps.length, 1);
    assert.equal(errors.length, 0);
    // The full publish still established the snapshot and delivered.
    assert.deepEqual(seen, [{ temp: 30, mode: 'eco' }]);
    assert.deepEqual(bus.getPatchSnapshot('state.devices', 't1'), { temp: 30, mode: 'eco' });
  });

  test('a patch subscription with no callbacks can gain them after the fact', async () => {
    const bus = patchBus();
    const gaps: PatchGapEvent[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', () => {});
    bus.revisePatchCallbacks(sub.id, { onPatchGap: (g) => gaps.push(g) });
    assert.deepEqual(patchRevisionAudits(bus)[0].detail, {
      onPatchGap: 'set',
      oldOnPatchGap: 'absent',
      onPatchApplyError: 'unchanged',
      oldOnPatchApplyError: 'absent',
    });
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 22 }]);
    await flushTimes(2);
    assert.equal(gaps.length, 1);
    assert.equal(gaps[0].receivedVersion, 1);
  });

  test('revising a plain subscription throws RangeError and the live state is untouched', async () => {
    const bus = new EventBus();
    const seen: unknown[] = [];
    const sub = bus.subscribe('t', (msg) => seen.push(msg.payload));
    assert.throws(
      () => bus.revisePatchCallbacks(sub.id, { onPatchGap: () => {} }),
      RangeError,
    );
    assert.equal(patchRevisionAudits(bus).length, 0);
    // The refused call left the subscription delivering normally.
    bus.publish('t', 'hi');
    await flushTimes(2);
    assert.deepEqual(seen, ['hi']);
  });

  test('unknown subscription and illegal values throw RangeError with live callbacks untouched', async () => {
    const bus = patchBus();
    const gaps: PatchGapEvent[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', () => {}, {
      onPatchGap: (g) => gaps.push(g),
    });
    assert.throws(() => bus.revisePatchCallbacks('no-such-sub', { onPatchGap: () => {} }), RangeError);
    assert.throws(() => bus.revisePatchCallbacks('', { onPatchGap: () => {} }), RangeError);
    for (const badPatch of [null, [], 'patch', 42, {}, { onPatchGap: undefined }]) {
      assert.throws(
        () => bus.revisePatchCallbacks(sub.id, badPatch as never),
        RangeError,
      );
    }
    for (const bad of ['yes', 1, {}, true]) {
      assert.throws(
        () => bus.revisePatchCallbacks(sub.id, { onPatchGap: bad as never }),
        RangeError,
      );
      assert.throws(
        () => bus.revisePatchCallbacks(sub.id, { onPatchApplyError: bad as never }),
        RangeError,
      );
    }
    // No audit was written by any refused call, and the live callback
    // still fires on the next gap.
    assert.equal(patchRevisionAudits(bus).length, 0);
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 22 }]);
    await flushTimes(2);
    assert.equal(gaps.length, 1);
  });

  test('a revision identical to the live callbacks is a silent no-op with no audit', () => {
    const bus = patchBus();
    const onPatchGap = () => {};
    const sub = bus.subscribePatch('state.devices', 't1', () => {}, { onPatchGap });
    bus.revisePatchCallbacks(sub.id, { onPatchGap });
    assert.equal(patchRevisionAudits(bus).length, 0);
    // Clearing a callback that was never set is a no-op too.
    bus.revisePatchCallbacks(sub.id, { onPatchApplyError: null });
    assert.equal(patchRevisionAudits(bus).length, 0);
    const plain = bus.subscribePatch('state.devices', 't1', () => {});
    bus.revisePatchCallbacks(plain.id, { onPatchGap: null, onPatchApplyError: null });
    assert.equal(patchRevisionAudits(bus).length, 0);
  });

  test('a throwing revised callback is swallowed and the snapshot path is unaffected', async () => {
    const bus = patchBus();
    const seen: unknown[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot), {
      onPatchGap: () => {
        throw new Error('observer blew up');
      },
    });
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 22 }]);
    await flushTimes(2);
    // The throwing observer never broke delivery: the next full
    // publish re-establishes the snapshot and the next patch applies.
    bus.publish('state.devices', { temp: 30, mode: 'eco' }, { key: 't1' });
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/mode', value: 'turbo' }]);
    await flushTimes(3);
    assert.deepEqual(seen, [
      { temp: 30, mode: 'eco' },
      { temp: 30, mode: 'turbo' },
    ]);
    void sub;
  });

  test('the two fields revise independently of each other', async () => {
    const bus = patchBus();
    const gaps: PatchGapEvent[] = [];
    const errors: PatchApplyErrorEvent[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', () => {});
    bus.revisePatchCallbacks(sub.id, { onPatchGap: (g) => gaps.push(g) });
    assert.deepEqual(patchRevisionAudits(bus)[0].detail, {
      onPatchGap: 'set',
      oldOnPatchGap: 'absent',
      onPatchApplyError: 'unchanged',
      oldOnPatchApplyError: 'absent',
    });
    bus.revisePatchCallbacks(sub.id, { onPatchApplyError: (e) => errors.push(e) });
    assert.deepEqual(patchRevisionAudits(bus)[1].detail, {
      onPatchGap: 'unchanged',
      oldOnPatchGap: 'set',
      onPatchApplyError: 'set',
      oldOnPatchApplyError: 'absent',
    });
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 22 }]);
    await flushTimes(2);
    assert.equal(gaps.length, 1);
    bus.publish(
      'state.devices',
      { patch: true, key: 't1', version: 1, ops: [{ op: 'replace', path: '/temp', value: 99 }] },
      { headers: { 'x-eb-patch': '1', 'x-eb-patch-version': 'bogus' } },
    );
    await flushTimes(2);
    assert.equal(errors.length, 1);
  });

  test('revision leaves the tracker version count and snapshot baseline unchanged', async () => {
    const bus = patchBus();
    const seen: unknown[] = [];
    const sub = bus.subscribePatch('state.devices', 't1', (snapshot) => seen.push(snapshot));
    bus.publish('state.devices', { temp: 21, mode: 'auto' }, { key: 't1' });
    bus.publishPatch('state.devices', 't1', [{ op: 'replace', path: '/temp', value: 23 }]);
    await flushTimes(2);
    assert.deepEqual(seen, [
      { temp: 21, mode: 'auto' },
      { temp: 23, mode: 'auto' },
    ]);
    assert.equal(bus.getPatchStats('state.devices', 't1').version, 2);

    bus.revisePatchCallbacks(sub.id, { onPatchGap: () => {} });
    assert.equal(bus.getPatchStats('state.devices', 't1').version, 2);
    assert.deepEqual(bus.getPatchSnapshot('state.devices', 't1'), { temp: 23, mode: 'auto' });
    // The next patch still applies in order on the same baseline.
    bus.publishPatch('state.devices', 't1', [{ op: 'add', path: '/humidity', value: 55 }]);
    await flushTimes(2);
    assert.deepEqual(seen[2], { temp: 23, mode: 'auto', humidity: 55 });
    assert.equal(bus.getPatchStats('state.devices', 't1').version, 3);
  });
});
