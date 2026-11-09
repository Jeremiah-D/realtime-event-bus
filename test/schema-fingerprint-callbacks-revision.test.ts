import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type FingerprintDrift,
  type FingerprintOversize,
} from '../src/bus.ts';

/**
 * Fingerprint watcher callback revision (EB-432):
 * `reviseTopicSchemaFingerprintCallbacks` swaps a registered watcher's
 * `onDrift` / `onOversize` in place — only the callbacks move, the
 * compiled matcher, the armed baseline, and the latest sample are
 * untouched, so the new callbacks govern from the next drift /
 * oversize skip. Re-registering the same pattern is the old path:
 * it replaces the callback but resets the watcher to sampling mode
 * (the old baseline is discarded).
 *
 * The oversize trigger reuses the EB-208 linkage: a subscriber with
 * `subscriberMaxPayloadBytes` skips an oversized message, and the
 * matching watcher's `onOversize` fires per skip.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A payload whose JSON UTF-8 encoding is exactly `size` bytes. */
const sizedPayload = (size: number) => ({ data: 'x'.repeat(Math.max(0, size - 11)) });

test('revising onDrift after promotion routes the next drift to the new callback, baseline intact', async () => {
  const bus = new EventBus();
  const oldDrifts: FingerprintDrift[] = [];
  const newDrifts: FingerprintDrift[] = [];
  bus.setTopicSchemaFingerprint('t.*', { onDrift: (d) => oldDrifts.push(d) });
  bus.publish('t.a', { x: 1 });
  await flush();
  assert.equal(bus.promoteTopicSchemaFingerprint('t.*'), true);

  bus.reviseTopicSchemaFingerprintCallbacks('t.*', { onDrift: (d) => newDrifts.push(d) });
  const audit = bus.getSubscriptionAudit({ action: 'schema_fingerprint_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].pattern, 't.*');
  assert.deepEqual(audit[0].detail, {
    onDrift: 'set',
    oldOnDrift: 'set',
    onOversize: 'unchanged',
    oldOnOversize: 'absent',
  });

  // No re-promotion needed: the armed baseline survived the revision,
  // so the very next drift fires the NEW callback.
  bus.publish('t.a', { x: 1, y: 2 });
  await flush();
  assert.equal(newDrifts.length, 1);
  assert.deepEqual(newDrifts[0].added, ['y']);
  assert.equal(oldDrifts.length, 0);
  assert.equal(bus.getStats().schemaDrifts, 1);

  // A second drift still fires the new callback — the baseline was
  // never discarded, only the callback identity moved.
  bus.publish('t.a', { x: 1, z: 3 });
  await flush();
  assert.equal(newDrifts.length, 2);
  assert.deepEqual(newDrifts[1].added, ['z']);
  // 'y' was never in the baseline — it only appeared in the drifting
  // payload above — so it is not "removed".
  assert.deepEqual(newDrifts[1].removed, []);
  assert.equal(oldDrifts.length, 0);
});

test('revising onOversize routes the next oversize skip to the new callback', async () => {
  const bus = new EventBus();
  const oldOversize: FingerprintOversize[] = [];
  const newOversize: FingerprintOversize[] = [];
  bus.subscribe('t', () => {}, { subscriberMaxPayloadBytes: 64 });
  bus.setTopicSchemaFingerprint('t', {
    onDrift: () => {},
    onOversize: (e) => oldOversize.push(e),
  });
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(oldOversize.length, 1);
  assert.equal(oldOversize[0].capBytes, 64);
  assert.equal(oldOversize[0].capSource, 'subscriber');

  bus.reviseTopicSchemaFingerprintCallbacks('t', { onOversize: (e) => newOversize.push(e) });
  const audit = bus.getSubscriptionAudit({ action: 'schema_fingerprint_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onDrift: 'unchanged',
    oldOnDrift: 'set',
    onOversize: 'set',
    oldOnOversize: 'set',
  });

  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(newOversize.length, 1);
  assert.equal(newOversize[0].payloadBytes, 128);
  assert.equal(oldOversize.length, 1);
});

test('clearing onOversize with null silences oversize alerts while drift keeps firing', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  const oversize: FingerprintOversize[] = [];
  bus.subscribe('t', () => {}, { subscriberMaxPayloadBytes: 64 });
  bus.setTopicSchemaFingerprint('t', {
    onDrift: (d) => drifts.push(d),
    onOversize: (e) => oversize.push(e),
  });
  // Sample the `{ data }` shape so the oversize payload below (same
  // shape, only bigger) does not also count as a drift.
  bus.publish('t', { data: 'v' });
  await flush();
  assert.equal(bus.promoteTopicSchemaFingerprint('t'), true);

  bus.reviseTopicSchemaFingerprintCallbacks('t', { onOversize: null });
  const audit = bus.getSubscriptionAudit({ action: 'schema_fingerprint_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onDrift: 'unchanged',
    oldOnDrift: 'set',
    onOversize: 'cleared',
    oldOnOversize: 'set',
  });

  bus.publish('t', sizedPayload(128)); // oversize skip: no callback anymore
  await flush();
  assert.equal(oversize.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  bus.publish('t', { data: 'v', extra: 1 }); // drift: the onDrift callback is untouched
  await flush();
  assert.equal(drifts.length, 1);
  assert.deepEqual(drifts[0].added, ['extra']);
});

test('unregistered pattern, empty pattern, and illegal patches throw RangeError fail-closed', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  const oversize: FingerprintOversize[] = [];
  bus.subscribe('t.a', () => {}, { subscriberMaxPayloadBytes: 64 });
  const onDrift = (d: FingerprintDrift) => drifts.push(d);
  const onOversize = (e: FingerprintOversize) => oversize.push(e);
  bus.setTopicSchemaFingerprint('t.*', { onDrift, onOversize });
  bus.publish('t.a', { data: 'v' });
  await flush();
  assert.equal(bus.promoteTopicSchemaFingerprint('t.*'), true);

  for (const bad of [
    () => bus.reviseTopicSchemaFingerprintCallbacks('nope.*', { onDrift }),
    () => bus.reviseTopicSchemaFingerprintCallbacks('', { onDrift }),
    () => bus.reviseTopicSchemaFingerprintCallbacks('t.*', {}),
    // @ts-expect-error: intentional bad input
    () => bus.reviseTopicSchemaFingerprintCallbacks('t.*', null),
    // @ts-expect-error: intentional bad input
    () => bus.reviseTopicSchemaFingerprintCallbacks('t.*', 'nope'),
    // @ts-expect-error: intentional bad input
    () => bus.reviseTopicSchemaFingerprintCallbacks('t.*', { onDrift: 'nope' }),
    // onDrift can never be cleared: null is not a function.
    () => bus.reviseTopicSchemaFingerprintCallbacks('t.*', { onDrift: null }),
    // @ts-expect-error: intentional bad input
    () => bus.reviseTopicSchemaFingerprintCallbacks('t.*', { onOversize: 'nope' }),
    // @ts-expect-error: intentional bad input
    () => bus.reviseTopicSchemaFingerprintCallbacks('t.*', { onOversize: 42 }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'schema_fingerprint_callbacks_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live callbacks are untouched: the old ones still fire. The
  // drift publish adds a field to the sampled `{ data }` baseline;
  // the oversize payload shares that shape, so it skips without
  // drifting.
  bus.publish('t.a', { data: 'v', extra: 1 });
  bus.publish('t.a', sizedPayload(128));
  await flush();
  assert.equal(drifts.length, 1);
  assert.equal(oversize.length, 1);
});

test('a same-value revision is a no-op with no audit', () => {
  const bus = new EventBus();
  const onDrift = () => {};
  bus.setTopicSchemaFingerprint('t.*', { onDrift });
  // Same onDrift reference: nothing moves.
  bus.reviseTopicSchemaFingerprintCallbacks('t.*', { onDrift });
  // Clearing an onOversize that was never set: nothing moves either.
  bus.reviseTopicSchemaFingerprintCallbacks('t.*', { onOversize: null });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'schema_fingerprint_callbacks_revised' }).length,
    0,
    'same-value revisions must not audit',
  );
});

test('a revision before promotion keeps the sample and arms the new callback', async () => {
  const bus = new EventBus();
  const oldDrifts: FingerprintDrift[] = [];
  const newDrifts: FingerprintDrift[] = [];
  bus.setTopicSchemaFingerprint('t.*', { onDrift: (d) => oldDrifts.push(d) });
  bus.publish('t.a', { x: 1 });
  await flush();
  // Revise while still sampling: the sample is preserved, so the
  // promotion below arms the NEW callback, not the old one.
  bus.reviseTopicSchemaFingerprintCallbacks('t.*', { onDrift: (d) => newDrifts.push(d) });
  assert.equal(bus.promoteTopicSchemaFingerprint('t.*'), true);
  bus.publish('t.a', { x: 1, y: 2 });
  await flush();
  assert.equal(newDrifts.length, 1);
  assert.equal(oldDrifts.length, 0);
});

test('re-registering still discards the baseline (old-path contrast)', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  const onDrift = (d: FingerprintDrift) => drifts.push(d);
  bus.setTopicSchemaFingerprint('t.*', { onDrift });
  bus.publish('t.a', { x: 1 });
  await flush();
  assert.equal(bus.promoteTopicSchemaFingerprint('t.*'), true);
  // Re-registering with the same callback resets to sampling mode:
  // the armed baseline is gone, so the drift below is silent.
  bus.setTopicSchemaFingerprint('t.*', { onDrift });
  bus.publish('t.a', { x: 1, y: 2 });
  await flush();
  assert.equal(drifts.length, 0);
  assert.equal(bus.getStats().schemaDrifts, 0);
  // Sampling learned the new shape; promoting re-arms on it.
  assert.equal(bus.promoteTopicSchemaFingerprint('t.*'), true);
  bus.publish('t.a', { x: 1 });
  await flush();
  assert.equal(drifts.length, 1);
});

test('revising both callbacks at once audits both presences', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  const oversize: FingerprintOversize[] = [];
  bus.subscribe('t', () => {}, { subscriberMaxPayloadBytes: 64 });
  bus.setTopicSchemaFingerprint('t', { onDrift: () => {} });
  // Sample the `{ data }` shape so the oversize payload below (same
  // shape, only bigger) does not also count as a drift.
  bus.publish('t', { data: 'v' });
  await flush();
  assert.equal(bus.promoteTopicSchemaFingerprint('t'), true);

  bus.reviseTopicSchemaFingerprintCallbacks('t', {
    onDrift: (d) => drifts.push(d),
    onOversize: (e) => oversize.push(e),
  });
  const audit = bus.getSubscriptionAudit({ action: 'schema_fingerprint_callbacks_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onDrift: 'set',
    oldOnDrift: 'set',
    onOversize: 'set',
    oldOnOversize: 'absent',
  });
  bus.publish('t', { data: 'v', extra: 1 });
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(drifts.length, 1);
  assert.equal(oversize.length, 1);
});
