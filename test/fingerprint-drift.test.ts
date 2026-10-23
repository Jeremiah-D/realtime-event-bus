import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  computePayloadFingerprint,
  extractFieldPaths,
  diffFieldPaths,
  type FingerprintDrift,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

test('extractFieldPaths: nested objects and arrays', () => {
  assert.deepEqual(extractFieldPaths({ a: 1, b: { c: 2 } }), ['a', 'b', 'b.c']);
  assert.deepEqual(extractFieldPaths({ items: [1, 2, 3] }), ['items', 'items[]']);
  assert.deepEqual(
    extractFieldPaths({ items: [{ x: 1 }, { y: 2 }] }),
    ['items', 'items[]', 'items[].x', 'items[].y'],
  );
  assert.deepEqual(extractFieldPaths(null), []);
  assert.deepEqual(extractFieldPaths(42), []);
  assert.deepEqual(extractFieldPaths('str'), []);
});

test('computePayloadFingerprint: stable and value-independent', () => {
  const f1 = computePayloadFingerprint({ a: 1, b: { c: 2 } });
  const f2 = computePayloadFingerprint({ b: { c: 99 }, a: -5 }); // key order + values differ
  assert.equal(f1, f2);
  const f3 = computePayloadFingerprint({ a: 1, b: { c: 2, d: 3 } }); // extra field
  assert.notEqual(f1, f3);
  // Array length is not structural.
  assert.equal(
    computePayloadFingerprint({ items: [1, 2] }),
    computePayloadFingerprint({ items: [1, 2, 3, 4, 5] }),
  );
});

test('diffFieldPaths: added and removed', () => {
  const { added, removed } = diffFieldPaths(['a', 'b'], ['b', 'c']);
  assert.deepEqual(added, ['c']);
  assert.deepEqual(removed, ['a']);
});

test('sampling mode: no alerts before promote', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  bus.setTopicSchemaFingerprint('orders.*', {
    onDrift: (d) => drifts.push(d),
  });
  bus.publish('orders.created', { id: 1, amount: 100 });
  bus.publish('orders.created', { id: 2, amount: 200, extra: true }); // different shape, no alert yet
  await flush();
  assert.equal(drifts.length, 0);
  assert.equal(bus.getStats().schemaDrifts, 0);
});

test('promote arms detection; drift fires onDrift with added/removed', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  bus.setTopicSchemaFingerprint('orders.*', {
    onDrift: (d) => drifts.push(d),
  });
  bus.publish('orders.created', { id: 1, amount: 100 });
  await flush();
  assert.equal(bus.promoteTopicSchemaFingerprint('orders.*'), true);

  // Same shape: no drift.
  bus.publish('orders.created', { id: 2, amount: 250 });
  await flush();
  assert.equal(drifts.length, 0);

  // Added field: drift.
  bus.publish('orders.created', { id: 3, amount: 300, currency: 'USD' });
  await flush();
  assert.equal(drifts.length, 1);
  assert.equal(drifts[0].topic, 'orders.created');
  assert.deepEqual(drifts[0].added, ['currency']);
  assert.deepEqual(drifts[0].removed, []);
  assert.equal(typeof drifts[0].fingerprint, 'string');
  assert.equal(bus.getStats().schemaDrifts, 1);

  // Removed field: drift (baseline never included 'currency' — it only
  // appeared in the drifting payload above, so it is not "removed").
  bus.publish('orders.created', { id: 4 }); // amount gone
  await flush();
  assert.equal(drifts.length, 2);
  assert.deepEqual(drifts[1].added, []);
  assert.deepEqual(drifts[1].removed, ['amount']);
  assert.equal(bus.getStats().schemaDrifts, 2);
});

test('promote with no sample returns false', () => {
  const bus = new EventBus();
  bus.setTopicSchemaFingerprint('empty.*', { onDrift: () => {} });
  assert.equal(bus.promoteTopicSchemaFingerprint('empty.*'), false);
});

test('promote unknown pattern throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.promoteTopicSchemaFingerprint('nope.*'), RangeError);
});

test('setTopicSchemaFingerprint validates inputs', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicSchemaFingerprint('', { onDrift: () => {} }), RangeError);
  assert.throws(
    // @ts-expect-error: intentional bad input
    () => bus.setTopicSchemaFingerprint('a.*', {}),
    RangeError,
  );
  assert.throws(
    // @ts-expect-error: intentional bad input
    () => bus.setTopicSchemaFingerprint('a.*', { onDrift: 'nope' }),
    RangeError,
  );
});

test('pattern scoping: only matching topics are watched', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  bus.setTopicSchemaFingerprint('payments.*', { onDrift: (d) => drifts.push(d) });
  bus.publish('payments.auth', { a: 1 });
  bus.publish('orders.created', { a: 1 });
  await flush();
  assert.equal(bus.promoteTopicSchemaFingerprint('payments.*'), true);
  bus.publish('orders.created', { a: 1, b: 2 }); // not watched: no drift
  bus.publish('payments.auth', { a: 1, b: 2 }); // watched: drift
  await flush();
  assert.equal(drifts.length, 1);
  assert.equal(drifts[0].topic, 'payments.auth');
});

test('drift never blocks publishing (observe-only)', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t.*', (msg) => received.push(msg.payload));
  bus.setTopicSchemaFingerprint('t.*', { onDrift: () => {} });
  bus.publish('t.a', { x: 1 });
  await flush();
  bus.promoteTopicSchemaFingerprint('t.*');
  const accepted = bus.publish('t.a', { x: 1, y: 2 }); // drifts, but still admitted
  await flush();
  assert.equal(accepted, 1);
  assert.deepEqual(received, [{ x: 1 }, { x: 1, y: 2 }]);
  assert.equal(bus.getStats().schemaDrifts, 1);
});

test('throwing onDrift callback is error-isolated', async () => {
  const bus = new EventBus();
  bus.setTopicSchemaFingerprint('t.*', {
    onDrift: () => {
      throw new Error('boom');
    },
  });
  bus.publish('t.a', { x: 1 });
  await flush();
  bus.promoteTopicSchemaFingerprint('t.*');
  // Must not throw out of publish.
  bus.publish('t.a', { x: 1, y: 2 });
  await flush();
  assert.equal(bus.getStats().schemaDrifts, 1);
});

test('re-registering resets to sampling mode', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  bus.setTopicSchemaFingerprint('t.*', { onDrift: (d) => drifts.push(d) });
  bus.publish('t.a', { x: 1 });
  await flush();
  bus.promoteTopicSchemaFingerprint('t.*');
  // Re-register: baseline discarded, back to sampling.
  bus.setTopicSchemaFingerprint('t.*', { onDrift: (d) => drifts.push(d) });
  bus.publish('t.a', { x: 1, y: 2 }); // would have drifted, but now sampling
  await flush();
  assert.equal(drifts.length, 0);
  assert.equal(bus.getStats().schemaDrifts, 0);
});

test('clearTopicSchemaFingerprint removes the watcher', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  bus.setTopicSchemaFingerprint('t.*', { onDrift: (d) => drifts.push(d) });
  assert.equal(bus.clearTopicSchemaFingerprint('t.*'), true);
  assert.equal(bus.clearTopicSchemaFingerprint('t.*'), false);
  bus.publish('t.a', { x: 1 }); // no watcher: nothing happens
  await flush();
  assert.equal(drifts.length, 0);
  assert.throws(() => bus.promoteTopicSchemaFingerprint('t.*'), RangeError);
});

test('rejected publishes do not shape the baseline', async () => {
  const bus = new EventBus();
  const drifts: FingerprintDrift[] = [];
  bus.setTopicSchemaFingerprint('t.*', { onDrift: (d) => drifts.push(d) });
  // tryPublish with backpressure shed (no subscribers over water here,
  // so use an invalid publish instead): empty topic throws, so use a
  // schema-rejected publish via a validator.
  bus.setTopicSchema('t.*', () => false); // reject everything
  bus.publish('t.a', { x: 1 }); // rejected: not observed
  await flush();
  bus.clearTopicSchema('t.*');
  // No sample was recorded (the only publish was rejected).
  assert.equal(bus.promoteTopicSchemaFingerprint('t.*'), false);
  assert.equal(bus.getStats().schemaDrifts, 0);
});
