import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level terminal batch maxSize default (EB-483):
 * `setDefaultBatchMaxSize` installs the `maxSize` new batch subscribers
 * fall back to when they opt into batching but omit their own — the
 * priority chain is subscriber-explicit > bus default > 100 — and the
 * default never enables batching: a subscriber that does not opt into
 * `batch` is untouched by it.
 *
 * Batch-size assertions follow `batch.test.ts`: full batches deliver
 * immediately on flush; a trailing partial batch lingers `maxWaitMs`
 * (default 10ms) and is collected after a short sleep.
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Records every handler invocation as the list of payloads it carried. */
function batchRecorder() {
  const calls: unknown[][] = [];
  const handler = (msgs: unknown) => {
    const arr = Array.isArray(msgs) ? msgs : [msgs];
    calls.push(arr.map((m) => (m as { payload: unknown }).payload));
  };
  return { calls, handler };
}

test('EB-483: the factory bus default is unset (batch: true still means maxSize 100)', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultBatchMaxSize(), undefined);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  for (let i = 0; i < 100; i++) bus.publish('t', i);
  await flush();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].length, 100);
});

test('EB-483: a new batch subscriber takes the bus default maxSize', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(3);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  for (let i = 0; i < 7; i++) bus.publish('t', i);
  await flush();
  // Full batches deliver immediately; the trailing partial lingers.
  assert.deepEqual(calls, [
    [0, 1, 2],
    [3, 4, 5],
  ]);
  await sleep(30);
  assert.deepEqual(calls, [[0, 1, 2], [3, 4, 5], [6]]);
});

test('EB-483: the bus default also fills an omitted maxSize in object form', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(3);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: { maxWaitMs: 0 } });
  for (let i = 0; i < 7; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  await flush();
  assert.deepEqual(calls, [
    [0, 1, 2],
    [3, 4, 5],
    [6],
  ]);
});

test('EB-483: an explicit maxSize beats the bus default in both directions', async () => {
  // Explicit 5 beats a bus default of 2.
  const loose = new EventBus();
  loose.setDefaultBatchMaxSize(2);
  const looseRec = batchRecorder();
  loose.subscribe('t', looseRec.handler, { batch: { maxSize: 5, maxWaitMs: 0 } });
  for (let i = 0; i < 10; i++) loose.publish('t', i);
  await flush();
  assert.deepEqual(looseRec.calls, [
    [0, 1, 2, 3, 4],
    [5, 6, 7, 8, 9],
  ]);

  // Explicit 2 beats a bus default of 5.
  const strict = new EventBus();
  strict.setDefaultBatchMaxSize(5);
  const strictRec = batchRecorder();
  strict.subscribe('t', strictRec.handler, { batch: { maxSize: 2, maxWaitMs: 0 } });
  for (let i = 0; i < 6; i++) strict.publish('t', i);
  await flush();
  assert.deepEqual(strictRec.calls, [
    [0, 1],
    [2, 3],
    [4, 5],
  ]);
});

test('EB-483: clearing with null returns new batch subscribers to 100', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(3);
  bus.setDefaultBatchMaxSize(null);
  assert.equal(bus.getDefaultBatchMaxSize(), undefined);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  for (let i = 0; i < 100; i++) bus.publish('t', i);
  await flush();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].length, 100);
});

test('EB-483: revising the bus default never moves an already-subscribed maxSize or pending batch', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(2);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(calls, [
    [0, 1],
    [2, 3],
  ]);
  bus.setDefaultBatchMaxSize(4);
  for (let i = 4; i < 8; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    calls,
    [
      [0, 1],
      [2, 3],
      [4, 5],
      [6, 7],
    ],
    'the early subscriber keeps the size it was baked with',
  );

  // A subscriber joining after the revision takes the new default.
  const lateRec = batchRecorder();
  bus.subscribe('u', lateRec.handler, { batch: true });
  for (let i = 0; i < 8; i++) bus.publish('u', i);
  await flush();
  assert.deepEqual(lateRec.calls, [
    [0, 1, 2, 3],
    [4, 5, 6, 7],
  ]);
});

test('EB-483: the default never enables batching, and maxWaitMs is untouched by it', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(5);
  // No `batch` opt-in: still one handler call per message.
  const plainRec = batchRecorder();
  bus.subscribe('t', plainRec.handler);
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(plainRec.calls, [[0], [1], [2]]);

  // A batch subscriber still lingers the documented 10ms default.
  const batchRec = batchRecorder();
  bus.subscribe('u', batchRec.handler, { batch: true });
  bus.publish('u', 'a');
  await flush();
  assert.deepEqual(batchRec.calls, [], 'the partial batch is still lingering at maxWaitMs 10');
  await sleep(50);
  assert.deepEqual(batchRec.calls, [['a']]);
});

test('EB-483: setBatch partial revision still takes the live value, and null still clears to one-at-a-time', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(2);
  const { calls, handler } = batchRecorder();
  const sub = bus.subscribe('t', handler, { batch: true });
  // Partial revision: only maxWaitMs moves; maxSize keeps its live 2.
  bus.setBatch(sub.id, { maxWaitMs: 0 });
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  await flush();
  await sleep(10);
  await flush();
  assert.deepEqual(calls, [
    [0, 1],
    [2, 3],
    [4],
  ]);
  // Null still clears the whole subscriber back to one-at-a-time.
  bus.setBatch(sub.id, null);
  bus.publish('t', 5);
  bus.publish('t', 6);
  await flush();
  assert.deepEqual(calls.slice(3), [[5], [6]]);
});

test('EB-483: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_max_size_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultBatchMaxSize(3);
  assert.equal(bus.getDefaultBatchMaxSize(), 3);
  bus.setDefaultBatchMaxSize(3);
  assert.equal(bus.getDefaultBatchMaxSize(), 3);
  const audit = bus.getSubscriptionAudit({ action: 'default_batch_max_size_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, batchMaxSize: 3 });
  bus.setDefaultBatchMaxSize(5, 'ops');
  assert.equal(bus.getDefaultBatchMaxSize(), 5);
  const revised = bus.getSubscriptionAudit({ action: 'default_batch_max_size_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, { previous: 3, batchMaxSize: 5 });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultBatchMaxSize(null);
  assert.equal(bus.getDefaultBatchMaxSize(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_batch_max_size_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, { previous: 5, batchMaxSize: null });
});

test('EB-483: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(3);
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, '3', {}] as unknown[]) {
    assert.throws(
      () => bus.setDefaultBatchMaxSize(bad as number),
      RangeError,
      `n=${String(bad)}`,
    );
    assert.equal(bus.getDefaultBatchMaxSize(), 3);
  }
  assert.throws(() => bus.setDefaultBatchMaxSize(5, ''), RangeError);
  assert.equal(bus.getDefaultBatchMaxSize(), 3);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_max_size_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});
