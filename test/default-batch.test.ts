import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level default batch delivery opt-in (EB-521):
 * `setDefaultBatch` installs the `batch` new subscribers fall back to
 * when they omit their own — the priority chain is
 * subscriber-explicit > bus default > one-at-a-time delivery — and,
 * unlike the scalar batch defaults (EB-483/EB-485), this default CAN
 * enable batching: `true` opts omit-`batch` subscribers in at the
 * scalar defaults, an object opts them in at its fields, an explicit
 * `false` still opts out, and `null` clears back to no default.
 *
 * Batch assertions follow `batch.test.ts` and
 * `default-batch-max-size.test.ts`: full batches deliver immediately
 * on flush; a trailing partial batch lingers `maxWaitMs` (default
 * 10ms) and is collected after a short sleep.
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

test('EB-521: the factory bus default is unset (omitting batch stays one-to-one)', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultBatch(), undefined);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler);
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(calls, [[0], [1], [2]]);
});

test('EB-521: a default of true batches omitted-batch subscribers at 100 / linger 10ms', async () => {
  const bus = new EventBus();
  bus.setDefaultBatch(true);
  assert.equal(bus.getDefaultBatch(), true);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler);
  for (let i = 0; i < 100; i++) bus.publish('t', i);
  await flush();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].length, 100);

  // A partial batch still lingers the documented 10ms.
  const partialRec = batchRecorder();
  bus.subscribe('u', partialRec.handler);
  bus.publish('u', 'a');
  await flush();
  assert.deepEqual(partialRec.calls, [], 'the partial batch is still lingering at maxWaitMs 10');
  await sleep(50);
  assert.deepEqual(partialRec.calls, [['a']]);
});

test('EB-521: a default object batches omitted-batch subscribers at its fields', async () => {
  const bus = new EventBus();
  bus.setDefaultBatch({ maxSize: 3 });
  assert.deepEqual(bus.getDefaultBatch(), { maxSize: 3 });
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler);
  for (let i = 0; i < 7; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(calls, [
    [0, 1, 2],
    [3, 4, 5],
  ]);
  await sleep(30);
  assert.deepEqual(calls, [[0, 1, 2], [3, 4, 5], [6]]);
});

test('EB-521: an explicit batch: false still delivers one-to-one under a default', async () => {
  const bus = new EventBus();
  bus.setDefaultBatch(true);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: false });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(calls, [[0], [1], [2]]);
});

test('EB-521: explicit batch object fields beat the default fields', async () => {
  const bus = new EventBus();
  bus.setDefaultBatch({ maxSize: 2, maxWaitMs: 0 });
  // Explicit maxSize 5 beats the default 2.
  const looseRec = batchRecorder();
  bus.subscribe('t', looseRec.handler, { batch: { maxSize: 5, maxWaitMs: 0 } });
  for (let i = 0; i < 10; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(looseRec.calls, [
    [0, 1, 2, 3, 4],
    [5, 6, 7, 8, 9],
  ]);

  // An explicit maxWaitMs 0 beats a default linger: the partial batch
  // delivers on the next tick instead of waiting out the default wait.
  const waitBus = new EventBus();
  waitBus.setDefaultBatch({ maxSize: 100, maxWaitMs: 500 });
  const waitRec = batchRecorder();
  waitBus.subscribe('u', waitRec.handler, { batch: { maxWaitMs: 0 } });
  waitBus.publish('u', 'a');
  await flush();
  await sleep(10);
  await flush();
  assert.deepEqual(waitRec.calls, [['a']]);
});

test('EB-521: scalar defaults still fill the fields a default leaves out', async () => {
  // Default true + a scalar maxSize default of 3 → batches of 3.
  const bus = new EventBus();
  bus.setDefaultBatch(true);
  bus.setDefaultBatchMaxSize(3);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler);
  for (let i = 0; i < 7; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(calls, [
    [0, 1, 2],
    [3, 4, 5],
  ]);
  await sleep(30);
  assert.deepEqual(calls, [[0, 1, 2], [3, 4, 5], [6]]);

  // Default { maxWaitMs: 50 } + a scalar maxSize default of 2 → pairs
  // that linger ~50ms instead of the built-in 10ms.
  const waitBus = new EventBus();
  waitBus.setDefaultBatch({ maxWaitMs: 50 });
  waitBus.setDefaultBatchMaxSize(2);
  const waitRec = batchRecorder();
  waitBus.subscribe('u', waitRec.handler);
  waitBus.publish('u', 'a');
  await flush();
  assert.deepEqual(waitRec.calls, [], 'the partial batch is still lingering at maxWaitMs 50');
  await sleep(20);
  assert.deepEqual(waitRec.calls, [], 'still lingering: 50ms has not elapsed');
  await sleep(60);
  assert.deepEqual(waitRec.calls, [['a']]);
});

test('EB-521: clearing with null returns new omit-batch subscribers to one-to-one', async () => {
  const bus = new EventBus();
  bus.setDefaultBatch(true);
  bus.setDefaultBatch(null);
  assert.equal(bus.getDefaultBatch(), undefined);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler);
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(calls, [[0], [1], [2]]);
});

test('EB-521: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  // Clearing an already-unset default audits nothing.
  bus.setDefaultBatch(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_revised' }).length,
    0,
  );
  bus.setDefaultBatch(true);
  bus.setDefaultBatch(true);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_revised' }).length,
    1,
    're-setting true audits nothing',
  );
  // Re-setting an equal object — including one spelled with an
  // explicitly-undefined field — audits nothing.
  bus.setDefaultBatch({ maxSize: 3 });
  bus.setDefaultBatch({ maxSize: 3, maxWaitMs: undefined });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_revised' }).length,
    2,
    're-setting an equal object audits nothing',
  );
  bus.setDefaultBatch(null);
  bus.setDefaultBatch(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_revised' }).length,
    3,
    'clearing an already-cleared default audits nothing',
  );
});

test('EB-521: illegal values throw RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultBatch({ maxSize: 3 });
  const bad = [
    false,
    0,
    1,
    'batch',
    ['x'],
    { maxSize: 0 },
    { maxSize: 1.5 },
    { maxSize: -1 },
    { maxWaitMs: -1 },
    { maxWaitMs: Number.NaN },
    { maxWaitMs: Number.POSITIVE_INFINITY },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultBatch(value as true),
      RangeError,
      `batch=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultBatch(), { maxSize: 3 });
  }
  assert.throws(() => bus.setDefaultBatch(true, ''), RangeError);
  assert.deepEqual(bus.getDefaultBatch(), { maxSize: 3 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-521: revising the default never moves an already-subscribed subscriber or pending batch', async () => {
  const bus = new EventBus();
  // A plain subscriber that joined before any default stays one-to-one.
  const plainRec = batchRecorder();
  bus.subscribe('plain', plainRec.handler);
  bus.setDefaultBatch(true);
  for (let i = 0; i < 3; i++) bus.publish('plain', i);
  await flush();
  assert.deepEqual(plainRec.calls, [[0], [1], [2]]);

  // A subscriber that took the default { maxSize: 3 } keeps it, pending
  // batch and all, across a revision to { maxSize: 5 } and a clear.
  bus.setDefaultBatch({ maxSize: 3 });
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler);
  bus.publish('t', 0);
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(calls, [], 'the partial batch of 2 is still lingering');
  bus.setDefaultBatch({ maxSize: 5 });
  bus.setDefaultBatch(null);
  await sleep(30);
  assert.deepEqual(calls, [[0, 1]], 'the pending batch still delivers whole at its baked size');
  for (let i = 2; i < 6; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(
    calls,
    [
      [0, 1],
      [2, 3, 4],
    ],
    'the early subscriber keeps the size it was baked with',
  );
  await sleep(30);
  assert.deepEqual(calls, [[0, 1], [2, 3, 4], [5]]);

  // A subscriber joining after the clear is one-to-one again.
  const lateRec = batchRecorder();
  bus.subscribe('u', lateRec.handler);
  for (let i = 0; i < 3; i++) bus.publish('u', i);
  await flush();
  assert.deepEqual(lateRec.calls, [[0], [1], [2]]);
});

test('EB-521: the combination gate reads the effective batch and maxInflight', () => {
  // Default batch + an explicit maxInflight still throws.
  const explicit = new EventBus();
  explicit.setDefaultBatch(true);
  assert.throws(
    () => explicit.subscribe('t', () => {}, { maxInflight: 2 }),
    RangeError,
  );

  // Default batch + the EB-466 bus default maxInflight still throws.
  const busDefault = new EventBus();
  busDefault.setDefaultBatch({ maxSize: 3 });
  busDefault.setDefaultMaxInFlight(2);
  assert.throws(() => busDefault.subscribe('t', () => {}, {}), RangeError);

  // An explicit batch: false bypasses the gate under both defaults.
  const bypass = new EventBus();
  bypass.setDefaultBatch(true);
  bypass.setDefaultMaxInFlight(2);
  const sub = bypass.subscribe('t', () => {}, { batch: false, maxInflight: 2 });
  assert.equal(typeof sub.id, 'string');

  // And with no default installed, batch + maxInflight still throws
  // exactly as before.
  const plain = new EventBus();
  assert.throws(
    () => plain.subscribe('t', () => {}, { batch: true, maxInflight: 2 }),
    RangeError,
  );
});

test('EB-521: setBatch(null) on a default-batched subscriber still clears to one-to-one and the default is not re-applied', async () => {
  const bus = new EventBus();
  bus.setDefaultBatch({ maxSize: 2 });
  const { calls, handler } = batchRecorder();
  const sub = bus.subscribe('t', handler);
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(calls, [
    [0, 1],
    [2, 3],
  ]);
  // Null clears the whole subscriber back to one-at-a-time delivery.
  bus.setBatch(sub.id, null);
  bus.publish('t', 4);
  bus.publish('t', 5);
  await flush();
  assert.deepEqual(calls.slice(2), [[4], [5]], 'the default is not re-applied after a clear');
  // A revision after the clear is still refused.
  assert.throws(() => bus.setBatch(sub.id, { maxSize: 5 }), RangeError);
});

test('EB-521: audit previous/batch are exact and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultBatch(true, 'ops');
  let audit = bus.getSubscriptionAudit({ action: 'default_batch_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, batch: true });
  assert.equal(audit[0].actor, 'ops');
  bus.setDefaultBatch({ maxSize: 3, maxWaitMs: 50 });
  assert.deepEqual(bus.getDefaultBatch(), { maxSize: 3, maxWaitMs: 50 });
  audit = bus.getSubscriptionAudit({ action: 'default_batch_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: true,
    batch: { maxSize: 3, maxWaitMs: 50 },
  });
  bus.setDefaultBatch(null);
  assert.equal(bus.getDefaultBatch(), undefined);
  audit = bus.getSubscriptionAudit({ action: 'default_batch_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[2].detail, {
    previous: { maxSize: 3, maxWaitMs: 50 },
    batch: null,
  });
});

test('EB-521: the setter stores a defensive copy and the getter returns copies for objects', () => {
  const bus = new EventBus();
  const input: { maxSize?: number; maxWaitMs?: number } = { maxSize: 3 };
  bus.setDefaultBatch(input);
  input.maxSize = 99;
  assert.deepEqual(bus.getDefaultBatch(), { maxSize: 3 }, 'mutating the setter argument must not move the live default');
  const got = bus.getDefaultBatch() as { maxSize?: number };
  assert.ok(typeof got === 'object' && got !== null);
  got.maxSize = 99;
  assert.deepEqual(bus.getDefaultBatch(), { maxSize: 3 }, 'mutating the getter result must not move the live default');
  // An empty object opts in and takes the whole scalar chain (100 / 10).
  const empty = new EventBus();
  empty.setDefaultBatch({});
  assert.deepEqual(empty.getDefaultBatch(), {});
});
