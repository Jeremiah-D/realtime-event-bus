import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/**
 * Bus-level terminal batch maxWaitMs default (EB-485):
 * `setDefaultBatchMaxWaitMs` installs the linger wait new batch
 * subscribers fall back to when they opt into batching but omit their
 * own `maxWaitMs` — the priority chain is subscriber-explicit > bus
 * default > 10 — and the default never enables batching: a subscriber
 * that does not opt into `batch` is untouched by it. Unlike the
 * maxSize default (EB-483), `0` is a legal default here: it disables
 * lingering for new batch subscribers.
 *
 * Linger assertions follow `batch.test.ts` and
 * `default-batch-max-size.test.ts`: a partial batch is collected after
 * its `maxWaitMs` elapses; a full batch delivers immediately on flush.
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

test('EB-485: the factory bus default is unset (batch: true still lingers 10ms)', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultBatchMaxWaitMs(), undefined);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  bus.publish('t', 'a');
  // Assert synchronously: publish never delivers a partial batch in
  // the same tick, so no timer can have fired yet — unlike awaiting a
  // setImmediate first, which under full-suite load can stall past
  // the 10ms linger and let the batch deliver before the check.
  assert.deepEqual(calls, [], 'the partial batch is still lingering at maxWaitMs 10');
  await flush();
  await sleep(30);
  assert.deepEqual(calls, [['a']]);
});

test('EB-485: a new batch subscriber takes the bus default maxWaitMs (50ms, real timer)', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxWaitMs(50);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  bus.publish('t', 'a');
  await flush();
  await sleep(20);
  assert.deepEqual(calls, [], '20ms in, the partial batch is still lingering at maxWaitMs 50');
  await sleep(130);
  assert.deepEqual(calls, [['a']], 'the partial batch is delivered once the 50ms linger elapses');
});

test('EB-485: an explicit maxWaitMs beats the bus default in both directions', async () => {
  // Explicit 0 beats a bus default of 50: no lingering at all.
  const instant = new EventBus();
  instant.setDefaultBatchMaxWaitMs(50);
  const instantRec = batchRecorder();
  instant.subscribe('t', instantRec.handler, { batch: { maxWaitMs: 0 } });
  instant.publish('t', 'a');
  await flush();
  // A 0ms linger still goes through the timer: the partial batch
  // delivers on the next timer tick, not inside the flush itself.
  await sleep(10);
  assert.deepEqual(instantRec.calls, [['a']]);

  // Explicit 200 beats a bus default of 50: the batch waits longer.
  const patient = new EventBus();
  patient.setDefaultBatchMaxWaitMs(50);
  const patientRec = batchRecorder();
  patient.subscribe('t', patientRec.handler, { batch: { maxWaitMs: 200 } });
  patient.publish('t', 'a');
  await flush();
  await sleep(100);
  assert.deepEqual(patientRec.calls, [], '100ms in, the explicit 200ms linger is still holding');
  await sleep(150);
  assert.deepEqual(patientRec.calls, [['a']]);
});

test('EB-485: a bus default of 0 disables lingering for new batch subscribers', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxWaitMs(0);
  assert.equal(bus.getDefaultBatchMaxWaitMs(), 0);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  // A 0ms linger still goes through the timer: the partial batch
  // delivers on the next timer tick, not inside the flush itself.
  await sleep(10);
  assert.deepEqual(calls, [['a', 'b']], 'with maxWaitMs 0 the partial batch delivers on the next tick');
});

test('EB-485: clearing with null returns new batch subscribers to 10ms', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxWaitMs(50);
  bus.setDefaultBatchMaxWaitMs(null);
  assert.equal(bus.getDefaultBatchMaxWaitMs(), undefined);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  bus.publish('t', 'a');
  await flush();
  await sleep(30);
  assert.deepEqual(calls, [['a']], '30ms is past the restored 10ms linger but short of 50ms');
});

test('EB-485: revising the bus default never moves an already-subscribed wait or an armed linger timer', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxWaitMs(50);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  bus.publish('t', 'a');
  await flush();
  // The linger timer for 'a' is already armed at 50ms; revising the
  // default to 200 must not stretch it.
  bus.setDefaultBatchMaxWaitMs(200);
  await sleep(100);
  assert.deepEqual(calls, [['a']], 'the armed timer keeps its old 50ms wait');

  // The subscriber keeps the 50ms wait it was baked with, too.
  bus.publish('t', 'b');
  await flush();
  await sleep(20);
  assert.deepEqual(calls, [['a']], 'the baked 50ms wait still holds at 20ms');
  await sleep(100);
  assert.deepEqual(calls, [['a'], ['b']]);

  // A subscriber joining after the revision takes the new default.
  const lateRec = batchRecorder();
  bus.subscribe('u', lateRec.handler, { batch: true });
  bus.publish('u', 'x');
  await flush();
  await sleep(100);
  assert.deepEqual(lateRec.calls, [], 'the late subscriber lingers the new 200ms default');
  await sleep(150);
  assert.deepEqual(lateRec.calls, [['x']]);
});

test('EB-485: the default never enables batching', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxWaitMs(50);
  // No `batch` opt-in: still one handler call per message.
  const plainRec = batchRecorder();
  bus.subscribe('t', plainRec.handler);
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(plainRec.calls, [[0], [1], [2]]);
});

test('EB-485: the maxSize default (EB-483) resolves independently of the maxWaitMs default', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxSize(3);
  bus.setDefaultBatchMaxWaitMs(50);
  const { calls, handler } = batchRecorder();
  bus.subscribe('t', handler, { batch: true });
  for (let i = 0; i < 3; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(calls, [[0, 1, 2]], 'a full batch of 3 delivers immediately');
  bus.publish('t', 3);
  await flush();
  await sleep(20);
  assert.deepEqual(calls, [[0, 1, 2]], 'the short batch lingers the 50ms default, not 10ms');
  await sleep(100);
  assert.deepEqual(calls, [
    [0, 1, 2],
    [3],
  ]);
});

test('EB-485: setBatch partial revision still takes the live value, and null still clears to one-at-a-time', async () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxWaitMs(50);
  const { calls, handler } = batchRecorder();
  const sub = bus.subscribe('t', handler, { batch: true });
  // Partial revision: only maxSize moves; maxWaitMs keeps its live 50.
  bus.setBatch(sub.id, { maxSize: 2 });
  bus.publish('t', 'a');
  await flush();
  await sleep(20);
  assert.deepEqual(calls, [], 'the live 50ms wait survives the partial setBatch');
  await sleep(100);
  assert.deepEqual(calls, [['a']]);
  // Null still clears the whole subscriber back to one-at-a-time, and
  // the bus default does not re-apply to it.
  bus.setBatch(sub.id, null);
  bus.publish('t', 'b');
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(calls.slice(1), [['b'], ['c']]);
});

test('EB-485: revising to the current default is a no-op with no audit, and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxWaitMs(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_max_wait_ms_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultBatchMaxWaitMs(50);
  assert.equal(bus.getDefaultBatchMaxWaitMs(), 50);
  bus.setDefaultBatchMaxWaitMs(50);
  assert.equal(bus.getDefaultBatchMaxWaitMs(), 50);
  const audit = bus.getSubscriptionAudit({ action: 'default_batch_max_wait_ms_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, batchMaxWaitMs: 50 });
  bus.setDefaultBatchMaxWaitMs(80, 'ops');
  assert.equal(bus.getDefaultBatchMaxWaitMs(), 80);
  const revised = bus.getSubscriptionAudit({ action: 'default_batch_max_wait_ms_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, { previous: 50, batchMaxWaitMs: 80 });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultBatchMaxWaitMs(0);
  assert.equal(bus.getDefaultBatchMaxWaitMs(), 0, '0 is a real default, distinct from unset');
  const zeroed = bus.getSubscriptionAudit({ action: 'default_batch_max_wait_ms_revised' });
  assert.equal(zeroed.length, 3);
  assert.deepEqual(zeroed[2].detail, { previous: 80, batchMaxWaitMs: 0 });
  bus.setDefaultBatchMaxWaitMs(null);
  assert.equal(bus.getDefaultBatchMaxWaitMs(), undefined);
  const cleared = bus.getSubscriptionAudit({ action: 'default_batch_max_wait_ms_revised' });
  assert.equal(cleared.length, 4);
  assert.deepEqual(cleared[3].detail, { previous: 0, batchMaxWaitMs: null });
});

test('EB-485: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setDefaultBatchMaxWaitMs(50);
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, '50', {}] as unknown[]) {
    assert.throws(
      () => bus.setDefaultBatchMaxWaitMs(bad as number),
      RangeError,
      `n=${String(bad)}`,
    );
    assert.equal(bus.getDefaultBatchMaxWaitMs(), 50);
  }
  assert.throws(() => bus.setDefaultBatchMaxWaitMs(80, ''), RangeError);
  assert.equal(bus.getDefaultBatchMaxWaitMs(), 50);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_batch_max_wait_ms_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});
