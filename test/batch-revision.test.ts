import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('setBatch partial revision keeps the omitted field and applies the new size', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  const sub = bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 3, maxWaitMs: 10_000 },
  });

  bus.setBatch(sub.id, { maxSize: 2 });
  bus.publish('t', 0);
  bus.publish('t', 1);
  await flush();
  assert.deepEqual(batches, [[0, 1]], 'two messages now fill the revised batch');

  const audit = bus.getSubscriptionAudit({ action: 'batch_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    batch: { maxSize: 2, maxWaitMs: 10_000 },
    oldBatch: { maxSize: 3, maxWaitMs: 10_000 },
  });
  sub.unsubscribe();
});

test('setBatch shrink delivers an already-over-limit pending batch whole before collecting', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  const sub = bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 5, maxWaitMs: 10_000 },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(batches, [], 'the partial batch is still lingering');

  bus.setBatch(sub.id, { maxSize: 1 });
  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(
    batches,
    [['a', 'b'], ['c']],
    'the over-limit pending batch is delivered whole, then the new size collects',
  );
  sub.unsubscribe();
});

test('setBatch new maxWaitMs applies from the next linger, not the armed one', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  const sub = bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 5, maxWaitMs: 40 },
  });
  bus.publish('t', 'a');
  await flush();
  // The linger timer is already armed with the old 40 ms wait.
  bus.setBatch(sub.id, { maxWaitMs: 10_000 });
  await sleep(90);
  assert.deepEqual(batches, [['a']], 'the armed timer keeps its old wait');

  // The next linger arms with the revised wait.
  bus.publish('t', 'b');
  await flush();
  await sleep(90);
  assert.deepEqual(batches, [['a']], 'the new linger has not fired yet');
  // Clear to deliver the still-lingering batch and leave no live timer.
  bus.setBatch(sub.id, null);
  assert.deepEqual(batches, [['a'], ['b']]);
  sub.unsubscribe();
});

test('setBatch null delivers the pending batch, cancels the linger, and goes direct', async () => {
  const bus = new EventBus();
  const calls: unknown[] = [];
  const sub = bus.subscribe(
    't',
    (msgs) => calls.push(Array.isArray(msgs) ? msgs.map((m) => m.payload) : (msgs as { payload: unknown }).payload),
    { batch: { maxSize: 5, maxWaitMs: 10_000 } },
  );
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  assert.deepEqual(calls, [], 'the partial batch is still lingering');

  bus.setBatch(sub.id, null);
  assert.deepEqual(calls, [['a', 'b']], 'clearing delivers the pending batch immediately, never discarding it');
  await sleep(30);
  assert.deepEqual(calls, [['a', 'b']], 'the cancelled linger timer never fires a second copy');

  bus.publish('t', 'c');
  await flush();
  assert.deepEqual(calls, [['a', 'b'], 'c'], 'later messages go straight to the handler one at a time');

  const audit = bus.getSubscriptionAudit({ action: 'batch_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    batch: null,
    oldBatch: { maxSize: 5, maxWaitMs: 10_000 },
  });

  assert.throws(() => bus.setBatch(sub.id, { maxSize: 2 }), RangeError, 'a revision after a clear is refused');
  sub.unsubscribe();
});

test('setBatch is fail-closed and leaves the live batch and audit untouched', async () => {
  const bus = new EventBus();
  const batches: unknown[][] = [];
  const sub = bus.subscribe('t', (msgs) => batches.push(msgs.map((m) => m.payload)), {
    batch: { maxSize: 3, maxWaitMs: 10_000 },
  });
  const plain = bus.subscribe('u', () => {});

  assert.throws(() => bus.setBatch('no-such-sub', { maxSize: 2 }), RangeError);
  assert.throws(() => bus.setBatch(plain.id, { maxSize: 2 }), RangeError, 'an unbatched subscriber cannot be revised');
  assert.throws(() => bus.setBatch(sub.id, undefined as never), RangeError, 'undefined is not a clear');
  assert.throws(() => bus.setBatch(sub.id, 'x' as never), RangeError);
  for (const bad of [
    { maxSize: 0 },
    { maxSize: -1 },
    { maxSize: 1.5 },
    { maxSize: NaN },
    { maxWaitMs: -1 },
    { maxWaitMs: NaN },
    { maxWaitMs: Infinity },
  ]) {
    assert.throws(() => bus.setBatch(sub.id, bad), RangeError);
  }
  assert.equal(bus.getSubscriptionAudit({ action: 'batch_revised' }).length, 0, 'no failed revision is audited');

  // The original batch parameters are untouched by every failed attempt.
  bus.publish('t', 0);
  bus.publish('t', 1);
  bus.publish('t', 2);
  await flush();
  assert.deepEqual(batches, [[0, 1, 2]]);
  sub.unsubscribe();
  plain.unsubscribe();
});
