/**
 * Bus-level pull buffer bound revision (EB-319).
 *
 * `setPullBuffer` retunes how many of the most recent admitted
 * messages per topic the in-memory ring retains for `pull()` — without
 * restarting the bus. The pull mechanism itself is never revised:
 * offsets stay caller-owned, long-poll waiters keep re-reading from
 * their own `fromOffset`, and pull stays independent of push-ACK. The
 * entries already buffered are retained, never cleared or rewritten:
 * shrinking does not evict immediately — the next publish on a topic
 * truncates that topic's ring down to the live bound — while a widened
 * bound simply retains more. Illegal values fail closed with the live
 * bound and the audit log untouched; an identical (or empty) revision
 * is a no-op with no audit. Only buses without `durableLogDir` are
 * affected in what `pull()` returns: with a durable log `pull()` reads
 * the log, not the ring.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

const audit = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'pull_buffer_revised' });

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-pull-buffer-'));

/** Publishes `count` numbered messages on topic `t` (seqs 1..count). */
function busWithMessages(capacity: number, count: number) {
  const bus = new EventBus({ pullBufferSize: capacity });
  for (let i = 1; i <= count; i += 1) {
    bus.publish('t', { n: i });
  }
  return bus;
}

const seqs = async (bus: EventBus, fromOffset = 0) =>
  (await bus.pull('t', { maxMessages: 100, fromOffset })).map((m) => m.seq);

test('constructor still rejects illegal pullBufferSize', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, '1000']) {
    assert.throws(() => new EventBus({ pullBufferSize: bad as never }), RangeError);
  }
});

test('setPullBuffer rejects illegal values and leaves the live bound untouched', async () => {
  const bus = busWithMessages(3, 5);
  assert.deepEqual(await seqs(bus), [3, 4, 5]);
  const bad: Array<unknown> = [
    { pullBufferSize: 0 },
    { pullBufferSize: -1 },
    { pullBufferSize: 1.5 },
    { pullBufferSize: Number.NaN },
    { pullBufferSize: '1000' },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setPullBuffer(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: same bound, same ring, no audit.
  assert.equal(audit(bus).length, 0);
  assert.deepEqual(await seqs(bus), [3, 4, 5]);
  // A valid revision afterwards still works on the untouched bound.
  bus.setPullBuffer({ pullBufferSize: 2 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { pullBufferSize: 2, oldPullBufferSize: 3 });
  // The ring is untouched until the next publish, which truncates it.
  assert.deepEqual(await seqs(bus), [3, 4, 5]);
  bus.publish('t', { n: 6 });
  assert.deepEqual(await seqs(bus), [5, 6]);
  bus.close();
});

test('setPullBuffer with identical values or an empty revision is a no-op with no audit', async () => {
  const bus = busWithMessages(2, 3);
  assert.deepEqual(await seqs(bus), [2, 3]);
  bus.setPullBuffer({ pullBufferSize: 2 });
  bus.setPullBuffer({});
  bus.setPullBuffer(null as never);
  assert.equal(audit(bus).length, 0);
  assert.deepEqual(await seqs(bus), [2, 3]);
  bus.close();
});

test('revision audits the old and new bound', () => {
  const bus = new EventBus();
  bus.setPullBuffer({ pullBufferSize: 500 });
  bus.setPullBuffer({ pullBufferSize: 2000 });
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, { pullBufferSize: 500, oldPullBufferSize: 1000 });
  assert.deepEqual(entries[1].detail, { pullBufferSize: 2000, oldPullBufferSize: 500 });
  bus.close();
});

test('shrinking truncates the oldest entries at the next publish, buffered entries retained', async () => {
  const bus = busWithMessages(3, 5);
  assert.deepEqual(await seqs(bus), [3, 4, 5]);
  bus.setPullBuffer({ pullBufferSize: 1 });
  // Shrinking evicts nothing immediately: the buffered entries stay.
  assert.deepEqual(await seqs(bus), [3, 4, 5]);
  // The next publish truncates the ring down to the live bound in one
  // step: only the freshest entry survives.
  bus.publish('t', { n: 6 });
  assert.deepEqual(await seqs(bus), [6]);
  bus.close();
});

test('widening the bound retains more entries without evicting', async () => {
  const bus = busWithMessages(1, 3);
  assert.deepEqual(await seqs(bus), [3]);
  bus.setPullBuffer({ pullBufferSize: 3 });
  bus.publish('t', { n: 4 });
  bus.publish('t', { n: 5 });
  assert.deepEqual(await seqs(bus), [3, 4, 5]);
  bus.close();
});

test('pull semantics untouched: fromOffset paging and pending waiters survive a revision', async () => {
  const bus = busWithMessages(5, 3);
  bus.setPullBuffer({ pullBufferSize: 2 });
  // Buffered entries are retained, so the caller-owned cursor still
  // pages through them; the next publish truncates to the live bound.
  assert.deepEqual(await seqs(bus, 1), [2, 3]);
  bus.publish('t', { n: 4 });
  assert.deepEqual(await seqs(bus, 0), [3, 4]);
  assert.deepEqual(await seqs(bus, 3), [4]);
  // A pending long-poll waiter re-reads from its own fromOffset on the
  // next publish, whatever the new bound is.
  const pending = bus.pull('t', { maxMessages: 10, waitMs: 5000, fromOffset: 4 });
  bus.setPullBuffer({ pullBufferSize: 1 });
  bus.publish('t', { n: 5 });
  assert.deepEqual(
    (await pending).map((m) => m.seq),
    [5],
  );
  bus.close();
});

test('durable-log boundary: with durableLogDir pull reads the log, not the ring', async () => {
  const bus = new EventBus({ durableLogDir: freshDir(), pullBufferSize: 2 });
  for (let i = 1; i <= 4; i += 1) {
    bus.publish('t', { n: i });
  }
  assert.deepEqual(await seqs(bus), [1, 2, 3, 4]);
  // The revision is still recorded, but what pull() returns does not
  // move: the log, not the ring, is the message source here.
  bus.setPullBuffer({ pullBufferSize: 1 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { pullBufferSize: 1, oldPullBufferSize: 2 });
  bus.publish('t', { n: 5 });
  assert.deepEqual(await seqs(bus), [1, 2, 3, 4, 5]);
  bus.close();
});
