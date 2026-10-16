import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Polls until `cond()` holds or the deadline passes; throws on timeout. */
async function waitFor(cond: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (cond()) return;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for: ${label}`);
    await sleep(10);
  }
}

test('auto-commit fires periodically and checkpoints the highest handed seq', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 15 } });
  for (let i = 0; i < 5; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 5, 5000, 'auto-commit to reach seq 5');
  assert.ok(bus.getStats().autoCommittedOffsets >= 1);
});

test('auto-commit is off by default: zero behavior change', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  // Several commit intervals' worth of wall-clock: nothing may commit.
  await sleep(200);
  assert.deepEqual(bus.getCommittedOffsets('g'), {});
  assert.equal(bus.getStats().autoCommittedOffsets, 0);
});

test('autoCommit option validation fails fast with RangeError', () => {
  const bus = new EventBus();
  const bad: unknown[] = [
    { intervalMs: 0 },
    { intervalMs: -5 },
    { intervalMs: NaN },
    { intervalMs: Number.POSITIVE_INFINITY },
    { intervalMs: 'ten' },
    'yes',
    42,
    [],
    { intervalMs: 10, bogus: true },
  ];
  for (const autoCommit of bad) {
    assert.throws(
      () => bus.subscribeToGroup('g', 't', () => {}, { autoCommit } as never),
      RangeError,
      `autoCommit ${JSON.stringify(autoCommit)}`,
    );
  }
  // Fail-fast before anything registers: no half-joined subscriber leaks.
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('leave flushes pending offsets synchronously, before the linger window is computed', () => {
  const dir = mkdtempSync(join(tmpdir(), 'eb-autocommit-'));
  const bus = new EventBus({ durableLogDir: dir });
  const events: GroupRebalanceEvent[] = [];
  bus.subscribeToGroup('g', 't', () => {}, { onRebalance: (e) => events.push(e) });
  const leaver = bus.subscribeToGroup('g', 't', () => {}, {
    autoCommit: { intervalMs: 60_000 }, // far away: only the leave flush can commit
    handoffLingerMs: 60_000,
  });
  bus.publish('t', {});
  bus.publish('t', {});
  bus.publish('t', {});
  leaver.unsubscribe();
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });
  // The flush ran before the linger window was computed: with nothing
  // left uncommitted, no window opened and the leave event carries none.
  const leaveEvent = events.find((e) => e.trigger === 'leave');
  assert.ok(leaveEvent !== undefined);
  assert.equal(leaveEvent.lingerUntil, undefined);
});

test('join rebalance flushes pending offsets (Kafka revoke hook)', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 60_000 } });
  bus.publish('t', {});
  bus.publish('t', {});
  // The join below fires a rebalance, which flushes the group's pending
  // checkpoints even though the commit timer is far away.
  bus.subscribeToGroup('g', 't', () => {});
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 2 });
  assert.ok(bus.getStats().autoCommittedOffsets >= 1);
});

test('manual commitOffset and auto-commit are idempotent (highest-seq-wins)', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 15 } });
  for (let i = 0; i < 5; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 5, 5000, 'auto-commit to reach 5');
  // The operator moves the checkpoint further by hand...
  bus.commitOffset('g', 't', 10);
  // ...and the auto-commit's older pending seq must never regress it.
  await sleep(200);
  assert.equal(bus.getCommittedOffsets('g')['t'], 10);
});

test('partitioned groups auto-commit the group-level checkpoint', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 15 }, partitions: 2 });
  for (let i = 0; i < 4; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 4, 5000, 'auto-commit to reach 4');
});

test('static member limbo flushes pending offsets; rejoin re-arms the timer', () => {
  const bus = new EventBus();
  const opts = { instanceId: 'a', sessionTimeoutMs: 5000, autoCommit: { intervalMs: 60_000 } } as const;
  const a = bus.subscribeToGroup('g', 't', () => {}, opts);
  bus.publish('t', {});
  bus.publish('t', {});
  a.unsubscribe(); // static limbo: flushes, keeps the roster slot and timer
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 2 });
  // Rejoin inside the session window: no rebalance, the timer re-arms in
  // place (no leak, no double refcount).
  const a2 = bus.subscribeToGroup('g', 't', () => {}, opts);
  bus.publish('t', {});
  a2.unsubscribe();
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });
});

test('prometheus exposes the auto-commit counter', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 15 } });
  bus.publish('t', {});
  await waitFor(() => bus.getStats().autoCommittedOffsets >= 1, 5000, 'stat to move');
  const text = renderPrometheus(bus.getStats());
  assert.match(text, /eventbus_autocommitted_offsets_total [1-9]/);
});
