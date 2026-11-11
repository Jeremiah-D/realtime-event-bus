import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Polls until `cond()` holds or the deadline passes; throws on timeout. */
async function waitFor(cond: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const start = Date.now();
  for (;;) {
    if (cond()) return;
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for: ${label}`);
    await sleep(10);
  }
}

// EB-468: bus-level terminal group auto-commit default live revision.

test('EB-468: the factory bus default is unset (no auto-commit)', () => {
  const bus = new EventBus();
  assert.equal(bus.getGroupDefaultAutoCommit(), undefined);
});

test('EB-468: a new group whose member omits autoCommit arms the bus-default timer', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupAutoCommit('g'), 15, 'the default interval is recorded on the group');
  for (let i = 0; i < 5; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 5, 5000, 'auto-commit to reach seq 5');
  assert.ok(bus.getStats().autoCommittedOffsets >= 1);
});

test('EB-468: an explicit interval beats the bus default in both directions', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  // Explicit 60_000 beats a bus default of 15.
  const m1 = bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 60_000 } });
  assert.equal(bus.getGroupAutoCommit('g'), 60_000);
  m1.unsubscribe();

  // Explicit 15 beats a bus default of 60_000.
  bus.setGroupDefaultAutoCommit(60_000);
  const m2 = bus.subscribeToGroup('h', 't', () => {}, { autoCommit: { intervalMs: 15 } });
  assert.equal(bus.getGroupAutoCommit('h'), 15);
  m2.unsubscribe();
});

test('EB-468: a later member opting in with a different interval still throws, including against a default-derived interval', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupAutoCommit('g'), 15);
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 30 } }),
    /auto-commit interval mismatch/,
  );
  assert.equal(bus.getGroupMembers('g', 't').length, 1);
  // A member that agrees with the derived interval joins fine.
  const m2 = bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 15 } });
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-468: a member that does not opt in joins a group with an interval freely and the interval is unchanged', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  const m1 = bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 15 } });
  const m2 = bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  assert.equal(bus.getGroupAutoCommit('g'), 15);
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 3, 5000, 'auto-commit to reach seq 3');
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-468: clearing with null returns new groups to no auto-commit', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  bus.setGroupDefaultAutoCommit(null);
  assert.equal(bus.getGroupDefaultAutoCommit(), undefined);
  bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupAutoCommit('g'), undefined);
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await sleep(200);
  assert.deepEqual(bus.getCommittedOffsets('g'), {});
  assert.equal(bus.getStats().autoCommittedOffsets, 0);
});

test('EB-468: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_auto_commit_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setGroupDefaultAutoCommit(15);
  bus.setGroupDefaultAutoCommit(15);
  const audit = bus.getSubscriptionAudit({ action: 'group_default_auto_commit_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, intervalMs: 15 });
});

test('EB-468: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  for (const bad of [0, -1, Number.NaN, Infinity, '15'] as unknown[]) {
    assert.throws(
      () => bus.setGroupDefaultAutoCommit(bad as number),
      RangeError,
      `intervalMs=${String(bad)}`,
    );
    assert.equal(bus.getGroupDefaultAutoCommit(), 15);
  }
  assert.throws(() => bus.setGroupDefaultAutoCommit(20, ''), RangeError);
  assert.equal(bus.getGroupDefaultAutoCommit(), 15);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_auto_commit_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-468: revising the bus default never moves a live group interval', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 5; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 5, 5000, 'auto-commit to reach seq 5');
  bus.setGroupDefaultAutoCommit(60_000);
  assert.equal(bus.getGroupAutoCommit('g'), 15, 'the live group keeps its recorded interval');
  assert.equal(bus.getGroupDefaultAutoCommit(), 60_000);
  // The live timer still ticks at the old cadence: a 60s timer could
  // not commit within the wait window.
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 8, 5000, 'auto-commit to reach seq 8');
  // A later member must still use the recorded interval, not the new default.
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 60_000 } }),
    /auto-commit interval mismatch/,
  );
  const m2 = bus.subscribeToGroup('g', 't', () => {}, { autoCommit: { intervalMs: 15 } });
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-468: a drained and recreated group takes the default in effect at recreation', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  for (let i = 0; i < 2; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 2, 5000, 'auto-commit to reach seq 2');
  m1.unsubscribe();
  await flush();

  bus.setGroupDefaultAutoCommit(20);
  const m2 = bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupAutoCommit('g'), 20, 'the new generation takes the new default');
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 5, 5000, 'auto-commit to reach seq 5');
  m2.unsubscribe();
});

test('EB-468: setGroupAutoCommit still revises one live group and leaves the bus default alone', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupAutoCommit('g'), 15);
  bus.setGroupAutoCommit('g', 60_000);
  assert.equal(bus.getGroupAutoCommit('g'), 60_000);
  assert.equal(bus.getGroupDefaultAutoCommit(), 15, 'the per-group revision must not move the default');
  bus.setGroupAutoCommit('g', null);
  assert.equal(bus.getGroupAutoCommit('g'), undefined);
  assert.equal(bus.getGroupDefaultAutoCommit(), 15, 'clearing one group must not move the default');
  // After the per-group clear, a fresh joiner takes the bus default again.
  const m2 = bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupAutoCommit('g'), 15);
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 3, 5000, 'auto-commit to reach seq 3');
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-468: setGroupDefaultAutoCommit audits each revision with its old and new interval and actor', () => {
  const bus = new EventBus();
  bus.setGroupDefaultAutoCommit(15, 'ops');
  bus.setGroupDefaultAutoCommit(null, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'group_default_auto_commit_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, intervalMs: 15 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 15, intervalMs: null });
});
