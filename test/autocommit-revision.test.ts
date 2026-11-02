import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

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

const noop = (): void => {};

test('setGroupAutoCommit retunes the live cadence: tightening commits sooner', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 60_000 } });
  assert.equal(bus.getGroupAutoCommit('g'), 60_000);
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  // Nothing committed yet at the 60s cadence.
  await sleep(100);
  assert.deepEqual(bus.getCommittedOffsets('g'), {});
  bus.setGroupAutoCommit('g', 15);
  assert.equal(bus.getGroupAutoCommit('g'), 15);
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 3, 5000, 'revised cadence to commit seq 3');
  const audit = bus.getSubscriptionAudit({ action: 'group_auto_commit_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].groupId, 'g');
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, { intervalMs: 15, oldIntervalMs: 60_000 });
});

test('setGroupAutoCommit loosening re-arms the timer: no commit until the new cadence', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 15 } });
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 3, 5000, 'initial cadence to commit seq 3');
  bus.setGroupAutoCommit('g', 60_000);
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  // The old 15ms timer is replaced, not left running: nothing more
  // commits for a long while at the new 60s cadence.
  await sleep(300);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });
  // And the new cadence is live: tightening back commits the rest.
  bus.setGroupAutoCommit('g', 15);
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 6, 5000, 're-tightened cadence to commit seq 6');
});

test('setGroupAutoCommit(null) flushes once, stops the timers, and clears the interval', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 60_000 } });
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  // The clear flushes synchronously at the old cadence.
  bus.setGroupAutoCommit('g', null);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });
  assert.equal(bus.getGroupAutoCommit('g'), undefined);
  for (let i = 0; i < 3; i++) bus.publish('t', { i });
  await sleep(200);
  assert.deepEqual(bus.getCommittedOffsets('g'), { t: 3 });
  const audit = bus.getSubscriptionAudit({ action: 'group_auto_commit_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { intervalMs: null, oldIntervalMs: 60_000 });
  // After a clear the group holds no interval: a further revision is
  // refused until a member opts in again at subscribe time.
  assert.throws(() => bus.setGroupAutoCommit('g', 15), RangeError);
  assert.throws(() => bus.setGroupAutoCommit('g', null), RangeError);
});

test('setGroupAutoCommit fail-closed: never-opted-in group, unknown group, illegal values', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('plain', 't', noop);
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 100 } });
  assert.throws(() => bus.setGroupAutoCommit('plain', 50), RangeError);
  assert.throws(() => bus.setGroupAutoCommit('nope', 50), RangeError);
  assert.throws(() => bus.setGroupAutoCommit('', 50), RangeError);
  const bad: unknown[] = [0, -5, NaN, Number.POSITIVE_INFINITY, 'ten', undefined, {}];
  for (const intervalMs of bad) {
    assert.throws(
      () => bus.setGroupAutoCommit('g', intervalMs as never),
      RangeError,
      `intervalMs ${JSON.stringify(intervalMs)}`,
    );
  }
  assert.throws(() => bus.setGroupAutoCommit('g', 50, ''), RangeError);
  // Every failure leaves the live interval and the audit log untouched.
  assert.equal(bus.getGroupAutoCommit('g'), 100);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_auto_commit_revised' }).length, 0);
  assert.equal(bus.getGroupAutoCommit('plain'), undefined);
  assert.throws(() => bus.getGroupAutoCommit(''), RangeError);
});

test('setGroupAutoCommit with the current interval is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 100 } });
  bus.setGroupAutoCommit('g', 100);
  assert.equal(bus.getSubscriptionAudit({ action: 'group_auto_commit_revised' }).length, 0);
  assert.equal(bus.getGroupAutoCommit('g'), 100);
});

test('a later member must opt in with the revised interval; a non-opt-in member joins freely', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 100 } });
  bus.setGroupAutoCommit('g', 250);
  const before = bus.getStats().totalSubscribers;
  assert.throws(
    () => bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 100 } }),
    /auto-commit interval mismatch: group uses 250ms, got 100ms/,
  );
  // Fail-fast before anything registers: no half-joined member leaks.
  assert.equal(bus.getStats().totalSubscribers, before);
  // The revised value joins fine, and so does a member that does not
  // opt in at all — it simply runs no timer.
  const revised = bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 250 } });
  const plain = bus.subscribeToGroup('g', 't', noop);
  assert.equal(bus.getGroupAutoCommit('g'), 250);
  revised.unsubscribe();
  plain.unsubscribe();
});

test('the revised interval is remembered while an armed member remains, and re-fixed by a new generation', async () => {
  const bus = new EventBus();
  const a = bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 100 } });
  const b = bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 100 } });
  bus.setGroupAutoCommit('g', 250);
  a.unsubscribe();
  // One armed member remains, so the revised interval persists.
  assert.equal(bus.getGroupAutoCommit('g'), 250);
  assert.throws(
    () => bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 100 } }),
    RangeError,
  );
  b.unsubscribe();
  // The last armed member left: the interval is dropped with it.
  assert.equal(bus.getGroupAutoCommit('g'), undefined);
  assert.throws(() => bus.setGroupAutoCommit('g', 250), RangeError);
  // A new generation re-fixes its interval on first opt-in.
  const c = bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 400 } });
  assert.equal(bus.getGroupAutoCommit('g'), 400);
  c.unsubscribe();
});

test('setGroupAutoCommit records a custom actor on the audit entry', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 100 } });
  bus.setGroupAutoCommit('g', 200, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'group_auto_commit_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[0].detail, { intervalMs: 200, oldIntervalMs: 100 });
});

test('revising re-arms every armed member: a second member commits on the new cadence too', async () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 60_000 } });
  bus.subscribeToGroup('g', 't', noop, { autoCommit: { intervalMs: 60_000 } });
  for (let i = 0; i < 4; i++) bus.publish('t', { i });
  bus.setGroupAutoCommit('g', 15);
  await waitFor(() => bus.getCommittedOffsets('g')['t'] === 4, 5000, 'both members committing on the revised cadence');
  assert.ok(bus.getStats().autoCommittedOffsets >= 1);
});
