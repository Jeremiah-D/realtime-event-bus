import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type ConsumerRebalanceListener,
  type GroupRebalanceEvent,
} from '../src/bus.ts';

/**
 * Consumer-group rebalance-callback revision (EB-439):
 * `setRebalanceCallback` swaps a group member's `onRebalance`
 * without re-subscribing — the only group-membership revision surface
 * the join/leave path left uncovered: the roster moves on its own,
 * this one moves who hears about the move. Only the callback moves:
 * the roster, the assignment, the debounce window, the linger
 * windows, and the member-level partition listener (EB-209) are
 * untouched, and a throwing callback is swallowed by `emitRebalance`,
 * so observation never blocks the rebalance.
 *
 * Join and leave notifications are synchronous: a `subscribeToGroup`
 * or `unsubscribe` call has already notified the survivors by the
 * time it returns (as in `groups.test.ts`).
 */

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = (): void => {};
const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-rebalance-cb-'));

/** The `rebalance_callback_revised` audit events on the bus. */
function callbackRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'rebalance_callback_revised' });
}

/** The `rebalance` audit events on the bus. */
function rebalanceAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'rebalance' });
}

test('swapping the callback routes the next join and leave to the new callback only', () => {
  const bus = new EventBus();
  const oldEvents: GroupRebalanceEvent[] = [];
  const newEvents: GroupRebalanceEvent[] = [];
  const subA = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: (e) => oldEvents.push(e),
  });
  const subB = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: (e) => oldEvents.push(e),
  });
  // Joins so far: A got one for its own join; both got one for B's join.
  assert.equal(oldEvents.filter((e) => e.trigger === 'join').length, 3);

  // Swap A's callback: the next join reaches it only.
  bus.setRebalanceCallback(subA.id, (e) => newEvents.push(e));
  const subC = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: (e) => oldEvents.push(e),
  });
  assert.equal(newEvents.length, 1);
  assert.equal(newEvents[0].groupId, 'workers');
  assert.equal(newEvents[0].pattern, 'jobs.*');
  assert.deepEqual(newEvents[0].members, [subA.id, subB.id, subC.id]);
  assert.equal(newEvents[0].trigger, 'join');
  assert.equal(newEvents[0].memberId, subC.id);
  assert.equal(oldEvents.filter((e) => e.trigger === 'join').length, 5, 'B and C heard the join on the old callback');

  // And the next leave reaches the new callback too.
  subB.unsubscribe();
  assert.equal(newEvents.length, 2);
  assert.equal(newEvents[1].trigger, 'leave');
  assert.equal(newEvents[1].memberId, subB.id);
  assert.deepEqual(newEvents[1].members, [subA.id, subC.id]);
  assert.equal(oldEvents.filter((e) => e.trigger === 'leave').length, 1, 'only C heard the leave on the old callback');
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: true,
    oldHasCallback: true,
  });
  subA.unsubscribe();
  subC.unsubscribe();
});

test('clearing with null stops delivery but the rebalance still happens and is still audited', () => {
  const bus = new EventBus();
  const eventsA: GroupRebalanceEvent[] = [];
  const eventsB: GroupRebalanceEvent[] = [];
  const subA = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: (e) => eventsA.push(e),
  });
  const subB = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: (e) => eventsB.push(e),
  });
  assert.equal(eventsA.length, 2, 'A heard its own join and B’s join');
  assert.equal(rebalanceAudits(bus).length, 2);

  // Clear A's callback, then add a member: A hears nothing, but the
  // rebalance still happens and is still audited.
  bus.setRebalanceCallback(subA.id, null);
  const subC = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: () => {},
  });
  assert.equal(eventsA.length, 2, 'a cleared callback never fires');
  assert.equal(eventsB.length, 2, 'the other member still hears the join');
  assert.equal(rebalanceAudits(bus).length, 3, 'the rebalance is still audited');
  assert.deepEqual(callbackRevisionAudits(bus)[0].detail, {
    hasCallback: false,
    oldHasCallback: true,
  });
  subA.unsubscribe();
  subB.unsubscribe();
  subC.unsubscribe();
});

test('setRebalanceCallback rejects unknown, empty, non-member, and invalid revisions fail-closed', () => {
  const bus = new EventBus();
  const eventsA: GroupRebalanceEvent[] = [];
  const subA = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: (e) => eventsA.push(e),
  });
  const plain = bus.subscribe('t2', () => {});
  const replacement = (_e: GroupRebalanceEvent): void => {};
  for (const bad of [
    () => bus.setRebalanceCallback('no-such-sub', replacement),
    () => bus.setRebalanceCallback('', replacement),
    () => bus.setRebalanceCallback(123 as never, replacement),
    () => bus.setRebalanceCallback(plain.id, replacement),
    () => bus.setRebalanceCallback(subA.id, 'nope' as never),
    () => bus.setRebalanceCallback(subA.id, 42 as never),
    () => bus.setRebalanceCallback(subA.id, {} as never),
    () => bus.setRebalanceCallback(subA.id, [] as never),
    () => bus.setRebalanceCallback(subA.id, undefined as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'failed revisions must not audit',
  );
  // The live callback, the roster, and the assignment are untouched:
  // the original callback still fires on the next join.
  const subB = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: () => {},
  });
  assert.equal(eventsA.length, 2);
  assert.equal(eventsA[1].trigger, 'join');
  assert.equal(eventsA[1].memberId, subB.id);
  subA.unsubscribe();
  subB.unsubscribe();
  plain.unsubscribe();
});

test('a revision identical to the live callback is a no-op with no audit', () => {
  const bus = new EventBus();
  const handler = (_e: GroupRebalanceEvent): void => {};
  const sub = bus.subscribeToGroup('workers', 'jobs.*', noop);
  // Clearing a callback that was never set is a no-op, not a clear.
  bus.setRebalanceCallback(sub.id, null);
  assert.equal(
    callbackRevisionAudits(bus).length,
    0,
    'clearing an absent callback must not audit',
  );

  bus.setRebalanceCallback(sub.id, handler);
  let audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });

  // Re-affirming the same function reference is a no-op.
  bus.setRebalanceCallback(sub.id, handler);
  audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 1, 'a same-value revision writes no second event');
  sub.unsubscribe();
});

test('a throwing callback is swallowed and the rebalance path is unaffected', () => {
  const bus = new EventBus();
  const eventsB: GroupRebalanceEvent[] = [];
  const eventsC: GroupRebalanceEvent[] = [];
  const subA = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: () => {
      throw new Error('boom');
    },
  });
  // Swap in the throwing callback via revision: B's join must still
  // notify B, and the join must still be audited.
  bus.setRebalanceCallback(subA.id, () => {
    throw new Error('boom');
  });
  const auditsBefore = rebalanceAudits(bus).length;
  let subB: { id: string; unsubscribe: () => void } | undefined;
  assert.doesNotThrow(() => {
    subB = bus.subscribeToGroup('workers', 'jobs.*', noop, {
      onRebalance: (e) => eventsB.push(e),
    });
  });
  assert.equal(eventsB.length, 1, 'the other member still hears the join');
  assert.equal(eventsB[0].trigger, 'join');
  assert.equal(rebalanceAudits(bus).length, auditsBefore + 1, 'the rebalance is still audited');

  // The same on a leave: C still hears B leave.
  const subC = bus.subscribeToGroup('workers', 'jobs.*', noop, {
    onRebalance: (e) => eventsC.push(e),
  });
  assert.equal(eventsC.length, 1);
  subB!.unsubscribe();
  const leaves = eventsC.filter((e) => e.trigger === 'leave');
  assert.equal(leaves.length, 1, 'the survivor still hears the leave');
  assert.equal(leaves[0].memberId, subB!.id);
  assert.equal(rebalanceAudits(bus).length, auditsBefore + 3);
  subA.unsubscribe();
  subC.unsubscribe();
});

test('the swap audits exact presence flags with the subscriber', () => {
  const bus = new EventBus();
  const first = (_e: GroupRebalanceEvent): void => {};
  const second = (_e: GroupRebalanceEvent): void => {};
  const sub = bus.subscribeToGroup('workers', 'jobs.*', noop);
  bus.setRebalanceCallback(sub.id, first);
  bus.setRebalanceCallback(sub.id, second);
  bus.setRebalanceCallback(sub.id, null);
  const audit = callbackRevisionAudits(bus);
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(audit[2].detail, { hasCallback: false, oldHasCallback: true });
  for (const entry of audit) {
    assert.equal(entry.subId, sub.id);
    assert.equal(entry.pattern, 'jobs.*');
    assert.equal(entry.actor, 'system');
  }
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
  sub.unsubscribe();
});

test('debounce merge counts and linger windows are unaffected by a callback revision', async () => {
  // Debounce: the merged event still carries the absorbed-change
  // count after the callback swap.
  const bus = new EventBus();
  const oldEvents: GroupRebalanceEvent[] = [];
  const newEvents: GroupRebalanceEvent[] = [];
  const a = bus.subscribeToGroup('g', 't', noop, {
    rebalanceDebounceMs: 60,
    onRebalance: (e) => oldEvents.push(e),
  });
  bus.setRebalanceCallback(a.id, (e) => newEvents.push(e));
  const b = bus.subscribeToGroup('g', 't', noop, {
    onRebalance: (e) => oldEvents.push(e),
  });
  const c = bus.subscribeToGroup('g', 't', noop, {
    onRebalance: (e) => oldEvents.push(e),
  });
  await sleep(150);
  assert.equal(newEvents.length, 1, 'one merged event reaches the new callback');
  assert.equal(newEvents[0].trigger, 'join');
  assert.equal(newEvents[0].memberId, c.id);
  assert.deepEqual(newEvents[0].members, [a.id, b.id, c.id]);
  assert.equal(newEvents[0].suppressedRebalances, 2, 'the merge count survives the swap');
  a.unsubscribe();
  b.unsubscribe();
  c.unsubscribe();

  // Linger: a leave still carries the window the leaver armed, and
  // the swapped callback of a survivor receives it intact.
  let now = 1_000_000;
  const dir = freshDir();
  const bus2 = new EventBus({ durableLogDir: dir, now: () => now });
  const survivorOld: GroupRebalanceEvent[] = [];
  const survivorNew: GroupRebalanceEvent[] = [];
  const observer: GroupRebalanceEvent[] = [];
  const leaver = bus2.subscribeToGroup('g', 'jobs.*', noop, { handoffLingerMs: 30_000 });
  const survivor = bus2.subscribeToGroup('g', 'jobs.*', noop, {
    handoffLingerMs: 30_000,
    onRebalance: (e) => survivorOld.push(e),
  });
  bus2.subscribeToGroup('g', 'jobs.*', noop, {
    onRebalance: (e) => observer.push(e),
  });
  for (let i = 0; i < 5; i += 1) bus2.publish('jobs.email', {});
  await flush();
  bus2.commitOffset('g', 'jobs.email', 3);
  // Swap the survivor's callback before the leave.
  bus2.setRebalanceCallback(survivor.id, (e) => survivorNew.push(e));
  leaver.unsubscribe();
  const leave = survivorNew.find((e) => e.trigger === 'leave' && e.memberId === leaver.id);
  assert.ok(leave !== undefined, 'the swapped callback hears the leave');
  assert.equal(leave.lingerUntil, now + 30_000, 'the armed window survives the swap');
  assert.ok(leave.lingering !== undefined && leave.lingering.length > 0);
  assert.equal(survivorOld.filter((e) => e.trigger === 'leave').length, 0, 'the old callback never fires again');
  const observerLeave = observer.find((e) => e.trigger === 'leave' && e.memberId === leaver.id);
  assert.ok(observerLeave !== undefined);
  assert.equal(observerLeave.lingerUntil, now + 30_000, 'the observer sees the same window');
  survivor.unsubscribe();
});

test('the EB-209 partition listener is unaffected by a callback revision', () => {
  const bus = new EventBus();
  interface ListenerCall {
    type: 'assigned' | 'revoked';
    member: string;
    partitions: number[];
  }
  const logA: ListenerCall[] = [];
  const logB: ListenerCall[] = [];
  const mkListener = (log: ListenerCall[]): ConsumerRebalanceListener => ({
    onPartitionsAssigned: (member, partitions) => {
      log.push({ type: 'assigned', member, partitions: [...partitions] });
    },
    onPartitionsRevoked: (member, partitions) => {
      log.push({ type: 'revoked', member, partitions: [...partitions] });
    },
  });
  const oldEvents: GroupRebalanceEvent[] = [];
  const newEvents: GroupRebalanceEvent[] = [];
  const mA = bus.subscribeToGroup('g', 'pt', noop, {
    partitions: 4,
    rebalanceListener: mkListener(logA),
    onRebalance: (e) => oldEvents.push(e),
  });
  assert.equal(logA.length, 1, 'the first join assigns every partition to A');
  assert.equal(logA[0].type, 'assigned');
  assert.deepEqual([...logA[0].partitions].sort((x, y) => x - y), [0, 1, 2, 3]);

  // Swap A's group-level callback: the member-level listener must
  // keep working exactly as before.
  bus.setRebalanceCallback(mA.id, (e) => newEvents.push(e));
  const mB = bus.subscribeToGroup('g', 'pt', noop, {
    partitions: 4,
    rebalanceListener: mkListener(logB),
  });
  const after = bus.getPartitionAssignment('g', 'pt');
  const bOwned = Object.entries(after)
    .filter(([, owner]) => owner === mB.id)
    .map(([p]) => Number(p))
    .sort((x, y) => x - y);
  assert.equal(logB.length, 1);
  assert.equal(logB[0].type, 'assigned');
  assert.deepEqual([...logB[0].partitions].sort((x, y) => x - y), bOwned);
  const aRevoked = logA.filter((c) => c.type === 'revoked');
  assert.equal(aRevoked.length, 1, 'A still hears the revoke through its listener');
  assert.deepEqual([...aRevoked[0].partitions].sort((x, y) => x - y), bOwned);
  assert.equal(oldEvents.length, 1, 'the old group-level callback never fires again');
  assert.equal(newEvents.length, 1, 'the new group-level callback hears the join');
  assert.equal(newEvents[0].trigger, 'join');
  assert.equal(newEvents[0].memberId, mB.id);
  assert.ok(newEvents[0].partitionRebalance !== undefined, 'the assignment still rides along');
  mA.unsubscribe();
  mB.unsubscribe();
});
