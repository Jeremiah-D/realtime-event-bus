import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type ConsumerRebalanceListener,
  type GroupRebalanceEvent,
  type RebalanceCommitOffset,
} from '../src/bus.ts';

/**
 * Consumer-group member rebalance-listener revision (EB-444):
 * `reviseRebalanceListener` swaps a group member's
 * `onPartitionsAssigned` / `onPartitionsRevoked` without
 * re-subscribing — the member-level partition-assignment hook
 * (EB-209), distinct from the group-level roster notification
 * `onRebalance` (EB-16, revised live by `setRebalanceCallback`,
 * EB-439). Only the listener moves: the roster, the assignment,
 * the debounce window, the linger windows, and the group-level
 * callback are untouched, and a throwing listener is swallowed by
 * `fireRebalanceListener`, so observation never blocks the
 * rebalance.
 *
 * Join and leave rebalances are synchronous: a `subscribeToGroup`
 * or `unsubscribe` call has already notified the survivors by the
 * time it returns (as in `rebalance-listener.test.ts`).
 */

const noop = (): void => {};

/** The `rebalance_listener_callbacks_revised` audit events on the bus. */
function listenerRevisionAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'rebalance_listener_callbacks_revised' });
}

/** The `rebalance` audit events on the bus. */
function rebalanceAudits(bus: EventBus) {
  return bus.getSubscriptionAudit({ action: 'rebalance' });
}

interface ListenerCall {
  type: 'assigned' | 'revoked';
  member: string;
  partitions: number[];
}

function recordingListener(log: ListenerCall[]): ConsumerRebalanceListener {
  return {
    onPartitionsAssigned: (member, partitions) => {
      log.push({ type: 'assigned', member, partitions: [...partitions] });
    },
    onPartitionsRevoked: (member, partitions) => {
      log.push({ type: 'revoked', member, partitions: [...partitions] });
    },
  };
}

/** Partitions of `after` owned by `memberId`. */
function ownedPartitions(after: Record<number, string>, memberId: string): number[] {
  return Object.entries(after)
    .filter(([, owner]) => owner === memberId)
    .map(([p]) => Number(p))
    .sort((a, b) => a - b);
}

test('swapping the listener routes the next revoke and assign to the new callbacks only', () => {
  const bus = new EventBus();
  const oldLog: ListenerCall[] = [];
  const newLog: ListenerCall[] = [];
  const committed: RebalanceCommitOffset[][] = [];
  const mA = bus.subscribeToGroup('g', 'pt', noop, {
    partitions: 4,
    rebalanceListener: recordingListener(oldLog),
  });
  assert.equal(oldLog.length, 1, 'the first join assigns every partition to A');
  assert.deepEqual([...oldLog[0].partitions].sort((a, b) => a - b), [0, 1, 2, 3]);

  // Swap A's listener: the next rebalance reaches the new callbacks only.
  // The new revoke hook still checkpoints through commitOffsetsSync.
  bus.reviseRebalanceListener(mA.id, {
    onPartitionsAssigned: (member, partitions) => {
      newLog.push({ type: 'assigned', member, partitions: [...partitions] });
    },
    onPartitionsRevoked: (member, partitions, ctx) => {
      newLog.push({ type: 'revoked', member, partitions: [...partitions] });
      const offsets = partitions.map((p) => ({ topic: 'pt', seq: 7, partition: p }));
      ctx.commitOffsetsSync(offsets);
      committed.push(offsets);
    },
  });
  const mB = bus.subscribeToGroup('g', 'pt', noop, { partitions: 4 });
  const afterJoin = bus.getPartitionAssignment('g', 'pt');
  const bOwned = ownedPartitions(afterJoin, mB.id);
  assert.ok(bOwned.length > 0, 'B must own partitions after joining');
  // A's revoke fired on the new callback with exactly the partitions B took.
  const revoked = newLog.filter((c) => c.type === 'revoked');
  assert.equal(revoked.length, 1);
  assert.equal(revoked[0].member, mA.id);
  assert.deepEqual([...revoked[0].partitions].sort((a, b) => a - b), bOwned);
  assert.equal(oldLog.length, 1, 'the old listener never fires again');
  // The revoke hook checkpointed through the commitOffset path.
  assert.equal(committed.length, 1);
  const pcommitted = bus.getPartitionCommittedOffsets('g', 'pt');
  for (const p of bOwned) {
    assert.equal(pcommitted[p]?.['pt'], 7, `partition ${p} must be checkpointed at seq 7`);
  }

  // B leaves: A regains the partitions through the new assigned callback.
  mB.unsubscribe();
  const assigned = newLog.filter((c) => c.type === 'assigned');
  assert.equal(assigned.length, 1);
  assert.equal(assigned[0].member, mA.id);
  assert.deepEqual([...assigned[0].partitions].sort((a, b) => a - b), bOwned);
  assert.equal(oldLog.length, 1, 'the old listener still never fires');
  assert.deepEqual(listenerRevisionAudits(bus)[0].detail, {
    onPartitionsAssigned: 'set',
    oldOnPartitionsAssigned: 'set',
    onPartitionsRevoked: 'set',
    oldOnPartitionsRevoked: 'set',
  });
  mA.unsubscribe();
});

test('clearing one callback leaves the other one live', () => {
  const bus = new EventBus();
  const log: ListenerCall[] = [];
  const mA = bus.subscribeToGroup('g', 'pt', noop, {
    partitions: 4,
    rebalanceListener: recordingListener(log),
  });
  assert.equal(log.length, 1);

  // Clear only the assigned callback: the revoke callback keeps firing.
  bus.reviseRebalanceListener(mA.id, { onPartitionsAssigned: null });
  const mB = bus.subscribeToGroup('g', 'pt', noop, { partitions: 4 });
  const afterJoin = bus.getPartitionAssignment('g', 'pt');
  const bOwned = ownedPartitions(afterJoin, mB.id);
  const revoked = log.filter((c) => c.type === 'revoked');
  assert.equal(revoked.length, 1, 'the surviving revoked callback still fires');
  assert.deepEqual([...revoked[0].partitions].sort((a, b) => a - b), bOwned);
  assert.equal(log.filter((c) => c.type === 'assigned').length, 1, 'the cleared assigned callback never fires');
  assert.deepEqual(listenerRevisionAudits(bus)[0].detail, {
    onPartitionsAssigned: 'cleared',
    oldOnPartitionsAssigned: 'set',
    onPartitionsRevoked: 'unchanged',
    oldOnPartitionsRevoked: 'set',
  });

  // Now clear only the revoked callback: the assigned callback (already
  // cleared) stays silent, and the next assign fires nowhere — while
  // the rebalance itself still happens.
  bus.reviseRebalanceListener(mA.id, { onPartitionsRevoked: null });
  const auditsBefore = rebalanceAudits(bus).length;
  mB.unsubscribe();
  assert.equal(log.length, 2, 'no listener callback fires after both are cleared');
  assert.equal(rebalanceAudits(bus).length, auditsBefore + 1, 'the rebalance still happens and is audited');
  assert.deepEqual(listenerRevisionAudits(bus)[1].detail, {
    onPartitionsAssigned: 'unchanged',
    oldOnPartitionsAssigned: 'absent',
    onPartitionsRevoked: 'cleared',
    oldOnPartitionsRevoked: 'set',
  });
  mA.unsubscribe();
});

test('clearing both callbacks leaves the rebalance running and audited', () => {
  const bus = new EventBus();
  const log: ListenerCall[] = [];
  const mA = bus.subscribeToGroup('g', 'pt', noop, {
    partitions: 4,
    rebalanceListener: recordingListener(log),
  });
  assert.equal(log.length, 1);
  bus.reviseRebalanceListener(mA.id, { onPartitionsAssigned: null, onPartitionsRevoked: null });
  const auditsBefore = rebalanceAudits(bus).length;
  const mB = bus.subscribeToGroup('g', 'pt', noop, { partitions: 4 });
  assert.equal(log.length, 1, 'no listener callback fires after a full clear');
  assert.equal(rebalanceAudits(bus).length, auditsBefore + 1, 'the rebalance still happens and is audited');
  const after = bus.getPartitionAssignment('g', 'pt');
  assert.ok(ownedPartitions(after, mB.id).length > 0, 'the assignment still moved');
  assert.deepEqual(listenerRevisionAudits(bus)[0].detail, {
    onPartitionsAssigned: 'cleared',
    oldOnPartitionsAssigned: 'set',
    onPartitionsRevoked: 'cleared',
    oldOnPartitionsRevoked: 'set',
  });
  mA.unsubscribe();
  mB.unsubscribe();
});

test('reviseRebalanceListener rejects unknown, empty, non-member, and invalid revisions fail-closed', () => {
  const bus = new EventBus();
  const log: ListenerCall[] = [];
  const mA = bus.subscribeToGroup('g', 'pt', noop, {
    partitions: 4,
    rebalanceListener: recordingListener(log),
  });
  const plain = bus.subscribe('t2', () => {});
  const replacement = (): void => {};
  for (const bad of [
    () => bus.reviseRebalanceListener('no-such-sub', { onPartitionsAssigned: replacement }),
    () => bus.reviseRebalanceListener('', { onPartitionsAssigned: replacement }),
    () => bus.reviseRebalanceListener(123 as never, { onPartitionsAssigned: replacement }),
    () => bus.reviseRebalanceListener(plain.id, { onPartitionsAssigned: replacement }),
    () => bus.reviseRebalanceListener(mA.id, 'nope' as never),
    () => bus.reviseRebalanceListener(mA.id, null as never),
    () => bus.reviseRebalanceListener(mA.id, [] as never),
    () => bus.reviseRebalanceListener(mA.id, {}),
    () => bus.reviseRebalanceListener(mA.id, { onPartitionsAssigned: 42 as never }),
    () => bus.reviseRebalanceListener(mA.id, { onPartitionsRevoked: 'nope' as never }),
    () => bus.reviseRebalanceListener(mA.id, { onPartitionsAssigned: undefined }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    listenerRevisionAudits(bus).length,
    0,
    'failed revisions must not audit',
  );
  // The live listener is untouched: the original callbacks still fire
  // on the next join.
  const mB = bus.subscribeToGroup('g', 'pt', noop, { partitions: 4 });
  const after = bus.getPartitionAssignment('g', 'pt');
  const bOwned = ownedPartitions(after, mB.id);
  const revoked = log.filter((c) => c.type === 'revoked');
  assert.equal(revoked.length, 1);
  assert.deepEqual([...revoked[0].partitions].sort((a, b) => a - b), bOwned);
  mA.unsubscribe();
  mB.unsubscribe();
  plain.unsubscribe();
});

test('a revision identical to the live listener is a no-op with no audit', () => {
  const bus = new EventBus();
  const assigned = (): void => {};
  const revoked = (): void => {};
  const sub = bus.subscribeToGroup('g', 'pt', noop, { partitions: 2 });
  // Clearing callbacks that were never set is a no-op, not a clear.
  bus.reviseRebalanceListener(sub.id, { onPartitionsAssigned: null, onPartitionsRevoked: null });
  assert.equal(
    listenerRevisionAudits(bus).length,
    0,
    'clearing absent callbacks must not audit',
  );

  bus.reviseRebalanceListener(sub.id, { onPartitionsAssigned: assigned });
  let audit = listenerRevisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    onPartitionsAssigned: 'set',
    oldOnPartitionsAssigned: 'absent',
    onPartitionsRevoked: 'unchanged',
    oldOnPartitionsRevoked: 'absent',
  });

  // Re-affirming the same function reference is a no-op.
  bus.reviseRebalanceListener(sub.id, { onPartitionsAssigned: assigned });
  audit = listenerRevisionAudits(bus);
  assert.equal(audit.length, 1, 'a same-value revision writes no second event');

  // Adding the second callback audits only its own change.
  bus.reviseRebalanceListener(sub.id, { onPartitionsRevoked: revoked });
  audit = listenerRevisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    onPartitionsAssigned: 'unchanged',
    oldOnPartitionsAssigned: 'set',
    onPartitionsRevoked: 'set',
    oldOnPartitionsRevoked: 'absent',
  });
  sub.unsubscribe();
});

test('a throwing revised listener is swallowed and counted, and the rebalance is unaffected', () => {
  const bus = new EventBus();
  const rebalanceEvents: GroupRebalanceEvent[] = [];
  const mA = bus.subscribeToGroup('g', 'pt', noop, {
    partitions: 2,
    onRebalance: (e) => rebalanceEvents.push(e),
  });
  // Swap in a throwing listener via revision: B's join must still
  // assign partitions and still notify the group.
  bus.reviseRebalanceListener(mA.id, {
    onPartitionsRevoked: () => {
      throw new Error('boom');
    },
  });
  const errorsBefore = bus.getStats().rebalanceListenerErrors;
  const auditsBefore = rebalanceAudits(bus).length;
  const mB = bus.subscribeToGroup('g', 'pt', noop, { partitions: 2 });
  const after = bus.getPartitionAssignment('g', 'pt');
  assert.ok(ownedPartitions(after, mB.id).length > 0, 'B must own partitions after the rebalance');
  assert.ok(
    rebalanceEvents.some((e) => e.trigger === 'join' && e.memberId === mB.id),
    'onRebalance must still fire when the listener throws',
  );
  assert.equal(bus.getStats().rebalanceListenerErrors, errorsBefore + 1);
  assert.equal(rebalanceAudits(bus).length, auditsBefore + 1, 'the rebalance is still audited');
  mA.unsubscribe();
  mB.unsubscribe();
});

test('the swap audits exact presence flags with the subscriber', () => {
  const bus = new EventBus();
  const first = (): void => {};
  const second = (): void => {};
  const sub = bus.subscribeToGroup('g', 'pt', noop, { partitions: 2 });
  bus.reviseRebalanceListener(sub.id, { onPartitionsAssigned: first });
  bus.reviseRebalanceListener(sub.id, { onPartitionsAssigned: second, onPartitionsRevoked: first });
  bus.reviseRebalanceListener(sub.id, { onPartitionsAssigned: null, onPartitionsRevoked: null });
  const audit = listenerRevisionAudits(bus);
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, {
    onPartitionsAssigned: 'set',
    oldOnPartitionsAssigned: 'absent',
    onPartitionsRevoked: 'unchanged',
    oldOnPartitionsRevoked: 'absent',
  });
  assert.deepEqual(audit[1].detail, {
    onPartitionsAssigned: 'set',
    oldOnPartitionsAssigned: 'set',
    onPartitionsRevoked: 'set',
    oldOnPartitionsRevoked: 'absent',
  });
  assert.deepEqual(audit[2].detail, {
    onPartitionsAssigned: 'cleared',
    oldOnPartitionsAssigned: 'set',
    onPartitionsRevoked: 'cleared',
    oldOnPartitionsRevoked: 'set',
  });
  for (const entry of audit) {
    assert.equal(entry.subId, sub.id);
    assert.equal(entry.pattern, 'pt');
    assert.equal(entry.actor, 'system');
  }
  // The functions themselves are never recorded — only their presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
  sub.unsubscribe();
});

test('the EB-439 group-level onRebalance callback is unaffected by a listener revision', () => {
  const bus = new EventBus();
  const oldLog: ListenerCall[] = [];
  const newLog: ListenerCall[] = [];
  const groupEvents: GroupRebalanceEvent[] = [];
  const mA = bus.subscribeToGroup('g', 'pt', noop, {
    partitions: 4,
    rebalanceListener: recordingListener(oldLog),
    onRebalance: (e) => groupEvents.push(e),
  });
  assert.equal(oldLog.length, 1);
  assert.equal(groupEvents.length, 1);

  // Swap only the member-level listener: the group-level callback must
  // keep hearing every join and leave.
  bus.reviseRebalanceListener(mA.id, {
    onPartitionsAssigned: (member, partitions) => {
      newLog.push({ type: 'assigned', member, partitions: [...partitions] });
    },
  });
  const mB = bus.subscribeToGroup('g', 'pt', noop, { partitions: 4 });
  assert.equal(newLog.length, 0, 'A gained no partitions on B’s join');
  assert.equal(groupEvents.length, 2, 'the group-level callback still hears the join');
  assert.equal(groupEvents[1].trigger, 'join');
  assert.equal(groupEvents[1].memberId, mB.id);
  assert.ok(groupEvents[1].partitionRebalance !== undefined, 'the assignment still rides along');
  mB.unsubscribe();
  assert.equal(groupEvents.length, 3, 'the group-level callback still hears the leave');
  assert.equal(groupEvents[2].trigger, 'leave');
  assert.equal(newLog.length, 1, 'the revised listener hears the re-assign on the leave');
  mA.unsubscribe();
});

test('a closed bus refuses the revision', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeToGroup('g', 'pt', noop, { partitions: 2 });
  await bus.close();
  assert.throws(
    () => bus.reviseRebalanceListener(sub.id, { onPartitionsAssigned: () => {} }),
    RangeError,
  );
  assert.equal(listenerRevisionAudits(bus).length, 0, 'a refused revision must not audit');
});
