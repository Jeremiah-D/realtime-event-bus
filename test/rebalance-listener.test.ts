import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type ConsumerRebalanceListener,
  type RebalanceCommitOffset,
} from '../src/bus.ts';

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

describe('consumer rebalance listener (EB-209)', () => {
  it('fires assigned on the newcomer and revoked on the members that lost partitions', () => {
    const bus = new EventBus();
    const logA: ListenerCall[] = [];
    const logB: ListenerCall[] = [];
    const mA = bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 4,
      rebalanceListener: recordingListener(logA),
    });
    // The first join assigns every partition to A — the listener fires
    // with the full set.
    assert.equal(logA.length, 1);
    assert.equal(logA[0].type, 'assigned');
    assert.equal(logA[0].member, mA.id);
    assert.deepEqual([...logA[0].partitions].sort((a, b) => a - b), [0, 1, 2, 3]);

    const mB = bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 4,
      rebalanceListener: recordingListener(logB),
    });
    const after = bus.getPartitionAssignment('g', 'pt');
    const bOwned = ownedPartitions(after, mB.id);
    // B's assigned callback names exactly the partitions B now owns.
    assert.equal(logB.length, 1);
    assert.equal(logB[0].type, 'assigned');
    assert.equal(logB[0].member, mB.id);
    assert.deepEqual([...logB[0].partitions].sort((a, b) => a - b), bOwned);
    // A's revoked callback names exactly the partitions A lost to B.
    const aRevoked = logA.filter((c) => c.type === 'revoked');
    assert.equal(aRevoked.length, 1);
    assert.equal(aRevoked[0].member, mA.id);
    assert.deepEqual([...aRevoked[0].partitions].sort((a, b) => a - b), bOwned);
    mA.unsubscribe();
    mB.unsubscribe();
  });

  it('revoke fires before assign for a member that both loses and gains partitions', () => {
    const bus = new EventBus();
    const log: Array<ListenerCall & { sub: string }> = [];
    const mk = (): ConsumerRebalanceListener => ({
      onPartitionsAssigned: (member, partitions) => {
        log.push({ type: 'assigned', member, partitions, sub: 'x' });
      },
      onPartitionsRevoked: (member, partitions) => {
        log.push({ type: 'revoked', member, partitions, sub: 'x' });
      },
    });
    const subs: Array<{ unsubscribe: () => void }> = [];
    for (let i = 0; i < 4; i += 1) {
      const mark = log.length;
      const s = bus.subscribeToGroup('g', 'pt', () => {}, { partitions: 6, rebalanceListener: mk() });
      subs.push(s);
      // Within this single rebalance emission, every member's revoked
      // callbacks precede its assigned callbacks — the Kafka
      // revoke-before-assign order.
      const slice = log.slice(mark);
      const byMember = new Map<string, string[]>();
      for (const c of slice) {
        const kinds = byMember.get(c.member) ?? [];
        kinds.push(c.type);
        byMember.set(c.member, kinds);
      }
      for (const [member, kinds] of byMember) {
        const firstAssigned = kinds.indexOf('assigned');
        const lastRevoked = kinds.lastIndexOf('revoked');
        assert.ok(
          lastRevoked === -1 || firstAssigned === -1 || lastRevoked < firstAssigned,
          `member ${member}: revoke must precede assign, got ${kinds.join(',')}`,
        );
      }
    }
    for (const s of subs) s.unsubscribe();
  });

  it('the revoke hook commitOffsetsSync checkpoints through the commitOffset path', () => {
    const bus = new EventBus();
    const committed: RebalanceCommitOffset[][] = [];
    const mA = bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 2,
      rebalanceListener: {
        onPartitionsRevoked: (member, partitions, ctx) => {
          const offsets = partitions.map((p) => ({ topic: 'pt', seq: 7, partition: p }));
          ctx.commitOffsetsSync(offsets);
          committed.push(offsets);
          assert.equal(member, mA.id);
        },
      },
    });
    const mB = bus.subscribeToGroup('g', 'pt', () => {}, { partitions: 2 });
    // The hook ran during B's join rebalance for the partitions A lost.
    assert.ok(committed.length > 0, 'revoke hook must have fired for A');
    const revokedPartitions = committed[0].map((o) => o.partition).sort();
    const after = bus.getPartitionAssignment('g', 'pt');
    assert.deepEqual(revokedPartitions, ownedPartitions(after, mB.id));
    // The per-partition checkpoints the hook wrote are readable through
    // the public API — the hook really went through commitOffset.
    const pcommitted = bus.getPartitionCommittedOffsets('g', 'pt');
    for (const p of revokedPartitions) {
      assert.equal(pcommitted[p]?.['pt'], 7, `partition ${p} must be checkpointed at seq 7`);
    }
    // Group-level checkpoint is readable through the public API.
    bus.commitOffset('g', 'pt', 9);
    assert.equal(bus.getCommittedOffsets('g')['pt'], 9);
    mA.unsubscribe();
    mB.unsubscribe();
  });

  it('commitOffsetsSync rejects invalid input like commitOffset', () => {
    const bus = new EventBus();
    let hook: ((offsets: RebalanceCommitOffset[]) => void) | undefined;
    const mA = bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 2,
      rebalanceListener: {
        onPartitionsRevoked: (_member, _partitions, ctx) => {
          hook = ctx.commitOffsetsSync;
        },
      },
    });
    bus.subscribeToGroup('g', 'pt', () => {}, { partitions: 2 });
    assert.ok(hook !== undefined, 'revoke hook must have fired');
    const h = hook as (offsets: RebalanceCommitOffset[]) => void;
    // Out-of-range partition goes through commitOffset validation.
    assert.throws(() => h([{ topic: 'pt', seq: 1, partition: 99 }]), RangeError);
    // Bad seq likewise.
    assert.throws(() => h([{ topic: 'pt', seq: 0 }]), RangeError);
    mA.unsubscribe();
  });

  it('a throwing listener never blocks the rebalance and is counted', () => {
    const bus = new EventBus();
    const rebalanceEvents: string[] = [];
    bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 2,
      rebalanceListener: {
        onPartitionsRevoked: () => {
          throw new Error('boom');
        },
      },
      onRebalance: (e) => rebalanceEvents.push(`${e.trigger}:${e.memberId}`),
    });
    const mB = bus.subscribeToGroup('g', 'pt', () => {}, { partitions: 2 });
    // The rebalance completed: B owns partitions and the group-level
    // notification still fired.
    const after = bus.getPartitionAssignment('g', 'pt');
    assert.ok(ownedPartitions(after, mB.id).length > 0, 'B must own partitions after the rebalance');
    assert.ok(
      rebalanceEvents.some((e) => e.startsWith('join:')),
      'onRebalance must still fire when the listener throws',
    );
    assert.equal(bus.getStats().rebalanceListenerErrors, 1);
  });

  it('the listener never fires for round-robin groups', () => {
    const bus = new EventBus();
    const log: ListenerCall[] = [];
    const mA = bus.subscribeToGroup('g', 'pt', () => {}, {
      rebalanceListener: recordingListener(log),
    });
    const mB = bus.subscribeToGroup('g', 'pt', () => {}, {
      rebalanceListener: recordingListener(log),
    });
    assert.equal(log.length, 0);
    assert.equal(bus.getStats().rebalanceListenerErrors, 0);
    mA.unsubscribe();
    mB.unsubscribe();
    assert.equal(log.length, 0);
  });

  it('a leave reassigns the partitions and notifies the remaining member', () => {
    const bus = new EventBus();
    const logA: ListenerCall[] = [];
    const logB: ListenerCall[] = [];
    const mA = bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 3,
      rebalanceListener: recordingListener(logA),
    });
    const mB = bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 3,
      rebalanceListener: recordingListener(logB),
    });
    const bCalls = logB.length;
    const before = bus.getPartitionAssignment('g', 'pt');
    const bOwnedBefore = ownedPartitions(before, mB.id);
    assert.ok(bOwnedBefore.length > 0, 'B must own some partitions before leaving');
    const aMark = logA.length;
    mB.unsubscribe();
    // The leaver's own listener is gone — only A observes the leave.
    assert.equal(logB.length, bCalls, 'the departed member gets no listener callback');
    const after = bus.getPartitionAssignment('g', 'pt');
    const aAssigned = logA
      .slice(aMark)
      .filter((c) => c.type === 'assigned')
      .flatMap((c) => c.partitions)
      .sort((a, b) => a - b);
    // A is assigned exactly the partitions B released.
    assert.deepEqual(aAssigned, bOwnedBefore);
    for (const p of aAssigned) {
      assert.equal(after[p], mA.id, `partition ${p} must now belong to A`);
    }
    mA.unsubscribe();
  });

  it('invalid rebalanceListener options fail fast at registration', () => {
    const bus = new EventBus();
    assert.throws(
      () => bus.subscribeToGroup('g', 'pt', () => {}, { rebalanceListener: 'x' as never }),
      RangeError,
    );
    assert.throws(
      () => bus.subscribeToGroup('g', 'pt', () => {}, { rebalanceListener: null as never }),
      RangeError,
    );
    assert.throws(
      () =>
        bus.subscribeToGroup('g', 'pt', () => {}, {
          rebalanceListener: { onPartitionsAssigned: 42 as never },
        }),
      RangeError,
    );
    assert.throws(
      () =>
        bus.subscribeToGroup('g', 'pt', () => {}, {
          rebalanceListener: { onPartitionsRevoked: 'nope' as never },
        }),
      RangeError,
    );
    // An empty listener object is fine — it just never fires.
    const s = bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 2,
      rebalanceListener: {},
    });
    s.unsubscribe();
  });

  it('a static member is addressed by its instanceId', () => {
    const bus = new EventBus();
    const log: ListenerCall[] = [];
    const mA = bus.subscribeToGroup('g', 'pt', () => {}, {
      partitions: 2,
      instanceId: 'static-a',
      rebalanceListener: recordingListener(log),
    });
    bus.subscribeToGroup('g', 'pt', () => {}, { partitions: 2 });
    const members = log.map((c) => c.member);
    assert.ok(members.includes('static-a'), `expected instanceId in ${members.join(',')}`);
    assert.ok(!members.includes(mA.id), 'the subscription id must not be used for static members');
    mA.unsubscribe();
  });
});
