import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

describe('consumer-group static member session timeout live revision (EB-347)', () => {
  it('rejects an unknown group/pattern/instance and a dynamic member, fail-fast', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
    });
    const dyn = bus.subscribeToGroup('g', 't.*', () => {}, {});
    assert.throws(() => bus.setGroupSessionTimeout('', 't.*', 'i1', 100), RangeError);
    assert.throws(() => bus.setGroupSessionTimeout('g', '', 'i1', 100), RangeError);
    assert.throws(() => bus.setGroupSessionTimeout('g', 't.*', '', 100), RangeError);
    assert.throws(
      () => bus.setGroupSessionTimeout('g', 't.*', 'i1', 100, ''),
      RangeError,
    );
    // Unknown group, unknown pattern, unknown instance.
    assert.throws(() => bus.setGroupSessionTimeout('nope', 't.*', 'i1', 100), RangeError);
    assert.throws(() => bus.setGroupSessionTimeout('g', 'other.*', 'i1', 100), RangeError);
    assert.throws(() => bus.setGroupSessionTimeout('g', 't.*', 'ghost', 100), RangeError);
    // A dynamic member holds no stored timeout, so it cannot be revised.
    assert.throws(
      () => bus.setGroupSessionTimeout('g', 't.*', dyn.id, 100),
      RangeError,
    );
    // Nothing changed and nothing was audited.
    assert.equal(bus.getGroupSessionTimeout('g', 't.*', 'i1'), 5000);
    assert.equal(bus.getGroupSessionTimeout('g', 't.*', 'ghost'), undefined);
    assert.equal(bus.getGroupSessionTimeout('g', 't.*', dyn.id), undefined);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_session_timeout_revised' }).length,
      0,
    );
    m1.unsubscribe();
    dyn.unsubscribe();
  });

  it('rejects an illegal timeout and leaves the old value untouched', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
    });
    for (const timeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      assert.throws(
        () => bus.setGroupSessionTimeout('g', 't.*', 'i1', timeoutMs),
        RangeError,
        `timeoutMs=${timeoutMs}`,
      );
    }
    assert.throws(
      () => bus.setGroupSessionTimeout('g', 't.*', 'i1', '100' as never),
      RangeError,
    );
    // The live timeout and the audit log are untouched.
    assert.equal(bus.getGroupSessionTimeout('g', 't.*', 'i1'), 5000);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_session_timeout_revised' }).length,
      0,
    );
    m1.unsubscribe();
  });

  it('revising to the current value is a no-op with no audit', () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
      onRebalance: (e) => events.push(e),
    });
    events.length = 0;
    bus.setGroupSessionTimeout('g', 't.*', 'i1', 5000);
    assert.equal(events.length, 0);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_session_timeout_revised' }).length,
      0,
    );
    assert.equal(bus.getGroupSessionTimeout('g', 't.*', 'i1'), 5000);
    m1.unsubscribe();
  });

  it('revision is audited with the old and new timeout and the operator', () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't.*', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
    });
    bus.setGroupSessionTimeout('g', 't.*', 'i1', 8000, 'ops');
    assert.equal(bus.getGroupSessionTimeout('g', 't.*', 'i1'), 8000);
    const audit = bus.getSubscriptionAudit({ action: 'group_session_timeout_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].groupId, 'g');
    assert.equal(audit[0].pattern, 't.*');
    assert.equal(audit[0].actor, 'ops');
    assert.deepEqual(audit[0].detail, {
      instanceId: 'i1',
      previous_timeout_ms: 5000,
      timeout_ms: 8000,
    });
    m1.unsubscribe();
  });

  it('revision fires no rebalance and an active member takes the new timeout on its next leave', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
    events.length = 0;

    bus.setGroupSessionTimeout('g', 't', 'i1', 60);
    assert.equal(events.length, 0, 'the revision itself must not rebalance');
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 60);

    m1.unsubscribe();
    await flush();
    assert.equal(events.length, 0, 'no rebalance while the slot is held');
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);

    // The old timeout was 5000ms: had the revision not applied, the slot
    // would still be held after 150ms. The revised 60ms window expires it.
    await sleep(150);
    const leaves = events.filter((e) => e.trigger === 'leave');
    assert.equal(leaves.length, 1);
    assert.equal(leaves[0].memberId, 'i1');
    assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id]);
    m2.unsubscribe();
  });

  it('a lingering session is re-armed from the revision moment at the new timeout', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
    events.length = 0;

    m1.unsubscribe();
    await flush();
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);

    // Re-arm mid-limbo: the new 250ms window runs from here, not from
    // the leave — 120ms in the slot must still be held.
    await sleep(30);
    bus.setGroupSessionTimeout('g', 't', 'i1', 250);
    assert.equal(events.length, 0, 're-arming must not rebalance');
    await sleep(120);
    assert.deepEqual(
      bus.getGroupMembers('g', 't'),
      ['i1', m2.id],
      'slot must still be held before the revised window elapses',
    );

    // Past the revised window (30 + 120 + 250 = 400ms after the
    // revision, 150ms past the 250ms deadline) the slot is released —
    // far sooner than the original 5000ms window would have allowed.
    await sleep(250);
    const leaves = events.filter((e) => e.trigger === 'leave');
    assert.equal(leaves.length, 1);
    assert.equal(leaves[0].memberId, 'i1');
    assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id]);
    // The member is gone: its stored timeout is gone with it.
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), undefined);
    assert.throws(() => bus.setGroupSessionTimeout('g', 't', 'i1', 100), RangeError);
    m2.unsubscribe();
  });

  it('tightening a lingering session expires the slot at the new deadline', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });

    m1.unsubscribe();
    await flush();
    await sleep(30);
    bus.setGroupSessionTimeout('g', 't', 'i1', 60);
    await sleep(150);
    const leaves = events.filter((e) => e.trigger === 'leave');
    assert.equal(leaves.length, 1);
    assert.equal(leaves[0].memberId, 'i1');
    assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id]);
    m2.unsubscribe();
  });

  it('a rejoin keeps the revised timeout and it governs the next leave', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
    events.length = 0;

    bus.setGroupSessionTimeout('g', 't', 'i1', 300);
    m1.unsubscribe();
    await flush();
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);

    // Rejoin inside the window without passing sessionTimeoutMs: the
    // revised value survives — no rebalance, same roster.
    const m1b = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      onRebalance,
    });
    await flush();
    assert.equal(events.length, 0, 'rejoin inside the window must not rebalance');
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 300);

    // The next leave arms the expiry at the revised 300ms, not the
    // original 5000ms.
    m1b.unsubscribe();
    await flush();
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);
    await sleep(450);
    const leaves = events.filter((e) => e.trigger === 'leave');
    assert.equal(leaves.length, 1);
    assert.equal(leaves[0].memberId, 'i1');
    assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id]);
    m2.unsubscribe();
  });

  it('an explicit sessionTimeoutMs at rejoin still wins over the revised value', async () => {
    const bus = new EventBus();
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
    });
    bus.subscribeToGroup('g', 't', () => {}, {});
    bus.setGroupSessionTimeout('g', 't', 'i1', 300);
    m1.unsubscribe();
    await flush();
    const m1b = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 9000,
    });
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 9000);
    m1b.unsubscribe();
  });
});
