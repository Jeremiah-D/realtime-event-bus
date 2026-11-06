import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

describe('consumer-group static session-timeout group default live revision (EB-403)', () => {
  it('a group default governs a new member without its own timeout; no default keeps the 30s module value', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    bus.setGroupDefaultSessionTimeout('g', 't', 60);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
    // No default for 'h': the member falls back to the 30s module default.
    const n1 = bus.subscribeToGroup('h', 't', () => {}, {
      instanceId: 'j1',
      onRebalance,
    });
    const n2 = bus.subscribeToGroup('h', 't', () => {}, { onRebalance });
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 60);
    assert.equal(bus.getGroupSessionTimeout('h', 't', 'j1'), 30_000);

    m1.unsubscribe();
    n1.unsubscribe();
    await flush();
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);
    assert.deepEqual(bus.getGroupMembers('h', 't'), ['j1', n2.id]);

    // The 60ms group default expires the slot; the 30s module default
    // still holds its slot after 150ms.
    await sleep(150);
    const leaves = events.filter((e) => e.trigger === 'leave');
    assert.equal(leaves.length, 1);
    assert.equal(leaves[0].memberId, 'i1');
    assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id]);
    assert.deepEqual(bus.getGroupMembers('h', 't'), ['j1', n2.id]);
    m2.unsubscribe();
    n2.unsubscribe();
  });

  it('a member-explicit sessionTimeoutMs wins over the group default in both directions', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    bus.setGroupDefaultSessionTimeout('g', 't', 60);
    // Explicit 5000 beats a 60ms default: the slot is still held.
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      sessionTimeoutMs: 5000,
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
    m1.unsubscribe();
    await flush();
    await sleep(150);
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);

    bus.setGroupDefaultSessionTimeout('h', 't', 5000);
    // Explicit 60 beats a 5000ms default: the slot expires.
    const n1 = bus.subscribeToGroup('h', 't', () => {}, {
      instanceId: 'j1',
      sessionTimeoutMs: 60,
      onRebalance,
    });
    const n2 = bus.subscribeToGroup('h', 't', () => {}, { onRebalance });
    n1.unsubscribe();
    await flush();
    await sleep(150);
    const leaves = events.filter((e) => e.trigger === 'leave');
    assert.equal(leaves.length, 1);
    assert.equal(leaves[0].memberId, 'j1');
    assert.deepEqual(bus.getGroupMembers('h', 't'), [n2.id]);
    m2.unsubscribe();
    n2.unsubscribe();
  });

  it('an EB-347 per-instance revised value wins over the group default and survives rejoin', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    bus.setGroupDefaultSessionTimeout('g', 't', 5000);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 5000);
    events.length = 0;
    // A per-instance revision beats the group default.
    bus.setGroupSessionTimeout('g', 't', 'i1', 300);
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 300);
    // Tightening the group default afterwards must not move the
    // per-instance value: a rejoin keeps 300.
    bus.setGroupDefaultSessionTimeout('g', 't', 60);
    m1.unsubscribe();
    await flush();
    const m1b = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      onRebalance,
    });
    await flush();
    assert.equal(events.length, 0, 'rejoin inside the window must not rebalance');
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 300);
    m1b.unsubscribe();
    await flush();
    await sleep(450);
    const leaves = events.filter((e) => e.trigger === 'leave');
    assert.equal(leaves.length, 1);
    assert.equal(leaves[0].memberId, 'i1');
    m2.unsubscribe();
  });

  it('a rejoin after expiry without an explicit value falls back to the group default, not 30s', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    bus.setGroupDefaultSessionTimeout('g', 't', 60);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
    m1.unsubscribe();
    await flush();
    await sleep(150);
    assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id]);
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), undefined);

    // Fresh join after expiry: the stored value is gone, so the group
    // default — not the 30s module default — is baked in again.
    const m1b = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      onRebalance,
    });
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 60);
    m1b.unsubscribe();
    await flush();
    await sleep(150);
    const leaves = events.filter((e) => e.trigger === 'leave');
    assert.equal(leaves.length, 2);
    assert.deepEqual(bus.getGroupMembers('g', 't'), [m2.id]);
    m2.unsubscribe();
  });

  it('clear restores the 30s module default, and clearing a never-set default is a false no-op', async () => {
    const bus = new EventBus();
    bus.setGroupDefaultSessionTimeout('g', 't', 60);
    assert.equal(bus.clearGroupDefaultSessionTimeout('g', 't'), true);
    assert.equal(bus.getGroupDefaultSessionTimeout('g', 't'), undefined);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, { instanceId: 'i1' });
    assert.equal(bus.getGroupSessionTimeout('g', 't', 'i1'), 30_000);
    const m2 = bus.subscribeToGroup('g', 't', () => {}, {});
    m1.unsubscribe();
    await flush();
    await sleep(150);
    assert.deepEqual(
      bus.getGroupMembers('g', 't'),
      ['i1', m2.id],
      'the slot must still be held under the restored 30s default',
    );

    // Clearing again: nothing stored, no audit.
    assert.equal(bus.clearGroupDefaultSessionTimeout('g', 't'), false);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_default_session_timeout_cleared' }).length,
      1,
    );
    m2.unsubscribe();
  });

  it('an illegal timeout throws RangeError and leaves the live default untouched', () => {
    const bus = new EventBus();
    bus.setGroupDefaultSessionTimeout('g', 't', 5000);
    for (const timeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      assert.throws(
        () => bus.setGroupDefaultSessionTimeout('g', 't', timeoutMs),
        RangeError,
        `timeoutMs=${timeoutMs}`,
      );
    }
    assert.throws(
      () => bus.setGroupDefaultSessionTimeout('g', 't', '100' as never),
      RangeError,
    );
    assert.equal(bus.getGroupDefaultSessionTimeout('g', 't'), 5000);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_default_session_timeout_revised' }).length,
      1,
    );
  });

  it('an empty groupId or pattern throws RangeError', () => {
    const bus = new EventBus();
    assert.throws(() => bus.setGroupDefaultSessionTimeout('', 't', 100), RangeError);
    assert.throws(() => bus.setGroupDefaultSessionTimeout('g', '', 100), RangeError);
    assert.throws(() => bus.setGroupDefaultSessionTimeout('g', 't', 100, ''), RangeError);
    assert.throws(() => bus.getGroupDefaultSessionTimeout('', 't'), RangeError);
    assert.throws(() => bus.getGroupDefaultSessionTimeout('g', ''), RangeError);
    assert.throws(() => bus.clearGroupDefaultSessionTimeout('', 't'), RangeError);
    assert.throws(() => bus.clearGroupDefaultSessionTimeout('g', ''), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_default_session_timeout_revised' }).length,
      0,
    );
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_default_session_timeout_cleared' }).length,
      0,
    );
  });

  it('revising to the current default is a no-op with no audit', () => {
    const bus = new EventBus();
    bus.setGroupDefaultSessionTimeout('g', 't', 5000);
    bus.setGroupDefaultSessionTimeout('g', 't', 5000);
    const audit = bus.getSubscriptionAudit({ action: 'group_default_session_timeout_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, { previous_timeout_ms: null, timeout_ms: 5000 });
  });

  it('revision is audited with the old and new timeout and the operator', () => {
    const bus = new EventBus();
    bus.setGroupDefaultSessionTimeout('g', 't.*', 5000, 'ops');
    bus.setGroupDefaultSessionTimeout('g', 't.*', 8000, 'ops');
    const revised = bus.getSubscriptionAudit({ action: 'group_default_session_timeout_revised' });
    assert.equal(revised.length, 2);
    assert.equal(revised[0].groupId, 'g');
    assert.equal(revised[0].pattern, 't.*');
    assert.equal(revised[0].actor, 'ops');
    assert.deepEqual(revised[0].detail, { previous_timeout_ms: null, timeout_ms: 5000 });
    assert.deepEqual(revised[1].detail, { previous_timeout_ms: 5000, timeout_ms: 8000 });

    assert.equal(bus.clearGroupDefaultSessionTimeout('g', 't.*', 'ops'), true);
    const cleared = bus.getSubscriptionAudit({ action: 'group_default_session_timeout_cleared' });
    assert.equal(cleared.length, 1);
    assert.equal(cleared[0].actor, 'ops');
    assert.deepEqual(cleared[0].detail, { previous_timeout_ms: 8000 });
  });

  it('the getter follows the live value and getStats reports the default', () => {
    const bus = new EventBus();
    assert.equal(bus.getGroupDefaultSessionTimeout('g', 't'), undefined);
    bus.subscribeToGroup('g', 't', () => {}, { instanceId: 'i1' });
    assert.deepEqual(bus.getStats().consumerGroups, [
      { groupId: 'g', pattern: 't', members: 1 },
    ]);
    bus.setGroupDefaultSessionTimeout('g', 't', 60_000);
    assert.equal(bus.getGroupDefaultSessionTimeout('g', 't'), 60_000);
    assert.deepEqual(bus.getStats().consumerGroups, [
      { groupId: 'g', pattern: 't', members: 1, defaultSessionTimeoutMs: 60_000 },
    ]);
    bus.clearGroupDefaultSessionTimeout('g', 't');
    assert.equal(bus.getGroupDefaultSessionTimeout('g', 't'), undefined);
    assert.deepEqual(bus.getStats().consumerGroups, [
      { groupId: 'g', pattern: 't', members: 1 },
    ]);
  });

  it('a member already in limbo is unaffected by a later group default', async () => {
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
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);

    // The default arrives after the limbo timer was armed at 5000ms:
    // the slot must still be held 150ms later.
    bus.setGroupDefaultSessionTimeout('g', 't', 60);
    await sleep(150);
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);
    assert.equal(events.filter((e) => e.trigger === 'leave').length, 0);
    m2.unsubscribe();
  });

  it('setting a default fires no rebalance', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent) => events.push(e);
    const m1 = bus.subscribeToGroup('g', 't', () => {}, {
      instanceId: 'i1',
      onRebalance,
    });
    const m2 = bus.subscribeToGroup('g', 't', () => {}, { onRebalance });
    events.length = 0;
    bus.setGroupDefaultSessionTimeout('g', 't', 60_000);
    assert.equal(events.length, 0, 'the revision itself must not rebalance');
    assert.deepEqual(bus.getGroupMembers('g', 't'), ['i1', m2.id]);
    bus.clearGroupDefaultSessionTimeout('g', 't');
    assert.equal(events.length, 0, 'clearing the default must not rebalance either');
    m1.unsubscribe();
    m2.unsubscribe();
  });
});
