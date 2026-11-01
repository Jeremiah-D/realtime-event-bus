import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const noop = (): void => {};

/**
 * A rebalance emit fans out to every member's `onRebalance`, so a
 * shared counting callback would count one push per member per emit.
 * These tests count emits: only the first member carries the counting
 * callback; later members omit `onRebalance` entirely.
 */
describe('setGroupRebalanceDebounce (EB-333)', () => {
  it('retunes the window for the next window opened after the revision', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      events.push({ ...e, members: [...e.members] });
    };
    const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 400, onRebalance });
    await sleep(500);
    assert.equal(events.length, 1, 'the first join fires after the original 400ms window');
    assert.equal(events[0].memberId, a.id);

    bus.setGroupRebalanceDebounce('g', 't', 60);
    const b = bus.subscribeToGroup('g', 't', noop);
    // Under the old 400ms window this would still be pending; under
    // the revised 60ms window it has fired.
    await sleep(150);
    assert.equal(events.length, 2, 'the revised window governs the next window');
    assert.equal(events[1].memberId, b.id);
    assert.equal(events[1].suppressedRebalances, 0);
    assert.deepEqual(events[1].members, [a.id, b.id]);

    const audit = bus.getSubscriptionAudit({ action: 'group_rebalance_debounce_revised' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].groupId, 'g');
    assert.equal(audit[0].pattern, 't');
    assert.deepEqual(audit[0].detail, { windowMs: 60, oldWindowMs: 400 });
    a.unsubscribe();
    b.unsubscribe();
  });

  it('an armed window flushes at the old value; the new window governs the next one', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      events.push({ ...e, members: [...e.members] });
    };
    const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 400, onRebalance });
    // Revise while the 400ms window is armed: the pending window must
    // keep the timer it was armed with, not the new 60ms value.
    bus.setGroupRebalanceDebounce('g', 't', 60);
    const b = bus.subscribeToGroup('g', 't', noop);
    await sleep(150);
    assert.equal(events.length, 0, 'the armed window still runs on its old 400ms timer');
    await sleep(400);
    assert.equal(events.length, 1, 'the armed window flushes at the old value');
    assert.equal(events[0].memberId, b.id);
    assert.equal(events[0].trigger, 'join');
    assert.equal(events[0].suppressedRebalances, 1);
    assert.deepEqual(events[0].members, [a.id, b.id]);

    // The next change opens a window at the revised value.
    const c = bus.subscribeToGroup('g', 't', noop);
    await sleep(150);
    assert.equal(events.length, 2, 'the next window opens at the revised 60ms');
    assert.equal(events[1].memberId, c.id);
    assert.equal(events[1].suppressedRebalances, 0);
    a.unsubscribe();
    b.unsubscribe();
    c.unsubscribe();
  });

  it('clearing returns the group to immediate rebalances, and 0 clears too', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      events.push({ ...e, members: [...e.members] });
    };
    const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 300, onRebalance });
    await sleep(400);
    assert.equal(events.length, 1);

    bus.setGroupRebalanceDebounce('g', 't', undefined);
    const b = bus.subscribeToGroup('g', 't', noop);
    assert.equal(events.length, 2, 'a join after a clear emits immediately');
    assert.equal(events[1].memberId, b.id);
    assert.equal('suppressedRebalances' in events[1], false);

    // Re-opt in, then clear with the construction-time spelling.
    bus.setGroupRebalanceDebounce('g', 't', 200);
    bus.setGroupRebalanceDebounce('g', 't', 0);
    const c = bus.subscribeToGroup('g', 't', noop);
    assert.equal(events.length, 3, 'a join after a 0-clear emits immediately');
    assert.equal(events[2].memberId, c.id);

    const audit = bus.getSubscriptionAudit({ action: 'group_rebalance_debounce_revised' });
    assert.equal(audit.length, 3);
    assert.deepEqual(audit[0].detail, { windowMs: null, oldWindowMs: 300 });
    assert.deepEqual(audit[1].detail, { windowMs: 200, oldWindowMs: null });
    assert.deepEqual(audit[2].detail, { windowMs: null, oldWindowMs: 200 });
    a.unsubscribe();
    b.unsubscribe();
    c.unsubscribe();
  });

  it('opts a never-debounced group in', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      events.push({ ...e, members: [...e.members] });
    };
    const a = bus.subscribeToGroup('g', 't', noop, { onRebalance });
    assert.equal(events.length, 1, 'the first join emits immediately without debounce');

    bus.setGroupRebalanceDebounce('g', 't', 200);
    const b = bus.subscribeToGroup('g', 't', noop);
    await sleep(100);
    assert.equal(events.length, 1, 'the opt-in window holds the second join');
    await sleep(200);
    assert.equal(events.length, 2, 'the second join fires after the opted-in window');
    assert.equal(events[1].memberId, b.id);

    const audit = bus.getSubscriptionAudit({ action: 'group_rebalance_debounce_revised' });
    assert.equal(audit.length, 1);
    assert.deepEqual(audit[0].detail, { windowMs: 200, oldWindowMs: null });
    a.unsubscribe();
    b.unsubscribe();
  });

  it('an identical revision is a no-op with no audit', () => {
    const bus = new EventBus();
    const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 200 });
    bus.setGroupRebalanceDebounce('g', 't', 200);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_rebalance_debounce_revised' }).length,
      0,
      're-affirming the current window audits nothing',
    );

    // Clearing an already-undebounced group is a no-op too.
    const b = bus.subscribeToGroup('h', 't', noop);
    bus.setGroupRebalanceDebounce('h', 't', undefined);
    bus.setGroupRebalanceDebounce('h', 't', 0);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_rebalance_debounce_revised' }).length,
      0,
      'clearing an undebounced group audits nothing',
    );
    a.unsubscribe();
    b.unsubscribe();
  });

  it('is fail-closed: unknown groups and illegal values leave the window untouched', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      events.push({ ...e, members: [...e.members] });
    };
    const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 300, onRebalance });
    await sleep(400);
    assert.equal(events.length, 1);

    assert.throws(() => bus.setGroupRebalanceDebounce('nope', 't', 100), RangeError);
    assert.throws(() => bus.setGroupRebalanceDebounce('g', 'other', 100), RangeError);
    assert.throws(() => bus.setGroupRebalanceDebounce('', 't', 100), RangeError);
    assert.throws(() => bus.setGroupRebalanceDebounce('g', '', 100), RangeError);
    for (const bad of [Number.NaN, -1, Number.POSITIVE_INFINITY, -0.5, 'x' as unknown as number]) {
      assert.throws(
        () => bus.setGroupRebalanceDebounce('g', 't', bad),
        RangeError,
        `expected RangeError for ${String(bad)}`,
      );
    }
    assert.equal(
      bus.getSubscriptionAudit({ action: 'group_rebalance_debounce_revised' }).length,
      0,
      'failed revisions audit nothing',
    );

    // The live window is untouched: the next join still waits out the
    // original 300ms window.
    const b = bus.subscribeToGroup('g', 't', noop);
    await sleep(150);
    assert.equal(events.length, 1, 'the original window still holds the join');
    await sleep(250);
    assert.equal(events.length, 2, 'the original window still flushes');
    assert.equal(events[1].memberId, b.id);

    // A fully drained group is unknown: its retained window config is
    // re-fixed by the next generation's first member, not revised.
    a.unsubscribe();
    b.unsubscribe();
    assert.deepEqual(bus.getGroupMembers('g', 't'), []);
    assert.throws(() => bus.setGroupRebalanceDebounce('g', 't', 100), RangeError);
  });
});
