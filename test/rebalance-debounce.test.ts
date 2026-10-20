import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const noop = (): void => {};

describe('rebalance debounce (EB-169)', () => {
  it('rejects invalid rebalanceDebounceMs with RangeError and rolls the join back', () => {
    const bus = new EventBus();
    for (const bad of [-1, NaN, Infinity, -0.5]) {
      assert.throws(
        () => bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: bad }),
        RangeError,
        `expected RangeError for ${String(bad)}`,
      );
    }
    assert.deepEqual(bus.getGroupMembers('g', 't'), []);
  });

  it('merges rapid joins into one event and counts the suppressed changes', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      events.push({ ...e, members: [...e.members] });
    };
    const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 60, onRebalance });
    const b = bus.subscribeToGroup('g', 't', noop, { onRebalance });
    const c = bus.subscribeToGroup('g', 't', noop, { onRebalance });
    await sleep(150);
    // One merged event per member — not one per join.
    assert.equal(events.length, 3);
    for (const e of events) {
      assert.equal(e.trigger, 'join');
      assert.equal(e.memberId, c.id);
      assert.deepEqual(e.members, [a.id, b.id, c.id]);
      assert.equal(e.suppressedRebalances, 2);
    }
  });

  it('fires a lone change as one event with suppressedRebalances 0', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    bus.subscribeToGroup('g', 't', noop, {
      rebalanceDebounceMs: 60,
      onRebalance: (e) => events.push(e),
    });
    await sleep(150);
    assert.equal(events.length, 1);
    assert.equal(events[0].trigger, 'join');
    assert.equal(events[0].suppressedRebalances, 0);
  });

  it('leaves non-debounced groups byte-identical: no suppressedRebalances key', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      events.push(e);
    };
    bus.subscribeToGroup('g', 't', noop, { onRebalance });
    bus.subscribeToGroup('g', 't', noop, { onRebalance });
    bus.subscribeToGroup('g', 't', noop, { onRebalance });
    await sleep(50);
    // 3 + 2 + 1 notifications, one per join per member present.
    assert.equal(events.length, 6);
    for (const e of events) {
      assert.ok(!('suppressedRebalances' in e), 'field must be absent without the opt-in');
    }
  });

  it('collapses a mixed join+leave window into one event naming the latest change', async () => {
    const bus = new EventBus();
    const aEvents: GroupRebalanceEvent[] = [];
    const a = bus.subscribeToGroup('g', 't', noop, {
      rebalanceDebounceMs: 60,
      onRebalance: (e) => aEvents.push({ ...e, members: [...e.members] }),
    });
    const b = bus.subscribeToGroup('g', 't', noop, { onRebalance: noop });
    await sleep(150);
    assert.equal(aEvents.length, 1);
    assert.deepEqual(aEvents[0].members, [a.id, b.id]);
    assert.equal(aEvents[0].suppressedRebalances, 1);

    const c = bus.subscribeToGroup('g', 't', noop, { onRebalance: noop });
    b.unsubscribe();
    await sleep(150);
    assert.equal(aEvents.length, 2);
    const merged = aEvents[1];
    assert.equal(merged.trigger, 'leave');
    assert.equal(merged.memberId, b.id);
    assert.deepEqual(merged.members, [a.id, c.id]);
    assert.equal(merged.suppressedRebalances, 1);
  });

  it('keeps the final partition assignment identical to sequential rebalances', async () => {
    const collect = (): GroupRebalanceEvent[] => [];
    const debouncedEvents = collect();
    const controlEvents = collect();

    const debounced = new EventBus();
    const da = debounced.subscribeToGroup('g', 't', noop, {
      partitions: 6,
      rebalanceDebounceMs: 60,
      onRebalance: (e) => debouncedEvents.push({ ...e, members: [...e.members] }),
    });
    const db = debounced.subscribeToGroup('g', 't', noop, { partitions: 6 });
    const dc = debounced.subscribeToGroup('g', 't', noop, {
      partitions: 6,
      onRebalance: (e) => debouncedEvents.push({ ...e, members: [...e.members] }),
    });
    db.unsubscribe();
    await sleep(150);

    const control = new EventBus();
    const ca = control.subscribeToGroup('g', 't', noop, {
      partitions: 6,
      onRebalance: (e) => controlEvents.push(e),
    });
    const cb = control.subscribeToGroup('g', 't', noop, { partitions: 6 });
    const cc = control.subscribeToGroup('g', 't', noop, { partitions: 6 });
    cb.unsubscribe();
    await sleep(50);

    // One merged event (delivered to the two survivors); three changes absorbed.
    assert.equal(debouncedEvents.length, 2);
    const merged = debouncedEvents[0];
    assert.deepEqual(merged.members, [da.id, dc.id]);
    assert.equal(merged.suppressedRebalances, 3);

    const lastControl = controlEvents[controlEvents.length - 1];
    assert.equal(lastControl.trigger, 'leave');
    // The final assignment is exactly what sequential rebalances produced.
    assert.deepEqual(merged.partitionRebalance?.assignment, lastControl.partitionRebalance?.assignment);
    // Net migration only: nothing migrates to the member that left mid-window.
    for (const m of merged.partitionRebalance?.migrated ?? []) {
      assert.ok(m.to === da.id || m.to === dc.id, `stale migration target ${m.to}`);
    }
    void ca;
    void cc;
  });

  it('does not count a static member reclaim as churn', async () => {
    const bus = new EventBus();
    const events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      events.push({ ...e, members: [...e.members] });
    };
    const s1 = bus.subscribeToGroup('g', 't', noop, {
      instanceId: 'i-1',
      sessionTimeoutMs: 5000,
      rebalanceDebounceMs: 60,
      onRebalance,
    });
    // All inside the window s1's join opened: limbo entry and reclaim
    // fire no rebalance, so only the dynamic join is absorbed.
    s1.unsubscribe();
    bus.subscribeToGroup('g', 't', noop, {
      instanceId: 'i-1',
      sessionTimeoutMs: 5000,
      rebalanceDebounceMs: 60,
      onRebalance,
    });
    const d = bus.subscribeToGroup('g', 't', noop, { onRebalance });
    await sleep(150);

    // s1 left before the window ended, so it never saw an event; the two
    // live members each saw the single merged one.
    assert.equal(events.length, 2);
    for (const e of events) {
      assert.equal(e.trigger, 'join');
      assert.equal(e.memberId, d.id);
      assert.deepEqual(e.members, ['i-1', d.id]);
      // 1 = the dynamic join only: the limbo entry and the reclaim were
      // never offered to the debouncer.
      assert.equal(e.suppressedRebalances, 1);
    }
  });

  it('throws RangeError when a later member disagrees on the window, rolling back', async () => {
    const bus = new EventBus();
    const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 100 });
    assert.throws(
      () => bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 200 }),
      RangeError,
    );
    assert.deepEqual(bus.getGroupMembers('g', 't'), [a.id]);
    // The same window is accepted.
    bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 100 });
    assert.equal(bus.getGroupMembers('g', 't').length, 2);
    await bus.close();
  });

  it('drops a pending window when the group fully drains — no cross-generation merge', async () => {
    const bus = new EventBus();
    const gen1Events: GroupRebalanceEvent[] = [];
    const onRebalance = (e: GroupRebalanceEvent): void => {
      gen1Events.push(e);
    };
    const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 60, onRebalance });
    const b = bus.subscribeToGroup('g', 't', noop, { onRebalance });
    a.unsubscribe();
    b.unsubscribe();
    await sleep(150);
    // The window (2 joins + 2 leaves) died with the generation: no event
    // fires into the next generation.
    assert.equal(gen1Events.length, 0);

    const gen2Events: GroupRebalanceEvent[] = [];
    const n = bus.subscribeToGroup('g', 't', noop, {
      rebalanceDebounceMs: 60,
      onRebalance: (e) => gen2Events.push(e),
    });
    await sleep(150);
    assert.equal(gen2Events.length, 1);
    assert.equal(gen2Events[0].suppressedRebalances, 0);
    assert.deepEqual(gen2Events[0].members, [n.id]);
  });

  it('lets a later member opt a non-debounced group in', async () => {
    const bus = new EventBus();
    const aEvents: GroupRebalanceEvent[] = [];
    bus.subscribeToGroup('g', 't', noop, {
      onRebalance: (e) => aEvents.push({ ...e, members: [...e.members] }),
    });
    // Opt-in lands before this join's rebalance, so the join is deferred.
    bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 60, onRebalance: noop });
    bus.subscribeToGroup('g', 't', noop, { onRebalance: noop });
    await sleep(150);
    assert.equal(aEvents.length, 2);
    // The pre-opt-in event is untouched by the feature.
    assert.ok(!('suppressedRebalances' in aEvents[0]));
    assert.equal(aEvents[1].suppressedRebalances, 1);
    assert.equal(aEvents[1].members.length, 3);
  });
});
