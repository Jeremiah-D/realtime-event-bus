import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type GroupRebalanceEvent } from '../src/bus.ts';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const noop = (): void => {};

// EB-469: bus-level terminal group rebalance-debounce default live revision.

test('EB-469: the factory bus default is unset (immediate rebalances)', () => {
  const bus = new EventBus();
  assert.equal(bus.getGroupDefaultRebalanceDebounce(), undefined);
});

test('EB-469: a new group whose members omit rebalanceDebounceMs takes the bus-default window', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent): void => {
    events.push({ ...e, members: [...e.members] });
  };
  const a = bus.subscribeToGroup('g', 't', noop, { onRebalance });
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

test('EB-469: a join and a leave inside one default window merge into one event', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  const aEvents: GroupRebalanceEvent[] = [];
  const a = bus.subscribeToGroup('g', 't', noop, {
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

test('EB-469: an explicit window beats the bus default in both directions', async () => {
  const bus = new EventBus();
  // Explicit 60 beats a bus default of 500: the merge fires by 150ms.
  bus.setGroupDefaultRebalanceDebounce(500);
  const fast: GroupRebalanceEvent[] = [];
  const onFast = (e: GroupRebalanceEvent): void => {
    fast.push(e);
  };
  const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 60, onRebalance: onFast });
  const b = bus.subscribeToGroup('g', 't', noop, { onRebalance: onFast });
  const c = bus.subscribeToGroup('g', 't', noop, { onRebalance: onFast });
  await sleep(150);
  assert.equal(fast.length, 3, 'a 500ms window would not have fired yet');
  assert.ok(fast.every((e) => e.suppressedRebalances === 2));
  assert.deepEqual(
    fast.map((e) => e.memberId),
    [c.id, c.id, c.id],
  );
  assert.deepEqual(
    fast.map((e) => e.members),
    [
      [a.id, b.id, c.id],
      [a.id, b.id, c.id],
      [a.id, b.id, c.id],
    ],
  );

  // Explicit 500 beats a bus default of 60: nothing fires by 150ms.
  bus.setGroupDefaultRebalanceDebounce(60);
  const slow: GroupRebalanceEvent[] = [];
  const onSlow = (e: GroupRebalanceEvent): void => {
    slow.push(e);
  };
  const d = bus.subscribeToGroup('h', 't', noop, { rebalanceDebounceMs: 500, onRebalance: onSlow });
  const e = bus.subscribeToGroup('h', 't', noop, { onRebalance: onSlow });
  await sleep(150);
  assert.equal(slow.length, 0, 'a 60ms window would have fired already');
  await sleep(600);
  assert.equal(slow.length, 2);
  for (const ev of slow) {
    assert.equal(ev.trigger, 'join');
    assert.equal(ev.memberId, e.id);
    assert.deepEqual(ev.members, [d.id, e.id]);
    assert.equal(ev.suppressedRebalances, 1);
  }
});

test('EB-469: an explicit 0 does not take the bus default — its rebalance fires immediately', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  const events: GroupRebalanceEvent[] = [];
  const a = bus.subscribeToGroup('g', 't', noop, {
    rebalanceDebounceMs: 0,
    onRebalance: (e) => events.push(e),
  });
  // A 60ms window would not have fired yet at 50ms.
  await sleep(50);
  assert.equal(events.length, 1, 'explicit 0 must fire immediately, not after the default window');
  assert.equal(events[0].trigger, 'join');
  assert.ok(!('suppressedRebalances' in events[0]), 'immediate events carry no suppressedRebalances');
  // A later member that omits the option takes the default and opts the
  // group in — the group had no window, so the default fills it.
  const b = bus.subscribeToGroup('g', 't', noop, { onRebalance: (e) => events.push(e) });
  await sleep(150);
  assert.equal(events.length, 3, 'one immediate event plus one merged event per member');
  for (const ev of events.slice(1)) {
    assert.equal(ev.trigger, 'join');
    assert.equal(ev.memberId, b.id);
    assert.deepEqual(ev.members, [a.id, b.id]);
    assert.equal(ev.suppressedRebalances, 0);
  }
});

test('EB-469: a member that omits the option inherits the group window instead of the bus default', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(500);
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent): void => {
    events.push({ ...e, members: [...e.members] });
  };
  const a = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 60, onRebalance });
  const b = bus.subscribeToGroup('g', 't', noop, { onRebalance });
  const c = bus.subscribeToGroup('g', 't', noop, { onRebalance });
  await sleep(150);
  // Had the omitting members taken the 500ms default, the join would
  // have thrown a window mismatch — and nothing would have fired by 150ms.
  assert.equal(events.length, 3);
  for (const e of events) {
    assert.equal(e.suppressedRebalances, 2);
    assert.deepEqual(e.members, [a.id, b.id, c.id]);
  }
});

test('EB-469: a later member naming a different window still throws, including against a default-derived window', () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  const m1 = bus.subscribeToGroup('g', 't', noop);
  assert.throws(
    () => bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 120 }),
    /debounce window mismatch/,
  );
  assert.equal(bus.getGroupMembers('g', 't').length, 1);
  // A member that agrees with the derived window joins fine.
  const m2 = bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 60 });
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-469: clearing with null returns new groups to immediate rebalances', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  bus.setGroupDefaultRebalanceDebounce(null);
  assert.equal(bus.getGroupDefaultRebalanceDebounce(), undefined);
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
  assert.ok(events.every((e) => !('suppressedRebalances' in e)));
});

test('EB-469: revising to the current default is a no-op with no audit; 0 clears like null', () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_rebalance_debounce_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setGroupDefaultRebalanceDebounce(60);
  bus.setGroupDefaultRebalanceDebounce(60);
  let audit = bus.getSubscriptionAudit({ action: 'group_default_rebalance_debounce_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, windowMs: 60 });
  // 0 is the "no debounce" spelling: it clears the default.
  bus.setGroupDefaultRebalanceDebounce(0);
  assert.equal(bus.getGroupDefaultRebalanceDebounce(), undefined);
  audit = bus.getSubscriptionAudit({ action: 'group_default_rebalance_debounce_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, { previous: 60, windowMs: null });
  // Clearing an already-unset default again is a no-op.
  bus.setGroupDefaultRebalanceDebounce(0);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_rebalance_debounce_revised' }).length,
    2,
  );
});

test('EB-469: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  for (const bad of [-1, Number.NaN, Infinity, '60'] as unknown[]) {
    assert.throws(
      () => bus.setGroupDefaultRebalanceDebounce(bad as number),
      RangeError,
      `windowMs=${String(bad)}`,
    );
    assert.equal(bus.getGroupDefaultRebalanceDebounce(), 60);
  }
  assert.throws(() => bus.setGroupDefaultRebalanceDebounce(120, ''), RangeError);
  assert.equal(bus.getGroupDefaultRebalanceDebounce(), 60);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_rebalance_debounce_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-469: revising the bus default never moves a live group window', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  const aEvents: GroupRebalanceEvent[] = [];
  const a = bus.subscribeToGroup('g', 't', noop, {
    onRebalance: (e) => aEvents.push({ ...e, members: [...e.members] }),
  });
  const b = bus.subscribeToGroup('g', 't', noop, { onRebalance: noop });
  await sleep(150);
  assert.equal(aEvents.length, 1);
  assert.equal(aEvents[0].suppressedRebalances, 1);
  bus.setGroupDefaultRebalanceDebounce(500);
  assert.equal(bus.getGroupDefaultRebalanceDebounce(), 500);
  // A later member still inherits the live 60ms window, not the new default.
  const c = bus.subscribeToGroup('g', 't', noop, { onRebalance: noop });
  await sleep(150);
  assert.equal(aEvents.length, 2, 'the live window still merges at 60ms');
  assert.equal(aEvents[1].suppressedRebalances, 0);
  assert.deepEqual(aEvents[1].members, [a.id, b.id, c.id]);
  // And a member naming the new default value is refused: the live
  // window is still 60ms.
  assert.throws(
    () => bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 500 }),
    /debounce window mismatch/,
  );
});

test('EB-469: a drained and recreated group takes the default in effect at recreation', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  const a = bus.subscribeToGroup('g', 't', noop);
  a.unsubscribe();
  await flush();
  assert.equal(bus.getGroupMembers('g', 't').length, 0);

  bus.setGroupDefaultRebalanceDebounce(120);
  const events: GroupRebalanceEvent[] = [];
  const onRebalance = (e: GroupRebalanceEvent): void => {
    events.push({ ...e, members: [...e.members] });
  };
  const b = bus.subscribeToGroup('g', 't', noop, { onRebalance });
  const c = bus.subscribeToGroup('g', 't', noop, { onRebalance });
  // A stale 60ms window would have fired by 90ms; the new 120ms default has not.
  await sleep(90);
  assert.equal(events.length, 0, 'the new generation must take the new 120ms default, not the stale 60ms window');
  await sleep(150);
  assert.equal(events.length, 2);
  for (const e of events) {
    assert.equal(e.suppressedRebalances, 1);
    assert.deepEqual(e.members, [b.id, c.id]);
  }
});

test('EB-469: setGroupRebalanceDebounce still revises one live group and leaves the bus default alone', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60);
  bus.subscribeToGroup('g', 't', noop, { onRebalance: noop });
  bus.setGroupRebalanceDebounce('g', 't', 200);
  assert.equal(bus.getGroupDefaultRebalanceDebounce(), 60, 'the per-group revision must not move the default');
  // A member naming the old window is refused: the live window is 200ms now.
  assert.throws(
    () => bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 60 }),
    /debounce window mismatch/,
  );
  bus.subscribeToGroup('g', 't', noop, { onRebalance: noop });
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  bus.setGroupRebalanceDebounce('g', 't', undefined);
  assert.equal(bus.getGroupDefaultRebalanceDebounce(), 60, 'clearing one group must not move the default');
  // After the per-group clear, a fresh joiner takes the bus default again.
  bus.subscribeToGroup('g', 't', noop, { onRebalance: noop });
  assert.throws(
    () => bus.subscribeToGroup('g', 't', noop, { rebalanceDebounceMs: 200 }),
    /debounce window mismatch/,
    'the group window is back to the 60ms bus default',
  );
  assert.equal(bus.getGroupMembers('g', 't').length, 3);
});

test('EB-469: setGroupDefaultRebalanceDebounce audits each revision with its old and new window and actor', () => {
  const bus = new EventBus();
  bus.setGroupDefaultRebalanceDebounce(60, 'ops');
  bus.setGroupDefaultRebalanceDebounce(null, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'group_default_rebalance_debounce_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, windowMs: 60 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 60, windowMs: null });
});
