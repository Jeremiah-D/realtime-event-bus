import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

// EB-467: bus-level terminal group max-members default live revision.

test('EB-467: the factory bus default is unset (no cap)', () => {
  const bus = new EventBus();
  assert.equal(bus.getGroupDefaultMaxMembers(), undefined);
});

test('EB-467: a new group whose first member omits maxMembers takes the bus default', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(2);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  const m2 = bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  assert.deepEqual(bus.getStats().consumerGroups, [
    { groupId: 'g', pattern: 't', members: 2, memberLimit: 2 },
  ]);
  // The default-derived limit is enforced exactly like an explicit one.
  assert.throws(() => bus.subscribeToGroup('g', 't', () => {}), /member limit reached/);
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-467: an explicit maxMembers beats the bus default in both directions', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(1);
  // Explicit 3 beats a bus default of 1.
  const members = [
    bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 3 }),
    bus.subscribeToGroup('g', 't', () => {}),
    bus.subscribeToGroup('g', 't', () => {}),
  ];
  assert.equal(bus.getGroupMembers('g', 't').length, 3);
  assert.equal(bus.getStats().consumerGroups[0].memberLimit, 3);
  for (const m of members) m.unsubscribe();

  // Explicit 1 beats a bus default of 3.
  bus.setGroupDefaultMaxMembers(3);
  const m1 = bus.subscribeToGroup('h', 't', () => {}, { maxMembers: 1 });
  assert.throws(() => bus.subscribeToGroup('h', 't', () => {}), /member limit reached/);
  assert.equal(bus.getStats().consumerGroups[0].memberLimit, 1);
  m1.unsubscribe();
});

test('EB-467: a later member naming a different limit still throws, including against a default-derived limit', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(2);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 3 }),
    /member limit mismatch/,
  );
  assert.equal(bus.getGroupMembers('g', 't').length, 1);
  // A member that agrees with the derived limit joins fine.
  const m2 = bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 2 });
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-467: clearing with null returns new groups to unlimited', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(1);
  bus.setGroupDefaultMaxMembers(null);
  assert.equal(bus.getGroupDefaultMaxMembers(), undefined);
  const members = [
    bus.subscribeToGroup('g', 't', () => {}),
    bus.subscribeToGroup('g', 't', () => {}),
    bus.subscribeToGroup('g', 't', () => {}),
  ];
  assert.equal(bus.getGroupMembers('g', 't').length, 3);
  assert.deepEqual(bus.getStats().consumerGroups, [
    { groupId: 'g', pattern: 't', members: 3 },
  ]);
  for (const m of members) m.unsubscribe();
});

test('EB-467: revising to the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_max_members_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setGroupDefaultMaxMembers(4);
  bus.setGroupDefaultMaxMembers(4);
  const audit = bus.getSubscriptionAudit({ action: 'group_default_max_members_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, { previous: null, maxMembers: 4 });
});

test('EB-467: an illegal value or actor throws RangeError and the live default is untouched', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(2);
  for (const bad of [0, -1, 1.5, Number.NaN, Infinity, '3'] as unknown[]) {
    assert.throws(
      () => bus.setGroupDefaultMaxMembers(bad as number),
      RangeError,
      `n=${String(bad)}`,
    );
    assert.equal(bus.getGroupDefaultMaxMembers(), 2);
  }
  assert.throws(() => bus.setGroupDefaultMaxMembers(3, ''), RangeError);
  assert.equal(bus.getGroupDefaultMaxMembers(), 2);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'group_default_max_members_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-467: revising the bus default never moves a live group limit', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(2);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  const m2 = bus.subscribeToGroup('g', 't', () => {});
  bus.setGroupDefaultMaxMembers(5);
  assert.throws(() => bus.subscribeToGroup('g', 't', () => {}), /member limit reached/);
  assert.equal(bus.getStats().consumerGroups[0].memberLimit, 2);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-467: a drained and recreated group takes the default in effect at recreation', async () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(1);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  assert.throws(() => bus.subscribeToGroup('g', 't', () => {}), /member limit reached/);
  m1.unsubscribe();
  await flush();

  bus.setGroupDefaultMaxMembers(3);
  const members = [
    bus.subscribeToGroup('g', 't', () => {}),
    bus.subscribeToGroup('g', 't', () => {}),
    bus.subscribeToGroup('g', 't', () => {}),
  ];
  assert.equal(bus.getGroupMembers('g', 't').length, 3);
  assert.equal(bus.getStats().consumerGroups[0].memberLimit, 3);
  assert.throws(() => bus.subscribeToGroup('g', 't', () => {}), /member limit reached/);
  for (const m of members) m.unsubscribe();
});

test('EB-467: setGroupMaxMembers still revises one live group and leaves the bus default alone', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(1);
  const m1 = bus.subscribeToGroup('g', 't', () => {});
  assert.throws(() => bus.subscribeToGroup('g', 't', () => {}), /member limit reached/);
  bus.setGroupMaxMembers('g', 't', 2);
  assert.equal(bus.getGroupDefaultMaxMembers(), 1, 'the per-group revision must not move the default');
  const m2 = bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  m1.unsubscribe();
  m2.unsubscribe();
});

test('EB-467: setGroupDefaultMaxMembers audits each revision with its old and new cap and actor', () => {
  const bus = new EventBus();
  bus.setGroupDefaultMaxMembers(2, 'ops');
  bus.setGroupDefaultMaxMembers(null, 'ops');
  const events = bus.getSubscriptionAudit({ action: 'group_default_max_members_revised' });
  assert.equal(events.length, 2);
  assert.equal(events[0].actor, 'ops');
  assert.deepEqual(events[0].detail, { previous: null, maxMembers: 2 });
  assert.equal(events[1].actor, 'ops');
  assert.deepEqual(events[1].detail, { previous: 2, maxMembers: null });
});
