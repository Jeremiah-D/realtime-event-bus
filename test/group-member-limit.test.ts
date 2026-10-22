import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

// EB-196: consumer-group member limit (`maxMembers` / `setGroupMaxMembers`).

test('EB-196: invalid maxMembers throws RangeError and registers nothing', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, 1.5, NaN, Infinity, '2' as unknown as number]) {
    assert.throws(
      () => bus.subscribeToGroup('g', 't', () => {}, { maxMembers: bad }),
      RangeError,
      `maxMembers=${String(bad)}`,
    );
  }
  assert.deepEqual(bus.getStats().consumerGroups, []);
  assert.deepEqual(bus.getGroupMembers('g', 't'), []);
});

test('EB-196: join past the limit fails fast, registers nothing', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  bus.subscribeToGroup('g', 't', (m) => seen.push(m.payload), { maxMembers: 1 });
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}, {}),
    /member limit reached/,
  );
  // No half-joined member: roster unchanged, no extra subscriber, no delivery.
  assert.equal(bus.getGroupMembers('g', 't').length, 1);
  assert.deepEqual(bus.getStats().subscribersByPattern, { t: 1 });
  bus.publish('t', 'x');
  await flush();
  assert.deepEqual(seen, ['x']);
});

test('EB-196: first member fixes the limit; a disagreeing joiner is rolled back', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 3 });
  bus.subscribeToGroup('g', 't', () => {});
  // A later member naming a different limit fails fast and rolls back.
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 4 }),
    /member limit mismatch/,
  );
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  // ...while a member that agrees with the limit joins fine.
  bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 3 });
  assert.equal(bus.getGroupMembers('g', 't').length, 3);
  // And the full group rejects further joins.
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}),
    /member limit reached/,
  );
  assert.equal(bus.getGroupMembers('g', 't').length, 3);
});

test('EB-196: setGroupMaxMembers sets the limit at runtime', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {});
  bus.subscribeToGroup('g', 't', () => {});
  bus.setGroupMaxMembers('g', 't', 2);
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}),
    /member limit reached/,
  );
  // Raising the limit takes effect for the next join.
  bus.setGroupMaxMembers('g', 't', 3);
  bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupMembers('g', 't').length, 3);
  // Re-setting the same value is a no-op.
  bus.setGroupMaxMembers('g', 't', 3);
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}),
    /member limit reached/,
  );
});

test('EB-196: setGroupMaxMembers validation', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {});
  bus.subscribeToGroup('g', 't', () => {});
  // Unknown group.
  assert.throws(() => bus.setGroupMaxMembers('nope', 't', 2), /unknown consumer group/);
  assert.throws(() => bus.setGroupMaxMembers('g', 'other', 2), /unknown consumer group/);
  // Bad limit values.
  for (const bad of [0, -1, 1.5, NaN, Infinity]) {
    assert.throws(() => bus.setGroupMaxMembers('g', 't', bad), RangeError, `n=${bad}`);
  }
  // Below the current member count: shrink first, then lower.
  assert.throws(
    () => bus.setGroupMaxMembers('g', 't', 1),
    /below the current member count/,
  );
  // Bad ids.
  assert.throws(() => bus.setGroupMaxMembers('', 't', 2), RangeError);
  assert.throws(() => bus.setGroupMaxMembers('g', '', 2), RangeError);
  // The failed calls changed nothing: the group still has no limit.
  assert.equal(bus.getStats().consumerGroups[0].memberLimit, undefined);
  bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupMembers('g', 't').length, 3);
});

test('EB-196: a member that leaves frees its slot', () => {
  const bus = new EventBus();
  const first = bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 1 });
  assert.throws(() => bus.subscribeToGroup('g', 't', () => {}), /member limit reached/);
  first.unsubscribe();
  bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupMembers('g', 't').length, 1);
});

test('EB-196: getStats().consumerGroups reports memberLimit', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('limited', 't', () => {}, { maxMembers: 2 });
  bus.subscribeToGroup('unlimited', 't', () => {});
  const rows = bus.getStats().consumerGroups;
  const limited = rows.find((r) => r.groupId === 'limited')!;
  const unlimited = rows.find((r) => r.groupId === 'unlimited')!;
  assert.equal(limited.memberLimit, 2);
  assert.equal(limited.members, 1);
  assert.ok(!('memberLimit' in unlimited));
});

test('EB-196: the limit is dropped when the group generation ends', () => {
  const bus = new EventBus();
  const a = bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 1 });
  a.unsubscribe();
  // The group died with its last member: the stale limit is dropped, so a
  // fresh generation joins unlimited unless it fixes its own limit.
  bus.subscribeToGroup('g', 't', () => {});
  bus.subscribeToGroup('g', 't', () => {});
  assert.equal(bus.getGroupMembers('g', 't').length, 2);
  assert.ok(!('memberLimit' in bus.getStats().consumerGroups[0]));
  // ...and a runtime limit applies cleanly to the new generation.
  bus.setGroupMaxMembers('g', 't', 2);
  assert.throws(() => bus.subscribeToGroup('g', 't', () => {}), /member limit reached/);
});

test('EB-196: static rejoin reclaims its slot — never rejected by the limit', () => {
  const bus = new EventBus({ now: () => 1_000 });
  const first = bus.subscribeToGroup('g', 't', () => {}, {
    maxMembers: 1,
    instanceId: 'worker-1',
    sessionTimeoutMs: 60_000,
  });
  // A *new* static member counts as a new roster entry: rejected.
  assert.throws(
    () =>
      bus.subscribeToGroup('g', 't', () => {}, {
        instanceId: 'worker-2',
        sessionTimeoutMs: 60_000,
      }),
    /member limit reached/,
  );
  // Leave (the slot is held for the session window), then rejoin with the
  // same instanceId: reclaims the slot, no rejection.
  first.unsubscribe();
  assert.deepEqual(bus.getGroupMembers('g', 't'), ['worker-1']);
  bus.subscribeToGroup('g', 't', () => {}, {
    instanceId: 'worker-1',
    sessionTimeoutMs: 60_000,
  });
  assert.deepEqual(bus.getGroupMembers('g', 't'), ['worker-1']);
});

test('EB-196: the already-online static guard keeps its precise error', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { maxMembers: 1, instanceId: 'worker-1' });
  // worker-1 is still online: the precise already-online error wins over
  // the member-limit error.
  assert.throws(
    () => bus.subscribeToGroup('g', 't', () => {}, { instanceId: 'worker-1' }),
    /already online/,
  );
});

test('EB-196: limited group still delivers and rebalances normally', async () => {
  const bus = new EventBus();
  const seenA: unknown[] = [];
  const seenB: unknown[] = [];
  bus.subscribeToGroup('g', 'jobs.*', (m) => seenA.push(m.payload), { maxMembers: 2 });
  bus.subscribeToGroup('g', 'jobs.*', (m) => seenB.push(m.payload));
  for (let i = 0; i < 4; i += 1) bus.publish('jobs.email', i);
  await flush();
  // Competing consumption is untouched by the limit.
  assert.equal(seenA.length + seenB.length, 4);
  assert.equal(bus.getStats().consumerGroups[0].memberLimit, 2);
});
