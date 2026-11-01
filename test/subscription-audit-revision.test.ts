/**
 * Bus-level subscription audit retention bound revision (EB-321).
 *
 * `setSubscriptionAudit` retunes how many entries the in-memory index
 * of the subscription lifecycle audit log retains — without restarting
 * the bus. The audit chain itself is never revised: `dir` / `now` and
 * the hash chain head are untouched. The revision is itself audited
 * into the same log it retunes, so a shrink is already reflected in
 * the in-memory index when the call returns (the revision event is
 * the record that evicts the oldest rows down to the new bound),
 * while a widened bound simply retains more. Illegal values fail
 * closed with the live bound and the audit log untouched; an identical
 * (or empty) revision is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

const audit = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'subscription_audit_revised' });

test('constructor still rejects illegal subscriptionAuditMaxEntries', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, '1000']) {
    assert.throws(() => new EventBus({ subscriptionAuditMaxEntries: bad as never }), RangeError);
  }
});

test('setSubscriptionAudit rejects illegal values and leaves the live bound untouched', () => {
  const bus = new EventBus({ subscriptionAuditMaxEntries: 3 });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  assert.equal(bus.getSubscriptionAudit().length, 2);
  const bad: Array<unknown> = [
    { maxEntries: 0 },
    { maxEntries: -1 },
    { maxEntries: 1.5 },
    { maxEntries: Number.NaN },
    { maxEntries: '1000' },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setSubscriptionAudit(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: same bound, same rows, no audit.
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getSubscriptionAudit().length, 2);
  // A valid revision afterwards still works on the untouched bound.
  bus.setSubscriptionAudit({ maxEntries: 2 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, { maxEntries: 2, oldMaxEntries: 3 });
  bus.close();
});

test('setSubscriptionAudit with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus({ subscriptionAuditMaxEntries: 2 });
  bus.setSubscriptionAudit({ maxEntries: 2 });
  bus.setSubscriptionAudit({});
  bus.setSubscriptionAudit(null as never);
  assert.equal(audit(bus).length, 0);
  assert.equal(bus.getSubscriptionAudit().length, 0);
  bus.close();
});

test('revision audits the old and new bound', () => {
  const bus = new EventBus();
  bus.setSubscriptionAudit({ maxEntries: 500 });
  bus.setSubscriptionAudit({ maxEntries: 2000 });
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, { maxEntries: 500, oldMaxEntries: 1000 });
  assert.deepEqual(entries[1].detail, { maxEntries: 2000, oldMaxEntries: 500 });
  bus.close();
});

test('shrinking is reflected at once: the revision event itself evicts to the new bound', () => {
  const bus = new EventBus({ subscriptionAuditMaxEntries: 3 });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  bus.subscribe('c', () => {});
  assert.deepEqual(
    bus.getSubscriptionAudit().map((e) => e.action),
    ['subscribe', 'subscribe', 'subscribe'],
  );
  bus.setSubscriptionAudit({ maxEntries: 1 });
  // The revision event is appended under the new bound, so it is the
  // record that evicts the three subscribe rows: only it remains.
  const after = bus.getSubscriptionAudit();
  assert.equal(after.length, 1);
  assert.equal(after[0].action, 'subscription_audit_revised');
  assert.deepEqual(after[0].detail, { maxEntries: 1, oldMaxEntries: 3 });
  // The next append keeps evicting to the live bound: the revision
  // event itself is evicted by the next subscribe.
  bus.subscribe('d', () => {});
  const next = bus.getSubscriptionAudit();
  assert.equal(next.length, 1);
  assert.equal(next[0].action, 'subscribe');
  assert.equal(next[0].pattern, 'd');
  bus.close();
});

test('widening the bound retains more rows without evicting', () => {
  const bus = new EventBus({ subscriptionAuditMaxEntries: 1 });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  assert.deepEqual(
    bus.getSubscriptionAudit().map((e) => [e.action, e.pattern]),
    [['subscribe', 'b']],
  );
  bus.setSubscriptionAudit({ maxEntries: 3 });
  // The revision event lands under the widened bound: nothing evicted.
  assert.deepEqual(
    bus.getSubscriptionAudit().map((e) => [e.action, e.pattern]),
    [
      ['subscribe', 'b'],
      ['subscription_audit_revised', undefined],
    ],
  );
  bus.subscribe('c', () => {});
  assert.deepEqual(
    bus.getSubscriptionAudit().map((e) => [e.action, e.pattern]),
    [
      ['subscribe', 'b'],
      ['subscription_audit_revised', undefined],
      ['subscribe', 'c'],
    ],
  );
  bus.close();
});

test('the hash chain stays valid after a shrink and further appends', () => {
  const bus = new EventBus({ subscriptionAuditMaxEntries: 3 });
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  bus.subscribe('c', () => {});
  bus.setSubscriptionAudit({ maxEntries: 1 });
  bus.subscribe('d', () => {});
  bus.subscribe('e', () => {});
  const check = bus.verifySubscriptionAuditChain();
  assert.equal(check.ok, true);
  assert.equal(bus.getSubscriptionAudit().length, 1);
  bus.close();
});

test('audit chain is untouched: dir/now and the chain head survive a revision', () => {
  const bus = new EventBus({ subscriptionAuditMaxEntries: 2 });
  bus.subscribe('a', () => {});
  bus.setSubscriptionAudit({ maxEntries: 5 });
  // The revision event links to the subscribe event: the chain head
  // advanced through the revision, it was not reset.
  const entries = bus.getSubscriptionAudit();
  assert.equal(entries.length, 2);
  assert.equal(entries[1].action, 'subscription_audit_revised');
  assert.equal(entries[1].prevHash, entries[0].hash);
  assert.equal(bus.verifySubscriptionAuditChain().ok, true);
  bus.close();
});
