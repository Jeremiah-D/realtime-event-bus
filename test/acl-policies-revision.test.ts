/**
 * ACL default and anonymous policies live revision (EB-424).
 *
 * `setAclPolicies` moves the two verdicts the rule list falls through
 * to — `defaultPolicy` (no rule decides) and `anonymousPolicy`
 * (anonymous publishes under a publisher-constrained allow rule) —
 * without restarting the bus. The verdict sites already read the live
 * fields, so the new policies govern from the very next verdict; a
 * revision identical to the live policies is a no-op with no audit.
 * Audited as `acl_policies_revised`
 * (`{ previous: { defaultPolicy, anonymousPolicy }, current:
 * { defaultPolicy, anonymousPolicy } }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, AclDeniedError } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** Publish verdicts observed through a subscriber on `t.x`. */
function admittedBus(): { bus: EventBus; received: unknown[] } {
  const received: unknown[] = [];
  const bus = new EventBus();
  bus.subscribe('t.x', (msg) => received.push(msg.payload));
  return { bus, received };
}

test('illegal policy values are rejected and the live policies stay put', async () => {
  const { bus, received } = admittedBus();
  const bad: Array<unknown> = ['sometimes', '', 0, 1, true, {}, [], null];
  for (const value of bad) {
    assert.throws(
      () => bus.setAclPolicies({ defaultPolicy: value as never }),
      RangeError,
      `expected RangeError for defaultPolicy=${String(value)}`,
    );
    assert.throws(
      () => bus.setAclPolicies({ anonymousPolicy: value as never }),
      RangeError,
      `expected RangeError for anonymousPolicy=${String(value)}`,
    );
  }
  // A non-object patch is refused outright.
  for (const patch of [null, 'deny', 42, undefined]) {
    assert.throws(() => bus.setAclPolicies(patch as never), RangeError);
  }
  // An illegal value in one field leaves the other field untouched too:
  // the whole patch is atomic.
  assert.throws(
    () => bus.setAclPolicies({ defaultPolicy: 'deny', anonymousPolicy: 'maybe' as never }),
    RangeError,
  );
  // The failed calls changed nothing: publishes are still admitted.
  assert.equal(bus.publish('t.x', 1), 1);
  await flush();
  assert.deepEqual(received, [1]);
  assert.equal(bus.getSubscriptionAudit({ action: 'acl_policies_revised' }).length, 0);
  bus.destroy?.();
});

test('an empty patch is rejected with no audit', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setAclPolicies({}), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'acl_policies_revised' }).length, 0);
  bus.destroy?.();
});

test('a same-value revision is a no-op with no audit', async () => {
  const { bus, received } = admittedBus();
  // Both fields already hold the construction-time defaults.
  bus.setAclPolicies({ defaultPolicy: 'allow', anonymousPolicy: 'deny' });
  bus.setAclPolicies({ defaultPolicy: 'allow' });
  bus.setAclPolicies({ anonymousPolicy: 'deny' });
  assert.equal(bus.getSubscriptionAudit({ action: 'acl_policies_revised' }).length, 0);
  assert.equal(bus.publish('t.x', 1), 1);
  await flush();
  assert.deepEqual(received, [1]);
  bus.destroy?.();
});

test('flipping defaultPolicy to deny blocks unmatched publishes and subscribes', async () => {
  const { bus, received } = admittedBus();
  // No rules: every verdict falls through to the default policy.
  assert.equal(bus.publish('t.x', 1), 1);
  bus.setAclPolicies({ defaultPolicy: 'deny' });
  assert.equal(bus.publish('t.x', 2), 0);
  await flush();
  assert.deepEqual(received, [1]);
  assert.equal(bus.getStats().authzDenied, 1);
  assert.throws(() => bus.subscribe('other.y', () => {}), AclDeniedError);
  assert.equal(bus.getStats().authzDenied, 2);
  // Existing subscriptions are not evicted by the flip.
  assert.equal(bus.publish('t.x', 3), 0);
  bus.destroy?.();
});

test('flipping anonymousPolicy governs anonymous publishes under a publisher-constrained allow rule', async () => {
  const received: unknown[] = [];
  const bus = new EventBus({
    acl: { rules: [{ pattern: 't.**', publish: 'allow', publishers: ['svc-a'] }] },
  });
  bus.subscribe('t.x', (msg) => received.push(msg.payload));
  // Anonymous publishes are denied while the policy is 'deny'.
  assert.equal(bus.publish('t.x', 1), 0);
  // A listed publisher is admitted throughout.
  assert.equal(bus.publish('t.x', 2, { publisherId: 'svc-a' }), 1);
  // An unlisted publisher is denied by the rule itself, not the policy.
  assert.equal(bus.publish('t.x', 3, { publisherId: 'other' }), 0);
  bus.setAclPolicies({ anonymousPolicy: 'allow' });
  assert.equal(bus.publish('t.x', 4), 1);
  // The rule still gates named publishers: unlisted stays denied.
  assert.equal(bus.publish('t.x', 5, { publisherId: 'other' }), 0);
  await flush();
  assert.deepEqual(received, [2, 4]);
  bus.destroy?.();
});

test('a closed bus refuses the revision', async () => {
  const bus = new EventBus();
  await bus.close();
  assert.throws(() => bus.setAclPolicies({ defaultPolicy: 'deny' }), RangeError);
  assert.throws(() => bus.setAclPolicies({ anonymousPolicy: 'allow' }), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'acl_policies_revised' }).length, 0);
});

test('setAclPolicies rejects an illegal actor', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setAclPolicies({ defaultPolicy: 'deny' }, ''), RangeError);
  assert.throws(() => bus.setAclPolicies({ defaultPolicy: 'deny' }, 42 as never), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'acl_policies_revised' }).length, 0);
  bus.destroy?.();
});

test('the revision audits exact previous/current policies with the actor', () => {
  const bus = new EventBus({ acl: { defaultPolicy: 'deny', anonymousPolicy: 'deny' } });
  bus.setAclPolicies({ defaultPolicy: 'allow' }, 'ops');
  bus.setAclPolicies({ anonymousPolicy: 'allow', defaultPolicy: 'deny' }, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'acl_policies_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, {
    previous: { defaultPolicy: 'deny', anonymousPolicy: 'deny' },
    current: { defaultPolicy: 'allow', anonymousPolicy: 'deny' },
  });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, {
    previous: { defaultPolicy: 'allow', anonymousPolicy: 'deny' },
    current: { defaultPolicy: 'deny', anonymousPolicy: 'allow' },
  });
  assert.equal(audit[1].actor, 'ops');
  bus.destroy?.();
});

test('the rule list is untouched by a policy revision', () => {
  const rules = [
    { pattern: 'admin.**', publish: 'deny' as const },
    { pattern: 'market.**', subscribe: 'allow' as const },
  ];
  const bus = new EventBus({ acl: { rules } });
  bus.setAclPolicies({ defaultPolicy: 'deny', anonymousPolicy: 'allow' });
  assert.deepEqual(bus.getAclRules(), rules);
  bus.destroy?.();
});

test('two consecutive revisions both take effect', async () => {
  const { bus, received } = admittedBus();
  bus.setAclPolicies({ defaultPolicy: 'deny' });
  assert.equal(bus.publish('t.x', 1), 0);
  bus.setAclPolicies({ defaultPolicy: 'allow' });
  assert.equal(bus.publish('t.x', 2), 1);
  await flush();
  assert.deepEqual(received, [2]);
  assert.equal(bus.getSubscriptionAudit({ action: 'acl_policies_revised' }).length, 2);
  bus.destroy?.();
});
