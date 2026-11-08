/**
 * Authorization-denial audit hook live redirection (EB-425).
 *
 * `setAuthzDeniedCallback` swaps the `onAuthzDenied` observer every ACL
 * denial is handed to without restarting the bus. There is no enable
 * gate — any open bus can install one — and `null` clears back to no
 * observer. The new hook governs from the next denial onward, the
 * denial counter moves regardless of whether a hook is installed, and a
 * throwing hook is swallowed by `emitAuthzDenied`. The callback function
 * itself is never written to the audit log: only whether one is
 * installed is recorded (`{ hasCallback, oldHasCallback }`).
 * Re-installing the live callback is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, AclDeniedError, type AuthzDeniedEvent } from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

/** A bus denying publishes to `admin.**` (and subscribes overlapping it). */
function deniedBus(extra: Record<string, unknown> = {}): EventBus {
  return new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny', subscribe: 'deny' }] },
    ...extra,
  });
}

test('installing a hook routes later denials to it', async () => {
  const seen: AuthzDeniedEvent[] = [];
  const bus = deniedBus();
  // No hook at construction: the denial is counted, not observed.
  assert.equal(bus.publish('admin.cmd', 1), 0);
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().authzDenied, 1);
  bus.setAuthzDeniedCallback((e) => seen.push(e));
  assert.equal(bus.publish('admin.cmd', 2), 0);
  assert.throws(() => bus.subscribe('admin.**', () => {}), AclDeniedError);
  assert.equal(seen.length, 2);
  assert.deepEqual(
    seen.map((e) => ({ action: e.action, topic: e.topic, pattern: e.pattern })),
    [
      { action: 'publish', topic: 'admin.cmd', pattern: undefined },
      { action: 'subscribe', topic: undefined, pattern: 'admin.**' },
    ],
  );
  assert.equal(bus.getStats().authzDenied, 3);
  bus.destroy?.();
});

test('clearing with null stops delivery and the counter keeps moving', async () => {
  const seen: AuthzDeniedEvent[] = [];
  const bus = deniedBus({ onAuthzDenied: (e: AuthzDeniedEvent) => seen.push(e) });
  assert.equal(bus.publish('admin.cmd', 1), 0);
  assert.equal(seen.length, 1);
  bus.setAuthzDeniedCallback(null);
  assert.equal(bus.publish('admin.cmd', 2), 0);
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().authzDenied, 2);
  // Clearing a bus with no callback is a no-op with no audit.
  const before = bus.getSubscriptionAudit({ action: 'authz_denied_callback_revised' }).length;
  bus.setAuthzDeniedCallback(null);
  assert.equal(bus.getSubscriptionAudit({ action: 'authz_denied_callback_revised' }).length, before);
  bus.destroy?.();
});

test('non-functions are rejected and the live hook stays put', async () => {
  const seen: AuthzDeniedEvent[] = [];
  const bus = deniedBus({ onAuthzDenied: (e: AuthzDeniedEvent) => seen.push(e) });
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const handler of bad) {
    assert.throws(
      () => bus.setAuthzDeniedCallback(handler as never),
      RangeError,
      `expected RangeError for ${String(handler)}`,
    );
  }
  // The failed calls changed nothing: the live hook still observes.
  assert.equal(bus.publish('admin.cmd', 1), 0);
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'authz_denied_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('re-installing the live callback is a no-op with no audit', () => {
  const handler = (_e: AuthzDeniedEvent): void => {};
  const bus = deniedBus({ onAuthzDenied: handler });
  bus.setAuthzDeniedCallback(handler);
  assert.equal(bus.getSubscriptionAudit({ action: 'authz_denied_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('a closed bus refuses the swap', async () => {
  const bus = deniedBus();
  await bus.close();
  const handler = (_e: AuthzDeniedEvent): void => {};
  assert.throws(() => bus.setAuthzDeniedCallback(handler), RangeError);
  assert.throws(() => bus.setAuthzDeniedCallback(null), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'authz_denied_callback_revised' }).length, 0);
});

test('a throwing hook is swallowed and the publish path is unaffected', async () => {
  const bus = deniedBus();
  bus.setAuthzDeniedCallback(() => {
    throw new Error('broken observer');
  });
  // The denial still returns 0 (not a throw) and the counter moves.
  assert.equal(bus.publish('admin.cmd', 1), 0);
  assert.equal(bus.getStats().authzDenied, 1);
  // An admitted publish still flows to subscribers.
  const received: unknown[] = [];
  bus.subscribe('market.btc', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('market.btc', 2), 1);
  await flush();
  assert.deepEqual(received, [2]);
  bus.destroy?.();
});

test('the swap audits exact presence flags with the actor', () => {
  const first = (_e: AuthzDeniedEvent): void => {};
  const second = (_e: AuthzDeniedEvent): void => {};
  const bus = deniedBus();
  bus.setAuthzDeniedCallback(first, 'ops');
  bus.setAuthzDeniedCallback(second, 'ops');
  bus.setAuthzDeniedCallback(null, 'ops');
  const audit = bus.getSubscriptionAudit({ action: 'authz_denied_callback_revised' });
  assert.equal(audit.length, 3);
  assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(audit[0].actor, 'ops');
  assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(audit[2].detail, { hasCallback: false, oldHasCallback: true });
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(audit).includes('first'));
  bus.destroy?.();
});

test('setAuthzDeniedCallback rejects an illegal actor', () => {
  const bus = deniedBus();
  const handler = (_e: AuthzDeniedEvent): void => {};
  assert.throws(() => bus.setAuthzDeniedCallback(handler, ''), RangeError);
  assert.throws(() => bus.setAuthzDeniedCallback(handler, 42 as never), RangeError);
  assert.equal(bus.getSubscriptionAudit({ action: 'authz_denied_callback_revised' }).length, 0);
  bus.destroy?.();
});

test('ACL rules and policies are untouched by a hook swap', async () => {
  const seen: AuthzDeniedEvent[] = [];
  const bus = deniedBus();
  const rules = bus.getAclRules();
  bus.setAuthzDeniedCallback((e) => seen.push(e));
  bus.setAuthzDeniedCallback(null);
  assert.deepEqual(bus.getAclRules(), rules);
  // Verdicts are unchanged: the deny rule still denies, the default
  // policy still allows everything else.
  assert.equal(bus.publish('admin.cmd', 1), 0);
  assert.equal(seen.length, 0);
  const received: unknown[] = [];
  bus.subscribe('market.btc', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('market.btc', 2), 1);
  await flush();
  assert.deepEqual(received, [2]);
  bus.destroy?.();
});

test('two consecutive redirects both take effect', () => {
  const firstSeen: AuthzDeniedEvent[] = [];
  const secondSeen: AuthzDeniedEvent[] = [];
  const bus = deniedBus();
  bus.setAuthzDeniedCallback((e) => firstSeen.push(e));
  assert.equal(bus.publish('admin.cmd', 1), 0);
  assert.equal(firstSeen.length, 1);
  bus.setAuthzDeniedCallback((e) => secondSeen.push(e));
  assert.equal(bus.publish('admin.cmd', 2), 0);
  assert.equal(firstSeen.length, 1);
  assert.equal(secondSeen.length, 1);
  assert.equal(bus.getSubscriptionAudit({ action: 'authz_denied_callback_revised' }).length, 2);
  bus.destroy?.();
});
