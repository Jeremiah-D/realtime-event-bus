import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  type AclRule,
  type AdmissionRejectionEvent,
  type AuthzDeniedEvent,
} from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

function aclBus(rules: AclRule[], extra?: Record<string, unknown>) {
  const admissionEvents: AdmissionRejectionEvent[] = [];
  const authzEvents: AuthzDeniedEvent[] = [];
  const bus = new EventBus({
    acl: { rules, ...(extra ?? {}) },
    onAdmissionRejected: (e) => admissionEvents.push(e),
    onAuthzDenied: (e) => authzEvents.push(e),
  });
  return { bus, admissionEvents, authzEvents };
}

test('rules without publishers behave exactly as before', async () => {
  const { bus } = aclBus([{ pattern: 'orders.**', publish: 'allow' }]);
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));
  // Anonymous and identified publishes are both admitted — no publisher
  // dimension was configured.
  assert.equal(bus.publish('orders.created', { a: 1 }), 1);
  assert.equal(bus.publish('orders.created', { b: 2 }, { publisherId: 'anyone' }), 1);
  await flush();
  assert.deepEqual(received, [{ a: 1 }, { b: 2 }]);
  bus.destroy?.();
});

test('allowlisted publisher admitted; unlisted publisher denied without fall-through', async () => {
  const { bus, admissionEvents, authzEvents } = aclBus([
    { pattern: 'orders.**', publish: 'allow', publishers: ['svc-payments'] },
    // Must not be reached by an unlisted publisher: the first matching
    // rule denies instead of falling through.
    { pattern: 'orders.**', publish: 'allow' },
  ]);
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));

  assert.equal(bus.publish('orders.created', { ok: true }, { publisherId: 'svc-payments' }), 1);
  assert.equal(bus.publish('orders.created', { no: true }, { publisherId: 'svc-other' }), 0);
  await flush();
  assert.deepEqual(received, [{ ok: true }]);

  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 'orders.created')!;
  assert.equal(topic.rejectedMessages, 1);
  assert.equal(admissionEvents.length, 1);
  assert.equal(admissionEvents[0].reason, 'acl');
  assert.equal(authzEvents.length, 1);
  assert.equal(authzEvents[0].action, 'publish');
  bus.destroy?.();
});

test('anonymous publishes are denied by default under a publisher-constrained rule', async () => {
  const { bus, admissionEvents } = aclBus([
    { pattern: 'orders.**', publish: 'allow', publishers: ['svc-payments'] },
  ]);
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('orders.created', { a: 1 }), 0);
  await flush();
  assert.deepEqual(received, []);
  assert.equal(admissionEvents.length, 1);
  assert.equal(admissionEvents[0].reason, 'acl');
  bus.destroy?.();
});

test("anonymousPolicy: 'allow' admits anonymous publishes under a constrained rule", async () => {
  const { bus } = aclBus([{ pattern: 'orders.**', publish: 'allow', publishers: ['svc-payments'] }], {
    anonymousPolicy: 'allow',
  });
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('orders.created', { a: 1 }), 1);
  // Unlisted identified publishers are still denied — the anonymous
  // policy only covers publishes with no publisher at all.
  assert.equal(bus.publish('orders.created', { b: 2 }, { publisherId: 'svc-other' }), 0);
  await flush();
  assert.deepEqual(received, [{ a: 1 }]);
  bus.destroy?.();
});

test("a 'deny' rule denies every publisher regardless of its publishers list", async () => {
  const { bus } = aclBus([{ pattern: 'admin.**', publish: 'deny', publishers: ['svc-admin'] }]);
  const received: unknown[] = [];
  bus.subscribe('admin.**', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('admin.cmd', { a: 1 }, { publisherId: 'svc-admin' }), 0);
  assert.equal(bus.publish('admin.cmd', { b: 2 }), 0);
  await flush();
  assert.deepEqual(received, []);
  bus.destroy?.();
});

test('the publisher dimension never affects subscribes', async () => {
  const { bus } = aclBus([
    { pattern: 'orders.**', publish: 'allow', publishers: ['svc-payments'] },
    { pattern: 'secret.**', subscribe: 'deny', publishers: ['svc-payments'] },
  ]);
  // Subscribing under a publishers-constrained publish rule works — the
  // subscribe decision ignores the publisher list.
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('orders.x', { a: 1 }, { publisherId: 'svc-payments' }), 1);
  await flush();
  assert.deepEqual(received, [{ a: 1 }]);
  // A subscribe deny still throws, publishers list or not.
  assert.throws(() => bus.subscribe('secret.**', () => {}), /ACL denied subscribe/);
  bus.destroy?.();
});

test('setAclRules takes effect immediately', async () => {
  const { bus } = aclBus([]);
  const received: unknown[] = [];
  bus.subscribe('orders.**', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('orders.x', { a: 1 }, { publisherId: 'svc-payments' }), 1);
  bus.setAclRules([{ pattern: 'orders.**', publish: 'allow', publishers: ['svc-other'] }]);
  assert.equal(bus.publish('orders.x', { b: 2 }, { publisherId: 'svc-payments' }), 0);
  assert.equal(bus.publish('orders.x', { c: 3 }, { publisherId: 'svc-other' }), 1);
  await flush();
  assert.deepEqual(received, [{ a: 1 }, { c: 3 }]);
  bus.destroy?.();
});

test('getAclRules round-trips the publishers list', () => {
  const { bus } = aclBus([
    { pattern: 'orders.**', publish: 'allow', publishers: ['svc-a', 'svc-b'] },
    { pattern: 'admin.**', publish: 'deny' },
  ]);
  assert.deepEqual(bus.getAclRules(), [
    { pattern: 'orders.**', publish: 'allow', publishers: ['svc-a', 'svc-b'] },
    { pattern: 'admin.**', publish: 'deny' },
  ]);
  bus.destroy?.();
});

test('anonymousPolicy does not apply when no rule matches (defaultPolicy still governs)', async () => {
  const { bus } = aclBus(
    [{ pattern: 'orders.**', publish: 'allow', publishers: ['svc-payments'] }],
    { defaultPolicy: 'allow' },
  );
  const received: unknown[] = [];
  bus.subscribe('other.**', (msg) => received.push(msg.payload));
  // No rule matches 'other.x': the default policy allows, and the
  // fail-closed anonymous policy does not override it.
  assert.equal(bus.publish('other.x', { a: 1 }), 1);
  await flush();
  assert.deepEqual(received, [{ a: 1 }]);
  bus.destroy?.();
});

test('invalid publisher ACL configuration throws RangeError', () => {
  const badRules: AclRule[][] = [
    [{ pattern: 'a.**', publish: 'allow', publishers: 'svc-a' as unknown as string[] }],
    [{ pattern: 'a.**', publish: 'allow', publishers: [''] }],
    [{ pattern: 'a.**', publish: 'allow', publishers: ['has space'] }],
    [{ pattern: 'a.**', publish: 'allow', publishers: ['toolong'.repeat(20)] }],
    [{ pattern: 'a.**', publish: 'allow', publishers: [42 as unknown as string] }],
    [{ pattern: 'a.**', publish: 'allow', publishers: [null as unknown as string] }],
  ];
  for (const rules of badRules) {
    assert.throws(() => new EventBus({ acl: { rules } }), RangeError);
    assert.throws(() => {
      const bus = new EventBus();
      try {
        bus.setAclRules(rules);
      } finally {
        bus.destroy?.();
      }
    }, RangeError);
  }
  assert.throws(
    () => new EventBus({ acl: { rules: [], anonymousPolicy: 'sometimes' as never } }),
    RangeError,
  );
  // An empty allowlist is valid: it allows no publisher (deny-all).
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'a.**', publish: 'allow', publishers: [] }] },
  });
  bus.subscribe('a.**', () => {});
  assert.equal(bus.publish('a.b', {}, { publisherId: 'svc-a' }), 0);
  bus.destroy?.();
});
