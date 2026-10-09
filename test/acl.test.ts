import test from 'node:test';
import assert from 'node:assert/strict';
import {
  EventBus,
  AclDeniedError,
  patternsOverlap,
  type AclRule,
  type AdmissionRejectionEvent,
  type AuthzDeniedEvent,
} from '../src/bus.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('patternsOverlap: segment wildcard overlap semantics', () => {
  assert.equal(patternsOverlap('a.b', 'a.b'), true);
  assert.equal(patternsOverlap('a.b', 'a.c'), false);
  assert.equal(patternsOverlap('market.**', 'market.btc'), true);
  assert.equal(patternsOverlap('market.**', 'market'), true); // ** matches zero segments
  assert.equal(patternsOverlap('admin.**', '**'), true);
  assert.equal(patternsOverlap('**', '**'), true);
  assert.equal(patternsOverlap('market.*', 'market.btc.trades'), false);
  assert.equal(patternsOverlap('**.btc', 'market.btc'), true);
  assert.equal(patternsOverlap('a.*.c', 'a.b.c'), true);
  assert.equal(patternsOverlap('a.*.c', 'a.b.d'), false);
  assert.equal(patternsOverlap('a.**', 'b.**'), false);
  assert.equal(patternsOverlap('a.**.**.b', 'a.x.y.b'), true); // collapsed like compilePattern
  assert.equal(patternsOverlap('a.**.b', 'a.b'), true);
  assert.equal(patternsOverlap('*', 'anything.at.all'), false);
  assert.equal(patternsOverlap('**', 'anything.at.all'), true);
});

test('no ACL configured: everything behaves as before', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('**', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('admin.cmd', { a: 1 }), 1);
  await flush();
  assert.deepEqual(received, [{ a: 1 }]);
  assert.equal(bus.getStats().authzDenied, 0);
  bus.destroy?.();
});

test('publish denied by ACL: returns 0, counted as rejection, audited', async () => {
  const admissionEvents: AdmissionRejectionEvent[] = [];
  const authzEvents: AuthzDeniedEvent[] = [];
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
    onAdmissionRejected: (e) => admissionEvents.push(e),
    onAuthzDenied: (e) => authzEvents.push(e),
  });
  const received: unknown[] = [];
  bus.subscribe('admin.**', (msg) => received.push(msg.payload));

  assert.equal(bus.publish('admin.cmd', { a: 1 }), 0);
  assert.equal(bus.publish('market.btc', { b: 2 }), 0); // no subscriber, but admitted
  await flush();
  assert.deepEqual(received, []);

  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 'admin.cmd')!;
  assert.equal(topic.rejectedMessages, 1);
  assert.equal(topic.publishedMessages, 0);
  assert.equal(topic.lastSeq, 0); // denial consumes no sequence number
  assert.equal(stats.rejectedMessages, 1);
  assert.equal(stats.totalPublished, 1); // the admitted market.btc publish (no subscribers)
  assert.equal(stats.authzDenied, 1);

  assert.equal(admissionEvents.length, 1);
  assert.equal(admissionEvents[0].reason, 'acl');
  assert.equal(admissionEvents[0].topic, 'admin.cmd');
  assert.equal(authzEvents.length, 1);
  assert.deepEqual(
    { action: authzEvents[0].action, topic: authzEvents[0].topic },
    { action: 'publish', topic: 'admin.cmd' }
  );
  bus.destroy?.();
});

test('ACL runs before schema validation', async () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
  });
  bus.setTopicSchema('admin.**', () => false); // would reject too — ACL wins
  const events: AdmissionRejectionEvent[] = [];
  const bus2 = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
    onAdmissionRejected: (e) => events.push(e),
  });
  bus2.setTopicSchema('admin.**', () => false);
  assert.equal(bus.publish('admin.x', {}), 0);
  assert.equal(bus2.publish('admin.x', {}), 0);
  assert.equal(events[0].reason, 'acl');
  bus.destroy?.();
  bus2.destroy?.();
});

test('first matching rule in order wins', () => {
  const allowFirst = new EventBus({
    acl: { rules: [{ pattern: '**', publish: 'allow' }, { pattern: 'admin.**', publish: 'deny' }] },
  });
  assert.equal(allowFirst.publish('admin.cmd', {}), 0); // admitted: no subscriber, not denied
  assert.equal(allowFirst.getStats().authzDenied, 0);

  const denyFirst = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }, { pattern: '**', publish: 'allow' }] },
  });
  assert.equal(denyFirst.publish('admin.cmd', {}), 0);
  assert.equal(denyFirst.getStats().authzDenied, 1);
  assert.equal(denyFirst.getStats().rejectedMessages, 1);
  allowFirst.destroy?.();
  denyFirst.destroy?.();
});

test("defaultPolicy 'deny' turns the rule set into a whitelist", () => {
  const bus = new EventBus({
    acl: {
      defaultPolicy: 'deny',
      rules: [{ pattern: 'market.**', publish: 'allow', subscribe: 'allow' }],
    },
  });
  assert.equal(bus.publish('market.btc', {}), 0); // admitted
  assert.equal(bus.publish('other.topic', {}), 0); // denied
  assert.equal(bus.getStats().authzDenied, 1);
  bus.subscribe('market.btc', () => {});
  assert.throws(() => bus.subscribe('other.topic', () => {}), AclDeniedError);
  assert.equal(bus.getStats().authzDenied, 2);
  bus.destroy?.();
});

test('subscribe denied: throws AclDeniedError, audited, nothing registered', () => {
  const authzEvents: AuthzDeniedEvent[] = [];
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', subscribe: 'deny' }] },
    onAuthzDenied: (e) => authzEvents.push(e),
  });
  const before = bus.getStats().totalSubscribers;

  assert.throws(() => bus.subscribe('admin.cmd', () => {}), (err: unknown) => {
    assert.ok(err instanceof AclDeniedError);
    assert.equal(err.name, 'AclDeniedError');
    assert.equal(err.pattern, 'admin.cmd');
    assert.match((err as Error).message, /ACL denied subscribe on pattern "admin\.cmd"/);
    return true;
  });
  // Overlap: the deny rule covers part of this subscription's scope, so the
  // whole subscription is denied — a broad pattern cannot slip past it.
  assert.throws(() => bus.subscribe('**', () => {}), AclDeniedError);
  assert.throws(() => bus.subscribe('admin.*', () => {}), AclDeniedError);
  // Unaffected scope still subscribes fine.
  bus.subscribe('market.btc', () => {});

  assert.equal(bus.getStats().totalSubscribers, before + 1);
  assert.equal(bus.getStats().authzDenied, 3);
  assert.equal(authzEvents.length, 3);
  assert.deepEqual(
    authzEvents.map((e) => [e.action, e.pattern]),
    [
      ['subscribe', 'admin.cmd'],
      ['subscribe', '**'],
      ['subscribe', 'admin.*'],
    ]
  );
  bus.destroy?.();
});

test('a throwing onAuthzDenied hook never disturbs the path', () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny', subscribe: 'deny' }] },
    onAuthzDenied: () => {
      throw new Error('broken observer');
    },
  });
  assert.equal(bus.publish('admin.cmd', {}), 0); // denial still counted
  assert.equal(bus.getStats().authzDenied, 1);
  assert.throws(() => bus.subscribe('admin.cmd', () => {}), AclDeniedError);
  assert.equal(bus.getStats().authzDenied, 2);
  bus.destroy?.();
});

test('setAclRules takes effect immediately; getAclRules returns a copy', async () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny', subscribe: 'deny' }] },
  });
  assert.equal(bus.publish('admin.cmd', {}), 0);
  assert.throws(() => bus.subscribe('admin.cmd', () => {}), AclDeniedError);

  bus.setAclRules([]);
  assert.equal(bus.publish('admin.cmd', {}), 0); // admitted now
  bus.subscribe('admin.cmd', () => {});
  assert.equal(bus.getStats().authzDenied, 2); // only the earlier denials

  bus.setAclRules([{ pattern: 'admin.**', publish: 'deny' }]);
  assert.equal(bus.publish('admin.cmd', {}), 0);
  assert.equal(bus.getStats().authzDenied, 3);

  const rules = bus.getAclRules();
  assert.deepEqual(rules, [{ pattern: 'admin.**', publish: 'deny' }]);
  rules.push({ pattern: '**', publish: 'allow' });
  assert.equal(bus.getAclRules().length, 1); // the copy is detached
  bus.destroy?.();
});

test('publishAtomic: an ACL-denied entry aborts the whole batch', async () => {
  const authzEvents: AuthzDeniedEvent[] = [];
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
    onAuthzDenied: (e) => authzEvents.push(e),
  });
  const received: unknown[] = [];
  bus.subscribe('market.**', (msg) => received.push(msg.payload));

  const result = bus.publishAtomic([
    { topic: 'market.btc', payload: 1 },
    { topic: 'admin.cmd', payload: 2 },
  ]);
  assert.deepEqual(result, {
    published: 0,
    rejected: { index: 1, topic: 'admin.cmd', reason: 'acl' },
  });
  await flush();
  assert.deepEqual(received, []); // nothing committed
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 1);
  assert.equal(stats.authzDenied, 1);
  assert.equal(stats.totalPublished, 0);
  assert.equal(authzEvents.length, 1);
  bus.destroy?.();
});

test('publishDelayed denied by ACL: fail-fast, nothing scheduled', async () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
  });
  assert.equal(bus.publishDelayed('admin.cmd', {}, { delayMs: 10 }), undefined);
  assert.equal(bus.getStats().rejectedMessages, 1);
  assert.equal(bus.getStats().authzDenied, 1);
  assert.equal(bus.getStats().pendingDelayed ?? 0, 0);
  bus.destroy?.();
});

test('publishIdempotent denied by ACL claims no dedup slot', () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
    idempotencyWindowMs: 60_000,
  });
  const denied = bus.publishIdempotent('admin.cmd', { n: 1 }, { messageId: 'm1' });
  assert.deepEqual(denied, { duplicate: false, accepted: 0 });
  assert.equal(bus.getStats().authzDenied, 1);

  // Lifting the ACL: the retry is a fresh publish, not a phantom duplicate.
  bus.setAclRules([]);
  const retry = bus.publishIdempotent('admin.cmd', { n: 1 }, { messageId: 'm1' });
  assert.deepEqual(retry, { duplicate: false, accepted: 0 }); // admitted (no subscriber)
  assert.equal(bus.getStats().totalPublished, 1);
  bus.destroy?.();
});

test('ACL rule validation fails fast at configuration time', () => {
  const bad: Array<{ rules: AclRule[]; match: RegExp }> = [
    { rules: [{ pattern: '', publish: 'deny' }], match: /pattern must be a non-empty string/ },
    {
      rules: [{ pattern: 'a', publish: 'maybe' as never }],
      match: /publish must be 'allow' or 'deny'/,
    },
    {
      rules: [{ pattern: 'a', subscribe: 'sometimes' as never }],
      match: /subscribe must be 'allow' or 'deny'/,
    },
    { rules: [{ pattern: 'a' }], match: /must decide at least one of publish\/subscribe/ },
    { rules: [null as never], match: /must be an object/ },
  ];
  for (const { rules, match } of bad) {
    assert.throws(() => new EventBus({ acl: { rules } }), match);
    assert.throws(() => new EventBus().setAclRules(rules), match);
  }
  assert.throws(() => new EventBus({ acl: { rules: 'nope' as never } }), /must be an array/);
  assert.throws(
    () => new EventBus({ acl: { defaultPolicy: 'maybe' as never } }),
    /defaultPolicy must be 'allow' or 'deny'/
  );
  assert.throws(
    () => new EventBus({ onAuthzDenied: 'nope' as never }),
    /onAuthzDenied must be a function/
  );
  // setAclRules with bad rules replaces nothing.
  const bus = new EventBus({ acl: { rules: [{ pattern: 'a.**', publish: 'deny' }] } });
  assert.throws(() => bus.setAclRules([{ pattern: '', publish: 'deny' }]), /non-empty string/);
  assert.deepEqual(bus.getAclRules(), [{ pattern: 'a.**', publish: 'deny' }]);
  bus.destroy?.();
});

test('subscribe-family wrappers inherit the ACL (subscribeReliable)', async () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', subscribe: 'deny' }] },
  });
  assert.throws(
    () => bus.subscribeReliable('admin.cmd', () => {}, { ackTimeoutMs: 100 }),
    AclDeniedError
  );
  assert.equal(bus.getStats().authzDenied, 1);
  bus.destroy?.();
});
