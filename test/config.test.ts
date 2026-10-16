import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, ConfigApplyError } from '../src/bus.ts';
import type { BusConfigSnapshot } from '../src/bus.ts';

/**
 * Declarative configuration snapshots (EB-91): export / dry-run /
 * apply / rollback / drift detection.
 */

function emptySnapshot(): BusConfigSnapshot {
  return new EventBus().exportBusConfig();
}

test('exportBusConfig on a fresh bus is a versioned empty snapshot', () => {
  const snap = emptySnapshot();
  assert.equal(snap.v, 1);
  assert.equal(typeof snap.exportedAt, 'number');
  assert.deepEqual(snap.acl, []);
  assert.deepEqual(snap.namespaces, {});
  assert.deepEqual(snap.publisherRateLimits, {});
  assert.deepEqual(snap.topicRules, {});
  assert.equal(snap.durableLogMaxAgeMs, undefined);
  // JSON-safe: survives a serialize round-trip byte-identically.
  assert.deepEqual(JSON.parse(JSON.stringify(snap)), snap);
});

test('export captures every covered rule table', () => {
  const bus = new EventBus();
  bus.setAclRules([{ pattern: 'market.**', publish: 'allow', subscribe: 'allow' }]);
  bus.createNamespace('t1', { allowPublish: true, allowSubscribe: false });
  bus.setPublisherRateLimit('svc-a', { messagesPerSec: 10, burst: 20 });
  bus.setTopicTtl('market.**', 60_000);
  bus.setTopicRetained('state.**', true);
  bus.setTopicRateLimit('hot.**', 100, { burst: 10 });
  bus.setTopicMaxSubscribers('hot.**', 5);
  bus.setTopicAllowedLateness('late.**', 5000);
  bus.setTopicCompression('logs.**', { thresholdBytes: 1024, level: 3 });
  bus.setTopicSigningKey('pay.**', {
    keys: [{ kid: 'k1', secret: Buffer.from('topsecret-topsecret-topsecret-00') }],
    graceMs: 1000,
  });
  bus.setTopicAlias('old.topic', 'new.topic');
  bus.setTopicRoute('src.topic', 'dst.topic');
  bus.setTopicPartitions('part.**', 4);

  const snap = bus.exportBusConfig();
  assert.deepEqual(snap.acl, [{ pattern: 'market.**', publish: 'allow', subscribe: 'allow' }]);
  assert.deepEqual(snap.namespaces, { t1: { allowPublish: true, allowSubscribe: false } });
  assert.deepEqual(snap.publisherRateLimits, { 'svc-a': { messagesPerSec: 10, burst: 20 } });
  assert.equal(snap.topicRules['market.**'].ttlMs, 60_000);
  assert.equal(snap.topicRules['state.**'].retained, true);
  assert.deepEqual(snap.topicRules['hot.**'].rateLimit, { messagesPerSec: 100, burst: 10 });
  assert.equal(snap.topicRules['hot.**'].maxSubscribers, 5);
  assert.equal(snap.topicRules['late.**'].allowedLatenessMs, 5000);
  assert.deepEqual(snap.topicRules['logs.**'].compression, { thresholdBytes: 1024, level: 3 });
  assert.equal(snap.topicRules['part.**'].partitions, 4);
  assert.deepEqual(snap.topicRules['old.topic'].alias, { newTopic: 'new.topic' });
  assert.deepEqual(snap.topicRules['src.topic'].route, { dst: 'dst.topic' });
});

test('exported signing keys carry metadata only — never key material', () => {
  const bus = new EventBus();
  const secret = Buffer.from('super-secret-key-material-0000');
  bus.setTopicSigningKey('pay.**', { keys: [{ kid: 'k1', secret }], graceMs: 500 });
  const snap = bus.exportBusConfig();
  const signingKeys = snap.topicRules['pay.**'].signingKeys;
  assert.deepEqual(signingKeys, { keyIds: ['k1'], graceMs: 500 });
  assert.ok(!JSON.stringify(snap).includes(secret.toString('utf8')));
  assert.ok(!JSON.stringify(snap).includes('super-secret'));
});

test('dryRun previews the change list without mutating anything', () => {
  const bus = new EventBus();
  bus.setTopicTtl('a.**', 1000);
  const target: BusConfigSnapshot = {
    ...bus.exportBusConfig(),
    topicRules: {
      'a.**': { ttlMs: 2000 },
      'b.**': { retained: true },
    },
  };
  const result = bus.applyConfig(target, { dryRun: true });
  assert.equal(result.dryRun, true);
  assert.deepEqual(result.applied, []);
  assert.ok(result.changes !== undefined && result.changes.length === 2);
  const paths = result.changes.map((c) => c.path).sort();
  assert.deepEqual(paths, ['topicRules["a.**"].ttlMs', 'topicRules["b.**"].retained']);
  // Nothing mutated.
  assert.deepEqual(bus.exportBusConfig().topicRules, { 'a.**': { ttlMs: 1000 } });
  // No audit noise from a dry run.
  assert.deepEqual(
    bus.getSubscriptionAudit().filter((e) => e.action === 'config_applied'),
    [],
  );
});

test('applyConfig applies set/modify/delete across sections and audits', () => {
  const bus = new EventBus();
  bus.setTopicTtl('gone.**', 1000);
  bus.setTopicRateLimit('gone.**', 50, { burst: 5 });
  bus.createNamespace('old-ns');

  const target: BusConfigSnapshot = {
    v: 1,
    exportedAt: 1,
    acl: [{ pattern: 'secure.**', publish: 'deny' }],
    namespaces: { 'new-ns': { allowPublish: true, allowSubscribe: true } },
    publisherRateLimits: { 'svc-b': { messagesPerSec: 5, burst: 5 } },
    topicRules: {
      'keep.**': { ttlMs: 9000, partitions: 2 },
      'gone.**': {}, // clears both rules on this pattern
    },
  };
  const result = bus.applyConfig(target);
  assert.equal(result.dryRun, false);
  assert.ok(result.applied.length > 0);
  assert.ok(result.applied.includes('acl'));
  assert.ok(result.applied.includes('namespaces["new-ns"]'));
  assert.ok(result.applied.includes('namespaces["old-ns"]'));
  assert.ok(result.applied.includes('topicRules["gone.**"].ttlMs'));
  assert.ok(result.applied.includes('topicRules["gone.**"].rateLimit'));

  const snap = bus.exportBusConfig();
  assert.deepEqual(snap.acl, [{ pattern: 'secure.**', publish: 'deny' }]);
  assert.deepEqual(Object.keys(snap.namespaces), ['new-ns']);
  assert.deepEqual(snap.publisherRateLimits, { 'svc-b': { messagesPerSec: 5, burst: 5 } });
  assert.deepEqual(snap.topicRules['keep.**'], { ttlMs: 9000, partitions: 2 });
  assert.deepEqual(snap.topicRules['gone.**'] ?? {}, {});

  const audit = bus
    .getSubscriptionAudit()
    .filter((e) => e.action === 'config_applied');
  assert.equal(audit.length, 1);
  assert.deepEqual((audit[0].detail as { applied: string[] }).applied, result.applied);
});

test('applyConfig round-trips: exporting and re-applying is a no-op', () => {
  const bus = new EventBus();
  bus.setTopicTtl('m.**', 3000);
  bus.setTopicCompression('m.**', { thresholdBytes: 512 });
  bus.setPublisherRateLimit('p1', { messagesPerSec: 7, burst: 7 });
  const snap = bus.exportBusConfig();
  const result = bus.applyConfig(JSON.parse(JSON.stringify(snap)));
  assert.deepEqual(result.applied, []);
  const { exportedAt: _r1, ...afterSnap } = bus.exportBusConfig();
  const { exportedAt: _r2, ...snapRest } = snap;
  assert.deepEqual(afterSnap, snapRest);
});

test('a mid-apply failure rolls everything back and throws ConfigApplyError', () => {
  const bus = new EventBus();
  bus.setTopicTtl('a-stable.**', 1000);
  const before = bus.exportBusConfig();

  // Passes shape validation (finite number) but the setter rejects it.
  // 'a-stable.**' sorts before 'z-broken.**', so one change applies
  // before the failure — the rollback must undo it.
  const target: BusConfigSnapshot = {
    ...before,
    topicRules: {
      'a-stable.**': { ttlMs: 2000 }, // applies fine
      'z-broken.**': { ttlMs: -1 }, // RangeError at the setter
    },
  };
  assert.throws(() => bus.applyConfig(target), (err) => {
    assert.ok(err instanceof ConfigApplyError);
    assert.equal(err.failedPath, 'topicRules["z-broken.**"].ttlMs');
    assert.deepEqual(err.applied, ['topicRules["a-stable.**"].ttlMs']);
    assert.equal(err.rolledBack, true);
    return true;
  });
  // The bus is exactly as before the call (modulo the export timestamp).
  const { exportedAt: _e1, ...beforeState } = before;
  const { exportedAt: _e2, ...afterState } = bus.exportBusConfig();
  assert.deepEqual(afterState, beforeState);

  const rolledBack = bus
    .getSubscriptionAudit()
    .filter((e) => e.action === 'config_rolled_back');
  assert.equal(rolledBack.length, 1);
  assert.equal((rolledBack[0].detail as { rolledBack: boolean }).rolledBack, true);
});

test('applyConfig refuses to change signing keys from a snapshot (fail-closed)', () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('pay.**', {
    keys: [{ kid: 'k1', secret: Buffer.from('super-secret-key-material-0000') }],
    graceMs: 500,
  });
  const before = bus.exportBusConfig();
  const target: BusConfigSnapshot = {
    ...before,
    topicRules: { 'pay.**': { signingKeys: { keyIds: ['k2'], graceMs: 500 } } },
  };
  assert.throws(() => bus.applyConfig(target), ConfigApplyError);
  // Unchanged apart from the export timestamp — the live keys were never touched.
  const { exportedAt: _k2, ...beforeKeys } = before;
  const { exportedAt: _k3, ...afterKeys } = bus.exportBusConfig();
  assert.deepEqual(afterKeys, beforeKeys);
});

test('applyConfig refuses predicate-bearing routes (fail-closed)', () => {
  const bus = new EventBus();
  bus.setTopicRoute('src.**', 'dst.old', { predicate: () => true });
  const before = bus.exportBusConfig();
  const target: BusConfigSnapshot = {
    ...before,
    topicRules: { 'src.**': { route: { dst: 'dst.new' } } },
  };
  assert.throws(() => bus.applyConfig(target), ConfigApplyError);
  // Unchanged apart from the export timestamp — the live route (and its
  // predicate) was never touched.
  const { exportedAt: _b, ...beforeRest } = before;
  const { exportedAt: _a, ...afterRest } = bus.exportBusConfig();
  assert.deepEqual(afterRest, beforeRest);
});

test('malformed snapshots fail fast before anything is mutated', () => {
  const bus = new EventBus();
  bus.setTopicTtl('a.**', 1000);
  const before = bus.exportBusConfig();
  assert.throws(() => bus.applyConfig({ v: 999 } as never), RangeError);
  assert.throws(() => bus.applyConfig({ v: 1 } as never), TypeError);
  const { exportedAt: _m1, ...afterState } = bus.exportBusConfig();
  const { exportedAt: _m2, ...beforeRest } = before;
  assert.deepEqual(afterState, beforeRest);
});

test('detectConfigDrift reports runtime changes against the last applied snapshot', () => {
  const bus = new EventBus();
  const target: BusConfigSnapshot = {
    v: 1,
    exportedAt: 1,
    acl: [],
    namespaces: {},
    publisherRateLimits: {},
    topicRules: { 'm.**': { ttlMs: 1000 } },
  };
  bus.applyConfig(target);
  assert.deepEqual(bus.detectConfigDrift(), []);

  // Drift: imperative change after the apply.
  bus.setTopicTtl('m.**', 2000);
  bus.setTopicRetained('other.**', true);
  const drift = bus.detectConfigDrift();
  const byPath = new Map(drift.map((d) => [d.path, d]));
  assert.equal(byPath.get('topicRules["m.**"].ttlMs')?.expected, 1000);
  assert.equal(byPath.get('topicRules["m.**"].ttlMs')?.actual, 2000);
  assert.equal(byPath.get('topicRules["other.**"].retained')?.expected, undefined);
  assert.equal(byPath.get('topicRules["other.**"].retained')?.actual, true);

  // Explicit baseline also works.
  const explicit = bus.detectConfigDrift(target);
  assert.ok(explicit.length >= 2);
});

test('detectConfigDrift without any baseline throws a clear error', () => {
  const bus = new EventBus();
  assert.throws(() => bus.detectConfigDrift(), /no baseline snapshot/);
});

test('namespace modify round-trips through delete+create', () => {
  const bus = new EventBus();
  bus.applyConfig({
    v: 1,
    exportedAt: 1,
    acl: [],
    namespaces: { t1: { allowPublish: true, allowSubscribe: true } },
    publisherRateLimits: {},
    topicRules: {},
  });
  assert.deepEqual(bus.exportBusConfig().namespaces, {
    t1: { allowPublish: true, allowSubscribe: true },
  });
  // Modify: flip allowSubscribe.
  const snap = bus.exportBusConfig();
  snap.namespaces.t1.allowSubscribe = false;
  bus.applyConfig(snap);
  assert.deepEqual(bus.exportBusConfig().namespaces, {
    t1: { allowPublish: true, allowSubscribe: false },
  });
});

test('alias expiry survives an export/apply round-trip as a TTL', () => {
  const clock = { nowMs: 10_000, now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  bus.setTopicAlias('old.t', 'new.t', { ttlMs: 5000 });
  const snap = bus.exportBusConfig();
  assert.equal(snap.topicRules['old.t'].alias?.newTopic, 'new.t');
  assert.equal(snap.topicRules['old.t'].alias?.expiresAt, 15_000);

  clock.nowMs = 12_000;
  bus.applyConfig(JSON.parse(JSON.stringify(snap)));
  const after = bus.exportBusConfig();
  // Re-registered with ~3s remaining.
  assert.ok(
    (after.topicRules['old.t'].alias?.expiresAt ?? 0) >= 14_900 &&
      (after.topicRules['old.t'].alias?.expiresAt ?? 0) <= 15_000,
  );
});
