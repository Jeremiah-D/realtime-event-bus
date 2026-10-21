import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { SchemaMigrationRegistry } from '../src/schemamigrate.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-migrate-'));

/** Collects every delivered message's payload on the pattern. */
function collectPayloads(bus: EventBus, pattern: string, opts?: { schemaVersion?: number }) {
  const seen: BusMessage[] = [];
  bus.subscribe(
    pattern,
    (msg) => {
      seen.push(msg);
    },
    opts === undefined ? undefined : { schemaVersion: opts.schemaVersion },
  );
  return seen;
}

// ---------------------------------------------------------------------------
// SchemaMigrationRegistry unit tests
// ---------------------------------------------------------------------------

test('registry: stepwise upcast resolves 1->2->3 as a two-hop path', () => {
  const reg = new SchemaMigrationRegistry();
  reg.register(1, 2, (p) => ({ ...(p as object), v: 2 }));
  reg.register(2, 3, (p) => ({ ...(p as object), v: 3 }));
  const path = reg.resolvePath(1, 3)!;
  assert.equal(path.length, 2);
  assert.deepEqual(
    path.map((s) => [s.fromVersion, s.toVersion]),
    [
      [1, 2],
      [2, 3],
    ],
  );
  const out = reg.migrate({ v: 1 }, 1, 3);
  assert.equal(out.ok, true);
  assert.deepEqual(out.ok ? out.payload : null, { v: 3 });
});

test('registry: downcast resolves in reverse', () => {
  const reg = new SchemaMigrationRegistry();
  reg.register(3, 2, (p) => ({ ...(p as object), v: 2 }));
  reg.register(2, 1, (p) => ({ ...(p as object), v: 1 }));
  const out = reg.migrate({ v: 3 }, 3, 1);
  assert.equal(out.ok, true);
  assert.deepEqual(out.ok ? out.payload : null, { v: 1 });
});

test('registry: a direct hop short-circuits the stepwise chain', () => {
  const reg = new SchemaMigrationRegistry();
  reg.register(1, 2, (p) => ({ ...(p as object), via: 'step' }));
  reg.register(2, 3, (p) => ({ ...(p as object), via: 'step' }));
  reg.register(1, 3, (p) => ({ ...(p as object), via: 'direct' }));
  const out = reg.migrate({ v: 1 }, 1, 3);
  assert.equal(out.ok, true);
  assert.deepEqual(out.ok ? out.payload : null, { v: 1, via: 'direct' });
});

test('registry: missing hop fails closed with the unbridgeable hop reported', () => {
  const reg = new SchemaMigrationRegistry();
  reg.register(1, 2, (p) => p);
  const out = reg.migrate({ v: 1 }, 1, 3);
  assert.equal(out.ok, false);
  if (!out.ok) {
    assert.equal(out.missingFrom, 2);
    assert.equal(out.missingTo, 3);
  }
});

test('registry: a throwing hop fails closed like a missing hop', () => {
  const reg = new SchemaMigrationRegistry();
  reg.register(1, 2, () => {
    throw new Error('boom');
  });
  const out = reg.migrate({ v: 1 }, 1, 2);
  assert.equal(out.ok, false);
  if (!out.ok) {
    assert.equal(out.missingFrom, 1);
    assert.equal(out.missingTo, 2);
  }
});

test('registry: registration validates fail-fast with RangeError', () => {
  const reg = new SchemaMigrationRegistry();
  assert.throws(() => reg.register(-1, 2, (p) => p), RangeError);
  assert.throws(() => reg.register(1, 1.5, (p) => p), RangeError);
  assert.throws(() => reg.register(1, 1, (p) => p), RangeError);
  assert.throws(() => reg.register(1, 2, 'nope' as unknown as (p: unknown) => unknown), RangeError);
  reg.register(1, 2, (p) => p);
  assert.throws(() => reg.register(1, 2, (p) => p), RangeError);
});

// ---------------------------------------------------------------------------
// Bus integration: delivery-time migration
// ---------------------------------------------------------------------------

test('migration: subscriber-declared version receives stepwise-migrated payload', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  bus.setSchemaMigration('orders', 2, 3, (p) => ({ ...(p as Record<string, unknown>), v: 3 }));
  const seen = collectPayloads(bus, 'orders', { schemaVersion: 3 });
  const plain = collectPayloads(bus, 'orders');
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].payload, { v: 3, item: 'book' });
  // The write version is stamped on the delivered envelope.
  assert.equal(seen[0].schemaVersion, 1);
  // A subscriber that declares no version sees the raw write payload.
  assert.equal(plain.length, 1);
  assert.deepEqual(plain[0].payload, { v: 1, item: 'book' });
  assert.equal(bus.getStats().migrationFailures, 0);
});

test('migration: downcast migrates a newer write to an older expected version', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 2, 1, (p) => {
    const { v: _v, ...rest } = p as Record<string, unknown>;
    return { ...rest, v: 1 };
  });
  const seen = collectPayloads(bus, 'orders', { schemaVersion: 1 });
  bus.publish('orders', { v: 2, item: 'book', extra: true }, { schemaVersion: 2 });
  await flush();
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].payload, { v: 1, item: 'book', extra: true });
});

test('migration: missing hop drops fail-closed and counts migrationFailures', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => p);
  const declared = collectPayloads(bus, 'orders', { schemaVersion: 3 });
  const plain = collectPayloads(bus, 'orders');
  bus.publish('orders', { v: 1 }, { schemaVersion: 1 });
  await flush();
  // The version-declaring subscriber gets nothing: no hop 2->3 exists.
  assert.equal(declared.length, 0);
  // The undeclared subscriber is unaffected.
  assert.equal(plain.length, 1);
  assert.deepEqual(plain[0].payload, { v: 1 });
  // Counted per-topic and globally.
  assert.equal(bus.getStats().topics[0].migrationFailures, 1);
  assert.equal(bus.getStats().migrationFailures, 1);
  // Prometheus exposition carries the counter, global and per-topic.
  const exposition = renderPrometheus(bus.getStats());
  assert.match(exposition, /eventbus_schema_migration_failures_total 1/);
  assert.match(exposition, /eventbus_schema_migration_failures_total\{topic="orders"\} 1/);
});

test('migration: a throwing hop drops fail-closed and counts', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, () => {
    throw new Error('bad transform');
  });
  const seen = collectPayloads(bus, 'orders', { schemaVersion: 2 });
  bus.publish('orders', { v: 1 }, { schemaVersion: 1 });
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().migrationFailures, 1);
});

test('migration: equal versions and unknown write versions deliver untouched', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const same = collectPayloads(bus, 'orders', { schemaVersion: 1 });
  const declared2 = collectPayloads(bus, 'orders', { schemaVersion: 2 });
  bus.publish('orders', { v: 1 }, { schemaVersion: 1 });
  // No stamped write version (internal-style publish): delivers as-is.
  bus.publish('orders', { v: 9 });
  await flush();
  assert.deepEqual(same.map((m) => m.payload), [{ v: 1 }, { v: 9 }]);
  assert.deepEqual(declared2.map((m) => m.payload), [{ v: 2 }, { v: 9 }]);
  assert.equal(bus.getStats().migrationFailures, 0);
});

test('migration: applies after compression — the subscriber sees the migrated inflated payload', async () => {
  const bus = new EventBus();
  bus.setTopicCompression('big', { thresholdBytes: 100 });
  bus.setSchemaMigration('big', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const seen = collectPayloads(bus, 'big', { schemaVersion: 2 });
  const bigPayload = { v: 1, blob: 'x'.repeat(500) };
  bus.publish('big', bigPayload, { schemaVersion: 1 });
  await flush();
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].payload, { v: 2, blob: 'x'.repeat(500) });
  assert.equal(bus.getStats().migrationFailures, 0);
});

test('migration: verifySignature passes on the write-version wire bytes, then the payload migrates', async () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('orders', { keys: [{ kid: 'k1', secret: 's3cret' }] });
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  const seen: BusMessage[] = [];
  bus.subscribe(
    'orders',
    (msg) => {
      seen.push(msg);
    },
    { verifySignature: true, schemaVersion: 2 },
  );
  bus.publish('orders', { v: 1 }, { schemaVersion: 1 });
  await flush();
  // Verified against the stored write-version bytes, then migrated —
  // the signature check did not drop it and the handler saw v2.
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].payload, { v: 2 });
  assert.equal(bus.getStats().signatureFailures, 0);
  assert.equal(bus.getStats().migrationFailures, 0);
});

test('migration: wildcard registry resolves; exact-topic registry wins', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders.*', 1, 2, (p) => ({ ...(p as Record<string, unknown>), via: 'wild' }));
  bus.setSchemaMigration('orders.eu', 1, 2, (p) => ({ ...(p as Record<string, unknown>), via: 'exact' }));
  const eu = collectPayloads(bus, 'orders.eu', { schemaVersion: 2 });
  const us = collectPayloads(bus, 'orders.us', { schemaVersion: 2 });
  bus.publish('orders.eu', { v: 1 }, { schemaVersion: 1 });
  bus.publish('orders.us', { v: 1 }, { schemaVersion: 1 });
  await flush();
  assert.deepEqual(eu[0].payload, { v: 1, via: 'exact' });
  assert.deepEqual(us[0].payload, { v: 1, via: 'wild' });
});

test('migration: clearSchemaMigration removes the registry — later messages fail closed', async () => {
  const bus = new EventBus();
  bus.setSchemaMigration('orders', 1, 2, (p) => p);
  assert.equal(bus.clearSchemaMigration('orders'), true);
  assert.equal(bus.clearSchemaMigration('orders'), false);
  const seen = collectPayloads(bus, 'orders', { schemaVersion: 2 });
  bus.publish('orders', { v: 1 }, { schemaVersion: 1 });
  await flush();
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().migrationFailures, 1);
  assert.equal(bus.clearSchemaMigration(), false);
});

test('setSchemaMigration validates fail-fast with RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setSchemaMigration('', 1, 2, (p) => p), RangeError);
  assert.throws(() => bus.setSchemaMigration('t', -1, 2, (p) => p), RangeError);
  assert.throws(() => bus.setSchemaMigration('t', 1, 1.5, (p) => p), RangeError);
  assert.throws(() => bus.setSchemaMigration('t', 2, 2, (p) => p), RangeError);
  assert.throws(
    () => bus.setSchemaMigration('t', 1, 2, 'x' as unknown as (p: unknown) => unknown),
    RangeError,
  );
});

test('subscribe: schemaVersion validates fail-fast with RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.subscribe('t', () => {}, { schemaVersion: -1 }), RangeError);
  assert.throws(() => bus.subscribe('t', () => {}, { schemaVersion: 1.5 }), RangeError);
  assert.throws(
    () => bus.subscribe('t', () => {}, { schemaVersion: '2' as unknown as number }),
    RangeError,
  );
});

// ---------------------------------------------------------------------------
// Durable log: write version stored, replay migrates
// ---------------------------------------------------------------------------

test('migration: durable log stores the write version; replay migrates per subscriber', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setSchemaMigration('orders', 1, 2, (p) => ({ ...(p as Record<string, unknown>), v: 2 }));
  bus.publish('orders', { v: 1, item: 'book' }, { schemaVersion: 1 });
  await flush();
  // The log record carries the write version alongside the raw payload.
  const { DurableTopicLog } = await import('../src/durablelog.ts');
  const log = DurableTopicLog.open({ dir });
  const records = log.readSince('orders', 0);
  assert.equal(records.length, 1);
  assert.equal(records[0].schemaVersion, 1);
  assert.deepEqual(records[0].payload, { v: 1, item: 'book' });
  // A resuming subscriber declaring v2 gets the replayed message
  // migrated from the stored write-version bytes.
  const replayed: BusMessage[] = [];
  bus.subscribe(
    'orders',
    (msg) => {
      replayed.push(msg);
    },
    { resumeFromSeq: 0, schemaVersion: 2 },
  );
  await flush();
  assert.equal(replayed.length, 1);
  assert.deepEqual(replayed[0].payload, { v: 2, item: 'book' });
  assert.equal(replayed[0].schemaVersion, 1);
});

test('migration: durable log keeps version 0 records parseable across restart', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('v0', { v: 0 }, { schemaVersion: 0 });
  await flush();
  const { DurableTopicLog } = await import('../src/durablelog.ts');
  const log = DurableTopicLog.open({ dir });
  const records = log.readSince('v0', 0);
  // Version 0 is a legal version (EB-86): the record must survive the
  // strict log-line parse instead of being dropped as corrupt.
  assert.equal(records.length, 1);
  assert.equal(records[0].schemaVersion, 0);
});
