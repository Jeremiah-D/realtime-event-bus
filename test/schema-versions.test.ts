import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const isV0 = (payload: unknown) => (payload as { v?: number })?.v === 0;
const isV1 = (payload: unknown) => (payload as { v?: number })?.v === 1;

test('EB-86: setTopicSchemaVersion validates its arguments', () => {
  const bus = new EventBus();
  const v = () => true;
  assert.throws(() => bus.setTopicSchemaVersion('', 0, v), RangeError);
  assert.throws(() => bus.setTopicSchemaVersion('t', -1, v), RangeError);
  assert.throws(() => bus.setTopicSchemaVersion('t', 1.5, v), RangeError);
  assert.throws(() => bus.setTopicSchemaVersion('t', 0, 'x' as never), RangeError);
  assert.throws(() => bus.setTopicSchemaVersion('t', 0, v, { graceMs: -1 }), RangeError);
  assert.throws(() => bus.setTopicSchemaVersion('t', 0, v, { graceMs: Number.NaN }), RangeError);
  assert.throws(
    () => bus.setTopicSchemaVersion('t', 0, v, { graceMs: Number.POSITIVE_INFINITY }),
    RangeError,
  );
  bus.setTopicSchemaVersion('t', 0, v);
  assert.throws(() => bus.setTopicSchemaVersion('t', 0, v), RangeError);
  // A different version on the same key is fine.
  bus.setTopicSchemaVersion('t', 1, v);
  // clearTopicSchemaVersion validation is lenient on unknown keys.
  assert.equal(bus.clearTopicSchemaVersion('nope'), false);
  assert.equal(bus.clearTopicSchemaVersion('t', 99), false);
});

test('EB-86: publish without schemaVersion validates against the latest', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1);
  assert.equal(bus.publish('t', { v: 1 }), 1);
  // v0 payload no longer validates under v1 (grace defaults to 0).
  assert.equal(bus.publish('t', { v: 0 }), 0);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].rejectedMessages, 1);
  assert.equal(stats.topics[0].deprecatedSchemaVersions, 0);
});

test('EB-86: explicit old version inside grace is admitted and counted deprecated', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload));
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  assert.equal(bus.publish('t', { v: 0 }, { schemaVersion: 0 }), 1);
  await flush();
  assert.deepEqual(seen, [{ v: 0 }]);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].deprecatedSchemaVersions, 1);
  assert.equal(stats.deprecatedSchemaVersions, 1);
  assert.equal(stats.topics[0].rejectedMessages, 0);
  // The old validator still decides: a v1 payload under version 0 fails.
  assert.equal(bus.publish('t', { v: 1 }, { schemaVersion: 0 }), 0);
  assert.equal(bus.getStats().topics[0].rejectedMessages, 1);
  assert.equal(bus.getStats().topics[0].deprecatedSchemaVersions, 1);
});

test('EB-86: old version past its grace window is rejected without consuming seq', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  clock.nowMs = 1_000 + 60_001;
  assert.equal(bus.publish('t', { v: 0 }, { schemaVersion: 0 }), 0);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].rejectedMessages, 1);
  assert.equal(stats.topics[0].deprecatedSchemaVersions, 0);
  assert.equal(stats.topics[0].lastSeq, 0);
  assert.equal(stats.topics[0].sequenceGaps, 0);
});

test('EB-86: grace window boundary is inclusive', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  clock.nowMs = 1_000 + 60_000;
  assert.equal(bus.publish('t', { v: 0 }, { schemaVersion: 0 }), 1);
  assert.equal(bus.getStats().topics[0].deprecatedSchemaVersions, 1);
});

test('EB-86: unknown schema version is rejected', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  assert.equal(bus.publish('t', { v: 0 }, { schemaVersion: 5 }), 0);
  assert.equal(bus.getStats().topics[0].rejectedMessages, 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('EB-86: invalid schemaVersion on publish throws before anything mutates', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  for (const bad of [-1, 1.5, Number.NaN, '0' as unknown as number]) {
    assert.throws(() => bus.publish('t', {}, { schemaVersion: bad }), RangeError);
    assert.throws(() => bus.publishBatch([{ topic: 't', payload: {}, schemaVersion: bad }]), RangeError);
    assert.throws(() => bus.publishAtomic([{ topic: 't', payload: {}, schemaVersion: bad }]), RangeError);
    assert.throws(() => bus.publishDelayed('t', {}, { delayMs: 10, schemaVersion: bad }), RangeError);
  }
  assert.equal(bus.getStats().topics.length, 0);
});

test('EB-86: schemaVersion is ignored when the topic has no versioned validators', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload));
  // Legacy single-version validator still applies.
  bus.setTopicSchema('t', (p) => p === 'ok');
  assert.equal(bus.publish('t', 'ok', { schemaVersion: 3 }), 1);
  assert.equal(bus.publish('t', 'bad', { schemaVersion: 3 }), 0);
  await flush();
  assert.deepEqual(seen, ['ok']);
});

test('EB-86: versioned registrations take precedence over the legacy validator', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchema('t', () => false);
  bus.setTopicSchemaVersion('t', 0, () => true);
  assert.equal(bus.publish('t', 'anything'), 1);
  // Clearing the versioned set restores the legacy validator.
  assert.equal(bus.clearTopicSchemaVersion('t'), true);
  assert.equal(bus.publish('t', 'anything'), 0);
});

test('EB-86: pattern rule sets resolve like the legacy schema rules', () => {
  const bus = new EventBus();
  bus.subscribe('orders.eu', () => {});
  bus.subscribe('orders.us', () => {});
  bus.setTopicSchemaVersion('orders.*', 0, isV0);
  bus.setTopicSchemaVersion('orders.eu', 0, isV1);
  // Exact-topic rule set wins over the pattern.
  assert.equal(bus.publish('orders.eu', { v: 1 }, { schemaVersion: 0 }), 1);
  assert.equal(bus.publish('orders.us', { v: 0 }, { schemaVersion: 0 }), 1);
  assert.equal(bus.publish('orders.us', { v: 1 }, { schemaVersion: 0 }), 0);
});

test('EB-86: getStats().topicSchemas exposes the registry without validators', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  assert.deepEqual(bus.getStats().topicSchemas, []);
  bus.setTopicSchemaVersion('t', 0, isV0);
  clock.nowMs = 2_000;
  bus.setTopicSchemaVersion('t', 2, isV1, { graceMs: 5_000 });
  const schemas = bus.getStats().topicSchemas;
  assert.equal(schemas.length, 1);
  assert.equal(schemas[0].rule, 't');
  assert.equal(schemas[0].latest, 2);
  assert.deepEqual(
    schemas[0].versions.map((v) => v.version),
    [0, 2],
  );
  const v0 = schemas[0].versions[0];
  assert.equal(v0.registeredAt, 1_000);
  // v0's effective window: granted by v1's registration (the migration
  // action declares the window).
  assert.equal(v0.graceMs, 5_000);
  assert.equal(v0.supersededAt, 2_000);
  const v2 = schemas[0].versions[1];
  // v2 is latest: its graceMs is the dormant grant it will give when a
  // newer version supersedes it.
  assert.equal(v2.graceMs, 5_000);
  assert.equal(v2.supersededAt, undefined);
  // Validators (functions) are never exposed.
  assert.ok(!JSON.stringify(schemas).includes('isV0'));
});

test('EB-86: clearTopicSchemaVersion removes one version or the whole set', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  assert.equal(bus.clearTopicSchemaVersion('t', 0), true);
  // v0 is gone: now an unknown version.
  assert.equal(bus.publish('t', { v: 0 }, { schemaVersion: 0 }), 0);
  // Removing the last version deletes the set; the topic falls back to
  // no validation.
  assert.equal(bus.clearTopicSchemaVersion('t', 1), true);
  assert.deepEqual(bus.getStats().topicSchemas, []);
  assert.equal(bus.publish('t', { v: 0 }), 1);
});

test('EB-86: publishAtomic counts a deprecated admission exactly once', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  const result = bus.publishAtomic([
    { topic: 't', payload: { v: 0 }, schemaVersion: 0 },
    { topic: 't', payload: { v: 1 } },
  ]);
  assert.deepEqual(result, { published: 2 });
  const stats = bus.getStats();
  assert.equal(stats.topics[0].deprecatedSchemaVersions, 1);
  assert.equal(stats.deprecatedSchemaVersions, 1);
  // An aborted batch counts nothing: the failing entry is rejected once.
  const bad = bus.publishAtomic([
    { topic: 't', payload: { v: 0 }, schemaVersion: 0 },
    { topic: 't', payload: { v: 9 }, schemaVersion: 9 },
  ]);
  assert.equal(bad.published, 0);
  assert.equal(bad.rejected?.reason, 'schema');
  assert.equal(bus.getStats().topics[0].deprecatedSchemaVersions, 1);
});

test('EB-86: publishBatch carries per-message schema versions', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload));
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  const accepted = bus.publishBatch([
    { topic: 't', payload: { v: 0 }, schemaVersion: 0 },
    { topic: 't', payload: { v: 0 } }, // latest (v1) rejects it
    { topic: 't', payload: { v: 1 } },
  ]);
  assert.equal(accepted, 2);
  await flush();
  assert.deepEqual(seen, [{ v: 0 }, { v: 1 }]);
  assert.equal(bus.getStats().topics[0].deprecatedSchemaVersions, 1);
});

test('EB-86: publishDelayed validates the version fail-fast at schedule time', async () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload));
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  const id = bus.publishDelayed('t', { v: 0 }, { delayMs: 0, schemaVersion: 0 });
  assert.ok(typeof id === 'string');
  assert.equal(bus.getStats().topics[0].deprecatedSchemaVersions, 1);
  // Rejected versions never become schedules.
  const bad = bus.publishDelayed('t', { v: 0 }, { delayMs: 0, schemaVersion: 7 });
  assert.equal(bad, undefined);
  await flush();
  await flush();
  assert.deepEqual(seen, [{ v: 0 }]);
});

test('EB-86: publishIdempotent carries the schema version through fan-out', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  const r = bus.publishIdempotent('t', { v: 0 }, { messageId: 'm1', schemaVersion: 0 });
  assert.deepEqual(r, { duplicate: false, accepted: 1 });
  assert.equal(bus.getStats().topics[0].deprecatedSchemaVersions, 1);
});

test('EB-86: throwing validators propagate to the publish caller', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, () => {
    throw new Error('boom');
  });
  assert.throws(() => bus.publish('t', {}), /boom/);
  // A throwing validator propagates before any counting: no stats entry
  // was created for the topic.
  assert.equal(bus.getStats().topics.length, 0);
});

test('EB-86: deprecated admissions render in the Prometheus exposition', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchemaVersion('t', 0, isV0);
  bus.setTopicSchemaVersion('t', 1, isV1, { graceMs: 60_000 });
  bus.publish('t', { v: 0 }, { schemaVersion: 0 });
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_deprecated_schema_messages_total 1'));
});
