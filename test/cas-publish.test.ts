import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type AdmissionRejectionEvent, type TopicStats } from '../src/bus.ts';
import { payloadEquals, compareAndSetMatches, validateCompareAndSetExpected } from '../src/caspublish.ts';

/** Per-topic stats row from a getStats() snapshot. */
function topicStats(bus: EventBus, topic: string): TopicStats {
  const row = bus.getStats().topics.find((t) => t.topic === topic);
  assert.ok(row, `expected stats row for topic ${topic}`);
  return row;
}

/** Injectable clock so versions and windows are deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise((resolve) => setImmediate(resolve));

function busWithStore() {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.enableStateStore();
  return bus;
}

test('version match admits and publishes; version bumps the row', async () => {
  const bus = busWithStore();
  const received: BusMessage[] = [];
  bus.subscribe('cfg.service', (msg) => received.push(msg));
  bus.publish('cfg.service', { replicas: 1 }, { key: 'deploy' });
  await flush();
  assert.equal(bus.getState('cfg.service', 'deploy')?.version, 1);

  const res = bus.publishCompareAndSet(
    'cfg.service',
    'deploy',
    { version: 1 },
    { replicas: 2 },
  );
  assert.equal(res.admitted, true);
  assert.equal(res.reason, undefined);
  assert.equal(res.accepted, 1);
  assert.equal(typeof res.seq, 'number');
  await flush();
  assert.equal(received.length, 2);
  assert.deepEqual(received[1].payload, { replicas: 2 });
  assert.equal(bus.getState('cfg.service', 'deploy')?.version, 2);
});

test('version mismatch rejects with cas-mismatch and zero side effects', async () => {
  const received: BusMessage[] = [];
  const rejections: AdmissionRejectionEvent[] = [];
  const bus2 = new EventBus({
    now: makeClock().now,
    onAdmissionRejected: (e) => rejections.push(e),
  });
  bus2.enableStateStore();
  bus2.subscribe('cfg.service', (msg) => received.push(msg));
  bus2.publish('cfg.service', { replicas: 1 }, { key: 'deploy' });
  await flush();

  const lastSeqBefore = topicStats(bus2, 'cfg.service').lastSeq;
  const res = bus2.publishCompareAndSet('cfg.service', 'deploy', { version: 7 }, { replicas: 9 });
  assert.deepEqual(res, {
    admitted: false,
    reason: 'cas-mismatch',
    accepted: 0,
    expectedVersion: 7,
    actualVersion: 1,
  });
  await flush();
  // Zero side effects: nothing published, no seq consumed, row untouched.
  assert.equal(received.length, 1);
  assert.equal(topicStats(bus2, 'cfg.service').lastSeq, lastSeqBefore);
  assert.deepEqual(bus2.getState('cfg.service', 'deploy')?.value, { replicas: 1 });
  // Hook fired on the unified channel with the new reason.
  assert.equal(rejections.length, 1);
  assert.equal(rejections[0].reason, 'cas-mismatch');
  assert.equal(rejections[0].topic, 'cfg.service');
  // Stats reconcile 1:1 with the hook.
  assert.equal(topicStats(bus2, 'cfg.service').casMismatches, 1);
  assert.equal(bus2.getStats().casMismatches, 1);
});

test('value predicate matches structurally, key order insignificant', async () => {
  const bus = busWithStore();
  bus.publish('cfg.service', { a: 1, b: [2, 3] }, { key: 'deploy' });
  await flush();
  const res = bus.publishCompareAndSet(
    'cfg.service',
    'deploy',
    { value: { b: [2, 3], a: 1 } },
    { a: 2, b: [2, 3] },
  );
  assert.equal(res.admitted, true);
  assert.deepEqual(bus.getState('cfg.service', 'deploy')?.value, { a: 2, b: [2, 3] });
});

test('value predicate mismatch rejects', () => {
  const bus = busWithStore();
  bus.publish('cfg.service', { replicas: 1 }, { key: 'deploy' });
  const res = bus.publishCompareAndSet('cfg.service', 'deploy', { value: { replicas: 2 } }, { replicas: 3 });
  assert.equal(res.admitted, false);
  assert.equal(res.reason, 'cas-mismatch');
  // No version fields on a value-only mismatch.
  assert.equal(res.expectedVersion, undefined);
  assert.equal(res.actualVersion, undefined);
});

test('create-if-absent: { version: 0 } admits on a missing key', async () => {
  const bus = busWithStore();
  const res = bus.publishCompareAndSet('cfg.new', 'k1', { version: 0 }, { v: 1 });
  assert.equal(res.admitted, true);
  await flush();
  assert.equal(bus.getState('cfg.new', 'k1')?.version, 1);
});

test('create-if-absent via value: { value: undefined } matches a missing row', () => {
  const bus = busWithStore();
  const res = bus.publishCompareAndSet('cfg.new', 'k2', { value: undefined }, { v: 1 });
  assert.equal(res.admitted, true);
});

test('version and value must both hold when both are given', () => {
  const bus = busWithStore();
  bus.publish('cfg.service', { replicas: 1 }, { key: 'deploy' });
  // Right version, wrong value.
  assert.equal(
    bus.publishCompareAndSet('cfg.service', 'deploy', { version: 1, value: { replicas: 2 } }, {}).admitted,
    false,
  );
  // Wrong version, right value.
  assert.equal(
    bus.publishCompareAndSet('cfg.service', 'deploy', { version: 5, value: { replicas: 1 } }, {}).admitted,
    false,
  );
  // Both right.
  assert.equal(
    bus.publishCompareAndSet('cfg.service', 'deploy', { version: 1, value: { replicas: 1 } }, { replicas: 2 })
      .admitted,
    true,
  );
});

test('requires enableStateStore — RangeError fail-fast', () => {
  const bus = new EventBus({ now: makeClock().now });
  assert.throws(
    () => bus.publishCompareAndSet('cfg.service', 'deploy', { version: 0 }, {}),
    /state store is not enabled/,
  );
});

test('malformed expected and key throw RangeError before anything is read', () => {
  const bus = busWithStore();
  assert.throws(() => bus.publishCompareAndSet('cfg.service', 'deploy', null as never, {}), RangeError);
  assert.throws(() => bus.publishCompareAndSet('cfg.service', 'deploy', {} as never, {}), RangeError);
  assert.throws(
    () => bus.publishCompareAndSet('cfg.service', 'deploy', { version: -1 }, {}),
    RangeError,
  );
  assert.throws(
    () => bus.publishCompareAndSet('cfg.service', 'deploy', { version: 1.5 }, {}),
    RangeError,
  );
  assert.throws(() => bus.publishCompareAndSet('cfg.service', '', { version: 0 }, {}), RangeError);
  assert.throws(() => bus.publishCompareAndSet('', 'deploy', { version: 0 }, {}), RangeError);
  // No stats moved on validation throws.
  assert.equal(bus.getStats().casMismatches, 0);
});

test('admission still applies after a matching compare (schema gate)', () => {
  const bus = busWithStore();
  bus.publish('cfg.service', { replicas: 1 }, { key: 'deploy' });
  bus.setTopicSchema('cfg.service', () => false);
  const res = bus.publishCompareAndSet('cfg.service', 'deploy', { version: 1 }, { replicas: 2 });
  assert.equal(res.admitted, false);
  assert.equal(res.reason, 'schema');
  // The schema rejection — not the CAS mismatch — owns the counters.
  assert.equal(bus.getStats().casMismatches, 0);
  assert.equal(topicStats(bus, 'cfg.service').rejectedMessages, 1);
});

test('namespace-scoped CAS addresses the namespaced row', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.enableStateStore();
  bus.createNamespace('tenant-a', { allowPublish: true, allowSubscribe: true });
  bus.publish('cfg.service', { v: 1 }, { key: 'deploy', namespace: 'tenant-a' });
  await flush();
  const res = bus.publishCompareAndSet('cfg.service', 'deploy', { version: 1 }, { v: 2 }, {
    namespace: 'tenant-a',
  });
  assert.equal(res.admitted, true);
  assert.deepEqual(bus.getState('tenant-a/cfg.service', 'deploy')?.value, { v: 2 });
});

test('payloadEquals: structural equality contract', () => {
  assert.equal(payloadEquals({ a: 1, b: [2, { c: 3 }] }, { b: [2, { c: 3 }], a: 1 }), true);
  assert.equal(payloadEquals({ a: 1 }, { a: 1, b: 2 }), false);
  assert.equal(payloadEquals([1, 2], [1, 2, 3]), false);
  assert.equal(payloadEquals(NaN, NaN), true);
  assert.equal(payloadEquals(0, -0), true);
  assert.equal(payloadEquals(new Date(1000), new Date(1000)), true);
  assert.equal(payloadEquals(new Date(1000), new Date(1001)), false);
  assert.equal(
    payloadEquals(new Uint8Array([1, 2, 3]), new Uint8Array([1, 2, 3])),
    true,
  );
  assert.equal(
    payloadEquals(new Uint8Array([1, 2, 3]), new Uint8Array([1, 2, 4])),
    false,
  );
  assert.equal(payloadEquals('x', 'x'), true);
  assert.equal(payloadEquals(null, undefined), false);
});

test('compareAndSetMatches: missing row reads as version 0 / value undefined', () => {
  assert.equal(compareAndSetMatches(undefined, { version: 0 }), true);
  assert.equal(compareAndSetMatches(undefined, { version: 1 }), false);
  assert.equal(compareAndSetMatches(undefined, { value: undefined }), true);
  assert.equal(compareAndSetMatches({ value: { a: 1 }, version: 3 }, { version: 3 }), true);
  assert.equal(compareAndSetMatches({ value: { a: 1 }, version: 3 }, { value: { a: 1 } }), true);
  assert.equal(compareAndSetMatches({ value: { a: 1 }, version: 3 }, { value: { a: 2 } }), false);
});

test('validateCompareAndSetExpected accepts version/value/both', () => {
  validateCompareAndSetExpected({ version: 0 }, 't');
  validateCompareAndSetExpected({ value: null }, 't');
  validateCompareAndSetExpected({ version: 2, value: { x: 1 } }, 't');
});
