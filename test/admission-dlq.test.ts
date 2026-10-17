import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionDlqRecord,
  type AdmissionRejectionEvent,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Subscribes to the DLQ topic and collects the record payloads. */
function collectDlq(bus: EventBus, dlqTopic: string): AdmissionDlqRecord[] {
  const records: AdmissionDlqRecord[] = [];
  bus.subscribe(dlqTopic, (msg) => {
    records.push(msg.payload as AdmissionDlqRecord);
  });
  return records;
}

test('registration validation: empty pattern / dlqTopic / bad retentionMs throw RangeError', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicDeadLetterTopic('', 'dlq'), RangeError);
  assert.throws(() => bus.setTopicDeadLetterTopic('orders', ''), RangeError);
  assert.throws(
    () => bus.setTopicDeadLetterTopic('orders', 'dlq', { retentionMs: 0 }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicDeadLetterTopic('orders', 'dlq', { retentionMs: -5 }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicDeadLetterTopic('orders', 'dlq', { retentionMs: Number.NaN }),
    RangeError,
  );
  // clear returns false when nothing was registered, true after registering.
  assert.equal(bus.clearTopicDeadLetterTopic('orders'), false);
  bus.setTopicDeadLetterTopic('orders', 'dlq');
  assert.equal(bus.clearTopicDeadLetterTopic('orders'), true);
  assert.equal(bus.clearTopicDeadLetterTopic('orders'), false);
});

test('opt-in: without a rule, rejections emit no DLQ record (backward compatible)', async () => {
  const bus = new EventBus();
  const records = collectDlq(bus, 'dlq.orders');
  bus.setTopicSchema('orders', (p) => typeof p === 'object' && p !== null);
  assert.equal(bus.publish('orders', 'bad'), 0);
  await flush();
  assert.deepEqual(records, []);
  assert.equal(bus.getStats().admissionDlqEvents, 0);
});

test('schema rejection emits a DLQ record with metadata, never the payload body', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicDeadLetterTopic('orders', 'dlq.orders');
  const records = collectDlq(bus, 'dlq.orders');
  bus.setTopicSchema('orders', (p) => typeof p === 'object' && p !== null);
  const canary = 'SECRET-PAYLOAD-CANARY-139';
  assert.equal(bus.publish('orders', canary), 0);
  await flush();
  assert.equal(records.length, 1);
  const [record] = records;
  assert.equal(record.originalTopic, 'orders');
  assert.equal(record.reason, 'schema');
  assert.equal(record.payloadBytes, Buffer.byteLength(JSON.stringify(canary), 'utf8'));
  assert.equal(record.at, 1_000);
  assert.ok(!('traceId' in record));
  assert.ok(!('publisherId' in record));
  // The rejected payload body never travels on the record.
  assert.ok(!JSON.stringify(record).includes(canary));
  assert.equal(bus.getStats().admissionDlqEvents, 1);
});

test('rate-limit shed emits a DLQ record with reason rate-limit', async () => {
  const bus = new EventBus();
  bus.setTopicDeadLetterTopic('hot', 'dlq.hot');
  const records = collectDlq(bus, 'dlq.hot');
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  bus.publish('hot', { n: 1 }); // takes the single token (0 accepted: no subscribers)
  assert.equal(bus.publish('hot', { n: 2 }), 0); // shed
  await flush();
  assert.equal(records.length, 1);
  assert.equal(records[0].reason, 'rate-limit');
  assert.equal(records[0].originalTopic, 'hot');
  assert.equal(bus.getStats().rateLimitedMessages, 1);
});

test('message-too-big rejection emits a DLQ record with reason message-too-big', async () => {
  const bus = new EventBus();
  bus.setTopicDeadLetterTopic('big', 'dlq.big');
  const records = collectDlq(bus, 'dlq.big');
  bus.setTopicMaxMessageBytes('big', 8);
  assert.equal(bus.publish('big', { n: 'way-too-long' }), 0);
  await flush();
  assert.equal(records.length, 1);
  assert.equal(records[0].reason, 'message-too-big');
});

test('ACL deny emits a DLQ record with reason acl', async () => {
  const bus = new EventBus({
    acl: { rules: [{ pattern: 'admin.**', publish: 'deny' }] },
  });
  bus.setTopicDeadLetterTopic('admin.**', 'dlq.admin');
  const records = collectDlq(bus, 'dlq.admin');
  assert.equal(bus.publish('admin.cmd', { a: 1 }), 0);
  await flush();
  assert.equal(records.length, 1);
  assert.equal(records[0].reason, 'acl');
  assert.equal(records[0].originalTopic, 'admin.cmd');
});

test('DLQ record carries publisherId and the traceId derived from traceparent', async () => {
  const bus = new EventBus();
  bus.setTopicDeadLetterTopic('orders', 'dlq.orders');
  const records = collectDlq(bus, 'dlq.orders');
  bus.setTopicSchema('orders', () => false);
  const traceparent = '00-4bf92f3577b34da6a3a929d1e0e47361-00f067aa0ba902b7-01';
  assert.equal(
    bus.publish('orders', 'bad', { publisherId: 'svc-orders', traceparent }),
    0,
  );
  await flush();
  assert.equal(records.length, 1);
  assert.equal(records[0].publisherId, 'svc-orders');
  assert.equal(records[0].traceId, '4bf92f3577b34da6a3a929d1e0e47361');
});

test('TTL drain expiry emits a DLQ record with reason ttl-expired', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicDeadLetterTopic('orders', 'dlq.orders');
  const records = collectDlq(bus, 'dlq.orders');
  const received: unknown[] = [];
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  bus.setTopicTtl('orders', 100); // published at t=1000, deadline t=1100
  bus.publish('orders', { n: 1 }, { publisherId: 'svc-a' });
  clock.nowMs = 1_200; // past the deadline when the flush drains
  await flush(); // drain expires the message and emits the record
  await flush(); // deliver the record
  assert.deepEqual(received, []);
  assert.equal(records.length, 1);
  const [record] = records;
  assert.equal(record.reason, 'ttl-expired');
  assert.equal(record.originalTopic, 'orders');
  assert.equal(record.publisherId, 'svc-a');
  assert.equal(record.payloadBytes, Buffer.byteLength('{"n":1}', 'utf8'));
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.equal(bus.getStats().admissionDlqEvents, 1);
});

test('pattern matching: exact-topic rule wins; earliest-registered pattern wins', async () => {
  const bus = new EventBus();
  bus.setTopicDeadLetterTopic('orders.*', 'dlq.pattern');
  bus.setTopicDeadLetterTopic('orders.us', 'dlq.exact');
  bus.setTopicDeadLetterTopic('**.x', 'dlq.first');
  bus.setTopicDeadLetterTopic('m.*', 'dlq.second');
  const viaPattern = collectDlq(bus, 'dlq.pattern');
  const viaExact = collectDlq(bus, 'dlq.exact');
  const viaFirst = collectDlq(bus, 'dlq.first');
  const viaSecond = collectDlq(bus, 'dlq.second');
  bus.setTopicSchema('orders.us', () => false);
  bus.setTopicSchema('orders.eu', () => false);
  bus.setTopicSchema('m.x', () => false);
  bus.publish('orders.us', 'bad'); // exact rule wins over the pattern
  bus.publish('orders.eu', 'bad'); // pattern rule
  bus.publish('m.x', 'bad'); // '**.x' registered before 'm.*'
  await flush();
  assert.equal(viaExact.length, 1);
  assert.equal(viaPattern.length, 1);
  assert.equal(viaFirst.length, 1);
  assert.equal(viaSecond.length, 0);
  assert.equal(viaExact[0].originalTopic, 'orders.us');
  assert.equal(viaPattern[0].originalTopic, 'orders.eu');
  assert.equal(viaFirst[0].originalTopic, 'm.x');
});

test('recursion cut: a rejected publish to the DLQ topic itself emits exactly one record', async () => {
  const bus = new EventBus();
  bus.setTopicDeadLetterTopic('*', 'dlq');
  const records = collectDlq(bus, 'dlq');
  bus.setTopicSchema('orders', () => false);
  bus.setTopicSchema('dlq', () => false); // the DLQ topic rejects too
  bus.publish('orders', 'bad');
  bus.publish('dlq', 'bad'); // rule matches the DLQ topic — must not loop
  await flush();
  // One record per rejection, no runaway: the test completing is the proof.
  assert.equal(records.length, 2);
  assert.deepEqual(
    records.map((r) => r.originalTopic).sort(),
    ['dlq', 'orders'],
  );
  assert.equal(bus.getStats().admissionDlqEvents, 2);
});

test('DLQ records bypass admission: a rejecting schema on the DLQ topic does not stop them', async () => {
  const hookEvents: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({ onAdmissionRejected: (e) => hookEvents.push(e) });
  bus.setTopicDeadLetterTopic('orders', 'dlq.orders');
  const records = collectDlq(bus, 'dlq.orders');
  bus.setTopicSchema('orders', () => false);
  bus.setTopicSchema('dlq.orders', () => false); // would reject the record under normal admission
  bus.publish('orders', 'bad');
  await flush();
  assert.equal(records.length, 1);
  assert.equal(records[0].originalTopic, 'orders');
  // The DLQ record itself never surfaces on the hook: it was never admitted-rejected.
  assert.deepEqual(
    hookEvents.map((e) => e.topic),
    ['orders'],
  );
});

test('independent retention: the record expires without a setTopicTtl rule, and never re-emits', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicDeadLetterTopic('orders', 'dlq.orders', { retentionMs: 100 });
  const received: unknown[] = [];
  bus.subscribe('dlq.orders', (msg) => received.push(msg.payload));
  bus.setTopicSchema('orders', () => false);
  bus.publish('orders', 'bad'); // record R1, retention deadline t=1100
  clock.nowMs = 1_200; // past R1's retention when the flush drains
  await flush(); // drain: R1 expires — the WeakSet mark stops a second record
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
  // Exactly one record was ever emitted: the expired DLQ record did not re-emit.
  assert.equal(bus.getStats().admissionDlqEvents, 1);
});

test('clearTopicDeadLetterTopic stops emission', async () => {
  const bus = new EventBus();
  bus.setTopicDeadLetterTopic('orders', 'dlq.orders');
  const records = collectDlq(bus, 'dlq.orders');
  bus.setTopicSchema('orders', () => false);
  bus.publish('orders', 'bad');
  await flush();
  assert.equal(records.length, 1);
  assert.equal(bus.clearTopicDeadLetterTopic('orders'), true);
  bus.publish('orders', 'bad');
  await flush();
  assert.equal(records.length, 1);
  assert.equal(bus.getStats().admissionDlqEvents, 1);
});

test('stats reconcile: admissionDlqEvents matches the DLQ-covered hook reasons', async () => {
  const hookEvents: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({ onAdmissionRejected: (e) => hookEvents.push(e) });
  bus.setTopicDeadLetterTopic('**', 'dlq.all');
  collectDlq(bus, 'dlq.all');
  bus.setTopicSchema('orders', () => false);
  bus.setTopicRateLimit('hot', 1, { burst: 1 });
  bus.publish('orders', 'bad'); // schema
  bus.publish('hot', { n: 1 });
  bus.publish('hot', { n: 2 }); // rate-limit
  bus.publishIdempotent('idem', 'x', { messageId: 'm-1' });
  bus.publishIdempotent('idem', 'x', { messageId: 'm-1' }); // duplicate — NOT DLQ-covered
  await flush();
  const dlqCovered = hookEvents.filter((e) =>
    ['schema', 'rate-limit', 'message-too-big', 'acl'].includes(e.reason),
  ).length;
  assert.equal(hookEvents.length, 3); // schema, rate-limit, duplicate
  assert.equal(dlqCovered, 2);
  assert.equal(bus.getStats().admissionDlqEvents, dlqCovered);
});

test('publishAtomic rejection emits one DLQ record for the failing entry', async () => {
  const bus = new EventBus();
  bus.setTopicDeadLetterTopic('orders', 'dlq.orders');
  const records = collectDlq(bus, 'dlq.orders');
  bus.setTopicSchema('orders', () => false);
  const outcome = bus.publishAtomic([
    { topic: 'orders', payload: { ok: 1 } },
    { topic: 'orders', payload: { ok: 2 } },
  ]);
  assert.equal(outcome.published, 0);
  assert.ok(outcome.rejected !== undefined);
  await flush();
  assert.equal(records.length, 1);
  assert.equal(records[0].reason, 'schema');
});

test('DLQ records skip the publish audit journal (internal health traffic)', async () => {
  const clock = controllableClock(5_000);
  const dir = mkdtempSync(join(tmpdir(), 'admission-dlq-audit-'));
  const bus = new EventBus({ durableLogDir: dir, publishAudit: true, now: clock.now });
  bus.setTopicDeadLetterTopic('orders', 'dlq.orders');
  bus.setTopicSchema('orders', () => false);
  bus.publish('orders', 'bad');
  const entries = bus.getPublishAudit();
  // Exactly one entry: the rejected publish. The DLQ record's own
  // publish is internal health traffic, not journaled.
  assert.equal(entries.length, 1);
  assert.equal(entries[0].topic, 'orders');
  assert.equal(entries[0].outcome, 'rejected');
});
