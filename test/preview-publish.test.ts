import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type AdmissionRejectReason,
  type PreviewGateName,
  type PreviewPublishResult,
} from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const state = { nowMs: startMs };
  return {
    now: () => state.nowMs,
    advance: (ms: number) => {
      state.nowMs += ms;
    },
  };
}

/** Collects per-message sequence numbers on one topic. */
function seqCollector() {
  const seqs: number[] = [];
  return {
    seqs,
    handler: (msg: { seq: number }) => {
      seqs.push(msg.seq);
    },
  };
}

/** JSON snapshot of the bus stats, for before/after side-effect diffing. */
function statsSnapshot(bus: EventBus): string {
  return JSON.stringify(bus.getStats());
}

/** Finds one topic's stats row (`getStats().topics` is a row array). */
function topicRow(bus: EventBus, topic: string): Record<string, number> | undefined {
  return (bus.getStats().topics as Array<Record<string, number>>).find(
    (row) => (row as { topic?: string }).topic === topic,
  );
}

test('happy path admits and touches nothing observable', async () => {
  const rejected: AdmissionRejectionEvent[] = [];
  const authzDenied: unknown[] = [];
  const bus = new EventBus({
    onAdmissionRejected: (e) => rejected.push(e),
    onAuthzDenied: (e) => authzDenied.push(e),
  });
  const seen: unknown[] = [];
  bus.subscribe('t', (msg) => seen.push(msg.payload));
  const before = statsSnapshot(bus);
  const auditBefore = JSON.stringify(bus.getSubscriptionAudit());

  const verdict: PreviewPublishResult = bus.previewPublish('t', { id: 1 });
  assert.deepEqual(verdict, { admitted: true });
  assert.equal(verdict.reason, undefined);
  assert.equal(verdict.failedGate, undefined);

  await flush();
  assert.deepEqual(seen, []);
  assert.deepEqual(rejected, []);
  assert.deepEqual(authzDenied, []);
  assert.equal(statsSnapshot(bus), before);
  assert.equal(JSON.stringify(bus.getSubscriptionAudit()), auditBefore);
});

test('alias-retired reports topic-alias gate', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicAlias('old', 'new', { ttlMs: 100 });
  clock.advance(101); // the alias TTL lapses — the old topic is read-only
  const verdict = bus.previewPublish('old', { id: 1 });
  assert.deepEqual(verdict, {
    admitted: false,
    reason: 'alias-retired',
    failedGate: 'topic-alias',
  });
  // ...while the live target still admits.
  assert.deepEqual(bus.previewPublish('new', { id: 1 }), { admitted: true });
});

test('topic pause reports topic-pause gate in shed and queue modes', () => {
  const bus = new EventBus();
  bus.pauseTopic('t');
  const shed: PreviewPublishResult = bus.previewPublish('t', { id: 1 });
  assert.deepEqual(shed, {
    admitted: false,
    reason: 'topic-paused',
    failedGate: 'topic-pause',
  });
  bus.resumeTopic('t');
  assert.deepEqual(bus.previewPublish('t', { id: 1 }), { admitted: true });

  // Queue mode also refuses the preview (it cannot be committed
  // synchronously), without touching the pause buffer.
  const dir = mkdtempSync(join(tmpdir(), 'preview-pause-'));
  const durable = new EventBus({ durableLogDir: dir });
  durable.pauseTopic('q', { mode: 'queue' });
  const queued = durable.previewPublish('q', { id: 1 });
  assert.deepEqual(queued, {
    admitted: false,
    reason: 'topic-paused',
    failedGate: 'topic-pause',
  });
  assert.equal(durable.getStats().pausedTopics[0].bufferedMessages, 0);
  durable.resumeTopic('q');
});

test('publisher rate-limit reports its gate and previews burn nothing', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const collector = seqCollector();
  bus.subscribe('t', collector.handler);
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });

  // Five previews against a burst-1 bucket: all admit, none burn.
  for (let i = 0; i < 5; i++) {
    const verdict = bus.previewPublish('t', { id: i }, { publisherId: 'svc' });
    assert.deepEqual(verdict, { admitted: true });
  }
  // The first real publish still takes the one token...
  const accepted = bus.publish('t', { id: 100 }, { publisherId: 'svc' });
  assert.equal(accepted, 1);
  await flush();
  assert.deepEqual(collector.seqs, [1]);
  // ...and now the budget is genuinely empty: the next preview sheds.
  const shed: PreviewPublishResult = bus.previewPublish('t', { id: 101 }, { publisherId: 'svc' });
  assert.deepEqual(shed, {
    admitted: false,
    reason: 'publisher-rate-limit',
    failedGate: 'publisher-rate-limit',
  });
  // A real publish agrees with the preview.
  assert.equal(bus.publish('t', { id: 102 }, { publisherId: 'svc' }), 0);
  assert.equal(bus.getStats().publisherRateLimited, 1);
  // Anonymous publishes are unlimited and never touch the gate.
  assert.deepEqual(bus.previewPublish('t', { id: 103 }), { admitted: true });
});

test('ACL denial reports acl gate and fires no authz audit', () => {
  const authzDenied: unknown[] = [];
  const rejected: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({
    onAuthzDenied: (e) => authzDenied.push(e),
    onAdmissionRejected: (e) => rejected.push(e),
  });
  bus.setAclRules([{ pattern: 'secret.*', publish: 'deny' }]);
  const verdict: PreviewPublishResult = bus.previewPublish('secret.x', { id: 1 });
  assert.deepEqual(verdict, { admitted: false, reason: 'acl', failedGate: 'acl' });
  // Zero side effects: no hook, no authz audit (the live path audits via
  // countAclDenied — the preview must not).
  assert.deepEqual(rejected, []);
  assert.deepEqual(authzDenied, []);
  assert.equal(bus.getStats().rejectedMessages, 0);
  // The open topic still admits.
  assert.deepEqual(bus.previewPublish('public.x', { id: 1 }), { admitted: true });
});

test('schema rejection reports schema gate', () => {
  const bus = new EventBus();
  bus.setTopicSchema('t', (payload) => typeof payload === 'object' && payload !== null && 'id' in payload);
  const bad: PreviewPublishResult = bus.previewPublish('t', { nope: true });
  assert.deepEqual(bad, { admitted: false, reason: 'schema', failedGate: 'schema' });
  assert.deepEqual(bus.previewPublish('t', { id: 1 }), { admitted: true });
  assert.equal(bus.getStats().rejectedMessages, 0);
});

test('versioned schemas: unknown and expired versions reject, graced admits without counting', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const v1ok = (payload: unknown) => typeof payload === 'object' && payload !== null && 'v' in payload;
  bus.setTopicSchemaVersion('t', 1, v1ok, { graceMs: 1_000 });
  bus.setTopicSchemaVersion('t', 2, v1ok);

  // Unknown version rejects like a schema failure.
  assert.deepEqual(bus.previewPublish('t', { v: 1 }, { schemaVersion: 99 }), {
    admitted: false,
    reason: 'schema',
    failedGate: 'schema',
  });
  // Superseded-but-graced version admits...
  assert.deepEqual(bus.previewPublish('t', { v: 1 }, { schemaVersion: 1 }), { admitted: true });
  // ...without moving the deprecated-schema counter (zero side effects).
  const stats = bus.getStats();
  assert.equal(stats.deprecatedSchemaVersions, 0);
  assert.equal(topicRow(bus, 't')?.deprecatedSchemaVersions ?? 0, 0);
  // Past the grace window the old version rejects.
  clock.advance(1_001);
  assert.deepEqual(bus.previewPublish('t', { v: 1 }, { schemaVersion: 1 }), {
    admitted: false,
    reason: 'schema',
    failedGate: 'schema',
  });
});

test('oversized payload reports max-message-bytes gate', () => {
  const bus = new EventBus();
  bus.setTopicMaxMessageBytes('t', 16);
  const big: PreviewPublishResult = bus.previewPublish('t', { id: 'x'.repeat(64) });
  assert.deepEqual(big, {
    admitted: false,
    reason: 'message-too-big',
    failedGate: 'max-message-bytes',
  });
  assert.deepEqual(bus.previewPublish('t', { id: 1 }), { admitted: true });
});

test('topic rate-limit reports topic-rate-limit gate and previews do not refill or burn', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const collector = seqCollector();
  bus.subscribe('t', collector.handler);
  bus.setTopicRateLimit('t', 1, { burst: 1 });

  // Previews against a full burst-1 bucket all admit and burn nothing.
  for (let i = 0; i < 3; i++) {
    assert.deepEqual(bus.previewPublish('t', { id: i }), { admitted: true });
  }
  // One real publish consumes the single token...
  assert.equal(bus.publish('t', { id: 10 }), 1);
  await flush();
  assert.deepEqual(collector.seqs, [1]);
  // ...so the next preview sheds, exactly like the real path would.
  const shed: PreviewPublishResult = bus.previewPublish('t', { id: 11 });
  assert.deepEqual(shed, {
    admitted: false,
    reason: 'rate-limit',
    failedGate: 'topic-rate-limit',
  });
  assert.equal(bus.getStats().rateLimitedMessages, 0);
});

test('idempotent dedup gate suppresses in-window retries and never claims the slot', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 60_000 });
  const collector = seqCollector();
  bus.subscribe('t', collector.handler);

  // A fresh messageId admits...
  assert.deepEqual(bus.previewPublish('t', { id: 1 }, { messageId: 'm-fresh' }), {
    admitted: true,
  });
  // ...and admits again: the preview never claimed the dedup slot.
  assert.deepEqual(bus.previewPublish('t', { id: 1 }, { messageId: 'm-fresh' }), {
    admitted: true,
  });
  // So a real idempotent publish still admits once...
  const first = bus.publishIdempotent('t', { id: 1 }, { messageId: 'm-fresh' });
  assert.deepEqual(first, { duplicate: false, accepted: 1 });
  await flush();
  assert.deepEqual(collector.seqs, [1]);
  // ...and from here the window is live: preview reports the duplicate...
  const dup: PreviewPublishResult = bus.previewPublish('t', { id: 1 }, { messageId: 'm-fresh' });
  assert.deepEqual(dup, {
    admitted: false,
    reason: 'duplicate',
    failedGate: 'idempotency-dedup',
  });
  // ...exactly like the real idempotent path does.
  assert.deepEqual(bus.publishIdempotent('t', { id: 1 }, { messageId: 'm-fresh' }), {
    duplicate: true,
    accepted: 0,
  });
  // Past the window the identity is fresh again.
  clock.advance(60_001);
  assert.deepEqual(bus.previewPublish('t', { id: 1 }, { messageId: 'm-fresh' }), {
    admitted: true,
  });
  // No messageId, no dedup gate: plain publishes never consult it.
  assert.deepEqual(bus.previewPublish('t', { id: 2 }), { admitted: true });
  assert.equal(bus.getStats().duplicateMessages, 1);
});

test('dedup suppression is checked before later gates, like publishIdempotent', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  // Claim the slot with a real idempotent publish...
  assert.deepEqual(bus.publishIdempotent('t', { id: 1 }, { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  // ...then pause the topic: the retry is still reported as a duplicate
  // (identity decides first), not as topic-paused.
  bus.pauseTopic('t');
  assert.deepEqual(bus.previewPublish('t', { id: 1 }, { messageId: 'm1' }), {
    admitted: false,
    reason: 'duplicate',
    failedGate: 'idempotency-dedup',
  });
  // A fresh identity on the paused topic reports the pause instead.
  assert.deepEqual(bus.previewPublish('t', { id: 2 }, { messageId: 'm2' }), {
    admitted: false,
    reason: 'topic-paused',
    failedGate: 'topic-pause',
  });
  bus.resumeTopic('t');
});

test('previews consume no sequence numbers: subscribers see no gap', async () => {
  const rejected: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({ onAdmissionRejected: (e) => rejected.push(e) });
  const collector = seqCollector();
  bus.subscribe('t', collector.handler);
  bus.setTopicSchema('t', (payload) => typeof payload === 'object' && payload !== null);

  bus.publish('t', { id: 1 });
  // A mix of admitting and rejecting previews — none may move the seq.
  assert.deepEqual(bus.previewPublish('t', { id: 2 }), { admitted: true });
  const bad = bus.previewPublish('t', 'not-an-object');
  assert.equal(bad.admitted, false);
  assert.equal(bad.reason, 'schema');
  bus.pauseTopic('t');
  assert.equal(bus.previewPublish('t', { id: 3 }).reason, 'topic-paused');
  bus.resumeTopic('t');
  bus.publish('t', { id: 4 });

  await flush();
  assert.deepEqual(collector.seqs, [1, 2]);
  assert.equal(topicRow(bus, 't')?.lastSeq, 2);
  assert.deepEqual(rejected, []);
});

test('no preview ever fires onAdmissionRejected, moves stats, or audits', () => {
  const clock = controllableClock();
  const rejected: AdmissionRejectionEvent[] = [];
  const authzDenied: unknown[] = [];
  const bus = new EventBus({
    now: clock.now,
    onAdmissionRejected: (e) => rejected.push(e),
    onAuthzDenied: (e) => authzDenied.push(e),
  });
  bus.setAclRules([{ pattern: 'secret.*', publish: 'deny' }]);
  bus.setTopicSchema('t', () => false);
  bus.setTopicMaxMessageBytes('big', 8);
  bus.setTopicRateLimit('rl', 1, { burst: 1 });
  bus.setPublisherRateLimit('svc', { messagesPerSec: 1, burst: 1 });
  bus.pauseTopic('paused');
  bus.setTopicAlias('old', 'new', { ttlMs: 100 });
  clock.advance(101);
  // Pre-burn the publisher budget with a real publish on a schema-free
  // topic (the publisher gate runs before schema, so the preview case
  // below sheds deterministically at that gate).
  bus.publish('burn', { id: 1 }, { publisherId: 'svc' });
  bus.subscribe('rl', () => {});

  const auditBefore = JSON.stringify(bus.getSubscriptionAudit());

  const cases: Array<[string, unknown, Record<string, unknown>?]> = [
    ['old', { id: 1 }],
    ['paused', { id: 1 }],
    ['t', { id: 1 }, { publisherId: 'svc' }],
    ['secret.x', { id: 1 }],
    ['t', { id: 1 }],
    ['big', { id: 'x'.repeat(64) }],
  ];
  const reasons: AdmissionRejectReason[] = [
    'alias-retired',
    'topic-paused',
    'publisher-rate-limit',
    'acl',
    'schema',
    'message-too-big',
  ];
  const gates: PreviewGateName[] = [
    'topic-alias',
    'topic-pause',
    'publisher-rate-limit',
    'acl',
    'schema',
    'max-message-bytes',
  ];
  for (let i = 0; i < cases.length; i++) {
    const [topic, payload, opts] = cases[i];
    const verdict = bus.previewPublish(topic, payload, opts);
    assert.equal(verdict.admitted, false);
    assert.equal(verdict.reason, reasons[i]);
    assert.equal(verdict.failedGate, gates[i]);
  }
  // Drain the topic bucket with a real publish on a schema-free topic, then
  // preview the shed.
  bus.publish('rl', { id: 1 });
  const shed = bus.previewPublish('rl', { id: 2 });
  assert.deepEqual(shed, {
    admitted: false,
    reason: 'rate-limit',
    failedGate: 'topic-rate-limit',
  });

  assert.deepEqual(rejected, []);
  assert.deepEqual(authzDenied, []);
  // Every preview left the rejection-family counters exactly alone; the
  // only rate-limited shed is the one the preview predicted, uncounted.
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 0);
  assert.equal(stats.rateLimitedMessages, 0);
  assert.equal(stats.publisherRateLimited, 0);
  assert.equal(stats.pausedMessages, 0);
  assert.equal(stats.duplicateMessages, 0);
  assert.equal(JSON.stringify(bus.getSubscriptionAudit()), auditBefore);
});

test('atomic shadow admission agrees with previewPublish', () => {
  const bus = new EventBus();
  bus.setTopicSchema('t', (payload) => typeof payload === 'object' && payload !== null && 'id' in payload);

  // Rejecting entry: same reason from both deciders.
  const preview = bus.previewPublish('t', { nope: true });
  assert.deepEqual(preview, { admitted: false, reason: 'schema', failedGate: 'schema' });
  const atomic = bus.publishAtomic([{ topic: 't', payload: { nope: true } }]);
  assert.deepEqual(atomic, {
    published: 0,
    rejected: { index: 0, topic: 't', reason: 'schema' },
  });

  // Pause: both refuse.
  bus.pauseTopic('t');
  assert.deepEqual(bus.previewPublish('t', { id: 1 }).reason, 'topic-paused');
  const atomicPaused = bus.publishAtomic([{ topic: 't', payload: { id: 1 } }]);
  assert.deepEqual(atomicPaused.rejected?.reason, 'topic-paused');
  bus.resumeTopic('t');

  // Happy entry: both admit.
  assert.deepEqual(bus.previewPublish('t', { id: 1 }), { admitted: true });
  assert.deepEqual(bus.publishAtomic([{ topic: 't', payload: { id: 1 } }]), { published: 1 });
});

test('redaction runs before the schema gate in preview, without counting', () => {
  const rejected: AdmissionRejectionEvent[] = [];
  const bus = new EventBus({ onAdmissionRejected: (e) => rejected.push(e) });
  bus.setTopicRedaction('t', ['secret']);
  // The validator only ever sees the redacted bytes — in preview exactly
  // like in the live path.
  bus.setTopicSchema('t', (payload) => (payload as { secret?: string }).secret === '***');
  assert.deepEqual(bus.previewPublish('t', { secret: 'hunter2' }), { admitted: true });
  // Zero side effects: the redaction counter did not move, no hook fired.
  assert.equal(bus.getStats().redactedFields, 0);
  assert.equal(topicRow(bus, 't')?.redactedFields ?? 0, 0);
  assert.deepEqual(rejected, []);
  // And the live path agrees with the preview.
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', { secret: 'hunter2' }), 1);
});

test('barrier cut markers take the slim admission path', () => {
  const bus = new EventBus();
  bus.setAclRules([{ pattern: 't', publish: 'deny' }]);
  // The one gate a control message keeps is the ACL.
  assert.deepEqual(bus.previewPublish('t', { x: 1 }, { barrier: { epoch: 7 } }), {
    admitted: false,
    reason: 'acl',
    failedGate: 'acl',
  });

  const open = new EventBus();
  // Schema would reject a data publish, but barriers skip schema —
  // mirroring fanOutBarrier.
  open.setTopicSchema('t', () => false);
  assert.deepEqual(open.previewPublish('t', { x: 1 }, { barrier: { epoch: 7 } }), {
    admitted: true,
  });
  assert.deepEqual(open.previewPublish('t', { x: 1 }), {
    admitted: false,
    reason: 'schema',
    failedGate: 'schema',
  });
});

test('namespace resolution applies before the gates', () => {
  const bus = new EventBus();
  bus.createNamespace('t1');
  // Rules key off the concrete `<namespace>/<topic>` (exact-topic match).
  bus.setTopicSchema('t1/orders', (payload) => typeof payload === 'object' && payload !== null);
  // The schema on the concrete topic governs the namespaced preview.
  assert.deepEqual(bus.previewPublish('orders', { id: 1 }, { namespace: 't1' }), {
    admitted: true,
  });
  assert.deepEqual(bus.previewPublish('orders', 'nope', { namespace: 't1' }), {
    admitted: false,
    reason: 'schema',
    failedGate: 'schema',
  });
  // Unknown namespaces throw, exactly like publish.
  assert.throws(() => bus.previewPublish('orders', { id: 1 }, { namespace: 'nope' }), RangeError);
});

test('invalid options throw exactly like publish', () => {
  const bus = new EventBus();
  const ok = { id: 1 };
  const throwsCases: Array<[string, () => unknown, new (...args: never[]) => Error]> = [
    ['empty key', () => bus.previewPublish('t', ok, { key: '' }), RangeError],
    ['non-string key', () => bus.previewPublish('t', ok, { key: 42 as unknown as string }), RangeError],
    ['bad publisherId', () => bus.previewPublish('t', ok, { publisherId: 'not valid!' }), RangeError],
    ['negative schemaVersion', () => bus.previewPublish('t', ok, { schemaVersion: -1 }), RangeError],
    [
      'fractional schemaVersion',
      () => bus.previewPublish('t', ok, { schemaVersion: 1.5 }),
      RangeError,
    ],
    ['negative partition', () => bus.previewPublish('t', ok, { partition: -1 }), RangeError],
    [
      'empty partitionKey',
      () => bus.previewPublish('t', ok, { partitionKey: '' }),
      RangeError,
    ],
    ['bad causal clock', () => bus.previewPublish('t', ok, { causal: { clock: -1 } }), RangeError],
    ['non-string traceparent', () => bus.previewPublish('t', ok, { traceparent: 7 as unknown as string }), RangeError],
    ['bad barrier epoch', () => bus.previewPublish('t', ok, { barrier: { epoch: -2 } }), RangeError],
    [
      'binary with non-bytes payload',
      () => bus.previewPublish('t', ok, { binary: true }),
      TypeError,
    ],
  ];
  for (const [name, fn, errorType] of throwsCases) {
    assert.throws(fn, errorType, name);
  }
  // Partition routing is walked: an out-of-range explicit partition throws
  // RangeError — never a rejection — mirroring publish.
  assert.throws(() => bus.previewPublish('t', ok, { partition: 4 }), RangeError);
  // ...while an in-range partition and a partitionKey admit.
  bus.setTopicPartitions('t', 4);
  assert.deepEqual(bus.previewPublish('t', ok, { partition: 3 }), { admitted: true });
  assert.deepEqual(bus.previewPublish('t', ok, { partitionKey: 'order-42' }), { admitted: true });
});
