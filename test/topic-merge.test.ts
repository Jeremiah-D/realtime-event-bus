import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'eb-merge-'));

function readHistory(dir: string, topic: string) {
  const log = DurableTopicLog.open({ dir });
  return log.readSince(topic, 0);
}

test('mergeTopics unions both histories in timestamp order with a fresh sequence space and retires both sources', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('orders.eu', 'eu-1');
  clock.nowMs = 2_000;
  bus.publish('orders.us', 'us-1');
  clock.nowMs = 3_000;
  bus.publish('orders.eu', 'eu-2');
  clock.nowMs = 4_000;
  bus.publish('orders.us', 'us-2');
  await flush();

  const result = bus.mergeTopics('orders.eu', 'orders.us', 'orders.all');
  assert.equal(result.targetTopic, 'orders.all');
  assert.deepEqual(result.sourceTopics, ['orders.eu', 'orders.us']);
  assert.equal(result.copied, 4);
  assert.deepEqual(result.sourceLastSeqs, [2, 2]);
  assert.equal(result.targetLastSeq, 4);
  assert.equal(result.lifecycleProvisioned, true);
  assert.equal(result.sourcesTombstoned, true);

  // The merged topic reads as a topic that was always the merge:
  // timestamp order across sources, re-seqed from 1.
  const merged = readHistory(dir, 'orders.all');
  assert.deepEqual(merged.map((r) => r.seq), [1, 2, 3, 4]);
  assert.deepEqual(merged.map((r) => r.payload), ['eu-1', 'us-1', 'eu-2', 'us-2']);
  assert.ok(merged.every((r) => r.topic === 'orders.all'));

  // The sources' histories are untouched and still replayable —
  // tombstoned means read-only, not destroyed.
  assert.deepEqual(readHistory(dir, 'orders.eu').map((r) => r.payload), ['eu-1', 'eu-2']);
  assert.deepEqual(readHistory(dir, 'orders.us').map((r) => r.payload), ['us-1', 'us-2']);

  // …but new publishes to a source are refused, like a deleted topic.
  assert.equal(bus.publish('orders.eu', 'eu-3'), 0);
  assert.equal(bus.publish('orders.us', 'us-3'), 0);
  assert.deepEqual(bus.previewPublish('orders.eu', 'eu-3'), {
    admitted: false,
    reason: 'alias-retired',
    failedGate: 'topic-lifecycle',
  });
  assert.equal(bus.getTopicLifecycle('orders.eu')?.provisioned, false);
  assert.equal(bus.getTopicLifecycle('orders.us')?.provisioned, false);

  // The target evolves independently afterwards, continuing its
  // own numbering — and the audit carries the merge provenance.
  const targetReceived: unknown[] = [];
  bus.subscribe('orders.all', (m) => targetReceived.push(m.payload));
  bus.publish('orders.all', 'all-3');
  await flush();
  assert.deepEqual(targetReceived, ['all-3']);
  assert.deepEqual(readHistory(dir, 'orders.all').map((r) => r.seq), [1, 2, 3, 4, 5]);
  const stats = bus.getStats();
  const targetStats = stats.topics.find((t) => t.topic === 'orders.all');
  assert.ok(targetStats !== undefined);
  assert.equal(targetStats.lastSeq, 5);
  assert.equal(bus.getTopicLifecycle('orders.all')?.provisioned, true);
  const mergedAudit = bus.getSubscriptionAudit({ action: 'topics_merged' });
  assert.equal(mergedAudit.length, 1);
  assert.equal(mergedAudit[0]?.topic, 'orders.all');
  assert.deepEqual(
    (mergedAudit[0]?.detail as { mergeSources?: unknown })?.mergeSources,
    ['orders.eu', 'orders.us'],
  );
});

test('mergeTopics breaks timestamp ties toward the first source', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('a', 'a-1');
  bus.publish('b', 'b-1');
  clock.nowMs = 2_000;
  bus.publish('b', 'b-2');
  bus.publish('a', 'a-2');
  await flush();

  const result = bus.mergeTopics('a', 'b', 'ab');
  assert.equal(result.copied, 4);
  // Same-timestamp records keep first-source-first order whatever
  // the publish order inside the timestamp was (b-2 was published
  // before a-2, but both carry at=2000), and neither source's
  // internal order ever inverts.
  assert.deepEqual(readHistory(dir, 'ab').map((r) => r.payload), ['a-1', 'b-1', 'a-2', 'b-2']);
});

test('mergeTopics fails closed: unknown source, used target, or duplicate names change nothing', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('a', 'a-1');
  bus.publish('b', 'b-1');
  await flush();

  assert.throws(() => bus.mergeTopics('missing', 'b', 'ab'), RangeError);
  assert.throws(() => bus.mergeTopics('a', 'missing', 'ab'), RangeError);
  assert.throws(() => bus.mergeTopics('a', 'a', 'ab'), RangeError);
  assert.throws(() => bus.mergeTopics('a', 'b', 'a'), RangeError);
  assert.throws(() => bus.mergeTopics('a', 'b', 'b'), RangeError);
  // A target already in use is refused, not overwritten.
  bus.publish('ab', 'existing');
  await flush();
  assert.throws(() => bus.mergeTopics('a', 'b', 'ab'), RangeError);

  // Nothing moved: the sources are still live, the failed target
  // never gained merged history, and no merge was audited.
  assert.equal(bus.getTopicLifecycle('a')?.provisioned, undefined);
  bus.publish('a', 'a-2');
  await flush();
  assert.deepEqual(readHistory(dir, 'a').map((r) => r.payload), ['a-1', 'a-2']);
  assert.deepEqual(readHistory(dir, 'ab').map((r) => r.payload), ['existing']);
  assert.equal(bus.getSubscriptionAudit({ action: 'topics_merged' }).length, 0);
});

test('mergeTopics requires a durable log', async () => {
  const bus = new EventBus();
  bus.publish('a', 'a-1');
  bus.publish('b', 'b-1');
  assert.throws(() => bus.mergeTopics('a', 'b', 'ab'), RangeError);
});
