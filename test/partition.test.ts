import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-partition-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Waits until the topic's closed segment is fully archived (gz + index, tmp gone). */
async function waitForArchived(dir: string, topic: string): Promise<void> {
  const prefix = `${encodeURIComponent(topic)}.seg-`;
  const segmentFiles = (ext: '.jsonl.gz' | '.jsonl.tmp') =>
    readdirSync(dir).filter((f) => f.endsWith(ext));
  await waitFor(
    () =>
      segmentFiles('.jsonl.tmp').filter((f) => f.startsWith(prefix)).length === 0 &&
      segmentFiles('.jsonl.gz').filter((f) => f.startsWith(prefix)).length > 0,
  );
}

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('setTopicPartitions: illegal counts throw RangeError, valid counts stick', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, 1.5, NaN, Infinity]) {
    assert.throws(() => bus.setTopicPartitions('t', bad), RangeError, `n=${bad}`);
  }
  assert.throws(() => bus.setTopicPartitions('', 2), RangeError);
  bus.setTopicPartitions('t', 4);
  // Re-setting replaces the rule.
  bus.setTopicPartitions('t', 2);
  assert.equal(bus.clearTopicPartitions('t'), true);
  assert.equal(bus.clearTopicPartitions('t'), false);
});

test('partition rule matching mirrors the TTL convention: exact wins, earliest pattern wins', async () => {
  const bus = new EventBus();
  const seen: Array<[string, number | undefined]> = [];
  bus.subscribe('orders.*', (m) => seen.push([m.topic, m.partition]));
  bus.setTopicPartitions('orders.*', 2);
  bus.setTopicPartitions('orders.eu', 8); // exact beats pattern
  bus.setTopicPartitions('orders.*', 2);
  bus.setTopicPartitions('orders.e*', 4); // later pattern loses to the earlier one
  bus.publish('orders.eu', 'a', { partition: 7 }); // 8 partitions: ok
  bus.publish('orders.us', 'b', { partition: 1 }); // earliest pattern (2): ok
  assert.throws(() => bus.publish('orders.us', 'c', { partition: 2 }), RangeError);
  await flush();
  assert.deepEqual(seen, [
    ['orders.eu', 7],
    ['orders.us', 1],
  ]);
});

test('explicit partitions get independent per-(topic, partition) sequence numbers', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('jobs', 3);
  const seen: Array<[number, number]> = [];
  bus.subscribe('jobs', (m) => seen.push([m.partition ?? -1, m.seq]));
  bus.publish('jobs', 'a', { partition: 2 });
  bus.publish('jobs', 'b', { partition: 0 });
  bus.publish('jobs', 'c', { partition: 2 });
  bus.publish('jobs', 'd', { partition: 1 });
  bus.publish('jobs', 'e', { partition: 2 });
  await flush();
  assert.deepEqual(seen, [
    [2, 1],
    [0, 1],
    [2, 2],
    [1, 1],
    [2, 3],
  ]);
});

test('explicit partition wins over partitionKey', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 8);
  const seen: number[] = [];
  bus.subscribe('t', (m) => seen.push(m.partition ?? -1));
  bus.publish('t', 'a', { partitionKey: 'user-1', partition: 5 });
  await flush();
  assert.deepEqual(seen, [5]);
});

test('partitionKey consistent hashing: same key, same partition — deterministic across buses', async () => {
  const mk = () => {
    const bus = new EventBus();
    bus.setTopicPartitions('t', 16);
    const seen: number[] = [];
    bus.subscribe('t', (m) => seen.push(m.partition ?? -1));
    return { bus, seen };
  };
  const a = mk();
  const b = mk();
  for (const key of ['alice', 'bob', 'carol', 'alice', 'bob']) {
    a.bus.publish('t', key, { partitionKey: key });
    b.bus.publish('t', key, { partitionKey: key });
  }
  await flush();
  // Same key always lands on the same partition, on both buses.
  assert.deepEqual(a.seen, b.seen);
  assert.equal(a.seen[0], a.seen[3]); // alice twice
  assert.equal(a.seen[1], a.seen[4]); // bob twice
  // And the mapping actually spreads (16 partitions, 3 distinct keys —
  // a degenerate all-zero hash would fail this).
  assert.ok(new Set(a.seen).size >= 1);
});

test('single-partition default: behavior compatible with unpartitioned topics', async () => {
  const bus = new EventBus();
  const seen: Array<[number | undefined, number]> = [];
  bus.subscribe('plain', (m) => seen.push([m.partition, m.seq]));
  bus.publish('plain', 'a');
  bus.publish('plain', 'b', { partition: 0 }); // the only valid explicit value
  bus.publish('plain', 'c', { partitionKey: 'anything' }); // hashes to 0
  await flush();
  assert.deepEqual(seen, [
    [0, 1],
    [0, 2],
    [0, 3],
  ]);
  // Out of range on the default single partition.
  assert.throws(() => bus.publish('plain', 'd', { partition: 1 }), RangeError);
  assert.throws(() => bus.publish('plain', 'd', { partition: -1 }), RangeError);
  // A failed publish consumes nothing: the next valid publish keeps numbering.
  bus.publish('plain', 'e');
  await flush();
  assert.deepEqual(seen[3], [0, 4]);
});

test('publish validates partition options fail-fast with RangeError', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 4);
  assert.throws(() => bus.publish('t', 'x', { partitionKey: '' }), RangeError);
  assert.throws(() => bus.publish('t', 'x', { partitionKey: 42 as unknown as string }), RangeError);
  assert.throws(() => bus.publish('t', 'x', { partition: -1 }), RangeError);
  assert.throws(() => bus.publish('t', 'x', { partition: 1.5 }), RangeError);
  assert.throws(() => bus.publish('t', 'x', { partition: NaN }), RangeError);
  assert.throws(() => bus.publish('t', 'x', { partition: 4 }), RangeError); // >= count
  assert.throws(() => bus.publish('t', 'x', { partition: 99 }), RangeError);
});

test('gap detection is per (topic, partition): a drop on one partition is invisible to the other', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 2);
  const received: Array<[number, number]> = [];
  bus.subscribe('t', (m) => received.push([m.partition ?? -1, m.seq]), {
    queueSize: 2,
    backpressure: 'drop-oldest',
  });
  // Baselines: one message per partition.
  bus.publish('t', 'p0-1', { partition: 0 });
  bus.publish('t', 'p1-1', { partition: 1 });
  await flush();
  // Three rapid partition-0 publishes into a capacity-2 queue: the oldest
  // (partition 0, seq 2) is evicted while the partition-1 baseline is
  // untouched.
  bus.publish('t', 'p0-2', { partition: 0 });
  bus.publish('t', 'p0-3', { partition: 0 });
  bus.publish('t', 'p0-4', { partition: 0 });
  await flush();
  assert.deepEqual(received, [
    [0, 1],
    [1, 1],
    [0, 3],
    [0, 4],
  ]);
  const stats = bus.getStats();
  assert.equal(stats.topics[0].sequenceGaps, 1); // exactly partition-0 seq 2 went missing
  assert.equal(stats.sequenceGaps, 1);
  // Partition 1 keeps flowing gap-free afterwards.
  bus.publish('t', 'p1-2', { partition: 1 });
  await flush();
  assert.deepEqual(received[4], [1, 2]);
  assert.equal(bus.getStats().sequenceGaps, 1);
});

test('interleaved partitions never count each other as gaps', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 4);
  const received: Array<[number, number]> = [];
  bus.subscribe('t', (m) => received.push([m.partition ?? -1, m.seq]));
  for (let i = 0; i < 5; i += 1) {
    for (let p = 0; p < 4; p += 1) bus.publish('t', `m${i}-${p}`, { partition: p });
  }
  await flush();
  assert.equal(received.length, 20);
  assert.equal(bus.getStats().sequenceGaps, 0);
  assert.equal(bus.getStats().topics[0].sequenceGaps, 0);
});

test('getStats exposes per-partition publish counters only for partitioned topics', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('jobs', 3);
  bus.publish('jobs', 'a', { partition: 2 });
  bus.publish('jobs', 'b', { partition: 2 });
  bus.publish('jobs', 'c', { partition: 0 });
  bus.publish('plain', 'x');
  await flush();
  const topics = bus.getStats().topics;
  const jobs = topics.find((t) => t.topic === 'jobs');
  assert.deepEqual(jobs?.partitions, [
    { partition: 0, publishedMessages: 1, lastSeq: 1 },
    { partition: 1, publishedMessages: 0, lastSeq: 0 },
    { partition: 2, publishedMessages: 2, lastSeq: 2 },
  ]);
  // Topic-level totals stay consistent with the per-partition rows.
  assert.equal(jobs?.publishedMessages, 3);
  assert.equal(jobs?.lastSeq, 2);
  // Single-partition topics omit the field — snapshots stay byte-identical.
  const plain = topics.find((t) => t.topic === 'plain');
  assert.equal(plain?.partitions, undefined);
  assert.ok(!('partitions' in (plain ?? {})));
});

test('metrics: eventbus_topic_partition_published_total per (topic, partition)', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('jobs', 2);
  bus.publish('jobs', 'a', { partition: 1 });
  bus.publish('jobs', 'b', { partition: 1 });
  bus.publish('jobs', 'c', { partition: 0 });
  bus.publish('plain', 'x');
  await flush();
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_topic_partition_published_total{topic="jobs",partition="0"} 1'));
  assert.ok(text.includes('eventbus_topic_partition_published_total{topic="jobs",partition="1"} 2'));
  assert.ok(!text.includes('topic="plain",partition='));
  // A bus without partitioned topics renders no such series at all.
  const plain2 = new EventBus();
  plain2.publish('solo', 'x');
  await flush();
  assert.ok(!renderPrometheus(plain2.getStats()).includes('eventbus_topic_partition_published_total'));
});

test('durable log: restart rebuilds per-partition seq counters — no restart, no reuse', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus1.setTopicPartitions('jobs', 3);
  bus1.publish('jobs', 'a', { partition: 2 });
  bus1.publish('jobs', 'b', { partition: 2 });
  bus1.publish('jobs', 'c', { partition: 0 });
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus2.setTopicPartitions('jobs', 3);
  const received: BusMessage[] = [];
  bus2.subscribe('jobs', (m) => received.push(m));
  bus2.publish('jobs', 'd', { partition: 2 });
  bus2.publish('jobs', 'e', { partition: 0 });
  bus2.publish('jobs', 'f', { partition: 1 });
  await flush();
  // Numbering continues per partition: partition 2 at 3, partition 0 at 2,
  // partition 1 starts at 1.
  assert.deepEqual(
    received.map((m) => [m.partition, m.seq]),
    [
      [2, 3],
      [0, 2],
      [1, 1],
    ],
  );
  const jobs = bus2.getStats().topics.find((t) => t.topic === 'jobs');
  assert.deepEqual(jobs?.partitions, [
    { partition: 0, publishedMessages: 2, lastSeq: 2 },
    { partition: 1, publishedMessages: 1, lastSeq: 1 },
    { partition: 2, publishedMessages: 3, lastSeq: 3 },
  ]);
});

test('durable log: resumeFromSeq replays partitioned history with partition identity', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.setTopicPartitions('jobs', 2);
  const live: Array<[number | undefined, number]> = [];
  bus.subscribe('jobs', (m) => live.push([m.partition, m.seq]));
  bus.publish('jobs', 'a', { partition: 1 });
  bus.publish('jobs', 'b', { partition: 0 });
  bus.publish('jobs', 'c', { partition: 1 });
  await flush();
  const resumed: Array<[number | undefined, number]> = [];
  bus.subscribe('jobs', (m) => resumed.push([m.partition, m.seq]), { resumeFromSeq: 0 });
  await flush();
  // Every replayed envelope keeps the partition it was logged with.
  assert.deepEqual(resumed, [
    [1, 1],
    [0, 1],
    [1, 2],
  ]);
});

test('keyed messages on different partitions keep per-partition seqs and global key order', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 4);
  const byPartition = new Map<number, number[]>();
  const keyOrder: string[] = [];
  bus.subscribe('t', (m) => {
    const p = m.partition ?? -1;
    if (!byPartition.has(p)) byPartition.set(p, []);
    byPartition.get(p)?.push(m.seq);
    keyOrder.push(m.payload as string);
  });
  bus.publish('t', 'k-a-1', { key: 'k', partition: 0 });
  bus.publish('t', 'k-a-2', { key: 'k', partition: 2 });
  bus.publish('t', 'k-a-3', { key: 'k', partition: 1 });
  await flush();
  // Per-partition seqs are independent (each partition's first message is 1)...
  assert.deepEqual(byPartition.get(0), [1]);
  assert.deepEqual(byPartition.get(2), [1]);
  assert.deepEqual(byPartition.get(1), [1]);
  // ...while the keyed publish order is preserved across partitions.
  assert.deepEqual(keyOrder, ['k-a-1', 'k-a-2', 'k-a-3']);
});

test('publishBatch carries per-entry partition routing', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 4);
  const seen: Array<[number | undefined, number]> = [];
  bus.subscribe('t', (m) => seen.push([m.partition, m.seq]));
  bus.publishBatch([
    { topic: 't', payload: 'a', partition: 3 },
    { topic: 't', payload: 'b', partitionKey: 'user-9' },
    { topic: 't', payload: 'c' },
  ]);
  await flush();
  assert.equal(seen.length, 3);
  assert.equal(seen[0][0], 3);
  assert.equal(seen[0][1], 1);
  assert.equal(seen[2][0], 0); // no routing: partition 0
  assert.equal(seen[2][1], 1);
  // partitionKey entries land deterministically on one partition.
  assert.equal(seen[1][0], seen[1][0]);
  assert.throws(
    () => bus.publishBatch([{ topic: 't', payload: 'bad', partition: 4 }]),
    RangeError,
  );
});

test('publishAtomic carries per-entry partition routing; a bad entry aborts the batch', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 2);
  const seen: Array<[number | undefined, number]> = [];
  bus.subscribe('t', (m) => seen.push([m.partition, m.seq]));
  const ok = bus.publishAtomic([
    { topic: 't', payload: 'a', partition: 1 },
    { topic: 't', payload: 'b', partitionKey: 'k' },
  ]);
  assert.deepEqual(ok, { published: 2 });
  await flush();
  assert.equal(seen.length, 2);
  assert.equal(seen[0][0], 1);
  // An out-of-range partition aborts before commit: nothing is published.
  assert.throws(
    () =>
      bus.publishAtomic([
        { topic: 't', payload: 'a', partition: 1 },
        { topic: 't', payload: 'bad', partition: 7 },
      ]),
    RangeError,
  );
  await flush();
  assert.equal(seen.length, 2); // no partial commit
});

test('publishIdempotent carries partition routing', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 4);
  const seen: Array<[number | undefined, number]> = [];
  bus.subscribe('t', (m) => seen.push([m.partition, m.seq]));
  const r1 = bus.publishIdempotent('t', 'a', { messageId: 'm1', partition: 2 });
  const r2 = bus.publishIdempotent('t', 'a', { messageId: 'm1', partition: 2 });
  assert.deepEqual([r1.duplicate, r2.duplicate], [false, true]);
  await flush();
  assert.deepEqual(seen, [[2, 1]]);
});

test('retained snapshot keeps the message partition', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 4);
  bus.setTopicRetained('t', true);
  bus.publish('t', 'v1', { retain: true, partition: 3 });
  await flush();
  const seen: Array<[number | undefined, number, unknown]> = [];
  bus.subscribe('t', (m) => seen.push([m.partition, m.seq, m.payload]));
  await flush();
  assert.deepEqual(seen, [[3, 1, 'v1']]);
});

test('clearTopicPartitions drops back to a single partition', async () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 4);
  assert.equal(bus.clearTopicPartitions('t'), true);
  const seen: Array<[number | undefined, number]> = [];
  bus.subscribe('t', (m) => seen.push([m.partition, m.seq]));
  bus.publish('t', 'a', { partitionKey: 'k' });
  await flush();
  assert.deepEqual(seen, [[0, 1]]);
  assert.throws(() => bus.publish('t', 'b', { partition: 2 }), RangeError);
  // getStats no longer exposes per-partition rows.
  assert.equal(bus.getStats().topics[0].partitions, undefined);
});

test('partition rules are audited', () => {
  const bus = new EventBus();
  bus.setTopicPartitions('t', 3);
  bus.clearTopicPartitions('t');
  const actions = bus.getSubscriptionAudit().map((e) => e.action);
  assert.ok(actions.includes('partition_rule_set'));
  assert.ok(actions.includes('partition_rule_cleared'));
});

test('durable log: archived segments fold per-partition seqs into recovery — no reuse', async () => {
  const dir = freshDir();
  const bus1 = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 4 },
  });
  bus1.setTopicPartitions('t', 2);
  bus1.publish('t', 'a', { partition: 0 });
  bus1.publish('t', 'b', { partition: 1 });
  bus1.publish('t', 'c', { partition: 0 });
  bus1.publish('t', 'd', { partition: 1 }); // 4th append closes the segment
  await waitForArchived(dir, 't');
  // Reopen over the archived segment: numbering continues per partition.
  const bus2 = new EventBus({ durableLogDir: dir });
  bus2.setTopicPartitions('t', 2);
  const seen: Array<[number | undefined, number]> = [];
  bus2.subscribe('t', (m) => seen.push([m.partition, m.seq]));
  bus2.publish('t', 'e', { partition: 0 });
  bus2.publish('t', 'f', { partition: 1 });
  await flush();
  assert.deepEqual(seen, [
    [0, 3],
    [1, 3],
  ]);
});
