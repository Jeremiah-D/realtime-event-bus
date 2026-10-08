import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-compact-'));

const linesOf = (dir: string, topic: string) =>
  readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0);

test('keyed compaction: repeated key updates collapse to the latest on disk', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 4 });
  for (let i = 1; i <= 5; i++) {
    log.append({ seq: i, topic: 'prices', at: 1000 + i, key: 'BTC', payload: { p: i } });
  }
  // 4 superseded records hit the keyed trigger (superseded >= maxEntries):
  // the file now holds only the latest value for the key.
  assert.equal(log.entryCount('prices'), 1);
  assert.deepEqual(linesOf(dir, 'prices').length, 1);
  const records = log.readSince('prices', 0);
  assert.deepEqual(
    records.map((r) => [r.seq, r.payload]),
    [[5, { p: 5 }]],
  );
});

test('keyed compaction: multiple keys each keep their latest; keyless capped', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 3 });
  log.append({ seq: 1, topic: 't', at: 1, payload: 'm1' });
  log.append({ seq: 2, topic: 't', at: 2, payload: 'm2' });
  log.append({ seq: 3, topic: 't', at: 3, payload: 'm3' });
  log.append({ seq: 4, topic: 't', at: 4, key: 'A', payload: 'a1' });
  log.append({ seq: 5, topic: 't', at: 5, key: 'B', payload: 'b1' });
  log.append({ seq: 6, topic: 't', at: 6, key: 'A', payload: 'a2' });
  log.append({ seq: 7, topic: 't', at: 7, key: 'A', payload: 'a3' });
  // 7 entries > 2 * 3: compact. Newest-first keeps a3, b1 (both keys),
  // then the 3 newest keyless (m1..m3) — keyed survivors are not capped.
  assert.deepEqual(
    log.readSince('t', 0).map((r) => [r.seq, r.key ?? null, r.payload]),
    [
      [1, null, 'm1'],
      [2, null, 'm2'],
      [3, null, 'm3'],
      [5, 'B', 'b1'],
      [7, 'A', 'a3'],
    ],
  );
  assert.equal(log.entryCount('t'), 5);
});

test('keyed compaction: keyless messages beyond the budget are dropped', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 2 });
  for (let i = 1; i <= 5; i++) log.append({ seq: i, topic: 't', at: i, payload: `m${i}` });
  log.append({ seq: 6, topic: 't', at: 6, key: 'A', payload: 'a1' });
  // count hits 6 > 2*2: compact keeps the 2 newest keyless + the keyed one.
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.seq),
    [4, 5, 6],
  );
});

test('keyed compaction: readSince dedupes even before compaction runs', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 1000 });
  log.append({ seq: 1, topic: 't', at: 1, key: 'K', payload: 'v1' });
  log.append({ seq: 2, topic: 't', at: 2, payload: 'plain' });
  log.append({ seq: 3, topic: 't', at: 3, key: 'K', payload: 'v2' });
  // No compaction triggered (superseded 1 < 1000, count 3 < 2000), but a
  // superseded value is never replayed.
  assert.equal(log.entryCount('t'), 3);
  assert.deepEqual(
    log.readSince('t', 0).map((r) => [r.seq, r.payload]),
    [
      [2, 'plain'],
      [3, 'v2'],
    ],
  );
  // A resume bound below the superseded record still skips it.
  assert.deepEqual(
    log.readSince('t', 1).map((r) => r.seq),
    [2, 3],
  );
});

test('keyed compaction: restart recovery rebuilds the key index', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 1000 });
  log.append({ seq: 1, topic: 't', at: 1, key: 'K', payload: 'v1' });
  log.append({ seq: 2, topic: 't', at: 2, key: 'K', payload: 'v2' });
  const reopened = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 1000 });
  assert.deepEqual(
    reopened.readSince('t', 0).map((r) => [r.seq, r.payload]),
    [[2, 'v2']],
  );
  // The rebuilt index knows K is taken: the next update supersedes v2.
  reopened.append({ seq: 3, topic: 't', at: 3, key: 'K', payload: 'v3' });
  assert.deepEqual(
    reopened.readSince('t', 0).map((r) => [r.seq, r.payload]),
    [[3, 'v3']],
  );
  assert.equal(reopened.lastSeq('t'), 3);
});

test('keyed compaction: seq-0 schedule records survive and are never deduped', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 2 });
  log.append({ seq: 0, topic: 't', at: 1, deliverAt: 5000, delayId: 'delayed-1', payload: 'sched' });
  log.append({ seq: 1, topic: 't', at: 2, key: 'K', payload: 'v1' });
  log.append({ seq: 2, topic: 't', at: 3, key: 'K', payload: 'v2' });
  log.append({ seq: 3, topic: 't', at: 4, key: 'K', payload: 'v3' });
  // superseded hits 2 >= maxEntries 2: compact keeps the schedule (timer
  // intent, never compacted away) plus the latest keyed message.
  const all = log.readSince('t', -1);
  assert.deepEqual(
    all.map((r) => [r.seq, r.delayId ?? null]),
    [
      [0, 'delayed-1'],
      [3, null],
    ],
  );
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.seq),
    [3],
  );
});

test('keyed compaction: disabled flag keeps every record (old behavior)', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, maxEntriesPerTopic: 1000 });
  log.append({ seq: 1, topic: 't', at: 1, key: 'K', payload: 'v1' });
  log.append({ seq: 2, topic: 't', at: 2, key: 'K', payload: 'v2' });
  // Keys are still persisted on disk (the format is stable), but without
  // the flag nothing dedupes.
  assert.deepEqual(
    log.readSince('t', 0).map((r) => [r.seq, r.payload]),
    [
      [1, 'v1'],
      [2, 'v2'],
    ],
  );
  assert.equal(log.stats().keyCompaction, false);
});

test('keyed compaction: malformed key lines are corrupt, not fatal', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, keyCompaction: true });
  log.append({ seq: 1, topic: 't', at: 1, key: 'K', payload: 'ok' });
  writeFileSync(
    join(dir, `${encodeURIComponent('t')}.log`),
    '{"v":1,"seq":2,"topic":"t","at":2,"key":123,"payload":"bad"}\n' +
      '{"v":1,"seq":3,"topic":"t","at":3,"key":"","payload":"empty"}\n',
    { flag: 'a' },
  );
  const reopened = DurableTopicLog.open({ dir, keyCompaction: true });
  assert.equal(reopened.stats().corruptLines, 2);
  assert.deepEqual(
    reopened.readSince('t', 0).map((r) => r.seq),
    [1],
  );
});

test('keyed compaction: bus end-to-end — keyed publishes replay only the latest', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogKeyCompaction: true, durableLogMaxEntriesPerTopic: 3 });
  bus.publish('prices', { p: 1 }, { key: 'BTC' });
  bus.publish('prices', { p: 2 }, { key: 'BTC' });
  bus.publish('prices', { p: 3 }, { key: 'ETH' });
  bus.publish('prices', { p: 4 }, { key: 'BTC' });
  await flush();
  assert.equal(bus.getStats().durableLog?.keyCompaction, true);
  // A restarted bus replays only the latest value per key.
  const bus2 = new EventBus({ durableLogDir: dir, durableLogKeyCompaction: true });
  const received: BusMessage[] = [];
  bus2.subscribe('prices', (m) => received.push(m), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    [{ p: 3 }, { p: 4 }],
  );
  // Per-topic seqs continued without reuse.
  bus2.publish('prices', { p: 5 }, { key: 'BTC' });
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    [{ p: 3 }, { p: 4 }, { p: 5 }],
  );
});

test('keyed compaction: publishBatch and publishAtomic carry keys', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogKeyCompaction: true, durableLogMaxEntriesPerTopic: 1000 });
  bus.publishBatch([
    { topic: 't', payload: 'a1', key: 'A' },
    { topic: 't', payload: 'b1', key: 'B' },
  ]);
  const res = bus.publishAtomic([
    { topic: 't', payload: 'a2', key: 'A' },
    { topic: 't', payload: 'plain' },
  ]);
  assert.deepEqual(res, { published: 2 });
  await flush();
  const log = DurableTopicLog.open({ dir, keyCompaction: true });
  assert.deepEqual(
    log.readSince('t', 0).map((r) => [r.seq, r.key ?? null]),
    [
      [2, 'B'],
      [3, 'A'],
      [4, null],
    ],
  );
});

test('keyed compaction: invalid keys throw RangeError and mutate nothing', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, durableLogKeyCompaction: true });
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  assert.throws(() => bus.publish('t', {}, { key: '' }), RangeError);
  assert.throws(() => bus.publish('t', {}, { key: 123 as unknown as string }), RangeError);
  assert.throws(() => bus.publishBatch([{ topic: 't', payload: {}, key: '' }]), RangeError);
  assert.throws(
    () =>
      bus.publishAtomic([
        { topic: 't', payload: 'ok' },
        { topic: 't', payload: {}, key: '' },
      ]),
    RangeError,
  );
  // The failed atomic batch left zero state: no seq consumed, nothing logged.
  bus.publish('t', { ok: true });
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    [{ ok: true }],
  );
  assert.equal(received[0].seq, 1);
  const log = DurableTopicLog.open({ dir, keyCompaction: true });
  assert.equal(log.messageCount('t'), 1);
});

test('keyed compaction: publishDelayed carries the key through restart', async () => {
  const dir = freshDir();
  const clock = controllableClock();
  const bus = new EventBus({ durableLogDir: dir, durableLogKeyCompaction: true, now: clock.now });
  const id = bus.publishDelayed('prices', { p: 9 }, { delayMs: 1000, key: 'BTC' });
  assert.ok(typeof id === 'string');
  assert.throws(() => bus.publishDelayed('t', {}, { delayMs: 1, key: '' }), RangeError);
  // Restart after the due time: recovery rebuilds the timer with its key,
  // and the delivery record carries it.
  clock.nowMs += 2000;
  const bus2 = new EventBus({ durableLogDir: dir, durableLogKeyCompaction: true, now: clock.now });
  await flush();
  const log = DurableTopicLog.open({ dir, keyCompaction: true });
  const deliveries = log.readSince('prices', 0);
  assert.equal(deliveries.length, 1);
  assert.equal(deliveries[0].key, 'BTC');
  assert.equal(deliveries[0].delayId, id);
  assert.deepEqual(deliveries[0].payload, { p: 9 });
  // The rebuilt timer already fanned out in bus2's constructor sweep, so a
  // late subscriber replays the keyed delivery.
  const received: BusMessage[] = [];
  bus2.subscribe('prices', (m) => received.push(m), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(
    received.map((m) => m.payload),
    [{ p: 9 }],
  );
});

test('keyed compaction: key option works without a durable log', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  const accepted = bus.publish('t', { v: 1 }, { key: 'K' });
  await flush();
  assert.equal(accepted, 1);
  assert.deepEqual(
    received.map((m) => m.payload),
    [{ v: 1 }],
  );
});
