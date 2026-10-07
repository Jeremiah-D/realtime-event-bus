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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-'));

test('DurableTopicLog: append and readSince round-trip in seq order', () => {
  const log = DurableTopicLog.open({ dir: freshDir() });
  log.append({ seq: 1, topic: 'a.b', at: 1000, payload: { x: 1 } });
  log.append({ seq: 2, topic: 'a.b', at: 1001, payload: [1, 2] });
  log.append({ seq: 1, topic: 'c', at: 1002, payload: 's' });
  assert.deepEqual(
    log.readSince('a.b', 0).map((r) => r.seq),
    [1, 2],
  );
  assert.deepEqual(
    log.readSince('a.b', 1).map((r) => r.payload),
    [[1, 2]],
  );
  assert.deepEqual(
    log.readSince('c', 0).map((r) => r.topic),
    ['c'],
  );
  assert.deepEqual(log.topics().sort(), ['a.b', 'c']);
  assert.equal(log.lastSeq('a.b'), 2);
  assert.equal(log.stats().entries, 3);
});

test('DurableTopicLog: reopening recovers topics, seqs and counts', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  log.append({ seq: 1, topic: 'a.b', at: 1000, payload: 'one' });
  log.append({ seq: 2, topic: 'a.b', at: 1001, payload: 'two' });
  const reopened = DurableTopicLog.open({ dir });
  assert.deepEqual(reopened.topics(), ['a.b']);
  assert.equal(reopened.lastSeq('a.b'), 2);
  assert.deepEqual(
    reopened.readSince('a.b', 0).map((r) => r.payload),
    ['one', 'two'],
  );
});

test('DurableTopicLog: corrupt lines are skipped and counted, not fatal', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  log.append({ seq: 1, topic: 'a.b', at: 1000, payload: 'ok' });
  writeFileSync(
    join(dir, `${encodeURIComponent('a.b')}.log`),
    'not json\n{"v":1,"seq":2,"topic":"wrong","at":1,"payload":null}\n',
    { flag: 'a' },
  );
  const reopened = DurableTopicLog.open({ dir });
  assert.equal(reopened.stats().corruptLines, 2);
  // The good record survives the corruption around it.
  assert.deepEqual(
    reopened.readSince('a.b', 0).map((r) => r.seq),
    [1],
  );
});

test('DurableTopicLog: compaction keeps the newest entries when over budget', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, maxEntriesPerTopic: 5 });
  for (let i = 1; i <= 11; i += 1) {
    log.append({ seq: i, topic: 'hot', at: 1000 + i, payload: i });
  }
  // 11 > 2*5 triggers one compaction down to the newest 5.
  assert.deepEqual(
    log.readSince('hot', 0).map((r) => r.seq),
    [7, 8, 9, 10, 11],
  );
  assert.equal(log.lastSeq('hot'), 11);
  assert.equal(log.entryCount('hot'), 5);
});

test('DurableTopicLog: rejects invalid options', () => {
  assert.throws(() => DurableTopicLog.open({ dir: '' }), RangeError);
  assert.throws(() => DurableTopicLog.open({ dir: freshDir(), maxEntriesPerTopic: 0 }), RangeError);
  assert.throws(() => DurableTopicLog.open({ dir: freshDir(), maxEntriesPerTopic: 1.5 }), RangeError);
});

test('bus: every publish is logged with its seq and TTL deadline', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.setTopicTtl('market.*', 100);
  const received: BusMessage[] = [];
  bus.subscribe('market.*', (m) => received.push(m));
  bus.publish('market.btc', { price: 1 });
  bus.publish('market.eth', { price: 2 });
  await flush();
  assert.equal(received.length, 2);
  const log = DurableTopicLog.open({ dir });
  const btc = log.readSince('market.btc', 0);
  assert.equal(btc.length, 1);
  assert.equal(btc[0].seq, 1);
  assert.equal(btc[0].expiresAt, 1100); // published at t=1000, TTL 100ms
  assert.deepEqual(btc[0].payload, { price: 1 });
  const stats = bus.getStats();
  assert.equal(stats.durableLog?.entries, 2);
  assert.equal(stats.durableLog?.topics, 2);
  assert.equal(stats.durableLog?.dir, dir);
});

test('bus: restart recovers seq continuity — no seq reuse after reopen', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus1.publish('jobs.email', 'a');
  bus1.publish('jobs.email', 'b');
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: BusMessage[] = [];
  bus2.subscribe('jobs.*', (m) => received.push(m));
  bus2.publish('jobs.email', 'c');
  await flush();
  // The restarted bus continues numbering at 3, and stats seed from history.
  assert.deepEqual(received.map((m) => m.seq), [3]);
  const topics = bus2.getStats().topics;
  assert.equal(topics[0].lastSeq, 3);
  assert.equal(topics[0].publishedMessages, 3);
});

test('bus: resumeFromSeq replays only the missed messages, in order', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const live: number[] = [];
  bus.subscribe('jobs.*', (m) => live.push(m.seq));
  for (let i = 0; i < 5; i += 1) bus.publish('jobs.email', i);
  await flush();
  assert.deepEqual(live, [1, 2, 3, 4, 5]);
  // A second consumer joins late, resuming after seq 3.
  const resumed: BusMessage[] = [];
  bus.subscribe('jobs.*', (m) => resumed.push(m), { resumeFromSeq: 3 });
  await flush();
  assert.deepEqual(resumed.map((m) => m.seq), [4, 5]);
  assert.deepEqual(resumed.map((m) => m.payload), [3, 4]);
});

test('bus: resumeFromSeq=0 replays everything logged for the pattern only', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('market.btc', 'btc1');
  bus.publish('orders.new', 'order1');
  bus.publish('market.eth', 'eth1');
  const resumed: BusMessage[] = [];
  bus.subscribe('market.*', (m) => resumed.push(m), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(resumed.map((m) => m.topic), ['market.btc', 'market.eth']);
  assert.deepEqual(resumed.map((m) => m.seq), [1, 1]); // per-topic seqs
});

test('bus: resumeFromSeq without durableLogDir throws instead of replaying nothing', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.subscribe('a.*', () => {}, { resumeFromSeq: 0 }),
    /durableLogDir/,
  );
  // And a failed resume leaves no half-registered subscriber behind.
  assert.equal(bus.subscriberCount(), 0);
});

test('bus: resumeFromSeq validates its value', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  for (const bad of [-1, 1.5, Number.NaN]) {
    assert.throws(
      () => bus.subscribe('a.*', () => {}, { resumeFromSeq: bad }),
      RangeError,
    );
  }
  assert.equal(bus.subscriberCount(), 0);
});

test('bus: replayed messages keep their TTL deadline — already-expired ones count as expired', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.setTopicTtl('jobs.*', 100); // deadline t=1100 for t=1000 publishes
  bus.publish('jobs.email', 'stale1');
  bus.publish('jobs.email', 'stale2');
  clock.nowMs = 1_200; // past the deadline before the resubscriber arrives
  const resumed: BusMessage[] = [];
  bus.subscribe('jobs.*', (m) => resumed.push(m), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(resumed, []); // dropped as expired at drain, not resurrected
  assert.equal(bus.getStats().expiredMessages, 2);
});

test('bus: replayed messages do not create phantom sequence gaps', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) bus.publish('jobs.email', i);
  const resumed: BusMessage[] = [];
  bus.subscribe('jobs.*', (m) => resumed.push(m), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(resumed.map((m) => m.seq), [1, 2, 3]);
  assert.equal(bus.getStats().sequenceGaps, 0);
  // Live messages after the replay continue the gap-free stream.
  bus.publish('jobs.email', 3);
  await flush();
  assert.deepEqual(resumed.map((m) => m.seq), [1, 2, 3, 4]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('bus: consumer-group members replay independently from their own offset', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  for (let i = 0; i < 4; i += 1) bus.publish('jobs.email', i);
  const seenA: number[] = [];
  const seenB: number[] = [];
  bus.subscribeToGroup('workers', 'jobs.*', (m) => seenA.push(m.seq), { resumeFromSeq: 2 });
  bus.subscribeToGroup('workers', 'jobs.*', (m) => seenB.push(m.seq), { resumeFromSeq: 0 });
  await flush();
  // Per-member replay: A resumes after its own offset 2, B replays everything.
  assert.deepEqual(seenA, [3, 4]);
  assert.deepEqual(seenB, [1, 2, 3, 4]);
});

test('bus: unserializable payloads are delivered live but skipped by the log', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const received: unknown[] = [];
  bus.subscribe('a.*', (m) => received.push(m.payload));
  bus.publish('a.b', { n: 10n }); // BigInt: JSON.stringify throws
  await flush();
  assert.equal(received.length, 1); // live delivery unaffected
  const log = DurableTopicLog.open({ dir });
  assert.deepEqual(log.readSince('a.b', 0), []); // nothing persisted
});

test('bus: log file is one JSON object per line, human-inspectable', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('a.b', { x: 1 });
  const text = readFileSync(join(dir, `${encodeURIComponent('a.b')}.log`), 'utf8');
  const lines = text.trim().split('\n');
  assert.equal(lines.length, 1);
  const parsed = JSON.parse(lines[0]) as Record<string, unknown>;
  assert.equal(parsed['v'], 1);
  assert.equal(parsed['seq'], 1);
  assert.equal(parsed['topic'], 'a.b');
  assert.deepEqual(parsed['payload'], { x: 1 });
});
