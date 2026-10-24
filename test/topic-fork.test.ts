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

const freshDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'eb-fork-'));

function readHistory(dir: string, topic: string) {
  const log = DurableTopicLog.open({ dir });
  return log.readSince(topic, 0);
}

test('forkTopic copies live history with a fresh sequence space and leaves the source untouched', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('market.btc', { price: 1 });
  bus.publish('market.btc', { price: 2 });
  bus.publish('market.btc', { price: 3 });
  await flush();

  const result = bus.forkTopic('market.btc', 'market.btc.v2');
  assert.equal(result.sourceTopic, 'market.btc');
  assert.equal(result.targetTopic, 'market.btc.v2');
  assert.equal(result.copied, 3);
  assert.equal(result.sourceLastSeq, 3);
  assert.equal(result.targetLastSeq, 3);
  assert.equal(result.lifecycleProvisioned, true);

  // The fork reads as a topic that was always the fork: same
  // payloads, same order, re-seqed from 1.
  const forked = readHistory(dir, 'market.btc.v2');
  assert.deepEqual(forked.map((r) => r.seq), [1, 2, 3]);
  assert.deepEqual(forked.map((r) => r.payload), [{ price: 1 }, { price: 2 }, { price: 3 }]);
  assert.ok(forked.every((r) => r.topic === 'market.btc.v2'));

  // The source is untouched: same seqs, same payloads.
  const source = readHistory(dir, 'market.btc');
  assert.deepEqual(source.map((r) => r.seq), [1, 2, 3]);
  assert.deepEqual(source.map((r) => r.payload), [{ price: 1 }, { price: 2 }, { price: 3 }]);

  // Both topics evolve independently afterwards, each continuing
  // its own numbering — no shared sequence space, no sync.
  const forkReceived: unknown[] = [];
  bus.subscribe('market.btc.v2', (m) => forkReceived.push(m.payload));
  bus.publish('market.btc.v2', { price: 4 });
  bus.publish('market.btc', { price: 5 });
  await flush();
  assert.deepEqual(forkReceived, [{ price: 4 }]);
  const forkedAfter = readHistory(dir, 'market.btc.v2');
  assert.deepEqual(forkedAfter.map((r) => r.seq), [1, 2, 3, 4]);
  const sourceAfter = readHistory(dir, 'market.btc');
  assert.deepEqual(sourceAfter.map((r) => r.seq), [1, 2, 3, 4]);
  assert.deepEqual(sourceAfter[3].payload, { price: 5 });
  const stats = bus.getStats();
  const forkStats = stats.topics.find((t) => t.topic === 'market.btc.v2');
  assert.ok(forkStats !== undefined);
  assert.equal(forkStats.lastSeq, 4);
  assert.equal(bus.getTopicLifecycle('market.btc.v2')?.provisioned, true);
});

test('forkTopic fromSeq is an exclusive lower bound on the source seq', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  for (let i = 1; i <= 5; i += 1) bus.publish('t', i);
  await flush();

  const result = bus.forkTopic('t', 't.v2', { fromSeq: 2 });
  assert.equal(result.copied, 3);
  const forked = readHistory(dir, 't.v2');
  assert.deepEqual(forked.map((r) => r.seq), [1, 2, 3]);
  assert.deepEqual(forked.map((r) => r.payload), [3, 4, 5]);
});

test('forkTopic fromTime is an exclusive lower bound on the publish timestamp', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('t', 'a');
  clock.nowMs = 2_000;
  bus.publish('t', 'b');
  clock.nowMs = 3_000;
  bus.publish('t', 'c');
  await flush();

  const result = bus.forkTopic('t', 't.v2', { fromTime: 2_000 });
  assert.equal(result.copied, 1);
  const forked = readHistory(dir, 't.v2');
  assert.deepEqual(forked.map((r) => r.payload), ['c']);
  // fromSeq and fromTime compose: a record copies only when it
  // passes both.
  const result2 = bus.forkTopic('t', 't.v3', { fromSeq: 1, fromTime: 500 });
  assert.equal(result2.copied, 2);
  assert.deepEqual(readHistory(dir, 't.v3').map((r) => r.payload), ['b', 'c']);
});

test('forkTopic keyedLatest keeps only the latest record per key', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('t', { sym: 'btc', price: 1 }, { key: 'btc' });
  bus.publish('t', { sym: 'eth', price: 2 }, { key: 'eth' });
  bus.publish('t', { sym: 'btc', price: 3 }, { key: 'btc' });
  bus.publish('t', 'keyless');
  await flush();

  const result = bus.forkTopic('t', 't.v2', { keyedLatest: true });
  assert.equal(result.copied, 3);
  const forked = readHistory(dir, 't.v2');
  assert.deepEqual(forked.map((r) => r.payload), [
    { sym: 'eth', price: 2 },
    { sym: 'btc', price: 3 },
    'keyless',
  ]);
  // Keyed records keep their key (and keySeq) on the fork.
  assert.equal(forked[0].key, 'eth');
  assert.equal(forked[1].key, 'btc');
});

test('forkTopic fails closed on unknown sources, used targets and bad options', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('t', 'x');
  bus.publish('taken', 'y');
  await flush();

  assert.throws(() => bus.forkTopic('nope', 'n1'), RangeError);
  assert.throws(() => bus.forkTopic('t', 'taken'), RangeError);
  assert.throws(() => bus.forkTopic('t', 't'), RangeError);
  assert.throws(() => bus.forkTopic('t', 'n1', { fromSeq: -1 }), RangeError);
  assert.throws(() => bus.forkTopic('t', 'n1', { fromSeq: 1.5 }), RangeError);
  assert.throws(() => bus.forkTopic('t', 'n1', { fromTime: -5 }), RangeError);
  assert.throws(() => bus.forkTopic('t', 'n1', { fromTime: Number.NaN }), RangeError);
  assert.throws(() => bus.forkTopic('market.*', 'n1'), RangeError);
  assert.throws(() => bus.forkTopic('ns/a', 'ns/b'), RangeError);
  // A subscribed-but-never-published topic has no log: fail-closed.
  bus.subscribe('unpublished', () => {});
  assert.throws(() => bus.forkTopic('unpublished', 'n2'), RangeError);
  // Nothing was created by the failed attempts.
  assert.equal(readHistory(dir, 'n1').length, 0);
  assert.equal(bus.getTopicLifecycle('n1'), undefined);

  // A bus without a durable log cannot fork: there is no history.
  const memoryBus = new EventBus();
  memoryBus.publish('m', 'x');
  assert.throws(() => memoryBus.forkTopic('m', 'm.v2'), RangeError);
});

test('forkTopic with filters matching nothing succeeds with zero copies', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  // The source has a log, but nothing passes the filter: a valid
  // fork with zero copies (distinct from the fail-closed no-log case).
  const result = bus.forkTopic('t', 't.v2', { fromSeq: 99 });
  assert.equal(result.copied, 0);
  assert.equal(result.targetLastSeq, 0);
  assert.equal(readHistory(dir, 't.v2').length, 0);
  // The empty fork still provisions and accepts new publishes,
  // numbered from 1 in its own sequence space.
  bus.publish('t.v2', 'first');
  await flush();
  assert.deepEqual(readHistory(dir, 't.v2').map((r) => r.seq), [1]);
});

test('DurableTopicLog.forkTopic drops per-(topic, seq) signatures on re-seq', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  // Signatures must be well-formed 64-hex HMACs or the log's line
  // parser treats the line as corrupt and skips it (EB-84 rule).
  const sigA = 'ab'.repeat(32);
  const sigB = 'cd'.repeat(32);
  log.append({ seq: 1, topic: 's', at: 1000, payload: 'a', signature: { kid: 'k1', sig: sigA } });
  log.append({ seq: 2, topic: 's', at: 1001, payload: 'b', publisherSignature: { kid: 'k2', sig: sigB } });
  const result = log.forkTopic('s', 's.v2');
  assert.equal(result.copied, 2);
  const forked = log.readSince('s.v2', 0);
  assert.deepEqual(forked.map((r) => r.seq), [1, 2]);
  assert.equal(forked[0].signature, undefined);
  assert.equal(forked[1].publisherSignature, undefined);
  // The source keeps its signatures.
  const source = log.readSince('s', 0);
  assert.deepEqual(source[0].signature, { kid: 'k1', sig: sigA });
  assert.deepEqual(source[1].publisherSignature, { kid: 'k2', sig: sigB });
  // Log-level fail-closed: unknown source, existing target.
  assert.throws(() => log.forkTopic('nope', 'x'), RangeError);
  assert.throws(() => log.forkTopic('s', 's.v2'), RangeError);
});
