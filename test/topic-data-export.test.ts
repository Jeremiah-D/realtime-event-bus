import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { exportTopicData, importTopicData } from '../src/topic-data-export.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'eb-topic-data-'));

function readHistory(dir: string, topic: string) {
  const log = DurableTopicLog.open({ dir });
  return log.readSince(topic, 0);
}

test('export orders by seq and carries only seq/payload/key/timestamp, signatures stripped', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  // Appends in seq order (the order the bus always writes in —
  // the log's seq-indexed read path assumes it) with a signed
  // record and a keyed record: export must come out in seq
  // order, with exactly the format fields.
  log.append({
    seq: 1,
    topic: 'events',
    at: 1_000,
    payload: { n: 1 },
    signature: { kid: 'k1', sig: 'a'.repeat(64) },
    publisherSignature: { kid: 'p1', sig: 'b'.repeat(64) },
  });
  log.append({ seq: 2, topic: 'events', at: 2_000, payload: { n: 2 }, key: 'k2' });
  log.append({ seq: 3, topic: 'events', at: 3_000, payload: { n: 3 } });

  const jsonl = exportTopicData(log, 'events');
  const lines = jsonl.trim().split('\n');
  assert.equal(lines.length, 3);
  const records = lines.map((line) => JSON.parse(line) as Record<string, unknown>);
  assert.deepEqual(records.map((r) => r['seq']), [1, 2, 3]);
  assert.deepEqual(records.map((r) => r['payload']), [{ n: 1 }, { n: 2 }, { n: 3 }]);
  assert.deepEqual(records.map((r) => r['timestamp']), [1_000, 2_000, 3_000]);
  assert.deepEqual(Object.keys(records[0] as object).sort(), ['payload', 'seq', 'timestamp']);
  assert.deepEqual(Object.keys(records[1] as object).sort(), ['key', 'payload', 'seq', 'timestamp']);
  assert.equal(records[1]?.['key'], 'k2');
  // No signature leakage, in either spelling, on any line.
  assert.ok(!jsonl.includes('signature'));
  assert.ok(!jsonl.includes('a'.repeat(64)));
  assert.ok(!jsonl.includes('b'.repeat(64)));
});

test('export rejects an unknown topic and a binary record, fail-closed', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  assert.throws(() => exportTopicData(log, 'nope'), /unknown topic/);
  log.append({ seq: 1, topic: 'bin', at: 1_000, payload: new Uint8Array([1, 2, 3]), binary: true });
  assert.throws(() => exportTopicData(log, 'bin'), /binary/);
});

test('import renumbers from 1 in import order, preserving payload, key and timestamp', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  const result = importTopicData(
    log,
    'copy',
    [
      { seq: 9, payload: 'second', key: 'k', timestamp: 9_000 },
      { seq: 4, payload: 'first', timestamp: 4_000 },
    ],
  );
  assert.equal(result.targetTopic, 'copy');
  assert.equal(result.imported, 2);
  assert.equal(result.targetLastSeq, 2);
  const history = log.readSince('copy', 0);
  assert.deepEqual(history.map((r) => r.seq), [1, 2]);
  assert.deepEqual(history.map((r) => r.payload), ['second', 'first']);
  assert.deepEqual(history.map((r) => r.at), [9_000, 4_000]);
  assert.equal(history[0]?.key, 'k');
  assert.equal(history[1]?.key, undefined);
});

test('import into an existing topic fails closed and leaves history untouched', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  log.append({ seq: 1, topic: 'taken', at: 1_000, payload: 'orig' });
  assert.throws(
    () => importTopicData(log, 'taken', [{ seq: 1, payload: 'new', timestamp: 2_000 }]),
    /already in use/,
  );
  assert.deepEqual(log.readSince('taken', 0).map((r) => r.payload), ['orig']);
});

test('a bad record fails the whole batch with zero writes, in both JSONL and array form', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  const badJsonl =
    '{"seq":1,"payload":"ok","timestamp":1000}\n' +
    '{"seq":2,"timestamp":2000}\n' + // missing payload
    '{"seq":3,"payload":"never","timestamp":3000}\n';
  assert.throws(() => importTopicData(log, 't1', badJsonl), /missing its payload/);
  assert.ok(!log.topics().includes('t1'));

  const badJson = '{"seq":1,"payload":"ok","timestamp":1000}\nnot json\n';
  assert.throws(() => importTopicData(log, 't2', badJson), /not valid JSON/);
  assert.ok(!log.topics().includes('t2'));

  const badSeq = [{ seq: 0, payload: 'x', timestamp: 1000 }];
  assert.throws(() => importTopicData(log, 't3', badSeq), /invalid seq/);
  assert.ok(!log.topics().includes('t3'));

  const badTimestamp = [{ seq: 1, payload: 'x', timestamp: 'soon' }];
  assert.throws(() => importTopicData(log, 't4', badTimestamp), /invalid timestamp/);
  assert.ok(!log.topics().includes('t4'));

  const badKey = [{ seq: 1, payload: 'x', timestamp: 1000, key: 42 }];
  assert.throws(() => importTopicData(log, 't5', badKey), /invalid key/);
  assert.ok(!log.topics().includes('t5'));

  // A rejected batch leaves no partial target behind: the same
  // name imports cleanly afterwards.
  const ok = importTopicData(log, 't1', [{ seq: 7, payload: 'retry', timestamp: 1000 }]);
  assert.equal(ok.imported, 1);
  assert.deepEqual(log.readSince('t1', 0).map((r) => r.payload), ['retry']);
});

test('empty history export/import round-trips as zero records', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  // An empty batch imports as zero records and creates no log
  // state — the target stays unknown until a real record lands.
  const imported = importTopicData(log, 'empty', '');
  assert.equal(imported.imported, 0);
  assert.equal(imported.targetLastSeq, 0);
  assert.ok(!log.topics().includes('empty'));
  const importedArray = importTopicData(log, 'empty2', []);
  assert.equal(importedArray.imported, 0);
  // A topic the log knows but with no messages (only a seq-0
  // schedule record, which is runtime state, not history)
  // exports as the empty string.
  log.append({ seq: 0, topic: 'sched-only', at: 1_000, payload: null, delayId: 'delayed-1' });
  assert.equal(exportTopicData(log, 'sched-only'), '');
});

test('bus round-trip: export from one bus, import into another, next publish continues numbering', async () => {
  const clock = controllableClock(1_000);
  const dirA = freshDir();
  const dirB = freshDir();
  const busA = new EventBus({ now: clock.now, durableLogDir: dirA });
  busA.publish('orders', { id: 1 });
  busA.publish('orders', { id: 2 }, { key: 'eu' });
  busA.publish('orders', { id: 3 });
  await flush();

  const jsonl = busA.exportTopicData('orders');
  assert.equal(jsonl.trim().split('\n').length, 3);

  const busB = new EventBus({ now: clock.now, durableLogDir: dirB });
  const result = busB.importTopicData('orders.v2', jsonl);
  assert.equal(result.imported, 3);
  assert.equal(result.targetLastSeq, 3);
  assert.equal(result.lifecycleProvisioned, true);

  assert.deepEqual(readHistory(dirB, 'orders.v2').map((r) => r.payload), [
    { id: 1 },
    { id: 2 },
    { id: 3 },
  ]);
  assert.deepEqual(readHistory(dirB, 'orders.v2').map((r) => r.seq), [1, 2, 3]);
  assert.equal(readHistory(dirB, 'orders.v2')[1]?.key, 'eu');

  // The imported topic is a live topic: the next publish
  // continues its numbering instead of colliding with imports.
  busB.publish('orders.v2', { id: 4 });
  await flush();
  assert.deepEqual(readHistory(dirB, 'orders.v2').map((r) => r.seq), [1, 2, 3, 4]);

  // Bus-level fail-closed: importing over an existing topic, or
  // exporting an unknown one, both throw.
  assert.throws(() => busB.importTopicData('orders.v2', jsonl), /already in use/);
  assert.throws(() => busB.exportTopicData('nope'), /unknown topic/);

  // An empty import leaves a known-but-historyless topic, which
  // exports as the empty string rather than an error.
  const emptyResult = busB.importTopicData('orders.empty', '');
  assert.equal(emptyResult.imported, 0);
  assert.equal(busB.exportTopicData('orders.empty'), '');
});
