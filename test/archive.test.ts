import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-archive-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

const segmentFiles = (dir: string, ext: '.jsonl.gz' | '.jsonl.tmp') =>
  readdirSync(dir).filter((f) => f.endsWith(ext));

/** Waits until the topic's closed segment is fully archived (gz + index, tmp gone). */
async function waitForArchived(dir: string, topic: string): Promise<string> {
  const prefix = `${encodeURIComponent(topic)}.seg-`;
  await waitFor(
    () =>
      segmentFiles(dir, '.jsonl.tmp').filter((f) => f.startsWith(prefix)).length === 0 &&
      segmentFiles(dir, '.jsonl.gz').filter((f) => f.startsWith(prefix)).length > 0,
  );
  const names = segmentFiles(dir, '.jsonl.gz').filter((f) => f.startsWith(prefix));
  assert.equal(names.length, 1);
  return names[0];
}

test('archive: count-based rotation closes the segment synchronously, archives in background', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
  for (let i = 1; i <= 5; i += 1) {
    log.append({ seq: i, topic: 't', at: 1000 + i, payload: i });
  }
  // Rotation fired synchronously on the 5th append: the live file is
  // renamed aside, but the gzip/index work has NOT run yet — the publish
  // path never waits for compression.
  const pending = segmentFiles(dir, '.jsonl.tmp');
  assert.equal(pending.length, 1);
  assert.ok(pending[0].startsWith(`${encodeURIComponent('t')}.seg-`));
  assert.deepEqual(segmentFiles(dir, '.jsonl.gz'), []);
  assert.deepEqual(log.readSince('t', 0), []); // live segment is empty again
  assert.equal(log.stats().segments['t'].live, 0);

  await waitForArchived(dir, 't');
  // The archive landed: one .gz, index line, pending file gone.
  const indexLines = readFileSync(join(dir, '__archive_index.jsonl'), 'utf8')
    .trim()
    .split('\n');
  assert.equal(indexLines.length, 1);
  const index = JSON.parse(indexLines[0]);
  assert.equal(index.topic, 't');
  assert.equal(index.segmentId, '1-5');
  assert.equal(index.seqStart, 1);
  assert.equal(index.seqEnd, 5);
  assert.equal(index.atStart, 1001);
  assert.equal(index.atEnd, 1005);
  assert.equal(index.entries, 5);
  assert.ok(index.path.endsWith('.jsonl.gz'));

  const stats = log.stats();
  assert.equal(stats.archived, 5);
  assert.deepEqual(stats.segments['t'], { live: 0, segments: 1, archived: 5 });

  // New appends start a fresh live segment; archived history is invisible
  // by default and readable opt-in.
  log.append({ seq: 6, topic: 't', at: 2000, payload: 6 });
  assert.deepEqual(
    log.readSince('t', 0).map((r) => r.seq),
    [6],
  );
  assert.deepEqual(
    log.readSince('t', 0, { includeArchived: true }).map((r) => r.seq),
    [1, 2, 3, 4, 5, 6],
  );
  assert.deepEqual(
    log.readSince('t', 3, { includeArchived: true }).map((r) => r.seq),
    [4, 5, 6],
  );
});

test('archive: age-based rotation closes the segment when it gets too old', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, segmentRotation: { maxSegmentAgeMs: 100 } });
  log.append({ seq: 1, topic: 't', at: 1000, payload: 'a' });
  log.append({ seq: 2, topic: 't', at: 1050, payload: 'b' }); // age 50 < 100
  assert.deepEqual(segmentFiles(dir, '.jsonl.tmp'), []);
  log.append({ seq: 3, topic: 't', at: 1100, payload: 'c' }); // age 100 >= 100
  assert.equal(segmentFiles(dir, '.jsonl.tmp').length, 1);
  await waitForArchived(dir, 't');
  const stats = log.stats();
  assert.equal(stats.archived, 3);
  assert.deepEqual(
    log.readSince('t', 0, { includeArchived: true }).map((r) => r.payload),
    ['a', 'b', 'c'],
  );
});

test('archive: without segmentRotation nothing is ever archived', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  for (let i = 1; i <= 10; i += 1) {
    log.append({ seq: i, topic: 't', at: 1000 + i, payload: i });
  }
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.deepEqual(segmentFiles(dir, '.jsonl.tmp'), []);
  assert.deepEqual(segmentFiles(dir, '.jsonl.gz'), []);
  assert.equal(readdirSync(dir).includes('__archive_index.jsonl'), false);
  const stats = log.stats();
  assert.equal(stats.archived, 0);
  assert.equal(stats.corruptArchives, 0);
  assert.deepEqual(stats.segments['t'], { live: 10, segments: 0, archived: 0 });
});

test('archive: corrupt gzip is skipped and counted, never fatal', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 2 } });
  log.append({ seq: 1, topic: 't', at: 1001, payload: 'a' });
  log.append({ seq: 2, topic: 't', at: 1002, payload: 'b' });
  const gzName = await waitForArchived(dir, 't');
  writeFileSync(join(dir, gzName), 'this is not gzip data');
  assert.deepEqual(log.readSince('t', 0, { includeArchived: true }), []);
  assert.equal(log.stats().corruptArchives, 1);
  // A second read counts again — the skip is per read, the archive stays.
  log.readSince('t', 0, { includeArchived: true });
  assert.equal(log.stats().corruptArchives, 2);
});

test('archive: corrupt index lines are skipped on recovery, valid entries load', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 2 } });
  log.append({ seq: 1, topic: 't', at: 1001, payload: 'a' });
  log.append({ seq: 2, topic: 't', at: 1002, payload: 'b' });
  await waitForArchived(dir, 't');
  writeFileSync(join(dir, '__archive_index.jsonl'), 'not json\n{"v":999}\n', { flag: 'a' });
  const reopened = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 2 } });
  assert.equal(reopened.stats().corruptLines, 2);
  assert.equal(reopened.stats().archived, 2);
  assert.deepEqual(
    reopened.readSince('t', 0, { includeArchived: true }).map((r) => r.seq),
    [1, 2],
  );
});

test('archive: restart rebuilds the archive view — no seq reuse, keySeqs reseeded', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({
    dir,
    keyCompaction: true,
    segmentRotation: { maxEntriesPerSegment: 2 },
  });
  log.append({ seq: 1, topic: 't', at: 1001, payload: 'a', key: 'k', keySeq: 1 });
  log.append({ seq: 2, topic: 't', at: 1002, payload: 'b', key: 'k', keySeq: 2 });
  await waitForArchived(dir, 't');
  // A "restart": a fresh log over the same directory.
  const reopened = DurableTopicLog.open({
    dir,
    keyCompaction: true,
    segmentRotation: { maxEntriesPerSegment: 2 },
  });
  assert.equal(reopened.lastSeq('t'), 2); // archived seqs count — never reused
  assert.equal(reopened.messageCount('t'), 2);
  assert.equal(reopened.stats().archived, 2);
  assert.equal(reopened.recoveredKeySeqs().get('k'), 2); // no keySeq renumbering
  // Keyed compaction dedupes across the archive boundary: the archived
  // seq 1 is superseded by seq 2 for key 'k'.
  assert.deepEqual(
    reopened.readSince('t', 0, { includeArchived: true }).map((r) => r.seq),
    [2],
  );
  // Appends continue numbering past the archived history.
  reopened.append({ seq: 3, topic: 't', at: 1003, payload: 'c' });
  assert.equal(reopened.lastSeq('t'), 3);
});

test('archive: keyed compaction dedupes across the live/archive boundary', async () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({
    dir,
    keyCompaction: true,
    segmentRotation: { maxEntriesPerSegment: 2 },
  });
  log.append({ seq: 1, topic: 't', at: 1001, payload: 'old', key: 'k', keySeq: 1 });
  log.append({ seq: 2, topic: 't', at: 1002, payload: 'mid', key: 'k', keySeq: 2 });
  await waitForArchived(dir, 't');
  log.append({ seq: 3, topic: 't', at: 1003, payload: 'new', key: 'k', keySeq: 3 });
  // The archived value is superseded by the live one: never resurrected.
  assert.deepEqual(
    log.readSince('t', 0, { includeArchived: true }).map((r) => r.payload),
    ['new'],
  );
});

test('archive: invalid rotation options throw RangeError', () => {
  assert.throws(() => DurableTopicLog.open({ dir: freshDir(), segmentRotation: {} }), RangeError);
  assert.throws(
    () => DurableTopicLog.open({ dir: freshDir(), segmentRotation: { maxEntriesPerSegment: 0 } }),
    RangeError,
  );
  assert.throws(
    () => DurableTopicLog.open({ dir: freshDir(), segmentRotation: { maxEntriesPerSegment: 1.5 } }),
    RangeError,
  );
  assert.throws(
    () => DurableTopicLog.open({ dir: freshDir(), segmentRotation: { maxSegmentAgeMs: 0 } }),
    RangeError,
  );
  assert.throws(
    () => DurableTopicLog.open({ dir: freshDir(), segmentRotation: { maxSegmentAgeMs: -10 } }),
    RangeError,
  );
  assert.throws(
    () => DurableTopicLog.open({ dir: freshDir(), segmentRotation: { maxSegmentAgeMs: Number.NaN } }),
    RangeError,
  );
  assert.throws(
    () =>
      DurableTopicLog.open({
        dir: freshDir(),
        segmentRotation: { maxSegmentAgeMs: Number.POSITIVE_INFINITY },
      }),
    RangeError,
  );
  // Through the bus: validated at construction, before anything runs.
  assert.throws(
    () => new EventBus({ durableLogDir: freshDir(), durableLogSegmentRotation: {} }),
    RangeError,
  );
});

test('bus: default replay reads only the live segment, never the archives', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 3 },
  });
  for (let i = 0; i < 4; i += 1) bus.publish('jobs.email', i);
  await waitForArchived(dir, 'jobs.email');
  // resumeFromSeq=0 replays only what is live (seq 4); the archived
  // seqs 1..3 are history, not resumed.
  const resumed: BusMessage[] = [];
  bus.subscribe('jobs.*', (m) => resumed.push(m), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(resumed.map((m) => m.seq), [4]);
  const stats = bus.getStats().durableLog;
  assert.equal(stats?.archived, 3);
  assert.deepEqual(stats?.segments['jobs.email'], { live: 1, segments: 1, archived: 3 });
});

test('bus: restart continues seqs past archived segments', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const rotation = { maxEntriesPerSegment: 2 };
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir, durableLogSegmentRotation: rotation });
  bus1.publish('jobs.email', 'a'); // seq 1
  bus1.publish('jobs.email', 'b'); // seq 2 -> rotates
  bus1.publish('jobs.email', 'c'); // seq 3
  await waitForArchived(dir, 'jobs.email');

  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir, durableLogSegmentRotation: rotation });
  const received: BusMessage[] = [];
  bus2.subscribe('jobs.*', (m) => received.push(m));
  bus2.publish('jobs.email', 'd');
  await flush();
  // No seq reuse: the restarted bus numbers past the archived history.
  assert.deepEqual(received.map((m) => m.seq), [4]);
  assert.equal(bus2.getStats().durableLog?.archived, 2);
});

test('bus: archiving never blocks the publish path', async () => {
  const dir = freshDir();
  const bus = new EventBus({
    durableLogDir: dir,
    durableLogSegmentRotation: { maxEntriesPerSegment: 500 },
  });
  const big = 'x'.repeat(500);
  for (let i = 0; i < 499; i += 1) bus.publish('bulk.data', { n: i, big });
  // The 500th publish closes the segment synchronously...
  const started = Date.now();
  bus.publish('bulk.data', { n: 499, big });
  const elapsed = Date.now() - started;
  // ...and returns before the gzip/index work runs: only the pending
  // file exists at this point, no archive yet.
  const names = readdirSync(dir);
  assert.ok(names.some((n) => n.endsWith('.jsonl.tmp')));
  assert.ok(!names.some((n) => n.endsWith('.jsonl.gz')));
  assert.ok(elapsed < 1000, `publish blocked on archiving: ${elapsed}ms`);
  await waitForArchived(dir, 'bulk.data');
  assert.equal(bus.getStats().durableLog?.archived, 500);
});
