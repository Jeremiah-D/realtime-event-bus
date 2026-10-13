import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  mkdtempSync,
  readdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog, type SegmentChecksumFailure } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-segchk-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Archives one 5-entry segment on topic 't' and returns its .gz name. */
async function archiveOneSegment(dir: string, log: DurableTopicLog): Promise<string> {
  for (let i = 1; i <= 5; i += 1) {
    log.append({ seq: i, topic: 't', at: 1000 + i, payload: i });
  }
  const prefix = `${encodeURIComponent('t')}.seg-`;
  await waitFor(
    () =>
      readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.tmp')).length === 0 &&
      readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz')).length > 0,
  );
  const names = readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz'));
  assert.equal(names.length, 1);
  return names[0];
}

/** Flips one byte of the archived segment so its checksum no longer matches. */
function corruptSegment(dir: string, gzName: string): void {
  const p = join(dir, gzName);
  const bytes = readFileSync(p);
  assert.ok(bytes.length > 10, 'segment must have content to corrupt');
  bytes[10] = bytes[10] ^ 0xff;
  writeFileSync(p, bytes);
}

describe('durable log segment checksums (EB-94)', () => {
  it('writes a SHA-256 sidecar when a segment archives', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const gzName = await archiveOneSegment(dir, log);
    const sidecar = readFileSync(join(dir, `${gzName}.sha256`), 'utf8').trim();
    const expected = createHash('sha256').update(readFileSync(join(dir, gzName))).digest('hex');
    assert.equal(sidecar, expected);
    assert.equal(log.stats().corruptSegments, 0);
  });

  it('a checksum mismatch at read time isolates the segment — never fatal', async () => {
    const dir = freshDir();
    const failures: SegmentChecksumFailure[] = [];
    const log = DurableTopicLog.open({
      dir,
      segmentRotation: { maxEntriesPerSegment: 5 },
      onSegmentChecksumFailed: (info) => failures.push(info),
    });
    const gzName = await archiveOneSegment(dir, log);
    // One more live message after the rotation, so the read has survivors.
    log.append({ seq: 6, topic: 't', at: 1006, payload: 'live' });
    corruptSegment(dir, gzName);
    // The archived read skips the corrupt segment instead of throwing;
    // the live segment still reads.
    const records = log.readSince('t', 0, { includeArchived: true });
    assert.deepEqual(
      records.map((r) => r.seq),
      [6],
    );
    assert.equal(log.stats().corruptSegments, 1);
    assert.equal(failures.length, 1);
    assert.equal(failures[0].topic, 't');
    assert.equal(failures[0].path, gzName);
    assert.equal(failures[0].segmentId, '1-5');
    // Each archived read that meets the corrupt segment isolates it again —
    // the counter is per isolation, like `corruptArchives`.
    log.readSince('t', 0, { includeArchived: true });
    assert.equal(log.stats().corruptSegments, 2);
  });

  it('a checksum mismatch at recovery keeps the segment out of the view', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const gzName = await archiveOneSegment(dir, log);
    log.append({ seq: 6, topic: 't', at: 1006, payload: 'live' });
    corruptSegment(dir, gzName);
    const failures: SegmentChecksumFailure[] = [];
    const reopened = DurableTopicLog.open({
      dir,
      segmentRotation: { maxEntriesPerSegment: 5 },
      onSegmentChecksumFailed: (info) => failures.push(info),
    });
    // The corrupt segment never entered the in-memory view: no archived
    // entries, only the live record reads back.
    const stats = reopened.stats();
    assert.equal(stats.corruptSegments, 1);
    assert.equal(stats.archived, 0);
    assert.deepEqual(
      reopened.readSince('t', 0, { includeArchived: true }).map((r) => r.seq),
      [6],
    );
    assert.equal(failures.length, 1);
    assert.equal(failures[0].segmentId, '1-5');
  });

  it('a segment without a sidecar (pre-checksum archive) still reads', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const gzName = await archiveOneSegment(dir, log);
    // Simulate an archive written before checksums existed.
    unlinkSync(join(dir, `${gzName}.sha256`));
    const reopened = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const stats = reopened.stats();
    assert.equal(stats.corruptSegments, 0);
    assert.equal(stats.archived, 5);
    assert.deepEqual(
      reopened.readSince('t', 0, { includeArchived: true }).map((r) => r.seq),
      [1, 2, 3, 4, 5],
    );
  });

  it('a corrupt orphan archive is adopted as corrupt, never healed into the view', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const gzName = await archiveOneSegment(dir, log);
    // Simulate a crash between the .gz write and the index append: drop
    // the index line, keep the .gz — then corrupt the bytes.
    writeFileSync(join(dir, '__archive_index.jsonl'), '', 'utf8');
    corruptSegment(dir, gzName);
    const failures: SegmentChecksumFailure[] = [];
    const reopened = DurableTopicLog.open({
      dir,
      segmentRotation: { maxEntriesPerSegment: 5 },
      onSegmentChecksumFailed: (info) => failures.push(info),
    });
    const stats = reopened.stats();
    assert.equal(stats.corruptSegments, 1);
    assert.equal(stats.archived, 0);
    assert.equal(failures.length, 1);
  });

  it('the bus surfaces corruptSegments in getStats().durableLog', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, durableLogSegmentRotation: { maxEntriesPerSegment: 5 } });
    for (let i = 1; i <= 5; i += 1) bus.publish('t', i);
    const prefix = `${encodeURIComponent('t')}.seg-`;
    await waitFor(
      () =>
        readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz')).length > 0 &&
        readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.tmp')).length === 0,
    );
    const gzName = readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz'))[0];
    corruptSegment(dir, gzName);
    // A restarted bus recovers with the segment isolated; the count is
    // visible on the bus stats surface.
    const bus2 = new EventBus({ durableLogDir: dir, durableLogSegmentRotation: { maxEntriesPerSegment: 5 } });
    assert.equal(bus2.getStats().durableLog?.corruptSegments, 1);
    assert.equal(bus2.getStats().durableLog?.archived, 0);
  });
});
