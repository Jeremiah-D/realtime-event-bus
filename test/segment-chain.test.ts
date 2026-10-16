import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import {
  mkdtempSync,
  readdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DurableTopicLog,
  SEGMENT_CHAIN_GENESIS_PREV,
  type SegmentChecksumFailure,
} from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-segchain-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

const segmentGzFiles = (dir: string, topic: string): string[] => {
  const prefix = `${encodeURIComponent(topic)}.seg-`;
  return readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz'));
};

const segmentTmpFiles = (dir: string, topic: string): string[] => {
  const prefix = `${encodeURIComponent(topic)}.seg-`;
  return readdirSync(dir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.tmp'));
};

/** Archives `count` 1-record-per-seq segments on topic 't' via count rotation. */
async function archiveSegments(
  dir: string,
  log: DurableTopicLog,
  segments: number,
  perSegment = 5,
): Promise<string[]> {
  let seq = 0;
  for (let s = 0; s < segments; s++) {
    for (let i = 0; i < perSegment; i++) {
      seq += 1;
      log.append({ seq, topic: 't', at: 1000 + seq, payload: seq });
    }
  }
  await waitFor(
    () => segmentTmpFiles(dir, 't').length === 0 && segmentGzFiles(dir, 't').length === segments,
  );
  await waitForChain(dir, segments);
  return segmentGzFiles(dir, 't').sort();
}

const chainManifestPath = (dir: string): string => join(dir, '__segment_chain.jsonl');

const chainLines = (dir: string): string[] =>
  readFileSync(chainManifestPath(dir), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0);

/** Waits until the chain manifest holds `n` lines (the archive pass is async). */
async function waitForChain(dir: string, n: number): Promise<void> {
  await waitFor(() => {
    try {
      return chainLines(dir).length === n;
    } catch {
      return false;
    }
  });
}

/** Flips one byte of the archived segment so its bytes no longer match the manifest digest. */
function corruptSegment(dir: string, gzName: string): void {
  const p = join(dir, gzName);
  const bytes = readFileSync(p);
  assert.ok(bytes.length > 10, 'segment must have content to corrupt');
  bytes[10] = bytes[10] ^ 0xff;
  writeFileSync(p, bytes);
}

describe('durable log segment hash chain (EB-118)', () => {
  it('builds a hash chain across archived segments and verify passes', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const gzNames = await archiveSegments(dir, log, 2);
    assert.deepEqual(gzNames, ['t.seg-1-5.jsonl.gz', 't.seg-6-10.jsonl.gz']);

    const lines = chainLines(dir).map((l) => JSON.parse(l));
    assert.equal(lines.length, 2);
    // Line envelope is versioned.
    assert.equal(lines[0].v, 2);
    // First link anchors at the deterministic genesis marker.
    assert.equal(lines[0].prevHash, SEGMENT_CHAIN_GENESIS_PREV);
    assert.equal(lines[0].segmentId, '1-5');
    // Second link chains to the first segment's digest.
    assert.equal(lines[1].prevHash, lines[0].sha256);
    assert.equal(lines[1].segmentId, '6-10');
    // Digests cover the gzipped archive bytes on disk; byte counts match.
    for (const [line, gzName] of [
      [lines[0], gzNames[0]],
      [lines[1], gzNames[1]],
    ] as const) {
      const bytes = readFileSync(join(dir, gzName));
      assert.equal(line.sha256, createHash('sha256').update(bytes).digest('hex'));
      assert.equal(line.bytes, bytes.length);
      assert.equal(line.entries, 5);
      assert.equal(line.path, gzName);
    }

    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, true);
    assert.equal(report.segments, 2);
    assert.deepEqual(report.issues, []);
  });

  it('detects reordered manifest lines and locates the break', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    await archiveSegments(dir, log, 2);
    // Tamper with the manifest order: swap the two lines.
    const lines = chainLines(dir);
    writeFileSync(chainManifestPath(dir), `${lines[1]}\n${lines[0]}\n`, 'utf8');

    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, false);
    assert.equal(report.segments, 2);
    const linkIssues = report.issues.filter((i) => i.kind === 'link');
    assert.ok(linkIssues.length >= 1, 'expected at least one link issue');
    // The break is located: the first line in file order (segment 6-10)
    // no longer anchors at genesis.
    const first = linkIssues.find((i) => i.segmentId === '6-10');
    assert.ok(first !== undefined, 'expected the break located at segment 6-10');
    assert.match(first.detail, /genesis/);
  });

  it('detects a missing segment and locates it', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const [gzName] = await archiveSegments(dir, log, 1);
    unlinkSync(join(dir, gzName));

    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, false);
    assert.equal(report.issues.length, 1);
    const issue = report.issues[0];
    assert.equal(issue.kind, 'missing');
    assert.equal(issue.path, gzName);
    assert.equal(issue.topic, 't');
    assert.equal(issue.segmentId, '1-5');
  });

  it('detects a replaced segment and locates it', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const [gzName] = await archiveSegments(dir, log, 1);
    // Same name, different bytes — a swapped segment.
    writeFileSync(join(dir, gzName), gzipSync('forged history'));

    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, false);
    assert.equal(report.issues.length, 1);
    const issue = report.issues[0];
    assert.equal(issue.kind, 'hash');
    assert.equal(issue.path, gzName);
    assert.equal(issue.segmentId, '1-5');
  });

  it('a damaged segment is skipped in-process and audited per EB-94; verify flags it', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    const [gzName] = await archiveSegments(dir, log, 1);
    log.append({ seq: 6, topic: 't', at: 1006, payload: 'live' });
    corruptSegment(dir, gzName);

    // In-process: the EB-94 convention — the damaged segment is isolated
    // (skipped, never fatal) and reported to the observer hook, which the
    // bus wires to the `segment_checksum_failed` subaudit event.
    const failures: SegmentChecksumFailure[] = [];
    const reopened = DurableTopicLog.open({
      dir,
      segmentRotation: { maxEntriesPerSegment: 5 },
      onSegmentChecksumFailed: (info) => failures.push(info),
    });
    assert.equal(reopened.stats().corruptSegments, 1);
    assert.equal(failures.length, 1);
    assert.equal(failures[0].path, gzName);
    assert.deepEqual(
      reopened.readSince('t', 0, { includeArchived: true }).map((r) => r.seq),
      [6],
    );

    // Offline: the chain still names the segment's original digest, so
    // verify reports the tamper instead of silently dropping the link.
    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, false);
    const hashIssues = report.issues.filter((i) => i.kind === 'hash');
    assert.equal(hashIssues.length, 1);
    assert.equal(hashIssues[0].path, gzName);
  });

  it('rotate and compact keep the chain unbroken', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    await archiveSegments(dir, log, 2);

    // Reopen with rotation effectively off and a tight count budget: the
    // live file compacts repeatedly while the archived chain sits
    // untouched.
    const log2 = DurableTopicLog.open({
      dir,
      segmentRotation: { maxEntriesPerSegment: 1000 },
      maxEntriesPerTopic: 2,
    });
    for (let seq = 11; seq <= 16; seq++) {
      log2.append({ seq, topic: 't', at: 1000 + seq, payload: seq });
    }
    // Count compaction ran (live holds only the newest records); no new
    // archives were produced.
    assert.deepEqual(
      log2.readSince('t', 0).map((r) => r.seq),
      [14, 15, 16],
    );
    assert.equal(chainLines(dir).length, 2);

    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, true);
    assert.equal(report.segments, 2);
  });

  it('an empty directory verifies as an empty chain (backward compatible)', () => {
    const dir = freshDir();
    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, true);
    assert.equal(report.segments, 0);
    assert.deepEqual(report.issues, []);
  });

  it('a log with no archives verifies as an empty chain', () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir });
    log.append({ seq: 1, topic: 't', at: 1000, payload: 'x' });
    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, true);
    assert.equal(report.segments, 0);
  });

  it('heals the chain for archives that predate it', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    await archiveSegments(dir, log, 2);
    // Simulate a directory archived before the chain existed.
    unlinkSync(chainManifestPath(dir));

    const reopened = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    assert.equal(reopened.stats().archived, 10);
    // The missing manifest was rebuilt in landing order on open.
    const lines = chainLines(dir).map((l) => JSON.parse(l));
    assert.equal(lines.length, 2);
    assert.equal(lines[0].prevHash, SEGMENT_CHAIN_GENESIS_PREV);
    assert.equal(lines[1].prevHash, lines[0].sha256);
    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, true);
    assert.equal(report.segments, 2);
  });

  it('reports a rogue segment file with no manifest line', async () => {
    const dir = freshDir();
    const log = DurableTopicLog.open({ dir, segmentRotation: { maxEntriesPerSegment: 5 } });
    await archiveSegments(dir, log, 1);
    // A segment file landed outside the chained archive path.
    writeFileSync(join(dir, 't.seg-100-104.jsonl.gz'), gzipSync('rogue'));

    const report = DurableTopicLog.verifySegmentChain(dir);
    assert.equal(report.ok, false);
    const rogue = report.issues.filter((i) => i.kind === 'rogue');
    assert.equal(rogue.length, 1);
    assert.equal(rogue[0].path, 't.seg-100-104.jsonl.gz');
    assert.equal(rogue[0].segmentId, '100-104');
  });
});
