/**
 * Bus-level durable-log segment-chain live verification (EB-569).
 *
 * `EventBus.verifyDurableLogSegmentChain()` checks the running bus's
 * archived-segment hash chain from the log's own in-memory chain view —
 * no stop, no manifest re-read, no race with the archive pass — so the
 * same five issue kinds as the offline `verifySegmentChain` are
 * observable while the bus keeps publishing. One deliberate deviation
 * from the backlog draft's test sketch: deleting a manifest line on
 * disk does NOT surface as a link issue while the bus runs — the
 * in-memory chain is the authority, and the next open heals the
 * missing line (EB-118). The link-issue test therefore tampers with
 * the chain's ordering (swapped lines), which healing cannot repair,
 * and a separate test asserts the deleted-line heal.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
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
import { EventBus } from '../src/bus.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-verify-chain-'));

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

const chainManifestPath = (dir: string): string => join(dir, '__segment_chain.jsonl');

const chainLines = (dir: string): string[] =>
  readFileSync(chainManifestPath(dir), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0);

/** Archives `segments` 5-record segments on topic 't' through the bus. */
async function archiveSegments(bus: EventBus, dir: string, segments: number): Promise<void> {
  let seq = 0;
  for (let s = 0; s < segments; s++) {
    for (let i = 0; i < 5; i++) {
      seq += 1;
      bus.publish('t', seq);
    }
  }
  await waitFor(
    () => segmentTmpFiles(dir, 't').length === 0 && segmentGzFiles(dir, 't').length === segments,
  );
  await waitFor(() => {
    try {
      return chainLines(dir).length === segments;
    } catch {
      return false;
    }
  });
}

/** Flips one byte of the archived segment so its bytes no longer match the chain digest. */
function corruptSegment(dir: string, gzName: string): void {
  const p = join(dir, gzName);
  const bytes = readFileSync(p);
  assert.ok(bytes.length > 10, 'segment must have content to corrupt');
  bytes[10] = bytes[10] ^ 0xff;
  writeFileSync(p, bytes);
}

describe('durable log segment chain live verification (EB-569)', () => {
  it('clean chain verifies ok and counts sealed segments', async () => {
    const dir = freshDir();
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    });
    await archiveSegments(bus, dir, 2);

    const report = bus.verifyDurableLogSegmentChain();
    assert.equal(report.ok, true);
    assert.equal(report.segments, 2);
    assert.deepEqual(report.issues, []);
    bus.close();
  });

  it('a rewritten segment is a live hash issue without restarting', async () => {
    const dir = freshDir();
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    });
    await archiveSegments(bus, dir, 1);
    const [gzName] = segmentGzFiles(dir, 't');
    corruptSegment(dir, gzName);

    // Same bus instance, no restart: the in-memory chain still names the
    // original digest, so the tamper surfaces immediately.
    const report = bus.verifyDurableLogSegmentChain();
    assert.equal(report.ok, false);
    assert.equal(report.segments, 1);
    assert.equal(report.issues.length, 1);
    const issue = report.issues[0];
    assert.equal(issue.kind, 'hash');
    assert.equal(issue.path, gzName);
    assert.equal(issue.topic, 't');
    assert.equal(issue.segmentId, '1-5');
    bus.close();
  });

  it('swapped manifest lines are a live link issue after reopen', async () => {
    const dir = freshDir();
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    });
    await archiveSegments(bus, dir, 2);
    bus.close();

    // Tamper with the persisted chain order: swap the two lines. Healing
    // cannot repair a reordered chain — it only re-links missing lines.
    const lines = chainLines(dir);
    writeFileSync(chainManifestPath(dir), `${lines[1]}\n${lines[0]}\n`, 'utf8');

    const reopened = new EventBus({ durableLogDir: dir });
    const report = reopened.verifyDurableLogSegmentChain();
    assert.equal(report.ok, false);
    assert.equal(report.segments, 2);
    const linkIssues = report.issues.filter((i) => i.kind === 'link');
    assert.ok(linkIssues.length >= 1, 'expected at least one link issue');
    const first = linkIssues.find((i) => i.segmentId === '6-10');
    assert.ok(first !== undefined, 'expected the break located at segment 6-10');
    assert.match(first.detail, /genesis/);
    reopened.close();
  });

  it('a deleted manifest line heals on reopen instead of breaking the live chain', async () => {
    const dir = freshDir();
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    });
    await archiveSegments(bus, dir, 2);
    // While the bus runs, the in-memory chain is the authority: deleting
    // a manifest line on disk changes nothing about the live report.
    const lines = chainLines(dir);
    writeFileSync(chainManifestPath(dir), `${lines[0]}\n`, 'utf8');
    const live = bus.verifyDurableLogSegmentChain();
    assert.equal(live.ok, true);
    assert.equal(live.segments, 2);
    bus.close();

    // On reopen the missing line is healed from the archived segment's
    // own bytes (EB-118), in landing order — the live chain is whole.
    const reopened = new EventBus({ durableLogDir: dir });
    assert.equal(chainLines(dir).length, 2);
    const report = reopened.verifyDurableLogSegmentChain();
    assert.equal(report.ok, true);
    assert.equal(report.segments, 2);
    assert.deepEqual(report.issues, []);
    reopened.close();
  });

  it('an unchained segment file on disk is a live rogue issue', async () => {
    const dir = freshDir();
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    });
    await archiveSegments(bus, dir, 1);
    // A segment file landed outside the chained archive path.
    writeFileSync(join(dir, 't.seg-100-104.jsonl.gz'), gzipSync('rogue'));

    const report = bus.verifyDurableLogSegmentChain();
    assert.equal(report.ok, false);
    const rogue = report.issues.filter((i) => i.kind === 'rogue');
    assert.equal(rogue.length, 1);
    assert.equal(rogue[0].path, 't.seg-100-104.jsonl.gz');
    assert.equal(rogue[0].segmentId, '100-104');
    bus.close();
  });

  it('a manifest-named segment file gone from disk is a live missing issue', async () => {
    const dir = freshDir();
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    });
    await archiveSegments(bus, dir, 1);
    const [gzName] = segmentGzFiles(dir, 't');
    unlinkSync(join(dir, gzName));

    const report = bus.verifyDurableLogSegmentChain();
    assert.equal(report.ok, false);
    assert.equal(report.issues.length, 1);
    const issue = report.issues[0];
    assert.equal(issue.kind, 'missing');
    assert.equal(issue.path, gzName);
    assert.equal(issue.topic, 't');
    assert.equal(issue.segmentId, '1-5');
    bus.close();
  });

  it('a bus without a durable log reports an empty chain', () => {
    const bus = new EventBus({});
    const report = bus.verifyDurableLogSegmentChain();
    assert.deepEqual(report, { ok: true, segments: 0, issues: [] });
    bus.close();
  });

  it('the live unsealed segment is never counted', async () => {
    const dir = freshDir();
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: { maxEntriesPerSegment: 5 },
    });
    for (let i = 1; i <= 3; i++) bus.publish('t', i);
    // Below the rotation budget: nothing sealed, nothing chained.
    assert.equal(segmentGzFiles(dir, 't').length, 0);

    const report = bus.verifyDurableLogSegmentChain();
    assert.deepEqual(report, { ok: true, segments: 0, issues: [] });
    bus.close();
  });

  it('namespace child log segments are not in the root report', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir });
    const team = bus.createNamespace('team');
    // Rotation fans out to the child log (EB-566).
    bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 5 }, 'ops');
    for (let i = 1; i <= 10; i++) bus.publish('t', i);
    for (let i = 1; i <= 10; i++) team.publish('orders', i);
    await waitFor(
      () => segmentTmpFiles(dir, 't').length === 0 && segmentGzFiles(dir, 't').length === 2,
    );
    const childDir = join(dir, 'namespaces', 'team');
    await waitFor(() => {
      try {
        // Child-log topics carry the namespace prefix: the segment files
        // are `team%2Forders.seg-<start>-<end>.jsonl.gz`.
        return (
          readdirSync(childDir).filter(
            (f) => f.includes('.seg-') && f.endsWith('.jsonl.gz'),
          ).length === 2
        );
      } catch {
        return false;
      }
    });

    // The child log really archived two segments — and the root report
    // still counts only the root log's two.
    const report = bus.verifyDurableLogSegmentChain();
    assert.equal(report.ok, true);
    assert.equal(report.segments, 2);
    assert.deepEqual(report.issues, []);
    bus.close();
  });
});
