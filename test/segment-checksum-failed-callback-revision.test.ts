/**
 * Segment-checksum-failure hook live revision (EB-436).
 *
 * `setSegmentChecksumFailedCallback` swaps the `onSegmentChecksumFailed`
 * observer every isolated segment is handed to without restarting the bus.
 * There is no enable gate — any open bus can install one — and `null`
 * clears back to audit-only. The new hook governs from the next checksum
 * failure onward, the `corruptSegments` counter moves regardless of
 * whether a hook is installed, and a throwing hook is swallowed by
 * `reportSegmentChecksumFailed`, so the read/recovery path is undisturbed.
 * The callback function itself is never written to the audit log: only
 * whether one is installed is recorded (`{ hasCallback, oldHasCallback }`).
 * Re-installing the live callback is a no-op with no audit. The root log
 * and every namespace child log share the one live cell, so a revision
 * governs the next failure on every log. The checksum verification, the
 * segment isolation/skip semantics, and the `corruptSegments` count are
 * untouched.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import type { SegmentChecksumFailure } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-segchk-cb-'));

/** Polls until `cond()` is true or the timeout elapses. */
async function waitFor(cond: () => boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (cond()) return;
    if (Date.now() > deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/**
 * Publishes 5 messages on `topic` (via the namespace handle when one is
 * given) and waits for the segment to archive; returns the .gz name.
 */
async function archiveOneSegment(
  dir: string,
  bus: EventBus,
  topic = 't',
  namespace?: string,
): Promise<string> {
  const handle = namespace === undefined ? undefined : bus.createNamespace(namespace);
  for (let i = 1; i <= 5; i += 1) {
    if (handle !== undefined) handle.publish(topic, i);
    else bus.publish(topic, i);
  }
  const logDir =
    namespace === undefined ? dir : join(dir, 'namespaces', encodeURIComponent(namespace));
  const concrete = namespace === undefined ? topic : `${namespace}/${topic}`;
  const prefix = `${encodeURIComponent(concrete)}.seg-`;
  await waitFor(
    () =>
      readdirSync(logDir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.tmp')).length ===
        0 &&
      readdirSync(logDir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz')).length > 0,
  );
  const names = readdirSync(logDir).filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl.gz'));
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

/**
 * Triggers an archived-segment read on `topic` — `rebuildPatchSnapshot`
 * with `{ includeArchived: true }` reads the sealed segments, so a
 * corrupt one is isolated and reported on the live bus (no restart).
 * The patch key carries no keyed messages; the rebuild returns the
 * (absent) tracker snapshot.
 */
function triggerArchivedRead(bus: EventBus, topic = 't', key = 'k', namespace?: string): void {
  bus.registerPatchKey(
    topic,
    key,
    undefined,
    namespace === undefined ? undefined : { namespace },
  );
  bus.rebuildPatchSnapshot(
    topic,
    key,
    namespace === undefined ? { includeArchived: true } : { namespace, includeArchived: true },
  );
}

/** The `segment_checksum_failed` audit events on the bus. */
function checksumFailedAudits(bus: EventBus) {
  return bus.getSubscriptionAudit().filter((e) => e.action === 'segment_checksum_failed');
}

const rotation = { maxEntriesPerSegment: 5 };

describe('segment checksum failure callback live revision (EB-436)', () => {
  it('the construction-time hook fires on recovery with exact fields', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, durableLogSegmentRotation: rotation });
    const gzName = await archiveOneSegment(dir, bus);
    corruptSegment(dir, gzName);
    // A restarted bus recovers with the segment isolated; the
    // construction-time hook observes it.
    const seen: SegmentChecksumFailure[] = [];
    const bus2 = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: rotation,
      onSegmentChecksumFailed: (info) => seen.push(info),
    });
    assert.equal(seen.length, 1);
    assert.deepEqual(seen[0], { topic: 't', path: gzName, segmentId: '1-5' });
    assert.equal(bus2.getStats().durableLog?.corruptSegments, 1);
    assert.equal(bus2.getStats().durableLog?.archived, 0);
    bus.destroy?.();
    bus2.destroy?.();
  });

  it('swapping the hook routes the next failure to the new hook only', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, durableLogSegmentRotation: rotation });
    const gzName = await archiveOneSegment(dir, bus);
    corruptSegment(dir, gzName);
    const firstSeen: SegmentChecksumFailure[] = [];
    const secondSeen: SegmentChecksumFailure[] = [];
    bus.setSegmentChecksumFailedCallback((info) => firstSeen.push(info));
    triggerArchivedRead(bus, 't', 'k1');
    assert.equal(firstSeen.length, 1);
    assert.deepEqual(firstSeen[0], { topic: 't', path: gzName, segmentId: '1-5' });
    assert.equal(secondSeen.length, 0);
    assert.equal(bus.getStats().durableLog?.corruptSegments, 1);
    // Each archived read that meets the corrupt segment isolates it again.
    bus.setSegmentChecksumFailedCallback((info) => secondSeen.push(info));
    triggerArchivedRead(bus, 't', 'k2');
    assert.equal(firstSeen.length, 1);
    assert.equal(secondSeen.length, 1);
    assert.deepEqual(secondSeen[0], { topic: 't', path: gzName, segmentId: '1-5' });
    assert.equal(bus.getStats().durableLog?.corruptSegments, 2);
    assert.equal(checksumFailedAudits(bus).length, 2);
    bus.destroy?.();
  });

  it('clearing with null stops delivery and the audit still records the failure', async () => {
    const dir = freshDir();
    const seen: SegmentChecksumFailure[] = [];
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: rotation,
      onSegmentChecksumFailed: (info) => seen.push(info),
    });
    const gzName = await archiveOneSegment(dir, bus);
    corruptSegment(dir, gzName);
    triggerArchivedRead(bus, 't', 'k1');
    assert.equal(seen.length, 1);
    bus.setSegmentChecksumFailedCallback(null);
    triggerArchivedRead(bus, 't', 'k2');
    assert.equal(seen.length, 1);
    assert.equal(bus.getStats().durableLog?.corruptSegments, 2);
    assert.equal(checksumFailedAudits(bus).length, 2);
    // Clearing a bus with no callback is a no-op with no audit.
    const before = bus.getSubscriptionAudit({
      action: 'segment_checksum_failed_callback_revised',
    }).length;
    bus.setSegmentChecksumFailedCallback(null);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'segment_checksum_failed_callback_revised' }).length,
      before,
    );
    bus.destroy?.();
  });

  it('non-functions and illegal actors are rejected and the live hook stays put', async () => {
    const dir = freshDir();
    const seen: SegmentChecksumFailure[] = [];
    const bus = new EventBus({
      durableLogDir: dir,
      durableLogSegmentRotation: rotation,
      onSegmentChecksumFailed: (info) => seen.push(info),
    });
    const gzName = await archiveOneSegment(dir, bus);
    corruptSegment(dir, gzName);
    const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
    for (const cb of bad) {
      assert.throws(
        () => bus.setSegmentChecksumFailedCallback(cb as never),
        RangeError,
        `expected RangeError for ${String(cb)}`,
      );
    }
    assert.throws(() => bus.setSegmentChecksumFailedCallback((info) => seen.push(info), ''), RangeError);
    assert.throws(
      () => bus.setSegmentChecksumFailedCallback((info) => seen.push(info), 42 as never),
      RangeError,
    );
    // The failed calls changed nothing: the live hook still observes.
    triggerArchivedRead(bus, 't', 'k1');
    assert.equal(seen.length, 1);
    assert.deepEqual(seen[0], { topic: 't', path: gzName, segmentId: '1-5' });
    assert.equal(
      bus.getSubscriptionAudit({ action: 'segment_checksum_failed_callback_revised' }).length,
      0,
    );
    bus.destroy?.();
  });

  it('re-installing the live callback is a no-op with no audit', () => {
    const handler = (_info: SegmentChecksumFailure): void => {};
    const bus = new EventBus({ onSegmentChecksumFailed: handler });
    bus.setSegmentChecksumFailedCallback(handler);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'segment_checksum_failed_callback_revised' }).length,
      0,
    );
    bus.destroy?.();
  });

  it('a closed bus refuses the swap', async () => {
    const bus = new EventBus();
    await bus.close();
    const handler = (_info: SegmentChecksumFailure): void => {};
    assert.throws(() => bus.setSegmentChecksumFailedCallback(handler), RangeError);
    assert.throws(() => bus.setSegmentChecksumFailedCallback(null), RangeError);
    assert.equal(
      bus.getSubscriptionAudit({ action: 'segment_checksum_failed_callback_revised' }).length,
      0,
    );
  });

  it('the swap audits exact presence flags with the actor', () => {
    const first = (_info: SegmentChecksumFailure): void => {};
    const second = (_info: SegmentChecksumFailure): void => {};
    const bus = new EventBus();
    bus.setSegmentChecksumFailedCallback(first, 'ops');
    bus.setSegmentChecksumFailedCallback(second, 'ops');
    bus.setSegmentChecksumFailedCallback(null, 'ops');
    const audit = bus.getSubscriptionAudit({ action: 'segment_checksum_failed_callback_revised' });
    assert.equal(audit.length, 3);
    assert.deepEqual(audit[0].detail, { hasCallback: true, oldHasCallback: false });
    assert.equal(audit[0].actor, 'ops');
    assert.deepEqual(audit[1].detail, { hasCallback: true, oldHasCallback: true });
    assert.deepEqual(audit[2].detail, { hasCallback: false, oldHasCallback: true });
    // The function itself is never recorded — only its presence.
    assert.ok(!JSON.stringify(audit).includes('first'));
    bus.destroy?.();
  });

  it('a throwing hook is swallowed and the read still isolates the segment', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, durableLogSegmentRotation: rotation });
    const gzName = await archiveOneSegment(dir, bus);
    corruptSegment(dir, gzName);
    bus.setSegmentChecksumFailedCallback(() => {
      throw new Error('broken observer');
    });
    // The archived read still isolates the segment instead of throwing;
    // the live segment still reads.
    bus.publish('t', 'live');
    triggerArchivedRead(bus, 't', 'k1');
    assert.equal(bus.getStats().durableLog?.corruptSegments, 1);
    assert.equal(checksumFailedAudits(bus).length, 1);
    assert.equal(checksumFailedAudits(bus)[0].detail?.path, gzName);
    bus.destroy?.();
  });

  it('a namespace child log failure reaches the revised hook', async () => {
    const dir = freshDir();
    const bus = new EventBus({ durableLogDir: dir, durableLogSegmentRotation: rotation });
    const nsDir = join(dir, 'namespaces', encodeURIComponent('ns'));
    const gzName = await archiveOneSegment(dir, bus, 't', 'ns');
    corruptSegment(nsDir, gzName);
    const seen: SegmentChecksumFailure[] = [];
    bus.setSegmentChecksumFailedCallback((info) => seen.push(info));
    triggerArchivedRead(bus, 't', 'k1', 'ns');
    assert.equal(seen.length, 1);
    assert.deepEqual(seen[0], { topic: 'ns/t', path: gzName, segmentId: '1-5' });
    // The child log counts the isolation in its own stats (the bus
    // `getStats().durableLog` surface covers the root log only); the
    // audit event is recorded on the bus either way.
    assert.equal(checksumFailedAudits(bus).length, 1);
    bus.destroy?.();
  });
});
