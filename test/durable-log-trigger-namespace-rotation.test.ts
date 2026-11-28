/**
 * Manual durable-log segment-rotation trigger for namespace child logs
 * (EB-575), the namespace twin of `triggerDurableLogSegmentRotation`
 * (EB-572).
 *
 * `triggerNamespaceDurableLogSegmentRotation(namespace, topic?, actor?)`
 * seals the live segment of one topic — or of every topic with live
 * records in that namespace when no topic is given — at once, so an
 * operator can close a namespaced topic's segments inside a maintenance
 * window instead of waiting for the next append to cross an entry or
 * age budget. It runs the child log's rotation directly: the same O(1)
 * rename seals the segment and the same background archive pass gzips
 * and indexes it, so the trigger itself never blocks publishing. Only
 * that namespace's child log rotates — the root log and every other
 * namespace's child log keep their live files. The trigger is audited
 * as `durable_log_namespace_segment_rotation_triggered`
 * (`{ namespace, topics }` — only the namespace and the rotated
 * topics, never segment names or seqs). An empty or unknown topic is
 * silently skipped (it seals nothing and writes no empty archive
 * segment), and a second trigger right after the first reports an
 * empty list. A bus with no durable log, an unknown namespace, a
 * namespace registered before the durable log was enabled (it has no
 * child log), or a child log with no rotation configured refuses
 * fail-closed — enable rotation live with
 * `setDurableLogSegmentRotation` (EB-566) first.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readdirSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-ns-rotation-trigger-'));

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
  existsSync(dir) ? readdirSync(dir).filter((f) => f.endsWith(ext)) : [];

/** One namespace's child-log directory under the bus's durable-log dir. */
const childDir = (dir: string, namespace: string) =>
  join(dir, 'namespaces', encodeURIComponent(namespace));

/** Waits until the topic's closed segment is fully archived (gz + index, tmp gone) in the child log. */
async function waitForChildArchived(dir: string, namespace: string, topic: string): Promise<void> {
  const cdir = childDir(dir, namespace);
  const prefix = `${encodeURIComponent(topic)}.seg-`;
  await waitFor(
    () =>
      segmentFiles(cdir, '.jsonl.tmp').filter((f) => f.startsWith(prefix)).length === 0 &&
      segmentFiles(cdir, '.jsonl.gz').filter((f) => f.startsWith(prefix)).length > 0,
  );
}

/** The child log's archive index entries for one topic, in index order. */
const childArchiveIndex = (dir: string, namespace: string, topic: string) => {
  const indexPath = join(childDir(dir, namespace), '__archive_index.jsonl');
  if (!existsSync(indexPath)) return [];
  return readFileSync(indexPath, 'utf8')
    .trim()
    .split('\n')
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line))
    .filter((entry) => entry.topic === topic);
};

/** A bus with segment rotation on its durable log (budget far above the test volumes). */
function rotationBus(dir?: string): EventBus {
  return new EventBus({
    durableLogDir: dir ?? freshDir(),
    durableLogSegmentRotation: { maxEntriesPerSegment: 100 },
  });
}

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_namespace_segment_rotation_triggered' });

/** The child log object of one namespace. */
const childLog = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!;

/** The bus's root durable log (private field, reached the way the other durable-log revision tests reach it). */
const logOf = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

test('trigger seals one namespace topic live segment; the child archive index records the real seq range, and later appends continue the numbering', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 0);
  ns.publish('orders', 1);
  ns.publish('orders', 2);
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders'), {
    rotated: ['t1/orders'],
  });
  await waitForChildArchived(dir, 't1', 't1/orders');
  const entries = childArchiveIndex(dir, 't1', 't1/orders');
  assert.equal(entries.length, 1);
  assert.equal(entries[0].topic, 't1/orders');
  assert.equal(entries[0].seqStart, 1);
  assert.equal(entries[0].seqEnd, 3);
  assert.equal(entries[0].entries, 3);
  // The live segment restarts empty: the next appends take seqs 4-5,
  // and the merged read returns the whole history in order.
  ns.publish('orders', 3);
  ns.publish('orders', 4);
  assert.deepEqual(
    childLog(bus, 't1')
      .readSince('t1/orders', 0, { includeArchived: true })
      .map((r) => r.seq),
    [1, 2, 3, 4, 5],
  );
  assert.deepEqual(
    childLog(bus, 't1')
      .readSince('t1/orders', 0)
      .map((r) => r.seq),
    [4, 5],
  );
});

test('a trigger with no topic seals every topic with live records in that namespace', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  const ns = bus.createNamespace('t1');
  ns.publish('a', 0);
  ns.publish('a', 1);
  ns.publish('b', 0);
  ns.publish('b', 1);
  ns.publish('b', 2);
  const result = bus.triggerNamespaceDurableLogSegmentRotation('t1');
  assert.deepEqual([...result.rotated].sort(), ['t1/a', 't1/b']);
  await waitForChildArchived(dir, 't1', 't1/a');
  await waitForChildArchived(dir, 't1', 't1/b');
  assert.equal(childArchiveIndex(dir, 't1', 't1/a')[0].seqEnd, 2);
  assert.equal(childArchiveIndex(dir, 't1', 't1/b')[0].seqEnd, 3);
});

test('an empty topic and an unknown topic are skipped silently and write no empty segment', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 0);
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1', 'empty'), {
    rotated: [],
  });
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1', 'nope'), {
    rotated: [],
  });
  // A relative name is an unknown topic to the child log, which keys
  // topics as `<namespace>/<topic>` — it skips silently too.
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1', 'orders'), {
    rotated: [],
  });
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1'), {
    rotated: ['t1/orders'],
  });
  await waitForChildArchived(dir, 't1', 't1/orders');
  // No pending or archived segment was ever minted for the skipped topics.
  const cdir = childDir(dir, 't1');
  assert.equal(
    segmentFiles(cdir, '.jsonl.tmp').filter(
      (f) => f.startsWith('empty.') || f.startsWith('nope.') || f.startsWith('orders.'),
    ).length,
    0,
  );
  assert.equal(
    segmentFiles(cdir, '.jsonl.gz').filter(
      (f) => f.startsWith('empty.') || f.startsWith('nope.') || f.startsWith('orders.'),
    ).length,
    0,
  );
  assert.equal(childArchiveIndex(dir, 't1', 'empty').length, 0);
  assert.equal(childArchiveIndex(dir, 't1', 'nope').length, 0);
});

test('bus without a durable log refuses; a bad actor refuses; an empty or non-string topic refuses; an invalid or unknown namespace refuses; no audit', () => {
  const bus = new EventBus({});
  assert.throws(
    () => bus.triggerNamespaceDurableLogSegmentRotation('t1'),
    /requires EventBusOptions.durableLogDir/,
  );
  const dir = freshDir();
  const logged = rotationBus(dir);
  logged.createNamespace('t1');
  assert.throws(
    () => logged.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders', ''),
    RangeError,
  );
  assert.throws(
    () => logged.triggerNamespaceDurableLogSegmentRotation('t1', '', 'ops'),
    /non-empty string/,
  );
  assert.throws(
    () => logged.triggerNamespaceDurableLogSegmentRotation('t1', 42 as never),
    /non-empty string/,
  );
  assert.throws(
    () => logged.triggerNamespaceDurableLogSegmentRotation('', 't1/orders'),
    /non-empty string/,
  );
  assert.throws(
    () => logged.triggerNamespaceDurableLogSegmentRotation('t1/x', 't1/orders'),
    /must not contain "\/"/,
  );
  assert.throws(
    () => logged.triggerNamespaceDurableLogSegmentRotation('nope', 'nope/orders'),
    /unknown namespace/,
  );
  assert.equal(audit(bus).length, 0);
  assert.equal(audit(logged).length, 0);
});

test('bus without rotation refuses, and enabling rotation live unlocks the namespace trigger', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 0);
  ns.publish('orders', 1);
  assert.throws(
    () => bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders'),
    /durableLogSegmentRotation/,
  );
  assert.equal(audit(bus).length, 0);
  // EB-566: the enable fans out to the child logs, so a namespace
  // registered before the enable gains rotation with it — and the
  // stored history seeds the first segment's identity, so the trigger
  // seals the real range instead of stamping seq 0.
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 100 }, 'ops');
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders'), {
    rotated: ['t1/orders'],
  });
  await waitForChildArchived(dir, 't1', 't1/orders');
  assert.equal(childArchiveIndex(dir, 't1', 't1/orders')[0].seqStart, 1);
  assert.equal(childArchiveIndex(dir, 't1', 't1/orders')[0].seqEnd, 2);
});

test('a namespace registered before the durable log was enabled has no child log and refuses, while its topics still seal via the root trigger', async () => {
  const dir = freshDir();
  const bus = new EventBus({});
  const ns = bus.createNamespace('t1');
  bus.setDurableLog(dir);
  bus.setDurableLogSegmentRotation({ maxEntriesPerSegment: 100 });
  ns.publish('orders', 0);
  ns.publish('orders', 1);
  assert.throws(
    () => bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders'),
    /has no child log/,
  );
  assert.equal(audit(bus).length, 0);
  // The namespace's topics fell back to the root log, which the root
  // trigger already covers — seal them there instead.
  assert.deepEqual(bus.triggerDurableLogSegmentRotation('t1/orders'), {
    rotated: ['t1/orders'],
  });
  await waitFor(
    () =>
      segmentFiles(dir, '.jsonl.gz').filter((f) =>
        f.startsWith(`${encodeURIComponent('t1/orders')}.seg-`),
      ).length > 0,
  );
  assert.deepEqual(
    logOf(bus)
      .readSince('t1/orders', 0, { includeArchived: true })
      .map((r) => r.seq),
    [1, 2],
  );
});

test('the root log and other namespace child logs are untouched', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  const ns1 = bus.createNamespace('t1');
  const ns2 = bus.createNamespace('t2');
  bus.publish('root', 0);
  ns1.publish('orders', 0);
  ns1.publish('orders', 1);
  ns2.publish('orders', 0);
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders'), {
    rotated: ['t1/orders'],
  });
  await waitForChildArchived(dir, 't1', 't1/orders');
  // The root topic and the other namespace's topic still hold their
  // live records — nothing was sealed outside t1.
  assert.deepEqual(
    logOf(bus)
      .readSince('root', 0)
      .map((r) => r.payload),
    [0],
  );
  assert.deepEqual(
    childLog(bus, 't2')
      .readSince('t2/orders', 0)
      .map((r) => r.payload),
    [0],
  );
  assert.equal(childArchiveIndex(dir, 't2', 't2/orders').length, 0);
  const rootIndexPath = join(dir, '__archive_index.jsonl');
  assert.equal(existsSync(rootIndexPath) ? readFileSync(rootIndexPath, 'utf8').trim().length : 0, 0);
});

test('the trigger is audited exactly: actor, namespace and rotated topics, never segment names or seqs', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 0);
  ns.publish('orders', 1);
  assert.deepEqual(
    bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders', 'ops-oncall'),
    { rotated: ['t1/orders'] },
  );
  assert.equal(audit(bus).length, 1);
  assert.equal(audit(bus)[0].actor, 'ops-oncall');
  assert.deepEqual(audit(bus)[0].detail, { namespace: 't1', topics: ['t1/orders'] });
  // A segment name or a seq range must never reach the audit trail.
  assert.equal(JSON.stringify(audit(bus)).includes('seg-1-2'), false);
  assert.equal(JSON.stringify(audit(bus)).includes('seqStart'), false);
  // A second trigger right after the first is audited with an empty list.
  assert.deepEqual(
    bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders', 'ops-oncall'),
    { rotated: [] },
  );
  assert.equal(audit(bus).length, 2);
  assert.deepEqual(audit(bus)[1].detail, { namespace: 't1', topics: [] });
});

test('an immediate second trigger reports an empty list and mints no second segment', async () => {
  const dir = freshDir();
  const bus = rotationBus(dir);
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 0);
  ns.publish('orders', 1);
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders'), {
    rotated: ['t1/orders'],
  });
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1', 't1/orders'), {
    rotated: [],
  });
  assert.deepEqual(bus.triggerNamespaceDurableLogSegmentRotation('t1'), { rotated: [] });
  await waitForChildArchived(dir, 't1', 't1/orders');
  assert.equal(childArchiveIndex(dir, 't1', 't1/orders').length, 1);
  assert.equal(
    segmentFiles(childDir(dir, 't1'), '.jsonl.gz').filter((f) =>
      f.startsWith(`${encodeURIComponent('t1/orders')}.seg-`),
    ).length,
    1,
  );
});
