/**
 * Bus-level durable-log keyed-compaction live switch (EB-364).
 *
 * `setDurableLogKeyCompaction(enabled, actor?)` flips the durable log
 * between Kafka-style latest-per-key retention and full-history
 * retention without restarting the bus. Only the flag moves: the read
 * paths keep re-deriving latest-per-key from the record set on every
 * read, so enabling flips the read view at once (superseded records
 * stop replaying even before the file is rewritten) and each log
 * rebuilds its key index and superseded counts from the records
 * already on disk, so the next append counts the dead weight that
 * predates the flip. Disabling is a view switch, not an un-delete:
 * the read view returns to full history at once, appends stop
 * maintaining the key index, and the superseded counts freeze until
 * the flag is flipped back on. The per-key TTL gate
 * (`setTopicKeyTtl`) follows the live flag. The revision fans out to
 * the root log and every namespace child log, and a namespace
 * registered later opens with the live bus flag. A bus without
 * `durableLogDir` refuses fail-closed; a bus constructed without
 * keyed compaction can still enable it. An identical revision is a
 * no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-key-comp-'));

const linesOf = (dir: string, topic: string) =>
  readFileSync(join(dir, `${encodeURIComponent(topic)}.log`), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0);

/** The bus's root durable log. */
const rootLog = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

/** One namespace's child log. */
const childLog = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!;

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_key_compaction_revised' });

test('DurableTopicLog.setKeyCompaction returns the old flag and currentKeyCompaction tracks the live flag', () => {
  const log = DurableTopicLog.open({ dir: freshDir() });
  assert.equal(log.currentKeyCompaction(), false);
  assert.equal(log.setKeyCompaction(true), false);
  assert.equal(log.currentKeyCompaction(), true);
  // Re-affirming the current flag returns the old value unchanged.
  assert.equal(log.setKeyCompaction(true), true);
  assert.equal(log.currentKeyCompaction(), true);
  assert.equal(log.setKeyCompaction(false), true);
  assert.equal(log.currentKeyCompaction(), false);
});

test('setDurableLogKeyCompaction requires durableLogDir, a boolean flag, and a valid actor', () => {
  const noLog = new EventBus();
  assert.throws(() => noLog.setDurableLogKeyCompaction(true), /durableLogDir/);
  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(
    () => bus.setDurableLogKeyCompaction('yes' as never),
    /must be a boolean/,
  );
  assert.throws(
    () => bus.setDurableLogKeyCompaction(true, ''),
    /actor must be a non-empty string/,
  );
  // The failed calls changed nothing.
  assert.equal(bus.getStats().durableLog?.keyCompaction, false);
  assert.equal(audit(bus).length, 0);
});

test('enabling flips the read view at once without rewriting the file', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('t', 'v1', { key: 'K' });
  bus.publish('t', 'v2', { key: 'K' });
  await flush();
  assert.deepEqual(
    rootLog(bus).readSince('t', 0).map((r) => r.payload),
    ['v1', 'v2'],
  );
  assert.equal(linesOf(dir, 't').length, 2);
  bus.setDurableLogKeyCompaction(true, 'ops');
  // The superseded value stops replaying immediately — the file still
  // holds both records until a compact rewrites it.
  assert.deepEqual(
    rootLog(bus).readSince('t', 0).map((r) => r.payload),
    ['v2'],
  );
  assert.equal(linesOf(dir, 't').length, 2);
});

test('enabling rebuilds the superseded count so the next append compacts on real dead weight', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, maxEntriesPerTopic: 2 });
  log.append({ seq: 1, topic: 't', at: 1, key: 'A', payload: 'a1' });
  log.append({ seq: 2, topic: 't', at: 2, key: 'A', payload: 'a2' });
  log.append({ seq: 3, topic: 't', at: 3, key: 'A', payload: 'a3' });
  // Compaction was off: nothing counted the dead weight, nothing compacted.
  assert.equal(log.entryCount('t'), 3);
  assert.equal(log.setKeyCompaction(true), false);
  // The flip rebuilt the index from disk: 2 superseded records already
  // on file. The next append for the same key reaches the trigger
  // (superseded >= maxEntriesPerTopic) and compacts at once.
  log.append({ seq: 4, topic: 't', at: 4, key: 'A', payload: 'a4' });
  assert.equal(log.entryCount('t'), 1);
  assert.deepEqual(
    log.readSince('t', 0).map((r) => [r.seq, r.payload]),
    [[4, 'a4']],
  );
  assert.equal(linesOf(dir, 't').length, 1);
});

test('disabling restores the full-history replay view — a view switch, not an un-delete', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('t', 'v1', { key: 'K' });
  bus.publish('t', 'v2', { key: 'K' });
  await flush();
  bus.setDurableLogKeyCompaction(true);
  assert.deepEqual(
    rootLog(bus).readSince('t', 0).map((r) => r.payload),
    ['v2'],
  );
  bus.setDurableLogKeyCompaction(false);
  // The superseded value reappears in replay: disabling only switched
  // the view — the file was never rewritten, nothing was un-deleted.
  assert.deepEqual(
    rootLog(bus).readSince('t', 0).map((r) => r.payload),
    ['v1', 'v2'],
  );
  assert.equal(linesOf(dir, 't').length, 2);
});

test('appends while disabled do not maintain the key index; re-enabling rebuilds it from disk', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir, keyCompaction: true, maxEntriesPerTopic: 2 });
  log.append({ seq: 1, topic: 't', at: 1, key: 'A', payload: 'a1' });
  log.append({ seq: 2, topic: 't', at: 2, key: 'A', payload: 'a2' });
  assert.equal(log.setKeyCompaction(false), true);
  // Disabled: the third value for the key is not indexed and not
  // counted as dead weight, so no compact fires.
  log.append({ seq: 3, topic: 't', at: 3, key: 'A', payload: 'a3' });
  assert.equal(log.entryCount('t'), 3);
  // Re-enabling rebuilds from disk: 2 superseded records on file, so
  // the next same-key append compacts at once.
  assert.equal(log.setKeyCompaction(true), false);
  log.append({ seq: 4, topic: 't', at: 4, key: 'A', payload: 'a4' });
  assert.equal(log.entryCount('t'), 1);
  assert.deepEqual(
    log.readSince('t', 0).map((r) => [r.seq, r.payload]),
    [[4, 'a4']],
  );
});

test('setTopicKeyTtl gate follows the live flag', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => bus.setTopicKeyTtl('t', 1000), /durableLogKeyCompaction/);
  bus.setDurableLogKeyCompaction(true);
  // Enabling re-opens the gate: the rule is accepted.
  bus.setTopicKeyTtl('t', 1000);
  bus.setDurableLogKeyCompaction(false);
  // Disabling closes it again.
  assert.throws(() => bus.setTopicKeyTtl('t', 1000), /durableLogKeyCompaction/);
});

test('re-affirming the current flag is a no-op with no audit', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.setDurableLogKeyCompaction(false);
  assert.equal(audit(bus).length, 0);
  bus.setDurableLogKeyCompaction(true, 'ops');
  assert.equal(audit(bus).length, 1);
  bus.setDurableLogKeyCompaction(true, 'ops');
  assert.equal(audit(bus).length, 1);
  bus.setDurableLogKeyCompaction(false, 'ops');
  assert.equal(audit(bus).length, 2);
  bus.setDurableLogKeyCompaction(false, 'ops');
  assert.equal(audit(bus).length, 2);
});

test('audit detail records enabled and oldEnabled exactly', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.setDurableLogKeyCompaction(true, 'ops');
  bus.setDurableLogKeyCompaction(false, 'ops');
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, { enabled: true, oldEnabled: false });
  assert.equal(entries[0].actor, 'ops');
  assert.deepEqual(entries[1].detail, { enabled: false, oldEnabled: true });
  assert.equal(entries[1].actor, 'ops');
});

test('revision fans out to namespace child logs; a later namespace opens with the live flag', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.createNamespace('early');
  const child = childLog(bus, 'early');
  child.append({ seq: 1, topic: 'nt', at: 1, key: 'K', payload: 'n1' });
  child.append({ seq: 2, topic: 'nt', at: 2, key: 'K', payload: 'n2' });
  assert.equal(child.currentKeyCompaction(), false);
  assert.deepEqual(
    child.readSince('nt', 0).map((r) => r.payload),
    ['n1', 'n2'],
  );
  bus.setDurableLogKeyCompaction(true);
  // The existing child log flipped with the root log.
  assert.equal(child.currentKeyCompaction(), true);
  assert.deepEqual(
    child.readSince('nt', 0).map((r) => r.payload),
    ['n2'],
  );
  // A namespace registered after the revision opens with the live flag.
  bus.createNamespace('late');
  assert.equal(childLog(bus, 'late').currentKeyCompaction(), true);
  bus.setDurableLogKeyCompaction(false);
  assert.equal(child.currentKeyCompaction(), false);
  assert.equal(childLog(bus, 'late').currentKeyCompaction(), false);
});

test('getStats().durableLog.keyCompaction reflects the live flag', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.equal(bus.getStats().durableLog?.keyCompaction, false);
  bus.setDurableLogKeyCompaction(true);
  assert.equal(bus.getStats().durableLog?.keyCompaction, true);
  bus.publish('t', 'v1', { key: 'K' });
  await flush();
  assert.equal(bus.getStats().durableLog?.keyCompaction, true);
  bus.setDurableLogKeyCompaction(false);
  assert.equal(bus.getStats().durableLog?.keyCompaction, false);
});
