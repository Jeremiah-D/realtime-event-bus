/**
 * Bus-level durable-log live enable (EB-567).
 *
 * `setDurableLog(dir, opts?, actor?)` on a bus constructed without
 * `durableLogDir` installs the whole log instead of refusing: the root
 * log opens over `dir` with the construction-time defaults for omitted
 * bounds, the publish-side idempotent-dedup window reseeds from the
 * journal, and per-topic sequence counters and stats reseed from the
 * recovered history, so numbering continues instead of restarting at 1
 * and already-sealed segments replay from the first subscribe after the
 * install. The install opens the durable-log revision gates, audits
 * `durable_log_enabled` with every old value `null`, and is live-only:
 * a restart restores the unconfigured state. Rotation, offload, and the
 * encryptor are NOT installed here — each has its own live enable.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import type { DurableLogInstallOptions } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-log-enable-'));

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_enabled' });

/** Reopens a log directory and reads back one topic's live records. */
const readBack = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0);

/** The bus's live root log. */
const liveLog = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog;

test('install on a fresh dir persists publishes and opens the revision gates', () => {
  const dir = freshDir();
  const bus = new EventBus();
  assert.throws(() => bus.setDurableLogMaxAgeMs(1000), RangeError);
  bus.setDurableLog(dir, undefined, 'ops');
  bus.publish('hot', 0);
  bus.publish('hot', 1);
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    [0, 1],
  );
  // The revision gates open with the install.
  bus.setDurableLogMaxAgeMs(1000, 'ops');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    dir,
    maxEntriesPerTopic: 10000,
    oldMaxEntriesPerTopic: null,
    maxAgeMs: null,
    oldMaxAgeMs: null,
    maxBytesPerTopic: null,
    oldMaxBytesPerTopic: null,
    keyCompaction: false,
    oldKeyCompaction: null,
    publishDedupMaxEntries: 10000,
    oldPublishDedupMaxEntries: null,
  });
  bus.close();
});

test('install with opts applies the bounds', () => {
  const dir = freshDir();
  const bus = new EventBus();
  bus.setDurableLog(dir, {
    maxEntriesPerTopic: 500,
    maxAgeMs: 60000,
    maxBytesPerTopic: 4096,
    keyCompaction: true,
    publishDedupMaxEntries: 250,
  });
  const log = liveLog(bus);
  assert.equal(log.currentMaxEntriesPerTopic(), 500);
  assert.equal((log as unknown as { maxAgeMs: number | undefined }).maxAgeMs, 60000);
  assert.equal(log.currentMaxBytesPerTopic(), 4096);
  assert.equal(log.currentKeyCompaction(), true);
  assert.equal(log.currentPublishDedupMaxEntries(), 250);
  assert.deepEqual(audit(bus)[0].detail, {
    dir,
    maxEntriesPerTopic: 500,
    oldMaxEntriesPerTopic: null,
    maxAgeMs: 60000,
    oldMaxAgeMs: null,
    maxBytesPerTopic: 4096,
    oldMaxBytesPerTopic: null,
    keyCompaction: true,
    oldKeyCompaction: null,
    publishDedupMaxEntries: 250,
    oldPublishDedupMaxEntries: null,
  });
  bus.close();
});

test('illegal dir or opts refuse and the bus stays unconfigured', () => {
  const dir = freshDir();
  const bus = new EventBus();
  const stillUnconfigured = () => {
    assert.throws(() => bus.setDurableLogMaxAgeMs(1000), RangeError);
    assert.equal(audit(bus).length, 0);
  };
  assert.throws(() => bus.setDurableLog(''), RangeError);
  stillUnconfigured();
  assert.throws(() => bus.setDurableLog(123 as unknown as string), RangeError);
  stillUnconfigured();
  assert.throws(() => bus.setDurableLog(dir, { maxAgeMs: -1 }), RangeError);
  stillUnconfigured();
  assert.throws(() => bus.setDurableLog(dir, { maxEntriesPerTopic: 0 }), RangeError);
  stillUnconfigured();
  assert.throws(
    () => bus.setDurableLog(dir, [] as unknown as DurableLogInstallOptions),
    RangeError,
  );
  stillUnconfigured();
  // A later valid call still enables it.
  bus.setDurableLog(dir);
  assert.equal(audit(bus).length, 1);
  bus.close();
});

test('an already-enabled bus refuses a second install', () => {
  // Constructed with a log.
  const constructed = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => constructed.setDurableLog(freshDir()), RangeError);
  assert.equal(audit(constructed).length, 0);
  constructed.close();
  // Installed live.
  const bus = new EventBus();
  bus.setDurableLog(freshDir());
  assert.throws(() => bus.setDurableLog(freshDir()), RangeError);
  assert.equal(audit(bus).length, 1);
  bus.close();
});

test('null is a silent no-op on any bus', () => {
  const bus = new EventBus();
  bus.setDurableLog(null);
  assert.equal(audit(bus).length, 0);
  assert.throws(() => bus.setDurableLogMaxAgeMs(1000), RangeError);
  bus.setDurableLog(freshDir());
  // Null after the install is still a silent no-op, not a refusal.
  bus.setDurableLog(null);
  assert.equal(audit(bus).length, 1);
  bus.close();
});

test('install recovers sealed segments and numbering continues', async () => {
  const dir = freshDir();
  const first = new EventBus({ durableLogDir: dir });
  for (let i = 0; i < 3; i += 1) first.publish('hot', i);
  first.close();

  const bus = new EventBus();
  bus.setDurableLog(dir);
  const replayed: Array<{ seq: number; payload: unknown }> = [];
  bus.subscribe('hot', (msg) => replayed.push({ seq: msg.seq, payload: msg.payload }), {
    resumeFromSeq: 0,
  });
  await flush();
  assert.deepEqual(
    replayed.map((r) => r.payload),
    [0, 1, 2],
  );
  assert.deepEqual(
    replayed.map((r) => r.seq),
    [1, 2, 3],
  );
  // The next publish continues the recovered numbering, not 1.
  bus.publish('hot', 3);
  const records = readBack(dir, 'hot');
  assert.deepEqual(
    records.map((r) => r.payload),
    [0, 1, 2, 3],
  );
  assert.deepEqual(
    records.map((r) => r.seq),
    [1, 2, 3, 4],
  );
  bus.close();
});

test('the publish-dedup window reseeds from the journal', () => {
  const dir = freshDir();
  const first = new EventBus({ durableLogDir: dir });
  const claimed = first.publishIdempotent('orders', { id: 1 }, { messageId: 'm1' });
  assert.equal(claimed.duplicate, false);
  first.close();

  const bus = new EventBus();
  bus.setDurableLog(dir);
  const retry = bus.publishIdempotent('orders', { id: 1 }, { messageId: 'm1' });
  assert.equal(retry.duplicate, true);
  assert.equal(retry.accepted, 0);
  // The retry claimed no new journal line: the log still holds one record.
  assert.equal(readBack(dir, 'orders').length, 1);
  bus.close();
});

test('a namespace registered after the install gets its own child log', async () => {
  const dir = freshDir();
  const bus = new EventBus();
  bus.setDurableLog(dir);
  const t1 = bus.createNamespace('t1');
  t1.publish('orders', { id: 1 });
  await flush();
  const childDir = join(dir, 'namespaces', encodeURIComponent('t1'));
  assert.ok(existsSync(childDir));
  const child = DurableTopicLog.open({ dir: childDir });
  assert.deepEqual(child.topics().sort(), ['t1/orders']);
  // The root log never saw the namespaced topic.
  const root = DurableTopicLog.open({ dir });
  assert.deepEqual(root.topics().sort(), []);
  bus.close();
});

test('a closed bus refuses the install', () => {
  const bus = new EventBus();
  bus.close();
  assert.throws(() => bus.setDurableLog(freshDir()), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('a restart restores the unconfigured state', () => {
  const dir = freshDir();
  const first = new EventBus();
  first.setDurableLog(dir);
  first.publish('hot', 'a');
  first.close();
  // A new bus over the same directory starts unconfigured: the revision
  // gates refuse until the log is installed again.
  const bus = new EventBus();
  assert.throws(() => bus.setDurableLogMaxAgeMs(1000), RangeError);
  bus.setDurableLog(dir);
  bus.publish('hot', 'b');
  assert.deepEqual(
    readBack(dir, 'hot').map((r) => r.payload),
    ['a', 'b'],
  );
  bus.close();
});
