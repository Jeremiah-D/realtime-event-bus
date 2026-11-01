/**
 * Bus-level durable-log publish-dedup journal bound revision (EB-328).
 *
 * `setDurableLogPublishDedupMaxEntries(maxEntries, actor?)` retunes how
 * many publish-side idempotent-dedup claims the `__publish_dedup.jsonl`
 * journal retains before it compacts down to the newest claims — without
 * restarting the bus. Only the bound moves: the amortized 2x compact
 * trigger keeps reading the bound live, claim identities and their
 * suppression semantics are unchanged, and the claims already persisted
 * are untouched — tightening takes effect from the next claim-triggered
 * compact, loosening simply allows more capacity. The revision fans out
 * to the root log and every namespace child log, and a namespace
 * registered later opens with the live bus bound. The bound starts at
 * the construction-time `idempotencyMaxEntries` and evolves
 * independently of it afterwards — `setIdempotency` never moves it. A
 * bus without `durableLogDir` refuses fail-closed; illegal values fail
 * closed with the live bound untouched; an identical revision is a
 * no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-durable-publish-dedup-'));

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'durable_log_publish_dedup_max_entries_revised' });

/** Reopens a log directory and reads back its publish-dedup journal. */
const readJournal = (dir: string) => {
  const log = DurableTopicLog.open({ dir });
  return {
    count: log.publishDedupEntryCount(),
    messageIds: log.recoveredPublishDedup().map((e) => e.messageId),
  };
};

/**
 * The bus's live publish-dedup journal bound — read off the root log
 * itself. (A reopened log reports the construction-time bound: the
 * revised bound is runtime-only, like every sibling revision.)
 */
const liveBound = (bus: EventBus) =>
  (bus as unknown as { durableLog: DurableTopicLog }).durableLog.currentPublishDedupMaxEntries();

/** The live publish-dedup journal bound of one namespace's child log. */
const liveChildBound = (bus: EventBus, prefix: string) =>
  (bus as unknown as { namespaceLogs: Map<string, DurableTopicLog> })
    .namespaceLogs.get(prefix)!
    .currentPublishDedupMaxEntries();

test('constructor still rejects illegal publishDedupMaxEntries at the log level', () => {
  for (const bad of [0, -1, 1.5, Number.NaN, '100', Infinity]) {
    assert.throws(
      () => DurableTopicLog.open({ dir: freshDir(), publishDedupMaxEntries: bad as never }),
      RangeError,
    );
  }
});

test('DurableTopicLog.setPublishDedupMaxEntries validates, returns the old bound, and leaves it untouched on illegal values', () => {
  const log = DurableTopicLog.open({ dir: freshDir(), publishDedupMaxEntries: 5 });
  assert.equal(log.currentPublishDedupMaxEntries(), 5);
  assert.equal(log.setPublishDedupMaxEntries(3), 5);
  assert.equal(log.currentPublishDedupMaxEntries(), 3);
  for (const bad of [0, -1, 1.5, Number.NaN, '3', Infinity]) {
    assert.throws(() => log.setPublishDedupMaxEntries(bad as never), RangeError);
  }
  // The failed calls changed nothing.
  assert.equal(log.currentPublishDedupMaxEntries(), 3);
});

test('the journal bound starts at the construction-time idempotencyMaxEntries', () => {
  const bus = new EventBus({ durableLogDir: freshDir(), idempotencyMaxEntries: 42 });
  assert.equal(liveBound(bus), 42);
  bus.close();

  const defaulted = new EventBus({ durableLogDir: freshDir() });
  // The default idempotency window is 10000: the journal bound follows.
  assert.equal(liveBound(defaulted), 10000);
  defaulted.close();
});

test('setDurableLogPublishDedupMaxEntries requires durableLogDir and a valid actor', () => {
  const noLog = new EventBus();
  assert.throws(() => noLog.setDurableLogPublishDedupMaxEntries(5), RangeError);
  assert.throws(
    () => noLog.setDurableLogPublishDedupMaxEntries(5, ''),
    /actor must be a non-empty string/,
  );
  noLog.close();

  const bus = new EventBus({ durableLogDir: freshDir() });
  assert.throws(() => bus.setDurableLogPublishDedupMaxEntries(5, '' as never), RangeError);
  assert.throws(() => bus.setDurableLogPublishDedupMaxEntries(5, 42 as never), RangeError);
  // The failed calls changed nothing: no audit, default bound intact.
  assert.equal(audit(bus).length, 0);
  assert.equal(liveBound(bus), 10000);
  bus.close();
});

test('revision rejects illegal values and leaves the live bound and audit untouched', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, idempotencyMaxEntries: 3 });
  for (const bad of [0, -1, 1.5, Number.NaN, '100', Infinity]) {
    assert.throws(() => bus.setDurableLogPublishDedupMaxEntries(bad as never), RangeError);
  }
  assert.equal(audit(bus).length, 0);
  assert.equal(liveBound(bus), 3);
  // A valid revision afterwards still works on the untouched bound.
  bus.setDurableLogPublishDedupMaxEntries(7, 'ops');
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    publishDedupMaxEntries: 7,
    oldPublishDedupMaxEntries: 3,
  });
  assert.equal(audit(bus)[0].actor, 'ops');
  assert.equal(liveBound(bus), 7);
  bus.close();
});

test('an identical revision is a no-op with no audit', () => {
  const bus = new EventBus({ durableLogDir: freshDir(), idempotencyMaxEntries: 5 });
  bus.setDurableLogPublishDedupMaxEntries(5);
  assert.equal(audit(bus).length, 0);

  const defaulted = new EventBus({ durableLogDir: freshDir() });
  // The default bound is 10000: re-affirming it is a no-op.
  defaulted.setDurableLogPublishDedupMaxEntries(10000);
  assert.equal(audit(defaulted).length, 0);
  bus.close();
  defaulted.close();
});

test('revision audits the old and new bound and moves the root log', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogPublishDedupMaxEntries(500);
  bus.setDurableLogPublishDedupMaxEntries(2000);
  const entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, {
    publishDedupMaxEntries: 500,
    oldPublishDedupMaxEntries: 10000,
  });
  assert.deepEqual(entries[1].detail, {
    publishDedupMaxEntries: 2000,
    oldPublishDedupMaxEntries: 500,
  });
  assert.equal(liveBound(bus), 2000);
  bus.close();
});

test('revision fans out to namespace child logs', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const ns = bus.createNamespace('t1');
  ns.publish('orders', 'one');
  bus.setDurableLogPublishDedupMaxEntries(7);
  assert.equal(liveBound(bus), 7);
  assert.equal(liveChildBound(bus, 't1'), 7);
  assert.deepEqual(audit(bus)[0].detail, {
    publishDedupMaxEntries: 7,
    oldPublishDedupMaxEntries: 10000,
  });
  bus.close();
});

test('a namespace registered after the revision opens with the live bound', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setDurableLogPublishDedupMaxEntries(4);
  bus.createNamespace('t2');
  assert.equal(liveChildBound(bus, 't2'), 4);
  bus.close();
});

test('setIdempotency never moves the journal bound, and the journal revision never moves the idempotency window', () => {
  const bus = new EventBus({ durableLogDir: freshDir(), idempotencyMaxEntries: 50 });
  bus.setIdempotency({ maxEntries: 7 });
  assert.equal(liveBound(bus), 50);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'idempotency_revised' }).length,
    1,
  );

  bus.setDurableLogPublishDedupMaxEntries(9);
  assert.equal(liveBound(bus), 9);
  // The in-memory window still follows setIdempotency alone: a second
  // idempotency revision audits its own values and leaves the journal
  // bound exactly where the journal revision put it.
  bus.setIdempotency({ maxEntries: 8 });
  assert.equal(liveBound(bus), 9);
  assert.deepEqual(
    bus.getSubscriptionAudit({ action: 'idempotency_revised' })[1].detail,
    { windowMs: 60000, oldWindowMs: 60000, maxEntries: 8, oldMaxEntries: 7 },
  );
  bus.close();
});

test('tightening does not rewrite the journal; the next claim past twice the new bound compacts to the newest claims', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, idempotencyMaxEntries: 100 });
  for (let i = 0; i < 10; i += 1) {
    bus.publishIdempotent('pay', i, { messageId: `m${i}` });
  }
  assert.deepEqual(readJournal(dir), {
    count: 10,
    messageIds: ['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8', 'm9'],
  });
  bus.setDurableLogPublishDedupMaxEntries(3);
  // No immediate rewrite: the ten persisted claims are still there.
  assert.deepEqual(readJournal(dir), {
    count: 10,
    messageIds: ['m0', 'm1', 'm2', 'm3', 'm4', 'm5', 'm6', 'm7', 'm8', 'm9'],
  });
  // The next claim trips the 2x trigger (11 > 2*3) and compacts the
  // journal down to the newest 3 claims.
  bus.publishIdempotent('pay', 10, { messageId: 'm10' });
  assert.deepEqual(readJournal(dir), {
    count: 3,
    messageIds: ['m8', 'm9', 'm10'],
  });
  bus.close();
});

test('loosening allows more capacity from the next claim', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, idempotencyMaxEntries: 3 });
  for (let i = 0; i < 10; i += 1) {
    bus.publishIdempotent('pay', i, { messageId: `m${i}` });
  }
  // Two compactions under the tight bound: the 7th claim trips 2x (7 >
  // 6) and compacts to the newest 3, then claims 8-10 accumulate
  // without another trigger.
  assert.deepEqual(readJournal(dir), {
    count: 6,
    messageIds: ['m4', 'm5', 'm6', 'm7', 'm8', 'm9'],
  });
  bus.setDurableLogPublishDedupMaxEntries(100);
  for (let i = 10; i < 15; i += 1) {
    bus.publishIdempotent('pay', i, { messageId: `m${i}` });
  }
  // No compact under the loosened bound: all eleven claims survive.
  assert.deepEqual(readJournal(dir), {
    count: 11,
    messageIds: ['m4', 'm5', 'm6', 'm7', 'm8', 'm9', 'm10', 'm11', 'm12', 'm13', 'm14'],
  });
  bus.close();
});

test('a tightened bound still suppresses within-window retries for the newest claims', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, idempotencyMaxEntries: 100 });
  for (let i = 0; i < 10; i += 1) {
    bus.publishIdempotent('pay', i, { messageId: `m${i}` });
  }
  bus.setDurableLogPublishDedupMaxEntries(3);
  bus.publishIdempotent('pay', 10, { messageId: 'm10' });
  // The journal compacted to the newest 3 — and the in-memory window
  // still suppresses a retry of the newest claim.
  assert.equal(bus.publishIdempotent('pay', 10, { messageId: 'm10' }).duplicate, true);
  assert.deepEqual(readJournal(dir).messageIds, ['m8', 'm9', 'm10']);
  bus.close();
});
