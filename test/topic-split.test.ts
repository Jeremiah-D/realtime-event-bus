import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

const flush = () => new Promise((resolve) => setImmediate(resolve));

function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'eb-split-'));

function readHistory(dir: string, topic: string) {
  const log = DurableTopicLog.open({ dir });
  return log.readSince(topic, 0);
}

const isError = (r: { payload: unknown }) =>
  typeof r.payload === 'object' &&
  r.payload !== null &&
  (r.payload as { level?: unknown }).level === 'error';

test('splitTopic partitions history losslessly: matches to A, the rest to B, each with its own sequence space, source retired', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('events', { level: 'info', msg: 'i1' });
  bus.publish('events', { level: 'error', msg: 'e1' });
  bus.publish('events', { level: 'info', msg: 'i2' });
  bus.publish('events', { level: 'error', msg: 'e2' });
  bus.publish('events', { level: 'warn', msg: 'w1' });
  await flush();

  const result = bus.splitTopic('events', isError, 'events.err', 'events.ok');
  assert.equal(result.sourceTopic, 'events');
  assert.equal(result.targetA, 'events.err');
  assert.equal(result.targetB, 'events.ok');
  assert.equal(result.copiedA, 2);
  assert.equal(result.copiedB, 3);
  assert.equal(result.copied, 5);
  assert.equal(result.sourceLastSeq, 5);
  assert.equal(result.targetALastSeq, 2);
  assert.equal(result.targetBLastSeq, 3);
  assert.equal(result.lifecycleProvisioned, true);
  assert.equal(result.sourceTombstoned, true);

  // No loss, no overlap: the two targets jointly cover the source
  // exactly once, in source order within each target, re-seqed
  // from 1 in their own spaces.
  const errs = readHistory(dir, 'events.err');
  const oks = readHistory(dir, 'events.ok');
  assert.deepEqual(errs.map((r) => r.seq), [1, 2]);
  assert.deepEqual(errs.map((r) => r.payload), [
    { level: 'error', msg: 'e1' },
    { level: 'error', msg: 'e2' },
  ]);
  assert.deepEqual(oks.map((r) => r.seq), [1, 2, 3]);
  assert.deepEqual(oks.map((r) => r.payload), [
    { level: 'info', msg: 'i1' },
    { level: 'info', msg: 'i2' },
    { level: 'warn', msg: 'w1' },
  ]);
  assert.ok(errs.every((r) => r.topic === 'events.err'));
  assert.ok(oks.every((r) => r.topic === 'events.ok'));

  // Every split record carries its provenance on the record itself.
  assert.ok(errs.every((r) => r.splitSource === 'events'));
  assert.ok(oks.every((r) => r.splitSource === 'events'));

  // The source history is untouched and still replayable —
  // tombstoned means read-only, not destroyed — but new publishes
  // to it are refused, like a deleted topic.
  assert.deepEqual(readHistory(dir, 'events').map((r) => r.payload), [
    { level: 'info', msg: 'i1' },
    { level: 'error', msg: 'e1' },
    { level: 'info', msg: 'i2' },
    { level: 'error', msg: 'e2' },
    { level: 'warn', msg: 'w1' },
  ]);
  assert.ok(readHistory(dir, 'events').every((r) => r.splitSource === undefined));
  assert.equal(bus.publish('events', { level: 'info', msg: 'late' }), 0);
  assert.deepEqual(bus.previewPublish('events', { level: 'info', msg: 'late' }), {
    admitted: false,
    reason: 'alias-retired',
    failedGate: 'topic-lifecycle',
  });
  assert.equal(bus.getTopicLifecycle('events')?.provisioned, false);

  // Both targets evolve independently afterwards, each continuing
  // its own numbering — and the audit carries the split provenance.
  const errReceived: unknown[] = [];
  const okReceived: unknown[] = [];
  bus.subscribe('events.err', (m) => errReceived.push(m.payload));
  bus.subscribe('events.ok', (m) => okReceived.push(m.payload));
  bus.publish('events.err', { level: 'error', msg: 'e3' });
  bus.publish('events.ok', { level: 'info', msg: 'i3' });
  bus.publish('events.ok', { level: 'info', msg: 'i4' });
  await flush();
  assert.deepEqual(errReceived, [{ level: 'error', msg: 'e3' }]);
  assert.deepEqual(okReceived, [
    { level: 'info', msg: 'i3' },
    { level: 'info', msg: 'i4' },
  ]);
  assert.deepEqual(readHistory(dir, 'events.err').map((r) => r.seq), [1, 2, 3]);
  assert.deepEqual(readHistory(dir, 'events.ok').map((r) => r.seq), [1, 2, 3, 4, 5]);
  const stats = bus.getStats();
  assert.equal(stats.topics.find((t) => t.topic === 'events.err')?.lastSeq, 3);
  assert.equal(stats.topics.find((t) => t.topic === 'events.ok')?.lastSeq, 5);
  assert.equal(bus.getTopicLifecycle('events.err')?.provisioned, true);
  assert.equal(bus.getTopicLifecycle('events.ok')?.provisioned, true);
  const splitAudit = bus.getSubscriptionAudit({ action: 'topics_split' });
  assert.equal(splitAudit.length, 1);
  assert.equal(
    (splitAudit[0]?.detail as { splitSource?: unknown })?.splitSource,
    'events',
  );
  assert.equal((splitAudit[0]?.detail as { copied?: unknown })?.copied, 5);
});

test('splitTopic with a predicate matching everything leaves an empty but live second target', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();

  const result = bus.splitTopic('t', () => true, 't.all', 't.none');
  assert.equal(result.copiedA, 2);
  assert.equal(result.copiedB, 0);
  assert.equal(result.targetBLastSeq, 0);
  assert.deepEqual(readHistory(dir, 't.none'), []);
  // The empty target still provisions and accepts new publishes,
  // numbered from 1 in its own sequence space.
  assert.equal(bus.getTopicLifecycle('t.none')?.provisioned, true);
  bus.publish('t.none', 'first');
  await flush();
  assert.deepEqual(readHistory(dir, 't.none').map((r) => r.seq), [1]);
  assert.deepEqual(readHistory(dir, 't.none').map((r) => r.payload), ['first']);
});

test('splitTopic fails closed: unknown source, used target, duplicate names, bad predicate, or no durable log change nothing', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('a', 'a-1');
  bus.publish('a', 'a-2');
  bus.publish('taken', 'x');
  await flush();

  assert.throws(() => bus.splitTopic('missing', () => true, 'a1', 'a2'), RangeError);
  assert.throws(() => bus.splitTopic('a', () => true, 'taken', 'a2'), RangeError);
  assert.throws(() => bus.splitTopic('a', () => true, 'a1', 'taken'), RangeError);
  assert.throws(() => bus.splitTopic('a', () => true, 'a', 'a2'), RangeError);
  assert.throws(() => bus.splitTopic('a', () => true, 'a1', 'a'), RangeError);
  assert.throws(() => bus.splitTopic('a', () => true, 'same', 'same'), RangeError);
  assert.throws(() => bus.splitTopic('a', undefined as never, 'a1', 'a2'), RangeError);
  assert.throws(() => bus.splitTopic('a', 'yes' as never, 'a1', 'a2'), RangeError);
  assert.throws(() => bus.splitTopic('a.*', () => true, 'a1', 'a2'), RangeError);
  assert.throws(() => bus.splitTopic('ns/a', () => true, 'ns/b', 'ns/c'), RangeError);
  // A subscribed-but-never-published topic has no log: fail-closed.
  bus.subscribe('unpublished', () => {});
  assert.throws(() => bus.splitTopic('unpublished', () => true, 'u1', 'u2'), RangeError);

  // Nothing moved: the source is still live, the failed targets
  // never gained split history, and no split was audited.
  assert.equal(bus.getTopicLifecycle('a')?.provisioned, undefined);
  bus.publish('a', 'a-3');
  await flush();
  assert.deepEqual(readHistory(dir, 'a').map((r) => r.payload), ['a-1', 'a-2', 'a-3']);
  assert.deepEqual(readHistory(dir, 'a1'), []);
  assert.deepEqual(readHistory(dir, 'a2'), []);
  assert.equal(bus.getSubscriptionAudit({ action: 'topics_split' }).length, 0);

  // A bus without a durable log cannot split: there is no history.
  const memoryBus = new EventBus();
  memoryBus.publish('m', 'x');
  assert.throws(() => memoryBus.splitTopic('m', () => true, 'm1', 'm2'), RangeError);
});

test('splitTopic refuses a second split of an already-retired source', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('t', 'a');
  await flush();
  bus.splitTopic('t', () => true, 't.a', 't.b');
  assert.throws(() => bus.splitTopic('t', () => true, 't.c', 't.d'), RangeError);
  assert.deepEqual(readHistory(dir, 't.c'), []);
  assert.deepEqual(readHistory(dir, 't.d'), []);
});

test('splitTopic predicate may judge metadata, not just payload', async () => {
  const clock = controllableClock(1_000);
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publish('t', 'keyed', { key: 'k1' });
  bus.publish('t', 'plain');
  bus.publish('t', 'keyed-2', { key: 'k2' });
  await flush();

  const result = bus.splitTopic('t', (r) => r.key !== undefined, 't.keyed', 't.plain');
  assert.equal(result.copiedA, 2);
  assert.equal(result.copiedB, 1);
  assert.deepEqual(readHistory(dir, 't.keyed').map((r) => r.payload), ['keyed', 'keyed-2']);
  assert.deepEqual(readHistory(dir, 't.plain').map((r) => r.payload), ['plain']);
  // Keyed records keep their key (and keySeq) on the split target.
  assert.equal(readHistory(dir, 't.keyed')[0].key, 'k1');
});

test('DurableTopicLog.splitTopics drops signatures, stamps splitSource, and re-seqs per target', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  const sig = 'ab'.repeat(32);
  log.append({ seq: 1, topic: 's', at: 1000, payload: 'a', signature: { kid: 'k1', sig } });
  log.append({ seq: 2, topic: 's', at: 1001, payload: 'b' });
  log.append({ seq: 3, topic: 's', at: 1002, payload: 'c' });
  const result = log.splitTopics('s', (r) => r.payload === 'b', 's.b', 's.rest');
  assert.equal(result.copiedA, 1);
  assert.equal(result.copiedB, 2);
  assert.equal(result.copied, 3);
  assert.equal(result.sourceLastSeq, 3);
  const matched = log.readSince('s.b', 0);
  const rest = log.readSince('s.rest', 0);
  assert.deepEqual(matched.map((r) => r.seq), [1]);
  assert.deepEqual(matched.map((r) => r.payload), ['b']);
  assert.deepEqual(rest.map((r) => r.seq), [1, 2]);
  assert.deepEqual(rest.map((r) => r.payload), ['a', 'c']);
  assert.ok(matched.every((r) => r.splitSource === 's'));
  assert.ok(rest.every((r) => r.splitSource === 's'));
  // The re-seqed copy is unsigned: the source's signature does not
  // travel to a new (topic, seq) statement.
  assert.equal(rest[0].signature, undefined);
  // The source keeps its signature and its seqs.
  const source = log.readSince('s', 0);
  assert.deepEqual(source.map((r) => r.seq), [1, 2, 3]);
  assert.deepEqual(source[0].signature, { kid: 'k1', sig });
  // Log-level fail-closed: unknown source, existing target,
  // duplicate targets, non-function predicate.
  assert.throws(() => log.splitTopics('nope', () => true, 'x', 'y'), RangeError);
  assert.throws(() => log.splitTopics('s', () => true, 's.b', 'z'), RangeError);
  assert.throws(() => log.splitTopics('s', () => true, 's', 'z'), RangeError);
  assert.throws(() => log.splitTopics('s', () => true, 'x', 'x'), RangeError);
  assert.throws(() => log.splitTopics('s', undefined as never, 'x', 'y'), RangeError);
});

test('DurableTopicLog.splitTopics on a source with no live messages succeeds with zero copies', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  // A seq-0 schedule record makes the topic known to the log without
  // contributing a live message — the split has nothing to partition.
  log.append({ seq: 0, topic: 's', at: 1000, payload: null, delayId: 'delayed-1', deliverAt: 2000 });
  const result = log.splitTopics('s', () => true, 's.a', 's.b');
  assert.equal(result.copied, 0);
  assert.equal(result.copiedA, 0);
  assert.equal(result.copiedB, 0);
  assert.equal(result.targetALastSeq, 0);
  assert.equal(result.targetBLastSeq, 0);
});

test('DurableTopicLog.splitTopics with a throwing predicate leaves both targets untouched', () => {
  const dir = freshDir();
  const log = DurableTopicLog.open({ dir });
  log.append({ seq: 1, topic: 's', at: 1000, payload: 'a' });
  log.append({ seq: 2, topic: 's', at: 1001, payload: 'b' });
  assert.throws(
    () =>
      log.splitTopics(
        's',
        (r) => {
          if (r.payload === 'b') throw new Error('predicate blew up');
          return true;
        },
        's.a',
        's.b',
      ),
    /predicate blew up/,
  );
  assert.deepEqual(log.readSince('s.a', 0), []);
  assert.deepEqual(log.readSince('s.b', 0), []);
  assert.deepEqual(log.readSince('s', 0).map((r) => r.payload), ['a', 'b']);
});
