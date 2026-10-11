import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, appendFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-pubdedup-'));
const journalPath = (dir: string) => join(dir, '__publish_dedup.jsonl');

test('no durableLogDir: the window stays purely in-memory, no journal file', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  assert.deepEqual(bus.publishIdempotent('t', 'x', { messageId: 'm' }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishIdempotent('t', 'x', { messageId: 'm' }), {
    duplicate: true,
    accepted: 0,
  });
  // Nothing to inspect on disk — the bus has no directory at all.
});

test('restart within the window: the retry is still suppressed as a duplicate', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    idempotencyWindowMs: 60_000,
  });
  bus1.subscribe('t', () => {});
  assert.deepEqual(bus1.publishIdempotent('t', 'x', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  assert.ok(existsSync(journalPath(dir)), 'the claim must be journaled');
  await flush();
  // Restart before the window expires: the retry must not double-publish.
  const bus2 = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    idempotencyWindowMs: 60_000,
  });
  bus2.subscribe('t', () => {});
  assert.deepEqual(bus2.publishIdempotent('t', 'x', { messageId: 'm1' }), {
    duplicate: true,
    accepted: 0,
  });
  // A fresh identity is unaffected by the recovered window.
  assert.deepEqual(bus2.publishIdempotent('t', 'x', { messageId: 'm2' }), {
    duplicate: false,
    accepted: 1,
  });
});

test('expired claims are not rehydrated: the window restarts clean after the TTL', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const opts = { now: clock.now, durableLogDir: dir, idempotencyWindowMs: 1_000 };
  const bus1 = new EventBus(opts);
  bus1.subscribe('t', () => {});
  bus1.publishIdempotent('t', 'x', { messageId: 'm1' });
  await flush();
  // The claim ages out before the restart: the identity is "unknown" again.
  clock.nowMs += 2_000;
  const bus2 = new EventBus(opts);
  bus2.subscribe('t', () => {});
  assert.deepEqual(bus2.publishIdempotent('t', 'x', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
});

test('recovery is bounded: only the newest idempotencyMaxEntries claims survive', () => {
  const clock = controllableClock();
  const dir = freshDir();
  const opts = {
    now: clock.now,
    durableLogDir: dir,
    idempotencyWindowMs: 60_000,
    idempotencyMaxEntries: 2,
  };
  const bus1 = new EventBus(opts);
  bus1.subscribe('t', () => {});
  for (const id of ['a', 'b', 'c']) {
    assert.deepEqual(bus1.publishIdempotent('t', id, { messageId: id }).duplicate, false);
  }
  const bus2 = new EventBus(opts);
  bus2.subscribe('t', () => {});
  // 'b' and 'c' still suppress (duplicate checks mutate nothing); 'a' was
  // evicted as the oldest claim, so it re-admits. Note the order: probing
  // 'a' first would admit it and evict 'b' before 'b' is checked.
  assert.deepEqual(bus2.publishIdempotent('t', 'b', { messageId: 'b' }).duplicate, true);
  assert.deepEqual(bus2.publishIdempotent('t', 'c', { messageId: 'c' }).duplicate, true);
  assert.deepEqual(bus2.publishIdempotent('t', 'a', { messageId: 'a' }).duplicate, false);
});

test('dedup identity is per topic: the same messageId on another topic is independent', () => {
  const clock = controllableClock();
  const dir = freshDir();
  const opts = { now: clock.now, durableLogDir: dir, idempotencyWindowMs: 60_000 };
  const bus1 = new EventBus(opts);
  bus1.subscribe('t1', () => {});
  bus1.subscribe('t2', () => {});
  bus1.publishIdempotent('t1', 'x', { messageId: 'm' });
  const bus2 = new EventBus(opts);
  bus2.subscribe('t1', () => {});
  bus2.subscribe('t2', () => {});
  assert.deepEqual(bus2.publishIdempotent('t1', 'x', { messageId: 'm' }).duplicate, true);
  assert.deepEqual(bus2.publishIdempotent('t2', 'x', { messageId: 'm' }).duplicate, false);
});

test('a re-claimed identity restarts its window: the journal fold keeps the latest claim', () => {
  const clock = controllableClock();
  const dir = freshDir();
  const opts = { now: clock.now, durableLogDir: dir, idempotencyWindowMs: 10_000 };
  const bus1 = new EventBus(opts);
  bus1.subscribe('t', () => {});
  bus1.publishIdempotent('t', 'x', { messageId: 'm' });
  clock.nowMs += 11_000; // the first claim expires; the retry re-admits and restarts the window
  bus1.publishIdempotent('t', 'x', { messageId: 'm' });
  // Restart inside the RESTARTED window: still suppressed — the fold kept
  // the later claim, not the expired first one.
  clock.nowMs += 5_000;
  const bus2 = new EventBus(opts);
  bus2.subscribe('t', () => {});
  assert.deepEqual(bus2.publishIdempotent('t', 'x', { messageId: 'm' }).duplicate, true);
});

test('corrupt journal lines are skipped, never fatal', () => {
  const clock = controllableClock();
  const dir = freshDir();
  const opts = { now: clock.now, durableLogDir: dir, idempotencyWindowMs: 60_000 };
  const bus1 = new EventBus(opts);
  bus1.subscribe('t', () => {});
  bus1.publishIdempotent('t', 'x', { messageId: 'good' });
  // Corrupt lines are appended after the valid claim — the valid claim
  // must survive them.
  appendFileSync(
    journalPath(dir),
    [
      'not json at all',
      JSON.stringify({ v: 2, topic: 't', messageId: 'x', at: 1000 }),
      JSON.stringify({ v: 1, topic: '', messageId: 'x', at: 1000 }),
      JSON.stringify({ v: 1, topic: 't', messageId: 'x', at: 'whenever' }),
      '',
    ].join('\n'),
    'utf8',
  );
  const bus2 = new EventBus(opts);
  bus2.subscribe('t', () => {});
  assert.deepEqual(bus2.publishIdempotent('t', 'x', { messageId: 'good' }).duplicate, true);
  const log = DurableTopicLog.open({ dir });
  assert.ok(log.stats().corruptLines >= 4);
});

test('journal compacts itself: lines stay bounded under an idempotent-heavy producer', () => {
  const clock = controllableClock();
  const dir = freshDir();
  const opts = {
    now: clock.now,
    durableLogDir: dir,
    idempotencyWindowMs: 60_000,
    idempotencyMaxEntries: 10,
  };
  const bus = new EventBus(opts);
  bus.subscribe('t', () => {});
  for (let i = 0; i < 100; i++) {
    bus.publishIdempotent('t', i, { messageId: `m-${i}` });
  }
  const lines = readFileSync(journalPath(dir), 'utf8')
    .split('\n')
    .filter((l) => l.length > 0);
  // Without compaction this would be 100 lines; the 2x trigger collapses
  // to the newest `idempotencyMaxEntries` claims, so the file hovers at
  // most one trigger-window above the bound.
  assert.ok(lines.length <= 25, `journal has ${lines.length} lines`);
  // And the newest claims still suppress after the compaction rewrote the file.
  const bus2 = new EventBus(opts);
  bus2.subscribe('t', () => {});
  assert.deepEqual(bus2.publishIdempotent('t', 99, { messageId: 'm-99' }).duplicate, true);
});

test('getStats().durableLog exposes the publish-dedup journal line count', () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', 'x', { messageId: 'm1' });
  bus.publishIdempotent('t', 'x', { messageId: 'm2' });
  const stats = bus.getStats().durableLog;
  assert.ok(stats !== undefined);
  assert.equal(stats.publishDedupEntries, 2);
});
