import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import type { BusMessage } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-contentdedup-'));
const journalPath = (dir: string) => join(dir, '__publish_dedup.jsonl');

test('invalid windowMs throws', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => bus.publishDedupedByContent('t', 'x', { windowMs: bad }),
      RangeError,
      `windowMs=${bad}`,
    );
  }
  assert.throws(
    () => bus.publishDedupedByContent('t', 'x', { windowMs: '100' as never }),
    RangeError,
  );
});

test('same content within the window publishes once; different content passes', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  const first = bus.publishDedupedByContent('t', { order: 1, amount: 100 });
  assert.deepEqual(first, { duplicate: false, accepted: 1 });
  const retry = bus.publishDedupedByContent('t', { order: 1, amount: 100 });
  assert.deepEqual(retry, { duplicate: true, accepted: 0 });
  const other = bus.publishDedupedByContent('t', { order: 1, amount: 101 });
  assert.deepEqual(other, { duplicate: false, accepted: 1 });
  await flush();
  assert.equal(received.length, 2);
  assert.deepEqual(received[0].payload, { order: 1, amount: 100 });
  assert.deepEqual(received[1].payload, { order: 1, amount: 101 });
  // Suppressed duplicates are observable, not silent.
  assert.equal(bus.getStats().duplicateMessages, 1);
});

test('the dedup identity is per-topic', async () => {
  const bus = new EventBus();
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  assert.deepEqual(bus.publishDedupedByContent('a', 'same'), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishDedupedByContent('b', 'same'), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishDedupedByContent('a', 'same'), {
    duplicate: true,
    accepted: 0,
  });
});

test('a duplicate consumes no sequence number: no phantom gap', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  bus.subscribe('t', (m) => seqs.push(m.seq));
  bus.publishDedupedByContent('t', 'one');
  bus.publishDedupedByContent('t', 'one'); // duplicate
  bus.publishDedupedByContent('t', 'one'); // duplicate
  bus.publishDedupedByContent('t', 'two');
  await flush();
  assert.deepEqual(seqs, [1, 2]);
});

test('a duplicate burns no publisher rate-limit budget', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.setPublisherRateLimit('pay', { messagesPerSec: 1000, burst: 2 });
  const pay = { publisherId: 'pay' } as const;
  assert.deepEqual(bus.publishDedupedByContent('t', 'a', pay), {
    duplicate: false,
    accepted: 1,
  });
  // Three duplicates: zero side effects, zero budget burned.
  for (let i = 0; i < 3; i += 1) {
    assert.deepEqual(bus.publishDedupedByContent('t', 'a', pay), {
      duplicate: true,
      accepted: 0,
    });
  }
  // One token left for genuinely new content; the next new publish sheds.
  assert.deepEqual(bus.publishDedupedByContent('t', 'b', pay), {
    duplicate: false,
    accepted: 1,
  });
  const shed = bus.publishDedupedByContent('t', 'c', pay);
  assert.equal(shed.duplicate, false);
  assert.equal(shed.accepted, 0);
  assert.equal(bus.getStats().publisherRateLimited, 1);
});

test('windowMs is injectable-clock deterministic and per-claim', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 60_000 });
  bus.subscribe('t', () => {});
  // Claim A with a 1s custom window.
  assert.deepEqual(bus.publishDedupedByContent('t', 'x', { windowMs: 1_000 }), {
    duplicate: false,
    accepted: 1,
  });
  clock.nowMs += 500;
  // A later call with a SHORTER window does not shorten the claim: the
  // claim defines the protection.
  assert.deepEqual(bus.publishDedupedByContent('t', 'x', { windowMs: 100 }), {
    duplicate: true,
    accepted: 0,
  });
  clock.nowMs += 501;
  // Past the claim's window: fresh publish again.
  assert.deepEqual(bus.publishDedupedByContent('t', 'x', { windowMs: 100 }), {
    duplicate: false,
    accepted: 1,
  });
});

test('the default window is the bus idempotencyWindowMs', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 2_000 });
  bus.subscribe('t', () => {});
  bus.publishDedupedByContent('t', 'x');
  clock.nowMs += 1_999;
  assert.deepEqual(bus.publishDedupedByContent('t', 'x'), {
    duplicate: true,
    accepted: 0,
  });
  clock.nowMs += 1;
  assert.deepEqual(bus.publishDedupedByContent('t', 'x'), {
    duplicate: false,
    accepted: 1,
  });
});

test('the table is bounded by idempotencyMaxEntries: oldest claim evicted', () => {
  const bus = new EventBus({ idempotencyMaxEntries: 2 });
  bus.subscribe('t', () => {});
  bus.publishDedupedByContent('t', 'a');
  bus.publishDedupedByContent('t', 'b');
  // Both still held within the bound.
  assert.deepEqual(bus.publishDedupedByContent('t', 'a'), {
    duplicate: true,
    accepted: 0,
  });
  // A third distinct claim evicts the oldest ('a').
  bus.publishDedupedByContent('t', 'c');
  assert.deepEqual(bus.publishDedupedByContent('t', 'a'), {
    duplicate: false,
    accepted: 1,
  });
  // 'c' (newest) is still held.
  assert.deepEqual(bus.publishDedupedByContent('t', 'c'), {
    duplicate: true,
    accepted: 0,
  });
});

test('composes with publishIdempotent: separate tables, no collision', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  // Same payload through both APIs: each admits once — the identities are
  // independent.
  assert.deepEqual(bus.publishIdempotent('t', 'pay', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishDedupedByContent('t', 'pay'), {
    duplicate: false,
    accepted: 1,
  });
  // Each suppresses only its own retries.
  assert.deepEqual(bus.publishIdempotent('t', 'pay', { messageId: 'm1' }), {
    duplicate: true,
    accepted: 0,
  });
  assert.deepEqual(bus.publishDedupedByContent('t', 'pay'), {
    duplicate: true,
    accepted: 0,
  });
  // A 64-hex messageId (the shape of a content hash) never collides with
  // a real content claim in the separate table.
  const hexLike = 'a'.repeat(64);
  assert.deepEqual(bus.publishIdempotent('t', 'other', { messageId: hexLike }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishDedupedByContent('t', 'other'), {
    duplicate: false,
    accepted: 1,
  });
});

test('a messageId in opts rides the envelope for subscriber-side dedup', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe(
    't',
    (m) => received.push(m),
    { deduplicateMessages: true },
  );
  const r = bus.publishDedupedByContent('t', 'x', { messageId: 'env-1' });
  assert.equal(r.duplicate, false);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].messageId, 'env-1');
});

test('restart within the window: the content claim survives (EB-66 journal)', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    idempotencyWindowMs: 60_000,
  });
  bus1.subscribe('t', () => {});
  assert.deepEqual(bus1.publishDedupedByContent('t', { cb: 1 }), {
    duplicate: false,
    accepted: 1,
  });
  assert.ok(existsSync(journalPath(dir)), 'the content claim must be journaled');
  await flush();
  // Restart before the window expires: the content retry must not
  // double-publish, and messageId claims still work too.
  const bus2 = new EventBus({
    now: clock.now,
    durableLogDir: dir,
    idempotencyWindowMs: 60_000,
  });
  bus2.subscribe('t', () => {});
  assert.deepEqual(bus2.publishDedupedByContent('t', { cb: 1 }), {
    duplicate: true,
    accepted: 0,
  });
  assert.deepEqual(bus2.publishDedupedByContent('t', { cb: 2 }), {
    duplicate: false,
    accepted: 1,
  });
});

test('binary payloads dedup by raw bytes', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const bytes = () => new Uint8Array([1, 2, 3]);
  assert.deepEqual(bus.publishDedupedByContent('t', bytes(), { binary: true }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishDedupedByContent('t', bytes(), { binary: true }), {
    duplicate: true,
    accepted: 0,
  });
  assert.deepEqual(
    bus.publishDedupedByContent('t', new Uint8Array([1, 2, 4]), { binary: true }),
    { duplicate: false, accepted: 1 },
  );
});

test('an ACL-denied publish claims no content slot', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setAclRules([{ pattern: 't', publish: 'deny' }]);
  const denied = bus.publishDedupedByContent('t', 'x');
  assert.deepEqual(denied, { duplicate: false, accepted: 0 });
  // After the ACL opens up, the same content is a fresh publish.
  bus.setAclRules([{ pattern: 't', publish: 'allow' }]);
  assert.deepEqual(bus.publishDedupedByContent('t', 'x'), {
    duplicate: false,
    accepted: 1,
  });
});
