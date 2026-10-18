import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  IdempotencyKeyConflictError,
  type BusMessage,
} from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-idemkey-'));
const journalPath = (dir: string) => join(dir, '__publish_dedup.jsonl');

test('invalid idempotencyKey throws RangeError at every publish entry point', () => {
  const bus = new EventBus();
  const badKeys = [
    '',
    'has space',
    'x'.repeat(129),
    'ünïcodé',
    'tab\there',
    42 as never,
    null as never,
  ];
  for (const key of badKeys) {
    assert.throws(() => bus.publish('t', 'x', { idempotencyKey: key }), RangeError, `publish key=${key}`);
    assert.throws(
      () => bus.publishIdempotent('t', 'x', { messageId: 'm', idempotencyKey: key }),
      RangeError,
      `publishIdempotent key=${key}`,
    );
    assert.throws(
      () => bus.publishDedupedByContent('t', 'x', { idempotencyKey: key }),
      RangeError,
      `publishDedupedByContent key=${key}`,
    );
    assert.throws(
      () => bus.publishBatch([{ topic: 't', payload: 'x', idempotencyKey: key }]),
      RangeError,
      `publishBatch key=${key}`,
    );
    assert.throws(
      () => bus.publishAtomic([{ topic: 't', payload: 'x', idempotencyKey: key }]),
      RangeError,
      `publishAtomic key=${key}`,
    );
    assert.throws(
      () => bus.previewPublish('t', 'x', { idempotencyKey: key }),
      RangeError,
      `previewPublish key=${key}`,
    );
  }
  // Boundary values are accepted: 1 char, 128 chars, visible ASCII.
  const okBus = new EventBus();
  okBus.subscribe('t', () => {});
  assert.equal(okBus.publish('t', 'a', { idempotencyKey: 'k' }), 1);
  assert.equal(okBus.publish('t', 'b', { idempotencyKey: 'x'.repeat(128) }), 1);
  assert.equal(okBus.publish('t', 'c', { idempotencyKey: 'pay:1/2-3_4.5~6!7' }), 1);
});

test('a retry with the same key returns the first publish result with zero side effects', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  // Two subscribers: the first result's accepted count is 2, and the
  // retry must replay exactly that.
  bus.subscribe('t', (m) => received.push(m));
  bus.subscribe('t', () => {});
  const first = bus.publish('t', { order: 1 }, { idempotencyKey: 'pay-1' });
  assert.equal(first, 2);
  const retry = bus.publish('t', { order: 1 }, { idempotencyKey: 'pay-1' });
  assert.equal(retry, 2);
  await flush();
  // One delivery per subscriber — the retry fanned out nothing.
  assert.equal(received.length, 1);
  assert.equal(received[0].seq, 1);
  // A fresh publish takes the very next sequence number: the retry
  // consumed none.
  bus.publish('t', { order: 2 });
  await flush();
  assert.deepEqual(received.map((m) => m.seq), [1, 2]);
  // Only the idempotent-hit counter moved — no publish, rejection, or
  // duplicate counters.
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 2);
  assert.equal(stats.idempotentHits, 1);
  assert.equal(stats.duplicateMessages, 0);
  assert.equal(stats.topics[0].idempotentHits, 1);
});

test('a duplicate burns no publisher or topic rate-limit budget', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.setPublisherRateLimit('pay', { messagesPerSec: 1000, burst: 2 });
  bus.setTopicRateLimit('t', 1000, { burst: 2 });
  const opts = { publisherId: 'pay', idempotencyKey: 'k1' } as const;
  assert.equal(bus.publish('t', 'a', opts), 1);
  // Three duplicates: zero side effects, zero budget burned.
  for (let i = 0; i < 3; i += 1) {
    assert.equal(bus.publish('t', 'a', opts), 1);
  }
  assert.equal(bus.getStats().idempotentHits, 3);
  // Both budgets still hold exactly one token: one fresh publish passes,
  // the next sheds.
  assert.equal(bus.publish('t', 'b', { publisherId: 'pay' }), 1);
  assert.equal(bus.publish('t', 'c', { publisherId: 'pay' }), 0);
  assert.equal(bus.getStats().publisherRateLimited, 1);
});

test('a duplicate is never written to the durable log', () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.publish('t', 'a', { idempotencyKey: 'k1' });
  bus.publish('t', 'a', { idempotencyKey: 'k1' });
  bus.publish('t', 'a', { idempotencyKey: 'k1' });
  const durable = bus.getStats().durableLog;
  assert.ok(durable != null);
  assert.equal(durable.entries, 1);
});

test('the key identity is per-topic', () => {
  const bus = new EventBus();
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  assert.equal(bus.publish('a', 'same', { idempotencyKey: 'k' }), 1);
  assert.equal(bus.publish('b', 'same', { idempotencyKey: 'k' }), 1);
  // Same key + same topic + same payload: suppressed.
  assert.equal(bus.publish('a', 'same', { idempotencyKey: 'k' }), 1);
  assert.equal(bus.getStats().idempotentHits, 1);
});

test('same key with a different payload throws IdempotencyKeyConflictError', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', { order: 1, amount: 100 }, { idempotencyKey: 'pay-9' });
  let err: IdempotencyKeyConflictError | undefined;
  try {
    bus.publish('t', { order: 1, amount: 999 }, { idempotencyKey: 'pay-9' });
  } catch (e) {
    assert.ok(e instanceof IdempotencyKeyConflictError);
    err = e;
  }
  assert.ok(err !== undefined, 'expected IdempotencyKeyConflictError');
  assert.ok(err.message.includes('pay-9'), 'message names the key');
  assert.ok(err.message.includes('"t"'), 'message names the topic');
  assert.equal(err.key, 'pay-9');
  assert.equal(err.topic, 't');
  assert.equal(err.code, 'IDEMPOTENCY_KEY_CONFLICT');
  // The conflict threw before anything was mutated: the original claim
  // still stands, and the conflicting payload was never delivered.
  const retry = bus.publish('t', { order: 1, amount: 100 }, { idempotencyKey: 'pay-9' });
  assert.equal(retry, 1);
  assert.equal(bus.getStats().idempotentHits, 1);
  assert.equal(bus.getStats().totalPublished, 1);
});

test('binary payloads compare by raw bytes for conflict detection', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const bytes = () => new Uint8Array([1, 2, 3]);
  bus.publish('t', bytes(), { binary: true, idempotencyKey: 'bin' });
  assert.equal(bus.publish('t', bytes(), { binary: true, idempotencyKey: 'bin' }), 1);
  assert.throws(
    () => bus.publish('t', new Uint8Array([1, 2, 4]), { binary: true, idempotencyKey: 'bin' }),
    IdempotencyKeyConflictError,
  );
});

test('an expired claim is a fresh publish that overwrites the old claim', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 1_000 });
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', 'v1', { idempotencyKey: 'k' }), 1);
  clock.nowMs += 999;
  // Still inside the window: a different payload conflicts.
  assert.throws(() => bus.publish('t', 'v2', { idempotencyKey: 'k' }), IdempotencyKeyConflictError);
  clock.nowMs += 1;
  // Past the window: the same key is a fresh publish — even with a
  // different payload — and the new claim replaces the old one.
  assert.equal(bus.publish('t', 'v2', { idempotencyKey: 'k' }), 1);
  assert.equal(bus.getStats().idempotentHits, 0);
  // The new claim is live: the old payload now conflicts, the new one hits.
  assert.throws(() => bus.publish('t', 'v1', { idempotencyKey: 'k' }), IdempotencyKeyConflictError);
  assert.equal(bus.publish('t', 'v2', { idempotencyKey: 'k' }), 1);
  assert.equal(bus.getStats().idempotentHits, 1);
});

test('the table is bounded by idempotencyMaxEntries: oldest key claim evicted', () => {
  const bus = new EventBus({ idempotencyMaxEntries: 2 });
  bus.subscribe('t', () => {});
  bus.publish('t', 'a', { idempotencyKey: 'k1' });
  bus.publish('t', 'b', { idempotencyKey: 'k2' });
  assert.equal(bus.publish('t', 'a', { idempotencyKey: 'k1' }), 1); // hit
  bus.publish('t', 'c', { idempotencyKey: 'k3' }); // evicts k1
  assert.equal(bus.publish('t', 'a', { idempotencyKey: 'k1' }), 1); // fresh, no hit
  assert.equal(bus.getStats().idempotentHits, 1);
});

test('an ACL-denied publish claims no key slot', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setAclRules([{ pattern: 't', publish: 'deny' }]);
  assert.equal(bus.publish('t', 'x', { idempotencyKey: 'k' }), 0);
  // After the ACL opens up, the same key is a fresh publish.
  bus.setAclRules([{ pattern: 't', publish: 'allow' }]);
  assert.equal(bus.publish('t', 'x', { idempotencyKey: 'k' }), 1);
  assert.equal(bus.getStats().idempotentHits, 0);
});

test('a rejected first attempt claims no slot: the retry stays fresh', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicSchema('t', (p) => p === 'ok');
  assert.equal(bus.publish('t', 'bad', { idempotencyKey: 'k' }), 0);
  assert.equal(bus.getStats().rejectedMessages, 1);
  // The rejected attempt claimed nothing: the retry admits.
  assert.equal(bus.publish('t', 'ok', { idempotencyKey: 'k' }), 1);
  assert.equal(bus.getStats().idempotentHits, 0);
  // Now the claim is live.
  assert.equal(bus.publish('t', 'ok', { idempotencyKey: 'k' }), 1);
  assert.equal(bus.getStats().idempotentHits, 1);
});

test('explicit key wins over the EB-146 content hash', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  // Keyed content publish: claims the key slot AND the content slot.
  const first = bus.publishDedupedByContent('t', { order: 7 }, { idempotencyKey: 'k7' });
  assert.deepEqual(first, { duplicate: false, accepted: 1 });
  // Same key, same payload via plain publish: key hit (not content).
  assert.equal(bus.publish('t', { order: 7 }, { idempotencyKey: 'k7' }), 1);
  assert.equal(bus.getStats().idempotentHits, 1);
  assert.equal(bus.getStats().duplicateMessages, 0);
  // Same key, different payload: key conflict — even though the content
  // differs from every content claim, the key gate runs first.
  assert.throws(
    () => bus.publishDedupedByContent('t', { order: 8 }, { idempotencyKey: 'k7' }),
    IdempotencyKeyConflictError,
  );
  // Same content, no key: the content slot (claimed by the first call)
  // still suppresses.
  assert.deepEqual(bus.publishDedupedByContent('t', { order: 7 }), {
    duplicate: true,
    accepted: 0,
  });
  assert.equal(bus.getStats().duplicateMessages, 1);
  await flush();
  assert.equal(received.length, 1);
});

test('key hit via publishDedupedByContent returns the duplicate shape', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  assert.deepEqual(bus.publishDedupedByContent('t', 'p', { idempotencyKey: 'kk' }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishDedupedByContent('t', 'p', { idempotencyKey: 'kk' }), {
    duplicate: true,
    accepted: 0,
  });
  assert.equal(bus.getStats().idempotentHits, 1);
});

test('restart within the window: the key claim survives with hash and first result', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir, idempotencyWindowMs: 60_000 });
  bus1.subscribe('t', () => {});
  bus1.subscribe('t', () => {});
  assert.equal(bus1.publish('t', { cb: 1 }, { idempotencyKey: 'rk' }), 2);
  assert.ok(existsSync(journalPath(dir)), 'the key claim must be journaled');
  await flush();
  // Restart before the window expires: the retry returns the first
  // result, and a different payload still conflicts.
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir, idempotencyWindowMs: 60_000 });
  bus2.subscribe('t', () => {});
  bus2.subscribe('t', () => {});
  assert.equal(bus2.publish('t', { cb: 1 }, { idempotencyKey: 'rk' }), 2);
  assert.throws(
    () => bus2.publish('t', { cb: 2 }, { idempotencyKey: 'rk' }),
    IdempotencyKeyConflictError,
  );
  assert.equal(bus2.getStats().idempotentHits, 1);
  // Past the window the key is fresh again.
  clock.nowMs += 60_000;
  const bus3 = new EventBus({ now: clock.now, durableLogDir: dir, idempotencyWindowMs: 60_000 });
  bus3.subscribe('t', () => {});
  assert.equal(bus3.publish('t', { cb: 3 }, { idempotencyKey: 'rk' }), 1);
  assert.equal(bus3.getStats().idempotentHits, 0);
});

test('publishBatch enforces the key per entry', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  const first = bus.publishBatch([
    { topic: 't', payload: 'a', idempotencyKey: 'bk' },
    { topic: 't', payload: 'b' },
  ]);
  assert.equal(first, 2);
  // The keyed entry is a hit on the second batch: contributes the first
  // result without fanning out.
  const second = bus.publishBatch([
    { topic: 't', payload: 'a', idempotencyKey: 'bk' },
    { topic: 't', payload: 'c' },
  ]);
  assert.equal(second, 2);
  assert.equal(bus.getStats().idempotentHits, 1);
  // A conflicting payload in a batch entry throws, like `publish`.
  assert.throws(
    () => bus.publishBatch([{ topic: 't', payload: 'zzz', idempotencyKey: 'bk' }]),
    IdempotencyKeyConflictError,
  );
  await flush();
  assert.deepEqual(received.map((m) => m.payload), ['a', 'b', 'c']);
});

test('publishAtomic: the same key twice in one batch is a whole-batch conflict', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  const result = bus.publishAtomic([
    { topic: 't', payload: 'a', idempotencyKey: 'ak' },
    { topic: 't', payload: 'a', idempotencyKey: 'ak' },
  ]);
  assert.deepEqual(result, {
    published: 0,
    rejected: { index: 1, topic: 't', reason: 'idempotency-conflict' },
  });
  // No partial claims: the first entry's key was never claimed, so a
  // later single publish with the key is fresh.
  assert.equal(bus.publish('t', 'a', { idempotencyKey: 'ak' }), 1);
  assert.equal(bus.getStats().idempotentHits, 0);
  await flush();
  assert.equal(received.length, 1);
});

test('publishAtomic: an entry colliding with a live claim aborts the batch', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  bus.publish('t', 'a', { idempotencyKey: 'ck' });
  // Same payload under the live key: still a batch conflict — atomicity
  // forbids suppressing one entry while publishing the rest.
  const same = bus.publishAtomic([
    { topic: 't', payload: 'b' },
    { topic: 't', payload: 'a', idempotencyKey: 'ck' },
  ]);
  assert.deepEqual(same, {
    published: 0,
    rejected: { index: 1, topic: 't', reason: 'idempotency-conflict' },
  });
  // Different payload under the live key: conflict as well.
  const different = bus.publishAtomic([
    { topic: 't', payload: 'a', idempotencyKey: 'ck' },
  ]);
  assert.deepEqual(different.rejected?.reason, 'idempotency-conflict');
  await flush();
  // Nothing from the rejected batches was delivered.
  assert.deepEqual(received.map((m) => m.payload), ['a']);
});

test('publishAtomic: admitted entries claim their keys at commit', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const result = bus.publishAtomic([
    { topic: 't', payload: 'x', idempotencyKey: 'mk' },
    { topic: 't', payload: 'y' },
  ]);
  assert.deepEqual(result, { published: 2 });
  // The committed entry's key is live: a later publish hits it.
  assert.equal(bus.publish('t', 'x', { idempotencyKey: 'mk' }), 1);
  assert.equal(bus.getStats().idempotentHits, 1);
  assert.throws(
    () => bus.publish('t', 'other', { idempotencyKey: 'mk' }),
    IdempotencyKeyConflictError,
  );
});

test('previewPublish validates the key but never consults or claims it', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 'a', { idempotencyKey: 'pk' });
  // A live claim does not change the preview verdict: idempotency is not
  // previewed.
  assert.deepEqual(bus.previewPublish('t', 'a', { idempotencyKey: 'pk' }), { admitted: true });
  // And the preview claimed nothing: the next real publish still hits
  // the ORIGINAL claim, not a preview claim.
  assert.equal(bus.publish('t', 'a', { idempotencyKey: 'pk' }), 1);
  assert.equal(bus.getStats().idempotentHits, 1);
  // Invalid keys still fail fast in previews.
  assert.throws(() => bus.previewPublish('t', 'a', { idempotencyKey: '' }), RangeError);
});

test('metrics: eventbus_idempotent_hits_total is activity-gated per topic', () => {
  const quiet = new EventBus();
  quiet.subscribe('t', () => {});
  quiet.publish('t', 'x');
  // No idempotency-key traffic: byte-identical exposition, zero series.
  const quietText = renderPrometheus(quiet.getStats());
  assert.ok(!quietText.includes('eventbus_idempotent_hits_total'));

  const bus = new EventBus();
  bus.subscribe('orders', () => {});
  bus.subscribe('shipments', () => {});
  bus.publish('orders', 'a', { idempotencyKey: 'm1' });
  bus.publish('orders', 'a', { idempotencyKey: 'm1' });
  bus.publish('orders', 'a', { idempotencyKey: 'm1' });
  bus.publish('shipments', 'b');
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('# HELP eventbus_idempotent_hits_total '));
  assert.ok(text.includes('# TYPE eventbus_idempotent_hits_total counter'));
  assert.ok(text.includes('eventbus_idempotent_hits_total{topic="orders"} 2'));
  assert.ok(text.includes('eventbus_idempotent_hits_total{topic="shipments"} 0'));
});

test('publishWithConfirmation reports a key hit as a duplicate rejection', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const first = await bus.publishWithConfirmation('t', 'a', { idempotencyKey: 'wk' });
  assert.equal(first.admitted, true);
  const retry = await bus.publishWithConfirmation('t', 'a', { idempotencyKey: 'wk' });
  assert.equal(retry.admitted, false);
  assert.equal(retry.reason, 'duplicate');
  assert.equal(bus.getStats().idempotentHits, 1);
});
