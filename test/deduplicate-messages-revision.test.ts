import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/**
 * Subscriber dedup window revision (EB-295):
 * `setDeduplicateMessages` retunes a dedup-enabled subscriber's
 * `windowMs` / `maxEntries` without re-subscribing — only those two
 * fields move, the `consumerId` and the recorded `(topic, messageId)`
 * table are untouched, and the new values govern from the next
 * arrival (which reads the live values).
 *
 * Determinism tricks (as in `dedup-window.test.ts`): an injected
 * clock (`EventBusOptions.now`) drives the window, and publishes
 * fan out synchronously into the handler's queue — each publish is
 * one dedup evaluation, exactly like the production path.
 */

/** Injectable clock so the dedup window is deterministic. */
function makeClock(startMs = 0) {
  let nowMs = startMs;
  return {
    now: () => nowMs,
    advance: (ms: number) => {
      nowMs += ms;
    },
  };
}

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-deduprev-'));

test('setDeduplicateMessages rejects unknown, empty, and non-dedup subscribers fail-closed', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, { deduplicateMessages: true });
  const plain = bus.subscribe('t2', () => {});
  for (const bad of [
    () => bus.setDeduplicateMessages('no-such-sub', { windowMs: 100 }),
    () => bus.setDeduplicateMessages('', { windowMs: 100 }),
    () => bus.setDeduplicateMessages(plain.id, { windowMs: 100 }),
    () => bus.setDeduplicateMessages(plain.id, { maxEntries: 5 }),
    () => bus.setDeduplicateMessages(sub.id, 'x' as never),
    () => bus.setDeduplicateMessages(sub.id, 5 as never),
    () => bus.setDeduplicateMessages(sub.id, [] as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live window is untouched: dedup still works as subscribed.
  const received: BusMessage[] = [];
  const probe = bus.subscribe('t3', (msg) => received.push(msg), { deduplicateMessages: true });
  bus.publish('t3', 'a', { messageId: 'm1' });
  bus.publish('t3', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().dedupDropped, 1);
  sub.unsubscribe();
  plain.unsubscribe();
  probe.unsubscribe();
});

test('illegal windowMs / maxEntries throw RangeError and leave the live window untouched', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg), {
    deduplicateMessages: { windowMs: 1000, maxEntries: 2 },
  });
  for (const bad of [
    () => bus.setDeduplicateMessages(sub.id, { windowMs: 0 }),
    () => bus.setDeduplicateMessages(sub.id, { windowMs: -5 }),
    () => bus.setDeduplicateMessages(sub.id, { windowMs: Number.NaN }),
    () => bus.setDeduplicateMessages(sub.id, { windowMs: Number.POSITIVE_INFINITY }),
    () => bus.setDeduplicateMessages(sub.id, { windowMs: '1000' as never }),
    () => bus.setDeduplicateMessages(sub.id, { maxEntries: 0 }),
    () => bus.setDeduplicateMessages(sub.id, { maxEntries: -1 }),
    () => bus.setDeduplicateMessages(sub.id, { maxEntries: 1.5 }),
    () => bus.setDeduplicateMessages(sub.id, { maxEntries: Number.NaN }),
    () => bus.setDeduplicateMessages(sub.id, { maxEntries: '2' as never }),
    // One illegal field vetoes the whole revision: the legal field
    // must not move either.
    () => bus.setDeduplicateMessages(sub.id, { windowMs: 500, maxEntries: 0 }),
    () => bus.setDeduplicateMessages(sub.id, { windowMs: 0, maxEntries: 5 }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live window is untouched: windowMs still 1000, maxEntries still 2.
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  clock.advance(999);
  bus.publish('t', 'a', { messageId: 'm1' }); // still inside the window
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().dedupDropped, 1);
  clock.advance(2); // 1001ms after the first delivery: the window lapsed
  bus.publish('t', 'a', { messageId: 'm1' }); // unknown again: delivered
  await flush();
  assert.equal(received.length, 2);
  bus.publish('t', 'b', { messageId: 'm2' });
  bus.publish('t', 'c', { messageId: 'm3' }); // evicts m1 (maxEntries still 2)
  bus.publish('t', 'a', { messageId: 'm1' }); // evicted: delivered again
  bus.publish('t', 'c', { messageId: 'm3' }); // retained: suppressed
  await flush();
  assert.deepEqual(received.map((m) => m.payload), ['a', 'a', 'b', 'c', 'a']);
  assert.equal(bus.getStats().dedupDropped, 2);
  sub.unsubscribe();
});

test('shrinking the window expires entries past the new horizon on the next arrival', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg), {
    deduplicateMessages: { windowMs: 1000 },
  });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  clock.advance(150);
  bus.setDeduplicateMessages(sub.id, { windowMs: 100 });
  bus.publish('t', 'a', { messageId: 'm1' }); // 150ms old: past the new 100ms window
  await flush();
  assert.equal(received.length, 2, 'the shrunk window must forget the aged-out entry');
  assert.equal(bus.getStats().dedupDropped, 0);
  const audit = bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' });
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.deepEqual(audit[0].detail, {
    windowMs: 100,
    oldWindowMs: 1000,
    maxEntries: 10_000,
    oldMaxEntries: 10_000,
  });
  sub.unsubscribe();
});

test('widening the window extends recorded entries to the new horizon', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg), {
    deduplicateMessages: { windowMs: 100 },
  });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  clock.advance(150); // past the old 100ms window, inside the new 1000ms one
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000 });
  bus.publish('t', 'a', { messageId: 'm1' }); // still remembered: suppressed
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().dedupDropped, 1);
  const audit = bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    windowMs: 1000,
    oldWindowMs: 100,
    maxEntries: 10_000,
    oldMaxEntries: 10_000,
  });
  sub.unsubscribe();
});

test('tightening maxEntries evicts the oldest entries on the next arrival', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg), {
    deduplicateMessages: { windowMs: 60_000, maxEntries: 3 },
  });
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'b', { messageId: 'm2' });
  bus.publish('t', 'c', { messageId: 'm3' });
  bus.setDeduplicateMessages(sub.id, { maxEntries: 1 });
  bus.publish('t', 'd', { messageId: 'm4' }); // evicts m1, m2, m3 down to the new bound
  bus.publish('t', 'a', { messageId: 'm1' }); // evicted: delivered again (and evicts m4: the bound is 1)
  bus.publish('t', 'a', { messageId: 'm1' }); // retained as the newest: suppressed
  bus.publish('t', 'd', { messageId: 'm4' }); // evicted by m1's re-insertion: delivered again
  await flush();
  assert.deepEqual(received.map((m) => m.payload), ['a', 'b', 'c', 'd', 'a', 'd']);
  assert.equal(bus.getStats().dedupDropped, 1);
  const audit = bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    windowMs: 60_000,
    oldWindowMs: 60_000,
    maxEntries: 1,
    oldMaxEntries: 3,
  });
  sub.unsubscribe();
});

test('a partial revision moves only the provided field', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg), {
    deduplicateMessages: { windowMs: 1000, maxEntries: 2 },
  });
  // Only maxEntries moves: the window stays 1000ms.
  bus.setDeduplicateMessages(sub.id, { maxEntries: 5 });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  clock.advance(999);
  bus.publish('t', 'a', { messageId: 'm1' }); // still inside the unchanged window
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().dedupDropped, 1);
  // Only windowMs moves: the bound stays 5.
  bus.setDeduplicateMessages(sub.id, { windowMs: 100 });
  bus.publish('t', 'b', { messageId: 'm2' });
  bus.publish('t', 'c', { messageId: 'm3' });
  bus.publish('t', 'd', { messageId: 'm4' });
  bus.publish('t', 'e', { messageId: 'm5' });
  bus.publish('t', 'f', { messageId: 'm6' }); // 5 entries at the unchanged bound
  bus.publish('t', 'b', { messageId: 'm2' }); // retained: suppressed
  await flush();
  assert.equal(bus.getStats().dedupDropped, 2);
  const audit = bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[0].detail, {
    windowMs: 1000,
    oldWindowMs: 1000,
    maxEntries: 5,
    oldMaxEntries: 2,
  });
  assert.deepEqual(audit[1].detail, {
    windowMs: 100,
    oldWindowMs: 1000,
    maxEntries: 5,
    oldMaxEntries: 5,
  });
  sub.unsubscribe();
});

test('re-affirming the current window, or revising nothing, is a no-op with no audit', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {}, {
    deduplicateMessages: { windowMs: 1000, maxEntries: 3 },
  });
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000, maxEntries: 3 });
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000 });
  bus.setDeduplicateMessages(sub.id, { maxEntries: 3 });
  bus.setDeduplicateMessages(sub.id, {});
  bus.setDeduplicateMessages(sub.id, undefined as never);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' }).length,
    0,
    'a same-window revision must not audit',
  );
  // The window is unchanged: dedup still behaves as subscribed.
  const received: BusMessage[] = [];
  const probe = bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: true });
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1);
  sub.unsubscribe();
  probe.unsubscribe();
});

test('a revision keeps the consumerId and the recorded entries', async () => {
  const dir = freshDir();
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg), {
    deduplicateMessages: { windowMs: 1000, maxEntries: 10, consumerId: 'c1' },
  });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  bus.setDeduplicateMessages(sub.id, { windowMs: 5000 });
  // The recorded entry survives the revision: still suppressed.
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().dedupDropped, 1);
  // The consumerId survives too: post-revision sightings keep
  // journaling under the same identity.
  bus.publish('t', 'b', { messageId: 'm2' });
  await flush();
  assert.equal(received.length, 2);
  const journal = readFileSync(join(dir, '__dedup.jsonl'), 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line));
  const byConsumer = journal.filter((entry) => entry.consumer === 'c1');
  assert.deepEqual(
    byConsumer.map((entry) => entry.messageId),
    ['m1', 'm2'],
    'both the pre- and post-revision sightings journal under c1',
  );
  const audit = bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    windowMs: 5000,
    oldWindowMs: 1000,
    maxEntries: 10,
    oldMaxEntries: 10,
  });
  sub.unsubscribe();
});
