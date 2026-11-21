import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/**
 * Subscriber dedup live enable (EB-536):
 * `setDeduplicateMessages` on a subscription that never opted into
 * `deduplicateMessages` enables exactly-once dedup instead of
 * refusing: `true` resolves through the subscribe-time rule
 * (`resolveDeduplicateMessagesOptions`), an object patch resolves
 * its missing fields the same way (bus default, then the
 * publish-side idempotency window / 10000), and `null` is a silent
 * no-op. The enable builds a fresh empty table — messageIds
 * delivered before it are not retro-suppressed — carries no
 * `consumerId`, so nothing is journaled, and is live-only: a
 * restart restores the un-enabled state. Audited as
 * `deduplicate_messages_revised` with the old side `null`.
 *
 * Determinism tricks (as in `deduplicate-messages-revision.test.ts`):
 * an injected clock (`EventBusOptions.now`) drives the window, and
 * publishes fan out synchronously into the handler's queue — each
 * publish is one dedup evaluation, exactly like the production path.
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

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-dedupenable-'));

const revisionAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'deduplicate_messages_revised' });

test('EB-536: true enables dedup — in-window duplicates suppressed, out-of-window delivered', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg));
  // No dedup yet: the duplicate is delivered.
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2);

  bus.setDeduplicateMessages(sub.id, true);
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, {
    windowMs: 60_000,
    oldWindowMs: null,
    maxEntries: 10_000,
    oldMaxEntries: null,
  });

  bus.publish('t', 'b', { messageId: 'm2' });
  bus.publish('t', 'b', { messageId: 'm2' });
  await flush();
  assert.equal(received.length, 3, 'the in-window duplicate must be suppressed');
  assert.equal(bus.getStats().dedupDropped, 1);
  clock.advance(60_001);
  bus.publish('t', 'b', { messageId: 'm2' });
  await flush();
  assert.equal(received.length, 4, 'past the default 60s window the id is unknown again');
  sub.unsubscribe();
});

test('EB-536: an object patch enables with missing fields from the bus default and the publish-side window', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 5000 });
  bus.setDefaultDeduplicateMessages({ maxEntries: 2 });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg));
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    windowMs: 1000,
    oldWindowMs: null,
    maxEntries: 2,
    oldMaxEntries: null,
  });
  // The resolved bound governs: the third id evicts the first.
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'b', { messageId: 'm2' });
  bus.publish('t', 'c', { messageId: 'm3' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.deepEqual(received.map((m) => m.payload), ['a', 'b', 'c', 'a']);
  sub.unsubscribe();

  // Without a bus default, `true` takes the publish-side window.
  const bus2 = new EventBus({ now: clock.now, idempotencyWindowMs: 5000 });
  const sub2 = bus2.subscribe('t', () => {});
  bus2.setDeduplicateMessages(sub2.id, true);
  assert.deepEqual(revisionAudits(bus2)[0].detail, {
    windowMs: 5000,
    oldWindowMs: null,
    maxEntries: 10_000,
    oldMaxEntries: null,
  });
  sub2.unsubscribe();
});

test('EB-536: messageIds delivered before the enable are not retro-suppressed', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg), {});
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1);
  bus.setDeduplicateMessages(sub.id, true);
  // The pre-enable sighting is not in the fresh empty table.
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2, 'the enable must not backfill the table');
  // But the post-enable arrival is recorded: a third copy suppresses.
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2);
  assert.equal(bus.getStats().dedupDropped, 1);
  sub.unsubscribe();
});

test('EB-536: post-enable arrivals are bound by the new window and bound', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg));
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000, maxEntries: 2 });
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'b', { messageId: 'm2' });
  bus.publish('t', 'c', { messageId: 'm3' }); // evicts m1
  bus.publish('t', 'a', { messageId: 'm1' }); // evicted: delivered again
  bus.publish('t', 'a', { messageId: 'm1' }); // retained: suppressed
  await flush();
  assert.deepEqual(received.map((m) => m.payload), ['a', 'b', 'c', 'a']);
  assert.equal(bus.getStats().dedupDropped, 1);
  clock.advance(1001);
  bus.publish('t', 'b', { messageId: 'm2' }); // past the window: delivered
  await flush();
  assert.equal(received.length, 5);
  sub.unsubscribe();
});

test('EB-536: null on a never-enabled subscriber is a silent no-op with no audit', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg));
  bus.setDeduplicateMessages(sub.id, null);
  assert.equal(revisionAudits(bus).length, 0);
  // Dedup is still off: duplicates are delivered.
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2);
  assert.equal(bus.getStats().dedupDropped, 0);
  sub.unsubscribe();
});

test('EB-536: unknown and empty subscriber ids still throw RangeError', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  for (const bad of [
    () => bus.setDeduplicateMessages('no-such-sub', true),
    () => bus.setDeduplicateMessages('no-such-sub', { windowMs: 100 }),
    () => bus.setDeduplicateMessages('', true),
    () => bus.setDeduplicateMessages(sub.id, 'x' as never),
    () => bus.setDeduplicateMessages(sub.id, 5 as never),
    () => bus.setDeduplicateMessages(sub.id, [] as never),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(revisionAudits(bus).length, 0, 'failed enables must not audit');
  sub.unsubscribe();
});

test('EB-536: illegal enable values throw RangeError and leave dedup off', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg));
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
    () => bus.setDeduplicateMessages(sub.id, { windowMs: 500, maxEntries: 0 }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(revisionAudits(bus).length, 0, 'failed enables must not audit');
  // Dedup is still off: duplicates are delivered.
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2);
  sub.unsubscribe();
});

test('EB-536: a partial revision after the enable still merges the live values', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000, maxEntries: 3 });
  bus.setDeduplicateMessages(sub.id, { maxEntries: 5 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    windowMs: 1000,
    oldWindowMs: 1000,
    maxEntries: 5,
    oldMaxEntries: 3,
  });
  sub.unsubscribe();
});

test('EB-536: re-affirming the enabled window is a no-op with no audit', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000, maxEntries: 3 });
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000, maxEntries: 3 });
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000 });
  bus.setDeduplicateMessages(sub.id, { maxEntries: 3 });
  bus.setDeduplicateMessages(sub.id, {});
  bus.setDeduplicateMessages(sub.id, undefined as never);
  assert.equal(revisionAudits(bus).length, 1, 'only the enable audits');
  sub.unsubscribe();
});

test('EB-536: the enable audit records a null previous and actor system', async () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  bus.setDeduplicateMessages(sub.id, { windowMs: 1000 });
  const audit = revisionAudits(bus);
  assert.equal(audit.length, 1);
  assert.equal(audit[0].subId, sub.id);
  assert.equal(audit[0].actor, 'system');
  assert.deepEqual(audit[0].detail, {
    windowMs: 1000,
    oldWindowMs: null,
    maxEntries: 10_000,
    oldMaxEntries: null,
  });
  sub.unsubscribe();
});

test('EB-536: an enable carries no consumerId — post-enable sightings are not journaled', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const received: BusMessage[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg));
  bus.setDeduplicateMessages(sub.id, true);
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1, 'the memory table still suppresses in-window');
  assert.equal(
    existsSync(join(dir, '__dedup.jsonl')),
    false,
    'an enable without a consumerId must not journal dedup sightings',
  );
  sub.unsubscribe();
});

test('EB-536: the live enable is gone after a restart', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const sub = bus.subscribe('t', () => {});
  bus.setDeduplicateMessages(sub.id, true);
  assert.equal(revisionAudits(bus).length, 1);
  // A fresh bus — the restart — has no record of the live enable:
  // a plain re-subscription delivers duplicates again.
  const bus2 = new EventBus({ durableLogDir: dir });
  const received: BusMessage[] = [];
  bus2.subscribe('t', (msg) => received.push(msg));
  bus2.publish('t', 'a', { messageId: 'm1' });
  bus2.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2, 'the enable must not survive a restart');
  assert.equal(revisionAudits(bus2).length, 0);
  sub.unsubscribe();
});
