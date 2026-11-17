/**
 * Bus-level default dead-letter queue capacity live revision (EB-527).
 *
 * `setDefaultMaxDeadLetterEntries` decides how many entries a new
 * reliable subscriber's dead-letter queue keeps before the oldest
 * entry is evicted, for new reliable subscribers that opt into a
 * dead-letter queue and omit `maxEntries` — `3` to keep only the
 * three freshest poison samples fleet-wide with one call, `null` to
 * clear the default back to 1000. The priority chain is explicit
 * `maxEntries` > bus default > 1000; an explicit capacity always
 * wins. The default never enables a dead-letter queue — a subscriber
 * that does not pass `deadLetter` still has none. A revision only
 * affects reliable subscribers created after it, tightening the
 * default never evicts an existing subscriber's entries (eviction
 * still happens only on the next dead-letter enqueue), and a
 * revision identical to the current default (compared on the
 * effective capacity, including setting `1000` on a factory bus) is
 * a no-op with no audit. Audited as
 * `default_max_dead_letter_entries_revised`
 * (`{ previous, maxEntries }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type DeadLetterEvent } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_max_dead_letter_entries_revised' });

/** A reliable subscriber whose handler always nacks (dead-letters immediately). */
function failingSub(
  bus: EventBus,
  topic: string,
  deadLetter:
    | { maxEntries?: number | null; onDeadLetter?: (e: DeadLetterEvent) => void }
    | true,
) {
  const events: DeadLetterEvent[] = [];
  const sub = bus.subscribeReliable(
    topic,
    (d) => {
      d.nack();
    },
    {
      ackTimeoutMs: 1000,
      deadLetter:
        deadLetter === true
          ? { maxRedeliveries: 0, onDeadLetter: (e) => events.push(e) }
          : { maxRedeliveries: 0, onDeadLetter: (e) => events.push(e), ...deadLetter },
    },
  );
  return { events, sub };
}

test('EB-527: a bus default capacity of 3 evicts the oldest entry on the fourth dead letter', async () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), undefined);
  bus.setDefaultMaxDeadLetterEntries(3);
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), 3);
  const { events, sub } = failingSub(bus, 't', true);
  bus.publish('t', 'one');
  bus.publish('t', 'two');
  bus.publish('t', 'three');
  bus.publish('t', 'four');
  await flush();
  await sleep(200);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 3, 'the default capacity keeps three entries');
  assert.deepEqual(entries.map((e) => e.payload), ['two', 'three', 'four']);
  assert.deepEqual(events.map((e) => e.evictedOldest), [false, false, false, true]);
  assert.equal(defaultAudits(bus).length, 1);
  assert.deepEqual(defaultAudits(bus)[0].detail, { previous: null, maxEntries: 3 });
});

test('EB-527: an explicit maxEntries always beats the bus default', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(1);
  const { sub } = failingSub(bus, 't', { maxEntries: 3 });
  bus.publish('t', 'one');
  bus.publish('t', 'two');
  bus.publish('t', 'three');
  bus.publish('t', 'four');
  await flush();
  await sleep(200);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 3, 'the explicit capacity of 3 ran, not the default 1');
  assert.deepEqual(entries.map((e) => e.payload), ['two', 'three', 'four']);
  // An explicit `null` keeps the subscribe-time gate's own behavior
  // (capacity 1000) — it is not swallowed into the bus default.
  const bus2 = new EventBus();
  bus2.setDefaultMaxDeadLetterEntries(1);
  const nulled = failingSub(bus2, 't', { maxEntries: null });
  bus2.publish('t', 'one');
  bus2.publish('t', 'two');
  await flush();
  await sleep(200);
  assert.equal(
    bus2.getDeadLetterMessages(nulled.sub.id).length,
    2,
    'an explicit null still means the old gate capacity of 1000, not the default 1',
  );
});

test('EB-527: the default never enables a dead-letter queue', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(3);
  let failures = 0;
  const sub = bus.subscribeReliable(
    't',
    (d) => {
      failures += 1;
      if (failures <= 1) d.nack();
      else d.ack();
    },
    { ackTimeoutMs: 1000 },
  );
  bus.publish('t', 'hiccup');
  await flush();
  await sleep(200);
  assert.equal(failures, 2, 'immediate redelivery only — no DLQ was created');
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
});

test('EB-527: clearing the default returns new subscribers to a capacity of 1000', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(3);
  bus.setDefaultMaxDeadLetterEntries(null);
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), undefined);
  const { sub } = failingSub(bus, 't', true);
  // The subscriber queue holds 100, so a 1001-message burst would
  // shed most publishes before delivery — dead-letter in waves of
  // 100 so every message reaches the DLQ.
  for (let wave = 0; wave < 10; wave += 1) {
    for (let i = 1; i <= 100; i += 1) {
      bus.publish('t', `m${wave * 100 + i}`);
    }
    await flush();
    await sleep(100);
  }
  bus.publish('t', 'm1001');
  await flush();
  await sleep(200);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1000, 'the cleared default is back to the factory capacity of 1000');
  assert.equal(entries[0].payload, 'm2', 'the oldest entry was evicted');
  assert.equal(entries[999].payload, 'm1001');
  assert.equal(bus.getStats().deadLetteredMessages, 1001);
  assert.equal(defaultAudits(bus).length, 2);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: 3, maxEntries: null });
});

test('EB-527: illegal values throw RangeError and leave the live default and audit untouched', () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(3);
  assert.throws(() => bus.setDefaultMaxDeadLetterEntries(undefined as never), RangeError, 'undefined is not a clear');
  assert.throws(() => bus.setDefaultMaxDeadLetterEntries('3' as never), RangeError);
  assert.throws(() => bus.setDefaultMaxDeadLetterEntries({} as never), RangeError);
  assert.throws(() => bus.setDefaultMaxDeadLetterEntries(3, ''), RangeError, 'an illegal actor throws');
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    assert.throws(() => bus.setDefaultMaxDeadLetterEntries(bad as never), RangeError);
  }
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), 3, 'no failed revision moved the live default');
  assert.equal(defaultAudits(bus).length, 1, 'no failed revision is audited');
});

test('EB-527: re-affirming the current default is a silent no-op', () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(null);
  assert.equal(defaultAudits(bus).length, 0, 'clearing an already-unset default audits nothing');
  bus.setDefaultMaxDeadLetterEntries(1000);
  assert.equal(defaultAudits(bus).length, 0, 'setting 1000 on a factory bus audits nothing');
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), undefined, 'setting 1000 on a factory bus installs no default');
  bus.setDefaultMaxDeadLetterEntries(3);
  bus.setDefaultMaxDeadLetterEntries(3);
  assert.equal(defaultAudits(bus).length, 1, 're-setting the same capacity audits nothing');
  bus.setDefaultMaxDeadLetterEntries(1000);
  assert.equal(defaultAudits(bus).length, 2, 'moving 3 to 1000 audits');
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), 1000);
  bus.setDefaultMaxDeadLetterEntries(null);
  assert.equal(defaultAudits(bus).length, 3, 'clearing an installed default audits, even when it was 1000');
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), undefined);
  bus.setDefaultMaxDeadLetterEntries(null);
  assert.equal(defaultAudits(bus).length, 3, 'clearing an already-cleared default audits nothing');
});

test('EB-527: a revision never moves an already-subscribed subscriber', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(3);
  // Subscribed under the default: baked with capacity 3.
  const early = failingSub(bus, 'early', true);
  bus.publish('early', 'one');
  bus.publish('early', 'two');
  await flush();
  await sleep(200);
  assert.equal(bus.getDeadLetterMessages(early.sub.id).length, 2);
  // Tighten the default to 1: the existing subscriber keeps capacity
  // 3 and its entries — the next two dead letters evict only the
  // oldest one, leaving three entries, not one.
  bus.setDefaultMaxDeadLetterEntries(1);
  bus.publish('early', 'three');
  bus.publish('early', 'four');
  await flush();
  await sleep(200);
  const earlyEntries = bus.getDeadLetterMessages(early.sub.id);
  assert.equal(earlyEntries.length, 3, 'the pre-revision subscriber keeps its baked capacity');
  assert.deepEqual(earlyEntries.map((e) => e.payload), ['two', 'three', 'four']);
  // Subscribed after the revision: takes the new default.
  const late = failingSub(bus, 'late', true);
  bus.publish('late', 'one');
  bus.publish('late', 'two');
  await flush();
  await sleep(200);
  const lateEntries = bus.getDeadLetterMessages(late.sub.id);
  assert.equal(lateEntries.length, 1, 'the post-revision subscriber keeps only one entry');
  assert.deepEqual(lateEntries.map((e) => e.payload), ['two']);
});

test('EB-527: a default-baked subscriber is still revisable per subscriber, and a later default revision does not re-apply', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(3);
  // Tightening per subscriber never trims immediately: three entries
  // stay until the next enqueue, which sheds exactly one oldest
  // entry (EB-365 single-shift eviction).
  const trimmed = failingSub(bus, 'trim', true);
  bus.publish('trim', 'one');
  bus.publish('trim', 'two');
  bus.publish('trim', 'three');
  await flush();
  await sleep(200);
  assert.deepEqual(
    bus.getDeadLetterMessages(trimmed.sub.id).map((e) => e.payload),
    ['one', 'two', 'three'],
  );
  bus.setDeadLetterBudget(trimmed.sub.id, { maxEntries: 1 });
  assert.deepEqual(
    bus.getDeadLetterMessages(trimmed.sub.id).map((e) => e.payload),
    ['one', 'two', 'three'],
    'tightening does not immediately trim',
  );
  bus.publish('trim', 'four');
  await flush();
  await sleep(200);
  assert.deepEqual(
    bus.getDeadLetterMessages(trimmed.sub.id).map((e) => e.payload),
    ['two', 'three', 'four'],
    'the next enqueue sheds one oldest entry',
  );
  // The per-subscriber revision governs at low counts: with one
  // entry present, tightening to 1 evicts it on the next enqueue,
  // while the baked default of 3 would have kept both.
  const { sub } = failingSub(bus, 't', true);
  bus.publish('t', 'one');
  await flush();
  await sleep(200);
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  bus.setDeadLetterBudget(sub.id, { maxEntries: 1 });
  bus.publish('t', 'two');
  await flush();
  await sleep(200);
  assert.deepEqual(
    bus.getDeadLetterMessages(sub.id).map((e) => e.payload),
    ['two'],
    'the per-subscriber revision to 1 governs',
  );
  // Move the bus default afterwards: the already-revised subscriber
  // must not pick the new default up — a re-applied capacity of 2
  // would have kept both entries.
  bus.setDefaultMaxDeadLetterEntries(2);
  bus.publish('t', 'three');
  await flush();
  await sleep(200);
  assert.deepEqual(
    bus.getDeadLetterMessages(sub.id).map((e) => e.payload),
    ['three'],
    'the revised subscriber keeps capacity 1 under the new default',
  );
});

test('EB-527: the audit records previous and maxEntries exactly and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(3, 'ops');
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), 3);
  assert.equal(defaultAudits(bus).length, 1);
  assert.deepEqual(defaultAudits(bus)[0].detail, { previous: null, maxEntries: 3 });
  assert.equal(defaultAudits(bus)[0].actor, 'ops');
  bus.setDefaultMaxDeadLetterEntries(2);
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), 2);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: 3, maxEntries: 2 });
  assert.equal(defaultAudits(bus)[1].actor, 'system');
  bus.setDefaultMaxDeadLetterEntries(null);
  assert.equal(bus.getDefaultMaxDeadLetterEntries(), undefined);
  assert.deepEqual(defaultAudits(bus)[2].detail, { previous: 2, maxEntries: null });
});

test('EB-527: an explicit maxEntries of 1 still evicts on the second dead letter while a default is installed', async () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterEntries(3);
  const { events, sub } = failingSub(bus, 't', { maxEntries: 1 });
  bus.publish('t', 'one');
  bus.publish('t', 'two');
  await flush();
  await sleep(200);
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.equal(entries.length, 1, 'an explicit 1 is an explicit close, not a fallback');
  assert.deepEqual(entries.map((e) => e.payload), ['two']);
  assert.deepEqual(events.map((e) => e.evictedOldest), [false, true]);
});
