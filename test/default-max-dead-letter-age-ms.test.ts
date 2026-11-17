/**
 * Bus-level default dead-letter entry TTL live revision (EB-528).
 *
 * `setDefaultMaxDeadLetterAgeMs` decides how long a dead-letter
 * entry survives before the next lazy sweep drops it, for new
 * reliable subscribers that opt into a dead-letter queue and omit
 * `maxAgeMs` — `60_000` to give every new poison sample a
 * one-minute retention window fleet-wide with one call, `null` to
 * clear the default back to no TTL. The priority chain is explicit
 * `maxAgeMs` > bus default > no TTL; an explicit TTL always wins.
 * The default never enables a dead-letter queue — a subscriber
 * that does not pass `deadLetter` still has none. A revision only
 * affects reliable subscribers created after it, and a revision
 * identical to the current default (re-setting the same TTL, or
 * clearing an already-unset default) is a no-op with no audit.
 * Audited as `default_max_dead_letter_age_ms_revised`
 * (`{ previous, maxAgeMs }`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type DeadLetterEvent } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const defaultAudits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_max_dead_letter_age_ms_revised' });

/** A reliable subscriber whose handler always nacks (dead-letters immediately). */
function failingSub(
  bus: EventBus,
  topic: string,
  deadLetter:
    | { maxAgeMs?: number | null; onDeadLetter?: (e: DeadLetterEvent) => void }
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

/** Dead-letters one message on a reliable subscription. */
async function deadLetterOne(bus: EventBus, topic: string, payload: unknown): Promise<void> {
  bus.publish(topic, payload);
  await flush();
  await flush();
  await flush();
}

test('EB-528: a bus default TTL of 60_000 expires the entry on the next lazy sweep and counts it', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), undefined);
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), 60_000);
  const { sub } = failingSub(bus, 't', true);
  await deadLetterOne(bus, 't', 'old');
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
  // Within the TTL the entry is still there.
  clock.nowMs += 59_999;
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
  // Past the TTL the query lazily drops it and counts it once.
  clock.nowMs += 1;
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
  // Re-querying does not double-count.
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
  assert.equal(defaultAudits(bus).length, 1);
  assert.deepEqual(defaultAudits(bus)[0].detail, { previous: null, maxAgeMs: 60_000 });
});

test('EB-528: an explicit maxAgeMs always beats the bus default', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
  const { sub } = failingSub(bus, 't', { maxAgeMs: 10_000 });
  await deadLetterOne(bus, 't', 'short-lived');
  clock.nowMs += 10_000;
  assert.deepEqual(bus.getDeadLetterMessages(sub.id), []);
  assert.equal(bus.getStats().dlqExpiredMessages, 1, 'the explicit 10s TTL ran, not the default 60s');
  // An explicit `null` keeps the subscribe-time gate's own behavior
  // (a RangeError) — it is not swallowed into the bus default.
  const bus2 = new EventBus({ now: clock.now });
  bus2.setDefaultMaxDeadLetterAgeMs(60_000);
  assert.throws(
    () =>
      bus2.subscribeReliable(
        't',
        (d) => {
          d.nack();
        },
        { deadLetter: { maxRedeliveries: 0, maxAgeMs: null } },
      ),
    RangeError,
    'an explicit null still throws at subscribe time, even with a default installed',
  );
});

test('EB-528: the default never enables a dead-letter queue', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
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

test('EB-528: clearing the default returns new subscribers to no TTL', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
  bus.setDefaultMaxDeadLetterAgeMs(null);
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), undefined);
  const { sub } = failingSub(bus, 't', true);
  await deadLetterOne(bus, 't', 'kept');
  clock.nowMs += 61_000;
  assert.equal(
    bus.getDeadLetterMessages(sub.id).length,
    1,
    'the cleared default is back to no TTL — the entry survives past 60s',
  );
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
  assert.equal(defaultAudits(bus).length, 2);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: 60_000, maxAgeMs: null });
});

test('EB-528: illegal values throw RangeError and leave the live default and audit untouched', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
  assert.throws(() => bus.setDefaultMaxDeadLetterAgeMs(undefined as never), RangeError, 'undefined is not a clear');
  assert.throws(() => bus.setDefaultMaxDeadLetterAgeMs('60000' as never), RangeError);
  assert.throws(() => bus.setDefaultMaxDeadLetterAgeMs({} as never), RangeError);
  assert.throws(() => bus.setDefaultMaxDeadLetterAgeMs(60_000, ''), RangeError, 'an illegal actor throws');
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    assert.throws(() => bus.setDefaultMaxDeadLetterAgeMs(bad as never), RangeError);
  }
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), 60_000, 'no failed revision moved the live default');
  assert.equal(defaultAudits(bus).length, 1, 'no failed revision is audited');
});

test('EB-528: re-affirming the current default is a silent no-op', () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterAgeMs(null);
  assert.equal(defaultAudits(bus).length, 0, 'clearing an already-unset default audits nothing');
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
  assert.equal(defaultAudits(bus).length, 1, 're-setting the same TTL audits nothing');
  bus.setDefaultMaxDeadLetterAgeMs(10_000);
  assert.equal(defaultAudits(bus).length, 2, 'moving 60_000 to 10_000 audits');
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), 10_000);
  bus.setDefaultMaxDeadLetterAgeMs(null);
  assert.equal(defaultAudits(bus).length, 3, 'clearing an installed default audits');
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), undefined);
  bus.setDefaultMaxDeadLetterAgeMs(null);
  assert.equal(defaultAudits(bus).length, 3, 'clearing an already-cleared default audits nothing');
});

test('EB-528: a revision never moves an already-subscribed subscriber or its entries', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
  // Subscribed under the default: baked with a 60s TTL.
  const early = failingSub(bus, 'early', true);
  await deadLetterOne(bus, 'early', 'one');
  assert.equal(bus.getDeadLetterMessages(early.sub.id).length, 1);
  // Tighten the default to 1s: the existing subscriber keeps its
  // baked 60s TTL and its entry — 2s later it is still there.
  bus.setDefaultMaxDeadLetterAgeMs(1_000);
  clock.nowMs += 2_000;
  assert.equal(
    bus.getDeadLetterMessages(early.sub.id).length,
    1,
    'the pre-revision subscriber keeps its baked TTL',
  );
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
  // Subscribed after the revision: takes the new default.
  const late = failingSub(bus, 'late', true);
  await deadLetterOne(bus, 'late', 'one');
  clock.nowMs += 1_001;
  assert.deepEqual(bus.getDeadLetterMessages(late.sub.id), [], 'the post-revision subscriber expires after 1s');
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
  // The early entry is still alive under its baked 60s TTL.
  assert.equal(bus.getDeadLetterMessages(early.sub.id).length, 1);
});

test('EB-528: a default-baked subscriber is still revisable per subscriber, and a later default revision does not re-apply', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultMaxDeadLetterAgeMs(60_000);
  const { sub } = failingSub(bus, 't', true);
  await deadLetterOne(bus, 't', 'one');
  // Clear the TTL per subscriber: the entry no longer expires.
  bus.setDeadLetterBudget(sub.id, { maxAgeMs: null });
  clock.nowMs += 61_000;
  assert.equal(
    bus.getDeadLetterMessages(sub.id).length,
    1,
    'the per-subscriber null clear stops expiry',
  );
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
  // Move the bus default afterwards: the already-revised subscriber
  // must not pick the new default up — a re-applied TTL of 1s would
  // have expired the entry.
  bus.setDefaultMaxDeadLetterAgeMs(1_000);
  clock.nowMs += 2_000;
  assert.equal(
    bus.getDeadLetterMessages(sub.id).length,
    1,
    'the revised subscriber keeps no TTL under the new default',
  );
  // A second default-baked subscriber can still be tightened per
  // subscriber: revising its TTL to 500ms expires the entry after
  // 500ms, while the baked 1s default would still have kept it.
  const tightened = failingSub(bus, 'tight', true);
  await deadLetterOne(bus, 'tight', 'one');
  bus.setDeadLetterBudget(tightened.sub.id, { maxAgeMs: 500 });
  clock.nowMs += 501;
  assert.deepEqual(bus.getDeadLetterMessages(tightened.sub.id), [], 'the per-subscriber revision governs');
  assert.equal(bus.getStats().dlqExpiredMessages, 1);
});

test('EB-528: the audit records previous and maxAgeMs exactly and the getter follows the live value', () => {
  const bus = new EventBus();
  bus.setDefaultMaxDeadLetterAgeMs(60_000, 'ops');
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), 60_000);
  assert.equal(defaultAudits(bus).length, 1);
  assert.deepEqual(defaultAudits(bus)[0].detail, { previous: null, maxAgeMs: 60_000 });
  assert.equal(defaultAudits(bus)[0].actor, 'ops');
  bus.setDefaultMaxDeadLetterAgeMs(10_000);
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), 10_000);
  assert.deepEqual(defaultAudits(bus)[1].detail, { previous: 60_000, maxAgeMs: 10_000 });
  assert.equal(defaultAudits(bus)[1].actor, 'system');
  bus.setDefaultMaxDeadLetterAgeMs(null);
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), undefined);
  assert.deepEqual(defaultAudits(bus)[2].detail, { previous: 10_000, maxAgeMs: null });
});

test('EB-528: a factory bus has no default TTL — entries never expire', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  assert.equal(bus.getDefaultMaxDeadLetterAgeMs(), undefined);
  const { sub } = failingSub(bus, 't', true);
  await deadLetterOne(bus, 't', 'kept');
  clock.nowMs += 10_000_000;
  assert.equal(bus.getDeadLetterMessages(sub.id).length, 1);
  assert.equal(bus.getStats().dlqExpiredMessages, 0);
});
