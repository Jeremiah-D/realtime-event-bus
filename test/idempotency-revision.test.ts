/**
 * Bus-level idempotency dedup tuning revision (EB-316).
 *
 * `setIdempotency` retunes the authoritative `(topic, messageId)` dedup
 * table's window and cap live — without restarting the bus. The table
 * itself is never cleared or rewritten: every lookup and eviction reads
 * both values live, so a revised window re-judges old EB-27 entries at
 * the next dedup check and a revised cap evicts the oldest entries down
 * to the new bound at the next claim. Illegal values fail closed with
 * the live tuning and the audit log untouched; an identical (or empty)
 * revision is a no-op with no audit.
 *
 * One verified asymmetry: content-dedup and explicit-key claims
 * (EB-146/EB-151) store their own `windowMs` at claim time, so a revised
 * bus window governs only claims made after the revision — an in-flight
 * content or key claim is still judged against the window it was claimed
 * with — while their eviction still follows the live `maxEntries` cap.
 * The bloom pre-filter (EB-214/EB-312) is untouched by this revision.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const audit = (bus: EventBus) => bus.getSubscriptionAudit({ action: 'idempotency_revised' });

test('constructor still rejects illegal idempotency tuning', () => {
  for (const bad of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, '60000']) {
    assert.throws(() => new EventBus({ idempotencyWindowMs: bad as never }), RangeError);
  }
  for (const bad of [0, -1, 1.5, Number.NaN, '10000']) {
    assert.throws(() => new EventBus({ idempotencyMaxEntries: bad as never }), RangeError);
  }
});

test('setIdempotency rejects illegal values and leaves the live tuning untouched', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' });
  assert.deepEqual(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' }), {
    duplicate: true,
    accepted: 0,
  });
  const bad: Array<unknown> = [
    { windowMs: 0 },
    { windowMs: -1 },
    { windowMs: Number.NaN },
    { windowMs: Number.POSITIVE_INFINITY },
    { windowMs: '60000' },
    { maxEntries: 0 },
    { maxEntries: -1 },
    { maxEntries: 1.5 },
    { maxEntries: Number.NaN },
    { maxEntries: '10000' },
    // One good field does not save a bad sibling: the whole call fails.
    { windowMs: 5_000, maxEntries: 0 },
    { windowMs: 0, maxEntries: 100 },
    'nope',
    42,
    [],
  ];
  for (const revision of bad) {
    assert.throws(
      () => bus.setIdempotency(revision as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(revision)}`,
    );
  }
  // The failed calls changed nothing: same window, same cap, no audit.
  assert.equal(audit(bus).length, 0);
  assert.deepEqual(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' }), {
    duplicate: true,
    accepted: 0,
  });
  // A valid revision afterwards still works on the untouched tuning.
  bus.setIdempotency({ windowMs: 5_000 });
  assert.equal(audit(bus).length, 1);
  assert.deepEqual(audit(bus)[0].detail, {
    windowMs: 5_000,
    oldWindowMs: 60_000,
    maxEntries: 10_000,
    oldMaxEntries: 10_000,
  });
  bus.close();
});

test('setIdempotency with identical values or an empty revision is a no-op with no audit', () => {
  const bus = new EventBus({ idempotencyWindowMs: 5_000, idempotencyMaxEntries: 100 });
  bus.setIdempotency({ windowMs: 5_000 });
  bus.setIdempotency({ maxEntries: 100 });
  bus.setIdempotency({ windowMs: 5_000, maxEntries: 100 });
  bus.setIdempotency({});
  bus.setIdempotency(null as never);
  assert.equal(audit(bus).length, 0);
  bus.close();
});

test('partial revision moves only the provided field', () => {
  const bus = new EventBus({ idempotencyWindowMs: 5_000, idempotencyMaxEntries: 100 });
  bus.setIdempotency({ maxEntries: 50 });
  let entries = audit(bus);
  assert.equal(entries.length, 1);
  assert.deepEqual(entries[0].detail, {
    windowMs: 5_000,
    oldWindowMs: 5_000,
    maxEntries: 50,
    oldMaxEntries: 100,
  });
  bus.setIdempotency({ windowMs: 30_000 });
  entries = audit(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[1].detail, {
    windowMs: 30_000,
    oldWindowMs: 5_000,
    maxEntries: 50,
    oldMaxEntries: 50,
  });
  bus.close();
});

test('tightening windowMs expires old EB-27 entries at the next dedup check', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' });
  clock.nowMs += 30_000;
  // 30 s in, still inside the 60 s window: a retry is a duplicate.
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' }).duplicate, true);
  bus.setIdempotency({ windowMs: 10_000 });
  // The same 30 s-old claim is now past the tightened 10 s window.
  assert.deepEqual(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' }), {
    duplicate: false,
    accepted: 1,
  });
  // The fresh claim is judged under the new window.
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' }).duplicate, true);
  bus.close();
});

test('widening windowMs re-judges old EB-27 entries under the new window', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 10_000 });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' });
  clock.nowMs += 30_000;
  // 30 s in, past the 10 s window: the retry would be fresh.
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' }).duplicate, false);
  // Re-claim, then widen the window and let the claim age past the old one.
  clock.nowMs += 30_000;
  bus.setIdempotency({ windowMs: 60_000 });
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'msg-1' }).duplicate, true);
  bus.close();
});

test('tightening maxEntries evicts the oldest entries down to the new cap at the next claim', () => {
  const bus = new EventBus({ idempotencyMaxEntries: 3 });
  bus.subscribe('t', () => {});
  for (const id of ['m1', 'm2', 'm3']) {
    bus.publishIdempotent('t', { n: 1 }, { messageId: id });
  }
  bus.setIdempotency({ maxEntries: 2 });
  // The next claim evicts down to the new cap: the two oldest go.
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'm4' });
  // Check the survivors first: a fresh retry re-claims and evicts again.
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'm3' }).duplicate, true);
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'm4' }).duplicate, true);
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'm1' }).duplicate, false);
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'm2' }).duplicate, false);
  bus.close();
});

test('widening maxEntries admits more claims without evicting', () => {
  const bus = new EventBus({ idempotencyMaxEntries: 2 });
  bus.subscribe('t', () => {});
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'm1' });
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'm2' });
  bus.setIdempotency({ maxEntries: 3 });
  bus.publishIdempotent('t', { n: 1 }, { messageId: 'm3' });
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'm1' }).duplicate, true);
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'm2' }).duplicate, true);
  assert.equal(bus.publishIdempotent('t', { n: 1 }, { messageId: 'm3' }).duplicate, true);
  bus.close();
});

test('content-dedup claims keep the window they were claimed under', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  bus.subscribe('u', () => {});
  // Content claim stamped with the 60 s bus window at claim time.
  bus.publishDedupedByContent('t', { order: 1, amount: 100 });
  // EB-27 claim of the same age, for contrast.
  bus.publishIdempotent('u', { n: 1 }, { messageId: 'msg-1' });
  bus.setIdempotency({ windowMs: 1_000 });
  clock.nowMs += 5_000;
  // The content claim is still judged against its claim-time 60 s window.
  assert.deepEqual(bus.publishDedupedByContent('t', { order: 1, amount: 100 }), {
    duplicate: true,
    accepted: 0,
  });
  // The EB-27 claim of the same age expired under the tightened window.
  assert.equal(bus.publishIdempotent('u', { n: 1 }, { messageId: 'msg-1' }).duplicate, false);
  bus.close();
});

test('explicit-key claims keep the window they were claimed under when widening', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now, idempotencyWindowMs: 1_000 });
  bus.subscribe('t', () => {});
  bus.subscribe('u', () => {});
  // Key claim stamped with the 1 s bus window at claim time.
  const first = bus.publishDedupedByContent(
    't',
    { order: 1, amount: 100 },
    { idempotencyKey: 'key-1' },
  );
  assert.equal(first.duplicate, false);
  // EB-27 claim of the same age, for contrast.
  bus.publishIdempotent('u', { n: 1 }, { messageId: 'msg-1' });
  bus.setIdempotency({ windowMs: 60_000 });
  clock.nowMs += 5_000;
  // The key claim expired against its claim-time 1 s window, so the retry
  // is a fresh publish even though the bus window is now 60 s.
  assert.deepEqual(
    bus.publishDedupedByContent('t', { order: 1, amount: 100 }, { idempotencyKey: 'key-1' }),
    { duplicate: false, accepted: 1 },
  );
  // The EB-27 claim of the same age is re-judged under the widened window.
  assert.equal(bus.publishIdempotent('u', { n: 1 }, { messageId: 'msg-1' }).duplicate, true);
  bus.close();
});
