import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusMessage, type DeduplicateMessagesDefaultOptions } from '../src/bus.ts';

/**
 * Bus-level default subscriber-side dedup (EB-502):
 * `setDefaultDeduplicateMessages` installs the `windowMs` /
 * `maxEntries` new opt-in `deduplicateMessages` subscribers fall back
 * to when they omit a field — the priority chain is
 * subscriber-explicit > bus default > built-in (`windowMs` falls back
 * to the bus's publish-side idempotency window, `maxEntries` to
 * 10000) — and the default never enables dedup: a subscriber that
 * does not opt into `deduplicateMessages` is untouched by it. The
 * default carries no `consumerId`: a durable identity can never be
 * shared fleet-wide.
 *
 * Dedup assertions follow `dedup-window.test.ts`: an injectable clock
 * makes the window deterministic — an entry expires once
 * `now - seenAt >= windowMs` — and the bounded eviction drops the
 * oldest entry once the table exceeds `maxEntries`.
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

test('EB-502: the factory bus default is unset (getter undefined, clearing audits nothing)', () => {
  const bus = new EventBus();
  assert.equal(bus.getDefaultDeduplicateMessages(), undefined);
  bus.setDefaultDeduplicateMessages(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_deduplicate_messages_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
});

test('EB-502: `true` opt-in takes the default window and capacity', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeduplicateMessages({ windowMs: 1_000, maxEntries: 2 });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: true });
  // Window: the default 1000ms governs — still suppressed at 999ms,
  // delivered again once the window has elapsed.
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1);
  clock.advance(999);
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1, 'still inside the default window');
  clock.advance(1);
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2, 'the default window has elapsed');
  // Capacity: the default bound of 2 evicts the oldest identity, so
  // a re-published m1 is unknown again after m2 and m3 arrive.
  const seen: string[] = [];
  bus.subscribe('u', (msg) => seen.push(msg.messageId ?? ''), {
    deduplicateMessages: true,
  });
  bus.publish('u', 'a', { messageId: 'm1' });
  bus.publish('u', 'a', { messageId: 'm2' });
  bus.publish('u', 'a', { messageId: 'm3' });
  bus.publish('u', 'a', { messageId: 'm1' });
  await flush();
  assert.deepEqual(seen, ['m1', 'm2', 'm3', 'm1'], 'm1 was evicted by the default bound');
});

test('EB-502: an explicit field always wins over the bus default', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeduplicateMessages({ windowMs: 1_000, maxEntries: 2 });
  // Explicit window: 10000ms beats the default 1000ms.
  const wide: BusMessage[] = [];
  bus.subscribe('t', (msg) => wide.push(msg), {
    deduplicateMessages: { windowMs: 10_000 },
  });
  bus.publish('t', 'a', { messageId: 'm1' });
  clock.advance(1_500);
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(wide.length, 1, 'the explicit window governs, not the default');
  clock.advance(8_500);
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(wide.length, 2, 'the explicit window has elapsed');
  // Explicit bound: 100 beats the default 2 — m1 survives m2 and m3.
  const roomy: string[] = [];
  bus.subscribe('u', (msg) => roomy.push(msg.messageId ?? ''), {
    deduplicateMessages: { maxEntries: 100 },
  });
  bus.publish('u', 'a', { messageId: 'm1' });
  bus.publish('u', 'a', { messageId: 'm2' });
  bus.publish('u', 'a', { messageId: 'm3' });
  bus.publish('u', 'a', { messageId: 'm1' });
  await flush();
  assert.deepEqual(roomy, ['m1', 'm2', 'm3'], 'the explicit bound governs, not the default');
});

test('EB-502: clearing with null returns new opt-in subscribers to the built-in window and bound', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeduplicateMessages({ windowMs: 1_000, maxEntries: 2 });
  bus.setDefaultDeduplicateMessages(null);
  assert.equal(bus.getDefaultDeduplicateMessages(), undefined);
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg), { deduplicateMessages: true });
  // Built-in window: the publish-side idempotency window (60s) — a
  // duplicate 2s later is still suppressed.
  bus.publish('t', 'a', { messageId: 'm1' });
  clock.advance(2_000);
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 1, 'the built-in 60s window still suppresses');
  clock.advance(58_000);
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2, 'the built-in window has elapsed');
  const audit = bus.getSubscriptionAudit({ action: 'default_deduplicate_messages_revised' });
  assert.equal(audit.length, 2);
  assert.deepEqual(audit[1].detail, {
    previous: { windowMs: 1_000, maxEntries: 2 },
    deduplicateMessages: null,
  });
});

test('EB-502: the default never enables dedup for a subscriber that does not opt in', async () => {
  const bus = new EventBus();
  bus.setDefaultDeduplicateMessages({ windowMs: 1_000, maxEntries: 2 });
  const received: BusMessage[] = [];
  bus.subscribe('t', (msg) => received.push(msg));
  bus.publish('t', 'a', { messageId: 'm1' });
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(received.length, 2, 'no opt-in, no dedup');
  assert.equal(bus.getStats().dedupDropped, 0);
});

test('EB-502: revising the bus default never moves an already-subscribed window or bound', async () => {
  const clock = makeClock();
  const bus = new EventBus({ now: clock.now });
  bus.setDefaultDeduplicateMessages({ windowMs: 1_000, maxEntries: 2 });
  const early: BusMessage[] = [];
  const earlySub = bus.subscribe('t', (msg) => early.push(msg), {
    deduplicateMessages: true,
  });
  bus.setDefaultDeduplicateMessages({ windowMs: 10_000, maxEntries: 100 });
  // The early subscriber keeps the window it was baked with: at
  // 1500ms its 1000ms window has elapsed, so the duplicate delivers.
  bus.publish('t', 'a', { messageId: 'm1' });
  clock.advance(1_500);
  bus.publish('t', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(early.length, 2, 'the early subscriber keeps its baked window');
  // A subscriber joining after the revision takes the new default.
  const late: BusMessage[] = [];
  bus.subscribe('u', (msg) => late.push(msg), { deduplicateMessages: true });
  bus.publish('u', 'a', { messageId: 'm1' });
  clock.advance(1_500);
  bus.publish('u', 'a', { messageId: 'm1' });
  await flush();
  assert.equal(late.length, 1, 'the late subscriber takes the revised 10000ms window');
  // The per-subscriber revision still works on a default-baked
  // subscriber without touching the bus default.
  bus.setDeduplicateMessages(earlySub.id, { windowMs: 20_000 });
  assert.deepEqual(bus.getDefaultDeduplicateMessages(), {
    windowMs: 10_000,
    maxEntries: 100,
  });
});

test('EB-502: illegal values throw RangeError with the live default untouched and no audit', () => {
  const bus = new EventBus();
  bus.setDefaultDeduplicateMessages({ windowMs: 1_000, maxEntries: 2 });
  const bad = [
    0,
    -1,
    1.5,
    'x',
    [],
    true,
    { windowMs: 0 },
    { windowMs: -5 },
    { windowMs: Number.NaN },
    { windowMs: Number.POSITIVE_INFINITY },
    { windowMs: '1000' },
    { maxEntries: 0 },
    { maxEntries: -1 },
    { maxEntries: 2.5 },
    { maxEntries: '2' },
    { windowMs: 1_000, maxEntries: 0 },
    undefined,
  ] as unknown[];
  for (const value of bad) {
    assert.throws(
      () => bus.setDefaultDeduplicateMessages(value as never),
      RangeError,
      `opts=${String(value)}`,
    );
    assert.deepEqual(bus.getDefaultDeduplicateMessages(), {
      windowMs: 1_000,
      maxEntries: 2,
    });
  }
  assert.throws(
    () => bus.setDefaultDeduplicateMessages({ windowMs: 2_000 }, ''),
    RangeError,
  );
  assert.deepEqual(bus.getDefaultDeduplicateMessages(), { windowMs: 1_000, maxEntries: 2 });
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_deduplicate_messages_revised' }).length,
    1,
    'failed revisions must not audit',
  );
});

test('EB-502: re-affirming the default is a no-op with no audit, the getter and setter copy defensively, and the audit records previous/current exactly', () => {
  const bus = new EventBus();
  bus.setDefaultDeduplicateMessages(null);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_deduplicate_messages_revised' }).length,
    0,
    'clearing an already-unset default audits nothing',
  );
  bus.setDefaultDeduplicateMessages({});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'default_deduplicate_messages_revised' }).length,
    0,
    '{} installs no field coverage, equivalent to unset',
  );
  assert.equal(bus.getDefaultDeduplicateMessages(), undefined);
  const opts: DeduplicateMessagesDefaultOptions = { windowMs: 1_000, maxEntries: 2 };
  bus.setDefaultDeduplicateMessages(opts);
  opts.windowMs = 999;
  opts.maxEntries = 999;
  assert.deepEqual(
    bus.getDefaultDeduplicateMessages(),
    { windowMs: 1_000, maxEntries: 2 },
    'the setter stores a defensive copy',
  );
  bus.setDefaultDeduplicateMessages({ windowMs: 1_000, maxEntries: 2 });
  bus.setDefaultDeduplicateMessages({ maxEntries: 2, windowMs: 1_000 });
  const audit = bus.getSubscriptionAudit({ action: 'default_deduplicate_messages_revised' });
  assert.equal(audit.length, 1, 're-affirming the default audits nothing');
  assert.deepEqual(audit[0].detail, {
    previous: null,
    deduplicateMessages: { windowMs: 1_000, maxEntries: 2 },
  });
  const held = bus.getDefaultDeduplicateMessages();
  assert.ok(held !== undefined);
  held.windowMs = 999;
  held.maxEntries = 999;
  assert.deepEqual(
    bus.getDefaultDeduplicateMessages(),
    { windowMs: 1_000, maxEntries: 2 },
    'the getter returns a copy',
  );
  // A partial revision keeps the untouched field and audits both.
  bus.setDefaultDeduplicateMessages({ windowMs: 2_000 }, 'ops');
  assert.deepEqual(bus.getDefaultDeduplicateMessages(), { windowMs: 2_000 });
  const revised = bus.getSubscriptionAudit({ action: 'default_deduplicate_messages_revised' });
  assert.equal(revised.length, 2);
  assert.deepEqual(revised[1].detail, {
    previous: { windowMs: 1_000, maxEntries: 2 },
    deduplicateMessages: { windowMs: 2_000 },
  });
  assert.equal(revised[1].actor, 'ops');
  bus.setDefaultDeduplicateMessages(null, 'ops');
  const cleared = bus.getSubscriptionAudit({ action: 'default_deduplicate_messages_revised' });
  assert.equal(cleared.length, 3);
  assert.deepEqual(cleared[2].detail, {
    previous: { windowMs: 2_000 },
    deduplicateMessages: null,
  });
});

test('EB-502: the default never enters a config snapshot', () => {
  const bus = new EventBus();
  bus.setDefaultDeduplicateMessages({ windowMs: 1_000, maxEntries: 2 });
  const snapshot = bus.exportBusConfig();
  assert.ok(!('deduplicateMessages' in snapshot), 'no top-level deduplicateMessages key');
  assert.ok(
    !JSON.stringify(snapshot).includes('deduplicateMessages'),
    'the default appears nowhere in the exported snapshot',
  );
  assert.ok(
    !JSON.stringify(snapshot).includes('defaultDeduplicateMessages'),
    'the field name appears nowhere in the exported snapshot',
  );
});
