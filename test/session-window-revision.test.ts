import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type SessionAggregateResult } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A bus with a manually advanced clock: no wall-clock timer can fire a gap close. */
function clockedBus(startMs = 1_000_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return { bus: new EventBus({ now: clock.now }), clock };
}

/** A session-windowed subscription collecting aggregates. */
function trackedBus(opts: {
  gapMs: number;
  aggregate?: 'count' | 'sum';
  field?: string;
  maxSessionsPerKey?: number;
}) {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: {
      gapMs: opts.gapMs,
      aggregate: opts.aggregate ?? 'count',
      ...(opts.field !== undefined ? { field: opts.field } : {}),
      ...(opts.maxSessionsPerKey !== undefined ? { maxSessionsPerKey: opts.maxSessionsPerKey } : {}),
    },
  });
  return { bus, clock, received, sub };
}

test('setSessionWindow rejects unknown, non-session-windowed, and undefined revisions fail-closed', () => {
  const { bus } = clockedBus();
  const plain = bus.subscribe('t', () => {});
  assert.throws(() => bus.setSessionWindow('no-such-sub', { gapMs: 10 }), RangeError);
  assert.throws(() => bus.setSessionWindow(plain.id, { gapMs: 10 }), RangeError);
  const { sub } = trackedBus({ gapMs: 100 });
  assert.throws(() => bus.setSessionWindow(sub.id, undefined as never), RangeError);
  assert.throws(() => bus.setSessionWindow(sub.id, 'x' as never), RangeError);
  assert.throws(() => bus.setSessionWindow(sub.id, [] as never), RangeError);
  assert.throws(() => bus.setSessionWindow('', { gapMs: 10 }), RangeError);
  assert.equal(
    bus.getSubscriptionAudit({ action: 'session_window_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  plain.unsubscribe();
  sub.unsubscribe();
});

test('setSessionWindow rejects aggregate and field changes, and illegal merged values, leaving the live configuration untouched', async () => {
  const { bus, clock, received, sub } = trackedBus({
    gapMs: 100,
    aggregate: 'sum',
    field: 'v',
    maxSessionsPerKey: 10,
  });
  bus.publish('t', { v: 5 }, { key: 'k' });
  await flush();
  for (const bad of [
    () => bus.setSessionWindow(sub.id, { aggregate: 'count' }),
    () => bus.setSessionWindow(sub.id, { field: 'other' }),
    () => bus.setSessionWindow(sub.id, { field: 'v.x' }),
    () => bus.setSessionWindow(sub.id, { gapMs: 0 }),
    () => bus.setSessionWindow(sub.id, { gapMs: -5 }),
    () => bus.setSessionWindow(sub.id, { gapMs: Number.NaN }),
    () => bus.setSessionWindow(sub.id, { gapMs: Infinity }),
    () => bus.setSessionWindow(sub.id, { maxSessionsPerKey: 0 }),
    () => bus.setSessionWindow(sub.id, { maxSessionsPerKey: 1.5 }),
    () => bus.setSessionWindow(sub.id, { field: 'a.__proto__.b' }),
  ]) {
    assert.throws(bad, RangeError);
  }
  assert.equal(
    bus.getSubscriptionAudit({ action: 'session_window_revised' }).length,
    0,
    'failed revisions must not audit',
  );
  // The live configuration is exactly the subscribe-time one: a gap
  // of 100 still holds the session open at +60, and closes it at +100.
  clock.nowMs += 60;
  bus.publish('t', { v: 7 }, { key: 'k' });
  await flush();
  assert.equal(received.length, 0, 'old gap must still hold the session open at +60');
  clock.nowMs += 100;
  bus.publish('t', { v: 1 }, { key: 'k' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 2);
  assert.equal(received[0].value, 12);
  sub.unsubscribe();
});

test('setSessionWindow restating the current aggregate and field with a new gap succeeds', () => {
  const { bus, sub } = trackedBus({ gapMs: 100, aggregate: 'sum', field: 'v' });
  bus.setSessionWindow(sub.id, { gapMs: 50, aggregate: 'sum', field: 'v' });
  const audit = bus.getSubscriptionAudit({ action: 'session_window_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    sessionWindow: { gapMs: 50, aggregate: 'sum', field: 'v', maxSessionsPerKey: 1024 },
    oldSessionWindow: { gapMs: 100, aggregate: 'sum', field: 'v', maxSessionsPerKey: 1024 },
  });
  sub.unsubscribe();
});

test('a tightened gap closes a quiet session on the next arrival; a loosened gap keeps it open', async () => {
  // Tighten: old gap 100, new gap 50 — an arrival at +60 closes.
  const tight = trackedBus({ gapMs: 100 });
  tight.bus.publish('t', 'a', { key: 'k' });
  tight.bus.publish('t', 'b', { key: 'k' });
  tight.bus.setSessionWindow(tight.sub.id, { gapMs: 50 });
  tight.clock.nowMs += 60;
  tight.bus.publish('t', 'c', { key: 'k' });
  await flush();
  assert.equal(tight.received.length, 1);
  assert.equal(tight.received[0].count, 2);
  tight.sub.unsubscribe();

  // Loosen: old gap 50, new gap 200 — an arrival at +60 does not close.
  const loose = trackedBus({ gapMs: 50 });
  loose.bus.publish('t', 'a', { key: 'k' });
  loose.bus.publish('t', 'b', { key: 'k' });
  loose.bus.setSessionWindow(loose.sub.id, { gapMs: 200 });
  loose.clock.nowMs += 60;
  loose.bus.publish('t', 'c', { key: 'k' });
  await flush();
  assert.equal(loose.received.length, 0, 'loosened gap must keep the session open at +60');
  loose.clock.nowMs += 200;
  loose.bus.publish('t', 'd', { key: 'k' });
  await flush();
  assert.equal(loose.received.length, 1);
  assert.equal(loose.received[0].count, 3);
  loose.sub.unsubscribe();
});

test('a shrunk session bound evicts nothing at revision time and governs only the next session opening', async () => {
  const { bus, received, sub } = trackedBus({ gapMs: 10_000, maxSessionsPerKey: 2 });
  bus.publish('t', 'a', { key: 'first' });
  bus.publish('t', 'b', { key: 'second' });
  bus.setSessionWindow(sub.id, { maxSessionsPerKey: 1 });
  await flush();
  assert.equal(received.length, 0, 'shrinking the bound must not evict an open session');
  // The next opening evicts the least-recently active session ('first').
  bus.publish('t', 'c', { key: 'third' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 1, 'the evicted session still delivers its aggregate');
  sub.unsubscribe();
});

test('a widened session bound admits a further session without eviction', async () => {
  const { bus, received, sub } = trackedBus({ gapMs: 10_000, maxSessionsPerKey: 1 });
  bus.publish('t', 'a', { key: 'first' });
  bus.setSessionWindow(sub.id, { maxSessionsPerKey: 2 });
  bus.publish('t', 'b', { key: 'second' });
  await flush();
  assert.equal(received.length, 0, 'widening must not evict, and the second session must open');
  sub.unsubscribe();
});

test('a revision identical to the current parameters is a no-op with no audit', () => {
  const { bus, sub } = trackedBus({ gapMs: 100, maxSessionsPerKey: 4 });
  bus.setSessionWindow(sub.id, { gapMs: 100, maxSessionsPerKey: 4 });
  bus.setSessionWindow(sub.id, {});
  assert.equal(
    bus.getSubscriptionAudit({ action: 'session_window_revised' }).length,
    0,
    'identical revisions must not audit',
  );
  sub.unsubscribe();
});

test('null clears session windowing, discards open sessions, and a later revision is refused', async () => {
  const { bus, clock, received, sub } = trackedBus({ gapMs: 100 });
  const raw: unknown[] = [];
  // A second, plain subscriber on the same topic observes raw traffic
  // after the clear; the cleared subscriber itself receives raw too.
  bus.publish('t', 'a', { key: 'k' });
  bus.setSessionWindow(sub.id, null);
  const audit = bus.getSubscriptionAudit({ action: 'session_window_revised' });
  assert.equal(audit.length, 1);
  assert.deepEqual(audit[0].detail, {
    sessionWindow: null,
    oldSessionWindow: { gapMs: 100, aggregate: 'count', maxSessionsPerKey: 1024 },
  });
  assert.throws(() => bus.setSessionWindow(sub.id, { gapMs: 50 }), RangeError);
  // No aggregate from the discarded session ever arrives, and later
  // publishes travel the raw path: the handler sees the payload itself.
  clock.nowMs += 200;
  bus.publish('t', 'raw-payload', { key: 'k' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0] as unknown, 'raw-payload');
  assert.equal(bus.getStats().aggregatedSessionWindows, 0);
  void raw;
  sub.unsubscribe();
});
