import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type SessionAggregateResult } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** A bus with a manually advanced clock: no wall-clock timer can fire a gap close. */
function clockedBus(startMs = 1_000_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return { bus: new EventBus({ now: clock.now }), clock };
}

test('session aggregates a burst into one message: sum, per key', async () => {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.subscribe('prices', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 100, aggregate: 'sum', field: 'price' },
  });
  for (let i = 1; i <= 5; i++) bus.publish('prices', { price: i * 10 }, { key: 'btc' });
  await flush();
  // Raw messages never reach the handler: the burst is still in the open session.
  assert.equal(received.length, 0);
  // The activity gap expires: the next message closes the old session first.
  clock.nowMs += 100;
  bus.publish('prices', { price: 1 }, { key: 'btc' });
  await flush();
  assert.equal(received.length, 1);
  const agg = received[0];
  assert.equal(agg.count, 5);
  assert.equal(agg.value, 150);
  assert.equal(agg.sessionStart, 1_000_000);
  assert.equal(agg.sessionEnd, 1_000_000);
  assert.equal(bus.getStats().aggregatedSessionWindows, 1);
});

test('a gap-exceeded arrival closes the old session and opens a new one', async () => {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 50, aggregate: 'count' },
  });
  bus.publish('t', 'a', { key: 'k' });
  bus.publish('t', 'b', { key: 'k' });
  clock.nowMs += 60;
  bus.publish('t', 'c', { key: 'k' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 2);
  assert.equal(received[0].sessionStart, 1_000_000);
  assert.equal(received[0].sessionEnd, 1_000_000);
  // The new session stays open until its own gap expires.
  clock.nowMs += 60;
  bus.publish('t', 'd', { key: 'k' });
  await flush();
  assert.equal(received.length, 2);
  assert.equal(received[1].count, 1);
  assert.equal(received[1].sessionStart, 1_000_060);
  assert.equal(bus.getStats().aggregatedSessionWindows, 2);
});

test('sessions are isolated per key: a hot key never closes a quiet key', async () => {
  const { bus, clock } = clockedBus();
  const received: Array<{ key: string; agg: SessionAggregateResult }> = [];
  bus.subscribe(
    't',
    (m) => received.push({ key: m.key ?? '', agg: m.payload as SessionAggregateResult }),
    { sessionWindow: { gapMs: 50, aggregate: 'count' } },
  );
  bus.publish('t', 'q1', { key: 'quiet' });
  // The hot key churns well past the quiet key's gap — the quiet session stays open.
  for (let i = 0; i < 5; i++) {
    clock.nowMs += 10;
    bus.publish('t', `h${i}`, { key: 'hot' });
  }
  // Let the quiet key's gap expire by message arrival.
  clock.nowMs += 60;
  bus.publish('t', 'q2', { key: 'quiet' });
  // Wait out the wall-clock sweep timer so the hot session's gap expiry
  // is observed deterministically, whatever the timer interleaving was.
  await sleep(150);
  await flush();
  // Exactly two aggregates, one per key: sessions never crossed keys —
  // the hot key's churn never folded into (or closed) the quiet session.
  // (The aggregate envelope carries no key — the spec result shape is
  // exactly {sessionStart, sessionEnd, count, value} — so the two
  // sessions are told apart by their distinct counts here.)
  assert.equal(received.length, 2);
  const counts = received.map((r) => r.agg.count).sort((a, b) => a - b);
  assert.deepEqual(counts, [1, 5]);
});

test('unkeyed messages share the empty-key session', async () => {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 40, aggregate: 'count' },
  });
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  clock.nowMs += 40;
  bus.publish('t', 'c');
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 2);
});

test('the sweep timer closes an idle session without new traffic', async () => {
  const bus = new EventBus();
  const received: SessionAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 30, aggregate: 'sum', field: 'v' },
  });
  bus.publish('t', { v: 1 }, { key: 'k' });
  bus.publish('t', { v: 2 }, { key: 'k' });
  await flush();
  assert.equal(received.length, 0);
  await sleep(120);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 2);
  assert.equal(received[0].value, 3);
  assert.equal(bus.getStats().aggregatedSessionWindows, 1);
});

test('every aggregate kind folds correctly', async () => {
  const cases: Array<{
    aggregate: 'count' | 'sum' | 'avg' | 'min' | 'max' | 'last' | 'concat';
    field?: string;
    payloads: unknown[];
    expected: unknown;
  }> = [
    { aggregate: 'count', payloads: ['a', 'b', 'c'], expected: 3 },
    { aggregate: 'sum', field: 'v', payloads: [{ v: 1 }, { v: 2 }, { v: 3 }], expected: 6 },
    { aggregate: 'avg', field: 'v', payloads: [{ v: 2 }, { v: 4 }], expected: 3 },
    { aggregate: 'min', field: 'v', payloads: [{ v: 5 }, { v: 1 }, { v: 9 }], expected: 1 },
    { aggregate: 'max', field: 'v', payloads: [{ v: 5 }, { v: 1 }, { v: 9 }], expected: 9 },
    { aggregate: 'last', field: 'v', payloads: [{ v: 'x' }, { v: 'y' }], expected: 'y' },
    { aggregate: 'concat', payloads: ['a', 'b', 'c'], expected: 'abc' },
    { aggregate: 'concat', field: 'v', payloads: [{ v: 1 }, { v: 2 }], expected: '12' },
  ];
  for (const c of cases) {
    const { bus, clock } = clockedBus();
    const received: SessionAggregateResult[] = [];
    bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
      sessionWindow: {
        gapMs: 20,
        aggregate: c.aggregate,
        ...(c.field ? { field: c.field } : {}),
      },
    });
    for (const p of c.payloads) bus.publish('t', p, { key: 'k' });
    clock.nowMs += 20;
    bus.publish('t', 'closer', { key: 'k' });
    // The closer opens a new session: close it too so the count is exact.
    clock.nowMs += 20;
    bus.publish('t', 'sentinel', { key: 'other' });
    await flush();
    const aggs = received.filter((a) => a.count === c.payloads.length);
    assert.equal(aggs.length, 1, `${c.aggregate}: exactly one full aggregate`);
    assert.deepEqual(aggs[0].value, c.expected, `${c.aggregate}: value`);
  }
});

test('maxSessionsPerKey evicts the least-recently active session — and still delivers it', async () => {
  const { bus } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 10_000, aggregate: 'count', maxSessionsPerKey: 2 },
  });
  bus.publish('t', 'a1', { key: 'a' });
  bus.publish('t', 'b1', { key: 'b' });
  // Touch 'a' again: 'b' becomes the least-recently active session.
  bus.publish('t', 'a2', { key: 'a' });
  // A third key evicts 'b' — its aggregate is delivered, not dropped.
  bus.publish('t', 'c1', { key: 'c' });
  await flush();
  assert.equal(received.length, 1);
  // 'b' folded exactly one message while 'a' folded two: count 1 proves
  // the evicted session was the least-recently active one ('b'), and
  // eviction delivered its aggregate instead of losing it.
  assert.equal(received[0].count, 1);
  assert.equal(bus.getStats().aggregatedSessionWindows, 1);
});

test('an already-expired delayed message never enters a session', async () => {
  const bus = new EventBus();
  const received: SessionAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 10_000, aggregate: 'count' },
  });
  bus.setTopicTtl('t', 30);
  bus.publish('t', 'live', { key: 'k' });
  // The topic-TTL deadline is stamped at schedule time (now + 30ms), so
  // when this fans out at ~80ms it is already expired: counted expired,
  // never folded into the open session.
  bus.publishDelayed('t', 'stale', { delayMs: 80, key: 'k' });
  await sleep(250);
  await flush();
  assert.equal(received.length, 0);
  const topicStats = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topicStats?.expiredMessages, 1);
});

test('filter applies before sessioning: rejected messages never fold', async () => {
  const { bus, clock } = clockedBus();
  const received: SessionAggregateResult[] = [];
  bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 50, aggregate: 'count' },
    filter: (p) => (p as { keep?: boolean }).keep === true,
  });
  bus.publish('t', { keep: true }, { key: 'k' });
  bus.publish('t', { keep: false }, { key: 'k' });
  clock.nowMs += 50;
  bus.publish('t', { keep: true }, { key: 'k' });
  await flush();
  assert.equal(received.length, 1);
  assert.equal(received[0].count, 1);
});

test('subscribe validation rejects bad sessionWindow values', () => {
  const bus = new EventBus();
  const h = () => {};
  assert.throws(() => bus.subscribe('t', h, { sessionWindow: { gapMs: 0, aggregate: 'count' } }), RangeError);
  assert.throws(() => bus.subscribe('t', h, { sessionWindow: { gapMs: -5, aggregate: 'count' } }), RangeError);
  assert.throws(() => bus.subscribe('t', h, { sessionWindow: { gapMs: NaN, aggregate: 'count' } }), RangeError);
  assert.throws(
    () =>
      bus.subscribe('t', h, {
        sessionWindow: { gapMs: 10, aggregate: 'bogus' as 'count' },
      }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', h, { sessionWindow: { gapMs: 10, aggregate: 'count', maxSessionsPerKey: 0 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', h, { sessionWindow: { gapMs: 10, aggregate: 'count', maxSessionsPerKey: 1.5 } }),
    RangeError,
  );
  assert.throws(
    () => bus.subscribe('t', h, { sessionWindow: { gapMs: 10, aggregate: 'count', field: 'a.__proto__.b' } }),
    RangeError,
  );
  assert.throws(() => bus.subscribe('t', h, { sessionWindow: 'x' as never }), TypeError);
});

test('sessionWindow cannot be combined with window', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.subscribe('t', () => {}, {
        window: { windowMs: 10, aggregate: 'count' },
        sessionWindow: { gapMs: 10, aggregate: 'count' },
      }),
    /sessionWindow cannot be combined with window/,
  );
});

test('unsubscribe clears the sweep timer: no phantom aggregates', async () => {
  const bus = new EventBus();
  const received: SessionAggregateResult[] = [];
  const sub = bus.subscribe('t', (m) => received.push(m.payload as SessionAggregateResult), {
    sessionWindow: { gapMs: 30, aggregate: 'count' },
  });
  bus.publish('t', 'a', { key: 'k' });
  sub.unsubscribe();
  await sleep(100);
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().aggregatedSessionWindows, 0);
});

test('the aggregate envelope carries the last message topic and seq', async () => {
  const { bus, clock } = clockedBus();
  const seen: Array<{ topic: string; seq: number; agg: SessionAggregateResult }> = [];
  bus.subscribe('events', (m) => seen.push({ topic: m.topic, seq: m.seq, agg: m.payload as SessionAggregateResult }), {
    sessionWindow: { gapMs: 50, aggregate: 'last', field: 'v' },
  });
  const s1 = bus.publish('events', { v: 'first' }, { key: 'k' });
  bus.publish('events', { v: 'second' }, { key: 'k' });
  clock.nowMs += 50;
  bus.publish('events', { v: 'closer' }, { key: 'k' });
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 'events');
  // publish() returns the fan-out width, not the seq: the aggregate
  // carries the last folded message's publish-order seq (2 here).
  assert.ok(s1 > 0, 'sanity: publish returns fan-out width');
  assert.equal(seen[0].seq, 2);
  assert.equal(seen[0].agg.value, 'second');
});
