import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

function tempDir(prefix: string): string {
  return mkdtempSync(join(tmpdir(), prefix));
}

/** Per-topic retained-expiry count from `getStats().topics`. */
function retainedExpiredFor(bus: EventBus, topic: string): number | undefined {
  return bus
    .getStats()
    .topics.find((row) => row.topic === topic)?.retainedExpiredMessages;
}

// ---------------------------------------------------------------------------
// rule registration and validation
// ---------------------------------------------------------------------------

test('setTopicRetainedTtl rejects empty patterns and bad ttlMs, leaving rules untouched', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicRetainedTtl('', 100), RangeError);
  assert.throws(() => bus.setTopicRetainedTtl('t', Number.NaN), RangeError);
  assert.throws(() => bus.setTopicRetainedTtl('t', Number.POSITIVE_INFINITY), RangeError);
  assert.throws(() => bus.setTopicRetainedTtl('t', -1), RangeError);
  assert.equal(bus.getTopicRetainedTtl('t'), undefined);

  // A failed call never moves a live rule.
  bus.setTopicRetainedTtl('t', 100);
  assert.throws(() => bus.setTopicRetainedTtl('t', -5), RangeError);
  assert.equal(bus.getTopicRetainedTtl('t'), 100);
  assert.deepEqual(
    bus.getSubscriptionAudit().map((e) => e.action),
    ['retained_ttl_rule_set'],
  );
});

test('getTopicRetainedTtl resolves exact-first, earliest-pattern-first', () => {
  const first = new EventBus();
  first.setTopicRetainedTtl('a.*', 100);
  first.setTopicRetainedTtl('a.**', 200);
  first.setTopicRetainedTtl('a.b', 300);
  assert.equal(first.getTopicRetainedTtl('a.b'), 300);
  assert.equal(first.getTopicRetainedTtl('a.c'), 100);
  assert.equal(first.getTopicRetainedTtl('a.b.c'), 200);
  assert.equal(first.getTopicRetainedTtl('zzz'), undefined);

  const second = new EventBus();
  second.setTopicRetainedTtl('a.**', 200);
  second.setTopicRetainedTtl('a.*', 100);
  assert.equal(second.getTopicRetainedTtl('a.c'), 200);
});

test('retained-TTL rule matchers never inflate patternCacheSize', () => {
  const bus = new EventBus();
  bus.setTopicRetainedTtl('a.*', 100);
  bus.setTopicRetainedTtl('b.**', 200);
  bus.setTopicRetained('a.*', true);
  bus.publish('a.1', { x: 1 }, { retain: true });
  assert.equal(bus.getStats().patternCacheSize, 0);
});

// ---------------------------------------------------------------------------
// store-time stamping: min(message TTL, retained TTL)
// ---------------------------------------------------------------------------

test('a retained snapshot expires once its retained TTL passes (new subscriber receives nothing)', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('t', true);
  bus.setTopicRetainedTtl('t', 100);
  bus.publish('t', { v: 1 }, { retain: true }); // stamped to expire at 1100
  assert.equal(bus.getStats().retainedTopics.length, 1);
  assert.equal(bus.getStats().retainedExpiredMessages, 0);

  clock.nowMs = 1_100; // exactly at the deadline: expired, like a message TTL
  assert.equal(bus.getStats().retainedTopics.length, 0);
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 0);
  // The serve-time prune counted the expiry once, globally and per topic.
  assert.equal(bus.getStats().retainedExpiredMessages, 1);
  assert.equal(retainedExpiredFor(bus, 't'), 1);
  // A second subscriber cannot recount an entry that is already gone.
  bus.subscribe('t', () => {});
  assert.equal(bus.getStats().retainedExpiredMessages, 1);
});

test('ttlMs = 0 stores-then-immediately-expires (never served)', () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.setTopicRetainedTtl('t', 0);
  bus.publish('t', { v: 1 }, { retain: true });
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().retainedExpiredMessages, 1);
});

test('min semantics: a retained TTL shorter than the message TTL wins', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('t', true);
  bus.setTopicRetained('u', true);
  bus.setTopicTtl('t', 10_000);
  bus.setTopicTtl('u', 10_000);
  bus.setTopicRetainedTtl('t', 100);
  bus.publish('t', { v: 1 }, { retain: true }); // message TTL says 11000, retained says 1100
  bus.publish('u', { v: 2 }, { retain: true }); // control: no retained rule, lives to 11000
  clock.nowMs = 2_000;
  const seenT: BusMessage[] = [];
  const seenU: BusMessage[] = [];
  bus.subscribe('t', (m) => seenT.push(m));
  bus.subscribe('u', (m) => seenU.push(m));
  assert.equal(seenT.length, 0);
  assert.equal(seenU.length, 1);
  assert.deepEqual(seenU[0].payload, { v: 2 });
  assert.equal(bus.getStats().retainedExpiredMessages, 1);
});

test('min semantics: a retained TTL longer than the message TTL loses', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('t', true);
  bus.setTopicRetained('u', true);
  bus.setTopicTtl('t', 100);
  bus.setTopicRetainedTtl('t', 10_000);
  bus.setTopicRetainedTtl('u', 10_000);
  bus.publish('t', { v: 1 }, { retain: true }); // message TTL says 1100, retained says 11000
  bus.publish('u', { v: 2 }, { retain: true }); // control: no message TTL, lives to 11000
  clock.nowMs = 2_000;
  const seenT: BusMessage[] = [];
  const seenU: BusMessage[] = [];
  bus.subscribe('t', (m) => seenT.push(m));
  bus.subscribe('u', (m) => seenU.push(m));
  assert.equal(seenT.length, 0);
  assert.equal(seenU.length, 1);
  assert.equal(bus.getStats().retainedExpiredMessages, 1);
});

test('without a retained-TTL rule a snapshot never expires', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('t', true);
  bus.publish('t', { v: 1 }, { retain: true });
  clock.nowMs = 1_000_000_000;
  assert.equal(bus.getStats().retainedTopics.length, 1);
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 1);
  assert.equal(bus.getStats().retainedExpiredMessages, 0);
});

// ---------------------------------------------------------------------------
// rule lifecycle: clear, tighten, loosen
// ---------------------------------------------------------------------------

test('clearing the rule frees new snapshots but old ones keep their stamped deadline', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('sensor.*', true);
  bus.setTopicRetainedTtl('sensor.*', 100);
  bus.publish('sensor.a', { v: 1 }, { retain: true }); // stamped to expire at 1100
  assert.equal(bus.clearTopicRetainedTtl('sensor.*'), true);
  clock.nowMs = 2_000;
  const seenOld: BusMessage[] = [];
  bus.subscribe('sensor.a', (m) => seenOld.push(m));
  assert.equal(seenOld.length, 0);
  assert.equal(bus.getStats().retainedExpiredMessages, 1);

  // A snapshot stored after the clear carries no deadline at all.
  bus.publish('sensor.b', { v: 2 }, { retain: true });
  clock.nowMs = 1_000_000_000;
  const seenNew: BusMessage[] = [];
  bus.subscribe('sensor.b', (m) => seenNew.push(m));
  assert.equal(seenNew.length, 1);
  assert.deepEqual(seenNew[0].payload, { v: 2 });
  assert.equal(bus.getStats().retainedExpiredMessages, 1);
});

test('tightening the rule immediately stops serving an already-stored snapshot', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('t', true);
  bus.setTopicRetainedTtl('t', 10_000);
  bus.publish('t', { v: 1 }, { retain: true }); // stamped to expire at 11000
  bus.setTopicRetainedTtl('t', 100); // tighten: live bound is now 1100
  clock.nowMs = 1_200;
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().retainedTopics.length, 0);
  assert.equal(bus.getStats().retainedExpiredMessages, 1);
  assert.equal(retainedExpiredFor(bus, 't'), 1);
});

test('loosening the rule never revives an already-expired snapshot', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('t', true);
  bus.setTopicRetainedTtl('t', 100);
  bus.publish('t', { v: 1 }, { retain: true }); // stamped to expire at 1100
  clock.nowMs = 2_000;
  bus.setTopicRetainedTtl('t', 1_000_000); // loosen: live bound would be 1002000
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 0);
  assert.equal(bus.getStats().retainedTopics.length, 0);
  assert.equal(bus.getStats().retainedExpiredMessages, 1);
});

// ---------------------------------------------------------------------------
// durability: the stamped deadline survives a restart
// ---------------------------------------------------------------------------

test('restart recovery honors the stamped retained-TTL deadline', () => {
  const dir = tempDir('retained-ttl-recover-');
  const clock = controllableClock(1_000);
  {
    const bus = new EventBus({ durableLogDir: dir, now: clock.now });
    bus.setTopicRetained('t', true);
    bus.setTopicRetainedTtl('t', 100);
    bus.publish('t', { v: 1 }, { retain: true }); // stamped to expire at 1100
  }
  clock.nowMs = 2_000;
  const bus = new EventBus({ durableLogDir: dir, now: clock.now });
  assert.equal(bus.getStats().retainedTopics.length, 0);
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 0);
});

test('restart recovery still serves a snapshot inside its retained TTL', () => {
  const dir = tempDir('retained-ttl-recover-live-');
  const clock = controllableClock(1_000);
  {
    const bus = new EventBus({ durableLogDir: dir, now: clock.now });
    bus.setTopicRetained('t', true);
    bus.setTopicRetainedTtl('t', 10_000);
    bus.publish('t', { v: 1 }, { retain: true }); // stamped to expire at 11000
  }
  clock.nowMs = 2_000;
  const bus = new EventBus({ durableLogDir: dir, now: clock.now });
  assert.equal(bus.getStats().retainedTopics.length, 1);
  const seen: BusMessage[] = [];
  bus.subscribe('t', (m) => seen.push(m));
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].payload, { v: 1 });
});

// ---------------------------------------------------------------------------
// observability: audit, counters
// ---------------------------------------------------------------------------

test('retained-TTL rule changes are audited; clearing a missing rule is silent', () => {
  const bus = new EventBus();
  assert.equal(bus.clearTopicRetainedTtl('nope'), false);
  bus.setTopicRetainedTtl('a.*', 100);
  bus.setTopicRetainedTtl('a.*', 200);
  assert.equal(bus.clearTopicRetainedTtl('a.*'), true);
  assert.equal(bus.clearTopicRetainedTtl('a.*'), false);
  bus.setTopicRetainedTtl('b', 50);
  const events = bus.getSubscriptionAudit();
  assert.deepEqual(
    events.map((e) => e.action),
    [
      'retained_ttl_rule_set',
      'retained_ttl_rule_set',
      'retained_ttl_rule_cleared',
      'retained_ttl_rule_set',
    ],
  );
  assert.equal(events[0].pattern, 'a.*');
  assert.deepEqual(events[0].detail, { ttlMs: 100 });
  assert.equal(events[1].pattern, 'a.*');
  assert.deepEqual(events[1].detail, { ttlMs: 200 });
  assert.equal(events[2].pattern, 'a.*');
  assert.deepEqual(events[2].detail, { ttlMs: 200 });
  assert.equal(events[3].pattern, 'b');
  assert.deepEqual(events[3].detail, { ttlMs: 50 });
  // The failed validation in the first test audits nothing either:
  // only the four events above exist.
  assert.equal(events.length, 4);
});

test('retainedExpiredMessages counts one per pruned snapshot across topics', () => {
  const clock = controllableClock(1_000);
  const bus = new EventBus({ now: clock.now });
  bus.setTopicRetained('s.*', true);
  bus.setTopicRetainedTtl('s.*', 100);
  bus.publish('s.a', { v: 1 }, { retain: true });
  bus.publish('s.b', { v: 2 }, { retain: true });
  bus.publish('s.c', { v: 3 }, { retain: true });
  // A fresh publish replaces s.c with a live entry: only a and b expire.
  bus.clearTopicRetainedTtl('s.*');
  bus.publish('s.c', { v: 4 }, { retain: true });
  clock.nowMs = 2_000;
  const seen: BusMessage[] = [];
  bus.subscribe('s.*', (m) => seen.push(m));
  assert.deepEqual(
    seen.map((m) => m.topic),
    ['s.c'],
  );
  assert.equal(bus.getStats().retainedExpiredMessages, 2);
  assert.equal(retainedExpiredFor(bus, 's.a'), 1);
  assert.equal(retainedExpiredFor(bus, 's.b'), 1);
  assert.equal(retainedExpiredFor(bus, 's.c'), 0);
});
