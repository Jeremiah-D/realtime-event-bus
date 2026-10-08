import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so due-time tests are deterministic instead of racing wall time. Due
 * sweeps also run at the start of every flush, so advancing this clock
 * past a `deliverAt` and then publishing anything delivers deterministically.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-delayed-'));

test('a delayed message is not delivered before its due time, then fans out once due', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'late', { delayMs: 1000 });
  assert.match(id as string, /^delayed-\d+$/);
  assert.equal(bus.getStats().pendingDelayed, 1);
  await flush();
  assert.deepEqual(received, []); // t=1000, due at t=2000: nothing yet
  assert.equal(bus.getStats().pendingDelayed, 1);
  clock.nowMs = 2_000;
  bus.publish('t', 'poke'); // any publish triggers the flush, whose sweep fans out due entries
  await flush();
  // 'poke' was queued synchronously during publish(); 'late' was fanned
  // out by the sweep at the start of the flush microtask.
  assert.deepEqual(received, ['poke', 'late']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('delayMs: 0 and a past deliverAt fan out on the next flush, like publish', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'now', { delayMs: 0 });
  bus.publishDelayed('t', 'past', { deliverAt: 500 }); // already due at t=1000
  await flush();
  assert.deepEqual(received, ['now', 'past']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('multiple delayed messages fan out in due-time order, not schedule order', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'second', { delayMs: 2000 });
  bus.publishDelayed('t', 'first', { delayMs: 1000 });
  clock.nowMs = 3_000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke', 'first', 'second']);
});

test('cancelDelayed cancels a pending delivery; unknown ids are a no-op false', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 'never', { delayMs: 1000 });
  assert.equal(bus.cancelDelayed(id as string), true);
  assert.equal(bus.cancelDelayed(id as string), false); // already cancelled
  assert.equal(bus.cancelDelayed('delayed-9999'), false); // never scheduled
  assert.equal(bus.getStats().pendingDelayed, 0);
  clock.nowMs = 5_000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']); // the cancelled message never arrives
});

test('cancelling after fan-out is a no-op false', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  const id = bus.publishDelayed('t', 'x', { delayMs: 0 });
  await flush();
  assert.equal(bus.cancelDelayed(id as string), false);
});

test('deliverAt is computed from the injected clock: delayMs is relative, deliverAt absolute', () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.publishDelayed('t', 'a', { delayMs: 5000 });
  bus.publishDelayed('t', 'b', { deliverAt: 9000 });
  const log = DurableTopicLog.open({ dir });
  const recs = log.readSince('t', -1);
  assert.equal(recs.length, 2);
  assert.equal(recs[0].seq, 0); // schedule records carry no sequence number
  assert.equal(recs[0].deliverAt, 6000); // 1000 + 5000, bus clock
  assert.equal(recs[1].deliverAt, 9000);
  assert.match(recs[0].delayId as string, /^delayed-\d+$/);
  // The on-disk JSONL line carries deliverAt verbatim.
  const line = readFileSync(join(dir, `${encodeURIComponent('t')}.log`), 'utf8').split('\n')[0];
  assert.ok(line.includes('"deliverAt":6000'), `schedule line should carry deliverAt, got: ${line}`);
});

test('a delayed message that outlives its TTL is dropped as expired, not delivered', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // deadline t=1100 for messages scheduled at t=1000
  bus.publishDelayed('t', 'doomed', { delayMs: 1000 }); // due t=2000, past its deadline
  clock.nowMs = 2_000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.pendingDelayed, 0);
  // A delayed message still within its TTL delivers normally.
  bus.publishDelayed('t', 'ok', { delayMs: 50 }); // scheduled t=2000, deadline t=2100, due t=2050
  clock.nowMs = 2_050;
  bus.publish('t', 'poke2');
  await flush();
  assert.deepEqual(received, ['poke', 'poke2', 'ok']);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('schema validation fails fast at publishDelayed: no id, nothing scheduled', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicSchema('t', (payload) => typeof payload === 'string');
  const id = bus.publishDelayed('t', 123, { delayMs: 100 });
  assert.equal(id, undefined); // fail-fast: no delay id
  assert.equal(bus.getStats().pendingDelayed, 0);
  assert.equal(bus.getStats().rejectedMessages, 1); // counted like a publish rejection
  clock.nowMs = 5_000;
  bus.publish('t', 'poke');
  await flush();
  assert.deepEqual(received, ['poke']); // the rejected payload never arrives
});

test('a throwing schema validator propagates from publishDelayed', () => {
  const bus = new EventBus();
  bus.setTopicSchema('t', () => {
    throw new Error('boom');
  });
  assert.throws(() => bus.publishDelayed('t', 'x', { delayMs: 10 }), /boom/);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('publishDelayed validates its timing options', () => {
  const bus = new EventBus();
  assert.throws(() => bus.publishDelayed('t', 1, {}), RangeError); // neither
  assert.throws(() => bus.publishDelayed('t', 1, { delayMs: 1, deliverAt: 2 }), RangeError); // both
  assert.throws(() => bus.publishDelayed('t', 1, { delayMs: -1 }), RangeError);
  assert.throws(() => bus.publishDelayed('t', 1, { delayMs: NaN }), RangeError);
  assert.throws(() => bus.publishDelayed('t', 1, { delayMs: Infinity }), RangeError);
  assert.throws(() => bus.publishDelayed('t', 1, { deliverAt: NaN }), RangeError);
  assert.throws(() => bus.publishDelayed('t', 1, { deliverAt: Infinity }), RangeError);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('a scheduled delayed message consumes no sequence number until fan-out', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  bus.publish('t', 'a'); // seq 1
  bus.publishDelayed('t', 'b', { delayMs: 1000 }); // no seq yet
  bus.publish('t', 'c'); // seq 2
  assert.equal(bus.getStats().topics[0].lastSeq, 2);
  clock.nowMs = 2_000;
  bus.publish('t', 'poke'); // seq 3
  await flush();
  // 'b' fanned out last and took the next number: the schedule never
  // reserved one.
  assert.deepEqual(seqs, [1, 2, 3, 4]);
  assert.equal(bus.getStats().topics[0].lastSeq, 4);
});

test('rate-limit budget is burned at fan-out time, not at schedule time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  bus.publishDelayed('t', 'first', { delayMs: 1000 });
  bus.publishDelayed('t', 'second', { delayMs: 1000 });
  // The schedules burned nothing: an immediate publish still finds the
  // bucket full.
  bus.publish('t', 'live');
  clock.nowMs = 2_000; // one token refilled at 1/s
  bus.publish('other', 'poke');
  await flush();
  assert.ok(received.includes('live'));
  assert.ok(received.includes('first'));
  assert.ok(!received.includes('second')); // shed at fan-out: bucket empty
  const stats = bus.getStats();
  assert.equal(stats.rateLimitedMessages, 1);
  assert.equal(stats.pendingDelayed, 0);
});

test('a delayed message fans out via the wall-clock timer with no other activity (real clock)', async () => {
  const bus = new EventBus(); // real clock
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publishDelayed('t', 'tock', { delayMs: 30 });
  assert.equal(bus.getStats().pendingDelayed, 1);
  await sleep(200); // timer (30ms) + margin; the timer is unref'd
  await flush();
  assert.deepEqual(received, ['tock']);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('restart rebuilds the pending timer from the durable log', async () => {
  const dir = freshDir();
  const clock1 = controllableClock();
  const bus1 = new EventBus({ now: clock1.now, durableLogDir: dir });
  bus1.subscribe('t', () => {});
  const id1 = bus1.publishDelayed('t', 'survivor', { delayMs: 5000 });
  // Simulate a restart: a new bus over the same directory, clock still t=1000.
  const clock2 = controllableClock();
  const bus2 = new EventBus({ now: clock2.now, durableLogDir: dir });
  assert.equal(bus2.getStats().pendingDelayed, 1);
  const received: unknown[] = [];
  bus2.subscribe('t', (msg) => received.push(msg.payload));
  clock2.nowMs = 6_000;
  bus2.publish('t', 'poke');
  await flush();
  assert.ok(received.includes('survivor'));
  assert.equal(bus2.getStats().pendingDelayed, 0);
  // Recovered ids are not reused by new schedules.
  const id2 = bus2.publishDelayed('t', 'second', { delayMs: 1000 });
  assert.notEqual(id2, id1);
});

test('restart past the due time fans the message out immediately', async () => {
  const dir = freshDir();
  const bus1 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  bus1.publishDelayed('t', 'late', { delayMs: 1000 }); // due t=2000
  // Restart at t=5000: the message is already due.
  const bus2 = new EventBus({ now: () => 5000, durableLogDir: dir });
  assert.equal(bus2.getStats().pendingDelayed, 0);
  const topic = bus2.getStats().topics.find((s) => s.topic === 't');
  assert.equal(topic?.publishedMessages, 1); // fanned out during recovery
  assert.equal(topic?.lastSeq, 1);
  // The delivery record carries the schedule identity for audit.
  const log = DurableTopicLog.open({ dir });
  const delivered = log.readSince('t', 0);
  assert.equal(delivered.length, 1);
  assert.equal(delivered[0].deliverAt, 2000);
  assert.match(delivered[0].delayId as string, /^delayed-\d+$/);
  // A further restart does not re-deliver: the delivery record closes the schedule.
  const bus3 = new EventBus({ now: () => 6000, durableLogDir: dir });
  assert.equal(bus3.getStats().pendingDelayed, 0);
  assert.equal(bus3.getStats().topics.find((s) => s.topic === 't')?.publishedMessages, 1);
});

test('a cancelled schedule stays cancelled across restart', async () => {
  const dir = freshDir();
  const bus1 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  const id = bus1.publishDelayed('t', 'x', { delayMs: 5000 });
  assert.equal(bus1.cancelDelayed(id as string), true);
  const bus2 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  assert.equal(bus2.getStats().pendingDelayed, 0); // tombstone: not resurrected
});

test('a schedule that expired while the process was down is dropped once, not recounted', () => {
  const dir = freshDir();
  const bus1 = new EventBus({ now: controllableClock().now, durableLogDir: dir });
  bus1.setTopicTtl('t', 100); // deadline t=1100
  bus1.publishDelayed('t', 'x', { delayMs: 5000 }); // due t=6000
  // Restart at t=2000: past the TTL deadline, before the due time.
  const bus2 = new EventBus({ now: () => 2000, durableLogDir: dir });
  assert.equal(bus2.getStats().expiredMessages, 1);
  assert.equal(bus2.getStats().pendingDelayed, 0);
  // Another restart: the tombstone closes the schedule, no recount.
  const bus3 = new EventBus({ now: () => 3000, durableLogDir: dir });
  assert.equal(bus3.getStats().expiredMessages, 0);
  assert.equal(bus3.getStats().pendingDelayed, 0);
});

test('publishDelayed throws when the durable log is on and the payload cannot be persisted', () => {
  const bus = new EventBus({ now: controllableClock().now, durableLogDir: freshDir() });
  // BigInt has no JSON encoding: the schedule could not survive a restart.
  assert.throws(() => bus.publishDelayed('t', 10n, { delayMs: 100 }), /could not be persisted/);
  assert.equal(bus.getStats().pendingDelayed, 0);
});

test('without a durable log, an unserializable payload still schedules in memory', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const id = bus.publishDelayed('t', 10n, { delayMs: 100 });
  assert.match(id as string, /^delayed-\d+$/);
  clock.nowMs = 1_100;
  bus.publish('t', 'poke');
  await flush();
  assert.ok(received.includes(10n));
});

test('pendingDelayed starts at zero on a fresh bus', () => {
  assert.equal(new EventBus().getStats().pendingDelayed, 0);
});
