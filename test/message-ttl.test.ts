import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/**
 * A manually-advanced clock handed to the bus via `EventBusOptions.now`,
 * so expiry tests are deterministic instead of racing wall time.
 */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-msg-ttl-'));

test('a per-message TTL shorter than the topic rule expires the message at its own deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 10_000); // topic rule: deadline t=11000
  bus.publish('t', 'short', { ttlMs: 100 }); // per-message: deadline t=1100
  bus.publish('t', 'plain'); // topic rule: deadline t=11000
  clock.nowMs = 1_200; // past the per-message deadline, inside the topic rule's
  await flush();
  assert.deepEqual(received, ['plain']);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics[0].expiredMessages, 1);
  assert.equal(stats.topics[0].publishedMessages, 2); // publishing still counted
});

test('a per-message TTL longer than the topic rule keeps the message alive past the rule deadline', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100); // topic rule: deadline t=1100
  bus.publish('t', 'protected', { ttlMs: 10_000 }); // per-message: deadline t=11000
  clock.nowMs = 1_200; // past the topic rule's deadline
  await flush();
  assert.deepEqual(received, ['protected']);
  assert.equal(bus.getStats().expiredMessages, 0);
});

test('without ttlMs the topic TTL rule applies unchanged', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100);
  bus.publish('t', 'stale');
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('ttlMs: 0 expires the message immediately — admitted, never delivered', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'zero', { ttlMs: 0 });
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics[0].expiredMessages, 1);
  assert.equal(stats.topics[0].publishedMessages, 1); // admission still counted
});

test('invalid ttlMs values throw RangeError with zero side effects', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.subscribe('t', () => {});
  bus.setTopicRateLimit('t', 1, { burst: 1 }); // one token: a burn would show
  const badValues: unknown[] = [1.5, -1, NaN, Infinity, -Infinity, '100', null, {}, true];
  for (const bad of badValues) {
    assert.throws(
      () => bus.publish('t', 'x', { ttlMs: bad as number }),
      RangeError,
      `ttlMs=${String(bad)} should throw`,
    );
  }
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 0); // no sequence number consumed
  assert.equal(stats.topics.find((t) => t.topic === 't')?.lastSeq ?? 0, 0);
  assert.equal(stats.rejectedMessages, 0); // a throw is not a counted rejection
  // Nothing reached the durable log.
  const log = DurableTopicLog.open({ dir });
  assert.deepEqual(log.readSince('t', 0), []);
  // The rate-limit budget was not burned: a valid publish is still admitted.
  assert.equal(bus.publish('t', 'ok'), 1);
  await flush();
});

test('per-message TTL expiry is drain-time, not an admission rejection', async () => {
  const clock = controllableClock();
  const rejections: unknown[] = [];
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: (e) => rejections.push(e) });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'doomed', { ttlMs: 100 });
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, []);
  assert.deepEqual(rejections, []); // drain-time expiry fires no admission hook
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('the durable-log record carries the per-message deadline, overriding the topic rule', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.subscribe('t', () => {});
  bus.setTopicTtl('t', 10_000); // topic rule would stamp t=11000
  bus.publish('t', 'm', { ttlMs: 100 }); // per-message stamps t=1100
  await flush();
  const log = DurableTopicLog.open({ dir });
  const records = log.readSince('t', 0);
  assert.equal(records.length, 1);
  assert.equal(records[0].expiresAt, 1_100);
});

test('replay keeps the original per-message deadline — never extended', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  bus.setTopicTtl('t', 10_000); // topic rule would keep it alive
  bus.publish('t', 'per-message', { ttlMs: 100 }); // deadline t=1100
  await flush();
  clock.nowMs = 1_200; // past the per-message deadline, inside the topic rule's
  const resumed: unknown[] = [];
  bus.subscribe('t', (msg) => resumed.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(resumed, []); // not resurrected: the original deadline held
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('publishDelayed: per-message TTL counts from fan-out time, ignoring the topic rule', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: unknown[] = [];
  bus.subscribe('d', (msg) => received.push(msg.payload));
  bus.setTopicTtl('d', 100); // schedule-time deadline would be t=1100
  bus.publishDelayed('d', 'delayed-msg', { delayMs: 500, ttlMs: 1_000 }); // due t=1500
  clock.nowMs = 1_600; // past the topic rule's schedule-time deadline
  bus.publish('d', 'poke'); // trigger the flush whose sweep fans out due entries
  await flush();
  // Delivered: the topic rule was ignored for the schedule, and the
  // per-message deadline is fan-out(1600) + 1000 = t=2600.
  assert.deepEqual(received, ['poke', 'delayed-msg']);
  assert.equal(bus.getStats().expiredMessages, 0);
  // The delivery record carries the fan-out-time deadline, not a
  // schedule-time one.
  const log = DurableTopicLog.open({ dir });
  const delivered = log.readSince('d', 0).filter((r) => r.delayId !== undefined && r.seq >= 1);
  assert.equal(delivered.length, 1);
  assert.equal(delivered[0].expiresAt, 2_600);
});

test('publishDelayed: an invalid ttlMs fails fast with zero side effects', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('d', () => {});
  assert.throws(() => bus.publishDelayed('d', 'x', { delayMs: 100, ttlMs: -1 }), RangeError);
  assert.throws(() => bus.publishDelayed('d', 'x', { delayMs: 100, ttlMs: 1.5 }), RangeError);
  assert.equal(bus.getStats().pendingDelayed, 0); // no schedule, nothing in the heap
  assert.equal(bus.getStats().totalPublished, 0);
});

test('restart rebuilds a per-message-TTL delayed schedule with the TTL intact', async () => {
  const dir = freshDir();
  const clock1 = controllableClock();
  const bus1 = new EventBus({ now: clock1.now, durableLogDir: dir });
  bus1.subscribe('t', () => {});
  bus1.setTopicTtl('t', 100); // schedule-time deadline would be t=1100
  bus1.publishDelayed('t', 'survivor', { delayMs: 5_000, ttlMs: 2_000 }); // due t=6000
  // The schedule record carries the TTL duration, not a deadline.
  const log = DurableTopicLog.open({ dir });
  const schedules = log.readSince('t', -1).filter((r) => r.delayId !== undefined && r.seq === 0);
  assert.equal(schedules.length, 1);
  assert.equal(schedules[0].ttlMs, 2_000);
  assert.equal(schedules[0].expiresAt, undefined);
  // Restart at the due time: the schedule was not dropped as expired
  // (no schedule-time deadline), fans out immediately, and the deadline
  // is fan-out(6000) + 2000 = t=8000.
  const clock2 = controllableClock(6_000);
  const bus2 = new EventBus({ now: clock2.now, durableLogDir: dir });
  assert.equal(bus2.getStats().pendingDelayed, 0); // fanned out during recovery
  const delivered = log.readSince('t', 0).filter((r) => r.delayId !== undefined && r.seq >= 1);
  assert.equal(delivered.length, 1);
  assert.equal(delivered[0].expiresAt, 8_000);
  await flush();
});

test('publishBatch: per-entry ttlMs overrides the topic rule independently', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 10_000);
  const accepted = bus.publishBatch([
    { topic: 't', payload: 'short', ttlMs: 100 }, // deadline t=1100
    { topic: 't', payload: 'plain' }, // topic rule: deadline t=11000
  ]);
  assert.equal(accepted, 2);
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, ['plain']);
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('publishBatch: an entry with an invalid ttlMs throws with zero side effects', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  assert.throws(
    () =>
      bus.publishBatch([
        { topic: 't', payload: 'ok' },
        { topic: 't', payload: 'bad', ttlMs: NaN },
      ]),
    RangeError,
  );
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 0);
  assert.equal(stats.topics.find((t) => t.topic === 't')?.lastSeq ?? 0, 0);
});

test('publishAtomic: an entry with an invalid ttlMs rejects the whole batch with zero side effects', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {});
  assert.throws(
    () =>
      bus.publishAtomic([
        { topic: 't', payload: 'ok' },
        { topic: 't', payload: 'bad', ttlMs: -1 },
      ]),
    RangeError,
  );
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 0);
  assert.equal(stats.topics.find((t) => t.topic === 't')?.lastSeq ?? 0, 0);
  assert.equal(stats.rejectedMessages, 0); // a throw is not a counted rejection
});

test('publishAtomic: per-entry ttlMs stamps independent deadlines at commit', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 10_000); // topic rule: deadline t=11000
  const res = bus.publishAtomic([
    { topic: 't', payload: 'short', ttlMs: 100 }, // deadline t=1100
    { topic: 't', payload: 'plain' }, // topic rule: deadline t=11000
  ]);
  assert.deepEqual(res, { published: 2 });
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, ['plain']); // 'short' expired at drain
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics[0].expiredMessages, 1);
});

test('per-message TTL composes with keyed ordering: an expired keyed message never wedges the key stream', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.publish('t', 'first', { key: 'k', ttlMs: 100 }); // deadline t=1100
  bus.publish('t', 'second', { key: 'k' }); // no TTL
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, ['second']); // 'first' expired at drain; the key stream advanced past it
  assert.equal(bus.getStats().expiredMessages, 1);
});

test('per-message TTL rides publishIdempotent like any PublishOptions field', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 10_000);
  const res = bus.publishIdempotent('t', 'idempotent', { messageId: 'm1', ttlMs: 100 });
  assert.equal(res.duplicate, false);
  clock.nowMs = 1_200;
  await flush();
  assert.deepEqual(received, []); // expired at the per-message deadline
  assert.equal(bus.getStats().expiredMessages, 1);
  assert.throws(() => bus.publishIdempotent('t', 'x', { ttlMs: -1 }), RangeError);
});
