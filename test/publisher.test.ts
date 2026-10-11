import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  type AdmissionRejectionEvent,
  type BusMessage,
} from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-publisher-'));

function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  return { events, hook: (event: AdmissionRejectionEvent) => void events.push(event) };
}

test('publisherId validation: bad values throw RangeError from every publish entry point', () => {
  const bus = new EventBus();
  const bad: unknown[] = ['', 'a'.repeat(65), 'has space', 'semi;colon', 'uni\u00e9', 123, {}];
  for (const publisherId of bad) {
    assert.throws(
      () => bus.publish('t', 'x', { publisherId: publisherId as string }),
      RangeError,
      `publish accepted ${JSON.stringify(publisherId)}`,
    );
    assert.throws(
      () => bus.publishIdempotent('t', 'x', { messageId: 'm', publisherId: publisherId as string }),
      RangeError,
    );
    assert.throws(
      () =>
        bus.publishBatch([{ topic: 't', payload: 'x', publisherId: publisherId as string }]),
      RangeError,
    );
    assert.throws(
      () =>
        bus.publishAtomic([{ topic: 't', payload: 'x', publisherId: publisherId as string }]),
      RangeError,
    );
    assert.throws(
      () =>
        bus.publishDelayed('t', 'x', {
          delayMs: 10,
          publisherId: publisherId as string,
        }),
      RangeError,
    );
  }
  // The rejected batch claims no dedup slot: nothing was counted.
  assert.equal(bus.getStats().totalPublished, 0);
});

test('publisherId validation: the full alphabet is accepted at the boundaries', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('t', (m) => received.push(m));
  for (const publisherId of ['a', 'Z', '0', '_', '-', 'svc-1', 'A_b-9'.repeat(1), 'x'.repeat(64)]) {
    assert.equal(bus.publish('t', 'x', { publisherId }), 1, `rejected ${publisherId}`);
  }
  await flush();
  assert.deepEqual(
    received.map((m) => m.publisherId),
    ['a', 'Z', '0', '_', '-', 'svc-1', 'A_b-9', 'x'.repeat(64)],
  );
});

test('per-publisher publish counting: admitted publishes aggregate, anonymous do not', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 'a', { publisherId: 'alpha' });
  bus.publish('t', 'b', { publisherId: 'alpha' });
  bus.publish('t', 'c', { publisherId: 'beta' });
  bus.publish('t', 'd'); // anonymous: total only, no per-publisher entry
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 4);
  assert.deepEqual(stats.publishers, [
    { publisher: 'alpha', publishedMessages: 2 },
    { publisher: 'beta', publishedMessages: 1 },
  ]);
});

test('publisher ranking: most-published first, ties break on the identity', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  for (const p of ['zed', 'zed', 'zed', 'amy', 'amy', 'amy', 'bob']) {
    bus.publish('t', 'x', { publisherId: p });
  }
  assert.deepEqual(
    bus.getStats().publishers.map((p) => p.publisher),
    ['amy', 'zed', 'bob'],
  );
});

test('schema-rejected publishes carry the publisher on the rejection event but are not counted', () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  bus.setTopicSchema('t', (p) => typeof p === 'number');
  bus.publish('t', 'not-a-number', { publisherId: 'svc-a' });
  bus.publish('t', 'also-bad'); // anonymous rejection: no publisher key at all
  assert.equal(events.length, 2);
  assert.equal(events[0].reason, 'schema');
  assert.equal(events[0].publisher, 'svc-a');
  assert.ok(!('publisher' in events[1]), 'anonymous rejection must not carry a publisher key');
  const stats = bus.getStats();
  assert.equal(stats.rejectedMessages, 2);
  assert.deepEqual(stats.publishers, []);
});

test('rate-limit shed: the rejection event carries the publisher, the shed publish still counts as admitted', () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  bus.publish('t', 'first', { publisherId: 'svc-b' });
  bus.publish('t', 'shed', { publisherId: 'svc-b' });
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'rate-limit');
  assert.equal(events[0].publisher, 'svc-b');
  // A shed publish cleared schema (admission) — publishedMessages counts
  // it, so the per-publisher total counts it too (same counting line).
  assert.deepEqual(bus.getStats().publishers, [{ publisher: 'svc-b', publishedMessages: 2 }]);
});

test('publishIdempotent: duplicate suppression carries the publisher on the rejection event', () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  bus.subscribe('t', () => {});
  assert.deepEqual(bus.publishIdempotent('t', 'x', { messageId: 'm1', publisherId: 'svc-c' }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishIdempotent('t', 'x', { messageId: 'm1', publisherId: 'svc-c' }), {
    duplicate: true,
    accepted: 0,
  });
  assert.equal(events.length, 1);
  assert.equal(events[0].reason, 'duplicate');
  assert.equal(events[0].publisher, 'svc-c');
  // The suppressed duplicate is not a publish: the publisher total counts
  // only the admitted one.
  assert.deepEqual(bus.getStats().publishers, [{ publisher: 'svc-c', publishedMessages: 1 }]);
});

test('publishBatch and publishAtomic attribute each entry to its own publisher', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publishBatch([
    { topic: 't', payload: 'a', publisherId: 'batch-a' },
    { topic: 't', payload: 'b', publisherId: 'batch-b' },
    { topic: 't', payload: 'c' },
  ]);
  const res = bus.publishAtomic([
    { topic: 't', payload: 'd', publisherId: 'atomic-a' },
    { topic: 't', payload: 'e', publisherId: 'atomic-a' },
  ]);
  assert.deepEqual(res, { published: 2 });
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 5);
  const byPublisher = Object.fromEntries(stats.publishers.map((p) => [p.publisher, p.publishedMessages]));
  assert.deepEqual(byPublisher, { 'batch-a': 1, 'batch-b': 1, 'atomic-a': 2 });
});

test('publishAtomic rejection carries the failing entry publisher on the hook', () => {
  const { events, hook } = makeSink();
  const bus = new EventBus({ onAdmissionRejected: hook });
  bus.setTopicSchema('t', (p) => typeof p === 'number');
  const res = bus.publishAtomic([
    { topic: 't', payload: 1, publisherId: 'good' },
    { topic: 't', payload: 'bad', publisherId: 'bad-producer' },
  ]);
  assert.equal(res.published, 0);
  assert.equal(res.rejected?.reason, 'schema');
  assert.equal(events.length, 1);
  assert.equal(events[0].publisher, 'bad-producer');
  assert.deepEqual(bus.getStats().publishers, []);
});

test('durable log: publisherId rides the record and replay restores it onto the envelope', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus1.publish('jobs.email', { to: 'a' }, { publisherId: 'mailer' });
  bus1.publish('jobs.email', { to: 'b' }); // anonymous: no field on the record
  const log = DurableTopicLog.open({ dir });
  const records = log.readSince('jobs.email', 0);
  assert.equal(records.length, 2);
  assert.equal(records[0].publisherId, 'mailer');
  assert.ok(!('publisherId' in records[1]), 'anonymous publishes write no publisher field');
  assert.equal(records[1].publisherId, undefined);
  // Replay preserves the attribution on the rehydrated envelope.
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: BusMessage[] = [];
  bus2.subscribe('jobs.*', (m) => received.push(m), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(
    received.map((m) => m.publisherId),
    ['mailer', undefined],
  );
});

test('durable log: a malformed publisherId line is corrupt and skipped', () => {
  const dir = freshDir();
  const file = join(dir, `${encodeURIComponent('t')}.log`);
  writeFileSync(
    file,
    [
      JSON.stringify({ v: 1, seq: 1, topic: 't', at: 1000, payload: 'ok', publisherId: 'good' }),
      JSON.stringify({ v: 1, seq: 2, topic: 't', at: 1001, payload: 'bad', publisherId: 123 }),
      '',
    ].join('\n'),
    'utf8',
  );
  const log = DurableTopicLog.open({ dir });
  const records = log.readSince('t', 0);
  assert.equal(records.length, 1);
  assert.equal(records[0].publisherId, 'good');
  assert.ok(log.stats().corruptLines >= 1);
});

test('publishDelayed: the due fan-out counts under the schedule publisher, across a restart', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus1.publishDelayed('t', 'late', { delayMs: 1000, publisherId: 'scheduler' });
  assert.deepEqual(bus1.getStats().publishers, []);
  // The schedule record persists the attribution (seq-0 records are
  // visible to readSince with a -1 bound, the same way recoverDelayed
  // reads them).
  const log = DurableTopicLog.open({ dir });
  const scheduleRecords = log.readSince('t', -1);
  assert.equal(scheduleRecords.length, 1);
  assert.equal(scheduleRecords[0].publisherId, 'scheduler');
  const raw = readFileSync(join(dir, `${encodeURIComponent('t')}.log`), 'utf8');
  assert.ok(raw.includes('"publisherId":"scheduler"'));
  // Restart before the due time: the rebuilt timer keeps the attribution.
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus2.subscribe('t', () => {});
  clock.nowMs = 2_000;
  bus2.publish('t', 'poke');
  await flush();
  assert.deepEqual(bus2.getStats().publishers, [
    { publisher: 'scheduler', publishedMessages: 1 },
  ]);
  // The anonymous poke is not attributed anywhere.
  assert.equal(bus2.getStats().publishers.length, 1);
});

test('publishDelayed restart: the rebuilt timer keeps the messageId too (fold regression)', async () => {
  const clock = controllableClock();
  const dir = freshDir();
  const bus1 = new EventBus({ now: clock.now, durableLogDir: dir });
  bus1.publishDelayed('t', 'late', { delayMs: 1000, messageId: 'mid-1' });
  const bus2 = new EventBus({ now: clock.now, durableLogDir: dir });
  const received: BusMessage[] = [];
  bus2.subscribe('t', (m) => received.push(m));
  clock.nowMs = 2_000;
  bus2.publish('t', 'poke');
  await flush();
  assert.deepEqual(
    received.map((m) => m.messageId),
    [undefined, 'mid-1'],
  );
});

test('topic route forward: the destination admission keeps the source publisher', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('src', 'dst');
  bus.subscribe('dst', () => {});
  bus.publish('src', 'x', { publisherId: 'router' });
  await flush();
  // One admitted publish on src, one on dst — both under the same publisher.
  assert.deepEqual(bus.getStats().publishers, [{ publisher: 'router', publishedMessages: 2 }]);
});

test('cross-bus forward: the destination bus attributes the forwarded publish', async () => {
  const src = new EventBus();
  const dst = new EventBus();
  src.forward(dst, 'a');
  dst.subscribe('a', () => {});
  src.publish('a', 'x', { publisherId: 'federated' });
  await flush();
  await flush();
  assert.deepEqual(dst.getStats().publishers, [{ publisher: 'federated', publishedMessages: 1 }]);
});

test('renderPrometheus exposes per-publisher counters, absent when no publisher published', () => {
  const anonymous = new EventBus();
  anonymous.subscribe('t', () => {});
  anonymous.publish('t', 'x');
  assert.ok(
    !renderPrometheus(anonymous.getStats()).includes('eventbus_publisher_published_messages_total'),
    'a publisher-free bus must render byte-identical exposition to before',
  );
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.publish('t', 'a', { publisherId: 'alpha' });
  bus.publish('t', 'b', { publisherId: 'alpha' });
  bus.publish('t', 'c', { publisherId: 'beta' });
  const exposition = renderPrometheus(bus.getStats());
  assert.ok(
    exposition.includes('eventbus_publisher_published_messages_total{publisher="alpha"} 2'),
  );
  assert.ok(
    exposition.includes('eventbus_publisher_published_messages_total{publisher="beta"} 1'),
  );
});
