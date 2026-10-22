import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  EventBus,
  BusClosedError,
  type AdmissionRejectionEvent,
  type TryPublishResult,
} from '../src/bus.ts';

/** Injectable clock so rate-limit refill timing is deterministic. */
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

/** Collects admission-rejection hook events in order. */
function makeSink() {
  const events: AdmissionRejectionEvent[] = [];
  return {
    events,
    hook: (event: AdmissionRejectionEvent) => {
      events.push(event);
    },
  };
}

/** Per-topic row from `getStats().topics`. */
function topicRow(bus: EventBus, topic: string) {
  const row = bus.getStats().topics.find((t) => t.topic === topic);
  assert.ok(row !== undefined, `expected a stats row for topic "${topic}"`);
  return row;
}

test('tryPublish admits on a healthy queue and behaves like publish', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
  });
  const result: TryPublishResult = bus.tryPublish('t', { a: 1 });
  assert.equal(result.admitted, true);
  assert.equal(result.reason, undefined);
  assert.equal(result.accepted, 1);
  assert.equal(result.seq, 1);
  await flush();
  assert.deepEqual(received, [{ a: 1 }]);
  const stats = bus.getStats();
  assert.equal(stats.backpressureRejected, 0);
  assert.equal(stats.rejectedMessages, 0);
});

test('tryPublish sheds when a matched subscriber reaches its high-water mark', () => {
  const clock = makeClock();
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.subscribe('t', () => {}, { queueSize: 10, highWaterMarkRatio: 0.5 });
  // Synchronous publishes pile backlog (the flush runs on a microtask):
  // 5 queued = exactly the high-water mark (10 * 0.5).
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', i), 1);
  const result = bus.tryPublish('t', 'hot');
  assert.deepEqual(result, { admitted: false, reason: 'backpressure', accepted: 0 });
  // The shed surfaces on the EB-39 channel with reason 'backpressure'.
  assert.equal(sink.events.length, 1);
  const event = sink.events[0];
  assert.equal(event.topic, 't');
  assert.equal(event.reason, 'backpressure');
  assert.ok(event.payloadBytes > 0);
  assert.equal(event.at, clock.now());
  // Dedicated counter + the shared rejectedMessages, per-topic and global.
  const row = topicRow(bus, 't');
  assert.equal(row.backpressureRejected, 1);
  assert.equal(row.rejectedMessages, 1);
  const stats = bus.getStats();
  assert.equal(stats.backpressureRejected, 1);
  assert.equal(stats.rejectedMessages, 1);
});

test('a backpressure shed has zero side effects: no seq, no durable write, no rate budget', async () => {
  const clock = makeClock(1_000_000);
  const dir = mkdtempSync(join(tmpdir(), 'trypublish-'));
  try {
    const sink = makeSink();
    const bus = new EventBus({
      now: clock.now,
      durableLogDir: dir,
      onAdmissionRejected: sink.hook,
    });
    bus.subscribe('t', () => {}, { queueSize: 10, highWaterMarkRatio: 0.5 });
    for (let i = 0; i < 5; i++) bus.publish('t', i);
    assert.equal(topicRow(bus, 't').lastSeq, 5);
    const entriesBefore = bus.getStats().durableLog?.entries ?? -1;
    assert.ok(entriesBefore >= 5, 'the 5 admitted publishes land in the durable log');
    // A single-token bucket installed AFTER the queue filled: the
    // rejected tryPublish must not touch it.
    bus.setTopicRateLimit('t', 10, { burst: 1 });
    const shed = bus.tryPublish('t', 'hot');
    assert.equal(shed.admitted, false);
    assert.equal(shed.reason, 'backpressure');
    // No sequence number consumed: the next admitted publish continues
    // the sequence gap-free.
    assert.equal(topicRow(bus, 't').lastSeq, 5);
    assert.equal(bus.getStats().sequenceGaps, 0);
    // Nothing written to the durable log by the shed.
    assert.equal(bus.getStats().durableLog?.entries, entriesBefore);
    // The token is still there: the real publish admits, the next sheds.
    assert.equal(bus.publish('t', 'after'), 1);
    assert.equal(bus.publish('t', 'overflow'), 0);
    await flush();
    // 'after' took seq 6; the rate-limit shed of 'overflow' consumed seq 7
    // (EB-19: rate-limit sheds consume a sequence number — unrelated to
    // tryPublish, whose own shed consumed nothing).
    assert.equal(topicRow(bus, 't').lastSeq, 7);
    assert.equal(bus.getStats().sequenceGaps, 0);
    assert.equal(topicRow(bus, 't').rateLimitedMessages, 1);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('plain publish still admits when the subscriber is over water', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe(
    't',
    (msg) => {
      received.push(msg.payload);
    },
    { queueSize: 10, highWaterMarkRatio: 0.5 },
  );
  for (let i = 0; i < 5; i++) bus.publish('t', i);
  // Over water, but publish never sheds — subscriber-side backpressure
  // keeps handling the overload downstream.
  assert.equal(bus.publish('t', 'late'), 1);
  const stats = bus.getStats();
  assert.equal(stats.backpressureRejected, 0);
  assert.equal(stats.rejectedMessages, 0);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4, 'late']);
});

test('a non-matching subscriber over water does not block tryPublish', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('other', () => {}, { queueSize: 4, highWaterMarkRatio: 0.5 });
  for (let i = 0; i < 2; i++) bus.publish('other', i); // at the mark (4 * 0.5)
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
  });
  const result = bus.tryPublish('t', 'x');
  assert.equal(result.admitted, true);
  assert.equal(result.accepted, 1);
  await flush();
  assert.deepEqual(received, ['x']);
  assert.equal(bus.getStats().backpressureRejected, 0);
});

test('tryPublish on a topic with no subscribers admits', () => {
  const bus = new EventBus();
  const result = bus.tryPublish('lonely', 'x');
  assert.deepEqual(result, { admitted: true, accepted: 0, seq: 1 });
  assert.equal(bus.getStats().backpressureRejected, 0);
});

test('a consumer-group member over water sheds (conservative, no assignment prediction)', () => {
  const bus = new EventBus();
  bus.subscribeToGroup('g', 't', () => {}, { queueSize: 4, highWaterMarkRatio: 0.5 });
  bus.subscribeToGroup('g', 't', () => {}, { queueSize: 4, highWaterMarkRatio: 0.5 });
  // Round-robin across the two members: 2 each, both at the mark.
  for (let i = 0; i < 4; i++) bus.publish('t', i);
  const result = bus.tryPublish('t', 'x');
  assert.equal(result.admitted, false);
  assert.equal(result.reason, 'backpressure');
  assert.equal(bus.getStats().backpressureRejected, 1);
});

test('tryPublish respects namespaces: the check runs on the concrete topic', async () => {
  const bus = new EventBus();
  const handle = bus.createNamespace('t1', { allowPublish: true, allowSubscribe: true });
  const received: unknown[] = [];
  handle.subscribe('x', (msg) => received.push(msg.payload), {
    queueSize: 10,
    highWaterMarkRatio: 0.5,
  });
  for (let i = 0; i < 5; i++) handle.publish('x', i);
  const result = bus.tryPublish('x', 'hot', { namespace: 't1' });
  assert.equal(result.admitted, false);
  assert.equal(result.reason, 'backpressure');
  // ...while the same topic outside the namespace is unaffected.
  const outside = bus.tryPublish('x', 'fine');
  assert.equal(outside.admitted, true);
  await flush();
  assert.deepEqual(received, [0, 1, 2, 3, 4]);
});

test('tryPublish on a closed bus throws like publish', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  await bus.close();
  assert.throws(() => bus.tryPublish('t', 'x'), BusClosedError);
  assert.throws(() => bus.publish('t', 'x'), BusClosedError);
});

test('a regular admission rejection after the pre-check still reports its own reason', () => {
  const clock = makeClock();
  const sink = makeSink();
  const bus = new EventBus({ now: clock.now, onAdmissionRejected: sink.hook });
  bus.subscribe('t', () => {});
  bus.setTopicRateLimit('t', 10, { burst: 1 });
  // Queue is healthy, so the pre-check passes; the rate-limit gate
  // refuses the second publish with its own reason.
  assert.equal(bus.tryPublish('t', 'one').admitted, true);
  const shed = bus.tryPublish('t', 'two');
  assert.equal(shed.admitted, false);
  assert.equal(shed.reason, 'rate-limit');
  assert.equal(sink.events.length, 1);
  assert.equal(sink.events[0].reason, 'rate-limit');
  assert.equal(bus.getStats().backpressureRejected, 0);
});
