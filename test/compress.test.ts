import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Highly compressible: JSON-serializes to tens of kilobytes. */
const bigPayload = () => ({ text: 'lorem ipsum dolor sit amet '.repeat(2000) });

test('payloads at or below the threshold pass through uncompressed', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicCompression('t', { thresholdBytes: 1024 });
  assert.equal(bus.publish('t', 'small'), 1);
  assert.equal(bus.publish('t', { a: 1 }), 1);
  await flush();
  assert.deepEqual(received, ['small', { a: 1 }]);
  const stats = bus.getStats();
  assert.equal(stats.compressedMessages, 0);
  assert.equal(stats.compressedBytesBefore, 0);
  assert.equal(stats.compressedBytesAfter, 0);
  assert.equal(stats.compressionRatio, 0);
  assert.equal(stats.meanCompressionMs, 0);
  const topic = stats.topics.find((t) => t.topic === 't')!;
  assert.equal(topic.compressedMessages, 0);
  assert.equal(topic.compressionRatio, 0);
});

test('payloads above the threshold are compressed and transparently inflated', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  const payload = bigPayload();
  assert.equal(bus.publish('t', payload), 1);
  await flush();
  // The subscriber transparently receives the ORIGINAL payload.
  assert.deepEqual(received, [payload]);
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 't')!;
  assert.equal(topic.compressedMessages, 1);
  assert.ok(topic.compressedBytesBefore > 1000, `before=${topic.compressedBytesBefore}`);
  assert.ok(topic.compressedBytesAfter < topic.compressedBytesBefore, 'deflate shrank the payload');
  assert.ok(topic.compressionRatio < 1 && topic.compressionRatio > 0);
  assert.ok(topic.meanCompressionMs >= 0);
  assert.equal(stats.compressedMessages, 1);
  assert.equal(stats.compressedBytesBefore, topic.compressedBytesBefore);
  assert.equal(stats.compressedBytesAfter, topic.compressedBytesAfter);
  assert.equal(stats.compressionRatio, topic.compressionRatio);
  assert.equal(stats.totalPublished, 1);
});

test('one inflation serves every subscriber; compression is counted once per publish', async () => {
  const bus = new EventBus();
  const receivedA: unknown[] = [];
  const receivedB: unknown[] = [];
  bus.subscribe('t', (msg) => receivedA.push(msg.payload));
  bus.subscribe('t', (msg) => receivedB.push(msg.payload));
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  const payload = bigPayload();
  bus.publish('t', payload);
  await flush();
  assert.deepEqual(receivedA, [payload]);
  assert.deepEqual(receivedB, [payload]);
  // Counted once at publish time, not once per subscriber.
  assert.equal(bus.getStats().compressedMessages, 1);
});

test('compression consumes no sequence number and preserves order', async () => {
  const bus = new EventBus();
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  bus.publish('t', 'small');
  bus.publish('t', bigPayload());
  bus.publish('t', 'small again');
  await flush();
  assert.deepEqual(seqs, [1, 2, 3]);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('clearTopicCompression restores passthrough for new publishes', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  const payload = bigPayload();
  bus.publish('t', payload);
  assert.equal(bus.clearTopicCompression('t'), true);
  assert.equal(bus.clearTopicCompression('t'), false);
  bus.publish('t', payload);
  await flush();
  assert.deepEqual(received, [payload, payload]);
  assert.equal(bus.getStats().compressedMessages, 1);
});

test('exact-topic rule wins over patterns', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('a.*', (msg) => received.push(msg.payload));
  bus.setTopicCompression('a.*', { thresholdBytes: 100 });
  // Exact rule with an unreachable threshold: effectively "do not compress".
  bus.setTopicCompression('a.b', { thresholdBytes: Number.MAX_SAFE_INTEGER });
  const payload = bigPayload();
  bus.publish('a.b', payload);
  bus.publish('a.c', payload);
  await flush();
  assert.deepEqual(received, [payload, payload]);
  const stats = bus.getStats();
  assert.equal(stats.topics.find((t) => t.topic === 'a.b')!.compressedMessages, 0);
  assert.equal(stats.topics.find((t) => t.topic === 'a.c')!.compressedMessages, 1);
});

test('earliest-registered pattern wins between patterns', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('a.x', (msg) => received.push(msg.payload));
  bus.setTopicCompression('a.*', { thresholdBytes: Number.MAX_SAFE_INTEGER });
  bus.setTopicCompression('**', { thresholdBytes: 100 });
  const payload = bigPayload();
  bus.publish('a.x', payload);
  await flush();
  assert.deepEqual(received, [payload]);
  assert.equal(bus.getStats().compressedMessages, 0);
});

test('an encoding that does not shrink the payload is never adopted', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Level 0 stores without compressing: deflate output is always slightly
  // larger than the input (block overhead), so the bus must pass the
  // message through uncompressed instead of adopting a larger encoding.
  bus.setTopicCompression('t', { thresholdBytes: 100, level: 0 });
  const payload = bigPayload();
  bus.publish('t', payload);
  await flush();
  assert.deepEqual(received, [payload]);
  assert.equal(bus.getStats().compressedMessages, 0);
});

test('unserializable payloads pass through untouched', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicCompression('t', { thresholdBytes: 1 });
  const fn = () => {};
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  bus.publish('t', undefined);
  bus.publish('t', fn);
  bus.publish('t', circular);
  await flush();
  assert.equal(received[0], undefined);
  assert.equal(received[1], fn);
  assert.equal(received[2], circular);
  assert.equal(bus.getStats().compressedMessages, 0);
});

test('schema validation sees the raw payload, not the envelope', async () => {
  const bus = new EventBus();
  const seenByValidator: unknown[] = [];
  const received: unknown[] = [];
  bus.subscribe('orders', (msg) => received.push(msg.payload));
  bus.setTopicSchema('orders', (payload) => {
    seenByValidator.push(payload);
    return (
      typeof payload === 'object' && payload !== null && typeof (payload as { id?: unknown }).id === 'number'
    );
  });
  bus.setTopicCompression('orders', { thresholdBytes: 100 });
  const payload = { id: 7, text: 'x'.repeat(5000) };
  assert.equal(bus.publish('orders', payload), 1);
  assert.equal(bus.publish('orders', { id: 'bad', text: 'y'.repeat(5000) }), 0);
  await flush();
  // The validator saw the raw application payload — never the envelope.
  assert.deepEqual(seenByValidator, [payload, { id: 'bad', text: 'y'.repeat(5000) }]);
  assert.ok(!('__busCompressed' in (seenByValidator[0] as object)));
  assert.deepEqual(received, [payload]);
  assert.equal(bus.getStats().compressedMessages, 1);
  assert.equal(bus.getStats().rejectedMessages, 1);
});

test('rate-limit shed happens before compression: shed messages pay no deflate', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicRateLimit('t', 1, { burst: 1 });
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  const payload = bigPayload();
  assert.equal(bus.publish('t', payload), 1);
  assert.equal(bus.publish('t', payload), 0); // shed: bucket empty
  await flush();
  assert.deepEqual(received, [payload]);
  const stats = bus.getStats();
  assert.equal(stats.rateLimitedMessages, 1);
  // Only the admitted message was compressed.
  assert.equal(stats.compressedMessages, 1);
});

test('durable log stores compressed bytes; resume inflates transparently', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-compress-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  const payload = bigPayload();
  bus.publish('t', payload);
  await flush();
  // The log holds the envelope, not the raw payload text.
  const files = readdirSync(dir);
  assert.deepEqual(files, ['t.log']);
  const line = readFileSync(join(dir, 't.log'), 'utf8').trim();
  assert.ok(line.includes('"__busCompressed":"deflate"'));
  assert.ok(!line.includes('lorem ipsum dolor sit amet lorem'));

  // A restarted bus (no compression rule configured) replays and inflates.
  const bus2 = new EventBus({ durableLogDir: dir });
  const received: unknown[] = [];
  bus2.subscribe('t', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(received, [payload]);
});

test('ACK redelivery of a compressed message delivers the original payload', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribeReliable(
    't',
    (delivery) => {
      received.push(delivery.msg.payload);
      if (received.length === 1) delivery.nack();
      else delivery.ack();
    },
    { ackTimeoutMs: 60_000 },
  );
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  const payload = bigPayload();
  bus.publish('t', payload);
  await flush();
  await flush(); // the nack requeue drains on the chained flush, like test/ack.test.ts
  assert.equal(received.length, 2);
  assert.deepEqual(received[0], payload);
  assert.deepEqual(received[1], payload);
  // One compression at publish; redelivery reuses the inflated message.
  assert.equal(bus.getStats().compressedMessages, 1);
});

test('TTL deadline is preserved for compressed messages', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicTtl('t', 100);
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  bus.publish('t', bigPayload()); // published at t=1000, deadline t=1100
  clock.nowMs = 1_200; // past the deadline when the flush drains
  await flush();
  assert.deepEqual(received, []);
  const stats = bus.getStats();
  assert.equal(stats.expiredMessages, 1);
  assert.equal(stats.topics[0].expiredMessages, 1);
  // The message was compressed at publish, then expired before delivery.
  assert.equal(stats.compressedMessages, 1);
});

test('a user payload shaped like the envelope is never mistaken for compressed', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Even with a compression rule active on the topic, a payload the bus did
  // not compress itself must pass through untouched.
  bus.setTopicCompression('t', { thresholdBytes: Number.MAX_SAFE_INTEGER });
  const colliding = { __busCompressed: 'deflate', data: 'aGVsbG8=' };
  bus.publish('t', colliding);
  await flush();
  assert.deepEqual(received, [colliding]);
  assert.equal(bus.getStats().compressedMessages, 0);
});

test('compression option validation', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicCompression('', { thresholdBytes: 100 }), RangeError);
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: 0 }), RangeError);
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: -5 }), RangeError);
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: NaN }), RangeError);
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: Infinity }), RangeError);
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: 100, level: -1 }), RangeError);
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: 100, level: 10 }), RangeError);
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: 100, level: 1.5 }), RangeError);
  // Level 0 and the default are accepted.
  bus.setTopicCompression('t', { thresholdBytes: 100, level: 0 });
  bus.setTopicCompression('u', { thresholdBytes: 100 });
  assert.equal(bus.clearTopicCompression('missing'), false);
});
