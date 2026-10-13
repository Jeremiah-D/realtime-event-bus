import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import type { BusMessage } from '../src/bus.ts';

/**
 * Binary payload passthrough (EB-92): `PublishOptions.binary` carries
 * raw bytes through admission, compression, signing, the durable log
 * (v2 lines) and delivery without JSON serialization.
 */

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const flushAll = async () => {
  await flush();
  await flush();
  await flush();
};
/** Per-topic stats row by concrete topic name (`getStats().topics` is an array). */
const topicStats = (bus: EventBus, topic: string) =>
  bus.getStats().topics.find((t) => t.topic === topic);
const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-binary-'));
const bytesOf = (...xs: number[]) => new Uint8Array(xs);

function lastLogLine(dir: string, topic: string): Record<string, unknown> {
  const file = join(dir, `${encodeURIComponent(topic)}.log`);
  const lines = readFileSync(file, 'utf8').trim().split('\n');
  return JSON.parse(lines[lines.length - 1]) as Record<string, unknown>;
}

test('binary publish delivers the same bytes, zero-copy', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  const bytes = bytesOf(1, 2, 3, 250, 251, 0, 255);
  const accepted = bus.publish('bin.a', bytes, { binary: true });
  await flushAll();
  assert.equal(accepted, 1);
  assert.equal(received.length, 1);
  assert.ok(received[0].payload instanceof Uint8Array);
  assert.deepEqual([...(received[0].payload as Uint8Array)], [1, 2, 3, 250, 251, 0, 255]);
  // Zero-copy: the subscriber sees the published buffer itself.
  assert.equal(received[0].payload, bytes);
});

test('binary: true with a non-bytes payload throws TypeError before mutating', () => {
  const bus = new EventBus();
  assert.throws(() => bus.publish('bin.a', { x: 1 }, { binary: true }), TypeError);
  assert.throws(() => bus.publish('bin.a', 'nope', { binary: true }), TypeError);
  assert.throws(() => bus.publish('bin.a', null, { binary: true }), TypeError);
  assert.equal(bus.getStats().totalPublished, 0);
});

test('ArrayBuffer payload is wrapped in a zero-copy view', async () => {
  const bus = new EventBus();
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  const ab = new Uint8Array([7, 8, 9]).buffer;
  bus.publish('bin.a', ab, { binary: true });
  await flushAll();
  assert.ok(received[0].payload instanceof Uint8Array);
  assert.deepEqual([...(received[0].payload as Uint8Array)], [7, 8, 9]);
});

test('max-message-bytes measures wire bytes for binary payloads', async () => {
  const bus = new EventBus();
  bus.setTopicMaxMessageBytes('bin.**', 4);
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  assert.equal(bus.publish('bin.a', bytesOf(1, 2, 3, 4), { binary: true }), 1);
  assert.equal(bus.publish('bin.a', bytesOf(1, 2, 3, 4, 5), { binary: true }), 0);
  await flushAll();
  assert.equal(received.length, 1);
  assert.equal(topicStats(bus, 'bin.a')?.rejectedMessages, 1);
});

test('schema validators receive the raw bytes for binary topics', async () => {
  const bus = new EventBus();
  const seen: unknown[] = [];
  bus.setTopicSchema('bin.**', (payload) => {
    seen.push(payload);
    return payload instanceof Uint8Array && payload.length > 0 && payload[0] === 0x42;
  });
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  assert.equal(bus.publish('bin.a', bytesOf(0x42, 0x01), { binary: true }), 1);
  assert.equal(bus.publish('bin.a', bytesOf(0x00), { binary: true }), 0);
  await flushAll();
  assert.equal(received.length, 1);
  assert.ok(seen[0] instanceof Uint8Array);
});

test('compression applies to wire bytes and inflates back to bytes', async () => {
  const bus = new EventBus();
  // Highly redundant bytes: deflate shrinks them well under any threshold.
  const bytes = new Uint8Array(4096).fill(0xab);
  bus.setTopicCompression('bin.**', { thresholdBytes: 64 });
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  bus.publish('bin.a', bytes, { binary: true });
  await flushAll();
  assert.equal(received.length, 1);
  const payload = received[0].payload;
  assert.ok(payload instanceof Uint8Array);
  assert.equal((payload as Uint8Array).length, 4096);
  assert.deepEqual([...(payload as Uint8Array).slice(0, 4)], [0xab, 0xab, 0xab, 0xab]);
  const stats = topicStats(bus, 'bin.a')!;
  assert.equal(stats.compressedMessages, 1);
  assert.ok(stats.compressedBytesBefore >= 4096);
});

test('small binary payloads skip compression like small JSON ones', async () => {
  const bus = new EventBus();
  bus.setTopicCompression('bin.**', { thresholdBytes: 1024 });
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  const bytes = bytesOf(1, 2, 3);
  bus.publish('bin.a', bytes, { binary: true });
  await flushAll();
  assert.equal(received[0].payload, bytes); // untouched, zero-copy
  assert.equal(topicStats(bus, 'bin.a')?.compressedMessages ?? 0, 0);
});

test('HMAC signing covers the wire bytes for binary messages', async () => {
  const bus = new EventBus();
  bus.setTopicSigningKey('bin.**', {
    keys: [{ kid: 'k1', secret: Buffer.from('test-signing-secret-0000000000000000') }],
    graceMs: 1000,
  });
  const received: BusMessage[] = [];
  const dropped: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  }, { verifySignature: true });
  bus.subscribe('bin.**', (msg) => {
    dropped.push(msg);
  });
  const bytes = bytesOf(10, 20, 30, 40);
  bus.publish('bin.a', bytes, { binary: true });
  await flushAll();
  assert.equal(received.length, 1);
  assert.equal(received[0].payload, bytes);
  assert.ok(received[0].signature !== undefined);
  assert.equal(received[0].signature?.kid, 'k1');
  // The non-verifying subscriber also got it (signature is advisory there).
  assert.equal(dropped.length, 1);
});

test('unsigned binary messages fail closed on verifySignature subscribers', async () => {
  const bus = new EventBus();
  // No signing rule: the binary message travels unsigned, so a
  // verifySignature subscriber must drop it (fail closed), exactly like
  // an unsigned JSON message.
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  }, { verifySignature: true });
  bus.publish('bin.a', bytesOf(1, 2, 3), { binary: true });
  await flushAll();
  assert.equal(received.length, 0);
  assert.equal(topicStats(bus, 'bin.a')?.signatureFailures, 1);
});

test('durable log stores binary messages as v2 lines and replays bytes', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const bytes = bytesOf(0xde, 0xad, 0xbe, 0xef, 0x00);
  bus.publish('bin.a', bytes, { binary: true });
  bus.publish('bin.a', { json: true });
  await flushAll();

  const line = lastLogLine(dir, 'bin.a');
  assert.equal(line['v'], 1); // last line is the JSON publish
  const allLines = readFileSync(join(dir, `${encodeURIComponent('bin.a')}.log`), 'utf8')
    .trim()
    .split('\n')
    .map((l) => JSON.parse(l) as Record<string, unknown>);
  const v2 = allLines.find((l) => l['v'] === 2);
  assert.ok(v2 !== undefined, 'expected a v2 line for the binary publish');
  assert.equal(typeof v2['payload_b64'], 'string');
  assert.equal(v2['payload'], undefined);
  assert.equal(
    Buffer.from(v2['payload_b64'] as string, 'base64').toString('hex'),
    'deadbeef00',
  );

  // A new bus over the same directory replays the bytes, not JSON.
  const bus2 = new EventBus({ durableLogDir: dir });
  const replayed: BusMessage[] = [];
  bus2.subscribe('bin.**', (msg) => {
    replayed.push(msg);
  }, { resumeFromSeq: 0 });
  await flushAll();
  assert.equal(replayed.length, 2);
  assert.ok(replayed[0].payload instanceof Uint8Array);
  assert.deepEqual([...(replayed[0].payload as Uint8Array)], [0xde, 0xad, 0xbe, 0xef, 0x00]);
  assert.deepEqual(replayed[1].payload, { json: true });
});

test('binary+compressed messages survive the durable log as bytes', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('bin.**', { thresholdBytes: 64 });
  const bytes = new Uint8Array(2048).fill(0x11);
  bus.publish('bin.a', bytes, { binary: true });
  await flushAll();

  const bus2 = new EventBus({ durableLogDir: dir });
  // The compression rule must exist on the new bus for dictionary-less
  // replay (deflate without a dictionary needs no registry).
  bus2.setTopicCompression('bin.**', { thresholdBytes: 64 });
  const replayed: BusMessage[] = [];
  bus2.subscribe('bin.**', (msg) => {
    replayed.push(msg);
  }, { resumeFromSeq: 0 });
  await flushAll();
  assert.equal(replayed.length, 1);
  const payload = replayed[0].payload;
  assert.ok(payload instanceof Uint8Array);
  assert.equal((payload as Uint8Array).length, 2048);
  assert.equal((payload as Uint8Array)[100], 0x11);
});

test('publishIdempotent supports binary payloads', async () => {
  const bus = new EventBus({ idempotencyWindowMs: 60_000 });
  const received: BusMessage[] = [];
  bus.subscribe('pay.**', (msg) => {
    received.push(msg);
  });
  const bytes = bytesOf(9, 9, 9);
  const first = bus.publishIdempotent('pay.evt', bytes, { binary: true, messageId: 'm1' });
  const second = bus.publishIdempotent('pay.evt', bytes, { binary: true, messageId: 'm1' });
  await flushAll();
  assert.deepEqual(first, { duplicate: false, accepted: 1 });
  assert.deepEqual(second, { duplicate: true, accepted: 0 });
  assert.equal(received.length, 1);
  assert.equal(received[0].payload, bytes);
});

test('binary messages survive a pause-buffer round-trip', async () => {
  const bus = new EventBus({ durableLogDir: freshDir() });
  bus.pauseTopic('bin.**', { mode: 'queue' });
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  const bytes = bytesOf(5, 6, 7, 8);
  bus.publish('bin.a', bytes, { binary: true });
  await flushAll();
  assert.equal(received.length, 0); // buffered, not delivered
  bus.resumeTopic('bin.**');
  await flushAll();
  assert.equal(received.length, 1);
  assert.ok(received[0].payload instanceof Uint8Array);
  assert.deepEqual([...(received[0].payload as Uint8Array)], [5, 6, 7, 8]);
});

test('binary messages stay binary across a topic route', async () => {
  const bus = new EventBus();
  bus.setTopicRoute('src.a', 'dst.routed');
  const received: BusMessage[] = [];
  bus.subscribe('dst.**', (msg) => {
    received.push(msg);
  });
  const bytes = bytesOf(3, 1, 4);
  bus.publish('src.a', bytes, { binary: true });
  await flushAll();
  const routed = received.filter((m) => m.topic === 'dst.routed');
  assert.equal(routed.length, 1);
  assert.ok(routed[0].payload instanceof Uint8Array);
  assert.deepEqual([...(routed[0].payload as Uint8Array)], [3, 1, 4]);
});

test('redaction skips binary payloads (bytes have no fields)', async () => {
  const bus = new EventBus();
  bus.setTopicRedaction('bin.**', ['secret']);
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  const bytes = bytesOf(1, 1, 2, 3);
  bus.publish('bin.a', bytes, { binary: true });
  await flushAll();
  assert.equal(received[0].payload, bytes);
});

test('binary messages respect TTL expiry like JSON ones', async () => {
  const clock = { nowMs: 1000, now: () => clock.nowMs };
  const bus = new EventBus({ now: clock.now });
  bus.setTopicTtl('bin.**', 100);
  const received: BusMessage[] = [];
  bus.subscribe('bin.**', (msg) => {
    received.push(msg);
  });
  bus.publish('bin.a', bytesOf(1), { binary: true });
  clock.nowMs += 500; // past the TTL
  await flushAll();
  assert.equal(received.length, 0);
  assert.equal(topicStats(bus, 'bin.a')?.expiredMessages, 1);
});

test('binary publish is counted in stats without breaking them', async () => {
  const bus = new EventBus();
  bus.subscribe('bin.**', () => {});
  bus.publish('bin.a', bytesOf(1, 2, 3), { binary: true });
  bus.publish('bin.a', { json: 1 });
  await flushAll();
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 2);
  assert.equal(topicStats(bus, 'bin.a')?.publishedMessages, 2);
  assert.equal(topicStats(bus, 'bin.a')?.lastSeq, 2);
});
