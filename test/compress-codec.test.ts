import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { randomBytes } from 'node:crypto';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A JSON payload with enough redundancy to compress well. */
function compressiblePayload(seed: string) {
  return {
    venue: 'market',
    symbol: 'BTC/USDT',
    seed,
    bid: 67234.5,
    ask: 67235.0,
    note: 'tick '.repeat(60),
  };
}

function withTempDir(fn: (dir: string) => void | Promise<void>) {
  return async () => {
    const dir = mkdtempSync(join(tmpdir(), 'compress-codec-'));
    try {
      await fn(dir);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  };
}

test(
  'brotli round-trip: publish compresses, subscriber receives the original',
  withTempDir(async () => {
    const bus = new EventBus();
    bus.setTopicCompression('t', { thresholdBytes: 100, codec: 'brotli', brotliQuality: 4 });
    const received: unknown[] = [];
    bus.subscribe('t', (msg) => {
      received.push(msg.payload);
    });
    const payload = compressiblePayload('a');
    bus.publish('t', payload);
    await flush();
    assert.deepEqual(received, [payload]);
    const stats = bus.getStats();
    assert.equal(stats.compressedMessages, 1);
    assert.ok(stats.compressionRatio > 0 && stats.compressionRatio < 1);
    const row = stats.topics.find((t) => t.topic === 't');
    assert.equal(row?.compressedMessages, 1);
  }),
);

test('deflate stays the default codec (EB-22 semantics unchanged)', async () => {
  const bus = new EventBus();
  bus.setTopicCompression('t', { thresholdBytes: 100 });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
  });
  const payload = compressiblePayload('b');
  bus.publish('t', payload);
  await flush();
  assert.deepEqual(received, [payload]);
  assert.equal(bus.getStats().compressedMessages, 1);
});

test('the codec id is persisted in the durable log and replay inflates by id', async () => {
  await withTempDir(async (dir) => {
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicCompression('t', { thresholdBytes: 100, codec: 'brotli' });
    bus.subscribe('t', () => {});
    const payload = compressiblePayload('c');
    bus.publish('t', payload);
    const line = readFileSync(join(dir, 't.log'), 'utf8').trim();
    assert.ok(
      line.includes('"__busCompressed":"brotli"'),
      `expected the brotli codec id on disk, got: ${line.slice(0, 120)}`,
    );
    // A fresh bus replays the record: inflation follows the envelope's
    // codec id, not whatever rule happens to be configured now.
    const bus2 = new EventBus({ durableLogDir: dir });
    const received: unknown[] = [];
    bus2.subscribe('t', (msg) => {
      received.push(msg.payload);
    }, { resumeFromSeq: 0 });
    await flush();
    assert.deepEqual(received, [payload]);
  })();
});

test('old deflate records stay replayable after the codec registry (backward compatible)', async () => {
  await withTempDir(async (dir) => {
    // Written by a pre-codec bus (deflate, the only codec then).
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicCompression('t', { thresholdBytes: 100 });
    bus.subscribe('t', () => {});
    const payload = compressiblePayload('d');
    bus.publish('t', payload);
    const line = readFileSync(join(dir, 't.log'), 'utf8').trim();
    assert.ok(line.includes('"__busCompressed":"deflate"'));
    // Replayed by a codec-aware bus with a brotli rule: the record's own
    // 'deflate' id wins over the live rule.
    const bus2 = new EventBus({ durableLogDir: dir });
    bus2.setTopicCompression('t', { thresholdBytes: 100, codec: 'brotli' });
    const received: unknown[] = [];
    bus2.subscribe('t', (msg) => {
      received.push(msg.payload);
    }, { resumeFromSeq: 0 });
    await flush();
    assert.deepEqual(received, [payload]);
  })();
});

test('setTopicCompression validates the codec options', () => {
  const bus = new EventBus();
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, codec: 'zstd' as never }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, codec: 'brotli', brotliQuality: 12 }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, codec: 'brotli', brotliQuality: -1 }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.setTopicCompression('t', {
        thresholdBytes: 10,
        codec: 'brotli',
        dictionary: new Uint8Array([1, 2, 3]),
      }),
    RangeError,
  );
  assert.throws(
    () =>
      bus.setTopicCompression('t', {
        thresholdBytes: 10,
        codec: 'brotli',
        autoTrain: { sampleCount: 10, sampleMaxBytes: 1000 },
      }),
    RangeError,
  );
  // Boundary qualities are accepted.
  bus.setTopicCompression('q0', { thresholdBytes: 10, codec: 'brotli', brotliQuality: 0 });
  bus.setTopicCompression('q11', { thresholdBytes: 10, codec: 'brotli', brotliQuality: 11 });
  assert.ok(bus.clearTopicCompression('q0'));
  assert.ok(bus.clearTopicCompression('q11'));
});

test('brotli never adopts an encoding larger than the input', async () => {
  const bus = new EventBus();
  bus.setTopicCompression('t', { thresholdBytes: 10, codec: 'brotli' });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
  });
  // High-entropy binary payload: brotli cannot shrink random bytes, so
  // the guard rejects the larger encoding and the message passes through
  // uncompressed and uncounted.
  const payload = new Uint8Array(randomBytes(512));
  bus.publish('t', payload, { binary: true });
  await flush();
  assert.equal(received.length, 1);
  assert.deepEqual(Buffer.from(received[0] as Uint8Array), Buffer.from(payload));
  assert.equal(bus.getStats().compressedMessages, 0);
});

test('a durable-log record with an unknown codec fails loudly at replay', async () => {
  await withTempDir(async (dir) => {
    const bus = new EventBus({ durableLogDir: dir });
    bus.setTopicCompression('t', { thresholdBytes: 100, codec: 'brotli' });
    bus.subscribe('t', () => {});
    bus.publish('t', compressiblePayload('e'));
    // Forge the codec id on disk: simulates a record written by a newer
    // bus whose codec this one does not know.
    const file = join(dir, 't.log');
    const forged = readFileSync(file, 'utf8').replace(
      '"__busCompressed":"brotli"',
      '"__busCompressed":"zstd"',
    );
    assert.ok(forged.includes('"__busCompressed":"zstd"'));
    writeFileSync(file, forged);
    const bus2 = new EventBus({ durableLogDir: dir });
    assert.throws(
      () =>
        bus2.subscribe('t', () => {}, {
          resumeFromSeq: 0,
        }),
      /unknown compression codec "zstd"/,
    );
  })();
});

test('brotli works for binary payloads (EB-92)', async () => {
  const bus = new EventBus();
  bus.setTopicCompression('t', { thresholdBytes: 100, codec: 'brotli' });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => {
    received.push(msg.payload);
  });
  const payload = new Uint8Array(randomBytes(512));
  bus.publish('t', payload, { binary: true });
  await flush();
  assert.equal(received.length, 1);
  assert.ok(received[0] instanceof Uint8Array);
  assert.deepEqual(Buffer.from(received[0] as Uint8Array), Buffer.from(payload));
});

test('brotli quality changes the wire bytes (higher quality, smaller output)', async () => {
  const payload = compressiblePayload('f');
  const sizes: number[] = [];
  for (const quality of [0, 11]) {
    const bus = new EventBus();
    bus.setTopicCompression('t', { thresholdBytes: 100, codec: 'brotli', brotliQuality: quality });
    const received: unknown[] = [];
    bus.subscribe('t', (msg) => {
      received.push(msg.payload);
    });
    bus.publish('t', payload);
    await flush();
    assert.deepEqual(received, [payload]);
    sizes.push(bus.getStats().topics.find((t) => t.topic === 't')?.compressedBytesAfter ?? -1);
  }
  assert.ok(sizes[0] > 0 && sizes[1] > 0);
  assert.ok(
    sizes[1] <= sizes[0],
    `expected quality 11 (${sizes[1]}) to compress at least as well as quality 0 (${sizes[0]})`,
  );
});
