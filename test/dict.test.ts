import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** Small, highly repetitive JSON — the shape preset dictionaries target. */
const tick = (i: number) => ({
  symbol: 'BTCUSDT',
  venue: 'binance-spot',
  side: i % 2 === 0 ? 'buy' : 'sell',
  price: 97123.45 + (i % 100) * 0.01,
  qty: 0.001 * (1 + (i % 7)),
  trade_id: 8_412_000_000 + i,
});

/** A dictionary built the way the docs recommend: sample payloads joined. */
const sampleDictionary = () =>
  Buffer.from(
    Array.from({ length: 200 }, (_, i) => JSON.stringify(tick(i))).join('\n'),
    'utf8',
  );

test('dictionary option validation', () => {
  const bus = new EventBus();
  // Empty / oversized / wrong-shaped dictionaries throw RangeError.
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: Buffer.alloc(0) }), RangeError);
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: Buffer.alloc(32 * 1024 + 1) }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: 'not-a-buffer' as never }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: 42 as never }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: {} as never }),
    RangeError,
  );
  // Accepted shapes: Buffer, Uint8Array, DataView, ArrayBuffer (exactly 32 KiB ok).
  bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: sampleDictionary() });
  bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: new Uint8Array([1, 2, 3]) });
  const u8 = new Uint8Array([4, 5, 6]);
  bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: new DataView(u8.buffer, u8.byteOffset, u8.byteLength) });
  bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: new ArrayBuffer(16) });
  bus.setTopicCompression('t', { thresholdBytes: 10, dictionary: Buffer.alloc(32 * 1024) });
  // Absent dictionary keeps working (legacy behavior).
  bus.setTopicCompression('t', { thresholdBytes: 10 });
});

test('dictionary is snapshotted: later caller mutation cannot corrupt inflation', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const source = sampleDictionary();
  bus.setTopicCompression('t', { thresholdBytes: 32, dictionary: source });
  // Trash the caller's buffer after registration.
  source.fill(0xAA);
  const payload = tick(1);
  assert.equal(bus.publish('t', payload), 1);
  await flush();
  assert.deepEqual(received, [payload]);
});

test('dictionary-compressed messages inflate transparently and count once', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicCompression('t', { thresholdBytes: 32, dictionary: sampleDictionary() });
  const payloads = Array.from({ length: 5 }, (_, i) => tick(i));
  for (const p of payloads) assert.equal(bus.publish('t', p), 1);
  await flush();
  assert.deepEqual(received, payloads);
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.compressedMessages, 5);
  assert.ok(topic.compressionRatio < 0.5, `ratio=${topic.compressionRatio}`);
});

test('a dictionary beats dictionary-less deflate on small repetitive JSON', async () => {
  const mk = () => {
    const bus = new EventBus();
    bus.subscribe('t', () => {});
    return bus;
  };
  const plain = mk();
  plain.setTopicCompression('t', { thresholdBytes: 32 });
  const withDict = mk();
  withDict.setTopicCompression('t', { thresholdBytes: 32, dictionary: sampleDictionary() });
  for (let i = 0; i < 20; i += 1) {
    plain.publish('t', tick(i));
    withDict.publish('t', tick(i));
  }
  await flush();
  const pStats = plain.getStats().topics.find((t) => t.topic === 't')!;
  const dStats = withDict.getStats().topics.find((t) => t.topic === 't')!;
  // With a dictionary every small message compresses; without one, some
  // messages fail the never-adopt-a-larger-encoding guard outright — the
  // dictionary is what makes small-JSON compression viable at all.
  assert.equal(dStats.compressedMessages, 20);
  assert.ok(dStats.compressionRatio < 0.5 * pStats.compressionRatio,
    `dict ratio=${dStats.compressionRatio.toFixed(3)} plain ratio=${pStats.compressionRatio.toFixed(3)}`);
});

test('replacing the rule swaps the dictionary per message, no cross-talk', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const dictA = Buffer.from('BTCUSDT binance-spot buy sell price qty', 'utf8');
  const dictB = Buffer.from('ETHUSDT coinbase-pro limit market maker', 'utf8');
  bus.setTopicCompression('t', { thresholdBytes: 16, dictionary: dictA });
  const first = tick(1);
  bus.publish('t', first);
  // Rule change mid-stream: the earlier message keeps dictionary A.
  bus.setTopicCompression('t', { thresholdBytes: 16, dictionary: dictB });
  const second = { symbol: 'ETHUSDT', venue: 'coinbase-pro', side: 'buy', price: 5123.1 };
  bus.publish('t', second);
  await flush();
  assert.deepEqual(received, [first, second]);
});

test('durable log replay with dictionary inflates when the same dictionary is re-registered', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-dict-'));
  const dict = sampleDictionary();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('t', { thresholdBytes: 32, dictionary: dict });
  const payload = tick(3);
  bus.publish('t', payload);
  await flush();

  // Restarted bus, same dictionary re-registered: replay inflates.
  const bus2 = new EventBus({ durableLogDir: dir });
  bus2.setTopicCompression('t', { thresholdBytes: 32, dictionary: Buffer.from(dict) });
  const received: unknown[] = [];
  bus2.subscribe('t', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(received, [payload]);
});

test('durable log replay fails closed when the dictionary is not registered', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-dict-missing-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('t', { thresholdBytes: 32, dictionary: sampleDictionary() });
  bus.publish('t', tick(9));
  await flush();

  // Restarted bus WITHOUT the rule: replay throws a clear error instead
  // of delivering garbage.
  const bus2 = new EventBus({ durableLogDir: dir });
  assert.throws(
    () => bus2.subscribe('t', () => {}, { resumeFromSeq: 0 }),
    /preset dictionary is not registered/,
  );
});

test('replayed dictionary-compressed records pass content filters on the raw payload', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-dict-filter-'));
  const dict = sampleDictionary();
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('t', { thresholdBytes: 32, dictionary: dict });
  bus.publish('t', tick(1)); // side: sell
  bus.publish('t', tick(2)); // side: buy
  await flush();

  const bus2 = new EventBus({ durableLogDir: dir });
  bus2.setTopicCompression('t', { thresholdBytes: 32, dictionary: Buffer.from(dict) });
  const received: unknown[] = [];
  bus2.subscribe('t', (msg) => received.push(msg.payload), {
    resumeFromSeq: 0,
    filter: (payload) => (payload as { side: string }).side === 'sell',
  });
  await flush();
  assert.deepEqual(received, [tick(1)]);
});

test('a log line with a malformed dictId is a corrupt line, never replayed', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'bus-dict-corrupt-'));
  const bus = new EventBus({ durableLogDir: dir });
  bus.setTopicCompression('t', { thresholdBytes: 32, dictionary: sampleDictionary() });
  bus.publish('t', tick(1));
  await flush();
  // Tamper with the dictId on disk.
  const { readFileSync, writeFileSync } = await import('node:fs');
  const file = join(dir, 't.log');
  const tampered = readFileSync(file, 'utf8').replace(/"dictId":"[0-9a-f]{64}"/, '"dictId":"forged"');
  assert.ok(tampered.includes('"dictId":"forged"'));
  writeFileSync(file, tampered);

  const bus2 = new EventBus({ durableLogDir: dir });
  const received: unknown[] = [];
  bus2.subscribe('t', (msg) => received.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(received, []);
  // The corrupt line is counted on each read of the file (construction +
  // replay scan), so only its presence — not an exact count — is asserted.
  assert.ok(bus2.getStats().durableLog.corruptLines >= 1);
});

test('a user payload shaped like the envelope still passes through under a dictionary rule', async () => {  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicCompression('t', { thresholdBytes: Number.MAX_SAFE_INTEGER, dictionary: sampleDictionary() });
  const colliding = { __busCompressed: 'deflate', data: 'aGVsbG8=' };
  bus.publish('t', colliding);
  await flush();
  assert.deepEqual(received, [colliding]);
  assert.equal(bus.getStats().compressedMessages, 0);
});
