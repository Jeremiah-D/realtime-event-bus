import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
/** Yields until unref'd zero-delay timers (the training pass) have run. */
const flushTimers = () => new Promise<void>((resolve) => setTimeout(resolve, 25));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-dicttrain-'));

/** Small, highly repetitive JSON — the shape preset dictionaries target. */
const tick = (i: number) => ({
  symbol: 'BTCUSDT',
  venue: 'binance-spot',
  side: i % 2 === 0 ? 'buy' : 'sell',
  price: 97123.45 + (i % 100) * 0.01,
  qty: 0.001 * (1 + (i % 7)),
  trade_id: 8_412_000_000 + i,
});

const sha256 = (bytes: Buffer): string => createHash('sha256').update(bytes).digest('hex');

/** Read durable-log records for a topic through a fresh log handle. */
const readRecords = (dir: string, topic: string) =>
  DurableTopicLog.open({ dir }).readSince(topic, 0);

test('autoTrain option validation fails fast with RangeError', () => {
  const bus = new EventBus();
  const good = { sampleCount: 10, sampleMaxBytes: 64 * 1024 };
  // Not an object.
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: null as never }), RangeError);
  assert.throws(() => bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: 42 as never }), RangeError);
  // sampleCount must be a positive integer.
  for (const sampleCount of [0, -3, 2.5, Number.NaN]) {
    assert.throws(
      () => bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: { ...good, sampleCount } }),
      RangeError,
    );
  }
  // sampleMaxBytes must be a positive finite number.
  for (const sampleMaxBytes of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: { ...good, sampleMaxBytes } }),
      RangeError,
    );
  }
  // Triggers must be positive when given.
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: { ...good, retrainEveryMessages: 0 } }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: { ...good, retrainEveryMessages: 1.5 } }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: { ...good, retrainEveryMs: -5 } }),
    RangeError,
  );
  assert.throws(
    () => bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: { ...good, onTrained: 'x' as never } }),
    RangeError,
  );
  // Valid configurations register without throwing.
  bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: good });
  bus.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: { ...good, retrainEveryMessages: 5, retrainEveryMs: 1000, onTrained: () => {} },
  });
  // A failed validation leaves no half-registered trainer behind.
  bus.clearTopicCompression('t');
  bus.setTopicCompression('t', { thresholdBytes: 10 });
  const row = bus.getStats().topics.find((r) => r.topic === 't');
  assert.equal(row, undefined);
});

test('autoTrain is disabled by default: no timers, no stats, no sampling cost', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicCompression('t', { thresholdBytes: 16 });
  for (let i = 0; i < 10; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  const row = bus.getStats().topics.find((r) => r.topic === 't')!;
  assert.equal('autoTrain' in row, false);
  assert.deepEqual(received, Array.from({ length: 10 }, (_, i) => tick(i)));
});

test('message-count trigger trains, swaps the dictionary, and keeps inflating', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const initialDict = Buffer.from('BTCUSDT binance-spot buy sell price qty trade_id', 'utf8');
  const initialId = sha256(initialDict);
  const trained: Array<{ version: number; dictionaryId: string }> = [];
  // Training must not run synchronously inside publish.
  let trainedDuringPublish = 0;
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    dictionary: initialDict,
    autoTrain: {
      sampleCount: 50,
      sampleMaxBytes: 1 << 20,
      retrainEveryMessages: 4,
      onTrained: (e) => {
        trained.push(e);
        trainedDuringPublish += 1;
      },
    },
  });
  for (let i = 0; i < 4; i += 1) bus.publish('t', tick(i));
  assert.equal(trainedDuringPublish, 0, 'training ran inline in the publish path');
  await flushTimers();
  assert.equal(trained.length, 1);
  assert.equal(trained[0]!.version, 1);
  assert.notEqual(trained[0]!.dictionaryId, initialId);

  // Stats expose the trained state.
  const row = bus.getStats().topics.find((r) => r.topic === 't')!;
  assert.equal(row.autoTrain!.version, 1);
  assert.equal(row.autoTrain!.dictionaryId, trained[0]!.dictionaryId);
  assert.ok(row.autoTrain!.lastTrainedAt !== undefined);
  assert.equal(row.autoTrain!.sampledMessages, 4);
  assert.ok(row.autoTrain!.sampledBytes > 0);

  // The durable log records the dictId in force at write time: pre-train
  // records carry the initial dictionary's id.
  const records = readRecords(dir, 't');
  assert.equal(records.length, 4);
  for (const rec of records) assert.equal(rec.dictId, initialId);

  // Post-train publishes compress with the new dictionary and still
  // inflate transparently.
  for (let i = 4; i < 8; i += 1) bus.publish('t', tick(i));
  await flush();
  assert.deepEqual(received, Array.from({ length: 8 }, (_, i) => tick(i)));
  const after = readRecords(dir, 't');
  assert.equal(after.length, 8);
  for (const rec of after.slice(4)) assert.equal(rec.dictId, trained[0]!.dictionaryId);
});

test('a publish admitted while training is pending still uses the old dictionary', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.subscribe('t', () => {});
  const initialDict = Buffer.from('BTCUSDT binance-spot buy sell price qty trade_id', 'utf8');
  const initialId = sha256(initialDict);
  const trained: string[] = [];
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    dictionary: initialDict,
    autoTrain: {
      sampleCount: 50,
      sampleMaxBytes: 1 << 20,
      retrainEveryMessages: 2,
      onTrained: (e) => trained.push(e.dictionaryId),
    },
  });
  bus.publish('t', tick(0));
  bus.publish('t', tick(1)); // hits the message trigger; training is queued, not run
  // Synchronous publish on the same tick: the queued training cannot have
  // run yet, so this message must carry the OLD dictionary id.
  bus.publish('t', tick(2));
  const before = readRecords(dir, 't');
  assert.equal(before.length, 3);
  assert.equal(before[2]!.dictId, initialId);
  await flushTimers();
  assert.equal(trained.length, 1);
  assert.notEqual(trained[0], initialId);
  bus.publish('t', tick(3));
  const after = readRecords(dir, 't');
  assert.equal(after[3]!.dictId, trained[0]);
});

test('old dictionaries stay registered: replay resolves pre-train records, fail-closed is unchanged', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.subscribe('t', () => {});
  const initialDict = Buffer.from('BTCUSDT binance-spot buy sell price qty trade_id', 'utf8');
  const trained: string[] = [];
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    dictionary: initialDict,
    autoTrain: {
      sampleCount: 50,
      sampleMaxBytes: 1 << 20,
      retrainEveryMessages: 4,
      onTrained: (e) => trained.push(e.dictionaryId),
    },
  });
  for (let i = 0; i < 4; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  assert.equal(trained.length, 1);
  const trainedId = trained[0]!;
  // Post-train publishes carry the trained dictionary's id.
  for (let i = 4; i < 8; i += 1) bus.publish('t', tick(i));
  await flush();
  const written = readRecords(dir, 't');
  assert.equal(written.length, 8);
  const initialId = sha256(initialDict);
  assert.deepEqual(
    written.map((r) => r.dictId),
    [initialId, initialId, initialId, initialId, trainedId, trainedId, trainedId, trainedId],
  );

  // A fresh bus that re-registers BOTH dictionaries (the initial one via
  // setTopicCompression, the trained one the same way — registration is
  // what matters, not the live rule) replays the whole log: old records
  // still resolve.
  const replayBoth = new EventBus({ durableLogDir: dir });
  const seen: unknown[] = [];
  replayBoth.setTopicCompression('t', { thresholdBytes: 16, dictionary: initialDict });
  // The trained dictionary's bytes are not exposed by the bus API, so
  // re-derive them the documented way: the trainer concatenates the
  // window's samples and takes the trailing 32 KiB. The training ran
  // after the first 4 publishes, so it saw exactly samples 0-3.
  const corpus = Array.from({ length: 4 }, (_, i) => JSON.stringify(tick(i))).join('\n');
  const trainedDict = Buffer.from(corpus, 'utf8');
  assert.equal(sha256(trainedDict), trainedId, 'test re-derivation must match the bus dictionary');
  replayBoth.setTopicCompression('t', { thresholdBytes: 16, dictionary: trainedDict });
  replayBoth.subscribe('t', (msg) => seen.push(msg.payload), { resumeFromSeq: 0 });
  await flush();
  assert.deepEqual(seen, Array.from({ length: 8 }, (_, i) => tick(i)));

  // Fail-closed is unchanged: a bus that only knows the initial
  // dictionary refuses to replay the trained-dictionary records loudly
  // instead of delivering garbage.
  const replayOld = new EventBus({ durableLogDir: dir });
  replayOld.setTopicCompression('t', { thresholdBytes: 16, dictionary: initialDict });
  assert.throws(() => replayOld.subscribe('t', () => {}, { resumeFromSeq: 0 }), /preset dictionary is not registered/);
});

test('retrainEveryMs cadence fires on wall-clock time, and stays quiet without new samples', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const trained: number[] = [];
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    autoTrain: {
      sampleCount: 50,
      sampleMaxBytes: 1 << 20,
      retrainEveryMs: 40,
      onTrained: (e) => trained.push(e.version),
    },
  });
  for (let i = 0; i < 3; i += 1) bus.publish('t', tick(i));
  await new Promise((resolve) => setTimeout(resolve, 120));
  assert.ok(trained.length >= 1, `expected at least one cadence training, got ${trained.length}`);
  const countAfterFirst = trained.length;
  // No new publishes: further cadence ticks must not re-train.
  await new Promise((resolve) => setTimeout(resolve, 120));
  assert.equal(trained.length, countAfterFirst);
  // New samples re-arm the cadence.
  for (let i = 3; i < 6; i += 1) bus.publish('t', tick(i));
  await new Promise((resolve) => setTimeout(resolve, 120));
  assert.ok(trained.length > countAfterFirst);
  bus.clearTopicCompression('t');
});

test('clearTopicCompression retires the trainer: no timers, no training, no stats', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  let trained = 0;
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    autoTrain: {
      sampleCount: 50,
      sampleMaxBytes: 1 << 20,
      retrainEveryMessages: 2,
      retrainEveryMs: 30,
      onTrained: () => {
        trained += 1;
      },
    },
  });
  assert.equal(bus.clearTopicCompression('t'), true);
  for (let i = 0; i < 10; i += 1) bus.publish('t', tick(i));
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.equal(trained, 0, 'a cleared rule trained after the clear');
  const row = bus.getStats().topics.find((r) => r.topic === 't')!;
  assert.equal('autoTrain' in row, false);
  assert.equal(bus.clearTopicCompression('t'), false);
});

test('replacing the rule retires the old trainer', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  let oldTrained = 0;
  let newTrained = 0;
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    autoTrain: {
      sampleCount: 50,
      sampleMaxBytes: 1 << 20,
      retrainEveryMessages: 2,
      onTrained: () => {
        oldTrained += 1;
      },
    },
  });
  // Replace before the old trainer's trigger can fire.
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    autoTrain: {
      sampleCount: 50,
      sampleMaxBytes: 1 << 20,
      retrainEveryMessages: 1000,
      onTrained: () => {
        newTrained += 1;
      },
    },
  });
  for (let i = 0; i < 10; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  assert.equal(oldTrained, 0, 'the replaced rule trained after replacement');
  assert.equal(newTrained, 0, 'the new rule trained before its own trigger');
  const row = bus.getStats().topics.find((r) => r.topic === 't')!;
  assert.equal(row.autoTrain!.version, 0);
  assert.equal(row.autoTrain!.sampledMessages, 10);
});

test('the sample window is dual-bounded and unserializable payloads are skipped', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const trained: number[] = [];
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    autoTrain: {
      sampleCount: 5,
      sampleMaxBytes: 400,
      retrainEveryMessages: 1000,
      onTrained: (e) => trained.push(e.version),
    },
  });
  for (let i = 0; i < 20; i += 1) bus.publish('t', tick(i));
  // Unserializable payloads never break sampling.
  bus.publish('t', undefined);
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  bus.publish('t', circular);
  await flushTimers();
  assert.equal(trained.length, 0);
  const row = bus.getStats().topics.find((r) => r.topic === 't')!;
  assert.equal(row.autoTrain!.sampledMessages, 20, 'only serializable payloads are sampled');
  assert.ok(row.autoTrain!.sampledBytes <= 400, `window bytes=${row.autoTrain!.sampledBytes}`);
});

test('a throwing onTrained callback cannot break the bus', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    autoTrain: {
      sampleCount: 50,
      sampleMaxBytes: 1 << 20,
      retrainEveryMessages: 2,
      onTrained: () => {
        throw new Error('boom');
      },
    },
  });
  for (let i = 0; i < 4; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  await flush();
  assert.deepEqual(received, Array.from({ length: 4 }, (_, i) => tick(i)));
  const row = bus.getStats().topics.find((r) => r.topic === 't')!;
  assert.equal(row.autoTrain!.version, 1, 'training completed despite the throwing callback');
});

test('stats before any training: version 0, initial dictionary id, no lastTrainedAt', async () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  const initialDict = Buffer.from('BTCUSDT binance-spot', 'utf8');
  bus.setTopicCompression('t', {
    thresholdBytes: 16,
    dictionary: initialDict,
    autoTrain: { sampleCount: 50, sampleMaxBytes: 1 << 20, retrainEveryMessages: 1000 },
  });
  for (let i = 0; i < 3; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  const row = bus.getStats().topics.find((r) => r.topic === 't')!;
  assert.equal(row.autoTrain!.version, 0);
  assert.equal(row.autoTrain!.dictionaryId, sha256(initialDict));
  assert.equal(row.autoTrain!.lastTrainedAt, undefined);
  assert.equal(row.autoTrain!.sampledMessages, 3);
  assert.ok(row.autoTrain!.sampledBytes > 0);
});
