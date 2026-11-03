/**
 * Live revision of compression auto-train parameters (EB-369).
 *
 * `setCompressionAutoTrain(pattern, patch)` retunes the trainer
 * `setTopicCompression` built for `pattern` in place — the trainer
 * instance, sample window, counters, version, dictionary, and rule
 * fields all survive; only the five auto-train parameters move. A `null`
 * patch retires the trainer entirely. Only a live trainer can be
 * revised; illegal values fail closed with everything untouched; an
 * identical revision is a no-op with no audit.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type { CompressionAutoTrainedEvent } from '../src/bus.ts';

/** Yields until unref'd zero-delay timers (the training pass) have run. */
const flushTimers = () => new Promise<void>((resolve) => setTimeout(resolve, 25));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** Small, highly repetitive JSON — the shape preset dictionaries target. */
const tick = (i: number) => ({
  symbol: 'BTCUSDT',
  venue: 'binance-spot',
  side: i % 2 === 0 ? 'buy' : 'sell',
  price: 97123.45 + i * 0.01,
  qty: 0.001 * (1 + (i % 7)),
  trade_id: 8_412_000_000 + i,
});
const tickBytes = (i: number) => Buffer.byteLength(JSON.stringify(tick(i)), 'utf8');

const autoTrainRow = (bus: EventBus, topic = 't') =>
  bus.getStats().topics.find((r) => r.topic === topic)?.autoTrain;
const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'compression_auto_train_revised' });

/** Bus with a live auto-trainer on 't' and a no-op subscriber. */
function trainedBus(opts: {
  sampleCount?: number;
  sampleMaxBytes?: number;
  retrainEveryMessages?: number;
  retrainEveryMs?: number;
  onTrained?: (e: CompressionAutoTrainedEvent) => void;
} = {}) {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: {
      sampleCount: opts.sampleCount ?? 10,
      sampleMaxBytes: opts.sampleMaxBytes ?? 1 << 20,
      ...(opts.retrainEveryMessages === undefined ? {} : { retrainEveryMessages: opts.retrainEveryMessages }),
      ...(opts.retrainEveryMs === undefined ? {} : { retrainEveryMs: opts.retrainEveryMs }),
      ...(opts.onTrained === undefined ? {} : { onTrained: opts.onTrained }),
    },
  });
  return bus;
}

test('setCompressionAutoTrain rejects unknown patterns, rules without autoTrain, and empty patterns', () => {
  const bus = trainedBus({ retrainEveryMessages: 5 });
  assert.throws(() => bus.setCompressionAutoTrain('', { sampleCount: 5 }), RangeError);
  assert.throws(() => bus.setCompressionAutoTrain('nope', { sampleCount: 5 }), RangeError);
  bus.setTopicCompression('plain', { thresholdBytes: 10 });
  assert.throws(() => bus.setCompressionAutoTrain('plain', { sampleCount: 5 }), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('invalid revisions fail closed: live params, samples, timers, and audit untouched', async () => {
  const trained: CompressionAutoTrainedEvent[] = [];
  const bus = trainedBus({
    retrainEveryMessages: 5,
    onTrained: (e) => trained.push(e),
  });
  for (let i = 0; i < 3; i += 1) bus.publish('t', tick(i));
  const before = autoTrainRow(bus);
  assert.equal(before?.sampledMessages, 3);
  assert.equal(before?.sampledBytes, tickBytes(0) + tickBytes(1) + tickBytes(2));
  const bad: Array<unknown> = [
    { sampleCount: 0 },
    { sampleCount: -3 },
    { sampleCount: 2.5 },
    { sampleCount: Number.NaN },
    { sampleMaxBytes: 0 },
    { sampleMaxBytes: -1 },
    { sampleMaxBytes: Number.NaN },
    { sampleMaxBytes: Number.POSITIVE_INFINITY },
    { retrainEveryMessages: 0 },
    { retrainEveryMessages: 1.5 },
    { retrainEveryMs: -5 },
    { retrainEveryMs: Number.NaN },
    { retrainEveryMs: Number.POSITIVE_INFINITY },
    { onTrained: 'x' },
    // One good field does not save a bad sibling.
    { sampleCount: 20, retrainEveryMessages: 0 },
    // A non-object patch asks nothing valid to move.
    42,
    'nope',
    [],
  ];
  for (const patch of bad) {
    assert.throws(
      () => bus.setCompressionAutoTrain('t', patch as never),
      RangeError,
      `expected RangeError for ${JSON.stringify(patch)}`,
    );
  }
  // Nothing moved: same window, same counters, no audit.
  assert.deepEqual(autoTrainRow(bus), before);
  assert.equal(audit(bus).length, 0);
  // The live trainer is intact: two more samples reach the unchanged
  // message trigger and train exactly once.
  for (let i = 3; i < 5; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  assert.equal(trained.length, 1);
  assert.equal(autoTrainRow(bus)?.version, 1);
  // A valid revision afterwards still works.
  bus.setCompressionAutoTrain('t', { sampleCount: 20 });
  assert.equal(audit(bus).length, 1);
});

test('partial revision moves only the provided fields', () => {
  const bus = trainedBus({ sampleCount: 10, sampleMaxBytes: 65_536, retrainEveryMessages: 5, onTrained: () => {} });
  bus.setCompressionAutoTrain('t', { sampleCount: 20 });
  assert.deepEqual(audit(bus)[0].detail, {
    topicPattern: 't',
    autoTrain: { sampleCount: 20, sampleMaxBytes: 65_536, retrainEveryMessages: 5, onTrained: 'set' },
    oldAutoTrain: { sampleCount: 10, sampleMaxBytes: 65_536, retrainEveryMessages: 5, onTrained: 'set' },
  });
  bus.setCompressionAutoTrain('t', { sampleMaxBytes: 4096 });
  assert.deepEqual(audit(bus)[1].detail, {
    topicPattern: 't',
    autoTrain: { sampleCount: 20, sampleMaxBytes: 4096, retrainEveryMessages: 5, onTrained: 'set' },
    oldAutoTrain: { sampleCount: 20, sampleMaxBytes: 65_536, retrainEveryMessages: 5, onTrained: 'set' },
  });
  assert.equal(audit(bus).length, 2);
});

test('tightening sampleCount prunes immediately, oldest first, newest kept', () => {
  const bus = trainedBus({ retrainEveryMessages: 100 });
  for (let i = 0; i < 5; i += 1) bus.publish('t', tick(i));
  assert.equal(
    autoTrainRow(bus)?.sampledBytes,
    tickBytes(0) + tickBytes(1) + tickBytes(2) + tickBytes(3) + tickBytes(4),
  );
  bus.setCompressionAutoTrain('t', { sampleCount: 3 });
  // The window holds the three newest samples at once — no publish needed.
  assert.equal(autoTrainRow(bus)?.sampledBytes, tickBytes(2) + tickBytes(3) + tickBytes(4));
  assert.equal(autoTrainRow(bus)?.sampledMessages, 5);
  // Tightening the byte cap below a single sample keeps only the newest.
  bus.setCompressionAutoTrain('t', { sampleMaxBytes: 1 });
  assert.equal(autoTrainRow(bus)?.sampledBytes, tickBytes(4));
  assert.equal(autoTrainRow(bus)?.sampledMessages, 5);
});

test('loosening bounds governs from the next sample', () => {
  const bus = trainedBus({ sampleCount: 3, retrainEveryMessages: 100 });
  for (let i = 0; i < 5; i += 1) bus.publish('t', tick(i));
  assert.equal(autoTrainRow(bus)?.sampledBytes, tickBytes(2) + tickBytes(3) + tickBytes(4));
  bus.setCompressionAutoTrain('t', { sampleCount: 10 });
  // Loosening adds no samples by itself.
  assert.equal(autoTrainRow(bus)?.sampledBytes, tickBytes(2) + tickBytes(3) + tickBytes(4));
  for (let i = 5; i < 7; i += 1) bus.publish('t', tick(i));
  assert.equal(
    autoTrainRow(bus)?.sampledBytes,
    tickBytes(2) + tickBytes(3) + tickBytes(4) + tickBytes(5) + tickBytes(6),
  );
  assert.equal(autoTrainRow(bus)?.sampledMessages, 7);
});

test('lowering retrainEveryMessages trains on the next sample; raising defers it', async () => {
  const trainedLower: CompressionAutoTrainedEvent[] = [];
  const lower = trainedBus({ retrainEveryMessages: 10, onTrained: (e) => trainedLower.push(e) });
  for (let i = 0; i < 3; i += 1) lower.publish('t', tick(i));
  await flushTimers();
  assert.equal(trainedLower.length, 0);
  // 3 samples in since the last training; lowering the trigger to 3
  // makes the very next sample reach it.
  lower.setCompressionAutoTrain('t', { retrainEveryMessages: 3 });
  lower.publish('t', tick(3));
  await flushTimers();
  assert.equal(trainedLower.length, 1);
  assert.equal(autoTrainRow(lower)?.version, 1);

  const trainedRaise: CompressionAutoTrainedEvent[] = [];
  const raise = trainedBus({ retrainEveryMessages: 10, onTrained: (e) => trainedRaise.push(e) });
  for (let i = 0; i < 3; i += 1) raise.publish('t', tick(i));
  raise.setCompressionAutoTrain('t', { retrainEveryMessages: 8 });
  for (let i = 3; i < 5; i += 1) raise.publish('t', tick(i));
  await flushTimers();
  assert.equal(trainedRaise.length, 0, 'raised trigger must defer training');
  for (let i = 5; i < 8; i += 1) raise.publish('t', tick(i));
  await flushTimers();
  assert.equal(trainedRaise.length, 1);
});

test('retrainEveryMs re-arms the cadence timer from revision time', async () => {
  const trained: CompressionAutoTrainedEvent[] = [];
  const bus = trainedBus({ retrainEveryMs: 5000, onTrained: (e) => trained.push(e) });
  bus.publish('t', tick(0));
  // The 5s cadence would not fire for seconds; the revised 40ms one
  // fires almost at once — the timer was re-armed at revision time.
  bus.setCompressionAutoTrain('t', { retrainEveryMs: 40 });
  await sleep(150);
  assert.equal(trained.length, 1);
  assert.equal(autoTrainRow(bus)?.version, 1);
});

test('retrainEveryMs unchanged leaves the cadence timer alone', async () => {
  const trained: CompressionAutoTrainedEvent[] = [];
  const bus = trainedBus({ retrainEveryMs: 600, onTrained: (e) => trained.push(e) });
  bus.publish('t', tick(0));
  await sleep(400);
  // Revising an unrelated field must not reset the 600ms cadence armed
  // at construction: training fires ~200ms later, not 600ms later.
  bus.setCompressionAutoTrain('t', { sampleCount: 20 });
  await sleep(350);
  assert.equal(trained.length, 1);
  assert.equal(autoTrainRow(bus)?.version, 1);
});

test('setting retrainEveryMessages clears the cadence', async () => {
  const trained: CompressionAutoTrainedEvent[] = [];
  const bus = trainedBus({ retrainEveryMs: 50, onTrained: (e) => trained.push(e) });
  bus.publish('t', tick(0));
  bus.setCompressionAutoTrain('t', { retrainEveryMessages: 3 });
  assert.deepEqual(audit(bus)[0].detail, {
    topicPattern: 't',
    autoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMessages: 3, onTrained: 'set' },
    oldAutoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMs: 50, onTrained: 'set' },
  });
  // The old 50ms cadence is gone: one more sample trains nothing even
  // after the cadence would have fired.
  bus.publish('t', tick(1));
  await sleep(120);
  assert.equal(trained.length, 0);
  // The message trigger governs now: the third sample trains.
  bus.publish('t', tick(2));
  await flushTimers();
  assert.equal(trained.length, 1);
});

test('clearing retrainEveryMessages falls back to the 60s default cadence', async () => {
  const trained: CompressionAutoTrainedEvent[] = [];
  const bus = trainedBus({ retrainEveryMessages: 5, onTrained: (e) => trained.push(e) });
  for (let i = 0; i < 2; i += 1) bus.publish('t', tick(i));
  bus.setCompressionAutoTrain('t', { retrainEveryMessages: null });
  assert.deepEqual(audit(bus)[0].detail, {
    topicPattern: 't',
    autoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMs: 60_000, onTrained: 'set' },
    oldAutoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMessages: 5, onTrained: 'set' },
  });
  // The message trigger is gone: five more samples train nothing.
  for (let i = 2; i < 7; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  assert.equal(trained.length, 0);
});

test('null patch retires the trainer, getStats row disappears, revise-after-clear is rejected', async () => {
  const trained: CompressionAutoTrainedEvent[] = [];
  const bus = trainedBus({ retrainEveryMessages: 5, onTrained: (e) => trained.push(e) });
  for (let i = 0; i < 2; i += 1) bus.publish('t', tick(i));
  bus.setCompressionAutoTrain('t', null);
  assert.deepEqual(audit(bus)[0].detail, {
    topicPattern: 't',
    autoTrain: null,
    oldAutoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMessages: 5, onTrained: 'set' },
  });
  const row = bus.getStats().topics.find((r) => r.topic === 't')!;
  assert.equal('autoTrain' in row, false);
  // Publishes no longer sample or train.
  for (let i = 2; i < 7; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  assert.equal(trained.length, 0);
  assert.throws(() => bus.setCompressionAutoTrain('t', { sampleCount: 5 }), RangeError);
  assert.throws(() => bus.setCompressionAutoTrain('t', null), RangeError);
  // Re-opting in through setTopicCompression builds a fresh trainer.
  bus.setTopicCompression('t', { thresholdBytes: 10, autoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20 } });
  assert.deepEqual(autoTrainRow(bus), { version: 0, sampledMessages: 0, sampledBytes: 0 });
  bus.publish('t', tick(0));
  assert.equal(autoTrainRow(bus)?.sampledMessages, 1);
});

test('identical revision is a no-op: no audit, no prune, no timer reset', () => {
  const bus = trainedBus({ sampleCount: 10, sampleMaxBytes: 65_536, retrainEveryMessages: 5 });
  for (let i = 0; i < 3; i += 1) bus.publish('t', tick(i));
  const before = autoTrainRow(bus);
  bus.setCompressionAutoTrain('t', { sampleCount: 10, sampleMaxBytes: 65_536, retrainEveryMessages: 5 });
  bus.setCompressionAutoTrain('t', { sampleCount: 10 });
  bus.setCompressionAutoTrain('t', {});
  assert.equal(audit(bus).length, 0);
  assert.deepEqual(autoTrainRow(bus), before);
});

test('audit detail is exact, including the null clear', () => {
  const first = () => {};
  const second = () => {};
  const bus = trainedBus({ sampleCount: 10, sampleMaxBytes: 65_536, retrainEveryMessages: 5, onTrained: first });
  bus.setCompressionAutoTrain('t', { sampleCount: 20, retrainEveryMs: 2000, onTrained: second });
  assert.deepEqual(audit(bus)[0].detail, {
    topicPattern: 't',
    autoTrain: { sampleCount: 20, sampleMaxBytes: 65_536, retrainEveryMessages: 5, retrainEveryMs: 2000, onTrained: 'set' },
    oldAutoTrain: { sampleCount: 10, sampleMaxBytes: 65_536, retrainEveryMessages: 5, onTrained: 'set' },
  });
  bus.setCompressionAutoTrain('t', null);
  assert.deepEqual(audit(bus)[1].detail, {
    topicPattern: 't',
    autoTrain: null,
    oldAutoTrain: { sampleCount: 20, sampleMaxBytes: 65_536, retrainEveryMessages: 5, retrainEveryMs: 2000, onTrained: 'set' },
  });
  assert.equal(audit(bus).length, 2);
});

test('onTrained replacement fires the new callback on the next training', async () => {
  const firstCalls: CompressionAutoTrainedEvent[] = [];
  const secondCalls: CompressionAutoTrainedEvent[] = [];
  const bus = trainedBus({ retrainEveryMessages: 2, onTrained: (e) => firstCalls.push(e) });
  bus.setCompressionAutoTrain('t', { onTrained: (e) => secondCalls.push(e) });
  for (let i = 0; i < 2; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  assert.equal(firstCalls.length, 0);
  assert.equal(secondCalls.length, 1);
  assert.equal(secondCalls[0].version, 1);
  assert.equal(secondCalls[0].topicPattern, 't');
});

test('revision preserves rule fields, version, lastTrainedAt, and sinceLastTrain', async () => {
  const received: unknown[] = [];
  const bus = new EventBus();
  bus.subscribe('t', (msg) => received.push(msg.payload));
  const trained: CompressionAutoTrainedEvent[] = [];
  bus.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: { sampleCount: 50, sampleMaxBytes: 1 << 20, retrainEveryMessages: 3, onTrained: (e) => trained.push(e) },
  });
  for (let i = 0; i < 3; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  assert.equal(trained.length, 1);
  const before = autoTrainRow(bus)!;
  assert.equal(before.version, 1);
  // Two samples since the last training.
  for (let i = 3; i < 5; i += 1) bus.publish('t', tick(i));
  bus.setCompressionAutoTrain('t', { sampleCount: 40 });
  // The revision itself trains nothing and moves no state.
  const after = autoTrainRow(bus)!;
  assert.equal(after.version, before.version);
  assert.equal(after.dictionaryId, before.dictionaryId);
  assert.equal(after.lastTrainedAt, before.lastTrainedAt);
  assert.equal(after.sampledMessages, 5);
  // sinceLastTrain survived: one more sample reaches the trigger of 3
  // and trains a second time — had it reset, three more would be needed.
  bus.publish('t', tick(5));
  await flushTimers();
  assert.equal(trained.length, 2);
  assert.equal(autoTrainRow(bus)?.version, 2);
  // The rule still compresses transparently: every payload arrived whole.
  assert.deepEqual(received, [0, 1, 2, 3, 4, 5].map((i) => tick(i)));
});
