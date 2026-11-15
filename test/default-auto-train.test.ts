/**
 * Bus-level default compression auto-train parameters (EB-512).
 *
 * `setDefaultAutoTrain(patch)` decides what a future auto-train rule
 * falls back to field by field when it opts into `autoTrain` but omits
 * a field — the default only completes a rule that already opted in,
 * it never enables training on its own. A rule without `autoTrain`
 * stays trainer-less; an explicit rule field always wins; the merged
 * object still goes through the construction-time trigger rule
 * (`resolveAutoTrainOptions`: 60_000 ms cadence when neither trigger
 * is set). `setCompressionAutoTrain` (EB-369) never consults the
 * default. `null` clears it; an identical revision is a no-op with no
 * audit; illegal values fail closed with everything untouched.
 * Audited as `default_auto_train_revised` (`{ previous, autoTrain }`,
 * the `onTrained` callback recorded only as `'set'`).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import type {
  CompressionAutoTrainOptions,
  CompressionAutoTrainedEvent,
} from '../src/bus.ts';

/** Yields until unref'd zero-delay timers (the training pass) have run. */
const flushTimers = () => new Promise<void>((resolve) => setTimeout(resolve, 25));

/** Small, highly repetitive JSON — the shape preset dictionaries target. */
const tick = (i: number) => ({
  symbol: 'BTCUSDT',
  venue: 'binance-spot',
  side: i % 2 === 0 ? 'buy' : 'sell',
  price: 97123.45 + i * 0.01,
  qty: 0.001 * (1 + (i % 7)),
  trade_id: 8_412_000_000 + i,
});

/** A partial autoTrain, as a rule would pass it when relying on the default. */
const partialAutoTrain = (o: Partial<CompressionAutoTrainOptions>) =>
  o as CompressionAutoTrainOptions;

type TrainerOptions = {
  sampleCount: number;
  sampleMaxBytes: number;
  retrainEveryMessages?: number;
  retrainEveryMs?: number;
  onTrained?: (e: CompressionAutoTrainedEvent) => void;
};

const trainerOptions = (bus: EventBus, pattern = 't'): TrainerOptions | undefined =>
  (
    bus as unknown as {
      compressionAutoTrainers: Map<string, { currentOptions(): TrainerOptions }>;
    }
  ).compressionAutoTrainers.get(pattern)?.currentOptions();

const autoTrainRow = (bus: EventBus, topic = 't') =>
  bus.getStats().topics.find((r) => r.topic === topic)?.autoTrain;

const audits = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'default_auto_train_revised' });

test('EB-512: the default fills missing sampleCount/sampleMaxBytes so a partial autoTrain builds a trainer', () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20 });
  bus.subscribe('t', () => {});
  bus.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: partialAutoTrain({ retrainEveryMessages: 5 }),
  });
  const opts = trainerOptions(bus);
  assert.equal(opts?.sampleCount, 10);
  assert.equal(opts?.sampleMaxBytes, 1 << 20);
  assert.equal(opts?.retrainEveryMessages, 5);
  // The message trigger is exclusive: no cadence alongside it.
  assert.equal(opts?.retrainEveryMs, undefined);
  // One publish puts the topic on the stats board with its trainer row.
  bus.publish('t', tick(0));
  assert.notEqual(autoTrainRow(bus), undefined);
});

test('EB-512: a rule without autoTrain gets no trainer despite a default', () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMessages: 3 });
  bus.subscribe('t', () => {});
  bus.setTopicCompression('t', { thresholdBytes: 10 });
  assert.equal(trainerOptions(bus), undefined);
  assert.equal(autoTrainRow(bus), undefined);
});

test('EB-512: explicit topic values win over the default', () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain({
    sampleCount: 10,
    sampleMaxBytes: 1 << 20,
    retrainEveryMessages: 3,
    retrainEveryMs: 30_000,
  });
  bus.subscribe('t', () => {});
  bus.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: { sampleCount: 50, sampleMaxBytes: 4096, retrainEveryMessages: 7 },
  });
  const opts = trainerOptions(bus);
  assert.equal(opts?.sampleCount, 50);
  assert.equal(opts?.sampleMaxBytes, 4096);
  assert.equal(opts?.retrainEveryMessages, 7);
  assert.equal(opts?.retrainEveryMs, 30_000);
});

test('EB-512: trigger interplay follows the construction-time rule on the merged pair', () => {
  // Neither trigger anywhere: the 60_000 ms default cadence applies.
  const neither = new EventBus();
  neither.subscribe('t', () => {});
  neither.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20 },
  });
  assert.equal(trainerOptions(neither)?.retrainEveryMs, 60_000);
  assert.equal(trainerOptions(neither)?.retrainEveryMessages, undefined);

  // The default supplies only the cadence: the rule's message trigger
  // still wins the pair, and the merged pair keeps both.
  const cadence = new EventBus();
  cadence.setDefaultAutoTrain({ retrainEveryMs: 30_000 });
  cadence.subscribe('t', () => {});
  cadence.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMessages: 5 },
  });
  assert.equal(trainerOptions(cadence)?.retrainEveryMessages, 5);
  assert.equal(trainerOptions(cadence)?.retrainEveryMs, 30_000);

  // The default supplies only the message trigger: no cadence at all.
  const messages = new EventBus();
  messages.setDefaultAutoTrain({ retrainEveryMessages: 4 });
  messages.subscribe('t', () => {});
  messages.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: partialAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20 }),
  });
  assert.equal(trainerOptions(messages)?.retrainEveryMessages, 4);
  assert.equal(trainerOptions(messages)?.retrainEveryMs, undefined);
});

test('EB-512: a default-supplied message trigger actually drives training', async () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMessages: 3 });
  bus.subscribe('t', () => {});
  bus.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: partialAutoTrain({}),
  });
  for (let i = 0; i < 3; i += 1) bus.publish('t', tick(i));
  await flushTimers();
  assert.equal(autoTrainRow(bus)?.version, 1);
});

test('EB-512: createTopic merges the default before validating, and a partial autoTrain without one still throws', () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20 });
  bus.subscribe('t', () => {});
  bus.createTopic('t', {
    compression: {
      thresholdBytes: 10,
      autoTrain: partialAutoTrain({ retrainEveryMessages: 5 }),
    },
  });
  assert.equal(trainerOptions(bus)?.sampleCount, 10);
  assert.equal(trainerOptions(bus)?.retrainEveryMessages, 5);

  const bare = new EventBus();
  bare.subscribe('u', () => {});
  assert.throws(
    () =>
      bare.createTopic('u', {
        compression: {
          thresholdBytes: 10,
          autoTrain: partialAutoTrain({ retrainEveryMessages: 5 }),
        },
      }),
    RangeError,
  );
  assert.equal(trainerOptions(bare, 'u'), undefined);
});

test('EB-512: null clears the default and a later partial autoTrain throws again', () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20 });
  assert.deepEqual(bus.getDefaultAutoTrain(), { sampleCount: 10, sampleMaxBytes: 1 << 20 });
  bus.setDefaultAutoTrain(null);
  assert.equal(bus.getDefaultAutoTrain(), undefined);
  bus.subscribe('t', () => {});
  assert.throws(
    () =>
      bus.setTopicCompression('t', {
        thresholdBytes: 10,
        autoTrain: partialAutoTrain({ retrainEveryMessages: 5 }),
      }),
    RangeError,
  );
  assert.equal(trainerOptions(bus), undefined);
  const cleared = audits(bus);
  assert.equal(cleared.length, 2);
  assert.deepEqual(cleared[1].detail, {
    previous: { sampleCount: 10, sampleMaxBytes: 1 << 20 },
    autoTrain: null,
  });
});

test('EB-512: illegal values fail closed with the live default and audit untouched', () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20 });
  const bad: Array<unknown> = [
    { sampleCount: 0 },
    { sampleCount: 2.5 },
    { sampleMaxBytes: 0 },
    { sampleMaxBytes: Number.NaN },
    { retrainEveryMessages: 0 },
    { retrainEveryMs: -1 },
    { onTrained: 'yes' },
    'fleet',
    42,
    [],
  ];
  for (const patch of bad) {
    assert.throws(
      () => bus.setDefaultAutoTrain(patch as never),
      RangeError,
      `patch=${JSON.stringify(patch)}`,
    );
    assert.deepEqual(bus.getDefaultAutoTrain(), { sampleCount: 10, sampleMaxBytes: 1 << 20 });
  }
  assert.throws(() => bus.setDefaultAutoTrain({ sampleCount: 5 }, ''), RangeError);
  assert.deepEqual(bus.getDefaultAutoTrain(), { sampleCount: 10, sampleMaxBytes: 1 << 20 });
  assert.equal(audits(bus).length, 1, 'failed revisions must not audit');
});

test('EB-512: the audit records previous and current precisely, with onTrained only as set', () => {
  const bus = new EventBus();
  const first = () => {};
  const second = () => {};
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20, onTrained: first });
  bus.setDefaultAutoTrain({ sampleCount: 20, retrainEveryMs: 30_000, onTrained: second });
  const entries = audits(bus);
  assert.equal(entries.length, 2);
  assert.deepEqual(entries[0].detail, {
    previous: null,
    autoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20, onTrained: 'set' },
  });
  assert.deepEqual(entries[1].detail, {
    previous: { sampleCount: 10, sampleMaxBytes: 1 << 20, onTrained: 'set' },
    autoTrain: { sampleCount: 20, retrainEveryMs: 30_000, onTrained: 'set' },
  });
});

test('EB-512: re-affirming the current default is a no-op with no audit', () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain(null);
  assert.equal(audits(bus).length, 0, 'clearing an already-unset default audits nothing');
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20 });
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20 });
  assert.equal(audits(bus).length, 1);
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMs: undefined });
  assert.equal(
    audits(bus).length,
    1,
    'an explicitly-undefined field compares equal to a missing one',
  );
  assert.deepEqual(bus.getDefaultAutoTrain(), { sampleCount: 10, sampleMaxBytes: 1 << 20 });
});

test('EB-512: setCompressionAutoTrain never consults the bus default', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  bus.setTopicCompression('t', {
    thresholdBytes: 10,
    autoTrain: { sampleCount: 10, sampleMaxBytes: 1 << 20, retrainEveryMessages: 5 },
  });
  // The default arrives after the trainer was baked — and it carries a
  // cadence the trainer never had.
  bus.setDefaultAutoTrain({ sampleCount: 99, sampleMaxBytes: 999, retrainEveryMs: 5_000 });
  bus.setCompressionAutoTrain('t', { sampleCount: 20 });
  const opts = trainerOptions(bus);
  assert.equal(opts?.sampleCount, 20);
  assert.equal(opts?.sampleMaxBytes, 1 << 20);
  assert.equal(opts?.retrainEveryMessages, 5);
  assert.equal(opts?.retrainEveryMs, undefined);
});

test('EB-512: getDefaultAutoTrain returns a copy — mutating it never moves the live default', () => {
  const bus = new EventBus();
  bus.setDefaultAutoTrain({ sampleCount: 10, sampleMaxBytes: 1 << 20 });
  const exposed = bus.getDefaultAutoTrain();
  assert.notEqual(exposed, undefined);
  exposed!.sampleCount = 999;
  assert.deepEqual(bus.getDefaultAutoTrain(), { sampleCount: 10, sampleMaxBytes: 1 << 20 });
});
