import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type AdmissionRejectionEvent } from '../src/bus.ts';
import { DurableTopicLog } from '../src/durablelog.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function makeClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/**
 * Scripted random source: cycles through `values`, so sampling decisions
 * are deterministic and reproducible across runs with the same script.
 */
function scriptedRandom(values: number[]): () => number {
  let i = 0;
  return () => values[i++ % values.length];
}

/** Random source that counts how many times it was drawn from. */
function countingRandom(value: number): { random: () => number; calls: () => number } {
  let calls = 0;
  return {
    random: () => {
      calls += 1;
      return value;
    },
    calls: () => calls,
  };
}

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-sampling-'));

test('no rule means no sampling', () => {
  const bus = new EventBus();
  bus.subscribe('t', () => {});
  for (let i = 0; i < 50; i++) assert.equal(bus.publish('t', i), 1);
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 50);
  assert.equal(stats.sampledMessages, 0);
  assert.equal(stats.topics.find((t) => t.topic === 't')!.sampledMessages, 0);
});

test('rate=1 samples every non-critical publish', async () => {
  const bus = new EventBus({ samplingRandom: () => 0 }); // always below rate
  bus.setTopicSampleRate('hot', 1);
  const received: unknown[] = [];
  bus.subscribe('hot', (msg) => received.push(msg.payload));
  const reasons: string[] = [];
  const bus2 = new EventBus({
    samplingRandom: () => 0,
    onAdmissionRejected: (e: AdmissionRejectionEvent) => reasons.push(e.reason),
  });
  bus2.setTopicSampleRate('hot', 1);
  bus2.subscribe('hot', () => {});
  for (let i = 0; i < 10; i++) {
    assert.equal(bus.publish('hot', i), 0);
    assert.equal(bus2.publish('hot', i), 0);
  }
  await flush();
  assert.deepEqual(received, []);
  assert.deepEqual(reasons, Array(10).fill('sampled'));
  const stats = bus.getStats();
  assert.equal(stats.sampledMessages, 10);
  assert.equal(stats.topics.find((t) => t.topic === 'hot')!.sampledMessages, 10);
  // Sampled publishes never became messages.
  assert.equal(stats.totalPublished, 0);
});

test('rate=0 keeps everything and never draws from the random source', () => {
  const { random, calls } = countingRandom(0);
  const bus = new EventBus({ samplingRandom: random });
  bus.setTopicSampleRate('t', 0);
  bus.subscribe('t', () => {});
  for (let i = 0; i < 20; i++) assert.equal(bus.publish('t', i), 1);
  assert.equal(calls(), 0); // short-circuit: rate 0 draws nothing
  assert.equal(bus.getStats().sampledMessages, 0);
});

test('injected sequence makes sampling deterministic and reproducible', () => {
  const script = [0.1, 0.9, 0.3, 0.7]; // rate 0.5: sample, keep, sample, keep
  const run = () => {
    const bus = new EventBus({ samplingRandom: scriptedRandom(script) });
    bus.setTopicSampleRate('t', 0.5);
    bus.subscribe('t', () => {});
    return [0, 1, 2, 3].map((i) => bus.publish('t', i));
  };
  assert.deepEqual(run(), [0, 1, 0, 1]);
  assert.deepEqual(run(), [0, 1, 0, 1]); // same script, same verdicts
});

test('critical publishes are exempt from sampling', async () => {
  const bus = new EventBus({ samplingRandom: () => 0 }); // would sample everything
  bus.setTopicSampleRate('t', 1);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  assert.equal(bus.publish('t', 'vital', { critical: true }), 1);
  assert.equal(bus.publish('t', 'bulk'), 0);
  assert.equal(bus.publish('t', 'vital2', { critical: true }), 1);
  await flush();
  assert.deepEqual(received, ['vital', 'vital2']);
  const stats = bus.getStats();
  assert.equal(stats.sampledMessages, 1);
  assert.equal(stats.totalPublished, 2);
});

test('sampled messages consume no sequence number — subscribers see no gap', async () => {
  const bus = new EventBus({ samplingRandom: scriptedRandom([0.1, 0.9, 0.1, 0.9]) });
  bus.setTopicSampleRate('t', 0.5);
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  // Verdicts: sampled, admitted, sampled, admitted.
  assert.deepEqual(
    [bus.publish('t', 'a'), bus.publish('t', 'b'), bus.publish('t', 'c'), bus.publish('t', 'd')],
    [0, 1, 0, 1],
  );
  await flush();
  // The admitted messages carry contiguous sequence numbers: the sampled
  // ones left no hole.
  assert.deepEqual(seqs, [1, 2]);
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 't')!;
  assert.equal(topic.lastSeq, 2);
  assert.equal(topic.sequenceGaps, 0);
  assert.equal(stats.sequenceGaps, 0);
});

test('sampled messages are never written to the durable log', () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir, samplingRandom: () => 0 });
  bus.setTopicSampleRate('t', 1);
  bus.subscribe('t', () => {});
  for (let i = 0; i < 3; i++) assert.equal(bus.publish('t', `dropped-${i}`), 0);
  assert.equal(bus.publish('t', 'kept', { critical: true }), 1);
  // The bus's durable-log append is synchronous: reopening the directory
  // must show only the admitted record.
  const reopened = DurableTopicLog.open({ dir });
  assert.deepEqual(reopened.topics(), ['t']);
  assert.equal(reopened.lastSeq('t'), 1);
  assert.deepEqual(
    reopened.readSince('t', 0).map((r) => r.payload),
    ['kept'],
  );
});

test('sampling runs before the hard rate limit and burns no budget', () => {
  const bus = new EventBus({ samplingRandom: () => 0 });
  bus.setTopicRateLimit('t', 1000, { burst: 2 });
  bus.setTopicSampleRate('t', 1);
  bus.subscribe('t', () => {});
  // Five sampled publishes must not touch the rate-limit bucket.
  for (let i = 0; i < 5; i++) assert.equal(bus.publish('t', i), 0);
  // The full burst is still available for critical publishes.
  assert.equal(bus.publish('t', 'c1', { critical: true }), 1);
  assert.equal(bus.publish('t', 'c2', { critical: true }), 1);
  // Bucket empty now: the hard limit sheds, consuming a sequence number.
  assert.equal(bus.publish('t', 'c3', { critical: true }), 0);
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 't')!;
  assert.equal(topic.sampledMessages, 5);
  assert.equal(topic.rateLimitedMessages, 1);
  assert.equal(stats.sampledMessages, 5);
  assert.equal(stats.rateLimitedMessages, 1);
});

test('sampling and the hard rate limit coexist with distinct verdicts', async () => {
  const clock = makeClock();
  const reasons: string[] = [];
  const bus = new EventBus({
    now: clock.now,
    samplingRandom: scriptedRandom([0.9, 0.1]), // keep, sample, keep, ...
    onAdmissionRejected: (e: AdmissionRejectionEvent) => reasons.push(e.reason),
  });
  bus.setTopicRateLimit('t', 1000, { burst: 1 });
  bus.setTopicSampleRate('t', 0.5);
  const seqs: number[] = [];
  bus.subscribe('t', (msg) => seqs.push(msg.seq));
  assert.equal(bus.publish('t', 'a'), 1); // draw 0.9: passes sampling, admitted (burst spent)
  assert.equal(bus.publish('t', 'b'), 0); // draw 0.1: sampled
  assert.equal(bus.publish('t', 'c'), 0); // draw 0.9: passes sampling, bucket empty -> rate-limit shed
  assert.deepEqual(reasons, ['sampled', 'rate-limit']);
  const stats = bus.getStats();
  assert.equal(stats.sampledMessages, 1);
  assert.equal(stats.rateLimitedMessages, 1);
  // The rate-limit shed consumed seq 2 (EB-19 gap semantics); the sampled
  // message consumed nothing. Refill and publish: the subscriber observes
  // the gap.
  clock.nowMs += 1000; // 1000/s refill
  assert.equal(bus.publish('t', 'd', { critical: true }), 1);
  await flush();
  assert.deepEqual(seqs, [1, 3]);
  assert.equal(bus.getStats().sequenceGaps, 1);
});

test('invalid rates and patterns throw RangeError', () => {
  const bus = new EventBus();
  for (const bad of [-0.1, 1.1, Number.NaN, Number.POSITIVE_INFINITY, 'half' as never]) {
    assert.throws(() => bus.setTopicSampleRate('t', bad), RangeError);
  }
  assert.throws(() => bus.setTopicSampleRate('', 0.5), RangeError);
  assert.throws(() => new EventBus({ samplingRandom: 42 as never }), RangeError);
  assert.throws(() => bus.publish('t', 'x', { critical: 'yes' as never }), RangeError);
  assert.throws(() => bus.publishIdempotent('t', 'x', { critical: 1 as never }), RangeError);
  // Boundary values are legal.
  bus.setTopicSampleRate('t', 0);
  bus.setTopicSampleRate('t', 1);
  bus.clearTopicSampleRate('t');
});

test('getStats exposes sampledMessages per topic and globally', () => {
  const bus = new EventBus({ samplingRandom: () => 0 });
  bus.setTopicSampleRate('a', 1);
  bus.setTopicSampleRate('b', 1);
  bus.subscribe('a', () => {});
  bus.subscribe('b', () => {});
  bus.publish('a', 1);
  bus.publish('a', 2);
  bus.publish('b', 3);
  const stats = bus.getStats();
  assert.equal(stats.topics.find((t) => t.topic === 'a')!.sampledMessages, 2);
  assert.equal(stats.topics.find((t) => t.topic === 'b')!.sampledMessages, 1);
  assert.equal(stats.sampledMessages, 3);
});

test('clearTopicSampleRate removes the rule', () => {
  const bus = new EventBus({ samplingRandom: () => 0 });
  bus.setTopicSampleRate('t', 1);
  bus.subscribe('t', () => {});
  assert.equal(bus.publish('t', 'a'), 0);
  assert.equal(bus.clearTopicSampleRate('t'), true);
  assert.equal(bus.clearTopicSampleRate('t'), false);
  assert.equal(bus.publish('t', 'b'), 1);
  assert.equal(bus.getStats().sampledMessages, 1);
});

test('rule matching: exact wins over pattern, earliest pattern wins', () => {
  const bus = new EventBus({ samplingRandom: () => 0 }); // would sample everything
  bus.setTopicSampleRate('t.*', 1);
  bus.setTopicSampleRate('t.hot', 0); // exact wins
  bus.subscribe('t.hot', () => {});
  bus.subscribe('t.cold', () => {});
  assert.equal(bus.publish('t.hot', 1), 1);
  assert.equal(bus.publish('t.cold', 1), 0);
  const stats = bus.getStats();
  assert.equal(stats.topics.find((t) => t.topic === 't.hot')!.sampledMessages, 0);
  assert.equal(stats.topics.find((t) => t.topic === 't.cold')!.sampledMessages, 1);

  // Earliest-registered matching pattern wins.
  const bus2 = new EventBus({ samplingRandom: () => 0 });
  bus2.setTopicSampleRate('a.*', 1);
  bus2.setTopicSampleRate('*.x', 0);
  bus2.subscribe('a.x', () => {});
  assert.equal(bus2.publish('a.x', 1), 0);
  assert.equal(bus2.getStats().sampledMessages, 1);

  // Re-setting a rule replaces it.
  bus2.setTopicSampleRate('a.*', 0);
  assert.equal(bus2.publish('a.x', 2), 1);
  assert.equal(bus2.getStats().sampledMessages, 1);
});

test('idempotency dedup is consulted before sampling', () => {
  const bus = new EventBus({ samplingRandom: () => 0 }); // would sample everything
  bus.setTopicSampleRate('t', 1);
  bus.subscribe('t', () => {});
  // A sampled first attempt claims no dedup slot: the retry stays fresh.
  assert.deepEqual(bus.publishIdempotent('t', 'x', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 0,
  });
  assert.deepEqual(bus.publishIdempotent('t', 'x', { messageId: 'm1' }), {
    duplicate: false,
    accepted: 0,
  });
  assert.equal(bus.getStats().sampledMessages, 2);
  // An admitted first attempt claims the slot: the retry is reported as a
  // duplicate, not sampled — dedup runs before the sampling gate.
  assert.deepEqual(bus.publishIdempotent('t', 'y', { messageId: 'm2', critical: true }), {
    duplicate: false,
    accepted: 1,
  });
  assert.deepEqual(bus.publishIdempotent('t', 'y', { messageId: 'm2' }), {
    duplicate: true,
    accepted: 0,
  });
  const stats = bus.getStats();
  assert.equal(stats.sampledMessages, 2);
  assert.equal(stats.duplicateMessages, 1);
});

test('publishWithConfirmation reports a sampled rejection', async () => {
  const bus = new EventBus({ samplingRandom: () => 0 });
  bus.setTopicSampleRate('t', 1);
  bus.subscribe('t', () => {});
  const receipt = await bus.publishWithConfirmation('t', 'x');
  assert.equal(receipt.admitted, false);
  if (!receipt.admitted) {
    assert.equal(receipt.topic, 't');
    assert.equal(receipt.reason, 'sampled');
    assert.equal(typeof receipt.rejectedAtMs, 'number');
  }
  assert.equal(bus.getStats().sampledMessages, 1);
});

test('sampling applies per publishBatch entry', async () => {
  const bus = new EventBus({ samplingRandom: () => 0 });
  bus.setTopicSampleRate('t', 1);
  const received: unknown[] = [];
  bus.subscribe('t', (msg) => received.push(msg.payload));
  // Batch entries carry no critical flag: all are non-critical.
  assert.equal(
    bus.publishBatch([
      { topic: 't', payload: 'a' },
      { topic: 't', payload: 'b' },
    ]),
    0,
  );
  await flush();
  assert.deepEqual(received, []);
  assert.equal(bus.getStats().sampledMessages, 2);
});
