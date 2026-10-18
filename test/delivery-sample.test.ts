import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-delivery-sample-'));

test('modulo determinism: N=3 delivers exactly the seqs divisible by 3', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq), { deliverySampleRate: 3 });
  for (let i = 1; i <= 9; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [3, 6, 9]);
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.sampledDeliveries, 3);
  assert.equal(topic.sampleDropped, 6);
  assert.equal(bus.getStats().sampledDeliveries, 3);
  assert.equal(bus.getStats().sampleDropped, 6);
  // Deliberate skips are not losses: no sequence gaps.
  assert.equal(topic.sequenceGaps, 0);
  assert.equal(bus.getStats().sequenceGaps, 0);
});

test('default rate keeps everything and counts nothing', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq));
  for (let i = 1; i <= 5; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [1, 2, 3, 4, 5]);
  const stats = bus.getStats();
  assert.equal(stats.sampledDeliveries, 0);
  assert.equal(stats.sampleDropped, 0);
  assert.equal(stats.topics.find((t) => t.topic === 't')!.sampledDeliveries, 0);
  assert.equal(stats.topics.find((t) => t.topic === 't')!.sampleDropped, 0);
});

test('rate=1 explicitly keeps everything', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq), { deliverySampleRate: 1 });
  for (let i = 1; i <= 4; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [1, 2, 3, 4]);
  assert.equal(bus.getStats().sampleDropped, 0);
});

test('invalid rates throw RangeError and leave no half-registered subscriber', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, 1.5, Number.NaN, '3', null]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { deliverySampleRate: bad as unknown as number }),
      RangeError,
      `rate ${String(bad)}`,
    );
  }
  assert.equal(bus.subscriberCount(), 0);
  assert.equal(bus.getSubscriptionAudit().length, 0);
});

test('skipped messages consume no backpressure budget', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  // Queue holds one message; drop-newest sheds overflow. With N=2 the
  // odd seqs never reach the queue, so only one message is ever shed.
  const sub = bus.subscribe('t', (msg) => received.push(msg.seq), {
    queueSize: 1,
    dropPolicy: 'drop-newest',
    deliverySampleRate: 2,
  });
  for (let i = 1; i <= 4; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [2]);
  // seq 4 was queue-shed; seqs 1 and 3 were sampling-skipped, never
  // counted as drops.
  assert.equal(bus.droppedCount(sub.id), 1);
  assert.equal(bus.getStats().sampleDropped, 2);
  assert.equal(bus.getStats().droppedMessages, 1);
});

test('skipped messages never reach the ack tracker or the DLQ', async () => {
  const bus = new EventBus();
  const sub = bus.subscribeReliable(
    't',
    (d) => d.nack(), // always fail: admitted messages exhaust redeliveries
    {
      ackTimeoutMs: 1000,
      deadLetter: { maxRedeliveries: 1 },
      deliverySampleRate: 2,
    },
  );
  for (let i = 1; i <= 4; i++) bus.publish('t', i);
  await flush();
  await flush();
  await flush();
  // Only the sampled seqs (2, 4) ever became deliveries; the skipped
  // seqs (1, 3) created no Delivery envelope and no ack obligation.
  // (DeadLetterEntry.seq is DLQ-local — the message identity here is
  // the payload, published as the integers 1..4.)
  const entries = bus.getDeadLetterMessages(sub.id);
  assert.deepEqual(
    entries.map((e) => e.payload).sort((a, b) => (a as number) - (b as number)),
    [2, 4],
  );
  assert.equal(bus.unackedCount(sub.id), 0);
  assert.equal(bus.getStats().deadLetteredMessages, 2);
  assert.equal(bus.getStats().sampleDropped, 2);
});

test('skipped messages are not recorded as consumer-group offsets', async () => {
  const bus = new EventBus();
  const seen: number[] = [];
  bus.subscribeToGroup('g', 't', (msg) => seen.push(msg.seq), { deliverySampleRate: 3 });
  // Delivered: 3, 6. Skipped: 1, 2, 4, 5, 7, 8. The last fanned-out
  // message (seq 8) is skipped, so the recorded offset must be 6 — a
  // skipped seq is not consumption.
  for (let i = 1; i <= 8; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(seen, [3, 6]);
  assert.deepEqual(bus.getGroupOffsets('g'), { t: 6 });
  // A filter-skipped message keeps the existing group-offset semantics:
  // it IS recorded. Only the sampling skip suppresses the record.
  const bus2 = new EventBus();
  const seen2: number[] = [];
  bus2.subscribeToGroup('g', 't', (msg) => seen2.push(msg.seq), {
    filter: (_payload, topic) => topic === 't' && false,
  });
  for (let i = 1; i <= 3; i++) bus2.publish('t', i);
  await flush();
  assert.deepEqual(seen2, []);
  assert.deepEqual(bus2.getGroupOffsets('g'), { t: 3 });
});

test('hot update takes effect immediately and is audited', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.seq));
  for (let i = 1; i <= 4; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [1, 2, 3, 4]);

  assert.equal(bus.setDeliverySampleRate(sub.id, 2), true);
  for (let i = 5; i <= 8; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [1, 2, 3, 4, 6, 8]);

  const events = bus.getSubscriptionAudit({ action: 'delivery_sample_rate_updated' });
  assert.equal(events.length, 1);
  assert.equal(events[0].subId, sub.id);
  assert.equal(events[0].pattern, 't');
  assert.deepEqual(events[0].detail, { from: 1, to: 2 });

  // Back to keep-everything.
  assert.equal(bus.setDeliverySampleRate(sub.id, 1), true);
  for (let i = 9; i <= 10; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(received, [1, 2, 3, 4, 6, 8, 9, 10]);
});

test('setDeliverySampleRate: unknown id returns false, illegal rate throws', () => {
  const bus = new EventBus();
  const sub = bus.subscribe('t', () => {});
  assert.equal(bus.setDeliverySampleRate('no-such-sub', 2), false);
  for (const bad of [0, -2, 2.5, Number.NaN]) {
    assert.throws(() => bus.setDeliverySampleRate(sub.id, bad), RangeError);
    assert.throws(() => bus.setDeliverySampleRate('no-such-sub', bad), RangeError);
  }
  // A no-op update (same rate) still returns true and audits nothing.
  assert.equal(bus.setDeliverySampleRate(sub.id, 1), true);
  assert.equal(bus.getSubscriptionAudit({ action: 'delivery_sample_rate_updated' }).length, 0);
});

test('metrics: activity-gated per-topic series', async () => {
  const quiet = new EventBus();
  quiet.subscribe('t', () => {});
  quiet.publish('t', 1);
  const quietOut = renderPrometheus(quiet.getStats());
  assert.ok(!quietOut.includes('eventbus_delivery_sampled_messages_total'));
  assert.ok(!quietOut.includes('eventbus_delivery_sample_dropped_total'));

  const bus = new EventBus();
  bus.subscribe('t', () => {}, { deliverySampleRate: 2 });
  bus.publish('t', 1);
  bus.publish('t', 2);
  const out = renderPrometheus(bus.getStats());
  assert.ok(out.includes('# HELP eventbus_delivery_sampled_messages_total'));
  assert.ok(out.includes('eventbus_delivery_sampled_messages_total{topic="t"} 1'));
  assert.ok(out.includes('# HELP eventbus_delivery_sample_dropped_total'));
  assert.ok(out.includes('eventbus_delivery_sample_dropped_total{topic="t"} 1'));
  // No confusion with the EB-38 publish-side series.
  assert.ok(!out.includes('eventbus_sampled_messages_total{'));
});

test('barrier control messages bypass the sampling gate', async () => {
  const bus = new EventBus();
  const barriers: number[] = [];
  const data: number[] = [];
  bus.subscribe(
    't',
    (msg) => {
      if (msg.barrier !== undefined) barriers.push(msg.barrier.epoch);
      else data.push(msg.seq);
    },
    { deliverySampleRate: 100, barrier: true },
  );
  for (let i = 1; i <= 3; i++) bus.publish('t', i);
  bus.publish('t', 'cut', { barrier: { epoch: 7 } });
  for (let i = 4; i <= 6; i++) bus.publish('t', i);
  await flush();
  await flush();
  // The barrier is a control signal, not business content: it is always
  // delivered even though no data seq satisfies seq % 100 === 0 here.
  assert.deepEqual(barriers, [7]);
  assert.deepEqual(data, []);
});

test('replay applies the same sampling decision (cross-reconnect consistency)', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  const live: number[] = [];
  bus.subscribe('t', (msg) => live.push(msg.seq), { deliverySampleRate: 3 });
  for (let i = 1; i <= 9; i++) bus.publish('t', i);
  await flush();
  assert.deepEqual(live, [3, 6, 9]);

  // A resubscribing consumer replays the log with the same rate: it sees
  // exactly the live sampled subsequence — never the skipped seqs.
  const replayed: number[] = [];
  bus.subscribe('t', (msg) => replayed.push(msg.seq), {
    deliverySampleRate: 3,
    resumeFromSeq: 0,
  });
  await flush();
  await flush();
  assert.deepEqual(replayed, [3, 6, 9]);
  // No gaps surfaced by the replayed skips either.
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.sequenceGaps, 0);
});

test('sampling composes orthogonally with EB-38 publish-side adaptive sampling', async () => {
  // Deterministic publish-side draw: 0 < rate keeps... use a scripted
  // random that drops every other non-critical publish.
  let draws = 0;
  const bus = new EventBus({
    samplingRandom: () => (draws++ % 2 === 0 ? 0.9 : 0.1),
  });
  bus.setTopicSampleRate('t', 0.5);
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq), { deliverySampleRate: 2 });
  for (let i = 1; i <= 8; i++) bus.publish('t', { n: i });
  await flush();
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 't')!;
  // Publish-side sampling shed 4 publishes before seq assignment; the
  // 4 admitted publishes drew seqs 1..4, and the subscriber-side gate
  // kept seqs 2 and 4.
  assert.equal(topic.sampledMessages, 4);
  assert.deepEqual(received, [2, 4]);
  assert.equal(topic.sampledDeliveries, 2);
  assert.equal(topic.sampleDropped, 2);
  // The two counters never mix: publish-side sheds vs delivery-side skips.
  assert.equal(stats.sampledMessages, 4);
  assert.equal(stats.sampledDeliveries, 2);
  assert.equal(stats.sampleDropped, 2);
});

test('sampled messages still pass through the content filter', async () => {
  const bus = new EventBus();
  const received: number[] = [];
  bus.subscribe('t', (msg) => received.push(msg.seq), {
    deliverySampleRate: 2,
    filter: (payload) => (payload as { keep: boolean }).keep,
  });
  bus.publish('t', { keep: true }); // seq 1: sampled? no (1 % 2) — skipped
  bus.publish('t', { keep: true }); // seq 2: sampled, filter passes
  bus.publish('t', { keep: false }); // seq 3: skipped by sampling
  bus.publish('t', { keep: false }); // seq 4: sampled, filter rejects
  await flush();
  assert.deepEqual(received, [2]);
  const topic = bus.getStats().topics.find((t) => t.topic === 't')!;
  assert.equal(topic.sampledDeliveries, 2);
  assert.equal(topic.sampleDropped, 2);
  assert.equal(topic.filteredMessages, 1);
  assert.equal(topic.sequenceGaps, 0);
});
