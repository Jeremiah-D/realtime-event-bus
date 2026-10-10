import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage, type LateMessageEvent } from '../src/bus.ts';
import {
  EventTimeWatermark,
  validateAllowedLatenessMs,
  validateEventTime,
} from '../src/watermark.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-watermark-'));

function collect(bus: EventBus, pattern = '**'): BusMessage[] {
  const seen: BusMessage[] = [];
  bus.subscribe(pattern, (msg) => {
    seen.push(msg);
  });
  return seen;
}

function topicStats(bus: EventBus, topic: string) {
  const row = bus.getStats().topics.find((t) => t.topic === topic);
  assert.ok(row !== undefined, `expected stats row for ${topic}`);
  return row;
}

test('EventTimeWatermark: first observation sets the peak, watermark trails by lateness', () => {
  const w = new EventTimeWatermark();
  assert.equal(w.watermarkFor(0), undefined);
  assert.equal(w.lateCount, 0);
  const first = w.observe(5000, 1000);
  assert.equal(first.late, false);
  assert.equal(first.watermark, 4000);
  assert.equal(w.watermarkFor(1000), 4000);
});

test('EventTimeWatermark: lateness is judged against the pre-fold watermark', () => {
  const w = new EventTimeWatermark();
  w.observe(5000, 1000); // watermark 4000
  // On-time but below the peak: folds in without moving the watermark.
  const inside = w.observe(4500, 1000);
  assert.equal(inside.late, false);
  assert.equal(inside.watermark, 4000);
  assert.equal(w.lateCount, 0);
  // Equal to the watermark is on time (late means strictly below).
  const boundary = w.observe(4000, 1000);
  assert.equal(boundary.late, false);
  // Strictly below the watermark: late, counted, peak untouched.
  const late = w.observe(3999, 1000);
  assert.equal(late.late, true);
  assert.equal(late.watermark, 4000);
  assert.equal(w.lateCount, 1);
  // A new peak advances the watermark monotonically.
  const peak = w.observe(9000, 1000);
  assert.equal(peak.late, false);
  assert.equal(peak.watermark, 8000);
  assert.equal(w.watermarkFor(1000), 8000);
});

test('validateAllowedLatenessMs / validateEventTime reject bad values', () => {
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 'x' as unknown]) {
    assert.throws(() => validateAllowedLatenessMs(bad, 'ctx'), RangeError);
  }
  validateAllowedLatenessMs(undefined, 'ctx');
  validateAllowedLatenessMs(0, 'ctx');
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 'x' as unknown]) {
    assert.throws(() => validateEventTime(bad, 'ctx'), RangeError);
  }
  validateEventTime(undefined, 'ctx');
  validateEventTime(0, 'ctx');
});

test('watermark advances with the maximum event time; plain publishes stay out', async () => {
  const bus = new EventBus();
  const seen = collect(bus, 't');
  bus.publish('t', 'a', { eventTime: 1000 });
  bus.publish('t', 'b', { eventTime: 3000 });
  bus.publish('t', 'c', { eventTime: 2000 }); // below the peak, inside the (zero) grace? 2000 < 3000 -> late
  await flush();
  assert.equal(seen.length, 3);
  const row = topicStats(bus, 't');
  assert.equal(row.watermark, 3000);
  assert.equal(row.lateMessages, 1); // 2000 < 3000 with zero lateness
  assert.equal(bus.getStats().lateMessages, 1);
});

test('messages without an event time never move the watermark', async () => {
  const bus = new EventBus();
  collect(bus, 't');
  bus.publish('t', 'a');
  bus.publish('t', 'b');
  await flush();
  let row = topicStats(bus, 't');
  assert.equal(row.watermark, undefined);
  assert.equal(row.lateMessages, 0);
  bus.publish('t', 'c', { eventTime: 5000 });
  bus.publish('t', 'd');
  row = topicStats(bus, 't');
  assert.equal(row.watermark, 5000);
  assert.equal(row.lateMessages, 0);
});

test('late message is delivered, counted, and reported on onLate', async () => {
  const events: LateMessageEvent[] = [];
  const bus = new EventBus({ onLate: (e) => events.push(e) });
  const seen = collect(bus, 'orders');
  bus.publish('orders', 'first', { eventTime: 5000 });
  bus.publish('orders', 'late', { eventTime: 1000 });
  await flush();
  // Still delivered — lateness never blocks delivery.
  assert.equal(seen.length, 2);
  assert.deepEqual(seen.map((m) => m.seq), [1, 2]);
  assert.equal(seen[1].eventTime, 1000);
  // Counted per topic and bus-wide.
  assert.equal(topicStats(bus, 'orders').lateMessages, 1);
  assert.equal(bus.getStats().lateMessages, 1);
  // Reported once with the judged watermark.
  assert.equal(events.length, 1);
  assert.deepEqual(events[0], {
    topic: 'orders',
    seq: 2,
    eventTime: 1000,
    watermark: 5000,
    allowedLatenessMs: 0,
  });
});

test('eventTime equal to the watermark is on time', () => {
  const bus = new EventBus();
  bus.publish('t', 'a', { eventTime: 5000 });
  bus.publish('t', 'b', { eventTime: 5000 });
  assert.equal(topicStats(bus, 't').lateMessages, 0);
  assert.equal(bus.getStats().lateMessages, 0);
});

test('bus-level allowedLatenessMs opens a grace window', () => {
  const events: LateMessageEvent[] = [];
  const bus = new EventBus({ allowedLatenessMs: 1000, onLate: (e) => events.push(e) });
  bus.publish('t', 'a', { eventTime: 5000 }); // watermark 4000
  assert.equal(topicStats(bus, 't').watermark, 4000);
  bus.publish('t', 'b', { eventTime: 4500 }); // inside the grace: on time
  assert.equal(topicStats(bus, 't').lateMessages, 0);
  bus.publish('t', 'c', { eventTime: 3999 }); // below the watermark: late
  assert.equal(topicStats(bus, 't').lateMessages, 1);
  assert.equal(events.length, 1);
  assert.equal(events[0].allowedLatenessMs, 1000);
  assert.equal(events[0].watermark, 4000);
});

test('per-topic override wins over the bus default; clearer restores it', () => {
  const bus = new EventBus({ allowedLatenessMs: 0 });
  bus.setTopicAllowedLateness('orders', 2000);
  bus.publish('orders', 'a', { eventTime: 10000 }); // watermark 8000 under the override
  assert.equal(topicStats(bus, 'orders').watermark, 8000);
  bus.publish('orders', 'b', { eventTime: 9000 }); // inside the per-topic grace
  assert.equal(topicStats(bus, 'orders').lateMessages, 0);
  assert.equal(bus.clearTopicAllowedLateness('orders'), true);
  bus.publish('orders', 'c', { eventTime: 9500 }); // back to default 0: watermark 10000
  assert.equal(topicStats(bus, 'orders').watermark, 10000);
  assert.equal(topicStats(bus, 'orders').lateMessages, 1);
  assert.equal(bus.clearTopicAllowedLateness('orders'), false);
  assert.equal(bus.clearTopicAllowedLateness('never-set'), false);
});

test('allowed-lateness resolution: exact wins over pattern, earliest pattern wins', () => {
  const bus = new EventBus();
  bus.setTopicAllowedLateness('orders.*', 1000);
  bus.setTopicAllowedLateness('orders.eu', 5000);
  bus.publish('orders.eu', 'a', { eventTime: 10000 }); // exact rule: watermark 5000
  assert.equal(topicStats(bus, 'orders.eu').watermark, 5000);
  bus.publish('orders.eu', 'b', { eventTime: 6000 }); // inside the exact grace
  assert.equal(topicStats(bus, 'orders.eu').lateMessages, 0);
  bus.publish('orders.us', 'a', { eventTime: 10000 }); // pattern rule: watermark 9000
  assert.equal(topicStats(bus, 'orders.us').watermark, 9000);
});

test('invalid lateness config and non-function onLate throw at configuration time', () => {
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 'x']) {
    assert.throws(() => new EventBus({ allowedLatenessMs: bad as number }), RangeError);
  }
  assert.throws(() => new EventBus({ onLate: 42 as unknown as () => void }), TypeError);
  const bus = new EventBus();
  assert.throws(() => bus.setTopicAllowedLateness('', 100), RangeError);
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 'x']) {
    assert.throws(
      () => bus.setTopicAllowedLateness('t', bad as number),
      RangeError,
      `expected RangeError for ${String(bad)}`,
    );
  }
  // No rule was stored by the failed calls.
  assert.equal(bus.clearTopicAllowedLateness('t'), false);
});

test('invalid eventTime throws RangeError from every publish entry point', () => {
  for (const bad of [-1, Number.NaN, Number.POSITIVE_INFINITY, 'x']) {
    const bus = new EventBus();
    assert.throws(() => bus.publish('t', 'x', { eventTime: bad as number }), RangeError);
    assert.throws(
      () => bus.publishBatch([{ topic: 't', payload: 'x', eventTime: bad as number }]),
      RangeError,
    );
    assert.throws(
      () => bus.publishAtomic([{ topic: 't', payload: 'x', eventTime: bad as number }]),
      RangeError,
    );
    assert.throws(
      () => bus.publishIdempotent('t', 'x', { eventTime: bad as number }),
      RangeError,
    );
    // A failed publish mutates nothing: no sequence consumed, no stats.
    assert.equal(bus.getStats().totalPublished, 0);
  }
});

test('a throwing onLate is isolated: delivery and counting continue', async () => {
  const bus = new EventBus({
    onLate: () => {
      throw new Error('broken observer');
    },
  });
  const seen = collect(bus, 't');
  bus.publish('t', 'a', { eventTime: 5000 });
  bus.publish('t', 'b', { eventTime: 1000 }); // late; the callback throws
  await flush();
  assert.equal(seen.length, 2);
  assert.equal(topicStats(bus, 't').lateMessages, 1);
  assert.equal(bus.getStats().lateMessages, 1);
});

test('watermark is orthogonal to publish-order seq: out-of-order business times are not late', () => {
  const bus = new EventBus({ allowedLatenessMs: 1500 });
  // Arrival order (seq 1,2,3) scrambles business time; the grace absorbs it.
  bus.publish('t', 'a', { eventTime: 3000 }); // seq 1, watermark 1500
  bus.publish('t', 'b', { eventTime: 2000 }); // seq 2, 2000 >= 1500: on time
  bus.publish('t', 'c', { eventTime: 4000 }); // seq 3, new peak: watermark 2500
  const row = topicStats(bus, 't');
  assert.equal(row.watermark, 2500);
  assert.equal(row.lateMessages, 0);
  // Only a business time trailing the peak by more than the grace is late,
  // no matter how orderly the arrivals are.
  bus.publish('t', 'd', { eventTime: 1000 }); // seq 4, 1000 < 2500: late
  assert.equal(topicStats(bus, 't').lateMessages, 1);
  assert.equal(topicStats(bus, 't').watermark, 2500); // a late message moves nothing
  assert.equal(topicStats(bus, 't').lastSeq, 4);
});

test('rejected and shed publishes never move the watermark', () => {
  const bus = new EventBus();
  bus.setTopicSchema('t', (p) => typeof p === 'string');
  bus.publish('t', 'ok', { eventTime: 5000 });
  assert.equal(bus.publish('t', 42 as unknown as string, { eventTime: 9000 }), 0);
  assert.equal(topicStats(bus, 't').watermark, 5000);
  assert.equal(topicStats(bus, 't').rejectedMessages, 1);

  bus.setTopicRateLimit('rl', 1, { burst: 1 });
  bus.publish('rl', 'a', { eventTime: 1000 });
  bus.publish('rl', 'b', { eventTime: 9000 }); // shed: never admitted
  assert.equal(topicStats(bus, 'rl').watermark, 1000);
  assert.equal(topicStats(bus, 'rl').rateLimitedMessages, 1);
});

test('eventTime rides publishBatch and publishAtomic entries', () => {
  const bus = new EventBus();
  bus.publishBatch([
    { topic: 't', payload: 'a', eventTime: 7000 },
    { topic: 't', payload: 'b' },
  ]);
  assert.equal(topicStats(bus, 't').watermark, 7000);
  const res = bus.publishAtomic([{ topic: 't2', payload: 'a', eventTime: 3000 }]);
  assert.equal(res.published, 1);
  assert.equal(topicStats(bus, 't2').watermark, 3000);
});

test('durable log persists eventTime and replay restores it onto the envelope', async () => {
  const dir = freshDir();
  const bus = new EventBus({ durableLogDir: dir });
  bus.publish('t', 'a', { eventTime: 4242 });
  const seen: BusMessage[] = [];
  bus.subscribe('t', (msg) => seen.push(msg), { resumeFromSeq: 0 });
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(seen[0].eventTime, 4242);
  assert.equal(seen[0].seq, 1);
});

test('renderPrometheus exposes late messages and the watermark per topic', () => {
  const bus = new EventBus();
  bus.publish('t', 'a', { eventTime: 5000 });
  bus.publish('t', 'b', { eventTime: 1000 });
  bus.publish('plain', 'x');
  const out = renderPrometheus(bus.getStats());
  assert.match(out, /eventbus_topic_late_messages_total\{topic="t"\} 1/);
  assert.match(out, /eventbus_topic_event_time_watermark\{topic="t"\} 5000/);
  assert.match(out, /eventbus_topic_late_messages_total\{topic="plain"\} 0/);
  assert.ok(!out.includes('event_time_watermark{topic="plain"}'));
  assert.match(out, /eventbus_late_messages_total 1/);
});
