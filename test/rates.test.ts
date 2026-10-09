import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusStats } from '../src/bus.ts';
import {
  PublishRateTable,
  HOT_TOPICS_LIMIT,
  DEFAULT_RATE_RING_CAPACITY,
  ZERO_RATES,
} from '../src/rates.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

const ratesOf = (stats: BusStats, topic: string) =>
  stats.topics.find((t) => t.topic === topic)?.rates;

test('sliding windows count publishes deterministically with an injected clock', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  for (let i = 0; i < 120; i += 1) bus.publish('rates.a', i);

  // 120 events inside all three windows: 120/1, 120/60, 120/300.
  assert.deepEqual(ratesOf(bus.getStats(), 'rates.a'), { r1s: 120, r1m: 2, r5m: 0.4 });

  // A sample exactly `windowMs` old still counts (inclusive edge)...
  clock.nowMs += 1_000;
  assert.equal(ratesOf(bus.getStats(), 'rates.a')?.r1s, 120);
  // ...one millisecond older and the 1s window empties.
  clock.nowMs += 1;
  assert.equal(ratesOf(bus.getStats(), 'rates.a')?.r1s, 0);
  assert.equal(ratesOf(bus.getStats(), 'rates.a')?.r1m, 2);

  // Past the 1m edge the 1m rate decays; the 5m window still holds.
  clock.nowMs += 60_000;
  assert.equal(ratesOf(bus.getStats(), 'rates.a')?.r1m, 0);
  assert.equal(ratesOf(bus.getStats(), 'rates.a')?.r5m, 0.4);

  // Everything aged out: all-zero rates.
  clock.nowMs += 300_000;
  assert.deepEqual(ratesOf(bus.getStats(), 'rates.a'), { r1s: 0, r1m: 0, r5m: 0 });
});

test('only admitted publishes sample: rejections, sheds and duplicates never count', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.setTopicSchema('strict', (p) => typeof p === 'number');
  bus.publish('strict', 1); // admitted
  bus.publish('strict', 'nope'); // schema-rejected: never sampled
  bus.setTopicRateLimit('capped', 1, { burst: 1 });
  bus.publish('capped', 'a'); // admitted
  bus.publish('capped', 'b'); // rate-limit shed: never sampled
  bus.publishIdempotent('idem', 'x', { messageId: 'm1' }); // admitted
  bus.publishIdempotent('idem', 'x', { messageId: 'm1' }); // duplicate: never sampled

  assert.deepEqual(ratesOf(bus.getStats(), 'strict'), {
    r1s: 1,
    r1m: 1 / 60,
    r5m: 1 / 300,
  });
  assert.equal(ratesOf(bus.getStats(), 'capped')?.r1s, 1);
  assert.equal(ratesOf(bus.getStats(), 'idem')?.r1s, 1);
});

test('delayed messages sample at fan-out time, not at schedule time', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.publishDelayed('later', 'payload', { delayMs: 5_000 });
  // The schedule is intent, not publish traffic: nothing sampled yet, and
  // the topic has no stats entry at all before its first fan-out.
  assert.equal(ratesOf(bus.getStats(), 'later'), undefined);

  // Advance past the due time; the next publish triggers a flush, whose
  // due-sweep fans the delayed message out through the normal pipeline.
  clock.nowMs += 6_000;
  bus.publish('trigger', 1);
  await flush();
  assert.deepEqual(ratesOf(bus.getStats(), 'later'), { r1s: 1, r1m: 1 / 60, r5m: 1 / 300 });
});

test('ring wraps past capacity: oldest samples evict, reads stay correct', () => {
  const table = new PublishRateTable(8);
  const t0 = 1_000_000;
  for (let i = 0; i < 10; i += 1) table.sample('burst', t0);
  // Only the newest 8 of the 10 samples survive the wrap.
  assert.deepEqual(table.ratesFor('burst', t0), { r1s: 8, r1m: 8 / 60, r5m: 8 / 300 });
  assert.equal(table.topicCount, 1);
  // A topic that never published gets all-zero rates.
  assert.deepEqual(table.ratesFor('never-published', t0), { ...ZERO_RATES });
  // Invalid capacities fail fast.
  assert.throws(() => new PublishRateTable(0), RangeError);
  assert.throws(() => new PublishRateTable(-4), RangeError);
});

test('hotTopics ranks by 1m rate, caps at 10, drops idle topics', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  // Twelve topics with strictly decreasing volumes.
  for (let i = 0; i < 12; i += 1) {
    for (let j = 0; j < (12 - i) * 10; j += 1) bus.publish(`hot.${i}`, j);
  }
  const hot = bus.getStats().hotTopics;
  assert.equal(hot.length, HOT_TOPICS_LIMIT);
  assert.equal(hot[0].topic, 'hot.0');
  assert.equal(hot[0].r1m, 120 / 60);
  assert.equal(hot[0].r1s, 120);
  assert.equal(hot[0].r5m, 120 / 300);
  // Strictly descending by 1m rate.
  for (let i = 1; i < hot.length; i += 1) {
    assert.ok(hot[i - 1].r1m > hot[i].r1m, `hot[${i - 1}].r1m not above hot[${i}].r1m`);
  }
  // The 11th and 12th topics are cut by the cap, not by lack of traffic.
  assert.ok(!hot.some((h) => h.topic === 'hot.10' || h.topic === 'hot.11'));

  // Past the 1m window nothing is hot anymore.
  clock.nowMs += 61_000;
  assert.deepEqual(bus.getStats().hotTopics, []);
});

test('getStats shape: per-topic rates and hotTopics are present', () => {
  const bus = new EventBus();
  bus.publish('shape.t', 1);
  const stats = bus.getStats();
  const topic = stats.topics.find((t) => t.topic === 'shape.t');
  assert.ok(topic !== undefined);
  assert.deepEqual(Object.keys(topic.rates).sort(), ['r1m', 'r1s', 'r5m']);
  assert.equal(topic.rates.r1s, 1); // published within the last second
  assert.ok(Array.isArray(stats.hotTopics));
  assert.equal(stats.hotTopics[0].topic, 'shape.t');
});

test('prometheus renders the rate gauges for hot topics', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  for (let i = 0; i < 120; i += 1) bus.publish('prom.a', i);
  const text = renderPrometheus(bus.getStats());
  assert.ok(
    text.includes('eventbus_topic_rate_msg_per_sec{topic="prom.a",window="1s"} 120'),
    `missing 1s rate gauge in:\n${text}`,
  );
  assert.ok(
    text.includes('eventbus_topic_rate_msg_per_sec{topic="prom.a",window="1m"} 2'),
    `missing 1m rate gauge in:\n${text}`,
  );
  assert.ok(
    text.includes('eventbus_topic_rate_msg_per_sec{topic="prom.a",window="5m"} 0.4'),
    `missing 5m rate gauge in:\n${text}`,
  );
});

test('prometheus tolerates a stats object without the rate fields', () => {
  const bus = new EventBus();
  bus.publish('legacy.t', 1);
  // Strip the new fields the way a caller built against an older BusStats
  // shape would: the renderer must degrade, not throw.
  const { hotTopics: _omitted, ...legacyStats } = bus.getStats();
  const text = renderPrometheus(legacyStats as BusStats);
  // No rate *series* (the HELP/TYPE headers still render, matching the
  // module's convention for empty series families like delivery latency).
  assert.ok(!text.includes('eventbus_topic_rate_msg_per_sec{'));
  // Everything else still renders.
  assert.ok(text.includes('eventbus_published_messages_total 1'));
});

test('default ring capacity documents the sizing tradeoff', () => {
  // 60_000 slots x 8 bytes = 480 KiB per topic; exact 5m window up to
  // 200 msg/s sustained (60_000 / 300).
  assert.equal(DEFAULT_RATE_RING_CAPACITY, 60_000);
  assert.equal(DEFAULT_RATE_RING_CAPACITY / 300, 200);
});
