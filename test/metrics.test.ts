import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type BusStats, type TopicStats } from '../src/bus.ts';
import { renderPrometheus, PROMETHEUS_CONTENT_TYPE } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

/** Finds the sample line for a metric name (no labels) in exposition text. */
function sampleLine(text: string, name: string): string | undefined {
  return text.split('\n').find((line) => line.startsWith(`${name} `));
}

test('delivered/dropped counters track fan-out and backpressure sheds', async () => {
  const bus = new EventBus();
  const received: unknown[] = [];
  const sub = bus.subscribe('t', (msg) => received.push(msg.payload), { queueSize: 2 });
  // 5 publishes into a capacity-2 queue before the first flush:
  // drop-oldest sheds the first 3, the last 2 are delivered.
  for (let i = 0; i < 5; i += 1) bus.publish('t', i);
  await flush();
  const stats = bus.getStats();
  assert.equal(stats.totalPublished, 5);
  assert.equal(stats.deliveredMessages, 2);
  assert.equal(stats.droppedMessages, 3);
  assert.deepEqual(received, [3, 4]);
  // The bus-level drop counter matches the per-subscriber droppedCount.
  assert.equal(stats.droppedMessages, bus.droppedCount(sub.id));
});

test('throttled counter tracks publish-side throttle sheds', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('t', () => {}, {
    queueSize: 100,
    throttle: { initialRatePerSec: 2, minRatePerSec: 1 },
  });
  for (let i = 0; i < 80; i += 1) bus.publish('t', i); // hits HWM, engages
  assert.equal(bus.getStats().throttledSubscribers, 1);
  // Bucket holds 2 tokens: 2 pass, 3 are shed at the publish side.
  for (let i = 0; i < 5; i += 1) bus.publish('t', `extra-${i}`);
  const stats = bus.getStats();
  assert.equal(stats.throttledMessages, 3);
  assert.equal(stats.droppedMessages, 0); // throttle sheds are not queue drops
});

test('expired/rejected counters track TTL discards and schema rejections', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('ttl-topic', () => {});
  bus.setTopicTtl('ttl-topic', 100);
  bus.publish('ttl-topic', 1);
  bus.publish('ttl-topic', 2);
  clock.nowMs += 200; // both expire before the first flush
  await flush();
  assert.equal(bus.getStats().expiredMessages, 2);
  assert.equal(bus.getStats().deliveredMessages, 0);

  bus.setTopicSchema('schema-topic', (payload) => payload === 'ok');
  bus.publish('schema-topic', 'bad');
  assert.equal(bus.getStats().rejectedMessages, 1);
  // A rejection happens before admission: it is not a publish.
  assert.equal(bus.getStats().totalPublished, 2);
});

test('renderPrometheus emits valid exposition for bus counters and gauges', async () => {
  const bus = new EventBus();
  bus.subscribe('market.btc', () => {});
  bus.subscribe('market.btc', () => {});
  bus.subscribe('news', () => {});
  bus.publish('market.btc', 1);
  bus.publish('news', 2);
  await flush();

  const text = renderPrometheus(bus.getStats());
  assert.ok(text.endsWith('\n'));
  assert.equal(sampleLine(text, 'eventbus_published_messages_total'), 'eventbus_published_messages_total 2');
  assert.equal(sampleLine(text, 'eventbus_delivered_messages_total'), 'eventbus_delivered_messages_total 3');
  assert.equal(sampleLine(text, 'eventbus_dropped_messages_total'), 'eventbus_dropped_messages_total 0');
  assert.equal(sampleLine(text, 'eventbus_rejected_messages_total'), 'eventbus_rejected_messages_total 0');
  assert.equal(sampleLine(text, 'eventbus_subscribers'), 'eventbus_subscribers 3');
  assert.ok(text.includes('# HELP eventbus_delivered_messages_total '));
  assert.ok(text.includes('# TYPE eventbus_delivered_messages_total counter'));
  assert.ok(text.includes('# TYPE eventbus_subscribers gauge'));
  // Per-topic series, in first-publish order.
  assert.ok(text.includes('eventbus_topic_published_messages_total{topic="market.btc"} 1'));
  assert.ok(text.includes('eventbus_topic_subscribers{topic="market.btc"} 2'));
  assert.ok(text.includes('eventbus_topic_subscribers{topic="news"} 1'));
});

test('renderPrometheus escapes label values per the exposition format', () => {
  // The renderer is a pure function of BusStats: fabricate a snapshot to
  // cover label characters a topic name can never carry through the
  // matcher (e.g. a newline never matches `**`, so no live publish can
  // produce it — but the escaper must still handle it).
  const topicStats = (topic: string): TopicStats => ({
    topic,
    subscriberCount: 1,
    publishedMessages: 1,
    expiredMessages: 0,
    lastSeq: 1,
    sequenceGaps: 0,
    rateLimitedMessages: 0,
    rejectedMessages: 0,
    duplicateMessages: 0,
    filteredMessages: 0,
    compressedMessages: 0,
    compressedBytesBefore: 0,
    compressedBytesAfter: 0,
    compressionRatio: 0,
    meanCompressionMs: 0,
    rates: { r1s: 0, r1m: 0, r5m: 0 },
  });
  const stats: BusStats = {
    totalSubscribers: 1,
    subscribersByPattern: {},
    totalPublished: 1,
    deliveredMessages: 1,
    droppedMessages: 0,
    throttledMessages: 0,
    expiredMessages: 0,
    unackedDeliveries: 0,
    patternCacheSize: 0,
    indexSize: 0,
    sequenceGaps: 0,
    rateLimitedMessages: 0,
    rejectedMessages: 0,
    duplicateMessages: 0,
    filteredMessages: 0,
    deadLetteredMessages: 0,
    diagnosticEvents: 0,
    queueBytes: 0,
    compressedMessages: 0,
    compressedBytesBefore: 0,
    compressedBytesAfter: 0,
    compressionRatio: 0,
    meanCompressionMs: 0,
    throttledSubscribers: 0,
    degradedSubscribers: 0,
    shapedSubscribers: 0,
    pendingDelayed: 0,
    deliveryLatency: [],
    slowestSubscribers: [],
    lag: [],
    laggingSubscribers: [],
    keyedReorderedMessages: 0,
    hotKeys: [],
    hotTopics: [],
    bridge: { inbound: 3, outbound: 7, dropped: 1 },
    topics: [topicStats('we"ird\ntopic\\name')],
    consumerGroups: [],
  };
  const text = renderPrometheus(stats);
  assert.ok(
    text.includes('eventbus_topic_subscribers{topic="we\\"ird\\ntopic\\\\name"} 1'),
    `unexpected label escaping in:\n${text}`,
  );
  assert.ok(
    text.includes('eventbus_topic_published_messages_total{topic="we\\"ird\\ntopic\\\\name"} 1'),
    `unexpected label escaping in:\n${text}`,
  );
  assert.ok(text.includes('eventbus_bridge_outbound_total 7'), `missing bridge series in:\n${text}`);
  assert.ok(text.includes('eventbus_bridge_inbound_total 3'), `missing bridge series in:\n${text}`);
  assert.ok(text.includes('eventbus_bridge_dropped_total 1'), `missing bridge series in:\n${text}`);
});

test('PROMETHEUS_CONTENT_TYPE matches the exposition 0.0.4 media type', () => {
  assert.equal(PROMETHEUS_CONTENT_TYPE, 'text/plain; version=0.0.4; charset=utf-8');
});
