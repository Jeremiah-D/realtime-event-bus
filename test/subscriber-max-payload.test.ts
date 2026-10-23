import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EventBus, type BusMessage } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

const freshDir = () => mkdtempSync(join(tmpdir(), 'eb-oversize-'));

const collect = (bus: EventBus, pattern: string, opts?: Parameters<EventBus['subscribe']>[2]) => {
  const received: BusMessage[] = [];
  const sub = bus.subscribe(pattern, (msg) => received.push(msg), opts);
  return { received, sub };
};

/** A payload whose JSON UTF-8 encoding is exactly `size` bytes. */
const sizedPayload = (size: number) => ({ data: 'x'.repeat(Math.max(0, size - 11)) });

test('subscriberMaxPayloadBytes: oversize messages are skipped, small ones delivered', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', { subscriberMaxPayloadBytes: 64 });
  bus.publish('t', sizedPayload(32));
  bus.publish('t', sizedPayload(128));
  bus.publish('t', sizedPayload(48));
  await flush();
  assert.deepEqual(
    received.map((m) => (m.payload as { data: string }).data.length),
    [sizedPayload(32).data.length, sizedPayload(48).data.length],
  );
  const stats = bus.getStats();
  assert.equal(stats.subscriberOversizeDropped, 1);
  assert.equal(stats.topics.find((t) => t.topic === 't')?.subscriberOversizeDropped, 1);
  // The skip is deliberate narrowing: never a sequence gap, never a drop.
  assert.equal(stats.sequenceGaps, 0);
  assert.equal(stats.droppedMessages, 0);
  assert.equal(stats.filteredMessages, 0);
});

test('subscriberMaxPayloadBytes: invalid values throw RangeError at subscribe time', () => {
  const bus = new EventBus();
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => bus.subscribe('t', () => {}, { subscriberMaxPayloadBytes: bad }),
      RangeError,
      `expected RangeError for ${String(bad)}`,
    );
  }
  // A rejected subscribe leaves no half-registered subscriber behind.
  assert.equal(bus.getStats().totalSubscribers, 0);
});

test('subscriberMaxPayloadBytes: absent means no cap (backward compatible)', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't');
  bus.publish('t', sizedPayload(4096));
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 0);
});

test('subscriberMaxPayloadBytes: binary payloads are measured as wire bytes', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', { subscriberMaxPayloadBytes: 16 });
  bus.publish('t', new Uint8Array(8), { binary: true });
  bus.publish('t', new Uint8Array(32), { binary: true });
  await flush();
  assert.equal(received.length, 1);
  assert.equal((received[0].payload as Uint8Array).byteLength, 8);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
});

test('subscriberMaxPayloadBytes: unserializable payloads measure 0 and always pass', async () => {
  const bus = new EventBus();
  const { received } = collect(bus, 't', { subscriberMaxPayloadBytes: 8 });
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  bus.publish('t', circular);
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 0);
});

test('subscriberMaxPayloadBytes: explicit cap wins over the inherited topic cap', async () => {
  const bus = new EventBus();
  bus.setTopicMaxPayloadBytes('t', 1024);
  const oversizeEvents: Array<{ capBytes: number; capSource: string }> = [];
  bus.setTopicSchemaFingerprint('t', {
    onDrift: () => {},
    onOversize: (e) => oversizeEvents.push({ capBytes: e.capBytes, capSource: e.capSource }),
  });
  const { received } = collect(bus, 't', { subscriberMaxPayloadBytes: 64 });
  bus.publish('t', sizedPayload(128)); // under the topic cap, over the subscriber cap
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
  assert.deepEqual(oversizeEvents, [{ capBytes: 64, capSource: 'subscriber' }]);
});

test('subscriberMaxPayloadBytes: inherits the topic cap when no explicit cap is set (replay path)', async () => {
  const dir = freshDir();
  const bus1 = new EventBus({ durableLogDir: dir });
  // No topic cap yet: the big message is admitted and logged.
  bus1.publish('t', sizedPayload(256));
  bus1.publish('t', sizedPayload(32));
  await flush();
  // The operator lowers the topic cap afterwards.
  bus1.setTopicMaxPayloadBytes('t', 64);
  const oversizeEvents: Array<{ capBytes: number; capSource: string; subscriberId: string }> = [];
  bus1.setTopicSchemaFingerprint('t', {
    onDrift: () => {},
    onOversize: (e) =>
      oversizeEvents.push({ capBytes: e.capBytes, capSource: e.capSource, subscriberId: e.subscriberId }),
  });
  // A fresh bus on the same log resumes history: the big record now
  // exceeds the inherited topic cap and is skipped.
  const bus2 = new EventBus({ durableLogDir: dir });
  bus2.setTopicMaxPayloadBytes('t', 64);
  bus2.setTopicSchemaFingerprint('t', {
    onDrift: () => {},
    onOversize: (e) =>
      oversizeEvents.push({ capBytes: e.capBytes, capSource: e.capSource, subscriberId: e.subscriberId }),
  });
  const received: BusMessage[] = [];
  const sub = bus2.subscribe('t', (m) => received.push(m), { resumeFromSeq: 0 });
  await flush();
  assert.equal(received.length, 1);
  assert.equal((received[0].payload as { data: string }).data.length, sizedPayload(32).data.length);
  const stats = bus2.getStats();
  assert.equal(stats.subscriberOversizeDropped, 1);
  assert.equal(oversizeEvents.length, 1);
  assert.equal(oversizeEvents[0].capBytes, 64);
  assert.equal(oversizeEvents[0].capSource, 'topic');
  assert.equal(oversizeEvents[0].subscriberId, sub.id);
  assert.equal(stats.sequenceGaps, 0);
});

test('subscriberMaxPayloadBytes: EB-200 onOversize linkage fires per matching watcher', async () => {
  const bus = new EventBus();
  const seen: Array<{ topic: string; subscriberId: string; payloadBytes: number; capBytes: number }> = [];
  const other: unknown[] = [];
  bus.setTopicSchemaFingerprint('orders.**', {
    onDrift: () => {},
    onOversize: (e) =>
      seen.push({ topic: e.topic, subscriberId: e.subscriberId, payloadBytes: e.payloadBytes, capBytes: e.capBytes }),
  });
  // A watcher on a non-matching pattern never fires.
  bus.setTopicSchemaFingerprint('other.**', {
    onDrift: () => {},
    onOversize: (e) => other.push(e),
  });
  const { sub } = collect(bus, 'orders.new', { subscriberMaxPayloadBytes: 32 });
  bus.publish('orders.new', sizedPayload(128));
  await flush();
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 'orders.new');
  assert.equal(seen[0].subscriberId, sub.id);
  assert.ok(seen[0].payloadBytes > 32);
  assert.equal(seen[0].capBytes, 32);
  assert.equal(other.length, 0);
});

test('subscriberMaxPayloadBytes: watcher without onOversize is inert', async () => {
  const bus = new EventBus();
  bus.setTopicSchemaFingerprint('t', { onDrift: () => {} });
  const { received } = collect(bus, 't', { subscriberMaxPayloadBytes: 32 });
  bus.publish('t', sizedPayload(128));
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
});

test('subscriberMaxPayloadBytes: a throwing onOversize never breaks delivery', async () => {
  const bus = new EventBus();
  bus.setTopicSchemaFingerprint('t', {
    onDrift: () => {},
    onOversize: () => {
      throw new Error('boom');
    },
  });
  const { received } = collect(bus, 't', { subscriberMaxPayloadBytes: 32 });
  bus.publish('t', sizedPayload(128));
  bus.publish('t', sizedPayload(16));
  await flush();
  assert.equal(received.length, 1);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
});

test('subscriberMaxPayloadBytes: invalid onOversize throws RangeError', () => {
  const bus = new EventBus();
  assert.throws(
    () =>
      bus.setTopicSchemaFingerprint('t', {
        onDrift: () => {},
        onOversize: 'nope' as unknown as () => void,
      }),
    RangeError,
  );
});

test('subscriberMaxPayloadBytes: skip burns no throttle/shaping budget and creates no ack obligation', async () => {
  const bus = new EventBus();
  const { received, sub } = collect(bus, 't', {
    subscriberMaxPayloadBytes: 32,
    deliveryShaping: { messagesPerSec: 1, burst: 1 },
  });
  bus.publish('t', sizedPayload(128)); // skipped: must not burn the shaping token
  bus.publish('t', sizedPayload(16)); // admitted: consumes the single token
  bus.publish('t', sizedPayload(16)); // shaped: waits for the next token, not dropped
  await flush();
  assert.equal(received.length, 1);
  const stats = bus.getStats();
  assert.equal(stats.subscriberOversizeDropped, 1);
  assert.equal(stats.throttledMessages, 0);
  assert.equal(bus.unackedCount(sub.id), 0);
});

test('subscriberMaxPayloadBytes: retained snapshot applies the cap', async () => {
  const bus = new EventBus();
  bus.setTopicRetained('t', true);
  bus.publish('t', sizedPayload(256), { retain: true });
  const { received } = collect(bus, 't', { subscriberMaxPayloadBytes: 64 });
  await flush();
  assert.equal(received.length, 0);
  assert.equal(bus.getStats().subscriberOversizeDropped, 1);
});

test('subscriberMaxPayloadBytes: renders in the Prometheus exposition', async () => {
  const bus = new EventBus();
  collect(bus, 't', { subscriberMaxPayloadBytes: 32 });
  bus.publish('t', sizedPayload(128));
  await flush();
  const text = renderPrometheus(bus.getStats());
  const globalLine = text.split('\n').find((l) => l.startsWith('eventbus_subscriber_oversize_dropped_total '));
  assert.equal(globalLine, 'eventbus_subscriber_oversize_dropped_total 1');
  const seriesLine = text
    .split('\n')
    .find((l) => l.startsWith('eventbus_subscriber_oversize_dropped_total{topic="t"}'));
  assert.equal(seriesLine, 'eventbus_subscriber_oversize_dropped_total{topic="t"} 1');
});

test('subscriberMaxPayloadBytes: exposition stays byte-identical without capped subscribers', async () => {
  const plain = new EventBus();
  collect(plain, 't');
  plain.publish('t', sizedPayload(128));
  await flush();
  const text = renderPrometheus(plain.getStats());
  assert.ok(!text.includes('eventbus_subscriber_oversize_dropped_total{'));
});
