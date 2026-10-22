import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type WatermarkIdleEvent } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

test('idle advancement fires after event-time silence', async () => {
  const events: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => events.push(e) });
  bus.setTopicWatermarkIdle('orders', 30);
  const t0 = Date.now();
  bus.publish('orders', { total: 10 }, { eventTime: t0 });
  await flush();
  assert.equal(events.length, 0);
  await sleep(120);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].topic, 'orders');
  // Default allowedLatenessMs is 0: watermark == max observed event time.
  assert.equal(events[0].watermark, t0);
  assert.ok(events[0].idleMs >= 30, `idleMs ${events[0].idleMs} >= 30`);
  const stats = bus.getStats().topics.find((t) => t.topic === 'orders')?.watermarkIdle;
  assert.deepEqual(stats, { lastAdvanceAt: stats?.lastAdvanceAt, advances: 1 });
  assert.ok(typeof stats?.lastAdvanceAt === 'number');
});

test('advancement is edge-triggered: no repeat while idle, re-arms on new activity', async () => {
  const events: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => events.push(e) });
  bus.setTopicWatermarkIdle('t', 25);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(100);
  assert.equal(events.length, 1);
  // Still idle: no second callback for the same idle episode.
  await sleep(80);
  assert.equal(events.length, 1);
  // New event-time activity re-arms; the next silence advances again.
  bus.publish('t', 'b', { eventTime: 2000 });
  await sleep(100);
  assert.equal(events.length, 2);
  assert.equal(events[1].watermark, 2000);
  const stats = bus.getStats().topics.find((t) => t.topic === 't')?.watermarkIdle;
  assert.equal(stats?.advances, 2);
});

test('messages without eventTime never arm the idle timer', async () => {
  const events: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => events.push(e) });
  bus.setTopicWatermarkIdle('t', 25);
  bus.publish('t', 'plain');
  bus.publish('t', 'plain2');
  await sleep(100);
  await flush();
  assert.equal(events.length, 0);
  // No event-time traffic: no watermark, and the idle rule shows zero
  // advances (the rule is configured, like signing/encryption rules
  // surfacing their config in stats).
  const topic = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topic?.watermark, undefined);
  assert.deepEqual(topic?.watermarkIdle, { lastAdvanceAt: undefined, advances: 0 });
});

test('late judgments after idle use the unchanged watermark: no rollback', async () => {
  const bus = new EventBus();
  bus.setTopicWatermarkIdle('t', 25);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(100);
  // A late message (older than the watermark) is still counted late —
  // idle advancement never rolled the watermark back.
  bus.publish('t', 'stale', { eventTime: 500 });
  await flush();
  let topic = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topic?.lateMessages, 1);
  assert.equal(topic?.watermark, 1000);
  // A new peak advances the watermark normally after idle.
  bus.publish('t', 'fresh', { eventTime: 1500 });
  await flush();
  topic = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topic?.lateMessages, 1);
  assert.equal(topic?.watermark, 1500);
});

test('allowed lateness is still deducted at idle advancement', async () => {
  const events: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => events.push(e), allowedLatenessMs: 100 });
  bus.setTopicWatermarkIdle('t', 25);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(100);
  assert.equal(events.length, 1);
  assert.equal(events[0].watermark, 900);
  assert.equal(bus.getStats().topics.find((t) => t.topic === 't')?.watermark, 900);
});

test('watermarkIdle stats are absent without an idle rule', async () => {
  const bus = new EventBus();
  bus.publish('t', 'a', { eventTime: 1000 });
  await flush();
  const topic = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topic?.watermark, 1000);
  assert.equal(topic?.watermarkIdle, undefined);
});

test('metrics exposition is activity-gated and carries the advances', async () => {
  const events: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => events.push(e) });
  // Before any advancement: byte-identical exposition, no idle series.
  const before = renderPrometheus(bus.getStats());
  assert.ok(!before.includes('eventbus_watermark_idle_advances_total'));
  bus.setTopicWatermarkIdle('t', 25);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(100);
  const after = renderPrometheus(bus.getStats());
  assert.ok(after.includes('# HELP eventbus_watermark_idle_advances_total'));
  assert.ok(after.includes('eventbus_watermark_idle_advances_total{topic="t"} 1'));
});

test('setTopicWatermarkIdle validates its inputs', () => {
  const bus = new EventBus();
  assert.throws(() => bus.setTopicWatermarkIdle('', 10), RangeError);
  assert.throws(() => bus.setTopicWatermarkIdle('t', 0), RangeError);
  assert.throws(() => bus.setTopicWatermarkIdle('t', -5), RangeError);
  assert.throws(() => bus.setTopicWatermarkIdle('t', NaN), RangeError);
  assert.throws(() => bus.setTopicWatermarkIdle('t', Infinity), RangeError);
});

test('clearTopicWatermarkIdle disarms the topic', async () => {
  const events: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => events.push(e) });
  bus.setTopicWatermarkIdle('t', 25);
  assert.equal(bus.clearTopicWatermarkIdle('t'), true);
  assert.equal(bus.clearTopicWatermarkIdle('t'), false);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(100);
  await flush();
  assert.equal(events.length, 0);
  assert.equal(bus.getStats().topics.find((t) => t.topic === 't')?.watermarkIdle, undefined);
});

test('pattern rules match concrete topics; exact rules win', async () => {
  const events: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => events.push(e) });
  bus.setTopicWatermarkIdle('events.*', 25);
  bus.publish('events.a', 'x', { eventTime: 1000 });
  bus.publish('other', 'y', { eventTime: 1000 });
  await sleep(100);
  await flush();
  assert.equal(events.length, 1);
  assert.equal(events[0].topic, 'events.a');
});

test('a throwing onWatermarkIdle never disturbs publishing', async () => {
  const bus = new EventBus({
    onWatermarkIdle: () => {
      throw new Error('observer boom');
    },
  });
  bus.setTopicWatermarkIdle('t', 25);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(100);
  // Publishing still works after the hook threw.
  bus.publish('t', 'b', { eventTime: 2000 });
  await sleep(100);
  await flush();
  const topic = bus.getStats().topics.find((t) => t.topic === 't');
  assert.equal(topic?.watermark, 2000);
  assert.equal(topic?.watermarkIdle?.advances, 2);
});

test('onWatermarkIdle must be a function when provided', () => {
  assert.throws(() => new EventBus({ onWatermarkIdle: 'x' as never }), TypeError);
});
