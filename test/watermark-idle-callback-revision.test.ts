/**
 * Watermark idle-advance hook live redirection (EB-431).
 *
 * `setWatermarkIdleCallback` swaps the `onWatermarkIdle` observer every
 * idle watermark advancement is handed to without restarting the bus.
 * There is no enable gate — any open bus can install one — and `null`
 * clears back to no observer. The new hook governs from the next idle
 * advancement onward, the advancement counters move regardless of
 * whether a hook is installed, and a throwing hook is swallowed by
 * `armWatermarkIdleTimer`. The callback function itself is never
 * written to the audit log: only whether one is installed is recorded
 * (`{ hasCallback, oldHasCallback }`). Re-installing the live callback
 * is a no-op with no audit. The setter throws `RangeError` for a
 * non-function, where the constructor throws `TypeError` — the
 * construction-time inconsistency is documented, not changed.
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus, type WatermarkIdleEvent } from '../src/bus.ts';

const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const noop = () => {};

const audit = (bus: EventBus) =>
  bus.getSubscriptionAudit({ action: 'watermark_idle_callback_revised' });
const advances = (bus: EventBus, topic: string) =>
  bus.getStats().topics.find((t) => t.topic === topic)?.watermarkIdle?.advances ?? -1;

test('installing a hook routes later advancements to it', async () => {
  const seen: WatermarkIdleEvent[] = [];
  const bus = new EventBus();
  bus.setTopicWatermarkIdle('orders', 30);
  // No hook at construction: the first advancement is counted, not observed.
  const t0 = Date.now();
  bus.publish('orders', { total: 10 }, { eventTime: t0 });
  await sleep(120);
  assert.equal(seen.length, 0);
  assert.equal(advances(bus, 'orders'), 1);
  bus.setWatermarkIdleCallback((e) => seen.push(e));
  // New event-time activity re-arms; the next silence advances again.
  const t1 = t0 + 1000;
  bus.publish('orders', { total: 11 }, { eventTime: t1 });
  await sleep(120);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 'orders');
  // Default allowedLatenessMs is 0: watermark == max observed event time.
  assert.equal(seen[0].watermark, t1);
  assert.ok(seen[0].idleMs >= 30, `idleMs ${seen[0].idleMs} >= 30`);
  assert.equal(advances(bus, 'orders'), 2);
  bus.destroy?.();
});

test('clearing with null stops delivery and the counter keeps moving', async () => {
  const seen: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => seen.push(e) });
  bus.setTopicWatermarkIdle('t', 30);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(120);
  assert.equal(seen.length, 1);
  bus.setWatermarkIdleCallback(null);
  bus.publish('t', 'b', { eventTime: 2000 });
  await sleep(120);
  assert.equal(seen.length, 1);
  assert.equal(advances(bus, 't'), 2);
  // Clearing a bus with no callback is a no-op with no audit.
  const before = audit(bus).length;
  bus.setWatermarkIdleCallback(null);
  assert.equal(audit(bus).length, before);
  bus.destroy?.();
});

test('non-functions are rejected and the live hook stays put', async () => {
  const seen: WatermarkIdleEvent[] = [];
  const bus = new EventBus({ onWatermarkIdle: (e) => seen.push(e) });
  const bad: Array<unknown> = ['nope', 42, {}, [], undefined];
  for (const handler of bad) {
    assert.throws(
      () => bus.setWatermarkIdleCallback(handler as never),
      RangeError,
      `expected RangeError for ${String(handler)}`,
    );
  }
  // The failed calls changed nothing: the live hook still observes.
  bus.setTopicWatermarkIdle('t', 30);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(120);
  assert.equal(seen.length, 1);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('re-installing the live callback is a no-op with no audit', () => {
  const handler = (_e: WatermarkIdleEvent): void => {};
  const bus = new EventBus({ onWatermarkIdle: handler });
  bus.setWatermarkIdleCallback(handler);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('a closed bus refuses the swap', async () => {
  const bus = new EventBus();
  await bus.close();
  const handler = (_e: WatermarkIdleEvent): void => {};
  assert.throws(() => bus.setWatermarkIdleCallback(handler), RangeError);
  assert.throws(() => bus.setWatermarkIdleCallback(null), RangeError);
  assert.equal(audit(bus).length, 0);
});

test('a throwing hook is swallowed and the publish path is unaffected', async () => {
  const bus = new EventBus();
  bus.setWatermarkIdleCallback(() => {
    throw new Error('broken observer');
  });
  bus.setTopicWatermarkIdle('t', 30);
  const received: unknown[] = [];
  bus.subscribe('t', (m) => received.push(m.payload));
  bus.publish('t', 'a', { eventTime: 1000 });
  await flush();
  assert.deepEqual(received, ['a']);
  await sleep(120);
  assert.equal(advances(bus, 't'), 1);
  // An admitted publish still flows to the subscriber after the throw.
  bus.publish('t', 'b', { eventTime: 2000 });
  await flush();
  assert.deepEqual(received, ['a', 'b']);
  bus.destroy?.();
});

test('the swap audits exact presence flags with the actor', () => {
  const first = (_e: WatermarkIdleEvent): void => {};
  const second = (_e: WatermarkIdleEvent): void => {};
  const bus = new EventBus();
  bus.setWatermarkIdleCallback(first, 'ops');
  bus.setWatermarkIdleCallback(second, 'ops');
  bus.setWatermarkIdleCallback(null, 'ops');
  const entries = audit(bus);
  assert.equal(entries.length, 3);
  assert.deepEqual(entries[0].detail, { hasCallback: true, oldHasCallback: false });
  assert.equal(entries[0].actor, 'ops');
  assert.deepEqual(entries[1].detail, { hasCallback: true, oldHasCallback: true });
  assert.deepEqual(entries[2].detail, { hasCallback: false, oldHasCallback: true });
  // The function itself is never recorded — only its presence.
  assert.ok(!JSON.stringify(entries).includes('first'));
  bus.destroy?.();
});

test('setWatermarkIdleCallback rejects an illegal actor', () => {
  const bus = new EventBus();
  const handler = (_e: WatermarkIdleEvent): void => {};
  assert.throws(() => bus.setWatermarkIdleCallback(handler, ''), RangeError);
  assert.throws(() => bus.setWatermarkIdleCallback(handler, 42 as never), RangeError);
  assert.equal(audit(bus).length, 0);
  bus.destroy?.();
});

test('a restart restores the construction-time callback', async () => {
  const constructionSeen: WatermarkIdleEvent[] = [];
  const liveSeen: WatermarkIdleEvent[] = [];
  const construction = (e: WatermarkIdleEvent) => constructionSeen.push(e);
  const live = (e: WatermarkIdleEvent) => liveSeen.push(e);
  // First bus: the live hook replaces the construction-time one.
  const bus = new EventBus({ onWatermarkIdle: construction });
  bus.setWatermarkIdleCallback(live);
  bus.setTopicWatermarkIdle('t', 30);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(120);
  assert.equal(constructionSeen.length, 0);
  assert.equal(liveSeen.length, 1);
  bus.destroy?.();
  // A fresh bus with the same construction options (a restart) fires
  // the construction-time hook again.
  const restarted = new EventBus({ onWatermarkIdle: construction });
  restarted.setTopicWatermarkIdle('t', 30);
  restarted.publish('t', 'a', { eventTime: 1000 });
  await sleep(120);
  assert.equal(constructionSeen.length, 1);
  assert.equal(liveSeen.length, 1);
  restarted.destroy?.();
});

test('a topic with no advancement is unaffected by the revision', async () => {
  const seen: WatermarkIdleEvent[] = [];
  const bus = new EventBus();
  bus.setWatermarkIdleCallback((e) => seen.push(e));
  bus.setTopicWatermarkIdle('a', 30);
  bus.setTopicWatermarkIdle('b', 30);
  // Only 'a' sees event-time traffic; 'b' has a rule but no watermark
  // (a plain publish creates its stats row without arming the timer).
  bus.publish('a', 'x', { eventTime: 1000 });
  bus.publish('b', 'plain');
  await sleep(120);
  assert.equal(seen.length, 1);
  assert.equal(seen[0].topic, 'a');
  assert.equal(advances(bus, 'a'), 1);
  assert.equal(advances(bus, 'b'), 0);
  bus.destroy?.();
});

test('two consecutive redirects both take effect', async () => {
  const firstSeen: WatermarkIdleEvent[] = [];
  const secondSeen: WatermarkIdleEvent[] = [];
  const bus = new EventBus();
  bus.setWatermarkIdleCallback((e) => firstSeen.push(e));
  bus.setTopicWatermarkIdle('t', 30);
  bus.publish('t', 'a', { eventTime: 1000 });
  await sleep(120);
  assert.equal(firstSeen.length, 1);
  assert.equal(secondSeen.length, 0);
  bus.setWatermarkIdleCallback((e) => secondSeen.push(e));
  bus.publish('t', 'b', { eventTime: 2000 });
  await sleep(120);
  assert.equal(firstSeen.length, 1);
  assert.equal(secondSeen.length, 1);
  assert.equal(secondSeen[0].watermark, 2000);
  assert.equal(audit(bus).length, 2);
  bus.destroy?.();
});

test('messages without eventTime never arm the timer, hook or not', async () => {
  const seen: WatermarkIdleEvent[] = [];
  const bus = new EventBus();
  bus.setWatermarkIdleCallback((e) => seen.push(e));
  bus.setTopicWatermarkIdle('t', 30);
  bus.publish('t', 'plain');
  await sleep(120);
  assert.equal(seen.length, 0);
  assert.equal(advances(bus, 't'), 0);
  bus.destroy?.();
});
