import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import { resolveKeyHotspotOptions } from '../src/keyhotspot.ts';
import type { KeyHotspotEvent } from '../src/keyhotspot.ts';

/** Yields until the bus's scheduled microtask flush has run. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

/** A manually-advanced clock handed to the bus via `EventBusOptions.now`. */
function controllableClock(startMs = 1_000) {
  const clock = { nowMs: startMs, now: () => clock.nowMs };
  return clock;
}

test('resolveKeyHotspotOptions rejects invalid configs', () => {
  assert.equal(resolveKeyHotspotOptions(undefined), undefined);
  assert.equal(resolveKeyHotspotOptions(false), undefined);
  assert.deepEqual(resolveKeyHotspotOptions(true)?.thresholdDepth, 100);
  for (const bad of [0, -1, 2.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => resolveKeyHotspotOptions({ thresholdDepth: bad }),
      RangeError,
      `thresholdDepth ${bad}`,
    );
  }
  assert.throws(
    () => resolveKeyHotspotOptions({ onKeyHotspot: 'nope' as never }),
    TypeError,
  );
  assert.throws(() => resolveKeyHotspotOptions(42 as never), TypeError);
});

test('hotspot alert fires once per excursion and re-arms after the buffer drains', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: {
      thresholdDepth: 2,
      onKeyHotspot: (e) => events.push(e),
    },
  });

  bus.publishDelayed('a', 'delayed', { delayMs: 10_000, key: 'k' }); // keySeq 1, due later
  bus.publish('b', 'live1', { key: 'k' }); // keySeq 2: held (depth 1), no alert yet
  assert.equal(events.length, 0);
  bus.publish('c', 'live2', { key: 'k' }); // keySeq 3: depth 2 -> alert fires
  assert.equal(events.length, 1);
  assert.deepEqual(events[0], {
    subscriberId: sub.id,
    pattern: '*',
    key: 'k',
    bufferedDepth: 2,
    thresholdDepth: 2,
    at: clock.nowMs,
  });
  bus.publish('d', 'live3', { key: 'k' }); // keySeq 4: depth 3, latch held -> no second alert
  assert.equal(events.length, 1);
  await flush();
  assert.deepEqual(received, []); // everything still held behind keySeq 1

  // Fan out the delayed predecessor: the cascade admits 2/3/4 in order and
  // the buffer drains, re-arming the latch.
  clock.nowMs += 10_000;
  bus.publish('x', 'trigger');
  await flush();
  assert.deepEqual(received, ['trigger', 'delayed', 'live1', 'live2', 'live3']);
  assert.equal(bus.getStats().hotKeys.length, 0);

  // Second excursion alerts again.
  bus.publishDelayed('a', 'delayed2', { delayMs: 10_000, key: 'k' });
  bus.publish('b', 'live4', { key: 'k' });
  assert.equal(events.length, 1);
  bus.publish('c', 'live5', { key: 'k' });
  assert.equal(events.length, 2);
  assert.equal(events[1].bufferedDepth, 2);
});

test('monitoring never disturbs per-key publish-order enforcement', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: { thresholdDepth: 1, onKeyHotspot: () => {} },
  });
  bus.publishDelayed('a', 'first', { delayMs: 200, key: 'k' }); // keySeq 1
  bus.publishDelayed('b', 'second', { delayMs: 100, key: 'k' }); // keySeq 2, due first: held
  clock.nowMs += 100;
  bus.publish('c', 'trigger');
  await flush();
  assert.deepEqual(received, ['trigger']); // keySeq 2 waits for keySeq 1 despite monitoring
  assert.equal(bus.getStats().keyedReorderedMessages, 1);
  clock.nowMs += 100;
  bus.publish('c', 'trigger2');
  await flush();
  assert.deepEqual(received, ['trigger', 'trigger2', 'first', 'second']);
});

test('hotKeys ranks by depth and is capped at 10', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('*', () => {}, { keyHotspot: true }); // no callback: reporting only
  // 12 keys, each with one delayed predecessor + one held live message.
  for (let i = 0; i < 12; i++) {
    const key = `k${i.toString().padStart(2, '0')}`;
    bus.publishDelayed('a', 'delayed', { delayMs: 60_000, key });
    bus.publish('b', 'live', { key });
  }
  const hotKeys = bus.getStats().hotKeys;
  assert.equal(hotKeys.length, 10); // HOT_KEYS_LIMIT
  for (const h of hotKeys) {
    assert.equal(h.bufferedDepth, 1);
    assert.equal(h.thresholdDepth, 100); // the `true` default
    assert.equal(h.pattern, '*');
  }
  // Ties break deterministically on (subscriberId, key).
  assert.deepEqual(
    hotKeys.map((h) => h.key),
    ['k00', 'k01', 'k02', 'k03', 'k04', 'k05', 'k06', 'k07', 'k08', 'k09'],
  );
});

test('hotKeys only samples monitored subscriptions', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  bus.subscribe('*', () => {}); // no keyHotspot
  bus.publishDelayed('a', 'delayed', { delayMs: 60_000, key: 'k' });
  bus.publish('b', 'live', { key: 'k' }); // held, but nobody is watching
  assert.deepEqual(bus.getStats().hotKeys, []);
  assert.ok(
    !renderPrometheus(bus.getStats()).includes('eventbus_key_hotspot_buffer_depth{'),
    'no hotspot series without opt-in',
  );
});

test('hotspot depth renders in the Prometheus exposition', () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const sub = bus.subscribe('*', () => {}, { keyHotspot: true });
  bus.publishDelayed('a', 'delayed', { delayMs: 60_000, key: 'k' });
  bus.publish('b', 'live1', { key: 'k' });
  bus.publish('c', 'live2', { key: 'k' });
  const text = renderPrometheus(bus.getStats());
  assert.ok(
    text.includes(
      `eventbus_key_hotspot_buffer_depth{subscriber="${sub.id}",pattern="*",key="k"} 2`,
    ),
    `missing hotspot gauge in:\n${text}`,
  );
});

test('cancelled delayed schedules re-arm the latch via skipKeySeq', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: KeyHotspotEvent[] = [];
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: { thresholdDepth: 1, onKeyHotspot: (e) => events.push(e) },
  });
  const delayId = bus.publishDelayed('a', 'doomed', { delayMs: 60_000, key: 'k' });
  assert.ok(delayId !== undefined);
  bus.publish('b', 'live', { key: 'k' }); // held behind the schedule
  assert.equal(events.length, 1);
  bus.cancelDelayed(delayId!);
  await flush();
  // The schedule's keySeq is skipped, the cascade admits the held message,
  // and the latch re-arms.
  assert.deepEqual(received, ['live']);
  assert.equal(bus.getStats().hotKeys.length, 0);
  bus.publishDelayed('a', 'doomed2', { delayMs: 60_000, key: 'k' });
  bus.publish('b', 'live2', { key: 'k' });
  assert.equal(events.length, 2);
});
