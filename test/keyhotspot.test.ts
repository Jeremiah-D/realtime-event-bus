import test from 'node:test';
import assert from 'node:assert/strict';
import { EventBus } from '../src/bus.ts';
import { renderPrometheus } from '../src/metrics.ts';
import { resolveKeyHotspotOptions } from '../src/keyhotspot.ts';
import type { KeyHotspotEvent, HotKeyMitigatedEvent } from '../src/keyhotspot.ts';

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

test('resolveKeyHotspotOptions validates autoMitigate', () => {
  // Absent = detection only.
  assert.equal(resolveKeyHotspotOptions(true)?.mitigate, undefined);
  assert.equal(resolveKeyHotspotOptions({ thresholdDepth: 2 })?.mitigate, undefined);
  assert.equal(resolveKeyHotspotOptions({ thresholdDepth: 2, autoMitigate: false })?.mitigate, undefined);
  // `true` enables defaults: recoverDepth mirrors the alert threshold.
  assert.deepEqual(resolveKeyHotspotOptions({ thresholdDepth: 5, autoMitigate: true })?.mitigate, {
    recoverDepth: 5,
    onHotKeyMitigated: undefined,
  });
  // Object form tunes the recovery depth and the callback.
  const cb = () => {};
  assert.deepEqual(
    resolveKeyHotspotOptions({ thresholdDepth: 5, autoMitigate: { recoverDepth: 2, onHotKeyMitigated: cb } })?.mitigate,
    { recoverDepth: 2, onHotKeyMitigated: cb },
  );
  for (const bad of [-1, 2.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () => resolveKeyHotspotOptions({ thresholdDepth: 5, autoMitigate: { recoverDepth: bad } }),
      RangeError,
      `recoverDepth ${bad}`,
    );
  }
  // recoverDepth above the alert threshold defeats the hysteresis band.
  assert.throws(
    () => resolveKeyHotspotOptions({ thresholdDepth: 5, autoMitigate: { recoverDepth: 6 } }),
    RangeError,
  );
  assert.throws(
    () => resolveKeyHotspotOptions({ thresholdDepth: 5, autoMitigate: { onHotKeyMitigated: 'nope' as never } }),
    TypeError,
  );
  assert.throws(
    () => resolveKeyHotspotOptions({ thresholdDepth: 5, autoMitigate: 42 as never }),
    TypeError,
  );
});

test('autoMitigate engages at the threshold and sheds new arrivals for the hot key', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const events: HotKeyMitigatedEvent[] = [];
  const received: unknown[] = [];
  const sub = bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: {
      thresholdDepth: 2,
      autoMitigate: { onHotKeyMitigated: (e) => events.push(e) },
    },
  });

  bus.publishDelayed('a', 'delayed', { delayMs: 10_000, key: 'k' }); // keySeq 1, due later
  bus.publish('b', 'live1', { key: 'k' }); // keySeq 2: held (depth 1)
  assert.equal(events.length, 0);
  bus.publish('c', 'live2', { key: 'k' }); // keySeq 3: depth 2 -> mitigation engages
  assert.equal(events.length, 1);
  assert.deepEqual(events[0], {
    subscriberId: sub.id,
    pattern: '*',
    key: 'k',
    bufferedDepth: 2,
    thresholdDepth: 2,
    recoverDepth: 2, // default mirrors the alert threshold
    at: clock.nowMs,
  });
  assert.equal(bus.getStats().hotKeys[0].mitigated, true);

  bus.publish('d', 'live3', { key: 'k' }); // keySeq 4: shed, never buffered
  bus.publish('e', 'live4', { key: 'k' }); // keySeq 5: shed
  assert.equal(events.length, 1); // one engagement per excursion
  assert.equal(bus.getStats().hotKeyMitigations, 1);
  assert.equal(bus.getStats().hotKeyShedMessages, 2);
  assert.equal(bus.getStats().hotKeys[0].bufferedDepth, 2); // the pile stopped growing

  // Fan out the delayed predecessor: the cascade admits keySeqs 2/3 in
  // order and skips the shed 4/5 — the stream never hangs on the gap.
  clock.nowMs += 10_000;
  bus.publish('x', 'trigger');
  await flush();
  assert.deepEqual(received, ['trigger', 'delayed', 'live1', 'live2']);
  // Buffer drained below recoverDepth -> mitigation released.
  assert.deepEqual(bus.getStats().hotKeys, []);
  assert.equal(bus.getStats().hotKeyShedMessages, 2);

  // The key flows again after release.
  bus.publish('f', 'live5', { key: 'k' });
  await flush();
  assert.ok(received.includes('live5'));
});

test('autoMitigate sheds only the hot key; other keys flow', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: { thresholdDepth: 1, autoMitigate: true },
  });
  bus.publishDelayed('a', 'delayed', { delayMs: 10_000, key: 'hot' }); // hot keySeq 1
  bus.publish('b', 'hot-live', { key: 'hot' }); // hot keySeq 2: depth 1 -> engage
  assert.equal(bus.getStats().hotKeyMitigations, 1);
  bus.publish('c', 'hot-shed', { key: 'hot' }); // hot keySeq 3: shed
  // A different key is untouched by the mitigation.
  bus.publish('d', 'cold1', { key: 'cold' });
  bus.publish('e', 'cold2', { key: 'cold' });
  await flush();
  assert.deepEqual(received, ['cold1', 'cold2']);
  assert.equal(bus.getStats().hotKeyShedMessages, 1);
  const byKey = new Map(bus.getStats().hotKeys.map((h) => [h.key, h]));
  assert.equal(byKey.get('hot')?.mitigated, true);
  assert.equal(byKey.get('cold'), undefined); // cold never buffered
});

test('autoMitigate is disabled by default: detection only, nothing shed', async () => {
  const clock = controllableClock();
  const bus = new EventBus({ now: clock.now });
  const received: unknown[] = [];
  bus.subscribe('*', (msg) => received.push(msg.payload), {
    keyHotspot: { thresholdDepth: 2 }, // no autoMitigate
  });
  bus.publishDelayed('a', 'delayed', { delayMs: 10_000, key: 'k' });
  bus.publish('b', 'live1', { key: 'k' });
  bus.publish('c', 'live2', { key: 'k' });
  bus.publish('d', 'live3', { key: 'k' });
  assert.equal(bus.getStats().hotKeys[0].bufferedDepth, 3);
  assert.equal(bus.getStats().hotKeys[0].mitigated, false);
  assert.equal(bus.getStats().hotKeyMitigations, 0);
  assert.equal(bus.getStats().hotKeyShedMessages, 0);
  clock.nowMs += 10_000;
  bus.publish('x', 'trigger');
  await flush();
  assert.deepEqual(received, ['trigger', 'delayed', 'live1', 'live2', 'live3']);
});

test('a throwing onHotKeyMitigated propagates to the publish call', () => {
  const bus = new EventBus();
  bus.subscribe('*', () => {}, {
    keyHotspot: {
      thresholdDepth: 1,
      autoMitigate: {
        onHotKeyMitigated: () => {
          throw new Error('boom');
        },
      },
    },
  });
  bus.publishDelayed('a', 'delayed', { delayMs: 10_000, key: 'k' });
  assert.throws(() => bus.publish('b', 'live', { key: 'k' }), /boom/);
  // The engagement latches before the callback runs, like onKeyHotspot.
  assert.equal(bus.getStats().hotKeyMitigations, 1);
  assert.equal(bus.getStats().hotKeys[0].mitigated, true);
});

test('mitigation counters render in the Prometheus exposition', () => {
  const bus = new EventBus();
  bus.subscribe('*', () => {}, {
    keyHotspot: { thresholdDepth: 1, autoMitigate: true },
  });
  bus.publishDelayed('a', 'delayed', { delayMs: 60_000, key: 'k' });
  bus.publish('b', 'live1', { key: 'k' }); // depth 1 -> engage
  bus.publish('c', 'live2', { key: 'k' }); // shed
  const text = renderPrometheus(bus.getStats());
  assert.ok(text.includes('eventbus_hotkey_mitigations_total 1'), text);
  assert.ok(text.includes('eventbus_hotkey_shed_messages_total 1'), text);
});
